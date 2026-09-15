"""Beijing charge days never change preparation's logical request or paid key."""
from __future__ import annotations

import json
from pathlib import Path
import unittest
from unittest.mock import patch

from tests import test_v8_account_preparation as fixture
from tests import test_v8_preparation_recovery as recovery_fixture
from tests import test_v8_resolver_parser_replay as resolver_fixture
from v8 import account_intake, account_preparation as prep, capture, capture_runtime as runtime, durable_runs
from v8.provider_budget import PaidScopeBlocked, assert_paid_scope_owner, paid_scope
from v8.storage import initialize_database

BEFORE = "2026-09-12T15:59:59Z"
AFTER = "2026-09-12T16:03:26Z"
LATER = "2026-09-12T16:10:00Z"
NEXT_DAY = "2026-09-13T16:03:26Z"
ACTIVE = fixture.ACTIVE


class PreparationBusinessDayV23Test(unittest.TestCase):
    submit = fixture.AccountPreparationTest.submit
    envelope = fixture.AccountPreparationTest.envelope
    scope = fixture.AccountPreparationTest.scope
    response = recovery_fixture.PreparationRecoveryTest.response
    raw_work = recovery_fixture.PreparationRecoveryTest.raw_work

    def setUp(self):
        with patch.object(fixture, "initialize_database",
                side_effect=lambda c, **kwargs: initialize_database(c, target_version=23)):
            fixture.AccountPreparationTest.setUp(self)
        self.assertEqual(self.db.execute("PRAGMA user_version").fetchone()[0], 23)
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))

    def work(self):
        return dict(self.db.execute("SELECT * FROM capture_work_items ORDER BY id DESC LIMIT 1").fetchone())

    def owner(self, work, *, at=AFTER):
        claim = runtime._claim_work(self.db, work, at=at)
        self.assertIsNotNone(claim)
        env = json.loads(work["envelope_json"])
        with paid_scope("reconcile", scheduler_run_id=claim.scheduler_run_id,
                scheduler_attempt_id=claim.attempt_id, business_day=runtime._business_day(at),
                **{key: env[key] for key in prep.SCOPE_FIELDS}):
            assert_paid_scope_owner(self.db)
        return claim

    def finish(self, claim, *, at=AFTER):
        durable_runs.finish_run_in_transaction(self.db, claim, status="partial", summary={"fixture": "no send"},
            next_resume_at=at, now=at)

    def test_after_midnight_new_work_uses_beijing_day_for_work_and_budget_task(self):
        self.submit()
        prep.enqueue_pending(self.db, active=ACTIVE, at=AFTER)
        work = self.work()
        self.assertEqual(work["data_business_day"], "2026-09-13")
        self.assertEqual(self.envelope()["task_id"], "account-preparation:2026-09-13")
        self.owner(work)
        before = self.db.total_changes
        self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER)["created"], 0)
        self.assertEqual(self.db.total_changes, before)

    def test_old_unclaimed_work_first_binds_current_charge_day_and_reuses_same_root(self):
        self.submit()
        prep.enqueue_pending(self.db, active=ACTIVE, at=BEFORE)
        original = self.work()
        self.assertEqual(original["data_business_day"], "2026-09-12")
        first = self.owner(original)
        self.finish(first)
        second = self.owner(original, at=LATER)
        self.assertEqual(second.scheduler_run_id, first.scheduler_run_id)
        self.assertEqual(second.attempt_number, 2)
        self.finish(second, at=LATER)
        child = self.owner(original, at=NEXT_DAY)
        saved = self.db.execute("SELECT * FROM scheduler_runs WHERE id=?", (child.scheduler_run_id,)).fetchone()
        first_root = self.db.execute("SELECT root_run_id FROM scheduler_runs WHERE id=?", (first.scheduler_run_id,)).fetchone()[0]
        self.assertEqual(saved["root_run_id"], first_root)
        self.assertEqual(saved["charge_business_day"], "2026-09-14")
        identity = json.loads(saved["details_json"])["identity"]
        self.assertEqual(identity["data_business_day"], "2026-09-12")
        self.assertEqual(identity["business_day"], "2026-09-14")
        self.assertEqual(self.work(), original)
        self.assertEqual(self.db.execute("SELECT count(*) FROM fetch_slots").fetchone()[0], 0)

    def test_old_unclaimed_content_and_discovery_keep_original_root_and_current_charge_child(self):
        self.submit()
        prep.enqueue_pending(self.db, active=ACTIVE, at=BEFORE)
        original = self.work()
        for stage in ("detail", "discovery"):
            with self.subTest(stage=stage):
                env = {**json.loads(original["envelope_json"]), "stage": stage}
                selected = {**original, "work_identity": prep.planning.digest({"fixture_stage": stage}),
                            "envelope_json": json.dumps(env)}
                expected = {"work_identity": selected["work_identity"], "business_day": "2026-09-12"}
                claim = runtime._claim_work(self.db, selected, at=AFTER)
                child = self.db.execute("SELECT * FROM scheduler_runs WHERE id=?", (claim.scheduler_run_id,)).fetchone()
                root = self.db.execute("SELECT * FROM scheduler_runs WHERE id=?", (child["root_run_id"],)).fetchone()
                frozen = json.loads(root["details_json"])
                self.assertEqual(frozen["identity"], expected)
                self.assertEqual(frozen["scan_id"], durable_runs.scan_identity(runtime.JOB, expected))
                self.assertEqual(root["scheduled_for"], "scan:" + frozen["scan_id"])
                self.assertEqual(root["status"], "partial")
                self.assertFalse(frozen["checkpoint"]["complete"])
                self.assertEqual(json.loads(child["details_json"])["identity"]["data_business_day"], "2026-09-12")
                with paid_scope("detail" if stage == "detail" else "reconcile", scheduler_run_id=claim.scheduler_run_id,
                        scheduler_attempt_id=claim.attempt_id, business_day="2026-09-13"):
                    assert_paid_scope_owner(self.db)
        for table in ("fetch_slots", "fetch_attempts", "provider_raw_responses", "provider_usage", "paid_provider_dispatch_events"):
            self.assertEqual(self.db.execute("SELECT count(*) FROM " + table).fetchone()[0], 0)

    def test_old_root_continues_next_day_without_rewriting_work_or_creating_paid_identity(self):
        self.submit()
        prep.enqueue_pending(self.db, active=ACTIVE, at=BEFORE)
        original = self.work()
        first = self.owner(original, at=BEFORE)
        self.finish(first, at=BEFORE)
        child = self.owner(original)
        self.finish(child)
        resumed = self.owner(original, at=LATER)
        self.assertEqual(resumed.scheduler_run_id, child.scheduler_run_id)
        self.assertEqual(resumed.attempt_number, 2)
        self.assertEqual(self.db.execute("SELECT count(*) FROM scheduler_runs WHERE root_run_id=?", (first.scheduler_run_id,)).fetchone()[0], 1)
        self.assertEqual(self.work(), original)
        for table in ("fetch_slots", "fetch_attempts", "provider_raw_responses", "provider_paid_scope_claims"):
            self.assertEqual(self.db.execute("SELECT count(*) FROM " + table).fetchone()[0], 0)

    def test_successful_raw_replays_after_midnight_with_original_window_and_no_purchase(self):
        _work, old_env, _request, raw_id, _response = self.raw_work(successful=True)
        self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=AFTER)["created"], 1)
        work, env = self.work(), self.envelope()
        self.assertEqual(env["logical_due"], old_env["logical_due"])
        self.assertEqual(env["replay_raw_response_id"], raw_id)
        claim = self.owner(work)
        self.db.commit()
        with paid_scope("reconcile", scheduler_run_id=claim.scheduler_run_id,
                scheduler_attempt_id=claim.attempt_id, business_day="2026-09-13",
                **{key: env[key] for key in prep.SCOPE_FIELDS}), \
             patch.object(capture, "execute_intake_fetch", side_effect=AssertionError("must not send")), \
             patch("v8.providers._budget_for_call", side_effect=AssertionError("must not buy")):
            result = prep.execute_step(env, db_path=Path(self.temp.name) / "test.sqlite3", at=AFTER)
        self.assertEqual(result["provider_cost"], 0)
        self.assertEqual(result["evidence"]["raw_response_ids"], [raw_id])
        self.assertEqual(self.db.execute("SELECT count(*) FROM fetch_attempts").fetchone()[0], 1)
        self.assertEqual(self.db.execute("SELECT count(*) FROM provider_raw_responses").fetchone()[0], 1)

    def test_prior_operation_raw_does_not_block_first_execution_of_current_operation(self):
        # A completed resolver is another logical request of the same intake;
        # current channel-info has no slot and must receive its own owner.
        resolver_fixture.ResolverParserReplayTest.raw_work(self)
        result = json.loads(self.db.execute("SELECT result_json FROM account_intake_requests").fetchone()[0])
        result["preparation_responses"] = [{"operation": "wechat_channels_resolve", "raw_response_id": self.raw_id}]
        self.db.execute("UPDATE account_intake_requests SET result_json=?", (json.dumps(result),))
        self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=AFTER)["created"], 1)
        current = PreparationBusinessDayV23Test.work(self)
        self.assertEqual(current["operation"], "wechat_channels_channel_info")
        self.owner(current)
        self.assertEqual(self.db.execute("SELECT count(*) FROM fetch_slots WHERE window_key=?",
            (json.loads(current["envelope_json"])["logical_due"],)).fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM provider_raw_responses").fetchone()[0], 1)

    def test_midnight_does_not_create_retry_for_unknown_billing(self):
        original, _env, _request, _raw_id, _response = self.raw_work(successful=False)
        result = prep.enqueue_pending(self.db, active=ACTIVE, at=AFTER)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["blocked"][0]["reason"], "preparation_billing_unverified")
        self.assertEqual(self.db.execute("SELECT count(*) FROM capture_work_items").fetchone()[0], 1)
        self.assertEqual(self.work()["id"], original["id"])
        self.assertEqual(self.work()["state"], "paid_identity_hold")

    def test_midnight_not_sent_pending_slot_resumes_naturally_on_existing_root_child(self):
        self.submit()
        prep.enqueue_pending(self.db, active=ACTIVE, at=BEFORE)
        original, env = self.work(), self.envelope()
        old_owner = self.owner(original, at=BEFORE)
        self.finish(old_owner, at=BEFORE)
        slot = capture.ensure_intake_slot(self.db, intake_request_id=env["intake_request_id"], stage="profile_prepare",
            window_key=env["logical_due"], provider="TikHub", adapter_version="fixture")
        # This is the exact post-cleanup boundary observed for intake115:
        # no send/attempt/raw, original pending slot, blocked work, partial root.
        self.db.execute("UPDATE fetch_slots SET status='pending',last_error_code='business_day_expired' WHERE id=?", (slot,))
        self.db.execute("UPDATE capture_work_items SET state='provider_blocked',reason='business_day_expired',attempt_count=3")
        slot_before = tuple(self.db.execute("SELECT * FROM fetch_slots WHERE id=?", (slot,)).fetchone())
        planned = prep.enqueue_pending(self.db, active=ACTIVE, at=AFTER)
        self.assertEqual(planned["created"], 0)
        self.assertNotIn("recovered_unsent", planned)
        with patch.object(prep, "readiness", return_value=("runnable", "")):
            reconsidered = runtime._plan_due(self.db, {"id": original["source_plan_id"], "cohort": []}, at=AFTER)
        self.assertEqual(reconsidered["reconsidered"], 1)
        work = self.work()
        self.assertEqual(work["state"], "runnable")
        current = self.owner(work)
        self.assertEqual(self.db.execute("SELECT root_run_id FROM scheduler_runs WHERE id=?", (current.scheduler_run_id,)).fetchone()[0], old_owner.scheduler_run_id)
        self.assertEqual(tuple(self.db.execute("SELECT * FROM fetch_slots WHERE id=?", (slot,)).fetchone()), slot_before)
        self.assertEqual(work["work_identity"], original["work_identity"])
        self.assertEqual(json.loads(work["envelope_json"])["logical_due"], env["logical_due"])
        self.assertEqual(work["attempt_count"], 3)
        for table in ("fetch_attempts", "provider_raw_responses", "provider_usage", "paid_provider_dispatch_events"):
            self.assertEqual(self.db.execute("SELECT count(*) FROM " + table).fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
