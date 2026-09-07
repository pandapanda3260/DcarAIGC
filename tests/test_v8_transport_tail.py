from __future__ import annotations

import json
import unittest
from datetime import timedelta
from contextlib import ExitStack
from time import monotonic
from unittest.mock import patch

from tests import test_v8_transport_runner as runner_fixture
from tests import test_v8_transport_members as member_fixture
from v8 import capture, durable_runs, paid_drain, tikhub_scan
from v8.source_routing import parse_time
from v8.storage import connect, transaction
from v8.transport_accounting import settle_primary_member_unknown

AT = runner_fixture.AT


class TransportTailTest(unittest.TestCase):
    def setUp(self):
        self.fixture = runner_fixture.TransportRunnerTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        seed = member_fixture.TransportMembersTest._before_hold

        def with_historical_run(fixture):
            seed(fixture)
            for job in ("paid_capture_direct", tikhub_scan.MATERIALIZATION_JOB):
                claim = durable_runs.claim_run(job, {"fixture": "pre-start-partial"},
                                               db_path=fixture.db, now="2026-09-05T02:00:00Z")
                durable_runs.finish_run(claim, status="partial", db_path=fixture.db,
                                        now="2026-09-05T02:01:00Z", next_resume_at="2026-09-05T02:06:00Z")

        with patch.object(member_fixture.TransportMembersTest, "_before_hold", with_historical_run):
            self.fixture.setUp()
        self.db = self.fixture.db
        self.drain_id = self.fixture.fixture.fixture.campaign["payload"]["hold_binding"]["drain_id"]

    def _verify(self, *, at=AT):
        with connect(self.db) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            result = paid_drain.verify_profile_drain_sealable(connection, self.drain_id, now=at)
            self.assertEqual(connection.total_changes, 0)
            return result

    def _settle(self, member_id):
        with connect(self.db) as connection, transaction(connection):
            return settle_primary_member_unknown(
                connection, member_id, scheduler=self.fixture.scheduler, at=AT,
                mirror_root=self.fixture.fixture.fixture.mirror_root,
            )

    def test_all_natural_members_and_zero_cost_derivations_close_exactly(self):
        self.fixture._run()
        proof = self._verify()["diagnostic_tail"]
        ids = proof["verified_ids"]
        self.assertEqual(len(ids["usage_ids"]), 20)
        self.assertEqual(len(ids["dispatch_event_ids"]), 60)
        self.assertEqual(len(ids["paid_run_ids"]), 64)
        self.assertEqual(len(ids["paid_attempt_ids"]), 64)
        self.assertEqual(len(ids["materialization_run_ids"]), 20)
        self.assertEqual(len(ids["materialization_attempt_ids"]), 20)
        self.assertGreater(len(ids["raw_response_ids"]), 20)
        self.assertEqual(len(ids["fetch_attempt_ids"]), len(ids["raw_response_ids"]))
        self.assertFalse(proof["qualified"])
        self.assertEqual(proof, self._verify()["diagnostic_tail"])
        with connect(self.db) as connection:
            self.assertEqual(paid_drain.dispatch_state(connection, at=AT).state, "draining")

    def test_unknown_blocks_until_full_conservative_accounting_then_closes(self):
        self.fixture.fail_rank = 1
        result = self.fixture._run()["receipt"]["payload"]
        with self.assertRaisesRegex(paid_drain.PaidDrainError, "accounting"):
            self._verify()
        settlement = self._settle(result["members"][0]["receipt_id"])
        proof = self._verify()["diagnostic_tail"]
        self.assertEqual(proof["accounting_receipt_ids"], [settlement["accounting_receipt_id"]])
        self.assertEqual(len(proof["verified_ids"]["usage_ids"]), 20)
        self.assertEqual(len(self.fixture.calls), 20)

    def test_full_error_prefix_closes_without_fake_full_sample(self):
        self.fixture.status = 402
        self.fixture.body_override = {"code": 402, "message": "insufficient balance"}
        result = self.fixture._run()["receipt"]["payload"]
        self.assertFalse(result["sample_complete"])
        proof = self._verify()["diagnostic_tail"]
        self.assertEqual(len(proof["verified_ids"]["usage_ids"]), 1)
        self.assertEqual(len(proof["verified_ids"]["raw_response_ids"]), 1)
        self.assertEqual(len(proof["verified_ids"]["paid_run_ids"]), 64)
        self.assertFalse(proof["qualified"])

    def test_extra_usage_raw_and_attempt_cannot_hide_behind_diagnostic_label(self):
        self.fixture._run()
        self._verify()
        with connect(self.db) as connection:
            for table, query in (
                ("usage", "INSERT INTO provider_usage(provider,operation,request_attempts,billed_requests,currency,amount,recorded_at,details_json) VALUES ('TikHub','douyin_user_posts',0,0,'USD',0,?,?)"),
                ("raw", "INSERT INTO provider_raw_responses(provider,operation,local_path,sha256,byte_size,http_status,captured_at,source) VALUES ('TikHub','douyin_user_posts','/nonexistent-extra-raw','0',1,200,?,'live')"),
                ("attempt", "INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at,response_finished_at,billed,amount) SELECT id,attempt_count+1,?,?,0,0 FROM fetch_slots ORDER BY id LIMIT 1"),
            ):
                with self.subTest(table=table):
                    connection.execute("BEGIN IMMEDIATE")
                    if table == "usage":
                        connection.execute(query, (AT, json.dumps({"state": "not_sent", "diagnostic_member": {"receipt_id": 99999}})))
                    elif table == "raw":
                        connection.execute(query, (AT,))
                    else:
                        connection.execute(query, (AT, AT))
                    with self.assertRaises(paid_drain.PaidDrainError):
                        paid_drain.verify_profile_drain_sealable(connection, self.drain_id, now=AT)
                    connection.rollback()
        self._verify()

    def test_post_start_new_attempt_on_old_paid_run_is_not_hidden_by_run_hwm(self):
        # This run really predates START; only its new attempt crosses the HWM.
        self.fixture._run()
        with connect(self.db) as connection, transaction(connection):
            before = connection.execute("SELECT id FROM scheduler_runs WHERE job_id='paid_capture_direct'").fetchone()[0]
            row = connection.execute("SELECT details_json FROM scheduler_runs WHERE id=?", (before,)).fetchone()
            identity = json.loads(row[0])["identity"]
            frozen = paid_drain._validated_profile_chain(connection)[-1].payload["frozen_dispatch"]
            self.assertLessEqual(before, frozen["scheduler_run_high_watermark"])
        later = durable_runs.claim_run("paid_capture_direct", identity, db_path=self.db,
                                       invocation_source="operator_retry", now=AT)
        self.assertIsNotNone(later)
        self.assertGreater(later.attempt_id, frozen["scheduler_attempt_high_watermark"])
        durable_runs.finish_run(later, status="partial", db_path=self.db, now=AT,
                                next_resume_at="2026-09-06T05:36:00Z")
        with self.assertRaises(paid_drain.PaidDrainError):
            self._verify()

    def test_new_or_resumed_orphan_materializer_blocks_even_without_fetch(self):
        self.fixture._run()
        self._verify()
        with connect(self.db) as connection:
            for identity in ({"fixture": "new-orphan"}, {"fixture": "pre-start-partial"}):
                with self.subTest(identity=identity), transaction(connection):
                    claim = durable_runs.claim_run_in_transaction(
                        connection, tikhub_scan.MATERIALIZATION_JOB, identity, now=AT,
                    )
                    self.assertIsNotNone(claim)
                    with self.assertRaisesRegex(paid_drain.PaidDrainError, "materialization_"):
                        paid_drain.verify_profile_drain_sealable(connection, self.drain_id, now=AT)
                    connection.rollback()

    def test_real_clock_advancing_during_derivation_uses_source_completion(self):
        tick = parse_time(AT)

        def clock():
            nonlocal tick
            tick += timedelta(seconds=1)
            return tick.isoformat().replace("+00:00", "Z")

        original_http = self.fixture._http

        def http(request, **kwargs):
            if not self.fixture.fixture.candidates:
                with connect(self.db) as connection:
                    self.fixture.fixture.candidates = runner_fixture.list_primary_due_candidates(connection, at=clock())
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_runs WHERE job_id='transport_receipt:member_permit'").fetchone()[0], 20)
            return original_http(request, **kwargs)

        with ExitStack() as stack:
            for module in (capture, runner_fixture.providers, runner_fixture.tikhub_scan,
                           runner_fixture.transport_execution, runner_fixture.transport_runner,
                           runner_fixture.metric_observations):
                stack.enter_context(patch.object(module, "now_utc", side_effect=clock))
            stack.enter_context(patch.object(runner_fixture.providers, "request_json_transport", side_effect=http))
            result = self.fixture._run()["receipt"]["payload"]
        self.assertTrue(result["sample_complete"])
        self.assertEqual(result["unresolved_billing_count"], 0)
        with connect(self.db) as connection:
            crossed = connection.execute(
                "SELECT COUNT(*) FROM provider_raw_responses r JOIN fetch_attempts a ON a.id=r.fetch_attempt_id "
                "JOIN scheduler_runs c ON c.job_id=? AND json_extract(c.details_json,'$.identity.account_id')="
                "(SELECT account_id FROM content_items WHERE id=r.content_id) "
                "WHERE r.source='derived_applied' AND a.response_finished_at>c.completed_at",
                (tikhub_scan.MATERIALIZATION_JOB,),
            ).fetchone()[0]
        self.assertGreater(crossed, 0)
        self._verify(at=clock())

    def test_local_only_recovery_closes_without_rewriting_failed_campaign(self):
        materialize = runner_fixture.providers.materialize_account_discovery_page
        attempts = 0

        def fail_once(**kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("fixture one local materialization failure")
            return materialize(**kwargs)

        with patch.object(runner_fixture.providers, "materialize_account_discovery_page", side_effect=fail_once):
            terminal = self.fixture._run()["receipt"]
        self.assertFalse(terminal["payload"]["results"][0]["materialized"])
        with self.assertRaises(paid_drain.PaidDrainError):
            self._verify()
        with connect(self.db) as connection:
            usage = list(connection.execute("SELECT * FROM provider_usage ORDER BY id"))
            dispatch = list(connection.execute("SELECT * FROM paid_provider_dispatch_events ORDER BY id"))
        recovered_at = "2026-09-06T05:45:00Z"
        with ExitStack() as stack:
            for module in (capture, runner_fixture.providers, runner_fixture.tikhub_scan, runner_fixture.metric_observations):
                stack.enter_context(patch.object(module, "now_utc", return_value=recovered_at))
            result = tikhub_scan.resume_local_materialization(
                terminal["payload"]["results"][0]["source_run_id"], db_path=self.db,
                raw_root=self.fixture.raw_root, now=recovered_at, deadline=monotonic() + 55,
            )
        self.assertTrue(result["complete"])
        self.assertTrue(result["materialization_finalized"])
        self.assertEqual(result["pages_this_run"], 0)
        self.assertEqual(len(self.fixture.calls), 20)
        with connect(self.db) as connection:
            self.assertEqual(usage, list(connection.execute("SELECT * FROM provider_usage ORDER BY id")))
            self.assertEqual(dispatch, list(connection.execute("SELECT * FROM paid_provider_dispatch_events ORDER BY id")))
        proof = self._verify(at=recovered_at)["diagnostic_tail"]
        self.assertEqual(len(proof["verified_ids"]["paid_attempt_ids"]), 65)
        self.assertEqual(len(proof["verified_ids"]["materialization_attempt_ids"]), 21)
        repeated = self.fixture._run()["receipt"]
        self.assertEqual(repeated, terminal)
