"""Real planner/Writer DB; installation authority and provider network are fixtures."""
from __future__ import annotations

import json
import socket
import unittest
from unittest.mock import patch

from tests import test_v8_account_roster_capture as fixture
from v8 import account_catalog_capture as catalog, account_directory, capture_runtime as runtime
from v8 import capture_planning as planning
from v8.account_catalog_capture_release import ACCOUNT_CATALOG_POLICY
from v8.account_operating_receipts import record_status_receipt
from v8.operations import upsert_account
from v8.profile_activations import activation_at
from v8.storage import connect, transaction

AT = fixture.AFTER
SEC = "MS4wLjAB" + "A" * 64


class CatalogCapturePlannerTest(unittest.TestCase):
    def setUp(self):
        self.base = fixture.AccountRosterCaptureTest()
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.db = self.base.db
        self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.enterContext(patch.object(catalog, "installed_policy", return_value=ACCOUNT_CATALOG_POLICY))
        self.account = upsert_account({"phone": "", "enabled": False, "platforms": [
            {"platform": "douyin", "uid": "123456789", "nickname": "catalog fixture"}]}, db_path=self.db)
        with connect(self.db) as connection, transaction(connection):
            self.aid = self.account["id"]
            self.iid = connection.execute("SELECT id FROM account_platform_identities WHERE account_id=?", (self.aid,)).fetchone()[0]
            connection.execute("INSERT INTO account_provider_references(account_identity_id,provider,reference_kind,reference_value,created_at,updated_at) VALUES(?,'TikHub','sec_user_id',?,?,?)",
                (self.iid, SEC, AT, AT))
            account_directory.ensure_account_directory_schema(connection)
            connection.execute("INSERT INTO account_directory_rows(source_sha256,source_name,source_sheet,source_row,account_id,platform,uid,account_status,identity_status,raw_json,imported_at,updated_at) VALUES(?,'fixture','fixture',2,?,'douyin','123456789','daily','existing_verified','{}',?,?)",
                ("a" * 64, self.aid, AT, AT))
            record_status_receipt(connection, request_id="catalog-fixture-admission", account_id=self.aid,
                account_identity_id=self.iid, requested_status="daily", update_frequency="daily",
                request={"account_status": "daily", "fields": {}, "admission": {"member": {
                    "platform": "douyin", "uid": "123456789", "metadata": {"sec_user_id": SEC}}}},
                actor="fixture", reason="verified profile admission", before={"enabled": False, "update_frequency": None},
                after={"enabled": True, "update_frequency": "daily"}, result={"id": self.aid,
                    "status_request_id": "catalog-fixture-admission", "account_status": "daily", "enabled": True,
                    "update_frequency": "daily"}, timestamp=AT)
        self.before = self.immutable_counts()

    def immutable_counts(self):
        with connect(self.db) as connection:
            return {name: connection.execute("SELECT count(*) FROM " + name).fetchone()[0] for name in (
                "account_roster_snapshots", "account_roster_members", "acquisition_profile_activations",
                "provider_usage", "provider_raw_responses", "content_items")}

    def plan(self, at=AT, shadow=False):
        with connect(self.db) as connection, transaction(connection):
            return runtime._cohort_plan(connection, activation_at(connection, at), at=at, shadow=shadow)

    def test_disabled_outside_old_roster_enters_real_immutable_plan_without_roster_writes(self):
        first = self.plan()
        self.assertEqual([m["identity_id"] for m in first["cohort"]], [self.iid])
        self.assertEqual(self.plan()["id"], first["id"])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT enabled FROM accounts WHERE id=?", (self.aid,)).fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT count(*) FROM account_roster_members WHERE account_identity_id=?", (self.iid,)).fetchone()[0], 0)
        self.assertEqual(self.immutable_counts(), self.before)

    def test_pause_keeps_planned_member_and_preserves_previous_snapshot(self):
        first = self.plan()
        with connect(self.db) as connection, transaction(connection):
            frozen = connection.execute("SELECT payload_json FROM capture_source_plans WHERE id=?", (first["id"],)).fetchone()[0]
            connection.execute("UPDATE account_directory_rows SET account_status='paused'")
        second = self.plan(at="2026-09-02T12:06:00Z")
        self.assertNotEqual(first["id"], second["id"])
        self.assertEqual([m["identity_id"] for m in second["cohort"]], [self.iid])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT payload_json FROM capture_source_plans WHERE id=?", (first["id"],)).fetchone()[0], frozen)
            self.assertEqual(catalog.validate_plan_member(connection, first["id"], self.iid, at=AT)["identity_id"], self.iid)
            self.assertEqual(connection.execute("SELECT enabled FROM accounts WHERE id=?", (self.aid,)).fetchone()[0], 1)
        self.assertEqual(self.immutable_counts(), self.before)

    def test_shadow_does_not_turn_on_legacy_projection(self):
        result = self.plan(shadow=True)
        self.assertEqual(len(result["cohort"]), 1)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT enabled FROM accounts WHERE id=?", (self.aid,)).fetchone()[0], 0)
        active = self.plan()
        self.assertNotEqual(active["id"], result["id"])
        self.assertFalse(active["shadow"])

    def test_real_enqueue_has_derived_scope_is_idempotent_and_transport_gate_survives(self):
        plan = self.plan()
        with connect(self.db) as connection, transaction(connection):
            args = dict(stage="discovery", operation="douyin_user_posts", logical_due="discovery:" + runtime._bucket(AT, 3600), at=AT)
            self.assertTrue(runtime._enqueue(connection, plan, plan["cohort"][0], **args))
            self.assertFalse(runtime._enqueue(connection, plan, plan["cohort"][0], **args))
            work = connection.execute("SELECT * FROM capture_work_items WHERE account_id=?", (self.aid,)).fetchone()
            envelope = json.loads(work["envelope_json"])
            self.assertEqual(envelope["catalog_plan_id"], plan["id"])
            self.assertEqual(work["state"], "runnable")
            body = {"provider": "tikhub", "operation": "douyin_user_posts", "state": "closed", "reason": "fixture hold", "evidence_json": "{}", "recorded_at": AT}
            connection.execute("INSERT INTO capture_paid_send_gate_events(provider,operation,state,reason,evidence_json,recorded_at,event_sha256) VALUES(?,?,?,?,?,?,?)", (*body.values(), planning.digest(body)))
            self.assertEqual(runtime._readiness(connection, envelope, at=AT), ("provider_blocked", "provider_transport_blocked"))
        self.assertEqual(self.immutable_counts(), self.before)

    def test_public_scope_uses_active_plan_instead_of_later_shadow_plan(self):
        self.plan()
        with connect(self.db) as connection:
            published = catalog.public_statuses(connection)
        self.assertEqual(len(published), 1)
        self.assertTrue(next(iter(published.values()))["eligible"])
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE account_directory_rows SET account_status='paused'")
        self.plan(at="2026-09-02T12:06:00Z", shadow=True)
        with connect(self.db) as connection:
            self.assertEqual(catalog.public_statuses(connection), published)

    def profile_work(self, at=AT, *, due=None):
        plan = self.plan(at=at)
        with connect(self.db) as connection, transaction(connection):
            created = runtime._enqueue(connection, plan, plan["cohort"][0], stage="account_metrics",
                operation="douyin_uid_profile", logical_due=due or "account-metrics:" + runtime._bucket(at, 6 * 3600),
                at=at)
            return created, [dict(row) for row in connection.execute(
                "SELECT * FROM capture_work_items WHERE account_id=? AND operation='douyin_uid_profile' ORDER BY id",
                (self.aid,))]

    def held_profile(self):
        created, rows = self.profile_work()
        self.assertTrue(created)
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE capture_work_items SET state='paid_identity_hold',reason='billing_unknown_retry_blocked' WHERE id=?",
                (rows[0]["id"],))
            return dict(connection.execute("SELECT * FROM capture_work_items WHERE id=?", (rows[0]["id"],)).fetchone())

    def test_profile_next_six_hour_cycle_is_idempotent_and_preserves_old_hold(self):
        old = self.held_profile()
        created, rows = self.profile_work("2026-09-02T18:10:00Z")
        self.assertTrue(created)
        self.assertEqual(rows[0], old)
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[1]["work_identity"], old["work_identity"])
        self.assertNotEqual(runtime._page_window(json.loads(rows[1]["envelope_json"])),
            runtime._page_window(json.loads(old["envelope_json"])))
        self.assertFalse(self.profile_work("2026-09-02T18:11:00Z")[0])
        self.assertEqual(self.immutable_counts(), self.before)

    def test_profile_same_cycle_and_invented_future_due_stay_blocked(self):
        old = self.held_profile()
        self.assertFalse(self.profile_work("2026-09-01T17:59:59Z")[0])
        self.assertFalse(self.profile_work("2026-09-02T18:10:00Z",
            due="account-metrics:2026-09-03T00:00:00Z")[0])
        self.assertEqual(self.profile_work()[1], [old])

    def test_profile_non_hold_work_still_owns_account(self):
        old = self.held_profile()
        for state in ("runnable", "provider_blocked", "budget_deferred", "leased", "running"):
            with self.subTest(state=state), connect(self.db) as connection, transaction(connection):
                connection.execute("UPDATE capture_work_items SET state=?,owner_token=? WHERE id=?",
                    (state, "owned" if state in {"leased", "running"} else None, old["id"]))
            self.assertFalse(self.profile_work("2026-09-02T18:10:00Z")[0])

    def test_profile_old_manual_compensation_identity_or_timestamp_cannot_gain_exception(self):
        old = self.held_profile()
        envelope = json.loads(old["envelope_json"])
        for fields in ({"kind": "metrics_update"}, {"manual_command_run_id": 9}, {"compensation": {}},
                {"request_batch_id": 1}, {"stage": "discovery"}, {"uid": "another"},
                {"source_plan_id": 999}, {"catalog_plan_id": 999}):
            with self.subTest(fields=fields):
                with connect(self.db) as connection, transaction(connection):
                    connection.execute("UPDATE capture_work_items SET envelope_json=? WHERE id=?",
                        (planning.canonical({**envelope, **fields}), old["id"]))
                self.assertFalse(self.profile_work("2026-09-02T18:10:00Z")[0])
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE capture_work_items SET envelope_json=?,updated_at=? WHERE id=?",
                (old["envelope_json"], "2026-09-02T18:00:00Z", old["id"]))
        self.assertFalse(self.profile_work("2026-09-02T18:10:00Z")[0])

    def test_profile_cycle_check_is_read_only_and_requires_current_authority(self):
        self.held_profile()
        at = "2026-09-02T18:10:00Z"
        plan = self.plan(at=at)
        with connect(self.db) as connection:
            connection.execute("PRAGMA query_only=ON")
            before = connection.total_changes
            self.assertFalse(runtime._account_metric_cycle_pending(connection, plan, plan["cohort"][0],
                operation="douyin_uid_profile", logical_due="account-metrics:2026-09-02T18:00:00Z", at=at))
            with patch.object(catalog, "validate_plan_member", side_effect=ValueError("identity changed")):
                self.assertTrue(runtime._account_metric_cycle_pending(connection, plan, plan["cohort"][0],
                    operation="douyin_uid_profile", logical_due="account-metrics:2026-09-02T18:00:00Z", at=at))
            self.assertEqual(connection.total_changes, before)

    def test_profile_new_cycle_still_obeys_transport_gate(self):
        old = self.held_profile()
        at = "2026-09-02T18:10:00Z"
        with connect(self.db) as connection, transaction(connection):
            body = {"provider": "tikhub", "operation": "douyin_uid_profile", "state": "closed",
                "reason": "fixture hold", "evidence_json": "{}", "recorded_at": at}
            connection.execute("INSERT INTO capture_paid_send_gate_events(provider,operation,state,reason,evidence_json,recorded_at,event_sha256) VALUES(?,?,?,?,?,?,?)",
                (*body.values(), planning.digest(body)))
        created, rows = self.profile_work(at)
        self.assertTrue(created)
        self.assertEqual(rows[0], old)
        self.assertEqual((rows[1]["state"], rows[1]["reason"]), ("provider_blocked", "provider_transport_blocked"))
        self.assertEqual(self.immutable_counts(), self.before)


if __name__ == "__main__":
    unittest.main()
