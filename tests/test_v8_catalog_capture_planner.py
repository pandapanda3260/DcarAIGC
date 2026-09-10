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

    def test_pause_changes_plan_immediately_and_preserves_previous_snapshot(self):
        first = self.plan()
        with connect(self.db) as connection, transaction(connection):
            frozen = connection.execute("SELECT payload_json FROM capture_source_plans WHERE id=?", (first["id"],)).fetchone()[0]
            connection.execute("UPDATE account_directory_rows SET account_status='paused'")
        second = self.plan(at="2026-09-02T12:06:00Z")
        self.assertNotEqual(first["id"], second["id"])
        self.assertEqual(second["cohort"], [])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT payload_json FROM capture_source_plans WHERE id=?", (first["id"],)).fetchone()[0], frozen)
            with self.assertRaisesRegex(RuntimeError, "暂停"):
                catalog.validate_plan_member(connection, first["id"], self.iid, at=AT)
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


if __name__ == "__main__":
    unittest.main()
