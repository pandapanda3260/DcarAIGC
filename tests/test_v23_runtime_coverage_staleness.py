"""Display-only stale classification with real sealed lineage and schema23 migration.

The original receipt is sealed by the installed schema20 test fixture, copied to
another disposable DB and migrated using the real 20->23 migrations. Corruption
is confined to savepoints in that copy; no runtime authority/read validator is
mocked, and all network access is prohibited by the composed fixture.
"""
from __future__ import annotations

import copy
from datetime import datetime
import json
import sqlite3
import unittest
from unittest.mock import patch

from tests import test_v8_catalog_runtime_receipts as fixtures
from v8 import api, capture_authorizations as auth, capture_day_coverage as catalog
from v8 import raw_archive, runtime_receipts as receipts
from v8.storage import initialize_database


class RuntimeCoverageStalenessV23Test(unittest.TestCase):
    def setUp(self):
        self.fixture = f = fixtures.CatalogRuntimeReceiptsTest(methodName="runTest")
        f.setUp(); self.addCleanup(f.doCleanups)
        f.seed_catalog_day()
        self.cutoff = f.cutoff
        self.template = dict(f.connection.execute("SELECT * FROM capture_source_plans WHERE id=?", (f.plan_id,)).fetchone())
        # Exactly the production failure's frozen 223 -> appended 224 shape.
        for _ in range(220):
            self.add_plan(f.connection, at=self.template["created_at"], shadow=True)
        f.connection.commit()
        self.sealed = f.seal(complete=True)
        self.binding = self.sealed["scope"]["source_binding"]["catalog"]
        self.assertEqual(len(self.binding["plan_inputs"]), 223)
        self.db_path = f.fixture.root / "coverage-schema23-copy.sqlite3"
        self.c = sqlite3.connect(self.db_path); self.c.row_factory = sqlite3.Row
        self.addCleanup(self.c.close)
        f.connection.backup(self.c)
        self.c.execute("PRAGMA recursive_triggers=ON")
        self.c.execute("PRAGMA foreign_keys=ON")
        initialize_database(self.c, target_version=23)
        self.c.commit()
        self.assertEqual(self.c.execute("PRAGMA user_version").fetchone()[0], 23)
        self.assertEqual(receipts.read_profile_day_coverage_receipt(self.c, at=self.cutoff)["self_sha256"], self.sealed["self_sha256"])
        self.assertTrue(receipts.latest_runtime_coverage(self.c, at=self.cutoff)["complete"])

    def add_plan(self, connection=None, *, at=None, shadow=False, payload_change=None, bad_hash=False):
        connection = self.c if connection is None else connection
        row = dict(self.template); row.pop("id")
        payload = json.loads(row["payload_json"])
        generation = connection.execute("SELECT coalesce(max(generation),0)+1 FROM capture_source_plans").fetchone()[0]
        payload.update(shadow=shadow, catalog_mode="shadow" if shadow else "active", generation=generation)
        if payload_change:
            payload_change(payload)
        row.update(mode="shadow" if shadow else "active", created_at=at or self.cutoff.replace("Z", ".000000Z"),
                   generation=generation,
                   payload_json=auth.canonical(payload), plan_sha256="0"*64 if bad_hash else auth.digest(payload))
        return connection.execute(f"INSERT INTO capture_source_plans({','.join(row)}) VALUES ({','.join('?' for _ in row)})", tuple(row.values())).lastrowid

    def drop_triggers(self, table):
        # Deliberate offline corruption only in a disposable savepoint.
        names = [r[0] for r in self.c.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?", (table,))]
        for name in names:
            self.c.execute('DROP TRIGGER "' + name + '"')

    def assert_unknown(self, reason):
        before = self.c.total_changes
        with patch.object(raw_archive, "read_response_entity", side_effect=AssertionError("status opened raw")):
            value = receipts.latest_runtime_coverage(self.c, at=self.cutoff)
            self.assertEqual(value["status"], "unknown", value)
            self.assertFalse(value["complete"], value)
            self.assertEqual(value["reason"], reason, value)
            self.assertIsNone(value.get("receipt"))
            with self.assertRaises(receipts.RuntimeReceiptError):
                receipts.read_profile_day_coverage_receipt(self.c, at=self.cutoff)
        self.assertEqual(self.c.total_changes, before)
        return value

    def test_same_second_223_to_224_is_unknown_and_health_projection_reads_without_writes(self):
        self.add_plan(); self.c.commit()
        with self.assertRaisesRegex(ValueError, "catalog_source_plan_inputs_changed"):
            catalog.validate_source_binding(self.c, self.binding, self.cutoff)
        self.assertTrue(catalog.validate_source_binding(self.c, self.binding, self.cutoff, allow_appended_inputs=True)["appended_inputs"])
        self.assert_unknown("profile_day_receipt_stale")
        before = self.c.total_changes
        result = api._data_freshness(self.c, current_at=datetime.fromisoformat(self.cutoff.replace("Z", "+00:00")))
        self.assertEqual(result["status"], "unknown")
        self.assertIsNone(result["last_successful_capture_at"])
        self.assertEqual(result["discovery_coverage"]["reason"], "profile_day_receipt_stale")
        self.assertEqual(self.c.total_changes, before)
        ready = receipts.current_activation_readiness(self.c, at=self.cutoff)
        self.assertFalse(ready["data_readiness"], ready)
        frozen = self.c.execute("SELECT receipt_sha256 FROM profile_day_coverage_receipts WHERE source_bridge_run_id=?", (self.sealed["run_id"],)).fetchone()[0]
        self.assertEqual(frozen, self.sealed["self_sha256"])

    def test_later_timestamp_does_not_invalidate_the_fixed_cutoff(self):
        self.add_plan(at=self.cutoff.replace("20:00:00", "20:00:01"))
        value = receipts.latest_runtime_coverage(self.c, at=self.cutoff)
        self.assertTrue(value["complete"], value)
        self.assertEqual(value["receipt"]["self_sha256"], self.sealed["self_sha256"])

    def test_schema20_default_does_not_enable_display_exception(self):
        self.add_plan(self.fixture.connection)
        self.assertEqual(self.fixture.connection.execute("PRAGMA user_version").fetchone()[0], 20)
        with self.assertRaises(receipts.RuntimeReceiptError):
            receipts.latest_runtime_coverage(self.fixture.connection, at=self.cutoff)

    def test_additional_inputs_require_valid_contract_mode_and_hash(self):
        mutations = (
            {"bad_hash": True},
            {"payload_change": lambda p: p.pop("contract_version")},
            {"payload_change": lambda p: p.update(catalog_mode="shadow")},
            {"payload_change": lambda p: p.update(business_day="invalid")},
        )
        for mutation in mutations:
            with self.subTest(mutation=tuple(mutation)):
                self.c.execute("SAVEPOINT broken_append")
                try:
                    self.add_plan(**mutation)
                    self.assert_unknown("profile_day_receipt_invalid")
                finally:
                    self.c.execute("ROLLBACK TO broken_append"); self.c.execute("RELEASE broken_append")

    def test_append_cannot_hide_removed_changed_or_reordered_original_input(self):
        self.add_plan()
        shadow_id = self.c.execute("SELECT id FROM capture_source_plans WHERE mode='shadow' ORDER BY id LIMIT 1").fetchone()[0]
        mutations = (
            ("DELETE FROM capture_source_plans WHERE id=?", (shadow_id,)),
            ("UPDATE capture_source_plans SET payload_json='{}' WHERE id=?", (shadow_id,)),
            ("UPDATE capture_source_plans SET created_at='2026-09-01T00:00:00Z' WHERE id=?", (shadow_id,)),
        )
        for sql, args in mutations:
            with self.subTest(sql=sql):
                self.c.execute("SAVEPOINT damaged_input")
                try:
                    self.drop_triggers("capture_source_plans")
                    self.c.execute(sql, args)
                    self.assert_unknown("profile_day_receipt_invalid")
                finally:
                    self.c.execute("ROLLBACK TO damaged_input"); self.c.execute("RELEASE damaged_input")

    def test_append_still_checks_work_attempt_quality_watermark_and_raw_metadata(self):
        self.add_plan()
        f = self.fixture
        mutations = (
            ("capture_work_items", f.work_id, "envelope_json='{}'"),
            ("scheduler_run_attempts", f.attempt_id, "details_json='{}'"),
            ("data_quality_receipts", f.quality_id, "payload_json='{}'"),
            ("capture_watermarks", f.watermark_id, "evidence_json='{}'"),
            ("provider_raw_responses", f.raw_id, "sha256='" + "0"*64 + "'"),
        )
        for table, identity, changed in mutations:
            with self.subTest(table=table):
                self.c.execute("SAVEPOINT damaged_lineage")
                try:
                    self.drop_triggers(table)
                    self.c.execute(f"UPDATE {table} SET {changed} WHERE id=?", (identity,))
                    self.assert_unknown("profile_day_receipt_invalid")
                finally:
                    self.c.execute("ROLLBACK TO damaged_lineage"); self.c.execute("RELEASE damaged_lineage")

    def rewrite_bridge(self, details):
        for table in ("scheduler_runs", "scheduler_run_attempts", "profile_day_coverage_receipts"):
            self.drop_triggers(table)
        details.pop("self_sha256", None)
        details = receipts._self_hashed(details)
        encoded = auth.canonical(details)
        self.c.execute("UPDATE scheduler_runs SET details_json=? WHERE id=?", (encoded, details["run_id"]))
        self.c.execute("UPDATE scheduler_run_attempts SET details_json=? WHERE id=?", (encoded, details["attempt_id"]))
        self.c.execute("UPDATE profile_day_coverage_receipts SET scope_json=?,summary_json=?,receipt_sha256=? WHERE source_bridge_run_id=?",
            (auth.canonical(details["scope"]), auth.canonical(details["summary"]), details["self_sha256"], details["run_id"]))

    def test_append_continues_final_native_source_revision_and_coverage_hash_checks(self):
        self.add_plan()
        for field in ("source_revision", "coverage_sha256"):
            with self.subTest(field=field):
                self.c.execute("SAVEPOINT damaged_seal")
                try:
                    details = copy.deepcopy(self.sealed)
                    details["scope"][field] = "0"*64
                    self.rewrite_bridge(details)
                    native = self.c.execute("SELECT * FROM profile_day_coverage_receipts WHERE source_bridge_run_id=?", (details["run_id"],)).fetchone()
                    # Intrinsic hash/native bridge checks pass; this must reach
                    # and reject the final check after catalog append handling.
                    with self.assertRaisesRegex(receipts.RuntimeReceiptError, "native profile-day source binding mismatch"):
                        receipts._validate_native_day_row(self.c, native, allow_appended_inputs=True)
                    self.assert_unknown("profile_day_receipt_invalid")
                finally:
                    self.c.execute("ROLLBACK TO damaged_seal"); self.c.execute("RELEASE damaged_seal")

    def test_append_cannot_hide_native_or_self_hash_corruption(self):
        self.add_plan()
        for table, change, identity in (
            ("profile_day_coverage_receipts", "receipt_sha256='"+"0"*64+"'", self.c.execute("SELECT id FROM profile_day_coverage_receipts WHERE source_bridge_run_id=?", (self.sealed["run_id"],)).fetchone()[0]),
            ("scheduler_runs", "details_json=json_set(details_json,'$.self_sha256','"+"0"*64+"')", self.sealed["run_id"]),
        ):
            with self.subTest(table=table):
                self.c.execute("SAVEPOINT broken_bridge")
                try:
                    self.drop_triggers(table)
                    self.c.execute(f"UPDATE {table} SET {change} WHERE id=?", (identity,))
                    self.assert_unknown("profile_day_receipt_invalid")
                finally:
                    self.c.execute("ROLLBACK TO broken_bridge"); self.c.execute("RELEASE broken_bridge")


if __name__ == "__main__":
    unittest.main()
