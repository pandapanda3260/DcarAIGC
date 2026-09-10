from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException
from pydantic import ValidationError

from v8.account_classification import classification_for_account, update_classification_in_transaction
from v8.account_directory import directory_account_items, import_account_directory
from v8.api import AccountMutationRequest, AccountSearchRequest, ContentMutationRequest, ContentPatchRequest, ApiConfig, _account_search, patch_v8_account
from v8.operations import upsert_account
from v8.schema_v21 import migrate, migration_proof, validate_lineage
from v8.storage import connect, initialize_database, require_schema_compatibility, transaction


class AccountClassificationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "candidate.sqlite3"
        self.connection = connect(self.db)
        self.addCleanup(self.connection.close)
        initialize_database(self.connection, target_version=20)
        self.account_id = upsert_account({"platforms": [{"platform": "douyin", "uid": "123456789"}]}, db_path=self.db)["id"]
        self.connection.execute("UPDATE accounts SET account_type='original',content_direction='new_car' WHERE id=?", (self.account_id,))
        self.connection.commit()
        self.raw = {"平台": "抖音", "UID": "123456789", "昵称": "测试", "更新状态": "日更",
                    "质量标签": "图文号", "业务标签": "二手车C2"}
        payload = {"sha256": "a" * 64, "source": "reviewed.xlsx", "sheet": "accounts", "records": [
            {"sourceRow": 2, "raw": self.raw},
            {"sourceRow": 3, "raw": {**self.raw, "UID": "", "质量标签": "创新号"}},
            {"sourceRow": 4, "raw": {**self.raw, "UID": "987654321", "质量标签": "混剪号"}},
        ]}
        with transaction(self.connection):
            result = import_account_directory(self.connection, payload, imported_at="2026-09-08T00:00:00Z")
        self.negative_id = -result["rows"][1]["directory_row_id"]
        self.unverified_id = result["rows"][2]["account_id"]
        # Emulate the pre-migration directory rather than hiding a migration gap.
        self.connection.execute("ALTER TABLE account_directory_rows DROP COLUMN account_group")
        self.connection.execute("ALTER TABLE account_directory_rows DROP COLUMN business_direction")
        self.connection.commit()
        self.backup = self.root / "source.sqlite3"
        with connect(self.backup) as source:
            self.connection.backup(source)
        self.receipt = migrate(self.connection)
        config = ApiConfig(db_path=self.db, reports_root=self.root / "reports", legacy_db_path=self.root / "legacy.sqlite3",
                           operator_freeze_lock=self.root / "freeze.lock", scheduler_enabled=False, startup_catchup_enabled=False, project_root=self.root)
        self.request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(config=config)))

    def business_state(self):
        tables = ("accounts", "account_platform_identities", "account_roster_snapshots", "account_roster_members",
                  "account_state_events", "scheduler_runs", "scheduler_run_attempts", "acquisition_profile_activations",
                  "pipeline_paid_drain_events", "paid_provider_dispatch_events", "content_items", "evaluation_versions")
        return {table: [tuple(row) for row in self.connection.execute(f"SELECT * FROM {table}")] for table in tables}

    def test_migration_removes_only_account_columns_and_seals_portable_proof(self):
        columns = {row[1] for row in self.connection.execute("PRAGMA table_info(accounts)")}
        self.assertFalse({"account_type", "content_direction"} & columns)
        self.assertIn("evaluation_content_direction", {row[1] for row in self.connection.execute("PRAGMA table_info(content_items)")})
        self.assertEqual(classification_for_account(self.connection, self.account_id),
                         {"account_group": "image_text", "business_direction": "used_car_c2"})
        self.assertEqual(require_schema_compatibility(self.connection), 21)
        with connect(self.backup) as source:
            self.assertTrue(validate_lineage(source, self.connection)["retained_tables_verified"])
        self.assertEqual(migration_proof(self.connection)["migrated_directory_rows"], 3)
        self.assertEqual(json.loads(self.connection.execute("SELECT raw_json FROM account_directory_rows WHERE account_id=?", (self.account_id,)).fetchone()[0]), self.raw)

    def test_classification_patch_linked_unlinked_and_unverified_never_changes_capture(self):
        before = self.business_state()
        with patch("v8.api.update_account_operating_status_in_transaction", side_effect=AssertionError("status flow called")), \
             patch("v8.api._schedule_writer_roster_activation", side_effect=AssertionError("capture flow called")):
            for target in (self.account_id, self.negative_id, self.unverified_id):
                result = patch_v8_account(self.request, target, AccountMutationRequest(account_group="innovation", business_direction="ai_xiaodong"))
                self.assertEqual(result["account_group"], "innovation")
        self.assertEqual(self.business_state(), before)
        self.assertEqual(migration_proof(self.connection)["migrated_directory_rows"], 3)
        values = directory_account_items(self.connection, roster={}, update_frequencies={}, admission_members={},
                                         account_group="innovation", business_direction="ai_xiaodong")
        self.assertEqual({value["id"] for value in values}, {self.account_id, self.negative_id, self.unverified_id})
        self.assertTrue(all("account_type" not in value and "directory_quality_label" not in value for value in values))

    def test_unsupported_enum_and_obsolete_api_fields_are_rejected(self):
        for body in ({"account_group": "original"}, {"business_direction": "used_car"},
                     {"account_type": "original"}, {"content_direction": "new_car"}):
            with self.assertRaises(ValidationError):
                AccountMutationRequest.model_validate(body)
        with self.assertRaises(ValidationError):
            AccountSearchRequest.model_validate({"account_type": "original"})
        with self.assertRaises(ValidationError):
            ContentPatchRequest.model_validate({"account_type": "original"})
        with self.assertRaises(ValidationError):
            ContentMutationRequest.model_validate({"platform": "douyin", "canonical_url": "https://example.test/v", "account_type": "original"})
        before = self.business_state()
        with self.assertRaises(HTTPException):
            patch_v8_account(self.request, -999, AccountMutationRequest(account_group="innovation"))
        self.assertEqual(self.business_state(), before)

    def test_migration_failure_rolls_back_and_preserves_source(self):
        with connect(self.backup) as source:
            source.execute("UPDATE account_directory_rows SET raw_json=? WHERE account_id=?",
                           (json.dumps({**self.raw, "质量标签": "unapproved"}), self.account_id))
            source.commit()
            with self.assertRaises(ValueError):
                migrate(source)
            self.assertEqual(source.execute("PRAGMA user_version").fetchone()[0], 20)
            self.assertIn("account_type", {row[1] for row in source.execute("PRAGMA table_info(accounts)")})
            self.assertNotIn("account_group", {row[1] for row in source.execute("PRAGMA table_info(account_directory_rows)")})

    def test_portable_proof_detects_unrelated_schema_drift(self):
        self.connection.execute("CREATE TABLE unexpected_schema_drift(id INTEGER)")
        with self.assertRaisesRegex(ValueError, "objects differ"):
            migration_proof(self.connection)

    def test_replayed_directory_status_does_not_restore_older_classification(self):
        body = AccountMutationRequest(account_status="weekly", status_request_id="directory-weekly-classification",
                                      account_group="innovation")
        first = patch_v8_account(self.request, self.negative_id, body)
        self.assertEqual(first["account_status"], "weekly")
        patch_v8_account(self.request, self.negative_id, AccountMutationRequest(account_group="image_text"))
        replay = patch_v8_account(self.request, self.negative_id, body)
        self.assertTrue(replay["status_replayed"])
        row = self.connection.execute("SELECT account_group FROM account_directory_rows WHERE id=?", (-self.negative_id,)).fetchone()
        self.assertEqual(row[0], "image_text")


if __name__ == "__main__":
    unittest.main()
