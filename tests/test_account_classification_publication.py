"""Classification publications use real schema/rows and no external services."""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from v8.account_classification import update_classification_in_transaction
from v8.account_cleanup_snapshot import CONTRACT as CLEANUP_CONTRACT
from v8.account_directory import import_account_directory
from v8.schema_v21 import migrate
from v8.snapshot_contract import descriptor
from v8.storage import connect, initialize_database, transaction


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("classification_publication_under_test", ROOT / "deploy/macos/publish_snapshot.py")
publisher = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = publisher
SPEC.loader.exec_module(publisher)
NOW = datetime(2026, 9, 8, 12, tzinfo=ZoneInfo("Asia/Shanghai"))
AT = NOW.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class AccountClassificationPublicationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.db = self.root / "source.sqlite3"
        self.connection = connect(self.db)
        self.addCleanup(self.connection.close)
        initialize_database(self.connection, target_version=20)
        raw = {"平台": "抖音", "更新状态": "日更", "质量标签": "图文号", "业务标签": "二手车C2"}
        with transaction(self.connection):
            imported = import_account_directory(self.connection,
                {"sha256": "a" * 64, "source": "test.xlsx", "sheet": "accounts", "records": [
                    {"sourceRow": 2, "raw": {**raw, "UID": "123456789"}},
                    {"sourceRow": 3, "raw": {**raw, "UID": ""}},
                ]}, imported_at=AT)
        self.linked_id = imported["rows"][0]["account_id"]
        self.unlinked_id = -imported["rows"][1]["directory_row_id"]
        self.old_evidence = self.observe()
        migrate(self.connection)
        self.active = {"activation_id": 1, "profile_id": "integrated_route_v1", "roster_snapshot_id": 1,
                       "roster_members_sha256": "b" * 64, "activation_sha256": "c" * 64, "effective_at": AT}
        self.deployment = {"contract_version": CLEANUP_CONTRACT, "deployment_id": "classification-test",
                           "status": "released", "receipt_sha256": "d" * 64, "coverage_complete": False,
                           "bindings": self.active}
        self.enterContext(patch.object(publisher, "_schema20_deployment", return_value=self.deployment))
        self.enterContext(patch("v8.profile_activations.activation_at", return_value=self.active))

    def observe(self, current=NOW):
        return publisher._observed_publication_evidence(self.connection, current=current,
            at=current.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            project_root=ROOT, boundary=NOW.date())

    def installed_evidence(self):
        return publisher._schema20_publication_evidence(self.connection, current=NOW, at=AT, project_root=ROOT)

    def update(self, target, **values):
        with transaction(self.connection):
            update_classification_in_transaction(self.connection, target, values)

    def test_schema21_identity_triggers_publication_without_changing_schema20_protocol(self):
        current = self.observe()
        self.assertNotIn("account_classification", self.old_evidence)
        self.assertEqual({key: value for key, value in current.items() if key != "account_classification"}, self.old_evidence)
        self.assertNotEqual(publisher._publication_fingerprint(current), publisher._publication_fingerprint(self.old_evidence))
        self.assertEqual(current["account_classification"]["row_count"], 2)
        self.assertEqual(current["account_classification"]["unlinked_row_count"], 1)
        self.assertEqual(publisher._validate_publication_evidence(self.old_evidence, expected_schema=20), self.old_evidence)

    def test_each_classification_edit_and_restore_changes_last_published_fingerprint(self):
        baseline = self.installed_evidence()
        for target, values in ((self.linked_id, {"account_group": "innovation"}),
                               (self.unlinked_id, {"business_direction": "ai_xiaodong"})):
            self.update(target, **values)
            changed = self.installed_evidence()
            self.assertNotEqual(publisher._publication_fingerprint(baseline), publisher._publication_fingerprint(changed))
            # A same-count classification update must be detected by row content.
            self.assertEqual(baseline["current_observation"], changed["current_observation"])
            self.update(target, account_group="image_text", business_direction="used_car_c2")
            restored = self.installed_evidence()
            self.assertNotEqual(publisher._publication_fingerprint(changed), publisher._publication_fingerprint(restored))
            self.assertEqual(publisher._publication_fingerprint(baseline), publisher._publication_fingerprint(restored))

    def test_observation_clocks_and_timestamp_only_changes_do_not_publish_again(self):
        baseline = self.observe()
        self.connection.execute("UPDATE account_directory_rows SET updated_at='2026-09-08T09:01:00Z'")
        self.connection.commit()
        current = self.observe(NOW + timedelta(minutes=5))
        self.assertEqual(publisher._publication_fingerprint(baseline), publisher._publication_fingerprint(current))

    def test_schema21_requires_a_well_formed_classification_digest(self):
        baseline = self.observe()
        publisher._validate_publication_evidence(baseline, expected_schema=21)
        missing = copy.deepcopy(baseline)
        missing.pop("account_classification")
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "missing account classification"):
            publisher._validate_publication_evidence(missing, expected_schema=21)
        for key, invalid in (("rows_sha256", "bad"), ("schema_version", 20), ("row_count", True), ("unlinked_row_count", 3)):
            changed = copy.deepcopy(baseline)
            changed["account_classification"][key] = invalid
            with self.assertRaises(publisher.SnapshotPublishError):
                publisher._validate_publication_evidence(changed, expected_schema=21)

    def test_detached_snapshot_recomputes_labels_and_rejects_stale_evidence(self):
        identity = {"database_schema_version": 21}
        self.enterContext(patch.object(publisher, "_database_runtime_identity", return_value=identity))
        output = self.root / "snapshot"
        target = output / "databases/dcar_insight.sqlite3"
        target.parent.mkdir(parents=True)

        def freeze():
            with sqlite3.connect(target) as destination:
                self.connection.backup(destination)
            return {"databases": [{"name": "dcar_insight.sqlite3", "bundle_path": "databases/dcar_insight.sqlite3",
                                    "sha256": hashlib.sha256(target.read_bytes()).hexdigest(), "byte_size": target.stat().st_size}],
                    "deployment_readiness": self.deployment, "files": []}

        evidence = self.installed_evidence()
        freshness = publisher.WriterFreshness(evidence=evidence, latest_published_at=None, content_count=0,
                                              runtime_identity=identity, snapshot_contract=descriptor())
        publisher._verify_snapshot_dependencies(output, freeze(), freshness, project_root=ROOT)
        self.update(self.unlinked_id, account_group="innovation")
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "current observations drifted"):
            publisher._verify_snapshot_dependencies(output, freeze(), freshness, project_root=ROOT)


if __name__ == "__main__":
    unittest.main()
