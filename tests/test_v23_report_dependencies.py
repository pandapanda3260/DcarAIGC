"""Read genuine frozen report dependencies across the schema23 migration."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from tests import test_v8_pipeline_cutover_receipts as fixture
from tests.v9_report_fixture import activate_v9_report_fixture
from v8 import pipeline_cutover, report_inputs, reports, scan_receipts
from v8.contracts import CURRENT_REPORT_VERSION, FOUR_PLATFORM_REPORT_VERSION
from v8.metric_source_policy import CURRENT_METRIC_POLICY, OPERATION_FIELD_POLICY_VERSION
from v8.source_routing import load_policy
from v8.storage import connect, initialize_database, transaction


class ReportDependenciesV23Test(unittest.TestCase):
    def setUp(self):
        self.fx = fixture.PipelineCutoverNativeReceiptsTest(methodName="runTest")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))
        activate_v9_report_fixture(self.fx.db, [])

    def migrate(self, version):
        with connect(self.fx.db) as db:
            initialize_database(db, target_version=version)

    def run_report(self, *, task_type="custom", task_id=None, at=fixture.NOW, period=fixture.BUSINESS_DAY):
        with ExitStack() as stack:
            for target in ("v8.reports.now_utc", "v8.report_inputs.now_utc", "v8.operations.now_utc"):
                stack.enter_context(patch(target, return_value=at))
            stack.enter_context(patch.object(reports, "PROJECT_ROOT", self.fx.root))
            stack.enter_context(patch.object(reports, "render_summary_png", return_value=False))
            if task_id is None:
                task = reports.create_and_run_task(task_type=task_type,
                    period_start=period, period_end=period,
                    creation_source="manual" if task_type == "custom" else "automatic",
                    db_path=self.fx.db, reports_root=self.fx.root / "reports")
            else:
                reports.retry_task(task_id, db_path=self.fx.db)
                reports.run_task(task_id, db_path=self.fx.db, reports_root=self.fx.root / "reports")
                task = reports.get_task(task_id, db_path=self.fx.db)
        self.assertIn(task["task_status"], {"succeeded", "partial"})
        return task

    def dependency(self, task_id, *, revision=None, at=fixture.NOW):
        with connect(self.fx.db) as db, patch.object(pipeline_cutover, "load_policy",
                side_effect=AssertionError("must read the frozen policy")), patch.object(scan_receipts,
                "verify_scan", side_effect=AssertionError("compact receipts must not re-read scan raw")):
            return pipeline_cutover.report_dependency(db, task_id, at=at,
                revision=revision, project_root=self.fx.root)

    def test_real_schema23_report_dependency_accepts_frozen_v810_and_v4(self):
        self.migrate(23)
        task = self.run_report()
        dependency = self.dependency(task["id"])
        with connect(self.fx.db) as db:
            inputs = report_inputs.load_event(db, task["id"], report_inputs.INPUT_EVENT)
            scope = report_inputs.load_event(db, task["id"], report_inputs.SCOPE_EVENT)
        self.assertEqual(inputs["payload"]["report_version"], FOUR_PLATFORM_REPORT_VERSION)
        self.assertEqual(scope["payload"]["report_version"], FOUR_PLATFORM_REPORT_VERSION)
        self.assertEqual(inputs["payload"]["input_references"]["source_policy"]["policy_version"], CURRENT_METRIC_POLICY)
        self.assertEqual(dependency["input_sha256"], inputs["sha256"])
        self.assertEqual(dependency["raw_files"], [])

    def test_schema19_scope_and_both_old_revisions_keep_v89_after_migration(self):
        task = self.run_report()
        original = self.dependency(task["id"])
        with connect(self.fx.db) as db:
            frozen_scope = report_inputs.load_event(db, task["id"], report_inputs.SCOPE_EVENT)
            frozen_input = report_inputs.load_event(db, task["id"], report_inputs.INPUT_EVENT)
        self.assertNotIn("report_version", frozen_scope["payload"])
        self.assertEqual(frozen_input["payload"]["report_version"], CURRENT_REPORT_VERSION)
        self.migrate(23)
        self.assertEqual(self.dependency(task["id"]), original)
        later = "2026-09-01T02:00:00Z"
        self.run_report(task_id=task["id"], at=later)
        for revision in (1, 2):
            dependency = self.dependency(task["id"], revision=revision, at=later)
            self.assertEqual(dependency["revision"], revision)
            self.assertEqual(dependency["input_sha256"], original["input_sha256"])
            self.assertEqual(dependency["scope_sha256"], original["scope_sha256"])
        with connect(self.fx.db) as db:
            self.assertEqual(report_inputs.load_event(db, task["id"], report_inputs.SCOPE_EVENT), frozen_scope)
            self.assertEqual(report_inputs.load_event(db, task["id"], report_inputs.INPUT_EVENT), frozen_input)
            self.assertEqual({row[0] for row in db.execute("SELECT contract_version FROM report_revisions WHERE task_id=?", (task["id"],))}, {CURRENT_REPORT_VERSION})

    def test_schema21_daily_operation_v3_report_keeps_frozen_dependency_on_23(self):
        self.migrate(21)
        at = "2026-09-12T01:00:00Z"
        task = self.run_report(task_type="daily", at=at, period="2026-09-11")
        original = self.dependency(task["id"], at=at)
        with connect(self.fx.db) as db:
            frozen = report_inputs.load_event(db, task["id"], report_inputs.INPUT_EVENT)
        self.assertEqual(frozen["payload"]["input_references"]["source_policy"]["policy_version"], OPERATION_FIELD_POLICY_VERSION)
        self.migrate(23)
        self.assertEqual(self.dependency(task["id"], at=at), original)

    def historical_classification_task(self):
        store = report_inputs._store
        def historical_inputs(connection, task_id, kind, payload):
            if kind == report_inputs.INPUT_EVENT:
                payload = copy.deepcopy(payload)
                payload["metadata"].pop("account_classification_version", None)
                payload["account_type_dimensions"] = payload.pop("account_group_dimensions")
                payload.pop("business_direction_dimensions")
                for row in payload["content_details"]:
                    row.pop("account_group", None)
                    row.pop("business_direction", None)
                    row["account_type"] = "original"
            return store(connection, task_id, kind, payload)
        with patch.object(report_inputs, "_store", side_effect=historical_inputs):
            return self.run_report()

    def test_historical_taxonomy_retry_has_verified_projection_and_unchanged_input_dependency(self):
        task = self.historical_classification_task()
        original = self.dependency(task["id"])
        with connect(self.fx.db) as db:
            frozen = report_inputs.load_event(db, task["id"], report_inputs.INPUT_EVENT)
        self.assertIn("account_type_dimensions", frozen["payload"])
        later = "2026-09-01T02:00:00Z"
        self.run_report(task_id=task["id"], at=later)
        retry = self.dependency(task["id"], revision=2, at=later)
        self.assertEqual(retry["input_sha256"], original["input_sha256"])
        self.assertEqual(retry["scope_sha256"], original["scope_sha256"])
        projected = json.loads(Path(retry["report_file"]["path"]).read_bytes())
        self.assertNotIn("account_type_dimensions", projected)
        self.assertNotIn("frozen_inputs", projected)
        self.assertEqual(projected["source_frozen_inputs"]["sha256"], frozen["sha256"])
        with connect(self.fx.db) as db:
            self.assertEqual(report_inputs.load_event(db, task["id"], report_inputs.INPUT_EVENT), frozen)

    def test_rehashed_projection_cannot_change_frozen_values_or_source_event(self):
        task = self.historical_classification_task()
        dependency = self.dependency(task["id"])
        path = Path(dependency["report_file"]["path"])
        original = json.loads(path.read_bytes())
        variants = []
        changed = copy.deepcopy(original)
        changed["summary_metrics"]["publication_count"]["value"] = 999
        variants.append(changed)
        changed = copy.deepcopy(original)
        changed["account_group_dimensions"] = [{"key": "innovation", "count": 999, "percentage": 100}]
        variants.append(changed)
        changed = copy.deepcopy(original)
        changed["source_frozen_inputs"]["event_id"] += 1
        variants.append(changed)
        for changed in variants:
            with self.subTest(changed=changed != original):
                body = json.dumps(changed, ensure_ascii=False).encode()
                checksum = hashlib.sha256(body).hexdigest()
                path.write_bytes(body)
                with connect(self.fx.db) as db, transaction(db):
                    db.execute("UPDATE report_revisions SET report_sha256=? WHERE task_id=? AND revision=1", (checksum, task["id"]))
                    db.execute("UPDATE report_files SET sha256=?,byte_size=? WHERE task_id=? AND revision=1 AND file_kind='report-json'",
                               (checksum, len(body), task["id"]))
                with self.assertRaisesRegex(pipeline_cutover.PublicationEvidenceError, "classification_projection_invalid"):
                    self.dependency(task["id"])

    def test_revision_or_explicit_scope_version_cannot_relabel_frozen_report(self):
        self.migrate(23)
        task = self.run_report()
        with connect(self.fx.db) as db, transaction(db):
            db.execute("UPDATE report_revisions SET contract_version=? WHERE task_id=?", (CURRENT_REPORT_VERSION, task["id"]))
        with self.assertRaisesRegex(pipeline_cutover.PublicationEvidenceError, "publication_report_contract_mismatch"):
            self.dependency(task["id"])
        with connect(self.fx.db) as db, transaction(db):
            db.execute("UPDATE report_revisions SET contract_version=? WHERE task_id=?", (FOUR_PLATFORM_REPORT_VERSION, task["id"]))
            scope = report_inputs.load_event(db, task["id"], report_inputs.SCOPE_EVENT)
            event = {key: value for key, value in scope.items() if key != "event_id"}
            del event["payload"]["report_version"]
            event["sha256"] = report_inputs.digest(event["payload"])
            db.execute("UPDATE task_events SET payload_json=? WHERE id=?", (json.dumps(event), scope["event_id"]))
        with self.assertRaisesRegex(pipeline_cutover.PublicationEvidenceError, "publication_report_contract_mismatch"):
            self.dependency(task["id"])

    def test_frozen_operation_policies_require_exact_immutable_files(self):
        for version in (OPERATION_FIELD_POLICY_VERSION, CURRENT_METRIC_POLICY):
            with self.subTest(version=version):
                value = load_policy(policy_version=version)
                pipeline_cutover.verify_frozen_source_policy(value, pipeline_cutover.digest(value))
                changed = copy.deepcopy(value)
                changed["fixture_changed_rule"] = True
                with self.assertRaisesRegex(pipeline_cutover.PublicationEvidenceError, "source_policy_invalid"):
                    pipeline_cutover.verify_frozen_source_policy(changed, pipeline_cutover.digest(changed))


if __name__ == "__main__":
    unittest.main()
