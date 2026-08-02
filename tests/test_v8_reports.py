from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from v8.evaluation import evaluate_content
from v8.reports import ReportTaskError, create_task, get_task, run_task
from v8.storage import PROJECT_ROOT, connect, initialize_database, now_utc


class V8ReportTaskTest(unittest.TestCase):
    def setUp(self) -> None:
        (PROJECT_ROOT / "tmp").mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=PROJECT_ROOT / "tmp")
        self.root = Path(self.temp.name)
        self.db = self.root / "reports.sqlite3"
        self.reports_root = self.root / "reports"
        captured_at = now_utc()
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id, version, status, definition, created_at, published_at
                ) VALUES ('taxonomy', 'selling-points-v5.0', 'published', 'test', ?, ?)
                """,
                (captured_at, captured_at),
            )
            point = connection.execute(
                """
                INSERT INTO selling_points(
                    taxonomy_id, code, tier, label, positive_evidence_json
                ) VALUES ('taxonomy', 'C1', 'core', '汽车服务', '["保养"]')
                """
            )
            connection.execute(
                "INSERT INTO selling_point_scenes(selling_point_id, scene) VALUES (?, 'media')",
                (point.lastrowid,),
            )
            content = connection.execute(
                """
                INSERT INTO content_items(
                    link_id, platform, platform_content_id, canonical_url,
                    published_at, title, body, content_type, imported_at, created_at, updated_at
                ) VALUES (
                    'A2BC3D', 'douyin', '1', 'https://www.douyin.com/video/1',
                    '2026-07-01T04:00:00Z', '汽车保养', '保养知识', 'video', ?, ?, ?
                )
                """,
                (captured_at, captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO content_items(
                    link_id, platform, platform_content_id, canonical_url,
                    published_at, title, content_type, imported_at, created_at, updated_at
                ) VALUES (
                    'E4FG5H', 'xiaohongshu', '2', 'https://www.xiaohongshu.com/explore/2',
                    NULL, '', 'normal', ?, ?, ?
                )
                """,
                (captured_at, captured_at, captured_at),
            )
            content_id = int(content.lastrowid)
            connection.execute(
                """
                INSERT INTO fetch_slots(
                    content_id, stage, window_key, provider, adapter_version,
                    status, attempt_count, created_at, updated_at
                ) VALUES (?, 'detail', 'lifetime', 'migration', 'v8', 'succeeded', 1, ?, ?)
                """,
                (content_id, captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO content_metric_snapshots(
                    content_id, captured_at, window_key, view_count, status, source
                ) VALUES (?, '2026-07-02T00:00:00Z', 'historical', 1234,
                          'available', 'migrated_historical')
                """,
                (content_id,),
            )
            connection.commit()
        evaluate_content(1, db_path=self.db)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_report_task_freezes_scope_and_appends_immutable_revisions(self) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        report = run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        self.assertEqual(report["task"]["task_status"], "partial")
        self.assertEqual(report["summary_metrics"]["publication_count"]["value"], 1)
        self.assertEqual(report["summary_metrics"]["view_count"]["status"], "stale")
        self.assertEqual(report["summary_metrics"]["comment_count"]["status"], "missing")
        self.assertEqual(report["summary_metrics"]["estimated_leads"]["status"], "not_calculable")
        self.assertEqual(report["summary_metrics"]["estimated_leads"]["unit"], "lead")
        self.assertEqual(report["scope"]["period_end"], "2026-07-02T00:00:00+08:00")
        state = get_task(task["id"], db_path=self.db)
        self.assertEqual(state["content_counts"], {"excluded_missing_boundary": 1, "included": 1})
        self.assertEqual(len(state["revisions"]), 1)
        first_path = PROJECT_ROOT / state["revisions"][0]["report_json_path"]
        first_bytes = first_path.read_bytes()
        first_value = json.loads(first_bytes)
        self.assertEqual(first_value["metadata"]["revision"], 1)
        self.assertTrue(any(item["file_kind"] == "summary-svg" for item in state["revisions"][0]["files"]))

        second = run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        self.assertEqual(second["metadata"]["revision"], 2)
        state = get_task(task["id"], db_path=self.db)
        self.assertEqual(len(state["revisions"]), 2)
        self.assertEqual(first_path.read_bytes(), first_bytes)

    def test_task_calendar_rules_are_strict(self) -> None:
        with self.assertRaisesRegex(ReportTaskError, "exactly one"):
            create_task(
                task_type="daily", period_start="2026-07-01", period_end="2026-07-02",
                creation_source="automatic", db_path=self.db,
            )
        with self.assertRaisesRegex(ReportTaskError, "Monday through Sunday"):
            create_task(
                task_type="weekly", period_start="2026-07-01", period_end="2026-07-07",
                creation_source="automatic", db_path=self.db,
            )


if __name__ == "__main__":
    unittest.main()
