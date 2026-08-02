from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from v8.evaluation import evaluate_content
from v8.reports import (
    ReportTaskError,
    create_task,
    get_task,
    request_task_cancel,
    retry_task,
    resume_task,
    run_task,
)
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
        self.assertEqual(report["summary_metrics"]["duplicate_rate"]["status"], "below_threshold")
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

    def test_succeeded_task_can_queue_a_new_immutable_revision(self) -> None:
        task = create_task(
            task_type="custom", period_start="2026-07-01", period_end="2026-07-01",
            creation_source="manual", db_path=self.db,
        )
        first = run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        queued = retry_task(task["id"], db_path=self.db)
        self.assertEqual(queued["task_status"], "queued")
        second = run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        self.assertEqual((first["metadata"]["revision"], second["metadata"]["revision"]), (1, 2))
        state = get_task(task["id"], db_path=self.db)
        self.assertEqual(len(state["revisions"]), 2)
        self.assertIn("retry_requested", [event["event_type"] for event in state["events"]])
    def test_cancelled_task_can_resume_and_create_a_new_revision(self) -> None:
        task = create_task(
            task_type="custom", period_start="2026-07-01", period_end="2026-07-01",
            creation_source="manual", db_path=self.db,
        )
        cancelled = request_task_cancel(task["id"], db_path=self.db)
        self.assertEqual(cancelled["task_status"], "cancelled")
        with self.assertRaisesRegex(ReportTaskError, "not runnable"):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        resumed = resume_task(task["id"], db_path=self.db)
        self.assertEqual(resumed["task_status"], "queued")
        run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        completed = get_task(task["id"], db_path=self.db)
        self.assertIn(completed["task_status"], {"succeeded", "partial"})
        self.assertEqual(len(completed["revisions"]), 1)
        self.assertEqual(
            [event["event_type"] for event in completed["events"]][1:3],
            ["cancelled", "resumed"],
        )
    def test_pending_gray_review_blocks_first_report_without_creating_revision(self) -> None:
        with connect(self.db) as connection:
            evaluation_id = connection.execute(
                "SELECT id FROM evaluation_versions WHERE content_id=1 ORDER BY id DESC"
            ).fetchone()[0]
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id, evaluation_id, reason_code, status, created_at, updated_at
                ) VALUES (1, ?, 'evaluation_gray_zone', 'pending', ?, ?)
                """,
                (evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
        task = create_task(
            task_type="custom", period_start="2026-07-01", period_end="2026-07-01",
            creation_source="manual", db_path=self.db,
        )
        with self.assertRaisesRegex(ReportTaskError, "1 条灰区内容"):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        state = get_task(task["id"], db_path=self.db)
        self.assertEqual(state["task_status"], "failed")
        self.assertEqual(state["revisions"], [])
        self.assertEqual(state["events"][-1]["event_type"], "review_gate_blocked")

    def test_pending_gray_review_is_reported_but_does_not_block_after_first_release(self) -> None:
        first = create_task(
            task_type="custom", period_start="2026-07-01", period_end="2026-07-01",
            creation_source="manual", db_path=self.db,
        )
        run_task(first["id"], db_path=self.db, reports_root=self.reports_root)
        with connect(self.db) as connection:
            evaluation_id = connection.execute(
                "SELECT id FROM evaluation_versions WHERE content_id=1 ORDER BY id DESC"
            ).fetchone()[0]
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id, evaluation_id, reason_code, status, created_at, updated_at
                ) VALUES (1, ?, 'evaluation_gray_zone', 'pending', ?, ?)
                """,
                (evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
        later = create_task(
            task_type="custom", period_start="2026-07-01", period_end="2026-07-02",
            creation_source="manual", db_path=self.db,
        )
        report = run_task(later["id"], db_path=self.db, reports_root=self.reports_root)
        self.assertEqual(report["metadata"]["revision"], 1)
        self.assertEqual(report["review_summary"][0]["status"], "pending")

    def test_schema_upgrade_marks_pre_gate_v8_report_audit_only(self) -> None:
        task = create_task(
            task_type="custom", period_start="2026-07-01", period_end="2026-07-01",
            creation_source="manual", db_path=self.db,
        )
        run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        released_status = get_task(task["id"], db_path=self.db)["task_status"]
        with connect(self.db) as connection:
            evaluation_id = connection.execute(
                "SELECT id FROM evaluation_versions WHERE content_id=1 ORDER BY id DESC"
            ).fetchone()[0]
            connection.execute(
                "UPDATE report_revisions SET contract_version='dcar-content-operations-report-v8.0'"
            )
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id, evaluation_id, reason_code, status, created_at, updated_at
                ) VALUES (1, ?, 'evaluation_gray_zone', 'pending', ?, ?)
                """,
                (evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
            initialize_database(connection)
            revision = connection.execute("SELECT * FROM report_revisions").fetchone()
            task_row = connection.execute(
                "SELECT * FROM report_tasks WHERE id=?", (task["id"],)
            ).fetchone()
        self.assertIsNotNone(revision["invalidated_at"])
        self.assertEqual(
            revision["invalidation_reason"],
            "released_before_gray_review_gate_cleared",
        )
        self.assertEqual(task_row["task_status"], "failed")

        with connect(self.db) as connection:
            connection.execute(
                "UPDATE review_queue SET status='resolved', resolved_at=?, updated_at=?",
                (now_utc(), now_utc()),
            )
            connection.commit()
        run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE review_queue SET status='pending', resolved_at=NULL, updated_at=?",
                (now_utc(),),
            )
            connection.commit()
            initialize_database(connection)
            current_task = connection.execute(
                "SELECT * FROM report_tasks WHERE id=?", (task["id"],)
            ).fetchone()
        self.assertEqual(current_task["task_status"], released_status)


if __name__ == "__main__":
    unittest.main()
