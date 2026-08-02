from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler

from v8.capture import ProviderResult
from v8.operations import upsert_account
from v8.scheduler import JOBS, execute_job, install_jobs, latest_occurrence, startup_catchup
from v8.storage import PROJECT_ROOT, connect, initialize_database, now_utc


SHANGHAI = ZoneInfo("Asia/Shanghai")


class V8SchedulerTest(unittest.TestCase):
    def setUp(self) -> None:
        (PROJECT_ROOT / "tmp").mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=PROJECT_ROOT / "tmp")
        self.root = Path(self.temp.name)
        self.db = self.root / "scheduler.sqlite3"
        self.reports = self.root / "reports"
        with connect(self.db) as connection:
            initialize_database(connection)
            captured_at = now_utc()
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id, version, status, definition, created_at, published_at
                ) VALUES ('taxonomy', 'selling-points-v5.0', 'published', 'test', ?, ?)
                """,
                (captured_at, captured_at),
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_four_fixed_jobs_are_registered_with_the_expected_times(self) -> None:
        scheduler = BackgroundScheduler(timezone=SHANGHAI)
        install_jobs(scheduler, db_path=self.db, reports_root=self.reports)
        jobs = {job.id: str(job.trigger) for job in scheduler.get_jobs()}
        self.assertEqual(set(jobs), {job.job_id for job in JOBS})
        self.assertIn("hour='2', minute='0'", jobs["daily_capture"])
        self.assertIn("hour='2', minute='30'", jobs["daily_evaluation"])
        self.assertIn("hour='8', minute='0'", jobs["daily_report"])
        self.assertIn("day_of_week='mon'", jobs["weekly_report"])

    def test_scheduler_run_is_idempotent_and_daily_report_targets_yesterday(self) -> None:
        occurrence = datetime(2026, 8, 2, 8, 0, tzinfo=SHANGHAI)
        first = execute_job(
            "daily_report", occurrence, db_path=self.db, reports_root=self.reports
        )
        second = execute_job(
            "daily_report", occurrence, db_path=self.db, reports_root=self.reports
        )
        self.assertEqual(first["status"], "succeeded")
        self.assertEqual(second["status"], "skipped_duplicate")
        with connect(self.db) as connection:
            task = connection.execute("SELECT * FROM report_tasks").fetchone()
            run_count = connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0]
        self.assertEqual(task["period_start"], "2026-08-01")
        self.assertEqual(task["period_end"], "2026-08-01")
        self.assertEqual(run_count, 1)

    def test_first_startup_runs_latest_due_slot_and_records_honest_capture_skip(self) -> None:
        now = datetime(2026, 8, 2, 9, 0, tzinfo=SHANGHAI)
        results = startup_catchup(now=now, db_path=self.db, reports_root=self.reports)
        self.assertEqual({item["job_id"] for item in results}, {job.job_id for job in JOBS})
        capture = next(item for item in results if item["job_id"] == "daily_capture")
        self.assertEqual(capture["status"], "skipped")
        self.assertIn("没有已启用", capture["details"]["reason"])
        weekly = next(item for item in results if item["job_id"] == "weekly_report")
        self.assertEqual(weekly["status"], "succeeded")
        with connect(self.db) as connection:
            statuses = {
                row["job_id"]: row["status"]
                for row in connection.execute("SELECT job_id, status FROM scheduler_runs")
            }
        self.assertEqual(statuses["daily_capture"], "skipped")
        self.assertEqual(statuses["daily_evaluation"], "succeeded")

    def test_latest_weekly_occurrence_is_monday_0830(self) -> None:
        weekly = next(job for job in JOBS if job.job_id == "weekly_report")
        occurrence = latest_occurrence(
            weekly, datetime(2026, 8, 2, 9, 0, tzinfo=SHANGHAI)
        )
        self.assertEqual(occurrence.isoformat(), "2026-07-27T08:30:00+08:00")

    def test_daily_capture_discovers_then_updates_content_with_real_slot_semantics(self) -> None:
        upsert_account(
            {
                "phone": "13800138000", "operator_name": "运营甲",
                "platforms": [{"platform": "douyin", "uid": "99887766", "nickname": "汽车号"}],
            },
            db_path=self.db,
        )

        def supplier_call(operation, record):
            if operation == "resolve_account":
                data = {"reference": "MS4w.test"}
            elif operation == "discover_content":
                data = {"items": [{
                    "platform": "douyin", "platform_content_id": "987654321",
                    "canonical_url": "https://www.douyin.com/video/987654321",
                    "title": "新车内容", "body": "新车内容完整正文",
                    "published_at": "2026-08-01T01:00:00Z", "content_type": "video",
                }]}
            elif operation == "detail":
                data = {
                    "title": "新车内容详情", "body": "新车内容详情完整正文",
                    "published_at": "2026-08-01T01:00:00Z", "account_uid": "99887766",
                    "account_name": "汽车号", "content_type": "video",
                }
            elif operation == "metrics":
                data = {"view_count": 100, "comment_count": 2, "like_count": 10, "share_count": 1, "collect_count": None}
            else:
                data = {"comment_count": 0, "comments": []}
            return ProviderResult(data, {"operation": operation, "data": data}, 200, True)

        result = execute_job(
            "daily_capture", datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db, reports_root=self.reports, capture_call_override=supplier_call,
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["details"]["monitored_accounts"], 1)
        self.assertEqual(result["details"]["monitored_contents"], 1)
        self.assertEqual(result["details"]["provider_cost"], 0.005)
        with connect(self.db) as connection:
            content = connection.execute(
                "SELECT * FROM content_items WHERE platform_content_id='987654321'"
            ).fetchone()
            snapshot = connection.execute("SELECT * FROM content_metric_snapshots").fetchone()
            slots = connection.execute("SELECT status FROM fetch_slots").fetchall()
        self.assertEqual(content["title"], "新车内容详情")
        self.assertEqual(snapshot["view_count"], 100)
        self.assertTrue(all(row["status"] == "succeeded" for row in slots))


if __name__ == "__main__":
    unittest.main()
