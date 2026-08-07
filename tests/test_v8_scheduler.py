from __future__ import annotations

import tempfile
import threading
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler

from v8.capture import CaptureError, ProviderResult
from v8.matcher_dsl import POINT_IDS, POINT_SCENES
from v8.operations import IdentityConflictError, upsert_account
from v8.reports import ReportTaskError
from v8.scheduler import (
    DAILY_CAPTURE_CONTENT_LIMIT,
    DAILY_CAPTURE_MAX_AMOUNT,
    JOBS,
    _select_due_capture_contents,
    execute_job,
    install_jobs,
    latest_occurrence,
    run_due_capture,
    run_media_cutoff,
    startup_catchup,
)
from v8.storage import PROJECT_ROOT, connect, initialize_database, now_utc
from v8.taxonomy_rule_backfill import backfill_v5_1_matcher_rules


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
            for code in sorted(POINT_IDS):
                point = connection.execute(
                    """
                    INSERT INTO selling_points(
                        taxonomy_id,code,tier,label,definition,matcher_rule_json
                    ) VALUES ('taxonomy',?,'other',?,?,'{}')
                    """,
                    (code, f"卖点 {code}", f"定义 {code}"),
                )
                for scene in sorted(POINT_SCENES[code]):
                    connection.execute(
                        """
                        INSERT INTO selling_point_scenes(selling_point_id,scene)
                        VALUES (?,?)
                        """,
                        (point.lastrowid, scene),
                    )
            connection.commit()
        matcher = backfill_v5_1_matcher_rules(db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE taxonomy_versions SET status='retired'
                WHERE version='selling-points-v5.0'
                """,
            )
            connection.execute(
                """
                UPDATE taxonomy_versions SET status='published',published_at=?
                WHERE version='selling-points-v5.1'
                """,
                (captured_at,),
            )
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at,activated_at
                ) VALUES ('evaluation-v8__selling-points-v5.1','evaluation-v8',
                          'selling-points-v5.1',?,'active',?,?,?)
                """,
                (
                    matcher["matcher_rule_sha256"],
                    captured_at,
                    captured_at,
                    captured_at,
                ),
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _insert_scheduled_content(
        self,
        *,
        account_id: int,
        link_id: str,
        platform_content_id: str,
        published_at: str,
    ) -> int:
        captured_at = now_utc()
        with connect(self.db) as connection:
            cursor = connection.execute(
                """
                INSERT INTO content_items(
                    link_id,platform,platform_content_id,canonical_url,account_id,
                    title,body,content_type,published_at,source_group,imported_at,
                    created_at,updated_at
                ) VALUES (?, 'douyin', ?, ?, ?, '排队测试', '排队测试正文',
                          'video', ?, '', ?, ?, ?)
                """,
                (
                    link_id,
                    platform_content_id,
                    f"https://www.douyin.com/video/{platform_content_id}",
                    account_id,
                    published_at,
                    captured_at,
                    captured_at,
                    captured_at,
                ),
            )
            connection.commit()
        return int(cursor.lastrowid)

    def _insert_capture_slot(
        self,
        *,
        content_id: int,
        stage: str,
        window_key: str,
        status: str,
        updated_at: str,
        provider: str = "TikHub",
    ) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO fetch_slots(
                    content_id,stage,window_key,provider,adapter_version,status,
                    attempt_count,created_at,updated_at,finished_at
                ) VALUES (?, ?, ?, ?, 'scheduler-selection-test', ?, 1, ?, ?, ?)
                """,
                (
                    content_id,
                    stage,
                    window_key,
                    provider,
                    status,
                    updated_at,
                    updated_at,
                    updated_at,
                ),
            )
            connection.commit()

    def _insert_comment_run(
        self,
        *,
        content_id: int,
        window_key: str,
        status: str = "succeeded",
    ) -> None:
        captured_at = "2026-08-04T18:00:00Z"
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO comment_capture_runs(
                    content_id,window_key,provider,adapter_version,status,
                    completion_kind,created_at,updated_at,completed_at
                ) VALUES (?,?,'TikHub','tikhub-comments-v8.0+paged-comments-v2',
                          ?,?,?,?,?)
                """,
                (
                    content_id,
                    window_key,
                    status,
                    "provider_exhausted" if status == "succeeded" else None,
                    captured_at,
                    captured_at,
                    captured_at if status == "succeeded" else None,
                ),
            )
            connection.commit()

    def test_six_fixed_jobs_are_registered_with_the_expected_times(self) -> None:
        scheduler = BackgroundScheduler(timezone=SHANGHAI)
        install_jobs(scheduler, db_path=self.db, reports_root=self.reports)
        jobs = {job.id: str(job.trigger) for job in scheduler.get_jobs()}
        self.assertEqual(set(jobs), {job.job_id for job in JOBS})
        self.assertIn("hour='2', minute='0'", jobs["daily_capture"])
        self.assertIn("hour='2', minute='20'", jobs["daily_media_download"])
        self.assertIn("hour='3', minute='0'", jobs["daily_media_processing"])
        self.assertIn("hour='7', minute='30'", jobs["daily_media_cutoff"])
        self.assertIn("hour='8', minute='0'", jobs["daily_report"])
        self.assertIn("day_of_week='mon'", jobs["weekly_report"])

    def test_scheduler_run_is_idempotent_and_daily_report_targets_yesterday(
        self,
    ) -> None:
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
            run_count = connection.execute(
                "SELECT COUNT(*) FROM scheduler_runs"
            ).fetchone()[0]
        self.assertEqual(task["period_start"], "2026-08-01")
        self.assertEqual(task["period_end"], "2026-08-01")
        self.assertEqual(run_count, 1)

    def test_unready_report_is_deferred_without_claim_then_same_slot_can_run(
        self,
    ) -> None:
        occurrence = datetime(2026, 8, 2, 8, 0, tzinfo=SHANGHAI)
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE evaluation_releases SET status='retired',retired_at=?
                WHERE status='active'
                """,
                (now_utc(),),
            )
            connection.commit()
        deferred = execute_job(
            "daily_report", occurrence, db_path=self.db, reports_root=self.reports
        )
        self.assertEqual(
            (deferred["status"], deferred["reason"]),
            ("deferred", "report_runtime_not_ready"),
        )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM report_tasks").fetchone()[0],
                0,
            )
            connection.execute(
                """
                UPDATE evaluation_releases
                SET status='active',retired_at=NULL,activated_at=?,updated_at=?
                WHERE id='evaluation-v8__selling-points-v5.1'
                """,
                (now_utc(), now_utc()),
            )
            connection.commit()
        completed = execute_job(
            "daily_report", occurrence, db_path=self.db, reports_root=self.reports
        )
        self.assertEqual(completed["status"], "succeeded")

    def test_second_report_guard_fails_before_task_creation(self) -> None:
        occurrence = datetime(2026, 8, 3, 8, 0, tzinfo=SHANGHAI)
        with (
            patch(
                "v8.scheduler._require_report_job_runtime_ready",
                side_effect=[{}, ReportTaskError("release changed")],
            ),
            self.assertRaisesRegex(ReportTaskError, "release changed"),
        ):
            execute_job(
                "daily_report",
                occurrence,
                db_path=self.db,
                reports_root=self.reports,
            )
        with connect(self.db) as connection:
            run = connection.execute(
                "SELECT status,details_json FROM scheduler_runs"
            ).fetchone()
            self.assertEqual(run["status"], "failed")
            self.assertIn("release changed", run["details_json"])
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM report_tasks").fetchone()[0],
                0,
            )

    def test_first_startup_runs_latest_due_slot_and_records_honest_capture_skip(
        self,
    ) -> None:
        now = datetime(2026, 8, 2, 9, 0, tzinfo=SHANGHAI)
        results = startup_catchup(now=now, db_path=self.db, reports_root=self.reports)
        self.assertEqual(
            {item["job_id"] for item in results}, {job.job_id for job in JOBS}
        )
        capture = next(item for item in results if item["job_id"] == "daily_capture")
        self.assertEqual(capture["status"], "skipped")
        self.assertIn("没有已启用", capture["details"]["reason"])
        weekly = next(item for item in results if item["job_id"] == "weekly_report")
        media_processing = next(
            item for item in results if item["job_id"] == "daily_media_processing"
        )
        self.assertEqual(weekly["status"], "succeeded")
        self.assertEqual(set(media_processing["details"]), {"media", "duplicates"})
        self.assertEqual(media_processing["details"]["duplicates"]["failed"], 0)
        with connect(self.db) as connection:
            statuses = {
                row["job_id"]: row["status"]
                for row in connection.execute(
                    "SELECT job_id, status FROM scheduler_runs"
                )
            }
        self.assertEqual(statuses["daily_capture"], "skipped")
        self.assertEqual(statuses["daily_media_download"], "succeeded")
        self.assertEqual(statuses["daily_media_processing"], "succeeded")
        self.assertEqual(statuses["daily_media_cutoff"], "succeeded")

        media_download = next(
            item for item in results if item["job_id"] == "daily_media_download"
        )
        self.assertEqual(set(media_download["details"]), {"fresh_content"})

    def test_startup_catchup_defers_report_jobs_without_claiming_them(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evaluation_releases SET status='retired' WHERE status='active'"
            )
            connection.commit()
        results = startup_catchup(
            now=datetime(2026, 8, 2, 9, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            reports_root=self.reports,
        )
        report_results = [
            item
            for item in results
            if item["job_id"] in {"daily_report", "weekly_report"}
        ]
        self.assertEqual(
            [(item["job_id"], item["status"]) for item in report_results],
            [("daily_report", "deferred"), ("weekly_report", "deferred")],
        )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM scheduler_runs
                    WHERE job_id IN ('daily_report','weekly_report')
                    """
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM scheduler_runs
                    WHERE job_id NOT IN ('daily_report','weekly_report')
                    """
                ).fetchone()[0],
                4,
            )

    def test_media_download_job_fails_only_for_retryable_failures(self) -> None:
        retryable_result = {
            "candidates": 1,
            "downloaded": 0,
            "failed": 1,
            "retryable_failed": 1,
            "terminal_failed": 0,
            "results": [{"content_id": 1, "status": "retryable_failed"}],
        }
        retryable_time = datetime(2026, 8, 2, 2, 20, tzinfo=SHANGHAI)
        with patch(
            "v8.scheduler.run_media_download_queue", return_value=retryable_result
        ) as download_queue:
            retryable = execute_job(
                "daily_media_download",
                retryable_time,
                db_path=self.db,
                reports_root=self.reports,
            )
        download_queue.assert_called_once_with(
            limit=500,
            db_path=self.db,
            published_start="2026-07-31T16:00:00Z",
            published_end="2026-08-01T18:20:00Z",
        )
        self.assertEqual(retryable["status"], "failed")
        self.assertEqual(retryable["details"]["fresh_content"], retryable_result)
        with connect(self.db) as connection:
            original_failed = dict(
                connection.execute(
                    """
                    SELECT status,started_at,completed_at,details_json
                    FROM scheduler_runs
                    WHERE job_id='daily_media_download' AND scheduled_for=?
                    """,
                    ("2026-08-01T18:20:00Z",),
                ).fetchone()
            )

        recovered_result = {
            "candidates": 1,
            "downloaded": 1,
            "failed": 0,
            "retryable_failed": 0,
            "terminal_failed": 0,
            "results": [{"content_id": 1, "status": "downloaded"}],
        }
        with patch(
            "v8.scheduler.run_media_download_queue", return_value=recovered_result
        ) as recovered_queue:
            retried = execute_job(
                "daily_media_download",
                retryable_time,
                db_path=self.db,
                reports_root=self.reports,
            )
        self.assertEqual(retried["status"], "skipped_duplicate")
        recovered_queue.assert_not_called()
        with connect(self.db) as connection:
            unchanged_failed = dict(
                connection.execute(
                    """
                    SELECT status,started_at,completed_at,details_json
                    FROM scheduler_runs
                    WHERE job_id='daily_media_download' AND scheduled_for=?
                    """,
                    ("2026-08-01T18:20:00Z",),
                ).fetchone()
            )
        self.assertEqual(unchanged_failed, original_failed)

        recovered_time = datetime(2026, 8, 3, 2, 20, tzinfo=SHANGHAI)
        with patch(
            "v8.scheduler.run_media_download_queue", return_value=recovered_result
        ):
            recovered = execute_job(
                "daily_media_download",
                recovered_time,
                db_path=self.db,
                reports_root=self.reports,
            )
        self.assertEqual(recovered["status"], "succeeded")
        self.assertEqual(recovered["details"]["fresh_content"], recovered_result)

        terminal_result = {
            "candidates": 1,
            "downloaded": 0,
            "failed": 1,
            "retryable_failed": 0,
            "terminal_failed": 1,
            "results": [{"content_id": 1, "status": "terminal_failed"}],
        }
        terminal_time = datetime(2026, 8, 4, 2, 20, tzinfo=SHANGHAI)
        with patch(
            "v8.scheduler.run_media_download_queue", return_value=terminal_result
        ):
            terminal = execute_job(
                "daily_media_download",
                terminal_time,
                db_path=self.db,
                reports_root=self.reports,
            )
        self.assertEqual(terminal["status"], "succeeded")
        self.assertEqual(terminal["details"]["fresh_content"], terminal_result)

        with connect(self.db) as connection:
            statuses = {
                row["scheduled_for"]: row["status"]
                for row in connection.execute(
                    "SELECT scheduled_for,status FROM scheduler_runs "
                    "WHERE job_id='daily_media_download'"
                )
            }
        self.assertEqual(statuses["2026-08-01T18:20:00Z"], "failed")
        self.assertEqual(statuses["2026-08-02T18:20:00Z"], "succeeded")
        self.assertEqual(statuses["2026-08-03T18:20:00Z"], "succeeded")

    def test_media_jobs_fail_when_candidate_probe_is_truncated(self) -> None:
        truncated_download = {
            "candidates": 500,
            "downloaded": 500,
            "failed": 0,
            "retryable_failed": 0,
            "terminal_failed": 0,
            "truncated": True,
            "has_more": True,
            "results": [],
        }
        download_time = datetime(2026, 8, 6, 2, 20, tzinfo=SHANGHAI)
        with patch(
            "v8.scheduler.run_media_download_queue", return_value=truncated_download
        ):
            result = execute_job(
                "daily_media_download",
                download_time,
                db_path=self.db,
                reports_root=self.reports,
            )
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["details"]["fresh_content"]["truncated"])

        truncated_processing = {
            "candidates": 500,
            "evidence_ready": 500,
            "failed": 0,
            "retryable_failed": 0,
            "terminal_failed": 0,
            "truncated": True,
            "has_more": True,
            "results": [],
        }
        duplicates_ok = {"candidates": 0, "completed": 0, "failed": 0, "results": []}
        processing_time = datetime(2026, 8, 6, 3, 0, tzinfo=SHANGHAI)
        with (
            patch(
                "v8.scheduler.run_media_processing_queue",
                return_value=truncated_processing,
            ),
            patch(
                "v8.scheduler.run_duplicate_fingerprint_queue",
                return_value=duplicates_ok,
            ),
        ):
            result = execute_job(
                "daily_media_processing",
                processing_time,
                db_path=self.db,
                reports_root=self.reports,
            )
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["details"]["media"]["truncated"])

    def test_running_occurrence_is_not_reclaimed_or_mutated(self) -> None:
        occurrence = datetime(2026, 8, 2, 8, 0, tzinfo=SHANGHAI)
        scheduled_for = "2026-08-02T00:00:00Z"
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id,scheduled_for,status,started_at,details_json
                ) VALUES ('daily_report',?,'running','2026-08-02T00:00:01Z',
                          '{"owner":"original"}')
                """,
                (scheduled_for,),
            )
            connection.commit()
        with patch("v8.scheduler._run_job_action") as action:
            result = execute_job(
                "daily_report",
                occurrence,
                db_path=self.db,
                reports_root=self.reports,
            )
        self.assertEqual(result["status"], "skipped_duplicate")
        action.assert_not_called()
        with connect(self.db) as connection:
            row = connection.execute(
                """
                SELECT status,started_at,completed_at,details_json
                FROM scheduler_runs WHERE job_id='daily_report' AND scheduled_for=?
                """,
                (scheduled_for,),
            ).fetchone()
        self.assertEqual(
            dict(row),
            {
                "status": "running",
                "started_at": "2026-08-02T00:00:01Z",
                "completed_at": None,
                "details_json": '{"owner":"original"}',
            },
        )

    def test_concurrent_claim_executes_an_occurrence_once(self) -> None:
        occurrence = datetime(2026, 8, 5, 8, 0, tzinfo=SHANGHAI)
        start = threading.Barrier(3)
        results = []
        result_lock = threading.Lock()
        action_count = 0
        action_lock = threading.Lock()

        def action(*_args, **_kwargs):
            nonlocal action_count
            with action_lock:
                action_count += 1
            return "succeeded", {"owner": "winner"}

        def worker() -> None:
            start.wait(timeout=2)
            result = execute_job(
                "daily_report",
                occurrence,
                db_path=self.db,
                reports_root=self.reports,
            )
            with result_lock:
                results.append(result["status"])

        with patch("v8.scheduler._run_job_action", side_effect=action):
            threads = [threading.Thread(target=worker) for _ in range(2)]
            for thread in threads:
                thread.start()
            start.wait(timeout=2)
            for thread in threads:
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())

        self.assertEqual(sorted(results), ["skipped_duplicate", "succeeded"])
        self.assertEqual(action_count, 1)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM scheduler_runs
                    WHERE job_id='daily_report' AND scheduled_for=?
                    """,
                    ("2026-08-05T00:00:00Z",),
                ).fetchone()[0],
                1,
            )

    def test_media_processing_job_reports_media_and_duplicate_failures(self) -> None:
        retryable_media = {
            "candidates": 2,
            "evidence_ready": 1,
            "failed": 1,
            "retryable_failed": 1,
            "terminal_failed": 0,
            "results": [{"content_id": 2, "status": "retryable_failed"}],
        }
        duplicates_ok = {"candidates": 2, "completed": 2, "failed": 0, "results": []}
        media_time = datetime(2026, 8, 2, 3, 0, tzinfo=SHANGHAI)
        with (
            patch(
                "v8.scheduler.run_media_processing_queue",
                return_value=retryable_media,
            ) as processing_queue,
            patch(
                "v8.scheduler.run_duplicate_fingerprint_queue",
                return_value=duplicates_ok,
            ),
        ):
            media_failed = execute_job(
                "daily_media_processing",
                media_time,
                db_path=self.db,
                reports_root=self.reports,
            )
        self.assertEqual(media_failed["status"], "failed")
        self.assertEqual(media_failed["details"]["media"], retryable_media)
        self.assertEqual(media_failed["details"]["duplicates"], duplicates_ok)
        processing_queue.assert_called_once_with(
            limit=500,
            db_path=self.db,
            published_start="2026-07-31T16:00:00Z",
            published_end="2026-08-01T19:00:00Z",
        )

        media_ok = {
            "candidates": 1,
            "evidence_ready": 0,
            "failed": 1,
            "retryable_failed": 0,
            "terminal_failed": 1,
            "results": [{"content_id": 3, "status": "terminal_failed"}],
        }
        duplicates_failed = {
            "candidates": 1,
            "completed": 0,
            "failed": 1,
            "results": [{"content_id": 3, "status": "retryable_failed"}],
        }
        terminal_time = datetime(2026, 8, 3, 3, 0, tzinfo=SHANGHAI)
        with (
            patch("v8.scheduler.run_media_processing_queue", return_value=media_ok),
            patch(
                "v8.scheduler.run_duplicate_fingerprint_queue",
                return_value=duplicates_ok,
            ),
        ):
            terminal_only = execute_job(
                "daily_media_processing",
                terminal_time,
                db_path=self.db,
                reports_root=self.reports,
            )
        self.assertEqual(terminal_only["status"], "succeeded")
        self.assertEqual(terminal_only["details"]["media"], media_ok)

        duplicate_time = datetime(2026, 8, 4, 3, 0, tzinfo=SHANGHAI)
        with (
            patch("v8.scheduler.run_media_processing_queue", return_value=media_ok),
            patch(
                "v8.scheduler.run_duplicate_fingerprint_queue",
                return_value=duplicates_failed,
            ),
        ):
            duplicate_failed = execute_job(
                "daily_media_processing",
                duplicate_time,
                db_path=self.db,
                reports_root=self.reports,
            )
        self.assertEqual(duplicate_failed["status"], "failed")
        self.assertEqual(duplicate_failed["details"]["media"], media_ok)
        self.assertEqual(duplicate_failed["details"]["duplicates"], duplicates_failed)

    def test_latest_weekly_occurrence_is_monday_0830(self) -> None:
        weekly = next(job for job in JOBS if job.job_id == "weekly_report")
        occurrence = latest_occurrence(
            weekly, datetime(2026, 8, 2, 9, 0, tzinfo=SHANGHAI)
        )
        self.assertEqual(occurrence.isoformat(), "2026-07-27T08:30:00+08:00")

    def test_daily_capture_discovers_then_updates_content_with_real_slot_semantics(
        self,
    ) -> None:
        upsert_account(
            {
                "phone": "13800138000",
                "operator_name": "运营甲",
                "platforms": [
                    {"platform": "douyin", "uid": "99887766", "nickname": "汽车号"}
                ],
            },
            db_path=self.db,
        )

        def supplier_call(operation, record):
            if operation == "resolve_account":
                data = {"reference": "MS4wLjAB" + "x" * 40}
            elif operation == "discover_content":
                data = {
                    "items": [
                        {
                            "platform": "douyin",
                            "platform_content_id": "987654321",
                            "canonical_url": "https://www.douyin.com/video/987654321",
                            "title": "新车内容",
                            "body": "新车内容完整正文",
                            "published_at": "2026-08-01T01:00:00Z",
                            "content_type": "video",
                        }
                    ]
                }
            elif operation == "detail":
                data = {
                    "title": "新车内容详情",
                    "body": "新车内容详情完整正文",
                    "published_at": "2026-08-01T01:00:00Z",
                    "account_uid": "99887766",
                    "account_name": "汽车号",
                    "content_type": "video",
                }
            elif operation == "metrics":
                data = {
                    "view_count": 100,
                    "comment_count": 2,
                    "like_count": 10,
                    "share_count": 1,
                    "collect_count": None,
                }
            else:
                data = {"comment_count": 0, "comments": []}
            return ProviderResult(
                data, {"operation": operation, "data": data}, 200, True
            )

        result = execute_job(
            "daily_capture",
            datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            reports_root=self.reports,
            capture_call_override=supplier_call,
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["details"]["monitored_accounts"], 1)
        self.assertEqual(result["details"]["monitored_contents"], 1)
        self.assertEqual(result["details"]["eligible_contents"], 1)
        self.assertEqual(result["details"]["task_id"], "daily-capture-2026-08-02-bjt")
        self.assertEqual(
            result["details"]["budget_max_amount"], DAILY_CAPTURE_MAX_AMOUNT
        )
        self.assertEqual(
            result["details"]["content_limit"], DAILY_CAPTURE_CONTENT_LIMIT
        )
        self.assertEqual(result["details"]["provider_cost"], 0.005)

        with connect(self.db) as connection:
            content = connection.execute(
                "SELECT * FROM content_items WHERE platform_content_id='987654321'"
            ).fetchone()
            snapshot = connection.execute(
                "SELECT * FROM content_metric_snapshots"
            ).fetchone()
            slots = connection.execute("SELECT status FROM fetch_slots").fetchall()
            task_ids = {
                row["task_id"]
                for row in connection.execute("SELECT task_id FROM provider_usage")
            }
        self.assertEqual(content["title"], "新车内容详情")
        self.assertEqual(snapshot["view_count"], 100)
        self.assertTrue(all(row["status"] == "succeeded" for row in slots))
        self.assertEqual(task_ids, {"daily-capture-2026-08-02-bjt"})

    def test_due_capture_selection_filters_completed_before_limit_and_uses_bjt(
        self,
    ) -> None:
        account = upsert_account(
            {
                "phone": "13800138120",
                "platforms": [{"platform": "douyin", "uid": "99888120"}],
            },
            db_path=self.db,
        )
        completed = self._insert_scheduled_content(
            account_id=int(account["id"]),
            link_id="A2BC3D",
            platform_content_id="812000001",
            published_at="2026-08-04T17:00:00Z",
        )
        yesterday = self._insert_scheduled_content(
            account_id=int(account["id"]),
            link_id="A2BC3E",
            platform_content_id="812000002",
            published_at="2026-08-03T17:00:00Z",
        )
        today = self._insert_scheduled_content(
            account_id=int(account["id"]),
            link_id="A2BC3F",
            platform_content_id="812000003",
            published_at="2026-08-04T16:30:00Z",
        )
        current_week = self._insert_scheduled_content(
            account_id=int(account["id"]),
            link_id="A2BC3G",
            platform_content_id="812000004",
            published_at="2026-08-03T02:00:00Z",
        )
        history = self._insert_scheduled_content(
            account_id=int(account["id"]),
            link_id="A2BC3H",
            platform_content_id="812000005",
            published_at="2026-07-20T02:00:00Z",
        )
        for content_id in [completed, yesterday, today, current_week, history]:
            self._insert_capture_slot(
                content_id=content_id,
                stage="detail",
                window_key="lifetime",
                status="terminal_failed",
                updated_at="2026-07-01T00:00:00Z",
            )
        self._insert_capture_slot(
            content_id=completed,
            stage="metrics",
            window_key="2026-08-05",
            status="succeeded",
            provider="legacy-cache",
            updated_at="2026-08-04T18:00:00Z",
        )
        self._insert_capture_slot(
            content_id=completed,
            stage="comments",
            window_key="2026-W32",
            status="succeeded",
            provider="legacy-cache",
            updated_at="2026-08-04T18:00:00Z",
        )
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO content_metric_snapshots(
                    content_id,captured_at,window_key,view_count,status,source
                ) VALUES (?, '2026-08-04T18:00:00Z', '2026-08-05', 10,
                          'available', 'douyin')
                """,
                (completed,),
            )
            connection.execute(
                """
                INSERT INTO comment_evidence_versions(
                    content_id,captured_at,iso_week,source,local_path,sha256,
                    comment_count,status,created_at
                ) VALUES (?, '2026-08-04T18:00:00Z', '2026-W32', 'test',
                          'test.json', ?, 0, 'available', '2026-08-04T18:00:00Z')
                """,
                (completed, "a" * 64),
            )
            connection.commit()
        self._insert_comment_run(
            content_id=completed,
            window_key="2026-W32",
        )

        selected = _select_due_capture_contents(
            datetime(2026, 8, 5, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            content_limit=3,
        )
        self.assertEqual(
            [int(item["id"]) for item in selected],
            [yesterday, today, current_week],
        )
        self.assertNotIn(completed, [int(item["id"]) for item in selected])

    def test_comment_due_state_is_owned_by_capture_run_not_legacy_slot(self) -> None:
        account = upsert_account(
            {
                "phone": "13800138122",
                "platforms": [{"platform": "douyin", "uid": "99888122"}],
            },
            db_path=self.db,
        )
        content_id = self._insert_scheduled_content(
            account_id=int(account["id"]),
            link_id="C2D3E4",
            platform_content_id="812200001",
            published_at="2026-08-03T17:00:00Z",
        )
        for stage, window in (
            ("detail", "lifetime"),
            ("metrics", "2026-08-05"),
            ("comments", "2026-W32"),
        ):
            self._insert_capture_slot(
                content_id=content_id,
                stage=stage,
                window_key=window,
                status="succeeded",
                provider="legacy-cache",
                updated_at="2026-08-04T18:00:00Z",
            )
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO content_metric_snapshots(
                    content_id,captured_at,window_key,view_count,status,source
                ) VALUES (?,'2026-08-04T18:00:00Z','2026-08-05',10,
                          'available','douyin')
                """,
                (content_id,),
            )
            connection.commit()

        due_without_run = _select_due_capture_contents(
            datetime(2026, 8, 5, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            content_limit=10,
        )
        self.assertEqual([int(item["id"]) for item in due_without_run], [content_id])

        self._insert_comment_run(content_id=content_id, window_key="2026-W32")
        due_with_run = _select_due_capture_contents(
            datetime(2026, 8, 5, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            content_limit=10,
        )
        self.assertEqual(due_with_run, [])

    def test_due_capture_selection_rotates_history_by_oldest_touch(self) -> None:
        account = upsert_account(
            {
                "phone": "13800138121",
                "platforms": [{"platform": "douyin", "uid": "99888121"}],
            },
            db_path=self.db,
        )
        history_ids = []
        history_cases = [
            ("B2C3D4", "2026-08-04T03:00:00Z"),
            ("B2C3E4", "2026-08-04T01:00:00Z"),
            ("B2C3F4", "2026-08-04T04:00:00Z"),
            ("B2C3G4", "2026-08-04T02:00:00Z"),
        ]
        for index, (link_id, updated_at) in enumerate(history_cases):
            content_id = self._insert_scheduled_content(
                account_id=int(account["id"]),
                link_id=link_id,
                platform_content_id=str(812100001 + index),
                published_at=f"2026-07-{20 + index:02d}T02:00:00Z",
            )
            history_ids.append(content_id)
            self._insert_capture_slot(
                content_id=content_id,
                stage="metrics",
                window_key="2026-08-05",
                status="retryable_failed",
                updated_at=updated_at,
            )

        selected = _select_due_capture_contents(
            datetime(2026, 8, 5, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            content_limit=2,
        )
        self.assertEqual(
            [int(item["id"]) for item in selected],
            [history_ids[1], history_ids[3]],
        )

    def test_daily_capture_keeps_cost_of_post_fetch_identity_conflict(self) -> None:
        upsert_account(
            {
                "phone": "13800138089",
                "platforms": [
                    {
                        "platform": "xiaohongshu",
                        "uid": "scheduler-identity-conflict",
                    }
                ],
            },
            db_path=self.db,
        )
        conflict = IdentityConflictError(
            "identity_conflict: 内容身份冲突",
            provider_cost=0.01,
        )
        with patch(
            "v8.scheduler.discover_account_content", side_effect=conflict
        ) as discover:
            result = run_due_capture(
                datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
                db_path=self.db,
            )
        discover.assert_called_once()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["provider_cost"], 0.01)
        self.assertEqual(result["discovery"][0]["status"], "failed")
        self.assertEqual(result["discovery"][0]["stopped_reason"], "identity_conflict")
        self.assertEqual(result["discovery"][0]["pages"][0]["provider_cost"], 0.01)

    def test_daily_capture_retries_transient_metrics_once_in_the_same_slot(
        self,
    ) -> None:
        upsert_account(
            {
                "phone": "13800138087",
                "platforms": [
                    {"platform": "douyin", "uid": "99887787", "nickname": "重试账号"}
                ],
            },
            db_path=self.db,
        )
        calls = {"metrics": 0}

        def supplier_call(operation, record):
            if operation == "resolve_account":
                data = {"reference": "MS4wLjAB" + "x" * 40}
            elif operation == "discover_content":
                data = {
                    "items": [
                        {
                            "platform": "douyin",
                            "platform_content_id": "987654322",
                            "canonical_url": "https://www.douyin.com/video/987654322",
                            "title": "待重试内容",
                            "published_at": "2026-08-01T01:00:00Z",
                            "content_type": "video",
                        }
                    ],
                    "has_more": False,
                }
            elif operation == "detail":
                data = {
                    "title": "待重试内容",
                    "body": "待重试内容完整正文",
                    "published_at": "2026-08-01T01:00:00Z",
                    "account_uid": "99887787",
                    "account_name": "重试账号",
                    "content_type": "video",
                }
            elif operation == "metrics":
                calls["metrics"] += 1
                if calls["metrics"] == 1:
                    raise CaptureError(
                        "TikHub HTTP 400",
                        retryable=True,
                        error_code="provider_retry_requested",
                        http_status=400,
                        billed=False,
                        raw_response={"detail": {"message": "Please retry"}},
                    )
                data = {
                    "view_count": 100,
                    "comment_count": 2,
                    "like_count": 10,
                    "share_count": 1,
                    "collect_count": None,
                }
            else:
                data = {"comment_count": 0, "comments": []}
            return ProviderResult(
                data, {"operation": operation, "data": data}, 200, True
            )

        result = run_due_capture(
            datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            call_override=supplier_call,
        )

        self.assertEqual(result["status"], "succeeded", result)
        self.assertEqual(calls["metrics"], 2)
        with connect(self.db) as connection:
            slot = connection.execute(
                """
                SELECT status,attempt_count FROM fetch_slots
                WHERE stage='metrics' AND window_key='2026-08-02'
                """
            ).fetchone()
            attempts = connection.execute(
                """
                SELECT fa.attempt_number,fa.error_code,fa.billed
                FROM fetch_attempts fa JOIN fetch_slots fs ON fs.id=fa.slot_id
                WHERE fs.stage='metrics' AND fs.window_key='2026-08-02'
                ORDER BY fa.attempt_number
                """
            ).fetchall()
        self.assertEqual((slot["status"], slot["attempt_count"]), ("succeeded", 2))
        self.assertEqual(
            [
                (row["attempt_number"], row["error_code"], row["billed"])
                for row in attempts
            ],
            [(1, "provider_retry_requested", 0), (2, None, 1)],
        )

    def test_daily_capture_retries_transient_discovery_once_then_stops(self) -> None:
        upsert_account(
            {
                "phone": "13800138086",
                "platforms": [
                    {"platform": "xiaohongshu", "uid": "retry-discovery-account"}
                ],
            },
            db_path=self.db,
        )
        calls = {"discover_content": 0}

        def supplier_call(operation, record):
            self.assertEqual(operation, "discover_content")
            calls["discover_content"] += 1
            raise CaptureError(
                "TikHub HTTP 400",
                retryable=True,
                error_code="provider_retry_requested",
                http_status=400,
                billed=False,
                raw_response={"detail": {"message": "Please retry"}},
            )

        result = run_due_capture(
            datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            call_override=supplier_call,
        )

        self.assertEqual(result["status"], "failed")
        self.assertEqual(calls["discover_content"], 2)
        with connect(self.db) as connection:
            slot = connection.execute(
                """
                SELECT status,attempt_count,last_error_code FROM fetch_slots
                WHERE stage='discovery' AND window_key='2026-08-02:xiaohongshu:page:1'
                """
            ).fetchone()
        self.assertEqual(
            (slot["status"], slot["attempt_count"], slot["last_error_code"]),
            ("retryable_failed", 2, "provider_retry_requested"),
        )

    def test_daily_capture_provider_cost_matches_ledger_after_billed_retries(
        self,
    ) -> None:
        upsert_account(
            {
                "phone": "13800138085",
                "platforms": [
                    {"platform": "douyin", "uid": "99887785", "nickname": "费用账号"}
                ],
            },
            db_path=self.db,
        )
        calls = {"metrics": 0}

        def supplier_call(operation, record):
            if operation == "resolve_account":
                data = {"reference": "MS4wLjAB" + "x" * 40}
            elif operation == "discover_content":
                data = {
                    "items": [
                        {
                            "platform": "douyin",
                            "platform_content_id": "987654323",
                            "canonical_url": "https://www.douyin.com/video/987654323",
                            "title": "费用重试内容",
                            "published_at": "2026-08-01T01:00:00Z",
                            "content_type": "video",
                        }
                    ],
                    "has_more": False,
                }
            elif operation == "detail":
                data = {
                    "title": "费用重试内容",
                    "body": "费用重试内容完整正文",
                    "published_at": "2026-08-01T01:00:00Z",
                    "account_uid": "99887785",
                    "account_name": "费用账号",
                    "content_type": "video",
                }
            elif operation == "metrics":
                calls["metrics"] += 1
                raise CaptureError(
                    "TikHub statistics omitted play_count for requested content",
                    retryable=True,
                    error_code="invalid_response",
                    http_status=200,
                    billed=True,
                    raw_response={"data": {"comment_count": 2}},
                )
            else:
                data = {"comment_count": 0, "comments": []}
            return ProviderResult(
                data, {"operation": operation, "data": data}, 200, True
            )

        result = run_due_capture(
            datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            call_override=supplier_call,
        )

        with connect(self.db) as connection:
            ledger_cost = float(
                connection.execute(
                    """
                    SELECT COALESCE(SUM(amount), 0) FROM provider_usage
                    WHERE task_id='daily-capture-2026-08-02-bjt'
                      AND currency='USD'
                    """
                ).fetchone()[0]
            )
        self.assertEqual(result["status"], "failed")
        self.assertEqual(calls["metrics"], 2)
        self.assertGreater(ledger_cost, 0)
        self.assertEqual(result["provider_cost"], ledger_cost)

    def test_xhs_block_opens_one_tikhub_circuit_and_never_uses_rnote(self) -> None:
        upsert_account(
            {
                "phone": "13800138001",
                "platforms": [
                    {
                        "platform": "xiaohongshu",
                        "uid": "67f6657f000000000e02c21c",
                        "nickname": "小红书号",
                    }
                ],
            },
            db_path=self.db,
        )
        upsert_account(
            {
                "phone": "13800138002",
                "platforms": [
                    {
                        "platform": "douyin",
                        "uid": "99887766",
                        "nickname": "抖音号",
                    }
                ],
            },
            db_path=self.db,
        )

        def blocked_call(operation, record):
            raise CaptureError(
                "TikHub balance blocked",
                retryable=True,
                error_code="provider_balance_blocked",
                http_status=402,
                billed=False,
                raw_response={"detail": "insufficient balance"},
            )

        result = run_due_capture(
            datetime(2026, 8, 3, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            call_override=blocked_call,
        )
        self.assertEqual(result["blocked_providers"], ["TikHub"])
        self.assertEqual(
            [item["status"] for item in result["discovery"]],
            ["failed", "circuit_break_skipped"],
        )
        with connect(self.db) as connection:
            providers = {
                row["provider"]
                for row in connection.execute("SELECT provider FROM provider_usage")
            }
        self.assertNotIn("Rnote", providers)

    def test_daily_capture_paginates_until_beijing_window_start(self) -> None:
        upsert_account(
            {
                "phone": "13800138003",
                "platforms": [
                    {
                        "platform": "douyin",
                        "uid": "99887777",
                        "nickname": "高频账号",
                    }
                ],
            },
            db_path=self.db,
        )
        discovery_cursors = []

        def content_item(content_id: int, published_at: str):
            return {
                "platform": "douyin",
                "platform_content_id": str(content_id),
                "canonical_url": f"https://www.douyin.com/video/{content_id}",
                "title": f"内容{content_id}",
                "body": f"内容{content_id}完整正文",
                "published_at": published_at,
                "content_type": "video",
                "view_count": 100,
                "comment_count": 0,
            }

        def supplier_call(operation, record):
            if operation == "resolve_account":
                data = {"reference": "MS4wLjAB" + "x" * 40}
            elif operation == "discover_content":
                cursor = record.get("cursor")
                discovery_cursors.append(cursor)
                if cursor is None:
                    data = {
                        "items": [
                            content_item(810000000 + index, "2026-08-01T10:00:00Z")
                            for index in range(20)
                        ],
                        "has_more": True,
                        "next_cursor": "page-2",
                    }
                else:
                    data = {
                        "items": [
                            content_item(820000001, "2026-08-01T09:00:00Z"),
                            content_item(820000002, "2026-07-31T15:00:00Z"),
                        ],
                        "has_more": True,
                        "next_cursor": "page-3",
                    }
            elif operation == "detail":
                data = {
                    "title": "内容详情",
                    "body": "内容详情完整正文",
                    "published_at": "2026-08-01T10:00:00Z",
                    "account_uid": "99887777",
                    "account_name": "高频账号",
                    "content_type": "video",
                }
            elif operation == "metrics":
                data = {
                    "view_count": 100,
                    "comment_count": 0,
                    "like_count": 1,
                    "share_count": 0,
                    "collect_count": None,
                }
            else:
                data = {"comment_count": 0, "comments": []}
            return ProviderResult(
                data, {"operation": operation, "data": data}, 200, True
            )

        result = run_due_capture(
            datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            call_override=supplier_call,
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(discovery_cursors, [None, "page-2"])
        self.assertEqual(len(result["discovery"][0]["pages"]), 2)
        self.assertEqual(
            result["discovery"][0]["stopped_reason"], "window_start_reached"
        )
        with connect(self.db) as connection:
            content_count = connection.execute(
                "SELECT COUNT(*) FROM content_items"
            ).fetchone()[0]
            discovery_windows = {
                row["window_key"]
                for row in connection.execute(
                    "SELECT window_key FROM fetch_slots WHERE stage='discovery'"
                )
            }
        self.assertEqual(content_count, 21)
        self.assertIn("2026-08-02:douyin:page:1", discovery_windows)
        self.assertIn("2026-08-02:douyin:page:2", discovery_windows)

    def test_daily_discovery_slots_are_isolated_by_platform(self) -> None:
        upsert_account(
            {
                "phone": "13800138005",
                "platforms": [
                    {"platform": "douyin", "uid": "99887789", "nickname": "双平台抖音"},
                    {
                        "platform": "xiaohongshu",
                        "uid": "67f6657f000000000e02c22c",
                        "nickname": "双平台小红书",
                    },
                ],
            },
            db_path=self.db,
        )

        def supplier_call(operation, record):
            if operation == "resolve_account":
                data = {"reference": "MS4wLjAB" + "x" * 40}
            elif operation == "discover_content":
                platform = record["platform"]
                content_id = (
                    "840000001" if platform == "douyin" else "67f6657f000000000e02c23c"
                )
                host = (
                    "www.douyin.com/video"
                    if platform == "douyin"
                    else "www.xiaohongshu.com/explore"
                )
                data = {
                    "items": [
                        {
                            "platform": platform,
                            "platform_content_id": content_id,
                            "canonical_url": f"https://{host}/{content_id}",
                            "title": f"{platform}内容",
                            "body": f"{platform}内容完整正文",
                            "published_at": "2026-08-01T10:00:00Z",
                            "content_type": "video" if platform == "douyin" else "note",
                            "view_count": 100,
                            "comment_count": 0,
                        }
                    ],
                    "has_more": False,
                }
            elif operation == "detail":
                data = {
                    "title": "内容详情",
                    "body": "内容详情完整正文",
                    "published_at": "2026-08-01T10:00:00Z",
                    "account_uid": "99887789",
                    "account_name": "双平台账号",
                    "content_type": record.get("content_type") or "video",
                }
            elif operation == "metrics":
                data = {
                    "view_count": 100,
                    "comment_count": 0,
                    "like_count": 1,
                    "share_count": 0,
                    "collect_count": None,
                }
            else:
                data = {"comment_count": 0, "comments": []}
            return ProviderResult(
                data, {"operation": operation, "data": data}, 200, True
            )

        result = run_due_capture(
            datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            call_override=supplier_call,
        )
        self.assertEqual(result["status"], "succeeded", result)
        self.assertEqual(
            [item["status"] for item in result["discovery"]],
            ["succeeded", "succeeded"],
        )
        with connect(self.db) as connection:
            platforms = {
                row["platform"]
                for row in connection.execute("SELECT platform FROM content_items")
            }
            discovery_windows = {
                row["window_key"]
                for row in connection.execute(
                    "SELECT window_key FROM fetch_slots WHERE stage='discovery'"
                )
            }
        self.assertEqual(platforms, {"douyin", "xiaohongshu"})
        self.assertIn("2026-08-02:douyin:page:1", discovery_windows)
        self.assertIn("2026-08-02:xiaohongshu:page:1", discovery_windows)

    def test_daily_capture_updates_independent_contents_concurrently(self) -> None:
        upsert_account(
            {
                "phone": "13800138004",
                "platforms": [
                    {
                        "platform": "douyin",
                        "uid": "99887788",
                        "nickname": "并发账号",
                    }
                ],
            },
            db_path=self.db,
        )
        detail_barrier = threading.Barrier(4)
        detail_threads = set()
        detail_lock = threading.Lock()

        def supplier_call(operation, record):
            if operation == "resolve_account":
                data = {"reference": "MS4wLjAB" + "x" * 40}
            elif operation == "discover_content":
                data = {
                    "items": [
                        {
                            "platform": "douyin",
                            "platform_content_id": str(830000000 + index),
                            "canonical_url": (
                                f"https://www.douyin.com/video/{830000000 + index}"
                            ),
                            "title": f"并发内容{index}",
                            "published_at": "2026-08-01T10:00:00Z",
                            "content_type": "video",
                        }
                        for index in range(4)
                    ],
                    "has_more": False,
                }
            elif operation == "detail":
                with detail_lock:
                    detail_threads.add(threading.current_thread().name)
                detail_barrier.wait(timeout=2)
                data = {
                    "title": "并发内容详情",
                    "body": "并发内容详情完整正文",
                    "published_at": "2026-08-01T10:00:00Z",
                    "account_uid": "99887788",
                    "account_name": "并发账号",
                    "content_type": "video",
                }
            elif operation == "metrics":
                data = {
                    "view_count": 100,
                    "comment_count": 0,
                    "like_count": 1,
                    "share_count": 0,
                    "collect_count": None,
                }
            else:
                data = {"comment_count": 0, "comments": []}
            return ProviderResult(
                data, {"operation": operation, "data": data}, 200, True
            )

        result = run_due_capture(
            datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            call_override=supplier_call,
            content_limit=4,
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(len(detail_threads), 4)
        self.assertEqual(
            [item["status"] for item in result["content_updates"]],
            ["succeeded"] * 4,
        )

    def test_media_cutoff_routes_only_new_unfinished_content(self) -> None:
        captured_at = "2026-08-02T00:00:00Z"
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO content_items(
                    link_id, platform, platform_content_id, canonical_url, title, body,
                    content_type, source_group, imported_at, created_at, updated_at
                ) VALUES ('A2BC3D','douyin','111111111','https://www.douyin.com/video/111111111',
                          '新内容','汽车新内容','video','',?,?,?)
                """,
                (captured_at, captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO content_items(
                    link_id, platform, platform_content_id, canonical_url, title, body,
                    content_type, source_group, imported_at, created_at, updated_at
                ) VALUES ('A2BC3E','douyin','222222222','https://www.douyin.com/video/222222222',
                          '迁移内容','汽车迁移内容','video','30-account-random-sample',?,?,?)
                """,
                (captured_at, captured_at, captured_at),
            )
            connection.commit()

        result = run_media_cutoff(
            datetime(2026, 8, 2, 7, 30, tzinfo=SHANGHAI), db_path=self.db
        )
        self.assertEqual(result["candidates"], 1)
        self.assertEqual(result["manual_required"], 1)
        self.assertEqual(result["threshold_status"], "below_threshold")
        with connect(self.db) as connection:
            queue = connection.execute(
                "SELECT content_id,reason_code,status FROM review_queue"
            ).fetchall()
        self.assertEqual(
            [(row["content_id"], row["reason_code"], row["status"]) for row in queue],
            [(1, "media_processing_incomplete", "manual_required")],
        )


if __name__ == "__main__":
    unittest.main()
