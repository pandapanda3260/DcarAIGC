from __future__ import annotations

import json
import hashlib
import tempfile
import threading
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler

from tests.roster_fixture import accept_roster
import v8.capture as capture_module
import v8.pipeline as pipeline_module
from v8 import durable_runs
from v8.capture import CaptureError, ProviderResult
from v8.duplicates import FINGERPRINT_VERSION
from v8.matcher_dsl import POINT_IDS, POINT_SCENES
from v8.media_state import MediaTerminalDetail
from v8.operations import IdentityConflictError
from v8.operations import upsert_account as _upsert_account
from v8.provider_budget import fault_state
from v8.reports import ReportTaskError, _parse_daily_capture_receipt
from v8.scheduler import (
    DAILY_CAPTURE_CONTENT_LIMIT,
    DAILY_CAPTURE_MAX_AMOUNT,
    DOUYIN_OPENAPI_RECONCILE_GUARD_JOB_ID,
    DOUYIN_OPENAPI_RECONCILE_JOB_ID,
    JOBS,
    PIPELINE_REPORT_EXECUTION_LOCK,
    SchedulerJobError,
    _claim_run,
    _daily_media_cohort,
    _finish_run,
    _report_catchup_occurrences,
    _select_due_capture_contents,
    current_day_daily_capture_guard,
    current_day_pipeline_guard,
    daily_capture_quality_gate,
    douyin_openapi_reconcile_guard,
    execute_douyin_openapi_reconcile,
    execute_job,
    install_jobs,
    latest_occurrence,
    recover_interrupted_scheduler_runs,
    run_due_capture,
    run_media_cutoff,
    startup_catchup,
)
from v8.storage import PROJECT_ROOT, connect, initialize_database, now_utc, transaction
from v8.taxonomy_rule_backfill import backfill_v5_1_matcher_rules
from tests.v9_report_fixture import (
    V9_FIXTURE_RELEASE_ID,
    activate_v9_report_fixture,
)


SHANGHAI = ZoneInfo("Asia/Shanghai")


def upsert_account(value, *, db_path):
    """Keep legacy helper tests on real schema18 accepted-member fixtures."""
    account = _upsert_account(value, db_path=db_path)
    with connect(db_path) as connection:
        accept_roster(connection)
    return account


class V8SchedulerTest(unittest.TestCase):
    def setUp(self) -> None:
        (PROJECT_ROOT / "tmp").mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=PROJECT_ROOT / "tmp")
        self.root = Path(self.temp.name)
        self.db = self.root / "scheduler.sqlite3"
        self.reports = self.root / "reports"
        raw_root_patch = patch.object(
            capture_module, "RAW_ROOT", self.root / "raw"
        )
        raw_root_patch.start()
        self.addCleanup(raw_root_patch.stop)
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
        activate_v9_report_fixture(self.db, [])

    def tearDown(self) -> None:
        try:
            with connect(self.db) as connection:
                raws = connection.execute("SELECT local_path,sha256,byte_size FROM provider_raw_responses").fetchall()
            for raw in raws:
                body = Path(raw["local_path"]).read_bytes()
                self.assertEqual(len(body), raw["byte_size"])
                self.assertEqual(hashlib.sha256(body).hexdigest(), raw["sha256"])
        finally:
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

    def _insert_media_cutoff_content(
        self,
        index: int,
        *,
        source_group: str = "",
    ) -> int:
        captured_at = "2026-08-02T00:00:00Z"
        platform_content_id = f"cutoff-{index}"
        with connect(self.db) as connection:
            cursor = connection.execute(
                """
                INSERT INTO content_items(
                    link_id,platform,platform_content_id,canonical_url,title,body,
                    content_type,source_group,published_at,imported_at,created_at,updated_at
                ) VALUES (?, 'douyin', ?, ?, '媒体截止内容', '汽车内容',
                          'video', ?, '2026-08-01T04:00:00Z', ?, ?, ?)
                """,
                (
                    f"C{index:05d}",
                    platform_content_id,
                    f"https://www.douyin.com/video/{platform_content_id}",
                    source_group,
                    captured_at,
                    captured_at,
                    captured_at,
                ),
            )
            connection.commit()
        assert cursor.lastrowid is not None
        return int(cursor.lastrowid)

    def _insert_media_source_artifact(self, content_id: int) -> None:
        captured_at = "2026-08-02T01:00:00Z"
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO evidence_artifacts(
                    content_id,artifact_type,local_path,status,byte_size,sha256,
                    captured_at,processor_version,metadata_json,created_at
                ) VALUES (?,'media_source',?,'available',1,?,?,
                          'media-source-v1','{}',?)
                """,
                (
                    content_id,
                    str(self.root / f"media-source-{content_id}.json"),
                    f"{content_id:064x}",
                    captured_at,
                    captured_at,
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

    @staticmethod
    def _authorization_details(accounts):
        return {
            "contract_version": "authorization-status-v1",
            "captured_at": "2026-08-01T18:00:05Z",
            "content_sync_enabled": False,
            "authorization_state": (
                "no_authorization" if not accounts else "available"
                if all(row["status"] == "available" for row in accounts) else "attention"
            ),
            "accounts": accounts,
        }

    @staticmethod
    def _authorization_account_receipt(
        *, account_id, platform_uid, status="available", authorization_id=None
    ):
        return {
            "authorization_id": authorization_id or f"{account_id:032x}",
            "account_id": account_id,
            "platform_uid": platform_uid,
            "status": status,
            "identity_matches": True,
            "needs_reauthorization": status == "attention",
            "access_expires_at": 1_800_000_000,
            "refresh_expires_at": 1_800_086_400,
            "reason": "reauthorization_required" if status == "attention" else "",
        }

    @staticmethod
    def _openapi_details(accounts):
        return {
            "window_start": "2026-07-26T16:00:00Z",
            "coverage_end": "2026-08-01T18:00:00Z",
            "accounts": accounts,
        }

    @classmethod
    def _openapi_account_receipt(
        cls,
        *,
        account_id: int,
        platform_uid: str,
        status: str = "succeeded",
        complete: bool = True,
        pages_fetched: int = 1,
        items_discovered: int = 0,
    ):
        return {
            "account_id": account_id,
            "platform_uid": platform_uid,
            "status": status,
            "coverage_start": "2026-07-26T16:00:00Z",
            "coverage_end": "2026-08-01T18:00:00Z",
            "coverage_complete": complete,
            "pagination_complete": complete,
            "materialization_complete": complete,
            "pages_fetched": pages_fetched,
            "items_discovered": items_discovered,
            **({"error_code": "fixture_failure"} if status != "succeeded" else {}),
        }

    def _insert_openapi_run(
        self,
        *,
        local_day: date,
        status: str,
        details,
    ) -> None:
        occurrence = datetime.combine(
            local_day,
            datetime.min.time().replace(hour=2),
            SHANGHAI,
        ).astimezone(ZoneInfo("UTC"))
        scheduled_for = occurrence.isoformat().replace("+00:00", "Z")
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id,scheduled_for,status,started_at,completed_at,details_json
                ) VALUES (?,?,?,?,?,?)
                """,
                (
                    DOUYIN_OPENAPI_RECONCILE_JOB_ID,
                    scheduled_for,
                    status,
                    scheduled_for,
                    None if status == "running" else scheduled_for,
                    json.dumps(details, ensure_ascii=False, sort_keys=True),
                ),
            )
            connection.commit()

    def test_matrix_jobs_replace_only_retired_registrations(self) -> None:
        expected_cron = {
            "matrix_account_metrics": ("matrix_account_metrics", "2,6,12,18", 0, None),
            "matrix_works_scan": ("matrix_works_scan", "2", 10, None),
            "matrix_works_refresh": ("matrix_works_scan", "6,12,18", 0, None),
            "tikhub_account_metrics": ("tikhub_account_metrics", "2,6,12,18", 0, None),
            "tikhub_works_scan": ("tikhub_works_scan", "2", 10, None),
            "tikhub_works_refresh": ("tikhub_works_scan", "6,12,18", 0, None),
            "tikhub_reconcile": ("tikhub_reconcile", "3", 0, None),
            "metrics_backfill": ("metrics_backfill", "6", 10, None),
            "metrics_backfill_close": ("metrics_backfill", "7", 0, None),
            "metrics_backfill_established": ("metrics_backfill", "8", 35, None),
            "daily_pipeline_summary": ("daily_pipeline_summary", "7", 30, None),
            "daily_report": ("daily_report", "8", 0, None),
            "weekly_report": ("weekly_report", "8", 30, "mon"),
        }
        expected_retired = {
            "daily_capture", "daily_media_download", "daily_media_processing",
            "daily_media_cutoff", "daily_capture_reconcile", "history_recovery",
        }
        health_jobs = {DOUYIN_OPENAPI_RECONCILE_JOB_ID, DOUYIN_OPENAPI_RECONCILE_GUARD_JOB_ID}
        control_cron = {
            "daily_pipeline_summary", "daily_report", "weekly_report",
        }
        intervals = {"content_pipeline": 5, "comments_refresh": 5,
                     "pipeline_reconcile": 5}
        self.assertEqual(pipeline_module.CRON_ROUNDS, expected_cron)
        self.assertEqual(pipeline_module.RETIRED_JOB_IDS, expected_retired)
        self.assertTrue(health_jobs.isdisjoint(expected_retired))
        scheduler = BackgroundScheduler(timezone=SHANGHAI)

        def unrelated_callback():
            return "unrelated"

        for retired in expected_retired | health_jobs:
            scheduler.add_job(unrelated_callback, "interval", hours=1, id=retired)
        unrelated = scheduler.add_job(unrelated_callback, "interval", hours=2, id="unrelated-fixture-job")

        def supplier_fixture(operation, payload):
            self.fail(f"Inactive pipeline must not dispatch provider operation {operation}")

        def authorization_fixture(**_kwargs):
            self.fail("Installing health jobs must not execute them")

        before = datetime.now(SHANGHAI)
        install_jobs(scheduler, db_path=self.db, reports_root=self.reports,
                     capture_call_override=supplier_fixture,
                     reconcile_effective_date=date(2026, 8, 21),
                     douyin_openapi_runner=authorization_fixture)
        after = datetime.now(SHANGHAI)
        jobs = {job.id: job for job in scheduler.get_jobs()}
        self.assertEqual(set(jobs), set(expected_cron) | set(intervals) | health_jobs | {"report_reconcile", unrelated.id})
        self.assertTrue(expected_retired.isdisjoint(jobs))
        self.assertTrue(all(scheduler.get_job(key) is None for key in expected_retired))
        self.assertIs(scheduler.get_job(unrelated.id), unrelated)
        self.assertIs(unrelated.func, unrelated_callback)
        self.assertEqual(str(unrelated.trigger), "interval[2:00:00]")
        self.assertEqual(len(JOBS), 6)  # Retain historical helpers without registering them.
        self.assertTrue({"daily_capture", "daily_report", "weekly_report"} <= {job.job_id for job in JOBS})

        midnight = datetime(2026, 8, 29, 0, 0, tzinfo=SHANGHAI)
        for registration, (kind, hours, minute, weekday) in expected_cron.items():
            job = jobs[registration]
            fields = {field.name: str(field) for field in job.trigger.fields}
            self.assertEqual((fields["hour"], fields["minute"], fields["day_of_week"]),
                             (hours, str(minute), weekday or "*"))
            self.assertEqual(str(job.trigger.timezone), "Asia/Shanghai")
            fire_day = date(2026, 8, 31) if weekday else midnight.date()
            expected_fire = datetime.combine(fire_day, datetime.min.time(), SHANGHAI).replace(
                hour=int(hours.split(",")[0]), minute=minute,
            )
            self.assertEqual(job.trigger.get_next_fire_time(None, midnight), expected_fire)
            self.assertIs(job.func, pipeline_module.dispatch)
            self.assertEqual(job.args, ())
            self.assertEqual(job.kwargs, {
                "job_id": kind, "registration_id": registration,
                "db_path": self.db, "reports_root": self.reports, "call_override": supplier_fixture,
                "automatic_from": date(2026, 8, 21),
            })
            self.assertTrue(job.coalesce)
            self.assertEqual(job.max_instances, 1)
            self.assertEqual(
                job.misfire_grace_time,
                None if registration in control_cron else 3600,
            )
            self.assertEqual(
                job.executor,
                "report" if registration in {"daily_report", "weekly_report"}
                else "control" if registration in control_cron else "default",
            )
            self.assertEqual(
                job.func(**job.kwargs, at="2026-08-31T01:00:00Z")["reason"],
                "pipeline_activation_required",
            )

        for key, minutes in intervals.items():
            job = jobs[key]
            self.assertEqual(job.trigger.interval, timedelta(minutes=minutes))
            self.assertEqual(str(job.trigger.timezone), "Asia/Shanghai")
            self.assertIs(job.func, pipeline_module.dispatch)
            self.assertEqual(job.kwargs, {"job_id": key, "db_path": self.db,
                                         "reports_root": self.reports, "call_override": supplier_fixture,
                                         "automatic_from": date(2026, 8, 21)})
            self.assertTrue(job.coalesce)
            self.assertEqual(job.max_instances, 1)
            self.assertIsNone(job.misfire_grace_time)
            self.assertEqual(
                job.executor,
                "reconcile" if key == "pipeline_reconcile" else "default",
            )
            self.assertGreaterEqual(job.next_run_time, before)
            self.assertLessEqual(job.next_run_time, after)
            self.assertEqual(job.func(**job.kwargs)["reason"], "pipeline_activation_required")

        health = jobs[DOUYIN_OPENAPI_RECONCILE_JOB_ID]
        self.assertEqual(health.func.__name__, "_douyin_openapi_live_job")
        self.assertEqual(health.trigger.get_next_fire_time(None, midnight), midnight.replace(hour=2))
        self.assertEqual(str(health.trigger.timezone), "Asia/Shanghai")
        self.assertEqual(health.kwargs, {"db_path": self.db, "runner": authorization_fixture})
        self.assertTrue(health.coalesce)
        self.assertEqual((health.max_instances, health.misfire_grace_time), (1, 3600))
        self.assertEqual(health.executor, "control")
        with patch("v8.scheduler._has_authorization_health_run", return_value=False), patch(
            "v8.scheduler.execute_douyin_openapi_reconcile"
        ) as run_health:
            health.func(**health.kwargs)
        run_health.assert_called_once()
        self.assertEqual(run_health.call_args.kwargs, {"db_path": self.db, "runner": authorization_fixture})
        self.assertEqual(str(run_health.call_args.args[0].tzinfo), "Asia/Shanghai")

        guard = jobs[DOUYIN_OPENAPI_RECONCILE_GUARD_JOB_ID]
        self.assertEqual(guard.func.__name__, "_douyin_openapi_reconcile_guard_job")
        self.assertEqual(guard.trigger.interval, timedelta(hours=1))
        self.assertEqual(str(guard.trigger.timezone), "Asia/Shanghai")
        self.assertEqual(guard.kwargs, {"effective_from": date(2026, 8, 21),
                                      "db_path": self.db, "runner": authorization_fixture})
        self.assertTrue(guard.coalesce)
        self.assertEqual(guard.max_instances, 1)
        self.assertIsNone(guard.misfire_grace_time)
        self.assertEqual(guard.executor, "control")
        self.assertGreaterEqual(guard.next_run_time, before)
        self.assertLessEqual(guard.next_run_time, after)
        with patch("v8.scheduler.douyin_openapi_reconcile_guard") as health_guard:
            guard.func(**guard.kwargs)
        health_guard.assert_called_once()
        self.assertEqual(health_guard.call_args.kwargs["effective_from"], date(2026, 8, 21))
        self.assertEqual(health_guard.call_args.kwargs["db_path"], self.db)
        self.assertIs(health_guard.call_args.kwargs["runner"], authorization_fixture)
        self.assertEqual(str(health_guard.call_args.kwargs["now"].tzinfo), "Asia/Shanghai")

        report = jobs["report_reconcile"]
        self.assertEqual(str(report.trigger), "interval[1:00:00]")
        self.assertEqual(report.func.__name__, "_report_reconcile_job")
        self.assertEqual(report.kwargs, {"db_path": self.db, "reports_root": self.reports,
                                         "effective_from": date(2026, 8, 21)})
        self.assertTrue(report.coalesce)
        self.assertEqual(report.max_instances, 1)
        self.assertIsNone(report.misfire_grace_time)
        self.assertEqual(report.executor, "report")
        self.assertGreaterEqual(report.next_run_time, before)
        self.assertLessEqual(report.next_run_time, after)
        with patch("v8.scheduler.startup_catchup", return_value=[]) as catchup:
            report.func(**report.kwargs)
        catchup.assert_called_once()
        self.assertEqual(catchup.call_args.kwargs["db_path"], self.db)
        self.assertEqual(catchup.call_args.kwargs["reports_root"], self.reports)
        self.assertEqual(catchup.call_args.kwargs["effective_from"], date(2026, 8, 21))
        self.assertEqual(str(catchup.call_args.kwargs["now"].tzinfo), "Asia/Shanghai")
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)

    def test_startup_report_catchup_waits_for_pipeline_report_lock(self) -> None:
        occurrence = datetime(2026, 8, 29, 8, 0, tzinfo=SHANGHAI)
        candidate_ready = threading.Event()
        executed = threading.Event()
        results = []

        def candidates(**_kwargs):
            candidate_ready.set()
            return [("daily_report", occurrence)]

        def execute(*_args, **_kwargs):
            executed.set()
            return {"job_id": "daily_report", "status": "partial"}

        def run():
            results.extend(
                startup_catchup(
                    now=occurrence,
                    db_path=self.db,
                    reports_root=self.reports,
                )
            )

        thread = threading.Thread(target=run)
        with patch(
            "v8.scheduler._report_catchup_occurrences", side_effect=candidates
        ), patch(
            "v8.scheduler._report_duplicate_input_retry_before", return_value=None
        ), patch("v8.scheduler.execute_job", side_effect=execute):
            with PIPELINE_REPORT_EXECUTION_LOCK:
                thread.start()
                self.assertTrue(candidate_ready.wait(1))
                self.assertFalse(executed.wait(0.05))
            thread.join(timeout=2)

        self.assertFalse(thread.is_alive())
        self.assertTrue(executed.is_set())
        self.assertEqual(results[0]["status"], "partial")

    def test_install_jobs_requires_all_paid_path_dependencies(self) -> None:
        scheduler = BackgroundScheduler(timezone=SHANGHAI)
        with self.assertRaises(TypeError):
            install_jobs(  # type: ignore[call-arg]
                scheduler,
                db_path=self.db,
                reports_root=self.reports,
            )

    def test_new_pipeline_round_is_claimed_once_without_serializing_dispatch(self) -> None:
        at = "2026-08-29T00:00:00Z"
        _upsert_account(
            {"phone": "", "platforms": [{"platform": "douyin", "uid": "pipeline"}]},
            db_path=self.db,
        )
        with connect(self.db) as connection:
            accept_roster(connection, accepted_at="2026-08-28T23:00:00Z")
        entered, release = threading.Event(), threading.Event()
        results = {}
        failures = []
        second_started = threading.Event()
        second_finished = threading.Event()

        def bounded_work(*args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise AssertionError("Concurrent dispatcher failed to release fixture")
            return {"status": "succeeded", "complete": True}

        def worker(name):
            try:
                if name == "second":
                    second_started.set()
                results[name] = pipeline_module.dispatch(
                    "daily_pipeline_summary",
                    at=at,
                    db_path=self.db,
                    reports_root=self.reports,
                )
            except Exception as error:
                failures.append(error)
            finally:
                if name == "second":
                    second_finished.set()

        with patch.object(pipeline_module, "_dispatch", side_effect=bounded_work) as work:
            first = threading.Thread(target=worker, args=("first",))
            second = threading.Thread(target=worker, args=("second",))
            first.start()
            try:
                self.assertTrue(entered.wait(5))
                second.start()
                self.assertTrue(second_started.wait(1))
                self.assertTrue(second_finished.wait(1))
            finally:
                release.set()
                first.join(5)
                second.join(5)
            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
            work.assert_called_once()
            self.assertEqual(work.call_args.args, ("daily_pipeline_summary",))
            self.assertEqual(work.call_args.kwargs["registration_id"], "daily_pipeline_summary")
            self.assertEqual(work.call_args.kwargs["at"], at)
        self.assertEqual(failures, [])
        self.assertEqual((results["first"]["status"], results["first"]["complete"]),
                         ("succeeded", True))
        self.assertEqual((results["second"]["status"], results["second"]["complete"]),
                         ("running", False))
        self.assertEqual(results["second"]["reason"], "round_already_claimed_or_finished")
        with connect(self.db) as connection:
            rows = connection.execute(
                "SELECT details_json FROM scheduler_runs "
                "WHERE job_id='pipeline_round:daily_pipeline_summary'"
            ).fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(
                json.loads(rows[0]["details_json"])["identity"]["scheduled_at"],
                "2026-08-28T23:30:00Z",
            )
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_run_attempts").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)

    def test_startup_recovery_preserves_new_durable_checkpoint_and_fences_old_owner(self) -> None:
        identity = {"provider": "TikHub", "purpose": "history", "identity_id": 1,
                    "window_start": "2026-08-02T16:00:00Z", "window_end": "2026-08-28T16:00:00Z"}
        first = durable_runs.claim_run("history_recovery", identity, db_path=self.db,
                                       initial_checkpoint={"page_number": 0, "cursor": 0})
        self.assertIsNotNone(first)
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(connection, first, {"page_number": 7, "cursor": 420})
            seed = json.loads(connection.execute("SELECT details_json FROM scheduler_run_attempts WHERE id=?",
                                                (first.attempt_id,)).fetchone()[0])
            self.assertEqual(seed["checkpoint"]["page_number"], 0)
        self.assertEqual(recover_interrupted_scheduler_runs(db_path=self.db), 1)
        recovered = durable_runs.get_run(first.scheduler_run_id, db_path=self.db)
        self.assertEqual(recovered["status"], "interrupted")
        self.assertEqual(recovered["details"]["checkpoint"]["page_number"], 7)
        self.assertEqual(recovered["details"]["checkpoint"]["cursor"], 420)
        with connect(self.db) as connection:
            old = connection.execute("SELECT * FROM scheduler_run_attempts WHERE id=?", (first.attempt_id,)).fetchone()
            self.assertEqual(old["status"], "interrupted")
            self.assertEqual(json.loads(old["details_json"])["checkpoint"]["page_number"], 7)
        second = durable_runs.claim_run("history_recovery", identity, db_path=self.db)
        self.assertEqual(second.scheduler_run_id, first.scheduler_run_id)
        self.assertNotEqual(second.attempt_id, first.attempt_id)
        self.assertNotEqual(second.owner_token, first.owner_token)
        with connect(self.db) as connection, transaction(connection), self.assertRaises(durable_runs.LostOwnership):
            durable_runs.checkpoint(connection, first, {"cursor": 999})
        self.assertEqual(durable_runs.get_run(second.scheduler_run_id, db_path=self.db)["details"]["checkpoint"]["cursor"], 420)

    def test_openapi_reconcile_uses_free_job_id_and_persists_receipts(self) -> None:
        occurrence = datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI)
        calls = []

        def runner(*, scheduled_for, db_path):
            calls.append((scheduled_for, db_path))
            return self._authorization_details(
                [
                    self._authorization_account_receipt(
                        account_id=7,
                        platform_uid="99887766",
                    )
                ]
            )

        result = execute_douyin_openapi_reconcile(
            occurrence,
            db_path=self.db,
            runner=runner,
        )
        duplicate = execute_douyin_openapi_reconcile(
            occurrence,
            db_path=self.db,
            runner=runner,
        )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["details"]["accounts"][0]["status"], "available")
        self.assertEqual(
            result["details"]["accounts"][0]["authorization_id"], f"{7:032x}"
        )
        self.assertEqual(result["details"]["authorization_state"], "available")
        self.assertFalse(result["details"]["content_sync_enabled"])
        self.assertEqual(duplicate["status"], "skipped_duplicate")
        self.assertEqual(calls, [(occurrence, self.db)])
        with self.assertRaisesRegex(SchedulerJobError, "unknown scheduler job"):
            execute_job(
                DOUYIN_OPENAPI_RECONCILE_JOB_ID,
                occurrence,
                db_path=self.db,
                reports_root=self.reports,
            )
        with connect(self.db) as connection:
            run = connection.execute(
                """
                SELECT status,details_json FROM scheduler_runs
                WHERE job_id=? AND scheduled_for='authorization-status-v1:2026-08-01T18:00:00Z'
                """,
                (DOUYIN_OPENAPI_RECONCILE_JOB_ID,),
            ).fetchone()
            attempts = connection.execute(
                """
                SELECT COUNT(*) FROM scheduler_run_attempts a
                JOIN scheduler_runs r ON r.id=a.scheduler_run_id
                WHERE r.job_id=?
                """,
                (DOUYIN_OPENAPI_RECONCILE_JOB_ID,),
            ).fetchone()[0]
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual(json.loads(run["details_json"])["status"], "succeeded")
        self.assertEqual(attempts, 1)

    def test_openapi_reconcile_statuses_and_hourly_failed_retry(self) -> None:
        occurrence = datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI)
        failed = execute_douyin_openapi_reconcile(
            occurrence,
            db_path=self.db,
            runner=lambda **_kwargs: self._authorization_details(
                [
                    self._authorization_account_receipt(
                        account_id=7,
                        platform_uid="99887766",
                        status="attention",
                    )
                ]
            ),
        )
        self.assertEqual(failed["status"], "failed")

        recovered = douyin_openapi_reconcile_guard(
            now=datetime(2026, 8, 2, 3, 0, tzinfo=SHANGHAI),
            effective_from=date(2026, 8, 2),
            db_path=self.db,
            runner=lambda **_kwargs: self._authorization_details(
                [
                    self._authorization_account_receipt(
                        account_id=7,
                        platform_uid="99887766",
                    )
                ]
            ),
        )
        self.assertEqual((recovered["status"], recovered["attempt_number"]), ("succeeded", 2))

        skipped = execute_douyin_openapi_reconcile(
            datetime(2026, 8, 3, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            runner=lambda **_kwargs: self._authorization_details([]),
        )
        partial = execute_douyin_openapi_reconcile(
            datetime(2026, 8, 4, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            runner=lambda **_kwargs: self._authorization_details(
                [
                    self._authorization_account_receipt(
                        account_id=7,
                        platform_uid="99887766",
                    ),
                    self._authorization_account_receipt(
                        account_id=8,
                        platform_uid="99887767",
                        status="attention",
                    ),
                ]
            ),
        )
        self.assertEqual(skipped["status"], "skipped")
        self.assertEqual(skipped["details"]["authorization_state"], "no_authorization")
        self.assertEqual(skipped["details"]["reason"], "no_authorization")
        self.assertEqual(partial["status"], "partial")

    def test_openapi_authorization_faults_are_isolated_and_bound_to_health_runs(self) -> None:
        authorization_a = "a" * 32
        authorization_b = "b" * 32
        authorization_mismatch = "c" * 32
        first_occurrence = datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI)
        first_receipt = self._authorization_details(
            [
                self._authorization_account_receipt(
                    account_id=7,
                    platform_uid="99887766",
                    status="attention",
                    authorization_id=authorization_a,
                ),
                self._authorization_account_receipt(
                    account_id=8,
                    platform_uid="99887767",
                    authorization_id=authorization_b,
                ),
                {
                    **self._authorization_account_receipt(
                        account_id=9,
                        platform_uid="99887768",
                        authorization_id=authorization_mismatch,
                    ),
                    "status": "attention",
                    "identity_matches": False,
                    "reason": "authorization_identity_mismatch",
                },
            ]
        )
        first = execute_douyin_openapi_reconcile(
            first_occurrence,
            db_path=self.db,
            runner=lambda **_kwargs: first_receipt,
        )
        self.assertEqual(first["status"], "partial")

        with connect(self.db) as connection:
            first_run_id = int(
                connection.execute(
                    "SELECT id FROM scheduler_runs WHERE job_id=? ORDER BY id LIMIT 1",
                    (DOUYIN_OPENAPI_RECONCILE_JOB_ID,),
                ).fetchone()[0]
            )
            fault_a = fault_state(
                connection,
                scope_kind="authorization_hard",
                provider="douyin_openapi",
                authorization_id=authorization_a,
                fault_class="account_authorization",
            )
            fault_b = fault_state(
                connection,
                scope_kind="authorization_hard",
                provider="douyin_openapi",
                authorization_id=authorization_b,
                fault_class="account_authorization",
            )
            mismatch_fault = fault_state(
                connection,
                scope_kind="authorization_hard",
                provider="douyin_openapi",
                authorization_id=authorization_mismatch,
                fault_class="account_authorization",
            )
        self.assertIsNotNone(fault_a)
        assert fault_a is not None
        self.assertTrue(fault_a["open"])
        self.assertEqual(fault_a["reason"], "reauthorization_required")
        self.assertEqual(fault_a["state_evidence"]["scheduler_run_id"], first_run_id)
        self.assertIsNone(fault_b)
        self.assertIsNone(mismatch_fault)

        second_occurrence = datetime(2026, 8, 3, 2, 0, tzinfo=SHANGHAI)
        second = execute_douyin_openapi_reconcile(
            second_occurrence,
            db_path=self.db,
            runner=lambda **_kwargs: self._authorization_details(
                [
                    self._authorization_account_receipt(
                        account_id=7,
                        platform_uid="99887766",
                        authorization_id=authorization_a,
                    ),
                    self._authorization_account_receipt(
                        account_id=8,
                        platform_uid="99887767",
                        status="attention",
                        authorization_id=authorization_b,
                    ),
                ]
            ),
        )
        self.assertEqual(second["status"], "partial")
        with connect(self.db) as connection:
            second_run_id = int(
                connection.execute(
                    "SELECT id FROM scheduler_runs WHERE job_id=? ORDER BY id DESC LIMIT 1",
                    (DOUYIN_OPENAPI_RECONCILE_JOB_ID,),
                ).fetchone()[0]
            )
            recovered_a = fault_state(
                connection,
                scope_kind="authorization_hard",
                provider="douyin_openapi",
                authorization_id=authorization_a,
                fault_class="account_authorization",
            )
            fault_b = fault_state(
                connection,
                scope_kind="authorization_hard",
                provider="douyin_openapi",
                authorization_id=authorization_b,
                fault_class="account_authorization",
            )
        self.assertIsNotNone(recovered_a)
        self.assertIsNotNone(fault_b)
        assert recovered_a is not None and fault_b is not None
        self.assertFalse(recovered_a["open"])
        self.assertEqual(recovered_a["recovery_evidence_id"], second_run_id)
        self.assertTrue(fault_b["open"])
        self.assertEqual(fault_b["state_evidence"]["scheduler_run_id"], second_run_id)

    def test_openapi_all_actionable_authorization_reasons_open_account_faults(self) -> None:
        authorization_ids = {
            "reauthorization_required": "d" * 32,
            "refresh_expired": "e" * 32,
            "access_expired": "f" * 32,
        }
        accounts = []
        for index, (reason, authorization_id) in enumerate(authorization_ids.items(), 20):
            account = self._authorization_account_receipt(
                account_id=index,
                platform_uid=f"9988{index:04d}",
                status="attention",
                authorization_id=authorization_id,
            )
            if reason == "refresh_expired":
                account.update(
                    needs_reauthorization=False,
                    refresh_expires_at=0,
                    reason=reason,
                )
            elif reason == "access_expired":
                account.update(
                    needs_reauthorization=False,
                    access_expires_at=0,
                    reason=reason,
                )
            accounts.append(account)

        result = execute_douyin_openapi_reconcile(
            datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            runner=lambda **_kwargs: self._authorization_details(accounts),
        )
        self.assertEqual(result["status"], "failed")
        with connect(self.db) as connection:
            for reason, authorization_id in authorization_ids.items():
                with self.subTest(reason=reason):
                    current = fault_state(
                        connection,
                        scope_kind="authorization_hard",
                        provider="douyin_openapi",
                        authorization_id=authorization_id,
                        fault_class="account_authorization",
                    )
                    self.assertIsNotNone(current)
                    assert current is not None
                    self.assertTrue(current["open"])
                    self.assertEqual(current["reason"], reason)

    def test_new_authorization_contract_runs_at_cutover_without_mutating_old_success(self) -> None:
        legacy_details = self._openapi_details([self._openapi_account_receipt(
            account_id=7, platform_uid="99887766", pages_fetched=4, items_discovered=30,
        )])
        self._insert_openapi_run(
            local_day=date(2026, 8, 2), status="succeeded", details=legacy_details,
        )
        with connect(self.db) as connection:
            before = dict(connection.execute("SELECT * FROM scheduler_runs").fetchone())
        cutover = datetime(2026, 8, 2, 12, 34, 56, tzinfo=SHANGHAI)
        calls = []

        def runner(**kwargs):
            calls.append(kwargs)
            receipt = self._authorization_details([self._authorization_account_receipt(
                account_id=7, platform_uid="99887766",
            )])
            receipt["captured_at"] = "2026-08-02T04:35:00Z"
            return receipt

        result = douyin_openapi_reconcile_guard(
            now=cutover, effective_from=cutover.date(), db_path=self.db, runner=runner,
        )
        duplicate = douyin_openapi_reconcile_guard(
            now=cutover + timedelta(hours=1), effective_from=cutover.date(), db_path=self.db, runner=runner,
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["details"]["contract_version"], "authorization-status-v1")
        self.assertEqual(result["details"]["scheduled_for"], "2026-08-02T04:34:56Z")
        self.assertEqual(result["details"]["logical_scheduled_for"], "2026-08-01T18:00:00Z")
        self.assertEqual(result["details"]["captured_at"], "2026-08-02T04:35:00Z")
        self.assertEqual(duplicate["status"], "skipped_duplicate")
        self.assertEqual(calls, [{"scheduled_for": cutover, "db_path": self.db}])
        with connect(self.db) as connection:
            self.assertEqual(dict(connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (before["id"],)).fetchone()), before)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0], 2)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_run_attempts").fetchone()[0], 1)
            for table in ("content_items", "content_metric_snapshots", "content_metric_observations", "fetch_slots", "fetch_attempts", "provider_usage"):
                self.assertEqual(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0, table)

    def test_first_authorization_check_before_two_does_not_consume_next_daily_slot(self) -> None:
        calls = []

        def runner(**kwargs):
            calls.append(kwargs["scheduled_for"])
            return self._authorization_details([])

        cutover = datetime(2026, 8, 2, 1, 15, tzinfo=SHANGHAI)
        first = douyin_openapi_reconcile_guard(
            now=cutover, effective_from=cutover.date(), db_path=self.db, runner=runner,
        )
        two_oclock = cutover.replace(hour=2, minute=0)
        second = douyin_openapi_reconcile_guard(
            now=two_oclock, effective_from=cutover.date(), db_path=self.db, runner=runner,
        )
        self.assertEqual((first["status"], second["status"]), ("skipped", "skipped"))
        self.assertEqual(calls, [cutover, two_oclock])
        self.assertNotEqual(first["details"]["logical_scheduled_for"], second["details"]["logical_scheduled_for"])

    def test_openapi_reconcile_missing_environment_defers_without_claim(self) -> None:
        occurrence = datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI)
        missing_environment = self.root / "missing-douyin-sync.env"

        with patch(
            "v8.douyin_openapi_client.DEFAULT_ENV_PATH",
            missing_environment,
        ):
            scheduled = execute_douyin_openapi_reconcile(
                occurrence,
                db_path=self.db,
            )
            guarded = douyin_openapi_reconcile_guard(
                now=datetime(2026, 8, 2, 3, 0, tzinfo=SHANGHAI),
                effective_from=date(2026, 8, 2),
                db_path=self.db,
            )

        self.assertEqual(
            scheduled,
            {
                "job_id": DOUYIN_OPENAPI_RECONCILE_JOB_ID,
                "status": "deferred",
                "reason": "douyin_sync_environment_not_installed",
            },
        )
        self.assertEqual(guarded, scheduled)
        with connect(self.db) as connection:
            run_count = connection.execute(
                "SELECT COUNT(*) FROM scheduler_runs WHERE job_id=?",
                (DOUYIN_OPENAPI_RECONCILE_JOB_ID,),
            ).fetchone()[0]
            attempt_count = connection.execute(
                """
                SELECT COUNT(*) FROM scheduler_run_attempts a
                JOIN scheduler_runs r ON r.id=a.scheduler_run_id
                WHERE r.job_id=?
                """,
                (DOUYIN_OPENAPI_RECONCILE_JOB_ID,),
            ).fetchone()[0]
        self.assertEqual((run_count, attempt_count), (0, 0))

        failed = execute_douyin_openapi_reconcile(
            occurrence,
            db_path=self.db,
            runner=lambda **_kwargs: self._authorization_details(
                [
                    self._authorization_account_receipt(
                        account_id=7,
                        platform_uid="99887766",
                        status="attention",
                    )
                ]
            ),
        )
        self.assertEqual((failed["status"], failed["attempt_number"]), ("failed", 1))
        with patch(
            "v8.douyin_openapi_client.DEFAULT_ENV_PATH",
            missing_environment,
        ):
            deferred_retry = douyin_openapi_reconcile_guard(
                now=datetime(2026, 8, 2, 4, 0, tzinfo=SHANGHAI),
                effective_from=date(2026, 8, 2),
                db_path=self.db,
            )
        self.assertEqual(deferred_retry, scheduled)
        with connect(self.db) as connection:
            stored = connection.execute(
                """
                SELECT r.status,COUNT(a.id) AS attempt_count
                FROM scheduler_runs r
                JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id
                WHERE r.job_id=?
                GROUP BY r.id
                """,
                (DOUYIN_OPENAPI_RECONCILE_JOB_ID,),
            ).fetchone()
        self.assertEqual((stored["status"], stored["attempt_count"]), ("failed", 1))

    def test_openapi_reconcile_rejects_extra_fields_and_scrubs_exceptions(self) -> None:
        occurrence = datetime(2026, 8, 5, 2, 0, tzinfo=SHANGHAI)
        leaked = self._authorization_details(
            [
                {
                    **self._authorization_account_receipt(
                        account_id=7,
                        platform_uid="99887766",
                    ),
                    "access_token": "forbidden",
                }
            ]
        )
        with self.assertRaisesRegex(SchedulerJobError, "fields are invalid"):
            execute_douyin_openapi_reconcile(
                occurrence,
                db_path=self.db,
                runner=lambda **_kwargs: leaked,
            )
        with connect(self.db) as connection:
            details = connection.execute(
                """
                SELECT details_json FROM scheduler_runs
                WHERE job_id=? AND scheduled_for=?
                """,
                (
                    DOUYIN_OPENAPI_RECONCILE_JOB_ID,
                    "authorization-status-v1:2026-08-04T18:00:00Z",
                ),
            ).fetchone()["details_json"]
        self.assertNotIn("access_token", details)
        self.assertEqual(
            json.loads(details),
            {"contract_version": "authorization-status-v1",
             "scheduled_for": "2026-08-04T18:00:00Z",
             "logical_scheduled_for": "2026-08-04T18:00:00Z",
             "error_code": "douyin_openapi_reconcile_failed"},
        )

        secret = "MACHINE-KEY-CANARY"
        with self.assertRaisesRegex(RuntimeError, secret):
            execute_douyin_openapi_reconcile(
                datetime(2026, 8, 6, 2, 0, tzinfo=SHANGHAI),
                db_path=self.db,
                runner=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError(secret)),
            )
        with connect(self.db) as connection:
            stored = connection.execute(
                """
                SELECT details_json FROM scheduler_runs
                WHERE job_id=? AND scheduled_for=?
                """,
                (
                    DOUYIN_OPENAPI_RECONCILE_JOB_ID,
                    "authorization-status-v1:2026-08-05T18:00:00Z",
                ),
            ).fetchone()["details_json"]
        self.assertNotIn(secret, stored)
        self.assertEqual(
            json.loads(stored),
            {"contract_version": "authorization-status-v1",
             "scheduled_for": "2026-08-05T18:00:00Z",
             "logical_scheduled_for": "2026-08-05T18:00:00Z",
             "error_code": "douyin_openapi_reconcile_failed"},
        )

    def test_authorization_health_rejects_legacy_contract_and_false_empty_health(self) -> None:
        valid_account = self._authorization_account_receipt(
            account_id=7,
            platform_uid="99887766",
        )
        cases = (
            self._openapi_details([]),
            {**self._authorization_details([]), "contract_version": "douyin-authorization-health-v1"},
            {**self._authorization_details([]), "authorization_state": "available"},
            {**self._authorization_details([]), "captured_at": "2026-08-02T00:00:00"},
            {**self._authorization_details([]), "content_sync_enabled": True},
            self._authorization_details(
                [{**valid_account, "authorization_id": "A" * 32}]
            ),
            self._authorization_details(
                [valid_account, {**valid_account, "account_id": 8, "platform_uid": "99887767"}]
            ),
        )
        for index, receipt in enumerate(cases):
            with self.subTest(index=index), self.assertRaises(SchedulerJobError):
                execute_douyin_openapi_reconcile(
                    datetime(2026, 8, 7 + index, 2, tzinfo=SHANGHAI),
                    db_path=self.db, runner=lambda **_kwargs: receipt,
                )
        with connect(self.db) as connection:
            rows = connection.execute("SELECT status,details_json FROM scheduler_runs").fetchall()
            self.assertEqual(len(rows), len(cases))
            for row in rows:
                self.assertEqual(row["status"], "failed")
                details = json.loads(row["details_json"])
                self.assertEqual(details["contract_version"], "authorization-status-v1")
                self.assertEqual(details["error_code"], "douyin_openapi_reconcile_failed")
                self.assertNotIn("authorization_state", details)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)

    def test_interruption_recovery_is_generic_for_openapi_job(self) -> None:
        occurrence = datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI)
        claim = _claim_run(
            DOUYIN_OPENAPI_RECONCILE_JOB_ID,
            occurrence,
            db_path=self.db,
            allow_retry=False,
            invocation_source="scheduled",
        )
        self.assertIsNotNone(claim)
        self.assertEqual(recover_interrupted_scheduler_runs(db_path=self.db), 1)
        with connect(self.db) as connection:
            run = connection.execute(
                "SELECT status,details_json FROM scheduler_runs WHERE job_id=?",
                (DOUYIN_OPENAPI_RECONCILE_JOB_ID,),
            ).fetchone()
        self.assertEqual(run["status"], "interrupted")
        self.assertEqual(
            json.loads(run["details_json"])["reason"],
            "writer_process_restarted",
        )

    def test_current_day_guard_never_scans_yesterday_or_before_completion_buffer(
        self,
    ) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id,scheduled_for,status,started_at,completed_at,details_json
                ) VALUES ('daily_capture','2026-08-18T18:00:00Z','failed',
                          '2026-08-18T18:00:01Z','2026-08-18T18:01:00Z','{}')
                """
            )
            connection.commit()
        with patch("v8.scheduler.execute_job") as action:
            result = current_day_daily_capture_guard(
                now=datetime(2026, 8, 20, 1, 59, tzinfo=SHANGHAI),
                effective_from=date(2026, 8, 20),
                db_path=self.db,
                reports_root=self.reports,
                capture_call_override=None,
            )
        self.assertEqual(result["status"], "before_today_slot")
        action.assert_not_called()
        with patch("v8.scheduler.execute_job") as buffered_action:
            buffered = current_day_daily_capture_guard(
                now=datetime(2026, 8, 20, 2, 9, tzinfo=SHANGHAI),
                effective_from=date(2026, 8, 20),
                db_path=self.db,
                reports_root=self.reports,
                capture_call_override=None,
            )
        self.assertEqual(buffered["status"], "before_today_slot")
        self.assertEqual(buffered["scheduled_for"], "2026-08-19T18:00:00Z")
        buffered_action.assert_not_called()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs WHERE job_id='daily_capture'"
                ).fetchone()[0],
                1,
            )

    def test_current_day_guard_calls_exact_today_slot_after_two(self) -> None:
        expected = {"job_id": "daily_capture", "status": "succeeded"}
        with patch("v8.scheduler.execute_job", return_value=expected) as action:
            result = current_day_daily_capture_guard(
                now=datetime(2026, 8, 20, 11, 5, tzinfo=SHANGHAI),
                effective_from=date(2026, 8, 20),
                db_path=self.db,
                reports_root=self.reports,
                capture_call_override=None,
            )
        self.assertEqual(result, expected)
        args, kwargs = action.call_args
        self.assertEqual(args[:2], ("daily_capture", datetime(2026, 8, 20, 2, 0, tzinfo=SHANGHAI)))
        self.assertEqual(kwargs["db_path"], self.db)
        self.assertEqual(kwargs["reports_root"], self.reports)
        self.assertIsNone(kwargs["capture_call_override"])
        self.assertFalse(kwargs["allow_retry"])
        self.assertEqual(kwargs["invocation_source"], "scheduled")

    def test_current_day_guard_is_zero_write_before_effective_date(self) -> None:
        with patch("v8.scheduler.execute_job") as action:
            result = current_day_daily_capture_guard(
                now=datetime(2026, 8, 20, 12, 0, tzinfo=SHANGHAI),
                effective_from=date(2026, 8, 21),
                db_path=self.db,
                reports_root=self.reports,
                capture_call_override=None,
            )
        self.assertEqual(result["status"], "before_effective_date")
        action.assert_not_called()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs WHERE job_id='daily_capture'"
                ).fetchone()[0],
                0,
            )

    def test_current_day_guard_treats_every_existing_status_as_attempted(self) -> None:
        statuses = (
            "running",
            "succeeded",
            "partial",
            "skipped",
            "failed",
            "interrupted",
        )
        with patch("v8.scheduler.execute_job") as action:
            for offset, status in enumerate(statuses):
                local_day = date(2026, 8, 20) + timedelta(days=offset)
                scheduled_for = datetime.combine(
                    local_day, datetime.min.time().replace(hour=2), SHANGHAI
                )
                started = scheduled_for.astimezone(ZoneInfo("UTC")).isoformat().replace(
                    "+00:00", "Z"
                )
                with connect(self.db) as connection:
                    connection.execute(
                        """
                        INSERT INTO scheduler_runs(
                            job_id,scheduled_for,status,started_at,completed_at,details_json
                        ) VALUES ('daily_capture',?,?,?,?, '{}')
                        """,
                        (
                            started,
                            status,
                            started,
                            None if status == "running" else started,
                        ),
                    )
                    connection.commit()
                result = current_day_daily_capture_guard(
                    now=datetime.combine(
                        local_day, datetime.min.time().replace(hour=12), SHANGHAI
                    ),
                    effective_from=date(2026, 8, 20),
                    db_path=self.db,
                    reports_root=self.reports,
                    capture_call_override=None,
                )
                self.assertEqual(
                    (result["status"], result["existing_status"]),
                    ("already_attempted", status),
                )
        action.assert_not_called()

    def test_hourly_pipeline_reconciles_all_due_jobs_in_dependency_order(self) -> None:
        current = datetime(2026, 8, 22, 9, 0, tzinfo=SHANGHAI)
        with (
            patch(
                "v8.scheduler._run_job_action",
                return_value=("succeeded", {}),
            ),
            patch(
                "v8.scheduler._require_report_job_runtime_ready",
                return_value={},
            ),
        ):
            result = current_day_pipeline_guard(
                now=current,
                effective_from=current.date(),
                db_path=self.db,
                reports_root=self.reports,
                capture_call_override=None,
            )

        self.assertEqual(result["status"], "reconciled")
        self.assertEqual(
            [item["job_id"] for item in result["results"]],
            [
                "daily_capture",
                "daily_media_download",
                "daily_media_processing",
                "daily_media_cutoff",
                "daily_report",
            ],
        )
        with connect(self.db) as connection:
            rows = connection.execute(
                """
                SELECT r.job_id,r.status,a.invocation_source
                FROM scheduler_runs r
                JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id
                ORDER BY r.scheduled_for,r.id
                """
            ).fetchall()
        self.assertEqual([str(row["status"]) for row in rows], ["succeeded"] * 5)
        self.assertEqual(
            [str(row["invocation_source"]) for row in rows],
            [
                "scheduled",
                "scheduled",
                "scheduled",
                "scheduled",
                "startup_report_catchup",
            ],
        )

    def test_hourly_pipeline_reruns_only_stale_today_downstream_chain(self) -> None:
        current = datetime(2026, 8, 22, 9, 0, tzinfo=SHANGHAI)
        rows = (
            (
                "daily_capture",
                "2026-08-21T18:00:00Z",
                "partial",
                "2026-08-21T20:27:00Z",
            ),
            (
                "daily_media_download",
                "2026-08-21T18:20:00Z",
                "succeeded",
                "2026-08-21T21:00:00Z",
            ),
            (
                "daily_media_processing",
                "2026-08-21T19:00:00Z",
                "succeeded",
                "2026-08-21T23:50:00Z",
            ),
            (
                "daily_media_cutoff",
                "2026-08-21T23:30:00Z",
                "succeeded",
                "2026-08-21T23:55:00Z",
            ),
            (
                "daily_report",
                "2026-08-22T00:00:00Z",
                "partial",
                "2026-08-22T00:05:00Z",
            ),
        )
        with connect(self.db) as connection:
            for job_id, scheduled_for, status, completed_at in rows:
                run = connection.execute(
                    """
                    INSERT INTO scheduler_runs(
                        job_id,scheduled_for,status,started_at,completed_at,details_json
                    ) VALUES (?,?,?,?,?,'{}')
                    """,
                    (job_id, scheduled_for, status, scheduled_for, completed_at),
                )
                connection.execute(
                    """
                    INSERT INTO scheduler_run_attempts(
                        scheduler_run_id,attempt_number,invocation_source,status,
                        started_at,completed_at,details_json
                    ) VALUES (?,1,'scheduled',?,?,?,'{}')
                    """,
                    (
                        int(run.lastrowid),
                        status,
                        scheduled_for,
                        completed_at,
                    ),
                )
            historical = connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id,scheduled_for,status,started_at,completed_at,details_json
                ) VALUES ('daily_media_cutoff','2026-08-20T23:30:00Z','succeeded',
                          '2026-08-20T23:30:00Z','2026-08-20T23:45:00Z','{}')
                """
            )
            connection.execute(
                """
                INSERT INTO scheduler_run_attempts(
                    scheduler_run_id,attempt_number,invocation_source,status,
                    started_at,completed_at,details_json
                ) VALUES (?,1,'scheduled','succeeded','2026-08-20T23:30:00Z',
                          '2026-08-20T23:45:00Z','{}')
                """,
                (int(historical.lastrowid),),
            )
            connection.commit()

        report_task = {
            "id": "D8-D-20260821-20260821",
            "task_status": "partial",
            "completed_at": "2026-08-22T00:05:00Z",
        }
        with (
            patch("v8.scheduler.run_media_cutoff", return_value={}),
            patch(
                "v8.scheduler._require_report_job_runtime_ready", return_value={}
            ),
            patch("v8.scheduler.create_task", return_value=report_task) as create,
            patch("v8.scheduler.retry_task", return_value={}) as retry,
            patch(
                "v8.scheduler.create_and_run_task", return_value=report_task
            ) as create_and_run,
        ):
            result = current_day_pipeline_guard(
                now=current,
                effective_from=current.date(),
                db_path=self.db,
                reports_root=self.reports,
                capture_call_override=None,
            )

        self.assertEqual(result["status"], "reconciled")
        create.assert_called_once()
        retry.assert_called_once_with(report_task["id"], db_path=self.db)
        create_and_run.assert_called_once()
        with connect(self.db) as connection:
            attempts = {
                str(row["job_id"]): int(row["attempts"])
                for row in connection.execute(
                    """
                    SELECT r.job_id,COUNT(*) attempts
                    FROM scheduler_runs r
                    JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id
                    WHERE r.scheduled_for>='2026-08-21T18:00:00Z'
                    GROUP BY r.job_id
                    """
                )
            }
            historical_attempts = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM scheduler_run_attempts a
                    JOIN scheduler_runs r ON r.id=a.scheduler_run_id
                    WHERE r.job_id='daily_media_cutoff'
                      AND r.scheduled_for='2026-08-20T23:30:00Z'
                    """
                ).fetchone()[0]
            )
        self.assertEqual(
            attempts,
            {
                "daily_capture": 1,
                "daily_media_download": 2,
                "daily_media_processing": 2,
                "daily_media_cutoff": 2,
                "daily_report": 2,
            },
        )
        self.assertEqual(historical_attempts, 1)

    def test_interrupted_stale_report_retry_preserves_revision_intent(self) -> None:
        occurrence = datetime(2026, 8, 22, 8, 0, tzinfo=SHANGHAI)
        scheduled_for = "2026-08-22T00:00:00Z"
        dependency_completed = datetime.fromisoformat("2026-08-22T00:10:00+00:00")
        with connect(self.db) as connection:
            run = connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id,scheduled_for,status,started_at,completed_at,details_json
                ) VALUES ('daily_report',?,'partial',?,?, '{}')
                """,
                (
                    scheduled_for,
                    scheduled_for,
                    "2026-08-22T00:05:00Z",
                ),
            )
            connection.execute(
                """
                INSERT INTO scheduler_run_attempts(
                    scheduler_run_id,attempt_number,invocation_source,status,
                    started_at,completed_at,details_json
                ) VALUES (?,1,'startup_report_catchup','partial',?,?, '{}')
                """,
                (
                    int(run.lastrowid),
                    scheduled_for,
                    "2026-08-22T00:05:00Z",
                ),
            )
            connection.commit()

        claim = _claim_run(
            "daily_report",
            occurrence,
            db_path=self.db,
            allow_retry=True,
            invocation_source="startup_report_catchup",
            retry_terminal_if_completed_before=dependency_completed,
        )
        self.assertIsNotNone(claim)
        self.assertEqual(recover_interrupted_scheduler_runs(db_path=self.db), 1)

        report_task = {
            "id": "D8-D-20260821-20260821",
            "task_status": "partial",
            "completed_at": "2026-08-22T00:05:00Z",
        }
        with (
            patch(
                "v8.scheduler._require_report_job_runtime_ready", return_value={}
            ),
            patch("v8.scheduler.create_task", return_value=report_task),
            patch("v8.scheduler.retry_task", return_value={}) as retry,
            patch(
                "v8.scheduler.create_and_run_task", return_value=report_task
            ),
        ):
            result = execute_job(
                "daily_report",
                occurrence,
                db_path=self.db,
                reports_root=self.reports,
                allow_retry=True,
                invocation_source="startup_report_catchup",
            )

        self.assertEqual((result["status"], result["attempt_number"]), ("partial", 3))
        retry.assert_called_once_with(report_task["id"], db_path=self.db)
        with connect(self.db) as connection:
            attempts = connection.execute(
                """
                SELECT attempt_number,status FROM scheduler_run_attempts
                ORDER BY attempt_number
                """
            ).fetchall()
        self.assertEqual(
            [tuple(row) for row in attempts],
            [(1, "partial"), (2, "interrupted"), (3, "partial")],
        )

    def test_failed_stale_report_retry_preserves_revision_intent(self) -> None:
        occurrence = datetime(2026, 8, 22, 8, 0, tzinfo=SHANGHAI)
        dependency_completed = datetime.fromisoformat("2026-08-22T00:10:00+00:00")
        with connect(self.db) as connection:
            run = connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id,scheduled_for,status,started_at,completed_at,details_json
                ) VALUES ('daily_report','2026-08-22T00:00:00Z','partial',
                          '2026-08-22T00:00:00Z','2026-08-22T00:05:00Z','{}')
                """
            )
            connection.execute(
                """
                INSERT INTO scheduler_run_attempts(
                    scheduler_run_id,attempt_number,invocation_source,status,
                    started_at,completed_at,details_json
                ) VALUES (?,1,'startup_report_catchup','partial',
                          '2026-08-22T00:00:00Z','2026-08-22T00:05:00Z','{}')
                """,
                (int(run.lastrowid),),
            )
            connection.commit()

        with (
            patch(
                "v8.scheduler._require_report_job_runtime_ready", return_value={}
            ),
            patch(
                "v8.scheduler._run_job_action",
                side_effect=RuntimeError("report action failed before retry_task"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "before retry_task"):
                execute_job(
                    "daily_report",
                    occurrence,
                    db_path=self.db,
                    reports_root=self.reports,
                    allow_retry=True,
                    invocation_source="startup_report_catchup",
                    retry_terminal_if_completed_before=dependency_completed,
                )

        report_task = {
            "id": "D8-D-20260821-20260821",
            "task_status": "partial",
            "completed_at": "2026-08-22T00:05:00Z",
        }
        with (
            patch(
                "v8.scheduler._require_report_job_runtime_ready", return_value={}
            ),
            patch("v8.scheduler.create_task", return_value=report_task),
            patch("v8.scheduler.retry_task", return_value={}) as retry,
            patch(
                "v8.scheduler.create_and_run_task", return_value=report_task
            ),
        ):
            result = execute_job(
                "daily_report",
                occurrence,
                db_path=self.db,
                reports_root=self.reports,
                allow_retry=True,
                invocation_source="startup_report_catchup",
            )

        self.assertEqual((result["status"], result["attempt_number"]), ("partial", 3))
        retry.assert_called_once_with(report_task["id"], db_path=self.db)
        with connect(self.db) as connection:
            attempts = connection.execute(
                """
                SELECT attempt_number,status FROM scheduler_run_attempts
                ORDER BY attempt_number
                """
            ).fetchall()
        self.assertEqual(
            [tuple(row) for row in attempts],
            [(1, "partial"), (2, "failed"), (3, "partial")],
        )

    def test_hourly_pipeline_retries_interrupted_capture_with_same_day_slot(self) -> None:
        occurrence = datetime(2026, 8, 22, 2, 0, tzinfo=SHANGHAI)
        with connect(self.db) as connection:
            run = connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id,scheduled_for,status,started_at,details_json
                ) VALUES ('daily_capture',?,'running',?,'{}')
                """,
                (
                    occurrence.astimezone(ZoneInfo("UTC"))
                    .isoformat(timespec="seconds")
                    .replace("+00:00", "Z"),
                    now_utc(),
                ),
            )
            connection.execute(
                """
                INSERT INTO scheduler_run_attempts(
                    scheduler_run_id,attempt_number,invocation_source,status,
                    started_at,details_json
                ) VALUES (?,1,'scheduled','running',?,'{}')
                """,
                (int(run.lastrowid), now_utc()),
            )
            connection.commit()
        recover_interrupted_scheduler_runs(db_path=self.db)

        with patch(
            "v8.scheduler._run_job_action", return_value=("succeeded", {})
        ):
            result = current_day_pipeline_guard(
                now=datetime(2026, 8, 22, 2, 10, tzinfo=SHANGHAI),
                effective_from=date(2026, 8, 22),
                db_path=self.db,
                reports_root=self.reports,
                capture_call_override=None,
            )

        self.assertEqual(result["status"], "reconciled")
        with connect(self.db) as connection:
            attempts = connection.execute(
                """
                SELECT attempt_number,invocation_source,status
                FROM scheduler_run_attempts ORDER BY attempt_number
                """
            ).fetchall()
        self.assertEqual(
            [tuple(row) for row in attempts],
            [(1, "scheduled", "interrupted"), (2, "scheduled", "succeeded")],
        )

    def test_scheduled_retry_cannot_reclaim_failed_paid_capture(self) -> None:
        occurrence = datetime(2026, 8, 22, 2, 0, tzinfo=SHANGHAI)
        scheduled_for = (
            occurrence.astimezone(ZoneInfo("UTC"))
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
        with connect(self.db) as connection:
            run = connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id,scheduled_for,status,started_at,completed_at,details_json
                ) VALUES ('daily_capture',?,'failed',?,?, '{}')
                """,
                (scheduled_for, now_utc(), now_utc()),
            )
            connection.execute(
                """
                INSERT INTO scheduler_run_attempts(
                    scheduler_run_id,attempt_number,invocation_source,status,
                    started_at,completed_at,details_json
                ) VALUES (?,1,'scheduled','failed',?,?, '{}')
                """,
                (int(run.lastrowid), now_utc(), now_utc()),
            )
            connection.commit()

        with patch("v8.scheduler._run_job_action") as action:
            result = execute_job(
                "daily_capture",
                occurrence,
                db_path=self.db,
                reports_root=self.reports,
                allow_retry=True,
                invocation_source="scheduled",
            )

        self.assertEqual(result["status"], "skipped_duplicate")
        action.assert_not_called()
        with connect(self.db) as connection:
            attempts = connection.execute(
                "SELECT COUNT(*) FROM scheduler_run_attempts"
            ).fetchone()[0]
        self.assertEqual(attempts, 1)

    def test_current_day_guard_concurrency_claims_one_run_and_attempt(self) -> None:
        start = threading.Barrier(3)
        results: list[dict[str, object]] = []

        def invoke() -> None:
            start.wait()
            results.append(
                current_day_daily_capture_guard(
                    now=datetime(2026, 8, 20, 2, 10, tzinfo=SHANGHAI),
                    effective_from=date(2026, 8, 20),
                    db_path=self.db,
                    reports_root=self.reports,
                    capture_call_override=lambda _operation, _payload: ProviderResult(
                        {}, {}, 200, False
                    ),
                )
            )

        threads = [threading.Thread(target=invoke) for _ in range(2)]
        for thread in threads:
            thread.start()
        start.wait()
        for thread in threads:
            thread.join()
        self.assertEqual(len(results), 2)
        with connect(self.db) as connection:
            run_count = connection.execute(
                """
                SELECT COUNT(*) FROM scheduler_runs
                WHERE job_id='daily_capture' AND scheduled_for='2026-08-19T18:00:00Z'
                """
            ).fetchone()[0]
            attempt_count = connection.execute(
                """
                SELECT COUNT(*) FROM scheduler_run_attempts sra
                JOIN scheduler_runs sr ON sr.id=sra.scheduler_run_id
                WHERE sr.job_id='daily_capture'
                  AND sr.scheduled_for='2026-08-19T18:00:00Z'
                """
            ).fetchone()[0]
        self.assertEqual((run_count, attempt_count), (1, 1))

    def test_daily_capture_quality_gate_uses_selected_cohort_ratios(self) -> None:
        discovery = ([{"status": "succeeded"}] * 178) + ([{"status": "failed"}] * 5)
        baseline = {
            "monitored_accounts": 183,
            "monitored_contents": 3000,
            "discovery": discovery,
            "content_updates": ([{"status": "succeeded"}] * 2189)
            + ([{"status": "partial"}] * 811),
            "blocked_providers": [],
            "budget_max_amount": 8.0,
            "reported_provider_cost": 6.151,
            "ledger_provider_cost": 6.151,
        }
        self.assertTrue(daily_capture_quality_gate(baseline)["passed"])
        poor = {
            **baseline,
            "content_updates": ([{"status": "succeeded"}])
            + ([{"status": "partial"}] * 2999),
        }
        self.assertFalse(daily_capture_quality_gate(poor)["passed"])
        blocked = {**baseline, "blocked_providers": ["TikHub"]}
        self.assertFalse(daily_capture_quality_gate(blocked)["passed"])
        mismatched = {**baseline, "discovery": discovery[:-1]}
        self.assertFalse(daily_capture_quality_gate(mismatched)["passed"])

    def test_daily_capture_quality_gate_rejects_reported_cost_above_ledger(
        self,
    ) -> None:
        details = {
            "monitored_accounts": 1,
            "monitored_contents": 1,
            "discovery": [{"status": "succeeded"}],
            "content_updates": [{"status": "succeeded"}],
            "blocked_providers": [],
            "budget_max_amount": 8.0,
            "reported_provider_cost": 1.001,
            "ledger_provider_cost": 1.0,
        }
        result = daily_capture_quality_gate(details)
        self.assertFalse(result["passed"])
        self.assertFalse(result["checks"]["ledger_covers_reported"])

    def test_daily_capture_quality_gate_rejects_invalid_cost_values(self) -> None:
        baseline = {
            "monitored_accounts": 1,
            "monitored_contents": 1,
            "discovery": [{"status": "succeeded"}],
            "content_updates": [{"status": "succeeded"}],
            "blocked_providers": [],
            "budget_max_amount": 8.0,
            "reported_provider_cost": 1.0,
            "ledger_provider_cost": 1.0,
        }
        for field, value in (
            ("reported_provider_cost", -0.001),
            ("ledger_provider_cost", -0.001),
            ("ledger_provider_cost", float("inf")),
            ("budget_max_amount", float("nan")),
        ):
            with self.subTest(field=field, value=value):
                result = daily_capture_quality_gate({**baseline, field: value})
                self.assertFalse(result["passed"])
                self.assertFalse(result["checks"]["cost_values_valid"])

        for missing_field, check in (
            ("ledger_provider_cost", "ledger_source_present"),
            ("budget_max_amount", "budget_declaration_present"),
        ):
            with self.subTest(missing_field=missing_field):
                incomplete = {**baseline}
                incomplete.pop(missing_field)
                result = daily_capture_quality_gate(incomplete)
                self.assertFalse(result["passed"])
                self.assertFalse(result["checks"][check])

    def test_daily_capture_quality_gate_accepts_production_cost_asymmetry(
        self,
    ) -> None:
        fixture_path = (
            Path(__file__).parent
            / "fixtures"
            / "daily_capture_2026-08-11_quality_gate.json"
        )
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        projection = fixture["quality_gate_projection"]

        def expand_stage(stage: str) -> list[dict[str, object]]:
            return [
                {
                    "status": group["status"],
                    "provider_cost": group["provider_cost"],
                }
                for group in projection["result_groups"][stage]
                for _ in range(group["count"])
            ]

        details = {
            "monitored_accounts": projection["monitored_accounts"],
            "monitored_contents": projection["monitored_contents"],
            "discovery": expand_stage("discovery"),
            "content_updates": expand_stage("content_updates"),
            "blocked_providers": projection["blocked_providers"],
            "budget_max_amount": projection["budget_max_amount"],
            # The historical payload has neither split field. ``provider_cost``
            # is its ledger-authoritative max value; the gate must derive the
            # reported subtotal from the projected result rows above.
            "provider_cost": projection["provider_cost"],
        }

        result = daily_capture_quality_gate(details)

        self.assertTrue(result["passed"], result)
        self.assertTrue(result["checks"]["ledger_covers_reported"])
        self.assertFalse(
            result["cost_reconciliation"]["ledger_exactly_matches_reported"]
        )
        self.assertEqual(result["cost_reconciliation"]["ledger_minus_reported"], 1.445)
        self.assertEqual(result["cost_reconciliation"]["reported_provider_cost"], 4.706)
        self.assertEqual(result["cost_reconciliation"]["ledger_provider_cost"], 6.151)

    def test_daily_capture_quality_gate_enforces_fixed_usd_hundred_contract(
        self,
    ) -> None:
        baseline = {
            "monitored_accounts": 1,
            "monitored_contents": 1,
            "discovery": [{"status": "succeeded"}],
            "content_updates": [{"status": "succeeded"}],
            "blocked_providers": [],
            "budget_max_amount": 8.0,
            "reported_provider_cost": 1.0,
            "ledger_provider_cost": 1.0,
        }
        enlarged_contract = {**baseline, "budget_max_amount": 100.001}
        enlarged_result = daily_capture_quality_gate(enlarged_contract)
        self.assertFalse(enlarged_result["passed"])
        self.assertFalse(enlarged_result["checks"]["budget_contract"])

        overspent = {
            **baseline,
            "budget_max_amount": 100.0,
            "reported_provider_cost": 100.001,
            "ledger_provider_cost": 100.001,
        }
        overspent_result = daily_capture_quality_gate(overspent)
        self.assertFalse(overspent_result["passed"])
        self.assertFalse(overspent_result["checks"]["within_budget"])

    def test_partial_daily_capture_is_terminal_even_for_operator_retry(self) -> None:
        occurrence = datetime(2026, 8, 20, 2, 0, tzinfo=SHANGHAI)
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id,scheduled_for,status,started_at,completed_at,details_json
                ) VALUES ('daily_capture','2026-08-19T18:00:00Z','partial',
                          '2026-08-19T18:00:01Z','2026-08-19T18:01:00Z','{}')
                """
            )
            connection.commit()
        with patch("v8.scheduler._run_job_action") as action:
            result = execute_job(
                "daily_capture",
                occurrence,
                db_path=self.db,
                reports_root=self.reports,
                capture_call_override=lambda _operation, _payload: ProviderResult(
                    {}, {}, 200, False
                ),
                allow_retry=True,
                invocation_source="operator_retry",
            )
        self.assertEqual(result["status"], "skipped_duplicate")
        action.assert_not_called()

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
        self.assertEqual(first["status"], "partial")
        self.assertEqual(second["status"], "skipped_duplicate")
        with connect(self.db) as connection:
            task = connection.execute("SELECT * FROM report_tasks").fetchone()
            run_count = connection.execute(
                "SELECT COUNT(*) FROM scheduler_runs"
            ).fetchone()[0]
            attempt_count = connection.execute(
                "SELECT COUNT(*) FROM scheduler_run_attempts"
            ).fetchone()[0]
            revision = connection.execute(
                """
                SELECT report_json_path FROM report_revisions
                WHERE task_id=? ORDER BY revision DESC LIMIT 1
                """,
                (task["id"],),
            ).fetchone()
        self.assertEqual(task["period_start"], "2026-08-01")
        self.assertEqual(task["period_end"], "2026-08-01")
        self.assertEqual(run_count, 1)
        self.assertEqual(attempt_count, 1)
        report = json.loads((PROJECT_ROOT / revision["report_json_path"]).read_text())
        self.assertEqual(
            report["metadata"]["collection_cutoff_at"], "2026-08-02T00:00:00Z"
        )

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
                WHERE id=?
                """,
                (now_utc(), now_utc(), V9_FIXTURE_RELEASE_ID),
            )
            connection.commit()
        completed = execute_job(
            "daily_report", occurrence, db_path=self.db, reports_root=self.reports
        )
        self.assertEqual(completed["status"], "partial")

    def test_weekly_report_defers_before_claim_until_sunday_daily_is_terminal(
        self,
    ) -> None:
        occurrence = datetime(2026, 8, 24, 8, 30, tzinfo=SHANGHAI)
        deferred = execute_job(
            "weekly_report",
            occurrence,
            db_path=self.db,
            reports_root=self.reports,
            allow_retry=True,
            invocation_source="startup_report_catchup",
        )
        self.assertEqual(deferred["status"], "deferred")
        self.assertEqual(deferred["reason"], "weekly_daily_dependency_not_ready")
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_run_attempts"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM report_tasks").fetchone()[0],
                0,
            )
            connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id,scheduled_for,status,started_at,completed_at,details_json
                ) VALUES ('daily_report','2026-08-24T00:00:00Z','partial',
                          '2026-08-24T00:00:00Z','2026-08-24T00:05:00Z','{}')
                """
            )
            connection.execute(
                """
                INSERT INTO report_tasks(
                    id,task_type,name,period_start,period_end,creation_source,
                    task_status,progress,message,created_at,started_at,completed_at,updated_at
                ) VALUES ('D8-D-20260823-20260823','daily','2026-08-23 日报',
                          '2026-08-23','2026-08-23','automatic','partial',100,'完成',
                          '2026-08-24T00:00:00Z','2026-08-24T00:00:00Z',
                          '2026-08-24T00:05:00Z','2026-08-24T00:05:00Z')
                """
            )
            connection.commit()
        with patch(
            "v8.scheduler._run_job_action",
            return_value=("partial", {"task_id": "weekly", "task_status": "partial"}),
        ):
            completed = execute_job(
                "weekly_report",
                occurrence,
                db_path=self.db,
                reports_root=self.reports,
                allow_retry=True,
                invocation_source="startup_report_catchup",
            )
        self.assertEqual(completed["status"], "partial")

    def test_report_catchup_enumerates_natural_days_from_scheduled_capture(
        self,
    ) -> None:
        with connect(self.db) as connection:
            run_id = int(
                connection.execute(
                    """
                    INSERT INTO scheduler_runs(
                        job_id,scheduled_for,status,started_at,completed_at,details_json
                    ) VALUES ('daily_capture','2026-08-21T18:00:00Z','interrupted',
                              '2026-08-21T18:00:00Z','2026-08-21T18:05:00Z','{}')
                    """
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO scheduler_run_attempts(
                    scheduler_run_id,attempt_number,invocation_source,status,
                    started_at,completed_at,details_json
                ) VALUES (?,1,'scheduled','interrupted',
                          '2026-08-21T18:00:00Z','2026-08-21T18:05:00Z','{}')
                """,
                (run_id,),
            )
            connection.commit()
        occurrences = _report_catchup_occurrences(
            current=datetime(2026, 8, 24, 9, 0, tzinfo=SHANGHAI),
            db_path=self.db,
        )
        self.assertEqual(
            [(job_id, occurrence.isoformat()) for job_id, occurrence in occurrences],
            [
                ("daily_report", "2026-08-22T08:00:00+08:00"),
                ("daily_report", "2026-08-23T08:00:00+08:00"),
                ("daily_report", "2026-08-24T08:00:00+08:00"),
                ("weekly_report", "2026-08-24T08:30:00+08:00"),
            ],
        )
        current = datetime(2026, 8, 24, 9, 0, tzinfo=SHANGHAI)
        self.assertEqual(
            _report_catchup_occurrences(
                current=current, db_path=self.db, effective_from=date(2026, 8, 24),
            ),
            [],
        )
        self.assertEqual(
            _report_catchup_occurrences(
                current=current, db_path=self.db, effective_from=date(2026, 8, 25),
            ),
            [],
        )

    def test_report_catchup_boundary_keeps_old_failures_and_terminal_reports_untouched(self) -> None:
        effective_from = date(2026, 8, 24)
        current = datetime(2026, 8, 25, 9, 0, tzinfo=SHANGHAI)
        rows = [
            ("daily_report", "2026-08-22T00:00:00Z", "partial"),
            ("daily_report", "2026-08-23T00:00:00Z", "failed"),
            ("weekly_report", "2026-08-17T00:30:00Z", "interrupted"),
            ("daily_report", "2026-08-24T00:00:00Z", "failed"),
            ("weekly_report", "2026-08-24T00:30:00Z", "interrupted"),
            ("daily_report", "2026-08-25T00:00:00Z", "failed"),
        ]
        with connect(self.db) as connection, transaction(connection):
            connection.executemany(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json) "
                "VALUES (?,?,?,'2026-08-24T00:00:00Z','2026-08-24T00:00:01Z','{}')",
                rows,
            )

        def retry_before(_job_id, occurrence, **_kwargs):
            self.assertGreaterEqual(occurrence.date(), effective_from)
            return None

        with (
            patch("v8.scheduler._report_duplicate_input_retry_before", side_effect=retry_before),
            patch("v8.scheduler.execute_job", return_value={"status": "partial"}) as execute,
        ):
            results = startup_catchup(
                now=current, db_path=self.db, reports_root=self.reports,
                effective_from=effective_from,
            )
        self.assertEqual(
            [(call.args[0], call.args[1].isoformat()) for call in execute.call_args_list],
            [("daily_report", "2026-08-25T08:00:00+08:00")],
        )
        self.assertEqual(len(results), 1)
        with connect(self.db) as connection:
            self.assertEqual(
                [tuple(row) for row in connection.execute(
                    "SELECT job_id,scheduled_for,status FROM scheduler_runs ORDER BY id"
                )],
                rows,
            )
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM report_tasks").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)

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

    def test_first_startup_is_report_only_and_never_claims_supplier_jobs(self) -> None:
        now = datetime(2026, 8, 2, 9, 0, tzinfo=SHANGHAI)
        with (
            patch("v8.scheduler.run_due_capture") as capture,
            patch("v8.scheduler.run_media_download_queue") as download,
            patch("v8.scheduler.run_media_processing_queue") as processing,
            patch("v8.scheduler.run_media_cutoff") as cutoff,
        ):
            results = startup_catchup(
                now=now, db_path=self.db, reports_root=self.reports
            )
        self.assertEqual(results, [])
        capture.assert_not_called()
        download.assert_not_called()
        processing.assert_not_called()
        cutoff.assert_not_called()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0],
                0,
            )

    def test_startup_report_source_rejects_every_supplier_job_before_claim(self) -> None:
        occurrence = datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI)
        supplier_jobs = (
            "daily_capture",
            "daily_media_download",
            "daily_media_processing",
            "daily_media_cutoff",
        )
        with patch("v8.scheduler._run_job_action") as action:
            for job_id in supplier_jobs:
                for allow_retry in (False, True):
                    with self.subTest(job_id=job_id, allow_retry=allow_retry):
                        with self.assertRaisesRegex(
                            SchedulerJobError,
                            "startup_report_catchup is report-only",
                        ):
                            execute_job(
                                job_id,
                                occurrence,
                                db_path=self.db,
                                reports_root=self.reports,
                                allow_retry=allow_retry,
                                invocation_source="startup_report_catchup",
                            )
            with self.assertRaisesRegex(
                SchedulerJobError,
                "startup_report_catchup requires allow_retry=True",
            ):
                execute_job(
                    "daily_report",
                    occurrence,
                    db_path=self.db,
                    reports_root=self.reports,
                    invocation_source="startup_report_catchup",
                )
        action.assert_not_called()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_run_attempts"
                ).fetchone()[0],
                0,
            )

    def test_startup_catchup_defers_report_jobs_without_claiming_them(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO content_items(
                    link_id,platform,platform_content_id,canonical_url,content_type,
                    published_at,imported_at,created_at,updated_at
                ) VALUES ('CU0001','douyin','catchup-01',
                          'https://www.douyin.com/video/catchup-01','video',
                          '2026-08-01T01:00:00Z',?,?,?)
                """,
                (now_utc(), now_utc(), now_utc()),
            )
            connection.commit()
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
            [("daily_report", "deferred")],
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
                connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0],
                0,
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
            scope_content_ids=[],
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

        with patch(
            "v8.scheduler.run_media_download_queue", return_value=recovered_result
        ) as recovered_queue:
            retried = execute_job(
                "daily_media_download",
                retryable_time,
                db_path=self.db,
                reports_root=self.reports,
                allow_retry=True,
                invocation_source="operator_retry",
            )
        self.assertEqual(retried["status"], "succeeded")
        self.assertEqual(retried["attempt_number"], 2)
        recovered_queue.assert_called_once()

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
        self.assertEqual(statuses["2026-08-01T18:20:00Z"], "succeeded")
        self.assertEqual(statuses["2026-08-02T18:20:00Z"], "succeeded")
        self.assertEqual(statuses["2026-08-03T18:20:00Z"], "succeeded")
        with connect(self.db) as connection:
            attempts = connection.execute(
                """
                SELECT sra.attempt_number,sra.invocation_source,sra.status
                FROM scheduler_run_attempts sra
                JOIN scheduler_runs sr ON sr.id=sra.scheduler_run_id
                WHERE sr.job_id='daily_media_download'
                  AND sr.scheduled_for='2026-08-01T18:20:00Z'
                ORDER BY sra.attempt_number
                """
            ).fetchall()
        self.assertEqual(
            [tuple(row) for row in attempts],
            [(1, "scheduled", "failed"), (2, "operator_retry", "succeeded")],
        )

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
            self.assertEqual(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM scheduler_run_attempts sra
                    JOIN scheduler_runs sr ON sr.id=sra.scheduler_run_id
                    WHERE sr.job_id='daily_report' AND sr.scheduled_for=?
                    """,
                    ("2026-08-05T00:00:00Z",),
                ).fetchone()[0],
                1,
            )

    def test_interrupted_attempt_is_retriable_and_late_owner_is_fenced(self) -> None:
        occurrence = datetime(2026, 6, 1, 8, 0, tzinfo=SHANGHAI)
        old_claim = _claim_run(
            "daily_report",
            occurrence,
            db_path=self.db,
            allow_retry=False,
            invocation_source="scheduled",
        )
        self.assertIsNotNone(old_claim)
        self.assertEqual(recover_interrupted_scheduler_runs(db_path=self.db), 1)
        new_claim = _claim_run(
            "daily_report",
            occurrence,
            db_path=self.db,
            allow_retry=True,
            invocation_source="startup_report_catchup",
        )
        self.assertIsNotNone(new_claim)
        assert old_claim is not None
        assert new_claim is not None
        self.assertEqual(new_claim.attempt_number, 2)
        with self.assertRaisesRegex(
            Exception, "scheduler attempt is no longer active"
        ):
            _finish_run(
                old_claim,
                status="succeeded",
                details={"owner": "stale"},
                db_path=self.db,
            )
        _finish_run(
            new_claim,
            status="succeeded",
            details={"owner": "current"},
            db_path=self.db,
        )
        with connect(self.db) as connection:
            run = connection.execute(
                "SELECT status,details_json FROM scheduler_runs"
            ).fetchone()
            attempts = connection.execute(
                """
                SELECT attempt_number,status FROM scheduler_run_attempts
                ORDER BY attempt_number
                """
            ).fetchall()
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual(json.loads(run["details_json"]), {"owner": "current"})
        self.assertEqual(
            [tuple(row) for row in attempts],
            [(1, "interrupted"), (2, "succeeded")],
        )

    def test_interruption_recovery_rejects_mismatched_running_ownership(self) -> None:
        with connect(self.db) as connection:
            ownerless_run_id = int(
                connection.execute(
                    """
                    INSERT INTO scheduler_runs(
                        job_id,scheduled_for,status,started_at,details_json
                    ) VALUES ('daily_report','2026-06-01T00:00:00Z','running',?,'{}')
                    """,
                    (now_utc(),),
                ).lastrowid
            )
            terminal_run_id = int(
                connection.execute(
                    """
                    INSERT INTO scheduler_runs(
                        job_id,scheduled_for,status,started_at,completed_at,details_json
                    ) VALUES (
                        'weekly_report','2026-06-01T00:30:00Z','succeeded',?,?, '{}'
                    )
                    """,
                    (now_utc(), now_utc()),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO scheduler_run_attempts(
                    scheduler_run_id,attempt_number,invocation_source,status,
                    started_at,details_json
                ) VALUES (?,1,'scheduled','running',?,'{}')
                """,
                (terminal_run_id, now_utc()),
            )
            connection.commit()
        with self.assertRaisesRegex(
            SchedulerJobError,
            "running scheduler occurrences and attempts are inconsistent",
        ):
            recover_interrupted_scheduler_runs(db_path=self.db)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM scheduler_runs WHERE id=?",
                    (ownerless_run_id,),
                ).fetchone()[0],
                "running",
            )
            self.assertEqual(
                connection.execute(
                    """
                    SELECT status FROM scheduler_run_attempts
                    WHERE scheduler_run_id=?
                    """,
                    (terminal_run_id,),
                ).fetchone()[0],
                "running",
            )

    def test_report_catchup_retries_failure_older_than_seven_days_only(self) -> None:
        occurrence = datetime(2026, 6, 1, 8, 0, tzinfo=SHANGHAI)
        with (
            patch("v8.scheduler._run_job_action", side_effect=RuntimeError("boom")),
            self.assertRaisesRegex(RuntimeError, "boom"),
        ):
            execute_job(
                "daily_report",
                occurrence,
                db_path=self.db,
                reports_root=self.reports,
            )
        with (
            patch(
                "v8.scheduler._run_job_action",
                return_value=("partial", {"task_id": "old", "task_status": "partial"}),
            ) as report_action,
            patch("v8.scheduler.run_due_capture") as capture,
            patch("v8.scheduler.run_media_download_queue") as download,
            patch("v8.scheduler.run_media_processing_queue") as processing,
            patch("v8.scheduler.run_media_cutoff") as cutoff,
        ):
            results = startup_catchup(
                now=datetime(2026, 8, 15, 9, 0, tzinfo=SHANGHAI),
                db_path=self.db,
                reports_root=self.reports,
            )
        self.assertEqual(
            [(item["job_id"], item["status"], item["attempt_number"]) for item in results],
            [("daily_report", "partial", 2)],
        )
        report_action.assert_called_once()
        capture.assert_not_called()
        download.assert_not_called()
        processing.assert_not_called()
        cutoff.assert_not_called()

    def test_report_reconcile_preserves_frozen_report_and_correction_uses_new_cutoff(
        self,
    ) -> None:
        occurrence = datetime(2026, 8, 2, 8, 0, tzinfo=SHANGHAI)
        first = execute_job(
            "daily_report", occurrence, db_path=self.db, reports_root=self.reports
        )
        self.assertEqual(first["status"], "partial")
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE scheduler_runs
                SET started_at='2026-08-02T00:00:00Z',
                    completed_at='2026-08-02T00:10:00Z'
                WHERE job_id='daily_report' AND scheduled_for='2026-08-02T00:00:00Z'
                """
            )
            connection.execute(
                """
                UPDATE report_tasks
                SET started_at='2026-08-02T00:00:00Z',
                    completed_at='2026-08-02T00:09:00Z',
                    updated_at='2026-08-02T00:09:00Z'
                WHERE id='D8-D-20260801-20260801'
                """
            )
            content_id = int(
                connection.execute(
                    """
                    INSERT INTO content_items(
                        link_id,platform,platform_content_id,canonical_url,content_type,
                        published_at,imported_at,created_at,updated_at
                    ) VALUES ('STALE1','douyin','stale-01',
                              'https://www.douyin.com/video/stale-01','video',
                              '2026-08-01T01:00:00Z','2026-08-03T00:00:00Z',
                              '2026-08-03T00:00:00Z','2026-08-03T00:00:00Z')
                    """
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO duplicate_fingerprints(
                    content_id,fingerprint_version,source_sha256,text_sha256,
                    media_sha256_json,frame_phashes_json,text_char_count,
                    asr_char_count,ocr_char_count,payload_json,created_at
                ) VALUES (?,?,?,'', '[]','[]',0,0,0,'{}','2026-08-03T00:00:00Z')
                """,
                (content_id, FINGERPRINT_VERSION, "a" * 64),
            )
            connection.commit()

        results = startup_catchup(
            now=datetime(2026, 8, 3, 9, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            reports_root=self.reports,
        )
        daily = [item for item in results if item["job_id"] == "daily_report"]
        self.assertEqual(
            [(item["status"], item["attempt_number"]) for item in daily],
            [],
        )
        with connect(self.db) as connection:
            revisions = connection.execute(
                """
                SELECT revision,report_json_path FROM report_revisions
                WHERE task_id='D8-D-20260801-20260801' ORDER BY revision
                """
            ).fetchall()
        self.assertEqual([int(row["revision"]) for row in revisions], [1])
        revised = json.loads(
            (PROJECT_ROOT / str(revisions[-1]["report_json_path"])).read_text()
        )
        self.assertEqual(revised["summary_metrics"]["publication_count"]["value"], 0)
        self.assertEqual(revised["metadata"]["collection_cutoff_at"], "2026-08-02T00:00:00Z")

        from v8.reports import create_correction_task, run_task
        correction = create_correction_task("D8-D-20260801-20260801", reason="迟到作品和重复指纹", db_path=self.db)
        corrected = run_task(correction["id"], db_path=self.db, reports_root=self.reports)
        self.assertNotEqual(correction["id"], "D8-D-20260801-20260801")
        self.assertEqual(corrected["metadata"]["revision"], 1)
        self.assertEqual(corrected["summary_metrics"]["publication_count"]["value"], 1)

        second = startup_catchup(
            now=datetime(2026, 8, 3, 10, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            reports_root=self.reports,
        )
        self.assertFalse(any(item["job_id"] == "daily_report" for item in second))

    def test_missing_august_fifth_and_seventh_reports_are_partial_once(self) -> None:
        from v8.reports import create_task, get_task

        create_task(
            task_type="daily",
            period_start="2026-08-01",
            period_end="2026-08-01",
            creation_source="automatic",
            db_path=self.db,
        )
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.executemany(
                """
                INSERT INTO content_items(
                    link_id,platform,platform_content_id,canonical_url,content_type,
                    published_at,imported_at,created_at,updated_at
                ) VALUES (?, 'douyin', ?, ?, 'video', ?, ?, ?, ?)
                """,
                (
                    (
                        "CU0805",
                        "catchup-0805",
                        "https://www.douyin.com/video/catchup-0805",
                        "2026-08-05T01:00:00Z",
                        captured_at,
                        captured_at,
                        captured_at,
                    ),
                    (
                        "CU0807",
                        "catchup-0807",
                        "https://www.douyin.com/video/catchup-0807",
                        "2026-08-07T01:00:00Z",
                        captured_at,
                        captured_at,
                        captured_at,
                    ),
                ),
            )
            connection.commit()
        first = startup_catchup(
            now=datetime(2026, 8, 15, 9, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            reports_root=self.reports,
        )
        statuses = {
            str(item["scheduled_for"]): str(item["status"])
            for item in first
            if item["job_id"] == "daily_report"
        }
        self.assertEqual(statuses["2026-08-06T00:00:00Z"], "partial")
        self.assertEqual(statuses["2026-08-08T00:00:00Z"], "partial")
        weekly = [item for item in first if item["job_id"] == "weekly_report"]
        self.assertEqual(
            [(item["scheduled_for"], item["status"]) for item in weekly],
            [("2026-08-10T00:30:00Z", "deferred")],
        )
        for task_id in ("D8-D-20260805-20260805", "D8-D-20260807-20260807"):
            task = get_task(task_id, db_path=self.db)
            self.assertEqual(task["task_status"], "partial")
            self.assertEqual(len(task["revisions"]), 1)
        second = startup_catchup(
            now=datetime(2026, 8, 15, 9, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            reports_root=self.reports,
        )
        self.assertEqual(
            [(item["job_id"], item["status"]) for item in second],
            [("weekly_report", "deferred")],
        )
        for task_id in ("D8-D-20260805-20260805", "D8-D-20260807-20260807"):
            self.assertEqual(len(get_task(task_id, db_path=self.db)["revisions"]), 1)

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
            ) as duplicate_queue,
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
            scope_content_ids=[],
        )
        duplicate_queue.assert_called_once_with(
            limit=500,
            db_path=self.db,
            scope_content_ids=[],
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
                "phone": "",
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
            observation = connection.execute(
                "SELECT * FROM content_metric_observations"
            ).fetchone()
            raw = connection.execute(
                """
                SELECT id,captured_at FROM provider_raw_responses
                WHERE operation='douyin_video_statistics'
                """
            ).fetchone()
            slots = connection.execute("SELECT status FROM fetch_slots").fetchall()
            task_ids = {
                row["task_id"]
                for row in connection.execute("SELECT task_id FROM provider_usage")
            }
        self.assertEqual(content["title"], "新车内容详情")
        self.assertEqual(snapshot["view_count"], 100)
        self.assertEqual(observation["view_count"], 100)
        self.assertEqual(observation["raw_response_id"], raw["id"])
        self.assertEqual(observation["captured_at"], raw["captured_at"])
        self.assertEqual(snapshot["captured_at"], raw["captured_at"])
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

    def test_daily_capture_never_retries_sent_transient_metrics_without_compensation(
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

        self.assertNotEqual(result["status"], "succeeded", result)
        self.assertEqual(calls["metrics"], 1)
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
            usage_count = connection.execute(
                "SELECT COUNT(*) FROM provider_usage WHERE operation='douyin_video_statistics'"
            ).fetchone()[0]
        self.assertEqual(
            (slot["status"], slot["attempt_count"]),
            ("retryable_failed", 1),
        )
        self.assertEqual(
            [
                (row["attempt_number"], row["error_code"], row["billed"])
                for row in attempts
            ],
            [(1, "provider_retry_requested", 0)],
        )
        self.assertEqual(usage_count, 1)

    def test_daily_capture_does_not_retry_sent_transient_discovery(self) -> None:
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
        self.assertEqual(calls["discover_content"], 1)
        with connect(self.db) as connection:
            slot = connection.execute(
                """
                SELECT status,attempt_count,last_error_code FROM fetch_slots
                WHERE stage='discovery' AND window_key='2026-08-02:xiaohongshu:page:1'
                """
            ).fetchone()
            usage_count = connection.execute(
                "SELECT COUNT(*) FROM provider_usage WHERE operation='xiaohongshu_user_posts'"
            ).fetchone()[0]
        self.assertEqual(
            (slot["status"], slot["attempt_count"], slot["last_error_code"]),
            ("retryable_failed", 1, "provider_retry_requested"),
        )
        self.assertEqual(usage_count, 1)

    def test_daily_capture_provider_cost_matches_ledger_without_repurchase(
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
        self.assertFalse(result["quality_gate"]["passed"])
        self.assertFalse(result["quality_gate"]["checks"]["metrics_complete"])
        self.assertEqual(calls["metrics"], 1)
        self.assertGreater(ledger_cost, 0)
        self.assertEqual(result["provider_cost"], ledger_cost)
        self.assertEqual(result["failed_operations"], 1)

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

    def test_task_budget_block_opens_one_tikhub_circuit(self) -> None:
        for index in range(2):
            upsert_account(
                {
                    "phone": f"138001381{index:02d}",
                    "platforms": [
                        {
                            "platform": "xiaohongshu",
                            "uid": f"budget-circuit-{index}",
                        }
                    ],
                },
                db_path=self.db,
            )

        calls = 0

        def supplier_call(operation, record):
            nonlocal calls
            self.assertEqual(operation, "discover_content")
            calls += 1
            data = {"items": [], "has_more": True, "next_cursor": "page-2"}
            return ProviderResult(
                data,
                {"operation": operation, "data": data},
                200,
                True,
            )

        result = run_due_capture(
            datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            call_override=supplier_call,
            max_amount=0.01,
        )

        self.assertEqual(calls, 1)
        self.assertEqual(result["blocked_providers"], [])
        self.assertEqual(
            [item["status"] for item in result["discovery"]],
            ["failed", "circuit_break_skipped"],
        )
        self.assertEqual(
            result["discovery"][0]["pages"][-1]["error_code"],
            "task_budget_exhausted",
        )
        self.assertEqual(
            result["circuit_break_counts"]["task_budget_exhausted"], 1
        )
        self.assertFalse(result["quality_gate"]["passed"])
        with connect(self.db) as connection:
            amount = float(
                connection.execute(
                    """
                    SELECT COALESCE(SUM(amount), 0) FROM provider_usage
                    WHERE task_id='daily-capture-2026-08-02-bjt'
                    """
                ).fetchone()[0]
            )
        self.assertEqual(amount, 0.01)

    def test_task_budget_exhaustion_is_partial_after_quality_floor(self) -> None:
        upsert_account(
            {
                "phone": "13800138991",
                "platforms": [{"platform": "douyin", "uid": "quality-budget"}],
            },
            db_path=self.db,
        )
        contents = [
            {
                "id": content_id,
                "detail_needed": True,
                "metrics_needed": False,
                "comments_needed": False,
            }
            for content_id in (1, 2, 3)
        ]

        def update_result(content_id, **_kwargs):
            if content_id == 3:
                return {
                    "content_id": content_id,
                    "status": "partial",
                    "stages": [
                        {
                            "stage": "detail",
                            "status": "failed",
                            "error_code": "task_budget_exhausted",
                            "retryable": False,
                        }
                    ],
                    "provider_cost": 0.0,
                }
            return {
                "content_id": content_id,
                "status": "succeeded",
                "stages": [{"stage": "detail", "status": "succeeded"}],
                "provider_cost": 0.0,
            }

        discovery = {
            "status": "succeeded",
            "has_more": False,
            "page_published_at": [],
            "inserted": 0,
            "updated": 0,
            "provider_cost": 0.0,
        }
        with patch(
            "v8.scheduler._select_due_capture_contents", return_value=contents
        ), patch(
            "v8.scheduler.discover_account_content", return_value=discovery
        ), patch(
            "v8.scheduler.update_content_data", side_effect=update_result
        ):
            result = run_due_capture(
                datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
                db_path=self.db,
            )

        self.assertEqual(result["status"], "partial")
        self.assertTrue(result["quality_gate"]["passed"])
        self.assertEqual(result["blocked_providers"], [])
        self.assertEqual(result["failed_operations"], 1)
        self.assertEqual(
            result["circuit_break_counts"]["task_budget_exhausted"], 1
        )

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

    def test_daily_capture_marks_missing_or_repeated_cursor_partial(self) -> None:
        upsert_account(
            {
                "phone": "13800138013",
                "platforms": [
                    {
                        "platform": "douyin",
                        "uid": "99887713",
                        "nickname": "游标异常账号",
                    }
                ],
            },
            db_path=self.db,
        )
        mode = {"value": "missing"}

        def supplier_call(operation, record):
            if operation == "resolve_account":
                data = {"reference": "MS4wLjAB" + "x" * 40}
            elif operation == "discover_content":
                data = {"items": [], "has_more": True}
                if mode["value"] == "repeated":
                    data["next_cursor"] = "same-cursor"
            else:
                raise AssertionError(f"unexpected operation: {operation}")
            return ProviderResult(
                data, {"operation": operation, "data": data}, 200, True
            )

        missing = run_due_capture(
            datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            call_override=supplier_call,
        )
        self.assertEqual(missing["status"], "failed")
        self.assertEqual(missing["discovery"][0]["status"], "partial")
        self.assertEqual(
            missing["discovery"][0]["stopped_reason"], "missing_next_cursor"
        )

        mode["value"] = "repeated"
        repeated = run_due_capture(
            datetime(2026, 8, 3, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            call_override=supplier_call,
        )
        self.assertEqual(repeated["status"], "failed")
        self.assertEqual(repeated["discovery"][0]["status"], "partial")
        self.assertEqual(
            repeated["discovery"][0]["stopped_reason"], "cursor_repeated"
        )

    def test_daily_discovery_slots_are_isolated_by_platform(self) -> None:
        for platform, uid, nickname in (
            ("douyin", "99887789", "独立抖音账号"),
            ("xiaohongshu", "67f6657f000000000e02c22c", "独立小红书账号"),
        ):
            upsert_account(
                {"phone": "13800138005", "platforms": [
                    {"platform": platform, "uid": uid, "nickname": nickname}
                ]},
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
                            "content_type": "video" if platform == "douyin" else "image",
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
                    "account_uid": "99887789" if record["platform"] == "douyin" else "67f6657f000000000e02c22c",
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

    def test_media_cutoff_keeps_scope_to_eligible_content(self) -> None:
        self._insert_media_cutoff_content(1)
        self._insert_media_cutoff_content(
            2, source_group="history-backfill"
        )

        result = run_media_cutoff(
            datetime(2026, 8, 2, 7, 30, tzinfo=SHANGHAI), db_path=self.db
        )
        self.assertEqual(result["candidates"], 1)
        self.assertEqual(
            result["state_counts"],
            {
                "complete": 0,
                "terminal_insufficient": 0,
                "terminal_failed": 0,
                "pending": 1,
            },
        )
        self.assertEqual(result["pending"], 1)
        self.assertEqual(result["terminal"], 0)
        self.assertEqual(result["terminal_coverage"], 0.0)
        self.assertEqual(result["threshold"], 90.0)
        self.assertEqual(result["threshold_status"], "below_threshold")
        self.assertNotIn("manual_required", result)

    def test_daily_media_cohort_uses_previous_bjt_day_and_half_open_bounds(
        self,
    ) -> None:
        included = self._insert_media_cutoff_content(101)
        end_boundary = self._insert_media_cutoff_content(102)
        archived = self._insert_media_cutoff_content(
            103, source_group="history-archive"
        )
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET published_at=? WHERE id=?",
                ("2026-07-31T16:00:00Z", included),
            )
            connection.execute(
                "UPDATE content_items SET published_at=? WHERE id=?",
                ("2026-08-01T16:00:00Z", end_boundary),
            )
            connection.execute(
                "UPDATE content_items SET published_at=? WHERE id=?",
                ("2026-08-01T00:00:00Z", archived),
            )
            connection.commit()

        content_ids, start, end = _daily_media_cohort(
            datetime(2026, 8, 2, 2, 20, tzinfo=SHANGHAI), db_path=self.db
        )

        self.assertEqual(content_ids, [included])
        self.assertEqual(start, "2026-07-31T16:00:00Z")
        self.assertEqual(end, "2026-08-01T16:00:00Z")

    def test_media_cutoff_evaluates_only_pending_evaluations_and_uses_ninety(
        self,
    ) -> None:
        content_ids = [
            self._insert_media_cutoff_content(index) for index in range(1, 11)
        ]
        self._insert_media_source_artifact(content_ids[8])
        initial_states = {
            content_id: MediaTerminalDetail("complete", "complete")
            for content_id in content_ids[:8]
        }
        initial_states[content_ids[8]] = MediaTerminalDetail(
            "pending", "evaluation_pending"
        )
        initial_states[content_ids[9]] = MediaTerminalDetail(
            "pending", "source_missing"
        )
        final_states = dict(initial_states)
        final_states[content_ids[8]] = MediaTerminalDetail(
            "terminal_insufficient", "terminal_insufficient"
        )
        with (
            patch(
                "v8.scheduler.media_terminal_state_details",
                side_effect=(initial_states, final_states),
            ) as state_details,
            patch(
                "v8.scheduler.evaluate_content",
                return_value=SimpleNamespace(
                    evaluation_id=901,
                    evidence_level="V1",
                    created=True,
                ),
            ) as evaluate,
        ):
            result = run_media_cutoff(
                datetime(2026, 8, 2, 7, 30, tzinfo=SHANGHAI),
                db_path=self.db,
            )

        self.assertEqual(state_details.call_count, 2)
        evaluate.assert_called_once_with(
            content_ids[8],
            db_path=self.db,
            expected_active_release_id=V9_FIXTURE_RELEASE_ID,
        )
        self.assertEqual(
            result["state_counts"],
            {
                "complete": 8,
                "terminal_insufficient": 1,
                "terminal_failed": 0,
                "pending": 1,
            },
        )
        self.assertEqual(result["pending"], 1)
        self.assertEqual(result["terminal"], 9)
        self.assertEqual(result["terminal_coverage"], 90.0)
        self.assertEqual(result["threshold"], 90.0)
        self.assertEqual(result["threshold_status"], "available")
        self.assertEqual(result["evaluation"]["candidates"], 1)
        self.assertEqual(result["evaluation"]["created"], 1)
        self.assertEqual(result["evaluation"]["reused"], 0)
        # v16 起媒体截止不再触碰任何队列：结果里没有队列字段
        self.assertNotIn("resolved_media_queue_rows", result)

    def test_media_cutoff_does_not_carry_previous_report_day_into_next_day(self) -> None:
        content_id = self._insert_media_cutoff_content(20)
        self._insert_media_source_artifact(content_id)
        processing = {content_id: MediaTerminalDetail("pending", "frames_pending")}

        with (
            patch(
                "v8.scheduler.media_terminal_state_details",
                side_effect=(
                    processing,
                    processing,
                    {},
                    {},
                ),
            ),
            patch(
                "v8.scheduler.evaluate_content",
                return_value=SimpleNamespace(
                    evaluation_id=902,
                    evidence_level="V1",
                    created=True,
                ),
            ) as evaluate,
        ):
            first = run_media_cutoff(
                datetime(2026, 8, 2, 7, 30, tzinfo=SHANGHAI),
                db_path=self.db,
            )
            second = run_media_cutoff(
                datetime(2026, 8, 3, 7, 30, tzinfo=SHANGHAI),
                db_path=self.db,
            )

        self.assertEqual(first["candidates"], 1)
        self.assertEqual(first["evaluation"]["candidates"], 0)
        self.assertEqual(first["terminal_coverage"], 0.0)
        self.assertEqual(second["candidates"], 0)
        self.assertEqual(second["evaluation"]["candidates"], 0)
        self.assertEqual(second["terminal_coverage"], 100.0)
        evaluate.assert_not_called()

    def test_media_cutoff_does_not_evaluate_unrelated_previous_day_content(self) -> None:
        evaluation_pending_id = self._insert_media_cutoff_content(30)
        processing_id = self._insert_media_cutoff_content(31)
        no_source_id = self._insert_media_cutoff_content(32)
        self._insert_media_source_artifact(evaluation_pending_id)
        self._insert_media_source_artifact(processing_id)
        with (
            patch(
                "v8.scheduler.media_terminal_state_details",
                side_effect=({}, {}),
            ) as state_details,
            patch(
                "v8.scheduler.evaluate_content",
                return_value=SimpleNamespace(
                    evaluation_id=903,
                    evidence_level="V2",
                    created=True,
                ),
            ) as evaluate,
        ):
            result = run_media_cutoff(
                datetime(2026, 8, 3, 7, 30, tzinfo=SHANGHAI),
                db_path=self.db,
            )

        self.assertEqual(result["candidates"], 0)
        self.assertEqual(result["evaluation"]["candidates"], 0)
        self.assertEqual(
            state_details.call_args_list[0].args[2],
            [],
        )
        self.assertNotIn(no_source_id, state_details.call_args_list[0].args[2])
        evaluate.assert_not_called()

    def test_media_cutoff_evaluation_failure_marks_the_run_failed(
        self,
    ) -> None:
        content_id = self._insert_media_cutoff_content(1)
        self._insert_media_source_artifact(content_id)
        states = {content_id: MediaTerminalDetail("pending", "evaluation_pending")}
        occurrence = datetime(2026, 8, 2, 7, 30, tzinfo=SHANGHAI)
        with (
            patch("v8.scheduler.media_terminal_state_details", return_value=states),
            patch(
                "v8.scheduler.evaluate_content",
                side_effect=RuntimeError("evaluation failed"),
            ),
            self.assertRaisesRegex(RuntimeError, "evaluation failed"),
        ):
            execute_job(
                "daily_media_cutoff",
                occurrence,
                db_path=self.db,
                reports_root=self.reports,
            )

        with connect(self.db) as connection:
            run = connection.execute(
                """
                SELECT status FROM scheduler_runs
                WHERE job_id='daily_media_cutoff' ORDER BY id DESC LIMIT 1
                """
            ).fetchone()
        self.assertEqual(run["status"], "failed")

    def test_legacy_capture_uses_only_accepted_current_enabled_members(self) -> None:
        accounts = [
            _upsert_account({"phone": "", "enabled": enabled, "platforms": [
                {"platform": "douyin", "uid": str(99889000 + index)}
            ]}, db_path=self.db)
            for index, enabled in enumerate((True, True, False))
        ]
        for index in (1, 2):
            self._insert_scheduled_content(
                account_id=accounts[index]["id"], link_id=f"E{index:05d}",
                platform_content_id=f"900000009{index:03d}", published_at="2026-08-01T12:00:00Z",
            )
        occurrence = datetime(2026, 8, 2, 2, tzinfo=SHANGHAI)
        with patch("v8.scheduler.discover_account_content") as unavailable:
            absent = run_due_capture(occurrence, db_path=self.db)
        self.assertEqual(absent["status"], "skipped")
        self.assertEqual(absent["monitored_accounts"], 0)
        unavailable.assert_not_called()
        with connect(self.db) as connection:
            identity_ids = [row[0] for row in connection.execute(
                "SELECT id FROM account_platform_identities WHERE account_id IN (?,?)",
                (accounts[0]["id"], accounts[2]["id"]),
            )]
            accept_roster(connection, identity_ids=identity_ids)
        calls = []

        def supplier(operation, record):
            calls.append(operation)
            self.assertEqual(record["account_id"], accounts[0]["id"])
            data = {"reference": "MS4wLjAB" + "x" * 40} if operation == "resolve_account" else {"items": [], "has_more": False}
            return ProviderResult(data, {"operation": operation, "data": data}, 200, False)

        result = run_due_capture(occurrence, db_path=self.db, call_override=supplier)
        self.assertEqual(result["status"], "succeeded", result)
        self.assertEqual(result["monitored_accounts"], 1)
        self.assertEqual(result["eligible_contents"], 0)
        self.assertEqual(result["content_updates"], [])
        self.assertEqual(calls, ["resolve_account", "discover_content"])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM fetch_slots WHERE content_id IS NOT NULL").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 2)

    def test_old_openapi_success_never_skips_tikhub_discovery(self) -> None:
        douyin = upsert_account(
            {
                "phone": "13800138201",
                "platforms": [
                    {"platform": "douyin", "uid": "99888201", "nickname": "开放平台"}
                ],
            },
            db_path=self.db,
        )
        xhs = upsert_account(
            {
                "phone": "13800138202",
                "platforms": [
                    {
                        "platform": "xiaohongshu",
                        "uid": "67f6657f000000000e02c201",
                        "nickname": "兜底账号",
                    }
                ],
            },
            db_path=self.db,
        )
        self._insert_openapi_run(
            local_day=date(2026, 8, 2),
            status="partial",
            details=self._openapi_details(
                [
                    self._openapi_account_receipt(
                        account_id=int(douyin["id"]),
                        platform_uid="99888201",
                        pages_fetched=2,
                        items_discovered=7,
                    ),
                    self._openapi_account_receipt(
                        account_id=int(xhs["id"]),
                        platform_uid="67f6657f000000000e02c201",
                        status="failed",
                        complete=False,
                        pages_fetched=0,
                    ),
                ]
            ),
        )
        calls = []

        def supplier_call(operation, record):
            calls.append((operation, record.get("platform")))
            data = (
                {"reference": "MS4wLjAB" + "x" * 40}
                if operation == "resolve_account" else {"items": [], "has_more": False}
            )
            return ProviderResult(
                data,
                {"operation": operation, "data": data},
                200,
                False,
            )

        result = run_due_capture(
            datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            call_override=supplier_call,
        )

        self.assertEqual(result["status"], "succeeded", result)
        self.assertEqual(calls, [("resolve_account", "douyin"),
                                 ("discover_content", "douyin"),
                                 ("discover_content", "xiaohongshu")])
        by_platform = {item["platform"]: item for item in result["discovery"]}
        actual = by_platform["douyin"]
        self.assertIsNone(actual.get("provider"))
        self.assertNotIn("items_discovered", actual)
        self.assertEqual([page["page"] for page in actual["pages"]], [1])
        self.assertNotIn("source_pages_fetched", actual["pages"][0])
        self.assertEqual(actual["pages"][0]["page_item_count"], 0)
        with connect(self.db) as connection:
            legacy = connection.execute(
                "SELECT details_json,status FROM scheduler_runs WHERE job_id=?",
                (DOUYIN_OPENAPI_RECONCILE_JOB_ID,),
            ).fetchone()
            self.assertEqual(legacy["status"], "partial")
            self.assertEqual(json.loads(legacy["details_json"])["accounts"][0]["items_discovered"], 7)
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM fetch_slots WHERE provider='DouyinOpenAPI'"
            ).fetchone()[0], 0)
        self.assertTrue(result["quality_gate"]["passed"], result)
        parsed = _parse_daily_capture_receipt(
            json.dumps(result, ensure_ascii=False, sort_keys=True)
        )
        self.assertEqual(parsed["covered"], 2)

    def test_daily_capture_falls_back_when_openapi_misses_the_two_oclock_tail(
        self,
    ) -> None:
        account = upsert_account(
            {
                "phone": "13800138204",
                "platforms": [
                    {"platform": "douyin", "uid": "99888204", "nickname": "尾段账号"}
                ],
            },
            db_path=self.db,
        )
        account_id = int(account["id"])
        receipt = self._openapi_details(
            [
                self._openapi_account_receipt(
                    account_id=account_id,
                    platform_uid="99888204",
                )
            ]
        )
        receipt["coverage_end"] = "2026-08-01T17:00:00Z"
        receipt["accounts"][0]["coverage_end"] = "2026-08-01T17:00:00Z"
        self._insert_openapi_run(
            local_day=date(2026, 8, 2),
            status="succeeded",
            details=receipt,
        )
        published_at = int(
            datetime(2026, 8, 2, 1, 30, tzinfo=SHANGHAI).timestamp()
        )
        calls = []

        def supplier_call(operation, _record):
            calls.append(operation)
            if operation == "resolve_account":
                data = {"reference": "MS4wLjAB" + "x" * 40}
            elif operation == "discover_content":
                data = {
                    "items": [
                        {
                            "platform": "douyin",
                            "platform_content_id": "900000008204",
                            "canonical_url": "https://www.douyin.com/video/900000008204",
                            "title": "01:30 发布作品",
                            "body": "01:30 发布作品",
                            "published_at": published_at,
                            "content_type": "video",
                            "account_uid": "99888204",
                            "account_name": "尾段账号",
                            "media_urls": ["https://example.invalid/video.mp4"],
                            "metrics": {
                                "view_count": 1,
                                "comment_count": 0,
                                "like_count": 0,
                                "share_count": 0,
                                "collect_count": 0,
                            },
                        }
                    ],
                    "has_more": False,
                    "next_cursor": None,
                }
            else:
                raise AssertionError(f"unexpected paid operation: {operation}")
            return ProviderResult(
                data,
                {"operation": operation, "data": data},
                200,
                False,
            )

        result = run_due_capture(
            datetime(2026, 8, 2, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            call_override=supplier_call,
        )

        self.assertEqual(result["discovery"][0]["provider_cost"], 0.0)
        self.assertEqual(calls, ["resolve_account", "discover_content"])
        with connect(self.db) as connection:
            stored = connection.execute(
                "SELECT platform_content_id FROM content_items"
            ).fetchone()[0]
        self.assertEqual(stored, "900000008204")

    def test_daily_capture_falls_back_for_incomplete_or_damaged_receipt(self) -> None:
        account = upsert_account(
            {
                "phone": "13800138203",
                "platforms": [
                    {"platform": "douyin", "uid": "99888203", "nickname": "兜底抖音"}
                ],
            },
            db_path=self.db,
        )
        account_id = int(account["id"])
        cases = (
            (
                date(2026, 8, 2),
                "partial",
                self._openapi_details(
                    [
                        self._openapi_account_receipt(
                            account_id=account_id,
                            platform_uid="99888203",
                            status="partial",
                            complete=False,
                        )
                    ]
                ),
            ),
            (
                date(2026, 8, 3),
                "succeeded",
                {"window_start": "x", "coverage_end": "y", "accounts": "bad"},
            ),
            (
                date(2026, 8, 4),
                "running",
                self._openapi_details(
                    [
                        self._openapi_account_receipt(
                            account_id=account_id,
                            platform_uid="99888203",
                        )
                    ]
                ),
            ),
            (
                date(2026, 8, 5),
                "succeeded",
                self._openapi_details(
                    [
                        self._openapi_account_receipt(
                            account_id=account_id,
                            platform_uid="99888204",
                        )
                    ]
                ),
            ),
        )
        calls = []

        def supplier_call(operation, record):
            calls.append((operation, record.get("platform")))
            if operation == "resolve_account":
                data = {"reference": "MS4wLjAB" + "x" * 40}
            else:
                data = {"items": [], "has_more": False}
            return ProviderResult(
                data,
                {"operation": operation, "data": data},
                200,
                False,
            )

        for local_day, status, details in cases:
            with self.subTest(local_day=local_day, status=status):
                self._insert_openapi_run(
                    local_day=local_day,
                    status=status,
                    details=details,
                )
                before = len(calls)
                result = run_due_capture(
                    datetime.combine(
                        local_day,
                        datetime.min.time().replace(hour=2),
                        SHANGHAI,
                    ),
                    db_path=self.db,
                    call_override=supplier_call,
                )
                self.assertGreater(len(calls), before)
                self.assertEqual(result["discovery"][0].get("provider"), None)
        before = len(calls)
        missing = run_due_capture(
            datetime(2026, 8, 6, 2, 0, tzinfo=SHANGHAI),
            db_path=self.db,
            call_override=supplier_call,
        )
        self.assertGreater(len(calls), before)
        self.assertIsNone(missing["discovery"][0].get("provider"))


if __name__ == "__main__":
    unittest.main()
