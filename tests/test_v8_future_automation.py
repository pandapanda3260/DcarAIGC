"""Exercise the real future-only planner with isolated SQLite and no network."""

from __future__ import annotations

import json
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from tests import test_v8_api as api_fixture
from tests import test_v8_pipeline as pipeline_fixture
from tests import test_v8_scan_receipts as receipt_fixture
from v8 import api, durable_runs, pipeline, scan_receipts, scheduler
from v8.automatic_scope import automatic_from_date, automatic_scope, within_automatic_scope
from v8.reconcile_control import reconcile_budget_scope
from v8.storage import connect, transaction


class FutureAutomationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = pipeline_fixture.PipelineTest(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.db = self.fixture.db
        self.reports = self.fixture.reports

    def test_business_day_fence_uses_beijing_midnight_and_rejects_unknown_dates(self) -> None:
        with automatic_scope(date(2026, 9, 6)):
            self.assertFalse(within_automatic_scope("2026-09-05T15:59:59Z"))
            self.assertTrue(within_automatic_scope("2026-09-05T16:00:00Z"))
            self.assertFalse(within_automatic_scope(None))

    def test_registered_jobs_restore_scope_inside_scheduler_worker_threads(self) -> None:
        jobs = api.BackgroundScheduler(timezone="Asia/Shanghai")
        pipeline.install_pipeline_jobs(jobs, db_path=self.db, reports_root=self.reports,
                                       authorization_effective_date=date(2026, 9, 6))
        dispatch_jobs = [job for job in jobs.get_jobs() if job.func is pipeline.dispatch]
        self.assertTrue(dispatch_jobs)
        with patch.dict(os.environ, {"DCAR_DAILY_CAPTURE_RECONCILE_FROM": ""}), patch(
            "v8.pipeline._dispatch_locked", side_effect=lambda *args, **kwargs: {"floor": automatic_from_date()}
        ), ThreadPoolExecutor(max_workers=3) as executor:
            results = [executor.submit(job.func, **job.kwargs) for job in dispatch_jobs]
            self.assertEqual([result.result()["floor"] for result in results],
                             [date(2026, 9, 6)] * len(dispatch_jobs))
            self.assertIsNone(automatic_from_date())

    def test_nested_discovery_threads_inherit_floor_without_environment_fallback(self) -> None:
        with patch.dict(os.environ, {"DCAR_DAILY_CAPTURE_RECONCILE_FROM": ""}), automatic_scope(date(2026, 9, 6)):
            results = pipeline._run_tikhub_discovery_workers(
                [{"id": 1}, {"id": 2}], lambda member: {"id": member["id"], "floor": automatic_from_date()},
            )
        self.assertEqual([item["floor"] for item in results], [date(2026, 9, 6)] * 2)

    def test_matrix_daily_window_does_not_purchase_earlier_days(self) -> None:
        self.fixture.activate()
        with automatic_scope(date(2026, 8, 29)), patch(
            "v8.matrix_scan.run_matrix_scan", return_value={"complete": True}
        ) as scan:
            pipeline.run_matrix_round("works", at="2026-08-29T18:10:00Z", db_path=self.db)
        self.assertEqual(scan.call_count, 2)
        for call in scan.call_args_list:
            self.assertEqual(call.kwargs["start_at"], "2026-08-28T16:00:00Z")
            self.assertEqual(call.kwargs["end_at"], "2026-08-29T16:00:00Z")
            self.assertEqual(call.kwargs["overall_start_at"], "2026-08-28T16:00:00Z")

    def test_tikhub_daily_reconcile_and_refresh_windows_share_the_floor(self) -> None:
        self.fixture.activate()
        for at, full in (("2026-08-29T18:10:00Z", False), ("2026-08-29T19:00:00Z", True), ("2026-08-29T22:00:00Z", False)):
            with self.subTest(at=at), automatic_scope(date(2026, 8, 29)), patch(
                "v8.tikhub_scan.run_account_scan", return_value={"complete": True}
            ) as scan:
                pipeline._run_tikhub_discovery_round(
                    at=at, db_path=self.db, call_override=None, frozen_roster=None, full_reconcile=full,
                )
                self.assertEqual(scan.call_count, 1)
                self.assertEqual(scan.call_args.kwargs["window_start"], "2026-08-28T16:00:00Z")

    def test_first_day_overnight_scans_do_not_fetch_previous_day(self) -> None:
        self.fixture.activate()
        with automatic_scope(date(2026, 8, 29)), patch("v8.tikhub_scan.run_account_scan") as tikhub, patch("v8.matrix_scan.run_matrix_scan") as matrix:
            result = pipeline._run_tikhub_discovery_round(
                at="2026-08-28T18:10:00Z", db_path=self.db, call_override=None, frozen_roster=None, full_reconcile=False,
            )
            pipeline.run_matrix_round("accounts", at="2026-08-28T18:00:00Z", db_path=self.db)
        self.assertEqual(result["reason"], "automatic_before_start")
        tikhub.assert_not_called()
        matrix.assert_not_called()

    def test_old_content_and_current_batch_with_old_content_never_reach_paid_or_local_work(self) -> None:
        self.fixture.activate()
        old = self.fixture.content(age=1)
        new = self.fixture.content(age=0)
        identity = {"kind": "content_pipeline", "created_for": pipeline_fixture.AT, "candidate_ids": [old["id"]]}
        claim = durable_runs.claim_run("content_pipeline", identity, db_path=self.db, now=pipeline_fixture.AT,
            initial_checkpoint={"complete": False, "pending_ids": [old["id"]], "items": [{"id": old["id"]}], "results": {}})
        durable_runs.finish_run(claim, status="partial", db_path=self.db, now=pipeline_fixture.AT, next_resume_at=pipeline_fixture.LATER)
        before = self.fixture.rows("scheduler_run_attempts")
        with automatic_scope(date(2026, 8, 29)), patch("v8.pipeline.update_content_data") as paid, patch("v8.pipeline._run_blocked_local_work") as local:
            candidates = pipeline._queue_candidates("content_pipeline", at=pipeline_fixture.LATER, db_path=self.db)
            self.assertEqual([row["id"] for row in candidates], [new["id"]])
            result = pipeline.run_content_batch("content_pipeline", at=pipeline_fixture.LATER, db_path=self.db, resume_run_id=claim.scheduler_run_id)
        self.assertEqual(result["reason"], "automatic_before_start")
        paid.assert_not_called()
        local.assert_not_called()
        self.assertEqual(self.fixture.rows("scheduler_run_attempts"), before)
        self.assertEqual(self.fixture.rows("provider_usage"), [])

    def test_resume_rejects_old_window_even_with_current_day_end(self) -> None:
        with automatic_scope(date(2026, 9, 6)):
            self.assertFalse(pipeline._resume_scope_is_current("tikhub_reconcile", {
                "provider": "TikHub", "window_start": "2026-08-06T16:00:00Z", "window_end": "2026-09-05T16:00:00Z",
            }, at="2026-09-06T04:00:00Z"))
            self.assertTrue(pipeline._resume_scope_is_current("tikhub_reconcile", {
                "provider": "TikHub", "window_start": "2026-09-05T16:00:00Z", "window_end": "2026-09-06T04:00:00Z",
            }, at="2026-09-06T04:00:00Z"))

    def test_metrics_and_comments_do_not_resume_old_content_in_new_batches(self) -> None:
        self.fixture.activate()
        old = self.fixture.content(age=1)
        for kind in ("metrics_backfill", "comments_refresh"):
            identity = {"kind": kind, "created_for": pipeline_fixture.AT, "candidate_ids": [old["id"]]}
            claim = durable_runs.claim_run(kind, identity, db_path=self.db, now=pipeline_fixture.AT,
                initial_checkpoint={"complete": False, "pending_ids": [old["id"]], "items": [{"id": old["id"]}], "results": {}})
            durable_runs.finish_run(claim, status="partial", db_path=self.db, now=pipeline_fixture.AT, next_resume_at=pipeline_fixture.LATER)
            before = self.fixture.rows("scheduler_run_attempts")
            with self.subTest(kind=kind), automatic_scope(date(2026, 8, 29)), patch("v8.pipeline.refresh_content_metrics") as metrics, patch("v8.pipeline.capture_content_comments_live") as comments:
                self.assertEqual(pipeline._queue_candidates(kind, at=pipeline_fixture.LATER, db_path=self.db), [])
                result = pipeline.run_content_batch(kind, at=pipeline_fixture.LATER, db_path=self.db, resume_run_id=claim.scheduler_run_id)
            self.assertEqual(result["reason"], "automatic_before_start")
            metrics.assert_not_called()
            comments.assert_not_called()
            self.assertEqual(self.fixture.rows("scheduler_run_attempts"), before)

    def test_new_round_freezes_business_day_and_old_report_cron_writes_nothing(self) -> None:
        self.fixture.activate()
        with patch("v8.pipeline._dispatch", return_value={"status": "succeeded", "complete": True}):
            pipeline.dispatch("daily_pipeline_summary", db_path=self.db, reports_root=self.reports,
                at=pipeline_fixture.AT, automatic_from=date(2026, 8, 29))
        rows = self.fixture.rows("scheduler_runs")
        summary = next(row for row in rows if row["job_id"] == "pipeline_round:daily_pipeline_summary")
        self.assertEqual(json.loads(summary["details_json"])["identity"]["automatic_from_date"], "2026-08-29")
        result = pipeline.dispatch("daily_report", db_path=self.db, reports_root=self.reports,
            at=pipeline_fixture.AT, automatic_from=date(2026, 8, 29))
        self.assertEqual(result["reason"], "automatic_before_start")
        self.assertEqual(self.fixture.rows("scheduler_runs"), rows)

    def test_old_raw_debt_does_not_starve_a_new_local_replay_slice(self) -> None:
        for index in range(51):
            identity = {"provider": "TikHub", "window_start": "2026-08-01T00:00:00Z" if index < 50 else "2026-09-05T16:00:00Z", "fixture": index}
            claim = durable_runs.claim_run("tikhub_reconcile", identity, db_path=self.db, now=pipeline_fixture.AT,
                initial_checkpoint={"complete": False, "pending_materialization": {"fixture": index}})
            durable_runs.finish_run(claim, status="partial", db_path=self.db, now=pipeline_fixture.AT, next_resume_at=pipeline_fixture.LATER)
        with automatic_scope(date(2026, 9, 6)), reconcile_budget_scope(), patch(
            "v8.tikhub_scan.resume_local_materialization", return_value={"processed_items": 1, "complete": True}
        ) as replay:
            pipeline._replay_materialization_debt(db_path=self.db, at="2026-09-06T04:00:00Z")
        replay.assert_called_once()
        self.assertEqual(replay.call_args.args[0], claim.scheduler_run_id)

    def test_report_catchup_recovers_all_owned_empty_days_but_no_earlier_report(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            connection.execute("INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json) VALUES ('daily_report','2026-09-05T00:00:00Z','failed','2026-09-05T00:00:00Z','2026-09-05T00:00:01Z','{}')")
        before = self.fixture.rows("scheduler_runs")
        with patch("v8.scheduler.execute_job", return_value={"status": "partial"}) as run:
            scheduler.startup_catchup(now=datetime(2026, 9, 9, 9, tzinfo=pipeline.BEIJING), db_path=self.db,
                reports_root=self.reports, effective_from=date(2026, 9, 6))
        self.assertEqual([(call.args[0], scheduler._report_period(call.args[0], call.args[1])[0].isoformat()) for call in run.call_args_list],
                         [("daily_report", "2026-09-06"), ("daily_report", "2026-09-07"), ("daily_report", "2026-09-08")])
        self.assertEqual(self.fixture.rows("scheduler_runs"), before)

    def test_frozen_discovery_scope_is_not_reinterpreted_by_new_environment(self) -> None:
        end = datetime(2026, 9, 7, tzinfo=pipeline.BEIJING)
        with patch.dict(os.environ, {"DCAR_DAILY_CAPTURE_RECONCILE_FROM": "2026-09-06"}):
            self.assertEqual(scan_receipts._discovery_window_start(end, 30, {}), end - timedelta(days=30))
            self.assertEqual(scan_receipts._discovery_window_start(end, 30, {"automatic_from_date": "2026-09-06"}), end - timedelta(days=1))

    def test_weekly_catchup_requires_the_entire_period_to_follow_the_floor(self) -> None:
        occurrences = scheduler._report_catchup_occurrences(
            current=datetime(2026, 9, 14, 9, tzinfo=pipeline.BEIJING), db_path=self.db, effective_from=date(2026, 9, 6),
        )
        weekly = [scheduler._report_period(job_id, occurrence) for job_id, occurrence in occurrences if job_id == "weekly_report"]
        self.assertEqual(weekly, [(date(2026, 9, 7), date(2026, 9, 13))])

    def test_real_forward_scan_receipts_remain_complete_without_the_environment(self) -> None:
        fixture = receipt_fixture.ScanReceiptsTest(methodName="runTest")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.addCleanup(fixture.tearDown)
        fixture._system_activate()
        old_item = receipt_fixture.dy_item(2, create_time=int(pipeline.parse_time(receipt_fixture.START).timestamp()) - 1)
        with automatic_scope(date(2026, 8, 28)):
            result = fixture._round(pages={
                "douyin_user_posts": [receipt_fixture.dy_page([receipt_fixture.dy_item(), old_item])],
                "xiaohongshu_user_posts": [receipt_fixture.xhs_page([receipt_fixture.xhs_item()])],
            })
        self.assertTrue(result["complete"])
        coverage = fixture._runtime()
        self.assertTrue(coverage["complete"], coverage)
        self.assertEqual(coverage["days"][0]["automatic_from_date"], "2026-08-28")
        self.assertEqual(len(fixture.tikhub_calls), 2)
        with connect(fixture.db) as connection:
            rows = connection.execute("SELECT published_at FROM content_items").fetchall()
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(pipeline.parse_time(row[0]) >= pipeline.parse_time(receipt_fixture.START) for row in rows))

    def test_health_and_scheduler_distinguish_paused_running_and_read_only(self) -> None:
        config = api_fixture._test_config(self.fixture.root / "api")
        api_fixture._seed_read_model_database(config.db_path)
        app = api.create_app(config)
        next_run = datetime(2026, 9, 7, 8, tzinfo=pipeline.BEIJING)
        with TestClient(app) as client:
            app.state.config = replace(config, daily_capture_reconcile_from=date(2026, 9, 6))
            app.state.scheduler = SimpleNamespace(state=api.STATE_PAUSED, get_jobs=lambda: [SimpleNamespace(id="daily_report", next_run_time=next_run)])
            paused = client.get("/api/v8/scheduler").json()
            self.assertEqual(paused["state"], "paused")
            self.assertFalse(paused["enabled"])
            self.assertIsNone(paused["next_run_at"])
            self.assertEqual(paused["reconcile_from"], "2026-09-06")
            health = client.get("/api/v8/health").json()["automation"]
            self.assertEqual(health["scheduler_state"], "paused")
            self.assertEqual(health["report_from_date"], "2026-09-06")
            app.state.scheduler.state = api.STATE_RUNNING
            running = client.get("/api/v8/scheduler").json()
            self.assertTrue(running["enabled"])
            self.assertEqual(running["next_run_at"], next_run.isoformat())
            app.state.config = replace(config, read_only=True, daily_capture_reconcile_from=date(2026, 9, 6))
            app.state.config.validate_daily_capture_reconcile_contract()
            self.assertEqual(client.get("/api/v8/health").json()["automation"]["scheduler_state"], "read_only")
