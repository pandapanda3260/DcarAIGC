from __future__ import annotations

import json
import hashlib
import sqlite3
import tempfile
import threading
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from tests.roster_fixture import accept_roster
from v8 import (
    durable_runs,
    paid_drain,
    pipeline,
    provider_budget,
    runtime_receipts,
    tikhub_scan,
)
from v8.capture import CaptureError
from v8.operations import upsert_account, upsert_content
from v8.profile_activations import TIKHUB_PROFILE, append_activation
from v8.providers import ProviderConfigurationError
from v8.source_routing import metric_cycle_key, parse_time
from v8.storage import connect, initialize_database, transaction

AT = "2026-08-29T00:12:00Z"
LATER = "2026-08-29T00:18:00Z"
AFTER = "2026-08-29T00:24:00Z"


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.db = self.root / "pipeline.sqlite3"
        self.reports = self.root / "reports"
        with connect(self.db) as connection:
            initialize_database(connection)
        self.account = self.account_row("100000000001")
        self.identity_id = self.account["platforms"][0]["id"] if "platforms" in self.account else None
        with connect(self.db) as connection:
            self.identity_id = int(connection.execute("SELECT id FROM account_platform_identities WHERE account_id=?", (self.account["id"],)).fetchone()[0])

    def account_row(self, uid, *, enabled=True, platform="douyin"):
        value = upsert_account({"phone": "", "enabled": enabled, "platforms": [
            {"platform": platform, "uid": uid, "nickname": "fixture"},
        ]}, db_path=self.db)
        return value

    def roster(
        self, ids=None, *, at="2026-08-28T16:00:00Z", activate_profile=True,
    ):
        with connect(self.db) as connection, transaction(connection):
            return accept_roster(
                connection, ids, accepted_at=at,
                activate_profile=activate_profile,
            )

    def activate(self, snapshot=None, *, cutover="2026-08-28T16:00:00Z"):
        snapshot = snapshot or self.roster()
        details = {
            "contract_version": pipeline.PIPELINE_VERSION, "mode": "active",
            "cutover_at": cutover, "roster_snapshot_id": snapshot["id"],
            "roster_snapshot_hash": snapshot["members_sha256"],
        }
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json) "
                "VALUES (?,?,'succeeded',?,?,?)",
                (pipeline.ACTIVATION_JOB, cutover, cutover, cutover, json.dumps(details)),
            )
        return snapshot

    def activate_tikhub_profile(
        self,
        *,
        effective_at="2026-08-29T16:00:00Z",
        created_at="2026-08-29T15:00:00Z",
    ):
        with connect(self.db) as connection, transaction(connection):
            rows = [
                dict(row) for row in connection.execute(
                    "SELECT * FROM account_platform_identities ORDER BY id"
                )
            ]
            keys = sorted(f"uid:{row['platform']}:{row['uid']}" for row in rows)
            members_sha256 = hashlib.sha256(json.dumps(
                keys, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False,
            ).encode()).hexdigest()
            source = json.dumps(keys, separators=(",", ":")).encode()
            source_sha256 = hashlib.sha256(source).hexdigest()
            path = self.root / f"system-roster-{effective_at.replace(':', '-')}.json"
            path.write_bytes(source)
            cursor = connection.execute(
                """INSERT INTO account_roster_snapshots(
                       source_family,source_type,scope_key,scope_json,
                       source_instance_id,source_captured_at,accepted_at,
                       declared_count,member_count,members_sha256,source_sha256,
                       source_path,contract_version,metadata_json)
                   VALUES ('system','system_managed','isolated-system','{}',?,?,?,?,
                           ?,?,?,?,'account-roster-v2','{\"fixture\":true}')""",
                (
                    f"system-{effective_at}", created_at, created_at,
                    len(rows), len(rows), members_sha256, source_sha256, str(path),
                ),
            )
            snapshot_id = int(cursor.lastrowid or 0)
            connection.executemany(
                """INSERT INTO account_roster_members(
                       snapshot_id,account_identity_id,platform,member_key,uid,
                       matrix_account_id,profile_ref,monitoring_status,
                       authorization_status,metadata_json)
                   VALUES (?,?,?,?,?,NULL,NULL,'unknown','unknown','{}')""",
                [
                    (
                        snapshot_id, int(row["id"]), str(row["platform"]),
                        f"uid:{row['platform']}:{row['uid']}", str(row["uid"]),
                    )
                    for row in rows
                ],
            )
            source_active = pipeline.activation(connection, at=created_at)
            value = append_activation(
                connection,
                profile_id=TIKHUB_PROFILE,
                roster_snapshot_id=snapshot_id,
                roster_members_sha256=members_sha256,
                effective_at=effective_at,
                build_receipt_sha256="c" * 64,
                actor="test",
                reason="switch to TikHub-managed fixture",
                created_at=created_at,
            )
            drain_id = f"switch-tikhub:{value['activation_id']}"
            binding = {
                "source_activation_id": (
                    int(source_active["activation_id"])
                    if source_active is not None
                    else int(value["activation_id"])
                ),
                "target_activation_id": int(value["activation_id"]),
                "business_day": parse_time(effective_at).astimezone(
                    pipeline.BEIJING
                ).date().isoformat(),
                "planned_effective_at": effective_at,
                "build_receipt_sha256": "c" * 64,
                "runtime_root_receipt_sha256": "d" * 64,
            }
            paid_drain.start_profile_drain_in_transaction(
                connection,
                drain_id,
                binding=binding,
                switch_kind="cross_profile",
                now=created_at,
            )
            paid_drain.seal_profile_drain_in_transaction(
                connection, drain_id, now=created_at
            )
            paid_drain.release_profile_drain_in_transaction(
                connection, drain_id, now=created_at
            )
        return {
            "id": snapshot_id,
            "members_sha256": members_sha256,
            "activation": value,
        }

    def content(self, *, age=2, uid="100000000001", source_group=""):
        with connect(self.db) as connection:
            ordinal = int(connection.execute("SELECT COUNT(*)+1 FROM content_items").fetchone()[0])
        published = parse_time(AT).astimezone(pipeline.BEIJING) - timedelta(days=age)
        created = upsert_content({
            "platform": "douyin", "platform_content_id": str(1000000 + ordinal),
            "canonical_url": "https://www.douyin.com/video/" + str(1000000 + ordinal),
            "title": "fixture", "published_at": pipeline._iso(published), "account_uid": uid,
            "content_type": "video",
        }, db_path=self.db, source_group_on_insert=source_group)
        with connect(self.db) as connection:
            return dict(connection.execute("SELECT * FROM content_items WHERE id=?", (created["id"],)).fetchone())

    def rows(self, table):
        with connect(self.db) as connection:
            return [dict(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]

    def slot(self, cid, stage, window, *, status="retryable_failed"):
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "INSERT INTO fetch_slots(content_id,stage,window_key,provider,adapter_version,status,created_at,updated_at) "
                "VALUES (?,?,?,'TikHub','fixture',?,?,?)", (cid, stage, window, status, AT, AT),
            )

    def durable(self, job, identity, *, complete=False, checkpoint=None, at=AT):
        claim = durable_runs.claim_run(job, identity, db_path=self.db, now=at, initial_checkpoint=checkpoint)
        if complete:
            with connect(self.db) as connection, transaction(connection):
                durable_runs.checkpoint(connection, claim, {"complete": True}, now=at)
        durable_runs.finish_run(
            claim, status="succeeded" if complete else "partial", db_path=self.db,
            now=at, next_resume_at=None if complete else at,
        )
        return claim.scheduler_run_id

    def finish(self, run_id, *, at=LATER):
        value = durable_runs.get_run(run_id, db_path=self.db)
        claim = durable_runs.claim_run(value["job_id"], value["details"]["identity"], db_path=self.db, now=at)
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(connection, claim, {"complete": True}, now=at)
        durable_runs.finish_run(claim, status="succeeded", db_path=self.db, now=at)

    def dispatch(self, job, **kwargs):
        return pipeline.dispatch(job, db_path=self.db, reports_root=self.reports, at=kwargs.pop("at", AT), **kwargs)

    def start_drain(self, drain_id="switch-profile"):
        with connect(self.db) as connection:
            active = pipeline.activation(connection)
        self.assertIsNotNone(active)
        return paid_drain.start_paid_drain(
            drain_id,
            binding={
                "source_activation_id": active["activation_run_id"],
                "target_activation_id": "planned:matrix-hybrid-v1",
                "business_day": "2026-08-29",
                "planned_effective_at": "2026-08-29T16:00:00Z",
                "build_receipt_sha256": "a" * 64,
                "runtime_root_receipt_sha256": "b" * 64,
            },
            db_path=self.db,
            now=AT,
        )

    def round_identity(self, snapshot, *, key="matrix_works_scan", at=AT):
        local = parse_time(at).astimezone(pipeline.BEIJING)
        scheduled = pipeline._scheduled_round_at(key, local)
        self.assertIsNotNone(scheduled)
        job_id = pipeline.CRON_ROUNDS[key][0]
        with connect(self.db) as connection:
            active = pipeline.activation(connection, at=pipeline._iso(scheduled))
        self.assertIsNotNone(active)
        return {
            "pipeline_version": pipeline.PIPELINE_VERSION,
            "beijing_day": local.date().isoformat(),
            "round_id": f"{key}:{scheduled.hour:02d}:{scheduled.minute:02d}",
            "registration_id": key,
            "job_id": job_id,
            "scheduled_at": pipeline._iso(scheduled),
            "roster_snapshot_id": snapshot["id"],
            "roster_snapshot_hash": snapshot["members_sha256"],
            "activation_id": active["activation_id"],
            "activation_sha256": active["activation_sha256"],
            "profile_id": active["profile_id"],
            "eligible_identity_ids": [self.identity_id],
        }

    def activation_identity(self, *, at=AT):
        with connect(self.db) as connection:
            active = pipeline.activation(connection, at=at)
        self.assertIsNotNone(active)
        return {
            "activation_id": active["activation_id"],
            "activation_sha256": active["activation_sha256"],
            "profile_id": active["profile_id"],
        }

    def claim_round(self, identity, *, at=AT):
        scope_key = {
            field: identity[field]
            for field in (
                "pipeline_version", "beijing_day", "registration_id",
                "scheduled_at", "activation_id", "profile_id",
            )
            if field in identity
        }
        claim = durable_runs.claim_run(
            "pipeline_round:" + identity["registration_id"],
            identity,
            db_path=self.db,
            now=at,
            scope_key=scope_key,
            initial_checkpoint={
                "child_run_ids": [], "remaining_profiles": [],
                "started": False, "complete": False,
            },
        )
        self.assertIsNotNone(claim)
        return claim

    def test_dispatch_requires_activation_before_work_is_dispatched(self):
        self.roster()
        with patch.object(pipeline, "activation", return_value=None), patch.object(
            pipeline, "_dispatch", side_effect=AssertionError("must not dispatch"),
        ):
            result = self.dispatch("matrix_works_scan")
        self.assertEqual(result["reason"], "pipeline_activation_required")
        self.assertFalse(result["complete"])
        self.assertEqual(self.rows("provider_usage"), [])

    def test_matrix_profile_gates_tikhub_managed_cron_without_business_call(self):
        self.activate()
        with patch.object(
            pipeline, "_dispatch", return_value={"status": "succeeded", "complete": True},
        ) as business:
            matrix = self.dispatch("matrix_works_scan")
            tikhub = self.dispatch(
                "tikhub_works_scan", registration_id="tikhub_works_scan",
            )

        self.assertTrue(matrix["complete"])
        self.assertEqual(
            (tikhub["status"], tikhub["reason"], tikhub["complete"]),
            ("skipped", "profile_not_scheduled", True),
        )
        business.assert_called_once()

    def test_tikhub_profile_gates_matrix_and_freezes_planned_activation(self):
        self.activate()
        system = self.activate_tikhub_profile()
        execution = "2026-08-29T18:12:00Z"  # Beijing 02:12 after the switch.
        with patch.object(
            pipeline, "_dispatch", return_value={"status": "succeeded", "complete": True},
        ) as business:
            matrix = self.dispatch("matrix_works_scan", at=execution)
            tikhub = self.dispatch(
                "tikhub_works_scan", registration_id="tikhub_works_scan", at=execution,
            )

        self.assertEqual(matrix["reason"], "profile_not_scheduled")
        self.assertTrue(matrix["complete"])
        self.assertTrue(tikhub["complete"])
        business.assert_called_once()
        frozen = business.call_args.kwargs["frozen_roster"]
        self.assertEqual(frozen["profile_id"], TIKHUB_PROFILE)
        self.assertEqual(
            frozen["activation_id"], system["activation"]["activation_id"],
        )
        self.assertEqual(frozen["roster_snapshot_id"], system["id"])

    def test_tikhub_account_job_never_invokes_matrix(self):
        self.activate()
        self.activate_tikhub_profile()
        execution = "2026-08-29T18:01:00Z"
        with patch.object(
            pipeline, "run_matrix_round", side_effect=AssertionError("Matrix forbidden"),
        ), patch.object(
            pipeline, "refresh_account_profile",
            return_value={"identity_id": self.identity_id, "status": "succeeded"},
        ) as profile:
            result = self.dispatch(
                "tikhub_account_metrics",
                registration_id="tikhub_account_metrics",
                at=execution,
            )

        self.assertTrue(result["complete"])
        profile.assert_called_once()
        self.assertEqual(self.rows("provider_usage"), [])

    def test_cross_midnight_paid_resume_closes_as_profile_superseded(self):
        self.activate()
        before_switch = "2026-08-29T10:01:00Z"  # Beijing 18:01.
        with patch.object(
            pipeline, "_dispatch", return_value={"status": "partial", "complete": False},
        ):
            first = self.dispatch("matrix_account_metrics", at=before_switch)
        self.activate_tikhub_profile()

        with patch.object(
            pipeline, "_dispatch", side_effect=AssertionError("provider forbidden"),
        ) as business:
            resumed = self.dispatch(
                "matrix_account_metrics",
                at="2026-08-29T16:05:00Z",
                resume_round_id=first["round_run_id"],
            )

        business.assert_not_called()
        self.assertEqual(
            (resumed["status"], resumed["reason"], resumed["complete"]),
            ("skipped", "profile_superseded", True),
        )
        run = durable_runs.get_run(first["round_run_id"], db_path=self.db)
        self.assertEqual((run["status"], run["details"]["complete"]), ("succeeded", True))

    def test_reconcile_terminalizes_superseded_child_and_parent_after_midnight(self):
        snapshot = self.activate()
        child_id = self.durable(
            "tikhub_reconcile", self.reconcile_scope(snapshot),
        )
        with patch.object(
            pipeline, "_dispatch", return_value={
                "status": "partial",
                "complete": False,
                "scans": [{
                    "scheduler_run_id": child_id,
                    "status": "partial",
                    "complete": False,
                }],
            },
        ):
            parent = self.dispatch("tikhub_reconcile")
        self.activate_tikhub_profile()

        with patch("v8.tikhub_scan.resume_account_scan") as provider:
            results = pipeline.resume_due_work(
                db_path=self.db,
                reports_root=self.reports,
                at="2026-08-29T16:05:00Z",
            )

        provider.assert_not_called()
        self.assertEqual(
            {
                item.get("scheduler_run_id", item.get("round_run_id"))
                for item in results
            },
            {child_id, parent["round_run_id"]},
        )
        child = durable_runs.get_run(child_id, db_path=self.db)
        self.assertEqual(child["status"], "failed")
        self.assertEqual(child["details"]["summary"]["reason"], "profile_superseded")
        parent_run = durable_runs.get_run(parent["round_run_id"], db_path=self.db)
        self.assertEqual((parent_run["status"], parent_run["details"]["complete"]),
                         ("succeeded", True))

    def test_installer_registers_static_union_of_profile_crons(self):
        from apscheduler.schedulers.background import BackgroundScheduler

        scheduler = BackgroundScheduler()
        with patch("v8.media_retention.install_lifecycle_jobs"):
            pipeline.install_pipeline_jobs(
                scheduler, db_path=self.db, reports_root=self.reports,
            )
        registered = {job.id for job in scheduler.get_jobs()}
        self.assertTrue(set(pipeline.CRON_ROUNDS) <= registered)
        self.assertTrue({
            "matrix_account_metrics", "matrix_works_scan", "matrix_works_refresh",
            "tikhub_account_metrics", "tikhub_works_scan", "tikhub_works_refresh",
        } <= registered)

    def test_paid_round_drain_deferral_is_one_shot_and_has_no_paid_side_effect(self):
        self.activate()
        self.start_drain()

        with patch.object(
            pipeline, "_dispatch", side_effect=AssertionError("paid business must not start"),
        ) as business:
            first = self.dispatch("matrix_works_scan")
            repeated = self.dispatch("matrix_works_scan")

        business.assert_not_called()
        self.assertEqual(
            (first["status"], first["reason"], first["deferred_reason"], first["complete"]),
            ("skipped", "dispatch_deferred", "profile_switch_drain", False),
        )
        self.assertEqual(repeated["deferred_run_id"], first["deferred_run_id"])
        self.assertEqual(repeated["deferred_attempt_id"], first["deferred_attempt_id"])
        deferred = [
            row for row in self.rows("scheduler_runs")
            if row["job_id"] == pipeline.DISPATCH_DEFERRED_JOB
        ]
        self.assertEqual(len(deferred), 1)
        details = json.loads(deferred[0]["details_json"])
        self.assertEqual(
            details["identity"],
            {
                "activation_id": first["activation_id"],
                "business_day": "2026-08-29",
                "drain_id": "switch-profile",
                "due_slot": "2026-08-28T18:10:00Z",
                "registration_id": "matrix_works_scan",
            },
        )
        attempts = [
            row for row in self.rows("scheduler_run_attempts")
            if row["scheduler_run_id"] == deferred[0]["id"]
        ]
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["status"], "skipped")
        self.assertEqual(attempts[0]["details_json"], deferred[0]["details_json"])
        self.assertEqual(
            [row for row in self.rows("scheduler_runs") if row["job_id"].startswith("pipeline_round:")],
            [],
        )
        self.assertEqual(self.rows("provider_usage"), [])
        self.assertEqual(self.rows("fetch_attempts"), [])

    def test_interval_drain_freezes_repeated_ticks_without_new_attempts(self):
        self.activate()
        self.start_drain()
        with patch.object(pipeline, "_dispatch", side_effect=AssertionError("blocked work must not start")):
            first = self.dispatch("content_pipeline")
            attempts = self.rows("scheduler_run_attempts")
            second = self.dispatch("content_pipeline", at=LATER)
            third = self.dispatch("content_pipeline", at=AFTER)
        self.assertEqual(first["deferred_run_id"], second["deferred_run_id"])
        self.assertEqual(first["deferred_run_id"], third["deferred_run_id"])
        self.assertEqual(self.rows("scheduler_run_attempts"), attempts)
        paid_drain.seal_paid_drain("switch-profile", db_path=self.db, now=LATER)
        sealed_attempts = self.rows("scheduler_run_attempts")
        sealed = self.dispatch("content_pipeline", at=AFTER)
        self.assertEqual(first["deferred_run_id"], sealed["deferred_run_id"])
        self.assertEqual(self.rows("scheduler_run_attempts"), sealed_attempts)
        self.assertEqual(self.rows("fetch_attempts"), [])
        self.assertEqual(self.rows("provider_usage"), [])

    def test_release_allows_due_round_to_be_created_by_reconcile(self):
        self.activate()
        self.start_drain("switch-release")
        deferred = self.dispatch("matrix_works_scan")
        paid_drain.seal_paid_drain("switch-release", db_path=self.db, now=LATER)
        paid_drain.release_paid_drain("switch-release", db_path=self.db, now=AFTER)

        with patch.object(
            pipeline, "_dispatch", return_value={"status": "succeeded", "complete": True},
        ) as business:
            recovered = self.dispatch("matrix_works_scan", at=AFTER)

        business.assert_called_once()
        self.assertEqual((recovered["status"], recovered["complete"]), ("succeeded", True))
        self.assertNotEqual(recovered["round_run_id"], deferred["deferred_run_id"])
        rounds = [
            row for row in self.rows("scheduler_runs")
            if row["job_id"] == "pipeline_round:matrix_works_scan"
        ]
        self.assertEqual(len(rounds), 1)

    def test_start_between_scheduler_gate_and_claim_cannot_create_paid_tail(self):
        self.activate()
        early_gate = pipeline._dispatch_deferred_receipt
        started = False

        def start_after_open_gate(**kwargs):
            nonlocal started
            result = early_gate(**kwargs)
            self.assertIsNone(result)
            if not started:
                started = True
                self.start_drain("switch-claim-race")
            return result

        with patch.object(
            pipeline, "_dispatch_deferred_receipt", side_effect=start_after_open_gate,
        ), patch.object(
            pipeline, "_dispatch", side_effect=AssertionError("paid business must not start"),
        ):
            result = self.dispatch("matrix_works_scan")

        self.assertEqual((result["status"], result["reason"]), ("skipped", "dispatch_deferred"))
        self.assertEqual(
            [row for row in self.rows("scheduler_runs") if row["job_id"].startswith("pipeline_round:")],
            [],
        )
        paid_drain.seal_paid_drain("switch-claim-race", db_path=self.db, now=LATER)

    def test_pipeline_reconcile_treats_deferred_as_healthy_and_summarizes_due_slots(self):
        self.activate()
        deferred = {
            "registration_id": "matrix_works_scan",
            "job_id": "matrix_works_scan",
            "scheduled_at": "2026-08-28T18:10:00Z",
            "status": "skipped",
            "complete": False,
            "reason": "dispatch_deferred",
            "deferred_reason": "profile_switch_drain",
        }
        with patch.object(
            pipeline, "resume_due_work", return_value=[],
        ), patch.object(
            pipeline, "_reconcile_current_day_rounds", return_value=[deferred],
        ), patch(
            "v8.runtime_receipts.refresh_runtime_receipts",
            return_value={
                "status": "succeeded",
                "scan_receipts": {"errors": {}, "limit_reached": False},
                "day_receipt": {"complete": True},
            },
        ) as receipt_refresh:
            result = pipeline._dispatch(
                "pipeline_reconcile", db_path=self.db, reports_root=self.reports, at=AT,
            )

        receipt_refresh.assert_called_once_with(db_path=self.db, cutoff_at=AT)
        self.assertEqual((result["status"], result["complete"]), ("succeeded", True))
        self.assertEqual(result["deferred_count"], 1)
        self.assertEqual(
            result["deferred_due_slots"],
            [{
                "registration_id": "matrix_works_scan",
                "scheduled_at": "2026-08-28T18:10:00Z",
            }],
        )

    def test_pipeline_reconcile_claims_current_rounds_before_resuming_debt(self):
        self.activate()
        events = []

        def reconcile_rounds(**_kwargs):
            events.append("rounds")
            return []

        def resume_work(**_kwargs):
            events.append("resume")
            return []

        with (
            patch.object(
                pipeline, "_replay_materialization_debt",
                side_effect=lambda **_kwargs: (events.append("local") or []),
            ),
            patch.object(
                pipeline,
                "_reconcile_current_day_rounds",
                side_effect=reconcile_rounds,
            ),
            patch.object(
                pipeline,
                "resume_due_work",
                side_effect=resume_work,
            ),
            patch(
                "v8.runtime_receipts.refresh_runtime_receipts",
                return_value={
                    "status": "succeeded",
                    "scan_receipts": {"errors": {}, "limit_reached": False},
                },
            ),
        ):
            result = pipeline._dispatch(
                "pipeline_reconcile",
                db_path=self.db,
                reports_root=self.reports,
                at=AT,
            )

        self.assertEqual(events, ["local", "rounds", "resume"])
        self.assertEqual((result["status"], result["complete"]), ("succeeded", True))

    def test_control_reconcile_queues_paid_round_once_without_network(self):
        self.activate()
        with pipeline.reconcile_budget_scope():
            with patch.object(pipeline, "_dispatch", side_effect=AssertionError("no inline business")):
                first = self.dispatch("tikhub_reconcile")
                before = self.rows("scheduler_run_attempts")
                repeated = self.dispatch("tikhub_reconcile", at=LATER)
        self.assertEqual(first["reason"], "execution_queued")
        self.assertFalse(first["complete"])
        self.assertEqual(repeated["round_run_id"], first["round_run_id"])
        self.assertEqual(self.rows("scheduler_run_attempts"), before)
        self.assertEqual(self.rows("provider_usage"), [])
        self.assertEqual(self.rows("fetch_attempts"), [])
        with patch.object(pipeline, "_dispatch", return_value={"status": "succeeded", "complete": True}) as work:
            resumed = self.dispatch("tikhub_reconcile", at=LATER, resume_round_id=first["round_run_id"])
        work.assert_called_once()
        self.assertTrue(resumed["complete"])

    def test_reconcile_shared_budget_stops_round_claims_at_limit_or_deadline(self):
        self.activate()
        clock = [0.0]
        with pipeline.reconcile_budget_scope(max_items=2, monotonic_fn=lambda: clock[0]) as budget:
            with patch.object(pipeline, "dispatch", return_value={"status": "succeeded", "complete": True}) as work:
                rounds = pipeline._reconcile_current_day_rounds(at=AT, db_path=self.db, reports_root=self.reports)
                self.assertEqual(len(rounds), 2)
                self.assertEqual(budget.used, 2)
                self.assertLessEqual(work.call_count, 2)
                self.assertEqual(pipeline._reconcile_current_day_rounds(at=LATER, db_path=self.db, reports_root=self.reports), [])
        with pipeline.reconcile_budget_scope(monotonic_fn=lambda: clock[0]):
            clock[0] = 60
            with patch.object(pipeline, "dispatch") as work:
                self.assertEqual(pipeline._reconcile_current_day_rounds(at=AT, db_path=self.db, reports_root=self.reports), [])
                work.assert_not_called()

    def test_reconcile_slice_receipt_preserves_pending_health_and_duplicate_is_noop(self):
        self.activate()
        with patch.object(pipeline, "_replay_materialization_debt", return_value=[]), patch.object(
            pipeline, "_reconcile_current_day_rounds", return_value=[{
                "status": "partial", "complete": False, "reason": "execution_queued",
            }]
        ), patch.object(pipeline, "resume_due_work", return_value=[]), patch(
            "v8.runtime_receipts.refresh_runtime_receipts",
            return_value={"status": "succeeded", "scan_receipts": {}},
        ):
            result = pipeline._dispatch("pipeline_reconcile", db_path=self.db, reports_root=self.reports, at=AT)
            before = self.rows("scheduler_run_attempts")
            repeated = pipeline._dispatch("pipeline_reconcile", db_path=self.db, reports_root=self.reports, at=AT)
        self.assertFalse(result["complete"])
        self.assertTrue(result["slice"]["continuation_required"])
        self.assertEqual(repeated["reason"], "reconcile_slice_already_claimed")
        self.assertEqual(self.rows("scheduler_run_attempts"), before)
        receipt = next(row for row in self.rows("scheduler_runs") if row["job_id"] == "pipeline_reconcile_slice")
        self.assertFalse(json.loads(receipt["details_json"])["summary"]["data_complete"])

    def test_pipeline_reconcile_receipt_failure_is_visible_partial_after_children(self):
        self.activate()
        completed = {"status": "succeeded", "complete": True, "scheduler_run_id": 901}
        with patch.object(
            pipeline, "resume_due_work", return_value=[completed],
        ), patch.object(
            pipeline, "_reconcile_current_day_rounds", return_value=[],
        ), patch(
            "v8.runtime_receipts.refresh_runtime_receipts",
            side_effect=RuntimeError("receipt disk unavailable"),
        ):
            result = pipeline._dispatch(
                "pipeline_reconcile", db_path=self.db, reports_root=self.reports, at=AT,
            )

        self.assertEqual((result["status"], result["complete"]), ("partial", False))
        self.assertEqual(result["resumed"], [completed])
        self.assertEqual(result["runtime_receipts"]["reason"], "RuntimeError")
        self.assertEqual(
            result["runtime_receipts"]["error"],
            "receipt disk unavailable",
        )

    def test_pipeline_reconcile_before_anchor_due_treats_receipt_skip_as_healthy(self):
        self.activate()
        before_anchor = "2026-08-28T18:59:59Z"
        with patch.object(
            pipeline, "resume_due_work", return_value=[],
        ), patch.object(
            pipeline, "_reconcile_current_day_rounds", return_value=[],
        ):
            result = pipeline._dispatch(
                "pipeline_reconcile",
                db_path=self.db,
                reports_root=self.reports,
                at=before_anchor,
            )

        self.assertEqual((result["status"], result["complete"]), ("succeeded", True))
        self.assertEqual(
            (
                result["runtime_receipts"]["status"],
                result["runtime_receipts"]["reason"],
            ),
            ("skipped", "profile_day_anchor_not_due"),
        )

    def test_pipeline_reconcile_anchor_skip_does_not_hide_scan_refresh_debt(self):
        self.activate()
        for index, scan_refresh in enumerate((
            {"errors": {"17": "receipt failed"}, "limit_reached": False},
            {"errors": {}, "limit_reached": True},
        )):
            with self.subTest(scan_refresh=scan_refresh), patch.object(
                pipeline, "resume_due_work", return_value=[],
            ), patch.object(
                pipeline, "_reconcile_current_day_rounds", return_value=[],
            ), patch(
                "v8.runtime_receipts.refresh_runtime_receipts",
                return_value={
                    "status": "skipped",
                    "reason": "profile_day_anchor_not_due",
                    "scan_receipts": scan_refresh,
                },
            ):
                result = pipeline._dispatch(
                    "pipeline_reconcile",
                    db_path=self.db,
                    reports_root=self.reports,
                    at=f"2026-08-28T18:59:{58 + index:02d}Z",
                )

            self.assertEqual(
                (result["status"], result["complete"]),
                ("partial", False),
            )
            self.assertEqual(
                result["runtime_receipts"]["reason"],
                "runtime_receipt_refresh_incomplete",
            )

    def test_pipeline_reconcile_after_anchor_due_requires_closeout_anchor(self):
        self.activate()
        business_day = runtime_receipts._coverage_business_day(AT)
        with connect(self.db) as connection:
            active = runtime_receipts._active_schema19_profile(
                connection,
                business_day=business_day,
            )
        coverage_without_anchor = {
            "contract_version": runtime_receipts.PROFILE_DAY_CONTRACT,
            "days": [
                {
                    "date": business_day,
                    **active,
                    "round_run_id": None,
                    "matrix_run_ids": [],
                    "tikhub_run_ids": [],
                }
            ],
        }
        with patch.object(
            pipeline, "resume_due_work", return_value=[],
        ), patch.object(
            pipeline, "_reconcile_current_day_rounds", return_value=[],
        ), patch(
            "v8.scan_receipts.runtime_coverage",
            return_value=coverage_without_anchor,
        ):
            result = pipeline._dispatch(
                "pipeline_reconcile",
                db_path=self.db,
                reports_root=self.reports,
                at=AT,
            )

        self.assertEqual((result["status"], result["complete"]), ("partial", False))
        self.assertEqual(result["runtime_receipts"]["status"], "partial")
        self.assertIn("closeout anchor", result["runtime_receipts"]["error"])

    def test_initial_tikhub_dispatch_alternates_from_oldest_platform_head_one_page_each(self):
        snapshot = self.activate()
        members = [
            {"identity_id": 21, "platform": "xiaohongshu", "uid": "xhs-oldest"},
            {"identity_id": 11, "platform": "douyin", "uid": "douyin-oldest"},
            {"identity_id": 12, "platform": "douyin", "uid": "douyin-next"},
            {"identity_id": 22, "platform": "xiaohongshu", "uid": "xhs-next"},
        ]

        with patch.object(
            pipeline, "_scope", return_value=(snapshot, members),
        ), patch(
            "v8.tikhub_scan.run_account_scan",
            side_effect=lambda identity_id, **_kwargs: {
                "scheduler_run_id": identity_id, "status": "succeeded", "complete": True,
            },
        ) as scan:
            result = pipeline._dispatch(
                "tikhub_reconcile", db_path=self.db, reports_root=self.reports, at=AT,
            )

        self.assertTrue(result["complete"])
        self.assertEqual(
            [call.args[0] for call in scan.call_args_list],
            [21, 11, 22, 12],
        )
        self.assertEqual(
            [call.kwargs["max_pages"] for call in scan.call_args_list],
            [1, 1, 1, 1],
        )

    def test_tikhub_discovery_workers_start_both_platforms_and_bound_concurrency(self):
        members = pipeline._fair_platform_order(
            [
                {"identity_id": 21, "platform": "xiaohongshu"},
                {"identity_id": 11, "platform": "douyin"},
                {"identity_id": 12, "platform": "douyin"},
                {"identity_id": 22, "platform": "xiaohongshu"},
            ],
            lambda member: member["platform"],
        )
        first_pair = threading.Barrier(2, timeout=2)
        lock = threading.Lock()
        started: list[int] = []
        active = 0
        maximum = 0

        def run(member):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
                started.append(member["identity_id"])
                ordinal = len(started)
            if ordinal <= 2:
                first_pair.wait()
            with lock:
                active -= 1
            return {"identity_id": member["identity_id"], "status": "succeeded", "complete": True}

        results = pipeline._run_tikhub_discovery_workers(members, run)

        self.assertEqual(maximum, 2)
        self.assertEqual(set(started[:2]), {21, 11})
        self.assertEqual(
            [item["identity_id"] for item in results],
            [21, 11, 22, 12],
        )

    def test_tikhub_discovery_workers_preserve_parent_retry_on_member_error(self):
        members = [
            {"identity_id": 1, "platform": "douyin"},
            {"identity_id": 2, "platform": "xiaohongshu"},
            {"identity_id": 3, "platform": "douyin"},
        ]

        def run(member):
            if member["identity_id"] == 2:
                raise CaptureError("fixture", retryable=False, error_code="fixture_failed")
            return {"identity_id": member["identity_id"], "status": "succeeded", "complete": True}

        with self.assertRaisesRegex(CaptureError, "fixture"):
            pipeline._run_tikhub_discovery_workers(members, run)

    def test_tikhub_discovery_workers_propagate_frozen_paid_scope(self):
        members = [
            {"identity_id": 1, "platform": "douyin"},
            {"identity_id": 2, "platform": "xiaohongshu"},
        ]

        with provider_budget.paid_scope(
            "reconcile",
            roster_snapshot_id=7,
            roster_snapshot_hash="a" * 64,
            business_day="2026-08-29",
        ):
            results = pipeline._run_tikhub_discovery_workers(
                members,
                lambda member: {
                    "identity_id": member["identity_id"],
                    "business_day": provider_budget._SCOPE.get().business_day,
                },
            )

        self.assertEqual(
            [item["business_day"] for item in results],
            ["2026-08-29", "2026-08-29"],
        )

    def test_tikhub_worker_recovers_claimed_child_and_parent_closes_after_resume(self):
        xhs = self.account_row("a" * 24, platform="xiaohongshu")
        with connect(self.db) as connection:
            xhs_identity_id = int(connection.execute(
                "SELECT id FROM account_platform_identities WHERE account_id=?",
                (xhs["id"],),
            ).fetchone()[0])
        self.activate()

        def frozen_scope(identity_id, kwargs):
            self.assertIs(type(kwargs["activation_id"]), int)
            self.assertEqual(kwargs["profile_id"], pipeline.MATRIX_PROFILE)
            self.assertEqual(len(kwargs["activation_sha256"]), 64)
            return tikhub_scan._freeze(
                identity_id,
                window_start=kwargs["window_start"],
                window_end=kwargs["window_end"],
                purpose=kwargs["purpose"],
                roster_snapshot_id=kwargs["roster_snapshot_id"],
                roster_snapshot_hash=kwargs["roster_snapshot_hash"],
                db_path=self.db,
                task_id=None,
                task_max_amount=None,
                activation_id=kwargs["activation_id"],
                profile_id=kwargs["profile_id"],
                activation_sha256=kwargs["activation_sha256"],
                at=kwargs["now"],
            )

        def scan(identity_id, **kwargs):
            scope = frozen_scope(identity_id, kwargs)
            claim = durable_runs.claim_run(
                "tikhub_reconcile",
                scope,
                db_path=self.db,
                now=AT,
                initial_checkpoint={"complete": False},
            )
            self.assertIsNotNone(claim)
            if identity_id == xhs_identity_id:
                raise ProviderConfigurationError("fixture after child claim")
            with connect(self.db) as connection, transaction(connection):
                durable_runs.checkpoint(
                    connection, claim, {"complete": True}, now=AT,
                )
            durable_runs.finish_run(
                claim, status="succeeded", db_path=self.db, now=AT,
            )
            return {
                "scheduler_run_id": claim.scheduler_run_id,
                "status": "succeeded",
                "complete": True,
            }

        with patch("v8.tikhub_scan.run_account_scan", side_effect=scan):
            first = self.dispatch("tikhub_reconcile")

        self.assertEqual((first["status"], first["complete"]), ("partial", False))
        parent_id = first["round_run_id"]
        checkpoint = durable_runs.get_run(parent_id, db_path=self.db)["details"]["checkpoint"]
        self.assertTrue(checkpoint["started"])
        self.assertEqual(len(checkpoint["child_run_ids"]), 2)
        children = [
            durable_runs.get_run(run_id, db_path=self.db)
            for run_id in checkpoint["child_run_ids"]
        ]
        self.assertEqual({child["status"] for child in children}, {"interrupted", "succeeded"})
        self.assertNotIn("running", {row["status"] for row in self.rows("scheduler_runs")})
        failed_id = next(child["id"] for child in children if child["status"] == "interrupted")

        def resume(run_id, **kwargs):
            self.assertEqual((run_id, kwargs["max_pages"]), (failed_id, 1))
            run = durable_runs.get_run(run_id, db_path=self.db)
            claim = durable_runs.claim_run(
                run["job_id"], run["details"]["identity"],
                db_path=self.db, now=kwargs["now"],
            )
            self.assertIsNotNone(claim)
            with connect(self.db) as connection, transaction(connection):
                durable_runs.checkpoint(
                    connection, claim, {"complete": True}, now=kwargs["now"],
                )
            durable_runs.finish_run(
                claim, status="succeeded", db_path=self.db, now=kwargs["now"],
            )
            return {
                "scheduler_run_id": run_id,
                "status": "succeeded",
                "complete": True,
            }

        with patch("v8.tikhub_scan.resume_account_scan", side_effect=resume):
            resumed = pipeline.resume_due_work(db_path=self.db, at=LATER)

        self.assertIn(failed_id, [item.get("scheduler_run_id") for item in resumed])
        final = self.dispatch(
            "tikhub_reconcile", at=AFTER, resume_round_id=parent_id,
        )
        self.assertEqual((final["status"], final["complete"]), ("succeeded", True))
        self.assertNotIn("running", {row["status"] for row in self.rows("scheduler_runs")})

    def test_tikhub_worker_error_before_claim_keeps_parent_unstarted_for_replay(self):
        self.activate()
        with patch(
            "v8.tikhub_scan.run_account_scan",
            side_effect=ProviderConfigurationError("fixture before child claim"),
        ):
            first = self.dispatch("tikhub_reconcile")

        parent_id = first["round_run_id"]
        checkpoint = durable_runs.get_run(parent_id, db_path=self.db)["details"]["checkpoint"]
        self.assertFalse(checkpoint["started"])
        self.assertEqual(checkpoint["child_run_ids"], [])
        self.assertEqual(
            [row for row in self.rows("scheduler_runs") if row["job_id"] == "tikhub_reconcile"],
            [],
        )

        def complete_scan(identity_id, **kwargs):
            scope = tikhub_scan._freeze(
                identity_id,
                window_start=kwargs["window_start"],
                window_end=kwargs["window_end"],
                purpose=kwargs["purpose"],
                roster_snapshot_id=kwargs["roster_snapshot_id"],
                roster_snapshot_hash=kwargs["roster_snapshot_hash"],
                db_path=self.db,
                task_id=None,
                task_max_amount=None,
                activation_id=kwargs["activation_id"],
                profile_id=kwargs["profile_id"],
                activation_sha256=kwargs["activation_sha256"],
                at=kwargs["now"],
            )
            claim = durable_runs.claim_run(
                "tikhub_reconcile", scope, db_path=self.db, now=LATER,
                initial_checkpoint={"complete": False},
            )
            self.assertIsNotNone(claim)
            with connect(self.db) as connection, transaction(connection):
                durable_runs.checkpoint(
                    connection, claim, {"complete": True}, now=LATER,
                )
            durable_runs.finish_run(
                claim, status="succeeded", db_path=self.db, now=LATER,
            )
            return {
                "scheduler_run_id": claim.scheduler_run_id,
                "status": "succeeded",
                "complete": True,
            }

        with patch("v8.tikhub_scan.run_account_scan", side_effect=complete_scan):
            second = self.dispatch(
                "tikhub_reconcile", at=LATER, resume_round_id=parent_id,
            )

        self.assertEqual((second["status"], second["complete"]), ("succeeded", True))

    def test_legacy_activation_marker_cannot_bypass_missing_profile_activation(self):
        # Schema 19 ignores the retired scheduler activation when no valid
        # acquisition activation and bound roster exist.
        self.activate({"id": 1, "members_sha256": "a" * 64})
        with patch.object(pipeline, "update_content_data") as supplier:
            result = self.dispatch("content_pipeline")
        self.assertEqual(result["reason"], "pipeline_activation_required")
        self.assertFalse(result["complete"])
        supplier.assert_not_called()
        self.assertEqual(self.rows("provider_usage"), [])

    def test_content_planner_tick_evaluates_transport_ratio_before_queue_work(self):
        self.activate_tikhub_profile(
            effective_at="2026-08-28T16:00:00Z",
            created_at="2026-08-28T15:00:00Z",
        )
        with patch.object(
            pipeline, "evaluate_transport_faults", return_value=[]
        ) as evaluate:
            result = pipeline._dispatch(
                "content_pipeline",
                db_path=self.db,
                reports_root=self.reports,
                at=AT,
            )
        self.assertIn(result["status"], {"succeeded", "skipped"})
        evaluate.assert_called_once()
        self.assertEqual(evaluate.call_args.kwargs["at"], AT)
        self.assertEqual(
            evaluate.call_args.kwargs["planner_tick_id"],
            f"content_pipeline:{AT}",
        )

    def test_candidates_are_current_members_and_enabled_only(self):
        active = self.content()
        paused_account = self.account_row("100000000002", enabled=False)
        paused = self.content(uid="100000000002")
        self.account_row("100000000003")
        outside = self.content(uid="100000000003")
        with connect(self.db) as connection:
            paused_identity = int(connection.execute("SELECT id FROM account_platform_identities WHERE account_id=?", (paused_account["id"],)).fetchone()[0])
        self.activate(self.roster([self.identity_id, paused_identity]))
        candidates = pipeline._queue_candidates("content_pipeline", at=AT, db_path=self.db)
        self.assertEqual([row["id"] for row in candidates], [active["id"]])
        self.assertNotIn(paused["id"], [row["id"] for row in candidates])
        self.assertNotIn(outside["id"], [row["id"] for row in candidates])

    def test_recent_metrics_are_daily_and_established_rotate_once_in_three_days(self):
        self.activate()
        recent = [self.content(age=3)["id"] for _ in range(3)]
        established = [self.content(age=12)["id"] for _ in range(3)]
        seen = {cid: 0 for cid in established}
        for offset in range(3):
            at = pipeline._iso(parse_time(AT) + timedelta(days=offset))
            candidates = pipeline._queue_candidates("metrics_backfill", at=at, db_path=self.db)
            ids = {row["id"] for row in candidates}
            self.assertTrue(set(recent) <= ids)
            for cid in established:
                seen[cid] += int(cid in ids)
        self.assertEqual(set(seen.values()), {1})

    def test_thirty_day_metrics_pool_is_not_truncated_by_history_recovery_start(self):
        self.activate()
        items = [self.content(age=30) for _ in range(3)]
        day = parse_time(AT).astimezone(pipeline.BEIJING).date()
        expected = {item["id"] for item in items if item["id"] % 3 == day.toordinal() % 3}
        self.assertEqual(len(expected), 1)
        actual = {item["id"] for item in pipeline._queue_candidates("metrics_backfill", at=AT, db_path=self.db)}
        self.assertEqual(actual, expected)

    def test_failed_metric_phase_continues_on_off_day_with_original_cycle(self):
        self.activate()
        day = parse_time(AT).astimezone(pipeline.BEIJING).date()
        values = [self.content(age=12) for _ in range(3)]
        content = next(item for item in values if item["id"] % 3 != day.toordinal() % 3)
        old_at = pipeline._iso(parse_time(AT) - timedelta(days=2))
        old_cycle = metric_cycle_key(content["id"], content["published_at"], as_of=old_at)
        self.slot(content["id"], "metrics", old_cycle + ":statistics")
        selected = pipeline._queue_candidates("metrics_backfill", at=AT, db_path=self.db, age_band="recent")
        current = next(item for item in selected if item["id"] == content["id"])
        self.assertEqual(current["cycle_key"], old_cycle)

    def test_successful_route_slots_are_not_rebought_when_fields_remain_missing(self):
        self.activate()
        content = self.content(age=2)
        cycle = metric_cycle_key(content["id"], content["published_at"], as_of=AT)
        for group in ("detail_counts", "statistics"):
            self.slot(content["id"], "metrics", cycle + ":" + group, status="succeeded")
        self.assertEqual(pipeline._queue_candidates("metrics_backfill", at=AT, db_path=self.db), [])

    def test_stable_candidate_limit_and_cross_queue_reservation(self):
        self.activate()
        recent = [{"id": index, "historical": False} for index in range(1, 20)]
        history = [{"id": index, "historical": True} for index in range(20, 30)]
        selected = pipeline.allocate_candidates(recent + history, 10)
        self.assertEqual([item["id"] for item in selected], list(range(1, 11)))
        self.assertEqual(sum(item["historical"] for item in selected), 0)
        self.assertEqual(len(selected), 10)
        held = self.content(source_group="history-backfill")
        self.durable(
            "content_pipeline", {"kind": "content_pipeline", "fixture": "held"},
            checkpoint={"pending_ids": [held["id"]]},
        )
        self.assertEqual(pipeline._queue_candidates("history_recovery", at=AT, db_path=self.db), [])
        self.assertEqual(pipeline._queue_candidates("content_pipeline", at=AT, db_path=self.db), [])

    def test_fresh_queue_reserves_candidates_before_releasing_execution(self):
        snapshot = self.activate()
        content = self.content()
        original_claim = durable_runs.claim_run_in_transaction
        first_claim_waiting = threading.Event()
        release_first_claim = threading.Event()
        provider_started = threading.Event()
        release_provider = threading.Event()
        claim_guard = threading.Lock()
        claim_count = 0
        outputs: dict[str, dict] = {}
        errors: list[BaseException] = []

        def delayed_claim(connection, job_id, *args, **kwargs):
            nonlocal claim_count
            if job_id == "content_pipeline":
                with claim_guard:
                    claim_count += 1
                    first = claim_count == 1
                if first:
                    first_claim_waiting.set()
                    release_first_claim.wait(timeout=10)
            return original_claim(connection, job_id, *args, **kwargs)

        def supplier(*_args, **_kwargs):
            provider_started.set()
            release_provider.wait(timeout=10)
            return {"status": "succeeded"}

        def run(label: str, at: str):
            try:
                outputs[label] = pipeline.run_content_batch(
                    "content_pipeline",
                    db_path=self.db,
                    at=at,
                    local_runner=lambda ids, **_: {
                        "terminal_ids": ids,
                        "pending_ids": [],
                    },
                )
            except BaseException as error:
                errors.append(error)

        with patch.object(
            pipeline.durable_runs, "claim_run_in_transaction", side_effect=delayed_claim,
        ), patch.object(
            pipeline, "update_content_data", side_effect=supplier,
        ) as provider:
            first = threading.Thread(target=run, args=("first", AT))
            second = threading.Thread(target=run, args=("second", LATER))
            first.start()
            self.assertTrue(first_claim_waiting.wait(timeout=5))
            second.start()
            release_first_claim.set()
            self.assertTrue(provider_started.wait(timeout=5))
            second.join(timeout=5)
            try:
                self.assertFalse(second.is_alive(), "second queue waited for provider execution")
                self.assertEqual(outputs["second"]["reason"], "queue_empty_not_discovery_complete")
                self.assertEqual(provider.call_count, 1)
            finally:
                release_provider.set()
                release_first_claim.set()
                first.join(timeout=5)
                second.join(timeout=5)

        self.assertEqual(errors, [])
        self.assertFalse(first.is_alive())
        runs = [row for row in self.rows("scheduler_runs") if row["job_id"] == "content_pipeline"]
        self.assertEqual(outputs["first"]["scheduler_run_id"], runs[0]["id"])
        attempts = [
            row for row in self.rows("scheduler_run_attempts")
            if row["scheduler_run_id"] == runs[0]["id"]
        ]
        self.assertEqual(len(runs), 1)
        self.assertEqual(len(attempts), 1)
        frozen_identity = json.loads(runs[0]["details_json"])["identity"]
        self.assertEqual(frozen_identity["candidate_ids"], [content["id"]])
        self.assertEqual(frozen_identity["profile_id"], pipeline.MATRIX_PROFILE)
        self.assertIs(type(frozen_identity["activation_id"]), int)
        self.assertEqual(frozen_identity["roster_snapshot_id"], snapshot["id"])

    def test_fresh_queue_partial_overlap_is_reserved_once(self):
        self.activate()
        contents = [self.content() for _ in range(3)]
        content_ids = [item["id"] for item in contents]
        original_claim = durable_runs.claim_run_in_transaction
        first_claim_waiting = threading.Event()
        release_first_claim = threading.Event()
        provider_started = threading.Event()
        release_provider = threading.Event()
        claim_guard = threading.Lock()
        provider_guard = threading.Lock()
        claim_count = 0
        provider_block_taken = False
        outputs: dict[str, dict] = {}
        errors: list[BaseException] = []
        provider_ids: list[int] = []

        def delayed_claim(connection, job_id, *args, **kwargs):
            nonlocal claim_count
            if job_id == "content_pipeline":
                with claim_guard:
                    claim_count += 1
                    first = claim_count == 1
                if first:
                    first_claim_waiting.set()
                    release_first_claim.wait(timeout=10)
            return original_claim(connection, job_id, *args, **kwargs)

        def supplier(content_id, *_args, **_kwargs):
            nonlocal provider_block_taken
            provider_ids.append(content_id)
            with provider_guard:
                should_block = not provider_block_taken
                provider_block_taken = True
            if should_block:
                provider_started.set()
                release_provider.wait(timeout=10)
            return {"status": "succeeded"}

        def run(label: str, at: str):
            try:
                outputs[label] = pipeline.run_content_batch(
                    "content_pipeline",
                    db_path=self.db,
                    at=at,
                    limit=2,
                    local_runner=lambda ids, **_: {
                        "terminal_ids": ids,
                        "pending_ids": [],
                    },
                )
            except BaseException as error:
                errors.append(error)

        with patch.object(
            pipeline.durable_runs, "claim_run_in_transaction", side_effect=delayed_claim,
        ), patch.object(
            pipeline, "update_content_data", side_effect=supplier,
        ):
            first = threading.Thread(target=run, args=("first", AT))
            second = threading.Thread(target=run, args=("second", LATER))
            first.start()
            self.assertTrue(first_claim_waiting.wait(timeout=5))
            second.start()
            release_first_claim.set()
            self.assertTrue(provider_started.wait(timeout=5))
            second.join(timeout=5)
            try:
                self.assertFalse(second.is_alive(), "disjoint queue waited for provider execution")
            finally:
                release_provider.set()
                release_first_claim.set()
                first.join(timeout=5)
                second.join(timeout=5)

        self.assertEqual(errors, [])
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        runs = [row for row in self.rows("scheduler_runs") if row["job_id"] == "content_pipeline"]
        frozen = [
            set(json.loads(row["details_json"])["identity"]["candidate_ids"])
            for row in runs
        ]
        self.assertEqual(len(frozen), 2)
        self.assertEqual(sorted(map(len, frozen)), [1, 2])
        self.assertFalse(frozen[0] & frozen[1])
        self.assertEqual(set().union(*frozen), set(content_ids))
        self.assertEqual(sorted(provider_ids), sorted(content_ids))
        run_ids = {row["id"] for row in runs}
        self.assertEqual(
            len([
                row for row in self.rows("scheduler_run_attempts")
                if row["scheduler_run_id"] in run_ids
            ]),
            2,
        )

    def test_automatic_queue_excludes_new_and_frozen_historical_items(self):
        snapshot = self.activate()
        recent = self.content()
        historical = self.content(source_group="history-backfill")
        automatic = pipeline._queue_candidates("content_pipeline", at=AT, db_path=self.db)
        self.assertIn(recent["id"], {item["id"] for item in automatic})
        self.assertNotIn(historical["id"], {item["id"] for item in automatic})
        self.assertIn(
            historical["id"],
            {item["id"] for item in pipeline._queue_candidates("history_recovery", at=AT, db_path=self.db)},
        )

        identity = {
            "pipeline_version": pipeline.PIPELINE_VERSION,
            "kind": "content_pipeline",
            "roster_snapshot_id": snapshot["id"],
            "roster_snapshot_hash": snapshot["members_sha256"],
            "created_for": AT,
            "candidate_ids": [historical["id"]],
        }
        run_id = self.durable(
            "content_pipeline",
            identity,
            checkpoint={
                "pending_ids": [historical["id"]],
                "items": [{
                    "id": historical["id"],
                    "identity_id": self.identity_id,
                    "historical": True,
                }],
                "results": {},
                "complete": False,
            },
        )
        with patch.object(pipeline, "update_content_data") as supplier:
            resumed = pipeline.run_content_batch(
                "content_pipeline",
                db_path=self.db,
                at=LATER,
                resume_run_id=run_id,
            )
        supplier.assert_not_called()
        self.assertTrue(resumed["complete"])
        self.assertEqual(
            resumed["details"]["checkpoint"]["results"][str(historical["id"])],
            {"status": "skipped", "reason": "automatic_history_disabled"},
        )

    def test_old_comment_week_debt_does_not_wait_for_current_phase(self):
        self.activate()
        content = self.content(age=5)
        old_week = "2026-W34"
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "INSERT INTO comment_capture_runs(content_id,window_key,provider,adapter_version,status,created_at,updated_at) "
                "VALUES (?,?,'TikHub','fixture','retryable_failed',?,?)",
                (content["id"], old_week, "2026-08-20T00:00:00Z", "2026-08-20T00:00:00Z"),
            )
        self.assertNotEqual(content["id"] % 7, parse_time(AT).astimezone(pipeline.BEIJING).weekday())
        with patch.object(pipeline, "capture_content_comments_live", return_value={"status": "succeeded", "provider_cost": 0}) as capture:
            result = pipeline.run_content_batch("comments_refresh", db_path=self.db, at=AT)
        self.assertTrue(result["complete"])
        self.assertEqual(capture.call_args.kwargs["as_of"], date.fromisocalendar(2026, 34, 1))
        self.assertEqual(capture.call_args.kwargs["task_max_amount"], 100.0)

    def test_circuit_failure_still_runs_local_local_completion_keeps_paid_debt(self):
        self.activate()
        content = self.content()
        local = Mock(return_value={"terminal_ids": [content["id"]], "pending_ids": [], "errors": []})
        error = CaptureError("balance exhausted", retryable=False, error_code="provider_balance_blocked", billed=False)
        with patch.object(pipeline, "update_content_data", side_effect=error):
            result = pipeline.run_content_batch("content_pipeline", db_path=self.db, at=AT, local_runner=local)
        local.assert_called_once()
        self.assertEqual(local.call_args.args[0], [content["id"]])
        self.assertFalse(result["complete"])
        self.assertIn(content["id"], result["details"]["checkpoint"]["pending_ids"])
        self.assertEqual(result["details"]["checkpoint"]["results"][str(content["id"])]["reason"], "provider_balance_blocked")

    def test_local_batch_exception_is_counted_and_surfaced_across_resumes(self):
        self.activate()
        content = self.content()
        local = Mock(side_effect=RuntimeError("content pipeline requires one active evaluation release"))
        detail = {"status": "succeeded", "provider_cost": 0.0}
        with patch.object(pipeline, "update_content_data", return_value=detail):
            first = pipeline.run_content_batch("content_pipeline", db_path=self.db, at=AT, local_runner=local)
        self.assertFalse(first["complete"])
        self.assertEqual(first["status"], "partial")
        self.assertEqual(first["reason"], "local_batch_error")
        self.assertEqual(first["local_batch_error"], "RuntimeError")
        checkpoint = first["details"]["checkpoint"]
        # The content stays pending (nothing was processed) and the failure is counted.
        self.assertEqual(checkpoint["pending_ids"], [content["id"]])
        self.assertEqual(checkpoint["local_batch_failures"], 1)
        self.assertEqual(first["details"]["summary"]["local_batch_error"], "RuntimeError")
        self.assertIsNotNone(first["details"].get("next_resume_at"))

        run_id = first["scheduler_run_id"]
        with patch.object(pipeline, "update_content_data", return_value=detail):
            second = pipeline.run_content_batch("content_pipeline", db_path=self.db, at=LATER,
                                                resume_run_id=run_id, local_runner=local)
        self.assertEqual(second["scheduler_run_id"], run_id)
        self.assertEqual(second["details"]["checkpoint"]["local_batch_failures"], 2)

        # A healthy local batch resets the counter and drains the round.
        healthy = Mock(return_value={"terminal_ids": [content["id"]], "pending_ids": [], "errors": []})
        # The second failure re-armed the round for LATER + 5 minutes.
        with patch.object(pipeline, "update_content_data", return_value=detail):
            third = pipeline.run_content_batch("content_pipeline", db_path=self.db, at="2026-08-29T00:30:00Z",
                                               resume_run_id=run_id, local_runner=healthy)
        self.assertTrue(third["complete"])
        self.assertIsNone(third["local_batch_error"])
        self.assertEqual(third["details"]["checkpoint"]["local_batch_failures"], 0)

    def test_local_batch_alert_opens_once_when_alert_table_exists(self):
        from v8 import durable_runs as durable

        claim = durable.DurableClaim(scheduler_run_id=9, attempt_id=1, attempt_number=1, owner_token="t", scan_id="abc")
        with connect(self.db) as connection:
            # schema19 fixture: no operational_alerts table, helper is a no-op.
            pipeline._open_local_batch_alert(connection, claim=claim, kind="content_pipeline",
                                             reason="RuntimeError", failures=3, at=AT)
            from v8 import schema_v20

            schema_v20.migrate(connection)
            for _ in range(2):
                pipeline._open_local_batch_alert(connection, claim=claim, kind="content_pipeline",
                                                 reason="RuntimeError", failures=3, at=AT)
            rows = connection.execute("SELECT severity,owner,status,dedupe_key FROM operational_alerts").fetchall()
        self.assertEqual([tuple(row) for row in rows], [("P1", "content-pipeline", "open", "content-pipeline-local:abc")])

    def test_detail_partial_response_keeps_pending_even_when_cache_is_terminal(self):
        self.activate()
        content = self.content()
        partial = {"status": "partial", "provider_cost": 0, "stages": [{"stage": "detail", "status": "failed", "error_code": "provider_circuit_open", "retryable": False}]}
        with patch.object(pipeline, "update_content_data", return_value=partial) as update:
            result = pipeline.run_content_batch(
                "content_pipeline", db_path=self.db, at=AT,
                local_runner=lambda ids, **_: {"terminal_ids": ids, "pending_ids": [], "errors": []},
            )
        self.assertFalse(result["complete"])
        self.assertEqual(result["details"]["checkpoint"]["pending_ids"], [content["id"]])
        self.assertEqual(update.call_args.kwargs["task_max_amount"], 100.0)

    def test_billing_unknown_guard_is_not_treated_as_terminal_content_failure(self):
        self.assertFalse(pipeline._paid_result_is_terminal({
            "status": "partial",
            "stages": [{
                "stage": "detail",
                "status": "failed",
                "error_code": "billing_unknown_retry_blocked",
                "retryable": False,
            }],
        }))

    def test_v3_budget_and_fault_guards_keep_paid_work_pending(self):
        for error_code in (
            "automatic_budget_exhausted",
            "discovery_budget_exhausted",
            "metrics_budget_exhausted",
            "repair_budget_exhausted",
            "incident_total_budget_exhausted",
            "incident_bucket_budget_exhausted",
            "incident_authorization_invalid",
            "compensation_authorization_required",
            "compensation_authorization_invalid",
            "compensation_authorization_consumed",
            "operation_blocked",
            "storage_hard",
            "authorization_hard",
        ):
            with self.subTest(error_code=error_code):
                self.assertFalse(
                    pipeline._paid_result_is_terminal(
                        {
                            "status": "partial",
                            "stages": [
                                {
                                    "stage": "detail",
                                    "status": "failed",
                                    "error_code": error_code,
                                    "retryable": False,
                                }
                            ],
                        }
                    )
                )

    def test_nonretryable_detail_failure_closes_terminal_local_item(self):
        self.activate()
        self.content()
        unavailable = {
            "status": "partial",
            "provider_cost": 0,
            "stages": [{
                "stage": "detail",
                "status": "failed",
                "error_code": "content_unavailable",
                "retryable": False,
            }],
        }
        with patch.object(pipeline, "update_content_data", return_value=unavailable):
            result = pipeline.run_content_batch(
                "content_pipeline",
                db_path=self.db,
                at=AT,
                local_runner=lambda ids, **_: {
                    "terminal_ids": ids,
                    "pending_ids": [],
                    "errors": [],
                },
            )
        self.assertTrue(result["complete"])
        self.assertEqual(result["details"]["checkpoint"]["pending_ids"], [])

    def test_expired_business_day_closes_pending_queue_without_provider_calls(self):
        snapshot = self.activate()
        expired_at = "2026-08-30T00:18:00Z"
        providers = {
            "content_pipeline": "update_content_data",
            "comments_refresh": "capture_content_comments_live",
        }
        for job_id, provider_name in providers.items():
            with self.subTest(job_id=job_id):
                content = self.content()
                item = {
                    "id": content["id"],
                    "identity_id": self.identity_id,
                    "historical": False,
                }
                if job_id == "comments_refresh":
                    item["comment_as_of"] = "2026-08-29"
                identity = {
                    "pipeline_version": pipeline.PIPELINE_VERSION,
                    "kind": job_id,
                    "roster_snapshot_id": snapshot["id"],
                    "roster_snapshot_hash": snapshot["members_sha256"],
                    "created_for": AT,
                    "candidate_ids": [content["id"]],
                }
                run_id = self.durable(
                    job_id,
                    identity,
                    checkpoint={
                        "pending_ids": [content["id"]],
                        "items": [item],
                        "results": {},
                        "complete": False,
                    },
                )
                local = Mock()
                with patch.object(pipeline, provider_name) as provider:
                    result = pipeline.run_content_batch(
                        job_id,
                        db_path=self.db,
                        at=expired_at,
                        resume_run_id=run_id,
                        local_runner=local,
                    )
                provider.assert_not_called()
                local.assert_not_called()
                self.assertTrue(result["complete"])
                checkpoint = result["details"]["checkpoint"]
                self.assertEqual(checkpoint["pending_ids"], [])
                self.assertEqual(
                    checkpoint["results"][str(content["id"])],
                    {"status": "skipped", "reason": "business_day_expired"},
                )
                if job_id == "content_pipeline":
                    candidates = pipeline._queue_candidates(
                        job_id,
                        at=expired_at,
                        db_path=self.db,
                    )
                    self.assertIn(content["id"], {item["id"] for item in candidates})

    def test_due_queue_without_progress_starts_one_disjoint_fresh_batch(self):
        snapshot = self.activate()
        content = self.content()
        identity = {
            "pipeline_version": pipeline.PIPELINE_VERSION,
            "kind": "content_pipeline",
            "roster_snapshot_id": snapshot["id"],
            "roster_snapshot_hash": snapshot["members_sha256"],
            "created_for": AT,
            "candidate_ids": [content["id"]],
        }
        run_id = self.durable(
            "content_pipeline",
            identity,
            checkpoint={
                "pending_ids": [content["id"]],
                "items": [{
                    "id": content["id"],
                    "identity_id": self.identity_id,
                    "historical": False,
                }],
                "results": {},
                "complete": False,
            },
        )
        fresh_content = self.content()
        resumed = {
            "status": "partial",
            "complete": False,
            "scheduler_run_id": run_id,
        }
        original_batch = pipeline.run_content_batch

        def run_batch(kind, **kwargs):
            if "resume_run_id" in kwargs:
                return resumed
            return original_batch(
                kind,
                **kwargs,
                local_runner=lambda ids, **_: {
                    "terminal_ids": ids,
                    "pending_ids": [],
                    "errors": [],
                },
            )

        with patch.object(
            pipeline, "run_content_batch", side_effect=run_batch,
        ) as batch, patch.object(
            pipeline, "update_content_data", return_value={"status": "succeeded"},
        ):
            result = pipeline._dispatch(
                "content_pipeline",
                db_path=self.db,
                reports_root=self.reports,
                at=LATER,
            )

        self.assertEqual(result["scheduler_run_id"], run_id)
        self.assertEqual(result["fresh_batch"], {
            "status": "succeeded",
            "complete": True,
            "scheduler_run_id": run_id + 1,
        })
        self.assertEqual(batch.call_count, 2)
        self.assertEqual(batch.call_args_list[0].kwargs["resume_run_id"], run_id)
        self.assertNotIn("resume_run_id", batch.call_args_list[1].kwargs)
        self.assertEqual(batch.call_args_list[1].kwargs["age_band"], "all")
        fresh_run = durable_runs.get_run(run_id + 1, db_path=self.db)
        self.assertEqual(
            fresh_run["details"]["identity"]["candidate_ids"],
            [fresh_content["id"]],
        )

    def test_due_queue_progress_does_not_start_fresh_batch(self):
        self.activate()
        content = self.content()
        run_id = self.durable(
            "content_pipeline",
            {"kind": "content_pipeline", "fixture": "progress"},
            checkpoint={"pending_ids": [content["id"]], "complete": False},
        )
        before = {
            "details": {
                "checkpoint": {"pending_ids": [content["id"]], "complete": False},
            },
        }
        after = {
            "details": {"checkpoint": {"pending_ids": [], "complete": True}},
        }
        resumed = {
            "status": "succeeded",
            "complete": True,
            "scheduler_run_id": run_id,
        }
        with patch.object(
            pipeline.durable_runs, "get_run", side_effect=[before, after],
        ), patch.object(
            pipeline, "run_content_batch", return_value=resumed,
        ) as batch:
            result = pipeline._dispatch(
                "content_pipeline",
                db_path=self.db,
                reports_root=self.reports,
                at=LATER,
            )

        self.assertEqual(result, resumed)
        batch.assert_called_once()

    def test_terminal_xhs_type_probe_is_not_selected_again(self):
        self.activate()
        content = self.content()
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "DELETE FROM fetch_slots WHERE content_id=? AND stage='detail'",
                (content["id"],),
            )
            connection.execute(
                "INSERT INTO taxonomy_versions(id,version,status,definition,created_at,published_at) "
                "VALUES ('fixture-taxonomy','fixture-taxonomy','published','fixture',?,?)",
                (AT, AT),
            )
            connection.execute(
                "INSERT INTO evaluation_releases(id,rule_version,taxonomy_version,matcher_rule_sha256,status,created_at,updated_at,activated_at) "
                "VALUES ('fixture-release','fixture-rule','fixture-taxonomy',?,'active',?,?,?)",
                ("a" * 64, AT, AT, AT),
            )
        self.slot(
            content["id"],
            "detail",
            "xhs-type-probe-v1",
            status="terminal_failed",
        )
        state = Mock(state="terminal_failed", reason="provider_terminal_failed")
        with patch.object(
            pipeline,
            "media_terminal_state_details",
            return_value={content["id"]: state},
        ), patch.object(pipeline, "_fingerprint_ready", return_value=True):
            candidates = pipeline._queue_candidates(
                "content_pipeline",
                at=AT,
                db_path=self.db,
            )
        self.assertNotIn(content["id"], {item["id"] for item in candidates})

    def test_offboarded_resume_still_processes_cache_without_supplier_call(self):
        self.activate()
        content = self.content()
        with patch.object(pipeline, "update_content_data", return_value={"status": "partial", "stages": [{"stage": "detail", "status": "failed"}]}):
            first = pipeline.run_content_batch(
                "content_pipeline", db_path=self.db, at=AT,
                local_runner=lambda ids, **_: {"terminal_ids": [], "pending_ids": ids},
            )
        self.roster([], at=LATER)
        local = Mock(return_value={"terminal_ids": [content["id"]], "pending_ids": []})
        with patch.object(pipeline, "update_content_data") as supplier:
            second = pipeline.run_content_batch(
                "content_pipeline", db_path=self.db, at=LATER,
                resume_run_id=first["scheduler_run_id"], local_runner=local,
            )
        supplier.assert_not_called()
        local.assert_called_once()
        self.assertTrue(second["complete"])
        self.assertEqual(second["reason"], "profile_superseded")
        self.assertEqual(second["details"]["checkpoint"]["pending_ids"], [])
        self.assertEqual(
            second["details"]["checkpoint"]["results"][str(content["id"])],
            {"status": "skipped", "reason": "profile_superseded"},
        )

    def test_lost_owner_during_local_work_cannot_overwrite_new_checkpoint(self):
        self.activate()
        content = self.content()
        replacement = []

        def local(ids, **kwargs):
            row = next(row for row in self.rows("scheduler_runs") if row["job_id"] == "content_pipeline")
            details = json.loads(row["details_json"])
            old_attempt = details["owner"]["attempt_id"]
            durable_runs.recover_run(row["id"], expected_attempt_id=old_attempt, db_path=self.db, now=LATER)
            replacement.append(durable_runs.claim_run("content_pipeline", details["identity"], db_path=self.db, now=LATER))
            return {"terminal_ids": ids, "pending_ids": []}

        with patch.object(pipeline, "update_content_data", return_value={"status": "succeeded"}):
            with self.assertRaises(durable_runs.LostOwnership):
                pipeline.run_content_batch("content_pipeline", db_path=self.db, at=AT, local_runner=local)
        checkpoint = durable_runs.get_run(replacement[0].scheduler_run_id, db_path=self.db)["details"]["checkpoint"]
        self.assertEqual(checkpoint["pending_ids"], [content["id"]])
        self.assertIs(checkpoint["complete"], False)



    def matrix_scope(self, snapshot, day_offset):
        end = datetime(2026, 8, 29, tzinfo=pipeline.BEIJING)
        stop = end - timedelta(days=day_offset)
        return {
            "provider": "newrank_matrix", "kind": "works", "purpose": "daily-works",
            "roster_snapshot_id": snapshot["id"],
            "roster_snapshot_hash": snapshot["members_sha256"],
            "start_at": pipeline._iso(stop - timedelta(days=1)),
            "end_at": pipeline._iso(stop),
            "overall_start_at": pipeline._iso(end - timedelta(days=30)),
            "overall_end_at": pipeline._iso(end),
        }

    def reconcile_scope(self, snapshot, *, start="2026-08-21T16:00:00Z", end="2026-08-28T16:00:00Z"):
        return {
            "provider": "TikHub", "identity_id": self.identity_id, "purpose": "reconcile",
            "roster_snapshot_id": snapshot["id"],
            "roster_snapshot_hash": snapshot["members_sha256"],
            "window_start": start, "window_end": end,
        }

    def catalogs(self):
        return [
            {"id": row["id"], **json.loads(row["details_json"])}
            for row in self.rows("scheduler_runs") if row["job_id"] == "history_scan_catalog"
        ]

    def test_empty_content_queue_is_not_complete_discovery(self):
        self.activate()
        result = pipeline.run_content_batch("content_pipeline", db_path=self.db, at=AT)
        self.assertTrue(result["complete"])
        summary = pipeline.pipeline_summary(db_path=self.db, at=AT)
        self.assertFalse(summary["discovery_complete"])
        self.assertEqual({key: summary["discovery_coverage"][key] for key in (
            "matrix_expected_windows", "matrix_complete_windows", "tikhub_expected_members", "tikhub_complete_members")}, {
            "matrix_expected_windows": 60, "matrix_complete_windows": 0,
            "tikhub_expected_members": None, "tikhub_complete_members": 0,
        })
        self.assertEqual(summary["discovery_coverage"]["status"], "unknown")
        self.assertEqual(summary["discovery_coverage"]["reason"], "profile_day_receipt_missing")

    def test_synthetic_complete_flags_cannot_replace_frozen_scope_or_pagination_proofs(self):
        another = self.account_row("100000000002")
        with connect(self.db) as connection:
            other_identity = int(connection.execute("SELECT id FROM account_platform_identities WHERE account_id=?", (another["id"],)).fetchone()[0])
        snapshot = self.activate()
        scopes = [
            {**self.matrix_scope(snapshot, day), "platform": platform}
            for day in range(30) for platform in ("douyin", "xiaohongshu")
        ]
        for identity in scopes[:-1]:
            self.durable("matrix_works_scan", identity, complete=True)
        summary = pipeline.pipeline_summary(db_path=self.db, at=AT)
        self.assertEqual(summary["discovery_coverage"]["matrix_complete_windows"], 0)
        self.assertEqual(summary["discovery_coverage"]["status"], "unknown")
        self.assertFalse(summary["discovery_complete"])
        self.durable("matrix_works_scan", scopes[-1], complete=True)
        self.durable("tikhub_reconcile", self.reconcile_scope(snapshot, start="2026-08-22T16:00:00Z"), complete=True)
        summary = pipeline.pipeline_summary(db_path=self.db, at=AT)
        self.assertEqual(summary["discovery_coverage"]["matrix_complete_windows"], 0)
        self.assertEqual(summary["discovery_coverage"]["tikhub_complete_members"], 0)
        self.assertFalse(summary["discovery_complete"])
        self.durable("tikhub_reconcile", self.reconcile_scope(snapshot), complete=True)
        summary = pipeline.pipeline_summary(db_path=self.db, at=AT)
        self.assertIsNone(summary["discovery_coverage"]["tikhub_expected_members"])
        self.assertEqual(summary["discovery_coverage"]["tikhub_complete_members"], 0)
        self.assertFalse(summary["discovery_complete"])
        self.durable("tikhub_reconcile", {**self.reconcile_scope(snapshot), "identity_id": other_identity}, complete=True)
        self.assertFalse(pipeline.pipeline_summary(db_path=self.db, at=AT)["discovery_complete"])

    def test_history_seed_is_network_free_and_uses_cutover_then_first_membership(self):
        cutover = "2026-08-04T16:00:00Z"
        self.activate(self.roster(at=cutover), cutover=cutover)
        with patch("v8.matrix_scan.run_matrix_scan", side_effect=AssertionError("network forbidden")), \
                patch("v8.tikhub_scan.run_account_scan", side_effect=AssertionError("network forbidden")):
            pipeline.seed_history_work(db_path=self.db, at="2026-08-10T00:00:00Z")
            initial = self.catalogs()[0]
            old_scope = next(item for item in initial["checkpoint"]["items"] if item["provider"] == "tikhub" and item["purpose"] == "history")
            self.assertEqual(old_scope["window_start"], "2026-08-02T16:00:00Z")
            self.assertEqual(old_scope["window_end"], cutover)
            new_account = self.account_row("100000000002")
            with connect(self.db) as connection:
                new_id = int(connection.execute("SELECT id FROM account_platform_identities WHERE account_id=?", (new_account["id"],)).fetchone()[0])
            first_accepted = "2026-08-19T16:00:00Z"
            self.roster(at=first_accepted)
            pipeline.seed_history_work(db_path=self.db, at="2026-08-21T00:00:00Z")
            new_catalog = next(item for item in self.catalogs() if item["identity"].get("initial_member_ids") == [new_id])
            new_scope = next(item for item in new_catalog["checkpoint"]["items"] if item["provider"] == "tikhub" and item["purpose"] == "history")
            self.assertEqual(new_scope["window_start"], "2026-08-02T16:00:00Z")
            self.assertEqual(new_scope["window_end"], first_accepted)
            before = self.rows("scheduler_runs")
            pipeline.seed_history_work(db_path=self.db, at="2026-08-21T00:00:00Z")
            self.assertEqual(self.rows("scheduler_runs"), before)
        self.assertEqual(self.rows("provider_usage"), [])

    def test_later_successful_recent_scan_does_not_erase_existing_old_gap(self):
        cutover = "2026-08-04T16:00:00Z"
        snapshot = self.activate(self.roster(at=cutover), cutover=cutover)
        pipeline.seed_history_work(db_path=self.db, at="2026-08-20T00:00:00Z")
        gap = next(item for item in self.catalogs() if item["identity"]["kind"] == "outage")
        self.assertEqual(gap["checkpoint"]["items"][0]["window_start"], cutover)
        self.assertEqual(gap["checkpoint"]["items"][0]["window_end"], "2026-08-12T16:00:00Z")
        self.durable("tikhub_reconcile", self.reconcile_scope(snapshot), complete=True)
        pending = pipeline.seed_history_work(db_path=self.db, at=AT)
        self.assertIn(gap["id"], pending)
        restored = next(item for item in self.catalogs() if item["id"] == gap["id"])
        self.assertEqual(restored, gap)


    def test_fixed_cron_round_keeps_scheduled_identity_and_uses_execution_time(self):
        snapshot = self.activate()
        with patch.object(pipeline, "_dispatch", return_value={"status": "succeeded", "complete": True}) as dispatch:
            first = self.dispatch("matrix_works_scan")
            second = self.dispatch("matrix_works_scan", at=LATER)
        dispatch.assert_called_once()
        self.assertEqual(first["round_run_id"], second["round_run_id"])
        self.assertTrue(second["complete"])
        row = durable_runs.get_run(first["round_run_id"], db_path=self.db)
        self.assertEqual(row["details"]["identity"]["scheduled_at"], "2026-08-28T18:10:00Z")
        self.assertEqual(row["details"]["identity"]["roster_snapshot_id"], snapshot["id"])
        self.assertEqual(dispatch.call_args.kwargs["at"], AT)

    def test_provider_dispatch_does_not_wait_for_report_lock_and_uses_entry_day(self):
        self.activate()
        clock_called = threading.Event()
        result = {}
        post_lock = "2026-08-29T18:12:00Z"  # Beijing 2026-08-30 02:12.

        def clock():
            clock_called.set()
            return post_lock

        def run():
            result.update(pipeline.dispatch(
                "matrix_works_scan", db_path=self.db, reports_root=self.reports,
            ))

        thread = threading.Thread(target=run)
        with patch.object(pipeline, "now_utc", side_effect=clock), patch.object(
            pipeline, "_dispatch", return_value={"status": "succeeded", "complete": True},
        ):
            from v8.scheduler import PIPELINE_REPORT_EXECUTION_LOCK

            with PIPELINE_REPORT_EXECUTION_LOCK:
                thread.start()
                self.assertTrue(clock_called.wait(1))
                thread.join(timeout=2)
            thread.join(timeout=2)

        self.assertFalse(thread.is_alive())
        self.assertTrue(clock_called.is_set())
        row = durable_runs.get_run(result["round_run_id"], db_path=self.db)
        self.assertEqual(row["started_at"], post_lock)
        self.assertEqual(row["details"]["identity"]["beijing_day"], "2026-08-30")
        self.assertEqual(row["details"]["identity"]["scheduled_at"], "2026-08-29T18:10:00Z")

    def test_partial_report_completes_parent_in_first_attempt(self):
        self.activate()
        with patch(
            "v8.scheduler.execute_job",
            return_value={"job_id": "daily_report", "status": "partial"},
        ) as execute:
            result = self.dispatch("daily_report")

        execute.assert_called_once()
        self.assertEqual((result["status"], result["complete"]), ("succeeded", True))
        run = durable_runs.get_run(result["round_run_id"], db_path=self.db)
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual(run["details"]["summary"]["result_status"], "partial")
        with connect(self.db) as connection:
            attempts = connection.execute(
                "SELECT COUNT(*) FROM scheduler_run_attempts WHERE scheduler_run_id=?",
                (result["round_run_id"],),
            ).fetchone()[0]
        self.assertEqual(attempts, 1)

    def test_report_duplicate_uses_exact_occurrence_terminal_status(self):
        self.activate()
        scheduled_for = "2026-08-29T00:00:00Z"
        with connect(self.db) as connection:
            connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json) "
                "VALUES ('daily_report',?,'partial',?,?, '{}')",
                (scheduled_for, scheduled_for, scheduled_for),
            )
            connection.commit()
        with patch(
            "v8.scheduler.execute_job",
            return_value={"job_id": "daily_report", "status": "skipped_duplicate"},
        ):
            result = self.dispatch("daily_report")

        self.assertEqual((result["status"], result["complete"]), ("succeeded", True))

    def test_report_duplicate_running_or_other_date_terminal_does_not_complete_parent(self):
        self.activate()
        with connect(self.db) as connection:
            connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) "
                "VALUES ('daily_report','2026-08-29T00:00:00Z','running',"
                "'2026-08-29T00:00:00Z','{}')",
            )
            connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json) "
                "VALUES ('daily_report','2026-08-28T00:00:00Z','succeeded',"
                "'2026-08-28T00:00:00Z','2026-08-28T00:01:00Z','{}')",
            )
            connection.commit()
        with patch(
            "v8.scheduler.execute_job",
            return_value={"job_id": "daily_report", "status": "skipped_duplicate"},
        ):
            result = self.dispatch("daily_report")

        self.assertEqual((result["status"], result["complete"]), ("partial", False))
        self.assertEqual(
            durable_runs.get_run(result["round_run_id"], db_path=self.db)["status"],
            "partial",
        )

    def test_same_cron_round_keeps_frozen_roster_even_after_new_acceptance(self):
        snapshot = self.activate()
        with patch.object(pipeline, "_dispatch", return_value={"status": "succeeded", "complete": True}) as dispatch:
            first = self.dispatch("matrix_works_scan")
            self.roster([], at=LATER, activate_profile=False)
            second = self.dispatch("matrix_works_scan", at=LATER)
        self.assertEqual(first["round_run_id"], second["round_run_id"])
        dispatch.assert_called_once()
        self.assertEqual(
            durable_runs.get_run(second["round_run_id"], db_path=self.db)["details"]["identity"]["roster_snapshot_id"],
            snapshot["id"],
        )

    def test_child_scans_use_claimed_roster_when_acceptance_changes_before_dispatch(self):
        snapshot = self.activate()
        original_dispatch = pipeline._dispatch

        def change_roster_then_dispatch(*args, **kwargs):
            self.roster([], at=LATER, activate_profile=False)
            return original_dispatch(*args, **kwargs)

        with patch.object(pipeline, "_dispatch", side_effect=change_roster_then_dispatch), patch(
            "v8.matrix_scan.run_matrix_scan", return_value={"complete": True, "status": "succeeded"},
        ) as scan:
            result = self.dispatch("matrix_works_scan")
        self.assertTrue(result["complete"])
        self.assertEqual(scan.call_count, 60)
        self.assertTrue(all(call.kwargs["roster_snapshot_id"] == snapshot["id"] for call in scan.call_args_list))
        self.assertTrue(all(call.kwargs["roster_snapshot_hash"] == snapshot["members_sha256"] for call in scan.call_args_list))

    def test_partial_round_reconciles_original_children_without_rebuilding_scope(self):
        snapshot = self.activate()
        child_scope = {**self.matrix_scope(snapshot, 0), "platform": "douyin"}
        child_id = self.durable("matrix_works_scan", child_scope)
        partial = {"status": "partial", "complete": False, "scans": [
            {"scheduler_run_id": child_id, "status": "partial", "complete": False},
        ]}
        with patch.object(pipeline, "_dispatch", return_value=partial) as dispatch:
            first = self.dispatch("matrix_works_scan")
            second = self.dispatch("matrix_works_scan", at=LATER)
            self.assertFalse(second["complete"])
            self.finish(child_id, at=AFTER)
            third = self.dispatch("matrix_works_scan", at=AFTER)
        dispatch.assert_called_once()
        self.assertEqual(first["round_run_id"], second["round_run_id"])
        self.assertEqual(first["round_run_id"], third["round_run_id"])
        self.assertTrue(third["complete"])
        self.assertEqual(durable_runs.get_run(child_id, db_path=self.db)["details"]["identity"], child_scope)
        checkpoint = durable_runs.get_run(first["round_run_id"], db_path=self.db)["details"]["checkpoint"]
        self.assertEqual(checkpoint["child_run_ids"], [child_id])

    def test_weekly_report_does_not_dispatch_outside_monday(self):
        self.activate()
        with patch.object(pipeline, "_dispatch") as dispatch:
            result = self.dispatch("weekly_report", at="2026-08-29T01:00:00Z")
        dispatch.assert_not_called()
        self.assertEqual(result["reason"], "round_not_due")

    def test_resumer_reuses_frozen_matrix_scope_and_injected_client(self):
        snapshot = self.activate()
        identity = {**self.matrix_scope(snapshot, 4), "platform": "douyin"}
        run_id = self.durable("matrix_works_scan", identity)
        self.roster([], at=LATER, activate_profile=False)
        client = object()
        expected = {key: value for key, value in identity.items() if key not in {"provider", "kind", "platform"}}
        with patch("v8.matrix_scan.run_matrix_scan", return_value={"scheduler_run_id": run_id, "status": "partial", "complete": False}) as scan:
            results = pipeline.resume_due_work(db_path=self.db, at=LATER, matrix_client=client)
        scan.assert_called_once_with("works", "douyin", db_path=self.db, now=LATER, client=client, **expected)
        self.assertEqual(results[0]["scheduler_run_id"], run_id)
        self.assertEqual(durable_runs.get_run(run_id, db_path=self.db)["details"]["identity"], identity)

    def test_resumer_filters_matrix_works_by_overall_end_not_slice_end(self):
        snapshot = self.activate()
        current = {**self.matrix_scope(snapshot, 12), "platform": "douyin"}
        current_id = self.durable("matrix_works_scan", current)
        old = {**current, "overall_end_at": "2026-08-27T16:00:00Z"}
        self.durable("matrix_works_scan", old)
        invalid = {**current, "overall_end_at": "not-a-time"}
        self.durable("matrix_works_scan", invalid)
        missing = dict(current)
        missing.pop("overall_end_at")
        self.durable("matrix_works_scan", missing)

        with patch(
            "v8.matrix_scan.run_matrix_scan",
            return_value={"scheduler_run_id": current_id, "status": "partial", "complete": False},
        ) as scan:
            results = pipeline.resume_due_work(db_path=self.db, at=LATER)

        self.assertEqual([item["scheduler_run_id"] for item in results], [current_id])
        scan.assert_called_once()
        self.assertEqual(scan.call_args.args, ("works", "douyin"))
        self.assertEqual(scan.call_args.kwargs["end_at"], current["end_at"])

    def test_resumer_filters_matrix_accounts_by_previous_rank_day(self):
        snapshot = self.activate()
        base = {
            "provider": "newrank_matrix",
            "kind": "accounts",
            "purpose": "daily-account-metrics",
            "platform": "douyin",
            "roster_snapshot_id": snapshot["id"],
            "roster_snapshot_hash": snapshot["members_sha256"],
        }
        current = {**base, "rank_date": "2026-08-28"}
        current_id = self.durable("matrix_account_metrics", current)
        self.durable("matrix_account_metrics", {**base, "rank_date": "2026-08-27"})
        self.durable("matrix_account_metrics", {**base, "rank_date": "not-a-date"})
        self.durable("matrix_account_metrics", base)

        with patch(
            "v8.matrix_scan.run_matrix_scan",
            return_value={"scheduler_run_id": current_id, "status": "partial", "complete": False},
        ) as scan:
            results = pipeline.resume_due_work(db_path=self.db, at=LATER)

        self.assertEqual([item["scheduler_run_id"] for item in results], [current_id])
        scan.assert_called_once()
        self.assertEqual(scan.call_args.args, ("accounts", "douyin"))
        self.assertEqual(scan.call_args.kwargs["rank_date"], "2026-08-28")

    def test_resumer_filters_tikhub_by_current_beijing_window_end(self):
        snapshot = self.activate()
        current = self.reconcile_scope(snapshot)
        current_id = self.durable("tikhub_reconcile", current)
        self.durable(
            "tikhub_reconcile",
            self.reconcile_scope(snapshot, end="2026-08-27T16:00:00Z"),
        )
        self.durable(
            "tikhub_reconcile",
            self.reconcile_scope(snapshot, end="2026-08-29T16:00:00Z"),
        )
        self.durable("tikhub_reconcile", {**current, "window_end": "invalid"})

        with patch(
            "v8.tikhub_scan.resume_account_scan",
            return_value={"scheduler_run_id": current_id, "status": "partial", "complete": False},
        ) as scan:
            results = pipeline.resume_due_work(db_path=self.db, at=LATER)

        self.assertEqual([item["scheduler_run_id"] for item in results], [current_id])
        scan.assert_called_once_with(
            current_id, db_path=self.db, max_pages=1, now=LATER, call_override=None,
        )

    def test_due_resumer_interleaves_only_tikhub_slots_before_bounded_limit(self):
        snapshot = self.activate()

        def tikhub_scope(platform, fixture):
            return {
                **self.reconcile_scope(snapshot),
                "platform": platform,
                "fixture": fixture,
            }

        xhs_oldest = self.durable(
            "tikhub_reconcile", tikhub_scope("xiaohongshu", "xhs-oldest"),
        )
        local_oldest = self.durable(
            "content_pipeline", {"kind": "content_pipeline", "fixture": "local-oldest"},
        )
        douyin_oldest = self.durable(
            "tikhub_reconcile", tikhub_scope("douyin", "douyin-oldest"),
        )
        douyin_next = self.durable(
            "tikhub_reconcile", tikhub_scope("douyin", "douyin-next"),
        )
        local_next = self.durable(
            "content_pipeline", {"kind": "content_pipeline", "fixture": "local-next"},
        )
        xhs_next = self.durable(
            "tikhub_reconcile", tikhub_scope("xiaohongshu", "xhs-next"),
        )
        calls = []

        def resume(run_id, **kwargs):
            calls.append(run_id)
            self.assertEqual(kwargs["max_pages"], 1)
            return {"scheduler_run_id": run_id, "status": "partial", "complete": False}

        def content(_kind, **kwargs):
            run_id = kwargs["resume_run_id"]
            calls.append(run_id)
            return {"scheduler_run_id": run_id, "status": "partial", "complete": False}

        with patch(
            "v8.tikhub_scan.resume_account_scan", side_effect=resume,
        ), patch.object(pipeline, "run_content_batch", side_effect=content):
            results = pipeline.resume_due_work(db_path=self.db, at=LATER, limit=4)

        self.assertEqual(
            calls,
            [xhs_oldest, local_oldest, douyin_oldest, xhs_next],
        )
        self.assertEqual(
            [item["scheduler_run_id"] for item in results],
            [xhs_oldest, local_oldest, douyin_oldest, xhs_next],
        )
        self.assertNotIn(douyin_next, calls)
        self.assertNotIn(local_next, calls)

    def test_resumer_never_automatically_runs_history_jobs(self):
        self.activate()
        self.durable("history_recovery", {"kind": "history_recovery"})
        self.durable("history_scan_catalog", {"kind": "initial"})

        with patch.object(pipeline, "run_content_batch") as content, patch.object(
            pipeline, "run_history_catalog",
        ) as catalog:
            results = pipeline.resume_due_work(db_path=self.db, at=LATER)

        self.assertEqual(results, [])
        content.assert_not_called()
        catalog.assert_not_called()

    def test_resumer_reports_drain_block_as_healthy_dispatch_deferral(self):
        snapshot = self.activate()
        run_id = self.durable(
            "matrix_works_scan",
            {**self.matrix_scope(snapshot, 0), "platform": "douyin"},
            checkpoint={
                "network_requests": 0,
                "complete": False,
            },
        )
        self.start_drain("switch-resume")

        results = pipeline.resume_due_work(
            db_path=self.db,
            at=LATER,
            reports_root=self.reports,
        )

        deferred = next(item for item in results if item["scheduler_run_id"] == run_id)
        self.assertEqual(
            (deferred["status"], deferred["reason"], deferred["deferred_reason"]),
            ("skipped", "dispatch_deferred", "profile_switch_drain"),
        )
        attempts = [
            row for row in self.rows("scheduler_run_attempts")
            if row["scheduler_run_id"] == run_id
        ]
        self.assertEqual(len(attempts), 1)
        paid_drain.seal_paid_drain("switch-resume", db_path=self.db, now=AFTER)

    def test_current_day_reconcile_uses_max_due_slots_in_planned_order(self):
        reconcile_at = "2026-08-30T21:21:00Z"  # Monday 05:21 in Beijing.
        self.activate()
        with patch.object(
            pipeline, "dispatch", return_value={"status": "succeeded", "complete": True},
        ) as dispatch:
            results = pipeline._reconcile_current_day_rounds(
                at=reconcile_at, db_path=self.db, reports_root=self.reports,
            )

        self.assertEqual(
            [item["registration_id"] for item in results],
            [
                "matrix_account_metrics", "tikhub_account_metrics",
                "matrix_works_scan", "tikhub_works_scan", "tikhub_reconcile",
            ],
        )
        self.assertEqual(
            [item["scheduled_at"] for item in results],
            [
                "2026-08-30T18:00:00Z", "2026-08-30T18:00:00Z",
                "2026-08-30T18:10:00Z", "2026-08-30T18:10:00Z",
                "2026-08-30T19:00:00Z",
            ],
        )
        self.assertEqual([call.kwargs["at"] for call in dispatch.call_args_list], [reconcile_at] * 3)
        self.assertEqual(
            [call.kwargs["registration_id"] for call in dispatch.call_args_list],
            ["matrix_account_metrics", "matrix_works_scan", "tikhub_reconcile"],
        )

    def test_current_day_reconcile_sorts_latest_slots_and_continues_after_error(self):
        reconcile_at = "2026-08-31T11:00:00Z"  # Monday 19:00 in Beijing.
        self.activate()

        def run(_job_id, **kwargs):
            if kwargs["registration_id"] == "tikhub_reconcile":
                raise RuntimeError("fixture-provider-error")
            return {"status": "succeeded", "complete": True}

        with patch.object(pipeline, "dispatch", side_effect=run) as dispatch:
            results = pipeline._reconcile_current_day_rounds(
                at=reconcile_at, db_path=self.db, reports_root=self.reports,
            )

        expected = [
            "matrix_works_scan",
            "tikhub_works_scan",
            "tikhub_reconcile",
            "metrics_backfill",
            "metrics_backfill_close",
            "daily_pipeline_summary",
            "daily_report",
            "weekly_report",
            "metrics_backfill_established",
            "matrix_account_metrics",
            "tikhub_account_metrics",
            "matrix_works_refresh",
            "tikhub_works_refresh",
        ]
        self.assertEqual([item["registration_id"] for item in results], expected)
        self.assertEqual(
            [call.kwargs["registration_id"] for call in dispatch.call_args_list],
            [item for item in expected if not item.startswith("tikhub_works")
             and item != "tikhub_account_metrics"],
        )
        failed = next(item for item in results if item["registration_id"] == "tikhub_reconcile")
        self.assertEqual((failed["status"], failed["reason"]), ("partial", "RuntimeError"))
        self.assertEqual(results[-1]["registration_id"], "tikhub_works_refresh")

    def test_current_day_reconcile_stops_new_paid_rounds_at_twenty_hundred(self):
        before_cutoff = "2026-08-31T11:59:00Z"
        at_cutoff = "2026-08-31T12:00:00Z"
        with patch.object(
            pipeline, "dispatch", return_value={"status": "succeeded", "complete": True},
        ) as dispatch:
            pipeline._reconcile_current_day_rounds(
                at=before_cutoff, db_path=self.db, reports_root=self.reports,
            )
            before_registrations = {
                call.kwargs["registration_id"] for call in dispatch.call_args_list
            }
            dispatch.reset_mock()
            results = pipeline._reconcile_current_day_rounds(
                at=at_cutoff, db_path=self.db, reports_root=self.reports,
            )
            after_registrations = {
                call.kwargs["registration_id"] for call in dispatch.call_args_list
            }

        self.assertTrue(pipeline.PAID_RECONCILE_REGISTRATIONS <= before_registrations)
        self.assertTrue(pipeline.PAID_RECONCILE_REGISTRATIONS.isdisjoint(after_registrations))
        self.assertEqual(
            after_registrations,
            {"daily_pipeline_summary", "daily_report", "weekly_report"},
        )
        skipped = {
            item["registration_id"] for item in results
            if item.get("reason") == "paid_round_reconcile_cutoff"
        }
        self.assertEqual(skipped, pipeline.PAID_RECONCILE_REGISTRATIONS)

    def test_pipeline_reconcile_resumes_current_work_without_seeding_history(self):
        self.activate()
        with patch.object(pipeline, "seed_history_work") as seed, patch.object(
            pipeline, "resume_due_work", return_value=[],
        ) as resume, patch.object(
            pipeline, "_reconcile_current_day_rounds", return_value=[],
        ) as rounds, patch(
            "v8.runtime_receipts.refresh_runtime_receipts",
            return_value={
                "status": "succeeded",
                "scan_receipts": {"errors": {}, "limit_reached": False},
                "day_receipt": {"complete": True},
            },
        ) as receipt_refresh:
            result = pipeline._dispatch(
                "pipeline_reconcile", db_path=self.db, reports_root=self.reports, at=LATER,
            )

        seed.assert_not_called()
        resume.assert_called_once()
        rounds.assert_called_once()
        receipt_refresh.assert_called_once_with(db_path=self.db, cutoff_at=LATER)
        self.assertEqual((result["status"], result["complete"]), ("succeeded", True))
        self.assertEqual(result["runtime_receipts"]["status"], "succeeded")

    def test_completed_checkpoint_resumes_finalization_only_after_cutoff(self):
        snapshot = self.activate()
        claim = self.claim_round(self.round_identity(snapshot))
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(connection, claim, {"complete": True}, now=AT)
        durable_runs.recover_run(
            claim.scheduler_run_id,
            expected_attempt_id=claim.attempt_id,
            db_path=self.db,
            now=LATER,
        )
        before_usage = self.rows("provider_usage")

        with patch.object(pipeline, "_dispatch") as business:
            results = pipeline.resume_due_work(
                db_path=self.db,
                reports_root=self.reports,
                at="2026-08-29T12:30:00Z",
            )

        business.assert_not_called()
        self.assertEqual(len(results), 1)
        self.assertEqual(
            (results[0]["status"], results[0]["complete"], results[0]["reason"]),
            ("succeeded", True, "completed_checkpoint_finalized"),
        )
        self.assertEqual(self.rows("provider_usage"), before_usage)
        run = durable_runs.get_run(claim.scheduler_run_id, db_path=self.db)
        self.assertEqual((run["status"], run["details"]["complete"]), ("succeeded", True))

    def test_completed_checkpoint_is_not_finalized_after_beijing_midnight(self):
        snapshot = self.activate()
        claim = self.claim_round(self.round_identity(snapshot))
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(connection, claim, {"complete": True}, now=AT)
        durable_runs.recover_run(
            claim.scheduler_run_id,
            expected_attempt_id=claim.attempt_id,
            db_path=self.db,
            now=LATER,
        )

        with patch.object(pipeline, "dispatch") as dispatch:
            results = pipeline.resume_due_work(
                db_path=self.db,
                reports_root=self.reports,
                at="2026-08-29T16:01:00Z",
            )

        dispatch.assert_not_called()
        self.assertEqual(results, [])
        self.assertEqual(
            durable_runs.get_run(claim.scheduler_run_id, db_path=self.db)["status"],
            "interrupted",
        )

    def test_finish_lock_recovers_then_completed_checkpoint_finishes_on_resume(self):
        self.activate()
        locked = sqlite3.OperationalError("database is locked")
        with patch.object(
            pipeline, "_dispatch", return_value={"status": "succeeded", "complete": True},
        ), patch.object(pipeline.durable_runs, "finish_run", side_effect=locked):
            first = self.dispatch("matrix_works_scan")

        self.assertEqual((first["status"], first["complete"]), ("interrupted", False))
        first_run = durable_runs.get_run(first["round_run_id"], db_path=self.db)
        self.assertTrue(first_run["details"]["checkpoint"]["complete"])
        with patch.object(pipeline, "_dispatch", side_effect=AssertionError("business reran")):
            second = self.dispatch("matrix_works_scan", at=LATER)

        self.assertEqual(
            (second["status"], second["complete"], second["reason"]),
            ("succeeded", True, "completed_checkpoint_finalized"),
        )
        with connect(self.db) as connection:
            attempts = connection.execute(
                "SELECT status FROM scheduler_run_attempts "
                "WHERE scheduler_run_id=? ORDER BY attempt_number",
                (first["round_run_id"],),
            ).fetchall()
        self.assertEqual([row["status"] for row in attempts], ["interrupted", "succeeded"])

    def test_checkpoint_failure_replays_completed_matrix_children_without_new_usage(self):
        self.activate()
        real_checkpoint = durable_runs.checkpoint
        parent_failed = False
        provider_calls = 0

        def checkpoint(connection, claim, changes, *, now=None):
            nonlocal parent_failed
            job_id = connection.execute(
                "SELECT job_id FROM scheduler_runs WHERE id=?", (claim.scheduler_run_id,),
            ).fetchone()["job_id"]
            if job_id.startswith("pipeline_round:") and not parent_failed:
                parent_failed = True
                raise sqlite3.OperationalError("database is locked")
            return real_checkpoint(connection, claim, changes, now=now)

        def matrix_scan(kind, platform, **kwargs):
            nonlocal provider_calls
            identity = {
                "provider": "newrank_matrix", "kind": kind, "platform": platform,
                **{
                    key: value for key, value in kwargs.items()
                    if key not in {"db_path", "client", "now"}
                },
            }
            claim = durable_runs.claim_run(
                "matrix_works_scan", identity, db_path=self.db, now=AT,
                initial_checkpoint={"complete": False},
            )
            if claim is not None:
                provider_calls += 1
                with connect(self.db) as connection, transaction(connection):
                    real_checkpoint(connection, claim, {"complete": True}, now=AT)
                durable_runs.finish_run(
                    claim, status="succeeded", db_path=self.db, now=AT,
                )
                run_id = claim.scheduler_run_id
            else:
                scheduled_for = "scan:" + durable_runs.scan_identity(
                    "matrix_works_scan", identity,
                )
                with connect(self.db) as connection:
                    run_id = int(connection.execute(
                        "SELECT id FROM scheduler_runs WHERE job_id=? AND scheduled_for=?",
                        ("matrix_works_scan", scheduled_for),
                    ).fetchone()["id"])
            return {"scheduler_run_id": run_id, "status": "succeeded", "complete": True}

        with patch.object(
            pipeline.durable_runs, "checkpoint", side_effect=checkpoint,
        ), patch("v8.matrix_scan.run_matrix_scan", side_effect=matrix_scan) as scan:
            first = self.dispatch("matrix_works_scan")
            self.assertEqual((first["status"], first["complete"]), ("interrupted", False))
            with connect(self.db) as connection:
                child_ids_before = [
                    int(row["id"]) for row in connection.execute(
                        "SELECT id FROM scheduler_runs WHERE job_id='matrix_works_scan' ORDER BY id",
                    )
                ]
                child_attempts_before = int(connection.execute(
                    "SELECT COUNT(*) FROM scheduler_run_attempts a "
                    "JOIN scheduler_runs r ON r.id=a.scheduler_run_id "
                    "WHERE r.job_id='matrix_works_scan'",
                ).fetchone()[0])
            second = self.dispatch("matrix_works_scan", at=LATER)

        self.assertEqual((second["status"], second["complete"]), ("succeeded", True))
        self.assertEqual(scan.call_count, 120)
        self.assertEqual(provider_calls, 60)
        with connect(self.db) as connection:
            child_ids_after = [
                int(row["id"]) for row in connection.execute(
                    "SELECT id FROM scheduler_runs WHERE job_id='matrix_works_scan' ORDER BY id",
                )
            ]
            child_attempts_after = int(connection.execute(
                "SELECT COUNT(*) FROM scheduler_run_attempts a "
                "JOIN scheduler_runs r ON r.id=a.scheduler_run_id "
                "WHERE r.job_id='matrix_works_scan'",
            ).fetchone()[0])
            parent_attempts = [
                str(row["status"]) for row in connection.execute(
                    "SELECT status FROM scheduler_run_attempts WHERE scheduler_run_id=? "
                    "ORDER BY attempt_number", (first["round_run_id"],),
                )
            ]
        self.assertEqual(child_ids_after, child_ids_before)
        self.assertEqual(child_attempts_after, child_attempts_before)
        self.assertEqual(parent_attempts, ["interrupted", "succeeded"])
        self.assertEqual(self.rows("provider_usage"), [])

    def test_checkpoint_failure_replays_completed_tikhub_child_without_new_scope_or_call(self):
        self.activate()
        real_checkpoint = durable_runs.checkpoint
        parent_failed = False
        qualification_checks = 0
        provider_calls = 0
        call_override = Mock()

        def checkpoint(connection, claim, changes, *, now=None):
            nonlocal parent_failed
            job_id = connection.execute(
                "SELECT job_id FROM scheduler_runs WHERE id=?", (claim.scheduler_run_id,),
            ).fetchone()["job_id"]
            if job_id.startswith("pipeline_round:") and not parent_failed:
                parent_failed = True
                raise sqlite3.OperationalError("database is locked")
            return real_checkpoint(connection, claim, changes, now=now)

        def tikhub_scan(identity_id, **kwargs):
            nonlocal qualification_checks, provider_calls
            qualification_checks += 1
            with connect(self.db) as connection:
                member = pipeline.require_active_member(
                    connection,
                    identity_id,
                    kwargs["roster_snapshot_id"],
                    kwargs["roster_snapshot_hash"],
                )
            identity = {
                "provider": "TikHub", "identity_id": identity_id,
                "account_id": int(member["account_id"]), "platform": member["platform"],
                "uid": member["uid"], "purpose": kwargs["purpose"],
                "window_start": kwargs["window_start"], "window_end": kwargs["window_end"],
                "roster_snapshot_id": kwargs["roster_snapshot_id"],
                "roster_snapshot_hash": kwargs["roster_snapshot_hash"],
            }
            claim = durable_runs.claim_run(
                "tikhub_reconcile", identity, db_path=self.db, now=AT,
                initial_checkpoint={"complete": False},
            )
            if claim is not None:
                provider_calls += 1
                with connect(self.db) as connection, transaction(connection):
                    real_checkpoint(connection, claim, {"complete": True}, now=AT)
                durable_runs.finish_run(
                    claim, status="succeeded", db_path=self.db, now=AT,
                )
                run_id = claim.scheduler_run_id
            else:
                scheduled_for = "scan:" + durable_runs.scan_identity(
                    "tikhub_reconcile", identity,
                )
                with connect(self.db) as connection:
                    run_id = int(connection.execute(
                        "SELECT id FROM scheduler_runs WHERE job_id=? AND scheduled_for=?",
                        ("tikhub_reconcile", scheduled_for),
                    ).fetchone()["id"])
            return {"scheduler_run_id": run_id, "status": "succeeded", "complete": True}

        with patch.object(
            pipeline.durable_runs, "checkpoint", side_effect=checkpoint,
        ), patch("v8.tikhub_scan.run_account_scan", side_effect=tikhub_scan):
            first = self.dispatch("tikhub_reconcile", call_override=call_override)
            second = self.dispatch("tikhub_reconcile", at=LATER, call_override=call_override)

        self.assertEqual((first["status"], first["complete"]), ("interrupted", False))
        self.assertEqual((second["status"], second["complete"]), ("succeeded", True))
        self.assertEqual((qualification_checks, provider_calls), (2, 1))
        call_override.assert_not_called()
        with connect(self.db) as connection:
            children = connection.execute(
                "SELECT id FROM scheduler_runs WHERE job_id='tikhub_reconcile'",
            ).fetchall()
            child_attempts = int(connection.execute(
                "SELECT COUNT(*) FROM scheduler_run_attempts a "
                "JOIN scheduler_runs r ON r.id=a.scheduler_run_id "
                "WHERE r.job_id='tikhub_reconcile'",
            ).fetchone()[0])
        self.assertEqual((len(children), child_attempts), (1, 1))
        self.assertEqual(self.rows("provider_usage"), [])

    def test_checkpoint_replay_resumes_due_partial_tikhub_on_same_child_scope(self):
        self.activate()
        real_checkpoint = durable_runs.checkpoint
        parent_failed = False
        provider_call = Mock(return_value={"code": 200, "data": {}})
        invocations = 0

        def checkpoint(connection, claim, changes, *, now=None):
            nonlocal parent_failed
            job_id = connection.execute(
                "SELECT job_id FROM scheduler_runs WHERE id=?", (claim.scheduler_run_id,),
            ).fetchone()["job_id"]
            if job_id.startswith("pipeline_round:") and not parent_failed:
                parent_failed = True
                raise sqlite3.OperationalError("database is locked")
            return real_checkpoint(connection, claim, changes, now=now)

        def tikhub_scan(identity_id, **kwargs):
            nonlocal invocations
            invocations += 1
            now = AT if invocations == 1 else AFTER
            identity = {
                "provider": "TikHub", "identity_id": identity_id,
                "purpose": kwargs["purpose"], "window_start": kwargs["window_start"],
                "window_end": kwargs["window_end"],
                "roster_snapshot_id": kwargs["roster_snapshot_id"],
                "roster_snapshot_hash": kwargs["roster_snapshot_hash"],
            }
            claim = durable_runs.claim_run(
                "tikhub_reconcile", identity, db_path=self.db, now=now,
                initial_checkpoint={"complete": False},
            )
            self.assertIsNotNone(claim)
            provider_call("fixture-page", {"attempt": invocations})
            complete = invocations == 2
            with connect(self.db) as connection, transaction(connection):
                real_checkpoint(connection, claim, {"complete": complete}, now=now)
            durable_runs.finish_run(
                claim,
                status="succeeded" if complete else "partial",
                db_path=self.db,
                now=now,
                next_resume_at=None if complete else LATER,
            )
            return {
                "scheduler_run_id": claim.scheduler_run_id,
                "status": "succeeded" if complete else "partial",
                "complete": complete,
            }

        with patch.object(
            pipeline.durable_runs, "checkpoint", side_effect=checkpoint,
        ), patch("v8.tikhub_scan.run_account_scan", side_effect=tikhub_scan):
            first = self.dispatch("tikhub_reconcile", call_override=provider_call)
            second = self.dispatch("tikhub_reconcile", at=AFTER, call_override=provider_call)

        self.assertEqual((first["status"], first["complete"]), ("interrupted", False))
        self.assertEqual((second["status"], second["complete"]), ("succeeded", True))
        self.assertEqual(provider_call.call_count, 2)
        with connect(self.db) as connection:
            children = connection.execute(
                "SELECT id FROM scheduler_runs WHERE job_id='tikhub_reconcile'",
            ).fetchall()
            attempts = connection.execute(
                "SELECT a.status FROM scheduler_run_attempts a "
                "JOIN scheduler_runs r ON r.id=a.scheduler_run_id "
                "WHERE r.job_id='tikhub_reconcile' ORDER BY a.attempt_number",
            ).fetchall()
        self.assertEqual(len(children), 1)
        self.assertEqual([row["status"] for row in attempts], ["partial", "succeeded"])

    def test_checkpoint_lock_retries_only_recovery_with_fixed_backoff(self):
        self.activate()
        real_recover = durable_runs.recover_run
        recovery_calls = 0

        def recover(*args, **kwargs):
            nonlocal recovery_calls
            recovery_calls += 1
            if recovery_calls < 3:
                raise sqlite3.OperationalError("database is locked")
            return real_recover(*args, **kwargs)

        with patch.object(
            pipeline, "_dispatch", return_value={"status": "succeeded", "complete": True},
        ), patch.object(
            pipeline.durable_runs, "checkpoint",
            side_effect=sqlite3.OperationalError("database is locked"),
        ) as checkpoint, patch.object(
            pipeline.durable_runs, "recover_run", side_effect=recover,
        ) as recover_run, patch.object(pipeline, "sleep") as sleep:
            result = self.dispatch("matrix_works_scan")

        self.assertEqual((result["status"], result["complete"]), ("interrupted", False))
        checkpoint.assert_called_once()
        self.assertEqual(recover_run.call_count, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [5, 10])
        self.assertEqual(
            durable_runs.get_run(result["round_run_id"], db_path=self.db)["status"],
            "interrupted",
        )

    def test_non_lock_checkpoint_error_recovers_before_original_error_is_raised(self):
        self.activate()
        error = durable_runs.DurableRunError("fixture checkpoint failure")
        with patch.object(
            pipeline, "_dispatch", return_value={"status": "succeeded", "complete": True},
        ), patch.object(pipeline.durable_runs, "checkpoint", side_effect=error):
            with self.assertRaisesRegex(durable_runs.DurableRunError, "fixture checkpoint failure"):
                self.dispatch("matrix_works_scan")

        with connect(self.db) as connection:
            run = connection.execute(
                "SELECT status FROM scheduler_runs WHERE job_id='pipeline_round:matrix_works_scan'",
            ).fetchone()
        self.assertEqual(run["status"], "interrupted")

    def test_recovery_exhaustion_raises_explicit_finalization_error(self):
        self.activate()
        locked = sqlite3.OperationalError("database is locked")
        with patch.object(
            pipeline, "_dispatch", return_value={"status": "succeeded", "complete": True},
        ), patch.object(
            pipeline.durable_runs, "checkpoint", side_effect=locked,
        ), patch.object(
            pipeline.durable_runs, "recover_run", side_effect=locked,
        ) as recover, patch.object(pipeline, "sleep") as sleep:
            with self.assertRaises(pipeline.PipelineFinalizationError) as raised:
                self.dispatch("matrix_works_scan")

        self.assertEqual(raised.exception.stage, "checkpoint")
        self.assertEqual(recover.call_count, 6)
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list], [5, 10, 20, 40, 60],
        )

    def test_sqlite_lock_classifier_uses_primary_codes_and_rejects_plain_runtime_errors(self):
        for code in (5, 6, 517, 262):
            error = sqlite3.OperationalError("fixture")
            error.sqlite_errorcode = code
            self.assertTrue(pipeline._is_sqlite_busy_or_locked(error))
        self.assertTrue(
            pipeline._is_sqlite_busy_or_locked(
                sqlite3.OperationalError("database schema is locked"),
            )
        )
        self.assertFalse(
            pipeline._is_sqlite_busy_or_locked(RuntimeError("database is locked")),
        )
        integrity = sqlite3.IntegrityError("constraint failed")
        integrity.sqlite_errorcode = sqlite3.SQLITE_CONSTRAINT
        self.assertFalse(pipeline._is_sqlite_busy_or_locked(integrity))

    def test_recovery_false_accepts_terminal_state_and_fences_new_owner(self):
        snapshot = self.activate()
        terminal_claim = self.claim_round(self.round_identity(snapshot))
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(connection, terminal_claim, {"complete": True}, now=AT)
        durable_runs.finish_run(
            terminal_claim, status="succeeded", db_path=self.db, now=AT,
        )
        terminal = pipeline._recover_pipeline_round_attempt(
            terminal_claim, db_path=self.db, stage="finish",
        )
        self.assertEqual((terminal["status"], terminal["complete"]), ("succeeded", True))

        other_identity = self.round_identity(snapshot, key="matrix_account_metrics")
        old_claim = self.claim_round(other_identity)
        durable_runs.recover_run(
            old_claim.scheduler_run_id,
            expected_attempt_id=old_claim.attempt_id,
            db_path=self.db,
            now=LATER,
        )
        new_claim = self.claim_round(other_identity, at=AFTER)
        self.assertNotEqual(old_claim.attempt_id, new_claim.attempt_id)
        with self.assertRaises(durable_runs.LostOwnership):
            pipeline._recover_pipeline_round_attempt(
                old_claim, db_path=self.db, stage="checkpoint",
            )

    def test_history_catalog_finishes_partial_child_with_same_frozen_request(self):
        snapshot = self.activate()
        identity = {**self.matrix_scope(snapshot, 5), "platform": "douyin"}
        child_id = self.durable("matrix_works_scan", identity)
        pipeline._enqueue_catalog(
            {
                "pipeline_version": pipeline.PIPELINE_VERSION,
                "kind": "initial",
                "fixture": "frozen",
                **self.activation_identity(),
            },
            [identity], at=AT, db_path=self.db,
        )
        catalog_id = self.catalogs()[0]["id"]
        client = object()
        call_count = 0

        def scan(**kwargs):
            nonlocal call_count
            call_count += 1
            self.assertEqual(kwargs, {
                **{key: value for key, value in identity.items() if key != "provider"},
                "db_path": self.db, "client": client, "now": AT if call_count == 1 else LATER,
            })
            if call_count == 2:
                self.finish(child_id, at=LATER)
            return {"scheduler_run_id": child_id, "status": "partial" if call_count == 1 else "succeeded", "complete": call_count == 2}

        with patch("v8.matrix_scan.run_matrix_scan", side_effect=scan):
            first = pipeline.run_history_catalog(catalog_id, db_path=self.db, at=AT, matrix_client=client)
            self.roster([], at=LATER, activate_profile=False)
            second = pipeline.run_history_catalog(catalog_id, db_path=self.db, at=LATER, matrix_client=client)
        self.assertFalse(first["complete"])
        self.assertTrue(second["complete"])
        self.assertEqual(call_count, 2)
        checkpoint = durable_runs.get_run(catalog_id, db_path=self.db)["details"]["checkpoint"]
        self.assertEqual(checkpoint["items"], [identity])
        self.assertEqual(checkpoint["pending_indices"], [])
        self.assertEqual(checkpoint["children"]["0"]["scheduler_run_id"], child_id)

    def test_history_catalog_alternates_only_tikhub_children_and_bounds_each_to_one_page(self):
        douyin_next = self.account_row("100000000002")
        xhs_oldest = self.account_row("a" * 24, platform="xiaohongshu")
        xhs_next = self.account_row("b" * 24, platform="xiaohongshu")
        with connect(self.db) as connection:
            douyin_next_id = int(connection.execute(
                "SELECT id FROM account_platform_identities WHERE account_id=?",
                (douyin_next["id"],),
            ).fetchone()[0])
            xhs_oldest_id = int(connection.execute(
                "SELECT id FROM account_platform_identities WHERE account_id=?",
                (xhs_oldest["id"],),
            ).fetchone()[0])
            xhs_next_id = int(connection.execute(
                "SELECT id FROM account_platform_identities WHERE account_id=?",
                (xhs_next["id"],),
            ).fetchone()[0])
        snapshot = self.activate()
        base = {
            "roster_snapshot_id": snapshot["id"],
            "roster_snapshot_hash": snapshot["members_sha256"],
            **self.activation_identity(),
            "purpose": "history",
            "window_start": "2026-08-02T16:00:00Z",
            "window_end": "2026-08-20T16:00:00Z",
        }
        matrix_douyin = {**self.matrix_scope(snapshot, 5), "platform": "douyin"}
        matrix_xhs = {**self.matrix_scope(snapshot, 4), "platform": "xiaohongshu"}
        requests = [
            matrix_douyin,
            {**base, "provider": "tikhub", "identity_id": xhs_oldest_id},
            {**base, "provider": "tikhub", "identity_id": self.identity_id},
            {**base, "provider": "tikhub", "identity_id": douyin_next_id},
            matrix_xhs,
            {**base, "provider": "tikhub", "identity_id": xhs_next_id},
        ]
        pipeline._enqueue_catalog(
            {
                "pipeline_version": pipeline.PIPELINE_VERSION,
                "kind": "initial",
                "fixture": "fair",
                **self.activation_identity(),
            },
            requests, at=AT, db_path=self.db,
        )
        catalog_id = self.catalogs()[0]["id"]
        calls = []

        def matrix_scan(**kwargs):
            calls.append(("matrix", kwargs["platform"]))
            return {"scheduler_run_id": 100 + len(calls), "status": "succeeded", "complete": True}

        def tikhub_scan(identity_id, **kwargs):
            calls.append(("tikhub", identity_id))
            self.assertEqual(kwargs["max_pages"], 1)
            self.assertEqual(
                {key: kwargs[key] for key in (
                    "activation_id", "profile_id", "activation_sha256"
                )},
                self.activation_identity(),
            )
            return {"scheduler_run_id": 100 + len(calls), "status": "succeeded", "complete": True}

        with patch(
            "v8.matrix_scan.run_matrix_scan", side_effect=matrix_scan,
        ), patch(
            "v8.tikhub_scan.run_account_scan", side_effect=tikhub_scan,
        ) as scan:
            result = pipeline.run_history_catalog(catalog_id, db_path=self.db, at=AT)

        self.assertTrue(result["complete"])
        self.assertEqual(calls, [
            ("matrix", "douyin"),
            ("tikhub", xhs_oldest_id),
            ("tikhub", self.identity_id),
            ("tikhub", xhs_next_id),
            ("matrix", "xiaohongshu"),
            ("tikhub", douyin_next_id),
        ])
        self.assertEqual([call.kwargs["max_pages"] for call in scan.call_args_list], [1] * 4)

if __name__ == "__main__":
    unittest.main()
