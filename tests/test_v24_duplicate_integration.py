"""Runtime and read-side integration; all databases and providers are isolated."""
from __future__ import annotations

import csv
import io
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from apscheduler.schedulers.background import BackgroundScheduler
from tests import test_v8_local_content_analysis as media_fixture
from tests.test_v24_duplicate_index import create_schema24_fixture, add_fingerprint
from v8 import duplicate_index as index, duplicate_runtime, duplicate_readiness as readiness
from v8 import operations, pipeline, schema_v24, storage
from v8.contracts import quality_gate_failures
from v8.duplicates import FINGERPRINT_VERSION, THRESHOLDS
try:
    from v8.read_cache import DatabaseRevision
except ModuleNotFoundError:
    DatabaseRevision = None


def calibrate(c):
    c.execute("INSERT INTO duplicate_calibration_runs(id,calibration_version,fingerprint_version,dataset_sha256,pair_count,positive_count,negative_count,"
        "predicted_positive_count,true_positive_count,false_positive_count,precision,recall,thresholds_json,status,created_at) "
        "VALUES('v24-test','test',?,'test',150,75,75,75,75,0,1,1,?,'passed',?)",
        (FINGERPRINT_VERSION, json.dumps(THRESHOLDS, sort_keys=True, separators=(",", ":")), storage.now_utc()))


class DuplicateReadIntegrationTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.db = Path(temp.name) / "consumer.sqlite3"
        self.c, self.gid = create_schema24_fixture(self.db)
        self.addCleanup(self.c.close)
        calibrate(self.c)
        self.ids = [add_fingerprint(self.c, media=media)[0] for media in (["a"], ["a", "b"], ["b"])]
        self.c.commit()

    def visible(self):
        return list(self.c.execute("SELECT d.* FROM duplicate_relations d WHERE d.status='confirmed' AND "
                                  + readiness.valid_relation_sql(self.c, "d")))

    def drain(self):
        result = duplicate_runtime.drain_duplicate_work(db_path=self.db)
        self.assertEqual(result["relation_status"], "ready", result)
        return result

    def test_dirty_component_is_hidden_then_atomic_ack_restores_coverage(self):
        self.assertEqual(readiness.relation_coverage(self.c, self.ids)["duplicate_relation_pending"], 3)
        self.drain()
        self.assertEqual(len(self.visible()), 2)
        self.assertEqual(readiness.relation_coverage(self.c, self.ids)["duplicate_relation_coverage"], 100)
        with storage.transaction(self.c):
            index.mark_content_dirty(self.c, self.ids[1])
        # Retained immutable/display rows must be suppressed while one member is dirty.
        self.assertEqual(self.c.execute("SELECT COUNT(*) FROM duplicate_relations").fetchone()[0], 2)
        self.assertEqual(self.visible(), [])
        self.assertEqual(readiness.relation_coverage(self.c, self.ids)["duplicate_relation_pending"], 3)
        self.drain()
        self.assertEqual(len(self.visible()), 2)

    def test_text_source_invalidated_before_write_and_rollback_is_atomic(self):
        self.drain()
        cid = self.ids[1]
        old = dict(self.c.execute("SELECT * FROM content_items WHERE id=?", (cid,)).fetchone())
        fingerprint_count = self.c.execute("SELECT count(*) FROM duplicate_fingerprints").fetchone()[0]
        self.c.execute("BEGIN")
        operations._invalidate_duplicate_inputs(self.c, cid, old, {"title": "new unrelated source title"})
        self.assertFalse(readiness.relation_states(self.c, [cid])[cid]["fingerprint_available"])
        self.assertEqual(self.c.execute("SELECT COUNT(*) FROM content_items c WHERE "
                         + readiness.current_fingerprint_sql(self.c, "c.id")).fetchone()[0], 2)
        self.assertEqual(self.visible(), [])
        self.assertEqual(self.c.execute("SELECT count(*) FROM duplicate_fingerprints").fetchone()[0], fingerprint_count)
        self.c.rollback()
        self.assertEqual(len(self.visible()), 2)
        with storage.transaction(self.c):
            operations._invalidate_duplicate_inputs(self.c, cid, old, {"title": "  " + old["title"].upper() + "  "})
        self.assertEqual(len(self.visible()), 2)

    def test_date_change_preserves_fingerprint_and_reselects_canonical(self):
        self.drain()
        cid = self.ids[2]
        old = dict(self.c.execute("SELECT * FROM content_items WHERE id=?", (cid,)).fetchone())
        fid = self.c.execute("SELECT fingerprint_id FROM duplicate_current_fingerprints WHERE content_id=?", (cid,)).fetchone()[0]
        with storage.transaction(self.c):
            operations._invalidate_duplicate_inputs(self.c, cid, old, {"published_at": "2000-01-01T00:00:00Z"})
            self.c.execute("UPDATE content_items SET published_at='2000-01-01T00:00:00Z' WHERE id=?", (cid,))
        self.assertEqual(self.c.execute("SELECT fingerprint_id FROM duplicate_current_fingerprints WHERE content_id=?", (cid,)).fetchone()[0], fid)
        self.assertEqual(self.visible(), [])
        self.drain()
        self.assertEqual({row["original_content_id"] for row in self.visible()}, {cid})

    def test_logical_identity_merge_keeps_evidence_and_acknowledges_loser(self):
        from v8.api import _content_search, ContentSearchRequest
        self.drain()
        winner, loser = self.ids[:2]
        with storage.transaction(self.c):
            operations._merge_content_records_schema20(self.c,
                self.c.execute("SELECT * FROM content_items WHERE id=?", (winner,)).fetchone(),
                self.c.execute("SELECT * FROM content_items WHERE id=?", (loser,)).fetchone(), histories={})
        self.assertIsNotNone(self.c.execute("SELECT id FROM content_items WHERE id=?", (loser,)).fetchone())
        self.assertEqual(readiness.relation_states(self.c, [loser])[loser]["relation_status"], "pending")
        self.assertEqual(_content_search(ContentSearchRequest(), db_path=self.db)["total"], 2)
        self.drain()
        self.assertEqual(readiness.relation_states(self.c, [loser])[loser]["relation_status"], "ready")
        self.assertEqual(readiness.relation_coverage(self.c, [loser])["duplicate_relation_coverage"], 100)
        self.assertEqual(readiness.relation_coverage(self.c, [loser], cutoff_at="2000-01-01T00:00:00Z")["duplicate_relation_coverage"], 0)
        self.assertFalse(readiness.relation_states(self.c, [loser])[loser]["fingerprint_available"])
        self.assertEqual(self.c.execute("SELECT count(*) FROM duplicate_relations WHERE method='identity_merge' "
                                      "AND duplicate_content_id=?", (loser,)).fetchone()[0], 1)
        self.assertEqual(self.c.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_unfingerprinted_legacy_text_row_remains_pending_without_deleting_history(self):
        self.c.execute("INSERT INTO content_items(link_id,platform,canonical_url,title,body,published_at,imported_at,created_at,updated_at) "
                       "VALUES('OLD001','douyin','old','unprocessed text','body','2026-01-01','2026-01-01','2026-01-01','2026-01-01')")
        cid = self.c.execute("SELECT id FROM content_items WHERE link_id='OLD001'").fetchone()[0]
        self.c.execute("INSERT INTO duplicate_relations(duplicate_content_id,original_content_id,method,confidence,evidence_json,status,created_at) "
                       "VALUES(?,?,'text_sha256',1,'{}','confirmed','2026-01-01')", (cid, self.ids[0]))
        self.c.commit()
        self.assertEqual(self.visible(), [])
        self.assertEqual(readiness.relation_states(self.c, [cid])[cid]["relation_status"], "pending")
        self.assertEqual(readiness.relation_coverage(self.c, [cid])["duplicate_relation_coverage"], 0)
        statements = []
        self.c.set_trace_callback(statements.append)
        operations._rebuild_text_duplicate_groups(self.c, {"anything"}, touched_content_ids={cid})
        self.c.set_trace_callback(None)
        self.assertFalse(any("FROM content_items ORDER BY id" in sql for sql in statements))
        self.assertEqual(self.c.execute("SELECT count(*) FROM duplicate_relations WHERE method='text_sha256'").fetchone()[0], 1)

    def test_content_and_report_exports_preserve_pending_status(self):
        from tests.v9_report_fixture import activate_v9_report_fixture
        from v8 import reports
        activate_v9_report_fixture(self.db, [])
        output = operations.export_contents_csv(db_path=self.db).decode("utf-8-sig")
        exported = list(csv.DictReader(io.StringIO(output)))
        self.assertEqual({row["relation_status"] for row in exported}, {"pending"})
        path = self.db.parent / "report.csv"
        reports._write_csv(path, [{"content_id": 1, "link_id": "000001", "relation_status": "pending"}])
        exported = list(csv.DictReader(io.StringIO(path.read_text(encoding="utf-8-sig"))))
        self.assertEqual(exported[0]["relation_status"], "pending")

    def test_report_cutoff_does_not_claim_a_later_ack_was_already_complete(self):
        self.drain()
        cutoff = "2000-01-01T00:00:00Z"
        self.assertEqual({row["relation_status"] for row in readiness.relation_states(self.c, self.ids, cutoff_at=cutoff).values()}, {"pending"})
        self.assertEqual(readiness.relation_coverage(self.c, self.ids, cutoff_at=cutoff)["duplicate_relation_ready"], 0)
        self.assertEqual(self.c.execute("SELECT COUNT(*) FROM content_items c WHERE "
                         + readiness.current_fingerprint_sql(self.c, "c.id", cutoff_at=cutoff)).fetchone()[0], 0)

    @unittest.skipIf(DatabaseRevision is None, "not part of installed r4 source")
    def test_relation_generation_changes_invalidate_cache_without_new_rows(self):
        revision = DatabaseRevision(self.db, check_interval=0)
        self.addCleanup(revision.close)
        before = revision.get("contents")
        with storage.transaction(self.c):
            index.mark_content_dirty(self.c, self.ids[0])
        self.assertNotEqual(before, revision.get("contents"))

    def test_report_gate_requires_all_current_relations_acknowledged(self):
        failures = quality_gate_failures({"duplicate_relation_coverage": 99.0})
        self.assertIn("duplicate_relation_coverage", [item["key"] for item in failures])
        failures = quality_gate_failures({"duplicate_relation_coverage": 100.0})
        self.assertNotIn("duplicate_relation_coverage", [item["key"] for item in failures])

    def test_api_content_read_exposes_pending_instead_of_definite_negative(self):
        from v8.api import _content_search, ContentSearchRequest
        result = _content_search(ContentSearchRequest(), db_path=self.db)
        self.assertEqual(len(result["items"]), 3, result)
        self.assertEqual({item["relation_status"] for item in result["items"]}, {"pending"})
        self.drain()
        result = _content_search(ContentSearchRequest(), db_path=self.db)
        self.assertEqual({item["relation_status"] for item in result["items"]}, {"ready"})

    def test_writer_start_closes_restore_before_recovery_but_read_only_does_not(self):
        from dataclasses import replace
        from fastapi.testclient import TestClient
        from v8 import api
        config = api.ApiConfig(db_path=self.db, reports_root=self.db.parent / "reports",
            legacy_db_path=self.db.parent / "legacy.sqlite3",
            operator_freeze_lock=self.db.parent / "freeze", writer_lock=self.db.parent / "writer.lock")
        self.c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        def count():
            with storage.connect(self.db) as c:
                return c.execute("SELECT COUNT(*) FROM duplicate_work_staging WHERE work_content_id=0 "
                                 "AND record_type='runtime_traffic_started'").fetchone()[0]
        with TestClient(api.create_app(replace(config, read_only=True))):
            self.assertEqual(count(), 0)
        original = api.recover_interrupted_scheduler_runs
        def recover(**kwargs):
            self.assertEqual(count(), 1, "traffic marker must commit before recovery")
            return original(**kwargs)
        with patch.object(api, "recover_interrupted_scheduler_runs", side_effect=recover):
            with TestClient(api.create_app(config)):
                self.assertEqual(count(), 1)
            with TestClient(api.create_app(config)):
                self.assertEqual(count(), 1, "restart must not create a second marker")

    def test_fresh_isolated_api_bootstraps_empty_index_without_calibration(self):
        from fastapi.testclient import TestClient
        from v8 import api
        new_db = self.db.parent / "fresh.sqlite3"
        config = api.ApiConfig(db_path=new_db, reports_root=self.db.parent / "reports",
            legacy_db_path=self.db.parent / "legacy.sqlite3",
            operator_freeze_lock=self.db.parent / "freeze", writer_lock=self.db.parent / "writer.lock")
        with TestClient(api.create_app(config)), storage.connect(new_db) as c:
            version = c.execute("PRAGMA user_version").fetchone()[0]
            if version == 19:
                # Installed r4 intentionally retains its schema19 fresh-fixture
                # default; the task does not change that unrelated bootstrap.
                self.assertEqual(c.execute("SELECT count(*) FROM content_items").fetchone()[0], 0)
                return
            self.assertEqual(version, 24)
            self.assertEqual(c.execute("SELECT count(*) FROM duplicate_index_generations WHERE state='ready'").fetchone()[0], 1)
            self.assertEqual(c.execute("SELECT count(*) FROM duplicate_calibration_runs WHERE status='passed'").fetchone()[0], 0)
            self.assertEqual(c.execute("SELECT count(*) FROM content_items").fetchone()[0], 0)
        with storage.connect(new_db) as c:
            c.execute("UPDATE duplicate_index_generations SET state='retired'")
        with self.assertRaisesRegex(Exception, "ready duplicate generation"):
            with TestClient(api.create_app(config)):
                pass


class IndexedPaidGateIntegrationTest(unittest.TestCase):
    def setUp(self):
        from tests import test_v8_paid_drain as paid_fixture
        self.fixture = paid_fixture.PaidDrainTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        with storage.connect(self.fixture.db) as c:
            storage.initialize_database(c, target_version=23)
            schema_v24.migrate(c)

    def test_index_keeps_mutated_bridge_job_visible_to_live_guard(self):
        import sqlite3
        from tests import test_v8_paid_drain as paid_fixture
        from v8 import paid_drain
        receipt = paid_drain.start_paid_drain("v24-index-identity", binding=paid_fixture.binding(),
            db_path=self.fixture.db, now=paid_fixture.STARTED_AT)
        with storage.connect(self.fixture.db) as c, storage.transaction(c):
            # Schema20+ strengthens the older guard with immutable run identity.
            with self.assertRaisesRegex(sqlite3.IntegrityError, "durable identity is immutable"):
                c.execute("UPDATE scheduler_runs SET job_id='hidden' WHERE id=?", (receipt.run_id,))
            with self.assertRaises(paid_drain.PaidDrainBlocked):
                paid_drain.require_paid_dispatch_open(c, provider="TikHub", operation="douyin_user_posts")

    def test_index_keeps_run_payload_tampering_fail_closed(self):
        self.fixture.test_run_tampering_is_invalid_and_fail_closed()

    def test_paid_guard_uses_covering_expression_index(self):
        with storage.connect(self.fixture.db) as c:
            plan = [row[3] for row in c.execute("EXPLAIN QUERY PLAN SELECT scheduler_run_id "
                "FROM scheduler_run_attempts WHERE json_extract(details_json,'$.contract_version')=?",
                ("pipeline-paid-drain-bridge-v1",))]
            self.assertTrue(any("USING COVERING INDEX idx_scheduler_attempts_contract_run_v24" in row for row in plan), plan)


class IndexedLocalAnalysisIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.fixture = media_fixture.LocalContentAnalysisTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.db = self.fixture.db
        with storage.connect(self.db) as c:
            storage.initialize_database(c, target_version=23)
            schema_v24.migrate(c)
            generation = index.create_generation(c)
            c.execute("UPDATE duplicate_index_generations SET state='ready' WHERE generation_id=?", (generation["generation_id"],))
            calibrate(c)
            c.commit()

    def test_pending_relations_do_not_redownload_or_rerun_media_on_next_tick(self):
        self.fixture.content()
        real_drain = pipeline.run_duplicate_relation_update
        with patch.object(pipeline, "run_duplicate_relation_update", return_value={"relation_status": "pending", "results": []}):
            result = self.fixture.run_tick()
            self.assertFalse(result["complete"], result)
            self.assertTrue(result["results"][0]["media_complete"], result)
            self.assertEqual(result["results"][0]["status"], "succeeded")
            again = self.fixture.run_tick(media_fixture.after(300))
        self.assertEqual(again["processed"], 0)
        self.assertFalse(again["complete"])
        self.fixture.download.assert_called_once()
        self.fixture.asr.assert_called_once()
        self.fixture.ocr.assert_called_once()
        recovered = real_drain(db_path=self.db)
        self.assertEqual(recovered["relation_status"], "ready", recovered)
        finished = self.fixture.run_tick(media_fixture.after(600))
        self.assertEqual(finished["processed"], 0)
        self.assertTrue(finished["complete"], finished)
        self.fixture.assert_no_provider()

    def test_soft_budget_stops_starting_media_at_55_seconds_and_reserves_relations(self):
        self.fixture.content(1)
        self.fixture.content(2)
        clock = [0.0]
        original = pipeline.run_local_batch
        def slow_one(ids, **kwargs):
            value = original(ids, **kwargs)
            clock[0] = 56.0
            return value
        with patch.object(pipeline, "monotonic", side_effect=lambda: clock[0]), \
             patch.object(pipeline, "run_local_batch", side_effect=slow_one) as media_run, \
             patch.object(pipeline, "run_duplicate_relation_update", return_value={"relation_status": "pending", "results": []}) as drain:
            result = self.fixture.run_tick()
        self.assertEqual(result["processed"], 1, result)
        media_run.assert_called_once()
        self.assertFalse(media_run.call_args.kwargs["process_relations"])
        self.assertEqual(drain.call_args.kwargs["time_budget_seconds"], 4.0)
        self.assertEqual(drain.call_args.kwargs["limit"], 20)
        self.fixture.assert_no_provider()

    def test_fingerprint_failure_is_per_content_and_does_not_block_next_media(self):
        from v8 import duplicates
        self.fixture.content(1)
        self.fixture.content(2)
        original = duplicates.fingerprint_content
        def flaky(cid, **kwargs):
            if cid == 2:  # newest content is dispatched first
                raise duplicates.DuplicateDetectionError("fixture fingerprint failure")
            return original(cid, **kwargs)
        with patch.object(duplicates, "fingerprint_content", side_effect=flaky):
            result = self.fixture.run_tick()
        self.assertEqual(result["processed"], 2, result)
        rows = {item["content_id"]: item for item in result["results"]}
        self.assertFalse(rows[2]["media_complete"])
        self.assertTrue(any("fixture fingerprint failure" in str(error) for error in rows[2]["errors"]))
        self.assertTrue(rows[1]["complete"], result)
        self.fixture.assert_no_provider()

    def test_reinvalidated_same_source_recovers_fingerprint_without_repeating_media(self):
        self.fixture.content()
        first = self.fixture.run_tick()
        self.assertTrue(first["complete"], first)
        with storage.connect(self.db) as c, storage.transaction(c):
            index.invalidate_content(c, 1, reason="artifact_revalidated")
        again = self.fixture.run_tick(media_fixture.after(300))
        self.assertTrue(again["complete"], again)
        self.assertEqual(again["processed"], 1)
        self.assertNotEqual(first["results"][0]["scheduler_run_id"], again["results"][0]["scheduler_run_id"])
        self.fixture.download.assert_called_once()
        self.fixture.asr.assert_called_once()
        self.fixture.ocr.assert_called_once()
        self.fixture.assert_no_provider()

    def test_independent_relation_job_is_local_minutely_and_bounded(self):
        scheduler = BackgroundScheduler()
        pipeline.install_pipeline_jobs(scheduler, db_path=self.db, reports_root=self.fixture.root / "reports",
                                       authorization_effective_date=media_fixture.START)
        jobs = {job.id: job for job in scheduler.get_jobs()}
        job = jobs[pipeline.DUPLICATE_RELATION_JOB]
        self.assertIs(job.func, pipeline.run_duplicate_relation_update)
        self.assertEqual(job.trigger.interval.total_seconds(), 60)
        self.assertEqual(job.max_instances, 1)
        self.assertEqual(job.executor, pipeline.SCHEDULER_DUPLICATE_EXECUTOR)
        self.assertNotIn(pipeline.DUPLICATE_RELATION_JOB, pipeline.PAID_DISPATCH_JOBS)
        self.assertEqual(jobs[pipeline.LOCAL_ANALYSIS_JOB].trigger.interval.total_seconds(), 300)

    def test_ready_relation_job_finishes_while_reconcile_worker_is_occupied(self):
        from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

        scheduler = BackgroundScheduler(executors={
            "default": {"type": "threadpool", "max_workers": 1},
            "reconcile": {"type": "threadpool", "max_workers": 1},
            "duplicate": {"type": "threadpool", "max_workers": 1},
        })
        pipeline.install_pipeline_jobs(scheduler, db_path=self.db,
            reports_root=self.fixture.root / "reports", authorization_effective_date=media_fixture.START)
        for job in scheduler.get_jobs():
            if job.id != pipeline.DUPLICATE_RELATION_JOB:
                scheduler.remove_job(job.id)
        entered, release, completed = threading.Event(), threading.Event(), threading.Event()
        events = []

        def maintenance():
            entered.set()
            release.wait(10)

        def listener(event):
            if event.job_id == pipeline.DUPLICATE_RELATION_JOB:
                events.append(event)
                completed.set()

        scheduler.add_listener(listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        scheduler.start(paused=True)
        scheduler.pause_job(pipeline.DUPLICATE_RELATION_JOB)
        scheduler.add_job(maintenance, id="capture_v25_maintenance", executor="reconcile")
        scheduler.resume()
        try:
            self.assertTrue(entered.wait(3), "maintenance did not occupy the reconcile worker")
            started = time.monotonic()
            scheduler.modify_job(pipeline.DUPLICATE_RELATION_JOB,
                                 next_run_time=pipeline.datetime.now(pipeline.BEIJING))
            self.assertTrue(completed.wait(3), "ready relation work waited behind maintenance")
            self.assertFalse(release.is_set())
            self.assertLess(time.monotonic() - started, 3)
            self.assertIsNone(events[0].exception)
            self.assertEqual(events[0].retval["relation_status"], "ready")
            self.assertEqual(events[0].retval["compared_pairs"], 0)
            self.assertEqual(events[0].retval["provider_calls"], 0)
        finally:
            release.set()
            scheduler.shutdown(wait=True)


if __name__ == "__main__":
    unittest.main()
