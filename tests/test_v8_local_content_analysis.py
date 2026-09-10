"""Integrated acquisition -> local analysis regression using a real temp ledger.

The paid provider boundary is forbidden. CDN bytes and ASR/OCR engines are
fixtures; download slots, real ffmpeg frames, evaluation-v9, fingerprints,
source projection, roster eligibility, durable attempts and recovery are real.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import socket
import subprocess
import sys
import tempfile
import types
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from apscheduler.schedulers.background import BackgroundScheduler
from tests.v9_report_fixture import activate_v9_report_fixture
from v8 import capture, media, media_lifecycle as lifecycle, pipeline, providers, storage
from v8.media_state import MediaTerminalDetail
from v8.profile_activations import append_activation
from v8.runtime_database import (
    DatabaseAccessMode, FileIdentity, InstalledWriterContract, ResolvedDatabaseAccess,
    acquire_writer_lock,
)

AT = "2026-09-09T00:00:00Z"
START = date(2026, 9, 6)


def after(seconds):
    return (datetime.fromisoformat(AT.replace("Z", "+00:00")) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


class LocalContentAnalysisTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.db = self.root / "analysis.sqlite3"
        self.clock = AT
        with storage.connect(self.db) as c:
            storage.initialize_database(c, target_version=20)
        self.release_id = activate_v9_report_fixture(self.db, [])
        self.video = self.root / "fixture.mp4"
        subprocess.run(["ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-f", "lavfi", "-i",
                        "color=c=black:s=320x240:d=1", "-f", "lavfi", "-i", "anullsrc=r=16000:cl=mono",
                        "-shortest", "-c:v", "libx264", "-c:a", "aac", str(self.video)], check=True, timeout=30)
        with storage.connect(self.db) as c, storage.transaction(c):
            c.execute("INSERT INTO accounts(id,phone,enabled,created_at,updated_at) VALUES(1,'',1,?,?)", (AT, AT))
            c.execute("INSERT INTO account_platform_identities(id,account_id,platform,uid,nickname,source,created_at,updated_at) "
                      "VALUES(1,1,'douyin','100000000001','fixture','manual',?,?)", (AT, AT))
            keys = ["uid:douyin:100000000001"]
            digest = hashlib.sha256(json.dumps(keys, separators=(",", ":")).encode()).hexdigest()
            source = self.root / "roster.json"
            source.write_text(json.dumps(keys))
            c.execute("""INSERT INTO account_roster_snapshots(id,source_family,source_type,scope_key,scope_json,
                source_instance_id,source_captured_at,accepted_at,declared_count,member_count,members_sha256,
                source_sha256,source_path,contract_version,metadata_json)
                VALUES(1,'system','system_managed','fixture','{}','fixture',?,?,1,1,?,?,?,'account-roster-v2','{}')""",
                (AT, AT, digest, hashlib.sha256(source.read_bytes()).hexdigest(), str(source)))
            c.execute("""INSERT INTO account_roster_members(snapshot_id,account_identity_id,platform,member_key,
                uid,monitoring_status,authorization_status,metadata_json)
                VALUES(1,1,'douyin','uid:douyin:100000000001','100000000001','monitored','unknown','{}')""")
            append_activation(c, profile_id="integrated_route_v1", roster_snapshot_id=1,
                roster_members_sha256=digest, effective_at=AT, build_receipt_sha256="a" * 64,
                actor="isolated-test", reason="local analysis fixture", created_at=AT)
        lock = self.root / "writer.lock"
        lock.touch(mode=0o600)
        installed = InstalledWriterContract(self.root, self.root / "fixture.plist", self.root,
                                            self.root / "fixture.py", self.db, lock, {})
        access = ResolvedDatabaseAccess(DatabaseAccessMode.WRITER, self.db,
            FileIdentity.from_stat(self.db.stat()), self.root, lock, installed)
        self.enterContext(acquire_writer_lock(access))
        self.network = self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.provider = self.enterContext(patch.object(providers, "_request_json", side_effect=AssertionError("paid provider forbidden")))
        self.paid = self.enterContext(patch.object(capture, "execute_content_fetch", side_effect=AssertionError("paid capture forbidden")))
        self.legacy = self.enterContext(patch.object(pipeline, "update_content_data", side_effect=AssertionError("legacy capture forbidden")))
        self.refresh = self.enterContext(patch.object(providers, "retry_content_media", side_effect=AssertionError("paid refresh forbidden")))
        self.enterContext(patch.object(pipeline, "now_utc", lambda: self.clock))
        self.enterContext(patch.object(media, "MEDIA_ROOT", self.root / "media"))
        self.enterContext(patch.object(media, "ocr_binary_path", return_value=Path("/usr/bin/true")))
        self.download = self.enterContext(patch.object(media, "_download_video", side_effect=self.download_bytes))
        self.asr = self.enterContext(patch.object(media, "_run_asr", side_effect=self.asr_output))
        self.ocr = self.enterContext(patch.object(media, "_run_ocr", side_effect=self.ocr_output))

    def download_bytes(self, urls, target, **kwargs):
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self.video, target)
        target.chmod(0o600)
        return target

    def asr_output(self, source, target, **kwargs):
        config = media.load_media_config()["asr"]
        text = "汽车保养知识，打开懂车帝查看车型配置和报价，比较不同车款。"
        media._atomic_json(target, {"status": "success", "processor_version": media.processor_versions()["asr"],
            "model_id": config["model_id"], "model_revision": config["model_revision"], "language": config["language"],
            "text": text, "segments": [{"start": 0.0, "end": 0.5, "text": text, "avg_logprob": -0.1,
                                       "no_speech_prob": 0.0}], "elapsed_seconds": 0.1})
        return target

    def ocr_output(self, manifest, target, **kwargs):
        count = len(json.loads(manifest.read_text())["frames"])
        observations = [{"text": "懂车帝车型配置对比和报价"} for _ in range(count)]
        media._atomic_json(target, {"status": "success", "processor_version": media.processor_versions()["ocr"],
            "source_count": count, "ocr_observation_count": count,
            "combined_text": "\n".join(item["text"] for item in observations), "observations": observations})
        return target

    def content(self, cid=1, *, published="2026-09-07T01:00:00Z", source=True, group="", account_id=1):
        with storage.connect(self.db) as c, storage.transaction(c):
            c.execute("""INSERT INTO content_items(id,link_id,platform,platform_content_id,canonical_url,account_id,
                raw_account_uid,title,body,content_type,published_at,source_group,imported_at,created_at,updated_at)
                VALUES(?,?,'douyin',?,?,?,'100000000001','汽车选购','懂车帝车型配置对比报价','video',?,?,?,?,?)""",
                (cid, f"FIX{cid:03}", str(9000000 + cid), f"https://www.douyin.com/video/{9000000+cid}", account_id,
                 published, group, AT, AT, AT))
            c.execute("INSERT INTO fetch_slots(content_id,stage,window_key,provider,adapter_version,status,created_at,updated_at) "
                      "VALUES(?,'detail','lifetime','TikHub','fixture','succeeded',?,?)", (cid, AT, AT))
        if source:
            self.source(cid)
        return cid

    def source(self, cid=1, suffix="initial"):
        return media.store_media_source_manifest(cid, media_kind="video", urls=[f"https://fixture.invalid/{cid}/{suffix}.mp4"],
            raw_response_id=cid, db_path=self.db, media_root=self.root / "sources")

    def enable_lifecycle(self):
        archive = self.root / "archive"
        archive.mkdir(mode=0o700)
        with storage.connect(self.db) as c, storage.transaction(c):
            lifecycle.activate(c, mode="active", activation_id="local-analysis-fixture", release="fixture-release",
                rules_sha256="f" * 64, archive_root=archive, canary_content_ids=(1,), now=AT,
                proofs={"contract_version": lifecycle.FIXTURE_PROOF_CONTRACT, "fixture_only": True,
                        "mac_consumers": True, "server_pairing": True, "canary_restore": True})

    def run_tick(self, at=AT, **kwargs):
        self.clock = at
        return pipeline.run_local_content_analysis(db_path=self.db, at=at, automatic_from=START, **kwargs)

    def rows(self, table):
        with storage.connect(self.db) as c:
            return [dict(row) for row in c.execute(f"SELECT * FROM {table} ORDER BY id")]

    def assert_no_provider(self):
        for mock in (self.network, self.provider, self.paid, self.legacy, self.refresh):
            mock.assert_not_called()
        self.assertEqual(self.rows("fetch_attempts"), [])

    def test_stored_detail_reaches_v9_evaluation_and_fingerprint_once(self):
        self.content()
        result = self.run_tick()
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["results"][0]["state"], "complete")
        self.assertEqual({row["processor_type"] for row in self.rows("media_processing_slots")},
                         {"download", "frames", "asr", "ocr", "duplicate_fingerprint"})
        evaluations = self.rows("evaluation_versions")
        self.assertEqual(len(evaluations), 1)
        self.assertEqual(evaluations[0]["release_id"], "evaluation-v9__selling-points-v5.2")
        self.assertEqual(evaluations[0]["evidence_level"], "V3")
        self.assertEqual(len(self.rows("duplicate_fingerprints")), 1)
        again = self.run_tick(after(300))
        self.assertEqual(again["processed"], 0)
        self.assertEqual(len(self.rows("evaluation_versions")), 1)
        self.download.assert_called_once()
        self.asr.assert_called_once()
        self.ocr.assert_called_once()
        self.assert_no_provider()

    def test_restart_after_download_resumes_same_ledger_across_day(self):
        self.content()
        with patch.object(pipeline, "run_media_processing_queue", side_effect=KeyboardInterrupt("writer stopped")):
            with self.assertRaises(KeyboardInterrupt):
                self.run_tick()
        run = self.rows("scheduler_runs")[0]
        self.assertEqual(run["status"], "running")
        self.assertEqual(self.rows("media_processing_slots")[0]["status"], "succeeded")
        result = self.run_tick(after(86400))
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["recovered"], 1)
        self.assertEqual(result["results"][0]["scheduler_run_id"], run["id"])
        self.assertEqual([r["status"] for r in self.rows("scheduler_run_attempts")], ["interrupted", "succeeded"])
        self.download.assert_called_once()
        self.assert_no_provider()

    def test_real_managed_bundle_creation_completes_current_run(self):
        self.content()
        self.enable_lifecycle()
        result = self.run_tick()
        self.assertTrue(result["complete"], result)
        with storage.connect(self.db) as c:
            bundle = lifecycle.current_bundle(c, 1)
        self.assertIsNotNone(bundle)
        self.assertEqual(next(row for row in self.rows("scheduler_runs")
                              if row["job_id"] == pipeline.LOCAL_ANALYSIS_JOB)["status"], "succeeded")
        self.assertEqual(self.run_tick(after(300))["processed"], 0)
        self.assert_no_provider()

    def test_managed_download_restart_keeps_same_input_identity(self):
        self.content()
        self.enable_lifecycle()
        with patch.object(pipeline, "run_media_processing_queue", side_effect=KeyboardInterrupt("writer stopped")):
            with self.assertRaises(KeyboardInterrupt):
                self.run_tick()
        with storage.connect(self.db) as c:
            self.assertIsNotNone(lifecycle.current_bundle(c, 1))
        original_run_id = next(row for row in self.rows("scheduler_runs")
                               if row["job_id"] == pipeline.LOCAL_ANALYSIS_JOB)["id"]
        result = self.run_tick(after(86400))
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["results"][0]["scheduler_run_id"], original_run_id)
        self.download.assert_called_once()
        self.assert_no_provider()

    def test_processing_failure_resumes_without_redownload(self):
        self.content()
        self.asr.side_effect = RuntimeError("temporary ASR failure")
        failed = self.run_tick()
        self.assertEqual(failed["results"][0]["status"], "partial")
        self.assertEqual(failed["results"][0]["reason"], "asr_pending")
        self.assertEqual(self.run_tick(after(60))["processed"], 0)
        self.asr.side_effect = self.asr_output
        result = self.run_tick(after(360))
        self.assertTrue(result["complete"], result)
        self.download.assert_called_once()
        self.assert_no_provider()

    def test_source_missing_is_visible_and_never_buys_a_source(self):
        self.content(source=False)
        result = self.run_tick()
        self.assertEqual(result["results"][0]["reason"], "source_missing")
        self.assertEqual(result["results"][0]["next_action"], "media_source_refresh_required")
        self.download.assert_not_called()
        self.source()
        self.assertTrue(self.run_tick(after(300))["complete"])
        self.assert_no_provider()

    def test_failed_url_exhaustion_stops_until_source_changes(self):
        self.content()
        self.download.side_effect = media.MediaProcessingError("CDN returned 403")
        self.run_tick()
        self.run_tick(after(360))
        failed = self.run_tick(after(1000))
        self.assertEqual(failed["results"][0]["status"], "failed")
        self.assertEqual(failed["results"][0]["next_action"], "media_source_refresh_required")
        self.assertEqual(self.download.call_count, 3)
        self.assertEqual(self.run_tick(after(86400))["processed"], 0)
        self.source(suffix="renewed-by-separate-authorized-capture")
        self.download.side_effect = self.download_bytes
        self.assertTrue(self.run_tick(after(87000))["complete"])
        self.assert_no_provider()

    def test_separately_reset_terminal_slot_can_recover_same_source(self):
        self.content()
        self.download.side_effect = media.MediaProcessingError("CDN temporarily unavailable")
        self.run_tick()
        self.run_tick(after(360))
        failed = self.run_tick(after(1000))
        failed_run_id = failed["results"][0]["scheduler_run_id"]
        with storage.connect(self.db) as c, storage.transaction(c):
            c.execute("UPDATE media_processing_slots SET status='retryable_failed',attempt_count=0 WHERE processor_type='download'")
        self.download.side_effect = self.download_bytes
        result = self.run_tick(after(1500))
        self.assertTrue(result["complete"], result)
        repaired_run = next(row for row in self.rows("scheduler_runs") if row["id"] == result["results"][0]["scheduler_run_id"])
        self.assertEqual(json.loads(repaired_run["details_json"])["identity"]["recovery_after_run_id"], failed_run_id)
        self.assert_no_provider()

    def test_current_scope_excludes_old_future_history_and_paused_accounts(self):
        self.content(1, published="2026-09-05T10:00:00Z")
        self.content(2, published="2026-09-11T10:00:00Z")
        self.content(3, group="history-backfill")
        self.content(4)
        with storage.connect(self.db) as c, storage.transaction(c):
            c.execute("UPDATE accounts SET enabled=0 WHERE id=1")
        self.assertEqual(self.run_tick()["eligible"], 0)
        self.download.assert_not_called()
        with storage.connect(self.db) as c, storage.transaction(c):
            c.execute("UPDATE accounts SET enabled=1 WHERE id=1")
        result = self.run_tick(after(300))
        self.assertEqual(result["eligible"], 1)
        self.assertEqual(result["results"][0]["content_id"], 4)
        self.assert_no_provider()

    def test_pause_between_items_is_rechecked(self):
        self.content(1)
        self.content(2)
        original = pipeline.run_local_batch
        def pause(ids, **kwargs):
            result = original(ids, **kwargs)
            with storage.connect(self.db) as c, storage.transaction(c):
                c.execute("UPDATE accounts SET enabled=0 WHERE id=1")
            return result
        with patch.object(pipeline, "run_local_batch", side_effect=pause):
            result = self.run_tick()
        self.assertEqual(result["processed"], 1)
        self.assertEqual([r["content_id"] for r in self.rows("evaluation_versions")], [1])
        self.assert_no_provider()

    def test_original_unavailable_is_recorded_without_local_download_or_paid_retry(self):
        self.content()
        with patch.object(pipeline, "media_terminal_state_details", return_value={1: MediaTerminalDetail("pending", "original_unavailable")}), \
             patch.object(pipeline, "run_local_batch", side_effect=AssertionError("unavailable original must not run")):
            result = self.run_tick()
        self.assertEqual(result["results"][0]["reason"], "original_unavailable")
        self.assertEqual(result["results"][0]["next_action"], "original_media_required")
        self.download.assert_not_called()
        self.assert_no_provider()

    def test_directory_successor_uses_directory_not_old_enabled_or_roster(self):
        self.content()
        with storage.connect(self.db) as c, storage.transaction(c):
            c.execute("UPDATE accounts SET enabled=0 WHERE id=1")
        catalog = types.ModuleType("v8.account_catalog_capture")
        catalog.installed_policy = Mock(return_value={"contract": "installed-policy-fixture"})
        eligibility = types.ModuleType("v8.account_capture_eligibility")
        eligibility.derive_capture_eligibility = Mock(return_value={"eligible_members": [
            {"identity_id": 1, "enabled": True, "uid": "100000000001"}], "selection_sha256": "f" * 64})
        eligibility.require_directory_capture_member = Mock(return_value={"identity_id": 1})
        with patch.dict(sys.modules, {catalog.__name__: catalog, eligibility.__name__: eligibility}), \
             patch.object(pipeline, "_scope", side_effect=AssertionError("legacy roster is not the installed authority")):
            result = self.run_tick()
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["results"][0]["scope_kind"], "account_directory")
        self.assertIsNone(result["results"][0]["roster_snapshot_id"])
        catalog.installed_policy.assert_called_once()
        eligibility.derive_capture_eligibility.assert_called_once()
        eligibility.require_directory_capture_member.assert_called_once()
        self.assert_no_provider()

    def test_installed_scheduler_registers_independent_local_worker(self):
        scheduler = BackgroundScheduler()
        pipeline.install_pipeline_jobs(scheduler, db_path=self.db, reports_root=self.root / "reports",
                                       authorization_effective_date=START)
        job = next(job for job in scheduler.get_jobs() if job.id == pipeline.LOCAL_ANALYSIS_JOB)
        self.assertIs(job.func, pipeline.run_local_content_analysis)
        self.assertEqual(job.kwargs["automatic_from"], START)
        self.assertEqual(job.max_instances, 1)
        self.assertNotIn(pipeline.LOCAL_ANALYSIS_JOB, pipeline.PAID_DISPATCH_JOBS)
        self.assertEqual(job.trigger.interval.total_seconds(), 300)
