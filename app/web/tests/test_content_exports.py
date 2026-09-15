"""Offline temporary-WAL fixtures; formal database access is always forbidden."""
from __future__ import annotations

import sys
import fcntl
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import uuid
from xml.etree import ElementTree as ET
import zipfile

sys.dont_write_bytecode = True

WEB = Path(__file__).resolve().parents[1]
BACKEND = Path(os.environ.get("DCAR_CONTENT_EXPORT_TEST_BACKEND", str(WEB.parents[1])))
SPEC = importlib.util.spec_from_file_location("content_exports_helper", WEB / "server/content_exports.py")
helper = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helper)
OWNER = "a" * 64
OTHER = "b" * 64
NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


class ContentExportsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.environment = os.environ.copy()
        cls.import_paths = sys.path[:]
        cls.modules = {key: value for key, value in sys.modules.items() if key == "v8" or key.startswith("v8.")}
        os.environ["DCAR_TEST_DENY_FORMAL_DB"] = "1"
        with tempfile.TemporaryDirectory() as tmp, patch("sqlite3.connect") as connect, \
                patch("threading.Thread.start") as thread, patch("socket.socket.connect") as network:
            cls.api, cls.storage = helper.search_helper.load_backend(BACKEND, Path(tmp) / "absent.db", BACKEND)
            connect.assert_not_called()
            thread.assert_not_called()
            network.assert_not_called()

    @classmethod
    def tearDownClass(cls):
        os.environ.clear()
        os.environ.update(cls.environment)
        sys.path[:] = cls.import_paths
        for key in list(sys.modules):
            if key == "v8" or key.startswith("v8."):
                sys.modules.pop(key)
        sys.modules.update(cls.modules)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.queue = helper.Queue(self.root / "exports")
        self.db = self.root / "fixture.sqlite3"
        self.writer = self.storage.connect(self.db, read_only=False)
        self.storage.initialize_database(self.writer)
        self.writer.commit()
        self.writer.execute("PRAGMA wal_autocheckpoint=0")
        self.writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.insert(1, "2026-09-06T15:59:59.999Z")
        self.insert(2, "2026-09-06T16:00:00Z")
        self.insert(3, "2026-09-07T15:59:59.999Z")
        self.insert(4, "2026-09-07T16:00:00Z")
        self.insert(5, "2026-09-07T04:00:00Z", platform="xiaohongshu")
        self.insert(6, None)
        self.writer.commit()

    def tearDown(self):
        self.writer.close()
        self.temp.cleanup()

    def insert(self, number, published, platform="douyin", account_id=None):
        self.writer.execute("""INSERT INTO content_items(
            id,link_id,platform,platform_content_id,canonical_url,published_at,title,
            content_type,account_id,raw_account_uid,raw_account_name,imported_at,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,?,'video',?,'9234567890123456789','汽车账号','now','now','now')""",
            (number, f"T{number:05}", platform, str(9234567890123456789 + number),
             f"https://example.test/{number}", published, "=HYPERLINK(\"https://invalid.test\") 汽车", account_id))

    def create(self, filters=None, owner=OWNER, request_id=None):
        return helper.command(dict(action="create", owner=owner, request_id=request_id or str(uuid.uuid4()),
                                   filters=filters or {"platform": "douyin"}), self.queue, self.api)

    def run_worker(self):
        with patch("socket.socket.connect", side_effect=AssertionError("must not use network")):
            return helper.run_worker(self.queue, self.db, self.api, self.storage)

    def sealed_snapshot(self):
        from v8.snapshot_contract import ARTIFACT_POLICY, descriptor
        self.writer.execute("INSERT INTO taxonomy_versions(id,version,status,definition,source_path,source_sha256,created_at,published_at) "
            "VALUES('snapshot-taxonomy','selling-points-v5.0','published','fixture','fixture','fixture','now','now')")
        self.storage.ensure_legacy_evaluation_release(self.writer,
            rule_version="evaluation-v7", taxonomy_version="selling-points-v5.0")
        self.writer.commit()
        identity = self.api._database_state(self.writer)["runtime_identity"]
        self.writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.writer.execute("PRAGMA journal_mode=DELETE")
        self.writer.close()
        self.db.chmod(0o640)
        digest = hashlib.sha256(self.db.read_bytes()).hexdigest()
        snapshot_id = "20260915T040000Z-123456789abc"
        receipt_path = self.root.resolve() / "runtime/active-snapshot.json"
        manifest_path = receipt_path.parent / "snapshot-history" / snapshot_id / "manifest.json"
        manifest = {"schema": "dcar-read-replica-snapshot-v2", "snapshot_id": snapshot_id,
            "writer_project_root": str(self.root / "writer"), "runtime_identity": identity,
            "snapshot_contract": descriptor(), "artifact_policy": ARTIFACT_POLICY,
            "files": [], "optional_reuse_files": [],
            "managed_originals": {"contract_version": "managed-originals-v1", "bundles": []},
            "databases": [{"name": "dcar_insight.sqlite3", "sha256": digest}]}
        receipt = {"schema": "dcar-read-replica-install-receipt-v1", "activation_status": "succeeded",
            "snapshot_id": snapshot_id, "writer_project_root": manifest["writer_project_root"],
            "runtime_identity": identity, "artifact_policy": ARTIFACT_POLICY,
            "manifest_path": str(manifest_path), "database_sha256": {"dcar_insight.sqlite3": digest}}
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text(json.dumps(manifest))
        manifest_path.chmod(0o640)
        receipt["manifest_sha256"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        receipt_path.write_text(json.dumps(receipt))
        receipt_path.chmod(0o640)
        return receipt_path, receipt, manifest_path

    def snapshot_worker(self, receipt_path):
        env = {key: os.environ[key] for key in ("PATH", "TMPDIR", "LANG") if key in os.environ}
        # A fresh process classifies its explicit DCAR_V8_DB as DEFAULT_DB, so
        # the general formal-DB test guard also rejects this disposable file.
        # Bind the real entry point to this test-owned, sealed read-only copy.
        self.assertTrue(self.db.resolve().is_relative_to(Path(self.temp.name).resolve()))
        env.update(DCAR_CONTENT_DATA_MODE="snapshot",
            DCAR_ACTIVE_SNAPSHOT=str(receipt_path), PYTHONDONTWRITEBYTECODE="1")
        return subprocess.run([sys.executable, "-B", "-I", str(WEB / "server/content_exports.py"),
            "--db", str(self.db), "--backend-root", str(BACKEND), "--project-root", str(self.root),
            "--jobs-root", str(self.queue.root), "--worker"], env=env,
            capture_output=True, text=True, timeout=30)

    def test_real_snapshot_worker_entry_exports_without_writer_authority_or_database_writes(self):
        created = self.create({"platform": "douyin", "published_from": "2026-09-07", "published_to": "2026-09-07"})
        receipt_path, _, _ = self.sealed_snapshot()
        before = self.db.read_bytes()
        result = self.snapshot_worker(receipt_path)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(json.loads(result.stdout)["processed"])
        job = self.queue.get(OWNER, created["job"]["id"])["job"]
        self.assertEqual((job["status"], job["total"], job["completed_rows"]), ("succeeded", 2, 2))
        self.assertEqual(self.db.read_bytes(), before)
        self.assertFalse(Path(str(self.db) + "-wal").exists())
        self.assertFalse(Path(str(self.db) + "-shm").exists())

    def test_snapshot_mode_alone_cannot_export_missing_unverified_or_substituted_database(self):
        receipt_path, receipt, manifest_path = self.sealed_snapshot()
        for fault in ("missing", "pending", "unprotected", "manifest", "database", "wal"):
            with self.subTest(fault=fault):
                created = self.create({"platform": "douyin"})
                receipt_path.write_text(json.dumps(receipt))
                receipt_path.chmod(0o640)
                manifest = manifest_path.read_bytes()
                database = self.db.read_bytes()
                if fault == "missing":
                    receipt_path.unlink()
                elif fault == "pending":
                    receipt_path.write_text(json.dumps({**receipt, "activation_status": "pending_smoke"}))
                elif fault == "unprotected":
                    receipt_path.chmod(0o666)
                elif fault == "manifest":
                    manifest_path.write_text("{}")
                elif fault == "database":
                    with sqlite3.connect(self.db) as connection:
                        connection.execute("UPDATE content_items SET title='substituted'")
                else:
                    Path(str(self.db) + "-wal").write_bytes(b"unsealed")
                result = self.snapshot_worker(receipt_path)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertEqual(json.loads(result.stdout)["error"]["status"], 503)
                self.assertEqual(self.queue.get(OWNER, created["job"]["id"])["job"]["status"], "failed")
                manifest_path.write_bytes(manifest)
                self.db.write_bytes(database)
                Path(str(self.db) + "-wal").unlink(missing_ok=True)

    def test_snapshot_verifier_never_uses_mac_writer_and_default_mode_never_falls_back(self):
        from v8 import runtime_database
        receipt_path, _, _ = self.sealed_snapshot()
        with patch.dict(os.environ, {"DCAR_CONTENT_DATA_MODE": "snapshot", "DCAR_READ_ONLY": "1",
                "DCAR_ACTIVE_SNAPSHOT": str(receipt_path)}), \
                patch.object(runtime_database, "resolve_installed_database_access", side_effect=AssertionError("Mac writer")):
            self.assertIsNotNone(helper.search_helper.verify_database(self.db, self.root))
            with patch.dict(os.environ, {"DCAR_CONTENT_DATA_MODE": "formal"}), self.assertRaisesRegex(AssertionError, "Mac writer"):
                helper.search_helper.verify_database(self.db, self.root)

    def rows(self, job_id, sheet=1):
        path = self.queue.get(OWNER, job_id)["file_path"]
        with zipfile.ZipFile(path) as archive:
            xml = ET.fromstring(archive.read(f"xl/worksheets/sheet{sheet}.xml"))
        result = []
        for row in xml.findall("m:sheetData/m:row", NS):
            result.append([cell.findtext("m:is/m:t", default=cell.findtext("m:v", default="", namespaces=NS), namespaces=NS)
                           for cell in row.findall("m:c", NS)])
        return result, xml

    def observe(self, *, captured_at, view_count, like_count, comment_count):
        from v8.metric_observations import persist_metric_observation
        raw = self.writer.execute("""INSERT INTO provider_raw_responses(
            content_id,provider,operation,local_path,sha256,byte_size,captured_at
        ) VALUES (2,'newrank_matrix','matrix_works_list',?,'fixture',1,?)""", (f'{captured_at}.json', captured_at))
        persist_metric_observation(self.writer, content_id=2, captured_at=captured_at,
            recorded_at=captured_at, window_key="2026-09-07", view_count=view_count,
            like_count=like_count, comment_count=comment_count, share_count=None, collect_count=None,
            status="available", provider="newrank_matrix", raw_response_id=int(raw.lastrowid),
            metadata_json=json.dumps({"operation": "matrix_works_list"}))
        self.writer.commit()

    def test_strict_filter_validation_and_normalization(self):
        invalid = [{}, {"query": "  "}, {"query": "%__%"}, {"query": []}, {"unknown": "x"},
                   {"published_from": "2026-02-30"}, {"published_from": "2026-09-09", "published_to": "2026-09-07"},
                   {"platform": "douyin", "page": True}, {"platform": "douyin", "page_size": 101},
                   {"account_group": "fake"}]
        for filters in invalid:
            with self.subTest(filters=filters), self.assertRaises(helper.ExportError):
                helper.normalize_filters(filters, self.api)
        self.assertEqual(helper.normalize_filters({"platform": "douyin", "query": " 汽车 ", "page": 20}, self.api),
                         {"platform": "douyin", "query": " 汽车 "})
        for value in ({"action": "list", "owner": "../"}, {"action": "list", "owner": OWNER, "id": "x"}):
            with self.assertRaises(helper.ExportError):
                helper.command(value, self.queue, self.api)
        for value in ("../../secret", 5, "{" + str(uuid.uuid4()) + "}"):
            with self.assertRaises(helper.ExportError):
                helper.identifier(value)

    def test_active_dedup_permanent_idempotency_and_owner_isolation(self):
        request = str(uuid.uuid4())
        first = self.create(request_id=request)
        alias = str(uuid.uuid4())
        again = self.create(filters={"platform": "douyin", "page": 7}, request_id=alias)
        self.assertTrue(again["reused"])
        self.assertEqual(first["job"]["id"], again["job"]["id"])
        other = self.create(owner=OTHER)
        self.assertNotEqual(first["job"]["id"], other["job"]["id"])
        with self.assertRaises(helper.ExportError) as rejected:
            self.create(filters={"platform": "xiaohongshu"}, request_id=request)
        self.assertEqual(rejected.exception.status, 409)
        with self.assertRaises(helper.ExportError) as missing:
            self.queue.get(OTHER, first["job"]["id"])
        self.assertEqual(missing.exception.status, 404)
        self.run_worker()
        replay = helper.Queue(self.queue.root).create(OWNER, alias, {"platform": "douyin"})
        self.assertEqual(replay["job"]["status"], "succeeded")
        self.assertEqual(replay["job"]["id"], first["job"]["id"])
        fresh = self.create()
        self.assertNotEqual(fresh["job"]["id"], first["job"]["id"])
        self.assertNotIn("file_path", replay["job"])
        self.assertNotIn("owner", replay["job"])

    def test_request_index_crash_recovers_after_worker_finished(self):
        request = str(uuid.uuid4())
        job_id = self.create(request_id=request)["job"]["id"]
        index = self.queue.root / "requests" / OWNER / f"{request}.json"
        index.unlink()
        self.run_worker()
        with self.assertRaises(helper.ExportError) as rejected:
            self.create(filters={"platform": "xiaohongshu"}, request_id=request)
        self.assertEqual(rejected.exception.status, 409)
        recovered = self.create(request_id=request)
        self.assertEqual(recovered["job"]["id"], job_id)
        self.assertTrue(index.is_file())
        self.assertEqual(len(list((self.queue.root / "jobs").glob("*.json"))), 1)

    def test_missing_successful_file_becomes_retryable_failure_in_get_and_list(self):
        for action in ("get", "list"):
            with self.subTest(action=action):
                job_id = self.create()["job"]["id"]
                self.run_worker()
                result = self.queue.get(OWNER, job_id)
                Path(result["file_path"]).unlink()
                if action == "get":
                    result = self.queue.get(OWNER, job_id)
                    self.assertNotIn("file_path", result)
                    job = result["job"]
                else:
                    job = next(item for item in self.queue.list(OWNER)["jobs"] if item["id"] == job_id)
                self.assertEqual(job["status"], "failed")
                self.assertEqual(job["error"], "导出文件已不可用，请重新生成。")
                self.assertIsNone(job["filename"])
                self.assertEqual(self.queue.read(OWNER, job_id)["status"], "failed")
        queued = self.create()["job"]
        self.assertEqual(self.queue.get(OWNER, queued["id"])["job"]["status"], "queued")

    def test_active_alias_index_crash_and_stale_worker_save_preserve_idempotency(self):
        job_id = self.create()["job"]["id"]
        # The worker retains this record while a second request is accepted.
        worker_record = self.queue.read(OWNER, job_id)
        alias = str(uuid.uuid4())
        self.assertEqual(self.create(request_id=alias)["job"]["id"], job_id)
        (self.queue.root / "requests" / OWNER / f"{alias}.json").unlink()
        with self.queue.locked():
            self.queue.save(worker_record)
        self.run_worker()
        with self.assertRaises(helper.ExportError) as rejected:
            self.create(filters={"platform": "xiaohongshu"}, request_id=alias)
        self.assertEqual(rejected.exception.status, 409)
        replay = self.create(request_id=alias)
        self.assertEqual(replay["job"]["id"], job_id)
        self.assertEqual(replay["job"]["status"], "succeeded")
        self.assertEqual(len(list((self.queue.root / "jobs").glob("*.json"))), 1)

    def test_bootstrap_failure_marks_queued_jobs_failed(self):
        job_id = self.create()["job"]["id"]
        argv = ["--db", str(self.db), "--backend-root", str(BACKEND), "--project-root", str(BACKEND),
                "--jobs-root", str(self.queue.root), "--worker"]
        stdout = io.StringIO()
        with patch.object(helper.search_helper, "verify_database", side_effect=RuntimeError("private config")), \
                patch.object(sys, "stdout", stdout):
            self.assertEqual(helper.main(argv), 1)
        result = self.queue.get(OWNER, job_id)
        self.assertEqual(result["job"]["status"], "failed")
        self.assertNotIn("private", json.dumps(result))
        self.assertNotIn("private", stdout.getvalue())

    def test_filtered_cross_page_xlsx_ids_formula_safety_and_beijing_bounds(self):
        self.observe(captured_at="2026-09-07T00:00:00Z", view_count=100, like_count=9, comment_count=2)
        self.observe(captured_at="2026-09-07T01:00:00Z", view_count=80, like_count=0, comment_count=None)
        filters = {"published_from": "2026-09-07", "published_to": "2026-09-07", "platform": "douyin", "query": "汽车"}
        expected = helper.search_helper.search(helper.search_helper.validate_request(filters, self.api), self.db, self.api, self.storage)
        job_id = self.create(filters)["job"]["id"]
        with patch.object(helper, "PAGE_SIZE", 1):
            self.run_worker()
        result = self.queue.get(OWNER, job_id)
        self.assertEqual(result["job"]["status"], "succeeded", result)
        self.assertEqual(result["job"]["completed_rows"], 2)
        rows, xml = self.rows(job_id)
        self.assertEqual([row[0] for row in rows[1:]], ["T00003", "T00002"])
        self.assertEqual(rows[2][1], "9234567890123456791")
        self.assertEqual(rows[2][7], "9234567890123456789")
        cells = {cell.attrib["r"]: cell for cell in xml.findall(".//m:c", NS)}
        self.assertEqual(cells["B3"].attrib["t"], "inlineStr")
        self.assertEqual(cells["H3"].attrib["t"], "inlineStr")
        self.assertEqual(cells["D3"].attrib["t"], "inlineStr")
        self.assertTrue(rows[2][3].startswith("=HYPERLINK"))
        self.assertFalse(xml.findall(".//m:f", NS))
        actual = next(item for item in expected["items"] if item["id"] == 2)
        self.assertEqual([rows[2][13], rows[2][14], rows[2][15]],
                         [str(actual[key]) if actual[key] is not None else "" for key in ("view_count", "like_count", "comment_count")])
        self.assertEqual(rows[2][13], "80")
        self.assertEqual(rows[2][14], "0")
        self.assertEqual(rows[1][13:16], ["", "", ""])
        # Actual Excel serial for the exact Shanghai start boundary: midnight.
        from v8 import report_export
        serial = report_export._excel_serial(report_export._parse_beijing_datetime("2026-09-06T16:00:00Z"))
        self.assertEqual(float(rows[2][6]), serial)
        notes, _ = self.rows(job_id, 2)
        text = json.dumps(notes, ensure_ascii=False)
        self.assertIn("最新有效累计值", text)
        self.assertIn("2026-09-07", text)
        self.assertIn(["实际导出条数", "2"], notes)

    def test_all_pages_share_one_read_only_live_wal_snapshot_and_count(self):
        job_id = self.create()["job"]["id"]
        original = self.api._content_search
        before_hash = hashlib.sha256(self.db.read_bytes()).hexdigest()
        observed = []
        count_queries = []

        def observe(payload, **kwargs):
            result = original(payload, **kwargs)
            observed.append(result["total"])
            if payload.page == 1:
                self.insert(7, "2026-09-07T05:00:00Z")
                self.writer.commit()
            return result

        original_connect = self.api.connect
        def checked_connect(path, *, read_only):
            self.assertTrue(read_only)
            connection = original_connect(path, read_only=read_only)
            self.assertEqual(connection.execute("PRAGMA query_only").fetchone()[0], 1)
            with self.assertRaises(sqlite3.OperationalError):
                connection.execute("UPDATE content_items SET title='bad'")
            connection.rollback()
            connection.set_trace_callback(lambda sql: count_queries.append(sql) if "SELECT COUNT(*) FROM content_items" in sql else None)
            return connection

        with patch.object(helper, "PAGE_SIZE", 2), patch.object(self.api, "_content_search", observe), \
                patch.object(self.api, "connect", checked_connect):
            self.run_worker()
        result = self.queue.get(OWNER, job_id)
        self.assertEqual(result["job"]["status"], "succeeded", result)
        self.assertEqual(observed, [5, 5, 5])
        self.assertEqual(len(count_queries), 1)
        rows, _ = self.rows(job_id)
        self.assertEqual(len(rows), 6)
        self.assertNotIn("T00007", [row[0] for row in rows])
        self.assertEqual(hashlib.sha256(self.db.read_bytes()).hexdigest(), before_hash)

    def test_interrupted_running_jobs_fail_and_can_retry_new_request(self):
        first = self.create()["job"]
        record = self.queue.read(OWNER, first["id"])
        record["status"] = "running"
        self.queue.save(record)
        partial = self.queue.root / "files" / f'{first["id"]}.partial'
        partial.write_text("unfinished")
        self.run_worker()
        self.assertFalse(partial.exists())
        failed = self.queue.get(OWNER, first["id"])["job"]
        self.assertEqual(failed["status"], "failed")
        self.assertIn("中断", failed["error"])
        retry = self.create()["job"]
        self.assertNotEqual(retry["id"], first["id"])
        self.run_worker()
        self.assertEqual(self.queue.get(OWNER, retry["id"])["job"]["status"], "succeeded")

    def test_worker_lock_prevents_duplicate_work_and_marks_active(self):
        job = self.create()["job"]
        with (self.queue.root / "worker.lock").open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertTrue(self.queue.list(OWNER)["worker_active"])
            self.assertFalse(self.run_worker())
            self.assertEqual(self.queue.get(OWNER, job["id"])["job"]["status"], "queued")
            fcntl.flock(lock, fcntl.LOCK_UN)
        self.run_worker()
        self.assertFalse(self.queue.list(OWNER)["worker_active"])

    def test_failed_export_does_not_leave_partial_or_disclose_internal_error(self):
        job_id = self.create()["job"]["id"]
        with patch.object(self.api, "_content_search", side_effect=RuntimeError("private path /secret/config")):
            self.run_worker()
        result = self.queue.get(OWNER, job_id)
        self.assertEqual(result["job"]["status"], "failed")
        self.assertNotIn("private", result["job"]["error"])
        self.assertNotIn("file_path", result)
        self.assertEqual(list((self.queue.root / "files").iterdir()), [])

    def test_history_keeps_twenty_recent_but_old_request_remains_replayable(self):
        first = None
        for index in range(24):
            request_id = str(uuid.uuid4())
            job = self.create(filters={"query": f"汽车{index}"}, request_id=request_id)["job"]
            if first is None:
                first = (request_id, job["id"])
        self.assertEqual(len(self.queue.list(OWNER)["jobs"]), 20)
        replay = self.create(filters={"query": "汽车0"}, request_id=first[0])
        self.assertEqual(replay["job"]["id"], first[1])
        self.assertEqual(len(list((self.queue.root / "jobs").glob("*.json"))), 24)

    def test_cli_safe_errors_and_short_commands_do_not_open_database(self):
        argv = ["--db", str(self.db), "--backend-root", str(BACKEND), "--project-root", str(BACKEND), "--jobs-root", str(self.queue.root)]
        stdout = io.StringIO()
        stdin = type("Stdin", (), {"buffer": io.BytesIO(json.dumps({"action": "list", "owner": OWNER}).encode())})()
        with patch.object(sys, "stdin", stdin), patch.object(sys, "stdout", stdout), patch("sqlite3.connect") as connect:
            self.assertEqual(helper.main(argv), 0)
            connect.assert_not_called()
        self.assertEqual(json.loads(stdout.getvalue())["jobs"], [])


if __name__ == "__main__":
    unittest.main()
