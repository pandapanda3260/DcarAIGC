"""Production-branch contract tests on temporary DB/media and fixture transports.

Only HTTP/SSH, the project path and current source hash are substituted.
OCR JSON is declared fixture input; completion sealing, fingerprints, evaluation,
archive/restore, SQLite transactions and protected receipt files execute normally.
"""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import io
import json
import os
import sqlite3
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from fastapi.testclient import TestClient
from PIL import Image

from tests import test_v8_managed_media as media_fixtures
from v8 import api, duplicates, evaluation, media, media_completion
from v8 import media_consumer_proofs as proofs
from v8 import media_lifecycle as lifecycle, media_retention as retention
from v8.snapshot_contract import ARTIFACT_POLICY, descriptor
from v8.storage import connect, initialize_database, now_utc, transaction


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class _HttpResponse(io.BytesIO):
    def __init__(self, body: bytes, url: str) -> None:
        super().__init__(body)
        self.url = url
        self.headers = {"Content-Length": str(len(body))}

    def geturl(self) -> str:
        return self.url


class MediaConsumerProofsTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.project = self.root / "project"
        self.media_root = self.project / "data/cache/v8/media"
        self.media_root.parent.mkdir(parents=True)
        self.db = self.project / "fixture.sqlite3"
        self.archive = self.root / "archive"
        self.archive.mkdir(mode=0o700)
        self.now = now_utc()
        self.code = proofs._IMPORTED_CODE
        self._patch(proofs, "PROJECT_ROOT", self.project)
        self._patch(proofs, "code_sha256", return_value=self.code)
        with connect(self.db) as connection:
            initialize_database(connection)
        with connect(self.db) as connection, transaction(connection):
            connection.execute("INSERT INTO accounts(id,phone,created_at,updated_at) VALUES(1,'',?,?)",
                               (self.now, self.now))
            connection.execute(
                "INSERT INTO account_platform_identities(account_id,platform,uid,created_at,updated_at) "
                "VALUES(1,'douyin','100001',?,?)", (self.now, self.now))
            connection.execute(
                "INSERT INTO content_items(id,link_id,platform,platform_content_id,canonical_url,account_id,"
                "raw_account_uid,title,body,content_type,published_at,imported_at,created_at,updated_at) "
                "VALUES(1,'PROOF1','douyin','9000000000000000001','https://www.douyin.com/video/9000000000000000001',"
                "1,'100001','汽车保养知识','判断刹车轮胎故障','image',?,?,?,?)",
                (self.now, self.now, self.now, self.now))
            self._current_release(connection)
            self.activation = lifecycle.activate(
                connection, mode="active", activation_id="consumer-proof-fixture", release="fixture-release",
                rules_sha256=descriptor()["media_retention_sha256"], archive_root=self.archive,
                canary_content_ids=(1,), proofs={"contract_version": lifecycle.FIXTURE_PROOF_CONTRACT,
                "fixture_only": True, "mac_consumers": True, "server_pairing": True, "canary_restore": True},
            )

    def _patch(self, target: Any, name: str, *args: Any, **kwargs: Any) -> Any:
        patcher = patch.object(target, name, *args, **kwargs)
        result = patcher.start()
        self.addCleanup(patcher.stop)
        return result

    def _current_release(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            "INSERT INTO taxonomy_versions(id,version,status,definition,created_at) "
            "VALUES('proof-current','selling-points-v5.2','published','fixture',?)", (self.now,))
        connection.execute(
            "INSERT INTO evaluation_releases(id,rule_version,taxonomy_version,matcher_rule_sha256,status,created_at,updated_at) "
            "VALUES('evaluation-v9__selling-points-v5.2','evaluation-v9','selling-points-v5.2',?,'active',?,?)",
            ("a" * 64, self.now, self.now))

    def _health(self, *, read_only: bool = False) -> dict[str, Any]:
        config = api.ApiConfig(
            db_path=self.db, reports_root=self.project / "reports", legacy_db_path=self.project / "legacy.sqlite3",
            operator_freeze_lock=self.project / "freeze", writer_lock=self.project / "writer",
            scheduler_enabled=False, startup_catchup_enabled=False, read_only=read_only,
        )
        app = api.create_app(config)
        app.state.database_sha256 = hashlib.sha256(self.db.read_bytes()).hexdigest()
        client = TestClient(app)  # Do not enter lifespan or start background jobs.
        try:
            response = client.get("/api/v8/health")
            self.assertEqual(response.status_code, 200, response.text)
            return response.json()
        finally:
            client.close()

    def _payload(self) -> tuple[dict[str, Any], dict[str, Any]]:
        mac, server = self._health(), self._health(read_only=True)
        server["media_consumers"]["project_root"] = "/fixture/server/release/app"
        transition = {
            "schema": proofs.TRANSITION_CONTRACT, "status": "succeeded",
            "from_schema": proofs.SOURCE_SCHEMA_VERSION,
            "to_schema": proofs.DATABASE_SCHEMA_VERSION,
            "code_sha256": self.code, "new_release": "/fixture/server/release/app", "completed_at": self.now,
            "sealed_manifest_sha256": "e" * 64, "runtime_identity": server["database_state"]["runtime_identity"],
            "snapshot_contract": descriptor(),
        }
        remote = {
            "schema": "dcar-remote-publisher-probe-v1", "current_release": transition["new_release"],
            "python_ready": True, "installer_ready": True, "free_bytes": 8 * 1024**3,
            "services": {f"dcar-{name}.service": "active" for name in ("api", "web", "auth", "douyin-control")},
            "directories": {name: True for name in ("db", "cache", "reports", "runtime", "incoming")},
            "schema_transition": copy.deepcopy(transition), "health": server, "overview": {"status": "ready"},
            "scheduler": {"read_only": True, "requested": False, "enabled": False},
            "active_receipt": {
                "schema": "dcar-read-replica-install-receipt-v1", "snapshot_id": "20260829T010000Z-abcdef123456",
                "manifest_sha256": "d" * 64, "runtime_identity": server["database_state"]["runtime_identity"],
                "database_sha256": {"dcar_insight.sqlite3": server["database_state"]["sha256"]},
                "snapshot_contract": descriptor(), "artifact_policy": ARTIFACT_POLICY,
            },
        }
        value = {
            "contract_version": proofs.PROOF_CONTRACT, "fixture_only": False, "captured_at": self.now,
            **{key: self.activation[key] for key in ("activation_id", "release", "rules_sha256", "canary_content_ids")},
            "database_path": str(self.db), "snapshot_contract": descriptor(),
            "mac_health_url": "http://127.0.0.1:8766/api/v8/health", "mac_health": mac,
            "server_transition": transition, "server_probe": remote,
        }
        return value, mac

    def _write_proof(self, value: dict[str, Any]) -> dict[str, Any]:
        root = self.project / "data/cache/media-consumer-proofs"
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        body = (json.dumps(value, sort_keys=True) + "\n").encode()
        digest = hashlib.sha256(body).hexdigest()
        path = root / (digest + ".json")
        path.write_bytes(body)
        path.chmod(0o600)
        return {"contract_version": proofs.PROOF_CONTRACT, "fixture_only": False,
                "consumer_receipt": {"path": str(path), "sha256": digest, "byte_size": len(body)}}

    def _production_record(self, bundle_ids: list[str] | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
        value, mac = self._payload()
        record = copy.deepcopy(self.activation)
        record["proofs"] = self._write_proof(value)
        if bundle_ids is not None:
            record["proofs"]["canary_bundles"] = bundle_ids
        return record, mac

    def _verify(self, record: dict[str, Any], mac: dict[str, Any], *, canary: bool = True) -> None:
        with patch.object(proofs, "_read_health", return_value=mac):
            with connect(self.db, read_only=True) as connection:
                proofs.verify_production(connection, record, require_canary=canary)

    def _real_canary(self) -> dict[str, Any]:
        url = "https://p3-sign.douyinpic.com/consumer-proof-fixture.png"
        buffer = io.BytesIO()
        Image.new("RGB", (96, 80), "navy").save(buffer, "PNG", compress_level=0)
        image_bytes = buffer.getvalue()
        self.assertGreater(len(image_bytes), 512)
        raw = {"code": 200, "data": {"status_code": 0, "aweme_detail": {
            "aweme_id": "9000000000000000001", "author": {"uid": "100001"}, "desc": "汽车保养知识",
            "images": [{"download_url_list": [url], "url_list": [url]}],
        }}}
        raw_path = self.project / "data/cache/source.json"
        raw_path.write_text(json.dumps(raw), encoding="utf-8")
        raw_path.chmod(0o600)
        with connect(self.db) as connection, transaction(connection):
            cursor = connection.execute(
                "INSERT INTO provider_raw_responses(account_id,content_id,provider,operation,local_path,sha256,"
                "byte_size,http_status,captured_at,source) VALUES(1,1,'TikHub','douyin_video_detail',?,?,?,200,?,'live_applied')",
                (str(raw_path), hashlib.sha256(raw_path.read_bytes()).hexdigest(), raw_path.stat().st_size, self.now))
            raw_id = cursor.lastrowid
        media.store_media_source_manifest(1, media_kind="image", urls=[url], raw_response_id=raw_id,
                                         db_path=self.db, media_root=self.media_root)

        def image_http(request: Any, **_kwargs: Any) -> _HttpResponse:
            self.assertEqual(request.full_url, url)
            return _HttpResponse(image_bytes, url)

        result = media.process_content_media(1, download_only=True, db_path=self.db, media_root=self.media_root,
                                             urlopen_fn=image_http, maximum_download_bytes=1024 * 1024)
        self.assertEqual(result["status"], "downloaded", result)
        with connect(self.db) as connection, transaction(connection):
            bundle = lifecycle.current_bundle(connection, 1)
            self.assertIsNotNone(bundle)
            version = media.processor_versions()["ocr"]
            source_sha = bundle["manifest"]["original_artifact"]["sha256"]
            ocr_path = media.managed_evidence_path(bundle, stage="ocr", source_sha256=source_sha,
                                                 processor_version=version, filename="ocr.json")
            ocr_path.parent.mkdir(parents=True)
            # Declared fixture OCR input, not a replacement of any processor.
            ocr = {"status": "success", "processor_version": version, "source_count": 1, "ocr_observation_count": 1,
                   "combined_text": "汽车刹车轮胎保养维修故障判断方法注意行车安全",
                   "observations": [{"text": "汽车刹车轮胎保养维修故障判断方法注意行车安全"}]}
            ocr_path.write_text(json.dumps(ocr), encoding="utf-8")
            ocr_path.chmod(0o600)
            artifact = media.register_artifact(
                connection, content_id=1, artifact_type="ocr", path=ocr_path, processor_version=version,
                metadata=media.managed_evidence_metadata(bundle, source_sha256=source_sha, processor_version=version))
            connection.execute(
                "INSERT INTO media_processing_slots(content_id,source_sha256,processor_type,processor_version,status,"
                "output_artifact_id,attempt_count,created_at,updated_at) VALUES(1,?,'ocr',?,'succeeded',?,1,?,?)",
                (media.managed_slot_source(bundle, source_sha), version, artifact.id, self.now, now_utc()))
            connection.execute("DELETE FROM evaluation_releases")
            connection.execute("DELETE FROM taxonomy_versions")
        release_fixture = media_fixtures.ManagedMediaTest()
        release_fixture.db, release_fixture.now = self.db, self.now
        release_fixture._release()  # Pure fixture taxonomy inserts; no setup/mocks.
        duplicates.fingerprint_content(1, db_path=self.db)
        evaluation.evaluate_content(1, db_path=self.db)
        complete = media_completion.seal_completion(bundle["bundle_id"], db_path=self.db)
        self.assertTrue(complete["ready"], complete)
        archived = retention.archive_bundle(bundle["bundle_id"], db_path=self.db)
        self.assertEqual(archived["status"], "archived", archived)
        restored = retention.restore_bundle(bundle["bundle_id"], db_path=self.db, request_id="consumer-proof-fixture")
        self.assertEqual(restored["status"], "restored", restored)
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE evaluation_releases SET status='retired' WHERE status='active'")
            self._current_release(connection)
            return lifecycle.current_bundle(connection, 1)

    def test_actual_api_health_contract_and_no_canary_production_validation(self) -> None:
        value, mac = self._payload()
        self.assertEqual(
            mac["database_state"]["user_version"], proofs.DATABASE_SCHEMA_VERSION
        )
        self.assertEqual(mac["database_path"], str(self.db))
        self.assertTrue(mac["media_consumers"]["current_code_matches_loaded"])
        self.assertFalse(mac["read_only"])
        proofs.validate_capture(value, self.activation, current_mac=mac)
        record, mac = self._production_record()
        self._verify(record, mac, canary=False)

    def test_real_completion_archive_hot_release_and_restore_pass_production_gate(self) -> None:
        bundle = self._real_canary()
        for key in ("completion_receipt", "archive_receipt", "hot_release_receipt", "restore_receipt"):
            self.assertIsInstance(bundle["state"][key], dict)
        record, mac = self._production_record([bundle["bundle_id"]])
        before = self.db.read_bytes()
        self._verify(record, mac)
        self.assertEqual(self.db.read_bytes(), before)

    def test_fixture_booleans_and_boolean_receipt_cannot_authorize_production(self) -> None:
        for proof in (
            self.activation["proofs"],
            {"mac_consumers": True, "server_pairing": True, "canary_restore": True},
            {"contract_version": proofs.PROOF_CONTRACT, "fixture_only": False, "consumer_receipt": True},
        ):
            with self.subTest(proof=proof):
                record = {**self.activation, "proofs": proof}
                with connect(self.db, read_only=True) as connection, self.assertRaises(ValueError):
                    proofs.verify_production(connection, record, require_canary=True)

    def test_activation_release_rules_and_canary_scope_are_bound_to_capture(self) -> None:
        value, mac = self._payload()
        for key, replacement in (
            ("activation_id", "different-activation"), ("release", "different-release"),
            ("rules_sha256", "b" * 64), ("canary_content_ids", [1, 2]),
        ):
            with self.subTest(field=key):
                changed = {**value, key: replacement}
                with self.assertRaisesRegex(ValueError, "consumer_proof_activation_mismatch"):
                    proofs.validate_capture(changed, self.activation, current_mac=mac)

    def test_code_change_and_mac_restart_invalidate_the_observation(self) -> None:
        value, mac = self._payload()
        with patch.object(proofs, "code_sha256", return_value="f" * 64):
            with self.assertRaisesRegex(ValueError, "consumer_runtime_not_loaded_release"):
                proofs.validate_capture(value, self.activation, current_mac=mac)
        restarted = copy.deepcopy(mac)
        restarted["media_consumers"]["boot_id"] = "f" * 32
        with self.assertRaisesRegex(ValueError, "mac_consumer_restarted_recapture_required"):
            proofs.validate_capture(value, self.activation, current_mac=restarted)
        stale_code = copy.deepcopy(mac)
        stale_code["media_consumers"]["current_code_matches_loaded"] = False
        with self.assertRaisesRegex(ValueError, "consumer_runtime_not_loaded_release"):
            proofs.validate_capture(value, self.activation, current_mac=stale_code)

    def test_legacy_unsettled_transition_and_wrong_server_schema_are_refused(self) -> None:
        value, mac = self._payload()
        for changes in (
            {
                "schema": proofs.LEGACY_TRANSITION_CONTRACT,
                "from_schema": 17,
                "to_schema": 18,
            },
            {"from_schema": 17},
            {"to_schema": 18},
            {"status": "running"},
            {"status": "rolled_back"},
        ):
            with self.subTest(changes=changes):
                changed = copy.deepcopy(value)
                changed["server_transition"].update(changes)
                changed["server_probe"]["schema_transition"].update(changes)
                with self.assertRaisesRegex(ValueError, "server_pairing_receipt_required"):
                    proofs.validate_capture(changed, self.activation, current_mac=mac)
        changed = copy.deepcopy(value)
        changed["server_probe"]["health"]["database_state"]["user_version"] = 18
        with self.assertRaisesRegex(ValueError, "server_pairing_runtime_mismatch"):
            proofs.validate_capture(changed, self.activation, current_mac=mac)

    def test_actual_mac_schema_and_connected_database_path_cannot_be_substituted(self) -> None:
        value, mac = self._payload()
        changed = copy.deepcopy(mac)
        changed["database_state"]["user_version"] = 18
        with self.assertRaisesRegex(ValueError, "mac_consumer_database_mismatch"):
            proofs.validate_capture(value, self.activation, current_mac=changed)
        value["database_path"] = str(self.root / "another-database.sqlite3")
        record = {**self.activation, "proofs": self._write_proof(value)}
        with self.assertRaisesRegex(ValueError, "consumer_proof_database_mismatch"):
            self._verify(record, mac, canary=False)

    def test_proof_file_tamper_and_nonprivate_permissions_are_rejected(self) -> None:
        value, _ = self._payload()
        reference = self._write_proof(value)["consumer_receipt"]
        path = Path(reference["path"])
        path.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "consumer_proof_not_private"):
            proofs._read_proof(reference)
        path.chmod(0o600)
        path.write_bytes(b"tampered-fixture")
        with self.assertRaisesRegex(ValueError, "consumer_proof_hash_mismatch"):
            proofs._read_proof(reference)

    def test_restore_missing_then_tampered_is_rejected(self) -> None:
        bundle = self._real_canary()
        record, mac = self._production_record([bundle["bundle_id"]])
        reference = bundle["state"]["restore_receipt"]
        with connect(self.db) as connection, transaction(connection):
            bundle = lifecycle.update_state(connection, bundle, {"restore_receipt": None}, bundle["state"]["revision"])
        with self.assertRaisesRegex(ValueError, "production_canary_restore_required"):
            self._verify(record, mac)
        with connect(self.db) as connection, transaction(connection):
            lifecycle.update_state(connection, bundle, {"restore_receipt": reference}, bundle["state"]["revision"])
        media._resolved(reference["path"]).write_bytes(b'{"tampered":true}\n')
        with self.assertRaisesRegex(lifecycle.LifecycleError, "lifecycle_artifact_bytes_changed"):
            self._verify(record, mac)

    def test_unrelated_successful_run_does_not_prove_canary_restore(self) -> None:
        bundle = self._real_canary()
        record, mac = self._production_record([bundle["bundle_id"]])
        old = bundle["state"]["restore_receipt"]
        body = json.loads(media._resolved(old["path"]).read_bytes())
        body["run_id"] = self.activation["activation_run_id"]  # Successful, but not media_restore.
        path = bundle["evidence_root"] / "fixture-unrelated-run.json"
        path.write_text(json.dumps(body), encoding="utf-8")
        path.chmod(0o600)
        with connect(self.db) as connection, transaction(connection):
            row = connection.execute("SELECT * FROM evidence_artifacts WHERE id=?", (old["artifact_id"],)).fetchone()
            replacement = media.register_artifact(
                connection, content_id=1, artifact_type="media_lifecycle_receipt", path=path,
                processor_version=retention.RETENTION_VERSION, metadata=json.loads(row["metadata_json"]))
            reference = {"artifact_id": replacement.id, "path": str(path), "sha256": replacement.sha256,
                         "byte_size": path.stat().st_size}
            lifecycle.update_state(connection, bundle, {"restore_receipt": reference}, bundle["state"]["revision"])
        with self.assertRaisesRegex(ValueError, "production_canary_run_not_succeeded"):
            self._verify(record, mac)

    def test_canary_membership_cannot_be_empty_duplicate_or_cover_another_activation(self) -> None:
        bundle = self._real_canary()
        for ids in ([], [bundle["bundle_id"], bundle["bundle_id"]]):
            record, mac = self._production_record(ids)
            with self.subTest(ids=ids), self.assertRaisesRegex(ValueError, "production_canary_restore_required"):
                self._verify(record, mac)
        record, mac = self._production_record([bundle["bundle_id"]])
        value = proofs._read_proof(record["proofs"]["consumer_receipt"])
        value["canary_content_ids"] = record["canary_content_ids"] = [1, 2]
        record["proofs"] = {**self._write_proof(value), "canary_bundles": [bundle["bundle_id"]]}
        with self.assertRaisesRegex(ValueError, "production_canary_scope_incomplete"):
            self._verify(record, mac)
        value["canary_content_ids"] = record["canary_content_ids"] = [1]
        value["activation_id"] = record["activation_id"] = "other-activation"
        record["proofs"] = {**self._write_proof(value), "canary_bundles": [bundle["bundle_id"]]}
        with self.assertRaisesRegex(ValueError, "production_canary_activation_mismatch"):
            self._verify(record, mac)

    def test_health_reader_never_accepts_remote_or_credential_bearing_urls(self) -> None:
        with patch.object(proofs, "build_opener", side_effect=AssertionError("HTTP must not run")):
            for url in ("https://127.0.0.1/api/v8/health", "http://fixture.invalid/api/v8/health",
                        "http://user:secret@127.0.0.1/api/v8/health", "http://127.0.0.1/api/v8/health?token=x"):
                with self.subTest(url=url), self.assertRaisesRegex(ValueError, "must_be_loopback"):
                    proofs._read_health(url)

    def test_capture_writes_private_hash_bound_file_without_activation_or_other_actions(self) -> None:
        value, mac = self._payload()
        env = self.root / "publisher.env"
        settings = {
            "DCAR_PUBLISH_SSH_ALIAS": "dcar-fixture-proof",
            "DCAR_PUBLISH_REMOTE_PROJECT_ROOT": "/fixture/server/current",
            "DCAR_PUBLISH_REMOTE_STATE_ROOT": "/fixture/server/state",
            "DCAR_PUBLISH_REMOTE_PYTHON": "/fixture/server/python",
            "DCAR_PUBLISH_SNAPSHOT_ROOT": str(self.root / "snapshots"),
            "DCAR_PUBLISH_MIN_REMOTE_FREE_BYTES": str(1024**3),
            "DCAR_PUBLISH_EXPECTED_USER_VERSION": "19", "DCAR_PUBLISH_MAX_CONTENT_LAG_DAYS": "1",
        }
        env.write_text("".join(f"{key}={item}\n" for key, item in settings.items()), encoding="utf-8")
        env.chmod(0o600)
        identity = self.root / "fixture-identity"
        identity.write_text("fixture-only-not-an-ssh-key", encoding="utf-8")
        identity.chmod(0o600)
        commands = []

        def ssh_transport(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
            self.assertEqual(command[0], "ssh")
            commands.append(command)
            output = (f"hostname fixture.invalid\nuser fixture\nidentityfile {identity}\n" if "-G" in command
                      else json.dumps(value["server_probe"]))
            return subprocess.CompletedProcess(command, 0, output, "")

        real_spec = importlib.util.spec_from_file_location

        def fixture_source(name: str, path: Any, *args: Any, **kwargs: Any) -> Any:
            if name == "_dcar_media_proof_publisher":
                path = REPOSITORY_ROOT / "deploy/macos/publish_snapshot.py"
            return real_spec(name, path, *args, **kwargs)

        with connect(self.db, read_only=True) as connection:
            before = list(connection.iterdump())
        with patch.object(importlib.util, "spec_from_file_location", side_effect=fixture_source), \
                patch.object(proofs.subprocess, "run", side_effect=ssh_transport), \
                patch.object(proofs, "_read_health", return_value=mac):
            result = proofs.capture(db_path=self.db, publisher_env=env, mac_health_url=value["mac_health_url"])
        reference = result["consumer_receipt"]
        path = Path(reference["path"])
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode), 0o700)
        self.assertEqual(path.stat().st_uid, os.getuid())
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), reference["sha256"])
        self.assertEqual(path.stat().st_size, reference["byte_size"])
        self.assertEqual(proofs._read_proof(reference)["database_path"], str(self.db))
        self.assertEqual(len(commands), 2)
        self.assertIn("-G", commands[0])
        self.assertIn("is-active", commands[1][-1])
        with connect(self.db, read_only=True) as connection:
            self.assertEqual(list(connection.iterdump()), before)


if __name__ == "__main__":
    unittest.main()
