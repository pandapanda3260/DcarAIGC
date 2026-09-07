"""Temporary SQLite and real manifest/file tests; no provider or production I/O."""
from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from starlette.types import Message

from v8 import api, duplicates, evaluation, media_api, media_completion, media_lifecycle, media_retention
from v8.source_routing import parse_time
from v8.storage import connect, transaction


class MediaLifecycleApiTest(unittest.TestCase):
    def setUp(self) -> None:
        from tests.test_v8_managed_media import ManagedMediaTest

        self.fixture = ManagedMediaTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture._source()
        self.fixture._activate()
        self.fixture._download(process=True)
        self.fixture._release()
        evaluation.evaluate_content(1, db_path=self.fixture.db)
        duplicates.fingerprint_content(1, db_path=self.fixture.db)
        self.bundle = self.fixture._bundle()
        proof = media_completion.seal_completion(self.bundle["bundle_id"], db_path=self.fixture.db, at=self.fixture.now)
        self.assertTrue(proof["ready"], proof)
        self.config = api.ApiConfig(db_path=self.fixture.db, reports_root=self.fixture.root / "reports",
                                    operator_freeze_lock=self.fixture.root / "freeze.lock",
                                    writer_lock=self.fixture.root / "writer.lock",
                                    legacy_db_path=self.fixture.root / "legacy.db", scheduler_enabled=False,
                                    startup_catchup_enabled=False)
        self.client = TestClient(api.create_app(self.config))
        self.addCleanup(self.client.close)
        self.addCleanup(patch.stopall)
        patch.object(media_api, "now_utc", side_effect=lambda: self.fixture.now).start()

    def _evidence(self, client: TestClient | None = None) -> dict[str, Any]:
        response = (client or self.client).get("/api/v8/contents/1/evidence")
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def _listed_media_available(self, client: TestClient) -> bool:
        response = client.post("/api/v8/contents/search", json={"page_size": 100})
        self.assertEqual(response.status_code, 200, response.text)
        return next(item["local_media_available"] for item in response.json()["items"] if item["id"] == 1)

    def _archive(self) -> None:
        result = media_retention.archive_bundle(self.bundle["bundle_id"], db_path=self.fixture.db, at=self.fixture.now)
        self.assertEqual(result["status"], "archived", result)
        self.bundle = self.fixture._bundle()

    def _database_fingerprint(self) -> str:
        with connect(self.fixture.db) as connection:
            return hashlib.sha256("\n".join(connection.iterdump()).encode()).hexdigest()

    def test_hot_original_and_preview_are_different_immutable_urls(self) -> None:
        before = self._database_fingerprint()
        value = self._evidence()
        self.assertEqual(len(value["media"]), 3)
        self.assertEqual(len(value["previews"]), 3)
        self.assertTrue(value["media_lifecycle"]["can_reprocess"])
        self.assertFalse(value["media_lifecycle"]["can_reacquire"])
        for original, preview in zip(value["media"], value["previews"], strict=True):
            self.assertNotEqual(original["url"], preview["url"])
            self.assertEqual(original["member_id"], preview["member_id"])
            response = self.client.get(original["url"])
            self.assertEqual(response.status_code, 200, response.text[:200])
            self.assertEqual(hashlib.sha256(response.content).hexdigest(), original["sha256"])
            thumbnail = self.client.get(preview["url"])
            self.assertEqual(thumbnail.status_code, 200, thumbnail.text[:100])
            self.assertEqual(hashlib.sha256(thumbnail.content).hexdigest(), preview["sha256"])
        self.assertEqual(before, self._database_fingerprint())

    def test_readonly_operational_routes_use_actual_evaluation_timestamp(self) -> None:
        replica = TestClient(api.create_app(replace(self.config, read_only=True)))
        self.addCleanup(replica.close)
        before = self._database_fingerprint()
        with connect(self.fixture.db, read_only=True) as connection:
            expected = connection.execute(
                "SELECT MAX(evaluated_at) FROM evaluation_versions "
                "WHERE evaluation_source='automatic' AND evaluation_status='evaluated' "
                "AND invalidated_at IS NULL"
            ).fetchone()[0]
        self.assertIsNotNone(expected)
        values = []
        for path in ("/api/v8/health", "/api/v8/overview", "/api/v8/scheduler"):
            with self.subTest(path=path):
                response = replica.get(path)
                self.assertEqual(response.status_code, 200, response.text)
                value = response.json()["data_freshness"]
                values.append(value)
                self.assertEqual(value["stage_data"]["local_evaluation"], expected)
                self.assertIsNone(value["stage_data"]["matrix_account_metrics"])
                self.assertEqual(value["status"], "unknown")
                self.assertIsNone(value["last_successful_capture_at"])
        self.assertTrue(all(value == values[0] for value in values))
        self.assertEqual(before, self._database_fingerprint())

    def test_original_range_keeps_exact_member_bytes(self) -> None:
        original = self._evidence()["media"][1]
        full = self.client.get(original["url"])
        response = self.client.get(original["url"], headers={"Range": "bytes=1-12"})
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.content, full.content[1:13])
        self.assertEqual(response.headers["Content-Range"], f"bytes 1-12/{len(full.content)}")
        invalid = self.client.get(original["url"], headers={"Range": "bytes=99999999-"})
        self.assertEqual(invalid.status_code, 416)

    def test_hot_bin_originals_report_sniffed_content_type(self) -> None:
        # Managed image members are stored as image-NNN.bin; the response must
        # carry the sniffed image type on full, ranged and HEAD reads alike.
        original = self._evidence()["media"][0]
        self.assertTrue(original["name"].endswith(".bin"), original["name"])
        response = self.client.get(original["url"])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "image/png")
        partial = self.client.get(original["url"], headers={"Range": "bytes=0-3"})
        self.assertEqual(partial.status_code, 206)
        self.assertEqual(partial.headers["content-type"], "image/png")
        head = self.client.head(original["url"])
        self.assertEqual(head.status_code, 200)
        self.assertEqual(head.headers["content-type"], "image/png")

    def test_archived_restore_is_idempotent_enqueue_and_never_paid(self) -> None:
        value = self._evidence()
        self._archive()
        before_calls = len(self.fixture.calls)
        before = self._database_fingerprint()
        archived = self._evidence()
        self.assertEqual(archived["display_evaluation_id"], value["display_evaluation_id"])
        self.assertTrue(archived["media_lifecycle"]["can_restore"])
        self.assertEqual(self.client.get(value["media"][0]["url"]).status_code, 409)
        self.assertEqual(self.client.get(value["previews"][0]["url"]).status_code, 200)
        self.assertEqual(before, self._database_fingerprint())
        payload = {"bundle_id": self.bundle["bundle_id"], "purpose": "evidence"}
        queued = self.client.post("/api/v8/contents/1/media/restore", json=payload)
        self.assertEqual(queued.status_code, 202, queued.text)
        repeated = self.client.post("/api/v8/contents/1/media/restore", json=payload)
        self.assertEqual(repeated.status_code, 202)
        self.assertEqual(queued.json()["run_id"], repeated.json()["run_id"])
        self.assertEqual(self.client.get(value["media"][0]["url"]).status_code, 202)
        self.assertEqual(len(self.fixture.calls), before_calls)
        self.assertFalse(any(Path(self.bundle["originals_root"]).iterdir()))

    def test_readonly_preview_works_original_and_restore_are_refused(self) -> None:
        replica = TestClient(api.create_app(replace(self.config, read_only=True)))
        self.addCleanup(replica.close)
        before = self._database_fingerprint()
        self.assertTrue(self._listed_media_available(self.client))
        self.assertTrue(self._listed_media_available(replica))
        value = self._evidence(replica)
        self.assertFalse(value["media_lifecycle"]["can_restore"])
        original = replica.get(value["media"][0]["url"])
        self.assertEqual(original.status_code, 409, original.text)
        self.assertEqual(original.json()["code"], "replica_original_omitted")
        self.assertEqual(replica.get(value["previews"][0]["url"]).status_code, 200)
        response = replica.post("/api/v8/contents/1/media/restore",
                                json={"bundle_id": self.bundle["bundle_id"], "purpose": "evidence"})
        self.assertEqual(response.status_code, 403)
        self.assertEqual(before, self._database_fingerprint())

    def test_readonly_without_preview_does_not_offer_hot_original(self) -> None:
        with connect(self.fixture.db) as connection, transaction(connection):
            connection.execute(
                "UPDATE evidence_artifacts SET status='missing' "
                "WHERE content_id=1 AND artifact_type='media_preview_manifest'"
            )
        replica = TestClient(api.create_app(replace(self.config, read_only=True)))
        self.addCleanup(replica.close)
        before = self._database_fingerprint()
        self.assertTrue(self._listed_media_available(self.client))
        self.assertFalse(self._listed_media_available(replica))
        value = self._evidence(replica)
        self.assertEqual(value["previews"], [])
        original = replica.get(value["media"][0]["url"])
        self.assertEqual(original.status_code, 409, original.text)
        self.assertEqual(original.json()["code"], "replica_original_omitted")
        self.assertEqual(before, self._database_fingerprint())

    def test_readonly_preview_flag_rejects_stale_source_or_changed_binding(self) -> None:
        replica = TestClient(api.create_app(replace(self.config, read_only=True)))
        self.addCleanup(replica.close)
        self.assertTrue(self._listed_media_available(replica))
        with connect(self.fixture.db, read_only=True) as connection:
            preview = dict(connection.execute(
                "SELECT * FROM evidence_artifacts WHERE content_id=1 "
                "AND artifact_type='media_preview_manifest' ORDER BY id DESC LIMIT 1"
            ).fetchone())
        metadata = json.loads(preview["metadata_json"])
        for field, changed in (("bundle_id", "0" * 32),
                               ("control_artifact_id", -1),
                               ("manifest_sha256", "0" * 64)):
            with self.subTest(field=field):
                invalid = {"media_lifecycle": {**metadata["media_lifecycle"], field: changed}}
                with connect(self.fixture.db) as connection, transaction(connection):
                    connection.execute("UPDATE evidence_artifacts SET metadata_json=? WHERE id=?",
                                       (json.dumps(invalid), preview["id"]))
                self.assertFalse(self._listed_media_available(replica))
        with connect(self.fixture.db) as connection, transaction(connection):
            connection.execute("UPDATE evidence_artifacts SET metadata_json=? WHERE id=?",
                               (preview["metadata_json"], preview["id"]))
        self.fixture._source(suffix="-new")
        # An old bundle's retained previews must not advertise media for a newly
        # captured source that has no matching managed bundle yet.
        before = self._database_fingerprint()
        self.assertFalse(self._listed_media_available(replica))
        self.assertEqual(before, self._database_fingerprint())

    def test_expiry_and_purging_never_look_restorable(self) -> None:
        value = self._evidence()
        self._archive()
        self.fixture.now = self.bundle["state"]["delete_due_at"]
        due = self.client.get(value["media"][0]["url"])
        self.assertEqual(due.status_code, 409)
        self.assertEqual(due.json()["code"], "original_expiry_pending")
        self.assertFalse(self._evidence()["media_lifecycle"]["can_restore"])
        with connect(self.fixture.db) as connection, transaction(connection):
            current = media_lifecycle.load_bundle(connection, self.bundle["bundle_id"])
            media_lifecycle.update_state(connection, current, {"operation_state": "purging"},
                                         expected_revision=current["state"]["revision"])
        deleting = self.client.get(value["media"][0]["url"])
        self.assertEqual(deleting.status_code, 409)
        self.assertEqual(deleting.json()["code"], "original_purge_in_progress")
        self.assertFalse(self._evidence()["media_lifecycle"]["can_restore"])

    def test_corrupt_preview_and_wrong_artifact_are_rejected(self) -> None:
        value = self._evidence()
        wrong = self.client.get(value["previews"][0]["url"].replace("/contents/1/", "/contents/2/"))
        self.assertEqual(wrong.status_code, 404)
        with connect(self.fixture.db) as connection:
            row, members = media_api._preview_members(connection, self.fixture._bundle())
        self.assertIsNotNone(row)
        members[1]["path"].write_bytes(b"corrupted preview")
        response = self.client.get(value["previews"][1]["url"])
        self.assertEqual(response.status_code, 503)
        self.assertNotEqual(response.content, b"corrupted preview")
        response = self.client.get(value["media"][1]["url"])
        self.assertEqual(response.status_code, 200)

    def test_stream_lease_survives_deadline_until_response_end(self) -> None:
        self._archive()
        restored = media_retention.restore_bundle(self.bundle["bundle_id"], db_path=self.fixture.db, at=self.fixture.now)
        self.assertEqual(restored["status"], "restored", restored)
        value = self._evidence()
        member = value["media"][0]
        attempts: list[str] = []
        messages: list[dict[str, Any]] = []
        next_time = (parse_time(self.fixture.now) + timedelta(hours=73)).isoformat().replace("+00:00", "Z")

        async def send(message: Message) -> None:
            messages.append(dict(message))
            if message["type"] == "http.response.body" and message.get("body"):
                self.fixture.now = next_time
                root = media_retention._runtime_root(self.fixture.db)
                with self.assertRaisesRegex(media_lifecycle.LifecycleError, "media_bundle_busy"):
                    with media_retention._file_lock(root / (self.bundle["bundle_id"] + ".lock"), exclusive=True):
                        attempts.append("incorrectly acquired")
                attempts.append("blocked")

        async def receive() -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"type": "http.request", "body": b"", "more_body": False}

        response = media_api.LeasedOriginalResponse(db_path=self.fixture.db, content_id=1,
                    artifact_id=member["artifact_id"], bundle_id=self.bundle["bundle_id"], index=member["index"])
        asyncio.run(response({"type": "http", "method": "GET", "headers": [],
                              "asgi": {"version": "3.0", "spec_version": "2.4"}}, receive, send))
        self.assertIn("blocked", attempts)
        self.assertEqual(messages[-1]["type"], "http.response.body")
        self.assertFalse(messages[-1].get("more_body", False))
        root = media_retention._runtime_root(self.fixture.db)
        with media_retention._file_lock(root / (self.bundle["bundle_id"] + ".lock"), exclusive=True):
            pass

    def test_restore_payload_and_subject_are_strict(self) -> None:
        self._archive()
        before = self._database_fingerprint()
        payload = {"bundle_id": self.bundle["bundle_id"], "purpose": "evidence"}
        wrong = self.client.post("/api/v8/contents/2/media/restore", json=payload)
        self.assertEqual(wrong.status_code, 404, wrong.text)
        extra = self.client.post("/api/v8/contents/1/media/restore", json={**payload, "allow_paid": True})
        self.assertEqual(extra.status_code, 422)
        self.assertEqual(before, self._database_fingerprint())

    def test_expired_keeps_preview_and_conclusion_but_refuses_original_restore_retry(self) -> None:
        before_value = self._evidence()
        self._archive()
        self.fixture.now = self.bundle["state"]["delete_due_at"]
        result = media_retention.purge_bundle(self.bundle["bundle_id"], db_path=self.fixture.db, at=self.fixture.now)
        self.assertEqual(result["status"], "expired", result)
        before = self._database_fingerprint()
        for route, payload in (("media/restore", {"bundle_id": self.bundle["bundle_id"], "purpose": "reprocess"}),
                               ("media/retry", {"allow_paid_refresh": False})):
            response = self.client.post("/api/v8/contents/1/" + route, json=payload)
            self.assertEqual(response.status_code, 410, response.text)
        self.assertEqual(self.client.get(before_value["media"][0]["url"]).status_code, 410)
        self.assertEqual(self.client.get(before_value["previews"][0]["url"]).status_code, 200)
        self.assertEqual(self._evidence()["display_evaluation_id"], before_value["display_evaluation_id"])
        self.assertEqual(before, self._database_fingerprint())

    def test_retry_paid_boolean_and_cold_local_retry_create_no_attempts(self) -> None:
        before = self._database_fingerprint()
        paid = self.client.post("/api/v8/contents/1/media/retry", json={"allow_paid_refresh": True})
        self.assertEqual(paid.status_code, 409, paid.text)
        self.assertEqual(paid.json()["code"], "explicit_reacquire_contract_not_bound")
        self.assertEqual(before, self._database_fingerprint())
        self._archive()
        before = self._database_fingerprint()
        cold = self.client.post("/api/v8/contents/1/media/retry", json={"allow_paid_refresh": False})
        self.assertEqual(cold.status_code, 409, cold.text)
        self.assertEqual(cold.json()["code"], "original_archived")
        self.assertEqual(before, self._database_fingerprint())

    def test_manual_protection_summary_and_expiry_debt_are_readonly(self) -> None:
        self._archive()
        old = self.bundle
        self.fixture._source(suffix="-unfinished")
        self.fixture._download()
        pending = self.fixture._bundle()
        with connect(self.fixture.db) as connection, transaction(connection):
            media_lifecycle.update_state(connection, pending, {"completion_blockers": ["ocr_missing", "fingerprint_missing"]},
                                         expected_revision=pending["state"]["revision"])
        self.fixture.now = (parse_time(self.fixture.now) + timedelta(days=15)).isoformat().replace("+00:00", "Z")
        aged = media_retention.age_incomplete_bundles(db_path=self.fixture.db, at=self.fixture.now)
        self.assertEqual(aged["added"], [pending["bundle_id"]])
        replica = TestClient(api.create_app(replace(self.config, read_only=True)))
        self.addCleanup(replica.close)
        before = self._database_fingerprint()
        response = replica.get("/api/v8/media/lifecycle")
        self.assertEqual(response.status_code, 200, response.text)
        value = response.json()
        self.assertIsNone(value["snapshot_captured_at"])
        self.assertIsNone(value["snapshot_lag_seconds"])
        self.assertEqual(value["manual_count"], 1)
        self.assertEqual(value["manual_bytes"], pending["manifest"]["byte_size"])
        todo = value["manual_todos"][0]
        self.assertEqual(todo["registered_at"], pending["manifest"]["registered_at"])
        self.assertEqual(todo["account_id"], 1)
        self.assertIsNone(todo["resolution"])
        self.assertFalse(todo["evidence_ready"])
        self.assertTrue(todo["protections"]["completion_gate_aged"])
        self.assertEqual(todo["latest_processing"]["status"], "succeeded")
        self.assertEqual({item["category"] for item in value["blocker_groups"]}, {"ocr", "fingerprint"})
        self.assertEqual(value["expiry_debt_bytes"], old["manifest"]["byte_size"])
        self.assertGreater(value["expiry_debt_longest_overdue_seconds"], 0)
        self.assertEqual(value["expiry_debt"][0]["delay_reason"], "awaiting_retention_worker")
        self.assertEqual(value["archive_root_health"], "not_mounted_in_replica")
        self.assertEqual(before, self._database_fingerprint())

    def _matrix_metrics(self, *, view_count: int | None = 100) -> None:
        from tests.roster_fixture import accept_roster
        from v8.metric_observations import persist_metric_observation

        body = json.dumps({"code": 0, "data": [{"awemeId": "9000000000000000001",
                                              "uid": "100001", "commentCount": 0}]}).encode()
        path = self.fixture.root / "matrix-global-raw.json"
        path.write_bytes(body)
        path.chmod(0o600)
        with connect(self.fixture.db) as connection, transaction(connection):
            accept_roster(connection, accepted_at=self.fixture.now)
            raw_id = connection.execute(
                "INSERT INTO provider_raw_responses(provider,operation,local_path,sha256,byte_size,http_status,captured_at,source) "
                "VALUES ('newrank_matrix','matrix_works_list',?,?,?,200,?,'live_applied')",
                (str(path), hashlib.sha256(body).hexdigest(), len(body), self.fixture.now),
            ).lastrowid
            persist_metric_observation(
                connection, content_id=1, captured_at=self.fixture.now, window_key="manual-matrix-fixture",
                view_count=view_count, comment_count=0, like_count=4, share_count=1, collect_count=2,
                status="available", source="newrank_matrix", raw_response_id=raw_id, metadata_json="{}",
                provider="newrank_matrix",
            )

    def test_manual_update_uses_matrix_zero_without_network_or_media_refresh(self) -> None:
        from v8 import capture, providers

        self._matrix_metrics()
        self._archive()
        before_calls = len(self.fixture.calls)
        with patch.object(capture, "RAW_ROOT", self.fixture.root / "raw"), patch.object(
            providers, "_douyin_call", side_effect=AssertionError("fresh Matrix fields must not buy TikHub")
        ) as network:
            response = self.client.post("/api/v8/contents/1/update-data")
        self.assertEqual(response.status_code, 200, response.text)
        value = response.json()
        self.assertEqual(value["provider_cost"], 0)
        self.assertEqual(value["provider_policy"], "matrix-first-v1")
        self.assertEqual(value["metrics"]["requests"], [])
        self.assertEqual(value["media"]["status"], "restore_required")
        self.assertEqual(len(self.fixture.calls), before_calls)
        network.assert_not_called()
        with connect(self.fixture.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)
            self.assertGreater(connection.execute(
                "SELECT COUNT(*) FROM provider_raw_responses WHERE source='derived_applied'"
            ).fetchone()[0], 0)

    def test_correction_creates_new_manual_task_and_keeps_original(self) -> None:
        from v8 import reports

        original = reports.create_task(task_type="custom", period_start="2026-08-01", period_end="2026-08-02",
                                       creation_source="manual", name="fixture report", db_path=self.fixture.db)
        before = reports.get_task(original["id"], db_path=self.fixture.db)
        with patch.object(api, "assert_report_runtime_ready"), patch.object(api, "_queue_task_run") as queued:
            response = self.client.post("/api/v8/tasks/" + original["id"] + "/corrections", json={"reason": "补充已核实的数据"})
        self.assertEqual(response.status_code, 202, response.text)
        corrected = response.json()
        self.assertNotEqual(corrected["id"], original["id"])
        self.assertEqual((corrected["task_type"], corrected["creation_source"]), ("custom", "manual"))
        self.assertEqual((corrected["period_start"], corrected["period_end"]), ("2026-08-01", "2026-08-02"))
        event = next(event for event in corrected["events"] if event["event_type"] == "corrects_report")
        self.assertEqual(json.loads(event["payload_json"])["original_task_id"], original["id"])
        self.assertEqual(reports.get_task(original["id"], db_path=self.fixture.db), before)
        queued.assert_called_once()
        invalid = self.client.post("/api/v8/tasks/" + original["id"] + "/corrections", json={"reason": "   "})
        self.assertEqual(invalid.status_code, 422)
        replica = TestClient(api.create_app(replace(self.config, read_only=True)))
        self.addCleanup(replica.close)
        denied = replica.post("/api/v8/tasks/" + original["id"] + "/corrections", json={"reason": "fixture"})
        self.assertEqual(denied.status_code, 403)

    def test_manual_missing_play_uses_only_statistics_with_budget_category(self) -> None:
        from v8 import capture, providers

        self._matrix_metrics(view_count=None)
        self._archive()

        def supplier(stage: str, content_key: str, key: str) -> Any:
            self.assertEqual(stage, "metrics")
            self.assertEqual(content_key, "9000000000000000001")
            raw = {"code": 200, "data": {"status_code": 0, "statistics_list": [{
                "aweme_id": content_key, "play_count": 88, "digg_count": 9, "share_count": 5,
            }]}}
            return providers._parse_douyin_stage_payload(stage, content_key, raw)

        with patch.object(capture, "RAW_ROOT", self.fixture.root / "raw"), patch.object(
            providers, "_load_key", return_value="fixture-only"
        ), patch.object(providers, "_douyin_call", side_effect=supplier) as network:
            first = self.client.post("/api/v8/contents/1/update-data")
            self.assertEqual(first.status_code, 200, first.text)
            repeated = self.client.post("/api/v8/contents/1/update-data")
        self.assertEqual(repeated.status_code, 200, repeated.text)
        self.assertEqual(first.json()["provider_cost"], .001)
        self.assertEqual(repeated.json()["provider_cost"], 0)
        network.assert_called_once()
        with connect(self.fixture.db) as connection:
            rows = connection.execute("SELECT * FROM provider_usage").fetchall()
            budgets = connection.execute(
                "SELECT max_amount FROM provider_budget_batches WHERE id LIKE 'task-%'"
            ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual([row["max_amount"] for row in budgets], [100])
        self.assertEqual(rows[0]["amount"], .001)
        self.assertTrue(rows[0]["task_id"].startswith("manual-content:"))
        self.assertEqual(json.loads(rows[0]["details_json"])["category"], "metrics")

    def test_manual_update_obeys_disabled_identity_and_global_budget(self) -> None:
        from v8 import capture, providers

        self._matrix_metrics(view_count=None)
        self._archive()
        with connect(self.fixture.db) as connection, transaction(connection):
            connection.execute("UPDATE accounts SET enabled=0 WHERE id=1")
        before = self._database_fingerprint()
        with patch.object(providers, "_douyin_call", side_effect=AssertionError("disabled identity must not dispatch")) as network:
            disabled = self.client.post("/api/v8/contents/1/update-data")
        self.assertEqual(disabled.status_code, 409, disabled.text)
        network.assert_not_called()
        self.assertEqual(before, self._database_fingerprint())
        with connect(self.fixture.db) as connection, transaction(connection):
            connection.execute("UPDATE accounts SET enabled=1 WHERE id=1")
            connection.execute(
                "INSERT INTO provider_usage(provider,operation,request_attempts,billed_requests,currency,amount,recorded_at,details_json) "
                "VALUES ('TikHub','historical',1,1,'USD',100,?,'{}')", (self.fixture.now,),
            )
        with patch.object(capture, "RAW_ROOT", self.fixture.root / "raw"), patch.object(
            providers, "_douyin_call", side_effect=AssertionError("exhausted ledger must not dispatch")
        ) as network:
            blocked = self.client.post("/api/v8/contents/1/update-data")
        self.assertEqual(blocked.status_code, 409, blocked.text)
        network.assert_not_called()
        with connect(self.fixture.db) as connection:
            self.assertEqual(connection.execute("SELECT SUM(amount) FROM provider_usage").fetchone()[0], 100)


class ApiScanFreshnessTest(unittest.TestCase):
    def test_only_verified_sixty_matrix_windows_and_frozen_tikhub_round_are_current(self) -> None:
        from tests.test_v8_scan_receipts import CUTOFF, ScanReceiptsTest
        from v8 import runtime_receipts

        fixture = ScanReceiptsTest()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.addCleanup(fixture.tearDown)
        fixture._activate()
        round_result = fixture._round()
        for index in range(30):
            for platform in ("douyin", "xiaohongshu"):
                result = fixture._runtime_slice(index, platform)
                self.assertTrue(result["complete"], result)
        receipt = runtime_receipts.refresh_runtime_receipts(
            db_path=fixture.db,
            cutoff_at=CUTOFF,
            evidence_root=fixture.root / "runtime-receipts",
        )
        self.assertEqual(receipt["day_receipt"]["summary"]["status"], "complete")
        self.assertTrue(receipt["day_receipt"]["summary"]["complete"])
        config = api.ApiConfig(db_path=fixture.db, reports_root=fixture.root / "reports",
                               operator_freeze_lock=fixture.root / "freeze.lock", writer_lock=fixture.root / "writer.lock",
                               legacy_db_path=fixture.root / "legacy.db", read_only=True, scheduler_enabled=False,
                               startup_catchup_enabled=False)
        client = TestClient(api.create_app(config))
        self.addCleanup(client.close)
        with connect(fixture.db, read_only=True) as connection:
            before = "\n".join(connection.iterdump())
        with patch.object(api, "datetime", wraps=datetime) as clock:
            clock.now.return_value = parse_time(CUTOFF)
            with patch(
                "v8.scan_receipts.runtime_coverage",
                side_effect=AssertionError("API hot path repeated deep scan verification"),
            ):
                response = client.get("/api/v8/scheduler")
        self.assertEqual(response.status_code, 200, response.text)
        freshness = response.json()["data_freshness"]
        self.assertEqual(freshness["status"], "current")
        self.assertTrue(freshness["discovery_coverage"]["complete"])
        self.assertEqual(freshness["discovery_coverage"]["matrix_complete_windows"], 60)
        self.assertEqual(freshness["discovery_coverage"]["tikhub_expected_members"], 2)
        self.assertEqual(freshness["discovery_coverage"]["tikhub_complete_members"], 2)
        self.assertEqual(freshness["latest_capture_run"]["id"], round_result["round_run_id"])
        self.assertIsNotNone(freshness["last_successful_capture_at"])
        with patch.object(api, "now_utc", return_value=CUTOFF), patch(
            "v8.scan_receipts.runtime_coverage",
            side_effect=AssertionError("readyz repeated deep scan verification"),
        ):
            ready = client.get("/api/v8/readyz")
        self.assertEqual(ready.status_code, 503, ready.text)
        self.assertEqual(ready.json()["status"], "not_ready")
        self.assertEqual(
            ready.json()["reason"], "current_activation_permit_missing"
        )
        self.assertFalse(ready.json()["data_readiness"])
        self.assertIsNotNone(ready.json()["closed_business_day_receipt"])
        self.assertEqual(ready.json()["profile_id"], "matrix_hybrid_v1")
        self.assertIs(type(ready.json()["activation_id"]), int)
        with connect(fixture.db, read_only=True) as connection:
            self.assertEqual("\n".join(connection.iterdump()), before)
