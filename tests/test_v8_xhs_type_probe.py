from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.roster_fixture import accept_roster
from v8 import capture, media, providers
from v8.capture import CaptureError
from v8.operations import upsert_account, upsert_content
from v8.providers import XHS_TYPE_PROBE_WINDOW, update_content_data
from v8.raw_evidence import read_raw_json
from v8.storage import connect, initialize_database, transaction

UID = "b" * 24
NOTE_ID = "a" * 24


def note(kind="normal", **changes):
    value = {
        "id": NOTE_ID, "type": kind, "title": "fixture note",
        "desc": "all fixture main images", "time": 1787800000,
        "user": {"userid": UID, "nickname": "fixture"},
        "comments_count": 0, "liked_count": 10, "shared_count": 2,
        "collected_count": 3,
        "images_list": [
            {"original": "https://cdn.example/main-1.webp"},
            {"original": "https://cdn.example/main-2.webp"},
        ],
    }
    if kind == "video":
        value["video_info_v2"] = {"media": {"stream": {"h264": [
            {"master_url": "https://cdn.example/main-video.mp4"}
        ]}}}
    value.update(changes)
    return value


def payload(value):
    return {"code": 200, "data": {"success": True, "code": 0, "data": [{"note_list": [value]}]}}


class XhsTypeProbeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.db = self.root / "xhs-probe.sqlite3"
        for patcher in (
            patch.object(capture, "RAW_ROOT", self.root / "raw"),
            patch.object(media, "MEDIA_ROOT", self.root / "media"),
            patch.dict(os.environ, {"TIKHUB_API_KEY": "fixture-only-key", "TIKHUB_API_BASE": "https://api.tikhub.io"}),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        with connect(self.db) as connection:
            initialize_database(connection)
        self.account = upsert_account({"phone": "", "platforms": [{
            "platform": "xiaohongshu", "uid": UID, "nickname": "fixture",
        }]}, db_path=self.db)
        self.content = upsert_content({
            "platform": "xiaohongshu", "platform_content_id": NOTE_ID,
            "canonical_url": "https://www.xiaohongshu.com/explore/" + NOTE_ID,
            "content_type": "unknown", "account_uid": UID,
        }, db_path=self.db)

    def roster(self):
        with connect(self.db) as connection, transaction(connection):
            return accept_roster(connection)

    def update(self, **kwargs):
        return update_content_data(
            self.content["id"], db_path=self.db, stages=["detail"],
            process_media=False, **kwargs,
        )

    def rows(self, table):
        with connect(self.db) as connection:
            return [dict(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]

    def slot(self, window):
        return next((row for row in self.rows("fetch_slots") if row["stage"] == "detail" and row["window_key"] == window), None)

    def actual_content(self):
        return self.rows("content_items")[0]

    def test_unknown_image_uses_one_paid_probe_then_free_lifetime(self):
        self.roster()
        raw_payload = payload(note())
        with patch.object(providers, "_request_json", return_value=(200, raw_payload)) as request:
            result = self.update(task_id="probe-image", task_max_amount=.01)
        self.assertEqual(result["status"], "succeeded", result)
        self.assertEqual(request.call_count, 1)
        self.assertTrue(request.call_args.args[0].endswith("/get_image_note_detail"))
        self.assertEqual(result["provider_cost"], .01)
        self.assertEqual(self.slot(XHS_TYPE_PROBE_WINDOW)["status"], "succeeded")
        self.assertEqual(self.slot("lifetime")["status"], "succeeded")
        self.assertEqual(self.slot("lifetime")["adapter_version"], "tikhub-xhs-probe-derived-detail-v1")
        self.assertEqual(self.actual_content()["content_type"], "image")
        usage = self.rows("provider_usage")
        self.assertEqual(len(usage), 1)
        self.assertEqual(usage[0]["amount"], .01)
        raw_rows = self.rows("provider_raw_responses")
        self.assertEqual(len(raw_rows), 2)
        probe_raw, derived_raw = raw_rows
        self.assertEqual(read_raw_json(Path(probe_raw["local_path"])), raw_payload)
        derived = read_raw_json(Path(derived_raw["local_path"]))
        self.assertEqual(derived["source_raw_response_id"], probe_raw["id"])
        self.assertEqual(derived["source_sha256"], probe_raw["sha256"])
        self.assertEqual(derived["data"]["media_urls"], [
            "https://cdn.example/main-1.webp", "https://cdn.example/main-2.webp",
        ])
        self.assertNotEqual(probe_raw["local_path"], derived_raw["local_path"])
        self.assertEqual([row["billed"] for row in self.rows("fetch_attempts")], [1, 0])
        observations = self.rows("content_metric_observations")
        self.assertEqual(len(observations), 2)
        self.assertEqual({row["raw_response_id"] for row in observations}, {probe_raw["id"], derived_raw["id"]})
        self.assertFalse(any(row["stage"] == "metrics" for row in self.rows("fetch_slots")))
        with patch.object(providers, "_request_json", side_effect=AssertionError("must reuse")):
            repeated = self.update()
        self.assertEqual(repeated["provider_cost"], 0)
        self.assertEqual(len(self.rows("provider_usage")), 1)

    def test_unknown_video_requests_image_then_video_with_separate_cost_and_raw(self):
        self.roster()
        raw_payload = payload(note("video"))
        with patch.object(providers, "_request_json", return_value=(200, raw_payload)) as request:
            result = self.update()
        self.assertEqual(result["status"], "succeeded", result)
        self.assertEqual(result["provider_cost"], .02)
        self.assertEqual([call.args[0].rsplit("/", 1)[-1] for call in request.call_args_list], [
            "get_image_note_detail", "get_video_note_detail",
        ])
        self.assertEqual(self.actual_content()["content_type"], "video")
        self.assertEqual(self.slot(XHS_TYPE_PROBE_WINDOW)["status"], "succeeded")
        self.assertEqual(self.slot("lifetime")["status"], "succeeded")
        raw_rows = self.rows("provider_raw_responses")
        self.assertEqual(len(raw_rows), 2)
        self.assertEqual(raw_rows[0]["sha256"], raw_rows[1]["sha256"])
        self.assertNotEqual(raw_rows[0]["local_path"], raw_rows[1]["local_path"])
        self.assertEqual(sum(row["amount"] for row in self.rows("provider_usage")), .02)
        sources = [row for row in self.rows("evidence_artifacts") if row["artifact_type"] == "media_source"]
        self.assertEqual(len(sources), 1)
        manifest = json.loads(Path(sources[0]["local_path"]).read_bytes())
        self.assertIn("https://cdn.example/main-video.mp4", json.dumps(manifest))
        self.assertNotIn("https://cdn.example/main-1.webp", json.dumps(manifest))

    def test_probe_success_video_failure_holds_same_paid_identity(self):
        self.roster()
        retry = CaptureError("temporary", retryable=True, error_code="upstream_error", billed=False, raw_response={"error": "temporary"})
        with patch.object(providers, "_request_json", side_effect=[(200, payload(note("video"))), retry]) as request:
            first = self.update()
        self.assertEqual(first["status"], "partial")
        self.assertEqual(first["provider_cost"], .01)
        self.assertEqual(self.actual_content()["content_type"], "video")
        self.assertEqual(self.slot(XHS_TYPE_PROBE_WINDOW)["status"], "succeeded")
        self.assertEqual(self.slot("lifetime")["status"], "retryable_failed")
        self.assertEqual(self.rows("evidence_artifacts"), [])
        self.assertEqual(request.call_count, 2)
        with patch.object(providers, "_request_json", return_value=(200, payload(note("video")))) as request:
            second = self.update()
        self.assertEqual(second["status"], "partial", second)
        self.assertEqual(second["stages"][-1]["error_code"], "paid_identity_hold")
        self.assertEqual(second["provider_cost"], 0)
        request.assert_not_called()
        probe_attempts = [row for row in self.rows("fetch_attempts") if row["slot_id"] == self.slot(XHS_TYPE_PROBE_WINDOW)["id"]]
        self.assertEqual(len(probe_attempts), 1)
        self.assertEqual(sum(row["amount"] for row in self.rows("provider_usage")), .01)

    def test_missing_type_does_not_infer_image_from_cover_or_finish_lifetime(self):
        self.roster()
        value = note()
        del value["type"]
        with patch.object(providers, "_request_json", return_value=(200, payload(value))) as request:
            first = self.update()
        self.assertEqual(first["status"], "partial")
        self.assertEqual(first["stages"][-1]["error_code"], "content_type_unresolved")
        self.assertEqual(first["provider_cost"], .01)
        self.assertEqual(self.actual_content()["content_type"], "unknown")
        self.assertEqual(self.slot(XHS_TYPE_PROBE_WINDOW)["status"], "terminal_failed")
        self.assertIsNone(self.slot("lifetime"))
        self.assertEqual(self.rows("evidence_artifacts"), [])
        self.assertEqual(request.call_count, 1)
        with patch.object(providers, "_request_json", side_effect=AssertionError("do not buy another probe")):
            repeated = self.update()
        self.assertEqual(repeated["status"], "partial")
        self.assertEqual(repeated["provider_cost"], 0)
        self.assertEqual(len(self.rows("provider_usage")), 1)

    def test_missing_roster_performs_no_key_load_request_or_paid_attempt(self):
        with patch.object(providers, "_load_key") as key, patch.object(providers, "_request_json") as request:
            result = self.update()
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["stages"][-1]["error_code"], "roster_not_ready")
        key.assert_not_called()
        request.assert_not_called()
        self.assertEqual(result["provider_cost"], 0)
        self.assertEqual(self.rows("provider_usage"), [])
        self.assertEqual(self.rows("fetch_attempts"), [])
        self.assertIsNone(self.slot("lifetime"))

    def test_override_keeps_detail_name_and_explicit_probe_and_lifetime_scopes(self):
        self.roster()
        calls = []

        def override(stage, content):
            calls.append((stage, dict(content)))
            return providers._parse_xhs_stage_payload(
                stage, NOTE_ID, content["content_type"], payload(note("video")),
            )

        result = self.update(call_override=override)
        self.assertEqual(result["status"], "succeeded", result)
        self.assertEqual([stage for stage, _ in calls], ["detail", "detail"])
        self.assertEqual(calls[0][1]["_detail_window_key"], XHS_TYPE_PROBE_WINDOW)
        self.assertTrue(calls[0][1]["_xhs_type_probe"])
        self.assertEqual(calls[0][1]["_xhs_endpoint"], "get_image_note_detail")
        self.assertEqual(calls[1][1]["_detail_window_key"], "lifetime")
        self.assertFalse(calls[1][1]["_xhs_type_probe"])
        self.assertEqual(calls[1][1]["_xhs_endpoint"], "get_video_note_detail")

    def test_existing_known_image_keeps_direct_detail_contract(self):
        self.roster()
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE content_items SET content_type='image' WHERE id=?", (self.content["id"],))
        with patch.object(providers, "_request_json", return_value=(200, payload(note()))) as request:
            result = self.update()
        self.assertEqual(result["status"], "succeeded", result)
        self.assertEqual(request.call_count, 1)
        self.assertIsNone(self.slot(XHS_TYPE_PROBE_WINDOW))
        self.assertEqual(result["provider_cost"], .01)

    def test_existing_known_video_keeps_direct_video_contract(self):
        self.roster()
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE content_items SET content_type='video' WHERE id=?", (self.content["id"],))
        with patch.object(providers, "_request_json", return_value=(200, payload(note("video")))) as request:
            result = self.update()
        self.assertEqual(result["status"], "succeeded", result)
        self.assertEqual(request.call_count, 1)
        self.assertTrue(request.call_args.args[0].endswith("/get_video_note_detail"))
        self.assertIsNone(self.slot(XHS_TYPE_PROBE_WINDOW))

    def test_image_probe_without_main_images_never_claims_lifetime(self):
        self.roster()
        with patch.object(providers, "_request_json", return_value=(200, payload(note(images_list=[])))):
            result = self.update()
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["stages"][-1]["error_code"], "xhs_probe_media_missing")
        self.assertEqual(self.slot(XHS_TYPE_PROBE_WINDOW)["status"], "succeeded")
        self.assertIsNone(self.slot("lifetime"))
        self.assertEqual(result["provider_cost"], .01)
        with patch.object(providers, "_request_json", side_effect=AssertionError("probe raw must be reused")):
            repeated = self.update()
        self.assertEqual(repeated["provider_cost"], 0)

    def test_wrong_author_keeps_raw_but_no_type_or_full_detail(self):
        self.roster()
        with patch.object(providers, "_request_json", return_value=(200, payload(note(user={"userid": "c" * 24})))):
            result = self.update()
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["stages"][-1]["error_code"], "identity_conflict")
        self.assertEqual(self.actual_content()["content_type"], "unknown")
        self.assertIsNone(self.slot("lifetime"))
        self.assertEqual(len(self.rows("provider_raw_responses")), 1)

    def test_paid_video_step_obeys_same_cross_operation_task_ceiling(self):
        self.roster()
        with patch.object(providers, "_request_json", return_value=(200, payload(note("video")))) as request:
            result = self.update(task_id="probe-video", task_max_amount=.01)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["stages"][-1]["error_code"], "task_budget_exhausted")
        self.assertEqual(result["provider_cost"], .01)
        self.assertEqual(request.call_count, 1)
        self.assertEqual(self.slot(XHS_TYPE_PROBE_WINDOW)["status"], "succeeded")
        self.assertIsNone(self.slot("lifetime"))

    def test_probe_raw_integrity_failure_cannot_trigger_repurchase(self):
        self.roster()
        with patch.object(providers, "_request_json", return_value=(200, payload(note("video")))):
            self.update(task_id="probe-corrupt", task_max_amount=.01)
        raw = self.rows("provider_raw_responses")[0]
        original_hash = raw["sha256"]
        Path(raw["local_path"]).write_bytes(b'{"changed":true}')
        self.assertNotEqual(hashlib.sha256(Path(raw["local_path"]).read_bytes()).hexdigest(), original_hash)
        with patch.object(providers, "_request_json", side_effect=AssertionError("corrupt evidence cannot repurchase")):
            result = self.update()
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["stages"][-1]["error_code"], "RawResponseIntegrityError")
        self.assertIsNone(self.slot("lifetime"))
        self.assertEqual(len(self.rows("provider_usage")), 1)

    def test_missing_discovery_type_stays_unknown_without_body_image_assumption(self):
        value = note()
        del value["type"]
        result = providers._parse_xhs_discovery_payload({
            "code": 200, "data": {"success": True, "code": 0, "data": {
                "notes": [value], "has_more": False,
            }},
        })
        self.assertEqual(result["items"][0]["content_type"], "unknown")
        self.assertEqual(result["items"][0]["media_urls"], [])


    def test_image_derivation_failure_replays_probe_without_any_paid_call(self):
        self.roster()
        with patch.object(providers, "_request_json", return_value=(200, payload(note()))):
            with patch.object(providers, "execute_derived_content_fetch", side_effect=RuntimeError("interrupted")):
                first = self.update()
        self.assertEqual(first["status"], "partial")
        self.assertEqual(first["provider_cost"], .01)
        self.assertEqual(self.slot(XHS_TYPE_PROBE_WINDOW)["status"], "succeeded")
        self.assertIsNone(self.slot("lifetime"))
        with patch.object(providers, "_request_json", side_effect=AssertionError("replay is free")):
            second = self.update()
        self.assertEqual(second["status"], "succeeded", second)
        self.assertEqual(second["provider_cost"], 0)
        self.assertEqual(len(self.rows("provider_usage")), 1)
        self.assertEqual(self.slot("lifetime")["status"], "succeeded")
        self.assertEqual(len(self.rows("content_metric_observations")), 2)

    def test_billed_video_failure_is_reported_with_the_probe_cost(self):
        self.roster()
        failure = CaptureError(
            "billed upstream failure", retryable=True, error_code="upstream_error",
            billed=True, raw_response={"error": "billed failure"},
        )
        with patch.object(providers, "_request_json", side_effect=[(200, payload(note("video"))), failure]):
            result = self.update()
        self.assertEqual(result["status"], "partial")
        self.assertEqual(sum(row["amount"] for row in self.rows("provider_usage")), .02)
        self.assertEqual(result["provider_cost"], .02)


if __name__ == "__main__":
    unittest.main()
