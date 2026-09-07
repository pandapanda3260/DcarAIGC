from __future__ import annotations

import hashlib
import tempfile
import unittest
from datetime import date
from pathlib import Path
from typing import Any, Mapping
from unittest.mock import patch

from tests.roster_fixture import accept_roster
import v8.capture as capture_module
from v8.capture import ProviderResult
from v8.operations import upsert_account, upsert_content
from v8.providers import (
    XHS_TYPE_PROBE_WINDOW,
    _xhs_type_probe,
    capture_content_comments_live,
    update_content_data,
)
from v8.storage import connect, initialize_database


class V8ProviderLocalOnlyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.db = self.root / "providers-local-only.sqlite3"
        self.raw_root_patch = patch.object(
            capture_module, "RAW_ROOT", self.root / "raw"
        )
        self.raw_root_patch.start()
        self.addCleanup(self.raw_root_patch.stop)
        with connect(self.db) as connection:
            initialize_database(connection)

    def tearDown(self) -> None:
        try:
            with connect(self.db) as connection:
                raws = connection.execute(
                    "SELECT local_path,sha256,byte_size FROM provider_raw_responses"
                ).fetchall()
            for raw in raws:
                body = Path(raw["local_path"]).read_bytes()
                self.assertEqual(len(body), raw["byte_size"])
                self.assertEqual(hashlib.sha256(body).hexdigest(), raw["sha256"])
        finally:
            self.temp.cleanup()

    def _content(
        self,
        *,
        platform: str = "douyin",
        content_type: str = "video",
    ) -> dict[str, Any]:
        uid = "fixture-douyin-user" if platform == "douyin" else "fixture-xhs-user"
        account = upsert_account(
            {
                "phone": "",
                "platforms": [{"platform": platform, "uid": uid}],
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            accept_roster(connection)
        platform_content_id = (
            "7500000000000000001" if platform == "douyin" else "a" * 24
        )
        content = upsert_content(
            {
                "platform": platform,
                "platform_content_id": platform_content_id,
                "canonical_url": (
                    f"https://www.douyin.com/video/{platform_content_id}"
                    if platform == "douyin"
                    else f"https://www.xiaohongshu.com/explore/{platform_content_id}"
                ),
                "account_uid": uid,
                "title": "local-only fixture",
                "content_type": content_type,
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            stored = dict(
                connection.execute(
                    "SELECT * FROM content_items WHERE id=?", (content["id"],)
                ).fetchone()
            )
        self.assertEqual(stored["account_id"], account["id"])
        return stored

    def _paid_counts(self) -> tuple[int, int, int, int]:
        with connect(self.db) as connection:
            return tuple(
                int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in (
                    "fetch_slots",
                    "fetch_attempts",
                    "provider_usage",
                    "paid_provider_dispatch_events",
                )
            )

    @staticmethod
    def _provider_fault(
        stage: str, content: Mapping[str, Any]
    ) -> ProviderResult:
        raise AssertionError(f"unexpected provider call: {stage}:{content['id']}")

    def _seed_xhs_probe(self, content: Mapping[str, Any], content_type: str) -> None:
        media_urls = (
            ["https://cdn.example/xhs-image.webp"]
            if content_type == "image"
            else ["https://cdn.example/xhs-video.mp4"]
        )
        data = {
            "title": "probe fixture",
            "body": "probe fixture body",
            "published_at": "2026-08-02T01:00:00Z",
            "account_uid": "fixture-xhs-user",
            "account_name": "fixture account",
            "content_type": content_type,
            "media_urls": media_urls,
            "metrics": {
                "view_count": None,
                "comment_count": 3,
                "like_count": 8,
                "share_count": 1,
                "collect_count": 2,
            },
        }

        def probe_call(stage: str, current: Mapping[str, Any]) -> ProviderResult:
            self.assertEqual(stage, "detail")
            self.assertTrue(current["_xhs_type_probe"])
            return ProviderResult(
                data,
                {"stage": "detail", "data": data},
                200,
                True,
            )

        with connect(self.db) as connection:
            current = dict(
                connection.execute(
                    "SELECT * FROM content_items WHERE id=?", (content["id"],)
                ).fetchone()
            )
        outcome = _xhs_type_probe(
            current,
            db_path=self.db,
            call_override=probe_call,
            task_id=None,
            task_max_amount=None,
        )
        self.assertTrue(outcome.billed)
        self.assertEqual(outcome.amount, 0.01)

    def test_missing_stage_fails_before_budget_claim_or_provider_call(self) -> None:
        content = self._content()
        before = self._paid_counts()
        with patch(
            "v8.providers._budget_for_call",
            side_effect=AssertionError("cache-only reached budget"),
        ):
            result = update_content_data(
                int(content["id"]),
                as_of=date(2026, 8, 2),
                db_path=self.db,
                call_override=self._provider_fault,
                stages=["metrics"],
                process_media=False,
                cache_only=True,
            )

        self.assertEqual(result["status"], "partial")
        self.assertEqual(
            result["stages"],
            [
                {
                    "stage": "metrics",
                    "window_key": "2026-08-02",
                    "status": "failed",
                    "error_code": "cache_only_stage_unavailable",
                    "retryable": True,
                    "message": (
                        "cache-only mode has no succeeded local evidence for "
                        "this stage"
                    ),
                    "billed": False,
                    "amount": 0.0,
                    "currency": "USD",
                }
            ],
        )
        self.assertEqual(self._paid_counts(), before)

    def test_successful_raw_replays_locally_during_provider_fault(self) -> None:
        content = self._content()
        metric_data = {
            "view_count": 100,
            "comment_count": 4,
            "like_count": 9,
            "share_count": 1,
            "collect_count": 2,
        }
        seeded = update_content_data(
            int(content["id"]),
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=lambda stage, current: ProviderResult(
                metric_data,
                {"stage": stage, "data": metric_data},
                200,
                True,
            ),
            stages=["metrics"],
            process_media=False,
        )
        self.assertEqual(seeded["provider_cost"], 0.001)
        with connect(self.db) as connection:
            connection.execute(
                "DELETE FROM content_metric_snapshots WHERE content_id=?",
                (content["id"],),
            )
            connection.commit()
        before = self._paid_counts()

        with patch(
            "v8.providers._budget_for_call",
            side_effect=AssertionError("raw replay reached budget"),
        ):
            replayed = update_content_data(
                int(content["id"]),
                as_of=date(2026, 8, 2),
                db_path=self.db,
                call_override=self._provider_fault,
                stages=["metrics"],
                process_media=False,
                cache_only=True,
            )

        self.assertEqual(replayed["status"], "succeeded")
        self.assertEqual(replayed["provider_cost"], 0.0)
        self.assertEqual(replayed["stages"][0]["status"], "replayed")
        self.assertEqual(self._paid_counts(), before)
        with connect(self.db) as connection:
            snapshot = connection.execute(
                "SELECT view_count,comment_count FROM content_metric_snapshots "
                "WHERE content_id=? AND window_key='2026-08-02'",
                (content["id"],),
            ).fetchone()
        self.assertEqual(tuple(snapshot), (100, 4))

    def test_first_xhs_probe_is_blocked_without_local_evidence(self) -> None:
        content = self._content(platform="xiaohongshu", content_type="unknown")
        before = self._paid_counts()
        with patch(
            "v8.providers._budget_for_call",
            side_effect=AssertionError("cache-only probe reached budget"),
        ):
            result = update_content_data(
                int(content["id"]),
                db_path=self.db,
                call_override=self._provider_fault,
                stages=["detail"],
                process_media=False,
                cache_only=True,
            )

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["stages"][0]["window_key"], XHS_TYPE_PROBE_WINDOW)
        self.assertEqual(
            result["stages"][0]["error_code"],
            "cache_only_stage_unavailable",
        )
        self.assertTrue(result["stages"][0]["retryable"])
        self.assertEqual(self._paid_counts(), before)

    def test_succeeded_xhs_image_probe_replays_and_derives_lifetime(self) -> None:
        content = self._content(platform="xiaohongshu", content_type="unknown")
        self._seed_xhs_probe(content, "image")
        before = self._paid_counts()

        with patch(
            "v8.providers._budget_for_call",
            side_effect=AssertionError("probe replay reached budget"),
        ):
            result = update_content_data(
                int(content["id"]),
                db_path=self.db,
                call_override=self._provider_fault,
                stages=["detail"],
                process_media=False,
                cache_only=True,
            )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["provider_cost"], 0.0)
        self.assertEqual(
            [(item["stage"], item["status"]) for item in result["stages"]],
            [("detail_type_probe", "replayed"), ("detail", "succeeded")],
        )
        after = self._paid_counts()
        self.assertEqual(after[2:], before[2:])
        self.assertEqual(after[1], before[1] + 1)
        with connect(self.db) as connection:
            lifetime = connection.execute(
                "SELECT provider,status FROM fetch_slots "
                "WHERE content_id=? AND stage='detail' AND window_key='lifetime'",
                (content["id"],),
            ).fetchone()
            billed_attempts = connection.execute(
                "SELECT COUNT(*) FROM fetch_attempts WHERE billed=1"
            ).fetchone()[0]
        self.assertEqual(tuple(lifetime), ("TikHub", "succeeded"))
        self.assertEqual(billed_attempts, 1)

    def test_succeeded_xhs_video_probe_blocks_lifetime_before_paid_claim(self) -> None:
        content = self._content(platform="xiaohongshu", content_type="unknown")
        self._seed_xhs_probe(content, "video")
        before = self._paid_counts()

        with patch(
            "v8.providers._budget_for_call",
            side_effect=AssertionError("video lifetime reached budget"),
        ):
            result = update_content_data(
                int(content["id"]),
                db_path=self.db,
                call_override=self._provider_fault,
                stages=["detail"],
                process_media=False,
                cache_only=True,
            )

        self.assertEqual(result["status"], "partial")
        self.assertEqual(
            [(item["stage"], item["status"]) for item in result["stages"]],
            [("detail_type_probe", "replayed"), ("detail", "failed")],
        )
        failure = result["stages"][1]
        self.assertEqual(failure["window_key"], "lifetime")
        self.assertEqual(failure["error_code"], "cache_only_stage_unavailable")
        self.assertTrue(failure["retryable"])
        self.assertEqual(self._paid_counts(), before)

    def test_succeeded_xhs_probe_without_linked_raw_never_falls_back_to_network(
        self,
    ) -> None:
        content = self._content(platform="xiaohongshu", content_type="unknown")
        self._seed_xhs_probe(content, "image")
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE provider_raw_responses SET fetch_attempt_id=NULL "
                "WHERE content_id=? AND operation='xiaohongshu_note_detail'",
                (content["id"],),
            )
            connection.commit()
        before = self._paid_counts()

        with patch(
            "v8.providers._budget_for_call",
            side_effect=AssertionError("missing probe raw reached budget"),
        ):
            result = update_content_data(
                int(content["id"]),
                db_path=self.db,
                call_override=self._provider_fault,
                stages=["detail"],
                process_media=False,
                cache_only=True,
            )

        self.assertEqual(result["status"], "partial")
        failure = result["stages"][0]
        self.assertEqual(failure["window_key"], XHS_TYPE_PROBE_WINDOW)
        self.assertEqual(failure["error_code"], "cache_only_stage_unavailable")
        self.assertTrue(failure["retryable"])
        self.assertEqual(self._paid_counts(), before)

    def test_comment_cache_miss_keeps_legacy_stop_reason_and_maps_stage_code(
        self,
    ) -> None:
        content = self._content()
        before = self._paid_counts()
        with patch(
            "v8.providers._budget_for_call",
            side_effect=AssertionError("comment cache miss reached budget"),
        ):
            direct = capture_content_comments_live(
                int(content["id"]),
                as_of=date(2026, 8, 2),
                db_path=self.db,
                call_override=self._provider_fault,
                cache_only=True,
            )
            updated = update_content_data(
                int(content["id"]),
                as_of=date(2026, 8, 2),
                db_path=self.db,
                call_override=self._provider_fault,
                stages=["comments"],
                process_media=False,
                cache_only=True,
            )

        self.assertEqual(direct["status"], "incomplete")
        self.assertEqual(direct["stop_reason"], "cache_only_page_unavailable")
        self.assertEqual(updated["status"], "partial")
        failure = updated["stages"][0]
        self.assertEqual(failure["error_code"], "cache_only_stage_unavailable")
        self.assertEqual(failure["stop_reason"], "cache_only_page_unavailable")
        self.assertTrue(failure["retryable"])
        self.assertEqual(self._paid_counts(), before)

    def test_zero_comments_are_derived_in_cache_only_mode(self) -> None:
        content = self._content()
        metric_data = {
            "view_count": 100,
            "comment_count": 0,
            "like_count": 2,
            "share_count": 0,
            "collect_count": 0,
        }
        seeded = update_content_data(
            int(content["id"]),
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=lambda stage, current: ProviderResult(
                metric_data,
                {"stage": stage, "data": metric_data},
                200,
                True,
            ),
            stages=["metrics"],
            process_media=False,
        )
        self.assertEqual(seeded["provider_cost"], 0.001)
        before = self._paid_counts()

        with patch(
            "v8.providers._budget_for_call",
            side_effect=AssertionError("zero-comment derivation reached budget"),
        ):
            result = update_content_data(
                int(content["id"]),
                as_of=date(2026, 8, 2),
                db_path=self.db,
                call_override=self._provider_fault,
                stages=["comments"],
                process_media=False,
                cache_only=True,
            )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["provider_cost"], 0.0)
        self.assertEqual(result["stages"][0]["completion_kind"], "zero_comments")
        after = self._paid_counts()
        self.assertEqual(after[2:], before[2:])
        self.assertEqual(after[1], before[1] + 1)
        with connect(self.db) as connection:
            evidence = connection.execute(
                "SELECT comment_count,status FROM comment_evidence_versions "
                "WHERE content_id=?",
                (content["id"],),
            ).fetchone()
        self.assertEqual(tuple(evidence), (0, "available"))


if __name__ == "__main__":
    unittest.main()
