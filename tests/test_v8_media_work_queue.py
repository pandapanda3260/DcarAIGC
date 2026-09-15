"""Fairness and read-only media debt projection; no provider network allowed."""
import json
from pathlib import Path
import tempfile
import unittest

from v8 import storage
from v8.media_work_queue import LocalMediaSelector, fair_local_order, list_pending_media_work, local_media_supported, refresh_reason_label


class MediaWorkQueueTest(unittest.TestCase):
    def test_refresh_business_failure_and_billing_uncertainty_are_distinct(self):
        known = refresh_reason_label("douyin_video_detail:paid_identity_hold:content_unavailable")
        unknown = refresh_reason_label("douyin_video_detail:paid_identity_hold:billing_unknown")
        self.assertIn("数据源本次未返回该作品", known)
        self.assertNotIn("计费结果尚未确认", known)
        self.assertIn("计费结果尚未确认", unknown)
        self.assertNotIn("未返回该作品", unknown)
        self.assertEqual(refresh_reason_label(""), "")

    def test_four_platform_video_and_only_proven_image_capabilities(self):
        for platform in ("douyin", "xiaohongshu", "kuaishou", "wechat_channels"):
            self.assertTrue(local_media_supported(platform, "video"))
            self.assertFalse(local_media_supported(platform, "unknown"))
        self.assertTrue(local_media_supported("xiaohongshu", "image"))
        self.assertFalse(local_media_supported("wechat_channels", "image"))
        self.assertFalse(local_media_supported("kuaishou", "image"))

    def test_rotation_continues_from_last_attempt_and_oldest_gets_fifth_pick(self):
        platforms = ("douyin", "xiaohongshu", "kuaishou", "wechat_channels")
        rows = [{"id": i, "platform": platforms[(i - 1) % 4], "created_at": "2026-09-10", "source_captured_at": "2026-09-11"} for i in range(1, 13)]
        rows.append({"id": 99, "platform": "douyin", "created_at": "2026-09-01"})
        states = {row["id"]: {"reason": "download_pending" if row["id"] != 99 else "asr_pending"} for row in rows}
        ordered = fair_local_order(rows, states, last_platform="douyin")
        self.assertEqual([r["platform"] for r in ordered[:4]], ["xiaohongshu", "kuaishou", "wechat_channels", "douyin"])
        self.assertEqual(ordered[4]["id"], 99)
        self.assertEqual(len(ordered), len(rows))
        self.assertEqual(len({r["id"] for r in ordered}), len(rows))

    def test_explicit_expiry_orders_within_platform_without_inventing_ttl(self):
        rows = [{"id": 1, "platform": "kuaishou", "source_captured_at": "2026-09-01"},
            {"id": 2, "platform": "kuaishou", "source_expires_at": "2026-09-12T03:00:00Z"},
            {"id": 3, "platform": "kuaishou", "source_expires_at": "2026-09-12T01:00:00Z"}]
        self.assertEqual([r["id"] for r in fair_local_order(rows, {i: {"reason": "download_pending"} for i in (1, 2, 3)})], [3, 2, 1])

    def test_one_item_rounds_do_not_starve_wechat_source_recovery(self):
        rows = [{"id": i, "platform": "douyin", "created_at": "2026-09-01"} for i in range(1, 21)]
        rows.append({"id": 99, "platform": "wechat_channels", "created_at": "2026-09-10"})
        states = {row["id"]: {"reason": "download_pending" if row["platform"] == "douyin" else "source_missing"} for row in rows}
        last_platform = "wechat_channels"
        attempted = []
        for _ in range(2):
            picked = fair_local_order(rows, states, last_platform=last_platform)[0]
            attempted.append(picked["platform"])
            last_platform = picked["platform"]
            rows = [row for row in rows if row["id"] != picked["id"]]
        self.assertEqual(attempted, ["douyin", "wechat_channels"])

    def test_one_item_rounds_reach_all_platforms_with_mixed_stage_debt(self):
        platforms = ("douyin", "xiaohongshu", "kuaishou", "wechat_channels")
        for download_platform in platforms:
            with self.subTest(download_platform=download_platform):
                rows = [{"id": i + 1, "platform": platform, "created_at": "2026-09-10"}
                        for i, platform in enumerate(platforms)]
                states = {row["id"]: {"reason": "download_pending" if row["platform"] == download_platform else "source_missing"} for row in rows}
                last_platform = "wechat_channels"
                attempted = []
                for _ in range(4):
                    picked = fair_local_order(rows, states, last_platform=last_platform)[0]
                    attempted.append(picked["platform"])
                    last_platform = picked["platform"]
                    # Retain debt to model a slow/unfinished item next tick.
                self.assertEqual(attempted, list(platforms))

    def test_download_priority_stays_within_the_rotating_platform(self):
        rows = [{"id": 1, "platform": "douyin", "source_expires_at": "2026-09-12T01:00:00Z"},
                {"id": 2, "platform": "wechat_channels", "created_at": "2026-09-01"},
                {"id": 3, "platform": "wechat_channels", "source_expires_at": "2026-09-12T03:00:00Z"}]
        states = {1: {"reason": "download_pending"}, 2: {"reason": "source_missing"},
                  3: {"reason": "download_pending"}}
        self.assertEqual([row["id"] for row in fair_local_order(rows, states, last_platform="douyin")], [3, 1, 2])

    def test_fifth_claim_slot_prefers_normal_debt_even_when_download_is_older(self):
        rows = [{"id": 1, "platform": "douyin", "created_at": "2026-09-01"},
                {"id": 2, "platform": "douyin", "created_at": "2026-09-10"},
                {"id": 3, "platform": "douyin", "created_at": "2026-09-11"}]
        states = {1: {"reason": "download_pending"}, 2: {"reason": "source_missing"},
                  3: {"reason": "asr_pending"}}
        selection = LocalMediaSelector(rows, states, claimed_count=4)
        self.assertEqual(selection.next_candidate()["id"], 2)
        # No successful claim: the fifth slot is still owed to normal debt.
        self.assertEqual(selection.next_candidate()["id"], 3)
        self.assertEqual(selection.claimed_count, 4)
        selection.record_claim()
        self.assertEqual(selection.claimed_count, 5)
        self.assertEqual(selection.next_candidate()["id"], 1)

    def test_oldest_slot_rotates_attempted_normal_debt_and_falls_back_when_none(self):
        rows = [{"id": 1, "platform": "douyin", "created_at": "2026-09-01"},
                {"id": 2, "platform": "douyin", "created_at": "2026-09-10"},
                {"id": 3, "platform": "douyin", "created_at": "2026-09-11"}]
        states = {1: {"reason": "download_pending"}, 2: {"reason": "source_missing"},
                  3: {"reason": "asr_pending"}}
        selection = LocalMediaSelector(rows, states, claimed_count=9, last_attempt_ids={1: 1, 2: 8, 3: 3})
        self.assertEqual(selection.next_candidate()["id"], 3)
        downloads = LocalMediaSelector(rows[:1], states, claimed_count=4, last_attempt_ids={1: 1})
        self.assertEqual(downloads.next_candidate()["id"], 1)

    def test_debt_read_has_stable_keys_and_never_writes_or_includes_history(self):
        with tempfile.TemporaryDirectory() as directory, storage.connect(Path(directory) / "fixture.db") as c:
            storage.initialize_database(c, target_version=23)
            for cid, kind, published, group in ((1, "unknown", "2026-09-10", ""), (2, "image", "2026-09-11", ""),
                    (3, "video", "2026-07-01", ""), (4, "video", "2026-09-10", "history-backfill"),
                    (5, "video", "2026-09-12", "")):
                c.execute("INSERT INTO content_items(id,link_id,platform,platform_content_id,canonical_url,title,content_type,published_at,source_group,imported_at,created_at,updated_at) VALUES(?,?, 'wechat_channels',?,'','fixture',?,?,?,?,?,?)",
                    (cid, f"Q{cid:05}", str(cid), kind, published, group, published, published, published))
            c.commit()
            changes = c.total_changes
            page = list_pending_media_work(c, at="2026-09-12T00:00:00Z")
            again = list_pending_media_work(c, at="2026-09-12T00:00:00Z", limit=1)
            self.assertEqual(c.total_changes, changes)
            self.assertEqual(page["total"], 3)
            self.assertEqual(page["items"][0]["work_key"], again["items"][0]["work_key"])
            self.assertEqual([i["reason"] for i in page["items"]], ["content_type_unresolved", "media_capability_unverified", "evaluation_release_required"])
            types = list_pending_media_work(c, at="2026-09-12T00:00:00Z", stage="类型")
            conclusions = list_pending_media_work(c, at="2026-09-12T00:00:00Z", stage="结论")
            self.assertEqual([i["content_id"] for i in types["items"]], [1, 2])
            self.assertEqual([i["content_id"] for i in conclusions["items"]], [5])
            self.assertEqual(c.total_changes, changes)
            self.assertFalse(any(i["paid_refresh_available"] for i in page["items"]))
            self.assertEqual(page["provider_calls"], 0)
            self.assertNotIn("decode_key", json.dumps(page))


if __name__ == "__main__":
    unittest.main()
