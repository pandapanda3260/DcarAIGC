from __future__ import annotations

import json
import http.client
import os
import tempfile
import unittest
from datetime import date
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError
from unittest.mock import patch

import v8.capture as capture_module
import v8.media as media_module
from v8.capture import CaptureError, ProviderResult, SlotUnavailable
from v8.evaluation import evaluate_content
from v8.matcher_dsl import POINT_IDS, POINT_SCENES
from v8.operations import IdentityConflictError, upsert_account, upsert_content
from v8.providers import (
    ProviderConfigurationError,
    _load_key,
    _collect_media_urls,
    _douyin_discovery_call,
    _douyin_image_url_groups,
    _douyin_media_urls,
    _douyin_reference_call,
    _parse_douyin_discovery_payload,
    _parse_douyin_stage_payload,
    _parse_xhs_discovery_payload,
    _rnote_call,
    _rnote_discovery_call,
    _request_json,
    _xhs_call,
    _xhs_media_urls,
    discover_account_content,
    materialize_zero_comment_evidence,
    retry_content_media,
    update_content_data,
)
from v8.storage import connect, initialize_database, now_utc
from v8.taxonomy_rule_backfill import backfill_v5_1_matcher_rules


VALID_SEC_UID = "MS4wLjAB" + "A" * 68


class ProviderCredentialLoadingTest(unittest.TestCase):
    def test_direct_environment_value_takes_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            configured = Path(temp) / "configured.key"
            configured.write_text("file-value\n", encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "TEST_PROVIDER_KEY": "  direct-value  ",
                    "TEST_PROVIDER_KEY_FILE": str(configured),
                },
                clear=True,
            ):
                self.assertEqual(
                    _load_key(Path(temp) / "default.key", "TEST_PROVIDER_KEY"),
                    "direct-value",
                )

    def test_environment_file_path_overrides_local_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            configured = Path(temp) / "configured.key"
            configured.write_text("file-value\n", encoding="utf-8")
            with patch.dict(
                os.environ,
                {"TEST_PROVIDER_KEY_FILE": str(configured)},
                clear=True,
            ):
                self.assertEqual(
                    _load_key(Path(temp) / "missing-default.key", "TEST_PROVIDER_KEY"),
                    "file-value",
                )


class V8ProviderUpdateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "providers.sqlite3"
        self.raw_root_patch = patch.object(
            capture_module, "RAW_ROOT", self.root / "raw"
        )
        self.raw_root_patch.start()
        self.addCleanup(self.raw_root_patch.stop)
        self.media_root_patch = patch("v8.media.MEDIA_ROOT", self.root / "media")
        self.media_root_patch.start()
        self.addCleanup(self.media_root_patch.stop)
        with connect(self.db) as connection:
            initialize_database(connection)
            captured_at = now_utc()
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id, version, status, definition, created_at, published_at
                ) VALUES ('taxonomy', 'selling-points-v5.0', 'published', 'test', ?, ?)
                """,
                (captured_at, captured_at),
            )
            for code in sorted(POINT_IDS):
                point = connection.execute(
                    """
                    INSERT INTO selling_points(
                        taxonomy_id, code, tier, label, positive_evidence_json
                    ) VALUES ('taxonomy', ?, ?, ?, ?)
                    """,
                    (
                        code,
                        "core" if code == "C1" else "other",
                        "汽车服务" if code == "C1" else f"卖点 {code}",
                        '["保养"]' if code == "C1" else "[]",
                    ),
                )
                for scene in sorted(POINT_SCENES[code]):
                    connection.execute(
                        """
                        INSERT INTO selling_point_scenes(selling_point_id, scene)
                        VALUES (?, ?)
                        """,
                        (point.lastrowid, scene),
                    )
            connection.commit()
        matcher = backfill_v5_1_matcher_rules(db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE taxonomy_versions SET status='retired'
                WHERE version='selling-points-v5.0'
                """
            )
            connection.execute(
                """
                UPDATE taxonomy_versions SET status='published', published_at=?
                WHERE version='selling-points-v5.1'
                """,
                (captured_at,),
            )
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id, rule_version, taxonomy_version, matcher_rule_sha256,
                    status, created_at, updated_at, activated_at
                ) VALUES (
                    'evaluation-v8__selling-points-v5.1', 'evaluation-v8',
                    'selling-points-v5.1', ?, 'active', ?, ?, ?
                )
                """,
                (
                    matcher["matcher_rule_sha256"],
                    captured_at,
                    captured_at,
                    captured_at,
                ),
            )
            connection.commit()
        content = upsert_content(
            {
                "platform": "douyin",
                "canonical_url": "https://www.douyin.com/video/123456789",
                "title": "待补详情",
                "content_type": "video",
            },
            db_path=self.db,
        )
        self.content_id = content["id"]

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_retired_rnote_private_adapters_fail_before_request(self) -> None:
        with patch("v8.providers._request_json") as request_json:
            with self.assertRaisesRegex(
                ProviderConfigurationError, "Rnote retired; use TikHub"
            ):
                _rnote_discovery_call("legacy-user", "legacy-key")
            with self.assertRaisesRegex(
                ProviderConfigurationError, "Rnote retired; use TikHub"
            ):
                _rnote_call("detail", "a" * 24, "legacy-key", "video")
        request_json.assert_not_called()

    def test_media_url_extraction_keeps_sources_and_excludes_covers_and_avatars(
        self,
    ) -> None:
        payload = {
            "video": {
                "play_addr": {"url_list": ["https://cdn.example/video.mp4"]},
                "subtitle_list": ["https://cdn.example/subtitle.json"],
                "cover": {"url_list": ["https://cdn.example/cover.jpg"]},
            },
            "author": {"avatar": "https://cdn.example/avatar.jpg"},
            "images": [
                {"url_default": "https://cdn.example/image-1.jpg"},
                {"url_pre": "https://cdn.example/image-2.jpg"},
            ],
        }
        self.assertEqual(
            _collect_media_urls(payload, "video"),
            ["https://cdn.example/video.mp4"],
        )
        self.assertEqual(
            _collect_media_urls(payload, "image"),
            ["https://cdn.example/image-1.jpg", "https://cdn.example/image-2.jpg"],
        )

    def test_douyin_image_urls_preserve_original_images_candidate_groups(self) -> None:
        standard = {
            "download_url_list": [
                "https://p3-sign.douyinpic.com/water/standard.webp",
                "https://p3-sign.douyinpic.com/water/standard.jpeg",
            ],
            "height": 1920,
            "url_list": [
                "https://p3-sign.douyinpic.com/aweme-images-v2/standard.heic",
                "https://p3-sign.douyinpic.com/aweme-images-v2/standard.jpeg",
            ],
            "width": 1080,
        }
        vvic = {
            "download_url_list": [
                "https://p3-sign.douyinpic.com/water/vvic.webp",
                "https://p3-sign.douyinpic.com/water/vvic.jpeg",
            ],
            "url_list": [
                "https://p11-sign.douyinpic.com/tos-cn-i/vvic-origin",
                "https://p3-sign.douyinpic.com/tos-cn-i/vvic-origin",
                "https://p3-sign.douyinpic.com/tos-cn-i/vvic-origin~tplv.jpeg",
            ],
        }
        kuchen = {
            "download_url_list": [
                "https://p3-sign.douyinpic.com/kuchen-v1-water/special.webp",
                "https://p3-sign.douyinpic.com/kuchen-v1-water/special.jpeg",
            ],
            "url_list": [
                "https://p26-sign.douyinpic.com/tos-cn-i/vvic-special",
                "https://p3-sign.douyinpic.com/tos-cn-i/vvic-special",
                "https://p3-sign.douyinpic.com/tos-cn-i/vvic-special~tplv.jpeg",
            ],
        }
        item = {"images": [standard, vvic, kuchen]}

        expected = [
            [*standard["download_url_list"], *standard["url_list"]],
            [*vvic["download_url_list"], *vvic["url_list"]],
            [*kuchen["download_url_list"], *kuchen["url_list"]],
        ]
        self.assertEqual(_douyin_image_url_groups(item), expected)
        self.assertEqual(
            _douyin_media_urls(item, "image"),
            [url for group in expected for url in group],
        )

    def test_douyin_image_groups_deduplicate_only_within_each_original_image(
        self,
    ) -> None:
        shared = "https://p3-sign.douyinpic.com/water/shared.jpeg"
        item = {
            "images": [
                {
                    "download_url_list": [shared, shared],
                    "url_list": [
                        "https://p3-sign.douyinpic.com/aweme-images-v2/one.jpeg"
                    ],
                },
                {
                    "download_url_list": [shared],
                    "url_list": [
                        "https://p3-sign.douyinpic.com/aweme-images-v2/two.jpeg"
                    ],
                },
            ]
        }

        self.assertEqual(
            _douyin_image_url_groups(item),
            [
                [
                    shared,
                    "https://p3-sign.douyinpic.com/aweme-images-v2/one.jpeg",
                ],
                [
                    shared,
                    "https://p3-sign.douyinpic.com/aweme-images-v2/two.jpeg",
                ],
            ],
        )

    def test_xhs_video_media_extraction_uses_streams_not_subtitles_or_frames(
        self,
    ) -> None:
        note = {
            "video_info_v2": {
                "image": {
                    "first_frame": "https://cdn.example/frame.webp",
                    "thumbnail": "https://cdn.example/thumb.webp",
                },
                "media": {
                    "stream": {
                        "h264": [
                            {
                                "default_stream": 1,
                                "master_url": "http://sns-v11.rednotecdn.com/stream/video.mp4",
                                "backup_urls": [
                                    "http://sns-v8.rednotecdn.com/stream/video.mp4",
                                ],
                            }
                        ],
                    },
                    "video": {
                        "subtitles": {
                            "zh-CN": [
                                {
                                    "url": "https://sns-subtitle.rednotecdn.com/subtitle.srt",
                                }
                            ]
                        },
                    },
                },
            },
        }
        self.assertEqual(
            _xhs_media_urls(note, "video"),
            [
                "http://sns-v11.rednotecdn.com/stream/video.mp4",
                "http://sns-v8.rednotecdn.com/stream/video.mp4",
            ],
        )

    def test_http_auth_failures_are_terminal_but_balance_can_retry(self) -> None:
        for status, retryable in ((401, False), (402, True), (403, False)):
            error = HTTPError(
                "https://provider.example/api", status, "blocked", {}, BytesIO(b"{}")
            )
            with (
                self.subTest(status=status),
                patch("v8.providers.urllib.request.urlopen", side_effect=error),
                self.assertRaises(CaptureError) as raised,
            ):
                _request_json(
                    "https://provider.example/api",
                    headers={},
                    params={},
                    provider="TestProvider",
                )
            self.assertEqual(raised.exception.retryable, retryable)

    def test_tikhub_explicit_unbilled_http_400_retry_stays_retryable(self) -> None:
        body = json.dumps(
            {"detail": {"message": "Request failed. Please retry."}}
        ).encode()
        error = HTTPError("https://api.tikhub.io/api", 400, "retry", {}, BytesIO(body))
        with (
            patch("v8.providers.urllib.request.urlopen", side_effect=error),
            self.assertRaises(CaptureError) as raised,
        ):
            _request_json(
                "https://api.tikhub.io/api",
                headers={},
                params={},
                provider="TikHub",
            )
        self.assertTrue(raised.exception.retryable)
        self.assertEqual(raised.exception.error_code, "provider_retry_requested")
        self.assertFalse(raised.exception.billed)

    def test_truncated_provider_response_is_retryable_transport_error(self) -> None:
        error = http.client.IncompleteRead(b"partial", 100)
        with (
            patch("v8.providers.urllib.request.urlopen", side_effect=error),
            self.assertRaises(CaptureError) as raised,
        ):
            _request_json(
                "https://api.tikhub.io/api",
                headers={},
                params={},
                provider="TikHub",
            )
        self.assertTrue(raised.exception.retryable)
        self.assertEqual(raised.exception.error_code, "transport_error")

    def test_complete_json_from_incomplete_read_is_recovered(self) -> None:
        payload = {"code": 200, "data": {"comments": [], "has_more": 0}}
        body = json.dumps(payload).encode()

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                raise http.client.IncompleteRead(body, len(body) + 100)

        with patch("v8.providers.urllib.request.urlopen", return_value=Response()):
            status, value = _request_json(
                "https://api.tikhub.io/api",
                headers={},
                params={},
                provider="TikHub",
            )
        self.assertEqual(status, 200)
        self.assertEqual(value, payload)

    def test_invalid_partial_from_response_stays_retryable_transport_error(self) -> None:
        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                raise http.client.IncompleteRead(b'{"unterminated":', 20)

        with (
            patch("v8.providers.urllib.request.urlopen", return_value=Response()),
            self.assertRaises(CaptureError) as raised,
        ):
            _request_json(
                "https://api.tikhub.io/api",
                headers={},
                params={},
                provider="TikHub",
            )
        self.assertEqual(raised.exception.error_code, "transport_error")
        self.assertTrue(raised.exception.retryable)

    def test_discovery_parsers_preserve_provider_pagination(self) -> None:
        xhs_payload = {
            "code": 200,
            "data": {
                "success": True,
                "code": 0,
                "data": {
                    "data": {
                        "notes": [
                            {
                                "note_id": "xhs-note-1",
                                "display_title": "图文首发",
                                "type": "normal",
                                "cursor": "xhs-next-cursor",
                                "user": {"nickname": "小红书汽车号"},
                                "images_list": [
                                    {
                                        "url_default": "https://cdn.example/xhs-image.jpg",
                                    }
                                ],
                                "view_count": 321,
                                "comments_count": 12,
                                "likes": 45,
                                "share_count": 6,
                                "collected_count": 7,
                            }
                        ],
                        "has_more": 1,
                    }
                },
            },
        }
        parsed = _parse_xhs_discovery_payload(xhs_payload)
        self.assertEqual(parsed["items"][0]["platform_content_id"], "xhs-note-1")
        self.assertEqual(parsed["items"][0]["content_type"], "image")
        self.assertEqual(
            parsed["items"][0]["media_urls"],
            ["https://cdn.example/xhs-image.jpg"],
        )
        self.assertEqual(parsed["items"][0]["metrics"]["view_count"], 321)
        self.assertEqual(parsed["items"][0]["metrics"]["comment_count"], 12)
        self.assertEqual(parsed["next_cursor"], "xhs-next-cursor")
        self.assertTrue(parsed["has_more"])

        douyin_payload = {
            "code": 200,
            "data": {
                "aweme_list": [
                    {
                        "aweme_id": "douyin-1",
                        "desc": "新车",
                        "author": {"nickname": "抖音汽车号"},
                        "video": {
                            "play_addr": {
                                "url_list": ["https://cdn.example/douyin-video.mp4"],
                            },
                        },
                        "statistics": {
                            "play_count": 456,
                            "comment_count": 23,
                            "digg_count": 67,
                            "share_count": 8,
                            "collect_count": 9,
                        },
                    }
                ],
                "max_cursor": 987654,
                "has_more": 1,
            },
        }
        with patch(
            "v8.providers._request_json", return_value=(200, douyin_payload)
        ) as request:
            outcome = _douyin_discovery_call("MS4w.test", "secret", 123456)
        self.assertEqual(request.call_args.kwargs["params"]["max_cursor"], 123456)
        self.assertEqual(outcome.data["next_cursor"], 987654)
        self.assertTrue(outcome.data["has_more"])
        self.assertEqual(
            outcome.data["items"][0]["media_urls"],
            ["https://cdn.example/douyin-video.mp4"],
        )
        self.assertEqual(outcome.data["items"][0]["metrics"]["view_count"], 456)
        self.assertEqual(outcome.data["items"][0]["metrics"]["comment_count"], 23)

    def test_discovery_parsers_fail_closed_when_has_more_is_missing(self) -> None:
        douyin_payload = {"code": 200, "data": {"aweme_list": []}}
        with self.assertRaises(CaptureError) as douyin_error:
            _parse_douyin_discovery_payload(douyin_payload)
        self.assertEqual(douyin_error.exception.error_code, "invalid_response")

        xhs_payload = {
            "code": 200,
            "data": {"data": {"data": {"notes": []}}},
        }
        with self.assertRaises(CaptureError) as xhs_error:
            _parse_xhs_discovery_payload(xhs_payload)
        self.assertEqual(xhs_error.exception.error_code, "invalid_response")

    def test_douyin_http_200_business_error_is_not_an_empty_success(self) -> None:
        payload = {
            "code": 200,
            "data": {
                "aweme_list": [],
                "has_more": 0,
                "status_code": 5,
                "status_msg": "参数不合法",
            },
        }
        with self.assertRaises(CaptureError) as raised:
            _parse_douyin_discovery_payload(payload)
        self.assertEqual(raised.exception.error_code, "upstream_invalid_request")
        self.assertFalse(raised.exception.retryable)
        self.assertTrue(raised.exception.billed)

    def test_douyin_statistics_requires_play_count_but_accepts_explicit_zero(
        self,
    ) -> None:
        missing_payload = {
            "code": 200,
            "data": {"statistics_list": [{"aweme_id": "123456789"}]},
        }
        with self.assertRaises(CaptureError) as raised:
            _parse_douyin_stage_payload(
                "metrics", "123456789", missing_payload, status=200
            )
        self.assertEqual(raised.exception.error_code, "invalid_response")
        self.assertTrue(raised.exception.retryable)
        self.assertTrue(raised.exception.billed)

        zero_payload = {
            "code": 200,
            "data": {"statistics_list": [{"aweme_id": "123456789", "play_count": 0}]},
        }
        parsed = _parse_douyin_stage_payload(
            "metrics", "123456789", zero_payload, status=200
        )
        self.assertEqual(parsed.data["view_count"], 0)

    def test_douyin_comments_rejects_invalid_pagination_cursor(self) -> None:
        payload = {
            "code": 200,
            "data": {
                "comments": [],
                "has_more": 1,
                "cursor": "not-a-number",
                "total": 0,
            },
        }
        with self.assertRaises(CaptureError) as raised:
            _parse_douyin_stage_payload(
                "comments", "123456789", payload, status=200
            )
        self.assertEqual(raised.exception.error_code, "invalid_response")
        self.assertTrue(raised.exception.retryable)
        self.assertTrue(raised.exception.billed)

    def test_douyin_comments_rejects_missing_pagination_cursor(self) -> None:
        payload = {
            "code": 200,
            "data": {"comments": [], "has_more": 1, "total": 20},
        }
        with self.assertRaises(CaptureError) as raised:
            _parse_douyin_stage_payload(
                "comments", "123456789", payload, status=200
            )
        self.assertEqual(raised.exception.error_code, "invalid_response")
        self.assertTrue(raised.exception.retryable)

    def test_discovery_zero_views_does_not_close_real_statistics_slot(self) -> None:
        account = upsert_account(
            {
                "phone": "13800138009",
                "platforms": [
                    {
                        "platform": "douyin",
                        "uid": "99887779",
                        "nickname": "零播放占位测试号",
                    }
                ],
            },
            db_path=self.db,
        )

        def discovery_call(operation, identity):
            if operation == "resolve_account":
                return ProviderResult(
                    {"reference": VALID_SEC_UID},
                    {"data": {"sec_user_id": VALID_SEC_UID}},
                    200,
                    True,
                )
            return ProviderResult(
                {
                    "items": [
                        {
                            "platform": "douyin",
                            "platform_content_id": "987654330",
                            "canonical_url": "https://www.douyin.com/video/987654330",
                            "title": "发现列表零播放占位",
                            "body": "发现列表零播放占位",
                            "published_at": "2026-08-02T01:00:00Z",
                            "content_type": "video",
                            "media_urls": ["https://cdn.example/zero-view.mp4"],
                            "metrics": {
                                "view_count": 0,
                                "comment_count": 3,
                                "like_count": 20,
                                "share_count": 1,
                                "collect_count": 2,
                            },
                        }
                    ],
                },
                {"data": {"aweme_list": [{"aweme_id": "987654330"}]}},
                200,
                True,
            )

        discovered = discover_account_content(
            int(account["id"]),
            "douyin",
            "99887779",
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=discovery_call,
        )
        self.assertEqual(discovered["derived_stages"]["created"], 1)
        self.assertEqual(discovered["derived_stages"]["skipped"], 1)
        with connect(self.db) as connection:
            content = connection.execute(
                "SELECT id FROM content_items WHERE platform_content_id='987654330'"
            ).fetchone()
            metrics_slot = connection.execute(
                "SELECT id FROM fetch_slots WHERE content_id=? AND stage='metrics'",
                (content["id"],),
            ).fetchone()
            snapshot = connection.execute(
                """
                SELECT view_count,comment_count,like_count,status,metadata_json
                FROM content_metric_snapshots WHERE content_id=?
                """,
                (content["id"],),
            ).fetchone()
        self.assertIsNone(metrics_slot)
        self.assertIsNone(snapshot["view_count"])
        self.assertEqual(snapshot["comment_count"], 3)
        self.assertEqual(snapshot["like_count"], 20)
        self.assertEqual(snapshot["status"], "missing")
        self.assertEqual(
            json.loads(snapshot["metadata_json"])["exposure_observation"],
            "missing_or_placeholder",
        )

        calls = []

        def statistics_call(stage, content_row):
            calls.append(stage)
            return ProviderResult(
                {
                    "view_count": 321,
                },
                {"operation": stage, "view_count": 321},
                200,
                True,
            )

        refreshed = update_content_data(
            int(content["id"]),
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=statistics_call,
            stages=["metrics"],
            process_media=False,
        )
        self.assertEqual(calls, ["metrics"])
        self.assertEqual(refreshed["status"], "succeeded")
        with connect(self.db) as connection:
            snapshot = connection.execute(
                """
                SELECT view_count,comment_count,like_count,status,metadata_json
                FROM content_metric_snapshots WHERE content_id=?
                """,
                (content["id"],),
            ).fetchone()
        self.assertEqual(snapshot["view_count"], 321)
        self.assertEqual(snapshot["comment_count"], 3)
        self.assertEqual(snapshot["like_count"], 20)
        self.assertEqual(snapshot["status"], "available")
        self.assertEqual(
            json.loads(snapshot["metadata_json"])["exposure_observation"],
            "observed",
        )

        def repeated_discovery_call(operation, identity):
            self.assertEqual(operation, "discover_content")
            return ProviderResult(
                {
                    "items": [
                        {
                            "platform": "douyin",
                            "platform_content_id": "987654330",
                            "canonical_url": "https://www.douyin.com/video/987654330",
                            "title": "发现列表零播放占位",
                            "body": "发现列表零播放占位",
                            "published_at": "2026-08-02T01:00:00Z",
                            "content_type": "video",
                            "media_urls": ["https://cdn.example/zero-view.mp4"],
                            "metrics": {
                                "view_count": 0,
                                "comment_count": 4,
                                "like_count": 21,
                                "share_count": 1,
                                "collect_count": 2,
                            },
                        }
                    ],
                    "has_more": False,
                },
                {"data": {"aweme_list": [{"aweme_id": "987654330"}]}},
                200,
                True,
            )

        discover_account_content(
            int(account["id"]),
            "douyin",
            "99887779",
            as_of=date(2026, 8, 2),
            window_key="2026-08-02:douyin:page:2",
            db_path=self.db,
            call_override=repeated_discovery_call,
        )
        with connect(self.db) as connection:
            preserved = connection.execute(
                """
                SELECT view_count,comment_count,like_count,status,metadata_json
                FROM content_metric_snapshots WHERE content_id=? AND window_key='2026-08-02'
                """,
                (content["id"],),
            ).fetchone()
        self.assertEqual(preserved["view_count"], 321)
        self.assertEqual(preserved["comment_count"], 3)
        self.assertEqual(preserved["like_count"], 20)
        self.assertEqual(preserved["status"], "available")
        self.assertEqual(
            json.loads(preserved["metadata_json"])["exposure_observation"],
            "observed",
        )

        def missing_statistics_call(stage, content_row):
            return ProviderResult(
                {"view_count": None},
                {"operation": stage, "statistics_list": [{}]},
                200,
                True,
            )

        update_content_data(
            int(content["id"]),
            as_of=date(2026, 8, 3),
            db_path=self.db,
            call_override=missing_statistics_call,
            stages=["metrics"],
            process_media=False,
        )
        with connect(self.db) as connection:
            missing_snapshot = connection.execute(
                """
                SELECT view_count,status,metadata_json
                FROM content_metric_snapshots
                WHERE content_id=? AND window_key='2026-08-03'
                """,
                (content["id"],),
            ).fetchone()
        self.assertIsNone(missing_snapshot["view_count"])
        self.assertEqual(missing_snapshot["status"], "missing")
        self.assertEqual(
            json.loads(missing_snapshot["metadata_json"])["exposure_observation"],
            "missing_from_statistics_response",
        )

    def test_douyin_numeric_uid_resolves_through_profile_endpoint(self) -> None:
        payload = {
            "code": 200,
            "data": {"user": {"sec_uid": VALID_SEC_UID}},
        }
        with patch(
            "v8.providers._request_json", return_value=(200, payload)
        ) as request:
            outcome = _douyin_reference_call("7634084008151188537", "secret")
        self.assertTrue(
            request.call_args.args[0].endswith("/fetch_user_profile_by_uid")
        )
        self.assertEqual(outcome.data["reference"], VALID_SEC_UID)

    def test_xhs_image_and_video_detail_use_app_v2_and_normalize_metrics(self) -> None:
        image_note = {
            "id": "xhs-image-1",
            "title": "图文标题",
            "desc": "图文正文",
            "time": 1782105579,
            "type": "normal",
            "user": {"userid": "author-image", "nickname": "图文作者"},
            "view_count": 100,
            "comments_count": 20,
            "liked_count": 30,
            "shared_count": 4,
            "collected_count": 5,
            "images_list": [{"original": "https://cdn.example/xhs-image.webp"}],
        }
        image_payload = {
            "code": 200,
            "data": {"success": True, "code": 0, "data": [{"note_list": [image_note]}]},
        }
        with patch(
            "v8.providers._request_json", return_value=(200, image_payload)
        ) as request:
            image = _xhs_call("detail", "xhs-image-1", "secret", "image")
        self.assertTrue(request.call_args.args[0].endswith("/get_image_note_detail"))
        self.assertEqual(
            image.data["media_urls"], ["https://cdn.example/xhs-image.webp"]
        )
        self.assertEqual(image.data["metrics"]["view_count"], 100)
        self.assertEqual(image.data["metrics"]["collect_count"], 5)
        with patch(
            "v8.providers._request_json", return_value=(200, image_payload)
        ) as request:
            metrics = _xhs_call("metrics", "xhs-image-1", "secret", "image")
        self.assertTrue(request.call_args.args[0].endswith("/get_image_note_detail"))
        self.assertEqual(metrics.data["like_count"], 30)

        video_note = {
            "id": "xhs-video-1",
            "title": "视频标题",
            "desc": "视频正文",
            "time": 1782105580,
            "type": "video",
            "user": {"userid": "author-video", "nickname": "视频作者"},
            "view_count": 200,
            "comments_count": 40,
            "liked_count": 60,
            "shared_count": 8,
            "collected_count": 10,
            "video_info_v2": {
                "media": {
                    "stream": {
                        "h264": [{"master_url": "https://cdn.example/xhs-video.mp4"}]
                    }
                }
            },
        }
        video_payload = {
            "code": 200,
            "data": {"success": True, "code": 0, "data": [video_note]},
        }
        with patch(
            "v8.providers._request_json", return_value=(200, video_payload)
        ) as request:
            video = _xhs_call("detail", "xhs-video-1", "secret", "video")
        self.assertTrue(request.call_args.args[0].endswith("/get_video_note_detail"))
        self.assertEqual(
            video.data["media_urls"], ["https://cdn.example/xhs-video.mp4"]
        )
        self.assertEqual(video.data["metrics"]["comment_count"], 40)

    def test_xhs_latest_comments_are_anonymized_before_raw_storage(self) -> None:
        payload = {
            "code": 200,
            "data": {
                "success": True,
                "code": 0,
                "data": {
                    "comment_count": 2,
                    "cursor": "comment-next",
                    "index": 1,
                    "pageArea": "UNFOLDED",
                    "has_more": True,
                    "comments": [
                        {
                            "id": "comment-1",
                            "content": "这款车保养多少钱",
                            "time": 1782814412,
                            "like_count": 3,
                            "user": {
                                "userid": "raw-user-secret",
                                "nickname": "昵称秘密",
                                "red_id": "private-red-id",
                            },
                            "sub_comments": [
                                {
                                    "id": "comment-2",
                                    "content": "同问",
                                    "time": 1782814420,
                                    "user": {
                                        "userid": "raw-reply-secret",
                                        "nickname": "回复昵称",
                                    },
                                }
                            ],
                        }
                    ],
                },
            },
        }
        with patch(
            "v8.providers._request_json", return_value=(200, payload)
        ) as request:
            outcome = _xhs_call("comments", "xhs-note-1", "secret", "image")
        params = request.call_args.kwargs["params"]
        self.assertEqual(params["sort_strategy"], "latest_v2")
        self.assertEqual(params["pageArea"], "UNFOLDED")
        self.assertEqual(len(outcome.data["comments"]), 2)
        self.assertEqual(outcome.data["comments"][1]["parent_comment_id"], "comment-1")
        raw_text = json.dumps(outcome.raw_response, ensure_ascii=False)
        self.assertNotIn("raw-user-secret", raw_text)
        self.assertNotIn("昵称秘密", raw_text)
        self.assertNotIn("private-red-id", raw_text)
        self.assertIn("这款车保养多少钱", raw_text)
        self.assertEqual(
            outcome.data["next_cursor_params"],
            {"cursor": "comment-next", "index": 1, "pageArea": "UNFOLDED"},
        )
        self.assertIn('"next_cursor_params"', raw_text)

    def test_xhs_plain_cursor_without_continuation_context_is_rejected(self) -> None:
        payload = {
            "code": 200,
            "data": {
                "success": True,
                "code": 0,
                "data": {
                    "comment_count_l1": 10,
                    "cursor": "opaque-only",
                    "has_more": True,
                    "comments": [],
                },
            },
        }
        with (
            patch("v8.providers._request_json", return_value=(200, payload)),
            self.assertRaises(CaptureError) as raised,
        ):
            _xhs_call("comments", "xhs-note-1", "secret", "image")
        self.assertEqual(raised.exception.error_code, "invalid_response")

    def test_xhs_comment_cursor_json_and_l1_total_are_preserved(self) -> None:
        cursor = {
            "cursor": "cursor-token",
            "index": 17,
            "pageArea": "FOLDED",
        }
        payload = {
            "code": 200,
            "data": {
                "success": True,
                "code": 0,
                "data": {
                    "comment_count": 37,
                    "comment_count_l1": 10,
                    "cursor": json.dumps(cursor),
                    "has_more": True,
                    "comments": [],
                },
            },
        }
        with patch("v8.providers._request_json", return_value=(200, payload)):
            first = _xhs_call("comments", "xhs-note-1", "secret", "image")
        self.assertEqual(first.data["declared_total"], 10)
        self.assertEqual(first.data["next_cursor_params"], cursor)

        exhausted = {
            "code": 200,
            "data": {
                "success": True,
                "code": 0,
                "data": {
                    "comment_count_l1": 10,
                    "has_more": False,
                    "comments": [],
                },
            },
        }
        with patch(
            "v8.providers._request_json", return_value=(200, exhausted)
        ) as request:
            _xhs_call(
                "comments",
                "xhs-note-1",
                "secret",
                "image",
                cursor=first.data["next_cursor_params"],
            )
        params = request.call_args.kwargs["params"]
        self.assertEqual(params["cursor"], "cursor-token")
        self.assertEqual(params["index"], 17)
        self.assertEqual(params["pageArea"], "FOLDED")

    @staticmethod
    def successful_call(stage, content):
        if stage == "detail":
            data = {
                "title": "汽车保养完整内容",
                "body": "汽车保养维修完整正文",
                "published_at": "2026-08-01T04:00:00Z",
                "account_uid": "99887766",
                "account_name": "汽车号",
                "content_type": "video",
            }
        elif stage == "metrics":
            data = {
                "view_count": 1000,
                "comment_count": None,
                "like_count": 50,
                "share_count": 5,
                "collect_count": None,
            }
        else:
            data = {
                "comment_count": 1,
                "comments": [
                    {
                        "platform_comment_id": "c1",
                        "anonymous_user_key": "U" + "a" * 64,
                        "body": "这款车保养多少钱",
                        "published_at": "2026-08-02T00:00:00Z",
                        "like_count": 3,
                        "parent_comment_id": None,
                    }
                ],
            }
        return ProviderResult(
            data=data,
            raw_response={"stage": stage, "data": data},
            http_status=200,
            billed=True,
        )

    def test_one_row_update_obeys_lifetime_daily_weekly_slots_and_records_costs(
        self,
    ) -> None:
        first = update_content_data(
            self.content_id, db_path=self.db, call_override=self.successful_call
        )
        self.assertEqual(first["status"], "succeeded")
        self.assertEqual(first["provider_cost"], 0.003)
        self.assertEqual(first["media"]["status"], "no_source")
        self.assertEqual(
            [item["stage"] for item in first["stages"]],
            ["detail", "metrics", "comments"],
        )
        second = update_content_data(
            self.content_id, db_path=self.db, call_override=self.successful_call
        )
        self.assertEqual(second["provider_cost"], 0)
        self.assertEqual(
            [item["status"] for item in second["stages"]],
            ["already_succeeded", "already_succeeded"],
        )
        with connect(self.db) as connection:
            content = connection.execute(
                "SELECT * FROM content_items WHERE id=?", (self.content_id,)
            ).fetchone()
            slots = connection.execute(
                "SELECT stage,status FROM fetch_slots ORDER BY stage"
            ).fetchall()
            snapshot = connection.execute(
                "SELECT * FROM content_metric_snapshots"
            ).fetchone()
            comment_version = connection.execute(
                "SELECT * FROM comment_evidence_versions"
            ).fetchone()
            comments = connection.execute("SELECT * FROM comments").fetchall()
            scores = connection.execute("SELECT * FROM comment_user_scores").fetchall()
            raw_count = connection.execute(
                "SELECT COUNT(*) FROM provider_raw_responses"
            ).fetchone()[0]
            usage = connection.execute(
                "SELECT SUM(amount) FROM provider_usage"
            ).fetchone()[0]
        self.assertEqual(content["title"], "汽车保养完整内容")
        self.assertEqual(snapshot["view_count"], 1000)
        self.assertEqual(comment_version["comment_count"], 1)
        self.assertEqual(len(comments), 1)
        self.assertEqual(len(scores), 1)
        self.assertEqual(raw_count, 3)
        self.assertAlmostEqual(usage, 0.003)
        self.assertTrue(all(row["status"] == "succeeded" for row in slots))

    def test_failed_unbilled_provider_call_stays_retryable_and_cost_is_released(
        self,
    ) -> None:
        with connect(self.db) as connection:
            captured_at = now_utc()
            for stage, key in (("detail", "lifetime"), ("comments", "2026-W31")):
                connection.execute(
                    """
                    INSERT INTO fetch_slots(
                        content_id, stage, window_key, provider, adapter_version,
                        status, attempt_count, created_at, updated_at
                    ) VALUES (?, ?, ?, 'legacy-cache', 'migration-v1', 'succeeded', 1, ?, ?)
                    """,
                    (self.content_id, stage, key, captured_at, captured_at),
                )
            connection.commit()

        def fail(stage, content):
            raise CaptureError(
                "insufficient balance",
                retryable=True,
                error_code="provider_balance_blocked",
                http_status=402,
                billed=False,
                raw_response={"detail": "insufficient balance"},
            )

        result = update_content_data(
            self.content_id, db_path=self.db, call_override=fail
        )
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["provider_cost"], 0)
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT * FROM fetch_slots WHERE stage='metrics' AND provider='TikHub'"
            ).fetchone()
            budget = connection.execute(
                "SELECT * FROM provider_budget_batches WHERE operation='douyin_video_statistics'"
            ).fetchone()
        self.assertEqual(slot["status"], "retryable_failed")
        self.assertEqual(budget["consumed_requests"], 0)
        self.assertEqual(budget["consumed_amount"], 0)

    def test_terminal_content_slot_remains_partial_on_resume(self) -> None:
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO fetch_slots(
                    content_id,stage,window_key,provider,adapter_version,status,
                    attempt_count,last_error_code,last_error_message,
                    created_at,updated_at,finished_at
                ) VALUES (?, 'detail', 'lifetime', 'TikHub', 'test-v1',
                          'terminal_failed', 1, 'content_unavailable',
                          'provider says content unavailable', ?, ?, ?)
                """,
                (self.content_id, captured_at, captured_at, captured_at),
            )
            connection.commit()

        def no_provider_call(stage, content):
            raise AssertionError(f"unexpected provider call: {stage}")

        for _ in range(2):
            result = update_content_data(
                self.content_id,
                db_path=self.db,
                call_override=no_provider_call,
                stages=["detail"],
                process_media=False,
            )
            self.assertEqual(result["status"], "partial")
            self.assertEqual(result["stages"][0]["status"], "failed")
            self.assertEqual(
                result["stages"][0]["error_code"], "content_unavailable"
            )
            self.assertEqual(result["stages"][0]["slot_status"], "terminal_failed")

    def test_account_discovery_caches_provider_reference_and_upserts_new_content(
        self,
    ) -> None:
        account = upsert_account(
            {
                "phone": "13800138000",
                "operator_name": "运营甲",
                "platforms": [
                    {"platform": "douyin", "uid": "99887766", "nickname": "汽车号"}
                ],
            },
            db_path=self.db,
        )

        def discovery_call(operation, identity):
            if operation == "resolve_account":
                return ProviderResult(
                    {"reference": VALID_SEC_UID},
                    {"data": {"sec_user_id": VALID_SEC_UID}},
                    200,
                    True,
                )
            return ProviderResult(
                {
                    "items": [
                        {
                            "platform": "douyin",
                            "platform_content_id": "987654321",
                            "canonical_url": "https://www.douyin.com/video/987654321",
                            "title": "新车首发",
                            "body": "新车首发完整内容",
                            "published_at": "2026-08-02T01:00:00Z",
                            "content_type": "video",
                            "media_urls": ["https://cdn.example/new-video.mp4"],
                            "metrics": {
                                "view_count": 1000,
                                "comment_count": 20,
                                "like_count": 80,
                                "share_count": 9,
                                "collect_count": 11,
                            },
                        }
                    ],
                    "has_more": False,
                },
                {
                    "data": {
                        "aweme_list": [
                            {
                                "aweme_id": "987654321",
                                "video": {
                                    "play_addr": {
                                        "url_list": [
                                            "https://cdn.example/new-video.mp4",
                                        ]
                                    }
                                },
                                "statistics": {"play_count": 1000},
                            }
                        ],
                        "has_more": 0,
                    },
                },
                200,
                True,
            )

        first = discover_account_content(
            int(account["id"]),
            "douyin",
            "99887766",
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=discovery_call,
        )
        second = discover_account_content(
            int(account["id"]),
            "douyin",
            "99887766",
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=discovery_call,
        )
        self.assertEqual(first["status"], "succeeded")
        self.assertEqual(first["reference_status"], "resolved")
        self.assertEqual(first["inserted"], 1)
        self.assertEqual(first["provider_cost"], 0.002)
        self.assertEqual(first["derived_stages"]["created"], 2)
        self.assertEqual(second["status"], "already_succeeded")
        self.assertEqual(second["provider_cost"], 0)
        self.assertEqual(second["derived_stages"]["already_succeeded"], 2)
        with connect(self.db) as connection:
            discovered = connection.execute(
                "SELECT * FROM content_items WHERE platform_content_id='987654321'"
            ).fetchone()
            references = connection.execute(
                "SELECT * FROM account_provider_references"
            ).fetchall()
            slots = connection.execute(
                "SELECT * FROM fetch_slots WHERE account_id=? ORDER BY window_key",
                (account["id"],),
            ).fetchall()
            raws = connection.execute(
                "SELECT * FROM provider_raw_responses WHERE account_id=?",
                (account["id"],),
            ).fetchall()
            content_slots = connection.execute(
                "SELECT stage,status,provider FROM fetch_slots WHERE content_id=? ORDER BY stage",
                (discovered["id"],),
            ).fetchall()
            snapshot = connection.execute(
                "SELECT * FROM content_metric_snapshots WHERE content_id=?",
                (discovered["id"],),
            ).fetchone()
            media_source = connection.execute(
                "SELECT * FROM evidence_artifacts WHERE content_id=? AND artifact_type='media_source'",
                (discovered["id"],),
            ).fetchone()
            usage = connection.execute(
                "SELECT COUNT(*) count, SUM(amount) amount FROM provider_usage"
            ).fetchone()
            derived_attempts = connection.execute(
                """
                SELECT fa.billed,fa.amount
                FROM fetch_attempts fa
                JOIN fetch_slots fs ON fs.id=fa.slot_id
                WHERE fs.content_id=? ORDER BY fs.stage
                """,
                (discovered["id"],),
            ).fetchall()
            derived_raws = connection.execute(
                "SELECT source FROM provider_raw_responses WHERE content_id=? ORDER BY operation",
                (discovered["id"],),
            ).fetchall()
        self.assertEqual(discovered["account_id"], account["id"])
        self.assertEqual(len(references), 1)
        self.assertEqual(references[0]["reference_value"], VALID_SEC_UID)
        self.assertEqual(len(slots), 2)
        self.assertTrue(all(row["status"] == "succeeded" for row in slots))
        self.assertEqual(len(raws), 2)
        self.assertEqual(
            [(row["stage"], row["status"], row["provider"]) for row in content_slots],
            [("detail", "succeeded", "TikHub"), ("metrics", "succeeded", "TikHub")],
        )
        self.assertEqual(snapshot["view_count"], 1000)
        discovery_raw = next(
            row for row in raws if row["operation"] == "douyin_user_posts"
        )
        self.assertEqual(snapshot["captured_at"], discovery_raw["captured_at"])
        self.assertGreaterEqual(discovered["updated_at"], discovered["created_at"])
        self.assertIsNotNone(media_source)
        self.assertEqual(usage["count"], 2)
        self.assertAlmostEqual(float(usage["amount"]), 0.002)
        self.assertEqual(
            [(row["billed"], row["amount"]) for row in derived_attempts],
            [(0, 0.0), (0, 0.0)],
        )
        self.assertEqual(
            [row["source"] for row in derived_raws],
            ["derived_applied", "derived_applied"],
        )

    def test_discovery_identity_conflict_is_terminal_and_does_not_rebill(self) -> None:
        account = upsert_account(
            {
                "phone": "13800138088",
                "platforms": [
                    {
                        "platform": "xiaohongshu",
                        "uid": "identity-conflict-account",
                        "nickname": "冲突账号",
                    }
                ],
            },
            db_path=self.db,
        )
        first_id = "a" * 24
        second_id = "b" * 24
        first_url = f"https://www.xiaohongshu.com/explore/{first_id}"
        second_url = f"https://www.xiaohongshu.com/explore/{second_id}"
        first = upsert_content(
            {
                "platform": "xiaohongshu",
                "platform_content_id": first_id,
                "canonical_url": first_url,
                "title": "第一条汽车保养内容",
                "body": "第一条汽车保养完整正文证据",
            },
            db_path=self.db,
        )
        second = upsert_content(
            {
                "platform": "xiaohongshu",
                "platform_content_id": second_id,
                "canonical_url": second_url,
                "title": "第二条汽车保养内容",
                "body": "第二条汽车保养完整正文证据",
            },
            db_path=self.db,
        )
        evaluations = {
            evaluate_content(int(first["id"]), db_path=self.db).evaluation_id,
            evaluate_content(int(second["id"]), db_path=self.db).evaluation_id,
        }
        with connect(self.db) as connection:
            review_count_before = connection.execute(
                "SELECT COUNT(*) FROM review_queue"
            ).fetchone()[0]

        calls = 0

        def discovery_call(operation, identity):
            nonlocal calls
            calls += 1
            return ProviderResult(
                {
                    "items": [
                        {
                            "platform": "xiaohongshu",
                            "platform_content_id": second_id,
                            "canonical_url": first_url,
                            "title": "冲突内容",
                            "body": "冲突内容正文",
                            "content_type": "image",
                        }
                    ],
                    "has_more": False,
                },
                {"data": {"items": [{"id": second_id}]}},
                200,
                True,
            )

        with self.assertRaisesRegex(
            IdentityConflictError, "identity_conflict"
        ) as raised:
            discover_account_content(
                int(account["id"]),
                "xiaohongshu",
                "identity-conflict-account",
                as_of=date(2026, 8, 2),
                window_key="identity-conflict-page",
                db_path=self.db,
                call_override=discovery_call,
            )
        self.assertEqual(calls, 1)
        self.assertEqual(raised.exception.error_code, "identity_conflict")
        self.assertAlmostEqual(raised.exception.provider_cost, 0.01)

        with connect(self.db) as connection:
            slot = connection.execute(
                """
                SELECT * FROM fetch_slots
                WHERE account_id=? AND stage='discovery'
                  AND window_key='identity-conflict-page'
                """,
                (account["id"],),
            ).fetchone()
            attempt = connection.execute(
                "SELECT * FROM fetch_attempts WHERE slot_id=?", (slot["id"],)
            ).fetchone()
            raw = connection.execute(
                "SELECT * FROM provider_raw_responses WHERE fetch_attempt_id=?",
                (attempt["id"],),
            ).fetchone()
            usage_before_retry = connection.execute(
                "SELECT COUNT(*) count,COALESCE(SUM(amount),0) amount FROM provider_usage"
            ).fetchone()
            relations = connection.execute(
                "SELECT * FROM duplicate_relations WHERE method='identity_conflict'"
            ).fetchall()
            review_count_after = connection.execute(
                "SELECT COUNT(*) FROM review_queue"
            ).fetchone()[0]
            evaluation_rows = connection.execute(
                "SELECT id,content_id FROM evaluation_versions WHERE id IN (?,?)",
                tuple(sorted(evaluations)),
            ).fetchall()
            content_ids = {
                int(row[0])
                for row in connection.execute(
                    "SELECT id FROM content_items WHERE id IN (?,?)",
                    (first["id"], second["id"]),
                )
            }
        self.assertEqual(slot["status"], "terminal_failed")
        self.assertEqual(slot["last_error_code"], "identity_conflict")
        self.assertEqual(slot["finished_at"], attempt["response_finished_at"])
        self.assertEqual(raw["source"], "live")
        self.assertEqual(tuple(usage_before_retry), (1, 0.01))
        self.assertEqual(len(relations), 1)
        self.assertEqual(relations[0]["status"], "pending_review")
        self.assertEqual(review_count_after, review_count_before)
        self.assertEqual({row["id"] for row in evaluation_rows}, evaluations)
        self.assertEqual(content_ids, {first["id"], second["id"]})

        def no_provider_call(operation, identity):
            raise AssertionError(f"unexpected provider call: {operation}")

        with self.assertRaisesRegex(IdentityConflictError, "identity_conflict"):
            discover_account_content(
                int(account["id"]),
                "xiaohongshu",
                "identity-conflict-account",
                as_of=date(2026, 8, 2),
                window_key="identity-conflict-page",
                db_path=self.db,
                call_override=no_provider_call,
            )
        with connect(self.db) as connection:
            usage_after_retry = connection.execute(
                "SELECT COUNT(*) count,COALESCE(SUM(amount),0) amount FROM provider_usage"
            ).fetchone()
            relation_count = connection.execute(
                """
                SELECT COUNT(*) FROM duplicate_relations
                WHERE method='identity_conflict' AND status='pending_review'
                """
            ).fetchone()[0]
            violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        self.assertEqual(tuple(usage_after_retry), tuple(usage_before_retry))
        self.assertEqual(relation_count, 1)
        self.assertEqual(violations, [])

    def test_replayed_discovery_identity_conflict_uses_canonical_writer(self) -> None:
        account = upsert_account(
            {
                "phone": "13800138089",
                "platforms": [
                    {
                        "platform": "xiaohongshu",
                        "uid": "replay-identity-conflict-account",
                        "nickname": "重放冲突账号",
                    }
                ],
            },
            db_path=self.db,
        )
        first_id = "c" * 24
        second_id = "d" * 24
        first_url = f"https://www.xiaohongshu.com/explore/{first_id}"
        second_url = f"https://www.xiaohongshu.com/explore/{second_id}"
        first = upsert_content(
            {
                "platform": "xiaohongshu",
                "platform_content_id": first_id,
                "canonical_url": second_url,
                "title": "重放冲突第一条汽车保养内容",
                "body": "重放冲突第一条汽车保养完整正文证据",
            },
            db_path=self.db,
        )
        second = upsert_content(
            {
                "platform": "xiaohongshu",
                "platform_content_id": second_id,
                "canonical_url": first_url,
                "title": "重放冲突第二条汽车保养内容",
                "body": "重放冲突第二条汽车保养完整正文证据",
            },
            db_path=self.db,
        )
        evaluations = {
            evaluate_content(int(first["id"]), db_path=self.db).evaluation_id,
            evaluate_content(int(second["id"]), db_path=self.db).evaluation_id,
        }
        with connect(self.db) as connection:
            contents_before = connection.execute(
                """
                SELECT id,platform_content_id,canonical_url,title,body,updated_at
                FROM content_items WHERE id IN (?,?) ORDER BY id
                """,
                (first["id"], second["id"]),
            ).fetchall()
            review_count_before = connection.execute(
                "SELECT COUNT(*) FROM review_queue"
            ).fetchone()[0]

        provider_calls = 0

        def discovery_call(operation, identity):
            nonlocal provider_calls
            provider_calls += 1
            return ProviderResult(
                {
                    "items": [
                        {
                            "platform": "xiaohongshu",
                            "platform_content_id": second_id,
                            "canonical_url": second_url,
                            "title": "重放身份冲突内容",
                            "body": "重放身份冲突正文",
                            "content_type": "image",
                        }
                    ],
                    "has_more": False,
                },
                {
                    "code": 200,
                    "data": {
                        "data": {
                            "notes": [
                                {
                                    "note_id": second_id,
                                    "display_title": "重放身份冲突内容",
                                    "desc": "重放身份冲突正文",
                                    "type": "normal",
                                }
                            ],
                            "has_more": 0,
                        }
                    },
                },
                200,
                True,
            )

        with patch(
            "v8.providers.upsert_content",
            side_effect=RuntimeError("simulated interruption before business write"),
        ):
            with self.assertRaisesRegex(RuntimeError, "simulated interruption"):
                discover_account_content(
                    int(account["id"]),
                    "xiaohongshu",
                    "replay-identity-conflict-account",
                    as_of=date(2026, 8, 2),
                    window_key="replay-identity-conflict-page",
                    db_path=self.db,
                    call_override=discovery_call,
                )
        self.assertEqual(provider_calls, 1)
        with connect(self.db) as connection:
            succeeded_slot = connection.execute(
                """
                SELECT * FROM fetch_slots
                WHERE account_id=? AND stage='discovery'
                  AND window_key='replay-identity-conflict-page'
                """,
                (account["id"],),
            ).fetchone()
            raw_before_replay = connection.execute(
                """
                SELECT * FROM provider_raw_responses
                WHERE fetch_attempt_id=(
                    SELECT id FROM fetch_attempts WHERE slot_id=?
                )
                """,
                (succeeded_slot["id"],),
            ).fetchone()
            usage_before_replay = connection.execute(
                "SELECT COUNT(*) count,COALESCE(SUM(amount),0) amount FROM provider_usage"
            ).fetchone()
        self.assertEqual(succeeded_slot["status"], "succeeded")
        self.assertEqual(raw_before_replay["source"], "live")
        self.assertEqual(tuple(usage_before_replay), (1, 0.01))

        def no_provider_call(operation, identity):
            raise AssertionError(f"unexpected provider call: {operation}")

        with self.assertRaisesRegex(
            IdentityConflictError, "identity_conflict"
        ) as raised:
            discover_account_content(
                int(account["id"]),
                "xiaohongshu",
                "replay-identity-conflict-account",
                as_of=date(2026, 8, 2),
                window_key="replay-identity-conflict-page",
                db_path=self.db,
                call_override=no_provider_call,
            )
        self.assertEqual(raised.exception.provider_cost, 0.0)

        with connect(self.db) as connection:
            terminal_slot = connection.execute(
                "SELECT * FROM fetch_slots WHERE id=?", (succeeded_slot["id"],)
            ).fetchone()
            attempt = connection.execute(
                "SELECT * FROM fetch_attempts WHERE slot_id=?",
                (succeeded_slot["id"],),
            ).fetchone()
            raw_after_replay = connection.execute(
                "SELECT * FROM provider_raw_responses WHERE id=?",
                (raw_before_replay["id"],),
            ).fetchone()
            usage_after_replay = connection.execute(
                "SELECT COUNT(*) count,COALESCE(SUM(amount),0) amount FROM provider_usage"
            ).fetchone()
            relations = connection.execute(
                """
                SELECT * FROM duplicate_relations
                WHERE method='identity_conflict' AND status='pending_review'
                """
            ).fetchall()
            review_count_after = connection.execute(
                "SELECT COUNT(*) FROM review_queue"
            ).fetchone()[0]
            evaluation_rows = connection.execute(
                "SELECT id,content_id FROM evaluation_versions WHERE id IN (?,?)",
                tuple(sorted(evaluations)),
            ).fetchall()
            contents_after = connection.execute(
                """
                SELECT id,platform_content_id,canonical_url,title,body,updated_at
                FROM content_items WHERE id IN (?,?) ORDER BY id
                """,
                (first["id"], second["id"]),
            ).fetchall()
            violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        self.assertEqual(terminal_slot["status"], "terminal_failed")
        self.assertEqual(terminal_slot["last_error_code"], "identity_conflict")
        self.assertEqual(terminal_slot["finished_at"], attempt["response_finished_at"])
        self.assertEqual(raw_after_replay["source"], "live")
        self.assertEqual(tuple(usage_after_replay), tuple(usage_before_replay))
        self.assertEqual(len(relations), 1)
        self.assertEqual(review_count_after, review_count_before)
        self.assertEqual({row["id"] for row in evaluation_rows}, evaluations)
        self.assertEqual(
            [tuple(row) for row in contents_after],
            [tuple(row) for row in contents_before],
        )
        self.assertEqual(violations, [])

    def test_discovery_enriches_legacy_detail_slot_without_a_second_paid_call(
        self,
    ) -> None:
        account = upsert_account(
            {
                "phone": "13800138006",
                "platforms": [
                    {
                        "platform": "douyin",
                        "uid": "99887776",
                        "nickname": "存量汽车号",
                    }
                ],
            },
            db_path=self.db,
        )
        content = upsert_content(
            {
                "platform": "douyin",
                "platform_content_id": "987654326",
                "canonical_url": "https://www.douyin.com/video/987654326",
                "account_uid": "99887776",
                "title": "存量内容",
                "published_at": "2026-08-02T01:00:00Z",
                "content_type": "video",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            captured_at = now_utc()
            connection.execute(
                """
                INSERT INTO fetch_slots(
                    content_id,stage,window_key,provider,adapter_version,status,
                    attempt_count,created_at,updated_at
                ) VALUES (?, 'detail', 'lifetime', 'legacy-cache', 'migration-v1',
                          'succeeded', 1, ?, ?)
                """,
                (content["id"], captured_at, captured_at),
            )
            connection.commit()

        def discovery_call(operation, identity):
            if operation == "resolve_account":
                return ProviderResult(
                    {"reference": VALID_SEC_UID},
                    {"data": {"sec_user_id": VALID_SEC_UID}},
                    200,
                    True,
                )
            data = {
                "items": [
                    {
                        "platform": "douyin",
                        "platform_content_id": "987654326",
                        "canonical_url": "https://www.douyin.com/video/987654326",
                        "title": "存量内容",
                        "body": "存量内容完整正文",
                        "published_at": "2026-08-02T01:00:00Z",
                        "content_type": "video",
                        "media_urls": ["https://cdn.example/legacy-video.mp4"],
                        "metrics": {"view_count": 5000},
                    }
                ],
                "next_cursor": None,
                "has_more": False,
            }
            return ProviderResult(data, {"data": {"aweme_list": []}}, 200, True)

        result = discover_account_content(
            int(account["id"]),
            "douyin",
            "99887776",
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=discovery_call,
        )
        self.assertEqual(result["status"], "succeeded", result)
        self.assertEqual(result["provider_cost"], 0.002)
        self.assertEqual(result["derived_stages"]["replayed"], 1)
        self.assertEqual(result["derived_stages"]["created"], 1)
        with connect(self.db) as connection:
            legacy_slot = connection.execute(
                "SELECT provider,status FROM fetch_slots WHERE content_id=? AND stage='detail'",
                (content["id"],),
            ).fetchone()
            media = connection.execute(
                "SELECT 1 FROM evidence_artifacts WHERE content_id=? AND artifact_type='media_source'",
                (content["id"],),
            ).fetchone()
            usage = connection.execute(
                "SELECT COUNT(*) count,SUM(amount) amount FROM provider_usage"
            ).fetchone()
        self.assertEqual(tuple(legacy_slot), ("legacy-cache", "succeeded"))
        self.assertIsNotNone(media)
        self.assertEqual(usage["count"], 2)
        self.assertAlmostEqual(float(usage["amount"]), 0.002)

    def test_successful_discovery_raw_replays_after_business_write_interruption(
        self,
    ) -> None:
        account = upsert_account(
            {
                "phone": "13800138009",
                "platforms": [
                    {
                        "platform": "douyin",
                        "uid": "99887769",
                        "nickname": "恢复号",
                    }
                ],
            },
            db_path=self.db,
        )

        def discovery_call(operation, identity):
            if operation == "resolve_account":
                return ProviderResult(
                    {"reference": VALID_SEC_UID},
                    {"data": {"sec_user_id": VALID_SEC_UID}},
                    200,
                    True,
                )
            return ProviderResult(
                {
                    "items": [
                        {
                            "platform": "douyin",
                            "platform_content_id": "987654329",
                            "canonical_url": "https://www.douyin.com/video/987654329",
                            "title": "中断恢复内容",
                            "body": "中断恢复内容正文",
                            "published_at": "2026-08-02T01:00:00Z",
                            "content_type": "video",
                            "media_urls": ["https://cdn.example/recovered-video.mp4"],
                            "metrics": {
                                "view_count": 2000,
                                "comment_count": 30,
                                "like_count": 90,
                                "share_count": 10,
                                "collect_count": 12,
                            },
                        }
                    ],
                    "next_cursor": 123,
                    "has_more": True,
                },
                {
                    "data": {
                        "aweme_list": [
                            {
                                "aweme_id": "987654329",
                                "desc": "中断恢复内容正文",
                                "create_time": 1785632400,
                                "video": {
                                    "play_addr": {
                                        "url_list": [
                                            "https://cdn.example/recovered-video.mp4",
                                        ]
                                    }
                                },
                                "statistics": {
                                    "play_count": 2000,
                                    "comment_count": 30,
                                    "digg_count": 90,
                                    "share_count": 10,
                                    "collect_count": 12,
                                },
                            }
                        ],
                        "max_cursor": 123,
                        "has_more": 1,
                    },
                },
                200,
                True,
            )

        first = discover_account_content(
            int(account["id"]),
            "douyin",
            "99887769",
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=discovery_call,
        )
        self.assertEqual(first["inserted"], 1)
        with connect(self.db) as connection:
            content = connection.execute(
                "SELECT id FROM content_items WHERE platform_content_id='987654329'"
            ).fetchone()
            usage_before = connection.execute(
                "SELECT COUNT(*) count, SUM(amount) amount FROM provider_usage"
            ).fetchone()
            connection.execute(
                "DELETE FROM evidence_artifacts WHERE content_id=?", (content["id"],)
            )
            connection.execute(
                "DELETE FROM content_metric_snapshots WHERE content_id=?",
                (content["id"],),
            )
            connection.execute(
                "DELETE FROM account_provider_references WHERE account_identity_id=(SELECT id FROM account_platform_identities WHERE account_id=?)",
                (account["id"],),
            )
            connection.execute(
                "UPDATE provider_raw_responses SET source='live' WHERE account_id=?",
                (account["id"],),
            )
            connection.execute(
                "UPDATE provider_raw_responses SET source='live' WHERE content_id=?",
                (content["id"],),
            )
            connection.commit()

        def no_provider_call(operation, identity):
            raise AssertionError(f"unexpected provider call: {operation}")

        replayed = discover_account_content(
            int(account["id"]),
            "douyin",
            "99887769",
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=no_provider_call,
        )
        self.assertEqual(replayed["status"], "already_succeeded", replayed)
        self.assertTrue(replayed["replayed"])
        self.assertEqual(replayed["reference_status"], "replayed")
        self.assertEqual(replayed["inserted"], 0)
        self.assertEqual(replayed["derived_stages"]["replayed"], 2)
        self.assertEqual(replayed["next_cursor"], 123)
        with connect(self.db) as connection:
            usage_after = connection.execute(
                "SELECT COUNT(*) count, SUM(amount) amount FROM provider_usage"
            ).fetchone()
            restored = connection.execute(
                "SELECT 1 FROM content_items WHERE platform_content_id='987654329'"
            ).fetchone()
            restored_slots = connection.execute(
                "SELECT stage,status FROM fetch_slots WHERE content_id=? ORDER BY stage",
                (content["id"],),
            ).fetchall()
            restored_snapshot = connection.execute(
                "SELECT view_count FROM content_metric_snapshots WHERE content_id=?",
                (content["id"],),
            ).fetchone()
            restored_media = connection.execute(
                "SELECT 1 FROM evidence_artifacts WHERE content_id=? AND artifact_type='media_source'",
                (content["id"],),
            ).fetchone()
        self.assertEqual(tuple(usage_after), tuple(usage_before))
        self.assertIsNotNone(restored)
        self.assertEqual(
            [(row["stage"], row["status"]) for row in restored_slots],
            [("detail", "succeeded"), ("metrics", "succeeded")],
        )
        self.assertEqual(restored_snapshot["view_count"], 2000)
        self.assertIsNotNone(restored_media)

    def test_successful_metrics_raw_replays_without_second_paid_call(self) -> None:
        first = update_content_data(
            self.content_id,
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=self.successful_call,
            stages=["metrics"],
            process_media=False,
        )
        self.assertEqual(first["provider_cost"], 0.001)
        with connect(self.db) as connection:
            usage_before = connection.execute(
                "SELECT COUNT(*) count, SUM(amount) amount FROM provider_usage"
            ).fetchone()
            connection.execute("DELETE FROM content_metric_snapshots")
            connection.execute(
                "UPDATE provider_raw_responses SET source='live' WHERE operation='douyin_video_statistics'"
            )
            connection.commit()

        def no_provider_call(stage, content):
            raise AssertionError(f"unexpected provider call: {stage}")

        replayed = update_content_data(
            self.content_id,
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=no_provider_call,
            stages=["metrics"],
            process_media=False,
        )
        self.assertEqual(replayed["provider_cost"], 0)
        self.assertEqual(replayed["stages"][0]["status"], "replayed")
        with connect(self.db) as connection:
            usage_after = connection.execute(
                "SELECT COUNT(*) count, SUM(amount) amount FROM provider_usage"
            ).fetchone()
            snapshot = connection.execute(
                "SELECT view_count FROM content_metric_snapshots"
            ).fetchone()
        self.assertEqual(tuple(usage_after), tuple(usage_before))
        self.assertEqual(snapshot["view_count"], 1000)

    def test_zero_comment_metric_creates_weekly_evidence_without_provider_cost(
        self,
    ) -> None:
        def zero_metric_call(stage, content):
            self.assertEqual(stage, "metrics")
            data = {
                "view_count": 100,
                "comment_count": 0,
                "like_count": 2,
                "share_count": 0,
                "collect_count": 0,
            }
            return ProviderResult(data, {"stage": stage, "data": data}, 200, True)

        metrics = update_content_data(
            self.content_id,
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=zero_metric_call,
            stages=["metrics"],
            process_media=False,
        )
        self.assertEqual(metrics["provider_cost"], 0.001)
        first = materialize_zero_comment_evidence(
            self.content_id, as_of=date(2026, 8, 2), db_path=self.db
        )
        second = materialize_zero_comment_evidence(
            self.content_id, as_of=date(2026, 8, 2), db_path=self.db
        )
        self.assertEqual(first["status"], "succeeded")
        self.assertEqual(second["status"], "already_succeeded")
        with connect(self.db) as connection:
            evidence = connection.execute(
                "SELECT comment_count,status FROM comment_evidence_versions"
            ).fetchone()
            comments = connection.execute("SELECT COUNT(*) FROM comments").fetchone()[0]
            slot = connection.execute(
                "SELECT provider,adapter_version,status FROM fetch_slots WHERE stage='comments'"
            ).fetchone()
            usage = connection.execute(
                "SELECT COUNT(*) count,SUM(amount) amount FROM provider_usage"
            ).fetchone()
        self.assertEqual(tuple(evidence), (0, "available"))
        self.assertEqual(comments, 0)
        self.assertEqual(
            tuple(slot),
            ("TikHub", "tikhub-comments-v8.0+paged-comments-v2", "succeeded"),
        )
        self.assertEqual(usage["count"], 1)
        self.assertAlmostEqual(float(usage["amount"]), 0.001)

    def test_update_comments_uses_paged_run_and_linked_evidence(self) -> None:
        result = update_content_data(
            self.content_id,
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=self.successful_call,
            stages=["comments"],
            process_media=False,
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["stages"][0]["completion_kind"], "provider_exhausted")
        with connect(self.db) as connection:
            run = connection.execute(
                "SELECT id,status,page_count FROM comment_capture_runs"
            ).fetchone()
            page = connection.execute(
                "SELECT capture_run_id FROM comment_capture_pages"
            ).fetchone()
            evidence = connection.execute(
                "SELECT capture_run_id,status FROM comment_evidence_versions"
            ).fetchone()
        self.assertEqual(run["status"], "succeeded")
        self.assertEqual(run["page_count"], 1)
        self.assertEqual(page["capture_run_id"], run["id"])
        self.assertEqual(evidence["capture_run_id"], run["id"])
        self.assertEqual(evidence["status"], "available")

    def test_update_comments_derives_zero_from_metric_without_comment_call(
        self,
    ) -> None:
        calls: list[str] = []

        def metric_only(stage, content):
            calls.append(stage)
            if stage != "metrics":
                raise AssertionError("zero metric must avoid comment provider call")
            data = {
                "view_count": 100,
                "comment_count": 0,
                "like_count": 2,
                "share_count": 0,
                "collect_count": 0,
            }
            return ProviderResult(data, {"stage": stage, "data": data}, 200, True)

        result = update_content_data(
            self.content_id,
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=metric_only,
            stages=["metrics", "comments"],
            process_media=False,
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(calls, ["metrics"])
        comments = next(item for item in result["stages"] if item["stage"] == "comments")
        self.assertEqual(comments["completion_kind"], "zero_comments")
        self.assertEqual(comments["amount"], 0.0)
        with connect(self.db) as connection:
            run = connection.execute(
                "SELECT status,completion_kind FROM comment_capture_runs"
            ).fetchone()
            usage = connection.execute(
                "SELECT COUNT(*) count,SUM(amount) amount FROM provider_usage"
            ).fetchone()
        self.assertEqual(tuple(run), ("succeeded", "zero_comments"))
        self.assertEqual(usage["count"], 1)
        self.assertAlmostEqual(float(usage["amount"]), 0.001)

    def test_xhs_discovery_uses_tikhub_cursor_and_explicit_page_window(self) -> None:
        account = upsert_account(
            {
                "phone": "13800138001",
                "operator_name": "运营乙",
                "platforms": [
                    {
                        "platform": "xiaohongshu",
                        "uid": "xhs-user-1",
                        "nickname": "小红书汽车号",
                    }
                ],
            },
            db_path=self.db,
        )

        def discovery_call(operation, identity):
            self.assertEqual(operation, "discover_content")
            self.assertEqual(identity["cursor"], "page-cursor-1")
            return ProviderResult(
                {
                    "items": [
                        {
                            "platform": "xiaohongshu",
                            "platform_content_id": "64abcdef1234567890abcdef",
                            "canonical_url": "https://www.xiaohongshu.com/explore/64abcdef1234567890abcdef",
                            "title": "小红书新车首发",
                            "body": "小红书新车首发正文",
                            "published_at": "2026-08-02T01:00:00Z",
                            "content_type": "image",
                            "media_urls": ["https://cdn.example/xhs-image.jpg"],
                            "metrics": {
                                "view_count": 3000,
                                "comment_count": 40,
                                "like_count": 120,
                                "share_count": 15,
                                "collect_count": 70,
                            },
                        }
                    ],
                    "next_cursor": "page-cursor-2",
                    "has_more": True,
                },
                {"code": 200, "data": {"success": True}},
                200,
                True,
            )

        result = discover_account_content(
            int(account["id"]),
            "xiaohongshu",
            "xhs-user-1",
            as_of=date(2026, 8, 2),
            cursor="page-cursor-1",
            window_key="2026-08-02:page:page-cursor-1",
            db_path=self.db,
            call_override=discovery_call,
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["provider_cost"], 0.01)
        self.assertEqual(result["derived_stages"]["created"], 2)
        self.assertEqual(result["next_cursor"], "page-cursor-2")
        self.assertTrue(result["has_more"])
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT * FROM fetch_slots WHERE account_id=?", (account["id"],)
            ).fetchone()
            raw = connection.execute(
                "SELECT * FROM provider_raw_responses WHERE account_id=?",
                (account["id"],),
            ).fetchone()
            discovered = connection.execute(
                "SELECT id FROM content_items WHERE platform_content_id='64abcdef1234567890abcdef'"
            ).fetchone()
            content_slots = connection.execute(
                "SELECT stage,status FROM fetch_slots WHERE content_id=? ORDER BY stage",
                (discovered["id"],),
            ).fetchall()
            snapshot = connection.execute(
                "SELECT view_count FROM content_metric_snapshots WHERE content_id=?",
                (discovered["id"],),
            ).fetchone()
            usage = connection.execute(
                "SELECT COUNT(*) count,SUM(amount) amount FROM provider_usage"
            ).fetchone()
        self.assertEqual(slot["provider"], "TikHub")
        self.assertEqual(slot["window_key"], "2026-08-02:page:page-cursor-1")
        self.assertEqual(raw["provider"], "TikHub")
        self.assertEqual(raw["operation"], "xiaohongshu_user_posts")
        self.assertEqual(
            [(row["stage"], row["status"]) for row in content_slots],
            [("detail", "succeeded"), ("metrics", "succeeded")],
        )
        self.assertEqual(snapshot["view_count"], 3000)
        self.assertEqual(usage["count"], 1)
        self.assertAlmostEqual(float(usage["amount"]), 0.01)

    def test_xhs_first_detail_and_metrics_share_one_paid_app_v2_call(self) -> None:
        content = upsert_content(
            {
                "platform": "xiaohongshu",
                "platform_content_id": "65abcdef1234567890abcdef",
                "canonical_url": "https://www.xiaohongshu.com/explore/65abcdef1234567890abcdef",
                "title": "待补详情",
                "content_type": "image",
            },
            db_path=self.db,
        )
        calls: list[str] = []

        def detail_call(stage, current):
            calls.append(stage)
            self.assertEqual(stage, "detail")
            data = {
                "title": "一次调用返回详情和指标",
                "body": "汽车保养完整正文",
                "published_at": "2026-08-01T04:00:00Z",
                "account_uid": "xhs-user",
                "account_name": "小红书汽车号",
                "content_type": "image",
                "media_urls": [],
                "metrics": {
                    "view_count": 1200,
                    "comment_count": 30,
                    "like_count": 80,
                    "share_count": 12,
                    "collect_count": 50,
                },
            }
            return ProviderResult(
                data,
                {"code": 200, "data": {"success": True, "code": 0, "data": data}},
                200,
                True,
            )

        result = update_content_data(
            int(content["id"]),
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=detail_call,
            stages=["detail", "metrics"],
            process_media=False,
        )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(calls, ["detail"])
        self.assertEqual(result["provider_cost"], 0.01)
        self.assertEqual(
            [
                (item["stage"], item["billed"], item["amount"])
                for item in result["stages"]
            ],
            [("detail", True, 0.01), ("metrics", False, 0.0)],
        )
        with connect(self.db) as connection:
            snapshot = connection.execute(
                "SELECT * FROM content_metric_snapshots WHERE content_id=?",
                (content["id"],),
            ).fetchone()
            raws = connection.execute(
                "SELECT * FROM provider_raw_responses WHERE content_id=? ORDER BY id",
                (content["id"],),
            ).fetchall()
            usage = connection.execute(
                "SELECT SUM(amount) amount FROM provider_usage",
            ).fetchone()
        self.assertEqual(snapshot["view_count"], 1200)
        self.assertEqual(
            [row["operation"] for row in raws],
            [
                "xiaohongshu_note_detail",
                "xiaohongshu_note_statistics",
            ],
        )
        self.assertEqual([row["provider"] for row in raws], ["TikHub", "TikHub"])
        self.assertAlmostEqual(float(usage["amount"]), 0.01)

    def test_xhs_metrics_resume_replays_successful_detail_without_second_call(
        self,
    ) -> None:
        content = upsert_content(
            {
                "platform": "xiaohongshu",
                "platform_content_id": "66abcdef1234567890abcdef",
                "canonical_url": "https://www.xiaohongshu.com/explore/66abcdef1234567890abcdef",
                "title": "中断续跑",
                "content_type": "image",
            },
            db_path=self.db,
        )
        calls: list[str] = []

        def detail_call(stage, current):
            calls.append(stage)
            data = {
                "title": "详情先成功",
                "body": "随后在指标派生前中断",
                "published_at": "2026-08-01T04:00:00Z",
                "content_type": "image",
                "media_urls": [],
                "metrics": {
                    "view_count": 321,
                    "comment_count": 12,
                    "like_count": 45,
                    "share_count": 6,
                    "collect_count": 7,
                },
            }
            return ProviderResult(data, {"stage": stage, "data": data}, 200, True)

        first = update_content_data(
            int(content["id"]),
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=detail_call,
            stages=["detail"],
            process_media=False,
        )
        self.assertEqual(first["provider_cost"], 0.01)
        self.assertEqual(calls, ["detail"])

        def no_provider_call(stage, current):
            raise AssertionError(f"unexpected provider call: {stage}")

        resumed = update_content_data(
            int(content["id"]),
            as_of=date(2026, 8, 2),
            db_path=self.db,
            call_override=no_provider_call,
            stages=["detail", "metrics"],
            process_media=False,
        )
        self.assertEqual(resumed["status"], "succeeded")
        self.assertEqual(resumed["provider_cost"], 0.0)
        self.assertEqual(
            [(item["stage"], item["status"]) for item in resumed["stages"]],
            [("detail", "replayed"), ("metrics", "succeeded")],
        )
        with connect(self.db) as connection:
            snapshot = connection.execute(
                "SELECT view_count FROM content_metric_snapshots WHERE content_id=?",
                (content["id"],),
            ).fetchone()
            usage = connection.execute(
                "SELECT COUNT(*) count,SUM(amount) amount FROM provider_usage"
            ).fetchone()
        self.assertEqual(snapshot["view_count"], 321)
        self.assertEqual(usage["count"], 1)
        self.assertAlmostEqual(float(usage["amount"]), 0.01)

    def test_detail_persists_media_source_and_update_data_runs_local_media(
        self,
    ) -> None:
        def detail_call(stage, content):
            self.assertEqual(stage, "detail")
            data = {
                "title": "汽车视频",
                "body": "汽车视频正文",
                "published_at": "2026-08-01T04:00:00Z",
                "account_uid": "99887766",
                "account_name": "汽车号",
                "content_type": "video",
                "media_urls": [
                    "http://sns-v11.rednotecdn.com/stream/video.mp4",
                ],
            }
            return ProviderResult(data, {"stage": stage, "data": data}, 200, True)

        with (
            patch("v8.media.MEDIA_ROOT", self.root / "media"),
            patch(
                "v8.providers.process_content_media",
                return_value={
                    "content_id": self.content_id,
                    "status": "evidence_ready",
                },
            ) as process_media,
        ):
            result = update_content_data(
                self.content_id,
                db_path=self.db,
                call_override=detail_call,
                stages=["detail"],
            )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["media"]["status"], "evidence_ready")
        self.assertIsNotNone(result["duplicates"])
        process_media.assert_called_once_with(self.content_id, db_path=self.db)
        with connect(self.db) as connection:
            source = connection.execute(
                "SELECT * FROM evidence_artifacts WHERE artifact_type='media_source'"
            ).fetchone()
            pending = connection.execute(
                "SELECT * FROM pending_platform_identities WHERE uid='99887766'"
            ).fetchone()
        self.assertIsNotNone(source)
        self.assertEqual(pending["content_count"], 1)
        self.assertEqual(pending["nickname"], "汽车号")
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM duplicate_fingerprints"
                ).fetchone()[0],
                1,
            )
        manifest = Path(str(source["local_path"]))
        manifest_value = json.loads(manifest.read_text(encoding="utf-8"))
        self.assertEqual(manifest_value["media_kind"], "video")
        self.assertEqual(
            manifest_value["urls"],
            ["http://sns-v11.rednotecdn.com/stream/video.mp4"],
        )

    def test_detail_immediately_links_a_claimed_provider_uid(self) -> None:
        account = upsert_account(
            {
                "phone": "13800138003",
                "platforms": [{"platform": "douyin", "uid": "99887767"}],
            },
            db_path=self.db,
        )

        def detail_call(stage, content):
            self.assertEqual(stage, "detail")
            data = {
                "title": "已认领账号内容",
                "account_uid": "99887767",
                "account_name": "已认领汽车号",
                "content_type": "video",
                "media_urls": ["https://cdn.example/claimed.mp4"],
            }
            return ProviderResult(data, {"stage": stage, "data": data}, 200, True)

        with patch(
            "v8.providers.process_content_media",
            return_value={"content_id": self.content_id, "status": "evidence_ready"},
        ):
            result = update_content_data(
                self.content_id,
                db_path=self.db,
                call_override=detail_call,
                stages=["detail"],
            )
        self.assertEqual(result["status"], "succeeded")
        with connect(self.db) as connection:
            content = connection.execute(
                "SELECT account_id,raw_account_uid FROM content_items WHERE id=?",
                (self.content_id,),
            ).fetchone()
            pending = connection.execute(
                "SELECT COUNT(*) FROM pending_platform_identities WHERE uid='99887767'"
            ).fetchone()[0]
        self.assertEqual(content["account_id"], account["id"])
        self.assertEqual(content["raw_account_uid"], "99887767")
        self.assertEqual(pending, 0)

    def _store_media_source(
        self,
        url: str,
        *,
        raw_response_id: int = 100,
    ) -> dict[str, object]:
        artifact = media_module.store_media_source_manifest(
            self.content_id,
            media_kind="video",
            urls=[url],
            raw_response_id=raw_response_id,
            db_path=self.db,
        )
        self.assertIsNotNone(artifact)
        state = media_module.get_media_source_state(
            self.content_id,
            db_path=self.db,
        )
        self.assertIsNotNone(state)
        return state

    def _insert_download_slot(
        self,
        source_sha256: str,
        *,
        status: str,
        attempt_count: int,
    ) -> None:
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO media_processing_slots(
                    content_id,source_sha256,processor_type,processor_version,
                    status,attempt_count,created_at,updated_at
                ) VALUES (?,?,'download','provider-media-download-v8.1',?,?,?,?)
                """,
                (
                    self.content_id,
                    source_sha256,
                    status,
                    attempt_count,
                    captured_at,
                    captured_at,
                ),
            )
            connection.commit()

    def _paid_refresh_result(self, media_url: str) -> ProviderResult:
        data = {
            "title": "媒体补证详情",
            "body": "汽车媒体补证正文",
            "published_at": "2026-08-01T04:00:00Z",
            "content_type": "video",
            "media_urls": [media_url],
        }
        return ProviderResult(data, {"data": data}, 200, True)

    def test_paid_media_retry_recovers_real_expired_download_then_refreshes_once(
        self,
    ) -> None:
        self._store_media_source("https://cdn.example/expired.mp4")
        process_calls = 0

        def process_media(content_id, *, db_path):
            nonlocal process_calls
            process_calls += 1
            if process_calls == 1:
                return media_module.process_content_media(content_id, db_path=db_path)
            return {"content_id": content_id, "status": "evidence_ready"}

        expired = HTTPError(
            "https://cdn.example/expired.mp4",
            403,
            "expired",
            {},
            BytesIO(b"expired"),
        )
        with (
            patch("v8.media.urllib.request.urlopen", side_effect=expired),
            patch("v8.providers.process_content_media", side_effect=process_media),
            patch(
                "v8.providers.evaluate_content",
                return_value=SimpleNamespace(evaluation_id=7, created=True),
            ),
            patch(
                "v8.providers.refresh_content_duplicates", return_value={"status": "ok"}
            ),
        ):
            result = retry_content_media(
                self.content_id,
                allow_paid_refresh=True,
                db_path=self.db,
                call_override=lambda stage, content: self._paid_refresh_result(
                    "https://cdn.example/refreshed.mp4"
                ),
            )

        self.assertEqual(process_calls, 2)
        self.assertEqual(result["status"], "evidence_ready")
        self.assertEqual(result["provider_cost"], 0.001)
        self.assertEqual(result["media_source_refresh"]["status"], "succeeded")
        self.assertEqual(result["media_source_refresh"]["billed"], True)
        self.assertIn("stale_recovery", result)
        with connect(self.db) as connection:
            old_download = connection.execute(
                """
                SELECT status,attempt_count FROM media_processing_slots
                WHERE processor_type='download' ORDER BY id LIMIT 1
                """
            ).fetchone()
            usage = connection.execute(
                "SELECT COUNT(*) count,SUM(amount) amount FROM provider_usage"
            ).fetchone()
        self.assertEqual(old_download["status"], "retryable_failed")
        self.assertEqual(old_download["attempt_count"], 1)
        self.assertEqual(usage["count"], 1)
        self.assertAlmostEqual(float(usage["amount"]), 0.001)

    def test_retry_content_media_scopes_stale_recovery_to_target_content(
        self,
    ) -> None:
        recovery = {
            "stale_candidates": 0,
            "recovered": 0,
            "retryable_failed": 0,
            "terminal_failed": 0,
            "cas_conflicts": 0,
            "exhausted_normalized": 0,
        }
        with (
            patch(
                "v8.providers.recover_stale_media_processing_slots",
                return_value=recovery,
            ) as recover,
            patch(
                "v8.providers.process_content_media",
                return_value={
                    "content_id": self.content_id,
                    "status": "evidence_ready",
                },
            ),
        ):
            result = retry_content_media(self.content_id, db_path=self.db)

        recover.assert_called_once_with(
            db_path=self.db,
            processor_version_by_type=media_module.processor_versions(),
            content_ids=(self.content_id,),
        )
        self.assertEqual(result["stale_recovery"], recovery)

    def test_expired_download_without_paid_authorization_never_calls_provider(
        self,
    ) -> None:
        self._store_media_source("https://cdn.example/expired-no-budget.mp4")
        expired = HTTPError(
            "https://cdn.example/expired-no-budget.mp4",
            403,
            "expired",
            {},
            BytesIO(b"expired"),
        )
        with (
            patch("v8.media.urllib.request.urlopen", side_effect=expired),
            self.assertRaisesRegex(ProviderConfigurationError, "付费刷新"),
        ):
            retry_content_media(
                self.content_id,
                allow_paid_refresh=False,
                db_path=self.db,
            )
        with connect(self.db) as connection:
            download = connection.execute(
                """
                SELECT status,attempt_count FROM media_processing_slots
                WHERE content_id=? AND processor_type='download'
                """,
                (self.content_id,),
            ).fetchone()
            usage_count = connection.execute(
                "SELECT COUNT(*) FROM provider_usage"
            ).fetchone()[0]
        self.assertEqual(download["status"], "retryable_failed")
        self.assertEqual(download["attempt_count"], 1)
        self.assertEqual(usage_count, 0)

    def test_terminal_media_source_requires_explicit_paid_authorization(self) -> None:
        state = self._store_media_source("https://cdn.example/terminal.mp4")
        self._insert_download_slot(
            str(state["source_sha256"]),
            status="terminal_failed",
            attempt_count=3,
        )
        with (
            patch("v8.providers.process_content_media") as process_media,
            self.assertRaisesRegex(ProviderConfigurationError, "付费刷新"),
        ):
            retry_content_media(
                self.content_id,
                allow_paid_refresh=False,
                db_path=self.db,
            )
        process_media.assert_not_called()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0],
                0,
            )

    def test_stale_terminal_download_is_recovered_before_retry_decision(self) -> None:
        state = self._store_media_source("https://cdn.example/stale-terminal.mp4")
        self._insert_download_slot(
            str(state["source_sha256"]),
            status="running",
            attempt_count=3,
        )
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE media_processing_slots
                SET updated_at='2000-01-01T00:00:00Z'
                WHERE content_id=? AND processor_type='download'
                """,
                (self.content_id,),
            )
            connection.commit()
        with (
            patch("v8.providers.process_content_media") as process_media,
            self.assertRaisesRegex(ProviderConfigurationError, "付费刷新"),
        ):
            retry_content_media(
                self.content_id,
                allow_paid_refresh=False,
                db_path=self.db,
            )
        process_media.assert_not_called()
        with connect(self.db) as connection:
            slot = connection.execute(
                """
                SELECT status FROM media_processing_slots
                WHERE content_id=? AND processor_type='download'
                """,
                (self.content_id,),
            ).fetchone()
        self.assertEqual(slot["status"], "terminal_failed")

    def test_terminal_media_source_skips_old_download_when_paid_refresh_is_authorized(
        self,
    ) -> None:
        state = self._store_media_source("https://cdn.example/terminal.mp4")
        self._insert_download_slot(
            str(state["source_sha256"]),
            status="terminal_failed",
            attempt_count=3,
        )
        provider_calls = 0

        def detail_call(stage, content):
            nonlocal provider_calls
            provider_calls += 1
            self.assertEqual(stage, "detail")
            return self._paid_refresh_result("https://cdn.example/refreshed.mp4")

        with (
            patch(
                "v8.providers.process_content_media",
                return_value={
                    "content_id": self.content_id,
                    "status": "evidence_ready",
                },
            ) as process_media,
            patch(
                "v8.providers.evaluate_content",
                return_value=SimpleNamespace(evaluation_id=8, created=True),
            ),
            patch(
                "v8.providers.refresh_content_duplicates", return_value={"status": "ok"}
            ),
        ):
            result = retry_content_media(
                self.content_id,
                allow_paid_refresh=True,
                db_path=self.db,
                call_override=detail_call,
            )

        self.assertEqual(provider_calls, 1)
        process_media.assert_called_once_with(self.content_id, db_path=self.db)
        self.assertNotEqual(
            result["media_source_refresh"]["previous_source_sha256"],
            result["media_source_refresh"]["source_sha256"],
        )

    def test_downstream_media_failures_never_trigger_paid_refresh(self) -> None:
        state = self._store_media_source("https://cdn.example/downloaded.mp4")
        self._insert_download_slot(
            str(state["source_sha256"]),
            status="succeeded",
            attempt_count=1,
        )
        for processor in ("frames", "asr", "ocr"):
            provider_calls = 0

            def detail_call(stage, content):
                nonlocal provider_calls
                provider_calls += 1
                return self._paid_refresh_result(
                    "https://cdn.example/should-not-run.mp4"
                )

            with (
                self.subTest(processor=processor),
                patch(
                    "v8.providers.process_content_media",
                    side_effect=media_module.MediaProcessingError(
                        f"{processor} processing failed"
                    ),
                ),
                self.assertRaisesRegex(
                    media_module.MediaProcessingError,
                    f"{processor} processing failed",
                ),
            ):
                retry_content_media(
                    self.content_id,
                    allow_paid_refresh=True,
                    db_path=self.db,
                    call_override=detail_call,
                )
            self.assertEqual(provider_calls, 0)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0],
                0,
            )

    def test_missing_media_source_can_use_one_explicit_paid_refresh(self) -> None:
        with (
            patch(
                "v8.providers.process_content_media",
                side_effect=[
                    {"content_id": self.content_id, "status": "no_source"},
                    {"content_id": self.content_id, "status": "evidence_ready"},
                ],
            ),
            patch(
                "v8.providers.evaluate_content",
                return_value=SimpleNamespace(evaluation_id=9, created=True),
            ),
            patch(
                "v8.providers.refresh_content_duplicates", return_value={"status": "ok"}
            ),
        ):
            result = retry_content_media(
                self.content_id,
                allow_paid_refresh=True,
                db_path=self.db,
                call_override=lambda stage, content: self._paid_refresh_result(
                    "https://cdn.example/first-source.mp4"
                ),
            )

        self.assertEqual(result["status"], "evidence_ready")
        self.assertIsNone(result["media_source_refresh"]["previous_source_sha256"])
        self.assertEqual(result["media_source_refresh"]["status"], "succeeded")

    def test_paid_refresh_rejects_missing_or_invalid_refreshed_media_source(
        self,
    ) -> None:
        state = self._store_media_source("https://cdn.example/terminal.mp4")
        self._insert_download_slot(
            str(state["source_sha256"]),
            status="terminal_failed",
            attempt_count=3,
        )
        for media_urls in ([], ["http://unsupported.example/new.mp4"]):
            with (
                self.subTest(media_urls=media_urls),
                patch("v8.providers.process_content_media") as process_media,
                self.assertRaisesRegex(
                    ProviderConfigurationError,
                    "媒体源",
                ),
            ):
                retry_content_media(
                    self.content_id,
                    allow_paid_refresh=True,
                    db_path=self.db,
                    call_override=lambda stage,
                    content,
                    urls=media_urls: ProviderResult(
                        {
                            "content_type": "video",
                            "media_urls": urls,
                        },
                        {"data": {"media_urls": urls}},
                        200,
                        True,
                    ),
                )
            process_media.assert_not_called()
            with connect(self.db) as connection:
                connection.execute("DELETE FROM fetch_attempts")
                connection.execute(
                    "DELETE FROM fetch_slots WHERE stage='media_source_refresh'"
                )
                connection.execute("DELETE FROM provider_usage")
                connection.execute("DELETE FROM provider_budget_batches")
                connection.commit()

    def test_paid_refresh_rejects_same_terminal_source_sha_without_reusing_it(
        self,
    ) -> None:
        old_url = "https://cdn.example/same-terminal.mp4"
        state = self._store_media_source(old_url)
        self._insert_download_slot(
            str(state["source_sha256"]),
            status="terminal_failed",
            attempt_count=3,
        )
        with (
            patch("v8.providers.process_content_media") as process_media,
            self.assertRaisesRegex(ProviderConfigurationError, "未提供新媒体源"),
        ):
            retry_content_media(
                self.content_id,
                allow_paid_refresh=True,
                db_path=self.db,
                call_override=lambda stage, content: self._paid_refresh_result(old_url),
            )
        process_media.assert_not_called()
        with connect(self.db) as connection:
            usage = connection.execute(
                "SELECT COUNT(*) count,SUM(amount) amount FROM provider_usage"
            ).fetchone()
        self.assertEqual(usage["count"], 1)
        self.assertAlmostEqual(float(usage["amount"]), 0.001)

    def test_succeeded_lifetime_refresh_slot_prevents_second_paid_call(self) -> None:
        old_state = self._store_media_source("https://cdn.example/terminal.mp4")
        self._insert_download_slot(
            str(old_state["source_sha256"]),
            status="terminal_failed",
            attempt_count=3,
        )
        provider_calls = 0

        def detail_call(stage, content):
            nonlocal provider_calls
            provider_calls += 1
            return self._paid_refresh_result("https://cdn.example/refreshed-once.mp4")

        with (
            patch(
                "v8.providers.process_content_media",
                return_value={
                    "content_id": self.content_id,
                    "status": "evidence_ready",
                },
            ),
            patch(
                "v8.providers.evaluate_content",
                return_value=SimpleNamespace(evaluation_id=10, created=True),
            ),
            patch(
                "v8.providers.refresh_content_duplicates", return_value={"status": "ok"}
            ),
        ):
            first = retry_content_media(
                self.content_id,
                allow_paid_refresh=True,
                db_path=self.db,
                call_override=detail_call,
            )
        refreshed_state = media_module.get_media_source_state(
            self.content_id,
            db_path=self.db,
        )
        self.assertIsNotNone(refreshed_state)
        self._insert_download_slot(
            str(refreshed_state["source_sha256"]),
            status="terminal_failed",
            attempt_count=3,
        )
        with (
            patch("v8.providers.process_content_media") as process_media,
            self.assertRaisesRegex(SlotUnavailable, "succeeded"),
        ):
            retry_content_media(
                self.content_id,
                allow_paid_refresh=True,
                db_path=self.db,
                call_override=detail_call,
            )
        process_media.assert_not_called()
        self.assertEqual(first["provider_cost"], 0.001)
        self.assertEqual(provider_calls, 1)
        with connect(self.db) as connection:
            usage = connection.execute(
                "SELECT COUNT(*) count,SUM(amount) amount FROM provider_usage"
            ).fetchone()
        self.assertEqual(usage["count"], 1)
        self.assertAlmostEqual(float(usage["amount"]), 0.001)


if __name__ == "__main__":
    unittest.main()
