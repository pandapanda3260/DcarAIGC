from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from typing import Mapping
from unittest.mock import patch
from zoneinfo import ZoneInfo

import httpx

import v8.capture as capture_module
from v8.douyin_openapi_client import (
    MACHINE_API_ORIGIN,
    DouyinMachineAPIError,
    DouyinMachineClient,
    load_douyin_sync_config,
)
from v8.douyin_openapi_sync import reconcile_with_client
from v8.scheduler import _select_due_capture_contents, prepare_due_capture_slots
from v8.storage import connect, initialize_database, now_utc


MACHINE_KEY = "machine-key-canary-value-12345678"
AUTHORIZATION_ID = "1" * 32
SHANGHAI = ZoneInfo("Asia/Shanghai")
SCHEDULED_FOR = datetime(2026, 8, 23, 2, 0, tzinfo=SHANGHAI)


def authorization(*, account_id: int = 1, uid: str = "123456789") -> dict:
    return {
        "authorization_id": AUTHORIZATION_ID,
        "account_id": account_id,
        "platform_uid": uid,
        "access_expires_at": 1_800_000_000,
        "refresh_expires_at": 1_800_086_400,
        "renew_count": 0,
        "scopes": ["user_info", "video.list"],
        "needs_reauthorization": False,
        "updated_at": 1_700_000_000,
    }


def video(
    video_id: str,
    created_at: int,
    *,
    is_top: bool = False,
    play_count: int | None = 10,
    media_type: int = 4,
) -> dict:
    return {
        "video_id": video_id,
        "title": f"作品 {video_id}",
        "create_time": created_at,
        "is_top": is_top,
        "is_reviewed": True,
        "video_status": 1,
        "share_url": f"https://www.douyin.com/video/{video_id}",
        "item_id": f"opaque-{video_id}",
        "media_type": media_type,
        "cover": "https://example.invalid/cover.jpg",
        "statistics": {
            "forward_count": 1,
            "comment_count": 2,
            "digg_count": 3,
            "download_count": 4,
            "play_count": play_count,
            "share_count": 5,
        },
    }


class FakeMachineClient:
    def __init__(self, authorizations: list[dict], pages: dict[int, dict]) -> None:
        self.authorizations = authorizations
        self.pages = pages
        self.requested_cursors: list[int] = []

    def list_authorizations(self) -> list[dict]:
        return list(self.authorizations)

    def video_list_page(
        self, *, authorization_id: str, cursor: int, count: int
    ) -> dict:
        if authorization_id != AUTHORIZATION_ID or count != 20:
            raise AssertionError("unexpected machine request")
        self.requested_cursors.append(cursor)
        return dict(self.pages[cursor])


class DouyinMachineClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.key = self.root / "machine.key"
        self.key.write_text(MACHINE_KEY + "\n", encoding="utf-8")
        self.key.chmod(0o600)
        self.env = self.root / "douyin-sync.env"
        self._write_env()

    def _write_env(self, extra: str = "") -> None:
        self.env.write_text(
            "DCAR_DOUYIN_SSH_ALIAS=dcar-douyin-sync-prod\n"
            "DCAR_DOUYIN_LOCAL_PORT=14175\n"
            f"DCAR_DOUYIN_MACHINE_KEY_FILE={self.key}\n"
            + extra,
            encoding="utf-8",
        )
        self.env.chmod(0o600)

    def test_strict_environment_allowlist_and_permissions(self) -> None:
        config = load_douyin_sync_config(self.env)
        self.assertEqual(config.local_port, 14175)
        self.assertEqual(config.machine_key_path, self.key)

        self._write_env("UNEXPECTED=value\n")
        with self.assertRaisesRegex(ValueError, "unsupported"):
            load_douyin_sync_config(self.env)
        self._write_env()
        self.env.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "0400 or 0600"):
            load_douyin_sync_config(self.env)

        self.env.chmod(0o600)
        self.key.write_text("x" * 31 + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "credential is invalid"):
            DouyinMachineClient(load_douyin_sync_config(self.env))

    def test_fixed_target_machine_header_and_strict_projection(self) -> None:
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(
                200,
                headers={"content-type": "application/json"},
                json={"items": [authorization()]},
            )

        client = DouyinMachineClient(
            load_douyin_sync_config(self.env),
            transport=httpx.MockTransport(handler),
        )
        self.addCleanup(client.close)
        self.assertEqual(client.list_authorizations(), [authorization()])
        self.assertEqual(str(requests[0].url), MACHINE_API_ORIGIN + "/internal/v1/authorizations")
        self.assertEqual(requests[0].headers["x-dcar-machine-key"], MACHINE_KEY)
        self.assertNotIn(MACHINE_KEY, repr(client.__dict__))

        def invalid_handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                headers={"content-type": "application/json"},
                json={"items": [{**authorization(), "access_token": "forbidden"}]},
            )

        invalid = DouyinMachineClient(
            load_douyin_sync_config(self.env),
            transport=httpx.MockTransport(invalid_handler),
        )
        self.addCleanup(invalid.close)
        with self.assertRaisesRegex(
            DouyinMachineAPIError, "invalid_authorization_projection"
        ):
            invalid.list_authorizations()

    def test_video_page_validation_redirect_and_response_limit(self) -> None:
        recent = int(SCHEDULED_FOR.timestamp()) - 60

        def success(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(
                json.loads(request.content),
                {"authorization_id": AUTHORIZATION_ID, "cursor": 0, "count": 20},
            )
            return httpx.Response(
                200,
                headers={"content-type": "application/json"},
                json={
                    "captured_at": recent,
                    "cursor": 20,
                    "has_more": False,
                    "items": [video("123456789012", recent, play_count=0)],
                },
            )

        client = DouyinMachineClient(
            load_douyin_sync_config(self.env), transport=httpx.MockTransport(success)
        )
        self.addCleanup(client.close)
        page = client.video_list_page(
            authorization_id=AUTHORIZATION_ID, cursor=0, count=20
        )
        self.assertEqual(page["items"][0]["statistics"]["play_count"], 0)

        for response, error in (
            (
                httpx.Response(
                    307,
                    headers={
                        "content-type": "application/json",
                        "location": "http://elsewhere.invalid/",
                    },
                    json={"detail": "redirected"},
                ),
                "machine_api_redirected",
            ),
            (
                httpx.Response(
                    200,
                    headers={
                        "content-type": "application/json",
                        "content-length": str(2 * 1024 * 1024 + 1),
                    },
                    content=b"{}",
                ),
                "machine_response_too_large",
            ),
        ):
            with self.subTest(error=error):
                failing = DouyinMachineClient(
                    load_douyin_sync_config(self.env),
                    transport=httpx.MockTransport(lambda _request, r=response: r),
                )
                self.addCleanup(failing.close)
                with self.assertRaisesRegex(DouyinMachineAPIError, error):
                    failing.list_authorizations()


class DouyinOpenAPIReconcileTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / "writer.sqlite3"
        self.raw = self.root / "raw"
        self.raw_patch = patch.object(capture_module, "RAW_ROOT", self.raw)
        self.raw_patch.start()
        self.addCleanup(self.raw_patch.stop)
        with connect(self.db) as connection:
            initialize_database(connection)
            captured_at = now_utc()
            connection.execute(
                """
                INSERT INTO accounts(
                    id,phone,phone_normalized,operator_name,enabled,created_at,updated_at
                ) VALUES (1,'13800000000','13800000000','operator',1,?,?)
                """,
                (captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO account_platform_identities(
                    account_id,platform,uid,nickname,created_at,updated_at
                ) VALUES (1,'douyin','123456789','账号',?,?)
                """,
                (captured_at, captured_at),
            )

    def test_window_pagination_top_rule_raw_first_and_authoritative_zero(self) -> None:
        coverage_start = int(
            datetime(2026, 8, 10, 0, 0, tzinfo=SHANGHAI).timestamp()
        )
        recent_one = int(datetime(2026, 8, 20, 12, tzinfo=SHANGHAI).timestamp())
        recent_two = int(datetime(2026, 8, 18, 12, tzinfo=SHANGHAI).timestamp())
        client = FakeMachineClient(
            [authorization()],
            {
                0: {
                    "captured_at": int(SCHEDULED_FOR.timestamp()),
                    "cursor": 20,
                    "has_more": True,
                    "items": [
                        video("900000000001", coverage_start - 100, is_top=True),
                        video("900000000002", recent_one, play_count=0),
                    ],
                },
                20: {
                    "captured_at": int(SCHEDULED_FOR.timestamp()),
                    "cursor": 40,
                    "has_more": True,
                    "items": [
                        video("900000000003", recent_two, media_type=2),
                        video("900000000004", coverage_start - 1, is_top=False),
                    ],
                },
            },
        )

        result = reconcile_with_client(
            scheduled_for=SCHEDULED_FOR,
            db_path=self.db,
            client=client,  # type: ignore[arg-type]
            raw_root=self.raw,
        )

        self.assertEqual(client.requested_cursors, [0, 20])
        self.assertEqual(
            result["window_start"], "2026-08-09T16:00:00Z"
        )
        account = result["accounts"][0]
        self.assertEqual(
            account,
            {
                "account_id": 1,
                "platform_uid": "123456789",
                "status": "succeeded",
                "coverage_start": "2026-08-09T16:00:00Z",
                "coverage_end": "2026-08-22T18:00:00Z",
                "coverage_complete": True,
                "pagination_complete": True,
                "materialization_complete": True,
                "pages_fetched": 2,
                "items_discovered": 2,
            },
        )
        serialized = json.dumps(result)
        self.assertNotIn(AUTHORIZATION_ID, serialized)
        self.assertNotIn("open_id", serialized)
        self.assertNotIn("token", serialized)
        daily_capture_at = datetime(2026, 8, 23, 2, 0, tzinfo=SHANGHAI)
        prepare_due_capture_slots(daily_capture_at, db_path=self.db)
        due_contents = _select_due_capture_contents(
            daily_capture_at,
            db_path=self.db,
            content_limit=100,
        )
        self.assertEqual(len(due_contents), 2)
        self.assertTrue(all(item["metrics_needed"] is False for item in due_contents))
        with connect(self.db) as connection:
            contents = connection.execute(
                """
                SELECT platform_content_id,canonical_url,content_type
                FROM content_items ORDER BY platform_content_id
                """
            ).fetchall()
            raw_rows = connection.execute(
                "SELECT provider,operation,source FROM provider_raw_responses ORDER BY id"
            ).fetchall()
            metric = connection.execute(
                """
                SELECT c.platform_content_id,o.view_count,o.status
                FROM content_metric_observations o
                JOIN content_items c ON c.id=o.content_id
                WHERE c.platform_content_id='900000000002'
                """
            ).fetchone()
        self.assertEqual(
            [tuple(row) for row in contents],
            [
                (
                    "900000000002",
                    "https://www.douyin.com/video/900000000002",
                    "video",
                ),
                (
                    "900000000003",
                    "https://www.douyin.com/video/900000000003",
                    "image",
                ),
            ],
        )
        self.assertGreaterEqual(len(raw_rows), 4)
        self.assertTrue(all(row["provider"] == "DouyinOpenAPI" for row in raw_rows))
        self.assertEqual(tuple(metric), ("900000000002", 0, "available"))

    def test_identity_mismatch_fails_without_calling_provider(self) -> None:
        client = FakeMachineClient(
            [authorization(uid="999999999")],
            {},
        )
        result = reconcile_with_client(
            scheduled_for=SCHEDULED_FOR,
            db_path=self.db,
            client=client,  # type: ignore[arg-type]
            raw_root=self.raw,
        )
        self.assertEqual(client.requested_cursors, [])
        self.assertEqual(result["accounts"][0]["status"], "failed")
        self.assertEqual(
            result["accounts"][0]["error_code"],
            "authorization_identity_mismatch",
        )

    def test_materialization_failure_keeps_raw_and_reopens_slot(self) -> None:
        recent = int(datetime(2026, 8, 20, 12, tzinfo=SHANGHAI).timestamp())
        client = FakeMachineClient(
            [authorization()],
            {
                0: {
                    "captured_at": recent,
                    "cursor": 0,
                    "has_more": False,
                    "items": [video("900000000010", recent)],
                }
            },
        )

        def fail_materialization(**_kwargs: object) -> Mapping[str, object]:
            raise RuntimeError("canary materialization failure")

        result = reconcile_with_client(
            scheduled_for=SCHEDULED_FOR,
            db_path=self.db,
            client=client,  # type: ignore[arg-type]
            raw_root=self.raw,
            materialize_page=fail_materialization,
        )
        account = result["accounts"][0]
        self.assertEqual(account["status"], "failed")
        self.assertEqual(account["error_code"], "derived_materialization_failed")
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT status,last_error_code FROM fetch_slots"
            ).fetchone()
            raw_count = connection.execute(
                "SELECT COUNT(*) FROM provider_raw_responses"
            ).fetchone()[0]
        self.assertEqual(tuple(slot), ("retryable_failed", "derived_materialization_failed"))
        self.assertEqual(raw_count, 1)

    def test_cursor_loop_is_partial_after_first_materialized_page(self) -> None:
        recent = int(datetime(2026, 8, 20, 12, tzinfo=SHANGHAI).timestamp())
        client = FakeMachineClient(
            [authorization()],
            {
                0: {
                    "captured_at": recent,
                    "cursor": 20,
                    "has_more": True,
                    "items": [video("900000000020", recent)],
                },
                20: {
                    "captured_at": recent,
                    "cursor": 20,
                    "has_more": True,
                    "items": [video("900000000021", recent)],
                },
            },
        )
        result = reconcile_with_client(
            scheduled_for=SCHEDULED_FOR,
            db_path=self.db,
            client=client,  # type: ignore[arg-type]
            raw_root=self.raw,
        )
        account = result["accounts"][0]
        self.assertEqual(account["status"], "partial")
        self.assertEqual(account["error_code"], "pagination_cursor_loop")
        self.assertFalse(account["pagination_complete"])
        with connect(self.db) as connection:
            slots = connection.execute(
                "SELECT status,last_error_code FROM fetch_slots ORDER BY id"
            ).fetchall()
        self.assertEqual(
            [tuple(row) for row in slots],
            [
                ("succeeded", None),
                ("succeeded", None),
                ("retryable_failed", "pagination_cursor_loop"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
