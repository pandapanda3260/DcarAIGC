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

    def _assert_no_content_side_effects(self, client: FakeMachineClient) -> None:
        self.assertEqual(client.requested_cursors, [])
        self.assertFalse(self.raw.exists())
        with connect(self.db) as connection:
            for table in (
                "content_items", "provider_raw_responses", "content_metric_observations",
                "content_metric_snapshots", "fetch_slots", "fetch_attempts", "provider_usage",
            ):
                self.assertEqual(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0, table)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM account_platform_identities").fetchone()[0], 1)

    def test_retired_window_pagination_and_authoritative_zero_cannot_write(self) -> None:
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

        self._assert_no_content_side_effects(client)
        self.assertEqual(result["contract_version"], "authorization-status-v1")
        self.assertEqual(result["authorization_state"], "available")
        self.assertFalse(result["content_sync_enabled"])
        self.assertNotIn("window_start", result)
        account = result["accounts"][0]
        self.assertEqual(
            account,
            {
                "authorization_id": AUTHORIZATION_ID,
                "account_id": 1,
                "platform_uid": "123456789",
                "status": "available",
                "identity_matches": True,
                "needs_reauthorization": False,
                "access_expires_at": 1_800_000_000,
                "refresh_expires_at": 1_800_086_400,
                "reason": "",
            },
        )
        serialized = json.dumps(result)
        self.assertIn(AUTHORIZATION_ID, serialized)
        self.assertNotIn("open_id", serialized)
        self.assertNotIn("token", serialized)

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
        self._assert_no_content_side_effects(client)
        self.assertEqual(result["accounts"][0]["status"], "attention")
        self.assertFalse(result["accounts"][0]["identity_matches"])
        self.assertEqual(
            result["accounts"][0]["reason"],
            "authorization_identity_mismatch",
        )

    def test_actual_health_time_is_not_the_scheduled_time_or_video_scope(self) -> None:
        current = {**authorization(), "scopes": ["user_info"]}
        client = FakeMachineClient([current], {})
        checked_at = "2026-08-29T01:00:00Z"
        with patch("v8.douyin_openapi_sync.now_utc", return_value=checked_at) as clock:
            result = reconcile_with_client(
                scheduled_for=SCHEDULED_FOR, db_path=self.db,
                client=client,  # type: ignore[arg-type]
            )
        clock.assert_called_once()
        self.assertEqual(result["captured_at"], checked_at)
        self.assertNotEqual(datetime.fromisoformat(checked_at.replace("Z", "+00:00")), SCHEDULED_FOR)
        self.assertEqual(result["authorization_state"], "available")
        self.assertEqual(result["accounts"][0]["status"], "available")
        self._assert_no_content_side_effects(client)

    def test_paused_account_keeps_its_authorization_identity(self) -> None:
        with connect(self.db) as connection:
            connection.execute("UPDATE accounts SET enabled=0 WHERE id=1")
        client = FakeMachineClient([authorization()], {})
        result = reconcile_with_client(
            scheduled_for=SCHEDULED_FOR, db_path=self.db,
            client=client,  # type: ignore[arg-type]
        )
        self.assertTrue(result["accounts"][0]["identity_matches"])
        self.assertEqual(result["accounts"][0]["status"], "available")
        self.assertEqual(result["accounts"][0]["reason"], "")
        self._assert_no_content_side_effects(client)

    def test_empty_authorization_directory_is_explicitly_not_all_healthy(self) -> None:
        client = FakeMachineClient([], {})
        result = reconcile_with_client(
            scheduled_for=SCHEDULED_FOR, db_path=self.db,
            client=client,  # type: ignore[arg-type]
        )
        self.assertEqual(result["authorization_state"], "no_authorization")
        self.assertEqual(result["accounts"], [])
        self.assertFalse(result["content_sync_enabled"])
        self._assert_no_content_side_effects(client)

    def test_expired_and_reauthorization_required_are_attention_only(self) -> None:
        for changes, reason in (
            ({"needs_reauthorization": True}, "reauthorization_required"),
            ({"refresh_expires_at": 0}, "refresh_expired"),
            ({"access_expires_at": 0}, "access_expired"),
        ):
            with self.subTest(reason=reason):
                client = FakeMachineClient([{**authorization(), **changes}], {})
                result = reconcile_with_client(
                    scheduled_for=SCHEDULED_FOR, db_path=self.db,
                    client=client,  # type: ignore[arg-type]
                )
                self.assertEqual(result["authorization_state"], "attention")
                self.assertEqual(result["accounts"][0]["status"], "attention")
                self.assertEqual(result["accounts"][0]["reason"], reason)
                self._assert_no_content_side_effects(client)

    def test_retired_materialization_is_never_called_or_reopens_slots(self) -> None:
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
            self.fail("retired OpenAPI must not materialize or retry content")

        result = reconcile_with_client(
            scheduled_for=SCHEDULED_FOR,
            db_path=self.db,
            client=client,  # type: ignore[arg-type]
            raw_root=self.raw,
            materialize_page=fail_materialization,
        )
        account = result["accounts"][0]
        self.assertEqual(account["status"], "available")
        self._assert_no_content_side_effects(client)

    def test_retired_cursor_loop_cannot_claim_any_content_coverage(self) -> None:
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
        self.assertEqual(account["status"], "available")
        self.assertNotIn("pagination_complete", account)
        self.assertNotIn("coverage_complete", account)
        self._assert_no_content_side_effects(client)


if __name__ == "__main__":
    unittest.main()
