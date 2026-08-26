from __future__ import annotations

import base64
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src" / "dcar_eval"))

from dcar_douyin_control.crypto import TokenCipher  # noqa: E402
from dcar_douyin_control.provider import (  # noqa: E402
    DouyinProviderError,
    RefreshTokenBundle,
    TokenBundle,
    VideoListPage,
)
from dcar_douyin_control.tokens import (  # noqa: E402
    DouyinTokenManager,
    ReauthorizationRequired,
)
from dcar_douyin_control.store import VaultStore  # noqa: E402


AUTHORIZATION_ID = "a" * 32
NOW = 2_000_000_000


class FakeStore:
    def __init__(self, row: dict[str, Any]) -> None:
        self.row = row
        self.calls: list[str] = []

    def get_active_authorization(self, authorization_id: str) -> dict[str, Any] | None:
        self.calls.append("get")
        return dict(self.row) if authorization_id == AUTHORIZATION_ID else None

    def acquire_refresh_lease(self, _id: str, **kwargs: Any) -> bool:
        self.calls.append("acquire")
        self.row["refresh_lease_owner"] = kwargs["lease_owner"]
        return True

    def release_refresh_lease(self, _id: str, **_kwargs: Any) -> bool:
        self.calls.append("release")
        self.row["refresh_lease_owner"] = None
        return True

    def renew_refresh_token(self, _id: str, **kwargs: Any) -> bool:
        self.calls.append("renew")
        self.row["refresh_token_ciphertext"] = kwargs["refresh_token_ciphertext"]
        self.row["refresh_expires_at"] = kwargs["refresh_expires_at"]
        self.row["renew_count"] += 1
        self.row["key_version"] = kwargs["key_version"]
        return True

    def update_refreshed_token_bundle(self, _id: str, **kwargs: Any) -> bool:
        self.calls.append("refresh")
        for key in (
            "access_token_ciphertext",
            "refresh_token_ciphertext",
            "access_expires_at",
            "refresh_expires_at",
            "key_version",
        ):
            self.row[key] = kwargs[key]
        return True

    def rotate_authorization_tokens(self, _id: str, **kwargs: Any) -> bool:
        self.calls.append("rotate")
        self.row["access_token_ciphertext"] = kwargs["access_token_ciphertext"]
        self.row["refresh_token_ciphertext"] = kwargs["refresh_token_ciphertext"]
        self.row["key_version"] = kwargs["key_version"]
        return True

    def mark_needs_reauthorization(self, _id: str, **_kwargs: Any) -> bool:
        self.calls.append("reauthorize")
        self.row["needs_reauthorization"] = True
        self.row["refresh_lease_owner"] = None
        return True


class FakeProvider:
    def __init__(self) -> None:
        self.refresh_calls = 0
        self.renew_calls = 0
        self.refresh_error: Exception | None = None
        self.video_calls = 0
        self.video_error_once: Exception | None = None

    async def refresh_access_token(
        self, refresh_token: str, *, expected_open_id: str
    ) -> TokenBundle:
        self.refresh_calls += 1
        if self.refresh_error is not None:
            raise self.refresh_error
        self.last_refresh_token = refresh_token
        return TokenBundle(
            open_id=expected_open_id,
            access_token="access-new",
            refresh_token="refresh-new",
            access_expires_at=NOW + 15 * 24 * 60 * 60,
            refresh_expires_at=NOW + 30 * 24 * 60 * 60,
            scopes=["user_info", "video.list"],
        )

    async def renew_refresh_token(self, refresh_token: str) -> RefreshTokenBundle:
        self.renew_calls += 1
        self.last_renew_token = refresh_token
        return RefreshTokenBundle(
            refresh_token="refresh-renewed",
            refresh_expires_at=NOW + 30 * 24 * 60 * 60,
        )

    async def video_list_page(
        self, *, access_token: str, open_id: str, cursor: int, count: int
    ) -> VideoListPage:
        self.video_calls += 1
        if self.video_error_once is not None:
            error = self.video_error_once
            self.video_error_once = None
            raise error
        self.last_video_access_token = access_token
        return VideoListPage(
            captured_at=NOW, cursor=cursor, has_more=False, items=[]
        )


class DouyinTokenManagerTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.key_v1 = Fernet.generate_key().decode("ascii")
        self.key_v2 = Fernet.generate_key().decode("ascii")
        (self.root / "hmac").write_text(
            base64.urlsafe_b64encode(b"h" * 32).decode("ascii"), encoding="utf-8"
        )
        (self.root / "keyring").write_text(
            f"2:{self.key_v2}\n1:{self.key_v1}\n", encoding="utf-8"
        )
        self.cipher = TokenCipher(self.root / "keyring", self.root / "hmac")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def row(
        self,
        *,
        access_expires_at: int = NOW + 3600,
        refresh_expires_at: int = NOW + 10 * 24 * 60 * 60,
        renew_count: int = 0,
        key_version: int = 2,
        cipher: TokenCipher | None = None,
    ) -> dict[str, Any]:
        selected = cipher or self.cipher
        open_id = "open-id"
        return {
            "id": AUTHORIZATION_ID,
            "status": "active",
            "needs_reauthorization": False,
            "open_id_fingerprint": self.cipher.open_id_fingerprint(open_id),
            "access_token_ciphertext": selected.encrypt(
                AUTHORIZATION_ID,
                "access",
                {"open_id": open_id, "token": "access-old"},
            ),
            "refresh_token_ciphertext": selected.encrypt(
                AUTHORIZATION_ID,
                "refresh",
                {"open_id": open_id, "token": "refresh-old"},
            ),
            "access_expires_at": access_expires_at,
            "refresh_expires_at": refresh_expires_at,
            "renew_count": renew_count,
            "key_version": key_version,
        }

    async def test_fresh_token_needs_no_maintenance(self) -> None:
        store = FakeStore(self.row())
        provider = FakeProvider()
        manager = DouyinTokenManager(
            store, self.cipher, provider, clock=lambda: NOW  # type: ignore[arg-type]
        )
        access = await manager.authorized_access(
            AUTHORIZATION_ID, actor="machine", request_id="request"
        )
        self.assertEqual(access.access_token, "access-old")
        self.assertEqual(store.calls, ["get"])
        self.assertEqual(provider.refresh_calls, 0)

    async def test_reads_tokens_written_by_real_vault_confirmation(self) -> None:
        store = VaultStore(self.root / "vault.sqlite3")
        store.initialize()
        state_digest = "b" * 64
        candidate = {
            "open_id": "open-id",
            "access_token": "access-old",
            "refresh_token": "refresh-old",
            "access_expires_at": NOW + 3600,
            "refresh_expires_at": NOW + 10 * 24 * 60 * 60,
            "scopes": ["user_info", "video.list"],
            "nickname": "账号",
            "avatar": "",
        }
        store.create_state(
            state_digest=state_digest,
            bound_username="operator",
            session_binding="a" * 64,
            account_id=7,
            platform_uid="123456789",
            scopes=["user_info", "video.list"],
            expires_at=NOW + 600,
            request_id="start",
            now=NOW,
        )
        store.begin_exchange(
            state_digest,
            "operator",
            "a" * 64,
            request_id="callback",
            now=NOW + 1,
        )
        fingerprint = self.cipher.open_id_fingerprint("open-id")
        created = store.complete_targeted_authorization(
            state_digest=state_digest,
            bound_username="operator",
            session_binding="a" * 64,
            open_id_fingerprint=fingerprint,
            candidate=candidate,
            cipher=self.cipher,
            request_id="targeted-complete",
            now=NOW + 2,
        )
        manager = DouyinTokenManager(
            store, self.cipher, FakeProvider(), clock=lambda: NOW + 4  # type: ignore[arg-type]
        )
        access = await manager.authorized_access(
            str(created["id"]), actor="machine", request_id="request"
        )
        self.assertEqual(access.access_token, "access-old")

    async def test_expiring_access_atomically_refreshes_both_tokens(self) -> None:
        store = FakeStore(self.row(access_expires_at=NOW + 60))
        provider = FakeProvider()
        manager = DouyinTokenManager(
            store, self.cipher, provider, clock=lambda: NOW  # type: ignore[arg-type]
        )
        access = await manager.authorized_access(
            AUTHORIZATION_ID, actor="machine", request_id="request"
        )
        self.assertEqual(access.access_token, "access-new")
        self.assertEqual(provider.refresh_calls, 1)
        self.assertIn("refresh", store.calls)
        self.assertEqual(store.calls[-1], "release")

    async def test_refresh_token_is_renewed_before_expiry(self) -> None:
        store = FakeStore(
            self.row(
                access_expires_at=NOW + 3600,
                refresh_expires_at=NOW + 24 * 60 * 60,
            )
        )
        provider = FakeProvider()
        manager = DouyinTokenManager(
            store, self.cipher, provider, clock=lambda: NOW  # type: ignore[arg-type]
        )
        access = await manager.authorized_access(
            AUTHORIZATION_ID, actor="machine", request_id="request"
        )
        self.assertEqual(access.access_token, "access-old")
        self.assertEqual(provider.renew_calls, 1)
        self.assertEqual(provider.refresh_calls, 0)
        self.assertIn("renew", store.calls)
        self.assertIn("rotate", store.calls)

    async def test_fifth_renewal_requires_new_authorization(self) -> None:
        store = FakeStore(
            self.row(
                refresh_expires_at=NOW + 24 * 60 * 60,
                renew_count=5,
            )
        )
        manager = DouyinTokenManager(
            store, self.cipher, FakeProvider(), clock=lambda: NOW  # type: ignore[arg-type]
        )
        with self.assertRaises(ReauthorizationRequired):
            await manager.authorized_access(
                AUTHORIZATION_ID, actor="machine", request_id="request"
            )
        self.assertTrue(store.row["needs_reauthorization"])
        self.assertIn("reauthorize", store.calls)

    async def test_provider_reject_marks_authorization(self) -> None:
        store = FakeStore(self.row(access_expires_at=NOW + 60))
        provider = FakeProvider()
        provider.refresh_error = DouyinProviderError(
            "access_refresh", "provider_rejected", provider_error_code=10010
        )
        manager = DouyinTokenManager(
            store, self.cipher, provider, clock=lambda: NOW  # type: ignore[arg-type]
        )
        with self.assertRaises(ReauthorizationRequired):
            await manager.authorized_access(
                AUTHORIZATION_ID, actor="machine", request_id="request"
            )
        self.assertTrue(store.row["needs_reauthorization"])

    async def test_expired_access_response_refreshes_and_retries_once(self) -> None:
        store = FakeStore(self.row())
        provider = FakeProvider()
        provider.video_error_once = DouyinProviderError(
            "video_list", "provider_rejected", provider_error_code=10008
        )
        manager = DouyinTokenManager(
            store, self.cipher, provider, clock=lambda: NOW  # type: ignore[arg-type]
        )
        page = await manager.video_list_page(
            AUTHORIZATION_ID,
            cursor=0,
            count=20,
            actor="machine",
            request_id="request",
        )
        self.assertFalse(page.has_more)
        self.assertEqual(provider.video_calls, 2)
        self.assertEqual(provider.refresh_calls, 1)
        self.assertEqual(provider.last_video_access_token, "access-new")

    async def test_old_ciphertext_is_lazily_rotated(self) -> None:
        (self.root / "old-keyring").write_text(
            f"1:{self.key_v1}\n", encoding="utf-8"
        )
        old_cipher = TokenCipher(self.root / "old-keyring", self.root / "hmac")
        store = FakeStore(self.row(key_version=1, cipher=old_cipher))
        manager = DouyinTokenManager(
            store, self.cipher, FakeProvider(), clock=lambda: NOW  # type: ignore[arg-type]
        )
        access = await manager.authorized_access(
            AUTHORIZATION_ID, actor="machine", request_id="request"
        )
        self.assertEqual(access.access_token, "access-old")
        self.assertEqual(store.row["key_version"], 2)
        self.assertIn("rotate", store.calls)


if __name__ == "__main__":
    unittest.main()
