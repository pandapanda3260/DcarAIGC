from __future__ import annotations

import asyncio
import secrets
import time
from dataclasses import dataclass
from typing import Any, Callable

from .crypto import TokenCipher
from .provider import DouyinOAuthClient, DouyinProviderError, VideoListPage
from .store import VaultStore


ACCESS_REFRESH_LEEWAY_SECONDS = 5 * 60
REFRESH_RENEW_WINDOW_SECONDS = 3 * 24 * 60 * 60
REFRESH_LEASE_SECONDS = 60
ACCESS_CIPHERTEXT_KIND = "access"
REFRESH_CIPHERTEXT_KIND = "refresh"


class TokenLifecycleError(RuntimeError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"douyin_token_{reason}")


class ReauthorizationRequired(TokenLifecycleError):
    pass


@dataclass(frozen=True)
class AuthorizedAccess:
    authorization_id: str
    open_id: str
    access_token: str
    access_expires_at: int


class DouyinTokenManager:
    """Keeps provider credentials inside the control plane.

    The process-local lock avoids duplicate work in the normal one-process service;
    the Vault lease remains the cross-process correctness boundary.
    """

    def __init__(
        self,
        store: VaultStore,
        cipher: TokenCipher,
        provider: DouyinOAuthClient,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._store = store
        self._cipher = cipher
        self._provider = provider
        self._clock = clock
        self._locks: dict[str, asyncio.Lock] = {}

    async def authorized_access(
        self,
        authorization_id: str,
        *,
        actor: str,
        request_id: str,
        force_refresh: bool = False,
    ) -> AuthorizedAccess:
        lock = self._locks.setdefault(authorization_id, asyncio.Lock())
        async with lock:
            return await self._authorized_access_locked(
                authorization_id,
                actor=actor,
                request_id=request_id,
                force_refresh=force_refresh,
            )

    async def video_list_page(
        self,
        authorization_id: str,
        *,
        cursor: int,
        count: int,
        actor: str,
        request_id: str,
    ) -> VideoListPage:
        access = await self.authorized_access(
            authorization_id, actor=actor, request_id=request_id
        )
        try:
            return await self._provider.video_list_page(
                access_token=access.access_token,
                open_id=access.open_id,
                cursor=cursor,
                count=count,
            )
        except DouyinProviderError as exc:
            if exc.provider_error_code not in {10008, 2190008}:
                raise
        access = await self.authorized_access(
            authorization_id,
            actor=actor,
            request_id=request_id,
            force_refresh=True,
        )
        return await self._provider.video_list_page(
            access_token=access.access_token,
            open_id=access.open_id,
            cursor=cursor,
            count=count,
        )

    async def _authorized_access_locked(
        self,
        authorization_id: str,
        *,
        actor: str,
        request_id: str,
        force_refresh: bool,
    ) -> AuthorizedAccess:
        now = int(self._clock())
        row = await asyncio.to_thread(
            self._store.get_active_authorization, authorization_id
        )
        if row is None:
            raise TokenLifecycleError("authorization_not_found")
        if bool(row["needs_reauthorization"]):
            raise ReauthorizationRequired("reauthorization_required")
        access, refresh = self._decrypt_tokens(authorization_id, row)
        access_due = force_refresh or int(row["access_expires_at"]) <= (
            now + ACCESS_REFRESH_LEEWAY_SECONDS
        )
        refresh_due = int(row["refresh_expires_at"]) <= (
            now + REFRESH_RENEW_WINDOW_SECONDS
        )
        rotation_due = int(row["key_version"]) != self._cipher.key_version
        if not access_due and not refresh_due and not rotation_due:
            return self._access_result(authorization_id, row, access)

        lease_owner = secrets.token_hex(16)
        acquired = await asyncio.to_thread(
            self._store.acquire_refresh_lease,
            authorization_id,
            lease_owner=lease_owner,
            lease_seconds=REFRESH_LEASE_SECONDS,
            actor=actor,
            request_id=request_id,
            now=now,
        )
        if not acquired:
            raise TokenLifecycleError("refresh_busy")

        lease_cleared = False
        try:
            # Another process may have completed maintenance before this lease.
            row = await asyncio.to_thread(
                self._store.get_active_authorization, authorization_id
            )
            if row is None or bool(row["needs_reauthorization"]):
                raise ReauthorizationRequired("reauthorization_required")
            access, refresh = self._decrypt_tokens(authorization_id, row)
            now = int(self._clock())
            access_due = force_refresh or int(row["access_expires_at"]) <= (
                now + ACCESS_REFRESH_LEEWAY_SECONDS
            )
            refresh_due = int(row["refresh_expires_at"]) <= (
                now + REFRESH_RENEW_WINDOW_SECONDS
            )

            if int(row["refresh_expires_at"]) <= now:
                await asyncio.to_thread(
                    self._store.mark_needs_reauthorization,
                    authorization_id,
                    actor=actor,
                    reason_code="refresh_token_expired",
                    request_id=request_id,
                    now=now,
                )
                lease_cleared = True
                raise ReauthorizationRequired("refresh_token_expired")

            if refresh_due:
                if int(row["renew_count"]) >= 5:
                    await asyncio.to_thread(
                        self._store.mark_needs_reauthorization,
                        authorization_id,
                        actor=actor,
                        reason_code="refresh_renew_limit_reached",
                        request_id=request_id,
                        now=now,
                    )
                    lease_cleared = True
                    raise ReauthorizationRequired("refresh_renew_limit_reached")
                renewed = await self._provider.renew_refresh_token(refresh["token"])
                refresh = {"open_id": refresh["open_id"], "token": renewed.refresh_token}
                refresh_ciphertext = self._cipher.encrypt(
                    authorization_id, REFRESH_CIPHERTEXT_KIND, refresh
                )
                renewed_ok = await asyncio.to_thread(
                    self._store.renew_refresh_token,
                    authorization_id,
                    lease_owner=lease_owner,
                    refresh_token_ciphertext=refresh_ciphertext,
                    refresh_expires_at=renewed.refresh_expires_at,
                    key_version=self._cipher.key_version,
                    actor=actor,
                    request_id=request_id,
                    now=now,
                )
                if not renewed_ok:
                    raise TokenLifecycleError("refresh_renew_race")
                row["refresh_token_ciphertext"] = refresh_ciphertext
                row["refresh_expires_at"] = renewed.refresh_expires_at
                row["renew_count"] = int(row["renew_count"]) + 1

            if access_due:
                try:
                    bundle = await self._provider.refresh_access_token(
                        refresh["token"], expected_open_id=access["open_id"]
                    )
                except DouyinProviderError as exc:
                    if exc.provider_error_code == 10010:
                        await asyncio.to_thread(
                            self._store.mark_needs_reauthorization,
                            authorization_id,
                            actor=actor,
                            reason_code="refresh_token_rejected",
                            request_id=request_id,
                            now=now,
                        )
                        lease_cleared = True
                        raise ReauthorizationRequired(
                            "refresh_token_rejected"
                        ) from exc
                    raise
                if bundle.open_id != access["open_id"]:
                    raise TokenLifecycleError("open_id_mismatch")
                access = {"open_id": bundle.open_id, "token": bundle.access_token}
                refresh = {"open_id": bundle.open_id, "token": bundle.refresh_token}
                access_ciphertext = self._cipher.encrypt(
                    authorization_id, ACCESS_CIPHERTEXT_KIND, access
                )
                refresh_ciphertext = self._cipher.encrypt(
                    authorization_id, REFRESH_CIPHERTEXT_KIND, refresh
                )
                updated = await asyncio.to_thread(
                    self._store.update_refreshed_token_bundle,
                    authorization_id,
                    lease_owner=lease_owner,
                    access_token_ciphertext=access_ciphertext,
                    refresh_token_ciphertext=refresh_ciphertext,
                    access_expires_at=bundle.access_expires_at,
                    refresh_expires_at=bundle.refresh_expires_at,
                    key_version=self._cipher.key_version,
                    actor=actor,
                    request_id=request_id,
                    now=now,
                )
                if not updated:
                    raise TokenLifecycleError("refresh_race")
                row["access_expires_at"] = bundle.access_expires_at
                row["refresh_expires_at"] = bundle.refresh_expires_at
            elif refresh_due or int(row["key_version"]) != self._cipher.key_version:
                access_ciphertext = self._cipher.rotate(
                    bytes(row["access_token_ciphertext"])
                )
                refresh_ciphertext = self._cipher.rotate(
                    bytes(row["refresh_token_ciphertext"])
                )
                rotated = await asyncio.to_thread(
                    self._store.rotate_authorization_tokens,
                    authorization_id,
                    lease_owner=lease_owner,
                    access_token_ciphertext=access_ciphertext,
                    refresh_token_ciphertext=refresh_ciphertext,
                    key_version=self._cipher.key_version,
                    actor=actor,
                    request_id=request_id,
                    now=now,
                )
                if not rotated:
                    raise TokenLifecycleError("rotation_race")

            return self._access_result(authorization_id, row, access)
        finally:
            if not lease_cleared:
                await asyncio.to_thread(
                    self._store.release_refresh_lease,
                    authorization_id,
                    lease_owner=lease_owner,
                    actor=actor,
                    request_id=request_id,
                    now=int(self._clock()),
                )

    def _decrypt_tokens(
        self, authorization_id: str, row: dict[str, Any]
    ) -> tuple[dict[str, str], dict[str, str]]:
        access = self._cipher.decrypt(
            authorization_id,
            ACCESS_CIPHERTEXT_KIND,
            bytes(row["access_token_ciphertext"]),
        )
        refresh = self._cipher.decrypt(
            authorization_id,
            REFRESH_CIPHERTEXT_KIND,
            bytes(row["refresh_token_ciphertext"]),
        )
        for payload in (access, refresh):
            if (
                not isinstance(payload.get("open_id"), str)
                or not isinstance(payload.get("token"), str)
                or not payload["open_id"]
                or not payload["token"]
            ):
                raise TokenLifecycleError("ciphertext_payload_invalid")
        if access["open_id"] != refresh["open_id"]:
            raise TokenLifecycleError("open_id_mismatch")
        if (
            self._cipher.open_id_fingerprint(access["open_id"])
            != row["open_id_fingerprint"]
        ):
            raise TokenLifecycleError("open_id_fingerprint_mismatch")
        return access, refresh

    @staticmethod
    def _access_result(
        authorization_id: str,
        row: dict[str, Any],
        access: dict[str, str],
    ) -> AuthorizedAccess:
        return AuthorizedAccess(
            authorization_id=authorization_id,
            open_id=access["open_id"],
            access_token=access["token"],
            access_expires_at=int(row["access_expires_at"]),
        )
