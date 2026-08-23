"""Strict Mac-side client for the loopback-only Douyin machine API."""

from __future__ import annotations

import json
import os
import re
import secrets
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

import httpx


MACHINE_API_ORIGIN = "http://127.0.0.1:14175"
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
DEFAULT_ENV_PATH = (
    Path.home() / "Library" / "Application Support" / "DcarAIGC" / "douyin-sync.env"
)
_AUTHORIZATION_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_PLATFORM_UID_RE = re.compile(r"^\d{6,24}$")
_VIDEO_ID_RE = re.compile(r"^\d{6,24}$")
_SSH_ALIAS_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class DouyinMachineAPIError(RuntimeError):
    """Stable failure that deliberately excludes credentials and response text."""

    def __init__(
        self,
        error_code: str,
        *,
        retryable: bool = True,
        http_status: int | None = None,
        raw_response: Mapping[str, Any] | None = None,
    ) -> None:
        self.error_code = error_code
        self.retryable = retryable
        self.http_status = http_status
        self.raw_response = dict(raw_response) if raw_response is not None else None
        super().__init__(error_code)


class _MachineCredential:
    """Keep accidental object/debug representations from exposing the key."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "<DouyinMachineCredential [REDACTED]>"


@dataclass(frozen=True)
class DouyinSyncConfig:
    ssh_alias: str
    machine_key_path: Path
    local_port: int = 14175


def _secure_regular_file(path: Path, label: str) -> None:
    if not path.is_absolute():
        raise ValueError(f"{label} path must be absolute")
    try:
        details = path.lstat()
    except OSError as exc:
        raise ValueError(f"{label} file is unavailable") from exc
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        raise ValueError(f"{label} must be a regular non-symlink file")
    if stat.S_IMODE(details.st_mode) not in {0o400, 0o600}:
        raise ValueError(f"{label} mode must be 0400 or 0600")
    if details.st_uid != os.getuid():
        raise ValueError(f"{label} owner is invalid")


def load_douyin_sync_config(path: Path = DEFAULT_ENV_PATH) -> DouyinSyncConfig:
    """Read the dedicated allowlisted environment without sourcing a shell."""

    resolved = Path(path)
    _secure_regular_file(resolved, "douyin-sync.env")
    allowed = {
        "DCAR_DOUYIN_SSH_ALIAS",
        "DCAR_DOUYIN_LOCAL_PORT",
        "DCAR_DOUYIN_MACHINE_KEY_FILE",
    }
    values: dict[str, str] = {}
    try:
        lines = resolved.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ValueError("douyin-sync.env is unreadable") from exc
    for raw_line in lines:
        line = raw_line.removesuffix("\r")
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError("unsupported douyin-sync.env entry")
        key, value = line.split("=", 1)
        if key not in allowed or key in values or not value:
            raise ValueError("unsupported or duplicate douyin-sync.env entry")
        if value != value.strip() or "\x00" in value:
            raise ValueError("invalid douyin-sync.env value")
        values[key] = value
    if set(values) != allowed:
        raise ValueError("douyin-sync.env is incomplete")
    alias = values["DCAR_DOUYIN_SSH_ALIAS"]
    if _SSH_ALIAS_RE.fullmatch(alias) is None:
        raise ValueError("invalid Douyin SSH alias")
    if values["DCAR_DOUYIN_LOCAL_PORT"] != "14175":
        raise ValueError("Douyin local port must be 14175")
    machine_key_path = Path(values["DCAR_DOUYIN_MACHINE_KEY_FILE"])
    _secure_regular_file(machine_key_path, "Douyin Machine credential")
    return DouyinSyncConfig(ssh_alias=alias, machine_key_path=machine_key_path)


def _read_machine_key(path: Path) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as exc:
        raise ValueError("Douyin Machine credential is unreadable") from exc
    if (
        not 32 <= len(value) <= 4096
        or any(ord(character) < 33 or ord(character) > 126 for character in value)
    ):
        raise ValueError("Douyin Machine credential is invalid")
    return value


class DouyinMachineClient:
    """Call only the fixed SSH-forwarded 4175 machine endpoints."""

    def __init__(
        self,
        config: DouyinSyncConfig,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if config.local_port != 14175:
            raise ValueError("Douyin local port must be 14175")
        self._machine_key = _MachineCredential(
            _read_machine_key(config.machine_key_path)
        )
        self._client = httpx.Client(
            base_url=MACHINE_API_ORIGIN,
            transport=transport,
            timeout=httpx.Timeout(8, connect=2, read=8, write=5, pool=2),
            follow_redirects=False,
            trust_env=False,
        )

    @classmethod
    def from_env(
        cls,
        path: Path = DEFAULT_ENV_PATH,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> "DouyinMachineClient":
        return cls(load_douyin_sync_config(path), transport=transport)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "DouyinMachineClient":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def list_authorizations(self) -> list[dict[str, Any]]:
        payload = self._request_json("GET", "/internal/v1/authorizations")
        if set(payload) != {"items"} or not isinstance(payload["items"], list):
            raise DouyinMachineAPIError("invalid_authorizations_response")
        items = [self._authorization(value) for value in payload["items"]]
        authorization_ids = [str(item["authorization_id"]) for item in items]
        platform_uids = [str(item["platform_uid"]) for item in items]
        if len(set(authorization_ids)) != len(items) or len(set(platform_uids)) != len(
            items
        ):
            raise DouyinMachineAPIError("duplicate_active_authorization")
        return items

    def video_list_page(
        self, *, authorization_id: str, cursor: int, count: int = 20
    ) -> dict[str, Any]:
        if _AUTHORIZATION_ID_RE.fullmatch(authorization_id) is None:
            raise ValueError("authorization_id must be 32 lowercase hexadecimal digits")
        if isinstance(cursor, bool) or not 0 <= cursor <= 2**63 - 1:
            raise ValueError("cursor is invalid")
        if isinstance(count, bool) or not 1 <= count <= 20:
            raise ValueError("count must be between 1 and 20")
        payload = self._request_json(
            "POST",
            "/internal/v1/video-list/page",
            json_body={
                "authorization_id": authorization_id,
                "cursor": cursor,
                "count": count,
            },
        )
        try:
            return self._video_page(payload, count=count)
        except DouyinMachineAPIError as exc:
            if exc.raw_response is not None:
                raise
            raise DouyinMachineAPIError(
                exc.error_code,
                retryable=exc.retryable,
                http_status=exc.http_status,
                raw_response=payload,
            ) from exc

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        json_body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if method not in {"GET", "POST"} or path not in {
            "/internal/v1/authorizations",
            "/internal/v1/video-list/page",
        }:
            raise ValueError("unsupported Douyin Machine API target")
        request = self._client.build_request(
            method,
            path,
            headers={
                "Accept": "application/json",
                "X-Dcar-Machine-Key": self._machine_key.reveal(),
                "X-Request-ID": secrets.token_hex(12),
            },
            json=json_body,
        )
        response: httpx.Response | None = None
        try:
            response = self._client.send(request, stream=True)
            content_length = response.headers.get("content-length")
            if content_length is not None:
                try:
                    declared = int(content_length)
                except ValueError as exc:
                    raise DouyinMachineAPIError("invalid_response_length") from exc
                if declared < 0 or declared > MAX_RESPONSE_BYTES:
                    raise DouyinMachineAPIError("machine_response_too_large")
            chunks: list[bytes] = []
            received = 0
            for chunk in response.iter_bytes():
                received += len(chunk)
                if received > MAX_RESPONSE_BYTES:
                    raise DouyinMachineAPIError("machine_response_too_large")
                chunks.append(chunk)
            body = b"".join(chunks)
        except DouyinMachineAPIError:
            raise
        except httpx.HTTPError as exc:
            raise DouyinMachineAPIError("machine_api_unavailable") from exc
        finally:
            if response is not None:
                response.close()
        media_type = response.headers.get("content-type", "").split(";", 1)[0]
        if media_type.lower() != "application/json":
            raise DouyinMachineAPIError("invalid_machine_content_type")
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise DouyinMachineAPIError("invalid_machine_json") from exc
        if not isinstance(payload, dict):
            raise DouyinMachineAPIError("invalid_machine_envelope")
        if response.status_code != 200:
            detail = payload.get("detail")
            safe_detail = (
                detail
                if isinstance(detail, str)
                and re.fullmatch(r"[a-z0-9_]{1,128}", detail) is not None
                else None
            )
            error_code = (
                f"machine_api_{safe_detail}" if safe_detail else "machine_api_rejected"
            )
            raise DouyinMachineAPIError(
                error_code,
                retryable=response.status_code >= 500,
                http_status=response.status_code,
                raw_response=payload,
            )
        return payload

    @staticmethod
    def _authorization(value: Any) -> dict[str, Any]:
        required = {
            "authorization_id",
            "account_id",
            "platform_uid",
            "access_expires_at",
            "refresh_expires_at",
            "renew_count",
            "scopes",
            "needs_reauthorization",
            "updated_at",
        }
        if not isinstance(value, dict) or set(value) != required:
            raise DouyinMachineAPIError("invalid_authorization_projection")
        authorization_id = value["authorization_id"]
        account_id = value["account_id"]
        platform_uid = value["platform_uid"]
        access_expires_at = value["access_expires_at"]
        refresh_expires_at = value["refresh_expires_at"]
        renew_count = value["renew_count"]
        scopes = value["scopes"]
        updated_at = value["updated_at"]
        if not isinstance(authorization_id, str) or _AUTHORIZATION_ID_RE.fullmatch(
            authorization_id
        ) is None:
            raise DouyinMachineAPIError("invalid_authorization_projection")
        if isinstance(account_id, bool) or not isinstance(account_id, int) or account_id < 1:
            raise DouyinMachineAPIError("invalid_authorization_projection")
        if not isinstance(platform_uid, str) or _PLATFORM_UID_RE.fullmatch(
            platform_uid
        ) is None:
            raise DouyinMachineAPIError("invalid_authorization_projection")
        if any(
            isinstance(item, bool) or not isinstance(item, int) or item < 0
            for item in (access_expires_at, refresh_expires_at, renew_count)
        ) or renew_count > 5:
            raise DouyinMachineAPIError("invalid_authorization_projection")
        if (
            not isinstance(scopes, list)
            or not scopes
            or len(scopes) > 20
            or len(set(scopes)) != len(scopes)
            or any(not isinstance(scope, str) or not scope for scope in scopes)
        ):
            raise DouyinMachineAPIError("invalid_authorization_projection")
        if not isinstance(value["needs_reauthorization"], bool):
            raise DouyinMachineAPIError("invalid_authorization_projection")
        if isinstance(updated_at, bool) or not isinstance(updated_at, int) or updated_at < 0:
            raise DouyinMachineAPIError("invalid_authorization_projection")
        return dict(value)

    @classmethod
    def _video_page(cls, value: Any, *, count: int) -> dict[str, Any]:
        required = {"captured_at", "cursor", "has_more", "items"}
        if not isinstance(value, dict) or set(value) != required:
            raise DouyinMachineAPIError("invalid_video_page")
        captured_at = value["captured_at"]
        cursor = value["cursor"]
        if any(
            isinstance(item, bool) or not isinstance(item, int) or item < 0
            for item in (captured_at, cursor)
        ):
            raise DouyinMachineAPIError("invalid_video_page")
        if not isinstance(value["has_more"], bool):
            raise DouyinMachineAPIError("invalid_video_page")
        if not isinstance(value["items"], list) or len(value["items"]) > count:
            raise DouyinMachineAPIError("invalid_video_page")
        items = [cls._video_item(item) for item in value["items"]]
        return {**value, "items": items}

    @staticmethod
    def _video_item(value: Any) -> dict[str, Any]:
        required = {
            "video_id",
            "title",
            "create_time",
            "is_top",
            "is_reviewed",
            "video_status",
            "share_url",
            "item_id",
            "media_type",
            "cover",
            "statistics",
        }
        if not isinstance(value, dict) or set(value) != required:
            raise DouyinMachineAPIError("invalid_video_item")
        video_id = value["video_id"]
        if not isinstance(video_id, str) or _VIDEO_ID_RE.fullmatch(video_id) is None:
            raise DouyinMachineAPIError("invalid_video_id")
        if not isinstance(value["title"], str) or len(value["title"]) > 10_000:
            raise DouyinMachineAPIError("invalid_video_item")
        create_time = value["create_time"]
        if isinstance(create_time, bool) or not isinstance(create_time, int) or create_time < 0:
            raise DouyinMachineAPIError("invalid_video_item")
        for name in ("is_top", "is_reviewed"):
            if value[name] is not None and not isinstance(value[name], bool):
                raise DouyinMachineAPIError("invalid_video_item")
        for name in ("video_status", "media_type"):
            field = value[name]
            if field is not None and (
                isinstance(field, bool) or not isinstance(field, int) or field < 0
            ):
                raise DouyinMachineAPIError("invalid_video_item")
        for name in ("share_url", "cover"):
            field = value[name]
            if field is not None:
                if not isinstance(field, str) or len(field) > 4096:
                    raise DouyinMachineAPIError("invalid_video_item")
                parsed = urlsplit(field)
                if (
                    parsed.scheme != "https"
                    or not parsed.netloc
                    or parsed.username
                    or parsed.password
                ):
                    raise DouyinMachineAPIError("invalid_video_item")
        item_id = value["item_id"]
        if item_id is not None and (
            not isinstance(item_id, str) or len(item_id) > 4096 or "\x00" in item_id
        ):
            raise DouyinMachineAPIError("invalid_video_item")
        statistics = value["statistics"]
        statistic_names = {
            "forward_count",
            "comment_count",
            "digg_count",
            "download_count",
            "play_count",
            "share_count",
        }
        if not isinstance(statistics, dict) or set(statistics) != statistic_names:
            raise DouyinMachineAPIError("invalid_video_statistics")
        for statistic in statistics.values():
            if statistic is not None and (
                isinstance(statistic, bool)
                or not isinstance(statistic, int)
                or statistic < 0
            ):
                raise DouyinMachineAPIError("invalid_video_statistics")
        return dict(value)
