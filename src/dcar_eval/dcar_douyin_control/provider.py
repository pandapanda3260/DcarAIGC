from __future__ import annotations

import time
from dataclasses import dataclass
import re
from typing import Any, Callable
from urllib.parse import urlencode, urlsplit

import httpx

from .config import DouyinControlConfig
from .crypto import read_shared_key


DOUYIN_ORIGIN = "https://open.douyin.com"
DOUYIN_AUTHORIZE_URL = f"{DOUYIN_ORIGIN}/platform/oauth/connect/"
DOUYIN_TOKEN_URL = f"{DOUYIN_ORIGIN}/oauth/access_token/"
DOUYIN_USERINFO_URL = f"{DOUYIN_ORIGIN}/oauth/userinfo/"
DOUYIN_REFRESH_URL = f"{DOUYIN_ORIGIN}/oauth/refresh_token/"
DOUYIN_RENEW_REFRESH_URL = f"{DOUYIN_ORIGIN}/oauth/renew_refresh_token/"
DOUYIN_VIDEO_LIST_URL = f"{DOUYIN_ORIGIN}/video/list/"
MAX_PROVIDER_RESPONSE_BYTES = 2 * 1024 * 1024
_SCOPE_RE = re.compile(r"[A-Za-z0-9._:-]{1,128}")


@dataclass(frozen=True)
class TokenBundle:
    open_id: str
    access_token: str
    refresh_token: str
    access_expires_at: int
    refresh_expires_at: int
    scopes: list[str]


@dataclass(frozen=True)
class RefreshTokenBundle:
    refresh_token: str
    refresh_expires_at: int


@dataclass(frozen=True)
class VideoListPage:
    captured_at: int
    cursor: int
    has_more: bool
    items: list[dict[str, Any]]

    def as_dict(self) -> dict[str, Any]:
        return {
            "captured_at": self.captured_at,
            "cursor": self.cursor,
            "has_more": self.has_more,
            "items": self.items,
        }


class DouyinProviderError(RuntimeError):
    """Stable provider failure that never includes credentials or response text."""

    def __init__(
        self,
        operation: str,
        category: str,
        *,
        provider_error_code: int | None = None,
        http_status: int | None = None,
    ) -> None:
        self.operation = operation
        self.category = category
        self.provider_error_code = provider_error_code
        self.http_status = http_status
        suffix = f":{provider_error_code}" if provider_error_code is not None else ""
        super().__init__(f"douyin_{operation}_{category}{suffix}")


class DouyinOAuthClient:
    """Production client with fixed endpoints and an explicit local proxy."""

    def __init__(
        self,
        config: DouyinControlConfig,
        client: httpx.AsyncClient | None = None,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if (
            not config.authorization_enabled
            or config.provider_mode != "douyin"
            or config.client_secret_path is None
            or not config.provider_proxy_url
        ):
            raise RuntimeError("douyin OAuth client is not fully configured")
        self._config = config
        self._clock = clock
        self._client_secret = read_shared_key(
            config.client_secret_path, "Douyin Client Secret"
        )
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            proxy=config.provider_proxy_url,
            timeout=httpx.Timeout(10, connect=2, read=10, write=5, pool=2),
            follow_redirects=False,
            trust_env=False,
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def authorization_url(self, state: str, scopes: list[str]) -> str:
        self._bounded_secret(state, "state", maximum=256, minimum=20)
        normalized_scopes = self._requested_scopes(scopes)
        return f"{DOUYIN_AUTHORIZE_URL}?" + urlencode(
            {
                "client_key": self._config.client_key,
                "response_type": "code",
                "scope": ",".join(normalized_scopes),
                "redirect_uri": self._config.callback_url,
                "state": state,
            }
        )

    async def exchange_code(self, code: str) -> TokenBundle:
        self._bounded_secret(code, "code", maximum=512)
        data = await self._request_data(
            "token_exchange",
            "POST",
            DOUYIN_TOKEN_URL,
            data={
                "client_key": self._config.client_key,
                "client_secret": self._client_secret,
                "code": code,
                "grant_type": "authorization_code",
            },
        )
        return self._token_bundle(data)

    async def userinfo(self, bundle: TokenBundle) -> dict[str, str]:
        self._bounded_secret(bundle.access_token, "access_token", maximum=4096)
        data = await self._request_data(
            "userinfo",
            "POST",
            DOUYIN_USERINFO_URL,
            headers={"access-token": bundle.access_token},
            json={},
        )
        returned_open_id = self._required_secret(data, "open_id", 256)
        if returned_open_id != bundle.open_id:
            raise DouyinProviderError("userinfo", "open_id_mismatch")
        nickname = self._optional_text(data, "nickname", 256) or ""
        avatar = self._optional_https_url(data, "avatar", 4096) or ""
        return {"nickname": nickname, "avatar": avatar}

    async def refresh_access_token(
        self,
        refresh_token: str,
        *,
        expected_open_id: str,
    ) -> TokenBundle:
        self._bounded_secret(refresh_token, "refresh_token", maximum=4096)
        self._bounded_secret(expected_open_id, "open_id", maximum=256)
        data = await self._request_data(
            "access_refresh",
            "POST",
            DOUYIN_REFRESH_URL,
            files={
                "client_key": (None, self._config.client_key),
                "grant_type": (None, "refresh_token"),
                "refresh_token": (None, refresh_token),
            },
        )
        bundle = self._token_bundle(data)
        if bundle.open_id != expected_open_id:
            raise DouyinProviderError("access_refresh", "open_id_mismatch")
        return bundle

    async def renew_refresh_token(self, refresh_token: str) -> RefreshTokenBundle:
        self._bounded_secret(refresh_token, "refresh_token", maximum=4096)
        data = await self._request_data(
            "refresh_renewal",
            "POST",
            DOUYIN_RENEW_REFRESH_URL,
            files={
                "client_key": (None, self._config.client_key),
                "refresh_token": (None, refresh_token),
            },
        )
        return RefreshTokenBundle(
            refresh_token=self._required_secret(data, "refresh_token", 4096),
            refresh_expires_at=(
                int(self._clock()) + self._positive_duration(data, "expires_in")
            ),
        )

    async def video_list_page(
        self,
        *,
        access_token: str,
        open_id: str,
        cursor: int,
        count: int = 20,
    ) -> VideoListPage:
        self._bounded_secret(access_token, "access_token", maximum=4096)
        self._bounded_secret(open_id, "open_id", maximum=256)
        if isinstance(cursor, bool) or not 0 <= cursor <= 2**63 - 1:
            raise ValueError("cursor is invalid")
        if isinstance(count, bool) or not 1 <= count <= 20:
            raise ValueError("count must be between 1 and 20")
        data = await self._request_data(
            "video_list",
            "GET",
            DOUYIN_VIDEO_LIST_URL,
            require_extra=True,
            headers={"access-token": access_token},
            params={"open_id": open_id, "cursor": cursor, "count": count},
        )
        returned_cursor = self._nonnegative_int(data, "cursor")
        has_more = data.get("has_more")
        if not isinstance(has_more, bool):
            raise DouyinProviderError("video_list", "malformed_response")
        raw_items = data.get("list")
        if not isinstance(raw_items, list) or len(raw_items) > count:
            raise DouyinProviderError("video_list", "malformed_response")
        try:
            items = [self._video_item(item) for item in raw_items]
        except (TypeError, ValueError) as exc:
            raise DouyinProviderError("video_list", "malformed_response") from exc
        return VideoListPage(
            captured_at=int(self._clock()),
            cursor=returned_cursor,
            has_more=has_more,
            items=items,
        )

    async def _request_data(
        self,
        operation: str,
        method: str,
        url: str,
        *,
        require_extra: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        try:
            response = await self._client.request(method, url, **kwargs)
        except httpx.HTTPError as exc:
            raise DouyinProviderError(operation, "network_error") from exc
        if len(response.content) > MAX_PROVIDER_RESPONSE_BYTES:
            raise DouyinProviderError(operation, "response_too_large")
        media_type = response.headers.get("content-type", "").split(";", 1)[0]
        if media_type.lower() != "application/json":
            raise DouyinProviderError(operation, "malformed_response")
        try:
            payload = response.json()
            data = self._data_envelope(
                payload, operation=operation, require_extra=require_extra
            )
        except DouyinProviderError:
            raise
        except (TypeError, ValueError) as exc:
            raise DouyinProviderError(operation, "malformed_response") from exc
        if not 200 <= response.status_code < 300:
            raise DouyinProviderError(
                operation, "http_error", http_status=response.status_code
            )
        return data

    def _token_bundle(self, data: dict[str, Any]) -> TokenBundle:
        now = int(self._clock())
        return TokenBundle(
            open_id=self._required_secret(data, "open_id", 256),
            access_token=self._required_secret(data, "access_token", 4096),
            refresh_token=self._required_secret(data, "refresh_token", 4096),
            access_expires_at=now + self._positive_duration(data, "expires_in"),
            refresh_expires_at=(
                now + self._positive_duration(data, "refresh_expires_in")
            ),
            scopes=self._returned_scopes(data.get("scope")),
        )

    @classmethod
    def _data_envelope(
        cls,
        payload: Any,
        *,
        operation: str,
        require_extra: bool,
    ) -> dict[str, Any]:
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
            raise ValueError("invalid data envelope")
        data = payload["data"]
        data_code = cls._error_code(data, required=True)
        extra = payload.get("extra")
        if require_extra and not isinstance(extra, dict):
            raise ValueError("missing extra envelope")
        if extra is not None:
            if not isinstance(extra, dict):
                raise ValueError("invalid extra envelope")
            extra_code = cls._error_code(extra, required=True)
            sub_error_code = cls._integer_field(
                extra, "sub_error_code", required=False, maximum=2**31 - 1
            )
            if extra_code != 0 or sub_error_code not in {None, 0}:
                raise DouyinProviderError(
                    operation,
                    "provider_rejected",
                    provider_error_code=extra_code or sub_error_code,
                )
            cls._optional_text(extra, "description", 2048)
            cls._optional_text(extra, "sub_description", 2048)
            if "logid" in extra:
                cls._optional_text(extra, "logid", 256)
            if "log_id" in extra:
                cls._optional_text(extra, "log_id", 256)
            cls._integer_field(extra, "now", required=False, maximum=2**63 - 1)
        if data_code != 0:
            raise DouyinProviderError(
                operation,
                "provider_rejected",
                provider_error_code=data_code,
            )
        cls._optional_text(data, "description", 2048)
        return data

    @classmethod
    def _video_item(cls, value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise ValueError("invalid video item")
        statistics = value.get("statistics", {})
        if not isinstance(statistics, dict):
            raise ValueError("invalid video statistics")
        normalized_statistics: dict[str, int | None] = {}
        for key in (
            "forward_count",
            "comment_count",
            "digg_count",
            "download_count",
            "play_count",
            "share_count",
        ):
            normalized_statistics[key] = cls._integer_field(
                statistics, key, required=False, maximum=2**63 - 1
            )
        return {
            "video_id": cls._required_secret(value, "video_id", 128),
            "title": cls._optional_text(value, "title", 10_000) or "",
            "create_time": cls._integer_field(
                value, "create_time", required=True, maximum=2**63 - 1
            ),
            "is_top": cls._optional_bool(value, "is_top"),
            "is_reviewed": cls._optional_bool(value, "is_reviewed"),
            "video_status": cls._integer_field(
                value, "video_status", required=False, maximum=2**31 - 1
            ),
            "share_url": cls._optional_https_url(value, "share_url", 4096),
            "item_id": cls._optional_text(value, "item_id", 4096),
            "media_type": cls._integer_field(
                value, "media_type", required=False, maximum=2**31 - 1
            ),
            "cover": cls._optional_https_url(value, "cover", 4096),
            "statistics": normalized_statistics,
        }

    @classmethod
    def _required_secret(cls, data: dict[str, Any], key: str, maximum: int) -> str:
        value = data.get(key)
        cls._bounded_secret(value, key, maximum=maximum)
        if not isinstance(value, str):  # Narrow the type after shared validation.
            raise ValueError(f"invalid provider field: {key}")
        return value

    @staticmethod
    def _bounded_secret(
        value: Any,
        label: str,
        *,
        maximum: int,
        minimum: int = 1,
    ) -> None:
        if (
            not isinstance(value, str)
            or not minimum <= len(value) <= maximum
            or any(ord(character) < 33 for character in value)
        ):
            raise ValueError(f"invalid {label}")

    @staticmethod
    def _optional_text(data: dict[str, Any], key: str, maximum: int) -> str | None:
        value = data.get(key)
        if value is None:
            return None
        if not isinstance(value, str) or len(value) > maximum or "\x00" in value:
            raise ValueError(f"invalid provider field: {key}")
        return value

    @classmethod
    def _optional_https_url(
        cls, data: dict[str, Any], key: str, maximum: int
    ) -> str | None:
        value = cls._optional_text(data, key, maximum)
        if not value:
            return value
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.username
            or parsed.password
        ):
            raise ValueError(f"invalid provider field: {key}")
        return value

    @staticmethod
    def _optional_bool(data: dict[str, Any], key: str) -> bool | None:
        value = data.get(key)
        if value is None:
            return None
        if not isinstance(value, bool):
            raise ValueError(f"invalid provider field: {key}")
        return value

    @classmethod
    def _positive_duration(cls, data: dict[str, Any], key: str) -> int:
        value = cls._integer_field(data, key, required=True, maximum=400 * 24 * 60 * 60)
        if value is None or value <= 0:
            raise ValueError(f"invalid provider field: {key}")
        return value

    @classmethod
    def _nonnegative_int(cls, data: dict[str, Any], key: str) -> int:
        value = cls._integer_field(data, key, required=True, maximum=2**63 - 1)
        if value is None:
            raise ValueError(f"invalid provider field: {key}")
        return value

    @staticmethod
    def _integer_field(
        data: dict[str, Any],
        key: str,
        *,
        required: bool,
        maximum: int,
    ) -> int | None:
        value = data.get(key)
        if value is None and not required:
            return None
        if value is None:
            raise ValueError(f"invalid provider field: {key}")
        if isinstance(value, bool):
            raise ValueError(f"invalid provider field: {key}")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid provider field: {key}") from exc
        if str(parsed) != str(value) or not 0 <= parsed <= maximum:
            raise ValueError(f"invalid provider field: {key}")
        return parsed

    @classmethod
    def _error_code(cls, data: dict[str, Any], *, required: bool) -> int:
        value = cls._integer_field(
            data, "error_code", required=required, maximum=2**31 - 1
        )
        if value is None:
            raise ValueError("missing provider error_code")
        return value

    @staticmethod
    def _requested_scopes(scopes: list[str]) -> list[str]:
        if (
            not isinstance(scopes, list)
            or not scopes
            or len(scopes) > 20
            or len(set(scopes)) != len(scopes)
            or any(_SCOPE_RE.fullmatch(scope) is None for scope in scopes)
            or "renew_refresh_token" in scopes
        ):
            raise ValueError("invalid OAuth scopes")
        return list(scopes)

    @staticmethod
    def _returned_scopes(value: Any) -> list[str]:
        if isinstance(value, str):
            scopes = value.split(",")
        elif isinstance(value, list):
            scopes = value
        else:
            raise ValueError("invalid provider scope")
        if (
            not scopes
            or len(scopes) > 20
            or any(not isinstance(scope, str) for scope in scopes)
            or len(set(scopes)) != len(scopes)
            or any(_SCOPE_RE.fullmatch(scope) is None for scope in scopes)
        ):
            raise ValueError("invalid provider scope")
        return list(scopes)


class MockOAuthClient:
    """Stage 0 client for a separate loopback-only mock OAuth provider."""

    def __init__(
        self,
        config: DouyinControlConfig,
        client: httpx.AsyncClient,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not config.authorization_enabled or config.client_secret_path is None:
            raise RuntimeError(
                "mock OAuth client cannot run while authorization is disabled"
            )
        self._config = config
        self._client = client
        self._clock = clock
        self._client_secret = read_shared_key(
            config.client_secret_path, "Douyin mock Client Secret"
        )

    def authorization_url(self, state: str, scopes: list[str]) -> str:
        query = urlencode(
            {
                "client_key": self._config.client_key,
                "response_type": "code",
                "scope": ",".join(scopes),
                "redirect_uri": self._config.callback_url,
                "state": state,
            }
        )
        separator = "&" if "?" in self._config.provider_authorize_url else "?"
        return f"{self._config.provider_authorize_url}{separator}{query}"

    async def exchange_code(self, code: str) -> TokenBundle:
        try:
            response = await self._client.post(
                self._config.provider_token_url,
                data={
                    "client_key": self._config.client_key,
                    "client_secret": self._client_secret,
                    "code": code,
                    "grant_type": "authorization_code",
                },
            )
            response.raise_for_status()
            data = self._data_envelope(response.json())
            now = int(self._clock())
            return TokenBundle(
                open_id=self._required(data, "open_id", 256),
                access_token=self._required(data, "access_token", 4096),
                refresh_token=self._required(data, "refresh_token", 4096),
                access_expires_at=now + self._positive_int(data, "expires_in"),
                refresh_expires_at=(
                    now + self._positive_int(data, "refresh_expires_in")
                ),
                scopes=self._scopes(data.get("scope")),
            )
        except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("mock_token_exchange_failed") from exc

    async def userinfo(self, bundle: TokenBundle) -> dict[str, str]:
        try:
            response = await self._client.post(
                self._config.provider_userinfo_url,
                data={
                    "access_token": bundle.access_token,
                    "open_id": bundle.open_id,
                },
            )
            response.raise_for_status()
            data = self._data_envelope(response.json())
            returned_open_id = self._required(data, "open_id", 256)
            if returned_open_id != bundle.open_id:
                raise ValueError("userinfo open_id mismatch")
            return {
                "nickname": str(data.get("nickname") or "")[:256],
                "avatar": str(data.get("avatar") or "")[:2048],
            }
        except (httpx.HTTPError, KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("mock_userinfo_failed") from exc

    @staticmethod
    def _data_envelope(payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
            raise ValueError("invalid provider envelope")
        data = payload["data"]
        error_code = data.get("error_code", 0)
        if error_code not in {0, "0", None}:
            raise ValueError("provider rejected request")
        return data

    @staticmethod
    def _required(data: dict[str, Any], key: str, maximum: int) -> str:
        value = data.get(key)
        if not isinstance(value, str) or not value or len(value) > maximum:
            raise ValueError(f"invalid provider field: {key}")
        return value

    @staticmethod
    def _positive_int(data: dict[str, Any], key: str) -> int:
        value = int(data[key])
        if value <= 0:
            raise ValueError(f"invalid provider field: {key}")
        return value

    @staticmethod
    def _scopes(value: Any) -> list[str]:
        if isinstance(value, str):
            scopes = [item for item in value.split(",") if item]
        elif isinstance(value, list):
            scopes = [str(item) for item in value if str(item)]
        else:
            raise ValueError("invalid provider scope")
        if not scopes or any(len(scope) > 128 for scope in scopes):
            raise ValueError("invalid provider scope")
        return scopes
