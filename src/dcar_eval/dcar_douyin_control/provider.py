from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import urlencode

import httpx

from .config import DouyinControlConfig
from .crypto import read_shared_key


@dataclass(frozen=True)
class TokenBundle:
    open_id: str
    access_token: str
    refresh_token: str
    access_expires_at: int
    refresh_expires_at: int
    scopes: list[str]


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
            raise RuntimeError("mock OAuth client cannot run while authorization is disabled")
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
