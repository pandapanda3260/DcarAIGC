from __future__ import annotations

import ipaddress
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def normalized_base_path(value: str) -> str:
    value = value.strip()
    if value in {"", "/"}:
        return ""
    if not value.startswith("/") or value.endswith("/") or "\\" in value:
        raise ValueError(
            "DCAR_DOUYIN_PUBLIC_BASE_PATH must start with '/' and not end with '/'"
        )
    return value


def enabled(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class DouyinControlConfig:
    public_base_path: str
    vault_path: Path
    edge_key_path: Path
    machine_key_path: Path
    fernet_keyring_path: Path
    open_id_hmac_key_path: Path
    api_upstream: str = "http://127.0.0.1:8765"
    authorization_enabled: bool = False
    provider_mode: str = "disabled"
    client_key: str = ""
    client_secret_path: Path | None = None
    provider_authorize_url: str = ""
    provider_token_url: str = ""
    provider_userinfo_url: str = ""
    callback_url: str = ""
    state_ttl_seconds: int = 10 * 60
    confirmation_ttl_seconds: int = 15 * 60
    cleanup_interval_seconds: int = 60

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "public_base_path", normalized_base_path(self.public_base_path)
        )
        normalized_api_upstream = self.api_upstream.rstrip("/")
        parsed = urlsplit(normalized_api_upstream)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("api_upstream must be an absolute HTTP URL")
        object.__setattr__(self, "api_upstream", normalized_api_upstream)
        if self.provider_mode not in {"disabled", "mock"}:
            raise ValueError("stage 0 provider mode must be disabled or mock")
        if self.authorization_enabled:
            if self.provider_mode != "mock":
                raise ValueError("enabled stage 0 authorization requires mock mode")
            if not self.client_key or self.client_secret_path is None:
                raise ValueError("mock OAuth credentials are required when enabled")
            for label, value in (
                ("authorize", self.provider_authorize_url),
                ("token", self.provider_token_url),
                ("userinfo", self.provider_userinfo_url),
            ):
                self._require_loopback_url(label, value)
            callback = urlsplit(self.callback_url)
            expected_path = self.public_route("/oauth/douyin/callback")
            if (
                callback.scheme not in {"http", "https"}
                or not callback.netloc
                or callback.path != expected_path
                or callback.query
                or callback.fragment
            ):
                raise ValueError("callback_url must be the exact public callback URL")
        elif self.provider_mode != "disabled":
            raise ValueError("disabled authorization requires disabled provider mode")
        if not 60 <= self.state_ttl_seconds <= 30 * 60:
            raise ValueError("state_ttl_seconds must be between 60 and 1800")
        if not 60 <= self.confirmation_ttl_seconds <= 60 * 60:
            raise ValueError("confirmation_ttl_seconds must be between 60 and 3600")
        if not 10 <= self.cleanup_interval_seconds <= 10 * 60:
            raise ValueError("cleanup_interval_seconds must be between 10 and 600")

    @staticmethod
    def _require_loopback_url(label: str, value: str) -> None:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError(f"mock {label} URL must be absolute")
        try:
            is_loopback = ipaddress.ip_address(parsed.hostname).is_loopback
        except ValueError:
            is_loopback = parsed.hostname.lower() == "localhost"
        if not is_loopback or parsed.username or parsed.password or parsed.fragment:
            raise ValueError(f"mock {label} URL must use a loopback host")

    @classmethod
    def from_env(cls) -> "DouyinControlConfig":
        credential_root = Path(os.environ.get("CREDENTIALS_DIRECTORY", "/run/credentials"))
        state_root = PROJECT_ROOT / "runtime" / "douyin-control"
        return cls(
            public_base_path=os.environ.get(
                "DCAR_DOUYIN_PUBLIC_BASE_PATH", "/dcar"
            ),
            vault_path=Path(
                os.environ.get(
                    "DCAR_DOUYIN_VAULT",
                    str(state_root / "vault.sqlite3"),
                )
            ),
            edge_key_path=Path(
                os.environ.get(
                    "DCAR_DOUYIN_EDGE_KEY_FILE",
                    str(credential_root / "douyin-edge-key"),
                )
            ),
            machine_key_path=Path(
                os.environ.get(
                    "DCAR_DOUYIN_MACHINE_KEY_FILE",
                    str(credential_root / "douyin-machine-key"),
                )
            ),
            fernet_keyring_path=Path(
                os.environ.get(
                    "DCAR_DOUYIN_FERNET_KEYRING_FILE",
                    str(credential_root / "douyin-fernet-keyring"),
                )
            ),
            open_id_hmac_key_path=Path(
                os.environ.get(
                    "DCAR_DOUYIN_OPEN_ID_HMAC_KEY_FILE",
                    str(credential_root / "douyin-open-id-hmac-key"),
                )
            ),
            api_upstream=os.environ.get(
                "DCAR_DOUYIN_API_UPSTREAM", "http://127.0.0.1:8765"
            ),
            authorization_enabled=enabled(
                "DOUYIN_AUTHORIZATION_ENABLED", default=False
            ),
            provider_mode=os.environ.get("DCAR_DOUYIN_PROVIDER", "disabled"),
            client_key=os.environ.get("DCAR_DOUYIN_CLIENT_KEY", ""),
            client_secret_path=(
                Path(os.environ["DCAR_DOUYIN_CLIENT_SECRET_FILE"])
                if os.environ.get("DCAR_DOUYIN_CLIENT_SECRET_FILE")
                else None
            ),
            provider_authorize_url=os.environ.get(
                "DCAR_DOUYIN_PROVIDER_AUTHORIZE_URL", ""
            ),
            provider_token_url=os.environ.get(
                "DCAR_DOUYIN_PROVIDER_TOKEN_URL", ""
            ),
            provider_userinfo_url=os.environ.get(
                "DCAR_DOUYIN_PROVIDER_USERINFO_URL", ""
            ),
            callback_url=os.environ.get("DCAR_DOUYIN_CALLBACK_URL", ""),
            state_ttl_seconds=int(
                os.environ.get("DCAR_DOUYIN_STATE_TTL_SECONDS", "600")
            ),
            confirmation_ttl_seconds=int(
                os.environ.get("DCAR_DOUYIN_CONFIRMATION_TTL_SECONDS", "900")
            ),
            cleanup_interval_seconds=int(
                os.environ.get("DCAR_DOUYIN_CLEANUP_INTERVAL_SECONDS", "60")
            ),
        )

    def public_route(self, path: str) -> str:
        return f"{self.public_base_path}{path}"
