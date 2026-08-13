"""Stable, content-scoped HMAC identifiers for comment deduplication."""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import stat
from pathlib import Path

from .contracts import PROJECT_ROOT


DEFAULT_SALT_PATH = PROJECT_ROOT / "data/cache/.comment_hash_salt"


class CommentHasher:
    def __init__(self, salt_path: Path = DEFAULT_SALT_PATH) -> None:
        self.salt_path = salt_path
        self.salt = self._load_or_create()

    def _load_or_create(self) -> bytes:
        self.salt_path.parent.mkdir(parents=True, exist_ok=True)
        if self.salt_path.exists():
            if self.salt_path.is_symlink() or not self.salt_path.is_file():
                raise ValueError("comment hash salt must be a regular file")
            value = self.salt_path.read_bytes()
            if len(value) < 32:
                raise ValueError("comment hash salt must contain at least 32 bytes")
            mode = stat.S_IMODE(self.salt_path.stat().st_mode)
            if mode not in {0o600, 0o640}:
                if os.environ.get("DCAR_READ_ONLY") == "1":
                    raise PermissionError("comment hash salt permissions are unsafe")
                os.chmod(self.salt_path, 0o600)
            return value
        value = secrets.token_bytes(32)
        descriptor = os.open(self.salt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(value)
        return value

    def user_key(self, platform: str, content_id: str, raw_platform_user_id: str) -> str:
        if not platform or not content_id or not raw_platform_user_id:
            return ""
        message = f"{platform}\0{content_id}\0{raw_platform_user_id}".encode("utf-8", "replace")
        return "U" + hmac.new(self.salt, message, hashlib.sha256).hexdigest()
