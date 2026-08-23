from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
from pathlib import Path
from typing import Any, Mapping

from cryptography.fernet import Fernet, InvalidToken, MultiFernet


def _credential_text(path: Path, label: str, *, minimum: int = 32) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not minimum <= len(value) <= 4096 or any(
        ord(character) < 33 for character in value
    ):
        raise RuntimeError(f"{label} credential has an invalid format")
    return value


def read_shared_key(path: Path, label: str) -> str:
    return _credential_text(path, label)


class TokenCipher:
    def __init__(self, keyring_path: Path, open_id_hmac_key_path: Path) -> None:
        rows = [
            row.strip()
            for row in keyring_path.read_text(encoding="utf-8").splitlines()
            if row.strip()
        ]
        if not rows or len(rows) > 10:
            raise RuntimeError("Douyin Fernet keyring must contain 1 to 10 keys")
        versions: list[int] = []
        fernets: list[Fernet] = []
        try:
            for row in rows:
                version_text, encoded_key = row.split(":", 1)
                version = int(version_text)
                if version <= 0 or version in versions:
                    raise ValueError
                versions.append(version)
                fernets.append(Fernet(encoded_key.encode("ascii")))
        except (UnicodeEncodeError, ValueError) as exc:
            raise RuntimeError("Douyin Fernet keyring is invalid") from exc
        self._fernet = MultiFernet(fernets)
        self.key_version = versions[0]

        hmac_text = _credential_text(
            open_id_hmac_key_path, "Douyin open_id HMAC"
        )
        try:
            hmac_key = base64.urlsafe_b64decode(hmac_text.encode("ascii"))
        except (UnicodeEncodeError, ValueError, binascii.Error) as exc:
            raise RuntimeError("Douyin open_id HMAC credential is invalid") from exc
        if len(hmac_key) < 32:
            raise RuntimeError("Douyin open_id HMAC credential is too short")
        self._open_id_hmac_key = hmac_key

    def encrypt(
        self, record_id: str, kind: str, payload: Mapping[str, Any]
    ) -> bytes:
        envelope = {
            "v": 1,
            "record_id": record_id,
            "kind": kind,
            "payload": dict(payload),
        }
        serialized = json.dumps(
            envelope,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return self._fernet.encrypt(serialized)

    def decrypt(
        self, record_id: str, kind: str, ciphertext: bytes
    ) -> dict[str, Any]:
        try:
            envelope = json.loads(self._fernet.decrypt(ciphertext))
        except (InvalidToken, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("Douyin token ciphertext is invalid") from exc
        if not isinstance(envelope, dict) or envelope.get("v") != 1:
            raise RuntimeError("Douyin token ciphertext envelope is invalid")
        if not hmac.compare_digest(str(envelope.get("record_id", "")), record_id):
            raise RuntimeError("Douyin token ciphertext record binding failed")
        if not hmac.compare_digest(str(envelope.get("kind", "")), kind):
            raise RuntimeError("Douyin token ciphertext kind binding failed")
        payload = envelope.get("payload")
        if not isinstance(payload, dict):
            raise RuntimeError("Douyin token ciphertext payload is invalid")
        return payload

    def rotate(self, ciphertext: bytes) -> bytes:
        try:
            return self._fernet.rotate(ciphertext)
        except InvalidToken as exc:
            raise RuntimeError("Douyin token ciphertext is invalid") from exc

    def open_id_fingerprint(self, open_id: str) -> str:
        if not open_id or len(open_id) > 256:
            raise ValueError("open_id is invalid")
        return hmac.new(
            self._open_id_hmac_key,
            open_id.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
