"""Offline, source-bound WeChat Channels video decryption.

Contract: https://docs.tikhub.io/472974842e0 and its linked reference decoder.
Use the pinned WxIsaac64 bytes; never guess a cipher from a decode_key field.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
from typing import Any, BinaryIO, Mapping

VERSION = "wechat-wxisaac64-1.2.46-v1"
PREFIX_SIZE = 131072
VENDOR_ROOT = Path(__file__).parent / "vendor" / "wechat_video_decode"


class WeChatDecryptionError(RuntimeError):
    def __init__(self, code: str):
        self.error_code = code
        super().__init__(code)


def media_material(data: Mapping[str, Any]) -> dict[str, str]:
    """Select one verified video asset without ever combining response entries."""
    entries = data.get("media_evidence")
    if (data.get("platform") != "wechat_channels" or data.get("content_type") != "video"
            or not isinstance(entries, list) or len(entries) != 1 or not isinstance(entries[0], Mapping)):
        raise WeChatDecryptionError("decryption_material_missing")
    item = entries[0]
    url, token, full, key = (item.get(name) for name in ("url", "url_token", "full_url", "decode_key"))
    if (not isinstance(url, str) or not isinstance(token, str) or full != url + token
            or not isinstance(key, str) or not re.fullmatch(r"[0-9]{1,20}", key)
            or int(key) > 2**64 - 1):
        raise WeChatDecryptionError("decryption_material_missing")
    from .media import is_supported_media_url
    if not is_supported_media_url(full):
        raise WeChatDecryptionError("decryption_material_missing")
    return {"url": full, "decode_key": key, "algorithm": VERSION}


def keystream(decode_key: str) -> bytes:
    if (type(decode_key) is not str or not re.fullmatch(r"[0-9]{1,20}", decode_key)
            or int(decode_key) > 2**64 - 1):
        raise WeChatDecryptionError("decryption_material_missing")
    node = shutil.which("node")
    if node is None:
        raise WeChatDecryptionError("decryption_runtime_unavailable")
    try:
        result = subprocess.run([node, str(VENDOR_ROOT / "keystream.cjs")], input=decode_key.encode("ascii"),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=35, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise WeChatDecryptionError("decryption_runtime_unavailable") from error
    if result.returncode or len(result.stdout) != PREFIX_SIZE:
        raise WeChatDecryptionError("decryption_runtime_unavailable")
    return result.stdout


def decrypt_spool(handle: BinaryIO, decode_key: str) -> dict[str, Any]:
    """Transform only a private temporary spool, returning hashes without secrets."""
    stream = keystream(decode_key)
    handle.seek(0)
    prefix = handle.read(PREFIX_SIZE)
    if len(prefix) < 16:
        raise WeChatDecryptionError("decryption_failed")
    plain = bytes(a ^ b for a, b in zip(prefix, stream))
    # Structural probe before modifying the private spool; full ffmpeg decoding
    # remains mandatory in the downloader before publishing a media artifact.
    if plain[4:8] != b"ftyp":
        raise WeChatDecryptionError("decryption_failed")
    encrypted = hashlib.sha256(prefix)
    for block in iter(lambda: handle.read(1024 * 1024), b""):
        encrypted.update(block)
    handle.seek(0)
    handle.write(plain)
    handle.flush()
    os.fsync(handle.fileno())
    handle.seek(0)
    digest = hashlib.sha256()
    size = 0
    for block in iter(lambda: handle.read(1024 * 1024), b""):
        digest.update(block)
        size += len(block)
    handle.seek(0)
    return {"algorithm": VERSION, "encrypted_sha256": encrypted.hexdigest(),
            "sha256": digest.hexdigest(), "byte_size": size, "header": plain[:16]}
