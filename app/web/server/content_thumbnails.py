"""Read saved online cover URLs. No network, media reads, capture or disk cache.

Content details and account post pages share the existing bounded raw reader.
CAS bytes use SQLite blob receipts; the isolated helper does not import the
writer's storage/provider modules or restore retired raw responses.
"""
from __future__ import annotations

import argparse
import codecs
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
from urllib.parse import urlsplit

sys.dont_write_bytecode = True

MAX_IDS = 100
MAX_STORED = 16 * 1024 * 1024
MAX_ENTITY = 64 * 1024 * 1024
MAX_TOTAL = 64 * 1024 * 1024
MAX_RAW_ROWS = 1000
MAX_DETAIL_ROWS_PER_CONTENT = 4
MAX_COVER_URLS = 3
MAX_COVER_URL_LENGTH = 4096
DETAIL_OPERATIONS = {"douyin": "douyin_video_detail", "xiaohongshu": "xiaohongshu_note_detail",
                     "kuaishou": "kuaishou_video_detail", "wechat_channels": "wechat_channels_video_detail"}
POST_OPERATIONS = {platform: platform + "_user_posts" for platform in DETAIL_OPERATIONS}


class ReadError(ValueError):
    pass


def _project_name(value: object) -> str:
    # Same file namespace as artifact_paths._project_path, without allocating
    # a PurePosixPath and all its parents for every manifest entry.
    if not isinstance(value, str) or not value or any(c in value for c in ("\0", "\n", "\r", "\\")):
        raise ReadError("invalid snapshot file name")
    if any(p in ("", ".", "..") for p in value.split("/")) or not value.startswith(("data/cache/", "reports/")):
        raise ReadError("invalid snapshot file namespace")
    return value


def _snapshot_manifest(path: Path, expected_sha: str, artifact_paths):
    """Verify every manifest byte while retaining only exact file receipts.

    A production snapshot can contain hundreds of thousands of files. The
    API's complete directory index is unnecessary for this file-only reader.
    Streaming the two file arrays avoids a full bytes + text + object copy.
    """
    before = artifact_paths._identity(path)
    if before[2] > artifact_paths.MAX_SNAPSHOT_MANIFEST_BYTES:
        raise ReadError("snapshot manifest oversized")
    digest = hashlib.sha256()
    def unique_object(pairs):
        result = dict(pairs)
        if len(result) != len(pairs):
            raise ReadError("repeated snapshot object key")
        return result

    def invalid_constant(_value):
        raise ReadError("invalid snapshot JSON constant")

    decoder = json.JSONDecoder(object_pairs_hook=unique_object, parse_constant=invalid_constant)
    utf8 = codecs.getincrementaldecoder("utf-8")()
    buffer, offset, eof, consumed = "", 0, False, 0
    files, metadata = {}, {}
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as source:
        def fill():
            nonlocal buffer, offset, eof, consumed
            block = source.read(64 * 1024)
            consumed += len(block)
            if consumed > artifact_paths.MAX_SNAPSHOT_MANIFEST_BYTES:
                raise ReadError("snapshot manifest exceeded byte limit")
            digest.update(block)
            eof = not block
            buffer = buffer[offset:] + utf8.decode(block, final=eof)
            offset = 0

        def peek():
            nonlocal offset
            while True:
                while offset < len(buffer) and buffer[offset] in " \t\r\n":
                    offset += 1
                if offset < len(buffer) or eof:
                    return buffer[offset:offset + 1]
                fill()

        def token(expected):
            nonlocal offset
            if peek() != expected:
                raise ReadError("invalid snapshot JSON structure")
            offset += 1

        def value():
            nonlocal offset
            peek()
            while True:
                try:
                    result, end = decoder.raw_decode(buffer, offset)
                    # A number at a chunk boundary may continue in the next
                    # block; do not accept a truncated scalar prefix.
                    if not eof and (end == len(buffer) or (type(result) in (int, float) and buffer[end:end + 1] in (".", "e", "E"))):
                        fill()
                        continue
                    offset = end
                    return result
                except json.JSONDecodeError:
                    if eof:
                        raise ReadError("invalid snapshot JSON value") from None
                    fill()

        token("{")
        while peek() != "}":
            key = value()
            if not isinstance(key, str) or key in metadata:
                raise ReadError("invalid or repeated snapshot key")
            token(":")
            if key in ("files", "optional_reuse_files"):
                metadata[key] = []
                token("[")
                while peek() != "]":
                    row = value()
                    if not isinstance(row, dict):
                        raise ReadError("invalid snapshot file receipt")
                    name = _project_name(row.get("project_path"))
                    sha, size = row.get("sha256"), row.get("byte_size")
                    if name in files or not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{64}", sha) or type(size) is not int or size < 0:
                        raise ReadError("invalid snapshot file identity")
                    files[name] = (sha, size)
                    if key == "files" and name.startswith(artifact_paths.RUNTIME_EVIDENCE_DIRECTORY + "/"):
                        metadata[key].append({"project_path": name, "sha256": sha, "byte_size": size})
                    if peek() == "]":
                        break
                    token(",")
                    if peek() == "]":
                        raise ReadError("invalid trailing snapshot comma")
                token("]")
            else:
                metadata[key] = value()
            if peek() == "}":
                break
            token(",")
            if peek() == "}":
                raise ReadError("invalid trailing snapshot comma")
        token("}")
        if peek() or before != artifact_paths._identity(path) or digest.hexdigest() != expected_sha:
            raise ReadError("snapshot manifest changed or hash mismatch")
    return metadata, files


def _replica_context(project_root: Path):
    # -I ignores PYTHONPATH: load only the configured, trusted release.
    sys.path.insert(0, str(project_root / "src/dcar_eval"))
    from v8 import artifact_paths
    from v8.snapshot_contract import ARTIFACT_POLICY, MANAGED_ORIGINALS_CONTRACT, validate_descriptor

    receipt_path = Path(os.environ.get("DCAR_ACTIVE_SNAPSHOT", str(artifact_paths.ACTIVE_SNAPSHOT)))
    if not receipt_path.is_absolute():
        raise ReadError("invalid replica receipt path")
    identity = artifact_paths._identity(receipt_path)
    receipt = artifact_paths._object(receipt_path, maximum_bytes=artifact_paths.MAX_SNAPSHOT_RECEIPT_BYTES)
    snapshot_id, digest = receipt.get("snapshot_id"), receipt.get("manifest_sha256")
    if not isinstance(snapshot_id, str) or artifact_paths._SNAPSHOT.fullmatch(snapshot_id) is None or not isinstance(digest, str) or artifact_paths._SHA.fullmatch(digest) is None:
        raise ReadError("invalid replica snapshot identity")
    manifest_path = receipt_path.parent / "snapshot-history" / snapshot_id / "manifest.json"
    if receipt.get("manifest_path") != str(manifest_path):
        raise ReadError("invalid replica manifest path")
    manifest, files = _snapshot_manifest(manifest_path, digest, artifact_paths)
    if identity != artifact_paths._identity(receipt_path):
        raise ReadError("replica receipt changed")
    if ("files" not in manifest or manifest.get("schema") != "dcar-read-replica-snapshot-v2" or manifest.get("snapshot_id") != snapshot_id
            or manifest.get("artifact_policy") != ARTIFACT_POLICY or receipt.get("artifact_policy") != ARTIFACT_POLICY
            or manifest.get("runtime_identity") != receipt.get("runtime_identity")):
        raise ReadError("replica manifest binding mismatch")
    databases = {row["name"]: row["sha256"] for row in manifest.get("databases", [])}
    if not databases or databases != receipt.get("database_sha256"):
        raise ReadError("replica database binding mismatch")
    validate_descriptor(manifest.get("snapshot_contract"))
    writer = Path(str(manifest.get("writer_project_root", "")))
    if not writer.is_absolute() or ".." in writer.parts or len(writer.parts) < 3 or receipt.get("writer_project_root") != str(writer):
        raise ReadError("invalid replica writer root")
    originals = manifest.get("managed_originals")
    if not isinstance(originals, dict) or originals.get("contract_version") != MANAGED_ORIGINALS_CONTRACT or not isinstance(originals.get("bundles"), list):
        raise ReadError("missing managed originals contract")
    members = set()
    for bundle in originals["bundles"]:
        for row in bundle["members"]:
            name = _project_name(row["project_path"])
            if name in files or name in members:
                raise ReadError("managed original was transferred")
            members.add(name)
    # The shared validator sees only the retained required runtime-evidence
    # subset; optional reuse must never authorize a runtime alias.
    aliases = artifact_paths.runtime_evidence_aliases(manifest)
    return {"writer_root": writer, "files": files, "aliases": aliases}


def parse_ids(value: str) -> list[int]:
    parts = value.split(",")
    if not 1 <= len(parts) <= MAX_IDS or any(not re.fullmatch(r"[1-9][0-9]{0,17}", p) for p in parts):
        raise ReadError("ids must contain 1 to 100 positive integers")
    return list(dict.fromkeys(int(p) for p in parts))


def _read(path: Path, limit: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= limit:
            raise ReadError("invalid evidence file")
        body = handle.read(limit + 1)
        if len(body) != info.st_size:
            raise ReadError("evidence changed")
        return body


def _checked(body: bytes, sha: object, size: object) -> None:
    if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{64}", sha):
        raise ReadError("missing receipt")
    if type(size) is not int or len(body) != size or hashlib.sha256(body).hexdigest() != sha:
        raise ReadError("receipt mismatch")


class Reader:
    """Each file is decoded once per invocation, including failures."""
    def __init__(self, project_root: Path | None = None, *, blobs: dict[int, dict] | None = None) -> None:
        self.root = (project_root or Path.cwd()).resolve()
        self.cache: dict[tuple, object] = {}
        self.used = 0
        self.replica = None
        self.receipts: dict[Path, dict] = {}
        self.files: dict[Path, bytes] = {}
        self.blobs = blobs if blobs is not None else {}
        if os.environ.get("DCAR_READ_ONLY", "0").strip() == "1":
            self.replica = _replica_context(self.root)

    def path(self, value: str) -> Path:
        if self.replica is not None:
            requested = Path(value)
            alias = self.replica["aliases"].get(str(requested))
            if alias is not None:
                name = alias["project_path"]
            elif requested.is_absolute():
                prefix = next((root for root in (self.replica["writer_root"], self.root) if requested.is_relative_to(root)), None)
                if prefix is None:
                    raise ReadError("replica file outside authorized roots")
                name = _project_name(requested.relative_to(prefix).as_posix())
            else:
                name = _project_name(value)
            path = self.root / name
            stored = self.replica["files"].get(name)
            if stored is None or path != path.resolve(strict=True):
                raise ReadError("unlisted or aliased replica file")
            self.receipts[path] = {"sha256": stored[0], "byte_size": stored[1]}
            return path
        path = Path(value)
        return path if path.is_absolute() else self.root / path

    def read(self, path: Path, limit: int) -> bytes:
        if path in self.files:
            body = self.files[path]
            if len(body) > limit:
                raise ReadError("evidence exceeds limit")
            return body
        if self.used >= MAX_TOTAL:
            raise ReadError("evidence budget exhausted")
        body = _read(path, min(limit, MAX_TOTAL - self.used))
        self.used += len(body)
        if self.replica is not None:
            receipt = self.receipts.get(path)
            if receipt is None:
                raise ReadError("missing replica file receipt")
            _checked(body, receipt.get("sha256"), receipt.get("byte_size"))
        self.files[path] = body
        return body

    def compressed_entity(self, body: bytes, receipt: dict) -> bytes:
        import io
        import zstandard
        size = receipt.get("entity_size")
        if type(size) is not int or not 0 < size <= min(MAX_ENTITY, MAX_TOTAL - self.used):
            raise ReadError("raw entity exceeds limit")
        self.used += size
        # Streaming reads bound decompression even for forged frame sizes.
        with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(body)) as stream:
            entity = stream.read(size + 1)
        _checked(entity, receipt.get("entity_sha256"), size)
        return entity

    def blob_entity(self, row: dict) -> bytes:
        blob = self.blobs.get(row["raw_blob_id"])
        # Thumbnails never restore archives or fall back around retirement.
        if not blob or blob.get("hot_state") != "present":
            raise ReadError("raw blob unavailable")
        if (row["sha256"], row["byte_size"]) != (blob.get("stored_sha256"), blob.get("stored_size")):
            raise ReadError("raw response differs from blob receipt")
        codec = blob.get("codec")
        if codec not in ("identity", "zstd"):
            raise ReadError("unsupported blob codec")
        location = blob["hot_path"]
        if (str(blob.get("codec_version", "")).startswith("legacy-")
                and isinstance(row["local_path"], str)
                and row["local_path"].endswith((".json", ".json.zst"))):
            # Legacy inventory may reference an external migration copy in
            # hot_path. Keep its original, snapshot-authorized file namespace.
            location = row["local_path"]
        path = self.path(location)
        body = self.read(path, MAX_STORED)
        _checked(body, blob.get("stored_sha256"), blob.get("stored_size"))
        if codec == "zstd":
            # CAS receipts live in SQLite, not a legacy .metadata.json sidecar.
            return self.compressed_entity(body, blob)
        size = blob.get("entity_size")
        if type(size) is not int or not 0 < size <= MAX_ENTITY:
            raise ReadError("raw entity exceeds limit")
        _checked(body, blob.get("entity_sha256"), size)
        return body

    def json(self, row: dict, *, raw: bool = False) -> object:
        blob_id = row.get("raw_blob_id") if raw else None
        if blob_id is not None:
            blob = self.blobs.get(blob_id, {})
            key = ("blob", blob_id, row["local_path"], row["sha256"], row["byte_size"],
                   *(blob.get(name) for name in ("hot_path", "hot_state", "codec", "codec_version",
                     "stored_sha256", "stored_size", "entity_sha256", "entity_size")))
        else:
            key = ("file", raw, row["local_path"], row["sha256"], row["byte_size"])
        if key in self.cache:
            return self.cache[key]
        self.cache[key] = None
        if self.used >= MAX_TOTAL:
            return None
        try:
            if blob_id is not None:
                self.cache[key] = json.loads(self.blob_entity(row))
                return self.cache[key]
            path = self.path(row["local_path"])
            body = self.read(path, MAX_STORED)
            _checked(body, row["sha256"], row["byte_size"])
            if raw and path.name.endswith(".json.zst"):
                sidecar_path = self.path(str(path.with_name(path.name + ".metadata.json")))
                sidecar = json.loads(self.read(sidecar_path, 64 * 1024))
                if (not isinstance(sidecar, dict) or sidecar.get("schema") != "provider-raw-sidecar-v1"
                        or sidecar.get("codec") != "zstd" or sidecar.get("raw_filename") != path.name):
                    raise ReadError("invalid compressed receipt")
                _checked(body, sidecar.get("stored_sha256"), sidecar.get("stored_size"))
                body = self.compressed_entity(body, sidecar)
            elif path.suffix != ".json":
                raise ReadError("unsupported evidence format")
            self.cache[key] = json.loads(body)
        except (OSError, ValueError, TypeError, ImportError, RecursionError):
            pass
        except Exception as error:
            # zstandard exceptions need not be imported when all inputs are JSON.
            if error.__class__.__module__ != "zstandard.backend_c":
                raise
        return self.cache[key]


def _object(value: object) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
            return decoded if isinstance(decoded, dict) else {}
        except (ValueError, RecursionError):
            pass
    return {}


def _at(value: dict, *keys: str) -> object:
    for key in keys:
        value = value.get(key) if isinstance(value, dict) else None
    return value


def _cover_candidate(value: object, *, upgrade_http: bool = False) -> tuple[str | None, str]:
    if not isinstance(value, str) or not value:
        return None, "not_found"
    if len(value) > MAX_COVER_URL_LENGTH or re.search(r"[\s\x00-\x20\x7f\\]", value):
        return None, "source_unavailable"
    try:
        if len(value.encode("utf-8")) > MAX_COVER_URL_LENGTH:
            return None, "source_unavailable"
        parsed = urlsplit(value)
        host = parsed.hostname
        if not host or "@" in parsed.netloc:
            return None, "source_unavailable"
        if ":" in host:
            ipaddress.IPv6Address(host)
        elif len(host) > 253 or not all(re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
                                      for label in host.rstrip(".").split(".")):
            return None, "source_unavailable"
        # A trailing colon is an explicit empty port, which urlsplit treats as None.
        explicit_port = parsed.netloc.endswith(":") or parsed.port is not None
        if parsed.scheme == "http" and upgrade_http and not explicit_port:
            # Saved, identity-checked cover fields already permit arbitrary HTTPS
            # hosts. Try the same URL over TLS without a CDN inventory or network
            # request here; the browser can try another saved candidate on failure.
            value = "https:" + value[len("http:"):]
            if len(value.encode("utf-8")) > MAX_COVER_URL_LENGTH:
                return None, "source_unavailable"
        elif parsed.scheme != "https" or parsed.port not in (None, 443) or parsed.netloc.endswith(":"):
            return None, "source_unavailable"
        suffixes = "heic|heif|kvif|kpg" if upgrade_http else "heic|heif"
        if re.search(rf"\.(?:{suffixes})(?:$|[!~])", parsed.path.lower()):
            return None, "unsupported_format"
        return value, ""
    except (ValueError, UnicodeError):
        return None, "source_unavailable"


def _url_values(value: object):
    if isinstance(value, list):
        for child in value:
            yield from _url_values(child)
    else:
        yield value


def _cover_candidates(values: object, *, upgrade_http: bool = False) -> tuple[list[str], str | None]:
    known, opaque = [], []
    reasons = set()
    for value in _url_values(values):
        url, reason = _cover_candidate(value, upgrade_http=upgrade_http)
        if not url:
            reasons.add(reason)
            continue
        if url in known or url in opaque:
            continue
        group = known if re.search(r"\.(?:jpe?g|png|webp|gif|avif)(?:$|[!~])", urlsplit(url).path.lower()) else opaque
        if len(group) < MAX_COVER_URLS:
            group.append(url)
        if len(known) == MAX_COVER_URLS:
            break
    # Keep existing browser-format preference, retaining opaque URLs as fallbacks.
    urls = (known + opaque)[:MAX_COVER_URLS]
    reason = None if urls else ("unsupported_format" if "unsupported_format" in reasons else
                               "source_unavailable" if "source_unavailable" in reasons else "not_found")
    return urls, reason


def _https(value: object) -> str | None:
    urls, _ = _cover_candidates(value)
    return urls[0] if urls else None


def _matching(value: object, identity: str, platform: str):
    if isinstance(value, dict):
        keys = {"douyin": ("aweme_id",), "xiaohongshu": ("note_id", "id"),
                "kuaishou": ("photo_id",), "wechat_channels": ("id", "objectId", "object_id")}.get(platform, ())
        identities = [str(value[key]) for key in keys if value.get(key) not in (None, "")]
        if identity in identities and (platform != "wechat_channels" or len(set(identities)) == 1):
            yield value
            return
        for child in value.values():
            yield from _matching(child, identity, platform)
    elif isinstance(value, list):
        for child in value:
            yield from _matching(child, identity, platform)


def _cover_result(body: object, content: dict) -> tuple[list[str], str | None, bool]:
    reasons = set()
    matched = False
    for item in _matching(body, str(content["platform_content_id"]), content["platform"]):
        matched = True
        if content["platform"] == "douyin":
            candidates = [_at(item, "video", "cover", "url_list"), _at(item, "video", "origin_cover", "url_list")]
            images = item.get("images")
            if isinstance(images, list) and images:
                candidates.append(_at(images[0], "url_list"))
        elif content["platform"] == "xiaohongshu":
            card = _object(item.get("note_card") or item.get("note")) or item
            candidates = [_at(card, "video_info_v2", "image", "thumbnail"),
                          _at(card, "cover", "url_default"), _at(card, "cover", "url"),
                          _at(card, "cover", "url_list")]
            images = card.get("image_list") or card.get("images_list")
            if isinstance(images, list) and images:
                candidates.extend([_at(images[0], "url_default"), _at(images[0], "url")])
        elif content["platform"] == "kuaishou":
            candidates = []
            for name in ("cover_thumbnail_urls", "override_cover_thumbnail_urls", "ff_cover_thumbnail_urls"):
                values = item.get(name)
                for entry in values if isinstance(values, list) else []:
                    candidates.append(_at(entry, "url"))
        elif content["platform"] == "wechat_channels":
            media = _at(item, "objectDesc", "media")
            first = media[0] if isinstance(media, list) and media else {}
            candidates = [_at(first, "fullCoverUrl"), _at(first, "coverUrl")]
        else:
            return [], "not_found", matched
        urls, reason = _cover_candidates(candidates, upgrade_http=content["platform"] == "kuaishou")
        if urls:
            return urls, None, matched
        reasons.add(reason)
    return [], ("unsupported_format" if "unsupported_format" in reasons else
                "source_unavailable" if "source_unavailable" in reasons else "not_found"), matched


def cover_candidates(body: object, content: dict) -> tuple[list[str], str | None]:
    urls, reason, _ = _cover_result(body, content)
    return urls, reason


def cover_url(body: object, content: dict) -> str | None:
    """Compatibility for callers that only need the first saved cover."""
    urls, _ = cover_candidates(body, content)
    return urls[0] if urls else None


def _raw_blobs(connection: sqlite3.Connection, rows) -> dict[int, dict]:
    ids = {row["raw_blob_id"] for row in rows if row.get("raw_blob_id") is not None}
    if not ids:
        return {}
    marks = ",".join("?" for _ in ids)
    return {row["id"]: dict(row) for row in connection.execute(
        f"SELECT id,hot_path,hot_state,codec,codec_version,stored_sha256,stored_size,entity_sha256,entity_size "
        f"FROM provider_raw_blobs WHERE id IN ({marks})", list(ids))}


def _raw_order(row: dict) -> tuple:
    return (row.get("capture_order") or 0, row["id"])


def _online_covers(connection: sqlite3.Connection, contents: list[dict], reader: Reader) -> dict:
    ids = [content["id"] for content in contents]
    accounts = list({content["account_id"] for content in contents if content["account_id"] is not None}) or [None]
    id_marks, account_marks = ",".join("?" for _ in ids), ",".join("?" for _ in accounts)
    # Read a bounded history, not just an account's newest page: a later page
    # may omit the requested work. Exact account and work identities are
    # checked below before projecting a URL. No media artifacts are consulted.
    details = [dict(row) for row in connection.execute(f"""
        SELECT * FROM (
            SELECT r.*,julianday(r.captured_at) AS capture_order,
                ROW_NUMBER() OVER (PARTITION BY r.content_id
                    ORDER BY julianday(r.captured_at) DESC,r.id DESC) AS content_position
            FROM provider_raw_responses r WHERE r.content_id IN ({id_marks})
                AND r.operation IN ('douyin_video_detail','xiaohongshu_note_detail',
                                    'kuaishou_video_detail','wechat_channels_video_detail'))
        WHERE content_position<=? ORDER BY capture_order DESC,id DESC""",
        [*ids, MAX_DETAIL_ROWS_PER_CONTENT])]
    pages = [dict(row) for row in connection.execute(f"""
        SELECT r.*,julianday(r.captured_at) AS capture_order FROM provider_raw_responses r
        WHERE r.content_id IS NULL AND r.account_id IN ({account_marks})
            AND r.operation IN ('douyin_user_posts','xiaohongshu_user_posts',
                                'kuaishou_user_posts','wechat_channels_user_posts')
        ORDER BY capture_order DESC,r.id DESC LIMIT ?""", [*accounts, MAX_RAW_ROWS])]
    # Preserve the existing detail coverage before spending the raw byte budget
    # on account history. A newer page can still replace its older detail URL.
    rows = [*details, *pages]
    reader.blobs.update(_raw_blobs(connection, rows))
    parents: dict[int, dict | None] = {}
    selected: dict[int, tuple[tuple, list[str]]] = {}
    observations: dict[int, set[str]] = {}

    def unavailable(targets):
        for content in targets:
            observations.setdefault(content["id"], set()).add("source_unavailable")

    for row in rows:
        targets = [content for content in contents
                   if ((row["content_id"] == content["id"]
                         and row["account_id"] in (None, content["account_id"])
                         and row["operation"] == DETAIL_OPERATIONS.get(content["platform"]))
                        or (row["content_id"] is None
                            and row["account_id"] == content["account_id"]
                            and row["operation"] == POST_OPERATIONS.get(content["platform"])))
                   and (content["id"] not in selected or selected[content["id"]][0] < _raw_order(row))]
        if not targets:
            continue
        body = reader.json(row, raw=True)
        if not isinstance(body, (dict, list)):
            unavailable(targets)
            continue
        source = row
        if isinstance(body, dict) and "source_raw_response_id" in body:
            parent_id = body["source_raw_response_id"]
            if type(parent_id) is not int or parent_id <= 0:
                unavailable(targets)
                continue
            if parent_id not in parents:
                parent = connection.execute(
                    "SELECT *,julianday(captured_at) AS capture_order FROM provider_raw_responses WHERE id=?",
                    (parent_id,)).fetchone()
                parents[parent_id] = dict(parent) if parent else None
                if parent:
                    reader.blobs.update(_raw_blobs(connection, [parents[parent_id]]))
            source = parents[parent_id]
            if (source is None or source["content_id"] is not None
                    or source["account_id"] != targets[0]["account_id"]
                    or source["operation"] != POST_OPERATIONS.get(targets[0]["platform"])
                    or source["sha256"] != body.get("source_sha256")
                    or source["captured_at"] != body.get("source_captured_at")
                    or _raw_order(source) > _raw_order(row)):
                unavailable(targets)
                continue
            body = reader.json(source, raw=True)
            if not isinstance(body, (dict, list)):
                unavailable(targets)
                continue
        for content in targets:
            try:
                urls, reason, matched = _cover_result(body, content)
            except (ValueError, TypeError, RecursionError):
                unavailable((content,))
                continue
            cid, order = content["id"], _raw_order(source)
            if urls and (cid not in selected or selected[cid][0] < order):
                selected[cid] = (order, urls)
            elif reason and matched:
                # Another work on an account page cannot prove this work has no
                # cover, or mask its unreadable detail with a not_found result.
                observations.setdefault(cid, set()).add(reason)
    # Keep local_url null for older clients during a frontend rollout.
    result = {}
    for content in contents:
        cid = content["id"]
        urls = selected.get(cid, (None, []))[1]
        reasons = observations.get(cid, set())
        # A damaged response must not hide what another readable response proves.
        reason = None if urls else ("unsupported_format" if "unsupported_format" in reasons else
                                   "not_found" if not reasons or "not_found" in reasons else "source_unavailable")
        result[str(cid)] = {"local_url": None, "remote_url": urls[0] if urls else None,
                            "remote_urls": urls, "reason": reason}
    return result


def project(db_path: Path, ids: list[int], *, project_root: Path | None = None) -> dict:
    if not ids or len(ids) > MAX_IDS or any(type(i) is not int or not 0 < i < 10**18 for i in ids):
        raise ReadError("invalid ids")
    uri = db_path.resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=2) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        schema = connection.execute("PRAGMA user_version").fetchone()[0]
        if schema not in (19, 20, 21, 22, 23, 24):
            raise ReadError("unsupported content database schema")
        placeholders = ",".join("?" for _ in ids)
        canonical = ("AND NOT EXISTS (SELECT 1 FROM content_identity_merge_events m WHERE m.loser_content_id=c.id)"
                     if schema >= 20 else "")
        contents = [dict(r) for r in connection.execute(f"""SELECT c.id,c.platform,c.platform_content_id,c.account_id
            FROM content_items c WHERE c.id IN ({placeholders})
            AND NOT EXISTS (SELECT 1 FROM accounts a WHERE a.id=c.account_id AND a.enabled=0) {canonical}""", ids)]
        if not contents:
            return {"items": {}}
        return {"items": _online_covers(connection, contents, Reader(project_root))}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--ids", required=True)
    parser.add_argument("--project-root", type=Path, default=Path.cwd(),
                        help="Base directory for relative evidence paths stored in SQLite")
    args = parser.parse_args()
    try:
        result = project(args.db, parse_ids(args.ids), project_root=args.project_root)
    except (ReadError, sqlite3.Error, OSError, ValueError, RecursionError):
        print(json.dumps({"error": "thumbnail_projection_unavailable"}))
        return 1
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
