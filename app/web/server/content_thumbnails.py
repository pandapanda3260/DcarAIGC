"""Read existing thumbnail candidates. No network, capture, migration or disk cache.

Legacy image files reuse the authenticated evidence/files endpoint. Video frame
manifests have no such endpoint and deliberately fall back to saved cover URLs.
Managed originals are not projected here: their lifecycle belongs to the API.
"""
from __future__ import annotations

import argparse
import codecs
import hashlib
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
    def __init__(self, project_root: Path | None = None) -> None:
        self.root = (project_root or Path.cwd()).resolve()
        self.cache: dict[tuple, object] = {}
        self.used = 0
        self.replica = None
        self.receipts: dict[Path, dict] = {}
        self.files: dict[Path, bytes] = {}
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

    def json(self, row: dict, *, raw: bool = False) -> object:
        key = (row["local_path"], row["sha256"], row["byte_size"])
        if key in self.cache:
            return self.cache[key]
        self.cache[key] = None
        if self.used >= MAX_TOTAL:
            return None
        try:
            path = self.path(row["local_path"])
            body = self.read(path, MAX_STORED)
            _checked(body, row["sha256"], row["byte_size"])
            if raw and path.name.endswith(".json.zst"):
                import zstandard
                sidecar_path = self.path(str(path.with_name(path.name + ".metadata.json")))
                sidecar = json.loads(self.read(sidecar_path, 64 * 1024))
                if (not isinstance(sidecar, dict) or sidecar.get("schema") != "provider-raw-sidecar-v1"
                        or sidecar.get("codec") != "zstd" or sidecar.get("raw_filename") != path.name):
                    raise ReadError("invalid compressed receipt")
                _checked(body, sidecar.get("stored_sha256"), sidecar.get("stored_size"))
                size = sidecar.get("entity_size")
                if type(size) is not int or not 0 < size <= min(MAX_ENTITY, MAX_TOTAL - self.used):
                    raise ReadError("raw entity exceeds limit")
                self.used += size
                # Streaming reads bound decompression even for forged frame sizes.
                import io
                with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(body)) as stream:
                    body = stream.read(size + 1)
                _checked(body, sidecar.get("entity_sha256"), size)
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


def _image(path: Path, reader: Reader) -> bool:
    if "contact-sheet" in path.name.lower():
        return False
    try:
        # No image decode, transformed file, or whole video read is needed.
        if reader.replica is not None:
            header = reader.read(path, MAX_STORED)[:16]
            regular = True
        else:
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(descriptor, "rb") as handle:
                info = os.fstat(handle.fileno())
                header = handle.read(16)
            regular = stat.S_ISREG(info.st_mode) and info.st_size > 0
        return regular and (
            header.startswith((b"\xff\xd8\xff", b"\x89PNG\r\n\x1a\n", b"GIF87a", b"GIF89a"))
            or (header.startswith(b"RIFF") and header[8:12] == b"WEBP")
        )
    except (OSError, ValueError, TypeError):
        return False


def local_url(content: dict, artifacts: list[dict], reader: Reader) -> str | None:
    # Do not let a legacy record bypass a managed source's expiry or replacement.
    if any(r["artifact_type"] == "media_lifecycle_manifest" or
           _object(r["metadata_json"]).get("media_lifecycle") for r in artifacts):
        return None
    latest: dict[str, dict] = {}
    for row in artifacts:
        latest.setdefault(row["artifact_type"], row)
    source = latest.get("media_source")
    source_sha = _object(source["metadata_json"]).get("source_sha256") if source else None
    for kind in ("media_manifest", "media"):
        row = latest.get(kind)
        if not row or row["status"] != "available":
            continue
        if source and (source["status"] != "available" or not source_sha or
                       _object(row["metadata_json"]).get("source_sha256") != source_sha):
            continue
        path = reader.path(row["local_path"])
        if path.suffix.lower() != ".json":
            if kind == "media" and _image(path, reader):
                return f"/api/v8/contents/{content['id']}/evidence/files/{row['id']}/0"
            continue
        if reader.replica is not None:
            # The read-only API rejects legacy JSON bundles without a
            # per-child SQLite receipt. An otherwise valid local image would
            # still return HTTP 410; retain the saved remote cover instead.
            continue
        body = _object(reader.json(row))
        image_paths = body.get("image_paths", [])
        if not isinstance(image_paths, list):
            continue
        offset = 1 if body.get("video_path") else 0
        for index, candidate in enumerate(p for p in image_paths if isinstance(p, str)):
            child = reader.path(candidate)
            # A registered manifest cannot redirect this projection elsewhere.
            try:
                child.resolve().relative_to(path.parent.resolve())
            except (ValueError, OSError):
                continue
            if _image(child, reader):
                return f"/api/v8/contents/{content['id']}/evidence/files/{row['id']}/{index + offset}"
    return None


def _at(value: dict, *keys: str) -> object:
    for key in keys:
        value = value.get(key) if isinstance(value, dict) else None
    return value


def _https(value: object) -> str | None:
    if isinstance(value, list):
        urls = [url for entry in value if (url := _https(entry))]
        # Providers may list HEIC before a separately signed JPEG. Select an
        # existing browser-friendly URL verbatim; never rewrite its signature.
        return next((url for url in urls if re.search(
            r"\.(?:jpe?g|png|webp|gif|avif)(?:$|[!~])", urlsplit(url).path.lower()
        )), urls[0] if urls else None)
    if not isinstance(value, str) or len(value) > 8192:
        return None
    try:
        parsed = urlsplit(value)
        if (parsed.scheme == "https" and parsed.hostname and not parsed.username and not parsed.password
                and not re.search(r"\.(?:heic|heif)(?:$|[!~])", parsed.path.lower())):
            return value
    except ValueError:
        pass
    return None


def _matching(value: object, identity: str, platform: str):
    if isinstance(value, dict):
        keys = ("aweme_id",) if platform == "douyin" else ("note_id", "id")
        if any(str(value.get(key, "")) == identity for key in keys):
            yield value
            return
        for child in value.values():
            yield from _matching(child, identity, platform)
    elif isinstance(value, list):
        for child in value:
            yield from _matching(child, identity, platform)


def cover_url(body: object, content: dict) -> str | None:
    for item in _matching(body, str(content["platform_content_id"]), content["platform"]):
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
        else:
            return None
        if result := _https(candidates):
            return result
    return None


def project(db_path: Path, ids: list[int], *, project_root: Path | None = None) -> dict:
    if not ids or len(ids) > MAX_IDS or any(type(i) is not int or not 0 < i < 10**18 for i in ids):
        raise ReadError("invalid ids")
    uri = db_path.resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=2) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        schema = connection.execute("PRAGMA user_version").fetchone()[0]
        if schema not in (19, 20, 21):
            raise ReadError("unsupported content database schema")
        placeholders = ",".join("?" for _ in ids)
        canonical = ("AND NOT EXISTS (SELECT 1 FROM content_identity_merge_events m WHERE m.loser_content_id=c.id)"
                     if schema in (20, 21) else "")
        contents = [dict(r) for r in connection.execute(f"""SELECT c.id,c.platform,c.platform_content_id,c.account_id
            FROM content_items c WHERE c.id IN ({placeholders})
            AND NOT EXISTS (SELECT 1 FROM accounts a WHERE a.id=c.account_id AND a.enabled=0) {canonical}""", ids)]
        allowed = [c["id"] for c in contents]
        if not allowed:
            return {"items": {}}
        placeholders = ",".join("?" for _ in allowed)
        rows = connection.execute(f"SELECT * FROM evidence_artifacts WHERE content_id IN ({placeholders}) ORDER BY id DESC", allowed)
        artifacts = {cid: [] for cid in allowed}
        for row in rows:
            artifacts[row["content_id"]].append(dict(row))
        raw_rows = connection.execute(f"""SELECT * FROM (
            SELECT r.*,ROW_NUMBER() OVER(PARTITION BY content_id ORDER BY id DESC) AS n
            FROM provider_raw_responses r WHERE content_id IN ({placeholders})
            AND operation IN ('douyin_video_detail','xiaohongshu_note_detail')) WHERE n=1""", allowed)
        raws = {row["content_id"]: dict(row) for row in raw_rows}
        reader = Reader(project_root)
        decoded = {cid: reader.json(row, raw=True) for cid, row in raws.items()}
        parent_ids = {b["source_raw_response_id"] for b in decoded.values()
                      if isinstance(b, dict) and type(b.get("source_raw_response_id")) is int and b["source_raw_response_id"] > 0}
        parents = {}
        if parent_ids:
            marks = ",".join("?" for _ in parent_ids)
            parents = {row["id"]: dict(row) for row in connection.execute(
                f"SELECT * FROM provider_raw_responses WHERE id IN ({marks})", list(parent_ids))}
        items = {}
        for content in contents:
            cid = content["id"]
            body = decoded.get(cid)
            if isinstance(body, dict) and body.get("source_raw_response_id"):
                parent = parents.get(body["source_raw_response_id"])
                if (parent and parent["account_id"] == content["account_id"] and
                        parent["sha256"] == body.get("source_sha256") and
                        parent["captured_at"] == body.get("source_captured_at")):
                    body = reader.json(parent, raw=True)
                else:
                    body = None
            try:
                local = local_url(content, artifacts[cid], reader)
            except (OSError, ValueError, TypeError, RecursionError):
                local = None
            try:
                remote = cover_url(body, content)
            except (ValueError, TypeError, RecursionError):
                remote = None
            items[str(cid)] = {"local_url": local, "remote_url": remote}
        return {"items": items}


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
