"""Seal immutable media-completion evidence; never archive or delete originals."""

from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
import stat
import uuid
from pathlib import Path
from typing import Any, Mapping

from PIL import Image, ImageOps

from . import media, media_lifecycle as lifecycle
from .media_policy import POLICY
from .source_routing import parse_time
from .storage import DEFAULT_DB, connect, now_utc, transaction

COMPLETION_VERSION = "media-completion-v1"
PREVIEW_VERSION = "media-preview-v1"
PREVIEW_MAX_EDGE = int(POLICY["preview"]["maximum_edge"])
PREVIEW_JPEG_QUALITY = int(POLICY["preview"]["quality"])
PREVIEW_MAX_BYTES = int(POLICY["preview"]["maximum_bytes"])
_KEEP_ARTIFACT_FIELDS = (
    "id", "content_id", "artifact_type", "local_path", "byte_size",
    "sha256", "processor_version", "captured_at",
)


class CompletionBlocked(RuntimeError):
    """A stable, safe-to-display reason why original bytes cannot be released."""


def _require(condition: Any, reason: str) -> None:
    if not condition:
        raise CompletionBlocked(reason)


def _canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode()


def _object(value: Any, reason: str) -> dict[str, Any]:
    if type(value) is str:
        try:
            value = json.loads(value)
        except (ValueError, TypeError) as error:
            raise CompletionBlocked(reason) from error
    _require(type(value) is dict, reason)
    return value


def _positive_get(connection: sqlite3.Connection, table: str, identity: int | str, reason: str) -> dict[str, Any]:
    row = connection.execute(f"SELECT * FROM {table} WHERE id=?", (identity,)).fetchone()
    _require(row is not None, reason)
    return dict(row)


def _path(value: str) -> Path:
    path = media._resolved(value)
    _require(".." not in path.parts, "evidence_path_unsafe")
    for candidate in (path, *path.parents):
        _require(not candidate.is_symlink(), "evidence_path_symlink")
    return path.absolute()


def _file_ref(path: Path, *, role: str, sha256: str | None = None, byte_size: int | None = None) -> dict[str, Any]:
    path = _path(str(path))
    evidence = media._read_private_file_evidence(path, label="completion " + role)
    _require(sha256 is None or evidence.sha256 == sha256, role + "_sha256_mismatch")
    _require(byte_size is None or evidence.byte_size == byte_size, role + "_byte_size_mismatch")
    return {"role": role, "path": media._relative(path), "sha256": evidence.sha256, "byte_size": evidence.byte_size}


def _read_object(reference: Mapping[str, Any]) -> dict[str, Any]:
    path = _path(str(reference["path"]))
    evidence = media._read_private_file_evidence(path, label="completion JSON", capture_body=True)
    _require(evidence.sha256 == reference["sha256"] and evidence.byte_size == reference["byte_size"], "json_file_changed")
    try:
        return _object(json.loads(evidence.body or b""), "json_object_required")
    except (ValueError, TypeError) as error:
        raise CompletionBlocked("json_invalid") from error


def _artifact_ref(connection: sqlite3.Connection, artifact_id: int, content_id: int, *, role: str,
                  artifact_type: str | None = None, evidence_root: Path | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    row = _positive_get(connection, "evidence_artifacts", artifact_id, role + "_artifact_missing")
    _require(row["content_id"] == content_id and row["status"] == "available", role + "_artifact_not_available")
    _require(artifact_type is None or row["artifact_type"] == artifact_type, role + "_artifact_type_mismatch")
    _require(media._valid_sha256(row["sha256"]) and type(row["byte_size"]) is int and row["byte_size"] > 0, role + "_artifact_identity_invalid")
    path = _path(str(row["local_path"]))
    if evidence_root is not None:
        _require(path.is_relative_to(evidence_root), role + "_not_bundle_version_bound")
    reference = _file_ref(path, role=role, sha256=row["sha256"], byte_size=row["byte_size"])
    return {key: row[key] for key in _KEEP_ARTIFACT_FIELDS}, {**reference, "artifact_id": artifact_id}


def _make_private_directory(path: Path, *, root: Path) -> None:
    _require(path.is_relative_to(root), "completion_output_outside_bundle")
    _path(str(root))
    _require(root.is_dir(), "bundle_evidence_root_missing")
    cursor = root
    for part in path.relative_to(root).parts:
        cursor = cursor / part
        try:
            cursor.mkdir(mode=0o700)
        except FileExistsError:
            _require(cursor.is_dir() and not cursor.is_symlink(), "completion_output_directory_unsafe")
        info = cursor.lstat()
        _require(stat.S_IMODE(info.st_mode) == 0o700 and info.st_uid == os.getuid(), "completion_output_directory_not_private")
    _path(str(path))


def _publish(path: Path, body: bytes, *, root: Path) -> dict[str, Any]:
    """Publish only our private staging inode using the media no-clobber rename."""
    _make_private_directory(path.parent, root=root)
    digest = hashlib.sha256(body).hexdigest()
    if os.path.lexists(path):
        return _file_ref(path, role="completion_output", sha256=digest, byte_size=len(body))
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    temporary_name = "." + uuid.uuid4().hex + ".tmp"
    descriptor = os.open(temporary_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        media._rename_exclusive_at(directory, temporary_name, path.name)
        os.fsync(directory)
    finally:
        os.close(directory)
    return _file_ref(path, role="completion_output", sha256=digest, byte_size=len(body))


_TABLES = frozenset({
    "evidence_artifacts", "provider_raw_responses", "media_processing_slots",
    "evidence_envelopes", "evaluation_versions", "evaluation_releases", "duplicate_fingerprints",
})
_SLOT_FIELDS = ("id", "content_id", "source_sha256", "processor_type", "processor_version", "status", "output_artifact_id", "attempt_count")


def _row_ref(table: str, row: Mapping[str, Any], fields: tuple[str, ...] | None = None) -> dict[str, Any]:
    _require(table in _TABLES, "completion_reference_table_invalid")
    values = dict(row) if fields is None else {key: row[key] for key in fields}
    return {"table": table, "id": row["id"], "values": values,
            "sha256": hashlib.sha256(_canonical(values)).hexdigest()}


def _check_row_ref(connection: sqlite3.Connection, reference: Mapping[str, Any]) -> None:
    table = reference.get("table")
    _require(table in _TABLES, "completion_reference_table_invalid")
    values = _object(reference.get("values"), "completion_reference_invalid")
    _require(hashlib.sha256(_canonical(values)).hexdigest() == reference.get("sha256"), "completion_reference_hash_invalid")
    actual = _positive_get(connection, str(table), reference["id"], "retained_database_reference_missing")
    _require(all(key in actual and actual[key] == value for key, value in values.items()), "retained_database_reference_changed")


def _bound_artifact(connection: sqlite3.Connection, bundle: Mapping[str, Any], *, kind: str,
                    version: str, input_sha256: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    manifest = bundle["manifest"]
    rows = connection.execute(
        "SELECT * FROM evidence_artifacts WHERE content_id=? AND artifact_type=? "
        "AND json_valid(metadata_json) AND json_extract(metadata_json,'$.media_lifecycle.bundle_id')=? "
        "AND processor_version=? ORDER BY id DESC",
        (manifest["content_id"], kind, manifest["bundle_id"], version),
    ).fetchall()
    _require(bool(rows), kind + "_artifact_missing")
    row = dict(rows[0])
    metadata = _object(row["metadata_json"], kind + "_metadata_invalid")
    expected = media.managed_evidence_metadata(bundle, source_sha256=input_sha256, processor_version=version)["media_lifecycle"]
    _require(metadata.get("media_lifecycle") == expected, kind + "_source_binding_changed")
    _, reference = _artifact_ref(connection, row["id"], manifest["content_id"], role=kind,
                                 artifact_type=kind, evidence_root=Path(bundle["evidence_root"]))
    return row, reference, _read_object(reference)


def _slot(connection: sqlite3.Connection, bundle: Mapping[str, Any], artifact: Mapping[str, Any],
          *, processor: str, input_sha256: str) -> dict[str, Any]:
    rows = connection.execute(
        "SELECT * FROM media_processing_slots WHERE content_id=? AND processor_type=? AND processor_version=? AND source_sha256=?",
        (bundle["manifest"]["content_id"], processor, artifact["processor_version"], media.managed_slot_source(bundle, input_sha256)),
    ).fetchall()
    _require(len(rows) == 1 and rows[0]["status"] == "succeeded"
             and rows[0]["output_artifact_id"] == artifact["id"] and rows[0]["attempt_count"] > 0,
             processor + "_slot_not_complete")
    return _row_ref("media_processing_slots", dict(rows[0]), _SLOT_FIELDS)


def _source_raw_dependencies(connection: sqlite3.Connection, bundle: Mapping[str, Any], raw: Mapping[str, Any],
                             source_body: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None]:
    """Freeze exact discovery ancestry without requiring this to be the newest source."""
    manifest = bundle["manifest"]
    content_id, account_id = manifest["content_id"], manifest["account_id"]
    extra_files: list[dict[str, Any]] = []
    extra_refs: list[dict[str, Any]] = []
    if manifest["platform"] != "douyin" or manifest["media_kind"] != "image":
        _require(raw["content_id"] == content_id and raw["account_id"] in {None, account_id}, "source_raw_binding_changed")
        return extra_files, extra_refs, None
    urls = source_body.get("urls")
    if not isinstance(urls, list) or not urls or any(type(value) is not str for value in urls):
        raise CompletionBlocked("source_urls_invalid")
    uid, aweme_id = manifest["account_uid"], manifest["platform_content_id"]
    _require(type(uid) is str and bool(uid) and type(aweme_id) is str and bool(aweme_id), "source_author_identity_missing")
    if account_id is not None:
        identities = connection.execute("SELECT uid FROM account_platform_identities WHERE account_id=? AND platform='douyin'", (account_id,)).fetchall()
        _require(len(identities) == 1 and identities[0]["uid"] == uid, "source_account_identity_changed")
    if raw["operation"] == "douyin_video_detail":
        _require(raw["content_id"] == content_id and raw["account_id"] in {None, account_id}, "source_raw_binding_changed")
    elif raw["operation"] == "douyin_user_posts":
        _require(account_id is not None and raw["account_id"] == account_id and raw["content_id"] is None, "source_raw_binding_changed")
    else:
        raise CompletionBlocked("source_raw_operation_unsupported")
    raw_row = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (raw["id"],)).fetchone()
    body = media._read_douyin_group_raw(raw_row, connection=connection)
    if "derived_from_operation" in body:
        normalized, source_id = body.get("data"), body.get("source_raw_response_id")
        _require(set(body) == {"stage", "data", "derived_from_operation", "source_raw_response_id", "source_sha256", "source_captured_at"}
                 and raw["operation"] == "douyin_video_detail" and body.get("stage") == "detail"
                 and body.get("derived_from_operation") == "douyin_user_posts"
                 and type(source_id) is int and source_id > 0 and source_id != raw["id"]
                 and isinstance(normalized, dict) and normalized.get("content_type") == "image"
                 and normalized.get("account_uid") == uid and normalized.get("media_urls") == urls, "derived_source_raw_binding_changed")
        discovery = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (source_id,)).fetchone()
        _require(discovery is not None and account_id is not None and discovery["account_id"] == account_id
                 and discovery["content_id"] is None and discovery["operation"] == "douyin_user_posts"
                 and discovery["sha256"] == body["source_sha256"] and discovery["captured_at"] == body["source_captured_at"], "derived_source_raw_binding_changed")
        body = media._read_douyin_group_raw(discovery, connection=connection)
        extra_refs.append(_row_ref("provider_raw_responses", dict(discovery)))
        extra_files.append(_file_ref(_path(discovery["local_path"]), role="source_discovery_raw",
                                     sha256=discovery["sha256"], byte_size=discovery["byte_size"]))
    groups = media._raw_douyin_image_groups(body, urls=urls, aweme_id=aweme_id, author_uid=uid)
    return extra_files, extra_refs, media.image_groups_sha256(groups)


def _source_and_originals(connection: sqlite3.Connection, bundle: Mapping[str, Any]) -> dict[str, Any]:
    manifest = bundle["manifest"]
    content_id = manifest["content_id"]
    content = _positive_get(connection, "content_items", content_id, "content_missing")
    for key, frozen in (("link_id", "link_id"), ("platform", "platform"), ("platform_content_id", "platform_content_id"),
                        ("account_id", "account_id"), ("raw_account_uid", "account_uid")):
        _require(content[key] == manifest[frozen], "completion_content_identity_changed")
    source = _positive_get(connection, "evidence_artifacts", manifest["source"]["artifact_id"], "source_artifact_missing")
    _require(lifecycle._artifact_identity(source) == {key: manifest["source"][key] for key in lifecycle._artifact_identity(source)}, "source_artifact_identity_changed")
    source_metadata = _object(source["metadata_json"], "source_metadata_invalid")
    _require(source_metadata.get("media_kind") == manifest["media_kind"]
             and source_metadata.get("raw_response_id") == manifest["source"]["raw_response_id"], "source_binding_changed")
    _, source_file = _artifact_ref(connection, source["id"], content_id, role="media_source", artifact_type="media_source")
    source_body = _read_object(source_file)
    _require(source_body.get("media_kind") == manifest["media_kind"], "source_kind_changed")
    raw_id = manifest["source"].get("raw_response_id")
    _require(type(raw_id) is int and raw_id > 0, "source_raw_missing")
    raw = _positive_get(connection, "provider_raw_responses", raw_id, "source_raw_missing")
    _require(raw["sha256"] == manifest["source"].get("raw_sha256"), "source_raw_binding_changed")
    raw_file = _file_ref(_path(raw["local_path"]), role="source_raw", sha256=raw["sha256"], byte_size=raw["byte_size"])
    extra_files, extra_refs, group_sha = _source_raw_dependencies(connection, bundle, raw, source_body)
    original = lifecycle.original_artifact(connection, bundle)
    _require(original["status"] == "available", "original_not_available")
    _require(lifecycle._download_members({"media_kind": manifest["media_kind"],
              "originals_root": bundle["originals_root"], "evidence_root": bundle["evidence_root"]}, original) == manifest["members"], "original_members_changed")
    download = _positive_get(connection, "media_processing_slots", manifest["download_slot"]["id"], "download_slot_missing")
    _require(download["content_id"] == content_id and download["processor_type"] == "download"
             and download["status"] == "succeeded" and download["attempt_count"] > 0
             and download["output_artifact_id"] == original["id"]
             and download["source_sha256"] == manifest["download_slot"]["source_sha256"]
             and download["processor_version"] == manifest["download_slot"]["processor_version"], "download_slot_binding_changed")
    _require(connection.execute("SELECT 1 FROM media_processing_slots WHERE content_id=? AND status='running' LIMIT 1", (content_id,)).fetchone() is None,
             "media_processing_in_flight")
    files = [source_file, raw_file, *extra_files]
    if manifest["media_kind"] == "image":
        original_manifest = _file_ref(_path(original["local_path"]), role="download_manifest", sha256=original["sha256"], byte_size=original["byte_size"])
        if group_sha is not None:
            _require(_read_object(original_manifest).get("image_groups_sha256") == group_sha, "source_image_groups_changed")
        files.append(original_manifest)
    refs = [_row_ref("evidence_artifacts", source, (*_KEEP_ARTIFACT_FIELDS, "metadata_json", "created_at")),
            _row_ref("provider_raw_responses", raw),
            _row_ref("evidence_artifacts", original, (*_KEEP_ARTIFACT_FIELDS, "created_at")),
            _row_ref("media_processing_slots", download, _SLOT_FIELDS), *extra_refs]
    return {"content": content, "original": original, "files": files, "database_refs": refs}


def _processing_evidence(connection: sqlite3.Connection, bundle: Mapping[str, Any]) -> dict[str, Any]:
    manifest = bundle["manifest"]
    original_sha = manifest["original_artifact"]["sha256"]
    versions = media.processor_versions()
    files: list[dict[str, Any]] = []
    refs: list[dict[str, Any]] = []
    asr: dict[str, Any] | None = None
    frame_files: list[dict[str, Any]] = []
    applicability: dict[str, str] = {"asr": "not_applicable_image", "frames": "not_applicable_image", "ocr": "required"}
    ocr_input = original_sha
    count = sum(item["kind"] == "image" for item in manifest["members"])
    if manifest["media_kind"] == "video":
        frames, frame_ref, frame_body = _bound_artifact(connection, bundle, kind="frames_manifest", version=versions["frames"], input_sha256=original_sha)
        frame_paths = media._validate_video_frames_output(frame_body, manifest_path=_path(frames["local_path"]),
            content_root=Path(bundle["instance_root"]), media_root=Path(bundle["instance_root"]),
            maximum_duration_seconds=None, frames_directory=_path(frames["local_path"]).parent)
        files.append(frame_ref)
        for index, frame in enumerate(frame_paths):
            reference = {**_file_ref(frame, role="video_frame", sha256=frame_body["frames"][index]["sha256"]), "index": index}
            frame_files.append(reference)
        if frame_body.get("contact_sheet") is not None:
            frame_files.append(_file_ref(_path(frame_body["contact_sheet"]), role="contact_sheet"))
        files.extend(frame_files)
        refs.extend([_row_ref("evidence_artifacts", frames, (*_KEEP_ARTIFACT_FIELDS, "metadata_json")),
                     _slot(connection, bundle, frames, processor="frames", input_sha256=original_sha)])
        asr, asr_ref, asr_body = _bound_artifact(connection, bundle, kind="asr", version=versions["asr"], input_sha256=original_sha)
        media._validate_asr_output_body(asr_body)
        files.append(asr_ref)
        refs.extend([_row_ref("evidence_artifacts", asr, (*_KEEP_ARTIFACT_FIELDS, "metadata_json")),
                     _slot(connection, bundle, asr, processor="asr", input_sha256=original_sha)])
        applicability.update({"asr": asr_body["status"], "frames": "success"})
        ocr_input, count = frames["sha256"], len(frame_paths)
    ocr, ocr_ref, ocr_body = _bound_artifact(connection, bundle, kind="ocr", version=versions["ocr"], input_sha256=ocr_input)
    media._validate_ocr_output_body(ocr_body, expected_source_count=count)
    files.append(ocr_ref)
    refs.extend([_row_ref("evidence_artifacts", ocr, (*_KEEP_ARTIFACT_FIELDS, "metadata_json")),
                 _slot(connection, bundle, ocr, processor="ocr", input_sha256=ocr_input)])
    return {"asr": asr, "ocr": ocr, "files": files, "database_refs": refs,
            "frame_files": frame_files, "applicability": applicability}


def _evaluation(connection: sqlite3.Connection, bundle: Mapping[str, Any], processing: Mapping[str, Any]) -> list[dict[str, Any]]:
    from .evaluation import sha256_json

    manifest = bundle["manifest"]
    asr_sha = processing["asr"]["sha256"] if processing["asr"] is not None else None
    row = connection.execute(
        "SELECT ev.* FROM evaluation_versions ev JOIN evidence_envelopes ee ON ee.id=ev.evidence_envelope_id "
        "JOIN evaluation_releases er ON er.id=ev.release_id "
        "WHERE ev.content_id=? AND ev.evaluation_source='automatic' AND ev.evaluation_status='evaluated' "
        "AND ev.evidence_level IN ('V2','V3') AND ev.invalidated_at IS NULL "
        "AND er.status IN ('active','retired') AND er.activated_at IS NOT NULL "
        "AND ee.media_sha256=? AND ee.asr_sha256 IS ? AND ee.ocr_sha256=? AND ee.detail_raw_sha256 IS ? "
        "ORDER BY ev.evaluated_at DESC,ev.id DESC LIMIT 1",
        (manifest["content_id"], manifest["original_artifact"]["sha256"], asr_sha, processing["ocr"]["sha256"], manifest["source"]["raw_sha256"]),
    ).fetchone()
    _require(row is not None, "formal_V2_V3_evaluation_missing")
    evaluation = dict(row)
    envelope = _positive_get(connection, "evidence_envelopes", evaluation["evidence_envelope_id"], "evaluation_envelope_missing")
    components = _object(envelope["components_json"], "evaluation_envelope_invalid")
    keys = ("detail_raw_sha256", "text_sha256", "media_sha256", "asr_sha256", "ocr_sha256", "comments_version_sha256", "manual_evidence_sha256")
    _require(components == {key: envelope[key] for key in keys}
             and sha256_json(components) == envelope["evidence_sha256"] == evaluation["evidence_sha256"]
             and envelope["content_id"] == manifest["content_id"], "evaluation_envelope_hash_mismatch")
    payload = _object(evaluation["payload_json"], "evaluation_payload_invalid")
    _require(payload.get("evidence_level") == evaluation["evidence_level"]
             and payload.get("evaluation_status") == "evaluated", "evaluation_payload_binding_changed")
    release = _positive_get(connection, "evaluation_releases", evaluation["release_id"], "evaluation_release_missing")
    return [_row_ref("evidence_envelopes", envelope), _row_ref("evaluation_versions", evaluation),
            _row_ref("evaluation_releases", release, ("id", "rule_version", "taxonomy_version", "matcher_rule_sha256", "activated_at"))]


def _fingerprint(connection: sqlite3.Connection, bundle: Mapping[str, Any], content: Mapping[str, Any],
                 processing: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    from . import duplicates

    manifest = bundle["manifest"]
    source = {"fingerprint_version": duplicates.FINGERPRINT_VERSION,
              "text_sha256": hashlib.sha256(duplicates._normalize_text(f"{content['title']}\n{content['body']}").encode()).hexdigest(),
              "media_artifact_sha256": manifest["original_artifact"]["sha256"],
              "asr_artifact_sha256": processing["asr"]["sha256"] if processing["asr"] is not None else None,
              "ocr_artifact_sha256": processing["ocr"]["sha256"],
              "bundle_id": manifest["bundle_id"], "media_source_sha256": manifest["source"]["sha256"]}
    source_sha = duplicates._sha256_json(source)
    artifact, reference, payload = _bound_artifact(connection, bundle, kind="duplicate_fingerprint",
                                                   version=duplicates.FINGERPRINT_VERSION, input_sha256=source_sha)
    row = connection.execute("SELECT * FROM duplicate_fingerprints WHERE content_id=? AND fingerprint_version=? AND source_sha256=?",
                             (manifest["content_id"], duplicates.FINGERPRINT_VERSION, source_sha)).fetchone()
    _require(row is not None and row["artifact_id"] == artifact["id"], "fingerprint_not_persisted")
    expected_media = list(dict.fromkeys(item["sha256"] for item in manifest["members"] if item["kind"] in {"video", "image"}))
    _require(bool(expected_media) and payload.get("schema_version") == "duplicate-fingerprint-v1"
             and payload.get("content_id") == manifest["content_id"] and payload.get("source_sha256") == source_sha
             and payload.get("fingerprint_version") == duplicates.FINGERPRINT_VERSION
             and payload.get("media_sha256") == expected_media
             and _object(row["payload_json"], "fingerprint_payload_invalid") == payload
             and json.loads(row["media_sha256_json"]) == expected_media
             and json.loads(row["frame_phashes_json"]) == payload.get("frame_phashes"), "fingerprint_binding_or_members_invalid")
    return reference, [_row_ref("duplicate_fingerprints", dict(row)),
                       _row_ref("evidence_artifacts", artifact, (*_KEEP_ARTIFACT_FIELDS, "metadata_json")),
                       _slot(connection, bundle, artifact, processor="duplicate_fingerprint", input_sha256=source_sha)]


def _previews(bundle: Mapping[str, Any], frames: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest = bundle["manifest"]
    root = Path(bundle["evidence_root"])
    members: list[dict[str, Any]] = []
    files: list[dict[str, Any]] = []
    if manifest["media_kind"] == "image":
        for item in manifest["members"]:
            if item["kind"] != "image":
                continue
            source = Path(bundle["originals_root"]) / item["relative_path"]
            evidence = media._read_private_file_evidence(source, label="preview source", capture_body=True)
            _require(evidence.sha256 == item["sha256"] and evidence.byte_size == item["byte_size"], "preview_source_changed")
            with Image.open(io.BytesIO(evidence.body or b"")) as original:
                original.load()
                preview = ImageOps.exif_transpose(original).convert("RGB")
                width, height = preview.size
                preview.thumbnail((PREVIEW_MAX_EDGE, PREVIEW_MAX_EDGE), Image.Resampling.LANCZOS)
                buffer = io.BytesIO()
                preview.save(buffer, format="JPEG", quality=PREVIEW_JPEG_QUALITY)
                body = buffer.getvalue()
                _require(0 < len(body) <= PREVIEW_MAX_BYTES and preview.width <= width and preview.height <= height, "preview_size_limit")
                dimensions = {"width": preview.width, "height": preview.height}
            path = root / "previews" / PREVIEW_VERSION / f"{item['member_id']}-{item['sha256']}.jpg"
            reference = {**_publish(path, body, root=root), "role": "image_preview"}
            files.append(reference)
            members.append({"index": len(members), "source_member_id": item["member_id"], "source_sha256": item["sha256"],
                            "relative_path": path.relative_to(root).as_posix(), "sha256": reference["sha256"],
                            "byte_size": reference["byte_size"], **dimensions})
    else:
        source = next(item for item in manifest["members"] if item["kind"] == "video")
        for index, reference in enumerate(frames):
            path = _path(reference["path"])
            _require(path.is_relative_to(root), "preview_not_retained")
            with Image.open(path) as frame:
                frame.load()
                dimensions = {"width": frame.width, "height": frame.height}
            members.append({"index": index, "source_member_id": source["member_id"], "source_sha256": source["sha256"],
                            "relative_path": path.relative_to(root).as_posix(), "sha256": reference["sha256"],
                            "byte_size": reference["byte_size"], "role": reference["role"], **dimensions})
    _require(bool(members), "preview_members_missing")
    body = _canonical({"contract_version": PREVIEW_VERSION, "bundle_id": manifest["bundle_id"],
                       "manifest_sha256": bundle["manifest_sha256"], "media_kind": manifest["media_kind"],
                       "max_edge": PREVIEW_MAX_EDGE, "jpeg_quality": PREVIEW_JPEG_QUALITY,
                       "max_bytes": PREVIEW_MAX_BYTES, "members": members})
    path = root / "previews" / PREVIEW_VERSION / (hashlib.sha256(body).hexdigest() + ".json")
    return {**_publish(path, body, root=root), "role": "preview_manifest"}, files


def _register_reference(connection: sqlite3.Connection, bundle: Mapping[str, Any], reference: Mapping[str, Any],
                        *, kind: str, version: str, timestamp: str) -> dict[str, Any]:
    metadata = {"media_lifecycle": {"bundle_id": bundle["manifest"]["bundle_id"],
                "control_artifact_id": bundle["control_artifact_id"], "manifest_sha256": bundle["manifest_sha256"]}}
    row = connection.execute("SELECT * FROM evidence_artifacts WHERE content_id=? AND artifact_type=? AND local_path=?",
                             (bundle["manifest"]["content_id"], kind, reference["path"])).fetchone()
    if row is not None:
        _require(row["sha256"] == reference["sha256"] and row["byte_size"] == reference["byte_size"]
                 and row["processor_version"] == version and row["status"] == "available"
                 and _object(row["metadata_json"], "proof_metadata_invalid") == metadata, "proof_artifact_conflict")
        return dict(row)
    cursor = connection.execute(
        "INSERT INTO evidence_artifacts(content_id,artifact_type,local_path,status,byte_size,sha256,captured_at,processor_version,metadata_json,created_at) "
        "VALUES (?,?,?,'available',?,?,?,?,?,?)",
        (bundle["manifest"]["content_id"], kind, reference["path"], reference["byte_size"], reference["sha256"], timestamp,
         version, json.dumps(metadata, sort_keys=True), timestamp),
    )
    return _positive_get(connection, "evidence_artifacts", int(cursor.lastrowid or 0), "proof_artifact_missing")


def _failure(error: BaseException) -> dict[str, Any]:
    reason = error.error_code if isinstance(error, lifecycle.LifecycleError) else str(error)
    return {"ready": False, "blockers": [reason or type(error).__name__], "receipt": None, "evidence_files": []}


def _verify(connection: sqlite3.Connection, bundle: Mapping[str, Any]) -> dict[str, Any]:
    pointer = bundle["state"].get("completion_receipt")
    _require(isinstance(pointer, dict) and type(pointer.get("artifact_id")) is int, "completion_not_sealed")
    row = _positive_get(connection, "evidence_artifacts", pointer["artifact_id"], "completion_receipt_missing")
    _require(row["artifact_type"] == "media_completion_receipt" and row["processor_version"] == COMPLETION_VERSION
             and row["local_path"] == pointer.get("path") and row["sha256"] == pointer.get("sha256")
             and row["byte_size"] == pointer.get("byte_size"), "completion_pointer_changed")
    _, receipt_ref = _artifact_ref(connection, row["id"], bundle["manifest"]["content_id"], role="completion_receipt",
                                   artifact_type="media_completion_receipt", evidence_root=Path(bundle["evidence_root"]))
    body = _read_object(receipt_ref)
    _require(body.get("contract_version") == COMPLETION_VERSION and body.get("bundle_id") == bundle["manifest"]["bundle_id"]
             and body.get("manifest_sha256") == bundle["manifest_sha256"]
             and body.get("original_artifact") == bundle["manifest"]["original_artifact"]
             and body.get("source") == bundle["manifest"]["source"], "completion_receipt_binding_changed")
    files, refs = body.get("evidence_files"), body.get("database_refs")
    if not isinstance(files, list) or not files or not isinstance(refs, list) or not refs:
        raise CompletionBlocked("completion_evidence_list_invalid")
    for reference in refs:
        _check_row_ref(connection, reference)
    lifecycle.original_artifact(connection, bundle)
    for reference in files:
        _file_ref(_path(reference["path"]), role=reference["role"], sha256=reference["sha256"], byte_size=reference["byte_size"])
    return {"ready": True, "blockers": [], "receipt": dict(pointer), "evidence_files": files + [receipt_ref]}


def verify_completion(bundle: Mapping[str, Any], *, db_path: Path = DEFAULT_DB) -> dict[str, Any]:
    """Verify frozen retained evidence only; never inspect cold original bytes."""
    try:
        with connect(db_path) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            current = lifecycle.load_bundle(connection, str(bundle["manifest"]["bundle_id"]))
            _require(current["manifest_sha256"] == bundle["manifest_sha256"], "bundle_manifest_changed")
            return _verify(connection, current)
    except (CompletionBlocked, lifecycle.LifecycleError, media.MediaProcessingError, OSError, ValueError, KeyError, TypeError) as error:
        return _failure(error)


def seal_completion(bundle_id: str, *, db_path: Path = DEFAULT_DB, at: str | None = None) -> dict[str, Any]:
    """Seal once after full local evidence closure; no provider or model calls."""
    from .media_retention import media_read_lease

    try:
        with connect(db_path) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            bundle = lifecycle.load_bundle(connection, bundle_id)
            if bundle["state"].get("completion_receipt"):
                return _verify(connection, bundle)
            _require(os.environ.get("DCAR_READ_ONLY", "0").strip() != "1", "read_only_completion_not_sealed")
            _require(at is None or not lifecycle._formal(connection), "production_clock_override_forbidden")
        timestamp = at or now_utc()
        _require(parse_time(timestamp) is not None, "completion_time_invalid")
        with media_read_lease(bundle["manifest"]["content_id"], db_path=db_path, purpose="completion", bundle_id=bundle_id):
            with connect(db_path) as connection:
                connection.execute("PRAGMA query_only=ON")
                connection.execute("BEGIN")
                bundle = lifecycle.load_bundle(connection, bundle_id)
                if bundle["state"].get("completion_receipt"):
                    return _verify(connection, bundle)
                base = _source_and_originals(connection, bundle)
                processing = _processing_evidence(connection, bundle)
                evaluation_refs = _evaluation(connection, bundle, processing)
                fingerprint_file, fingerprint_refs = _fingerprint(connection, bundle, base["content"], processing)
            preview_file, preview_files = _previews(bundle, processing["frame_files"])
            files = [*base["files"], *processing["files"], fingerprint_file, *preview_files, preview_file]
            refs = [*base["database_refs"], *processing["database_refs"], *evaluation_refs, *fingerprint_refs]
            with connect(db_path) as connection, transaction(connection):
                current = lifecycle.load_bundle(connection, bundle_id)
                if current["state"].get("completion_receipt"):
                    return _verify(connection, current)
                _require(current["manifest_sha256"] == bundle["manifest_sha256"], "bundle_manifest_changed")
                for reference in refs:
                    _check_row_ref(connection, reference)
                for reference in files:
                    _file_ref(_path(reference["path"]), role=reference["role"], sha256=reference["sha256"], byte_size=reference["byte_size"])
                preview_artifact = _register_reference(connection, bundle, preview_file, kind="media_preview_manifest", version=PREVIEW_VERSION, timestamp=timestamp)
                refs.append(_row_ref("evidence_artifacts", preview_artifact, (*_KEEP_ARTIFACT_FIELDS, "metadata_json")))
                body = _canonical({"contract_version": COMPLETION_VERSION, "bundle_id": bundle_id,
                                   "manifest_sha256": bundle["manifest_sha256"], "source": bundle["manifest"]["source"],
                                   "original_artifact": bundle["manifest"]["original_artifact"], "sealed_at": timestamp,
                                   "applicability": processing["applicability"], "evidence_files": files, "database_refs": refs})
                path = Path(bundle["evidence_root"]) / "completion" / (hashlib.sha256(body).hexdigest() + ".json")
                receipt = _publish(path, body, root=Path(bundle["evidence_root"]))
                artifact = _register_reference(connection, bundle, receipt, kind="media_completion_receipt", version=COMPLETION_VERSION, timestamp=timestamp)
                pointer = {"artifact_id": artifact["id"], **{key: receipt[key] for key in ("path", "sha256", "byte_size")}}
                bundle = lifecycle.update_state(connection, current, {"completion_receipt": pointer}, current["state"]["revision"])
                return _verify(connection, bundle)
    except (CompletionBlocked, lifecycle.LifecycleError, media.MediaProcessingError, OSError, ValueError, KeyError, TypeError) as error:
        return _failure(error)
