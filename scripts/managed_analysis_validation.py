"""Read-only managed-v1 analysis acceptance, separate from frozen legacy runs.

This verifier never downloads, processes, seals, restores, archives or deletes.
Cold acceptance proves retained evidence and recorded lifecycle state, not the
current availability of an archive on another host or the ability to restore.
SQLite's WAL-aware read-only connection can create/update its own WAL/SHM
reader bookkeeping. Zero mutations means database rows and media/evidence;
this command never checkpoints or removes those database sidecars.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import stat
from collections import Counter
from contextlib import closing
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import quote

from PIL import Image

from v8 import media, media_completion as completion, media_lifecycle as lifecycle
from v8.media_policy import POLICY
from v8.source_routing import parse_time

MANAGED_CONTRACT_VERSION = "local-analysis-managed-v1"
GENERATED_MANAGED_ARTIFACT_TYPES = frozenset({
    "media", "media_manifest", "frames_manifest", "asr", "ocr", "duplicate_fingerprint",
    "media_lifecycle_manifest", "media_preview_manifest", "media_completion_receipt", "media_lifecycle_receipt",
})
STAGES = frozenset({"downloaded", "processed", "sealed"})
_BUNDLE_FIELDS = {"content_id", "bundle_id", "manifest_sha256", "stage", "storage_state", "originals_state", "artifact_ids"}
_DERIVED = {"frames_manifest": ("frames", "frames.json"), "asr": ("asr", "asr.json"),
            "ocr": ("ocr", "ocr.json"), "duplicate_fingerprint": ("fingerprint", "fingerprint.json")}


class ManagedAnalysisValidationError(RuntimeError):
    pass


def _require(condition: Any, reason: str) -> None:
    if not condition:
        raise ManagedAnalysisValidationError(reason)


def _canonical(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def _object(value: Any, reason: str) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    _require(isinstance(value, dict), reason)
    return value


def _absolute(value: Any, *, directory: bool) -> Path:
    _require(type(value) is str and bool(value), "managed_path_required")
    path = Path(value)
    _require(path.is_absolute() and ".." not in path.parts, "managed_path_not_absolute")
    _require(path == path.resolve(strict=True), "managed_path_alias")
    _require(path.is_dir() if directory else path.is_file(), "managed_path_missing")
    if not directory:
        info = path.lstat()
        _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, "managed_path_not_private_file")
    return path


def _reader(path: Path) -> sqlite3.Connection:
    # Live writer readers must see WAL; immutable=1 belongs only to sealed
    # snapshots and is intentionally not used by this acceptance contract.
    connection = sqlite3.connect(f"file:{quote(str(path), safe='/')}?mode=ro", uri=True, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("BEGIN")
    return connection


def _file(path: Path, *, sha256: str, byte_size: int) -> dict[str, Any]:
    evidence = media._read_private_file_evidence(path, label="managed acceptance evidence")
    _require(evidence.sha256 == sha256 and evidence.byte_size == byte_size, "managed_file_identity_changed")
    return {"path": str(path), "sha256": sha256, "byte_size": byte_size}


def _json_row(row: Mapping[str, Any]) -> dict[str, Any]:
    path = media._resolved(str(row["local_path"]))
    _file(path, sha256=row["sha256"], byte_size=row["byte_size"])
    return _object(json.loads(path.read_bytes()), "managed_json_not_object")


def _scoped_files(root: Path) -> set[Path]:
    """Only this explicitly frozen instance, never a link/cache-wide scan."""
    found: set[Path] = set()
    for path in root.iterdir():
        info = path.lstat()
        _require(not stat.S_ISLNK(info.st_mode), "managed_output_symlink")
        if stat.S_ISDIR(info.st_mode):
            found.update(_scoped_files(path))
        else:
            _require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, "managed_output_not_private_file")
            _require(not path.name.startswith(".") and not path.name.endswith((".tmp", ".candidate")), "managed_unsettled_output")
            found.add(path)
    return found


def _contract_shape(contract: Mapping[str, Any]) -> tuple[Path, Path, list[dict[str, Any]]]:
    _require(set(contract) == {"schema_version", "database", "media_root", "bundles"}
             and contract["schema_version"] == MANAGED_CONTRACT_VERSION, "managed_contract_version_or_fields_invalid")
    database = _absolute(contract["database"], directory=False)
    root = _absolute(contract["media_root"], directory=True)
    entries = contract["bundles"]
    if not isinstance(entries, list) or not entries:
        raise ManagedAnalysisValidationError("managed_bundle_scope_empty")
    seen: set[str] = set()
    for entry in entries:
        _require(isinstance(entry, dict) and set(entry) == _BUNDLE_FIELDS, "managed_bundle_fields_invalid")
        _require(type(entry["content_id"]) is int and entry["content_id"] > 0
                 and type(entry["bundle_id"]) is str and re.fullmatch(r"[0-9a-f]{32}", entry["bundle_id"]) is not None
                 and media._valid_sha256(entry["manifest_sha256"]) and entry["stage"] in STAGES
                 and entry["storage_state"] in {"hot", "archived", "expired"}
                 and entry["originals_state"] in {"online", "archived", "expired"}, "managed_bundle_identity_invalid")
        _require(entry["bundle_id"] not in seen, "managed_bundle_scope_duplicated")
        seen.add(entry["bundle_id"])
        ids = entry["artifact_ids"]
        _require(isinstance(ids, list) and bool(ids) and all(type(value) is int and value > 0 for value in ids), "managed_artifact_ids_invalid")
        _require(ids == sorted(set(ids)), "managed_artifact_ids_not_exact")
        _require(entry["stage"] == "sealed" or entry["storage_state"] == "hot" and entry["originals_state"] == "online", "unsealed_bundle_cannot_be_cold")
    return database, root, entries


def _artifact_rows(connection: sqlite3.Connection, bundle: Mapping[str, Any], entry: Mapping[str, Any]) -> list[dict[str, Any]]:
    root = Path(bundle["instance_root"])
    rows = connection.execute("SELECT * FROM evidence_artifacts WHERE content_id=? ORDER BY id", (entry["content_id"],)).fetchall()
    owned = []
    for raw in rows:
        row = dict(raw)
        metadata = _object(row["metadata_json"], "managed_artifact_metadata_invalid")
        namespace = metadata.get("media_lifecycle")
        in_bundle = isinstance(namespace, dict) and namespace.get("bundle_id") == entry["bundle_id"]
        path = media._resolved(str(row["local_path"]))
        in_directory = path.is_relative_to(root)
        if not in_bundle and not in_directory:
            continue
        if not in_bundle or not in_directory or not isinstance(namespace, dict):
            raise ManagedAnalysisValidationError("managed_artifact_ownership_mismatch")
        _require(row["artifact_type"] in GENERATED_MANAGED_ARTIFACT_TYPES, "managed_artifact_not_whitelisted")
        _require(namespace.get("control_artifact_id") == bundle["control_artifact_id"]
                 or row["id"] == bundle["control_artifact_id"], "managed_artifact_control_mismatch")
        owned.append(row)
    _require([row["id"] for row in owned] == entry["artifact_ids"], "managed_artifact_set_changed")
    kinds = Counter(row["artifact_type"] for row in owned)
    required = {"media_lifecycle_manifest", "media" if bundle["manifest"]["media_kind"] == "video" else "media_manifest"}
    if entry["stage"] != "downloaded":
        required.add("ocr")
        if bundle["manifest"]["media_kind"] == "video":
            required.update({"asr", "frames_manifest"})
    if entry["stage"] == "sealed":
        required.update({"duplicate_fingerprint", "media_preview_manifest", "media_completion_receipt"})
    allowed = set(required)
    if entry["stage"] == "processed":
        allowed.add("duplicate_fingerprint")
    if entry["storage_state"] != "hot":
        allowed.add("media_lifecycle_receipt")
    _require(set(kinds) <= allowed and required <= set(kinds), "managed_stage_artifact_types_invalid")
    _require(all(count == 1 for kind, count in kinds.items() if kind != "media_lifecycle_receipt"), "managed_stage_artifact_count_invalid")
    return owned


def _receipt(connection: sqlite3.Connection, bundle: Mapping[str, Any], row: Mapping[str, Any]) -> dict[str, Any]:
    body = _json_row(row)
    state, manifest = bundle["state"], bundle["manifest"]
    operations = {"archive_full_restore_verified": "archive", "initial_hot_release": "hot-release",
                  "restored_hot_release": "hot-release", "restore": "restore", "permanent_delete": "deletion"}
    operation = body.get("operation")
    _require(isinstance(operation, str) and operation in operations and body.get("contract") == POLICY["contract_version"]
             and body.get("bundle_id") == bundle["bundle_id"], "managed_receipt_contract_invalid")
    expected = Path(bundle["evidence_root"]) / "lifecycle" / operations[str(operation)] / f"{row['sha256']}.json"
    _require(media._resolved(row["local_path"]) == expected and row["processor_version"] == POLICY["contract_version"], "managed_receipt_path_or_version_invalid")
    if operation in {"archive_full_restore_verified", "restore", "permanent_delete"}:
        _require(body.get("manifest_sha256") == bundle["manifest_sha256"] and body.get("members") == manifest["members"], "managed_receipt_members_changed")
    if operation == "archive_full_restore_verified":
        _require(body.get("full_decode") is True and body.get("archive_key") == manifest["archive_key"]
                 and body.get("completion_receipt") == state["completion_receipt"], "managed_archive_proof_invalid")
        first_at, verified_at = state.get("archive_verified_at"), body.get("verified_at")
        if not isinstance(first_at, str) or not isinstance(verified_at, str):
            raise ManagedAnalysisValidationError("managed_archive_time_invalid")
        first, verified = parse_time(first_at), parse_time(verified_at)
        _require(verified <= first, "managed_archive_time_invalid")
    if operation in {"restore", "permanent_delete"}:
        _require(body.get("archive_verified_at") == state["archive_verified_at"] and body.get("delete_due_at") == state["delete_due_at"], "managed_receipt_deadline_changed")
    if operation == "restore":
        _require(body.get("full_decode") is True, "managed_restore_proof_invalid")
    if operation == "permanent_delete":
        _require(body.get("deleted_at") == state["deleted_at"] and body.get("settled") == state.get("deleted_members"), "managed_deletion_proof_changed")
    for key in ("archive_receipt", "hot_release_receipt", "restore_receipt", "deletion_receipt"):
        pointer = state.get(key)
        if isinstance(pointer, dict) and pointer.get("artifact_id") == row["id"]:
            _require(all(pointer.get(name) == row[column] for name, column in (("sha256", "sha256"), ("byte_size", "byte_size"), ("path", "local_path"))), "managed_receipt_pointer_changed")
    return body


def _preview(bundle: Mapping[str, Any], row: Mapping[str, Any]) -> set[Path]:
    body = _json_row(row)
    root, manifest = Path(bundle["evidence_root"]), bundle["manifest"]
    _require(body.get("contract_version") == completion.PREVIEW_VERSION
             and body.get("bundle_id") == bundle["bundle_id"] and body.get("manifest_sha256") == bundle["manifest_sha256"]
             and body.get("media_kind") == manifest["media_kind"] and body.get("max_edge") == POLICY["preview"]["maximum_edge"]
             and body.get("jpeg_quality") == POLICY["preview"]["quality"] and body.get("max_bytes") == POLICY["preview"]["maximum_bytes"], "managed_preview_contract_invalid")
    members = body.get("members")
    if not isinstance(members, list) or not members:
        raise ManagedAnalysisValidationError("managed_previews_missing")
    originals = {item["member_id"]: item for item in manifest["members"]}
    files: set[Path] = set()
    source_ids = []
    for index, item in enumerate(members):
        _require(isinstance(item, dict) and type(item.get("index")) is int and item["index"] == index, "managed_preview_index_invalid")
        source_id = item.get("source_member_id")
        _require(source_id in originals and item.get("source_sha256") == originals[source_id]["sha256"], "managed_preview_source_changed")
        relative = lifecycle._member_path(item.get("relative_path"))
        path = root / relative
        _require(path not in files, "managed_preview_path_shared")
        _file(path, sha256=item["sha256"], byte_size=item["byte_size"])
        with Image.open(path) as preview:
            preview.load()
            _require(preview.width == item.get("width") and preview.height == item.get("height"), "managed_preview_dimensions_changed")
            if manifest["media_kind"] == "image":
                _require(preview.format == "JPEG" and max(preview.size) <= POLICY["preview"]["maximum_edge"]
                         and item["byte_size"] <= POLICY["preview"]["maximum_bytes"], "managed_image_preview_limit_invalid")
        files.add(path)
        source_ids.append(source_id)
    if manifest["media_kind"] == "image":
        _require(source_ids == [item["member_id"] for item in manifest["members"] if item["kind"] == "image"], "managed_preview_primary_set_changed")
    else:
        _require(all(item == manifest["members"][0]["member_id"] for item in source_ids), "managed_video_preview_source_changed")
    return files


def _validate_bundle(connection: sqlite3.Connection, root: Path, entry: Mapping[str, Any]) -> dict[str, Any]:
    bundle = lifecycle.load_bundle(connection, entry["bundle_id"])
    manifest, state = bundle["manifest"], bundle["state"]
    _require(manifest["content_id"] == entry["content_id"] and bundle["manifest_sha256"] == entry["manifest_sha256"], "managed_bundle_identity_changed")
    _require(bundle["instance_root"] == root / "managed-v1" / manifest["link_id"] / bundle["bundle_id"], "managed_bundle_outside_media_root")
    _require(state["storage_state"] == entry["storage_state"], "managed_storage_phase_changed")
    _require(state["operation_state"] == "idle", "managed_operation_in_progress")
    owned = _artifact_rows(connection, bundle, entry)
    by_type = {row["artifact_type"]: row for row in owned if row["artifact_type"] != "media_lifecycle_receipt"}
    original = lifecycle.original_artifact(connection, bundle)
    originals = {Path(bundle["originals_root"]) / item["relative_path"]: item for item in manifest["members"]}
    present = {path for path in originals if os.path.lexists(path)}
    if entry["originals_state"] == "online":
        _require(state["storage_state"] in {"hot", "archived"} and present == set(originals), "managed_originals_not_online")
        _require(original["status"] == "available", "managed_online_artifact_not_available")
    else:
        expected_state = "expired" if entry["originals_state"] == "expired" else "archived"
        _require(state["storage_state"] == expected_state and not present, "managed_originals_absence_unproven")
        _require(original["status"] == ("missing" if manifest["media_kind"] == "video" else "available"), "managed_cold_artifact_status_invalid")
    for path in present:
        item = originals[path]
        _file(path, sha256=item["sha256"], byte_size=item["byte_size"])
    expected_files: set[Path] = set(present)
    receipt_bodies: dict[int, dict[str, Any]] = {}
    for row in owned:
        path = media._resolved(str(row["local_path"]))
        kind = row["artifact_type"]
        if row["id"] == original["id"] and kind == "media" and entry["originals_state"] != "online":
            continue
        _require(row["status"] == "available", "managed_required_artifact_not_available")
        _file(path, sha256=row["sha256"], byte_size=row["byte_size"])
        expected_files.add(path)
        if kind in _DERIVED:
            namespace = _object(row["metadata_json"], "managed_metadata_invalid")["media_lifecycle"]
            expected_namespace = media.managed_evidence_metadata(bundle, source_sha256=namespace.get("input_sha256"), processor_version=row["processor_version"])["media_lifecycle"]
            _require(namespace == expected_namespace, "managed_derived_binding_changed")
            stage, filename = _DERIVED[kind]
            _require(path == media.managed_evidence_path(bundle, stage=stage, source_sha256=namespace["input_sha256"], processor_version=row["processor_version"], filename=filename), "managed_derived_path_changed")
        elif kind == "media_preview_manifest":
            _require(path == Path(bundle["evidence_root"]) / "previews" / completion.PREVIEW_VERSION / f"{row['sha256']}.json", "managed_preview_path_changed")
            expected_files.update(_preview(bundle, row))
        elif kind == "media_completion_receipt":
            _require(path == Path(bundle["evidence_root"]) / "completion" / f"{row['sha256']}.json", "managed_completion_path_changed")
        elif kind == "media_lifecycle_receipt":
            receipt_bodies[row["id"]] = _receipt(connection, bundle, row)
    if entry["stage"] == "sealed":
        verified = completion._verify(connection, bundle)
        expected_files.update(media._resolved(item["path"]) for item in verified["evidence_files"]
                              if media._resolved(item["path"]).is_relative_to(bundle["instance_root"]))
    else:
        _require(state.get("completion_receipt") is None, "managed_stage_already_sealed")
        base = completion._source_and_originals(connection, bundle)
        if entry["stage"] == "processed":
            processing = completion._processing_evidence(connection, bundle)
            expected_files.update(media._resolved(item["path"]) for item in processing["files"])
            if "duplicate_fingerprint" in by_type:
                completion._fingerprint(connection, bundle, base["content"], processing)
    if entry["storage_state"] in {"archived", "expired"}:
        pointer = state.get("archive_receipt")
        _require(isinstance(pointer, dict) and pointer.get("artifact_id") in receipt_bodies
                 and receipt_bodies[pointer["artifact_id"]].get("operation") == "archive_full_restore_verified", "managed_archive_receipt_missing")
    if entry["storage_state"] == "expired":
        pointer = state.get("deletion_receipt")
        _require(isinstance(pointer, dict) and pointer.get("artifact_id") in receipt_bodies
                 and receipt_bodies[pointer["artifact_id"]].get("operation") == "permanent_delete", "managed_deletion_receipt_missing")
    _require(_scoped_files(Path(bundle["instance_root"])) == expected_files, "managed_output_file_closure_changed")
    return {"bundle_id": bundle["bundle_id"], "content_id": manifest["content_id"], "manifest_sha256": bundle["manifest_sha256"],
            "stage": entry["stage"], "storage_state": state["storage_state"], "originals_state": entry["originals_state"],
            "control_artifact_id": bundle["control_artifact_id"], "revision": state["revision"],
            "artifact_ids": [row["id"] for row in owned], "artifact_counts": dict(sorted(Counter(row["artifact_type"] for row in owned).items())),
            "original_member_count": len(originals), "original_member_bytes": manifest["byte_size"], "online_file_count": len(expected_files),
            "original_artifact": manifest["original_artifact"], "archive_verified_at": state["archive_verified_at"],
            "delete_due_at": state["delete_due_at"], "deleted_at": state["deleted_at"], "restore_availability_checked": False}


def validate_managed_contract(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Return a read-only acceptance result for exact caller-frozen instances."""
    try:
        database, root, entries = _contract_shape(contract)
        with closing(_reader(database)) as connection:
            results = [_validate_bundle(connection, root, entry) for entry in entries]
        # A separate WAL-aware snapshot detects lifecycle changes during file
        # reads. This is evidence at this instant, never a deletion permission.
        with closing(_reader(database)) as connection:
            for result in results:
                current = lifecycle.load_bundle(connection, result["bundle_id"])
                _require(current["state"]["revision"] == result["revision"] and current["manifest_sha256"] == result["manifest_sha256"], "managed_state_changed_during_validation")
        return {"ok": True, "status": "verified", "schema_version": MANAGED_CONTRACT_VERSION,
                "contract_sha256": hashlib.sha256(_canonical(contract)).hexdigest(), "provider_calls": 0,
                "mutations": 0, "mutation_scope": "database_rows_and_media_evidence",
                "database_reader": "wal_aware_mode_ro_query_only", "sqlite_reader_sidecars_possible": True,
                "bundles": results}
    except ManagedAnalysisValidationError:
        raise
    except (lifecycle.LifecycleError, completion.CompletionBlocked, media.MediaProcessingError,
            OSError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
        raise ManagedAnalysisValidationError(str(error) or type(error).__name__) from error


def verify_managed_contract_file(path: Path, *, expected_sha256: str) -> dict[str, Any]:
    _require(media._valid_sha256(expected_sha256), "managed_expected_contract_hash_invalid")
    path = _absolute(str(path), directory=False)
    body = path.read_bytes()
    _require(hashlib.sha256(body).hexdigest() == expected_sha256, "managed_contract_file_hash_changed")
    result = validate_managed_contract(_object(json.loads(body), "managed_contract_not_object"))
    return {**result, "contract_file_sha256": expected_sha256}


def is_managed_verification(argv: Sequence[str]) -> bool:
    return any(value == "--verify-managed-contract" or value.startswith("--verify-managed-contract=") for value in argv)


def managed_verification_main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description="Verify managed-v1 evidence without analysis or business writes (SQLite reader sidecars may be created).")
    parser.add_argument("--verify-managed-contract", type=Path, required=True)
    parser.add_argument("--expected-contract-sha256", required=True)
    arguments = parser.parse_args(argv)
    try:
        result = verify_managed_contract_file(arguments.verify_managed_contract, expected_sha256=arguments.expected_contract_sha256)
    except (ManagedAnalysisValidationError, OSError, ValueError, TypeError) as error:
        result = {"ok": False, "status": "blocked", "schema_version": MANAGED_CONTRACT_VERSION,
                  "error": str(error), "provider_calls": 0, "mutations": 0,
                  "mutation_scope": "database_rows_and_media_evidence", "sqlite_reader_sidecars_possible": True}
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0
