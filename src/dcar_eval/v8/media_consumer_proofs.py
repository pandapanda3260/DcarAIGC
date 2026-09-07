"""Runtime-bound receipts required before releasing production originals.

The collector makes read-only health/SSH requests; no provider calls, service
changes or automatic activation. Fixture booleans never authorize production.
"""
from __future__ import annotations

import hashlib
import json
import os
import argparse
import sqlite3
import stat
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, build_opener

from .snapshot_contract import descriptor, validate_descriptor
from .runtime_paths import project_root as runtime_project_root, source_root

PROJECT_ROOT = runtime_project_root(Path(__file__).resolve().parents[3])
RUNTIME_CONTRACT = "media-consumer-runtime-v1"
PROOF_CONTRACT = "media-production-consumer-proof-v1"
LEGACY_TRANSITION_CONTRACT = "dcar-schema17-to18-server-transition-v1"
TRANSITION_CONTRACT = "dcar-schema18-to19-server-transition-v1"
SOURCE_SCHEMA_VERSION = 18
DATABASE_SCHEMA_VERSION = 19
_EXTENSIONS = {".py", ".json", ".ts", ".tsx", ".js", ".mjs", ".css", ".toml", ".lock"}
_CODE_DIRS = ("src/dcar_eval", "config", "app/web/app", "app/web/components",
              "app/web/lib", "app/web/worker")
_CODE_FILES = ("pyproject.toml", "uv.lock", "scripts/build_server_snapshot.py",
               "deploy/macos/publish_snapshot.py", "deploy/server/install_snapshot.py",
               "app/web/package.json", "app/web/package-lock.json", "app/web/next.config.ts")


def code_sha256(project_root: Path) -> str:
    """Same source closure on Mac and release/app; excludes outputs and secrets."""
    root = source_root(project_root)
    paths = {root / name for name in _CODE_FILES}
    for name in _CODE_DIRS:
        directory = root / name
        if directory.is_dir():
            paths.update(path for path in directory.rglob("*") if path.suffix in _EXTENSIONS and path.is_file())
    digest = hashlib.sha256()
    for path in sorted(paths):
        info = path.lstat()
        if path != path.resolve(strict=True) or not stat.S_ISREG(info.st_mode):
            raise ValueError("consumer_source_not_regular")
        body = path.read_bytes()
        after = path.stat()
        if (info.st_ino, info.st_size, info.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValueError("consumer_source_changed_while_reading")
        digest.update(path.relative_to(root).as_posix().encode() + b"\0" + hashlib.sha256(body).digest())
    return digest.hexdigest()


# Imported by the API before lifespan startup; never replace this fingerprint
# with a later on-disk value and call that the already-running process's code.
_IMPORTED_AT = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
_BOOT_ID = uuid.uuid4().hex
_IMPORTED_CODE = code_sha256(PROJECT_ROOT)


def runtime_identity() -> dict[str, Any]:
    return {"contract_version": RUNTIME_CONTRACT, "started_at": _IMPORTED_AT,
            "boot_id": _BOOT_ID, "pid": os.getpid(), "project_root": str(PROJECT_ROOT),
            "code_sha256": _IMPORTED_CODE,
            "current_code_matches_loaded": code_sha256(PROJECT_ROOT) == _IMPORTED_CODE,
            "snapshot_contract": descriptor()}


def _read_health(url: str) -> dict[str, Any]:
    parsed = urlsplit(url)
    if (parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
            or parsed.path != "/api/v8/health" or parsed.query or parsed.fragment
            or parsed.username or parsed.password):
        raise ValueError("mac_consumer_health_must_be_loopback")
    # Never forward a local health request through an environment proxy.
    with build_opener(ProxyHandler({})).open(url, timeout=5) as response:
        if response.geturl() != url:
            raise ValueError("consumer_health_redirected")
        value = json.loads(response.read(2 * 1024 * 1024))
    if not isinstance(value, dict):
        raise ValueError("consumer_health_invalid")
    return value


def _runtime(value: Any, *, code: str, root: str | None = None) -> dict[str, Any]:
    if (not isinstance(value, dict) or value.get("contract_version") != RUNTIME_CONTRACT
            or value.get("current_code_matches_loaded") is not True or value.get("code_sha256") != code
            or type(value.get("pid")) is not int or value["pid"] <= 0
            or not isinstance(value.get("boot_id"), str) or len(value["boot_id"]) != 32
            or root is not None and value.get("project_root") != root):
        raise ValueError("consumer_runtime_not_loaded_release")
    validate_descriptor(value.get("snapshot_contract"))
    return value


def validate_capture(value: Mapping[str, Any], activation: Mapping[str, Any], *, current_mac: Mapping[str, Any]) -> None:
    """Validate real observations and their exact activation/release bindings."""
    if (value.get("contract_version") != PROOF_CONTRACT or value.get("fixture_only") is not False
            or value.get("activation_id") != activation["activation_id"]
            or value.get("release") != activation["release"]
            or value.get("rules_sha256") != activation["rules_sha256"]
            or value.get("canary_content_ids") != activation["canary_content_ids"]):
        raise ValueError("consumer_proof_activation_mismatch")
    validate_descriptor(value.get("snapshot_contract"))
    code = code_sha256(PROJECT_ROOT)
    saved = value.get("mac_health")
    if not isinstance(saved, dict) or saved.get("read_only") is not False or current_mac.get("read_only") is not False:
        raise ValueError("mac_consumer_is_not_writer")
    saved_runtime = _runtime(saved.get("media_consumers"), code=code, root=str(PROJECT_ROOT))
    live_runtime = _runtime(current_mac.get("media_consumers"), code=code, root=str(PROJECT_ROOT))
    if saved_runtime != live_runtime:
        raise ValueError("mac_consumer_restarted_recapture_required")
    for health in (saved, current_mac):
        if (health.get("status") != "ok"
                or health.get("database_state", {}).get("user_version") != DATABASE_SCHEMA_VERSION
                or health.get("database_path") != value.get("database_path")):
            raise ValueError("mac_consumer_database_mismatch")
    transition, remote = value.get("server_transition"), value.get("server_probe")
    if (not isinstance(transition, dict) or not isinstance(remote, dict)
            or transition.get("schema") != TRANSITION_CONTRACT or transition.get("status") != "succeeded"
            or transition.get("from_schema") != SOURCE_SCHEMA_VERSION
            or transition.get("to_schema") != DATABASE_SCHEMA_VERSION
            or transition.get("code_sha256") != code
            or transition.get("new_release") != remote.get("current_release")
            or not transition.get("completed_at") or not transition.get("sealed_manifest_sha256")):
        raise ValueError("server_pairing_receipt_required")
    validate_descriptor(transition.get("snapshot_contract"))
    health = remote.get("health", {})
    active = remote.get("active_receipt", {})
    if (health.get("status") != "ok" or health.get("read_only") is not True
            or health.get("lifecycle_jobs_enabled") is not False
            or health.get("database_state", {}).get("user_version") != DATABASE_SCHEMA_VERSION
            or not active.get("snapshot_id") or not active.get("manifest_sha256")
            or active.get("runtime_identity") != transition.get("runtime_identity")
            or health.get("database_state", {}).get("runtime_identity") != transition.get("runtime_identity")
            or health.get("database_state", {}).get("sha256") != active.get("database_sha256", {}).get("dcar_insight.sqlite3")):
        raise ValueError("server_pairing_runtime_mismatch")
    _runtime(health.get("media_consumers"), code=code)
    validate_descriptor(health.get("snapshot_contract"))
    if current_mac.get("database_state", {}).get("runtime_identity") != transition.get("runtime_identity"):
        raise ValueError("mac_server_runtime_identity_mismatch")
    if value.get("server_transition") != remote.get("schema_transition"):
        raise ValueError("server_pairing_observation_mismatch")


def _read_proof(reference: Any) -> dict[str, Any]:
    if not isinstance(reference, dict) or set(reference) != {"path", "sha256", "byte_size"}:
        raise ValueError("consumer_proof_reference_required")
    path = Path(reference["path"])
    root = PROJECT_ROOT / "data/cache/media-consumer-proofs"
    if not path.is_absolute() or not path.is_relative_to(root) or path != path.resolve(strict=True):
        raise ValueError("consumer_proof_path_invalid")
    before = path.stat()
    if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_uid != os.getuid()
            or stat.S_IMODE(before.st_mode) != 0o600 or before.st_size > 4 * 1024 * 1024):
        raise ValueError("consumer_proof_not_private")
    body = path.read_bytes()
    if hashlib.sha256(body).hexdigest() != reference["sha256"] or len(body) != reference["byte_size"]:
        raise ValueError("consumer_proof_hash_mismatch")
    value = json.loads(body)
    if not isinstance(value, dict):
        raise ValueError("consumer_proof_invalid")
    return value


def verify_production(connection: sqlite3.Connection, activation: Mapping[str, Any], *, require_canary: bool) -> None:
    """No boolean shortcuts. Recapture after a Mac restart or source change."""
    from . import media_lifecycle as lifecycle, media_retention

    proofs = activation.get("proofs", {})
    if proofs.get("contract_version") != PROOF_CONTRACT or proofs.get("fixture_only") is not False:
        raise ValueError("production_lifecycle_proofs_not_bound")
    value = _read_proof(proofs.get("consumer_receipt"))
    database = connection.execute("PRAGMA database_list").fetchone()
    if database is None or value.get("database_path") != str(Path(database[2]).resolve(strict=True)):
        raise ValueError("consumer_proof_database_mismatch")
    validate_capture(value, activation, current_mac=_read_health(value["mac_health_url"]))
    if not require_canary:
        return
    bundle_ids = proofs.get("canary_bundles")
    if not isinstance(bundle_ids, list) or not bundle_ids or len(set(bundle_ids)) != len(bundle_ids):
        raise ValueError("production_canary_restore_required")
    covered: set[int] = set()
    for bundle_id in bundle_ids:
        bundle = lifecycle.load_bundle(connection, bundle_id)
        if bundle["manifest"]["activation_id"] != activation["activation_id"]:
            raise ValueError("production_canary_activation_mismatch")
        state = bundle["state"]
        for field, operation in (("archive_receipt", "archive_full_restore_verified"), ("restore_receipt", "restore")):
            reference = state.get(field)
            if not isinstance(reference, dict) or "artifact_id" not in reference:
                raise ValueError("production_canary_restore_required")
            artifact = lifecycle._artifact(connection, reference["artifact_id"])
            proof = lifecycle._verified_json(artifact)
            if (artifact["sha256"] != reference["sha256"] or proof.get("bundle_id") != bundle_id
                    or proof.get("manifest_sha256") != bundle["manifest_sha256"]
                    or proof.get("operation") != operation or proof.get("full_decode") is not True
                    or proof.get("members") != media_retention._members(bundle)):
                raise ValueError("production_canary_restore_mismatch")
            run = connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (proof.get("run_id"),)).fetchone()
            run_operation = "archive" if field == "archive_receipt" else "restore"
            details = json.loads(run["details_json"]) if run else {}
            identity = details.get("identity", {})
            attempt = connection.execute("SELECT * FROM scheduler_run_attempts WHERE id=? AND scheduler_run_id=?",
                (details.get("owner", {}).get("attempt_id"), proof.get("run_id"))).fetchone()
            if (run is None or run["status"] != "succeeded" or run["job_id"] != "media_" + run_operation
                    or identity.get("bundle_id") != bundle_id or identity.get("manifest_sha256") != bundle["manifest_sha256"]
                    or identity.get("operation") != run_operation or identity.get("contract") != media_retention.RETENTION_VERSION
                    or details.get("complete") is not True or attempt is None or attempt["status"] != "succeeded"
                    or json.loads(attempt["details_json"]) != details):
                raise ValueError("production_canary_run_not_succeeded")
            if field == "restore_receipt" and (
                    proof.get("archive_verified_at") != state.get("archive_verified_at")
                    or proof.get("delete_due_at") != state.get("delete_due_at")
                    or not lifecycle._time(state["archive_verified_at"]) <= lifecycle._time(proof["finished_at"]) < lifecycle._time(state["delete_due_at"])):
                raise ValueError("production_canary_restore_time_invalid")
        if (not state.get("hot_release_receipt") or not state.get("archive_verified_at")
                or state.get("delete_due_at") != media_retention._later(state["archive_verified_at"], hours=72)):
            raise ValueError("production_canary_release_required")
        release_ref = state["hot_release_receipt"]
        release_artifact = lifecycle._artifact(connection, release_ref["artifact_id"])
        release_proof = lifecycle._verified_json(release_artifact)
        if (release_artifact["sha256"] != release_ref["sha256"] or release_proof.get("bundle_id") != bundle_id
                or release_proof.get("operation") not in {"initial_hot_release", "restored_hot_release"}):
            raise ValueError("production_canary_release_required")
        for member in media_retention._members(bundle):
            settled = release_proof.get("settled", {}).get("initial_release:hot:" + member["member_id"], {})
            if any(settled.get(key) != member[key] for key in ("member_id", "sha256", "byte_size")) or settled.get("absent") is not True:
                raise ValueError("production_canary_release_incomplete")
        covered.add(bundle["manifest"]["content_id"])
    if covered != set(activation["canary_content_ids"]):
        raise ValueError("production_canary_scope_incomplete")


def capture(*, db_path: Path, publisher_env: Path, mac_health_url: str) -> dict[str, Any]:
    """Capture current runtime via real read-only I/O; never attach or activate."""
    import importlib.util
    import sys
    name = "_dcar_media_proof_publisher"
    spec = importlib.util.spec_from_file_location(name, source_root(PROJECT_ROOT) / "deploy/macos/publish_snapshot.py")
    if spec is None or spec.loader is None:
        raise ValueError("consumer_publisher_module_missing")
    publisher = importlib.util.module_from_spec(spec)
    sys.modules[name] = publisher
    spec.loader.exec_module(publisher)
    from .media_lifecycle import activation
    from .storage import connect

    with connect(db_path, read_only=True) as connection:
        record = activation(connection)
    if record is None or record["mode"] == "paused":
        raise ValueError("media_enrollment_required_before_consumer_capture")
    config = publisher._read_external_env(publisher_env, project_root=PROJECT_ROOT)
    ssh = publisher._check_ssh_alias(config, runner=subprocess.run)
    remote = publisher._remote_probe(config, ssh, runner=subprocess.run)
    publisher._validate_remote_probe(remote, config=config)
    mac = _read_health(mac_health_url)
    value = {"contract_version": PROOF_CONTRACT, "fixture_only": False,
             **{key: record[key] for key in ("activation_id", "release", "rules_sha256", "canary_content_ids")},
             "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
             "database_path": str(db_path.resolve(strict=True)), "snapshot_contract": descriptor(),
             "mac_health_url": mac_health_url, "mac_health": mac,
             "server_probe": remote, "server_transition": remote.get("schema_transition")}
    validate_capture(value, record, current_mac=_read_health(mac_health_url))
    body = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    digest = hashlib.sha256(body).hexdigest()
    root = PROJECT_ROOT / "data/cache/media-consumer-proofs"
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root != root.resolve(strict=True) or stat.S_IMODE(root.stat().st_mode) != 0o700:
        raise ValueError("consumer_proof_directory_not_private")
    path = root / (digest + ".json")
    reference = {"path": str(path), "sha256": digest, "byte_size": len(body)}
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        _read_proof(reference)
    else:
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        directory_fd = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    return {"contract_version": PROOF_CONTRACT, "fixture_only": False, "consumer_receipt": reference}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    collector = commands.add_parser("capture", help="Read real runtimes; write a hash-bound proof only")
    collector.add_argument("--publisher-env", type=Path, required=True)
    collector.add_argument("--mac-health-url", default="http://127.0.0.1:8766/api/v8/health")
    activation_parser = commands.add_parser("activate", help="Append a mode receipt; never delete/download")
    activation_parser.add_argument("--mode", choices=("enrollment_only", "active", "paused"), required=True)
    activation_parser.add_argument("--activation-id", required=True)
    activation_parser.add_argument("--release", required=True)
    activation_parser.add_argument("--canary-content-id", type=int, action="append", default=[])
    activation_parser.add_argument("--consumer-receipt", type=Path)
    activation_parser.add_argument("--canary-bundle", action="append", default=[])
    args = parser.parse_args()
    from . import media_lifecycle as lifecycle
    from .storage import connect, is_formal_database_path, require_schema_compatibility, transaction
    db_path = args.db.resolve(strict=True)
    if is_formal_database_path(db_path) and PROJECT_ROOT != Path("/Users/mark/Projects/DcarAIGC"):
        raise ValueError("isolated_checkout_cannot_activate_formal_database")
    with connect(db_path, read_only=True) as connection:
        require_schema_compatibility(
            connection, supported_versions=frozenset({DATABASE_SCHEMA_VERSION})
        )
    if args.command == "capture":
        result = capture(db_path=db_path, publisher_env=args.publisher_env, mac_health_url=args.mac_health_url)
    else:
        proofs = None
        if args.consumer_receipt:
            body = args.consumer_receipt.read_bytes()
            reference = {"path": str(args.consumer_receipt), "sha256": hashlib.sha256(body).hexdigest(), "byte_size": len(body)}
            _read_proof(reference)
            proofs = {"contract_version": PROOF_CONTRACT, "fixture_only": False,
                      "consumer_receipt": reference, "canary_bundles": args.canary_bundle}
        with connect(db_path) as connection, transaction(connection):
            result = lifecycle.activate(connection, mode=args.mode, activation_id=args.activation_id,
                release=args.release, rules_sha256=descriptor()["media_retention_sha256"],
                canary_content_ids=tuple(args.canary_content_id), proofs=proofs)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
