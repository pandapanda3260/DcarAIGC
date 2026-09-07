#!/usr/bin/env python3
"""Verify, atomically install, or roll back a Dcar read-replica snapshot."""

from __future__ import annotations

import argparse
import ast
import fcntl
import grp
import hashlib
import json
import os
import pwd
import re
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from urllib.parse import quote
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterator, Mapping, Optional, Sequence


BUNDLE_SCHEMA = "dcar-read-replica-snapshot-v2"
ARTIFACT_POLICY = {
    "name": "thin-server-v2",
    "included": "reports-and-small-text-evidence",
    "optional_reuse": "active-same-path-size-sha256-only",
    "on_optional_missing_or_mismatch": "omitted",
    "delete_unlisted": False,
}
DATABASE_NAMES = frozenset({"dcar_insight.sqlite3", "web_mvp.sqlite3"})
SNAPSHOT_ID_RE = re.compile(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}")
SHA256_RE = re.compile(r"[0-9a-f]{64}")
RUNTIME_IDENTITY_SCHEMA = "dcar-runtime-identity-v1"
EXPECTED_REPORT_VERSION = "dcar-content-operations-report-v8.9"
EXPECTED_DATABASE_SCHEMA_VERSION = 19
EXPECTED_DATABASE_SCHEMA_MIGRATION = "dual-acquisition-profile-roster-v1"
EXPECTED_ACTIVE_RELEASE_ID = "evaluation-v9__selling-points-v5.2"
EXPECTED_ACTIVE_RELEASE_STATUS = "active"
EXPECTED_RULE_VERSION = "evaluation-v9"
EXPECTED_TAXONOMY_VERSION = "selling-points-v5.2"
EXPECTED_TAXONOMY_STATUS = "published"
RUNTIME_IDENTITY_KEYS = frozenset(
    {
        "schema",
        "report_version",
        "database_schema_version",
        "database_schema_migration",
        "active_release_id",
        "active_release_status",
        "rule_version",
        "taxonomy_version",
        "taxonomy_status",
        "matcher_rule_sha256",
    }
)
ServiceAction = Callable[[str], None]
SmokeCheck = Callable[[], None]
UpgradeServiceAction = Callable[[str, str], None]
CheckpointHook = Callable[[str], None]
LEGACY_SCHEMA_TRANSITION_CONTRACT = "dcar-schema17-to18-server-transition-v1"
SCHEMA_TRANSITION_CONTRACT = "dcar-schema18-to19-server-transition-v1"
PREDECESSOR_SCHEMA_TRANSITION_CONTRACT = "dcar-schema16-to17-server-transition-v1"
LEGACY_SCHEMA_SEAL_CONTRACT = "dcar-schema17-to18-release-seal-v1"
SCHEMA_SEAL_CONTRACT = "dcar-schema18-to19-release-seal-v1"
INTEGRATED_SCHEMA_TRANSITION_CONTRACT = "dcar-schema19-to20-server-transition-v1"
INTEGRATED_SCHEMA_SEAL_CONTRACT = "dcar-schema19-to20-release-seal-v1"
SUPPORTED_SCHEMA_VERSIONS = frozenset({17, 18, 19, 20})
SUPPORTED_SCHEMA_TRANSITIONS = frozenset({(17, 18), (18, 19), (19, 20)})
RELEASE_CONTRACTS = {
    17: ("dcar-content-operations-report-v8.7", "optional-account-phone"),
    18: ("dcar-content-operations-report-v8.8", "matrix-roster-source-routing"),
    19: (
        "dcar-content-operations-report-v8.9",
        "dual-acquisition-profile-roster-v1",
    ),
    20: ("dcar-content-operations-report-v8.9", "integrated-video-capture-v25"),
}
TRANSITION_CONTRACTS = {
    (17, 18): (LEGACY_SCHEMA_TRANSITION_CONTRACT, LEGACY_SCHEMA_SEAL_CONTRACT),
    (18, 19): (SCHEMA_TRANSITION_CONTRACT, SCHEMA_SEAL_CONTRACT),
    (19, 20): (INTEGRATED_SCHEMA_TRANSITION_CONTRACT, INTEGRATED_SCHEMA_SEAL_CONTRACT),
}
SCHEMA_SERVICES = ("dcar-auth.service", "dcar-douyin-control.service", "dcar-web.service", "dcar-api.service")
SETTLED_TRANSITIONS = frozenset({"succeeded", "rolled_back"})


class SnapshotInstallError(RuntimeError):
    """A snapshot operation could not be completed safely."""


@dataclass(frozen=True)
class InstallConfig:
    database_root: Path
    cache_root: Path
    reports_root: Path
    runtime_root: Path
    service: str = "dcar-api.service"
    health_url: str = "http://127.0.0.1:8765/api/v8/health"
    overview_url: str = "http://127.0.0.1:8765/api/v8/overview"
    scheduler_url: str = "http://127.0.0.1:8765/api/v8/scheduler"
    request_timeout_seconds: float = 120.0
    start_wait_seconds: float = 180.0
    owner_uid: int = field(default_factory=os.getuid)
    owner_gid: int = field(default_factory=os.getgid)
    current_release: Path = Path("/var/www/dcar-aigc/current")
    releases_root: Path = Path("/var/www/dcar-aigc/releases")
    systemd_root: Path = Path("/etc/systemd/system")
    nginx_root: Path = Path("/etc/nginx")
    config_root: Path = Path("/etc/dcar-aigc")

    @property
    def history_root(self) -> Path:
        return self.runtime_root / "snapshot-history"

    @property
    def lock_path(self) -> Path:
        return self.runtime_root / "snapshot-install.lock"

    @property
    def active_manifest_path(self) -> Path:
        return self.runtime_root / "active-snapshot.json"

    @property
    def transition_path(self) -> Path:
        return self.runtime_root / "schema-upgrade-transition.json"


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _release_contract(version: int) -> tuple[str, str]:
    try:
        return RELEASE_CONTRACTS[version]
    except KeyError as exc:
        raise SnapshotInstallError(
            f"schema{version} has no supported release contract"
        ) from exc


def _transition_contract(
    from_schema: int, to_schema: int
) -> tuple[str, str]:
    try:
        return TRANSITION_CONTRACTS[(from_schema, to_schema)]
    except KeyError as exc:
        raise SnapshotInstallError(
            "schema-upgrade supports only sealed 17-to-18, 18-to-19 or 19-to-20"
        ) from exc


def _validate_runtime_identity(
    value: object,
    *,
    label: str,
    expected_schema: int = EXPECTED_DATABASE_SCHEMA_VERSION,
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != RUNTIME_IDENTITY_KEYS:
        raise SnapshotInstallError(f"{label} runtime identity has an invalid shape")
    expected_report, expected_migration = _release_contract(expected_schema)
    expected = {
        "schema": RUNTIME_IDENTITY_SCHEMA,
        "report_version": expected_report,
        "database_schema_version": expected_schema,
        "database_schema_migration": expected_migration,
        "active_release_id": EXPECTED_ACTIVE_RELEASE_ID,
        "active_release_status": EXPECTED_ACTIVE_RELEASE_STATUS,
        "rule_version": EXPECTED_RULE_VERSION,
        "taxonomy_version": EXPECTED_TAXONOMY_VERSION,
        "taxonomy_status": EXPECTED_TAXONOMY_STATUS,
    }
    for key, expected_value in expected.items():
        if value.get(key) != expected_value:
            raise SnapshotInstallError(
                f"{label} runtime identity mismatch for {key}: "
                f"{value.get(key)!r}, expected {expected_value!r}"
            )
    matcher_sha = value.get("matcher_rule_sha256")
    if not isinstance(matcher_sha, str) or SHA256_RE.fullmatch(matcher_sha) is None:
        raise SnapshotInstallError(
            f"{label} runtime identity has an invalid matcher_rule_sha256"
        )
    return dict(value)


def _database_runtime_identity(
    path: Path, *, expected_schema: int = EXPECTED_DATABASE_SCHEMA_VERSION
) -> dict[str, Any]:
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        migration_rows = connection.execute(
            "SELECT name FROM schema_migrations WHERE version=?", (user_version,)
        ).fetchall()
        max_migration = connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()[0]
        release_rows = connection.execute(
            """
            SELECT er.id,er.rule_version,er.taxonomy_version,
                   er.matcher_rule_sha256,er.status release_status,
                   tv.status taxonomy_status
            FROM evaluation_releases er
            JOIN taxonomy_versions tv ON tv.version=er.taxonomy_version
            WHERE er.status='active'
            ORDER BY er.id
            """
        ).fetchall()
    except sqlite3.Error as exc:
        raise SnapshotInstallError(
            "snapshot database lacks the required runtime identity tables"
        ) from exc
    finally:
        connection.close()
    if len(migration_rows) != 1 or max_migration != user_version:
        raise SnapshotInstallError(
            "snapshot database has an ambiguous schema migration identity"
        )
    if len(release_rows) != 1:
        raise SnapshotInstallError(
            "snapshot database must have exactly one active evaluation release"
        )
    release = release_rows[0]
    expected_report, _ = _release_contract(expected_schema)
    return _validate_runtime_identity(
        {
            "schema": RUNTIME_IDENTITY_SCHEMA,
            "report_version": expected_report,
            "database_schema_version": user_version,
            "database_schema_migration": str(migration_rows[0]["name"]),
            "active_release_id": str(release["id"]),
            "active_release_status": str(release["release_status"]),
            "rule_version": str(release["rule_version"]),
            "taxonomy_version": str(release["taxonomy_version"]),
            "taxonomy_status": str(release["taxonomy_status"]),
            "matcher_rule_sha256": str(release["matcher_rule_sha256"]),
        },
        label="snapshot database", expected_schema=expected_schema,
    )


def _snapshot_modules() -> tuple[Any, Any]:
    source_root = str(Path(__file__).resolve().parents[2] / "src" / "dcar_eval")
    if source_root not in sys.path:
        sys.path.insert(0, source_root)
    from v8 import media_lifecycle, snapshot_contract
    return snapshot_contract, media_lifecycle


def _strict_schema(path: Path, version: int) -> None:
    _snapshot_modules()
    from v8 import storage
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        if version == 17:
            storage._validate_v17_structure(connection)
        elif version == 18:
            storage._validate_v18_structure(connection)
        elif version == 19:
            storage._validate_v19_structure(connection)
        elif version == 20:
            from v8.schema_v20 import validate_structure
            validate_structure(connection)
        else:
            raise SnapshotInstallError("unsupported schema transition version")
    except (ValueError, RuntimeError, sqlite3.Error) as exc:
        raise SnapshotInstallError("database does not have the exact sealed schema structure") from exc
    finally:
        connection.close()


def _file_record(path: Path, *, allow_interpreter_link: bool = False) -> dict[str, Any]:
    link = os.readlink(path) if path.is_symlink() else None
    if link is not None and not allow_interpreter_link:
        raise SnapshotInstallError(f"sealed file must not be a symlink: {path}")
    info = path.stat()
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or info.st_uid not in {0, os.getuid()} or info.st_mode & 0o022):
        raise SnapshotInstallError(f"sealed file is unsafe: {path}")
    digest = _sha256(path)
    after = path.stat()
    if (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise SnapshotInstallError(f"sealed file changed while reading: {path}")
    return {"sha256": digest, "byte_size": info.st_size, "mode": stat.S_IMODE(info.st_mode),
            "uid": info.st_uid, "gid": info.st_gid, "symlink_target": link}


def _canonical_directory(path: Path) -> Path:
    if not path.is_absolute() or path != path.resolve(strict=True) or not path.is_dir():
        raise SnapshotInstallError(f"directory must be absolute, real and canonical: {path}")
    return path


def _release_directory(path: Path, config: InstallConfig) -> Path:
    root = _canonical_directory(config.releases_root)
    path = _canonical_directory(path)
    if path.parent != root or path.name.startswith("."):
        raise SnapshotInstallError("release must be a direct named child of releases_root")
    if not (path / ".venv").is_dir() or (path / ".venv").is_symlink():
        raise SnapshotInstallError("release needs its independent .venv directory")
    return path


def _current_release(config: InstallConfig) -> Path:
    current = config.current_release
    if not current.is_absolute() or current.parent != current.parent.resolve(strict=True) or not current.is_symlink():
        raise SnapshotInstallError("current must be an explicit symlink under its real deployment directory")
    return _release_directory(current.resolve(strict=True), config)


def _code_inventory(release: Path) -> list[dict[str, Any]]:
    trees = ("src", "config", "deploy/server", "deploy/macos", "scripts", "app/web/dist")
    mandatory = ("src/dcar_eval/v8/storage.py", "src/dcar_eval/v8/contracts.py",
                 "deploy/server/install_snapshot.py", "deploy/macos/publish_snapshot.py",
                 "scripts/build_server_snapshot.py", "deploy/server/requirements-api.txt",
                 "pyproject.toml", "uv.lock", "app/web/package.json", "app/web/package-lock.json",
                 "app/web/node_modules/vinext/dist/cli.js",
                 ".venv/pyvenv.cfg", ".venv/bin/python")
    paths = set(mandatory)
    for tree in trees:
        root = _canonical_directory(release / tree)
        count = 0
        for directory, subdirectories, files in os.walk(root, followlinks=False):
            subdirectories[:] = [name for name in subdirectories if name != "__pycache__"]
            if any((Path(directory) / item).is_symlink() for item in subdirectories):
                raise SnapshotInstallError("release code directories must not contain aliases")
            for name in files:
                if name.endswith((".pyc", ".pyo")):
                    continue
                paths.add(str((Path(directory) / name).relative_to(release)))
                count += 1
        if not count:
            raise SnapshotInstallError(f"release code tree is empty: {tree}")
    records = []
    for relative in sorted(paths):
        record = _file_record(release / relative, allow_interpreter_link=relative == ".venv/bin/python")
        if relative == ".venv/bin/python" and not record["mode"] & 0o111:
            raise SnapshotInstallError("release interpreter is not executable")
        records.append({"path": relative, **record})
    return records


def _consumer_sha(release: Path) -> str:
    _snapshot_modules()
    from v8.media_consumer_proofs import code_sha256
    return code_sha256(release)


def _literal(path: Path, name: str) -> Any:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for statement in tree.body:
        if isinstance(statement, ast.Assign) and any(isinstance(target, ast.Name) and target.id == name for target in statement.targets):
            try:
                return ast.literal_eval(statement.value)
            except ValueError as exc:
                raise SnapshotInstallError(f"release constant is not literal: {name}") from exc
    raise SnapshotInstallError(f"release constant is missing: {name}")


def _verify_release_contract(release: Path, version: int) -> None:
    expected_report, expected_migration = _release_contract(version)
    # Schema20 is deliberately explicit; bootstrap remains19. Verify its real
    # migration/read-support closure instead of changing global fixture defaults.
    bootstrap = 19 if version == 20 else version
    checks: tuple[tuple[str, str, Any], ...] = (("src/dcar_eval/v8/storage.py", "SCHEMA_VERSION", bootstrap),
              ("src/dcar_eval/v8/storage.py", "CURRENT_SCHEMA_MIGRATION_NAME", _release_contract(bootstrap)[1]),
              ("src/dcar_eval/v8/contracts.py", "CURRENT_REPORT_VERSION", expected_report))
    if version == 20:
        checks += (("src/dcar_eval/v8/storage.py", "LATEST_SCHEMA_VERSION", 20),
                   ("src/dcar_eval/v8/schema_v20.py", "SCHEMA_VERSION", 20),
                   ("src/dcar_eval/v8/schema_v20.py", "MIGRATION_NAME", expected_migration))
        migrations = _literal(release / "src/dcar_eval/v8/storage.py", "SCHEMA_MIGRATION_NAMES")
        if not isinstance(migrations, dict) or migrations.get(20) != expected_migration:
            raise SnapshotInstallError("sealed release does not explicitly support schema20 reads")
        definitions = {node.name for node in ast.parse((release / "src/dcar_eval/v8/schema_v20.py").read_text()).body
                       if isinstance(node, ast.FunctionDef)}
        if not {"migrate", "validate_structure", "validate_lineage"} <= definitions:
            raise SnapshotInstallError("sealed release has no complete schema20 migration contract")
    if version in {18, 19}:
        checks += tuple((name, constant, expected) for name in (
            "deploy/server/install_snapshot.py", "deploy/macos/publish_snapshot.py", "scripts/build_server_snapshot.py"
        ) for constant, expected in (
            ("EXPECTED_DATABASE_SCHEMA_VERSION", version),
            ("EXPECTED_DATABASE_SCHEMA_MIGRATION", expected_migration),
            ("EXPECTED_REPORT_VERSION", expected_report),
        ))
    for name, constant, expected in checks:
        if _literal(release / name, constant) != expected:
            raise SnapshotInstallError(f"sealed release has incompatible {name}:{constant}")


def _config_targets(config: InstallConfig) -> dict[str, Path]:
    return {
        **{"systemd/" + service: config.systemd_root / service for service in SCHEMA_SERVICES},
        "nginx/dcar-http.conf": config.nginx_root / "conf.d" / "dcar-http.conf",
        "nginx/dcar-proxy.conf": config.nginx_root / "snippets" / "dcar-proxy.conf",
        "config/dcar.env": config.config_root / "dcar.env",
        "config/douyin-stage1.env": config.config_root / "douyin-stage1.env",
    }


def _configuration_plan(release: Path, config: InstallConfig) -> list[dict[str, Any]]:
    result = []
    for key, target in sorted(_config_targets(config).items()):
        if not target.is_absolute() or target.parent != target.parent.resolve():
            raise SnapshotInstallError("configuration target parent is an alias")
        exists = target.exists() or target.is_symlink()
        previous = _file_record(target) if exists else None
        source = None if key.startswith("config/") else "deploy/server/" + key
        new = _file_record(release / source) if source else None
        result.append({"key": key, "target": str(target), "source": source, "previous": previous, "new": new})
    return result


def _database_inventory(config: InstallConfig) -> list[dict[str, Any]]:
    records = []
    for name in sorted(DATABASE_NAMES):
        for suffix in ("", "-wal", "-shm"):
            path = config.database_root / (name + suffix)
            if path.exists() or path.is_symlink():
                records.append({"name": path.name, **_file_record(path)})
    if not any(item["name"] == "dcar_insight.sqlite3" for item in records):
        raise SnapshotInstallError("old database is missing")
    return records


def _config_roots(config: InstallConfig) -> dict[str, str]:
    names = ("database_root", "cache_root", "reports_root", "runtime_root", "current_release",
             "releases_root", "systemd_root", "nginx_root", "config_root")
    return {name: str(getattr(config, name)) for name in names}


def seal_schema_upgrade(
    bundle: Path,
    config: InstallConfig,
    *,
    release_dir: Path,
    expected_current_release: Path,
    from_schema: int,
    to_schema: int,
) -> dict[str, Any]:
    """Write only a verified seal in the staged release; no service or data change."""
    transition_contract, seal_contract = _transition_contract(
        from_schema, to_schema
    )
    with _install_lock(config):
        _assert_transition_settled(config)
        old = _release_directory(expected_current_release, config)
        new = _release_directory(release_dir, config)
        if new == old or _current_release(config) != old:
            raise SnapshotInstallError("current release does not match the declared old release")
        _verify_release_contract(old, from_schema)
        _verify_release_contract(new, to_schema)
        active = config.database_root / "dcar_insight.sqlite3"
        _validate_sqlite(active, expected_user_version=from_schema)
        _strict_schema(active, from_schema)
        old_identity = _database_runtime_identity(
            active, expected_schema=from_schema
        )
        old_services = _service_states()
        _require_running_services(old_services)
        manifest = verify_bundle(bundle, config, expected_schema=to_schema)
        _strict_schema(_bundle_member(bundle, next(item["bundle_path"] for item in manifest["databases"]
                                                if item["name"] == "dcar_insight.sqlite3")), to_schema)
        receipt = _file_record(config.active_manifest_path) if config.active_manifest_path.exists() else None
        seal = {
            "schema": seal_contract,
            "transition_schema": transition_contract,
            "from_schema": from_schema,
            "to_schema": to_schema,
            "old_release": str(old), "new_release": str(new), "roots": _config_roots(config),
            "old_databases": _database_inventory(config), "old_runtime_identity": old_identity,
            "old_active_receipt": receipt, "old_code": _code_inventory(old), "new_code": _code_inventory(new),
            "old_services": old_services,
            "snapshot_id": manifest["snapshot_id"], "snapshot_manifest_sha256": _sha256(bundle / "manifest.json"),
            "runtime_identity": manifest["runtime_identity"], "snapshot_contract": manifest["snapshot_contract"],
            "code_sha256": _consumer_sha(new),
            "configurations": _configuration_plan(new, config), "created_at": _utc_now(),
        }
        path = new / "schema-upgrade-manifest.json"
        checksum = new / "schema-upgrade-manifest.sha256"
        if path.exists() or checksum.exists():
            previous = _read_release_seal(new)
            comparable = {key: value for key, value in previous.items() if key != "created_at"}
            if comparable != {key: value for key, value in seal.items() if key != "created_at"}:
                raise SnapshotInstallError("existing release seal differs; it cannot be overwritten")
            return previous
        _write_json_atomic(path, seal)
        _write_bytes_atomic(checksum, (_sha256(path) + "  " + path.name + "\n").encode("ascii"))
        return seal


def _read_release_seal(release: Path) -> dict[str, Any]:
    path = release / "schema-upgrade-manifest.json"
    checksum_path = release / "schema-upgrade-manifest.sha256"
    _file_record(checksum_path)
    checksum = checksum_path.read_text(encoding="ascii")
    _file_record(path)
    if checksum != _sha256(path) + "  " + path.name + "\n":
        raise SnapshotInstallError("schema-upgrade seal checksum does not match")
    seal = _read_object(path)
    pair = (seal.get("from_schema"), seal.get("to_schema"))
    if any(type(item) is not int for item in pair) or pair not in SUPPORTED_SCHEMA_TRANSITIONS:
        raise SnapshotInstallError("unsupported schema-upgrade seal")
    transition_contract, seal_contract = _transition_contract(seal["from_schema"], seal["to_schema"])
    if (
        seal.get("schema") != seal_contract
        or seal.get("transition_schema") != transition_contract
    ):
        raise SnapshotInstallError("unsupported schema-upgrade seal")
    return seal


def _read_object(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise SnapshotInstallError(f"required JSON is missing or unsafe: {path}")
    try:
        value = json.loads(path.read_bytes())
    except (ValueError, UnicodeError) as exc:
        raise SnapshotInstallError(f"required JSON is invalid: {path}") from exc
    if not isinstance(value, dict):
        raise SnapshotInstallError(f"required JSON must be an object: {path}")
    return value


def _assert_transition_settled(config: InstallConfig) -> None:
    path = config.transition_path
    if not path.exists() and not path.is_symlink():
        return
    value = _read_object(path)
    pair = (value.get("from_schema"), value.get("to_schema"))
    current_settled = all(type(item) is int for item in pair) and pair in SUPPORTED_SCHEMA_TRANSITIONS and (
        value.get("schema") == _transition_contract(value["from_schema"], value["to_schema"])[0]
        and value.get("status") in SETTLED_TRANSITIONS
    )
    predecessor_succeeded = (
        value.get("schema") == PREDECESSOR_SCHEMA_TRANSITION_CONTRACT
        and value.get("status") == "succeeded"
        and value.get("from_schema") == 16
        and value.get("to_schema") == 17
    )
    if (
        not (current_settled or predecessor_succeeded)
        or not isinstance(value.get("completed_at"), str)
    ):
        raise SnapshotInstallError("unsettled schema transition blocks normal snapshot operations")
    try:
        datetime.fromisoformat(value["completed_at"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise SnapshotInstallError("schema transition completion time is invalid") from exc


def _project_path(value: Any, writer_root: str) -> str:
    if not isinstance(value, str) or not value or any(part in value for part in ("\x00", "\n", "\r", "\\")):
        raise SnapshotInstallError("artifact project path is invalid")
    path = PurePosixPath(value)
    if path.is_absolute():
        try:
            path = path.relative_to(PurePosixPath(writer_root))
        except ValueError as exc:
            raise SnapshotInstallError("artifact absolute path is outside the writer project") from exc
    if any(part in {"", ".", ".."} for part in str(path).split("/")):
        raise SnapshotInstallError("artifact project path is not canonical")
    if not ((path.parts[:2] == ("data", "cache") and len(path.parts) > 2)
            or (path.parts[:1] == ("reports",) and len(path.parts) > 1)):
        raise SnapshotInstallError("artifact project path is outside cache/reports")
    return str(path)


def _writer_root(manifest: Mapping[str, Any]) -> str:
    value = manifest.get("writer_project_root")
    if (not isinstance(value, str) or not PurePosixPath(value).is_absolute()
            or len(PurePosixPath(value).parts) < 3
            or any(part in {"", ".", ".."} for part in value.split("/")[1:])):
        raise SnapshotInstallError("snapshot writer_project_root is invalid")
    return value


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink() or not path.is_dir():
        raise SnapshotInstallError(f"required directory is unsafe: {path}")


def _apply_owner(path: Path, config: InstallConfig) -> None:
    try:
        os.chown(path, config.owner_uid, config.owner_gid, follow_symlinks=False)
    except PermissionError as exc:
        if (config.owner_uid, config.owner_gid) != (os.getuid(), os.getgid()):
            raise SnapshotInstallError(
                f"cannot set replica ownership on {path}; run installer with sudo"
            ) from exc


def _ensure_managed_directory(path: Path, config: InstallConfig) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o750)
    if path.is_symlink() or not path.is_dir():
        raise SnapshotInstallError(f"managed directory is unsafe: {path}")
    os.chmod(path, 0o750, follow_symlinks=False)
    _apply_owner(path, config)


def _ensure_managed_parent(root: Path, target: Path, config: InstallConfig) -> None:
    _ensure_managed_directory(root, config)
    try:
        relative_parent = target.parent.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise SnapshotInstallError(f"managed target escapes its root: {target}") from exc
    current = root
    for part in relative_parent.parts:
        current = current / part
        _ensure_managed_directory(current, config)


def _apply_file_access(path: Path, config: InstallConfig) -> None:
    if path.is_symlink() or not path.is_file():
        raise SnapshotInstallError(f"managed file is unsafe: {path}")
    os.chmod(path, 0o640, follow_symlinks=False)
    _apply_owner(path, config)


@contextmanager
def _install_lock(config: InstallConfig) -> Iterator[None]:
    _ensure_private_directory(config.runtime_root)
    descriptor = os.open(config.lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise SnapshotInstallError("snapshot install lock is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SnapshotInstallError("snapshot install lock is busy") from exc
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _read_manifest(bundle: Path) -> dict[str, Any]:
    bundle = bundle.resolve()
    if bundle.is_symlink() or not bundle.is_dir():
        raise SnapshotInstallError(f"bundle is not a regular directory: {bundle}")
    manifest_path = bundle / "manifest.json"
    checksum_path = bundle / "manifest.sha256"
    if (
        manifest_path.is_symlink()
        or checksum_path.is_symlink()
        or not manifest_path.is_file()
        or not checksum_path.is_file()
    ):
        raise SnapshotInstallError("bundle manifest files are missing or unsafe")
    checksum_fields = checksum_path.read_text(encoding="ascii").strip().split()
    if len(checksum_fields) != 2 or checksum_fields[1] != "manifest.json":
        raise SnapshotInstallError("manifest.sha256 has an invalid shape")
    expected_manifest_sha = checksum_fields[0]
    if not SHA256_RE.fullmatch(expected_manifest_sha):
        raise SnapshotInstallError("manifest.sha256 does not contain a SHA-256")
    if _sha256(manifest_path) != expected_manifest_sha:
        raise SnapshotInstallError("manifest SHA-256 mismatch")
    try:
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotInstallError("manifest JSON is invalid") from exc
    if not isinstance(value, dict) or value.get("schema") != BUNDLE_SCHEMA:
        raise SnapshotInstallError("unsupported snapshot bundle schema")
    snapshot_id = value.get("snapshot_id")
    if not isinstance(snapshot_id, str) or not SNAPSHOT_ID_RE.fullmatch(snapshot_id):
        raise SnapshotInstallError("snapshot_id is invalid")
    return value


def _bundle_member(bundle: Path, value: str) -> Path:
    if not value or "\x00" in value or "\\" in value:
        raise SnapshotInstallError("bundle path is unsafe")
    relative = PurePosixPath(value)
    if relative.is_absolute() or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise SnapshotInstallError(f"bundle path is unsafe: {value}")
    candidate = (bundle / Path(*relative.parts)).resolve()
    root = bundle.resolve()
    if root not in candidate.parents:
        raise SnapshotInstallError(f"bundle path escapes its root: {value}")
    return candidate


def _validate_sqlite(
    path: Path, *, expected_user_version: Optional[int]
) -> dict[str, Any]:
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        quick_rows = [
            str(row[0]) for row in connection.execute("PRAGMA quick_check").fetchall()
        ]
        if quick_rows != ["ok"]:
            raise SnapshotInstallError(
                f"SQLite quick_check failed for {path.name}: {quick_rows[:5]}"
            )
        foreign_key_rows = connection.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_rows:
            raise SnapshotInstallError(
                f"SQLite foreign_key_check failed for {path.name}: "
                f"{len(foreign_key_rows)} violation(s)"
            )
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    finally:
        connection.close()
    if expected_user_version is not None and user_version != expected_user_version:
        raise SnapshotInstallError(
            f"SQLite schema mismatch for {path.name}: {user_version}, "
            f"expected {expected_user_version}"
        )
    return {
        "quick_check": "ok",
        "foreign_key_violations": 0,
        "user_version": user_version,
    }


def _artifact_target(config: InstallConfig, item: Mapping[str, Any]) -> Path:
    root_name = item.get("root")
    relative_value = item.get("path")
    if root_name not in {"cache", "reports"} or not isinstance(relative_value, str):
        raise SnapshotInstallError("artifact manifest entry is invalid")
    relative = PurePosixPath(relative_value)
    if relative.is_absolute() or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise SnapshotInstallError(f"artifact path is unsafe: {relative_value}")
    root = (
        config.cache_root if root_name == "cache" else config.reports_root
    ).resolve()
    candidate = root / Path(*relative.parts)
    if root not in candidate.resolve().parents:
        raise SnapshotInstallError(f"artifact path escapes its root: {relative_value}")
    return candidate


def _staged_artifact_target(bundle: Path, item: Mapping[str, Any]) -> Path:
    root_name = item.get("root")
    relative_value = item.get("path")
    if root_name not in {"cache", "reports"} or not isinstance(relative_value, str):
        raise SnapshotInstallError("artifact manifest entry is invalid")
    relative = PurePosixPath(relative_value)
    if relative.is_absolute() or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise SnapshotInstallError(f"artifact path is unsafe: {relative_value}")
    staged_root = (bundle.parent / "artifacts" / str(root_name)).resolve()
    candidate = staged_root / Path(*relative.parts)
    if staged_root not in candidate.resolve().parents:
        raise SnapshotInstallError(
            f"staged artifact path escapes its root: {relative_value}"
        )
    return candidate


def _path_contains_symlink(root: Path, target: Path) -> bool:
    current = root
    for part in target.relative_to(root).parts:
        current /= part
        if current.is_symlink():
            return True
    return False


def _verified_artifact_source(
    bundle: Path, item: Mapping[str, Any], config: InstallConfig
) -> Path:
    staged = _staged_artifact_target(bundle, item)
    staged_root = (bundle.parent / "artifacts" / str(item["root"])).resolve()
    if _path_contains_symlink(staged_root, staged):
        raise SnapshotInstallError(f"staged artifact is unsafe: {staged}")
    source = staged if staged.exists() else _artifact_target(config, item)
    active_root = (
        config.cache_root if item["root"] == "cache" else config.reports_root
    ).resolve()
    if (
        not source.is_file()
        or source.stat().st_nlink != 1
        or (source == _artifact_target(config, item) and _path_contains_symlink(active_root, source))
    ):
        raise SnapshotInstallError(f"artifact is missing or unsafe: {source}")
    if (
        source.stat().st_size != item["byte_size"]
        or _sha256(source) != item["sha256"]
    ):
        raise SnapshotInstallError(f"artifact drifted: {source}")
    return source


def verify_bundle(
    bundle: Path,
    config: InstallConfig,
    *,
    verify_artifacts: bool = True,
    expected_schema: int = EXPECTED_DATABASE_SCHEMA_VERSION,
) -> dict[str, Any]:
    bundle = bundle.resolve()
    manifest = _read_manifest(bundle)
    expected_runtime_identity = _validate_runtime_identity(
        manifest.get("runtime_identity"),
        label="snapshot manifest",
        expected_schema=expected_schema,
    )
    if manifest.get("artifact_policy") != ARTIFACT_POLICY:
        raise SnapshotInstallError("snapshot artifact policy is missing or unsupported")
    contract, _ = _snapshot_modules()
    try:
        contract.validate_descriptor(manifest.get("snapshot_contract"))
    except ValueError as exc:
        raise SnapshotInstallError("snapshot consumer contract is invalid") from exc
    _writer_root(manifest)
    raw_databases = manifest.get("databases")
    if not isinstance(raw_databases, list) or not raw_databases:
        raise SnapshotInstallError("snapshot contains no databases")
    seen_names: set[str] = set()
    for item in raw_databases:
        if not isinstance(item, dict):
            raise SnapshotInstallError("database manifest entry is invalid")
        name = item.get("name")
        bundle_path = item.get("bundle_path")
        expected_sha = item.get("sha256")
        expected_size = item.get("byte_size")
        expected_user_version = item.get("user_version")
        if (
            name not in DATABASE_NAMES
            or name in seen_names
            or not isinstance(bundle_path, str)
            or not isinstance(expected_sha, str)
            or not SHA256_RE.fullmatch(expected_sha)
            or not isinstance(expected_size, int)
            or expected_size <= 0
            or not isinstance(expected_user_version, int)
        ):
            raise SnapshotInstallError("database manifest entry is invalid")
        seen_names.add(name)
        source = _bundle_member(bundle, bundle_path)
        if source.is_symlink() or not source.is_file():
            raise SnapshotInstallError(f"database payload is missing or unsafe: {name}")
        if source.stat().st_size != expected_size or _sha256(source) != expected_sha:
            raise SnapshotInstallError(f"database payload drifted: {name}")
        validation = _validate_sqlite(
            source, expected_user_version=expected_user_version
        )
        if (
            item.get("quick_check") != validation["quick_check"]
            or item.get("foreign_key_violations")
            != validation["foreign_key_violations"]
        ):
            raise SnapshotInstallError(f"database validation manifest drifted: {name}")
        if name == "dcar_insight.sqlite3":
            if expected_user_version != expected_schema:
                raise SnapshotInstallError(
                    "snapshot main database schema does not match the requested contract"
                )
            database_runtime_identity = _database_runtime_identity(
                source, expected_schema=expected_schema
            )
            if database_runtime_identity != expected_runtime_identity:
                raise SnapshotInstallError(
                    "snapshot database runtime identity does not match the manifest"
                )
            if expected_schema == 20:
                _strict_schema(source, 20)
                _verify_schema20_deployment(source, manifest, bundle=bundle)
    if "dcar_insight.sqlite3" not in seen_names:
        raise SnapshotInstallError("snapshot omits dcar_insight.sqlite3")
    raw_files = manifest.get("files")
    if not isinstance(raw_files, list):
        raise SnapshotInstallError("artifact manifest is invalid")
    if manifest.get("file_count") != len(raw_files):
        raise SnapshotInstallError("artifact count does not match manifest")
    seen_files: set[tuple[str, str]] = set()
    byte_total = 0
    file_set_digest = hashlib.sha256()
    for item in raw_files:
        if not isinstance(item, dict):
            raise SnapshotInstallError("artifact manifest entry is invalid")
        root_name = item.get("root")
        relative_path = item.get("path")
        expected_sha = item.get("sha256")
        expected_size = item.get("byte_size")
        if (
            root_name not in {"cache", "reports"}
            or not isinstance(relative_path, str)
            or not isinstance(expected_sha, str)
            or not SHA256_RE.fullmatch(expected_sha)
            or not isinstance(expected_size, int)
            or expected_size < 0
        ):
            raise SnapshotInstallError("artifact manifest entry is invalid")
        identity = (root_name, relative_path)
        if identity in seen_files:
            raise SnapshotInstallError(f"duplicate artifact entry: {relative_path}")
        seen_files.add(identity)
        byte_total += expected_size
        file_set_digest.update(
            (f"{root_name}\0{relative_path}\0{expected_size}\0{expected_sha}\n").encode(
                "utf-8"
            )
        )
        if verify_artifacts:
            _verified_artifact_source(bundle, item, config)
    if manifest.get("file_byte_size") != byte_total:
        raise SnapshotInstallError("artifact byte total does not match manifest")
    if manifest.get("file_set_sha256") != file_set_digest.hexdigest():
        raise SnapshotInstallError("artifact set SHA-256 does not match manifest")
    raw_optional = manifest.get("optional_reuse_files")
    if not isinstance(raw_optional, list):
        raise SnapshotInstallError("optional-reuse artifact manifest is invalid")
    if manifest.get("optional_reuse_file_count") != len(raw_optional):
        raise SnapshotInstallError(
            "optional-reuse artifact count does not match manifest"
        )
    seen_optional: set[tuple[str, str]] = set()
    optional_byte_total = 0
    optional_set_digest = hashlib.sha256()
    for item in raw_optional:
        if not isinstance(item, dict):
            raise SnapshotInstallError("optional-reuse artifact entry is invalid")
        root_name = item.get("root")
        relative_path = item.get("path")
        expected_sha = item.get("sha256")
        expected_size = item.get("byte_size")
        reason = item.get("reason")
        if (
            root_name not in {"cache", "reports"}
            or not isinstance(relative_path, str)
            or not isinstance(expected_sha, str)
            or not SHA256_RE.fullmatch(expected_sha)
            or not isinstance(expected_size, int)
            or expected_size < 0
            or reason not in {"large_binary", "source_missing"}
        ):
            raise SnapshotInstallError("optional-reuse artifact entry is invalid")
        identity = (str(root_name), relative_path)
        if identity in seen_optional or identity in seen_files:
            raise SnapshotInstallError(
                f"duplicate optional-reuse artifact entry: {relative_path}"
            )
        seen_optional.add(identity)
        optional_byte_total += expected_size
        optional_set_digest.update(
            (f"{root_name}\0{relative_path}\0{expected_size}\0{expected_sha}\n").encode(
                "utf-8"
            )
        )
        # This only validates path safety. Optional-reuse files are deliberately
        # absent from staging and are never required for installation.
        _artifact_target(config, item)
    if manifest.get("optional_reuse_byte_size") != optional_byte_total:
        raise SnapshotInstallError(
            "optional-reuse artifact byte total does not match manifest"
        )
    if manifest.get("optional_reuse_set_sha256") != optional_set_digest.hexdigest():
        raise SnapshotInstallError(
            "optional-reuse artifact set SHA-256 does not match manifest"
        )
    main_database = next(item for item in raw_databases if item["name"] == "dcar_insight.sqlite3")
    _verify_managed_originals(
        bundle,
        manifest,
        _bundle_member(bundle, main_database["bundle_path"]),
        config,
        verify_artifacts=verify_artifacts,
    )
    return manifest


def _verify_portable_release_decision(connection: sqlite3.Connection, proof: Mapping[str, Any],
                                      payload: Mapping[str, Any], *, recorded_at: str) -> None:
    """Rebind the Writer-verified canonical decision to its immutable DB rows.

    Mac private files remain referenced, never opened on the snapshot reader.
    This is installation provenance only; it does not qualify or send requests.
    """
    from v8.capture_release import CONTINUITY_OPERATIONS
    from v8.raw_evidence import canonical_json_bytes

    decision = proof.get("release_decision")
    if not isinstance(decision, dict) or proof.get("e2e_status") != "deferred":
        raise ValueError("portable deferred release decision missing")
    body = {key: value for key, value in decision.items() if key != "decision_sha256"}
    expected = {"contract_version": "v25-user-release-decision-v1", "business_e2e": "deferred_by_user",
        "production_rollout": "approved_by_user", "transport_qualification": "not_verified",
        "approved_target_profile": "integrated_route_v1"}
    if (set(body) != {*expected, "actor", "reason", "operations", "issued_at", "candidate_id",
                    "candidate_receipt_sha256", "bindings", "runtime_bindings", "transport_manifest", "runtime_evidence"}
            or any(body.get(key) != value for key, value in expected.items())):
        raise ValueError("portable decision claims unknown or qualified authority")
    ref = payload["evidence"]["release_decision"]
    if (not isinstance(ref, dict) or ref.get("result") != "deferred_by_user"
            or not Path(str(ref.get("path", ""))).is_absolute()
            or not SHA256_RE.fullmatch(str(ref.get("sha256")))
            or decision.get("decision_sha256") != ref["sha256"]
            or hashlib.sha256(canonical_json_bytes(body)).hexdigest() != ref["sha256"]):
        raise ValueError("portable decision differs from its original immutable file hash")
    operations = body["operations"]
    if (not isinstance(operations, list) or not operations or any(not isinstance(op, str) for op in operations)
            or operations != sorted(set(operations)) or not set(operations).issubset(CONTINUITY_OPERATIONS)):
        raise ValueError("portable decision operation scope invalid")
    for key, maximum in (("actor", 128), ("reason", 2000)):
        if not isinstance(body[key], str) or not body[key].strip() or len(body[key]) > maximum:
            raise ValueError("portable decision actor/reason missing")
    candidate = connection.execute("SELECT * FROM deployment_readiness_receipts WHERE deployment_id=?",
                                   (body["candidate_id"],)).fetchone()
    if (candidate is None or candidate["status"] != "candidate"
            or body["candidate_id"] != payload.get("candidate_id")
            or candidate["receipt_sha256"] != body["candidate_receipt_sha256"]):
        raise ValueError("portable decision candidate missing or changed")
    prior = json.loads(candidate["payload_json"])
    envelope = {"deployment_id": candidate["deployment_id"], "status": "candidate", "payload": prior,
                "recorded_at": candidate["recorded_at"]}
    if hashlib.sha256(json.dumps(envelope, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode()).hexdigest() != candidate["receipt_sha256"]:
        raise ValueError("portable decision candidate digest invalid")
    original = dict(payload)
    original.pop("acceptance_mode", None)
    original.pop("candidate_id", None)
    original["evidence"] = {key: value for key, value in payload["evidence"].items() if key != "release_decision"}
    if original != prior or body["bindings"] != prior["bindings"] or payload.get("coverage_complete") is not False:
        raise ValueError("portable deferred acceptance changed candidate evidence or coverage")
    times = [datetime.fromisoformat(value.replace("Z", "+00:00"))
             for value in (candidate["recorded_at"], body["issued_at"], recorded_at)]
    if any(value.tzinfo is None for value in times) or not times[0] <= times[1] <= times[2]:
        raise ValueError("portable decision time differs")
    runtime = body["runtime_bindings"]
    if (not isinstance(runtime, dict) or set(runtime) != {"build_sha256", "runtime_sha256", "config_sha256"}
            or any(not SHA256_RE.fullmatch(str(value)) for value in runtime.values())
            or runtime["config_sha256"] != prior["bindings"]["config_sha256"]
            or body["transport_manifest"] != prior["configuration_evidence"]["transport_manifest"]):
        raise ValueError("portable decision runtime/config/transport differs")
    refs = body["runtime_evidence"]
    if not isinstance(refs, dict) or set(refs) != {"build", "runtime"}:
        raise ValueError("portable decision installed proof missing")
    for key in ("build", "runtime"):
        ref = refs[key]
        if (not isinstance(ref, dict) or not Path(str(ref.get("path", ""))).is_absolute()
                or ref.get("result") != "passed" or ref.get("sha256") != runtime[f"{key}_sha256"]):
            raise ValueError("portable decision installed reference differs")


def _verify_schema20_deployment(database: Path, manifest: Mapping[str, Any], *, bundle: Path) -> None:
    """Bind portable Writer proof to snapshot rows; never grant paid authority.

    build_snapshot validates the original external evidence/storage on Writer.
    The server cannot restat Mac paths and preserves candidate/coverage states.
    """
    proof = manifest.get("deployment_readiness")
    if not isinstance(proof, dict):
        raise SnapshotInstallError("schema20 snapshot has no deployment readiness proof")
    connection = sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute("SELECT * FROM deployment_readiness_receipts WHERE deployment_id=?",
                                 (proof.get("deployment_id"),)).fetchone()
        if row is None:
            raise ValueError("deployment receipt missing")
        payload = json.loads(row["payload_json"])
        envelope = {"deployment_id": row["deployment_id"], "status": row["status"],
                    "payload": payload, "recorded_at": row["recorded_at"]}
        digest = hashlib.sha256(json.dumps(envelope, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode()).hexdigest()
        accepted = row["status"] == "accepted"
        if (row["status"] not in {"candidate", "accepted"}
                or proof.get("receipt_sha256") != row["receipt_sha256"] or digest != row["receipt_sha256"]
                or proof.get("status") != row["status"]
                or payload.get("contract_version") != "v25-bounded-deployment-readiness-v2-local-retention"
                or proof.get("contract_version") != payload["contract_version"]
                or payload.get("schema_version") != 20 or payload.get("schema_migration") != RELEASE_CONTRACTS[20][1]
                or proof.get("bindings") != payload.get("bindings")
                or proof.get("evidence") != payload.get("evidence")
                or proof.get("validation_scope") != "release"
                or proof.get("deployment_eligible") is not accepted
                or type(proof.get("coverage_complete")) is not bool
                or proof["coverage_complete"] is True and payload.get("coverage_complete") is not True):
            raise ValueError("deployment proof differs from the frozen Writer receipt")
        bindings = payload["bindings"]
        if any(not SHA256_RE.fullmatch(str(bindings.get(key))) for key in
               ("build_sha256", "runtime_sha256", "config_sha256", "activation_sha256", "roster_members_sha256")):
            raise ValueError("deployment binding digest invalid")
        mode = payload.get("acceptance_mode")
        deferred = mode == "user_authorized_deferred_e2e"
        if mode not in {None, "user_authorized_deferred_e2e"}:
            raise ValueError("unknown deployment acceptance mode")
        if deferred:
            if not accepted or "bounded_e2e" in payload["evidence"] or "native_cohort_id" in payload:
                raise ValueError("deferred acceptance cannot also claim verified E2E")
            _verify_portable_release_decision(connection, proof, payload, recorded_at=row["recorded_at"])
        elif "release_decision" in payload["evidence"] or proof.get("release_decision") is not None:
            raise ValueError("release decision requires explicit deferred acceptance")
        elif accepted and proof.get("e2e_status", "passed") != "passed":
            raise ValueError("verified E2E acceptance cannot claim deferred status")
        for key in ("source_archive", "migration", "rollback", "full_checks", "install",
                    *(["bounded_e2e"] if accepted and not deferred else [])):
            reference = payload["evidence"][key]
            if (not isinstance(reference, dict) or not Path(str(reference.get("path", ""))).is_absolute()
                    or not SHA256_RE.fullmatch(str(reference.get("sha256"))) or reference.get("result") != "passed"):
                raise ValueError("deployment external evidence reference invalid")
        active = connection.execute("SELECT * FROM acquisition_profile_activations WHERE id=?",
                                    (bindings["activation_id"],)).fetchone()
        if active is None or any(active[key] != bindings[key] for key in
                ("profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256")):
            raise ValueError("deployment activation differs")
        if connection.execute("SELECT 1 FROM activation_cancellations WHERE activation_id=?", (active["id"],)).fetchone():
            raise ValueError("deployment activation cancelled")
        receipt_path = bundle / "snapshot-source-receipt.json"
        source_receipt = None
        if receipt_path.exists() or receipt_path.is_symlink():
            if receipt_path.is_symlink() or not receipt_path.is_file():
                raise ValueError("snapshot source receipt is unsafe")
            source_receipt = _read_object(receipt_path)
            if source_receipt.get("manifest_sha256") != _sha256(bundle / "manifest.json"):
                raise ValueError("snapshot source receipt manifest differs")
        _verify_schema20_activation(connection, proof, manifest, source_receipt=source_receipt)
    except (KeyError, TypeError, ValueError, sqlite3.Error) as exc:
        raise SnapshotInstallError("schema20 portable deployment proof is invalid") from exc
    finally:
        connection.close()


def _verify_schema20_activation(connection: sqlite3.Connection, deployment: Mapping[str, Any],
                                manifest: Mapping[str, Any], *, source_receipt: Mapping[str, Any] | None) -> None:
    """Recompute only the portable, legal successor chain; never a paid gate."""
    from v8 import capture_authorizations as auth
    from v8.capture_activation_release import validate_installed_activation_successor
    from v8.capture_code_successor import validate_portable
    from v8.profile_activations import activation_at

    try:
        at = manifest["created_at"]
        published_successor = None
        published_code_successor = None
        if source_receipt is not None:
            envelope = dict(source_receipt)
            claimed = envelope.pop("payload_sha256", None)
            publication = envelope["publication_evidence"]
            if (envelope.get("contract_version") != "snapshot-source-receipt-v1"
                    or claimed != auth.digest(envelope)
                    or envelope.get("snapshot_id") != manifest["snapshot_id"]
                    or envelope.get("runtime_identity") != manifest["runtime_identity"]
                    or envelope.get("publication_evidence_sha256") != auth.digest(publication)):
                raise ValueError("snapshot source receipt differs")
            at = publication["verified_at"]
            published_successor = publication.get("activation_successor")
            published_code_successor = publication.get("code_successor")
        parsed_at = datetime.fromisoformat(at.replace("Z", "+00:00"))
        created = datetime.fromisoformat(manifest["created_at"].replace("Z", "+00:00"))
        if parsed_at.tzinfo is None or created.tzinfo is None or parsed_at < created:
            raise ValueError("snapshot activation observation predates creation")
        code_successor = manifest.get("code_successor")
        if code_successor != published_code_successor:
            raise ValueError("snapshot published code successor differs")
        if code_successor is not None:
            if validate_portable(connection, code_successor, deployment=deployment, at=at) != code_successor:
                raise ValueError("snapshot code successor differs from its ledger proof")
        active = activation_at(connection, at)
        keys = ("activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256")
        if active is None:
            raise ValueError("snapshot has no current activation")
        if all(active[key] == deployment["bindings"][key] for key in keys):
            if published_successor is not None:
                raise ValueError("same activation must not claim a successor")
            return
        if active["profile_id"] != "integrated_route_v1" or published_successor is None:
            raise ValueError("snapshot current activation has no published legal successor")
        frozen = active["metadata"]["capture_operation_source"]
        # Deployment binds the pre-install ancestor. The immutable source
        # snapshot and Publisher proof bind the actual post-install runtime.
        runtime = frozen["runtime_bindings"]
        if runtime["config_sha256"] != deployment["bindings"]["config_sha256"]:
            raise ValueError("successor configuration differs from installed deployment")
        successor = validate_installed_activation_successor(connection, source_deployment=deployment,
            current_active=active, runtime_bindings=runtime,
            manifest=frozen["manifest"], at=at, portable=True)
        if successor != published_successor:
            raise ValueError("published activation successor differs from snapshot")
    except (KeyError, TypeError, ValueError, auth.AuthorizationError) as exc:
        raise SnapshotInstallError("schema20 snapshot activation proof is invalid") from exc


def _private_deployment_reference_index(manifest: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Validate Writer-classified private references without reading Mac files."""
    directory = manifest.get("private_deployment_references")
    if directory is None:
        return {}
    try:
        deployment = manifest["deployment_readiness"]
        if (not isinstance(directory, dict)
                or set(directory) != {"contract_version", "deployment_id", "deployment_receipt_sha256", "references"}
                or directory["contract_version"] != "private-deployment-references-v1"
                or directory["deployment_id"] != deployment["deployment_id"]
                or directory["deployment_receipt_sha256"] != deployment["receipt_sha256"]
                or not isinstance(directory["references"], list)):
            raise ValueError("private deployment directory binding differs")
        expected = {"deployment." + key: value for key, value in deployment["evidence"].items()
                    if key in {"source_archive", "migration", "rollback", "full_checks", "install", "bounded_e2e", "release_decision"}}
        decision = deployment.get("release_decision")
        if decision is not None:
            expected.update({"decision." + key: decision["runtime_evidence"][key] for key in ("build", "runtime")})
        code_successor = manifest.get("code_successor")
        if code_successor is not None:
            from v8.capture_code_successor import PRIVATE_ROLES

            references = code_successor["private_references"]
            if (not isinstance(references, list) or len(references) != len(PRIVATE_ROLES)
                    or {reference["role"] for reference in references} != set(PRIVATE_ROLES)):
                raise ValueError("private code successor roles differ")
            expected.update({"code_successor." + reference["role"]: reference for reference in references})
        allowed = set(expected) | ({"decision.build.full_checks"} if decision is not None else set())
        seen: set[str] = set()
        result: dict[str, dict[str, Any]] = {}
        for entry in directory["references"]:
            role = entry["role"]
            child = role == "decision.build.full_checks"
            if (role not in allowed or role in seen
                    or set(entry) != {"role", "path", "sha256", "byte_size", *(["parent_sha256"] if child else [])}
                    or not isinstance(entry["path"], str) or not Path(entry["path"]).is_absolute()
                    or Path(entry["path"]).is_relative_to(_writer_root(manifest))
                    or not SHA256_RE.fullmatch(str(entry["sha256"]))
                    or type(entry["byte_size"]) is not int or entry["byte_size"] < 0):
                raise ValueError("private deployment directory entry invalid")
            if child:
                if entry["parent_sha256"] != expected["decision.build"]["sha256"]:
                    raise ValueError("private postseal checks are not anchored to the verified build")
            elif (any(entry[key] != expected[role][key] for key in ("path", "sha256"))
                  or expected[role].get("byte_size") not in {None, entry["byte_size"]}):
                raise ValueError("private deployment directory differs from portable proof")
            identity = {key: entry[key] for key in ("path", "sha256", "byte_size")}
            if entry["path"] in result and result[entry["path"]] != identity:
                raise ValueError("private deployment directory identities conflict")
            result[entry["path"]] = identity
            seen.add(role)
        if seen != allowed:
            raise ValueError("private deployment directory roles missing")
        return result
    except (KeyError, TypeError, ValueError) as exc:
        raise SnapshotInstallError("private deployment reference directory is invalid") from exc


def _verify_managed_originals(
    bundle: Path,
    manifest: Mapping[str, Any],
    database: Path,
    config: InstallConfig,
    *,
    verify_artifacts: bool,
) -> None:
    contract, lifecycle = _snapshot_modules()
    from v8.artifact_paths import ArtifactPathError, runtime_evidence_aliases
    try:
        aliases = runtime_evidence_aliases(dict(manifest))
    except (ArtifactPathError, KeyError, TypeError) as exc:
        raise SnapshotInstallError("runtime evidence alias contract is invalid") from exc
    private_references = _private_deployment_reference_index(manifest)
    if set(private_references) & set(aliases):
        raise SnapshotInstallError("private deployment evidence must not be exported to business cache")

    def verify_external_pointers(value: Any) -> None:
        if isinstance(value, dict):
            path = next((value[key] for key in ("project_path", "local_path", "path")
                         if isinstance(value.get(key), str)), None)
            if (path is not None and "sha256" in value and Path(path).is_absolute()
                    and not Path(path).is_relative_to(_writer_root(manifest))):
                private = private_references.get(path)
                if private is not None:
                    size = value.get("byte_size")
                    if (value["sha256"] != private["sha256"]
                            or size is not None and (type(size) is not int or size != private["byte_size"])):
                        raise SnapshotInstallError("private deployment reference SHA-256 or size differs")
                    # Keep traversing nested values: this exact pointer does
                    # not exempt other business references in the same object.
                else:
                    alias = aliases.get(path)
                    if alias is None or any(alias[key] != value.get(key) for key in ("sha256", "byte_size")):
                        raise SnapshotInstallError("external runtime evidence pointer is not hash-bound to the manifest")
            for child in value.values():
                verify_external_pointers(child)
        elif isinstance(value, list):
            for child in value:
                verify_external_pointers(child)
    managed = manifest.get("managed_originals")
    if (not isinstance(managed, dict) or set(managed) != {"contract_version", "bundles"}
            or managed.get("contract_version") != contract.MANAGED_ORIGINALS_CONTRACT
            or not isinstance(managed.get("bundles"), list)):
        raise SnapshotInstallError("managed originals contract is missing or invalid")
    writer_root = _writer_root(manifest)
    included: dict[str, Mapping[str, Any]] = {}
    optional: dict[str, Mapping[str, Any]] = {}
    for rows, target in ((manifest["files"], included), (manifest["optional_reuse_files"], optional)):
        for item in rows:
            canonical = _project_path(item.get("project_path"), writer_root)
            expected = ("data/cache/" if item["root"] == "cache" else "reports/") + item["path"]
            if canonical != expected or canonical in included or canonical in optional:
                raise SnapshotInstallError("artifact root/path/project_path disagree")
            target[canonical] = item

    def required(project_path: str, sha256: Any, byte_size: Any) -> Mapping[str, Any]:
        row = included.get(project_path)
        if row is None or row["sha256"] != sha256 or row["byte_size"] != byte_size:
            raise SnapshotInstallError("managed proof must be a required, hash-bound artifact")
        return row

    original_paths: dict[str, Mapping[str, Any]] = {}
    seen_bundles: set[str] = set()
    connection = sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        controls = {row["id"]: dict(row) for row in connection.execute(
            "SELECT * FROM evidence_artifacts WHERE artifact_type='media_lifecycle_manifest'"
        )}
        for entry in managed["bundles"]:
            if not isinstance(entry, dict):
                raise SnapshotInstallError("managed bundle entry is invalid")
            bundle_id = entry.get("bundle_id")
            if (not isinstance(bundle_id, str) or re.fullmatch(r"[0-9a-f]{32}", bundle_id) is None
                    or bundle_id in seen_bundles):
                raise SnapshotInstallError("managed bundle identity is invalid or duplicated")
            seen_bundles.add(bundle_id)
            control = controls.get(entry.get("control_artifact_id"))
            if control is None or control["content_id"] != entry.get("content_id") or control["status"] != "available":
                raise SnapshotInstallError("managed control row does not match snapshot")
            control_path = _project_path(control["local_path"], writer_root)
            frozen = {"project_path": control_path, "sha256": control["sha256"], "byte_size": control["byte_size"]}
            if entry.get("manifest") != frozen:
                raise SnapshotInstallError("managed manifest reference disagrees with database")
            control_file = required(control_path, control["sha256"], control["byte_size"])
            state = json.loads(control["metadata_json"]).get("media_lifecycle", {})
            if state.get("bundle_id") != bundle_id or state.get("manifest_sha256") != control["sha256"]:
                raise SnapshotInstallError("managed control metadata is unbound")
            for key in ("storage_state", "operation_state", "archive_verified_at", "delete_due_at", "deleted_at"):
                if entry.get(key) != state.get(key):
                    raise SnapshotInstallError("managed storage disposition is not the database snapshot")
            if not verify_artifacts:
                raise SnapshotInstallError("managed bundles require their complete proof files for verification")
            body = _read_object(
                _verified_artifact_source(bundle, control_file, config)
            )
            try:
                lifecycle._validate_manifest(body)
                lifecycle._validate_state(state, body)
                original = lifecycle.original_artifact(connection, {
                    "manifest": body, "bundle_id": bundle_id,
                    "control_artifact_id": control["id"], "manifest_sha256": control["sha256"],
                })
            except (ValueError, RuntimeError, KeyError, TypeError) as exc:
                raise SnapshotInstallError("managed immutable identity or state is invalid") from exc
            if (body["content_id"] != control["content_id"] or body["registered_at"] != control["created_at"]
                    or state.get("source_artifact_id") != body["source"]["artifact_id"]
                    or state.get("source_sha256") != body["source"]["sha256"]
                    or entry.get("original_artifact_id") != original["id"]):
                raise SnapshotInstallError("managed original or source identity drifted")
            root = PurePosixPath(control_path).parent.parent
            if (root.name != bundle_id or root.parent.name != body["link_id"]
                    or root.parent.parent.name != "managed-v1"
                    or PurePosixPath(control_path).name != f"lifecycle-manifest-{control['sha256']}.json"):
                raise SnapshotInstallError("managed instance root is invalid")
            expected_members = []
            for member in body["members"]:
                name = _project_path(str(root / "originals" / member["relative_path"]), writer_root)
                root_name = "cache" if name.startswith("data/cache/") else "reports"
                relative = name.removeprefix("data/cache/" if root_name == "cache" else "reports/")
                expected_member = {key: member[key] for key in ("member_id", "index", "kind", "sha256", "byte_size")}
                expected_member.update(root=root_name, path=relative, project_path=name)
                if name in included or name in optional or name in original_paths:
                    raise SnapshotInstallError("managed originals must not be transferred or optionally reused")
                original_paths[name] = expected_member
                expected_members.append(expected_member)
            if entry.get("members") != expected_members:
                raise SnapshotInstallError("managed member order, identity or hash was changed")
            source = body["source"]
            required(_project_path(source["local_path"], writer_root), source["sha256"], source["byte_size"])
            original_path = _project_path(original["local_path"], writer_root)
            if original["artifact_type"] == "media_manifest":
                required(original_path, original["sha256"], original["byte_size"])
            elif original_path not in original_paths:
                raise SnapshotInstallError("managed video artifact is not a frozen original member")
            proofs = entry.get("proofs")
            if not isinstance(proofs, list):
                raise SnapshotInstallError("managed proofs list is invalid")
            seen_proofs: set[str] = set()
            for proof in proofs:
                if not isinstance(proof, dict) or not isinstance(proof.get("role"), str) or not proof["role"]:
                    raise SnapshotInstallError("managed proof entry is invalid")
                name = _project_path(proof.get("project_path"), writer_root)
                if name in seen_proofs:
                    raise SnapshotInstallError("managed proof is duplicated")
                seen_proofs.add(name)
                required(name, proof.get("sha256"), proof.get("byte_size"))
        if len(seen_bundles) != len(controls):
            raise SnapshotInstallError("snapshot omits a registered managed bundle")
        # A namespace label alone never exempts a missing file. Only members
        # derived from the exact registered control manifest may be omitted.
        for row in connection.execute("SELECT * FROM evidence_artifacts WHERE status='available'"):
            name = _project_path(row["local_path"], writer_root)
            if row["artifact_type"] == "comments" and name not in included:
                prefix = name + "/"
                children = sorted(
                    (child_name, item)
                    for child_name, item in included.items()
                    if child_name.startswith(prefix)
                )
                if (not children or any(
                        child_name.startswith(prefix)
                        for child_name in [*optional, *original_paths])):
                    raise SnapshotInstallError(
                        "comment directory has no exact required-file disposition"
                    )
                digest = hashlib.sha256()
                byte_size = 0
                for child_name, item in children:
                    relative = child_name.removeprefix(prefix)
                    byte_size += int(item["byte_size"])
                    digest.update(relative.encode("utf-8"))
                    digest.update(b"\0")
                    digest.update(str(item["sha256"]).encode("ascii"))
                    digest.update(b"\0")
                if digest.hexdigest() != row["sha256"] or byte_size != row["byte_size"]:
                    raise SnapshotInstallError(
                        "comment directory required-file identity changed"
                    )
                continue
            disposition = included.get(name) or optional.get(name) or original_paths.get(name)
            if disposition is None or disposition["sha256"] != row["sha256"] or disposition["byte_size"] != row["byte_size"]:
                raise SnapshotInstallError("available artifact has no exact required/optional/managed disposition")
        for row in connection.execute("SELECT details_json FROM scheduler_runs"):
            verify_external_pointers(json.loads(row[0]))
    except (sqlite3.Error, ValueError, KeyError, TypeError) as exc:
        raise SnapshotInstallError("snapshot managed-artifact database contract is invalid") from exc
    finally:
        connection.close()

    if verify_artifacts:
        def paths(value: Any) -> Iterator[str]:
            if isinstance(value, str):
                if value.startswith(("data/cache/", "reports/", writer_root + "/")):
                    yield _project_path(value, writer_root)
            elif isinstance(value, list):
                for child in value:
                    yield from paths(child)
            elif isinstance(value, dict):
                for child in value.values():
                    yield from paths(child)

        parents = {str(parent) for name in [*included, *optional, *original_paths]
                   for parent in PurePosixPath(name).parents}
        for item in included.values():
            if str(item["path"]).lower().endswith(".json"):
                body = _read_object(_verified_artifact_source(bundle, item, config))
                verify_external_pointers(body)
                for name in paths(body):
                    if name not in included and name not in optional and name not in original_paths and name not in parents:
                        raise SnapshotInstallError("required JSON refers to an unknown missing artifact")


def _default_service_action(service: str) -> ServiceAction:
    def action(verb: str) -> None:
        if verb not in {"start", "stop"}:
            raise SnapshotInstallError(f"unsupported service action: {verb}")
        completed = subprocess.run(
            ["systemctl", verb, service],
            check=False,
            capture_output=True,
            text=True,
            timeout=90,
        )
        if completed.returncode != 0:
            detail = completed.stderr.strip()[-1000:]
            raise SnapshotInstallError(f"systemctl {verb} {service} failed: {detail}")

    return action


def _read_json_url(url: str, timeout: float) -> dict[str, Any]:
    request = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": "DcarSnapshot/1"}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if int(response.status) != 200:
                raise SnapshotInstallError(
                    f"smoke endpoint returned {response.status}: {url}"
                )
            value = json.loads(response.read().decode("utf-8"))
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        urllib.error.URLError,
    ) as exc:
        raise SnapshotInstallError(f"smoke endpoint failed: {url}") from exc
    if not isinstance(value, dict):
        raise SnapshotInstallError(f"smoke endpoint returned non-object JSON: {url}")
    return value


def _default_smoke_check(
    config: InstallConfig,
    manifest: Optional[Mapping[str, Any]] = None,
    *,
    expected_schema: int = EXPECTED_DATABASE_SCHEMA_VERSION,
) -> SmokeCheck:
    if expected_schema not in SUPPORTED_SCHEMA_VERSIONS:
        raise SnapshotInstallError("unsupported smoke contract")
    expected_contract = None
    if expected_schema in {18, 19}:
        contract, _ = _snapshot_modules()
        expected_contract = manifest["snapshot_contract"] if manifest else contract.descriptor()
    expected_freshness = manifest.get("freshness", {}) if manifest else {}
    expected_runtime_identity = (
        _validate_runtime_identity(
            manifest.get("runtime_identity"),
            label="snapshot manifest",
            expected_schema=expected_schema,
        )
        if manifest
        else None
    )
    expected_main = (
        next(
            (
                item
                for item in manifest.get("databases", [])
                if item.get("name") == "dcar_insight.sqlite3"
            ),
            None,
        )
        if manifest
        else None
    )

    def check() -> None:
        deadline = time.monotonic() + config.start_wait_seconds
        last_error: Optional[Exception] = None
        while time.monotonic() < deadline:
            try:
                health = _read_json_url(
                    config.health_url, config.request_timeout_seconds
                )
                if health.get("status") != "ok":
                    raise SnapshotInstallError("health endpoint is not ok")
                if expected_schema in {18, 19} and (
                    health.get("report_version")
                    != _release_contract(expected_schema)[0]
                    or health.get("snapshot_contract") != expected_contract
                    or health.get("lifecycle_jobs_enabled") is not False
                ):
                    raise SnapshotInstallError("replica report/storage contract or lifecycle mode is not exact")
                database_state = health.get("database_state")
                if not isinstance(database_state, dict):
                    raise SnapshotInstallError("health omitted database identity")
                overview = _read_json_url(
                    config.overview_url, config.request_timeout_seconds
                )
                if (
                    overview.get("status") != "ready"
                    or not isinstance(overview.get("windows"), dict)
                    or not isinstance(overview.get("data_freshness"), dict)
                ):
                    raise SnapshotInstallError(
                        "overview did not read the active database"
                    )
                scheduler = _read_json_url(
                    config.scheduler_url, config.request_timeout_seconds
                )
                catchup = scheduler.get("startup_catchup")
                if (
                    health.get("read_only") is not True
                    or scheduler.get("read_only") is not True
                ):
                    raise SnapshotInstallError("replica API is not in read-only mode")
                if scheduler.get("requested") is not False or scheduler.get("enabled") is not False:
                    raise SnapshotInstallError(
                        "replica scheduler is unexpectedly enabled"
                    )
                if (not isinstance(catchup, dict) or catchup.get("requested") is not False
                        or catchup.get("enabled") is not False):
                    raise SnapshotInstallError(
                        "replica startup catch-up is unexpectedly enabled"
                    )
                if expected_main is not None:
                    if database_state.get("sha256") != expected_main.get("sha256"):
                        raise SnapshotInstallError(
                            "replica database SHA-256 is not the staged snapshot"
                        )
                    if database_state.get("user_version") != expected_main.get(
                        "user_version"
                    ):
                        raise SnapshotInstallError(
                            "replica schema is not the staged snapshot"
                        )
                    if database_state.get("content_count") != expected_freshness.get(
                        "content_count"
                    ):
                        raise SnapshotInstallError(
                            "replica content count is not the staged snapshot"
                        )
                    if database_state.get(
                        "latest_published_at"
                    ) != expected_freshness.get("latest_published_at"):
                        raise SnapshotInstallError(
                            "replica freshness is not the staged snapshot"
                        )
                    if (
                        database_state.get("runtime_identity")
                        != expected_runtime_identity
                    ):
                        raise SnapshotInstallError(
                            "replica runtime identity is not the staged snapshot"
                        )
                return
            except Exception as exc:  # retry during a bounded service startup window
                last_error = exc
                time.sleep(1.0)
        raise SnapshotInstallError(f"post-install smoke check failed: {last_error}")

    return check


def _checkpoint_database(path: Path) -> None:
    if not path.exists():
        return
    if path.is_symlink() or not path.is_file():
        raise SnapshotInstallError(f"active database is unsafe: {path}")
    connection = sqlite3.connect(path)
    try:
        row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    finally:
        connection.close()
    if row is not None and int(row[0]) != 0:
        raise SnapshotInstallError(f"WAL checkpoint remained busy for {path.name}")


def _copy_file_durable(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as reader, target.open("xb") as writer:
        shutil.copyfileobj(reader, writer, length=1024 * 1024)
        writer.flush()
        os.fsync(writer.fileno())
    shutil.copystat(source, target, follow_symlinks=False)


def _backup_active_databases(config: InstallConfig, destination: Path) -> list[str]:
    _ensure_private_directory(destination)
    names: list[str] = []
    for name in sorted(DATABASE_NAMES):
        active = config.database_root / name
        if not active.exists():
            continue
        _checkpoint_database(active)
        _copy_file_durable(active, destination / name)
        names.append(name)
        for suffix in ("-wal", "-shm"):
            sidecar = active.with_name(active.name + suffix)
            if sidecar.exists():
                if sidecar.is_symlink() or not sidecar.is_file():
                    raise SnapshotInstallError(f"database sidecar is unsafe: {sidecar}")
                os.replace(sidecar, destination / sidecar.name)
    _fsync_directory(destination)
    return names


def _database_payloads(bundle: Path, manifest: Mapping[str, Any]) -> dict[str, Path]:
    payloads: dict[str, Path] = {}
    for item in manifest["databases"]:
        payloads[str(item["name"])] = _bundle_member(bundle, str(item["bundle_path"]))
    return payloads


def _atomic_replace_database(config: InstallConfig, name: str, source: Path) -> None:
    if name not in DATABASE_NAMES:
        raise SnapshotInstallError(f"unsupported active database name: {name}")
    _ensure_managed_directory(config.database_root, config)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{name}.incoming-", dir=config.database_root
    )
    temporary = Path(temporary_name)
    try:
        with source.open("rb") as reader, os.fdopen(descriptor, "wb") as writer:
            shutil.copyfileobj(reader, writer, length=1024 * 1024)
            writer.flush()
            os.fsync(writer.fileno())
        _apply_file_access(temporary, config)
        os.replace(temporary, config.database_root / name)
        _fsync_directory(config.database_root)
    finally:
        if temporary.exists():
            temporary.unlink()


def _restore_databases(
    config: InstallConfig,
    backup_dir: Path,
    names: Sequence[str],
    *,
    remove_absent: Sequence[str] = (),
) -> None:
    for name in names:
        source = backup_dir / name
        if not source.is_file() or source.is_symlink():
            raise SnapshotInstallError(
                f"rollback database is missing or unsafe: {source}"
            )
        _atomic_replace_database(config, name, source)
    for name in remove_absent:
        if name not in names:
            target = config.database_root / name
            if target.exists():
                quarantine = backup_dir / f"removed-{name}"
                os.replace(target, quarantine)
    _fsync_directory(config.database_root)


def _artifact_backup_target(backup_dir: Path, item: Mapping[str, Any]) -> Path:
    root_name = item.get("root")
    relative_value = item.get("path")
    if root_name not in {"cache", "reports"} or not isinstance(relative_value, str):
        raise SnapshotInstallError("artifact change entry is invalid")
    relative = PurePosixPath(relative_value)
    if relative.is_absolute() or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        raise SnapshotInstallError(f"artifact change path is unsafe: {relative_value}")
    root = backup_dir / "artifacts" / str(root_name)
    candidate = (root / Path(*relative.parts)).resolve()
    if root.resolve() not in candidate.parents:
        raise SnapshotInstallError(
            f"artifact backup path escapes its root: {relative_value}"
        )
    return candidate


def _atomic_activate_artifact(
    source: Path, target: Path, *, root: Path, config: InstallConfig
) -> None:
    if source.is_symlink() or not source.is_file():
        raise SnapshotInstallError(f"staged artifact is missing or unsafe: {source}")
    if target.exists() and (target.is_symlink() or not target.is_file()):
        raise SnapshotInstallError(f"active artifact is unsafe: {target}")
    _ensure_managed_parent(root, target, config)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.incoming-", dir=target.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    temporary.unlink()
    try:
        # Consumers validate a single-link immutable file. Sharing an inode
        # with staging both violates that contract and permits later mutation.
        _copy_file_durable(source, temporary)
        _fsync_file(temporary)
        _apply_file_access(temporary, config)
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _artifact_change(item: Mapping[str, Any], *, had_previous: bool) -> dict[str, Any]:
    return {
        "root": str(item["root"]),
        "path": str(item["path"]),
        "had_previous": had_previous,
    }


def _install_artifacts(
    bundle: Path,
    manifest: Mapping[str, Any],
    config: InstallConfig,
    backup_dir: Path,
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for item in manifest["files"]:
        target = _artifact_target(config, item)
        expected_size = int(item["byte_size"])
        expected_sha = str(item["sha256"])
        if target.exists():
            if target.is_symlink() or not target.is_file():
                raise SnapshotInstallError(f"active artifact is unsafe: {target}")
            if (
                target.stat().st_size == expected_size
                and _sha256(target) == expected_sha
            ):
                root = (
                    config.cache_root
                    if item["root"] == "cache"
                    else config.reports_root
                )
                _ensure_managed_parent(root, target, config)
                _apply_file_access(target, config)
                continue
            backup = _artifact_backup_target(backup_dir, item)
            _copy_file_durable(target, backup)
            changes.append(_artifact_change(item, had_previous=True))
        else:
            changes.append(_artifact_change(item, had_previous=False))
    _write_json_atomic(backup_dir / "artifact-changes.json", {"changes": changes})
    try:
        for change in changes:
            source = _staged_artifact_target(bundle, change)
            target = _artifact_target(config, change)
            root = config.cache_root if change["root"] == "cache" else config.reports_root
            _atomic_activate_artifact(source, target, root=root, config=config)
    except Exception:
        _restore_artifacts(config, backup_dir, changes)
        raise
    return changes


def _optional_reuse_summary(
    manifest: Mapping[str, Any], config: InstallConfig
) -> dict[str, int]:
    reused_count = 0
    reused_bytes = 0
    omitted_count = 0
    omitted_bytes = 0
    for item in manifest["optional_reuse_files"]:
        target = _artifact_target(config, item)
        expected_size = int(item["byte_size"])
        expected_sha = str(item["sha256"])
        reusable = (
            target.is_file()
            and not target.is_symlink()
            and target.stat().st_size == expected_size
            and _sha256(target) == expected_sha
        )
        if reusable:
            reused_count += 1
            reused_bytes += expected_size
        else:
            omitted_count += 1
            omitted_bytes += expected_size
    return {
        "reused_count": reused_count,
        "reused_bytes": reused_bytes,
        "omitted_count": omitted_count,
        "omitted_bytes": omitted_bytes,
    }


def _restore_artifacts(
    config: InstallConfig,
    backup_dir: Path,
    changes: Sequence[Mapping[str, Any]],
) -> None:
    for change in changes:
        target = _artifact_target(config, change)
        if change.get("had_previous") is True:
            backup = _artifact_backup_target(backup_dir, change)
            if backup.is_symlink() or not backup.is_file():
                raise SnapshotInstallError(
                    f"rollback artifact is missing or unsafe: {backup}"
                )
            root = config.cache_root if change["root"] == "cache" else config.reports_root
            _atomic_activate_artifact(backup, target, root=root, config=config)
        elif change.get("had_previous") is False:
            if target.exists():
                if target.is_symlink() or not target.is_file():
                    raise SnapshotInstallError(
                        f"active rollback artifact is unsafe: {target}"
                    )
                target.unlink()
                _fsync_directory(target.parent)
        else:
            raise SnapshotInstallError("artifact change entry has invalid state")


def _read_artifact_changes(backup_dir: Path) -> list[dict[str, Any]]:
    path = backup_dir / "artifact-changes.json"
    if not path.exists():
        return []
    if path.is_symlink() or not path.is_file():
        raise SnapshotInstallError(f"artifact change ledger is unsafe: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotInstallError(
            f"artifact change ledger is invalid: {path}"
        ) from exc
    changes = value.get("changes") if isinstance(value, dict) else None
    if not isinstance(changes, list) or not all(
        isinstance(item, dict) for item in changes
    ):
        raise SnapshotInstallError(f"artifact change ledger is invalid: {path}")
    return changes


def _backup_artifact_targets(
    config: InstallConfig,
    destination: Path,
    identities: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for item in identities:
        target = _artifact_target(config, item)
        if target.exists():
            if target.is_symlink() or not target.is_file():
                raise SnapshotInstallError(f"active artifact is unsafe: {target}")
            _copy_file_durable(target, _artifact_backup_target(destination, item))
            changes.append(_artifact_change(item, had_previous=True))
        else:
            changes.append(_artifact_change(item, had_previous=False))
    _write_json_atomic(destination / "artifact-changes.json", {"changes": changes})
    return changes


def _apply_record_access(descriptor: int, record: Mapping[str, Any]) -> None:
    info = os.fstat(descriptor)
    owner = (int(record["uid"]), int(record["gid"]))
    if (info.st_uid, info.st_gid) != owner:
        os.fchown(descriptor, *owner)
    os.fchmod(descriptor, int(record["mode"]))
    after = os.fstat(descriptor)
    if (after.st_uid, after.st_gid) != owner or stat.S_IMODE(after.st_mode) != int(record["mode"]):
        raise SnapshotInstallError("paired file permissions could not be restored exactly")


def _write_bytes_atomic(path: Path, payload: bytes, *, record: Mapping[str, Any] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or path.parent != path.parent.resolve(strict=True):
        raise SnapshotInstallError("atomic file target must not be an alias")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            if record is not None:
                _apply_record_access(handle.fileno(), record)
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    payload = (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    _write_bytes_atomic(path, payload)


def _copy_replace(source: Path, target: Path, record: Mapping[str, Any]) -> None:
    if _sha256(source) != record["sha256"] or source.stat().st_size != record["byte_size"]:
        raise SnapshotInstallError("paired backup or sealed source bytes changed")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.is_symlink() or target.parent != target.parent.resolve(strict=True):
        raise SnapshotInstallError("paired replacement target is unsafe")
    descriptor, name = tempfile.mkstemp(prefix="." + target.name + ".", dir=target.parent)
    temporary = Path(name)
    try:
        with source.open("rb") as reader, os.fdopen(descriptor, "wb") as writer:
            shutil.copyfileobj(reader, writer, length=1024 * 1024)
            writer.flush()
            _apply_record_access(writer.fileno(), record)
            os.fsync(writer.fileno())
        if _sha256(temporary) != record["sha256"]:
            raise SnapshotInstallError("paired source changed during copy")
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _snapshot_receipt(bundle: Path, manifest: Mapping[str, Any], config: InstallConfig,
                      history_dir: Path, *, previous_databases: Sequence[str],
                      artifact_changes: Sequence[Mapping[str, Any]], status: str) -> dict[str, Any]:
    manifest_path = history_dir / "manifest.json"
    if manifest_path.exists():
        if manifest_path.is_symlink() or _sha256(manifest_path) != _sha256(bundle / "manifest.json"):
            raise SnapshotInstallError("durable snapshot manifest was changed")
    else:
        _copy_file_durable(bundle / "manifest.json", manifest_path)
    _apply_file_access(manifest_path, config)
    receipt = {
        "schema": "dcar-read-replica-install-receipt-v1", "snapshot_id": manifest["snapshot_id"],
        "installed_at": _utc_now(), "activation_status": status,
        "database_sha256": {item["name"]: item["sha256"] for item in manifest["databases"]},
        "runtime_identity": manifest["runtime_identity"], "snapshot_contract": manifest["snapshot_contract"],
        "writer_project_root": manifest["writer_project_root"],
        "manifest_path": str(manifest_path), "manifest_sha256": _sha256(manifest_path),
        "previous_databases": list(previous_databases), "artifact_changes": len(artifact_changes),
        "artifact_policy": manifest["artifact_policy"], "included_artifact_count": manifest["file_count"],
        "included_artifact_bytes": manifest["file_byte_size"], "optional_reuse": _optional_reuse_summary(manifest, config),
    }
    _write_json_atomic(history_dir / "install-receipt.json", receipt)
    _apply_file_access(history_dir / "install-receipt.json", config)
    _write_json_atomic(config.active_manifest_path, receipt)
    _apply_file_access(config.active_manifest_path, config)
    return receipt


def _backup_receipt(config: InstallConfig, destination: Path) -> bool:
    source = config.active_manifest_path
    if not source.exists() and not source.is_symlink():
        return False
    if source.is_symlink() or not source.is_file():
        raise SnapshotInstallError("active snapshot receipt is unsafe")
    _copy_file_durable(source, destination / "previous-active-snapshot.json")
    return True


def _restore_receipt(config: InstallConfig, source: Path, *, existed: bool) -> None:
    if existed:
        backup = source / "previous-active-snapshot.json"
        if not backup.is_file() or backup.is_symlink():
            raise SnapshotInstallError("previous snapshot receipt backup is missing")
        record = {
            **_file_record(backup),
            "mode": 0o640,
            "uid": config.owner_uid,
            "gid": config.owner_gid,
        }
        _copy_replace(backup, config.active_manifest_path, record)
    elif config.active_manifest_path.exists():
        # Preserve the failed receipt for diagnosis instead of deleting it.
        target = source / ("failed-active-snapshot-" + str(time.time_ns()) + ".json")
        os.replace(config.active_manifest_path, target)
        _fsync_directory(config.runtime_root)


def install_bundle(
    bundle: Path, config: InstallConfig, *, service_action: Optional[ServiceAction] = None,
    smoke_check: Optional[SmokeCheck] = None,
    expected_schema: int = EXPECTED_DATABASE_SCHEMA_VERSION,
) -> dict[str, Any]:
    with _install_lock(config):
        _assert_transition_settled(config)
        active = config.database_root / "dcar_insight.sqlite3"
        if not active.is_file():
            raise SnapshotInstallError(
                f"normal install requires an existing schema{expected_schema} database"
            )
        _database_runtime_identity(active, expected_schema=expected_schema)
        if expected_schema == 20:
            _verify_release_contract(_current_release(config), 20)
            _strict_schema(active, 20)
        return _install_bundle_locked(bundle, config, service_action=service_action, smoke_check=smoke_check,
                                      expected_schema=expected_schema)


def _install_bundle_locked(
    bundle: Path, config: InstallConfig, *, service_action: Optional[ServiceAction] = None,
    smoke_check: Optional[SmokeCheck] = None,
    expected_schema: int = EXPECTED_DATABASE_SCHEMA_VERSION,
) -> dict[str, Any]:
    """Caller holds snapshot-install.lock; never acquire a nested flock."""
    bundle = bundle.resolve()
    service_action = service_action or _default_service_action(config.service)
    manifest = verify_bundle(bundle, config, verify_artifacts=True, expected_schema=expected_schema)
    install_smoke_check = smoke_check or _default_smoke_check(config, manifest, expected_schema=expected_schema)
    rollback_smoke_check = smoke_check or _default_smoke_check(config, expected_schema=expected_schema)
    history_dir = config.history_root / str(manifest["snapshot_id"])
    if history_dir.exists():
        raise SnapshotInstallError(f"snapshot was already installed: {manifest['snapshot_id']}")
    _ensure_managed_directory(config.runtime_root, config)
    _ensure_managed_directory(config.history_root, config)
    payloads = _database_payloads(bundle, manifest)
    service_stopped = backup_completed = receipt_existed = False
    old_names: list[str] = []
    artifact_changes: list[dict[str, Any]] = []
    try:
        service_action("stop")
        service_stopped = True
        _ensure_managed_directory(history_dir, config)
        receipt_existed = _backup_receipt(config, history_dir)
        old_names = _backup_active_databases(config, history_dir)
        backup_completed = True
        artifact_changes = _install_artifacts(bundle, manifest, config, history_dir)
        for name, source in sorted(payloads.items()):
            _atomic_replace_database(config, name, source)
        _snapshot_receipt(bundle, manifest, config, history_dir, previous_databases=old_names,
                          artifact_changes=artifact_changes, status="pending_smoke")
        service_action("start")
        service_stopped = False
        install_smoke_check()
        return _snapshot_receipt(bundle, manifest, config, history_dir, previous_databases=old_names,
                                 artifact_changes=artifact_changes, status="succeeded")
    except Exception as install_error:
        try:
            if backup_completed:
                if not service_stopped:
                    service_action("stop")
                _restore_databases(config, history_dir, old_names, remove_absent=tuple(payloads))
                _restore_artifacts(config, history_dir, _read_artifact_changes(history_dir))
                _restore_receipt(config, history_dir, existed=receipt_existed)
                service_action("start")
                rollback_smoke_check()
            elif service_stopped:
                service_action("start")
                rollback_smoke_check()
        except Exception as rollback_error:
            raise SnapshotInstallError(
                f"install failed ({install_error}); paired rollback also failed ({rollback_error})"
            ) from install_error
        raise SnapshotInstallError(f"install failed and the previous database/artifact/receipt set was restored: {install_error}") from install_error


def _upgrade_services(verb: str, service: str) -> None:
    _default_service_action(service)(verb)


def _service_states() -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    for service in SCHEMA_SERVICES:
        process = subprocess.run(
            ["systemctl", "show", service, "--property=LoadState,ActiveState,SubState,MainPID,ReadOnlyPaths,BindReadOnlyPaths"],
            capture_output=True, text=True, timeout=15, check=False,
        )
        if process.returncode:
            raise SnapshotInstallError("cannot verify original service state")
        result[service] = dict(line.split("=", 1) for line in process.stdout.splitlines() if "=" in line)
    return result


def _require_running_services(states: Mapping[str, Any]) -> None:
    if set(states) != set(SCHEMA_SERVICES):
        raise SnapshotInstallError("four original service states are required")
    for state in states.values():
        if (not isinstance(state, dict) or state.get("LoadState") != "loaded"
                or state.get("ActiveState") != "active" or state.get("SubState") != "running"
                or not str(state.get("MainPID", "")).isdigit() or int(state["MainPID"]) <= 0):
            raise SnapshotInstallError("schema upgrade requires all four services already running")


def _verify_installed_pair(
    config: InstallConfig,
    manifest: Mapping[str, Any],
    release: Path,
    *,
    expected_schema: int,
) -> None:
    if _current_release(config) != release:
        raise SnapshotInstallError("installed code release differs from the transition")
    for item in manifest["databases"]:
        path = config.database_root / item["name"]
        if _sha256(path) != item["sha256"]:
            raise SnapshotInstallError("installed database differs from the transition")
    identity = _database_runtime_identity(
        config.database_root / "dcar_insight.sqlite3",
        expected_schema=expected_schema,
    )
    active = _read_object(config.active_manifest_path)
    expected_manifest = config.history_root / manifest["snapshot_id"] / "manifest.json"
    if (active.get("schema") != "dcar-read-replica-install-receipt-v1"
            or active.get("snapshot_id") != manifest["snapshot_id"]
            or active.get("runtime_identity") != identity or identity != manifest["runtime_identity"]
            or active.get("snapshot_contract") != manifest["snapshot_contract"]
            or active.get("artifact_policy") != manifest["artifact_policy"]
            or active.get("writer_project_root") != manifest["writer_project_root"]
            or active.get("manifest_path") != str(expected_manifest)
            or active.get("manifest_sha256") != _sha256(expected_manifest)
            or _read_object(expected_manifest) != dict(manifest)
            or active.get("database_sha256") != {item["name"]: item["sha256"] for item in manifest["databases"]}):
        raise SnapshotInstallError("installed active receipt is not the paired snapshot")


def _old_upgrade_smoke(config: InstallConfig, seal: Mapping[str, Any]) -> SmokeCheck:
    def check() -> None:
        from_schema = int(seal["from_schema"])
        _default_smoke_check(config, expected_schema=from_schema)()
        health = _read_json_url(config.health_url, config.request_timeout_seconds)
        state = health.get("database_state", {})
        old = next(row for row in seal["old_databases"] if row["name"] == "dcar_insight.sqlite3")
        if (state.get("runtime_identity") != seal["old_runtime_identity"]
                or state.get("sha256") != old["sha256"]
                or state.get("user_version") != from_schema):
            raise SnapshotInstallError("restored service is not serving the sealed old database")
        _require_running_services(_service_states())
    return check


def _read_http_bytes(url: str, timeout: float) -> bytes:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        if response.status != 200:
            raise SnapshotInstallError("retained evidence HTTP read failed")
        return response.read()


def _retained_http_smoke(config: InstallConfig, manifest: Mapping[str, Any], base: str) -> dict[str, Any]:
    files = {_project_path(row["project_path"], _writer_root(manifest)): row for row in manifest["files"]}

    def target(value: str) -> Path:
        key = _project_path(value, _writer_root(manifest))
        if key not in files:
            raise SnapshotInstallError("HTTP smoke reference is not in the required manifest")
        return _artifact_target(config, files[key])

    connection = sqlite3.connect(f"{(config.database_root / 'dcar_insight.sqlite3').as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    reports: list[dict[str, Any]] = []
    previews: list[dict[str, Any]] = []
    try:
        seen_versions: set[str] = set()
        for row in connection.execute(
            "SELECT task_id,revision,contract_version,report_json_path,report_sha256 "
            "FROM report_revisions ORDER BY created_at DESC,task_id,revision DESC"
        ):
            if row["contract_version"] in seen_versions:
                continue
            expected = target(row["report_json_path"])
            if _sha256(expected) != row["report_sha256"]:
                raise SnapshotInstallError("retained report SHA differs from its revision")
            url = base + "/api/v8/tasks/" + quote(row["task_id"], safe="") + "/revisions/" + str(row["revision"]) + "/report"
            payload = _read_json_url(url, config.request_timeout_seconds)
            if payload != _read_object(expected):
                raise SnapshotInstallError("HTTP report differs from its frozen revision")
            seen_versions.add(row["contract_version"])
            reports.append({"task_id": row["task_id"], "revision": row["revision"],
                            "contract_version": row["contract_version"], "sha256": row["report_sha256"]})
        row = connection.execute(
            "SELECT id,content_id,local_path FROM evidence_artifacts "
            "WHERE artifact_type='media_preview_manifest' AND status='available' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row is not None:
            body = _read_object(target(row["local_path"]))
            for member in body["members"]:
                url = base + f"/api/v8/contents/{row['content_id']}/evidence/previews/{row['id']}/{member['index']}"
                payload_bytes = _read_http_bytes(url, config.request_timeout_seconds)
                if (hashlib.sha256(payload_bytes).hexdigest() != member["sha256"]
                        or len(payload_bytes) != member["byte_size"]):
                    raise SnapshotInstallError("HTTP preview changed its frozen member bytes")
                previews.append({"artifact_id": row["id"], "index": member["index"], "sha256": member["sha256"]})
    finally:
        connection.close()
    html = _read_http_bytes("http://127.0.0.1:4174/dcar", config.request_timeout_seconds)
    if b"<html" not in html.lower() or b"<script" not in html.lower():
        raise SnapshotInstallError("public Web consumer did not render the application")
    originals: list[dict[str, Any]] = []
    for entry in manifest["managed_originals"]["bundles"][:1]:
        member = entry["members"][0]
        url = base + f"/api/v8/contents/{entry['content_id']}/evidence/files/{entry['original_artifact_id']}/{member['index']}"
        try:
            with urllib.request.urlopen(url, timeout=config.request_timeout_seconds):
                raise SnapshotInstallError("thin replica unexpectedly returned managed original bytes")
        except urllib.error.HTTPError as error:
            payload = json.loads(error.read().decode("utf-8"))
            if (error.code not in {409, 410} or payload.get("can_restore") is not False
                    or payload.get("code") not in {"replica_original_omitted", "original_expired",
                                                    "original_expiry_pending", "original_purge_in_progress"}):
                raise SnapshotInstallError("thin replica original disposition is not explicit and non-restorable") from error
            originals.append({"bundle_id": entry["bundle_id"], "member_id": member["member_id"],
                              "http_status": error.code, "code": payload["code"]})
    return {"reports": reports, "previews": previews, "originals": originals,
            "web_sha256": hashlib.sha256(html).hexdigest()}


def _new_upgrade_smoke(config: InstallConfig, manifest: Mapping[str, Any],
                       seal: Mapping[str, Any]) -> Callable[[], dict[str, Any]]:
    def check() -> dict[str, Any]:
        to_schema = int(seal["to_schema"])
        expected_report, _ = _release_contract(to_schema)
        _default_smoke_check(
            config, manifest, expected_schema=to_schema
        )()
        health = _read_json_url(config.health_url, config.request_timeout_seconds)
        consumer = health.get("media_consumers", {})
        if (health.get("report_version") != expected_report
                or health.get("snapshot_contract") != manifest["snapshot_contract"]
                or health.get("lifecycle_jobs_enabled") is not False
                or consumer.get("contract_version") != "media-consumer-runtime-v1"
                or consumer.get("code_sha256") != seal["code_sha256"]
                or consumer.get("project_root") != seal["new_release"]
                or consumer.get("current_code_matches_loaded") is not True
                or consumer.get("snapshot_contract") != manifest["snapshot_contract"]
                or type(consumer.get("pid")) is not int or consumer["pid"] <= 0):
            raise SnapshotInstallError("running media consumers do not implement the sealed contract")
        services = _service_states()
        _require_running_services(services)
        mounts = services["dcar-api.service"].get("ReadOnlyPaths", "").split()
        if not {str(config.database_root), str(config.cache_root), str(config.reports_root),
                str(config.runtime_root)}.issubset(set(mounts)):
            raise SnapshotInstallError("replica API data/runtime mounts are not read-only")
        base = config.health_url.removesuffix("/api/v8/health")
        lifecycle = _read_json_url(base + "/api/v8/media/lifecycle", config.request_timeout_seconds)
        if lifecycle.get("read_only") is not True:
            raise SnapshotInstallError("replica lifecycle endpoint is not read-only")
        bundles = manifest["managed_originals"]["bundles"]
        restore_content = bundles[0]["content_id"] if bundles else 1
        restore_bundle = bundles[0]["bundle_id"] if bundles else "0" * 32
        request = urllib.request.Request(
            base + f"/api/v8/contents/{restore_content}/media/restore", method="POST",
            data=json.dumps({"bundle_id": restore_bundle, "purpose": "evidence"}).encode(),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=config.request_timeout_seconds):
                raise SnapshotInstallError("replica accepted a restore request")
        except urllib.error.HTTPError as error:
            if error.code != 403:
                raise SnapshotInstallError("replica restore denial is not explicit readonly 403") from error
        checked = 0
        for item in manifest["files"]:
            path = _artifact_target(config, item)
            if not path.is_file() or path.stat().st_size != item["byte_size"] or _sha256(path) != item["sha256"]:
                raise SnapshotInstallError("installed report or retained evidence hash changed")
            checked += 1
        return {"health": health, "services": services, "restore_status": 403,
                "retained_file_count_verified": checked, "verified_at": _utc_now(),
                "http_consumers": _retained_http_smoke(config, manifest, base)}
    return check


def _reload_configuration() -> None:
    for command in (["systemctl", "daemon-reload"], ["nginx", "-t"], ["systemctl", "reload", "nginx"]):
        result = subprocess.run(command, capture_output=True, text=True, timeout=90, check=False)
        if result.returncode:
            raise SnapshotInstallError("configuration reload failed: " + " ".join(command))


def _atomic_current(config: InstallConfig, release: Path, allowed: set[Path]) -> None:
    if _current_release(config) not in allowed:
        raise SnapshotInstallError("current release drifted during transition")
    temporary = config.current_release.with_name(".current-upgrade-" + str(time.time_ns()))
    try:
        os.symlink(release, temporary)
        os.replace(temporary, config.current_release)
        _fsync_directory(config.current_release.parent)
    finally:
        if temporary.is_symlink():
            temporary.unlink()


def _quarantine_file(path: Path, directory: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if path.is_symlink() or not path.is_file():
        raise SnapshotInstallError("replacement residue is not a regular file")
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / (str(time.time_ns()) + "-" + path.name)
    os.replace(path, target)
    _fsync_directory(path.parent)
    _fsync_directory(directory)


def _verify_sealed_code(seal: Mapping[str, Any], config: InstallConfig) -> tuple[Path, Path]:
    old = _release_directory(Path(seal["old_release"]), config)
    new = _release_directory(Path(seal["new_release"]), config)
    if seal.get("roots") != _config_roots(config) or old == new:
        raise SnapshotInstallError("sealed deployment roots differ")
    _require_running_services(seal.get("old_services", {}))
    from_schema = int(seal["from_schema"])
    to_schema = int(seal["to_schema"])
    transition_contract, seal_contract = _transition_contract(
        from_schema, to_schema
    )
    if (
        seal.get("schema") != seal_contract
        or seal.get("transition_schema") != transition_contract
    ):
        raise SnapshotInstallError("sealed transition contract changed")
    _verify_release_contract(old, from_schema)
    _verify_release_contract(new, to_schema)
    if _code_inventory(old) != seal["old_code"] or _code_inventory(new) != seal["new_code"]:
        raise SnapshotInstallError("sealed release files changed")
    if _consumer_sha(new) != seal.get("code_sha256"):
        raise SnapshotInstallError("consumer fingerprint changed after sealing")
    targets = _config_targets(config)
    rows = seal.get("configurations", [])
    if not isinstance(rows, list) or len(rows) != len(targets):
        raise SnapshotInstallError("sealed configuration inventory is incomplete")
    if {row.get("key") for row in rows} != set(targets):
        raise SnapshotInstallError("sealed configuration targets are invalid")
    for row in rows:
        key = row["key"]
        source = None if key.startswith("config/") else "deploy/server/" + key
        if row["target"] != str(targets[key]) or row["source"] != source:
            raise SnapshotInstallError("sealed configuration target was changed")
        if source is not None and _file_record(new / source) != row["new"]:
            raise SnapshotInstallError("sealed configuration source changed")
    return old, new


def _verify_old_pair(seal: Mapping[str, Any], config: InstallConfig) -> None:
    if _current_release(config) != Path(seal["old_release"]):
        raise SnapshotInstallError("current is not the sealed old release")
    active = config.database_root / "dcar_insight.sqlite3"
    from_schema = int(seal["from_schema"])
    _validate_sqlite(active, expected_user_version=from_schema)
    _strict_schema(active, from_schema)
    if (
        _database_runtime_identity(active, expected_schema=from_schema)
        != seal["old_runtime_identity"]
    ):
        raise SnapshotInstallError("old runtime identity changed")
    if _database_inventory(config) != seal["old_databases"]:
        raise SnapshotInstallError("old database or sidecars changed after sealing")
    receipt = _file_record(config.active_manifest_path) if config.active_manifest_path.exists() else None
    if receipt != seal["old_active_receipt"]:
        raise SnapshotInstallError("old active receipt changed after sealing")
    for row in seal["configurations"]:
        target = Path(row["target"])
        current = _file_record(target) if target.exists() or target.is_symlink() else None
        if current != row["previous"]:
            raise SnapshotInstallError("old configuration changed after sealing")


def _verify_superseded_rollback(
    config: InstallConfig,
    prior: Mapping[str, Any],
    *,
    expected_old: Path,
    from_schema: int,
    to_schema: int,
) -> int:
    """Prove a completed rollback against its own seal and durable backup."""

    transition_contract, _ = _transition_contract(from_schema, to_schema)
    attempt = prior.get("attempt")
    completed_at = prior.get("completed_at")
    if (
        prior.get("schema") != transition_contract
        or prior.get("status") != "rolled_back"
        or prior.get("from_schema") != from_schema
        or prior.get("to_schema") != to_schema
        or type(attempt) is not int
        or attempt < 1
        or not isinstance(completed_at, str)
    ):
        raise SnapshotInstallError("superseded rollback is incomplete")
    try:
        completed = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SnapshotInstallError(
            "superseded schema transition completion time is invalid"
        ) from exc
    if completed.tzinfo is None:
        raise SnapshotInstallError(
            "superseded schema transition completion time is invalid"
        )

    try:
        prior_old = _release_directory(Path(str(prior["old_release"])), config)
        prior_new = _release_directory(Path(str(prior["new_release"])), config)
    except (KeyError, TypeError, ValueError, OSError) as exc:
        raise SnapshotInstallError("superseded rollback release is invalid") from exc
    if prior_old != expected_old or _current_release(config) != expected_old:
        raise SnapshotInstallError("superseded rollback did not restore the old release")
    try:
        prior_seal = _read_release_seal(prior_new)
        sealed_path = prior_new / "schema-upgrade-manifest.json"
        sealed_manifest_sha256 = _sha256(sealed_path)
    except OSError as exc:
        raise SnapshotInstallError("superseded rollback seal is missing") from exc
    identity_fields = (
        "from_schema",
        "to_schema",
        "old_release",
        "new_release",
        "snapshot_id",
        "snapshot_manifest_sha256",
        "runtime_identity",
        "snapshot_contract",
        "code_sha256",
    )
    if (
        any(prior.get(field) != prior_seal.get(field) for field in identity_fields)
        or prior.get("sealed_manifest_sha256") != sealed_manifest_sha256
    ):
        raise SnapshotInstallError("superseded rollback differs from its sealed transition")
    sealed_old, sealed_new = _verify_sealed_code(prior_seal, config)
    if sealed_old != prior_old or sealed_new != prior_new:
        raise SnapshotInstallError("superseded rollback release binding changed")

    snapshot_id = str(prior.get("snapshot_id", ""))
    if SNAPSHOT_ID_RE.fullmatch(snapshot_id) is None:
        raise SnapshotInstallError("superseded rollback snapshot identity is invalid")
    backup = Path(str(prior.get("backup_dir", "")))
    expected_backup = (
        config.history_root
        / snapshot_id
        / "schema-upgrade"
        / ("attempt-" + str(attempt))
    )
    try:
        canonical_backup = backup.resolve(strict=True)
    except OSError as exc:
        raise SnapshotInstallError("superseded rollback backup is missing") from exc
    if backup != expected_backup or canonical_backup != expected_backup:
        raise SnapshotInstallError("superseded rollback backup location changed")
    checkpoints = prior.get("checkpoints")
    if not isinstance(checkpoints, list) or not all(
        isinstance(item, dict) and isinstance(item.get("name"), str)
        for item in checkpoints
    ):
        raise SnapshotInstallError("superseded rollback checkpoints are invalid")
    checkpoint_names = {str(item["name"]) for item in checkpoints}

    try:
        if "backup_complete" in checkpoint_names:
            saved_seal = backup / "sealed-manifest.json"
            _file_record(saved_seal)
            if _read_object(saved_seal) != prior_seal:
                raise SnapshotInstallError("superseded rollback backup seal changed")
            for record in prior_seal["old_databases"]:
                source = backup / "databases" / record["name"]
                actual = _file_record(source)
                if any(
                    actual[key] != record[key] for key in ("sha256", "byte_size")
                ):
                    raise SnapshotInstallError(
                        "superseded rollback database backup changed"
                    )
            for row in prior_seal["configurations"]:
                if row["previous"] is None:
                    continue
                source = backup / "configurations" / row["key"]
                actual = _file_record(source)
                if any(
                    actual[key] != row["previous"][key]
                    for key in ("sha256", "byte_size")
                ):
                    raise SnapshotInstallError(
                        "superseded rollback configuration backup changed"
                    )
            previous_receipt = prior_seal["old_active_receipt"]
            receipt_backup = backup / "previous-active-snapshot.json"
            if previous_receipt is None:
                if receipt_backup.exists() or receipt_backup.is_symlink():
                    raise SnapshotInstallError(
                        "superseded rollback receipt backup is unexpected"
                    )
            else:
                actual = _file_record(receipt_backup)
                if any(
                    actual[key] != previous_receipt[key]
                    for key in ("sha256", "byte_size")
                ):
                    raise SnapshotInstallError(
                        "superseded rollback receipt backup changed"
                    )

        history_manifest = config.history_root / snapshot_id / "manifest.json"
        if "receipt_activated" in checkpoint_names:
            _file_record(history_manifest)
            if (
                _sha256(history_manifest) != prior_seal["snapshot_manifest_sha256"]
                or _read_object(history_manifest).get("snapshot_id") != snapshot_id
            ):
                raise SnapshotInstallError(
                    "superseded rollback snapshot manifest changed"
                )

        artifact_ledger = backup / "artifact-changes.json"
        if artifact_ledger.exists() or artifact_ledger.is_symlink():
            _file_record(artifact_ledger)
            changes = _read_artifact_changes(backup)
            seen_changes: set[tuple[str, str]] = set()
            for change in changes:
                key = (str(change.get("root")), str(change.get("path")))
                if key in seen_changes or type(change.get("had_previous")) is not bool:
                    raise SnapshotInstallError(
                        "superseded rollback artifact ledger is invalid"
                    )
                seen_changes.add(key)
                if change["had_previous"] is True:
                    _file_record(_artifact_backup_target(backup, change))
        elif "artifacts_applied" in checkpoint_names:
            raise SnapshotInstallError("superseded rollback artifact ledger is missing")
    except OSError as exc:
        raise SnapshotInstallError("superseded rollback evidence is missing") from exc
    return attempt


def _upgrade_checkpoint(config: InstallConfig, state: dict[str, Any], name: str,
                        hook: CheckpointHook | None) -> None:
    state["checkpoint"] = name
    state.setdefault("checkpoints", []).append({"name": name, "recorded_at": _utc_now()})
    _write_json_atomic(config.transition_path, state)
    if hook is not None:
        hook(name)


def _backup_upgrade(config: InstallConfig, seal: Mapping[str, Any], backup: Path) -> None:
    for record in seal["old_databases"]:
        _copy_file_durable(config.database_root / record["name"], backup / "databases" / record["name"])
    for row in seal["configurations"]:
        if row["previous"] is not None:
            _copy_file_durable(Path(row["target"]), backup / "configurations" / row["key"])
    _backup_receipt(config, backup)
    _write_json_atomic(backup / "sealed-manifest.json", seal)
    # Re-read the entire old pair after copying, before any mutation.
    _verify_old_pair(seal, config)
    _fsync_directory(backup)


def _restore_upgrade(config: InstallConfig, seal: Mapping[str, Any], state: dict[str, Any],
                     service_action: UpgradeServiceAction, reload_configuration: Callable[[], None],
                     rollback_smoke: SmokeCheck) -> None:
    state["status"] = "rolling_back"
    _write_json_atomic(config.transition_path, state)
    try:
        for service in SCHEMA_SERVICES:
            service_action("stop", service)
        old, new = _verify_sealed_code(seal, config)
        if _current_release(config) not in {old, new}:
            raise SnapshotInstallError("third-party current change prevents paired recovery")
        backup = Path(state["backup_dir"])
        expected = config.history_root / state["snapshot_id"] / "schema-upgrade" / ("attempt-" + str(state["attempt"]))
        if backup != expected or backup != backup.resolve(strict=True):
            raise SnapshotInstallError("transition backup location changed")
        if any(item["name"] == "backup_complete" for item in state["checkpoints"]):
            saved = _read_object(backup / "sealed-manifest.json")
            if saved != dict(seal):
                raise SnapshotInstallError("paired backup seal was changed")
            old_databases = {record["name"]: record for record in seal["old_databases"]}
            for name in sorted(DATABASE_NAMES):
                for suffix in ("", "-wal", "-shm"):
                    target = config.database_root / (name + suffix)
                    record = old_databases.get(target.name)
                    if record is None:
                        _quarantine_file(target, backup / "residue")
                    else:
                        _copy_replace(backup / "databases" / target.name, target, record)
            _restore_artifacts(config, backup, _read_artifact_changes(backup))
            for row in seal["configurations"]:
                target = Path(row["target"])
                if row["previous"] is None:
                    _quarantine_file(target, backup / "residue")
                else:
                    _copy_replace(backup / "configurations" / row["key"], target, row["previous"])
            if seal["old_active_receipt"] is None:
                _quarantine_file(config.active_manifest_path, backup / "residue")
            else:
                _copy_replace(backup / "previous-active-snapshot.json", config.active_manifest_path,
                              seal["old_active_receipt"])
            _atomic_current(config, old, {old, new})
            reload_configuration()
        _verify_old_pair(seal, config)
        for service in reversed(SCHEMA_SERVICES):
            service_action("start", service)
        rollback_smoke()
        state.update(status="rolled_back", completed_at=_utc_now())
        _write_json_atomic(config.transition_path, state)
    except Exception as error:
        state.update(status="rollback_failed", rollback_error=str(error), completed_at=None)
        _write_json_atomic(config.transition_path, state)
        raise SnapshotInstallError("paired recovery failed; normal installation remains blocked") from error


def schema_upgrade(
    bundle: Path, config: InstallConfig, *, release_dir: Path, expected_current_release: Path,
    from_schema: int, to_schema: int,
    service_action: UpgradeServiceAction | None = None,
    reload_configuration: Callable[[], None] | None = None,
    smoke_check: Callable[[], dict[str, Any]] | None = None,
    rollback_smoke_check: SmokeCheck | None = None,
    checkpoint_hook: CheckpointHook | None = None,
) -> dict[str, Any]:
    """The only cross-schema entry point. All pairing and recovery share one lock."""
    transition_contract, _ = _transition_contract(from_schema, to_schema)
    predecessor_pair, predecessor_contract = (
        ((16, 17), PREDECESSOR_SCHEMA_TRANSITION_CONTRACT)
        if (from_schema, to_schema) == (17, 18)
        else ((18, 19), SCHEMA_TRANSITION_CONTRACT) if (from_schema, to_schema) == (19, 20)
        else ((17, 18), LEGACY_SCHEMA_TRANSITION_CONTRACT)
    )
    service_action = service_action or _upgrade_services
    reload_configuration = reload_configuration or _reload_configuration
    with _install_lock(config):
        new = _release_directory(release_dir, config)
        seal = _read_release_seal(new)
        if (seal.get("from_schema"), seal.get("to_schema")) != (
            from_schema,
            to_schema,
        ):
            raise SnapshotInstallError(
                "requested schema pair differs from the frozen contract"
            )
        old, new = _verify_sealed_code(seal, config)
        if old != expected_current_release or new != release_dir:
            raise SnapshotInstallError("requested releases differ from the frozen contract")
        manifest = verify_bundle(bundle, config, expected_schema=to_schema)
        digest = _sha256(new / "schema-upgrade-manifest.json")
        if (seal["snapshot_id"] != manifest["snapshot_id"]
                or seal["snapshot_manifest_sha256"] != _sha256(bundle / "manifest.json")
                or seal["runtime_identity"] != manifest["runtime_identity"]
                or seal["snapshot_contract"] != manifest["snapshot_contract"]):
            raise SnapshotInstallError("staged snapshot differs from the frozen contract")
        rollback_smoke = rollback_smoke_check or _old_upgrade_smoke(config, seal)
        check = smoke_check or _new_upgrade_smoke(config, manifest, seal)
        prior = _read_object(config.transition_path) if config.transition_path.exists() else None
        predecessor_transition = None
        superseded_transition = None
        superseded_attempt = 0
        if prior is not None and (
            prior.get("schema") == predecessor_contract
            and prior.get("status") == "succeeded"
            and prior.get("from_schema") == predecessor_pair[0]
            and prior.get("to_schema") == predecessor_pair[1]
            and isinstance(prior.get("completed_at"), str)
        ):
            try:
                datetime.fromisoformat(prior["completed_at"].replace("Z", "+00:00"))
            except ValueError as exc:
                raise SnapshotInstallError(
                    "predecessor schema transition completion time is invalid"
                ) from exc
            predecessor_transition = prior
            prior = None
        if prior is not None:
            same_transition = (
                prior.get("schema") == transition_contract
                and prior.get("from_schema") == from_schema
                and prior.get("to_schema") == to_schema
                and prior.get("sealed_manifest_sha256") == digest
                and prior.get("old_release") == str(old)
                and prior.get("new_release") == str(new)
                and prior.get("snapshot_manifest_sha256")
                == seal["snapshot_manifest_sha256"]
            )
            supersedable_rollback = (
                prior.get("schema") == transition_contract
                and prior.get("status") == "rolled_back"
                and prior.get("from_schema") == from_schema
                and prior.get("to_schema") == to_schema
                and prior.get("old_release") == str(old)
                and isinstance(prior.get("completed_at"), str)
                and type(prior.get("attempt")) is int
                and prior["attempt"] >= 1
            )
            if not same_transition and not supersedable_rollback:
                raise SnapshotInstallError(
                    "unfinished or previous transition belongs to another contract"
                )
            if supersedable_rollback and not same_transition:
                superseded_attempt = _verify_superseded_rollback(
                    config,
                    prior,
                    expected_old=old,
                    from_schema=from_schema,
                    to_schema=to_schema,
                )
                superseded_transition = prior
                prior = None
        if prior is not None:
            if prior.get("status") == "succeeded":
                _verify_installed_pair(
                    config,
                    manifest,
                    new,
                    expected_schema=to_schema,
                )
                return prior
            if prior.get("status") not in {"rolled_back", "in_progress", "rolling_back", "rollback_failed"}:
                raise SnapshotInstallError("transition state is invalid")
            if prior["status"] != "rolled_back":
                _restore_upgrade(config, seal, prior, service_action, reload_configuration, rollback_smoke)
            predecessor_transition = prior.get("predecessor_transition")
        _verify_old_pair(seal, config)
        original_services = _service_states()
        _require_running_services(original_services)
        main = next(item for item in manifest["databases"] if item["name"] == "dcar_insight.sqlite3")
        _strict_schema(_bundle_member(bundle, main["bundle_path"]), to_schema)
        attempt = (
            superseded_attempt + 1
            if superseded_transition is not None
            else (1 if prior is None else int(prior["attempt"]) + 1)
        )
        history = config.history_root / manifest["snapshot_id"]
        backup = history / "schema-upgrade" / ("attempt-" + str(attempt))
        for directory in (config.runtime_root, config.history_root, history, backup):
            _ensure_managed_directory(directory, config)
        state: dict[str, Any] = {
            "schema": transition_contract,
            "status": "in_progress",
            "from_schema": from_schema,
            "to_schema": to_schema,
            "old_release": str(old), "new_release": str(new), "snapshot_id": manifest["snapshot_id"],
            "snapshot_manifest_sha256": seal["snapshot_manifest_sha256"],
            "runtime_identity": manifest["runtime_identity"], "snapshot_contract": manifest["snapshot_contract"],
            "sealed_manifest_sha256": digest, "code_sha256": seal["code_sha256"],
            "attempt": attempt, "backup_dir": str(backup), "started_at": _utc_now(),
            "completed_at": None, "checkpoints": [],
            "original_services": original_services,
        }
        if predecessor_transition is not None:
            state["predecessor_transition"] = predecessor_transition
        if superseded_transition is not None:
            state["superseded_transition"] = superseded_transition
        try:
            _upgrade_checkpoint(config, state, "prepared", checkpoint_hook)
            for service in SCHEMA_SERVICES:
                service_action("stop", service)
                _upgrade_checkpoint(config, state, "stopped:" + service, checkpoint_hook)
            _backup_upgrade(config, seal, backup)
            _upgrade_checkpoint(config, state, "backup_complete", checkpoint_hook)
            for row in seal["configurations"]:
                if row["source"] is not None:
                    _copy_replace(new / row["source"], Path(row["target"]), row["new"])
            _upgrade_checkpoint(config, state, "configurations_applied", checkpoint_hook)
            changes = _install_artifacts(bundle, manifest, config, backup)
            _upgrade_checkpoint(config, state, "artifacts_applied", checkpoint_hook)
            for name in DATABASE_NAMES:
                for suffix in ("-wal", "-shm"):
                    _quarantine_file(config.database_root / (name + suffix), backup / "residue")
            for name, source in sorted(_database_payloads(bundle, manifest).items()):
                _atomic_replace_database(config, name, source)
            _upgrade_checkpoint(config, state, "databases_applied", checkpoint_hook)
            old_names = [row["name"] for row in seal["old_databases"] if row["name"] in DATABASE_NAMES]
            _snapshot_receipt(bundle, manifest, config, history, previous_databases=old_names,
                              artifact_changes=changes, status="pending_smoke")
            _upgrade_checkpoint(config, state, "receipt_activated", checkpoint_hook)
            _atomic_current(config, new, {old})
            reload_configuration()
            _upgrade_checkpoint(config, state, "code_activated", checkpoint_hook)
            for service in reversed(SCHEMA_SERVICES):
                service_action("start", service)
                _upgrade_checkpoint(config, state, "started:" + service, checkpoint_hook)
            state["smoke_details"] = check()
            _upgrade_checkpoint(config, state, "smoke_verified", checkpoint_hook)
            _verify_installed_pair(
                config,
                manifest,
                new,
                expected_schema=to_schema,
            )
            _snapshot_receipt(bundle, manifest, config, history, previous_databases=old_names,
                              artifact_changes=changes, status="succeeded")
            state.update(status="succeeded", completed_at=_utc_now())
            _upgrade_checkpoint(config, state, "succeeded", checkpoint_hook)
            return state
        except Exception as error:
            state["error"] = str(error)
            _restore_upgrade(config, seal, state, service_action, reload_configuration, rollback_smoke)
            raise SnapshotInstallError("upgrade failed; the sealed old code/data/config/artifact/receipt set was restored") from error


def _rollback_candidates(
    config: InstallConfig, *, require_install_receipt: bool
) -> list[Path]:
    if not config.history_root.is_dir():
        return []
    return sorted(
        (
            path
            for path in config.history_root.iterdir()
            if path.is_dir()
            and not path.is_symlink()
            and SNAPSHOT_ID_RE.fullmatch(path.name)
            and (
                not require_install_receipt or (path / "install-receipt.json").is_file()
            )
        ),
        key=lambda path: path.name,
        reverse=True,
    )


def _active_snapshot_id(config: InstallConfig) -> str:
    path = config.active_manifest_path
    if path.is_symlink() or not path.is_file():
        raise SnapshotInstallError(
            "active snapshot manifest must be a regular non-symlink file"
        )
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotInstallError("active snapshot manifest is invalid") from exc
    snapshot_id = None
    if isinstance(value, dict):
        snapshot_id = value.get("snapshot_id")
        if snapshot_id is None and value.get("schema") == (
            "dcar-read-replica-rollback-receipt-v1"
        ):
            snapshot_id = value.get("restored_from_snapshot")
    if not isinstance(snapshot_id, str) or SNAPSHOT_ID_RE.fullmatch(snapshot_id) is None:
        raise SnapshotInstallError("active snapshot manifest has no valid snapshot_id")
    return snapshot_id


def _prunable_snapshot_directories(root: Path) -> list[Path]:
    if not root.exists():
        return []
    if root.is_symlink() or not root.is_dir():
        raise SnapshotInstallError(f"snapshot retention root is unsafe: {root}")
    return sorted(
        (
            path
            for path in root.iterdir()
            if SNAPSHOT_ID_RE.fullmatch(path.name)
            and not path.is_symlink()
            and path.is_dir()
        ),
        key=lambda path: path.name,
        reverse=True,
    )


def _directory_file_bytes(path: Path) -> int:
    total = 0
    for current, directories, files in os.walk(path, topdown=True, followlinks=False):
        current_path = Path(current)
        directories[:] = [
            name for name in directories if not (current_path / name).is_symlink()
        ]
        for name in files:
            candidate = current_path / name
            metadata = candidate.lstat()
            if stat.S_ISREG(metadata.st_mode):
                total += metadata.st_size
    return total


def _prune_snapshot_root(
    root: Path,
    *,
    active_snapshot_id: str,
    retain_count: int,
    require_active: bool = False,
) -> dict[str, Any]:
    candidates = _prunable_snapshot_directories(root)
    kept: list[Path] = []
    active = next(
        (path for path in candidates if path.name == active_snapshot_id), None
    )
    if require_active and active is None:
        raise SnapshotInstallError(
            f"active snapshot retention directory is missing: {root / active_snapshot_id}"
        )
    if active is not None:
        kept.append(active)
    for candidate in candidates:
        if candidate == active or len(kept) >= retain_count:
            continue
        kept.append(candidate)
    kept_names = {path.name for path in kept}
    deleted = [path for path in candidates if path.name not in kept_names]
    reclaimed_bytes = sum(_directory_file_bytes(path) for path in deleted)
    for path in deleted:
        if path.is_symlink() or not path.is_dir():
            raise SnapshotInstallError(
                f"snapshot directory changed while pruning: {path}"
            )
        try:
            shutil.rmtree(path)
        except OSError as exc:
            raise SnapshotInstallError(
                f"cannot prune snapshot directory: {path}"
            ) from exc
    if deleted:
        _fsync_directory(root)
    return {
        "root": str(root),
        "kept": sorted(kept_names, reverse=True),
        "deleted": [path.name for path in deleted],
        "reclaimed_bytes": reclaimed_bytes,
    }


def prune_snapshots(
    config: InstallConfig,
    *,
    incoming_root: Path = Path("/var/lib/dcar-aigc/incoming"),
    retain_count: int = 3,
) -> dict[str, Any]:
    if retain_count < 1:
        raise SnapshotInstallError("snapshot retain count must be at least 1")
    incoming_root = incoming_root.expanduser()
    if not incoming_root.is_absolute():
        raise SnapshotInstallError("incoming snapshot root must be absolute")
    expected_incoming_root = config.runtime_root.parent / "incoming"
    if (
        incoming_root.is_symlink()
        or incoming_root.resolve() != expected_incoming_root.resolve()
    ):
        raise SnapshotInstallError(
            "incoming snapshot root must be the managed state-root sibling"
        )
    with _install_lock(config):
        _assert_transition_settled(config)
        active_snapshot_id = _active_snapshot_id(config)
        history_candidates = _prunable_snapshot_directories(config.history_root)
        if not any(
            path.name == active_snapshot_id for path in history_candidates
        ):
            raise SnapshotInstallError(
                "active snapshot retention directory is missing: "
                f"{config.history_root / active_snapshot_id}"
            )
        incoming = _prune_snapshot_root(
            incoming_root,
            active_snapshot_id=active_snapshot_id,
            retain_count=retain_count,
        )
        history = _prune_snapshot_root(
            config.history_root,
            active_snapshot_id=active_snapshot_id,
            retain_count=retain_count,
            require_active=True,
        )
        return {
            "schema": "dcar-read-replica-prune-receipt-v1",
            "active_snapshot_id": active_snapshot_id,
            "retain_count": retain_count,
            "incoming": incoming,
            "snapshot_history": history,
            "reclaimed_bytes": int(incoming["reclaimed_bytes"])
            + int(history["reclaimed_bytes"]),
        }


def rollback_snapshot(
    config: InstallConfig,
    *,
    snapshot_id: Optional[str] = None,
    service_action: Optional[ServiceAction] = None,
    smoke_check: Optional[SmokeCheck] = None,
    expected_schema: int = EXPECTED_DATABASE_SCHEMA_VERSION,
) -> dict[str, Any]:
    service_action = service_action or _default_service_action(config.service)
    smoke_check = smoke_check or _default_smoke_check(config, expected_schema=expected_schema)
    with _install_lock(config):
        _assert_transition_settled(config)
        if snapshot_id is not None and not SNAPSHOT_ID_RE.fullmatch(snapshot_id):
            raise SnapshotInstallError("rollback snapshot_id is invalid")
        candidates = _rollback_candidates(
            config,
            require_install_receipt=snapshot_id is None,
        )
        source = next(
            (
                path
                for path in candidates
                if snapshot_id is None or path.name == snapshot_id
            ),
            None,
        )
        if source is None:
            raise SnapshotInstallError("no matching rollback snapshot exists")
        names = [name for name in sorted(DATABASE_NAMES) if (source / name).is_file()]
        if not names:
            raise SnapshotInstallError(
                f"rollback snapshot contains no databases: {source}"
            )
        # Only the sealed transition owns paired cross-version recovery.
        _database_runtime_identity(config.database_root / "dcar_insight.sqlite3", expected_schema=expected_schema)
        _database_runtime_identity(source / "dcar_insight.sqlite3", expected_schema=expected_schema)
        if expected_schema == 20:
            _verify_release_contract(_current_release(config), 20)
            _strict_schema(source / "dcar_insight.sqlite3", 20)
        if snapshot_id is None:
            source_receipt = _read_object(source / "install-receipt.json")
            if source_receipt.get("artifact_policy") != ARTIFACT_POLICY:
                raise SnapshotInstallError("legacy snapshot history requires an explicit rollback snapshot ID")
        source_artifact_changes = _read_artifact_changes(source)
        rollback_id = "rollback-" + datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%SZ"
        )
        safety_backup = config.history_root / rollback_id
        _ensure_private_directory(safety_backup)
        service_stopped = False
        backup_completed = False
        current_names: list[str] = []
        current_artifact_changes: list[dict[str, Any]] = []
        active_receipt_existed = False
        try:
            service_action("stop")
            service_stopped = True
            active_receipt_existed = _backup_receipt(config, safety_backup)
            current_names = _backup_active_databases(config, safety_backup)
            backup_completed = True
            current_artifact_changes = _backup_artifact_targets(
                config,
                safety_backup,
                source_artifact_changes,
            )
            _restore_databases(
                config,
                source,
                names,
                remove_absent=tuple(sorted(DATABASE_NAMES)),
            )
            _restore_artifacts(config, source, source_artifact_changes)
            _restore_receipt(config, source, existed=(source / "previous-active-snapshot.json").is_file())
            service_action("start")
            service_stopped = False
            smoke_check()
        except Exception as rollback_error:
            recovery_error: Optional[Exception] = None
            try:
                if backup_completed:
                    if not service_stopped:
                        service_action("stop")
                    _restore_databases(
                        config,
                        safety_backup,
                        current_names,
                        remove_absent=tuple(sorted(DATABASE_NAMES)),
                    )
                    _restore_artifacts(
                        config,
                        safety_backup,
                        current_artifact_changes,
                    )
                    _restore_receipt(config, safety_backup, existed=active_receipt_existed)
                    service_action("start")
                    smoke_check()
                elif service_stopped:
                    service_action("start")
                    smoke_check()
            except Exception as exc:
                recovery_error = exc
            if recovery_error is not None:
                raise SnapshotInstallError(
                    f"rollback failed ({rollback_error}); current-state recovery also failed "
                    f"({recovery_error})"
                ) from rollback_error
            raise SnapshotInstallError(
                f"rollback failed and the current databases were restored: {rollback_error}"
            ) from rollback_error
        receipt = {
            "schema": "dcar-read-replica-rollback-receipt-v1",
            "rolled_back_at": _utc_now(),
            "restored_from_snapshot": source.name,
            "safety_backup": safety_backup.name,
            "database_names": names,
        }
        _write_json_atomic(safety_backup / "rollback-receipt.json", receipt)
        return receipt


def _common_arguments(parser: argparse.ArgumentParser) -> None:
    for name, default in (
        ("current-release", "/var/www/dcar-aigc/current"),
        ("releases-root", "/var/www/dcar-aigc/releases"),
        ("systemd-root", "/etc/systemd/system"), ("nginx-root", "/etc/nginx"),
        ("config-root", "/etc/dcar-aigc"),
    ):
        parser.add_argument("--" + name, type=Path, default=Path(default))
    parser.add_argument(
        "--database-root", type=Path, default=Path("/var/lib/dcar-aigc/db")
    )
    parser.add_argument(
        "--cache-root", type=Path, default=Path("/var/lib/dcar-aigc/cache")
    )
    parser.add_argument(
        "--reports-root", type=Path, default=Path("/var/lib/dcar-aigc/reports")
    )
    parser.add_argument(
        "--runtime-root", type=Path, default=Path("/var/lib/dcar-aigc/runtime")
    )
    parser.add_argument("--service", default="dcar-api.service")
    parser.add_argument("--health-url", default="http://127.0.0.1:8765/api/v8/health")
    parser.add_argument(
        "--overview-url", default="http://127.0.0.1:8765/api/v8/overview"
    )
    parser.add_argument(
        "--scheduler-url", default="http://127.0.0.1:8765/api/v8/scheduler"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    verify = commands.add_parser("verify", help="Verify a staged bundle and artifacts.")
    verify.add_argument("--bundle", type=Path, required=True)
    _common_arguments(verify)
    install = commands.add_parser("install", help="Install a staged bundle atomically.")
    install.add_argument("--bundle", type=Path, required=True)
    _common_arguments(install)
    for name in ("seal-schema-upgrade", "schema-upgrade"):
        upgrade = commands.add_parser(
            name,
            help="Seal or execute an explicitly supported paired schema transition.",
        )
        upgrade.add_argument("--bundle", type=Path, required=True)
        upgrade.add_argument("--release-dir", type=Path, required=True)
        upgrade.add_argument("--expected-current-release", type=Path, required=True)
        upgrade.add_argument("--from-schema", type=int, required=True)
        upgrade.add_argument("--to-schema", type=int, required=True)
        _common_arguments(upgrade)
    rollback = commands.add_parser(
        "rollback", help="Restore a prior active database set."
    )
    rollback.add_argument("--snapshot-id")
    _common_arguments(rollback)
    for explicit in (verify, install, rollback):
        explicit.add_argument("--expected-schema", type=int, choices=(19, 20), default=19)
    prune = commands.add_parser(
        "prune", help="Prune inactive snapshot staging and rollback history."
    )
    prune.add_argument(
        "--incoming-root",
        type=Path,
        default=Path("/var/lib/dcar-aigc/incoming"),
    )
    prune.add_argument("--retain-count", type=int, default=3)
    _common_arguments(prune)
    return parser


def _config(arguments: argparse.Namespace) -> InstallConfig:
    try:
        owner_uid = pwd.getpwnam("root").pw_uid
        owner_gid = grp.getgrnam("dcar-aigc").gr_gid
    except KeyError as exc:
        raise SnapshotInstallError(
            "required replica account/group is missing: root:dcar-aigc"
        ) from exc
    return InstallConfig(
        database_root=arguments.database_root.resolve(),
        cache_root=arguments.cache_root.resolve(),
        reports_root=arguments.reports_root.resolve(),
        runtime_root=arguments.runtime_root.resolve(),
        service=arguments.service,
        health_url=arguments.health_url,
        overview_url=arguments.overview_url,
        scheduler_url=arguments.scheduler_url,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
        current_release=arguments.current_release,
        releases_root=arguments.releases_root,
        systemd_root=arguments.systemd_root,
        nginx_root=arguments.nginx_root,
        config_root=arguments.config_root,
    )


def main() -> int:
    arguments = _parser().parse_args()
    try:
        config = _config(arguments)
        if arguments.command == "verify":
            manifest = verify_bundle(arguments.bundle, config, verify_artifacts=True, expected_schema=arguments.expected_schema)
            result: Mapping[str, Any] = {
                "status": "verified",
                "snapshot_id": manifest["snapshot_id"],
                "database_names": [item["name"] for item in manifest["databases"]],
                "file_count": manifest["file_count"],
                "snapshot_contract": manifest["snapshot_contract"],
                "manifest_sha256": _sha256(arguments.bundle / "manifest.json"),
            }
        elif arguments.command == "install":
            result = install_bundle(arguments.bundle, config, expected_schema=arguments.expected_schema)
        elif arguments.command == "seal-schema-upgrade":
            result = seal_schema_upgrade(arguments.bundle, config, release_dir=arguments.release_dir,
                                         expected_current_release=arguments.expected_current_release,
                                         from_schema=arguments.from_schema,
                                         to_schema=arguments.to_schema)
        elif arguments.command == "schema-upgrade":
            release = arguments.release_dir
            if (Path(__file__).resolve() != release / "deploy/server/install_snapshot.py"
                    or Path(sys.prefix).resolve() != release / ".venv"):
                raise SnapshotInstallError("schema-upgrade must run from the sealed new release and its own virtualenv")
            result = schema_upgrade(arguments.bundle, config, release_dir=release,
                                    expected_current_release=arguments.expected_current_release,
                                    from_schema=arguments.from_schema, to_schema=arguments.to_schema)
        elif arguments.command == "rollback":
            result = rollback_snapshot(config, snapshot_id=arguments.snapshot_id, expected_schema=arguments.expected_schema)
        else:
            result = prune_snapshots(
                config,
                incoming_root=arguments.incoming_root,
                retain_count=arguments.retain_count,
            )
    except SnapshotInstallError as exc:
        raise SystemExit(f"snapshot operation refused: {exc}") from exc
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
