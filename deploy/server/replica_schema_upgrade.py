"""Explicit, sealed data upgrade for independently deployed read-only consumers.

The complete Writer migration chain is verified on the incoming snapshot. The
replica then replaces its databases once, with rollback, keeping code, service
configuration, Auth data and the shared release pointer unchanged.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import stat

CONTRACT = "dcar-replica-schema-chain-transition-v1"
SEAL_CONTRACT = "dcar-replica-schema-chain-seal-v1"
PAIRS = ((21, 22), (22, 23), (23, 24))


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(",", ":")).encode()).hexdigest()


def chain_shape(value):
    return (isinstance(value, list) and len(value) == 3
        and all(isinstance(row, dict) and set(row) == {"from_schema", "to_schema", "receipt_sha256"}
            and type(row["from_schema"]) is int and type(row["to_schema"]) is int
            and (row["from_schema"], row["to_schema"]) == pair
            and isinstance(row["receipt_sha256"], str) and len(row["receipt_sha256"]) == 64
            and all(c in "0123456789abcdef" for c in row["receipt_sha256"])
            for row, pair in zip(value, PAIRS)))


def settled(value, version=None):
    if not isinstance(value, dict) or value.get("schema") != CONTRACT:
        return False
    state = value.get("status")
    expected = 24 if state == "succeeded" else 21 if state == "rolled_back" else None
    return (expected is not None and value.get("from_schema") == 21 and value.get("to_schema") == 24
        and (version is None or version == expected) and chain_shape(value.get("migration_chain"))
        and isinstance(value.get("sealed_manifest_sha256"), str)
        and len(value["sealed_manifest_sha256"]) == 64
        and all(c in "0123456789abcdef" for c in value["sealed_manifest_sha256"])
        and isinstance(value.get("completed_at"), str))


def require(condition, message, impl):
    if not condition:
        raise impl.SnapshotInstallError("replica schema upgrade: " + message)


def migration_chain(manifest, impl):
    proof = manifest.get("deployment_readiness", {})
    result = []
    for name, pair in zip(("account_intake_migration", "four_platform_flow_migration",
                          "duplicate_index_migration"), PAIRS):
        part = proof.get(name, {})
        result.append({"from_schema": pair[0], "to_schema": pair[1],
                       "receipt_sha256": part.get("receipt_sha256")})
    require(proof.get("schema_version") == 24 and chain_shape(result),
            "incoming snapshot has no complete immutable 21-to-24 migration chain", impl)
    return result


def deployment_files(config, impl):
    """Record hashes, never configuration values or secret bytes."""
    paths = set(impl._config_targets(config).values())
    for service in impl.SCHEMA_SERVICES:
        root = config.systemd_root / (service + ".d")
        if root.exists():
            paths.update(p for p in root.rglob("*") if p.is_file() or p.is_symlink())
    if config.config_root.exists():
        paths.update(p for p in config.config_root.rglob("*") if p.is_file() or p.is_symlink())
    result = []
    for path in sorted(paths):
        if not path.exists() and not path.is_symlink():
            result.append({"path": str(path), "absent": True})
            continue
        before = path.lstat()
        target = path.resolve(strict=True)
        require(target.is_file(), "configuration is not a file", impl)
        row = {"path": str(path), "resolved": str(target), "sha256": impl._sha256(target),
               "mode": stat.S_IMODE(before.st_mode), "uid": before.st_uid, "gid": before.st_gid}
        require(path.lstat() == before, "configuration changed during inventory", impl)
        result.append(row)
    return result


def consumer_identity(health):
    media = health.get("media_consumers", {})
    return {k: media.get(k) for k in ("contract_version", "code_sha256", "project_root")}


def receiver_inventory(impl):
    """Pin the receiver's code closure without requiring a Web/venv deployment."""
    root = Path(__file__).resolve().parents[2]
    records = []
    for directory in ("src", "config", "deploy/server", "deploy/macos", "scripts"):
        base = impl._canonical_directory(root / directory)
        for path in sorted(base.rglob("*")):
            if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
                continue
            require(not path.is_symlink(), "receiver source has an alias", impl)
            if path.is_file():
                records.append({"path": path.relative_to(root).as_posix(), **impl._file_record(path)})
    return records


def verify_consumers(health, expected, impl):
    media = health.get("media_consumers", {})
    require(health.get("read_only") is True and health.get("lifecycle_jobs_enabled") is False
        and media.get("current_code_matches_loaded") is True
        and consumer_identity(health) == expected, "read-only consumer code or safety changed", impl)


def seal(bundle, config, impl):
    with impl._install_lock(config):
        impl._assert_transition_settled(config)
        active = config.database_root / "dcar_insight.sqlite3"
        impl._validate_sqlite(active, expected_user_version=21)
        impl._strict_schema(active, 21)
        states = impl._service_states()
        impl._require_running_services(states)
        old_receipt = impl._read_object(config.active_manifest_path)
        require(old_receipt.get("activation_status") == "succeeded"
            and old_receipt.get("database_sha256", {}).get("dcar_insight.sqlite3") == impl._sha256(active),
            "current database does not match a successful snapshot", impl)
        manifest = impl.verify_bundle(bundle, config, expected_schema=24)
        chain = migration_chain(manifest, impl)
        health = impl._read_json_url(config.health_url, config.request_timeout_seconds)
        require(health.get("database_state", {}).get("sha256") == impl._sha256(active)
            and 24 in health.get("database_state", {}).get("schema_compatibility", {}).get("supported_versions", []),
            "running API does not verify the old database and support schema24", impl)
        consumers = consumer_identity(health)
        verify_consumers(health, consumers, impl)
        value = {"schema": SEAL_CONTRACT, "from_schema": 21, "to_schema": 24,
            "migration_chain": chain, "roots": impl._config_roots(config),
            "current_release": str(impl._current_release(config)),
            "snapshot_id": manifest["snapshot_id"], "manifest_sha256": impl._sha256(bundle / "manifest.json"),
            "old_databases": impl._database_inventory(config), "old_receipt_sha256": impl._sha256(config.active_manifest_path),
            "old_receipt": impl._file_record(config.active_manifest_path),
            "old_transition": impl._read_object(config.transition_path) if config.transition_path.exists() else None,
            "configurations": deployment_files(config, impl), "consumers": consumers,
            "source_code_sha256": digest(receiver_inventory(impl)),
            "created_at": impl._utc_now()}
        path = bundle / "replica-upgrade-seal.json"
        if path.exists():
            previous = impl._read_object(path)
            require({k:v for k,v in previous.items() if k != "created_at"}
                == {k:v for k,v in value.items() if k != "created_at"}, "existing seal differs", impl)
            value = previous
        else:
            impl._write_json_atomic(path, value)
        return {"status": "sealed", "seal_sha256": impl._sha256(path), "seal": value}


def verify_seal(bundle, config, seal_sha256, impl):
    path = bundle / "replica-upgrade-seal.json"
    require(impl._sha256(path) == seal_sha256, "seal SHA differs", impl)
    value = impl._read_object(path)
    require(value.get("schema") == SEAL_CONTRACT and value.get("from_schema") == 21
        and value.get("to_schema") == 24 and chain_shape(value.get("migration_chain")), "seal contract differs", impl)
    require(value["roots"] == impl._config_roots(config)
        and value["current_release"] == str(impl._current_release(config))
        and value["configurations"] == deployment_files(config, impl)
        and value["source_code_sha256"] == digest(receiver_inventory(impl)),
        "code, configuration or release pointer changed", impl)
    require(value["manifest_sha256"] == impl._sha256(bundle / "manifest.json"), "snapshot manifest changed", impl)
    return value


def rollback(bundle, config, seal_sha256, impl, *, service_action=None):
    action = service_action or impl._default_service_action(config.service)
    with impl._install_lock(config):
        value = verify_seal(bundle, config, seal_sha256, impl)
        state = impl._read_object(config.transition_path)
        require(state.get("schema") == CONTRACT and state.get("sealed_manifest_sha256") == seal_sha256,
                "rollback belongs to another transition", impl)
        if state.get("status") == "rolled_back":
            impl._default_smoke_check(config, expected_schema=21)()
            return state
        active = impl._read_object(config.active_manifest_path)
        require(active.get("snapshot_id") in {state["snapshot_id"], state["old_snapshot_id"]},
                "a later snapshot is active; use its rollback first", impl)
        return _restore(value, state, config, impl, action)


def _restore(value, state, config, impl, action):
    state.update(status="rolling_back", completed_at=None)
    impl._write_json_atomic(config.transition_path, state)
    backup = Path(state["backup_dir"])
    expected = config.history_root / state["snapshot_id"] / "replica-schema-upgrade"
    require(backup == expected and backup.resolve() == expected, "rollback path differs", impl)
    try:
        action("stop")
        if state.get("backup_complete"):
            for record in value["old_databases"]:
                saved = backup / record["name"]
                require(impl._sha256(saved) == record["sha256"]
                    and saved.stat().st_size == record["byte_size"], "rollback database backup changed", impl)
            impl._restore_databases(config, backup, state["previous_databases"])
            impl._restore_artifacts(config, backup, impl._read_artifact_changes(backup)
                if (backup / "artifact-changes.json").exists() else [])
            impl._copy_replace(backup / "previous-active-snapshot.json", config.active_manifest_path,
                value["old_receipt"])
        require(impl._database_inventory(config) == value["old_databases"]
            and impl._sha256(config.active_manifest_path) == value["old_receipt_sha256"],
            "restored databases or receipt differ from sealed predecessor", impl)
        action("start")
        impl._default_smoke_check(config, expected_schema=21)()
        require(deployment_files(config, impl) == value["configurations"], "configuration changed during rollback", impl)
        health = impl._read_json_url(config.health_url, config.request_timeout_seconds)
        expected_sha = next(x["sha256"] for x in value["old_databases"] if x["name"] == "dcar_insight.sqlite3")
        require(health.get("database_state", {}).get("sha256") == expected_sha, "old database was not restored", impl)
        verify_consumers(health, value["consumers"], impl)
        state.update(status="rolled_back", completed_at=impl._utc_now())
        impl._write_json_atomic(config.transition_path, state)
        return state
    except Exception as error:
        state.update(status="rollback_failed", rollback_error=str(error), completed_at=None)
        impl._write_json_atomic(config.transition_path, state)
        raise


def execute(bundle, config, seal_sha256, impl, *, service_action=None, checkpoint_hook=None):
    action = service_action or impl._default_service_action(config.service)
    with impl._install_lock(config):
        value = verify_seal(bundle, config, seal_sha256, impl)
        prior = impl._read_object(config.transition_path) if config.transition_path.exists() else None
        if isinstance(prior, dict) and prior.get("schema") == CONTRACT:
            require(prior.get("sealed_manifest_sha256") == seal_sha256, "another data upgrade owns the transition", impl)
            if prior.get("status") == "succeeded":
                manifest = impl.verify_bundle(bundle, config, expected_schema=24)
                impl._default_smoke_check(config, manifest, expected_schema=24)()
                verify_consumers(impl._read_json_url(config.health_url, config.request_timeout_seconds),
                    value["consumers"], impl)
                impl._require_running_services(impl._service_states())
                return prior
            raise impl.SnapshotInstallError("existing attempt requires explicit rollback before retry")
        impl._assert_transition_settled(config)
        require(prior == value["old_transition"] and impl._database_inventory(config) == value["old_databases"]
            and impl._sha256(config.active_manifest_path) == value["old_receipt_sha256"],
            "active database or receipt changed since sealing", impl)
        manifest = impl.verify_bundle(bundle, config, expected_schema=24)
        require(migration_chain(manifest, impl) == value["migration_chain"], "migration chain changed", impl)
        impl._require_running_services(impl._service_states())
        backup = config.history_root / manifest["snapshot_id"] / "replica-schema-upgrade"
        require(not backup.exists(), "rollback directory already exists", impl)
        impl._ensure_managed_directory(backup, config)
        state = {"schema": CONTRACT, "from_schema": 21, "to_schema": 24, "status": "in_progress",
            "migration_chain": value["migration_chain"], "sealed_manifest_sha256": seal_sha256,
            "snapshot_id": manifest["snapshot_id"], "snapshot_manifest_sha256": value["manifest_sha256"],
            "old_snapshot_id": impl._read_object(config.active_manifest_path)["snapshot_id"],
            "backup_dir": str(backup), "backup_complete": False, "started_at": impl._utc_now(),
            "completed_at": None, "configuration_changed": False, "current_pointer_changed": False,
            "old_transition": prior, "checkpoints": []}
        def checkpoint(name):
            state["checkpoints"].append(name)
            impl._write_json_atomic(config.transition_path, state)
            if checkpoint_hook:
                checkpoint_hook(name)
        checkpoint("prepared")
        try:
            action("stop")
            state["previous_databases"] = impl._backup_active_databases(config, backup)
            impl._backup_receipt(config, backup)
            for record in value["old_databases"]:
                saved = backup / record["name"]
                require(impl._sha256(saved) == record["sha256"]
                    and saved.stat().st_size == record["byte_size"], "old database changed before backup", impl)
            state["backup_complete"] = True
            checkpoint("backup_complete")
            changes = impl._install_artifacts(bundle, manifest, config, backup)
            checkpoint("artifacts_applied")
            for name, source in sorted(impl._database_payloads(bundle, manifest).items()):
                impl._atomic_replace_database(config, name, source)
            history = config.history_root / manifest["snapshot_id"]
            impl._snapshot_receipt(bundle, manifest, config, history, previous_databases=state["previous_databases"],
                artifact_changes=changes, status="pending_smoke")
            checkpoint("databases_applied")
            action("start")
            impl._default_smoke_check(config, manifest, expected_schema=24)()
            health = impl._read_json_url(config.health_url, config.request_timeout_seconds)
            verify_consumers(health, value["consumers"], impl)
            require(deployment_files(config, impl) == value["configurations"]
                and str(impl._current_release(config)) == value["current_release"],
                "configuration or current release changed", impl)
            impl._require_running_services(impl._service_states())
            impl._snapshot_receipt(bundle, manifest, config, history, previous_databases=state["previous_databases"],
                artifact_changes=changes, status="succeeded")
            state.update(status="succeeded", completed_at=impl._utc_now(), consumer_identity=consumer_identity(health))
            checkpoint("succeeded")
            return state
        except Exception as error:
            state["error"] = str(error)
            _restore(value, state, config, impl, action)
            raise impl.SnapshotInstallError("data upgrade failed; previous replica restored") from error
