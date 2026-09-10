"""Seal the exact cleanup source and planned installation identity, before activation.

The final install receipt is issued separately after preparation, avoiding
cycles between the build, activation hash and final candidate database hash.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(canonical(value) + "\n")
    return {"path": str(path), "sha256": sha(path), "byte_size": path.stat().st_size}


def envelope(contract, value):
    return {"contract_version": contract, "payload": value,
            "payload_sha256": hashlib.sha256(canonical(value).encode()).hexdigest()}


def git(root, *args):
    return subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True).stdout


def package(*, checkout, source_root, evidence_root, candidate, migration, authority, data_root, formal_db):
    if source_root.exists() or (evidence_root / "build.json").exists():
        raise ValueError("New source and build paths are required")
    migration_value = json.loads(migration.read_text())
    authority_value = json.loads(authority.read_text())
    if (migration_value.get("status") != "candidate_verified"
            or sha(candidate) != migration_value["candidate"]["sha256"]
            or migration_value["source_backup"]["sha256"] != authority_value["source_database_sha256"]):
        raise ValueError("Verified migration, candidate and source authority differ")
    branch = git(checkout, "symbolic-ref", "--short", "HEAD").decode().strip()
    subprocess.run(["git", "clone", "--local", "--no-hardlinks", "--branch", branch, str(checkout), str(source_root)], check=True, capture_output=True)
    names = [os.fsdecode(item) for item in git(checkout, "ls-files", "--cached", "--others", "--exclude-standard", "-z").split(b"\0") if item]
    for name in names:
        src, dest = checkout / name, source_root / name
        if not src.exists():
            dest.unlink(missing_ok=True)
            continue
        if src.is_symlink() or not src.is_file():
            raise ValueError(f"Unexpected sealed source entry: {name}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
    status = git(source_root, "status", "--porcelain=v1", "--untracked-files=all")
    git_record = {"mode": "working-tree-source-v1", "head": git(source_root, "rev-parse", "HEAD").decode().strip(),
                  "tree": git(source_root, "rev-parse", "HEAD^{tree}").decode().strip(), "branch": branch,
                  "status_porcelain_sha256": hashlib.sha256(status).hexdigest()}
    records = []
    for name in sorted(names):
        path = source_root / name
        if path.exists():
            records.append({"path": name, "sha256": sha(path), "byte_size": path.stat().st_size,
                            "mode": stat.S_IMODE(path.stat().st_mode)})
    tree_ref = write(evidence_root / "source-tree.json", {"contract": "writer-source-tree-v1", "source_root": str(source_root), "git": git_record, "files": records})
    plan_ref = write(evidence_root / "source-plan.json", {"contract": "account-cleanup-source-plan-v1", "transition": "account-cleanup-0907-v1",
                     "project_root": str(data_root), "source_root": str(source_root), "git": git_record, "source_tree": tree_ref})
    identity = candidate.stat()
    at = datetime.now(timezone.utc).isoformat()
    runtime_ref = write(evidence_root / "runtime.json", envelope("runtime-root-binding-v1", {
        "status": "succeeded", "binding_stage": "prepared_installation", "project_root": str(data_root), "source_root": str(source_root),
        "formal_database": {"path": str(formal_db), "device": identity.st_dev, "inode": identity.st_ino}, "created_at": at}))
    def reference(path):
        return {"path": str(path), "sha256": sha(path), "byte_size": path.stat().st_size}
    operation = next(iter(authority_value["operations"].values()))
    config_sha = json.loads(operation["gate"]["evidence_json"])["bindings"]["config_receipt_sha256"]
    generation = {"contract": "account-cleanup-generation-v1", "generation_id": "account-cleanup-0907-v1",
        "source_database_sha256": authority_value["source_database_sha256"], "migration_receipt": reference(migration),
        "source_authority": reference(authority), "source_tree": tree_ref,
        "selection_sha256": authority_value["selection_sha256"], "config_sha256": config_sha,
        "transport_manifest": authority_value["transport_manifest"], "actor": "user-approved-cleanup",
        "reason": "User confirmed removal of out-of-list accounts and their exclusive content; retain valid history and existing authorized capture subset",
        "issued_at": at}
    # Source inventory is verified in full before import. Critical files are
    # additionally bound for running admission checks.
    critical = {row["path"]: row["sha256"] for row in records if row["path"].startswith(("src/", "config/")) and row["path"].endswith((".py", ".json"))}
    build_ref = write(evidence_root / "build.json", envelope("sealed-build-receipt-v1", {
        "status": "succeeded", "git": git_record, "project_root": str(data_root), "source_root": str(source_root),
        "runtime_root_receipt": runtime_ref, "schema_contract": {"code_schema": 20, "formal_schema": 20},
        "code_successor_plan": plan_ref, "critical_files": critical, "account_cleanup_generation": generation,
        "created_at": at, "validation_scope": "targeted account cleanup, directory, media and installation tests"}))
    return {"build_receipt": build_ref, "runtime_receipt": runtime_ref, "source_plan": plan_ref, "source_tree": tree_ref, "source_root": str(source_root)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("checkout", "source-root", "evidence-root", "candidate", "migration", "authority", "data-root", "formal-db"):
        parser.add_argument("--" + key, required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(package(**vars(args)), ensure_ascii=False))


if __name__ == "__main__":
    main()
