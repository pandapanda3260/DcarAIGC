"""Keep installed mutable data separate from an explicitly sealed source tree.

Path resolution is not authorization. The launcher and the code-successor
contract verify the source receipt before using an isolated Writer runtime.
Unconfigured checkouts and explicitly supplied fixture roots retain their
existing behavior.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import plistlib
import pwd
import re
import stat
import subprocess
import sys
from pathlib import Path
from typing import Sequence


def verified_git(root: Path, *arguments: str) -> bytes:
    """Read local Git without running repository filters, monitors or textconv."""
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith("GIT_CONFIG_") and key not in {
                       "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR",
                       "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES",
                       "GIT_EXTERNAL_DIFF", "GIT_DIFF_OPTS", "GIT_ATTR_SOURCE"}}
    environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                       GIT_NO_LAZY_FETCH="1", GIT_OPTIONAL_LOCKS="0", GIT_PAGER="cat")
    prefix = ["git", "-c", "core.fsmonitor=false", "-C", str(root)]
    configured = subprocess.run([*prefix, "config", "--null", "--list"], check=True,
                                env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout
    for item in configured.split(b"\0"):
        key, _, value = item.partition(b"\n")
        name = key.decode("utf-8").lower()
        if (name.startswith("filter.") or name == "diff.external"
                or re.fullmatch(r"diff\..+\.(command|textconv)", name)
                or (name == "core.fsmonitor" and value.lower() not in {b"false", b"0", b"no", b"off"})):
            raise ValueError("external Git helper is forbidden in a sealed source repository")
    return subprocess.run([*prefix, *arguments], check=True, env=environment,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout


def _raw_file(path: Path, *, private: bool = False, limit: int = 16 * 1024 * 1024) -> bytes:
    before = path.lstat()
    if (not path.is_absolute() or path.resolve(strict=True) != path
            or not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
            or before.st_nlink != 1 or before.st_mode & 0o022
            or (private and stat.S_IMODE(before.st_mode) != 0o600)
            or not 0 <= before.st_size <= limit):
        raise ValueError("unsafe bootstrap source or receipt")
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as handle:
        body = handle.read(limit + 1)
        opened = os.fstat(handle.fileno())
    def identity(value):
        return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns
    if identity(before) != identity(opened) or identity(before) != identity(path.lstat()) or len(body) != before.st_size:
        raise ValueError("bootstrap source or receipt changed")
    return body


def _json_object(body: bytes) -> dict:
    def unique(pairs):
        result = dict(pairs)
        if len(result) != len(pairs):
            raise ValueError("duplicate bootstrap receipt field")
        return result
    result = json.loads(body, object_pairs_hook=unique)
    if not isinstance(result, dict):
        raise ValueError("bootstrap receipt must be an object")
    return result


def _reference(reference: dict) -> dict:
    body = _raw_file(Path(reference["path"]), private=True)
    if hashlib.sha256(body).hexdigest() != reference.get("sha256"):
        raise ValueError("bootstrap receipt SHA differs")
    return _json_object(body)


def verify_source_before_import(*, data: Path, source: Path, build_receipt: Path | None,
                                home: Path | None = None) -> dict:
    """Stdlib-only verification: no source module executes before all hashes pass."""
    git_directory = source / ".git"
    if (not git_directory.is_dir() or git_directory.resolve(strict=True) != git_directory
            or (git_directory / "commondir").exists()
            or (git_directory / "objects/info/alternates").exists()):
        raise ValueError("sealed source requires independent Git objects")
    home = home or Path(pwd.getpwuid(os.geteuid()).pw_dir)
    installed = plistlib.loads(_raw_file(home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"))
    environment = installed.get("EnvironmentVariables", {})
    selected = Path(environment.get("DCAR_LOADED_BUILD_RECEIPT", ""))
    if (installed.get("Label") != "cn.tj.dcar.writer-worker"
            or installed.get("WorkingDirectory") != str(data)
            or installed.get("ProgramArguments") != [str(source / "deploy/macos/run_writer_worker.sh")]
            or environment.get("DCAR_PROJECT_ROOT") != str(data)
            or environment.get("DCAR_WRITER_SOURCE_ROOT") != str(source)
            or (build_receipt is not None and build_receipt != selected)):
        raise ValueError("bootstrap does not match the installed writer")
    envelope = _json_object(_raw_file(selected, private=True))
    payload = envelope.get("payload")
    digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if (envelope.get("contract_version") != "sealed-build-receipt-v1"
            or envelope.get("payload_sha256") != digest or not isinstance(payload, dict)):
        raise ValueError("bootstrap build envelope differs")
    plan = _reference(payload["code_successor_plan"])
    if (plan.get("contract") != "writer-source-isolation-successor-plan-v1"
            or plan.get("transition") != "writer-source-isolation-20260907-v1"
            or plan.get("project_root") != str(data) or plan.get("source_root") != str(source)
            or plan.get("git") != payload.get("git") or payload.get("status") != "succeeded"):
        raise ValueError("bootstrap source plan differs")
    manifest = _reference(plan["source_tree"])
    git_record = plan["git"]
    if (manifest.get("contract") != "writer-source-tree-v1" or manifest.get("source_root") != str(source)
            or manifest.get("git") != git_record or not isinstance(manifest.get("files"), list)):
        raise ValueError("bootstrap manifest binding differs")
    def git(*args):
        return verified_git(source, *args)
    status = git("status", "--porcelain=v1", "--untracked-files=all")
    if (git("rev-parse", "HEAD").decode().strip() != git_record["head"]
            or git("rev-parse", "HEAD^{tree}").decode().strip() != git_record["tree"]
            or git("symbolic-ref", "--quiet", "--short", "HEAD").decode().strip() != git_record["branch"]
            or hashlib.sha256(status).hexdigest() != git_record["status_porcelain_sha256"]):
        raise ValueError("bootstrap source Git state differs")
    actual_names = {os.fsdecode(name) for name in git("ls-files", "--cached", "--others", "--exclude-standard", "-z").split(b"\0") if name}
    actual_names = {name for name in actual_names if (source / name).exists() or (source / name).is_symlink()}
    names = set()
    total = 0
    for record in manifest["files"]:
        name = record["path"]
        if (not isinstance(name, str) or name.startswith("/") or name in names
                or any(part in ("", ".", "..", ".git") for part in name.split("/"))
                or any(char in name for char in ("\0", "\n", "\r", "\\"))):
            raise ValueError("bootstrap source name differs")
        path = source / name
        body = _raw_file(path, limit=512 * 1024 * 1024 - total)
        total += len(body)
        if (hashlib.sha256(body).hexdigest() != record.get("sha256") or len(body) != record.get("byte_size")
                or stat.S_IMODE(path.stat().st_mode) != record.get("mode")):
            raise ValueError("bootstrap source content differs")
        names.add(name)
    if names != actual_names:
        raise ValueError("bootstrap source inventory differs")
    for relative in ("src", "scripts", "deploy", "config"):
        for directory, folders, files in os.walk(source / relative, followlinks=False):
            if any((Path(directory) / name).is_symlink() for name in folders):
                raise ValueError("bootstrap source directory is a symlink")
            for name in files:
                path = Path(directory) / name
                if path.suffix in {".py", ".pyc", ".so", ".sh", ".json", ".yaml", ".yml", ".toml"} and path.relative_to(source).as_posix() not in names:
                    raise ValueError("unlisted bootstrap executable or configuration")
    if git("status", "--porcelain=v1", "--untracked-files=all") != status:
        raise ValueError("bootstrap Git changed during verification")
    return {"source_tree_sha256": plan["source_tree"]["sha256"], "files": len(names)}


def _absolute_directory(value: str, label: str) -> Path:
    path = Path(value)
    if (not path.is_absolute() or not path.is_dir()
            or path.resolve(strict=True) != path or path.is_symlink()):
        raise ValueError(f"{label} must be an existing absolute non-symlink directory")
    return path


def _roots() -> tuple[Path, Path] | None:
    selected = os.environ.get("DCAR_WRITER_SOURCE_ROOT")
    if not selected:
        return None
    data = _absolute_directory(os.environ.get("DCAR_PROJECT_ROOT", ""), "data project root")
    source = _absolute_directory(selected, "writer source root")
    if source == data or source.is_relative_to(data) or data.is_relative_to(source):
        raise ValueError("writer source and mutable project roots must be independent")
    return data, source


def project_root(default: Path) -> Path:
    """Map modules loaded from the sealed tree back to their original data root."""
    roots = _roots()
    resolved = default.resolve(strict=True)
    return roots[0] if roots is not None and resolved == roots[1] else resolved


def source_root(project: Path) -> Path:
    """Resolve source only for the explicitly selected installed data root."""
    roots = _roots()
    resolved = project.resolve(strict=True)
    return roots[1] if roots is not None and resolved == roots[0] else resolved


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-source", action="store_true", required=True)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--mode", choices=("writer", "publisher"), required=True)
    parser.add_argument("--build-receipt", type=Path)
    args = parser.parse_args(argv)
    data = _absolute_directory(str(args.project_root), "data project root")
    source = _absolute_directory(str(args.source_root), "writer source root")
    if _roots() != (data, source) or Path(__file__).resolve().parents[3] != source:
        parser.error("source verifier does not match the selected installed runtime")
    verify_source_before_import(data=data, source=source, build_receipt=args.build_receipt)
    sys.path[:0] = [str(source / "src/dcar_eval"), str(source / "scripts")]
    from v8.runtime_source_successor import verify_bootstrap
    result = verify_bootstrap(project_root=data, source_root=source,
                              build_receipt=args.build_receipt, mode=args.mode)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
