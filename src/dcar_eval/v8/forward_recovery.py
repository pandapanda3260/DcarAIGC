"""Explicit future-only recovery of an existing current-activation HOLD.

This contract is a bounded operational release, not a 200-sample operation
qualification. Historical unknown charges, request identities, the original
HOLD and its complete diagnostic tail remain authoritative.
"""

from __future__ import annotations

import hashlib
import ast
import io
import json
import os
import re
import shlex
import shutil
import sqlite3
import stat
import subprocess
import tarfile
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from apscheduler.schedulers.base import BaseScheduler, STATE_PAUSED, STATE_RUNNING  # type: ignore[import-untyped]

from . import paid_drain, provider_budget
from .automatic_scope import automatic_from_date
from .profile_control import (
    ProfileControlError, _current_hold_prerequisites, _hold_event,
    _write_native_event_mirror,
)
from .raw_evidence import canonical_json_bytes
from .runtime_database import require_current_process_writer_lock
from .source_routing import parse_time
from .storage import PROJECT_ROOT, connect, now_utc, transaction
from .runtime_paths import source_root, verified_git

CONTRACT_VERSION = "current_activation_forward_release_v1"
PURPOSE = "forward_only_release"
MIN_SCOPE_START = date(2026, 9, 6)
MIN_FREE_BYTES = 10 * 1024**3
BEIJING = ZoneInfo("Asia/Shanghai")
_SUCCESSOR_SEND_FILES = tuple(f"src/dcar_eval/v8/{name}.py" for name in (
    "capture", "paid_dispatch", "paid_identity", "providers", "provider_transport",
    "raw_evidence",
)) + ("src/dcar_eval/tikhub_config.py",)
_SUCCESSOR_READER_SHA256 = "d9ea4d1dfe49a73e5c4a6185ba1c0b65674bdca19e62854a939b1a71407759ba"
_SUCCESSOR_OVERVIEW_TRANSITIONS = {
    "src/dcar_eval/v8/api.py": (
        "19512d05ce58f94cfc442ffe3c98b7f57004d5ff3469ab403bcfe444d6b8f915",
        "14b6f528bbccbea878a348d748883358d6f211fe2771c351de283e8c6e64a255",
    ),
    "src/dcar_eval/v8/source_routing.py": (
        "ff799e2652b6d251a166dd001487b715ba6f18878c733c41037ba296852b77f1",
        "0834007f6985e85693b2610b3d8ab991125717f9ffdd2208d28e9c675db7b569",
    ),
}
_SUCCESSOR_ACCOUNT_READ_TRANSITIONS = frozenset({
    ("19512d05ce58f94cfc442ffe3c98b7f57004d5ff3469ab403bcfe444d6b8f915",
     "81c4b9ff0ab25b4b0a924b3debde873cb89309a146e7ebab835a6b47862b283c"),
    ("14b6f528bbccbea878a348d748883358d6f211fe2771c351de283e8c6e64a255",
     "81c4b9ff0ab25b4b0a924b3debde873cb89309a146e7ebab835a6b47862b283c"),
})

_SUCCESSOR_METRIC_READ_TRANSITIONS = frozenset({
    ("0834007f6985e85693b2610b3d8ab991125717f9ffdd2208d28e9c675db7b569",
     "de9c232f8896c27ea927f6beb58fbfa0bed95b826b4157a8a4e263b0a8181557"),
})

# An account operating-state write is a business mutation, not a read-only API
# exemption. Only the jointly reviewed source transition may inherit the same
# schema19 collector authority. The hashes bind the reviewed account-status release only.
_SUCCESSOR_ACCOUNT_STATUS_CRITICAL_ADDITIONS = frozenset(f"src/dcar_eval/v8/{name}.py" for name in (
    "account_operating_status", "account_operating_receipts", "statistics_scope",
    "operations", "report_inputs", "spu_audience", "system_roster",
))
_SUCCESSOR_ACCOUNT_STATUS_NEW_MODULES = frozenset(f"src/dcar_eval/v8/{name}.py" for name in (
    "account_operating_status", "account_operating_receipts", "statistics_scope",
))
_SUCCESSOR_ACCOUNT_STATUS_TRANSITIONS: dict[str, tuple[str | None, str]] = {
    "scripts/seal_r0_receipts.py": (
        '0c09d722120a13a8f7492527930bb7deccecb0ca119924e1d0c87af8f1e95f68',
        "2d2e39b7f65aa7fe1a56780361d828e1e2bbd194348e94b81b3442feb7dfa34e",
    ),
    "src/dcar_eval/v8/account_operating_receipts.py": (
        None,
        "6132bc2780a7c54449d476500cf6733b5daf25b4d37fb33d88412e8344b9d719",
    ),
    "src/dcar_eval/v8/account_operating_status.py": (
        None,
        "3b4ccd8e51e77a03b218f20feb848c7e29c4e47e360db25625c2aeedcdf75cba",
    ),
    "src/dcar_eval/v8/api.py": (
        '81c4b9ff0ab25b4b0a924b3debde873cb89309a146e7ebab835a6b47862b283c',
        "9cac7ad7aad388be9ff978c554caa0c9c64295655686aa509217b23556194168",
    ),
    "src/dcar_eval/v8/operations.py": (
        'b7d672535965391a3404977609606ab73a80aa5dd66d0fae6077d39f17ea99b2',
        "bbeefc2da78eb17cef8ed3c87792343b47cc44f400d0b5c700eeadb9c542a995",
    ),
    "src/dcar_eval/v8/report_inputs.py": (
        'd1c7618e0c0c31c405ed2fe03bdad9154290fd84ad9562a13ec590fe41987106',
        "258f56f3675509c8b1a2200fdba7f5bf4330e34ec3cb71bbba4dea8d7a82f383",
    ),
    "src/dcar_eval/v8/spu_audience.py": (
        '23811ed87ed850ddd35698faf51f684467b6e4e73dc34ae5308a5606f57e54ac',
        "fd22effc4da29bc4bae320cbcf4f6ef763e0479a6fecb96ac704471d2f1c2275",
    ),
    "src/dcar_eval/v8/statistics_scope.py": (
        None,
        "34cdfea4d3e8fa1618d6fd838cbd6709c04c7fadc9dd30d46b927efb90c8610e",
    ),
    "src/dcar_eval/v8/system_roster.py": (
        '4a8bed5a8e62ae234418dc31599f4a4ab54070e11306974732777ca3baa619bc',
        "48b71cf7a95576e973fda7cd0d6f7f6d33e255b3f9421c7ae41f13db85bbd607",
    ),
}


def _require(condition: bool, code: str, message: str) -> None:
    if not condition:
        raise ProfileControlError(code, message)


def _digest(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def is_forward_release(control: Mapping[str, Any]) -> bool:
    return control.get("contract_version") == CONTRACT_VERSION and control.get("control_purpose") == PURPOSE


def forward_release_matches(
    control: Mapping[str, Any], *, active: Mapping[str, Any],
    start: Mapping[str, Any], released_at: str,
) -> bool:
    """Recognize only an explicit same-HOLD release, without recursive gates."""
    binding = control.get("hold_binding")
    try:
        scope = date.fromisoformat(str(control.get("scope_start")))
        return bool(
            is_forward_release(control) and isinstance(binding, dict)
            and control.get("released_at") == released_at
            and MIN_SCOPE_START <= scope <= parse_time(released_at).astimezone(BEIJING).date()
            and control.get("scope_start") == scope.isoformat()
            and binding.get("start_event_id") == start["event_id"]
            and binding.get("start_event_hash") == start["event_hash"]
            and binding.get("drain_id") == start["drain_id"]
            and start["payload"]["control"].get("contract_version") == paid_drain.CURRENT_ACTIVATION_HOLD_CONTRACT
            and start["payload"]["control"].get("control_purpose") == "hold_begin"
            and all(control.get(key) == binding.get(key) == value for key, value in {
                "activation_id": int(active["activation_id"]),
                "roster_snapshot_id": int(active["roster_snapshot_id"]),
                "roster_snapshot_hash": active["roster_members_sha256"],
            }.items())
            and control.get("operation_qualified") is False
            and control.get("historical_backfill_authorized") is False
        )
    except (TypeError, ValueError, KeyError):
        return False


def _scope(scope_start: str, at: str) -> None:
    try:
        requested = date.fromisoformat(scope_start)
    except (TypeError, ValueError) as exc:
        raise ProfileControlError("forward_scope_invalid", "Forward release needs a canonical business date") from exc
    _require(
        scope_start == requested.isoformat()
        and MIN_SCOPE_START <= requested <= parse_time(at).astimezone(BEIJING).date()
        and automatic_from_date() == requested,
        "forward_scope_invalid", "Forward release must match the configured future-only business date",
    )


def _private_receipt(path: Path, expected_sha: str, contract: str) -> dict[str, Any]:
    try:
        st = path.lstat()
        _require(path.is_absolute() and stat.S_ISREG(st.st_mode) and st.st_nlink == 1
                 and st.st_uid == os.geteuid() and stat.S_IMODE(st.st_mode) == 0o600,
                 "forward_build_invalid", "Runtime evidence must be a private, single-link regular file")
        data = path.read_bytes()
        envelope = json.loads(data)
        # sealed-build/runtime-root receipts use seal_r0_receipts._canonical_json:
        # compact sorted UTF-8 JSON without the raw-evidence trailing newline.
        payload_digest = hashlib.sha256(json.dumps(
            envelope.get("payload"), ensure_ascii=False, sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        _require(hashlib.sha256(data).hexdigest() == expected_sha
                 and envelope.get("contract_version") == contract
                 and isinstance(envelope.get("payload"), dict)
                 and envelope.get("payload_sha256") == payload_digest,
                 "forward_build_invalid", "Runtime evidence hash or contract differs")
        return dict(envelope["payload"])
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        raise ProfileControlError("forward_build_invalid", "Runtime evidence is unreadable") from exc


def _runtime_identity(connection: sqlite3.Connection, binding: Mapping[str, Any]) -> dict[str, Any]:
    """Bind the actual loaded receipt, code files and live database inode."""
    build_sha = str(binding["build_receipt_sha256"])
    _require(os.environ.get("DCAR_LOADED_BUILD_ID") == "sha256:" + build_sha,
             "forward_build_invalid", "Loaded Writer build does not match the final HOLD generation")
    build = _private_receipt(Path(os.environ.get("DCAR_LOADED_BUILD_RECEIPT", "")), build_sha,
                             "sealed-build-receipt-v1")
    runtime_ref = build.get("runtime_root_receipt", {})
    _require(build.get("status") == "succeeded" and isinstance(runtime_ref, dict)
             and runtime_ref.get("sha256") == binding["runtime_root_receipt_sha256"],
             "forward_build_invalid", "Loaded build does not bind the final runtime receipt")
    runtime = _private_receipt(Path(str(runtime_ref.get("path", ""))),
                               str(binding["runtime_root_receipt_sha256"]), "runtime-root-binding-v1")
    _require(runtime.get("project_root") == str(PROJECT_ROOT.resolve()),
             "forward_runtime_invalid", "Loaded runtime belongs to a different checkout")
    critical = build.get("critical_files")
    _require(isinstance(critical, dict) and bool(critical), "forward_build_invalid", "Build has no verified code inventory")
    assert isinstance(critical, dict)
    for relative, expected in critical.items():
        path = source_root(PROJECT_ROOT) / relative
        _require(not path.is_symlink() and path.is_file() and path.resolve().is_relative_to(source_root(PROJECT_ROOT))
                 and hashlib.sha256(path.read_bytes()).hexdigest() == expected,
                 "forward_build_invalid", "Runtime code changed after the sealed build")
    database = Path(str(connection.execute("PRAGMA database_list").fetchone()[2])).resolve(strict=True)
    current = database.stat()
    frozen = runtime.get("formal_database", {})
    _require(isinstance(frozen, dict) and frozen.get("path") == str(database)
             and frozen.get("device") == current.st_dev and frozen.get("inode") == current.st_ino,
             "forward_runtime_invalid", "Runtime receipt does not identify the open formal database")
    return {"build_receipt_sha256": build_sha, "runtime_root_receipt_sha256": binding["runtime_root_receipt_sha256"],
            "database_path": str(database), "database_device": current.st_dev, "database_inode": current.st_ino}


@lru_cache(maxsize=256)
def _successor_git(project: str, *arguments: str) -> bytes:
    try:
        return verified_git(source_root(Path(project)), *arguments)
    except (OSError, subprocess.SubprocessError) as exc:
        raise ProfileControlError("forward_build_invalid", "Sealed successor source is unavailable") from exc


def _successor_receipt(reference: Mapping[str, Any], contract: str) -> dict[str, Any]:
    _require(isinstance(reference, dict), "forward_build_invalid", "Successor receipt reference is invalid")
    value = _private_receipt(Path(str(reference.get("path", ""))), str(reference.get("sha256", "")), contract)
    if "payload_sha256" in reference:
        digest = hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                          separators=(",", ":")).encode()).hexdigest()
        _require(reference["payload_sha256"] == digest,
                 "forward_build_invalid", "Referenced producer payload hash differs")
    return value


def _successor_patch(before: bytes, section: bytes) -> bytes:
    """Apply an exact textual Git hunk in memory, never to the live checkout."""
    lines, original = section.splitlines(keepends=True), before.splitlines(keepends=True)
    output: list[bytes] = []
    consumed = 0
    index = 0
    while index < len(lines):
        line = lines[index]
        match = re.match(rb"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", line)
        index += 1
        if not match:
            _require(not line.startswith((b"GIT binary patch", b"Binary files", b"rename from", b"rename to")),
                     "forward_build_invalid", "Successor source archive uses an unsupported binary or rename patch")
            continue
        old_start, old_count = int(match[1]), int(match[2] or b"1")
        new_start, new_count = int(match[3]), int(match[4] or b"1")
        target = old_start - 1 if old_count else old_start
        _require(consumed <= target <= len(original), "forward_build_invalid", "Sealed patch hunk is outside its source")
        output.extend(original[consumed:target])
        consumed = target
        _require(len(output) == (new_start - 1 if new_count else new_start),
                 "forward_build_invalid", "Sealed patch new-file position differs")
        old_seen = new_seen = 0
        while index < len(lines) and not lines[index].startswith(b"@@ "):
            change = lines[index]
            index += 1
            _require(change[:1] in (b" ", b"-", b"+"), "forward_build_invalid", "Sealed patch hunk has an invalid line")
            body = change[1:]
            if index < len(lines) and lines[index].startswith(b"\\ No newline at end of file"):
                body = body.removesuffix(b"\n")
                index += 1
            if change[:1] in (b" ", b"-"):
                _require(consumed < len(original) and original[consumed] == body,
                         "forward_build_invalid", "Sealed patch does not match the original source")
                consumed += 1
                old_seen += 1
            if change[:1] in (b" ", b"+"):
                output.append(body)
                new_seen += 1
        _require((old_seen, new_seen) == (old_count, new_count),
                 "forward_build_invalid", "Sealed patch hunk line count differs")
    output.extend(original[consumed:])
    return b"".join(output)


@lru_cache(maxsize=32)
def _successor_patch_sources(project: str, head: str, staged: bytes, unstaged: bytes) -> dict[str, bytes | None]:
    sources: dict[str, bytes | None] = {}
    for patch in (staged, unstaged):
        for section in re.split(rb"(?=^diff --git )", patch, flags=re.MULTILINE):
            if not section:
                continue
            header = shlex.split(section.splitlines()[0].decode("utf-8"))
            _require(len(header) == 4 and header[:2] == ["diff", "--git"]
                     and header[2].startswith("a/") and header[3] == "b/" + header[2][2:],
                     "forward_build_invalid", "Sealed source patch paths are invalid")
            relative = header[2][2:]
            _require(not Path(relative).is_absolute() and not ({"..", ".git"} & set(Path(relative).parts)),
                     "forward_build_invalid", "Sealed source patch escapes the checkout")
            added = b"--- /dev/null\n" in section or b"new file mode " in section
            removed = b"+++ /dev/null\n" in section or b"deleted file mode " in section
            before = sources.get(relative) if relative in sources else (
                b"" if added else _successor_git(project, "show", f"{head}:{relative}")
            )
            result = _successor_patch(before or b"", section)
            _require(not removed or result == b"", "forward_build_invalid", "Deleted source patch retains content")
            sources[relative] = None if removed else result
    return sources


def _successor_archive(build: Mapping[str, Any], *, live: bool = False) -> dict[str, bytes | None]:
    git = build["git"]
    if git.get("mode") != "working-tree-source-v1":
        _require(not git.get("working_tree"), "forward_build_invalid", "Unrecognized working-tree source declaration")
        return {}
    reference, manifest = build.get("source_archive", {}), git.get("working_tree", {})
    _require(isinstance(reference, dict) and isinstance(manifest, dict)
             and isinstance(manifest.get("patches"), dict) and set(manifest["patches"]) == {"staged.patch", "unstaged.patch"}
             and isinstance(manifest.get("untracked_files"), list),
             "forward_build_invalid", "Working-tree source archive or manifest is missing")
    path = Path(str(reference.get("path", "")))
    try:
        metadata = path.lstat()
        _require(path.is_absolute() and stat.S_ISREG(metadata.st_mode) and metadata.st_uid == os.geteuid()
                 and metadata.st_nlink == 1 and stat.S_IMODE(metadata.st_mode) == 0o600,
                 "forward_build_invalid", "Source archive must be an owned private regular file")
        data = path.read_bytes()
        _require(hashlib.sha256(data).hexdigest() == reference.get("sha256")
                 and len(data) == reference.get("size"), "forward_build_invalid", "Sealed source archive hash or length differs")
        expected = {name: {**value, "mode": 0o600} for name, value in manifest["patches"].items()}
        for item in manifest["untracked_files"]:
            relative = Path(str(item.get("path", "")))
            _require(not relative.is_absolute() and bool(str(relative))
                     and not ({"..", ".git"} & set(relative.parts)),
                     "forward_build_invalid", "Untracked source path is invalid")
            name = "untracked/" + relative.as_posix()
            _require(name not in expected, "forward_build_invalid", "Duplicate untracked source declaration")
            expected[name] = item
        contents = {}
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
            for member in archive:
                spec = expected.get(member.name)
                _require(spec is not None and member.name not in contents and member.isfile()
                         and member.size == spec["bytes"] and member.mode == spec["mode"],
                         "forward_build_invalid", "Source archive member inventory differs")
                assert spec is not None
                stream = archive.extractfile(member)
                assert stream is not None
                body = stream.read()
                _require(hashlib.sha256(body).hexdigest() == spec["sha256"],
                         "forward_build_invalid", "Source archive member hash differs")
                contents[member.name] = body
        _require(set(contents) == set(expected), "forward_build_invalid", "Source archive member is missing")
        result = dict(_successor_patch_sources(str(PROJECT_ROOT.resolve()), str(git["head"]),
                      contents["staged.patch"], contents["unstaged.patch"]))
        result.update({name.removeprefix("untracked/"): body for name, body in contents.items()
                       if name.startswith("untracked/")})
        if live:
            for source_relative, source_body in result.items():
                source = source_root(PROJECT_ROOT) / source_relative
                _require((source_body is None and not source.exists()) or (source_body is not None
                         and not source.is_symlink() and source.is_file()
                         and source.resolve().is_relative_to(source_root(PROJECT_ROOT)) and source.read_bytes() == source_body),
                         "forward_build_invalid", "Live working-tree source differs from its tested archive")
        return result
    except (OSError, ValueError, KeyError, TypeError, tarfile.TarError) as exc:
        raise ProfileControlError("forward_build_invalid", "Sealed working-tree source is unreadable") from exc


def _successor_build(build: Mapping[str, Any], connection: sqlite3.Connection, *, live: bool = False) -> dict[str, Any]:
    """Verify one real code-only sealed build without replaying its old tests."""
    schema = build.get("schema_contract", {})
    git = build.get("git", {})
    _require(build.get("status") == "succeeded" and isinstance(schema, dict)
             and schema.get("formal_schema") == schema.get("code_schema") == 19
             and schema.get("operation") == "code_update" and schema.get("transition") == "19-to-19"
             and connection.execute("PRAGMA user_version").fetchone()[0] == 19
             and isinstance(git, dict) and len(str(git.get("head", ""))) == 40,
             "forward_build_invalid", "Successor must retain the released schema19 code-update contract")
    assert isinstance(git, dict)
    project = str(PROJECT_ROOT.resolve())
    tree = _successor_git(project, "rev-parse", str(git["head"]) + "^{tree}").decode().strip()
    _require(tree == git.get("tree"), "forward_build_invalid", "Sealed Git tree differs from the source commit")
    _successor_archive(build)
    tests = _successor_receipt(build.get("test_results_receipt", {}), "test-results-v1")
    required = {"backend", "frontend", "lint", "typecheck", "ruff", "mypy"}
    test_git, results = tests.get("git", {}), tests.get("results", {})
    _require(tests.get("status") == "passed" and isinstance(test_git, dict)
             and all(test_git.get(key) == git.get(key) for key in ("head", "tree"))
             and (git.get("mode") != "working-tree-source-v1" or test_git == git)
             and set(tests.get("required_results", [])) >= required and isinstance(results, dict)
             and all(isinstance(results.get(name), dict) and results[name].get("status") == "passed"
                     and results[name].get("exit_code") == 0 for name in required),
             "forward_build_invalid", "Successor lacks successful tests sealed to the same source")
    runtime = _successor_receipt(build.get("runtime_root_receipt", {}), "runtime-root-binding-v1")
    database = Path(str(connection.execute("PRAGMA database_list").fetchone()[2])).resolve(strict=True)
    current = database.stat()
    frozen = runtime.get("formal_database", {})
    _require(Path(str(runtime.get("project_root", ""))).is_absolute()
             and (not live or runtime.get("project_root") == project) and isinstance(frozen, dict)
             and frozen.get("path") == str(database) and frozen.get("device") == current.st_dev
             and frozen.get("inode") == current.st_ino and frozen.get("user_version") == 19,
             "forward_runtime_invalid", "Successor runtime is not the same formal database inode")
    critical = build.get("critical_files")
    _require(isinstance(critical, dict) and bool(critical), "forward_build_invalid", "Successor code inventory is missing")
    return runtime


def _successor_reader_compatible(before: bytes, after: bytes) -> bool:
    """Allow only this reader upgrade; retain every original release/send guard."""
    helpers = {"_successor_git", "_successor_receipt", "_successor_patch", "_successor_patch_sources",
               "_successor_archive", "_successor_build", "_successor_reader_compatible",
               "_successor_source", "_released_runtime_identity"}
    try:
        old, new = ast.parse(before), ast.parse(after)
        additions = [node for node in new.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in helpers]
        helper_nodes: list[ast.stmt] = [node for node in new.body if node in additions
            or (isinstance(node, ast.Assign) and any(isinstance(name, ast.Name)
                and name.id in {"_SUCCESSOR_SEND_FILES", "_SUCCESSOR_OVERVIEW_TRANSITIONS", "_SUCCESSOR_ACCOUNT_READ_TRANSITIONS", "_SUCCESSOR_METRIC_READ_TRANSITIONS",
                                "_SUCCESSOR_ACCOUNT_STATUS_CRITICAL_ADDITIONS", "_SUCCESSOR_ACCOUNT_STATUS_NEW_MODULES"} for name in node.targets))
            or (isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
                and node.target.id == "_SUCCESSOR_ACCOUNT_STATUS_TRANSITIONS")]
        fingerprint = hashlib.sha256(ast.dump(ast.Module(body=helper_nodes, type_ignores=[]),
                                             include_attributes=False).encode()).hexdigest()
        added_names = {node.name for node in additions}
        previous_helpers = helpers - {"_successor_patch", "_successor_patch_sources", "_successor_archive"}
        if not ((added_names == helpers and fingerprint in {_SUCCESSOR_READER_SHA256,
                    "31acd2bb75ee381e6aec6f7d2ae9d2e6cebbe1918848058eb45f695a9c27e657",
                    "0ed69304d32786c44c437db2f340e3273453bbbbab57c175040df6422eb9cc99",
                    "fd51095ef365cb0bfa1f7565d0747debefee65ed0caabba0a1aa973b90bf612d"})
                or (added_names == previous_helpers
                    and fingerprint == "0a4dcede71d796714b8fd9c22a4bb7717ffa321da7362c509656dc4b599e0b5e")):
            return False
        class Rewrite(ast.NodeTransformer):
            def visit_Name(self, node: ast.Name) -> ast.Name:
                if node.id == "_runtime_identity":
                    node.id = "_released_runtime_identity"
                return node
        for module in (old, new):
            # Only the two reviewed identity readers may differ: the original
            # checkout reader and the sealed-source reader with identical guards.
            identities = [node for node in module.body if isinstance(node, ast.FunctionDef)
                          and node.name == "_runtime_identity"]
            if len(identities) != 1 or hashlib.sha256(ast.dump(identities[0], include_attributes=False).encode()).hexdigest() not in {
                "9d2122a460073f1000513bd3e255f17bcdbc744a8d6f86b162887dfe959348e4",
                "d1cd8a2aec0e7d95548e770e35cca9861087857ee120b532757863caf8cc723e",
            }:
                return False
            module.body[module.body.index(identities[0])] = ast.parse("def _runtime_identity(): pass").body[0]
            for node in module.body:
                if isinstance(node, ast.FunctionDef) and node.name == "validate_forward_release":
                    Rewrite().visit(node)
            module.body = [node for node in module.body
                if not (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in helpers)
                and not (isinstance(node, ast.Import) and [part.name for part in node.names]
                         in (["ast"], ["subprocess"], ["io"], ["re"], ["shlex"], ["tarfile"]))
                and not (isinstance(node, ast.ImportFrom) and node.module == "functools")
                and not (isinstance(node, ast.ImportFrom) and node.level == 1
                         and node.module == "runtime_paths"
                         and [(part.name, part.asname) for part in node.names]
                         == [("source_root", None), ("verified_git", None)])
                and not (isinstance(node, ast.Assign) and any(isinstance(name, ast.Name)
                         and name.id in {"_SUCCESSOR_SEND_FILES", "_SUCCESSOR_READER_SHA256", "_SUCCESSOR_OVERVIEW_TRANSITIONS", "_SUCCESSOR_ACCOUNT_READ_TRANSITIONS", "_SUCCESSOR_METRIC_READ_TRANSITIONS",
                                         "_SUCCESSOR_ACCOUNT_STATUS_CRITICAL_ADDITIONS", "_SUCCESSOR_ACCOUNT_STATUS_NEW_MODULES"}
                         for name in node.targets))
                and not (isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
                         and node.target.id == "_SUCCESSOR_ACCOUNT_STATUS_TRANSITIONS")]
        return ast.dump(old, include_attributes=False) == ast.dump(new, include_attributes=False)
    except (SyntaxError, ValueError, TypeError):
        return False


def _successor_source(previous: Mapping[str, Any], current: Mapping[str, Any], *, live: bool) -> None:
    """Verify exact approved source changes while retaining physical-send guards."""
    project = str(PROJECT_ROOT.resolve())
    old_head, new_head = str(previous["git"]["head"]), str(current["git"]["head"])
    _successor_git(project, "merge-base", "--is-ancestor", old_head, new_head)
    old_critical, new_critical = previous["critical_files"], current["critical_files"]
    _require(set(old_critical) == set(new_critical)
             or (bool(_SUCCESSOR_ACCOUNT_STATUS_TRANSITIONS)
                 and set(new_critical) - set(old_critical) == _SUCCESSOR_ACCOUNT_STATUS_CRITICAL_ADDITIONS
                 and not set(old_critical) - set(new_critical)),
             "forward_build_invalid", "Successor changed the sealed critical inventory")
    old_sources, new_sources = _successor_archive(previous), _successor_archive(current, live=live)
    def source_at(sources: Mapping[str, bytes | None], head: str, relative: str) -> bytes | None:
        if relative in sources:
            return sources[relative]
        if relative in _SUCCESSOR_ACCOUNT_STATUS_NEW_MODULES and not _successor_git(project, "ls-tree", head, "--", relative):
            return None
        return _successor_git(project, "show", f"{head}:{relative}")
    guarded = set(old_critical) | set(new_critical) | set(_SUCCESSOR_SEND_FILES) | set(_SUCCESSOR_OVERVIEW_TRANSITIONS)
    sources = {relative: (source_at(old_sources, old_head, relative), source_at(new_sources, new_head, relative))
               for relative in guarded}
    changes = {relative: (hashlib.sha256(before).hexdigest() if isinstance(before, bytes) else None,
                          hashlib.sha256(after).hexdigest() if isinstance(after, bytes) else None)
               for relative, (before, after) in sources.items()}
    account_status_upgrade = bool(_SUCCESSOR_ACCOUNT_STATUS_TRANSITIONS) and (
        set(new_critical) - set(old_critical) == _SUCCESSOR_ACCOUNT_STATUS_CRITICAL_ADDITIONS
        and not set(old_critical) - set(new_critical)
        and set(_SUCCESSOR_ACCOUNT_STATUS_TRANSITIONS) == (
            set(_SUCCESSOR_ACCOUNT_STATUS_CRITICAL_ADDITIONS) | {"src/dcar_eval/v8/api.py", "scripts/seal_r0_receipts.py"})
        and all(changes.get(relative) == expected for relative, expected in _SUCCESSOR_ACCOUNT_STATUS_TRANSITIONS.items())
    )
    _require(set(old_critical) == set(new_critical) or account_status_upgrade,
             "forward_build_invalid", "Successor changed the sealed critical inventory")
    for relative, (before, after) in sources.items():
        _require(isinstance(after, bytes) and (isinstance(before, bytes)
                 or (account_status_upgrade and relative in _SUCCESSOR_ACCOUNT_STATUS_NEW_MODULES and before is None)),
                 "forward_build_invalid", "Successor deleted required runtime source")
        assert isinstance(after, bytes)
        if relative in old_critical:
            _require(changes[relative][0] == old_critical[relative],
                     "forward_build_invalid", "Sealed source file does not match its Git blob")
        if relative in new_critical:
            _require(changes[relative][1] == new_critical[relative],
                     "forward_build_invalid", "Sealed source file does not match its Git blob")
        approved_overview = _SUCCESSOR_OVERVIEW_TRANSITIONS.get(relative)
        change = changes[relative]
        _require(before == after or (account_status_upgrade and relative in _SUCCESSOR_ACCOUNT_STATUS_TRANSITIONS)
                 or (not account_status_upgrade and (
                     (approved_overview is not None and approved_overview == change)
                     or (relative == "src/dcar_eval/v8/api.py" and change in _SUCCESSOR_ACCOUNT_READ_TRANSITIONS)
                     or (relative == "src/dcar_eval/v8/source_routing.py" and change in _SUCCESSOR_METRIC_READ_TRANSITIONS)))
                 or (relative == "src/dcar_eval/v8/forward_recovery.py" and isinstance(before, bytes)
                     and _successor_reader_compatible(before, after)),
                 "forward_build_invalid", "Successor changed release, budget, capture or transport safety code")
        if live:
            path = source_root(PROJECT_ROOT) / relative
            _require(not path.is_symlink() and path.is_file()
                     and path.resolve().is_relative_to(source_root(PROJECT_ROOT)) and path.read_bytes() == after,
                     "forward_build_invalid", "Loaded safety code differs from the tested successor")


def _released_runtime_identity(connection: sqlite3.Connection, binding: Mapping[str, Any]) -> dict[str, Any]:
    """Resolve an existing RELEASE through an exact, tested code-only successor chain.

    The returned identity remains the original release authority. The actual
    loaded build is independently verified at every hop; no receipt is rewritten.
    """
    loaded_id = os.environ.get("DCAR_LOADED_BUILD_ID", "")
    _require(connection.execute("PRAGMA user_version").fetchone()[0] == 19,
             "forward_build_invalid", "Forward release continuation cannot cross a database schema change")
    if loaded_id == "sha256:" + str(binding["build_receipt_sha256"]):
        return _runtime_identity(connection, binding)
    _require(loaded_id.startswith("sha256:") and len(loaded_id) == 71,
             "forward_build_invalid", "Loaded successor build identity is missing")
    current_sha = loaded_id[7:]
    current = _private_receipt(Path(os.environ.get("DCAR_LOADED_BUILD_RECEIPT", "")), current_sha,
                               "sealed-build-receipt-v1")
    runtime = _successor_build(current, connection, live=True)
    seen = {current_sha}
    for hop in range(16):
        lineage = current.get("postmigration_lineage", {})
        reference = lineage.get("previous_build_receipt", {}) if isinstance(lineage, dict) else {}
        previous_sha = str(reference.get("sha256", ""))
        _require(previous_sha not in seen and len(previous_sha) == 64,
                 "forward_build_invalid", "Successor build chain is absent or cyclic")
        previous = _successor_receipt(reference, "sealed-build-receipt-v1")
        _successor_build(previous, connection)
        previous_lineage = previous.get("postmigration_lineage", {})
        _require(isinstance(previous_lineage, dict)
                 and all(isinstance(lineage.get(key), dict) and lineage[key] == previous_lineage.get(key)
                     for key in ("install_receipt", "migration_receipt"))
                 and current["schema_contract"] == previous["schema_contract"]
                 and current.get("report_contract") == previous.get("report_contract"),
                 "forward_build_invalid", "Successor changed install, schema or report lineage")
        _successor_source(previous, current, live=hop == 0)
        if previous_sha == binding["build_receipt_sha256"]:
            _require(previous["runtime_root_receipt"]["sha256"] == binding["runtime_root_receipt_sha256"],
                     "forward_build_invalid", "Successor chain reached another release runtime")
            database = runtime["formal_database"]
            return {"build_receipt_sha256": previous_sha,
                    "runtime_root_receipt_sha256": binding["runtime_root_receipt_sha256"],
                    "database_path": database["path"], "database_device": database["device"],
                    "database_inode": database["inode"]}
        seen.add(previous_sha)
        current = previous
    raise ProfileControlError("forward_build_invalid", "Successor chain exceeds its bounded depth")


def _capacity(connection: sqlite3.Connection) -> dict[str, Any]:
    from .capture import RAW_ROOT

    database = Path(str(connection.execute("PRAGMA database_list").fetchone()[2])).resolve(strict=True)
    readings = []
    for path in (database.parent, RAW_ROOT):
        _require(not path.is_symlink() and path.is_dir(), "forward_capacity_invalid", "Runtime data directory is unavailable")
        usage = shutil.disk_usage(path)
        _require(usage.free >= MIN_FREE_BYTES and usage.used / usage.total < 0.9,
                 "forward_capacity_blocked", "Runtime disk needs at least 10 GiB free and less than 90% utilization")
        readings.append({"path": str(path.resolve()), "device": path.stat().st_dev,
                         "total_bytes": usage.total, "used_bytes": usage.used, "free_bytes": usage.free})
    return {"minimum_free_bytes": MIN_FREE_BYTES, "maximum_used_ratio": 0.9, "readings": readings,
            "archive_runway_qualified": False}


def _route() -> dict[str, Any]:
    from .providers import _freeze_tikhub_transport

    transport = _freeze_tikhub_transport()
    _require(isinstance(transport, dict) and isinstance(transport.get("manifest"), dict),
             "forward_route_invalid", "Production transport configuration is unavailable")
    assert transport is not None
    return dict(transport["manifest"])


def _verified_unbilled_retry(connection: sqlite3.Connection, member: Mapping[str, Any]) -> dict[str, Any]:
    """Recognize the observed provider error, never infer billing from HTTP 400."""
    from .capture import _read_verified_raw_response

    raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (member.get("raw_response_id"),)).fetchone()
    usage = connection.execute("SELECT * FROM provider_usage WHERE id=?", (member.get("usage_id"),)).fetchone()
    attempt = connection.execute("SELECT * FROM fetch_attempts WHERE id=?", (member.get("fetch_attempt_id"),)).fetchone()
    _require(raw is not None and usage is not None and attempt is not None
             and member.get("state") == "failed" and member.get("accounting_terminal") is True
             and member.get("billing_settled") is True and member.get("response_complete") is True
             and member.get("amount_microusd") == 0
             and raw["http_status"] == attempt["http_status"] == 400
             and raw["fetch_attempt_id"] == attempt["id"] and attempt["response_finished_at"] is not None
             and attempt["error_code"] == "provider_retry_requested"
             and attempt["billed"] == usage["billed_requests"] == 0
             and usage["request_attempts"] == 1 and usage["currency"] == "USD"
             and provider_budget.micro_usd(usage["amount"]) == provider_budget.micro_usd(attempt["amount"]) == 0,
             "forward_retry_unverified", "Retry member does not have complete, explicitly zero-charge accounting")
    assert raw is not None and usage is not None
    metadata = json.loads(usage["details_json"])
    _, payload = _read_verified_raw_response(raw)
    detail = payload.get("detail") if isinstance(payload, dict) else None
    _require(isinstance(detail, dict) and type(detail.get("code")) is int and detail["code"] == 400
             and metadata.get("state") == "failed" and metadata.get("error_code") == "provider_retry_requested",
             "forward_retry_unverified", "Retry member lacks the verified provider error envelope")
    assert isinstance(detail, dict)
    messages = [detail.get(key) for key in ("message", "message_zh")]
    explicit = any(isinstance(value, str)
                   and any(retry in value.lower() for retry in ("please retry", "请重试"))
                   and any(unbilled in value.lower() for unbilled in ("won't be charged", "will not be charged", "不会被扣费"))
                   for value in messages)
    _require(explicit, "forward_retry_unverified", "The original provider response must explicitly request retry without charge")
    return {"member_receipt_id": member["member_receipt_id"], "raw_response_id": raw["id"],
            "raw_sha256": raw["sha256"], "usage_id": usage["id"], "amount_microusd": 0}


def _control_admission(connection: sqlite3.Connection, payload: Mapping[str, Any],
                       owners: Mapping[str, Any], members: list[dict[str, Any]]) -> dict[str, Any]:
    _require(len(members) == 20 and [member.get("rank") for member in members] == list(range(1, 21))
             and len(owners["members"]) == 20
             and all(member.get("effective_starts") == 1 and member.get("response_complete") is True
                     and member.get("accounting_terminal") is True and member.get("state") != "billing_unknown"
                     for member in members),
             "forward_control_not_passed", "Forward recovery requires twenty complete, accounted transport responses")
    unusable = [member for member, owner in zip(members, owners["members"], strict=True)
                if member.get("state") != "succeeded" or owner.get("materialization_run_id") is None]
    if not unusable:
        _require(payload.get("status") == "passed" and payload.get("route_passed") is True
                 and payload.get("usable_page_count") == 20,
                 "forward_control_not_passed", "Complete control differs from its original verdict")
        return {"admission": "complete_transport", "retry_members": []}
    _require(len(unusable) == 1 and payload.get("status") == "failed" and payload.get("route_passed") is False
             and payload.get("usable_page_count") == 19 and payload.get("selected_route") is None,
             "forward_control_not_passed", "Only one explicitly unbilled provider retry is admissible")
    retry = _verified_unbilled_retry(connection, unusable[0])
    return {"admission": "complete_transport_with_one_unbilled_retry", "retry_members": [retry]}


def _passed_control(connection: sqlite3.Connection, receipt_id: int, binding: Mapping[str, Any], *, at: str) -> dict[str, Any]:
    from .transport_accounting import read_primary_member_accounting
    from .transport_owner_evidence import read_primary_campaign_owners
    from .transport_receipts import read_transport_receipt

    verdict = read_transport_receipt(connection, receipt_id)
    payload = verdict["payload"]
    campaign = read_transport_receipt(connection, payload["campaign_receipt_id"])
    header = campaign["payload"]
    historical_hold = payload.get("hold_binding", {})
    _require(verdict["kind"] == "route_verdict" and payload.get("contract_version") == "transport-primary-route-verdict-v1"
             and header.get("arm") in {"control_io", "control_legacy"}
             and payload.get("campaign_receipt_sha256") == campaign["self_sha256"]
             and historical_hold == header.get("hold_binding")
             and all(historical_hold.get(key) == binding.get(key) for key in (
                 "drain_id", "start_event_id", "start_event_hash", "activation_id", "roster_snapshot_id", "roster_snapshot_hash"))
             and parse_time(verdict["recorded_at"]) <= parse_time(at)
             and payload.get("operation_qualified") is False
             and payload.get("ordinary_paid_authorized") is False
             and all(payload.get(key) == 20 for key in ("sample_limit", "effective_starts", "response_complete_count"))
             and all(payload.get(key) == 0 for key in ("transport_uncertain_count", "original_billing_unknown_count")),
             "forward_control_not_passed", "Forward release requires a real, complete twenty-request control on this HOLD")
    owners = read_primary_campaign_owners(connection, campaign["receipt_id"], at=verdict["recorded_at"])
    members = [read_primary_member_accounting(connection, item["member_receipt_id"], at=verdict["recorded_at"])
               for item in owners["members"]]
    terminal = read_transport_receipt(connection, owners["terminal_receipt_id"])
    _require(_digest(owners) == payload.get("owner_evidence_sha256")
             and _digest(members) == payload.get("member_evidence_sha256")
             and terminal["receipt_id"] == payload.get("campaign_terminal_id")
             and terminal["self_sha256"] == payload.get("campaign_terminal_sha256")
             and header["request_transport"]["manifest"] == _route(),
             "forward_control_evidence_changed", "Control raw/owner/accounting evidence or the selected production route changed")
    admission = _control_admission(connection, payload, owners, members)
    if payload.get("route_passed") is True:
        _require(payload.get("selected_route") == header["request_transport"]["manifest"],
                 "forward_control_evidence_changed", "The passed control selected a different route")
    return {"receipt_id": receipt_id, "receipt_sha256": verdict["self_sha256"],
            "campaign_receipt_id": campaign["receipt_id"], "campaign_receipt_sha256": campaign["self_sha256"],
            "arm": header["arm"], "selected_route": header["request_transport"]["manifest"],
            "source_verdict_status": payload["status"], "source_route_passed": payload["route_passed"], **admission}


def _live_guards(connection: sqlite3.Connection, *, at: str, check_capacity: bool = True,
                 release_admission: bool = False) -> dict[str, Any]:
    provider_budget.require_storage_ready(connection)
    circuit = provider_budget.circuit_state(connection)
    _require(not circuit or not circuit.get("open"), "provider_blocked", "A live provider fault still blocks future dispatch")
    budget = provider_budget.budget_summary(connection, at=at)
    _require(budget["total_microusd"] < budget["automatic_limit_microusd"]
             and (not release_admission or all(
                 budget["buckets_microusd"][bucket] < budget["bucket_limits_microusd"][bucket]
                 for bucket in ("discovery", "metrics"))),
             "forward_budget_blocked", "The existing automatic daily budget has no remaining capacity")
    return {"capacity": _capacity(connection) if check_capacity else None, "budget": budget}


def validate_forward_release(connection: sqlite3.Connection, *, active: Mapping[str, Any],
                             release_control: Mapping[str, Any], at: str, check_runtime: bool = True) -> None:
    """Validate frozen release evidence and live guards; expiry is release-only."""
    control = release_control
    binding = control.get("hold_binding", {})
    released = _hold_event(connection, drain_id=str(control.get("drain_id", "")), event_type="release")
    _require(released is not None and released["payload"].get("control") == dict(control)
             and parse_time(str(released["created_at"])) <= parse_time(at),
             "forward_release_invalid", "Forward release evidence is absent or changed")
    start = _hold_event(connection, drain_id=str(control.get("drain_id", "")), event_type="start")
    _require(start is not None and forward_release_matches(control, active=active, start=start,
                                                          released_at=str(control.get("released_at", ""))),
             "forward_release_invalid", "Forward release lost its same-HOLD activation binding")
    if check_runtime:
        _scope(str(control["scope_start"]), at)
    expected = {kind: binding[field] for kind, field in {
        "build": "build_receipt_sha256", "runtime": "runtime_root_receipt_sha256", "config": "config_receipt_sha256",
        "price": "price_receipt_sha256", "budget": "budget_receipt_sha256"}.items()}
    receipts = _current_hold_prerequisites(connection, drain_id=str(control["drain_id"]), generation=int(binding["generation"]),
                                         active=active, expected=expected, at=str(control["released_at"]))
    _require(receipts == binding["prerequisites"], "forward_prerequisite_changed", "Release prerequisites changed after release")
    if check_runtime:
        _require(_released_runtime_identity(connection, binding) == control.get("runtime_identity"),
                 "forward_runtime_invalid", "Runtime identity changed after release")
    from .transport_receipts import read_transport_receipt

    route = control["route_control"]
    verdict = read_transport_receipt(connection, route["receipt_id"])
    campaign = read_transport_receipt(connection, route["campaign_receipt_id"])
    _require(verdict["self_sha256"] == route["receipt_sha256"]
             and campaign["self_sha256"] == route["campaign_receipt_sha256"] == verdict["payload"].get("campaign_receipt_sha256")
             and verdict["payload"].get("campaign_receipt_id") == route["campaign_receipt_id"]
             and verdict["payload"].get("status") == route["source_verdict_status"]
             and verdict["payload"].get("route_passed") == route["source_route_passed"]
             and campaign["payload"]["request_transport"]["manifest"] == route["selected_route"]
             and (not check_runtime or _route() == route["selected_route"]),
             "forward_route_invalid", "Selected control or production route changed after release")
    _live_guards(connection, at=at, check_capacity=check_runtime)


def _record_forward_release_command(*, command_claim: dict[str, Any], drain_id: str, route_verdict_receipt_id: int,
                                scope_start: str, actor: str, scheduler: BaseScheduler | None,
                                db_path: Path, mirror_root: Path | None) -> dict[str, Any]:
    from .transport_hold_binding import read_current_diagnostic_hold, _latest_prerequisite_payloads

    _require(isinstance(scheduler, BaseScheduler) and scheduler.state == STATE_PAUSED,
             "forward_scheduler_active", "Forward recovery must execute in the paused Writer")
    _require(mirror_root is not None and bool(actor.strip()), "forward_owner_invalid", "Forward recovery needs an actor and immutable mirror")
    at = now_utc()
    with connect(db_path) as connection, transaction(connection):
        require_current_process_writer_lock(connection)
        owner = connection.execute(
            "SELECT r.details_json FROM scheduler_runs r JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id "
            "WHERE r.id=? AND a.id=? AND r.job_id='current_activation_hold_command' AND r.status='running' AND a.status='running'",
            (command_claim["run_id"], command_claim["attempt_id"]),
        ).fetchone()
        _require(owner is not None and json.loads(owner["details_json"]) == command_claim["details"],
                 "forward_owner_invalid", "Forward recovery lost its durable Writer command claim")
        _scope(scope_start, at)
        previous = _hold_event(connection, drain_id=drain_id, event_type="release")
        if previous is not None:
            from .profile_activations import activation_at

            control = previous["payload"].get("control", {})
            _require(is_forward_release(control) and control.get("scope_start") == scope_start
                     and control.get("actor") == actor
                     and control.get("route_control", {}).get("receipt_id") == route_verdict_receipt_id
                     and paid_drain.dispatch_state(connection, at=at).permit_event_id == previous["event_id"],
                     "forward_release_conflict", "An existing release cannot change its scope or control evidence")
            active = activation_at(connection, at)
            _require(active is not None, "forward_release_invalid", "No active acquisition profile")
            assert active is not None
            validate_forward_release(connection, active=active, release_control=control, at=at)
            return {"release": previous, "scope_start": scope_start, "already_released": True,
                    "operation_qualified": False, "scheduler_resumed": False}
        binding = read_current_diagnostic_hold(connection, drain_id=drain_id, at=at)
        runtime = _runtime_identity(connection, binding)
        config = _latest_prerequisite_payloads(connection, drain_id=drain_id, generation=binding["generation"])["config"]["payload"]
        _require(config["evidence"].get("selected_route") == _route(),
                 "forward_config_invalid", "The final config prerequisite must bind the selected production route")
        route = _passed_control(connection, route_verdict_receipt_id, binding, at=at)
        from .transport_accounting import settle_closed_hold_unknowns

        assert scheduler is not None and mirror_root is not None
        accounting = settle_closed_hold_unknowns(connection, drain_id=drain_id, scheduler=scheduler,
                                                 at=at, mirror_root=mirror_root)
        # Quiescence is proved by the original START's exact frozen dispatches
        # and every subsequent owner/usage/attempt/dispatch/raw set. A legacy
        # NULL response timestamp or pre-policy "reserved" label alone is not
        # live send authority. Recounting those globally would make unrelated
        # historical records permanently block this prospective release.
        # Strict seal still rejects every unfinished owner, unexplained tail,
        # real in-flight reservation/send and unaccounted diagnostic request.
        tail = paid_drain.verify_profile_drain_sealable(connection, drain_id, now=at)
        circuit = provider_budget.circuit_state(connection)
        classification = None
        if circuit and circuit.get("open") and circuit.get("fault_class") == "legacy_unclassified":
            classification = provider_budget.classify_legacy_transport_fault(
                connection, expected_receipt_id=int(circuit["receipt_id"]), actor=actor, at=at,
            )
        live = _live_guards(connection, at=at, release_admission=True)
        control = {
            "contract_version": CONTRACT_VERSION, "control_purpose": PURPOSE, "drain_id": drain_id,
            "activation_id": binding["activation_id"], "roster_snapshot_id": binding["roster_snapshot_id"],
            "roster_snapshot_hash": binding["roster_snapshot_hash"], "hold_binding": binding,
            "scope_start": scope_start, "released_at": at, "actor": actor,
            "route_control": route, "runtime_identity": runtime, "release_checks": live,
            "tail_sha256": _digest(tail), "legacy_fault_classification": classification,
            "diagnostic_accounting": accounting,
            "operation_qualified": False, "historical_backfill_authorized": False,
        }
        sealed = paid_drain.seal_profile_drain_in_transaction(
            connection, drain_id, now=at, force_strict=True, control={**control, "control_purpose": "forward_only_seal"},
        )
        released = paid_drain.release_profile_drain_in_transaction(connection, drain_id, now=at, control=control)
        assert mirror_root is not None
        for event_type in ("sealed", "release"):
            event = _hold_event(connection, drain_id=drain_id, event_type=event_type)
            assert event is not None
            _write_native_event_mirror(event, mirror_root)
        return {"sealed": sealed.as_dict(), "release": released.as_dict(), "scope_start": scope_start,
                "operation_qualified": False, "scheduler_resumed": False}


def run_forward_release_command(*, command_claim: dict[str, Any], drain_id: str, route_verdict_receipt_id: int,
                                scope_start: str, actor: str, scheduler: BaseScheduler | None,
                                db_path: Path, mirror_root: Path | None) -> dict[str, Any]:
    result = _record_forward_release_command(
        command_claim=command_claim, drain_id=drain_id, route_verdict_receipt_id=route_verdict_receipt_id,
        scope_start=scope_start, actor=actor, scheduler=scheduler, db_path=db_path, mirror_root=mirror_root,
    )
    # The transaction has committed before any scheduled job can start. A
    # failed resume leaves its durable release intact for a verified retry,
    # and is reported as a failed command rather than a false running state.
    assert scheduler is not None
    try:
        if scheduler.get_job("pipeline_reconcile") is not None:
            scheduler.modify_job("pipeline_reconcile", next_run_time=parse_time(now_utc()))
        scheduler.resume()
        _require(scheduler.state == STATE_RUNNING, "forward_scheduler_resume_failed", "Writer scheduler did not resume")
    except Exception as exc:
        raise ProfileControlError("forward_scheduler_resume_failed", "Release committed, but the Writer scheduler failed to resume") from exc
    return {**result, "scheduler_resumed": True}
