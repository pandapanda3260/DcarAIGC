"""A bounded account taxonomy successor of the exact installed schema23 build.

The parent verifier executes from its complete immutable source. Its migration,
index installation, operation authority and paid evidence remain byte-for-byte
unchanged. This proof authorizes only the separately checked code presentation.
"""

from __future__ import annotations

import ast
from contextlib import contextmanager
from datetime import datetime
import importlib
from pathlib import Path
import sqlite3
import sys
import threading
import types
from typing import Any, Mapping

from .account_classification_release import (
    digest,
    object_at,
    payload_at,
    raw,
    records,
    reference,
)
from .account_intake_release import inventory

CONTRACT = "account-taxonomy-code-successor-v1"
FIELD = "account_taxonomy_code_successor"
CHECK_CONTRACT = "account-taxonomy-release-check-v1"
REVIEW_CONTRACT = "account-taxonomy-reviewed-business-delta-v1"
SCOPE = "account_group_and_business_direction_only"
MODULE = "src/dcar_eval/v8/account_taxonomy_code_successor.py"
ENTRY = "src/dcar_eval/v8/four_platform_flow_release.py"
SCRIPT = "scripts/prepare_account_taxonomy_release.py"
CHECKS = frozenset({"taxonomy_api", "taxonomy_reports", "taxonomy_release"})
TEST_TARGETS = {
    "taxonomy_api": (
        "tests.test_v8_account_classification",
        "tests.test_v8_operations",
        "tests.test_v8_api.V8ReviewAndTaxonomyApiTest.test_archived_report_json_and_direct_downloads_project_new_account_classification",
        "tests.test_v8_api.V8ReviewAndTaxonomyApiTest.test_custom_task_generates_revision_and_downloads_run_scoped_files",
        "tests.test_v8_api.V8ReviewAndTaxonomyApiTest.test_single_report_downloads_isolate_artifacts_and_preserve_integrity_checks",
        "tests.test_v8_api.V8ReviewAndTaxonomyApiTest.test_content_search_total_matches_rows_for_every_count_dependency",
        "tests.test_v8_api.V8ApiTest.test_content_filters_reject_obsolete_account_type",
        "tests.test_v8_api.ApiStartupSafetyTest",
    ),
    "taxonomy_reports": (
        "tests.test_v8_report_inputs",
        "tests.test_v8_report_export",
        "tests.test_v23_report_dependencies",
        "tests.test_report_metric_validity",
    ),
    "taxonomy_release": (
        "tests.test_account_taxonomy_code_successor",
        "tests.test_v23_runtime_evidence_context",
        "tests.test_v23_capture_authority_preflight",
    ),
}
BUSINESS_FILES = frozenset(
    "src/dcar_eval/v8/" + name + ".py"
    for name in (
        "api",
        "operations",
        "reports",
        "contracts",
        "report_inputs",
        "report_export",
        "pipeline_cutover",
    )
)
CONTRACT_FILES = frozenset(
    {"config/report_contract_v8_9.json", "config/report_contract_v8_10.json"}
)
TEST_FILES = frozenset(
    {
        "tests/test_account_taxonomy_code_successor.py",
        "tests/test_v8_account_classification.py",
        "tests/test_v8_api.py",
        "tests/test_v8_operations.py",
        "tests/test_v8_report_inputs.py",
        "tests/test_v8_report_export.py",
        "tests/test_v8_reports.py",
        "tests/test_v23_report_dependencies.py",
    }
)
ALLOWED_FILES = BUSINESS_FILES | CONTRACT_FILES | TEST_FILES | {MODULE, ENTRY, SCRIPT}
REQUIRED_CHANGES = BUSINESS_FILES | CONTRACT_FILES | {MODULE, ENTRY, SCRIPT}
_PARENT_IMPORT_LOCK = threading.RLock()


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError("Account taxonomy successor: " + reason)


def _dispatch_ast(body: bytes, *, child: bool) -> str:
    """Keep S5's native reuse first; permit only its following exact dispatch."""
    module = ast.parse(body)
    function = next(
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name == "verify_inheritance"
    )
    dispatch = ast.parse("""if build.get("account_taxonomy_code_successor") is not None:
    from .account_taxonomy_code_successor import verify_inheritance as verify_taxonomy
    return verify_taxonomy(build=build, build_ref=build_ref, install_path=install_path,
        database=database, source=source, at=at, connection=connection)
""").body[0]
    prefix = ast.parse("""from .runtime_evidence_context import reuse_inheritance
reused = reuse_inheritance(connection=connection, build=build, build_ref=build_ref,
    install_path=install_path, database=database, source=source, at=at)
if reused is not None:
    return reused
""").body
    require(
        [ast.dump(node, include_attributes=False) for node in function.body[:3]]
        == [ast.dump(node, include_attributes=False) for node in prefix],
        "S5 native inheritance reuse prefix changed or bypassed",
    )
    if child:
        require(
            len(function.body) > 3
            and ast.dump(function.body[3], include_attributes=False)
            == ast.dump(dispatch, include_attributes=False),
            "flow entry changed beyond the exact taxonomy dispatch",
        )
        function.body.pop(3)
    return ast.dump(module, include_attributes=False)


def source_changes(
    parent_tree: Mapping[str, Any], tree: Mapping[str, Any]
) -> dict[str, Any]:
    left, right = records(parent_tree), records(tree)
    changed = {
        name for name in left.keys() | right.keys() if left.get(name) != right.get(name)
    }
    require(
        REQUIRED_CHANGES <= changed and changed <= ALLOWED_FILES,
        "candidate differs outside the bounded taxonomy files or omits required changes",
    )
    for name in changed:
        require(
            name in right
            and (name not in left or left[name]["mode"] == right[name]["mode"]),
            "file deletion or permission change is not allowed",
        )
    require(
        _dispatch_ast(
            raw(Path(parent_tree["source_root"]) / ENTRY, private=False), child=False
        )
        == _dispatch_ast(
            raw(Path(tree["source_root"]) / ENTRY, private=False), child=True
        ),
        "existing flow verifier changed",
    )
    return {
        name: {
            "before_sha256": left[name]["sha256"] if name in left else None,
            "after_sha256": right[name]["sha256"],
        }
        for name in sorted(changed)
    }


@contextmanager
def _parent_verifier(source: Path, identity: str):
    # S5's runtime context registers a process-wide audit hook on import.
    # Retain one isolated module per immutable build, not one per paid A/B.
    # parent_context still rechecks every source byte around each cold proof.
    require(
        len(identity) == 64 and all(c in "0123456789abcdef" for c in identity),
        "parent module requires the complete build SHA",
    )
    name = "_dcar_taxonomy_parent_" + identity
    package_path = [str(source / "src/dcar_eval/v8")]
    with _PARENT_IMPORT_LOCK:
        if name not in sys.modules:
            package = types.ModuleType(name)
            package.__path__ = package_path
            package.__package__ = name
            sys.modules[name] = package
        require(
            list(getattr(sys.modules[name], "__path__", ())) == package_path,
            "cached parent verifier source differs",
        )
        verifier = importlib.import_module(name + ".four_platform_flow_release")
        require(
            Path(verifier.__file__).resolve(strict=True) == source / ENTRY,
            "cached parent verifier entry differs",
        )
    yield verifier


def parent_context(
    parent_ref: Mapping[str, Any],
    *,
    install_path: Path,
    database: Path,
    at: str | None = None,
    connection=None,
) -> tuple[dict, dict]:
    parent = payload_at(parent_ref, "sealed-build-receipt-v1")
    require(
        parent.get("status") == "succeeded"
        and FIELD not in parent
        and parent.get("schema_contract") == {"code_schema": 23, "formal_schema": 23}
        and parent.get("four_platform_flow_successor"),
        "installed schema23 parent required",
    )
    source = Path(parent["source_root"])
    tree = object_at(parent["account_cleanup_generation"]["source_tree"])
    require(
        inventory(source) == tree
        and parent["critical_files"].get(ENTRY) == records(tree)[ENTRY]["sha256"],
        "complete parent source or verifier changed",
    )
    with _parent_verifier(source, parent_ref["sha256"]) as verifier:
        inherited = verifier.verify_inheritance(
            build=parent,
            build_ref=parent_ref,
            install_path=install_path,
            database=database,
            source=source,
            at=at,
            connection=connection,
        )
    require(
        inventory(source) == tree
        and inherited.get("four_platform_flow_proof", {}).get("loaded_build")
        == dict(parent_ref),
        "parent flow evidence is incomplete or changed",
    )
    return parent, inherited


def test_command(name: str, parent: Mapping[str, Any]) -> list[str]:
    require(name in CHECKS, "unknown focused check")
    return [
        str(Path(parent["project_root"]) / ".venv/bin/python"),
        "-B",
        "-m",
        "unittest",
        *TEST_TARGETS[name],
    ]


def verify_review(
    review_ref: Mapping[str, Any],
    *,
    parent_ref: Mapping[str, Any],
    changes: Mapping[str, Any],
) -> None:
    review = object_at(review_ref)
    require(
        review.get("contract") == REVIEW_CONTRACT
        and review.get("status") == "reviewed"
        and review.get("scope") == SCOPE
        and review.get("parent_build") == parent_ref
        and review.get("changes")
        == {name: changes[name] for name in sorted(BUSINESS_FILES | CONTRACT_FILES)}
        and isinstance(review.get("reviewed_by"), list)
        and bool(review["reviewed_by"])
        and all(
            isinstance(name, str) and name.strip() for name in review["reviewed_by"]
        ),
        "business delta differs from the independently reviewed before/after hashes",
    )
    require(
        datetime.fromisoformat(review["reviewed_at"].replace("Z", "+00:00")).utcoffset()
        is not None,
        "review timestamp requires timezone",
    )


def verify_checks(
    checks: Mapping[str, Mapping[str, Any]],
    *,
    tree_ref: Mapping[str, Any],
    changes: Mapping[str, Any],
    parent_ref: Mapping[str, Any],
    review_ref: Mapping[str, Any],
) -> None:
    verify_review(review_ref, parent_ref=parent_ref, changes=changes)
    parent = payload_at(parent_ref, "sealed-build-receipt-v1")
    require(set(checks) == CHECKS, "all focused taxonomy checks are required")
    for name, ref in checks.items():
        check = object_at(ref)
        require(
            check.get("contract") == CHECK_CONTRACT
            and check.get("name") == name
            and check.get("status") == "passed"
            and check.get("exit_code") == 0
            and check.get("source_tree") == tree_ref
            and check.get("changes") == changes
            and check.get("review_manifest") == review_ref
            and check.get("parent_build") == parent_ref
            and check.get("command") == test_command(name, parent)
            and isinstance(check.get("test_count"), int)
            and check["test_count"] > 0
            and check.get("skipped_tests") == 0
            and reference(Path(check["output"]["path"])) == check["output"],
            "check does not bind the final source and successful output",
        )


def verify_inheritance(
    *,
    build: Mapping[str, Any],
    build_ref: Mapping[str, Any],
    install_path: Path,
    database: Path,
    source: Path,
    at: str | None = None,
    connection=None,
) -> dict[str, Any]:
    plan = build.get(FIELD, {})
    require(
        plan.get("contract") == CONTRACT
        and dict(build) == payload_at(build_ref, "sealed-build-receipt-v1"),
        "child build receipt differs",
    )
    parent, inherited = parent_context(
        plan["parent_build"],
        install_path=install_path,
        database=database,
        at=at,
        connection=connection,
    )
    allowed = {
        "source_root",
        "git",
        "critical_files",
        "code_successor_plan",
        "account_cleanup_generation",
        FIELD,
        "created_at",
        "validation_scope",
    }
    require(
        {k: v for k, v in build.items() if k not in allowed}
        == {k: v for k, v in parent.items() if k not in allowed}
        and {
            k: v
            for k, v in build["account_cleanup_generation"].items()
            if k != "source_tree"
        }
        == {
            k: v
            for k, v in parent["account_cleanup_generation"].items()
            if k != "source_tree"
        },
        "parent migration, index, controls or paid authority changed",
    )
    tree_ref = plan["source_tree"]
    tree = object_at(tree_ref)
    parent_source, data = Path(parent["source_root"]), Path(parent["project_root"])
    require(
        source.resolve(strict=True) == source
        and source.is_absolute()
        and all(
            source != other
            and not source.is_relative_to(other)
            and not other.is_relative_to(source)
            for other in (parent_source, data)
        )
        and build["source_root"] == str(source)
        and inventory(source) == tree
        and build["git"] == tree["git"]
        and build["account_cleanup_generation"]["source_tree"] == tree_ref,
        "child source is not independently sealed",
    )
    changes = source_changes(
        object_at(parent["account_cleanup_generation"]["source_tree"]), tree
    )
    require(
        changes == plan.get("changes")
        and build["critical_files"]
        == {
            name: row["sha256"]
            for name, row in records(tree).items()
            if name.startswith(("src/", "config/")) and name.endswith((".py", ".json"))
        },
        "child source delta changed",
    )
    require(
        object_at(build["code_successor_plan"])
        == {
            "contract": "account-cleanup-source-plan-v1",
            "transition": "account-cleanup-0907-v1",
            "project_root": str(data),
            "source_root": str(source),
            "git": tree["git"],
            "source_tree": tree_ref,
        },
        "child source plan differs",
    )
    require(
        plan.get("scope") == SCOPE
        and plan.get("schema_migration_repeated") is False
        and plan.get("database_writes") == 0
        and plan.get("paid_gates_reopened") is False
        and plan.get("publisher_authorized") is False
        and plan.get("remote_database_authorized") is False
        and plan.get("parent_inheritance_sha256") == digest(inherited),
        "code successor changes original authority",
    )
    identity = database.stat()
    require(
        plan.get("database_identity")
        == {"path": str(database), "device": identity.st_dev, "inode": identity.st_ino},
        "formal database identity changed",
    )
    issued = datetime.fromisoformat(plan["issued_at"].replace("Z", "+00:00"))
    require(
        issued.utcoffset() is not None
        and build["created_at"] == plan["issued_at"]
        and datetime.fromisoformat(parent["created_at"].replace("Z", "+00:00"))
        <= issued
        and (at is None or issued <= datetime.fromisoformat(at.replace("Z", "+00:00")))
        and all(
            isinstance(plan.get(key), str) and plan[key].strip()
            for key in ("actor", "reason")
        ),
        "code successor provenance differs",
    )
    verify_checks(
        plan.get("checks", {}),
        tree_ref=tree_ref,
        changes=changes,
        parent_ref=plan["parent_build"],
        review_ref=plan["review_manifest"],
    )
    proof = {**plan, "loaded_build": dict(build_ref)}
    proof["proof_sha256"] = digest(proof)
    # Keeping the exact prior flow proof also keeps its paid-gate binding.
    return {**inherited, "account_taxonomy_code_proof": proof}


def quiescence(connection: sqlite3.Connection, *, at: str) -> dict[str, int]:
    """Pending work is allowed; any execution or unexpired owner blocks install."""
    require(
        connection.execute("PRAGMA user_version").fetchone()[0] == 23,
        "schema23 database required",
    )
    conditions = {
        "scheduler_runs": "status='running'",
        "scheduler_run_attempts": "status='running' OR (owner_token IS NOT NULL AND julianday(lease_expires_at)>=julianday(?))",
        "capture_work_items": "state IN ('leased','running') OR (owner_token IS NOT NULL AND julianday(lease_expires_at)>=julianday(?))",
        "fetch_slots": "status='running'",
        "media_processing_slots": "status='running'",
        "report_tasks": "task_status IN ('running','cancel_requested')",
    }
    return {
        table: connection.execute(
            "SELECT COUNT(*) FROM " + table + " WHERE " + where,
            (at,) if "?" in where else (),
        ).fetchone()[0]
        for table, where in conditions.items()
    }
