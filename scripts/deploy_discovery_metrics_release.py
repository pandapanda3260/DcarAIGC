#!/usr/bin/env python3
"""Deploy discovery metrics while preserving the installed overview proposal through the pinned catalog deployer.

No invocation occurs on import. Only proposal/lineage/proof preflight differs;
paired service stop, idle checks, maintenance lease, backup and rollback are
inherited unchanged. The overview successor preserves every historical proof and authorization.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import os
from pathlib import Path
import plistlib
import re
import stat
import sys

sys.dont_write_bytecode = True
DEPLOYER_SOURCE_PATH = Path("/Users/mark/Documents/ChatGPT/DcarAIGC/outputs/automatic-capture-unification-20260910/deploy_catalog_capture.py")
DEPLOYER_SOURCE_SHA256 = "d27a259b72cf38e93b9651262b4a2b9779d6bcbe6cf234149b8de8cbf115a4dc"


def _load_pinned(path=DEPLOYER_SOURCE_PATH, expected_sha256=DEPLOYER_SOURCE_SHA256):
    before = path.lstat()
    if (not path.is_absolute() or path.resolve(strict=True) != path or not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1 or before.st_uid != os.geteuid() or before.st_mode & 0o022
            or not 0 < before.st_size <= 1024 * 1024):
        raise ValueError("Pinned deployer source metadata differs")
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
        body, opened = stream.read(1024 * 1024 + 1), os.fstat(stream.fileno())
    def identity(value):
        return value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns
    if (identity(before) != identity(opened) or identity(opened) != identity(path.lstat())
            or hashlib.sha256(body).hexdigest() != expected_sha256):
        raise ValueError("Pinned deployer source SHA256 or identity changed; review required")
    spec = importlib.util.spec_from_file_location("overview_pinned_catalog_deployer", path)
    if spec is None or spec.loader is None:
        raise ValueError("Pinned deployer source cannot load")
    module = importlib.util.module_from_spec(spec)
    # Execute exactly the checked bytes, not a second read of the file.
    exec(compile(body, str(path), "exec"), module.__dict__)
    return module


EXPECTED_PARENT_BUILD_SHA256 = "bdc2baee628b10ca32d5603b17eed407e88d1e54d189bd3a612adfe6e09acc8f"
EXPECTED_OVERVIEW_PARENT_BUILD_SHA256 = "e5d53ba78459301648fbe080aa26beff810b135edd2f91021ea9bbc1f51931e8"
EXPECTED_PUBLISHED_DAILY_BUILD_SHA256 = "f5851e8c94031f7d5bd56784840b45fb57eb83df344a1203c5cee9b3eeb92005"
EXPECTED_PREVIOUS_PARENT_BUILD_SHA256 = "7f83f8b695f6f7f4e7eb474ec04c4667718c0f3e5c46f9364459292e2484b9cb"
EXPECTED_RECOVERY_BUILD_SHA256 = "e994617379b578b0a68a6d0e3c15bdae58ffe9d2006be553d6b6d6ccf0ae67c3"

base = _load_pinned()
require = base.require


class Deployer(base.Deployer):
    def policy_proof(self):
        verifier = base.load_verified_module(self.child, "src/dcar_eval/v8/discovery_metrics_release.py",
                                            "overview_deployer_lineage")
        inherited = verifier.verify_inheritance(build=self.child, build_ref=self.proposal["child_build"],
            install_path=self.install_path, database=self.database, source=Path(self.child["source_root"]),
            at=datetime.now(timezone.utc).isoformat())
        bindings = {
            "inherited_catalog_proof_sha256": ("catalog_capture_proof", self.parent["metric_gap_successor"]["parent_build"]),
            "inherited_control_simplification_proof_sha256": ("control_simplification_proof", self.parent["publisher_snapshot_successor"]["parent_build"]),
            "inherited_publisher_snapshot_proof_sha256": ("publisher_snapshot_proof", self.parent["publisher_capacity_successor"]["parent_build"]),
            "inherited_publisher_capacity_proof_sha256": ("publisher_capacity_proof", self.parent["account_profile_successor"]["parent_build"]),
            "inherited_account_profile_proof_sha256": ("account_profile_proof", self.parent["account_profile_recovery_successor"]["parent_build"]),
            "inherited_profile_operation_authority_sha256": ("profile_operation_authority", self.parent["account_profile_recovery_successor"]["parent_build"]),
            "profile_compensation_authority_sha256": ("profile_compensation_authority", inherited["previous_daily_pipeline_proof"]["parent_build"]),
            "account_profile_recovery_proof_sha256": ("account_profile_recovery_proof", inherited["previous_daily_pipeline_proof"]["parent_build"]),
            "inherited_metric_gap_proof_sha256": ("metric_gap_proof", self.parent["control_simplification_successor"]["parent_build"]),
            "inherited_manual_content_scope_proof_sha256": ("manual_content_scope_proof", self.parent["account_catalog_capture_successor"]["parent_build"]),
            "inherited_account_classification_proof_sha256": ("proof", self.parent["manual_content_scope_successor"]["parent_build"]),
            "inherited_previous_daily_pipeline_proof_sha256": ("previous_daily_pipeline_proof", inherited["published_daily_pipeline_proof"]["parent_build"]),
            "inherited_published_daily_pipeline_proof_sha256": ("published_daily_pipeline_proof", self.parent["daily_pipeline_successor"]["parent_build"]),
            "inherited_daily_pipeline_proof_sha256": ("daily_pipeline_proof", self.parent["overview_successor"]["parent_build"]),
            "inherited_overview_proof_sha256": ("overview_proof", self.proposal["parent_build"]),
            "discovery_metrics_proof_sha256": ("discovery_metrics_proof", self.proposal["child_build"]),
        }
        for field, (name, loaded) in bindings.items():
            require(inherited[name]["proof_sha256"] == self.proposal[field]
                and inherited[name]["loaded_build"] == loaded,
                "Verified overview proof differs from proposal: " + name)
            if field != "discovery_metrics_proof_sha256":
                require(self.child["discovery_metrics_successor"].get(field) == self.proposal[field],
                    "Overview proposal and source plan proof differ: " + name)
        previous, published, daily, overview = (inherited[name] for name in (
            "previous_daily_pipeline_proof", "published_daily_pipeline_proof", "daily_pipeline_proof", "overview_proof"))
        require(previous["parent_build"].get("sha256") == EXPECTED_RECOVERY_BUILD_SHA256
            and previous["loaded_build"].get("sha256") == EXPECTED_PREVIOUS_PARENT_BUILD_SHA256
            and published["parent_build"] == previous["loaded_build"]
            and published["loaded_build"].get("sha256") == EXPECTED_PUBLISHED_DAILY_BUILD_SHA256
            and daily["parent_build"] == published["loaded_build"]
            and daily.get("transition") == "daily-metric-validity-20260911-v1"
            and overview["parent_build"] == self.parent["overview_successor"]["parent_build"]
            and overview.get("contract") == "overview-code-successor-v1"
            and overview.get("transition") == "overview-four-platforms-20260912-v1",
            "Historical daily or overview proof parent/transition differs")
        discovery = inherited["discovery_metrics_proof"]
        require(discovery.get("contract") == "discovery-metrics-code-successor-v1"
            and discovery.get("transition") == "discovery-metrics-20260912-v2"
            and discovery["parent_build"] == self.proposal["parent_build"],
            "Discovery metrics proof parent or transition differs")
        authority = inherited["profile_operation_authority"]
        compensation = inherited["profile_compensation_authority"]
        recovery = inherited["account_profile_recovery_proof"]
        require(authority["authorization"] == self.parent["account_profile_successor"]["authorization"]
            and compensation["authorization"] == recovery["authorization"]
                == self.parent["account_profile_recovery_successor"]["authorization"]
            and compensation["profile_authority_proof_sha256"] == authority["proof_sha256"],
            "Published profile authorization changed; overview cannot issue or renew authority")
        policy = inherited["catalog_capture_policy"]
        require(inherited["catalog_capture_policy_sha256"] == base.digest(policy), "Catalog policy digest differs")
        return {"policy": policy, "policy_sha256": inherited["catalog_capture_policy_sha256"],
                "proof_sha256": inherited["catalog_capture_proof"]["proof_sha256"],
                "controls_proof_sha256": inherited["control_simplification_proof"]["proof_sha256"],
                "publisher_snapshot_proof_sha256": inherited["publisher_snapshot_proof"]["proof_sha256"],
                "publisher_capacity_proof_sha256": inherited["publisher_capacity_proof"]["proof_sha256"],
                "account_profile_proof_sha256": inherited["account_profile_proof"]["proof_sha256"],
                "profile_operation_authority_sha256": authority["proof_sha256"],
                "profile_compensation_authority_sha256": compensation["proof_sha256"],
                "account_profile_recovery_proof_sha256": recovery["proof_sha256"],
                "metric_gap_proof_sha256": inherited["metric_gap_proof"]["proof_sha256"],
                "manual_content_scope_proof_sha256": inherited["manual_content_scope_proof"]["proof_sha256"],
                "account_classification_proof_sha256": inherited["proof"]["proof_sha256"],
                "previous_daily_pipeline_proof_sha256": previous["proof_sha256"],
                "published_daily_pipeline_proof_sha256": published["proof_sha256"],
                "daily_pipeline_proof_sha256": daily["proof_sha256"],
                "overview_proof_sha256": overview["proof_sha256"],
                "discovery_metrics_proof_sha256": discovery["proof_sha256"]}

    def preflight(self):
        require(sys.version_info[:2] >= (3, 12),
                "Deployment requires Python 3.12 or newer; use /Users/mark/Projects/DcarAIGC/.venv/bin/python")
        require(re.fullmatch(r"[0-9a-f]{64}", self.args.expected_child_build), "Expected child build must be its SHA256")
        self.proposal = proposal = base.object_json(base.raw(self.args.proposal, private=True))
        require(proposal.get("contract") == "discovery-metrics-install-proposal-v1"
            and proposal.get("status") == "prepared"
            and "authorization" not in proposal
            and proposal["parent_build"]["sha256"] == EXPECTED_PARENT_BUILD_SHA256
            and proposal["child_build"]["sha256"] == self.args.expected_child_build
            and proposal.get("database_writes") == 0 and proposal.get("provider_calls") == 0
            and proposal.get("services_changed") is False and proposal.get("paid_gates_reopened") is False
            and proposal.get("schema_migration_repeated") is False and proposal.get("business_scope_change") == "none",
            "Unexpected overview proposal, scope or parent/child build")
        self.child, self.parent = base.build_at(proposal["child_build"]), base.build_at(proposal["parent_build"])
        parent_successors = ("overview_successor", "daily_pipeline_successor", "account_profile_recovery_successor", "account_profile_successor",
            "publisher_capacity_successor", "publisher_snapshot_successor", "control_simplification_successor",
            "metric_gap_successor", "account_catalog_capture_successor", "manual_content_scope_successor",
            "account_classification_successor")
        require(isinstance(self.parent.get("daily_pipeline_successor"), dict)
            and self.parent.get("discovery_metrics_successor") is None
            and self.parent.get("overview_successor", {}).get("contract") == "overview-code-successor-v1"
            and self.parent["overview_successor"].get("transition") == "overview-four-platforms-20260912-v1"
            and self.parent["overview_successor"]["parent_build"].get("sha256") == EXPECTED_OVERVIEW_PARENT_BUILD_SHA256
            and self.parent["daily_pipeline_successor"].get("contract") == "daily-pipeline-code-successor-v1"
            and self.parent["daily_pipeline_successor"].get("transition") == "daily-metric-validity-20260911-v1"
            and self.parent["daily_pipeline_successor"].get("parent_build", {}).get("sha256") == EXPECTED_PUBLISHED_DAILY_BUILD_SHA256
            and "authorization" not in self.parent["daily_pipeline_successor"]
            and self.child.get("discovery_metrics_successor", {}).get("parent_build") == proposal["parent_build"]
            and self.child["discovery_metrics_successor"].get("contract") == "discovery-metrics-code-successor-v1"
            and self.child["discovery_metrics_successor"].get("transition") == "discovery-metrics-20260912-v2"
            and "authorization" not in self.child["discovery_metrics_successor"]
            and all(isinstance(self.parent.get(name), dict) and self.child.get(name) == self.parent[name]
                    for name in parent_successors)
            and self.parent.get("schema_contract") == self.child.get("schema_contract") == {"code_schema": 21, "formal_schema": 21},
            "Prepared overview parent or inherited successor/schema lineage differs")
        code_fields = {"source_root", "git", "critical_files", "code_successor_plan", "account_cleanup_generation",
                       "discovery_metrics_successor", "created_at", "validation_scope"}
        require({key: value for key, value in self.child.items() if key not in code_fields}
                == {key: value for key, value in self.parent.items() if key not in code_fields}
            and {key: value for key, value in self.child["account_cleanup_generation"].items() if key != "source_tree"}
                == {key: value for key, value in self.parent["account_cleanup_generation"].items() if key != "source_tree"},
            "Overview changes non-code build fields or historical operator authority")
        self.database, self.database_identity = Path(proposal["formal_database"]), proposal["database_identity"]
        require(self.database.resolve(strict=True) == self.database and base.identity(self.database) == self.database_identity,
                "Formal database identity changed")
        self.pairs = [(Path(proposal["installed_plist"]), base.checked(proposal["before_plist"]), base.checked(proposal["next_plist"]))]
        publisher = proposal.get("publisher")
        require(isinstance(publisher, dict), "A paired Publisher proposal is required")
        self.pairs.append((Path(publisher["installed_plist"]), base.checked(publisher["before_plist"]), base.checked(publisher["next_plist"])))
        for path, before, _ in self.pairs:
            require(base.raw(path) == before, "Installed plist differs from reviewed parent")
        writer_before, writer_next = (plistlib.loads(value) for value in self.pairs[0][1:])
        source, env = Path(self.child["source_root"]), writer_before["EnvironmentVariables"]
        require(writer_before.get("Label") == base.WRITER and env.get("DCAR_V8_DB") == str(self.database)
            and env.get("DCAR_LOADED_BUILD_RECEIPT") == proposal["parent_build"]["path"]
            and env.get("DCAR_WRITER_SOURCE_ROOT") == self.parent["source_root"], "Parent Writer contract differs")
        self.install_path = Path(env["DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT"])
        expected = {**writer_before, "EnvironmentVariables": {**env,
            "DCAR_LOADED_BUILD_RECEIPT": proposal["child_build"]["path"], "DCAR_WRITER_SOURCE_ROOT": str(source),
            "PYTHONPATH": str(source / "src/dcar_eval") + os.pathsep + str(source / "scripts")},
            "ProgramArguments": [str(source / "deploy/macos/run_writer_worker.sh")]}
        require(writer_next == expected, "Writer proposal changes unrelated configuration or controls")
        pub_before, pub_next = (plistlib.loads(value) for value in self.pairs[1][1:])
        require(pub_before.get("Label") == base.PUBLISHER
            and pub_before["EnvironmentVariables"].get("DCAR_V8_DB") == str(self.database)
            and pub_before["EnvironmentVariables"].get("DCAR_WRITER_SOURCE_ROOT") == self.parent["source_root"],
                "Parent Publisher contract differs")
        require(pub_next == {**pub_before, "EnvironmentVariables": {**pub_before["EnvironmentVariables"], "DCAR_WRITER_SOURCE_ROOT": str(source)},
            "ProgramArguments": [str(source / "deploy/macos/run_snapshot_publisher.sh")]}, "Publisher proposal changes unrelated configuration")
        verified = {"parent": self.verify_bootstrap(self.parent, proposal["parent_build"], self.pairs[0][1]),
                    "child": self.verify_bootstrap(self.child, proposal["child_build"], self.pairs[0][2]), "policy": self.policy_proof()}
        code, health = base.read_json_endpoint("/api/v8/health")
        ready_code, ready = base.read_json_endpoint("/api/v8/readyz")
        # A control-ready installed Publisher parent is required; missing daily data
        # coverage is distinct from a broken runtime permit.
        readiness = self.require_health(code, health, ready_code, ready, proposal["parent_build"]["sha256"])
        publisher_state = self.job_state(base.PUBLISHER)
        require(self.job_state(base.WRITER)["registered"] and publisher_state["registered"],
                "Both reviewed parent services must be registered")
        require(publisher_state["pid"] is None,
                "Active Publisher must finish before deployment; transfer and receiver left running")
        active = base.read_activity(self.database)
        self.save("preflight.json", {"health": health, "readiness": ready, "active_counts": active,
            "verified": verified, "readiness_acceptance": readiness, "database_identity": self.database_identity,
            "deployer_source_sha256": DEPLOYER_SOURCE_SHA256})
        self.event("preflight_checked", active_counts=active, parent_build=proposal["parent_build"]["sha256"],
                   readyz_http_status=ready_code, readiness_reason=ready.get("reason"))
        require(not any(active["blocking_active"].values()), "Active paid work must finish before deployment; services and retained uncertainty unchanged")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proposal", type=lambda value: Path(value).resolve(strict=True), required=True)
    parser.add_argument("--output-root", type=lambda value: Path(value).absolute(), required=True)
    parser.add_argument("--expected-child-build", required=True)
    Deployer(parser.parse_args()).run()


if __name__ == "__main__":
    main()
