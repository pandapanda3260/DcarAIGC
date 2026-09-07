"""Real temporary migration, source archive, sealer, release and successor chain.

Only the OS-installed-process boundary is isolated. Test-log markers are named
parser fixtures, never production test evidence or transport qualification.
"""
from __future__ import annotations

import copy
import json
import os
import shutil
import sqlite3
import subprocess
import unittest
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from tests import test_issue_v20_deployment_receipt as candidate
from tests import test_v20_deferred_acceptance as deferred
from v8 import capture_activation_release as activation, capture_authorizations as auth
from v8 import capture_code_successor as code, capture_release as release
from v8 import forward_recovery, paid_drain, profile_control, runtime_database, storage
from v8.metric_field_facts import utc
from v8.profile_activations import activation_at
from v8.source_routing import parse_time

sealer = deferred.issuer.sealer
ROOT = Path(__file__).resolve().parents[1]
AT = "2026-09-06T00:01:00Z"


class CaptureCodeSuccessorTest(unittest.TestCase):
    @contextmanager
    def ready(self, *, previous_pipeline: bytes | None = None, operations=None):
        original_scenario = candidate.CandidateIssuerTest._scenario
        @contextmanager
        def populated_scenario(paired):
            with original_scenario(paired) as layout:
                for relative in (*sealer.V20_CRITICAL_FILES, Path("src/dcar_eval/v8/provider_transport.py"), Path("src/dcar_eval/tikhub_config.py")):
                    target = layout["project"] / relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(ROOT / relative, target)
                if previous_pipeline is not None:
                    (layout["project"] / "src/dcar_eval/v8/pipeline.py").write_bytes(previous_pipeline)
                yield layout
        base = deferred.DeferredAcceptanceTest(methodName="runTest")
        with patch.object(sealer, "V20_CRITICAL_FILES", sealer.V20_LEGACY_CRITICAL_FILES), patch.object(
            code, "_installed_build_path", side_effect=lambda: Path(os.environ["DCAR_LOADED_BUILD_RECEIPT"]) if os.environ.get("DCAR_LOADED_BUILD_RECEIPT") else None,
        ), patch.object(candidate.CandidateIssuerTest, "_scenario", populated_scenario), patch.object(
            runtime_database, "load_installed_writer_contract", return_value=None,
        ), base.ready() as (
            connection, args, evidence, project, original_logs,
        ), patch.object(forward_recovery, "PROJECT_ROOT", project):
            previous_path = Path(os.environ["DCAR_LOADED_BUILD_RECEIPT"])
            previous = sealer._read_receipt(previous_path, contract_version=sealer.SEALED_BUILD_CONTRACT)
            # The generic fixture adds result labels. Match the real lineage
            # producer before issuing the immutable old acceptance.
            for name in ("migration_receipt", "install_receipt"):
                reference = previous["postmigration_lineage"][name]
                previous["postmigration_lineage"][name] = sealer._external_receipt(
                    Path(reference["path"]), project, allow_schema20_migration=name == "migration_receipt")[1]
            previous_path.write_text(json.dumps(sealer._envelope(sealer.SEALED_BUILD_CONTRACT, previous)))
            evidence["build_sha256"] = sealer._sha256_file(previous_path)
            if operations is not None:
                args["operations"] = list(operations)
            accepted = deferred.issuer.issue_deferred_acceptance(connection, **args)
            evidence["deployment"] = accepted
            source = evidence["active"]
            paid_drain.start_profile_drain_in_transaction(connection, "isolated-mode-b", switch_kind="cross_profile", now=deferred.AT,
                binding={"source_activation_id": source["activation_id"], "target_activation_id": source["activation_id"],
                    "business_day": "2026-09-06", "planned_effective_at": source["effective_at"],
                    "build_receipt_sha256": evidence["build_sha256"], "runtime_root_receipt_sha256": evidence["runtime_sha256"]})
            paid_drain.seal_profile_drain_in_transaction(connection, "isolated-mode-b", now=deferred.AT)
            paid_drain.release_profile_drain_in_transaction(connection, "isolated-mode-b", now=deferred.AT)
            for operation in args["operations"]:
                release.publish_operation_gate(connection, operation=operation, at=AT)
            frozen = activation.snapshot_source_operations(connection, operations=args["operations"], at=AT)
            begun = profile_control.begin_cross_profile_switch_in_transaction(connection, drain_id="isolated-integrated",
                target_profile_id="integrated_route_v1", roster_snapshot_id=source["roster_snapshot_id"],
                build_receipt_sha256=evidence["build_sha256"], runtime_root_receipt_sha256=evidence["runtime_sha256"],
                actor="isolated-test-user", reason="Real temporary integrated ledger", now=AT, capture_source_operations=frozen)
            complete_at = utc((parse_time(begun["activation"]["effective_at"]) - timedelta(minutes=5)).isoformat())
            at = utc((parse_time(begun["activation"]["effective_at"]) + timedelta(minutes=1)).isoformat())
            database = Path(connection.execute("PRAGMA database_list").fetchone()[2])
            connection.commit()
            profile_control.complete_cross_profile_switch(db_path=database, drain_id="isolated-integrated", now=complete_at)
            connection.execute("BEGIN IMMEDIATE")
            active = activation_at(connection, at)
            evidence["active"] = active
            evidence["activation_successor"] = activation.validate_installed_activation_successor(connection,
                source_deployment=accepted, current_active=active, runtime_bindings=activation._runtime(evidence),
                manifest=evidence["manifest"], at=at)
            for operation in args["operations"]:
                release.publish_operation_gate(connection, operation=operation, at=at)
            before = [tuple(row) for row in connection.execute("SELECT * FROM deployment_readiness_receipts ORDER BY id")]
            original_decision = args["decision_receipt_path"].read_bytes()
            path = project / code.BUSINESS
            path.write_bytes(path.read_bytes() + b"\n# isolated planner-budget source delta\n")
            logs = dict(original_logs)
            for name in ("planner_budget", "code_successor"):
                logs[name] = previous_path.parent / (name + "-parser-fixture.log")
                logs[name].write_text(f"isolated parser fixture only; not actual production results\nDCAR_TEST_RESULT name={name} exit=0\n")
                logs[name].chmod(0o600)
            parent = previous_path.parent / "successor-private"
            parent.mkdir(mode=0o700)
            plan_ref = code.prepare_plan(connection, project_root=project, previous_build=previous_path,
                evidence_dir=parent / "evidence", tests=logs, actor="isolated-test-user",
                reason="Planner budget fixture only", at=at)
            yield {"connection": connection, "project": project, "database": database,
                "accepted": accepted, "evidence": evidence, "previous": previous_path,
                "installed_plist": previous_path.parent / "fixture.plist",
                "plan": plan_ref, "at": at, "before": before, "decision_path": args["decision_receipt_path"],
                "original_decision": original_decision, "logs": logs}

    def seal(self, case):
        connection, project = case["connection"], case["project"]
        plan = sealer._read_private_json(Path(case["plan"]["path"]))
        folder = Path(case["plan"]["path"]).parent
        installed = runtime_database.InstalledWriterContract(project, case["installed_plist"], project,
            project / "fixture-python", case["database"], project / "runtime/operator-freeze.lock", {
                "Label": "isolated-writer", "ProgramArguments": [str(project / "fixture-python")],
                "EnvironmentVariables": {"DCAR_PROJECT_ROOT": str(project), "DCAR_V8_DB": str(case["database"]),
                    "DCAR_WRITER_LOCK": str(project / "runtime/operator-freeze.lock"),
                    "DCAR_V8_REPORTS_ROOT": str(project / sealer.ROOT_PATHS["reports"]),
                    "DCAR_LOADED_BUILD_RECEIPT": str(case["previous"])}})
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.close()
        try:
            runtime = sealer._runtime_payload(project, installed=installed, formal_schema=20, code_schema=20)
            runtime_path = folder / "runtime-root-binding-v1.json"
            sealer._write_exclusive(runtime_path, sealer._envelope(sealer.RUNTIME_ROOT_CONTRACT, runtime))
            with patch.object(sealer, "_utc_now", return_value=case["at"]):
                build = sealer._build_payload(project, installed=installed, runtime_receipt_path=runtime_path,
                    test_results_receipt_path=Path(plan["full_checks"]["path"]), formal_schema=20, code_schema=20,
                    install_receipt=Path(case["accepted"]["evidence"]["install"]["path"]),
                    allow_working_tree=True, code_successor_plan=Path(case["plan"]["path"]))
        finally:
            # Reopen the same fixture-owned object for its outer managed scope;
            # no DB descriptor remains open while the real sealer runs.
            connection.__init__(str(case["database"]))
            connection.row_factory = sqlite3.Row
            storage.configure_connection_safety(connection)
        build_path = folder / "sealed-build-receipt-v1.json"
        sealer._write_exclusive(build_path, sealer._envelope(sealer.SEALED_BUILD_CONTRACT, build))
        connection.execute("BEGIN IMMEDIATE")
        return build_path

    def test_actual_sealer_decision_preserves_acceptance_and_portable_proof(self):
        with self.ready() as case:
            build_path = self.seal(case)
            connection = case["connection"]
            with self.assertRaisesRegex(auth.AuthorizationError, "has not been issued"):
                code.current_proof(connection, project_root=case["project"], build_path=build_path, at=case["at"])
            proof = code.issue_decision(connection, project_root=case["project"], build_path=build_path,
                mirror_root=build_path.parent / "control-mirrors", at=case["at"])
            self.assertEqual(proof, code.issue_decision(connection, project_root=case["project"], build_path=build_path,
                mirror_root=build_path.parent / "control-mirrors", at=case["at"]))
            self.assertEqual(proof["origin_runtime_bindings"], case["accepted"]["release_decision"]["runtime_bindings"])
            self.assertNotEqual(proof["runtime_bindings"]["build_sha256"], proof["origin_runtime_bindings"]["build_sha256"])
            self.assertEqual(proof["plan_payload"]["business_e2e"], "deferred_by_user")
            self.assertEqual(proof["decision_receipt"]["payload"]["transport_qualification"], "not_verified")
            case["evidence"].update(proof["runtime_bindings"])
            case["evidence"]["code_successor"] = proof
            for operation in proof["plan_payload"]["operations"]:
                result = release.publish_operation_gate(connection, operation=operation, at=case["at"])
                self.assertEqual(result["qualification"], "operator_authorized")
                bindings = release.current_runtime_bindings(connection, operation, case["at"])
                self.assertEqual(bindings["build_receipt_sha256"], proof["runtime_bindings"]["build_sha256"])
                self.assertEqual(bindings["activation_id"], proof["active"]["activation_id"])
            self.assertEqual(case["before"], [tuple(row) for row in connection.execute("SELECT * FROM deployment_readiness_receipts ORDER BY id")])
            self.assertEqual(case["decision_path"].read_bytes(), case["original_decision"])
            with patch.object(release, "_release_tools", side_effect=AssertionError("server cannot read private files")):
                self.assertEqual(code.validate_portable(connection, proof, deployment=case["accepted"], at=case["at"]), proof)
            for key in ("provider_usage", "provider_request_start_events", "fetch_attempts"):
                self.assertEqual(connection.execute(f"SELECT count(*) FROM {key}").fetchone()[0], 0)
            for mutate in (
                lambda p: p["runtime_bindings"].update(build_sha256="f" * 64),
                lambda p: p["plan_payload"].update(operations=[]),
                lambda p: p.update(release_event_id=999),
            ):
                changed = copy.deepcopy(proof)
                mutate(changed)
                changed["proof_sha256"] = auth.digest({key: value for key, value in changed.items() if key != "proof_sha256"})
                with self.assertRaises(auth.AuthorizationError):
                    code.validate_portable(connection, changed, deployment=case["accepted"], at=case["at"])

    def test_changed_source_and_real_test_log_fail_before_new_decision(self):
        with self.ready() as case:
            build_path = self.seal(case)
            path = case["project"] / code.BUSINESS
            original = path.read_bytes()
            path.write_bytes(original + b"# drift after sealing\n")
            with self.assertRaisesRegex(auth.AuthorizationError, "live source"):
                code.current_proof(case["connection"], project_root=case["project"], build_path=build_path,
                    at=case["at"], require_decision=False)
            path.write_bytes(original)
            case["logs"]["planner_budget"].write_text("FAILED real parser fixture mutation\nDCAR_TEST_RESULT name=planner_budget exit=1\n")
            with self.assertRaises(sealer.R0ReceiptError):
                code.current_proof(case["connection"], project_root=case["project"], build_path=build_path,
                    at=case["at"], require_decision=False)
            self.assertEqual(case["connection"].execute("SELECT count(*) FROM scheduler_runs WHERE job_id='transport_receipt:campaign_terminal'").fetchone()[0], 0)

    def test_runtime_issuer_rejects_pipeline_only_successor_with_signed_parent(self):
        # The first generation contains the actual pipeline before 72c9b35;
        # only the second generation receives that exact reviewed repair.
        pipeline_before = subprocess.run(["git", "show", "72c9b35^:src/dcar_eval/v8/pipeline.py"],
            cwd=ROOT, check=True, capture_output=True).stdout
        pipeline_after = subprocess.run(["git", "show", "72c9b35:src/dcar_eval/v8/pipeline.py"],
            cwd=ROOT, check=True, capture_output=True).stdout
        self.assertNotEqual(pipeline_before, pipeline_after)
        with self.ready(previous_pipeline=pipeline_before) as case:
            build1 = self.seal(case)
            connection, project = case["connection"], case["project"]
            pipeline = project / "src/dcar_eval/v8/pipeline.py"
            logs = dict(case["logs"])
            extra = build1.parent.parent / "integrated-pipeline-parser-fixture.log"
            extra.write_text("isolated parser fixture only; not production evidence\nDCAR_TEST_RESULT name=integrated_pipeline exit=0\n")
            extra.chmod(0o600)
            logs["integrated_pipeline"] = extra
            def prepare(name):
                with patch.dict(os.environ, {"DCAR_LOADED_BUILD_RECEIPT": str(build1)}):
                    return code.prepare_plan(connection, project_root=project, previous_build=build1,
                        evidence_dir=build1.parent.parent / name, tests=logs, actor="isolated-test-user",
                        reason="Exact integrated control pipeline fixture", at=case["at"])
            pipeline.write_bytes(pipeline_after)
            with self.assertRaises(auth.AuthorizationError):
                prepare("unsigned-parent")
            pipeline.write_bytes(pipeline_before)
            code.issue_decision(connection, project_root=project, build_path=build1,
                mirror_root=build1.parent / "control-mirrors", at=case["at"])
            pipeline.write_bytes(pipeline_after + b"\n# unreviewed extra pipeline edit\n")
            with self.assertRaisesRegex(auth.AuthorizationError, "pipeline|source"):
                prepare("wrong-pipeline")
            pipeline.write_bytes(pipeline_after)
            # The current runtime-v2 issuer does not mint another historical
            # V1 pipeline-only generation, even with an authentic signed parent.
            # Historical two-generation validation remains covered by V2 tests.
            with self.assertRaisesRegex(auth.AuthorizationError, "runtime successor"):
                prepare("pipeline-only-runtime-plan")
            self.assertEqual(case["before"], [tuple(row) for row in connection.execute("SELECT * FROM deployment_readiness_receipts ORDER BY id")])
            self.assertEqual(case["decision_path"].read_bytes(), case["original_decision"])
            self.assertEqual(connection.execute("SELECT count(*) FROM scheduler_runs WHERE job_id='transport_receipt:campaign_terminal'").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)
