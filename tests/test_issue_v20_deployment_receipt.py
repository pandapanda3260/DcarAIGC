"""Offline issuance and real native A/B/C proof, without production or HTTP."""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from tests import test_v20_release_tools as pair
from tests import test_v8_native_qualification as native
from tests.capture_v25_fixture import local_storage_receipt
from tests.v9_report_fixture import activate_v9_report_fixture
from v8 import capture_code_successor, capture_release, tikhub_scan, transport_execution
from v8.profile_activations import TIKHUB_PROFILE, append_activation
from v8.runtime_database import (
    DatabaseAccessMode, FileIdentity, InstalledWriterContract, ResolvedDatabaseAccess, RuntimeDatabaseError,
    acquire_writer_lock,
)
from v8.storage import connect, transaction
from v8.system_roster import seal_system_members

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("test_deployment_issuer", ROOT / "scripts/issue_v20_deployment_receipt.py")
assert spec is not None and spec.loader is not None
issuer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(issuer)
AT = "2026-09-06T00:00:00Z"


class CandidateIssuerTest(unittest.TestCase):
    _scenario = pair.IntegratedOfflineToolsTest._scenario
    _build_schema18 = pair.IntegratedOfflineToolsTest._build_schema18
    _backup = pair.IntegratedOfflineToolsTest._backup
    _build = pair.IntegratedOfflineToolsTest._build
    _install = pair.IntegratedOfflineToolsTest._install

    @contextmanager
    def ready_pair(self, *, lock=True):
        # Initial candidate fixtures have no installed build. Descendant
        # successor fixtures explicitly bind their temporary sealed build.
        with self._scenario() as layout, patch.object(pair.safety, "code_identity", pair.REAL_CODE_IDENTITY), patch.object(
            capture_code_successor, "_installed_build_path",
            side_effect=lambda: Path(os.environ["DCAR_LOADED_BUILD_RECEIPT"])
            if os.environ.get("DCAR_LOADED_BUILD_RECEIPT") else None,
        ):
            layout = {key: value.resolve() for key, value in layout.items()}
            project = layout["project"]
            subprocess.run(["git", "init", "-b", "main", str(project)], check=True, capture_output=True)
            (project / ".gitignore").write_text("app/data/\ndata/\nruntime/\nreports/\n")
            subprocess.run(["git", "-C", str(project), "add", ".gitignore"], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(project), "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                            "commit", "-m", "isolated issuer fixture"], check=True, capture_output=True)
            cache = project / "data/cache"
            cache.mkdir(parents=True)
            for name in (".comment_hash_salt", ".platform_user_salt"):
                (cache / name).write_bytes(b"t" * 32)
                (cache / name).chmod(0o600)
            activate_v9_report_fixture(layout["formal"], [])
            with connect(layout["formal"]) as connection:
                roster = seal_system_members(connection, [{"platform": "douyin", "uid": "1234567890",
                    "profile_ref": "https://www.douyin.com/user/MS4w.fixture"}], raw_root=cache / "roster",
                    actor="fixture", reason="source", sealed_at=pair.offline_fixture.STAMP)
                checksum = connection.execute("SELECT members_sha256 FROM account_roster_snapshots WHERE id=?", (roster["snapshot_id"],)).fetchone()[0]
                append_activation(connection, profile_id=TIKHUB_PROFILE, roster_snapshot_id=roster["snapshot_id"],
                    roster_members_sha256=checksum, effective_at=pair.offline_fixture.STAMP,
                    build_receipt_sha256="a" * 64, actor="fixture", created_at=pair.offline_fixture.STAMP)
                connection.commit()
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                connection.execute("PRAGMA journal_mode=DELETE")
            evidence = layout["migration_receipt"].parent
            sealer = issuer.sealer
            for relative in sealer.ROOT_PATHS.values():
                (project / relative).mkdir(parents=True, exist_ok=True)
            plist = evidence / "fixture.plist"
            plist.write_text("isolated installed-runtime fixture\n")
            plist.chmod(0o600)
            installed = InstalledWriterContract(project, plist, project,
                project / "fixture-python", layout["formal"], layout["freeze"], {
                    "Label": "isolated-writer", "ProgramArguments": [str(project / "fixture-python")],
                    "EnvironmentVariables": {"DCAR_PROJECT_ROOT": str(project), "DCAR_V8_DB": str(layout["formal"]),
                        "DCAR_WRITER_LOCK": str(layout["freeze"]), "DCAR_V8_REPORTS_ROOT": str(project / sealer.ROOT_PATHS["reports"])}})
            runtime_before = sealer._runtime_payload(project, installed=installed, formal_schema=19, code_schema=20)
            self._backup(layout)
            self._build(layout)
            self._install(layout)
            runtime_after = sealer._runtime_payload(project, installed=installed, formal_schema=20, code_schema=20)
            git = sealer._git_record(project, allow_working_tree=True)
            archive = evidence / "source.tar"
            sealer._write_source_archive(project, archive, git)
            logs = {}
            for name in sealer.REQUIRED_TEST_RESULTS:
                logs[name] = evidence / f"{name}.log"
                logs[name].write_text(f"isolated parser fixture only\nDCAR_TEST_RESULT name={name} exit=0\n")
                logs[name].chmod(0o600)
            def receipt(name, contract, payload):
                path = evidence / name
                path.write_text(json.dumps(sealer._envelope(contract, payload)))
                path.chmod(0o600)
                return {"path": str(path), "sha256": sealer._sha256_file(path)}
            checks = receipt("checks.json", sealer.TEST_RESULTS_CONTRACT,
                sealer._test_results_payload(project, paths=logs, git_record=git))
            runtime = receipt("runtime.json", sealer.RUNTIME_ROOT_CONTRACT, runtime_before)
            post_runtime = receipt("runtime20.json", sealer.RUNTIME_ROOT_CONTRACT, runtime_after)
            layout["runtime20_receipt"] = Path(post_runtime["path"])
            build = receipt("build.json", sealer.SEALED_BUILD_CONTRACT, {
                "status": "succeeded", "schema_contract": sealer._schema_contract(19, 20), "git": git,
                "installed_runtime": runtime_before["installed_runtime"],
                "source_archive": sealer._source_archive_record(archive, git), "critical_files": {},
                "runtime_root_receipt": runtime, "test_results_receipt": checks})
            args = {"deployment_id": "candidate-test", "project_root": project,
                "preinstall_build_receipt": Path(build["path"]), "install_receipt": layout["install_receipt"],
                "storage_policy": local_storage_receipt(evidence / "local-raw"), "at": AT}
            access = ResolvedDatabaseAccess(DatabaseAccessMode.WRITER, layout["formal"],
                FileIdentity.from_stat(layout["formal"].stat()), project, layout["freeze"], installed)
            from contextlib import nullcontext
            with acquire_writer_lock(access) if lock else nullcontext(), connect(layout["formal"]) as connection:
                connection.execute("BEGIN IMMEDIATE")
                yield connection, args, layout, logs

    def test_actual_pair_candidate_zero_qualification_is_idempotent_and_paid_stays_closed(self):
        with self.ready_pair() as (connection, args, _, _):
            first = issuer.issue_candidate(connection, **args)
            self.assertEqual(first["status"], "candidate")
            self.assertFalse(first["deployment_eligible"])
            self.assertFalse(first["ordinary_paid_authorized"])
            self.assertTrue(issuer.issue_candidate(connection, **args)["idempotent"])
            self.assertEqual(connection.execute("SELECT count(*) FROM deployment_readiness_receipts").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT count(*) FROM capture_paid_send_gate_events").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)
            with self.assertRaisesRegex(issuer.contract.ReleaseContractError, "different evidence"):
                issuer.issue_candidate(connection, **{**args, "storage_policy": local_storage_receipt(Path(args["storage_policy"]["root"]), daily_stored_p95=2)})

    def test_missing_real_writer_lease_rejects_before_receipt(self):
        with self.ready_pair(lock=False) as (connection, args, _, _):
            with self.assertRaises(RuntimeDatabaseError):
                issuer.issue_candidate(connection, **args)
            self.assertEqual(connection.execute("SELECT count(*) FROM deployment_readiness_receipts").fetchone()[0], 0)

    def test_failed_full_suite_is_not_relabelled_as_passed(self):
        with self.ready_pair() as (connection, args, _, logs):
            logs["backend"].write_text("FAILED (failures=16, errors=65, skipped=1)\nDCAR_TEST_RESULT name=backend exit=1\n")
            with self.assertRaisesRegex(issuer.contract.ReleaseContractError, "invalid"):
                issuer.issue_candidate(connection, **args)
            self.assertEqual(connection.execute("SELECT count(*) FROM deployment_readiness_receipts").fetchone()[0], 0)

    def test_install_inode_and_source_archive_tamper_fail_without_receipt(self):
        with self.ready_pair() as (connection, args, layout, _):
            original = layout["install_receipt"].read_bytes()
            install = json.loads(original)
            install["installed"]["file"]["inode"] += 1
            layout["install_receipt"].write_text(json.dumps(install))
            with self.assertRaisesRegex(issuer.contract.ReleaseContractError, "inode/path"):
                issuer.issue_candidate(connection, **args)
            layout["install_receipt"].write_bytes(original)
            (layout["migration_receipt"].parent / "source.tar").write_bytes(b"not the sealed archive")
            with self.assertRaises(issuer.contract.ReleaseContractError):
                issuer.issue_candidate(connection, **args)
            self.assertEqual(connection.execute("SELECT count(*) FROM deployment_readiness_receipts").fetchone()[0], 0)


class NativeProofTest(unittest.TestCase):
    def setUp(self):
        self.native = native.NativeQualificationTest(methodName="runTest")
        self.native.setUp()
        self.addCleanup(self.native.doCleanups)
        self.native.cohort = self.native.freeze()

    def proof(self):
        with connect(self.native.db) as connection:
            return issuer._actual_e2e(connection, cohort_id=self.native.cohort["receipt_id"],
                evidence=self.native.evidence, at=self.native.at)

    def test_real_native_dispatch_raw_and_own_facts_prove_bounded_e2e_without_200_or_old_qualification(self):
        with patch.object(capture_release, "_qualification", side_effect=AssertionError("old qualification forbidden")):
            self.native.execute(1)
            result = self.proof()
        self.assertEqual(len(self.native.calls), 1)
        self.assertEqual(len(result["fetch_attempt_ids"]), 1)
        self.assertTrue(result["content_ids"])
        self.assertFalse(result["coverage_complete"])
        with connect(self.native.db) as connection:
            self.assertEqual(connection.execute("SELECT state FROM capture_paid_send_gate_events ORDER BY id DESC LIMIT 1").fetchone()[0], "diagnostic_only")
        self.assertEqual(self.proof(), result)
        self.assertEqual(len(self.native.calls), 1)

    def test_no_send_or_raw_without_own_facts_does_not_claim_e2e_then_local_recovery_does(self):
        with self.assertRaisesRegex(issuer.contract.ReleaseContractError, "own materialized"):
            self.proof()
        with patch.object(tikhub_scan, "_apply", side_effect=RuntimeError("post-C crash")):
            with self.assertRaisesRegex(RuntimeError, "post-C crash"):
                self.native.execute(1)
        with self.assertRaisesRegex(issuer.contract.ReleaseContractError, "own materialized"):
            self.proof()
        transport_execution.resume_native_member_local(self.native.cohort["receipt_id"], 1,
            db_path=self.native.db, raw_root=self.native.raw_root, at=self.native.at)
        self.assertTrue(self.proof()["content_ids"])
        self.assertEqual(len(self.native.calls), 1)

    def test_accepted_writer_entry_requires_current_candidate_before_e2e(self):
        with connect(self.native.db) as connection, transaction(connection):
            self.native.evidence["deployment"] = {"status": "candidate", "deployment_id": "real-candidate"}
            with self.assertRaisesRegex(issuer.contract.ReleaseContractError, "current installed candidate"):
                issuer.issue_accepted(connection, deployment_id="accepted", candidate_id="another-candidate",
                    native_cohort_id=self.native.cohort["receipt_id"], e2e_receipt_path=self.native.root / "e2e.json", at=self.native.at)
            self.assertEqual(connection.execute("SELECT count(*) FROM deployment_readiness_receipts").fetchone()[0], 0)
        self.assertEqual(self.native.calls, [])

    def test_accepted_real_proof_persists_once_and_recovers_post_fsync_rollback(self):
        # The existing native fixture isolates the installed-plist verifier.
        # All offline files, native send/raw/facts, storage checks and actual
        # deployment validator/INSERT below are real temporary evidence.
        paired = CandidateIssuerTest(methodName="runTest")
        with paired.ready_pair() as (pair_db, args, layout, _):
            issuer.issue_candidate(pair_db, **args)
            payload = json.loads(pair_db.execute("SELECT payload_json FROM deployment_readiness_receipts").fetchone()[0])
            payload["bindings"].update({key: self.native.active[key] for key in (
                "activation_id", "profile_id", "roster_snapshot_id", "roster_members_sha256", "activation_sha256")})
            project = layout["project"]
            with connect(self.native.db) as connection, transaction(connection):
                candidate = issuer._append(connection, deployment_id="native-candidate", status="candidate",
                    payload=payload, at=self.native.at, project_root=project)
            self.native.evidence["deployment"] = candidate
            self.native.execute(1)
            e2e = layout["migration_receipt"].parent / "bounded-e2e.json"
            arguments = dict(deployment_id="accepted-test", candidate_id="native-candidate",
                native_cohort_id=self.native.cohort["receipt_id"], e2e_receipt_path=e2e, at=self.native.at)
            transport_code = capture_release._transport_code()
            with patch.object(capture_release, "PROJECT_ROOT", project), patch.object(
                capture_release, "_transport_code", return_value=transport_code,
            ):
                with connect(self.native.db) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    accepted = issuer.issue_accepted(connection, **arguments)
                    self.assertTrue(accepted["deployment_eligible"])
                    self.assertFalse(accepted["ordinary_paid_authorized"])
                    self.assertFalse(accepted["coverage_complete"])
                    self.assertTrue(e2e.exists())
                    original = e2e.read_bytes()
                    connection.rollback()  # Simulate outer command transaction loss.
                    self.assertEqual(connection.execute("SELECT count(*) FROM deployment_readiness_receipts WHERE status='accepted'").fetchone()[0], 0)
                    connection.execute("BEGIN IMMEDIATE")
                    accepted = issuer.issue_accepted(connection, **arguments)
                    self.native.evidence["deployment"] = accepted
                    self.assertTrue(issuer.issue_accepted(connection, **arguments)["idempotent"])
                    self.assertEqual(e2e.read_bytes(), original)
                    self.assertEqual(connection.execute("SELECT count(*) FROM deployment_readiness_receipts WHERE status='accepted'").fetchone()[0], 1)
                    with self.assertRaisesRegex(issuer.contract.ReleaseContractError, "current installed candidate"):
                        issuer.issue_accepted(connection, **{**arguments, "deployment_id": "accepted-second"})
                    self.assertEqual(connection.execute("SELECT state FROM capture_paid_send_gate_events ORDER BY id DESC LIMIT 1").fetchone()[0], "diagnostic_only")
                    connection.commit()
            self.assertEqual(len(self.native.calls), 1)


if __name__ == "__main__":
    unittest.main()
