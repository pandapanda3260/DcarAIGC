"""Real temporary source trees and immutable receipts; never a formal database."""
from __future__ import annotations

import copy
import hashlib
import json
import os
import py_compile
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import seal_r0_receipts as sealer
from v8 import forward_recovery, runtime_source_successor as source


def sha(body):
    return hashlib.sha256(body).hexdigest()


class SourceTreeBoundaryTest(unittest.TestCase):
    def setUp(self):
        self.temp = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.root = self.temp / "source"
        self.root.mkdir()
        self.data = self.temp / "data-project"
        self.data.mkdir()
        self.git("init", "-q", "-b", "frozen-release")
        (self.root / "src").mkdir()
        (self.root / "src/service.py").write_text("VALUE = 1\n")
        (self.root / ".gitignore").write_text("ignored.py\n__pycache__/\n")
        self.commit()
        self.enterContext(patch.object(source, "_code", return_value=SimpleNamespace(_tools=lambda: (sealer, None))))
        self.manifest = {"contract": source.SOURCE_CONTRACT, "source_root": str(self.root),
            "git": sealer._git_record(self.root, allow_working_tree=True), "files": source._records(self.root)}
        self.reference = self.receipt("source-tree.json", self.manifest)

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.root), *args], check=True, capture_output=True).stdout

    def commit(self):
        self.git("add", "-A")
        self.git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture")

    def receipt(self, name, payload):
        path = self.temp / name
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        path.chmod(0o600)
        return {"path": str(path), "sha256": sha(path.read_bytes()), "byte_size": path.stat().st_size}

    def verify(self, reference=None):
        return source.verify_source_tree(source_root=self.root, reference=reference or self.reference, git=self.manifest["git"])

    def test_developer_commit_does_not_change_frozen_source(self):
        subprocess.run(["git", "clone", "-q", str(self.root), str(self.data)], check=True)
        (self.data / "src/service.py").write_text("VALUE = 'developer edit'\n")
        subprocess.run(["git", "-C", str(self.data), "add", "-A"], check=True)
        subprocess.run(["git", "-C", str(self.data), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                        "commit", "-qm", "developer change"], check=True)
        self.assertEqual(self.verify(), self.manifest)

    def test_source_edits_commits_and_mode_changes_are_rejected(self):
        path = self.root / "src/service.py"
        path.write_text("VALUE = 2\n")
        with self.assertRaisesRegex(source.auth.AuthorizationError, "frozen source"):
            self.verify()
        self.commit()
        with self.assertRaisesRegex(source.auth.AuthorizationError, "frozen source"):
            self.verify()
        self.git("reset", "--hard", self.manifest["git"]["head"])
        path.chmod(0o755)
        with self.assertRaisesRegex(source.auth.AuthorizationError, "frozen source"):
            self.verify()

    def test_ignored_executable_is_rejected_even_with_clean_git(self):
        (self.root / "src/ignored.py").write_text("raise RuntimeError('unexpected module')\n")
        self.assertEqual(self.git("status", "--porcelain"), b"")
        with self.assertRaisesRegex(source.auth.AuthorizationError, "unlisted executable"):
            self.verify()

    def test_existing_ignored_bytecode_is_rejected_despite_no_bytecode_writes(self):
        path = self.root / "src/service.py"
        # -B does not stop Python reading this cache on its next import.
        py_compile.compile(str(path), doraise=True)
        self.assertEqual(self.git("status", "--porcelain"), b"")
        with self.assertRaisesRegex(source.auth.AuthorizationError, "unlisted executable"):
            self.verify()

    def test_symlink_source_and_parent_directory_are_rejected(self):
        path = self.root / "src/service.py"
        other = self.temp / "other.py"
        other.write_bytes(path.read_bytes())
        path.unlink()
        path.symlink_to(other)
        with self.assertRaises((ValueError, RuntimeError)):
            self.verify()
        path.unlink()
        path.write_bytes(other.read_bytes())
        shutil.move(str(self.root / "src"), str(self.temp / "src"))
        (self.root / "src").symlink_to(self.temp / "src", target_is_directory=True)
        with self.assertRaises((ValueError, RuntimeError)):
            self.verify()

    def test_manifest_hash_duplicate_fields_and_file_inventory_fail_closed(self):
        altered = {**self.reference, "sha256": "f" * 64}
        with self.assertRaisesRegex(source.auth.AuthorizationError, "reference changed"):
            self.verify(altered)
        for files in ([], [*self.manifest["files"], self.manifest["files"][0]]):
            with self.subTest(files=len(files)), self.assertRaises(source.auth.AuthorizationError):
                self.verify(self.receipt("altered.json", {**self.manifest, "files": files}))
        path = self.temp / "duplicate.json"
        path.write_text('{"contract":"a","contract":"b"}')
        path.chmod(0o600)
        with self.assertRaisesRegex(source.auth.AuthorizationError, "duplicate"):
            self.verify({"path": str(path), "sha256": sha(path.read_bytes()), "byte_size": path.stat().st_size})

    def test_bootstrap_requires_installed_build_and_exact_private_source_chain(self):
        changes = {"fixture": {"before_sha256": None, "after_sha256": "a" * 64}}
        plan = {"contract": source.PLAN, "transition": source.TRANSITION, "source_root": str(self.root),
            "project_root": str(self.data), "git": self.manifest["git"], "changes": changes, "source_tree": self.reference}
        plan_ref = self.receipt("plan.json", plan)
        build = {"status": "succeeded", "git": plan["git"], "schema_contract": sealer._schema_contract(20, 20), "code_successor_plan": plan_ref}
        build_ref = self.receipt("build.json", sealer._envelope(sealer.SEALED_BUILD_CONTRACT, build))
        environment = {"DCAR_PROJECT_ROOT": str(self.data), "DCAR_WRITER_SOURCE_ROOT": str(self.root), "DCAR_LOADED_BUILD_RECEIPT": build_ref["path"]}
        installed = SimpleNamespace(project_root=self.data, payload={"EnvironmentVariables": environment})
        with patch("v8.runtime_database.load_installed_writer_contract", return_value=installed), patch.dict(os.environ, environment), patch.object(source, "approved_changes", return_value=changes):
            for mode in ("writer", "publisher"):
                self.assertEqual(source.verify_bootstrap(project_root=self.data, source_root=self.root, build_receipt=Path(build_ref["path"]), mode=mode)["status"], "verified")
            with self.assertRaisesRegex(source.auth.AuthorizationError, "build is not installed"):
                source.verify_bootstrap(project_root=self.data, source_root=self.root, build_receipt=self.temp / "other", mode="writer")
            Path(plan_ref["path"]).write_text("{}")
            with self.assertRaisesRegex(source.auth.AuthorizationError, "reference changed"):
                source.verify_bootstrap(project_root=self.data, source_root=self.root, mode="publisher")

    def test_bootstrap_cannot_substitute_uninstalled_source_root(self):
        installed = SimpleNamespace(project_root=self.data, payload={"EnvironmentVariables": {"DCAR_WRITER_SOURCE_ROOT": "/uninstalled"}})
        with patch("v8.runtime_database.load_installed_writer_contract", return_value=installed), self.assertRaisesRegex(source.auth.AuthorizationError, "environment"):
            source.verify_bootstrap(project_root=self.data, source_root=self.root, mode="writer")


class ReviewedSourceDeltaTest(unittest.TestCase):
    setUp = SourceTreeBoundaryTest.setUp
    git = SourceTreeBoundaryTest.git
    commit = SourceTreeBoundaryTest.commit
    receipt = SourceTreeBoundaryTest.receipt

    def archive(self, name):
        git = sealer._git_record(self.root, allow_working_tree=True)
        path = self.temp / name
        sealer._write_source_archive(self.root, path, git)
        return {"git": git, "source_archive": sealer._source_archive_record(path, git)}

    def test_cross_commit_delta_is_exact_and_unreviewed_changes_are_rejected(self):
        before = self.archive("before.tar")
        path = self.root / "src/service.py"
        old = path.read_bytes()
        path.write_text("VALUE = 2\n")
        module = self.root / source.MODULE
        module.parent.mkdir(parents=True)
        module.write_bytes(source._LOADED_SOURCE)
        self.commit()
        after = self.archive("after.tar")
        with patch.object(forward_recovery, "PROJECT_ROOT", self.root), patch.object(source, "SOURCE_TRANSITIONS", {"src/service.py": (sha(old), sha(path.read_bytes()))}):
            self.assertNotEqual(before["git"]["head"], after["git"]["head"])
            self.assertEqual(source.source_delta(self.root, before, after), source.approved_changes())
            (self.root / "src/provider_transport.py").write_text("UNREVIEWED = True\n")
            self.commit()
            unreviewed = self.archive("unreviewed.tar")
            with self.assertRaisesRegex(source.auth.AuthorizationError, "exact reviewed"):
                source.source_delta(self.root, before, unreviewed)

    def test_unfrozen_pins_and_wrong_parent_cannot_authorize_a_release(self):
        with patch.object(source, "SOURCE_TRANSITIONS", {}), self.assertRaises(source.auth.AuthorizationError):
            source.approved_changes()
        with self.assertRaisesRegex(source.auth.AuthorizationError, "parent"):
            source._parent({"sha256": "b" * 64}, {"build_reference": {"sha256": "b" * 64}})


class HistoricalAccountTest(unittest.TestCase):
    def test_old_account_self_hash_is_fixed_independently_of_loaded_verifier(self):
        from v8 import account_code_successor as account
        self.assertNotEqual(account.HISTORICAL_MODULE_SHA256, sha(account._LOADED_SOURCE))
        self.assertEqual(account.approved_changes(historical=True)[account.MODULE]["after_sha256"], account.HISTORICAL_MODULE_SHA256)
        self.assertEqual(account.approved_changes()[account.MODULE]["after_sha256"], sha(account._LOADED_SOURCE))

    def test_historical_account_boundary_still_rejects_changed_authority(self):
        from tests import test_v8_account_code_successor as prior
        fixture = prior.AccountPortableLedgerTest(methodName="runTest")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.assertEqual(fixture.verify(), fixture.proof)
        altered = copy.deepcopy(fixture.proof)
        altered["plan_payload"]["operations"] = []
        fixture.fixture.seal(altered)
        with self.assertRaises(source.auth.AuthorizationError):
            fixture.verify(altered)


class SourceSuccessorIntegrationTest(unittest.TestCase):
    def test_real_account_to_frozen_source_preserves_authority_and_ignores_developer_edits(self):
        from tests import test_v8_capture_code_successor as original
        from tests.test_v8_account_code_release_integration import AccountReleaseIntegrationTest
        from v8 import account_code_successor as account, capture_code_successor as code
        root = Path(__file__).resolve().parents[1]
        fixture = original.CaptureCodeSuccessorTest(methodName="runTest")
        self.addCleanup(fixture.doCleanups)
        checks = AccountReleaseIntegrationTest(methodName="runTest")
        full_critical = tuple(sealer.V20_CRITICAL_FILES)
        with fixture.ready(operations=("douyin_user_posts", "douyin_video_detail", "douyin_video_statistics")) as case:
            project, connection = case["project"], case["connection"]
            mirrors = project.parent / "source-test-mirrors"
            mirrors.mkdir(mode=0o700)
            first = fixture.seal(case)
            code.issue_decision(connection, project_root=project, build_path=first, mirror_root=mirrors, at=case["at"])
            business = project / code.BUSINESS
            business.write_bytes(business.read_bytes() + b"\n# temporary runtime-v2 fixture\n")
            runtime_logs = checks._logs(case["logs"], first.parent.parent, code.V2_CHECKS)
            with patch.dict(os.environ, {"DCAR_LOADED_BUILD_RECEIPT": str(first)}):
                plan = code.prepare_plan(connection, project_root=project, previous_build=first,
                    evidence_dir=first.parent.parent / "source-runtime-v2", tests=runtime_logs,
                    actor="fixture", reason="isolated historical runtime fixture", at=case["at"])
                case.update(previous=first, plan=plan, logs=runtime_logs)
                runtime_build = fixture.seal(case)
                code.issue_decision(connection, project_root=project, build_path=runtime_build, mirror_root=mirrors, at=case["at"])
            api = project / "src/dcar_eval/v8/api.py"
            before_api = sha(api.read_bytes())
            api.write_bytes(api.read_bytes() + b"\n# reviewed historical account fixture\n")
            account_pairs = {"src/dcar_eval/v8/api.py": (before_api, sha(api.read_bytes()))}
            for name in (account.MODULE, "src/dcar_eval/v8/capture_code_successor.py"):
                target = project / name
                old = sha(target.read_bytes()) if target.exists() else None
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(root / name, target)
                if name != account.MODULE:
                    account_pairs[name] = (old, sha(target.read_bytes()))
            account_logs = checks._logs(runtime_logs, runtime_build.parent.parent, account.REQUIRED_CHECKS)
            with patch.object(account, "PARENT_BUILD_SHA256", sha(runtime_build.read_bytes())), patch.object(account, "SOURCE_TRANSITIONS", account_pairs), patch.object(account, "HISTORICAL_MODULE_SHA256", sha(account._LOADED_SOURCE)), patch.object(sealer, "V20_CRITICAL_FILES", sealer.V20_ACCOUNT_CRITICAL_FILES), patch.dict(os.environ, {"DCAR_LOADED_BUILD_RECEIPT": str(runtime_build)}):
                plan = code.prepare_plan(connection, project_root=project, previous_build=runtime_build,
                    evidence_dir=runtime_build.parent.parent / "source-account", tests=account_logs,
                    actor="fixture", reason="isolated account fixture", at=case["at"], transition=account.TRANSITION)
                case.update(previous=runtime_build, plan=plan, logs=account_logs)
                account_build = fixture.seal(case)
                account_proof = code.issue_decision(connection, project_root=project, build_path=account_build, mirror_root=mirrors, at=case["at"])
                frozen = project.parent / "frozen-source"
                shutil.copytree(project, frozen)
                pairs = {}
                for name in (source.MODULE, "src/dcar_eval/v8/runtime_paths.py"):
                    target = frozen / name
                    old = sha(target.read_bytes()) if target.exists() else None
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(root / name, target)
                    if name != source.MODULE:
                        pairs[name] = (old, sha(target.read_bytes()))
                logs = checks._logs(account_logs, account_build.parent.parent, source.REQUIRED_CHECKS)
                environment = {"DCAR_PROJECT_ROOT": str(project), "DCAR_WRITER_SOURCE_ROOT": str(frozen), "DCAR_LOADED_BUILD_RECEIPT": str(account_build)}
                with patch.object(source, "PARENT_BUILD_SHA256", sha(account_build.read_bytes())), patch.object(source, "SOURCE_TRANSITIONS", pairs), patch.object(sealer, "V20_CRITICAL_FILES", full_critical), patch.dict(os.environ, environment):
                    plan = code.prepare_plan(connection, project_root=project, previous_build=account_build,
                        evidence_dir=account_build.parent.parent / "source-isolation", tests=logs,
                        actor="fixture", reason="isolated source release", at=case["at"], transition=source.TRANSITION)
                    case.update(previous=account_build, plan=plan, logs=logs)
                    build = fixture.seal(case)
                    with self.assertRaisesRegex(source.auth.AuthorizationError, "decision"):
                        code.current_proof(connection, project_root=project, build_path=build, at=case["at"])
                    proof = code.issue_decision(connection, project_root=project, build_path=build, mirror_root=mirrors, at=case["at"])
                    self.assertEqual(proof["contract"], source.PROOF)
                    self.assertEqual(proof["origin_runtime_bindings"], account_proof["origin_runtime_bindings"])
                    self.assertEqual(code.validate_portable(connection, proof, deployment=case["accepted"], at=case["at"]), proof)
                    api.write_bytes(api.read_bytes() + b"\n# unrelated developer edit\n")
                    subprocess.run(["git", "-C", str(project), "add", "-A"], check=True, capture_output=True)
                    subprocess.run(["git", "-C", str(project), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "developer edit"], check=True, capture_output=True)
                    self.assertEqual(code.current_proof(connection, project_root=project, build_path=build, at=case["at"]), proof)
                    # Source verification must not need the developer's Git
                    # object store after a release has its own complete copy.
                    shutil.move(str(project / ".git"), str(project.parent / "developer-git-saved"))
                    self.assertEqual(code.current_proof(connection, project_root=project, build_path=build, at=case["at"]), proof)
                    self.assertEqual(case["before"], [tuple(row) for row in connection.execute("SELECT * FROM deployment_readiness_receipts ORDER BY id")])
                    self.assertEqual(case["decision_path"].read_bytes(), case["original_decision"])
                    for table in ("provider_usage", "provider_request_start_events", "fetch_attempts"):
                        self.assertEqual(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0], 0)
                    (frozen / source.MODULE).write_bytes(b"# corrupted frozen source\n")
                    with self.assertRaises((ValueError, RuntimeError)):
                        code.current_proof(connection, project_root=project, build_path=build, at=case["at"])
