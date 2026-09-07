from __future__ import annotations

import importlib.util
import json
import os
import plistlib
import sqlite3
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/seal_r0_receipts.py"


def _load_script():
    spec = importlib.util.spec_from_file_location("seal_r0_receipts_tested", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


receipts = _load_script()


class ReceiptFixture:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.project = self.root / "project"
        self.home = self.root / "home"
        self.evidence_parent = self.root / "evidence"
        self.evidence = self.evidence_parent / "r0-test"
        self.database = (
            self.home / "Library/Application Support/DcarAIGC/data/dcar_insight.sqlite3"
        )
        self.lock = (
            self.home
            / "Library/Application Support/DcarAIGC/runtime/writer-worker.lock"
        )
        self.plist = self.home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        self.test_logs: dict[str, Path] = {}
        self._create_project()
        self._create_installed_runtime()
        self._create_test_results()

    def _write(self, relative: str, payload: str = "fixture\n") -> None:
        path = self.project / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")

    def _create_project(self) -> None:
        self.project.mkdir()
        for relative in receipts.CRITICAL_FILES:
            self._write(relative.as_posix())
        self._write(
            "config/report_contract_v8_9.json",
            json.dumps(
                {
                    "report_version": receipts.EXPECTED_REPORT_VERSION,
                    "rule_version": receipts.EXPECTED_RULE_VERSION,
                    "evidence_version": receipts.EXPECTED_EVIDENCE_VERSION,
                },
                sort_keys=True,
            ),
        )
        for relative in (
            "data/cache/v8/raw_responses/raw.json",
            "data/cache/v8/media/media.bin",
            "reports/runs/v8/report.json",
            "runtime/campaign.json",
            "app/data/legacy.txt",
        ):
            self._write(relative)
        subprocess.run(
            ["git", "init", "-b", "main", str(self.project)],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        subprocess.run(
            ["git", "-C", str(self.project), "config", "user.email", "r0@test.invalid"],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(self.project), "config", "user.name", "R0 Test"],
            check=True,
        )
        subprocess.run(["git", "-C", str(self.project), "add", "."], check=True)
        subprocess.run(
            ["git", "-C", str(self.project), "commit", "-m", "fixture"],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        self.head = subprocess.run(
            ["git", "-C", str(self.project), "rev-parse", "HEAD"],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        ).stdout.strip()

    def _create_installed_runtime(self) -> None:
        self.database.parent.mkdir(parents=True)
        connection = sqlite3.connect(self.database)
        try:
            connection.executescript(
                """
                PRAGMA user_version=18;
                CREATE TABLE schema_migrations(
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                );
                INSERT INTO schema_migrations(version,name)
                VALUES(18,'matrix-roster-source-routing');
                CREATE TABLE retained_data(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
                INSERT INTO retained_data(value) VALUES('kept');
                """
            )
            connection.commit()
        finally:
            connection.close()
        os.chmod(self.database, 0o600)
        self.lock.parent.mkdir(parents=True)
        self.lock.write_text("stopped\n", encoding="ascii")
        os.chmod(self.lock, 0o600)
        self.plist.parent.mkdir(parents=True)
        payload = {
            "Label": "cn.tj.dcar.writer-worker",
            "WorkingDirectory": str(self.project),
            "ProgramArguments": [
                str(self.project / "deploy/macos/run_writer_worker.sh")
            ],
            "EnvironmentVariables": {
                "DCAR_PROJECT_ROOT": str(self.project),
                "DCAR_V8_DB": str(self.database),
                "DCAR_WRITER_LOCK": str(self.lock),
                "DCAR_V8_REPORTS_ROOT": str(self.project / "reports/runs/v8"),
            },
        }
        self.plist.write_bytes(plistlib.dumps(payload))
        os.chmod(self.plist, 0o600)
        self.evidence_parent.mkdir()
        os.chmod(self.evidence_parent, 0o700)

    def _create_test_results(self) -> None:
        directory = self.root / "test-results"
        directory.mkdir()
        for name in sorted(receipts.REQUIRED_TEST_RESULTS):
            path = directory / f"{name}.log"
            path.write_text(
                f"{name} checks passed\nDCAR_TEST_RESULT name={name} exit=0\n",
                encoding="utf-8",
            )
            os.chmod(path, 0o600)
            self.test_logs[name] = path

    def test_result_specifications(self) -> list[str]:
        return [f"{name}={path}" for name, path in sorted(self.test_logs.items())]


class SealR0ReceiptsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.fixture = ReceiptFixture(Path(self.temporary.name))
        self.holders = patch.object(receipts, "_database_holders", return_value=[])
        self.holders.start()

    def tearDown(self) -> None:
        self.holders.stop()
        self.temporary.cleanup()

    def seal(self, **kwargs) -> dict[str, str]:
        return receipts.seal(
            project_root=self.fixture.project,
            evidence_dir=self.fixture.evidence,
            expected_head=self.fixture.head,
            test_results=self.fixture.test_result_specifications(),
            home=self.fixture.home,
            **kwargs,
        )

    def prepare_postmigration_lineage(self) -> tuple[Path, dict[str, str]]:
        previous = self.seal()
        self.fixture.evidence = self.fixture.evidence_parent / "postmigration"
        with sqlite3.connect(self.fixture.database) as connection:
            connection.execute("PRAGMA user_version=19")
            connection.execute(
                "INSERT INTO schema_migrations VALUES(19,?)",
                (receipts.EXPECTED_TARGET_MIGRATION,),
            )
        previous_payload = json.loads(
            Path(previous["sealed_build_receipt"]).read_text(encoding="utf-8")
        )["payload"]
        database = self.fixture.database.stat()
        candidate = {
            "device": database.st_dev,
            "inode": database.st_ino,
            "sha256": receipts._sha256_file(self.fixture.database),
        }
        directory = self.fixture.evidence_parent / "migration"
        directory.mkdir()
        migration_path = directory / "migration.json"
        migration = {
            "schema_version": "dcar-v19-offline-migration-v1",
            "status": "candidate_ready",
            "from_version": 18,
            "to_version": 19,
            "from_migration": receipts.EXPECTED_FORMAL_MIGRATION,
            "to_migration": receipts.EXPECTED_TARGET_MIGRATION,
            "code_identity": previous_payload["git"]["code_identity"],
            "formal_source": {"path": str(self.fixture.database)},
            "candidate": {"file": candidate},
        }
        migration_path.write_text(json.dumps(migration), encoding="utf-8")
        os.chmod(migration_path, 0o600)
        migration_sha = receipts._sha256_file(migration_path)
        install_path = directory / "install.json"
        install = {
            "schema_version": "dcar-writer-database-v19-install-v1",
            "status": "installed",
            "formal_database": str(self.fixture.database),
            "code_identity": migration["code_identity"],
            "expected": {
                "candidate_sha256": candidate["sha256"],
                "migration_receipt_sha256": migration_sha,
            },
            "migration_receipt": {
                "path": str(migration_path),
                "file": {"sha256": migration_sha},
            },
            "installed": {
                "file": candidate,
                "validation": {
                    "schema_version": 19,
                    "schema_migration": receipts.EXPECTED_TARGET_MIGRATION,
                    "quick_check": "ok",
                    "integrity_check": "ok",
                    "foreign_key_violation_count": 0,
                },
            },
        }
        install_path.write_text(json.dumps(install), encoding="utf-8")
        os.chmod(install_path, 0o600)
        plist = plistlib.loads(self.fixture.plist.read_bytes())
        plist["EnvironmentVariables"]["DCAR_LOADED_BUILD_RECEIPT"] = previous[
            "sealed_build_receipt"
        ]
        self.fixture.plist.write_bytes(plistlib.dumps(plist))
        return install_path, previous

    def test_explicit_schema19_seals_code_update_and_previous_install_lineage(self) -> None:
        install_path, previous = self.prepare_postmigration_lineage()
        before = self.fixture.database.read_bytes()
        result = self.seal(formal_schema=19, install_receipt=install_path)
        runtime = json.loads(Path(result["runtime_root_receipt"]).read_bytes())
        build = json.loads(Path(result["sealed_build_receipt"]).read_bytes())
        self.assertEqual(runtime["payload"]["formal_database"]["user_version"], 19)
        self.assertEqual(
            build["payload"]["schema_contract"],
            {
                "code_schema": 19,
                "formal_schema": 19,
                "transition": "19-to-19",
                "operation": "code_update",
                "source_migration": receipts.EXPECTED_TARGET_MIGRATION,
                "target_migration": receipts.EXPECTED_TARGET_MIGRATION,
            },
        )
        lineage = build["payload"]["postmigration_lineage"]
        self.assertEqual(
            lineage["previous_build_receipt"]["sha256"],
            previous["sealed_build_receipt_sha256"],
        )
        self.assertEqual(
            lineage["install_receipt"]["sha256"], receipts._sha256_file(install_path)
        )
        self.assertEqual(
            lineage["migration_receipt"]["sha256"],
            receipts._sha256_file(install_path.with_name("migration.json")),
        )
        verified = receipts.verify(
            project_root=self.fixture.project,
            evidence_dir=self.fixture.evidence,
            home=self.fixture.home,
            formal_schema=19,
            install_receipt=install_path,
        )
        self.assertEqual(verified["status"], "verified")
        self.assertEqual(self.fixture.database.read_bytes(), before)
        for key in ("runtime_root_receipt", "test_results_receipt", "sealed_build_receipt"):
            metadata = Path(result[key]).stat()
            self.assertEqual(stat.S_IMODE(metadata.st_mode), 0o600)
            self.assertEqual(metadata.st_nlink, 1)

    def test_default_schema18_refuses_schema19_before_evidence_creation(self) -> None:
        self.prepare_postmigration_lineage()
        with self.assertRaisesRegex(receipts.R0ReceiptError, "clean schema18"):
            self.seal()
        self.assertFalse(self.fixture.evidence.exists())

    def test_schema19_selection_refuses_schema18_before_evidence_creation(self) -> None:
        with self.assertRaisesRegex(receipts.R0ReceiptError, "clean schema19"):
            self.seal(formal_schema=19)
        self.assertFalse(self.fixture.evidence.exists())

    def test_verify_rejects_cross_schema_receipts(self) -> None:
        install_path, previous = self.prepare_postmigration_lineage()
        self.seal(formal_schema=19, install_receipt=install_path)
        for evidence, selected in (
            (self.fixture.evidence, 18),
            (Path(previous["sealed_build_receipt"]).parent, 19),
        ):
            with self.subTest(selected=selected), self.assertRaisesRegex(
                receipts.R0ReceiptError, "formal schema selection"
            ):
                receipts.verify(
                    project_root=self.fixture.project,
                    evidence_dir=evidence,
                    home=self.fixture.home,
                    formal_schema=selected,
                    install_receipt=install_path if selected == 19 else None,
                )

    def test_schema19_requires_install_receipt_before_evidence_creation(self) -> None:
        self.prepare_postmigration_lineage()
        with self.assertRaisesRegex(receipts.R0ReceiptError, "install receipt is required"):
            self.seal(formal_schema=19)
        self.assertFalse(self.fixture.evidence.exists())

    def test_schema19_rejects_changed_migration_receipt(self) -> None:
        install_path, _ = self.prepare_postmigration_lineage()
        with install_path.with_name("migration.json").open("a", encoding="utf-8") as handle:
            handle.write("\n")
        with self.assertRaisesRegex(receipts.R0ReceiptError, "migration receipt SHA-256"):
            self.seal(formal_schema=19, install_receipt=install_path)
        self.assertFalse(self.fixture.evidence.exists())

    def test_schema19_rejects_unrelated_install_identity(self) -> None:
        install_path, _ = self.prepare_postmigration_lineage()
        install = json.loads(install_path.read_bytes())
        install["installed"]["file"]["inode"] += 1
        install_path.write_text(json.dumps(install), encoding="utf-8")
        with self.assertRaisesRegex(receipts.R0ReceiptError, "installed database identity"):
            self.seal(formal_schema=19, install_receipt=install_path)
        self.assertFalse(self.fixture.evidence.exists())

    def test_schema19_rejects_inexact_migration_name(self) -> None:
        install_path, _ = self.prepare_postmigration_lineage()
        with sqlite3.connect(self.fixture.database) as connection:
            connection.execute("UPDATE schema_migrations SET name='other' WHERE version=19")
        with self.assertRaisesRegex(receipts.R0ReceiptError, "clean schema19"):
            self.seal(formal_schema=19, install_receipt=install_path)
        self.assertFalse(self.fixture.evidence.exists())

    def test_schema19_keeps_holder_test_log_and_wal_gates(self) -> None:
        install_path, _ = self.prepare_postmigration_lineage()
        with (
            patch.object(receipts, "_database_holders", return_value=["p123"]),
            self.assertRaisesRegex(receipts.R0ReceiptError, "open holder"),
        ):
            self.seal(formal_schema=19, install_receipt=install_path)
        self.assertFalse(self.fixture.evidence.exists())
        backend = self.fixture.test_logs["backend"]
        os.chmod(backend, 0o644)
        try:
            with self.assertRaisesRegex(receipts.R0ReceiptError, "0600"):
                self.seal(formal_schema=19, install_receipt=install_path)
        finally:
            os.chmod(backend, 0o600)
        self.assertFalse(self.fixture.evidence.exists())
        connection = sqlite3.connect(self.fixture.database)
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("INSERT INTO retained_data(value) VALUES('pending')")
            connection.commit()
            with self.assertRaisesRegex(receipts.R0ReceiptError, "uncheckpointed WAL"):
                self.seal(formal_schema=19, install_receipt=install_path)
        finally:
            connection.close()
        self.assertFalse(self.fixture.evidence.exists())

    def test_schema19_verify_detects_install_receipt_drift(self) -> None:
        install_path, _ = self.prepare_postmigration_lineage()
        self.seal(formal_schema=19, install_receipt=install_path)
        with install_path.open("a", encoding="utf-8") as handle:
            handle.write("\n")
        with self.assertRaisesRegex(receipts.R0ReceiptError, "sealed build identity"):
            receipts.verify(
                project_root=self.fixture.project,
                evidence_dir=self.fixture.evidence,
                home=self.fixture.home,
                formal_schema=19,
                install_receipt=install_path,
            )

    def test_cli_formal_schema_defaults_to_18_and_explicitly_accepts_19(self) -> None:
        common = ["--project-root", str(self.fixture.project),
                  "--evidence-dir", str(self.fixture.evidence)]
        for command in ("seal", "verify"):
            extra = (["--expected-head", self.fixture.head, "--test-result", "backend=test"]
                     if command == "seal" else [])
            parser = receipts._parser()
            self.assertEqual(parser.parse_args([command, *common, *extra]).formal_schema, 18)
            self.assertEqual(
                parser.parse_args([command, *common, *extra, "--formal-schema", "19"]).formal_schema,
                19,
            )

    def test_seal_and_verify_three_private_self_hashed_receipts(self) -> None:
        database_before = self.fixture.database.read_bytes()
        lock_before = self.fixture.lock.read_bytes()
        plist_before = self.fixture.plist.read_bytes()
        result = self.seal()
        self.assertEqual(
            set(result),
            {
                "runtime_root_receipt",
                "runtime_root_receipt_sha256",
                "test_results_receipt",
                "test_results_receipt_sha256",
                "sealed_build_receipt",
                "sealed_build_receipt_sha256",
            },
        )
        runtime_path = self.fixture.evidence / receipts.RUNTIME_ROOT_FILENAME
        tests_path = self.fixture.evidence / receipts.TEST_RESULTS_FILENAME
        build_path = self.fixture.evidence / receipts.SEALED_BUILD_FILENAME
        for path in (runtime_path, tests_path, build_path):
            metadata = path.stat()
            self.assertEqual(stat.S_IMODE(metadata.st_mode), 0o600)
            self.assertEqual(metadata.st_nlink, 1)
        runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
        tests = json.loads(tests_path.read_text(encoding="utf-8"))
        build = json.loads(build_path.read_text(encoding="utf-8"))
        self.assertEqual(runtime["contract_version"], receipts.RUNTIME_ROOT_CONTRACT)
        self.assertEqual(runtime["payload"]["action"], "retain")
        self.assertEqual(
            runtime["payload"]["mutation_counts"],
            {"moved_files": 0, "copied_bytes": 0, "deleted_files": 0},
        )
        self.assertEqual(
            set(runtime["payload"]["roots"]),
            {"project", "raw", "media", "reports", "runtime", "app_data"},
        )
        self.assertEqual(build["contract_version"], receipts.SEALED_BUILD_CONTRACT)
        self.assertEqual(tests["contract_version"], receipts.TEST_RESULTS_CONTRACT)
        self.assertEqual(tests["payload"]["status"], "passed")
        self.assertEqual(
            set(tests["payload"]["results"]), receipts.REQUIRED_TEST_RESULTS
        )
        self.assertTrue(
            all(
                result["status"] == "passed" and result["exit_code"] == 0
                for result in tests["payload"]["results"].values()
            )
        )
        self.assertEqual(build["payload"]["git"]["head"], self.fixture.head)
        self.assertEqual(build["payload"]["schema_contract"]["transition"], "18-to-19")
        self.assertEqual(
            build["payload"]["runtime_root_receipt"]["sha256"],
            result["runtime_root_receipt_sha256"],
        )
        self.assertEqual(
            build["payload"]["test_results_receipt"]["sha256"],
            result["test_results_receipt_sha256"],
        )
        verified = receipts.verify(
            project_root=self.fixture.project,
            evidence_dir=self.fixture.evidence,
            home=self.fixture.home,
        )
        self.assertEqual(verified["status"], "verified")
        connection = sqlite3.connect(self.fixture.database)
        try:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone()[0], 18
            )
            self.assertEqual(
                connection.execute("SELECT value FROM retained_data").fetchone()[0],
                "kept",
            )
        finally:
            connection.close()
        self.assertEqual(self.fixture.database.read_bytes(), database_before)
        self.assertEqual(self.fixture.lock.read_bytes(), lock_before)
        self.assertEqual(self.fixture.plist.read_bytes(), plist_before)

    def test_dirty_checkout_is_rejected_before_evidence_creation(self) -> None:
        (self.fixture.project / "src/dcar_eval/v8/storage.py").write_text(
            "dirty\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(receipts.R0ReceiptError, "clean Git checkout"):
            self.seal()
        self.assertFalse(self.fixture.evidence.exists())

    def _dirty_sources(self) -> None:
        self.fixture._write("src/dcar_eval/v8/pipeline.py", "staged source\n")
        subprocess.run(["git", "-C", str(self.fixture.project), "add", "src/dcar_eval/v8/pipeline.py"], check=True)
        self.fixture._write("src/dcar_eval/v8/pipeline.py", "working source\n")
        binary = self.fixture.project / "src/fixture.bin"
        binary.write_bytes(b"\0staged binary\xff")
        subprocess.run(["git", "-C", str(self.fixture.project), "add", "src/fixture.bin"], check=True)
        binary.write_bytes(b"\0working binary\xfe")
        self.fixture._write("tests/new_test.py", "private fixture source\n")
        self.fixture._write("notes/new.txt", "non-source untracked fixture\n")

    def test_working_tree_seal_binds_tests_and_reconstructs_actual_source(self) -> None:
        self._dirty_sources()
        before = receipts._git_record(self.fixture.project, allow_working_tree=True)
        result = self.seal(allow_working_tree=True)
        build = json.loads(Path(result["sealed_build_receipt"]).read_bytes())["payload"]
        tests = json.loads(Path(result["test_results_receipt"]).read_bytes())["payload"]
        self.assertEqual(build["git"], before)
        self.assertEqual(tests["git"], before)
        self.assertEqual({item["path"] for item in before["working_tree"]["untracked_files"]}, {"tests/new_test.py", "notes/new.txt"})
        self.assertNotIn("private fixture source", json.dumps(build))
        archive_path = Path(build["source_archive"]["path"])
        self.assertEqual(archive_path.parent, self.fixture.evidence)
        self.assertEqual(stat.S_IMODE(archive_path.stat().st_mode), 0o600)
        restored = self.fixture.root / "restored"
        subprocess.run(["git", "clone", "--quiet", str(self.fixture.project), str(restored)], check=True)
        with tarfile.open(archive_path, "r:") as archive:
            for name, arguments in (("staged.patch", ["--index"]), ("unstaged.patch", [])):
                body = archive.extractfile(name).read()
                subprocess.run(["git", "-C", str(restored), "apply", *arguments, "-"], input=body, check=True)
            for item in before["working_tree"]["untracked_files"]:
                target = restored / item["path"]
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.extractfile("untracked/" + item["path"]).read())
                os.chmod(target, item["mode"])
        self.assertEqual(receipts._git_record(restored, allow_working_tree=True), before)
        self.assertEqual(receipts.verify(project_root=self.fixture.project, evidence_dir=self.fixture.evidence, home=self.fixture.home)["status"], "verified")

    def test_working_tree_verify_detects_same_named_untracked_test_content_drift(self) -> None:
        self._dirty_sources()
        self.seal(allow_working_tree=True)
        before = receipts.database_safety.code_identity(self.fixture.project)
        self.fixture._write("tests/new_test.py", "changed private fixture source\n")
        # The legacy migration identity does not hash untracked tests. The new
        # seal manifest must close that gap without altering old receipt hashes.
        self.assertEqual(receipts.database_safety.code_identity(self.fixture.project), before)
        with self.assertRaisesRegex(receipts.R0ReceiptError, "Git identity.*drifted"):
            receipts.verify(project_root=self.fixture.project, evidence_dir=self.fixture.evidence, home=self.fixture.home)

    def test_working_tree_verify_detects_untracked_add_delete_and_mode_changes(self) -> None:
        self._dirty_sources()
        self.seal(allow_working_tree=True)
        path = self.fixture.project / "notes/new.txt"
        body, mode = path.read_bytes(), stat.S_IMODE(path.stat().st_mode)
        for mutation in ("add", "delete", "mode"):
            with self.subTest(mutation=mutation):
                extra = self.fixture.project / "tests/extra.py"
                if mutation == "add":
                    extra.write_text("new\n")
                elif mutation == "delete":
                    path.unlink()
                else:
                    os.chmod(path, mode ^ 0o100)
                with self.assertRaises(receipts.R0ReceiptError):
                    receipts.verify(project_root=self.fixture.project, evidence_dir=self.fixture.evidence, home=self.fixture.home)
                extra.unlink(missing_ok=True)
                path.write_bytes(body)
                os.chmod(path, mode)

    def test_working_tree_archive_tamper_is_rejected(self) -> None:
        self._dirty_sources()
        self.seal(allow_working_tree=True)
        path = self.fixture.evidence / receipts.WORKING_TREE_ARCHIVE
        body = path.read_bytes()
        self.assertIn(b"working source", body)
        path.write_bytes(body.replace(b"working source", b"changed source", 1))
        with self.assertRaisesRegex(receipts.R0ReceiptError, "source archive"):
            receipts.verify(project_root=self.fixture.project, evidence_dir=self.fixture.evidence, home=self.fixture.home)

    def test_working_tree_cannot_drop_new_critical_modules(self) -> None:
        self._dirty_sources()
        result = self.seal(allow_working_tree=True)
        path = Path(result["sealed_build_receipt"])
        build = json.loads(path.read_bytes())
        build["payload"]["critical_files"] = receipts._critical_files(self.fixture.project, receipts.LEGACY_CRITICAL_FILES)
        build["payload_sha256"] = receipts._digest(build["payload"])
        path.write_text(json.dumps(build))
        with self.assertRaisesRegex(receipts.R0ReceiptError, "critical file inventory is incomplete"):
            receipts.verify(project_root=self.fixture.project, evidence_dir=self.fixture.evidence, home=self.fixture.home)

    def test_account_status_dependencies_are_all_sealed_and_cannot_use_old_inventory(self) -> None:
        expected = {f"src/dcar_eval/v8/{name}.py" for name in (
            "account_operating_status", "account_operating_receipts", "statistics_scope",
            "operations", "report_inputs", "spu_audience", "system_roster")}
        self.assertEqual({path.as_posix() for path in receipts.ACCOUNT_STATUS_CRITICAL_FILES}, expected)
        self._dirty_sources()
        result = self.seal(allow_working_tree=True)
        path = Path(result["sealed_build_receipt"])
        build = json.loads(path.read_bytes())
        self.assertTrue(expected <= set(build["payload"]["critical_files"]))
        build["payload"]["critical_files"] = receipts._critical_files(self.fixture.project, receipts.PRE_ACCOUNT_STATUS_CRITICAL_FILES)
        build["payload_sha256"] = receipts._digest(build["payload"])
        path.write_text(json.dumps(build))
        with self.assertRaisesRegex(receipts.R0ReceiptError, "critical file inventory is incomplete"):
            receipts.verify(project_root=self.fixture.project, evidence_dir=self.fixture.evidence, home=self.fixture.home)

    def test_working_tree_rejects_untracked_symlink_before_evidence_creation(self) -> None:
        self.fixture._write("tests/safe.py", "fixture\n")
        (self.fixture.project / "tests/link.py").symlink_to(self.fixture.database)
        with self.assertRaisesRegex(receipts.R0ReceiptError, "untracked source.*unsafe"):
            self.seal(allow_working_tree=True)
        self.assertFalse(self.fixture.evidence.exists())

    def test_working_tree_keeps_formal_holder_gate(self) -> None:
        self._dirty_sources()
        with patch.object(receipts, "_database_holders", return_value=["p123"]), self.assertRaisesRegex(receipts.R0ReceiptError, "open holder"):
            self.seal(allow_working_tree=True)
        self.assertFalse(self.fixture.evidence.exists())

    def test_legacy_clean_receipt_critical_inventory_remains_verifiable(self) -> None:
        result = self.seal()
        path = Path(result["sealed_build_receipt"])
        build = json.loads(path.read_bytes())
        build["payload"]["critical_files"] = receipts._critical_files(self.fixture.project, receipts.LEGACY_CRITICAL_FILES)
        build["payload_sha256"] = receipts._digest(build["payload"])
        path.write_text(json.dumps(build))
        self.assertEqual(receipts.verify(project_root=self.fixture.project, evidence_dir=self.fixture.evidence, home=self.fixture.home)["status"], "verified")

    def test_working_tree_cli_is_explicit_and_verification_uses_receipt_mode(self) -> None:
        common = ["seal", "--project-root", str(self.fixture.project), "--evidence-dir", str(self.fixture.evidence),
                  "--expected-head", self.fixture.head, "--test-result", "backend=test"]
        self.assertFalse(receipts._parser().parse_args(common).allow_working_tree)
        self.assertTrue(receipts._parser().parse_args([*common, "--allow-working-tree"]).allow_working_tree)

    def test_formal_database_holder_is_rejected_before_evidence_creation(self) -> None:
        with (
            patch.object(receipts, "_database_holders", return_value=["p123"]),
            self.assertRaisesRegex(receipts.R0ReceiptError, "open holder"),
        ):
            self.seal()
        self.assertFalse(self.fixture.evidence.exists())

    def test_database_validation_does_not_create_wal_sidecars(self) -> None:
        connection = sqlite3.connect(self.fixture.database)
        try:
            self.assertEqual(
                connection.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal"
            )
        finally:
            connection.close()
        for suffix in ("-wal", "-shm"):
            Path(f"{self.fixture.database}{suffix}").unlink(missing_ok=True)

        record = receipts._database_record(self.fixture.database)

        self.assertEqual(record["user_version"], 18)
        self.assertFalse(Path(f"{self.fixture.database}-wal").exists())
        self.assertFalse(Path(f"{self.fixture.database}-shm").exists())

    def test_database_validation_rejects_uncheckpointed_wal(self) -> None:
        connection = sqlite3.connect(self.fixture.database)
        try:
            self.assertEqual(
                connection.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal"
            )
            connection.execute("INSERT INTO retained_data(value) VALUES('pending')")
            connection.commit()
            self.assertGreater(Path(f"{self.fixture.database}-wal").stat().st_size, 0)
            with self.assertRaisesRegex(
                receipts.R0ReceiptError, "uncheckpointed WAL"
            ):
                receipts._database_record(self.fixture.database)
        finally:
            connection.close()

    def test_missing_required_test_result_is_rejected_before_evidence_creation(
        self,
    ) -> None:
        specifications = [
            specification
            for specification in self.fixture.test_result_specifications()
            if not specification.startswith("backend=")
        ]
        with self.assertRaisesRegex(receipts.R0ReceiptError, "required test results"):
            receipts.seal(
                project_root=self.fixture.project,
                evidence_dir=self.fixture.evidence,
                expected_head=self.fixture.head,
                test_results=specifications,
                home=self.fixture.home,
            )
        self.assertFalse(self.fixture.evidence.exists())

    def test_existing_evidence_directory_is_never_overwritten(self) -> None:
        first = self.seal()
        runtime_before = Path(first["runtime_root_receipt"]).read_bytes()
        with self.assertRaisesRegex(receipts.R0ReceiptError, "already exists"):
            self.seal()
        self.assertEqual(
            Path(first["runtime_root_receipt"]).read_bytes(), runtime_before
        )

    def test_tampered_receipt_is_rejected(self) -> None:
        self.seal()
        path = self.fixture.evidence / receipts.RUNTIME_ROOT_FILENAME
        value = json.loads(path.read_text(encoding="utf-8"))
        value["payload"]["action"] = "moved"
        path.write_text(json.dumps(value), encoding="utf-8")
        os.chmod(path, 0o600)
        with self.assertRaisesRegex(receipts.R0ReceiptError, "payload SHA-256"):
            receipts.verify(
                project_root=self.fixture.project,
                evidence_dir=self.fixture.evidence,
                home=self.fixture.home,
            )

    def test_tampered_test_log_is_rejected(self) -> None:
        self.seal()
        backend = self.fixture.test_logs["backend"]
        with backend.open("a", encoding="utf-8") as handle:
            handle.write("tampered after passing marker\n")
        with self.assertRaisesRegex(receipts.R0ReceiptError, "passing marker"):
            receipts.verify(
                project_root=self.fixture.project,
                evidence_dir=self.fixture.evidence,
                home=self.fixture.home,
            )

    def test_installed_lock_identity_drift_is_rejected(self) -> None:
        self.seal()
        replacement = self.fixture.lock.with_name("replacement.lock")
        replacement.write_text("replacement\n", encoding="ascii")
        os.chmod(replacement, 0o600)
        os.replace(replacement, self.fixture.lock)
        with self.assertRaisesRegex(
            receipts.R0ReceiptError, "identity or inventory drifted"
        ):
            receipts.verify(
                project_root=self.fixture.project,
                evidence_dir=self.fixture.evidence,
                home=self.fixture.home,
            )

    def test_shallow_project_inventory_ignores_only_git_and_temporary_output(self) -> None:
        project = self.fixture.project
        before = receipts._inventory(project, recursive=False)
        temporary = project / 'tmp'
        temporary.mkdir()
        child = temporary / 'parallel-test'
        child.mkdir()
        output = child / 'output.log'
        output.write_text('parallel test output\n')
        self.assertEqual(receipts._inventory(project, recursive=False), before)
        output.unlink()
        child.rmdir()
        self.assertEqual(receipts._inventory(project, recursive=False), before)
        temporary.rmdir()
        self.assertEqual(receipts._inventory(project, recursive=False), before)
        (project / 'other-output').mkdir()
        self.assertNotEqual(receipts._inventory(project, recursive=False), before)

    def test_recursive_runtime_inventory_still_detects_tmp_content_changes(self) -> None:
        runtime = self.fixture.project / 'runtime'
        before = receipts._inventory(runtime)
        temporary = runtime / 'tmp'
        temporary.mkdir()
        self.assertNotEqual(receipts._inventory(runtime), before)
        with_directory = receipts._inventory(runtime)
        output = temporary / 'state.json'
        output.write_text('{"state":"new"}\n')
        self.assertNotEqual(receipts._inventory(runtime), with_directory)
        with_file = receipts._inventory(runtime)
        output.write_text('{"state":"changed"}\n')
        self.assertNotEqual(receipts._inventory(runtime), with_file)
        output.unlink()
        self.assertNotEqual(receipts._inventory(runtime), with_file)


if __name__ == "__main__":
    unittest.main()
