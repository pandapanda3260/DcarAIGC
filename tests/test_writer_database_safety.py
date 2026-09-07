from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
import unittest
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from unittest.mock import patch

import v8.storage as storage


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import writer_database_safety as safety  # noqa: E402


def load_script(name: str):
    spec = importlib.util.spec_from_file_location(
        f"{name}_contract_test", SCRIPTS / f"{name}.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


COLD_IMPORT_HELP_PROGRAM = r'''
import contextlib
import importlib.util
import io
import os
from pathlib import Path
import sys

sys.dont_write_bytecode = True
root = Path(sys.argv[1])
blocked = {
    "sqlite3.connect", "os.mkdir", "os.remove", "os.rename", "os.rmdir",
    "os.link", "os.symlink", "os.chmod", "os.chown", "os.truncate",
    "subprocess.Popen", "socket.connect", "socket.bind",
}
write_flags = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND

def guard(event, arguments):
    if event in blocked:
        raise AssertionError(f"side effect during import/help: {event}")
    if event == "open":
        filename, mode, flags = arguments
        if isinstance(filename, (str, bytes, os.PathLike)):
            path = os.fsdecode(filename)
            if "/Documents/key/" in path or Path(path).name.startswith(".env"):
                raise AssertionError(f"credential access during import/help: {path}")
        if (isinstance(mode, str) and any(value in mode for value in "wax+")) or (
            isinstance(flags, int) and flags & write_flags
        ):
            raise AssertionError(f"file write during import/help: {filename}")

sys.addaudithook(guard)
for name in (
    "writer_database_safety", "migrate_v8_schema",
    "install_writer_database_candidate", "restore_writer_database_backup",
):
    spec = importlib.util.spec_from_file_location(name, root / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    if name == "writer_database_safety":
        continue
    help_forms = [["--help"]]
    if name == "migrate_v8_schema":
        help_forms += [["prepare-backup", "--help"], ["build-candidate", "--help"]]
    for args in help_forms:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            try:
                module.main(args)
            except SystemExit as error:
                assert error.code == 0, (name, args, error.code)
            else:
                raise AssertionError("help did not exit through argparse")
        assert "usage:" in output.getvalue(), (name, args)
print("four imports and five help forms passed without side effects")
'''


class WriterDatabaseSafetyContractTest(unittest.TestCase):
    def test_contracts_are_frozen_and_version_specific(self) -> None:
        old = safety.LEGACY_V15_V16
        new = safety.MATRIX_V17_V18
        dual = safety.DUAL_V18_V19
        self.assertEqual((old.source.version, old.candidate.version), (15, 16))
        self.assertEqual(old.candidate.migration, "remove-manual-review")
        self.assertEqual((new.source.version, new.candidate.version), (17, 18))
        self.assertEqual(new.source.migration, "optional-account-phone")
        self.assertEqual(new.candidate.migration, "matrix-roster-source-routing")
        self.assertEqual(new.backup_receipt_schema, "dcar-v18-offline-backup-v1")
        self.assertEqual(new.migration_receipt_schema, "dcar-v18-offline-migration-v1")
        self.assertEqual(new.install_receipt_schema, "dcar-writer-database-v18-install-v1")
        self.assertEqual((dual.source.version, dual.candidate.version), (18, 19))
        self.assertEqual(dual.source.migration, "matrix-roster-source-routing")
        self.assertEqual(
            dual.candidate.migration, "dual-acquisition-profile-roster-v1"
        )
        self.assertEqual(dual.backup_receipt_schema, "dcar-v19-offline-backup-v1")
        self.assertEqual(
            dual.migration_receipt_schema, "dcar-v19-offline-migration-v1"
        )
        self.assertEqual(
            dual.install_receipt_schema, "dcar-writer-database-v19-install-v1"
        )
        self.assertEqual(
            dual.restore_receipt_schema, "dcar-writer-database-v18-restore-v1"
        )
        self.assertEqual(
            dual.lock_payload, b"dcar-v19-offline-migration-lock-v1\n"
        )
        self.assertNotEqual(old.allowed_differences_schema, new.allowed_differences_schema)
        self.assertNotEqual(new.allowed_differences_schema, dual.allowed_differences_schema)
        self.assertFalse(safety.requires_code_identity(old))
        self.assertTrue(safety.requires_code_identity(new))
        self.assertTrue(safety.requires_code_identity(dual))
        with self.assertRaises(FrozenInstanceError):
            setattr(new, "backup_receipt_schema", old.backup_receipt_schema)
        with self.assertRaises(FrozenInstanceError):
            setattr(new.source, "version", 15)

    def test_unimplemented_and_unknown_migrations_are_not_executable(self) -> None:
        self.assertIs(safety.contract_for_versions(15, 16), safety.LEGACY_V15_V16)
        self.assertIs(
            safety.contract_for_versions(17, 18, require_implemented=False),
            safety.MATRIX_V17_V18,
        )
        self.assertIs(safety.contract_for_versions(17, 18), safety.MATRIX_V17_V18)
        self.assertIs(safety.contract_for_versions(18, 19), safety.DUAL_V18_V19)
        with (
            patch.object(safety, "IMPLEMENTED_MIGRATIONS", frozenset({(15, 16)})),
            self.assertRaisesRegex(safety.OfflineContractError, "not implemented"),
        ):
            safety.contract_for_versions(17, 18)
        with self.assertRaisesRegex(safety.OfflineContractError, "unsupported"):
            safety.contract_for_versions(16, 17)
        forged = replace(safety.LEGACY_V15_V16, allowed_differences_schema="unchecked")
        with self.assertRaisesRegex(safety.OfflineContractError, "sealed"):
            safety.require_implemented_contract(forged)

    def test_operation_dispatch_selects_and_releases_the_v18_v19_contract(self) -> None:
        class OperationError(RuntimeError):
            pass

        @safety.bind_operation_contract("migration", error_type=OperationError)
        def operation(
            *, source_database: Path, from_version: int, to_version: int
        ) -> safety.OfflineDatabaseContract:
            del source_database, from_version, to_version
            return safety.current_contract()

        with patch.dict(os.environ, {"DCAR_TEST_DENY_FORMAL_DB": "0"}):
            selected = operation(
                source_database=Path("isolated.sqlite3"),
                from_version=18,
                to_version=19,
            )
        self.assertIs(selected, safety.DUAL_V18_V19)
        self.assertIs(safety.current_contract(), safety.LEGACY_V15_V16)
        self.assertIn(
            "18->19", safety._implemented_version_help(backup=False)
        )

    def test_receipts_keep_the_original_version(self) -> None:
        for kind in ("backup", "migration", "install", "restore"):
            with self.subTest(kind=kind):
                old = getattr(safety.LEGACY_V15_V16, f"{kind}_receipt_schema")
                self.assertIs(
                    safety.contract_for_receipt(old, kind=kind),
                    safety.LEGACY_V15_V16,
                )
                new = getattr(safety.MATRIX_V17_V18, f"{kind}_receipt_schema")
                self.assertIs(
                    safety.contract_for_receipt(new, kind=kind), safety.MATRIX_V17_V18,
                )
                dual = getattr(safety.DUAL_V18_V19, f"{kind}_receipt_schema")
                self.assertIs(
                    safety.contract_for_receipt(dual, kind=kind), safety.DUAL_V18_V19,
                )
                with (
                    patch.object(safety, "IMPLEMENTED_MIGRATIONS", frozenset({(15, 16)})),
                    self.assertRaisesRegex(safety.OfflineContractError, "not implemented"),
                ):
                    safety.contract_for_receipt(new, kind=kind)
                with self.assertRaisesRegex(safety.OfflineContractError, "unsupported"):
                    safety.contract_for_receipt("unknown", kind=kind)

    def test_cli_contracts_ignore_new_current_schema_constants(self) -> None:
        with patch.multiple(
            storage,
            SCHEMA_VERSION=99,
            CURRENT_SCHEMA_MIGRATION_NAME="future-schema",
            SCHEMA_MIGRATION_NAMES={99: "future-schema"},
        ):
            for name in ("migrate_v8_schema", "install_writer_database_candidate"):
                with self.subTest(name=name):
                    tool = load_script(name)
                    self.assertIs(tool.OFFLINE_CONTRACT, safety.LEGACY_V15_V16)
                    self.assertEqual(tool.OFFLINE_CONTRACT.source.version, 15)
                    self.assertEqual(tool.OFFLINE_CONTRACT.candidate.version, 16)

    def test_cold_import_and_help_do_not_access_database_or_mutate_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            result = subprocess.run(
                [sys.executable, "-c", COLD_IMPORT_HELP_PROGRAM, str(ROOT)],
                cwd=temporary,
                env={
                    "PATH": os.defpath,
                    "PYTHONPATH": f"{ROOT}{os.pathsep}{ROOT / 'src' / 'dcar_eval'}",
                    "PYTHONDONTWRITEBYTECODE": "1",
                    "DCAR_TEST_DENY_FORMAL_DB": "1",
                    "DCAR_LLM_DISABLED": "1",
                },
                text=True,
                capture_output=True,
                timeout=30,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("four imports and five help forms passed", result.stdout)
            self.assertEqual(list(Path(temporary).iterdir()), [])
