from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import v8.storage as storage
from tests import test_install_writer_database_candidate as install_fixture


safety = install_fixture.installer
migrator = install_fixture.migrator


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "restore_writer_database_backup.py"
SPEC = importlib.util.spec_from_file_location(
    "restore_writer_database_backup",
    SCRIPT,
)
assert SPEC is not None and SPEC.loader is not None
restorer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = restorer
SPEC.loader.exec_module(restorer)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class WriterDatabaseBackupRestoreTest(unittest.TestCase):
    def _layout(
        self,
        root: Path,
        *,
        sidecars: bool = False,
    ) -> dict[str, Path]:
        project = root / "project"
        external = root / "external"
        formal = project / "app" / "data" / "dcar_insight.sqlite3"
        formal_backups = formal.parent / "backups"
        freeze = project / "runtime" / "operator-freeze.lock"
        verified_backup = external / "backups" / "source-v15.sqlite3"
        backup_receipt = external / "receipts" / "backup.json"
        candidate = external / "candidates" / "candidate-v16.sqlite3"
        migration_receipt = external / "receipts" / "migration.json"
        migration_lock = external / "locks" / "migration.lock"
        restore_receipt = external / "receipts" / "restore.json"
        rollback_directory = formal_backups / "restore-001"
        for directory in (
            formal.parent,
            formal_backups,
            freeze.parent,
            verified_backup.parent,
            backup_receipt.parent,
            candidate.parent,
            migration_lock.parent,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        migration_lock.parent.chmod(0o700)
        freeze.write_text("frozen\n", encoding="utf-8")
        freeze.chmod(0o600)
        fixture = install_fixture.WriterDatabaseCandidateInstallerTest(
            methodName="runTest"
        )
        fixture._build_v15_database(formal)

        with patch.multiple(
            migrator,
            PROJECT_ROOT=project,
            FORMAL_DATABASE=formal,
            CANONICAL_OPERATOR_FREEZE_LOCK=freeze,
        ):
            migrator.prepare_verified_backup(
                source_database=formal,
                backup=verified_backup,
                expected_source_sha256=_sha256(formal),
                from_version=15,
                freeze_lock=freeze,
                migration_lock=migration_lock,
                receipt=backup_receipt,
                holder_checker=lambda _: [],
            )
            migrator.build_migration_candidate(
                source_database=formal,
                candidate=candidate,
                expected_source_sha256=_sha256(formal),
                from_version=15,
                to_version=16,
                freeze_lock=freeze,
                migration_lock=migration_lock,
                backup_receipt=backup_receipt,
                receipt=migration_receipt,
                holder_checker=lambda _: [],
            )
        os.replace(candidate, formal)
        formal.chmod(0o600)
        if sidecars:
            Path(f"{formal}-wal").write_bytes(b"")
            Path(f"{formal}-shm").write_bytes(b"original-v16-shm")
        return {
            "project": project,
            "formal": formal,
            "formal_backups": formal_backups,
            "freeze": freeze,
            "backup": verified_backup,
            "backup_receipt": backup_receipt,
            "migration_receipt": migration_receipt,
            "migration_lock": migration_lock,
            "rollback": rollback_directory,
            "receipt": restore_receipt,
        }

    def _arguments(self, layout: dict[str, Path]) -> dict[str, Any]:
        return {
            "formal_database": layout["formal"],
            "expected_formal_v16_sha256": _sha256(layout["formal"]),
            "backup_receipt": layout["backup_receipt"],
            "expected_backup_receipt_sha256": _sha256(
                layout["backup_receipt"]
            ),
            "rollback_directory": layout["rollback"],
            "receipt": layout["receipt"],
            "freeze_lock": layout["freeze"],
        }

    def _patches(self, layout: dict[str, Path]) -> tuple[Any, Any]:
        return (
            patch.multiple(
                restorer,
                FORMAL_DATABASE=layout["formal"],
                FORMAL_BACKUP_ROOT=layout["formal_backups"],
                CANONICAL_OPERATOR_FREEZE_LOCK=layout["freeze"],
            ),
            patch.multiple(
                safety,
                PROJECT_ROOT=layout["project"],
                FORMAL_DATABASE=layout["formal"],
                FORMAL_BACKUP_ROOT=layout["formal_backups"],
                CANONICAL_OPERATOR_FREEZE_LOCK=layout["freeze"],
            ),
        )

    def _assert_version(self, path: Path, version: int) -> None:
        connection = storage.connect(path, read_only=True)
        try:
            self.assertEqual(
                storage.require_schema_compatibility(
                    connection,
                    supported_versions=frozenset({version}),
                ),
                version,
            )
        finally:
            connection.close()

    def _assert_lock_released(self, path: Path) -> None:
        descriptor = os.open(path, os.O_RDWR)
        parent_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            fcntl.flock(parent_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)
            os.close(parent_descriptor)

    def test_success_atomically_restores_v15_and_archives_exact_v16(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), sidecars=True)
            v16_sha = _sha256(layout["formal"])
            backup_sha = _sha256(layout["backup"])
            backup_receipt_sha = _sha256(layout["backup_receipt"])
            shm_sha = _sha256(Path(f"{layout['formal']}-shm"))
            first, second = self._patches(layout)
            holders = MagicMock(return_value=[])
            with first, second:
                result = restorer.restore_verified_backup(
                    **self._arguments(layout),
                    holder_checker=holders,
                )

            self.assertEqual(result["status"], "restored_v15")
            self._assert_version(layout["formal"], 15)
            archived = layout["rollback"] / layout["formal"].name
            self.assertEqual(_sha256(archived), v16_sha)
            self.assertEqual(
                _sha256(layout["rollback"] / f"{layout['formal'].name}-shm"),
                shm_sha,
            )
            self.assertFalse(Path(f"{layout['formal']}-wal").exists())
            self.assertFalse(Path(f"{layout['formal']}-shm").exists())
            self.assertEqual(_sha256(layout["backup"]), backup_sha)
            self.assertEqual(_sha256(layout["backup_receipt"]), backup_receipt_sha)
            self.assertEqual(
                json.loads(layout["receipt"].read_text(encoding="utf-8")),
                result,
            )
            self.assertGreaterEqual(holders.call_count, 3)
            self._assert_lock_released(layout["migration_lock"])

    def test_final_lock_binding_failure_restores_v16_before_context_exit(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), sidecars=True)
            v16_sha = _sha256(layout["formal"])
            wal_sha = _sha256(Path(f"{layout['formal']}-wal"))
            shm_sha = _sha256(Path(f"{layout['formal']}-shm"))
            backup_sha = _sha256(layout["backup"])
            backup_receipt_sha = _sha256(layout["backup_receipt"])
            original_lock_context = safety._exclusive_existing_migration_lock

            @contextmanager
            def replace_before_commit(path: Path) -> Any:
                with original_lock_context(path) as lease:
                    class CommitLease:
                        identity = lease.identity

                        def verify_for_commit(self) -> None:
                            displaced = path.with_suffix(".commit-displaced")
                            path.replace(displaced)
                            path.write_bytes(safety.MIGRATION_LOCK_PAYLOAD)
                            path.chmod(0o600)
                            lease.verify_for_commit()

                    yield CommitLease()

            first, second = self._patches(layout)
            with (
                first,
                second,
                patch.object(
                    safety,
                    "_exclusive_existing_migration_lock",
                    replace_before_commit,
                ),
                self.assertRaisesRegex(
                    safety.CandidateInstallError,
                    "original v16 was restored",
                ),
            ):
                restorer.restore_verified_backup(
                    **self._arguments(layout),
                    holder_checker=lambda _: [],
                )

            self.assertEqual(_sha256(layout["formal"]), v16_sha)
            self.assertEqual(_sha256(Path(f"{layout['formal']}-wal")), wal_sha)
            self.assertEqual(_sha256(Path(f"{layout['formal']}-shm")), shm_sha)
            self.assertEqual(_sha256(layout["backup"]), backup_sha)
            self.assertEqual(
                _sha256(layout["backup_receipt"]),
                backup_receipt_sha,
            )
            self.assertFalse(layout["receipt"].exists())
            self._assert_version(layout["formal"], 16)
            self._assert_lock_released(layout["migration_lock"])

    def test_every_restore_checkpoint_returns_to_exact_original_v16(self) -> None:
        for checkpoint in restorer.RESTORE_CHECKPOINTS:
            with (
                self.subTest(checkpoint=checkpoint),
                tempfile.TemporaryDirectory() as temporary,
            ):
                layout = self._layout(Path(temporary), sidecars=True)
                v16_sha = _sha256(layout["formal"])
                wal_sha = _sha256(Path(f"{layout['formal']}-wal"))
                shm_sha = _sha256(Path(f"{layout['formal']}-shm"))
                backup_sha = _sha256(layout["backup"])
                backup_receipt_sha = _sha256(layout["backup_receipt"])

                def fail_at(name: str) -> None:
                    if name == checkpoint:
                        raise RuntimeError(f"fault:{checkpoint}")

                first, second = self._patches(layout)
                with (
                    first,
                    second,
                    self.assertRaisesRegex(
                        safety.CandidateInstallError,
                        "original v16 was restored",
                    ),
                ):
                    restorer.restore_verified_backup(
                        **self._arguments(layout),
                        holder_checker=lambda _: [],
                        fault_injector=fail_at,
                    )
                self.assertEqual(_sha256(layout["formal"]), v16_sha)
                self.assertEqual(_sha256(Path(f"{layout['formal']}-wal")), wal_sha)
                self.assertEqual(_sha256(Path(f"{layout['formal']}-shm")), shm_sha)
                self.assertEqual(_sha256(layout["backup"]), backup_sha)
                self.assertEqual(
                    _sha256(layout["backup_receipt"]),
                    backup_receipt_sha,
                )
                self.assertFalse(layout["receipt"].exists())
                self._assert_version(layout["formal"], 16)
                self._assert_lock_released(layout["migration_lock"])

    def test_restore_contract_rejects_tampering_holders_and_existing_receipt(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            arguments = self._arguments(layout)
            v16_sha = _sha256(layout["formal"])
            first, second = self._patches(layout)
            with first, second:
                with self.assertRaisesRegex(
                    safety.CandidateInstallError,
                    "handles",
                ):
                    restorer.restore_verified_backup(
                        **arguments,
                        holder_checker=lambda _: [
                            {"command": "python", "pid": 123, "descriptor": "4r"}
                        ],
                    )
            self.assertEqual(_sha256(layout["formal"]), v16_sha)

            layout["receipt"].write_text("operator-owned", encoding="utf-8")
            first, second = self._patches(layout)
            with first, second:
                with self.assertRaisesRegex(
                    safety.CandidateInstallError,
                    "receipt path must be new",
                ):
                    restorer.restore_verified_backup(
                        **arguments,
                        holder_checker=lambda _: [],
                    )
            self.assertEqual(
                layout["receipt"].read_text(encoding="utf-8"),
                "operator-owned",
            )
            self.assertEqual(_sha256(layout["formal"]), v16_sha)

    def test_rebinds_v16_inputs_and_receipt_lock_immediately_before_cutover(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))

            def mutate_after_materialization(name: str) -> None:
                if name == "after_preflight":
                    with storage.connect(layout["formal"]) as connection:
                        connection.execute(
                            "UPDATE schema_migrations SET applied_at=? WHERE version=16",
                            ("2099-01-01T00:00:00Z",),
                        )

            first, second = self._patches(layout)
            with first, second:
                with self.assertRaisesRegex(
                    safety.CandidateInstallError,
                    "inputs changed before cutover",
                ):
                    restorer.restore_verified_backup(
                        **self._arguments(layout),
                        holder_checker=lambda _: [],
                        fault_injector=mutate_after_materialization,
                    )
            self.assertFalse(layout["rollback"].exists())
            self.assertFalse(layout["receipt"].exists())

        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            original_lock_context = safety._exclusive_existing_migration_lock

            @contextmanager
            def replace_then_lock(path: Path) -> Any:
                displaced = path.with_suffix(".displaced")
                path.replace(displaced)
                path.write_bytes(safety.MIGRATION_LOCK_PAYLOAD)
                path.chmod(0o600)
                with original_lock_context(path) as identity:
                    yield identity

            first, second = self._patches(layout)
            with (
                first,
                second,
                patch.object(
                    safety,
                    "_exclusive_existing_migration_lock",
                    replace_then_lock,
                ),
                self.assertRaisesRegex(
                    safety.CandidateInstallError,
                    "held migration lock",
                ),
            ):
                restorer.restore_verified_backup(
                    **self._arguments(layout),
                    holder_checker=lambda _: [],
                )
            self._assert_version(layout["formal"], 16)
            self.assertFalse(layout["rollback"].exists())
            self.assertFalse(layout["receipt"].exists())

    def test_rollback_removes_sidecars_that_did_not_exist_on_original_v16(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), sidecars=False)
            v16_sha = _sha256(layout["formal"])

            def create_rogue_v15_sidecars(name: str) -> None:
                if name == "after_v15_installed":
                    Path(f"{layout['formal']}-wal").write_bytes(b"rogue-v15-wal")
                    Path(f"{layout['formal']}-shm").write_bytes(b"rogue-v15-shm")
                    raise RuntimeError("fault-with-rogue-sidecars")

            first, second = self._patches(layout)
            with (
                first,
                second,
                self.assertRaisesRegex(
                    safety.CandidateInstallError,
                    "original v16 was restored",
                ),
            ):
                restorer.restore_verified_backup(
                    **self._arguments(layout),
                    holder_checker=lambda _: [],
                    fault_injector=create_rogue_v15_sidecars,
                )
            self.assertEqual(_sha256(layout["formal"]), v16_sha)
            self._assert_version(layout["formal"], 16)
            for suffix in safety.SQLITE_TRANSIENT_SUFFIXES:
                self.assertFalse(Path(f"{layout['formal']}{suffix}").exists())

    def test_cli_forwards_the_receipt_bound_restore_contract(self) -> None:
        root = Path("/tmp/restore-cli-fixture")
        result = {"status": "restored_v15"}
        stdout = io.StringIO()
        with (
            patch.object(restorer, "restore_verified_backup", return_value=result) as call,
            redirect_stdout(stdout),
        ):
            self.assertEqual(
                restorer.main(
                    [
                        "--formal-db",
                        str(root / "formal.sqlite3"),
                        "--expected-formal-v16-sha256",
                        "a" * 64,
                        "--backup-receipt",
                        str(root / "backup.json"),
                        "--expected-backup-receipt-sha256",
                        "b" * 64,
                        "--rollback-dir",
                        str(root / "rollback"),
                        "--receipt",
                        str(root / "restore.json"),
                        "--freeze-lock",
                        str(root / "freeze.lock"),
                    ]
                ),
                0,
            )
        self.assertEqual(json.loads(stdout.getvalue()), result)
        self.assertEqual(call.call_args.kwargs["formal_database"], root / "formal.sqlite3")


if __name__ == "__main__":
    unittest.main()
