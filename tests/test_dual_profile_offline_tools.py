from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from collections.abc import Callable, Iterator
from contextlib import ExitStack, closing, contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch

import v8.storage as storage
from tests import test_install_writer_database_candidate as install_fixture
from tests import test_v19_schema_migration as schema19_fixture


ROOT = Path(__file__).resolve().parents[1]
installer = install_fixture.installer
migrator = install_fixture.migrator
restorer = install_fixture._load_script(
    "restore_writer_database_backup_dual_profile_tests",
    ROOT / "scripts" / "restore_writer_database_backup.py",
)
shared_safety = installer.shared_safety
CONTRACT = shared_safety.DUAL_V18_V19
CODE_IDENTITY = {"git_head": "c" * 40, "working_tree_sha256": "d" * 64}
STAMP = "2026-09-01T00:00:00Z"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class DualProfileOfflineToolsTest(unittest.TestCase):
    """Exercise the real sealed schema18/schema19 offline state machines."""

    def _build_schema18(self, path: Path) -> None:
        fixture = schema19_fixture.V19SchemaMigrationTest(methodName="runTest")
        fixture.root = path.parent
        fixture._v18(path.name)
        with storage.connect(path) as connection:
            connection.execute(
                "INSERT INTO accounts("
                "id,phone,phone_normalized,operator_name,enabled,created_at,updated_at"
                ") VALUES (10,'13800000010','13800000010','retained',1,?,?)",
                (STAMP, STAMP),
            )
            connection.execute(
                "INSERT INTO account_platform_identities("
                "id,account_id,platform,uid,created_at,updated_at"
                ") VALUES (10,10,'douyin','retained-uid',?,?)",
                (STAMP, STAMP),
            )
            connection.execute(
                "INSERT INTO content_items("
                "id,link_id,platform,canonical_url,account_id,title,"
                "imported_at,created_at,updated_at"
                ") VALUES (10,'DUL001','douyin','https://example.test/dual',"
                "10,'retained-title',?,?,?)",
                (STAMP, STAMP, STAMP),
            )
            connection.commit()
        fixture._seed_bridges(path)
        with sqlite3.connect(path, isolation_level=None) as connection:
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            self.assertEqual(
                str(connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]),
                "delete",
            )
        for suffix in migrator.SQLITE_TRANSIENT_SUFFIXES:
            sidecar = Path(f"{path}{suffix}")
            if sidecar.exists():
                self.assertEqual(sidecar.stat().st_size, 0)
                sidecar.unlink()
        path.chmod(0o600)

    @contextmanager
    def _scenario(self) -> Iterator[dict[str, Path]]:
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
            root = Path(temporary)
            project = root / "project"
            external = root / "external"
            formal = project / "app" / "data" / "dcar_insight.sqlite3"
            layout = {
                "project": project,
                "formal": formal,
                "backups": formal.parent / "backups",
                "freeze": project / "runtime" / "operator-freeze.lock",
                "verified_backup": external / "backups" / "source-v18.sqlite3",
                "candidate": external / "candidates" / "candidate-v19.sqlite3",
                "backup_receipt": external / "receipts" / "backup.json",
                "migration_receipt": external / "receipts" / "migration.json",
                "install_receipt": external / "receipts" / "install.json",
                "restore_receipt": external / "receipts" / "restore.json",
                "migration_lock": external / "locks" / "migration.lock",
                "install_backup": formal.parent / "backups" / "install-001",
                "restore_rollback": formal.parent / "backups" / "restore-001",
            }
            for key in (
                "formal",
                "freeze",
                "verified_backup",
                "candidate",
                "backup_receipt",
                "migration_lock",
            ):
                layout[key].parent.mkdir(parents=True, exist_ok=True)
            layout["backups"].mkdir()
            layout["migration_lock"].parent.chmod(0o700)
            layout["freeze"].write_text("frozen\n", encoding="utf-8")
            layout["freeze"].chmod(0o600)
            self._build_schema18(formal)
            stack.enter_context(
                patch.multiple(
                    migrator,
                    PROJECT_ROOT=project,
                    CANONICAL_OPERATOR_FREEZE_LOCK=layout["freeze"],
                )
            )
            stack.enter_context(
                patch.multiple(
                    installer,
                    PROJECT_ROOT=project,
                    CANONICAL_OPERATOR_FREEZE_LOCK=layout["freeze"],
                    _formal_mutation_lease=install_fixture._isolated_formal_mutation,
                )
            )
            stack.enter_context(
                patch.multiple(
                    restorer,
                    CANONICAL_OPERATOR_FREEZE_LOCK=layout["freeze"],
                )
            )
            stack.enter_context(
                patch.object(
                    restorer.safety,
                    "_formal_mutation_lease",
                    install_fixture._isolated_formal_mutation,
                )
            )
            stack.enter_context(
                patch.object(installer, "_database_handles", return_value=[])
            )
            stack.enter_context(
                patch.object(
                    shared_safety, "code_identity", return_value=dict(CODE_IDENTITY)
                )
            )
            yield layout

    def _backup(self, layout: dict[str, Path], **extra: Any) -> dict[str, Any]:
        return migrator.prepare_verified_backup(
            source_database=layout["formal"],
            backup=layout["verified_backup"],
            expected_source_sha256=_sha256(layout["formal"]),
            from_version=18,
            freeze_lock=layout["freeze"],
            migration_lock=layout["migration_lock"],
            receipt=layout["backup_receipt"],
            isolated=True,
            holder_checker=lambda _: [],
            **extra,
        )

    def _build(self, layout: dict[str, Path], **extra: Any) -> dict[str, Any]:
        return migrator.build_migration_candidate(
            source_database=layout["formal"],
            candidate=layout["candidate"],
            expected_source_sha256=_sha256(layout["formal"]),
            from_version=18,
            to_version=19,
            freeze_lock=layout["freeze"],
            migration_lock=layout["migration_lock"],
            backup_receipt=layout["backup_receipt"],
            receipt=layout["migration_receipt"],
            isolated=True,
            holder_checker=lambda _: [],
            **extra,
        )

    def _install(self, layout: dict[str, Path], **extra: Any) -> dict[str, Any]:
        return installer.install_candidate(
            formal_database=layout["formal"],
            candidate=layout["candidate"],
            migration_receipt=layout["migration_receipt"],
            expected_migration_receipt_sha256=_sha256(
                layout["migration_receipt"]
            ),
            backup_directory=layout["install_backup"],
            receipt=layout["install_receipt"],
            freeze_lock=layout["freeze"],
            **extra,
        )

    def _restore(self, layout: dict[str, Path], **extra: Any) -> dict[str, Any]:
        arguments = {
            "formal_database": layout["formal"],
            "expected_formal_v16_sha256": _sha256(layout["formal"]),
            "backup_receipt": layout["backup_receipt"],
            "expected_backup_receipt_sha256": _sha256(layout["backup_receipt"]),
            "rollback_directory": layout["restore_rollback"],
            "receipt": layout["restore_receipt"],
            "freeze_lock": layout["freeze"],
            "install_receipt": layout["install_receipt"],
            "expected_install_receipt_sha256": _sha256(layout["install_receipt"]),
            "holder_checker": lambda _: [],
        }
        arguments.update(extra)
        return restorer.restore_verified_backup(**arguments)

    def _ready(self, layout: dict[str, Path], *, install: bool = False) -> None:
        self._backup(layout)
        self._build(layout)
        if install:
            self._install(layout)

    def _assert_version(self, path: Path, expected: int) -> None:
        with closing(
            sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
        ) as connection:
            connection.row_factory = sqlite3.Row
            storage.configure_connection_safety(connection)
            self.assertEqual(storage.require_schema_compatibility(connection), expected)
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])

    def _retained_row(self, path: Path) -> tuple[Any, ...]:
        with closing(
            sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
        ) as connection:
            return tuple(
                connection.execute(
                    "SELECT id,link_id,account_id,title FROM content_items WHERE id=10"
                ).fetchone()
            )

    @staticmethod
    def _injected(checkpoint: str) -> Callable[[str], None]:
        def inject(name: str) -> None:
            if name == checkpoint:
                raise RuntimeError(f"injected failure at {checkpoint}")

        return inject

    def test_backup_build_install_and_pristine_restore_are_receipt_bound(
        self,
    ) -> None:
        with self._scenario() as layout:
            source_sha = _sha256(layout["formal"])
            retained = self._retained_row(layout["formal"])
            backup = self._backup(layout)
            migration = self._build(layout)
            self.assertEqual(_sha256(layout["formal"]), source_sha)
            self.assertEqual(backup["schema_version"], CONTRACT.backup_receipt_schema)
            self.assertEqual(
                migration["schema_version"], CONTRACT.migration_receipt_schema
            )
            self.assertEqual(
                migration["lineage"]["schema_version"],
                CONTRACT.allowed_differences_schema,
            )
            self.assertEqual(migration["lineage"]["appended_migration_versions"], [19])
            self.assertEqual(migration["code_identity"], CODE_IDENTITY)
            candidate_sha = _sha256(layout["candidate"])
            self._assert_version(layout["candidate"], 19)
            self.assertEqual(self._retained_row(layout["candidate"]), retained)

            installed = self._install(layout)
            self.assertEqual(installed["schema_version"], CONTRACT.install_receipt_schema)
            self.assertEqual(installed["code_identity"], CODE_IDENTITY)
            self.assertEqual(_sha256(layout["formal"]), candidate_sha)
            self.assertEqual(self._retained_row(layout["formal"]), retained)
            self.assertEqual(
                _sha256(layout["install_backup"] / layout["formal"].name),
                source_sha,
            )
            self.assertFalse(layout["candidate"].exists())
            self._assert_version(layout["formal"], 19)

            restored = self._restore(layout)
            self.assertEqual(restored["schema_version"], CONTRACT.restore_receipt_schema)
            self.assertEqual(restored["status"], "restored_v18")
            self.assertEqual(restored["code_identity"], CODE_IDENTITY)
            self.assertEqual(_sha256(layout["formal"]), backup["backup_sha256"])
            self.assertEqual(
                _sha256(layout["restore_rollback"] / layout["formal"].name),
                candidate_sha,
            )
            self.assertEqual(self._retained_row(layout["formal"]), retained)
            self._assert_version(layout["formal"], 18)
            for key, value in (
                ("backup_receipt", backup),
                ("migration_receipt", migration),
                ("install_receipt", installed),
                ("restore_receipt", restored),
            ):
                self.assertEqual(
                    json.loads(layout[key].read_text(encoding="utf-8")), value
                )
                self.assertEqual(value["code_identity"], CODE_IDENTITY)
            self.assertIs(
                shared_safety.current_contract(), shared_safety.LEGACY_V15_V16
            )

    def test_any_schema19_write_forbids_automatic_schema18_restore(self) -> None:
        with self._scenario() as layout:
            self._ready(layout, install=True)
            installed_sha = _sha256(layout["formal"])
            with sqlite3.connect(layout["formal"]) as connection:
                connection.execute(
                    "UPDATE accounts SET operator_name='post-cutover-write' WHERE id=10"
                )
                connection.commit()
            changed_sha = _sha256(layout["formal"])
            self.assertNotEqual(changed_sha, installed_sha)
            with self.assertRaisesRegex(
                installer.CandidateInstallError,
                "changed after install.*restore is forbidden",
            ):
                self._restore(layout)
            self.assertEqual(_sha256(layout["formal"]), changed_sha)
            self.assertFalse(layout["restore_rollback"].exists())
            self.assertFalse(layout["restore_receipt"].exists())
            self._assert_version(layout["formal"], 19)

    def test_install_and_restore_faults_return_the_exact_previous_database(self) -> None:
        with self.subTest(operation="install"), self._scenario() as layout:
            self._ready(layout)
            source_sha = _sha256(layout["formal"])
            candidate_sha = _sha256(layout["candidate"])
            with self.assertRaisesRegex(
                installer.CandidateInstallError, "injected failure"
            ):
                self._install(
                    layout,
                    fault_injector=self._injected("after_candidate_installed"),
                )
            self.assertEqual(_sha256(layout["formal"]), source_sha)
            self.assertEqual(_sha256(layout["candidate"]), candidate_sha)
            self.assertFalse(layout["install_receipt"].exists())
            self._assert_version(layout["formal"], 18)

        with self.subTest(operation="restore"), self._scenario() as layout:
            self._ready(layout, install=True)
            installed_sha = _sha256(layout["formal"])
            with self.assertRaisesRegex(
                installer.CandidateInstallError, "injected failure"
            ):
                self._restore(
                    layout,
                    fault_injector=self._injected("after_v18_installed"),
                )
            self.assertEqual(_sha256(layout["formal"]), installed_sha)
            self.assertFalse(layout["restore_receipt"].exists())
            self._assert_version(layout["formal"], 19)


if __name__ == "__main__":
    unittest.main()
