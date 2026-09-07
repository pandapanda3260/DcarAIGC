from __future__ import annotations

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
from tests.schema_fixture import initialize_historical_schema


ROOT = Path(__file__).resolve().parents[1]
installer = install_fixture.installer
migrator = install_fixture.migrator
restorer = install_fixture._load_script(
    "restore_writer_database_backup_matrix_tests",
    ROOT / "scripts" / "restore_writer_database_backup.py",
)
shared_safety = installer.shared_safety
CONTRACT = shared_safety.MATRIX_V17_V18
CODE_IDENTITY = {"git_head": "a" * 40, "working_tree_sha256": "b" * 64}
STAMP = "2026-08-28T00:00:00Z"
sha256 = install_fixture._sha256


class MatrixOfflineToolsTest(unittest.TestCase):
    """Exercise the real v17/v18 state machines against disposable databases."""

    def _build_v17_database(self, path: Path) -> None:
        with closing(sqlite3.connect(path)) as connection:
            connection.row_factory = sqlite3.Row
            storage.configure_connection_safety(connection)
            initialize_historical_schema(connection, target_version=17)
            connection.executemany(
                "INSERT INTO accounts(id,phone,phone_normalized,operator_name,enabled,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                [
                    (10, "13800000010", "13800000010", "fixture", 1, STAMP, STAMP),
                    (20, "", None, "fixture-null-phone", 0, STAMP, STAMP),
                ],
            )
            connection.executemany(
                "INSERT INTO account_platform_identities(id,account_id,platform,uid,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?)",
                [
                    (100, 10, "douyin", "10000010", STAMP, STAMP),
                    (101, 10, "xiaohongshu", "0123456789abcdef01234567", STAMP, STAMP),
                    (102, 20, "douyin", "10000020", STAMP, STAMP),
                ],
            )
            connection.executemany(
                "INSERT INTO content_items(id,link_id,platform,canonical_url,account_id,title,imported_at,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                [
                    (
                        1,
                        "MX0001",
                        "douyin",
                        "https://example.test/dy",
                        10,
                        "dy",
                        STAMP,
                        STAMP,
                        STAMP,
                    ),
                    (
                        2,
                        "MX0002",
                        "xiaohongshu",
                        "https://example.test/xhs",
                        10,
                        "xhs",
                        STAMP,
                        STAMP,
                        STAMP,
                    ),
                    (
                        3,
                        "MX0003",
                        "xiaohongshu",
                        "https://example.test/unlinked",
                        None,
                        "unlinked",
                        STAMP,
                        STAMP,
                        STAMP,
                    ),
                ],
            )
            connection.execute(
                "INSERT INTO account_provider_references(account_identity_id,provider,reference_kind,reference_value,created_at,updated_at) "
                "VALUES (100,'TikHub','sec_user_id','fixture-ref',?,?)",
                (STAMP, STAMP),
            )
            connection.execute(
                "INSERT INTO content_metric_snapshots(id,content_id,captured_at,window_key,status,source) "
                "VALUES (7,2,?,'daily','available','xiaohongshu')",
                (STAMP,),
            )
            connection.commit()
            self.assertEqual(storage.require_schema_compatibility(connection), 17)
            self.assertEqual(
                connection.execute("PRAGMA journal_mode").fetchone()[0], "delete"
            )
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
                "verified_backup": external / "backups" / "source-v17.sqlite3",
                "candidate": external / "candidates" / "candidate-v18.sqlite3",
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
            self._build_v17_database(formal)
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
            # The temporary project has no Git metadata. Only the identity
            # provider is fixed; all receipt/code binding checks run normally.
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
            expected_source_sha256=sha256(layout["formal"]),
            from_version=17,
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
            expected_source_sha256=sha256(layout["formal"]),
            from_version=17,
            to_version=18,
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
            expected_migration_receipt_sha256=sha256(layout["migration_receipt"]),
            backup_directory=layout["install_backup"],
            receipt=layout["install_receipt"],
            freeze_lock=layout["freeze"],
            **extra,
        )

    def _restore(self, layout: dict[str, Path], **extra: Any) -> dict[str, Any]:
        arguments = {
            "formal_database": layout["formal"],
            "expected_formal_v16_sha256": sha256(layout["formal"]),
            "backup_receipt": layout["backup_receipt"],
            "expected_backup_receipt_sha256": sha256(layout["backup_receipt"]),
            "rollback_directory": layout["restore_rollback"],
            "receipt": layout["restore_receipt"],
            "freeze_lock": layout["freeze"],
            "install_receipt": layout["install_receipt"],
            "expected_install_receipt_sha256": sha256(layout["install_receipt"]),
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
            self.assertEqual(
                connection.execute("PRAGMA foreign_key_check").fetchall(), []
            )

    def _bootstrap_files(self, layout: dict[str, Path]) -> tuple[Path, Path, Path]:
        directory = layout["backup_receipt"].parent.parent / "roster"
        directory.mkdir()
        source = directory / "official-export.json"
        envelope = directory / "bootstrap-envelope.json"
        raw_root = directory / "retained-originals"
        members = [
            {
                "platform": platform,
                "matrix_account_id": matrix_id,
                "profile_ref": profile,
                "uid": uid,
                "nickname": matrix_id,
                "monitoring_status": "monitored",
                "authorization_status": "unknown",
            }
            for platform, matrix_id, uid, profile in (
                (
                    "douyin",
                    "matrix-fixture-dy",
                    "10000010",
                    "https://www.douyin.com/user/MS4w-fixture-dy",
                ),
                (
                    "xiaohongshu",
                    "matrix-fixture-xhs",
                    "0123456789abcdef01234567",
                    "https://www.xiaohongshu.com/user/profile/0123456789abcdef01234567",
                ),
                (
                    "douyin",
                    "matrix-fixture-paused",
                    "10000020",
                    "https://www.douyin.com/user/MS4w-fixture-paused",
                ),
            )
        ]
        source.write_text(
            json.dumps({"members": members, "exported_at": STAMP}), encoding="utf-8"
        )
        payload = {
            "source_type": "bootstrap_export",
            "source_captured_at": STAMP,
            "require_existing_identities": True,
            "scope": {
                "organization": "Offline integration fixture",
                "coverage": "full",
                "account_scope": "all_added_accounts",
                "platforms": ["douyin", "kuaishou", "wechat_channels", "xiaohongshu"],
            },
            "source_evidence": {
                "kind": "official_export",
                "evidence_kind": "operator_declaration",
                "export_record_id": "offline-bootstrap-fixture",
                "exported_at": STAMP,
                "source_sha256": sha256(source),
                "source_name": source.name,
                "scope_evidence": "Complete synthetic export covering all added fixture accounts.",
            },
            "declared_count": len(members),
            "pagination": {
                "pages": [1],
                "expected_pages": 1,
                "terminal": True,
                "declared_totals": [len(members)],
            },
            "members": members,
        }
        envelope.write_text(
            json.dumps(
                {
                    "payload": payload,
                    "source_path": str(source),
                    "source_sha256": sha256(source),
                }
            ),
            encoding="utf-8",
        )
        return envelope, raw_root, source

    @staticmethod
    def _injected(checkpoint: str, seen: list[str]) -> Callable[[str], None]:
        def inject(name: str) -> None:
            seen.append(name)
            if name == checkpoint:
                raise RuntimeError(f"injected failure at {checkpoint}")

        return inject

    def test_successful_backup_build_install_restore_preserves_both_versions(
        self,
    ) -> None:
        with self._scenario() as layout:
            source_sha = sha256(layout["formal"])
            freeze_sha = sha256(layout["freeze"])
            backup = self._backup(layout)
            self.assertEqual(backup["schema_version"], CONTRACT.backup_receipt_schema)
            self.assertEqual(backup["source_schema_version"], 17)
            self.assertTrue(backup["restore_verified"])
            migration = self._build(layout)
            self.assertEqual(sha256(layout["formal"]), source_sha)
            self.assertEqual(
                migration["schema_version"], CONTRACT.migration_receipt_schema
            )
            self.assertEqual(
                (migration["from_version"], migration["to_version"]), (17, 18)
            )
            self.assertEqual(migration["lineage"]["appended_migration_versions"], [18])
            self.assertEqual(
                set(migration["lineage"]["added_tables"]),
                {
                    "account_roster_snapshots",
                    "account_roster_members",
                    "account_metric_observations",
                },
            )
            candidate_sha = sha256(layout["candidate"])
            installed = self._install(layout)
            self.assertEqual(
                installed["schema_version"], CONTRACT.install_receipt_schema
            )
            self.assertEqual(installed["status"], "installed")
            self.assertEqual(sha256(layout["formal"]), candidate_sha)
            self.assertEqual(
                sha256(layout["install_backup"] / layout["formal"].name), source_sha
            )
            self.assertFalse(layout["candidate"].exists())
            self._assert_version(layout["formal"], 18)
            with closing(
                sqlite3.connect(
                    layout["formal"].as_uri() + "?mode=ro&immutable=1", uri=True
                )
            ) as connection:
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 3
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT id,account_id FROM account_platform_identities ORDER BY id"
                    ).fetchall(),
                    [(100, 10), (101, 21), (102, 20)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT id,account_id FROM content_items ORDER BY id"
                    ).fetchall(),
                    [(1, 10), (2, 21), (3, None)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT id FROM content_metric_snapshots"
                    ).fetchall(),
                    [(7,)],
                )
            restored = self._restore(layout)
            self.assertEqual(
                restored["schema_version"], CONTRACT.restore_receipt_schema
            )
            self.assertEqual(restored["status"], "restored_v17")
            self.assertEqual(sha256(layout["formal"]), backup["backup_sha256"])
            self.assertEqual(
                sha256(layout["restore_rollback"] / layout["formal"].name),
                candidate_sha,
            )
            self._assert_version(layout["formal"], 17)
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
            self.assertEqual(
                restored["unwritten_install_receipt"]["sha256"],
                sha256(layout["install_receipt"]),
            )
            self.assertEqual(sha256(layout["freeze"]), freeze_sha)
            self.assertEqual(
                shared_safety.current_contract(), shared_safety.LEGACY_V15_V16
            )

    def test_backup_fault_after_publication_removes_only_owned_outputs(self) -> None:
        for checkpoint in (
            "after_verified_backup_promoted",
            "after_backup_receipt_written",
        ):
            with self.subTest(checkpoint=checkpoint), self._scenario() as layout:
                source_sha = sha256(layout["formal"])
                seen: list[str] = []
                with self.assertRaisesRegex(
                    migrator.OfflineMigrationError, "injected failure"
                ):
                    self._backup(
                        layout, fault_injector=self._injected(checkpoint, seen)
                    )
                self.assertIn(checkpoint, seen)
                self.assertEqual(sha256(layout["formal"]), source_sha)
                self.assertFalse(layout["verified_backup"].exists())
                self.assertFalse(layout["backup_receipt"].exists())
                self._assert_version(layout["formal"], 17)

    def test_bootstrap_backup_build_install_restore_keeps_existing_identities(
        self,
    ) -> None:
        with self._scenario() as layout:
            envelope, raw_root, original = self._bootstrap_files(layout)
            source_sha = sha256(layout["formal"])
            original_sha = sha256(original)
            backup = self._backup(layout)
            migration = self._build(
                layout, bootstrap_roster=envelope, roster_raw_root=raw_root
            )
            bootstrap = migration["lineage"]["bootstrap"]
            self.assertEqual(bootstrap["member_count"], 3)
            self.assertEqual(bootstrap["source_sha256"], original_sha)
            self.assertEqual(sha256(layout["formal"]), source_sha)
            self.assertEqual(
                len(bootstrap["added_rows"]["account_provider_references"]), 6
            )
            candidate_sha = sha256(layout["candidate"])
            installed = self._install(layout)
            self.assertEqual(
                installed["candidate"]["source_lineage"]["bootstrap"], bootstrap
            )
            with closing(
                sqlite3.connect(
                    layout["formal"].as_uri() + "?mode=ro&immutable=1", uri=True
                )
            ) as connection:
                self.assertEqual(
                    connection.execute(
                        "SELECT id,account_id FROM account_platform_identities ORDER BY id"
                    ).fetchall(),
                    [(100, 10), (101, 21), (102, 20)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT id,enabled FROM accounts ORDER BY id"
                    ).fetchall(),
                    [(10, 1), (20, 0), (21, 1)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT account_identity_id FROM account_roster_members ORDER BY account_identity_id"
                    ).fetchall(),
                    [(100,), (101,), (102,)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM account_provider_references"
                    ).fetchone()[0],
                    7,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM scheduler_runs"
                    ).fetchone()[0],
                    1,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT COUNT(*) FROM provider_raw_responses"
                    ).fetchone()[0],
                    1,
                )
                retained_source = Path(
                    connection.execute(
                        "SELECT local_path FROM provider_raw_responses"
                    ).fetchone()[0]
                )
            self.assertEqual(sha256(retained_source), original_sha)
            restored = self._restore(layout)
            self.assertEqual(restored["status"], "restored_v17")
            self.assertEqual(sha256(layout["formal"]), backup["backup_sha256"])
            self.assertEqual(
                sha256(layout["restore_rollback"] / layout["formal"].name),
                candidate_sha,
            )
            self.assertEqual(sha256(original), original_sha)
            self.assertEqual(sha256(retained_source), original_sha)
            self._assert_version(layout["formal"], 17)

    def test_accepted_bootstrap_rejects_unrelated_appends_and_old_account_edits(
        self,
    ) -> None:
        mutations = (
            (
                "run",
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at) VALUES ('unrelated','fixture','succeeded','2026-08-28T00:00:00Z')",
                storage.SchemaMigrationError,
                "Bootstrap may append exactly one",
            ),
            (
                "raw",
                "INSERT INTO provider_raw_responses(provider,operation,local_path,sha256,byte_size,captured_at) VALUES ('other','unrelated','unrelated.json','"
                + "0" * 64
                + "',2,'2026-08-28T00:00:00Z')",
                storage.SchemaMigrationError,
                "Bootstrap may append exactly one",
            ),
            (
                "account",
                "UPDATE accounts SET operator_name='unauthorized' WHERE id=10",
                storage.SchemaMigrationError,
                "protected rows.*accounts",
            ),
        )
        for label, statement, error_type, message in mutations:
            with self.subTest(mutation=label), self._scenario() as layout:
                envelope, raw_root, _ = self._bootstrap_files(layout)
                self._backup(layout)
                self._build(layout, bootstrap_roster=envelope, roster_raw_root=raw_root)
                source_sha = sha256(layout["formal"])
                with closing(
                    sqlite3.connect(
                        layout["formal"].as_uri() + "?mode=ro&immutable=1", uri=True
                    )
                ) as source:
                    with closing(sqlite3.connect(layout["candidate"])) as candidate:
                        for connection in (source, candidate):
                            connection.row_factory = sqlite3.Row
                            storage.configure_connection_safety(connection)
                        self.assertEqual(
                            storage.validate_v17_v18_lineage(source, candidate)[
                                "bootstrap"
                            ]["member_count"],
                            3,
                        )
                        candidate.execute(statement)
                        candidate.commit()
                        # The same independent validator is called by builder
                        # and installer. This tests the projection guard, not
                        # merely rejection of a stale outer file fingerprint.
                        with self.assertRaisesRegex(error_type, message):
                            storage.validate_v17_v18_lineage(source, candidate)
                with shared_safety.using_contract(CONTRACT):
                    with self.assertRaisesRegex(
                        migrator.OfflineMigrationError, message
                    ):
                        migrator._validate_lineage(
                            layout["formal"], layout["candidate"]
                        )
                    with self.assertRaisesRegex(
                        installer.CandidateInstallError, message
                    ):
                        installer._validate_source_candidate_lineage(
                            layout["formal"], layout["candidate"]
                        )
                self.assertEqual(sha256(layout["formal"]), source_sha)
                self.assertFalse(layout["install_receipt"].exists())

    def test_bootstrap_wrong_original_source_hash_is_rejected_before_publication(
        self,
    ) -> None:
        with self._scenario() as layout:
            envelope, raw_root, original = self._bootstrap_files(layout)
            self._backup(layout)
            before = {
                key: sha256(layout[key])
                for key in ("formal", "verified_backup", "backup_receipt")
            }
            document = json.loads(envelope.read_text(encoding="utf-8"))
            document["source_sha256"] = "f" * 64
            envelope.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(
                migrator.OfflineMigrationError,
                "bootstrap original source SHA-256 differs",
            ):
                self._build(layout, bootstrap_roster=envelope, roster_raw_root=raw_root)
            self.assertEqual({key: sha256(layout[key]) for key in before}, before)
            self.assertNotEqual(sha256(original), document["source_sha256"])
            self.assertFalse(layout["candidate"].exists())
            self.assertFalse(layout["migration_receipt"].exists())
            self.assertFalse(raw_root.exists())

    def test_candidate_fault_after_publication_preserves_source_and_backup(
        self,
    ) -> None:
        for checkpoint in ("after_candidate_promoted", "after_receipt_written"):
            with self.subTest(checkpoint=checkpoint), self._scenario() as layout:
                self._backup(layout)
                before = {
                    key: sha256(layout[key])
                    for key in ("formal", "verified_backup", "backup_receipt", "freeze")
                }
                seen: list[str] = []
                with self.assertRaisesRegex(
                    migrator.OfflineMigrationError, "injected failure"
                ):
                    self._build(layout, fault_injector=self._injected(checkpoint, seen))
                self.assertIn(checkpoint, seen)
                self.assertEqual({key: sha256(layout[key]) for key in before}, before)
                self.assertFalse(layout["candidate"].exists())
                self.assertFalse(layout["migration_receipt"].exists())

    def test_install_critical_checkpoints_restore_exact_v17_and_candidate(self) -> None:
        for checkpoint in (
            "after_source_database_moved",
            "after_candidate_installed",
            "after_receipt_written",
        ):
            with self.subTest(checkpoint=checkpoint), self._scenario() as layout:
                self._ready(layout)
                before = {
                    key: sha256(layout[key])
                    for key in ("formal", "candidate", "verified_backup", "freeze")
                }
                seen: list[str] = []
                with self.assertRaisesRegex(
                    installer.CandidateInstallError, "injected failure"
                ):
                    self._install(
                        layout, fault_injector=self._injected(checkpoint, seen)
                    )
                self.assertIn(checkpoint, seen)
                self.assertEqual({key: sha256(layout[key]) for key in before}, before)
                self.assertFalse(layout["install_receipt"].exists())
                self._assert_version(layout["formal"], 17)

    def test_restore_critical_checkpoints_put_exact_v18_back(self) -> None:
        for checkpoint in (
            "after_v18_database_moved",
            "after_v17_installed",
            "after_restore_receipt_written",
        ):
            with self.subTest(checkpoint=checkpoint), self._scenario() as layout:
                self._ready(layout, install=True)
                before = {
                    key: sha256(layout[key])
                    for key in (
                        "formal",
                        "verified_backup",
                        "install_receipt",
                        "freeze",
                    )
                }
                seen: list[str] = []
                self.assertIn(checkpoint, restorer.MATRIX_RESTORE_CHECKPOINTS)
                with self.assertRaisesRegex(
                    installer.CandidateInstallError, "injected failure"
                ):
                    self._restore(
                        layout, fault_injector=self._injected(checkpoint, seen)
                    )
                self.assertIn(checkpoint, seen)
                self.assertEqual({key: sha256(layout[key]) for key in before}, before)
                self.assertFalse(layout["restore_receipt"].exists())
                self._assert_version(layout["formal"], 18)

    def test_tampered_candidate_is_rejected_before_source_replacement(self) -> None:
        with self._scenario() as layout:
            self._ready(layout)
            source_sha = sha256(layout["formal"])
            with closing(sqlite3.connect(layout["candidate"])) as connection:
                connection.execute(
                    "UPDATE content_items SET title='tampered' WHERE id=2"
                )
                connection.commit()
            with self.assertRaisesRegex(
                installer.CandidateInstallError,
                r"candidate\.file no longer matches",
            ):
                self._install(layout)
            self.assertEqual(sha256(layout["formal"]), source_sha)
            self.assertFalse(layout["install_backup"].exists())
            self.assertFalse(layout["install_receipt"].exists())

    def test_wrong_receipt_schema_version_or_code_identity_is_rejected(self) -> None:
        for field, value in (
            ("schema_version", shared_safety.LEGACY_V15_V16.migration_receipt_schema),
            ("to_version", 17),
            ("code_identity", {"git_head": "c" * 40, "working_tree_sha256": "d" * 64}),
        ):
            with self.subTest(field=field), self._scenario() as layout:
                self._ready(layout)
                source_sha = sha256(layout["formal"])
                receipt = json.loads(
                    layout["migration_receipt"].read_text(encoding="utf-8")
                )
                receipt[field] = value
                layout["migration_receipt"].write_text(
                    json.dumps(receipt), encoding="utf-8"
                )
                # Recompute the expected receipt digest: an internally invalid
                # contract must fail even when its outer digest is supplied.
                with self.assertRaises(installer.CandidateInstallError):
                    self._install(layout)
                self.assertEqual(sha256(layout["formal"]), source_sha)
                self.assertFalse(layout["install_backup"].exists())

    def test_code_change_between_backup_and_build_is_rejected(self) -> None:
        with self._scenario() as layout:
            self._backup(layout)
            source_sha = sha256(layout["formal"])
            with (
                patch.object(
                    shared_safety,
                    "code_identity",
                    return_value={
                        "git_head": "c" * 40,
                        "working_tree_sha256": "d" * 64,
                    },
                ),
                self.assertRaisesRegex(migrator.OfflineMigrationError, "code identity"),
            ):
                self._build(layout)
            self.assertEqual(sha256(layout["formal"]), source_sha)
            self.assertFalse(layout["candidate"].exists())
            self.assertFalse(layout["migration_receipt"].exists())

    def test_v18_new_observation_forbids_automatic_v17_restore(self) -> None:
        with self._scenario() as layout:
            self._ready(layout, install=True)
            installed_sha = sha256(layout["formal"])
            with closing(sqlite3.connect(layout["formal"])) as connection:
                connection.execute(
                    "INSERT INTO account_metric_observations(account_identity_id,source,captured_at,recorded_at,contract_version,payload_json,observation_sha256) "
                    "VALUES (100,'newrank_matrix',?,?,'fixture-v1','{\"followers\":3}',?)",
                    (STAMP, STAMP, "e" * 64),
                )
                connection.commit()
            changed_sha = sha256(layout["formal"])
            self.assertNotEqual(installed_sha, changed_sha)
            # _restore supplies the new formal digest, so rejecting only a
            # stale CLI expected hash cannot make this test pass.
            with self.assertRaisesRegex(
                installer.CandidateInstallError,
                "changed after install.*v17 restore is forbidden",
            ):
                self._restore(layout)
            self.assertEqual(sha256(layout["formal"]), changed_sha)
            self.assertFalse(layout["restore_rollback"].exists())
            self.assertFalse(layout["restore_receipt"].exists())
            self._assert_version(layout["formal"], 18)

    def test_restore_requires_exact_successful_install_receipt(self) -> None:
        for arguments, message in (
            (
                {"install_receipt": None, "expected_install_receipt_sha256": None},
                "exact successful install receipt",
            ),
            (
                {"expected_install_receipt_sha256": "f" * 64},
                "install receipt SHA-256 differs",
            ),
        ):
            with self.subTest(arguments=arguments), self._scenario() as layout:
                self._ready(layout, install=True)
                source_sha = sha256(layout["formal"])
                with self.assertRaisesRegex(installer.CandidateInstallError, message):
                    self._restore(layout, **arguments)
                self.assertEqual(sha256(layout["formal"]), source_sha)
                self.assertFalse(layout["restore_rollback"].exists())
                self.assertFalse(layout["restore_receipt"].exists())


if __name__ == "__main__":
    unittest.main()
