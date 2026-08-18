from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import v8.storage as storage
from tests.schema_fixture import initialize_historical_schema


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "migrate_v8_schema.py"
SPEC = importlib.util.spec_from_file_location("migrate_v8_schema", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
migrator = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = migrator
SPEC.loader.exec_module(migrator)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class OfflineSchemaMigrationTest(unittest.TestCase):
    def _build_v15_database(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        stamp = "2026-08-18T00:00:00Z"
        with storage.connect(path) as connection:
            initialize_historical_schema(connection, target_version=12)
            storage._migrate_v12_to_v13(connection)
            storage._migrate_v13_to_v14(connection)
            storage._migrate_v14_to_v15(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('tax-v15','selling-points-v15','published','fixture',?,?)
                """,
                (stamp, stamp),
            )
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at,activated_at
                ) VALUES (
                    'release-v15','evaluation-v15','selling-points-v15',?,
                    'active',?,?,?
                )
                """,
                ("a" * 64, stamp, stamp, stamp),
            )
            content_id = int(
                connection.execute(
                    """
                    INSERT INTO content_items(
                        link_id,platform,platform_content_id,canonical_url,title,body,
                        content_type,published_at,source_group,source_label,source_path,
                        imported_at,created_at,updated_at
                    ) VALUES (
                        'M00001','douyin','migrate-fixture',
                        'https://example.test/migrate','迁移前标题','迁移前正文',
                        'video','2026-08-17T00:00:00Z','','fixture','fixture.json',?,?,?
                    )
                    """,
                    (stamp, stamp, stamp),
                ).lastrowid
            )
            envelope_id = int(
                connection.execute(
                    """
                    INSERT INTO evidence_envelopes(
                        content_id,schema_version,text_sha256,evidence_sha256,
                        components_json,created_at
                    ) VALUES (?,'evidence-envelope-v3',?,?,'{}',?)
                    """,
                    (content_id, "b" * 64, "c" * 64, stamp),
                ).lastrowid
            )
            automatic_id = int(
                connection.execute(
                    """
                    INSERT INTO evaluation_versions(
                        content_id,evidence_envelope_id,release_id,parent_evaluation_id,
                        review_id,rule_version,taxonomy_version,matcher_rule_sha256,
                        evidence_sha256,evaluation_source,evaluation_status,evidence_level,
                        primary_selling_point_code,selling_point_score,
                        selling_point_included,content_direction,content_automotive_score,
                        audience_automotive_score,acquisition_potential_score,
                        pending_review,payload_json,evaluated_at
                    ) VALUES (
                        ?,?,'release-v15',NULL,NULL,'evaluation-v15',
                        'selling-points-v15',?,?,'automatic','evaluated','V3',
                        NULL,81,1,'media',88,79,66,0,'{"source":"automatic"}',?
                    )
                    """,
                    (content_id, envelope_id, "a" * 64, "c" * 64, stamp),
                ).lastrowid
            )
            queue_id = int(
                connection.execute(
                    """
                    INSERT INTO review_queue(
                        content_id,evaluation_id,reason_code,priority,status,
                        created_at,updated_at,resolved_at
                    ) VALUES (?,?,'fixture-review',50,'resolved',?,?,?)
                    """,
                    (content_id, automatic_id, stamp, stamp, stamp),
                ).lastrowid
            )
            review_id = int(
                connection.execute(
                    """
                    INSERT INTO evaluation_reviews(
                        queue_id,content_id,previous_evaluation_id,resulting_evaluation_id,
                        decision,reason,reviewer,created_at
                    ) VALUES (?,?,?,NULL,'override','fixture','tester',?)
                    """,
                    (queue_id, content_id, automatic_id, stamp),
                ).lastrowid
            )
            manual_id = int(
                connection.execute(
                    """
                    INSERT INTO evaluation_versions(
                        content_id,evidence_envelope_id,release_id,parent_evaluation_id,
                        review_id,rule_version,taxonomy_version,matcher_rule_sha256,
                        evidence_sha256,evaluation_source,evaluation_status,evidence_level,
                        primary_selling_point_code,selling_point_score,
                        selling_point_included,content_direction,content_automotive_score,
                        audience_automotive_score,acquisition_potential_score,
                        pending_review,payload_json,evaluated_at
                    ) VALUES (
                        ?,?,'release-v15',?,?,'evaluation-v15',
                        'selling-points-v15',?,?,'manual_review','evaluated','V3',
                        NULL,93,1,'media',95,84,73,0,'{"source":"manual"}',?
                    )
                    """,
                    (
                        content_id,
                        envelope_id,
                        automatic_id,
                        review_id,
                        "a" * 64,
                        "d" * 64,
                        stamp,
                    ),
                ).lastrowid
            )
            connection.execute(
                "UPDATE evaluation_reviews SET resulting_evaluation_id=? WHERE id=?",
                (manual_id, review_id),
            )
            connection.execute(
                """
                INSERT INTO manual_evidence(
                    review_id,content_id,evidence_type,text_value,sha256,created_at
                ) VALUES (?,?,'review_note','fixture',?,?)
                """,
                (review_id, content_id, "e" * 64, stamp),
            )
            connection.commit()
        connection.close()
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

    def _layout(
        self,
        root: Path,
        *,
        prepare_backup: bool = True,
    ) -> dict[str, Path]:
        project = root / "project"
        external = root / "external"
        source = project / "app" / "data" / "dcar_insight.sqlite3"
        freeze = project / "runtime" / "operator-freeze.lock"
        backup = external / "backups" / "dcar_insight-v15.sqlite3"
        backup_receipt = external / "receipts" / "backup.json"
        candidate = external / "candidates" / "dcar_insight-v16.sqlite3"
        receipt = external / "receipts" / "migration.json"
        migration_lock = external / "locks" / "migration.lock"
        for directory in (
            source.parent,
            freeze.parent,
            backup.parent,
            backup_receipt.parent,
            candidate.parent,
            migration_lock.parent,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        migration_lock.parent.chmod(0o700)
        freeze.write_text('{"status":"frozen"}\n', encoding="utf-8")
        freeze.chmod(0o600)
        self._build_v15_database(source)
        layout = {
            "project": project,
            "source": source,
            "freeze": freeze,
            "backup": backup,
            "backup_receipt": backup_receipt,
            "candidate": candidate,
            "receipt": receipt,
            "migration_lock": migration_lock,
        }
        if prepare_backup:
            with self._constant_patch(layout):
                migrator.prepare_verified_backup(
                    source_database=source,
                    backup=backup,
                    expected_source_sha256=_sha256(source),
                    from_version=15,
                    freeze_lock=freeze,
                    migration_lock=migration_lock,
                    receipt=backup_receipt,
                    holder_checker=lambda _: [],
                )
        return layout

    def _arguments(self, layout: dict[str, Path]) -> dict[str, Any]:
        return {
            "source_database": layout["source"],
            "candidate": layout["candidate"],
            "expected_source_sha256": _sha256(layout["source"]),
            "from_version": 15,
            "to_version": 16,
            "freeze_lock": layout["freeze"],
            "migration_lock": layout["migration_lock"],
            "backup_receipt": layout["backup_receipt"],
            "receipt": layout["receipt"],
        }

    def _backup_arguments(self, layout: dict[str, Path]) -> dict[str, Any]:
        return {
            "source_database": layout["source"],
            "backup": layout["backup"],
            "expected_source_sha256": _sha256(layout["source"]),
            "from_version": 15,
            "freeze_lock": layout["freeze"],
            "migration_lock": layout["migration_lock"],
            "receipt": layout["backup_receipt"],
        }

    def _constant_patch(self, layout: dict[str, Path]):
        return patch.multiple(
            migrator,
            PROJECT_ROOT=layout["project"],
            FORMAL_DATABASE=layout["source"],
            CANONICAL_OPERATOR_FREEZE_LOCK=layout["freeze"],
        )

    def _assert_lock_released(self, path: Path) -> None:
        if not path.exists():
            return
        descriptor = os.open(path, os.O_RDWR)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def _assert_no_candidate_artifacts(self, layout: dict[str, Path]) -> None:
        self.assertFalse(layout["candidate"].exists())
        self.assertFalse(layout["receipt"].exists())
        self.assertEqual(
            list(layout["candidate"].parent.glob(".*.migrating-*")),
            [],
        )

    def _assert_no_backup_artifacts(self, layout: dict[str, Path]) -> None:
        self.assertFalse(layout["backup"].exists())
        self.assertFalse(layout["backup_receipt"].exists())
        self.assertEqual(
            list(layout["backup"].parent.glob(".*.preparing-*")),
            [],
        )
        self.assertEqual(
            list(layout["backup"].parent.glob(".*.restore-check-*")),
            [],
        )

    def test_prepare_backup_creates_restore_verified_receipt_without_source_change(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), prepare_backup=False)
            source_sha = _sha256(layout["source"])
            source_identity = layout["source"].stat().st_ino
            holders = MagicMock(return_value=[])
            with self._constant_patch(layout):
                result = migrator.prepare_verified_backup(
                    **self._backup_arguments(layout),
                    holder_checker=holders,
                )
            self.assertEqual(result["schema_version"], migrator.BACKUP_RECEIPT_SCHEMA)
            self.assertTrue(result["restore_verified"])
            self.assertEqual(_sha256(layout["source"]), source_sha)
            self.assertEqual(layout["source"].stat().st_ino, source_identity)
            self.assertNotEqual(_sha256(layout["backup"]), source_sha)
            self.assertEqual(layout["backup"].stat().st_mode & 0o777, 0o600)
            self.assertEqual(layout["backup_receipt"].stat().st_mode & 0o777, 0o600)
            self.assertEqual(
                json.loads(layout["backup_receipt"].read_text(encoding="utf-8")),
                result,
            )
            self.assertGreaterEqual(holders.call_count, 2)
            self._assert_lock_released(layout["migration_lock"])

    def test_every_backup_fault_checkpoint_cleans_outputs_and_preserves_source(
        self,
    ) -> None:
        for checkpoint in migrator.BACKUP_CHECKPOINTS:
            with (
                self.subTest(checkpoint=checkpoint),
                tempfile.TemporaryDirectory() as temporary,
            ):
                layout = self._layout(Path(temporary), prepare_backup=False)
                source_sha = _sha256(layout["source"])

                def inject(name: str, *, target: str = checkpoint) -> None:
                    if name == target:
                        raise RuntimeError(f"backup-fault:{target}")

                with self._constant_patch(layout):
                    with self.assertRaisesRegex(
                        migrator.OfflineMigrationError,
                        f"backup-fault:{checkpoint}",
                    ):
                        migrator.prepare_verified_backup(
                            **self._backup_arguments(layout),
                            holder_checker=lambda _: [],
                            fault_injector=inject,
                        )
                self.assertEqual(_sha256(layout["source"]), source_sha)
                self._assert_no_backup_artifacts(layout)
                self._assert_lock_released(layout["migration_lock"])

    def test_prepare_backup_fails_closed_on_holders_contract_and_existing_outputs(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), prepare_backup=False)
            arguments = self._backup_arguments(layout)
            holder = [{"command": "python", "pid": 123, "descriptor": "4r"}]
            with self._constant_patch(layout):
                with self.assertRaisesRegex(
                    migrator.OfflineMigrationError,
                    "handles",
                ):
                    migrator.prepare_verified_backup(
                        **arguments,
                        holder_checker=lambda _: holder,
                    )
                with self.assertRaisesRegex(
                    migrator.OfflineMigrationError,
                    "exact --from 15",
                ):
                    migrator.prepare_verified_backup(
                        **{**arguments, "from_version": 14},
                        holder_checker=lambda _: [],
                    )
                with self.assertRaisesRegex(
                    migrator.OfflineMigrationError,
                    "before backup completion",
                ):
                    migrator.prepare_verified_backup(
                        **arguments,
                        holder_checker=MagicMock(
                            side_effect=[[], [], holder]
                        ),
                    )
                self._assert_no_backup_artifacts(layout)
                layout["backup_receipt"].write_text(
                    "operator-owned",
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(
                    migrator.OfflineMigrationError,
                    "receipt path must be new",
                ):
                    migrator.prepare_verified_backup(
                        **arguments,
                        holder_checker=lambda _: [],
                    )
            self.assertFalse(layout["backup"].exists())
            self.assertEqual(
                layout["backup_receipt"].read_text(encoding="utf-8"),
                "operator-owned",
            )

    def test_success_builds_verified_v16_candidate_without_changing_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            source_sha = _sha256(layout["source"])
            source_stat = layout["source"].stat()
            holders = MagicMock(return_value=[])
            with self._constant_patch(layout):
                result = migrator.build_migration_candidate(
                    **self._arguments(layout),
                    holder_checker=holders,
                )

            self.assertEqual(result["status"], "candidate_ready")
            self.assertEqual(result["schema_version"], migrator.MIGRATION_RECEIPT_SCHEMA)
            self.assertEqual(_sha256(layout["source"]), source_sha)
            self.assertEqual(layout["source"].stat().st_ino, source_stat.st_ino)
            self.assertTrue(layout["candidate"].is_file())
            self.assertTrue(layout["receipt"].is_file())
            self.assertNotEqual(_sha256(layout["source"]), _sha256(layout["backup"]))
            self.assertEqual(layout["candidate"].stat().st_mode & 0o777, 0o600)
            self.assertEqual(layout["receipt"].stat().st_mode & 0o777, 0o600)
            self.assertGreaterEqual(holders.call_count, 2)
            for suffix in migrator.SQLITE_TRANSIENT_SUFFIXES:
                self.assertFalse(Path(f"{layout['candidate']}{suffix}").exists())

            source_connection = storage.connect(layout["source"], read_only=True)
            candidate_connection = storage.connect(layout["candidate"], read_only=True)
            try:
                self.assertEqual(
                    storage.require_schema_compatibility(
                        source_connection,
                        supported_versions=frozenset({15}),
                    ),
                    15,
                )
                self.assertEqual(
                    storage.require_schema_compatibility(
                        candidate_connection,
                        supported_versions=frozenset({16}),
                    ),
                    16,
                )
                with self.assertRaises(storage.SchemaMigrationError):
                    storage.require_schema_compatibility(
                        candidate_connection,
                        supported_versions=frozenset({15}),
                    )
                objects = {
                    str(row[0])
                    for row in candidate_connection.execute(
                        "SELECT name FROM sqlite_master WHERE type IN ('table','index')"
                    )
                }
                self.assertFalse(
                    objects
                    & (
                        set(storage._V16_DROPPED_TABLES)
                        | set(storage._V16_REMOVED_INDEXES)
                    )
                )
                self.assertEqual(
                    int(
                        candidate_connection.execute(
                            "SELECT COUNT(*) FROM evaluation_versions"
                        ).fetchone()[0]
                    ),
                    2,
                )
                self.assertEqual(
                    int(
                        candidate_connection.execute(
                            "SELECT COUNT(*) FROM evaluation_versions "
                            "WHERE evaluation_source='manual_review'"
                        ).fetchone()[0]
                    ),
                    1,
                )
            finally:
                candidate_connection.close()
                source_connection.close()
            receipt = json.loads(layout["receipt"].read_text(encoding="utf-8"))
            self.assertEqual(receipt["candidate"]["file"]["sha256"], _sha256(layout["candidate"]))
            self.assertEqual(receipt["lineage"]["manual_review_row_count"], 1)
            self.assertEqual(receipt["lineage"]["added_tables"], [])
            self.assertEqual(receipt["lineage"]["appended_migration_versions"], [16])
            self.assertEqual(receipt["database_handles"], [])
            self._assert_lock_released(layout["migration_lock"])

    def test_every_fault_checkpoint_cleans_candidate_and_preserves_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            source_sha = _sha256(layout["source"])
            for checkpoint in migrator.MIGRATION_CHECKPOINTS:
                with self.subTest(checkpoint=checkpoint), self._constant_patch(layout):
                    def inject(name: str, *, target: str = checkpoint) -> None:
                        if name == target:
                            raise RuntimeError(f"fault:{target}")

                    with self.assertRaisesRegex(
                        migrator.OfflineMigrationError,
                        f"fault:{checkpoint}",
                    ):
                        migrator.build_migration_candidate(
                            **self._arguments(layout),
                            holder_checker=lambda _: [],
                            fault_injector=inject,
                        )
                self.assertEqual(_sha256(layout["source"]), source_sha)
                self._assert_no_candidate_artifacts(layout)
                self._assert_lock_released(layout["migration_lock"])

    def test_transaction_failure_rolls_back_private_copy_and_preserves_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            source_sha = _sha256(layout["source"])

            def fail_inside_transaction(connection: sqlite3.Connection) -> None:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "UPDATE content_items SET title='must-not-escape'"
                )
                raise RuntimeError("injected transaction failure")

            with self._constant_patch(layout):
                with self.assertRaisesRegex(
                    migrator.OfflineMigrationError,
                    "injected transaction failure",
                ):
                    migrator.build_migration_candidate(
                        **self._arguments(layout),
                        holder_checker=lambda _: [],
                        migration_runner=fail_inside_transaction,
                    )
            self.assertEqual(_sha256(layout["source"]), source_sha)
            with sqlite3.connect(layout["source"]) as connection:
                self.assertEqual(
                    str(connection.execute("SELECT title FROM content_items").fetchone()[0]),
                    "迁移前标题",
                )
            self._assert_no_candidate_artifacts(layout)
            self._assert_lock_released(layout["migration_lock"])

    def test_candidate_rejects_any_extra_schema_object(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            source_sha = _sha256(layout["source"])

            def add_destructive_trigger(connection: sqlite3.Connection) -> None:
                storage.initialize_database(connection)
                connection.execute(
                    """
                    CREATE TRIGGER injected_accounts_delete
                    AFTER INSERT ON accounts
                    BEGIN
                        DELETE FROM accounts WHERE id=NEW.id;
                    END
                    """
                )
                connection.commit()

            with self._constant_patch(layout):
                with self.assertRaisesRegex(
                    migrator.OfflineMigrationError,
                    "schema object lineage",
                ):
                    migrator.build_migration_candidate(
                        **self._arguments(layout),
                        holder_checker=lambda _: [],
                        migration_runner=add_destructive_trigger,
                    )
            self.assertEqual(_sha256(layout["source"]), source_sha)
            self._assert_no_candidate_artifacts(layout)
            self._assert_lock_released(layout["migration_lock"])

    def test_holder_checks_fail_closed_before_and_after_copy(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            source_sha = _sha256(layout["source"])
            holder = [{"command": "python", "pid": 123, "descriptor": "4r"}]
            for side_effect in ([holder], [[], holder], [[], [], holder]):
                with self.subTest(side_effect=side_effect), self._constant_patch(layout):
                    checker = MagicMock(side_effect=side_effect)
                    with self.assertRaisesRegex(
                        migrator.OfflineMigrationError,
                        "handles",
                    ):
                        migrator.build_migration_candidate(
                            **self._arguments(layout),
                            holder_checker=checker,
                        )
                self.assertEqual(_sha256(layout["source"]), source_sha)
                self._assert_no_candidate_artifacts(layout)
                self._assert_lock_released(layout["migration_lock"])

    def test_backup_receipt_contract_is_strict(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            original = json.loads(layout["backup_receipt"].read_text(encoding="utf-8"))
            cases = {
                "source_path": str(layout["project"] / "wrong.sqlite3"),
                "source_sha256": "0" * 64,
                "backup_sha256": "1" * 64,
                "restore_verified": False,
                "quick_check": "not-ok",
                "foreign_key_violation_count": 1,
            }
            for field, value in cases.items():
                with self.subTest(field=field):
                    changed = dict(original)
                    changed[field] = value
                    layout["backup_receipt"].write_text(
                        json.dumps(changed, sort_keys=True),
                        encoding="utf-8",
                    )
                    with self._constant_patch(layout):
                        with self.assertRaises(migrator.OfflineMigrationError):
                            migrator.build_migration_candidate(
                                **self._arguments(layout),
                                holder_checker=lambda _: [],
                            )
                    self._assert_no_candidate_artifacts(layout)
            changed = {**original, "unsupported_field": "must-fail"}
            layout["backup_receipt"].write_text(
                json.dumps(changed, sort_keys=True),
                encoding="utf-8",
            )
            with self._constant_patch(layout):
                with self.assertRaisesRegex(
                    migrator.OfflineMigrationError,
                    "exact supported contract",
                ):
                    migrator.build_migration_candidate(
                        **self._arguments(layout),
                        holder_checker=lambda _: [],
                    )
            self._assert_no_candidate_artifacts(layout)
            layout["backup_receipt"].write_text(
                json.dumps(original, sort_keys=True),
                encoding="utf-8",
            )

    def test_backup_data_must_match_source_even_with_a_matching_receipt_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            receipt = json.loads(
                layout["backup_receipt"].read_text(encoding="utf-8")
            )
            with sqlite3.connect(layout["backup"]) as connection:
                connection.execute(
                    "UPDATE content_items SET title='tampered verified backup'"
                )
            receipt["backup_sha256"] = _sha256(layout["backup"])
            receipt["backup_byte_size"] = layout["backup"].stat().st_size
            layout["backup_receipt"].write_text(
                json.dumps(receipt, sort_keys=True),
                encoding="utf-8",
            )

            with self._constant_patch(layout):
                with self.assertRaisesRegex(
                    migrator.OfflineMigrationError,
                    "data projection differs",
                ):
                    migrator.build_migration_candidate(
                        **self._arguments(layout),
                        holder_checker=lambda _: [],
                    )
            self._assert_no_candidate_artifacts(layout)

    def test_exact_versions_freeze_lock_and_migration_lock_are_required(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            arguments = self._arguments(layout)
            with self._constant_patch(layout):
                with self.assertRaisesRegex(
                    migrator.OfflineMigrationError,
                    "exact --from 15 --to 16",
                ):
                    migrator.build_migration_candidate(
                        **{**arguments, "from_version": 14},
                        holder_checker=lambda _: [],
                    )

                layout["freeze"].chmod(0o644)
                with self.assertRaisesRegex(
                    migrator.OfflineMigrationError,
                    "permissions must be 0600",
                ):
                    migrator.build_migration_candidate(
                        **arguments,
                        holder_checker=lambda _: [],
                    )
                layout["freeze"].chmod(0o600)

                lock_descriptor = os.open(
                    layout["migration_lock"],
                    os.O_RDWR | os.O_CREAT,
                    0o600,
                )
                try:
                    fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    with self.assertRaisesRegex(
                        migrator.OfflineMigrationError,
                        "another offline migration holds the lock",
                    ):
                        migrator.build_migration_candidate(
                            **arguments,
                            holder_checker=lambda _: [],
                        )
                finally:
                    fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
                    os.close(lock_descriptor)
            self._assert_no_candidate_artifacts(layout)

    def test_lock_is_identity_bound_and_never_overwrites_foreign_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), prepare_backup=False)
            foreign = b"operator-owned-foreign-file\n"
            layout["migration_lock"].write_bytes(foreign)
            layout["migration_lock"].chmod(0o600)
            with self._constant_patch(layout):
                with self.assertRaisesRegex(
                    migrator.OfflineMigrationError,
                    "initial migration lock path must be new",
                ):
                    migrator.prepare_verified_backup(
                        **self._backup_arguments(layout),
                        holder_checker=lambda _: [],
                    )
            self.assertEqual(layout["migration_lock"].read_bytes(), foreign)
            self._assert_no_backup_artifacts(layout)

            layout["migration_lock"].write_bytes(migrator.MIGRATION_LOCK_PAYLOAD)
            layout["migration_lock"].chmod(0o600)
            replacement = layout["migration_lock"].with_suffix(".replaced")
            with self.assertRaisesRegex(
                migrator.OfflineMigrationError,
                "lock path identity changed",
            ):
                with migrator._exclusive_migration_lock(layout["migration_lock"]):
                    layout["migration_lock"].replace(replacement)
                    layout["migration_lock"].write_bytes(
                        migrator.MIGRATION_LOCK_PAYLOAD
                    )
                    layout["migration_lock"].chmod(0o600)

    def test_filesystem_identity_blocks_aliases_and_project_external_bypass(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = root / "first"
            alias = root / "alias"
            first.write_bytes(b"sentinel")
            os.link(first, alias)
            with self.assertRaisesRegex(
                migrator.OfflineMigrationError,
                "filesystem identity",
            ):
                migrator._require_distinct_paths(
                    (first, alias),
                    label="alias paths",
                )
            self.assertEqual(first.read_bytes(), b"sentinel")

            project = root / "project"
            apparent_external = root / "apparent-external"
            project.mkdir()
            apparent_external.mkdir()
            real_samefile = migrator.os.path.samefile

            def identity_alias(left: Any, right: Any) -> bool:
                if (
                    Path(left).resolve() == apparent_external.resolve()
                    and Path(right).resolve() == project.resolve()
                ):
                    return True
                return real_samefile(left, right)

            with (
                patch.object(migrator, "PROJECT_ROOT", project),
                patch.object(
                    migrator.os.path,
                    "samefile",
                    side_effect=identity_alias,
                ),
                self.assertRaisesRegex(
                    migrator.OfflineMigrationError,
                    "outside the project root",
                ),
            ):
                migrator._require_project_external(
                    apparent_external / "lock",
                    label="migration lock",
                )

    def test_receipt_writer_is_o_excl_and_removes_partial_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            receipt = root / "receipt.json"
            receipt.write_text("operator-owned", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                migrator._write_json_exclusive(receipt, {"status": "new"})
            self.assertEqual(receipt.read_text(encoding="utf-8"), "operator-owned")

            receipt.unlink()
            original_write = migrator.os.write
            calls = 0

            def write_then_stall(descriptor: int, payload: bytes) -> int:
                nonlocal calls
                calls += 1
                if calls == 1:
                    return original_write(descriptor, payload[:7])
                return 0

            with patch.object(migrator.os, "write", side_effect=write_then_stall):
                with self.assertRaisesRegex(OSError, "made no progress"):
                    migrator._write_json_exclusive(receipt, {"status": "partial"})
            self.assertFalse(receipt.exists())

            original_fsync_directory = migrator._fsync_directory
            failed = False

            def fail_first_directory_sync(path: Path) -> None:
                nonlocal failed
                if not failed:
                    failed = True
                    raise OSError("injected receipt directory fsync failure")
                original_fsync_directory(path)

            with patch.object(
                migrator,
                "_fsync_directory",
                side_effect=fail_first_directory_sync,
            ):
                with self.assertRaisesRegex(OSError, "directory fsync failure"):
                    migrator._write_json_exclusive(receipt, {"status": "complete"})
            self.assertTrue(failed)
            self.assertFalse(receipt.exists())

    def test_test_guard_refuses_formal_path_before_opening_database(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fake_formal = Path(temporary) / "never-open.sqlite3"
            with (
                patch.dict(os.environ, {"DCAR_TEST_DENY_FORMAL_DB": "1"}),
                patch.multiple(
                    migrator,
                    DEFAULT_DB=fake_formal,
                    FORMAL_DATABASE=fake_formal,
                ),
            ):
                with self.assertRaisesRegex(
                    migrator.OfflineMigrationError,
                    "test process attempted to open the formal",
                ):
                    migrator.build_migration_candidate(
                        source_database=fake_formal,
                        candidate=Path(temporary) / "candidate.sqlite3",
                        expected_source_sha256="0" * 64,
                        from_version=15,
                        to_version=16,
                        freeze_lock=Path(temporary) / "freeze.lock",
                        migration_lock=Path(temporary) / "migration.lock",
                        backup_receipt=Path(temporary) / "backup.json",
                        receipt=Path(temporary) / "receipt.json",
                    )

    def test_cli_dispatches_prepare_backup_and_preserves_build_candidate_alias(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            backup_arguments = [
                "prepare-backup",
                "--source-db",
                str(root / "source.sqlite3"),
                "--backup",
                str(root / "backup.sqlite3"),
                "--expected-source-sha256",
                "a" * 64,
                "--from",
                "15",
                "--freeze-lock",
                str(root / "freeze.lock"),
                "--migration-lock",
                str(root / "migration.lock"),
                "--receipt",
                str(root / "backup.json"),
            ]
            stdout = io.StringIO()
            with (
                patch.object(
                    migrator,
                    "prepare_verified_backup",
                    return_value={"status": "backup-ready"},
                ) as prepare,
                redirect_stdout(stdout),
            ):
                self.assertEqual(migrator.main(backup_arguments), 0)
            self.assertEqual(json.loads(stdout.getvalue()), {"status": "backup-ready"})
            prepare.assert_called_once_with(
                source_database=root / "source.sqlite3",
                backup=root / "backup.sqlite3",
                expected_source_sha256="a" * 64,
                from_version=15,
                freeze_lock=root / "freeze.lock",
                migration_lock=root / "migration.lock",
                receipt=root / "backup.json",
            )

            candidate_arguments = [
                "build-candidate",
                "--source-db",
                str(root / "source.sqlite3"),
                "--candidate",
                str(root / "candidate.sqlite3"),
                "--expected-source-sha256",
                "a" * 64,
                "--from",
                "15",
                "--to",
                "16",
                "--freeze-lock",
                str(root / "freeze.lock"),
                "--migration-lock",
                str(root / "migration.lock"),
                "--backup-receipt",
                str(root / "backup.json"),
                "--receipt",
                str(root / "migration.json"),
            ]
            with patch.object(
                migrator,
                "build_migration_candidate",
                return_value={"status": "candidate-ready"},
            ) as build, redirect_stdout(io.StringIO()):
                self.assertEqual(migrator.main(candidate_arguments), 0)
            build.assert_called_once()


if __name__ == "__main__":
    unittest.main()
