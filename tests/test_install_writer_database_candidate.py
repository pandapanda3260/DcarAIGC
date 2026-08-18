from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

import v8.storage as storage
from tests.schema_fixture import initialize_historical_schema


ROOT = Path(__file__).resolve().parents[1]


def _load_script(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


installer = _load_script(
    "install_writer_database_candidate",
    ROOT / "scripts" / "install_writer_database_candidate.py",
)
migrator = _load_script(
    "migrate_v8_schema_for_installer_tests",
    ROOT / "scripts" / "migrate_v8_schema.py",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class WriterDatabaseCandidateInstallerTest(unittest.TestCase):
    def _build_v15_database(self, path: Path) -> None:
        stamp = "2026-08-18T00:00:00Z"
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = storage.connect(path)
        try:
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
                        'I00001','douyin','installer-fixture',
                        'https://example.test/installer','迁移前标题','迁移前正文',
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
        finally:
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

    def _layout(self, root: Path, *, sidecars: bool = False) -> dict[str, Path]:
        project = root / "project"
        external = root / "external"
        formal = project / "app" / "data" / "dcar_insight.sqlite3"
        backups = formal.parent / "backups"
        freeze = project / "runtime" / "operator-freeze.lock"
        candidate = external / "candidates" / "candidate-v16.sqlite3"
        verified_backup = external / "backups" / "source-v15.sqlite3"
        backup_receipt = external / "receipts" / "backup.json"
        migration_receipt = external / "receipts" / "migration.json"
        migration_lock = external / "locks" / "migration.lock"
        install_receipt = external / "receipts" / "install.json"
        for directory in (
            backups,
            freeze.parent,
            candidate.parent,
            verified_backup.parent,
            backup_receipt.parent,
            migration_lock.parent,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        migration_lock.parent.chmod(0o700)
        freeze.write_text("frozen\n", encoding="utf-8")
        freeze.chmod(0o600)
        self._build_v15_database(formal)
        if sidecars:
            Path(f"{formal}-wal").write_bytes(b"")
            Path(f"{formal}-shm").write_bytes(b"preserved source shm")

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
        return {
            "project": project,
            "formal": formal,
            "candidate": candidate,
            "backups": backups,
            "backup": backups / "install-001",
            "lock": freeze,
            "verified_backup": verified_backup,
            "backup_receipt": backup_receipt,
            "migration_receipt": migration_receipt,
            "migration_lock": migration_lock,
            "receipt": install_receipt,
        }

    def _arguments(self, layout: dict[str, Path]) -> dict[str, Any]:
        return {
            "formal_database": layout["formal"],
            "candidate": layout["candidate"],
            "migration_receipt": layout["migration_receipt"],
            "expected_migration_receipt_sha256": _sha256(
                layout["migration_receipt"]
            ),
            "backup_directory": layout["backup"],
            "receipt": layout["receipt"],
            "freeze_lock": layout["lock"],
        }

    def _constant_patches(self, layout: dict[str, Path]) -> tuple[Any, Any, Any]:
        return (
            patch.object(installer, "FORMAL_DATABASE", layout["formal"]),
            patch.object(installer, "FORMAL_BACKUP_ROOT", layout["backups"]),
            patch.multiple(
                installer,
                PROJECT_ROOT=layout["project"],
                CANONICAL_OPERATOR_FREEZE_LOCK=layout["lock"],
            ),
        )

    def _rewrite_migration_receipt(
        self,
        layout: dict[str, Path],
        mutate: Any,
    ) -> None:
        value = json.loads(
            layout["migration_receipt"].read_text(encoding="utf-8")
        )
        mutate(value)
        layout["migration_receipt"].write_text(
            json.dumps(value, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )

    def test_success_atomically_preserves_v15_database_and_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), sidecars=True)
            arguments = self._arguments(layout)
            source_sha = _sha256(layout["formal"])
            candidate_sha = _sha256(layout["candidate"])
            wal_sha = _sha256(Path(f"{layout['formal']}-wal"))
            shm_sha = _sha256(Path(f"{layout['formal']}-shm"))
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_handles", return_value=[]),
            ):
                result = installer.install_candidate(**arguments)

            self.assertEqual(result["status"], "installed")
            self.assertEqual(_sha256(layout["formal"]), candidate_sha)
            self.assertFalse(layout["candidate"].exists())
            preserved = layout["backup"] / layout["formal"].name
            self.assertEqual(_sha256(preserved), source_sha)
            self.assertEqual(
                _sha256(layout["backup"] / f"{layout['formal'].name}-wal"),
                wal_sha,
            )
            self.assertEqual(
                _sha256(layout["backup"] / f"{layout['formal'].name}-shm"),
                shm_sha,
            )
            self.assertFalse(Path(f"{layout['formal']}-wal").exists())
            self.assertFalse(Path(f"{layout['formal']}-shm").exists())
            self.assertEqual(layout["formal"].stat().st_mode & 0o777, 0o600)
            receipt = json.loads(layout["receipt"].read_text(encoding="utf-8"))
            self.assertEqual(receipt, result)
            self.assertEqual(receipt["before"]["database"]["sha256"], source_sha)
            self.assertEqual(receipt["installed"]["file"]["sha256"], candidate_sha)
            self.assertEqual(receipt["candidate"]["validation"]["schema_version"], 16)
            self.assertEqual(
                receipt["candidate"]["source_lineage"]["added_tables"],
                [],
            )
            self.assertEqual(
                receipt["candidate"]["source_lineage"][
                    "appended_migration_versions"
                ],
                [16],
            )
            self.assertEqual(receipt["database_handles"], [])

    def test_final_lock_binding_failure_rolls_back_before_context_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), sidecars=True)
            arguments = self._arguments(layout)
            source_sha = _sha256(layout["formal"])
            candidate_sha = _sha256(layout["candidate"])
            wal_sha = _sha256(Path(f"{layout['formal']}-wal"))
            shm_sha = _sha256(Path(f"{layout['formal']}-shm"))
            original_lock_context = installer._exclusive_existing_migration_lock

            @contextmanager
            def replace_before_commit(path: Path) -> Any:
                with original_lock_context(path) as lease:
                    class CommitLease:
                        identity = lease.identity

                        def verify_for_commit(self) -> None:
                            displaced = path.with_suffix(".commit-displaced")
                            path.replace(displaced)
                            path.write_bytes(installer.MIGRATION_LOCK_PAYLOAD)
                            path.chmod(0o600)
                            lease.verify_for_commit()

                    yield CommitLease()

            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_handles", return_value=[]),
                patch.object(
                    installer,
                    "_exclusive_existing_migration_lock",
                    replace_before_commit,
                ),
                self.assertRaisesRegex(installer.CandidateInstallError, "rolled back"),
            ):
                installer.install_candidate(**arguments)

            self.assertEqual(_sha256(layout["formal"]), source_sha)
            self.assertEqual(_sha256(layout["candidate"]), candidate_sha)
            self.assertEqual(_sha256(Path(f"{layout['formal']}-wal")), wal_sha)
            self.assertEqual(_sha256(Path(f"{layout['formal']}-shm")), shm_sha)
            self.assertFalse(layout["receipt"].exists())
            marker = json.loads(
                (layout["backup"] / "FAILED.json").read_text(encoding="utf-8")
            )
            self.assertEqual(marker["status"], "rolled_back")

    def test_every_durable_checkpoint_rolls_back_all_original_paths(self) -> None:
        for checkpoint in installer.INSTALL_CHECKPOINTS:
            with (
                self.subTest(checkpoint=checkpoint),
                tempfile.TemporaryDirectory() as temporary,
            ):
                layout = self._layout(Path(temporary), sidecars=True)
                arguments = self._arguments(layout)
                source_sha = _sha256(layout["formal"])
                candidate_sha = _sha256(layout["candidate"])
                migration_receipt_sha = _sha256(layout["migration_receipt"])
                wal_path = Path(f"{layout['formal']}-wal")
                shm_path = Path(f"{layout['formal']}-shm")
                wal_sha = _sha256(wal_path)
                shm_sha = _sha256(shm_path)

                def fail_at(name: str) -> None:
                    if name == checkpoint:
                        raise RuntimeError(f"injected failure at {name}")

                first, second, third = self._constant_patches(layout)
                with (
                    first,
                    second,
                    third,
                    patch.object(installer, "_database_handles", return_value=[]),
                    self.assertRaisesRegex(
                        installer.CandidateInstallError,
                        "rolled back",
                    ),
                ):
                    installer.install_candidate(
                        **arguments,
                        fault_injector=fail_at,
                    )

                self.assertEqual(_sha256(layout["formal"]), source_sha)
                self.assertEqual(_sha256(layout["candidate"]), candidate_sha)
                self.assertEqual(
                    _sha256(layout["migration_receipt"]),
                    migration_receipt_sha,
                )
                self.assertEqual(_sha256(wal_path), wal_sha)
                self.assertEqual(_sha256(shm_path), shm_sha)
                self.assertFalse(layout["receipt"].exists())
                if layout["backup"].exists():
                    marker = json.loads(
                        (layout["backup"] / "FAILED.json").read_text(encoding="utf-8")
                    )
                    self.assertEqual(marker["status"], "rolled_back")

    def test_rollback_quarantines_candidate_sidecars_before_source_restore(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), sidecars=True)
            arguments = self._arguments(layout)
            source_sha = _sha256(layout["formal"])
            candidate_sha = _sha256(layout["candidate"])
            original_wal_sha = _sha256(Path(f"{layout['formal']}-wal"))
            original_shm_sha = _sha256(Path(f"{layout['formal']}-shm"))

            def create_sidecars(name: str) -> None:
                if name == "after_installed_file_synced":
                    Path(f"{layout['formal']}-wal").write_bytes(b"candidate wal")
                    Path(f"{layout['formal']}-shm").write_bytes(b"candidate shm")
                    raise RuntimeError("injected candidate sidecars")

            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_handles", return_value=[]),
                self.assertRaisesRegex(installer.CandidateInstallError, "rolled back"),
            ):
                installer.install_candidate(
                    **arguments,
                    fault_injector=create_sidecars,
                )
            self.assertEqual(_sha256(layout["formal"]), source_sha)
            self.assertEqual(_sha256(layout["candidate"]), candidate_sha)
            self.assertEqual(_sha256(Path(f"{layout['formal']}-wal")), original_wal_sha)
            self.assertEqual(_sha256(Path(f"{layout['formal']}-shm")), original_shm_sha)
            self.assertEqual(
                (
                    layout["backup"]
                    / f"FAILED-installed-{layout['formal'].name}-wal"
                ).read_bytes(),
                b"candidate wal",
            )

    def test_handle_racing_rollback_fails_closed_without_mixing_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), sidecars=True)
            arguments = self._arguments(layout)
            source_sha = _sha256(layout["formal"])
            candidate_sha = _sha256(layout["candidate"])
            calls = 0

            def handles(_databases: Any) -> list[dict[str, Any]]:
                nonlocal calls
                calls += 1
                if calls < 4:
                    return []
                return [{"command": "python", "pid": 999, "descriptor": "4u"}]

            def fail_after_install(name: str) -> None:
                if name == "after_installed_file_synced":
                    Path(f"{layout['formal']}-wal").write_bytes(b"writer wal")
                    raise RuntimeError("writer raced installation")

            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_handles", side_effect=handles),
                self.assertRaisesRegex(
                    installer.CandidateInstallError,
                    "rollback was incomplete",
                ),
            ):
                installer.install_candidate(
                    **arguments,
                    fault_injector=fail_after_install,
                )
            self.assertEqual(_sha256(layout["formal"]), candidate_sha)
            self.assertFalse(layout["candidate"].exists())
            self.assertEqual(
                _sha256(layout["backup"] / layout["formal"].name),
                source_sha,
            )
            marker = json.loads(
                (layout["backup"] / "FAILED.json").read_text(encoding="utf-8")
            )
            self.assertEqual(marker["status"], "rollback_incomplete")

    def test_racing_formal_path_is_quarantined_and_source_returns(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            arguments = self._arguments(layout)
            source_sha = _sha256(layout["formal"])
            candidate_sha = _sha256(layout["candidate"])

            def recreate_formal(name: str) -> None:
                if name == "after_source_shm_moved":
                    layout["formal"].write_bytes(b"racing database")

            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_handles", return_value=[]),
                self.assertRaisesRegex(installer.CandidateInstallError, "rolled back"),
            ):
                installer.install_candidate(
                    **arguments,
                    fault_injector=recreate_formal,
                )
            self.assertEqual(_sha256(layout["formal"]), source_sha)
            self.assertEqual(_sha256(layout["candidate"]), candidate_sha)
            self.assertEqual(
                (
                    layout["backup"]
                    / f"FAILED-racing-{layout['formal'].name}"
                ).read_bytes(),
                b"racing database",
            )

    def test_receipt_fsync_failure_rolls_back_and_removes_partial_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary), sidecars=True)
            arguments = self._arguments(layout)
            source_sha = _sha256(layout["formal"])
            candidate_sha = _sha256(layout["candidate"])
            original = installer._fsync_directory
            failed = False

            def fail_once(path: Path) -> None:
                nonlocal failed
                if path == layout["receipt"].parent and not failed:
                    failed = True
                    raise OSError("injected receipt fsync failure")
                original(path)

            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_handles", return_value=[]),
                patch.object(installer, "_fsync_directory", side_effect=fail_once),
                self.assertRaisesRegex(installer.CandidateInstallError, "rolled back"),
            ):
                installer.install_candidate(**arguments)
            self.assertTrue(failed)
            self.assertFalse(layout["receipt"].exists())
            self.assertEqual(_sha256(layout["formal"]), source_sha)
            self.assertEqual(_sha256(layout["candidate"]), candidate_sha)

    def test_refuses_read_or_write_database_handle_before_cutover(self) -> None:
        for descriptor in ("4r", "5u", "6w"):
            with (
                self.subTest(descriptor=descriptor),
                tempfile.TemporaryDirectory() as temporary,
            ):
                layout = self._layout(Path(temporary))
                arguments = self._arguments(layout)
                first, second, third = self._constant_patches(layout)
                with (
                    first,
                    second,
                    third,
                    patch.object(
                        installer,
                        "_database_handles",
                        return_value=[
                            {
                                "command": "python",
                                "pid": 123,
                                "descriptor": descriptor,
                            }
                        ],
                    ),
                    self.assertRaisesRegex(
                        installer.CandidateInstallError,
                        "database handles",
                    ),
                ):
                    installer.install_candidate(**arguments)
                self.assertFalse(layout["backup"].exists())

    def test_migration_receipt_contract_is_strict_and_hash_anchored(self) -> None:
        cases = (
            ("schema", lambda value: value.__setitem__("schema_version", "wrong")),
            ("from", lambda value: value.__setitem__("from_version", 14)),
            (
                "source-path",
                lambda value: value["formal_source"].__setitem__(
                    "path", "/tmp/wrong.sqlite3"
                ),
            ),
            (
                "candidate-path",
                lambda value: value["candidate"].__setitem__(
                    "path", "/tmp/wrong.sqlite3"
                ),
            ),
            (
                "lineage",
                lambda value: value["lineage"].__setitem__(
                    "added_tables", ["unexpected"]
                ),
            ),
            (
                "backup",
                lambda value: value["verified_backup"]["database"].__setitem__(
                    "sha256", "f" * 64
                ),
            ),
            ("extra", lambda value: value.__setitem__("unsupported", True)),
        )
        for name, mutate in cases:
            with (
                self.subTest(case=name),
                tempfile.TemporaryDirectory() as temporary,
            ):
                layout = self._layout(Path(temporary))
                self._rewrite_migration_receipt(layout, mutate)
                arguments = self._arguments(layout)
                first, second, third = self._constant_patches(layout)
                with (
                    first,
                    second,
                    third,
                    patch.object(installer, "_database_handles", return_value=[]),
                    self.assertRaises(installer.CandidateInstallError),
                ):
                    installer.install_candidate(**arguments)
                self.assertFalse(layout["backup"].exists())

        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            arguments = self._arguments(layout)
            arguments["expected_migration_receipt_sha256"] = "f" * 64
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                self.assertRaisesRegex(installer.CandidateInstallError, "SHA-256"),
            ):
                installer.install_candidate(**arguments)

    def test_recomputes_lineage_after_candidate_tampering_even_if_receipt_is_rehashed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            with sqlite3.connect(layout["candidate"]) as connection:
                connection.execute(
                    "UPDATE content_items SET title='tampered candidate'"
                )

            def update_file(value: dict[str, Any]) -> None:
                value["candidate"]["file"] = installer._fingerprint(
                    layout["candidate"]
                )

            self._rewrite_migration_receipt(layout, update_file)
            arguments = self._arguments(layout)
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_handles", return_value=[]),
                self.assertRaisesRegex(
                    installer.CandidateInstallError,
                    "retained table projection",
                ),
            ):
                installer.install_candidate(**arguments)
            self.assertFalse(layout["backup"].exists())

    def test_rejects_extra_candidate_trigger_even_if_receipt_is_rehashed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            with sqlite3.connect(layout["candidate"]) as connection:
                connection.execute(
                    """
                    CREATE TRIGGER injected_accounts_delete
                    AFTER INSERT ON accounts
                    BEGIN
                        DELETE FROM accounts WHERE id=NEW.id;
                    END
                    """
                )

            def update_file(value: dict[str, Any]) -> None:
                value["candidate"]["file"] = installer._fingerprint(
                    layout["candidate"]
                )

            self._rewrite_migration_receipt(layout, update_file)
            arguments = self._arguments(layout)
            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_handles", return_value=[]),
                self.assertRaisesRegex(
                    installer.CandidateInstallError,
                    "schema object lineage",
                ),
            ):
                installer.install_candidate(**arguments)
            self.assertFalse(layout["backup"].exists())

    def test_refuses_changed_backup_held_migration_lock_and_candidate_sidecar(
        self,
    ) -> None:
        scenarios = ("backup", "migration-lock", "candidate-sidecar")
        for scenario in scenarios:
            with (
                self.subTest(scenario=scenario),
                tempfile.TemporaryDirectory() as temporary,
            ):
                layout = self._layout(Path(temporary))
                lock_descriptor: int | None = None
                if scenario == "backup":
                    with layout["verified_backup"].open("ab") as handle:
                        handle.write(b"changed")
                elif scenario == "migration-lock":
                    lock_descriptor = os.open(layout["migration_lock"], os.O_RDWR)
                    fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                else:
                    Path(f"{layout['candidate']}-wal").write_bytes(b"")
                arguments = self._arguments(layout)
                first, second, third = self._constant_patches(layout)
                try:
                    with (
                        first,
                        second,
                        third,
                        patch.object(installer, "_database_handles", return_value=[]),
                        self.assertRaises(installer.CandidateInstallError),
                    ):
                        installer.install_candidate(**arguments)
                finally:
                    if lock_descriptor is not None:
                        fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
                        os.close(lock_descriptor)
                self.assertFalse(layout["backup"].exists())

    def test_refuses_symlink_hardlink_cross_device_and_noncanonical_freeze(self) -> None:
        for scenario in ("symlink", "hardlink", "cross-device", "freeze-mode"):
            with (
                self.subTest(scenario=scenario),
                tempfile.TemporaryDirectory() as temporary,
            ):
                layout = self._layout(Path(temporary))
                arguments = self._arguments(layout)
                identity_patch: Any = patch.object(
                    installer,
                    "_stat_identity",
                    wraps=installer._stat_identity,
                )
                if scenario == "symlink":
                    real = layout["candidate"].with_name("real.sqlite3")
                    layout["candidate"].replace(real)
                    layout["candidate"].symlink_to(real)
                elif scenario == "hardlink":
                    os.link(
                        layout["candidate"],
                        layout["candidate"].with_name("second-link.sqlite3"),
                    )
                elif scenario == "cross-device":
                    original = installer._stat_identity

                    def different_device(path: Path) -> Any:
                        value = original(path)
                        if path == layout["candidate"]:
                            return replace(value, device=value.device + 1)
                        return value

                    identity_patch = patch.object(
                        installer,
                        "_stat_identity",
                        side_effect=different_device,
                    )
                else:
                    layout["lock"].chmod(0o644)
                first, second, third = self._constant_patches(layout)
                with (
                    first,
                    second,
                    third,
                    identity_patch,
                    self.assertRaises(installer.CandidateInstallError),
                ):
                    installer.install_candidate(**arguments)
                self.assertFalse(layout["backup"].exists())

    def test_install_receipt_is_o_excl_and_racer_is_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            layout = self._layout(Path(temporary))
            arguments = self._arguments(layout)
            source_sha = _sha256(layout["formal"])
            candidate_sha = _sha256(layout["candidate"])
            original = installer._write_json_exclusive

            def race(path: Path, value: Any, **kwargs: Any) -> None:
                if path == layout["receipt"]:
                    path.write_text("operator-owned", encoding="utf-8")
                original(path, value, **kwargs)

            first, second, third = self._constant_patches(layout)
            with (
                first,
                second,
                third,
                patch.object(installer, "_database_handles", return_value=[]),
                patch.object(installer, "_write_json_exclusive", side_effect=race),
                self.assertRaisesRegex(installer.CandidateInstallError, "rolled back"),
            ):
                installer.install_candidate(**arguments)
            self.assertEqual(
                layout["receipt"].read_text(encoding="utf-8"),
                "operator-owned",
            )
            self.assertEqual(_sha256(layout["formal"]), source_sha)
            self.assertEqual(_sha256(layout["candidate"]), candidate_sha)

    def test_lsof_parser_reports_readers_and_writers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "db.sqlite3"
            database.write_bytes(b"fixture")
            output = "\n".join(
                (
                    "COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME",
                    f"python 10 mark 4r REG 1,1 1 1 {database}",
                    f"python 11 mark 5u REG 1,1 1 1 {database}",
                    f"python 12 mark txt REG 1,1 1 1 {database}",
                )
            )
            with patch.object(
                installer.subprocess,
                "run",
                return_value=subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout=output,
                    stderr="",
                ),
            ):
                handles = installer._database_handles((database,))
            self.assertEqual(
                [item["descriptor"] for item in handles],
                ["4r", "5u"],
            )

    def test_cli_requires_and_forwards_migration_receipt_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            arguments = [
                "--formal-db",
                str(root / "formal.sqlite3"),
                "--candidate",
                str(root / "candidate.sqlite3"),
                "--migration-receipt",
                str(root / "migration.json"),
                "--expected-migration-receipt-sha256",
                "a" * 64,
                "--backup-dir",
                str(root / "backup"),
                "--receipt",
                str(root / "install.json"),
                "--freeze-lock",
                str(root / "freeze.lock"),
            ]
            expected = {"status": "installed"}
            stdout = io.StringIO()
            with (
                patch.object(
                    installer,
                    "install_candidate",
                    return_value=expected,
                ) as call,
                redirect_stdout(stdout),
            ):
                self.assertEqual(installer.main(arguments), 0)
            self.assertEqual(json.loads(stdout.getvalue()), expected)
            call.assert_called_once_with(
                formal_database=root / "formal.sqlite3",
                candidate=root / "candidate.sqlite3",
                migration_receipt=root / "migration.json",
                expected_migration_receipt_sha256="a" * 64,
                backup_directory=root / "backup",
                receipt=root / "install.json",
                freeze_lock=root / "freeze.lock",
            )
            with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
                installer.main(arguments[:-2])

    def test_test_guard_refuses_actual_default_before_database_open(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fake_formal = Path(temporary) / "never-open.sqlite3"
            with (
                patch.dict(os.environ, {"DCAR_TEST_DENY_FORMAL_DB": "1"}),
                patch.multiple(
                    installer,
                    DEFAULT_DB=fake_formal,
                    FORMAL_DATABASE=fake_formal,
                ),
                self.assertRaisesRegex(
                    installer.CandidateInstallError,
                    "test process attempted to open the formal",
                ),
            ):
                installer.install_candidate(
                    formal_database=fake_formal,
                    candidate=Path(temporary) / "candidate.sqlite3",
                    migration_receipt=Path(temporary) / "migration.json",
                    expected_migration_receipt_sha256="a" * 64,
                    backup_directory=Path(temporary) / "backup",
                    receipt=Path(temporary) / "install.json",
                    freeze_lock=Path(temporary) / "freeze.lock",
                )


if __name__ == "__main__":
    unittest.main()
