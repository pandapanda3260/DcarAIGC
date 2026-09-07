from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.schema_fixture import initialize_historical_schema
from v8 import paid_drain, runtime_receipts, schema_v19, storage
from v8.profile_activations import MATRIX_PROFILE, activation_by_id, append_activation
from v8.snapshot_contract import descriptor
from v8.source_routing import load_policy


STAMP = "2026-09-01T00:00:00Z"


def _digest(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class V19SchemaMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.pair_index = 0

    def _v18(self, name: str = "candidate.sqlite3") -> Path:
        path = self.root / name
        connection = sqlite3.connect(path)
        connection.row_factory = sqlite3.Row
        storage.configure_connection_safety(connection)
        initialize_historical_schema(connection, target_version=17)
        storage.migrate_database(connection, from_version=17, to_version=18)
        connection.close()
        return path

    def _seed_bridges(
        self,
        path: Path,
        *,
        release_drain: bool = True,
        activation_completed_at: str = STAMP,
        omit_activation_field: str | None = None,
        report_version: str = "dcar-content-operations-report-v8.8",
    ) -> dict[str, int]:
        with storage.connect(path) as connection:
            connection.execute(
                """INSERT INTO taxonomy_versions(
                       id,version,status,definition,created_at,published_at)
                   VALUES ('taxonomy-v5.2','selling-points-v5.2','published',
                           'schema19 bridge fixture',?,?)""",
                (STAMP, STAMP),
            )
            connection.execute(
                """INSERT INTO evaluation_releases(
                       id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                       created_at,updated_at,activated_at)
                   VALUES ('evaluation-v9__selling-points-v5.2','evaluation-v9',
                           'selling-points-v5.2',?,'active',?,?,?)""",
                ("9" * 64, STAMP, STAMP, STAMP),
            )
            connection.execute(
                "INSERT INTO accounts(id,phone,created_at,updated_at) VALUES (1,'',?,?)",
                (STAMP, STAMP),
            )
            connection.execute(
                "INSERT INTO account_platform_identities("
                "id,account_id,platform,uid,created_at,updated_at) "
                "VALUES (1,1,'douyin','uid-1',?,?)",
                (STAMP, STAMP),
            )
            connection.execute(
                """INSERT INTO account_roster_snapshots(
                       id,source_type,scope_key,scope_json,source_instance_id,
                       source_captured_at,accepted_at,declared_count,member_count,
                       members_sha256,source_sha256,source_path,contract_version)
                   VALUES (1,'manual_export','org','{}','matrix-export',?,?,1,1,?,?,
                           'matrix.json','matrix-roster-v1')""",
                (STAMP, STAMP, "a" * 64, "b" * 64),
            )
            connection.execute(
                """INSERT INTO account_roster_members(
                       snapshot_id,account_identity_id,platform,matrix_account_id,
                       profile_ref,monitoring_status,authorization_status)
                   VALUES (1,1,'douyin','matrix-1','profile-1','unknown','unknown')"""
            )
            activation = {
                "contract_version": "matrix-first-pipeline-v1",
                "mode": "active",
                "cutover_at": STAMP,
                "roster_snapshot_id": 1,
                "roster_snapshot_hash": "a" * 64,
                "source_policy_sha256": _digest(load_policy()),
                "snapshot_contract": descriptor(),
                "schema_version": 18,
                "schema_migration": "matrix-roster-source-routing",
                "report_version": report_version,
                "active_release_id": "evaluation-v9__selling-points-v5.2",
                "matcher_rule_sha256": "9" * 64,
            }
            if omit_activation_field is not None:
                activation.pop(omit_activation_field)
            encoded = json.dumps(
                activation,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            activation_run = int(
                connection.execute(
                    """INSERT INTO scheduler_runs(
                           job_id,scheduled_for,status,started_at,completed_at,details_json)
                       VALUES ('matrix_pipeline_activation',?,'succeeded',?,?,?)""",
                    (STAMP, STAMP, activation_completed_at, encoded),
                ).lastrowid
                or 0
            )
            activation_attempt = int(
                connection.execute(
                    """INSERT INTO scheduler_run_attempts(
                           scheduler_run_id,attempt_number,invocation_source,status,
                           started_at,completed_at,details_json)
                       VALUES (?,1,'operator_retry','succeeded',?,?,?)""",
                    (activation_run, STAMP, activation_completed_at, encoded),
                ).lastrowid
                or 0
            )
            scan_details = "{}"
            scan_run = int(
                connection.execute(
                    """INSERT INTO scheduler_runs(
                           job_id,scheduled_for,status,started_at,completed_at,details_json)
                       VALUES ('tikhub_reconcile','scan','succeeded',?,?,?)""",
                    (STAMP, STAMP, scan_details),
                ).lastrowid
                or 0
            )
            scan_attempt = int(
                connection.execute(
                    """INSERT INTO scheduler_run_attempts(
                           scheduler_run_id,attempt_number,invocation_source,status,
                           started_at,completed_at,details_json)
                       VALUES (?,1,'scheduled','succeeded',?,?,?)""",
                    (scan_run, STAMP, STAMP, scan_details),
                ).lastrowid
                or 0
            )
            connection.commit()
            _run, _details, binding = runtime_receipts._terminal_scan_binding(
                connection, scan_run
            )
        scan_scope = {
            "contract_version": "scan-verification-receipt-v2",
            **binding,
        }
        scan_receipt = runtime_receipts._record_one_shot(
            db_path=path,
            job_id=runtime_receipts.SCAN_RECEIPT_JOB,
            scope=scan_scope,
            summary={
                "verified": True,
                "scan_run_id": scan_run,
                "scan_status": "succeeded",
            },
            evidence={"path": "scan.json", "sha256": "c" * 64, "byte_size": 1},
            recorded_at=STAMP,
        )
        day_scope = {
            "contract_version": "profile-day-scope-v2",
            "activation_id": activation_run,
            "profile_id": runtime_receipts.MODE_A_PROFILE,
            "roster_snapshot_id": 1,
            "roster_snapshot_hash": "a" * 64,
            "business_day": "2026-08-31",
            "source_revision": "d" * 64,
        }
        day_receipt = runtime_receipts._record_one_shot(
            db_path=path,
            job_id=runtime_receipts.DAY_RECEIPT_JOB,
            scope=day_scope,
            summary={
                "sequence": 1,
                "sealed_at": STAMP,
                "status": "complete",
                "complete": True,
                "coverage": {"complete": True, "partial_publishable": False},
            },
            evidence={"path": "day.json", "sha256": "e" * 64, "byte_size": 1},
            recorded_at=STAMP,
        )
        binding = {
            "source_activation_id": activation_run,
            "target_activation_id": "schema19-bridge",
            "business_day": "2026-09-01",
            "planned_effective_at": "2026-09-02T00:00:00Z",
            "build_receipt_sha256": "f" * 64,
            "runtime_root_receipt_sha256": "0" * 64,
        }
        paid_drain.start_paid_drain(
            "schema19-migration", binding=binding, db_path=path, now=STAMP
        )
        paid_drain.seal_paid_drain(
            "schema19-migration", db_path=path, now="2026-09-01T00:01:00Z"
        )
        if release_drain:
            paid_drain.release_paid_drain(
                "schema19-migration", db_path=path, now="2026-09-01T00:02:00Z"
            )
        return {
            "activation_run": activation_run,
            "activation_attempt": activation_attempt,
            "scan_run": scan_run,
            "scan_attempt": scan_attempt,
            "scan_receipt_run": int(scan_receipt["run_id"]),
            "day_receipt_run": int(day_receipt["run_id"]),
        }

    @staticmethod
    def _append_drain_cycle(
        path: Path,
        *,
        activation_run: int,
        drain_id: str = "schema19-migration-retry",
        build_receipt_sha256: str = "1" * 64,
        target_activation_id: str = "schema19-bridge",
        release_drain: bool = True,
    ) -> None:
        binding = {
            "source_activation_id": activation_run,
            "target_activation_id": target_activation_id,
            "business_day": "2026-09-01",
            "planned_effective_at": "2026-09-02T00:00:00Z",
            "build_receipt_sha256": build_receipt_sha256,
            "runtime_root_receipt_sha256": "2" * 64,
        }
        paid_drain.start_paid_drain(
            drain_id,
            binding=binding,
            db_path=path,
            now="2026-09-01T01:00:00Z",
        )
        paid_drain.seal_paid_drain(
            drain_id, db_path=path, now="2026-09-01T01:01:00Z"
        )
        if release_drain:
            paid_drain.release_paid_drain(
                drain_id, db_path=path, now="2026-09-01T01:02:00Z"
            )

    def _migrated_pair(self) -> tuple[Path, Path]:
        self.pair_index += 1
        source = self._v18(f"lineage-source-{self.pair_index}.sqlite3")
        self._seed_bridges(source)
        candidate = self.root / f"lineage-candidate-{self.pair_index}.sqlite3"
        source_connection = sqlite3.connect(
            source.resolve().as_uri() + "?mode=ro", uri=True
        )
        candidate_connection = sqlite3.connect(candidate)
        try:
            source_connection.backup(candidate_connection)
            candidate_connection.commit()
        finally:
            candidate_connection.close()
            source_connection.close()
        with storage.connect(candidate) as connection:
            storage.migrate_database(connection, from_version=18, to_version=19)
        return source, candidate

    @staticmethod
    def _replace_immutable_trigger(
        connection: sqlite3.Connection,
        *,
        trigger: str,
        mutation: str,
    ) -> None:
        connection.execute(f'DROP TRIGGER "{trigger}"')
        connection.execute(mutation)
        connection.execute(schema_v19.expected_schema_objects()[("trigger", trigger)])
        connection.commit()

    def test_fresh_v19_has_the_sealed_profile_objects(self) -> None:
        path = self.root / "fresh.sqlite3"
        with storage.connect(path) as connection:
            storage.initialize_database(connection)
            self.assertEqual(storage.require_schema_compatibility(connection), 19)
            self.assertEqual(
                {
                    "acquisition_profile_activations",
                    "activation_cancellations",
                    "account_state_events",
                    "scan_verification_receipts",
                    "profile_day_coverage_receipts",
                    "runtime_receipt_revocations",
                    "pipeline_paid_drain_events",
                    "paid_provider_dispatch_events",
                }
                - storage._table_names(connection),
                set(),
            )

    def test_v18_v19_lineage_proves_only_roster_and_bridge_delta(self) -> None:
        source, candidate = self._migrated_pair()
        with storage.connect(source) as source_connection, storage.connect(
            candidate
        ) as candidate_connection:
            proof = storage.validate_v18_v19_lineage(
                source_connection, candidate_connection
            )
        self.assertEqual(
            proof["schema_version"], "dcar-v19-offline-allowed-differences-v1"
        )
        self.assertEqual(proof["appended_migration_versions"], [19])
        self.assertEqual(
            set(proof["added_tables"]), set(schema_v19.NEW_TABLES)
        )
        self.assertEqual(proof["roster_snapshots"]["row_count"], 1)
        self.assertEqual(proof["roster_members"]["row_count"], 1)
        self.assertEqual(
            proof["bridge_tables"]["pipeline_paid_drain_events"]["row_count"],
            3,
        )
        self.assertEqual(
            proof["bridge_tables"]["paid_provider_dispatch_events"]["row_count"],
            0,
        )

    def test_v18_v19_lineage_rejects_protected_old_row_changes(self) -> None:
        source, candidate = self._migrated_pair()
        with storage.connect(candidate) as connection:
            connection.execute(
                "UPDATE accounts SET operator_name='tampered' WHERE id=1"
            )
            connection.commit()
        with storage.connect(source) as source_connection, storage.connect(
            candidate
        ) as candidate_connection, self.assertRaisesRegex(
            storage.SchemaMigrationError, "protected rows or columns in accounts"
        ):
            storage.validate_v18_v19_lineage(source_connection, candidate_connection)

    def test_v18_v19_lineage_rejects_roster_transform_changes(self) -> None:
        source, candidate = self._migrated_pair()
        with storage.connect(candidate) as connection:
            self._replace_immutable_trigger(
                connection,
                trigger="trg_roster_members_no_update",
                mutation=(
                    "UPDATE account_roster_members "
                    "SET member_key='matrix:douyin:tampered'"
                ),
            )
        with storage.connect(source) as source_connection, storage.connect(
            candidate
        ) as candidate_connection, self.assertRaisesRegex(
            storage.SchemaMigrationError, "roster member transform"
        ):
            storage.validate_v18_v19_lineage(source_connection, candidate_connection)

    def test_v18_v19_lineage_rejects_bridge_and_sequence_changes(self) -> None:
        mutations = (
            (
                "bridge",
                lambda connection: self._replace_immutable_trigger(
                    connection,
                    trigger="trg_scan_receipts_no_update",
                    mutation=(
                        "UPDATE scan_verification_receipts "
                        "SET summary_json='{}'"
                    ),
                ),
                "migration bridge rows in scan_verification_receipts",
            ),
            (
                "sequence",
                lambda connection: connection.execute(
                    "UPDATE sqlite_sequence SET seq=99 "
                    "WHERE name='pipeline_paid_drain_events'"
                ),
                "sqlite_sequence",
            ),
        )
        for label, mutate, message in mutations:
            with self.subTest(label=label):
                source, candidate = self._migrated_pair()
                with storage.connect(candidate) as connection:
                    mutate(connection)
                    connection.commit()
                with storage.connect(source) as source_connection, storage.connect(
                    candidate
                ) as candidate_connection, self.assertRaisesRegex(
                    storage.SchemaMigrationError, message
                ):
                    storage.validate_v18_v19_lineage(
                        source_connection, candidate_connection
                    )

    def test_v18_roster_activation_receipts_and_drain_are_migrated(self) -> None:
        path = self._v18()
        legacy = self._seed_bridges(path)
        with storage.connect(path) as connection:
            plan = storage.migration_v19_plan(connection)
            self.assertEqual(plan["roster_snapshot_count"], 1)
            self.assertEqual(plan["roster_member_count"], 1)
            self.assertEqual(len(plan["scan_receipts"]), 1)
            self.assertEqual(len(plan["day_receipts"]), 1)
            self.assertEqual(len(plan["drain_receipts"]), 3)
            storage.migrate_database(connection, from_version=18, to_version=19)
            self.assertEqual(storage.require_schema_compatibility(connection), 19)
            snapshot = dict(
                connection.execute("SELECT * FROM account_roster_snapshots").fetchone()
            )
            member = dict(
                connection.execute("SELECT * FROM account_roster_members").fetchone()
            )
            self.assertEqual(snapshot["source_family"], "matrix")
            self.assertEqual(member["member_key"], "matrix:douyin:matrix-1")
            self.assertEqual(member["uid"], "uid-1")
            activation = activation_by_id(connection, 1)
            self.assertEqual(activation["profile_id"], "matrix_hybrid_v1")
            self.assertEqual(activation["build_receipt_sha256"], "f" * 64)
            self.assertEqual(activation["created_at"], "2026-09-01T00:00:00.000000Z")
            self.assertEqual(
                activation["metadata"],
                {
                    "migration": "schema18-activation-bridge-v1",
                    "legacy_run_id": legacy["activation_run"],
                    "legacy_attempt_id": legacy["activation_attempt"],
                    "legacy_details_sha256": hashlib.sha256(
                        connection.execute(
                            "SELECT details_json FROM scheduler_runs WHERE id=?",
                            (legacy["activation_run"],),
                        ).fetchone()[0].encode()
                    ).hexdigest(),
                },
            )
            self.assertEqual(
                connection.execute(
                    "SELECT source_bridge_run_id FROM scan_verification_receipts"
                ).fetchone()[0],
                legacy["scan_receipt_run"],
            )
            self.assertEqual(
                connection.execute(
                    "SELECT source_bridge_run_id FROM profile_day_coverage_receipts"
                ).fetchone()[0],
                legacy["day_receipt_run"],
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM pipeline_paid_drain_events"
                ).fetchone()[0],
                3,
            )
            self.assertEqual(
                {
                    int(row[0])
                    for row in connection.execute(
                        "SELECT target_activation_id FROM pipeline_paid_drain_events"
                    )
                },
                {activation["activation_id"]},
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM paid_provider_dispatch_events"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_v18_sealed_drain_is_a_valid_migration_boundary(self) -> None:
        path = self._v18()
        self._seed_bridges(path, release_drain=False)
        with storage.connect(path) as connection:
            storage.migrate_database(connection, from_version=18, to_version=19)
            rows = connection.execute(
                "SELECT event_type,target_activation_id "
                "FROM pipeline_paid_drain_events ORDER BY id"
            ).fetchall()
            self.assertEqual(
                [(row["event_type"], row["target_activation_id"]) for row in rows],
                [("start", 1), ("sealed", 1)],
            )

    def test_retried_released_drain_imports_both_cycles_and_latest_build(self) -> None:
        path = self._v18()
        legacy = self._seed_bridges(path)
        self._append_drain_cycle(path, activation_run=legacy["activation_run"])
        with storage.connect(path) as connection:
            plan = storage.migration_v19_plan(connection)
            self.assertEqual(len(plan["drain_receipts"]), 6)
            self.assertEqual(
                plan["legacy_activation"]["build_receipt_sha256"], "1" * 64
            )
            storage.migrate_database(connection, from_version=18, to_version=19)
            activation = activation_by_id(connection, 1)
            self.assertEqual(activation["build_receipt_sha256"], "1" * 64)
            events = paid_drain._validated_profile_chain(connection)
            self.assertEqual(
                [(event.drain_id, event.event_type) for event in events],
                [
                    ("schema19-migration", "start"),
                    ("schema19-migration", "sealed"),
                    ("schema19-migration", "release"),
                    ("schema19-migration-retry", "start"),
                    ("schema19-migration-retry", "sealed"),
                    ("schema19-migration-retry", "release"),
                ],
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM acquisition_profile_activations"
                ).fetchone()[0],
                1,
            )

    def test_retried_sealed_drain_imports_history_and_keeps_fence_closed(self) -> None:
        path = self._v18()
        legacy = self._seed_bridges(path)
        self._append_drain_cycle(
            path,
            activation_run=legacy["activation_run"],
            release_drain=False,
        )
        with storage.connect(path) as connection:
            storage.migrate_database(connection, from_version=18, to_version=19)
            activation = activation_by_id(connection, 1)
            self.assertEqual(activation["build_receipt_sha256"], "1" * 64)
            events = paid_drain._validated_profile_chain(connection)
            self.assertEqual(len(events), 5)
            self.assertEqual(events[-1].drain_id, "schema19-migration-retry")
            self.assertEqual(events[-1].event_type, "sealed")
            self.assertEqual(
                paid_drain.dispatch_state(
                    connection, at="2026-09-01T02:00:00Z"
                ).state,
                "sealed",
            )

    def test_overlapping_prior_drain_chain_is_rejected(self) -> None:
        path = self._v18()
        legacy = self._seed_bridges(path, release_drain=False)
        with storage.connect(path) as connection, storage.transaction(connection):
            previous = paid_drain._validated_chain(connection)[-1]
            paid_drain._insert_event(
                connection,
                drain_id="schema19-overlap",
                event_type="start",
                payload={
                    "binding": {
                        "source_activation_id": legacy["activation_run"],
                        "target_activation_id": "schema19-bridge",
                        "business_day": "2026-09-01",
                        "planned_effective_at": "2026-09-02T00:00:00Z",
                        "build_receipt_sha256": "1" * 64,
                        "runtime_root_receipt_sha256": "2" * 64,
                    },
                    "frozen_dispatch": {},
                },
                created_at="2026-09-01T01:00:00Z",
                previous=previous,
            )
        with storage.connect(path) as connection, self.assertRaisesRegex(
            paid_drain.PaidDrainError, "multiple active paid drains"
        ):
            storage.migrate_database(connection, from_version=18, to_version=19)
        with storage.connect(path) as connection:
            self.assertEqual(storage.require_schema_compatibility(connection), 18)

    def test_retried_drain_with_invalid_activation_binding_is_rejected(self) -> None:
        path = self._v18()
        legacy = self._seed_bridges(path)
        self._append_drain_cycle(
            path,
            activation_run=legacy["activation_run"],
            target_activation_id="other-schema19-bridge",
        )
        with storage.connect(path) as connection, self.assertRaisesRegex(
            storage.SchemaMigrationError,
            "activation bridge paid drain binding is invalid",
        ):
            storage.migrate_database(connection, from_version=18, to_version=19)
        with storage.connect(path) as connection:
            self.assertEqual(storage.require_schema_compatibility(connection), 18)

    def test_legacy_activation_created_at_is_its_real_start(self) -> None:
        path = self._v18()
        completed_at = "2026-09-01T00:05:00Z"
        self._seed_bridges(path, activation_completed_at=completed_at)
        with storage.connect(path) as connection:
            storage.migrate_database(connection, from_version=18, to_version=19)
            activation = activation_by_id(connection, 1)
            self.assertEqual(
                activation["created_at"], "2026-09-01T00:00:00.000000Z"
            )
            self.assertEqual(
                activation["effective_at"], "2026-09-01T00:00:00.000000Z"
            )

    def test_incomplete_legacy_activation_contract_is_rejected(self) -> None:
        path = self._v18()
        self._seed_bridges(path, omit_activation_field="report_version")
        with storage.connect(path) as connection:
            with self.assertRaisesRegex(
                storage.SchemaMigrationError,
                "activation bridge contract is invalid",
            ):
                storage.migrate_database(connection, from_version=18, to_version=19)
            self.assertEqual(storage.require_schema_compatibility(connection), 18)

    def test_schema19_report_version_cannot_rewrite_schema18_bridge_history(self) -> None:
        path = self._v18()
        self._seed_bridges(
            path, report_version="dcar-content-operations-report-v8.9"
        )
        with storage.connect(path) as connection:
            with self.assertRaisesRegex(
                storage.SchemaMigrationError,
                "activation bridge contract is invalid",
            ):
                storage.migrate_database(connection, from_version=18, to_version=19)
            self.assertEqual(storage.require_schema_compatibility(connection), 18)

    def test_release_for_one_activation_cannot_permit_another(self) -> None:
        path = self._v18()
        legacy = self._seed_bridges(path)
        with storage.connect(path) as connection:
            storage.migrate_database(connection, from_version=18, to_version=19)
            second = append_activation(
                connection,
                profile_id=MATRIX_PROFILE,
                roster_snapshot_id=1,
                roster_members_sha256="a" * 64,
                effective_at="2026-09-02T00:00:00Z",
                build_receipt_sha256="f" * 64,
                actor="test",
                reason="permit isolation",
                created_at="2026-09-01T12:00:00Z",
            )
            permit_id = int(
                connection.execute(
                    "SELECT id FROM pipeline_paid_drain_events "
                    "WHERE event_type='release'"
                ).fetchone()[0]
            )
            with self.assertRaisesRegex(
                sqlite3.IntegrityError, "paid dispatch event chain is invalid"
            ):
                connection.execute(
                    """INSERT INTO paid_provider_dispatch_events(
                           dispatch_id,sequence,event_type,provider,operation,
                           activation_id,business_day,permit_event_id,
                           scheduler_run_id,scheduler_attempt_id,scope_json,
                           previous_event_id,previous_event_hash,contract_version,
                           event_hash,created_at)
                       VALUES ('cross-activation',1,'reserved','tikhub',
                               'douyin_user_posts',?,'2026-09-01',?,?,?,'{}',
                               NULL,NULL,'paid-provider-dispatch-v1',?,?)""",
                    (
                        second["activation_id"],
                        permit_id,
                        legacy["activation_run"],
                        legacy["activation_attempt"],
                        "8" * 64,
                        STAMP,
                    ),
                )
            connection.rollback()

    def test_invalid_schema18_bridge_hash_aborts_without_partial_schema(self) -> None:
        path = self._v18()
        legacy = self._seed_bridges(path)
        with storage.connect(path) as connection:
            connection.execute(
                "UPDATE scheduler_runs SET details_json='{}' WHERE id=?",
                (legacy["scan_receipt_run"],),
            )
            connection.commit()
            before = "\n".join(connection.iterdump())
            with self.assertRaises(
                (runtime_receipts.RuntimeReceiptError, storage.SchemaMigrationError)
            ):
                storage.migrate_database(connection, from_version=18, to_version=19)
            self.assertEqual("\n".join(connection.iterdump()), before)
            self.assertEqual(storage.require_schema_compatibility(connection), 18)

    def test_v19_migration_checkpoints_are_atomic(self) -> None:
        for checkpoint in (
            "v19_preflight_complete",
            "v19_bridges_imported",
            "v19_before_commit",
        ):
            with self.subTest(checkpoint=checkpoint):
                path = self._v18(f"{checkpoint}.sqlite3")
                self._seed_bridges(path)
                with storage.connect(path) as connection:
                    before = "\n".join(connection.iterdump())

                    def inject(name: str) -> None:
                        if name == checkpoint:
                            raise RuntimeError("injected")

                    with patch.object(
                        storage, "_migration_checkpoint", side_effect=inject
                    ), self.assertRaisesRegex(RuntimeError, "injected"):
                        storage.migrate_database(
                            connection, from_version=18, to_version=19
                        )
                    self.assertEqual("\n".join(connection.iterdump()), before)
                    self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
                    self.assertEqual(
                        connection.execute("PRAGMA legacy_alter_table").fetchone()[0], 0
                    )
                    self.assertEqual(storage.require_schema_compatibility(connection), 18)


if __name__ == "__main__":
    unittest.main()
