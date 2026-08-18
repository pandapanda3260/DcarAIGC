from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.schema_fixture import initialize_historical_schema
import v8.storage as storage


CAPTURED_AT = "2026-08-15T00:00:00Z"


class V13SchemaMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "v13.sqlite3"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _downgrade_current_to_v12(self, connection: sqlite3.Connection) -> None:
        initialize_historical_schema(connection, target_version=12)
        self.assertEqual(storage.require_schema_compatibility(connection), 12)

    def _seed_v12_runs(
        self, connection: sqlite3.Connection
    ) -> tuple[list[str], str]:
        self._downgrade_current_to_v12(connection)
        rows = (
            (
                3,
                "daily_capture",
                "2026-08-01T18:00:00Z",
                "succeeded",
                "2026-08-01T18:00:01Z",
                "2026-08-01T18:01:00Z",
                '{"captured":3}',
            ),
            (
                7,
                "daily_report",
                "2026-08-02T00:00:00Z",
                "failed",
                "2026-08-02T00:00:01Z",
                "2026-08-02T00:00:02Z",
                '{"error":"gate"}',
            ),
            (
                11,
                "weekly_report",
                "2026-08-03T00:30:00Z",
                "skipped",
                "2026-08-03T00:30:01Z",
                "2026-08-03T00:30:01Z",
                '{"reason":"none"}',
            ),
            (
                19,
                "daily_media_cutoff",
                "2026-08-04T23:30:00Z",
                "running",
                "2026-08-04T23:30:01Z",
                None,
                "{}",
            ),
        )
        connection.executemany(
            """
            INSERT INTO scheduler_runs(
                id,job_id,scheduled_for,status,started_at,completed_at,details_json
            ) VALUES (?,?,?,?,?,?,?)
            """,
            rows,
        )
        connection.commit()
        columns = storage._table_columns(connection, "scheduler_runs")
        return columns, storage._table_projection_sha256(
            connection, "scheduler_runs", columns
        )

    def test_fresh_schema_has_v13_manifest_and_exact_scheduler_objects(self) -> None:
        with storage.connect(self.database) as connection:
            storage.initialize_database(connection)
            self.assertEqual(
                [
                    tuple(row)
                    for row in connection.execute(
                        "SELECT version,name FROM schema_migrations ORDER BY version"
                    )
                ],
                [
                    (9, "release-bound-evaluation-schema"),
                    (10, "audience-interaction-user-domain"),
                    *sorted(storage.SCHEMA_MIGRATION_NAMES.items()),
                ],
            )
            self.assertEqual(
                int(connection.execute("PRAGMA user_version").fetchone()[0]),
                storage.SCHEMA_VERSION,
            )
            storage._validate_v13_structure(connection)
            self.assertTrue(storage.schema_compatibility_state(connection)["compatible"])

    def test_v12_runs_are_backfilled_once_without_projection_drift(self) -> None:
        with storage.connect(self.database) as connection:
            columns, before_hash = self._seed_v12_runs(connection)
            before = [
                tuple(row)
                for row in connection.execute(
                    f"SELECT {','.join(columns)} FROM scheduler_runs ORDER BY id"
                )
            ]
            storage.initialize_database(connection)

            self.assertEqual(
                storage._table_projection_sha256(
                    connection, "scheduler_runs", columns
                ),
                before_hash,
            )
            attempts = connection.execute(
                """
                SELECT scheduler_run_id,attempt_number,invocation_source,status,
                       started_at,completed_at,details_json
                FROM scheduler_run_attempts ORDER BY scheduler_run_id
                """
            ).fetchall()
            self.assertEqual(len(attempts), len(before))
            self.assertEqual(
                [tuple(row) for row in attempts],
                [
                    (
                        row[0],
                        1,
                        "legacy_migration",
                        row[3],
                        row[4],
                        row[5],
                        row[6],
                    )
                    for row in before
                ],
            )
            changes = connection.total_changes
            storage.initialize_database(connection)
            self.assertEqual(connection.total_changes, changes)
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            self.assertEqual(connection.execute("PRAGMA quick_check").fetchone()[0], "ok")
            self.assertEqual(
                connection.execute("PRAGMA integrity_check").fetchone()[0], "ok"
            )

    def test_attempt_constraints_and_one_running_attempt_per_occurrence(self) -> None:
        with storage.connect(self.database) as connection:
            storage.initialize_database(connection)
            run_id = int(
                connection.execute(
                    """
                    INSERT INTO scheduler_runs(
                        job_id,scheduled_for,status,started_at,details_json
                    ) VALUES ('daily_report',?,'running',?,'{}')
                    """,
                    (CAPTURED_AT, CAPTURED_AT),
                ).lastrowid
            )
            connection.execute(
                """
                INSERT INTO scheduler_run_attempts(
                    scheduler_run_id,attempt_number,invocation_source,status,
                    started_at,details_json
                ) VALUES (?,1,'scheduled','running',?,'{}')
                """,
                (run_id, CAPTURED_AT),
            )
            connection.commit()
            for sql, parameters in (
                (
                    """
                    INSERT INTO scheduler_run_attempts(
                        scheduler_run_id,attempt_number,invocation_source,status,
                        started_at,details_json
                    ) VALUES (?,1,'operator_retry','running',?,'{}')
                    """,
                    (run_id, CAPTURED_AT),
                ),
                (
                    """
                    INSERT INTO scheduler_run_attempts(
                        scheduler_run_id,attempt_number,invocation_source,status,
                        started_at,details_json
                    ) VALUES (?,2,'operator_retry','running',?,'{}')
                    """,
                    (run_id, CAPTURED_AT),
                ),
                (
                    """
                    INSERT INTO scheduler_run_attempts(
                        scheduler_run_id,attempt_number,invocation_source,status,
                        started_at,details_json
                    ) VALUES (?,2,'unknown','running',?,'{}')
                    """,
                    (run_id, CAPTURED_AT),
                ),
                (
                    """
                    INSERT INTO scheduler_run_attempts(
                        scheduler_run_id,attempt_number,invocation_source,status,
                        started_at,details_json
                    ) VALUES (999,1,'scheduled','running',?,'{}')
                    """,
                    (CAPTURED_AT,),
                ),
            ):
                with self.subTest(sql=sql):
                    with self.assertRaises(sqlite3.IntegrityError):
                        connection.execute(sql, parameters)
                    connection.rollback()

    def test_attempt_update_is_exactly_once_and_delete_is_forbidden(self) -> None:
        with storage.connect(self.database) as connection:
            storage.initialize_database(connection)
            run_id = int(
                connection.execute(
                    """
                    INSERT INTO scheduler_runs(
                        job_id,scheduled_for,status,started_at,details_json
                    ) VALUES ('daily_report',?,'running',?,'{}')
                    """,
                    (CAPTURED_AT, CAPTURED_AT),
                ).lastrowid
            )
            attempt_id = int(
                connection.execute(
                    """
                    INSERT INTO scheduler_run_attempts(
                        scheduler_run_id,attempt_number,invocation_source,status,
                        started_at,details_json
                    ) VALUES (?,1,'scheduled','running',?,'{}')
                    """,
                    (run_id, CAPTURED_AT),
                ).lastrowid
            )
            connection.commit()
            with self.assertRaisesRegex(
                sqlite3.IntegrityError, "running-to-terminal"
            ):
                connection.execute(
                    "UPDATE scheduler_run_attempts SET details_json='{}' WHERE id=?",
                    (attempt_id,),
                )
            connection.rollback()

            connection.execute(
                """
                UPDATE scheduler_run_attempts
                SET status='partial',completed_at=?,details_json='{"coverage":57.18}'
                WHERE id=?
                """,
                ("2026-08-15T00:01:00Z", attempt_id),
            )
            connection.commit()
            with self.assertRaisesRegex(
                sqlite3.IntegrityError, "running-to-terminal"
            ):
                connection.execute(
                    "UPDATE scheduler_run_attempts SET status='succeeded' WHERE id=?",
                    (attempt_id,),
                )
            connection.rollback()
            with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                connection.execute(
                    "DELETE FROM scheduler_run_attempts WHERE id=?", (attempt_id,)
                )
            connection.rollback()

    def test_insert_or_replace_cannot_rewrite_terminal_or_active_attempts(self) -> None:
        with storage.connect(self.database) as connection:
            storage.initialize_database(connection)
            attempts: dict[str, tuple[int, int]] = {}
            for offset, (label, status, completed_at) in enumerate(
                (
                    ("terminal_primary", "succeeded", "2026-08-15T00:01:00Z"),
                    ("terminal_pending", "succeeded", "2026-08-15T00:02:00Z"),
                    ("active", "running", None),
                ),
                start=1,
            ):
                run_id = int(
                    connection.execute(
                        """
                        INSERT INTO scheduler_runs(
                            job_id,scheduled_for,status,started_at,completed_at,
                            details_json
                        ) VALUES ('daily_report',?,?,?,?,?)
                        """,
                        (
                            f"2026-08-{15 + offset:02d}T00:00:00Z",
                            status,
                            CAPTURED_AT,
                            completed_at,
                            '{"original":true}',
                        ),
                    ).lastrowid
                )
                attempt_id = int(
                    connection.execute(
                        """
                        INSERT INTO scheduler_run_attempts(
                            scheduler_run_id,attempt_number,invocation_source,status,
                            started_at,completed_at,details_json
                        ) VALUES (?,1,'scheduled',?,?,?,'{"original":true}')
                        """,
                        (run_id, status, CAPTURED_AT, completed_at),
                    ).lastrowid
                )
                attempts[label] = (run_id, attempt_id)
            connection.commit()

            run_id, attempt_id = attempts["terminal_primary"]
            cases = (
                (
                    "terminal_primary_key",
                    """
                    INSERT OR REPLACE INTO scheduler_run_attempts(
                        id,scheduler_run_id,attempt_number,invocation_source,status,
                        started_at,completed_at,details_json
                    ) VALUES (?, ?, 1, 'operator_retry', 'failed', ?, ?,
                              '{"tampered":true}')
                    """,
                    (
                        attempt_id,
                        run_id,
                        CAPTURED_AT,
                        "2026-08-15T01:00:00Z",
                    ),
                ),
                (
                    "terminal_business_key_reopened_as_pending",
                    """
                    INSERT OR REPLACE INTO scheduler_run_attempts(
                        scheduler_run_id,attempt_number,invocation_source,status,
                        started_at,details_json
                    ) VALUES (?, 1, 'operator_retry', 'running', ?,
                              '{"tampered":true}')
                    """,
                    (attempts["terminal_pending"][0], CAPTURED_AT),
                ),
                (
                    "active_business_key",
                    """
                    INSERT OR REPLACE INTO scheduler_run_attempts(
                        scheduler_run_id,attempt_number,invocation_source,status,
                        started_at,details_json
                    ) VALUES (?, 1, 'operator_retry', 'running', ?,
                              '{"tampered":true}')
                    """,
                    (attempts["active"][0], CAPTURED_AT),
                ),
            )
            for label, sql, parameters in cases:
                with self.subTest(label=label):
                    with self.assertRaisesRegex(
                        sqlite3.IntegrityError, "append-only"
                    ):
                        connection.execute(sql, parameters)
                    connection.rollback()

            self.assertEqual(
                [
                    tuple(row)
                    for row in connection.execute(
                        """
                        SELECT scheduler_run_id,status,invocation_source,details_json
                        FROM scheduler_run_attempts ORDER BY scheduler_run_id
                        """
                    )
                ],
                [
                    (
                        attempts["terminal_primary"][0],
                        "succeeded",
                        "scheduled",
                        '{"original":true}',
                    ),
                    (
                        attempts["terminal_pending"][0],
                        "succeeded",
                        "scheduled",
                        '{"original":true}',
                    ),
                    (
                        attempts["active"][0],
                        "running",
                        "scheduled",
                        '{"original":true}',
                    ),
                ],
            )

    def test_v13_checkpoint_failure_rolls_back_and_clean_retry_succeeds(self) -> None:
        for checkpoint in (
            "v13_scheduler_runs_rebuilt",
            "v13_scheduler_attempts_backfilled",
            "v13_scheduler_schema_stamped",
        ):
            with self.subTest(checkpoint=checkpoint), tempfile.TemporaryDirectory() as temp:
                database = Path(temp) / "rollback.sqlite3"
                with storage.connect(database) as connection:
                    columns, before_hash = self._seed_v12_runs(connection)
                    with patch.object(
                        storage,
                        "_migration_checkpoint",
                        side_effect=lambda name: (
                            (_ for _ in ()).throw(RuntimeError("injected v13 failure"))
                            if name == checkpoint
                            else None
                        ),
                    ):
                        with self.assertRaisesRegex(
                            RuntimeError, "injected v13 failure"
                        ):
                            storage.initialize_database(connection)

                    self.assertEqual(
                        int(connection.execute("PRAGMA user_version").fetchone()[0]),
                        12,
                    )
                    self.assertIsNone(
                        connection.execute(
                            """
                            SELECT 1 FROM sqlite_master
                            WHERE type='table' AND name='scheduler_run_attempts'
                            """
                        ).fetchone()
                    )
                    self.assertEqual(
                        storage._table_projection_sha256(
                            connection, "scheduler_runs", columns
                        ),
                        before_hash,
                    )
                    self.assertEqual(
                        storage._normalized_schema_sql(
                            str(
                                connection.execute(
                                    """
                                    SELECT sql FROM sqlite_master
                                    WHERE type='table' AND name='scheduler_runs'
                                    """
                                ).fetchone()[0]
                            )
                        ),
                        storage._normalized_schema_sql(storage._V12_SCHEDULER_RUNS_SQL),
                    )

                    storage.initialize_database(connection)
                    self.assertEqual(
                        int(connection.execute("PRAGMA user_version").fetchone()[0]),
                        storage.SCHEMA_VERSION,
                    )
                    self.assertEqual(
                        int(
                            connection.execute(
                                "SELECT COUNT(*) FROM scheduler_run_attempts"
                            ).fetchone()[0]
                        ),
                        4,
                    )

    def test_v11_ladder_runs_v12_then_v13(self) -> None:
        with storage.connect(self.database) as connection:
            initialize_historical_schema(connection, target_version=11)
            connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id,scheduled_for,status,started_at,completed_at,details_json
                ) VALUES ('daily_report',?,'failed',?,?,?)
                """,
                (
                    CAPTURED_AT,
                    CAPTURED_AT,
                    "2026-08-15T00:01:00Z",
                    '{"error":"legacy"}',
                ),
            )
            connection.commit()

            storage.initialize_database(connection)
            self.assertEqual(
                int(connection.execute("PRAGMA user_version").fetchone()[0]),
                storage.SCHEMA_VERSION,
            )
            self.assertEqual(
                [
                    tuple(row)
                    for row in connection.execute(
                        """
                        SELECT attempt_number,invocation_source,status,details_json
                        FROM scheduler_run_attempts
                        """
                    )
                ],
                [(1, "legacy_migration", "failed", '{"error":"legacy"}')],
            )

    def test_partial_v13_residue_and_counterfeit_objects_fail_closed(self) -> None:
        with storage.connect(self.database) as connection:
            self._downgrade_current_to_v12(connection)
            connection.execute(
                "CREATE TABLE scheduler_run_attempts(id INTEGER PRIMARY KEY)"
            )
            connection.commit()
            with self.assertRaisesRegex(
                storage.SchemaMigrationError, "objects already exist"
            ):
                storage.initialize_database(connection)
            self.assertEqual(
                int(connection.execute("PRAGMA user_version").fetchone()[0]), 12
            )

        counterfeit = Path(self.temporary.name) / "counterfeit.sqlite3"
        with storage.connect(counterfeit) as connection:
            storage.initialize_database(connection)
            connection.execute("DROP TABLE scheduler_run_attempts")
            connection.execute(
                """
                CREATE TABLE scheduler_run_attempts(
                    id INTEGER PRIMARY KEY,
                    scheduler_run_id INTEGER,
                    attempt_number INTEGER,
                    invocation_source TEXT,
                    status TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    details_json TEXT
                )
                """
            )
            connection.commit()
            self.assertFalse(
                storage.schema_compatibility_state(connection)["compatible"]
            )
            with self.assertRaisesRegex(
                storage.SchemaMigrationError,
                "scheduler attempt columns drifted|object definition drifted",
            ):
                storage.initialize_database(connection)


if __name__ == "__main__":
    unittest.main()
