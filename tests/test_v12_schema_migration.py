from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import v8.storage as storage
from v8.operations import merge_content_records


CAPTURED_AT = "2026-08-11T00:00:00Z"


class V12SchemaMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "v12.sqlite3"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _seed_v11_metric_snapshots(
        self, connection: sqlite3.Connection
    ) -> tuple[list[str], str]:
        storage.initialize_database(connection)
        connection.executemany(
            """
            INSERT INTO content_items(
                id,link_id,platform,platform_content_id,canonical_url,
                imported_at,created_at,updated_at
            ) VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                (
                    1,
                    "V12A01",
                    "douyin",
                    "1001",
                    "https://example.com/1",
                    CAPTURED_AT,
                    CAPTURED_AT,
                    CAPTURED_AT,
                ),
                (
                    2,
                    "V12A02",
                    "xiaohongshu",
                    "2002",
                    "https://example.com/2",
                    CAPTURED_AT,
                    CAPTURED_AT,
                    CAPTURED_AT,
                ),
                (
                    3,
                    "V12A03",
                    "douyin",
                    "3003",
                    "https://example.com/3",
                    CAPTURED_AT,
                    CAPTURED_AT,
                    CAPTURED_AT,
                ),
            ),
        )
        connection.executemany(
            """
            INSERT INTO content_identities(
                content_id,identity_kind,identity_value,platform_identity_key,
                is_primary,created_at
            ) VALUES (?,'platform_content_id',?,?,1,?)
            """,
            (
                (1, "1001", "douyin:1001", CAPTURED_AT),
                (3, "3003", "douyin:3003", CAPTURED_AT),
            ),
        )
        connection.executemany(
            """
            INSERT INTO provider_raw_responses(
                id,content_id,provider,operation,local_path,sha256,byte_size,
                http_status,captured_at,source
            ) VALUES (?,?,?,'metrics',?,?,?,200,?,'live')
            """,
            (
                (1, 1, "TikHub", "raw/1.json", "a" * 64, 101, CAPTURED_AT),
                (2, 2, "XHS", "raw/2.json", "b" * 64, 102, CAPTURED_AT),
            ),
        )
        connection.executemany(
            """
            INSERT INTO content_metric_snapshots(
                id,content_id,captured_at,window_key,view_count,comment_count,
                like_count,share_count,collect_count,status,source,
                raw_response_id,metadata_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                (
                    11,
                    1,
                    CAPTURED_AT,
                    "2026-08-10",
                    100,
                    10,
                    20,
                    2,
                    3,
                    "available",
                    "TikHub",
                    1,
                    '{"z":1,"a":2}',
                ),
                (
                    12,
                    2,
                    CAPTURED_AT,
                    "2026-08-10",
                    None,
                    None,
                    None,
                    None,
                    None,
                    "missing",
                    "XHS",
                    2,
                    '{"reason":"missing"}',
                ),
                (
                    13,
                    3,
                    "2026-07-01T00:00:00Z",
                    "migrated",
                    7,
                    1,
                    2,
                    None,
                    None,
                    "stale",
                    "migrated_historical",
                    None,
                    "{}",
                ),
            ),
        )
        connection.execute("DROP TABLE scheduler_run_attempts")
        connection.execute(
            "ALTER TABLE scheduler_runs RENAME TO scheduler_runs_v13_current"
        )
        connection.execute(storage._V12_SCHEDULER_RUNS_SQL)
        scheduler_columns = (
            "id,job_id,scheduled_for,status,started_at,completed_at,details_json"
        )
        connection.execute(
            f"INSERT INTO scheduler_runs({scheduler_columns}) "
            f"SELECT {scheduler_columns} FROM scheduler_runs_v13_current"
        )
        connection.execute("DROP TABLE scheduler_runs_v13_current")
        connection.execute("DELETE FROM schema_migrations WHERE version=13")
        connection.execute("PRAGMA user_version=12")
        connection.execute("DROP TABLE content_metric_observations")
        connection.execute("DROP INDEX idx_content_identities_content_primary")
        connection.execute("DELETE FROM schema_migrations WHERE version=12")
        connection.execute("PRAGMA user_version=11")
        connection.commit()
        columns = storage._table_columns(connection, "content_metric_snapshots")
        return columns, storage._table_projection_sha256(
            connection, "content_metric_snapshots", columns
        )

    def test_fresh_schema_has_v12_manifest_and_immutable_observation_objects(
        self,
    ) -> None:
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
                    (11, "interaction-user-v1-fallback-keys"),
                    (12, "append-only-metric-observations"),
                    (13, "scheduler-run-attempt-history"),
                ],
            )
            self.assertEqual(
                int(connection.execute("PRAGMA user_version").fetchone()[0]), 13
            )
            self.assertIn(
                "content_metric_observations", storage._table_names(connection)
            )
            self.assertIsNotNone(
                connection.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type='index'
                      AND name='idx_content_identities_content_primary'
                    """
                ).fetchone()
            )
            self.assertIsNotNone(
                connection.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type='trigger'
                      AND name='trg_metric_observations_immutable_payload'
                    """
                ).fetchone()
            )
            self.assertIsNotNone(
                connection.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type='trigger'
                      AND name='trg_metric_observations_no_delete'
                    """
                ).fetchone()
            )

    def test_v11_snapshots_are_backfilled_exactly_once_without_projection_drift(
        self,
    ) -> None:
        with storage.connect(self.database) as connection:
            columns, before_hash = self._seed_v11_metric_snapshots(connection)
            storage.initialize_database(connection)

            self.assertEqual(
                storage._table_projection_sha256(
                    connection, "content_metric_snapshots", columns
                ),
                before_hash,
            )
            rows = connection.execute(
                """
                SELECT * FROM content_metric_observations
                ORDER BY legacy_snapshot_id
                """
            ).fetchall()
            self.assertEqual(len(rows), 3)
            self.assertEqual(
                [str(row["subject_key"]) for row in rows],
                ["douyin:1001", "link:V12A02", "douyin:3003"],
            )
            self.assertEqual([row["raw_response_id"] for row in rows], [1, 2, None])
            self.assertEqual(
                {str(row["observation_origin"]) for row in rows},
                {"legacy_snapshot_baseline"},
            )
            for row in rows:
                self.assertEqual(
                    str(row["observation_sha256"]),
                    storage.metric_observation_sha256(
                        observation_origin=str(row["observation_origin"]),
                        legacy_snapshot_id=int(row["legacy_snapshot_id"]),
                        subject_key=str(row["subject_key"]),
                        captured_at=str(row["captured_at"]),
                        window_key=str(row["window_key"]),
                        view_count=row["view_count"],
                        comment_count=row["comment_count"],
                        like_count=row["like_count"],
                        share_count=row["share_count"],
                        collect_count=row["collect_count"],
                        status=str(row["status"]),
                        source=str(row["source"]),
                        raw_response_id=row["raw_response_id"],
                        metadata_json=str(row["metadata_json"]),
                    ),
                )
            changes = connection.total_changes
            storage.initialize_database(connection)
            self.assertEqual(connection.total_changes, changes)
            self.assertIsNone(connection.execute("PRAGMA foreign_key_check").fetchone())
            self.assertEqual(
                str(connection.execute("PRAGMA integrity_check").fetchone()[0]), "ok"
            )

    def test_v12_checkpoint_failure_rolls_back_and_clean_retry_succeeds(self) -> None:
        with storage.connect(self.database) as connection:
            columns, before_hash = self._seed_v11_metric_snapshots(connection)
            with patch.object(
                storage,
                "_migration_checkpoint",
                side_effect=lambda name: (
                    (_ for _ in ()).throw(RuntimeError("injected v12 failure"))
                    if name == "v12_metric_observations_backfilled"
                    else None
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "injected v12 failure"):
                    storage.initialize_database(connection)

            self.assertEqual(
                int(connection.execute("PRAGMA user_version").fetchone()[0]), 11
            )
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT MAX(version) FROM schema_migrations"
                    ).fetchone()[0]
                ),
                11,
            )
            self.assertNotIn(
                "content_metric_observations", storage._table_names(connection)
            )
            self.assertIsNone(
                connection.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type='index'
                      AND name='idx_content_identities_content_primary'
                    """
                ).fetchone()
            )
            self.assertEqual(
                storage._table_projection_sha256(
                    connection, "content_metric_snapshots", columns
                ),
                before_hash,
            )

            storage.initialize_database(connection)
            self.assertEqual(
                int(connection.execute("PRAGMA user_version").fetchone()[0]), 13
            )
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT COUNT(*) FROM content_metric_observations"
                    ).fetchone()[0]
                ),
                3,
            )

    def test_v12_partial_preexisting_object_fails_closed(self) -> None:
        with storage.connect(self.database) as connection:
            self._seed_v11_metric_snapshots(connection)
            connection.execute(
                "CREATE TABLE content_metric_observations(id INTEGER PRIMARY KEY)"
            )
            connection.commit()
            with self.assertRaisesRegex(
                storage.SchemaMigrationError, "objects already exist"
            ):
                storage.initialize_database(connection)
            self.assertEqual(
                int(connection.execute("PRAGMA user_version").fetchone()[0]), 11
            )

    def test_manifest_and_pragma_version_mismatch_fails_before_writes(self) -> None:
        with storage.connect(self.database) as connection:
            storage.initialize_database(connection)
            connection.execute("PRAGMA user_version=11")
            before = connection.total_changes
            with self.assertRaisesRegex(
                storage.SchemaMigrationError,
                "manifest and PRAGMA user_version disagree",
            ):
                storage.initialize_database(connection)
            self.assertEqual(connection.total_changes, before)
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT MAX(version) FROM schema_migrations"
                    ).fetchone()[0]
                ),
                13,
            )
            self.assertEqual(
                int(connection.execute("PRAGMA user_version").fetchone()[0]), 11
            )

    def test_counterfeit_v12_objects_fail_structural_compatibility(self) -> None:
        with storage.connect(self.database) as connection:
            storage.initialize_database(connection)
            connection.execute("DROP TABLE content_metric_observations")
            connection.executescript(
                """
                CREATE TABLE content_metric_observations(
                    id INTEGER PRIMARY KEY,
                    content_id INTEGER,
                    captured_at TEXT
                );
                CREATE INDEX idx_metric_observations_content_capture
                ON content_metric_observations(content_id,captured_at DESC,id DESC);
                CREATE TRIGGER trg_metric_observations_immutable_payload
                BEFORE UPDATE ON content_metric_observations
                BEGIN
                    SELECT 1;
                END;
                CREATE TRIGGER trg_metric_observations_no_delete
                BEFORE DELETE ON content_metric_observations
                BEGIN
                    SELECT 1;
                END;
                """
            )
            connection.commit()
            self.assertFalse(
                storage.schema_compatibility_state(connection)["compatible"]
            )
            with self.assertRaisesRegex(
                storage.SchemaMigrationError,
                "metric observation columns|object definition drifted",
            ):
                storage.initialize_database(connection)

    def test_observation_payload_is_immutable_but_content_rekey_is_allowed(
        self,
    ) -> None:
        with storage.connect(self.database) as connection:
            self._seed_v11_metric_snapshots(connection)
            storage.initialize_database(connection)
            observation = connection.execute(
                "SELECT * FROM content_metric_observations WHERE legacy_snapshot_id=11"
            ).fetchone()
            self.assertIsNotNone(observation)
            with self.assertRaisesRegex(
                sqlite3.IntegrityError, "observations are append-only"
            ):
                connection.execute(
                    "DELETE FROM content_metric_observations WHERE id=?",
                    (observation["id"],),
                )
            connection.rollback()
            with self.assertRaisesRegex(
                sqlite3.IntegrityError, "observation payload is immutable"
            ):
                connection.execute(
                    "UPDATE content_metric_observations SET id=100 WHERE id=?",
                    (observation["id"],),
                )
            connection.rollback()
            with self.assertRaisesRegex(
                sqlite3.IntegrityError, "observation payload is immutable"
            ):
                connection.execute(
                    "UPDATE content_metric_observations SET view_count=101 WHERE id=?",
                    (observation["id"],),
                )
            connection.rollback()

            connection.execute(
                "UPDATE content_metric_observations SET content_id=2 WHERE id=?",
                (observation["id"],),
            )
            connection.commit()
            moved = connection.execute(
                "SELECT * FROM content_metric_observations WHERE id=?",
                (observation["id"],),
            ).fetchone()
            self.assertEqual(int(moved["content_id"]), 2)
            self.assertEqual(str(moved["subject_key"]), "douyin:1001")
            self.assertEqual(
                str(moved["observation_sha256"]),
                str(observation["observation_sha256"]),
            )

            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO content_metric_observations(
                        content_id,subject_key,captured_at,window_key,status,source,
                        metadata_json,observation_origin,observation_sha256,recorded_at
                    ) VALUES (1,'douyin:other',?,'other','missing','test','{}',
                              'provider_capture',?,?)
                    """,
                    (
                        CAPTURED_AT,
                        observation["observation_sha256"],
                        CAPTURED_AT,
                    ),
                )
            connection.rollback()

            later = "2026-08-12T00:00:00Z"
            digest = storage.metric_observation_sha256(
                observation_origin="provider_capture",
                legacy_snapshot_id=None,
                subject_key="douyin:1001",
                captured_at=later,
                window_key="2026-08-11",
                view_count=110,
                comment_count=11,
                like_count=21,
                share_count=3,
                collect_count=4,
                status="available",
                source="TikHub",
                raw_response_id=1,
                metadata_json="{}",
            )
            connection.execute(
                """
                INSERT INTO content_metric_observations(
                    content_id,subject_key,captured_at,window_key,
                    view_count,comment_count,like_count,share_count,collect_count,
                    status,source,raw_response_id,metadata_json,observation_origin,
                    observation_sha256,recorded_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    1,
                    "douyin:1001",
                    later,
                    "2026-08-11",
                    110,
                    11,
                    21,
                    3,
                    4,
                    "available",
                    "TikHub",
                    1,
                    "{}",
                    "provider_capture",
                    digest,
                    later,
                ),
            )
            connection.commit()
            self.assertEqual(
                int(
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM content_metric_observations
                        WHERE subject_key='douyin:1001'
                        """
                    ).fetchone()[0]
                ),
                2,
            )

    def test_content_identity_merge_rekeys_observations_before_loser_delete(
        self,
    ) -> None:
        with storage.connect(self.database) as connection:
            self._seed_v11_metric_snapshots(connection)
            storage.initialize_database(connection)
            survivor = merge_content_records(connection, 1, 2)
            connection.commit()
            self.assertEqual(survivor, 1)
            self.assertIsNone(
                connection.execute("SELECT 1 FROM content_items WHERE id=2").fetchone()
            )
            moved = connection.execute(
                """
                SELECT content_id,subject_key FROM content_metric_observations
                WHERE legacy_snapshot_id=12
                """
            ).fetchone()
            self.assertEqual(int(moved["content_id"]), 1)
            self.assertEqual(str(moved["subject_key"]), "link:V12A02")


if __name__ == "__main__":
    unittest.main()
