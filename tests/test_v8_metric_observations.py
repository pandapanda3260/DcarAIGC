from __future__ import annotations

import sqlite3
import re
import tempfile
import unittest
from pathlib import Path

from v8.metric_observations import MetricObservationError, persist_metric_observation
from v8.storage import connect, initialize_database, transaction


CAPTURED_AT = "2026-08-11T00:00:00Z"


class MetricObservationsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "metrics.sqlite3"
        with connect(self.database) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO content_items(
                    id,link_id,platform,platform_content_id,canonical_url,
                    imported_at,created_at,updated_at
                ) VALUES (1,'MET001','douyin','1001','https://example.com/1',?,?,?)
                """,
                (CAPTURED_AT, CAPTURED_AT, CAPTURED_AT),
            )
            connection.execute(
                """
                INSERT INTO content_identities(
                    content_id,identity_kind,identity_value,platform_identity_key,
                    is_primary,created_at
                ) VALUES (1,'platform_content_id','1001','douyin:1001',1,?)
                """,
                (CAPTURED_AT,),
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _persist(
        self,
        connection: sqlite3.Connection,
        *,
        captured_at: str = CAPTURED_AT,
        view_count: int | None = 100,
        status: str = "available",
        metadata_json: str = "{}",
        snapshot_mode: str = "merge",
        observation_origin: str = "provider_capture",
    ):
        return persist_metric_observation(
            connection,
            content_id=1,
            captured_at=captured_at,
            window_key="2026-08-10",
            view_count=view_count,
            comment_count=10,
            like_count=20,
            share_count=2,
            collect_count=3,
            status=status,
            source="douyin",
            raw_response_id=None,
            metadata_json=metadata_json,
            snapshot_mode=snapshot_mode,  # type: ignore[arg-type]
            observation_origin=observation_origin,  # type: ignore[arg-type]
            recorded_at="2026-08-11T01:00:00Z",
        )

    def test_requires_an_existing_caller_transaction(self) -> None:
        with connect(self.database) as connection:
            with self.assertRaisesRegex(
                MetricObservationError, "requires an active caller transaction"
            ):
                self._persist(connection)

    def test_production_snapshot_writes_are_centralized(self) -> None:
        package = Path(__file__).resolve().parents[1] / "src" / "dcar_eval" / "v8"
        direct_write = re.compile(
            r"(?:INSERT\s+INTO|UPDATE)\s+content_metric_snapshots",
            re.IGNORECASE,
        )
        offenders = []
        for path in sorted(package.glob("*.py")):
            if path.name == "metric_observations.py":
                continue
            if direct_write.search(path.read_text(encoding="utf-8")):
                offenders.append(path.name)
        self.assertEqual(offenders, [])

    def test_exact_replay_reuses_observation_and_restores_missing_snapshot(self) -> None:
        with connect(self.database) as connection:
            with transaction(connection):
                first = self._persist(connection)
            self.assertTrue(first.observation_created)
            connection.execute("DELETE FROM content_metric_snapshots")
            connection.commit()

            with transaction(connection):
                replay = self._persist(connection)
            self.assertFalse(replay.observation_created)
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT COUNT(*) FROM content_metric_observations"
                    ).fetchone()[0]
                ),
                1,
            )
            snapshot = connection.execute(
                "SELECT * FROM content_metric_snapshots"
            ).fetchone()
            self.assertEqual(int(snapshot["view_count"]), 100)
            self.assertEqual(str(snapshot["captured_at"]), CAPTURED_AT)

    def test_placeholder_is_audited_without_overwriting_available_exposure(self) -> None:
        with connect(self.database) as connection:
            with transaction(connection):
                self._persist(connection)
            with transaction(connection):
                placeholder = self._persist(
                    connection,
                    captured_at="2026-08-11T02:00:00Z",
                    view_count=None,
                    status="missing",
                    metadata_json='{"exposure_observation":"missing_or_placeholder"}',
                    snapshot_mode="preserve_existing_exposure",
                )
            self.assertTrue(placeholder.observation_created)
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT COUNT(*) FROM content_metric_observations"
                    ).fetchone()[0]
                ),
                2,
            )
            snapshot = connection.execute(
                "SELECT * FROM content_metric_snapshots"
            ).fetchone()
            self.assertEqual(int(snapshot["view_count"]), 100)
            self.assertEqual(str(snapshot["captured_at"]), CAPTURED_AT)

    def test_system_correction_replaces_projection_and_preserves_both_facts(self) -> None:
        with connect(self.database) as connection:
            with transaction(connection):
                self._persist(connection, view_count=0)
            with transaction(connection):
                self._persist(
                    connection,
                    view_count=None,
                    status="missing",
                    metadata_json='{"repair_reason":"invalid_discovery_exposure"}',
                    snapshot_mode="replace",
                    observation_origin="system_correction",
                )
            rows = connection.execute(
                """
                SELECT observation_origin,view_count,status
                FROM content_metric_observations ORDER BY id
                """
            ).fetchall()
            self.assertEqual(
                [tuple(row) for row in rows],
                [
                    ("provider_capture", 0, "available"),
                    ("system_correction", None, "missing"),
                ],
            )
            snapshot = connection.execute(
                "SELECT view_count,status FROM content_metric_snapshots"
            ).fetchone()
            self.assertEqual(tuple(snapshot), (None, "missing"))

    def test_observation_and_snapshot_roll_back_together(self) -> None:
        with connect(self.database) as connection:
            connection.execute(
                """
                CREATE TRIGGER fail_metric_snapshot_insert
                BEFORE INSERT ON content_metric_snapshots
                BEGIN
                    SELECT RAISE(ABORT,'injected snapshot failure');
                END
                """
            )
            connection.commit()
            with self.assertRaisesRegex(sqlite3.IntegrityError, "injected snapshot"):
                with transaction(connection):
                    self._persist(connection)
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT COUNT(*) FROM content_metric_observations"
                    ).fetchone()[0]
                ),
                0,
            )
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT COUNT(*) FROM content_metric_snapshots"
                    ).fetchone()[0]
                ),
                0,
            )
            connection.execute("DROP TRIGGER fail_metric_snapshot_insert")
            connection.commit()
            with transaction(connection):
                self._persist(connection)
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT COUNT(*) FROM content_metric_observations"
                    ).fetchone()[0]
                ),
                1,
            )


if __name__ == "__main__":
    unittest.main()
