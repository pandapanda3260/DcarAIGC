from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from v8.metric_observations import (
    MetricObservationError,
    persist_metric_observation,
    rebuild_metric_snapshots,
)
from v8.operations import IdentityConflictError, merge_content_records
from v8.source_routing import select_content_metrics
from v8.storage import connect, initialize_database, metric_observation_sha256, transaction


WINDOW = "2026-08-01"
CUTOFF = "2026-08-29T00:00:00Z"


class MetricIdentityMergeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = self.root / "merge.sqlite3"
        with connect(self.database) as connection:
            initialize_database(connection)
            connection.executemany(
                """INSERT INTO content_items(
                    id,link_id,platform,canonical_url,imported_at,created_at,updated_at
                ) VALUES (?,?,'douyin',?,'2026-07-01T00:00:00Z',?,?)""",
                [
                    (1, "MER001", "https://example.invalid/first",
                     "2026-07-01T00:00:00Z", "2026-07-01T00:00:00Z"),
                    (2, "MER002", "https://example.invalid/second",
                     "2026-07-02T00:00:00Z", "2026-07-02T00:00:00Z"),
                ],
            )
            connection.commit()

    def _fact(self, connection, content_id, *, day, value, window=WINDOW, legacy=True):
        captured = f"2026-08-{day:02d}T00:00:00Z"
        body = json.dumps({"content_id": content_id, "view_count": value, "day": day}).encode()
        digest = hashlib.sha256(body).hexdigest()
        raw_path = self.root / f"raw-{digest}.json"
        raw_path.write_bytes(body)
        slot = connection.execute(
            """INSERT INTO fetch_slots(
                content_id,stage,window_key,provider,adapter_version,status,
                attempt_count,created_at,updated_at
            ) VALUES (?,'metrics',?,'TikHub','fixture-v1','succeeded',1,?,?)""",
            (content_id, f"merge-fixture:{day}:{window}", captured, captured),
        )
        attempt = connection.execute(
            """INSERT INTO fetch_attempts(
                slot_id,attempt_number,request_started_at,response_finished_at,
                http_status,billed
            ) VALUES (?,1,?,?,200,0)""",
            (int(slot.lastrowid), captured, captured),
        )
        raw = connection.execute(
            """INSERT INTO provider_raw_responses(
                fetch_attempt_id,content_id,provider,operation,local_path,sha256,
                byte_size,http_status,captured_at
            ) VALUES (?,?,'TikHub','douyin_video_statistics',?,?,?,200,?)""",
            (
                int(attempt.lastrowid),
                content_id,
                str(raw_path),
                digest,
                len(body),
                captured,
            ),
        )
        return persist_metric_observation(
            connection, content_id=content_id, captured_at=captured, window_key=window,
            view_count=value, comment_count=value // 10, like_count=value // 2,
            share_count=1, collect_count=2, status="available", source="douyin",
            raw_response_id=raw.lastrowid, metadata_json='{"fixture":"metric-merge"}',
            observation_origin="legacy_snapshot_baseline" if legacy else "provider_capture",
            recorded_at=captured,
        )

    def _rows(self, connection, table):
        return [dict(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]

    def _assert_facts_preserved(self, connection, before) -> None:
        after = {row["id"]: row for row in self._rows(connection, "content_metric_observations")}
        for original in before:
            self.assertEqual(after[original["id"]], {**original, "content_id": 1})
            row = after[original["id"]]
            self.assertEqual(row["observation_sha256"], metric_observation_sha256(**{
                name: row[name] for name in (
                    "observation_origin", "legacy_snapshot_id", "subject_key", "captured_at",
                    "window_key", "view_count", "comment_count", "like_count", "share_count",
                    "collect_count", "status", "source", "raw_response_id", "metadata_json",
                )
            }))

    def test_overlapping_and_disjoint_windows_preserve_ids_facts_and_future_writes(self) -> None:
        with connect(self.database) as connection:
            with transaction(connection):
                original = self._fact(connection, 1, day=1, value=100)
                duplicate = self._fact(connection, 2, day=2, value=200)
                disjoint = self._fact(connection, 2, day=1, value=75, window="2026-07-31")
            facts = self._rows(connection, "content_metric_observations")
            raw = self._rows(connection, "provider_raw_responses")
            with transaction(connection):
                self.assertEqual(merge_content_records(connection, 1, 2), 1)
            self._assert_facts_preserved(connection, facts)
            self.assertEqual(len(self._rows(connection, "content_metric_observations")), 3)
            self.assertEqual(self._rows(connection, "provider_raw_responses"),
                             [{**row, "content_id": 1} for row in raw])
            self.assertIsNone(connection.execute("SELECT 1 FROM content_items WHERE id=2").fetchone())
            snapshots = self._rows(connection, "content_metric_snapshots")
            self.assertEqual({row["window_key"]: row["id"] for row in snapshots},
                             {WINDOW: original.snapshot_id, "2026-07-31": disjoint.snapshot_id})
            self.assertNotIn(duplicate.snapshot_id, {row["id"] for row in snapshots})
            selected = select_content_metrics(connection, [1], cutoff_at=CUTOFF, window_key=WINDOW)[1]
            self.assertEqual(selected["legacy_snapshot_id"], duplicate.snapshot_id)
            self.assertEqual(selected["view_count"], 200)
            self.assertEqual(next(row for row in snapshots if row["window_key"] == WINDOW)["view_count"], 200)

            # The selector still references the retired loser baseline after
            # a later provider append; the surviving canonical ID must persist.
            with transaction(connection):
                appended = self._fact(connection, 1, day=3, value=300, legacy=False)
            self.assertEqual(appended.snapshot_id, original.snapshot_id)
            self._assert_facts_preserved(connection, facts)
            self.assertEqual(len(self._rows(connection, "content_metric_observations")), 4)
            with transaction(connection):
                rebuilt = rebuild_metric_snapshots(connection, [1], cutoff_at=CUTOFF)
                repeated = rebuild_metric_snapshots(connection, [1], cutoff_at=CUTOFF)
            rebuilt_ids = rebuilt["snapshot_ids"]
            assert isinstance(rebuilt_ids, list)
            self.assertEqual(set(rebuilt_ids), {original.snapshot_id, disjoint.snapshot_id})
            self.assertEqual(repeated["changed"], 0)
            row = connection.execute(
                "SELECT id,view_count FROM content_metric_snapshots WHERE content_id=1 AND window_key=?",
                (WINDOW,),
            ).fetchone()
            self.assertEqual(tuple(row), (original.snapshot_id, 300))
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_provider_survivor_can_absorb_legacy_loser_and_continue_rebuilding(self) -> None:
        with connect(self.database) as connection:
            with transaction(connection):
                original = self._fact(connection, 1, day=1, value=100, legacy=False)
                duplicate = self._fact(connection, 2, day=2, value=200)
            facts = self._rows(connection, "content_metric_observations")
            with transaction(connection):
                merge_content_records(connection, 1, 2)
                first = rebuild_metric_snapshots(connection, [1], cutoff_at=CUTOFF)
            self.assertEqual(first["snapshot_ids"], [original.snapshot_id])
            self._assert_facts_preserved(connection, facts)
            selected = select_content_metrics(connection, [1], cutoff_at=CUTOFF, window_key=WINDOW)[1]
            self.assertEqual(selected["legacy_snapshot_id"], duplicate.snapshot_id)
            with transaction(connection):
                appended = self._fact(connection, 1, day=3, value=350, legacy=False)
                rebuild_metric_snapshots(connection, [1], cutoff_at=CUTOFF)
            self.assertEqual(appended.snapshot_id, original.snapshot_id)
            self._assert_facts_preserved(connection, facts)

    def test_deleted_legacy_reference_without_merge_alias_is_rejected(self) -> None:
        with connect(self.database) as connection:
            with transaction(connection):
                self._fact(connection, 1, day=1, value=100)
                duplicate = self._fact(connection, 2, day=2, value=200)
                # A payload-preserving but unproven manual rekey is NOT a
                # canonical merge receipt and cannot relax snapshot identity.
                connection.execute("UPDATE content_metric_observations SET content_id=1 WHERE content_id=2")
                connection.execute("UPDATE provider_raw_responses SET content_id=1 WHERE content_id=2")
                connection.execute("DELETE FROM content_metric_snapshots WHERE id=?", (duplicate.snapshot_id,))
            before = self._rows(connection, "content_metric_snapshots")
            with self.assertRaisesRegex(MetricObservationError, "legacy snapshot identity would change"):
                with transaction(connection):
                    rebuild_metric_snapshots(connection, [1], cutoff_at=CUTOFF)
            self.assertEqual(self._rows(connection, "content_metric_snapshots"), before)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_aliases").fetchone()[0], 0)

    def test_projection_failure_rolls_back_metric_facts_raw_aliases_and_content(self) -> None:
        with connect(self.database) as connection:
            with transaction(connection):
                self._fact(connection, 1, day=1, value=100)
                self._fact(connection, 2, day=2, value=200)
            tables = ("content_items", "content_aliases", "provider_raw_responses",
                      "content_metric_observations", "content_metric_snapshots")
            before = {table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table}")]
                      for table in tables}
            connection.executescript(
                """CREATE TRIGGER reject_merged_projection BEFORE INSERT ON content_metric_snapshots
                   BEGIN SELECT RAISE(ABORT,'fixture projection write rejected'); END;"""
            )
            with self.assertRaises(IdentityConflictError):
                merge_content_records(connection, 1, 2)
            for table in tables:
                self.assertEqual([tuple(row) for row in connection.execute(f"SELECT * FROM {table}")], before[table])
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM duplicate_relations WHERE method='identity_conflict'"
            ).fetchone()[0], 1)

    def test_snapshot_only_overlap_is_not_discarded(self) -> None:
        with connect(self.database) as connection:
            connection.executemany(
                """INSERT INTO content_metric_snapshots(
                    content_id,captured_at,window_key,view_count,status,source
                ) VALUES (?,'2026-08-01T00:00:00Z',?,?,'stale','douyin')""",
                [(1, WINDOW, 100), (2, WINDOW, 200)],
            )
            connection.commit()
            before = self._rows(connection, "content_metric_snapshots")
            with self.assertRaisesRegex(MetricObservationError, "lack immutable facts"):
                merge_content_records(connection, 1, 2)
            self.assertEqual(self._rows(connection, "content_metric_snapshots"), before)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 2)


if __name__ == "__main__":
    unittest.main()
