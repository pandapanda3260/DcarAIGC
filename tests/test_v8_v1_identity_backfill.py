"""Schema v11 + legacy v1-key identity backfill (2026-08-07 owner decision).

Pre-v8.4 comments only carry content-scoped ``content-user-hmac-v1`` keys;
raw uids were never stored, so platform-level keys are underivable. The v11
migration widens ``interaction_users.key_version`` and the backfill script
turns those keys into real interaction users so historical windows publish a
user universe instead of "missing".
"""

from __future__ import annotations

import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path

import v8.audience_rate as ar
import v8.storage as storage

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BACKFILL_SCRIPT = PROJECT_ROOT / "scripts" / "backfill_legacy_interaction_identity.py"


def _load_backfill_module():
    spec = importlib.util.spec_from_file_location(
        "backfill_legacy_interaction_identity", BACKFILL_SCRIPT
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _seed_contents(connection: sqlite3.Connection, count: int = 2) -> None:
    with storage.transaction(connection):
        for content_id in range(1, count + 1):
            connection.execute(
                """
                INSERT INTO content_items(
                    id, link_id, platform, canonical_url, published_at,
                    imported_at, created_at, updated_at
                ) VALUES (?, ?, 'douyin', ?, '2026-07-28T00:00:00Z',
                          '2026-07-28T00:00:00Z','2026-07-28T00:00:00Z','2026-07-28T00:00:00Z')
                """,
                (content_id, f"LNK{content_id:03d}", f"https://www.douyin.com/video/{content_id}"),
            )
            connection.execute(
                """
                INSERT INTO comment_evidence_versions(
                    id, content_id, captured_at, iso_week, source, local_path,
                    sha256, comment_count, status, created_at
                ) VALUES (?, ?, '2026-08-03T07:00:00Z','2026-W32','douyin', ?,
                          lower(hex(randomblob(32))), 0, 'available','2026-08-03T07:00:00Z')
                """,
                (content_id, content_id, f"data/cache/c{content_id}.json"),
            )


def _seed_legacy_comment(
    connection: sqlite3.Connection,
    *,
    evidence_id: int,
    cid: str,
    v1_key: str,
    parent: str | None = None,
) -> None:
    with storage.transaction(connection):
        connection.execute(
            """
            INSERT INTO comments(
                evidence_version_id, platform_comment_id, anonymous_user_key,
                body, published_at, like_count, parent_comment_id,
                interaction_user_id, comment_identity_key
            ) VALUES (?, ?, ?, '历史评论', '2026-07-28T01:00:00Z', 0, ?, NULL, ?)
            """,
            (evidence_id, cid, v1_key, parent, f"cid:{cid}"),
        )


class SchemaV11Test(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "v11.sqlite3"
        self.connection = storage.connect(self.db)
        self.addCleanup(self.connection.close)
        storage.initialize_database(self.connection)

    def test_fresh_schema_is_v11_and_accepts_both_key_versions(self) -> None:
        versions = {
            int(row[0])
            for row in self.connection.execute(
                "SELECT version FROM schema_migrations"
            )
        }
        self.assertIn(11, versions)
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO interaction_users(
                    platform, pseudonymous_user_key, key_version,
                    first_seen_at, last_seen_at
                ) VALUES ('douyin','Uv1key','content-user-hmac-v1',
                          '2026-07-28T00:00:00Z','2026-07-28T00:00:00Z')
                """
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                """
                INSERT INTO interaction_users(
                    platform, pseudonymous_user_key, key_version,
                    first_seen_at, last_seen_at
                ) VALUES ('douyin','X','some-other-version',
                          '2026-07-28T00:00:00Z','2026-07-28T00:00:00Z')
                """
            )


class LegacyIdentityBackfillTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "backfill.sqlite3"
        self.backups = Path(self.temp.name) / "backups"
        connection = storage.connect(self.db)
        storage.initialize_database(connection)
        _seed_contents(connection, count=2)
        # same person (same v1 key impossible across contents by construction,
        # so cross-content keys differ) — two users on content 1, one on 2.
        _seed_legacy_comment(connection, evidence_id=1, cid="c1", v1_key="U-a")
        _seed_legacy_comment(connection, evidence_id=1, cid="c2", v1_key="U-b")
        _seed_legacy_comment(connection, evidence_id=1, cid="c3", v1_key="U-a")
        _seed_legacy_comment(connection, evidence_id=2, cid="c4", v1_key="U-c")
        _seed_legacy_comment(
            connection, evidence_id=2, cid="c5", v1_key="U-c", parent="c4"
        )
        connection.close()
        self.module = _load_backfill_module()

    def test_dry_run_reports_without_writing(self) -> None:
        summary = self.module.backfill(self.db, apply=False, backup_dir=self.backups)
        self.assertTrue(summary["ok"])
        per_platform = summary["candidates"]["per_platform"]
        self.assertEqual(per_platform[0]["comments"], 5)
        self.assertEqual(per_platform[0]["distinct_v1_keys"], 3)
        connection = sqlite3.connect(self.db)
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM interaction_users"
                ).fetchone()[0],
                0,
            )
        finally:
            connection.close()

    def test_apply_links_users_and_universe_becomes_countable(self) -> None:
        summary = self.module.backfill(self.db, apply=True, backup_dir=self.backups)
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["comments_linked"], 5)
        self.assertEqual(summary["fallback_users_without_comments"], 0)
        self.assertTrue(list(self.backups.glob("*.sqlite3")))

        connection = storage.connect(self.db)
        try:
            users = connection.execute(
                "SELECT key_version, COUNT(*) n FROM interaction_users GROUP BY 1"
            ).fetchall()
            self.assertEqual(
                {(row["key_version"], row["n"]) for row in users},
                {("content-user-hmac-v1", 3)},
            )
            unlinked = connection.execute(
                "SELECT COUNT(*) FROM comments WHERE interaction_user_id IS NULL"
            ).fetchone()[0]
            self.assertEqual(unlinked, 0)

            result = ar.compute_slice_rate(
                connection,
                [1, 2],
                publication_count=2,
                classifier_state="uncalibrated",
                evidence_window_start="2026-05-09T00:00:00Z",
                evidence_window_end="2026-08-07T00:00:00Z",
                report_cutoff_at="2026-08-07T00:00:00Z",
                warm_up=True,
            )
            metric = result["metric"]
            quality = result["audience_quality"]
            # candidate users from v1 fallback keys: U-a, U-b, U-c = 3
            self.assertEqual(metric["denominator"], 3)
            # users exist but none classified yet -> held back, never a fake 0%
            self.assertEqual(metric["status"], "below_threshold")
            self.assertIsNone(metric["percentage"])
            self.assertIn("完成用户分类的比例", metric["reason"])
            self.assertEqual(quality["classified_user_count"], 0)
            self.assertEqual(quality["classification_coverage_percentage"], 0.0)
            self.assertEqual(quality["user_key_version"], "content-user-hmac-v1")
            self.assertEqual(quality["identity_coverage_percentage"], 100.0)
        finally:
            connection.close()

    def test_apply_is_idempotent(self) -> None:
        first = self.module.backfill(self.db, apply=True, backup_dir=self.backups)
        self.assertTrue(first["ok"])
        second = self.module.backfill(self.db, apply=True, backup_dir=self.backups)
        self.assertTrue(second["ok"])
        self.assertEqual(second["comments_linked"], 0)
        connection = sqlite3.connect(self.db)
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM interaction_users"
                ).fetchone()[0],
                3,
            )
        finally:
            connection.close()


class MissingReasonSplitTest(unittest.TestCase):
    """`missing` now states the actual cause instead of a generic phrase."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "missing.sqlite3"
        self.connection = storage.connect(self.db)
        self.addCleanup(self.connection.close)
        storage.initialize_database(self.connection)
        _seed_contents(self.connection, count=2)
        # content 3: published but NO evidence version at all
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO content_items(
                    id, link_id, platform, canonical_url, published_at,
                    imported_at, created_at, updated_at
                ) VALUES (3,'LNK003','douyin','https://www.douyin.com/video/3',
                          '2026-07-28T00:00:00Z','2026-07-28T00:00:00Z',
                          '2026-07-28T00:00:00Z','2026-07-28T00:00:00Z')
                """
            )

    def _rate(self, ids):
        return ar.compute_slice_rate(
            self.connection,
            ids,
            publication_count=len(ids),
            classifier_state="uncalibrated",
            evidence_window_start="2026-05-09T00:00:00Z",
            evidence_window_end="2026-08-07T00:00:00Z",
            report_cutoff_at="2026-08-07T00:00:00Z",
            warm_up=True,
        )

    def test_not_captured_yet(self) -> None:
        metric = self._rate([3])["metric"]
        self.assertEqual(metric["status"], "missing")
        self.assertIn("评论还没有采集", metric["reason"])

    def test_captured_but_zero_first_level_comments(self) -> None:
        metric = self._rate([1])["metric"]
        self.assertEqual(metric["status"], "missing")
        self.assertIn("所选时间内没有评论互动", metric["reason"])

    def test_comments_without_any_identity(self) -> None:
        _seed_legacy_comment(
            self.connection, evidence_id=1, cid="c9", v1_key="U-z"
        )
        metric = self._rate([1])["metric"]
        self.assertEqual(metric["status"], "missing")
        self.assertIn("无法识别评论用户", metric["reason"])
        # identity coverage over first-level comments is 0/1 -> gate holds too
        self.assertIsNone(metric["percentage"])


if __name__ == "__main__":
    unittest.main()
