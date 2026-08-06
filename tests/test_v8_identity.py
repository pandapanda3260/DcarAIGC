from __future__ import annotations

import importlib
import random
import sqlite3
import tempfile
import unittest
from pathlib import Path

import v8.evaluation as evaluation
import v8.identity as identity
import v8.storage as storage


def _seed_content_and_evidence(connection: sqlite3.Connection) -> None:
    with storage.transaction(connection):
        connection.execute(
            """
            INSERT INTO content_items(
                id, link_id, platform, canonical_url,
                imported_at, created_at, updated_at
            ) VALUES
                (1,'AAAAAA','douyin','https://www.douyin.com/video/1',
                 '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
            """
        )
        connection.execute(
            """
            INSERT INTO comment_evidence_versions(
                id, content_id, captured_at, iso_week, source, local_path,
                sha256, comment_count, status, created_at
            ) VALUES
                (1,1,'2026-08-01T01:00:00Z','2026-W31','douyin','data/cache/c1.json',
                 lower(hex(randomblob(32))),4,'available','2026-08-01T01:00:00Z')
            """
        )


class PlatformUserHasherTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.salt_path = Path(self.temp.name) / ".platform_user_salt"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_key_is_stable_across_contents_and_instances(self) -> None:
        first = identity.PlatformUserHasher(salt_path=self.salt_path)
        second = identity.PlatformUserHasher(salt_path=self.salt_path)
        key = first.user_key("douyin", "raw-uid-1")
        self.assertTrue(key.startswith("P"))
        self.assertEqual(key, second.user_key("douyin", "raw-uid-1"))

    def test_platforms_do_not_share_keys(self) -> None:
        hasher = identity.PlatformUserHasher(salt_path=self.salt_path)
        self.assertNotEqual(
            hasher.user_key("douyin", "raw-uid-1"),
            hasher.user_key("xiaohongshu", "raw-uid-1"),
        )

    def test_independent_salts_produce_independent_keys(self) -> None:
        other_path = Path(self.temp.name) / ".other_salt"
        first = identity.PlatformUserHasher(salt_path=self.salt_path)
        second = identity.PlatformUserHasher(salt_path=other_path)
        self.assertNotEqual(
            first.user_key("douyin", "raw-uid-1"),
            second.user_key("douyin", "raw-uid-1"),
        )

    def test_missing_inputs_produce_empty_key(self) -> None:
        hasher = identity.PlatformUserHasher(salt_path=self.salt_path)
        self.assertEqual(hasher.user_key("douyin", ""), "")
        self.assertEqual(hasher.user_key("", "raw-uid-1"), "")


class CommentIdentityKeyTest(unittest.TestCase):
    def test_platform_comment_id_wins(self) -> None:
        self.assertEqual(
            identity.comment_identity_key(
                platform_comment_id="c-1",
                pseudonymous_user_key="Pabc",
                body="你好",
                published_at="2026-08-01T00:00:00Z",
            ),
            "cid:c-1",
        )

    def test_sha_fallback_is_deterministic(self) -> None:
        first = identity.comment_identity_key(
            platform_comment_id="",
            pseudonymous_user_key="Pabc",
            body="这车提速真不错",
            published_at="2026-08-01T00:00:00Z",
        )
        second = identity.comment_identity_key(
            platform_comment_id=None,
            pseudonymous_user_key="Pabc",
            body="这车提速真不错",
            published_at="2026-08-01T00:00:00Z",
        )
        self.assertEqual(first, second)
        self.assertTrue(str(first).startswith("sha:"))

    def test_empty_comment_has_no_identity(self) -> None:
        self.assertIsNone(
            identity.comment_identity_key(
                platform_comment_id="",
                pseudonymous_user_key="Pabc",
                body="",
                published_at=None,
            )
        )


class InteractionUserDomainTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "identity.sqlite3"
        self.connection = storage.connect(self.db)
        storage.initialize_database(self.connection)
        _seed_content_and_evidence(self.connection)

    def tearDown(self) -> None:
        self.connection.close()
        self.temp.cleanup()

    def test_ensure_interaction_user_touches_seen_range(self) -> None:
        with storage.transaction(self.connection):
            first = identity.ensure_interaction_user(
                self.connection,
                platform="douyin",
                pseudonymous_user_key="Puser",
                seen_at="2026-08-02T00:00:00Z",
            )
            second = identity.ensure_interaction_user(
                self.connection,
                platform="douyin",
                pseudonymous_user_key="Puser",
                seen_at="2026-08-01T00:00:00Z",
            )
            third = identity.ensure_interaction_user(
                self.connection,
                platform="douyin",
                pseudonymous_user_key="Puser",
                seen_at="2026-08-03T00:00:00Z",
            )
        self.assertEqual(first, second)
        self.assertEqual(first, third)
        row = self.connection.execute(
            "SELECT first_seen_at, last_seen_at FROM interaction_users WHERE id=?",
            (first,),
        ).fetchone()
        self.assertEqual(
            (row["first_seen_at"], row["last_seen_at"]),
            ("2026-08-01T00:00:00Z", "2026-08-03T00:00:00Z"),
        )
        self.assertIsNone(
            identity.ensure_interaction_user(
                self.connection,
                platform="douyin",
                pseudonymous_user_key="",
                seen_at="2026-08-01T00:00:00Z",
            )
        )

    def test_insert_comment_rows_links_users_and_replays_deduplicate(self) -> None:
        comments = [
            {
                "platform_comment_id": "c-1",
                "anonymous_user_key": "Uv1-a",
                "pseudonymous_user_key": "Pv2-a",
                "body": "这车提速真不错",
                "published_at": "2026-07-31T10:00:00Z",
                "like_count": 3,
                "parent_comment_id": None,
                "comment_identity_key": "cid:c-1",
            },
            {
                "platform_comment_id": "",
                "anonymous_user_key": "Uv1-a",
                "pseudonymous_user_key": "Pv2-a",
                "body": "油耗多少",
                "published_at": "2026-07-31T11:00:00Z",
                "like_count": 0,
                "parent_comment_id": None,
            },
            {
                # historical replay item without a platform-level pseudonym
                "platform_comment_id": "c-9",
                "anonymous_user_key": "Uv1-b",
                "body": "好看",
                "published_at": "2026-07-31T12:00:00Z",
            },
        ]
        with storage.transaction(self.connection):
            inserted = identity.insert_comment_rows(
                self.connection,
                platform="douyin",
                evidence_version_id=1,
                comments=comments,
                captured_at="2026-08-01T01:00:00Z",
            )
        self.assertEqual(inserted, 3)
        self.assertEqual(
            int(
                self.connection.execute(
                    "SELECT COUNT(*) FROM interaction_users"
                ).fetchone()[0]
            ),
            1,
        )
        linked = self.connection.execute(
            """
            SELECT COUNT(*) FROM comments
            WHERE interaction_user_id IS NOT NULL
            """
        ).fetchone()[0]
        self.assertEqual(int(linked), 2)
        unlinked = self.connection.execute(
            """
            SELECT comment_identity_key FROM comments
            WHERE interaction_user_id IS NULL
            """
        ).fetchone()
        self.assertEqual(unlinked["comment_identity_key"], "cid:c-9")

        with storage.transaction(self.connection):
            replayed = identity.insert_comment_rows(
                self.connection,
                platform="douyin",
                evidence_version_id=1,
                comments=comments,
                captured_at="2026-08-01T02:00:00Z",
            )
        self.assertEqual(replayed, 0)
        self.assertEqual(
            int(self.connection.execute("SELECT COUNT(*) FROM comments").fetchone()[0]),
            3,
        )

    def test_raw_platform_uid_never_reaches_the_database(self) -> None:
        salt_path = Path(self.temp.name) / ".salt"
        hasher = identity.PlatformUserHasher(salt_path=salt_path)
        raw_uid = "MS4wLjABAAAA-raw-secret-uid"
        comment = {
            "platform_comment_id": "c-1",
            "anonymous_user_key": "Uv1-a",
            "pseudonymous_user_key": hasher.user_key("douyin", raw_uid),
            "body": "这车提速真不错",
            "published_at": "2026-07-31T10:00:00Z",
        }
        with storage.transaction(self.connection):
            identity.insert_comment_rows(
                self.connection,
                platform="douyin",
                evidence_version_id=1,
                comments=[comment],
                captured_at="2026-08-01T01:00:00Z",
            )
        dump = "\n".join(self.connection.iterdump())
        self.assertNotIn(raw_uid, dump)
        self.assertIn(comment["pseudonymous_user_key"], dump)

    def test_legacy_gate_respects_min_valid_commenters(self) -> None:
        scorer = importlib.import_module("three_proposition_scoring")
        minimum = int(scorer.MIN_VALID_COMMENTERS)
        rows = [
            {
                "anonymous_user_key": f"Uv1-{index}",
                "audience_automotive_score": 70,
                "action_intent_score": 50,
            }
            for index in range(minimum)
        ]
        evaluation.upsert_comment_user_scores(1, 1, rows[: minimum - 1], db_path=self.db)
        audience, action, count = evaluation._comment_scores(self.connection, 1)
        self.assertIsNone(audience)
        self.assertIsNone(action)
        self.assertEqual(count, minimum - 1)

        evaluation.upsert_comment_user_scores(1, 1, rows, db_path=self.db)
        audience, action, count = evaluation._comment_scores(self.connection, 1)
        self.assertEqual((audience, action, count), (70, 50, minimum))
        versions = self.connection.execute(
            "SELECT DISTINCT key_version, score_rule_version FROM comment_user_scores"
        ).fetchall()
        self.assertEqual(
            [tuple(row) for row in versions],
            [(storage.LEGACY_COMMENT_USER_KEY_VERSION,
              storage.LEGACY_COMMENT_SCORE_RULE_VERSION)],
        )


class LegacyScoreCompatibilityTest(unittest.TestCase):
    def test_top_three_distinct_texts_scored_once_per_user(self) -> None:
        scoring = importlib.import_module("analyze_douyin_tikhub_v6")
        comments = [
            {
                "anonymous_user_key": "Uv1-a",
                "body": "这车提速真不错",
                "published_at": "2026-07-31T10:00:00Z",
                "platform_comment_id": "c-1",
            },
            {
                "anonymous_user_key": "Uv1-a",
                "body": "油耗多少",
                "published_at": "2026-07-31T11:00:00Z",
                "platform_comment_id": "c-2",
            },
            {
                # duplicate text is collapsed
                "anonymous_user_key": "Uv1-a",
                "body": "油耗多少",
                "published_at": "2026-07-31T11:30:00Z",
                "platform_comment_id": "c-3",
            },
            {
                "anonymous_user_key": "Uv1-a",
                "body": "内饰做工怎么样",
                "published_at": "2026-07-31T12:00:00Z",
                "platform_comment_id": "c-4",
            },
            {
                # fourth distinct text never enters the joined evidence
                "anonymous_user_key": "Uv1-a",
                "body": "多少钱落地",
                "published_at": "2026-07-31T13:00:00Z",
                "platform_comment_id": "c-5",
            },
            {
                "anonymous_user_key": "Uv1-b",
                "body": "好看",
                "published_at": "2026-07-31T14:00:00Z",
                "platform_comment_id": "c-6",
            },
            {"anonymous_user_key": "", "body": "无身份"},
            {"anonymous_user_key": "Uv1-c", "body": ""},
        ]
        rows = identity.legacy_user_score_rows(comments)
        self.assertEqual(
            [row["anonymous_user_key"] for row in rows], ["Uv1-a", "Uv1-b"]
        )
        joined = "；".join(["这车提速真不错", "油耗多少", "内饰做工怎么样"])
        expected_audience = scoring.audience_user_score(
            joined, context_automotive=True
        )
        expected_action = scoring.action_user_score(joined, context_automotive=True)
        self.assertEqual(rows[0]["audience_automotive_score"], expected_audience)
        self.assertEqual(rows[0]["action_intent_score"], expected_action)

        shuffled = list(comments)
        random.Random(42).shuffle(shuffled)
        self.assertEqual(rows, identity.legacy_user_score_rows(shuffled))


if __name__ == "__main__":
    unittest.main()
