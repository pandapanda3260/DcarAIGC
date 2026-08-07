"""First-level comment parent-marker normalization (douyin reply_id "0").

Regression suite for the defect where TikHub douyin payloads mark top-level
comments with ``reply_id="0"`` and the sanitizer persisted that literal into
``comments.parent_comment_id``. Every interaction-user query filters on
``parent_comment_id IS NULL``, so the douyin user universe collapsed to 0 and
``automotive_user_rate`` reported ``missing`` (有效样本不足) on every slice.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import v8.audience_rate as audience_rate
import v8.identity as identity
import v8.providers as providers
import v8.storage as storage

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPAIR_SCRIPT = PROJECT_ROOT / "scripts" / "repair_comment_parent_zero.py"


def _load_repair_module():
    spec = importlib.util.spec_from_file_location(
        "repair_comment_parent_zero", REPAIR_SCRIPT
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _seed_content_and_evidence(
    connection: sqlite3.Connection, *, published_at: str = "2026-08-01T00:00:00Z"
) -> None:
    with storage.transaction(connection):
        connection.execute(
            """
            INSERT INTO content_items(
                id, link_id, platform, canonical_url, published_at,
                imported_at, created_at, updated_at
            ) VALUES
                (1,'AAAAAA','douyin','https://www.douyin.com/video/1',?,
                 '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
            """,
            (published_at,),
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


class NormalizedParentCommentIdTest(unittest.TestCase):
    def test_no_parent_spellings_become_null(self) -> None:
        for value in (None, "", "0", 0, " 0 ", "  "):
            self.assertIsNone(
                identity.normalized_parent_comment_id(value), repr(value)
            )

    def test_real_reply_ids_pass_through(self) -> None:
        self.assertEqual(
            identity.normalized_parent_comment_id("7669030889586393882"),
            "7669030889586393882",
        )
        self.assertEqual(identity.normalized_parent_comment_id(123), "123")
        self.assertEqual(
            identity.normalized_parent_comment_id("6a6fa967000000000802dfb0"),
            "6a6fa967000000000802dfb0",
        )


class DouyinCommentsSanitizerTest(unittest.TestCase):
    def _payload(self, comments) -> dict:
        return {
            "code": 200,
            "data": {
                "comments": comments,
                "cursor": 20,
                "has_more": False,
                "total": len(comments),
            },
        }

    def test_reply_id_zero_is_first_level(self) -> None:
        payload = self._payload(
            [
                {
                    "cid": "c-top",
                    "text": "内饰真不错",
                    "digg_count": 2,
                    "create_time": 1754006400,
                    "user": {"uid": "raw-uid-1"},
                    "reply_id": "0",
                },
                {
                    "cid": "c-reply",
                    "text": "同问落地价",
                    "digg_count": 0,
                    "create_time": 1754006500,
                    "user": {"uid": "raw-uid-2"},
                    "reply_id": "7669030889586393882",
                },
            ]
        )
        result = providers._parse_douyin_stage_payload("comments", "123", payload)
        by_cid = {c["platform_comment_id"]: c for c in result.data["comments"]}
        self.assertIsNone(by_cid["c-top"]["parent_comment_id"])
        self.assertEqual(
            by_cid["c-reply"]["parent_comment_id"], "7669030889586393882"
        )
        # the persisted evidence payload must carry the same normalization
        safe = {
            c["platform_comment_id"]: c
            for c in result.raw_response["data"]["comments"]
        }
        self.assertIsNone(safe["c-top"]["parent_comment_id"])

    def test_replayed_sanitized_comments_are_normalized(self) -> None:
        payload = self._payload(
            [
                {
                    "platform_comment_id": "c-legacy",
                    "anonymous_user_key": "Ulegacy",
                    "body": "历史证据回放",
                    "parent_comment_id": "0",
                }
            ]
        )
        result = providers._parse_douyin_stage_payload("comments", "123", payload)
        (comment,) = result.data["comments"]
        self.assertIsNone(comment["parent_comment_id"])


class InsertCommentRowsParentTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "parent.sqlite3"
        self.connection = storage.connect(self.db)
        storage.initialize_database(self.connection)
        _seed_content_and_evidence(self.connection)

    def tearDown(self) -> None:
        self.connection.close()
        self.temp.cleanup()

    def test_markers_are_normalized_at_the_database_boundary(self) -> None:
        comments = [
            {
                "platform_comment_id": "c-1",
                "anonymous_user_key": "Uv1-a",
                "pseudonymous_user_key": "Pv2-a",
                "body": "楼主说得对",
                "published_at": "2026-08-01T02:00:00Z",
                "parent_comment_id": "0",
            },
            {
                "platform_comment_id": "c-2",
                "anonymous_user_key": "Uv1-b",
                "pseudonymous_user_key": "Pv2-b",
                "body": "问下油耗",
                "published_at": "2026-08-01T02:05:00Z",
                "parent_comment_id": "",
            },
            {
                "platform_comment_id": "c-3",
                "anonymous_user_key": "Uv1-c",
                "pseudonymous_user_key": "Pv2-c",
                "body": "已经是一级",
                "published_at": "2026-08-01T02:10:00Z",
                "parent_comment_id": None,
            },
            {
                "platform_comment_id": "c-4",
                "anonymous_user_key": "Uv1-d",
                "pseudonymous_user_key": "Pv2-d",
                "body": "回复楼上",
                "published_at": "2026-08-01T02:15:00Z",
                "parent_comment_id": "c-1",
            },
        ]
        with storage.transaction(self.connection):
            inserted = identity.insert_comment_rows(
                self.connection,
                platform="douyin",
                evidence_version_id=1,
                comments=comments,
                captured_at="2026-08-01T03:00:00Z",
            )
        self.assertEqual(inserted, 4)
        rows = {
            row["platform_comment_id"]: row["parent_comment_id"]
            for row in self.connection.execute(
                "SELECT platform_comment_id, parent_comment_id FROM comments"
            )
        }
        self.assertIsNone(rows["c-1"])
        self.assertIsNone(rows["c-2"])
        self.assertIsNone(rows["c-3"])
        self.assertEqual(rows["c-4"], "c-1")

        universe = audience_rate._slice_user_universe(self.connection, [1])
        # candidate users = distinct identified L1 users (classification of the
        # eligible denominator happens later in the pipeline)
        self.assertEqual(universe["candidate_users"], 3)
        self.assertEqual(universe["identity_coverage_percentage"], 100.0)


class RepairScriptTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "repair.sqlite3"
        self.backup_dir = Path(self.temp.name) / "backups"
        connection = storage.connect(self.db)
        storage.initialize_database(connection)
        _seed_content_and_evidence(connection)
        with storage.transaction(connection):
            connection.executemany(
                """
                INSERT INTO comments(
                    evidence_version_id, platform_comment_id, anonymous_user_key,
                    body, published_at, like_count, parent_comment_id,
                    interaction_user_id, comment_identity_key
                ) VALUES (1, ?, ?, ?, '2026-08-01T02:00:00Z', 0, ?, NULL, ?)
                """,
                [
                    ("c-1", "Ua", "第一条", "0", "cid:c-1"),
                    ("c-2", "Ub", "第二条", "0", "cid:c-2"),
                    ("c-3", "Uc", "空串标记", "", "cid:c-3"),
                    ("c-4", "Ud", "真实回复", "c-1", "cid:c-4"),
                    ("c-5", "Ue", "本来就好", None, "cid:c-5"),
                ],
            )
        connection.close()
        self.repair = _load_repair_module()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _markers(self) -> int:
        connection = sqlite3.connect(self.db)
        try:
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM comments WHERE parent_comment_id IN ('','0')"
                ).fetchone()[0]
            )
        finally:
            connection.close()

    def test_dry_run_reports_without_writing(self) -> None:
        summary = self.repair.repair(
            self.db, apply=False, backup_dir=self.backup_dir
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["mode"], "dry_run")
        self.assertEqual(summary["rows_to_normalize"], 3)
        self.assertEqual(self._markers(), 3)
        self.assertFalse(self.backup_dir.exists())

    def test_apply_normalizes_backs_up_and_verifies(self) -> None:
        summary = self.repair.repair(
            self.db, apply=True, backup_dir=self.backup_dir
        )
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["rows_normalized"], 3)
        self.assertEqual(summary["quick_check"], "ok")
        self.assertEqual(self._markers(), 0)
        backups = list(self.backup_dir.glob("*.sqlite3"))
        self.assertEqual(len(backups), 1)
        json.dumps(summary)  # summary must stay JSON-serializable

        connection = sqlite3.connect(self.db)
        try:
            kept = connection.execute(
                "SELECT parent_comment_id FROM comments WHERE platform_comment_id='c-4'"
            ).fetchone()[0]
            null_count = connection.execute(
                "SELECT COUNT(*) FROM comments WHERE parent_comment_id IS NULL"
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(kept, "c-1")
        self.assertEqual(null_count, 4)

        again = self.repair.repair(self.db, apply=False, backup_dir=self.backup_dir)
        self.assertEqual(again["rows_to_normalize"], 0)


if __name__ == "__main__":
    unittest.main()
