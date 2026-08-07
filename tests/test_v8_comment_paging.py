from __future__ import annotations

import hashlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import v8.comment_paging as paging
import v8.storage as storage
from v8.identity import PlatformUserHasher, comment_identity_key


def hashlib_sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class _Result:
    def __init__(self, data: Mapping[str, Any]) -> None:
        self.data = dict(data)


def _make_comment(
    hasher: PlatformUserHasher,
    *,
    cid: str,
    raw_uid: str,
    body: str,
    parent: Optional[str] = None,
) -> Dict[str, Any]:
    pseudonymous = hasher.user_key("douyin", raw_uid)
    digits = "".join(ch for ch in cid if ch.isdigit()) or "0"
    published = f"2026-07-31T10:{int(digits) % 60:02d}:00Z"
    comment = {
        "platform_comment_id": cid,
        "anonymous_user_key": f"U-{raw_uid}",
        "pseudonymous_user_key": pseudonymous,
        "body": body,
        "published_at": published,
        "like_count": 0,
        "parent_comment_id": parent,
    }
    comment["comment_identity_key"] = comment_identity_key(
        platform_comment_id=cid,
        pseudonymous_user_key=pseudonymous,
        body=body,
        published_at=published,
    )
    return comment


class _FakePages:
    """In-memory provider whose pages carry real raw/slot rows on demand."""

    def __init__(
        self, connection: sqlite3.Connection, content_id: int, pages: List[Mapping[str, Any]]
    ) -> None:
        self.connection = connection
        self.content_id = content_id
        self.pages = pages
        self.calls: List[int] = []
        self._raw_seq = 0
        self._slot_seq = 0

    def fetch(self, page_number: int, cursor: Optional[Mapping[str, Any]]) -> paging.PageFetch:
        self.calls.append(page_number)
        page = self.pages[page_number - 1]
        # Materialize (or reuse) a real fetch slot + raw response so page FKs
        # resolve and a replayed page never double-inserts (idempotent slot).
        window = paging.page_window_key("2026-W31", cursor)
        with storage.transaction(self.connection):
            existing = self.connection.execute(
                """
                SELECT fs.id AS slot_id, pr.id AS raw_id
                FROM fetch_slots fs
                JOIN provider_raw_responses pr
                  ON pr.content_id=fs.content_id
                 AND pr.local_path=?
                WHERE fs.content_id=? AND fs.stage='comments' AND fs.window_key=?
                """,
                (
                    f"data/cache/raw/{self.content_id}_{window}.json",
                    self.content_id,
                    window,
                ),
            ).fetchone()
            if existing is not None:
                return paging.PageFetch(
                    raw_response_id=int(existing["raw_id"]),
                    fetch_slot_id=int(existing["slot_id"]),
                    result=_Result(page),
                    already_stored=True,
                )
            slot_cursor = self.connection.execute(
                """
                INSERT INTO fetch_slots(
                    content_id, stage, window_key, provider, adapter_version,
                    status, created_at, updated_at
                ) VALUES (?, 'comments', ?, 'TikHub', 'tikhub-comments-v8.0',
                          'succeeded', '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z')
                """,
                (self.content_id, window),
            )
            slot_id = int(slot_cursor.lastrowid)
            raw_cursor = self.connection.execute(
                """
                INSERT INTO provider_raw_responses(
                    content_id, provider, operation, local_path, sha256,
                    byte_size, captured_at
                ) VALUES (?, 'TikHub', 'douyin_video_comments', ?, ?, 10,
                          '2026-08-01T00:00:00Z')
                """,
                (
                    self.content_id,
                    f"data/cache/raw/{self.content_id}_{window}.json",
                    hashlib_sha(window),
                ),
            )
            raw_id = int(raw_cursor.lastrowid)
        return paging.PageFetch(
            raw_response_id=raw_id,
            fetch_slot_id=slot_id,
            result=_Result(page),
        )


class CommentPagingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "paging.sqlite3"
        self.connection = storage.connect(self.db)
        storage.initialize_database(self.connection)
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO content_items(
                    id, link_id, platform, canonical_url,
                    imported_at, created_at, updated_at
                ) VALUES (1,'AAAAAA','douyin','https://www.douyin.com/video/1',
                          '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
                """
            )
        self.content = {"id": 1, "platform": "douyin"}
        self.hasher = PlatformUserHasher(
            salt_path=Path(self.temp.name) / ".platform_salt"
        )

    def tearDown(self) -> None:
        self.connection.close()
        self.temp.cleanup()

    def _run(self, fake: _FakePages, **kwargs: Any) -> Dict[str, Any]:
        return paging.capture_content_comments(
            self.content,
            window_key="2026-W31",
            page_fetcher=fake.fetch,
            provider="TikHub",
            adapter_version="tikhub-comments-v8.0",
            db_path=self.db,
            **kwargs,
        )

    def test_provider_exhausted_folds_pages_into_one_evidence(self) -> None:
        pages = [
            {
                "comments": [
                    _make_comment(self.hasher, cid="c01", raw_uid="u1", body="这车提速真不错"),
                    _make_comment(self.hasher, cid="c02", raw_uid="u2", body="油耗多少"),
                ],
                "declared_total": 3,
                "has_more": True,
                "next_cursor_params": {"cursor": 20},
            },
            {
                "comments": [
                    _make_comment(self.hasher, cid="c03", raw_uid="u3", body="内饰做工怎么样"),
                    # a reply rides along but must not be counted as L1
                    _make_comment(
                        self.hasher, cid="c04", raw_uid="u4", body="同问", parent="c01"
                    ),
                ],
                "declared_total": 3,
                "has_more": False,
                "next_cursor": None,
            },
        ]
        fake = _FakePages(self.connection, 1, pages)
        result = self._run(fake)
        self.assertEqual(result["completion_kind"], "provider_exhausted")
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["pages_fetched"], 2)
        self.assertEqual(result["distinct_l1_comments"], 3)
        self.assertEqual(fake.calls, [1, 2])

        evidence = self.connection.execute(
            "SELECT COUNT(*) FROM comment_evidence_versions WHERE content_id=1"
        ).fetchone()[0]
        self.assertEqual(int(evidence), 1)
        page_rows = self.connection.execute(
            "SELECT COUNT(*) FROM comment_capture_pages"
        ).fetchone()[0]
        self.assertEqual(int(page_rows), 2)
        stored_comments = self.connection.execute(
            "SELECT COUNT(*) FROM comments"
        ).fetchone()[0]
        self.assertEqual(int(stored_comments), 4)
        interaction_users = self.connection.execute(
            "SELECT COUNT(*) FROM interaction_users"
        ).fetchone()[0]
        self.assertEqual(int(interaction_users), 4)
        run = self.connection.execute(
            "SELECT status, completion_kind, valid_comment_count FROM comment_capture_runs"
        ).fetchone()
        self.assertEqual(
            (run["status"], run["completion_kind"], run["valid_comment_count"]),
            ("succeeded", "provider_exhausted", 3),
        )
        scored = self.connection.execute(
            "SELECT COUNT(*) FROM comment_user_scores WHERE content_id=1"
        ).fetchone()[0]
        # one score row per distinct v1 user (c04's u4 included)
        self.assertEqual(int(scored), 4)

    def test_coverage_target_stops_before_provider_exhaustion(self) -> None:
        first = [
            _make_comment(self.hasher, cid=f"c{i:02d}", raw_uid=f"u{i}", body=f"配置问题{i}")
            for i in range(9)
        ]
        pages = [
            {
                "comments": first,
                "declared_total": 10,
                "has_more": True,
                "next_cursor_params": {"cursor": 20},
            },
            {
                "comments": [
                    _make_comment(self.hasher, cid="c99", raw_uid="u99", body="还有一条")
                ],
                "declared_total": 10,
                "has_more": True,
                "next_cursor_params": {"cursor": 40},
            },
        ]
        fake = _FakePages(self.connection, 1, pages)
        result = self._run(fake)
        self.assertEqual(result["completion_kind"], "coverage_target_reached")
        self.assertEqual(result["pages_fetched"], 1)
        self.assertEqual(fake.calls, [1])
        self.assertEqual(result["distinct_l1_comments"], 9)
        self.assertAlmostEqual(
            result["comment_collection_coverage_percentage"], 90.0
        )

    def test_cap_reached_at_1000_first_level_comments(self) -> None:
        big = [
            _make_comment(self.hasher, cid=f"c{i:05d}", raw_uid=f"u{i}", body=f"评论{i}")
            for i in range(1000)
        ]
        pages = [
            {
                "comments": big,
                "declared_total": 5000,
                "has_more": True,
                "next_cursor_params": {"cursor": 1000},
            },
            {
                "comments": [
                    _make_comment(self.hasher, cid="cX", raw_uid="uX", body="不应到达")
                ],
                "declared_total": 5000,
                "has_more": True,
                "next_cursor_params": {"cursor": 2000},
            },
        ]
        fake = _FakePages(self.connection, 1, pages)
        result = self._run(fake, comment_cap=1000)
        self.assertEqual(result["completion_kind"], "cap_reached")
        self.assertEqual(result["pages_fetched"], 1)
        self.assertEqual(result["distinct_l1_comments"], 1000)
        run = self.connection.execute(
            "SELECT status FROM comment_capture_runs"
        ).fetchone()
        self.assertEqual(run["status"], "succeeded")

    def test_exact_cap_with_provider_exhausted_is_not_marked_capped(self) -> None:
        comments = [
            _make_comment(
                self.hasher, cid=f"c{i:05d}", raw_uid=f"u{i}", body=f"评论{i}"
            )
            for i in range(1000)
        ]
        fake = _FakePages(
            self.connection,
            1,
            [
                {
                    "comments": comments,
                    "declared_total": 1000,
                    "has_more": False,
                    "next_cursor": None,
                }
            ],
        )
        result = self._run(fake, comment_cap=1000)
        self.assertEqual(result["completion_kind"], "provider_exhausted")

    def test_declared_total_never_shrinks_on_later_pages(self) -> None:
        pages = [
            {
                "comments": [
                    _make_comment(self.hasher, cid="c01", raw_uid="u1", body="第一页")
                ],
                "declared_total": 100,
                "has_more": True,
                "next_cursor_params": {"cursor": 20},
            },
            {
                "comments": [
                    _make_comment(self.hasher, cid="c02", raw_uid="u2", body="第二页")
                ],
                "declared_total": 10,
                "has_more": False,
                "next_cursor": None,
            },
        ]
        result = self._run(
            _FakePages(self.connection, 1, pages), coverage_target=2.0
        )
        self.assertEqual(result["declared_total"], 100)

    def test_later_declared_total_spike_does_not_rewrite_first_page_snapshot(
        self,
    ) -> None:
        pages = [
            {
                "comments": [
                    _make_comment(self.hasher, cid="c01", raw_uid="u1", body="第一页")
                ],
                "declared_total": 10,
                "has_more": True,
                "next_cursor_params": {"cursor": 20},
            },
            {
                "comments": [
                    _make_comment(self.hasher, cid="c02", raw_uid="u2", body="第二页")
                ],
                "declared_total": 999,
                "has_more": False,
                "next_cursor": None,
            },
        ]
        result = self._run(
            _FakePages(self.connection, 1, pages), coverage_target=2.0
        )
        self.assertEqual(result["declared_total"], 10)
        self.assertEqual(result["completion_kind"], "provider_exhausted")
        self.assertEqual(result["comment_collection_coverage_percentage"], 100.0)
        stored = self.connection.execute(
            """
            SELECT provider_declared_total FROM comment_capture_pages
            ORDER BY page_number
            """
        ).fetchall()
        self.assertEqual([row[0] for row in stored], [10, 999])

    def test_zero_comments_completion(self) -> None:
        pages = [
            {
                "comments": [],
                "declared_total": 0,
                "has_more": False,
                "next_cursor": None,
            }
        ]
        fake = _FakePages(self.connection, 1, pages)
        result = self._run(fake)
        self.assertEqual(result["completion_kind"], "zero_comments")
        self.assertEqual(result["distinct_l1_comments"], 0)
        evidence = self.connection.execute(
            "SELECT status FROM comment_evidence_versions WHERE content_id=1"
        ).fetchone()
        self.assertEqual(evidence["status"], "available")

    def test_cursor_cycle_is_detected(self) -> None:
        looping = {
            "comments": [
                _make_comment(self.hasher, cid="c01", raw_uid="u1", body="重复游标")
            ],
            "declared_total": 999,
            "has_more": True,
            "next_cursor_params": {"cursor": 0},  # same as the initial None-derived? no; distinct sha
        }
        # page 1 cursor=None -> next {cursor:0}; page 2 cursor={cursor:0} -> next {cursor:0}
        pages = [looping, looping]
        fake = _FakePages(self.connection, 1, pages)
        result = self._run(fake, max_pages=10)
        self.assertIsNone(result["completion_kind"])
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["stop_reason"], "cursor_cycle_detected")
        self.assertLessEqual(result["pages_fetched"], 2)
        run = self.connection.execute(
            "SELECT status,completion_kind FROM comment_capture_runs"
        ).fetchone()
        self.assertEqual(run["status"], "retryable_failed")
        self.assertIsNone(run["completion_kind"])

    def test_has_more_without_next_cursor_is_retryable_failure(self) -> None:
        page = {
            "comments": [
                _make_comment(self.hasher, cid="c01", raw_uid="u1", body="缺失游标")
            ],
            "declared_total": 20,
            "has_more": True,
            "next_cursor": None,
        }
        result = self._run(_FakePages(self.connection, 1, [page]))
        self.assertEqual(result["status"], "incomplete")
        self.assertIsNone(result["completion_kind"])
        self.assertEqual(result["stop_reason"], "missing_next_cursor")

    def test_resume_replays_stored_pages_without_new_slots(self) -> None:
        pages = [
            {
                "comments": [
                    _make_comment(self.hasher, cid="c01", raw_uid="u1", body="第一页评论")
                ],
                "declared_total": 4,
                "has_more": True,
                "next_cursor_params": {"cursor": 20},
            },
            {
                "comments": [
                    _make_comment(self.hasher, cid="c02", raw_uid="u2", body="第二页评论")
                ],
                "declared_total": 4,
                "has_more": False,
                "next_cursor": None,
            },
        ]

        # First run: only the first page exists (simulate an interruption by
        # capping max_pages=1 so the run stays incomplete).
        partial = _FakePages(self.connection, 1, pages)
        first = self._run(partial, max_pages=1)
        self.assertEqual(first["status"], "incomplete")
        self.assertEqual(
            int(
                self.connection.execute(
                    "SELECT COUNT(*) FROM comment_capture_pages"
                ).fetchone()[0]
            ),
            1,
        )

        # Resume: fetcher replays page 1 then fetches page 2 to completion.
        resume = _FakePages(self.connection, 1, pages)
        second = self._run(resume)
        self.assertEqual(second["completion_kind"], "provider_exhausted")
        self.assertEqual(second["distinct_l1_comments"], 2)
        self.assertEqual(
            int(
                self.connection.execute(
                    "SELECT COUNT(*) FROM comment_capture_pages"
                ).fetchone()[0]
            ),
            2,
        )
        # page 1 replayed (call 1) then page 2 fetched (call 2)
        self.assertEqual(resume.calls, [1, 2])


if __name__ == "__main__":
    unittest.main()
