from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import v8.storage as storage

try:  # providers pulls in media/duplicates (Pillow, imagehash)
    import v8.providers as providers
    from v8.identity import PlatformUserHasher, comment_identity_key

    _PROVIDERS_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - environment without media deps
    providers = None  # type: ignore[assignment]
    _PROVIDERS_AVAILABLE = False


@unittest.skipUnless(
    _PROVIDERS_AVAILABLE, "providers module requires Pillow/imagehash"
)
class CommentPagingLiveBridgeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "live.sqlite3"
        self.raw_root = self.root / "raw"
        self._patch_roots()
        self.connection = storage.connect(self.db)
        storage.initialize_database(self.connection)
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO content_items(
                    id, link_id, platform, platform_content_id, canonical_url,
                    content_type, imported_at, created_at, updated_at
                ) VALUES (1,'AAAAAA','douyin','aweme-1',
                          'https://www.douyin.com/video/aweme-1','video',
                          '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
                """
            )
        self.connection.close()
        self.hasher = PlatformUserHasher(salt_path=self.root / ".platform_salt")
        self.call_log: List[Optional[Mapping[str, Any]]] = []

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _patch_roots(self) -> None:
        import v8.capture as capture
        import v8.comment_paging as paging

        self._orig_capture_raw = capture.RAW_ROOT
        self._orig_manifest = paging.MANIFEST_ROOT
        capture.RAW_ROOT = self.raw_root
        paging.MANIFEST_ROOT = self.root / "manifests"
        self.addCleanup(setattr, capture, "RAW_ROOT", self._orig_capture_raw)
        self.addCleanup(setattr, paging, "MANIFEST_ROOT", self._orig_manifest)

    def _pages(self) -> List[Dict[str, Any]]:
        def comment(cid: str, uid: str, body: str) -> Dict[str, Any]:
            return {
                "platform_comment_id": cid,
                "anonymous_user_key": f"U-{uid}",
                "pseudonymous_user_key": self.hasher.user_key("douyin", uid),
                "body": body,
                "published_at": "2026-07-31T10:00:00Z",
                "like_count": 1,
                "parent_comment_id": None,
                "comment_identity_key": comment_identity_key(
                    platform_comment_id=cid,
                    pseudonymous_user_key=self.hasher.user_key("douyin", uid),
                    body=body,
                    published_at="2026-07-31T10:00:00Z",
                ),
            }

        return [
            {
                "comment_count": 3,
                "declared_total": 3,
                "has_more": True,
                "next_cursor": 20,
                "next_cursor_params": {"cursor": 20},
                "comments": [
                    comment("c1", "u1", "这车提速真不错"),
                    comment("c2", "u2", "油耗多少"),
                ],
            },
            {
                "comment_count": 3,
                "declared_total": 3,
                "has_more": False,
                "next_cursor": None,
                "next_cursor_params": None,
                "comments": [comment("c3", "u3", "内饰做工怎么样")],
            },
        ]

    def _override(self, pages: List[Dict[str, Any]]):
        def call_override(stage: str, content: Mapping[str, Any]):
            assert stage == "comments"
            cursor = content.get("_comment_cursor")
            self.call_log.append(cursor)
            index = 0 if cursor is None else int(cursor.get("cursor", 0)) // 20
            page = pages[index]
            return providers.ProviderResult(dict(page), {"data": page}, 200, True)

        return call_override

    def test_live_pagination_folds_pages_and_never_double_bills(self) -> None:
        pages = self._pages()
        result = providers.capture_content_comments_live(
            1,
            as_of=__import__("datetime").date(2026, 7, 31),
            db_path=self.db,
            call_override=self._override(pages),
        )
        self.assertEqual(result["completion_kind"], "provider_exhausted")
        self.assertEqual(result["distinct_l1_comments"], 3)
        self.assertEqual(result["pages_fetched"], 2)

        connection = storage.connect(self.db)
        try:
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT COUNT(*) FROM comment_capture_pages"
                    ).fetchone()[0]
                ),
                2,
            )
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT COUNT(*) FROM comments"
                    ).fetchone()[0]
                ),
                3,
            )
            # Two paid pages, two distinct page-scoped comment slots.
            slots = int(
                connection.execute(
                    "SELECT COUNT(*) FROM fetch_slots WHERE stage='comments'"
                ).fetchone()[0]
            )
            self.assertEqual(slots, 2)

            # Re-run: the run is already succeeded, so no new pages/slots/billing.
            before_attempts = int(
                connection.execute(
                    "SELECT COUNT(*) FROM fetch_attempts"
                ).fetchone()[0]
            )
        finally:
            connection.close()

        rerun = providers.capture_content_comments_live(
            1,
            as_of=__import__("datetime").date(2026, 7, 31),
            db_path=self.db,
            call_override=self._override(pages),
        )
        self.assertEqual(rerun["status"], "already_succeeded")

        connection = storage.connect(self.db)
        try:
            after_attempts = int(
                connection.execute(
                    "SELECT COUNT(*) FROM fetch_attempts"
                ).fetchone()[0]
            )
            self.assertEqual(after_attempts, before_attempts)
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT COUNT(*) FROM comment_capture_pages"
                    ).fetchone()[0]
                ),
                2,
            )
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
