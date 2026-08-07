from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from scripts.backfill_comment_capture_runs import main
from v8.capture import ProviderResult, execute_content_fetch
from v8.storage import connect, initialize_database


class BackfillCommentCaptureRunsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "backfill.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO content_items(
                    id,link_id,platform,platform_content_id,canonical_url,content_type,
                    imported_at,created_at,updated_at
                ) VALUES (1,'AAAAAA','douyin','aweme-1',
                          'https://www.douyin.com/video/aweme-1','video',
                          '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z',
                          '2026-08-01T00:00:00Z')
                """
            )
            connection.commit()
        page = {
            "comments": [],
            "declared_total": 0,
            "has_more": False,
            "next_cursor": None,
        }
        execute_content_fetch(
            content_id=1,
            stage="comments",
            window_key="2026-W31",
            provider="TikHub",
            adapter_version="tikhub-comments-v8.0",
            operation="douyin_video_comments",
            call=lambda: ProviderResult(
                page, {"stage": "comments", "data": page}, 200, True
            ),
            db_path=self.db,
            raw_root=self.root / "raw",
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _run(self, *args: str) -> dict:
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(["--db", str(self.db), *args]), 0)
        return json.loads(output.getvalue())

    def test_dry_run_is_read_only_and_apply_adopts_without_provider_state_change(
        self,
    ) -> None:
        dry_run = self._run()
        self.assertEqual(dry_run["candidates"], 1)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM comment_capture_runs"
                ).fetchone()[0],
                0,
            )
        with patch("v8.comment_paging.MANIFEST_ROOT", self.root / "manifests"):
            applied = self._run("--apply")
        self.assertTrue(applied["provider_state_unchanged"])
        self.assertEqual(applied["provider_cost"], 0.0)
        self.assertEqual(applied["statuses"], {"succeeded": 1})
        self.assertEqual(applied["completion_kinds"], {"zero_comments": 1})
        with connect(self.db) as connection:
            run = connection.execute(
                "SELECT status,completion_kind FROM comment_capture_runs"
            ).fetchone()
            evidence = connection.execute(
                "SELECT capture_run_id,status FROM comment_evidence_versions"
            ).fetchone()
        self.assertEqual(tuple(run), ("succeeded", "zero_comments"))
        self.assertIsNotNone(evidence["capture_run_id"])
        self.assertEqual(evidence["status"], "available")


if __name__ == "__main__":
    unittest.main()
