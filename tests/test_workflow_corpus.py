from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from workflow.bootstrap_corpus import bootstrap
from workflow.cache_index import preflight
from workflow.storage import connect, migrate


ROOT = Path(__file__).resolve().parents[1]


class WorkflowCorpusTest(unittest.TestCase):
    def test_migration_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temporary:
            db = Path(temporary) / "test.sqlite3"
            with connect(db) as connection:
                self.assertEqual(migrate(connection), 2)
                self.assertEqual(migrate(connection), 2)
                tables = {
                    row[0] for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
            self.assertTrue({"runs", "content_items", "evidence_assets", "corpus_snapshots"} <= tables)

    def test_real_frozen_corpus_imports_expected_counts_without_tokens(self):
        with tempfile.TemporaryDirectory() as temporary:
            db = Path(temporary) / "test.sqlite3"
            result = bootstrap(db, ROOT)
            self.assertEqual(result["douyin_count"], 438)
            self.assertEqual(result["xiaohongshu_count"], 338)
            self.assertEqual(result["xiaohongshu_audit_count"], 375)
            with connect(db) as connection:
                xhs = connection.execute(
                    "SELECT COUNT(*) FROM content_items WHERE platform='xiaohongshu'"
                ).fetchone()[0]
                token_urls = connection.execute(
                    "SELECT COUNT(*) FROM content_items WHERE canonical_url LIKE '%xsec_token%'"
                ).fetchone()[0]
                duplicates = connection.execute(
                    "SELECT COUNT(*) FROM content_import_audit WHERE status LIKE '%duplicate'"
                ).fetchone()[0]
                invalid = connection.execute(
                    "SELECT COUNT(*) FROM content_import_audit WHERE status='invalid_creator_page'"
                ).fetchone()[0]
            self.assertEqual(xhs, 338)
            self.assertEqual(token_urls, 0)
            self.assertEqual(duplicates, 33)
            self.assertEqual(invalid, 4)

    def test_cache_index_reports_real_missing_counts_without_provider_calls(self):
        with tempfile.TemporaryDirectory() as temporary:
            db = Path(temporary) / "test.sqlite3"
            result = bootstrap(db, ROOT)
            value = result["preflight"]
            self.assertEqual(value["provider_calls"], 0)
            self.assertEqual(value["channels"]["douyin"]["evidence"]["media"]["available"], 438)
            self.assertEqual(value["channels"]["douyin"]["evidence"]["transcript"]["available"], 438)
            self.assertEqual(value["channels"]["douyin"]["evidence"]["ocr"]["available"], 438)
            self.assertEqual(value["channels"]["douyin"]["evidence"]["comments"]["available"], 438)
            self.assertEqual(value["channels"]["xiaohongshu"]["evidence"]["provider_content"]["available"], 56)
            self.assertEqual(value["channels"]["xiaohongshu"]["evidence"]["comments"]["available"], 56)
            self.assertEqual(value["paid_refresh_gap"]["xiaohongshu_provider_content_missing"], 282)
            self.assertEqual(value["paid_refresh_gap"]["xiaohongshu_comments_missing"], 282)

    def test_bootstrap_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temporary:
            db = Path(temporary) / "test.sqlite3"
            first = bootstrap(db, ROOT)
            second = bootstrap(db, ROOT)
            self.assertEqual(first["snapshot_id"], second["snapshot_id"])
            with connect(db) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 776)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_import_audit").fetchone()[0], 375)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM evidence_assets").fetchone()[0], 3542)


if __name__ == "__main__":
    unittest.main()
