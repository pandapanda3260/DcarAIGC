from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from v8.duplicates import FINGERPRINT_VERSION
from v8.operations import OperationError, upsert_content
from v8.storage import connect, initialize_database, transaction


class ContentTransactionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.db = self.root / "content.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
        self.payload = {
            "platform": "douyin", "platform_content_id": "7380000000000000041",
            "canonical_url": "https://www.douyin.com/video/7380000000000000041",
            "title": "汽车 空间", "body": "测试后排空间",
        }

    def tearDown(self):
        self.temp.cleanup()

    def test_caller_rollback_includes_content_identity_and_relation_writes(self):
        with connect(self.db) as connection:
            with self.assertRaisesRegex(RuntimeError, "page interrupted"):
                with transaction(connection):
                    upsert_content(self.payload, db_path=self.db, connection=connection)
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 1)
                    raise RuntimeError("page interrupted")
            for table in ("content_items", "content_identities", "duplicate_relations"):
                self.assertEqual(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)

    def test_external_connection_must_be_matching_active_transaction(self):
        with connect(self.db) as connection:
            with self.assertRaisesRegex(OperationError, "active"):
                upsert_content(self.payload, db_path=self.db, connection=connection)
            other = self.root / "other.sqlite3"
            with connect(other) as different:
                initialize_database(different)
                with transaction(different):
                    with self.assertRaisesRegex(OperationError, "different"):
                        upsert_content(self.payload, db_path=self.db, connection=different)

    def test_unchanged_rediscovery_keeps_exact_fingerprint_relations(self):
        first = upsert_content(self.payload, db_path=self.db)
        second = upsert_content(self.payload | {
            "platform_content_id": "7380000000000000042",
            "canonical_url": "https://www.douyin.com/video/7380000000000000042",
        }, db_path=self.db)
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "INSERT INTO duplicate_relations(duplicate_content_id,original_content_id,method,confidence,evidence_json,status,created_at) "
                "VALUES (?,?,'fingerprint_v1',1.0,'{\"proof\":\"unchanged\"}','confirmed','2026-08-29T00:00:00Z')",
                (second["id"], first["id"]),
            )
            for cid in (first["id"], second["id"]):
                connection.execute(
                    "INSERT INTO duplicate_fingerprints(content_id,fingerprint_version,source_sha256,text_sha256,payload_json,created_at) "
                    "VALUES (?,?,?,?,?,'2026-08-29T00:00:00Z')",
                    (cid, FINGERPRINT_VERSION, f"{cid:064x}", "b" * 64, '{"test_proof":true}'),
                )
            before = [tuple(row) for row in connection.execute("SELECT * FROM duplicate_relations")]
            fingerprints = [tuple(row) for row in connection.execute("SELECT * FROM duplicate_fingerprints")]
        with connect(self.db) as connection, transaction(connection):
            result = upsert_content(self.payload | {"_preserve_existing_content_fields": True},
                                    db_path=self.db, connection=connection)
            self.assertEqual(result["id"], first["id"])
            self.assertEqual([tuple(row) for row in connection.execute("SELECT * FROM duplicate_relations")], before)
            self.assertEqual([tuple(row) for row in connection.execute("SELECT * FROM duplicate_fingerprints")], fingerprints)

    def test_actual_text_change_invalidates_only_current_fingerprint_relation(self):
        first = upsert_content(self.payload, db_path=self.db)
        second = upsert_content(self.payload | {
            "platform_content_id": "7380000000000000042",
            "canonical_url": "https://www.douyin.com/video/7380000000000000042",
        }, db_path=self.db)
        with connect(self.db) as connection, transaction(connection):
            for method in ("fingerprint_v1", "operator_proof"):
                connection.execute(
                    "INSERT INTO duplicate_relations(duplicate_content_id,original_content_id,method,confidence,evidence_json,status,created_at) "
                    "VALUES (?,?,?,1.0,'{}','confirmed','2026-08-29T00:00:00Z')",
                    (second["id"], first["id"], method),
                )
        upsert_content(self.payload | {"body": "不同的续航证据"}, db_path=self.db)
        with connect(self.db) as connection:
            self.assertEqual([row[0] for row in connection.execute("SELECT method FROM duplicate_relations")], ["operator_proof"])


if __name__ == "__main__":
    unittest.main()
