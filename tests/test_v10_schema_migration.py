from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import v8.storage as storage


V9_FROZEN_SCHEMA = (
    Path(__file__).resolve().parent / "fixtures" / "schema_v9_frozen.sql"
).read_text(encoding="utf-8")


def _create_v9_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(V9_FROZEN_SCHEMA)
        connection.executescript(
            """
            INSERT INTO schema_migrations VALUES
                (8,'append-only-review-reopen-audit','2026-08-02T00:00:00Z'),
                (9,'release-bound-evaluation-schema','2026-08-04T00:00:00Z');
            INSERT INTO content_items(
                id, link_id, platform, canonical_url, imported_at, created_at, updated_at
            ) VALUES
                (1,'AAAAAA','douyin','https://www.douyin.com/video/1',
                 '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z','2026-08-01T00:00:00Z'),
                (2,'BBBBBB','xiaohongshu','https://www.xiaohongshu.com/explore/2',
                 '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z','2026-08-01T00:00:00Z');
            INSERT INTO comment_evidence_versions(
                id, content_id, captured_at, iso_week, source, local_path, sha256,
                comment_count, status, created_at
            ) VALUES
                (1,1,'2026-08-01T01:00:00Z','2026-W31','douyin','data/cache/c1.json',
                 lower(hex(randomblob(32))),3,'available','2026-08-01T01:00:00Z'),
                (2,2,'2026-08-01T02:00:00Z','2026-W31','xiaohongshu','data/cache/c2.json',
                 lower(hex(randomblob(32))),1,'available','2026-08-01T02:00:00Z');
            INSERT INTO comments(
                id, evidence_version_id, platform_comment_id, anonymous_user_key,
                body, published_at, like_count, parent_comment_id
            ) VALUES
                (1,1,'c-1','Uaaa','这车提速真不错','2026-07-31T10:00:00Z',3,NULL),
                (2,1,'c-2','Ubbb','油耗多少','2026-07-31T11:00:00Z',0,NULL),
                (3,1,'c-3','Uccc','回复：同问','2026-07-31T12:00:00Z',0,'c-1'),
                (4,2,'c-9','Uddd','好看','2026-07-31T13:00:00Z',1,NULL);
            INSERT INTO comment_user_scores(
                content_id, evidence_version_id, anonymous_user_key,
                audience_automotive_score, action_intent_score, evaluated_at
            ) VALUES
                (1,1,'Uaaa',100,0,'2026-08-01T03:00:00Z'),
                (1,1,'Ubbb',30,50,'2026-08-01T03:00:00Z'),
                (2,2,'Uddd',0,0,'2026-08-01T03:00:00Z');
            DELETE FROM comments WHERE id=3;
            PRAGMA user_version=9;
            """
        )
        connection.commit()
    finally:
        connection.close()


class V10SchemaMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "schema.sqlite3"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _expect_integrity_error(self, connection, sql: str) -> None:
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute(sql)
        connection.rollback()

    def test_fresh_database_is_v10_and_reinitialization_is_idempotent(self) -> None:
        with storage.connect(self.db) as connection:
            storage.initialize_database(connection)
            versions = connection.execute(
                "SELECT version, name FROM schema_migrations ORDER BY version"
            ).fetchall()
            self.assertEqual(
                [(int(row[0]), str(row[1])) for row in versions],
                [
                    (9, "release-bound-evaluation-schema"),
                    (10, "audience-interaction-user-domain"),
                ],
            )
            self.assertEqual(
                int(connection.execute("PRAGMA user_version").fetchone()[0]), 10
            )
            tables = storage._table_names(connection)
            for table in storage._NEW_V10_TABLES:
                self.assertIn(table, tables)
            storage.initialize_database(connection)
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT COUNT(*) FROM schema_migrations"
                    ).fetchone()[0]
                ),
                2,
            )

    def test_migrating_v9_database_preserves_rows_and_backfills_versions(self) -> None:
        _create_v9_database(self.db)
        reference = sqlite3.connect(self.db)
        reference.row_factory = sqlite3.Row
        old_columns = {
            table: storage._table_columns(reference, table)
            for table in storage._REBUILT_V10_TABLES
        }
        old_hashes = {
            table: storage._table_projection_sha256(reference, table, columns)
            for table, columns in old_columns.items()
        }
        old_sequence = int(
            reference.execute(
                "SELECT seq FROM sqlite_sequence WHERE name='comments'"
            ).fetchone()[0]
        )
        reference.close()

        with storage.connect(self.db) as connection:
            storage.initialize_database(connection)
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT MAX(version) FROM schema_migrations"
                    ).fetchone()[0]
                ),
                10,
            )
            self.assertEqual(
                int(connection.execute("PRAGMA user_version").fetchone()[0]), 10
            )
            for table, columns in old_columns.items():
                self.assertEqual(
                    storage._table_projection_sha256(connection, table, columns),
                    old_hashes[table],
                    table,
                )
            self.assertEqual(
                [
                    tuple(row)
                    for row in connection.execute(
                        """
                        SELECT DISTINCT key_version, score_rule_version
                        FROM comment_user_scores
                        """
                    )
                ],
                [(storage.LEGACY_COMMENT_USER_KEY_VERSION,
                  storage.LEGACY_COMMENT_SCORE_RULE_VERSION)],
            )
            self.assertIn(
                "capture_run_id",
                storage._table_columns(connection, "comment_evidence_versions"),
            )
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT COUNT(*) FROM interaction_users"
                    ).fetchone()[0]
                ),
                0,
            )
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT seq FROM sqlite_sequence WHERE name='comments'"
                    ).fetchone()[0]
                ),
                old_sequence,
            )
            indexes = {
                str(row["name"])
                for row in connection.execute("PRAGMA index_list(comments)")
            }
            self.assertIn("uq_comments_identity_per_evidence", indexes)
            self.assertEqual(
                connection.execute("PRAGMA foreign_key_check").fetchall(), []
            )
            self.assertEqual(
                connection.execute("PRAGMA integrity_check").fetchone()[0], "ok"
            )

    def test_v10_constraints_enforce_key_versions_and_capture_uniqueness(self) -> None:
        with storage.connect(self.db) as connection:
            storage.initialize_database(connection)
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
                         lower(hex(randomblob(32))),0,'partial','2026-08-01T01:00:00Z')
                    """
                )

            self._expect_integrity_error(
                connection,
                """
                    INSERT INTO comment_user_scores(
                        content_id, evidence_version_id, anonymous_user_key,
                        audience_automotive_score, action_intent_score,
                        key_version, score_rule_version, evaluated_at
                    ) VALUES (1,1,'P2aaa',100,0,'platform-user-hmac-v2',
                              'legacy-audience-action-v1','2026-08-01T03:00:00Z')
                    """,
            )
            self._expect_integrity_error(
                connection,
                """
                    INSERT INTO interaction_users(
                        platform, pseudonymous_user_key, key_version,
                        first_seen_at, last_seen_at
                    ) VALUES ('douyin','P1aaa','content-user-hmac-v1',
                              '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
                    """,
            )

            with storage.transaction(connection):
                connection.execute(
                    """
                    INSERT INTO interaction_users(
                        platform, pseudonymous_user_key,
                        first_seen_at, last_seen_at
                    ) VALUES ('douyin','P2aaa','2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
                    """
                )
            self._expect_integrity_error(
                connection,
                """
                    INSERT INTO interaction_users(
                        platform, pseudonymous_user_key,
                        first_seen_at, last_seen_at
                    ) VALUES ('douyin','P2aaa','2026-08-02T00:00:00Z','2026-08-02T00:00:00Z')
                    """,
            )
            self._expect_integrity_error(
                connection,
                """
                    INSERT INTO interaction_user_classification_versions(
                        interaction_user_id, audience_definition_version,
                        classifier_version, evidence_window_start, evidence_window_end,
                        evidence_sha256, label, created_at
                    ) VALUES (1,'audience-definition-v1','classifier-v1',
                              '2026-05-01T00:00:00Z','2026-08-01T00:00:00Z',
                              lower(hex(randomblob(32))),'maybe','2026-08-01T00:00:00Z')
                    """,
            )

            with storage.transaction(connection):
                connection.execute(
                    """
                    INSERT INTO comment_capture_runs(
                        id, content_id, window_key, provider, adapter_version,
                        status, created_at, updated_at
                    ) VALUES (1,1,'2026-W31','tikhub','tikhub-comments-v8.2',
                              'running','2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
                    """
                )
            self._expect_integrity_error(
                connection,
                """
                    INSERT INTO comment_capture_runs(
                        content_id, window_key, provider, adapter_version,
                        status, created_at, updated_at
                    ) VALUES (1,'2026-W31','tikhub','tikhub-comments-v8.3',
                              'pending','2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
                    """,
            )

            with storage.transaction(connection):
                connection.execute(
                    """
                    INSERT INTO fetch_slots(
                        id, content_id, stage, window_key, provider, adapter_version,
                        status, created_at, updated_at
                    ) VALUES
                        (1,1,'comments','2026-W31:page:aaa','tikhub','v1','succeeded',
                         '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z'),
                        (2,1,'comments','2026-W31:page:bbb','tikhub','v1','succeeded',
                         '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
                    """
                )
                connection.execute(
                    """
                    INSERT INTO provider_raw_responses(
                        id, content_id, provider, operation, local_path, sha256,
                        byte_size, captured_at
                    ) VALUES
                        (1,1,'tikhub','comments_page','data/cache/p1.json',
                         lower(hex(randomblob(32))),10,'2026-08-01T00:00:00Z'),
                        (2,1,'tikhub','comments_page','data/cache/p2.json',
                         lower(hex(randomblob(32))),10,'2026-08-01T00:00:00Z')
                    """
                )
                connection.execute(
                    """
                    INSERT INTO comment_capture_pages(
                        capture_run_id, page_number, request_cursor_json,
                        request_cursor_sha256, next_cursor_json, next_cursor_sha256,
                        fetch_slot_id, raw_response_id, has_more, received_count,
                        captured_at
                    ) VALUES (1,1,'{"cursor":0}',lower(hex(randomblob(32))),
                              '{"cursor":20}',lower(hex(randomblob(32))),
                              1,1,1,20,'2026-08-01T00:00:00Z')
                    """
                )
            for duplicate in (
                # duplicate page number for the same run
                """
                INSERT INTO comment_capture_pages(
                    capture_run_id, page_number, request_cursor_json,
                    request_cursor_sha256, fetch_slot_id, raw_response_id,
                    has_more, captured_at
                ) VALUES (1,1,'{"cursor":20}',lower(hex(randomblob(32))),
                          2,2,0,'2026-08-01T00:01:00Z')
                """,
                # duplicate fetch slot for another page
                """
                INSERT INTO comment_capture_pages(
                    capture_run_id, page_number, request_cursor_json,
                    request_cursor_sha256, fetch_slot_id, raw_response_id,
                    has_more, captured_at
                ) VALUES (1,2,'{"cursor":20}',lower(hex(randomblob(32))),
                          1,2,0,'2026-08-01T00:01:00Z')
                """,
            ):
                self._expect_integrity_error(connection, duplicate)

            with storage.transaction(connection):
                connection.execute(
                    """
                    INSERT INTO comments(
                        evidence_version_id, platform_comment_id, body,
                        comment_identity_key
                    ) VALUES (1,'c-1','第一条','idkey-1')
                    """
                )
            self._expect_integrity_error(
                connection,
                """
                    INSERT INTO comments(
                        evidence_version_id, platform_comment_id, body,
                        comment_identity_key
                    ) VALUES (1,'c-2','重复身份','idkey-1')
                    """,
            )
            with storage.transaction(connection):
                connection.execute(
                    """
                    INSERT INTO comments(
                        evidence_version_id, platform_comment_id, body,
                        comment_identity_key
                    ) VALUES (1,'c-3','无身份键一',NULL)
                    """
                )
                connection.execute(
                    """
                    INSERT INTO comments(
                        evidence_version_id, platform_comment_id, body,
                        comment_identity_key
                    ) VALUES (1,'c-4','无身份键二',NULL)
                    """
                )

    def test_mid_v10_migration_failure_rolls_back_schema_and_data(self) -> None:
        _create_v9_database(self.db)
        with storage.connect(self.db) as connection:
            before = storage._table_projection_sha256(
                connection,
                "comment_user_scores",
                storage._table_columns(connection, "comment_user_scores"),
            )
            with patch.object(
                storage,
                "_migration_checkpoint",
                side_effect=lambda name: (
                    (_ for _ in ()).throw(RuntimeError("injected v10 failure"))
                    if name == "v10_rows_copied"
                    else None
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "injected v10 failure"):
                    storage.initialize_database(connection)

            self.assertEqual(
                int(connection.execute("PRAGMA foreign_keys").fetchone()[0]), 1
            )
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT MAX(version) FROM schema_migrations"
                    ).fetchone()[0]
                ),
                9,
            )
            tables = storage._table_names(connection)
            self.assertNotIn("interaction_users", tables)
            self.assertFalse(
                [name for name in tables if name.endswith("_v10_new")]
            )
            self.assertNotIn(
                "key_version",
                storage._table_columns(connection, "comment_user_scores"),
            )
            self.assertEqual(
                storage._table_projection_sha256(
                    connection,
                    "comment_user_scores",
                    storage._table_columns(connection, "comment_user_scores"),
                ),
                before,
            )
            storage.initialize_database(connection)
            self.assertEqual(
                int(
                    connection.execute(
                        "SELECT MAX(version) FROM schema_migrations"
                    ).fetchone()[0]
                ),
                10,
            )

    def test_partial_v10_residue_fails_closed(self) -> None:
        _create_v9_database(self.db)
        with storage.connect(self.db) as connection:
            connection.execute("CREATE TABLE comments_v10_new(id INTEGER PRIMARY KEY)")
            connection.commit()
            with self.assertRaisesRegex(
                storage.SchemaMigrationError, "partial v10 tables"
            ):
                storage.initialize_database(connection)


if __name__ == "__main__":
    unittest.main()
