from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import v8.storage as storage
from tests.schema_fixture import initialize_historical_schema




def _seed_v8_fixture(connection: sqlite3.Connection) -> None:
    initialize_historical_schema(connection, target_version=8)
    captured_at = "2026-08-02T00:00:00Z"
    connection.execute(
        """
        INSERT INTO taxonomy_versions(id,version,status,definition,created_at,published_at)
        VALUES ('taxonomy','selling-points-v5.0','published','fixture',?,?)
        """,
        (captured_at, captured_at),
    )
    connection.execute(
        """
        INSERT INTO selling_points(taxonomy_id,code,tier,label)
        VALUES ('taxonomy','C1','core','fixture')
        """
    )
    connection.executemany(
        """INSERT INTO content_items(
            id,link_id,platform,canonical_url,imported_at,created_at,updated_at
        ) VALUES (?,?,'douyin',?,?,?,?)""",
        [
            (item, f"TEST0{item}", f"https://example.com/{item}",
             captured_at, captured_at, captured_at)
            for item in (1, 2)
        ],
    )
    connection.executemany(
        """INSERT INTO evidence_envelopes(
            id,content_id,schema_version,text_sha256,evidence_sha256,components_json,created_at
        ) VALUES (?,?,'evidence-envelope-v1',?,?,'{}',?)""",
        [(item, item, "a" * 64, "b" * 64, captured_at) for item in (1, 2)],
    )

    rows = [
        (1, 1, 1, "evaluation-v6", "automatic", "a" * 64, {}, "media"),
        (
            2,
            1,
            1,
            "evaluation-v7",
            "automatic",
            "a" * 64,
            {"upgraded_from_rule_version": "evaluation-v6"},
            "media",
        ),
        (3, 2, 2, "evaluation-v7", "automatic", "b" * 64, {}, "new_car"),
        (4, 1, 1, "evaluation-v6", "manual_review", "c" * 64, {}, "media"),
        (
            5,
            1,
            1,
            "evaluation-v7",
            "manual_review",
            "c" * 64,
            {"upgraded_from_rule_version": "evaluation-v6"},
            "media",
        ),
        (6, 2, 2, "evaluation-v6", "migrated_from_v5", "d" * 64, {}, "used_car"),
        (
            7,
            2,
            2,
            "evaluation-v7",
            "migrated_from_v5",
            "d" * 64,
            {"upgraded_from_rule_version": "evaluation-v6"},
            "used_car",
        ),
    ]
    connection.executemany(
        """
        INSERT INTO evaluation_versions(
            id,content_id,evidence_envelope_id,rule_version,taxonomy_version,
            evidence_sha256,evaluation_source,evaluation_status,evidence_level,
            primary_selling_point_code,selling_point_score,selling_point_included,
            content_direction,pending_review,payload_json,evaluated_at
        ) VALUES (?,?,?,?,'selling-points-v5.0',?,?,'evaluated','V3',
                  'C1',90,1,?,0,?,?)
        """,
        [
            (
                row_id,
                content_id,
                envelope_id,
                rule_version,
                evidence_sha,
                source,
                direction,
                json.dumps(payload, separators=(",", ":")),
                captured_at,
            )
            for (
                row_id,
                content_id,
                envelope_id,
                rule_version,
                source,
                evidence_sha,
                payload,
                direction,
            ) in rows
        ],
    )
    connection.executemany(
        """
        INSERT INTO evaluation_matches(
            evaluation_id,selling_point_code,match_role,score,evidence_json
        ) VALUES (?,'C1','primary',90,'{}')
        """,
        [(1,), (2,), (3,)],
    )
    connection.execute(
        """
        INSERT INTO review_queue(
            id,content_id,evaluation_id,reason_code,status,created_at,updated_at
        ) VALUES (1,1,4,'evaluation_gray_zone','resolved',?,?)
        """,
        (captured_at, captured_at),
    )
    connection.execute(
        """
        INSERT INTO evaluation_reviews(
            id,queue_id,content_id,previous_evaluation_id,resulting_evaluation_id,
            decision,reason,reviewer,created_at
        ) VALUES (1,1,1,1,4,'override','fixture','reviewer',?)
        """,
        (captured_at,),
    )
    connection.execute(
        """
        INSERT INTO review_reopen_events(
            id,queue_id,content_id,previous_review_id,base_evaluation_id,
            reopened_by,reason,created_at
        ) VALUES (1,1,1,1,4,'reviewer','fixture',?)
        """,
        (captured_at,),
    )
    connection.execute(
        """
        INSERT INTO manual_evidence(
            id,review_id,content_id,evidence_type,text_value,sha256,created_at
        ) VALUES (1,1,1,'review_note','fixture',?,?)
        """,
        ("e" * 64, captured_at),
    )
    connection.execute(
        """INSERT INTO report_tasks(
            id,task_type,name,period_start,period_end,creation_source,task_status,created_at,updated_at
        ) VALUES ('task','daily','fixture','2026-08-01','2026-08-02','manual','queued',?,?)""",
        (captured_at, captured_at),
    )
    connection.executemany(
        """
        INSERT INTO report_revisions(
            task_id,revision,contract_version,rule_version,taxonomy_version,
            report_json_path,report_sha256,created_at
        ) VALUES ('task',?,'contract',?,'selling-points-v5.0',?,?,?)
        """,
        [
            (1, "evaluation-v6", "v6.json", "f" * 64, captured_at),
            (2, "evaluation-v7", "v7.json", "0" * 64, captured_at),
        ],
    )
    connection.executemany(
        """
        INSERT INTO report_files(
            id,task_id,revision,file_kind,local_path,sha256,byte_size,status,created_at
        ) VALUES (?,'task',?,'report-json',?,?,1,'available',?)
        """,
        [
            ("file-v6", 1, "v6.json", "1" * 64, captured_at),
            ("file-v7", 2, "v7.json", "2" * 64, captured_at),
        ],
    )
    connection.execute("PRAGMA user_version=8")
    connection.commit()


def _seed_v9_constraint_fixture(connection: sqlite3.Connection) -> dict[str, int]:
    storage.initialize_database(connection)
    captured_at = "2026-08-04T00:00:00Z"
    connection.execute(
        """
        INSERT INTO taxonomy_versions(
            id,version,status,definition,created_at,published_at
        ) VALUES ('taxonomy','selling-points-v5.0','published','fixture',?,?)
        """,
        (captured_at, captured_at),
    )
    storage.ensure_legacy_evaluation_release(
        connection,
        rule_version="evaluation-v6",
        taxonomy_version="selling-points-v5.0",
    )
    storage.ensure_legacy_evaluation_release(
        connection,
        rule_version="evaluation-v7",
        taxonomy_version="selling-points-v5.0",
    )
    connection.executemany(
        """
        INSERT INTO content_items(
            id,link_id,platform,canonical_url,imported_at,created_at,updated_at
        ) VALUES (?,?,'douyin',?,?,?,?)
        """,
        [
            (
                1,
                "V9T001",
                "https://example.com/1",
                captured_at,
                captured_at,
                captured_at,
            ),
            (
                2,
                "V9T002",
                "https://example.com/2",
                captured_at,
                captured_at,
                captured_at,
            ),
        ],
    )
    automatic = connection.execute(
        """
        INSERT INTO evaluation_versions(
            content_id,release_id,rule_version,taxonomy_version,matcher_rule_sha256,
            evidence_sha256,evaluation_source,evaluation_status,evidence_level,
            payload_json,evaluated_at
        ) VALUES (1,?,'evaluation-v7','selling-points-v5.0',?,?,'automatic',
                  'evaluated','V3','{}',?)
        """,
        (
            storage.LEGACY_V7_RELEASE_ID,
            storage.LEGACY_MATCHER_RULE_SHA256,
            "a" * 64,
            captured_at,
        ),
    )
    automatic_id = int(automatic.lastrowid)
    connection.commit()
    return {"automatic_id": automatic_id}


def _insert_v9_evaluation(
    connection: sqlite3.Connection,
    *,
    content_id: int,
    release_id: str,
    rule_version: str,
    evidence_sha256: str,
    source: str,
    parent_evaluation_id: int | None = None,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO evaluation_versions(
            content_id,release_id,parent_evaluation_id,rule_version,
            taxonomy_version,matcher_rule_sha256,evidence_sha256,evaluation_source,
            evaluation_status,evidence_level,payload_json,evaluated_at
        ) VALUES (?,?,?,?,'selling-points-v5.0',?,?,?,'evaluated','V3','{}',?)
        """,
        (
            content_id,
            release_id,
            parent_evaluation_id,
            rule_version,
            storage.LEGACY_MATCHER_RULE_SHA256,
            evidence_sha256,
            source,
            "2026-08-04T00:00:00Z",
        ),
    )
    return int(cursor.lastrowid)


class V9SchemaMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "schema.sqlite3"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_fresh_schema_is_current_and_second_initialization_is_read_only(self) -> None:
        with storage.connect(self.db) as connection:
            storage.initialize_database(connection)
            self.assertEqual(
                connection.execute(
                    "SELECT MAX(version) FROM schema_migrations"
                ).fetchone()[0],
                storage.SCHEMA_VERSION,
            )
            self.assertIn(
                "matcher_rule_json",
                storage._table_columns(connection, "selling_points"),
            )
            self.assertNotIn(
                "uq_evaluation_idempotency",
                {
                    row["name"]
                    for row in connection.execute(
                        "PRAGMA index_list(evaluation_versions)"
                    )
                },
            )
            unique_indexes = {
                str(row["name"]): [
                    str(column["name"])
                    for column in connection.execute(
                        f"PRAGMA index_info('{row['name']}')"
                    )
                ]
                for row in connection.execute("PRAGMA index_list(evaluation_versions)")
                if int(row["unique"]) == 1
            }
            self.assertNotIn(
                ["content_id", "rule_version", "taxonomy_version", "evidence_sha256"],
                unique_indexes.values(),
            )
            self.assertEqual(
                unique_indexes["uq_evaluation_automatic_idempotency"],
                ["content_id", "release_id", "evidence_sha256"],
            )
            # v16 起复核域删除：人工幂等索引（键含 review_id）不复存在
            self.assertNotIn("uq_evaluation_manual_idempotency", unique_indexes)
            self.assertEqual(
                unique_indexes["uq_evaluation_migrated_parent_idempotency"],
                ["release_id", "parent_evaluation_id"],
            )
            migrated_index_sql = str(
                connection.execute(
                    """
                    SELECT sql FROM sqlite_master
                    WHERE type='index' AND name='uq_evaluation_migrated_parent_idempotency'
                    """
                ).fetchone()[0]
            )
            self.assertIn("parent_evaluation_id IS NOT NULL", migrated_index_sql)
            before = connection.total_changes
            statements: list[str] = []
            connection.set_trace_callback(statements.append)
            storage.initialize_database(connection)
            connection.set_trace_callback(None)
            self.assertEqual(connection.total_changes, before)
            write_prefixes = (
                "BEGIN",
                "CREATE",
                "ALTER",
                "DROP",
                "INSERT",
                "UPDATE",
                "DELETE",
            )
            self.assertFalse(
                [
                    value
                    for value in statements
                    if value.lstrip().upper().startswith(write_prefixes)
                ]
            )

    def test_v9_source_specific_idempotency_constraints(self) -> None:
        with storage.connect(self.db) as connection:
            fixture = _seed_v9_constraint_fixture(connection)
            with self.assertRaises(sqlite3.IntegrityError):
                _insert_v9_evaluation(
                    connection,
                    content_id=1,
                    release_id=storage.LEGACY_V7_RELEASE_ID,
                    rule_version="evaluation-v7",
                    evidence_sha256="a" * 64,
                    source="automatic",
                )
            connection.rollback()
            _insert_v9_evaluation(
                connection,
                content_id=1,
                release_id=storage.LEGACY_V7_RELEASE_ID,
                rule_version="evaluation-v7",
                evidence_sha256="b" * 64,
                source="automatic",
            )
            _insert_v9_evaluation(
                connection,
                content_id=1,
                release_id=storage.LEGACY_V6_RELEASE_ID,
                rule_version="evaluation-v6",
                evidence_sha256="a" * 64,
                source="automatic",
            )
            # v16 移除了 uq_evaluation_manual_idempotency（键含 review_id）。
            # 人工结论版本保留，但不再有复核维度的幂等约束；这里改为确认
            # 自动评估的幂等键仍然生效、人工版本可自由并存。
            manual_v7 = _insert_v9_evaluation(
                connection,
                content_id=1,
                release_id=storage.LEGACY_V7_RELEASE_ID,
                rule_version="evaluation-v7",
                evidence_sha256="m" * 64,
                source="manual_review",
                parent_evaluation_id=fixture["automatic_id"],
            )
            manual_v6 = _insert_v9_evaluation(
                connection,
                content_id=1,
                release_id=storage.LEGACY_V6_RELEASE_ID,
                rule_version="evaluation-v6",
                evidence_sha256="m" * 64,
                source="manual_review",
                parent_evaluation_id=fixture["automatic_id"],
            )
            connection.commit()
            self.assertEqual(
                {manual_v7, manual_v6},
                {
                    int(row[0])
                    for row in connection.execute(
                        "SELECT id FROM evaluation_versions WHERE evaluation_source='manual_review'"
                    )
                },
            )

    def test_migrated_parent_idempotency_preserves_the_null_root_hole(self) -> None:
        with storage.connect(self.db) as connection:
            fixture = _seed_v9_constraint_fixture(connection)
            child = _insert_v9_evaluation(
                connection,
                content_id=1,
                release_id=storage.LEGACY_V7_RELEASE_ID,
                rule_version="evaluation-v7",
                evidence_sha256="c" * 64,
                source="migrated_from_v5",
                parent_evaluation_id=fixture["automatic_id"],
            )
            connection.commit()
            with self.assertRaises(sqlite3.IntegrityError):
                _insert_v9_evaluation(
                    connection,
                    content_id=2,
                    release_id=storage.LEGACY_V7_RELEASE_ID,
                    rule_version="evaluation-v7",
                    evidence_sha256="d" * 64,
                    source="migrated_from_v5",
                    parent_evaluation_id=fixture["automatic_id"],
                )
            connection.rollback()
            cross_release = _insert_v9_evaluation(
                connection,
                content_id=1,
                release_id=storage.LEGACY_V6_RELEASE_ID,
                rule_version="evaluation-v6",
                evidence_sha256="c" * 64,
                source="migrated_from_v5",
                parent_evaluation_id=fixture["automatic_id"],
            )
            root_one = _insert_v9_evaluation(
                connection,
                content_id=2,
                release_id=storage.LEGACY_V6_RELEASE_ID,
                rule_version="evaluation-v6",
                evidence_sha256="r" * 64,
                source="migrated_from_v5",
            )
            root_two = _insert_v9_evaluation(
                connection,
                content_id=2,
                release_id=storage.LEGACY_V6_RELEASE_ID,
                rule_version="evaluation-v6",
                evidence_sha256="r" * 64,
                source="migrated_from_v5",
            )
            connection.commit()
            self.assertEqual(len({child, cross_release, root_one, root_two}), 4)

    def test_release_scene_and_lineage_constraints_fail_closed(self) -> None:
        with storage.connect(self.db) as connection:
            fixture = _seed_v9_constraint_fixture(connection)
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE evaluation_releases SET status='active' WHERE id=?",
                    (storage.LEGACY_V6_RELEASE_ID,),
                )
            connection.rollback()
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    """
                    INSERT INTO evaluation_matches(
                        evaluation_id,selling_point_code,scene,match_role,evidence_json
                    ) VALUES (?,'C1','other','primary','{}')
                    """,
                    (fixture["automatic_id"],),
                )
            connection.rollback()
            parent = _insert_v9_evaluation(
                connection,
                content_id=2,
                release_id=storage.LEGACY_V7_RELEASE_ID,
                rule_version="evaluation-v7",
                evidence_sha256="p" * 64,
                source="automatic",
            )
            child = _insert_v9_evaluation(
                connection,
                content_id=2,
                release_id=storage.LEGACY_V7_RELEASE_ID,
                rule_version="evaluation-v7",
                evidence_sha256="q" * 64,
                source="migrated_from_v5",
                parent_evaluation_id=parent,
            )
            manual = _insert_v9_evaluation(
                connection,
                content_id=1,
                release_id=storage.LEGACY_V7_RELEASE_ID,
                rule_version="evaluation-v7",
                evidence_sha256="m" * 64,
                source="manual_review",
                parent_evaluation_id=fixture["automatic_id"],
            )
            connection.commit()
            for sql, parameters in (
                (
                    "DELETE FROM evaluation_releases WHERE id=?",
                    (storage.LEGACY_V7_RELEASE_ID,),
                ),
                ("DELETE FROM evaluation_versions WHERE id=?", (parent,)),
            ):
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(sql, parameters)
                connection.rollback()
            self.assertEqual(
                {parent, child, manual}.issubset(
                    {
                        int(row[0])
                        for row in connection.execute(
                            "SELECT id FROM evaluation_versions"
                        )
                    }
                ),
                True,
            )
            self.assertEqual(
                connection.execute("PRAGMA foreign_key_check").fetchall(), []
            )

    def test_v8_graph_migrates_with_lineage_and_projection_intact(self) -> None:
        with storage.connect(self.db) as connection:
            _seed_v8_fixture(connection)
            surviving = tuple(
                table
                for table in storage._REBUILT_V9_TABLES
                if table not in storage._LEGACY_REVIEW_TABLES
            )
            removed_columns = {"review_id", "pending_review"}
            old_columns = {
                table: [
                    column
                    for column in storage._table_columns(connection, table)
                    if column not in removed_columns
                ]
                for table in surviving
            }
            old_hashes = {
                table: storage._table_projection_sha256(connection, table, columns)
                for table, columns in old_columns.items()
            }
            storage.initialize_database(connection)

            self.assertEqual(
                [
                    tuple(row)
                    for row in connection.execute(
                        "SELECT id,status FROM evaluation_releases ORDER BY id"
                    )
                ],
                [
                    (storage.LEGACY_V6_RELEASE_ID, "retired"),
                    (storage.LEGACY_V7_RELEASE_ID, "active"),
                ],
            )
            self.assertEqual(
                {
                    int(row["id"]): row["parent_evaluation_id"]
                    for row in connection.execute(
                        "SELECT id,parent_evaluation_id FROM evaluation_versions"
                    )
                },
                {1: None, 2: 1, 3: None, 4: None, 5: 4, 6: None, 7: 6},
            )
            # v16 删除复核域后人工结论保留为自然沉底版本，只是不再回链复核记录
            self.assertEqual(
                [
                    int(row[0])
                    for row in connection.execute(
                        """
                        SELECT id FROM evaluation_versions
                        WHERE evaluation_source='manual_review' ORDER BY id
                        """
                    )
                ],
                [4, 5],
            )
            surviving_tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            self.assertEqual(
                surviving_tables
                & {
                    "review_queue",
                    "evaluation_reviews",
                    "review_reopen_events",
                    "manual_evidence",
                },
                set(),
            )
            self.assertEqual(
                {
                    row["scene"]
                    for row in connection.execute(
                        "SELECT scene FROM evaluation_matches"
                    )
                },
                {"media", "new_car"},
            )
            self.assertEqual(
                [
                    tuple(row)
                    for row in connection.execute(
                        "SELECT release_id,COUNT(*) FROM report_revisions GROUP BY release_id ORDER BY release_id"
                    )
                ],
                [
                    (storage.LEGACY_V6_RELEASE_ID, 1),
                    (storage.LEGACY_V7_RELEASE_ID, 1),
                ],
            )
            for table, columns in old_columns.items():
                self.assertEqual(
                    storage._table_projection_sha256(connection, table, columns),
                    old_hashes[table],
                    table,
                )
            self.assertEqual(
                connection.execute("PRAGMA foreign_key_check").fetchall(), []
            )
            self.assertEqual(
                connection.execute("PRAGMA integrity_check").fetchone()[0], "ok"
            )

    def test_mid_migration_failure_rolls_back_schema_and_data(self) -> None:
        with storage.connect(self.db) as connection:
            _seed_v8_fixture(connection)
            before = storage._table_projection_sha256(
                connection,
                "evaluation_versions",
                storage._table_columns(connection, "evaluation_versions"),
            )
            with patch.object(
                storage,
                "_migration_checkpoint",
                side_effect=lambda name: (
                    (_ for _ in ()).throw(RuntimeError("injected migration failure"))
                    if name == "rows_copied"
                    else None
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "injected migration failure"):
                    storage.initialize_database(connection)

            self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
            self.assertEqual(
                connection.execute(
                    "SELECT MAX(version) FROM schema_migrations"
                ).fetchone()[0],
                8,
            )
            self.assertNotIn("evaluation_releases", storage._table_names(connection))
            self.assertFalse(
                [
                    name
                    for name in storage._table_names(connection)
                    if name.endswith("_v9_new")
                ]
            )
            self.assertEqual(
                storage._table_projection_sha256(
                    connection,
                    "evaluation_versions",
                    storage._table_columns(connection, "evaluation_versions"),
                ),
                before,
            )
            storage.initialize_database(connection)
            self.assertEqual(
                connection.execute(
                    "SELECT MAX(version) FROM schema_migrations"
                ).fetchone()[0],
                storage.SCHEMA_VERSION,
            )


if __name__ == "__main__":
    unittest.main()
