from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import v8.storage as storage
from tests.schema_fixture import initialize_historical_schema


STAMP = "2026-08-28T00:00:00Z"


class V18SchemaMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source_path = self.root / "source17.sqlite3"
        with sqlite3.connect(self.source_path) as c:
            c.row_factory = sqlite3.Row
            storage.configure_connection_safety(c)
            initialize_historical_schema(c, target_version=17)
            c.executemany(
                "INSERT INTO accounts(id,phone,phone_normalized,operator_name,enabled,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                [
                    (10, "13800000010", "13800000010", "owner", 0, STAMP, STAMP),
                    (20, "13800000020", "13800000020", "other", 1, STAMP, STAMP),
                    (30, "", "", "archive", 1, STAMP, STAMP),
                ],
            )
            c.executemany(
                "INSERT INTO account_platform_identities(id,account_id,platform,uid,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?)",
                [
                    (100, 10, "douyin", "dy", STAMP, STAMP),
                    (101, 10, "xiaohongshu", "xhs", STAMP, STAMP),
                    (102, 20, "xiaohongshu", "other", STAMP, STAMP),
                ],
            )
            c.executemany(
                "INSERT INTO content_items(id,link_id,platform,canonical_url,account_id,imported_at,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                [
                    (
                        1,
                        "AAAAA1",
                        "xiaohongshu",
                        "https://example.com/1",
                        10,
                        STAMP,
                        STAMP,
                        STAMP,
                    ),
                    (
                        2,
                        "AAAAA2",
                        "douyin",
                        "https://example.com/2",
                        10,
                        STAMP,
                        STAMP,
                        STAMP,
                    ),
                    (
                        3,
                        "AAAAA3",
                        "xiaohongshu",
                        "https://example.com/3",
                        20,
                        STAMP,
                        STAMP,
                        STAMP,
                    ),
                    (
                        4,
                        "AAAAA4",
                        "xiaohongshu",
                        "https://example.com/4",
                        None,
                        STAMP,
                        STAMP,
                        STAMP,
                    ),
                ],
            )
            c.executemany(
                "INSERT INTO fetch_slots(id,account_id,stage,window_key,provider,adapter_version,status,created_at,updated_at) "
                "VALUES (?,10,'discovery',?,'TikHub',?,'succeeded',?,?)",
                [
                    (
                        1,
                        "no-platform-hint",
                        "tikhub-xhs-app-v2-user-posts-v8.1",
                        STAMP,
                        STAMP,
                    ),
                    (
                        2,
                        "xiaohongshu-looking-window",
                        "tikhub-user-posts-v8.1",
                        STAMP,
                        STAMP,
                    ),
                ],
            )
            c.executemany(
                "INSERT INTO fetch_attempts(id,slot_id,attempt_number,request_started_at) VALUES (?,?,1,?)",
                [(1, 1, STAMP), (2, 2, STAMP)],
            )
            c.executemany(
                "INSERT INTO provider_raw_responses(id,fetch_attempt_id,account_id,content_id,provider,operation,"
                "local_path,sha256,byte_size,captured_at) VALUES (?,?,10,?,'TikHub',?,?,?,1,?)",
                [
                    (1, 1, None, "xiaohongshu_user_posts", "xhs.json", "a" * 64, STAMP),
                    (2, 2, None, "douyin_user_posts", "dy.json", "b" * 64, STAMP),
                    (
                        3,
                        None,
                        1,
                        "xiaohongshu_note_detail",
                        "detail.json",
                        "c" * 64,
                        STAMP,
                    ),
                ],
            )
            c.execute(
                "INSERT INTO account_provider_references(account_identity_id,provider,reference_kind,reference_value,created_at,updated_at) "
                "VALUES (100,'TikHub','sec_user_id','known',?,?)",
                (STAMP, STAMP),
            )
            c.execute(
                "INSERT INTO content_metric_snapshots(id,content_id,captured_at,window_key,status,source) "
                "VALUES (7,1,?,'daily','available','xiaohongshu')",
                (STAMP,),
            )
            c.execute(
                "INSERT INTO evidence_artifacts(content_id,artifact_type,local_path,status,sha256,created_at) "
                "VALUES (1,'image','history/image.jpg','available',?,?)",
                ("d" * 64, STAMP),
            )
            c.execute("UPDATE sqlite_sequence SET seq=1000 WHERE name='accounts'")
        self.source = sqlite3.connect(
            self.source_path.as_uri() + "?mode=ro&immutable=1", uri=True
        )
        self.source.row_factory = sqlite3.Row
        storage.configure_connection_safety(self.source)
        self.addCleanup(self.source.close)

    def candidate(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.root / "candidate.sqlite3")
        c.row_factory = sqlite3.Row
        self.source.backup(c)
        storage.configure_connection_safety(c)
        self.addCleanup(c.close)
        return c

    def test_fresh18_structure_is_independent_of_future_current_ddl(self) -> None:
        with sqlite3.connect(":memory:") as c:
            c.row_factory = sqlite3.Row
            self.source.backup(c)
            storage.configure_connection_safety(c)
            storage.migrate_database(c, from_version=17, to_version=18)
            self.assertEqual(
                storage.require_schema_compatibility(
                    c, supported_versions=frozenset({18})
                ),
                18,
            )
            self.assertEqual(len(storage._table_names(c)), 59)
            with patch.object(storage, "SCHEMA_SQL", "future SQL"):
                storage._validate_v18_structure(c)
            c.execute("DROP INDEX uq_metric_snapshot_canonical")
            self.assertFalse(storage.schema_compatibility_state(c)["compatible"])

    def test_exact_split_mapping_conserves_old_rows_and_media(self) -> None:
        c = self.candidate()
        before_media = [tuple(r) for r in c.execute("SELECT * FROM evidence_artifacts")]
        plan = storage.migration_v18_plan(self.source)
        self.assertEqual(
            plan["counts"],
            {
                "account_platform_identities": 1,
                "content_items": 1,
                "fetch_slots": 1,
                "provider_raw_responses": 2,
            },
        )
        storage.migrate_database(c, from_version=17, to_version=18)
        proof = storage.validate_v17_v18_lineage(self.source, c)
        self.assertEqual(proof["account_lineage"], plan)
        self.assertEqual(proof["source_table_count"], 56)
        self.assertEqual(proof["candidate_table_count"], 59)
        self.assertEqual(
            c.execute(
                "SELECT account_id FROM account_platform_identities WHERE id=101"
            ).fetchone()[0],
            1001,
        )
        self.assertEqual(
            c.execute("SELECT enabled FROM accounts WHERE id=1001").fetchone()[0], 0
        )
        self.assertEqual(
            c.execute("SELECT account_id FROM content_items WHERE id=4").fetchone()[0],
            None,
        )
        self.assertEqual(
            c.execute("SELECT account_id FROM fetch_slots WHERE id=2").fetchone()[0], 10
        )
        self.assertEqual(
            [tuple(r) for r in c.execute("SELECT * FROM evidence_artifacts")],
            before_media,
        )
        self.assertEqual(
            c.execute("SELECT id FROM content_metric_snapshots").fetchone()[0], 7
        )
        self.assertEqual(c.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 4)
        self.assertFalse(c.execute("PRAGMA foreign_key_check").fetchall())
        changes = c.total_changes
        storage.require_schema_compatibility(c, supported_versions=frozenset({18}))
        self.assertEqual(c.total_changes, changes)

    def test_every_durable_migration_checkpoint_rolls_back(self) -> None:
        for checkpoint in (
            "v18_preflight_complete",
            "v18_accounts_split",
            "v18_references_rekeyed",
            "v18_schema_created",
            "v18_before_commit",
        ):
            with self.subTest(checkpoint=checkpoint):
                c = sqlite3.connect(":memory:")
                c.row_factory = sqlite3.Row
                self.source.backup(c)
                storage.configure_connection_safety(c)
                before = "\n".join(c.iterdump())

                def inject(name: str) -> None:
                    if name == checkpoint:
                        raise RuntimeError("injected")

                with patch.object(storage, "_migration_checkpoint", side_effect=inject):
                    with self.assertRaisesRegex(RuntimeError, "injected"):
                        storage.migrate_database(c, from_version=17, to_version=18)
                self.assertEqual("\n".join(c.iterdump()), before)
                self.assertEqual(c.execute("PRAGMA foreign_keys").fetchone()[0], 1)
                self.assertEqual(
                    c.execute("PRAGMA legacy_alter_table").fetchone()[0], 0
                )
                self.assertEqual(storage.require_schema_compatibility(c), 17)
                c.close()

    def test_snapshot_collision_is_refused_without_merge(self) -> None:
        c = self.candidate()
        c.execute(
            "INSERT INTO content_metric_snapshots(content_id,captured_at,window_key,status,source) "
            "VALUES (1,?,'daily','available','other')",
            (STAMP,),
        )
        c.commit()
        before = "\n".join(c.iterdump())
        with self.assertRaisesRegex(storage.SchemaMigrationError, "snapshot conflict"):
            storage.migrate_database(c, from_version=17, to_version=18)
        self.assertEqual("\n".join(c.iterdump()), before)

    def test_reference_collision_is_refused(self) -> None:
        c = self.candidate()
        c.execute(
            "INSERT INTO account_provider_references(account_identity_id,provider,reference_kind,reference_value,created_at,updated_at) "
            "VALUES (102,'TikHub','sec_user_id','known',?,?)",
            (STAMP, STAMP),
        )
        c.commit()
        with self.assertRaisesRegex(
            storage.SchemaMigrationError, "reference collision"
        ):
            storage.migrate_database(c, from_version=17, to_version=18)
        self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 17)

    def test_conflicting_raw_operation_and_content_are_refused(self) -> None:
        c = self.candidate()
        c.execute(
            "UPDATE provider_raw_responses SET operation='douyin_user_posts' WHERE id=3"
        )
        c.commit()
        with self.assertRaisesRegex(
            storage.SchemaMigrationError, "cannot prove platform"
        ):
            storage.migrate_database(c, from_version=17, to_version=18)

    def test_historical_mixed_slot_keeps_individual_raw_origins_and_attempts(
        self,
    ) -> None:
        c = self.candidate()
        c.execute("UPDATE fetch_attempts SET slot_id=1,attempt_number=2 WHERE id=2")
        c.commit()
        source = sqlite3.connect(":memory:")
        self.addCleanup(source.close)
        source.row_factory = sqlite3.Row
        c.backup(source)
        storage.configure_connection_safety(source)
        attempts = [
            tuple(row) for row in c.execute("SELECT * FROM fetch_attempts ORDER BY id")
        ]
        plan = storage.migration_v18_plan(source)
        self.assertEqual(len(plan["historical_mixed_slots"]), 1)
        self.assertEqual(
            plan["historical_mixed_slots"][0]["slot_platform"], "xiaohongshu"
        )
        storage.migrate_database(c, from_version=17, to_version=18)
        storage.validate_v17_v18_lineage(source, c)
        self.assertEqual(
            c.execute("SELECT account_id FROM fetch_slots WHERE id=1").fetchone()[0],
            1001,
        )
        self.assertEqual(
            c.execute(
                "SELECT account_id FROM provider_raw_responses WHERE id=1"
            ).fetchone()[0],
            1001,
        )
        self.assertEqual(
            c.execute(
                "SELECT account_id FROM provider_raw_responses WHERE id=2"
            ).fetchone()[0],
            10,
        )
        self.assertEqual(
            [
                tuple(row)
                for row in c.execute("SELECT * FROM fetch_attempts ORDER BY id")
            ],
            attempts,
        )

    def test_lineage_rejects_unauthorized_content_or_new_account_change(self) -> None:
        c = self.candidate()
        storage.migrate_database(c, from_version=17, to_version=18)
        c.execute("UPDATE content_items SET title='tamper' WHERE id=1")
        c.commit()
        with self.assertRaisesRegex(storage.SchemaMigrationError, "protected rows"):
            storage.validate_v17_v18_lineage(self.source, c)
        c.execute("UPDATE content_items SET title='' WHERE id=1")
        c.execute("UPDATE accounts SET enabled=1 WHERE id=1001")
        c.commit()
        with self.assertRaisesRegex(
            storage.SchemaMigrationError, "does not preserve parent"
        ):
            storage.validate_v17_v18_lineage(self.source, c)

    def test_nullable_uid_repeated_phone_and_single_platform_constraints(self) -> None:
        c = self.candidate()
        storage.migrate_database(c, from_version=17, to_version=18)
        self.assertEqual(
            c.execute(
                "SELECT COUNT(*) FROM accounts WHERE phone_normalized='13800000010'"
            ).fetchone()[0],
            2,
        )
        c.execute(
            "INSERT INTO account_platform_identities(account_id,platform,uid,created_at,updated_at) "
            "VALUES (30,'douyin',NULL,?,?)",
            (STAMP, STAMP),
        )
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute(
                "INSERT INTO account_platform_identities(account_id,platform,uid,created_at,updated_at) "
                "VALUES (30,'xiaohongshu',NULL,?,?)",
                (STAMP, STAMP),
            )

    def test_new_accepted_facts_are_append_only(self) -> None:
        c = self.candidate()
        storage.migrate_database(c, from_version=17, to_version=18)
        c.execute(
            "INSERT INTO account_roster_snapshots(source_type,scope_key,scope_json,source_instance_id,"
            "source_captured_at,accepted_at,declared_count,member_count,members_sha256,source_sha256,"
            "source_path,contract_version) VALUES ('bootstrap_export','org','{}','export-1',?,?,1,1,?,?,"
            "'roster.json','matrix-roster-v1')",
            (STAMP, STAMP, "a" * 64, "b" * 64),
        )
        c.execute(
            "INSERT INTO account_roster_members(snapshot_id,account_identity_id,platform,matrix_account_id,"
            "profile_ref,monitoring_status,authorization_status) VALUES (1,101,'xiaohongshu','official',"
            "'https://example.com/profile','unknown','unknown')"
        )
        c.execute(
            "INSERT INTO account_metric_observations(account_identity_id,source,captured_at,recorded_at,"
            "contract_version,payload_json,observation_sha256) VALUES (101,'newrank_matrix',?,?,"
            "'account-metrics-v1','{}',?)",
            (STAMP, STAMP, "c" * 64),
        )
        c.commit()
        for table in (
            "account_roster_snapshots",
            "account_roster_members",
            "account_metric_observations",
        ):
            with self.subTest(table=table):
                with self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"):
                    c.execute(f'DELETE FROM "{table}"')
                c.rollback()
        with self.assertRaisesRegex(
            storage.SchemaMigrationError, "bare migration populated"
        ):
            storage.validate_v17_v18_lineage(self.source, c)


if __name__ == "__main__":
    unittest.main()
