"""Paused accounts leave current statistics without erasing archived facts."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from v8 import report_inputs, spu_audience, system_roster
from v8.account_roster import (
    RosterError, SYSTEM_SOURCE_FAMILY, get_current_members, latest_family_snapshot,
    require_active_member,
)
from v8.account_states import set_account_enabled_in_transaction
from v8.metric_observations import persist_metric_observation
from v8.statistics_scope import content_statistics_scope_sql
from v8.storage import connect, initialize_database, transaction


DATA_AT = "2026-07-01T04:00:00Z"
CHANGE_AT = "2026-07-02T04:00:00Z"
CUTOFF_AT = "2026-07-03T04:00:00Z"


class StatisticsScopeTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.db = Path(temporary.name) / "statistics.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 19)
            self.assertNotIn("update_frequency", {row[1] for row in connection.execute("PRAGMA table_info(accounts)")})
            with transaction(connection):
                spu_audience.ensure_assets(connection)
                for account_id, enabled in ((1, 1), (2, 1), (3, 0), (4, 1)):
                    connection.execute(
                        "INSERT INTO accounts(id,phone,enabled,created_at,updated_at) "
                        "VALUES (?,'',?,?,?)",
                        (account_id, enabled, DATA_AT, DATA_AT),
                    )
                    connection.execute(
                        "INSERT INTO account_platform_identities("
                        "id,account_id,platform,uid,created_at,updated_at) "
                        "VALUES (?,?,'douyin',?,?,?)",
                        (account_id, account_id, str(account_id), DATA_AT, DATA_AT),
                    )
                # No roster exists: enabled historical accounts stay eligible.
                for content_id, views in ((1, 100), (2, 200), (3, 900), (4, 50), (5, 25)):
                    self._insert_content(
                        connection, content_id,
                        account_id=content_id if content_id < 5 else None,
                    )
                    persist_metric_observation(
                        connection, content_id=content_id, captured_at=DATA_AT,
                        recorded_at=DATA_AT, window_key="historical", view_count=views,
                        comment_count=None, like_count=None, share_count=None,
                        collect_count=None, status="available", source="legacy",
                        raw_response_id=None, metadata_json="{}",
                        observation_origin="legacy_snapshot_baseline",
                    )
                connection.execute(
                    "INSERT INTO spu_catalog("
                    "spu_id,brand,series,series_slug,is_series_node,created_at,updated_at) "
                    "VALUES ('test-series','测试品牌','测试车系','test-series',1,?,?)",
                    (DATA_AT, DATA_AT),
                )
                for content_id in range(1, 5):
                    connection.execute(
                        "INSERT INTO content_spu_links("
                        "content_id,spu_id,resolved_level,is_primary,status,score,"
                        "rule_version,created_at) "
                        "VALUES (?,'test-series','series',1,'confirmed',90,'test',?)",
                        (content_id, DATA_AT),
                    )
                    connection.execute(
                        "INSERT INTO content_scene_links("
                        "content_id,scene_code,score,rule_version,created_at) "
                        "VALUES (?,'S1',90,'test',?)",
                        (content_id, DATA_AT),
                    )
                    connection.execute(
                        "INSERT INTO content_audience_links("
                        "content_id,audience_code,source,rule_version,created_at) "
                        "VALUES (?,'P1','rule_prior','test',?)",
                        (content_id, DATA_AT),
                    )

    @staticmethod
    def _insert_content(connection, content_id, *, account_id, published_at=DATA_AT):
        connection.execute(
            "INSERT INTO content_items("
            "id,link_id,platform,platform_content_id,canonical_url,account_id,"
            "published_at,imported_at,created_at,updated_at) "
            "VALUES (?,?,'douyin',?,?,?,?,?,?,?)",
            (content_id, f"T{content_id:05d}", str(content_id),
             f"https://www.douyin.com/video/{content_id}", account_id,
             published_at, DATA_AT, DATA_AT, DATA_AT),
        )

    @staticmethod
    def _set_enabled(connection, identity_id, enabled):
        set_account_enabled_in_transaction(
            connection, identity_id, enabled=enabled, effective_at=CHANGE_AT,
            created_at=CHANGE_AT, actor="test", reason="manual account status",
        )

    @staticmethod
    def _freeze(connection, task_id):
        connection.execute(
            "INSERT OR IGNORE INTO report_tasks("
            "id,task_type,name,period_start,period_end,creation_source,task_status,"
            "created_at,updated_at) "
            "VALUES (?,'custom','scope test','2026-07-01','2026-07-01',"
            "'manual','queued',?,?)",
            (task_id, CUTOFF_AT, CUTOFF_AT),
        )
        return report_inputs.freeze_scope(
            connection, {"id": task_id}, start_at="2026-07-01T00:00:00Z",
            end_at="2026-07-02T00:00:00Z", cutoff_at=CUTOFF_AT,
        )

    def _archived_facts(self):
        with connect(self.db, read_only=True) as connection:
            return {
                table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]
                for table in (
                    "content_items", "content_metric_observations", "content_spu_links",
                    "content_scene_links", "content_audience_links",
                )
            }

    def test_scope_keeps_enabled_history_and_unassociated_contents(self):
        with connect(self.db, read_only=True) as connection:
            for alias in ("c", "content", "c_statistics_account"):
                ids = [row[0] for row in connection.execute(
                    f"SELECT {alias}.id FROM content_items {alias} "
                    f"WHERE {content_statistics_scope_sql(alias)} ORDER BY {alias}.id"
                )]
                self.assertEqual(ids, [1, 2, 4, 5])

    def test_scope_rejects_unsafe_aliases(self):
        for alias in ("", "c.id", "c); DROP TABLE accounts;--", 'c"', "a b", "c\n", "x" * 129):
            with self.subTest(alias=alias), self.assertRaises(ValueError):
                content_statistics_scope_sql(alias)

    def test_spu_statistics_and_denominators_restore_without_fact_changes(self):
        archived = self._archived_facts()
        paused = spu_audience.build_stats(db_path=self.db, read_only=True)
        self.assertEqual(paused["totals"], {"posts": 4, "valid_exposure_views": 375})
        self.assertEqual(paused["coverage"]["spu_percentage"], 75.0)
        self.assertEqual(paused["coverage"]["audience_percentage"], 75.0)
        self.assertEqual(paused["coverage"]["scene_percentage"], 75.0)
        classified = next(row for row in paused["spu_rollup"] if row["key"] == "test-series")
        self.assertEqual(classified["posts"], 3)
        self.assertEqual(classified["channels"]["douyin"]["post_share"], 75.0)
        self.assertEqual(sum(row["posts"] for row in paused["detail"]), 4)
        self.assertEqual(sum(row["views"] for row in paused["detail"]), 375)
        self.assertEqual(self._archived_facts(), archived)

        with connect(self.db) as connection, transaction(connection):
            self._set_enabled(connection, 3, True)
        resumed = spu_audience.build_stats(db_path=self.db, read_only=True)
        self.assertEqual(resumed["totals"], {"posts": 5, "valid_exposure_views": 1275})
        self.assertEqual(resumed["coverage"]["spu_percentage"], 80.0)
        self.assertEqual(sum(row["posts"] for row in resumed["detail"]), 5)
        self.assertEqual(sum(row["views"] for row in resumed["detail"]), 1275)
        self.assertEqual(self._archived_facts(), archived)

    def test_report_freeze_survives_pause_and_resume_while_new_scope_changes(self):
        with connect(self.db) as connection, transaction(connection):
            first = self._freeze(connection, "first")
            self.assertEqual(first["payload"]["content_ids"], [1, 2, 4, 5])
            self._set_enabled(connection, 1, False)
            self._set_enabled(connection, 3, True)
            retry = self._freeze(connection, "first")
            self.assertEqual(retry["sha256"], first["sha256"])
            self.assertEqual(retry["payload"], first["payload"])
            self.assertEqual(retry["event_id"], first["event_id"])
            later = self._freeze(connection, "later")
            self.assertEqual(later["payload"]["content_ids"], [2, 3, 4, 5])
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 5)

    def test_report_missing_boundary_audit_uses_the_same_account_scope(self):
        with connect(self.db) as connection, transaction(connection):
            self._insert_content(connection, 6, account_id=3, published_at=None)
            self._insert_content(connection, 7, account_id=None, published_at=None)
            self._freeze(connection, "boundaries")
            audited = [row[0] for row in connection.execute(
                "SELECT content_id FROM task_contents "
                "WHERE task_id='boundaries' AND inclusion_status='excluded_missing_boundary'"
            )]
            self.assertEqual(audited, [7])
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0], 7)

    def test_frozen_report_payload_and_hash_survive_pause(self):
        with connect(self.db) as connection, transaction(connection):
            self._freeze(connection, "frozen-report")
            frozen = report_inputs.store_report(connection, "frozen-report", {
                "metadata": {"revision": 1, "generated_at": CUTOFF_AT},
                "totals": {"content_count": 4, "views": 375},
                "content_ids": [1, 2, 4, 5],
            })
            self._set_enabled(connection, 1, False)
            loaded = report_inputs.frozen_report(connection, "frozen-report")
            self.assertEqual(loaded, frozen)
            later = self._freeze(connection, "fresh-report")
            self.assertEqual(later["payload"]["content_ids"], [2, 4, 5])
        self.assertEqual(spu_audience.build_stats(db_path=self.db, read_only=True)["totals"],
                         {"posts": 3, "valid_exposure_views": 275})

    def test_missing_boundary_created_after_cutoff_is_not_retroactively_audited(self):
        with connect(self.db) as connection, transaction(connection):
            self._insert_content(connection, 6, account_id=1, published_at=None)
            self._insert_content(connection, 7, account_id=1, published_at=None)
            connection.execute("UPDATE content_items SET created_at='2026-07-04T00:00:00Z' WHERE id=7")
            self._freeze(connection, "cutoff-boundary")
            rows = [row[0] for row in connection.execute(
                "SELECT content_id FROM task_contents WHERE task_id='cutoff-boundary' "
                "AND inclusion_status='excluded_missing_boundary' ORDER BY content_id")]
            self.assertEqual(rows, [6])
            self.assertEqual(connection.execute("SELECT count(*) FROM content_items").fetchone()[0], 7)

    def test_pause_blocks_dispatch_from_old_snapshot_and_restore_is_idempotent(self):
        raw_root = self.db.parent / "system-roster"
        archived = self._archived_facts()
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE account_platform_identities SET uid='111111111111' WHERE id=1")
            first = system_roster.upsert_system_members(connection,
                [{"platform": "douyin", "uid": "111111111111", "nickname": "account one"}],
                raw_root=raw_root, actor="test", reason="initial list", sealed_at=DATA_AT)
            original = latest_family_snapshot(connection, SYSTEM_SOURCE_FAMILY)
            assert original is not None
            snapshot_id = int(first["snapshot_id"])
            snapshot_hash = str(original["members_sha256"])
            require_active_member(connection, 1, snapshot_id, snapshot_hash)
            physical_members = [tuple(row) for row in connection.execute(
                "SELECT * FROM account_roster_members WHERE snapshot_id=?", (snapshot_id,))]

            self._set_enabled(connection, 1, False)
            system_roster.remove_system_member(connection, 1, raw_root=raw_root,
                actor="test", reason="paused", sealed_at=CHANGE_AT)
            self.assertEqual(get_current_members(connection, snapshot_id=snapshot_id,
                enabled_only=True, source_family=SYSTEM_SOURCE_FAMILY), [])
            with self.assertRaises(RosterError) as rejected:
                require_active_member(connection, 1, snapshot_id, snapshot_hash)
            self.assertEqual(rejected.exception.code, "member_scope_changed")
            self.assertEqual([tuple(row) for row in connection.execute(
                "SELECT * FROM account_roster_members WHERE snapshot_id=?", (snapshot_id,))], physical_members)

            set_account_enabled_in_transaction(connection, 1, enabled=True,
                effective_at=CUTOFF_AT, created_at=CUTOFF_AT,
                actor="test", reason="restored")
            resumed = system_roster.upsert_system_members(connection,
                [{"platform": "douyin", "uid": "111111111111", "nickname": "account one"}],
                raw_root=raw_root, actor="test", reason="restored", sealed_at=CUTOFF_AT)
            self.assertNotEqual(resumed["snapshot_id"], snapshot_id)
            current = latest_family_snapshot(connection, SYSTEM_SOURCE_FAMILY)
            assert current is not None
            self.assertEqual(current["id"], resumed["snapshot_id"])
            self.assertNotEqual(current["source_sha256"], original["source_sha256"])
            total_snapshots = connection.execute("SELECT count(*) FROM account_roster_snapshots").fetchone()[0]
            duplicate = system_roster.upsert_system_members(connection,
                [{"platform": "douyin", "uid": "111111111111", "nickname": "account one"}],
                raw_root=raw_root, actor="test", reason="repeat restore", sealed_at=CUTOFF_AT)
            self.assertEqual(duplicate["status"], "unchanged")
            self.assertEqual(duplicate["snapshot_id"], resumed["snapshot_id"])
            self.assertEqual(connection.execute("SELECT count(*) FROM account_roster_snapshots").fetchone()[0], total_snapshots)
        self.assertEqual(self._archived_facts(), archived)


if __name__ == "__main__":
    unittest.main()
