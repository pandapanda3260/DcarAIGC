from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from v8.evaluation import evaluate_content
from v8.operations import (
    OperationError,
    import_accounts,
    import_contents,
    upsert_account,
    upsert_content,
)
from v8.storage import connect, initialize_database, now_utc


class V8OperationsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "operations.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
            captured_at = now_utc()
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id, version, status, definition, created_at, published_at
                ) VALUES ('taxonomy', 'selling-points-v5.0', 'published', 'test', ?, ?)
                """,
                (captured_at, captured_at),
            )
            point = connection.execute(
                """
                INSERT INTO selling_points(
                    taxonomy_id, code, tier, label, positive_evidence_json
                ) VALUES ('taxonomy', 'C1', 'core', '汽车服务', '["保养"]')
                """
            )
            connection.execute(
                "INSERT INTO selling_point_scenes(selling_point_id, scene) VALUES (?, 'media')",
                (point.lastrowid,),
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_phone_is_the_account_upsert_key_and_new_import_overwrites(self) -> None:
        created = upsert_account(
            {
                "phone": "13800138000", "operator_name": "张三",
                "account_type": "original", "content_direction": "new_car",
                "platforms": [{"platform": "douyin", "uid": "123456789", "real_name_status": "yes"}],
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            identity_id = connection.execute(
                "SELECT id FROM account_platform_identities"
            ).fetchone()[0]
            connection.execute(
                """
                INSERT INTO account_provider_references(
                    account_identity_id, provider, reference_kind, reference_value,
                    created_at, updated_at
                ) VALUES (?, 'TikHub', 'sec_user_id', 'MS4w.cached', ?, ?)
                """,
                (identity_id, now_utc(), now_utc()),
            )
            connection.commit()
        updated = upsert_account(
            {
                "phone": "+86 138-0013-8000", "operator_name": "李四",
                "account_type": "boutique_ip", "content_direction": "media",
                "platforms": [{"platform": "douyin", "uid": "123456789", "nickname": "车号", "real_name_status": "no"}],
            },
            db_path=self.db,
        )
        self.assertEqual(created["id"], updated["id"])
        self.assertEqual(updated["action"], "updated")
        with connect(self.db) as connection:
            account = connection.execute("SELECT * FROM accounts").fetchone()
            identity = connection.execute("SELECT * FROM account_platform_identities").fetchone()
            reference = connection.execute("SELECT * FROM account_provider_references").fetchone()
        self.assertEqual(account["phone"], "+86 138-0013-8000")
        self.assertEqual(account["operator_name"], "李四")
        self.assertEqual(identity["nickname"], "车号")
        self.assertEqual(identity["real_name_status"], "no")
        self.assertEqual(identity["id"], identity_id)
        self.assertEqual(reference["reference_value"], "MS4w.cached")

    def test_account_import_keeps_only_last_duplicate_row(self) -> None:
        result = import_accounts(
            [
                {"phone": "13800138000", "operator_name": "旧", "platforms": []},
                {"phone": "+8613800138000", "operator_name": "新", "platforms": []},
            ],
            source_name="accounts.csv",
            db_path=self.db,
        )
        self.assertEqual(result, {"batch_id": result["batch_id"], "inserted_rows": 1, "updated_rows": 0, "rejected_rows": 1})
        with connect(self.db) as connection:
            account = connection.execute("SELECT * FROM accounts").fetchone()
            rows = connection.execute("SELECT status FROM import_rows ORDER BY source_row").fetchall()
        self.assertEqual(account["operator_name"], "新")
        self.assertEqual([row[0] for row in rows], ["duplicate_in_file", "inserted"])

    def test_account_identity_claims_pending_uid_and_backfills_content(self) -> None:
        content = upsert_content(
            {
                "platform": "douyin",
                "platform_content_id": "778899",
                "canonical_url": "https://www.douyin.com/video/778899",
                "account_uid": "99887766",
                "account_name": "待归属车号",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            initialize_database(connection)
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM pending_platform_identities"
                ).fetchone()[0],
                1,
            )
        account = upsert_account(
            {
                "phone": "13800138000",
                "platforms": [{"platform": "douyin", "uid": "99887766"}],
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            linked = connection.execute(
                "SELECT account_id FROM content_items WHERE id=?", (content["id"],)
            ).fetchone()[0]
            pending = connection.execute(
                "SELECT COUNT(*) FROM pending_platform_identities"
            ).fetchone()[0]
        self.assertEqual(linked, account["id"])
        self.assertEqual(pending, 0)

    def test_account_export_shape_round_trips_through_flat_csv_import_rows(self) -> None:
        result = import_accounts(
            [{
                "phone": "13900139000", "operator_name": "运营乙",
                "account_type": "mixed_edit", "content_direction": "used_car", "enabled": "1",
                "xiaohongshu_uid": "5c668b3e0000000012021605",
                "xiaohongshu_nickname": "二手车号",
                "xiaohongshu_real_name_status": "unknown",
            }],
            source_name="export-roundtrip.csv",
            db_path=self.db,
        )
        self.assertEqual(result["inserted_rows"], 1)
        self.assertEqual(result["rejected_rows"], 0)
        with connect(self.db) as connection:
            account = connection.execute("SELECT * FROM accounts").fetchone()
            identity = connection.execute("SELECT * FROM account_platform_identities").fetchone()
        self.assertEqual(account["operator_name"], "运营乙")
        self.assertEqual(account["enabled"], 1)
        self.assertEqual(identity["platform"], "xiaohongshu")
        self.assertEqual(identity["uid"], "5c668b3e0000000012021605")

    def test_douyin_and_xhs_require_platform_ids_but_other_platforms_allow_url_fallback(self) -> None:
        with self.assertRaisesRegex(OperationError, "短链需先展开"):
            upsert_content(
                {"platform": "douyin", "canonical_url": "https://v.douyin.com/abc/"},
                db_path=self.db,
            )
        with self.assertRaisesRegex(OperationError, "24 位"):
            upsert_content(
                {"platform": "xiaohongshu", "canonical_url": "https://www.xiaohongshu.com/explore/bad"},
                db_path=self.db,
            )
        first = upsert_content(
            {
                "platform": "wechat_channels", "canonical_url": "https://channels.weixin.qq.com/post/a?x=1",
                "title": "汽车保养方法详解", "body": "汽车保养方法详解完整正文",
            },
            db_path=self.db,
        )
        second = upsert_content(
            {
                "platform": "wechat_channels", "canonical_url": "http://channels.weixin.qq.com/post/a?y=2",
                "title": "新标题",
            },
            db_path=self.db,
        )
        self.assertEqual(first["id"], second["id"])
        with connect(self.db) as connection:
            count = connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0]
        self.assertEqual(count, 1)

    def test_identity_upgrade_merges_two_rows_and_preserves_earliest_link_and_versions(self) -> None:
        fallback = upsert_content(
            {
                "platform": "kuaishou", "canonical_url": "https://www.kuaishou.com/short-video/fallback",
                "title": "汽车保养完整方法", "body": "汽车保养完整方法正文证据",
            },
            db_path=self.db,
        )
        identified = upsert_content(
            {
                "platform": "kuaishou", "platform_content_id": "KS123",
                "canonical_url": "https://www.kuaishou.com/short-video/other",
                "title": "另一条汽车内容", "body": "另一条汽车内容正文证据",
            },
            db_path=self.db,
        )
        first_eval = evaluate_content(fallback["id"], db_path=self.db)
        second_eval = evaluate_content(identified["id"], db_path=self.db)
        with connect(self.db) as connection:
            before_link = connection.execute(
                "SELECT link_id FROM content_items WHERE id=?", (fallback["id"],)
            ).fetchone()[0]
            loser_link = connection.execute(
                "SELECT link_id FROM content_items WHERE id=?", (identified["id"],)
            ).fetchone()[0]
            connection.execute(
                """
                INSERT INTO content_metric_snapshots(
                    content_id, captured_at, window_key, view_count, status, source
                ) VALUES (?, ?, 'day-a', 10, 'available', 'test')
                """,
                (fallback["id"], now_utc()),
            )
            connection.execute(
                """
                INSERT INTO content_metric_snapshots(
                    content_id, captured_at, window_key, view_count, status, source
                ) VALUES (?, ?, 'day-b', 20, 'available', 'test')
                """,
                (identified["id"], now_utc()),
            )
            connection.commit()
        merged = upsert_content(
            {
                "platform": "kuaishou", "platform_content_id": "KS123",
                "canonical_url": "https://www.kuaishou.com/short-video/fallback",
                "title": "合并后的汽车保养内容", "body": "合并后的汽车保养内容正文证据",
            },
            db_path=self.db,
        )
        self.assertEqual(merged["id"], fallback["id"])
        with connect(self.db) as connection:
            contents = connection.execute("SELECT * FROM content_items").fetchall()
            alias = connection.execute("SELECT * FROM content_aliases WHERE alias_link_id=?", (loser_link,)).fetchone()
            snapshots = connection.execute("SELECT * FROM content_metric_snapshots ORDER BY window_key").fetchall()
            evaluations = connection.execute("SELECT * FROM evaluation_versions ORDER BY id").fetchall()
        self.assertEqual(len(contents), 1)
        self.assertEqual(contents[0]["link_id"], before_link)
        self.assertEqual(contents[0]["platform_content_id"], "KS123")
        self.assertEqual(alias["content_id"], fallback["id"])
        self.assertEqual([row["view_count"] for row in snapshots], [10, 20])
        self.assertEqual({row["id"] for row in evaluations}, {first_eval.evaluation_id, second_eval.evaluation_id})
        self.assertTrue(all(row["content_id"] == fallback["id"] for row in evaluations))

    def test_content_import_overwrites_by_platform_identity_and_duplicate_reminder_points_to_earliest(self) -> None:
        rows = [
            {
                "platform": "douyin", "canonical_url": "https://www.douyin.com/video/123456789",
                "published_at": "2026-07-01T08:00:00+08:00",
                "title": "汽车保养知识全集", "body": "这是一段汽车保养知识全集正文",
            },
            {
                "platform": "douyin", "canonical_url": "https://www.douyin.com/video/987654321",
                "published_at": "2026-07-02T08:00:00+08:00",
                "title": "汽车保养知识全集", "body": "这是一段汽车保养知识全集正文",
            },
            {
                "platform": "douyin", "canonical_url": "https://www.douyin.com/video/987654321",
                "published_at": "2026-07-02T08:00:00+08:00",
                "title": "汽车保养知识全集", "body": "这是一段汽车保养知识全集正文",
                "account_name": "覆盖后的昵称",
            },
        ]
        result = import_contents(rows, source_name="contents.csv", db_path=self.db)
        self.assertEqual(result["inserted_rows"], 2)
        self.assertEqual(result["rejected_rows"], 1)
        with connect(self.db) as connection:
            contents = connection.execute("SELECT * FROM content_items ORDER BY published_at").fetchall()
            relation = connection.execute("SELECT * FROM duplicate_relations").fetchone()
        self.assertEqual(len(contents), 2)
        self.assertEqual(contents[1]["raw_account_name"], "覆盖后的昵称")
        self.assertEqual(relation["original_content_id"], contents[0]["id"])
        self.assertEqual(relation["duplicate_content_id"], contents[1]["id"])


if __name__ == "__main__":
    unittest.main()
