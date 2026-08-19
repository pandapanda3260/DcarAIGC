from __future__ import annotations

import csv
import io
import tempfile
import unittest
from pathlib import Path

from v8.operations import (
    CONTENT_CHILD_MERGE_POLICIES,
    IdentityConflictError,
    OperationError,
    export_contents_csv,
    import_accounts,
    import_contents,
    normalize_unknown_content_directions,
    update_content,
    upsert_account,
    upsert_content,
)
from v8.storage import (
    connect,
    ensure_legacy_evaluation_release,
    initialize_database,
    now_utc,
)


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

    def _insert_legacy_evaluation_history(self, content_id: int) -> int:
        """Attach immutable legacy history without invoking the automatic matcher."""

        with connect(self.db) as connection:
            release = ensure_legacy_evaluation_release(
                connection,
                rule_version="evaluation-v7",
                taxonomy_version="selling-points-v5.0",
            )
            evaluated_at = now_utc()
            evaluation = connection.execute(
                """
                INSERT INTO evaluation_versions(
                    content_id,release_id,rule_version,taxonomy_version,
                    matcher_rule_sha256,evidence_sha256,evaluation_source,
                    evaluation_status,evidence_level,selling_point_included,
                    content_direction,payload_json,evaluated_at
                ) VALUES (?,?,?,?,?,?,'migrated_from_v5','evaluated','V1',0,
                          'unknown','{}',?)
                """,
                (
                    content_id,
                    release["id"],
                    release["rule_version"],
                    release["taxonomy_version"],
                    release["matcher_rule_sha256"],
                    f"{content_id:064x}",
                    evaluated_at,
                ),
            )
            connection.commit()
        self.assertIsNotNone(evaluation.lastrowid)
        return int(evaluation.lastrowid)

    def test_contents_csv_neutralizes_spreadsheet_formulas_at_source(self) -> None:
        content = upsert_content(
            {
                "platform": "kuaishou",
                "platform_content_id": "KS-FORMULA",
                "canonical_url": "https://www.kuaishou.com/short-video/KS-FORMULA",
                "title": '=HYPERLINK("https://example.invalid")',
                "body": "公式注入导出测试",
                "account_name": "  @SUM(1,1)",
            },
            db_path=self.db,
        )
        self._insert_legacy_evaluation_history(int(content["id"]))

        rows = list(
            csv.DictReader(
                io.StringIO(
                    export_contents_csv(db_path=self.db).decode("utf-8-sig"),
                    newline="",
                )
            )
        )

        self.assertEqual(rows[0]["title"], "'=HYPERLINK(\"https://example.invalid\")")
        self.assertEqual(rows[0]["account_name"], "'@SUM(1,1)")

    def test_phone_is_the_account_upsert_key_and_new_import_overwrites(self) -> None:
        created = upsert_account(
            {
                "phone": "13800138000",
                "operator_name": "张三",
                "account_type": "original",
                "content_direction": "new_car",
                "platforms": [
                    {
                        "platform": "douyin",
                        "uid": "123456789",
                        "real_name_status": "yes",
                    }
                ],
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
                "phone": "+86 138-0013-8000",
                "operator_name": "李四",
                "account_type": "boutique_ip",
                "content_direction": "media",
                "platforms": [
                    {
                        "platform": "douyin",
                        "uid": "123456789",
                        "nickname": "车号",
                        "real_name_status": "no",
                    }
                ],
            },
            db_path=self.db,
        )
        self.assertEqual(created["id"], updated["id"])
        self.assertEqual(updated["action"], "updated")
        with connect(self.db) as connection:
            account = connection.execute("SELECT * FROM accounts").fetchone()
            identity = connection.execute(
                "SELECT * FROM account_platform_identities"
            ).fetchone()
            reference = connection.execute(
                "SELECT * FROM account_provider_references"
            ).fetchone()
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
        self.assertEqual(
            result,
            {
                "batch_id": result["batch_id"],
                "inserted_rows": 1,
                "updated_rows": 0,
                "rejected_rows": 1,
            },
        )
        with connect(self.db) as connection:
            account = connection.execute("SELECT * FROM accounts").fetchone()
            rows = connection.execute(
                "SELECT status FROM import_rows ORDER BY source_row"
            ).fetchall()
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
            pending = connection.execute(
                "SELECT * FROM pending_platform_identities"
            ).fetchone()
            self.assertIsNotNone(pending)
            self.assertEqual(pending["platform"], "douyin")
            self.assertEqual(pending["uid"], "99887766")
            self.assertEqual(pending["nickname"], "待归属车号")
            self.assertEqual(pending["content_count"], 1)
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

    def test_content_uid_change_clears_stale_account_and_materializes_pending(
        self,
    ) -> None:
        account = upsert_account(
            {
                "phone": "13800138001",
                "platforms": [{"platform": "douyin", "uid": "claimed-uid"}],
            },
            db_path=self.db,
        )
        content = upsert_content(
            {
                "platform": "douyin",
                "platform_content_id": "778900",
                "canonical_url": "https://www.douyin.com/video/778900",
                "account_uid": "claimed-uid",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT account_id FROM content_items WHERE id=?", (content["id"],)
                ).fetchone()[0],
                account["id"],
            )
        upsert_content(
            {
                "platform": "douyin",
                "platform_content_id": "778900",
                "canonical_url": "https://www.douyin.com/video/778900",
                "account_uid": "unclaimed-uid",
                "account_name": "待认领账号",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            row = connection.execute(
                "SELECT account_id,raw_account_uid FROM content_items WHERE id=?",
                (content["id"],),
            ).fetchone()
            pending = connection.execute(
                "SELECT * FROM pending_platform_identities"
            ).fetchone()
        self.assertIsNone(row["account_id"])
        self.assertEqual(row["raw_account_uid"], "unclaimed-uid")
        self.assertEqual(pending["uid"], "unclaimed-uid")
        self.assertEqual(pending["nickname"], "待认领账号")

    def test_removing_account_identity_recreates_pending_assignment(self) -> None:
        account = upsert_account(
            {
                "phone": "13800138002",
                "platforms": [{"platform": "douyin", "uid": "removable-uid"}],
            },
            db_path=self.db,
        )
        content = upsert_content(
            {
                "platform": "douyin",
                "platform_content_id": "778901",
                "canonical_url": "https://www.douyin.com/video/778901",
                "account_uid": "removable-uid",
                "account_name": "解除绑定后待认领",
            },
            db_path=self.db,
        )
        upsert_account(
            {"phone": "13800138002", "platforms": []},
            db_path=self.db,
        )
        with connect(self.db) as connection:
            row = connection.execute(
                "SELECT account_id FROM content_items WHERE id=?", (content["id"],)
            ).fetchone()
            pending = connection.execute(
                "SELECT * FROM pending_platform_identities WHERE uid='removable-uid'"
            ).fetchone()
        self.assertIsNone(row["account_id"])
        self.assertEqual(pending["content_count"], 1)
        self.assertEqual(pending["nickname"], "解除绑定后待认领")
        self.assertEqual(account["action"], "inserted")

    def test_account_export_shape_round_trips_through_flat_csv_import_rows(
        self,
    ) -> None:
        result = import_accounts(
            [
                {
                    "phone": "13900139000",
                    "operator_name": "运营乙",
                    "account_type": "mixed_edit",
                    "content_direction": "used_car",
                    "enabled": "1",
                    "xiaohongshu_uid": "5c668b3e0000000012021605",
                    "xiaohongshu_nickname": "二手车号",
                    "xiaohongshu_real_name_status": "unknown",
                }
            ],
            source_name="export-roundtrip.csv",
            db_path=self.db,
        )
        self.assertEqual(result["inserted_rows"], 1)
        self.assertEqual(result["rejected_rows"], 0)
        with connect(self.db) as connection:
            account = connection.execute("SELECT * FROM accounts").fetchone()
            identity = connection.execute(
                "SELECT * FROM account_platform_identities"
            ).fetchone()
        self.assertEqual(account["operator_name"], "运营乙")
        self.assertEqual(account["enabled"], 1)
        self.assertEqual(identity["platform"], "xiaohongshu")
        self.assertEqual(identity["uid"], "5c668b3e0000000012021605")

    def test_douyin_and_xhs_require_platform_ids_but_other_platforms_allow_url_fallback(
        self,
    ) -> None:
        with self.assertRaisesRegex(OperationError, "短链需先展开"):
            upsert_content(
                {"platform": "douyin", "canonical_url": "https://v.douyin.com/abc/"},
                db_path=self.db,
            )
        with self.assertRaisesRegex(OperationError, "24 位"):
            upsert_content(
                {
                    "platform": "xiaohongshu",
                    "canonical_url": "https://www.xiaohongshu.com/explore/bad",
                },
                db_path=self.db,
            )
        first = upsert_content(
            {
                "platform": "wechat_channels",
                "canonical_url": "https://channels.weixin.qq.com/post/a?x=1",
                "title": "汽车保养方法详解",
                "body": "汽车保养方法详解完整正文",
            },
            db_path=self.db,
        )
        second = upsert_content(
            {
                "platform": "wechat_channels",
                "canonical_url": "http://channels.weixin.qq.com/post/a?y=2",
                "title": "新标题",
            },
            db_path=self.db,
        )
        self.assertEqual(first["id"], second["id"])
        with connect(self.db) as connection:
            count = connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[
                0
            ]
        self.assertEqual(count, 1)

    def test_partial_content_update_preserves_omitted_fields_and_can_clear_identity_text(
        self,
    ) -> None:
        created = upsert_content(
            {
                "platform": "kuaishou",
                "platform_content_id": "KS-PATCH",
                "canonical_url": "https://www.kuaishou.com/short-video/patch",
                "published_at": "2026-08-03T08:00:00Z",
                "title": "原始标题",
                "body": "原始正文",
                "content_type": "video",
                "account_uid": "patch-uid",
                "account_name": "原始昵称",
                "account_type": "original",
                "content_direction": "media",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            before = dict(
                connection.execute(
                    "SELECT * FROM content_items WHERE id=?", (created["id"],)
                ).fetchone()
            )

        result = update_content(
            int(created["id"]), {"title": "仅修改标题"}, db_path=self.db
        )
        self.assertEqual(result, {"id": created["id"], "action": "updated"})
        with connect(self.db) as connection:
            after = dict(
                connection.execute(
                    "SELECT * FROM content_items WHERE id=?", (created["id"],)
                ).fetchone()
            )
        self.assertEqual(after["title"], "仅修改标题")
        for field in (
            "platform",
            "platform_content_id",
            "canonical_url",
            "normalized_url_hash",
            "raw_account_uid",
            "raw_account_name",
            "legacy_account_type",
            "body",
            "content_type",
            "published_at",
            "published_at_raw",
            "manual_content_direction",
        ):
            self.assertEqual(after[field], before[field], field)

        update_content(
            int(created["id"]),
            {"account_uid": "", "account_name": ""},
            db_path=self.db,
        )
        with connect(self.db) as connection:
            cleared = connection.execute(
                "SELECT raw_account_uid,raw_account_name FROM content_items WHERE id=?",
                (created["id"],),
            ).fetchone()
        self.assertIsNone(cleared["raw_account_uid"])
        self.assertIsNone(cleared["raw_account_name"])

    def test_non_identity_patch_preserves_legacy_account_assignment_and_pending_row(
        self,
    ) -> None:
        account = upsert_account(
            {
                "phone": "13800138111",
                "platforms": [{"platform": "douyin", "uid": "claimed-other-uid"}],
            },
            db_path=self.db,
        )
        content = upsert_content(
            {
                "platform": "douyin",
                "platform_content_id": "778911",
                "canonical_url": "https://www.douyin.com/video/778911",
                "account_uid": "legacy-unclaimed-uid",
                "account_name": "迁移待认领账号",
                "title": "原始迁移标题",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET account_id=? WHERE id=?",
                (account["id"], content["id"]),
            )
            connection.commit()
            pending_before = dict(
                connection.execute(
                    """
                    SELECT * FROM pending_platform_identities
                    WHERE platform='douyin' AND uid='legacy-unclaimed-uid'
                    """
                ).fetchone()
            )

        update_content(int(content["id"]), {"title": "只修改迁移标题"}, db_path=self.db)

        with connect(self.db) as connection:
            linked_account = connection.execute(
                "SELECT account_id FROM content_items WHERE id=?", (content["id"],)
            ).fetchone()[0]
            pending_after = dict(
                connection.execute(
                    """
                    SELECT * FROM pending_platform_identities
                    WHERE platform='douyin' AND uid='legacy-unclaimed-uid'
                    """
                ).fetchone()
            )
        self.assertEqual(linked_account, account["id"])
        self.assertEqual(pending_after, pending_before)

    def test_title_patch_removes_stale_relation_when_content_was_original(self) -> None:
        common = {
            "title": "完全相同的汽车保养知识标题",
            "body": "完全相同的汽车保养知识正文内容",
            "content_type": "video",
        }
        original = upsert_content(
            {
                **common,
                "platform": "kuaishou",
                "platform_content_id": "KS-DUP-ORIGINAL",
                "canonical_url": "https://www.kuaishou.com/short-video/dup-original",
                "published_at": "2026-08-01T00:00:00Z",
            },
            db_path=self.db,
        )
        duplicate = upsert_content(
            {
                **common,
                "platform": "kuaishou",
                "platform_content_id": "KS-DUP-SECOND",
                "canonical_url": "https://www.kuaishou.com/short-video/dup-second",
                "published_at": "2026-08-02T00:00:00Z",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            relation_before = connection.execute(
                "SELECT * FROM duplicate_relations WHERE method='text_sha256'"
            ).fetchone()
        self.assertEqual(relation_before["original_content_id"], original["id"])
        self.assertEqual(relation_before["duplicate_content_id"], duplicate["id"])

        update_content(
            int(original["id"]), {"title": "已经变更为唯一标题"}, db_path=self.db
        )

        with connect(self.db) as connection:
            relation_count = connection.execute(
                "SELECT COUNT(*) FROM duplicate_relations WHERE method='text_sha256'"
            ).fetchone()[0]
        self.assertEqual(relation_count, 0)

    def test_published_at_patch_reorders_text_duplicate_original(self) -> None:
        common = {
            "title": "发布日期重排汽车保养知识标题",
            "body": "发布日期重排汽车保养知识正文内容",
            "content_type": "video",
        }
        first = upsert_content(
            {
                **common,
                "platform": "kuaishou",
                "platform_content_id": "KS-DATE-FIRST",
                "canonical_url": "https://www.kuaishou.com/short-video/date-first",
                "published_at": "2026-08-02T00:00:00Z",
            },
            db_path=self.db,
        )
        second = upsert_content(
            {
                **common,
                "platform": "kuaishou",
                "platform_content_id": "KS-DATE-SECOND",
                "canonical_url": "https://www.kuaishou.com/short-video/date-second",
                "published_at": "2026-08-03T00:00:00Z",
            },
            db_path=self.db,
        )

        update_content(
            int(second["id"]),
            {"published_at": "2026-08-01T00:00:00Z"},
            db_path=self.db,
        )

        with connect(self.db) as connection:
            relation = connection.execute(
                "SELECT * FROM duplicate_relations WHERE method='text_sha256'"
            ).fetchone()
        self.assertEqual(relation["original_content_id"], second["id"])
        self.assertEqual(relation["duplicate_content_id"], first["id"])

    def test_identity_patch_with_two_protected_histories_fails_closed(self) -> None:
        first_id = "1" * 24
        second_id = "2" * 24
        first_url = f"https://www.xiaohongshu.com/explore/{first_id}"
        second_url = f"https://www.xiaohongshu.com/explore/{second_id}"
        first = upsert_content(
            {
                "platform": "xiaohongshu",
                "platform_content_id": first_id,
                "canonical_url": first_url,
                "title": "人工修改冲突第一条汽车保养内容",
                "body": "人工修改冲突第一条汽车保养正文证据",
            },
            db_path=self.db,
        )
        second = upsert_content(
            {
                "platform": "xiaohongshu",
                "platform_content_id": second_id,
                "canonical_url": second_url,
                "title": "人工修改冲突第二条汽车保养内容",
                "body": "人工修改冲突第二条汽车保养正文证据",
            },
            db_path=self.db,
        )
        evaluation_ids = {
            self._insert_legacy_evaluation_history(int(first["id"])),
            self._insert_legacy_evaluation_history(int(second["id"])),
        }
        with connect(self.db) as connection:
            contents_before = [
                tuple(row)
                for row in connection.execute(
                    """
                    SELECT id,platform_content_id,canonical_url,title,body,updated_at
                    FROM content_items WHERE id IN (?,?) ORDER BY id
                    """,
                    (first["id"], second["id"]),
                )
            ]

        with self.assertRaisesRegex(IdentityConflictError, "identity_conflict"):
            update_content(
                int(first["id"]),
                {"platform_content_id": second_id},
                db_path=self.db,
            )

        with connect(self.db) as connection:
            contents_after = [
                tuple(row)
                for row in connection.execute(
                    """
                    SELECT id,platform_content_id,canonical_url,title,body,updated_at
                    FROM content_items WHERE id IN (?,?) ORDER BY id
                    """,
                    (first["id"], second["id"]),
                )
            ]
            evaluations = connection.execute(
                "SELECT id,content_id FROM evaluation_versions WHERE id IN (?,?)",
                tuple(sorted(evaluation_ids)),
            ).fetchall()
            relations = connection.execute(
                """
                SELECT * FROM duplicate_relations
                WHERE method='identity_conflict' AND status='pending_review'
                """
            ).fetchall()
            violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        self.assertEqual(contents_after, contents_before)
        self.assertEqual({row["id"] for row in evaluations}, evaluation_ids)
        self.assertEqual(len(relations), 1)
        self.assertEqual(violations, [])

    def test_three_way_identity_patch_rejects_before_any_partial_merge(self) -> None:
        no_history = upsert_content(
            {
                "platform": "kuaishou",
                "platform_content_id": "KS-THREE-WAY-ID",
                "canonical_url": "https://www.kuaishou.com/short-video/no-history",
                "title": "没有历史但带有子表记录的内容",
                "body": "没有历史但带有子表记录的完整正文",
            },
            db_path=self.db,
        )
        url_history = upsert_content(
            {
                "platform": "kuaishou",
                "platform_content_id": "KS-URL-HISTORY",
                "canonical_url": "https://www.kuaishou.com/short-video/url-history",
                "title": "链接命中的保护历史内容",
                "body": "链接命中的保护历史内容完整正文",
            },
            db_path=self.db,
        )
        target = upsert_content(
            {
                "platform": "kuaishou",
                "platform_content_id": "KS-TARGET-HISTORY",
                "canonical_url": "https://www.kuaishou.com/short-video/target-history",
                "title": "正在修改的保护历史内容",
                "body": "正在修改的保护历史内容完整正文",
            },
            db_path=self.db,
        )
        evaluation_ids = {
            self._insert_legacy_evaluation_history(int(url_history["id"])),
            self._insert_legacy_evaluation_history(int(target["id"])),
        }
        with connect(self.db) as connection:
            captured_at = now_utc()
            connection.execute(
                """
                INSERT INTO fetch_slots(
                    content_id,stage,window_key,provider,adapter_version,status,
                    attempt_count,created_at,updated_at
                ) VALUES (?, 'detail', 'lifetime', 'test', 'test-v1',
                          'pending', 0, ?, ?)
                """,
                (no_history["id"], captured_at, captured_at),
            )
            connection.commit()
            contents_before = [
                tuple(row)
                for row in connection.execute("SELECT * FROM content_items ORDER BY id")
            ]
            aliases_before = [
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM content_aliases ORDER BY alias_link_id"
                )
            ]
            identities_before = [
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM content_identities ORDER BY id"
                )
            ]
            slots_before = [
                tuple(row)
                for row in connection.execute("SELECT * FROM fetch_slots ORDER BY id")
            ]

        with self.assertRaisesRegex(IdentityConflictError, "identity_conflict"):
            update_content(
                int(target["id"]),
                {
                    "platform_content_id": "KS-THREE-WAY-ID",
                    "canonical_url": "https://www.kuaishou.com/short-video/url-history",
                },
                db_path=self.db,
            )

        with connect(self.db) as connection:
            contents_after = [
                tuple(row)
                for row in connection.execute("SELECT * FROM content_items ORDER BY id")
            ]
            aliases_after = [
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM content_aliases ORDER BY alias_link_id"
                )
            ]
            identities_after = [
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM content_identities ORDER BY id"
                )
            ]
            slots_after = [
                tuple(row)
                for row in connection.execute("SELECT * FROM fetch_slots ORDER BY id")
            ]
            evaluations = connection.execute(
                "SELECT id,content_id FROM evaluation_versions WHERE id IN (?,?)",
                tuple(sorted(evaluation_ids)),
            ).fetchall()
            relations = connection.execute(
                """
                SELECT * FROM duplicate_relations
                WHERE method='identity_conflict' AND status='pending_review'
                ORDER BY id
                """
            ).fetchall()
            violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        self.assertEqual(contents_after, contents_before)
        self.assertEqual(aliases_after, aliases_before)
        self.assertEqual(identities_after, identities_before)
        self.assertEqual(slots_after, slots_before)
        self.assertEqual({row["id"] for row in evaluations}, evaluation_ids)
        self.assertEqual(len(relations), 2)
        self.assertEqual(violations, [])

    def test_identity_only_merge_rebuilds_text_groups_and_pending_identities(
        self,
    ) -> None:
        group_a = {
            "title": "身份合并文本分组甲的汽车内容",
            "body": "身份合并文本分组甲的汽车完整正文",
        }
        group_b = {
            "title": "身份合并文本分组乙的新车内容",
            "body": "身份合并文本分组乙的新车完整正文",
        }
        target = upsert_content(
            {
                **group_a,
                "platform": "kuaishou",
                "platform_content_id": "KS-MERGE-TARGET",
                "canonical_url": "https://www.kuaishou.com/short-video/merge-target",
                "account_uid": "merge-pending-a",
                "account_name": "待认领甲",
            },
            db_path=self.db,
        )
        group_a_duplicate = upsert_content(
            {
                **group_a,
                "platform": "kuaishou",
                "platform_content_id": "KS-MERGE-A-DUP",
                "canonical_url": "https://www.kuaishou.com/short-video/merge-a-dup",
            },
            db_path=self.db,
        )
        candidate = upsert_content(
            {
                **group_b,
                "platform": "kuaishou",
                "platform_content_id": "KS-MERGE-CANDIDATE",
                "canonical_url": "https://www.kuaishou.com/short-video/merge-candidate",
                "account_uid": "merge-pending-b",
                "account_name": "待认领乙",
            },
            db_path=self.db,
        )
        group_b_duplicate = upsert_content(
            {
                **group_b,
                "platform": "kuaishou",
                "platform_content_id": "KS-MERGE-B-DUP",
                "canonical_url": "https://www.kuaishou.com/short-video/merge-b-dup",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            relations_before = connection.execute(
                """
                SELECT duplicate_content_id,original_content_id
                FROM duplicate_relations WHERE method='text_sha256'
                ORDER BY duplicate_content_id
                """
            ).fetchall()
            pending_before = connection.execute(
                """
                SELECT uid,content_count FROM pending_platform_identities
                WHERE uid IN ('merge-pending-a','merge-pending-b') ORDER BY uid
                """
            ).fetchall()
        self.assertEqual(
            [tuple(row) for row in relations_before],
            [
                (group_a_duplicate["id"], target["id"]),
                (group_b_duplicate["id"], candidate["id"]),
            ],
        )
        self.assertEqual(
            [tuple(row) for row in pending_before],
            [("merge-pending-a", 1), ("merge-pending-b", 1)],
        )

        result = update_content(
            int(target["id"]),
            {"platform_content_id": "KS-MERGE-CANDIDATE"},
            db_path=self.db,
        )
        self.assertEqual(result["id"], target["id"])

        with connect(self.db) as connection:
            content_ids = [
                row[0]
                for row in connection.execute(
                    "SELECT id FROM content_items ORDER BY id"
                )
            ]
            relations_after = connection.execute(
                """
                SELECT duplicate_content_id,original_content_id
                FROM duplicate_relations WHERE method='text_sha256'
                ORDER BY duplicate_content_id
                """
            ).fetchall()
            pending_after = connection.execute(
                """
                SELECT uid,content_count FROM pending_platform_identities
                WHERE uid IN ('merge-pending-a','merge-pending-b') ORDER BY uid
                """
            ).fetchall()
            violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        self.assertEqual(
            content_ids,
            [target["id"], group_a_duplicate["id"], group_b_duplicate["id"]],
        )
        self.assertEqual(
            [tuple(row) for row in relations_after],
            [(group_a_duplicate["id"], target["id"])],
        )
        self.assertEqual(
            [tuple(row) for row in pending_after], [("merge-pending-a", 1)]
        )
        self.assertEqual(violations, [])

    def test_content_import_normalizes_unknown_and_preserves_operator_fields(
        self,
    ) -> None:
        identity = {
            "platform": "kuaishou",
            "platform_content_id": "KS-IMPORT-PRESERVE",
            "canonical_url": "https://www.kuaishou.com/short-video/import-preserve",
        }
        created = upsert_content(identity, db_path=self.db)
        with connect(self.db) as connection:
            initial = connection.execute(
                """
                SELECT manual_content_direction,legacy_account_type,content_type
                FROM content_items WHERE id=?
                """,
                (created["id"],),
            ).fetchone()
        self.assertIsNone(initial["manual_content_direction"])
        self.assertEqual(initial["legacy_account_type"], "unknown")
        self.assertEqual(initial["content_type"], "unknown")

        update_content(
            int(created["id"]),
            {
                "content_direction": "media",
                "account_type": "original",
                "content_type": "video",
            },
            db_path=self.db,
        )
        for incoming in (
            {},
            {
                "content_direction": "",
                "account_type": "",
                "content_type": "",
            },
            {
                "content_direction": "unknown",
                "account_type": "unknown",
                "content_type": "unknown",
            },
        ):
            upsert_content({**identity, **incoming}, db_path=self.db)
            with connect(self.db) as connection:
                preserved = connection.execute(
                    """
                    SELECT manual_content_direction,legacy_account_type,content_type
                    FROM content_items WHERE id=?
                    """,
                    (created["id"],),
                ).fetchone()
            self.assertEqual(preserved["manual_content_direction"], "media")
            self.assertEqual(preserved["legacy_account_type"], "original")
            self.assertEqual(preserved["content_type"], "video")

        upsert_content(
            {
                **identity,
                "content_direction": "used_car",
                "account_type": "mixed_edit",
                "content_type": "image",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            explicit = connection.execute(
                """
                SELECT manual_content_direction,legacy_account_type,content_type
                FROM content_items WHERE id=?
                """,
                (created["id"],),
            ).fetchone()
        self.assertEqual(explicit["manual_content_direction"], "used_car")
        self.assertEqual(explicit["legacy_account_type"], "mixed_edit")
        self.assertEqual(explicit["content_type"], "image")

    def test_unknown_direction_data_normalization_is_idempotent(self) -> None:
        first = upsert_content(
            {
                "platform": "kuaishou",
                "canonical_url": "https://www.kuaishou.com/short-video/normalize-1",
            },
            db_path=self.db,
        )
        second = upsert_content(
            {
                "platform": "kuaishou",
                "canonical_url": "https://www.kuaishou.com/short-video/normalize-2",
                "content_direction": "media",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET manual_content_direction='unknown' WHERE id=?",
                (first["id"],),
            )
            connection.commit()
        normalized = normalize_unknown_content_directions(db_path=self.db)
        repeated = normalize_unknown_content_directions(db_path=self.db)
        self.assertEqual(normalized["updated_rows"], 1)
        self.assertEqual(repeated["updated_rows"], 0)
        self.assertEqual(normalized["total_rows"], 2)
        self.assertEqual(normalized["before"].get("unknown"), 1)
        self.assertEqual(normalized["after"].get("unknown", 0), 0)
        self.assertEqual(normalized["after"].get("media"), 1)
        self.assertEqual(normalized["after"].get("null"), 1)
        with connect(self.db) as connection:
            rows = connection.execute(
                "SELECT id,manual_content_direction FROM content_items ORDER BY id"
            ).fetchall()
        self.assertEqual(
            [(row["id"], row["manual_content_direction"]) for row in rows],
            [(first["id"], None), (second["id"], "media")],
        )

    def test_identity_upgrade_with_two_evaluation_histories_fails_closed(
        self,
    ) -> None:
        fallback = upsert_content(
            {
                "platform": "kuaishou",
                "canonical_url": "https://www.kuaishou.com/short-video/fallback",
                "title": "汽车保养完整方法",
                "body": "汽车保养完整方法正文证据",
            },
            db_path=self.db,
        )
        identified = upsert_content(
            {
                "platform": "kuaishou",
                "platform_content_id": "KS123",
                "canonical_url": "https://www.kuaishou.com/short-video/other",
                "title": "另一条汽车内容",
                "body": "另一条汽车内容正文证据",
            },
            db_path=self.db,
        )
        first_evaluation_id = self._insert_legacy_evaluation_history(fallback["id"])
        second_evaluation_id = self._insert_legacy_evaluation_history(identified["id"])
        with connect(self.db) as connection:
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
        merge_value = {
            "platform": "kuaishou",
            "platform_content_id": "KS123",
            "canonical_url": "https://www.kuaishou.com/short-video/fallback",
            "title": "合并后的汽车保养内容",
            "body": "合并后的汽车保养内容正文证据",
        }
        for _ in range(2):
            with self.assertRaisesRegex(IdentityConflictError, "identity_conflict"):
                upsert_content(merge_value, db_path=self.db)
        with connect(self.db) as connection:
            contents = connection.execute("SELECT * FROM content_items").fetchall()
            snapshots = connection.execute(
                "SELECT * FROM content_metric_snapshots ORDER BY window_key"
            ).fetchall()
            evaluations = connection.execute(
                "SELECT * FROM evaluation_versions ORDER BY id"
            ).fetchall()
            relations = connection.execute(
                "SELECT * FROM duplicate_relations WHERE method='identity_conflict'"
            ).fetchall()
            aliases = connection.execute("SELECT * FROM content_aliases").fetchall()
            violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        self.assertEqual(len(contents), 2)
        self.assertEqual(aliases, [])
        self.assertEqual([row["view_count"] for row in snapshots], [10, 20])
        self.assertEqual(
            {row["id"] for row in evaluations},
            {first_evaluation_id, second_evaluation_id},
        )
        self.assertEqual(
            {row["content_id"] for row in evaluations},
            {fallback["id"], identified["id"]},
        )
        self.assertEqual(len(relations), 1)
        self.assertEqual(relations[0]["status"], "pending_review")
        self.assertIn(
            "both_contents_have_protected_history", relations[0]["evidence_json"]
        )
        self.assertEqual(violations, [])

    def test_identity_upgrade_without_protected_history_keeps_earliest_link(
        self,
    ) -> None:
        fallback = upsert_content(
            {
                "platform": "kuaishou",
                "canonical_url": "https://www.kuaishou.com/short-video/no-history",
            },
            db_path=self.db,
        )
        identified = upsert_content(
            {
                "platform": "kuaishou",
                "platform_content_id": "KS-NO-HISTORY",
                "canonical_url": "https://www.kuaishou.com/short-video/identified",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            earliest_link = connection.execute(
                "SELECT link_id FROM content_items WHERE id=?", (fallback["id"],)
            ).fetchone()[0]
            loser_link = connection.execute(
                "SELECT link_id FROM content_items WHERE id=?", (identified["id"],)
            ).fetchone()[0]
            connection.execute(
                """
                INSERT INTO content_aliases(alias_link_id,content_id,reason,created_at)
                VALUES ('OLD001',?,'previous_merge',?)
                """,
                (identified["id"], now_utc()),
            )
            connection.execute(
                """
                INSERT INTO duplicate_fingerprints(
                    content_id,fingerprint_version,source_sha256,payload_json,created_at
                ) VALUES (?,'fingerprint-v1','source-loser','{}',?)
                """,
                (identified["id"], now_utc()),
            )
            connection.commit()
        merged = upsert_content(
            {
                "platform": "kuaishou",
                "platform_content_id": "KS-NO-HISTORY",
                "canonical_url": "https://www.kuaishou.com/short-video/no-history",
            },
            db_path=self.db,
        )
        self.assertEqual(merged["id"], fallback["id"])
        with connect(self.db) as connection:
            content = connection.execute("SELECT * FROM content_items").fetchone()
            aliases = connection.execute(
                "SELECT * FROM content_aliases ORDER BY alias_link_id"
            ).fetchall()
            fingerprints = connection.execute(
                "SELECT * FROM duplicate_fingerprints"
            ).fetchall()
            violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        self.assertEqual(content["link_id"], earliest_link)
        self.assertEqual(content["platform_content_id"], "KS-NO-HISTORY")
        self.assertEqual(
            {(row["alias_link_id"], row["content_id"]) for row in aliases},
            {("OLD001", fallback["id"]), (loser_link, fallback["id"])},
        )
        self.assertEqual(len(fingerprints), 1)
        self.assertEqual(fingerprints[0]["content_id"], fallback["id"])
        self.assertEqual(violations, [])

    def test_pending_duplicate_relation_prevents_automatic_identity_merge(self) -> None:
        first = upsert_content(
            {
                "platform": "kuaishou",
                "canonical_url": "https://www.kuaishou.com/short-video/pending-a",
            },
            db_path=self.db,
        )
        second = upsert_content(
            {
                "platform": "kuaishou",
                "platform_content_id": "KS-PENDING",
                "canonical_url": "https://www.kuaishou.com/short-video/pending-b",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO duplicate_relations(
                    duplicate_content_id,original_content_id,method,confidence,
                    evidence_json,status,created_at
                ) VALUES (?,?,'fingerprint_v1',0.75,'{}','pending_review',?)
                """,
                (second["id"], first["id"], now_utc()),
            )
            connection.commit()
        with self.assertRaisesRegex(IdentityConflictError, "identity_conflict"):
            upsert_content(
                {
                    "platform": "kuaishou",
                    "platform_content_id": "KS-PENDING",
                    "canonical_url": "https://www.kuaishou.com/short-video/pending-a",
                },
                db_path=self.db,
            )
        with connect(self.db) as connection:
            content_count = connection.execute(
                "SELECT COUNT(*) FROM content_items"
            ).fetchone()[0]
            relations = connection.execute(
                "SELECT method,status FROM duplicate_relations ORDER BY method"
            ).fetchall()
        self.assertEqual(content_count, 2)
        self.assertIn(
            ("fingerprint_v1", "pending_review"), [tuple(r) for r in relations]
        )
        self.assertIn(
            ("identity_conflict", "pending_review"), [tuple(r) for r in relations]
        )

    def test_every_content_foreign_key_has_an_identity_merge_policy(self) -> None:
        with connect(self.db) as connection:
            tables = [
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                )
            ]
            referencing = {
                table
                for table in tables
                if any(
                    str(foreign_key[2]) == "content_items"
                    for foreign_key in connection.execute(
                        f'PRAGMA foreign_key_list("{table}")'
                    )
                )
            }
        self.assertEqual(referencing, set(CONTENT_CHILD_MERGE_POLICIES))

    def test_identity_upgrade_keeps_the_only_protected_history_side(self) -> None:
        upsert_content(
            {
                "platform": "kuaishou",
                "canonical_url": "https://www.kuaishou.com/short-video/older",
            },
            db_path=self.db,
        )
        protected = upsert_content(
            {
                "platform": "kuaishou",
                "platform_content_id": "KS-PROTECTED",
                "canonical_url": "https://www.kuaishou.com/short-video/newer",
                "title": "汽车保养完整方法",
                "body": "汽车保养完整方法正文证据",
            },
            db_path=self.db,
        )
        evaluation_id = self._insert_legacy_evaluation_history(protected["id"])
        with connect(self.db) as connection:
            connection.execute(
                """
                CREATE TRIGGER forbid_evaluation_delete
                BEFORE DELETE ON evaluation_versions
                BEGIN SELECT RAISE(ABORT, 'evaluation delete forbidden'); END
                """
            )
            connection.commit()
        merged = upsert_content(
            {
                "platform": "kuaishou",
                "platform_content_id": "KS-PROTECTED",
                "canonical_url": "https://www.kuaishou.com/short-video/older",
            },
            db_path=self.db,
        )
        self.assertEqual(merged["id"], protected["id"])
        with connect(self.db) as connection:
            contents = connection.execute("SELECT id FROM content_items").fetchall()
            evaluation_row = connection.execute(
                "SELECT * FROM evaluation_versions WHERE id=?",
                (evaluation_id,),
            ).fetchone()
            violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        self.assertEqual([row["id"] for row in contents], [protected["id"]])
        self.assertEqual(evaluation_row["content_id"], protected["id"])
        self.assertEqual(violations, [])

    def test_content_import_overwrites_by_platform_identity_and_duplicate_reminder_points_to_earliest(
        self,
    ) -> None:
        rows = [
            {
                "platform": "douyin",
                "canonical_url": "https://www.douyin.com/video/123456789",
                "published_at": "2026-07-01T08:00:00+08:00",
                "title": "汽车保养知识全集",
                "body": "这是一段汽车保养知识全集正文",
            },
            {
                "platform": "douyin",
                "canonical_url": "https://www.douyin.com/video/987654321",
                "published_at": "2026-07-02T08:00:00+08:00",
                "title": "汽车保养知识全集",
                "body": "这是一段汽车保养知识全集正文",
            },
            {
                "platform": "douyin",
                "canonical_url": "https://www.douyin.com/video/987654321",
                "published_at": "2026-07-02T08:00:00+08:00",
                "title": "汽车保养知识全集",
                "body": "这是一段汽车保养知识全集正文",
                "account_name": "覆盖后的昵称",
            },
        ]
        result = import_contents(rows, source_name="contents.csv", db_path=self.db)
        self.assertEqual(result["inserted_rows"], 2)
        self.assertEqual(result["rejected_rows"], 1)
        with connect(self.db) as connection:
            contents = connection.execute(
                "SELECT * FROM content_items ORDER BY published_at"
            ).fetchall()
            relation = connection.execute(
                "SELECT * FROM duplicate_relations"
            ).fetchone()
        self.assertEqual(len(contents), 2)
        self.assertEqual(contents[1]["raw_account_name"], "覆盖后的昵称")
        self.assertEqual(relation["original_content_id"], contents[0]["id"])
        self.assertEqual(relation["duplicate_content_id"], contents[1]["id"])

    def test_content_import_cannot_set_internal_history_scope(self) -> None:
        result = import_contents(
            [
                {
                    "platform": "douyin",
                    "canonical_url": "https://www.douyin.com/video/1122334455",
                    "title": "普通导入内容",
                    "source_group": "history-archive",
                }
            ],
            source_name="contents.csv",
            db_path=self.db,
        )
        self.assertEqual(result["inserted_rows"], 1)
        with connect(self.db) as connection:
            row = connection.execute(
                "SELECT source_group FROM content_items WHERE platform_content_id=?",
                ("1122334455",),
            ).fetchone()
        self.assertEqual(row["source_group"], "")

        with self.assertRaisesRegex(OperationError, "内部来源分组无效"):
            upsert_content(
                {
                    "platform": "douyin",
                    "canonical_url": "https://www.douyin.com/video/1122334466",
                    "title": "非法内部标签",
                },
                db_path=self.db,
                source_group_on_insert="manual-import",
            )


if __name__ == "__main__":
    unittest.main()
