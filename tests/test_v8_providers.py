from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

from v8.capture import CaptureError, ProviderResult
from v8.operations import upsert_account, upsert_content
from v8.providers import discover_account_content, update_content_data
from v8.storage import connect, initialize_database, now_utc


class V8ProviderUpdateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "providers.sqlite3"
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
        content = upsert_content(
            {
                "platform": "douyin", "canonical_url": "https://www.douyin.com/video/123456789",
                "title": "待补详情", "content_type": "video",
            },
            db_path=self.db,
        )
        self.content_id = content["id"]

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def successful_call(stage, content):
        if stage == "detail":
            data = {
                "title": "汽车保养完整内容", "body": "汽车保养维修完整正文",
                "published_at": "2026-08-01T04:00:00Z", "account_uid": "99887766",
                "account_name": "汽车号", "content_type": "video",
            }
        elif stage == "metrics":
            data = {
                "view_count": 1000, "comment_count": None, "like_count": 50,
                "share_count": 5, "collect_count": None,
            }
        else:
            data = {
                "comment_count": 1,
                "comments": [{
                    "platform_comment_id": "c1", "anonymous_user_key": "U" + "a" * 64,
                    "body": "这款车保养多少钱", "published_at": "2026-08-02T00:00:00Z",
                    "like_count": 3, "parent_comment_id": None,
                }],
            }
        return ProviderResult(data=data, raw_response={"stage": stage, "data": data}, http_status=200, billed=True)

    def test_one_row_update_obeys_lifetime_daily_weekly_slots_and_records_costs(self) -> None:
        first = update_content_data(
            self.content_id, db_path=self.db, call_override=self.successful_call
        )
        self.assertEqual(first["status"], "succeeded")
        self.assertEqual(first["provider_cost"], 0.003)
        self.assertEqual([item["stage"] for item in first["stages"]], ["detail", "metrics", "comments"])
        second = update_content_data(
            self.content_id, db_path=self.db, call_override=self.successful_call
        )
        self.assertEqual(second["provider_cost"], 0)
        self.assertEqual(
            [item["status"] for item in second["stages"]],
            ["already_succeeded", "already_succeeded"],
        )
        with connect(self.db) as connection:
            content = connection.execute("SELECT * FROM content_items WHERE id=?", (self.content_id,)).fetchone()
            slots = connection.execute("SELECT stage,status FROM fetch_slots ORDER BY stage").fetchall()
            snapshot = connection.execute("SELECT * FROM content_metric_snapshots").fetchone()
            comment_version = connection.execute("SELECT * FROM comment_evidence_versions").fetchone()
            comments = connection.execute("SELECT * FROM comments").fetchall()
            scores = connection.execute("SELECT * FROM comment_user_scores").fetchall()
            raw_count = connection.execute("SELECT COUNT(*) FROM provider_raw_responses").fetchone()[0]
            usage = connection.execute("SELECT SUM(amount) FROM provider_usage").fetchone()[0]
        self.assertEqual(content["title"], "汽车保养完整内容")
        self.assertEqual(snapshot["view_count"], 1000)
        self.assertEqual(comment_version["comment_count"], 1)
        self.assertEqual(len(comments), 1)
        self.assertEqual(len(scores), 1)
        self.assertEqual(raw_count, 3)
        self.assertAlmostEqual(usage, 0.003)
        self.assertTrue(all(row["status"] == "succeeded" for row in slots))

    def test_failed_unbilled_provider_call_stays_retryable_and_cost_is_released(self) -> None:
        with connect(self.db) as connection:
            captured_at = now_utc()
            for stage, key in (("detail", "lifetime"), ("comments", "2026-W31")):
                connection.execute(
                    """
                    INSERT INTO fetch_slots(
                        content_id, stage, window_key, provider, adapter_version,
                        status, attempt_count, created_at, updated_at
                    ) VALUES (?, ?, ?, 'legacy-cache', 'migration-v1', 'succeeded', 1, ?, ?)
                    """,
                    (self.content_id, stage, key, captured_at, captured_at),
                )
            connection.commit()

        def fail(stage, content):
            raise CaptureError(
                "insufficient balance", retryable=True,
                error_code="provider_balance_blocked", http_status=402,
                billed=False, raw_response={"detail": "insufficient balance"},
            )

        result = update_content_data(self.content_id, db_path=self.db, call_override=fail)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["provider_cost"], 0)
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT * FROM fetch_slots WHERE stage='metrics' AND provider='TikHub'"
            ).fetchone()
            budget = connection.execute(
                "SELECT * FROM provider_budget_batches WHERE operation='douyin_video_statistics'"
            ).fetchone()
        self.assertEqual(slot["status"], "retryable_failed")
        self.assertEqual(budget["consumed_requests"], 0)
        self.assertEqual(budget["consumed_amount"], 0)

    def test_account_discovery_caches_provider_reference_and_upserts_new_content(self) -> None:
        account = upsert_account(
            {
                "phone": "13800138000", "operator_name": "运营甲",
                "platforms": [{"platform": "douyin", "uid": "99887766", "nickname": "汽车号"}],
            },
            db_path=self.db,
        )

        def discovery_call(operation, identity):
            if operation == "resolve_account":
                return ProviderResult(
                    {"reference": "MS4w.test"}, {"data": {"sec_user_id": "MS4w.test"}}, 200, True
                )
            return ProviderResult(
                {
                    "items": [{
                        "platform": "douyin", "platform_content_id": "987654321",
                        "canonical_url": "https://www.douyin.com/video/987654321",
                        "title": "新车首发", "body": "新车首发完整内容",
                        "published_at": "2026-08-02T01:00:00Z", "content_type": "video",
                    }]
                },
                {"data": {"aweme_list": [{"aweme_id": "987654321"}]}}, 200, True,
            )

        first = discover_account_content(
            int(account["id"]), "douyin", "99887766", as_of=date(2026, 8, 2),
            db_path=self.db, call_override=discovery_call,
        )
        second = discover_account_content(
            int(account["id"]), "douyin", "99887766", as_of=date(2026, 8, 2),
            db_path=self.db, call_override=discovery_call,
        )
        self.assertEqual(first["status"], "succeeded")
        self.assertEqual(first["reference_status"], "resolved")
        self.assertEqual(first["inserted"], 1)
        self.assertEqual(first["provider_cost"], 0.002)
        self.assertEqual(second["status"], "already_succeeded")
        self.assertEqual(second["provider_cost"], 0)
        with connect(self.db) as connection:
            discovered = connection.execute(
                "SELECT * FROM content_items WHERE platform_content_id='987654321'"
            ).fetchone()
            references = connection.execute("SELECT * FROM account_provider_references").fetchall()
            slots = connection.execute(
                "SELECT * FROM fetch_slots WHERE account_id=? ORDER BY window_key", (account["id"],)
            ).fetchall()
            raws = connection.execute(
                "SELECT * FROM provider_raw_responses WHERE account_id=?", (account["id"],)
            ).fetchall()
        self.assertEqual(discovered["account_id"], account["id"])
        self.assertEqual(len(references), 1)
        self.assertEqual(references[0]["reference_value"], "MS4w.test")
        self.assertEqual(len(slots), 2)
        self.assertTrue(all(row["status"] == "succeeded" for row in slots))
        self.assertEqual(len(raws), 2)


if __name__ == "__main__":
    unittest.main()
