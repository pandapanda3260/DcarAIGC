from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from v8.account_metrics import (
    ACCOUNT_FIELDS, AccountMetricError, parse_tikhub_profile,
    persist_account_metric_observation, select_account_metrics,
)
from v8.operations import account_read_model, upsert_account
from v8.storage import connect, initialize_database, transaction


class AccountMetricsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "account-metrics.sqlite3"
        self.connection = connect(self.db)
        initialize_database(self.connection)
        self.account = upsert_account({"phone": "", "operator_name": "local owner", "platforms": [
            {"platform": "douyin", "uid": "1234567890123456789", "nickname": "account"}
        ]}, db_path=self.db)
        self.identity = dict(self.connection.execute("SELECT * FROM account_platform_identities").fetchone())

    def tearDown(self) -> None:
        self.connection.close()
        self.temp.cleanup()

    def normalized(self, count=100, *, day="2026-08-28", status="provided") -> dict:
        return {
            "identity": {"platform": "douyin", "uid": self.identity["uid"]},
            "statistics_date": day,
            "metrics": {field: count if field == "follower_count" else None for field in ACCOUNT_FIELDS},
            "field_status": {field: {"status": status if field == "follower_count" else "not_requested"}
                             for field in ACCOUNT_FIELDS},
        }

    def profile(self, count=100) -> dict:
        return {"code": 200, "data": {"status_code": 0, "data": {
            "id_str": self.identity["uid"], "follow_info": {"follower_count": count},
            "aweme_count": 999, "total_favorited": 99999,
        }}}

    def persist(self, normalized=None, *, source="newrank_matrix", captured="2026-08-29T00:00:00Z",
                recorded=None, raw_id=None) -> dict:
        with transaction(self.connection):
            if raw_id is None:
                cursor = self.connection.execute(
                    """INSERT INTO provider_raw_responses(account_id,provider,operation,local_path,sha256,byte_size,http_status,captured_at)
                    VALUES (?,?,?,?,?,10,200,?)""",
                    (self.identity["account_id"], "TikHub" if source == "tikhub" else source,
                     "douyin_uid_profile" if source == "tikhub" else "matrix_account_list",
                     f"raw-{self.connection.total_changes}.json", "a" * 64, captured),
                )
                raw_id = int(cursor.lastrowid)
            result = persist_account_metric_observation(
                self.connection, account_identity_id=self.identity["id"], provider=source,
                raw_response_id=raw_id, normalized=normalized or self.normalized(),
                captured_at=captured, recorded_at=recorded or captured,
            )
        return result | {"raw_id": raw_id}

    def selected(self, cutoff="2026-08-29T01:00:00Z") -> dict:
        return select_account_metrics(self.connection, [self.identity["id"]], cutoff_at=cutoff)[self.identity["id"]]

    def test_unknown_is_not_local_count_or_zero(self) -> None:
        result = self.selected()
        self.assertIsNone(result["follower_count"])
        self.assertIsNone(result["platform_work_count"])
        self.assertEqual(result["data_status"], "not_collected")

    def test_profile_requires_all_status_and_subject_checks(self) -> None:
        for replacement in (None, {}, {"code": 200, "data": {"status_code": 1}},
                            {"code": 200, "data": {"status_code": 0, "data": {"id_str": "different"}}}):
            with self.subTest(payload=replacement), self.assertRaises(AccountMetricError):
                parse_tikhub_profile(replacement, platform="douyin", uid=self.identity["uid"])
        with self.assertRaises(AccountMetricError):
            parse_tikhub_profile(self.profile(), platform="douyin", uid=self.identity["uid"], http_status=500)

    def test_profile_fans_only_zero_is_real_xhs_disabled(self) -> None:
        result = parse_tikhub_profile(self.profile(0), platform="douyin", uid=self.identity["uid"])
        self.assertEqual(result["metrics"]["follower_count"], 0)
        self.assertEqual(result["field_status"]["platform_work_count"]["status"], "not_requested")
        self.assertIsNone(result["metrics"]["total_likes"])
        self.assertIsNone(result["statistics_date"])
        with self.assertRaisesRegex(AccountMetricError, "not enabled"):
            parse_tikhub_profile(self.profile(), platform="xiaohongshu", uid=self.identity["uid"])

    def test_invalid_profile_values_are_not_zero(self) -> None:
        for value, expected in [(None, "missing"), (-1, "invalid"), (True, "invalid"), (3.4, "invalid"), ("1万", "invalid")]:
            result = parse_tikhub_profile(self.profile(value), platform="douyin", uid=self.identity["uid"])
            self.assertEqual(result["field_status"]["follower_count"]["status"], expected)
            self.assertIsNone(result["metrics"]["follower_count"])

    def test_matrix_preference_is_not_maximum_or_latest_row(self) -> None:
        self.persist(self.normalized(80))
        profile = parse_tikhub_profile(self.profile(100), platform="douyin", uid=self.identity["uid"])
        self.persist(profile, source="tikhub", captured="2026-08-29T00:20:00Z")
        result = self.selected()
        self.assertEqual(result["follower_count"], 80)
        self.assertEqual(result["metric_fields"]["follower_count"]["effective_provider"], "newrank_matrix")
        self.assertEqual(result["data_date"], "2026-08-28")

    def test_latest_matrix_missing_allows_tikhub_then_old_stale(self) -> None:
        self.persist(self.normalized(80))
        self.persist(self.normalized(None, status="missing"), captured="2026-08-29T00:10:00Z")
        self.assertEqual(self.selected()["metric_fields"]["follower_count"]["freshness"], "stale")
        profile = parse_tikhub_profile(self.profile(0), platform="douyin", uid=self.identity["uid"])
        self.persist(profile, source="tikhub", captured="2026-08-29T00:20:00Z")
        self.assertEqual(self.selected()["follower_count"], 0)
        self.assertEqual(self.selected()["follower_trend"]["status"], "break")

    def test_two_time_cutoff_excludes_late_materialization(self) -> None:
        self.persist(self.normalized(80), recorded="2026-08-30T00:00:00Z")
        self.assertIsNone(self.selected()["follower_count"])
        self.assertEqual(self.selected("2026-08-30T01:00:00Z")["data_status"], "stale")

    def test_replay_keeps_first_recorded_time_and_payload_immutable(self) -> None:
        first = self.persist()
        replay = self.persist(raw_id=first["raw_id"], recorded="2026-08-30T00:00:00Z")
        self.assertFalse(replay["created"])
        self.assertEqual(first["id"], replay["id"])
        row = self.connection.execute("SELECT * FROM account_metric_observations").fetchone()
        self.assertEqual(row["recorded_at"], "2026-08-29T00:00:00Z")
        with self.assertRaisesRegex(AccountMetricError, "immutable"):
            self.persist(self.normalized(200), raw_id=first["raw_id"])
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute("UPDATE account_metric_observations SET source='changed'")
        self.connection.rollback()

    def test_raw_subject_provider_and_time_must_match(self) -> None:
        first = self.persist()
        for kwargs in ({"source": "tikhub"}, {"captured": "2026-08-29T02:00:00Z"}):
            with self.assertRaises(AccountMetricError):
                self.persist(raw_id=first["raw_id"], **kwargs)
        bad = self.normalized()
        bad["identity"]["uid"] = "another"
        with self.assertRaisesRegex(AccountMetricError, "subject"):
            self.persist(bad)

    def test_date_required_and_signed_daily_increment_not_lifetime_count(self) -> None:
        bad = self.normalized(day=None)
        with self.assertRaisesRegex(AccountMetricError, "rankDate"):
            self.persist(bad)
        value = self.normalized(0)
        value["metrics"].update(work_like_daily_increment=-5, total_likes=300)
        value["field_status"].update(work_like_daily_increment={"status": "provided"}, total_likes={"status": "provided"})
        self.persist(value)
        self.assertEqual(self.selected()["work_like_daily_increment"], -5)
        self.assertEqual(self.selected()["total_likes"], 300)
        self.assertIsNone(self.selected()["collect_daily_increment"])
        self.assertIsNone(self.selected()["total_likes_and_collects"])

    def test_consecutive_day_same_basis_trend_can_be_negative(self) -> None:
        self.persist(self.normalized(100, day="2026-08-27"), captured="2026-08-28T00:00:00Z")
        self.persist(self.normalized(90))
        self.assertEqual(self.selected()["follower_trend"], {"status": "available", "reason": "consecutive_same_basis", "delta": -10})
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)

    def test_account_read_model_keeps_local_ops_and_shared_projection(self) -> None:
        from unittest.mock import patch
        self.persist(self.normalized(90) | {"nickname": "daily provider name", "avatar_url": "https://example.com/avatar.jpg", "display_account_id": "public-name"})
        account = dict(self.connection.execute("SELECT * FROM accounts").fetchone())
        with patch("v8.account_metrics.now_utc", return_value="2026-08-29T01:00:00Z"):
            result = account_read_model(self.connection, account)
        self.assertEqual(result["operator_name"], "local owner")
        self.assertEqual(result["phone"], "")
        self.assertEqual(result["platforms"][0]["follower_count"], 90)
        self.assertEqual(result["platforms"][0]["content_count"], 0)
        self.assertEqual(result["platforms"][0]["nickname"], "daily provider name")
        self.assertEqual(result["platforms"][0]["unique_id"], "public-name")
        self.assertEqual(self.connection.execute("SELECT nickname FROM account_platform_identities").fetchone()[0], "account")
        self.assertIsNone(result["platforms"][0]["platform_work_count"])
        self.assertEqual(json.loads(self.connection.execute("SELECT payload_json FROM account_metric_observations").fetchone()[0])["statistics_date"], "2026-08-28")


if __name__ == "__main__":
    unittest.main()
