from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import tempfile
import threading
import unittest
from contextlib import ExitStack, contextmanager, redirect_stdout
from contextvars import Context
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from scripts import run_august_range_backfill as runner
from tests.roster_fixture import accept_roster
from tests.schema_fixture import initialize_historical_schema
from tests.v9_report_fixture import activate_v9_report_fixture
from v8 import capture, media, provider_budget, providers
from v8.capture import ProviderResult, execute_content_fetch
from v8.operations import upsert_account, upsert_content
from v8.storage import connect, migrate_database


SHANGHAI = ZoneInfo("Asia/Shanghai")
AS_OF = datetime(2026, 8, 29, 10, 0, tzinfo=SHANGHAI)
RAW_CAPTURED_AT = "2026-08-29T02:00:00Z"
PUBLISHED_AT = "2026-08-22T04:00:00Z"
UIDS = {"douyin": "12345678901", "xiaohongshu": "67f6657f000000000e02c21c"}


class AugustRangeBackfillTest(unittest.TestCase):
    """Exercise the real SQLite/slot/usage path without provider credentials."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.db = self.root / "fixture.sqlite3"
        self.run_root = self.root / "campaign"
        self.raw_root = self.root / "raw"
        self.media_root = self.root / "media"
        self.patches = ExitStack()
        self.addCleanup(self.patches.close)
        self.patches.enter_context(patch.object(capture, "RAW_ROOT", self.raw_root))
        self.patches.enter_context(patch.object(media, "MEDIA_ROOT", self.media_root))
        self.patches.enter_context(
            patch("v8.providers._request_json", side_effect=AssertionError("network forbidden"))
        )
        self.patches.enter_context(
            patch("urllib.request.urlopen", side_effect=AssertionError("network forbidden"))
        )
        self.patches.enter_context(
            patch.object(providers, "_load_key", side_effect=AssertionError("credentials forbidden"))
        )
        with connect(self.db) as connection:
            initialize_historical_schema(connection, target_version=17)
            migrate_database(connection, from_version=17, to_version=18)
            connection.commit()
        activate_v9_report_fixture(self.db, [])
        for platform, uid in UIDS.items():
            upsert_account(
                {
                    "phone": "",
                    "enabled": True,
                    "platforms": [{"platform": platform, "uid": uid, "nickname": platform}],
                },
                db_path=self.db,
            )
        with connect(self.db) as connection:
            accept_roster(connection, accepted_at="2026-08-29T01:00:00Z")
            connection.commit()
        self.sequence = 0

    def campaign(self, as_of: datetime = AS_OF):
        return runner.Campaign(db_path=self.db, run_root=self.run_root, as_of=as_of)

    @contextmanager
    def ordinary_processors(
        self, *, discovery: bool = False, content: bool = False
    ):
        """Run real processors as ordinary fixture capture, outside history.

        The campaign still enters its production ``history`` scope.  Tests of
        pagination, capture and budget accounting inject the real processor in
        a fresh Context so repair=0 is not mistaken for purchase authorization.
        Tests of the repair gate deliberately omit this fixture.
        """

        with ExitStack() as stack:
            if discovery:
                original_discovery = runner.rb.discover_account_content
                stack.enter_context(
                    patch.object(
                        runner.rb,
                        "discover_account_content",
                        side_effect=lambda *args, **kwargs: Context().run(
                            original_discovery, *args, **kwargs
                        ),
                    )
                )
            if content:
                original_update = runner.providers.update_content_data
                stack.enter_context(
                    patch.object(
                        runner.providers,
                        "update_content_data",
                        side_effect=lambda *args, **kwargs: Context().run(
                            original_update, *args, **kwargs
                        ),
                    )
                )
            yield

    def content(self, platform: str = "douyin", *, campaign=None) -> int:
        self.sequence += 1
        platform_id = (
            str(7500000000000000000 + self.sequence)
            if platform == "douyin"
            else f"{self.sequence:024x}"
        )
        url = (
            f"https://www.douyin.com/video/{platform_id}"
            if platform == "douyin"
            else f"https://www.xiaohongshu.com/explore/{platform_id}"
        )
        content = upsert_content(
            {
                "platform": platform,
                "platform_content_id": platform_id,
                "canonical_url": url,
                "title": f"测试汽车内容 {self.sequence}",
                "body": "待补全",
                "content_type": "video" if platform == "douyin" else "image",
                "account_uid": UIDS[platform],
                "published_at": PUBLISHED_AT,
            },
            db_path=self.db,
            source_group_on_insert="history-backfill" if campaign is not None else "",
        )
        content_id = int(content["id"])
        if campaign is not None:
            campaign.state["new_ids"].append(content_id)
            campaign.state["discovery_complete"] = True
            campaign._save()
        return content_id

    def row(self, content_id: int) -> dict[str, Any]:
        with connect(self.db) as connection:
            return dict(connection.execute("SELECT * FROM content_items WHERE id=?", (content_id,)).fetchone())

    @staticmethod
    def result(stage: str, content: dict[str, Any], *, with_metrics: bool = True) -> ProviderResult:
        if stage == "detail":
            data: dict[str, Any] = {
                "title": "汽车保养完整正文",
                "body": "懂车帝选车和二手车交易内容",
                "published_at": PUBLISHED_AT,
                "account_uid": UIDS[str(content["platform"])],
                "content_type": str(content["content_type"]),
                "media_urls": [],
            }
            if str(content["platform"]) == "xiaohongshu" and with_metrics:
                data["metrics"] = {
                    "view_count": 1200,
                    "comment_count": 12,
                    "like_count": 80,
                    "share_count": 6,
                    "collect_count": 7,
                }
        elif stage == "metrics":
            data = {
                "view_count": 1000,
                "comment_count": 12,
                "like_count": 50,
                "share_count": 5,
                "collect_count": 3,
            }
        else:
            raise AssertionError(f"unapproved paid stage: {stage}")
        return ProviderResult(data, {"stage": stage, "data": data}, 200, True)

    def seed_stage(
        self,
        content_id: int,
        stage: str,
        *,
        apply: bool = True,
        window_key: str | None = None,
        with_metrics: bool = True,
    ):
        content = self.row(content_id)
        provider, adapter, operation, price = providers.STAGE_CONFIG[(str(content["platform"]), stage)]
        key = window_key or ("lifetime" if stage == "detail" else "2026-08-29")
        budget = providers.ensure_task_budget(
            provider=provider,
            operation=operation,
            price=price,
            task_id=runner.TASK_ID,
            task_max_amount=runner.MAX_AMOUNT,
            db_path=self.db,
        )
        with (
            patch.object(capture, "now_utc", return_value=RAW_CAPTURED_AT),
            patch.object(providers, "now_utc", return_value=RAW_CAPTURED_AT),
        ):
            # This helper constructs already-existing provider evidence for the
            # campaign tests.  It is a normal stage capture, not a historical
            # repair purchase (which is deliberately zero-budget by default).
            with provider_budget.paid_scope(stage):
                outcome = execute_content_fetch(
                    content_id=content_id,
                    stage=stage,
                    window_key=key,
                    provider=provider,
                    adapter_version=adapter,
                    operation=operation,
                    call=lambda: self.result(stage, content, with_metrics=with_metrics),
                    db_path=self.db,
                    budget_id=budget,
                    task_id=runner.TASK_ID,
                    task_max_amount=runner.MAX_AMOUNT,
                )
            if apply:
                providers._store_stage_result(content, stage, key, outcome, db_path=self.db)
        return outcome

    def usage(self) -> tuple[int, float, tuple[str, ...]]:
        with connect(self.db) as connection:
            rows = connection.execute(
                "SELECT task_id,operation,amount FROM provider_usage ORDER BY id"
            ).fetchall()
        self.assertTrue(all(row["task_id"] == runner.TASK_ID for row in rows))
        return len(rows), round(sum(float(row["amount"]) for row in rows), 6), tuple(str(row["operation"]) for row in rows)

    def metrics(self, content_id: int) -> list[dict[str, Any]]:
        with connect(self.db) as connection:
            return [dict(row) for row in connection.execute(
                "SELECT * FROM content_metric_snapshots WHERE content_id=? ORDER BY id", (content_id,)
            )]

    def assert_no_comments(self) -> None:
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM fetch_slots WHERE stage='comments'").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM comment_capture_runs").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage WHERE operation LIKE '%comments%'").fetchone()[0], 0)

    def legacy_partial_state(self) -> tuple[Path, str, int, int, int]:
        baseline_id = self.content()
        outside_uid = "98765432109"
        outside = upsert_account(
            {
                "phone": "",
                "enabled": True,
                "platforms": [{
                    "platform": "douyin",
                    "uid": outside_uid,
                    "nickname": "legacy-outside-roster",
                }],
            },
            db_path=self.db,
        )
        campaign = self.campaign()
        with campaign.window():
            accepted_id = self.content(campaign=campaign)
            rejected = upsert_content(
                {
                    "platform": "douyin",
                    "platform_content_id": str(7600000000000000000 + self.sequence),
                    "canonical_url": f"https://www.douyin.com/video/{7600000000000000000 + self.sequence}",
                    "title": "旧扫描范围外账号内容",
                    "body": "只留库存，不得继续采购",
                    "content_type": "video",
                    "account_uid": outside_uid,
                    "published_at": PUBLISHED_AT,
                },
                db_path=self.db,
                source_group_on_insert="history-backfill",
            )
            rejected_id = int(rejected["id"])
            current_contract = campaign.state["contract"]
            legacy_identities = [
                [int(row[1]), str(row[2]), str(row[3])]
                for row in current_contract["identities"]
            ][:-1]
            with connect(self.db) as connection:
                outside_identity = connection.execute(
                    "SELECT account_id,platform,uid FROM account_platform_identities WHERE account_id=?",
                    (outside["id"],),
                ).fetchone()
            legacy_identities.append(list(outside_identity))
            legacy_contract = json.loads(json.dumps(current_contract))
            legacy_contract.update(
                schema=runner.LEGACY_SCHEMA,
                roster=None,
                identities=legacy_identities,
                identity_sha256=runner._digest(legacy_identities),
            )
            campaign.state.update(
                contract=legacy_contract,
                existing_pending_ids=[baseline_id],
                new_ids=[accepted_id, rejected_id],
                visited_ids=[baseline_id, accepted_id, rejected_id],
                relation_pending_ids=[accepted_id, rejected_id],
                results={
                    "content": {
                        str(accepted_id): {"status": "succeeded"},
                        str(rejected_id): {"status": "succeeded"},
                    },
                },
                refresh_intents={
                    str(accepted_id): {"state": "prepared"},
                    str(rejected_id): {"state": "prepared"},
                },
                discovery_complete=False,
            )
            self.seed_stage(accepted_id, "detail")
            campaign._save()
        manifest = self.run_root / "2026-08-29" / f"{runner.TASK_ID}.contents.json"
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text(json.dumps({
            "task_id": runner.TASK_ID,
            "start": runner.rb._iso(runner.START),
            "end": runner.rb._iso(runner.END),
            "contents": [{"content_id": rejected_id}],
        }))
        source = self.run_root / "campaign.json"
        source_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
        return source, source_sha256, baseline_id, accepted_id, rejected_id

    def test_fixed_campaign_contract_and_initial_state(self) -> None:
        self.assertEqual(runner.START, datetime(2026, 8, 17, tzinfo=SHANGHAI))
        self.assertEqual(runner.END, datetime(2026, 8, 27, 23, 59, 59, tzinfo=SHANGHAI))
        self.assertEqual(runner.MAX_AMOUNT, 50.0)
        self.assertEqual(runner.MAX_PAGES, 100)
        existing_id = self.content()
        campaign = self.campaign()
        with campaign.window():
            self.assertIn(existing_id, campaign.state["baseline_ids"])
            self.assertEqual(campaign.state["new_ids"], [])
            for key in ("existing_pending_ids", "visited_ids", "results", "refresh_intents", "days"):
                self.assertIn(key, campaign.state)
        self.assertTrue((self.run_root / "campaign.json").is_file())

    def test_cli_campaign_parameters_override_defaults_without_crossing_contracts(
        self,
    ) -> None:
        start = datetime(2026, 8, 28, tzinfo=SHANGHAI)
        end = datetime(2026, 8, 30, 23, 59, 59, tzinfo=SHANGHAI)
        task_id = "tikhub-backfill-20260828-20260830-v1"
        mocked = Mock()
        mocked.status.return_value = {"status": "succeeded"}
        with patch.object(runner, "Campaign", return_value=mocked) as constructor:
            with redirect_stdout(io.StringIO()):
                code = runner.main([
                    "status",
                    "--db", str(self.db),
                    "--run-root", str(self.run_root),
                    "--as-of", "2026-08-30T23:59:59+08:00",
                    "--task-id", task_id,
                    "--start", "2026-08-28T00:00:00+08:00",
                    "--end", "2026-08-30T23:59:59+08:00",
                    "--max-amount", "12.5",
                ])
        self.assertEqual(code, 0)
        constructor.assert_called_once_with(
            self.db,
            self.run_root,
            datetime(2026, 8, 30, 23, 59, 59, tzinfo=SHANGHAI),
            task_id=task_id,
            start=start,
            end=end,
            max_amount=12.5,
        )

    def test_parameterized_discovery_uses_own_range_task_raw_and_budget(self) -> None:
        task_id = "fixture-range-20260820-20260823"
        start = datetime(2026, 8, 20, tzinfo=SHANGHAI)
        end = datetime(2026, 8, 23, 23, 59, 59, tzinfo=SHANGHAI)
        campaign = runner.Campaign(
            self.db,
            self.root / "parameterized-campaign",
            AS_OF,
            task_id=task_id,
            start=start,
            end=end,
            max_amount=7.5,
        )

        def discovery_call(operation, identity):
            if operation == "resolve_account":
                reference = "MS4wLjABAAAA" + "b" * 64
                return ProviderResult(
                    {"reference": reference},
                    {"data": {"sec_user_id": reference}},
                    200,
                    True,
                )
            normalized = {"items": [], "has_more": False, "next_cursor": ""}
            return ProviderResult(normalized, {"data": normalized}, 200, True)

        with self.ordinary_processors(discovery=True), campaign.window():
            campaign.discover(call_override=discovery_call)
            self.assertEqual(campaign.state["contract"]["task_id"], task_id)
            self.assertEqual(campaign.state["contract"]["start"], runner.rb._iso(start))
            self.assertEqual(campaign.state["contract"]["end"], runner.rb._iso(end))
            self.assertEqual(campaign.state["contract"]["archive_before"], runner.rb._iso(start))
            self.assertEqual(campaign.state["contract"]["max_amount"], 7.5)
            raw_repair = campaign.repair_discovery_raw_links()

        prefix = "range:2026-08-20:20260823T235959:"
        with connect(self.db) as connection:
            windows = [str(row[0]) for row in connection.execute(
                "SELECT window_key FROM fetch_slots "
                "WHERE stage='discovery' AND window_key LIKE 'range:%'"
            )]
            task_ids = {str(row[0]) for row in connection.execute(
                "SELECT DISTINCT task_id FROM provider_usage"
            )}
            budgets = {float(row[0]) for row in connection.execute(
                "SELECT DISTINCT max_amount FROM provider_budget_batches"
            )}
            raw_paths = [str(row[0]) for row in connection.execute(
                "SELECT pr.local_path FROM provider_raw_responses pr "
                "JOIN fetch_attempts fa ON fa.id=pr.fetch_attempt_id "
                "JOIN fetch_slots fs ON fs.id=fa.slot_id "
                "WHERE fs.stage='discovery' AND fs.window_key LIKE 'range:%'"
            )]
        self.assertTrue(windows)
        self.assertTrue(all(window.startswith(prefix) for window in windows))
        self.assertEqual(task_ids, {task_id})
        self.assertEqual(budgets, {7.5})
        self.assertTrue(raw_paths)
        self.assertEqual(raw_repair["checked"], len(raw_paths))
        self.assertTrue(
            (campaign.day_root / f"{task_id}.json").is_file()
        )

    def test_owner_approved_budget_migration_updates_contracts_and_batches_once(
        self,
    ) -> None:
        with patch.object(runner, "MAX_AMOUNT", runner.PREVIOUS_MAX_AMOUNT):
            legacy = self.campaign()
            with legacy.window():
                legacy._phase_contract("content")
                budget_id = providers.ensure_task_budget(
                    provider="TikHub",
                    operation="douyin_video_detail",
                    price=providers.TIKHUB_PRICE,
                    task_id=runner.TASK_ID,
                    task_max_amount=runner.MAX_AMOUNT,
                    db_path=self.db,
                )
                with connect(self.db) as connection:
                    connection.execute(
                        """
                        UPDATE provider_budget_batches
                        SET consumed_requests=123,consumed_amount=0.123
                        WHERE id=?
                        """,
                        (budget_id,),
                    )
                    connection.commit()

        migrated = self.campaign(AS_OF + timedelta(hours=5))
        with migrated.window(prepare=False):
            result = migrated.raise_task_budget()
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["from_amount"], 30.0)
        self.assertEqual(result["to_amount"], 50.0)
        with connect(self.db) as connection:
            row = connection.execute(
                """
                SELECT max_amount,max_billable_requests,daily_quota,
                  consumed_requests,consumed_amount,status
                FROM provider_budget_batches WHERE id=?
                """,
                (budget_id,),
            ).fetchone()
        self.assertEqual(
            tuple(row),
            (50.0, 50000, 50000, 123, 0.123, "approved"),
        )
        state = json.loads((self.run_root / "campaign.json").read_text())
        self.assertEqual(state["contract"]["max_amount"], 50.0)
        self.assertEqual(len(state["budget_migrations"]), 1)
        range_state = json.loads(
            (
                self.run_root
                / "2026-08-29"
                / f"{runner.TASK_ID}.json"
            ).read_text()
        )
        self.assertEqual(range_state["contract"]["max_amount"], 50.0)
        self.assertTrue(all(
            contract["max_amount"] == 50.0
            for contract in range_state["phase_contracts"].values()
        ))

        resumed = self.campaign()
        with resumed.window():
            self.assertEqual(resumed.state["contract"]["max_amount"], 50.0)
        with resumed.window(prepare=False):
            repeated = resumed.raise_task_budget()
        self.assertEqual(repeated["status"], "already_succeeded")
        final_state = json.loads((self.run_root / "campaign.json").read_text())
        self.assertEqual(len(final_state["budget_migrations"]), 1)

    def test_contract_excludes_enabled_identity_outside_accepted_roster(self) -> None:
        outside = upsert_account(
            {
                "phone": "",
                "enabled": True,
                "platforms": [{
                    "platform": "douyin",
                    "uid": "98765432109",
                    "nickname": "outside-roster",
                }],
            },
            db_path=self.db,
        )
        campaign = self.campaign()
        with campaign.window():
            contract = campaign.state["contract"]
            self.assertEqual(len(contract["identities"]), 2)
            self.assertIn(int(outside["id"]), contract["excluded_account_ids"])
        self.assertEqual(self.usage()[:2], (0, 0))

    def test_real_discovery_uses_range_budget_and_preserves_detail_gap(self) -> None:
        calls: list[str] = []

        def discovery_call(operation, identity):
            calls.append(operation)
            if operation == "resolve_account":
                reference = "MS4wLjABAAAA" + "a" * 64
                return ProviderResult(
                    {"reference": reference}, {"data": {"sec_user_id": reference}}, 200, True
                )
            self.assertEqual(operation, "discover_content")
            if identity["platform"] == "douyin":
                items: list[dict[str, Any]] = []
            else:
                items = [{
                    "platform": "xiaohongshu",
                    "platform_content_id": "a" * 24,
                    "canonical_url": "https://www.xiaohongshu.com/explore/" + "a" * 24,
                    "title": "目标区间作品",
                    "body": "列表页摘要",
                    "published_at": PUBLISHED_AT,
                    "content_type": "image",
                }]
            normalized = {"items": items, "has_more": False, "next_cursor": ""}
            return ProviderResult(normalized, {"data": normalized}, 200, True)

        campaign = self.campaign()
        with self.ordinary_processors(discovery=True), campaign.window():
            campaign.discover(call_override=discovery_call)
            self.assertEqual(len(campaign.state["new_ids"]), 1)
            content_id = campaign.state["new_ids"][0]
            self.assertEqual(self.row(content_id)["source_group"], "history-backfill")
            with connect(self.db) as connection:
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM fetch_slots WHERE content_id=? AND stage='detail' AND status='succeeded'", (content_id,)
                ).fetchone()[0], 0)
                budgets = connection.execute("SELECT DISTINCT max_amount FROM provider_budget_batches").fetchall()
            self.assertEqual([float(row[0]) for row in budgets], [50.0])
        self.assertIn("discover_content", calls)
        self.assert_no_comments()

    def test_campaign_repairs_only_verified_discovery_hardlinks_without_provider_cost(self) -> None:
        def discovery_call(operation, identity):
            if operation == "resolve_account":
                reference = "MS4wLjABAAAA" + "a" * 64
                return ProviderResult(
                    {"reference": reference}, {"data": {"sec_user_id": reference}}, 200, True
                )
            normalized = {"items": [], "has_more": False, "next_cursor": ""}
            return ProviderResult(normalized, {"data": normalized}, 200, True)

        campaign = self.campaign()
        with self.ordinary_processors(discovery=True), campaign.window():
            campaign.discover(call_override=discovery_call)
        with connect(self.db) as connection:
            row = connection.execute("""
                SELECT pr.local_path FROM fetch_slots fs
                JOIN fetch_attempts fa ON fa.slot_id=fs.id
                JOIN provider_raw_responses pr ON pr.fetch_attempt_id=fa.id
                WHERE fs.stage='discovery' AND fs.status='succeeded'
                  AND fs.provider='TikHub'
                  AND fs.window_key LIKE 'range:2026-08-17:20260827T235959:%'
                  AND pr.id=(
                    SELECT pr2.id FROM fetch_attempts fa2
                    JOIN provider_raw_responses pr2 ON pr2.fetch_attempt_id=fa2.id
                    WHERE fa2.slot_id=fs.id
                    ORDER BY fa2.attempt_number DESC,pr2.id DESC LIMIT 1
                  )
                ORDER BY pr.id LIMIT 1
            """).fetchone()
        raw_path = Path(str(row["local_path"]))
        if not raw_path.is_absolute():
            raw_path = runner.storage.PROJECT_ROOT / raw_path
        rollback_copy = self.root / "schema18-rollback-hardlink.json"
        original_inode = raw_path.stat().st_ino
        os.link(raw_path, rollback_copy)
        self.assertEqual(raw_path.stat().st_nlink, 2)
        before_usage = self.usage()
        with campaign.window():
            result = campaign.repair_discovery_raw_links()
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["repaired"], 1)
        self.assertEqual(result["already_single"], 1)
        self.assertNotEqual(raw_path.stat().st_ino, original_inode)
        self.assertEqual(raw_path.stat().st_nlink, 1)
        self.assertEqual(rollback_copy.stat().st_ino, original_inode)
        self.assertEqual(rollback_copy.stat().st_nlink, 1)
        self.assertEqual(self.usage(), before_usage)
        with campaign.window():
            repeated = campaign.repair_discovery_raw_links()
        self.assertEqual(repeated["repaired"], 0)
        self.assertEqual(repeated["already_single"], 2)

    def test_new_douyin_pays_only_detail_and_metrics_and_same_day_reuses(self) -> None:
        campaign = self.campaign()
        calls: list[str] = []

        def provider_call(stage, content):
            calls.append(stage)
            return self.result(stage, content)

        with self.ordinary_processors(content=True), campaign.window():
            content_id = self.content(campaign=campaign)
            campaign.capture_one(content_id, call_override=provider_call)
            self.assertEqual(calls, ["detail", "metrics"])
            self.assertEqual(self.usage()[:2], (2, 0.002))
            before = self.usage()
            forbidden = Mock(side_effect=AssertionError("repeat provider call"))
            campaign.capture_one(content_id, call_override=forbidden)
            forbidden.assert_not_called()
            self.assertEqual(self.usage(), before)
            self.assertEqual(len(self.metrics(content_id)), 1)
        self.assert_no_comments()

    def test_successful_metrics_are_reused_on_a_later_day_without_new_purchase(self) -> None:
        first = self.campaign()
        with self.ordinary_processors(content=True), first.window():
            content_id = self.content(campaign=first)
            first.capture_one(content_id, call_override=self.result)
        before_usage = self.usage()
        before_metrics = self.metrics(content_id)
        second = self.campaign(AS_OF + timedelta(days=1))
        forbidden = Mock(side_effect=AssertionError("cross-day repurchase"))
        with second.window():
            second.capture_one(content_id, call_override=forbidden)
            self.assertIn("2026-08-29", second.state["days"])
            self.assertIn("2026-08-30", second.state["days"])
        forbidden.assert_not_called()
        self.assertEqual(self.usage(), before_usage)
        self.assertEqual(self.metrics(content_id), before_metrics)
        self.assert_no_comments()

    def test_successful_unapplied_raw_materializes_without_another_paid_call(self) -> None:
        campaign = self.campaign()
        forbidden = Mock(side_effect=AssertionError("raw replay became paid"))
        with campaign.window():
            content_id = self.content(campaign=campaign)
            self.seed_stage(content_id, "detail")
            raw = self.seed_stage(content_id, "metrics", apply=False)
            self.assertEqual(self.metrics(content_id), [])
            before = self.usage()
            campaign.capture_one(content_id, call_override=forbidden)
            after = self.metrics(content_id)
            self.assertEqual(len(after), 1)
            self.assertEqual(after[0]["view_count"], 1000)
            self.assertEqual(after[0]["raw_response_id"], raw.raw_response_id)
            self.assertEqual(after[0]["captured_at"], RAW_CAPTURED_AT)
            self.assertEqual(self.usage(), before)
            with connect(self.db) as connection:
                source = connection.execute("SELECT source FROM provider_raw_responses WHERE id=?", (raw.raw_response_id,)).fetchone()[0]
            self.assertEqual(source, "live_applied")
        forbidden.assert_not_called()

    def test_missing_snapshot_replays_original_cross_day_raw_without_relabeling_date(self) -> None:
        first = self.campaign()
        with first.window():
            content_id = self.content(campaign=first)
            self.seed_stage(content_id, "detail")
            raw = self.seed_stage(content_id, "metrics")
        before = self.usage()
        with connect(self.db) as connection:
            observation = dict(connection.execute(
                "SELECT * FROM content_metric_observations WHERE content_id=?", (content_id,)
            ).fetchone())
            connection.execute("DELETE FROM content_metric_snapshots WHERE content_id=?", (content_id,))
            connection.commit()
        second = self.campaign(AS_OF + timedelta(days=1))
        forbidden = Mock(side_effect=AssertionError("missing projection repurchased"))
        with second.window():
            second.capture_one(content_id, call_override=forbidden)
        forbidden.assert_not_called()
        self.assertEqual(self.usage(), before)
        restored = self.metrics(content_id)
        self.assertEqual(len(restored), 1)
        self.assertEqual(restored[0]["window_key"], "2026-08-29")
        self.assertEqual(restored[0]["captured_at"], RAW_CAPTURED_AT)
        self.assertEqual(restored[0]["raw_response_id"], raw.raw_response_id)
        with connect(self.db) as connection:
            observations = [dict(row) for row in connection.execute(
                "SELECT * FROM content_metric_observations WHERE content_id=?", (content_id,)
            )]
        self.assertEqual(observations, [observation])

    def test_xhs_successful_detail_reuses_routed_metrics_for_original_raw_day_for_free(self) -> None:
        campaign = self.campaign(AS_OF + timedelta(days=1))
        forbidden = Mock(side_effect=AssertionError("XHS metrics-only request forbidden"))
        with campaign.window():
            content_id = self.content("xiaohongshu", campaign=campaign)
            detail = self.seed_stage(content_id, "detail")
            before = self.usage()
            self.assertEqual(before[:2], (1, 0.01))
            initial = self.metrics(content_id)
            self.assertEqual(len(initial), 1)
            self.assertEqual(initial[0]["raw_response_id"], detail.raw_response_id)
            campaign.capture_one(content_id, call_override=forbidden)
            self.assertEqual(self.usage(), before)
            snapshots = self.metrics(content_id)
            self.assertEqual(len(snapshots), 1)
            self.assertEqual(snapshots[0]["window_key"], "2026-08-29")
            self.assertEqual(snapshots[0]["captured_at"], RAW_CAPTURED_AT)
            self.assertIsNone(snapshots[0]["view_count"])
            metadata = json.loads(snapshots[0]["metadata_json"])
            self.assertEqual(
                metadata["fields"]["view_count"]["reason"],
                "xiaohongshu_exposure_unsupported",
            )
            with connect(self.db) as connection:
                raw = connection.execute("SELECT captured_at FROM provider_raw_responses WHERE id=?", (detail.raw_response_id,)).fetchone()
                self.assertEqual(raw[0], RAW_CAPTURED_AT)
        forbidden.assert_not_called()
        self.assert_no_comments()

    def test_xhs_new_detail_and_metrics_use_only_one_paid_request(self) -> None:
        campaign = self.campaign()
        calls: list[str] = []

        def provider_call(stage, content):
            calls.append(stage)
            return self.result(stage, content)

        with self.ordinary_processors(content=True), campaign.window(), patch.object(
            capture, "now_utc", return_value=RAW_CAPTURED_AT
        ), patch.object(providers, "now_utc", return_value=RAW_CAPTURED_AT):
            content_id = self.content("xiaohongshu", campaign=campaign)
            campaign.capture_one(content_id, call_override=provider_call)
        self.assertEqual(calls, ["detail"])
        self.assertEqual(self.usage()[:2], (1, 0.01))
        self.assertEqual(len(self.metrics(content_id)), 1)
        self.assert_no_comments()

    def test_xhs_detail_without_metrics_never_triggers_metrics_only_purchase(self) -> None:
        campaign = self.campaign(AS_OF + timedelta(days=1))
        forbidden = Mock(side_effect=AssertionError("unquoted XHS metrics-only purchase"))
        with campaign.window():
            content_id = self.content("xiaohongshu", campaign=campaign)
            self.seed_stage(content_id, "detail", with_metrics=False)
            before = self.usage()
            result = campaign.capture_one(content_id, call_override=forbidden)
            self.assertEqual(result["status"], "partial")
            errors = {stage.get("error_code") for stage in result["stages"]}
            self.assertIn("xhs_detail_metrics_missing", errors)
            self.assertEqual(self.usage(), before)
            self.assertEqual(self.metrics(content_id), [])
        forbidden.assert_not_called()
        self.assert_no_comments()

    def test_task_ceiling_spans_operations_and_cross_day_resume_without_reset(self) -> None:
        first = self.campaign()
        calls: list[str] = []

        def provider_call(stage, content):
            calls.append(stage)
            return self.result(stage, content)

        with self.ordinary_processors(content=True), first.window():
            content_id = self.content(campaign=first)
            original_contract = json.loads(json.dumps(first.state["contract"]))
            # Aggregate earlier task usage in a real ledger. The subsequent
            # detail/metrics executions must share this different operation's cap.
            discovery_budget = providers.ensure_task_budget(
                provider="TikHub", operation="douyin_user_posts", price=0.001,
                task_id=runner.TASK_ID, task_max_amount=runner.MAX_AMOUNT, db_path=self.db,
            )
            with connect(self.db) as connection:
                connection.execute(
                    """INSERT INTO provider_usage(
                        task_id,budget_batch_id,provider,operation,request_attempts,
                        billed_requests,currency,amount,recorded_at,details_json
                    ) VALUES (?,?, 'TikHub','douyin_user_posts',49999,49999,'USD',49.999,?,'{}')""",
                    (runner.TASK_ID, discovery_budget, "2026-08-28T02:00:00Z"),
                )
                connection.execute(
                    "UPDATE provider_budget_batches SET consumed_requests=49999,consumed_amount=49.999 WHERE id=?",
                    (discovery_budget,),
                )
                connection.commit()
            result = first.run_phase("content", limit=1, call_override=provider_call)
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["processed"], 1)
            self.assertEqual(calls, ["detail"])
            self.assertEqual(self.usage()[:2], (2, 50.0))
            self.assertEqual(self.metrics(content_id), [])
            stage_results = first.state["results"]["content"][str(content_id)]["stages"]
            self.assertIn("task_budget_exhausted", {item.get("error_code") for item in stage_results})
        before = self.usage()
        forbidden = Mock(side_effect=AssertionError("budget reset on date change"))
        second = self.campaign(AS_OF + timedelta(days=1))
        with second.window():
            self.assertEqual(second.state["contract"], original_contract)
            result = second.run_phase("content", limit=1, call_override=forbidden)
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["usage"]["task_id"], runner.TASK_ID)
            self.assertEqual(result["usage"]["max_amount"], 50.0)
            self.assertEqual(result["usage"]["amount"], 50.0)
        forbidden.assert_not_called()
        self.assertEqual(self.usage(), before)
        self.assertEqual(self.metrics(content_id), [])
        with connect(self.db) as connection:
            budgets = connection.execute("SELECT DISTINCT max_amount FROM provider_budget_batches").fetchall()
        self.assertEqual([float(row[0]) for row in budgets], [50.0])
        self.assert_no_comments()

    def test_existing_baseline_is_never_allowed_to_purchase_even_if_added_to_new_ids(self) -> None:
        existing = self.content()
        original = self.row(existing)
        campaign = self.campaign()
        forbidden = Mock(side_effect=AssertionError("baseline purchase forbidden"))
        with campaign.window():
            campaign.state["new_ids"].append(existing)
            with self.assertRaises(RuntimeError):
                campaign.capture_one(existing, call_override=forbidden)
        forbidden.assert_not_called()
        self.assertEqual(self.usage()[:2], (0, 0))
        self.assertEqual(self.row(existing), original)

    def test_existing_history_backfill_debt_is_allowed_through_content_phase(self) -> None:
        existing = self.content()
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET source_group=? WHERE id=?",
                ("history-backfill", existing),
            )
            connection.commit()
        campaign = self.campaign()
        calls: list[str] = []

        def provider_call(stage, content):
            calls.append(stage)
            return self.result(stage, content)

        with self.ordinary_processors(content=True), campaign.window():
            self.assertIn(existing, campaign.state["baseline_ids"])
            self.assertIn(existing, campaign.state["existing_pending_ids"])
            campaign.state["discovery_complete"] = True
            campaign._save()
            result = campaign.run_phase(
                "content",
                limit=1,
                call_override=provider_call,
            )
        self.assertEqual(result["processed_content_ids"], [existing])
        self.assertEqual(calls, ["detail", "metrics"])
        self.assertEqual(self.usage()[:2], (2, 0.002))

    def test_same_day_resume_preserves_first_as_of_and_later_day_has_new_contract(self) -> None:
        first = self.campaign()
        with first.window():
            original_day = json.loads(json.dumps(first.state["days"]["2026-08-29"]))
        second = self.campaign(AS_OF + timedelta(hours=4))
        with second.window():
            self.assertEqual(second.state["days"]["2026-08-29"], original_day)
        third = self.campaign(AS_OF + timedelta(days=1))
        with third.window():
            self.assertEqual(third.state["days"]["2026-08-29"], original_day)
            self.assertIn("2026-08-30", third.state["days"])
        self.assertEqual(self.usage()[:2], (0, 0))

    def test_identity_drift_blocks_resume_before_new_requests(self) -> None:
        with self.campaign().window():
            pass
        with connect(self.db) as connection:
            connection.execute("UPDATE account_platform_identities SET uid='99999999999' WHERE platform='douyin'")
            connection.commit()
        with self.assertRaises(RuntimeError):
            with self.campaign().window():
                self.fail("identity drift was accepted")
        self.assertEqual(self.usage()[:2], (0, 0))

    def test_verified_schema17_state_migrates_to_fresh_schema18_root_and_filters_scope(self) -> None:
        source, source_sha256, baseline_id, accepted_id, rejected_id = self.legacy_partial_state()
        source_before = source.read_bytes()
        migrated = runner.Campaign(
            db_path=self.db,
            run_root=self.root / "campaign-schema18",
            as_of=AS_OF,
        )
        with self.assertRaisesRegex(RuntimeError, "contract changed"):
            with self.campaign().window():
                self.fail("legacy state resumed without explicit migration")
        before_usage = self.usage()
        with patch.object(runner, "LEGACY_STATE_SHA256", source_sha256):
            with migrated.window(prepare=False):
                result = migrated.migrate_legacy_state(self.run_root)
            self.assertEqual(result["status"], "succeeded")
            self.assertEqual(result["excluded_new_contents"], 1)
            self.assertEqual(migrated.state["baseline_ids"], [baseline_id])
            self.assertEqual(migrated.state["new_ids"], [accepted_id])
            self.assertEqual(migrated.state["visited_ids"], [baseline_id, accepted_id])
            self.assertEqual(migrated.state["relation_pending_ids"], [accepted_id])
            self.assertEqual(set(migrated.state["results"]["content"]), {str(accepted_id)})
            self.assertEqual(set(migrated.state["refresh_intents"]), {str(accepted_id)})
            self.assertEqual(migrated.state["manifest_sha256"], {})
            self.assertEqual(
                migrated.state["contract_migrations"][-1]["source_sha256"],
                source_sha256,
            )
            receipt = migrated.state["contract_migrations"][-1]
            self.assertEqual(receipt["added_identity_count"], 1)
            self.assertEqual(receipt["removed_identity_count"], 1)
            self.assertEqual(receipt["retained_identity_count"], 1)
            self.assertTrue(migrated.status()["contract_matches"])
            with migrated.window():
                self.assertNotIn(rejected_id, migrated.state["new_ids"])
            with migrated.window(prepare=False):
                repeated = migrated.migrate_legacy_state(self.run_root)
            self.assertEqual(repeated["status"], "already_succeeded")
        self.assertEqual(source.read_bytes(), source_before)
        self.assertEqual(self.usage(), before_usage)

    def test_legacy_state_migration_rejects_unapproved_or_occupied_target_atomically(self) -> None:
        source, source_sha256, *_ = self.legacy_partial_state()
        target_root = self.root / "campaign-schema18"
        migrated = runner.Campaign(db_path=self.db, run_root=target_root, as_of=AS_OF)
        with patch.object(runner, "LEGACY_STATE_SHA256", "0" * 64):
            with migrated.window(prepare=False), self.assertRaisesRegex(RuntimeError, "sha256"):
                migrated.migrate_legacy_state(self.run_root)
        self.assertFalse((target_root / "campaign.json").exists())
        occupied = {"contract": {"task_id": "another-campaign"}}
        (target_root / "campaign.json").write_text(json.dumps(occupied))
        with patch.object(runner, "LEGACY_STATE_SHA256", source_sha256):
            with migrated.window(prepare=False), self.assertRaisesRegex(RuntimeError, "another state"):
                migrated.migrate_legacy_state(self.run_root)
        self.assertEqual(json.loads((target_root / "campaign.json").read_text()), occupied)
        self.assertTrue(source.is_file())

    def test_release_drift_blocks_resume_before_new_requests(self) -> None:
        with self.campaign().window():
            pass
        with connect(self.db) as connection:
            connection.execute("UPDATE evaluation_releases SET matcher_rule_sha256=? WHERE status='active'", ("f" * 64,))
            connection.commit()
        with self.assertRaises(RuntimeError):
            with self.campaign().window():
                self.fail("release drift was accepted")
        self.assertEqual(self.usage()[:2], (0, 0))

    def test_wrong_schema_blocks_before_campaign_state_and_requests(self) -> None:
        with connect(self.db) as connection:
            connection.execute("PRAGMA user_version=19")
            connection.commit()
        with self.assertRaises(RuntimeError):
            with self.campaign().window():
                self.fail("schema19 was accepted")
        self.assertFalse((self.run_root / "campaign.json").exists())
        self.assertEqual(self.usage()[:2], (0, 0))

    def formal_paths(self):
        stack = ExitStack()
        # All "formal" paths below remain within this test's temporary root.
        # Remove the test-process guard only here, so freeze/flock are exercised.
        stack.enter_context(patch.dict(os.environ, {"DCAR_TEST_DENY_FORMAL_DB": "0"}))
        stack.enter_context(patch.object(runner, "FORMAL_DB", self.db))
        stack.enter_context(patch.object(runner, "PRODUCTION_ROOT", runner.REPO))
        stack.enter_context(patch.object(runner, "FREEZE_LOCK", self.root / "operator-freeze.lock"))
        stack.enter_context(patch.object(runner, "WRITER_LOCK", self.root / "writer-worker.lock"))
        stack.enter_context(patch.object(media, "pinned_whisper_model_path", return_value=self.root / "model"))
        stack.enter_context(patch.object(media, "ocr_binary_path", return_value=self.db))
        stack.enter_context(patch.object(runner.shutil, "which", return_value="/test-only/bin/tool"))
        return stack

    def test_formal_database_without_freeze_is_rejected(self) -> None:
        with self.formal_paths(), self.assertRaisesRegex(RuntimeError, "freeze lock"):
            with self.campaign().window():
                self.fail("formal mutation without freeze was accepted")
        self.assertFalse((self.run_root / "campaign.json").exists())

    def test_installed_formal_database_guard_does_not_depend_on_env_default(self) -> None:
        installed = self.root / "Application Support" / "DcarAIGC" / "data" / "dcar_insight.sqlite3"
        installed.parent.mkdir(parents=True)
        installed.hardlink_to(self.db)
        unrelated_default = self.root / "unrelated.sqlite3"
        campaign = runner.Campaign(
            db_path=installed,
            run_root=self.run_root,
            as_of=AS_OF,
        )
        with (
            patch.object(runner, "FORMAL_DB", unrelated_default),
            patch.object(runner.storage, "DEFAULT_DB", unrelated_default),
            patch.object(runner.storage, "INSTALLED_DEFAULT_DB", installed),
            patch.object(runner.storage, "CHECKOUT_DEFAULT_DB", unrelated_default),
            patch.dict(os.environ, {"DCAR_TEST_DENY_FORMAL_DB": "1"}),
        ):
            self.assertTrue(campaign._is_formal())
            with self.assertRaisesRegex(RuntimeError, "formal DCar database"):
                with campaign._read():
                    self.fail("installed formal database bypassed the test guard")

    def test_busy_writer_lock_blocks_formal_window(self) -> None:
        freeze = self.root / "operator-freeze.lock"
        freeze.write_text("test-owned maintenance window\n", encoding="utf-8")
        freeze.chmod(0o600)
        lock = self.root / "writer-worker.lock"
        with lock.open("a+b") as held:
            fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.formal_paths(), self.assertRaisesRegex(RuntimeError, "another writer"):
                with self.campaign().window():
                    self.fail("busy writer lock was accepted")
            fcntl.flock(held.fileno(), fcntl.LOCK_UN)
        self.assertFalse((self.run_root / "campaign.json").exists())

    def test_isolated_checkout_cannot_write_even_when_formal_freeze_exists(self) -> None:
        freeze = self.root / "operator-freeze.lock"
        freeze.write_text("test-owned maintenance window\n", encoding="utf-8")
        with self.formal_paths(), patch.object(runner, "PRODUCTION_ROOT", self.root / "other-checkout"):
            with self.assertRaisesRegex(RuntimeError, "isolated checkout"):
                with self.campaign().window():
                    self.fail("isolated checkout was allowed to write a formal database")
        self.assertFalse((self.run_root / "campaign.json").exists())

    def test_temporary_campaign_lock_rejects_second_writer(self) -> None:
        with self.campaign().window():
            with self.assertRaises(RuntimeError):
                with self.campaign().window():
                    self.fail("second campaign writer was accepted")

    def test_status_is_read_only_and_does_not_prepare_state_or_budget(self) -> None:
        self.content()
        result = self.campaign().status()
        self.assertFalse(result["initialized"])
        self.assertEqual(result["enabled_identities"], 2)
        self.assertEqual(result["purchase_estimate"]["comments"], 0)
        self.assertFalse(self.run_root.exists())
        self.assertEqual(self.usage()[:2], (0, 0))

    def test_downstream_phases_require_completed_discovery(self) -> None:
        campaign = self.campaign()
        with campaign.window():
            self.content(campaign=campaign)
            campaign.state["discovery_complete"] = False
            campaign._save()
            for phase in ("content", "download", "local"):
                with self.subTest(phase=phase), self.assertRaisesRegex(
                    RuntimeError, "completed discovery"
                ):
                    campaign.run_phase(phase, limit=1)
        self.assertEqual(self.usage()[:2], (0, 0))

    def test_content_and_quote_exclude_new_ids_without_history_tag(self) -> None:
        campaign = self.campaign()
        calls: list[int] = []

        def provider_call(stage, content):
            calls.append(int(content["id"]))
            return self.result(stage, content)

        with self.ordinary_processors(content=True), campaign.window():
            tagged = self.content(campaign=campaign)
            cleared = self.content(campaign=campaign)
            with connect(self.db) as connection:
                connection.execute(
                    "UPDATE content_items SET source_group='' WHERE id=?", (cleared,)
                )
                connection.commit()
            quote = campaign.status()["purchase_estimate"]
            with patch.object(campaign, "download_one", return_value={
                "content_id": tagged, "status": "downloaded"
            }):
                result = campaign.run_phase(
                    "content", limit=10, call_override=provider_call
                )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["processed"], 1)
        self.assertEqual(calls, [tagged, tagged])
        self.assertEqual(quote["requests"], {
            "douyin_detail": 1,
            "douyin_metrics": 1,
            "xiaohongshu_detail": 0,
        })
        self.assertEqual(quote["amount"], 0.002)
        self.assertNotIn(str(cleared), campaign.state["results"]["content"])
        self.assert_no_comments()

    def test_stable_missing_play_count_is_deferred_without_another_purchase(self) -> None:
        campaign = self.campaign()
        calls: list[int] = []

        def provider_call(stage, content):
            calls.append(int(content["id"]))
            return self.result(stage, content)

        with self.ordinary_processors(content=True), campaign.window():
            deferred = self.content(campaign=campaign)
            provider, adapter, _operation, _price = providers.STAGE_CONFIG[
                ("douyin", "metrics")
            ]
            with connect(self.db) as connection:
                slot_id = capture.ensure_content_slot(
                    connection,
                    content_id=deferred,
                    stage="metrics",
                    window_key="2026-08-29",
                    provider=provider,
                    adapter_version=adapter,
                )
                connection.execute(
                    """
                    UPDATE fetch_slots SET status='retryable_failed',attempt_count=3,
                      last_error_code='invalid_response',
                      last_error_message=
                        'TikHub statistics omitted play_count for requested content'
                    WHERE id=?
                    """,
                    (slot_id,),
                )
                connection.commit()
            campaign.state["results"].setdefault("content", {})[str(deferred)] = {
                "content_id": deferred,
                "status": "partial",
                "stages": [{
                    "stage": "metrics",
                    "status": "failed",
                    "error_code": "invalid_response",
                    "message": "TikHub statistics omitted play_count for requested content",
                }],
            }
            next_content = self.content(campaign=campaign)
            with patch.object(campaign, "download_one", return_value={
                "content_id": next_content, "status": "downloaded"
            }):
                result = campaign.run_phase(
                    "content", limit=1, call_override=provider_call
                )
            quote = campaign.status()["purchase_estimate"]
        self.assertEqual(result["processed"], 1)
        self.assertEqual(calls, [next_content, next_content])
        self.assertEqual(
            campaign.state["results"]["content"][str(deferred)]["status"],
            "metrics_deferred_to_daily",
        )
        self.assertEqual(quote["requests"], {
            "douyin_detail": 1,
            "douyin_metrics": 0,
            "xiaohongshu_detail": 0,
        })
        self.assertEqual(quote["amount"], 0.001)
        self.assertEqual(self.usage()[:2], (2, 0.002))
        self.assert_no_comments()

    def test_partial_checkpoint_is_saved_and_resume_does_not_repeat_purchase(self) -> None:
        first = self.campaign()
        with self.ordinary_processors(content=True), first.window():
            ids = [self.content(campaign=first) for _ in range(3)]
            with patch.object(first, "download_one", side_effect=lambda cid, **kw: {
                "content_id": cid, "status": "downloaded"
            }):
                result = first.run_phase("content", limit=3, call_override=self.result)
            self.assertEqual(result["status"], "succeeded")
            self.assertEqual(result["processed"], 3)
            self.assertEqual(result["remaining"], 0)
        saved = json.loads((self.run_root / "campaign.json").read_text())
        self.assertEqual(set(saved["results"]["content"]), {str(cid) for cid in ids})
        self.assertEqual(set(saved["results"]["download"]), {str(cid) for cid in ids})
        before = self.usage()
        forbidden = Mock(side_effect=AssertionError("completed batch was bought again"))
        second = self.campaign(AS_OF + timedelta(days=1))
        with second.window():
            result = second.run_phase("content", limit=3, call_override=forbidden)
        self.assertEqual(result["processed"], 0)
        forbidden.assert_not_called()
        self.assertEqual(self.usage(), before)

    def test_content_downloads_use_bounded_media_concurrency_without_refresh(self) -> None:
        campaign = self.campaign()
        worker_count = runner.FRESH_DOWNLOAD_WORKERS
        active = 0
        peak = 0
        lock = threading.Lock()
        workers_started = threading.Event()
        refresh_flags: list[bool] = []

        def captured(content_id: int, **_kwargs) -> dict[str, Any]:
            return {"content_id": content_id, "status": "succeeded", "stages": []}

        def downloaded(content_id: int, **kwargs) -> dict[str, Any]:
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
                refresh_flags.append(bool(kwargs["allow_refresh"]))
                if active == worker_count:
                    workers_started.set()
            if not workers_started.wait(timeout=2):
                raise AssertionError("media downloads did not overlap")
            with lock:
                active -= 1
            return {"content_id": content_id, "status": "downloaded"}

        with campaign.window():
            ids = [self.content(campaign=campaign) for _ in range(worker_count + 2)]
            with (
                patch.object(campaign, "capture_one", side_effect=captured),
                patch.object(campaign, "download_one", side_effect=downloaded),
            ):
                result = campaign.run_phase("content", limit=len(ids))

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["processed"], len(ids))
        self.assertEqual(peak, worker_count)
        self.assertEqual(refresh_flags, [False] * len(ids))
        self.assertEqual(
            set(campaign.state["results"]["download"]),
            {str(content_id) for content_id in ids},
        )

    def test_campaign_window_scopes_effective_tikhub_network_concurrency(self) -> None:
        campaign = self.campaign()
        default_slots = capture.TIKHUB_NETWORK_SLOTS
        with campaign.window():
            campaign_slots = capture.TIKHUB_NETWORK_SLOTS
            self.assertIsNot(campaign_slots, default_slots)
            for _ in range(runner.CAPTURE_WORKERS):
                self.assertTrue(campaign_slots.acquire(blocking=False))
            self.assertFalse(campaign_slots.acquire(blocking=False))
        self.assertIs(capture.TIKHUB_NETWORK_SLOTS, default_slots)

    def test_balance_block_stops_dispatch_after_bounded_inflight_calls(self) -> None:
        calls: list[int] = []

        def out_of_balance(stage, content):
            calls.append(int(content["id"]))
            raise capture.CaptureError(
                "fixture balance exhausted", retryable=False,
                error_code="provider_balance_blocked", http_status=402, billed=False,
            )

        campaign = self.campaign()
        with self.ordinary_processors(content=True), campaign.window():
            content_count = runner.CAPTURE_WORKERS + 2
            for _ in range(content_count):
                self.content(campaign=campaign)
            result = campaign.run_phase(
                "content", limit=content_count, call_override=out_of_balance
            )
        self.assertEqual(result["status"], "blocked")
        self.assertGreaterEqual(len(calls), 1)
        self.assertLessEqual(len(calls), runner.CAPTURE_WORKERS)
        with connect(self.db) as connection:
            rows = [dict(row) for row in connection.execute(
                "SELECT request_attempts,billed_requests,amount,details_json "
                "FROM provider_usage ORDER BY id"
            )]
        self.assertEqual(sum(row["billed_requests"] for row in rows), 0)
        self.assertEqual(sum(row["request_attempts"] for row in rows), len(calls))
        self.assertEqual(sum(float(row["amount"]) for row in rows), 0)
        states = {json.loads(row["details_json"])["state"] for row in rows}
        self.assertIn("failed", states)
        self.assertLessEqual(states, {"failed", "not_sent"})
        self.assert_no_comments()

    def test_paid_scope_hard_blocks_stop_after_bounded_inflight_calls(self) -> None:
        for error_code in (
            "category_budget_exhausted",
            "global_budget_exhausted",
            "provider_circuit_open",
        ):
            with self.subTest(error_code=error_code):
                calls: list[int] = []
                campaign = runner.Campaign(
                    db_path=self.db,
                    run_root=self.root / f"campaign-{error_code}",
                    as_of=AS_OF,
                )

                def blocked(content_id, **_kwargs):
                    calls.append(int(content_id))
                    raise provider_budget.PaidScopeBlocked(
                        error_code, "fixture paid scope blocked"
                    )

                with campaign.window():
                    content_count = runner.CAPTURE_WORKERS + 2
                    for _ in range(content_count):
                        self.content(campaign=campaign)
                    with patch.object(campaign, "capture_one", side_effect=blocked):
                        result = campaign.run_phase(
                            "content", limit=content_count
                        )
                self.assertEqual(result["status"], "blocked")
                self.assertGreaterEqual(len(calls), 1)
                self.assertLessEqual(len(calls), runner.CAPTURE_WORKERS)
        self.assert_no_comments()

    def test_nonfinite_window_limit_is_rejected_before_processing(self) -> None:
        campaign = self.campaign()
        with campaign.window():
            for seconds in (float("nan"), float("inf"), 0):
                with self.assertRaises(RuntimeError):
                    campaign.run_phase("content", max_seconds=seconds)
        self.assertEqual(self.usage()[:2], (0, 0))

    def test_download_balance_block_leaves_remaining_ids_and_refresh_intents_untouched(self) -> None:
        campaign = self.campaign()
        with campaign.window():
            ids = [self.content(campaign=campaign) for _ in range(3)]
            with patch.object(campaign, "download_one", side_effect=capture.CaptureError(
                "fixture balance exhausted", retryable=False,
                error_code="provider_balance_blocked", http_status=402, billed=False,
            )) as download:
                result = campaign.run_phase("download", limit=3)
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["processed"], 1)
            self.assertEqual(download.call_count, 1)
            self.assertEqual(set(campaign.state["results"]["download"]), {str(ids[0])})
            self.assertEqual(campaign.state["refresh_intents"], {})
        self.assertEqual(self.usage()[:2], (0, 0))

    def test_blocked_dispatch_does_not_consume_an_unattempted_source_refresh(self) -> None:
        campaign = self.campaign()
        with campaign.window():
            cid = self.content(campaign=campaign)
            self.seed_stage(cid, "detail")
            before = self.usage()
            campaign._stop_paid.set()
            forbidden = Mock(side_effect=AssertionError("blocked source refresh reached provider"))
            with self.assertRaisesRegex(RuntimeError, "no further paid calls"):
                campaign._refresh(self.row(cid), {"source_sha256": "a" * 64}, forbidden)
            self.assertEqual(campaign.state["refresh_intents"], {})
            self.assertEqual(self.usage(), before)
            forbidden.assert_not_called()


if __name__ == "__main__":
    unittest.main()
