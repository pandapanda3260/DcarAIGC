"""Explicit targets use actual Writer, routing, A/B, usage and raw ledgers.

Only private installation/sample qualification and provider responses are
fixtures. Membership, target binding, routes and budget guards remain real.
"""
from __future__ import annotations

import json
import unittest
import urllib.request
from unittest.mock import patch

from tests import test_v8_account_roster_capture as fixture
from tests import test_v8_provider_transport as transport_fixture
from v8 import capture, capture_commands, capture_manual, capture_planning
from v8 import capture_authorizations as auth, capture_release as release
from v8 import provider_budget, providers, provider_updates
from v8.operations import upsert_account, upsert_content
from v8.provider_transport import request_json
from v8.storage import connect, transaction

AT = fixture.AFTER
PID = "7380000000000000001"
UID = "123456789"
OP = "douyin_video_statistics"


class ManualContentScopeTest(unittest.TestCase):
    def setUp(self):
        self.base = fixture.AccountRosterCaptureTest()
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.db, self.root = self.base.db, self.base.root.resolve()
        for name in ("capture", "providers", "provider_updates", "provider_budget",
                     "capture_commands", "metric_observations", "storage"):
            self.enterContext(patch("v8." + name + ".now_utc", return_value=AT))
        self.enterContext(patch.object(capture, "RAW_ROOT", self.root / "manual-raw"))
        self.enterContext(auth.runtime_authority(release.current_runtime_bindings))
        self.account = upsert_account({"phone": "", "enabled": False, "platforms": [
            {"platform": "douyin", "uid": UID, "nickname": "manual fixture"},
        ]}, db_path=self.db)
        self.content = upsert_content({"platform": "douyin", "platform_content_id": PID,
            "canonical_url": "https://www.douyin.com/video/" + PID,
            "account_uid": UID, "content_type": "video", "title": "Keep title",
            "published_at": "2026-08-31T00:00:00Z"}, db_path=self.db)
        self.cid = self.content["id"]
        with connect(self.db) as connection:
            self.identity_id = capture_manual.freeze_target(connection, self.cid)["identity_id"]
        self.calls = []
        self.command = self.submit()

    def submit(self, **kwargs):
        return capture_commands.submit_command(db_path=self.db, content_id=self.cid,
            kind="metrics_update", at=AT, allowed_groups=kwargs.pop("allowed_groups", ["statistics", "detail_counts"]),
            task_id=kwargs.pop("task_id", "explicit-metrics-fixture"),
            task_max_amount=kwargs.pop("task_max_amount", 0.1),
            cycle_key="explicit-fixture-cycle", **kwargs)

    def response(self, group, content):
        self.calls.append(group)
        if group == "detail_counts":
            raw = {"code": 200, "data": {"status_code": 0, "aweme_detail": {
                "aweme_id": content["platform_content_id"], "author": {"uid": UID},
                "desc": "Must not replace title", "statistics": {"comment_count": 3, "collect_count": 2},
            }}}
            parsed = providers._parse_douyin_stage_payload("detail", content["platform_content_id"], raw)
            data = parsed.data["metrics"]
        else:
            raw = {"code": 200, "data": {"status_code": 0, "statistics_list": [{
                "aweme_id": content["platform_content_id"], "play_count": 88, "digg_count": 9,
            }]}}
            data = providers._parse_douyin_stage_payload("metrics", content["platform_content_id"], raw).data
        entity = json.dumps(raw, separators=(",", ":")).encode()
        url = "https://fixture.invalid/metrics"
        response = request_json(urllib.request.Request(url), route_id="fixture-route",
            route_generation="fixture-generation", timeout=45,
            opener=transport_fixture.FakeOpener(transport_fixture.FakeResponse(entity,
                headers={"Content-Length": str(len(entity))}, response_url=url)), clock=lambda: AT)
        return capture.ProviderResult(data, response.payload, response.status, True,
            entity_bytes=response.entity_body, transport_receipt=response.receipt)

    def refresh(self, **kwargs):
        return provider_updates.refresh_content_metrics(self.cid, db_path=self.db, at=AT,
            manual_command_run_id=kwargs.pop("manual_command_run_id", self.command["run_id"]),
            call_override=kwargs.pop("call_override", self.response), **kwargs)

    def snapshot(self):
        with connect(self.db) as connection:
            return {table: [tuple(row) for row in connection.execute("SELECT * FROM " + table + " ORDER BY 1")]
                    for table in ("accounts", "account_roster_snapshots", "account_roster_members",
                                  "content_items", "evidence_artifacts", "evaluation_versions")}

    def test_disabled_outside_roster_can_capture_without_changing_automatic_scope(self):
        before = self.snapshot()
        with connect(self.db) as connection:
            target = capture_manual.freeze_target(connection, self.cid)
            self.assertEqual(connection.execute("SELECT count(*) FROM account_roster_members WHERE account_identity_id=?",
                (target["identity_id"],)).fetchone()[0], 0)
            with self.assertRaises(provider_budget.PaidScopeBlocked):
                provider_budget.freeze_scope(connection, content_id=self.cid, account_id=None, stage="metrics")
        result = self.refresh()
        self.assertEqual(self.calls, ["detail_counts", "statistics"])
        self.assertEqual(result["provider_cost"], .002)
        self.assertEqual(self.snapshot(), before)
        with connect(self.db) as connection:
            self.assertIsNone(capture_planning.resolve_route(connection,
                account_id=self.account["id"], content_id=self.cid, operation=OP, at=AT))
            usages = connection.execute("SELECT details_json FROM provider_usage").fetchall()
            self.assertEqual(len(usages), 2)
            self.assertTrue(all(json.loads(row[0])["scope"]["manual_command_run_id"] == self.command["run_id"] for row in usages))
            self.assertEqual(connection.execute("SELECT count(*) FROM fetch_request_executions").fetchone()[0], 2)
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_raw_responses WHERE fetch_attempt_id IS NOT NULL").fetchone()[0], 2)
            with self.assertRaises(provider_budget.PaidScopeBlocked):
                provider_budget.freeze_scope(connection, content_id=self.cid, account_id=None, stage="metrics")

    def test_target_pid_uid_and_operation_cannot_be_borrowed(self):
        for table, key, column, changed in (
            ("content_items", self.cid, "platform_content_id", "7380000000000000999"),
            ("account_platform_identities", self.identity_id, "uid", "999999999"),
        ):
            with self.subTest(column=column):
                with connect(self.db) as connection, transaction(connection):
                    original = connection.execute(f"SELECT {column} FROM {table} WHERE id=?", (key,)).fetchone()[0]
                    connection.execute(f"UPDATE {table} SET {column}=? WHERE id=?", (changed, key))
                try:
                    with self.assertRaises(Exception):
                        self.refresh()
                finally:
                    with connect(self.db) as connection, transaction(connection):
                        connection.execute(f"UPDATE {table} SET {column}=? WHERE id=?", (original, key))
        with connect(self.db) as connection:
            with self.assertRaises(Exception):
                capture_manual.validate_command(connection, self.command["run_id"], content_id=self.cid,
                    operation="douyin_video_comments", stage="comments")
        self.assertEqual(self.calls, [])

    def test_partial_success_and_failed_business_write_replay_without_repurchase(self):
        with patch.object(providers, "_store_stage_result", side_effect=RuntimeError("fixture write failure")):
            with self.assertRaisesRegex(RuntimeError, "fixture write failure"):
                self.refresh(allowed_groups=["statistics"])
        result = self.refresh(allowed_groups=["statistics"])
        self.assertEqual(result["provider_cost"], 0)
        self.assertEqual(result["requests"][0]["status"], "replayed")
        self.assertEqual(result["status"], "partial")
        self.refresh(allowed_groups=["statistics"])
        self.assertEqual(self.calls, ["statistics"])

    def test_frozen_budget_and_group_restrictions_reject_before_network(self):
        for overrides in ({"task_max_amount": 3}, {"task_id": "different"},
                          {"cycle_key": "different"}, {"allowed_groups": ["comments"]}):
            with self.subTest(overrides=overrides):
                with self.assertRaises(providers.ProviderConfigurationError):
                    self.refresh(**overrides)
        tiny = self.submit(task_id="tiny-explicit-budget", task_max_amount=.0005)
        with self.assertRaises(Exception):
            self.refresh(manual_command_run_id=tiny["run_id"], allowed_groups=["statistics"])
        self.assertEqual(self.calls, [])

    def test_global_operation_gate_still_blocks_explicit_content(self):
        with connect(self.db) as connection, transaction(connection):
            body = {"provider": "tikhub", "operation": OP, "state": "closed", "reason": "fixture hold",
                    "evidence_json": "{}", "recorded_at": AT}
            connection.execute("INSERT INTO capture_paid_send_gate_events(provider,operation,state,reason,evidence_json,recorded_at,event_sha256) VALUES(?,?,?,?,?,?,?)",
                (*body.values(), auth.digest(body)))
        with self.assertRaises(Exception):
            self.refresh(allowed_groups=["statistics"])
        self.assertEqual(self.calls, [])

    def test_unknown_paid_identity_is_not_retried_by_another_manual_command(self):
        def unknown(group, content):
            self.calls.append(group)
            raise capture.CaptureError("fixture incomplete response", retryable=True,
                error_code="transport_error", billed=None)
        with self.assertRaises(capture.CaptureError):
            self.refresh(allowed_groups=["statistics"], call_override=unknown)
        second = self.submit(task_id="another-explicit-command")
        with self.assertRaises(Exception):
            self.refresh(manual_command_run_id=second["run_id"], allowed_groups=["statistics"], call_override=unknown)
        self.assertEqual(self.calls, ["statistics"])

    def test_legacy_manual_helper_submits_durable_command_without_provider_calls(self):
        result = providers.update_content_data_manual(self.cid, db_path=self.db, call_override=self.response)
        self.assertEqual(result["status"], "pending")
        with connect(self.db) as connection:
            spec = capture_manual.validate_command(connection, result["run_id"], content_id=self.cid)
        self.assertEqual(spec["kind"], "manual_update")
        self.assertEqual(self.calls, [])

    def test_manual_update_cannot_borrow_direct_metrics_with_another_cycle(self):
        command = capture_commands.submit_command(db_path=self.db, content_id=self.cid,
            kind="manual_update", at=AT)
        with self.assertRaisesRegex(providers.ProviderConfigurationError, "metrics_update.*frozen cycle"):
            self.refresh(manual_command_run_id=command["run_id"], cycle_key="unrequested-cycle")
        self.assertEqual(self.calls, [])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
