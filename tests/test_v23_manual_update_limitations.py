"""Manual updates preserve unavailable stages without granting more requests."""
import json
import unittest
from unittest.mock import patch

from tests import test_v23_media_source_refresh as fixture
from v8 import capture_commands, capture_manual, capture_runtime, storage


class ManualUpdateLimitationsTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixture.MediaSourceRefreshTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.db, self.at = self.fixture.db, fixture.AT
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))
        self.enterContext(patch.object(capture_runtime, "activation_at", return_value={
            "activation_id": 4, "profile_id": "integrated_route_v1",
            "roster_snapshot_id": None, "roster_members_sha256": "a" * 64}))
        self.enterContext(patch.object(capture_runtime, "_readiness", return_value=("runnable", "")))

    def configure_kuaishou(self):
        with storage.connect(self.db) as connection:
            connection.execute("UPDATE account_platform_identities SET platform='kuaishou',uid='001234'")
            connection.execute("UPDATE content_items SET platform='kuaishou',platform_content_id='5234567890123456789',"
                "canonical_url='https://www.kuaishou.com/short-video/3xwork',raw_account_uid='001234' WHERE id=1")

    def submit(self, kind="manual_update"):
        return capture_commands.submit_command(db_path=self.db, content_id=1, kind=kind, at=self.at)

    def read(self, run_id):
        with storage.connect(self.db) as connection:
            connection.execute("PRAGMA query_only=ON")
            return capture_commands.read_command(connection, run_id=run_id, content_id=1)

    def complete(self, run_id):
        capture_commands.process_commands(db_path=self.db, at=self.at)
        with storage.connect(self.db) as connection:
            capture_manual.validate_command(connection, run_id, content_id=1)
            connection.execute("UPDATE capture_work_items SET state='terminal',reason='',completed_at=?", (self.at,))
        return self.read(run_id)

    def test_kuaishou_limit_is_frozen_repeated_submission_reuses_and_completion_is_partial(self):
        self.configure_kuaishou()
        accepted = self.submit()
        self.assertEqual(self.submit()["run_id"], accepted["run_id"])
        self.assertEqual(accepted["status"], "pending")
        self.assertEqual(accepted["limited_stages"][0]["stage"], "comments")
        with storage.connect(self.db) as connection:
            before = connection.execute("SELECT details_json FROM scheduler_runs WHERE id=?", (accepted["run_id"],)).fetchone()[0]
            spec = json.loads(before)["identity"]["specification"]
            self.assertEqual({item["stage"] for item in spec["targets"]}, {"detail", "metrics"})
        result = self.complete(accepted["run_id"])
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["reason"], "comments_contract_unverified")
        self.assertIn("评论正文能力尚未核验", result["reason_label"])
        self.assertEqual(result["limited_stages"], spec["limited_stages"])
        with storage.connect(self.db) as connection:
            after = json.loads(connection.execute("SELECT details_json FROM scheduler_runs WHERE id=?", (accepted["run_id"],)).fetchone()[0])
            self.assertEqual(after["identity"]["specification"], spec)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM capture_work_items").fetchone()[0], 2)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0], 0)

    def test_limit_does_not_hide_running_or_blocked_work(self):
        self.configure_kuaishou()
        accepted = self.submit()
        capture_commands.process_commands(db_path=self.db, at=self.at)
        for state, reason, expected in (("running", "", "running"),
                ("paid_identity_hold", "billing_unknown", "blocked")):
            with self.subTest(state=state), storage.connect(self.db) as connection:
                connection.execute("UPDATE capture_work_items SET state=?,reason=?,owner_token=?",
                    (state, reason, "fixture-owner" if state == "running" else None))
                result = capture_commands.read_command(connection, run_id=accepted["run_id"], content_id=1)
                self.assertEqual(result["status"], expected)
                self.assertEqual(result["limited_stages"], accepted["limited_stages"])
                if reason:
                    self.assertIn(reason, result["reason"])

    def test_supported_manual_update_and_metrics_only_keep_success_semantics(self):
        accepted = self.submit()
        self.assertNotIn("limited_stages", accepted)
        self.assertEqual(self.complete(accepted["run_id"])["status"], "succeeded")

    def test_kuaishou_metrics_only_does_not_request_or_limit_comments(self):
        self.configure_kuaishou()
        accepted = self.submit("metrics_update")
        self.assertNotIn("limited_stages", accepted)
        self.assertEqual(self.complete(accepted["run_id"])["status"], "succeeded")

    def test_old_frozen_kuaishou_spec_without_limitations_remains_readable(self):
        self.configure_kuaishou()
        with storage.connect(self.db) as connection, storage.transaction(connection):
            spec = capture_runtime.manual_work_spec(connection, content_id=1, kind="manual_update", at=self.at)
            spec.pop("limited_stages")
            accepted = capture_commands.persist_specification(connection, specification=spec, at=self.at)
        result = self.complete(accepted["run_id"])
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["reason"], "")
        self.assertNotIn("limited_stages", result)


if __name__ == "__main__":
    unittest.main()
