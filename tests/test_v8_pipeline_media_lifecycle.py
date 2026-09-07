"""Queue/lease-boundary tests with real SQLite ledgers and stubbed state views.

The physical archive/restore and completion gates have separate real-media
integration tests. These tests forbid supplier work and local recomputation
while a lifecycle prerequisite is blocked, including old durable debt.
"""
from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from tests import test_v8_pipeline as fixture
from v8 import durable_runs, pipeline
from v8.media_lifecycle import LifecycleError
from v8.media_state import MediaTerminalDetail
from v8.storage import connect


class PipelineMediaLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixture.PipelineTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.db = self.fixture.db
        self.snapshot = self.fixture.activate()
        self.content = self.fixture.content()
        self.cid = self.content["id"]
        self.bundle = {"bundle_id": "a" * 32, "state": {}}
        with connect(self.db) as connection:
            connection.execute("INSERT INTO taxonomy_versions(id,version,status,definition,created_at,published_at) "
                               "VALUES ('queue-fixture','queue-fixture','published','{}',?,?)", (fixture.AT, fixture.AT))
            connection.execute("INSERT INTO evaluation_releases(id,rule_version,taxonomy_version,matcher_rule_sha256,status,created_at,updated_at,activated_at) "
                               "VALUES ('queue-fixture','queue-fixture','queue-fixture',?,'active',?,?,?)",
                               ("a" * 64, fixture.AT, fixture.AT, fixture.AT))
            connection.commit()

    def states(self, reason):
        return patch.object(pipeline, "media_terminal_state_details", return_value={
            self.cid: MediaTerminalDetail("pending", reason),
        })

    def pending_run(self):
        identity = {"pipeline_version": pipeline.PIPELINE_VERSION, "kind": "content_pipeline",
                    "roster_snapshot_id": self.snapshot["id"],
                    "roster_snapshot_hash": self.snapshot["members_sha256"],
                    "created_for": fixture.AT, "candidate_ids": [self.cid]}
        return self.fixture.durable("content_pipeline", identity, checkpoint={
            "pending_ids": [self.cid], "items": [{"id": self.cid,
                "identity_id": self.fixture.identity_id, "historical": False}],
            "results": {}, "complete": False,
        })

    def test_unenrolled_new_source_is_pending_not_a_queue_wide_exception(self):
        with patch.object(pipeline, "_current_source_state", side_effect=LifecycleError("managed_source_pending")):
            with connect(self.db) as connection:
                self.assertFalse(pipeline._fingerprint_ready(connection, self.cid))
            candidates = pipeline._queue_candidates("content_pipeline", at=fixture.AT, db_path=self.db)
        self.assertEqual([row["id"] for row in candidates], [self.cid])
        with patch.object(pipeline, "_current_source_state", side_effect=LifecycleError("manifest_corrupt")):
            with connect(self.db) as connection, self.assertRaisesRegex(LifecycleError, "manifest_corrupt"):
                pipeline._fingerprint_ready(connection, self.cid)

    def test_blocked_originals_never_enter_automatic_recompute_but_metrics_and_comments_do(self):
        for reason in pipeline.MEDIA_BLOCKED_REASONS:
            with self.subTest(reason=reason), self.states(reason), patch.object(pipeline, "current_bundle", return_value=self.bundle):
                blocked = {}
                self.assertEqual(pipeline._queue_candidates("content_pipeline", at=fixture.AT,
                                                           db_path=self.db, blocked_media=blocked), [])
                self.assertEqual(blocked[str(self.cid)]["reason"], reason)
                for kind in ("metrics_backfill", "comments_refresh"):
                    self.assertIn(self.cid, [row["id"] for row in pipeline._queue_candidates(kind, at=fixture.AT, db_path=self.db)])
        self.assertEqual(self.fixture.rows("scheduler_runs")[-1]["job_id"], pipeline.ACTIVATION_JOB)
        self.assertEqual(self.fixture.rows("provider_usage"), [])

    def test_expired_old_pending_records_terminal_block_without_spend_retry_or_false_completion(self):
        run_id = self.pending_run()
        slots = self.fixture.rows("fetch_slots")
        local = Mock(side_effect=AssertionError("must not recompute"))
        with self.states("expired_non_replayable"), patch.object(pipeline, "current_bundle", return_value=self.bundle), \
                patch.object(pipeline, "update_content_data", side_effect=AssertionError("must not buy")) as supplier:
            result = pipeline.run_content_batch("content_pipeline", db_path=self.db, at=fixture.LATER,
                                                resume_run_id=run_id, local_runner=local)
        self.assertEqual(result["status"], "blocked")
        self.assertFalse(result["complete"])
        self.assertEqual(result["details"]["checkpoint"]["pending_ids"], [])
        self.assertEqual(result["details"]["checkpoint"]["blocked_media"][str(self.cid)]["reason"], "expired_non_replayable")
        self.assertEqual(durable_runs.get_run(run_id, db_path=self.db)["status"], "failed")
        self.assertNotIn("next_resume_at", result["details"])
        supplier.assert_not_called()
        local.assert_not_called()
        self.assertEqual(self.fixture.rows("fetch_slots"), slots)
        self.assertEqual(self.fixture.rows("provider_usage"), [])

    def test_restore_prerequisite_enqueues_once_then_reuses_pending_local_run(self):
        local = Mock(side_effect=AssertionError("must await restore"))
        with self.states("restore_required"), patch.object(pipeline, "current_bundle", return_value=self.bundle), \
                patch("v8.media_retention.request_restore", return_value={"run_id": 123, "status": "pending"}) as restore, \
                patch.object(pipeline, "update_content_data", side_effect=AssertionError("must not buy")) as supplier:
            result = pipeline.run_content_batch("content_pipeline", db_path=self.db, at=fixture.AT, local_runner=local)
            self.bundle["state"]["restore_request"] = {"run_id": 123, "status": "pending"}
            again = pipeline.run_content_batch("content_pipeline", db_path=self.db, at=fixture.LATER, local_runner=local)
        self.assertFalse(result["complete"])
        self.assertEqual(again["blocked_media"][str(self.cid)]["restore_run_id"], 123)
        restore.assert_called_once_with(self.cid, self.bundle["bundle_id"], "reprocess", db_path=self.db)
        supplier.assert_not_called()
        local.assert_not_called()
        with self.states("evaluation_pending"):
            self.assertIn(self.cid, [row["id"] for row in pipeline._queue_candidates("content_pipeline", at=fixture.AFTER, db_path=self.db)])

    def test_old_cold_pending_is_not_reserved_forever_after_queuing_restore(self):
        run_id = self.pending_run()
        with self.states("restore_required"), patch.object(pipeline, "current_bundle", return_value=self.bundle), \
                patch("v8.media_retention.request_restore", return_value={"run_id": 123, "status": "pending"}), \
                patch.object(pipeline, "update_content_data", side_effect=AssertionError("must not buy")):
            result = pipeline.run_content_batch("content_pipeline", db_path=self.db, at=fixture.LATER, resume_run_id=run_id)
        self.assertEqual(result["details"]["checkpoint"]["blocked_media"][str(self.cid)]["restore_run_id"], 123)
        with self.states("evaluation_pending"):
            self.assertEqual([row["id"] for row in pipeline._queue_candidates("content_pipeline", at=fixture.AFTER, db_path=self.db)], [self.cid])

    def test_archive_race_after_selection_still_blocks_before_provider_or_local_work(self):
        calls = 0

        def state_view(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            return {self.cid: MediaTerminalDetail("pending", "download_pending" if calls == 1 else "original_unavailable")}

        local = Mock(side_effect=AssertionError("must not recompute"))
        with patch.object(pipeline, "media_terminal_state_details", side_effect=state_view), \
                patch.object(pipeline, "current_bundle", return_value=self.bundle), \
                patch.object(pipeline, "update_content_data", side_effect=AssertionError("must not buy")) as supplier:
            result = pipeline.run_content_batch("content_pipeline", db_path=self.db, at=fixture.AT, local_runner=local)
        self.assertEqual(result["status"], "blocked")
        self.assertFalse(result["complete"])
        supplier.assert_not_called()
        local.assert_not_called()

    def test_expiry_during_local_stage_is_settled_as_blocked_not_retried_forever(self):
        local = Mock(return_value={"terminal_ids": [], "pending_ids": [self.cid],
                                   "blocked_media": {str(self.cid): {"reason": "expired_non_replayable", "bundle_id": self.bundle["bundle_id"]}}})
        with patch.object(pipeline, "update_content_data", return_value={"status": "succeeded"}):
            result = pipeline.run_content_batch("content_pipeline", db_path=self.db, at=fixture.AT, local_runner=local)
        self.assertEqual(result["status"], "blocked")
        self.assertFalse(result["complete"])
        self.assertEqual(result["details"]["checkpoint"]["pending_ids"], [])
        self.assertNotIn("next_resume_at", result["details"])


if __name__ == "__main__":
    unittest.main()
