"""Installed F8/F9 preparation recovery with schema23 authority and lock boundaries."""
from __future__ import annotations

from threading import get_ident
import unittest
from unittest.mock import patch

from tests import test_v8_account_preparation as preparation_fixture
from tests import test_v8_preparation_never_sent_recovery as unsent_fixture
from tests import test_v8_preparation_profile_reuse as reuse_fixture
from tests import test_v8_resolver_parser_replay as resolver_fixture
from v8 import account_capture_eligibility, account_preparation as prep, capture_plan_reuse
from v8 import capture_runtime as runtime, raw_archive, storage
from v8.provider_budget import PaidScopeBlocked
from v8.storage import initialize_database, transaction

ACTIVE, LATER = reuse_fixture.ACTIVE, reuse_fixture.LATER


class ProfileReuseV23Test(unittest.TestCase):
    def setUp(self):
        self.fx = reuse_fixture.PreparationProfileReuseTest()
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.db = self.fx.db
        initialize_database(self.db, target_version=23)
        self.assertEqual(self.db.execute("PRAGMA user_version").fetchone()[0], 23)

    def proof(self):
        self.assertFalse(self.db.in_transaction)
        self.db.execute("BEGIN")
        try:
            return prep.prepare_profile_reuse(self.db, active=ACTIVE, at=LATER)
        finally:
            self.db.rollback()

    def test_profile_evidence_is_read_outside_writer_and_reused_without_raw_reads_inside(self):
        self.fx.pending()
        self.fx.restore()
        original = raw_archive.read_response_entity
        def read_without_writer(*args, **kwargs):
            self.assertNotEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, get_ident())
            return original(*args, **kwargs)
        with patch.object(raw_archive, "read_response_entity", side_effect=read_without_writer) as reads:
            proof = self.proof()
        self.assertGreater(reads.call_count, 0)
        self.assertIn(self.fx.base.iid, proof["members"])
        with transaction(self.db), \
             patch.object(raw_archive, "read_response_entity", side_effect=AssertionError("raw bytes under writer lock")), \
             patch.object(account_capture_eligibility, "derive_capture_eligibility", side_effect=AssertionError("rescan under writer lock")):
            result = prep.enqueue_pending(self.db, active=ACTIVE, at=LATER, reuse_proof=proof)
        self.assertEqual((result["created"], result["pending"], result["reused"]), (0, 0, 1))
        self.assertEqual(self.fx.result()["status"], "ready")
        self.assertEqual(self.db.execute("SELECT state,reason FROM capture_work_items").fetchone()[:],
            ("terminal", "existing_profile_evidence_reused"))
        proof = self.proof()
        with transaction(self.db):
            before = self.db.total_changes
            self.assertEqual(prep.enqueue_pending(self.db, active=ACTIVE, at=LATER, reuse_proof=proof)["created"], 0)
            self.assertEqual(self.db.total_changes, before)

    def test_changed_catalog_revision_is_rejected_before_any_write_or_reuse(self):
        self.fx.pending()
        self.fx.restore()
        proof = self.proof()
        with transaction(self.db):
            self.db.execute("UPDATE accounts SET enabled=0 WHERE id=?", (self.fx.base.aid,))
        with transaction(self.db):
            before = self.db.total_changes
            with self.assertRaisesRegex(PaidScopeBlocked, "preparation_reuse_proof_changed"):
                prep.enqueue_pending(self.db, active=ACTIVE, at=LATER, reuse_proof=proof)
            self.assertEqual(self.db.total_changes, before)
            self.assertEqual(self.fx.result()["status"], "accepted")

    def test_changed_activation_or_planning_time_cannot_borrow_profile_proof(self):
        self.fx.pending()
        self.fx.restore()
        proof = self.proof()
        for active, at in (({**ACTIVE, "activation_id": 2}, LATER), (ACTIVE, "2026-09-12T01:01:00Z")):
            with self.subTest(active=active, at=at), transaction(self.db):
                before = self.db.total_changes
                with self.assertRaisesRegex(PaidScopeBlocked, "preparation_reuse_proof_changed"):
                    prep.enqueue_pending(self.db, active=active, at=at, reuse_proof=proof)
                self.assertEqual(self.db.total_changes, before)

    def test_missing_proof_and_corrupt_raw_do_not_finish_or_scan_inside_writer(self):
        self.fx.pending()
        proof = self.proof()
        self.assertEqual(proof["members"], {})
        for provided in (proof, None):
            with self.subTest(provided=provided is not None), transaction(self.db), \
                 patch.object(account_capture_eligibility, "derive_capture_eligibility", side_effect=AssertionError("writer rescan")):
                self.assertNotIn("reused", prep.enqueue_pending(self.db, active=ACTIVE, at=LATER, reuse_proof=provided))
                self.assertEqual(self.fx.result()["status"], "accepted")

    def test_started_work_cannot_finish_using_fresh_profile_proof(self):
        self.fx.pending()
        self.fx.restore()
        proof = self.proof()
        with transaction(self.db):
            self.db.execute("UPDATE capture_work_items SET attempt_count=1")
            result = prep.enqueue_pending(self.db, active=ACTIVE, at=LATER, reuse_proof=proof)
        self.assertNotIn("reused", result)
        self.assertEqual(self.fx.result()["status"], "accepted")

    def test_real_plan_tick_passes_fresh_read_snapshot_proof_into_short_writer(self):
        self.fx.pending()
        self.fx.restore()
        original_prepare, original_enqueue = prep.prepare_profile_reuse, prep.enqueue_pending
        def prepare_outside_writer(*args, **kwargs):
            self.assertNotEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, get_ident())
            return original_prepare(*args, **kwargs)
        def enqueue_without_scan(*args, **kwargs):
            self.assertEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, get_ident())
            self.assertIn(self.fx.base.iid, kwargs["reuse_proof"]["members"])
            with patch.object(account_capture_eligibility, "derive_capture_eligibility", side_effect=AssertionError("writer rescan")), \
                 patch.object(raw_archive, "read_response_entity", side_effect=AssertionError("writer raw read")):
                return original_enqueue(*args, **kwargs)
        with patch.object(runtime, "activation_at", return_value=ACTIVE), \
             patch.object(prep, "prepare_profile_reuse", side_effect=prepare_outside_writer) as prepared, \
             patch.object(prep, "enqueue_pending", side_effect=enqueue_without_scan), \
             patch.object(capture_plan_reuse, "ensure", side_effect=capture_plan_reuse.PlanDeferred("fixture_after_preparation")):
            result = runtime._plan_tick_v23(self.fx.base.db, at=LATER, shadow=False)
        self.assertEqual(prepared.call_count, 1)
        self.assertEqual(result["preparation"]["reused"], 1)
        self.assertEqual(self.fx.result()["status"], "ready")


class ResolverParserReplayV23Test(resolver_fixture.ResolverParserReplayTest):
    def setUp(self):
        with patch.object(preparation_fixture, "initialize_database",
                side_effect=lambda c, **kwargs: initialize_database(c, target_version=23)):
            super().setUp()
        self.assertEqual(self.db.execute("PRAGMA user_version").fetchone()[0], 23)


class NeverSentPreparationRecoveryV23Test(unsent_fixture.NeverSentPreparationRecoveryTest):
    def setUp(self):
        with patch.object(unsent_fixture, "initialize_database",
                side_effect=lambda c, **kwargs: initialize_database(c, target_version=23)):
            super().setUp()
        self.assertEqual(self.db.execute("PRAGMA user_version").fetchone()[0], 23)


if __name__ == "__main__":
    unittest.main()
