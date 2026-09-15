from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from v8 import account_intake, account_preparation as prep, capture_runtime
from v8.provider_budget import PaidScope, PaidScopeBlocked, freeze_scope, paid_scope
from v8.storage import connect, initialize_database

AT = "2026-09-12T03:00:00Z"
ACTIVE = {"activation_id": 1, "profile_id": "integrated_route_v1", "roster_snapshot_id": None, "roster_members_sha256": "a"*64}
POLICY = {"account_preparation": prep.CONTRACT}


class AccountPreparationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.db = connect(Path(self.temp.name)/"test.sqlite3"); self.addCleanup(self.db.close)
        initialize_database(self.db, target_version=22)
        self.enterContext(patch.object(prep, "_policy", return_value=POLICY))
        self.enterContext(patch("v8.profile_activations.activation_at", return_value=ACTIVE))
        self.db.execute("BEGIN")

    def submit(self, key="fixture", uid="123456789", **extra):
        return account_intake.submit_account_intake(self.db, request_key=key,
            value={"platform":"douyin", "uid":uid, "account_status":"paused", **extra}, source={"kind":"web"}, at=AT)

    def envelope(self):
        return json.loads(self.db.execute("SELECT envelope_json FROM capture_work_items ORDER BY id DESC").fetchone()[0])

    def scope(self, envelope):
        return PaidScope(purpose="reconcile", **{key:envelope[key] for key in prep.SCOPE_FIELDS})

    def test_uid_only_paused_account_gets_independent_preparation_work(self):
        request = self.submit()
        result = prep.enqueue_pending(self.db, active=ACTIVE, at=AT)
        self.assertEqual(result["created"],1)
        row = self.db.execute("SELECT * FROM capture_work_items").fetchone()
        self.assertIsNone(row["account_id"])
        self.assertEqual(row["intake_request_id"],request["intake_id"])
        self.assertEqual(row["reason"],"provider_transport_blocked")
        self.assertEqual(self.db.execute("SELECT account_status FROM account_directory_rows").fetchone()[0],"paused")
        self.assertEqual(self.db.execute("SELECT count(*) FROM accounts").fetchone()[0],0)

    def test_replanning_and_two_entries_do_not_double_queue_or_change_inputs(self):
        self.submit(); self.submit("excel",phone="00123456789")
        one = prep.enqueue_pending(self.db, active=ACTIVE, at=AT)
        writes = self.db.total_changes
        two = prep.enqueue_pending(self.db, active=ACTIVE, at=AT)
        self.assertEqual((one["created"],two["created"]),(1,0))
        self.assertEqual(self.db.total_changes,writes)

    def test_send_scope_needs_frozen_input_but_not_content_eligibility(self):
        self.submit(); prep.enqueue_pending(self.db,active=ACTIVE,at=AT)
        env = self.envelope(); scope = self.scope(env)
        target = prep.validate_paid_target(self.db,scope,at=AT)
        self.assertEqual(target["subject"],"123456789")
        frozen = freeze_scope(self.db,content_id=None,account_id=None,stage="profile_prepare",
            intake_request_id=env["intake_request_id"],scope=scope)
        self.assertEqual(frozen.preparation_subject,"123456789")
        self.assertIsNone(frozen.account_id)
        self.assertIsNone(frozen.identity_id)
        self.db.execute("UPDATE account_directory_rows SET uid='987654321'")
        with self.assertRaisesRegex(PaidScopeBlocked,"preparation_input_changed"):
            prep.validate_paid_target(self.db,scope,at=AT)

    def test_existing_supported_uids_queue_first_with_stable_order(self):
        values = [{"platform":"kuaishou","uid":"87654321"},
                  {"platform":"douyin","display_account_id":"fixture_handle"},
                  {"platform":"xiaohongshu","uid":"a"*24},
                  {"platform":"douyin","uid":"123456789"}]
        ids = [account_intake.submit_account_intake(self.db,request_key="priority-"+str(i),
            value=value,source={"kind":"fixture"},at=AT)["intake_id"] for i,value in enumerate(values)]
        self.assertEqual(prep.enqueue_pending(self.db,active=ACTIVE,at=AT)["created"],4)
        queued = [row[0] for row in self.db.execute("SELECT intake_request_id FROM capture_work_items ORDER BY id")]
        self.assertEqual(queued,[ids[2],ids[3],ids[0],ids[1]])
        before = self.db.total_changes
        self.assertEqual(prep.enqueue_pending(self.db,active=ACTIVE,at=AT)["created"],0)
        self.assertEqual(self.db.total_changes,before)

    def test_old_policy_or_shadow_cannot_send(self):
        self.submit()
        with patch.object(prep,"_policy",return_value=None):
            result=prep.enqueue_pending(self.db,active=ACTIVE,at=AT)
            self.assertTrue(result["shadow"])
            with self.assertRaisesRegex(PaidScopeBlocked,"preparation_policy_unavailable"):
                prep.validate_paid_target(self.db,self.scope(self.envelope()),at=AT)
        with self.assertRaisesRegex(PaidScopeBlocked,"preparation_plan_changed"):
            prep.validate_paid_target(self.db,self.scope(self.envelope()),at=AT)

    def test_request_mutation_conflict_or_different_subject_prevents_send(self):
        self.submit(); prep.enqueue_pending(self.db,active=ACTIVE,at=AT)
        env=self.envelope(); env["preparation_subject"]="987654321"
        with self.assertRaisesRegex(PaidScopeBlocked,"preparation_step_changed"):
            prep.validate_paid_target(self.db,self.scope(env),at=AT)
        self.db.execute("UPDATE account_intake_requests SET input_json='{}'")
        with self.assertRaisesRegex(PaidScopeBlocked,"preparation_input_changed"):
            prep.validate_paid_target(self.db,self.scope(self.envelope()),at=AT)

    def test_content_or_manual_authority_cannot_be_borrowed(self):
        self.submit();prep.enqueue_pending(self.db,active=ACTIVE,at=AT)
        env=self.envelope()
        with self.assertRaises(PaidScopeBlocked):
            freeze_scope(self.db,content_id=1,account_id=None,stage="profile_prepare",intake_request_id=env["intake_request_id"],scope=self.scope(env))
        with self.assertRaises(PaidScopeBlocked),paid_scope("reconcile",intake_request_id=env["intake_request_id"],catalog_plan_id=1):
            pass

    def test_runtime_dispatch_and_readiness_use_preparation_service(self):
        env={"stage":"profile_prepare"}
        with patch.object(prep,"readiness",return_value=("provider_blocked","fixture")) as readiness:
            self.assertEqual(capture_runtime._readiness(self.db,env,at=AT),("provider_blocked","fixture"))
            readiness.assert_called_once()
        with patch.object(prep,"execute_step",return_value={"fixture":True}) as execute:
            self.assertEqual(capture_runtime._execute_one(env,db_path=Path("fixture"),at=AT),{"fixture":True})
            execute.assert_called_once()


if __name__ == "__main__":
    unittest.main()
