"""True schema23 coverage: typed preparation lineage and immutable v1 readers."""
from __future__ import annotations

import copy
import json
import unittest

from tests import test_v23_runtime_coverage_staleness as fixtures
from v8 import capture_day_coverage as coverage, capture_planning as planning
from v8 import platform_adapters, runtime_receipts


class CatalogPlanTypesV23Test(unittest.TestCase):
    def setUp(self):
        self.fixture = f = fixtures.RuntimeCoverageStalenessV23Test(methodName="runTest")
        f.setUp(); self.addCleanup(f.doCleanups)
        self.c, self.cutoff, self.day = f.c, f.cutoff, f.fixture.day
        self.active = f.fixture.active
        self.at = "2026-09-08T04:00:00Z"

    def insert(self, table, value, connection=None):
        connection = self.c if connection is None else connection
        return connection.execute(f"INSERT INTO {table}({','.join(value)}) VALUES ({','.join('?' for _ in value)})", tuple(value.values())).lastrowid

    def add_preparation(self, *, connection=None, with_work=True, shadow=False):
        connection = self.c if connection is None else connection
        target = platform_adapters.request("douyin_uid_profile", {"uid":"123456789"}, platform="douyin", subject="123456789")
        input_value = {"platform":"douyin", "uid":"123456789"}
        input_sha, key = planning.digest(input_value), "a"*64
        ident = 1
        if with_work:
            ident = self.insert("account_intake_requests", {
                "request_key":"coverage-preparation", "input_sha256":input_sha, "preparation_key":key,
                "platform":"douyin", "input_json":planning.canonical(input_value), "source_json":"{}",
                "created_at":self.at, "updated_at":self.at}, connection)
        member = {"intake_request_id":ident,"input_sha256":input_sha,"preparation_key":key,"target":target,
                  "attempt":{"generation":0,"due_at":self.at}}
        payload = {"contract":coverage.PREPARATION_CONTRACT,"members":[member],"shadow":shadow,"policy_sha256":"b"*64,
                   **{k:self.active[k] for k in ("activation_id","profile_id","roster_snapshot_id","roster_members_sha256")}}
        template = dict(self.fixture.template); template.pop("id")
        template.update(payload_json=planning.canonical(payload), plan_sha256=planning.digest(payload),
                        business_day=self.day,created_at=self.at, mode="shadow" if shadow else "active",generation=999)
        plan = self.insert("capture_source_plans",template,connection)
        if not with_work:
            return plan,None,None
        assignment = planning.assign_route(connection, scope_type="intake",scope_key=str(ident),provider="tikhub",
            operation=target["operation"],expected_generation=0,route="integrated",mode="shadow" if shadow else "active",
            effective_at=self.at,recorded_at=self.at,intake_request_id=ident,source_plan_id=plan)
        env = {"contract_version":coverage.PREPARATION_CONTRACT,"stage":"profile_prepare","capture_stage":"profile_prepare",
            "category":"reconcile","account_id":None,"content_id":None,"identity_id":None,"uid":None,"platform":"douyin",
            "intake_request_id":ident,"preparation_plan_id":plan,"preparation_key":key,"preparation_revision":ident,
            "preparation_subject":target["subject"],"request":target,"operation":target["operation"],
            "preparation_attempt_generation":0,"assignment_id":assignment,"source_plan_id":plan,
            "logical_due":"prepare:"+key+":"+planning.digest(target)+":revision:"+str(ident),
            **{k:payload[k] for k in ("activation_id","profile_id","roster_snapshot_id","roster_members_sha256")}}
        work = self.insert("capture_work_items", {"work_identity":planning.digest(env),"assignment_id":assignment,
            "source_plan_id":plan,"intake_request_id":ident,"provider":"tikhub","operation":target["operation"],
            "due_at":self.at,"data_business_day":self.day,"state":"runnable","reason":"provider_transport_blocked",
            "envelope_json":planning.canonical(env),"created_at":self.at,"updated_at":self.at},connection)
        return plan,work,assignment

    def read(self):
        before=self.c.total_changes
        result=coverage.catalog_day_coverage(self.c,day=self.day,cutoff_at=self.cutoff)
        self.assertEqual(self.c.total_changes,before)
        self.assertEqual(result["source_binding"]["contract"],coverage.TYPED_SOURCE_CONTRACT)
        return result

    def test_legitimate_preparation_and_pending_work_do_not_invalidate_complete_discovery(self):
        plan,work,_=self.add_preparation()
        result=self.read();self.assertTrue(result["complete"],result)
        binding=result["source_binding"]
        self.assertEqual(binding["preparation_inputs"][0]["plan"]["id"],plan)
        self.assertEqual(binding["preparation_inputs"][0]["work_bindings"][0]["work_scope"]["id"],work)
        self.assertNotIn(plan,[r["id"] for r in binding["plans"]])
        self.assertIn(plan,[r["id"] for r in binding["plan_inputs"]])
        self.assertTrue(coverage.validate_source_binding(self.c,binding,self.cutoff)["valid"])

    def test_shadow_preparation_is_typed_but_does_not_create_discovery_obligations(self):
        self.add_preparation(shadow=True)
        result=self.read();self.assertTrue(result["complete"],result)
        self.assertTrue(coverage.validate_source_binding(self.c,result["source_binding"],self.cutoff)["valid"])

    def test_valid_preparation_cannot_mask_a_damaged_real_catalog_plan(self):
        self.add_preparation()
        baseline=self.c.execute("SELECT id FROM capture_source_plans WHERE business_day<'2026-09-08' ORDER BY id LIMIT 1").fetchone()[0]
        self.fixture.drop_triggers("capture_source_plans")
        self.c.execute("UPDATE capture_source_plans SET payload_json='{}' WHERE id=?",(baseline,))
        result=self.read()
        self.assertEqual(result["reason"],"catalog_plan_invalid")
        self.assertFalse(result["complete"])

    def test_native_source_revision_dispatches_v2_without_reinterpreting_v1(self):
        self.add_preparation()
        day=self.read()
        legacy=self.fixture.sealed
        aggregate=copy.deepcopy(legacy["summary"]["coverage"])
        aggregate["days"]=[day]
        active={k:legacy["scope"][k] for k in ("activation_id","activation_sha256","profile_id",
            "source_family","roster_snapshot_id","roster_snapshot_hash")}
        revision,binding=runtime_receipts._compact_coverage_source_revision(self.c,coverage=aggregate,
            active=active,business_day=self.day,cutoff_at=self.cutoff)
        self.assertEqual(binding["catalog"]["contract"],coverage.TYPED_SOURCE_CONTRACT)
        self.assertEqual(len(revision),64)
        self.assertEqual(runtime_receipts._compact_coverage_source_revision(self.c,coverage=aggregate,
            active=active,business_day=self.day,cutoff_at=self.cutoff,frozen_binding=binding),(revision,binding))
        damaged=copy.deepcopy(binding);damaged["catalog"]["preparation_inputs"]=[]
        with self.assertRaisesRegex(runtime_receipts.RuntimeReceiptError,"frozen source binding mismatch"):
            runtime_receipts._compact_coverage_source_revision(self.c,coverage=aggregate,
                active=active,business_day=self.day,cutoff_at=self.cutoff,frozen_binding=damaged)

    def test_bad_catalog_cannot_hide_as_a_preparation_or_unknown_contract(self):
        plan,_,_=self.add_preparation(with_work=False)
        for mutation in (lambda p:p.update(catalog_mode="active"),lambda p:p.update(contract="unknown-v1"),
                         lambda p:p.update(cohort=[])):
            with self.subTest(mutation=mutation):
                self.c.execute("SAVEPOINT corrupt_plan")
                try:
                    self.fixture.drop_triggers("capture_source_plans")
                    payload=json.loads(self.c.execute("SELECT payload_json FROM capture_source_plans WHERE id=?",(plan,)).fetchone()[0])
                    mutation(payload)
                    self.c.execute("UPDATE capture_source_plans SET payload_json=?,plan_sha256=? WHERE id=?",(planning.canonical(payload),planning.digest(payload),plan))
                    result=self.read();self.assertFalse(result["known"],result);self.assertFalse(result["complete"])
                finally:self.c.execute("ROLLBACK TO corrupt_plan");self.c.execute("RELEASE corrupt_plan")

    def test_wrong_hash_or_member_type_never_gets_excluded(self):
        plan,_,_=self.add_preparation(with_work=False)
        for changed in ("plan_sha256='"+"0"*64+"'", "payload_json=json_set(payload_json,'$.members','invalid')"):
            with self.subTest(changed=changed):
                self.c.execute("SAVEPOINT corrupt_type")
                try:
                    self.fixture.drop_triggers("capture_source_plans")
                    self.c.execute("UPDATE capture_source_plans SET "+changed+" WHERE id=?",(plan,))
                    result=self.read();self.assertEqual(result["reason"],"catalog_preparation_plan_invalid");self.assertFalse(result["complete"])
                finally:self.c.execute("ROLLBACK TO corrupt_type");self.c.execute("RELEASE corrupt_type")

    def test_preparation_work_stage_request_and_assignment_must_remain_bound(self):
        _,work,assignment=self.add_preparation();binding=self.read()["source_binding"]
        changes=(
            ("capture_work_items",work,"envelope_json=json_set(envelope_json,'$.stage','discovery')"),
            ("capture_work_items",work,"envelope_json=json_set(envelope_json,'$.request.subject','different')"),
            ("capture_work_items",work,"envelope_json=json_set(envelope_json,'$.logical_due','different')"),
            ("capture_route_assignments",assignment,"scope_key='different'"),
        )
        for table,identity,change in changes:
            with self.subTest(table=table,change=change):
                self.c.execute("SAVEPOINT changed_work")
                try:
                    self.fixture.drop_triggers(table)
                    self.c.execute(f"UPDATE {table} SET {change} WHERE id=?",(identity,))
                    with self.assertRaises(ValueError):coverage.validate_source_binding(self.c,binding,self.cutoff)
                    if 'logical_due' not in change:
                        self.assertFalse(self.read()["complete"])
                finally:self.c.execute("ROLLBACK TO changed_work");self.c.execute("RELEASE changed_work")

    def test_legacy_v1_unknown_is_unchanged_after_real_migration_to_schema23(self):
        old=self.fixture.fixture.connection
        self.add_preparation(connection=old,with_work=False)
        legacy=coverage.catalog_day_coverage(old,day=self.day,cutoff_at=self.cutoff)
        self.assertEqual(old.execute("PRAGMA user_version").fetchone()[0],20)
        self.assertEqual(legacy["source_binding"]["contract"],coverage.SOURCE_CONTRACT)
        self.assertEqual(legacy["reason"],"catalog_plan_invalid")
        self.add_preparation(with_work=False)
        before=copy.deepcopy(legacy["source_binding"])
        verdict=coverage.validate_source_binding(self.c,legacy["source_binding"],self.cutoff)
        self.assertFalse(verdict["known"]);self.assertEqual(verdict["reason"],"catalog_plan_invalid")
        self.assertEqual(legacy["source_binding"],before)
        self.assertTrue(self.read()["complete"])

    def test_v2_receipt_cannot_be_consumed_by_schema20(self):
        binding=self.read()["source_binding"]
        with self.assertRaisesRegex(ValueError,"catalog_source_binding_invalid"):
            coverage.validate_source_binding(self.fixture.fixture.connection,binding,self.cutoff)

    def test_same_cutoff_preparation_append_is_display_only_and_proven(self):
        binding=self.read()["source_binding"]
        self.at=self.cutoff
        self.add_preparation()
        with self.assertRaisesRegex(ValueError,"catalog_source_plan_inputs_changed"):
            coverage.validate_source_binding(self.c,binding,self.cutoff)
        verdict=coverage.validate_source_binding(self.c,binding,self.cutoff,allow_appended_inputs=True)
        self.assertTrue(verdict["appended_inputs"])


if __name__ == '__main__':
    unittest.main()
