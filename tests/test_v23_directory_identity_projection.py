"""Verified identity projections on real schema23; no network or paid work."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from v8 import account_intake as intake, account_capture_eligibility as eligibility
from v8 import account_catalog_capture as catalog, capture_runtime as runtime
from v8 import capture_planning as planning, platform_adapters as adapters
from v8.account_catalog_capture_release import ACCOUNT_CATALOG_POLICY_V3 as POLICY
from v8.account_directory_reconciliation import reconcile_directory
from v8.statistics_scope import content_statistics_scope_sql
from v8.storage import connect, initialize_database, transaction

AT = "2026-09-12T03:00:00Z"
LATER = "2026-09-12T04:00:00Z"
UIDS = {"douyin": "123456789", "xiaohongshu": "0123456789abcdef01234567",
        "kuaishou": "987654321", "wechat_channels": "v2_0123456789abcdef@finder"}


class DirectoryIdentityProjectionV23Test(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.c = connect(self.root/"fixture.sqlite3"); self.addCleanup(self.c.close)
        initialize_database(self.c, target_version=23)
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))

    def row(self, did):
        return dict(self.c.execute("SELECT * FROM account_directory_rows WHERE id=?", (did,)).fetchone())

    def prepared(self, platform, *, no_directory=False, key=None, apply=True):
        uid = UIDS[platform]
        value = intake.normalize_intake({"platform": platform, "uid": uid, "account_status": "paused"})
        key = key or platform
        if no_directory:
            # A durable accepted journal can precede its first directory (the
            # URL/reference-only entry path). Exercise that real apply branch.
            accepted = intake._save_request(self.c, request_key=key, value=value,
                source={"kind": "web"}, at=AT)
            self.assertIsNone(accepted["directory_row_id"])
        else:
            accepted = intake.submit_account_intake(self.c, request_key=key, value=value,
                source={"kind": "web"}, at=AT)
        request = dict(self.c.execute("SELECT * FROM account_intake_requests WHERE id=?", (accepted["intake_id"],)).fetchone())
        value = json.loads(request["input_json"])
        target = adapters.next_profile_request(value)
        responses = []
        if platform == "wechat_channels":
            # Canonical finder still requires real channel-info evidence before
            # the profile response; this is the same offline adapter chain.
            info = {"code":200,"router":target["path"],"params":target["params"],"data":{
                "baseResponse":{"ret":0},"sections":[{"items":[{"title":"视频号ID","content":"sphabc123"}]}]}}
            info_body=planning.canonical(info).encode();info_path=self.root/(key+"-info.json");info_path.write_bytes(info_body)
            info_id=self.c.execute("""INSERT INTO provider_raw_responses(intake_request_id,provider,operation,
                local_path,sha256,byte_size,http_status,captured_at) VALUES(?,'TikHub',?,?,?,?,200,?)""",
                (request["id"],target["operation"],str(info_path),hashlib.sha256(info_body).hexdigest(),len(info_body),AT)).lastrowid
            responses.append({"operation":target["operation"],"raw_response_id":info_id,"payload":info})
            target=adapters.next_profile_request(value,responses)
        if platform == "douyin":
            data = {"status_code": 0, "data": {"id_str": uid, "nickname": "fixture", "sec_uid": "MS4wLjAB"+"A"*64}}
        elif platform == "xiaohongshu":
            data = {"code": 0, "success": True, "data": {"userid": uid, "nickname": "fixture", "red_id": "fixture_id",
                "result": {"code": 0, "success": True}}}
        elif platform == "kuaishou":
            data = {"result": 1, "userProfile": {"profile": {"user_id": int(uid), "user_name": "fixture"}, "ownerCount": {"fan": 0}}}
        else:
            data = {"baseResponse": {"ret": 0}, "contact": {"username": uid, "nickname": "fixture"}}
        payload = {"code": 200, "router": target["path"], "params": target["params"], "data": data}
        body = planning.canonical(payload).encode()
        path = self.root/(key+".json"); path.write_bytes(body)
        raw = self.c.execute("""INSERT INTO provider_raw_responses(intake_request_id,provider,operation,
            local_path,sha256,byte_size,http_status,captured_at) VALUES(?,'TikHub',?,?,?,?,200,?)""",
            (request["id"],target["operation"],str(path),hashlib.sha256(body).hexdigest(),len(body),AT)).lastrowid
        responses.append({"operation":target["operation"],"raw_response_id":raw,"payload":payload})
        self.assertIsNone(adapters.next_profile_request(value, responses))
        profile = adapters.normalize_profile(platform, value, payload, prior_responses=responses)
        if not apply:
            return accepted,path,profile,raw
        result = intake.apply_prepared_profile(self.c, request["id"], profile, raw, LATER)
        self.assertTrue(eligibility.identity_capture_evidence(self.c, result["account_identity_id"])["eligible"])
        return result, path, profile, raw

    def content(self, result):
        cid = self.c.execute("""INSERT INTO content_items(link_id,platform,platform_content_id,canonical_url,
            account_id,raw_account_uid,content_type,published_at,imported_at,created_at,updated_at)
            VALUES(?,?,?,?,?,?,'video',?,?,?,?)""", (f"T{result['account_id']:05d}",result["platform"],"123456789",
            "https://fixture.invalid/"+result["platform"],result["account_id"],result["uid"],AT,AT,AT,AT)).lastrowid
        return self.c.execute("SELECT * FROM content_items WHERE id=?", (cid,)).fetchone()

    def visible(self, cid):
        return self.c.execute("SELECT count(*) FROM content_items c WHERE c.id=? AND "+
            content_statistics_scope_sql(connection=self.c), (cid,)).fetchone()[0] == 1

    def legacy(self, platform):
        result, path, profile, raw = self.prepared(platform)
        self.c.execute("UPDATE account_directory_rows SET identity_status='uid_unverified',updated_at=? WHERE id=?",
            (LATER,result["directory_row_id"]))
        return result,path,profile,raw

    def test_four_platform_new_directory_after_profile_is_verified_and_visible(self):
        for platform in UIDS:
            with self.subTest(platform=platform), transaction(self.c):
                result,_,profile,raw = self.prepared(platform, no_directory=True)
                self.assertEqual(self.row(result["directory_row_id"])["identity_status"],"existing_verified")
                self.assertTrue(self.visible(self.content(result)["id"]))
                before = self.c.total_changes
                self.assertTrue(intake.apply_prepared_profile(self.c,result["intake_id"],profile,raw,LATER)["replayed"])
                self.assertEqual(self.c.total_changes,before)

    def test_four_platform_existing_directory_profile_is_verified_and_visible(self):
        for platform in UIDS:
            with self.subTest(platform=platform), transaction(self.c):
                result,*_ = self.prepared(platform)
                self.assertEqual(self.row(result["directory_row_id"])["identity_status"],"existing_verified")
                self.assertTrue(self.visible(self.content(result)["id"]))

    def test_uid_only_import_cannot_upgrade_directory_or_visibility(self):
        with transaction(self.c):
            for platform,uid in UIDS.items():
                result = intake.submit_account_intake(self.c,request_key=platform,value={"platform":platform,"uid":uid},
                    source={"kind":"excel"},at=AT)
                self.assertEqual(result["status"],"accepted")
                self.assertNotEqual(self.row(result["directory_row_id"])["identity_status"],"existing_verified")
            self.assertEqual(eligibility.derive_capture_eligibility(self.c)["eligible_members"],[])
            reconcile_directory(self.c,at=LATER)
            self.assertEqual(self.c.execute("SELECT count(*) FROM account_directory_rows WHERE identity_status='existing_verified'").fetchone()[0],0)

    def test_legacy_ready_projection_preserves_paused_fields_and_replays_with_zero_writes(self):
        with transaction(self.c):
            results = [self.legacy(platform)[0] for platform in UIDS]
            contents = [self.content(result) for result in results]
            before = {r["directory_row_id"]: self.row(r["directory_row_id"]) for r in results}
            journals = [tuple(r) for r in self.c.execute("SELECT * FROM account_intake_requests ORDER BY id")]
            raw = [tuple(r) for r in self.c.execute("SELECT * FROM provider_raw_responses ORDER BY id")]
            self.assertTrue(all(not self.visible(c["id"]) for c in contents))
            with patch.object(eligibility,"derive_capture_eligibility",wraps=eligibility.derive_capture_eligibility) as derive:
                result = reconcile_directory(self.c,at="2026-09-12T05:00:00Z")
            self.assertEqual(derive.call_count,1)
            self.assertEqual(sum(bool(r.get("identity_projection_updated")) for r in result["rows"]),4)
            for did,original in before.items():
                current=self.row(did)
                self.assertEqual(current["account_status"],"paused")
                self.assertEqual({k:v for k,v in current.items() if k not in {"identity_status","updated_at"}},
                    {k:v for k,v in original.items() if k not in {"identity_status","updated_at"}})
            self.assertTrue(all(self.visible(c["id"]) for c in contents))
            self.assertEqual([tuple(r) for r in self.c.execute("SELECT * FROM account_intake_requests ORDER BY id")],journals)
            self.assertEqual([tuple(r) for r in self.c.execute("SELECT * FROM provider_raw_responses ORDER BY id")],raw)
            changes=self.c.total_changes
            self.assertEqual(reconcile_directory(self.c,at=LATER)["sql_writes"],0)
            self.assertEqual(self.c.total_changes,changes)
            self.assertEqual(self.c.execute("SELECT count(*) FROM provider_usage").fetchone()[0],0)

    def test_ready_label_with_missing_or_corrupt_raw_does_not_upgrade(self):
        for platform in UIDS:
            with self.subTest(platform=platform), transaction(self.c):
                result,path,_,_=self.legacy(platform)
                path.write_text("{}")
                reconcile_directory(self.c,at=LATER)
                self.assertEqual(self.row(result["directory_row_id"])["identity_status"],"uid_unverified")
                self.assertFalse(eligibility.identity_capture_evidence(self.c,result["account_identity_id"])["eligible"])
                path.unlink()
                reconcile_directory(self.c,at=LATER)
                self.assertEqual(self.row(result["directory_row_id"])["identity_status"],"uid_unverified")

    def test_conflicting_directory_binding_cannot_upgrade(self):
        with transaction(self.c):
            result,*_=self.legacy("kuaishou")
            self.c.execute("UPDATE account_directory_rows SET uid='11223344' WHERE id=?",(result["directory_row_id"],))
            reconcile_directory(self.c,at=LATER)
            self.assertEqual(self.row(result["directory_row_id"])["identity_status"],"uid_unverified")

    def test_xhs_projection_never_bypasses_prepared_raw_or_reference_after_upgrade(self):
        with transaction(self.c):
            result,path,_,raw=self.legacy("xiaohongshu")
            reconcile_directory(self.c,at=LATER)
            self.assertEqual(self.row(result["directory_row_id"])["identity_status"],"existing_verified")
            self.assertEqual(eligibility.identity_capture_evidence(self.c,result["account_identity_id"])["locator_evidence"]["kind"],"prepared_profile_chain")
            original=path.read_bytes()
            for missing in (False,True):
                with self.subTest(raw_missing=missing):
                    if missing:path.unlink()
                    else:path.write_text("{}")
                    self.assertFalse(eligibility.identity_capture_evidence(self.c,result["account_identity_id"])["eligible"])
                    path.write_bytes(original)
            self.c.execute("DELETE FROM account_provider_references WHERE account_identity_id=?",(result["account_identity_id"],))
            self.assertFalse(eligibility.identity_capture_evidence(self.c,result["account_identity_id"])["eligible"])

    def test_legacy_verified_xhs_without_preparation_preserves_original_contract(self):
        with transaction(self.c):
            result,*_=self.prepared("xiaohongshu")
            # Build the old installed-directory shape in this disposable DB:
            # no intake journal or profile raw exists for this identity.
            self.c.execute("DELETE FROM account_provider_references")
            self.c.execute("DELETE FROM provider_raw_responses")
            self.c.execute("DELETE FROM account_intake_requests")
            proof=eligibility.identity_capture_evidence(self.c,result["account_identity_id"])
            self.assertTrue(proof["eligible"])
            self.assertEqual(proof["locator_evidence"]["kind"],"verified_directory_identity")

    def test_xhs_prepared_origin_does_not_fallback_when_result_uid_or_input_hash_changes(self):
        with transaction(self.c):
            result,*_=self.legacy("xiaohongshu")
            reconcile_directory(self.c,at=LATER)
            for column,value in (("result_json",None),("input_sha256","0"*64)):
                with self.subTest(column=column):
                    self.c.execute("SAVEPOINT changed_input")
                    try:
                        if column=="result_json":
                            row=self.c.execute("SELECT result_json FROM account_intake_requests WHERE id=?",(result["intake_id"],)).fetchone()
                            changed=json.loads(row[0]);changed["uid"]="f"*24;value=planning.canonical(changed)
                        self.c.execute("UPDATE account_intake_requests SET "+column+"=? WHERE id=?",(value,result["intake_id"]))
                        self.assertFalse(eligibility.identity_capture_evidence(self.c,result["account_identity_id"])["eligible"])
                    finally:
                        self.c.execute("ROLLBACK TO changed_input");self.c.execute("RELEASE changed_input")

    def test_pending_failed_or_other_directory_intake_does_not_replace_legacy_xhs_contract(self):
        with transaction(self.c):
            result,*_=self.prepared("xiaohongshu")
            self.c.execute("DELETE FROM account_provider_references")
            self.c.execute("DELETE FROM provider_raw_responses")
            for status,action,completed,did in (("accepted","accepted",None,result["directory_row_id"]),
                    ("blocked","accepted",LATER,result["directory_row_id"]),("ready","prepared",LATER,None)):
                with self.subTest(status=status,directory=did):
                    self.c.execute("UPDATE account_intake_requests SET result_json=?,completed_at=?,directory_row_id=? WHERE id=?",
                        (planning.canonical({"status":status,"action":action}),completed,did,result["intake_id"]))
                    proof=eligibility.identity_capture_evidence(self.c,result["account_identity_id"])
                    self.assertTrue(proof["eligible"])
                    self.assertEqual(proof["locator_evidence"]["kind"],"verified_directory_identity")

    def test_later_profile_application_failure_rolls_back_verification_and_identity_creation(self):
        with transaction(self.c):
            request,_,profile,raw=self.prepared("kuaishou",no_directory=True,apply=False)
            before={table:[tuple(row) for row in self.c.execute("SELECT * FROM "+table)]
                for table in ("accounts","account_platform_identities","account_directory_rows","account_intake_requests")}
            with patch("v8.account_reference_storage.store_reference",side_effect=RuntimeError("offline store failure")), \
                    self.assertRaisesRegex(RuntimeError,"offline store failure"):
                intake.apply_prepared_profile(self.c,request["intake_id"],profile,raw,LATER)
            self.assertEqual({table:[tuple(row) for row in self.c.execute("SELECT * FROM "+table)] for table in before},before)

    def test_projection_keeps_existing_ordinary_work_and_frozen_plan_executable(self):
        for platform in UIDS:
            with self.subTest(platform=platform), transaction(self.c), patch.object(catalog,"installed_policy",return_value=POLICY):
                result,*_=self.legacy(platform)
                content=self.content(result)
                snapshot=catalog.freeze_snapshot(self.c,policy=POLICY)
                members=snapshot["eligibility"]["eligible_members"]
                member=next(m for m in members if m["identity_id"]==result["account_identity_id"])
                active={"activation_id":1,"activation_sha256":"b"*64,"profile_id":"integrated_route_v1",
                    "roster_snapshot_id":1,"roster_members_sha256":"a"*64}
                body={"contract_version":runtime.CONTRACT,**active,"business_day":"2026-09-12","shadow":False,
                    "catalog_mode":"active","catalog_snapshot":snapshot,"catalog_snapshot_sha256":snapshot["snapshot_sha256"],"cohort":members}
                change=self.c.execute("INSERT INTO routing_input_changes(change_kind,payload_json,effective_at,recorded_at,change_sha256) VALUES('policy','{}',?,?,?)",(AT,AT,planning.digest(body))).lastrowid
                pid=self.c.execute("INSERT INTO capture_source_plans(roster_change_id,business_day,generation,mode,payload_json,created_at,plan_sha256) VALUES(?,'2026-09-12',1,'active',?,?,?)",(change,planning.canonical(body),AT,planning.digest(body))).lastrowid
                plan={"id":pid,**body}
                operation=runtime.providers.STAGE_CONFIG[(platform,"detail")][2]
                planning.assign_route(self.c,scope_type="account",scope_key="catalog-account:"+str(result["account_id"]),
                    provider="tikhub",operation=operation,expected_generation=0,route="integrated",mode="active",
                    effective_at=AT,recorded_at=AT,account_id=result["account_id"])
                self.assertTrue(runtime._enqueue(self.c,plan,member,stage="detail",operation=operation,
                    logical_due="lifetime",at=AT,content=content))
                work=dict(self.c.execute("SELECT * FROM capture_work_items WHERE content_id=?",(content["id"],)).fetchone())
                env=json.loads(work["envelope_json"])
                before=catalog.validate_plan_member(self.c,pid,member["identity_id"],at=AT,policy=POLICY)
                readiness=runtime._readiness(self.c,env,at=AT)
                self.assertEqual(readiness,("provider_blocked","provider_transport_blocked"))
                reconcile_directory(self.c,at=LATER)
                after=catalog.validate_plan_member(self.c,pid,member["identity_id"],at=LATER,policy=POLICY)
                self.assertEqual({k:before[k] for k in catalog.IDENTITY_KEYS},{k:after[k] for k in catalog.IDENTITY_KEYS})
                self.assertEqual(runtime._readiness(self.c,env,at=LATER),readiness)
                with catalog.planning_validation(self.c,POLICY,snapshot,plan=plan):
                    self.assertEqual(runtime._readiness(self.c,env,at=LATER),readiness)
                self.assertFalse(runtime._enqueue(self.c,plan,member,stage="detail",operation=operation,
                    logical_due="lifetime",at=LATER,content=content))
                self.assertEqual(dict(self.c.execute("SELECT * FROM capture_work_items WHERE id=?",(work["id"],)).fetchone()),work)
                saved=self.c.execute("SELECT payload_json,plan_sha256 FROM capture_source_plans WHERE id=?",(pid,)).fetchone()
                self.assertEqual(tuple(saved),(planning.canonical(body),planning.digest(body)))


if __name__ == "__main__":
    unittest.main()
