"""Real schema23 work/slot/claim persistence; provider boundary is local only."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

from tests import test_v8_source_routing as fixture
from tests.test_v8_kuaishou_adapter import detail_payload
from tests.test_v8_wechat_channels_adapter import OBJECT, UID, response, video
from v8 import capture, capture_planning as planning, capture_runtime as runtime
from v8 import capture_shared_requests as shared, providers, raw_evidence
from v8.storage import connect, initialize_database, transaction

NOW = "2026-08-29T04:00:00Z"
CREATED = "2026-08-29T03:01:00Z"


class SharedRequestOwnershipTest(unittest.TestCase):
    def setUp(self):
        self.fx = fixture.SourceRoutingTest()
        with patch.object(fixture,"initialize_database",side_effect=lambda c:initialize_database(c,target_version=23)):
            self.fx.setUp()
        self.addCleanup(self.fx.tearDown)
        self.db, self.c = self.fx.db.resolve(),self.fx.connection
        self.enterContext(patch("socket.socket.connect",side_effect=AssertionError("network forbidden")))
        store = providers._store_stage_result
        self.enterContext(patch.object(providers,"_store_stage_result",side_effect=lambda *a,**kw:store(*a,**kw,media_root=self.db.parent/"media")))

    def work(self,platform,stage,*,due=None,state="running",kind=None,created=CREATED):
        cid = 1 if platform == "kuaishou" else 2
        identifier,uid = ("5234567890123456789","001234") if platform == "kuaishou" else (OBJECT,UID)
        group = runtime.load_policy()["metric_supplement_groups"][platform][0]["name"]
        due = due or ("lifetime" if stage == "detail" else "metrics:2026-08-29T03:00:00Z:"+group)
        operation = providers.STAGE_CONFIG[(platform,stage)][2]
        env = {"stage":stage,"source_stage":stage,"platform":platform,"content_id":cid,
            "operation":operation,"uid":uid,"logical_due":due,"cursor":None,
            "seen_cursors":[],"page_count":0,"raw_ids":[]}
        if kind:
            env.update(kind=kind,manual_command_run_id=100,manual_command_run_ids=[100])
        identity = shared._identity(env)
        with transaction(self.c):
            self.c.execute("UPDATE content_items SET platform=?,platform_content_id=?,canonical_url=?,raw_account_uid=?,content_type='video',title='Existing title',body='Existing body' WHERE id=?",
                (platform,identifier,"https://www.kuaishou.com/short-video/3xwork" if platform == "kuaishou" else f"https://channels.weixin.qq.com/video/{OBJECT}?object_nonce_id=12345",uid,cid))
            assignment_row = self.c.execute("SELECT id FROM capture_route_assignments WHERE content_id=? AND operation=?",(cid,operation)).fetchone()
            assignment = assignment_row[0] if assignment_row else self.c.execute("INSERT INTO capture_route_assignments(scope_type,scope_key,provider,operation,content_id,generation,route,mode,effective_at,recorded_at,assignment_sha256) VALUES('content',?,'tikhub',?,?,1,'integrated','active',?,?,?)",(str(cid),operation,cid,created,created,identity)).lastrowid
            wid = self.c.execute("INSERT INTO capture_work_items(work_identity,assignment_id,content_id,provider,operation,due_at,data_business_day,state,envelope_json,created_at,updated_at,owner_token) VALUES(?,?,?,'tikhub',?,?,'2026-08-29',?,?,?,?,?)",(identity,assignment,cid,operation,created,state,json.dumps(env),created,created,identity if state in {"running","leased"} else None)).lastrowid
        return env,int(wid)

    def physical(self,env,*,payload=None,source="live",partial=False,singleton_markers=None):
        """Persist exact operation+params+scope and a real durable send marker."""
        with connect(self.db) as c,transaction(c):
            content = dict(c.execute("SELECT * FROM content_items WHERE id=?",(env["content_id"],)).fetchone())
            desc = shared._descriptor(c.execute("SELECT * FROM capture_work_items WHERE work_identity=?",(shared._identity(env),)).fetchone(),content)
            scope = desc["paid_scope_identity"]
            slot = capture.ensure_content_slot(c,content_id=content["id"],stage=env["stage"],window_key=env["logical_due"],provider="TikHub",adapter_version="fixture")
            raw_evidence.claim_paid_send(self.db.parent/"paid_send_claims",paid_scope_identity=scope,sequence=0,claim={"operation":env["operation"],"slot_id":slot})
            batch = c.execute("INSERT INTO fetch_request_batches(request_scope_identity,sequence,provider,operation,parameters_json,created_at) VALUES(?,0,'tikhub',?,?,?)",(scope,env["operation"],planning.canonical(desc["request"]["parameters"]),NOW)).lastrowid
            if singleton_markers is not None:
                member = c.execute("INSERT INTO fetch_request_batch_members(batch_id,member_scope_identity,sequence,content_id) VALUES(?,?,0,?)",(batch,scope,content["id"])).lastrowid
            attempt = c.execute("INSERT INTO fetch_attempts(slot_id,request_batch_id,attempt_number,request_started_at,response_finished_at,http_status,billed) VALUES(?,?,1,?,?,200,1)",(slot if singleton_markers is None else None,batch if singleton_markers is not None else None,NOW,NOW)).lastrowid
            payload = payload or (detail_payload() if env["platform"] == "kuaishou" else response(video()))
            if partial:
                # Parser exposes missing counters as missing; same phase is spent.
                payload = json.loads(json.dumps(payload))
                def remove(value):
                    if isinstance(value,dict):
                        for key in list(value):
                            if key in {"view_count","viewCount","playCount","viewNum","view_num"}:
                                value.pop(key)
                            else:
                                remove(value[key])
                    elif isinstance(value,list):
                        for item in value:remove(item)
                remove(payload)
            body = json.dumps(payload,ensure_ascii=False).encode()
            path = self.db.parent/(scope+".json");path.write_bytes(body);path.chmod(0o600)
            raw = c.execute("INSERT INTO provider_raw_responses(fetch_attempt_id,content_id,provider,operation,local_path,sha256,byte_size,captured_at,http_status,source,paid_scope_identity,sequence) VALUES(?,?,'TikHub',?,?,?,?,?,200,?,?,0)",(attempt,content["id"],env["operation"],str(path),hashlib.sha256(body).hexdigest(),len(body),NOW,source,scope)).lastrowid
            c.execute("UPDATE fetch_slots SET status='succeeded',attempt_count=1 WHERE id=?",(slot,))
            if singleton_markers is not None:
                from tests.roster_fixture import accept_roster
                from v8.paid_drain import dispatch_state
                from v8 import paid_dispatch
                accept_roster(c,accepted_at=NOW)
                assignment = c.execute("SELECT assignment_id FROM capture_work_items WHERE work_identity=?",(shared._identity(env),)).fetchone()[0]
                c.execute("INSERT INTO fetch_request_executions(batch_id,fetch_attempt_id,assignment_id,execution_identity,started_at) VALUES(?,?,?,?,?)",(batch,attempt,assignment,planning.digest({"scope":scope}),NOW))
                c.execute("INSERT INTO fetch_request_member_dispositions(member_id,disposition,raw_response_id,evidence_json,recorded_at) VALUES(?,'valid',?,'{}',?)",(member,raw,NOW))
                run = c.execute("INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,details_json) VALUES(?,?,'running',?,'{}')",("capture-shared-fixture:"+scope,NOW,NOW)).lastrowid
                ra = c.execute("INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,status,started_at,details_json) VALUES(?,1,'scheduled','running',?,'{}')",(run,NOW)).lastrowid
                usage = c.execute("INSERT INTO provider_usage(provider,operation,request_attempts,billed_requests,amount,currency,recorded_at) VALUES('TikHub',?,0,0,0,'USD',?)",(env["operation"],NOW)).lastrowid
                active = dispatch_state(c,at=NOW).activation_id
                for _ in range(singleton_markers):
                    event = paid_dispatch.reserve_dispatch_in_transaction(c,provider="TikHub",operation=env["operation"],activation_id=active,business_day="2026-08-29",scheduler_run_id=run,scheduler_attempt_id=ra,scope={"content_id":content["id"]},provider_usage_id=usage,fetch_slot_id=slot,created_at=NOW)
                    paid_dispatch.mark_dispatch_sent_in_transaction(c,event.dispatch_id,fetch_attempt_id=attempt,created_at=NOW)
                    paid_dispatch.finish_dispatch_in_transaction(c,event.dispatch_id,outcome="succeeded",raw_response_id=raw,created_at=NOW)
                self.assertIsNone(c.execute("SELECT slot_id FROM fetch_attempts WHERE id=?",(attempt,)).fetchone()[0])
                self.assertEqual(c.execute("PRAGMA foreign_key_check").fetchall(),[])
        parsed = providers._parse_content_payload(env["platform"],env["stage"],content["platform_content_id"],"video",payload,expected_uid=env["uid"])
        return capture.CaptureOutcome(slot,int(attempt),int(raw),parsed.data,True,.001,"USD")

    def stored(self,wid):
        return json.loads(self.c.execute("SELECT envelope_json FROM capture_work_items WHERE id=?",(wid,)).fetchone()[0])

    def test_concurrent_consumers_one_physical_claim_and_same_raw(self):
        for platform in ("kuaishou","wechat_channels"):
            with self.subTest(platform=platform):
                detail,did = self.work(platform,"detail")
                metrics,mid = self.work(platform,"metrics")
                started,release = threading.Event(),threading.Event()
                calls = []
                def execute(**kw):
                    calls.append(kw["operation"]);started.set()
                    self.assertTrue(release.wait(5))
                    return self.physical(detail)
                with patch.object(providers,"_budget_for_call",return_value="local-fixture"),patch.object(providers,"_load_key",return_value="unused-fixture"),patch.object(providers,"_freeze_tikhub_transport",return_value=None),patch.object(capture,"execute_content_fetch",side_effect=execute):
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        owner = pool.submit(runtime._content_request,detail,db_path=self.db,at=NOW)
                        self.assertTrue(started.wait(5))
                        waiting = pool.submit(runtime._content_request,metrics,db_path=self.db,at=NOW).result(5)
                        self.assertEqual(waiting["reason"],"shared_request_owner_pending")
                        self.assertTrue(waiting["continuation"])
                        release.set();paid = owner.result(5)
                    final = runtime._content_request(metrics,db_path=self.db,at=NOW)
                self.assertEqual(calls,[detail["operation"]])
                self.assertTrue(final["complete"])
                self.assertEqual(final["provider_cost"],0)
                self.assertEqual(self.stored(did)[shared.FIELD],self.stored(mid)[shared.FIELD])
                self.assertEqual(final["evidence"]["raw_response_ids"],paid["evidence"]["raw_response_ids"])

    def test_simultaneous_binding_from_two_connections_freezes_one_owner(self):
        detail,did = self.work("kuaishou","detail")
        metrics,mid = self.work("kuaishou","metrics")
        barrier = threading.Barrier(2)
        def bind(env):
            barrier.wait(5);return shared.bind(env,db_path=self.db)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(bind,[detail,metrics]))
        self.assertEqual(sum(r["is_owner"] for r in results),1)
        self.assertEqual(results[0]["binding"],results[1]["binding"])
        self.assertEqual(self.stored(did)[shared.FIELD],self.stored(mid)[shared.FIELD])

    def test_metrics_first_before_detail_adopts_existing_operation_and_raw(self):
        metrics,mid = self.work("wechat_channels","metrics")
        self.assertTrue(shared.bind(metrics,db_path=self.db)["is_owner"])
        outcome = self.physical(metrics)
        detail,did = self.work("wechat_channels","detail")
        relationship = shared.bind(detail,db_path=self.db)
        self.assertEqual(relationship["binding"]["owner"]["work_id"],mid)
        result = shared.consume(detail,relationship,db_path=self.db)
        self.assertEqual(result["evidence"]["producer_operation"],metrics["operation"])
        self.assertEqual(result["evidence"]["raw_response_ids"],[outcome.raw_response_id])

    def test_new_detail_during_metrics_pre_send_retains_frozen_metrics_owner(self):
        metrics,mid = self.work("kuaishou","metrics")
        first = shared.bind(metrics,db_path=self.db)
        detail,_ = self.work("kuaishou","detail")
        second = shared.bind(detail,db_path=self.db)
        self.assertEqual(second["binding"],first["binding"])
        self.assertEqual(second["binding"]["owner"]["work_id"],mid)
        self.assertFalse(second["is_owner"])
        self.assertEqual(shared.consume(detail,second,db_path=self.db)["reason"],"shared_request_owner_pending")

    def test_success_raw_before_materialization_replays_with_partial_fields(self):
        detail,_ = self.work("kuaishou","detail")
        metrics,_ = self.work("kuaishou","metrics")
        shared.bind(detail,db_path=self.db)
        outcome = self.physical(detail,partial=True)
        self.assertEqual(self.c.execute("SELECT source FROM provider_raw_responses WHERE id=?",(outcome.raw_response_id,)).fetchone()[0],"live")
        result = shared.consume(metrics,shared.bind(metrics,db_path=self.db),db_path=self.db)
        self.assertTrue(result["complete"])
        self.assertIn("view_count",result["evidence"]["missing_metric_fields"])
        self.assertEqual(result["reason"],"shared_response_partial_metrics")
        self.assertEqual(self.c.execute("SELECT title,body FROM content_items WHERE id=1").fetchone()[:],("Existing title","Existing body"))

    def test_send_marker_without_usage_or_raw_is_hold_and_never_changes_owner(self):
        detail,did = self.work("kuaishou","detail")
        metrics,_ = self.work("kuaishou","metrics")
        relation = shared.bind(detail,db_path=self.db)
        desc = relation["binding"]["owner"]
        raw_evidence.claim_paid_send(self.db.parent/"paid_send_claims",paid_scope_identity=desc["paid_scope_identity"],sequence=0,claim={})
        with transaction(self.c):self.c.execute("UPDATE capture_work_items SET state='paid_identity_hold',owner_token=NULL WHERE id=?",(desc["work_id"],))
        result = shared.consume(metrics,shared.bind(metrics,db_path=self.db),db_path=self.db)
        self.assertFalse(result["complete"])
        self.assertIn("identity_hold",result["reason"])
        self.assertEqual(self.c.execute("SELECT count(*) FROM provider_usage").fetchone()[0],0)

    def test_legacy_unknown_without_marker_cannot_switch_to_other_running_work(self):
        detail,did = self.work("kuaishou","detail",state="paid_identity_hold")
        metrics,_ = self.work("kuaishou","metrics")
        with transaction(self.c):self.c.execute("UPDATE capture_work_items SET reason='billing_unknown' WHERE id=?",(did,))
        relationship = shared.bind(metrics,db_path=self.db)
        self.assertEqual(relationship["binding"]["owner"]["work_id"],did)
        result = shared.consume(metrics,relationship,db_path=self.db)
        self.assertIn("identity_hold",result["reason"])

    def test_real_singleton_null_slot_and_unique_dispatch_replays(self):
        detail,_ = self.work("kuaishou","detail")
        metrics,_ = self.work("kuaishou","metrics")
        shared.bind(detail,db_path=self.db)
        raw = self.physical(detail,singleton_markers=1)
        result = shared.consume(metrics,shared.bind(metrics,db_path=self.db),db_path=self.db)
        self.assertTrue(result["complete"])
        self.assertEqual(result["evidence"]["raw_response_ids"],[raw.raw_response_id])

    def test_singleton_missing_or_duplicate_dispatch_cannot_supply_consumer(self):
        for platform,markers in (("kuaishou",0),("wechat_channels",2)):
            detail,_ = self.work(platform,"detail")
            metrics,_ = self.work(platform,"metrics")
            shared.bind(detail,db_path=self.db)
            self.physical(detail,singleton_markers=markers)
            result = shared.consume(metrics,shared.bind(metrics,db_path=self.db),db_path=self.db)
            self.assertFalse(result["complete"])
            self.assertEqual(result["provider_cost"],0)

    def test_existing_two_paid_owners_are_conflict(self):
        detail,_ = self.work("kuaishou","detail")
        metrics,_ = self.work("kuaishou","metrics")
        self.physical(detail);self.physical(metrics)
        with self.assertRaisesRegex(capture.CaptureError,"multiple_existing_physical_owners"):
            shared.bind(detail,db_path=self.db)

    def test_natural_later_cycle_and_explicit_refresh_remain_independent(self):
        detail,_ = self.work("kuaishou","detail")
        metrics,_ = self.work("kuaishou","metrics")
        shared.bind(detail,db_path=self.db)
        later,_ = self.work("kuaishou","metrics",due=metrics["logical_due"].replace("03:00","06:00"))
        refresh,_ = self.work("kuaishou","detail",due="media-refresh:fixture:generation-2",kind="media_source_refresh")
        self.assertIsNone(shared.bind(later,db_path=self.db))
        self.assertIsNone(shared.bind(refresh,db_path=self.db))

    def test_first_metrics_strictly_later_than_lifetime_is_independent(self):
        detail,_ = self.work("kuaishou","detail",created="2026-08-28T00:00:00Z")
        metrics,_ = self.work("kuaishou","metrics")
        self.assertIsNone(shared.bind(metrics,db_path=self.db))
        self.assertTrue(shared.bind(detail,db_path=self.db)["is_owner"])

    def test_manual_association_before_and_after_binding_preserves_same_window(self):
        detail,did = self.work("kuaishou","detail",kind="manual_update")
        metrics,mid = self.work("kuaishou","metrics",kind="manual_update")
        relation = shared.bind(detail,db_path=self.db)
        self.assertFalse(shared.bind(metrics,db_path=self.db)["is_owner"])
        with transaction(self.c):
            result = runtime.preserve_manual_work_context(self.c,work_id=mid,envelope=metrics)
            self.c.execute("UPDATE capture_work_items SET envelope_json=? WHERE id=?",(planning.canonical(result),mid))
        self.assertEqual(self.stored(mid)[shared.FIELD],relation["binding"])
        corrupt = {**metrics,shared.FIELD:{**relation["binding"],"sha256":"0"*64}}
        with self.assertRaises(capture.CaptureError):
            runtime.preserve_manual_work_context(self.c,work_id=mid,envelope=corrupt)

    def test_binding_digest_and_nonce_drift_fail_closed(self):
        detail,did = self.work("wechat_channels","detail")
        self.work("wechat_channels","metrics")
        relation = shared.bind(detail,db_path=self.db)
        with transaction(self.c):self.c.execute("UPDATE content_items SET canonical_url=replace(canonical_url,'12345','99999') WHERE id=2")
        with self.assertRaisesRegex(capture.CaptureError,"owner_changed"):
            shared.bind(detail,db_path=self.db)
        stored = self.stored(did);stored[shared.FIELD]["owner"]["operation"]="wrong"
        with self.assertRaisesRegex(capture.CaptureError,"binding_changed"):
            shared.preserve(stored,detail)

    def test_content_snapshot_nonce_change_before_bind_cannot_send_old_parameters(self):
        detail,_ = self.work("wechat_channels","detail")
        self.work("wechat_channels","metrics")
        original = shared.bind
        changed = False
        def bind(*a,**kw):
            nonlocal changed
            if not changed:
                changed = True
                with connect(self.db) as c,transaction(c):c.execute("UPDATE content_items SET canonical_url=replace(canonical_url,'12345','99999') WHERE id=2")
            return original(*a,**kw)
        with patch.object(shared,"bind",side_effect=bind),patch.object(capture,"execute_content_fetch",side_effect=AssertionError("send forbidden")),self.assertRaisesRegex(capture.CaptureError,"execution_parameters_changed"):
            runtime._content_request(detail,db_path=self.db,at=NOW)

    def test_raw_scope_and_author_drift_never_repurchase(self):
        detail,_ = self.work("kuaishou","detail")
        metrics,_ = self.work("kuaishou","metrics")
        shared.bind(detail,db_path=self.db)
        outcome = self.physical(detail)
        with transaction(self.c):self.c.execute("UPDATE provider_raw_responses SET paid_scope_identity=NULL WHERE id=?",(outcome.raw_response_id,))
        with patch.object(capture,"execute_content_fetch",side_effect=AssertionError("send forbidden")),self.assertRaisesRegex(capture.CaptureError,"raw_owner_conflict"):
            shared.consume(metrics,shared.bind(metrics,db_path=self.db),db_path=self.db)

    def test_raw_byte_tamper_blocks_shared_replay_without_another_send(self):
        detail,_ = self.work("kuaishou","detail")
        metrics,_ = self.work("kuaishou","metrics")
        shared.bind(detail,db_path=self.db)
        outcome = self.physical(detail)
        path = self.c.execute("SELECT local_path FROM provider_raw_responses WHERE id=?",(outcome.raw_response_id,)).fetchone()[0]
        Path(path).write_text("tampered")
        with patch.object(capture,"execute_content_fetch",side_effect=AssertionError("send forbidden")),self.assertRaises(capture.RawResponseIntegrityError):
            shared.consume(metrics,shared.bind(metrics,db_path=self.db),db_path=self.db)

    def test_metrics_owner_projection_does_not_invalidate_type_neutral_request(self):
        metrics,_ = self.work("kuaishou","metrics")
        detail,_ = self.work("kuaishou","detail")
        with transaction(self.c):self.c.execute("UPDATE content_items SET content_type='unknown' WHERE id=1")
        relationship = shared.bind(detail,db_path=self.db)
        with transaction(self.c):self.c.execute("UPDATE content_items SET content_type='video' WHERE id=1")
        self.assertEqual(shared.bind(detail,db_path=self.db),relationship)

    def test_missing_work_cannot_silently_enter_paid_path(self):
        detail,_ = self.work("kuaishou","detail")
        with transaction(self.c):self.c.execute("DELETE FROM capture_work_items")
        with self.assertRaisesRegex(capture.CaptureError,"work_missing"):
            shared.bind(detail,db_path=self.db)

    def test_success_wakes_only_existing_pending_consumers(self):
        detail,_ = self.work("kuaishou","detail")
        metrics,mid = self.work("kuaishou","metrics")
        relation = shared.bind(detail,db_path=self.db)
        with transaction(self.c):self.c.execute("UPDATE capture_work_items SET state='runnable',owner_token=NULL,reason='shared_request_owner_pending',due_at='2026-09-01T00:00:00Z' WHERE id=?",(mid,))
        before = self.c.execute("SELECT count(*) FROM capture_work_items").fetchone()[0]
        shared.wake_consumers(relation,db_path=self.db,at=NOW)
        self.assertEqual(self.c.execute("SELECT due_at FROM capture_work_items WHERE id=?",(mid,)).fetchone()[0],planning.timestamp(NOW))
        self.assertEqual(self.c.execute("SELECT count(*) FROM capture_work_items").fetchone()[0],before)
