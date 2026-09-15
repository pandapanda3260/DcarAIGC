"""Frozen selection rejects scope changes before real durable claim writes."""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from v8 import capture_repair as repair
from v8 import capture_runtime as runtime, runtime_evidence_context, storage
from tests import test_four_platform_flow_release as fixtures


def plan_for(works):
    at = datetime.now(timezone.utc)
    return {"contract_version": repair.CONTRACT, "mode": "publish_probe", "repair_run_id": "offline-repair",
        "authorization": "offline fixture only", "issued_at": (at - timedelta(minutes=1)).isoformat(),
        "expires_at": (at + timedelta(minutes=15)).isoformat(), "window_seconds": 900,
        "publish_operations": [], "works": works}


class CaptureRepairTest(unittest.TestCase):
    def test_real_module_entry_and_claim_import_share_one_context(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve(); marker=root/'entry.json'
            (root/'sitecustomize.py').write_text("import atexit,sys,json\nfrom pathlib import Path\n"
                "def check():\n m=sys.modules.get('v8.capture_repair')\n"
                " Path("+repr(str(marker))+").write_text(json.dumps({'same_module':m is sys.modules['__main__']}))\n"
                "atexit.register(check)\n")
            source=Path(__file__).resolve().parents[1]/'src/dcar_eval'
            env={key:value for key,value in os.environ.items() if not key.startswith(('DCAR_','TIKHUB_'))}
            env.update(PYTHONPATH=str(root)+os.pathsep+str(source),PYTHONDONTWRITEBYTECODE='1',
                DCAR_TEST_DENY_FORMAL_DB='1',DCAR_WRITER_REPAIR_PLAN=str(root/'missing-plan.json'))
            result=subprocess.run([sys.executable,'-m','v8.capture_repair'],env=env,capture_output=True,text=True)
            self.assertEqual(result.returncode,1)  # missing plan fails before DB access
            self.assertTrue(json.loads(marker.read_text())['same_module'])

    def test_private_plan_digest_permissions_and_bounds(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory).resolve() / "plan.json"
            value = plan_for([])
            path.write_text(json.dumps(value)); path.chmod(0o600)
            expected = repair.hashlib.sha256(path.read_bytes()).hexdigest()
            self.assertEqual(repair._private_json(path, expected), value)
            repair.validate_plan_document(value, at=storage.now_utc())
            with self.assertRaises(repair.RepairRejected): repair._private_json(path, "0" * 64)
            path.chmod(0o644)
            with self.assertRaises(repair.RepairRejected): repair._private_json(path, expected)
            for invalid in ({**value, "works": [{"id": 1}, {"id": 1}]},
                            {**value, "window_seconds": 901}, {**value, "mode": "arbitrary_module"}):
                with self.assertRaises(repair.RepairRejected):
                    repair.validate_plan_document(invalid, at=storage.now_utc())

    def test_fixed_plan_rejects_expanded_targets(self):
        value = {**plan_for([]), 'mode':'fixed_repair', 'fixed_plan':{},
            'fixed_stage_keys':['xhs_detail'], 'local_recovery':{
                'work_id':12450, 'fetch_attempt_id':142631, 'media_status':'available'}}
        repair.validate_plan_document(value, at=storage.now_utc())
        repair.validate_plan_document({**value, 'fixed_retry_stage_keys':['ks_metrics']}, at=storage.now_utc())
        for invalid in ({**value, 'fixed_stage_keys':['profile_prepare']},
                {**value, 'fixed_stage_keys':['xhs_detail','xhs_detail']},
                {**value, 'fixed_retry_stage_keys':['xhs_metrics']},
                {**value, 'fixed_retry_stage_keys':['ks_metrics','ks_metrics']},
                {**value, 'fixed_stage_keys':['ks_metrics'], 'fixed_retry_stage_keys':['ks_metrics']},
                {**value, 'local_recovery':{**value['local_recovery'], 'work_id':1}}):
            with self.assertRaises(repair.RepairRejected):
                repair.validate_plan_document(invalid, at=storage.now_utc())

    def test_real_claim_selects_only_frozen_work_without_recovering_other_work(self):
        case = fixtures.FourPlatformFlowReleaseTest(); case.setUp(); self.addCleanup(case.doCleanups)
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('network forbidden')))
        self.enterContext(patch.object(runtime_evidence_context, '_loaded_source_root', return_value=case.source))
        with case.flow_runtime():
            connection = case.f.connection
            at = storage.now_utc(); active = runtime.activation_at(connection, at)
            account = connection.execute('SELECT id FROM accounts ORDER BY id LIMIT 1').fetchone()[0]
            identity = connection.execute('SELECT id FROM account_platform_identities WHERE account_id=? LIMIT 1', (account,)).fetchone()[0]
            with storage.transaction(connection):
                content = connection.execute("INSERT INTO content_items(account_id,link_id,platform,platform_content_id,canonical_url,title,content_type,created_at,updated_at,imported_at) VALUES(?,'REPAIR','douyin','7380000000000000042','https://www.douyin.com/video/7380000000000000042','offline repair','video',?,?,?)", (account,at,at,at)).lastrowid
                assignment = connection.execute("INSERT INTO capture_route_assignments(scope_type,scope_key,provider,operation,content_id,generation,route,mode,effective_at,recorded_at,assignment_sha256) VALUES('content','repair','tikhub','douyin_video_detail',?,1,'integrated','active',?,?,?)", (content,at,at,'a'*64)).lastrowid
                env = {'account_id':account,'identity_id':identity,'content_id':content,'platform':'douyin',
                    'stage':'detail','capture_stage':'detail','source_stage':'detail','category':'detail',
                    'operation':'douyin_video_detail','assignment_id':assignment,'logical_due':'repair-fixture',
                    **{key:active[key] for key in ('activation_id','profile_id','roster_snapshot_id','roster_members_sha256')}}
                work = connection.execute("INSERT INTO capture_work_items(work_identity,assignment_id,account_id,content_id,provider,operation,due_at,data_business_day,state,reason,envelope_json,created_at,updated_at) VALUES(?,?,?,?,'tikhub','douyin_video_detail',?,?,'runnable','',?,?,?)", ('b'*64,assignment,account,content,runtime.planning.timestamp(at),runtime._business_day(at),json.dumps(env),at,at)).lastrowid
            frozen = plan_for([{'id':work,'work_identity':'b'*64,'operation':'douyin_video_detail','envelope_sha256':repair._digest(env)}])
            frozen['activation'] = {key:active[key] for key in ('activation_id','activation_sha256')}
            original = connection.execute('SELECT count(*) FROM scheduler_run_attempts').fetchone()[0]
            with self.assertRaises(repair.RepairRejected): runtime._run_single(case.f.db, at, repair_work_id=work)
            self.assertEqual(connection.execute('SELECT count(*) FROM scheduler_run_attempts').fetchone()[0], original)
            changed = json.loads(json.dumps(frozen)); changed['works'][0]['envelope_sha256'] = '0'*64
            with repair.execution_context(changed), self.assertRaises(repair.RepairRejected):
                runtime._run_single(case.f.db, at, repair_work_id=work)
            self.assertEqual(connection.execute('SELECT count(*) FROM scheduler_run_attempts').fetchone()[0], original)
            def complete(envelope, **_):
                return {'complete':True,'continuation':False,'envelope':envelope,'evidence':{'raw_response_ids':[]},'reason':'','provider_cost':0}
            from contextlib import nullcontext
            with repair.execution_context(frozen), patch.object(runtime, '_recover_work', side_effect=AssertionError('global recovery forbidden')), \
                    patch.object(runtime, '_readiness', return_value=('runnable','')) as ready, \
                    patch.object(runtime.planning, 'execution_route_context', return_value=nullcontext()), \
                    patch.object(runtime, '_execute_one', side_effect=complete) as execute, \
                    patch.object(runtime, '_verify_raws'):
                result = runtime._run_single(case.f.db, at, repair_work_id=work)
            self.assertEqual(result['status'], 'terminal'); self.assertEqual(result['work_id'], work)
            self.assertEqual(ready.call_count, 1); self.assertEqual(execute.call_count, 1)
            self.assertEqual(connection.execute('SELECT count(*) FROM scheduler_run_attempts').fetchone()[0], original+1)
            self.assertEqual(connection.execute('SELECT count(*) FROM fetch_attempts').fetchone()[0], 0)


if __name__ == '__main__': unittest.main()
