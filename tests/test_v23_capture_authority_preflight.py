"""Actual temporary schema23 install and live A/B authority with prepared lineage.

No provider transport is called. The temporary installed release, migration,
source proofs, Writer lock, operation gates and budget checks remain real.
"""
import json
import os
import time
from datetime import timedelta
from unittest.mock import patch
import unittest

from tests import test_four_platform_flow_release as fixtures
from v8 import capture_authorizations as auth, capture_release as release
from v8 import four_platform_flow_release as flow, runtime_evidence_context as context
from v8 import provider_budget, runtime_database
from v8.storage import transaction, now_utc
from v8.source_routing import parse_time


class CaptureAuthorityPreflightTest(unittest.TestCase):
    def setUp(self):
        fixture = self.fixture = fixtures.FourPlatformFlowReleaseTest()
        fixture.setUp(); self.addCleanup(fixture.doCleanups)
        self.f = fixture.f
        self.enterContext(patch('socket.socket.connect',side_effect=AssertionError('provider network forbidden')))
        self.enterContext(patch.object(context,'_loaded_source_root',return_value=fixture.source))

    def authorize(self, connection, at, expected=None):
        operation = 'wechat_channels_video_comments'
        bindings = release.current_runtime_bindings(connection,operation,at)
        return auth.validate_authorization(connection,runtime_bindings=bindings,operation=operation,
            request_identity='f'*64,amount_microusd=provider_budget.PRICES_MICROUSD[operation],
            at=at,expected_authority_sha256=expected)

    def test_real_schema23_authority_reuses_full_proof_but_live_gate_revocation_wins(self):
        with self.fixture.flow_runtime():
            c = self.f.connection
            operation='wechat_channels_video_comments'
            self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0],23)
            with transaction(c):release.publish_operation_gate(c,operation=operation,at=now_utc(),mirror_root=self.f.root/'gates')
            before=c.execute('SELECT count(*) FROM provider_usage').fetchone()[0]
            cold=[]
            for _ in range(3):
                started=time.perf_counter()
                with transaction(c):
                    a=self.authorize(c,now_utc())
                    self.authorize(c,now_utc(),a['authority_sha256'])
                cold.append(time.perf_counter()-started)
            real_verify=flow.verify_inheritance
            calls=[]
            def observe(**kwargs):
                calls.append((type(kwargs['connection']).__name__,c.in_transaction))
                return real_verify(**kwargs)
            with patch.object(flow,'verify_inheritance',side_effect=observe):
                hot=[]; preparation=[]
                for _ in range(3):
                    outside=time.perf_counter()
                    with context.prepare_inheritance(self.f.db) as prepared:
                        preparation.append(time.perf_counter()-outside)
                        self.assertGreater(len(prepared.files),100)
                        started=time.perf_counter()
                        with transaction(c),context.inheritance_boundary(c):
                            first=self.authorize(c,now_utc())
                            second=self.authorize(c,now_utc(),first['authority_sha256'])
                            self.assertEqual(first['authority_sha256'],second['authority_sha256'])
                            self.assertEqual(first['provider_calls'],0)
                        hot.append(time.perf_counter()-started)
                self.assertEqual(sum(not inside for _name,inside in calls),3,calls)
                self.assertTrue(all(name=='_ReadSet' for name,inside in calls if not inside),calls)
            print('\n'+json.dumps({'contract':'temporary-schema23-authority-lock-performance-v1',
                'cold_lock_seconds':cold,'prepared_lock_seconds':hot,'preparation_seconds':preparation,
                'lock_reduction_percent':100*(1-max(hot)/max(cold)),
                'provider_calls':0,'formal_runtime_acceptance':False},sort_keys=True))
            # Live accounting changes are intentionally absent from the
            # immutable inheritance read set. They still deny at the ordinary
            # installed budget check, then rollback restores eligibility.
            with context.prepare_inheritance(self.f.db),transaction(c),context.inheritance_boundary(c):
                c.execute('SAVEPOINT temporary_unknown_charge')
                at=now_utc()
                c.execute("""INSERT INTO provider_usage(provider,operation,request_attempts,billed_requests,
                    currency,amount,recorded_at,details_json) VALUES('TikHub',?,1,0,'USD',?,?,?)""",
                    (operation,provider_budget.AUTOMATIC_MICROUSD/1000000,at,
                     json.dumps({'state':'billing_unknown','budget_day':provider_budget.budget_day(at),'category':'comments'})))
                with self.assertRaisesRegex(auth.AuthorizationError,'budget|capacity'):
                    self.authorize(c,at,first['authority_sha256'])
                c.execute('ROLLBACK TO temporary_unknown_charge');c.execute('RELEASE temporary_unknown_charge')
                self.authorize(c,now_utc(),first['authority_sha256'])
                with self.assertRaisesRegex(auth.AuthorizationError,'changed between'):
                    self.authorize(c,now_utc(),'0'*64)
                expired=(parse_time(now_utc())+timedelta(hours=25)).isoformat()
                with self.assertRaisesRegex(auth.AuthorizationError,'expired'):
                    self.authorize(c,expired,first['authority_sha256'])
            # A real changed lease inode is rejected, without mocking the
            # process ownership check. Restore only the disposable fixture.
            with context.prepare_inheritance(self.f.db):
                lock=runtime_database.load_installed_writer_contract(required=True).writer_lock
                saved=lock.with_name(lock.name+'.saved-fixture')
                lock.rename(saved);lock.write_text('replaced');lock.chmod(0o600)
                try:
                    with self.assertRaises(runtime_database.RuntimeDatabaseError),transaction(c),context.inheritance_boundary(c):pass
                finally:
                    lock.unlink();saved.rename(lock)
            # Current and actually verified ancestor code both receive fences.
            for source in (self.fixture.source,self.fixture.fixture.source):
                code=source/'src/dcar_eval/v8/provider_budget.py';body=code.read_bytes();metadata=code.stat()
                with context.prepare_inheritance(self.f.db):
                    code.write_bytes(body+b'\n# changed while waiting\n')
                    os.utime(code,ns=(metadata.st_atime_ns,metadata.st_mtime_ns))
                    try:
                        with self.assertRaises(context.RuntimeEvidenceChanged),transaction(c),context.inheritance_boundary(c):pass
                    finally:code.write_bytes(body)
            # Prepare B, then commit a legitimate operator close before B gets
            # the lock. The inheritance itself is unchanged; live gate denies.
            with context.prepare_inheritance(self.f.db):
                with transaction(c):
                    old=c.execute('SELECT * FROM capture_paid_send_gate_events WHERE operation=? ORDER BY id DESC LIMIT 1',(operation,)).fetchone()
                    row={k:old[k] for k in ('provider','operation','state','reason','evidence_json','recorded_at')}
                    row.update(state='closed',reason='offline operator revoke',recorded_at=now_utc())
                    c.execute(f"INSERT INTO capture_paid_send_gate_events({','.join(row)},event_sha256) VALUES ({','.join('?' for _ in range(len(row)+1))})",(*row.values(),auth.digest(row)))
                with transaction(c),context.inheritance_boundary(c):
                    with self.assertRaisesRegex(auth.AuthorizationError,'current authorization'):
                        self.authorize(c,now_utc(),first['authority_sha256'])
            self.assertEqual(c.execute('SELECT count(*) FROM provider_usage').fetchone()[0],before)
            self.assertLess(max(hot),1.0)
            self.assertLess(max(hot),max(cold)*.1)
