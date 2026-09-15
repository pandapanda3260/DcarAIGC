"""Isolated local authorization, real gate issuance and renewal; no network."""
from __future__ import annotations

import copy
import json
import unittest
from unittest.mock import patch

from v8 import account_preparation_authority as preparation, capture_authorizations as auth
from v8.account_catalog_capture_release import ACCOUNT_CATALOG_POLICY_V3
from tests.test_v8_account_profile_authority import fixture_evidence as old_fixture, reference

AT = '2026-09-07T12:00:00Z'


def fixture_evidence(base=None):
    evidence = copy.deepcopy(base or old_fixture())
    policy = copy.deepcopy(ACCOUNT_CATALOG_POLICY_V3)
    intake = {'contract':'account-intake-schema-successor-v1','loaded_build':reference('intake.json','4'),
              'parent_build':reference('parent.json','5'),'source_tree':reference('source.json','6'),
              'migration':reference('migration.json','7'),'account_catalog_policy_sha256':auth.digest(policy)}
    intake['proof_sha256'] = auth.digest(intake)
    install = evidence['install']
    approval = {'contract':preparation.AUTHORIZATION_CONTRACT,'scope':'local_writer_capture_only','schema_version':22,
        'operations':sorted(preparation.OPERATIONS),'platforms':list(preparation.PLATFORMS),'manual_statuses':list(preparation.STATUSES),
        'qualification':'operator_authorized','business_e2e':'required','transport_qualification':'not_verified',
        'publisher_authorized':False,'remote_database_authorized':False,
        'parent_build':intake['parent_build'],'source_tree':intake['source_tree'],'migration':intake['migration'],
        'formal_database':{'path':install['formal_database'], 'device':install['installed']['device'],'inode':install['installed']['inode']},
        'catalog_policy_sha256':auth.digest(policy),'activation':{k:evidence['active'][k] for k in preparation.ACTIVE_KEYS},
        'actor':'offline fixture','reason':'test exact local approval','user_instruction':'Enable local capture; do not publish',
        'source_thread_id':'offline-test','issued_at':AT}
    proof = {'contract':preparation.PROOF_CONTRACT,'operations':sorted(preparation.OPERATIONS),
        'authorization':reference('authorization.json','8'),'authorization_payload':approval,
        'loaded_build':intake['loaded_build'],'source_tree':intake['source_tree'],'intake_proof_sha256':intake['proof_sha256'],
        'runtime_root_receipt':{'path':'/private/fixture/runtime.json','sha256':evidence['runtime_sha256']},
        'transport_manifest':evidence['manifest']}
    proof['proof_sha256'] = auth.digest(proof)
    evidence.update(account_intake_successor=intake, catalog_capture_proof=intake,
                    catalog_capture_policy=policy,catalog_capture_policy_sha256=auth.digest(policy))
    evidence[preparation.EVIDENCE_KEY] = proof
    return evidence


class PreparationAuthorityTest(unittest.TestCase):
    def test_exact_operations_use_new_local_authority_without_claiming_qualification(self):
        from v8.capture_operator_release import _decision
        evidence = fixture_evidence()
        for operation in preparation.OPERATIONS:
            with self.subTest(operation=operation):
                value = _decision(evidence, operation, AT)
                self.assertEqual(value['contract_version'],preparation.DECISION_CONTRACT)
                self.assertEqual(value['scope'],'local_writer_capture_only')
                self.assertEqual(value['production_rollout'],'not_authorized')
                self.assertEqual(value['qualification'],'operator_authorized')
                self.assertEqual(value['business_e2e'],'required')
                self.assertEqual(value['transport_qualification'],'not_verified')

    def test_missing_proof_or_unapproved_operation_has_no_new_authority(self):
        self.assertIsNone(preparation.decision(old_fixture(),'xiaohongshu_user_profile',AT))
        self.assertIsNone(preparation.decision(fixture_evidence(),'invented_operation',AT))

    def test_modified_approval_never_expands_local_scope(self):
        for key,value in [('operations',['douyin_uid_profile']),('publisher_authorized',True),
                ('remote_database_authorized',True),('business_e2e','passed'),('transport_qualification','qualified'),
                ('scope','production'),('source_thread_id',''),('user_instruction','')]:
            evidence = fixture_evidence();proof = evidence[preparation.EVIDENCE_KEY]
            proof['authorization_payload'][key] = value
            proof['proof_sha256'] = auth.digest({k:v for k,v in proof.items() if k!='proof_sha256'})
            with self.subTest(key=key), self.assertRaises(auth.AuthorizationError):
                preparation.decision(evidence,'xiaohongshu_user_profile',AT)

    def test_changed_live_database_activation_policy_and_transport_are_rejected(self):
        changes = [lambda e:e['install']['installed'].update(inode=999),
                   lambda e:e['active'].update(activation_id=999),
                   lambda e:e['catalog_capture_policy'].update(statuses=['daily']),
                   lambda e:e.update(manifest={'changed':'route'}),
                   lambda e:e.pop('account_intake_successor')]
        for change in changes:
            evidence = fixture_evidence();change(evidence)
            with self.assertRaises(auth.AuthorizationError):
                preparation.decision(evidence,'xiaohongshu_user_profile',AT)

    def test_command_surface_allows_only_operator_publish_and_renew(self):
        from v8.capture_release_commands import validate_parameters
        from v8.profile_control import ProfileControlError
        for operation in preparation.OPERATIONS:
            for action in ('operation_publish','operation_renew'):
                self.assertEqual(validate_parameters({'action':action,'operation':operation})['operation'],operation)
        for action in ('native_freeze','continuity_publish','integrated_publish'):
            with self.assertRaises(ProfileControlError):
                validate_parameters({'action':action,'operation':'xiaohongshu_user_profile'})


class PreparationOperatorIntegrationTest(unittest.TestCase):
    def setUp(self):
        from tests.test_v8_account_cleanup_runtime import CleanupRuntimeTest
        from v8 import capture_release as release
        self.fixture = CleanupRuntimeTest('runTest')
        self.addCleanup(self.fixture.doCleanups);self.fixture.setUp()
        self.connection = self.fixture.connection
        original = release._installed_evidence
        self.evidence = fixture_evidence(original(self.connection,at=AT))
        self.enterContext(patch.object(release,'_installed_evidence',return_value=self.evidence))

    def test_first_issue_validates_real_A_B_and_renewal_preserves_budget(self):
        from v8 import capture_release as release, capture_operator_release as operator, provider_budget
        operation = 'xiaohongshu_user_profile'
        first = release.publish_operation_gate(self.connection,operation=operation,at=AT)
        self.assertEqual(first['qualification'],'operator_authorized')
        self.assertEqual(first['provider_calls'],0)
        self.assertFalse(first['coverage_complete'])
        gate = self.connection.execute('SELECT * FROM capture_paid_send_gate_events WHERE operation=? ORDER BY id DESC LIMIT 1',(operation,)).fetchone()
        payload = json.loads(gate['evidence_json'])
        self.assertEqual(payload['budget']['total_microusd'],provider_budget.AUTOMATIC_MICROUSD)
        bindings = release.current_runtime_bindings(self.connection,operation,AT)
        arguments = dict(runtime_bindings=bindings,operation=operation,request_identity=auth.digest({'fixture':'new-profile'}),
                         at=AT,amount_microusd=provider_budget.PRICES_MICROUSD[operation])
        phase_a = auth.validate_authorization(self.connection,**arguments)
        phase_b = auth.validate_authorization(self.connection,expected_authority_sha256=phase_a['authority_sha256'],**arguments)
        self.assertEqual(phase_a['authority_sha256'],phase_b['authority_sha256'])
        fresh = release.renew_operation_gate(self.connection,operation=operation,at='2026-09-07T13:00:00Z')
        self.assertEqual(fresh['status'],'fresh')
        renewed = release.renew_operation_gate(self.connection,operation=operation,at='2026-09-08T06:00:00Z')
        self.assertEqual(renewed['status'],'renewed')
        self.assertEqual(renewed['business_e2e'],'required')
        self.assertEqual(self.connection.execute('SELECT count(*) FROM provider_request_start_events').fetchone()[0],0)
        self.assertEqual(self.fixture.network.call_count,0)

    def test_no_first_gate_means_renewal_fails_and_maintenance_does_not_open_it(self):
        from v8 import capture_release as release
        with self.assertRaises(auth.AuthorizationError):
            release.renew_operation_gate(self.connection,operation='kuaishou_user_profile',at=AT)
        result = self.fixture.maintenance(AT)
        self.assertEqual(result['operations']['kuaishou_user_profile']['status'],'not_enabled')

    def test_closed_gate_cannot_be_reopened_by_maintenance(self):
        from v8 import capture_release as release
        operation = 'wechat_channels_resolve'
        release.publish_operation_gate(self.connection,operation=operation,at=AT)
        gate={'provider':'tikhub','operation':operation,'state':'closed','reason':'explicit closure','evidence_json':'{}','recorded_at':AT}
        self.connection.execute('INSERT INTO capture_paid_send_gate_events(provider,operation,state,reason,evidence_json,recorded_at,event_sha256) VALUES(?,?,?,?,?,?,?)',(*gate.values(),auth.digest(gate)))
        result=self.fixture.maintenance('2026-09-08T06:00:00Z')
        self.assertEqual(result['operations'][operation]['status'],'not_enabled')

