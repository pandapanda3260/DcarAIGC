"""Schema22 publisher evidence from real temporary databases; no external I/O."""
from __future__ import annotations
import copy
import hashlib
import sqlite3
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from tests import test_account_classification_publication as fixtures
from tests import test_account_classification_snapshot_deployment as deployment_fixtures
from tests.test_macos_snapshot_publisher import FakeRunner
from v8 import schema_v22
from v8.snapshot_contract import descriptor

publisher=fixtures.publisher


class AccountIntakePublicationTest(unittest.TestCase):
    def setUp(self):
        self.fixture=fixtures.AccountClassificationPublicationTest('test_schema21_identity_triggers_publication_without_changing_schema20_protocol')
        self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.before=self.fixture.observe()
        schema_v22.migrate(self.fixture.connection)

    def add_request(self):
        c=self.fixture.connection
        c.execute("INSERT INTO account_intake_requests(request_key,input_sha256,preparation_key,platform,input_json,source_json,created_at,updated_at) VALUES ('fixture',?,'fixture','douyin','{}','{}',?,?)",('a'*64,fixtures.AT,fixtures.AT))
        c.commit()

    def test_real22_retains_original_classification_and_binds_migration_lineage(self):
        current=self.fixture.observe();intake=current['account_intake']
        publisher._validate_publication_evidence(current,expected_schema=22)
        publisher._validate_publication_evidence(self.before,expected_schema=21)
        self.assertEqual(current['account_classification'],self.before['account_classification'])
        self.assertEqual(intake['schema_version'],22)
        self.assertEqual(intake['inherited_classification_receipt_sha256'],self.before['account_classification']['migration_receipt_sha256'])
        self.assertEqual(intake['request_count'],0)
        self.assertNotEqual(publisher._publication_fingerprint(current),publisher._publication_fingerprint(self.before))

    def test_pending_input_and_same_count_preparation_outcome_change_fingerprint(self):
        before=self.fixture.observe();self.add_request();added=self.fixture.observe()
        self.assertEqual(before['account_classification'],added['account_classification'])
        self.assertEqual(added['account_intake']['request_count'],1)
        self.assertNotEqual(publisher._publication_fingerprint(before),publisher._publication_fingerprint(added))
        self.fixture.connection.execute("UPDATE account_intake_requests SET result_json=?",('{"status":"accepted","preparation_error":"locator_resolution_required"}',))
        self.fixture.connection.commit();changed=self.fixture.observe()
        self.assertEqual(changed['account_intake']['request_count'],1)
        self.assertNotEqual(added['account_intake']['requests_sha256'],changed['account_intake']['requests_sha256'])

    def test_missing_wrong_count_or_wrong_inherited_receipt_is_rejected(self):
        baseline=self.fixture.observe();missing=copy.deepcopy(baseline);missing.pop('account_intake')
        with self.assertRaisesRegex(publisher.SnapshotPublishError,'missing account intake'):
            publisher._validate_publication_evidence(missing,expected_schema=22)
        for key,value in (('request_count',True),('completed_request_count',1),('requests_sha256','bad'),
                          ('inherited_classification_receipt_sha256','a'*64),('source_schema_sha256','bad')):
            changed=copy.deepcopy(baseline);changed['account_intake'][key]=value
            with self.subTest(key=key),self.assertRaises(publisher.SnapshotPublishError):
                publisher._validate_publication_evidence(changed,expected_schema=22)
        with self.assertRaises(publisher.SnapshotPublishError):
            publisher._validate_publication_evidence(baseline,expected_schema=21)

    def test_detached_snapshot_recomputes_intake_and_rejects_stale_response(self):
        f=self.fixture;identity={'database_schema_version':22}
        self.enterContext(patch.object(publisher,'_database_runtime_identity',return_value=identity))
        output=f.root/'snapshot';target=output/'databases/dcar_insight.sqlite3';target.parent.mkdir(parents=True)
        def freeze():
            with sqlite3.connect(target) as destination:f.connection.backup(destination)
            return {'databases':[{'name':'dcar_insight.sqlite3','bundle_path':'databases/dcar_insight.sqlite3',
                'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'byte_size':target.stat().st_size}],
                'deployment_readiness':f.deployment,'files':[]}
        evidence=f.installed_evidence()
        freshness=publisher.WriterFreshness(evidence=evidence,latest_published_at=None,content_count=0,
            runtime_identity=identity,snapshot_contract=descriptor())
        publisher._verify_snapshot_dependencies(output,freeze(),freshness,project_root=fixtures.ROOT)
        self.add_request()
        with self.assertRaisesRegex(publisher.SnapshotPublishError,'current observations drifted'):
            publisher._verify_snapshot_dependencies(output,freeze(),freshness,project_root=fixtures.ROOT)


class AccountIntakeRemotePairTest(unittest.TestCase):
    def setUp(self):
        self.fixture=deployment_fixtures.ClassificationSnapshotDeploymentTest('test_actual21_identity_structure_and_explicit_pair')
        self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.config=replace(self.fixture.config,expected_user_version=22)
        self.identity={**self.fixture.identity,'database_schema_version':22,'database_schema_migration':'unified-account-intake-v1'}

    def probe(self):
        value=FakeRunner(self.identity).probe()
        value['health']['database_state']['user_version']=22
        value['installer_schema_support']={'versions':[17,18,19,20,21,22],
            'transitions':[[17,18],[18,19],[19,20],[20,21],[21,22]]}
        value['schema_transition']={'schema':publisher.INTAKE_TRANSITION_SCHEMA,'from_schema':21,'to_schema':22,
            'status':'succeeded','completed_at':'2026-09-12T03:00:00Z'}
        return value

    def test_22_requires_receiver_support_and_completed_exact_pair(self):
        publisher._validate_remote_probe(self.probe(),config=self.config)
        for field,value in (('schema',publisher.CLASSIFICATION_TRANSITION_SCHEMA),('status','rolled_back'),('from_schema',20)):
            probe=self.probe();probe['schema_transition'][field]=value
            with self.subTest(field=field),self.assertRaises(publisher.SnapshotPublishError):
                publisher._validate_remote_probe(probe,config=self.config)
        probe=self.probe();probe['installer_schema_support']['transitions'].remove([21,22])
        with self.assertRaises(publisher.SnapshotPublishError):publisher._validate_remote_probe(probe,config=self.config)

    def test_21_rollback_marker_only_allows_21_current_health(self):
        probe=self.fixture.probe()
        probe['schema_transition']={'schema':publisher.INTAKE_TRANSITION_SCHEMA,'from_schema':21,'to_schema':22,
            'status':'rolled_back','completed_at':'2026-09-12T03:00:00Z'}
        publisher._validate_remote_probe(probe,config=self.fixture.config)
        probe['health']['database_state']['user_version']=22
        with self.assertRaises(publisher.SnapshotPublishError):publisher._validate_remote_probe(probe,config=self.fixture.config)
