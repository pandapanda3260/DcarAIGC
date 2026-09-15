"""Real migrated SQLite and artifact rollback; only service/HTTP and authority fixtures are substituted."""
import copy
import json
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests import test_server_schema_upgrade as engine
from tests import test_server_snapshot_deployment as deployment
from v8 import storage, schema_v20, schema_v21, schema_v22, schema_v23, schema_v24

installer = deployment.installer
import replica_schema_upgrade as upgrade


class ReplicaChainTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.template = Path(cls.directory.name).resolve() / 'source'
        engine._source_template(cls.template)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def setUp(self):
        f = self.f = engine.ServerSchemaUpgradeTest()
        f.template = self.template
        f.setUp(); self.addCleanup(f.doCleanups)
        item = next(r for r in f.manifest['databases'] if r['name'] == 'dcar_insight.sqlite3')
        incoming = f.bundle / item['bundle_path']
        for database, initial, target in ((f.active_database, 18, 21), (incoming, 19, 24)):
            with storage.connect(database) as c:
                if initial == 18: storage.migrate_database(c, from_version=18, to_version=19)
                schema_v20.migrate(c, legacy_project_root=f.writer); schema_v21.migrate(c)
                if target == 24:
                    schema_v22.migrate(c); schema_v23.migrate(c); schema_v24.migrate(c)
                c.execute('PRAGMA wal_checkpoint(TRUNCATE)'); c.execute('PRAGMA journal_mode=DELETE')
        item.update(byte_size=incoming.stat().st_size, sha256=installer._sha256(incoming),
            **installer._validate_sqlite(incoming, expected_user_version=24))
        f.manifest['runtime_identity'] = installer._database_runtime_identity(incoming, expected_schema=24)
        # Authority boundary fixture only. Actual recursive immutable proofs are
        # covered by test_v24_snapshot_successor and the production verify gate.
        f.manifest['deployment_readiness'] = {'schema_version':24, **{k: {'receipt_sha256': h * 64}
            for k,h in [('account_intake_migration','a'),('four_platform_flow_migration','b'),('duplicate_index_migration','c')]}}
        deployment._write_bundle_manifest(f.bundle, f.manifest)
        installer._write_json_atomic(f.config.active_manifest_path, {'snapshot_id':'20260911T000000Z-000000000021',
            'activation_status':'succeeded','database_sha256':{'dcar_insight.sqlite3':installer._sha256(f.active_database)}})
        installer._write_json_atomic(f.config.transition_path, {'schema':installer._transition_contract(20,21)[0],
            'from_schema':20,'to_schema':21,'status':'succeeded','completed_at':'2026-09-11T00:00:00Z'})
        for name, value in [('_verify_schema20_deployment', None), ('_read_json_url', self.http)]:
            p = patch.object(installer, name, **({'side_effect':value} if callable(value) else {'return_value':value}))
            p.start(); self.addCleanup(p.stop)
        self.baseline = f._protected_state()
        self.sealed = upgrade.seal(f.bundle, f.config, installer)

    def http(self, url, timeout):
        f = self.f
        if url == f.config.scheduler_url:
            return {'read_only':True,'requested':False,'enabled':False,'startup_catchup':{'requested':False,'enabled':False}}
        if url == f.config.overview_url:
            return {'status':'ready','windows':{},'data_freshness':{}}
        with sqlite3.connect(f.active_database) as c:
            version=c.execute('PRAGMA user_version').fetchone()[0]
            count, latest=c.execute('SELECT COUNT(*),MAX(published_at) FROM content_items').fetchone()
        return {'status':'ok','read_only':True,'lifecycle_jobs_enabled':False,
            'media_consumers':{'contract_version':'media-consumer-runtime-v1','code_sha256':'d'*64,
                'project_root':str(f.old),'current_code_matches_loaded':True},
            'database_state':{'sha256':installer._sha256(f.active_database),'user_version':version,
                'content_count':count,'latest_published_at':latest,
                'runtime_identity':installer._database_runtime_identity(f.active_database,expected_schema=version),
                'schema_compatibility':{'supported_versions':[19,20,21,22,23,24]}}}

    def run_upgrade(self, **kwargs):
        f=self.f
        return upgrade.execute(f.bundle,f.config,self.sealed['seal_sha256'],installer,
            service_action=lambda verb:f._service_action(verb,'dcar-api.service'),**kwargs)

    def test_complete_chain_preserves_code_config_and_only_restarts_api(self):
        f=self.f
        value=self.run_upgrade()
        self.assertTrue(upgrade.settled(value,24))
        installer._assert_transition_settled(f.config)
        self.assertEqual(self.run_upgrade(),value)
        self.assertEqual(f.events,[('stop','dcar-api.service'),('start','dcar-api.service')])
        self.assertEqual(str(f.config.current_release.resolve()),str(f.old))
        self.assertEqual(upgrade.deployment_files(f.config,installer),self.sealed['seal']['configurations'])
        for row in f.manifest['databases']:
            self.assertEqual(installer._sha256(f.config.database_root/row['name']),row['sha256'])
        upgrade.rollback(f.bundle,f.config,self.sealed['seal_sha256'],installer,
            service_action=lambda verb:f._service_action(verb,'dcar-api.service'))
        self.assertEqual(f._protected_state(),self.baseline)

    def test_failure_after_artifact_or_database_switch_restores_every_old_byte(self):
        # One execution per fixture; separate tests cover both cut points.
        def fail(point):
            if point == 'databases_applied': raise RuntimeError('injected failure')
        with self.assertRaisesRegex(installer.SnapshotInstallError,'previous replica restored'):
            self.run_upgrade(checkpoint_hook=fail)
        self.assertEqual(self.f._protected_state(),self.baseline)
        self.assertTrue(upgrade.settled(installer._read_object(self.f.config.transition_path),21))

    def test_failure_during_artifact_switch_restores_every_old_byte(self):
        def fail(point):
            if point == 'artifacts_applied': raise RuntimeError('injected failure')
        with self.assertRaisesRegex(installer.SnapshotInstallError,'previous replica restored'):
            self.run_upgrade(checkpoint_hook=fail)
        self.assertEqual(self.f._protected_state(),self.baseline)

    def test_changed_configuration_refuses_before_service_stop(self):
        path=next(iter(installer._config_targets(self.f.config).values()))
        path.write_text('concurrent admin edit')
        with self.assertRaisesRegex(installer.SnapshotInstallError,'configuration'):
            self.run_upgrade()
        self.assertEqual(self.f.events,[])
        self.assertEqual(path.read_text(),'concurrent admin edit')

    def test_missing_chain_and_unsettled_state_are_rejected(self):
        for name in ('account_intake_migration','four_platform_flow_migration','duplicate_index_migration'):
            value=copy.deepcopy(self.f.manifest);value['deployment_readiness'].pop(name)
            with self.assertRaises(installer.SnapshotInstallError):upgrade.migration_chain(value,installer)
        state={'schema':upgrade.CONTRACT,'from_schema':21,'to_schema':24,'status':'in_progress'}
        installer._write_json_atomic(self.f.config.transition_path,state)
        with self.assertRaisesRegex(installer.SnapshotInstallError,'unsettled'):
            installer._assert_transition_settled(self.f.config)
