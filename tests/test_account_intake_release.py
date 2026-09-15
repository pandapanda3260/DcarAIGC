"""Temporary schema21 -> 22 inheritance, source checks and stdlib bootstrap."""
from __future__ import annotations
import copy
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import plistlib
import shutil
import unittest
from unittest.mock import patch

from tests import test_account_classification_release as classification_fixtures
from v8 import account_intake_release as release, schema_v21, schema_v22, runtime_database, runtime_paths

ROOT=Path(__file__).resolve().parents[1]
AT='2026-09-07T12:00:00Z'


class FixedClock(datetime):
    @classmethod
    def now(cls,tz=None):
        return datetime.fromisoformat(AT.replace('Z','+00:00'))


class AccountIntakeReleaseTest(unittest.TestCase):
    def setUp(self):
        self.fixture=classification_fixtures.AccountClassificationReleaseTest('test_schema21_retains_capture_authority_gates_and_budget_without_writes')
        self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        f=self.fixture;f.make_child()
        self.parent=copy.deepcopy(f.child);self.parent_ref=dict(f.child_ref)
        f.connection.commit()
        self.legacy=schema_v21.migration_proof(f.connection)
        with patch.object(runtime_database,'load_installed_writer_contract',return_value=None),patch.object(schema_v22,'datetime',FixedClock):
            schema_v22.migrate(f.connection)
        self.assertEqual(release.inherited_classification_proof(f.connection),self.legacy)
        self.source=f.root/'intake-source';shutil.copytree(f.child_source,self.source)
        required=release.REQUIRED_SOURCE | {'src/dcar_eval/v8/schema_v20.py','src/dcar_eval/v8/schema_v21.py',
            'src/dcar_eval/v8/account_classification.py','src/dcar_eval/v8/account_classification_release.py',
            'src/dcar_eval/v8/account_catalog_capture_release.py'}
        for name in required:
            target=self.source/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/name,target)
        self.tree=release.inventory(self.source)
        self.tree_ref=f.write('intake-tree.json',self.tree)
        self.changes=release.source_changes(release.object_at(self.parent['account_cleanup_generation']['source_tree']),self.tree)
        self.checks={}
        for name in ('intake_schema','intake_execution'):
            output=f.root/(name+'.log');output.write_text('temporary fixture check output\n');output.chmod(0o600)
            self.checks[name]=f.write(name+'.json',{'contract':release.CHECK_CONTRACT,'name':name,'status':'passed','exit_code':0,
                'source_tree':self.tree_ref,'changes':self.changes,'command':['fixture-only-check'],'output':release.reference(output)})
        self.checks={name:release.reference(Path(ref['path'])) for name,ref in self.checks.items()}
        identity=f.db.stat()
        self.migration={'contract':release.INSTALL_CONTRACT,'status':'migrated','from_schema':21,'to_schema':22,
            'formal_database':str(f.db),'database_identity':{'device':identity.st_dev,'inode':identity.st_ino},
            'authority_build':self.parent_ref,'authority_install':f.install_ref,'source_tree':self.tree_ref,'checks':self.checks,
            'preserved_tables_verified':True,'paid_gates_issued':0,'migration_proof':schema_v22.migration_proof(f.connection),
            'inherited_classification_proof':self.legacy,'migrated_at':AT}
        self.migration['receipt_sha256']=release.digest(self.migration)
        self.migration_ref=f.write('intake-migration.json',self.migration)
        from v8.account_catalog_capture_release import ACCOUNT_CATALOG_POLICY_V3
        plan={'contract':release.CONTRACT,'parent_build':self.parent_ref,'source_tree':self.tree_ref,'changes':self.changes,
            'checks':self.checks,'migration':self.migration_ref,'account_catalog_policy':ACCOUNT_CATALOG_POLICY_V3,
            'account_catalog_policy_sha256':release.digest(ACCOUNT_CATALOG_POLICY_V3),'legacy_execution_controls':'inherited_unchanged',
            'provider_qualification':'required_per_operation','production_rollout':'approved_by_user',
            'actor':'temporary fixture','reason':'offline test only','issued_at':AT}
        source_plan=f.write('intake-source-plan.json',{'contract':'account-cleanup-source-plan-v1','transition':'account-cleanup-0907-v1',
            'project_root':str(f.project),'source_root':str(self.source),'git':self.tree['git'],'source_tree':self.tree_ref})
        self.build={**self.parent,'source_root':str(self.source),'git':self.tree['git'],'created_at':AT,
            'critical_files':{row['path']:row['sha256'] for row in self.tree['files']
                if row['path'].startswith(('src/','config/')) and row['path'].endswith(('.py','.json'))},
            'code_successor_plan':source_plan,'account_cleanup_generation':{**self.parent['account_cleanup_generation'],'source_tree':self.tree_ref},
            'account_intake_successor':plan,'schema_contract':{'code_schema':22,'formal_schema':22},'validation_scope':'fixture'}
        self.seal()

    def seal(self):
        self.build_ref=self.fixture.envelope('intake-build.json','sealed-build-receipt-v1',self.build)

    def verify(self):
        f=self.fixture
        return release.verify_inheritance(build=self.build,build_ref=self.build_ref,install_path=f.install_path,
            database=f.db,source=self.source,at=AT,connection=f.connection)

    def test_original_receipt_provenance_and_no_write_inheritance(self):
        f=self.fixture;before=f.connection.total_changes
        result=self.verify()
        self.assertEqual(release.object_at(result['proof']['migration'])['migration_proof'],self.legacy)
        self.assertEqual(result['catalog_capture_policy']['contract'],'account-catalog-automatic-capture-policy-v3')
        self.assertEqual(f.connection.total_changes,before)
        with self.assertRaises(ValueError):
            schema_v21.migration_proof(f.connection)

    def test_real_schema22_local_authorization_is_source_bound_and_optional(self):
        from v8 import account_preparation_authority as authority
        from v8.profile_activations import activation_at
        self.assertNotIn(authority.EVIDENCE_KEY,self.verify())
        f=self.fixture;plan=self.build['account_intake_successor'];active=activation_at(f.connection,AT)
        approval={'contract':authority.AUTHORIZATION_CONTRACT,'scope':'local_writer_capture_only','schema_version':22,
            'operations':sorted(authority.OPERATIONS),'platforms':list(authority.PLATFORMS),'manual_statuses':list(authority.STATUSES),
            'qualification':'operator_authorized','business_e2e':'required','transport_qualification':'not_verified',
            'publisher_authorized':False,'remote_database_authorized':False,
            'parent_build':self.parent_ref,'source_tree':self.tree_ref,'migration':self.migration_ref,
            'formal_database':{'path':str(f.db),**self.migration['database_identity']},
            'catalog_policy_sha256':plan['account_catalog_policy_sha256'],
            'activation':{key:active[key] for key in authority.ACTIVE_KEYS},
            'actor':'offline fixture','reason':'local capture only','user_instruction':'Enable local capture without publishing',
            'source_thread_id':'fixture-local-authorization','issued_at':AT}
        plan['operation_authorization']=f.write('local-capture-authorization.json',approval)
        plan.update(production_rollout='not_authorized',local_activation='approved_by_user');self.seal()
        result=self.verify();proof=result[authority.EVIDENCE_KEY]
        self.assertEqual(proof['intake_proof_sha256'],result['intake_proof']['proof_sha256'])
        self.assertEqual(proof['authorization_payload'],approval)
        self.assertEqual(proof['operations'],sorted(authority.OPERATIONS))
        approval['publisher_authorized']=True
        plan['operation_authorization']=f.write('wrong-local-authorization.json',approval);self.seal()
        with self.assertRaisesRegex(ValueError,'local operation scope'):
            self.verify()

    def test_exact_checked_source_and_modified_output_rejected(self):
        checked=release.verify_candidate_source(source=self.source,parent_build_ref=self.parent_ref,checks=self.checks)
        self.assertEqual(checked['source_tree_ref'],self.tree_ref)
        output=Path(release.object_at(self.checks['intake_schema'])['output']['path']);output.write_text('changed\n')
        with self.assertRaisesRegex(ValueError,'check'):
            release.verify_candidate_source(source=self.source,parent_build_ref=self.parent_ref,checks=self.checks)
        with self.assertRaisesRegex(ValueError,'check'):
            self.verify()

    def test_wrong_inode_or_policy_cannot_inherit_authority(self):
        self.migration['database_identity']['inode']+=1
        self.migration['receipt_sha256']=release.digest({k:v for k,v in self.migration.items() if k!='receipt_sha256'})
        self.build['account_intake_successor']['migration']=self.fixture.write('bad-migration.json',self.migration)
        self.seal()
        with self.assertRaisesRegex(ValueError,'installation proof'):
            self.verify()
        self.build['account_intake_successor']['migration']=self.migration_ref
        self.build['account_intake_successor']['account_catalog_policy']={'invented':'unsafe'}
        self.seal()
        with self.assertRaisesRegex(ValueError,'execution scope'):
            self.verify()

    def test_bootstrap_prefers_intake_and_imports_only_verified_source(self):
        f=self.fixture
        env={**f.child_env,'DCAR_WRITER_SOURCE_ROOT':str(self.source),'DCAR_LOADED_BUILD_RECEIPT':self.build_ref['path']}
        home=f.root/'intake-home';plist=home/'Library/LaunchAgents/cn.tj.dcar.writer-worker.plist'
        plist.parent.mkdir(parents=True);plist.write_bytes(plistlib.dumps({'Label':'cn.tj.dcar.writer-worker',
            'WorkingDirectory':str(f.project),'ProgramArguments':[str(self.source/'deploy/macos/run_writer_worker.sh')],
            'EnvironmentVariables':env}));plist.chmod(0o600)
        result=runtime_paths.verify_source_before_import(data=f.project,source=self.source,build_receipt=Path(self.build_ref['path']),home=home)
        self.assertEqual(result['files'],len(self.tree['files']))
        (self.source/'src/dcar_eval/v8/account_intake_release.py').write_text('raise AssertionError("unverified code must not execute")\n')
        with self.assertRaisesRegex(ValueError,'source content'):
            runtime_paths.verify_source_before_import(data=f.project,source=self.source,build_receipt=Path(self.build_ref['path']),home=home)

    def test_prepare_emits_installable_plist_without_changing_database_or_installed_plist(self):
        from types import SimpleNamespace
        path=ROOT/'scripts/prepare_account_intake_release.py'
        spec=importlib.util.spec_from_file_location('fixture_prepare_intake',path)
        cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
        f=self.fixture;installed=f.home/'Library/LaunchAgents/cn.tj.dcar.writer-worker.plist'
        original=installed.read_bytes();before=f.connection.total_changes
        self.migration.update(previous_writer_plist_sha256=hashlib.sha256(original).hexdigest(),previous_loaded_build=self.parent_ref)
        self.migration['receipt_sha256']=release.digest({k:v for k,v in self.migration.items() if k!='receipt_sha256'})
        migration=f.write('prepare-migration.json',self.migration)
        args=SimpleNamespace(approve_production_rollout=True,parent_build=Path(self.parent_ref['path']),parent_install=f.install_path,
            installed_plist=installed,migration=Path(migration['path']),evidence_root=f.root/'prepared-intake',
            check_report=[name+'='+ref['path'] for name,ref in self.checks.items()],actor='temporary fixture',reason='local test')
        with patch.object(cli,'ROOT',self.source):
            plan=cli.prepare(args)
        proposal=plistlib.loads(Path(plan['next_plist']['path']).read_bytes())
        self.assertEqual(proposal['EnvironmentVariables']['DCAR_WRITER_SOURCE_ROOT'],str(self.source))
        self.assertEqual(proposal['EnvironmentVariables']['DCAR_LOADED_BUILD_RECEIPT'],plan['child_build']['path'])
        self.assertEqual(plan['bootstrap_verification']['files'],len(self.tree['files']))
        self.assertEqual(installed.read_bytes(),original)
        self.assertEqual(f.connection.total_changes,before)
        self.assertEqual(plan['paid_gates_issued'],0)
        instruction=f.root/'user-instruction.txt';instruction.write_text('Enable the local automation; do not publish or touch remote databases.');instruction.chmod(0o600)
        args.approve_production_rollout=False;args.approve_local_capture=True
        args.user_instruction_file=instruction;args.source_thread_id='local-test-thread';args.evidence_root=f.root/'prepared-local-intake'
        with patch.object(cli,'ROOT',self.source):
            local=cli.prepare(args)
        local_build=release.payload_at(local['child_build'],'sealed-build-receipt-v1')
        successor=local_build['account_intake_successor']
        self.assertEqual(successor['production_rollout'],'not_authorized')
        self.assertEqual(successor['local_activation'],'approved_by_user')
        authorization=release.object_at(successor['operation_authorization'])
        self.assertEqual(authorization['user_instruction'],instruction.read_text())
        self.assertFalse(authorization['publisher_authorized'])
        self.assertEqual(installed.read_bytes(),original)
        self.assertEqual(f.connection.total_changes,before)

    def test_freeze_and_real_subprocess_check_bind_identical_source(self):
        from types import SimpleNamespace
        import sys
        spec=importlib.util.spec_from_file_location('fixture_check_intake',ROOT/'scripts/prepare_account_intake_release.py')
        cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
        f=self.fixture;frozen=f.root/'frozen-intake';manifest=f.root/'frozen-intake-tree.json'
        with patch.object(cli,'ROOT',self.source):
            result=cli.freeze(SimpleNamespace(parent_build=Path(self.parent_ref['path']),source_root=frozen,source_tree=manifest))
        self.assertEqual(release.object_at(result['source_tree'])['files'],self.tree['files'])
        args=SimpleNamespace(parent_build=Path(self.parent_ref['path']),source_tree=manifest,name='intake_schema',
            output=f.root/'actual-check.json',command=[sys.executable,'-c','print("temporary check executed")'])
        with patch.object(cli,'ROOT',frozen):
            report=cli.check(args)
        self.assertEqual(report['status'],'passed')
        self.assertIn('temporary check executed',Path(report['output']['path']).read_text())
        self.assertEqual(report['source_tree'],release.reference(manifest))
        self.assertFalse((frozen/'src/dcar_eval/v8/__pycache__').exists())

    def test_installed_runtime_inherits_old_authority_and_exposes_new_policy_without_writes(self):
        import os
        from dataclasses import replace
        from v8 import capture_release
        f=self.fixture;original=runtime_database.load_installed_writer_contract(required=True)
        env={**original.payload['EnvironmentVariables'],'DCAR_WRITER_SOURCE_ROOT':str(self.source),
            'DCAR_LOADED_BUILD_RECEIPT':self.build_ref['path']}
        installed=replace(original,payload={**original.payload,'EnvironmentVariables':env})
        before=f.connection.total_changes
        with patch.object(runtime_database,'load_installed_writer_contract',return_value=installed),patch.dict(os.environ,
            {**env,'DCAR_LOADED_BUILD_ID':'sha256:'+self.build_ref['sha256']}):
            result=capture_release._installed_evidence(f.connection,at=AT)
        self.assertEqual(result['catalog_capture_policy']['contract'],'account-catalog-automatic-capture-policy-v3')
        self.assertEqual(result['account_intake_successor']['parent_build'],self.parent_ref)
        self.assertEqual(result['active']['activation_id'],f.active['activation_id'])
        self.assertEqual(f.connection.total_changes,before)


    def test_publisher_proposal_changes_only_source_and_expected_schema(self):
        import tempfile
        spec=importlib.util.spec_from_file_location('fixture_publisher_proposal',ROOT/'scripts/prepare_account_intake_release.py')
        cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
        f=self.fixture;environment=f.root/'publisher.env'
        environment.write_text("\n".join(['DCAR_PUBLISH_SSH_ALIAS=fixture-remote',
            'DCAR_PUBLISH_REMOTE_PROJECT_ROOT=/var/www/dcar/current','DCAR_PUBLISH_REMOTE_STATE_ROOT=/var/lib/dcar',
            'DCAR_PUBLISH_REMOTE_PYTHON=/var/www/dcar/current/.venv/bin/python',
            'DCAR_PUBLISH_SNAPSHOT_ROOT='+str(f.root/'snapshots'),'DCAR_PUBLISH_MIN_REMOTE_FREE_BYTES=1073741824',
            'DCAR_PUBLISH_EXPECTED_USER_VERSION=21','DCAR_PUBLISH_MAX_CONTENT_LAG_DAYS=1'])+'\n')
        environment.chmod(0o600)
        env={'DCAR_PROJECT_ROOT':str(f.project),'DCAR_WRITER_SOURCE_ROOT':self.parent['source_root'],
            'DCAR_V8_DB':str(f.db),'DCAR_READ_ONLY':'1','DCAR_SCHEDULER_ENABLED':'0','DCAR_STARTUP_CATCHUP_ENABLED':'0',
            'DCAR_PUBLISHER_ENV_FILE':str(environment)}
        payload={'Label':'cn.tj.dcar.snapshot-publisher','WorkingDirectory':str(f.project),
            'EnvironmentVariables':env,'ProgramArguments':[str(Path(self.parent['source_root'])/'deploy/macos/run_snapshot_publisher.sh')]}
        installed=f.root/'publisher.plist';installed.write_bytes(plistlib.dumps(payload));installed.chmod(0o600)
        original_environment=environment.read_bytes();original_plist=installed.read_bytes()
        evidence=f.root/'publisher-proposal';evidence.mkdir()
        result=cli.publisher_proposal(installed,evidence_root=evidence,source=self.source,parent=self.parent,database=f.db)
        self.assertEqual(environment.read_bytes(),original_environment)
        self.assertEqual(installed.read_bytes(),original_plist)
        self.assertEqual(Path(result['next_environment']['path']).read_bytes(),original_environment.replace(b'USER_VERSION=21',b'USER_VERSION=22'))
        next_plist=plistlib.loads(Path(result['next_plist']['path']).read_bytes())
        self.assertEqual(next_plist['EnvironmentVariables'],{**env,'DCAR_WRITER_SOURCE_ROOT':str(self.source)})
        self.assertEqual(result['status'],'prepared')
