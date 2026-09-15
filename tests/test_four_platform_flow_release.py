"""Actual temporary schema22 -> 23 install, bootstrap and operation proofs."""
from __future__ import annotations
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
import ast
import importlib.util
import os
from pathlib import Path
import plistlib
import shutil
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import test_account_intake_release as fixtures
from v8 import account_preparation_authority as old_authority
from v8 import four_platform_flow_release as release, runtime_database, runtime_paths, schema_v22, schema_v23
from v8.profile_activations import activation_at

ROOT = Path(__file__).resolve().parents[1]
FROZEN_PARENT = Path('/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260912-account-intake-v3')


def script(name):
    spec = importlib.util.spec_from_file_location(name,ROOT/'scripts'/(name+'.py'))
    module = importlib.util.module_from_spec(spec);sys.modules[name] = module;spec.loader.exec_module(module)
    return module


class FourPlatformFlowReleaseTest(unittest.TestCase):
    def setUp(self):
        if not FROZEN_PARENT.is_dir():
            self.skipTest('Immutable schema22 parent fixture source is not installed on this host')
        fixture = self.fixture = fixtures.AccountIntakeReleaseTest()
        acquire = runtime_database.acquire_writer_lock
        def remember(access):
            self.writer_context = acquire(access); return self.writer_context
        with patch.object(fixtures,'ROOT',FROZEN_PARENT), patch.object(runtime_database,'acquire_writer_lock',side_effect=remember):
            fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.writer_context.__exit__(None,None,None)
        self.f = f = fixture.fixture
        active = activation_at(f.connection,fixtures.AT); plan = fixture.build['account_intake_successor']
        approval = {'contract':old_authority.AUTHORIZATION_CONTRACT,'scope':'local_writer_capture_only','schema_version':22,
            'operations':sorted(old_authority.OPERATIONS),'platforms':list(old_authority.PLATFORMS),'manual_statuses':list(old_authority.STATUSES),
            'qualification':'operator_authorized','business_e2e':'required','transport_qualification':'not_verified',
            'publisher_authorized':False,'remote_database_authorized':False,'parent_build':fixture.parent_ref,
            'source_tree':fixture.tree_ref,'migration':fixture.migration_ref,'formal_database':{'path':str(f.db),**fixture.migration['database_identity']},
            'catalog_policy_sha256':plan['account_catalog_policy_sha256'],'activation':{key:active[key] for key in old_authority.ACTIVE_KEYS},
            'actor':'offline fixture','reason':'temporary inheritance fixture','user_instruction':'Temporary fixture local capture only',
            'source_thread_id':'offline-fixture','issued_at':fixtures.AT}
        plan.update(operation_authorization=f.write('flow-parent-approval.json',approval),production_rollout='not_authorized',local_activation='approved_by_user')
        fixture.seal();f.connection.commit()
        self.parent_ref = release.reference(Path(fixture.build_ref['path']))
        self.parent, self.inherited = release.parent_context(self.parent_ref,install_path=f.install_path,database=f.db,
            at=fixtures.AT,connection=f.connection)
        self.source = f.root/'flow-source';shutil.copytree(fixture.source,self.source)
        for folder in ('src','config','scripts','deploy'):
            shutil.copytree(ROOT/folder,self.source/folder,dirs_exist_ok=True,ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
        for row in fixture.tree['files']:
            if (self.source/row['path']).is_file():
                (self.source/row['path']).chmod(row['mode'])
        self.tree = release.inventory(self.source);self.tree_ref = release.reference(Path(f.write('flow-tree.json',self.tree)['path']))
        changes = release.source_changes(fixture.tree,self.tree);self.checks = {}
        for name in sorted(release.CHECKS):
            log = f.root/(name+'.log');log.write_text('Offline disposable fixture only; no production acceptance.\n');log.chmod(0o600)
            self.checks[name] = release.reference(Path(f.write(name+'.json',{'contract':release.CHECK_CONTRACT,'name':name,'status':'passed','exit_code':0,
                'source_tree':self.tree_ref,'changes':changes,'command':['offline-fixture-only'],'output':release.reference(log)})['path']))
        installed = runtime_database.load_installed_writer_contract(required=True)
        env = {**f.child_env,'DCAR_WRITER_SOURCE_ROOT':str(fixture.source),'DCAR_LOADED_BUILD_RECEIPT':self.parent_ref['path'],
            'DCAR_WRITER_LOCK':str(installed.writer_lock)}
        self.parent_plist = {'Label':'cn.tj.dcar.writer-worker','WorkingDirectory':str(f.project),
            'ProgramArguments':[str(fixture.source/'deploy/macos/run_writer_worker.sh')],'EnvironmentVariables':env}
        self.installed_path = f.home/'Library/LaunchAgents/cn.tj.dcar.writer-worker.plist'
        self.installed_path.write_bytes(plistlib.dumps(self.parent_plist));self.installed_path.chmod(0o600)
        self.installed = replace(installed,payload=self.parent_plist)
        self.enterContext(patch.object(runtime_database,'load_installed_writer_contract',return_value=self.installed))
        self.enterContext(patch.dict(os.environ,{**env,'DCAR_LOADED_BUILD_ID':'sha256:'+self.parent_ref['sha256']}))
        self.installer = script('install_four_platform_flow');self.prepare_cli = script('prepare_four_platform_flow_release')
        self.enterContext(patch.object(self.installer,'ROOT',self.source));self.enterContext(patch.object(self.prepare_cli,'ROOT',self.source))
        self.args = SimpleNamespace(database=f.db,project_root=f.project,installed_plist=self.installed_path,
            parent_build=Path(self.parent_ref['path']),parent_install=f.install_path,source_tree=Path(self.tree_ref['path']),
            output_dir=f.root/'flow-install',check_report=[name+'='+ref['path'] for name,ref in self.checks.items()])

    def prepare(self, migration):
        instruction = self.f.root/'flow-instruction.txt';instruction.write_text('Offline fixture approves the local forward flow without historical data backfill.');instruction.chmod(0o600)
        return self.prepare_cli.prepare(SimpleNamespace(approve_local_capture=True,user_instruction_file=instruction,
            source_thread_id='offline-fixture',actor='offline fixture',reason='test only',parent_build=self.args.parent_build,
            parent_install=self.args.parent_install,check_report=self.args.check_report,installed_plist=self.installed_path,
            migration=Path(migration['path']),evidence_root=self.f.root/'flow-prepared'))

    @contextmanager
    def flow_runtime(self):
        migration = self.installer.install(self.args);plan = self.prepare(migration)
        next_plist = plistlib.loads(Path(plan['next_plist']['path']).read_bytes())
        installed = replace(self.installed,payload=next_plist)
        with patch.object(runtime_database,'load_installed_writer_contract',return_value=installed),patch.dict(os.environ,
                {**next_plist['EnvironmentVariables'],'DCAR_LOADED_BUILD_ID':'sha256:'+plan['child_build']['sha256']}):
            access = runtime_database.resolve_installed_database_access(runtime_database.DatabaseAccessMode.WRITER,
                database=self.f.db,project_root=self.f.project)
            with runtime_database.acquire_writer_lock(access):
                yield plan

    def comments_release_command(self, action, at):
        from v8 import capture_release_commands, profile_control, runtime_evidence_context
        parameters = {'action':action,'operation':'wechat_channels_video_comments'}
        command = profile_control.enqueue_current_activation_hold_command(db_path=self.f.db,
            command_id='offline-comments-'+action,command='capture_release',parameters=parameters,
            submitted_at=at)
        self.assertEqual(command['status'],'queued')
        # This fixture imports the test checkout while executing a fully sealed
        # disposable installed source. Keep the real verifier and fences; bind
        # only that test process's source location and controlled clock.
        with patch.object(runtime_evidence_context,'_loaded_source_root',return_value=self.source), \
                patch.object(runtime_evidence_context,'_now',return_value=at), \
                patch.object(profile_control,'now_utc',return_value=at), \
                patch.object(capture_release_commands,'now_utc',return_value=at):
            processed = profile_control.process_current_activation_hold_commands(db_path=self.f.db,
                limit=1,mirror_root=self.f.root/'release-command')
        self.assertEqual(processed['count'],1,processed)
        self.assertEqual(processed['processed'][0]['run_id'],command['run_id'])
        saved = profile_control.current_activation_hold_command_status(self.f.connection,run_id=command['run_id'])
        self.assertEqual(len(saved['attempts']),1)
        return saved

    def test_explicit_schema23_comments_gate_is_maintained_without_auto_first_issue_or_reopening(self):
        from v8 import capture_release, capture_authorizations as auth, capture_release_commands, profile_control
        from v8.source_routing import parse_time
        from v8.storage import transaction
        operation = 'wechat_channels_video_comments'
        self.enterContext(patch('socket.socket.connect',side_effect=AssertionError('provider network forbidden')))
        with self.flow_runtime():
            connection = self.f.connection
            self.assertEqual(connection.execute('PRAGMA user_version').fetchone()[0],23)
            at = self.installer.now()
            def maintain(timestamp):
                with transaction(connection):
                    result = capture_release.maintain_operation_qualifications(connection,
                        at=timestamp,mirror_root=self.f.root/'operation-maintenance')
                self.assertEqual(result['status'],'checked',result)
                self.assertEqual(result['provider_calls'],0)
                return result['operations'][operation]
            def count():
                return connection.execute('SELECT count(*) FROM capture_paid_send_gate_events WHERE operation=?',(operation,)).fetchone()[0]
            provider_usage_before = connection.execute('SELECT count(*) FROM provider_usage').fetchone()[0]
            for action in ('continuity_publish','native_freeze','integrated_publish'):
                with self.assertRaises(profile_control.ProfileControlError):
                    capture_release_commands.validate_parameters({'action':action,'operation':operation})
            self.assertEqual(maintain(at)['status'],'not_enabled')
            self.assertEqual(count(),0)
            command = self.comments_release_command('operation_publish',at)
            self.assertEqual(command['status'],'succeeded',command)
            published = command['result']
            self.assertEqual(published['provider_calls'],0)
            self.assertTrue(published['ordinary_paid_authorized'])
            self.assertEqual(count(),1)
            self.assertEqual(maintain(at)['status'],'fresh')
            self.assertEqual(count(),1)
            due = (parse_time(at)+timedelta(hours=19)).isoformat()
            renewed = maintain(due)
            self.assertEqual(renewed['status'],'renewed')
            self.assertGreater(parse_time(renewed['expires_at']),parse_time(published['expires_at']))
            self.assertEqual(count(),2)
            explicit_renew = self.comments_release_command('operation_renew',due)
            self.assertEqual(explicit_renew['status'],'succeeded',explicit_renew)
            self.assertEqual(explicit_renew['result']['status'],'fresh')
            self.assertEqual(count(),2)
            with transaction(connection):
                previous = connection.execute('SELECT * FROM capture_paid_send_gate_events WHERE operation=? ORDER BY id DESC LIMIT 1',(operation,)).fetchone()
                gate = {key:previous[key] for key in ('provider','operation','state','reason','evidence_json','recorded_at')}
                gate.update(state='closed',reason='explicit-offline-operator-close',recorded_at=due)
                connection.execute(f"INSERT INTO capture_paid_send_gate_events({','.join(gate)},event_sha256) VALUES ({','.join('?' for _ in range(len(gate)+1))})",(*gate.values(),auth.digest(gate)))
            self.assertEqual(maintain((parse_time(at)+timedelta(hours=40)).isoformat())['status'],'not_enabled')
            self.assertEqual(count(),3)
            self.assertEqual(connection.execute('SELECT state FROM capture_paid_send_gate_events WHERE operation=? ORDER BY id DESC LIMIT 1',(operation,)).fetchone()[0],'closed')
            self.assertEqual(connection.execute('SELECT count(*) FROM provider_usage').fetchone()[0],provider_usage_before)

    def test_old_schema22_installed_authority_cannot_issue_wechat_comments_gate(self):
        from v8 import capture_release, capture_operator_release
        operation = 'wechat_channels_video_comments'
        self.enterContext(patch('socket.socket.connect',side_effect=AssertionError('provider network forbidden')))
        access = runtime_database.resolve_installed_database_access(runtime_database.DatabaseAccessMode.WRITER,
            database=self.f.db,project_root=self.f.project)
        with runtime_database.acquire_writer_lock(access):
            self.assertEqual(self.f.connection.execute('PRAGMA user_version').fetchone()[0],22)
            evidence = capture_release._installed_evidence(self.f.connection,at=self.installer.now())
            self.assertNotIn('four_platform_flow_successor',evidence)
            self.assertIsNone(capture_operator_release._decision(evidence,operation,self.installer.now()))
            command = self.comments_release_command('operation_publish',self.installer.now())
            self.assertEqual(command['status'],'failed',command)
            self.assertEqual(command['error']['type'],'AuthorizationError')
            self.assertEqual(self.f.connection.execute('SELECT count(*) FROM capture_paid_send_gate_events WHERE operation=?',(operation,)).fetchone()[0],0)

    def test_install_prepare_bootstrap_and_local_activation_preserve_parent_and_inode(self):
        f = self.f; original = f.db.stat(); before_plist = self.installed_path.read_bytes()
        parent_proof = schema_v22.migration_proof(f.connection)
        migration = self.installer.install(self.args); receipt = release.object_at(migration)
        self.assertEqual(receipt['migration_proof'],schema_v23.migration_proof(f.connection))
        self.assertEqual(receipt['parent_migration_proof'],parent_proof)
        self.assertEqual(receipt['paid_gates_issued'],0)
        self.assertEqual((f.db.stat().st_ino,f.db.stat().st_mode),(original.st_ino,original.st_mode))
        plan = self.prepare(migration)
        self.assertEqual(self.installed_path.read_bytes(),before_plist)
        self.assertEqual(plan['bootstrap_verification']['files'],len(self.tree['files']))
        build = release.payload_at(plan['child_build'],'sealed-build-receipt-v1')
        result = release.verify_inheritance(build=build,build_ref=plan['child_build'],install_path=f.install_path,
            database=f.db,source=self.source,connection=f.connection)
        self.assertEqual(result['intake_proof'],self.inherited['intake_proof'])
        self.assertEqual(result['preparation_operation_authority'],self.inherited['preparation_operation_authority'])
        self.assertEqual(result['four_platform_flow_proof']['authorization_payload']['metric_policy']['policy_version'],'source-routing-operation-field-v4')
        activation_args = SimpleNamespace(proposal=Path(plan['proposal']['path']),output=f.root/'flow-activated.json')
        installed = self.installer.activate(activation_args)
        self.assertEqual(release.object_at(installed)['status'],'installed_stopped')
        self.assertEqual(plistlib.loads(self.installed_path.read_bytes())['EnvironmentVariables']['DCAR_LOADED_BUILD_RECEIPT'],plan['child_build']['path'])
        self.assertEqual(f.db.stat().st_ino,original.st_ino)
        self.assertEqual(self.installer.activate(activation_args),installed)

    def test_source_change_is_rejected_before_migration(self):
        target = self.source/'src/dcar_eval/v8/metric_source_policy.py';target.write_bytes(target.read_bytes()+b'\n# later change\n')
        with self.assertRaisesRegex(ValueError,'check differs'):
            self.installer.install(self.args)
        self.assertEqual(self.f.connection.execute('PRAGMA user_version').fetchone()[0],22)
        self.assertFalse(self.args.output_dir.exists())

    def test_migration_receipt_failure_recovers_without_repeating_migration(self):
        original = self.installer.write
        def fail(path,value):
            if path.name == 'migration-install.json': raise OSError('fixture receipt failure')
            return original(path,value)
        with patch.object(self.installer,'write',side_effect=fail),self.assertRaisesRegex(OSError,'receipt failure'):
            self.installer.install(self.args)
        proof = schema_v23.migration_proof(self.f.connection)
        result = self.installer.recover(self.args)
        self.assertEqual(release.object_at(result)['migration_proof'],proof)
        self.assertEqual(self.installer.recover(self.args),result)

    def test_backup_tampering_fails_closed_and_retains_newer_business_data(self):
        migration = self.installer.install(self.args)
        self.f.connection.execute("UPDATE accounts SET operator_name='later business write' WHERE id=(SELECT min(id) FROM accounts)")
        self.f.connection.commit()
        with Path(release.object_at(migration)['backup']['path']).open('ab') as stream: stream.write(b'tampered')
        with self.assertRaisesRegex(ValueError,'backup changed'):
            self.installer.recover(self.args)
        self.assertEqual(self.f.connection.execute('PRAGMA user_version').fetchone()[0],23)
        self.assertEqual(self.f.connection.execute('SELECT operator_name FROM accounts ORDER BY id LIMIT 1').fetchone()[0],'later business write')

    def test_schema23_code_repair_preserves_migration_and_rebinds_only_verified_source(self):
        from v8 import capture_release, capture_operator_release
        migration = self.installer.install(self.args); origin_plan = self.prepare(migration)
        origin_source = self.source
        self.installed_path.write_bytes(Path(origin_plan['next_plist']['path']).read_bytes())
        schema_before = schema_v23.migration_proof(self.f.connection)
        usage_before = self.f.connection.execute('SELECT count(*) FROM provider_usage').fetchone()[0]
        source = self.f.root/'flow-code-source'; shutil.copytree(origin_source,source)
        for name in (release.MODULE,'src/dcar_eval/v8/capture_runtime.py'):
            path = source/name; path.write_bytes(path.read_bytes()+b'\n# Isolated code successor fixture.\n')
        tree = release.inventory(source); tree_ref = release.reference(Path(self.f.write('code-tree.json',tree)['path']))
        changes = release.source_changes(self.fixture.tree,tree); checks = {}
        for name in sorted(release.CHECKS):
            log = self.f.root/('code-'+name+'.log');log.write_text('Offline code successor fixture only.');log.chmod(0o600)
            checks[name] = release.reference(Path(self.f.write('code-'+name+'.json',{
                'contract':release.CHECK_CONTRACT,'name':name,'status':'passed','exit_code':0,
                'source_tree':tree_ref,'changes':changes,'command':['offline-code-fixture'],
                'output':release.reference(log)})['path']))
        instruction = self.f.root/'code-instruction.txt';instruction.write_text('Approved same local forward repair.');instruction.chmod(0o600)
        args = SimpleNamespace(approve_local_capture=True,user_instruction_file=instruction,
            source_thread_id='offline-code-fixture',actor='offline fixture',reason='queue and metric planning repair',
            parent_build=self.args.parent_build,parent_install=self.args.parent_install,
            check_report=[name+'='+ref['path'] for name,ref in checks.items()],installed_plist=self.installed_path,
            migration=Path(migration['path']),evidence_root=self.f.root/'code-prepared',
            code_predecessor_build=Path(origin_plan['child_build']['path']))
        with patch.object(self.prepare_cli,'ROOT',source):
            proposal = self.prepare_cli.prepare(args)
        child = release.payload_at(proposal['child_build'],'sealed-build-receipt-v1')
        proof = release.verify_inheritance(build=child,build_ref=proposal['child_build'],
            install_path=self.args.parent_install,database=self.f.db,source=source)
        repair = child[release.FIELD]['code_predecessor']
        self.assertEqual(repair['build'],origin_plan['child_build'])
        self.assertFalse(repair['schema_migration_repeated'])
        self.assertEqual(schema_v23.migration_proof(self.f.connection),schema_before)
        self.assertEqual(child[release.FIELD]['migration'],origin_plan['migration'])
        self.assertEqual(self.f.connection.execute('SELECT count(*) FROM provider_usage').fetchone()[0],usage_before)
        self.assertEqual(proof['four_platform_flow_proof']['loaded_build'],proposal['child_build'])
        # A new installed source still needs its own actual runtime decision.
        next_plist = plistlib.loads(Path(proposal['next_plist']['path']).read_bytes())
        installed = replace(self.installed,payload=next_plist)
        with patch.object(runtime_database,'load_installed_writer_contract',return_value=installed),patch.dict(os.environ,
            {**next_plist['EnvironmentVariables'],'DCAR_LOADED_BUILD_ID':'sha256:'+proposal['child_build']['sha256']}):
            access = runtime_database.resolve_installed_database_access(runtime_database.DatabaseAccessMode.WRITER,
                database=self.f.db,project_root=self.f.project)
            with runtime_database.acquire_writer_lock(access):
                evidence = capture_release._installed_evidence(self.f.connection,at=self.installer.now(),maintenance_only=True)
                decision = capture_operator_release._decision(evidence,'wechat_channels_video_comments',self.installer.now())
        self.assertEqual(decision['loaded_build'],proposal['child_build'])
        self.assertEqual(decision['source_tree'],tree_ref)
        self.assertEqual(decision['production_rollout'],'not_authorized')
        self.assertEqual(self.f.connection.execute('SELECT count(*) FROM capture_paid_send_gate_events').fetchone()[0],0)
        # Tampering with either predecessor or migration cannot be resealed away.
        for key,value in [('proof_sha256','0'*64),('database_writes',1)]:
            broken = {**child,release.FIELD:{**child[release.FIELD],
                'code_predecessor':{**repair,key:value}}}
            ref = self.f.write('broken-code-'+key+'.json',{'contract_version':'sealed-build-receipt-v1',
                'payload':broken,'payload_sha256':release.digest(broken)})
            with self.assertRaisesRegex(ValueError,'immutable schema23 origin or scope'):
                release.verify_inheritance(build=broken,build_ref=release.reference(Path(ref['path'])),
                    install_path=self.args.parent_install,database=self.f.db,source=source)
        disallowed = source/'config/source_routing_operation_field_v4.json'
        body = disallowed.read_bytes();disallowed.write_bytes(body+b'\n')
        with self.assertRaisesRegex(ValueError,'reviewed execution files'):
            release.code_repair_changes(release.payload_at(origin_plan['child_build'],'sealed-build-receipt-v1'),release.inventory(source))
        disallowed.write_bytes(body)

    def test_schema23_runtime_decision_binds_new_source_without_expanding_paid_scope(self):
        from v8 import capture_release, capture_operator_release
        from v8 import four_platform_flow_authority as authority
        migration = self.installer.install(self.args);plan = self.prepare(migration)
        next_plist = plistlib.loads(Path(plan['next_plist']['path']).read_bytes());env = next_plist['EnvironmentVariables']
        installed = replace(self.installed,payload=next_plist)
        # Runtime is a real temporary Writer lease, not a bypassed issuer.
        with patch.object(runtime_database,'load_installed_writer_contract',return_value=installed),patch.dict(os.environ,
            {**env,'DCAR_LOADED_BUILD_ID':'sha256:'+plan['child_build']['sha256']}):
            access = runtime_database.resolve_installed_database_access(runtime_database.DatabaseAccessMode.WRITER,
                database=self.f.db,project_root=self.f.project)
            with runtime_database.acquire_writer_lock(access):
                evidence = capture_release._installed_evidence(self.f.connection,at=self.installer.now(),maintenance_only=True)
                decisions = [capture_operator_release._decision(evidence,operation,self.installer.now()) for operation in sorted(authority.OPERATIONS)]
                decision = capture_operator_release._decision(evidence,'wechat_channels_video_comments',self.installer.now())
        self.assertEqual(decision['contract_version'],authority.DECISION_CONTRACT)
        self.assertEqual(decision['loaded_build'],plan['child_build'])
        self.assertTrue(all(item['loaded_build'] == plan['child_build'] for item in decisions))
        self.assertNotIn('wechat_channels_video_comments',old_authority.OPERATIONS)
        self.assertEqual(evidence['preparation_operation_authority'],self.inherited['preparation_operation_authority'])
        broken = dict(evidence);broken['four_platform_flow_successor'] = dict(evidence['four_platform_flow_successor'])
        broken['four_platform_flow_successor']['authorization_payload'] = {**broken['four_platform_flow_successor']['authorization_payload'],'historical_backfill_authorized':True}
        broken['four_platform_flow_successor']['proof_sha256'] = release.digest({k:v for k,v in broken['four_platform_flow_successor'].items() if k != 'proof_sha256'})
        with self.assertRaisesRegex(ValueError,'approved forward local scope'):
            authority.decision(broken,'kuaishou_video_statistics',self.installer.now())


class BackupGenerationTest(unittest.TestCase):
    def test_unchanged_backup_hash_reused_and_same_size_mutation_invalidates_it(self):
        import tempfile
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary).resolve()/'backup.sqlite3';path.write_bytes(b'original');path.chmod(0o600)
            ref = release.file_reference(path)
            with patch.object(release,'file_reference',wraps=release.file_reference) as hashing:
                release.verify_backup(ref);release.verify_backup(ref)
                self.assertEqual(hashing.call_count,1)
                path.write_bytes(b'modified')
                with self.assertRaisesRegex(ValueError,'backup changed'):
                    release.verify_backup(ref)
                self.assertEqual(hashing.call_count,2)


class FourPlatformCodeOnlyChainTest(unittest.TestCase):
    """Frozen ancestor verifiers validate an indexed R3 predecessor in a temp DB."""

    def setUp(self):
        from tests import test_capture_work_index_install as index_fixtures
        frozen = Path('/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260913-four-platform-flow-v3')
        frozen_v5 = Path('/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260913-four-platform-flow-v5')
        frozen_v6 = Path('/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260913-four-platform-flow-v6')
        frozen_r1b = Path('/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260914-four-platform-flow-r1b')
        frozen_r2 = Path('/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260914-four-platform-flow-r2')
        frozen_r3 = Path('/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260914-four-platform-flow-r3')
        if not all(source.is_dir() for source in (frozen, frozen_v5, frozen_v6, frozen_r1b, frozen_r2, frozen_r3)):
            self.skipTest('Immutable schema23 v3/v5/v6/R1b/R2/R3 source fixture is unavailable')
        self.h = h = index_fixtures.CaptureWorkIndexInstallTest()
        h.setUp()
        self.addCleanup(h.doCleanups)
        self.f = h.f
        self.index_ref = h.installer.install(h.args)
        instruction = self.f.root/'chain-instruction.txt'
        instruction.write_text('Offline fixture only: local code repair; no new DDL or historical backfill.')
        instruction.chmod(0o600)
        self.instruction = instruction
        self.origin = h.prepare_cli.prepare(SimpleNamespace(approve_local_capture=True,
            user_instruction_file=instruction, source_thread_id='offline-chain-fixture', actor='offline fixture',
            reason='temporary frozen v3 predecessor', parent_build=h.args.parent_build,
            parent_install=h.args.parent_install, check_report=h.args.check_report,
            installed_plist=h.args.installed_plist, migration=Path(h.migration['path']),
            evidence_root=self.f.root/'chain-v3-prepared', code_predecessor_build=h.args.code_predecessor_build,
            index_install=Path(self.index_ref['path'])))
        self.assertEqual((h.source/release.MODULE).read_bytes(), (frozen/release.MODULE).read_bytes())
        h.args.installed_plist.write_bytes(Path(self.origin['next_plist']['path']).read_bytes())
        # Construct each disposable predecessor with its actual frozen verifier,
        # never the current narrowed scope or widened ancestor rules.
        import importlib, types
        previous_source = h.source
        for version, frozen_parent in (('v5', frozen_v5), ('v6', frozen_v6), ('r1b', frozen_r1b), ('r2', frozen_r2), ('r3', frozen_r3)):
            package_name = '_fixture_frozen_' + version + '_' + self.f.root.name.replace('-', '_')
            package = types.ModuleType(package_name)
            package.__path__ = [str(frozen_parent/'src/dcar_eval/v8')]
            package.__package__ = package_name
            sys.modules[package_name] = package
            parent_release = importlib.import_module(package_name+'.four_platform_flow_release')
            self.parent_source = self.f.root/('chain-'+version+'-source')
            shutil.copytree(previous_source, self.parent_source)
            for name in parent_release.CODE_ONLY_REPAIR_FILES:
                source = frozen_parent/name
                if source.is_file():
                    target = self.parent_source/name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(source, target)
                    target.chmod(source.stat().st_mode & 0o777)
            parent_checks = self.checks_for(self.parent_source, 'chain-'+version)
            with patch.object(h.prepare_cli, 'ROOT', self.parent_source), patch.object(h.prepare_cli, 'release', parent_release):
                self.origin = h.prepare_cli.prepare(SimpleNamespace(approve_local_capture=True,
                    user_instruction_file=instruction, source_thread_id='offline-chain-fixture', actor='offline fixture',
                    reason='temporary actual frozen '+version+' predecessor', parent_build=h.args.parent_build,
                    parent_install=h.args.parent_install, check_report=parent_checks,
                    installed_plist=h.args.installed_plist, migration=Path(h.migration['path']),
                    evidence_root=self.f.root/('chain-'+version+'-prepared'),
                    code_predecessor_build=Path(self.origin['child_build']['path']),
                    inherited_index_install=Path(self.index_ref['path'])))
            self.assertEqual((self.parent_source/release.MODULE).read_bytes(), (frozen_parent/release.MODULE).read_bytes())
            h.args.installed_plist.write_bytes(Path(self.origin['next_plist']['path']).read_bytes())
            previous_source = self.parent_source
        self.parent = release.payload_at(self.origin['child_build'], 'sealed-build-receipt-v1')
        self.source = self.f.root/'chain-v8-source'
        shutil.copytree(self.parent_source, self.source)
        for name in release.CODE_ONLY_REPAIR_FILES:
            source = ROOT/name
            if source.is_file():
                target = self.source/name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
                target.chmod(source.stat().st_mode & 0o777)
        self.tree = release.inventory(self.source)
        self.tree_ref = self.f.write('chain-v8-tree.json', self.tree)
        self.args = SimpleNamespace(approve_local_capture=True, user_instruction_file=instruction,
            source_thread_id='offline-chain-fixture', actor='offline fixture', reason='bounded forward discovery coverage recovery',
            parent_build=h.args.parent_build, parent_install=h.args.parent_install,
            check_report=self.checks_for(self.source, 'chain-v8'),
            installed_plist=h.args.installed_plist, migration=Path(h.migration['path']),
            evidence_root=self.f.root/'chain-v8-prepared', code_predecessor_build=Path(self.origin['child_build']['path']),
            inherited_index_install=Path(self.index_ref['path']))

    def checks_for(self, source, label):
        tree = release.inventory(source)
        tree_ref = self.f.write(label+'-tree.json', tree)
        changes = release.source_changes(self.h.h.fixture.tree, tree)
        checks = {}
        for name in sorted(release.CHECKS):
            log = self.f.root/(label+'-'+name+'.log')
            log.write_text('Offline disposable chained-code fixture; no production acceptance.')
            log.chmod(0o600)
            checks[name] = self.f.write(label+'-'+name+'.json', {
                'contract':release.CHECK_CONTRACT,'name':name,'status':'passed','exit_code':0,
                'source_tree':tree_ref,'changes':changes,'command':['offline-chain-fixture'],
                'output':release.reference(log)})
        return [name+'='+ref['path'] for name,ref in checks.items()]

    def prepare(self, **overrides):
        args = SimpleNamespace(**{**vars(self.args), **overrides})
        with patch.object(self.h.prepare_cli, 'ROOT', self.source):
            return self.h.prepare_cli.prepare(args)

    def verify(self, child, name):
        ref = self.f.write(name+'.json', {'contract_version':'sealed-build-receipt-v1',
            'payload':child,'payload_sha256':release.digest(child)})
        return release.verify_inheritance(build=child, build_ref=ref, install_path=self.args.parent_install,
            database=self.f.db, source=self.source, connection=self.f.connection)

    def test_full_frozen_v6_v5_v3_chain_survives_unrelated_catalog_change(self):
        from v8 import account_intake, capture_release, runtime_evidence_context as evidence
        from v8.storage import now_utc, transaction

        proposal = self.prepare()
        next_body = Path(proposal['next_plist']['path']).read_bytes()
        self.args.installed_plist.write_bytes(next_body)
        next_plist = plistlib.loads(next_body)
        installed = replace(self.h.h.installed, payload=next_plist)
        # These are the same temporary installed source/environment bindings
        # used by the release fixture, not mocked inheritance/lease decisions.
        with patch.object(runtime_database, 'load_installed_writer_contract', return_value=installed), \
                patch.dict(os.environ, {**next_plist['EnvironmentVariables'],
                    'DCAR_LOADED_BUILD_ID':'sha256:'+proposal['child_build']['sha256']}), \
                patch.object(evidence, '_loaded_source_root', return_value=self.source):
            access = runtime_database.resolve_installed_database_access(runtime_database.DatabaseAccessMode.WRITER,
                database=self.f.db, project_root=self.f.project)
            with runtime_database.acquire_writer_lock(access):
                connection = self.f.connection
                before_proof = schema_v23.migration_proof(connection)
                before_usage = connection.execute('SELECT count(*) FROM provider_usage').fetchone()[0]
                observed = set()
                recognize = evidence._legacy_catalog_structure_read
                def observe(sql, arguments, caller):
                    accepted = recognize(sql, arguments, caller)
                    if accepted:
                        observed.add(str(Path(caller.f_code.co_filename).parents[3]))
                    return accepted
                with patch.object(evidence, '_legacy_catalog_structure_read', side_effect=observe):
                    with evidence.prepare_inheritance(self.f.db) as prepared:
                        self.assertTrue({str(self.parent_source), str(self.f.root/'chain-v5-source'),
                                         str(self.h.source)} <= observed, observed)
                        sql = [key[0] for key, _ in prepared.queries]
                        self.assertNotIn(evidence._LEGACY_CATALOG_STRUCTURE_SQL, sql)
                        self.assertEqual(sql.count(evidence._CATALOG_STRUCTURE_SQL), 1)
                        before_revision = connection.execute('SELECT revision FROM capture_catalog_revision').fetchone()[0]
                        with transaction(connection):
                            account_intake.submit_account_intake(connection, request_key='concurrent-chain-intake',
                                value={'platform':'kuaishou','uid':'123456789','account_status':'paused'},
                                source={'kind':'web'}, at=now_utc())
                        self.assertGreater(connection.execute('SELECT revision FROM capture_catalog_revision').fetchone()[0],
                                           before_revision)
                        with transaction(connection), evidence.inheritance_boundary(connection):
                            published = capture_release.publish_operation_gate(connection,
                                operation='wechat_channels_video_comments', at=now_utc(),
                                mirror_root=self.f.root/'concurrent-chain-gates')
                        self.assertEqual(published['provider_calls'], 0)
                self.assertEqual(schema_v23.migration_proof(connection), before_proof)
                self.assertEqual(connection.execute('SELECT count(*) FROM provider_usage').fetchone()[0], before_usage)
                self.assertIsNone(evidence._PREPARED.get())

    def test_r4_inherits_frozen_r3_index_and_original_migration_without_writes(self):
        from v8.schema_v22 import _table_digests
        before, schema = _table_digests(self.f.connection), schema_v23.objects(self.f.connection)
        original_migration = Path(self.h.migration['path']).read_bytes()
        original_index = Path(self.index_ref['path']).read_bytes()
        original_build = Path(self.origin['child_build']['path']).read_bytes()
        proposal = self.prepare()
        child = release.payload_at(proposal['child_build'],'sealed-build-receipt-v1')
        repair = child[release.FIELD]['code_predecessor']
        self.assertEqual(repair['contract'], release.CODE_ONLY_CONTRACT)
        self.assertEqual(repair['scope'], release.CODE_ONLY_SCOPE)
        self.assertTrue(set(repair['changes']) <= release.CODE_ONLY_REPAIR_FILES)
        for name in ('src/dcar_eval/v8/capture_runtime.py',
                     'src/dcar_eval/v8/capture_day_coverage.py',
                     'src/dcar_eval/v8/capture_discovery_recovery.py',
                     'src/dcar_eval/v8/providers.py'):
            self.assertIn(name, repair['changes'])
        for name in ('src/dcar_eval/v8/capture_repair.py',
                     'src/dcar_eval/v8/capture_repair_fixed.py',
                     'src/dcar_eval/v8/capture_compensation.py',
                     'src/dcar_eval/v8/capture_transport_recovery.py',
                     'src/dcar_eval/v8/provider_transport.py',
                     'src/dcar_eval/v8/runtime_phase_timing.py',
                     'src/dcar_eval/v8/operations.py', 'src/dcar_eval/v8/spu_audience.py',
                     'src/dcar_eval/v8/capture_shared_requests.py',
                     'src/dcar_eval/v8/runtime_receipts.py'):
            self.assertNotIn(name, repair['changes'])
        self.assertEqual(repair['database_writes'], 0)
        self.assertFalse(repair['schema_migration_repeated'])
        self.assertEqual(repair['build'], self.origin['child_build'])
        self.assertEqual(repair['inherited_index_install'], self.index_ref)
        self.assertNotIn('index_install', repair)
        proof = self.verify(child,'verified-v7')
        self.assertEqual(proof['four_platform_flow_proof']['code_predecessor'], repair)
        self.assertEqual(child[release.FIELD]['migration'], self.parent[release.FIELD]['migration'])
        self.assertNotEqual(child[release.FIELD]['source_tree'], self.parent[release.FIELD]['source_tree'])
        self.assertEqual(_table_digests(self.f.connection), before)
        self.assertEqual(schema_v23.objects(self.f.connection), schema)
        self.assertEqual(self.f.db.stat().st_ino, self.h.original_inode)
        self.assertEqual(Path(self.h.migration['path']).read_bytes(), original_migration)
        self.assertEqual(Path(self.index_ref['path']).read_bytes(), original_index)
        self.assertEqual(Path(self.origin['child_build']['path']).read_bytes(), original_build)
        # All metadata is re-sealed in these negative cases. A valid outer hash
        # cannot substitute a predecessor, new index binding or migration origin.
        wrong_index = release.object_at(self.index_ref)
        wrong_index = {**wrong_index, 'source_tree':self.tree_ref}
        wrong_index['receipt_sha256'] = release.digest({k:v for k,v in wrong_index.items() if k != 'receipt_sha256'})
        wrong_ref = self.f.write('wrong-inherited-index.json',wrong_index)
        for key,value in (('proof_sha256','0'*64),('database_writes',1),('database_writes',False),
                ('inherited_index_install',wrong_ref),('build',self.h.origin_ref),
                ('scope','catalog_runtime_coverage_stale_read_recovery')):
            broken = {**child,release.FIELD:{**child[release.FIELD],
                'code_predecessor':{**repair,key:value}}}
            with self.subTest(field=key), self.assertRaises(ValueError):
                self.verify(broken,'bad-chain-'+key)
        migration = release.object_at(self.h.migration)
        migration = {**migration,'source_tree':self.parent[release.FIELD]['source_tree'],
            'checks':self.parent[release.FIELD]['checks']}
        migration['receipt_sha256'] = release.digest({k:v for k,v in migration.items() if k != 'receipt_sha256'})
        migration_ref = self.f.write('wrong-chain-migration.json',migration)
        with self.assertRaisesRegex(ValueError,'verified predecessor|index or migration'):
            self.verify({**child,release.FIELD:{**child[release.FIELD],'migration':migration_ref}},'bad-chain-migration')
        for name in ('src/dcar_eval/v8/schema_v22.py','src/dcar_eval/v8/capture_work_index.py',
                'src/dcar_eval/v8/provider_budget.py'):
            path = self.source/name; body = path.read_bytes();path.write_bytes(body+b'\n# forbidden repair\n')
            try:
                with self.subTest(extra_file=name), self.assertRaisesRegex(ValueError,'reviewed execution files'):
                    release.code_repair_changes(self.parent, release.inventory(self.source), code_only=True)
            finally:
                path.write_bytes(body)
        parent_entry = self.parent_source/release.MODULE
        body = parent_entry.read_bytes();parent_entry.write_bytes(body+b'\n# changed ancestor\n')
        try:
            with self.assertRaisesRegex(ValueError,'predecessor source changed'):
                self.verify(child,'bad-chain-source')
        finally:
            parent_entry.write_bytes(body)
        self.f.connection.execute('CREATE INDEX forbidden_chain_index ON capture_work_items(account_id)')
        self.f.connection.commit()
        try:
            with self.assertRaises(ValueError):
                self.verify(child,'bad-chain-ddl')
        finally:
            self.f.connection.execute('DROP INDEX forbidden_chain_index');self.f.connection.commit()

    def test_chained_prepare_rejects_new_index_or_missing_inherited_reference(self):
        with self.assertRaisesRegex(ValueError,'mutually exclusive'):
            self.prepare(index_install=Path(self.index_ref['path']))
        with self.assertRaisesRegex(ValueError,'inherited index receipt'):
            self.prepare(inherited_index_install=None)


class FourPlatformCodeChainGuardTest(unittest.TestCase):
    def test_code_only_repair_does_not_expand_frozen_v3_index_allowlist(self):
        frozen = Path('/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260913-four-platform-flow-v3') / release.MODULE
        if not frozen.is_file():
            self.skipTest('Immutable schema23 v3 source fixture is unavailable')
        assignments = [node for node in ast.parse(frozen.read_text()).body
            if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name)
                and target.id == 'CODE_REPAIR_FILES' for target in node.targets)]
        self.assertEqual(len(assignments), 1)
        expression = assignments[0].value
        self.assertIsInstance(expression, ast.Call)
        self.assertEqual(expression.func.id, 'frozenset')
        self.assertEqual(len(expression.args), 1)
        original = set()
        for item in expression.args[0].elts:
            if isinstance(item, ast.Name):
                self.assertEqual(item.id, 'MODULE')
                original.add(release.MODULE)
            else:
                original.add(ast.literal_eval(item))
        self.assertEqual(release.CODE_REPAIR_FILES, frozenset(original))
        self.assertFalse({'src/dcar_eval/v8/capture_work_index.py', 'src/dcar_eval/v8/provider_budget.py',
            'src/dcar_eval/tikhub_config.py', 'src/dcar_eval/v8/provider_transport.py',
            'src/dcar_eval/v8/account_profile_recovery.py',
            'src/dcar_eval/v8/usage_settlements.py'} & release.CODE_ONLY_REPAIR_FILES)
        self.assertEqual(release.CODE_ONLY_SCOPE, 'bounded_forward_discovery_coverage_recovery')
        self.assertEqual(release.CODE_ONLY_REPAIR_FILES, frozenset({release.MODULE,
            'docs/four-platform-flow-release.md',
            'tests/test_four_platform_flow_release.py',
            'src/dcar_eval/v8/capture_runtime.py',
            'src/dcar_eval/v8/capture_discovery_recovery.py',
            'src/dcar_eval/v8/capture_day_coverage.py',
            'src/dcar_eval/v8/providers.py',
            'tests/test_v23_discovery_recovery.py'}))

    def test_r4_source_requires_discovery_recovery_and_inherited_execution_modules(self):
        rows = [{'path': name, 'sha256': '1'*64, 'byte_size': 1, 'mode': 0o644}
                for name in release.CODE_ONLY_REQUIRED_SOURCE]
        original = {'contract': 'writer-source-tree-v1', 'files': rows}
        parent = {'account_cleanup_generation': {'source_tree': {'path': '/offline/parent.json'}}}
        for missing in ('deploy/macos/run_writer_worker.sh', 'scripts/run_capture_repair.py',
                        'src/dcar_eval/v8/capture_repair.py', 'src/dcar_eval/v8/runtime_phase_timing.py',
                        'src/dcar_eval/v8/capture_transport_recovery.py', 'src/dcar_eval/v8/capture_repair_fixed.py',
                        'src/dcar_eval/v8/capture_compensation.py',
                        'src/dcar_eval/v8/capture_discovery_recovery.py'):
            tree = {'contract': 'writer-source-tree-v1',
                    'files': [row for row in rows if row['path'] != missing]}
            with patch.object(release, 'object_at', return_value=original), self.subTest(missing=missing), \
                    self.assertRaisesRegex(ValueError, 'required repair execution'):
                release.code_repair_changes(parent, tree, code_only=True)

    def test_r4_discovery_delta_rejects_transport_billing_schema_profile_and_permission_changes(self):
        def row(name, checksum='1', mode=0o644):
            return {'path': name, 'sha256': checksum*64, 'byte_size': 1, 'mode': mode}
        parent = {'account_cleanup_generation': {'source_tree': {'path': '/offline/parent.json'}}}
        entry = 'src/dcar_eval/v8/capture_discovery_recovery.py'
        original = {'contract': 'writer-source-tree-v1',
                    'files': [row(name, '0' if name in (release.MODULE, entry) else '1')
                              for name in release.CODE_ONLY_REQUIRED_SOURCE]}
        tree = {'contract': 'writer-source-tree-v1',
                'files': [row(name) for name in release.CODE_ONLY_REQUIRED_SOURCE]}
        with patch.object(release, 'object_at', return_value=original):
            self.assertEqual(set(release.code_repair_changes(parent, tree, code_only=True)),
                             {release.MODULE, entry})
            for forbidden in ('src/dcar_eval/tikhub_config.py',
                              'src/dcar_eval/v8/provider_transport.py',
                              'src/dcar_eval/v8/capture_transport_recovery.py',
                              'src/dcar_eval/v8/account_profile_recovery.py',
                              'config/source_routing_operation_field_v4.json',
                              'src/dcar_eval/v8/schema_v23.py', 'src/dcar_eval/v8/provider_budget.py',
                              'src/dcar_eval/v8/usage_settlements.py'):
                with self.subTest(forbidden=forbidden), self.assertRaisesRegex(ValueError, 'reviewed execution files'):
                    replaced = [item for item in tree['files'] if item['path'] != forbidden]+[row(forbidden, '2')]
                    release.code_repair_changes(parent, {**tree, 'files': replaced}, code_only=True)
            with self.assertRaisesRegex(ValueError, 'deleted files or changed permissions'):
                changed_mode = [{**item, 'mode': 0o755} if item['path'] == release.MODULE else item
                                for item in tree['files']]
                release.code_repair_changes(parent, {**tree, 'files': changed_mode}, code_only=True)

    def test_cyclic_and_excessive_chains_fail_before_importing_verifiers(self):
        ref = {'path':'/offline/build.json','sha256':'1'*64}
        payload = {'status':'succeeded','schema_contract':{'code_schema':23,'formal_schema':23},
            release.FIELD:{'contract':release.CONTRACT,'code_predecessor':{'build':ref}}}
        with patch.object(release,'payload_at',return_value=payload), self.assertRaisesRegex(ValueError,'cyclic'):
            release._validate_code_chain(ref)
        def following(current, _contract):
            n = int(current['sha256'],16)+1
            return {**payload,release.FIELD:{'contract':release.CONTRACT,
                'code_predecessor':{'build':{'path':f'/offline/{n}.json','sha256':f'{n:064x}'}}}}
        with patch.object(release,'payload_at',side_effect=following), self.assertRaisesRegex(ValueError,'depth limit'):
            release._validate_code_chain({'path':'/offline/0.json','sha256':'0'*64})


if __name__ == '__main__':
    unittest.main()
