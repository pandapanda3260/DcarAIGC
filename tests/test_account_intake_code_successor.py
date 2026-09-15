"""Disposable schema22 fixtures; no runtime installation or paid authority writes."""
from __future__ import annotations

import ast
import copy
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import importlib.util
import os
from pathlib import Path
import plistlib
import shutil
import sys
from threading import Barrier
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tests import test_account_intake_release as fixtures
from v8 import account_intake_release as intake, account_intake_code_successor as code, runtime_paths
from v8 import account_preparation_authority as authority
from v8.profile_activations import activation_at

CURRENT_ROOT = Path(__file__).resolve().parents[1]
# This suite protects the immutable schema22 successor contract. Schema23 has
# a separate release/authority suite and must never pass as a schema22 delta.
ROOT = Path('/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260912-account-intake-v9')
FROZEN_PARENT = Path('/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260912-account-intake-v3')
FROZEN_PREVIOUS = Path('/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260912-account-intake-v4')
AT = fixtures.AT


class AdapterCodeBoundaryTest(unittest.TestCase):
    def setUp(self):
        if not FROZEN_PARENT.is_dir() or not ROOT.is_dir():
            self.skipTest('This boundary compares the immutable original F3 adapter')
        self.parent = (FROZEN_PARENT/code.ADAPTERS).read_bytes()
        self.current = (ROOT/code.ADAPTERS).read_text()

    def test_exact_filter_normalizes_to_original_complete_adapter(self):
        self.assertEqual(code._stable_adapter_nodes(self.parent), code._stable_adapter_nodes(self.current.encode()))
        self.assertIn(code.ADAPTERS, code.ALLOWED_FILES)
        self.assertIn('tests/test_v8_wechat_mixed_search.py', code.ALLOWED_FILES)

    def test_filter_cannot_accept_unclassified_or_contradictory_candidates(self):
        for old, new in (
            ('item.get("accTypeName") == "公众号"', 'True'),
            ('r"gh_[0-9a-f]{12}"', 'r".*"'),
            ('notice.get("finderUsername") in (None, "")', 'True'),
            ('if not valid_uid("wechat_channels", value):', 'if False:'),
            ('if len(candidates) > 10:', 'if len(candidates) > 100:'),
            ('params.get("channel_id") != channel', 'False'),
            ('raise PlatformAdapterError("channel_id_evidence_missing")', 'return "unchecked"'),
        ):
            with self.subTest(old=old):
                changed = self.current.replace(old, new, 1)
                self.assertNotEqual(changed, self.current)
                try:
                    result = code._stable_adapter_nodes(changed.encode())
                except ValueError:
                    continue
                self.assertNotEqual(code._stable_adapter_nodes(self.parent), result)

    def test_filter_cannot_move_after_candidate_parsing(self):
        marker = '                add(jump.get("userName"))\n'
        changed = self.current.replace(marker, '', 1).replace(
            '                if (item.get("accTypeName") == "公众号"',
            marker + '                if (item.get("accTypeName") == "公众号"', 1)
        self.assertNotEqual(changed, self.current)
        with self.assertRaisesRegex(ValueError, 'filter position'):
            code._stable_adapter_nodes(changed.encode())


class PreparationCodeBoundaryTest(unittest.TestCase):
    def setUp(self):
        if not FROZEN_PARENT.is_dir() or not ROOT.is_dir():
            self.skipTest('This boundary compares the immutable original F3 preparation code')
        self.parent = (FROZEN_PARENT/code.PLANNING).read_bytes()
        self.current = (ROOT/code.PLANNING).read_text()

    def test_approved_recovery_retains_original_retry_and_paid_execution_ast(self):
        self.assertEqual(code._stable_planning_nodes(self.parent), code._stable_planning_nodes(self.current.encode()))

    def test_parser_replay_cannot_widen_failed_raw_or_paid_fallback(self):
        for old, new in (
            ('and attempt["slot_status"] == "terminal_failed" and attempt["error_code"] == "invalid_finder_candidate"',
             'and attempt["slot_status"] == "terminal_failed"'),
            ('and raw["http_status"] == 200 and error_code is None and next_target is not None',
             'and next_target is not None'),
            ('and next_target["operation"] == "wechat_channels_channel_info"', 'and True'),
            ('if _retry_evidence(connection, previous_work_id=proof["previous_work_id"], request=request, target=target) != proof:',
             'if False:'),
            ('request=_current_request(connection, intake_id), target=target)', 'request=request, target=target)'),
            ('except capture.SlotUnavailable:', 'except Exception:'),
            ('budget_id = providers._budget_for_call(', 'budget_id = skipped_budget_check('),
            ('outcome = capture.execute_intake_fetch(', 'outcome = unchecked_provider_fetch('),
        ):
            with self.subTest(old=old):
                changed = self.current.replace(old, new, 1)
                self.assertNotEqual(changed, self.current)
                try:
                    result = code._stable_planning_nodes(changed.encode())
                except ValueError:
                    continue
                self.assertNotEqual(code._stable_planning_nodes(self.parent), result)

    def test_parser_replay_requires_its_exact_offline_loader(self):
        module = ast.parse(self.current)
        module.body = [node for node in module.body if not (
            isinstance(node, ast.FunctionDef) and node.name == '_load_resolver_parser_replay')]
        with self.assertRaisesRegex(ValueError, 'must remain coupled'):
            code._stable_planning_nodes(ast.unparse(module).encode())

    def test_never_sent_recovery_cannot_drop_send_evidence_shadow_or_transaction_guards(self):
        for old, new in (
            ('if [event.event_type for event in events] != ["reserved", "not_sent"]:', 'if False:'),
            ('usage["amount"] != 0 or usage["request_attempts"] != 0', 'usage["amount"] != 999'),
            ('if not shadow and _recover_never_sent_preparation(', 'if _recover_never_sent_preparation('),
            ('if not connection.in_transaction:\n        raise ValueError("preparation planning requires a writer transaction")',
             'if False:\n        raise ValueError("preparation planning requires a writer transaction")'),
            ("UPDATE fetch_slots SET status='retryable_failed',updated_at=? WHERE id=?",
             "UPDATE fetch_slots SET status='succeeded',updated_at=? WHERE id=?"),
        ):
            with self.subTest(old=old):
                changed = self.current.replace(old, new, 1)
                self.assertNotEqual(changed, self.current)
                with self.assertRaisesRegex(ValueError, 'non-planning or payment'):
                    code._stable_planning_nodes(changed.encode())

    def test_never_sent_recovery_requires_both_exact_helpers(self):
        for name in ('_never_sent_reservation_evidence', '_recover_never_sent_preparation'):
            with self.subTest(name=name):
                module = ast.parse(self.current)
                module.body = [node for node in module.body if not (
                    isinstance(node, ast.FunctionDef) and node.name == name)]
                with self.assertRaisesRegex(ValueError, 'must remain coupled'):
                    code._stable_planning_nodes(ast.unparse(module).encode())


class AdmissionRefreshCodeBoundaryTest(unittest.TestCase):
    def setUp(self):
        if not FROZEN_PARENT.is_dir() or not ROOT.is_dir():
            self.skipTest('This boundary compares the immutable original F3 capture code')
        self.parent = (FROZEN_PARENT/code.CAPTURE).read_bytes()
        self.current = (ROOT/code.CAPTURE).read_text()

    def test_exact_amount_day_refresh_preserves_original_capture_ast(self):
        self.assertEqual(code._stable_capture_nodes(self.parent), code._stable_capture_nodes(self.current.encode()))

    def test_admission_refresh_cannot_change_sent_guard_amount_or_paid_parameters(self):
        for old, new in (
            ("WHERE admission_reservations.state='released_unsent'", 'WHERE 1=1'),
            ('amount_microusd=excluded.amount_microusd', 'amount_microusd=0'),
            ('charge_business_day=excluded.charge_business_day', "charge_business_day='2000-01-01'"),
            ('(request_batch_id, micro_usd(unit_price), budget_day(claimed_at), claimed_at,',
             '(request_batch_id, 0, budget_day(claimed_at), claimed_at,'),
        ):
            with self.subTest(old=old):
                changed = self.current.replace(old, new, 1)
                self.assertNotEqual(changed, self.current)
                try:
                    result = code._stable_capture_nodes(changed.encode())
                except ValueError:
                    continue
                self.assertNotEqual(code._stable_capture_nodes(self.parent), result)


class IntakePlanningCodeSuccessorTest(unittest.TestCase):
    def setUp(self):
        if not FROZEN_PARENT.is_dir() or not ROOT.is_dir():
            self.skipTest('This exact F3 successor fixture requires the immutable local F3 source')
        self.fixture = fixture = fixtures.AccountIntakeReleaseTest()
        # Use the real original F3 verifier bytes against temporary migration
        # receipts; never fabricate a production migration or open its database.
        with patch.object(fixtures, 'ROOT', FROZEN_PARENT), patch.object(intake, 'REQUIRED_SOURCE', intake.REQUIRED_SOURCE | {code.PIPELINE}):
            fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        f = fixture.fixture; self.f = f
        plan = fixture.build['account_intake_successor']; active = activation_at(f.connection, AT)
        approval = {'contract':authority.AUTHORIZATION_CONTRACT,'scope':'local_writer_capture_only','schema_version':22,
            'operations':sorted(authority.OPERATIONS),'platforms':list(authority.PLATFORMS),'manual_statuses':list(authority.STATUSES),
            'qualification':'operator_authorized','business_e2e':'required','transport_qualification':'not_verified',
            'publisher_authorized':False,'remote_database_authorized':False,
            'parent_build':fixture.parent_ref,'source_tree':fixture.tree_ref,'migration':fixture.migration_ref,
            'formal_database':{'path':str(f.db),**fixture.migration['database_identity']},
            'catalog_policy_sha256':plan['account_catalog_policy_sha256'],
            'activation':{key:active[key] for key in authority.ACTIVE_KEYS},
            'actor':'temporary fixture','reason':'offline local authority fixture','user_instruction':'Local fixture only',
            'source_thread_id':'offline-fixture','issued_at':AT}
        plan.update(operation_authorization=f.write('local-approval.json',approval),
                    production_rollout='not_authorized',local_activation='approved_by_user')
        fixture.seal(); f.connection.commit()
        self.parent_ref = intake.reference(Path(fixture.build_ref['path']))
        self.parent, self.inherited = code.parent_context(self.parent_ref, install_path=f.install_path,
            database=f.db, at=AT, connection=f.connection)
        self.source = f.root/'repaired-source'; shutil.copytree(fixture.source,self.source)
        source_repository = FROZEN_PREVIOUS if self._testMethodName == 'test_prepare_replaces_installed_F4_without_changing_F3_authority_or_database' else ROOT
        copied_files = code.ALLOWED_FILES
        if source_repository == FROZEN_PREVIOUS:
            # Recreate the actual predecessor's delta, not files newly allowed
            # by this successor. Its immutable verifier must still accept it.
            with code._parent_module(source_repository, 'fixture-previous') as verifier:
                copied_files = importlib.import_module(verifier.__package__+'.account_intake_code_successor').ALLOWED_FILES
        for name in copied_files:
            if (source_repository/name).is_file():
                target=self.source/name; target.parent.mkdir(parents=True,exist_ok=True); shutil.copyfile(source_repository/name,target)
        self.tree = intake.inventory(self.source); self.tree_ref=f.write('repair-tree.json',self.tree)
        self.changes = code.source_changes(fixture.tree,self.tree)
        self.checks={}
        for name in sorted(code.CHECKS):
            log=f.root/(name+'.log');log.write_text('Temporary fixture check, not live acceptance.\n');log.chmod(0o600)
            self.checks[name]=f.write(name+'.json',{'contract':code.CHECK_CONTRACT,'name':name,
                'source_tree':self.tree_ref,'changes':self.changes,'command':['offline-fixture'],
                'status':'passed','exit_code':0,'output':intake.reference(log)})
        source_plan=f.write('repair-source-plan.json',{'contract':'account-cleanup-source-plan-v1',
            'transition':'account-cleanup-0907-v1','project_root':self.parent['project_root'],'source_root':str(self.source),
            'git':self.tree['git'],'source_tree':self.tree_ref})
        identity=f.db.stat()
        self.plan={'contract':code.CONTRACT,'parent_build':self.parent_ref,'source_tree':self.tree_ref,
            'changes':self.changes,'checks':self.checks,'issued_at':AT,'actor':'offline fixture','reason':'planning repair fixture',
            'scope':code.repair_scope(self.changes),'schema_migration_repeated':False,'database_writes':0,
            'paid_gates_reopened':False,'publisher_authorized':False,'remote_database_authorized':False,
            'business_scope_change':'none','database_identity':{'path':str(f.db),'device':identity.st_dev,'inode':identity.st_ino},
            'parent_intake_proof_sha256':self.inherited['intake_proof']['proof_sha256'],
            'parent_operation_authority_sha256':self.inherited['preparation_operation_authority']['proof_sha256']}
        self.build={**self.parent,'source_root':str(self.source),'git':self.tree['git'],
            'critical_files':{name:row['sha256'] for name,row in intake.records(self.tree).items()
                if name.startswith(('src/','config/')) and name.endswith(('.py','.json'))},
            'code_successor_plan':source_plan,
            'account_cleanup_generation':{**self.parent['account_cleanup_generation'],'source_tree':self.tree_ref},
            code.FIELD:self.plan,'created_at':AT,'validation_scope':'offline fixture'}
        self.seal()

    def seal(self):
        result=self.f.envelope('repair-build.json','sealed-build-receipt-v1',self.build)
        self.ref=intake.reference(Path(result['path']))

    def verify(self):
        return intake.verify_inheritance(build=self.build,build_ref=self.ref,install_path=self.f.install_path,
            database=self.f.db,source=self.source,at=AT,connection=self.f.connection)

    def test_inherits_original_migration_and_local_authority_without_writes(self):
        before=self.f.connection.total_changes; result=self.verify()
        self.assertEqual(result['intake_proof'],self.inherited['intake_proof'])
        self.assertEqual(result['preparation_operation_authority'],self.inherited['preparation_operation_authority'])
        self.assertEqual(result['intake_code_proof']['loaded_build'],self.ref)
        self.assertEqual(self.f.connection.total_changes,before)

    def test_parent_import_namespaces_are_independent_during_concurrent_verification(self):
        barrier=Barrier(2)
        def load():
            with code._parent_module(self.fixture.source,self.parent_ref['sha256']) as module:
                name=module.__package__
                barrier.wait(timeout=10)
                self.assertIs(sys.modules[name+'.account_intake_release'],module)
                return name
        with ThreadPoolExecutor(max_workers=2) as executor:
            names=list(executor.map(lambda _:load(),range(2)))
        self.assertNotEqual(*names)
        self.assertFalse(any(key==name or key.startswith(name+'.') for key in sys.modules for name in names))

    def test_real_installed_catalog_and_operator_decisions_keep_original_authority(self):
        from v8 import runtime_database, account_catalog_capture, capture_release, capture_operator_release
        installed=runtime_database.load_installed_writer_contract(required=True)
        env={**self.f.child_env,'DCAR_WRITER_SOURCE_ROOT':str(self.source),
             'DCAR_LOADED_BUILD_RECEIPT':self.ref['path']}
        current=replace(installed,payload={**installed.payload,'EnvironmentVariables':env})
        before=self.f.connection.total_changes
        with patch.dict(os.environ,{**env,'DCAR_LOADED_BUILD_ID':'sha256:'+self.ref['sha256']}), patch.object(runtime_database,'load_installed_writer_contract',return_value=current):
            evidence=capture_release._installed_evidence(self.f.connection,at=AT,maintenance_only=True)
            policy=account_catalog_capture.installed_policy(self.f.connection,at=AT,use_planning_cache=False)
            self.assertEqual(policy,self.inherited['catalog_capture_policy'])
            for operation in sorted(authority.OPERATIONS):
                decision=capture_operator_release._decision(evidence,operation,AT)
                self.assertEqual(decision['loaded_build'],self.parent_ref)
                self.assertEqual(decision['qualification'],'operator_authorized')
        self.assertEqual(self.f.connection.total_changes,before)

    def test_original_source_tamper_is_rejected_before_parent_module_execution(self):
        (self.fixture.source/code.ENTRY).write_text('raise AssertionError("must not execute")\n')
        with self.assertRaisesRegex(ValueError,'parent source changed'):
            self.verify()

    def test_child_cannot_replace_migration_authority_or_database_inode(self):
        for mutate in ('migration','authority','inode'):
            with self.subTest(mutate=mutate):
                original=copy.deepcopy(self.build)
                if mutate=='inode': self.build[code.FIELD]['database_identity']['inode']+=1
                else: self.build['account_intake_successor']['migration' if mutate=='migration' else 'operation_authorization']={'invented':'invalid'}
                self.seal()
                with self.assertRaisesRegex(ValueError,'authority changed|inode changed'):
                    self.verify()
                self.build=original;self.seal()

    def test_nonplanning_payment_change_and_unrelated_source_are_rejected(self):
        target=self.source/code.PLANNING;before=target.read_text()
        target.write_text(before.replace('def execute_step(', 'def replaced_execute_step(',1))
        with self.assertRaisesRegex(ValueError,'non-planning or payment'):
            code.source_changes(self.fixture.tree,intake.inventory(self.source))
        target.write_text(before)
        changed=before.replace('attempt["slot_status"] != "retryable_failed"',
                               'attempt["slot_status"] == "retryable_failed"',1)
        self.assertNotEqual(changed,before)
        target.write_text(changed)
        with self.assertRaisesRegex(ValueError,'non-planning or payment'):
            code.source_changes(self.fixture.tree,intake.inventory(self.source))
        target.write_text(before)
        changed=before.replace('(request["id"], envelope["logical_due"])).fetchone()',
                               '(request["id"], "wrong-window")).fetchone()',1)
        self.assertNotEqual(changed,before)
        target.write_text(changed)
        with self.assertRaisesRegex(ValueError,'non-planning or payment'):
            code.source_changes(self.fixture.tree,intake.inventory(self.source))
        target.write_text(before)
        runtime=self.source/code.RUNTIME; runtime_before=runtime.read_text()
        runtime.write_text(runtime_before.replace('def retry_backoff(', 'def changed_retry_backoff(',1))
        with self.assertRaisesRegex(ValueError,'runtime code outside _plan_due'):
            code.source_changes(self.fixture.tree,intake.inventory(self.source))
        runtime.write_text(runtime_before)
        (self.source/'src/unrelated.py').write_text('x = 1\n')
        with self.assertRaisesRegex(ValueError,'allowlist'):
            code.source_changes(self.fixture.tree,intake.inventory(self.source))

    def test_bootstrap_uses_existing_schema22_entry_with_new_full_tree(self):
        env={**self.f.child_env,'DCAR_WRITER_SOURCE_ROOT':str(self.source),'DCAR_LOADED_BUILD_RECEIPT':self.ref['path']}
        home=self.f.root/'repair-home';plist=home/'Library/LaunchAgents/cn.tj.dcar.writer-worker.plist'
        plist.parent.mkdir(parents=True);plist.write_bytes(plistlib.dumps({'Label':'cn.tj.dcar.writer-worker',
            'WorkingDirectory':str(self.f.project),'ProgramArguments':[str(self.source/'deploy/macos/run_writer_worker.sh')],
            'EnvironmentVariables':env}));plist.chmod(0o600)
        result=runtime_paths.verify_source_before_import(data=self.f.project,source=self.source,
            build_receipt=Path(self.ref['path']),home=home)
        self.assertEqual(result['files'],len(self.tree['files']))

    def test_periodic_recovery_cannot_drop_live_guard_change_fee_handling_or_widen_schema(self):
        for name,old,new in (
            (code.CAPTURE,'a.owner_token IS NOT NULL','a.owner_token IS NULL'),
            (code.CAPTURE,"a.status='running'","a.status='run ning'"),
            (code.CAPTURE,"last_error_code='interrupted'","last_error_code='cleared'"),
            (code.PIPELINE,'preserve_live_owners=True','preserve_live_owners=False'),
            (code.PIPELINE,'if schema_version == 22:','if schema_version in {20,21,22}:')):
            target=self.source/name; before=target.read_text(); changed=before.replace(old,new,1)
            self.assertNotEqual(changed,before)
            with self.subTest(name=name,old=old):
                target.write_text(changed)
                with self.assertRaisesRegex(ValueError,'periodic recovery'):
                    code.source_changes(self.fixture.tree,intake.inventory(self.source))
                target.write_text(before)

    def test_queue_repair_cannot_change_existing_readiness_claim_or_paid_execution(self):
        target=self.source/code.RUNTIME; before=target.read_text()
        for old,new in (
            ('state, reason = _readiness(connection, envelope, at=at)', 'state, reason = ("runnable", "")'),
            ('claim = _claim_work(connection, work, at=claim_at)', 'claim = None'),
            ('result = _execute_one(envelope, db_path=db_path, at=claim_at)', 'result = {"complete": True}'),
            ('filters=normal_operation + only, parameters=ids', 'filters=only, parameters=ids')):
            changed=before.replace(old,new,1); self.assertNotEqual(changed,before)
            with self.subTest(old=old):
                target.write_text(changed)
                with self.assertRaisesRegex(ValueError,'runtime'):
                    code.source_changes(self.fixture.tree,intake.inventory(self.source))
                target.write_text(before)

    def test_adapter_change_cannot_bypass_raw_identity_or_candidate_bounds(self):
        target = self.source/code.ADAPTERS
        before = target.read_text()
        for old, new in (
            ('if len(candidates) > 10:', 'if len(candidates) > 100:'),
            ('notice.get("finderUsername") in (None, "")', 'True'),
            ('raise PlatformAdapterError("channel_id_evidence_missing")', 'return "unchecked"'),
        ):
            with self.subTest(old=old):
                changed = before.replace(old, new, 1)
                self.assertNotEqual(changed, before)
                target.write_text(changed)
                with self.assertRaisesRegex(ValueError, 'platform adapter'):
                    code.source_changes(self.fixture.tree, intake.inventory(self.source))
                target.write_text(before)

    def test_rolling_bounds_schema_and_intake_semantics_remain_fixed(self):
        for name,old,new,reason in (
            (code.RUNTIME,'_ROLLING_MAX_ITEMS = 16','_ROLLING_MAX_ITEMS = 160','bounded selection'),
            (code.PIPELINE,'rolling=schema_version == 22','rolling=True','schema22 gate'),
            (code.INTAKE,"if result.get('status') == 'ready':","if result.get('status') != 'ready':",'display text')):
            target=self.source/name;before=target.read_text();changed=before.replace(old,new,1)
            self.assertNotEqual(changed,before)
            with self.subTest(name=name):
                target.write_text(changed)
                with self.assertRaisesRegex(ValueError,reason):
                    code.source_changes(self.fixture.tree,intake.inventory(self.source))
                target.write_text(before)

    def test_prepare_creates_backup_and_proposal_without_install_or_database_write(self):
        script=ROOT/'scripts/prepare_account_intake_code_successor.py'
        spec=importlib.util.spec_from_file_location('planning_prepare_fixture',script)
        cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
        env={**self.f.child_env,'DCAR_WRITER_SOURCE_ROOT':str(self.fixture.source),
            'DCAR_LOADED_BUILD_RECEIPT':self.parent_ref['path']}
        lock=self.f.root/'repair-prepare-writer.lock';lock.touch();env['DCAR_WRITER_LOCK']=str(lock)
        installed=self.f.root/'writer.installed.plist';installed.write_bytes(plistlib.dumps({
            'Label':'cn.tj.dcar.writer-worker','WorkingDirectory':str(self.f.project),
            'ProgramArguments':[str(self.fixture.source/'deploy/macos/run_writer_worker.sh')],'EnvironmentVariables':env}))
        before=installed.read_bytes(); changes=self.f.connection.total_changes
        args=SimpleNamespace(parent_build=Path(self.parent_ref['path']),installed_plist=installed,
            evidence_root=self.f.root/'repair-proposal',check_report=[name+'='+ref['path'] for name,ref in self.checks.items()],
            actor='offline fixture',reason='planning only')
        import fcntl
        with lock.open('r+') as owner, patch.object(cli,'ROOT',self.source):
            fcntl.flock(owner.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                cli.prepare(args)
            self.assertFalse(args.evidence_root.exists())
        with patch.object(cli,'ROOT',self.source):
            result=cli.prepare(args)
        self.assertTrue(Path(result['backup']['path']).is_file())
        self.assertFalse(result['services_changed']);self.assertEqual(result['database_writes'],0)
        self.assertEqual(installed.read_bytes(),before);self.assertEqual(self.f.connection.total_changes,changes)
        self.assertEqual(result['database_identity']['inode'],self.f.db.stat().st_ino)

    def test_prepare_replaces_installed_F4_without_changing_F3_authority_or_database(self):
        script=ROOT/'scripts/prepare_account_intake_code_successor.py'
        spec=importlib.util.spec_from_file_location('planning_prepare_next_fixture',script)
        cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
        next_source=self.f.root/'next-repaired-source';shutil.copytree(self.source,next_source)
        for name in code.ALLOWED_FILES:
            if (ROOT/name).is_file():
                target=next_source/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/name,target)
        tree=intake.inventory(next_source);tree_ref=self.f.write('next-repair-tree.json',tree)
        changes=code.source_changes(self.fixture.tree,tree)
        checks={}
        for name, ref in self.checks.items():
            checks[name]=self.f.write('next-'+name+'.json',{
                **intake.object_at(ref),'source_tree':tree_ref,'changes':changes})
        lock=self.f.root/'next-prepare-writer.lock';lock.touch()
        env={**self.f.child_env,'DCAR_WRITER_SOURCE_ROOT':str(self.source),
             'DCAR_LOADED_BUILD_RECEIPT':self.ref['path'],'DCAR_WRITER_LOCK':str(lock)}
        installed=self.f.root/'writer.previous.plist';installed.write_bytes(plistlib.dumps({
            'Label':'cn.tj.dcar.writer-worker','WorkingDirectory':str(self.f.project),
            'ProgramArguments':[str(self.source/'deploy/macos/run_writer_worker.sh')], 'EnvironmentVariables':env}))
        installed_before=installed.read_bytes();before=self.f.connection.total_changes
        previous_tree=intake.inventory(self.source);original_tree=intake.inventory(self.fixture.source)
        args=SimpleNamespace(parent_build=Path(self.parent_ref['path']),installed_build=Path(self.ref['path']),
            installed_plist=installed,evidence_root=self.f.root/'next-repair-proposal',
            check_report=[name+'='+ref['path'] for name,ref in checks.items()],actor='offline fixture',reason='planning only')
        with patch.object(cli,'ROOT',next_source):
            result=cli.prepare(args)
        child=intake.payload_at(result['child_build'],'sealed-build-receipt-v1')
        self.assertEqual(child[code.FIELD]['previous_code_build'],self.ref)
        self.assertEqual(child[code.FIELD]['parent_build'],self.parent_ref)
        self.assertEqual(child['account_intake_successor'],self.parent['account_intake_successor'])
        self.assertEqual(installed.read_bytes(),installed_before)
        self.assertEqual(intake.inventory(self.source),previous_tree)
        self.assertEqual(intake.inventory(self.fixture.source),original_tree)
        self.assertEqual(self.f.connection.total_changes,before)
        self.assertEqual(result['database_identity']['inode'],self.f.db.stat().st_ino)


class Schema23DoesNotBorrowLegacyAuthorityTest(unittest.TestCase):
    def test_current_four_platform_source_requires_its_separate_release(self):
        if not ROOT.is_dir():
            self.skipTest('Immutable schema22 successor source is not installed on this host')
        with self.assertRaises(ValueError):
            code.source_changes(intake.inventory(ROOT), intake.inventory(CURRENT_ROOT))


if __name__=='__main__':
    unittest.main()
