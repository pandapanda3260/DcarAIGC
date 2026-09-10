"""Verify the new catalog policy separately from inherited operational authority.

Temporary schema21 databases, Git trees and receipt fixtures exercise the real
bootstrap and historical inheritance chain. No service or provider is contacted.
"""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import plistlib
import re
import shutil
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import test_manual_content_scope_release as manual_fixtures
from v8 import account_catalog_capture_release as catalog, runtime_paths

AT = manual_fixtures.AT


class AccountCatalogCaptureReleaseTest(unittest.TestCase):
    def setUp(self):
        self.manual_fixture = m = manual_fixtures.ManualContentScopeReleaseTest()
        self.addCleanup(m.doCleanups)
        m.setUp()
        self.fixture = f = m.fixture
        self.parent = copy.deepcopy(m.build)
        self.parent_ref = dict(m.ref)
        self.baseline = m.verify()
        self.before = f.snapshots()
        self.source = f.root / 'catalog-source'
        shutil.copytree(m.source, self.source)
        path = self.source / 'src/dcar_eval/v8/api.py'
        old_sha = hashlib.sha256(path.read_bytes()).hexdigest()
        path.write_text('# reviewed catalog-membership fixture\n')
        changes = {'src/dcar_eval/v8/api.py': {'before_sha256': old_sha,
            'after_sha256': hashlib.sha256(path.read_bytes()).hexdigest()}}
        body = catalog._LOADED_SOURCE.decode()
        body = re.sub(r'^PARENT_BUILD_SHA256 = .*$', 'PARENT_BUILD_SHA256 = ' + repr(self.parent_ref['sha256']), body, flags=re.M)
        body = re.sub(r'^REVIEWED_CHANGES:.*$', 'REVIEWED_CHANGES: dict[str, dict[str, str | None]] = ' + repr(changes), body, flags=re.M)
        module_path = self.source / catalog.MODULE
        module_path.write_text(body)
        spec = importlib.util.spec_from_file_location('fixture_catalog_release', module_path)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.tree = {'contract': 'writer-source-tree-v1', 'source_root': str(self.source),
            'git': f.git_record(self.source), 'files': []}
        for file in sorted(self.source.rglob('*.py')):
            self.tree['files'].append({'path': file.relative_to(self.source).as_posix(),
                'sha256': hashlib.sha256(file.read_bytes()).hexdigest(), 'byte_size': file.stat().st_size, 'mode': 0o644})
        tree_ref = self.write('catalog-tree.json', self.tree)
        changes = self.module.source_changes(self.module.object_at(self.parent['account_cleanup_generation']['source_tree']), self.tree)
        checks = {}
        for name in self.module.REQUIRED_CHECKS:
            log = f.root / (name + '.fixture.log')
            log.write_text('synthetic fixture only, not a production test result\n')
            log.chmod(0o600)
            checks[name] = self.write(name + '.fixture.json', {'contract': self.module.CHECK_CONTRACT,
                'name': name, 'status': 'passed', 'exit_code': 0, 'changes': changes,
                'command': ['fixture-only'], 'output': self.module.reference(log)})
        source_plan = self.write('catalog-plan.json', {'contract': 'account-cleanup-source-plan-v1',
            'transition': 'account-cleanup-0907-v1', 'project_root': str(f.project),
            'source_root': str(self.source), 'git': self.tree['git'], 'source_tree': tree_ref})
        self.build = {**self.parent, 'source_root': str(self.source), 'git': self.tree['git'],
            'critical_files': {r['path']: r['sha256'] for r in self.tree['files']},
            'code_successor_plan': source_plan,
            'account_cleanup_generation': {**self.parent['account_cleanup_generation'], 'source_tree': tree_ref},
            'account_catalog_capture_successor': {'contract': self.module.CONTRACT, 'transition': self.module.TRANSITION,
                'parent_build': self.parent_ref, 'source_tree': tree_ref, 'changes': changes, 'checks': checks,
                'actor': 'offline fixture', 'reason': 'approved catalog membership fixture', 'issued_at': AT,
                'production_rollout': 'approved_by_user', 'transport_qualification': 'not_verified',
                'schema_migration_repeated': False, 'provider_qualification_repeated': False,
                'account_catalog_policy': copy.deepcopy(self.module.ACCOUNT_CATALOG_POLICY),
                'account_catalog_policy_sha256': self.module.digest(self.module.ACCOUNT_CATALOG_POLICY),
                'business_scope_change': 'approved_by_user', 'legacy_execution_controls': 'inherited_unchanged'},
            'created_at': AT, 'validation_scope': 'temporary catalog membership test'}
        self.seal()
        environment = plistlib.loads(m.plist.read_bytes())['EnvironmentVariables']
        environment = {**environment, 'DCAR_LOADED_BUILD_RECEIPT': self.ref['path'], 'DCAR_WRITER_SOURCE_ROOT': str(self.source)}
        self.home = f.root / 'catalog-home'
        self.plist = self.home / 'Library/LaunchAgents/cn.tj.dcar.writer-worker.plist'
        self.plist.parent.mkdir(parents=True)
        self.plist.write_bytes(plistlib.dumps({'Label': 'cn.tj.dcar.writer-worker', 'WorkingDirectory': str(f.project),
            'ProgramArguments': [str(self.source / 'deploy/macos/run_writer_worker.sh')], 'EnvironmentVariables': environment}))
        self.plist.chmod(0o600)

    def write(self, name, value):
        path = self.fixture.root / name
        path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')))
        path.chmod(0o600)
        return self.module.reference(path)

    def seal(self):
        self.ref = self.write('catalog-build.json', {'contract_version': 'sealed-build-receipt-v1',
            'payload': self.build, 'payload_sha256': self.module.digest(self.build)})

    def verify(self):
        return self.module.verify_inheritance(build=self.build, build_ref=self.ref,
            install_path=self.fixture.install_path, database=self.fixture.db, source=self.source, at=AT)

    def bootstrap(self):
        return runtime_paths.verify_source_before_import(data=self.fixture.project, source=self.source,
            build_receipt=Path(self.ref['path']), home=self.home)

    def test_new_policy_is_explicit_and_inherited_execution_authority_is_unchanged(self):
        self.assertEqual(self.bootstrap()['files'], len(self.tree['files']))
        result = self.verify()
        self.assertEqual({k: v for k, v in result.items() if not k.startswith('catalog_capture_')}, self.baseline)
        self.assertEqual(result['catalog_capture_policy'], self.module.ACCOUNT_CATALOG_POLICY)
        self.assertEqual(result['catalog_capture_policy_sha256'], self.module.digest(self.module.ACCOUNT_CATALOG_POLICY))
        self.assertEqual(result['catalog_capture_proof']['business_scope_change'], 'approved_by_user')
        self.assertEqual(self.fixture.snapshots(), self.before)

    def test_policy_and_explicit_approval_cannot_be_removed_or_broadened(self):
        original = copy.deepcopy(self.build)
        for change in ('policy', 'digest', 'approval', 'controls'):
            self.build = copy.deepcopy(original)
            plan = self.build['account_catalog_capture_successor']
            if change == 'policy':
                plan['account_catalog_policy']['statuses'].append('paused')
                plan['account_catalog_policy_sha256'] = self.module.digest(plan['account_catalog_policy'])
            elif change == 'digest':
                plan['account_catalog_policy_sha256'] = '0' * 64
            elif change == 'approval':
                del plan['business_scope_change']
            else:
                plan['legacy_execution_controls'] = 'replaced'
            self.seal()
            with self.assertRaisesRegex(ValueError, 'policy or approval'):
                self.verify()

    def test_cannot_modify_legacy_roster_budget_provider_or_migration_evidence(self):
        original = copy.deepcopy(self.build)
        for change in ('selection_sha256', 'config_sha256', 'transport_manifest', 'migration'):
            self.build = copy.deepcopy(original)
            if change == 'migration':
                self.build['account_classification_successor']['migration']['sha256'] = '0' * 64
            else:
                self.build['account_cleanup_generation'][change] = '0' * 64
            self.seal()
            with self.assertRaises(ValueError):
                self.verify()
        self.assertEqual(self.fixture.snapshots(), self.before)

    def test_no_qualification_can_be_fabricated_by_the_policy_release(self):
        original = copy.deepcopy(self.build)
        for change in ('provider_qualification_repeated', 'transport_qualification', 'schema_migration_repeated'):
            self.build = copy.deepcopy(original)
            plan = self.build['account_catalog_capture_successor']
            plan[change] = True if change.endswith('repeated') else 'qualified'
            self.seal()
            with self.assertRaisesRegex(ValueError, 'release scope'):
                self.verify()

    def test_frozen_delta_checks_and_real_logs_are_required(self):
        with patch.object(self.module, 'REVIEWED_CHANGES', {}):
            with self.assertRaisesRegex(ValueError, 'not frozen'):
                self.verify()
        original = copy.deepcopy(self.build)
        self.build['account_catalog_capture_successor']['checks'] = {}
        self.seal()
        with self.assertRaisesRegex(ValueError, 'checks are incomplete'):
            self.verify()
        self.build = original
        self.seal()
        report = self.module.object_at(next(iter(self.build['account_catalog_capture_successor']['checks'].values())))
        Path(report['output']['path']).write_text('changed check log\n')
        with self.assertRaisesRegex(ValueError, 'check output changed'):
            self.verify()

    def test_packaging_check_records_real_failed_command_and_refuses_success(self):
        script = Path(__file__).resolve().parents[1] / 'scripts/prepare_account_catalog_capture_release.py'
        spec = importlib.util.spec_from_file_location('fixture_catalog_packaging', script)
        package = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(package)
        output = self.fixture.root / 'failed-catalog-check.json'
        args = SimpleNamespace(checkout=self.source, parent_build=Path(self.parent_ref['path']),
            installed_plist=self.manual_fixture.plist, name='catalog_release', output=output,
            command=[sys.executable, '-B', '-c', "print('expected offline fixture failure'); raise SystemExit(3)"])
        # Legacy fixtures contain a runtime_paths stub; use the real inventory
        # primitives while still hashing every actual temporary source member.
        with patch.object(package.base, 'load', return_value=runtime_paths):
            with self.assertRaisesRegex(ValueError, 'Focused check failed'):
                package.check(args)
        report = json.loads(output.read_text())
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['exit_code'], 3)
        self.assertEqual(report['command'], args.command)
        self.assertIn('expected offline fixture failure', output.with_suffix('.log').read_text())
        self.assertEqual(self.fixture.snapshots(), self.before)

    def test_parent_source_and_database_identity_remain_pinned(self):
        module_path = Path(self.parent['source_root']) / 'src/dcar_eval/v8/manual_content_scope_release.py'
        body = module_path.read_bytes()
        module_path.write_bytes(body + b'\n# unexpected change\n')
        with self.assertRaisesRegex(ValueError, 'parent verifier changed'):
            self.verify()
        module_path.write_bytes(body)
        other = self.fixture.root / 'other.sqlite3'
        other.write_bytes(b'not the installed DB')
        with self.assertRaises(ValueError):
            self.module.verify_inheritance(build=self.build, build_ref=self.ref,
                install_path=self.fixture.install_path, database=other, source=self.source, at=AT)

    def test_bootstrap_rejects_unreviewed_source_and_parent_still_supports_rollback(self):
        self.bootstrap()
        self.assertEqual(self.manual_fixture.bootstrap()['files'], len(self.manual_fixture.tree['files']))
        path = self.source / 'src/dcar_eval/v8/api.py'
        path.write_bytes(path.read_bytes() + b'\n# unexpected change\n')
        with self.assertRaisesRegex(ValueError, 'source content'):
            self.bootstrap()


if __name__ == '__main__':
    unittest.main()
