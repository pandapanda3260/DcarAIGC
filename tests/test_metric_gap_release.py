"""Focused source-only successor tests on temporary schema21 fixtures.

All parent pins and receipt fixtures are local test data. The real source
bootstrap and runtime authority checks run; no formal DB or network is used.
"""
from __future__ import annotations

import copy
from dataclasses import replace
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import plistlib
import re
import shutil
import unittest
from unittest.mock import patch

from tests import test_account_classification_release as classification_fixtures
from tests import test_account_catalog_capture_release as catalog_fixtures
from v8 import metric_gap_release as manual, runtime_database, runtime_paths

AT = classification_fixtures.AT


class MetricGapReleaseTest(unittest.TestCase):
    def setUp(self):
        self.manual_fixture = mf = catalog_fixtures.AccountCatalogCaptureReleaseTest()
        self.addCleanup(mf.doCleanups)
        mf.setUp()
        self.fixture = f = mf.fixture
        self.parent = copy.deepcopy(mf.build)
        self.parent_ref = dict(mf.ref)
        self.baseline = mf.verify()
        self.before = f.snapshots()
        self.source = f.root / 'metric-source'
        shutil.copytree(mf.source, self.source)
        path = self.source / 'src/dcar_eval/v8/api.py'
        old_sha = hashlib.sha256(path.read_bytes()).hexdigest()
        path.write_text('# reviewed metric-gap fixture\n')
        changes = {'src/dcar_eval/v8/api.py': {'before_sha256': old_sha,
            'after_sha256': hashlib.sha256(path.read_bytes()).hexdigest()}}
        body = manual._LOADED_SOURCE.decode()
        body = re.sub(r'^PARENT_BUILD_SHA256 = .*$', 'PARENT_BUILD_SHA256 = ' + repr(self.parent_ref['sha256']), body, flags=re.M)
        body = re.sub(r'^REVIEWED_CHANGES:.*$', 'REVIEWED_CHANGES: dict[str, dict[str, str | None]] = ' + repr(changes), body, flags=re.M)
        module_path = self.source / manual.MODULE
        module_path.write_text(body)
        spec = importlib.util.spec_from_file_location('fixture_metric_release', module_path)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.tree = {'contract': 'writer-source-tree-v1', 'source_root': str(self.source),
            'git': f.git_record(self.source), 'files': []}
        for file in sorted(self.source.rglob('*.py')):
            self.tree['files'].append({'path': file.relative_to(self.source).as_posix(),
                'sha256': hashlib.sha256(file.read_bytes()).hexdigest(), 'byte_size': file.stat().st_size, 'mode': 0o644})
        tree_ref = self.write('metric-tree.json', self.tree)
        changes = self.module.source_changes(self.module.object_at(self.parent['account_cleanup_generation']['source_tree']), self.tree)
        checks = {}
        for name in self.module.REQUIRED_CHECKS:
            log = f.root / (name + '.fixture.log')
            log.write_text('synthetic fixture record, not a production test result\n')
            log.chmod(0o600)
            checks[name] = self.write(name + '.fixture.json', {'contract': self.module.CHECK_CONTRACT,
                'name': name, 'status': 'passed', 'exit_code': 0, 'changes': changes,
                'command': ['fixture-only'], 'output': self.module.reference(log)})
        plan = self.write('metric-plan.json', {'contract': 'account-cleanup-source-plan-v1',
            'transition': 'account-cleanup-0907-v1', 'project_root': str(f.project),
            'source_root': str(self.source), 'git': self.tree['git'], 'source_tree': tree_ref})
        self.build = {**self.parent, 'source_root': str(self.source), 'git': self.tree['git'],
            'critical_files': {r['path']: r['sha256'] for r in self.tree['files']},
            'code_successor_plan': plan,
            'account_cleanup_generation': {**self.parent['account_cleanup_generation'], 'source_tree': tree_ref},
            'metric_gap_successor': {'contract': self.module.CONTRACT, 'transition': self.module.TRANSITION,
                'parent_build': self.parent_ref, 'source_tree': tree_ref, 'changes': changes, 'checks': checks,
                'actor': 'offline fixture', 'reason': 'temporary code-only test', 'issued_at': AT,
                'production_rollout': 'approved_by_user', 'transport_qualification': 'not_verified',
                'schema_migration_repeated': False, 'provider_qualification_repeated': False},
            'created_at': AT, 'validation_scope': 'temporary metric-gap test'}
        self.seal()
        installed = runtime_database.load_installed_writer_contract(required=True)
        environment = {**installed.payload['EnvironmentVariables'], 'DCAR_LOADED_BUILD_RECEIPT': self.ref['path'],
            'DCAR_WRITER_SOURCE_ROOT': str(self.source)}
        current = replace(installed, payload={**installed.payload, 'EnvironmentVariables': environment})
        self.enterContext(patch.object(runtime_database, 'load_installed_writer_contract', return_value=current))
        self.enterContext(patch.dict(os.environ, {**environment, 'DCAR_LOADED_BUILD_ID': 'sha256:' + self.ref['sha256']}))
        self.home = f.root / 'metric-home'
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
        self.ref = self.write('metric-build.json', {'contract_version': 'sealed-build-receipt-v1',
            'payload': self.build, 'payload_sha256': self.module.digest(self.build)})

    def verify(self):
        return self.module.verify_inheritance(build=self.build, build_ref=self.ref,
            install_path=self.fixture.install_path, database=self.fixture.db, source=self.source, at=AT)

    def bootstrap(self):
        return runtime_paths.verify_source_before_import(data=self.fixture.project, source=self.source,
            build_receipt=Path(self.ref['path']), home=self.home)

    def test_metric_bootstrap_preserves_original_authority_and_database(self):
        self.assertEqual(self.bootstrap()['files'], len(self.tree['files']))
        current = self.verify()
        self.assertEqual({k: v for k, v in current.items() if k != 'metric_gap_proof'}, self.baseline)
        self.assertEqual(self.fixture.snapshots(), self.before)
        self.assertEqual(self.verify()['parent_build_ref'], self.baseline['parent_build_ref'])

    def test_metric_rejects_changed_authority_and_claimed_qualification(self):
        original = copy.deepcopy(self.build)
        for change in ('authority', 'qualification', 'schema', 'manual_parent'):
            self.build = copy.deepcopy(original)
            if change == 'authority':
                self.build['account_cleanup_generation']['config_sha256'] = '0' * 64
            elif change == 'qualification':
                self.build['metric_gap_successor']['provider_qualification_repeated'] = True
            elif change == 'schema':
                self.build['schema_contract']['formal_schema'] = 22
            else:
                del self.build['manual_content_scope_successor']
            self.seal()
            with self.assertRaises(ValueError):
                self.verify()

    def test_metric_rejects_unknown_source_and_modified_test_log(self):
        changed = copy.deepcopy(self.tree)
        changed['files'][0]['sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'source delta'):
            self.module.source_changes(self.module.object_at(self.parent['account_cleanup_generation']['source_tree']), changed)
        Path(next(iter(self.build['metric_gap_successor']['checks'].values()))['path']).write_text('{}')
        with self.assertRaisesRegex(ValueError, 'reference differs'):
            self.verify()

    def test_metric_rejects_parent_tamper_and_database_replacement(self):
        for name in ('account_classification_release.py', 'manual_content_scope_release.py', 'account_catalog_capture_release.py'):
            parent_module = Path(self.parent['source_root']) / 'src/dcar_eval/v8' / name
            body = parent_module.read_bytes()
            parent_module.write_bytes(body + b'\n# unexpected change\n')
            with self.assertRaisesRegex(ValueError, 'parent .*verifier changed'):
                self.verify()
            parent_module.write_bytes(body)
        other = self.fixture.root / 'other.sqlite3'
        other.write_bytes(b'not the installed DB')
        with self.assertRaises(ValueError):
            self.module.verify_inheritance(build=self.build, build_ref=self.ref,
                install_path=self.fixture.install_path, database=other, source=self.source, at=AT)

    def test_metric_bootstrap_rejects_live_tamper_and_shared_git(self):
        path = self.source / 'src/dcar_eval/v8/api.py'
        body = path.read_bytes()
        path.write_bytes(body + b'\n# unexpected change\n')
        with self.assertRaisesRegex(ValueError, 'source content'):
            self.bootstrap()
        path.write_bytes(body)
        shared = self.source / '.git/commondir'
        shared.write_text(str(Path(self.parent['source_root']) / '.git'))
        with self.assertRaisesRegex(ValueError, 'independent Git objects'):
            self.bootstrap()

    def test_metric_previous_code_remains_a_valid_rollback(self):
        self.bootstrap()
        f = self.fixture
        self.assertEqual(self.manual_fixture.bootstrap()['files'], len(self.manual_fixture.tree['files']))
        parent, inherited = self.module.parent_context(self.parent_ref, install_path=f.install_path, database=f.db, at=AT)
        self.assertEqual(parent, self.parent)
        self.assertEqual(inherited['proof'], self.baseline['proof'])
        self.assertEqual(inherited['manual_content_scope_proof'], self.baseline['manual_content_scope_proof'])
        self.assertEqual(inherited['catalog_capture_proof'], self.baseline['catalog_capture_proof'])
        self.assertEqual(f.snapshots(), self.before)


if __name__ == '__main__':
    unittest.main()
