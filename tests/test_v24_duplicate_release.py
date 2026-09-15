"""Exact-source receipt gates, actual parent schema and fallback binding."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from v8 import duplicate_index_release as release, schema_v24
from v8.storage import connect, initialize_database


class DuplicateReleaseTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.source, self.parent, self.data = [self.root / name for name in ('source', 'parent', 'data')]
        for path in (self.source, self.parent, self.data): path.mkdir()
        subprocess.run(['git', 'init', '-b', 'fixture', str(self.source)], check=True, capture_output=True)
        (self.source / 'README.md').write_text('isolated release fixture\n')
        subprocess.run(['git', '-C', str(self.source), 'add', 'README.md'], check=True, capture_output=True)
        subprocess.run(['git', '-C', str(self.source), '-c', 'user.name=Fixture', '-c', 'user.email=fixture@invalid',
                        'commit', '-m', 'fixture'], check=True, capture_output=True)
        for name in release.REQUIRED_SOURCE:
            path = self.source / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_text('# fixture\n')
        parent_tree = {'contract': 'writer-source-tree-v1', 'source_root': str(self.parent), 'git': {}, 'files': []}
        self.parent_build = {'status': 'succeeded', 'schema_contract': {'code_schema': 23, 'formal_schema': 23},
            'source_root': str(self.parent), 'project_root': str(self.data),
            'account_cleanup_generation': {'source_tree': self.write('parent-tree.json', parent_tree)}}
        self.parent_ref = self.seal('parent.json', self.parent_build)
        self.tree = release.inventory(self.source)
        tree_ref = self.write('source-tree.json', self.tree)
        self.changes = release.source_changes(parent_tree, self.tree)
        self.checks = {}
        for name in sorted(release.CHECKS):
            output = self.write(name + '.output.json', {'result': 'fixture passed'})
            self.checks[name] = self.write(name + '.json', {'contract': release.CHECK_CONTRACT, 'name': name,
                'status': 'passed', 'exit_code': 0, 'command': ['fixture-only'], 'changes': self.changes,
                'source_tree': tree_ref, 'output': output})

    def write(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value, sort_keys=True)); path.chmod(0o600)
        return release.reference(path)

    def seal(self, name, payload):
        return self.write(name, {'contract_version': 'sealed-build-receipt-v1',
                                'payload': payload, 'payload_sha256': release.digest(payload)})

    def verify(self, **kwargs):
        return release.verify_candidate_source(**{'source': self.source, 'parent_build_ref': self.parent_ref,
                                                   'checks': self.checks, **kwargs})

    def test_all_final_source_checks_required_and_outputs_bound(self):
        self.assertEqual(self.verify()['changes'], self.changes)
        for name in release.CHECKS:
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'six final-source'):
                self.verify(checks={key: value for key, value in self.checks.items() if key != name})
        (self.root / 'locks.output.json').write_text('changed output')
        with self.assertRaisesRegex(ValueError, 'check is missing, failed'):
            self.verify()

    def test_source_edits_and_data_directory_sources_are_rejected(self):
        (self.source / 'src/dcar_eval/v8/duplicate_index.py').write_text('# changed after check\n')
        with self.assertRaisesRegex(ValueError, 'different final source'):
            self.verify()
        nested = self.data / 'release'; nested.mkdir()
        with self.assertRaisesRegex(ValueError, 'independent source'):
            self.verify(source=nested)

    def test_missing_release_implementation_is_rejected(self):
        (self.source / 'src/dcar_eval/v8/snapshot_schema_successor.py').unlink()
        with self.assertRaisesRegex(ValueError, 'required implementation'):
            self.verify()

    def code_fixture(self):
        parent = {**self.parent_build, 'schema_contract': {'code_schema': 24, 'formal_schema': 24},
                  'created_at': '2026-09-14T00:00:00Z', 'critical_files': {},
                  release.FIELD: {'contract': release.CONTRACT, 'engine': 'mih',
                                  'migration': {'path': '/immutable/original-migration.json', 'sha256': 'original'}}}
        parent_ref = self.seal('code-parent.json', parent)
        checked = release.verify_candidate_source(source=self.source, parent_build_ref=parent_ref, checks=self.checks)
        inherited = {'authority': 'unchanged fixture authority',
                     'duplicate_index_proof': {'loaded_build': parent_ref, 'proof_sha256': 'original-proof'}}
        source_plan = self.write('code-source-plan.json', {'contract': 'account-cleanup-source-plan-v1',
            'transition': 'account-cleanup-0907-v1', 'project_root': str(self.data), 'source_root': str(self.source),
            'git': self.tree['git'], 'source_tree': checked['source_tree_ref']})
        plan = {'contract': release.CODE_CONTRACT, 'parent_build': parent_ref, 'source_tree': checked['source_tree_ref'],
                'changes': checked['changes'], 'checks': self.checks, 'engine': 'full_scan',
                'schema_migration_repeated': False, 'database_writes': 0, 'paid_gates_issued': 0,
                'parent_proof_sha256': release.digest(inherited), 'issued_at': '2026-09-15T00:00:00Z'}
        child = {**parent, 'source_root': str(self.source), 'git': self.tree['git'],
            'critical_files': {name: row['sha256'] for name, row in release.records(self.tree).items()
                              if name.startswith(('src/', 'config/')) and name.endswith(('.py', '.json'))},
            'code_successor_plan': source_plan,
            'account_cleanup_generation': {**parent['account_cleanup_generation'], 'source_tree': checked['source_tree_ref']},
            release.CODE_FIELD: plan, 'created_at': plan['issued_at']}
        return parent, parent_ref, inherited, child

    def verify_code_fixture(self, parent, inherited, child, name):
        ref = self.seal(name, child)
        with patch.object(release, 'duplicate_code_parent_context', return_value=(parent, inherited)) as historical:
            result = release.verify_inheritance(build=child, build_ref=ref, install_path=self.root/'install.json',
                database=self.data/'formal.sqlite3', source=self.source)
            historical.assert_called_once()
        return ref, result

    def test_schema24_code_successor_retains_original_migration_and_authority(self):
        parent, _, inherited, child = self.code_fixture()
        ref, result = self.verify_code_fixture(parent, inherited, child, 'code-child.json')
        self.assertEqual(child[release.FIELD], parent[release.FIELD])
        self.assertEqual(result['authority'], inherited['authority'])
        self.assertEqual(result['duplicate_index_proof']['loaded_build'], ref)
        self.assertEqual(result['duplicate_index_code_proof']['engine'], 'full_scan')

    def test_resigned_code_successor_cannot_rewrite_migration_or_existing_authority(self):
        parent, _, inherited, child = self.code_fixture()
        mutations = {
            'migration': {release.FIELD: {**child[release.FIELD], 'migration': {'path': '/forged'}}},
            'data-root': {'project_root': '/another-data-root'},
            'generation-authority': {'account_cleanup_generation': {**child['account_cleanup_generation'], 'new_grant': True}},
        }
        for name, values in mutations.items():
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'migration history or existing authority'):
                self.verify_code_fixture(parent, inherited, {**child, **values}, name+'.json')

    def test_code_successor_rejects_new_database_writes_and_wrong_predecessor_proof(self):
        parent, _, inherited, child = self.code_fixture()
        for field, value in [('schema_migration_repeated', True), ('database_writes', 1),
                             ('paid_gates_issued', 1), ('parent_proof_sha256', 'forged')]:
            bad = {**child, release.CODE_FIELD: {**child[release.CODE_FIELD], field: value}}
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.verify_code_fixture(parent, inherited, bad, field+'.json')

    def test_code_predecessor_rejects_schema23_before_loading_any_source(self):
        with self.assertRaisesRegex(ValueError, 'code predecessor must be schema24'):
            release.duplicate_code_parent_context(self.parent_ref, install_path=self.root/'install.json',
                                        database=self.data/'formal.sqlite3')

    def test_parent_verifier_receives_actual_frozen_schema23(self):
        from v8 import four_platform_flow_release
        self.assertIs(release.code_parent_context, four_platform_flow_release.code_parent_context)
        backup = self.root / 'before.sqlite3'; formal = self.data / 'formal.sqlite3'
        for path in (backup, formal):
            c = sqlite3.connect(path); c.execute('PRAGMA user_version=23'); c.close(); path.chmod(0o600)
        reference = release.file_reference(backup)
        def historical(*args, **kwargs):
            c = kwargs['connection']
            self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0], 23)
            self.assertEqual(Path(c.execute('PRAGMA database_list').fetchone()[2]), backup)
            self.assertEqual(kwargs['database'], formal)
            return self.parent_build, {'inherited': True}
        with patch.object(release, 'code_parent_context', side_effect=historical):
            self.assertEqual(release.parent_context(self.parent_ref, install_path=self.root / 'install.json',
                database=formal, backup_ref=reference)[1], {'inherited': True})
        c = sqlite3.connect(backup); c.execute('PRAGMA user_version=24'); c.close()
        with self.assertRaisesRegex(ValueError, 'actual schema23'):
            release.parent_context(self.parent_ref, install_path=self.root / 'install.json', database=formal,
                                   backup_ref=release.file_reference(backup))

    def test_mutable_backup_and_wrong_engine_receipt_fail_closed(self):
        backup = self.root / 'before.sqlite3'; backup.write_bytes(b'fixture'); backup.chmod(0o600)
        reference = release.file_reference(backup)
        Path(str(backup) + '-wal').write_bytes(b'')
        with self.assertRaisesRegex(ValueError, 'mutable SQLite sidecars'): release.verified_backup(reference)
        build = {'schema_contract': {'code_schema': 24, 'formal_schema': 24},
                 'source_root': str(Path(release.__file__).resolve().parents[3]), release.FIELD: {'engine': 'full_scan'}}
        ref = self.seal('fallback.json', build)
        with patch.dict(os.environ, {'DCAR_LOADED_BUILD_RECEIPT': ref['path'], 'DCAR_LOADED_BUILD_ID': 'sha256:' + ref['sha256']}):
            self.assertEqual(release.active_engine(), 'full_scan')
        build[release.FIELD]['engine'] = 'mih'
        build[release.CODE_FIELD] = {'engine': 'full_scan'}
        ref = self.seal('code-fallback.json', build)
        with patch.dict(os.environ, {'DCAR_LOADED_BUILD_RECEIPT': ref['path'], 'DCAR_LOADED_BUILD_ID': 'sha256:' + ref['sha256']}):
            self.assertEqual(release.active_engine(), 'full_scan')
            Path(ref['path']).write_text('{}')
            with self.assertRaisesRegex(ValueError, 'loaded engine receipt changed'): release.active_engine()

    def test_migration_receipt_binds_real24_and_immutable_history(self):
        path = self.root / 'migration.sqlite3'
        c = connect(path); self.addCleanup(c.close)
        initialize_database(c, target_version=23); schema_v24.migrate(c)
        proof = schema_v24.migration_proof(c)
        value = {'contract': release.INSTALL_CONTRACT, 'status': 'migrated', 'from_schema': 23, 'to_schema': 24,
                 'migration_proof': proof, 'paid_gates_issued': 0, 'preserved_tables_verified': True}
        value['receipt_sha256'] = release.digest(value)
        self.assertEqual(release.verify_migration(c, value), proof)
        value['paid_gates_issued'] = 1
        value['receipt_sha256'] = release.digest({key: val for key, val in value.items() if key != 'receipt_sha256'})
        with self.assertRaisesRegex(ValueError, 'installation proof differs'): release.verify_migration(c, value)

    def test_maintenance_blocks_live_runs_and_retains_old_unknown_billing(self):
        scripts = str(Path(__file__).resolve().parents[1] / 'scripts')
        with patch.object(sys, 'path', [scripts, *sys.path]):
            from install_duplicate_index_release import verify_maintenance_quiescence
        c = connect(self.root / 'quiescence.sqlite3'); self.addCleanup(c.close)
        initialize_database(c, target_version=23)
        at = '2026-09-15T00:00:00Z'
        run = c.execute("INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at) "
                        "VALUES('capture_integrated_work',?,'running',?)", (at, at)).lastrowid
        c.commit()
        with self.assertRaisesRegex(ValueError, 'still running'): verify_maintenance_quiescence(c)
        c.execute("UPDATE scheduler_runs SET status='interrupted',completed_at=? WHERE id=?", (at, run))
        attempt = c.execute("INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,status,started_at,completed_at) "
            "VALUES(?,1,'scheduled','interrupted',?,?)", (run, at, at)).lastrowid
        detail = {'state': 'billing_unknown', 'scope': {'scheduler_run_id': run, 'scheduler_attempt_id': attempt}}
        usage = c.execute("INSERT INTO provider_usage(provider,operation,recorded_at,details_json) VALUES('TikHub','fixture',?,?)",
                          (at, json.dumps(detail))).lastrowid
        c.commit(); writes = c.total_changes
        result = verify_maintenance_quiescence(c)
        self.assertEqual(result['retained_historical_unknown_ids'], [usage])
        self.assertFalse(result['billing_reconciled']); self.assertEqual(c.total_changes, writes)
        detail['state'] = 'sent'
        c.execute('UPDATE provider_usage SET details_json=? WHERE id=?', (json.dumps(detail), usage)); c.commit()
        with self.assertRaisesRegex(ValueError, 'still in flight'): verify_maintenance_quiescence(c)
