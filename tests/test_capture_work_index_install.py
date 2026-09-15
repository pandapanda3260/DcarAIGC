"""Real schema23 index maintenance with the frozen v2 predecessor verifier."""
from __future__ import annotations

from dataclasses import replace
import os
from pathlib import Path
import plistlib
import shutil
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import test_four_platform_flow_release as fixtures
from v8 import four_platform_flow_release as release, runtime_database, schema_v23
from v8.capture_work_index import INDEX_NAME, INDEX_OBJECT
from v8.schema_v22 import _table_digests

ROOT = Path(__file__).resolve().parents[1]
FROZEN_V2 = Path('/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260912-four-platform-flow-v2')
FROZEN_V3 = Path('/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260913-four-platform-flow-v3')


class CaptureWorkIndexInstallTest(unittest.TestCase):
    def setUp(self):
        if not FROZEN_V2.is_dir() or not FROZEN_V3.is_dir():
            self.skipTest('Immutable schema23 v2/v3 index-contract source fixture is unavailable')
        # Composition avoids importing the fixture's unrelated tests. Its origin
        # is the actual old source, whose schema verifier rejects every extra index.
        self.h = h = fixtures.FourPlatformFlowReleaseTest()
        with patch.object(fixtures, 'ROOT', FROZEN_V2):
            h.setUp()
        self.addCleanup(h.doCleanups)
        self.f = f = h.f
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('provider network forbidden')))
        self.migration = h.installer.install(h.args)
        self.origin = h.prepare(self.migration)
        self.origin_ref = self.origin['child_build']
        self.origin_payload = release.payload_at(self.origin_ref, 'sealed-build-receipt-v1')
        next_body = Path(self.origin['next_plist']['path']).read_bytes()
        h.installed_path.write_bytes(next_body)
        next_plist = plistlib.loads(next_body)
        self.enterContext(patch.object(runtime_database, 'load_installed_writer_contract',
            return_value=replace(h.installed, payload=next_plist)))
        self.enterContext(patch.dict(os.environ, {**next_plist['EnvironmentVariables'],
            'DCAR_LOADED_BUILD_ID': 'sha256:' + self.origin_ref['sha256']}))
        self.source = f.root / 'index-source'
        shutil.copytree(h.source, self.source)
        # Keep the original index successor's actual verifier contract. A new
        # code-only successor may require modules deliberately excluded from
        # the old DDL allowlist; copying its verifier alone is not a valid v3.
        for name in release.CODE_REPAIR_FILES:
            source = FROZEN_V3 / name
            if source.is_file():
                target = self.source / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
                target.chmod(source.stat().st_mode & 0o777)
        self.tree = release.inventory(self.source)
        self.tree_ref = release.reference(Path(f.write('index-tree.json', self.tree)['path']))
        changes = release.source_changes(h.fixture.tree, self.tree)
        self.checks = {}
        for name in sorted(release.CHECKS):
            log = f.root / ('index-' + name + '.log')
            log.write_text('Offline disposable index fixture; no production acceptance.\n')
            log.chmod(0o600)
            self.checks[name] = release.reference(Path(f.write('index-' + name + '.json', {
                'contract': release.CHECK_CONTRACT, 'name': name, 'status': 'passed', 'exit_code': 0,
                'source_tree': self.tree_ref, 'changes': changes, 'command': ['offline-index-fixture'],
                'output': release.reference(log)})['path']))
        self.installer = fixtures.script('install_capture_work_index')
        self.prepare_cli = fixtures.script('prepare_four_platform_flow_release')
        self.enterContext(patch.object(self.installer, 'ROOT', self.source))
        self.enterContext(patch.object(self.prepare_cli, 'ROOT', self.source))
        self.args = SimpleNamespace(database=f.db, project_root=f.project,
            installed_plist=h.installed_path, parent_build=h.args.parent_build,
            parent_install=h.args.parent_install, source_tree=Path(self.tree_ref['path']),
            check_report=[name + '=' + ref['path'] for name, ref in self.checks.items()],
            code_predecessor_build=Path(self.origin_ref['path']), output_dir=f.root / 'index-install')
        self.proof = schema_v23.migration_proof(f.connection)
        self.original_inode = f.db.stat().st_ino

    def verify_index(self, ref):
        return release.verify_index_install(ref, origin_ref=self.origin_ref,
            source_tree_ref=self.tree_ref, checks=self.checks, database=self.f.db,
            connection=self.f.connection)

    def has_index(self):
        return self.f.connection.execute('SELECT sql FROM sqlite_master WHERE name=?', (INDEX_NAME,)).fetchone()

    def test_install_old_verifier_backup_and_index_bound_prepare_activate(self):
        retained = _table_digests(self.f.connection)
        plist_before = self.h.installed_path.read_bytes()
        installed = self.installer.install(self.args)
        receipt = self.verify_index(installed)
        self.assertEqual(_table_digests(self.f.connection), retained)
        self.assertEqual(self.f.db.stat().st_ino, self.original_inode)
        self.assertEqual(schema_v23.migration_proof(self.f.connection), self.proof)
        self.assertEqual(self.h.installed_path.read_bytes(), plist_before)
        self.assertEqual(tuple(self.f.connection.execute(
            'SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name=?', (INDEX_NAME,)).fetchone()), INDEX_OBJECT)
        self.assertEqual(receipt['provider_calls'], 0)
        self.assertFalse(receipt['schema_migration_repeated'])
        with self.assertRaisesRegex(ValueError, 'structure or proof changed'):
            release.code_parent_context(self.origin_ref, install_path=self.args.parent_install,
                database=self.f.db, connection=self.f.connection)
        _, inherited = release.code_parent_context(self.origin_ref, install_path=self.args.parent_install,
            database=self.f.db, origin_backup=receipt['backup'])
        self.assertEqual(inherited['four_platform_flow_proof']['loaded_build'], self.origin_ref)
        instruction = self.f.root / 'index-instruction.txt'
        instruction.write_text('Offline fixture approves the sole performance index and local forward execution repair.')
        instruction.chmod(0o600)
        proposal = self.prepare_cli.prepare(SimpleNamespace(approve_local_capture=True,
            user_instruction_file=instruction, source_thread_id='offline-index-fixture', actor='offline fixture',
            reason='single index maintenance fixture', parent_build=self.args.parent_build,
            parent_install=self.args.parent_install, check_report=self.args.check_report,
            installed_plist=self.args.installed_plist, migration=Path(self.migration['path']),
            evidence_root=self.f.root / 'index-prepared', code_predecessor_build=self.args.code_predecessor_build,
            index_install=Path(installed['path'])))
        child = release.payload_at(proposal['child_build'], 'sealed-build-receipt-v1')
        repair = child[release.FIELD]['code_predecessor']
        self.assertEqual(repair['index_install'], installed)
        self.assertEqual(repair['database_writes'], 1)
        self.assertEqual(child[release.FIELD]['migration'], self.migration)
        self.assertEqual(child[release.FIELD]['parent_build'], self.h.parent_ref)
        self.assertEqual(self.h.installed_path.read_bytes(), plist_before)
        unbound = {**child, release.FIELD: {**child[release.FIELD], 'code_predecessor': {
            **{key: value for key, value in repair.items() if key != 'index_install'}, 'database_writes': 0}}}
        unbound_ref = release.reference(Path(self.f.write('unbound-index-build.json', {
            'contract_version': 'sealed-build-receipt-v1', 'payload': unbound,
            'payload_sha256': release.digest(unbound)})['path']))
        with self.assertRaisesRegex(ValueError, 'own sealed installation proof'):
            release.verify_inheritance(build=unbound, build_ref=unbound_ref,
                install_path=self.args.parent_install, database=self.f.db, source=self.source)
        for key, value in (('origin_proof_sha256', '0' * 64),
                ('parent_build', self.origin_ref), ('parent_install', self.origin_ref)):
            with self.subTest(parent_binding=key):
                wrong_receipt = {**receipt, key: value}
                wrong_receipt['receipt_sha256'] = release.digest({k: v for k, v in wrong_receipt.items() if k != 'receipt_sha256'})
                wrong_ref = release.reference(Path(self.f.write('wrong-parent-index-' + key + '.json', wrong_receipt)['path']))
                wrong_child = {**child, release.FIELD: {**child[release.FIELD],
                    'code_predecessor': {**repair, 'index_install': wrong_ref}}}
                wrong_build_ref = release.reference(Path(self.f.write('wrong-parent-build-' + key + '.json', {
                    'contract_version': 'sealed-build-receipt-v1', 'payload': wrong_child,
                    'payload_sha256': release.digest(wrong_child)})['path']))
                with self.assertRaisesRegex(ValueError, 'performance index parent binding differs'):
                    release.verify_inheritance(build=wrong_child, build_ref=wrong_build_ref,
                        install_path=self.args.parent_install, database=self.f.db, source=self.source)
        activation = fixtures.script('install_four_platform_flow')
        with patch.object(activation, 'ROOT', self.source):
            result = activation.activate(SimpleNamespace(proposal=Path(proposal['proposal']['path']),
                output=self.f.root / 'index-activated.json'))
        self.assertEqual(release.object_at(result)['status'], 'installed_stopped')
        self.assertEqual(_table_digests(self.f.connection), retained)
        self.assertEqual(schema_v23.migration_proof(self.f.connection), self.proof)
        self.assertEqual(self.f.db.stat().st_ino, self.original_inode)

    def test_receipt_failure_recovers_without_reinstalling_index(self):
        write = self.installer.write
        def fail_receipt(path, value):
            if path.name == 'index-install.json':
                raise OSError('offline receipt failure')
            return write(path, value)
        retained = _table_digests(self.f.connection)
        with patch.object(self.installer, 'write', side_effect=fail_receipt):
            with self.assertRaisesRegex(OSError, 'offline receipt failure'):
                self.installer.install(self.args)
        self.assertIsNotNone(self.has_index())
        recovered = self.installer.recover(SimpleNamespace(output_dir=self.args.output_dir))
        self.verify_index(recovered)
        self.assertEqual(self.installer.recover(SimpleNamespace(output_dir=self.args.output_dir)), recovered)
        self.assertEqual(_table_digests(self.f.connection), retained)
        self.assertEqual(schema_v23.migration_proof(self.f.connection), self.proof)

    def test_rollback_drops_only_index_and_preserves_later_business_write(self):
        self.installer.install(self.args)
        self.f.connection.execute("UPDATE accounts SET operator_name='later business write' WHERE id=(SELECT min(id) FROM accounts)")
        self.f.connection.commit()
        retained = _table_digests(self.f.connection)
        result = self.installer.rollback(SimpleNamespace(output_dir=self.args.output_dir))
        self.assertIsNone(self.has_index())
        self.assertFalse(release.object_at(result)['backup_restored'])
        self.assertEqual(_table_digests(self.f.connection), retained)
        self.assertEqual(schema_v23.migration_proof(self.f.connection), self.proof)
        self.assertEqual(self.f.db.stat().st_ino, self.original_inode)
        release.code_parent_context(self.origin_ref, install_path=self.args.parent_install,
            database=self.f.db, connection=self.f.connection)

    def test_rollback_receipt_failure_can_be_retried_after_index_is_already_removed(self):
        self.installer.install(self.args)
        write = self.installer.write_recovered
        def fail_receipt(path, value, **kwargs):
            if path.name == 'index-rollback.json':
                raise OSError('offline rollback receipt failure')
            return write(path, value, **kwargs)
        retained = _table_digests(self.f.connection)
        with patch.object(self.installer, 'write_recovered', side_effect=fail_receipt):
            with self.assertRaisesRegex(OSError, 'offline rollback receipt failure'):
                self.installer.rollback(SimpleNamespace(output_dir=self.args.output_dir))
        self.assertIsNone(self.has_index())
        result = self.installer.rollback(SimpleNamespace(output_dir=self.args.output_dir))
        self.assertEqual(release.object_at(result)['status'], 'rolled_back')
        self.assertEqual(_table_digests(self.f.connection), retained)
        self.assertEqual(schema_v23.migration_proof(self.f.connection), self.proof)
        self.assertEqual(self.f.db.stat().st_ino, self.original_inode)

    def test_wrong_ddl_cannot_be_inherited_or_dropped_as_the_approved_index(self):
        installed = self.installer.install(self.args)
        self.f.connection.execute('DROP INDEX ' + INDEX_NAME)
        self.f.connection.execute('CREATE INDEX ' + INDEX_NAME + ' ON capture_work_items(account_id)')
        self.f.connection.commit()
        retained = _table_digests(self.f.connection)
        wrong = self.has_index()[0]
        with self.assertRaisesRegex(ValueError, 'sole approved performance index'):
            self.verify_index(installed)
        with self.assertRaisesRegex(ValueError, 'other schema objects'):
            self.installer.rollback(SimpleNamespace(output_dir=self.args.output_dir))
        self.assertEqual(self.has_index()[0], wrong)
        self.assertEqual(_table_digests(self.f.connection), retained)

    def test_install_failure_rolls_back_ddl_and_does_not_change_tables(self):
        retained = _table_digests(self.f.connection)
        with patch.object(self.installer, 'verify_after', side_effect=ValueError('offline post-DDL failure')):
            with self.assertRaisesRegex(ValueError, 'offline post-DDL failure'):
                self.installer.install(self.args)
        self.assertIsNone(self.has_index())
        self.assertEqual(_table_digests(self.f.connection), retained)
        self.assertEqual(schema_v23.migration_proof(self.f.connection), self.proof)

    def test_backup_tampering_blocks_recovery_and_rollback_without_touching_live_rows(self):
        installed = self.installer.install(self.args)
        receipt = release.object_at(installed)
        self.f.connection.execute("UPDATE accounts SET operator_name='retained after backup tamper' WHERE id=(SELECT min(id) FROM accounts)")
        self.f.connection.commit()
        retained = _table_digests(self.f.connection)
        with Path(receipt['backup']['path']).open('ab') as stream:
            stream.write(b'tampered')
        for action in (self.installer.recover, self.installer.rollback):
            with self.assertRaisesRegex(ValueError, 'backup changed'):
                action(SimpleNamespace(output_dir=self.args.output_dir))
        self.assertIsNotNone(self.has_index())
        self.assertEqual(_table_digests(self.f.connection), retained)

    def test_receipt_binding_rejects_other_inode_source_checks_or_predecessor(self):
        installed = self.installer.install(self.args)
        receipt = release.object_at(installed)
        for key, value in (
            ('database_identity', {**receipt['database_identity'], 'inode': self.original_inode + 1}),
            ('source_tree', self.origin_payload['account_cleanup_generation']['source_tree']),
            ('checks', self.origin_payload[release.FIELD]['checks']),
            ('code_predecessor_build', self.h.parent_ref),
        ):
            with self.subTest(binding=key):
                changed = {**receipt, key: value}
                changed['receipt_sha256'] = release.digest({k: v for k, v in changed.items() if k != 'receipt_sha256'})
                ref = release.reference(Path(self.f.write('wrong-index-' + key + '.json', changed)['path']))
                with self.assertRaisesRegex(ValueError, 'index install proof differs'):
                    self.verify_index(ref)
        self.assertEqual(schema_v23.migration_proof(self.f.connection), self.proof)


if __name__ == '__main__':
    unittest.main()
