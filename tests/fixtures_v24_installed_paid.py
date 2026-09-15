"""Disposable, genuinely migrated schema24 Writer for full A/B integration.

Only source-location and fixture install pins are supplied by the existing
release fixtures. All real migration proofs, source hashes, lease checks,
operation gates, route checks, admission and budget ledgers remain active.
"""
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
import os
from pathlib import Path
import plistlib
import shutil
import sqlite3
import unittest
from unittest.mock import patch

from tests import test_four_platform_flow_release as fixtures
from v8 import duplicate_index_release as release, runtime_database, schema_v24, storage

ROOT = Path(__file__).resolve().parents[1]


class InstalledDuplicateFixture(unittest.TestCase):
    def setUp(self):
        base = self.base = fixtures.FourPlatformFlowReleaseTest(); base.setUp()
        self.addCleanup(base.doCleanups)
        f = self.f = base.f
        migration23 = base.installer.install(base.args)
        proposal23 = base.prepare(migration23)
        plist23 = plistlib.loads(Path(proposal23['next_plist']['path']).read_bytes())
        base.installed_path.write_bytes(plistlib.dumps(plist23)); base.installed_path.chmod(0o600)
        installed23 = replace(base.installed, payload=plist23)
        self.enterContext(patch.object(runtime_database, 'load_installed_writer_contract', return_value=installed23))
        self.enterContext(patch.dict(os.environ, {**plist23['EnvironmentVariables'],
            'DCAR_LOADED_BUILD_ID': 'sha256:' + proposal23['child_build']['sha256']}))
        self.parent_ref = proposal23['child_build']
        self.parent = release.payload_at(self.parent_ref, 'sealed-build-receipt-v1')
        f.connection.commit()
        self.backup = f.root / 'duplicate-before23.sqlite3'
        with sqlite3.connect(self.backup) as destination:
            f.connection.backup(destination); destination.execute('PRAGMA journal_mode=DELETE')
        self.backup.chmod(0o600)
        self.backup_ref = release.file_reference(self.backup)
        _, inherited = release.parent_context(self.parent_ref, install_path=f.install_path,
            database=f.db, backup_ref=self.backup_ref)
        self.source = f.root / 'duplicate-source'
        shutil.copytree(base.source, self.source)
        # This distinct fixture transition supplies the current implementation;
        # the comment gives an actual changed receipt-owned migration module.
        module = self.source / release.MODULE
        module.write_bytes(module.read_bytes() + b'\n# Disposable schema24 A/B fixture transition.\n')
        self.tree = release.inventory(self.source)
        self.tree_ref = f.write('duplicate-tree.json', self.tree)
        parent_tree = release.object_at(self.parent['account_cleanup_generation']['source_tree'])
        changes = release.source_changes(parent_tree, self.tree)
        checks = {}
        for name in sorted(release.CHECKS):
            log = f.root / ('duplicate-' + name + '.log')
            log.write_text('Disposable test authority; not production acceptance.\n'); log.chmod(0o600)
            checks[name] = f.write('duplicate-' + name + '.json', {
                'contract': release.CHECK_CONTRACT, 'name': name, 'status': 'passed', 'exit_code': 0,
                'source_tree': self.tree_ref, 'changes': changes, 'command': ['offline-fixture-only'],
                'output': release.reference(log)})
        with runtime_database.hold_formal_mutation(f.db, project_root=f.project) as access:
            schema_v24.migrate(f.connection, maintenance=schema_v24.MaintenanceContext(
                access, self.backup, self.backup_ref['sha256']))
        proof = schema_v24.migration_proof(f.connection)
        ident = f.db.stat()
        migration = {'contract': release.INSTALL_CONTRACT, 'status': 'migrated', 'from_schema': 23, 'to_schema': 24,
            'formal_database': str(f.db), 'database_identity': {'device': ident.st_dev, 'inode': ident.st_ino},
            'authority_build': self.parent_ref, 'authority_install': release.reference(f.install_path),
            'source_tree': self.tree_ref, 'changes': changes, 'checks': checks, 'backup': self.backup_ref,
            'parent_inheritance_sha256': release.digest(inherited), 'migration_proof': proof,
            'paid_gates_issued': 0, 'preserved_tables_verified': True, 'migrated_at': proof['applied_at']}
        migration['receipt_sha256'] = release.digest(migration)
        self.migration_ref = f.write('duplicate-migration.json', migration)
        at = datetime.now(timezone.utc).isoformat()
        plan = {'contract': release.CONTRACT, 'parent_build': self.parent_ref,
            'parent_install': release.reference(f.install_path), 'source_tree': self.tree_ref, 'changes': changes,
            'checks': checks, 'migration': self.migration_ref, 'engine': 'mih', 'issued_at': at}
        source_plan = f.write('duplicate-source-plan.json', {'contract': 'account-cleanup-source-plan-v1',
            'transition': 'account-cleanup-0907-v1', 'project_root': str(f.project), 'source_root': str(self.source),
            'git': self.tree['git'], 'source_tree': self.tree_ref})
        self.build = {**self.parent, 'source_root': str(self.source), 'git': self.tree['git'],
            'critical_files': {row['path']: row['sha256'] for row in self.tree['files']
                if row['path'].startswith(('src/', 'config/')) and row['path'].endswith(('.py', '.json'))},
            'code_successor_plan': source_plan,
            'account_cleanup_generation': {**self.parent['account_cleanup_generation'], 'source_tree': self.tree_ref},
            release.FIELD: plan, 'schema_contract': {'code_schema': 24, 'formal_schema': 24},
            'created_at': at, 'validation_scope': 'disposable A/reserve/B fixture only'}
        self.build_ref = f.envelope('duplicate-build.json', 'sealed-build-receipt-v1', self.build)
        release.verify_inheritance(build=self.build, build_ref=self.build_ref, install_path=f.install_path,
            database=f.db, source=self.source, connection=f.connection)
        plist24 = {**plist23, 'EnvironmentVariables': {**plist23['EnvironmentVariables'],
            'DCAR_WRITER_SOURCE_ROOT': str(self.source), 'DCAR_LOADED_BUILD_RECEIPT': self.build_ref['path']},
            'ProgramArguments': [str(self.source / 'deploy/macos/run_writer_worker.sh')]}
        base.installed_path.write_bytes(plistlib.dumps(plist24)); base.installed_path.chmod(0o600)
        self.installed = replace(installed23, payload=plist24)
        self.enterContext(patch.object(runtime_database, 'load_installed_writer_contract', return_value=self.installed))
        self.enterContext(patch.dict(os.environ, {**plist24['EnvironmentVariables'],
            'DCAR_LOADED_BUILD_ID': 'sha256:' + self.build_ref['sha256']}))
        from v8 import runtime_evidence_context as evidence, duplicate_index as index
        self.enterContext(patch.object(evidence, '_loaded_source_root', return_value=self.source))
        self.enterContext(patch.object(release, '__file__', str(self.source / release.MODULE)))
        access = runtime_database.resolve_installed_database_access(runtime_database.DatabaseAccessMode.WRITER,
            database=f.db, project_root=f.project)
        self.enterContext(runtime_database.acquire_writer_lock(access))
        with storage.transaction(f.connection):
            self.generation_id = index.create_generation(f.connection)['generation_id']
            f.connection.execute("UPDATE duplicate_index_generations SET state='ready' WHERE generation_id=?", (self.generation_id,))
