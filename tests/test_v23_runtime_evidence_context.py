"""Generation/read-set fences; the full sealed schema23 integration is separate."""
from contextlib import contextmanager
from datetime import datetime, timezone
from dataclasses import FrozenInstanceError
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from v8 import runtime_evidence_context as context, four_platform_flow_release as release, runtime_database
from v8 import forward_recovery as recovery


class RuntimeEvidenceContextTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.root = Path(temp.name).resolve()
        self.db = self.root/'db.sqlite3'
        self.connection = sqlite3.connect(self.db); self.addCleanup(self.connection.close)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript("PRAGMA journal_mode=WAL; PRAGMA user_version=23; CREATE TABLE proof(value); INSERT INTO proof VALUES('original');")
        self.source = self.root/'source'; (self.source/'src').mkdir(parents=True)
        (self.source/'.git/refs/heads').mkdir(parents=True)
        self.code = self.source/'src/code.py'; self.code.write_text('safe = True\n')
        (self.source/'.git/refs/heads/main').write_text('old')
        self.backup = self.root/'backup.sqlite3'; self.backup.write_bytes(b'verified cached backup')
        tree = {'contract':'writer-source-tree-v1','source_root':str(self.source),'files':[
            {'path':'src/code.py','sha256':hashlib.sha256(self.code.read_bytes()).hexdigest()}]}
        tree_ref = self.write('tree.json',tree)
        backup_ref = {'path':str(self.backup),'sha256':hashlib.sha256(self.backup.read_bytes()).hexdigest()}
        self.build = {'source_root':str(self.source),'tree':tree_ref,'backup':backup_ref}
        self.build_ref = self.write('build.json',{'contract_version':'sealed-build-receipt-v1',
            'payload':self.build,'payload_sha256':release.digest(self.build)})
        self.install_ref = self.write('install.json',{})
        self.plist = self.root/'writer.plist'; self.plist.write_bytes(b'installed')
        self.enterContext(patch.dict(os.environ,{'DCAR_LOADED_BUILD_ID':'sha256:'+self.build_ref['sha256'],
            'DCAR_LOADED_BUILD_RECEIPT':self.build_ref['path'],'DCAR_WRITER_SOURCE_ROOT':str(self.source),
            'DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT':self.install_ref['path']}))
        self.enterContext(patch.object(context,'_loaded_source_root',return_value=self.source))
        self.enterContext(patch.object(runtime_database,'load_installed_writer_contract',return_value=SimpleNamespace(database=self.db,plist_path=self.plist)))
        self.owner = self.enterContext(patch.object(runtime_database,'require_current_process_writer_lock'))
        self.calls = 0
        def proof(**kwargs):
            self.calls += 1
            self.assertFalse(self.connection.in_transaction, 'full proof entered the write transaction')
            self.assertEqual(self.code.read_text(),'safe = True\n')
            c = kwargs['connection']
            c.execute('PRAGMA foreign_keys').fetchone()
            c.execute('PRAGMA recursive_triggers').fetchone()
            row = c.execute('SELECT value FROM proof').fetchone()
            # The second read is deduplicated only in the closed WAL snapshot.
            c.execute('SELECT value FROM proof').fetchone()
            return {'immutable':row[0]}
        self.verifier = self.enterContext(patch.object(release,'verify_inheritance',side_effect=proof))

    def write(self,name,value):
        path = self.root/name;path.write_text(json.dumps(value));path.chmod(0o600)
        return release.reference(path)

    def reuse(self, **changes):
        values = dict(connection=self.connection,build=self.build,build_ref=self.build_ref,
            install_path=Path(self.install_ref['path']),database=self.db,source=self.source,
            at=datetime.now(timezone.utc).isoformat())
        values.update(changes)
        return context.reuse_inheritance(**values)

    @contextmanager
    def boundary(self):
        self.connection.execute('PRAGMA foreign_keys=ON'); self.connection.execute('PRAGMA recursive_triggers=ON')
        self.connection.execute('BEGIN IMMEDIATE')
        try:
            with context.inheritance_boundary(self.connection):yield
        finally:self.connection.rollback()

    def test_scope_is_closed_reader_single_proof_and_live_read_set(self):
        with context.prepare_inheritance(self.db) as prepared:
            self.assertFalse(self.connection.in_transaction)
            with self.boundary():
                self.assertEqual(self.reuse(),{'immutable':'original'})
                value = self.reuse();value['immutable']='caller mutation'
                self.assertEqual(self.reuse(),{'immutable':'original'})
                self.assertEqual(len(prepared.queries),3)
            self.assertEqual(self.calls,1)
        self.assertIsNone(self.reuse())
        with context.prepare_inheritance(self.db),self.boundary():self.assertEqual(self.reuse(),{'immutable':'original'})
        self.assertEqual(self.calls,2)

    def test_prepared_proof_and_read_set_are_deeply_immutable(self):
        with context.prepare_inheritance(self.db) as prepared,self.boundary():
            with self.assertRaises(FrozenInstanceError):prepared.proof = '{}'
            with self.assertRaises(TypeError):prepared.files[self.code] = context._generation(self.code)
            with self.assertRaises(TypeError):prepared.environment['DCAR_LOADED_BUILD_ID'] = 'bad'
            self.assertIsInstance(prepared.queries,tuple)
            self.assertIsInstance(prepared.queries[0][1][0],tuple)
            self.assertEqual(self.reuse(),{'immutable':'original'})

    def test_worktree_and_external_git_objects_are_rejected(self):
        for name in ('commondir','objects/info/alternates'):
            path = self.source/'.git'/name;path.parent.mkdir(parents=True,exist_ok=True)
            path.write_text(str(self.root))
            with self.assertRaises(context.RuntimeEvidenceChanged),context.prepare_inheritance(self.db):pass
            path.unlink()
        git=self.source/'.git';git.rename(self.source/'saved-git');git.write_text('gitdir: saved-git')
        with self.assertRaises(context.RuntimeEvidenceChanged),context.prepare_inheritance(self.db):pass

    def test_no_context_and_connection_none_keep_cold_verifier_contract(self):
        self.assertIsNone(self.reuse())
        with context.prepare_inheritance(self.db):self.assertIsNone(self.reuse(connection=None))

    def test_same_inode_edit_with_restored_mtime_is_rejected(self):
        with context.prepare_inheritance(self.db):
            old = self.code.stat();self.code.write_text('evil = True\n');os.utime(self.code,ns=(old.st_atime_ns,old.st_mtime_ns))
            self.assertEqual(self.code.stat().st_ino,old.st_ino)
            with self.assertRaises(context.RuntimeEvidenceChanged),self.boundary():pass
        self.assertEqual(self.calls,1)

    def test_source_change_after_reuse_rolls_back_at_exit_and_preserves_original_exception(self):
        self.connection.execute('CREATE TABLE staged_send(value)');self.connection.commit()
        with context.prepare_inheritance(self.db):
            with self.assertRaises(context.RuntimeEvidenceChanged),self.boundary():
                self.reuse()
                self.connection.execute("INSERT INTO staged_send VALUES('would send after commit')")
                old=self.code.stat();self.code.write_text('evil = True\n')
                os.utime(self.code,ns=(old.st_atime_ns,old.st_mtime_ns))
                self.reuse()  # DB-only intermediate check; commit fence denies.
        self.assertEqual(self.connection.execute('SELECT count(*) FROM staged_send').fetchone()[0],0)
        self.code.write_text('safe = True\n')
        with context.prepare_inheritance(self.db):
            with self.assertRaisesRegex(RuntimeError,'original failure'),self.boundary():
                self.code.write_text('evil = True\n')
                raise RuntimeError('original failure')

    def test_new_ignored_executable_git_ref_and_installed_plist_invalidate(self):
        for target, body in ((self.source/'src/extra.py','injected'),
                             (self.source/'.git/refs/heads/main','new'),(self.plist,'changed')):
            with self.subTest(path=target.name):
                old = target.read_bytes() if target.exists() else None
                with context.prepare_inheritance(self.db):
                    target.write_text(body)
                    with self.assertRaises(context.RuntimeEvidenceChanged),self.boundary():pass
                if old is None:target.unlink()
                else:target.write_bytes(old)

    def test_cached_backup_not_opened_by_verifier_is_still_fenced(self):
        with context.prepare_inheritance(self.db) as prepared:
            self.assertIn(self.backup,prepared.files)
            self.backup.write_bytes(b'bad cached backup data')
            with self.assertRaises(context.RuntimeEvidenceChanged),self.boundary():pass

    def test_sqlite_backup_wal_appearance_invalidates_the_cached_read_view(self):
        with context.prepare_inheritance(self.db):
            Path(str(self.backup)+'-wal').write_bytes(b'new backup read view')
            with self.assertRaises(context.RuntimeEvidenceChanged),self.boundary():pass

    def runtime_identity(self, roots):
        metadata=self.db.stat()
        build={'status':'succeeded','runtime_root_receipt':{'path':str(self.root/'runtime.json'),'sha256':'b'*64},
            'critical_files':{'src/code.py':hashlib.sha256(b'safe = True\n').hexdigest()}}
        runtime={'project_root':str(self.root),'formal_database':{'path':str(self.db),
            'device':metadata.st_dev,'inode':metadata.st_ino}}
        with patch.object(recovery,'PROJECT_ROOT',self.root),patch.object(recovery,'source_root',side_effect=roots) as resolver, \
             patch.object(recovery,'_private_receipt',side_effect=[build,runtime]):
            result=recovery._runtime_identity(self.connection,{'build_receipt_sha256':self.build_ref['sha256'],
                'runtime_root_receipt_sha256':'b'*64})
            self.assertEqual(resolver.call_count,2)
            return result

    def test_runtime_identity_resolves_root_once_and_rechecks_drift(self):
        self.assertEqual(self.runtime_identity([self.source,self.source])['database_inode'],self.db.stat().st_ino)
        with self.assertRaisesRegex(Exception,'source root changed'):
            self.runtime_identity([self.source,self.root])

    def test_runtime_identity_still_rejects_symlink_and_wrong_file_bytes(self):
        target=self.root/'same-content.py';target.write_bytes(self.code.read_bytes())
        self.code.unlink();self.code.symlink_to(target)
        with self.assertRaisesRegex(Exception,'Runtime code changed'):
            self.runtime_identity([self.source,self.source])
        self.code.unlink();self.code.write_text('evil = True\n')
        with self.assertRaisesRegex(Exception,'Runtime code changed'):
            self.runtime_identity([self.source,self.source])

    def test_read_set_rechecks_after_savepoint_rollback_without_total_changes_cache(self):
        with context.prepare_inheritance(self.db),self.boundary():
            self.assertEqual(self.reuse(),{'immutable':'original'})
            self.connection.execute('SAVEPOINT before_update')
            self.connection.execute("UPDATE proof SET value='changed'")
            total = self.connection.total_changes
            with self.assertRaises(context.RuntimeEvidenceChanged):self.reuse()
            self.connection.execute('ROLLBACK TO before_update')
            self.assertEqual(self.connection.total_changes,total)
            self.assertEqual(self.reuse(),{'immutable':'original'})
            self.connection.execute('RELEASE before_update')

    def test_read_set_cas_drift_between_prepare_and_begin_never_calls_cold_under_lock(self):
        with context.prepare_inheritance(self.db):
            self.connection.execute("UPDATE proof SET value='revoked'");self.connection.commit()
            with self.assertRaises(context.RuntimeEvidenceChanged),self.boundary():pass
        self.assertEqual(self.calls,1)

    def test_environment_clock_owner_and_connection_drift_fail_closed(self):
        with context.prepare_inheritance(self.db),self.boundary():
            with patch.dict(os.environ,{'DCAR_WRITER_SOURCE_ROOT':str(self.root)}):
                with self.assertRaises(context.RuntimeEvidenceChanged):self.reuse()
            with self.assertRaises(context.RuntimeEvidenceChanged):self.reuse(at='2000-01-01T00:00:00Z')
            with patch.object(runtime_database,'require_current_process_writer_lock',side_effect=RuntimeError('owner lost')):
                with self.assertRaises(RuntimeError):self.reuse()
            other = sqlite3.connect(self.db)
            try:
                with self.assertRaises(context.RuntimeEvidenceChanged):self.reuse(connection=other)
            finally:other.close()

    def test_wrong_build_source_or_database_has_no_cold_fallback(self):
        with context.prepare_inheritance(self.db),self.boundary():
            for values in ({'build':{}},{'source':self.root},{'database':self.root/'another.db'}):
                with self.assertRaises(context.RuntimeEvidenceChanged):self.reuse(**values)
        self.assertEqual(self.calls,1)

    def test_readonly_snapshot_rejects_write_pragma_and_cursor_escape(self):
        with sqlite3.connect(self.db) as c:
            values=context._ReadSet(c)
            for sql in ('DELETE FROM proof','PRAGMA foreign_keys=OFF','PRAGMA writable_schema=ON'):
                with self.assertRaises(context.RuntimeEvidenceChanged):values.execute(sql)
            with self.assertRaises(context.RuntimeEvidenceChanged):values.cursor()

    def test_schema22_and_diagnostic_keep_original_contract(self):
        self.connection.execute('PRAGMA user_version=22')
        with context.prepare_inheritance(self.db) as prepared:self.assertIsNone(prepared)
        with context.prepare_inheritance(self.db,enabled=False) as prepared:self.assertIsNone(prepared)
        self.assertEqual(self.calls,0)
