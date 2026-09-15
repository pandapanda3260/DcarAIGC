#!/usr/bin/env python3
"""Install or remove one exact schema23 performance index under maintenance.

No Writer launch changes, table rewrites, migration replacement or provider
calls. A complete schema23 snapshot preserves the original verifier's input.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import plistlib
import sqlite3
import sys

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/dcar_eval'))
sys.path.insert(0, str(ROOT / 'scripts'))
from v8 import four_platform_flow_release as release, schema_v23
from v8.capture_work_index import INDEX_NAME, INDEX_SQL, INDEX_OBJECT
from v8.runtime_database import hold_formal_mutation
from v8.schema_v22 import _table_digests
from install_account_classification import connect, write, write_recovered


def now():
    return datetime.now(timezone.utc).isoformat()


def inputs(args, *, origin_backup=None):
    checks = {}
    for value in args.check_report:
        name, separator, path = value.partition('=')
        release.require(separator and name not in checks, 'unique named checks required')
        checks[name] = release.reference(Path(path))
    origin_ref = release.reference(args.code_predecessor_build)
    checked = release.verify_candidate_source(source=ROOT,
        parent_build_ref=release.reference(args.parent_build), checks=checks)
    release.require(checked['source_tree_ref'] == release.reference(args.source_tree), 'index source differs')
    origin, inherited = release.code_parent_context(origin_ref, install_path=args.parent_install,
        database=args.database, origin_backup=origin_backup)
    release.require(origin[release.FIELD]['parent_build'] == release.reference(args.parent_build)
        and origin[release.FIELD]['parent_install'] == release.reference(args.parent_install), 'index lineage differs')
    body = release.raw(args.installed_plist, private=False); plist = plistlib.loads(body)
    env = plist.get('EnvironmentVariables', {})
    release.require(plist.get('Label') == 'cn.tj.dcar.writer-worker'
        and plist.get('WorkingDirectory') == str(args.project_root) == origin['project_root']
        and env.get('DCAR_PROJECT_ROOT') == str(args.project_root)
        and env.get('DCAR_V8_DB') == str(args.database)
        and env.get('DCAR_WRITER_SOURCE_ROOT') == origin['source_root']
        and release.reference(Path(env.get('DCAR_LOADED_BUILD_RECEIPT',''))) == origin_ref
        and env.get('DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT') == str(args.parent_install)
        and plist.get('ProgramArguments') == [str(Path(origin['source_root'])/'deploy/macos/run_writer_worker.sh')],
        'index maintenance requires unchanged original schema23 Writer plist')
    return {'source_tree': checked['source_tree_ref'], 'checks': checks,
        'changes': release.code_repair_changes(origin, checked['source_tree']),
        'code_predecessor_build': origin_ref, 'parent_build': release.reference(args.parent_build),
        'parent_install': release.reference(args.parent_install),
        'origin_proof_sha256': inherited['four_platform_flow_proof']['proof_sha256'],
        'installed_plist': str(args.installed_plist),
        'previous_writer_plist_sha256': hashlib.sha256(body).hexdigest()}


def receipt(preflight):
    value = {**preflight, 'contract': 'capture-work-index-install-v1', 'status': 'installed',
        'index_name': INDEX_NAME, 'index_sql': INDEX_SQL, 'schema_version': 23,
        'schema_migration_repeated': False, 'business_rows_changed': 0,
        'provider_calls': 0, 'backfill_jobs': 0, 'retained_tables_verified': True}
    return {**value, 'receipt_sha256': release.digest(value)}


def verify_after(connection, preflight, *, check_rows):
    with release.readonly_database(Path(preflight['backup']['path'])) as original:
        before = schema_v23.objects(original)
    expected = sorted([*before, INDEX_OBJECT], key=lambda row:(row[0],row[1]))
    release.require(schema_v23.objects(connection) == expected
        and release.digest(expected) == preflight['after_schema_sha256']
        and schema_v23.migration_proof(connection) == preflight['original_migration_proof'],
        'performance index changed other schema objects or migration proof')
    if check_rows:
        release.require(_table_digests(connection) == preflight['retained_tables'], 'index installation changed business rows')


def install(args):
    release.require(not args.output_dir.exists() and not args.output_dir.is_symlink(), 'new index evidence directory required')
    checked = inputs(args)
    args.output_dir.mkdir(mode=0o700, parents=True)
    backup = args.output_dir/'before.sqlite3'
    with hold_formal_mutation(args.database, project_root=args.project_root) as access:
        release.require(inputs(args) == checked, 'index authority changed before maintenance')
        identity = access.database.stat()
        with connect(access.database) as live:
            before = schema_v23.objects(live)
            release.require(not any(row[1] == INDEX_NAME for row in before), 'performance index already exists')
            migration = schema_v23.migration_proof(live)
            retained = _table_digests(live)
            os.close(os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600))
            with sqlite3.connect(backup) as target: live.backup(target)
            backup_ref = release.file_reference(backup)
            with connect(backup, read_only=True) as original:
                release.require(original.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
                    and schema_v23.migration_proof(original) == migration
                    and _table_digests(original) == retained, 'schema23 index backup differs')
            release.require(inputs(args, origin_backup=backup_ref) == checked, 'original schema23 backup proof differs')
            preflight = {**checked, 'contract':'capture-work-index-preflight-v1',
                'formal_database':str(access.database), 'project_root':str(args.project_root),
                'database_identity':{'device':identity.st_dev,'inode':identity.st_ino},
                'database_mode':identity.st_mode, 'backup':backup_ref,
                'original_migration_proof':migration, 'retained_tables':retained,
                'before_schema_sha256':release.digest(before),
                'after_schema_sha256':release.digest(sorted([*before,INDEX_OBJECT],key=lambda row:(row[0],row[1]))),
                'prepared_at':now()}
            write(args.output_dir/'index-preflight.json',preflight)
            try:
                live.execute('BEGIN IMMEDIATE')
                live.execute(INDEX_SQL)
                verify_after(live,preflight,check_rows=True)
                release.require(live.execute('PRAGMA foreign_key_check').fetchone() is None, 'index foreign key violation')
                live.commit()
            except BaseException:
                live.rollback();raise
            after=access.database.stat()
            release.require((after.st_dev,after.st_ino,after.st_mode)==(identity.st_dev,identity.st_ino,identity.st_mode)
                and live.execute('PRAGMA integrity_check').fetchone()[0]=='ok', 'index database identity or integrity changed')
            live.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        return write(args.output_dir/'index-install.json',receipt(preflight))


def retained_context(args):
    p=release.object_at(release.reference(args.output_dir/'index-preflight.json'))
    release.require(p.get('contract')=='capture-work-index-preflight-v1', 'original index preflight required')
    release.verify_backup(p['backup'])
    source=argparse.Namespace(database=Path(p['formal_database']),project_root=Path(p['project_root']),
        installed_plist=Path(p['installed_plist']),parent_build=Path(p['parent_build']['path']),
        parent_install=Path(p['parent_install']['path']),source_tree=Path(p['source_tree']['path']),
        check_report=[k+'='+v['path'] for k,v in p['checks'].items()],
        code_predecessor_build=Path(p['code_predecessor_build']['path']))
    release.require(all(p.get(k)==v for k,v in inputs(source,origin_backup=p['backup']).items()), 'index recovery authority changed')
    identity=source.database.stat()
    release.require(p['database_identity']=={'device':identity.st_dev,'inode':identity.st_ino}
        and p['database_mode']==identity.st_mode, 'index recovery database identity changed')
    return p,source


def recover(args):
    p,source=retained_context(args)
    with hold_formal_mutation(source.database,project_root=source.project_root):
        release.require(retained_context(args)[0]==p, 'index recovery changed before maintenance')
        with connect(source.database) as live: verify_after(live,p,check_rows=True)
        return write_recovered(args.output_dir/'index-install.json',receipt(p))


def rollback(args):
    p,source=retained_context(args)
    with hold_formal_mutation(source.database,project_root=source.project_root):
        release.require(retained_context(args)[0]==p, 'index rollback changed before maintenance')
        with connect(source.database) as live:
            if not any(row[1]==INDEX_NAME for row in schema_v23.objects(live)):
                # A prior DROP may have committed before its external receipt
                # was saved. Verify the exact original structure, then only
                # recover the receipt; later business rows remain untouched.
                release.require(release.digest(schema_v23.objects(live))==p['before_schema_sha256']
                    and schema_v23.migration_proof(live)==p['original_migration_proof'],
                    'index rollback recovery structure differs')
            else:
                verify_after(live,p,check_rows=False)
                # Preserve later business writes. Never restore the snapshot file.
                retained=_table_digests(live)
                try:
                    live.execute('BEGIN IMMEDIATE');live.execute('DROP INDEX '+INDEX_NAME)
                    release.require(release.digest(schema_v23.objects(live))==p['before_schema_sha256']
                        and schema_v23.migration_proof(live)==p['original_migration_proof']
                        and _table_digests(live)==retained, 'index rollback modified business data')
                    live.commit()
                except BaseException:live.rollback();raise
        inputs(source)
    return write_recovered(args.output_dir/'index-rollback.json',{'contract':'capture-work-index-rollback-v1',
        'status':'rolled_back','formal_database':str(source.database),'backup_restored':False,
        'business_rows_changed':0,'provider_calls':0,'completed_at':now()},ignore_time=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='action',required=True)
    p=sub.add_parser('install')
    for name in ('database','project-root','installed-plist','parent-build','parent-install','source-tree','code-predecessor-build','output-dir'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--check-report',action='append',default=[])
    for name in ('recover-receipt','rollback'):sub.add_parser(name).add_argument('--output-dir',type=Path,required=True)
    args=parser.parse_args()
    for value in vars(args).values():
        if isinstance(value,Path):release.require(value.is_absolute() and value.resolve()==value,'absolute nonsymlink paths required')
    print(json.dumps({'install':install,'recover-receipt':recover,'rollback':rollback}[args.action](args),ensure_ascii=False))

if __name__=='__main__':main()
