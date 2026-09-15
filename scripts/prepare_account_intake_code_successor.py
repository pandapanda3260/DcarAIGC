#!/usr/bin/env python3
"""Prepare an immutable local schema22 code repair; never install or start it."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import plistlib
import sqlite3
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src/dcar_eval'))
sys.path.insert(0, str(ROOT/'scripts'))
from v8 import account_intake_release as intake, account_intake_code_successor as release
from v8.runtime_paths import verified_git
from prepare_account_intake_release import write, write_bytes, checks_at


def _now():
    return datetime.now(timezone.utc).isoformat()


def _candidate(parent_ref):
    parent = intake.payload_at(parent_ref, 'sealed-build-receipt-v1')
    intake.require(parent.get('schema_contract') == {'code_schema':22, 'formal_schema':22}
                   and release.FIELD not in parent, 'original schema22 parent required')
    tree = intake.inventory(ROOT)
    changes = release.source_changes(intake.object_at(parent['account_cleanup_generation']['source_tree']), tree)
    return parent, tree, changes


def freeze(args):
    parent, before, _ = _candidate(intake.reference(args.parent_build))
    data = Path(parent['project_root'])
    intake.require(not args.source_root.exists() and args.source_root.resolve() == args.source_root and
        all(not args.source_root.is_relative_to(other) and not other.is_relative_to(args.source_root)
            for other in (ROOT, data, Path(parent['source_root']))), 'new independent source directory required')
    intake.require(not args.source_tree.is_relative_to(ROOT) and not args.source_tree.is_relative_to(args.source_root),
                   'source manifest must be external')
    args.source_root.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(['git','clone','--local','--no-hardlinks','--branch',before['git']['branch'],str(ROOT),str(args.source_root)],
        check=True, capture_output=True, env={**os.environ,'GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':os.devnull})
    tracked = {os.fsdecode(value) for value in verified_git(args.source_root,'ls-files','-z').split(b'\0') if value}
    for name in tracked - {row['path'] for row in before['files']}:
        (args.source_root/name).unlink()
    for row in before['files']:
        body = intake.raw(ROOT/row['path'], private=False)
        intake.require(hashlib.sha256(body).hexdigest() == row['sha256'], 'source changed during freeze')
        target = args.source_root/row['path']; target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body); target.chmod(row['mode'])
    after = intake.inventory(args.source_root)
    intake.require(intake.inventory(ROOT) == before and after['files'] == before['files'] and
                   after['git'] == before['git'], 'frozen source differs from candidate')
    args.source_tree.parent.mkdir(parents=True, exist_ok=True)
    return {'status':'frozen','source_tree':write(args.source_tree, after),'services_changed':False}


def check(args):
    parent_ref = intake.reference(args.parent_build)
    _, before, changes = _candidate(parent_ref)
    tree_ref = intake.reference(args.source_tree)
    intake.require(intake.object_at(tree_ref) == before, 'checks must run against the exact frozen source')
    command = args.command[1:] if args.command and args.command[0] == '--' else args.command
    intake.require(args.name in release.CHECKS and bool(command), 'required check and command missing')
    intake.require(not args.output.is_relative_to(ROOT), 'check output must be external')
    environment = {k:v for k,v in os.environ.items() if not k.startswith('DCAR_')}
    environment.update(PYTHONDONTWRITEBYTECODE='1', PYTHONPATH=os.pathsep.join(str(ROOT/p) for p in
        ('src/dcar_eval','tests','scripts')), DCAR_SCHEDULER_ENABLED='0', DCAR_STARTUP_CATCHUP_ENABLED='0',
        DCAR_TEST_DENY_FORMAL_DB='1')
    result = subprocess.run(command, cwd=ROOT, env=environment, capture_output=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    output = write_bytes(args.output.with_suffix('.log'), result.stdout+result.stderr)
    intake.require(intake.inventory(ROOT) == before, 'source changed during focused check')
    report = {'contract':release.CHECK_CONTRACT,'name':args.name,'source_tree':tree_ref,'changes':changes,
        'status':'passed' if result.returncode == 0 else 'failed','exit_code':result.returncode,
        'command':command,'output':output,'completed_at':_now()}
    ref = write(args.output, report)
    intake.require(result.returncode == 0, 'focused check failed; inspect '+str(args.output.with_suffix('.log')))
    return {'report':ref,'status':report['status']}


def _sha(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for body in iter(lambda: stream.read(1024*1024), b''):
            result.update(body)
    return result.hexdigest()


def _database_files(database):
    return {str(path):{'sha256':_sha(path),'bytes':path.stat().st_size}
            for path in (database, Path(str(database)+'-wal')) if path.exists()}


def prepare(args):
    parent_ref = intake.reference(args.parent_build)
    parent, tree, changes = _candidate(parent_ref)
    installed_ref = intake.reference(args.installed_build) if getattr(args, 'installed_build', None) else parent_ref
    previous = intake.payload_at(installed_ref, 'sealed-build-receipt-v1')
    data = Path(parent['project_root'])
    installed_bytes = intake.raw(args.installed_plist, private=False)
    installed = plistlib.loads(installed_bytes); env = installed.get('EnvironmentVariables', {})
    database = Path(env.get('DCAR_V8_DB',''))
    install_path = Path(env.get('DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT',''))
    intake.require(installed.get('Label') == 'cn.tj.dcar.writer-worker' and
        installed.get('WorkingDirectory') == str(data) and env.get('DCAR_PROJECT_ROOT') == str(data) and
        env.get('DCAR_WRITER_SOURCE_ROOT') == previous['source_root'] and
        intake.reference(Path(env.get('DCAR_LOADED_BUILD_RECEIPT',''))) == installed_ref and
        installed.get('ProgramArguments') == [str(Path(previous['source_root'])/'deploy/macos/run_writer_worker.sh')],
        'installed Writer does not match the exact schema22 installed build')
    intake.require(database.is_absolute() and database.resolve(strict=True) == database and
        not args.evidence_root.exists() and args.evidence_root.resolve() == args.evidence_root and
        all(not args.evidence_root.is_relative_to(other) for other in (ROOT, data, Path(parent['source_root']), Path(previous['source_root']))),
        'database or independent evidence path differs')
    checks = checks_at(args.check_report)
    intake.require(set(checks) == release.CHECKS, 'both focused checks required')
    refs = [intake.object_at(ref).get('source_tree') for ref in checks.values()]
    intake.require(all(ref == refs[0] for ref in refs) and intake.object_at(refs[0]) == tree,
                   'checks bind another source')
    tree_ref = refs[0]
    lock = Path(env.get('DCAR_WRITER_LOCK',''))
    intake.require(lock.is_absolute() and lock.is_file() and not lock.is_symlink(), 'installed Writer lock unavailable')
    lock_fd = os.open(lock, os.O_RDWR | os.O_NOFOLLOW)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        held = os.fstat(lock_fd)
        intake.require((held.st_dev, held.st_ino) == (lock.stat().st_dev, lock.stat().st_ino), 'Writer lease inode changed')
        identity = database.stat(); before = _database_files(database)
        at = _now()
        with sqlite3.connect(database.as_uri()+'?mode=ro', uri=True) as connection:
            connection.row_factory = sqlite3.Row; connection.execute('PRAGMA query_only=ON')
            connection.execute('PRAGMA foreign_keys=ON'); connection.execute('PRAGMA recursive_triggers=ON')
            intake.require(connection.execute('PRAGMA user_version').fetchone()[0] == 22, 'formal schema22 required')
            _, inherited = release.parent_context(parent_ref, install_path=install_path, database=database,
                                                   at=at, connection=connection)
            previous_proof = None
            if installed_ref != parent_ref:
                intake.require(previous.get(release.FIELD, {}).get('parent_build') == parent_ref and
                    previous.get('schema_contract') == parent['schema_contract'] and
                    previous.get('project_root') == parent['project_root'] and
                    Path(previous['source_root']) != ROOT,
                    'installed code predecessor must retain the same original schema22 parent')
                previous_tree = intake.object_at(previous['account_cleanup_generation']['source_tree'])
                intake.require(intake.inventory(Path(previous['source_root'])) == previous_tree,
                    'complete installed predecessor source changed')
                # Execute the installed frozen verifier only after checking all
                # of its bytes. It is installation provenance, not a new root of authority.
                with release._parent_module(Path(previous['source_root']), installed_ref['sha256']) as verifier:
                    verified = verifier.verify_inheritance(build=previous, build_ref=installed_ref,
                        install_path=install_path, database=database, source=Path(previous['source_root']),
                        at=at, connection=connection)
                intake.require(intake.inventory(Path(previous['source_root'])) == previous_tree and
                    verified['intake_proof'] == inherited['intake_proof'] and
                    verified['preparation_operation_authority'] == inherited['preparation_operation_authority'] and
                    verified['intake_code_proof']['loaded_build'] == installed_ref,
                    'installed predecessor source or original capture authority changed')
                previous_proof = verified['intake_code_proof']['proof_sha256']
            args.evidence_root.mkdir(mode=0o700, parents=True)
            backup = args.evidence_root/'before.sqlite3'
            with sqlite3.connect(backup) as target:
                connection.backup(target)
                intake.require(target.execute('PRAGMA quick_check').fetchone()[0] == 'ok', 'backup integrity failed')
                intake.require(not target.execute('PRAGMA foreign_key_check').fetchall(), 'backup foreign keys failed')
            backup.chmod(0o600)
            source_plan = write(args.evidence_root/'source-plan.json', {'contract':'account-cleanup-source-plan-v1',
                'transition':'account-cleanup-0907-v1','project_root':str(data),'source_root':str(ROOT),
                'git':tree['git'],'source_tree':tree_ref})
            plan = {'contract':release.CONTRACT,'parent_build':parent_ref,'source_tree':tree_ref,'changes':changes,
                'checks':checks,'issued_at':at,'actor':args.actor,'reason':args.reason,
                'scope':release.repair_scope(changes),'schema_migration_repeated':False,'database_writes':0,
                'paid_gates_reopened':False,'publisher_authorized':False,'remote_database_authorized':False,
                'business_scope_change':'none','database_identity':{'path':str(database),'device':identity.st_dev,'inode':identity.st_ino},
                'parent_intake_proof_sha256':inherited['intake_proof']['proof_sha256'],
                'parent_operation_authority_sha256':inherited['preparation_operation_authority']['proof_sha256']}
            if previous_proof is not None:
                plan.update(previous_code_build=installed_ref, previous_code_proof_sha256=previous_proof)
            build = {**parent,'source_root':str(ROOT),'git':tree['git'],
                'critical_files':{name:row['sha256'] for name,row in intake.records(tree).items()
                    if name.startswith(('src/','config/')) and name.endswith(('.py','.json'))},
                'code_successor_plan':source_plan,
                'account_cleanup_generation':{**parent['account_cleanup_generation'],'source_tree':tree_ref},
                release.FIELD:plan,'created_at':at,'validation_scope':release.repair_scope(changes)+'; unchanged migration and capture authority'}
            child_ref = write(args.evidence_root/'build.json', {'contract_version':'sealed-build-receipt-v1',
                'payload':build,'payload_sha256':intake.digest(build)})
            release.verify_inheritance(build=build, build_ref=child_ref, install_path=install_path,
                database=database, source=ROOT, at=at, connection=connection)
        next_plist = {**installed,'ProgramArguments':[str(ROOT/'deploy/macos/run_writer_worker.sh')],
            'EnvironmentVariables':{**env,'DCAR_WRITER_SOURCE_ROOT':str(ROOT),'DCAR_LOADED_BUILD_RECEIPT':child_ref['path'],
                'PYTHONPATH':str(ROOT/'src/dcar_eval')+os.pathsep+str(ROOT/'scripts')}}
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary).resolve()
            test_plist = home/'Library/LaunchAgents/cn.tj.dcar.writer-worker.plist'
            test_plist.parent.mkdir(parents=True); test_plist.write_bytes(plistlib.dumps(next_plist)); test_plist.chmod(0o600)
            from v8.runtime_paths import verify_source_before_import
            bootstrap = verify_source_before_import(data=data, source=ROOT, build_receipt=Path(child_ref['path']), home=home)
        intake.require(_database_files(database) == before and database.stat().st_ino == identity.st_ino and
            database.stat().st_dev == identity.st_dev and intake.raw(args.installed_plist,private=False) == installed_bytes,
            'formal database or installed service changed during preparation')
        receipt = {'contract':release.CONTRACT+'-install-proposal','status':'prepared','parent_build':parent_ref,
            'previous_code_build':installed_ref, 'previous_code_proof_sha256':previous_proof,
            'child_build':child_ref,'source_root':str(ROOT),'database_identity':plan['database_identity'],
            'database_files':before,'backup':{'path':str(backup),'sha256':_sha(backup),'bytes':backup.stat().st_size},
            'writer_lease':{'path':str(lock),'device':held.st_dev,'inode':held.st_ino},
            'before_plist':write_bytes(args.evidence_root/'writer.before.plist',installed_bytes),
            'next_plist':write_bytes(args.evidence_root/'writer.next.plist',plistlib.dumps(next_plist,sort_keys=True)),
            'bootstrap_verification':bootstrap,'schema_migration_repeated':False,'database_writes':0,
            'services_changed':False,'publisher_authorized':False,
            'install_steps':['Keep Publisher disabled. Keep Writer stopped and acquire its maintenance lease.',
                'Recheck previous Writer plist hash, schema22 migration proof, backup and unchanged database device/inode.',
                'Replace only the Writer plist with writer.next.plist; never alter earlier frozen sources or receipts.',
                'Release maintenance lease, start Writer, verify the proposed build, unchanged data, bounded planning and durable commands.']}
        write(args.evidence_root/'install-proposal.json',receipt)
        return receipt
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN); os.close(lock_fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__); commands = parser.add_subparsers(dest='mode',required=True)
    for name in ('freeze','check','prepare'):
        command = commands.add_parser(name); command.add_argument('--parent-build',type=Path,required=True)
        if name == 'freeze':
            command.add_argument('--source-root',type=Path,required=True); command.add_argument('--source-tree',type=Path,required=True)
        elif name == 'check':
            command.add_argument('--source-tree',type=Path,required=True); command.add_argument('--name',choices=sorted(release.CHECKS),required=True)
            command.add_argument('--output',type=Path,required=True); command.add_argument('command',nargs=argparse.REMAINDER)
        else:
            command.add_argument('--installed-build',type=Path)
            command.add_argument('--installed-plist',type=Path,required=True); command.add_argument('--evidence-root',type=Path,required=True)
            command.add_argument('--check-report',action='append',default=[]); command.add_argument('--actor',required=True); command.add_argument('--reason',required=True)
    args=parser.parse_args(); print(json.dumps(globals()[args.mode](args),ensure_ascii=False,sort_keys=True))


if __name__ == '__main__':
    main()
