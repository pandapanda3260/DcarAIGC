#!/usr/bin/env python3
"""Build an offline candidate, freeze checked code, and prepare schema24 receipts."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import plistlib
import sqlite3
import subprocess
import sys

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/dcar_eval'))
sys.path.insert(0, str(ROOT / 'scripts'))
from v8 import duplicate_index_release as release, schema_v24
from v8.duplicate_index_build import build_existing_fingerprints
from v8.runtime_paths import verified_git
from v8.storage import configure_connection_safety, initialize_database
from prepare_account_intake_release import write, write_bytes


def now():
    return datetime.now(timezone.utc).isoformat()


def candidate(args):
    from v8.storage import is_formal_database_path, PROJECT_ROOT
    release.require(not is_formal_database_path(args.candidate), 'candidate cannot be the installed formal database')
    release.require(args.backup != args.candidate and not args.output.exists(), 'new candidate receipt required')
    backup_ref = release.file_reference(args.backup)
    args.candidate.parent.mkdir(parents=True, exist_ok=True)
    capacity = release.require_capacity(args.candidate.parent, required_bytes=backup_ref['byte_size'] * 2)
    binding = args.candidate.with_suffix('.input.json')
    if args.resume:
        saved_input = release.object_at(release.reference(binding))
        release.require(args.candidate.is_file() and saved_input['backup'] == backup_ref
                        and saved_input.get('data_root') == str(PROJECT_ROOT),
                        'resume candidate belongs to another frozen backup or media data root')
    else:
        release.require(not args.candidate.exists(), 'candidate already exists; explicit resume required')
        args.candidate.parent.mkdir(parents=True, exist_ok=True)
        os.close(os.open(args.candidate, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
        with release.readonly_frozen(backup_ref) as original, closing(sqlite3.connect(args.candidate)) as target:
            original.backup(target)
            target.execute('PRAGMA journal_mode=DELETE')
        write(binding, {'contract': 'duplicate-index-candidate-input-v1', 'backup': backup_ref,
                        'data_root': str(PROJECT_ROOT), 'created_at': now()})
    connection = sqlite3.connect(args.candidate)
    connection.row_factory = sqlite3.Row
    configure_connection_safety(connection)
    try:
        version = connection.execute('PRAGMA user_version').fetchone()[0]
        if version < 24:
            initialize_database(connection, target_version=24)
        schema_v24.validate_structure(connection)
        proof = build_existing_fingerprints(connection, progress=lambda value: print(json.dumps(value), flush=True))
        release.require(connection.execute('PRAGMA integrity_check').fetchone()[0] == 'ok', 'candidate integrity failed')
        connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        connection.execute('PRAGMA journal_mode=DELETE')
    finally:
        connection.close()
    release.require(release.file_reference(args.backup) == backup_ref, 'source backup changed during build')
    return write(args.output, {'contract': 'duplicate-index-candidate-v1', 'status': 'built',
                              'backup': backup_ref, 'candidate': release.file_reference(args.candidate),
                              'proof': proof, 'capacity': capacity, 'completed_at': now()})


def freeze(args):
    before = release.inventory(ROOT)
    parent = release.payload_at(release.reference(args.parent_build), 'sealed-build-receipt-v1')
    release.source_changes(release.object_at(parent['account_cleanup_generation']['source_tree']), before,
                           schema_transition=parent.get('schema_contract', {}).get('formal_schema') != 24)
    release.require(not args.source_root.exists() and args.source_root.resolve() == args.source_root
                    and all(not args.source_root.is_relative_to(Path(parent[key]))
                            and not Path(parent[key]).is_relative_to(args.source_root)
                            for key in ('source_root', 'project_root'))
                    and not args.source_tree.is_relative_to(args.source_root), 'independent new frozen source required')
    args.source_root.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(['git', 'clone', '--local', '--no-hardlinks', '--branch', before['git']['branch'], str(ROOT), str(args.source_root)],
                   check=True, capture_output=True, env={**os.environ, 'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': os.devnull})
    tracked = {os.fsdecode(name) for name in verified_git(args.source_root, 'ls-files', '-z').split(b'\0') if name}
    names = {row['path'] for row in before['files']}
    for name in tracked - names:
        (args.source_root / name).unlink()
    for item in before['files']:
        body = release.raw(ROOT / item['path'], private=False)
        release.require(hashlib.sha256(body).hexdigest() == item['sha256'], 'source changed during freeze')
        target = args.source_root / item['path']; target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body); target.chmod(item['mode'])
    after = release.inventory(args.source_root)
    release.require(before == release.inventory(ROOT) and before['files'] == after['files'] and before['git'] == after['git'],
                    'frozen source differs from checked candidate')
    return write(args.source_tree, after)


def check(args):
    tree = release.object_at(release.reference(args.source_tree))
    release.require(tree == release.inventory(ROOT), 'run checks from the exact frozen source')
    parent = release.payload_at(release.reference(args.parent_build), 'sealed-build-receipt-v1')
    changes = release.source_changes(release.object_at(parent['account_cleanup_generation']['source_tree']), tree,
                                     schema_transition=parent.get('schema_contract', {}).get('formal_schema') != 24)
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    release.require(bool(command), 'check command required')
    result = subprocess.run(command, cwd=ROOT, capture_output=True,
                            env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'})
    output = write_bytes(args.output, result.stdout + result.stderr)
    release.require(release.inventory(ROOT) == tree, 'test command changed the final source')
    report = {'contract': release.CHECK_CONTRACT, 'name': args.name, 'status': 'passed' if result.returncode == 0 else 'failed',
              'exit_code': result.returncode, 'command': command, 'changes': changes,
              'source_tree': release.reference(args.source_tree), 'output': output, 'completed_at': now()}
    ref = write(args.report, report)
    release.require(result.returncode == 0, 'check failed; report retained at ' + str(args.report))
    return ref


def prepare(args):
    from install_duplicate_index_release import checks_at
    checked = release.verify_candidate_source(source=ROOT, parent_build_ref=release.reference(args.parent_build), checks=checks_at(args.check_report))
    parent, tree, tree_ref = checked['parent'], checked['source_tree'], checked['source_tree_ref']
    release.require(parent.get('schema_contract') == {'code_schema': 23, 'formal_schema': 23},
                    'prepare requires the schema23 origin; use code for an installed schema24 successor')
    migration_ref = release.reference(args.migration)
    migration = release.object_at(migration_ref)
    database = Path(migration['formal_database'])
    with release.readonly_database(database) as live:
        release.verify_migration(live, migration)
    old_body = release.raw(args.installed_plist, private=False); installed = plistlib.loads(old_body)
    env = installed.get('EnvironmentVariables', {})
    require_parent = (env.get('DCAR_WRITER_SOURCE_ROOT') == parent['source_root']
        and release.reference(Path(env.get('DCAR_LOADED_BUILD_RECEIPT', ''))) == release.reference(args.parent_build))
    # Preparing the fallback after activation is also allowed, but only from
    # the same schema24 migration/source/authority lineage.
    if not require_parent:
        current = release.payload_at(release.reference(Path(env['DCAR_LOADED_BUILD_RECEIPT'])), 'sealed-build-receipt-v1')
        require_parent = current.get(release.FIELD, {}).get('migration') == migration_ref and current['source_root'] == str(ROOT)
    release.require(require_parent and env.get('DCAR_V8_DB') == str(database)
                    and env.get('DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT') == str(args.parent_install), 'installed Writer changed')
    args.evidence_root.mkdir(mode=0o700, parents=True, exist_ok=False)
    at = now()
    source_plan = write(args.evidence_root / 'source-plan.json', {'contract': 'account-cleanup-source-plan-v1',
        'transition': 'account-cleanup-0907-v1', 'project_root': parent['project_root'], 'source_root': str(ROOT),
        'git': tree['git'], 'source_tree': tree_ref})
    plan = {'contract': release.CONTRACT, 'parent_build': release.reference(args.parent_build),
            'parent_install': release.reference(args.parent_install), 'source_tree': tree_ref,
            'changes': checked['changes'], 'checks': checked['checks'], 'migration': migration_ref,
            'engine': args.engine, 'issued_at': at}
    child = {**parent, 'source_root': str(ROOT), 'git': tree['git'],
             'critical_files': {row['path']: row['sha256'] for row in tree['files']
                               if row['path'].startswith(('src/', 'config/')) and row['path'].endswith(('.py', '.json'))},
             'code_successor_plan': source_plan,
             'account_cleanup_generation': {**parent['account_cleanup_generation'], 'source_tree': tree_ref},
             release.FIELD: plan, 'schema_contract': {'code_schema': 24, 'formal_schema': 24}, 'created_at': at,
             'validation_scope': 'fingerprint index, true graph and bounded writes; existing provider authority retained'}
    child_ref = write(args.evidence_root / 'build.json', {'contract_version': 'sealed-build-receipt-v1', 'payload': child, 'payload_sha256': release.digest(child)})
    release.verify_inheritance(build=child, build_ref=child_ref, install_path=args.parent_install, database=database, source=ROOT, at=at)
    next_plist = {**installed, 'EnvironmentVariables': {**env, 'DCAR_LOADED_BUILD_RECEIPT': child_ref['path'],
        'DCAR_WRITER_SOURCE_ROOT': str(ROOT), 'PYTHONPATH': os.pathsep.join(str(ROOT / p) for p in ('src/dcar_eval', 'scripts'))},
        'ProgramArguments': [str(ROOT / 'deploy/macos/run_writer_worker.sh')]}
    proposal = {'contract': 'duplicate-index-install-proposal-v1', 'status': 'prepared', 'formal_database': str(database),
                'installed_plist': str(args.installed_plist), 'before_plist': write_bytes(args.evidence_root / 'writer.before.plist', old_body),
                'next_plist': write_bytes(args.evidence_root / 'writer.next.plist', plistlib.dumps(next_plist, sort_keys=True)),
                'parent_install': release.reference(args.parent_install), 'child_build': child_ref, 'migration': migration_ref,
                'engine': args.engine, 'services_started': False, 'created_at': at}
    return write(args.evidence_root / 'install-proposal.json', proposal)


def code(args):
    """Prepare a checked code successor without touching live schema24 data."""
    from install_duplicate_index_release import checks_at
    parent_ref = release.reference(args.parent_build)
    checked = release.verify_candidate_source(source=ROOT, parent_build_ref=parent_ref, checks=checks_at(args.check_report))
    parent, tree, tree_ref = checked['parent'], checked['source_tree'], checked['source_tree_ref']
    release.require(parent.get('schema_contract') == {'code_schema': 24, 'formal_schema': 24}, 'code requires schema24')
    before = release.raw(args.installed_plist, private=False)
    installed = plistlib.loads(before); env = installed.get('EnvironmentVariables', {})
    database = Path(env['DCAR_V8_DB'])
    installed_ref = release.reference(Path(env['DCAR_LOADED_BUILD_RECEIPT']))
    accepted = installed_ref == parent_ref and env.get('DCAR_WRITER_SOURCE_ROOT') == parent['source_root']
    if not accepted:
        current = release.payload_at(installed_ref, 'sealed-build-receipt-v1')
        accepted = (current.get(release.CODE_FIELD, {}).get('parent_build') == parent_ref
                    and current.get('source_root') == str(ROOT)
                    and current.get(release.CODE_FIELD, {}).get('source_tree') == tree_ref)
    release.require(accepted and env.get('DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT') == str(args.parent_install)
                    and env.get('DCAR_PROJECT_ROOT') == parent['project_root'], 'installed code predecessor changed')
    _, inherited = release.duplicate_code_parent_context(parent_ref, install_path=args.parent_install, database=database, at=now())
    args.evidence_root.mkdir(mode=0o700, parents=True, exist_ok=False)
    at = now()
    source_plan = write(args.evidence_root / 'source-plan.json', {'contract': 'account-cleanup-source-plan-v1',
        'transition': 'account-cleanup-0907-v1', 'project_root': parent['project_root'], 'source_root': str(ROOT),
        'git': tree['git'], 'source_tree': tree_ref})
    plan = {'contract': release.CODE_CONTRACT, 'parent_build': parent_ref, 'source_tree': tree_ref,
        'changes': checked['changes'], 'checks': checked['checks'], 'engine': args.engine,
        'schema_migration_repeated': False, 'database_writes': 0, 'paid_gates_issued': 0,
        'parent_proof_sha256': release.digest(inherited), 'issued_at': at}
    child = {**parent, 'source_root': str(ROOT), 'git': tree['git'],
        'critical_files': {row['path']: row['sha256'] for row in tree['files']
                           if row['path'].startswith(('src/', 'config/')) and row['path'].endswith(('.py', '.json'))},
        'code_successor_plan': source_plan,
        'account_cleanup_generation': {**parent['account_cleanup_generation'], 'source_tree': tree_ref},
        release.CODE_FIELD: plan, 'created_at': at,
        'validation_scope': 'checked schema24 code successor; original migration and provider authority retained'}
    child_ref = write(args.evidence_root / 'build.json', {'contract_version': 'sealed-build-receipt-v1',
        'payload': child, 'payload_sha256': release.digest(child)})
    release.verify_inheritance(build=child, build_ref=child_ref, install_path=args.parent_install,
                               database=database, source=ROOT, at=at)
    next_plist = {**installed, 'EnvironmentVariables': {**env, 'DCAR_LOADED_BUILD_RECEIPT': child_ref['path'],
        'DCAR_WRITER_SOURCE_ROOT': str(ROOT), 'PYTHONPATH': os.pathsep.join(str(ROOT / p) for p in ('src/dcar_eval', 'scripts'))},
        'ProgramArguments': [str(ROOT / 'deploy/macos/run_writer_worker.sh')]}
    release.require(release.raw(args.installed_plist, private=False) == before, 'installed Writer changed during code preparation')
    proposal = {'contract': 'duplicate-index-install-proposal-v1', 'status': 'prepared',
        'formal_database': str(database), 'installed_plist': str(args.installed_plist),
        'before_plist': write_bytes(args.evidence_root / 'writer.before.plist', before),
        'next_plist': write_bytes(args.evidence_root / 'writer.next.plist', plistlib.dumps(next_plist, sort_keys=True)),
        'parent_install': release.reference(args.parent_install), 'child_build': child_ref,
        'migration': parent[release.FIELD]['migration'], 'engine': args.engine,
        'schema_migration_repeated': False, 'database_writes': 0, 'services_started': False, 'created_at': at}
    return write(args.evidence_root / 'install-proposal.json', proposal)


def main():
    parser = argparse.ArgumentParser(description=__doc__); commands = parser.add_subparsers(dest='action', required=True)
    p = commands.add_parser('candidate')
    for name in ('backup', 'candidate', 'output'): p.add_argument('--' + name, type=Path, required=True)
    p.add_argument('--resume', action='store_true')
    p = commands.add_parser('freeze')
    for name in ('parent-build', 'source-root', 'source-tree'): p.add_argument('--' + name, type=Path, required=True)
    p = commands.add_parser('check')
    for name in ('parent-build', 'source-tree', 'output', 'report'): p.add_argument('--' + name, type=Path, required=True)
    p.add_argument('--name', choices=sorted(release.CHECKS), required=True); p.add_argument('command', nargs=argparse.REMAINDER)
    p = commands.add_parser('prepare')
    for name in ('parent-build', 'parent-install', 'migration', 'installed-plist', 'evidence-root'): p.add_argument('--' + name, type=Path, required=True)
    p.add_argument('--engine', choices=('mih', 'full_scan'), default='mih'); p.add_argument('--check-report', action='append', default=[])
    p = commands.add_parser('code')
    for name in ('parent-build', 'parent-install', 'installed-plist', 'evidence-root'): p.add_argument('--' + name, type=Path, required=True)
    p.add_argument('--engine', choices=('mih', 'full_scan'), default='mih'); p.add_argument('--check-report', action='append', default=[])
    args = parser.parse_args()
    for value in vars(args).values():
        if isinstance(value, Path): release.require(value.is_absolute() and value.resolve() == value, 'absolute nonsymlink paths required')
    print(json.dumps(globals()[args.action](args), ensure_ascii=False))


if __name__ == '__main__':
    main()
