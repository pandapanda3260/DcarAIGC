#!/usr/bin/env python3
"""Stopped-Writer prepare/import/activate/restore for the schema24 generation."""
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
import sys
import tempfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/dcar_eval'))
sys.path.insert(0, str(ROOT / 'scripts'))
from v8 import duplicate_index_release as release, schema_v23, schema_v24
from v8.duplicate_index import validate_postings
from v8.runtime_database import hold_formal_mutation
from v8.storage import configure_connection_safety
from v8.schema_v20 import row_digest
from v8.schema_v22 import _table_digests
from prepare_account_intake_release import write, write_bytes


def now():
    return datetime.now(timezone.utc).isoformat()


def checks_at(values):
    result = {}
    for value in values:
        name, separator, path = value.partition('=')
        release.require(separator and name not in result, 'checks must be unique name=absolute_path')
        result[name] = release.reference(Path(path))
    return result


def verify_maintenance_quiescence(connection):
    """Drain live execution; retain historical unresolved billing verbatim."""
    active = connection.execute("SELECT r.id,r.job_id,r.status,a.id,a.status FROM scheduler_runs r "
        "LEFT JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id WHERE r.status='running' OR a.status='running' "
        "UNION ALL SELECT a.scheduler_run_id,NULL,NULL,a.id,a.status FROM scheduler_run_attempts a "
        "LEFT JOIN scheduler_runs r ON r.id=a.scheduler_run_id WHERE r.id IS NULL AND a.status='running'").fetchall()
    release.require(not active, 'scheduler runs or attempts are still running; wait for Writer drain')
    states, historical_unknown, unlinked_unknown = {}, [], []
    for row in connection.execute('SELECT id,provider,details_json FROM provider_usage'):
        detail = json.loads(row['details_json'] or '{}')
        release.require(isinstance(detail, dict), 'provider usage details are malformed')
        state = detail.get('state'); states[str(state)] = states.get(str(state), 0) + 1
        release.require(state not in ('reserved', 'sent'), 'paid requests are still in flight')
        if state not in ('billing_unknown', 'charged_unverified'):
            continue
        scope = detail.get('scope')
        run_id = scope.get('scheduler_run_id') if isinstance(scope, dict) else None
        attempt_id = scope.get('scheduler_attempt_id') if isinstance(scope, dict) else None
        linked = None
        if type(run_id) is int and run_id > 0 and type(attempt_id) is int and attempt_id > 0:
            linked = connection.execute('SELECT r.status,a.status FROM scheduler_runs r JOIN scheduler_run_attempts a '
                'ON a.scheduler_run_id=r.id WHERE r.id=? AND a.id=?', (run_id, attempt_id)).fetchone()
        (historical_unknown if linked and all(value != 'running' for value in linked) else unlinked_unknown).append(row['id'])
    return {'live_runs_and_attempts': 0, 'inflight_reservations': 0, 'usage_states': states,
            'retained_historical_unknown_ids': historical_unknown, 'retained_unlinked_unknown_ids': unlinked_unknown,
            'billing_reconciled': False, 'provider_usage_changes': 0}


def connection_at(path, *, read_only=False):
    connection = sqlite3.connect(path.as_uri() + ('?mode=ro' if read_only else '?mode=rw'), uri=True)
    connection.row_factory = sqlite3.Row
    configure_connection_safety(connection)
    if read_only: connection.execute('PRAGMA query_only=ON')
    return connection


def identity(path):
    value = path.stat()
    return {'device': value.st_dev, 'inode': value.st_ino, 'mode': value.st_mode}


def require_identity(path, expected):
    release.require(identity(path) == expected, 'formal database path/inode/permissions changed')


def assert_retained(connection, expected, *, allow_projection=False):
    for table, value in expected.items():
        if table == 'schema_migrations' or (allow_projection and table == 'duplicate_relations'):
            continue
        release.require(row_digest(connection, table, value['columns']) == value, 'business data advanced since freeze: ' + table)


def assert_relation_facts(connection, original, *, allowed_text_deletions=()):
    """Only the derived fingerprint projection and proven old text can differ."""
    before = {row['id']: dict(row) for row in original.execute("SELECT * FROM duplicate_relations WHERE method!='fingerprint_v1'")}
    after = {row['id']: dict(row) for row in connection.execute("SELECT * FROM duplicate_relations WHERE method!='fingerprint_v1'")}
    allowed = set(allowed_text_deletions)
    release.require(all(rid in before and before[rid]['method'] == 'text_sha256' for rid in allowed),
                    'unproved non-fingerprint deletion')
    release.require(set(after) <= set(before) and all(before[rid] == row for rid, row in after.items())
                    and set(before) - set(after) <= allowed,
                    'business data advanced since freeze: non-fingerprint duplicate relations')


def installation_receipt(preflight, proof, candidate_receipt, posting_proof):
    value = {key: preflight[key] for key in ('formal_database', 'authority_build', 'authority_install', 'source_tree', 'changes', 'checks',
        'backup', 'parent_migration_proof', 'parent_inheritance_sha256')}
    value.update(contract=release.INSTALL_CONTRACT, status='migrated', from_schema=23, to_schema=24,
        database_identity={key: preflight['database_identity'][key] for key in ('device', 'inode')},
        migration_proof=proof, preserved_tables_verified=True, paid_gates_issued=0, migrated_at=proof['applied_at'],
        candidate_receipt=release.reference(candidate_receipt), posting_verification=posting_proof)
    value['receipt_sha256'] = release.digest(value)
    return value


def prepare(args):
    checked = release.verify_candidate_source(source=ROOT, parent_build_ref=release.reference(args.parent_build), checks=checks_at(args.check_report))
    release.require(checked['parent'].get('schema_contract') == {'code_schema': 23, 'formal_schema': 23},
                    'maintenance migration requires its schema23 predecessor')
    release.require(not args.output_dir.exists(), 'new final maintenance evidence directory required')
    args.output_dir.mkdir(mode=0o700, parents=True)
    capacity = release.require_capacity(args.output_dir, required_bytes=args.database.stat().st_size * 3)
    backup = args.output_dir / 'before-schema23.sqlite3'
    with hold_formal_mutation(args.database, project_root=args.project_root):
        parent = checked['parent']
        installed_body = release.raw(args.installed_plist, private=False)
        installed = plistlib.loads(installed_body); env = installed.get('EnvironmentVariables', {})
        release.require(installed.get('WorkingDirectory') == str(args.project_root)
            and env.get('DCAR_V8_DB') == str(args.database) and env.get('DCAR_PROJECT_ROOT') == str(args.project_root)
            and env.get('DCAR_WRITER_SOURCE_ROOT') == parent['source_root']
            and release.reference(Path(env.get('DCAR_LOADED_BUILD_RECEIPT', ''))) == release.reference(args.parent_build)
            and env.get('DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT') == str(args.parent_install), 'Writer predecessor changed')
        before = identity(args.database)
        with closing(connection_at(args.database, read_only=True)) as original:
            proof = schema_v23.migration_proof(original)
            quiescence = verify_maintenance_quiescence(original)
            os.close(os.open(backup, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
            with closing(sqlite3.connect(backup)) as target:
                original.backup(target)
                target.execute('PRAGMA journal_mode=DELETE')
        backup_ref = release.file_reference(backup)
        _, inherited = release.parent_context(release.reference(args.parent_build), install_path=args.parent_install,
                                              database=args.database, backup_ref=backup_ref, at=now())
        with closing(connection_at(backup, read_only=True)) as original:
            release.require(schema_v23.migration_proof(original) == proof, 'frozen schema23 proof differs')
            retained = _table_digests(original)
        require_identity(args.database, before)
        value = {'contract': 'duplicate-index-maintenance-preflight-v1', 'formal_database': str(args.database),
            'project_root': str(args.project_root), 'installed_plist': str(args.installed_plist), 'database_identity': before,
            'authority_build': release.reference(args.parent_build), 'authority_install': release.reference(args.parent_install),
            'source_tree': checked['source_tree_ref'], 'changes': checked['changes'], 'checks': checked['checks'],
            'backup': backup_ref, 'before_plist': write_bytes(args.output_dir / 'writer.before.plist', installed_body),
            'parent_migration_proof': proof, 'parent_inheritance_sha256': release.digest(inherited),
            'retained_tables': retained, 'quiescence': quiescence, 'capacity': capacity, 'prepared_at': now(), 'traffic_resumed': False}
        return write(args.output_dir / 'preflight.json', value)


def import_candidate(args):
    preflight = release.object_at(release.reference(args.preflight))
    built = release.object_at(release.reference(args.candidate_receipt))
    release.require(preflight.get('contract') == 'duplicate-index-maintenance-preflight-v1'
        and built.get('contract') == 'duplicate-index-candidate-v1' and built.get('status') == 'built'
        and built.get('backup') == preflight['backup'] and preflight.get('traffic_resumed') is False,
        'candidate is not built from the final maintenance backup')
    candidate = release.verified_backup(built['candidate']); database = Path(preflight['formal_database'])
    release.require_capacity(database.parent, required_bytes=database.stat().st_size * 2)
    checked = release.verify_candidate_source(source=ROOT, parent_build_ref=preflight['authority_build'], checks=preflight['checks'])
    release.require(checked['source_tree_ref'] == preflight['source_tree'], 'final checked source changed')
    with hold_formal_mutation(database, project_root=Path(preflight['project_root'])) as access:
        require_identity(database, preflight['database_identity'])
        release.require(release.raw(Path(preflight['installed_plist']), private=False)
                        == release.raw(Path(preflight['before_plist']['path'])), 'Writer installation changed before import')
        backup = release.verified_backup(preflight['backup'])
        with closing(connection_at(database)) as live, closing(connection_at(candidate, read_only=True)) as source, \
                closing(connection_at(backup, read_only=True)) as original:
            assert_retained(live, preflight['retained_tables'], allow_projection=live.execute('PRAGMA user_version').fetchone()[0] == 24)
            assert_relation_facts(live, original, allowed_text_deletions=built['proof'].get('removed_legacy_text_relation_ids', []))
            if live.execute('PRAGMA user_version').fetchone()[0] == 23:
                schema_v24.migrate(live, maintenance=schema_v24.MaintenanceContext(access, backup, preflight['backup']['sha256']))
            schema_v24.validate_structure(live)
            generation = source.execute("SELECT * FROM duplicate_index_generations WHERE state='ready'").fetchone()
            release.require(generation is not None, 'candidate generation is not complete')
            gid = generation['generation_id']
            installed_generation = live.execute('SELECT state FROM duplicate_index_generations WHERE generation_id=?', (gid,)).fetchone()
            if installed_generation and installed_generation[0] == 'ready':
                # Crash after activation but before receipt creation: prove the
                # entire imported image, then recover the receipt without writes.
                for table in (*schema_v24.RUNTIME_TABLES, 'duplicate_relations'):
                    columns = [row[1] for row in live.execute('PRAGMA table_info(' + table + ')')]
                    release.require(row_digest(live, table, columns) == row_digest(source, table, columns),
                                    'active import differs from final candidate: ' + table)
                posting = validate_postings(live, generation_id=gid)
                require_identity(database, preflight['database_identity'])
                return write(args.output, installation_receipt(preflight, schema_v24.migration_proof(live), args.candidate_receipt, posting))
            # A resumed import replaces only this derived building generation.
            # All original business rows remain protected by assert_retained.
            with live:
                old = live.execute('SELECT state FROM duplicate_index_generations WHERE generation_id=?', (gid,)).fetchone()
                release.require(old is None or old[0] == 'building', 'already active generation cannot be reimported')
                live.execute('DELETE FROM duplicate_index_generations WHERE generation_id=?', (gid,))
                columns = list(generation.keys()); values = dict(generation); values['state'] = 'building'
                live.execute('INSERT INTO duplicate_index_generations(' + ','.join(columns) + ') VALUES(' + ','.join('?' for _ in columns) + ')',
                             tuple(values[name] for name in columns))
            for table in schema_v24.RUNTIME_TABLES[1:]:
                cursor = source.execute('SELECT * FROM ' + table + ' WHERE generation_id=?', (gid,))
                columns = [row[0] for row in cursor.description]
                sql = 'INSERT INTO ' + table + '(' + ','.join(columns) + ') VALUES(' + ','.join('?' for _ in columns) + ')'
                while rows := cursor.fetchmany(1000):
                    with live: live.executemany(sql, [tuple(row) for row in rows])
                release.require(row_digest(live, table, columns) == row_digest(source, table, columns), 'imported derived rows differ: ' + table)
            with live:
                # Copy only fingerprint_v1 projection, keeping identity/manual
                # and legacy text rows byte-for-byte as the candidate does.
                live.execute("DELETE FROM duplicate_relations WHERE method='fingerprint_v1'")
                rows = source.execute("SELECT * FROM duplicate_relations WHERE method='fingerprint_v1'").fetchall()
                if rows:
                    columns = rows[0].keys()
                    live.executemany('INSERT INTO duplicate_relations(' + ','.join(columns) + ') VALUES(' + ','.join('?' for _ in columns) + ')',
                                     [tuple(row) for row in rows])
                for relation_id in built['proof'].get('removed_legacy_text_relation_ids', []):
                    live.execute("DELETE FROM duplicate_relations WHERE id=? AND method='text_sha256'", (relation_id,))
            relation_columns = [row[1] for row in live.execute('PRAGMA table_info(duplicate_relations)')]
            release.require(row_digest(live, 'duplicate_relations', relation_columns)
                            == row_digest(source, 'duplicate_relations', relation_columns), 'imported relation projection differs')
            posting_proof = validate_postings(live, generation_id=gid)
            release.require(live.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
                            and live.execute('PRAGMA foreign_key_check').fetchone() is None, 'formal import integrity failed')
            assert_retained(live, preflight['retained_tables'], allow_projection=True)
            assert_relation_facts(live, original, allowed_text_deletions=built['proof'].get('removed_legacy_text_relation_ids', []))
            with live:
                live.execute("UPDATE duplicate_index_generations SET state='ready' WHERE generation_id=?", (gid,))
            live.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            proof = schema_v24.migration_proof(live)
        require_identity(database, preflight['database_identity'])
        return write(args.output, installation_receipt(preflight, proof, args.candidate_receipt, posting_proof))


def atomic_file(path, body):
    descriptor, name = tempfile.mkstemp(prefix='.duplicate-index-', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'wb') as handle:
            handle.write(body); handle.flush(); os.fsync(handle.fileno())
        os.chmod(name, 0o600); os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        if os.path.exists(name): os.unlink(name)


def activate(args):
    proposal = release.object_at(release.reference(args.proposal))
    release.require(proposal.get('contract') == 'duplicate-index-install-proposal-v1' and proposal.get('status') == 'prepared', 'checked proposal required')
    database = Path(proposal['formal_database']); child_ref = proposal['child_build']
    child = release.payload_at(child_ref, 'sealed-build-receipt-v1'); plist = Path(proposal['installed_plist'])
    release.require(child['source_root'] == str(ROOT), 'activate from the frozen source')
    with hold_formal_mutation(database, project_root=Path(child['project_root'])):
        current = release.raw(plist, private=False)
        release.require(current in (release.raw(Path(proposal['before_plist']['path'])),
                                    release.raw(Path(proposal['next_plist']['path']))), 'installed Writer changed after proposal')
        release.verify_inheritance(build=child, build_ref=child_ref, install_path=Path(proposal['parent_install']['path']), database=database, source=ROOT, at=now())
        body = release.raw(Path(proposal['next_plist']['path']))
        release.require(release.reference(Path(proposal['next_plist']['path'])) == proposal['next_plist'], 'proposed plist changed')
        from v8.runtime_paths import verify_source_before_import
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp).resolve(); target = home / 'Library/LaunchAgents/cn.tj.dcar.writer-worker.plist'
            target.parent.mkdir(parents=True); target.write_bytes(body); target.chmod(0o600)
            bootstrap = verify_source_before_import(data=Path(child['project_root']), source=ROOT, build_receipt=Path(child_ref['path']), home=home)
        if current != body: atomic_file(plist, body)
    return write(args.output, {'contract': 'duplicate-index-activation-v1', 'status': 'installed_stopped',
        'child_build': child_ref, 'bootstrap': bootstrap, 'installed_plist_sha256': hashlib.sha256(body).hexdigest(),
        'services_started': False, 'provider_calls': 0, 'completed_at': now()})


def restore_database_before_traffic(connection, original, *, expected_identity, database, allowed_text_deletions=()):
    """Restore through SQLite into the same inode; caller owns maintenance lease."""
    require_identity(database, expected_identity)
    version = connection.execute('PRAGMA user_version').fetchone()[0]
    release.require(version in (23, 24), 'unexpected rollback schema')
    if version == 24:
        release.require(connection.execute("SELECT 1 FROM duplicate_work_staging WHERE work_content_id=0 "
            "AND record_type='runtime_traffic_started' LIMIT 1").fetchone() is None,
            'Writer traffic already started; use schema24 fallback and retain current data')
    expected = _table_digests(original)
    assert_retained(connection, expected, allow_projection=True)
    assert_relation_facts(connection, original, allowed_text_deletions=allowed_text_deletions)
    if connection.in_transaction: raise ValueError('rollback requires idle connection')
    connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    original.backup(connection)
    connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    configure_connection_safety(connection)
    schema_v23.validate_structure(connection)
    release.require(connection.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
                    and _table_digests(connection) == expected, 'restored schema23 data differs')
    require_identity(database, expected_identity)
    return {'user_version': 23, 'integrity_check': 'ok', 'same_inode': True, 'all_original_tables_verified': True}


def restore(args):
    preflight = release.object_at(release.reference(args.preflight))
    release.require(preflight.get('contract') == 'duplicate-index-maintenance-preflight-v1'
                    and preflight.get('traffic_resumed') is False, 'original before-traffic preflight required')
    database = Path(preflight['formal_database']); backup = release.verified_backup(preflight['backup'])
    built = release.object_at(release.reference(args.candidate_receipt))
    release.require(built.get('contract') == 'duplicate-index-candidate-v1' and built.get('status') == 'built'
                    and built['backup'] == preflight['backup'], 'rollback candidate differs from maintenance backup')
    release.verified_backup(built['candidate'])
    with hold_formal_mutation(database, project_root=Path(preflight['project_root'])):
        # Validate both sides of the code/database pair before changing either.
        body = release.raw(Path(preflight['before_plist']['path']))
        release.require(release.reference(Path(preflight['before_plist']['path'])) == preflight['before_plist'], 'original plist changed')
        old_env = plistlib.loads(body).get('EnvironmentVariables', {})
        release.require(old_env.get('DCAR_V8_DB') == str(database)
                        and release.reference(Path(old_env['DCAR_LOADED_BUILD_RECEIPT'])) == preflight['authority_build'],
                        'original plist does not select the frozen schema23 predecessor')
        installed = Path(preflight['installed_plist']); current = release.raw(installed, private=False)
        if current != body:
            env = plistlib.loads(current).get('EnvironmentVariables', {})
            child_ref = release.reference(Path(env['DCAR_LOADED_BUILD_RECEIPT']))
            child = release.payload_at(child_ref, 'sealed-build-receipt-v1')
            plan = child.get(release.FIELD, {})
            release.require(plan.get('parent_build') == preflight['authority_build']
                            and release.object_at(plan['migration'])['backup'] == preflight['backup'],
                            'installed Writer is outside this maintenance lineage')
            release.verify_inheritance(build=child, build_ref=child_ref, install_path=Path(preflight['authority_install']['path']),
                                       database=database, source=ROOT, at=now())
        # Prepare and fsync replacement before SQLite restore. No service can
        # start while the maintenance lease is held; failures retain both bytes.
        prepared = installed.parent / ('.duplicate-index-restore-' + preflight['backup']['sha256'][:16] + '.plist')
        atomic_file(prepared, body)
        with closing(connection_at(database)) as live, closing(connection_at(backup, read_only=True)) as original:
            proof = restore_database_before_traffic(live, original, expected_identity=preflight['database_identity'], database=database,
                allowed_text_deletions=built['proof'].get('removed_legacy_text_relation_ids', []))
        try:
            os.replace(prepared, installed)
        except OSError as error:
            write(args.output, {'contract': 'duplicate-index-before-traffic-restore-v1',
                'status': 'database_restored_source_pending', **proof, 'services_started': False,
                'prepared_plist': release.reference(prepared), 'installed_plist': str(installed),
                'error': str(error), 'completed_at': now()})
            raise RuntimeError('schema23 data restored; Writer remains stopped; install the verified prepared plist') from error
        directory = os.open(installed.parent, os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
    return write(args.output, {'contract': 'duplicate-index-before-traffic-restore-v1', 'status': 'restored_stopped',
                              **proof, 'services_started': False, 'completed_at': now()})


def main():
    parser = argparse.ArgumentParser(description=__doc__); commands = parser.add_subparsers(dest='action', required=True)
    p = commands.add_parser('prepare')
    for name in ('database', 'project-root', 'installed-plist', 'parent-build', 'parent-install', 'output-dir'): p.add_argument('--' + name, type=Path, required=True)
    p.add_argument('--check-report', action='append', default=[])
    p = commands.add_parser('import')
    for name in ('preflight', 'candidate-receipt', 'output'): p.add_argument('--' + name, type=Path, required=True)
    p = commands.add_parser('activate')
    for name in ('proposal', 'output'): p.add_argument('--' + name, type=Path, required=True)
    p = commands.add_parser('restore-before-traffic')
    for name in ('preflight', 'candidate-receipt', 'output'): p.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    for value in vars(args).values():
        if isinstance(value, Path): release.require(value.is_absolute() and value.resolve() == value, 'absolute nonsymlink paths required')
    result = {'prepare': prepare, 'import': import_candidate, 'activate': activate, 'restore-before-traffic': restore}[args.action](args)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__': main()
