#!/usr/bin/env python3
"""Submit existing directory accounts to unified preparation, entirely offline.

Dry run is the default and simulates writes in an in-memory SQLite copy only.
Apply requires an explicit isolated schema22/23 candidate and a pre-write backup.
No provider request, scheduler, deployment or historical status change occurs.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import socket
import sqlite3
import sys

from import_account_summary import backup, readonly, stamp
from preview_account_summary import configure_read_only_paths
from verify_local_account_evidence import private_report


def directory_value(row):
    from v8.account_directory_reconciliation import directory_value as value
    return value(row)


def submit_directory(connection, *, at):
    from v8.account_directory_reconciliation import reconcile_directory
    return reconcile_directory(connection, at=at)


def snapshot(connection):
    # Identity IDs and manual state must not change just by submitting requests.
    return {table:[tuple(row) for row in connection.execute(sql)] for table,sql in {
        'accounts':'SELECT * FROM accounts ORDER BY id',
        'identities':'SELECT * FROM account_platform_identities ORDER BY id',
        'directory':'SELECT * FROM account_directory_rows ORDER BY id',
    }.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db',type=Path,required=True)
    parser.add_argument('--evidence-root',type=Path,required=True)
    parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('--backup',type=Path)
    parser.add_argument('--apply',action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    paths = [args.db,args.evidence_root,args.report]+([args.backup] if args.backup else [])
    if any(not path.is_absolute() or path.is_symlink() or path != path.resolve() for path in paths):
        parser.error('Use explicit absolute non-symlink paths')
    files = [args.db,args.report]+([args.backup] if args.backup else [])
    if any(a.resolve()==b.resolve() or a.exists() and b.exists() and a.samefile(b)
           for index,a in enumerate(files) for b in files[index+1:]):
        parser.error('Database, backup and report must be distinct files')
    if args.apply and not args.backup:
        parser.error('--apply requires --backup')
    code_root = Path(__file__).resolve().parents[1]
    configure_read_only_paths(args.db,code_root=code_root,evidence_root=args.evidence_root)
    def deny(*_args,**_kwargs): raise OSError('Directory preparation forbids network access')
    socket.socket.connect = socket.socket.connect_ex = socket.create_connection = socket.getaddrinfo = deny
    from v8.runtime_database import resolve_isolated_candidate
    from v8.schema_v22 import migrate
    access = resolve_isolated_candidate(args.db)
    at = stamp()
    with readonly(access.database) as original, sqlite3.connect(':memory:') as trial:
        original.row_factory = trial.row_factory = sqlite3.Row
        version = original.execute('PRAGMA user_version').fetchone()[0]
        if version not in {21,22,23} or args.apply and version not in {22,23}:
            parser.error('--apply requires a migrated schema22/23 candidate; dry-run accepts schema21/22/23')
        original.backup(trial)
        trial.execute('PRAGMA foreign_keys=ON');trial.execute('PRAGMA recursive_triggers=ON')
        if version == 21:
            migrate(trial)  # memory only, never upgrades the input database
        trial.execute('BEGIN IMMEDIATE')
        preserved = snapshot(trial)
        result = submit_directory(trial,at=at)
        if snapshot(trial) != preserved:
            raise ValueError('Preparation submission changed existing identities or manual state')
        trial.rollback()
    backup_receipt = None
    if args.apply and result['sql_writes']:
        backup_receipt = backup(access.database,args.backup)
        with sqlite3.connect(access.database) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute('PRAGMA foreign_keys=ON');connection.execute('PRAGMA recursive_triggers=ON')
            connection.execute('BEGIN IMMEDIATE')
            preserved = snapshot(connection)
            result = submit_directory(connection,at=at)
            if snapshot(connection) != preserved or connection.execute('PRAGMA foreign_key_check').fetchone():
                raise ValueError('Candidate preservation check failed')
    report = {'contract':'account-directory-preparation-v1','status':'PASS','mode':'apply' if args.apply else 'dry_run',
              'database':str(access.database),'source_schema_version':version,'backup':backup_receipt,**result,
              'database_writes':result['sql_writes'] if args.apply else 0,'network_requests':0,'scheduler_started':False,
              'completed_at':at}
    private_report(args.report,report,database=access.database)
    print(json.dumps({key:report[key] for key in ('status','mode','total','counts','database_writes','network_requests')},ensure_ascii=False))


if __name__=='__main__':
    main()
