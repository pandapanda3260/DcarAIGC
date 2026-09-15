#!/usr/bin/env python3
"""Freeze and check schema22 source, then prepare a Writer installation proposal.

freeze/check touch only new local source/evidence files. prepare reads the real
separately recorded migration and emits a new build/plist; it never installs,
starts services, sends provider requests, or mutates a database.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
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
sys.path.insert(0, str(ROOT / 'src/dcar_eval'))
from v8 import account_intake_release as release
from v8.account_catalog_capture_release import ACCOUNT_CATALOG_POLICY_V3
from v8.runtime_paths import verified_git


def write_bytes(path: Path, body: bytes) -> dict:
    release.require(path.is_absolute() and path.resolve()==path,'evidence path must be absolute without symlinks')
    descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(descriptor,'wb') as stream:
        stream.write(body);stream.flush();os.fsync(stream.fileno())
    return release.reference(path)


def write(path: Path, value: dict) -> dict:
    return write_bytes(path,(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode())


def checks_at(arguments: list[str]) -> dict:
    result={}
    for argument in arguments:
        name,separator,path=argument.partition('=')
        release.require(bool(separator) and name not in result,'checks must be unique name=absolute_path')
        result[name]=release.reference(Path(path))
    return result


def freeze(args) -> dict:
    before=release.inventory(ROOT)
    parent=release.payload_at(release.reference(args.parent_build),'sealed-build-receipt-v1')
    release.require(parent.get('schema_contract')=={'code_schema':21,'formal_schema':21},'parent must be schema21')
    release.source_changes(release.object_at(parent['account_cleanup_generation']['source_tree']),before)
    data=Path(parent['project_root'])
    release.require(not args.source_root.exists() and args.source_root.resolve()==args.source_root
                    and not args.source_root.is_relative_to(data) and not data.is_relative_to(args.source_root)
                    and not args.source_root.is_relative_to(ROOT),'new independent source directory required')
    release.require(not args.source_tree.is_relative_to(args.source_root) and not args.source_tree.is_relative_to(ROOT),
                    'source manifest must be outside source trees')
    args.source_root.parent.mkdir(parents=True,exist_ok=True)
    subprocess.run(['git','clone','--local','--no-hardlinks','--branch',before['git']['branch'],str(ROOT),str(args.source_root)],
        check=True,capture_output=True,env={**os.environ,'GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':os.devnull})
    tracked={os.fsdecode(value) for value in verified_git(args.source_root,'ls-files','-z').split(b'\0') if value}
    live={row['path'] for row in before['files']}
    for name in tracked-live:
        (args.source_root/name).unlink()
    for item in before['files']:
        body=release.raw(ROOT/item['path'],private=False)
        release.require(hashlib.sha256(body).hexdigest()==item['sha256'],'source changed during freeze')
        target=args.source_root/item['path'];target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(body);target.chmod(item['mode'])
    after=release.inventory(args.source_root)
    release.require(before==release.inventory(ROOT) and before['files']==after['files'] and before['git']==after['git'],
                    'frozen source differs from reviewed source')
    return {'status':'frozen','source_root':str(args.source_root),'source_tree':write(args.source_tree,after),'services_changed':False}


def check(args) -> dict:
    before=release.inventory(ROOT)
    tree_ref=release.reference(args.source_tree)
    release.require(release.object_at(tree_ref)==before,'run checks from the unchanged frozen source')
    parent=release.payload_at(release.reference(args.parent_build),'sealed-build-receipt-v1')
    changes=release.source_changes(release.object_at(parent['account_cleanup_generation']['source_tree']),before)
    command=args.command[1:] if args.command and args.command[0]=='--' else args.command
    release.require(args.name in {'intake_schema','intake_execution'} and bool(command),'required check and command required')
    release.require(not args.output.is_relative_to(ROOT),'check evidence must be external to source')
    environment=dict(os.environ)
    for name in list(environment):
        if name.startswith('DCAR_'):
            environment.pop(name)
    environment.update(PYTHONDONTWRITEBYTECODE='1',PYTHONPATH=os.pathsep.join(str(ROOT/name) for name in ('src/dcar_eval','tests','scripts')),
                       DCAR_SCHEDULER_ENABLED='0', DCAR_STARTUP_CATCHUP_ENABLED='0')
    result=subprocess.run(command,cwd=ROOT,env=environment,capture_output=True)
    output=write_bytes(args.output.with_suffix('.log'),result.stdout+result.stderr)
    release.require(release.inventory(ROOT)==before,'source changed during check; no passing receipt issued')
    report={'contract':release.CHECK_CONTRACT,'name':args.name,'source_tree':tree_ref,'changes':changes,
        'status':'passed' if result.returncode==0 else 'failed','exit_code':result.returncode,
        'command':command,'output':output,'completed_at':datetime.now(timezone.utc).isoformat()}
    reference=write(args.output,report)
    release.require(result.returncode==0,'check failed; inspect '+str(args.output.with_suffix('.log')))
    return {'report':reference,**report}


def publisher_proposal(path: Path, *, evidence_root: Path, source: Path, parent: dict, database: Path) -> dict:
    """Prepare paired publisher source/schema files; never replace installed files."""
    data=Path(parent['project_root']);body=release.raw(path,private=False);payload=plistlib.loads(body)
    env=payload.get('EnvironmentVariables',{})
    release.require(payload.get('Label')=='cn.tj.dcar.snapshot-publisher'
        and payload.get('WorkingDirectory')==str(data)
        and payload.get('ProgramArguments')==[str(Path(parent['source_root'])/'deploy/macos/run_snapshot_publisher.sh')]
        and env.get('DCAR_WRITER_SOURCE_ROOT')==parent['source_root'] and env.get('DCAR_PROJECT_ROOT')==str(data)
        and env.get('DCAR_V8_DB')==str(database) and env.get('DCAR_READ_ONLY')=='1'
        and env.get('DCAR_SCHEDULER_ENABLED')=='0' and env.get('DCAR_STARTUP_CATCHUP_ENABLED')=='0'
        and not any(env.get(key) for key in ('TIKHUB_API_KEY','TIKHUB_API_KEY_FILE')),'publisher does not match read-only parent')
    environment_path=Path(env.get('DCAR_PUBLISHER_ENV_FILE',''))
    original=release.raw(environment_path,private=False)
    spec=importlib.util.spec_from_file_location('_checked_intake_publisher',source/'deploy/macos/publish_snapshot.py')
    release.require(spec is not None and spec.loader is not None,'publisher source missing')
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module
    try:
        spec.loader.exec_module(module)
        config=module._read_external_env(environment_path,project_root=data)
        release.require(config.expected_user_version==21,'publisher environment must match parent schema21')
        lines=original.decode().splitlines(keepends=True)
        for index,line in enumerate(lines):
            if line.strip().partition('=')[0]=='DCAR_PUBLISH_EXPECTED_USER_VERSION':
                lines[index]='DCAR_PUBLISH_EXPECTED_USER_VERSION=22'+('\n' if line.endswith('\n') else '')
        next_environment=write_bytes(evidence_root/'publisher.next.env',''.join(lines).encode())
        release.require(module._read_external_env(Path(next_environment['path']),project_root=data).expected_user_version==22,
                        'publisher next environment did not pin schema22')
    finally:
        sys.modules.pop(spec.name,None)
    proposal={**payload,'EnvironmentVariables':{**env,'DCAR_WRITER_SOURCE_ROOT':str(source)},
        'ProgramArguments':[str(source/'deploy/macos/run_snapshot_publisher.sh')]}
    return {'status':'prepared','installed_plist':str(path),'installed_environment':str(environment_path),
        'before_plist':write_bytes(evidence_root/'publisher.before.plist',body),
        'next_plist':write_bytes(evidence_root/'publisher.next.plist',plistlib.dumps(proposal,sort_keys=True)),
        'before_environment':write_bytes(evidence_root/'publisher.before.env',original),'next_environment':next_environment,
        'services_changed':False,'remote_pairing':'explicit_schema21_to22_upgrade_required'}


def prepare(args) -> dict:
    local_capture = bool(getattr(args, 'approve_local_capture', False))
    release.require(args.approve_production_rollout or local_capture,'explicit local activation or future rollout approval flag required')
    build_ref=release.reference(args.parent_build);install_ref=release.reference(args.parent_install)
    checked=release.verify_candidate_source(source=ROOT,parent_build_ref=build_ref,checks=checks_at(args.check_report))
    parent=checked['parent'];data=Path(parent['project_root'])
    install=release.object_at(install_ref);database=Path(install['formal_database'])
    installed=release.validate_installed_writer(installed_plist=args.installed_plist,parent_build_ref=build_ref,
        parent_install_ref=install_ref,database=database,project_root=data)
    migration_ref=release.reference(args.migration);migration=release.object_at(migration_ref)
    release.require(migration.get('previous_writer_plist_sha256')==hashlib.sha256(installed['bytes']).hexdigest()
        and migration.get('previous_loaded_build')==build_ref,'installed Writer changed since migration')
    release.require(not args.evidence_root.exists() and args.evidence_root.resolve()==args.evidence_root
        and not args.evidence_root.is_relative_to(ROOT) and not args.evidence_root.is_relative_to(data),
        'new external evidence directory required')
    args.evidence_root.mkdir(mode=0o700,parents=True)
    at=datetime.now(timezone.utc).isoformat();tree=checked['source_tree'];tree_ref=checked['source_tree_ref']
    source_plan=write(args.evidence_root/'source-plan.json',{'contract':'account-cleanup-source-plan-v1',
        'transition':'account-cleanup-0907-v1','project_root':str(data),'source_root':str(ROOT),'git':tree['git'],'source_tree':tree_ref})
    successor={'contract':release.CONTRACT,'parent_build':build_ref,'parent_install':install_ref,'source_tree':tree_ref,
        'changes':checked['changes'],'checks':checked['checks'],'migration':migration_ref,
        'issued_at':at,'actor':args.actor,'reason':args.reason,'production_rollout':'approved_by_user',
        'legacy_execution_controls':'inherited_unchanged','provider_qualification':'required_per_operation',
        'account_catalog_policy':ACCOUNT_CATALOG_POLICY_V3,'account_catalog_policy_sha256':release.digest(ACCOUNT_CATALOG_POLICY_V3)}
    if local_capture:
        successor.update(production_rollout='not_authorized', local_activation='approved_by_user')
        from v8.account_preparation_authority import AUTHORIZATION_CONTRACT, OPERATIONS, PLATFORMS, STATUSES, ACTIVE_KEYS
        from v8.profile_activations import activation_at
        instruction_path = getattr(args, 'user_instruction_file', None)
        thread_id = getattr(args, 'source_thread_id', None)
        release.require(isinstance(instruction_path, Path) and instruction_path.is_absolute()
            and instruction_path.resolve() == instruction_path and not instruction_path.is_symlink()
            and isinstance(thread_id, str) and thread_id.strip(), 'real user instruction file and source thread are required')
        instruction = release.raw(instruction_path, private=True).decode('utf-8').strip()
        release.require(bool(instruction), 'user instruction is empty')
        with sqlite3.connect(database.as_uri()+'?mode=ro', uri=True) as connection:
            connection.row_factory = sqlite3.Row
            active = activation_at(connection, at)
        release.require(active is not None, 'local capture requires an existing activation')
        authority = {'contract':AUTHORIZATION_CONTRACT,'scope':'local_writer_capture_only','schema_version':22,
            'operations':sorted(OPERATIONS),'platforms':list(PLATFORMS),'manual_statuses':list(STATUSES),
            'qualification':'operator_authorized','business_e2e':'required','transport_qualification':'not_verified',
            'publisher_authorized':False,'remote_database_authorized':False,
            'parent_build':build_ref,'source_tree':tree_ref,'migration':migration_ref,
            'formal_database':{'path':str(database),**migration['database_identity']},
            'catalog_policy_sha256':release.digest(ACCOUNT_CATALOG_POLICY_V3),
            'activation':{key:active[key] for key in ACTIVE_KEYS},
            'actor':args.actor,'reason':args.reason,'user_instruction':instruction,'source_thread_id':thread_id,'issued_at':at}
        successor['operation_authorization'] = write(args.evidence_root/'local-capture-authorization.json', authority)
    build={**parent,'source_root':str(ROOT),'git':tree['git'],
        'critical_files':{row['path']:row['sha256'] for row in tree['files']
            if row['path'].startswith(('src/','config/')) and row['path'].endswith(('.py','.json'))},
        'code_successor_plan':source_plan,'account_cleanup_generation':{**parent['account_cleanup_generation'],'source_tree':tree_ref},
        'account_intake_successor':successor,'schema_contract':{'code_schema':22,'formal_schema':22},'created_at':at,
        'validation_scope':'schema22 unified intake; inherited execution controls; provider qualification remains per operation'}
    child_ref=write(args.evidence_root/'build.json',{'contract_version':'sealed-build-receipt-v1','payload':build,
                                                  'payload_sha256':release.digest(build)})
    release.verify_inheritance(build=build,build_ref=child_ref,install_path=args.parent_install,database=database,source=ROOT,at=at)
    proposal={**installed['payload'],'EnvironmentVariables':dict(installed['payload']['EnvironmentVariables'])}
    proposal['EnvironmentVariables'].update(DCAR_LOADED_BUILD_RECEIPT=child_ref['path'],DCAR_WRITER_SOURCE_ROOT=str(ROOT),
        PYTHONPATH=str(ROOT/'src/dcar_eval')+os.pathsep+str(ROOT/'scripts'))
    proposal['ProgramArguments']=[str(ROOT/'deploy/macos/run_writer_worker.sh')]
    before_ref=write_bytes(args.evidence_root/'writer.before.plist',installed['bytes'])
    next_ref=write_bytes(args.evidence_root/'writer.next.plist',plistlib.dumps(proposal,sort_keys=True))
    # Exercise the same stdlib bootstrap used by Writer without changing the
    # installed plist: the temporary home contains only the proposed launch file.
    from v8.runtime_paths import verify_source_before_import
    with tempfile.TemporaryDirectory() as temporary:
        home=Path(temporary).resolve();target=home/'Library/LaunchAgents/cn.tj.dcar.writer-worker.plist'
        target.parent.mkdir(parents=True);target.write_bytes(plistlib.dumps(proposal));target.chmod(0o600)
        bootstrap=verify_source_before_import(data=data,source=ROOT,build_receipt=Path(child_ref['path']),home=home)
    publisher=publisher_proposal(args.publisher_plist,evidence_root=args.evidence_root,source=ROOT,parent=parent,database=database) if getattr(args,'publisher_plist',None) else {'status':'not_prepared'}
    plan={'contract':'account-intake-install-proposal-v1','status':'prepared','created_at':at,
        'installed_plist':str(args.installed_plist),'before_plist':before_ref,'next_plist':next_ref,
        'parent_build':build_ref,'parent_install':install_ref,'child_build':child_ref,'migration':migration_ref,
        'bootstrap_verification':bootstrap,'publisher':publisher,'formal_database':str(database),'database_identity':migration['database_identity'],
        'services_changed':False,'paid_gates_issued':0,'local_capture_authorized':local_capture,
        'publisher_activation_authorized':False if local_capture else None,'install_steps':[
            'Keep Writer stopped after the separately recorded formal-mutation migration.',
            'Recheck source/check/migration/build hashes and that installed plist equals writer.before.plist.',
            'Require paired Publisher next plist/environment and completed remote schema21-to22 transition before normal publishing.',
            'Replace Writer and prepared Publisher plist/environment together; preserve the original cleanup install receipt.',
            'Start Writer and verify exact loaded build, schema22, unchanged database inode and data-backed account API.',
            'Complete qualification, pricing, budget and activation checks per actual preparation/discovery/metrics operation before provider execution.',
            'If startup verification fails, keep Writer stopped. Preserve the backup and newer database; do not run old schema21 code against schema22 or overwrite newer data.']}
    if local_capture:
        plan['install_steps'] = [
            'Keep Publisher unloaded; this authorization permits local capture only and does not permit remote publishing.',
            'Keep Writer stopped and recheck the migration, source, checks, build and original Writer plist hashes.',
            'Replace only the local Writer plist; preserve the original cleanup install receipt and database inode.',
            'Start Writer and verify schema22, exact loaded build and a data-backed local account API.',
            'Issue each exact authorized operation through the durable Writer operation_publish command, then verify real budget and transport gates.',
            'Run account preparation and verify original responses, identity binding, content pagination and subsequent metric capture independently.',
            'Keep Publisher unloaded until a separate user-approved remote transition. Preserve newer data if local startup fails.']
    return {'proposal':write(args.evidence_root/'install-proposal.json',plan),**plan}


def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='action',required=True)
    for action in ('freeze','check','prepare'):
        command=sub.add_parser(action)
        command.add_argument('--parent-build',type=Path,required=True)
    frozen,checking,prepared=(sub.choices[key] for key in ('freeze','check','prepare'))
    frozen.add_argument('--source-root',type=Path,required=True);frozen.add_argument('--source-tree',type=Path,required=True)
    checking.add_argument('--source-tree',type=Path,required=True)
    checking.add_argument('--name',choices=['intake_schema','intake_execution'],required=True)
    checking.add_argument('--output',type=Path,required=True);checking.add_argument('command',nargs=argparse.REMAINDER)
    for name in ('parent-install','installed-plist','migration','evidence-root'):
        prepared.add_argument('--'+name,type=Path,required=True)
    prepared.add_argument('--publisher-plist',type=Path)
    prepared.add_argument('--check-report',action='append',default=[])
    prepared.add_argument('--actor',required=True);prepared.add_argument('--reason',required=True)
    prepared.add_argument('--approve-production-rollout',action='store_true')
    prepared.add_argument('--approve-local-capture',action='store_true',help='Record explicit user authorization for local Writer capture only; never authorize publishing')
    prepared.add_argument('--user-instruction-file',type=Path,help='Private UTF-8 file containing the actual user instruction')
    prepared.add_argument('--source-thread-id',help='Thread carrying that explicit user instruction')
    args=parser.parse_args()
    for value in vars(args).values():
        if isinstance(value,Path):
            release.require(value.is_absolute() and value.resolve()==value,'absolute non-symlink paths required')
    result={'freeze':freeze,'check':check,'prepare':prepare}[args.action](args)
    print(json.dumps(result,ensure_ascii=False,sort_keys=True))


if __name__=='__main__':
    main()
