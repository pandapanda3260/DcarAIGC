"""Schema22 successor verification; candidate checks never issue paid authority.

The original schema21 verifier is loaded from its hash-bound parent source.
Schema21 receipts keep their original meaning; schema22 proves preservation
through its own immutable migration receipt and a separate installation record.
"""
from __future__ import annotations
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
from typing import Any, Mapping
from .account_classification_release import digest, raw, reference, object_at, payload_at, records

CONTRACT = 'account-intake-schema-successor-v1'
CHECK_CONTRACT = 'account-intake-release-check-v1'
INSTALL_CONTRACT = 'account-intake-install-v1'
MODULE = 'src/dcar_eval/v8/account_intake_release.py'
REQUIRED_SOURCE = frozenset('src/dcar_eval/v8/'+name+'.py' for name in (
    'account_intake','account_preparation','platform_adapters','schema_v22','account_intake_release',
    'account_cleanup_runtime','provider_budget','capture','capture_runtime','account_capture_eligibility','runtime_paths',
    'account_reference_storage','account_cleanup_snapshot','account_preparation_authority',
    'account_directory_reconciliation')) | frozenset({
    'deploy/macos/publish_snapshot.py','deploy/macos/run_snapshot_publisher.sh',
    'scripts/build_server_snapshot.py','deploy/server/install_snapshot.py',
    'scripts/install_account_intake.py','scripts/prepare_account_intake_release.py',
    'scripts/import_account_summary.py','scripts/import_installed_account_summary.py'})


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError('account intake release: '+reason)


def inherited_classification_proof(connection: sqlite3.Connection) -> dict[str, Any]:
    """Check the untouched schema21 receipt through the schema22 source hash."""
    from .schema_v22 import migration_proof
    current = migration_proof(connection)
    rows = connection.execute('SELECT applied_at,payload_json,receipt_sha256 FROM account_classification_migrations').fetchall()
    require(len(rows)==1,'original schema21 migration receipt missing')
    at, value, checksum = tuple(rows[0])
    receipt = json.loads(value)
    body = dict(receipt); supplied = body.pop('sha256',None)
    manifest = connection.execute('SELECT name,applied_at FROM schema_migrations WHERE version=21').fetchone()
    require(manifest is not None and tuple(manifest)==('account-classification-v1',at)
        and checksum==supplied==digest(body)
        and receipt.get('contract_version')=='account-classification-migration-v1'
        and receipt.get('source_version')==20 and receipt.get('target_version')==21
        and receipt.get('migration_name')=='account-classification-v1' and receipt.get('applied_at')==at
        and receipt.get('provider_calls')==0 and receipt.get('capture_state_changed') is False
        and not {'account_type','content_direction'} & set(receipt['after']['accounts_columns'])
        and {'account_group','business_direction'} <= set(receipt['after']['directory_columns'])
        and current['source_schema_sha256']==receipt['after']['schema_sha256'],
        'schema21 provenance does not match the schema22 source')
    # Schema22 also sealed the exact old receipt table bytes before migration.
    from .schema_v20 import row_digest
    migrated = json.loads(connection.execute('SELECT payload_json FROM account_intake_migrations').fetchone()[0])
    preserved = migrated['preserved_tables']['account_classification_migrations']
    require(row_digest(connection,'account_classification_migrations',preserved['columns'])==preserved,
            'historical schema21 receipt changed after migration')
    return {'contract_version':'account-classification-migration-proof-v1','schema_version':21,
        'schema_migration':'account-classification-v1','receipt_sha256':checksum,'applied_at':at,
        'source_schema_sha256':receipt['before']['schema_sha256'],
        'target_schema_sha256':receipt['after']['schema_sha256'],
        'migrated_directory_rows':len(receipt['changed_rows'])}


def source_changes(parent: Mapping[str, Any], current: Mapping[str, Any]) -> dict[str, Any]:
    left, right = records(parent), records(current)
    require(REQUIRED_SOURCE <= set(right),'new source manifest omits intake execution code')
    result = {}
    for name in sorted(set(left)|set(right)):
        old,new = left.get(name),right.get(name)
        if old == new:
            continue
        require(old is None or new is None or old['mode']==new['mode'],'source permissions unexpectedly changed')
        require(old is None or new is None or old['sha256']!=new['sha256'],'source metadata differs without changed bytes')
        result[name]={'before_sha256':old['sha256'] if old else None,'after_sha256':new['sha256'] if new else None}
    require(MODULE in result,'schema22 source does not contain the new successor verifier')
    return result


def _parent_context(parent_ref: Mapping[str, Any], *, install_path: Path, database: Path, at: str | None):
    parent = payload_at(parent_ref,'sealed-build-receipt-v1')
    require(parent.get('status')=='succeeded' and parent.get('schema_contract')=={'code_schema':21,'formal_schema':21}
            and not parent.get('account_intake_successor'),'parent is not a schema21 build')
    source=Path(parent['source_root'])
    module_path=source/'src/dcar_eval/v8/account_classification_release.py'
    require(hashlib.sha256(raw(module_path,private=False)).hexdigest()==parent['critical_files'].get('src/dcar_eval/v8/account_classification_release.py'),
            'original parent verifier bytes changed')
    spec=importlib.util.spec_from_file_location('intake_parent_classification_verifier',module_path)
    require(spec is not None and spec.loader is not None,'parent verifier unavailable')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    inherited=module.verify_inheritance(build=parent,build_ref=parent_ref,install_path=install_path,
                                       database=database,source=source,at=at)
    return parent,inherited


def verify_inheritance(*, build: Mapping[str,Any], build_ref: Mapping[str,Any], install_path: Path,
                       database: Path, source: Path, at: str | None=None,
                       connection: sqlite3.Connection | None=None) -> dict[str,Any]:
    if build.get('account_intake_code_successor') is not None:
        from .account_intake_code_successor import verify_inheritance as verify_code_successor
        return verify_code_successor(build=build, build_ref=build_ref, install_path=install_path,
            database=database, source=source, at=at, connection=connection)
    from datetime import datetime
    from .account_catalog_capture_release import ACCOUNT_CATALOG_POLICY_V3
    from .schema_v22 import migration_proof
    plan=build.get('account_intake_successor',{})
    require(plan.get('contract')==CONTRACT,'missing schema22 successor')
    require(dict(build)==payload_at(build_ref,'sealed-build-receipt-v1'),'loaded build differs from receipt')
    parent,inherited=_parent_context(plan['parent_build'],install_path=install_path,database=database,at=at)
    allowed={'source_root','git','critical_files','code_successor_plan','account_cleanup_generation',
             'account_intake_successor','schema_contract','created_at','validation_scope'}
    require({k:v for k,v in build.items() if k not in allowed}=={k:v for k,v in parent.items() if k not in allowed},
            'inherited execution controls changed')
    require(build.get('schema_contract')=={'code_schema':22,'formal_schema':22}
            and build['source_root']==str(source) and source!=Path(parent['source_root']), 'new schema/source binding differs')
    generation=build['account_cleanup_generation']
    require({k:v for k,v in generation.items() if k!='source_tree'}=={k:v for k,v in parent['account_cleanup_generation'].items() if k!='source_tree'},
            'operator/capsule authority changed')
    require(generation['source_tree']==plan['source_tree'],'new source generation differs')
    tree=object_at(plan['source_tree']); original=object_at(parent['account_cleanup_generation']['source_tree'])
    require(tree['source_root']==str(source) and tree['git']==build['git'],'source inventory root/git differs')
    changes=source_changes(original,tree)
    require(changes==plan.get('changes'),'source change list differs')
    current=records(tree)
    require(current[MODULE]['sha256']==hashlib.sha256(raw(source/MODULE,private=False)).hexdigest(), 'loaded successor module differs')
    require(build['critical_files']=={name:item['sha256'] for name,item in current.items()
        if name.startswith(('src/','config/')) and name.endswith(('.py','.json'))},'critical source inventory differs')
    require(object_at(build['code_successor_plan'])=={'contract':'account-cleanup-source-plan-v1','transition':'account-cleanup-0907-v1',
        'project_root':build['project_root'],'source_root':str(source),'git':build['git'],'source_tree':plan['source_tree']},'source plan differs')
    identity=database.stat()
    migration=object_at(plan['migration'])
    require(migration.get('contract')==INSTALL_CONTRACT and migration.get('status')=='migrated'
        and migration.get('from_schema')==21 and migration.get('to_schema')==22
        and migration.get('formal_database')==str(database)
        and migration.get('database_identity')=={'device':identity.st_dev,'inode':identity.st_ino}
        and migration.get('authority_build')==plan['parent_build'] and migration.get('authority_install')==reference(install_path)
        and migration.get('source_tree')==plan['source_tree'] and migration.get('checks')==plan['checks']
        and migration.get('preserved_tables_verified') is True and migration.get('paid_gates_issued')==0
        and migration.get('receipt_sha256')==digest({k:v for k,v in migration.items() if k!='receipt_sha256'}),
        'schema22 installation proof differs')
    owned=connection is None
    if owned:
        connection=sqlite3.connect(database.as_uri()+'?mode=ro',uri=True)
        connection.row_factory=sqlite3.Row
        connection.execute('PRAGMA foreign_keys=ON');connection.execute('PRAGMA recursive_triggers=ON')
    try:
        actual=migration_proof(connection); legacy=inherited_classification_proof(connection)
        require(actual==migration.get('migration_proof') and legacy==migration.get('inherited_classification_proof'),
                'installed database migration chain differs')
        original_install=object_at(parent['account_classification_successor']['migration'])
        require(legacy==original_install.get('migration_proof'),'schema21 installed migration was replaced')
    finally:
        if owned: connection.close()
    checks=plan.get('checks',{})
    require(set(checks)=={'intake_schema','intake_execution'},'schema and execution checks are required')
    for name,ref in checks.items():
        check=object_at(ref)
        require(check.get('contract')==CHECK_CONTRACT and check.get('name')==name and check.get('status')=='passed'
            and check.get('exit_code')==0 and check.get('source_tree')==plan['source_tree'] and check.get('changes')==changes
            and check.get('command') and reference(Path(check['output']['path']))==check['output'], 'focused check differs from final source')
    require(plan.get('account_catalog_policy')==ACCOUNT_CATALOG_POLICY_V3
        and plan.get('account_catalog_policy_sha256')==digest(ACCOUNT_CATALOG_POLICY_V3)
        and plan.get('legacy_execution_controls')=='inherited_unchanged'
        and plan.get('provider_qualification')=='required_per_operation'
        and (plan.get('production_rollout')=='approved_by_user' or
             plan.get('production_rollout')=='not_authorized' and plan.get('local_activation')=='approved_by_user'
             and plan.get('operation_authorization') is not None)
        and bool(str(plan.get('actor') or '').strip()) and bool(str(plan.get('reason') or '').strip()), 'successor execution scope differs')
    issued=datetime.fromisoformat(plan['issued_at'].replace('Z','+00:00'))
    require(issued.utcoffset() is not None and datetime.fromisoformat(migration['migrated_at'].replace('Z','+00:00'))<=issued
            and build['created_at']==plan['issued_at'],'successor issuance time differs')
    if at is not None:
        require(issued<=datetime.fromisoformat(at.replace('Z','+00:00')),'future successor receipt')
    proof={'contract':CONTRACT,'loaded_build':dict(build_ref),'parent_build':plan['parent_build'],
        'source_tree':plan['source_tree'],'migration':plan['migration'],'changes':changes,'checks':checks,
        'issued_at':plan['issued_at'],'actor':plan['actor'],'reason':plan['reason'],
        'account_catalog_policy':dict(ACCOUNT_CATALOG_POLICY_V3),'account_catalog_policy_sha256':digest(ACCOUNT_CATALOG_POLICY_V3),
        'provider_qualification':'required_per_operation','legacy_execution_controls':'inherited_unchanged'}
    proof['proof_sha256']=digest(proof)
    result = {**inherited,'intake_proof':proof,'catalog_capture_proof':proof,
        'catalog_capture_policy':dict(ACCOUNT_CATALOG_POLICY_V3),'catalog_capture_policy_sha256':digest(ACCOUNT_CATALOG_POLICY_V3)}
    if plan.get('operation_authorization') is not None:
        from .account_preparation_authority import validate_authorization, PROOF_CONTRACT, OPERATIONS
        approval = object_at(plan['operation_authorization'])
        validate_authorization(approval, parent_build=plan['parent_build'], source_tree=plan['source_tree'],
            migration=plan['migration'], database={'path':str(database),'device':identity.st_dev,'inode':identity.st_ino},
            policy_sha256=digest(ACCOUNT_CATALOG_POLICY_V3), at=at or plan['issued_at'])
        require(datetime.fromisoformat(approval['issued_at'].replace('Z','+00:00')) <= issued,
                'operation authorization was issued after the sealed build')
        authority = {'contract':PROOF_CONTRACT,'operations':sorted(OPERATIONS),
            'authorization':plan['operation_authorization'],'authorization_payload':approval,
            'loaded_build':dict(build_ref),'source_tree':plan['source_tree'],'intake_proof_sha256':proof['proof_sha256'],
            'runtime_root_receipt':build['runtime_root_receipt'],
            'transport_manifest':generation['transport_manifest']}
        authority['proof_sha256'] = digest(authority)
        result['preparation_operation_authority'] = authority
    return result


def inventory(source: Path) -> dict[str,Any]:
    """Full tracked and nonignored source manifest, checked for concurrent edits."""
    import os
    import stat
    from .runtime_paths import verified_git
    def git(*args): return verified_git(source,*args)
    state=git('status','--porcelain=v1','--untracked-files=all')
    revision={'mode':'working-tree-source-v1','head':git('rev-parse','HEAD').decode().strip(),
        'tree':git('rev-parse','HEAD^{tree}').decode().strip(),'branch':git('symbolic-ref','--short','HEAD').decode().strip(),
        'status_porcelain_sha256':hashlib.sha256(state).hexdigest()}
    files=[]
    for name in sorted({os.fsdecode(item) for item in git('ls-files','--cached','--others','--exclude-standard','-z').split(b'\0') if item}):
        path=source/name
        if not path.exists() and not path.is_symlink(): continue
        body=raw(path,private=False)
        files.append({'path':name,'sha256':hashlib.sha256(body).hexdigest(),'byte_size':len(body),'mode':stat.S_IMODE(path.stat().st_mode)})
    require(git('status','--porcelain=v1','--untracked-files=all')==state,'source changed during inventory')
    return {'contract':'writer-source-tree-v1','source_root':str(source),'git':revision,'files':files}


def verify_candidate_source(*, source: Path, parent_build_ref: Mapping[str,Any],
                            checks: Mapping[str,Mapping[str,Any]]) -> dict[str,Any]:
    """Installer preflight: exact tested source, no database writes or gate issue."""
    require(set(checks)=={'intake_schema','intake_execution'},'required source checks missing')
    parent=payload_at(parent_build_ref,'sealed-build-receipt-v1')
    require(parent.get('status')=='succeeded' and parent.get('schema_contract')=={'code_schema':21,'formal_schema':21},'parent schema differs')
    data=Path(parent['project_root']);original_source=Path(parent['source_root'])
    require(source.is_absolute() and source.resolve(strict=True)==source
        and source!=original_source and not source.is_relative_to(original_source) and not original_source.is_relative_to(source)
        and not source.is_relative_to(data) and not data.is_relative_to(source), 'candidate source must be independent of parent and data')
    tree=inventory(source)
    changes=source_changes(object_at(parent['account_cleanup_generation']['source_tree']),tree)
    refs=[]
    for name,ref in checks.items():
        check=object_at(ref);tree_ref=check.get('source_tree',{})
        require(check.get('contract')==CHECK_CONTRACT and check.get('name')==name and check.get('status')=='passed'
                and check.get('exit_code')==0 and check.get('changes')==changes and check.get('command')
                and object_at(tree_ref)==tree and reference(Path(check['output']['path']))==check['output'],
                'check does not bind exact candidate source')
        refs.append(tree_ref)
    require(all(ref==refs[0] for ref in refs),'checks bind different source manifests')
    return {'parent':parent,'source_tree':tree,'source_tree_ref':refs[0],'changes':changes,'checks':dict(checks)}


def validate_installed_writer(*, installed_plist: Path, parent_build_ref: Mapping[str,Any],
                              parent_install_ref: Mapping[str,Any], database: Path, project_root: Path,
                              at: str | None=None) -> dict[str,Any]:
    import plistlib
    body=raw(installed_plist,private=False); installed=plistlib.loads(body)
    parent,inherited=_parent_context(parent_build_ref,install_path=Path(parent_install_ref['path']),database=database,at=at)
    require(reference(Path(parent_install_ref['path']))==parent_install_ref,'parent install receipt changed')
    env=installed.get('EnvironmentVariables',{})
    require(env.get('DCAR_PROJECT_ROOT')==str(project_root)==parent['project_root']
        and env.get('DCAR_V8_DB')==str(database) and env.get('DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT')==parent_install_ref['path']
        and env.get('DCAR_WRITER_SOURCE_ROOT')==parent['source_root']
        and reference(Path(env.get('DCAR_LOADED_BUILD_RECEIPT','')))==parent_build_ref
        and installed.get('ProgramArguments')==[str(Path(parent['source_root'])/'deploy/macos/run_writer_worker.sh')],
        'installed Writer differs from the exact schema21 parent')
    return {'bytes':body,'payload':installed,'parent':parent,'inherited':inherited}
