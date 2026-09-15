#!/usr/bin/env python3
"""Freeze/check schema23, then prepare its explicit local Writer authorization.

No action here changes the formal database, installed launch files, paid gates,
or service state. Initial schema23 requires the completed migration. A code-only
successor inherits the actual installed schema23 build without repeating DDL.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import plistlib
import subprocess
import sys
import tempfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"src/dcar_eval"))
sys.path.insert(0, str(ROOT/"scripts"))
from v8 import four_platform_flow_release as release, four_platform_flow_authority as authority
from v8.metric_source_policy import current_policy_binding
from v8.runtime_paths import verified_git
from install_four_platform_flow import checks_at
from prepare_account_intake_release import write, write_bytes


def freeze(args):
    before = release.inventory(ROOT)
    parent = release.payload_at(release.reference(args.parent_build), "sealed-build-receipt-v1")
    release.require(parent.get("schema_contract") == {"code_schema":22,"formal_schema":22}, "parent must be schema22")
    release.source_changes(release.object_at(parent["account_cleanup_generation"]["source_tree"]), before)
    data = Path(parent["project_root"])
    release.require(not args.source_root.exists() and args.source_root.resolve() == args.source_root
        and all(not args.source_root.is_relative_to(other) and not other.is_relative_to(args.source_root)
            for other in (data,ROOT,Path(parent["source_root"])))
        and not args.source_tree.is_relative_to(args.source_root) and not args.source_tree.is_relative_to(ROOT),
        "new independent source and external manifest required")
    args.source_root.parent.mkdir(parents=True,exist_ok=True)
    subprocess.run(["git","clone","--local","--no-hardlinks","--branch",before["git"]["branch"],str(ROOT),str(args.source_root)],
        check=True,capture_output=True,env={**os.environ,"GIT_CONFIG_NOSYSTEM":"1","GIT_CONFIG_GLOBAL":os.devnull})
    tracked = {os.fsdecode(value) for value in verified_git(args.source_root,"ls-files","-z").split(b"\0") if value}
    live = {row["path"] for row in before["files"]}
    for name in tracked-live: (args.source_root/name).unlink()
    for item in before["files"]:
        body = release.raw(ROOT/item["path"],private=False)
        release.require(hashlib.sha256(body).hexdigest() == item["sha256"], "source changed during freeze")
        target = args.source_root/item["path"]; target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(body); target.chmod(item["mode"])
    after = release.inventory(args.source_root)
    release.require(before == release.inventory(ROOT) and before["files"] == after["files"] and before["git"] == after["git"],
        "frozen source differs from reviewed source")
    return {"status":"frozen", "source_root":str(args.source_root), "source_tree":write(args.source_tree,after), "services_changed":False}


def check(args):
    before = release.inventory(ROOT); tree_ref = release.reference(args.source_tree)
    release.require(release.object_at(tree_ref) == before, "check must run from unchanged frozen source")
    parent = release.payload_at(release.reference(args.parent_build), "sealed-build-receipt-v1")
    changes = release.source_changes(release.object_at(parent["account_cleanup_generation"]["source_tree"]), before)
    command = args.command[1:] if args.command and args.command[0] == "--" else args.command
    release.require(args.name in release.CHECKS and command and not args.output.is_relative_to(ROOT), "named check and external output required")
    environment = {key:value for key,value in os.environ.items() if not key.startswith("DCAR_")}
    environment.update(PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=os.pathsep.join(str(ROOT/name) for name in ("src/dcar_eval","tests","scripts")),
        DCAR_SCHEDULER_ENABLED="0", DCAR_STARTUP_CATCHUP_ENABLED="0")
    result = subprocess.run(command,cwd=ROOT,env=environment,capture_output=True)
    output = write_bytes(args.output.with_suffix(".log"),result.stdout+result.stderr)
    release.require(before == release.inventory(ROOT), "source changed during check")
    value = {"contract":release.CHECK_CONTRACT,"name":args.name,"source_tree":tree_ref,"changes":changes,
        "status":"passed" if result.returncode == 0 else "failed", "exit_code":result.returncode,
        "command":command,"output":output,"completed_at":datetime.now(timezone.utc).isoformat()}
    ref = write(args.output,value)
    release.require(result.returncode == 0, "check failed; inspect "+str(args.output.with_suffix(".log")))
    return {"report":ref,**value}


def prepare(args):
    release.require(args.approve_local_capture, "explicit local activation approval required")
    instruction = release.raw(args.user_instruction_file).decode("utf-8").strip()
    release.require(instruction and args.source_thread_id.strip() and args.actor.strip() and args.reason.strip(), "actual user instruction and provenance required")
    build_ref, install_ref = release.reference(args.parent_build), release.reference(args.parent_install)
    checked = release.verify_candidate_source(source=ROOT,parent_build_ref=build_ref,checks=checks_at(args.check_report))
    parent = checked["parent"]; data = Path(parent["project_root"])
    migration_ref = release.reference(args.migration); migration = release.object_at(migration_ref)
    database = Path(migration["formal_database"])
    release.require(release.file_reference(Path(migration["backup"]["path"])) == migration["backup"], "migration backup changed")
    code_ref = release.reference(args.code_predecessor_build) if getattr(args,"code_predecessor_build",None) else None
    index_ref = release.reference(args.index_install) if getattr(args,"index_install",None) else None
    inherited_index_ref = release.reference(args.inherited_index_install) if getattr(args,"inherited_index_install",None) else None
    release.require(index_ref is None or code_ref is not None, "performance index requires a schema23 predecessor")
    release.require(inherited_index_ref is None or code_ref is not None,
        "inherited performance index requires a schema23 predecessor")
    release.require(not (index_ref and inherited_index_ref), "new and inherited index receipts are mutually exclusive")
    repair = None
    if code_ref is None:
        with release.readonly_database(Path(migration["backup"]["path"])) as backup:
            installed = release.validate_installed_writer(installed_plist=args.installed_plist,parent_build_ref=build_ref,
                parent_install_ref=install_ref,database=database,project_root=data,connection=backup)
        release.require(migration["previous_loaded_build"] == build_ref
            and migration["previous_writer_plist_sha256"] == hashlib.sha256(installed["bytes"]).hexdigest(), "installed parent changed since migration")
    else:
        predecessor = release.payload_at(code_ref,"sealed-build-receipt-v1")
        code_only = predecessor.get(release.FIELD,{}).get("code_predecessor") is not None
        release.require(not code_only or (index_ref is None and inherited_index_ref is not None),
            "chained repair requires the inherited index receipt and no new DDL")
        release.require(code_only or inherited_index_ref is None,
            "original schema23 build cannot inherit an index install")
        index = release.verify_index_install(index_ref, origin_ref=code_ref,
            source_tree_ref=checked["source_tree_ref"], checks=checked["checks"], database=database) if index_ref else None
        origin, inherited = release.code_parent_context(code_ref,install_path=args.parent_install,database=database,
            origin_backup=index["backup"] if index else None)
        origin_plan = origin[release.FIELD]
        body = release.raw(args.installed_plist,private=False); payload = plistlib.loads(body)
        env = payload.get("EnvironmentVariables",{})
        release.require(origin_plan["parent_build"] == build_ref and origin_plan["migration"] == migration_ref
            and origin_plan["parent_install"] == install_ref and payload.get("Label") == "cn.tj.dcar.writer-worker"
            and payload.get("WorkingDirectory") == str(data) == origin["project_root"]
            and env.get("DCAR_PROJECT_ROOT") == str(data) and env.get("DCAR_V8_DB") == str(database)
            and env.get("DCAR_WRITER_SOURCE_ROOT") == origin["source_root"]
            and release.reference(Path(env.get("DCAR_LOADED_BUILD_RECEIPT",""))) == code_ref
            and env.get("DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT") == str(args.parent_install)
            and payload.get("ProgramArguments") == [str(Path(origin["source_root"])/"deploy/macos/run_writer_worker.sh")],
            "installed Writer differs from code predecessor")
        installed = {"bytes":body,"payload":payload,"inherited":inherited}
        if code_only:
            # Each generation supplies its own narrow scope and file list; the
            # verified predecessor retains the original migration/index proof.
            release.require(inherited_index_ref == release.inherited_index_reference(origin,inherited),
                "inherited index differs from the verified code predecessor")
            repair = {"contract":release.CODE_ONLY_CONTRACT,"build":code_ref,
                "proof_sha256":inherited["four_platform_flow_proof"]["proof_sha256"],
                "changes":release.code_repair_changes(origin,checked["source_tree"],code_only=True),
                "scope":release.CODE_ONLY_SCOPE,"schema_migration_repeated":False,
                "database_writes":0,"inherited_index_install":inherited_index_ref}
        else:
            repair = {"build":code_ref,"proof_sha256":inherited["four_platform_flow_proof"]["proof_sha256"],
                "changes":release.code_repair_changes(origin,checked["source_tree"]),
                "scope":"queue_fairness_and_metric_plan_reuse","schema_migration_repeated":False,
                "database_writes":1 if index else 0, **({"index_install":index_ref} if index else {})}
    with release.readonly_database(database) as live:
        release.verify_migration(live,migration)
        from v8.profile_activations import activation_at
        at = datetime.now(timezone.utc).isoformat(); active = activation_at(live,at)
    release.require(active is not None, "existing activation required")
    release.require(not args.evidence_root.exists() and args.evidence_root.resolve() == args.evidence_root
        and not args.evidence_root.is_relative_to(ROOT) and not args.evidence_root.is_relative_to(data), "new external evidence directory required")
    args.evidence_root.mkdir(mode=0o700,parents=True)
    tree, tree_ref = checked["source_tree"], checked["source_tree_ref"]
    approval = {"contract":authority.AUTHORIZATION_CONTRACT,"schema_version":23,"scope":"local_writer_forward_flow_only",
        "historical_backfill_authorized":False,"operations":sorted(authority.OPERATIONS),"platforms":list(authority.PLATFORMS),
        "manual_statuses":list(authority.STATUSES),"qualification":"operator_authorized","business_e2e":"required",
        "transport_qualification":"not_verified","publisher_authorized":False,"remote_database_authorized":False,
        "parent_build":build_ref,"source_tree":tree_ref,"migration":migration_ref,
        "formal_database":{"path":str(database),**migration["database_identity"]},
        "catalog_policy_sha256":installed["inherited"]["catalog_capture_policy_sha256"],"metric_policy":current_policy_binding(),
        "activation":{key:active[key] for key in authority.ACTIVE_KEYS},"actor":args.actor,"reason":args.reason,
        "user_instruction":instruction,"source_thread_id":args.source_thread_id,"issued_at":at}
    approval_ref = write(args.evidence_root/"local-flow-authorization.json",approval)
    source_plan = write(args.evidence_root/"source-plan.json",{"contract":"account-cleanup-source-plan-v1",
        "transition":"account-cleanup-0907-v1","project_root":str(data),"source_root":str(ROOT),"git":tree["git"],"source_tree":tree_ref})
    successor = {"contract":release.CONTRACT,"parent_build":build_ref,"parent_install":install_ref,"source_tree":tree_ref,
        "changes":checked["changes"],"checks":checked["checks"],"migration":migration_ref,
        "operation_authorization":approval_ref,"issued_at":at,"actor":args.actor,"reason":args.reason}
    if repair is not None:
        successor["code_predecessor"] = repair
    child = {**parent,"source_root":str(ROOT),"git":tree["git"],"critical_files":{row["path"]:row["sha256"] for row in tree["files"]
        if row["path"].startswith(("src/","config/")) and row["path"].endswith((".py",".json"))},
        "code_successor_plan":source_plan,"account_cleanup_generation":{**parent["account_cleanup_generation"],"source_tree":tree_ref},
        release.FIELD:successor,"schema_contract":{"code_schema":23,"formal_schema":23},"created_at":at,
        "validation_scope":"four platform forward flow; immutable schema22 lineage; no historical backfill or remote publishing"}
    child_ref = write(args.evidence_root/"build.json",{"contract_version":"sealed-build-receipt-v1","payload":child,"payload_sha256":release.digest(child)})
    release.verify_inheritance(build=child,build_ref=child_ref,install_path=args.parent_install,database=database,source=ROOT,at=at)
    proposal = {**installed["payload"],"EnvironmentVariables":{**installed["payload"]["EnvironmentVariables"],
        "DCAR_LOADED_BUILD_RECEIPT":child_ref["path"],"DCAR_WRITER_SOURCE_ROOT":str(ROOT),
        "PYTHONPATH":os.pathsep.join(str(ROOT/name) for name in ("src/dcar_eval","scripts"))},
        "ProgramArguments":[str(ROOT/"deploy/macos/run_writer_worker.sh")]}
    before_ref = write_bytes(args.evidence_root/"writer.before.plist",installed["bytes"])
    next_ref = write_bytes(args.evidence_root/"writer.next.plist",plistlib.dumps(proposal,sort_keys=True))
    from v8.runtime_paths import verify_source_before_import
    with tempfile.TemporaryDirectory() as temporary:
        home = Path(temporary).resolve(); target = home/"Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        target.parent.mkdir(parents=True); target.write_bytes(plistlib.dumps(proposal)); target.chmod(0o600)
        bootstrap = verify_source_before_import(data=data,source=ROOT,build_receipt=Path(child_ref["path"]),home=home)
    plan = {"contract":"four-platform-flow-install-proposal-v1","status":"prepared","created_at":at,
        "installed_plist":str(args.installed_plist),"before_plist":before_ref,"next_plist":next_ref,
        "parent_build":build_ref,"parent_install":install_ref,"child_build":child_ref,"migration":migration_ref,
        "formal_database":str(database),"database_identity":migration["database_identity"],"bootstrap_verification":bootstrap,
        "local_capture_authorized":True,"publisher_activation_authorized":False,"services_changed":False,"paid_gates_issued":0}
    return {"proposal":write(args.evidence_root/"install-proposal.json",plan),**plan}


def main():
    parser = argparse.ArgumentParser(description=__doc__); sub = parser.add_subparsers(dest="action",required=True)
    for action in ("freeze","check","prepare"):
        sub.add_parser(action).add_argument("--parent-build",type=Path,required=True)
    frozen,checking,prepared = (sub.choices[name] for name in ("freeze","check","prepare"))
    for name in ("source-root","source-tree"): frozen.add_argument("--"+name,type=Path,required=True)
    checking.add_argument("--source-tree",type=Path,required=True);checking.add_argument("--name",choices=sorted(release.CHECKS),required=True)
    checking.add_argument("--output",type=Path,required=True);checking.add_argument("command",nargs=argparse.REMAINDER)
    for name in ("parent-install","installed-plist","migration","evidence-root","user-instruction-file"):
        prepared.add_argument("--"+name,type=Path,required=True)
    for name in ("actor","reason","source-thread-id"): prepared.add_argument("--"+name,required=True)
    prepared.add_argument("--approve-local-capture",action="store_true")
    prepared.add_argument("--code-predecessor-build",type=Path,
        help="Exact installed schema23 build; retain its verified original migration receipt")
    prepared.add_argument("--index-install",type=Path,
        help="Sealed optional single-index maintenance receipt; original schema23 migration is unchanged")
    prepared.add_argument("--inherited-index-install",type=Path,
        help="Exact index receipt already verified by the installed code predecessor; performs no DDL")
    prepared.add_argument("--check-report",action="append",default=[])
    args = parser.parse_args()
    for value in vars(args).values():
        if isinstance(value,Path): release.require(value.is_absolute() and value.resolve() == value,"absolute nonsymlink paths required")
    print(json.dumps({"freeze":freeze,"check":check,"prepare":prepare}[args.action](args),ensure_ascii=False))


if __name__ == "__main__":
    main()
