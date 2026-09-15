"""Shared account intake, merge rules and durable preparation requests.

Web, workbook and legacy import adapters use these same commands. The caller
owns database access, transactions and backup; workers own network I/O. Existing
schema21 import receipts retain their original offline contract and identities.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from typing import Any, Mapping

from .account_classification import ACCOUNT_GROUPS, BUSINESS_DIRECTIONS

HEADERS = (
    "平台", "运营人员", "账号名称", "ID（抖音号/快手号/小红书号/视频号）", "uid", "粉丝",
    "更新状态", "质量标签", "业务标签", "是否开通接单", "手机号", "手机号开卡人姓名",
    "使用人证件号码", "持卡人", "是否实名", "实名来源",
)
DISPLAY_ID = HEADERS[3]
PLATFORMS = {"抖音": "douyin", "快手": "kuaishou", "小红书": "xiaohongshu", "视频号": "wechat_channels"}
STATUSES = {"日更": "daily", "周更": "weekly", "暂停": "paused"}
ROLE_FIELDS = frozenset({"运营人员", "手机号", "手机号开卡人姓名", "使用人证件号码", "持卡人", "是否实名", "实名来源"})
UNKNOWN = frozenset({"未知", "无", "未填写", "未提供", "待补充", "待确认", "待分类", "无数据", "-", "—", "/", "null", "None"})
CONTRACT = "local-account-summary-import-v1"


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _rows(connection: sqlite3.Connection, sql: str, args: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    cursor = connection.execute(sql, args)
    keys = [column[0] for column in cursor.description or ()]
    return [dict(zip(keys, row)) for row in cursor.fetchall()]


def _text(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return ""
    return str(value).strip()


def _known(value: Any) -> bool:
    text = _text(value)
    return bool(text and text not in UNKNOWN and not any(mark in text for mark in ("待核实", "待核验", "待确认")))


def _uid(platform: str | None, value: Any) -> str | None:
    text = _text(value)
    patterns = {"douyin": r"[0-9]{6,24}", "xiaohongshu": r"[0-9a-fA-F]{24}",
                "kuaishou": r"(?:[0-9]{1,24}|3x[A-Za-z0-9]{4,64})",
                "wechat_channels": r"(?:sph[A-Za-z0-9]{8,100}|[A-Za-z0-9_-]{8,100}@finder)"}
    if platform not in patterns or not re.fullmatch(patterns[platform], text):
        return None
    return text.lower() if platform == "xiaohongshu" else text


def _display(value: Any) -> str | None:
    text = _text(value)
    return text if _known(value) and re.fullmatch(r"[A-Za-z0-9_.@-]{1,128}", text) else None


def _raw_json(value: str) -> dict[str, Any]:
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("Existing directory raw_json is not an object")
    return parsed


def _check_schema(connection: sqlite3.Connection) -> None:
    required = {
        "accounts": {"id", "phone", "phone_normalized", "operator_name", "enabled", "created_at", "updated_at"},
        "account_platform_identities": {"id", "account_id", "platform", "uid", "nickname", "real_name_status", "source", "created_at", "updated_at"},
        "account_directory_rows": {"id", "account_id", "platform", "uid", "nickname", "display_account_id", "phone", "operator_name", "account_status", "identity_status", "raw_json", "source_sha256", "source_name", "source_sheet", "source_row", "account_group", "business_direction", "imported_at", "updated_at"},
    }
    for table, columns in required.items():
        existing = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
        if not columns <= existing:
            raise ValueError("Account summary import requires the existing account-directory schema")


def _input(payload: Mapping[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    sha = _text(payload.get("sha256"))
    if not re.fullmatch(r"[0-9a-f]{64}", sha):
        raise ValueError("Account summary requires a source SHA256")
    records = payload.get("records")
    if not isinstance(records, list) or not records:
        raise ValueError("Account summary requires nonempty records")
    result, seen_rows = [], set()
    for record in records:
        if not isinstance(record, Mapping) or type(record.get("sourceRow")) is not int or record["sourceRow"] < 1:
            raise ValueError("Account summary requires positive source row numbers")
        row = record["sourceRow"]
        if row in seen_rows:
            raise ValueError("Account summary repeats a source row")
        seen_rows.add(row)
        raw = record.get("raw")
        if not isinstance(raw, Mapping) or set(raw) != set(HEADERS):
            raise ValueError("Account summary requires exactly the sixteen agreed fields")
        if any(v is not None and (isinstance(v, bool) or not isinstance(v, (str, int))) for v in raw.values()):
            raise ValueError("Workbook fields must preserve exact text or integer values")
        metadata = record.get("metadata", {})
        comment = record.get("comment", "")
        if not isinstance(metadata, Mapping) or not isinstance(comment, str):
            raise ValueError("Account summary metadata/comment types are invalid")
        normalized = {"sourceRow": row, "raw": dict(raw), "comment": comment, "metadata": dict(metadata)}
        fingerprint = hashlib.sha256(_json(normalized).encode()).hexdigest()
        platform = PLATFORMS.get(_text(raw["平台"]))
        candidate_uid = _uid(platform, raw["uid"])
        verified = metadata.get("enrichment_status") == "verified" and candidate_uid is not None and _uid(platform, metadata.get("verified_uid")) == candidate_uid
        display = _display(raw[DISPLAY_ID])
        profile = metadata.get("enrichment_profile")
        profile = profile if verified and isinstance(profile, Mapping) else {}
        display_unverified = platform == "kuaishou" and display is not None and _display(profile.get("ID")) != display
        if display_unverified:
            display = None
        conflicts = metadata.get("conflict_fields", [])
        if "uid" in conflicts:
            candidate_uid, verified = None, False
        if DISPLAY_ID in conflicts:
            display = None
        result.append({**normalized, "record_sha256": fingerprint, "platform": platform, "uid": candidate_uid,
                       "verified_uid": verified, "display_id": display, "display_unverified": display_unverified})
    return sha, result


def _base_fields(directory: Mapping[str, Any] | None, account: Mapping[str, Any] | None,
                 identity: Mapping[str, Any] | None, raw: Mapping[str, Any]) -> dict[str, Any]:
    fields = {header: None for header in HEADERS}
    for header in HEADERS:
        if _known(raw.get(header)):
            fields[header] = raw[header]
    summary = raw.get("account_summary")
    if isinstance(summary, Mapping) and isinstance(summary.get("fields"), Mapping):
        fields.update({key: value for key, value in summary["fields"].items() if key in HEADERS and _known(value)})
    directory, account, identity = directory or {}, account or {}, identity or {}
    platform = directory.get("platform") or identity.get("platform")
    fields["平台"] = next((label for label, code in PLATFORMS.items() if code == platform), fields["平台"])
    for header, key in (("运营人员", "operator_name"), ("手机号", "phone")):
        value = directory.get(key)
        if not _known(value):
            value = account.get(key)
        if _known(value):
            fields[header] = value
    for header, value in (("账号名称", directory.get("nickname") if _known(directory.get("nickname")) else identity.get("nickname")),
                          (DISPLAY_ID, directory.get("display_account_id")),
                          ("uid", identity.get("uid") or directory.get("uid")),
                          ("更新状态", next((k for k, v in STATUSES.items() if v == directory.get("account_status")), None)),
                          ("质量标签", ACCOUNT_GROUPS.get(directory.get("account_group"))),
                          ("业务标签", BUSINESS_DIRECTIONS.get(directory.get("business_direction")))):
        if _known(value):
            fields[header] = value
    status = identity.get("real_name_status")
    if status in {"yes", "no"}:
        fields["是否实名"] = "是" if status == "yes" else "否"
    return fields


def _accept(record: Mapping[str, Any], before: Mapping[str, Any], *, duplicate_display: bool) -> tuple[dict[str, Any], dict[str, str]]:
    fields, pending = dict(before), {}
    conflicts = record["metadata"].get("conflict_fields", [])
    name_pending = not _known(record["raw"]["账号名称"]) and bool(_text(record["raw"]["账号名称"])) or "账号名称" in conflicts
    for header, value in record["raw"].items():
        if value is None or value == "":
            continue
        reason = None
        if not _known(value) or header in conflicts:
            reason = "原表字段未明确或有冲突，保留已有有效值"
        elif header in ROLE_FIELDS and name_pending:
            reason = "账号名称归属待核实，人员及手机号关系暂不重绑定"
        elif header == "uid" and record["uid"] is None:
            reason = "UID格式或来源冲突未通过"
        elif header == DISPLAY_ID and (record["display_unverified"] or duplicate_display or record["display_id"] is None):
            reason = "显示ID未独立确认或存在重复冲突，不用内部UID充当显示号"
        elif header == "更新状态" and _text(value) not in STATUSES:
            reason = "更新状态没有明确映射"
        elif header == "质量标签" and _text(value) not in set(ACCOUNT_GROUPS.values()) - {"未填写"}:
            reason = "质量标签没有明确映射"
        elif header == "业务标签" and _text(value) not in set(BUSINESS_DIRECTIONS.values()) - {"未填写"}:
            reason = "业务标签没有明确映射"
        elif header == "是否实名" and _text(value) not in {"是", "否"}:
            reason = "实名状态未明确"
        if reason:
            pending[header] = reason
        else:
            fields[header] = record["uid"] if header == "uid" else _text(value) if header != "粉丝" else value
    return fields, pending


def _update(connection: sqlite3.Connection, table: str, row: Mapping[str, Any], patch: Mapping[str, Any], at: str) -> bool:
    changed = {key: value for key, value in patch.items() if row.get(key) != value}
    if not changed:
        return False
    changed["updated_at"] = at
    connection.execute(f"UPDATE {table} SET " + ",".join(f"{key}=?" for key in changed) + " WHERE id=?", (*changed.values(), row["id"]))
    return True


def _apply_summary(connection: sqlite3.Connection, payload: Mapping[str, Any], *, imported_at: str, prepare_only: bool = False, allow_uid_directory: bool = False,
                   prevent_new_directory_reason: str | None = None) -> dict[str, Any]:
    """Apply a complete increment under the caller transaction; never commit.

    Review and asset dispositions carry their complete source material for the
    caller's private audit file. Exception rollback is local to this function's
    savepoint. Successful replay performs zero writes, including timestamps.
    """
    if not connection.in_transaction:
        raise ValueError("Account summary import requires the caller transaction")
    if not isinstance(imported_at, str) or not imported_at:
        raise ValueError("Account summary requires an import timestamp")
    _check_schema(connection)
    sha, records = _input(payload)
    if prepare_only:
        for record in records:
            record["verified_uid"] = False
            record["display_id"] = (None if DISPLAY_ID in record["metadata"].get("conflict_fields", [])
                                    else _display(record["raw"].get(DISPLAY_ID)))
            record["display_unverified"] = False
    accounts = {r["id"]: r for r in _rows(connection, "SELECT * FROM accounts")}
    identities = _rows(connection, "SELECT * FROM account_platform_identities")
    directories = _rows(connection, "SELECT * FROM account_directory_rows")
    by_uid, by_display, by_account, identity_account, directory_uid = (defaultdict(list) for _ in range(5))
    imported = {}
    for identity in identities:
        by_uid[(identity["platform"], _uid(identity["platform"], identity["uid"]))].append(identity)
        identity_account[identity["account_id"]].append(identity)
    for directory in directories:
        by_account[directory["account_id"]].append(directory)
        if _display(directory["display_account_id"]):
            by_display[(directory["platform"], directory["display_account_id"])].append(directory)
        if _uid(directory["platform"], directory["uid"]):
            directory_uid[(directory["platform"], _uid(directory["platform"], directory["uid"]))].append(directory)
        raw = _raw_json(directory["raw_json"])
        summary = raw.get("account_summary", {})
        for receipt in summary.get("imports", []) if isinstance(summary, dict) else []:
            key = (receipt["source_sha256"], receipt["source_sheet"], receipt["source_row"])
            if key in imported:
                raise ValueError("Import receipt resolves to multiple directory rows")
            imported[key] = (directory, receipt)
    if "locator_revision" in {row[1] for row in connection.execute("PRAGMA table_info(account_directory_rows)")}:
        # Imported display labels are not ownership evidence. Current intake
        # may reuse only a typed alias previously bound by a verified profile.
        by_display.clear()
        for reference in _rows(connection, """SELECT i.account_id,i.platform,r.reference_value
            FROM account_provider_references r JOIN account_platform_identities i ON i.id=r.account_identity_id
            WHERE r.reference_kind IN ('display_account_id','channel_id') AND r.source_raw_response_id IS NOT NULL"""):
            for directory in by_account.get(reference["account_id"], []):
                key = (reference["platform"], reference["reference_value"])
                if not any(item["id"] == directory["id"] for item in by_display[key]):
                    by_display[key].append(directory)
    uid_counts = Counter((r["platform"], r["uid"]) for r in records if r["uid"])
    display_counts = Counter((r["platform"], r["display_id"]) for r in records if r["display_id"])
    plans, result_rows = [], []
    for record in records:
        row = {"source_row": record["sourceRow"], "status": None, "directory_row_id": None,
               "account_id": None, "identity_id": None, "changes": [], "pending_fields": {}}
        result_rows.append(row)
        receipt_key = (sha, _text(payload.get("sheet")), record["sourceRow"])
        if receipt_key in imported:
            directory, receipt = imported[receipt_key]
            if receipt["record_sha256"] != record["record_sha256"]:
                raise ValueError("Repeated source SHA/row has different input contents")
            row.update(status="unchanged", directory_row_id=directory["id"], account_id=directory["account_id"])
            members = identity_account.get(directory["account_id"], [])
            row["identity_id"] = members[0]["id"] if len(members) == 1 else None
            continue
        metadata = record["metadata"]
        if metadata.get("account_record_count") == 0 and int(metadata.get("phone_record_count") or 0) > 0:
            row.update(status="asset", reason="仅手机卡资料，不建立账号主体", source_record={key: record[key] for key in ("raw", "comment", "metadata")})
            continue
        reason = None
        platform, candidate_uid = record["platform"], record["uid"]
        duplicate_display = bool(record["display_id"] and display_counts[(platform, record["display_id"])] > 1)
        uid_matches = by_uid.get((platform, candidate_uid), []) if candidate_uid else []
        uid_directories = directory_uid.get((platform, candidate_uid), []) if candidate_uid else []
        display_matches = by_display.get((platform, record["display_id"]), []) if record["display_id"] and not duplicate_display else []
        identity = uid_matches[0] if len(uid_matches) == 1 else None
        directory = None
        if platform is None:
            reason = "平台未明确，不建立伪账号"
        elif candidate_uid and uid_counts[(platform, candidate_uid)] > 1:
            reason = "同批重复平台UID，需先核对源记录"
        elif len(uid_matches) > 1 or len(uid_directories) > 1 or len(display_matches) > 1:
            reason = "定位标识对应多条现存记录"
        else:
            targets = {r["id"]: r for r in uid_directories + display_matches}
            if identity:
                for target in by_account.get(identity["account_id"], []):
                    targets[target["id"]] = target
            if len(targets) > 1:
                reason = "UID和显示ID指向不同目录，暂不合并"
            elif targets:
                directory = next(iter(targets.values()))
                if directory["platform"] != platform:
                    reason = "现存主体平台不一致"
                existing_uid_raw = _text(directory.get("uid"))
                existing_uid = _uid(platform, existing_uid_raw)
                members = identity_account.get(directory["account_id"], []) if directory["account_id"] else []
                if len(members) > 1:
                    reason = "现存主体包含多个平台身份"
                if members:
                    existing_uid_raw = _text(members[0]["uid"])
                    existing_uid = _uid(platform, existing_uid_raw)
                    if members[0]["platform"] != platform:
                        reason = "现存主体平台不一致"
                    if identity and members[0]["id"] != identity["id"]:
                        reason = "UID和显示ID指向不同主体"
                    identity = identity or members[0]
                if existing_uid_raw and existing_uid is None:
                    reason = "现存UID格式异常，不能按显示ID重新绑定"
                if candidate_uid and existing_uid and candidate_uid != existing_uid:
                    reason = "源UID与显示ID已绑定的UID冲突"
        if not reason and not identity and not directory and prevent_new_directory_reason:
            reason = prevent_new_directory_reason
        if not reason and not identity and not directory and not record["verified_uid"] and (not record["display_id"] or duplicate_display) and not (allow_uid_directory and candidate_uid):
            reason = "同平台显示ID重复且UID无法独立确认，暂不合并" if duplicate_display else "没有可安全建档的已核验UID或独立显示ID"
        if reason:
            row.update(status="review", reason=reason, source_record={key: record[key] for key in ("raw", "comment", "metadata")})
            continue
        account = accounts.get(identity["account_id"]) if identity else None
        old_raw = _raw_json(directory["raw_json"]) if directory else {}
        before = _base_fields(directory, account, identity, old_raw)
        fields, pending = _accept(record, before, duplicate_display=duplicate_display)
        if directory and directory["account_id"] is not None and account is None:
            raise ValueError("Directory has a missing or unresolved account identity")
        changes = [{"field": key, "before": before[key], "after": fields[key]} for key in HEADERS if fields[key] != before[key]]
        row["pending_fields"], row["changes"] = pending, changes
        plans.append((record, row, directory, account, identity, old_raw, fields))

    target_counts = Counter(("account", plan[4]["account_id"]) if plan[4] else ("directory", plan[2]["id"])
                            for plan in plans if plan[4] or plan[2])
    safe_plans = []
    for plan in plans:
        record, row, directory, _, identity, _, _ = plan
        target = ("account", identity["account_id"]) if identity else ("directory", directory["id"]) if directory else None
        if target is not None and target_counts[target] > 1:
            row.update(status="review", reason="同批多行指向同一现存主体或目录，需先核对", changes=[],
                       source_record={key: record[key] for key in ("raw", "comment", "metadata")})
        else:
            safe_plans.append(plan)
    plans = safe_plans
    before_changes = connection.total_changes
    new_accounts = 0
    existing_account_directories = 0
    connection.execute("SAVEPOINT account_summary_import")
    try:
        for record, row, directory, account, identity, old_raw, fields in plans:
            created = directory is None
            if created and identity is not None:
                existing_account_directories += 1
            if identity is None and record["verified_uid"]:
                cursor = connection.execute("INSERT INTO accounts(phone,phone_normalized,operator_name,enabled,created_at,updated_at) VALUES('',NULL,'',0,?,?)", (imported_at, imported_at))
                account_id = int(cursor.lastrowid)
                new_accounts += 1
                account = {"id": account_id, "phone": "", "phone_normalized": None, "operator_name": "", "enabled": 0}
                cursor = connection.execute("INSERT INTO account_platform_identities(account_id,platform,uid,nickname,real_name_status,source,created_at,updated_at) VALUES(?,?,?,'','unknown','account_summary',?,?)", (account_id, record["platform"], record["uid"], imported_at, imported_at))
                identity = {"id": int(cursor.lastrowid), "account_id": account_id, "platform": record["platform"], "uid": record["uid"], "nickname": "", "real_name_status": "unknown"}
            if account:
                patch = {}
                for header, key in (("手机号", "phone"), ("运营人员", "operator_name")):
                    if header not in row["pending_fields"] and _known(record["raw"][header]):
                        patch[key] = _text(fields[header])
                if "phone" in patch:
                    digits = re.sub(r"\D", "", patch["phone"])
                    patch["phone_normalized"] = digits if 7 <= len(digits) <= 20 else None
                _update(connection, "accounts", account, patch, imported_at)
            if identity:
                patch = {}
                if "账号名称" not in row["pending_fields"] and _known(record["raw"]["账号名称"]):
                    patch["nickname"] = _text(fields["账号名称"])
                if "是否实名" not in row["pending_fields"] and record["raw"]["是否实名"] in {"是", "否"}:
                    patch["real_name_status"] = "yes" if fields["是否实名"] == "是" else "no"
                _update(connection, "account_platform_identities", identity, patch, imported_at)
            previous = old_raw.get("account_summary", {})
            previous = previous if isinstance(previous, dict) else {}
            imports = list(previous.get("imports", []))
            imports.append({"source_sha256": sha, "source_sheet": _text(payload.get("sheet")), "source_row": record["sourceRow"], "record_sha256": record["record_sha256"]})
            history = list(previous.get("history", []))
            if directory:
                history.append({"source_sha256": directory["source_sha256"], "source_sheet": directory["source_sheet"],
                    "source_row": directory["source_row"], "previous_raw": old_raw if not previous else {k: v for k, v in previous.items() if k not in {"history", "imports"}},
                    "changes": row["changes"], "replaced_at": imported_at})
            raw = dict(old_raw)
            raw.update({key: value for key, value in fields.items() if _known(value)})
            raw["account_summary"] = {"contract": CONTRACT, "fields": fields, "raw": record["raw"],
                "source_sha256": sha, "source_name": _text(payload.get("source")), "source_sheet": _text(payload.get("sheet")),
                "source_row": record["sourceRow"], "comment": record["comment"], "metadata": record["metadata"],
                "history": history, "imports": imports, "pending_fields": row["pending_fields"], "imported_at": imported_at}
            account_id = identity["account_id"] if identity else None
            status = STATUSES.get(_text(fields["更新状态"]), (directory or {}).get("account_status", "unmarked"))
            group = next((key for key, label in ACCOUNT_GROUPS.items() if label == fields["质量标签"]), (directory or {}).get("account_group", "unknown"))
            direction = next((key for key, label in BUSINESS_DIRECTIONS.items() if label == fields["业务标签"]), (directory or {}).get("business_direction", "unknown"))
            directory_uid_value = identity["uid"] if identity else (directory or {}).get("uid")
            if identity is None and "uid" not in row["pending_fields"] and record["uid"]:
                # A clear source UID remains a matching key even before it is
                # independently verified. It does not admit capture or create
                # a subject; later verified imports must reuse this directory.
                directory_uid_value = record["uid"]
            patch = {"account_id": account_id, "platform": record["platform"], "uid": directory_uid_value,
                "nickname": _text(fields["账号名称"]), "display_account_id": _text(fields[DISPLAY_ID]),
                "phone": _text(fields["手机号"]), "operator_name": _text(fields["运营人员"]),
                "account_status": status, "account_group": group, "business_direction": direction,
                "identity_status": directory["identity_status"] if directory and directory["account_id"] is not None else "uid_unverified" if identity else "identity_missing",
                "raw_json": _json(raw)}
            if prepare_only and directory and "locator_revision" in directory:
                # A redundant UID+handle submission adds an assertion to verify;
                # it cannot revise the current locator and invalidate its owner.
                if patch["display_account_id"] != directory["display_account_id"]:
                    row["pending_fields"][DISPLAY_ID] = "补充的展示号等待主页证据核验，保留当前目录定位"
                    patch["display_account_id"] = directory["display_account_id"]
            if directory:
                _update(connection, "account_directory_rows", directory, patch, imported_at)
                directory_id = directory["id"]
            else:
                values = {"source_sha256": sha, "source_name": _text(payload.get("source")), "source_sheet": _text(payload.get("sheet")),
                    "source_row": record["sourceRow"], **patch, "imported_at": imported_at, "updated_at": imported_at}
                cursor = connection.execute("INSERT INTO account_directory_rows(" + ",".join(values) + ") VALUES(" + ",".join("?" for _ in values) + ")", tuple(values.values()))
                directory_id = int(cursor.lastrowid)
            row.update(status="added" if created else "updated", directory_row_id=directory_id,
                       account_id=account_id, identity_id=identity["id"] if identity else None)
        connection.execute("RELEASE account_summary_import")
    except Exception:
        connection.execute("ROLLBACK TO account_summary_import")
        connection.execute("RELEASE account_summary_import")
        raise
    counts = {key: sum(row["status"] == key for row in result_rows) for key in ("added", "updated", "unchanged", "review", "asset")}
    return {"status": "unchanged" if not plans else "applied", "source_sha256": sha, "total": len(records),
            "counts": counts, "rows": result_rows, "writes": connection.total_changes - before_changes,
            "new_account_count": new_accounts, "new_directory_count": counts["added"],
            "existing_account_directory_added_count": existing_account_directories}


def has_account_intake(connection: sqlite3.Connection) -> bool:
    return connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='account_intake_requests'").fetchone() is not None


def _require_intake(connection: sqlite3.Connection) -> None:
    if not connection.in_transaction:
        raise ValueError("Account intake requires the caller transaction")
    if not has_account_intake(connection):
        raise ValueError("账号接入需要先完成离线数据库升级")


def normalize_intake(value: Mapping[str, Any]) -> dict[str, Any]:
    """Preserve user input; distinguish canonical identity from typed locators."""
    if not isinstance(value, Mapping):
        raise ValueError("账号输入必须是对象")
    result = dict(value)
    for key in ("uid", "display_account_id", "phone", "profile_url", "operator_name", "nickname"):
        item = result.get(key)
        if item is not None and not isinstance(item, str):
            raise ValueError(f"{key} 必须按文本提交，不能转换长标识或前导零")
        result[key] = _text(item)
    from .platform_adapters import normalize_account_input
    normalized = normalize_account_input(result)
    result.update(normalized)
    for key in ("uid","profile_url","display_account_id"):
        result[key] = result.get(key) or ""
    # Preserve typed references in the same normalized contract used by workers.
    if normalized["references"].get("eid"):
        result["eid"] = normalized["references"]["eid"]
    if normalized["references"].get("channel_id") and not result["display_account_id"]:
        result["display_account_id"] = normalized["references"]["channel_id"]
    if result.get("account_status") not in {None, "", "daily", "weekly", "paused", "unmarked"}:
        raise ValueError("账号状态无效")
    return result


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def preparation_key(value: Mapping[str, Any]) -> str:
    """The strongest canonical locator, independent of redundant input fields."""
    from .platform_adapters import valid_uid
    platform = value["platform"]
    if valid_uid(platform, value.get("uid")):
        return _fingerprint({"platform": platform, "uid": value["uid"]})
    references = value.get("references") or {}
    for kind in ("sec_user_id", "eid", "channel_id", "share_url"):
        locator = references.get(kind) or value.get(kind)
        if locator:
            return _fingerprint({"platform": platform, "locator_kind": kind, "locator": locator})
    for kind in ("profile_url", "display_account_id"):
        if value.get(kind):
            return _fingerprint({"platform": platform, "locator_kind": kind, "locator": value[kind]})
    return _fingerprint({"platform": platform, "unresolved": value.get("uid") or ""})


def locator_fields(value: Mapping[str, Any]) -> dict[str, Any]:
    return {"platform": value["platform"], "uid": value.get("uid") or "",
            "display_account_id": value.get("display_account_id") or "",
            "profile_url": value.get("profile_url") or "", "references": dict(value.get("references") or {})}


class DirectoryIdentityConflict(ValueError):
    def __init__(self, code: str, message: str):
        self.code = self.error_code = code
        super().__init__(message)


def update_directory_identity(connection: sqlite3.Connection, *, directory_row_id: int, request_key: str,
                              expected_locator_sha256: str, value: dict, source: dict, at: str) -> dict[str, Any]:
    """CAS one directory locator and submit its ordinary preparation revision."""
    _require_intake(connection)
    columns = {row[1] for row in connection.execute("PRAGMA table_info(account_directory_rows)")}
    if not {"locator_json", "locator_revision"} <= columns:
        raise DirectoryIdentityConflict("directory_identity_upgrade_required", "补充身份需要先完成数据库升级。")
    normalized = normalize_intake(value)
    submitted = {"directory_row_id": directory_row_id, "expected_locator_sha256": expected_locator_sha256,
                 "value": normalized, "source": source}
    previous = _rows(connection, "SELECT * FROM account_intake_requests WHERE request_key=?", (request_key,))
    if previous:
        if json.loads(previous[0]["source_json"]).get("identity_submission") != submitted:
            raise DirectoryIdentityConflict("directory_request_conflict", "同一请求编号不能用于不同身份修改。")
        return _intake_result(connection, previous[0], replayed=True)
    entries = _rows(connection, "SELECT * FROM account_directory_rows WHERE id=?", (directory_row_id,))
    if len(entries) != 1 or type(directory_row_id) is not int:
        raise DirectoryIdentityConflict("directory_missing", "原账号目录行不存在，请刷新账号页。")
    directory = entries[0]
    from .account_directory_reconciliation import directory_locator_snapshot, _supersede
    if _fingerprint(directory_locator_snapshot(directory)) != expected_locator_sha256:
        raise DirectoryIdentityConflict("directory_locator_changed", "账号定位已被修改，请刷新后重新补充。")
    if normalized["platform"] != directory["platform"]:
        raise DirectoryIdentityConflict("directory_platform_conflict", "补充身份不能改变原账号的平台。")
    if directory["account_id"] and not normalized.get("uid"):
        normalized = normalize_intake({**normalized, "uid": directory["uid"]})
    if directory["account_id"] and normalized.get("uid") and directory["uid"] != normalized["uid"]:
        raise DirectoryIdentityConflict("directory_identity_conflict", "该目录已有平台身份，不能改绑其他 UID。")
    matches = _rows(connection, """SELECT d.id FROM account_directory_rows d
        LEFT JOIN account_platform_identities i ON i.account_id=d.account_id AND i.platform=d.platform
        WHERE d.platform=? AND d.id<>? AND (?<>'' AND (d.uid=? OR i.uid=?))""",
        (normalized["platform"], directory_row_id, normalized["uid"], normalized["uid"], normalized["uid"]))
    if matches:
        raise DirectoryIdentityConflict("directory_identity_conflict", "该平台 UID 已属于另一目录行，请先核对身份冲突。")
    for kind, locator in normalized.get("references", {}).items():
        if kind == "share_url":
            continue
        owned = _rows(connection, """SELECT d.id FROM account_provider_references r
            JOIN account_platform_identities i ON i.id=r.account_identity_id
            JOIN account_directory_rows d ON d.account_id=i.account_id
            WHERE i.platform=? AND r.reference_kind=? AND r.reference_value=?
              AND r.source_raw_response_id IS NOT NULL AND d.id<>?""", (normalized["platform"], kind, locator, directory_row_id))
        if owned:
            raise DirectoryIdentityConflict("directory_identity_conflict", "该主页定位已属于另一目录行，请先核对身份冲突。")
    if directory["account_id"]:
        identities = _rows(connection, "SELECT id FROM account_platform_identities WHERE account_id=? AND platform=?", (directory["account_id"], directory["platform"]))
        if len(identities) != 1:
            raise DirectoryIdentityConflict("directory_identity_conflict", "原目录主体身份不唯一。")
        _assert_input_references(connection, identities[0]["id"], normalized)
    connection.execute("SAVEPOINT directory_identity_update")
    try:
        current_locator = locator_fields(normalized)
        changed = connection.execute("""UPDATE account_directory_rows SET uid=?,display_account_id=?,locator_json=?,
            locator_revision=locator_revision+1,updated_at=? WHERE id=? AND locator_revision=?""",
            (normalized["uid"] or None, normalized["display_account_id"], _json(current_locator), at,
             directory_row_id, directory["locator_revision"])).rowcount
        if changed != 1:
            raise DirectoryIdentityConflict("directory_locator_changed", "账号定位已被修改，请刷新后重试。")
        _supersede(connection, directory_row_id, at=at)
        result = submit_account_intake(connection, request_key=request_key, value=normalized,
            source={"kind": "directory_identity_update", "directory_row_id": directory_row_id,
                    "identity_submission": submitted, "actor": source.get("actor", "api-operator")}, at=at)
        connection.execute("RELEASE directory_identity_update")
        return result
    except Exception:
        connection.execute("ROLLBACK TO directory_identity_update")
        connection.execute("RELEASE directory_identity_update")
        raise


def _record_for_intake(value: Mapping[str, Any], source: Mapping[str, Any]) -> dict[str, Any]:
    original = source.get("record")
    raw = dict(original.get("raw", {})) if isinstance(original, Mapping) else {}
    raw = {key: raw.get(key) for key in HEADERS}
    raw["平台"] = next(label for label, platform in PLATFORMS.items() if platform == value["platform"])
    for key, header in (("uid", "uid"), ("display_account_id", DISPLAY_ID), ("nickname", "账号名称"),
                        ("phone", "手机号"), ("operator_name", "运营人员")):
        if value.get(key):
            raw[header] = value[key]
    # Alias-shaped source UIDs are retained in source_json, not the UID column.
    if not value.get("uid"):
        raw["uid"] = None
    status = next((label for label, code in STATUSES.items() if code == value.get("account_status")), None)
    if status:
        raw["更新状态"] = status
    for key, header, labels in (("account_group", "质量标签", ACCOUNT_GROUPS), ("business_direction", "业务标签", BUSINESS_DIRECTIONS)):
        if value.get(key) in labels and value[key] != "unknown":
            raw[header] = labels[value[key]]
    metadata = dict(original.get("metadata", {})) if isinstance(original, Mapping) else {}
    constraints = source.get("intake_constraints", {})
    if isinstance(constraints, Mapping) and constraints.get("display_account_id") in {"batch_duplicate", "unresolved_locator", "source_display_unverified"}:
        # Keep the exact original record in source_json. This derived import
        # projection retains the disputed value but cannot match/write by it.
        metadata["conflict_fields"] = sorted(set(metadata.get("conflict_fields", [])) | {DISPLAY_ID})
        metadata["intake_constraints"] = dict(constraints)
    metadata.update(enrichment_status="unverified", verified_uid=None)
    # A supplied display handle is an input locator, never evidence. The old
    # workbook-specific KS heuristic is not needed in the common input contract.
    if value["platform"] == "kuaishou" and value.get("display_account_id"):
        metadata["enrichment_profile"] = {"ID": value["display_account_id"]}
    return {"sourceRow": int(source.get("row") or 1), "raw": raw,
            "comment": original.get("comment", "") if isinstance(original, Mapping) else str(source.get("comment") or ""),
            "metadata": metadata}


def _intake_result(connection: sqlite3.Connection, row: Mapping[str, Any], *, replayed: bool = False) -> dict[str, Any]:
    result = json.loads(row["result_json"])
    technical = preparation_status(row, _latest_preparation_work(connection, int(row["id"])).get(row["id"]))
    locator = {}
    if row["directory_row_id"]:
        directories = _rows(connection, "SELECT * FROM account_directory_rows WHERE id=?", (row["directory_row_id"],))
        if directories:
            from .account_directory_reconciliation import directory_locator_snapshot
            locator = {"locator_sha256": _fingerprint(directory_locator_snapshot(directories[0])),
                       "locator_revision": directories[0].get("locator_revision", 0)}
    return {**result, **locator, "message":technical["message"], "preparation":technical, "intake_id": row["id"], "request_id": row["request_key"],
            "directory_row_id": row["directory_row_id"], "account_id": row["account_id"],
            "account_identity_id": row["account_identity_id"], "platform": row["platform"],
            "preparation_key": row["preparation_key"], "replayed": replayed}


def _assert_input_references(connection: sqlite3.Connection, identity_id: int, value: Mapping[str, Any]) -> None:
    for kind, locator in value.get("references", {}).items():
        if kind == "share_url":
            continue
        prior = _rows(connection,"SELECT reference_value FROM account_provider_references WHERE account_identity_id=? AND lower(provider)='tikhub' AND reference_kind=?",(identity_id,kind))
        if prior and any(row["reference_value"] != locator for row in prior):
            raise ValueError("输入主页定位与该 UID 已绑定的定位不一致")


def _save_request(connection: sqlite3.Connection, *, request_key: str, value: dict, source: dict, at: str,
                  outcome: dict | None = None) -> dict[str, Any]:
    fingerprint = _fingerprint(value)
    old = _rows(connection, "SELECT * FROM account_intake_requests WHERE request_key=?", (request_key,))
    if old:
        if json.loads(old[0]["result_json"]).get("submitted_input_sha256",old[0]["input_sha256"]) != fingerprint or json.loads(old[0]["source_json"]) != source:
            raise ValueError("同一接入请求编号不能用于不同内容")
        return _intake_result(connection, old[0], replayed=True)
    outcome = outcome or {}
    aid, iid, did = outcome.get("account_id"), outcome.get("identity_id"), outcome.get("directory_row_id")
    locator_contract = "locator_revision" in {row[1] for row in connection.execute("PRAGMA table_info(account_directory_rows)")}
    verification_required = source.get("kind") == "directory_identity_update"
    if iid:
        identity = _rows(connection, "SELECT platform,uid FROM account_platform_identities WHERE id=?", (iid,))[0]
        if identity["platform"] != value["platform"] or value.get("uid") and value["uid"] != identity["uid"]:
            raise ValueError("接入结果身份不一致")
        if locator_contract and not value.get("uid"):
            # Mutable display aliases must still resolve by the supplied input.
            # The known account_id fences the result; it is not a substitute for
            # the requested profile lookup's identity evidence.
            verification_required = True
        else:
            value = {**value, "uid": identity["uid"]}
        if locator_contract:
            current = _rows(connection, "SELECT display_account_id FROM account_directory_rows WHERE id=?", (did,)) if did else []
            if value.get("display_account_id") and (not current or current[0]["display_account_id"] != value["display_account_id"]):
                verification_required = True
            for kind, locator in value.get("references", {}).items():
                if kind == "share_url":
                    continue
                if not connection.execute("SELECT 1 FROM account_provider_references WHERE account_identity_id=? AND reference_kind=? AND reference_value=? AND source_raw_response_id IS NOT NULL",
                                          (iid, kind, locator)).fetchone():
                    verification_required = True
    status = "conflict" if outcome.get("status") in {"review", "asset"} else "blocked" if outcome.get("status") == "blocked" else "accepted"
    reason = outcome.get("reason")
    if iid and status == "accepted":
        _assert_input_references(connection, iid, value)
        from .account_capture_eligibility import identity_capture_evidence
        try:
            evidence = identity_capture_evidence(connection, int(iid))
        except sqlite3.OperationalError:
            # Minimal import fixtures and incomplete historical databases cannot
            # prove readiness. Absence never becomes a positive admission.
            evidence = {"eligible": False}
        if evidence.get("eligible") and not verification_required:
            status = "ready"
    result = {"status": status, "submitted_input_sha256":fingerprint, "action": outcome.get("status", "accepted"), "uid": value.get("uid", ""),
              "account_status": value.get("account_status") or "unmarked", "reason": reason,
              "activation_status": "preparing" if status == "accepted" else "prepared" if status == "ready" else "blocked",
              "message": "资料已接收，系统将自动准备账号并接入采集。" if status == "accepted" else
                         "账号资料已保存，已有有效主页凭据，后续按采集计划执行。" if status == "ready" else
                         "资料已保留，暂未绑定账号：" + str(reason or "身份存在冲突"),
              "pending_fields": outcome.get("pending_fields", {}), "changes": outcome.get("changes", [])}
    if status == "blocked":
        result["preparation_error"] = reason or "locator_resolution_required"
    if did:
        from .account_directory_reconciliation import directory_locator_snapshot
        directory = _rows(connection, "SELECT * FROM account_directory_rows WHERE id=?", (did,))[0]
        if outcome.get("status") == "added" and "locator_revision" in directory and not directory["locator_revision"]:
            connection.execute("UPDATE account_directory_rows SET locator_json=?,locator_revision=1 WHERE id=?",
                               (_json(locator_fields(value)), did))
            directory = _rows(connection, "SELECT * FROM account_directory_rows WHERE id=?", (did,))[0]
        snapshot = directory_locator_snapshot(directory)
        result.update(directory_locator_snapshot=snapshot, directory_locator_sha256=_fingerprint(snapshot))
    cursor = connection.execute("""INSERT INTO account_intake_requests
        (request_key,input_sha256,preparation_key,platform,input_json,source_json,directory_row_id,
         account_id,account_identity_id,result_json,created_at,updated_at,completed_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""", (request_key,_fingerprint(value),preparation_key(value),value["platform"],_json(value),_json(source),did,
        aid,iid,_json(result),at,at,at if status != "accepted" else None))
    return _intake_result(connection, _rows(connection,"SELECT * FROM account_intake_requests WHERE id=?",(cursor.lastrowid,))[0])


def directory_intake_outcome(connection: sqlite3.Connection, directory_row_id: int,
                             normalized: Mapping[str, Any]) -> dict[str, Any]:
    """Validate one existing directory binding without writing or performing I/O."""
    did = directory_row_id
    entries = _rows(connection,"SELECT * FROM account_directory_rows WHERE id=?",(did,)) if type(did) is int else []
    if len(entries) != 1 or entries[0]["platform"] != normalized["platform"]:
        raise ValueError("存量接入来源目录不存在或平台不一致")
    directory = entries[0]
    matches = _rows(connection,"""SELECT d.id FROM account_directory_rows d
        LEFT JOIN account_platform_identities i ON i.account_id=d.account_id
        WHERE d.platform=? AND (?<>'' AND (d.uid=? OR i.uid=?))""",
        (normalized["platform"],normalized["uid"],normalized["uid"],normalized["uid"]))
    if any(row["id"] != did for row in matches):
        raise ValueError("存量定位标识还指向其他目录，不能强行绑定")
    members = _rows(connection,"SELECT * FROM account_platform_identities WHERE account_id=?",(directory["account_id"],)) if directory["account_id"] else []
    if directory["account_id"] and (len(members) != 1 or members[0]["platform"] != normalized["platform"]
            or members[0]["uid"] != directory["uid"]):
        raise ValueError("存量目录与主体身份不一致")
    if normalized["uid"] and directory["uid"] not in {None,"",normalized["uid"]}:
        raise ValueError("存量 UID 与接入输入不一致")
    if members:
        _assert_input_references(connection, members[0]["id"], normalized)
    return {"status":"unchanged","directory_row_id":did,"account_id":directory["account_id"],
               "identity_id":members[0]["id"] if members else None,"changes":[],"pending_fields":{}}


def submit_account_intake(connection: sqlite3.Connection, *, request_key: str, value: dict, source: dict, at: str) -> dict[str, Any]:
    """Persist one common intake command. No network, commit, scheduler or DDL."""
    _require_intake(connection)
    if not isinstance(request_key, str) or not request_key or len(request_key) > 256 or not at or not isinstance(source, dict):
        raise ValueError("接入请求编号、来源或时间无效")
    normalized = normalize_intake(value)
    existing = _rows(connection, "SELECT * FROM account_intake_requests WHERE request_key=?", (request_key,))
    if existing:
        if json.loads(existing[0]["result_json"]).get("submitted_input_sha256",existing[0]["input_sha256"]) != _fingerprint(normalized) or json.loads(existing[0]["source_json"]) != source:
            raise ValueError("同一接入请求编号不能用于不同内容")
        return _intake_result(connection, existing[0], replayed=True)
    connection.execute("SAVEPOINT account_intake_submit")
    try:
        from .platform_adapters import next_profile_request, PlatformAdapterError
        preparation_error = None
        try:
            next_profile_request(normalized)
        except PlatformAdapterError as error:
            preparation_error = error.error_code
        # URL/eid-only input is durably accepted before an identity exists. Do
        # not invent a directory UID or subject just to satisfy queue targets.
        outcome = None
        if source.get("kind") in {"directory_backfill", "directory_identity_update"}:
            # Submitting an existing row for preparation is not another import.
            # Preserve raw_json, source time and all user fields byte-for-byte.
            outcome = directory_intake_outcome(connection, source.get("directory_row_id"), normalized)
        elif normalized.get("uid") or normalized.get("display_account_id"):
            projected_source = source
            if preparation_error:
                # Unsupported source IDs remain verbatim in the journal. They
                # cannot match or overwrite a directory's public display ID.
                projected_source = {**source, "intake_constraints": {
                    **source.get("intake_constraints", {}), "display_account_id": "unresolved_locator"}}
            record = _record_for_intake(normalized, projected_source)
            result = _apply_summary(connection, {"sha256": _fingerprint({"request_key": request_key, "source": source}),
                "source": source.get("name", source.get("kind", "account_intake")), "sheet": source.get("sheet", ""),
                "records": [record]}, imported_at=at, prepare_only=True, allow_uid_directory=True,
                prevent_new_directory_reason=preparation_error)
            outcome = result["rows"][0]
        if preparation_error and (not outcome or outcome.get("reason") == preparation_error):
            outcome = {**(outcome or {}), "status": "blocked", "reason": preparation_error}
        result = _save_request(connection, request_key=request_key, value=normalized, source=source, at=at, outcome=outcome)
        connection.execute("RELEASE account_intake_submit")
        return result
    except Exception:
        connection.execute("ROLLBACK TO account_intake_submit")
        connection.execute("RELEASE account_intake_submit")
        raise


def preparation_inputs(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """Ready-to-prepare requests; the existing planner owns leasing and dedupe."""
    if not has_account_intake(connection):
        return []
    rows = _rows(connection, "SELECT * FROM account_intake_requests WHERE completed_at IS NULL ORDER BY id")
    return [{**row, "value": json.loads(row["input_json"]), "source": json.loads(row["source_json"])} for row in rows
            if json.loads(row["result_json"]).get("status") == "accepted"]


def import_account_summary(connection: sqlite3.Connection, payload: Mapping[str, Any], *, imported_at: str) -> dict[str, Any]:
    """Workbook adapter; keep batch conflict planning and exact original sources."""
    if not has_account_intake(connection):
        return _apply_summary(connection, payload, imported_at=imported_at)
    _require_intake(connection)
    sha, records = _input(payload)
    # Batch conflicts must be found BEFORE per-row commands to avoid later-row
    # overwrite. All offending original rows are retained as blocked requests.
    uid_counts = Counter((r["platform"],r["uid"]) for r in records if r["uid"])
    display_counts = Counter((r["platform"],r["display_id"]) for r in records if r["display_id"])
    targets = {}
    for record in records:
        duplicate_display = bool(record["display_id"] and display_counts[(record["platform"],record["display_id"])] > 1)
        # An ambiguous display handle cannot turn a different UID into the
        # same account target, or veto a unique canonical UID's own target.
        display_locator = None if duplicate_display else record["display_id"]
        found = _rows(connection,"""SELECT d.id,d.account_id FROM account_directory_rows d
             LEFT JOIN account_platform_identities i ON i.account_id=d.account_id AND i.platform=d.platform
             WHERE d.platform=? AND ? IS NOT NULL AND (d.uid=? OR i.uid=?)""",
             (record["platform"],record["uid"],record["uid"],record["uid"]))
        if not found and record["uid"]:
            identities = _rows(connection,"SELECT id FROM account_platform_identities WHERE platform=? AND uid=?",
                               (record["platform"],record["uid"]))
            if len(identities) == 1:
                targets[record["sourceRow"]] = ("identity",identities[0]["id"])
                continue
        if not found and display_locator:
            display_rows = _rows(connection,"""SELECT d.id,d.account_id,d.uid,i.uid AS identity_uid
                FROM account_directory_rows d LEFT JOIN account_platform_identities i
                ON i.account_id=d.account_id AND i.platform=d.platform
                WHERE d.platform=? AND d.display_account_id=?""", (record["platform"],display_locator))
            found = [row for row in display_rows if not record["uid"] or
                all(value in {None,"",record["uid"]} for value in (row["uid"],row["identity_uid"]))]
        if len(found) == 1:
            targets[record["sourceRow"]] = ("directory",found[0]["id"])
    target_counts = Counter(targets.values())
    rows = []
    before = connection.total_changes
    for record in records:
        raw = record["raw"]
        source = {"kind": "excel", "name": _text(payload.get("source")), "sha256": sha,
                  "sheet": _text(payload.get("sheet")), "row": record["sourceRow"],
                  "record": {key: record[key] for key in ("raw","comment","metadata")}}
        key = "excel:" + _fingerprint({"sha256":sha,"sheet":source["sheet"],"row":source["row"]})
        value = {"platform":record["platform"],"uid":_text(raw.get("uid")) if _known(raw.get("uid")) else "",
                 "display_account_id":_text(raw.get(DISPLAY_ID)) if _known(raw.get(DISPLAY_ID)) else ""}
        duplicate_display = bool(record["display_id"] and display_counts[(record["platform"],record["display_id"])] > 1)
        if duplicate_display and record["uid"] and uid_counts[(record["platform"],record["uid"])] == 1:
            value["display_account_id"] = ""
            source["intake_constraints"] = {"display_account_id": "batch_duplicate"}
        if record["display_unverified"] and record["uid"]:
            value["display_account_id"] = ""
            source["intake_constraints"] = {"display_account_id": "source_display_unverified"}
        for header, name in (("账号名称","nickname"),("手机号","phone"),("运营人员","operator_name")):
            if _known(raw.get(header)):
                value[name] = _text(raw[header])
        if _text(raw.get("更新状态")) in STATUSES:
            value["account_status"] = STATUSES[_text(raw["更新状态"])]
        reason = None
        if record["sourceRow"] in targets and target_counts[targets[record["sourceRow"]]] > 1:
            reason = "同批多行指向同一现存主体或目录，不以后行覆盖前行"
        elif record["uid"] and uid_counts[(record["platform"],record["uid"])] > 1:
            reason = "同批重复平台 UID，原始记录均保留，不以后行覆盖前行"
        elif duplicate_display and not record["uid"]:
            reason = "同批账号 ID 重复，原始记录均保留，不按显示号合并"
        elif any(field in record["metadata"].get("conflict_fields",[]) for field in ("uid",DISPLAY_ID)):
            reason = "源表定位字段存在冲突"
        try:
            normalized = normalize_intake(value)
            if reason:
                response = _save_request(connection,request_key=key,value=normalized,source=source,at=imported_at,
                                         outcome={"status":"review","reason":reason})
            else:
                response = submit_account_intake(connection,request_key=key,value=value,source=source,at=imported_at)
        except ValueError as error:
            # Preserve even unusable/asset rows in the request journal. A blank
            # platform is not a fabricated account identity or an executable job.
            if record["platform"] is None:
                rows.append({"source_row":source["row"],"status":"asset" if record["metadata"].get("account_record_count") == 0 else "review",
                             "directory_row_id":None,"account_id":None,"identity_id":None,"intake_id":None,
                             "reason":str(error),"source_record":source["record"],"changes":[],"pending_fields":{}})
                continue
            retained = {**value,"platform":record["platform"]}
            response = _save_request(connection,request_key=key,value=retained,source=source,at=imported_at,
                                     outcome={"status":"review","reason":str(error)})
        rows.append({"source_row":source["row"],"status":"unchanged" if response["replayed"] else
                     "review" if response["status"] == "conflict" else response["action"],
                     "directory_row_id":response["directory_row_id"],"account_id":response["account_id"],
                     "identity_id":response["account_identity_id"],"intake_id":response["intake_id"],
                     "reason":response.get("reason"),"changes":response.get("changes",[]),
                     "pending_fields":response.get("pending_fields",{})})
    counts = {key:sum(row["status"] == key for row in rows) for key in ("added","updated","unchanged","review","asset","accepted","blocked")}
    return {"status":"unchanged" if connection.total_changes == before else "applied", "source_sha256":sha,
            "total":len(rows),"rows":rows,"counts":counts,"writes":connection.total_changes-before,
            "new_account_count":0,"new_directory_count":counts["added"],"existing_account_directory_added_count":0}


def _bind_prepared_directory(connection: sqlite3.Connection, directory: dict[str, Any],
                             identity: dict[str, Any] | None, profile: Mapping[str, Any], *, at: str) -> dict[str, Any]:
    """Bind the explicit target; never rematch it by mutable operator fields."""
    uid, platform, did = profile["uid"], profile["platform"], directory["id"]
    occupied = _rows(connection, "SELECT id FROM account_directory_rows WHERE platform=? AND uid=? AND id<>?",
                     (platform, uid, did))
    if identity:
        occupied += _rows(connection, "SELECT id FROM account_directory_rows WHERE account_id=? AND id<>?", (identity["account_id"], did))
    if occupied:
        raise DirectoryIdentityConflict("directory_identity_conflict", "准备结果的 UID 已属于另一目录行，不能自动合并。")
    if identity is None:
        if directory["account_id"] is not None:
            raise DirectoryIdentityConflict("directory_identity_conflict", "原目录的账号身份无法核对。")
        phone = directory["phone"]
        digits = re.sub(r"\D", "", phone)
        aid = connection.execute("INSERT INTO accounts(phone,phone_normalized,operator_name,enabled,created_at,updated_at) VALUES(?,?,?,0,?,?)",
            (phone, digits if 7 <= len(digits) <= 20 else None, directory["operator_name"], at, at)).lastrowid
        iid = connection.execute("""INSERT INTO account_platform_identities
            (account_id,platform,uid,nickname,real_name_status,source,created_at,updated_at)
            VALUES(?,?,?,?,'unknown','account_preparation',?,?)""",
            (aid, platform, uid, directory["nickname"] if _known(directory["nickname"]) else profile.get("nickname", ""), at, at)).lastrowid
    else:
        aid, iid = identity["account_id"], identity["id"]
        if directory["account_id"] not in {None, aid}:
            raise DirectoryIdentityConflict("directory_identity_conflict", "准备结果不能改变原目录主体。")
    fields = {"account_id": aid, "uid": uid, "identity_status": "existing_verified"}
    if not _known(directory["nickname"]) and profile.get("nickname"):
        fields["nickname"] = profile["nickname"]
    if profile.get("display_account_id"):
        fields["display_account_id"] = profile["display_account_id"]
    if directory.get("locator_revision", 0):
        current = json.loads(directory["locator_json"])
        current.update(uid=uid, display_account_id=fields.get("display_account_id", directory["display_account_id"]))
        if _json(current) != directory["locator_json"]:
            fields.update(locator_json=_json(current), locator_revision=directory["locator_revision"] + 1)
    _update(connection, "account_directory_rows", directory, fields, at)
    return {"status": "updated", "directory_row_id": did, "account_id": aid, "identity_id": iid}


def _profile_confirms_input(value: Mapping[str, Any], profile: Mapping[str, Any]) -> bool:
    if value.get("uid") and value["uid"] != profile.get("uid"):
        return False
    references = profile.get("references", {})
    for kind, locator in value.get("references", {}).items():
        if kind != "share_url" and references.get(kind) != locator:
            return False
    return not value.get("display_account_id") or profile.get("display_account_id") == value["display_account_id"]


def apply_prepared_profile(connection: sqlite3.Connection, intake_id: int, normalized_profile: Mapping[str, Any],
                           raw_response_id: int, at: str) -> dict[str, Any]:
    """Bind an adapter-verified profile atomically without replacing identities.

    Caller must validate original bytes and the platform's complete identity
    chain before calling. This boundary independently checks request ownership,
    live binding, source IDs and reference conflicts. It never performs I/O.
    """
    _require_intake(connection)
    requests = _rows(connection,"SELECT * FROM account_intake_requests WHERE id=?",(intake_id,))
    if len(requests) != 1:
        raise ValueError("账号接入请求不存在")
    request = requests[0]
    existing_result = json.loads(request["result_json"])
    if request["completed_at"] is not None:
        if (existing_result.get("status") == "ready" and existing_result.get("raw_response_id") == raw_response_id
                and existing_result.get("profile") == dict(normalized_profile)):
            return _intake_result(connection,request,replayed=True)
        raise ValueError("接入请求已完成或已被阻断，不能覆盖结果")
    from .account_directory_reconciliation import validate_request_directory
    validate_request_directory(connection, request)
    value = json.loads(request["input_json"])
    profile = normalize_intake(dict(normalized_profile))
    uid, platform = profile["uid"], profile["platform"]
    if not uid or platform != request["platform"] or value.get("uid") and value["uid"] != uid:
        raise ValueError("主页返回与请求 UID 不一致")
    if not value.get("uid") and value.get("display_account_id") and profile.get("display_account_id") != value["display_account_id"]:
        raise ValueError("主页返回未精确确认输入的账号 ID")
    references = normalized_profile.get("references", {})
    if not isinstance(references, Mapping) or any(not isinstance(k,str) or not k or not isinstance(v,str) or not v for k,v in references.items()):
        raise ValueError("主页定位引用无效")
    raw_ids = set(normalized_profile.get("source_raw_response_ids", [])) | {raw_response_id}
    ref_sources = normalized_profile.get("reference_raw_response_ids", {})
    if not isinstance(ref_sources, Mapping) or any(key not in references or value not in raw_ids for key,value in ref_sources.items()):
        raise ValueError("定位引用缺少所属原始响应")
    if any(type(item) is not int or item <= 0 for item in raw_ids):
        raise ValueError("原始响应编号无效")
    raw_rows = _rows(connection,"SELECT * FROM provider_raw_responses WHERE id IN ("+",".join("?" for _ in raw_ids)+")",tuple(raw_ids))
    if len(raw_rows) != len(raw_ids) or any(row.get("intake_request_id") != intake_id for row in raw_rows):
        raise ValueError("原始响应不属于当前接入请求")
    old_directory = _rows(connection,"SELECT * FROM account_directory_rows WHERE id=?",(request["directory_row_id"],)) if request["directory_row_id"] else []
    if request["directory_row_id"] and not old_directory:
        raise ValueError("原接入目录已经不存在")
    if old_directory:
        current = old_directory[0]
        if current["platform"] != platform or current.get("uid") and current["uid"] != uid:
            raise ValueError("账号定位在准备期间已变化，旧任务不得写入")
        if request["account_id"] is not None and current["account_id"] != request["account_id"]:
            raise ValueError("账号绑定在准备期间已变化")
    matches = _rows(connection,"SELECT * FROM account_platform_identities WHERE platform=? AND uid=?",(platform,uid))
    if len(matches) > 1:
        raise ValueError("同一平台 UID 对应多个主体")
    identity = matches[0] if matches else None
    if identity and request["account_id"] is not None and identity["account_id"] != request["account_id"]:
        raise ValueError("准备结果指向不同账号主体")
    known_aid = identity["account_id"] if identity else request["account_id"]
    if any(row.get("account_id") is not None and row["account_id"] != known_aid for row in raw_rows):
        raise ValueError("原始响应已绑定其他账号")
    for kind, locator in references.items():
        sql = "SELECT r.*,i.platform AS identity_platform FROM account_provider_references r JOIN account_platform_identities i ON i.id=r.account_identity_id WHERE lower(r.provider)='tikhub' AND r.reference_kind=? AND (r.reference_value=? OR r.account_identity_id=?)"
        for old in _rows(connection,sql,(kind,locator,identity["id"] if identity else -1)):
            if old["identity_platform"] != platform:
                continue
            same_identity = identity is not None and old["account_identity_id"] == identity["id"]
            confirmed_display_change = (same_identity and kind == "display_account_id" and value.get("uid") == uid
                                        and locator == profile.get("display_account_id"))
            if not same_identity or (old["reference_value"] != locator and not confirmed_display_change):
                raise ValueError("已存在指向其他身份或不同定位的引用")
    connection.execute("SAVEPOINT account_intake_apply")
    try:
        # Existing directory manual fields may have changed while I/O ran. Only
        # enrich missing names here; source edits were applied at submission.
        source = json.loads(request["source_json"])
        merge_value = {**value, **profile} if not old_directory else {"platform":platform,"uid":uid}
        if profile.get("display_account_id"):
            merge_value["display_account_id"] = profile["display_account_id"]
        if profile.get("nickname") and (not old_directory or not _known(old_directory[0]["nickname"])):
            merge_value["nickname"] = profile["nickname"]
        record = _record_for_intake(merge_value, source if not old_directory else {})
        record["sourceRow"] = 1
        record["metadata"].update(enrichment_status="verified", verified_uid=uid,
            enrichment_profile={"uid":uid,"ID":profile.get("display_account_id","")},
            profile_preparation={"intake_request_id":intake_id,"raw_response_id":raw_response_id,
                                 "source_raw_response_ids":sorted(raw_ids),"metadata":normalized_profile.get("metadata",{})})
        if old_directory:
            outcome = _bind_prepared_directory(connection, old_directory[0], identity, profile, at=at)
        else:
            response = _apply_summary(connection,{"sha256":_fingerprint({"intake_id":intake_id,"raw_ids":sorted(raw_ids),"profile":dict(normalized_profile)}),
                                      "source":"account_preparation","sheet":"profile","records":[record]},
                                      imported_at=at,allow_uid_directory=True)
            outcome = response["rows"][0]
        if outcome["status"] == "review" or not outcome.get("identity_id"):
            raise ValueError(outcome.get("reason") or "主页未能唯一绑定账号")
        aid, iid, did = outcome["account_id"],outcome["identity_id"],outcome["directory_row_id"]
        if request["directory_row_id"] and did != request["directory_row_id"]:
            raise ValueError("主页匹配到了不同目录，不能合并历史目录")
        # Plain new intake can create its directory through the summary writer,
        # whose UID-only contract deliberately never marks an identity verified.
        # Complete that projection only inside this verified-profile savepoint,
        # after checking the exact resulting directory and identity together.
        bound = connection.execute("""SELECT 1 FROM account_directory_rows d
            JOIN account_platform_identities i ON i.id=? AND i.account_id=d.account_id
            WHERE d.id=? AND d.account_id=? AND d.platform=? AND d.uid=?
              AND i.platform=d.platform AND i.uid=d.uid""", (iid, did, aid, platform, uid)).fetchone()
        if bound is None:
            raise ValueError("准备结果与最终目录身份不一致")
        connection.execute("""UPDATE account_directory_rows SET identity_status='existing_verified',updated_at=?
            WHERE id=? AND identity_status<>'existing_verified'""", (at, did))
        # The original response target is immutable; canonical account binding
        # lives on the intake request and references, never rewrites raw rows.
        from .account_reference_storage import store_reference
        reference_replacements = []
        for kind, locator in references.items():
            old = _rows(connection,"SELECT * FROM account_provider_references WHERE account_identity_id=? AND lower(provider)='tikhub' AND reference_kind=?",(iid,kind))
            reference_raw = ref_sources.get(kind,raw_response_id)
            if len(old) > 1:
                raise ValueError("同一平台引用存在多个服务方拼写，不能确定当前证据")
            if old and (old[0]["source_raw_response_id"] != reference_raw or old[0]["reference_value"] != locator):
                replacement = {"reference_kind":kind,"previous_raw_response_id":old[0]["source_raw_response_id"],
                               "current_raw_response_id":reference_raw}
                if old[0]["reference_value"] != locator:
                    replacement.update(previous_value=old[0]["reference_value"],current_value=locator)
                reference_replacements.append(replacement)
            # Identity and locator equality were checked before this savepoint.
            # A fresh verified entity repairs stale/broken evidence pointers;
            # every historical raw entity and immutable index remains intact.
            if not old or old[0]["source_raw_response_id"] != reference_raw or old[0]["reference_value"] != locator:
                store_reference(connection,account_identity_id=iid,platform=platform,provider="tikhub",
                    reference_kind=kind,reference_value=locator,source_raw_response_id=reference_raw,
                    created_at=at,updated_at=at,update_existing=True)
        result = {"status":"ready","submitted_input_sha256":existing_result.get("submitted_input_sha256",request["input_sha256"]),"action":"prepared","uid":uid,"raw_response_id":raw_response_id,
                  "source_raw_response_ids":sorted(raw_ids),"reference_replacements":reference_replacements,"profile":dict(normalized_profile),
                  "activation_status":"prepared","message":"账号主页准备完成，后续自动进入内容采集计划。"}
        connection.execute("UPDATE account_intake_requests SET directory_row_id=?,account_id=?,account_identity_id=?,result_json=?,updated_at=?,completed_at=? WHERE id=?",(did,aid,iid,_json(result),at,at,intake_id))
        # Equivalent pending requests from another entry point share the same
        # successful preparation. Their source journals remain separate.
        siblings = _rows(connection,"SELECT * FROM account_intake_requests WHERE platform=? AND completed_at IS NULL AND id<>?",(platform,intake_id))
        for sibling in siblings:
            if preparation_key(json.loads(sibling["input_json"])) != preparation_key(value):
                continue
            if (sibling["account_id"] not in {None,aid} or sibling["directory_row_id"] not in {None,did}
                    or not _profile_confirms_input(json.loads(sibling["input_json"]), profile)):
                blocked = {**json.loads(sibling["result_json"]), "status": "blocked", "preparation_error": "identity_conflict",
                           "reason": "identity_conflict", "activation_status": "blocked"}
                connection.execute("UPDATE account_intake_requests SET result_json=?,updated_at=?,completed_at=? WHERE id=?",
                                   (_json(blocked), at, at, sibling["id"]))
                continue
            shared = {**result,"submitted_input_sha256":json.loads(sibling["result_json"]).get("submitted_input_sha256",sibling["input_sha256"]),"reused_from_intake_request_id":intake_id}
            connection.execute("UPDATE account_intake_requests SET directory_row_id=?,account_id=?,account_identity_id=?,result_json=?,updated_at=?,completed_at=? WHERE id=?",
                               (did,aid,iid,_json(shared),at,at,sibling["id"]))
        connection.execute("RELEASE account_intake_apply")
        return _intake_result(connection,_rows(connection,"SELECT * FROM account_intake_requests WHERE id=?",(intake_id,))[0])
    except Exception:
        connection.execute("ROLLBACK TO account_intake_apply")
        connection.execute("RELEASE account_intake_apply")
        raise


_PREPARATION_REASONS = {
    'identity_missing':'缺少可以准确定位的账号信息', 'invalid_uid':'UID 格式不正确',
    'identity_conflict':'输入与平台返回的账号身份不一致', 'display_id_resolution_unavailable':'账号 ID 暂未解析成功',
    'operation_price_unverified':'接口价格尚未核实，任务未发送', 'preparation_policy_unavailable':'采集规则尚未启用',
    'provider_business_failure':'平台接口没有返回成功结果', 'provider_blocked':'平台接口暂不可用',
    'budget_deferred':'等待可用的采集预算', 'paid_identity_hold':'上次请求结果尚未确认，系统正在保留并恢复该请求',
    'preparation_input_changed':'定位资料已变化，需要按新资料重新准备',
    'unsupported_profile_host':'此链接暂不能用于账号定位', 'short_link_resolution_required':'分享链接尚未完成展开',
    'profile_expansion_required':'分享短链需要先展开为官方账号主页',
    'locator_resolution_required':'当前账号标识类型无法定位主页，请补充可用 UID、官方账号主页或该平台支持的账号标识',
    'wechat_content_share_not_profile':'这是视频号作品分享链接，请补充账号 sph 标识或 finder UID',
    'video_share_profile_unsupported':'这是视频号作品分享链接，请补充账号 sph 标识或 finder UID',
    'request_identity_mismatch':'返回的账号身份与本次查询目标不一致',
    'channel_id_evidence_missing':'平台返回缺少可核对的视频号账号标识',
    'invalid_finder_candidate':'平台没有返回有效的视频号 finder UID',
    'raw_response_required':'缺少完整原始响应证据，准备尚未完成',
    'invalid_locator':'账号定位资料格式不正确',
    'platform_unsupported':'此平台尚无可用的账号准备适配器',
    'directory_source_not_object':'目录原始资料格式不完整，尚不能提取账号定位信息',
    'multiple_source_profile_links':'原始资料包含不同的账号主页，尚不能确定准确身份',
    'directory_locator_projection_changed':'目录定位信息发生变化，请从原账号行重新补充身份',
    'preparation_owner_changed':'同一账号已由另一准备任务处理，等待共享核验结果',
    'preparation_previous_revision_running':'上次准备任务仍在执行，结束后自动恢复',
    'preparation_previous_revision_active':'上次准备任务仍未完成，等待原任务处理',
    'preparation_previous_revision_billing_unverified':'上次请求账单尚未核实，确认后再恢复准备',
    'preparation_billing_unverified':'请求账单尚未核实，未重复发起付费请求',
    'preparation_retry_exhausted':'自动重试次数已用完，请核对定位信息后重试',
    'batch_reservation_expired':'发送前预占已过期，等待原任务自动重试',
}


def _latest_preparation_work(connection: sqlite3.Connection, intake_id: int | None = None,
                             *, intake_ids: list[int] | None = None) -> dict[int, dict[str, Any]]:
    columns = {row[1] for row in connection.execute('PRAGMA table_info(capture_work_items)')}
    if 'intake_request_id' not in columns:
        return {}
    if intake_id is not None:
        rows = _rows(connection,"SELECT id,intake_request_id,state,reason,due_at FROM capture_work_items WHERE intake_request_id=? ORDER BY id DESC LIMIT 1",(intake_id,))
        return {row["intake_request_id"]:row for row in rows}
    if intake_ids is not None:
        if not intake_ids:
            return {}
        rows = _rows(connection,"""SELECT id,intake_request_id,state,reason,due_at FROM capture_work_items
            WHERE id IN (SELECT MAX(id) FROM capture_work_items
                WHERE intake_request_id IN (SELECT value FROM json_each(?)) GROUP BY intake_request_id)""",
            (json.dumps(intake_ids),))
        return {row['intake_request_id']:row for row in rows}
    rows = _rows(connection,"""SELECT id,intake_request_id,state,reason,due_at FROM capture_work_items
        WHERE id IN (SELECT MAX(id) FROM capture_work_items WHERE intake_request_id IS NOT NULL GROUP BY intake_request_id)""")
    return {row['intake_request_id']:row for row in rows}


def preparation_status(request: Mapping[str, Any], work: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """A technical read model; never writes an operator's account status."""
    result = json.loads(request['result_json'])
    state, reason, due_at = 'queued', None, None
    if result.get('status') == 'ready':
        state = 'ready'
    elif result.get('status') == 'conflict':
        state, reason = 'blocked', result.get('reason') or 'identity_conflict'
    elif result.get('preparation_error'):
        state, reason = 'blocked', result['preparation_error']
    elif work:
        due_at = work.get('due_at')
        if work['state'] in {'running','leased'}:
            state = 'running'
        elif work['state'] in {'provider_blocked','paid_identity_hold'}:
            state, reason = 'blocked', work.get('reason') or work['state']
        elif work['state'] == 'budget_deferred':
            reason = work.get('reason') or 'budget_deferred'
        elif work.get('reason') and work['state'] == 'runnable':
            reason = work['reason']
    if reason in {'preparation_policy_unavailable','operation_price_unverified','budget_deferred','batch_reservation_expired'}:
        state = 'queued'
    label = {'queued':'待接入','running':'正在准备','blocked':'准备失败','ready':'主页准备完成'}[state]
    detail = _PREPARATION_REASONS.get(str(reason), str(reason)) if reason else None
    message = label + ('：' + detail if detail else '')
    if state == 'ready':
        message += '；后续按有效采集计划执行，尚不表示作品抓取已完成。'
    elif state == 'queued' and not reason:
        message += '：资料已接收，等待账号准备任务。'
    return {'intake_id':request['id'],'state':state,'label':label,'reason':reason,'reason_label':detail,
            'due_at':due_at,'message':message}


def annotate_account_preparation(connection: sqlite3.Connection, items: list[dict[str, Any]]) -> None:
    directory_ids = list({item['directory_row_id'] for item in items if item.get('directory_row_id') is not None})
    if not directory_ids or not has_account_intake(connection):
        return
    requests = _rows(connection,"""SELECT * FROM account_intake_requests WHERE id IN
        (SELECT MAX(id) FROM account_intake_requests
            WHERE directory_row_id IN (SELECT value FROM json_each(?)) GROUP BY directory_row_id)""",
        (json.dumps(directory_ids),))
    work = _latest_preparation_work(connection, intake_ids=[row['id'] for row in requests])
    by_directory = {row['directory_row_id']:preparation_status(row,work.get(row['id'])) for row in requests}
    for item in items:
        state = by_directory.get(item.get('directory_row_id'))
        if state:
            item['account_preparation'] = state
