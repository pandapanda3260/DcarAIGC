"""Shared, pure platform work identity parsing and bounded public redirects."""
from __future__ import annotations

import hashlib
import re
import json
import sqlite3
import time
from typing import Any, Callable
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit, urlunsplit

HOSTS = {
    "douyin": {"douyin.com", "www.douyin.com", "v.douyin.com"},
    "xiaohongshu": {"xiaohongshu.com", "www.xiaohongshu.com", "xhslink.com", "www.xhslink.com", "xhslink.cn"},
    "kuaishou": {"kuaishou.com", "www.kuaishou.com", "v.kuaishou.com"},
    "wechat_channels": {"weixin.qq.com", "channels.weixin.qq.com"},
}
SHORT_HOSTS = {"v.douyin.com", "v.kuaishou.com", "xhslink.com", "www.xhslink.com", "xhslink.cn"}
ID_PATTERNS = {"douyin": r"[0-9]{6,24}", "xiaohongshu": r"[0-9a-fA-F]{24}",
               "kuaishou": r"[A-Za-z0-9]{1,128}", "wechat_channels": r"[0-9]{1,32}"}


class ContentIdentityError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _checked(platform: str, url: str):
    try:
        parsed = urlsplit(url)
        valid = (platform in HOSTS and parsed.scheme in {"http", "https"}
            and parsed.hostname in HOSTS[platform] and parsed.port in {None, 80, 443}
            and parsed.username is None and parsed.password is None and "\\" not in url
            and len(url) <= 8192 and not any(ch.isspace() or ord(ch) < 32 for ch in url))
    except ValueError:
        valid = False
    if not valid:
        raise ContentIdentityError("invalid_content_host", "请填写与所选平台一致的官方作品链接。")
    return parsed


def parse(platform: str, url: str, explicit_id: Any = None, *, allow_missing_url: bool = False,
          verified_provider_alias: bool = False, connection: sqlite3.Connection | None = None) -> dict[str, Any]:
    if platform not in HOSTS:
        raise ContentIdentityError("unsupported_platform", "不支持的平台。")
    original = str(url or "").strip()
    if explicit_id not in (None, "") and (isinstance(explicit_id, bool) or not isinstance(explicit_id, (str, int))):
        raise ContentIdentityError("invalid_identifier", "平台作品 ID 必须是准确的文本或整数，不能使用小数或科学计数法。")
    explicit = str(explicit_id if explicit_id is not None else "").strip()
    extracted = ""
    refs = {}
    canonical = ""
    if original:
        parsed = _checked(platform, original)
        if parsed.hostname in SHORT_HOSTS:
            raise ContentIdentityError("identity_unresolved", "分享短链需先展开，才能确定作品身份。")
        patterns = {"douyin": r"/(?:video|note)/([0-9]+)(?:/|$)",
            "xiaohongshu": r"/(?:explore|discovery/item)/([0-9a-fA-F]+)(?:/|$)",
            "kuaishou": r"/(?:short-video|photo)/([A-Za-z0-9]+)(?:/|$)",
            "wechat_channels": r"/video/([0-9]+)(?:/|$)"}
        match = re.fullmatch(patterns[platform], parsed.path)
        if match:
            extracted = match.group(1)
        query = parse_qs(parsed.query, keep_blank_values=True)
        if platform == "douyin" and "modal_id" in query:
            modal_ids = query["modal_id"]
            if len(modal_ids) != 1 or not modal_ids[0]:
                raise ContentIdentityError("identity_conflict", "作品链接包含冲突的定位信息。")
            if not re.fullmatch(ID_PATTERNS[platform], modal_ids[0]):
                raise ContentIdentityError("identity_unresolved", "抖音链接中的作品 ID 格式不正确。")
            if extracted and extracted != modal_ids[0]:
                raise ContentIdentityError("identity_conflict", "链接中的作品 ID 不一致。")
            extracted = modal_ids[0]
        if platform == "wechat_channels":
            for key in ("object_id", "object_nonce_id"):
                if key in query:
                    if len(query[key]) != 1 or not query[key][0]:
                        raise ContentIdentityError("identity_conflict", "作品链接包含冲突的定位信息。")
                    refs[key] = query[key][0]
            if refs.get("object_id"):
                if extracted and extracted != refs["object_id"]:
                    raise ContentIdentityError("identity_conflict", "链接中的作品 ID 不一致。")
                extracted = refs["object_id"]
        canonical = urlunsplit(("https", parsed.hostname, re.sub(r"/{2,}", "/", parsed.path).rstrip("/") or "/",
            urlencode(sorted(refs.items())) if platform == "wechat_channels" else "", ""))
    elif not allow_missing_url:
        raise ContentIdentityError("identity_unresolved", "请填写作品链接。")
    if original and not extracted:
        raise ContentIdentityError("identity_unresolved", "作品身份待解析，请补充完整作品链接。")
    same = explicit.lower() == extracted.lower() if platform == "xiaohongshu" else explicit == extracted
    aliases = []
    if explicit and extracted and not same:
        heterogeneous = (platform == "kuaishou" and explicit.isascii() and explicit.isdecimal()
                         and re.fullmatch(r"3x[A-Za-z0-9]{1,126}", extracted) is not None)
        proven = False
        if heterogeneous and connection is not None:
            owner = alias_content(connection, platform, explicit)
            alias = alias_content(connection, platform, extracted)
            proven = owner is not None and alias is not None and owner["id"] == alias["id"]
        if not heterogeneous or not (verified_provider_alias or proven):
            raise ContentIdentityError("identity_conflict", "填写的作品 ID 与链接不一致。")
        aliases.append(extracted)
    identity = explicit or extracted
    if not re.fullmatch(ID_PATTERNS[platform], identity):
        raise ContentIdentityError("identity_unresolved", "作品身份待解析，请补充完整作品链接或准确的平台作品 ID。")
    if platform == "xiaohongshu":
        identity = identity.lower()
    # These public canonical forms are known. Never invent a Channels URL from
    # an object ID: some objects require the original share/nonce locator.
    if original and platform != "wechat_channels":
        prefix = {"douyin":"https://www.douyin.com/video/", "xiaohongshu":"https://www.xiaohongshu.com/explore/",
                  "kuaishou":"https://www.kuaishou.com/short-video/"}[platform]
        canonical = prefix + (extracted if aliases else identity)
    return {"platform":platform,"platform_content_id":identity,"canonical_url":canonical,
        "original_url":original,"locator_references":refs,"resolution_status":"resolved",
        "normalized_url_hash":hashlib.sha256(canonical.encode()).hexdigest() if canonical else None,
        "identity_key":f"{platform}:{identity}", "identity_aliases":aliases}


def expand(platform: str, url: str, *, request_once: Callable | None = None) -> str:
    """Reuse the pinned-DNS bounded transport; no browser/HTML guessing."""
    from . import account_profile_public as public
    current = url
    deadline = time.monotonic() + public.LOOKUP_TIMEOUT
    seen = set()
    for hop in range(public.MAX_REDIRECTS + 1):
        parsed = _checked(platform, current)
        if current in seen:
            break
        seen.add(current)
        if parsed.hostname not in SHORT_HOSTS:
            return current
        if hop == public.MAX_REDIRECTS:
            break
        # _request_once rejects non-public DNS destinations and bounds body/time.
        target = urlunsplit(("https", parsed.hostname, parsed.path, parsed.query, ""))
        response = (request_once or public._request_once)(target, deadline=deadline)
        locations = response.values("Location")
        if response.status not in {301,302,303,307,308} or len(locations) != 1:
            break
        current = urljoin(target, locations[0])
    raise ContentIdentityError("identity_unresolved", "分享短链暂时无法展开，链接已保留；请补充完整作品链接。")


def alias_content(connection: sqlite3.Connection, platform: str, identifier: str) -> dict[str, Any] | None:
    """Resolve an existing exact alias; no nickname or cross-platform matching."""
    from .metric_field_facts import resolve_metric_content_id
    row = connection.execute("SELECT content_id FROM content_identities WHERE platform_identity_key=?",
                             (f"{platform}:{identifier}",)).fetchone()
    if row is None:
        row = connection.execute("SELECT id FROM content_items WHERE platform=? AND platform_content_id=?",
                                 (platform, identifier)).fetchone()
    if row is None:
        return None
    identifier = resolve_metric_content_id(connection, int(row[0])) if connection.execute("PRAGMA user_version").fetchone()[0] >= 20 else int(row[0])
    row = connection.execute("SELECT * FROM content_items WHERE id=?", (identifier,)).fetchone()
    return dict(row) if row is not None else None


def alias_url_key(platform: str, url: str) -> str:
    """Keep identity-bearing query fields and short-link query tokens."""
    parsed = _checked(platform, url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    identity_fields = {"douyin": ("modal_id",), "wechat_channels": ("object_id", "object_nonce_id")}
    kept = query if parsed.hostname in SHORT_HOSTS else {
        key: query[key] for key in identity_fields.get(platform, ()) if key in query}
    normalized = urlunsplit(("https", parsed.hostname, re.sub(r"/{2,}", "/", parsed.path).rstrip("/") or "/",
                             urlencode(sorted((key, value) for key, values in kept.items() for value in values)), ""))
    return f"{platform}:url:{hashlib.sha256(normalized.encode()).hexdigest()}"


def normalize_submission(value: dict[str, Any], *, connection: sqlite3.Connection | None = None) -> dict[str, Any]:
    result = dict(value)
    platform = str(result.get("platform") or "")
    original = str(result.get("canonical_url") or result.get("url") or "").strip()
    parsed = _checked(platform, original) if original else None
    if connection is not None and parsed is not None and parsed.hostname in SHORT_HOSTS:
        saved_alias = connection.execute("SELECT content_id FROM content_identities WHERE platform_identity_key=?",
                                         (alias_url_key(platform, original),)).fetchone()
        if saved_alias is not None:
            from .metric_field_facts import resolve_metric_content_id
            owner_id = resolve_metric_content_id(connection, int(saved_alias[0])) if connection.execute("PRAGMA user_version").fetchone()[0] >= 20 else int(saved_alias[0])
            saved = connection.execute("SELECT platform_content_id,canonical_url FROM content_items WHERE id=? AND platform=?", (owner_id,platform)).fetchone()
            if saved is not None and saved["platform_content_id"] and saved["canonical_url"]:
                supplied_id = result.get("platform_content_id")
                if supplied_id not in (None, ""):
                    owner = alias_content(connection, platform, str(supplied_id))
                    if owner is None or owner["id"] != owner_id:
                        raise ContentIdentityError("identity_conflict", "填写的作品 ID 与已验证的分享链接不一致。")
                resolved = parse(platform, saved["canonical_url"], saved["platform_content_id"], connection=connection)
                return {**result,"platform_content_id":resolved["platform_content_id"],"canonical_url":resolved["canonical_url"],
                    "_content_alias_urls":[original],"_locator_references":resolved["locator_references"]}
    expanded = expand(platform, original) if parsed and parsed.hostname in SHORT_HOSTS else original
    identity = parse(platform, expanded, result.get("platform_content_id"), connection=connection)
    if connection is not None:
        saved = alias_content(connection, platform, identity["platform_content_id"])
        if (saved is not None and platform == "kuaishou" and saved["platform_content_id"]
                and str(saved["platform_content_id"]).isascii() and str(saved["platform_content_id"]).isdecimal()):
            identity = parse(platform, expanded, saved["platform_content_id"], connection=connection)
    return {**result, "platform_content_id":identity["platform_content_id"],"canonical_url":identity["canonical_url"],
        "_content_alias_urls":list(dict.fromkeys([original,expanded])),"_locator_references":identity["locator_references"]}


def enqueue_pending_link(connection: sqlite3.Connection, submitted: dict[str, Any], *, reason: str,
                         at: str, request_key: str | None = None) -> dict[str, Any]:
    if not connection.in_transaction or connection.execute("PRAGMA user_version").fetchone()[0] < 23:
        raise ValueError("pending link requires schema23 writer transaction")
    platform = str(submitted.get("platform") or "")
    original = str(submitted.get("canonical_url") or submitted.get("url") or "").strip()
    _checked(platform, original)
    encoded = json.dumps(submitted, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    key = request_key or hashlib.sha256(encoded.encode()).hexdigest()
    previous = connection.execute("SELECT * FROM content_link_intakes WHERE request_key=?", (key,)).fetchone()
    if previous:
        if previous["input_json"] != encoded:
            raise ContentIdentityError("identity_conflict", "同一提交编号对应不同作品链接。")
        return {"status":"resolved" if previous["status"] == "resolved" else "pending_identity",
            "intake_id":previous["id"], "content_id":previous["content_id"], "replayed":True,
            "reason":"identity_unresolved", "message":previous["reason"]}
    row = connection.execute("INSERT INTO content_link_intakes(request_key,platform,original_url,input_json,status,reason,created_at,updated_at) "
        "VALUES(?,?,?,?,'pending',?,?,?)", (key,platform,original,encoded,reason,at,at))
    return {"status":"pending_identity", "intake_id":row.lastrowid, "reason":"identity_unresolved", "message":reason, "replayed":False}


def prepare_pending_resolution(connection: sqlite3.Connection, intake_id: int,
                               value: dict[str, Any] | None = None) -> dict[str, Any]:
    """Resolve only an explicitly selected pending link, outside a write lock."""
    if connection.in_transaction:
        raise ValueError("link resolution must run outside a write transaction")
    row = connection.execute("SELECT * FROM content_link_intakes WHERE id=?", (intake_id,)).fetchone()
    if row is None:
        raise ContentIdentityError("intake_missing", "待解析链接不存在。")
    source = dict(row)
    if source["status"] == "resolved":
        resolution = json.loads(source.get("resolution_json") or "{}")
        if value and dict(value) != resolution.get("correction", {}):
            raise ContentIdentityError("intake_changed", "该链接已解析为作品；不同身份请通过作品编辑处理。")
        return {"intake_id":intake_id,"source":source,"replayed":True,"content_id":source["content_id"]}
    submitted = json.loads(source["input_json"])
    updates = value or {}
    if set(updates) - {"canonical_url", "platform_content_id"}:
        raise ContentIdentityError("identity_conflict", "补充作品身份只能修改链接和作品 ID。")
    try:
        normalized = normalize_submission({**submitted, **updates}, connection=connection)
    except ContentIdentityError as error:
        return {"intake_id":intake_id,"source":source,"error_code":error.code,"message":str(error),"correction":dict(updates)}
    aliases = list(normalized.get("_content_alias_urls", []))
    # Preserve original intake URL as an alias only after its redirect is proved.
    # A replacement URL supplied by an operator does not prove the old short URL.
    return {"intake_id":intake_id,"source":source,"normalized":{**normalized,"_content_alias_urls":aliases},
            "correction":dict(updates)}


def apply_pending_resolution(connection: sqlite3.Connection, prepared: dict[str, Any], *, at: str) -> dict[str, Any]:
    if not connection.in_transaction:
        raise ValueError("pending link apply requires writer transaction")
    row = connection.execute("SELECT * FROM content_link_intakes WHERE id=?", (prepared["intake_id"],)).fetchone()
    if row is None:
        raise ContentIdentityError("intake_missing", "待解析链接不存在。")
    if row["status"] == "resolved":
        saved = json.loads(row["resolution_json"] or "{}")
        if not prepared.get("replayed") and (prepared.get("normalized") != saved.get("normalized")
                or prepared.get("correction", {}) != saved.get("correction", {})):
            raise ContentIdentityError("intake_changed", "该链接已由另一条身份解析结果完成，请刷新核对。")
        return {"status":"resolved","intake_id":row["id"],"content_id":row["content_id"],"replayed":True}
    if dict(row) != prepared["source"]:
        raise ContentIdentityError("intake_changed", "待解析链接已更新，请刷新后重试。")
    if prepared.get("error_code"):
        status = "pending" if prepared["error_code"] == "identity_unresolved" else "conflict"
        resolution = json.dumps({"contract":"content-link-resolution-v1", "correction":prepared.get("correction", {}),
            "error_code":prepared["error_code"], "message":prepared["message"]},
            ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if row["status"] != status or row["reason"] != prepared["message"] or row["resolution_json"] != resolution:
            connection.execute("UPDATE content_link_intakes SET status=?,reason=?,resolution_json=?,updated_at=? WHERE id=?",
                               (status,prepared["message"],resolution,at,row["id"]))
        return {"status":"pending_identity" if status == "pending" else "conflict","intake_id":row["id"],
                "reason":prepared["error_code"],"message":prepared["message"]}
    from .operations import upsert_content
    from pathlib import Path
    database = Path(connection.execute("PRAGMA database_list").fetchone()[2])
    result = upsert_content(prepared["normalized"], db_path=database, connection=connection)
    resolution = json.dumps({"contract":"content-link-resolution-v1", "correction":prepared.get("correction", {}),
        "normalized":prepared["normalized"], "content_id":result["id"], "resolved_at":at},
        ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    connection.execute("UPDATE content_link_intakes SET status='resolved',reason='',content_id=?,resolution_json=?,updated_at=? WHERE id=?",
                       (result["id"],resolution,at,row["id"]))
    return {"status":"resolved","intake_id":row["id"],"content_id":result["id"],"action":result["action"],"replayed":False}


def record_provider_content_aliases(connection: sqlite3.Connection, content_id: int,
                                    parsed: dict[str, Any], raw_response_id: int, at: str) -> dict[str, Any]:
    """Add a raw-proven KS numeric/eid pair without changing frozen primary ID.

    The normalized argument is checked against the saved, hash-verified entity;
    arbitrary caller data cannot assert a relationship between two work IDs.
    Discovery owns its alias binding in upsert_content and does not call this.
    """
    if not connection.in_transaction:
        raise ValueError("provider alias recording requires writer transaction")
    current = connection.execute("SELECT * FROM content_items WHERE id=?", (content_id,)).fetchone()
    if current is None:
        raise ContentIdentityError("content_missing", "作品不存在。")
    if current["platform"] != "kuaishou":
        return {"content_id":content_id,"aliases_created":0}
    raw = connection.execute("SELECT * FROM provider_raw_responses WHERE id=?", (raw_response_id,)).fetchone()
    if (raw is None or raw["content_id"] != content_id or str(raw["provider"]).lower() != "tikhub"
            or raw["operation"] not in {"kuaishou_video_detail", "kuaishou_video_statistics"} or raw["http_status"] != 200):
        raise ContentIdentityError("identity_conflict", "作品别名缺少归属一致的详情原始证据。")
    uid = str(current["raw_account_uid"] or "")
    if current["account_id"] is not None:
        owners = connection.execute("SELECT uid FROM account_platform_identities WHERE account_id=? AND platform='kuaishou'", (current["account_id"],)).fetchall()
        if len(owners) != 1 or uid not in ("", owners[0]["uid"]):
            raise ContentIdentityError("identity_conflict", "作品作者与已保存账号身份不一致。")
        uid = owners[0]["uid"]
    if not uid or parsed.get("account_uid") != uid:
        raise ContentIdentityError("identity_conflict", "作品别名不能改变原作者。")
    from . import raw_archive, kuaishou_adapter
    from .capture import CaptureError
    from .raw_evidence import RawEvidenceError
    try:
        payload = json.loads(raw_archive.read_response_entity(connection, raw_response_id))
        requested = payload.get("params", {}).get("photo_id") if isinstance(payload.get("params"), dict) else None
        requested = str(requested) if requested is not None else str(current["platform_content_id"] or "")
        proven = kuaishou_adapter.parse_stage("detail", requested, payload, expected_uid=uid)
        identity = parse("kuaishou", proven["canonical_url"], proven["platform_content_id"],
                         verified_provider_alias=True, connection=connection)
    except (ValueError, TypeError, KeyError, OSError, CaptureError, RawEvidenceError) as error:
        raise ContentIdentityError("identity_conflict", "作品别名原始证据校验失败。") from error
    if (parsed.get("platform_content_id") != proven["platform_content_id"]
            or parsed.get("canonical_url") != proven["canonical_url"]
            or str(current["platform_content_id"] or "") not in {identity["platform_content_id"], *identity["identity_aliases"]}):
        raise ContentIdentityError("identity_conflict", "详情内容、已保存作品和原始别名证据不一致。")
    # Keep the current primary and every active command's identity stable.
    # A later discovery upsert may promote the provider numeric ID normally.
    from .operations import _save_content_identity_aliases
    aliases = list(dict.fromkeys([identity["platform_content_id"], *identity["identity_aliases"]]))
    before = connection.total_changes
    _save_content_identity_aliases(connection, content_id=content_id,
        identity={**identity,"identity_aliases":aliases}, alias_urls=[identity["canonical_url"]], at=at)
    return {"content_id":content_id,"aliases_created":connection.total_changes-before}
