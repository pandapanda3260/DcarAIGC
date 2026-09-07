"""Pure homepage parsing and read-only identity resolution for account creation.

There is deliberately no default network client or database connection here.
An injected short-link expander must validate *every* network hop (including
public DNS/address binding, timeout and response limits); this module validates
its input and final result. A profile lookup must be supplied by the separately
authorized, budgeted onboarding dispatcher, never an ordinary capture bypass.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from typing import Any, Callable, Mapping
from urllib.parse import SplitResult, urlsplit

from .account_operating_receipts import AccountOperatingStatusError, load_admission_members


CONTRACT_VERSION = "account-profile-input-v1"
_NUMERIC_UID = re.compile(r"[0-9]{6,24}")
_XHS_UID = re.compile(r"[0-9a-fA-F]{24}")
_SEC_UID = re.compile(r"MS4wLjAB[A-Za-z0-9_-]{32,120}")
_LONG_HOSTS = {
    "douyin.com": "douyin", "www.douyin.com": "douyin",
    "xiaohongshu.com": "xiaohongshu", "www.xiaohongshu.com": "xiaohongshu",
}
_SHORT_HOSTS = {
    "v.douyin.com": "douyin",
    "xhslink.com": "xiaohongshu", "www.xhslink.com": "xiaohongshu",
}
_REFERENCE_KINDS = ("sec_uid", "sec_user_id", "user_id", "profile_id", "profile_url")


class ProfileInputError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class ParsedProfile:
    platform: str
    profile_url: str
    uid: str | None
    sec_user_id: str | None


@dataclass(frozen=True, slots=True)
class ResolvedProfile(ParsedProfile):
    uid: str
    nickname: str = ""
    display_account_id: str = ""
    account_id: int | None = None
    identity_id: int | None = None
    source: str = "local"
    source_raw_response_id: int | None = None


ExpandUrl = Callable[[str], str]
ProfileLookup = Callable[[ParsedProfile], Mapping[str, Any]]


def _url(value: str) -> SplitResult:
    if not isinstance(value, str) or not value.strip() or len(value) > 3000:
        raise ProfileInputError("invalid_profile_url", "请填写有效的账号主页链接。")
    value = value.strip()
    if any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value) or "\\" in value:
        raise ProfileInputError("invalid_profile_url", "主页链接包含无效字符。")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as error:
        raise ProfileInputError("invalid_profile_url", "主页链接格式无效。") from error
    if (parsed.scheme != "https" or parsed.username is not None or parsed.password is not None
            or port not in (None, 443) or not parsed.hostname):
        raise ProfileInputError("invalid_profile_url", "请使用不含登录信息的 HTTPS 主页链接。")
    if parsed.hostname not in _LONG_HOSTS and parsed.hostname not in _SHORT_HOSTS:
        raise ProfileInputError("unsupported_profile_host", "目前只支持抖音和小红书的官方主页链接。")
    return parsed


def _long_profile(parsed: SplitResult) -> ParsedProfile:
    host = str(parsed.hostname)
    platform = _LONG_HOSTS.get(host)
    if platform is None:
        raise ProfileInputError("profile_expansion_failed", "分享链接未解析为账号主页。")
    path = parsed.path.removesuffix("/")
    if platform == "douyin":
        match = re.fullmatch(r"/user/([^/]+)", path)
        token = match.group(1) if match else ""
        uid = token if _NUMERIC_UID.fullmatch(token) else None
        sec = token if _SEC_UID.fullmatch(token) else None
        if uid is None and sec is None:
            raise ProfileInputError("invalid_profile_path", "请粘贴抖音账号主页，不能使用作品链接。")
        return ParsedProfile(platform, "https://www.douyin.com" + path, uid, sec)
    match = re.fullmatch(r"/user/profile/([0-9a-fA-F]{24})", path)
    if match is None:
        raise ProfileInputError("invalid_profile_path", "请粘贴小红书账号主页，不能使用笔记链接。")
    uid = match.group(1).lower()
    return ParsedProfile(platform, "https://www.xiaohongshu.com/user/profile/" + uid, uid, None)


def parse_profile_input(profile_url: str, *, expand_url: ExpandUrl | None = None) -> ParsedProfile:
    """Identify the platform, discard tracking fields, and extract stable tokens."""
    parsed = _url(profile_url)
    short_platform = _SHORT_HOSTS.get(str(parsed.hostname))
    if short_platform is None:
        return _long_profile(parsed)
    if not re.fullmatch(r"/(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+/?", parsed.path) or len(parsed.path) > 256:
        raise ProfileInputError("invalid_short_link", "分享链接格式无效。")
    if expand_url is None:
        raise ProfileInputError("profile_expansion_required", "分享链接需要解析，请使用完整主页链接或稍后重试。")
    # Queries/fragments are not needed for the supported opaque short-link IDs.
    canonical_short = "https://" + str(parsed.hostname) + parsed.path
    try:
        expanded = expand_url(canonical_short)
    except ProfileInputError:
        raise
    except Exception as error:
        raise ProfileInputError("profile_expansion_failed", "暂时无法解析分享链接，请稍后重试。") from error
    result = _long_profile(_url(expanded))
    if result.platform != short_platform:
        raise ProfileInputError("profile_platform_conflict", "分享链接跳转到了其他平台，无法确认账号。")
    return result


def _rows(connection: sqlite3.Connection, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    cursor = connection.execute(query, params)
    names = [column[0] for column in cursor.description or ()]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _checked_uid(platform: str, uid: Any) -> str:
    pattern = _NUMERIC_UID if platform == "douyin" else _XHS_UID
    if not isinstance(uid, str) or not pattern.fullmatch(uid):
        raise ProfileInputError("identity_unresolved", "未能确认账号的真实平台 UID，请稍后重试。")
    return uid.lower() if platform == "xiaohongshu" else uid


def _local_profile(parsed: ParsedProfile, connection: sqlite3.Connection) -> ParsedProfile | ResolvedProfile:
    candidates: dict[int, dict[str, Any]] = {}
    if parsed.uid is not None:
        matches = _rows(connection, "SELECT id,account_id,platform,uid,nickname FROM account_platform_identities "
                        "WHERE platform=? AND uid=? COLLATE NOCASE", (parsed.platform, parsed.uid))
        candidates.update({int(row["id"]): row for row in matches})
    references = {parsed.profile_url, parsed.profile_url.rsplit("/", 1)[-1]}
    if parsed.sec_user_id:
        references.add(parsed.sec_user_id)
    markers = ",".join("?" for _ in references)
    kind_markers = ",".join("?" for _ in _REFERENCE_KINDS)
    matches = _rows(connection,
        "SELECT i.id,i.account_id,i.platform,i.uid,i.nickname FROM account_provider_references r "
        "JOIN account_platform_identities i ON i.id=r.account_identity_id "
        f"WHERE lower(r.provider) IN ('tikhub','newrank_matrix') AND r.reference_kind IN ({kind_markers}) "
        f"AND r.reference_value IN ({markers})",
        (*_REFERENCE_KINDS, *sorted(references)))
    for row in matches:
        if row["platform"] != parsed.platform:
            raise ProfileInputError("identity_conflict", "主页引用与已保存账号的平台冲突。")
        candidates[int(row["id"])] = row
    try:
        admissions = load_admission_members(connection)
    except AccountOperatingStatusError as error:
        raise ProfileInputError("identity_conflict", "已保存账号的资料凭据异常，无法确认主页归属。") from error
    admission_profiles: dict[int, dict[str, Any]] = {}
    for identity_id, binding in admissions.items():
        member = binding["member"]
        metadata = member.get("metadata") or {}
        homepage = member.get("profile_ref")
        tokens = {str(homepage), str(metadata.get("sec_user_id") or member.get("sec_user_id") or "")}
        if homepage:
            tokens.add(urlsplit(str(homepage)).path.rstrip("/").rsplit("/", 1)[-1])
        if not references.intersection(tokens) and not (
            parsed.uid is not None and member.get("platform") == parsed.platform
            and member.get("uid") == parsed.uid
        ):
            continue
        identities = _rows(connection,
            "SELECT id,account_id,platform,uid,nickname FROM account_platform_identities WHERE id=?",
            (identity_id,))
        if (len(identities) != 1 or identities[0]["account_id"] != binding["account_id"]
                or identities[0]["platform"] != member.get("platform")
                or identities[0]["uid"] != member.get("uid")
                or member.get("platform") != parsed.platform):
            raise ProfileInputError("identity_conflict", "已保存主页凭据与当前账号身份不一致。")
        candidates[identity_id] = identities[0]
        admission_profiles[identity_id] = member
    if len(candidates) > 1:
        raise ProfileInputError("identity_conflict", "主页和 UID 对应多个已保存身份，请先检查账号记录。")
    if not candidates:
        return parsed
    identity = next(iter(candidates.values()))
    uid = _checked_uid(parsed.platform, identity["uid"])
    if parsed.uid is not None and uid != parsed.uid:
        raise ProfileInputError("identity_conflict", "主页 UID 与已保存身份不一致。")
    known_refs = _rows(connection,
        "SELECT reference_kind,reference_value FROM account_provider_references WHERE account_identity_id=? "
        "AND lower(provider) IN ('tikhub','newrank_matrix')",
        (int(identity["id"]),))
    admission = admission_profiles.get(int(identity["id"]), {})
    admission_metadata = admission.get("metadata") or {}
    sec_values = {str(row["reference_value"]) for row in known_refs
                  if row["reference_kind"] in ("sec_uid", "sec_user_id")}
    admission_sec = admission_metadata.get("sec_user_id") or admission.get("sec_user_id")
    if admission_sec:
        sec_values.add(str(admission_sec))
    if parsed.platform == "douyin":
        if any(not _SEC_UID.fullmatch(value) for value in sec_values):
            raise ProfileInputError("identity_conflict", "已保存的抖音主页标识无效。")
        if parsed.sec_user_id:
            sec_values.add(parsed.sec_user_id)
        if len(sec_values) > 1:
            raise ProfileInputError("identity_conflict", "同一账号存在冲突的抖音主页标识。")
    display_ids = {str(row["reference_value"]) for row in known_refs
                   if row["reference_kind"] in ("display_account_id", "unique_id", "short_id")}
    if admission_metadata.get("display_account_id"):
        display_ids.add(str(admission_metadata["display_account_id"]))
    return ResolvedProfile(parsed.platform, parsed.profile_url, uid,
        next(iter(sec_values), None) if parsed.platform == "douyin" else None,
        nickname=str(identity["nickname"] or admission.get("nickname") or ""),
        display_account_id=next(iter(display_ids)) if len(display_ids) == 1 else "",
        account_id=int(identity["account_id"]), identity_id=int(identity["id"]), source="local")


def resolve_known_profile(profile_url: str, connection: sqlite3.Connection, *,
                          expand_url: ExpandUrl | None = None) -> ParsedProfile | ResolvedProfile:
    """Use existing trusted provider mappings; never create or alter an identity."""
    return _local_profile(parse_profile_input(profile_url, expand_url=expand_url), connection)


def resolve_profile(profile_url: str, connection: sqlite3.Connection, *,
                    profile_lookup: ProfileLookup | None = None,
                    expand_url: ExpandUrl | None = None) -> ParsedProfile | ResolvedProfile:
    """Resolve known identities first, then validate an explicitly injected lookup."""
    known = resolve_known_profile(profile_url, connection, expand_url=expand_url)
    complete = (isinstance(known, ResolvedProfile) and bool(known.nickname.strip())
                and (known.platform != "douyin" or known.sec_user_id is not None))
    if complete:
        return known
    if profile_lookup is None:
        # A known UID alone is not a complete profile. Preserve the proven
        # tokens without presenting an incomplete cache hit as successful.
        return ParsedProfile(known.platform, known.profile_url, known.uid, known.sec_user_id)
    try:
        result = profile_lookup(known)
    except ProfileInputError:
        raise
    except Exception as error:
        raise ProfileInputError("profile_lookup_failed", "账号资料查询失败，请稍后重试。") from error
    if not isinstance(result, Mapping) or result.get("platform") != known.platform:
        raise ProfileInputError("identity_conflict", "查询结果与主页平台不一致。")
    if result.get("account_id") is not None or result.get("identity_id") is not None:
        raise ProfileInputError("identity_conflict", "平台查询不能指定本地账号身份。")
    uid = _checked_uid(known.platform, result.get("uid"))
    if known.uid is not None and uid != known.uid:
        raise ProfileInputError("identity_conflict", "查询返回的 UID 与主页不一致。")
    sec = result.get("sec_user_id")
    if known.platform == "douyin":
        if not isinstance(sec, str) or not _SEC_UID.fullmatch(sec):
            raise ProfileInputError("identity_unresolved", "查询结果缺少有效的抖音主页标识。")
        if known.sec_user_id is not None and sec != known.sec_user_id:
            raise ProfileInputError("identity_conflict", "查询返回的抖音主页标识不一致。")
    elif sec is not None:
        raise ProfileInputError("identity_conflict", "小红书查询结果包含其他平台的身份标识。")
    nickname = result.get("nickname")
    display_id = result.get("display_account_id", "")
    if not isinstance(nickname, str) or not nickname.strip() or len(nickname) > 200:
        raise ProfileInputError("profile_incomplete", "查询结果缺少有效的账号昵称。")
    if not isinstance(display_id, str) or len(display_id) > 128:
        raise ProfileInputError("profile_incomplete", "查询返回的平台账号格式无效。")
    source_raw_id = result.get("source_raw_response_id")
    if source_raw_id is not None and (type(source_raw_id) is not int or source_raw_id <= 0):
        raise ProfileInputError("profile_incomplete", "查询结果的来源凭据无效。")
    resolved = ResolvedProfile(known.platform, known.profile_url, uid, sec,
        nickname=nickname.strip(), display_account_id=display_id, source="lookup",
        source_raw_response_id=source_raw_id)
    # The resolved UID may already exist under another homepage. Recheck both
    # tokens together so a callback cannot silently join conflicting identities.
    existing = _local_profile(ParsedProfile(known.platform, known.profile_url, uid, sec), connection)
    if isinstance(existing, ResolvedProfile):
        return ResolvedProfile(resolved.platform, resolved.profile_url, uid, sec,
            nickname=resolved.nickname, display_account_id=display_id,
            account_id=existing.account_id, identity_id=existing.identity_id,
            source="lookup", source_raw_response_id=source_raw_id)
    return resolved
