"""Pure four-platform contracts shared by account intake and collection.

No network, database or budget authority is granted here. Callers persist exact
HTTP entities before passing parsed payloads and retain each step's raw ID.
"""
from __future__ import annotations

import re
from typing import Any, Mapping
from urllib.parse import urlsplit

CONTRACT_VERSION = "account-platform-adapters-v1"
PLATFORMS = frozenset({"douyin", "xiaohongshu", "kuaishou", "wechat_channels"})
UID_PATTERNS = {
    "douyin": r"[0-9]{6,24}", "xiaohongshu": r"[0-9a-f]{24}",
    "kuaishou": r"[0-9]{1,24}", "wechat_channels": r"v2_[0-9a-fA-F]+@finder",
}
SEC_PATTERN = r"MS4wLjAB[A-Za-z0-9_-]{32,120}"
EID_PATTERN = r"3x[A-Za-z0-9_-]{5,64}"
CHANNEL_PATTERN = r"sph[A-Za-z0-9_-]{1,61}"
PROFILE_OPERATIONS = {
    "douyin": "douyin_uid_profile", "xiaohongshu": "xiaohongshu_user_profile",
    "kuaishou": "kuaishou_user_profile", "wechat_channels": "wechat_channels_user_profile",
}
POST_OPERATIONS = {platform: platform + "_user_posts" for platform in PLATFORMS}
DETAIL_OPERATIONS = {"douyin": "douyin_video_detail", "xiaohongshu": "xiaohongshu_note_detail",
                     "kuaishou": "kuaishou_video_detail", "wechat_channels": "wechat_channels_video_detail"}
ROUTES = {
    "douyin_uid_profile": ("GET", "/api/v1/douyin/web/fetch_user_profile_by_uid"),
    "douyin_sec_profile": ("GET", "/api/v1/douyin/app/v3/handler_user_profile"),
    "douyin_display_profile": ("GET", "/api/v1/douyin/web/handler_user_profile_v2"),
    "xiaohongshu_user_profile": ("GET", "/api/v1/xiaohongshu/app_v2/get_user_info"),
    "xiaohongshu_user_search": ("GET", "/api/v1/xiaohongshu/app_v2/search_users"),
    "kuaishou_user_profile": ("GET", "/api/v1/kuaishou/app/fetch_one_user_v2"),
    "wechat_channels_resolve": ("POST", "/api/v1/wechat_channels/v2/fetch_channel_id_to_username"),
    "wechat_channels_channel_info": ("POST", "/api/v1/wechat_channels/v2/fetch_channel_info"),
    "wechat_channels_user_profile": ("POST", "/api/v1/wechat_channels/v2/fetch_user_profile"),
    "douyin_user_posts": ("GET", "/api/v1/douyin/app/v3/fetch_user_post_videos"),
    "xiaohongshu_user_posts": ("GET", "/api/v1/xiaohongshu/app_v2/fetch_user_post_notes"),
    "kuaishou_user_posts": ("GET", "/api/v1/kuaishou/app/fetch_user_post_v2"),
    "wechat_channels_user_posts": ("POST", "/api/v1/wechat_channels/v2/fetch_user_videos"),
    "kuaishou_video_detail": ("GET", "/api/v1/kuaishou/app/fetch_one_video"),
    "kuaishou_video_statistics": ("GET", "/api/v1/kuaishou/app/fetch_one_video"),
    "wechat_channels_video_detail": ("POST", "/api/v1/wechat_channels/v2/fetch_video_detail"),
    "wechat_channels_video_statistics": ("POST", "/api/v1/wechat_channels/v2/fetch_video_detail"),
    "wechat_channels_video_comments": ("POST", "/api/v1/wechat_channels/v2/fetch_video_comments"),
}


class PlatformAdapterError(ValueError):
    def __init__(self, code: str, message: str | None = None):
        self.code = self.error_code = code
        super().__init__(message or code)


def identifier(value: Any) -> str:
    """Provider JSON integers are exact in Python; floats/bools are never IDs."""
    if type(value) is int and value >= 0:
        return str(value)
    if isinstance(value, str) and value.strip() == value and value:
        return value
    raise PlatformAdapterError("invalid_identifier")


def valid_uid(platform: str, uid: Any) -> bool:
    return isinstance(uid, str) and platform in UID_PATTERNS and re.fullmatch(UID_PATTERNS[platform], uid) is not None


def _one(obj: Mapping[str, Any], keys: tuple[str, ...], *, required: bool = True) -> str:
    values = {identifier(obj[key]) for key in keys if obj.get(key) not in (None, "")}
    if len(values) != 1:
        if not values and not required:
            return ""
        raise PlatformAdapterError("identity_conflict" if values else "identity_missing")
    return next(iter(values))


def normalize_account_input(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PlatformAdapterError("invalid_account_input")
    aliases = {"抖音": "douyin", "小红书": "xiaohongshu", "快手": "kuaishou", "视频号": "wechat_channels", "wechat": "wechat_channels"}
    platform = aliases.get(value.get("platform"), value.get("platform"))
    if not platform and isinstance(value.get("profile_url"), str):
        host = urlsplit(value["profile_url"].strip()).hostname
        hosts = {"douyin": {"douyin.com", "www.douyin.com", "v.douyin.com"},
                 "xiaohongshu": {"xiaohongshu.com", "www.xiaohongshu.com", "xhslink.com", "www.xhslink.com", "xhslink.cn"},
                 "kuaishou": {"kuaishou.com", "www.kuaishou.com", "v.kuaishou.com"},
                 "wechat_channels": {"weixin.qq.com", "channels.weixin.qq.com"}}
        platform = next((key for key, values in hosts.items() if host in values), None)
    if platform not in PLATFORMS:
        raise PlatformAdapterError("platform_unsupported")
    result = {"platform": platform, "uid": None, "display_account_id": None, "profile_url": None, "references": {}}
    for key in ("uid", "display_account_id", "profile_url"):
        item = value.get(key)
        if item not in (None, ""):
            if not isinstance(item, str):
                raise PlatformAdapterError("identifier_requires_text")
            result[key] = item.strip()
    refs = value.get("references") or {}
    if not isinstance(refs, Mapping):
        raise PlatformAdapterError("invalid_references")
    for kind in ("sec_user_id", "eid", "channel_id"):
        item = refs.get(kind) or value.get(kind)
        if item not in (None, ""):
            if not isinstance(item, str):
                raise PlatformAdapterError("identifier_requires_text")
            result["references"][kind] = item.strip()
    uid = result["uid"]
    if uid and platform == "xiaohongshu":
        result["uid"] = uid = uid.lower()
    # Historical imports sometimes put public locator aliases in UID. They
    # remain candidate references, never become canonical platform identities.
    if uid and not valid_uid(platform, uid):
        kind = "eid" if platform == "kuaishou" and re.fullmatch(EID_PATTERN, uid) else "channel_id" if platform == "wechat_channels" and re.fullmatch(CHANNEL_PATTERN, uid) else None
        if kind is None:
            raise PlatformAdapterError("invalid_uid")
        if result["references"].get(kind, uid) != uid:
            raise PlatformAdapterError("identity_conflict")
        result["references"][kind], result["uid"] = uid, None
    url = result["profile_url"]
    if url:
        try:
            parsed = urlsplit(url)
            if (parsed.scheme != "https" or parsed.username or parsed.password or parsed.port not in (None, 443)
                    or any(ord(c) < 33 for c in url) or "\\" in url):
                raise ValueError()
        except ValueError:
            raise PlatformAdapterError("invalid_profile_url") from None
        host, path = parsed.hostname, parsed.path.rstrip("/")
        token = None
        if platform == "douyin" and host in {"douyin.com", "www.douyin.com"}:
            m = re.fullmatch(r"/user/([^/]+)", path)
            token = m.group(1) if m else None
            if token and re.fullmatch(SEC_PATTERN, token):
                if result["references"].get("sec_user_id", token) != token:
                    raise PlatformAdapterError("identity_conflict")
                result["references"]["sec_user_id"] = token
                token = None
            elif not token or not valid_uid(platform, token):
                raise PlatformAdapterError("invalid_profile_url")
        elif platform == "xiaohongshu" and host in {"xiaohongshu.com", "www.xiaohongshu.com"}:
            m = re.fullmatch(r"/user/profile/([a-fA-F0-9]{24})", path)
            if not m:
                raise PlatformAdapterError("invalid_profile_url")
            token = m.group(1).lower()
        elif platform == "kuaishou" and host in {"kuaishou.com", "www.kuaishou.com"}:
            m = re.fullmatch(r"/profile/([A-Za-z0-9_-]+)", path)
            token = m.group(1) if m else None
            if token and re.fullmatch(EID_PATTERN, token):
                if result["references"].get("eid", token) != token:
                    raise PlatformAdapterError("identity_conflict")
                result["references"]["eid"] = token
                token = None
            elif not token or not valid_uid(platform, token):
                raise PlatformAdapterError("invalid_profile_url")
        elif platform == "wechat_channels" and host == "weixin.qq.com" and re.fullmatch(r"/sph/[A-Za-z0-9_-]+", path):
            raise PlatformAdapterError("video_share_profile_unsupported", "这是视频号作品分享链接；请填写该账号的 sph 视频号 ID 或 finder UID。")
        elif platform == "douyin" and host == "v.douyin.com" or platform == "kuaishou" and host == "v.kuaishou.com":
            raise PlatformAdapterError("profile_expansion_required", "账号分享短链需要先展开；请使用新增入口解析或填写完整主页链接、账号 UID。")
        elif platform == "xiaohongshu" and host in {"xhslink.com", "www.xhslink.com", "xhslink.cn"}:
            result["references"]["share_url"] = url
        else:
            raise PlatformAdapterError("unsupported_profile_host")
        if token:
            if result["uid"] and result["uid"] != token:
                raise PlatformAdapterError("identity_conflict")
            result["uid"] = token
    if platform == "wechat_channels" and result["display_account_id"]:
        channel = result["display_account_id"]
        if re.fullmatch(CHANNEL_PATTERN, channel):
            if result["references"].get("channel_id", channel) != channel:
                raise PlatformAdapterError("identity_conflict")
            result["references"]["channel_id"] = channel
    for kind, pattern in (("sec_user_id", SEC_PATTERN), ("eid", EID_PATTERN), ("channel_id", CHANNEL_PATTERN)):
        if kind in result["references"] and re.fullmatch(pattern, result["references"][kind]) is None:
            raise PlatformAdapterError("invalid_locator")
    if not any((result["uid"], result["display_account_id"], result["profile_url"], result["references"])):
        raise PlatformAdapterError("identity_missing")
    return result


def normalize_submission_input(value: Mapping[str, Any], *, expand_url=None) -> dict[str, Any]:
    """Resolve official redirects before entering the submission transaction.

    Web/CLI input adapters may use this bounded existing public transport; the
    deterministic database service and paid worker never run network in a txn.
    """
    candidate = dict(value)
    url = candidate.get("profile_url")
    if isinstance(url, str) and urlsplit(url.strip()).hostname in {"v.douyin.com", "v.kuaishou.com"}:
        if expand_url is None:
            from .account_profile_public import expand_public_profile_url
            expand_url = expand_public_profile_url
        expanded = expand_url(url.strip())
        normalized = normalize_account_input({**candidate, "profile_url": expanded})
        original_platform = "douyin" if urlsplit(url.strip()).hostname == "v.douyin.com" else "kuaishou"
        if normalized["platform"] != original_platform:
            raise PlatformAdapterError("identity_conflict")
        candidate["profile_url"] = expanded
    return {**candidate, **normalize_account_input(candidate)}


def request(operation: str, params: Mapping[str, Any], *, platform: str, subject: str) -> dict[str, Any]:
    if operation not in ROUTES:
        raise PlatformAdapterError("unsupported_operation")
    method, path = ROUTES[operation]
    return {"operation": operation, "method": method, "path": path, "params": dict(params),
            "platform": platform, "subject": subject}


def response_data(payload: Any, platform: str) -> dict[str, Any]:
    if not isinstance(payload, dict) or type(payload.get("code")) is not int or payload["code"] != 200:
        raise PlatformAdapterError("provider_business_failure")
    data = payload.get("data")
    if not isinstance(data, dict) or data.get("error"):
        raise PlatformAdapterError("provider_business_failure")
    if platform == "douyin":
        if type(data.get("status_code")) is not int or data["status_code"] != 0:
            raise PlatformAdapterError("provider_business_failure")
    elif platform == "xiaohongshu":
        if type(data.get("code")) is not int or data["code"] != 0 or data.get("success") is not True:
            raise PlatformAdapterError("provider_business_failure")
    elif platform == "kuaishou":
        if type(data.get("result")) is not int or data["result"] != 1:
            raise PlatformAdapterError("provider_business_failure")
    elif platform == "wechat_channels":
        if data.get("success") is False or any(key in data and (type(data[key]) is not int or data[key] != 0)
                for key in ("ret", "errcode", "errCode")):
            raise PlatformAdapterError("provider_business_failure")
        status = data.get("baseResponse")
        if status is not None and (not isinstance(status, dict) or type(status.get("ret")) is not int or status["ret"] != 0):
            raise PlatformAdapterError("provider_business_failure")
    else:
        raise PlatformAdapterError("platform_unsupported")
    return data


def _raw_ids(responses: list[Mapping[str, Any]]) -> list[int]:
    ids = [entry.get("raw_response_id") for entry in responses]
    if any(type(value) is not int or value <= 0 for value in ids):
        raise PlatformAdapterError("raw_response_required")
    return list(dict.fromkeys(ids))


def _channel_info(payload: Any, expected_uid: str, expected_channel: str | None) -> str:
    data = response_data(payload, "wechat_channels")
    # Only real info section values count. The converter's channel_id and the
    # simplified info finder's request echo are deliberately not identity proof.
    sections = data.get("sections")
    if not isinstance(sections, list):
        raise PlatformAdapterError("channel_id_evidence_missing")
    values = {str(item["content"]).strip() for section in sections if isinstance(section, dict)
              for item in section.get("items", []) if isinstance(item, dict)
              and item.get("title") in {"视频号ID", "视频号 ID", "Channels ID"} and item.get("content")}
    if len(values) != 1:
        raise PlatformAdapterError("channel_id_evidence_missing")
    channel = next(iter(values))
    if not re.fullmatch(CHANNEL_PATTERN, channel) or expected_channel and channel != expected_channel:
        raise PlatformAdapterError("identity_conflict")
    params = payload.get("params")
    if (payload.get("router") != ROUTES["wechat_channels_channel_info"][1]
            or not isinstance(params, dict) or params.get("username") != expected_uid):
        raise PlatformAdapterError("request_identity_mismatch")
    return channel


def _finder_candidates(payload: Any, channel: str) -> list[str]:
    data = response_data(payload, "wechat_channels")
    params = payload.get("params")
    if (payload.get("router") != ROUTES["wechat_channels_resolve"][1]
            or not isinstance(params, dict) or params.get("channel_id") != channel):
        raise PlatformAdapterError("request_identity_mismatch")
    candidates: list[str] = []
    def add(value: Any):
        if value in (None, ""):
            return
        if not valid_uid("wechat_channels", value):
            raise PlatformAdapterError("invalid_finder_candidate")
        if value not in candidates:
            candidates.append(value)
    add(data.get("username"))
    def groups(values: Any):
        if not isinstance(values, list):
            return
        for group in values:
            if not isinstance(group, dict):
                continue
            for item in group.get("items") or []:
                if not isinstance(item, dict):
                    continue
                jump, notice = item.get("jumpInfo") or {}, item.get("noticeParam") or {}
                # Resolver search can mix official accounts and Channels.
                # Skip only a positively identified official-account item;
                # malformed or contradictory finder candidates still fail.
                if (item.get("accTypeName") == "公众号"
                        and isinstance(jump.get("userName"), str)
                        and re.fullmatch(r"gh_[0-9a-f]{12}", jump["userName"])
                        and notice.get("finderUsername") in (None, "")):
                    continue
                add(jump.get("userName"))
                add(notice.get("finderUsername"))
            groups(group.get("subBoxes"))
    groups(data.get("data"))
    if not candidates:
        raise PlatformAdapterError("identity_unresolved")
    if len(candidates) > 10:
        raise PlatformAdapterError("finder_candidate_limit")
    return candidates


def _kuaishou_candidate(payload: Any, eid: str) -> str:
    data = response_data(payload, "kuaishou")
    params = payload.get("params")
    if (payload.get("router") != ROUTES["kuaishou_user_profile"][1]
            or not isinstance(params, dict) or params.get("user_id") != eid):
        raise PlatformAdapterError("request_identity_mismatch")
    profile = data.get("userProfile")
    user = profile.get("profile") if isinstance(profile, dict) else None
    if not isinstance(user, dict):
        raise PlatformAdapterError("profile_object_missing")
    uid = _one(user, ("user_id", "userId"))
    if not valid_uid("kuaishou", uid):
        raise PlatformAdapterError("invalid_uid")
    returned = _one(user, ("eid", "eId"), required=False)
    if returned and returned != eid:
        raise PlatformAdapterError("identity_conflict")
    return uid


def _xhs_candidate(payload: Any, display_id: str) -> str:
    data = response_data(payload, "xiaohongshu")
    params = payload.get("params")
    if (payload.get("router") != ROUTES["xiaohongshu_user_search"][1]
            or not isinstance(params, dict) or params.get("keyword") != display_id
            or type(params.get("page")) is not int or params["page"] != 1):
        raise PlatformAdapterError("request_identity_mismatch")
    users = (data.get("data") or {}).get("users")
    if not isinstance(users, list):
        raise PlatformAdapterError("profile_search_object_missing")
    matches = {_one(user, ("id", "user_id", "userid")).lower() for user in users
               if isinstance(user, dict) and user.get("red_id") == display_id}
    if len(matches) != 1 or not valid_uid("xiaohongshu", next(iter(matches), None)):
        raise PlatformAdapterError("identity_conflict" if len(matches) > 1 else "display_id_not_resolved")
    return next(iter(matches))


def next_profile_request(value: Mapping[str, Any], responses: list[Mapping[str, Any]] | None = None,
                         *, require_raw_evidence: bool = True) -> dict[str, Any] | None:
    """Return one paid step. Responses are durable exact entities, in order."""
    normalized = normalize_account_input(value)
    platform, uid, refs = normalized["platform"], normalized["uid"], normalized["references"]
    responses = responses or []
    if platform == "xiaohongshu" and not uid and not normalized["profile_url"] and normalized["display_account_id"]:
        if not responses:
            return request("xiaohongshu_user_search", {"keyword": normalized["display_account_id"], "page": 1}, platform=platform, subject=normalized["display_account_id"])
        if responses[0].get("operation") != "xiaohongshu_user_search":
            raise PlatformAdapterError("unexpected_profile_steps")
        candidate = _xhs_candidate(responses[0]["payload"], normalized["display_account_id"])
        if len(responses) == 1:
            return request("xiaohongshu_user_profile", {"user_id": candidate}, platform=platform, subject=candidate)
        if len(responses) != 2 or responses[1].get("operation") != "xiaohongshu_user_profile":
            raise PlatformAdapterError("unexpected_profile_steps")
        normalize_profile(platform, normalized, responses[-1]["payload"], prior_responses=responses,
                          require_raw_evidence=require_raw_evidence)
        return None
    if platform == "kuaishou" and refs.get("eid"):
        if not responses:
            return request("kuaishou_user_profile", {"user_id": refs["eid"]}, platform=platform, subject=refs["eid"])
        if responses[0].get("operation") != "kuaishou_user_profile":
            raise PlatformAdapterError("unexpected_profile_steps")
        candidate = _kuaishou_candidate(responses[0]["payload"], refs["eid"])
        if uid and candidate != uid:
            raise PlatformAdapterError("identity_conflict")
        if len(responses) == 1:
            return request("kuaishou_user_profile", {"user_id": candidate}, platform=platform, subject=candidate)
        if len(responses) != 2 or responses[1].get("operation") != "kuaishou_user_profile":
            raise PlatformAdapterError("unexpected_profile_steps")
        normalize_profile(platform, normalized, responses[-1]["payload"], prior_responses=responses,
                          require_raw_evidence=require_raw_evidence)
        return None
    if platform != "wechat_channels":
        if responses:
            if len(responses) != 1:
                raise PlatformAdapterError("unexpected_profile_steps")
            expected = next_profile_request(normalized)
            if responses[0].get("operation") != expected["operation"]:
                raise PlatformAdapterError("unexpected_profile_steps")
            normalize_profile(platform, normalized, responses[-1]["payload"], prior_responses=responses,
                          require_raw_evidence=require_raw_evidence)
            return None
        if platform == "douyin":
            if uid:
                return request("douyin_uid_profile", {"uid": uid}, platform=platform, subject=uid)
            if refs.get("sec_user_id"):
                return request("douyin_sec_profile", {"sec_user_id": refs["sec_user_id"]}, platform=platform, subject=refs["sec_user_id"])
            if normalized["display_account_id"]:
                return request("douyin_display_profile", {"unique_id": normalized["display_account_id"]}, platform=platform, subject=normalized["display_account_id"])
        elif platform == "xiaohongshu":
            if uid:
                return request("xiaohongshu_user_profile", {"user_id": uid}, platform=platform, subject=uid)
            if normalized["profile_url"]:
                return request("xiaohongshu_user_profile", {"share_text": normalized["profile_url"]}, platform=platform, subject=normalized["profile_url"])
        elif platform == "kuaishou" and (uid or refs.get("eid")):
            locator = uid or refs["eid"]
            return request("kuaishou_user_profile", {"user_id": locator}, platform=platform, subject=locator)
        raise PlatformAdapterError("locator_resolution_required")
    channel = refs.get("channel_id")
    offset = 0
    candidates = [uid] if uid else []
    if not uid:
        if not channel:
            raise PlatformAdapterError("locator_resolution_required")
        if not responses:
            return request("wechat_channels_resolve", {"channel_id": channel, "raw": True}, platform=platform, subject=channel)
        first = responses[0]
        if first.get("operation") != "wechat_channels_resolve":
            raise PlatformAdapterError("unexpected_profile_steps")
        candidates = _finder_candidates(first["payload"], channel)
        offset = 1
    for uid in candidates:
        if len(responses) == offset:
            return request("wechat_channels_channel_info", {"username": uid, "raw": True}, platform=platform, subject=uid)
        info = responses[offset]
        if info.get("operation") != "wechat_channels_channel_info":
            raise PlatformAdapterError("unexpected_profile_steps")
        returned_channel = _channel_info(info["payload"], uid, None)
        offset += 1
        if channel and returned_channel != channel:
            continue
        if len(responses) == offset:
            return request("wechat_channels_user_profile", {"username": uid, "raw": True}, platform=platform, subject=uid)
        if len(responses) != offset + 1 or responses[-1].get("operation") != "wechat_channels_user_profile":
            raise PlatformAdapterError("unexpected_profile_steps")
        normalize_profile(platform, normalized, responses[-1]["payload"], prior_responses=responses,
                          require_raw_evidence=require_raw_evidence)
        return None
    raise PlatformAdapterError("channel_id_not_matched")


def _count(value: Any) -> int | None:
    if type(value) is int and value >= 0:
        return value
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        return int(value)
    return None


def normalize_profile(platform: str, requested: Mapping[str, Any], payload: Any, *,
                      prior_responses: list[Mapping[str, Any]] | None = None,
                      require_raw_evidence: bool = True) -> dict[str, Any]:
    normalized = normalize_account_input({**requested, "platform": platform})
    data = response_data(payload, platform)
    if platform != "wechat_channels":
        expected = next_profile_request(normalized)
        if platform == "kuaishou" and normalized["references"].get("eid"):
            if not prior_responses or len(prior_responses) != 2:
                raise PlatformAdapterError("eid_mapping_evidence_missing")
            candidate = _kuaishou_candidate(prior_responses[0]["payload"], normalized["references"]["eid"])
            expected = request("kuaishou_user_profile", {"user_id": candidate}, platform=platform, subject=candidate)
        elif platform == "xiaohongshu" and expected["operation"] == "xiaohongshu_user_search":
            if not prior_responses or len(prior_responses) != 2:
                raise PlatformAdapterError("display_mapping_evidence_missing")
            candidate = _xhs_candidate(prior_responses[0]["payload"], normalized["display_account_id"])
            expected = request("xiaohongshu_user_profile", {"user_id": candidate}, platform=platform, subject=candidate)
    else:
        expected = request("wechat_channels_user_profile", {"username": normalized["uid"]}, platform=platform, subject=normalized["uid"] or "")
    if payload.get("router") != expected["path"]:
        raise PlatformAdapterError("request_route_mismatch")
    params = payload.get("params")
    if not isinstance(params, dict) or any(params.get(key) != value for key, value in expected["params"].items() if value is not None):
        raise PlatformAdapterError("request_identity_mismatch")
    references: dict[str, str] = {}
    metrics: dict[str, Any] = {}
    proof: dict[str, Any] = {"contract": CONTRACT_VERSION}
    if platform == "douyin":
        user = data.get("data") or data.get("user")
        if not isinstance(user, dict):
            raise PlatformAdapterError("profile_object_missing")
        uid = _one(user, ("id_str", "uid", "user_id"))
        sec = _one(user, ("sec_uid", "sec_user_id"))
        if not re.fullmatch(SEC_PATTERN, sec) or normalized["references"].get("sec_user_id", sec) != sec:
            raise PlatformAdapterError("identity_conflict")
        references["sec_user_id"] = sec
        display = _one(user, ("unique_id",), required=False) or _one(user, ("short_id",), required=False)
        nickname = user.get("nickname")
        follow = user.get("follow_info") or {}
        metrics["follower_count"] = _count(follow.get("follower_count", user.get("follower_count")))
    elif platform == "xiaohongshu":
        user = data.get("data")
        if not isinstance(user, dict) or not isinstance(user.get("result"), dict) or user["result"].get("success") is not True or user["result"].get("code") != 0:
            raise PlatformAdapterError("profile_object_missing")
        uid = _one(user, ("userid", "uid", "user_id", "id_str")).lower()
        if expected["params"].get("user_id") and uid != expected["params"]["user_id"]:
            raise PlatformAdapterError("identity_conflict")
        display = _one(user, ("red_id", "redid"), required=False)
        nickname = user.get("nickname")
        references["user_id"] = uid
        metrics["follower_count"] = _count(user.get("fans", user.get("fans_count")))
    elif platform == "kuaishou":
        profile = data.get("userProfile")
        user = profile.get("profile") if isinstance(profile, dict) else None
        if not isinstance(user, dict):
            raise PlatformAdapterError("profile_object_missing")
        uid = _one(user, ("user_id", "userId"))
        eid = _one(user, ("eid", "eId"), required=False)
        requested_eid = normalized["references"].get("eid")
        if requested_eid:
            if prior_responses:
                if uid != candidate or eid and eid != requested_eid:
                    raise PlatformAdapterError("identity_conflict")
                eid = requested_eid
            elif eid != requested_eid:
                raise PlatformAdapterError("eid_mapping_evidence_missing")
        if eid:
            references["eid"] = eid
        references["user_id"] = uid
        display = _one(user, ("kwaiId", "kwai_id"), required=False)
        nickname = user.get("user_name") or user.get("userName")
        metrics["follower_count"] = _count((profile.get("ownerCount") or {}).get("fan"))
    elif platform == "wechat_channels":
        user = data.get("contact")
        if not isinstance(user, dict) or not isinstance(data.get("baseResponse"), dict):
            raise PlatformAdapterError("profile_object_missing")
        uid = _one(user, ("username",))
        nickname = user.get("nickname")
        info = [item for item in (prior_responses or []) if item.get("operation") == "wechat_channels_channel_info"
                and isinstance(item.get("payload"), dict) and (item["payload"].get("params") or {}).get("username") == uid]
        if len(info) != 1:
            raise PlatformAdapterError("channel_id_evidence_missing")
        display = _channel_info(info[0]["payload"], uid, normalized["references"].get("channel_id"))
        if params.get("username") != uid:
            raise PlatformAdapterError("request_identity_mismatch")
        if not normalized["uid"]:
            candidates = [item for item in (prior_responses or []) if item.get("operation") == "wechat_channels_resolve"]
            if len(candidates) != 1 or uid not in _finder_candidates(candidates[0]["payload"], normalized["references"].get("channel_id")):
                raise PlatformAdapterError("identity_conflict")
        references.update(username=uid, channel_id=display)
        # The route documents zero counters for accounts with hidden metrics.
        # Preserve them as unavailable observations rather than asserting zero.
        count = _count(data.get("fansCount"))
        metrics["follower_count"] = count if count else None
        if count == 0:
            proof["follower_missing_reason"] = "provider_zero_visibility_unknown"
    else:
        raise PlatformAdapterError("platform_unsupported")
    if not valid_uid(platform, uid) or normalized["uid"] and normalized["uid"] != uid:
        raise PlatformAdapterError("identity_conflict")
    if normalized["display_account_id"] and normalized["display_account_id"] != display:
        if not normalized["uid"]:
            # An opaque short link is not an independent identity proof for a
            # separately supplied display ID. That ID must be confirmed too.
            raise PlatformAdapterError("identity_conflict")
        if display:
            proof["display_account_id_change"] = {"previous": normalized["display_account_id"], "observed": display}
    if not isinstance(nickname, str) or not nickname.strip():
        raise PlatformAdapterError("profile_nickname_missing")
    # Require a genuine match for inputs that had no canonical UID or locator.
    if not normalized["uid"] and not normalized["references"] and not normalized["profile_url"] and display != normalized["display_account_id"]:
        raise PlatformAdapterError("identity_conflict")
    if display:
        references["display_account_id"] = display
    ids = _raw_ids(prior_responses) if prior_responses and require_raw_evidence else []
    ref_ids = {key: ids[-1] for key in references} if ids else {}
    if platform == "kuaishou" and ids and normalized["references"].get("eid"):
        ref_ids["eid"] = ids[0]
    if platform == "wechat_channels" and ids:
        ref_ids["channel_id"] = info[0]["raw_response_id"]
        ref_ids["display_account_id"] = info[0]["raw_response_id"]
    return {"platform": platform, "uid": uid, "nickname": nickname.strip(), "display_account_id": display,
            "references": references, "metrics": metrics, "metadata": proof,
            "source_raw_response_ids": ids, "reference_raw_response_ids": ref_ids}
