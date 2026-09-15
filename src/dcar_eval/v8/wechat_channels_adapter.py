"""Pure TikHub WeChat Channels V2 request/response adapters.

Contracts checked against the supplier's public docs on 2026-09-12:
https://docs.tikhub.io/472974839e0 through 472974843e0 and 472974845e0.
All operations cost $0.01/request in those docs and require a 30-second
transport timeout. This module performs no transport, storage or hashing I/O.

Resolver search results are candidates: callers must also reverse-check the
public channel ID with ``parse_channel_info`` before binding an identity.
Comment ``raw_user_id`` values exist only in the intermediate in-memory
result; callers must apply the existing dual-HMAC privacy projection before
persistence. Encrypted media stays separate from ordinary playable URLs.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Mapping

from .capture import CaptureError

PLATFORM = "wechat_channels"
PRICE_USD = 0.01
TIMEOUT_SECONDS = 30
_BASE = "/api/v1/wechat_channels/v2/"
_FINDER = re.compile(r"v2_[0-9a-fA-F]+@finder\Z")
_CHANNEL = re.compile(r"sph[A-Za-z0-9_-]+\Z")
_DIGITS = re.compile(r"[0-9]+\Z")
_BUFFER = re.compile(r"[A-Za-z0-9+/=_-]*\Z")


def _error(message: str, payload: Any = None, *, identity: bool = False) -> None:
    raise CaptureError(
        "WeChat Channels " + message,
        retryable=not identity,
        error_code="identity_conflict" if identity else "invalid_response",
        http_status=200 if payload is not None else None,
        billed=True if payload is not None else False,
        raw_response=payload,
    )


def _map(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text_id(value: Any, *, finder: bool = False, channel: bool = False) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        _error("identifier must be an exact string or integer")
    text = str(value)
    pattern = _FINDER if finder else _CHANNEL if channel else _DIGITS
    if not pattern.fullmatch(text) or len(text) > (256 if finder else 64 if channel else 32):
        _error("identifier has an unsupported format")
    return text


def _cursor(cursor: Any) -> dict[str, Any]:
    if cursor in (None, ""):
        return {}
    value = dict(cursor) if isinstance(cursor, Mapping) else {"last_buffer": cursor}
    if set(value) - {"last_buffer", "comment_id"}:
        _error("pagination contains unsupported context")
    buffer = value.get("last_buffer", "")
    if not isinstance(buffer, str) or not _BUFFER.fullmatch(buffer):
        _error("pagination buffer must be an unmodified base64 string")
    value["last_buffer"] = buffer
    if value.get("comment_id") not in (None, ""):
        value["comment_id"] = _text_id(value["comment_id"])
    return value


def request_spec(stage: str, subject: Any, cursor: Any = None) -> dict[str, Any]:
    """Build a POST JSON-body request; IDs and opaque buffers remain text."""
    body: dict[str, Any] = {"raw": True}
    if stage in {"reference", "resolve"}:
        endpoint = "fetch_channel_id_to_username"
        body["channel_id"] = _text_id(subject, channel=True)
    elif stage in {"profile", "account_metrics", "channel_info", "discovery"}:
        endpoint = {
            "profile": "fetch_user_profile", "account_metrics": "fetch_user_profile",
            "channel_info": "fetch_channel_info", "discovery": "fetch_user_videos",
        }[stage]
        body["username"] = _text_id(subject, finder=True)
    elif stage in {"detail", "metrics", "comments"}:
        endpoint = "fetch_video_comments" if stage == "comments" else "fetch_video_detail"
        identity = dict(subject) if isinstance(subject, Mapping) else {"object_id": subject}
        if set(identity) - {"object_id", "object_nonce_id"}:
            _error("unsupported video request identity")
        body["object_id"] = _text_id(identity.get("object_id"))
        if identity.get("object_nonce_id") not in (None, ""):
            body["object_nonce_id"] = _text_id(identity["object_nonce_id"])
        if stage == "comments":
            body.pop("object_nonce_id", None)
    else:
        _error("unsupported stage")
    page = _cursor(cursor)
    if stage in {"discovery", "comments"}:
        if stage != "comments" and page.get("comment_id") not in (None, ""):
            _error("comment context cannot be used for video pagination")
        body.update(page)
        body.setdefault("last_buffer", "")
    elif page:
        _error("this stage does not accept pagination")
    return {"method": "POST", "path": _BASE + endpoint, "body": body}


def _data(payload: Any) -> Mapping[str, Any]:
    value = _map(payload)
    # Do not accept a truthy value, Boolean or the string "200" as success.
    if type(value.get("code")) is not int or value["code"] != 200:
        _error("supplier response was not successful", payload)
    data = value.get("data")
    if not isinstance(data, Mapping):
        _error("response omitted data object", payload)
    if isinstance(data.get("message"), str) and not any(
        key in data for key in ("baseResponse", "ret", "object", "objects", "contact", "username", "videos", "comments")
    ):
        # A real failed continuation returned outer code=200 with only this
        # upstream error message and debug fields. It is not an empty page.
        _error("upstream returned an error message instead of requested data", payload)
    for level in (value, data):
        if level.get("error") not in (None, "", {}, []) or ("success" in level and level["success"] is not True):
            _error("response contains an inner error", payload)
        if level is data and "code" in level and (type(level["code"]) is not int or level["code"] not in (0, 200)):
            _error("response contains an inner failure code", payload)
        if "baseResponse" in level:
            base = _map(level["baseResponse"])
            if type(base.get("ret")) is not int or base["ret"] != 0:
                _error("upstream response was not successful", payload)
        for name in ("errcode", "errCode", "ret"):
            if name in level and (type(level[name]) is not int or level[name] != 0):
                _error("response contains an upstream error code", payload)
    return data


def _identity(value: Mapping[str, Any], expected_uid: str | None, payload: Any) -> str:
    identities = [
        obj[name]
        for obj in (value, _map(value.get("contact")), _map(value.get("finderUserInfo")))
        for name in ("username", "userName", "finder_username")
        if obj.get(name) not in (None, "")
    ]
    if not identities:
        _error("response omitted author identity", payload, identity=True)
    try:
        distinct = {_text_id(item, finder=True) for item in identities}
    except CaptureError:
        _error("response contains an invalid author identity", payload, identity=True)
    if len(distinct) != 1 or (expected_uid is not None and distinct != {expected_uid}):
        _error("returned author does not match requested identity", payload, identity=True)
    return next(iter(distinct))


def _counter(value: Mapping[str, Any], *names: str) -> tuple[int | None, dict[str, Any]]:
    present = [name for name in names if name in value]
    name = present[0] if present else None
    raw = value.get(name) if name else None
    parsed = int(raw) if isinstance(raw, str) and _DIGITS.fullmatch(raw) else raw
    valid = type(parsed) is int and parsed >= 0
    reason = "missing" if raw is None else "provided" if valid else "invalid"
    if len(present) > 1:
        numbers = [int(value[n]) if isinstance(value[n], str) and _DIGITS.fullmatch(value[n]) else value[n] for n in present]
        if any(type(n) is not int or n != parsed for n in numbers):
            reason = "invalid"
    return (parsed if reason == "provided" else None), {
        "status": reason, "reason": reason, "source_field": name,
    }


def _metrics(value: Mapping[str, Any]) -> dict[str, Any]:
    aliases = {
        "view_count": ("readCount", "read_count"),
        "like_count": ("likeCount", "like_count"),
        "comment_count": ("commentCount", "comment_count"),
        "collect_count": ("favCount", "fav_count"),
        "share_count": ("forwardCount", "forward_count"),
    }
    output: dict[str, Any] = {"_field_status": {}}
    for name, fields in aliases.items():
        output[name], output["_field_status"][name] = _counter(value, *fields)
    if output["view_count"] == 0:
        # The contract lists readCount but does not warrant that zero means
        # measured zero views. Live detail responses returned readCount=0
        # alongside nonzero likes/comments/favourites. Preserve the original
        # raw counter, but do not publish an unverified zero-view observation.
        output["view_count"] = None
        output["_field_status"]["view_count"].update(
            status="invalid", reason="upstream_zero_not_authoritative")
    return output


def _time(value: Any) -> str | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        number = float(value)
        if number > 100_000_000_000:
            number /= 1000
        return datetime.fromtimestamp(number, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _page(data: Mapping[str, Any], payload: Any, *, comments: bool = False) -> tuple[bool, Any]:
    if comments:
        keys = ("downContinueFlag", "down_continue")
    elif "continueFlag" in data:
        # Captured V2 responses contain both continueFlag/lastBuffer (older
        # posts) and upContinueFlag/upLastbuffer (newer posts). The supplier's
        # prose conflates those directions: upContinueFlag=0 MUST NOT truncate
        # a 15-item page whose continueFlag=1 and total feeds exceed 600.
        keys = ("continueFlag",)
    elif "object" in data:
        _error("raw discovery omitted the backward continueFlag", payload)
    else:
        # Compatibility for the supplier's simplified shape. Our requests use
        # raw=true so the independent backward completion flag is retained.
        keys = ("up_continue",)
    flags = [data[k] for k in keys if k in data]
    if not flags or any(type(v) not in (bool, int) or v not in (0, 1) for v in flags):
        _error("response omitted a valid pagination completion flag", payload)
    if any(bool(v) != bool(flags[0]) for v in flags):
        _error("pagination flags disagree", payload)
    buffers = [data[k] for k in ("lastBuffer", "last_buffer") if k in data]
    if len(buffers) > 1 and any(v != buffers[0] for v in buffers):
        _error("pagination buffers disagree", payload)
    buffer = buffers[0] if buffers else None
    if buffer is not None and (not isinstance(buffer, str) or not _BUFFER.fullmatch(buffer)):
        _error("invalid pagination buffer", payload)
    if flags[0] and not buffer:
        _error("response has another page but omitted its buffer", payload)
    if not comments and "continueFlag" not in data and not flags[0] and buffer and data.get("videos"):
        _error("simplified discovery did not prove backward pagination exhaustion", payload)
    return bool(flags[0]), buffer


def parse_reference(payload: Any, expected_channel_id: str) -> dict[str, Any]:
    """Return one candidate; channel-info reverse confirmation is mandatory."""
    requested = _text_id(expected_channel_id, channel=True)
    data = _data(payload)
    echoes = [data[k] for k in ("query", "channel_id") if k in data]
    if not echoes or any(v != requested for v in echoes):
        _error("resolver did not echo requested channel ID", payload, identity=True)
    candidates: set[str] = set()
    if data.get("username"):
        candidates.add(_text_id(data["username"], finder=True))
    def visit_groups(groups: Any) -> None:
        if not isinstance(groups, list):
            return
        for group in groups:
            obj = _map(group)
            for item in obj.get("items") or []:
                row = _map(item)
                for val in (_map(row.get("jumpInfo")).get("userName"), _map(row.get("noticeParam")).get("finderUsername")):
                    if val is not None:
                        candidates.add(_text_id(val, finder=True))
            visit_groups(obj.get("subBoxes"))
    visit_groups(data.get("data"))
    if len(candidates) != 1:
        _error("resolver did not produce exactly one finder candidate", payload, identity=True)
    uid = next(iter(candidates))
    return {"uid": uid, "account_uid": uid, "provider_reference": uid,
            "channel_id": requested, "identity_verified": False,
            "requires_channel_confirmation": True}


def parse_channel_info(payload: Any, expected_uid: str, expected_channel_id: str | None = None) -> dict[str, Any]:
    """Confirm the finder/public-ID pair using account information, not name."""
    uid = _text_id(expected_uid, finder=True)
    data = _data(payload)
    # Raw channel-info omits username; the supplier's request echo binds it.
    echoed = _map(payload).get("params")
    identities = {str(v) for v in (data.get("finder_username"), data.get("username"), _map(echoed).get("username")) if v not in (None, "")}
    if identities != {uid}:
        _error("channel information lacks its finder request binding", payload, identity=True)
    ids = {str(data[k]) for k in ("channel_id",) if data.get(k)}
    info = _map(data.get("info"))
    # The live upstream uses the English title even when nearby labels are
    # Chinese. Accept these two evidenced labels, never an arbitrary ID field.
    for label in ("视频号ID", "Channels ID"):
        if info.get(label):
            ids.add(str(info[label]))
    for section in data.get("sections") or []:
        for item in _map(section).get("items") or []:
            if _map(item).get("title") in {"视频号ID", "Channels ID"}:
                ids.add(str(_map(item).get("content", "")))
    if len(ids) != 1:
        _error("channel information omitted an unambiguous public ID", payload, identity=True)
    channel_id = _text_id(next(iter(ids)), channel=True)
    if expected_channel_id is not None and channel_id != _text_id(expected_channel_id, channel=True):
        _error("finder public ID does not match the requested channel", payload, identity=True)
    return {"uid": uid, "account_uid": uid, "channel_id": channel_id, "identity_verified": True}


def parse_profile(payload: Any, expected_uid: str) -> dict[str, Any]:
    uid = _text_id(expected_uid, finder=True)
    data = _data(payload)
    envelope = _map(payload)
    if envelope.get("router") not in (None, "", _BASE + "fetch_user_profile"):
        _error("profile response route differs from its request", payload, identity=True)
    params = envelope.get("params")
    if params is not None:
        if not isinstance(params, Mapping):
            _error("profile request echo is malformed", payload, identity=True)
        if "username" in params and params["username"] != uid:
            _error("profile request echo has a different finder identity", payload, identity=True)
    _identity(data, uid, payload)
    contact = _map(data.get("contact"))
    nickname = contact.get("nickname", data.get("nickname"))
    if not isinstance(nickname, str) or not nickname.strip():
        _error("profile omitted nickname", payload)
    metrics: dict[str, Any] = {"_field_status": {}}
    for name, fields in {
        "follower_count": ("fansCount", "fans_count"),
        "platform_work_count": ("feedsCount", "feeds_count"),
        "like_count": ("feedsLikeCount", "like_count"),
        "collect_count": ("feedsFavCount", "fav_count"),
        "share_count": ("feedsForwardCount", "forward_count"),
    }.items():
        metrics[name], metrics["_field_status"][name] = _counter(data, *fields)
        if name != "platform_work_count" and metrics[name] == 0:
            # The official profile contract explicitly reports unavailable
            # fan/aggregate counters as zero for some accounts. Two real
            # profiles with 617/1123 feeds exhibited that exact shape. Keep
            # the zero in the immutable raw response, never overwrite a
            # previously observed positive value with an unproven zero.
            metrics[name] = None
            metrics["_field_status"][name].update(
                status="invalid", reason="upstream_zero_not_authoritative")
    return {"uid": uid, "account_uid": uid, "nickname": nickname, "account_name": nickname,
            "provider_reference": uid, "follower_count": metrics["follower_count"],
            "metrics": metrics, "signature": contact.get("signature", data.get("signature")),
            "avatar_url": contact.get("headUrl", data.get("head_url"))}


def _video(item: Mapping[str, Any], payload: Any, expected_uid: str | None = None) -> dict[str, Any]:
    uid = _identity(item, expected_uid, payload)
    ids = [_text_id(item[k]) for k in ("id", "objectId", "object_id") if item.get(k) not in (None, "")]
    if not ids or len(set(ids)) != 1:
        _error("video omitted an exact unambiguous object ID", payload, identity=True)
    desc = _map(item.get("objectDesc", item.get("object_desc")))
    media = desc.get("media", item.get("media"))
    media = [media] if isinstance(media, Mapping) else media if isinstance(media, list) else []
    evidence = []
    # The supplier's pinned real video fixture uses media_type=4. Unknown
    # kinds must not become playable videos merely because they have a URL.
    is_video = len(media) == 1 and _map(media[0]).get("mediaType", _map(media[0]).get("media_type")) in (4, "4")
    for entry in media:
        row = _map(entry)
        url, token = row.get("url"), row.get("urlToken", row.get("url_token", ""))
        full = row.get("full_url") or (url + token if isinstance(url, str) and isinstance(token, str) else None)
        key = row.get("decodeKey", row.get("decode_key"))
        evidence.append({"url": url, "url_token": token, "full_url": full,
                         "decode_key": str(key) if isinstance(key, (int, str)) and not isinstance(key, bool) else None,
                         "status": "requires_decryption"})
    title = item.get("title", desc.get("description", ""))
    canonical = item.get("share_url")
    if not isinstance(canonical, str) or not re.fullmatch(r"https://weixin\.qq\.com/sph/[A-Za-z0-9]+/?", canonical):
        canonical = ""
    return {"platform": PLATFORM, "platform_content_id": ids[0], "canonical_url": canonical,
            "title": title if isinstance(title, str) else "", "body": title if isinstance(title, str) else "",
            "published_at": _time(item.get("createtime", item.get("createTime", item.get("create_time")))),
            "content_type": "video" if is_video else "unknown", "account_uid": uid,
            "account_name": _map(item.get("contact")).get("nickname", item.get("nickname", "")),
            "media_urls": [], "media_evidence": evidence,
            "media_processing_status": "requires_decryption" if is_video and evidence else "content_type_unresolved",
            "metrics": _metrics(item)}


def parse_discovery(payload: Any, expected_uid: str) -> dict[str, Any]:
    uid = _text_id(expected_uid, finder=True)
    data = _data(payload)
    header_sources = (data, _map(data.get("contact")), _map(data.get("finderUserInfo")))
    has_header_identity = any(
        source.get(key) not in (None, "")
        for source in header_sources for key in ("username", "userName", "finder_username")
    )
    if has_header_identity:
        _identity(data, uid, payload)
    else:
        # Continuations may omit profile headers. A matching request echo plus
        # explicit raw upstream success binds the page; every item below still
        # independently has to carry the same author. The error-only response
        # observed during the live probe cannot satisfy these conditions.
        envelope = _map(payload)
        if (_map(envelope.get("params")).get("username") != uid
                or envelope.get("router") != _BASE + "fetch_user_videos"
                or type(_map(data.get("baseResponse")).get("ret")) is not int
                or _map(data.get("baseResponse"))["ret"] != 0):
            _error("discovery lacks an exact author or request binding", payload, identity=True)
    raw = data.get("object", data.get("videos"))
    if not isinstance(raw, list) or any(not isinstance(row, Mapping) for row in raw):
        _error("discovery omitted a video array", payload)
    items = [_video(row, payload, uid) for row in raw]
    if len({row["platform_content_id"] for row in items}) != len(items):
        _error("discovery repeated an object within one page", payload)
    has_more, buffer = _page(data, payload)
    return {"items": items, "has_more": has_more, "next_cursor": buffer}


def parse_stage(stage: str, content_id: str, payload: Any, expected_uid: str | None = None) -> dict[str, Any]:
    requested = _text_id(content_id)
    if expected_uid is not None:
        expected_uid = _text_id(expected_uid, finder=True)
    data = _data(payload)
    if stage in {"detail", "metrics"}:
        objects = data.get("objects", [data])
        if not isinstance(objects, list) or len(objects) != 1 or not isinstance(objects[0], Mapping):
            _error("detail did not return exactly one requested video", payload)
        item = _video(objects[0], payload, expected_uid)
        if item["platform_content_id"] != requested:
            _error("detail returned a different object", payload, identity=True)
        return item["metrics"] if stage == "metrics" else item
    if stage != "comments":
        _error("unsupported stage", payload)
    # Raw comment responses bind to the supplier's echoed request params.
    request_context = _map(_map(payload).get("params"))
    bindings = [value for value in (data.get("object_id"), request_context.get("object_id")) if value not in (None, "")]
    if not bindings or any(_text_id(value) != requested for value in bindings):
        _error("comments returned a different object", payload, identity=True)
    rows = data.get("commentInfo", data.get("comments"))
    if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
        _error("comments omitted their array", payload)
    context = request_context
    if data.get("comment_id") not in (None, "") and context.get("comment_id") not in (None, "") and str(data["comment_id"]) != str(context["comment_id"]):
        _error("comments returned a different reply context", payload, identity=True)
    parent = data.get("comment_id", context.get("comment_id"))
    parent = _text_id(parent) if parent not in (None, "", 0, "0") else None
    comments = []
    for row in rows:
        body = row.get("content")
        if not isinstance(body, str) or not body.strip():
            _error("comment omitted its text", payload)
        user = row.get("username")
        if not isinstance(user, str) or not user:
            _error("comment omitted its user identity", payload)
        cid = _text_id(row.get("commentId", row.get("comment_id")))
        comments.append({"platform_comment_id": cid, "raw_user_id": user,
                         "body": " ".join(body.split())[:2000], "parent_comment_id": parent,
                         "published_at": _time(row.get("createtime", row.get("create_time"))),
                         "like_count": _counter(row, "likeCount", "like_count")[0]})
    has_more, buffer = _page(data, payload, comments=True)
    total = _counter(_map(data.get("monotonicData")), "commentCount")[0]
    next_params = {"last_buffer": buffer} if has_more else None
    if next_params is not None and parent is not None:
        next_params["comment_id"] = parent
    return {"comments": comments, "comment_count": total, "declared_total": total,
            "has_more": has_more, "next_cursor": buffer, "next_cursor_params": next_params}
