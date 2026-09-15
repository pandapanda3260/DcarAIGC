"""Pure TikHub Kuaishou App request and response contracts.

Requests were checked against the official endpoint documentation on 2026-09-12:
https://docs.tikhub.io/467698476e0 (profile), 467698477e0 (posts),
467698469e0 (detail). Those OpenAPI schemas leave ``data`` untyped; parsers
therefore accept explicit evidenced shapes, never recursive guessed aliases.
The profile shape is backed by the existing complete 2026-09-10 local response;
feeds and photos by complete 2026-09-12 provider responses retained with receipts.
Transport, paid-request accounting and raw-response persistence belong to callers.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, NoReturn
from urllib.parse import parse_qs, urlsplit

from .capture import CaptureError


PROFILE_PATH = "/api/v1/kuaishou/app/fetch_one_user_v2"
DISCOVERY_PATH = "/api/v1/kuaishou/app/fetch_user_post_v2"
DETAIL_PATH = "/api/v1/kuaishou/app/fetch_one_video"
SUPPORTED_STAGES = frozenset({"profile", "account_metrics", "discovery", "detail", "metrics"})


def _fail(message: str, payload: Any, *, identity: bool = False) -> NoReturn:
    raise CaptureError(
        f"TikHub Kuaishou {message}",
        retryable=not identity,
        error_code="identity_conflict" if identity else "invalid_response",
        http_status=200,
        billed=True,
        raw_response=payload,
    )


def _identifier(value: Any) -> str:
    # Strings retain every leading zero. Float/scientific-notation IDs are unsafe.
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return ""
    text = str(value).strip()
    return text if re.fullmatch(r"[A-Za-z0-9]+", text, flags=re.ASCII) else ""


def _uid(value: Any) -> str:
    text = _identifier(value)
    return text if re.fullmatch(r"[0-9]+", text, flags=re.ASCII) else ""


def request_spec(stage: str, subject: Any, cursor: Any = None) -> Dict[str, Any]:
    """Build a request without secrets, network access, or implicit ID conversion.

    ``subject`` is a numeric account UID for profile/discovery and a numeric or
    short content ID for detail/metrics. Resolving an eid into UID is a separate
    identity operation, not an implicit fallback from a display handle.
    """
    if stage not in SUPPORTED_STAGES:
        raise ValueError(f"Unsupported Kuaishou stage: {stage}")
    if stage in {"profile", "account_metrics", "discovery"}:
        identity = _uid(subject)
        if not identity:
            raise ValueError("Kuaishou account capture requires an exact numeric UID")
        params: Dict[str, Any] = {"user_id": identity}
        path = PROFILE_PATH
        if stage == "discovery":
            if cursor is not None and not isinstance(cursor, str):
                raise ValueError("Kuaishou pcursor must be an opaque string")
            if cursor == "no_more":
                raise ValueError("Kuaishou terminal cursor cannot request another page")
            params.update(pcursor=cursor or "", sort="latest")
            path = DISCOVERY_PATH
    else:
        identity = _identifier(subject)
        if not identity:
            raise ValueError("Kuaishou content capture requires an exact photo ID")
        path, params = DETAIL_PATH, {"photo_id": identity}
    return {"method": "GET", "path": path, "params": params}


def _data(payload: Any, path: str, parameter: str, expected: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping) or type(payload.get("code")) is not int:
        _fail("response omitted its success envelope", payload)
    if payload["code"] != 200:
        raise CaptureError(
            "TikHub Kuaishou provider returned an unsuccessful response",
            retryable=True, error_code="upstream_error", http_status=200,
            billed=False, raw_response=payload,
        )
    if payload.get("router") not in (None, "", path):
        _fail("response endpoint does not match request", payload, identity=True)
    params = payload.get("params")
    if params is not None:
        if not isinstance(params, Mapping):
            _fail("response parameters are malformed", payload)
        if parameter in params and _identifier(params[parameter]) != expected:
            _fail("response request identity does not match", payload, identity=True)
    data = payload.get("data")
    if not isinstance(data, Mapping):
        _fail("response omitted upstream data", payload)
    if type(data.get("result")) is not int or data["result"] != 1:
        _fail("response did not prove upstream success", payload)
    return data


def _count(raw: Any) -> Any:
    if isinstance(raw, str) and re.fullmatch(r"[0-9]+", raw, flags=re.ASCII):
        return int(raw)
    return raw if isinstance(raw, int) and not isinstance(raw, bool) and raw >= 0 else None


def _counts(value: Mapping[str, Any], fields: Mapping[str, str]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    statuses = {}
    for field, source in fields.items():
        raw = value.get(source)
        count = _count(raw)
        state = "missing" if raw is None else "provided" if count is not None else "invalid"
        result[field] = count
        statuses[field] = {"status": state, "reason": state, "source_field": source}
    result["_field_status"] = statuses
    return result


def parse_profile(payload: Any, expected_uid: str) -> Dict[str, Any]:
    uid = _uid(expected_uid)
    if not uid:
        raise ValueError("Kuaishou profile parsing requires an exact numeric UID")
    data = _data(payload, PROFILE_PATH, "user_id", uid)
    user = data.get("userProfile")
    profile = user.get("profile") if isinstance(user, Mapping) else None
    if not isinstance(profile, Mapping):
        _fail("profile response omitted userProfile.profile", payload)
    if _uid(profile.get("user_id")) != uid:
        _fail("profile UID does not match requested account", payload, identity=True)
    name = profile.get("user_name")
    if not isinstance(name, str) or not name.strip():
        _fail("profile omitted its user name", payload)
    owner_count = user.get("ownerCount")
    counts = _counts(owner_count if isinstance(owner_count, Mapping) else {}, {
        "follower_count": "fan", "platform_work_count": "photo_public",
        "total_likes": "total_photo_like",
    })
    return {
        "uid": uid, "account_uid": uid, "account_name": name,
        "description": profile.get("user_text") if isinstance(profile.get("user_text"), str) else None,
        "avatar_url": profile.get("headurl") if isinstance(profile.get("headurl"), str) else None,
        "metrics": {key: value for key, value in counts.items() if key != "_field_status"},
        "field_status": counts["_field_status"],
        **counts,
    }


def _published_at(value: Any) -> Any:
    timestamp = _count(value)
    if timestamp is None or timestamp == 0:
        return None
    if timestamp > 100_000_000_000:
        timestamp /= 1000
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError):
        return None


_METRICS = {field: field for field in (
    "view_count", "comment_count", "like_count", "share_count", "collect_count",
)}


def _photo_eid(item: Mapping[str, Any]) -> str:
    share = item.get("share_info")
    if not isinstance(share, str):
        return ""
    values = parse_qs(share, keep_blank_values=True).get("photoId", [])
    return _identifier(values[0]) if len(values) == 1 else ""


def _media(item: Mapping[str, Any]) -> list[str]:
    # Only the observed video source field; cover, avatar and music are unrelated.
    sources = item.get("main_mv_urls")
    urls: list[str] = []
    if not isinstance(sources, list):
        return urls
    for source in sources:
        raw = source.get("url") if isinstance(source, Mapping) else None
        if not isinstance(raw, str):
            continue
        try:
            parsed = urlsplit(raw)
        except ValueError:
            continue
        if parsed.scheme in {"http", "https"} and parsed.hostname and raw not in urls:
            urls.append(raw)
    return urls


def _item(item: Any, payload: Any, expected_uid: str | None) -> Dict[str, Any]:
    if not isinstance(item, Mapping):
        _fail("feed contains a malformed work", payload)
    photo_id = _identifier(item.get("photo_id"))
    uid = _uid(item.get("user_id"))
    if not photo_id or not uid:
        _fail("work omitted its exact photo ID or author UID", payload, identity=True)
    if expected_uid is not None and uid != expected_uid:
        _fail("work author does not match requested account", payload, identity=True)
    media_urls = _media(item)
    # The evidenced type=1 responses contain main_mv_urls. Other types remain
    # unknown until their image/audio contracts have been checked.
    content_type = "video" if type(item.get("type")) is int and item["type"] == 1 else "unknown"
    caption = item.get("caption") if isinstance(item.get("caption"), str) else ""
    return {
        "platform": "kuaishou", "platform_content_id": photo_id,
        "canonical_url": f"https://www.kuaishou.com/short-video/{_photo_eid(item) or photo_id}",
        "title": caption, "body": caption,
        "published_at": _published_at(item.get("timestamp")),
        "content_type": content_type, "account_uid": uid,
        "account_name": item.get("user_name") if isinstance(item.get("user_name"), str) else "",
        "media_urls": media_urls if content_type == "video" else [],
        "metrics": _counts(item, _METRICS),
    }


def parse_discovery(payload: Any, expected_uid: str) -> Dict[str, Any]:
    """Parse the exact App V2 feeds page, retaining its opaque pcursor."""
    uid = _uid(expected_uid)
    if not uid:
        raise ValueError("Kuaishou discovery parsing requires an exact numeric UID")
    data = _data(payload, DISCOVERY_PATH, "user_id", uid)
    feeds = data.get("feeds")
    if not isinstance(feeds, list):
        _fail("discovery response omitted feeds", payload)
    cursor = data.get("pcursor")
    if not isinstance(cursor, str) or not cursor.strip():
        _fail("discovery response omitted an explicit pagination cursor", payload)
    params = payload.get("params")
    if not isinstance(params, Mapping) or _uid(params.get("user_id")) != uid:
        # In particular, an empty page has no author row to prove its subject.
        _fail("discovery response omitted its exact requested UID", payload, identity=True)
    if isinstance(params, Mapping):
        if params.get("sort") not in (None, "latest"):
            _fail("discovery response was not ordered by latest", payload)
        if cursor != "no_more" and cursor == params.get("pcursor"):
            _fail("discovery response repeated the request cursor", payload)
    items: list[Dict[str, Any]] = []
    seen: dict[str, Mapping[str, Any]] = {}
    for raw in feeds:
        normalized = _item(raw, payload, uid)
        identity = normalized["platform_content_id"]
        if identity in seen:
            if dict(seen[identity]) != dict(raw):
                _fail("discovery contains conflicting versions of the same work", payload)
            continue
        seen[identity] = raw
        items.append(normalized)
    has_more = cursor != "no_more"
    return {
        "items": items, "has_more": has_more,
        "next_cursor": cursor if has_more else None,
        "pagination_evidence": {"source_field": "data.pcursor", "value": cursor},
    }


def parse_stage(
    stage: str, content_id: str, payload: Any, expected_uid: str | None = None,
) -> Dict[str, Any]:
    """Normalize one ``data.photos[0]`` detail or metrics response.

    Detail and metrics deliberately share the same endpoint. The runtime may
    derive metrics from a persisted detail result without a second paid call.
    """
    if stage not in {"detail", "metrics"}:
        raise ValueError(f"Unsupported Kuaishou content stage: {stage}")
    requested_id = _identifier(content_id)
    uid = _uid(expected_uid) if expected_uid is not None else None
    if not requested_id or (expected_uid is not None and not uid):
        raise ValueError("Kuaishou detail parsing requires exact content/account IDs")
    data = _data(payload, DETAIL_PATH, "photo_id", requested_id)
    photos = data.get("photos")
    if not isinstance(photos, list) or len(photos) != 1 or not isinstance(photos[0], Mapping):
        _fail("single-work response omitted exactly one photos entry", payload)
    photo = photos[0]
    actual_id = _identifier(photo.get("photo_id"))
    if requested_id not in {actual_id, _photo_eid(photo)}:
        _fail("detail photo ID does not match requested work", payload, identity=True)
    normalized = _item(photo, payload, uid)
    if stage == "metrics":
        return {
            **normalized["metrics"],
            "account_uid": normalized["account_uid"],
            "platform_content_id": normalized["platform_content_id"],
        }
    return normalized
