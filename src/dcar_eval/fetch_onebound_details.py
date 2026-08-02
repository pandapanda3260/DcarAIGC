#!/usr/bin/env python3
"""Safely smoke-test OneBound Xiaohongshu note-detail retrieval.

Reads the blind pilot file, makes a strictly bounded number of requests, and
writes only normalized note content. Credentials and raw provider responses are
never printed or persisted.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from project_paths import ARCHIVE_PROCESSED_DIR, XHS_INPUT_DIR


KEY_ROOT = Path("/Users/mark/Documents/key/DcarKey")
API_NAMES = {"item_get_video_pro", "item_get_video"}
SUCCESS_CODES = {"0000", "0"}
STOP_CODES = {"4003", "4004", "4005", "4006", "4007", "4009", "4010", "4012", "4013", "4014", "4016"}
NOTE_ID_RE = re.compile(r"^[0-9a-fA-F]{24}$")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


def load_env(path: Path) -> Dict[str, str]:
    if path.stat().st_mode & 0o077:
        raise ValueError(f"{path.name} must not be readable by group or other users")
    values: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"Invalid line in {path.name}; expected NAME=value")
        name, value = line.split("=", 1)
        values[name.strip()] = value.strip().strip('"').strip("'")
    key_value = values.get("ONEBOUND_KEY") or values.get("onebound_key")
    secret_value = values.get("ONEBOUND_SECRET") or values.get("onebound_secret")
    if not key_value or not secret_value:
        raise ValueError(f"Missing ONEBOUND_KEY or ONEBOUND_SECRET in {path.name}")
    return {"key": key_value, "secret": secret_value}


def sanitize(value: Any, secrets: Iterable[str], limit: int = 160) -> str:
    text = re.sub(r"[\r\n\t]+", " ", str(value or "")).strip()
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    text = re.sub(r"([?&](?:key|secret)=)[^&\s]+", r"\1[REDACTED]", text, flags=re.I)
    return text[:limit]


def normalize_code(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(int(value))
    return str(value).strip()


def xsec_token_from_url(url: str) -> str:
    query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    return (query.get("xsec_token") or [""])[0]


def request_detail(
    key: str,
    secret: str,
    api_name: str,
    note_id: str,
    xsec_token: str,
    timeout: int,
) -> Tuple[int, Dict[str, Any], bool]:
    params = {"key": key, "secret": secret, "num_iid": note_id, "result_type": "json"}
    if api_name == "item_get_video" and xsec_token:
        params["xsec_token"] = xsec_token
    query = urllib.parse.urlencode(params)
    endpoint = f"https://api-gw.onebound.cn/smallredbook/{api_name}/"
    request = urllib.request.Request(
        f"{endpoint}?{query}",
        headers={"Accept": "application/json", "Accept-Encoding": "identity", "Connection": "close"},
        method="GET",
    )
    opener = urllib.request.build_opener(NoRedirect())
    try:
        with opener.open(request, timeout=timeout) as response:
            status = int(response.status)
            body = response.read()
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        body = exc.read()
    try:
        payload = json.loads(body.decode("utf-8-sig"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return status, {}, False
    if not isinstance(payload, dict):
        return status, {}, False
    return status, payload, True


def first_nonempty(mapping: Dict[str, Any], names: Iterable[str]) -> Any:
    for name in names:
        value = mapping.get(name)
        if value not in (None, "", [], {}):
            return value
    return ""


def item_object(payload: Dict[str, Any]) -> Dict[str, Any]:
    candidate = payload.get("item")
    if isinstance(candidate, list):
        candidate = candidate[0] if candidate else {}
    if isinstance(candidate, dict):
        return candidate
    items = payload.get("items")
    if isinstance(items, dict):
        nested = items.get("item", items)
        if isinstance(nested, list):
            nested = nested[0] if nested else {}
        if isinstance(nested, dict):
            return nested
    return {}


def string_value(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def url_value(value: Any) -> str:
    if isinstance(value, str):
        return value.strip() if value.strip().startswith("https://") else ""
    if isinstance(value, dict):
        for key in ("url", "masterUrl", "master_url", "pic_url", "video_url", "original"):
            result = url_value(value.get(key))
            if result:
                return result
    if isinstance(value, list):
        for entry in value:
            result = url_value(entry)
            if result:
                return result
    return ""


def tag_values(value: Any) -> List[str]:
    if isinstance(value, dict):
        value = list(value.values())
    if not isinstance(value, list):
        value = [value] if isinstance(value, str) else []
    result: List[str] = []
    for entry in value:
        if isinstance(entry, str):
            tag = entry.strip()
        elif isinstance(entry, dict):
            tag = string_value(first_nonempty(entry, ("name", "tagName", "title")))
        else:
            tag = ""
        if tag and tag not in result:
            result.append(tag)
    return result


def image_urls(item: Dict[str, Any]) -> List[str]:
    raw = first_nonempty(item, ("item_imgs", "images", "image_list", "pics"))
    if isinstance(raw, dict):
        raw = list(raw.values())
    if not isinstance(raw, list):
        raw = []
    result: List[str] = []
    for entry in raw:
        value = url_value(entry)
        if value and value not in result:
            result.append(value)
    cover = url_value(first_nonempty(item, ("pic_url", "cover_url", "cover")))
    if cover and not result:
        result.append(cover)
    return result


def normalize_item(item: Dict[str, Any], requested_note_id: str, pilot_id: str, url: str) -> Tuple[Dict[str, Any], str]:
    returned_note_id = string_value(first_nonempty(item, ("num_iid", "note_id", "id")))
    id_status = "not_returned"
    if returned_note_id:
        id_status = "match" if returned_note_id.lower() == requested_note_id.lower() else "mismatch"
    tags = tag_values(first_nonempty(item, ("tag_list", "tags", "topics")))
    parts = urllib.parse.urlsplit(url)
    clean_url = urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    normalized = {
        "pilot_id": pilot_id,
        "note_id": requested_note_id,
        "url": clean_url,
        "title": string_value(first_nonempty(item, ("title", "note_title", "name"))),
        "desc": string_value(first_nonempty(item, ("desc", "description", "content", "note_desc"))),
        "note_type": string_value(first_nonempty(item, ("type", "note_type"))),
        "tags": tags,
        "cover_url": url_value(first_nonempty(item, ("pic_url", "cover_url", "cover"))),
        "image_urls": image_urls(item),
        "video_url": url_value(first_nonempty(item, ("video", "video_url", "video_addr"))),
    }
    return normalized, id_status


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=XHS_INPUT_DIR / "pilot_sample_10_blind.csv")
    parser.add_argument("--env", type=Path, default=KEY_ROOT / "onebound.env")
    parser.add_argument("--api-name", choices=sorted(API_NAMES), default="item_get_video_pro")
    parser.add_argument("--limit", type=int, default=1)
    parser.add_argument("--max-requests", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--prefix", default="onebound_detail_smoke")
    args = parser.parse_args()
    if min(args.limit, args.max_requests, args.timeout) < 1:
        parser.error("numeric limits must be at least 1")

    env = load_env(args.env)
    key, secret = env["key"], env["secret"]
    with args.input.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    selected = [row for row in rows if NOTE_ID_RE.fullmatch(row.get("note_id", ""))][: args.limit]
    result_path = ARCHIVE_PROCESSED_DIR / f"{args.prefix}_results.csv"
    content_path = ARCHIVE_PROCESSED_DIR / f"{args.prefix}_content.jsonl"
    result_rows: List[Dict[str, Any]] = []
    contents: List[Dict[str, Any]] = []
    attempts = 0

    print(json.dumps({"api_name": args.api_name, "request_budget": args.max_requests, "selected_notes": len(selected)}, ensure_ascii=False))
    for row in selected:
        token = row.get("xsec_token", "").strip() or xsec_token_from_url(row["url"])
        if args.api_name == "item_get_video" and not token:
            result_rows.append(
                {
                    "pilot_id": row["pilot_id"], "note_id": row["note_id"], "api_name": args.api_name,
                    "status": "missing_xsec_token", "http_status": "", "error_code": "", "reason": "",
                    "id_status": "not_checked", "title_present": 0, "desc_present": 0, "image_count": 0,
                    "video_present": 0, "request_id": "", "elapsed_seconds": 0,
                }
            )
            print(json.dumps({"pilot_id": row["pilot_id"], "status": "missing_xsec_token", "error_code": ""}, ensure_ascii=False))
            continue
        if attempts >= args.max_requests:
            break
        attempts += 1
        started = time.monotonic()
        status = "request_failed"
        http_status: Any = ""
        error_code = ""
        reason = ""
        request_id = ""
        id_status = "not_checked"
        normalized: Dict[str, Any] = {}
        try:
            http_status, payload, json_ok = request_detail(
                key,
                secret,
                args.api_name,
                row["note_id"],
                token,
                args.timeout,
            )
        except (urllib.error.URLError, socket.timeout, TimeoutError, OSError):
            reason = "network_or_timeout_error"
            json_ok = False
            payload = {}

        if not json_ok:
            status = "http_error" if http_status and int(http_status) != 200 else "invalid_api_response"
        else:
            error_code = normalize_code(payload.get("error_code"))
            reason = sanitize(payload.get("reason") or payload.get("error"), (key, secret))
            request_id = sanitize(payload.get("request_id"), (key, secret), 80)
            api_type = string_value(payload.get("api_type"))
            if http_status != 200:
                status = "http_error"
            elif api_type and api_type != "smallredbook":
                status = "unexpected_api_type"
                reason = "API response api_type did not match smallredbook"
            elif error_code not in SUCCESS_CODES:
                status = "api_error"
            else:
                item = item_object(payload)
                normalized, id_status = normalize_item(item, row["note_id"], row["pilot_id"], row["url"])
                if id_status == "not_returned":
                    status = "detail_id_missing"
                    normalized = {}
                elif id_status == "mismatch":
                    status = "detail_id_mismatch"
                    normalized = {}
                elif not normalized["title"] and not normalized["desc"] and not normalized["image_urls"] and not normalized["video_url"]:
                    status = "detail_empty"
                else:
                    status = "success"
                    contents.append(normalized)

        result_rows.append(
            {
                "pilot_id": row["pilot_id"],
                "note_id": row["note_id"],
                "api_name": args.api_name,
                "status": status,
                "http_status": http_status,
                "error_code": error_code,
                "reason": reason,
                "id_status": id_status,
                "title_present": int(bool(normalized.get("title"))),
                "desc_present": int(bool(normalized.get("desc"))),
                "image_count": len(normalized.get("image_urls", [])),
                "video_present": int(bool(normalized.get("video_url"))),
                "request_id": request_id,
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
        )
        print(json.dumps({"pilot_id": row["pilot_id"], "status": status, "error_code": error_code}, ensure_ascii=False))
        if error_code in STOP_CODES:
            break

    fields = [
        "pilot_id", "note_id", "api_name", "status", "http_status", "error_code", "reason",
        "id_status", "title_present", "desc_present", "image_count", "video_present",
        "request_id", "elapsed_seconds",
    ]
    with result_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(result_rows)
    with content_path.open("w", encoding="utf-8") as handle:
        for content in contents:
            handle.write(json.dumps(content, ensure_ascii=False) + "\n")
    print(json.dumps({"results": result_path.name, "content": content_path.name, "requests": attempts}, ensure_ascii=False))
    return 0 if contents else 2


if __name__ == "__main__":
    raise SystemExit(main())
