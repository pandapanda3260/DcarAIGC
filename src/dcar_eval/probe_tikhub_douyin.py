#!/usr/bin/env python3
"""Run a minimal, cached TikHub Douyin acceptance probe.

The probe fetches one work detail, its dedicated statistics, and the first
comment page.  Successful and error responses are recursively redacted before
being cached, and an existing cache is reused unless ``--refresh`` is given.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
from typing import Any
import urllib.parse

from project_paths import RAW_RESPONSE_CACHE_DIR
from tikhub_config import (
    DEFAULT_TIKHUB_API_BASE,
    DEFAULT_TIKHUB_CONFIG_FILE,
    TikHubConfigurationError,
    load_tikhub_api_base,
    load_tikhub_api_key,
)


KEY_FILE = DEFAULT_TIKHUB_CONFIG_FILE
# Shared scoring modules import this probe for pure helpers.  Configuration is
# validated by load_key only when a caller actually requests provider access.
BASE_URL = DEFAULT_TIKHUB_API_BASE
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/138.0.0.0 Safari/537.36"
)
SENSITIVE_NAMES = {
    "authorization",
    "cookie",
    "set-cookie",
    "x-api-key",
    "api-key",
    "api_key",
    "token",
}
HTTP_MARKER = "\n__TIKHUB_HTTP_STATUS__="


def load_key(path: Path) -> str:
    try:
        load_tikhub_api_base(path, honor_environment=False)
        return load_tikhub_api_key(path, honor_environment=False)
    except TikHubConfigurationError as error:
        raise RuntimeError(str(error)) from error


def redact(value: Any, secret: str) -> Any:
    if isinstance(value, dict):
        return {
            key: "[REDACTED]"
            if key.lower() in SENSITIVE_NAMES
            else redact(item, secret)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item, secret) for item in value]
    if isinstance(value, str) and secret:
        return value.replace(secret, "[REDACTED]")
    return value


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def fetch(endpoint: str, params: dict[str, Any], key: str) -> tuple[int, Any]:
    query = urllib.parse.urlencode(params)
    url = f"{BASE_URL}{endpoint}?{query}"
    command = [
        "curl",
        "-sS",
        "--http1.1",
        "--connect-timeout",
        "15",
        "--max-time",
        "45",
        "-w",
        HTTP_MARKER + "%{http_code}",
        url,
        "-H",
        f"Authorization: Bearer {key}",
        "-H",
        "Accept: application/json",
        "-H",
        "Accept-Language: zh-CN,zh;q=0.9,en;q=0.8",
        "-H",
        f"User-Agent: {USER_AGENT}",
    ]
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed with exit code {result.returncode}")
    if HTTP_MARKER not in result.stdout:
        raise RuntimeError("TikHub response did not include an HTTP status marker")
    body, raw_status = result.stdout.rsplit(HTTP_MARKER, 1)
    status = int(raw_status.strip())
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        payload = {"parse_error": "response was not valid JSON"}
    return status, payload


def safe_message(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    detail = payload.get("detail")
    if isinstance(detail, dict):
        return str(detail.get("message_zh") or detail.get("message") or "")
    return str(payload.get("message_zh") or payload.get("message") or "")


def run_probe(args: argparse.Namespace) -> list[dict[str, Any]]:
    key = load_key(args.key_file)
    calls = [
        (
            "detail",
            "/api/v1/douyin/app/v3/fetch_one_video",
            {"aweme_id": args.aweme_id},
        ),
        (
            "statistics",
            "/api/v1/douyin/app/v3/fetch_video_statistics",
            {"aweme_ids": args.aweme_id},
        ),
        (
            "comments",
            "/api/v1/douyin/app/v3/fetch_video_comments",
            {"aweme_id": args.aweme_id, "cursor": 0, "count": 20},
        ),
    ]
    summaries: list[dict[str, Any]] = []
    for name, endpoint, params in calls:
        cache_path = args.cache_dir / f"{name}.json"
        source = "cache"
        if cache_path.exists() and not args.refresh:
            record = json.loads(cache_path.read_text(encoding="utf-8"))
        else:
            source = "api"
            status, payload = fetch(endpoint, params, key)
            safe_payload = redact(payload, key)
            record = {
                "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "http_status": status,
                "request": {"endpoint": endpoint, "params": params},
                "response": safe_payload,
            }
            atomic_write_json(cache_path, record)
        payload = record.get("response") or {}
        summaries.append(
            {
                "name": name,
                "source": source,
                "http_status": record.get("http_status"),
                "api_code": payload.get("code") if isinstance(payload, dict) else None,
                "message": safe_message(payload),
                "cache_path": str(cache_path),
            }
        )
        if name == "comments" and args.comment_pages >= 2:
            data = payload.get("data") if isinstance(payload, dict) else None
            cursor = data.get("cursor") if isinstance(data, dict) else None
            has_more = data.get("has_more") if isinstance(data, dict) else None
            if cursor is not None and has_more:
                calls.append(
                    (
                        "comments_page_2",
                        endpoint,
                        {"aweme_id": args.aweme_id, "cursor": cursor, "count": 20},
                    )
                )
    return summaries


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aweme-id", required=True)
    parser.add_argument("--key-file", type=Path, default=KEY_FILE)
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=RAW_RESPONSE_CACHE_DIR / "tikhub_douyin_probe",
    )
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--comment-pages", type=int, choices=(1, 2), default=1)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summaries = run_probe(args)
    print(json.dumps(summaries, ensure_ascii=False, indent=2))
    return 0 if all(item["http_status"] == 200 for item in summaries) else 1


if __name__ == "__main__":
    raise SystemExit(main())
