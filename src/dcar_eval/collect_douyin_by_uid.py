#!/usr/bin/env python3
"""Collect recent public Douyin posts by numeric account UID.

The collector uses a transient guest ``ttwid`` and a locally generated
``a_bogus`` signature.  Neither value is written to disk.  Raw public API
responses and normalized records are cached so subsequent runs make no
network calls unless ``--refresh`` is supplied.

The signature implementation in ``douyin_abogus.py`` is derived from
JoeanAmier/TikTokDownloader and Evil0ctal/Douyin_TikTok_Download_API; see that
file for attribution and license notices.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import http.client
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Iterable
import urllib.error
import urllib.parse
import urllib.request

from project_paths import CONFIG_DIR, DOUYIN_DEPENDENCY_DIR, DOUYIN_PUBLIC_CACHE_DIR


LOCAL_DEPS = DOUYIN_DEPENDENCY_DIR
if LOCAL_DEPS.exists():
    sys.path.insert(0, str(LOCAL_DEPS))

try:
    from douyin_abogus import ABogus
except ModuleNotFoundError as exc:
    if exc.name == "gmssl":
        raise SystemExit(
            "缺少 gmssl。请先执行：python3 -m pip install --target "
            f"'{LOCAL_DEPS}' -r '{CONFIG_DIR / 'requirements-douyin.txt'}'"
        ) from exc
    raise


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/90.0.4430.212 Safari/537.36"
)
PROFILE_ENDPOINT = "https://www.douyin.com/aweme/v1/web/user/profile/other/"
POST_ENDPOINT = "https://www.douyin.com/aweme/v1/web/aweme/post/"
TTWID_ENDPOINT = "https://ttwid.bytedance.com/ttwid/union/register/"
TTWID_PAYLOAD = {
    "region": "cn",
    "aid": 1768,
    "needFid": False,
    "service": "www.ixigua.com",
    "migrate_info": {"ticket": "", "source": "node"},
    "cbUrlProtocol": "https",
    "union": True,
}
BASE_PARAMS: dict[str, Any] = {
    "device_platform": "webapp",
    "aid": "6383",
    "channel": "channel_pc_web",
    "pc_client_type": 1,
    "version_code": "290100",
    "version_name": "29.1.0",
    "cookie_enabled": "true",
    "screen_width": 1920,
    "screen_height": 1080,
    "browser_language": "zh-CN",
    "browser_platform": "Win32",
    "browser_name": "Chrome",
    "browser_version": "90.0.4430.212",
    "browser_online": "true",
    "engine_name": "Blink",
    "engine_version": "90.0.4430.212",
    "os_name": "Windows",
    "os_version": "10",
    "cpu_core_num": 12,
    "device_memory": 8,
    "platform": "PC",
    "downlink": "10",
    "effective_type": "4g",
    "from_user_page": "1",
    "locate_query": "false",
    "need_time_list": "1",
    "pc_libra_divert": "Windows",
    "publish_video_strategy_type": "2",
    "round_trip_time": "0",
    "show_live_replay_strategy": "1",
    "time_list_query": "0",
    "whale_cut_token": "",
    "update_version_code": "170400",
    "msToken": "",
}


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    path.chmod(0o600)


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    path.chmod(0o600)


def first_url(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    urls = value.get("url_list")
    if isinstance(urls, list):
        return next((str(item) for item in urls if item), "")
    return ""


def image_urls(item: dict[str, Any]) -> list[str]:
    output: list[str] = []
    for image in item.get("images") or []:
        if not isinstance(image, dict):
            continue
        url = first_url(image) or first_url(image.get("display_image"))
        if url and url not in output:
            output.append(url)
    return output


def media_urls(item: dict[str, Any]) -> tuple[str, str, list[str]]:
    video = item.get("video") if isinstance(item.get("video"), dict) else {}
    images = image_urls(item)
    cover = first_url(video.get("cover")) or first_url(video.get("origin_cover"))
    if not cover and images:
        cover = images[0]
    video_url = first_url(video.get("play_addr")) or first_url(video.get("play_addr_h264"))
    if not video_url:
        for bit_rate in video.get("bit_rate") or []:
            if isinstance(bit_rate, dict):
                video_url = first_url(bit_rate.get("play_addr"))
                if video_url:
                    break
    return cover, video_url, images


def cn_iso(timestamp: Any) -> str:
    try:
        zone = dt.timezone(dt.timedelta(hours=8))
        return dt.datetime.fromtimestamp(int(timestamp), zone).isoformat()
    except (TypeError, ValueError, OSError):
        return ""


def normalize_post(uid: str, profile: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    user = profile.get("user") or {}
    author = item.get("author") or {}
    stats = item.get("statistics") or {}
    cover_url, video_url, images = media_urls(item)
    aweme_id = str(item.get("aweme_id") or "")
    # Use a canonical URL instead of the provider response's tracking parameters.
    share_url = f"https://www.douyin.com/video/{aweme_id}" if aweme_id else ""
    content_type = "image_text" if images else ("video" if video_url else "unknown")
    return {
        "uid": uid,
        "sec_uid": str(user.get("sec_uid") or ""),
        "account_name": str(user.get("nickname") or ""),
        "unique_id": str(user.get("unique_id") or ""),
        "aweme_id": aweme_id,
        "desc": str(item.get("desc") or ""),
        "create_time": int(item.get("create_time") or 0),
        "create_time_cn": cn_iso(item.get("create_time")),
        "content_type": content_type,
        "media_type": item.get("media_type"),
        "is_top": bool(item.get("is_top")),
        "duration_ms": (item.get("video") or {}).get("duration"),
        "digg_count": stats.get("digg_count"),
        "comment_count": stats.get("comment_count"),
        "collect_count": stats.get("collect_count"),
        "share_count": stats.get("share_count"),
        "share_url": share_url,
        "cover_url": cover_url,
        "video_url": video_url,
        "image_urls": images,
        "returned_author_uid": str(author.get("uid") or ""),
    }


class DouyinPublicClient:
    def __init__(self, timeout: int = 45) -> None:
        self.timeout = timeout
        self._ttwid_cookie = ""

    def _guest_cookie(self) -> str:
        if self._ttwid_cookie:
            return self._ttwid_cookie
        request = urllib.request.Request(
            TTWID_ENDPOINT,
            data=json.dumps(TTWID_PAYLOAD, separators=(",", ":")).encode("utf-8"),
            headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            set_cookie = response.headers.get("Set-Cookie", "")
        cookie = set_cookie.split(";", 1)[0]
        if not cookie.startswith("ttwid="):
            raise RuntimeError("未能取得游客 ttwid")
        self._ttwid_cookie = cookie
        return cookie

    def signed_get(self, endpoint: str, params: dict[str, Any], referer: str) -> dict[str, Any]:
        signature = urllib.parse.quote(ABogus().get_value(params), safe="")
        url = f"{endpoint}?{urllib.parse.urlencode(params)}&a_bogus={signature}"
        last_error: Exception | None = None
        for attempt in range(3):
            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Encoding": "identity",
                    "Accept-Language": "zh-CN,zh;q=0.9",
                    "Connection": "close",
                    "Referer": referer,
                    "Cookie": self._guest_cookie(),
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    body = response.read()
                if not body:
                    raise RuntimeError("抖音接口返回空响应，可能触发临时风控")
                return json.loads(body)
            except (http.client.IncompleteRead, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = exc
                time.sleep(0.8 * (attempt + 1))
        assert last_error is not None
        raise last_error

    def profile(self, uid: str) -> dict[str, Any]:
        params = dict(BASE_PARAMS)
        params.update({"sec_user_id": "", "source": "publish", "user_id": uid})
        return self.signed_get(PROFILE_ENDPOINT, params, "https://www.douyin.com/")

    def recent_posts(self, sec_uid: str, count: int) -> dict[str, Any]:
        params = dict(BASE_PARAMS)
        params.update({"sec_user_id": sec_uid, "max_cursor": 0, "count": count})
        return self.signed_get(
            POST_ENDPOINT,
            params,
            f"https://www.douyin.com/user/{sec_uid}",
        )


def collect_uid(
    client: DouyinPublicClient,
    uid: str,
    cache_dir: Path,
    count: int,
    refresh: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    account_dir = cache_dir / "accounts" / uid
    profile_path = account_dir / "profile_raw.json"
    posts_path = account_dir / "posts_page_001_raw.json"
    from_cache = profile_path.exists() and posts_path.exists() and not refresh
    if from_cache:
        profile = json.loads(profile_path.read_text(encoding="utf-8"))
        posts = json.loads(posts_path.read_text(encoding="utf-8"))
    else:
        profile = client.profile(uid)
        user = profile.get("user") or {}
        if profile.get("status_code") != 0 or str(user.get("uid") or "") != uid:
            raise RuntimeError(f"UID {uid} 账号解析校验失败")
        sec_uid = str(user.get("sec_uid") or "")
        if not sec_uid:
            raise RuntimeError(f"UID {uid} 未返回 sec_uid")
        posts = client.recent_posts(sec_uid, count)
        write_json(profile_path, profile)
        write_json(posts_path, posts)

    # Validate cached responses too, so a stale or corrupted cache cannot be
    # silently attributed to the requested UID.
    user = profile.get("user") or {}
    if profile.get("status_code") != 0 or str(user.get("uid") or "") != uid:
        raise RuntimeError(f"UID {uid} 账号解析校验失败")
    if not str(user.get("sec_uid") or ""):
        raise RuntimeError(f"UID {uid} 未返回 sec_uid")
    if posts.get("status_code") != 0:
        raise RuntimeError(f"UID {uid} 作品接口状态异常: {posts.get('status_code')}")

    items = posts.get("aweme_list") or []
    normalized = [normalize_post(uid, profile, item) for item in items if isinstance(item, dict)]
    normalized.sort(key=lambda row: row["create_time"], reverse=True)
    mismatches = sorted({row["returned_author_uid"] for row in normalized if row["returned_author_uid"] != uid})
    if mismatches:
        raise RuntimeError(f"UID {uid} 作品作者校验失败: {mismatches}")
    user = profile.get("user") or {}
    account = {
        "uid": uid,
        "sec_uid": str(user.get("sec_uid") or ""),
        "account_name": str(user.get("nickname") or ""),
        "unique_id": str(user.get("unique_id") or ""),
        "reported_aweme_count": user.get("aweme_count"),
        "collected_recent_count": len(normalized),
        "has_more": bool(posts.get("has_more")),
        "next_cursor": posts.get("max_cursor"),
        "cache_used": from_cache,
        "collection_scope": "latest_first_page",
    }
    write_json(account_dir / "account.json", account)
    write_jsonl(account_dir / "posts.jsonl", normalized)
    return account, normalized


def main() -> int:
    parser = argparse.ArgumentParser(description="按抖音数字 UID 采集最近公开作品")
    parser.add_argument("uids", nargs="+", help="一个或多个抖音数字UID")
    parser.add_argument("--cache-dir", type=Path, default=DOUYIN_PUBLIC_CACHE_DIR)
    parser.add_argument("--count", type=int, default=20, help="每个账号最近作品数，1-20")
    parser.add_argument("--refresh", action="store_true", help="忽略缓存并重新请求")
    parser.add_argument("--delay", type=float, default=0.8, help="账号间请求间隔秒数")
    args = parser.parse_args()
    if not 1 <= args.count <= 20:
        parser.error("--count 必须在 1 到 20 之间")
    uids = list(dict.fromkeys(str(uid).strip() for uid in args.uids))
    if any(not uid.isdigit() for uid in uids):
        parser.error("UID 必须全部为数字")

    cache_dir = args.cache_dir.resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(cache_dir, 0o700)
    except OSError:
        pass
    client = DouyinPublicClient()
    accounts: list[dict[str, Any]] = []
    all_posts: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for index, uid in enumerate(uids):
        current_cache_used = False
        try:
            account, posts = collect_uid(client, uid, cache_dir, args.count, args.refresh)
            current_cache_used = bool(account.get("cache_used"))
            accounts.append(account)
            all_posts.extend(posts)
            print(f"{uid}: {account['account_name']}，采集 {len(posts)} 条，cache={account['cache_used']}")
        except Exception as exc:  # retain per-account progress
            errors.append({"uid": uid, "error": f"{type(exc).__name__}: {exc}"})
            print(f"{uid}: FAILED {errors[-1]['error']}", file=sys.stderr)
        should_delay = index + 1 < len(uids) and not current_cache_used
        if should_delay:
            time.sleep(max(0.0, args.delay))

    all_posts.sort(key=lambda row: (row["uid"], -row["create_time"]))
    write_jsonl(cache_dir / "douyin_accounts.jsonl", accounts)
    write_jsonl(cache_dir / "douyin_posts.jsonl", all_posts)
    unique_aweme_ids = {row["aweme_id"] for row in all_posts if row["aweme_id"]}
    author_uid_mismatches = sum(row["uid"] != row["returned_author_uid"] for row in all_posts)
    write_json(cache_dir / "collection_summary.json", {
        "schema_version": "1.0",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "requested_uids": uids,
        "successful_accounts": len(accounts),
        "failed_accounts": len(errors),
        "normalized_posts": len(all_posts),
        "unique_aweme_ids": len(unique_aweme_ids),
        "duplicate_aweme_ids": len(all_posts) - len(unique_aweme_ids),
        "author_uid_mismatches": author_uid_mismatches,
        "accounts_with_more": sum(bool(account.get("has_more")) for account in accounts),
        "content_type_counts": {
            "video": sum(row["content_type"] == "video" for row in all_posts),
            "image_text": sum(row["content_type"] == "image_text" for row in all_posts),
            "unknown": sum(row["content_type"] == "unknown" for row in all_posts),
        },
        "errors": errors,
        "note": "公开网页接口仅验证最近第一页；has_more=true表示账号仍有更早作品未采集。",
    })
    csv_path = cache_dir / "douyin_posts.csv"
    fields = [
        "uid", "account_name", "unique_id", "aweme_id", "desc", "create_time",
        "create_time_cn", "content_type", "media_type", "is_top", "duration_ms",
        "digg_count", "comment_count", "collect_count", "share_count", "share_url",
        "cover_url", "video_url", "image_urls",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in all_posts:
            out = dict(row)
            out["image_urls"] = " | ".join(row["image_urls"])
            writer.writerow(out)
    csv_path.chmod(0o600)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
