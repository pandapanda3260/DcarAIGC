"""Bounded, public-only homepage identity lookup for manual account creation.

No database, capture dispatcher, paid provider, persistent cookie or cache is
used here. A blocked/login/challenge response fails closed; it is never solved
or retried automatically. Transport bypasses environment proxies and pins the
validated public DNS address while retaining TLS hostname verification.
"""

from __future__ import annotations

import http.client
import importlib
import ipaddress
import json
import queue
import re
import socket
import ssl
import threading
import time
from dataclasses import dataclass
from html.parser import HTMLParser
from http.cookies import CookieError, SimpleCookie
from types import ModuleType
from typing import Any, Mapping
from urllib.parse import quote, urlencode, urljoin, urlsplit

from .account_profile_input import ParsedProfile, ProfileInputError, parse_profile_input


MAX_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_REDIRECTS = 4
REQUEST_TIMEOUT = 12.0
LOOKUP_TIMEOUT = 25.0
DNS_TIMEOUT = 3.0
_DNS_SLOTS = threading.BoundedSemaphore(4)
_HOST_PLATFORMS = {
    "douyin.com": "douyin", "www.douyin.com": "douyin", "v.douyin.com": "douyin",
    "xiaohongshu.com": "xiaohongshu", "www.xiaohongshu.com": "xiaohongshu",
    "xhslink.com": "xiaohongshu", "www.xhslink.com": "xiaohongshu",
}
_SHORT_HOSTS = {"v.douyin.com", "xhslink.com", "www.xhslink.com"}
_ALLOWED_HOSTS = {*_HOST_PLATFORMS, "ttwid.bytedance.com"}
_UID = re.compile(r"[0-9]{6,24}")
_SEC_UID = re.compile(r"MS4wLjAB[A-Za-z0-9_-]{32,120}")
_XHS_UID = re.compile(r"[0-9a-fA-F]{24}")
_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
_REDIRECTS = {301, 302, 303, 307, 308}


@dataclass(frozen=True)
class _Response:
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes

    def values(self, name: str) -> list[str]:
        return [value for key, value in self.headers if key.lower() == name.lower()]


def _error(code: str = "public_profile_unavailable") -> ProfileInputError:
    return ProfileInputError(code, "暂时无法公开读取该账号资料，请使用完整主页链接或稍后重试。")


def _remaining(deadline: float) -> float:
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise TimeoutError("public profile deadline exceeded")
    return seconds


def _checked_url(url: str) -> tuple[str, str]:
    if (not isinstance(url, str) or len(url) > 8192 or "\\" in url
            or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in url)):
        raise _error("unsafe_profile_destination")
    try:
        parsed = urlsplit(url)
        if (parsed.scheme != "https" or parsed.hostname not in _ALLOWED_HOSTS
                or parsed.port not in (None, 443) or parsed.username is not None
                or parsed.password is not None or parsed.fragment):
            raise _error("unsafe_profile_destination")
    except ValueError as error:
        if isinstance(error, ProfileInputError):
            raise
        raise _error("unsafe_profile_destination") from error
    return str(parsed.hostname), (parsed.path or "/") + ("?" + parsed.query if parsed.query else "")


def _resolve_public(host: str, deadline: float) -> tuple[Any, ...]:
    """Bound DNS work without permitting unlimited stuck resolver threads."""
    if not _DNS_SLOTS.acquire(blocking=False):
        raise _error("public_profile_busy")
    result: queue.Queue[Any] = queue.Queue(maxsize=1)

    def resolve() -> None:
        try:
            result.put(socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM))
        except Exception as error:
            result.put(error)
        finally:
            _DNS_SLOTS.release()

    threading.Thread(target=resolve, daemon=True, name="public-profile-dns").start()
    try:
        addresses = result.get(timeout=min(DNS_TIMEOUT, _remaining(deadline)))
    except queue.Empty as error:
        raise TimeoutError("public profile DNS timeout") from error
    if isinstance(addresses, Exception):
        raise addresses
    if not addresses:
        raise _error("unsafe_profile_destination")
    for family, kind, _protocol, _canonical, address in addresses:
        ip = ipaddress.ip_address(address[0])
        if (family not in (socket.AF_INET, socket.AF_INET6) or kind != socket.SOCK_STREAM
                or not ip.is_global or (isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped)):
            raise _error("unsafe_profile_destination")
    return tuple(addresses[0])


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host: str, address: tuple[Any, ...], deadline: float):
        self._tls_context = ssl.create_default_context()
        super().__init__(host, timeout=_remaining(deadline), context=self._tls_context)
        self._address = address
        self._deadline = deadline
        self._transport_socket: socket.socket | None = None

    def connect(self) -> None:
        family, kind, protocol, _canonical, address = self._address
        raw = socket.socket(family, kind, protocol)
        self._transport_socket = raw
        self.sock = raw
        try:
            raw.settimeout(_remaining(self._deadline))
            raw.connect(address)
            self.sock = self._tls_context.wrap_socket(raw, server_hostname=self.host)
            self._transport_socket = self.sock
            self.sock.settimeout(_remaining(self._deadline))
        except Exception:
            raw.close()
            raise

    def abort(self) -> None:
        # A wall-clock watchdog also bounds slow header/body trickles. Closing
        # alone does not reliably interrupt a socket read in another thread.
        # http.client clears connection.sock on Connection: close as soon as
        # headers arrive, while HTTPResponse's file still owns the live fd.
        if self._transport_socket is not None:
            try:
                self._transport_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.close()


def _request_once(url: str, *, deadline: float, method: str = "GET",
                  headers: Mapping[str, str] | None = None, body: bytes | None = None) -> _Response:
    host, target = _checked_url(url)
    request_deadline = min(deadline, time.monotonic() + REQUEST_TIMEOUT)
    connection: _PinnedHTTPSConnection | None = None
    response: http.client.HTTPResponse | None = None
    watchdog: threading.Timer | None = None
    try:
        address = _resolve_public(host, request_deadline)
        connection = _PinnedHTTPSConnection(host, address, request_deadline)
        watchdog = threading.Timer(_remaining(request_deadline), connection.abort)
        watchdog.daemon = True
        watchdog.start()
        request_headers = {"User-Agent": _USER_AGENT, "Accept-Encoding": "identity", "Connection": "close"}
        request_headers.update(headers or {})
        connection.request(method, target, body=body, headers=request_headers)
        response = connection.getresponse()
        response_headers = tuple(response.getheaders())
        encoding = response.getheader("Content-Encoding", "identity").lower()
        length = response.getheader("Content-Length")
        if encoding not in ("", "identity") or (length is not None and (not length.isdigit() or int(length) > MAX_RESPONSE_BYTES)):
            raise _error("public_profile_response_invalid")
        chunks: list[bytes] = []
        size = 0
        while True:
            _remaining(request_deadline)
            chunk = response.read1(min(65536, MAX_RESPONSE_BYTES + 1 - size))
            _remaining(request_deadline)
            size += len(chunk)
            if size > MAX_RESPONSE_BYTES:
                raise _error("public_profile_response_too_large")
            if not chunk:
                break
            chunks.append(chunk)
        return _Response(response.status, response_headers, b"".join(chunks))
    except ProfileInputError:
        raise
    except (OSError, TimeoutError, http.client.HTTPException, ValueError) as error:
        raise _error("public_profile_transport_failed") from error
    finally:
        if watchdog is not None:
            watchdog.cancel()
        if response is not None:
            response.close()
        if connection is not None:
            connection.close()


def expand_public_profile_url(url: str) -> str:
    """Only follow bounded official redirects; HTML/JavaScript redirects fail."""
    host, _target = _checked_url(url)
    platform = _HOST_PLATFORMS.get(host)
    if platform is None:
        raise _error("unsafe_profile_destination")
    deadline = time.monotonic() + LOOKUP_TIMEOUT
    seen: set[str] = set()
    current = url
    for hop in range(MAX_REDIRECTS + 1):
        host, _target = _checked_url(current)
        if _HOST_PLATFORMS.get(host) != platform or current in seen:
            raise _error("profile_expansion_failed")
        seen.add(current)
        if host not in _SHORT_HOSTS:
            return parse_profile_input(current).profile_url
        if hop == MAX_REDIRECTS:
            break
        response = _request_once(current, deadline=deadline)
        locations = response.values("Location")
        if response.status not in _REDIRECTS or len(locations) != 1:
            break
        current = urljoin(current, locations[0])
    raise ProfileInputError("profile_expansion_failed", "暂时无法解析分享链接，请粘贴抖音或小红书的完整账号主页链接。")


def _douyin_protocol() -> ModuleType:
    # The existing collector configures its local gmssl directory. Its legacy
    # missing-dependency SystemExit must never terminate the web API process.
    try:
        return importlib.import_module("collect_douyin_by_uid")
    except (ImportError, SystemExit) as error:
        raise ProfileInputError("public_profile_dependency_unavailable", "账号识别组件暂不可用，请联系管理员检查抖音公开资料依赖后重试。") from error


def _json(body: bytes) -> Any:
    def unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate public profile key")
            result[key] = value
        return result

    def invalid_constant(value: str) -> Any:
        raise ValueError("invalid JSON constant")

    try:
        return json.loads(body.decode("utf-8"), object_pairs_hook=unique_keys, parse_constant=invalid_constant)
    except (UnicodeError, ValueError) as error:
        raise _error("public_profile_response_invalid") from error


def _display_id(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str) or len(value) > 128 or any(ord(char) < 32 for char in value):
        raise _error("profile_incomplete")
    return value.strip()


def _nickname(value: Any) -> str:
    if (not isinstance(value, str) or not value.strip() or len(value) > 200
            or any(ord(char) < 32 for char in value)):
        raise ProfileInputError("profile_incomplete", "未能确认账号昵称，请检查主页链接或稍后重试。")
    return value.strip()


def _douyin(parsed: ParsedProfile, deadline: float) -> Mapping[str, Any]:
    protocol = _douyin_protocol()
    guest = _request_once(protocol.TTWID_ENDPOINT, deadline=deadline, method="POST",
                          headers={"Content-Type": "application/json", "User-Agent": protocol.USER_AGENT},
                          body=json.dumps(protocol.TTWID_PAYLOAD).encode("utf-8"))
    if guest.status != 200:
        raise _error()
    cookies: set[str] = set()
    for header in guest.values("Set-Cookie"):
        try:
            cookie = SimpleCookie()
            cookie.load(header)
            if "ttwid" in cookie:
                value = cookie["ttwid"].value
                if re.fullmatch(r"[A-Za-z0-9._%+|=:/-]{1,4096}", value):
                    cookies.add(value)
        except CookieError:
            continue
    if len(cookies) != 1:
        raise _error()
    params = dict(protocol.BASE_PARAMS)
    params.update({"sec_user_id": parsed.sec_user_id or "", "source": "publish"})
    if parsed.uid is not None:
        params["user_id"] = parsed.uid
    try:
        signature = quote(protocol.ABogus().get_value(params), safe="")
    except Exception as error:
        raise _error("public_profile_signing_failed") from error
    url = protocol.PROFILE_ENDPOINT + "?" + urlencode(params) + "&a_bogus=" + signature
    response = _request_once(url, deadline=deadline, headers={
        "User-Agent": protocol.USER_AGENT, "Accept": "application/json", "Referer": parsed.profile_url,
        "Cookie": "ttwid=" + next(iter(cookies)),
    })
    if response.status != 200:
        raise _error()
    payload = _json(response.body)
    if not isinstance(payload, dict) or type(payload.get("status_code")) is not int or payload["status_code"] != 0:
        raise _error()
    user = payload.get("user")
    if not isinstance(user, dict):
        raise _error("profile_incomplete")
    uid, sec = user.get("uid"), user.get("sec_uid")
    if not isinstance(uid, str) or not _UID.fullmatch(uid) or not isinstance(sec, str) or not _SEC_UID.fullmatch(sec):
        raise _error("identity_unresolved")
    if (parsed.uid is not None and parsed.uid != uid) or (parsed.sec_user_id is not None and parsed.sec_user_id != sec):
        raise ProfileInputError("identity_conflict", "平台返回的账号身份与主页不一致，无法新增。")
    return {"platform": "douyin", "uid": uid, "sec_user_id": sec,
            "nickname": _nickname(user.get("nickname")), "display_account_id": _display_id(user.get("unique_id"))}


class _Scripts(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.scripts: list[str] = []
        self._parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "script":
            self._parts = []

    def handle_data(self, data: str) -> None:
        if self._parts is not None:
            self._parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "script" and self._parts is not None:
            self.scripts.append("".join(self._parts))
            self._parts = None


def _initial_state(html: str) -> Mapping[str, Any]:
    scripts = _Scripts()
    scripts.feed(html)
    states: list[Mapping[str, Any]] = []
    for script in scripts.scripts:
        match = re.match(r"\s*window\.__INITIAL_STATE__\s*=\s*", script)
        if match is None:
            continue
        source = script[match.end():].strip().removesuffix(";").rstrip()
        # XHS uses bare undefined in otherwise JSON state. Replace only tokens
        # outside quoted strings; never execute JavaScript or alter nicknames.
        source = re.sub(r'"(?:[^"\\]|\\.)*"|\bundefined\b',
                        lambda token: "null" if token.group() == "undefined" else token.group(), source)
        value = _json(source.encode("utf-8"))
        if not isinstance(value, dict):
            raise _error("profile_incomplete")
        states.append(value)
    if len(states) != 1:
        raise _error("profile_incomplete")
    return states[0]


def _xiaohongshu(parsed: ParsedProfile, deadline: float) -> Mapping[str, Any]:
    response = _request_once(parsed.profile_url, deadline=deadline, headers={"Accept": "text/html"})
    if response.status != 200:
        raise _error()
    try:
        state = _initial_state(response.body.decode("utf-8"))
    except UnicodeError as error:
        raise _error("public_profile_response_invalid") from error
    user = state.get("user")
    page = user.get("userPageData") if isinstance(user, dict) else None
    info = page.get("basicInfo") if isinstance(page, dict) else None
    if not isinstance(page, dict) or not isinstance(info, dict):
        raise _error("profile_incomplete")
    ids = [source["userId"] for source in (page, info) if "userId" in source]
    # Current public homepage state anchors the viewed user in noteQueries,
    # including profiles with no visible notes. Never infer identity from a
    # note's author, which can refer to reposted or recommended content.
    queries = user.get("noteQueries") if isinstance(user, dict) else None
    if queries is not None:
        if not isinstance(queries, list) or any(not isinstance(query, dict) or "userId" not in query for query in queries):
            raise _error("identity_unresolved")
        ids.extend(query["userId"] for query in queries)
    if not ids or any(not isinstance(uid, str) or not _XHS_UID.fullmatch(uid) for uid in ids):
        raise _error("identity_unresolved")
    normalized = {uid.lower() for uid in ids}
    if len(normalized) != 1 or parsed.uid not in normalized:
        raise ProfileInputError("identity_conflict", "平台返回的账号身份与主页不一致，无法新增。")
    return {"platform": "xiaohongshu", "uid": next(iter(normalized)), "sec_user_id": None,
            "nickname": _nickname(info.get("nickname")), "display_account_id": _display_id(info.get("redId"))}


def public_profile_lookup(parsed: ParsedProfile) -> Mapping[str, Any]:
    """Return a verified stable identity or a user-readable failure, never a guess."""
    canonical = parse_profile_input(parsed.profile_url)
    # Local resolution can enrich a sec-UID homepage with its numeric UID (or
    # vice versa). Keep both as constraints on the returned public profile.
    if (canonical.platform != parsed.platform or canonical.profile_url != parsed.profile_url
            or (canonical.uid is not None and canonical.uid != parsed.uid)
            or (canonical.sec_user_id is not None and canonical.sec_user_id != parsed.sec_user_id)
            or (parsed.uid is not None and (not isinstance(parsed.uid, str)
                or not (_UID if parsed.platform == "douyin" else _XHS_UID).fullmatch(parsed.uid)))
            or (parsed.sec_user_id is not None and (parsed.platform != "douyin"
                or not isinstance(parsed.sec_user_id, str) or not _SEC_UID.fullmatch(parsed.sec_user_id)))):
        raise ProfileInputError("identity_conflict", "账号主页与待识别的身份不一致。")
    deadline = time.monotonic() + LOOKUP_TIMEOUT
    if parsed.platform == "douyin":
        return _douyin(parsed, deadline)
    return _xiaohongshu(parsed, deadline)
