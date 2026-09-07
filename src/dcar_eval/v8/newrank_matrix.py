"""Fixed Matrix contracts: bounded pages, not a roster or a media provider.

Retries and persistence live above this adapter. A short nonempty page is not
terminal, and malformed rows retain their positions for the raw-first ledger.
"""

from __future__ import annotations

import copy
import hashlib
import http.client
import json
import math
import os
import re
import stat
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Collection, Mapping
from zoneinfo import ZoneInfo

from .capture import CaptureError


PROVIDER = "newrank_matrix"
CONTRACT_VERSION = "matrix-first-v1"
GATEWAY = "https://gw.newrank.cn/api/data_management_api"
WORKS_PATH = "/api/matrix/v3.2/aweme/list"
ACCOUNTS_PATH = "/api/matrix/v1/account/list"
DEFAULT_CONFIG_FILE = Path("/Users/mark/Documents/key/DcarKey/dcar.env.local")
SHANGHAI = ZoneInfo("Asia/Shanghai")
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
PLATFORM_CODES = {"douyin": 2, "xiaohongshu": 6}
CONTENT_METRIC_FIELDS = {
    "view_count": "playCount",
    "comment_count": "commentCount",
    "like_count": "diggCount",
    "share_count": "shareCount",
    "collect_count": "favoriteCount",
}
ACCOUNT_METRIC_FIELDS = {
    "follower_count": "cTotalFans",
    "platform_work_count": "awemeCount",
    "total_likes": "totalFavorited",
    "total_likes_and_collects": "totalFavorited",
    "work_view_daily_increment": "workPlayCountAdd",
    "work_like_daily_increment": "workFavoritedCountAdd",
    "work_comment_daily_increment": "workCommentCountAdd",
    "work_share_daily_increment": "workShareCountAdd",
    "collect_daily_increment": None,
}
_CONFIG_NAMES = {
    "NEWRANK_MATRIX_API_URL",
    "NEWRANK_MATRIX_N_TOKEN",
    "NEWRANK_MATRIX_KEY_ID",
    "NEWRANK_MATRIX_SECRET_KEY",
    "secretKey",
}
_SECRET_FIELDS = {
    "authorization", "cookie", "setcookie", "ntoken", "token", "apikey",
    "accesstoken", "secretkey", "keyid", "sign", "newrankmatrixntoken",
    "newrankmatrixsecretkey", "newrankmatrixkeyid", "newrankmatrixsign",
}


class MatrixConfigurationError(RuntimeError):
    """The approved gateway or credentials cannot be used safely."""


class MatrixRowError(ValueError):
    """One row is unidentifiable; the caller must retain its disposition."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def normalize_gateway(value: str) -> str:
    candidate = value.strip().rstrip("/")
    try:
        parsed = urllib.parse.urlsplit(candidate)
        port = parsed.port
    except ValueError:
        raise MatrixConfigurationError("Matrix API URL is invalid") from None
    if (
        parsed.scheme.lower() != "https"
        or (parsed.hostname or "").lower() != "gw.newrank.cn"
        or port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in (
            "/api/data_management_api", "/api/data_management_api" + WORKS_PATH
        )
        or parsed.query
        or parsed.fragment
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise MatrixConfigurationError(
            "Matrix API URL must use the approved Newrank gateway"
        )
    return GATEWAY


@dataclass(frozen=True)
class MatrixConfig:
    api_url: str
    n_token: str = field(repr=False)
    key_id: str = field(repr=False)
    secret_key: str = field(repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "api_url", normalize_gateway(self.api_url))
        for name in ("n_token", "key_id", "secret_key"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip() or any(
                ord(char) < 32 or ord(char) == 127 for char in value
            ):
                raise MatrixConfigurationError(
                    f"Matrix {name} is missing or invalid"
                )
            object.__setattr__(self, name, value.strip())


def _read_config(path: Path) -> dict[str, str]:
    if path.is_symlink():
        raise MatrixConfigurationError(
            "Matrix configuration must not be a symlink"
        )
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return {}
    except OSError:
        raise MatrixConfigurationError(
            "Matrix configuration cannot be opened"
        ) from None
    info = os.fstat(descriptor)
    if (
        not stat.S_ISREG(info.st_mode)
        or stat.S_IMODE(info.st_mode) not in (0o400, 0o600)
    ):
        os.close(descriptor)
        raise MatrixConfigurationError(
            "Matrix configuration must be a private 0400/0600 file"
        )
    with os.fdopen(descriptor, "r", encoding="utf-8-sig") as stream:
        try:
            lines = stream.read().splitlines()
        except UnicodeDecodeError:
            raise MatrixConfigurationError(
                "Matrix configuration must be UTF-8"
            ) from None
    values: dict[str, str] = {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").lstrip()
        if "=" not in line:
            continue
        name, raw_value = line.split("=", 1)
        name = name.strip()
        if name not in _CONFIG_NAMES:
            continue
        if name in values:
            raise MatrixConfigurationError(
                f"Matrix configuration repeats {name}"
            )
        value = raw_value.strip()
        if value[:1] in {"'", '"'}:
            match = re.fullmatch(r"(['\"])(.*?)\1(?:\s+#.*)?", value)
            if match is None:
                raise MatrixConfigurationError(
                    f"Matrix configuration has invalid quotes for {name}"
                )
            value = match.group(2)
        else:
            value = re.split(r"\s+#", value, maxsplit=1)[0].strip()
        values[name] = value
    return values


def load_config(
    path: Path | None = None, *, environ: Mapping[str, str] | None = None
) -> MatrixConfig:
    """Read just this module; never execute dotenv text or accept static sign."""
    env = os.environ if environ is None else environ
    configured_path = path or Path(
        env.get("NEWRANK_MATRIX_CONFIG_FILE", "").strip()
        or DEFAULT_CONFIG_FILE
    ).expanduser()
    file_values = _read_config(configured_path)
    values = {
        name: env.get(name, "").strip() or file_values.get(name, "")
        for name in _CONFIG_NAMES
    }
    secret = values["NEWRANK_MATRIX_SECRET_KEY"]
    legacy_secret = values["secretKey"]
    if secret and legacy_secret and secret != legacy_secret:
        raise MatrixConfigurationError("Matrix secretKey aliases conflict")
    url = values["NEWRANK_MATRIX_API_URL"]
    if not url:
        raise MatrixConfigurationError("NEWRANK_MATRIX_API_URL is not configured")
    return MatrixConfig(
        api_url=url,
        n_token=values["NEWRANK_MATRIX_N_TOKEN"],
        key_id=values["NEWRANK_MATRIX_KEY_ID"],
        secret_key=secret or legacy_secret,
    )


def sign_request(secret_key: str, path_name: str, req_json: str) -> str:
    if path_name not in (WORKS_PATH, ACCOUNTS_PATH):
        raise ValueError("unverified Matrix path")
    value = (
        secret_key + "pathName" + path_name + "reqJson" + req_json
        + "secretKey" + secret_key
    )
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def _utc(value: datetime | str) -> datetime:
    try:
        instant = (
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            if isinstance(value, str) else value
        )
    except ValueError:
        raise ValueError("a timezone-aware timestamp is required") from None
    if (
        not isinstance(instant, datetime)
        or instant.tzinfo is None
        or instant.utcoffset() is None
    ):
        raise ValueError("a timezone-aware timestamp is required")
    return instant.astimezone(timezone.utc)


def _utc_text(value: datetime | str) -> str:
    return _utc(value).isoformat(timespec="seconds").replace("+00:00", "Z")


def beijing_query_bounds(
    start_at: datetime | str, end_at: datetime | str
) -> tuple[str, str]:
    """Convert second-aligned UTC [start,end) to Matrix's inclusive seconds."""
    start, end = _utc(start_at), _utc(end_at)
    if start.microsecond or end.microsecond or end <= start:
        raise ValueError("Matrix windows must be increasing and second-aligned")
    return (
        start.astimezone(SHANGHAI).strftime("%Y-%m-%d %H:%M:%S"),
        (end - timedelta(seconds=1)).astimezone(SHANGHAI)
        .strftime("%Y-%m-%d %H:%M:%S"),
    )


def _date(value: date | str) -> str:
    if isinstance(value, datetime):
        raise ValueError("statistics date must be YYYY-MM-DD, not a timestamp")
    if isinstance(value, date):
        return value.isoformat()
    try:
        parsed = date.fromisoformat(value)
    except (ValueError, TypeError):
        raise ValueError("statistics date must be YYYY-MM-DD") from None
    if parsed.isoformat() != value:
        raise ValueError("statistics date must be YYYY-MM-DD")
    return value


def _json_loads(value: str) -> Any:
    def invalid_constant(constant: str) -> Any:
        raise ValueError("non-finite JSON constant")

    return json.loads(value, parse_constant=invalid_constant)


def _scrub(value: Any, secrets: Collection[str] = ()) -> Any:
    if isinstance(value, dict):
        return {
            key: "[REDACTED]"
            if re.sub(r"[^a-z0-9]", "", str(key).lower()) in _SECRET_FIELDS
            else _scrub(child, secrets)
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [_scrub(child, secrets) for child in value]
    if isinstance(value, str):
        # data is commonly JSON text. Keep that text unchanged unless an
        # embedded credential needs redaction.
        if value.lstrip().startswith(("{", "[")):
            try:
                decoded = _json_loads(value)
            except (ValueError, RecursionError):
                decoded = None
            if isinstance(decoded, (dict, list)):
                cleaned = _scrub(decoded, secrets)
                if cleaned != decoded:
                    value = json.dumps(
                        cleaned, ensure_ascii=False, separators=(",", ":")
                    )
        for secret in sorted(
            (item for item in secrets if item), key=len, reverse=True
        ):
            value = value.replace(secret, "[REDACTED]")
    return value


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self, req: Any, fp: Any, code: Any, msg: Any, headers: Any, newurl: Any
    ) -> None:
        return None


Transport = Callable[[urllib.request.Request, float], tuple[int, bytes]]


def _transport(
    request: urllib.request.Request, timeout: float
) -> tuple[int, bytes]:
    opener = urllib.request.build_opener(_NoRedirect())
    try:
        try:
            response = opener.open(request, timeout=timeout)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            if response.geturl() != request.full_url:
                raise CaptureError(
                    "Matrix redirect rejected",
                    retryable=False,
                    error_code="matrix_redirect_rejected",
                )
            try:
                body = response.read(MAX_RESPONSE_BYTES + 1)
            except http.client.IncompleteRead as error:
                # The caller still requires a complete valid JSON document.
                body = error.partial
            return int(response.status), body
    except CaptureError:
        raise
    except (OSError, urllib.error.URLError, http.client.HTTPException):
        raise CaptureError(
            "Matrix transport failed",
            retryable=True,
            error_code="matrix_transport_failed",
        ) from None


def _cursor(value: Any) -> list[Any]:
    if not isinstance(value, list) or not value:
        raise ValueError("Matrix scrollId must be a nonempty JSON array")
    try:
        json.dumps(value, ensure_ascii=False, allow_nan=False)
    except (ValueError, TypeError, RecursionError):
        raise ValueError("Matrix scrollId must be a finite JSON array") from None
    return copy.deepcopy(value)


@dataclass(frozen=True)
class MatrixPage:
    raw_response: dict[str, Any]
    rows: list[Any]
    next_cursor: list[Any] | None
    terminal: bool
    pagination_error: str | None
    query: dict[str, Any]
    captured_at: str
    http_status: int
    path_name: str


class NewrankMatrixClient:
    def __init__(
        self,
        config: MatrixConfig | None = None,
        *,
        transport: Transport | None = None,
        timeout: float = 45,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not math.isfinite(timeout) or not 0 < timeout <= 120:
            raise ValueError("Matrix timeout must be finite and within 120 seconds")
        self._config = config
        self._transport = transport or _transport
        self._timeout = timeout
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    @staticmethod
    def _query(
        platform: str, page_size: int, scroll_id: list[Any] | None
    ) -> dict[str, Any]:
        if platform not in PLATFORM_CODES:
            raise ValueError("Matrix platform must be douyin or xiaohongshu")
        if type(page_size) is not int or not 1 <= page_size <= 100:
            raise ValueError("Matrix page_size must be an integer within 1..100")
        query: dict[str, Any] = {
            "pageSize": page_size, "platType": PLATFORM_CODES[platform]
        }
        if scroll_id is not None:
            query["scrollId"] = _cursor(scroll_id)
        return query

    def fetch_works_page(
        self,
        platform: str,
        start_at: datetime | str,
        end_at: datetime | str,
        *,
        scroll_id: list[Any] | None = None,
        page_size: int = 100,
    ) -> MatrixPage:
        query = self._query(platform, page_size, scroll_id)
        query["startDate"], query["endDate"] = beijing_query_bounds(
            start_at, end_at
        )
        return self._fetch(WORKS_PATH, query)

    def fetch_accounts_page(
        self,
        platform: str,
        rank_date: date | str | None = None,
        *,
        scroll_id: list[Any] | None = None,
        page_size: int = 100,
    ) -> MatrixPage:
        query = self._query(platform, page_size, scroll_id)
        query["rankData"] = (
            _date(rank_date) if rank_date is not None
            else (
                _utc(self._clock()).astimezone(SHANGHAI).date()
                - timedelta(days=1)
            ).isoformat()
        )
        return self._fetch(ACCOUNTS_PATH, query)

    def _fetch(self, path_name: str, query: dict[str, Any]) -> MatrixPage:
        config = self._config or load_config()
        req_json = json.dumps(
            query, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        )
        signature = sign_request(config.secret_key, path_name, req_json)
        body = {
            "keyId": config.key_id, "sign": signature,
            "pathName": path_name, "reqJson": req_json,
        }
        request = urllib.request.Request(
            config.api_url + path_name,
            method="POST",
            data=json.dumps(
                body, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8"),
            headers={
                "N-Token": config.n_token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        captured_at = _utc_text(self._clock())
        try:
            status, wire = self._transport(request, self._timeout)
        except CaptureError:
            raise
        except (OSError, urllib.error.URLError, http.client.HTTPException):
            raise CaptureError(
                "Matrix transport failed",
                retryable=True,
                error_code="matrix_transport_failed",
            ) from None
        secrets = (config.n_token, config.key_id, config.secret_key, signature)
        if not isinstance(wire, bytes) or len(wire) > MAX_RESPONSE_BYTES:
            raise CaptureError(
                "Matrix response exceeded the bounded limit",
                retryable=True,
                error_code="matrix_response_size_limit",
                http_status=status,
            )
        try:
            payload = _json_loads(wire.decode("utf-8-sig"))
        except (UnicodeDecodeError, ValueError, RecursionError):
            payload = {"unparsed_body": wire.decode("utf-8", "replace")}
        safe = _scrub(payload, secrets)
        if not 200 <= status < 300:
            raise CaptureError(
                f"Matrix HTTP {status}",
                retryable=status in (408, 429) or status >= 500,
                error_code=(
                    "matrix_redirect_rejected" if 300 <= status < 400
                    else f"matrix_http_{status}"
                ),
                http_status=status,
                raw_response=safe,
            )
        if not isinstance(payload, dict) or "code" not in payload:
            raise CaptureError(
                "Matrix response omitted a valid business envelope",
                retryable=True,
                error_code="matrix_invalid_envelope",
                http_status=status,
                raw_response=safe,
            )
        code = payload["code"]
        if not (
            type(code) is int and code == 0
            or type(code) is str and code == "0"
        ):
            raise CaptureError(
                "Matrix business request failed",
                retryable=code in (500, 5000, "500", "5000"),
                error_code="matrix_business_error",
                http_status=status,
                raw_response=safe,
            )
        data = safe.get("data")
        if isinstance(data, str):
            try:
                data = _json_loads(data)
            except (ValueError, RecursionError):
                data = None
        if not isinstance(data, list):
            raise CaptureError(
                "Matrix data is not a record array",
                retryable=True,
                error_code="matrix_invalid_data",
                http_status=status,
                raw_response=safe,
            )
        cursor = None
        pagination_error = None
        if data:
            try:
                cursor = _cursor(
                    data[-1].get("scrollId") if isinstance(data[-1], dict)
                    else None
                )
            except ValueError:
                pagination_error = "missing_or_invalid_last_scroll_id"
        return MatrixPage(
            raw_response=safe,
            rows=data,
            next_cursor=cursor,
            terminal=not data,
            pagination_error=pagination_error,
            query=copy.deepcopy(query),
            captured_at=captured_at,
            http_status=status,
            path_name=path_name,
        )


def _field(
    status: str, source_field: str | None, reason: str | None = None
) -> dict[str, Any]:
    return {"status": status, "source_field": source_field, "reason": reason}


def _value(
    row: Mapping[str, Any], source: str
) -> tuple[Any, dict[str, Any]]:
    if source not in row or row[source] is None or row[source] == "":
        return None, _field(
            "missing", source, "absent" if source not in row else "null_or_empty"
        )
    return row[source], _field("provided", source)


def _identifier(value: Any) -> str | None:
    if type(value) is int:
        return str(value) if value > 0 else None
    if isinstance(value, str):
        text = value.strip()
        if text and text != "0" and re.fullmatch(r"[A-Za-z0-9_-]+", text):
            return text
    return None


def _identity_field(
    row: Mapping[str, Any], key: str
) -> tuple[str | None, dict[str, Any]]:
    value, status = _value(row, key)
    if status["status"] == "missing":
        return None, status
    parsed = _identifier(value)
    return parsed, (
        status if parsed is not None
        else _field("invalid", key, "invalid_identifier")
    )


def _text_field(
    row: Mapping[str, Any], key: str
) -> tuple[str | None, dict[str, Any]]:
    value, status = _value(row, key)
    if status["status"] == "missing":
        return None, status
    if not isinstance(value, str) or not value.strip():
        return None, _field("invalid", key, "not_text")
    return value, status


def _metric_fields(
    row: Mapping[str, Any],
    fields: Mapping[str, str | None],
    *,
    requested_fields: Collection[str] | None,
    not_applicable: Collection[str] = (),
    signed_fields: Collection[str] = (),
) -> tuple[dict[str, int | None], dict[str, dict[str, Any]]]:
    requested = set(fields) if requested_fields is None else set(requested_fields)
    if requested - fields.keys():
        raise ValueError("unverified Matrix requested field")
    metrics: dict[str, int | None] = {}
    statuses: dict[str, dict[str, Any]] = {}
    for local, source in fields.items():
        metrics[local] = None
        if local in not_applicable:
            statuses[local] = _field(
                "not_applicable", source, "platform_contract"
            )
            continue
        if local not in requested or source is None:
            statuses[local] = _field(
                "not_requested", source,
                "unverified_contract" if source is None
                else "outside_requested_fields",
            )
            continue
        value, status = _value(row, source)
        statuses[local] = status
        if status["status"] != "provided":
            continue
        if isinstance(value, str) and re.fullmatch(
            r"-?[0-9]+" if local in signed_fields else r"[0-9]+",
            value.strip(),
        ):
            try:
                value = int(value)
            except ValueError:
                value = None
        lower_bound = -(2**63) if local in signed_fields else 0
        if type(value) is not int or not lower_bound <= value <= 2**63 - 1:
            statuses[local] = _field(
                "invalid", source, "invalid_integer_count"
            )
            continue
        metrics[local] = value
    return metrics, statuses


def _share_url_field(
    row: Mapping[str, Any], key: str
) -> tuple[str | None, dict[str, Any]]:
    value, status = _text_field(row, key)
    if value is None:
        return None, status
    try:
        parsed = urllib.parse.urlsplit(value)
        valid = (
            parsed.scheme in {"http", "https"} and bool(parsed.hostname)
            and parsed.username is None and parsed.password is None
            and not any(ord(char) < 33 or ord(char) == 127 for char in value)
        )
        parsed.port
    except ValueError:
        valid = False
    if not valid:
        return None, _field("invalid", key, "invalid_share_url")
    return value, status


def _row_platform(row: Any, platform: str) -> Mapping[str, Any]:
    if platform not in PLATFORM_CODES:
        raise ValueError("Matrix platform must be douyin or xiaohongshu")
    if not isinstance(row, Mapping):
        raise MatrixRowError("row_not_object")
    code = row.get("platType")
    if not (type(code) is int or isinstance(code, str)) or (
        str(code) != str(PLATFORM_CODES[platform])
    ):
        raise MatrixRowError("missing_or_mismatched_platform")
    return row


def normalize_work(
    row: Any, *, platform: str, requested_fields: Collection[str] | None = None
) -> dict[str, Any]:
    row = _row_platform(row, platform)
    work_id, work_id_status = _identity_field(row, "awemeId")
    if work_id is None:
        raise MatrixRowError("missing_or_invalid_work_id")
    metrics, statuses = _metric_fields(
        row, CONTENT_METRIC_FIELDS, requested_fields=requested_fields,
        not_applicable=("view_count",) if platform == "xiaohongshu" else (),
    )
    account_uid, statuses["account_uid"] = _identity_field(row, "uid")
    title, statuses["title"] = _text_field(row, "title")
    account_name, statuses["account_name"] = _text_field(row, "nickname")
    share_key = "dyShareUrl" if platform == "douyin" else "xhsShareUrl"
    share_url, statuses["original_share_url"] = _share_url_field(row, share_key)
    published_at = None
    published_value, statuses["published_at"] = _value(row, "createTime")
    if published_value is not None:
        try:
            if not isinstance(published_value, str) or not re.match(
                r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}", published_value
            ):
                raise ValueError("invalid time")
            instant = datetime.fromisoformat(
                published_value.replace("Z", "+00:00")
            )
            if instant.tzinfo is None:
                instant = instant.replace(tzinfo=SHANGHAI)
            published_at = _utc_text(instant)
        except ValueError:
            statuses["published_at"] = _field(
                "invalid", "createTime", "invalid_published_at"
            )
    statuses["platform_content_id"] = work_id_status
    canonical_url = (
        f"https://www.douyin.com/video/{work_id}" if platform == "douyin"
        else f"https://www.xiaohongshu.com/explore/{work_id}"
    )
    return {
        "provider": PROVIDER,
        "platform": platform,
        "platform_content_id": work_id,
        "canonical_url": canonical_url,
        "original_share_url": share_url,
        "account_uid": account_uid,
        "account_name": account_name,
        "title": title,
        "published_at": published_at,
        "content_type": "unknown",
        "body": None,
        "metrics": metrics,
        "field_status": statuses,
        "metadata": {
            "contract_version": CONTRACT_VERSION,
            "raw_only": {
                name: _scrub(row[name]) for name in (
                    "cover", "topicName", "dyAddressName", "dyCreationType",
                    "mType", "duration", "anaInteraction",
                    "firstDayFansCountIncrease", "totalAddFansCount",
                ) if name in row
            },
        },
    }


def normalize_account(
    row: Any,
    *,
    platform: str,
    rank_date: date | str,
    requested_fields: Collection[str] | None = None,
) -> dict[str, Any]:
    row = _row_platform(row, platform)
    requested_date = _date(rank_date)
    uid, uid_status = _identity_field(row, "uid")
    if uid is None:
        raise MatrixRowError("missing_or_invalid_account_uid")
    not_applicable = (
        ("total_likes_and_collects",) if platform == "douyin"
        else ("total_likes", "work_view_daily_increment")
    )
    metrics, statuses = _metric_fields(
        row, ACCOUNT_METRIC_FIELDS,
        requested_fields=requested_fields, not_applicable=not_applicable,
        signed_fields=tuple(
            name for name in ACCOUNT_METRIC_FIELDS
            if name.endswith("_daily_increment")
        ),
    )
    statistics_date = None
    raw_date, date_status = _value(row, "rankDate")
    if raw_date is not None:
        try:
            statistics_date = _date(raw_date)
            if statistics_date != requested_date:
                date_status = _field(
                    "invalid", "rankDate", "statistics_date_mismatch"
                )
        except ValueError:
            date_status = _field(
                "invalid", "rankDate", "invalid_statistics_date"
            )
    if date_status["status"] != "provided":
        for name, status in statuses.items():
            if status["status"] not in {"not_requested", "not_applicable"}:
                metrics[name] = None
                statuses[name] = _field(
                    "invalid", status["source_field"],
                    "unverified_statistics_date",
                )
    statuses["statistics_date"] = date_status
    statuses["uid"] = uid_status
    result: dict[str, Any] = {
        "identity": {"platform": platform, "uid": uid},
        "provider": PROVIDER,
        "platform": platform,
        "uid": uid,
        "statistics_date": statistics_date,
        "requested_statistics_date": requested_date,
        "basis": "matrix_daily",
        "metrics": metrics,
        "field_status": statuses,
        "metadata": {
            "contract_version": CONTRACT_VERSION,
            "raw_only": {
                name: _scrub(row[name]) for name in (
                    "dyAllFans", "dayIndexRank", "weekIndexRank", "monthIndexRank",
                ) if name in row
            },
        },
    }
    for local, source in {
        "display_account_id": "uniqueId",
        "nickname": "cNickname",
        "avatar_url": "cAvatar",
        "description": "cDescible",
        "enterprise_verify_reason": "enterpriseVerifyReason",
    }.items():
        result[local], statuses[local] = _text_field(row, source)
    result["verify_type"], statuses["verify_type"] = _value(row, "verifyType")
    return result
