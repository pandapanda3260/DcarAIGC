"""SMS delivery for verification codes.

``TencentSmsSender`` calls Tencent Cloud SMS ``SendSms`` (API 3.0, version
``2021-01-11``) directly with the official ``TC3-HMAC-SHA256`` request
signature, assembled exactly the way ``tencentcloud-sdk-python`` does it
(``SignedHeaders=content-type;host``, JSON body); phone numbers and codes
travel in the JSON body so they never appear in a URL.  ``LogSmsSender`` is
the local development channel.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Optional

import httpx

from dcar_auth.store import CODE_TTL_SECONDS


LOGGER = logging.getLogger("dcar-auth")
TENCENT_HOST = "sms.tencentcloudapi.com"
TENCENT_SERVICE = "sms"
TENCENT_ACTION = "SendSms"
TENCENT_VERSION = "2021-01-11"
TENCENT_ALGORITHM = "TC3-HMAC-SHA256"
TENCENT_SIGNED_HEADERS = "content-type;host"
JSON_CONTENT_TYPE = "application/json"
DEFAULT_REGION = "ap-guangzhou"
CODE_VALIDITY_MINUTES = str(CODE_TTL_SECONDS // 60)
# The credential file uses the variable names of Mark's dcar.env.local so the
# Tencent section can be copied over verbatim; unrelated keys are ignored.
CREDENTIAL_KEYS = (
    "TENCENT_SMS_SECRET_ID",
    "TENCENT_SMS_SECRET_KEY",
    "TENCENT_SMS_SDK_APP_ID",
    "TENCENT_SMS_SIGN_NAME",
    "TENCENT_SMS_TEMPLATE_ID",
)
OPTIONAL_CREDENTIAL_KEYS = ("TENCENT_SMS_REGION", "TENCENT_SMS_CODE_TTL_MINUTES")
# Per-number SendStatus codes that mean "too many messages for this number":
# the user is asked to wait; anything else is a generic delivery failure.
RATE_LIMIT_CODES = frozenset(
    {
        "LimitExceeded.PhoneNumberThirtySecondLimit",
        "LimitExceeded.PhoneNumberOneHourLimit",
        "LimitExceeded.PhoneNumberDailyLimit",
        "LimitExceeded.PhoneNumberSameContentDailyLimit",
        "LimitExceeded.DeliveryFrequencyLimit",
    }
)
BAD_NUMBER_CODES = frozenset({"InvalidParameterValue.IncorrectPhoneNumber"})
UNKNOWN_RESULT_CODES = frozenset(
    {"InternalError.Timeout", "InternalError.SendAndRecvFail"}
)


@dataclass(frozen=True)
class SmsCredentials:
    secret_id: str
    secret_key: str
    sdk_app_id: str
    sign_name: str
    template_id: str
    region: str = DEFAULT_REGION
    # Second template variable ("valid for N minutes"); None when the approved
    # template only has the code variable.
    code_ttl_minutes: Optional[str] = None


@dataclass(frozen=True)
class SmsOutcome:
    """``status`` is the challenge status to record: sent / rejected / unknown."""

    status: str
    provider_code: str
    request_id: str = ""
    serial_no: str = ""


def _provider_reference(value: object) -> str:
    """Keep only the documented opaque reference alphabet; never echo messages."""
    if not isinstance(value, str) or len(value) > 128:
        return ""
    return value if all(c.isascii() and (c.isalnum() or c in "-_:.") for c in value) else ""


def _failure_status(code: str) -> str:
    # Internal dispatch failures cannot prove that no message was sent.
    return "unknown" if not code or code.startswith("InternalError") or code in UNKNOWN_RESULT_CODES else "rejected"


def _parse_env_file(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def load_sms_credentials(path: Path) -> SmsCredentials:
    values = _parse_env_file(Path(path).read_text(encoding="utf-8"))
    missing = [key for key in CREDENTIAL_KEYS if not values.get(key)]
    if missing:
        raise RuntimeError(
            "SMS credential file is missing: " + ", ".join(missing)
        )
    for key in CREDENTIAL_KEYS + OPTIONAL_CREDENTIAL_KEYS:
        if any(ord(character) < 32 for character in values.get(key, "")):
            raise RuntimeError(f"SMS credential {key} has an invalid format")
    for key in ("TENCENT_SMS_SDK_APP_ID", "TENCENT_SMS_TEMPLATE_ID"):
        if not values[key].isdigit():
            raise RuntimeError(f"SMS credential {key} must be numeric")
    region = values.get("TENCENT_SMS_REGION", "") or DEFAULT_REGION
    if not region.isascii() or " " in region:
        raise RuntimeError("SMS credential TENCENT_SMS_REGION has an invalid format")
    ttl = values.get("TENCENT_SMS_CODE_TTL_MINUTES", "") or None
    if ttl is not None and ttl != CODE_VALIDITY_MINUTES:
        # The template text promises a validity period; it must match the
        # gateway's real code lifetime instead of lying to the user.
        raise RuntimeError(
            "SMS credential TENCENT_SMS_CODE_TTL_MINUTES must be "
            f"{CODE_VALIDITY_MINUTES} (the verification-code validity)"
        )
    return SmsCredentials(
        secret_id=values["TENCENT_SMS_SECRET_ID"],
        secret_key=values["TENCENT_SMS_SECRET_KEY"],
        sdk_app_id=values["TENCENT_SMS_SDK_APP_ID"],
        sign_name=values["TENCENT_SMS_SIGN_NAME"],
        template_id=values["TENCENT_SMS_TEMPLATE_ID"],
        region=region,
        code_ttl_minutes=ttl,
    )


def _hmac_sha256(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def sign_tc3_request(
    credentials: SmsCredentials,
    *,
    service: str,
    host: str,
    action: str,
    version: str,
    region: str,
    body: str,
    timestamp: int,
) -> dict[str, str]:
    """Return the request headers (including Authorization) for a JSON POST.

    Mirrors ``tencentcloud.common.abstract_client``: only ``content-type`` and
    ``host`` are signed, the payload hash covers the exact body bytes, and the
    credential scope date is the UTC date of ``timestamp``.
    """
    date = datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d")
    payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
    canonical_headers = f"content-type:{JSON_CONTENT_TYPE}\nhost:{host}\n"
    canonical_request = "\n".join(
        ["POST", "/", "", canonical_headers, TENCENT_SIGNED_HEADERS, payload_hash]
    )
    credential_scope = f"{date}/{service}/tc3_request"
    string_to_sign = "\n".join(
        [
            TENCENT_ALGORITHM,
            str(timestamp),
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    secret_date = _hmac_sha256(("TC3" + credentials.secret_key).encode("utf-8"), date)
    secret_service = _hmac_sha256(secret_date, service)
    secret_signing = _hmac_sha256(secret_service, "tc3_request")
    signature = hmac.new(
        secret_signing, string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return {
        "Content-Type": JSON_CONTENT_TYPE,
        "Host": host,
        "X-TC-Action": action,
        "X-TC-Version": version,
        "X-TC-Timestamp": str(timestamp),
        "X-TC-Region": region,
        "Authorization": (
            f"{TENCENT_ALGORITHM} Credential={credentials.secret_id}/{credential_scope}, "
            f"SignedHeaders={TENCENT_SIGNED_HEADERS}, Signature={signature}"
        ),
    }


def send_sms_payload(credentials: SmsCredentials, phone: str, code: str) -> str:
    """JSON body for one mainland-China code message (E.164 ``+86`` number)."""
    template_params = [code]
    if credentials.code_ttl_minutes is not None:
        template_params.append(credentials.code_ttl_minutes)
    params: Mapping[str, object] = {
        "PhoneNumberSet": [f"+86{phone}"],
        "SmsSdkAppId": credentials.sdk_app_id,
        "SignName": credentials.sign_name,
        "TemplateId": credentials.template_id,
        "TemplateParamSet": template_params,
    }
    # Same serialisation as the official SDK (json.dumps defaults).
    return json.dumps(params)


class TencentSmsSender:
    def __init__(
        self,
        credentials: SmsCredentials,
        *,
        transport: Optional[httpx.AsyncBaseTransport] = None,
        host: str = TENCENT_HOST,
    ) -> None:
        self.credentials = credentials
        self.host = host
        self.client = httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(10, connect=5),
            follow_redirects=False,
            trust_env=False,
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    def build_request(
        self, phone: str, code: str, *, timestamp: Optional[int] = None
    ) -> httpx.Request:
        body = send_sms_payload(self.credentials, phone, code)
        headers = sign_tc3_request(
            self.credentials,
            service=TENCENT_SERVICE,
            host=self.host,
            action=TENCENT_ACTION,
            version=TENCENT_VERSION,
            region=self.credentials.region,
            body=body,
            timestamp=int(time.time()) if timestamp is None else timestamp,
        )
        return self.client.build_request(
            "POST",
            f"https://{self.host}/",
            headers=headers,
            content=body.encode("utf-8"),
        )

    async def send(self, phone: str, code: str) -> SmsOutcome:
        """Deliver one code.  Never retries: SendSms is not idempotent."""
        request = self.build_request(phone, code)
        try:
            response = await self.client.send(request)
        except httpx.TimeoutException:
            return SmsOutcome("unknown", "timeout")
        except httpx.TransportError:
            return SmsOutcome("unknown", "transport_error")
        try:
            payload = response.json()
        except ValueError:
            return SmsOutcome("unknown", "no_json")
        body = payload.get("Response") if isinstance(payload, dict) else None
        if not isinstance(body, dict):
            return SmsOutcome("unknown", "no_json")
        request_id = _provider_reference(body.get("RequestId"))
        error = body.get("Error")
        if isinstance(error, dict):
            code = _provider_reference(error.get("Code"))[:64]
            return SmsOutcome(_failure_status(code), code or "error", request_id)
        statuses = body.get("SendStatusSet")
        if not isinstance(statuses, list) or not statuses or not isinstance(statuses[0], dict):
            return SmsOutcome("unknown", "no_status", request_id)
        code_value = _provider_reference(statuses[0].get("Code"))[:64]
        serial_no = _provider_reference(statuses[0].get("SerialNo"))
        if response.status_code >= 500:
            return SmsOutcome("unknown", code_value or "http_error", request_id, serial_no)
        if code_value == "Ok":
            return SmsOutcome("sent", "Ok", request_id, serial_no)
        return SmsOutcome(_failure_status(code_value), code_value or "no_code", request_id, serial_no)


class LogSmsSender:
    """Development channel: the code is written to the gateway log."""

    async def aclose(self) -> None:
        return None

    async def send(self, phone: str, code: str) -> SmsOutcome:
        LOGGER.warning(
            "sms[log] phone=%s code=%s", mask_phone(phone), code
        )
        return SmsOutcome("sent", "LOG")


def mask_phone(phone: str) -> str:
    if len(phone) < 7:
        return "*" * len(phone)
    return f"{phone[:3]}****{phone[-4:]}"
