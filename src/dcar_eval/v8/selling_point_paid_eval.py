"""Budgeted paid bake-off runner for selling-points v5.3 stage A."""

from __future__ import annotations

import http.client
import json
import math
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import quantiles
from typing import Any, Callable, Iterable, Mapping, Sequence

from .llm_assist import llm_config
from .selling_point_label_cards import load_label_cards
from .selling_point_offline import (
    CharNgramTfidfIndex,
    SellingPointOfflineError,
    build_prompt,
    canonical_json,
    parse_model_json,
    second_call_reason,
    sha256_file,
    sha256_json,
    validate_model_response,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MODEL_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "selling_point_stage_a_models_v1.json"
)
RUN_VERSION = "selling-point-stage-a-run-v1"
PRICE_MANIFEST_VERSION = "selling-point-stage-a-price-manifest-v1"
RESULT_VERSION = "selling-point-stage-a-group-result-v1"
SUMMARY_VERSION = "selling-point-stage-a-summary-v1"
MAX_OUTPUT_TOKENS = 800
INPUT_TOKEN_RESERVE_PER_CHAR = 2.0
SECOND_CALL_EXTRA_CHARS = 5000
CALL_TIMEOUT_SECONDS = 45
MAX_TRANSPORT_ATTEMPTS = 2
RATE_LIMIT_RETRY_SECONDS = 20
G1_DENOMINATOR = 228
G1_PASS_COUNT = 160
G1_SCENE_DENOMINATORS = {"X": 112, "E": 93, "M": 23}
G1_SCENE_PASS_COUNTS = {"X": 84, "E": 66, "M": 14}


class SellingPointPaidEvalError(RuntimeError):
    """Raised when a paid run cannot satisfy identity, budget, or API contracts."""


class SellingPointCallError(SellingPointPaidEvalError):
    """Raised for a terminal API call failure."""

    def __init__(
        self,
        message: str,
        *,
        attempts: int,
        reserved_input_tokens: int,
        reserved_output_tokens: int,
    ) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.reserved_input_tokens = reserved_input_tokens
        self.reserved_output_tokens = reserved_output_tokens


class GroupEvaluationError(SellingPointPaidEvalError):
    """Preserve paid calls when a later transport attempt terminates a group."""

    def __init__(
        self,
        message: str,
        *,
        completed_calls: Sequence[Mapping[str, Any]],
        failure: SellingPointCallError,
    ) -> None:
        super().__init__(message)
        self.completed_calls = [dict(call) for call in completed_calls]
        self.failure = failure


@dataclass(frozen=True)
class ModelPrice:
    slot: str
    requested_model: str
    model: str
    input_rate: float
    output_rate: float
    supports_json_object: bool
    price_source_url: str
    replacement_reason: str | None

    def cost(self, input_tokens: int, output_tokens: int) -> float:
        return (
            input_tokens * self.input_rate + output_tokens * self.output_rate
        ) / 1_000_000


@dataclass(frozen=True)
class ApiCallResult:
    raw: str
    input_tokens: int
    output_tokens: int
    latency_ms: int
    unmetered_transport_attempts: int = 0
    reserved_input_tokens: int = 0
    reserved_output_tokens: int = 0


ModelCaller = Callable[[ModelPrice, str, str], ApiCallResult]


def budgeted_caller(caller: ModelCaller, *, remaining_cny: float) -> ModelCaller:
    """Reserve all attempts using the actual prompt before a logical call.

    Do not recycle reservations during a batch: a timeout may have been billed
    and a concurrent repair call must not spend another worker's retry budget.
    Resume uses the persisted cumulative receipts, not these temporary reserves.
    """
    lock = threading.Lock()
    reserved = 0.0

    def call(model: ModelPrice, system: str, user: str) -> ApiCallResult:
        nonlocal reserved
        required = MAX_TRANSPORT_ATTEMPTS * model.cost(
            math.ceil((len(system) + len(user)) * INPUT_TOKEN_RESERVE_PER_CHAR),
            MAX_OUTPUT_TOKENS,
        )
        with lock:
            if reserved + required > remaining_cny:
                raise SellingPointCallError(
                    "shared budget exhausted before dispatch (including retries)",
                    attempts=0, reserved_input_tokens=0, reserved_output_tokens=0,
                )
            reserved += required
        return caller(model, system, user)

    return call


def paced_caller(caller: ModelCaller, *, minimum_interval_seconds: float) -> ModelCaller:
    """Serialize request starts so account RPM/TPM windows are not flooded."""

    if minimum_interval_seconds < 0:
        raise SellingPointPaidEvalError("request interval must be non-negative")
    if minimum_interval_seconds == 0:
        return caller
    lock = threading.Lock()
    next_start = 0.0

    def call(model: ModelPrice, system: str, user: str) -> ApiCallResult:
        nonlocal next_start
        with lock:
            now = time.monotonic()
            if next_start > now:
                time.sleep(next_start - now)
            next_start = time.monotonic() + minimum_interval_seconds
        return caller(model, system, user)

    return call


def model_concurrency_limit(model: ModelPrice, overall_concurrency: int) -> int:
    """Keep DeepSeek transport serial without throttling the Doubao canaries."""
    if model.model.startswith("deepseek-"):
        return 1
    return overall_concurrency


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def load_model_config(path: Path = DEFAULT_MODEL_CONFIG_PATH) -> dict[str, Any]:
    try:
        payload = path.resolve().read_bytes()
        value = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SellingPointPaidEvalError(f"cannot load model config: {error}") from error
    if not isinstance(value, dict) or value.get("version") not in {
        "selling-point-stage-a-models-v1",
        "selling-point-stage-a-models-v2",
    }:
        raise SellingPointPaidEvalError("unsupported stage A model config")
    if value.get("currency") != "CNY" or value.get("unit") != "per_million_tokens":
        raise SellingPointPaidEvalError("model prices must be CNY per million tokens")
    config_version = str(value["version"])
    expected_budget = 500.0 if config_version.endswith("v1") else 200.0
    if float(value.get("budget_limit_cny") or 0) != expected_budget:
        raise SellingPointPaidEvalError(
            f"{config_version} budget limit must be exactly {expected_budget:g} CNY"
        )
    raw_models = value.get("models")
    expected_models = 3 if config_version.endswith("v1") else 1
    if not isinstance(raw_models, list) or len(raw_models) != expected_models:
        raise SellingPointPaidEvalError(
            f"{config_version} requires exactly {expected_models} model slot(s)"
        )
    models: list[ModelPrice] = []
    for raw in raw_models:
        if not isinstance(raw, Mapping):
            raise SellingPointPaidEvalError("model slots must be objects")
        input_rate = float(raw.get("input_cny_per_million_tokens") or 0)
        output_rate = float(raw.get("output_cny_per_million_tokens") or 0)
        source_url = str(raw.get("price_source_url") or "")
        if input_rate <= 0 or output_rate <= 0 or not source_url.startswith("https://"):
            raise SellingPointPaidEvalError("model slot has no valid public price")
        models.append(
            ModelPrice(
                slot=str(raw.get("slot") or ""),
                requested_model=str(raw.get("requested_model") or ""),
                model=str(raw.get("model") or ""),
                input_rate=input_rate,
                output_rate=output_rate,
                supports_json_object=bool(raw.get("supports_json_object")),
                price_source_url=source_url,
                replacement_reason=(
                    str(raw["replacement_reason"])
                    if raw.get("replacement_reason") is not None
                    else None
                ),
            )
        )
    if any(not model.slot or not model.model for model in models):
        raise SellingPointPaidEvalError("model slot/model ids must not be empty")
    if len({model.slot for model in models}) != expected_models or len(
        {model.model for model in models}
    ) != expected_models:
        raise SellingPointPaidEvalError("model slots and effective ids must be unique")
    if config_version.endswith("v2") and models[0].model != (
        "doubao-seed-2-1-pro-260628"
    ):
        raise SellingPointPaidEvalError("stage A v2 must freeze Doubao Pro only")
    value["models_loaded"] = models
    value["config_sha256"] = sha256_file(path.resolve())
    return value


def select_model_prices(
    models: Sequence[ModelPrice], selected_model_ids: Sequence[str] | None
) -> list[ModelPrice]:
    if selected_model_ids is None:
        return list(models)
    selected = list(selected_model_ids)
    if not selected or len(selected) != len(set(selected)):
        raise SellingPointPaidEvalError("selected model ids must be non-empty and unique")
    configured = {model.model: model for model in models}
    unknown = sorted(set(selected) - set(configured))
    if unknown:
        raise SellingPointPaidEvalError(f"selected models are not configured: {unknown}")
    return [configured[model_id] for model_id in selected]


def _model_catalog(api: Mapping[str, str]) -> dict[str, dict[str, Any]]:
    request = urllib.request.Request(
        str(api["api_base"]).rstrip("/") + "/models",
        headers={"Authorization": f"Bearer {api['api_key']}"},
        method="GET",
    )
    value: Any = None
    last_error: Exception | None = None
    for attempt in range(2):
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                value = json.loads(response.read().decode("utf-8"))
            break
        except (
            OSError,
            urllib.error.URLError,
            http.client.IncompleteRead,
            json.JSONDecodeError,
        ) as error:
            last_error = error
            if attempt == 0:
                time.sleep(2)
                continue
    if value is None:
        raise SellingPointPaidEvalError(
            f"cannot query Ark model catalog: {last_error}"
        ) from last_error
    raw_models = value.get("data") if isinstance(value, Mapping) else None
    if not isinstance(raw_models, list):
        raise SellingPointPaidEvalError("Ark model catalog has no data list")
    return {
        str(item.get("id")): dict(item)
        for item in raw_models
        if isinstance(item, Mapping) and item.get("id")
    }


def verify_model_availability(
    models: Sequence[ModelPrice], api: Mapping[str, str]
) -> dict[str, Any]:
    catalog = _model_catalog(api)
    snapshot: list[dict[str, Any]] = []
    for model in models:
        current = catalog.get(model.model)
        if current is None or current.get("status") == "Shutdown":
            raise SellingPointPaidEvalError(
                f"effective model is unavailable: {model.model}"
            )
        requested = catalog.get(model.requested_model)
        if model.requested_model != model.model:
            if requested is None or requested.get("status") != "Shutdown":
                raise SellingPointPaidEvalError(
                    f"replacement source is not confirmed Shutdown: {model.requested_model}"
                )
            if not model.replacement_reason:
                raise SellingPointPaidEvalError(
                    f"replacement reason is missing for {model.slot}"
                )
        snapshot.append(
            {
                "slot": model.slot,
                "requested": {
                    "id": model.requested_model,
                    "status": requested.get("status") if requested else None,
                    "name": requested.get("name") if requested else None,
                    "version": requested.get("version") if requested else None,
                },
                "effective": {
                    "id": model.model,
                    "status": current.get("status"),
                    "name": current.get("name"),
                    "version": current.get("version"),
                    "features": current.get("features"),
                    "token_limits": current.get("token_limits"),
                },
            }
        )
    return {"queried_at": _now_utc(), "models": snapshot}


def manifest_groups(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[int, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (int(row["content_id"]), str(row["evidence_sha256"]))
        groups[key].append(row)
    output: list[dict[str, Any]] = []
    for (content_id, evidence_sha256), members in sorted(groups.items()):
        evidence_levels = {str(row["evidence_level"]) for row in members}
        package_hashes = {
            str(row["evidence_package"]["package_sha256"]) for row in members
        }
        if len(evidence_levels) != 1 or len(package_hashes) != 1:
            raise SellingPointPaidEvalError(
                f"duplicate content {content_id} has inconsistent evidence"
            )
        output.append(
            {
                "group_key": f"{content_id}:{evidence_sha256}",
                "content_id": content_id,
                "evidence_sha256": evidence_sha256,
                "evidence_level": next(iter(evidence_levels)),
                "target": min(members, key=lambda row: int(row["excel_row"])),
                "excel_rows": sorted(int(row["excel_row"]) for row in members),
            }
        )
    return output


def estimate_reservation(
    groups: Sequence[Mapping[str, Any]],
    *,
    index: CharNgramTfidfIndex,
    models: Sequence[ModelPrice],
    transport_attempts: int = 1,
) -> dict[str, Any]:
    prompt_chars: dict[str, int] = {}
    for group in groups:
        if group["evidence_level"] == "V0":
            continue
        prompt = build_prompt(group["target"], index=index)
        prompt_chars[str(group["group_key"])] = len(prompt["system"]) + len(
            prompt["user"]
        )
    by_model: list[dict[str, Any]] = []
    total = 0.0
    for model in models:
        model_total = 0.0
        for characters in prompt_chars.values():
            first_input = math.ceil(characters * INPUT_TOKEN_RESERVE_PER_CHAR)
            second_input = math.ceil(
                (characters + SECOND_CALL_EXTRA_CHARS) * INPUT_TOKEN_RESERVE_PER_CHAR
            )
            model_total += model.cost(first_input, MAX_OUTPUT_TOKENS)
            model_total += model.cost(second_input, MAX_OUTPUT_TOKENS)
        model_total *= transport_attempts
        total += model_total
        by_model.append(
            {
                "model": model.model,
                "potential_calls": len(prompt_chars) * 2,
                "reserved_cny": round(model_total, 8),
            }
        )
    return {
        "input_token_reserve_per_char": INPUT_TOKEN_RESERVE_PER_CHAR,
        "second_call_extra_chars": SECOND_CALL_EXTRA_CHARS,
        "max_output_tokens_per_call": MAX_OUTPUT_TOKENS,
        "unique_model_targets": len(prompt_chars),
        "by_model": by_model,
        # Keep frozen price-manifest estimates stable. Runtime admission uses
        # all transport attempts and rounds up, never below the upper bound.
        "total_reserved_cny": (
            round(total, 8) if transport_attempts == 1
            else math.ceil(total * 100_000_000) / 100_000_000
        ),
    }


def _post_chat(
    api: Mapping[str, str], model: ModelPrice, system: str, user: str
) -> ApiCallResult:
    payload: dict[str, Any] = {
        "model": model.model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": MAX_OUTPUT_TOKENS,
    }
    if model.supports_json_object:
        payload["response_format"] = {"type": "json_object"}
    if model.model.startswith("doubao-"):
        payload["thinking"] = {"type": "disabled"}
    url = str(api["api_base"]).rstrip("/") + str(api["endpoint"])
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    curl = shutil.which("curl")
    if curl is None:
        raise SellingPointCallError(
            "curl is required for the batch hard wall-clock timeout",
            attempts=0,
            reserved_input_tokens=0,
            reserved_output_tokens=0,
        )
    api_key = str(api["api_key"])
    if any(character in api_key for character in "\r\n"):
        raise SellingPointCallError(
            "API key contains an invalid header character",
            attempts=0,
            reserved_input_tokens=0,
            reserved_output_tokens=0,
        )
    config_key = api_key.replace("\\", "\\\\").replace('"', '\\"')
    curl_config = (
        f'header = "Authorization: Bearer {config_key}"\n'
        'header = "Content-Type: application/json"\n'
    ).encode("utf-8")
    started = time.monotonic()
    attempts = 0
    value: Any = None
    last_error: Exception | None = None
    with tempfile.NamedTemporaryFile(prefix="dcar-selling-point-", suffix=".json") as data:
        data.write(body)
        data.flush()
        for attempt in range(MAX_TRANSPORT_ATTEMPTS):
            attempts += 1
            try:
                completed = subprocess.run(  # noqa: S603 - fixed curl binary/args
                    [
                        curl,
                        "--config",
                        "-",
                        "--silent",
                        "--show-error",
                        "--request",
                        "POST",
                        "--connect-timeout",
                        "10",
                        "--max-time",
                        str(CALL_TIMEOUT_SECONDS),
                        "--write-out",
                        "\n%{http_code}",
                        "--data-binary",
                        f"@{data.name}",
                        url,
                    ],
                    input=curl_config,
                    capture_output=True,
                    check=False,
                    timeout=CALL_TIMEOUT_SECONDS + 5,
                )
                if completed.returncode != 0:
                    detail = completed.stderr.decode("utf-8", "replace")[:500]
                    raise OSError(
                        f"curl exit {completed.returncode}: {detail or 'no detail'}"
                    )
                response_text, separator, status_text = completed.stdout.decode(
                    "utf-8"
                ).rpartition("\n")
                if not separator or not status_text.isdigit():
                    raise OSError("curl response has no HTTP status")
                status = int(status_text)
                if status < 200 or status >= 300:
                    last_error = SellingPointCallError(
                        f"{model.model} HTTP {status}: {response_text[:500]}",
                        attempts=attempts,
                        reserved_input_tokens=0,
                        reserved_output_tokens=0,
                    )
                    if status in {429, 500, 502, 503, 504} and attempt + 1 < MAX_TRANSPORT_ATTEMPTS:
                        time.sleep(RATE_LIMIT_RETRY_SECONDS if status == 429 else 2)
                        continue
                    break
                value = json.loads(response_text)
                break
            except (OSError, subprocess.TimeoutExpired) as error:
                last_error = error
                if attempt + 1 < MAX_TRANSPORT_ATTEMPTS:
                    time.sleep(2)
                    continue
                break
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                last_error = error
                break
    reserved_input = math.ceil(
        (len(system) + len(user)) * INPUT_TOKEN_RESERVE_PER_CHAR
    )
    if value is None:
        message = (
            str(last_error)
            if last_error is not None
            else f"{model.model} returned no response"
        )
        raise SellingPointCallError(
            f"{model.model} transport failure after {attempts} attempt(s): {message}",
            attempts=attempts,
            reserved_input_tokens=reserved_input * attempts,
            reserved_output_tokens=MAX_OUTPUT_TOKENS * attempts,
        ) from last_error
    latency_ms = round((time.monotonic() - started) * 1000)
    try:
        raw = str(value["choices"][0]["message"]["content"] or "")
    except (KeyError, IndexError, TypeError) as error:
        raise SellingPointCallError(
            f"{model.model} response has no content",
            attempts=attempts,
            reserved_input_tokens=reserved_input * attempts,
            reserved_output_tokens=MAX_OUTPUT_TOKENS * attempts,
        ) from error
    usage = value.get("usage")
    if not isinstance(usage, Mapping):
        raise SellingPointCallError(
            f"{model.model} response has no usage",
            attempts=attempts,
            reserved_input_tokens=reserved_input * attempts,
            reserved_output_tokens=MAX_OUTPUT_TOKENS * attempts,
        )
    input_tokens = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
    output_tokens = int(
        usage.get("completion_tokens") or usage.get("output_tokens") or 0
    )
    if input_tokens <= 0 or output_tokens <= 0:
        raise SellingPointCallError(
            f"{model.model} response usage is incomplete",
            attempts=attempts,
            reserved_input_tokens=reserved_input * attempts,
            reserved_output_tokens=MAX_OUTPUT_TOKENS * attempts,
        )
    return ApiCallResult(
        raw=raw,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        latency_ms=latency_ms,
        unmetered_transport_attempts=max(0, attempts - 1),
        reserved_input_tokens=reserved_input * max(0, attempts - 1),
        reserved_output_tokens=MAX_OUTPUT_TOKENS * max(0, attempts - 1),
    )


def api_caller(api: Mapping[str, str]) -> ModelCaller:
    return lambda model, system, user: _post_chat(api, model, system, user)


def _call_payload(call: ApiCallResult, model: ModelPrice, *, reason: str) -> dict[str, Any]:
    cost = model.cost(call.input_tokens, call.output_tokens) + model.cost(
        call.reserved_input_tokens, call.reserved_output_tokens
    )
    return {
        "reason": reason,
        "raw_response": call.raw,
        "input_tokens": call.input_tokens,
        "output_tokens": call.output_tokens,
        "latency_ms": call.latency_ms,
        "unmetered_transport_attempts": call.unmetered_transport_attempts,
        "reserved_input_tokens": call.reserved_input_tokens,
        "reserved_output_tokens": call.reserved_output_tokens,
        "cost_cny_upper_bound": round(cost, 10),
    }


def _repair_user(
    original_user: str,
    *,
    previous_raw: str,
    failure: str,
    two_label_codes: Sequence[str] = (),
) -> str:
    instruction: dict[str, Any] = {
        "second_call": True,
        "failure_or_reason": failure,
        "instruction": (
            "重新阅读原任务，只输出符合原schema的JSON；证据quote必须逐字来自channel。"
        ),
        "previous_response": previous_raw[:5000],
    }
    if two_label_codes:
        instruction["instruction"] = (
            "只在给定两个标签之间二判，仍按原schema输出JSON，primary和top3不得出现其他代码。"
        )
        instruction["two_label_codes"] = list(two_label_codes)
    return original_user + "\n\n" + canonical_json(instruction)


def evaluate_group(
    group: Mapping[str, Any],
    *,
    model: ModelPrice,
    index: CharNgramTfidfIndex,
    caller: ModelCaller,
    valid_codes: Iterable[str],
    allow_boundary_second_pass: bool = True,
) -> dict[str, Any]:
    if group["evidence_level"] == "V0":
        raise SellingPointPaidEvalError("V0 groups must never call evaluate_group")
    target = group["target"]
    prompt = build_prompt(target, index=index)
    calls: list[dict[str, Any]] = []
    try:
        first = caller(model, prompt["system"], prompt["user"])
    except SellingPointCallError as error:
        raise GroupEvaluationError(
            str(error), completed_calls=(), failure=error
        ) from error
    calls.append(_call_payload(first, model, reason="base"))
    first_decision: dict[str, Any] | None = None
    validation_error: str | None = None
    try:
        first_decision = validate_model_response(
            parse_model_json(first.raw),
            target=target,
            priority=prompt["priority"],
            valid_codes=valid_codes,
        )
    except SellingPointOfflineError as error:
        validation_error = str(error)

    reason = (
        second_call_reason(first_decision) if first_decision is not None else None
    )
    if reason == "top2_boundary_second_pass" and not allow_boundary_second_pass:
        reason = None
    if validation_error is not None:
        reason = "structure_or_closed_set_repair"
    final_decision = first_decision
    if reason is not None:
        top2: list[str] = []
        second_priority = dict(prompt["priority"])
        if reason == "top2_boundary_second_pass" and first_decision is not None:
            top2 = [str(item["code"]) for item in first_decision["top3"][:2]]
            second_priority["allowed_codes"] = top2
            second_priority["forced_code"] = None
        second_user = _repair_user(
            prompt["user"],
            previous_raw=first.raw,
            failure=validation_error or reason,
            two_label_codes=top2,
        )
        try:
            second = caller(model, prompt["system"], second_user)
        except SellingPointCallError as error:
            raise GroupEvaluationError(
                str(error), completed_calls=calls, failure=error
            ) from error
        calls.append(_call_payload(second, model, reason=reason))
        try:
            second_decision = validate_model_response(
                parse_model_json(second.raw),
                target=target,
                priority=second_priority,
                valid_codes=valid_codes,
            )
            if top2 and second_decision["primary_code"] not in top2:
                raise SellingPointOfflineError("boundary second pass escaped top2")
            final_decision = second_decision
        except SellingPointOfflineError as error:
            if first_decision is None:
                validation_error = f"second call invalid: {error}"
            else:
                validation_error = f"second call ignored: {error}"
                final_decision = first_decision

    status = "accepted" if final_decision is not None else "error"
    return {
        "version": RESULT_VERSION,
        "group_key": str(group["group_key"]),
        "content_id": int(group["content_id"]),
        "evidence_sha256": str(group["evidence_sha256"]),
        "evidence_level": str(group["evidence_level"]),
        "excel_rows": list(group["excel_rows"]),
        "model": model.model,
        "model_slot": model.slot,
        "prompt_version": prompt["prompt_version"],
        "retrieval_index_sha256": index.index_sha256,
        "status": status,
        "decision": final_decision,
        "validation_note": validation_error,
        "calls": calls,
        "input_tokens": sum(int(call["input_tokens"]) for call in calls),
        "output_tokens": sum(int(call["output_tokens"]) for call in calls),
        "latency_ms": sum(int(call["latency_ms"]) for call in calls),
        "cost_cny_upper_bound": round(
            sum(float(call["cost_cny_upper_bound"]) for call in calls), 10
        ),
        "completed_at": _now_utc(),
    }


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    output: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise SellingPointPaidEvalError("result line must be an object")
            output.append(value)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SellingPointPaidEvalError(f"cannot read paid results: {error}") from error
    return output


def terminal_results_by_key(
    results: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str], Mapping[str, Any]]:
    """Return the latest bounded semantic outcome for every model target."""
    output: dict[tuple[str, str], Mapping[str, Any]] = {}
    for result in results:
        status = str(result.get("status") or "")
        if status not in {"accepted", "error"}:
            raise SellingPointPaidEvalError(
                f"unsupported paid result terminal status: {status or '<empty>'}"
            )
        output[(str(result["model"]), str(result["group_key"]))] = result
    return output


def retryable_transport_result(result: Mapping[str, Any]) -> bool:
    return str(result.get("status") or "") == "error" and str(
        result.get("validation_note") or ""
    ).startswith("transport_failure:")


def transport_error_result(
    *,
    failure: Mapping[str, Any],
    group: Mapping[str, Any],
    model: ModelPrice,
    retrieval_index_sha256: str,
) -> dict[str, Any]:
    """Map an exhausted two-attempt transport failure to an incorrect terminal."""
    return {
        "version": RESULT_VERSION,
        "group_key": str(group["group_key"]),
        "content_id": int(group["content_id"]),
        "evidence_sha256": str(group["evidence_sha256"]),
        "evidence_level": str(group["evidence_level"]),
        "excel_rows": list(group["excel_rows"]),
        "model": model.model,
        "model_slot": model.slot,
        "prompt_version": None,
        "retrieval_index_sha256": retrieval_index_sha256,
        "status": "error",
        "decision": None,
        "validation_note": f"transport_failure: {failure.get('error')}",
        # Paid successful calls made before the terminal transport error live in
        # the failure ledger so cost/tokens are not double-counted here.
        "calls": [],
        "input_tokens": 0,
        "output_tokens": 0,
        "latency_ms": 0,
        "cost_cny_upper_bound": 0.0,
        "completed_at": str(failure.get("failed_at") or _now_utc()),
    }


_APPEND_LOCK = threading.Lock()


def _append_ndjson(path: Path, value: Mapping[str, Any]) -> None:
    payload = canonical_json(value) + "\n"
    with _APPEND_LOCK, path.open("a", encoding="utf-8") as output:
        output.write(payload)
        output.flush()


def _write_json_atomic(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _p95(values: Sequence[int]) -> int | None:
    if not values:
        return None
    if len(values) < 20:
        return max(values)
    return round(quantiles(values, n=100, method="inclusive")[94])


def summarize_results(
    *,
    manifest: Mapping[str, Any],
    models: Sequence[ModelPrice],
    group_results: Sequence[Mapping[str, Any]],
    failure_results: Sequence[Mapping[str, Any]],
    reservation: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows = manifest.get("rows")
    if not isinstance(rows, list):
        raise SellingPointPaidEvalError("development manifest rows are missing")
    result_by_key = {
        (str(result["model"]), str(result["group_key"])): result
        for result in group_results
    }
    row_results: list[dict[str, Any]] = []
    model_summaries: list[dict[str, Any]] = []
    for model in models:
        model_rows: list[dict[str, Any]] = []
        for row in rows:
            group_key = f"{int(row['content_id'])}:{row['evidence_sha256']}"
            if row["evidence_level"] == "V0":
                result = {
                    "excel_row": int(row["excel_row"]),
                    "content_id": int(row["content_id"]),
                    "gold_code": str(row["gold_code"]),
                    "predicted_code": None,
                    "matched": False,
                    "status": "v0_no_call",
                    "model": model.model,
                    "confidence": None,
                    "top3": [],
                    "quote_status": None,
                }
            else:
                group_result = result_by_key.get((model.model, group_key))
                if group_result is None:
                    raise SellingPointPaidEvalError(
                        f"missing group result for {model.model} {group_key}"
                    )
                decision = group_result.get("decision")
                predicted = (
                    str(decision.get("primary_code"))
                    if isinstance(decision, Mapping)
                    else None
                )
                result = {
                    "excel_row": int(row["excel_row"]),
                    "content_id": int(row["content_id"]),
                    "gold_code": str(row["gold_code"]),
                    "predicted_code": predicted,
                    "matched": predicted == str(row["gold_code"]),
                    "status": str(group_result["status"]),
                    "model": model.model,
                    "confidence": (
                        decision.get("confidence")
                        if isinstance(decision, Mapping)
                        else None
                    ),
                    "top3": (
                        decision.get("top3", [])
                        if isinstance(decision, Mapping)
                        else []
                    ),
                    "quote_status": (
                        decision.get("status")
                        if isinstance(decision, Mapping)
                        else None
                    ),
                }
            model_rows.append(result)
            row_results.append(result)
        exact = sum(bool(row["matched"]) for row in model_rows)
        strict_scene_contract = isinstance(manifest.get("derived_gold"), Mapping)
        scene_results: dict[str, dict[str, Any]] = {}
        for scene, expected_denominator in G1_SCENE_DENOMINATORS.items():
            scene_rows = [
                row for row in model_rows if str(row["gold_code"]).startswith(scene)
            ]
            denominator = len(scene_rows)
            if strict_scene_contract and denominator != expected_denominator:
                raise SellingPointPaidEvalError(
                    f"unexpected {scene} scene denominator: {denominator}"
                )
            scene_exact = sum(bool(row["matched"]) for row in scene_rows)
            scene_results[scene] = {
                "exact_count": scene_exact,
                "denominator": denominator,
                "accuracy": round(scene_exact / denominator, 6)
                if denominator
                else None,
                "pass_count": G1_SCENE_PASS_COUNTS[scene],
                "passed": (
                    scene_exact >= G1_SCENE_PASS_COUNTS[scene]
                    if strict_scene_contract
                    else True
                ),
            }
        all_codes = sorted(load_label_cards()["cards"])
        label_results: dict[str, dict[str, Any]] = {}
        for code in all_codes:
            code_rows = [row for row in model_rows if row["gold_code"] == code]
            code_exact = sum(bool(row["matched"]) for row in code_rows)
            support = len(code_rows)
            label_results[code] = {
                "support": support,
                "exact_count": code_exact,
                "accuracy": round(code_exact / support, 6) if support else None,
            }
        result_calls = [
            call
            for result in group_results
            if result["model"] == model.model
            for call in result.get("calls", [])
        ]
        model_failures = [
            failure
            for failure in failure_results
            if failure.get("model") == model.model
        ]
        calls = result_calls + [
            call
            for failure in model_failures
            for call in failure.get("completed_calls", [])
        ]
        failed_cost = sum(
            float(failure.get("cost_cny_upper_bound") or 0)
            for failure in model_failures
        )
        overall_passed = exact >= G1_PASS_COUNT
        model_summaries.append(
            {
                "slot": model.slot,
                "model": model.model,
                "exact_count": exact,
                "denominator": G1_DENOMINATOR,
                "accuracy": round(exact / G1_DENOMINATOR, 6),
                "g1_pass": overall_passed
                and all(item["passed"] for item in scene_results.values()),
                "overall_passed": overall_passed,
                "scene_results": scene_results,
                "label_results": label_results,
                "zero_sample_codes": [
                    code for code, item in label_results.items() if item["support"] == 0
                ],
                "api_calls": len(calls),
                "input_tokens": sum(int(call["input_tokens"]) for call in calls),
                "output_tokens": sum(int(call["output_tokens"]) for call in calls),
                "cost_cny_upper_bound": round(
                    sum(
                        float(call["cost_cny_upper_bound"])
                        for call in result_calls
                    )
                    + failed_cost,
                    8,
                ),
                "p95_latency_ms": _p95([int(call["latency_ms"]) for call in calls]),
                "error_groups": sum(
                    result["status"] != "accepted"
                    for (result_model, _), result in result_by_key.items()
                    if result_model == model.model
                ),
                "failed_transport_records": len(model_failures),
                "failed_transport_cost_cny_upper_bound": round(failed_cost, 8),
            }
        )
    model_summaries.sort(
        key=lambda item: (
            -int(item["exact_count"]),
            float(item["cost_cny_upper_bound"]),
            int(item["p95_latency_ms"] or 10**12),
        )
    )
    winner = model_summaries[0]
    total_cost = round(
        sum(float(item["cost_cny_upper_bound"]) for item in model_summaries), 8
    )
    summary: dict[str, Any] = {
        "version": SUMMARY_VERSION,
        "generated_at": _now_utc(),
        "development_manifest_sha256": manifest["manifest_sha256"],
        "g1_contract": {
            "pass_count": G1_PASS_COUNT,
            "denominator": G1_DENOMINATOR,
            "scene_denominators": G1_SCENE_DENOMINATORS,
            "scene_pass_counts": G1_SCENE_PASS_COUNTS,
        },
        "model_results": model_summaries,
        "winner": winner,
        "g1_pass": bool(winner["g1_pass"]),
        "total_cost_cny_upper_bound": total_cost,
        "budget_limit_cny": float(reservation["budget_limit_cny"]),
        "reservation": dict(reservation),
    }
    summary["summary_sha256"] = sha256_json(summary)
    return summary, row_results


def _report_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# 卖点 v5.3 阶段 A 三模型评测",
        "",
        f"- G1：{'通过' if summary['g1_pass'] else '未通过'}",
        f"- 门槛：{G1_PASS_COUNT}/{G1_DENOMINATOR}",
        f"- 公开价费用上界：{float(summary['total_cost_cny_upper_bound']):.4f} 元",
        "",
        "| 模型 | 精确一致 | 准确度 | API调用 | 费用上界(元) | P95(ms) |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for item in summary["model_results"]:
        lines.append(
            f"| {item['model']} | {item['exact_count']}/228 | "
            f"{float(item['accuracy']):.2%} | {item['api_calls']} | "
            f"{float(item['cost_cny_upper_bound']):.4f} | "
            f"{item['p95_latency_ms'] or '-'} |"
        )
    lines.extend(
        [
            "",
            f"获胜模型：`{summary['winner']['model']}`。",
            "",
        ]
    )
    return "\n".join(lines)


def run_bakeoff(
    *,
    manifest: Mapping[str, Any],
    model_config_path: Path,
    output_dir: Path,
    concurrency: int,
    caller: ModelCaller | None = None,
    selected_model_ids: Sequence[str] | None = None,
    allow_boundary_second_pass: bool = True,
    retry_terminal_transport_errors: bool = False,
    minimum_request_interval_seconds: float = 0.0,
    budget_limit_cny: float | None = None,
    shared_spent_cny_upper_bound: float = 0.0,
) -> dict[str, Any]:
    if not 1 <= concurrency <= 16:
        raise SellingPointPaidEvalError("concurrency must be in 1..16")
    rows = manifest.get("rows")
    if not isinstance(rows, list) or len(rows) != G1_DENOMINATOR:
        raise SellingPointPaidEvalError("paid bake-off requires the frozen 228 rows")
    model_config = load_model_config(model_config_path)
    configured_models = model_config["models_loaded"]
    if not isinstance(configured_models, list) or not all(
        isinstance(model, ModelPrice) for model in configured_models
    ):
        raise SellingPointPaidEvalError("loaded model prices are invalid")
    models = select_model_prices(configured_models, selected_model_ids)
    labels = load_label_cards()
    index = CharNgramTfidfIndex(rows)
    groups = manifest_groups(rows)
    reservation = estimate_reservation(groups, index=index, models=models)
    configured_budget_limit = float(model_config["budget_limit_cny"])
    budget_limit = (
        configured_budget_limit
        if budget_limit_cny is None
        else float(budget_limit_cny)
    )
    if not 0 < budget_limit <= configured_budget_limit:
        raise SellingPointPaidEvalError(
            "run budget must be positive and no greater than the frozen config cap"
        )
    reservation["budget_limit_cny"] = budget_limit
    shared_spent = float(shared_spent_cny_upper_bound)
    if not math.isfinite(shared_spent) or not 0 <= shared_spent <= budget_limit:
        raise SellingPointPaidEvalError("invalid shared budget spend")
    if float(reservation["total_reserved_cny"]) > budget_limit:
        raise SellingPointPaidEvalError(
            "worst-case reserved spend exceeds the 500 CNY hard limit"
        )
    api = llm_config()
    availability = verify_model_availability(models, api)
    # The config timestamp records the exact catalog/price freeze used for this
    # run.  Keeping it stable makes a partially completed run safely resumable;
    # availability is still re-queried on every resume before any paid request.
    availability["queried_at"] = str(model_config["queried_at"])
    resolved_caller = paced_caller(
        caller or api_caller(api),
        minimum_interval_seconds=minimum_request_interval_seconds,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    results_path = output_dir / "group_results.ndjson"
    price_manifest_path = output_dir / "price_manifest.json"
    run_manifest_path = output_dir / "run_manifest.json"
    receipt_path = output_dir / "receipt.json"
    failures_path = output_dir / "transport_failures.ndjson"
    summary_path = output_dir / "summary.json"
    row_results_path = output_dir / "row_results.json"
    report_path = output_dir / "stage_a_report.md"

    price_manifest: dict[str, Any] = {
        "version": PRICE_MANIFEST_VERSION,
        "frozen_at": str(model_config["queried_at"]),
        "model_config_path": str(model_config_path.resolve()),
        "model_config_sha256": model_config["config_sha256"],
        "currency": "CNY",
        "unit": "per_million_tokens",
        "cost_basis": model_config["cost_basis"],
        "budget_limit_cny": budget_limit,
        "availability": availability,
        "reservation": reservation,
        "models": [
            {
                "slot": model.slot,
                "requested_model": model.requested_model,
                "model": model.model,
                "replacement_reason": model.replacement_reason,
                "input_cny_per_million_tokens": model.input_rate,
                "output_cny_per_million_tokens": model.output_rate,
                "price_source_url": model.price_source_url,
            }
            for model in models
        ],
    }
    price_manifest["price_manifest_sha256"] = sha256_json(price_manifest)
    run_identity = {
        "version": RUN_VERSION,
        "development_manifest_sha256": manifest["manifest_sha256"],
        "model_config_sha256": model_config["config_sha256"],
        "retrieval_index_sha256": index.index_sha256,
        "price_manifest_sha256": price_manifest["price_manifest_sha256"],
        "allow_boundary_second_pass": allow_boundary_second_pass,
    }
    run_manifest = {**run_identity, "started_at": _now_utc()}

    if run_manifest_path.exists():
        existing = json.loads(run_manifest_path.read_text(encoding="utf-8"))
        if not isinstance(existing, Mapping) or any(
            existing.get(key) != value for key, value in run_identity.items()
        ):
            raise SellingPointPaidEvalError("existing run directory identity mismatch")
        if not price_manifest_path.is_file():
            raise SellingPointPaidEvalError("resumed run has no price manifest")
        existing_price = json.loads(price_manifest_path.read_text(encoding="utf-8"))
        if existing_price.get("price_manifest_sha256") != price_manifest[
            "price_manifest_sha256"
        ]:
            raise SellingPointPaidEvalError("resumed price manifest identity mismatch")
    else:
        if any(
            path.exists()
            for path in (
                results_path,
                price_manifest_path,
                receipt_path,
                failures_path,
                summary_path,
                row_results_path,
                report_path,
            )
        ):
            raise SellingPointPaidEvalError("new run directory contains stale artifacts")
        _write_json_atomic(price_manifest_path, price_manifest)
        _write_json_atomic(run_manifest_path, run_manifest)

    existing_results = _read_ndjson(results_path)
    existing_failures = _read_ndjson(failures_path)
    terminal_existing = terminal_results_by_key(existing_results)
    expected_targets = {
        (model.model, str(group["group_key"])): (model, group)
        for group in groups
        if group["evidence_level"] != "V0"
        for model in models
    }
    latest_failure_by_key = {
        (str(failure.get("model")), str(failure.get("group_key"))): failure
        for failure in existing_failures
    }
    failure_counts: Counter[tuple[str, str]] = Counter(
        (str(failure.get("model")), str(failure.get("group_key")))
        for failure in existing_failures
    )
    unresolved_before_reconciliation = set(expected_targets) - set(terminal_existing)
    reconciled_transport_errors = 0
    for key in sorted(unresolved_before_reconciliation):
        failure = latest_failure_by_key.get(key)
        if failure is None:
            continue
        model, group = expected_targets[key]
        terminal_error = transport_error_result(
            failure=failure,
            group=group,
            model=model,
            retrieval_index_sha256=index.index_sha256,
        )
        _append_ndjson(results_path, terminal_error)
        existing_results.append(terminal_error)
        terminal_existing[key] = terminal_error
        reconciled_transport_errors += 1
    if retry_terminal_transport_errors:
        for key, result in list(terminal_existing.items()):
            if retryable_transport_result(result) and failure_counts[key] < 3:
                terminal_existing.pop(key)
    completed_keys = set(terminal_existing)
    actual_cost = sum(
        float(result.get("cost_cny_upper_bound") or 0) for result in existing_results
    ) + sum(
        float(result.get("cost_cny_upper_bound") or 0)
        for result in existing_failures
    )
    if actual_cost + shared_spent > budget_limit:
        raise SellingPointPaidEvalError("existing receipt already exceeds budget")

    tasks: list[tuple[ModelPrice, Mapping[str, Any]]] = []
    # Interleave model slots so the first in-flight batch acts as a real canary
    # for every effective model before most of the paid corpus is dispatched.
    for group in groups:
        for model in models:
            if group["evidence_level"] == "V0":
                continue
            key = (model.model, str(group["group_key"]))
            if key not in completed_keys:
                tasks.append((model, group))
    total_tasks = len(tasks)
    # Admit the entire pending batch before any request starts. Each group may
    # make two logical calls, and each call may make two billed transport
    # attempts. Historical failures and the other G1 phase share this cap.
    pending_reserve = sum(
        float(estimate_reservation(
            [group], index=index, models=[model],
            transport_attempts=MAX_TRANSPORT_ATTEMPTS,
        )["total_reserved_cny"])
        for model, group in tasks
    )
    if actual_cost + shared_spent + pending_reserve > budget_limit:
        raise SellingPointPaidEvalError(
            "pending calls and transport retries exceed the shared budget"
        )
    resolved_caller = budgeted_caller(
        resolved_caller, remaining_cny=budget_limit - actual_cost - shared_spent
    )
    run_target_count = total_tasks + reconciled_transport_errors
    completed_now = reconciled_transport_errors
    pending: dict[Future[dict[str, Any]], tuple[ModelPrice, Mapping[str, Any]]] = {}
    remaining_tasks = list(tasks)
    in_flight_by_model: dict[str, int] = defaultdict(int)
    model_concurrency_limits = {
        model.model: model_concurrency_limit(model, concurrency) for model in models
    }

    def submit_next(executor: ThreadPoolExecutor) -> bool:
        selected_index = next(
            (
                index
                for index, (candidate, _) in enumerate(remaining_tasks)
                if in_flight_by_model[candidate.model]
                < model_concurrency_limits[candidate.model]
            ),
            None,
        )
        if selected_index is None:
            return False
        model, group = remaining_tasks.pop(selected_index)
        future = executor.submit(
            evaluate_group,
            group,
            model=model,
            index=index,
            caller=resolved_caller,
            valid_codes=labels["cards"],
            allow_boundary_second_pass=allow_boundary_second_pass,
        )
        pending[future] = (model, group)
        in_flight_by_model[model.model] += 1
        return True

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        for _ in range(min(concurrency, total_tasks)):
            submit_next(executor)
        while pending:
            done, _ = wait(tuple(pending), return_when=FIRST_COMPLETED)
            for future in done:
                completed_model, completed_group = pending.pop(future)
                in_flight_by_model[completed_model.model] -= 1
                try:
                    result = future.result()
                except Exception as error:
                    failure_cost = 0.0
                    failure_record: dict[str, Any] | None = None
                    if isinstance(error, GroupEvaluationError):
                        failure_cost = sum(
                            float(call.get("cost_cny_upper_bound") or 0)
                            for call in error.completed_calls
                        ) + completed_model.cost(
                            error.failure.reserved_input_tokens,
                            error.failure.reserved_output_tokens,
                        )
                        failure_record = {
                            "version": RUN_VERSION,
                            "failed_at": _now_utc(),
                            "model": completed_model.model,
                            "model_slot": completed_model.slot,
                            "group_key": str(completed_group["group_key"]),
                            "content_id": int(completed_group["content_id"]),
                            "excel_rows": list(completed_group["excel_rows"]),
                            "error": str(error),
                            "completed_calls": error.completed_calls,
                            "unmetered_transport_attempts": error.failure.attempts,
                            "reserved_input_tokens": (
                                error.failure.reserved_input_tokens
                            ),
                            "reserved_output_tokens": (
                                error.failure.reserved_output_tokens
                            ),
                            "cost_cny_upper_bound": round(failure_cost, 10),
                        }
                        _append_ndjson(failures_path, failure_record)
                        existing_failures.append(failure_record)
                        actual_cost += failure_cost
                        if actual_cost > budget_limit:
                            for outstanding in pending:
                                outstanding.cancel()
                            raise SellingPointPaidEvalError(
                                "terminal transport reserves reached budget limit"
                            )
                        terminal_error = transport_error_result(
                            failure=failure_record,
                            group=completed_group,
                            model=completed_model,
                            retrieval_index_sha256=index.index_sha256,
                        )
                        _append_ndjson(results_path, terminal_error)
                        existing_results.append(terminal_error)
                        completed_keys.add(
                            (
                                completed_model.model,
                                str(completed_group["group_key"]),
                            )
                        )
                        completed_now += 1
                        failure_receipt = {
                            "version": RUN_VERSION,
                            "updated_at": _now_utc(),
                            "completed_group_model_results": len(existing_results),
                            "completed_group_model_targets": len(completed_keys),
                            "transport_failure_records": len(existing_failures),
                            "newly_completed": completed_now,
                            "pending_at_start": run_target_count,
                            "reconciled_transport_errors": (
                                reconciled_transport_errors
                            ),
                            "actual_cost_cny_upper_bound": round(actual_cost, 8),
                            "budget_limit_cny": budget_limit,
                            "budget_remaining_cny": round(
                                budget_limit - actual_cost, 8
                            ),
                            "model_concurrency_limits": model_concurrency_limits,
                            "price_manifest_sha256": price_manifest[
                                "price_manifest_sha256"
                            ],
                        }
                        failure_receipt["receipt_sha256"] = sha256_json(
                            failure_receipt
                        )
                        _write_json_atomic(receipt_path, failure_receipt)
                        print(
                            canonical_json(
                                {
                                    "terminal_transport_error": completed_model.model,
                                    "group_key": str(completed_group["group_key"]),
                                    "cost_cny_upper_bound": round(actual_cost, 6),
                                }
                            ),
                            flush=True,
                        )
                        submit_next(executor)
                        continue
                    for outstanding in pending:
                        outstanding.cancel()
                    raise SellingPointPaidEvalError(
                        "paid call failed for "
                        f"{completed_model.model} {completed_group['group_key']}: {error}"
                    ) from error
                result_cost = float(result["cost_cny_upper_bound"])
                if actual_cost + result_cost > budget_limit:
                    for outstanding in pending:
                        outstanding.cancel()
                    raise SellingPointPaidEvalError("actual paid usage reached budget limit")
                actual_cost += result_cost
                _append_ndjson(results_path, result)
                existing_results.append(result)
                completed_keys.add((str(result["model"]), str(result["group_key"])))
                completed_now += 1
                receipt = {
                    "version": RUN_VERSION,
                    "updated_at": _now_utc(),
                    "completed_group_model_results": len(existing_results),
                    "completed_group_model_targets": len(completed_keys),
                    "transport_failure_records": len(existing_failures),
                    "newly_completed": completed_now,
                    "pending_at_start": run_target_count,
                    "reconciled_transport_errors": reconciled_transport_errors,
                    "actual_cost_cny_upper_bound": round(actual_cost, 8),
                    "budget_limit_cny": budget_limit,
                    "budget_remaining_cny": round(budget_limit - actual_cost, 8),
                    "model_concurrency_limits": model_concurrency_limits,
                    "price_manifest_sha256": price_manifest[
                        "price_manifest_sha256"
                    ],
                }
                receipt["receipt_sha256"] = sha256_json(receipt)
                _write_json_atomic(receipt_path, receipt)
                if completed_now % 10 == 0 or completed_now == run_target_count:
                    print(
                        canonical_json(
                            {
                                "progress": f"{completed_now}/{run_target_count}",
                                "model": completed_model.model,
                                "cost_cny_upper_bound": round(actual_cost, 6),
                            }
                        ),
                        flush=True,
                    )
                submit_next(executor)

    final_results = _read_ndjson(results_path)
    expected_results = len(models) * sum(
        group["evidence_level"] != "V0" for group in groups
    )
    terminal_final = terminal_results_by_key(final_results)
    if len(terminal_final) != expected_results:
        raise SellingPointPaidEvalError("paid result target coverage is incomplete")
    summary, row_results = summarize_results(
        manifest=manifest,
        models=models,
        group_results=final_results,
        failure_results=existing_failures,
        reservation=reservation,
    )
    if not math.isclose(
        float(summary["total_cost_cny_upper_bound"]),
        actual_cost,
        rel_tol=0.0,
        abs_tol=1e-8,
    ):
        raise SellingPointPaidEvalError("summary and receipt cost totals diverge")
    _write_json_atomic(summary_path, summary)
    _write_json_atomic(row_results_path, row_results)
    report_path.write_text(_report_markdown(summary), encoding="utf-8")
    final_receipt = {
        "version": RUN_VERSION,
        "updated_at": _now_utc(),
        "completed_group_model_results": len(final_results),
        "completed_group_model_targets": len(terminal_final),
        "transport_failure_records": len(existing_failures),
        "newly_completed": completed_now,
        "pending_at_start": run_target_count,
        "reconciled_transport_errors": reconciled_transport_errors,
        "actual_cost_cny_upper_bound": round(actual_cost, 8),
        "budget_limit_cny": budget_limit,
        "budget_remaining_cny": round(budget_limit - actual_cost, 8),
        "model_concurrency_limits": model_concurrency_limits,
        "price_manifest_sha256": price_manifest["price_manifest_sha256"],
    }
    final_receipt["receipt_sha256"] = sha256_json(final_receipt)
    _write_json_atomic(receipt_path, final_receipt)
    return {
        "summary": summary,
        "summary_path": str(summary_path),
        "row_results_path": str(row_results_path),
        "report_path": str(report_path),
        "receipt_path": str(receipt_path),
        "price_manifest_path": str(price_manifest_path),
        "newly_completed_group_model_results": completed_now,
    }
