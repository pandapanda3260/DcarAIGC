"""SPU 关联 LLM 辅助链路（B 链，无人工复核版）。

设计基线：`docs/SPU人群场景关联与统计方案_v0.1.md` §4.3（2026-08-15 修订：
取消人工复核队列，改为"机器四道闸自动把关 + 全程留痕 + 一键回滚"）。

规则优先，LLM 只做三件事：
1. 补空白——规则没挂上车型/场景/人群的内容；
2. 裁灰区——规则灰区（60–74）：与规则同向（置信 ≥0.7）升级为确认，
   相反且置信 ≥0.8 时按 LLM 落新行并在证据里记冲突；
3. 款型细化——规则已确认车系、库里有款型但未细化时，在该车系款型
   闭集里选（款型库为空时自动跳过此项）。

四道自动闸（validate_judgement）：
1. 闭集选择：车系/款型/场景/人群只能从候选清单里选 id，答出清单外直接
   丢弃（库外新车系只累计计数进运行摘要）；
2. 原文证据校验：每项结论必须附原文片段，程序逐字核对该片段存在于
   发给模型的对应通道文本里，对不上丢弃（零成本防幻觉）；
3. 置信度门槛：低于 0.7 丢弃（灰区反向覆盖需 ≥0.8）；
4. 规则优先：规则明确命中（confirmed）的车型/人群/场景 LLM 一律不推翻。

落库与回滚：LLM 补充的行写现有三张 links 表，rule_version 固定为
``spu-llm-v1``，evidence_json 带置信度与原文引文；反悔时执行
``invalidate_llm_links``（或 SQL：UPDATE 三张表 SET invalidated_at=当前时间
WHERE rule_version='spu-llm-v1' AND invalidated_at IS NULL）后重跑刷新即回
纯规则状态。

成本控制：判定结果按 (content_id, 文本哈希, 模型, prompt 版本) 缓存进
``llm_judgements``；规则重算后文本没变的内容直接重放缓存判定，不再调用
大模型付费。调用凭据从 ``/Users/mark/Documents/key/DcarKey/dcar.env.local``
的 PROJECT_CLASSIFIER_* 变量读取（火山方舟 OpenAI 兼容接口），key 缺失时
整条 B 链自动停用，规则链路不受影响。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

LOGGER = logging.getLogger(__name__)

LLM_RULE_VERSION = "spu-llm-v1"
# 场景闭集 v0.2 重新定义了 S6（纯商务接待）与 S8（承接载货）。
# prompt 版本同步提升，防止旧缓存把「载货」判为 S6 后重放。
LLM_PROMPT_VERSION = "llm-prompt-v2"
DEFAULT_LLM_MODEL = "doubao-seed-2-1-pro-260628"
DEFAULT_API_BASE = "https://ark.cn-beijing.volces.com/api/v3"
DEFAULT_API_ENDPOINT = "/chat/completions"
KEY_FILE = Path("/Users/mark/Documents/key/DcarKey/dcar.env.local")

CONFIDENCE_THRESHOLD = 0.7
OVERRIDE_THRESHOLD = 0.8
MAX_QUOTE_CHARS = 80
MIN_QUOTE_CHARS = 2
MAX_SCENES_PER_CONTENT = 3

#: 喂给模型的各通道文本截断上限（字符）；证据校验只对照截断后的文本。
TEXT_LIMITS = {"title": 200, "body": 1500, "asr": 1800, "ocr": 800}

CALL_TIMEOUT_SECONDS = 90
CALL_BACKOFFS = (3.0, 10.0)
DEFAULT_CONCURRENCY = 8
CONSECUTIVE_ERROR_ABORT = 8
PERSIST_BATCH = 50

_CHANNELS = ("title", "body", "asr", "ocr")

#: 接口自适应开关：部分模型不接受 thinking / response_format 字段，收到
#: 相应 400 后本进程内不再携带（跨线程只置位，无需加锁）。
_API_FLAGS = {"drop_thinking": False, "drop_response_format": False}


class LlmDisabledError(RuntimeError):
    """凭据缺失或被禁用时抛出；调用方降级为纯规则。"""


class LlmCallError(RuntimeError):
    """单次调用在重试后仍失败。"""


# ---------------------------------------------------------------------------
# 配置装载（凭据只在运行时读取，不进仓库、不进日志）
# ---------------------------------------------------------------------------

def _read_env_file(path: Path) -> Dict[str, str]:
    if not path.is_file():
        return {}
    values: Dict[str, str] = {}
    try:
        text = path.read_text(encoding="utf-8-sig")
    except OSError:
        return {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        values[name.strip()] = value.strip().strip("\"'")
    return values


def llm_config() -> Dict[str, str]:
    """装配调用配置；key 不可用时抛 LlmDisabledError。"""

    if os.environ.get("DCAR_LLM_DISABLED", "").strip() == "1":
        raise LlmDisabledError("LLM 辅助已通过 DCAR_LLM_DISABLED 关闭")
    key_file = Path(os.environ.get("DCAR_LLM_KEY_FILE", "").strip() or KEY_FILE)
    file_values = _read_env_file(key_file)

    def _value(name: str, default: str = "") -> str:
        return os.environ.get(name, "").strip() or file_values.get(name, "") or default

    api_key = _value("PROJECT_CLASSIFIER_API_KEY")
    if not api_key or api_key.lower().startswith("your"):
        raise LlmDisabledError(f"未找到可用的大模型 key（{key_file}）")
    return {
        "api_key": api_key,
        "api_base": _value("PROJECT_CLASSIFIER_API_BASE", DEFAULT_API_BASE),
        "endpoint": _value("PROJECT_CLASSIFIER_API_ENDPOINT", DEFAULT_API_ENDPOINT),
        "model": _value("PROJECT_CLASSIFIER_MODEL", DEFAULT_LLM_MODEL),
    }


def llm_available() -> bool:
    try:
        llm_config()
    except LlmDisabledError:
        return False
    return True


# ---------------------------------------------------------------------------
# 文本准备与候选闭集
# ---------------------------------------------------------------------------

def prepare_texts(texts: Dict[str, str]) -> Dict[str, str]:
    """按通道截断，作为喂给模型与证据校验共用的口径。"""

    return {
        channel: str(texts.get(channel, "") or "")[: TEXT_LIMITS[channel]]
        for channel in _CHANNELS
    }


def text_sha256(prepared: Dict[str, str]) -> str:
    canonical = json.dumps(
        {channel: prepared[channel] for channel in _CHANNELS},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_candidates(assets: Dict[str, Any]) -> Dict[str, Any]:
    """从规则资产构建候选闭集（车系/款型/场景/人群）。"""

    series_lines: List[str] = []
    trims_by_series: Dict[str, List[Tuple[str, str]]] = {}
    for spu in sorted(
        assets["catalog"].values(), key=lambda item: str(item["series_slug"])
    ):
        if int(spu["is_series_node"]):
            series_lines.append(
                f"{spu['series_slug']}|{spu['brand']}|{spu['series']}"
            )
        else:
            label_parts = [
                str(spu["trim_label"] or ""),
                f"{spu['model_year']}款" if spu["model_year"] is not None else "",
            ]
            trims_by_series.setdefault(str(spu["series_slug"]), []).append(
                (str(spu["spu_id"]), " ".join(part for part in label_parts if part))
            )
    scene_lines = [
        f"{scene['code']}|{scene['label']}|{scene['definition']}"
        for scene in assets["scenes"]
    ]
    audience_lines = [
        f"{audience['code']}|{audience['label']}|{audience['definition']}"
        for audience in assets["audiences"]
    ]
    return {
        "series_lines": series_lines,
        "series_slugs": {line.split("|", 1)[0] for line in series_lines},
        "trims_by_series": trims_by_series,
        "scene_lines": scene_lines,
        "scene_codes": {str(scene["code"]) for scene in assets["scenes"]},
        "audience_lines": audience_lines,
        "audience_codes": {str(audience["code"]) for audience in assets["audiences"]},
    }


def build_system_prompt(candidates: Dict[str, Any]) -> str:
    """系统提示词：任务定义 + 候选闭集 + 输出契约（全部调用共用，利于前缀缓存）。"""

    return (
        "你是汽车内容打标助手。根据给定的内容文本（标题/正文/口播ASR/画面文字OCR），"
        "判定内容主要讨论的车系、用车场景与目标人群。\n"
        "硬规则：\n"
        "1. 车系、场景、人群只能从下方候选清单里选 id，禁止自造；\n"
        "2. 每项判定必须给出 channel（title/body/asr/ocr）和 quote——quote 必须是该通道原文里"
        f"逐字连续出现的片段（{MIN_QUOTE_CHARS}-{MAX_QUOTE_CHARS} 字），不得改写；\n"
        "3. confidence 取 0-1，拿不准就给低值或返回 null，宁缺毋滥；\n"
        "4. 内容明确讨论某个具体车系但候选清单里没有时，把车系名写进 out_of_catalog，"
        "series_slug 返回 null；\n"
        "5. 场景最多给 " + str(MAX_SCENES_PER_CONTENT) + " 个；内容与用车无关时三项都返回 null。\n"
        "只输出 JSON，格式：\n"
        '{"spu":{"series_slug":"...","trim_id":null,"confidence":0.9,'
        '"channel":"title","quote":"..."},"out_of_catalog":null,'
        '"scenes":[{"code":"S1","confidence":0.8,"channel":"body","quote":"..."}],'
        '"audience":{"code":"P5","confidence":0.8,"channel":"asr","quote":"..."}}\n'
        "spu/audience 无法判定时对应字段取 null，scenes 取 []。\n\n"
        "候选车系（series_slug|品牌|车系）：\n" + "\n".join(candidates["series_lines"]) + "\n\n"
        "候选用车场景（code|名称|定义）：\n" + "\n".join(candidates["scene_lines"]) + "\n\n"
        "候选目标人群（code|名称|定义）：\n" + "\n".join(candidates["audience_lines"])
    )


def build_user_prompt(
    prepared: Dict[str, str], target: Dict[str, Any], candidates: Dict[str, Any]
) -> str:
    sections = [
        f"标题：{prepared['title'] or '（无）'}",
        f"正文：{prepared['body'] or '（无）'}",
        f"口播ASR：{prepared['asr'] or '（无）'}",
        f"画面文字OCR：{prepared['ocr'] or '（无）'}",
    ]
    notes: List[str] = []
    if target.get("spu_state") == "confirmed" and target.get("primary_slug"):
        notes.append(
            f"规则已确认车系 {target['primary_slug']}，spu 判定仅在需要款型细化时使用"
        )
        trims = candidates["trims_by_series"].get(str(target["primary_slug"]), [])
        if trims and not target.get("trim_resolved"):
            trim_lines = "\n".join(f"{trim_id}|{label}" for trim_id, label in trims)
            notes.append("请在该车系候选款型（trim_id|款型）里细化：\n" + trim_lines)
    elif target.get("gray_slugs"):
        notes.append(
            "规则给出灰区候选车系：" + "、".join(target["gray_slugs"]) + "，请独立判定"
        )
    if notes:
        sections.append("补充信息：" + "；".join(notes))
    return "\n".join(sections)


# ---------------------------------------------------------------------------
# 大模型调用（线程安全：worker 只做 HTTP，不碰 SQLite）
# ---------------------------------------------------------------------------

def _build_payload(config: Dict[str, str], system: str, user: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "model": config["model"],
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 800,
    }
    if not _API_FLAGS["drop_response_format"]:
        payload["response_format"] = {"type": "json_object"}
    if not _API_FLAGS["drop_thinking"]:
        payload["thinking"] = {"type": "disabled"}
    return payload


def _post_json(url: str, api_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=CALL_TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


def call_llm(
    config: Dict[str, str], system: str, user: str
) -> Tuple[str, Dict[str, int]]:
    """带重试与字段自适应的单次调用；返回 (原始文本, token 用量)。"""

    url = config["api_base"].rstrip("/") + config["endpoint"]
    last_error: Optional[Exception] = None
    for attempt in range(len(CALL_BACKOFFS) + 1):
        payload = _build_payload(config, system, user)
        try:
            data = _post_json(url, config["api_key"], payload)
        except urllib.error.HTTPError as error:
            detail = ""
            try:
                detail = error.read().decode("utf-8", "replace")[:500]
            except Exception:  # noqa: BLE001
                pass
            if error.code == 400 and "thinking" in detail and not _API_FLAGS["drop_thinking"]:
                _API_FLAGS["drop_thinking"] = True
                continue
            if (
                error.code == 400
                and "response_format" in detail
                and not _API_FLAGS["drop_response_format"]
            ):
                _API_FLAGS["drop_response_format"] = True
                continue
            if error.code in (429, 500, 502, 503, 504) and attempt < len(CALL_BACKOFFS):
                last_error = error
                sleep(CALL_BACKOFFS[attempt])
                continue
            raise LlmCallError(f"大模型调用失败 HTTP {error.code}: {detail[:200]}") from error
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            if attempt < len(CALL_BACKOFFS):
                last_error = error
                sleep(CALL_BACKOFFS[attempt])
                continue
            raise LlmCallError(f"大模型调用网络失败：{error}") from error
        try:
            content = str(data["choices"][0]["message"]["content"] or "")
        except (KeyError, IndexError, TypeError) as error:
            raise LlmCallError(f"大模型响应缺少 choices：{str(data)[:200]}") from error
        usage = data.get("usage") or {}
        return content, {
            "input_tokens": int(usage.get("prompt_tokens") or 0),
            "output_tokens": int(usage.get("completion_tokens") or 0),
        }
    raise LlmCallError(f"大模型调用重试仍失败：{last_error}")


_JSON_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)


def parse_response(raw: str) -> Optional[Dict[str, Any]]:
    text = _JSON_FENCE_RE.sub("", str(raw or "").strip()).strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match is None:
            return None
        try:
            parsed = json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
    return parsed if isinstance(parsed, dict) else None


# ---------------------------------------------------------------------------
# 四道闸校验（纯函数，便于单测）
# ---------------------------------------------------------------------------

_WHITESPACE_RE = re.compile(r"\s+")


def _normalize(text: str) -> str:
    return _WHITESPACE_RE.sub("", str(text or "")).lower()


def _quote_ok(
    entry: Dict[str, Any], prepared: Dict[str, str]
) -> Tuple[bool, str]:
    channel = str(entry.get("channel") or "")
    quote = str(entry.get("quote") or "").strip()
    if channel not in _CHANNELS:
        return False, "channel-invalid"
    if not (MIN_QUOTE_CHARS <= len(quote) <= MAX_QUOTE_CHARS):
        return False, "quote-length"
    if _normalize(quote) not in _normalize(prepared.get(channel, "")):
        return False, "quote-not-found"
    return True, ""


def _confidence(entry: Dict[str, Any]) -> float:
    try:
        value = float(entry.get("confidence"))
    except (TypeError, ValueError):
        return 0.0
    return value if 0.0 <= value <= 1.0 else 0.0


def validate_judgement(
    parsed: Optional[Dict[str, Any]],
    prepared: Dict[str, str],
    candidates: Dict[str, Any],
    target: Dict[str, Any],
) -> Dict[str, Any]:
    """四道闸：闭集选择 → 原文证据 → 置信度门槛 → 规则优先。

    返回 verdict：``spu``/``trim``/``scenes``/``audience`` 为通过校验、允许
    落库的判定；``rejects`` 记录每一条被拦下的原因；``out_of_catalog`` 是
    库外新车系名（只计数，不落库）。
    """

    verdict: Dict[str, Any] = {
        "spu": None,
        "trim": None,
        "scenes": [],
        "audience": None,
        "rejects": [],
        "out_of_catalog": None,
    }
    if not isinstance(parsed, dict):
        verdict["rejects"].append({"dim": "all", "reason": "parse-error"})
        return verdict

    def _reject(dim: str, reason: str) -> None:
        verdict["rejects"].append({"dim": dim, "reason": reason})

    out_of_catalog = parsed.get("out_of_catalog")
    if isinstance(out_of_catalog, str) and out_of_catalog.strip():
        verdict["out_of_catalog"] = out_of_catalog.strip()[:80]

    spu_state = str(target.get("spu_state") or "none")
    spu_entry = parsed.get("spu")
    if isinstance(spu_entry, dict) and (
        spu_entry.get("series_slug") or spu_entry.get("trim_id")
    ):
        slug = str(spu_entry.get("series_slug") or "")
        confidence = _confidence(spu_entry)
        quote_valid, quote_reason = _quote_ok(spu_entry, prepared)
        trims_by_series = candidates["trims_by_series"]
        if slug and slug not in candidates["series_slugs"]:
            _reject("spu", "series-not-in-catalog")
        elif not quote_valid:
            _reject("spu", quote_reason)
        elif spu_state == "confirmed":
            # 规则优先：不推翻已确认车系，只允许款型细化。
            primary_slug = str(target.get("primary_slug") or "")
            trim_id = str(spu_entry.get("trim_id") or "")
            if not trim_id:
                pass  # 模型未给款型，无事可做
            elif target.get("trim_resolved"):
                pass  # 规则已细化，不覆盖
            elif slug and slug != primary_slug:
                _reject("trim", "series-mismatch-rule-priority")
            elif trim_id not in {
                candidate_id
                for candidate_id, _ in trims_by_series.get(primary_slug, [])
            }:
                _reject("trim", "trim-not-in-series")
            elif confidence < CONFIDENCE_THRESHOLD:
                _reject("trim", "low-confidence")
            else:
                verdict["trim"] = {
                    "trim_id": trim_id,
                    "confidence": confidence,
                    "channel": str(spu_entry.get("channel")),
                    "quote": str(spu_entry.get("quote") or "").strip(),
                }
        elif not slug:
            pass  # 只有 out_of_catalog 或空判定
        else:
            gray_slugs = set(target.get("gray_slugs") or [])
            if spu_state == "gray" and slug in gray_slugs:
                required, action = CONFIDENCE_THRESHOLD, "gray-upgrade"
            elif spu_state == "gray":
                required, action = OVERRIDE_THRESHOLD, "gray-override"
            else:
                required, action = CONFIDENCE_THRESHOLD, "fill"
            if confidence < required:
                _reject("spu", "low-confidence")
            else:
                trim_id = str(spu_entry.get("trim_id") or "")
                if trim_id and trim_id not in {
                    candidate_id
                    for candidate_id, _ in trims_by_series.get(slug, [])
                }:
                    trim_id = ""
                verdict["spu"] = {
                    "series_slug": slug,
                    "trim_id": trim_id or None,
                    "confidence": confidence,
                    "channel": str(spu_entry.get("channel")),
                    "quote": str(spu_entry.get("quote") or "").strip(),
                    "action": action,
                }

    linked_scenes = set(target.get("scene_codes") or [])
    scenes_entry = parsed.get("scenes")
    if isinstance(scenes_entry, list):
        for entry in scenes_entry[:MAX_SCENES_PER_CONTENT]:
            if not isinstance(entry, dict):
                continue
            code = str(entry.get("code") or "")
            if code not in candidates["scene_codes"]:
                _reject("scene", "scene-not-in-dim")
                continue
            if code in linked_scenes:
                continue  # 规则已识别，跳过不算拒绝
            quote_valid, quote_reason = _quote_ok(entry, prepared)
            if not quote_valid:
                _reject("scene", quote_reason)
                continue
            confidence = _confidence(entry)
            if confidence < CONFIDENCE_THRESHOLD:
                _reject("scene", "low-confidence")
                continue
            verdict["scenes"].append({
                "code": code,
                "confidence": confidence,
                "channel": str(entry.get("channel")),
                "quote": str(entry.get("quote") or "").strip(),
            })

    audience_entry = parsed.get("audience")
    if isinstance(audience_entry, dict) and audience_entry.get("code"):
        if target.get("has_audience"):
            pass  # 规则已归因，不覆盖
        else:
            code = str(audience_entry.get("code") or "")
            quote_valid, quote_reason = _quote_ok(audience_entry, prepared)
            confidence = _confidence(audience_entry)
            if code not in candidates["audience_codes"]:
                _reject("audience", "audience-not-in-dim")
            elif not quote_valid:
                _reject("audience", quote_reason)
            elif confidence < CONFIDENCE_THRESHOLD:
                _reject("audience", "low-confidence")
            else:
                verdict["audience"] = {
                    "code": code,
                    "confidence": confidence,
                    "channel": str(audience_entry.get("channel")),
                    "quote": str(audience_entry.get("quote") or "").strip(),
                }
    return verdict


def verdict_accepts(verdict: Dict[str, Any]) -> bool:
    return bool(
        verdict.get("spu")
        or verdict.get("trim")
        or verdict.get("scenes")
        or verdict.get("audience")
    )


# ---------------------------------------------------------------------------
# 落库（与规则行同表，rule_version='spu-llm-v1'）
# ---------------------------------------------------------------------------

def _evidence_payload(entry: Dict[str, Any], model: str, **extra: Any) -> str:
    payload = {
        "source": "llm",
        "model": model,
        "confidence": entry.get("confidence"),
        "channel": entry.get("channel"),
        "quote": entry.get("quote"),
    }
    payload.update(extra)
    return json.dumps(payload, ensure_ascii=False)


def apply_verdict(
    connection: sqlite3.Connection,
    target: Dict[str, Any],
    verdict: Dict[str, Any],
    assets: Dict[str, Any],
    model: str,
    captured_at: str,
    evidence_level: str,
) -> Dict[str, int]:
    """把通过四道闸的判定写进 links 表；返回各维度落库计数。

    调用方须已处于事务中。规则行一概不动；灰区升级/款型细化通过
    "失效旧行 + 追加 LLM 行"完成，保证审计链完整。
    """

    counters = {
        "spu_filled": 0,
        "gray_upgraded": 0,
        "gray_overridden": 0,
        "trim_refined": 0,
        "scene_filled": 0,
        "audience_filled": 0,
    }
    content_id = int(target["content_id"])
    score_cap = 100 if evidence_level == "V3" else 90

    def _has_primary() -> bool:
        row = connection.execute(
            """
            SELECT 1 FROM content_spu_links
            WHERE content_id=? AND invalidated_at IS NULL AND is_primary=1
            LIMIT 1
            """,
            (content_id,),
        ).fetchone()
        return row is not None

    spu_entry = verdict.get("spu")
    if spu_entry:
        slug = str(spu_entry["series_slug"])
        action = str(spu_entry.get("action") or "fill")
        score = min(int(round(spu_entry["confidence"] * 100)), score_cap)
        extra: Dict[str, Any] = {"action": action}
        if action == "gray-upgrade":
            gray_row = connection.execute(
                """
                SELECT id, score FROM content_spu_links
                WHERE content_id=? AND spu_id=? AND status='gray'
                  AND invalidated_at IS NULL
                ORDER BY score DESC LIMIT 1
                """,
                (content_id, slug),
            ).fetchone()
            if gray_row is not None:
                connection.execute(
                    "UPDATE content_spu_links SET invalidated_at=? WHERE id=?",
                    (captured_at, int(gray_row["id"])),
                )
                extra["rule_gray_score"] = int(gray_row["score"])
                score = max(score, int(gray_row["score"]))
        elif action == "gray-override":
            extra["conflict_with_gray"] = sorted(target.get("gray_slugs") or [])
        trim_id = spu_entry.get("trim_id")
        spu_id = str(trim_id) if trim_id else slug
        resolved_level = "trim" if trim_id else "series"
        is_primary = 0 if _has_primary() else 1
        connection.execute(
            """
            INSERT INTO content_spu_links(
                content_id, spu_id, resolved_level, is_primary, status, score,
                evidence_json, rule_version, created_at, invalidated_at
            ) VALUES (?, ?, ?, ?, 'confirmed', ?, ?, ?, ?, NULL)
            """,
            (
                content_id, spu_id, resolved_level, is_primary, score,
                _evidence_payload(spu_entry, model, **extra),
                LLM_RULE_VERSION, captured_at,
            ),
        )
        if action == "gray-upgrade":
            counters["gray_upgraded"] += 1
        elif action == "gray-override":
            counters["gray_overridden"] += 1
        else:
            counters["spu_filled"] += 1

    trim_entry = verdict.get("trim")
    if trim_entry:
        primary_row = connection.execute(
            """
            SELECT id, spu_id, score FROM content_spu_links
            WHERE content_id=? AND invalidated_at IS NULL AND is_primary=1
              AND resolved_level='series' AND status='confirmed'
            LIMIT 1
            """,
            (content_id,),
        ).fetchone()
        if primary_row is not None:
            connection.execute(
                "UPDATE content_spu_links SET invalidated_at=? WHERE id=?",
                (captured_at, int(primary_row["id"])),
            )
            connection.execute(
                """
                INSERT INTO content_spu_links(
                    content_id, spu_id, resolved_level, is_primary, status, score,
                    evidence_json, rule_version, created_at, invalidated_at
                ) VALUES (?, ?, 'trim', 1, 'confirmed', ?, ?, ?, ?, NULL)
                """,
                (
                    content_id, str(trim_entry["trim_id"]),
                    int(primary_row["score"]),
                    _evidence_payload(
                        trim_entry, model,
                        action="trim-refine",
                        rule_score=int(primary_row["score"]),
                        rule_series=str(primary_row["spu_id"]),
                    ),
                    LLM_RULE_VERSION, captured_at,
                ),
            )
            counters["trim_refined"] += 1

    llm_scene_codes: List[str] = []
    for scene_entry in verdict.get("scenes") or []:
        score = min(int(round(scene_entry["confidence"] * 100)), score_cap)
        connection.execute(
            """
            INSERT INTO content_scene_links(
                content_id, scene_code, score, evidence_json, rule_version,
                created_at, invalidated_at
            ) VALUES (?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                content_id, str(scene_entry["code"]), score,
                _evidence_payload(scene_entry, model),
                LLM_RULE_VERSION, captured_at,
            ),
        )
        llm_scene_codes.append(str(scene_entry["code"]))
        counters["scene_filled"] += 1

    audience_entry = verdict.get("audience")
    if audience_entry:
        code = str(audience_entry["code"])
        scene_map = assets.get("scene_map") or {}
        allowed_entry = scene_map.get(code, {"core": set(), "related": set()})
        allowed_codes = set(allowed_entry["core"]) | set(allowed_entry["related"])
        all_scene_codes = set(target.get("scene_codes") or []) | set(llm_scene_codes)
        consistency_flag = any(
            scene_code not in allowed_codes for scene_code in all_scene_codes
        )
        connection.execute(
            """
            INSERT INTO content_audience_links(
                content_id, audience_code, source, conflict_flag,
                consistency_flag, evidence_json, rule_version, created_at,
                invalidated_at
            ) VALUES (?, ?, 'llm', 0, ?, ?, ?, ?, NULL)
            """,
            (
                content_id, code, 1 if consistency_flag else 0,
                _evidence_payload(audience_entry, model),
                LLM_RULE_VERSION, captured_at,
            ),
        )
        counters["audience_filled"] += 1
    return counters


def invalidate_llm_links(
    connection: sqlite3.Connection, captured_at: str
) -> int:
    """一键回滚：失效所有 LLM 补充行（判定缓存保留，重跑可重放）。"""

    total = 0
    for table in (
        "content_spu_links", "content_scene_links", "content_audience_links"
    ):
        cursor = connection.execute(
            f"""
            UPDATE {table} SET invalidated_at=?
            WHERE rule_version=? AND invalidated_at IS NULL
            """,
            (captured_at, LLM_RULE_VERSION),
        )
        total += int(cursor.rowcount)
    return total


# ---------------------------------------------------------------------------
# 判定缓存（重算不重复付费）
# ---------------------------------------------------------------------------

def fetch_cached_response(
    connection: sqlite3.Connection,
    content_id: int,
    digest: str,
    config: Dict[str, str],
) -> Optional[str]:
    row = connection.execute(
        """
        SELECT status, response_json FROM llm_judgements
        WHERE content_id=? AND text_sha256=? AND model=? AND prompt_version=?
        """,
        (content_id, digest, config["model"], LLM_PROMPT_VERSION),
    ).fetchone()
    if row is None or str(row["status"]) == "error":
        return None
    return str(row["response_json"] or "")


def upsert_judgement(
    connection: sqlite3.Connection,
    content_id: int,
    digest: str,
    config: Dict[str, str],
    *,
    status: str,
    response_json: str,
    verdict_json: str,
    usage: Dict[str, int],
    captured_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO llm_judgements(
            content_id, text_sha256, model, prompt_version, status,
            response_json, verdict_json, input_tokens, output_tokens, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(content_id, text_sha256, model, prompt_version)
        DO UPDATE SET status=excluded.status,
            response_json=excluded.response_json,
            verdict_json=excluded.verdict_json,
            input_tokens=excluded.input_tokens,
            output_tokens=excluded.output_tokens,
            created_at=excluded.created_at
        """,
        (
            content_id, digest, config["model"], LLM_PROMPT_VERSION, status,
            response_json, verdict_json,
            int(usage.get("input_tokens") or 0),
            int(usage.get("output_tokens") or 0),
            captured_at,
        ),
    )


# ---------------------------------------------------------------------------
# 批量执行（run_association 的 llm_hook / CLI 共用）
# ---------------------------------------------------------------------------

def target_needs_llm(
    target: Dict[str, Any], candidates: Dict[str, Any]
) -> bool:
    if str(target.get("spu_state")) != "confirmed":
        return True
    if not target.get("scene_codes"):
        return True
    if not target.get("has_audience"):
        return True
    if not target.get("trim_resolved"):
        primary_slug = str(target.get("primary_slug") or "")
        if candidates["trims_by_series"].get(primary_slug):
            return True
    return False


def run_llm_pass(
    connection: sqlite3.Connection,
    *,
    run_id: Optional[int],
    targets: Sequence[Dict[str, Any]],
    assets: Dict[str, Any],
    assemble_texts: Callable[[Dict[str, Any]], Dict[str, str]],
    now_utc: Callable[[], str],
    transaction: Callable[[sqlite3.Connection], Any],
    config: Optional[Dict[str, str]] = None,
    limit: Optional[int] = None,
    concurrency: Optional[int] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> Dict[str, Any]:
    """对规则链后仍未解决的内容执行 LLM 补空/裁灰/细化。

    - ``targets``：run_association 循环收集的未解决内容元数据；
    - ``assemble_texts``：按 target 重新装配四通道文本（含 ASR/OCR 读盘）；
    - worker 线程只做 HTTP；校验与落库全部在主线程分批事务完成；
    - 连续多次调用失败（CONSECUTIVE_ERROR_ABORT）自动中止，摘要带 aborted。
    """

    started = monotonic()
    if config is None:
        config = llm_config()  # 可能抛 LlmDisabledError，由调用方决定降级
    candidates = build_candidates(assets)
    system_prompt = build_system_prompt(candidates)
    pending = [
        target for target in targets if target_needs_llm(target, candidates)
    ]
    if limit is not None:
        pending = pending[: max(0, int(limit))]
    summary: Dict[str, Any] = {
        "enabled": True,
        "model": config["model"],
        "targets": len(pending),
        "processed": 0,
        "called": 0,
        "cache_hits": 0,
        "accepted": 0,
        "rejected": 0,
        "errors": 0,
        "spu_filled": 0,
        "gray_upgraded": 0,
        "gray_overridden": 0,
        "trim_refined": 0,
        "scene_filled": 0,
        "audience_filled": 0,
        "out_of_catalog": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "aborted": None,
    }
    if not pending:
        summary["duration_seconds"] = round(monotonic() - started, 1)
        return summary

    workers = max(1, int(
        concurrency
        or os.environ.get("DCAR_LLM_CONCURRENCY", "").strip()
        or DEFAULT_CONCURRENCY
    ))
    consecutive_errors = 0
    reject_reasons: Dict[str, int] = {}

    def _update_run_progress() -> None:
        if run_id is None:
            return
        connection.execute(
            "UPDATE spu_association_runs SET summary_json=? WHERE id=?",
            (
                json.dumps(
                    {
                        "phase": "llm",
                        "llm_processed": summary["processed"],
                        "llm_total": summary["targets"],
                    },
                    ensure_ascii=False,
                ),
                run_id,
            ),
        )

    def _call_worker(
        item: Tuple[Dict[str, Any], Dict[str, str], str]
    ) -> Tuple[Dict[str, Any], Dict[str, str], str, Optional[str], Dict[str, int], Optional[str]]:
        target, prepared, digest = item
        try:
            raw, usage = call_llm(
                config, system_prompt, build_user_prompt(prepared, target, candidates)
            )
            return target, prepared, digest, raw, usage, None
        except LlmCallError as error:
            return target, prepared, digest, None, {}, str(error)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        batch_size = max(PERSIST_BATCH, workers * 4)
        for start in range(0, len(pending), batch_size):
            if summary["aborted"]:
                break
            batch = pending[start:start + batch_size]
            cached_items: List[Tuple[Dict[str, Any], Dict[str, str], str, str]] = []
            to_call: List[Tuple[Dict[str, Any], Dict[str, str], str]] = []
            for target in batch:
                prepared = prepare_texts(assemble_texts(target))
                digest = text_sha256(prepared)
                cached = fetch_cached_response(
                    connection, int(target["content_id"]), digest, config
                )
                if cached is not None:
                    cached_items.append((target, prepared, digest, cached))
                else:
                    to_call.append((target, prepared, digest))
            results: List[
                Tuple[Dict[str, Any], Dict[str, str], str, Optional[str], Dict[str, int], Optional[str]]
            ] = [
                (target, prepared, digest, raw, {}, None)
                for target, prepared, digest, raw in cached_items
            ]
            summary["cache_hits"] += len(cached_items)
            if to_call:
                called_results = list(executor.map(_call_worker, to_call))
                summary["called"] += len(to_call)
                results.extend(called_results)
            captured_at = now_utc()
            with transaction(connection):
                for target, prepared, digest, raw, usage, error in results:
                    summary["processed"] += 1
                    summary["input_tokens"] += int(usage.get("input_tokens") or 0)
                    summary["output_tokens"] += int(usage.get("output_tokens") or 0)
                    if error is not None:
                        summary["errors"] += 1
                        consecutive_errors += 1
                        upsert_judgement(
                            connection, int(target["content_id"]), digest, config,
                            status="error", response_json="",
                            verdict_json=json.dumps(
                                {"error": error[:300]}, ensure_ascii=False
                            ),
                            usage=usage, captured_at=captured_at,
                        )
                        if consecutive_errors >= CONSECUTIVE_ERROR_ABORT:
                            summary["aborted"] = (
                                f"连续 {consecutive_errors} 次调用失败，"
                                "本轮补充已中止（规则结果不受影响）"
                            )
                        continue
                    consecutive_errors = 0
                    verdict = validate_judgement(
                        parse_response(raw or ""), prepared, candidates, target
                    )
                    if verdict.get("out_of_catalog"):
                        summary["out_of_catalog"] += 1
                    accepted = verdict_accepts(verdict)
                    if accepted:
                        counters = apply_verdict(
                            connection, target, verdict, assets, config["model"],
                            captured_at, str(target.get("evidence_level") or "V2"),
                        )
                        for key, value in counters.items():
                            summary[key] += value
                        summary["accepted"] += 1
                    else:
                        summary["rejected"] += 1
                        for reject in verdict.get("rejects") or []:
                            reason = str(reject.get("reason"))
                            reject_reasons[reason] = reject_reasons.get(reason, 0) + 1
                    if usage:  # 只有真实调用才写缓存；缓存重放不覆盖原判定
                        upsert_judgement(
                            connection, int(target["content_id"]), digest, config,
                            status="accepted" if accepted else "rejected",
                            response_json=str(raw or ""),
                            verdict_json=json.dumps(verdict, ensure_ascii=False),
                            usage=usage, captured_at=captured_at,
                        )
                _update_run_progress()
            if progress_callback is not None:
                progress_callback(summary["processed"], summary["targets"])
    summary["reject_reasons"] = dict(
        sorted(reject_reasons.items(), key=lambda item: -item[1])[:10]
    )
    summary["duration_seconds"] = round(monotonic() - started, 1)
    return summary
