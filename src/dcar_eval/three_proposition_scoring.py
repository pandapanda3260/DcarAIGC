#!/usr/bin/env python3
"""Executable scoring rules for the three Xiaohongshu propositions."""

from __future__ import annotations

from typing import Mapping


MIN_VALID_COMMENTERS = 5


def _validate_score(value: float | int, name: str) -> float:
    number = float(value)
    if not 0 <= number <= 100:
        raise ValueError(f"{name} must be between 0 and 100")
    return number


def _validate_reliability(value: float | int, name: str) -> float:
    number = float(value)
    if not 0 <= number <= 1:
        raise ValueError(f"{name} must be between 0 and 1")
    return number


def content_auto_score(
    *,
    text_score: float | int | None,
    text_reliability: float | int,
    media_score: float | int | None,
    media_reliability: float | int,
    comment_topic_score: float | int | None = None,
    valid_unique_commenters: int | None = None,
) -> tuple[int, float]:
    """Return (integer content score, continuous comment adjustment)."""

    text_r = _validate_reliability(text_reliability, "text_reliability")
    media_r = _validate_reliability(media_reliability, "media_reliability")
    weighted_sum = 0.0
    weight_sum = 0.0
    if text_score is not None and text_r > 0:
        weighted_sum += 0.55 * _validate_score(text_score, "text_score") * text_r
        weight_sum += 0.55 * text_r
    if media_score is not None and media_r > 0:
        weighted_sum += 0.45 * _validate_score(media_score, "media_score") * media_r
        weight_sum += 0.45 * media_r
    if weight_sum == 0:
        raise ValueError("at least one usable text or media evidence group is required")

    base = weighted_sum / weight_sum
    adjustment = 0.0
    if (
        valid_unique_commenters is not None
        and valid_unique_commenters >= MIN_VALID_COMMENTERS
        and comment_topic_score is not None
    ):
        topic = _validate_score(comment_topic_score, "comment_topic_score")
        adjustment = max(-5.0, min(5.0, 0.10 * (topic - base)))
    final = round(max(0.0, min(100.0, base + adjustment)))
    return final, adjustment


def audience_auto_score(
    score_counts: Mapping[int | str, int],
    *,
    valid_unique_commenters: int | None,
    comment_sample_status: str,
) -> int | None:
    """Average unique-commenter scores; return None below the 5-person gate."""

    normalized = {int(key): int(value) for key, value in score_counts.items()}
    allowed = {0, 30, 70, 100}
    if set(normalized) - allowed:
        raise ValueError("audience score buckets must be 0, 30, 70, or 100")
    if any(value < 0 for value in normalized.values()):
        raise ValueError("audience score bucket counts cannot be negative")
    total = sum(normalized.values())
    if comment_sample_status != "scorable" or valid_unique_commenters is None:
        return None
    if total != valid_unique_commenters:
        raise ValueError("audience score bucket counts must equal valid_unique_commenters")
    if valid_unique_commenters < MIN_VALID_COMMENTERS:
        return None
    return round(sum(score * count for score, count in normalized.items()) / total)


def dcd_acquisition_score(
    *,
    content_score: float | int,
    audience_score: float | int | None,
    dcd_fit_score: float | int | None,
    action_intent_score: float | int | None,
) -> int | None:
    """Return the predicted acquisition score, or None when comments are unscorable."""

    if audience_score is None or dcd_fit_score is None or action_intent_score is None:
        return None
    content = _validate_score(content_score, "content_score")
    audience = _validate_score(audience_score, "audience_score")
    fit = _validate_score(dcd_fit_score, "dcd_fit_score")
    intent = _validate_score(action_intent_score, "action_intent_score")
    return round(0.20 * content + 0.25 * audience + 0.35 * fit + 0.20 * intent)


def content_conclusion(score: int) -> str:
    _validate_score(score, "score")
    if score >= 85:
        return "明确属于汽车内容"
    if score >= 70:
        return "主体属于汽车内容，同时包含其他场景"
    if score >= 40:
        return "与汽车有关，但汽车不是明确主体，需复核"
    return "不属于汽车内容"


def audience_conclusion(score: int | None) -> str | None:
    if score is None:
        return None
    _validate_score(score, "score")
    if score >= 80:
        return "互动用户明确是汽车目标人群"
    if score >= 60:
        return "多数互动用户具有汽车兴趣或需求"
    if score >= 40:
        return "汽车用户与泛兴趣用户混合"
    return "互动主要来自非汽车或泛娱乐人群"


def acquisition_conclusion(score: int | None) -> str | None:
    if score is None:
        return None
    _validate_score(score, "score")
    if score >= 80:
        return "具备明确下载理由，建议优先进入拉新实验"
    if score >= 65:
        return "存在清晰承接需求，值得进入拉新实验"
    if score >= 40:
        return "与懂车帝有关联，但下载理由不足，只建议小流量验证"
    return "缺少迁移到懂车帝的理由，当前不建议用于拉新"
