"""User-level automotive-interest rate (A/U) per channel and business scene.

The rate is a ratio over the *user union* of a slice, never an average of
per-content percentages:

    automotive_user_rate = |A| / |U| x 100%

* ``U`` — platform interaction users (``platform-user-hmac-v2``) with a valid
  first-level comment on the slice's contents inside the report window,
  deduplicated once per slice. The same user may appear in several scenes, so
  scene user counts do not sum to the channel total.
* ``A`` — the subset of ``U`` classified ``automotive`` under the active
  classifier version.

The status machine is fixed priority (first match wins):

1. no published content in the window                 -> not_applicable
2. content exists but U = 0                            -> missing
3. classifier rejected, identity coverage < 95%,
   classification coverage < 100%, or U < 30          -> below_threshold (no %)
4. classifier conservative or uncalibrated,
   30 <= U < 100, or comment collection coverage
   unknown / < 90%                                     -> sample_only (shows %)
5. classifier approved, U >= 100, identity >= 95%,
   classification = 100%, comment coverage >= 90%     -> available

``uncalibrated`` (2026-08-07 owner decision, Mark): when the gold-set
calibration record has never been created, machine estimates publish anyway,
permanently capped at ``sample_only`` — never ``available``. A record that
EXISTS but fails validation or fails the precision/recall gates still
collapses to ``rejected`` and publishes nothing: a measured-bad or tampered
classifier is worse than an unmeasured one.

Identity coverage outranks headcount on purpose: missing identities are a
systematic selection bias that more volume cannot fix.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .audience_classifier import (
    AUDIENCE_DEFINITION_VERSION,
    CLASSIFIER_VERSION,
    EVIDENCE_WINDOW_DAYS,
    state_from_counts,
)
from .contracts import ratio_metric
from .audience_selectors import (
    latest_comment_rows,
    latest_user_classifications,
    timestamp_at_or_after,
    timestamp_before,
)
from .storage import PROJECT_ROOT

MIN_USERS_AVAILABLE = 100
MIN_USERS_SAMPLE = 30
IDENTITY_COVERAGE_GATE = 95.0
COMMENT_COVERAGE_GATE = 90.0
CLASSIFICATION_COVERAGE_GATE = 100.0
COMMENT_CAP = 1000

CALIBRATION_RECORD_VERSION = "audience-calibration-v1"
CALIBRATION_RECORD_PATH = PROJECT_ROOT / "config" / "audience_calibration_v1.json"
CALIBRATION_PLATFORMS = ("douyin", "xiaohongshu")
MIN_CALIBRATION_SAMPLE = 500

_CLASSIFIER_STATES = {"approved", "conservative", "rejected", "uncalibrated"}
_STATE_RANK = {"approved": 0, "conservative": 1, "rejected": 2}


def _slice_user_universe(
    connection: sqlite3.Connection,
    content_ids: Sequence[int],
    *,
    report_cutoff_at: Optional[str] = None,
    evidence_window_start: Optional[str] = None,
    evidence_window_end: Optional[str] = None,
) -> Dict[str, Any]:
    """Distinct users and classifications from one canonical as-of snapshot."""

    comments = latest_comment_rows(
        connection,
        content_ids,
        report_cutoff_at=report_cutoff_at,
        evidence_window_start=evidence_window_start,
        evidence_window_end=evidence_window_end,
    )
    total_comments = len(comments)
    stable_comments = [
        row for row in comments if row.get("interaction_user_id") is not None
    ]
    stable = len(stable_comments)
    user_ids = sorted({int(row["interaction_user_id"]) for row in stable_comments})
    latest_evidence_by_user: Dict[int, str] = {}
    latest_behavior_by_user: Dict[int, str] = {}
    for row in stable_comments:
        user_id = int(row["interaction_user_id"])
        captured_at = str(row["evidence_captured_at"])
        current = latest_evidence_by_user.get(user_id)
        if current is None or timestamp_at_or_after(captured_at, current):
            latest_evidence_by_user[user_id] = captured_at
        published_at = str(row["published_at"])
        current_behavior = latest_behavior_by_user.get(user_id)
        if current_behavior is None or timestamp_at_or_after(
            published_at, current_behavior
        ):
            latest_behavior_by_user[user_id] = published_at

    classifications = latest_user_classifications(
        connection,
        user_ids,
        audience_definition_version=AUDIENCE_DEFINITION_VERSION,
        classifier_version=CLASSIFIER_VERSION,
        report_cutoff_at=report_cutoff_at,
        evidence_window_end=evidence_window_end,
    )
    fresh_classifications = {
        user_id: value
        for user_id, value in classifications.items()
        if timestamp_at_or_after(
            str(value["created_at"]), latest_evidence_by_user[user_id]
        )
        and timestamp_before(
            latest_behavior_by_user[user_id], str(value["evidence_window_end"])
        )
    }
    candidate_users = len(user_ids)
    classified_users = len(fresh_classifications)
    eligible_users = {
        user_id
        for user_id, value in fresh_classifications.items()
        if value.get("label") != "excluded"
    }
    automotive_users = sum(
        value.get("label") == "automotive"
        for value in fresh_classifications.values()
    )
    identity_coverage = (
        round(stable * 100 / total_comments, 2) if total_comments else None
    )
    classification_coverage = (
        round(classified_users * 100 / candidate_users, 2)
        if candidate_users
        else None
    )
    evidence_contents = int(
        connection.execute(
            f"""
            SELECT COUNT(DISTINCT content_id) FROM comment_evidence_versions
            WHERE content_id IN ({",".join("?" for _ in content_ids)})
            """,
            tuple(int(cid) for cid in content_ids),
        ).fetchone()[0]
    )
    user_key_versions = sorted(
        {
            str(row["interaction_user_key_version"])
            for row in stable_comments
            if row.get("interaction_user_key_version")
        }
    )
    return {
        "candidate_users": candidate_users,
        "total_users": len(eligible_users),
        "automotive_users": automotive_users,
        "classified_users": classified_users,
        "identity_coverage_percentage": identity_coverage,
        "classification_coverage_percentage": classification_coverage,
        "first_level_comment_count": total_comments,
        "evidence_content_count": evidence_contents,
        "user_key_versions": user_key_versions,
    }


def _slice_comment_coverage(
    connection: sqlite3.Connection,
    content_ids: Sequence[int],
    *,
    report_cutoff_at: str,
) -> Dict[str, Any]:
    """Weighted comment-collection coverage from capture runs.

    coverage = Σ captured_distinct / Σ max(declared, captured) x 100. Contents
    whose declared total is unknown while more pages remain make the whole
    slice coverage unknown (None). A content capped at 1,000 does not by itself
    drag the slice down — it contributes captured to both sides.
    """

    if not content_ids:
        return {
            "captured": 0,
            "declared": 0,
            "coverage_percentage": None,
            "capped_content_count": 0,
        }
    ids = list(dict.fromkeys(int(cid) for cid in content_ids))
    placeholders = ",".join("?" for _ in ids)
    rows = connection.execute(
        f"""
        WITH ranked AS (
            SELECT r.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY r.content_id
                       ORDER BY julianday(COALESCE(r.completed_at,r.updated_at)) DESC,
                                r.id DESC
                   ) selector_rank
            FROM comment_capture_runs r
            WHERE r.content_id IN ({placeholders})
              AND julianday(COALESCE(r.completed_at,r.updated_at)) <= julianday(?)
        )
        SELECT content_id, status, completion_kind, declared_total_count,
               captured_distinct_count, comment_cap
        FROM ranked WHERE selector_rank=1
        """,
        (*ids, report_cutoff_at),
    ).fetchall()
    captured_sum = 0
    denom_sum = 0
    capped = 0
    unknown = len(rows) != len(ids)
    for row in rows:
        captured = int(row["captured_distinct_count"] or 0)
        declared = row["declared_total_count"]
        completion = row["completion_kind"]
        captured_sum += captured
        if completion == "cap_reached":
            capped += 1
            denom_sum += captured
        elif completion in {"provider_exhausted", "zero_comments"}:
            # These runs consumed the complete provider-accessible L1 set.
            # Upstream declared totals can include replies/filtered rows and
            # are retained on the run for audit, not used as missing pages.
            denom_sum += captured
        elif declared is not None:
            denom_sum += max(int(declared), captured)
        else:
            unknown = True
    if unknown:
        coverage = None
    else:
        coverage = round(captured_sum * 100 / denom_sum, 2) if denom_sum else None
    return {
        "captured": captured_sum,
        "declared": denom_sum,
        "coverage_percentage": coverage,
        "capped_content_count": capped,
    }


@dataclass(frozen=True)
class _MetricDecision:
    status: str
    reason: str


def _decide_metric(
    *,
    publication_count: int,
    candidate_users: int,
    classified_users: int,
    total_users: int,
    identity_coverage: Optional[float],
    classification_coverage: Optional[float],
    classification_complete: bool,
    comment_coverage: Optional[float],
    capped_content_count: int,
    classifier_state: str,
    first_level_comments: int = 0,
    evidence_contents: int = 0,
    approximate_identity_keys: bool = False,
) -> _MetricDecision:
    """Resolve status and its canonical reason in one priority-ordered pass."""

    if publication_count == 0:
        return _MetricDecision(
            "not_applicable", "所选时间内没有相关内容"
        )
    if candidate_users == 0:
        if evidence_contents == 0:
            reason = "评论还没有采集，暂时无法计算互动用户"
        elif first_level_comments == 0:
            reason = "所选时间内没有评论互动"
        else:
            reason = "有评论，但无法识别评论用户"
        return _MetricDecision("missing", reason)
    if identity_coverage is None or identity_coverage < IDENTITY_COVERAGE_GATE:
        display = identity_coverage if identity_coverage is not None else "未知"
        return _MetricDecision(
            "below_threshold",
            f"能够识别用户的评论占 {display}%，低于至少 {IDENTITY_COVERAGE_GATE:.0f}% 的要求",
        )
    if not classification_complete:
        display = (
            classification_coverage
            if classification_coverage is not None
            else "未知"
        )
        return _MetricDecision(
            "below_threshold",
            "完成用户分类的比例为 "
            f"{display}%（{classified_users}/{candidate_users}），还有用户未完成分类",
        )
    if total_users == 0:
        return _MetricDecision(
            "missing",
            "评论用户都不符合统计条件，暂时无法计算比例",
        )
    if classifier_state == "rejected":
        return _MetricDecision(
            "below_threshold", "用户分类结果还没完成校验，暂不显示比例"
        )
    if total_users < MIN_USERS_SAMPLE:
        return _MetricDecision(
            "below_threshold",
            f"去掉重复用户后只有 {total_users} 人，少于至少 {MIN_USERS_SAMPLE} 人的要求",
        )
    if classifier_state == "uncalibrated":
        return _MetricDecision(
            "sample_only", "用户分类结果还没有人工核对，数值仅供参考"
        )
    if comment_coverage is None:
        return _MetricDecision("sample_only", "无法确认评论是否采集完整，结果仅供参考")
    if comment_coverage < COMMENT_COVERAGE_GATE:
        return _MetricDecision(
            "sample_only",
            f"已采集评论占 {comment_coverage}%，低于至少 90%，结果仅供参考",
        )
    if capped_content_count > 0:
        return _MetricDecision(
            "sample_only",
            f"有 {capped_content_count} 条内容只采集了前 {COMMENT_CAP} 条评论，结果仅供参考",
        )
    if total_users < MIN_USERS_AVAILABLE:
        return _MetricDecision(
            "sample_only",
            f"去掉重复用户后有 {total_users} 人，人数仍然较少，结果仅供参考",
        )
    if approximate_identity_keys:
        return _MetricDecision(
            "sample_only", "部分历史评论用户只能近似识别，数值仅供参考"
        )
    if classifier_state == "conservative":
        return _MetricDecision("sample_only", "用户分类采用保守判断，数值仅供参考")
    return _MetricDecision("available", "")


def compute_slice_rate(
    connection: sqlite3.Connection,
    content_ids: Sequence[int],
    *,
    publication_count: int,
    classifier_state: str,
    evidence_window_start: str,
    evidence_window_end: str,
    report_cutoff_at: str,
    warm_up: bool,
) -> Dict[str, Any]:
    """Return ``{"metric": ratio_metric, "audience_quality": {...}}`` for a slice."""

    if classifier_state not in _CLASSIFIER_STATES:
        raise ValueError(f"unknown classifier state: {classifier_state}")
    unique_ids = list(dict.fromkeys(int(cid) for cid in content_ids))
    if unique_ids:
        universe = _slice_user_universe(
            connection,
            unique_ids,
            report_cutoff_at=report_cutoff_at,
            evidence_window_start=evidence_window_start,
            evidence_window_end=evidence_window_end,
        )
        coverage = _slice_comment_coverage(
            connection,
            unique_ids,
            report_cutoff_at=report_cutoff_at,
        )
    else:
        universe = {
            "total_users": 0,
            "candidate_users": 0,
            "automotive_users": 0,
            "classified_users": 0,
            "identity_coverage_percentage": None,
            "classification_coverage_percentage": None,
            "first_level_comment_count": 0,
            "evidence_content_count": 0,
            "user_key_versions": [],
        }
        coverage = {
            "captured": 0,
            "declared": 0,
            "coverage_percentage": None,
            "capped_content_count": 0,
        }
    candidate_users = int(universe["candidate_users"])
    total_users = int(universe["total_users"])
    automotive_users = int(universe["automotive_users"])
    classified_users = int(universe["classified_users"])
    identity_coverage = universe["identity_coverage_percentage"]
    classification_coverage = universe["classification_coverage_percentage"]
    comment_coverage = coverage["coverage_percentage"]

    classification_complete = classified_users == candidate_users
    user_key_versions = sorted(universe["user_key_versions"], reverse=True)
    decision = _decide_metric(
        publication_count=publication_count,
        candidate_users=candidate_users,
        classified_users=classified_users,
        total_users=total_users,
        identity_coverage=identity_coverage,
        classification_coverage=classification_coverage,
        classification_complete=classification_complete,
        comment_coverage=comment_coverage,
        capped_content_count=int(coverage["capped_content_count"]),
        classifier_state=classifier_state,
        first_level_comments=int(universe["first_level_comment_count"]),
        evidence_contents=int(universe["evidence_content_count"]),
        approximate_identity_keys=any(
            value != "platform-user-hmac-v2" for value in user_key_versions
        ),
    )
    status = decision.status
    publishes_percentage = status in {"available", "sample_only"}
    metric_denominator = total_users if classification_complete else candidate_users
    metric = ratio_metric(
        automotive_users if publishes_percentage else None,
        metric_denominator,
        status=status,
        eligible_count=metric_denominator,
        coverage_percentage=comment_coverage,
        reason=decision.reason,
    )
    audience_quality = {
        "captured_comment_count": int(coverage["captured"]),
        "declared_comment_count": int(coverage["declared"]),
        "comment_collection_coverage_percentage": comment_coverage,
        "identity_coverage_percentage": identity_coverage,
        "candidate_user_count": candidate_users,
        "classified_user_count": classified_users,
        "classification_coverage_percentage": classification_coverage,
        "capped_content_count": int(coverage["capped_content_count"]),
        "audience_definition_version": AUDIENCE_DEFINITION_VERSION,
        "classifier_version": CLASSIFIER_VERSION,
        # 如实标注该切片用户实际使用的键域；含 v1 回溯键即为混合降级口径。
        "user_key_version": "+".join(user_key_versions) or "platform-user-hmac-v2",
        "evidence_window_start": evidence_window_start,
        "evidence_window_end": evidence_window_end,
        "report_cutoff_at": report_cutoff_at,
        "warm_up": bool(warm_up),
    }
    return {"metric": metric, "audience_quality": audience_quality}


def load_calibration_record(
    record_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Load and fail-closed-validate the gold-set calibration record.

    The record is the auditable file ``config/audience_calibration_v1.json``
    written by the production runbook after the 500-user/platform human
    labeling pass. Every defect — missing file, unparsable JSON, version
    mismatch, missing platform, sub-500 sample, negative/absent counts, or a
    declared ``expected_state`` that disagrees with the recomputed one —
    collapses the effective state to ``rejected``. States are always
    recomputed from the confusion counts through the fixed gates
    (:func:`audience_classifier.state_from_counts`); the file cannot assert a
    state the counts do not support.

    Returns ``{"state", "platforms", "reasons", "record"}`` where ``state``
    is the effective publication state: the weakest platform state when the
    record is defect-free, otherwise ``rejected``.
    """

    path = Path(record_path) if record_path is not None else CALIBRATION_RECORD_PATH
    if not path.exists():
        # Never-created record: publish uncalibrated machine estimates,
        # capped at sample_only (2026-08-07 owner decision, Mark). This is
        # distinct from a record that exists but is defective — that stays
        # rejected below, because it means tampering or a failed calibration.
        return {
            "state": "uncalibrated",
            "platforms": {},
            "reasons": ["定标记录尚未建立，按未定标口径发布（上限仅样本）"],
            "record": None,
        }
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {
            "state": "rejected",
            "platforms": {},
            "reasons": ["定标记录存在但不可解析"],
            "record": None,
        }
    reasons: List[str] = []
    if not isinstance(record, Mapping):
        return {
            "state": "rejected",
            "platforms": {},
            "reasons": ["定标记录必须是 JSON 对象"],
            "record": None,
        }
    for key, expected in (
        ("record_version", CALIBRATION_RECORD_VERSION),
        ("audience_definition_version", AUDIENCE_DEFINITION_VERSION),
        ("classifier_version", CLASSIFIER_VERSION),
    ):
        if record.get(key) != expected:
            reasons.append(f"{key} 必须等于 {expected}")
    platforms_block = record.get("platforms")
    if not isinstance(platforms_block, Mapping):
        reasons.append("platforms 必须是对象")
        platforms_block = {}
    platform_states: Dict[str, str] = {}
    for platform in CALIBRATION_PLATFORMS:
        block = platforms_block.get(platform)
        if not isinstance(block, Mapping):
            reasons.append(f"{platform} 缺少定标结果")
            continue
        valid = True
        for key in (
            "true_positive",
            "false_positive",
            "false_negative",
            "true_negative",
        ):
            value = block.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                reasons.append(f"{platform}.{key} 必须是非负整数")
                valid = False
        if not valid:
            continue
        true_positive = int(block["true_positive"])
        false_positive = int(block["false_positive"])
        false_negative = int(block["false_negative"])
        true_negative = int(block["true_negative"])
        sample = true_positive + false_positive + false_negative + true_negative
        if sample < MIN_CALIBRATION_SAMPLE:
            reasons.append(
                f"{platform} 金标样本 {sample} 人，低于 {MIN_CALIBRATION_SAMPLE} 人门槛"
            )
            continue
        state = state_from_counts(true_positive, false_positive, false_negative)
        expected_state = block.get("expected_state")
        if expected_state is not None and expected_state != state:
            reasons.append(
                f"{platform} 声明状态 {expected_state} 与按固定门槛重算的 {state} 不一致"
            )
            continue
        platform_states[platform] = state
    missing = [
        platform
        for platform in CALIBRATION_PLATFORMS
        if platform not in platform_states
    ]
    if reasons or missing:
        effective = "rejected"
    else:
        effective = max(
            (platform_states[platform] for platform in CALIBRATION_PLATFORMS),
            key=lambda state: _STATE_RANK[state],
        )
    return {
        "state": effective,
        "platforms": platform_states,
        "reasons": reasons,
        "record": dict(record),
    }


def active_classifier_state(
    connection: sqlite3.Connection,
    *,
    record_path: Optional[Path] = None,
) -> str:
    """Return the calibrated classifier state for publication decisions.

    Resolves through :func:`load_calibration_record`. A never-created record
    yields ``uncalibrated`` — the rate publishes machine estimates capped at
    ``sample_only`` (2026-08-07 owner decision). A record that exists but is
    defective or fails the gates yields ``rejected`` and publishes nothing.
    With a valid record the effective state is the weakest platform state
    (both douyin and xiaohongshu must pass). Overview and report generation
    must both resolve the state through this single function.
    """

    del connection  # the record is file-based (schema 10 is frozen)
    return str(load_calibration_record(record_path)["state"])


def default_warm_up(evidence_window_end: str, switchover_date: Optional[str]) -> bool:
    """Warm-up while the 90-day cross-content path has not fully accumulated."""

    if not switchover_date:
        return False
    from datetime import datetime, timedelta

    try:
        end = datetime.fromisoformat(evidence_window_end.replace("Z", "+00:00"))
        switch = datetime.fromisoformat(switchover_date.replace("Z", "+00:00"))
    except ValueError:
        return False
    return end < switch + timedelta(days=EVIDENCE_WINDOW_DAYS)


def build_channel_audience_rates(
    connection: sqlite3.Connection,
    rows: Sequence[Mapping[str, Any]],
    *,
    classifier_state: str,
    evidence_window_start: str,
    evidence_window_end: str,
    report_cutoff_at: str,
    warm_up: bool,
    channels: Sequence[tuple[str, str]],
    scenes: Sequence[tuple[str, str]],
) -> Dict[str, Dict[str, Any]]:
    """Compute automotive_user_rate + audience_quality for every slice."""

    result: Dict[str, Dict[str, Any]] = {}
    for platform, _label in channels:
        channel_rows = [r for r in rows if r["platform"] == platform]
        channel_ids = [int(r["content_id"]) for r in channel_rows]

        def slice_value(content_ids: Sequence[int], publication_count: int) -> Dict[str, Any]:
            return compute_slice_rate(
                connection,
                content_ids,
                publication_count=publication_count,
                classifier_state=classifier_state,
                evidence_window_start=evidence_window_start,
                evidence_window_end=evidence_window_end,
                report_cutoff_at=report_cutoff_at,
                warm_up=warm_up,
            )

        scene_values: Dict[str, Any] = {}
        for scene, _scene_label in scenes:
            scene_ids = [
                int(r["content_id"])
                for r in channel_rows
                if r["content_direction"] == scene
            ]
            scene_values[scene] = slice_value(scene_ids, len(scene_ids))
        result[platform] = {
            "summary": slice_value(channel_ids, len(channel_rows)),
            "scenes": scene_values,
        }
    return result
