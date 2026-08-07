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
   or U < 30                                           -> below_threshold (no %)
4. classifier conservative, 30 <= U < 100, or comment
   collection coverage unknown / < 90%                 -> sample_only (shows %)
5. classifier approved, U >= 100, identity >= 95%,
   comment coverage >= 90%                             -> available

Identity coverage outranks headcount on purpose: missing identities are a
systematic selection bias that more volume cannot fix.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .audience_classifier import (
    AUDIENCE_DEFINITION_VERSION,
    CLASSIFIER_VERSION,
    EVIDENCE_WINDOW_DAYS,
    state_from_counts,
)
from .contracts import ratio_metric
from .storage import PROJECT_ROOT

MIN_USERS_AVAILABLE = 100
MIN_USERS_SAMPLE = 30
IDENTITY_COVERAGE_GATE = 95.0
COMMENT_COVERAGE_GATE = 90.0
COMMENT_CAP = 1000

CALIBRATION_RECORD_VERSION = "audience-calibration-v1"
CALIBRATION_RECORD_PATH = PROJECT_ROOT / "config" / "audience_calibration_v1.json"
CALIBRATION_PLATFORMS = ("douyin", "xiaohongshu")
MIN_CALIBRATION_SAMPLE = 500

_CLASSIFIER_STATES = {"approved", "conservative", "rejected"}
_STATE_RANK = {"approved": 0, "conservative": 1, "rejected": 2}


def _slice_user_universe(
    connection: sqlite3.Connection, content_ids: Sequence[int]
) -> Dict[str, Any]:
    """Distinct in-slice platform users and their automotive subset."""

    placeholders = ",".join("?" for _ in content_ids)
    params = tuple(int(cid) for cid in content_ids)
    universe = connection.execute(
        f"""
        SELECT COUNT(DISTINCT c.interaction_user_id) AS u
        FROM comments c
        JOIN comment_evidence_versions cev ON cev.id=c.evidence_version_id
        WHERE cev.content_id IN ({placeholders})
          AND c.parent_comment_id IS NULL
          AND c.interaction_user_id IS NOT NULL
        """,
        params,
    ).fetchone()
    total_users = int(universe["u"] or 0)

    automotive = connection.execute(
        f"""
        SELECT COUNT(DISTINCT c.interaction_user_id) AS a
        FROM comments c
        JOIN comment_evidence_versions cev ON cev.id=c.evidence_version_id
        JOIN interaction_user_classification_versions cls
          ON cls.interaction_user_id=c.interaction_user_id
         AND cls.audience_definition_version=?
         AND cls.classifier_version=?
         AND cls.label='automotive'
        WHERE cev.content_id IN ({placeholders})
          AND c.parent_comment_id IS NULL
          AND c.interaction_user_id IS NOT NULL
        """,
        (AUDIENCE_DEFINITION_VERSION, CLASSIFIER_VERSION, *params),
    ).fetchone()
    automotive_users = int(automotive["a"] or 0)

    # Identity coverage: valid L1 comments with a stable platform pseudonym
    # over all valid L1 comments.
    coverage = connection.execute(
        f"""
        SELECT
            SUM(CASE WHEN c.interaction_user_id IS NOT NULL THEN 1 ELSE 0 END) AS stable,
            COUNT(*) AS total
        FROM comments c
        JOIN comment_evidence_versions cev ON cev.id=c.evidence_version_id
        WHERE cev.content_id IN ({placeholders})
          AND c.parent_comment_id IS NULL
        """,
        params,
    ).fetchone()
    stable = int(coverage["stable"] or 0)
    total_comments = int(coverage["total"] or 0)
    identity_coverage = (
        round(stable * 100 / total_comments, 2) if total_comments else None
    )
    return {
        "total_users": total_users,
        "automotive_users": automotive_users,
        "identity_coverage_percentage": identity_coverage,
    }


def _slice_comment_coverage(
    connection: sqlite3.Connection, content_ids: Sequence[int]
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
    placeholders = ",".join("?" for _ in content_ids)
    rows = connection.execute(
        f"""
        SELECT content_id, status, completion_kind, declared_total_count,
               captured_distinct_count, comment_cap
        FROM comment_capture_runs
        WHERE content_id IN ({placeholders})
        """,
        tuple(int(cid) for cid in content_ids),
    ).fetchall()
    captured_sum = 0
    denom_sum = 0
    capped = 0
    unknown = False
    for row in rows:
        captured = int(row["captured_distinct_count"] or 0)
        declared = row["declared_total_count"]
        completion = row["completion_kind"]
        captured_sum += captured
        if completion == "cap_reached":
            capped += 1
        if declared is not None:
            denom_sum += max(int(declared), captured)
        elif completion in {"provider_exhausted", "zero_comments", "cap_reached"}:
            # Declared unknown but the provider is exhausted (or capped): treat
            # the captured count as the inferred total.
            denom_sum += captured
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


def _decide_status(
    *,
    publication_count: int,
    total_users: int,
    identity_coverage: Optional[float],
    comment_coverage: Optional[float],
    classifier_state: str,
) -> str:
    if publication_count == 0:
        return "not_applicable"
    if total_users == 0:
        return "missing"
    if (
        classifier_state == "rejected"
        or identity_coverage is None
        or identity_coverage < IDENTITY_COVERAGE_GATE
        or total_users < MIN_USERS_SAMPLE
    ):
        return "below_threshold"
    if (
        classifier_state == "conservative"
        or total_users < MIN_USERS_AVAILABLE
        or comment_coverage is None
        or comment_coverage < COMMENT_COVERAGE_GATE
    ):
        return "sample_only"
    return "available"


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
        universe = _slice_user_universe(connection, unique_ids)
        coverage = _slice_comment_coverage(connection, unique_ids)
    else:
        universe = {
            "total_users": 0,
            "automotive_users": 0,
            "identity_coverage_percentage": None,
        }
        coverage = {
            "captured": 0,
            "declared": 0,
            "coverage_percentage": None,
            "capped_content_count": 0,
        }
    total_users = int(universe["total_users"])
    automotive_users = int(universe["automotive_users"])
    identity_coverage = universe["identity_coverage_percentage"]
    comment_coverage = coverage["coverage_percentage"]

    status = _decide_status(
        publication_count=publication_count,
        total_users=total_users,
        identity_coverage=identity_coverage,
        comment_coverage=comment_coverage,
        classifier_state=classifier_state,
    )
    publishes_percentage = status in {"available", "sample_only"}
    reason = _reason_for(status, total_users, identity_coverage, comment_coverage)
    metric = ratio_metric(
        automotive_users if publishes_percentage else None,
        total_users,
        status=status,
        eligible_count=total_users,
        coverage_percentage=comment_coverage,
        reason=reason,
    )
    audience_quality = {
        "captured_comment_count": int(coverage["captured"]),
        "declared_comment_count": int(coverage["declared"]),
        "comment_collection_coverage_percentage": comment_coverage,
        "identity_coverage_percentage": identity_coverage,
        "capped_content_count": int(coverage["capped_content_count"]),
        "audience_definition_version": AUDIENCE_DEFINITION_VERSION,
        "classifier_version": CLASSIFIER_VERSION,
        "user_key_version": "platform-user-hmac-v2",
        "evidence_window_start": evidence_window_start,
        "evidence_window_end": evidence_window_end,
        "report_cutoff_at": report_cutoff_at,
        "warm_up": bool(warm_up),
    }
    return {"metric": metric, "audience_quality": audience_quality}


def _reason_for(
    status: str,
    total_users: int,
    identity_coverage: Optional[float],
    comment_coverage: Optional[float],
) -> str:
    if status == "not_applicable":
        return "该切片在所选窗口没有发布内容"
    if status == "missing":
        return "有内容但没有可识别的有效评论用户"
    if status == "below_threshold":
        if identity_coverage is None or identity_coverage < IDENTITY_COVERAGE_GATE:
            return (
                f"用户身份覆盖率 {identity_coverage if identity_coverage is not None else '未知'}%"
                f"，低于 {IDENTITY_COVERAGE_GATE:.0f}% 门槛"
            )
        if total_users < MIN_USERS_SAMPLE:
            return f"去重有效用户 {total_users} 人，低于 {MIN_USERS_SAMPLE} 人门槛"
        return "分类器未通过定标，暂不发布比例"
    if status == "sample_only":
        if comment_coverage is None:
            return "评论采集覆盖率未知，仅作样本"
        if comment_coverage < COMMENT_COVERAGE_GATE:
            return f"评论采集覆盖率 {comment_coverage}%，低于 90%，仅作样本"
        if total_users < MIN_USERS_AVAILABLE:
            return f"去重有效用户 {total_users} 人，处于 30–99 样本区间"
        return "分类器为保守识别，仅作样本"
    return ""


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
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {
            "state": "rejected",
            "platforms": {},
            "reasons": ["定标记录缺失或不可解析"],
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

    Resolves through :func:`load_calibration_record`: without a valid
    500-user/platform gold-set calibration record the state stays
    ``rejected``, so ``automotive_user_rate`` never publishes a percentage
    before calibration. The effective state is the weakest platform state
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
