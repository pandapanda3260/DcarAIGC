"""Deterministic, auditable automotive-interest user classifier (v1).

The classifier assigns each platform-level interaction user one of three
labels within a fixed 90-day evidence window:

* ``automotive`` — a high-precision positive; either one *strong* evidence
  comment (owner experience, buy/sell, quote, test drive, maintenance, a
  concrete model/config or a technical problem) OR at least two *substantive*
  automotive-interest comments spread across two different contents.
* ``not_identified`` — evidence is insufficient. This is NOT a claim the user
  is non-automotive; the user stays in the denominator.
* ``excluded`` — author replies, empty/pure-emoji/spam bodies, or a comment
  with no stable platform identity. Excluded users never enter the universe.

Design constraints (from the v8.4 audience definition):

* Generic phrases ("好看", "哈哈", "多少钱") never qualify a user on their own.
* Content context (title/body/ASR/OCR) is *explicit* evidence attached to each
  comment, never a blanket ``context_automotive=True``.
* The frozen 0/30/70/100 rules are reused only as candidate features, never
  turned directly into user labels.
* Re-running the same evidence under the same classifier version is idempotent.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .storage import now_utc, transaction


AUDIENCE_DEFINITION_VERSION = "audience-definition-v1"
CLASSIFIER_VERSION = "audience-classifier-v1"
EVIDENCE_WINDOW_DAYS = 90
SUBSTANTIVE_MIN_COMMENTS = 2
SUBSTANTIVE_MIN_CONTENTS = 2

_LABELS = ("automotive", "not_identified", "excluded")

# Reused frozen feature detectors (candidate features only).
_scoring = importlib.import_module("analyze_douyin_tikhub_v6")
STRONG_OWNER_OR_TRANSACTION = _scoring.OWNER_OR_TRANSACTION
TECHNICAL_AUTO = _scoring.TECHNICAL_AUTO  # broad: models AND generic spec attributes
GENERIC_AUTO = _scoring.GENERIC_AUTO
PERSONAL_AUTO_ACTION = _scoring.PERSONAL_AUTO_ACTION
HIGH_ACTION = _scoring.HIGH_ACTION
INFO_ACTION = _scoring.INFO_ACTION

# A concrete vehicle identity (brand/model) is strong on its own; a generic
# spec attribute ("油耗", "空间") from TECHNICAL_AUTO is only substantive.
STRONG_MODEL = re.compile(
    r"(?:丰田|本田|大众|宝马|奔驰|奥迪|比亚迪|吉利|奇瑞|长安|五菱|特斯拉|理想|蔚来|小鹏|"
    r"问界|小米|红旗|广汽|传祺|凯迪拉克|雷克萨斯|路虎|保时捷|卡罗拉|轩逸|秦L|锋兰达|"
    r"雅阁|宝马3|A6L|A4L|CR-?V|H6|C200L?|E300|X5|Q7|CT5|途观|汉兰达|凯美瑞|迈腾|"
    r"科鲁兹|君威|锐界|哈弗|帕杰罗|天逸|雪铁龙|铃木|海豹|瑞虎|飞度|奥拓|速腾|森林人|"
    r"桑塔纳|凌志|极氪|零跑|鸿蒙智行|SU7|YU7|DM-?i|DM-?p)"
)
# Explicit ownership/technical-problem phrasing is strong.
STRONG_TECH_PROBLEM = re.compile(
    r"(?:异响|顿挫|抖动|烧机油|趴窝|亏电|自燃|漏油|故障|趴窝|生锈|失灵|召回|通病|"
    r"变速箱.{0,4}(?:坏|修|问题)|发动机.{0,4}(?:坏|修|问题)|"
    r"刹车.{0,4}(?:软|失灵|问题)|电池.{0,4}(?:衰减|亏|坏))"
)

# Pure-generic phrases that must never qualify a user on their own.
_GENERIC_ONLY = re.compile(
    r"^(?:哈+|呵+|好+看?|美+|漂亮|不错|可以|支持|加油|沙发|前排|路过|"
    r"多少钱|价格|贵吗|便宜吗|真好|好棒|棒棒|厉害|牛|赞|顶|收到|谢谢|"
    r"[!！。，,.\s~]*)$"
)
_EMOJI_OR_PUNCT = re.compile(
    r"^[\s\W_]*$|^(?:\[[^\]]{1,8}\])+$"
)
_SPAM = re.compile(
    r"(?:加(?:我)?(?:微信|vx|VX|威信|扣扣|qq|QQ)|"
    r"代(?:办|开|刷)|优惠券|领取|私(?:信|我)领|点击.{0,4}链接|"
    r"www\.|https?://|包(?:邮|退)|批发|一手货源)"
)


@dataclass
class CommentEvidence:
    """One comment attributed to a platform user inside the window."""

    content_id: int
    body: str
    published_at: Optional[str]
    is_author_reply: bool = False
    context_automotive: bool = False  # explicit: content is automotive AND relevant


@dataclass
class UserEvidence:
    interaction_user_id: int
    platform: str
    comments: Sequence[CommentEvidence]


@dataclass
class Classification:
    label: str
    confidence: Optional[float]
    reasons: List[str] = field(default_factory=list)
    evidence_sha256: str = ""

    def as_reason_json(self) -> str:
        return json.dumps(
            {"reasons": self.reasons},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )


def _is_excluded_body(body: str) -> bool:
    text = body.strip()
    if not text:
        return True
    if _EMOJI_OR_PUNCT.match(text):
        return True
    if _SPAM.search(text):
        return True
    return False


def _is_strong(comment: CommentEvidence) -> bool:
    text = comment.body
    if STRONG_OWNER_OR_TRANSACTION.search(text):
        return True
    if PERSONAL_AUTO_ACTION.search(text) and (
        TECHNICAL_AUTO.search(text) or GENERIC_AUTO.search(text)
    ):
        return True
    # A concrete vehicle identity or an explicit technical problem is strong;
    # a bare generic spec attribute ("油耗", "空间") is not.
    if STRONG_MODEL.search(text) or STRONG_TECH_PROBLEM.search(text):
        return True
    # A high-intent action only counts as strong when the content context is
    # explicitly automotive (not a blanket assumption).
    if comment.context_automotive and HIGH_ACTION.search(text):
        return True
    return False


def _is_substantive(comment: CommentEvidence) -> bool:
    """Real automotive interest, but weaker than a strong signal."""

    text = comment.body
    if _GENERIC_ONLY.match(text.strip()):
        return False
    if TECHNICAL_AUTO.search(text) or STRONG_OWNER_OR_TRANSACTION.search(text):
        return True
    if GENERIC_AUTO.search(text) and (
        INFO_ACTION.search(text) or HIGH_ACTION.search(text)
    ):
        return True
    if comment.context_automotive and (
        INFO_ACTION.search(text) or HIGH_ACTION.search(text)
    ):
        return True
    return False


def _evidence_sha256(user: UserEvidence) -> str:
    payload = {
        "audience_definition_version": AUDIENCE_DEFINITION_VERSION,
        "classifier_version": CLASSIFIER_VERSION,
        "interaction_user_id": user.interaction_user_id,
        "platform": user.platform,
        "comments": sorted(
            (
                int(comment.content_id),
                str(comment.published_at or ""),
                comment.body,
                bool(comment.is_author_reply),
                bool(comment.context_automotive),
            )
            for comment in user.comments
        ),
    }
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def classify_user(user: UserEvidence) -> Classification:
    evidence_sha = _evidence_sha256(user)
    usable = [
        comment
        for comment in user.comments
        if not comment.is_author_reply and not _is_excluded_body(comment.body)
    ]
    if not usable:
        return Classification(
            label="excluded",
            confidence=None,
            reasons=["no_usable_comment_after_filtering"],
            evidence_sha256=evidence_sha,
        )

    strong = [c for c in usable if _is_strong(c)]
    if strong:
        return Classification(
            label="automotive",
            confidence=0.98,
            reasons=[
                "strong_evidence",
                *sorted({f"content:{c.content_id}" for c in strong}),
            ],
            evidence_sha256=evidence_sha,
        )

    substantive = [c for c in usable if _is_substantive(c)]
    distinct_contents = {c.content_id for c in substantive}
    if (
        len(substantive) >= SUBSTANTIVE_MIN_COMMENTS
        and len(distinct_contents) >= SUBSTANTIVE_MIN_CONTENTS
    ):
        return Classification(
            label="automotive",
            confidence=0.9,
            reasons=[
                "two_substantive_comments_across_contents",
                *sorted({f"content:{c}" for c in distinct_contents}),
            ],
            evidence_sha256=evidence_sha,
        )

    return Classification(
        label="not_identified",
        confidence=None,
        reasons=(
            ["insufficient_substantive_evidence"]
            if substantive
            else ["no_automotive_signal"]
        ),
        evidence_sha256=evidence_sha,
    )


def _window_bounds(evidence_window_end: str) -> tuple[str, str]:
    end = datetime.fromisoformat(evidence_window_end.replace("Z", "+00:00"))
    start = end - timedelta(days=EVIDENCE_WINDOW_DAYS)
    return start.isoformat(), end.isoformat()


def _within_window(published_at: Optional[str], start: str, end: str) -> bool:
    if not published_at:
        # No behavior time: usable as in-window interaction but cannot back a
        # cross-content 90-day argument. Callers gate that separately.
        return True
    try:
        ts = datetime.fromisoformat(str(published_at).replace("Z", "+00:00"))
    except ValueError:
        return True
    return (
        datetime.fromisoformat(start) <= ts < datetime.fromisoformat(end)
    )


def gather_user_evidence(
    connection: sqlite3.Connection,
    *,
    content_ids: Sequence[int],
    evidence_window_end: str,
) -> List[UserEvidence]:
    """Aggregate in-window comment evidence per platform interaction user."""

    if not content_ids:
        return []
    start, end = _window_bounds(evidence_window_end)
    placeholders = ",".join("?" for _ in content_ids)
    rows = connection.execute(
        f"""
        SELECT c.interaction_user_id, iu.platform, cev.content_id,
               c.body, c.published_at, c.parent_comment_id
        FROM comments c
        JOIN comment_evidence_versions cev ON cev.id=c.evidence_version_id
        JOIN interaction_users iu ON iu.id=c.interaction_user_id
        WHERE cev.content_id IN ({placeholders})
          AND c.interaction_user_id IS NOT NULL
          AND c.parent_comment_id IS NULL
        """,
        tuple(int(cid) for cid in content_ids),
    ).fetchall()

    directions = _content_context(connection, content_ids)
    grouped: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        user_id = int(row["interaction_user_id"])
        entry = grouped.setdefault(
            user_id, {"platform": str(row["platform"]), "comments": []}
        )
        entry["comments"].append(
            CommentEvidence(
                content_id=int(row["content_id"]),
                body=str(row["body"] or ""),
                published_at=row["published_at"],
                is_author_reply=False,
                context_automotive=directions.get(int(row["content_id"]), False),
            )
        )
    result: List[UserEvidence] = []
    for user_id, entry in grouped.items():
        in_window = [
            comment
            for comment in entry["comments"]
            if _within_window(comment.published_at, start, end)
        ]
        if in_window:
            result.append(
                UserEvidence(
                    interaction_user_id=user_id,
                    platform=entry["platform"],
                    comments=in_window,
                )
            )
    return result


def _content_context(
    connection: sqlite3.Connection, content_ids: Sequence[int]
) -> Dict[int, bool]:
    """Explicit automotive context per content, from the current evaluation."""

    placeholders = ",".join("?" for _ in content_ids)
    rows = connection.execute(
        f"""
        SELECT content_id, content_automotive_score
        FROM evaluation_versions
        WHERE content_id IN ({placeholders}) AND invalidated_at IS NULL
        """,
        tuple(int(cid) for cid in content_ids),
    ).fetchall()
    context: Dict[int, bool] = {}
    for row in rows:
        score = row["content_automotive_score"]
        if score is not None:
            context[int(row["content_id"])] = int(score) >= 60
    return context


def classify_window(
    connection: sqlite3.Connection,
    *,
    content_ids: Sequence[int],
    evidence_window_start: str,
    evidence_window_end: str,
    persist: bool = True,
) -> Dict[str, Any]:
    """Classify every in-window user and append idempotent version rows."""

    users = gather_user_evidence(
        connection,
        content_ids=content_ids,
        evidence_window_end=evidence_window_end,
    )
    counts = {label: 0 for label in _LABELS}
    classifications: List[tuple[int, Classification]] = []
    for user in users:
        classification = classify_user(user)
        counts[classification.label] += 1
        classifications.append((user.interaction_user_id, classification))

    if persist:
        created_at = now_utc()
        with transaction(connection):
            for user_id, classification in classifications:
                connection.execute(
                    """
                    INSERT INTO interaction_user_classification_versions(
                        interaction_user_id, audience_definition_version,
                        classifier_version, evidence_window_start, evidence_window_end,
                        evidence_sha256, label, confidence, reason_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(
                        interaction_user_id, audience_definition_version,
                        classifier_version, evidence_sha256
                    ) DO NOTHING
                    """,
                    (
                        user_id,
                        AUDIENCE_DEFINITION_VERSION,
                        CLASSIFIER_VERSION,
                        evidence_window_start,
                        evidence_window_end,
                        classification.evidence_sha256,
                        classification.label,
                        classification.confidence,
                        classification.as_reason_json(),
                        created_at,
                    ),
                )
    return {
        "classifier_version": CLASSIFIER_VERSION,
        "audience_definition_version": AUDIENCE_DEFINITION_VERSION,
        "evidence_window_start": evidence_window_start,
        "evidence_window_end": evidence_window_end,
        "total_users": len(users),
        "label_counts": counts,
    }


# --------------------------------------------------------------------------
# Calibration framework
# --------------------------------------------------------------------------


@dataclass
class CalibrationResult:
    precision: Optional[float]
    recall: Optional[float]
    true_positive: int
    false_positive: int
    false_negative: int
    true_negative: int
    state: str
    classifier_version: str = CLASSIFIER_VERSION

    def as_dict(self) -> Dict[str, Any]:
        return {
            "classifier_version": self.classifier_version,
            "precision": self.precision,
            "recall": self.recall,
            "confusion": {
                "true_positive": self.true_positive,
                "false_positive": self.false_positive,
                "false_negative": self.false_negative,
                "true_negative": self.true_negative,
            },
            "state": self.state,
        }


PRECISION_GATE = 0.95
RECALL_GATE = 0.80


def state_from_counts(
    true_positive: int, false_positive: int, false_negative: int
) -> str:
    """Fixed-gate calibration state from confusion counts.

    Precision is the hard gate (mislabeling a non-automotive user as
    automotive is the costly error); recall only splits approved vs
    conservative and is never traded against the precision bar.
    """

    positive_predictions = true_positive + false_positive
    precision = true_positive / positive_predictions if positive_predictions else None
    positive_truths = true_positive + false_negative
    recall = true_positive / positive_truths if positive_truths else None
    if precision is None or precision < PRECISION_GATE:
        return "rejected"
    if recall is not None and recall >= RECALL_GATE:
        return "approved"
    return "conservative"


def evaluate_calibration(
    predictions: Mapping[Any, str],
    gold: Mapping[Any, str],
) -> CalibrationResult:
    """Score predictions against a human gold set on the ``automotive`` positive.

    ``precision`` is the hard gate — mislabeling a non-automotive user as
    automotive is the costly error. ``recall`` decides approved vs conservative;
    it is never traded away by lowering the precision bar.
    """

    tp = fp = fn = tn = 0
    for key, truth in gold.items():
        predicted = predictions.get(key, "not_identified")
        pred_pos = predicted == "automotive"
        truth_pos = truth == "automotive"
        if pred_pos and truth_pos:
            tp += 1
        elif pred_pos and not truth_pos:
            fp += 1
        elif not pred_pos and truth_pos:
            fn += 1
        else:
            tn += 1
    precision = tp / (tp + fp) if (tp + fp) else None
    recall = tp / (tp + fn) if (tp + fn) else None
    return CalibrationResult(
        precision=precision,
        recall=recall,
        true_positive=tp,
        false_positive=fp,
        false_negative=fn,
        true_negative=tn,
        state=state_from_counts(tp, fp, fn),
    )
