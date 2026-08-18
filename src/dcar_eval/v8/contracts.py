"""DCar Insight v8 report metrics and contract validation."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from .storage import PROJECT_ROOT


CONTRACT_PATH = PROJECT_ROOT / "config" / "report_contract_v8_7.json"
LEGACY_CONTRACT_PATHS = {
    "dcar-content-operations-report-v8.2": (
        PROJECT_ROOT / "config" / "report_contract_v8_2.json"
    ),
    # Every published v8.3 revision was generated under evaluation-v7 /
    # evidence-v1 (the short-lived v8.3+evaluation-v8 live contract never
    # produced a revision), so the frozen v8.3 file pins those versions.
    "dcar-content-operations-report-v8.3": (
        PROJECT_ROOT / "config" / "report_contract_v8_3.json"
    ),
    "dcar-content-operations-report-v8.4": (
        PROJECT_ROOT / "config" / "report_contract_v8_4.json"
    ),
    "dcar-content-operations-report-v8.5": (
        PROJECT_ROOT / "config" / "report_contract_v8_5.json"
    ),
    # v8.6 是最后一个含 review_summary 必备键的契约；v8.7 起系统无人工复核
    # 域，报告不再携带该段。历史 v8.6 修订版仍按冻结契约校验。
    "dcar-content-operations-report-v8.6": (
        PROJECT_ROOT / "config" / "report_contract_v8_6.json"
    ),
}
CURRENT_REPORT_VERSION = "dcar-content-operations-report-v8.7"
CURRENT_REPORT_RULE_VERSION = "evaluation-v9"
CURRENT_REPORT_EVIDENCE_VERSION = "evidence-v2"
REPORT_RULE_VERSIONS = {
    CURRENT_REPORT_VERSION: CURRENT_REPORT_RULE_VERSION,
    "dcar-content-operations-report-v8.6": "evaluation-v9",
    "dcar-content-operations-report-v8.5": "evaluation-v8",
    "dcar-content-operations-report-v8.4": "evaluation-v8",
    "dcar-content-operations-report-v8.3": "evaluation-v7",
    "dcar-content-operations-report-v8.2": "evaluation-v6",
}


class V8ContractViolation(ValueError):
    """Raised when a v8 operational report violates the frozen contract."""


def load_contract(
    path: Optional[Path] = None,
    *,
    report_version: Optional[str] = None,
) -> Dict[str, Any]:
    if path is not None:
        selected = path
    elif not report_version or report_version == CURRENT_REPORT_VERSION:
        selected = CONTRACT_PATH
    else:
        try:
            selected = LEGACY_CONTRACT_PATHS[report_version]
        except KeyError as exc:
            raise V8ContractViolation(
                f"report_version {report_version!r} has no registered contract"
            ) from exc
    return json.loads(selected.read_text(encoding="utf-8"))


def quantity_metric(
    value: Optional[float],
    *,
    unit: str,
    status: str,
    coverage_percentage: Optional[float] = None,
    reason: str = "",
) -> Dict[str, Any]:
    return {
        "kind": "quantity",
        "value": value,
        "unit": unit,
        "status": status,
        "coverage_percentage": coverage_percentage,
        "reason": reason,
    }


def ratio_metric(
    numerator: Optional[int],
    denominator: int,
    *,
    status: str,
    eligible_count: Optional[int] = None,
    coverage_percentage: Optional[float] = None,
    reason: str = "",
) -> Dict[str, Any]:
    percentage = None
    if (
        status in {"available", "sample_only"}
        and numerator is not None
        and denominator > 0
    ):
        percentage = round(numerator * 100 / denominator, 2)
    return {
        "kind": "ratio",
        "numerator": numerator,
        "denominator": denominator,
        "percentage": percentage,
        "unit": "percent",
        "status": status,
        "eligible_count": eligible_count,
        "coverage_percentage": coverage_percentage,
        "reason": reason,
    }


def score_metric(
    value: Optional[float],
    *,
    status: str,
    scorable_items: int,
    total_items: int,
    reason: str = "",
) -> Dict[str, Any]:
    coverage = round(scorable_items * 100 / total_items, 2) if total_items else None
    return {
        "kind": "score",
        "value": value,
        "unit": "point",
        "scale": 100,
        "status": status,
        "scorable_items": scorable_items,
        "total_items": total_items,
        "coverage_percentage": coverage,
        "reason": reason,
    }


def expected_terminal_task_status(
    data_quality: Mapping[str, Any],
    *,
    data_quality_details: Optional[Mapping[str, Any]] = None,
    contract: Optional[Mapping[str, Any]] = None,
    enforce_boolean_quality_gates: bool = True,
) -> str:
    return (
        "partial"
        if quality_gate_failures(
            data_quality,
            data_quality_details=data_quality_details,
            contract=contract,
            enforce_boolean_quality_gates=enforce_boolean_quality_gates,
        )
        else "succeeded"
    )


def quality_gate_failures(
    data_quality: Mapping[str, Any],
    *,
    data_quality_details: Optional[Mapping[str, Any]] = None,
    contract: Optional[Mapping[str, Any]] = None,
    enforce_boolean_quality_gates: bool = True,
) -> List[Dict[str, Any]]:
    """Return every unmet quality prerequisite from the selected contract."""

    active_contract = contract or load_contract()
    failures: List[Dict[str, Any]] = []
    if enforce_boolean_quality_gates:
        for key, required in active_contract.get(
            "required_boolean_quality_gates", {}
        ).items():
            if data_quality.get(key) is not required:
                failures.append(
                    {
                        "key": key,
                        "kind": "boolean",
                        "actual": data_quality.get(key),
                        "required": required,
                    }
                )
    thresholds = active_contract["required_coverage_thresholds"]
    for key, minimum in thresholds.items():
        try:
            actual = float(data_quality[key])
        except (KeyError, TypeError, ValueError):
            failures.append(
                {
                    "key": key,
                    "kind": "coverage",
                    "actual": data_quality.get(key),
                    "required": float(minimum),
                }
            )
            continue
        if actual < float(minimum):
            failures.append(
                {
                    "key": key,
                    "kind": "coverage",
                    "actual": actual,
                    "required": float(minimum),
                }
            )
    details = data_quality_details or {}
    for key, spec in active_contract.get("required_quality_details", {}).items():
        detail = details.get(key) if isinstance(details, Mapping) else None
        minimum = float(spec["minimum_percentage"])
        status = detail.get("status") if isinstance(detail, Mapping) else None
        if status == "not_applicable" and bool(
            spec.get("allow_not_applicable_when_empty")
        ):
            empty_detail = False
            if isinstance(detail, Mapping) and data_quality.get(key) is None:
                if key == "discovery_coverage":
                    empty_detail = (
                        detail.get("covered_identity_occurrence_count") == 0
                        and detail.get("eligible_identity_occurrence_count") == 0
                        and detail.get("percentage") is None
                    )
                elif key == "metrics_freshness":
                    empty_detail = (
                        detail.get("fresh_count") == 0
                        and detail.get("as_of_snapshot_count") == 0
                        and detail.get("eligible_count") == 0
                        and detail.get("percentage") is None
                    )
            if empty_detail:
                continue
        detail_actual = (
            detail.get("percentage") if isinstance(detail, Mapping) else None
        )
        if (
            isinstance(detail_actual, bool)
            or not isinstance(detail_actual, (int, float))
            or not 0 <= float(detail_actual) <= 100
            or float(detail_actual) < minimum
        ):
            failures.append(
                {
                    "key": key,
                    "kind": "coverage",
                    "actual": detail_actual,
                    "required": minimum,
                }
            )
    return failures


def _walk_metric_statuses(value: Any, path: str = "$") -> List[str]:
    invalid: List[str] = []
    if isinstance(value, Mapping):
        if "status" in value and value.get("kind") in {"quantity", "ratio", "score"}:
            if value["status"] == "partial":
                invalid.append(f"{path}.status")
        for key, child in value.items():
            invalid.extend(_walk_metric_statuses(child, f"{path}.{key}"))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            invalid.extend(_walk_metric_statuses(child, f"{path}[{index}]"))
    return invalid


def _validate_metric(
    metric: Any,
    expected: Mapping[str, str],
    path: str,
    errors: List[str],
    contract: Mapping[str, Any],
) -> None:
    if not isinstance(metric, Mapping):
        errors.append(f"{path} must be an object")
        return
    if metric.get("kind") != expected["kind"]:
        errors.append(f"{path}.kind must be {expected['kind']}")
    if metric.get("unit") != expected["unit"]:
        errors.append(f"{path}.unit must be {expected['unit']}")
    status = metric.get("status")
    if status not in contract["metric_statuses"]:
        errors.append(f"{path}.status is not a valid metric status")
    if status == "partial":
        errors.append(f"{path}.status must use below_threshold, not partial")

    kind = expected["kind"]
    if kind == "quantity":
        required = {"value", "unit", "status", "coverage_percentage", "reason"}
        if expected["unit"] not in contract["quantity_units"]:
            errors.append(f"{path}.unit is not an allowed quantity unit")
        if (
            status in {"not_calculable", "not_applicable", "missing"}
            and metric.get("value") is not None
        ):
            errors.append(f"{path}.value must be null for status {status}")
    elif kind == "ratio":
        required = {
            "numerator",
            "denominator",
            "percentage",
            "unit",
            "status",
            "eligible_count",
            "coverage_percentage",
            "reason",
        }
        denominator = metric.get("denominator")
        numerator = metric.get("numerator")
        if not isinstance(denominator, int) or denominator < 0:
            errors.append(f"{path}.denominator must be a non-negative integer")
        if numerator is not None and (not isinstance(numerator, int) or numerator < 0):
            errors.append(f"{path}.numerator must be null or a non-negative integer")
        if (
            isinstance(numerator, int)
            and isinstance(denominator, int)
            and numerator > denominator
        ):
            errors.append(f"{path}.numerator cannot exceed denominator")
        if (
            status in {"not_calculable", "not_applicable", "missing"}
            and metric.get("percentage") is not None
        ):
            errors.append(f"{path}.percentage must be null for status {status}")
    else:
        required = {
            "value",
            "unit",
            "scale",
            "status",
            "scorable_items",
            "total_items",
            "coverage_percentage",
            "reason",
        }
        if metric.get("scale") != 100:
            errors.append(f"{path}.scale must equal 100")
    missing = sorted(required - set(metric.keys()))
    if missing:
        errors.append(f"{path} missing {missing}")


def _validate_summary_quality_alignment(
    summary: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    publication_value: Any,
    contract: Mapping[str, Any],
    errors: List[str],
) -> None:
    """Keep v8.6 summary badges aligned with the declared quality coverage."""

    display_thresholds = contract.get("metric_display_coverage_thresholds")
    if not isinstance(display_thresholds, Mapping):
        return
    required_thresholds = contract.get("required_coverage_thresholds", {})
    if not isinstance(required_thresholds, Mapping):
        return

    def coverage_value(metric_name: str) -> Optional[float]:
        metric = summary.get(metric_name)
        if not isinstance(metric, Mapping):
            return None
        value = metric.get("coverage_percentage")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return float(value)

    def validate_display_threshold(metric_name: str, minimum: float) -> None:
        metric = summary.get(metric_name)
        if not isinstance(metric, Mapping):
            return
        status = metric.get("status")
        coverage = coverage_value(metric_name)
        path = f"$.summary_metrics.{metric_name}"
        if status == "sample_only":
            errors.append(f"{path}.status cannot bypass the coverage threshold")
            return
        if status not in {"available", "below_threshold"}:
            return
        if coverage is None:
            errors.append(
                f"{path}.coverage_percentage is required for status {status}"
            )
            return
        expected = "available" if coverage >= minimum else "below_threshold"
        if status != expected:
            errors.append(
                f"{path}.status must be {expected} for coverage {coverage:g}% "
                f"and threshold {minimum:g}%"
            )
        if status == "below_threshold" and metric.get("percentage") is not None:
            errors.append(f"{path}.percentage must be null below threshold")

    validate_display_threshold(
        "view_count", float(display_thresholds["view_count"])
    )
    validate_display_threshold(
        "comment_count", float(display_thresholds["comment_count"])
    )

    if (
        isinstance(publication_value, bool)
        or not isinstance(publication_value, (int, float))
        or publication_value <= 0
    ):
        return

    evaluation_coverage = data_quality.get("evaluation_coverage")
    if isinstance(evaluation_coverage, (int, float)) and not isinstance(
        evaluation_coverage, bool
    ):
        evaluation_minimum = float(required_thresholds["evaluation_coverage"])
        for metric_name in ("verticality_rate", "selling_point_coverage_rate"):
            metric = summary.get(metric_name)
            if not isinstance(metric, Mapping):
                continue
            path = f"$.summary_metrics.{metric_name}"
            metric_coverage = coverage_value(metric_name)
            if metric_coverage != float(evaluation_coverage):
                errors.append(
                    f"{path}.coverage_percentage must equal "
                    "$.data_quality.evaluation_coverage"
                )
            expected = (
                "available"
                if float(evaluation_coverage) >= evaluation_minimum
                else "below_threshold"
            )
            if metric.get("status") != expected:
                errors.append(
                    f"{path}.status must be {expected} for evaluation coverage"
                )
            if expected == "below_threshold" and metric.get("percentage") is not None:
                errors.append(f"{path}.percentage must be null below threshold")

    fingerprint_coverage = data_quality.get("duplicate_fingerprint_coverage")
    calibration_ready = data_quality.get("duplicate_calibration_ready")
    duplicate = summary.get("duplicate_rate")
    if (
        isinstance(duplicate, Mapping)
        and isinstance(fingerprint_coverage, (int, float))
        and not isinstance(fingerprint_coverage, bool)
    ):
        path = "$.summary_metrics.duplicate_rate"
        duplicate_coverage = coverage_value("duplicate_rate")
        if duplicate_coverage != float(fingerprint_coverage):
            errors.append(
                f"{path}.coverage_percentage must equal "
                "$.data_quality.duplicate_fingerprint_coverage"
            )
        if calibration_ready is False:
            if duplicate.get("status") != "not_calculable":
                errors.append(
                    f"{path}.status must be not_calculable when calibration is not ready"
                )
            if duplicate.get("percentage") is not None:
                errors.append(
                    f"{path}.percentage must be null when calibration is not ready"
                )
            if not str(duplicate.get("reason") or "").strip():
                errors.append(
                    f"{path}.reason is required when calibration is not ready"
                )
        elif calibration_ready is True:
            fingerprint_minimum = float(
                required_thresholds["duplicate_fingerprint_coverage"]
            )
            expected = (
                "available"
                if float(fingerprint_coverage) >= fingerprint_minimum
                else "below_threshold"
            )
            if duplicate.get("status") != expected:
                errors.append(
                    f"{path}.status must be {expected} for fingerprint coverage"
                )
            if expected == "below_threshold" and duplicate.get("percentage") is not None:
                errors.append(f"{path}.percentage must be null below threshold")


def _validate_audience_quality(
    quality: Any,
    required_keys: List[str],
    path: str,
    errors: List[str],
) -> None:
    if not isinstance(quality, Mapping):
        errors.append(f"{path} must be an object")
        return
    missing = sorted(set(required_keys) - set(quality.keys()))
    if missing:
        errors.append(f"{path} missing {missing}")
    for key in (
        "comment_collection_coverage_percentage",
        "identity_coverage_percentage",
        "classification_coverage_percentage",
    ):
        if key in quality:
            value = quality.get(key)
            if value is not None and (
                not isinstance(value, (int, float)) or not 0 <= float(value) <= 100
            ):
                errors.append(f"{path}.{key} must be null or between 0 and 100")
    for key in ("candidate_user_count", "classified_user_count"):
        if key in quality:
            value = quality.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                errors.append(f"{path}.{key} must be a non-negative integer")
    if "warm_up" in quality and not isinstance(quality.get("warm_up"), bool):
        errors.append(f"{path}.warm_up must be a boolean")


def _validate_automotive_user_rate(
    metric: Any, path: str, errors: List[str]
) -> None:
    if not isinstance(metric, Mapping):
        return
    denominator = metric.get("denominator")
    eligible = metric.get("eligible_count")
    if not isinstance(eligible, int) or eligible != denominator:
        errors.append(f"{path}.eligible_count must equal denominator")
    coverage = metric.get("coverage_percentage")
    if coverage is not None and (
        not isinstance(coverage, (int, float)) or not 0 <= float(coverage) <= 100
    ):
        errors.append(f"{path}.coverage_percentage must be null or between 0 and 100")
    status = metric.get("status")
    percentage = metric.get("percentage")
    if (
        status in {"below_threshold", "not_applicable", "missing", "not_calculable"}
        and percentage is not None
    ):
        errors.append(f"{path}.percentage must be null for status {status}")
    if status in {"available", "sample_only"} and (
        percentage is None or metric.get("numerator") is None
    ):
        errors.append(
            f"{path} must publish numerator and percentage for status {status}"
        )


def _validate_channel_conclusions(
    channels: Any,
    contract: Mapping[str, Any],
    errors: List[str],
) -> None:
    spec = contract["channel_conclusion_metrics"]
    layout = contract.get("channels", {})
    platforms = [str(value) for value in layout.get("platforms", [])]
    scenes = [str(value) for value in layout.get("scenes", [])]
    required_quality = [
        str(value) for value in contract.get("audience_quality_required_keys", [])
    ]
    if not isinstance(channels, Mapping):
        errors.append("$.channels must be an object")
        return
    if sorted(channels.keys()) != sorted(platforms):
        errors.append(f"$.channels must contain exactly the platforms {platforms}")

    def validate_group(group: Any, path: str) -> None:
        if not isinstance(group, Mapping):
            errors.append(f"{path} must be an object")
            return
        for key in ("label", "publication_count", "audience_quality", "metrics"):
            if key not in group:
                errors.append(f"{path}.{key} is required")
        _validate_audience_quality(
            group.get("audience_quality"),
            required_quality,
            f"{path}.audience_quality",
            errors,
        )
        metrics = group.get("metrics")
        if not isinstance(metrics, Mapping):
            errors.append(f"{path}.metrics must be an object")
            return
        if sorted(metrics.keys()) != sorted(spec.keys()):
            errors.append(
                f"{path}.metrics must contain exactly the channel conclusion metrics"
            )
        for name, expected in spec.items():
            _validate_metric(
                metrics.get(name),
                expected,
                f"{path}.metrics.{name}",
                errors,
                contract,
            )
        _validate_automotive_user_rate(
            metrics.get("automotive_user_rate"),
            f"{path}.metrics.automotive_user_rate",
            errors,
        )

    for platform, block in channels.items():
        if platform not in platforms:
            continue
        path = f"$.channels.{platform}"
        if not isinstance(block, Mapping):
            errors.append(f"{path} must be an object")
            continue
        validate_group(block.get("summary"), f"{path}.summary")
        scenes_block = block.get("scenes")
        if not isinstance(scenes_block, Mapping):
            errors.append(f"{path}.scenes must be an object")
            continue
        if sorted(scenes_block.keys()) != sorted(scenes):
            errors.append(f"{path}.scenes must contain exactly the scenes {scenes}")
        for scene, group in scenes_block.items():
            if scene in scenes:
                validate_group(group, f"{path}.scenes.{scene}")


def _validate_utc_timestamp(
    value: Any, path: str, errors: List[str]
) -> Optional[datetime]:
    if not isinstance(value, str):
        errors.append(f"{path} must be an RFC3339 UTC timestamp")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        errors.append(f"{path} must be an RFC3339 UTC timestamp")
        return None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        errors.append(f"{path} must be an RFC3339 UTC timestamp")
        return None
    return parsed


def _validate_discovery_coverage_detail(
    detail: Any,
    scalar: Any,
    specification: Mapping[str, Any],
    path: str,
    errors: List[str],
) -> None:
    if not isinstance(detail, Mapping):
        errors.append(f"{path} must be an object")
        return
    required_fields = {
        "status",
        "covered_identity_occurrence_count",
        "eligible_identity_occurrence_count",
        "observed_occurrence_count",
        "expected_occurrence_count",
        "percentage",
        "eligible_basis",
        "reason",
    }
    missing = sorted(required_fields - set(detail.keys()))
    if missing:
        errors.append(f"{path} missing {missing}")

    status = detail.get("status")
    if status not in {"available", "below_threshold", "not_applicable"}:
        errors.append(f"{path}.status is invalid")

    count_fields = (
        "covered_identity_occurrence_count",
        "eligible_identity_occurrence_count",
        "observed_occurrence_count",
        "expected_occurrence_count",
    )
    counts: Dict[str, Optional[int]] = {}
    for field in count_fields:
        value = detail.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            errors.append(f"{path}.{field} must be a non-negative integer")
            counts[field] = None
        else:
            counts[field] = value
    covered_count = counts["covered_identity_occurrence_count"]
    eligible_count = counts["eligible_identity_occurrence_count"]
    observed_count = counts["observed_occurrence_count"]
    expected_count = counts["expected_occurrence_count"]
    if (
        covered_count is not None
        and eligible_count is not None
        and covered_count > eligible_count
    ):
        errors.append(
            f"{path}.covered_identity_occurrence_count cannot exceed "
            "eligible_identity_occurrence_count"
        )
    if (
        observed_count is not None
        and expected_count is not None
        and observed_count > expected_count
    ):
        errors.append(
            f"{path}.observed_occurrence_count cannot exceed "
            "expected_occurrence_count"
        )
    if expected_count is not None and expected_count <= 0:
        errors.append(f"{path}.expected_occurrence_count must be positive")
    if observed_count == 0 and covered_count not in {None, 0}:
        errors.append(
            f"{path}.covered_identity_occurrence_count must be zero when "
            "observed_occurrence_count is zero"
        )

    if detail.get("eligible_basis") != specification.get("eligible_basis"):
        errors.append(
            f"{path}.eligible_basis must equal "
            f"{specification.get('eligible_basis')}"
        )
    reason = detail.get("reason")
    if not isinstance(reason, str):
        errors.append(f"{path}.reason must be a string")
    percentage = detail.get("percentage")
    minimum = float(specification["minimum_percentage"])

    if status == "not_applicable":
        if not bool(specification.get("allow_not_applicable_when_empty")):
            errors.append(f"{path}.status cannot be not_applicable")
        if eligible_count != 0 or covered_count != 0:
            errors.append(
                f"{path}.not_applicable requires an empty eligible account set"
            )
        if percentage is not None:
            errors.append(f"{path}.percentage must be null for not_applicable")
        if scalar is not None:
            errors.append(
                "$.data_quality.discovery_coverage must be null for not_applicable"
            )
        if isinstance(reason, str) and not reason.strip():
            errors.append(f"{path}.reason is required for not_applicable")
        return

    if eligible_count is not None and eligible_count <= 0:
        errors.append(
            f"{path}.eligible_identity_occurrence_count must be positive "
            "when applicable"
        )
    if (
        isinstance(percentage, bool)
        or not isinstance(percentage, (int, float))
        or not 0 <= float(percentage) <= 100
    ):
        errors.append(f"{path}.percentage must be 0..100 when applicable")
        expected_percentage: Optional[float] = None
    elif covered_count is not None and eligible_count is not None and eligible_count > 0:
        expected_percentage = round(covered_count * 100 / eligible_count, 2)
        if abs(float(percentage) - expected_percentage) > 1e-9:
            errors.append(
                f"{path}.percentage must equal "
                "covered_identity_occurrence_count / "
                "eligible_identity_occurrence_count"
            )
    else:
        expected_percentage = None
    if (
        isinstance(scalar, bool)
        or not isinstance(scalar, (int, float))
        or expected_percentage is None
        or abs(float(scalar) - expected_percentage) > 1e-9
    ):
        errors.append(
            "$.data_quality.discovery_coverage must equal the structured percentage"
        )
    if expected_percentage is not None:
        expected_status = (
            "available" if expected_percentage >= minimum else "below_threshold"
        )
        if status != expected_status:
            errors.append(
                f"{path}.status must be {expected_status} for the declared percentage"
            )
    if status == "below_threshold" and isinstance(reason, str) and not reason.strip():
        errors.append(f"{path}.reason is required for below_threshold")


def _validate_data_quality_details(
    details: Any,
    data_quality: Mapping[str, Any],
    publication_value: Any,
    contract: Mapping[str, Any],
    errors: List[str],
) -> None:
    specifications = contract.get("required_quality_details", {})
    if not specifications:
        return
    if not isinstance(details, Mapping):
        errors.append("$.data_quality_details must be an object")
        return
    if (
        isinstance(publication_value, bool)
        or not isinstance(publication_value, int)
        or publication_value < 0
    ):
        errors.append(
            "$.summary_metrics.publication_count.value must be a non-negative integer"
        )
        publication_count: Optional[int] = None
    else:
        publication_count = publication_value

    required_fields = {
        "status",
        "fresh_count",
        "as_of_snapshot_count",
        "eligible_count",
        "percentage",
        "window_hours",
        "eligible_basis",
        "reason",
    }
    allowed_statuses = {"available", "below_threshold", "not_applicable"}
    for key, specification in specifications.items():
        path = f"$.data_quality_details.{key}"
        if key not in data_quality:
            errors.append(f"$.data_quality.{key} is required")
        detail = details.get(key)
        if key == "discovery_coverage":
            _validate_discovery_coverage_detail(
                detail,
                data_quality.get(key),
                specification,
                path,
                errors,
            )
            continue
        if not isinstance(detail, Mapping):
            errors.append(f"{path} must be an object")
            continue
        missing = sorted(required_fields - set(detail.keys()))
        if missing:
            errors.append(f"{path} missing {missing}")
        status = detail.get("status")
        if status not in allowed_statuses:
            errors.append(f"{path}.status is invalid")
        counts: Dict[str, Optional[int]] = {}
        for field in ("fresh_count", "as_of_snapshot_count", "eligible_count"):
            value = detail.get(field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                errors.append(f"{path}.{field} must be a non-negative integer")
                counts[field] = None
            else:
                counts[field] = value
        fresh_count = counts["fresh_count"]
        snapshot_count = counts["as_of_snapshot_count"]
        eligible_count = counts["eligible_count"]
        if (
            fresh_count is not None
            and snapshot_count is not None
            and fresh_count > snapshot_count
        ):
            errors.append(f"{path}.fresh_count cannot exceed as_of_snapshot_count")
        if (
            snapshot_count is not None
            and eligible_count is not None
            and snapshot_count > eligible_count
        ):
            errors.append(f"{path}.as_of_snapshot_count cannot exceed eligible_count")
        if (
            eligible_count is not None
            and publication_count is not None
            and eligible_count > publication_count
        ):
            errors.append(f"{path}.eligible_count cannot exceed publication_count")

        percentage = detail.get("percentage")
        scalar = data_quality.get(key)
        reason = detail.get("reason")
        if not isinstance(reason, str):
            errors.append(f"{path}.reason must be a string")
        if detail.get("window_hours") != specification.get("window_hours"):
            errors.append(
                f"{path}.window_hours must equal {specification.get('window_hours')}"
            )
        if detail.get("eligible_basis") != specification.get("eligible_basis"):
            errors.append(
                f"{path}.eligible_basis must equal "
                f"{specification.get('eligible_basis')}"
            )
        if (
            eligible_count is not None
            and publication_count is not None
            and eligible_count != publication_count
        ):
            errors.append(
                f"{path}.eligible_count must equal publication_count for "
                "all_window_contents"
            )
        minimum = float(specification["minimum_percentage"])
        if status == "not_applicable":
            if not bool(specification.get("allow_not_applicable_when_empty")):
                errors.append(f"{path}.status cannot be not_applicable")
            if publication_count != 0 or any(
                value != 0 for value in (fresh_count, snapshot_count, eligible_count)
            ):
                errors.append(
                    f"{path}.not_applicable requires an empty publication window"
                )
            if percentage is not None:
                errors.append(f"{path}.percentage must be null for not_applicable")
            if scalar is not None:
                errors.append(
                    f"$.data_quality.{key} must be null for not_applicable"
                )
            if isinstance(reason, str) and not reason.strip():
                errors.append(f"{path}.reason is required for not_applicable")
            continue

        if eligible_count is not None and eligible_count <= 0:
            errors.append(f"{path}.eligible_count must be positive when applicable")
        if (
            isinstance(percentage, bool)
            or not isinstance(percentage, (int, float))
            or not 0 <= float(percentage) <= 100
        ):
            errors.append(f"{path}.percentage must be 0..100 when applicable")
            expected_percentage: Optional[float] = None
        elif fresh_count is not None and eligible_count is not None and eligible_count > 0:
            expected_percentage = round(fresh_count * 100 / eligible_count, 2)
            if abs(float(percentage) - expected_percentage) > 1e-9:
                errors.append(
                    f"{path}.percentage must equal fresh_count / eligible_count"
                )
        else:
            expected_percentage = None
        if (
            isinstance(scalar, bool)
            or not isinstance(scalar, (int, float))
            or expected_percentage is None
            or abs(float(scalar) - expected_percentage) > 1e-9
        ):
            errors.append(
                f"$.data_quality.{key} must equal the structured percentage"
            )
        if expected_percentage is not None:
            expected_status = (
                "available" if expected_percentage >= minimum else "below_threshold"
            )
            if status != expected_status:
                errors.append(
                    f"{path}.status must be {expected_status} for the declared percentage"
                )
        if status == "below_threshold" and isinstance(reason, str) and not reason.strip():
            errors.append(f"{path}.reason is required for below_threshold")


def validate_report(
    report: Mapping[str, Any],
    *,
    contract_path: Optional[Path] = None,
) -> None:
    report_version = report.get("report_version")
    contract = load_contract(
        contract_path,
        report_version=str(report_version) if report_version is not None else None,
    )
    errors: List[str] = []
    missing = [key for key in contract["required_top_level_keys"] if key not in report]
    if missing:
        errors.append(f"$ missing {missing}")
    for key in ("report_version", "rule_version", "evidence_version"):
        expected = contract[key]
        if report.get(key) != expected:
            errors.append(f"$.{key} must equal {expected}")
    taxonomy_version = report.get("taxonomy_version")
    if (
        not isinstance(taxonomy_version, str)
        or re.fullmatch(contract["taxonomy_version_pattern"], taxonomy_version) is None
    ):
        errors.append("$.taxonomy_version must be a versioned selling-points taxonomy")

    metadata_timestamps: Dict[str, Optional[datetime]] = {}
    metadata = report.get("metadata")
    if not isinstance(metadata, Mapping):
        errors.append("$.metadata must be an object")
    else:
        for key in contract.get(
            "metadata_required_keys", ("task_id", "revision", "generated_at")
        ):
            if key not in metadata:
                errors.append(f"$.metadata.{key} is required")
        for key in contract.get("metadata_utc_timestamp_keys", ()):
            if key in metadata:
                metadata_timestamps[key] = _validate_utc_timestamp(
                    metadata.get(key), f"$.metadata.{key}", errors
                )

    period_end_at: Optional[datetime] = None
    scope = report.get("scope")
    if not isinstance(scope, Mapping):
        errors.append("$.scope must be an object")
    else:
        for key in ("period_start", "period_end", "timezone"):
            if key not in scope:
                errors.append(f"$.scope.{key} is required")
        if scope.get("timezone") != contract["timezone"]:
            errors.append(f"$.scope.timezone must equal {contract['timezone']}")
        if contract.get("required_quality_details") and "period_end" in scope:
            try:
                parsed_period_end = datetime.fromisoformat(
                    str(scope["period_end"]).replace("Z", "+00:00")
                )
            except ValueError:
                errors.append("$.scope.period_end must be an RFC3339 timestamp")
            else:
                if (
                    parsed_period_end.tzinfo is None
                    or parsed_period_end.utcoffset() != timedelta(hours=8)
                ):
                    errors.append(
                        "$.scope.period_end must use the Asia/Shanghai UTC+08:00 offset"
                    )
                else:
                    period_end_at = parsed_period_end

    task = report.get("task")
    task_status = task.get("task_status") if isinstance(task, Mapping) else None
    if isinstance(task, Mapping):
        for key in contract.get("task_required_keys", ("task_status",)):
            if key not in task:
                errors.append(f"$.task.{key} is required")
        task_type = task.get("task_type")
        creation_source = task.get("creation_source")
        if creation_source == "automatic" and task_type not in {"daily", "weekly"}:
            errors.append(
                "$.task automatic creation_source requires daily or weekly task_type"
            )
        elif creation_source == "manual" and task_type != "custom":
            errors.append(
                "$.task manual creation_source requires custom task_type"
            )
        elif creation_source not in {"automatic", "manual"}:
            errors.append("$.task.creation_source is invalid")
    if task_status not in contract["task_statuses"]:
        errors.append("$.task.task_status is invalid")
    generated_at = metadata_timestamps.get("generated_at")
    collection_cutoff_at = metadata_timestamps.get("collection_cutoff_at")
    if (
        generated_at is not None
        and collection_cutoff_at is not None
        and generated_at < collection_cutoff_at
    ):
        errors.append(
            "$.metadata.generated_at must be at or after collection_cutoff_at"
        )
    if collection_cutoff_at is not None and period_end_at is not None:
        period_end_utc = period_end_at.astimezone(collection_cutoff_at.tzinfo)
        if collection_cutoff_at < period_end_utc:
            errors.append(
                "$.metadata.collection_cutoff_at must be at or after scope.period_end"
            )
        if isinstance(task, Mapping) and task.get("creation_source") == "automatic":
            task_type = task.get("task_type")
            if task_type == "daily":
                expected_cutoff = period_end_utc + timedelta(hours=8)
            elif task_type == "weekly":
                expected_cutoff = period_end_utc + timedelta(hours=8, minutes=30)
            else:
                expected_cutoff = None
            if expected_cutoff is not None and collection_cutoff_at != expected_cutoff:
                errors.append(
                    "$.metadata.collection_cutoff_at must equal the automatic "
                    f"{task_type} collection cutoff"
                )

    data_quality = report.get("data_quality")
    summary_payload = report.get("summary_metrics")
    publication_payload = (
        summary_payload.get("publication_count")
        if isinstance(summary_payload, Mapping)
        else None
    )
    publication_value = (
        publication_payload.get("value")
        if isinstance(publication_payload, Mapping)
        else None
    )
    enforce_boolean_quality_gates = bool(
        isinstance(publication_value, (int, float))
        and not isinstance(publication_value, bool)
        and publication_value > 0
    )
    if not isinstance(data_quality, Mapping):
        errors.append("$.data_quality must be an object")
    else:
        for key in contract["required_coverage_thresholds"]:
            value = data_quality.get(key)
            if not isinstance(value, (int, float)) or not 0 <= float(value) <= 100:
                errors.append(f"$.data_quality.{key} must be 0..100")
        for key in contract.get("required_boolean_quality_gates", {}):
            if enforce_boolean_quality_gates and key not in data_quality:
                errors.append(f"$.data_quality.{key} is required")
            elif key in data_quality and not isinstance(data_quality[key], bool):
                errors.append(f"$.data_quality.{key} must be boolean")
        for key in contract.get("boolean_quality_fields", ()):
            if key not in data_quality:
                errors.append(f"$.data_quality.{key} is required")
            elif not isinstance(data_quality[key], bool):
                errors.append(f"$.data_quality.{key} must be boolean")
        _validate_data_quality_details(
            report.get("data_quality_details"),
            data_quality,
            publication_value,
            contract,
            errors,
        )
        if task_status in {"succeeded", "partial"}:
            expected_status = expected_terminal_task_status(
                data_quality,
                data_quality_details=(
                    report.get("data_quality_details")
                    if isinstance(report.get("data_quality_details"), Mapping)
                    else None
                ),
                contract=contract,
                enforce_boolean_quality_gates=enforce_boolean_quality_gates,
            )
            if task_status != expected_status:
                errors.append(
                    f"$.task.task_status must be {expected_status} for the declared required coverage"
                )

    summary = report.get("summary_metrics")
    if not isinstance(summary, Mapping):
        errors.append("$.summary_metrics must be an object")
    else:
        for name, expected in contract["summary_metrics"].items():
            _validate_metric(
                summary.get(name),
                expected,
                f"$.summary_metrics.{name}",
                errors,
                contract,
            )
        publication = summary.get("publication_count")
        publication_value = (
            publication.get("value") if isinstance(publication, Mapping) else None
        )
        if isinstance(publication_value, (int, float)):
            for name in (
                "verticality_rate",
                "selling_point_coverage_rate",
                "duplicate_rate",
            ):
                metric = summary.get(name)
                if isinstance(metric, Mapping) and metric.get("denominator") != int(
                    publication_value
                ):
                    errors.append(
                        f"$.summary_metrics.{name}.denominator must equal publication_count.value"
                    )
        if isinstance(data_quality, Mapping):
            _validate_summary_quality_alignment(
                summary,
                data_quality,
                publication_value,
                contract,
                errors,
            )

    if "channel_conclusion_metrics" in contract:
        _validate_channel_conclusions(report.get("channels"), contract, errors)

    for key in (
        "platform_dimensions",
        "account_type_dimensions",
        "content_direction_dimensions",
        "selling_point_dimensions",
        "duplicates",
        "capture_summary",
        "provider_costs",
        "content_details",
        "files",
    ):
        if key in report and not isinstance(report[key], list):
            errors.append(f"$.{key} must be an array")

    forbidden_partial = _walk_metric_statuses(report)
    if forbidden_partial:
        errors.append(f"metric status partial is forbidden at {forbidden_partial}")
    if errors:
        raise V8ContractViolation("; ".join(errors))
