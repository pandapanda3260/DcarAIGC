"""DCar Insight v8 report metrics and contract validation."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from .storage import PROJECT_ROOT


CONTRACT_PATH = PROJECT_ROOT / "config" / "report_contract_v8_5.json"
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
}
CURRENT_REPORT_VERSION = "dcar-content-operations-report-v8.5"
CURRENT_REPORT_RULE_VERSION = "evaluation-v8"
CURRENT_REPORT_EVIDENCE_VERSION = "evidence-v2"
REPORT_RULE_VERSIONS = {
    CURRENT_REPORT_VERSION: CURRENT_REPORT_RULE_VERSION,
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
    selected = path or LEGACY_CONTRACT_PATHS.get(report_version or "", CONTRACT_PATH)
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
    contract: Optional[Mapping[str, Any]] = None,
    enforce_boolean_quality_gates: bool = True,
) -> str:
    return (
        "partial"
        if quality_gate_failures(
            data_quality,
            contract=contract,
            enforce_boolean_quality_gates=enforce_boolean_quality_gates,
        )
        else "succeeded"
    )


def quality_gate_failures(
    data_quality: Mapping[str, Any],
    *,
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

    metadata = report.get("metadata")
    if not isinstance(metadata, Mapping):
        errors.append("$.metadata must be an object")
    else:
        for key in ("task_id", "revision", "generated_at"):
            if key not in metadata:
                errors.append(f"$.metadata.{key} is required")

    scope = report.get("scope")
    if not isinstance(scope, Mapping):
        errors.append("$.scope must be an object")
    else:
        for key in ("period_start", "period_end", "timezone"):
            if key not in scope:
                errors.append(f"$.scope.{key} is required")
        if scope.get("timezone") != contract["timezone"]:
            errors.append(f"$.scope.timezone must equal {contract['timezone']}")

    task = report.get("task")
    task_status = task.get("task_status") if isinstance(task, Mapping) else None
    if task_status not in contract["task_statuses"]:
        errors.append("$.task.task_status is invalid")

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
        if task_status in {"succeeded", "partial"}:
            expected_status = expected_terminal_task_status(
                data_quality,
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

    if "channel_conclusion_metrics" in contract:
        _validate_channel_conclusions(report.get("channels"), contract, errors)

    for key in (
        "platform_dimensions",
        "account_type_dimensions",
        "content_direction_dimensions",
        "selling_point_dimensions",
        "duplicates",
        "review_summary",
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
