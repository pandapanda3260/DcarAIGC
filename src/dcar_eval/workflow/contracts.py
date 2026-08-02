"""Machine-readable v7 report contract and validation helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONTRACT_PATH = PROJECT_ROOT / "config" / "report_contract_v7.json"


class ContractViolation(ValueError):
    """Raised when a v7 report violates a frozen data rule."""


def load_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def ratio_metric(
    numerator: int | None,
    denominator: int,
    *,
    status: str,
    qualitative: str,
    scope: str,
    reason: str = "",
) -> dict[str, Any]:
    percentage = (
        round(numerator * 100 / denominator, 2)
        if numerator is not None and denominator
        else None
    )
    return {
        "kind": "ratio",
        "numerator": numerator,
        "denominator": denominator,
        "percentage": percentage,
        "status": status,
        "qualitative": qualitative,
        "scope": scope,
        "reason": reason,
    }


def score_metric(
    score: int | None,
    scorable_items: int,
    total_items: int,
    *,
    status: str,
    qualitative: str,
    scope: str,
    reason: str = "",
) -> dict[str, Any]:
    coverage = round(scorable_items * 100 / total_items, 2) if total_items else None
    return {
        "kind": "score",
        "score": score,
        "scale": 100,
        "scorable_items": scorable_items,
        "total_items": total_items,
        "coverage_percentage": coverage,
        "status": status,
        "qualitative": qualitative,
        "scope": scope,
        "reason": reason,
    }


def _forbidden_key_paths(value: Any, fragments: list[str], path: str = "$") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if any(fragment in str(key).lower() for fragment in fragments):
                found.append(child_path)
            found.extend(_forbidden_key_paths(child, fragments, child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(_forbidden_key_paths(child, fragments, f"{path}[{index}]"))
    return found


def _missing_keys(value: Mapping[str, Any], required: list[str]) -> list[str]:
    return [key for key in required if key not in value]


def _validate_ratio_metric(
    metric: Mapping[str, Any], *, expected_denominator: int | None, path: str, errors: list[str]
) -> None:
    required = [
        "kind", "numerator", "denominator", "percentage", "status",
        "qualitative", "scope", "reason",
    ]
    missing = _missing_keys(metric, required)
    if missing:
        errors.append(f"{path} missing {missing}")
        return
    if metric["kind"] != "ratio":
        errors.append(f"{path}.kind must be ratio")
    if expected_denominator is not None and metric["denominator"] != expected_denominator:
        errors.append(
            f"{path}.denominator={metric['denominator']} expected {expected_denominator}"
        )


def _validate_score_metric(
    metric: Mapping[str, Any], *, total_items: int, path: str, errors: list[str]
) -> None:
    required = [
        "kind", "score", "scale", "scorable_items", "total_items",
        "coverage_percentage", "status", "qualitative", "scope", "reason",
    ]
    missing = _missing_keys(metric, required)
    if missing:
        errors.append(f"{path} missing {missing}")
        return
    if metric["kind"] != "score" or metric["scale"] != 100:
        errors.append(f"{path} must be a 100-point score metric")
    if metric["total_items"] != total_items:
        errors.append(f"{path}.total_items must equal its declared scope denominator")
    score = metric["score"]
    if score is not None and not 0 <= score <= 100:
        errors.append(f"{path}.score must be null or 0..100")
    scorable = int(metric["scorable_items"])
    if scorable == 0 and score is not None:
        errors.append(f"{path}.score must be null when no item is scorable")
    if 0 < scorable < total_items and metric["status"] != "sample_only":
        errors.append(f"{path}.status must be sample_only for partial coverage")


def _validate_count_distribution(
    count: Any,
    *,
    denominator: int,
    path: str,
    contract: Mapping[str, Any],
    errors: list[str],
) -> None:
    if not isinstance(count, Mapping):
        errors.append(f"{path} is missing")
        return
    for metric_name in contract["count_metrics"]:
        metric = count.get(metric_name)
        if not isinstance(metric, Mapping):
            errors.append(f"{path}.{metric_name} is missing")
        else:
            _validate_ratio_metric(
                metric,
                expected_denominator=denominator,
                path=f"{path}.{metric_name}",
                errors=errors,
            )
    diagnostics = count.get("diagnostics")
    if not isinstance(diagnostics, Mapping):
        errors.append(f"{path}.diagnostics is missing")
    else:
        missing = _missing_keys(diagnostics, contract["count_diagnostics"])
        if missing:
            errors.append(f"{path}.diagnostics missing {missing}")
    if all(isinstance(count.get(name), Mapping) for name in contract["count_metrics"]):
        selling = count["selling_point_covered"]["numerator"]
        core = count["core_selling_point"]["numerator"]
        other = count["other_selling_point"]["numerator"]
        if core + other != selling:
            errors.append(f"{path} core + other must equal selling-point coverage")


def _validate_exposure_distribution(
    exposure: Any,
    *,
    path: str,
    contract: Mapping[str, Any],
    errors: list[str],
) -> None:
    if not isinstance(exposure, Mapping):
        errors.append(f"{path} is missing")
        return
    coverage = exposure.get("coverage")
    if not isinstance(coverage, Mapping):
        errors.append(f"{path}.coverage is missing")
        exposure_denominator = None
    else:
        missing = _missing_keys(coverage, contract["exposure_coverage"])
        if missing:
            errors.append(f"{path}.coverage missing {missing}")
        exposure_denominator = coverage.get("total_valid_exposure")
        if coverage.get("required_percentage") != contract["exposure_cross_coverage_required_percentage"]:
            errors.append(f"{path} exposure threshold must be 90")
    for metric_name in contract["exposure_metrics"]:
        metric = exposure.get(metric_name)
        if not isinstance(metric, Mapping):
            errors.append(f"{path}.{metric_name} is missing")
        else:
            _validate_ratio_metric(
                metric,
                expected_denominator=exposure_denominator,
                path=f"{path}.{metric_name}",
                errors=errors,
            )


def _validate_verticality(
    verticality: Any,
    *,
    total_items: int,
    path: str,
    contract: Mapping[str, Any],
    errors: list[str],
) -> None:
    if not isinstance(verticality, Mapping):
        errors.append(f"{path} is missing")
        return
    for metric_name in contract["verticality_metrics"]:
        metric = verticality.get(metric_name)
        if not isinstance(metric, Mapping):
            errors.append(f"{path}.{metric_name} is missing")
        else:
            _validate_score_metric(
                metric,
                total_items=total_items,
                path=f"{path}.{metric_name}",
                errors=errors,
            )
    coverage = verticality.get("coverage")
    if not isinstance(coverage, Mapping):
        errors.append(f"{path}.coverage is missing")
    else:
        missing = _missing_keys(coverage, contract["verticality_coverage"])
        if missing:
            errors.append(f"{path}.coverage missing {missing}")
        if coverage.get("total_items") != total_items:
            errors.append(f"{path}.coverage.total_items must equal its scope denominator")
        if coverage.get("audience_gate") != contract["audience_gate"]["minimum_valid_unique_commenters"]:
            errors.append(f"{path}.coverage.audience_gate must be 20")


def validate_report(report: Mapping[str, Any]) -> None:
    """Validate the frozen v7 hierarchy and cross-field invariants."""

    contract = load_contract()
    errors: list[str] = []
    if report.get("report_version") != contract["report_version"]:
        errors.append("$.report_version does not match v7 contract")
    if report.get("rule_version") != contract["rule_version"]:
        errors.append("$.rule_version does not match v5 rules")
    for key in ("metadata", "run_summary", "conclusion_summary", "assets"):
        if key not in report:
            errors.append(f"$.{key} is missing")

    forbidden = _forbidden_key_paths(report, contract["forbidden_key_fragments"])
    if forbidden:
        errors.append(f"forbidden actual-acquisition keys: {forbidden}")

    channels = report.get("channels")
    if not isinstance(channels, Mapping):
        errors.append("$.channels must be an object")
        channels = {}

    for channel_name in contract["channels"]:
        channel = channels.get(channel_name)
        path = f"$.channels.{channel_name}"
        if not isinstance(channel, Mapping):
            errors.append(f"{path} is missing")
            continue
        denominator = channel.get("denominator")
        if not isinstance(denominator, int) or denominator < 0:
            errors.append(f"{path}.denominator must be a non-negative integer")
            continue

        _validate_count_distribution(
            channel.get("count_distribution"),
            denominator=denominator,
            path=f"{path}.count_distribution",
            contract=contract,
            errors=errors,
        )
        _validate_exposure_distribution(
            channel.get("exposure_distribution"),
            path=f"{path}.exposure_distribution",
            contract=contract,
            errors=errors,
        )
        _validate_verticality(
            channel.get("verticality"),
            total_items=denominator,
            path=f"{path}.verticality",
            contract=contract,
            errors=errors,
        )

        target = channel.get("channel_targets")
        if not isinstance(target, Mapping) or "core_selling_point_publication_share" not in target:
            errors.append(f"{path}.channel_targets is missing the channel-only core target")

        scenes = channel.get("scenes")
        if not isinstance(scenes, Mapping) or list(scenes) != contract["business_scenes"]:
            errors.append(f"{path}.scenes must preserve the three-scene order")
            scenes = {}
        for scene_name in contract["business_scenes"]:
            scene = scenes.get(scene_name)
            scene_path = f"{path}.scenes.{scene_name}"
            if not isinstance(scene, Mapping):
                continue
            if "channel_targets" in scene:
                errors.append(f"{scene_path} must not contain the channel 60%-70% target")
            publication_n = scene.get("publication_n")
            if not isinstance(publication_n, int) or publication_n < 0:
                errors.append(f"{scene_path}.publication_n must be a non-negative integer")
                continue
            _validate_count_distribution(
                scene.get("count_distribution"),
                denominator=denominator,
                path=f"{scene_path}.count_distribution",
                contract=contract,
                errors=errors,
            )
            _validate_exposure_distribution(
                scene.get("exposure_distribution"),
                path=f"{scene_path}.exposure_distribution",
                contract=contract,
                errors=errors,
            )
            _validate_verticality(
                scene.get("verticality"),
                total_items=publication_n,
                path=f"{scene_path}.verticality",
                contract=contract,
                errors=errors,
            )
            internal = scene.get("scene_internal")
            if not isinstance(internal, Mapping):
                errors.append(f"{scene_path}.scene_internal is missing")
            else:
                core_metric = internal.get("core_share_within_scene_publications")
                if not isinstance(core_metric, Mapping):
                    errors.append(f"{scene_path} internal core share is missing")
                elif core_metric.get("denominator") != publication_n:
                    errors.append(f"{scene_path} internal core share must use scene publications")

        details = channel.get("content_details")
        if not isinstance(details, list):
            errors.append(f"{path}.content_details must be a list")
        else:
            for index, detail in enumerate(details):
                if not isinstance(detail, Mapping):
                    continue
                audience = detail.get("audience_automotive")
                potential = detail.get("acquisition_potential")
                if isinstance(audience, Mapping) and isinstance(potential, Mapping):
                    if audience.get("score") is None and potential.get("score") is not None:
                        errors.append(
                            f"{path}.content_details[{index}] acquisition must be null when audience is null"
                        )

    if errors:
        raise ContractViolation("; ".join(errors))
