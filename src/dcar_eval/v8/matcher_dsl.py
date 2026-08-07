"""Pure, data-driven selling-point matcher DSL.

The evaluator in this module deliberately has no database or service imports.
It executes a checked-in JSON bundle and is therefore suitable for differential
testing before it is wired into the v8 runtime.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Literal


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_BUNDLE_PATH = PROJECT_ROOT / "config" / "selling_point_matcher_v3.json"
V4_BUNDLE_PATH = PROJECT_ROOT / "config" / "selling_point_matcher_v4.json"
SOURCES = ("asr", "ocr", "visual", "desc")
SCENES = {"used_car", "new_car", "media"}
ENGINE_VERSION = "matcher-dsl-engine-v1"
MATERIALIZED_RULE_VERSION = 1
V5_1_POINT_SPEC = {
    **{f"E{index}": {"used_car"} for index in range(1, 8)},
    **{f"X{index}": {"new_car"} for index in range(1, 9)},
    **{f"M{index}": {"media"} for index in range(1, 7)},
    "C1": {"used_car", "media"},
    "C2": {"used_car", "new_car"},
    "C3": {"media"},
    "C4": {"used_car", "new_car", "media"},
}
V5_2_POINT_SPEC = {
    **{f"E{index}": {"used_car"} for index in range(1, 11)},
    **{f"X{index}": {"new_car"} for index in range(1, 12)},
    **{f"M{index}": {"media"} for index in range(1, 7)},
    "M8": {"media"},
}
# Backward-compatible aliases for the active v5.1 runtime. Newer taxonomies must
# pass their point spec explicitly instead of changing process-global matcher state.
POINT_SCENES = V5_1_POINT_SPEC
POINT_IDS = set(POINT_SCENES)
EXPRESSION_OPS = {
    "all",
    "any",
    "not",
    "ref",
    "field_eq",
    "contains_any",
    "term_count",
    "compare",
    "regex_any",
    "regex_fullmatch",
    "terms_near",
    "chinese_count",
    "source_any",
    "context_regex_any",
}
MAX_EXPRESSION_DEPTH = 64
MAX_TERM_SETS = 256
MAX_TERMS_PER_SET = 512
MAX_TERM_LENGTH = 256
MAX_REGEX_LENGTH = 4096
MAX_CONTEXT_WINDOW = 500
ExpressionKind = Literal["bool", "number", "string"]


class MatcherDslError(ValueError):
    """Raised when a matcher bundle is malformed or cannot be evaluated."""


def _reject_unknown(value: Mapping[str, Any], allowed: set[str], label: str) -> None:
    unknown = set(value) - allowed
    if unknown:
        raise MatcherDslError(f"unknown {label} keys: {sorted(unknown)}")


def _require_keys(value: Mapping[str, Any], required: set[str], label: str) -> None:
    missing = required - set(value)
    if missing:
        raise MatcherDslError(f"missing {label} keys: {sorted(missing)}")


def _bounded_int(
    value: Any,
    *,
    label: str,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MatcherDslError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise MatcherDslError(f"{label} must be between {minimum} and {maximum}")
    return value


def _reject_nonfinite_floats(value: Any, path: str = "bundle") -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise MatcherDslError(f"{path} contains a non-finite float")
    if isinstance(value, Mapping):
        for key, item in value.items():
            _reject_nonfinite_floats(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_nonfinite_floats(item, f"{path}[{index}]")


def _validate_flags(value: Any, label: str) -> None:
    if value not in {None, "", "i"}:
        raise MatcherDslError(f"{label} only supports the i flag")


def _compile_pattern(pattern: Any, flags: Any, label: str) -> None:
    if not isinstance(pattern, str) or not pattern:
        raise MatcherDslError(f"{label} must be a non-empty string")
    if len(pattern) > MAX_REGEX_LENGTH:
        raise MatcherDslError(f"{label} exceeds {MAX_REGEX_LENGTH} characters")
    _validate_flags(flags, f"{label}.flags")
    _validate_safe_regex(pattern, label)
    try:
        re.compile(pattern, re.IGNORECASE if flags == "i" else 0)
    except re.error as error:
        raise MatcherDslError(f"invalid {label}: {error}") from error


def _validate_safe_regex(pattern: str, label: str) -> None:
    """Reject constructs that can cause unbounded backtracking.

    The DSL intentionally supports lookarounds used by the legacy price rule,
    but rejects backreferences and nested unbounded quantifiers.  This is a
    conservative guard for checked-in rules, not a general regex sanitizer.
    """

    if re.search(r"(?<!\\)\\[1-9]|\(\?P=|\(\?\(", pattern):
        raise MatcherDslError(f"{label} contains a forbidden regex construct")
    if pattern.count("|") > 128:
        raise MatcherDslError(f"{label} contains too many alternatives")
    stack: list[bool] = []
    escaped = False
    in_class = False
    group_count = 0
    index = 0
    while index < len(pattern):
        char = pattern[index]
        if escaped:
            escaped = False
            index += 1
            continue
        if char == "\\":
            escaped = True
            index += 1
            continue
        if char == "[":
            in_class = True
            index += 1
            continue
        if char == "]" and in_class:
            in_class = False
            index += 1
            continue
        if in_class:
            index += 1
            continue
        if char == "(":
            group_count += 1
            if group_count > 128:
                raise MatcherDslError(f"{label} contains too many groups")
            stack.append(False)
        elif char in {"*", "+"} and stack:
            stack[-1] = True
        elif char == "{" and stack:
            closing = pattern.find("}", index + 1)
            if closing >= 0 and re.fullmatch(r"\{\d+,\}", pattern[index : closing + 1]):
                stack[-1] = True
        elif char == ")" and stack:
            contains_unbounded = stack.pop()
            following = pattern[index + 1 :]
            outer_unbounded = bool(re.match(r"(?:[*+]|\{\d+,\})", following))
            if contains_unbounded and outer_unbounded:
                raise MatcherDslError(f"{label} contains nested unbounded quantifiers")
            if contains_unbounded and stack:
                stack[-1] = True
        index += 1


def _scalar_kind(value: Any) -> ExpressionKind:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "number"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise MatcherDslError("expression numbers must be finite")
        return "number"
    if isinstance(value, str):
        return "string"
    raise MatcherDslError(f"unsupported expression literal: {type(value).__name__}")


def _expression_kind(
    expression: Any,
    predicates: Mapping[str, Any],
    *,
    active: tuple[str, ...] = (),
) -> ExpressionKind:
    if not isinstance(expression, dict):
        return _scalar_kind(expression)
    op = str(expression["op"])
    if op == "ref":
        name = str(expression["name"])
        if name in active:
            raise MatcherDslError(
                f"cyclic predicate kind reference: {' -> '.join((*active, name))}"
            )
        return _expression_kind(predicates[name], predicates, active=(*active, name))
    if op in {"all", "any"}:
        for item in expression["args"]:
            if _expression_kind(item, predicates, active=active) != "bool":
                raise MatcherDslError(f"{op} operands must be boolean expressions")
        return "bool"
    if op == "not":
        if _expression_kind(expression["arg"], predicates, active=active) != "bool":
            raise MatcherDslError("not operand must be a boolean expression")
        return "bool"
    if op == "compare":
        left_kind = _expression_kind(expression["left"], predicates, active=active)
        right_kind = _expression_kind(expression["right"], predicates, active=active)
        comparator = expression["cmp"]
        if comparator == "eq" and left_kind != right_kind:
            raise MatcherDslError("eq operands must have the same kind")
        if comparator != "eq" and (left_kind, right_kind) != (
            "number",
            "number",
        ):
            raise MatcherDslError(f"{comparator} operands must both be numbers")
        return "bool"
    if op in {"term_count", "chinese_count"}:
        return "number"
    if op == "source_any":
        if _expression_kind(expression["expr"], predicates, active=active) != "bool":
            raise MatcherDslError("source_any.expr must be boolean")
        return "bool"
    if op == "context_regex_any":
        if _expression_kind(expression["where"], predicates, active=active) != "bool":
            raise MatcherDslError("context_regex_any.where must be boolean")
        return "bool"
    if op in {
        "field_eq",
        "contains_any",
        "regex_any",
        "regex_fullmatch",
        "terms_near",
    }:
        return "bool"
    raise MatcherDslError(f"operator {op!r} has no static expression kind")


def _require_expression_kind(
    expression: Any,
    expected: ExpressionKind,
    predicates: Mapping[str, Any],
    label: str,
) -> None:
    actual = _expression_kind(expression, predicates)
    if actual != expected:
        raise MatcherDslError(f"{label} must be {expected}, got {actual}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def bundle_sha256(
    bundle: Mapping[str, Any],
    *,
    point_spec: Mapping[str, set[str]] = V5_1_POINT_SPEC,
) -> str:
    validate_bundle(bundle, point_spec=point_spec)
    return hashlib.sha256(canonical_json(bundle).encode("utf-8")).hexdigest()


def load_bundle_bytes(
    payload: bytes,
    *,
    point_spec: Mapping[str, set[str]] = V5_1_POINT_SPEC,
) -> dict[str, Any]:
    """Parse and validate one exact UTF-8 matcher-bundle byte payload."""

    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise MatcherDslError(f"cannot load matcher bundle: {error}") from error
    if not isinstance(value, dict):
        raise MatcherDslError("matcher bundle must be a JSON object")
    validate_bundle(value, point_spec=point_spec)
    return value


def load_bundle(
    path: Path = DEFAULT_BUNDLE_PATH,
    *,
    point_spec: Mapping[str, set[str]] = V5_1_POINT_SPEC,
) -> dict[str, Any]:
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise MatcherDslError(f"cannot load matcher bundle: {error}") from error
    return load_bundle_bytes(payload, point_spec=point_spec)


def project_rule_explain(
    bundle: Mapping[str, Any],
    point_id: str,
    *,
    point_spec: Mapping[str, set[str]] = V5_1_POINT_SPEC,
) -> dict[str, Any]:
    """Return the checked projection used by taxonomy authoring surfaces."""

    validate_bundle(bundle, point_spec=point_spec)
    rule = next(
        (item for item in bundle["rules"] if item["point_id"] == point_id), None
    )
    if rule is None:
        raise MatcherDslError(f"unknown point id: {point_id!r}")
    scene = rule["scene"]
    scenes = (
        {scene}
        if isinstance(scene, str)
        else {str(scene["default"]), *(str(case["value"]) for case in scene["cases"])}
    )
    explain = rule["explain"]
    return {
        "positive_evidence": list(explain["positive_evidence"]),
        "negative_evidence": list(explain["negative_evidence"]),
        "boundary_rules": list(explain["boundary_rules"]),
        "scenes": sorted(scenes),
    }


_BUNDLE_SHARED_KEYS = (
    "dsl_version",
    "engine_version",
    "legacy_matcher_sha256",
    "normalization",
    "views",
    "term_sets",
    "predicates",
    "predicate_scopes",
    "scoring",
    "thresholds",
)
_MATERIALIZED_RULE_KEYS = {
    "materialized_rule_version",
    *_BUNDLE_SHARED_KEYS,
    "rule",
}


def _rule_scene_values(rule: Mapping[str, Any]) -> set[str]:
    scene = rule.get("scene")
    if isinstance(scene, str):
        return {scene}
    if not isinstance(scene, Mapping):
        raise MatcherDslError("scene must be a scene or ordered case object")
    cases = scene.get("cases")
    if not isinstance(cases, list):
        raise MatcherDslError("scene.cases must be a list")
    return {
        str(scene.get("default") or ""),
        *(str(case.get("value") or "") for case in cases if isinstance(case, Mapping)),
    }


def _collect_expression_dependencies(
    value: Any,
    *,
    views: set[str],
    predicates: set[str],
    term_sets: set[str],
) -> None:
    if isinstance(value, Mapping):
        if value.get("op") == "ref" and isinstance(value.get("name"), str):
            predicates.add(str(value["name"]))
        view = value.get("view")
        if isinstance(view, str) and not view.startswith("$"):
            views.add(view)
        for key in ("terms", "patterns", "left_terms", "right_terms"):
            dependency = value.get(key)
            if isinstance(dependency, str):
                term_sets.add(dependency)
        for item in value.values():
            _collect_expression_dependencies(
                item,
                views=views,
                predicates=predicates,
                term_sets=term_sets,
            )
    elif isinstance(value, list):
        for item in value:
            _collect_expression_dependencies(
                item,
                views=views,
                predicates=predicates,
                term_sets=term_sets,
            )


def _dependency_closed_sections(
    bundle: Mapping[str, Any], rule: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Return the minimal executable view/predicate/term dependency closure."""

    required_views = set(SOURCES)
    required_predicates = {"explicit_dcar"}
    required_term_sets = {str(rule["evidence"]["terms"])}
    _collect_expression_dependencies(
        rule["when"],
        views=required_views,
        predicates=required_predicates,
        term_sets=required_term_sets,
    )
    scene = rule["scene"]
    if isinstance(scene, Mapping):
        for case in scene["cases"]:
            _collect_expression_dependencies(
                case["when"],
                views=required_views,
                predicates=required_predicates,
                term_sets=required_term_sets,
            )
    source_expr = rule["evidence"].get("source_expr")
    if source_expr is not None:
        _collect_expression_dependencies(
            source_expr,
            views=required_views,
            predicates=required_predicates,
            term_sets=required_term_sets,
        )

    processed_views: set[str] = set()
    processed_predicates: set[str] = set()
    while (
        processed_views != required_views or processed_predicates != required_predicates
    ):
        for name in sorted(required_views - processed_views):
            definition = bundle["views"][name]
            if definition["op"] == "concat":
                required_views.update(str(item) for item in definition["views"])
            elif definition["op"] == "select":
                required_views.update(
                    {str(definition["then"]), str(definition["else"])}
                )
                _collect_expression_dependencies(
                    definition["when"],
                    views=required_views,
                    predicates=required_predicates,
                    term_sets=required_term_sets,
                )
            processed_views.add(name)
        for name in sorted(required_predicates - processed_predicates):
            _collect_expression_dependencies(
                bundle["predicates"][name],
                views=required_views,
                predicates=required_predicates,
                term_sets=required_term_sets,
            )
            processed_predicates.add(name)

    views = {
        name: copy.deepcopy(bundle["views"][name]) for name in sorted(required_views)
    }
    predicates = {
        name: copy.deepcopy(bundle["predicates"][name])
        for name in sorted(required_predicates)
    }
    predicate_scopes = {
        name: copy.deepcopy(bundle["predicate_scopes"].get(name, []))
        for name in sorted(required_predicates)
    }
    term_sets = {
        name: copy.deepcopy(bundle["term_sets"][name])
        for name in sorted(required_term_sets)
    }
    return views, predicates, predicate_scopes, term_sets


def materialize_point_rule(
    bundle: Mapping[str, Any],
    point_id: str,
    *,
    point_spec: Mapping[str, set[str]] = V5_1_POINT_SPEC,
) -> dict[str, Any]:
    """Build a standalone database rule with no dependency on the bundle file.

    Shared normalization, views, term sets, predicates, scoring and thresholds
    are intentionally embedded beside the single point rule.  The resulting
    JSON can be validated and projected after the source bundle is unavailable.
    """

    validate_bundle(bundle, point_spec=point_spec)
    rule = next(
        (item for item in bundle["rules"] if item["point_id"] == point_id), None
    )
    if rule is None:
        raise MatcherDslError(f"unknown point id: {point_id!r}")
    views, predicates, predicate_scopes, term_sets = _dependency_closed_sections(
        bundle, rule
    )
    materialized = {
        "materialized_rule_version": MATERIALIZED_RULE_VERSION,
        "dsl_version": copy.deepcopy(bundle["dsl_version"]),
        "engine_version": copy.deepcopy(bundle["engine_version"]),
        "legacy_matcher_sha256": copy.deepcopy(bundle["legacy_matcher_sha256"]),
        "normalization": copy.deepcopy(bundle["normalization"]),
        "views": views,
        "term_sets": term_sets,
        "predicates": predicates,
        "predicate_scopes": predicate_scopes,
        "scoring": copy.deepcopy(bundle["scoring"]),
        "thresholds": copy.deepcopy(bundle["thresholds"]),
        "rule": copy.deepcopy(rule),
    }
    validate_materialized_rule(materialized, point_spec=point_spec)
    return materialized


def _materialized_as_bundle(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **{key: value[key] for key in _BUNDLE_SHARED_KEYS},
        "rules": [value["rule"]],
    }


def validate_materialized_rule(
    value: Mapping[str, Any],
    *,
    point_spec: Mapping[str, set[str]] = V5_1_POINT_SPEC,
) -> None:
    """Validate one self-contained point rule without loading global state."""

    _reject_nonfinite_floats(value, "materialized_rule")
    _reject_unknown(value, _MATERIALIZED_RULE_KEYS, "materialized rule")
    _require_keys(value, _MATERIALIZED_RULE_KEYS, "materialized rule")
    if value.get("materialized_rule_version") != MATERIALIZED_RULE_VERSION:
        raise MatcherDslError("unsupported materialized_rule_version")
    rule = value.get("rule")
    if not isinstance(rule, Mapping):
        raise MatcherDslError("materialized rule.rule must be an object")
    point_id = str(rule.get("point_id") or "")
    if not re.fullmatch(r"[A-Z][1-9][0-9]?", point_id, re.ASCII):
        raise MatcherDslError("materialized rule point_id is invalid")
    expected_scenes = (
        point_spec[point_id] if point_id in point_spec else _rule_scene_values(rule)
    )
    _validate_bundle(
        _materialized_as_bundle(value),
        expected_point_scenes={point_id: expected_scenes},
    )


def canonical_materialized_rule(
    value: Mapping[str, Any],
    *,
    point_spec: Mapping[str, set[str]] = V5_1_POINT_SPEC,
) -> str:
    validate_materialized_rule(value, point_spec=point_spec)
    return canonical_json(value)


def materialized_rule_sha256(
    value: Mapping[str, Any],
    *,
    point_spec: Mapping[str, set[str]] = V5_1_POINT_SPEC,
) -> str:
    return hashlib.sha256(
        canonical_materialized_rule(value, point_spec=point_spec).encode("utf-8")
    ).hexdigest()


def project_materialized_rule(
    value: Mapping[str, Any],
    *,
    point_spec: Mapping[str, set[str]] = V5_1_POINT_SPEC,
) -> dict[str, Any]:
    validate_materialized_rule(value, point_spec=point_spec)
    rule = value["rule"]
    explain = rule["explain"]
    return {
        "positive_evidence": list(explain["positive_evidence"]),
        "negative_evidence": list(explain["negative_evidence"]),
        "boundary_rules": list(explain["boundary_rules"]),
        "scenes": sorted(_rule_scene_values(rule)),
    }


def taxonomy_matcher_sha256(
    rules: Mapping[str, Mapping[str, Any]],
    *,
    point_spec: Mapping[str, set[str]] = V5_1_POINT_SPEC,
) -> str:
    """Hash a taxonomy's canonical point rules independent of input ordering."""

    if not rules:
        raise MatcherDslError("taxonomy matcher rules cannot be empty")
    canonical_rules: dict[str, Any] = {}
    for code, value in rules.items():
        validate_materialized_rule(value, point_spec=point_spec)
        point_id = str(value["rule"]["point_id"])
        if code != point_id:
            raise MatcherDslError(
                f"taxonomy rule key {code!r} does not match point_id {point_id!r}"
            )
        canonical_rules[code] = json.loads(
            canonical_materialized_rule(value, point_spec=point_spec)
        )
    payload = {"rules": canonical_rules}
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _validate_expression(
    expression: Any,
    *,
    predicates: set[str],
    views: set[str],
    term_sets: set[str],
    term_set_values: Mapping[str, Sequence[str]],
    predicate_scopes: Mapping[str, set[str]],
    scope: set[str] | None = None,
    depth: int = 0,
) -> None:
    scope = scope or set()
    if depth > MAX_EXPRESSION_DEPTH:
        raise MatcherDslError("matcher expression exceeds maximum depth")
    if isinstance(expression, (bool, int, float, str)) or expression is None:
        return
    if not isinstance(expression, dict):
        raise MatcherDslError("expression must be a scalar or object")
    op = expression.get("op")
    if op not in EXPRESSION_OPS:
        raise MatcherDslError(f"unknown matcher operator: {op!r}")
    required_by_op = {
        "all": {"op", "args"},
        "any": {"op", "args"},
        "not": {"op", "arg"},
        "ref": {"op", "name"},
        "field_eq": {"op", "field", "value"},
        "contains_any": {"op", "view", "terms"},
        "term_count": {"op", "view", "terms"},
        "compare": {"op", "cmp", "left", "right"},
        "regex_any": {"op", "view", "patterns"},
        "regex_fullmatch": {"op", "view", "pattern"},
        "terms_near": {"op", "view", "left_terms", "right_terms"},
        "chinese_count": {"op", "view"},
        "source_any": {"op", "expr"},
        "context_regex_any": {"op", "view", "pattern", "where"},
    }
    _require_keys(expression, required_by_op[op], f"{op} expression")
    if op in {"all", "any"}:
        _reject_unknown(expression, {"op", "args"}, f"{op} expression")
        args = expression.get("args")
        if not isinstance(args, list) or not args or len(args) > 64:
            raise MatcherDslError(f"{op}.args must contain 1..64 expressions")
        for item in args:
            if not isinstance(item, dict):
                raise MatcherDslError(f"{op}.args entries must be expressions")
            _validate_expression(
                item,
                predicates=predicates,
                views=views,
                term_sets=term_sets,
                term_set_values=term_set_values,
                predicate_scopes=predicate_scopes,
                scope=scope,
                depth=depth + 1,
            )
    elif op == "not":
        _reject_unknown(expression, {"op", "arg"}, "not expression")
        if not isinstance(expression.get("arg"), dict):
            raise MatcherDslError("not.arg must be an expression")
        _validate_expression(
            expression.get("arg"),
            predicates=predicates,
            views=views,
            term_sets=term_sets,
            term_set_values=term_set_values,
            predicate_scopes=predicate_scopes,
            scope=scope,
            depth=depth + 1,
        )
    elif op == "ref":
        _reject_unknown(expression, {"op", "name"}, "ref expression")
        if expression.get("name") not in predicates:
            raise MatcherDslError(f"unknown predicate: {expression.get('name')!r}")
        missing_scope = predicate_scopes[str(expression["name"])] - scope
        if missing_scope:
            raise MatcherDslError(
                f"predicate {expression['name']!r} requires local scope {sorted(missing_scope)}"
            )
    elif op == "field_eq":
        _reject_unknown(
            expression, {"op", "field", "coerce", "value"}, "field_eq expression"
        )
        if expression.get("field") not in {"content_type", "media_type"}:
            raise MatcherDslError(f"unknown matcher field: {expression.get('field')!r}")
        if expression.get("coerce") not in {None, "int", "str"}:
            raise MatcherDslError("field_eq.coerce must be int or str")
        if expression.get("coerce") == "int" and (
            isinstance(expression.get("value"), bool)
            or not isinstance(expression.get("value"), int)
        ):
            raise MatcherDslError("integer field_eq requires an integer value")
        if expression.get("coerce") == "str" and not isinstance(
            expression.get("value"), str
        ):
            raise MatcherDslError("string field_eq requires a string value")
    elif op in {
        "contains_any",
        "term_count",
        "regex_any",
        "regex_fullmatch",
        "terms_near",
        "chinese_count",
        "context_regex_any",
    }:
        allowed_keys = {
            "contains_any": {"op", "view", "terms"},
            "term_count": {"op", "view", "terms", "mode"},
            "regex_any": {"op", "view", "patterns", "flags"},
            "regex_fullmatch": {"op", "view", "pattern", "flags"},
            "terms_near": {
                "op",
                "view",
                "left_terms",
                "right_terms",
                "distance",
            },
            "chinese_count": {"op", "view"},
            "context_regex_any": {
                "op",
                "view",
                "pattern",
                "flags",
                "transforms",
                "before",
                "after",
                "around_before",
                "around_after",
                "where",
            },
        }[op]
        _reject_unknown(expression, allowed_keys, f"{op} expression")
        view = expression.get("view")
        if isinstance(view, str) and view.startswith("$"):
            if view[1:] not in scope:
                raise MatcherDslError(f"local matcher view is out of scope: {view!r}")
        elif view not in views:
            raise MatcherDslError(f"unknown matcher view: {view!r}")
        for key in ("terms", "left_terms", "right_terms", "patterns"):
            value = expression.get(key)
            if isinstance(value, str) and value not in term_sets:
                raise MatcherDslError(f"unknown term set: {value!r}")
            if value is not None and not isinstance(value, (str, list)):
                raise MatcherDslError(f"{op}.{key} must be a term-set name or list")
            if isinstance(value, list) and (
                not value
                or len(value) > MAX_TERMS_PER_SET
                or not all(
                    isinstance(item, str) and 0 < len(item) <= MAX_TERM_LENGTH
                    for item in value
                )
            ):
                raise MatcherDslError(f"{op}.{key} contains invalid inline terms")
        if op == "term_count" and expression.get("mode", "distinct") not in {
            "distinct",
            "occurrences",
        }:
            raise MatcherDslError("term_count.mode must be distinct or occurrences")
        if op == "terms_near":
            _bounded_int(
                expression.get("distance", 50),
                label="terms_near.distance",
                minimum=0,
                maximum=MAX_CONTEXT_WINDOW,
            )
        if op in {"regex_any", "regex_fullmatch"}:
            _validate_flags(expression.get("flags"), f"{op}.flags")
            patterns_value = expression.get("patterns")
            patterns = (
                term_set_values[patterns_value]
                if isinstance(patterns_value, str)
                else patterns_value
            )
            if op == "regex_fullmatch":
                patterns = [expression.get("pattern")]
            if not isinstance(patterns, Sequence) or not patterns:
                raise MatcherDslError(f"{op} requires at least one pattern")
            for pattern in patterns:
                _compile_pattern(pattern, expression.get("flags"), op)
        if op == "context_regex_any":
            _compile_pattern(
                expression.get("pattern"),
                expression.get("flags"),
                "context_regex_any.pattern",
            )
            transforms = expression.get("transforms", [])
            if (
                not isinstance(transforms, list)
                or len(transforms) > 4
                or any(
                    item not in {"lower", "remove_whitespace"} for item in transforms
                )
            ):
                raise MatcherDslError("invalid contextual regex transforms")
            for key in ("before", "after", "around_before", "around_after"):
                _bounded_int(
                    expression.get(key, 0),
                    label=f"context_regex_any.{key}",
                    minimum=0,
                    maximum=MAX_CONTEXT_WINDOW,
                )
            if not isinstance(expression.get("where"), dict):
                raise MatcherDslError("context_regex_any.where must be an expression")
            _validate_expression(
                expression.get("where"),
                predicates=predicates,
                views=views,
                term_sets=term_sets,
                term_set_values=term_set_values,
                predicate_scopes=predicate_scopes,
                scope=scope | {"match", "before", "after", "nearby", "around"},
                depth=depth + 1,
            )
    elif op == "compare":
        _reject_unknown(
            expression, {"op", "cmp", "left", "right"}, "compare expression"
        )
        if expression.get("cmp") not in {"eq", "gt", "gte", "lt", "lte"}:
            raise MatcherDslError(f"unknown comparator: {expression.get('cmp')!r}")
        _validate_expression(
            expression.get("left"),
            predicates=predicates,
            views=views,
            term_sets=term_sets,
            term_set_values=term_set_values,
            predicate_scopes=predicate_scopes,
            scope=scope,
            depth=depth + 1,
        )
        _validate_expression(
            expression.get("right"),
            predicates=predicates,
            views=views,
            term_sets=term_sets,
            term_set_values=term_set_values,
            predicate_scopes=predicate_scopes,
            scope=scope,
            depth=depth + 1,
        )
    elif op == "source_any":
        _reject_unknown(expression, {"op", "expr"}, "source_any expression")
        if not isinstance(expression.get("expr"), dict):
            raise MatcherDslError("source_any.expr must be an expression")
        _validate_expression(
            expression.get("expr"),
            predicates=predicates,
            views=views,
            term_sets=term_sets,
            term_set_values=term_set_values,
            predicate_scopes=predicate_scopes,
            scope=scope | {"source_text", "source_name"},
            depth=depth + 1,
        )


def _validate_bundle(
    bundle: Mapping[str, Any],
    *,
    expected_point_scenes: Mapping[str, set[str]],
) -> None:
    _reject_nonfinite_floats(bundle)
    allowed_top_level = {
        "dsl_version",
        "engine_version",
        "legacy_matcher_sha256",
        "normalization",
        "views",
        "term_sets",
        "predicates",
        "predicate_scopes",
        "scoring",
        "thresholds",
        "rules",
    }
    unknown = set(bundle) - allowed_top_level
    if unknown:
        raise MatcherDslError(f"unknown matcher bundle keys: {sorted(unknown)}")
    if bundle.get("dsl_version") != 1:
        raise MatcherDslError("unsupported matcher dsl_version")
    _require_keys(bundle, allowed_top_level, "matcher bundle")
    if bundle.get("engine_version") != ENGINE_VERSION:
        raise MatcherDslError("matcher bundle engine_version does not match engine")
    legacy_sha = str(bundle.get("legacy_matcher_sha256") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", legacy_sha):
        raise MatcherDslError("legacy_matcher_sha256 must be lowercase SHA-256")
    normalization = bundle.get("normalization")
    if not isinstance(normalization, dict):
        raise MatcherDslError("normalization must be an object")
    _require_keys(normalization, {"ordered_replacements"}, "normalization")
    _reject_unknown(normalization, {"ordered_replacements"}, "normalization")
    replacements = normalization.get("ordered_replacements")
    if (
        not isinstance(replacements, list)
        or not replacements
        or len(replacements) > 128
    ):
        raise MatcherDslError("ordered_replacements must contain 1..128 pairs")
    for replacement in replacements:
        if (
            not isinstance(replacement, list)
            or len(replacement) != 2
            or not all(
                isinstance(item, str) and 0 < len(item) <= MAX_TERM_LENGTH
                for item in replacement
            )
        ):
            raise MatcherDslError("ordered_replacements entries must be string pairs")
    views_value = bundle.get("views")
    terms_value = bundle.get("term_sets")
    predicates_value = bundle.get("predicates")
    predicate_scopes_value = bundle.get("predicate_scopes")
    rules_value = bundle.get("rules")
    if not isinstance(views_value, dict) or not isinstance(terms_value, dict):
        raise MatcherDslError("views and term_sets must be objects")
    if (
        not isinstance(predicates_value, dict)
        or not isinstance(predicate_scopes_value, dict)
        or not isinstance(rules_value, list)
    ):
        raise MatcherDslError("predicates must be an object and rules a list")
    if not views_value or len(views_value) > 64:
        raise MatcherDslError("views must contain 1..64 definitions")
    if not terms_value or len(terms_value) > MAX_TERM_SETS:
        raise MatcherDslError(f"term_sets must contain 1..{MAX_TERM_SETS} sets")
    if not predicates_value or len(predicates_value) > 512:
        raise MatcherDslError("predicates must contain 1..512 definitions")
    views = set(views_value)
    term_sets = set(terms_value)
    predicates = set(predicates_value)
    if set(predicate_scopes_value) - predicates:
        raise MatcherDslError("predicate_scopes contains unknown predicates")
    predicate_scopes: dict[str, set[str]] = {}
    for name in predicates:
        declared_scope = predicate_scopes_value.get(name, [])
        if (
            not isinstance(declared_scope, list)
            or len(declared_scope) > 2
            or len(declared_scope) != len(set(declared_scope))
            or any(
                item not in {"source_text", "source_name"} for item in declared_scope
            )
        ):
            raise MatcherDslError(f"invalid predicate scope for {name!r}")
        predicate_scopes[name] = set(declared_scope)
    for name, terms in terms_value.items():
        if not isinstance(name, str) or not name or len(name) > 128:
            raise MatcherDslError("term-set names must be non-empty strings")
        if (
            not isinstance(terms, list)
            or not terms
            or len(terms) > MAX_TERMS_PER_SET
            or not all(
                isinstance(term, str) and 0 < len(term) <= MAX_TERM_LENGTH
                for term in terms
            )
        ):
            raise MatcherDslError(f"term set {name!r} must contain non-empty strings")
    for name, definition in views_value.items():
        if not isinstance(name, str) or not name or len(name) > 128:
            raise MatcherDslError("view names must be non-empty strings")
        if not isinstance(definition, dict) or definition.get("op") not in {
            "source",
            "concat",
            "select",
        }:
            raise MatcherDslError("view definitions must use source, concat, or select")
        if definition["op"] == "source":
            _require_keys(definition, {"op", "source"}, "source view")
            _reject_unknown(definition, {"op", "source", "transforms"}, "source view")
            if definition.get("source") not in {
                "row.desc",
                "transcript.text",
                "ocr.combined_text",
                "visual.summary",
            }:
                raise MatcherDslError(
                    f"unknown source binding: {definition.get('source')!r}"
                )
            transforms = definition.get("transforms", [])
            if (
                not isinstance(transforms, list)
                or len(transforms) > 4
                or any(
                    item not in {"strip_hashtags", "canonical"} for item in transforms
                )
            ):
                raise MatcherDslError("invalid source-view transforms")
        elif definition["op"] == "concat":
            _require_keys(definition, {"op", "views"}, "concat view")
            _reject_unknown(definition, {"op", "views", "separator"}, "concat view")
            members = definition.get("views")
            if (
                not isinstance(members, list)
                or not members
                or len(members) > 16
                or any(member not in views for member in members)
            ):
                raise MatcherDslError("concat view contains invalid members")
            separator = definition.get("separator", "")
            if not isinstance(separator, str) or len(separator) > 16:
                raise MatcherDslError("concat separator must be at most 16 characters")
        else:
            _require_keys(definition, {"op", "when", "then", "else"}, "select view")
            _reject_unknown(definition, {"op", "when", "then", "else"}, "select view")
            if (
                definition.get("then") not in views
                or definition.get("else") not in views
            ):
                raise MatcherDslError("select view branches must name declared views")
            if not isinstance(definition.get("when"), dict):
                raise MatcherDslError("select view when must be an expression")
            _validate_expression(
                definition.get("when"),
                predicates=set(predicates_value),
                views=views,
                term_sets=set(terms_value),
                term_set_values=terms_value,
                predicate_scopes=predicate_scopes,
            )
    for name, expression in predicates_value.items():
        if not isinstance(name, str) or not name or len(name) > 128:
            raise MatcherDslError("predicate names must be non-empty strings")
        if not isinstance(expression, dict):
            raise MatcherDslError(f"predicate {name!r} must be an expression")
        _validate_expression(
            expression,
            predicates=predicates,
            views=views,
            term_sets=term_sets,
            term_set_values=terms_value,
            predicate_scopes=predicate_scopes,
            scope=predicate_scopes[name],
        )
    predicate_references: dict[str, set[str]] = {name: set() for name in predicates}

    def collect_references(expression: Any, target: set[str]) -> None:
        if not isinstance(expression, dict):
            return
        if expression.get("op") == "ref":
            target.add(str(expression["name"]))
        for value in expression.values():
            if isinstance(value, dict):
                collect_references(value, target)
            elif isinstance(value, list):
                for item in value:
                    collect_references(item, target)

    for name, expression in predicates_value.items():
        collect_references(expression, predicate_references[name])

    def visit_predicate(name: str, active: tuple[str, ...]) -> None:
        if name in active:
            raise MatcherDslError(
                f"cyclic predicate reference: {' -> '.join((*active, name))}"
            )
        for child in predicate_references[name]:
            visit_predicate(child, (*active, name))

    for name in predicates:
        visit_predicate(name, ())

    dependency_graph: dict[str, set[str]] = {
        **{f"predicate:{name}": set() for name in predicates},
        **{f"view:{name}": set() for name in views},
    }

    def collect_dependencies(expression: Any, target: set[str]) -> None:
        if not isinstance(expression, dict):
            return
        if expression.get("op") == "ref":
            target.add(f"predicate:{expression['name']}")
        view_name = expression.get("view")
        if isinstance(view_name, str) and not view_name.startswith("$"):
            target.add(f"view:{view_name}")
        for value in expression.values():
            if isinstance(value, dict):
                collect_dependencies(value, target)
            elif isinstance(value, list):
                for item in value:
                    collect_dependencies(item, target)

    for name, expression in predicates_value.items():
        collect_dependencies(expression, dependency_graph[f"predicate:{name}"])
    for name, definition in views_value.items():
        target = dependency_graph[f"view:{name}"]
        if definition["op"] == "concat":
            target.update(f"view:{member}" for member in definition["views"])
        elif definition["op"] == "select":
            target.update({f"view:{definition['then']}", f"view:{definition['else']}"})
            collect_dependencies(definition["when"], target)

    resolved_nodes: set[str] = set()

    def visit_dependency(node: str, active: tuple[str, ...]) -> None:
        if node in resolved_nodes:
            return
        if node in active:
            raise MatcherDslError(
                f"cyclic matcher dependency: {' -> '.join((*active, node))}"
            )
        for child in dependency_graph[node]:
            visit_dependency(child, (*active, node))
        resolved_nodes.add(node)

    for node in dependency_graph:
        visit_dependency(node, ())
    for name, expression in predicates_value.items():
        _expression_kind(expression, predicates_value, active=(name,))
    for name, definition in views_value.items():
        if definition["op"] == "select":
            _require_expression_kind(
                definition["when"],
                "bool",
                predicates_value,
                f"view {name}.when",
            )

    scoring = bundle.get("scoring")
    if not isinstance(scoring, dict):
        raise MatcherDslError("scoring must be an object")
    scoring_keys = {
        "linkage_explicit",
        "linkage_implicit",
        "force_linkage_points",
        "video_scores",
        "desc_only_video_score",
        "prominence_multi",
        "prominence_video",
        "prominence_desc",
        "caps",
        "no_video_cap",
    }
    _reject_unknown(scoring, scoring_keys, "scoring")
    _require_keys(scoring, scoring_keys, "scoring")
    for key in scoring_keys - {"force_linkage_points", "video_scores", "caps"}:
        _bounded_int(scoring.get(key), label=f"scoring.{key}", minimum=0, maximum=100)
    force_points = scoring.get("force_linkage_points")
    required_force_points = {f"M{index}" for index in range(1, 7)}
    if not isinstance(force_points, list) or set(force_points) != required_force_points:
        raise MatcherDslError("force_linkage_points must be exactly M1..M6")
    for key in ("video_scores", "caps"):
        values = scoring.get(key)
        if not isinstance(values, dict):
            raise MatcherDslError(f"scoring.{key} must be an object")
        _reject_unknown(values, {"V0", "V1", "V2", "V3"}, f"scoring.{key}")
        if set(values) != {"V0", "V1", "V2", "V3"}:
            raise MatcherDslError(f"scoring.{key} must define V0..V3")
        for evidence_level, value in values.items():
            _bounded_int(
                value,
                label=f"scoring.{key}.{evidence_level}",
                minimum=0,
                maximum=100,
            )
    expected_video_scores = {"V0": 0, "V1": 6, "V2": 16, "V3": 20}
    expected_caps = {"V0": 0, "V1": 74, "V2": 90, "V3": 100}
    if scoring["video_scores"] != expected_video_scores:
        raise MatcherDslError("video_scores do not match matcher-v3 contract")
    if scoring["caps"] != expected_caps:
        raise MatcherDslError("caps do not match matcher-v3 contract")
    if not (
        scoring["linkage_explicit"] >= scoring["linkage_implicit"]
        and scoring["prominence_multi"]
        >= scoring["prominence_video"]
        >= scoring["prominence_desc"]
        and scoring["no_video_cap"] <= scoring["caps"]["V1"]
    ):
        raise MatcherDslError("scoring weights violate matcher-v3 ordering")

    thresholds = bundle.get("thresholds")
    if not isinstance(thresholds, dict):
        raise MatcherDslError("thresholds must be an object")
    _reject_unknown(
        thresholds,
        {"included_min", "review_min", "max_secondary"},
        "thresholds",
    )
    _require_keys(
        thresholds,
        {"included_min", "review_min", "max_secondary"},
        "thresholds",
    )
    included_min = _bounded_int(
        thresholds.get("included_min"),
        label="thresholds.included_min",
        minimum=0,
        maximum=100,
    )
    review_min = _bounded_int(
        thresholds.get("review_min"),
        label="thresholds.review_min",
        minimum=0,
        maximum=100,
    )
    max_secondary = _bounded_int(
        thresholds.get("max_secondary"),
        label="thresholds.max_secondary",
        minimum=0,
        maximum=10,
    )
    if review_min > included_min:
        raise MatcherDslError("review_min cannot exceed included_min")
    if max_secondary != 2:
        raise MatcherDslError(
            "matcher-dsl-engine-v1 requires thresholds.max_secondary=2"
        )

    point_ids: list[str] = []
    for rule in rules_value:
        if not isinstance(rule, dict):
            raise MatcherDslError("rules must be objects")
        _reject_unknown(
            rule,
            {
                "point_id",
                "when",
                "scene",
                "evidence",
                "explain",
                "score",
                "priority",
            },
            "rule",
        )
        _require_keys(
            rule,
            {
                "point_id",
                "when",
                "scene",
                "evidence",
                "explain",
                "score",
                "priority",
            },
            "rule",
        )
        point_id = str(rule.get("point_id") or "")
        point_ids.append(point_id)
        if point_id not in expected_point_scenes:
            raise MatcherDslError(f"unknown point id: {point_id!r}")
        if not isinstance(rule.get("when"), dict):
            raise MatcherDslError("rule.when must be an expression")
        _validate_expression(
            rule.get("when"),
            predicates=predicates,
            views=views,
            term_sets=term_sets,
            term_set_values=terms_value,
            predicate_scopes=predicate_scopes,
        )
        _require_expression_kind(
            rule["when"], "bool", predicates_value, f"rule {point_id}.when"
        )
        scene = rule.get("scene")
        scene_values: set[str] = set()
        if isinstance(scene, str):
            if scene not in SCENES:
                raise MatcherDslError(f"unknown scene: {scene!r}")
            scene_values.add(scene)
        elif isinstance(scene, dict):
            _require_keys(scene, {"cases", "default"}, "scene")
            _reject_unknown(scene, {"cases", "default"}, "scene")
            if scene.get("default") not in SCENES:
                raise MatcherDslError(
                    f"unknown default scene: {scene.get('default')!r}"
                )
            scene_values.add(str(scene["default"]))
            cases = scene.get("cases")
            if not isinstance(cases, list) or not cases or len(cases) > 10:
                raise MatcherDslError("scene.cases must contain 1..10 cases")
            for case in cases:
                if not isinstance(case, dict):
                    raise MatcherDslError("scene cases must be objects")
                _reject_unknown(case, {"when", "value"}, "scene case")
                _require_keys(case, {"when", "value"}, "scene case")
                if case.get("value") not in SCENES:
                    raise MatcherDslError(f"unknown scene: {case.get('value')!r}")
                scene_values.add(str(case["value"]))
                if not isinstance(case.get("when"), dict):
                    raise MatcherDslError("scene case when must be an expression")
                _validate_expression(
                    case.get("when"),
                    predicates=predicates,
                    views=views,
                    term_sets=term_sets,
                    term_set_values=terms_value,
                    predicate_scopes=predicate_scopes,
                )
                _require_expression_kind(
                    case["when"],
                    "bool",
                    predicates_value,
                    f"rule {point_id}.scene.when",
                )
        else:
            raise MatcherDslError("scene must be a scene or ordered case object")
        if scene_values != expected_point_scenes[point_id]:
            raise MatcherDslError(
                f"point {point_id} must emit exactly "
                f"{sorted(expected_point_scenes[point_id])}"
            )
        evidence = rule.get("evidence")
        if not isinstance(evidence, dict):
            raise MatcherDslError("rule evidence must be an object")
        _reject_unknown(evidence, {"terms", "source_expr", "reason"}, "evidence")
        _require_keys(evidence, {"terms", "reason"}, "evidence")
        terms_name = evidence.get("terms")
        if terms_name not in term_sets:
            raise MatcherDslError(f"unknown evidence term set: {terms_name!r}")
        source_expr = evidence.get("source_expr")
        if source_expr is not None:
            if not isinstance(source_expr, dict):
                raise MatcherDslError("evidence.source_expr must be an expression")
            _validate_expression(
                source_expr,
                predicates=predicates,
                views=views,
                term_sets=term_sets,
                term_set_values=terms_value,
                predicate_scopes=predicate_scopes,
                scope={"source_text", "source_name"},
            )
            _require_expression_kind(
                source_expr,
                "bool",
                predicates_value,
                f"rule {point_id}.evidence.source_expr",
            )
        reason = evidence.get("reason")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 500:
            raise MatcherDslError("evidence.reason must be 1..500 characters")
        explain = rule.get("explain")
        if not isinstance(explain, dict):
            raise MatcherDslError("rule.explain must be an object")
        explain_keys = {
            "positive_evidence",
            "negative_evidence",
            "boundary_rules",
        }
        _reject_unknown(explain, explain_keys, "rule explain")
        _require_keys(explain, explain_keys, "rule explain")
        for key in explain_keys:
            values = explain[key]
            if (
                not isinstance(values, list)
                or not values
                or len(values) > 100
                or len(values) != len(set(values))
                or any(
                    not isinstance(item, str)
                    or item != item.strip()
                    or not item
                    or len(item) > 500
                    for item in values
                )
            ):
                raise MatcherDslError(
                    f"rule.explain.{key} must contain unique trimmed strings"
                )
        score = rule.get("score")
        if not isinstance(score, dict):
            raise MatcherDslError("rule.score must be an object")
        _reject_unknown(score, {"semantic_fit", "user_benefit"}, "rule score")
        _require_keys(score, {"semantic_fit", "user_benefit"}, "rule score")
        for key in ("semantic_fit", "user_benefit"):
            _bounded_int(
                score.get(key),
                label=f"rule.score.{key}",
                minimum=0,
                maximum=100,
            )
        _bounded_int(
            rule.get("priority"),
            label="rule.priority",
            minimum=0,
            maximum=100,
        )
    expected_point_ids = set(expected_point_scenes)
    if (
        len(point_ids) != len(expected_point_ids)
        or set(point_ids) != expected_point_ids
    ):
        missing = sorted(expected_point_ids - set(point_ids))
        duplicates = sorted({item for item in point_ids if point_ids.count(item) > 1})
        raise MatcherDslError(
            "matcher rules do not match the expected point set; "
            f"missing={missing}, duplicates={duplicates}"
        )


def validate_bundle(
    bundle: Mapping[str, Any],
    *,
    point_spec: Mapping[str, set[str]] = V5_1_POINT_SPEC,
) -> None:
    _validate_bundle(bundle, expected_point_scenes=point_spec)


class _Matcher:
    def __init__(
        self,
        bundle: Mapping[str, Any],
        row: Mapping[str, Any],
        transcript: Mapping[str, Any],
        ocr: Mapping[str, Any],
        evidence: str,
        visual: Mapping[str, Any],
    ) -> None:
        self.bundle = bundle
        self.row = row
        self.transcript = transcript
        self.ocr = ocr
        self.evidence = evidence
        self.visual = visual
        self._views: dict[str, str] = {}

    def _canonical(self, text: str) -> str:
        for old, new in self.bundle["normalization"]["ordered_replacements"]:
            text = text.replace(old, new)
        return text

    def _view(self, name: str, local: Mapping[str, Any]) -> str:
        if name.startswith("$"):
            return str(local.get(name[1:], ""))
        if name in self._views:
            return self._views[name]
        definition = self.bundle["views"].get(name)
        if definition is None:
            raise MatcherDslError(f"unknown view at runtime: {name!r}")
        op = definition["op"]
        if op == "source":
            source = definition["source"]
            if source == "row.desc":
                value = str(self.row.get("desc") or "")
            elif source == "transcript.text":
                value = str(self.transcript.get("text") or "")
            elif source == "ocr.combined_text":
                value = str(self.ocr.get("combined_text") or "")
            elif source == "visual.summary":
                value = str(self.visual.get("summary") or "")
            else:
                raise MatcherDslError(f"unknown source binding: {source!r}")
            for transform in definition.get("transforms", []):
                if transform == "strip_hashtags":
                    value = re.sub(r"#([^#\s]+)", " ", value.replace("＃", "#"))
                elif transform == "canonical":
                    value = self._canonical(value)
                else:
                    raise MatcherDslError(f"unknown normalizer: {transform!r}")
        elif op == "concat":
            value = str(definition.get("separator", "")).join(
                self._view(item, local) for item in definition["views"]
            )
        else:
            branch = "then" if bool(self._eval(definition["when"], local)) else "else"
            value = self._view(str(definition[branch]), local)
        self._views[name] = value
        return value

    def _terms(self, value: Any) -> list[str]:
        if isinstance(value, str):
            return [str(item) for item in self.bundle["term_sets"][value]]
        if isinstance(value, list):
            return [str(item) for item in value]
        raise MatcherDslError("terms must be a term-set name or list")

    def _contains_any(self, text: str, terms: Sequence[str]) -> bool:
        low = text.lower()
        return any(term.lower() in low for term in terms)

    def _eval(self, expression: Any, local: Mapping[str, Any] | None = None) -> Any:
        local = local or {}
        if not isinstance(expression, dict):
            return expression
        op = expression["op"]
        if op == "all":
            return all(bool(self._eval(item, local)) for item in expression["args"])
        if op == "any":
            return any(bool(self._eval(item, local)) for item in expression["args"])
        if op == "not":
            return not bool(self._eval(expression["arg"], local))
        if op == "ref":
            return self._eval(self.bundle["predicates"][expression["name"]], local)
        if op == "field_eq":
            value: Any = self.row.get(expression["field"])
            if expression.get("coerce") == "int":
                try:
                    value = int(value or 0)
                except (TypeError, ValueError, OverflowError) as error:
                    raise MatcherDslError(
                        f"field {expression['field']!r} is not an integer"
                    ) from error
            elif expression.get("coerce") == "str":
                value = str(value or "")
            return value == expression.get("value")
        if op == "contains_any":
            return self._contains_any(
                self._view(expression["view"], local),
                self._terms(expression["terms"]),
            )
        if op == "term_count":
            text = self._view(expression["view"], local).lower()
            terms = self._terms(expression["terms"])
            if expression.get("mode", "distinct") == "occurrences":
                return sum(text.count(term.lower()) for term in terms)
            return sum(1 for term in terms if term.lower() in text)
        if op == "compare":
            left = self._eval(expression["left"], local)
            right = self._eval(expression["right"], local)
            try:
                comparator = expression["cmp"]
                if comparator == "eq":
                    return left == right
                if comparator == "gt":
                    return left > right
                if comparator == "gte":
                    return left >= right
                if comparator == "lt":
                    return left < right
                return left <= right
            except TypeError as error:
                raise MatcherDslError(
                    "comparison operands have incompatible kinds"
                ) from error
        if op in {"regex_any", "regex_fullmatch"}:
            text = self._view(expression["view"], local)
            flags = re.IGNORECASE if "i" in str(expression.get("flags", "")) else 0
            patterns = (
                self._terms(expression["patterns"])
                if "patterns" in expression
                else [str(expression["pattern"])]
            )
            matcher = re.fullmatch if op == "regex_fullmatch" else re.search
            return any(
                matcher(pattern, text, flags) is not None for pattern in patterns
            )
        if op == "terms_near":
            text = self._view(expression["view"], local).lower()
            left_positions = sorted(
                match.start()
                for term in self._terms(expression["left_terms"])
                for match in re.finditer(re.escape(term.lower()), text)
            )
            right_positions = sorted(
                match.start()
                for term in self._terms(expression["right_terms"])
                for match in re.finditer(re.escape(term.lower()), text)
            )
            distance = int(expression.get("distance", 50))
            left_index = 0
            right_index = 0
            while left_index < len(left_positions) and right_index < len(
                right_positions
            ):
                left = left_positions[left_index]
                right = right_positions[right_index]
                if abs(left - right) <= distance:
                    return True
                if left < right:
                    left_index += 1
                else:
                    right_index += 1
            return False
        if op == "chinese_count":
            return len(
                re.findall(r"[\u4e00-\u9fff]", self._view(expression["view"], local))
            )
        if op == "source_any":
            return any(
                bool(
                    self._eval(
                        expression["expr"],
                        {
                            **local,
                            "source_name": source,
                            "source_text": self._view(source, local),
                        },
                    )
                )
                for source in SOURCES
            )
        if op == "context_regex_any":
            text = self._view(expression["view"], local)
            for transform in expression.get("transforms", []):
                if transform == "lower":
                    text = text.lower()
                elif transform == "remove_whitespace":
                    text = re.sub(r"\s+", "", text)
                else:
                    raise MatcherDslError(
                        f"unknown contextual transform: {transform!r}"
                    )
            flags = re.IGNORECASE if "i" in str(expression.get("flags", "")) else 0
            before_width = int(expression.get("before", 0))
            after_width = int(expression.get("after", 0))
            around_before = int(expression.get("around_before", before_width))
            around_after = int(expression.get("around_after", after_width))
            for match in re.finditer(str(expression["pattern"]), text, flags):
                before = text[max(0, match.start() - before_width) : match.start()]
                after = text[match.end() : min(len(text), match.end() + after_width)]
                context = {
                    **local,
                    "match": match.group(),
                    "before": before,
                    "after": after,
                    "nearby": before + after,
                    "around": text[
                        max(0, match.start() - around_before) : min(
                            len(text), match.end() + around_after
                        )
                    ],
                }
                if bool(self._eval(expression["where"], context)):
                    return True
            return False
        raise MatcherDslError(f"unsupported matcher operator: {op!r}")

    def _source_flags(self, rule: Mapping[str, Any]) -> tuple[bool, bool, bool, bool]:
        evidence = rule["evidence"]
        expression = evidence.get("source_expr")
        if expression is None:
            terms = self._terms(evidence["terms"])
            return tuple(
                self._contains_any(self._view(source, {}), terms) for source in SOURCES
            )  # type: ignore[return-value]
        return tuple(
            bool(
                self._eval(
                    expression,
                    {
                        "source_name": source,
                        "source_text": self._view(source, {}),
                    },
                )
            )
            for source in SOURCES
        )  # type: ignore[return-value]

    def _scene(self, value: Any) -> str:
        if isinstance(value, str):
            return value
        for case in value.get("cases", []):
            if bool(self._eval(case["when"])):
                return str(case["value"])
        return str(value["default"])

    def _score(
        self,
        rule: Mapping[str, Any],
        flags: tuple[bool, bool, bool, bool],
    ) -> tuple[int, dict[str, int]]:
        in_asr, in_ocr, in_visual, in_desc = flags
        point_id = str(rule["point_id"])
        scoring = self.bundle["scoring"]
        explicit_dcar = bool(self._eval({"op": "ref", "name": "explicit_dcar"}))
        linkage = (
            int(scoring["linkage_explicit"])
            if explicit_dcar or point_id in scoring["force_linkage_points"]
            else int(scoring["linkage_implicit"])
        )
        label_has_video_evidence = in_asr or in_ocr or in_visual
        if label_has_video_evidence:
            video_score = int(scoring["video_scores"][self.evidence])
        else:
            video_score = int(scoring["desc_only_video_score"] if in_desc else 0)
        folded_ocr = in_ocr or in_visual
        source_count = sum((in_asr, folded_ocr, in_desc))
        if source_count >= 2 and (in_asr or folded_ocr):
            prominence = int(scoring["prominence_multi"])
        elif in_asr or folded_ocr:
            prominence = int(scoring["prominence_video"])
        else:
            prominence = int(scoring["prominence_desc"])
        dimensions = {
            "semantic_fit": int(rule["score"]["semantic_fit"]),
            "dcar_linkage": linkage,
            "video_evidence": video_score,
            "user_benefit": int(rule["score"]["user_benefit"]),
            "narrative_prominence": prominence,
        }
        cap = int(scoring["caps"][self.evidence])
        if not label_has_video_evidence:
            cap = min(cap, int(scoring["no_video_cap"]))
        return min(sum(dimensions.values()), cap), dimensions

    def _snippet(self, text: str, terms: Sequence[str]) -> str:
        low = text.lower()
        positions = [
            low.find(term.lower()) for term in terms if low.find(term.lower()) >= 0
        ]
        start = max(0, (min(positions) if positions else 0) - 30)
        return re.sub(r"\s+", " ", text[start : start + 150]).strip()

    def run(self) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        for rule in self.bundle["rules"]:
            if not bool(self._eval(rule["when"])):
                continue
            flags = self._source_flags(rule)
            score, dimensions = self._score(rule, flags)
            source_index = next((index for index, flag in enumerate(flags) if flag), 3)
            source = SOURCES[source_index]
            source_label = {
                "asr": "ASR",
                "ocr": "OCR",
                "visual": "关键帧画面语义",
                "desc": "正文",
            }[source]
            terms = self._terms(rule["evidence"]["terms"])
            matches.append(
                {
                    "id": rule["point_id"],
                    "scene": self._scene(rule["scene"]),
                    "score": score,
                    "dimensions": dimensions,
                    "reason": rule["evidence"]["reason"],
                    "evidence_snippet": self._snippet(self._view(source, {}), terms),
                    "source": source_label,
                }
            )
        unique: dict[str, dict[str, Any]] = {}
        for item in matches:
            previous = unique.get(str(item["id"]))
            if previous is None or int(item["score"]) > int(previous["score"]):
                unique[str(item["id"])] = item
        priorities = {
            str(rule["point_id"]): int(rule["priority"])
            for rule in self.bundle["rules"]
        }
        return sorted(
            unique.values(),
            key=lambda item: (
                -int(item["score"]),
                priorities[str(item["id"])],
                str(item["id"]),
            ),
        )


def match_points(
    bundle: Mapping[str, Any],
    row: Mapping[str, Any],
    transcript: Mapping[str, Any],
    ocr: Mapping[str, Any],
    evidence: str,
    visual: Mapping[str, Any] | None = None,
    *,
    point_spec: Mapping[str, set[str]] = V5_1_POINT_SPEC,
) -> list[dict[str, Any]]:
    """Execute a validated matcher bundle without database or global state."""

    validate_bundle(bundle, point_spec=point_spec)
    if evidence not in {"V0", "V1", "V2", "V3"}:
        raise MatcherDslError(f"unknown evidence level: {evidence!r}")
    return _Matcher(bundle, row, transcript, ocr, evidence, visual or {}).run()


class MaterializedMatcher:
    """Validated immutable runtime assembled only from database point rules."""

    _CONSISTENT_KEYS = (
        "dsl_version",
        "engine_version",
        "legacy_matcher_sha256",
        "normalization",
        "scoring",
        "thresholds",
    )

    def __init__(
        self,
        rules: Mapping[str, Mapping[str, Any]],
        *,
        point_spec: Mapping[str, set[str]] = V5_1_POINT_SPEC,
    ) -> None:
        expected_point_ids = set(point_spec)
        if set(rules) != expected_point_ids or len(rules) != len(expected_point_ids):
            raise MatcherDslError(
                "materialized matcher requires exactly the approved "
                f"{len(expected_point_ids)} point rules"
            )
        canonical_rules: dict[str, dict[str, Any]] = {}
        common: dict[str, str] | None = None
        for code in sorted(rules):
            value = rules[code]
            validate_materialized_rule(value, point_spec=point_spec)
            point_id = str(value["rule"]["point_id"])
            if code != point_id:
                raise MatcherDslError(
                    f"materialized rule key {code!r} does not match point_id {point_id!r}"
                )
            normalized = json.loads(
                canonical_materialized_rule(value, point_spec=point_spec)
            )
            normalized_common = {
                key: canonical_json(normalized[key]) for key in self._CONSISTENT_KEYS
            }
            if common is None:
                common = normalized_common
            elif normalized_common != common:
                differing = sorted(
                    key
                    for key in self._CONSISTENT_KEYS
                    if normalized_common[key] != common[key]
                )
                raise MatcherDslError(
                    f"materialized rules disagree on shared matcher fields: {differing}"
                )
            canonical_rules[code] = normalized
        self._rules = canonical_rules
        self.matcher_rule_sha256 = taxonomy_matcher_sha256(
            canonical_rules,
            point_spec=point_spec,
        )
        first = canonical_rules[sorted(canonical_rules)[0]]
        self.thresholds = copy.deepcopy(first["thresholds"])

    def match_points(
        self,
        row: Mapping[str, Any],
        transcript: Mapping[str, Any],
        ocr: Mapping[str, Any],
        evidence: str,
        visual: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if evidence not in {"V0", "V1", "V2", "V3"}:
            raise MatcherDslError(f"unknown evidence level: {evidence!r}")
        matches: list[dict[str, Any]] = []
        priorities: dict[str, int] = {}
        for code, materialized in self._rules.items():
            bundle = _materialized_as_bundle(materialized)
            matches.extend(
                _Matcher(bundle, row, transcript, ocr, evidence, visual or {}).run()
            )
            priorities[code] = int(materialized["rule"]["priority"])
        unique: dict[str, dict[str, Any]] = {}
        for item in matches:
            code = str(item["id"])
            previous = unique.get(code)
            if previous is None or int(item["score"]) > int(previous["score"]):
                unique[code] = item
        return sorted(
            unique.values(),
            key=lambda item: (
                -int(item["score"]),
                priorities[str(item["id"])],
                str(item["id"]),
            ),
        )


def match_materialized_rules(
    rules: Mapping[str, Mapping[str, Any]],
    row: Mapping[str, Any],
    transcript: Mapping[str, Any],
    ocr: Mapping[str, Any],
    evidence: str,
    visual: Mapping[str, Any] | None = None,
    *,
    point_spec: Mapping[str, set[str]] = V5_1_POINT_SPEC,
) -> list[dict[str, Any]]:
    """Validate and execute the complete database matcher snapshot."""

    return MaterializedMatcher(rules, point_spec=point_spec).match_points(
        row, transcript, ocr, evidence, visual
    )
