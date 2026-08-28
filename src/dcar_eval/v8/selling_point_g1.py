"""G1 main/second-judgement runner for the frozen selling-points v5.3 set."""

from __future__ import annotations

import json
import math
import time
from collections import Counter, defaultdict, deque
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import selling_point_paid_eval as paid
from .llm_assist import llm_config
from .selling_point_label_cards import load_label_cards
from .selling_point_offline import (
    CharNgramTfidfIndex,
    SellingPointOfflineError,
    canonical_json,
    hard_priority,
    map_quote_to_original,
    parse_model_json,
    sha256_json,
)


G1_RUN_VERSION = "selling-point-g1-v11"
SECOND_PROMPT_VERSION = "selling-point-g1-second-judge-v1"
CONFUSION_EDGE_MINIMUM = 2
COMPONENT_EXEMPT_CONFIDENCE = 0.85
COMPONENT_EXEMPT_MARGIN = 0.20
HISTORICAL_COST_CNY_UPPER_BOUND = 49.492789
MAIN_REQUEST_INTERVAL_SECONDS = 12.0
SECOND_REQUEST_INTERVAL_SECONDS = 6.0


class SellingPointG1Error(RuntimeError):
    """Raised when the frozen G1 contract cannot be executed safely."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SellingPointG1Error(f"cannot read JSON artifact {path}: {error}") from error
    if not isinstance(value, dict):
        raise SellingPointG1Error(f"JSON artifact must be an object: {path}")
    return value


def _latest_group_results(path: Path, *, model: str) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for item in paid._read_ndjson(path):  # noqa: SLF001 - shared stage-A ledger
        if str(item.get("model")) == model:
            latest[str(item["group_key"])] = dict(item)
    return latest


def _row_group_key(row: Mapping[str, Any]) -> str:
    return f"{int(row['content_id'])}:{row['evidence_sha256']}"


def _main_prediction(result: Mapping[str, Any] | None) -> str | None:
    decision = result.get("decision") if isinstance(result, Mapping) else None
    if not isinstance(decision, Mapping):
        return None
    value = str(decision.get("primary_code") or "")
    return value or None


def build_confusion_graph(
    manifest: Mapping[str, Any],
    main_results: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Freeze directed errors and undirected edges seen at least twice."""

    rows = manifest.get("rows")
    if not isinstance(rows, list) or len(rows) != paid.G1_DENOMINATOR:
        raise SellingPointG1Error("G1 confusion graph requires the frozen 228 rows")
    directed: Counter[tuple[str, str]] = Counter()
    for row in rows:
        gold = str(row["gold_code"])
        predicted = _main_prediction(main_results.get(_row_group_key(row)))
        if predicted is not None and predicted != gold:
            directed[(gold, predicted)] += 1
    edge_directions = {
        pair: count
        for pair, count in directed.items()
        if count >= CONFUSION_EDGE_MINIMUM
    }
    edge_weights: Counter[tuple[str, str]] = Counter()
    for (gold, predicted), count in edge_directions.items():
        edge_key = (min(gold, predicted), max(gold, predicted))
        edge_weights[edge_key] += count
    edges = [
        {
            "left": left,
            "right": right,
            "weight": weight,
            "directions": [
                {
                    "gold": gold,
                    "predicted": predicted,
                    "count": count,
                }
                for (gold, predicted), count in sorted(edge_directions.items())
                if {gold, predicted} == {left, right}
            ],
        }
        for (left, right), weight in sorted(edge_weights.items())
    ]
    return {
        "version": "selling-point-g1-confusion-graph-v1",
        "development_manifest_sha256": manifest["manifest_sha256"],
        "edge_minimum_same_direction": CONFUSION_EDGE_MINIMUM,
        "directed_errors": [
            {"gold": gold, "predicted": predicted, "count": count}
            for (gold, predicted), count in sorted(
                directed.items(), key=lambda item: (-item[1], item[0])
            )
        ],
        "edges": edges,
    }


def _edge_maps(
    graph: Mapping[str, Any],
) -> tuple[set[frozenset[str]], dict[str, set[str]], dict[frozenset[str], int]]:
    edges: set[frozenset[str]] = set()
    adjacency: dict[str, set[str]] = defaultdict(set)
    weights: dict[frozenset[str], int] = {}
    for raw in graph.get("edges", []):
        if not isinstance(raw, Mapping):
            continue
        left = str(raw["left"])
        right = str(raw["right"])
        edge = frozenset((left, right))
        edges.add(edge)
        adjacency[left].add(right)
        adjacency[right].add(left)
        weights[edge] = int(raw["weight"])
    return edges, adjacency, weights


def _component(start: str, adjacency: Mapping[str, set[str]]) -> set[str]:
    output: set[str] = set()
    queue: deque[str] = deque([start])
    while queue:
        code = queue.popleft()
        if code in output:
            continue
        output.add(code)
        queue.extend(sorted(adjacency.get(code, set()) - output))
    return output


def build_mask_analysis(
    manifest: Mapping[str, Any],
    main_results: Mapping[str, Mapping[str, Any]],
    graph: Mapping[str, Any],
) -> dict[str, Any]:
    """Build component/edge masks without exposing target gold to prompts."""

    rows = manifest.get("rows")
    if not isinstance(rows, list):
        raise SellingPointG1Error("development manifest rows are missing")
    labels = load_label_cards()["cards"]
    edges, adjacency, weights = _edge_maps(graph)
    records: list[dict[str, Any]] = []
    for group in paid.manifest_groups(rows):
        group_key = str(group["group_key"])
        target = group["target"]
        result = main_results.get(group_key)
        decision = result.get("decision") if isinstance(result, Mapping) else None
        calls_used = len(result.get("calls", [])) if isinstance(result, Mapping) else 0
        base = {
            "group_key": group_key,
            "content_id": int(group["content_id"]),
            "excel_rows": list(group["excel_rows"]),
            "evidence_level": str(group["evidence_level"]),
            "main_calls_used": calls_used,
            "main_predicted_code": _main_prediction(result),
        }
        empty = {"trigger": False, "candidates": [], "reason": "no-main-decision"}
        if group["evidence_level"] == "V0" or not isinstance(decision, Mapping):
            records.append({**base, "C": dict(empty), "E": dict(empty)})
            continue
        top3 = [
            str(item.get("code") or "")
            for item in decision.get("top3", [])
            if isinstance(item, Mapping) and str(item.get("code") or "") in labels
        ]
        top1 = str(decision["primary_code"])
        if not top3 or top3[0] != top1:
            raise SellingPointG1Error(f"invalid frozen main ranking for {group_key}")
        allowed = set(
            hard_priority(target["evidence_package"], labels)["allowed_codes"]
        )
        if calls_used >= 2:
            reason = "second-call-capacity-consumed-by-main-repair"
            records.append(
                {
                    **base,
                    "C": {"trigger": False, "candidates": [top1], "reason": reason},
                    "E": {"trigger": False, "candidates": [top1], "reason": reason},
                }
            )
            continue

        component_codes = _component(top1, adjacency) & allowed
        component_candidates = [code for code in top3 if code in component_codes]
        if top1 not in component_candidates:
            component_candidates.insert(0, top1)
        if len(component_candidates) < 2:
            neighbors = sorted(
                adjacency.get(top1, set()) & allowed,
                key=lambda code: (-weights.get(frozenset((top1, code)), 0), code),
            )
            for code in neighbors:
                if code not in component_candidates:
                    component_candidates.append(code)
                if len(component_candidates) >= 2:
                    break
        component_candidates = component_candidates[:3]
        confidence = float(decision.get("confidence") or 0)
        top2_confidence = (
            float(decision["top3"][1]["confidence"])
            if len(decision.get("top3", [])) >= 2
            else 0.0
        )
        exempt = confidence >= COMPONENT_EXEMPT_CONFIDENCE and (
            confidence - top2_confidence >= COMPONENT_EXEMPT_MARGIN
        )
        component_trigger = len(component_candidates) >= 2 and not exempt
        component_reason = (
            "wide-margin-exemption"
            if exempt
            else "component-candidates"
            if component_trigger
            else "no-component-candidate"
        )

        edge_candidates = [top1]
        for code in top3[1:]:
            if code in allowed and frozenset((top1, code)) in edges:
                edge_candidates.append(code)
        edge_candidates = edge_candidates[:3]
        edge_trigger = len(edge_candidates) >= 2
        records.append(
            {
                **base,
                "C": {
                    "trigger": component_trigger,
                    "candidates": component_candidates,
                    "reason": component_reason,
                },
                "E": {
                    "trigger": edge_trigger,
                    "candidates": edge_candidates,
                    "reason": "top3-confusion-edge" if edge_trigger else "no-top3-edge",
                },
            }
        )

    analysis: dict[str, Any] = {
        "version": "selling-point-g1-mask-analysis-v1",
        "development_manifest_sha256": manifest["manifest_sha256"],
        "confusion_graph": graph,
        "records": records,
    }
    analysis["theoretical"] = {
        mask: score_mask(manifest, records, mask=mask, second_results=None, oracle=True)
        for mask in ("C", "E")
    }
    analysis["feasible_masks"] = [
        mask
        for mask in ("C", "E")
        if analysis["theoretical"][mask]["g1_pass"]
    ]
    analysis["analysis_sha256"] = sha256_json(analysis)
    return analysis


def _empty_metric() -> dict[str, int]:
    return {"exact_count": 0, "denominator": 0}


def _metric_summary(row_results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    exact = sum(bool(row["matched"]) for row in row_results)
    scenes: dict[str, dict[str, Any]] = {}
    for scene, denominator in paid.G1_SCENE_DENOMINATORS.items():
        rows = [row for row in row_results if str(row["gold_code"]).startswith(scene)]
        if len(rows) != denominator:
            raise SellingPointG1Error(f"unexpected G1 {scene} denominator")
        scene_exact = sum(bool(row["matched"]) for row in rows)
        scenes[scene] = {
            "exact_count": scene_exact,
            "denominator": denominator,
            "accuracy": round(scene_exact / denominator, 6),
            "pass_count": paid.G1_SCENE_PASS_COUNTS[scene],
            "passed": scene_exact >= paid.G1_SCENE_PASS_COUNTS[scene],
        }
    cards = load_label_cards()["cards"]
    labels: dict[str, dict[str, Any]] = {}
    for code in sorted(cards):
        code_rows = [row for row in row_results if row["gold_code"] == code]
        support = len(code_rows)
        code_exact = sum(bool(row["matched"]) for row in code_rows)
        labels[code] = {
            "support": support,
            "exact_count": code_exact,
            "accuracy": round(code_exact / support, 6) if support else None,
        }
    overall_passed = exact >= paid.G1_PASS_COUNT
    return {
        "exact_count": exact,
        "denominator": paid.G1_DENOMINATOR,
        "accuracy": round(exact / paid.G1_DENOMINATOR, 6),
        "overall_passed": overall_passed,
        "scenes": scenes,
        "labels": labels,
        "zero_sample_codes": [
            code for code, item in labels.items() if item["support"] == 0
        ],
        "g1_pass": overall_passed and all(item["passed"] for item in scenes.values()),
    }


def score_mask(
    manifest: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    *,
    mask: str,
    second_results: Mapping[str, Mapping[str, Any]] | None,
    oracle: bool,
) -> dict[str, Any]:
    by_group = {str(record["group_key"]): record for record in records}
    row_results: list[dict[str, Any]] = []
    triggered_groups = 0
    exposed_correct_rows = 0
    recoverable_error_rows = 0
    for row in manifest["rows"]:
        key = _row_group_key(row)
        record = by_group[key]
        mask_value = record[mask]
        predicted = record.get("main_predicted_code")
        gold = str(row["gold_code"])
        triggered = bool(mask_value["trigger"])
        if triggered and int(row["excel_row"]) == min(record["excel_rows"]):
            triggered_groups += 1
        if triggered and predicted == gold:
            exposed_correct_rows += 1
        if triggered and predicted != gold and gold in mask_value["candidates"]:
            recoverable_error_rows += 1
        if triggered:
            if oracle and gold in mask_value["candidates"]:
                predicted = gold
            elif not oracle and second_results is not None:
                second = second_results.get(key)
                decision = second.get("decision") if isinstance(second, Mapping) else None
                if isinstance(decision, Mapping):
                    ranking = [str(item["code"]) for item in decision["ranking"]]
                    predicted = next(
                        (code for code in ranking if code in mask_value["candidates"]),
                        predicted,
                    )
        row_results.append(
            {
                "excel_row": int(row["excel_row"]),
                "content_id": int(row["content_id"]),
                "gold_code": gold,
                "main_predicted_code": record.get("main_predicted_code"),
                "predicted_code": predicted,
                "matched": predicted == gold,
                "triggered": triggered,
                "candidates": list(mask_value["candidates"]),
            }
        )
    metric = _metric_summary(row_results)
    metric.update(
        {
            "mask": mask,
            "oracle": oracle,
            "triggered_groups": triggered_groups,
            "triggered_rows": sum(bool(row["triggered"]) for row in row_results),
            "exposed_correct_rows": exposed_correct_rows,
            "recoverable_error_rows": recoverable_error_rows,
            "row_results": row_results,
        }
    )
    return metric


def _candidate_card(card: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "code": str(card["id"]),
        "label": str(card["label"]),
        "definition": str(card["definition"]),
        "positive_evidence": list(card["positive_evidence"]),
        "negative_evidence": list(card["negative_evidence"]),
        "boundary_rules": list(card["boundary_rules"]),
    }


def build_second_prompt(
    target: Mapping[str, Any],
    *,
    main_decision: Mapping[str, Any],
    union_candidates: Sequence[str],
    masks: Mapping[str, Sequence[str]],
    index: CharNgramTfidfIndex,
) -> dict[str, str]:
    labels = load_label_cards()
    cards = labels["cards"]
    candidates = list(dict.fromkeys(str(code) for code in union_candidates))
    if not 2 <= len(candidates) <= 4 or str(main_decision["primary_code"]) not in candidates:
        raise SellingPointG1Error("second judgement candidates must contain Top1 and 2..4 codes")
    package = target["evidence_package"]
    system = {
        "role": "你是懂车帝唯一主卖点保守二判员。",
        "task": "仅在给定候选中按植入的主要用户任务排序，不得引入其他标签。",
        "default_policy": (
            "默认维持首轮Top1；只有原文证据明确排除Top1并明确支持另一候选时才改。"
            "证据打平、只有泛主题或只有OCR偶然菜单词时，Top1必须排第一。"
        ),
        "rules": [
            "先比较懂车帝锚点附近的CTA、搜索口令、演示动作和反复任务，再看视频泛主题。",
            "候选定义、正反证据和边界规则都要逐项核对。",
            "similar_gold_examples是目标内容之外的已裁决先例，只按任务链路类比。",
            "ranking必须覆盖candidate_codes且不重复，confidence按降序。",
            "anchor_quote必须逐字来自指定channel，不能改写。",
            "只输出一个JSON对象。",
        ],
        "candidate_cards": [_candidate_card(cards[code]) for code in candidates],
        "output_schema": {
            "recommended_code": "candidate_codes之一",
            "ranking": [{"code": "candidate_codes之一", "confidence": "0..1"}],
            "channel": "title|body|asr|ocr",
            "anchor_quote": "2..120字逐字原文",
            "reason": "说明为何维持或改变Top1，最多500字",
        },
    }
    user = {
        "candidate_codes": candidates,
        "mask_candidates": {name: list(values) for name, values in masks.items()},
        "first_pass": {
            "primary_code": main_decision["primary_code"],
            "confidence": main_decision["confidence"],
            "top3": main_decision["top3"],
            "channel": main_decision["channel"],
            "anchor_quote": main_decision["anchor_quote"],
            "reason": main_decision["reason"],
        },
        "anchor_windows": package["anchor_windows"],
        "channels": package["channels"],
        "similar_gold_examples": index.select_for_codes(target, candidates),
    }
    identity = {
        "contract": SECOND_PROMPT_VERSION,
        "label_cards_sha256": labels["source_sha256"],
        "retrieval_index_sha256": index.index_sha256,
    }
    return {
        "prompt_version": f"{SECOND_PROMPT_VERSION}-{sha256_json(identity)[:16]}",
        "system": canonical_json(system),
        "user": canonical_json(user),
    }


def _confidence(value: Any, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SellingPointOfflineError(f"{field} must be numeric")
    number = float(value)
    if not 0 <= number <= 1:
        raise SellingPointOfflineError(f"{field} must be in 0..1")
    return number


def validate_second_response(
    parsed: Mapping[str, Any],
    *,
    target: Mapping[str, Any],
    candidates: Sequence[str],
) -> dict[str, Any]:
    expected = list(candidates)
    raw_ranking = parsed.get("ranking")
    if not isinstance(raw_ranking, list) or len(raw_ranking) != len(expected):
        raise SellingPointOfflineError("second ranking must cover every candidate")
    ranking: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, item in enumerate(raw_ranking):
        if not isinstance(item, Mapping):
            raise SellingPointOfflineError("second ranking entries must be objects")
        code = str(item.get("code") or "")
        if code not in expected or code in seen:
            raise SellingPointOfflineError("second ranking violates candidate set")
        seen.add(code)
        ranking.append(
            {
                "code": code,
                "confidence": _confidence(
                    item.get("confidence"), field=f"ranking[{index}].confidence"
                ),
            }
        )
    if seen != set(expected):
        raise SellingPointOfflineError("second ranking omits a candidate")
    if any(
        ranking[index]["confidence"] < ranking[index + 1]["confidence"]
        for index in range(len(ranking) - 1)
    ):
        raise SellingPointOfflineError("second ranking confidence must be non-increasing")
    recommended = str(parsed.get("recommended_code") or "")
    if recommended != ranking[0]["code"]:
        raise SellingPointOfflineError("recommended_code must equal ranking[0]")
    channel = str(parsed.get("channel") or "")
    quote = str(parsed.get("anchor_quote") or "").strip()
    if channel not in {"title", "body", "asr", "ocr"} or not 2 <= len(quote) <= 120:
        raise SellingPointOfflineError("second quote channel/length is invalid")
    quote_mapping = map_quote_to_original(target, channel=channel, quote=quote)
    reason = str(parsed.get("reason") or "").strip()
    if not reason or len(reason) > 500:
        raise SellingPointOfflineError("second reason must contain 1..500 characters")
    return {
        "recommended_code": recommended,
        "ranking": ranking,
        "channel": channel,
        "anchor_quote": quote,
        "quote_status": quote_mapping["status"],
        "original_quote": quote_mapping.get("original_quote"),
        "reason": reason,
    }


def evaluate_second_group(
    group: Mapping[str, Any],
    *,
    model: paid.ModelPrice,
    main_result: Mapping[str, Any],
    mask_record: Mapping[str, Any],
    feasible_masks: Sequence[str],
    index: CharNgramTfidfIndex,
    caller: paid.ModelCaller,
) -> dict[str, Any]:
    decision = main_result.get("decision")
    if not isinstance(decision, Mapping):
        raise SellingPointG1Error("second judgement requires an accepted main decision")
    masks = {
        mask: list(mask_record[mask]["candidates"])
        for mask in feasible_masks
        if mask_record[mask]["trigger"]
    }
    union = list(
        dict.fromkeys(
            [str(decision["primary_code"])]
            + [code for mask in feasible_masks for code in masks.get(mask, [])]
        )
    )
    prompt = build_second_prompt(
        group["target"],
        main_decision=decision,
        union_candidates=union,
        masks=masks,
        index=index,
    )
    call = caller(model, prompt["system"], prompt["user"])
    call_payload = paid._call_payload(  # noqa: SLF001 - shared paid-call receipt
        call, model, reason="g1_masked_second_judgement"
    )
    second_decision: dict[str, Any] | None = None
    validation_note: str | None = None
    try:
        second_decision = validate_second_response(
            parse_model_json(call.raw),
            target=group["target"],
            candidates=union,
        )
    except SellingPointOfflineError as error:
        validation_note = str(error)
    return {
        "version": "selling-point-g1-second-result-v1",
        "group_key": str(group["group_key"]),
        "content_id": int(group["content_id"]),
        "excel_rows": list(group["excel_rows"]),
        "model": model.model,
        "prompt_version": prompt["prompt_version"],
        "masks": masks,
        "union_candidates": union,
        "status": "accepted" if second_decision is not None else "error",
        "decision": second_decision,
        "validation_note": validation_note,
        "calls": [call_payload],
        "input_tokens": int(call_payload["input_tokens"]),
        "output_tokens": int(call_payload["output_tokens"]),
        "latency_ms": int(call_payload["latency_ms"]),
        "cost_cny_upper_bound": float(call_payload["cost_cny_upper_bound"]),
        "completed_at": paid._now_utc(),  # noqa: SLF001 - shared run timestamp
    }


def _transport_error_second_result(
    *,
    group: Mapping[str, Any],
    model: paid.ModelPrice,
    error: paid.SellingPointCallError,
) -> dict[str, Any]:
    cost = model.cost(error.reserved_input_tokens, error.reserved_output_tokens)
    return {
        "version": "selling-point-g1-second-result-v1",
        "group_key": str(group["group_key"]),
        "content_id": int(group["content_id"]),
        "excel_rows": list(group["excel_rows"]),
        "model": model.model,
        "status": "error",
        "decision": None,
        "validation_note": f"transport_failure: {error}",
        "calls": [],
        "reserved_input_tokens": error.reserved_input_tokens,
        "reserved_output_tokens": error.reserved_output_tokens,
        "cost_cny_upper_bound": round(cost, 10),
        "completed_at": paid._now_utc(),  # noqa: SLF001 - shared run timestamp
    }


def _second_targets(
    groups: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
    feasible_masks: Sequence[str],
) -> list[tuple[Mapping[str, Any], Mapping[str, Any]]]:
    groups_by_key = {str(group["group_key"]): group for group in groups}
    output: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    for record in records:
        if int(record["main_calls_used"]) >= 2:
            continue
        if any(bool(record[mask]["trigger"]) for mask in feasible_masks):
            output.append((groups_by_key[str(record["group_key"])], record))
    return output


def _run_second_union(
    *,
    output_dir: Path,
    manifest: Mapping[str, Any],
    main_results: Mapping[str, Mapping[str, Any]],
    mask_analysis: Mapping[str, Any],
    model: paid.ModelPrice,
    model_config: Mapping[str, Any],
    main_cost: float,
    budget_limit_cny: float,
    concurrency: int,
    caller: paid.ModelCaller,
) -> dict[str, dict[str, Any]]:
    feasible_masks = [str(mask) for mask in mask_analysis["feasible_masks"]]
    if not feasible_masks:
        return {}
    rows = manifest["rows"]
    index = CharNgramTfidfIndex(rows)
    groups = paid.manifest_groups(rows)
    targets = _second_targets(groups, mask_analysis["records"], feasible_masks)
    output_dir.mkdir(parents=True, exist_ok=True)
    results_path = output_dir / "second_results.ndjson"
    run_manifest_path = output_dir / "second_run_manifest.json"
    identity = {
        "version": "selling-point-g1-second-run-v1",
        "development_manifest_sha256": manifest["manifest_sha256"],
        "mask_analysis_sha256": mask_analysis["analysis_sha256"],
        "model_config_sha256": model_config["config_sha256"],
        "model": model.model,
        "feasible_masks": feasible_masks,
        "retrieval_index_sha256": index.index_sha256,
    }
    if run_manifest_path.exists():
        existing_identity = _read_json(run_manifest_path)
        if any(existing_identity.get(key) != value for key, value in identity.items()):
            raise SellingPointG1Error("existing second-run identity mismatch")
    else:
        paid._write_json_atomic(  # noqa: SLF001 - shared atomic artifact writer
            run_manifest_path,
            {**identity, "started_at": paid._now_utc()},  # noqa: SLF001
        )
    history = paid._read_ndjson(results_path)  # noqa: SLF001
    existing = {str(item["group_key"]): item for item in history}
    attempt_counts = Counter(str(item["group_key"]) for item in history)
    pending = [
        item
        for item in targets
        if str(item[0]["group_key"]) not in existing
        or (
            paid.retryable_transport_result(existing[str(item[0]["group_key"])])
            and attempt_counts[str(item[0]["group_key"])] < 3
        )
    ]
    existing_cost = sum(
        float(item.get("cost_cny_upper_bound") or 0) for item in history
    )
    reserve = 0.0
    for group, record in pending:
        decision = main_results[str(group["group_key"])]["decision"]
        masks = {
            mask: list(record[mask]["candidates"])
            for mask in feasible_masks
            if record[mask]["trigger"]
        }
        union = list(
            dict.fromkeys(
                [str(decision["primary_code"])]
                + [code for mask in feasible_masks for code in masks.get(mask, [])]
            )
        )
        prompt = build_second_prompt(
            group["target"],
            main_decision=decision,
            union_candidates=union,
            masks=masks,
            index=index,
        )
        input_tokens = math.ceil(
            (len(prompt["system"]) + len(prompt["user"]))
            * paid.INPUT_TOKEN_RESERVE_PER_CHAR
        )
        # Reserve every possible transport attempt before dispatching any
        # concurrent second judgement, including unmetered timeout retries.
        reserve += paid.MAX_TRANSPORT_ATTEMPTS * model.cost(
            input_tokens, paid.MAX_OUTPUT_TOKENS
        )
    budget_limit = float(budget_limit_cny)
    if main_cost + existing_cost + reserve > budget_limit:
        raise SellingPointG1Error(
            "G1 second-judgement reservation exceeds the 200 CNY hard limit"
        )
    caller = paid.budgeted_caller(
        caller, remaining_cny=budget_limit - main_cost - existing_cost
    )

    if pending:
        # Main and second judgements share one account TPM window.  A bounded
        # cooldown prevents the first second-pass batch from inheriting the
        # final main-pass minute without creating an unbounded wait.
        time.sleep(55)

    future_map: dict[
        Future[dict[str, Any]], tuple[Mapping[str, Any], Mapping[str, Any]]
    ] = {}
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        for group, record in pending:
            future = executor.submit(
                evaluate_second_group,
                group,
                model=model,
                main_result=main_results[str(group["group_key"])],
                mask_record=record,
                feasible_masks=feasible_masks,
                index=index,
                caller=caller,
            )
            future_map[future] = (group, record)
        for completed, future in enumerate(as_completed(future_map), start=1):
            group, _ = future_map[future]
            try:
                result = future.result()
            except paid.SellingPointCallError as error:
                result = _transport_error_second_result(
                    group=group, model=model, error=error
                )
            paid._append_ndjson(results_path, result)  # noqa: SLF001
            existing_cost += float(result.get("cost_cny_upper_bound") or 0)
            existing[str(group["group_key"])] = result
            if completed % 20 == 0 or completed == len(pending):
                print(
                    canonical_json(
                        {
                            "second_judgement_progress": f"{completed}/{len(pending)}",
                            "cost_cny_upper_bound": round(
                                main_cost + existing_cost,
                                6,
                            ),
                        }
                    ),
                    flush=True,
                )
    return existing


def _g1_report(summary: Mapping[str, Any]) -> str:
    lines = [
        "# 卖点 v5.3 G1 离线评测报告",
        "",
        f"- 结果：{'通过' if summary['g1_pass'] else '未通过'}",
        f"- 选定二判：{summary.get('selected_mask') or '无'}",
        f"- 本轮新增费用上界：{float(summary['new_cost_cny_upper_bound']):.6f} 元",
        f"- 历史费用上界（不回摊）：{HISTORICAL_COST_CNY_UPPER_BOUND:.6f} 元",
        "",
        "| 方案 | 完全一致 | 新车 | 二手车 | 媒体 | 触发内容 | G1 |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for name in ("main", "C", "E"):
        item = summary["results"][name]
        lines.append(
            f"| {name} | {item['exact_count']}/228 | "
            f"{item['scenes']['X']['exact_count']}/112 | "
            f"{item['scenes']['E']['exact_count']}/93 | "
            f"{item['scenes']['M']['exact_count']}/23 | "
            f"{item.get('triggered_groups', 0)} | "
            f"{'通过' if item['g1_pass'] else '未通过'} |"
        )
    lines.extend(
        [
            "",
            "阻断门槛：整体 160/228；新车 84/112；二手车 66/93；媒体 14/23。",
            "零样本盲区：M1、M3、X11；这些标签未被本开发集验证。",
            "",
        ]
    )
    return "\n".join(lines)


def run_g1(
    *,
    manifest: Mapping[str, Any],
    model_config_path: Path,
    output_dir: Path,
    concurrency: int = 6,
    prior_new_cost_cny_upper_bound: float = 0.0,
) -> dict[str, Any]:
    """Run/resume the frozen Pro main pass, masks and one union second pass."""

    if not 1 <= concurrency <= 16:
        raise SellingPointG1Error("G1 concurrency must be in 1..16")
    model_config = paid.load_model_config(model_config_path)
    models = model_config["models_loaded"]
    if len(models) != 1 or models[0].model != "doubao-seed-2-1-pro-260628":
        raise SellingPointG1Error("G1 must use the frozen Doubao Pro only")
    model = models[0]
    configured_budget = float(model_config["budget_limit_cny"])
    prior_cost = float(prior_new_cost_cny_upper_bound)
    if not 0 <= prior_cost < configured_budget:
        raise SellingPointG1Error("prior G1 cost leaves no executable budget")
    remaining_budget = configured_budget - prior_cost
    output_dir.mkdir(parents=True, exist_ok=True)
    main_dir = output_dir / "main"
    # Resuming the main phase must not spend money already consumed by a
    # previous second phase. This runtime allowance is not a frozen identity.
    existing_second_cost = sum(
        float(result.get("cost_cny_upper_bound") or 0)
        for result in paid._read_ndjson(output_dir / "second_results.ndjson")
    )
    main_run = paid.run_bakeoff(
        manifest=manifest,
        model_config_path=model_config_path,
        output_dir=main_dir,
        concurrency=concurrency,
        allow_boundary_second_pass=False,
        retry_terminal_transport_errors=True,
        minimum_request_interval_seconds=MAIN_REQUEST_INTERVAL_SECONDS,
        budget_limit_cny=remaining_budget,
        shared_spent_cny_upper_bound=existing_second_cost,
    )
    main_results = _latest_group_results(
        main_dir / "group_results.ndjson", model=model.model
    )
    graph = build_confusion_graph(manifest, main_results)
    mask_analysis = build_mask_analysis(manifest, main_results, graph)
    paid._write_json_atomic(  # noqa: SLF001 - shared atomic artifact writer
        output_dir / "mask_analysis.json", mask_analysis
    )
    main_cost = float(main_run["summary"]["total_cost_cny_upper_bound"])
    caller = paid.paced_caller(
        paid.api_caller(llm_config()),
        minimum_interval_seconds=SECOND_REQUEST_INTERVAL_SECONDS,
    )
    second_results = _run_second_union(
        output_dir=output_dir,
        manifest=manifest,
        main_results=main_results,
        mask_analysis=mask_analysis,
        model=model,
        model_config=model_config,
        main_cost=main_cost,
        budget_limit_cny=remaining_budget,
        concurrency=concurrency,
        caller=caller,
    )
    records = mask_analysis["records"]
    main_rows = [
        {
            "excel_row": int(row["excel_row"]),
            "content_id": int(row["content_id"]),
            "gold_code": str(row["gold_code"]),
            "predicted_code": _main_prediction(main_results.get(_row_group_key(row))),
            "matched": _main_prediction(main_results.get(_row_group_key(row)))
            == str(row["gold_code"]),
        }
        for row in manifest["rows"]
    ]
    main_metric = _metric_summary(main_rows)
    actual = {
        mask: score_mask(
            manifest,
            records,
            mask=mask,
            second_results=second_results,
            oracle=False,
        )
        for mask in ("C", "E")
    }
    passing = [mask for mask in ("C", "E") if actual[mask]["g1_pass"]]
    selected_mask = "E" if "E" in passing else "C" if "C" in passing else None
    second_history = paid._read_ndjson(  # noqa: SLF001 - cumulative cost ledger
        output_dir / "second_results.ndjson"
    )
    second_cost = sum(
        float(result.get("cost_cny_upper_bound") or 0)
        for result in second_history
    )
    current_run_cost = round(main_cost + second_cost, 8)
    new_cost = round(prior_cost + current_run_cost, 8)
    if new_cost > configured_budget:
        raise SellingPointG1Error("actual G1 cost exceeds the 200 CNY hard limit")
    call_counts: dict[str, int] = {}
    for record in records:
        key = str(record["group_key"])
        call_counts[key] = int(record["main_calls_used"]) + int(key in second_results)
    if call_counts and max(call_counts.values()) > 2:
        raise SellingPointG1Error("a content exceeded the two-semantic-call limit")

    row_output: list[dict[str, Any]] = []
    c_rows = {row["excel_row"]: row for row in actual["C"].pop("row_results")}
    e_rows = {row["excel_row"]: row for row in actual["E"].pop("row_results")}
    for main_row in main_rows:
        raw_excel_row = main_row["excel_row"]
        if isinstance(raw_excel_row, bool) or not isinstance(raw_excel_row, int):
            raise SellingPointG1Error("G1 row output has a non-integer Excel row")
        excel_row = raw_excel_row
        row_output.append(
            {
                **main_row,
                "component_predicted_code": c_rows[excel_row]["predicted_code"],
                "edge_predicted_code": e_rows[excel_row]["predicted_code"],
                "selected_predicted_code": (
                    e_rows[excel_row]["predicted_code"]
                    if selected_mask == "E"
                    else c_rows[excel_row]["predicted_code"]
                    if selected_mask == "C"
                    else main_row["predicted_code"]
                ),
                "component_triggered": c_rows[excel_row]["triggered"],
                "edge_triggered": e_rows[excel_row]["triggered"],
            }
        )
    summary: dict[str, Any] = {
        "version": G1_RUN_VERSION,
        "generated_at": paid._now_utc(),  # noqa: SLF001
        "development_manifest_sha256": manifest["manifest_sha256"],
        "model": model.model,
        "model_config_sha256": model_config["config_sha256"],
        "g1_contract": {
            "overall": {"pass_count": 160, "denominator": 228},
            "scene_pass_counts": paid.G1_SCENE_PASS_COUNTS,
            "scene_denominators": paid.G1_SCENE_DENOMINATORS,
        },
        "feasible_masks": mask_analysis["feasible_masks"],
        "selected_mask": selected_mask,
        "g1_pass": selected_mask is not None,
        "correction_loop_required": selected_mask is None,
        "results": {"main": main_metric, **actual},
        "theoretical": mask_analysis["theoretical"],
        "main_api_calls": sum(len(result.get("calls", [])) for result in main_results.values()),
        "second_api_calls": len(second_results),
        "maximum_semantic_calls_per_content": max(call_counts.values(), default=0),
        "new_cost_cny_upper_bound": new_cost,
        "prior_new_cost_cny_upper_bound": prior_cost,
        "current_run_cost_cny_upper_bound": current_run_cost,
        "main_cost_cny_upper_bound": main_cost,
        "second_cost_cny_upper_bound": round(second_cost, 8),
        "historical_cost_cny_upper_bound": HISTORICAL_COST_CNY_UPPER_BOUND,
        "budget_limit_cny": configured_budget,
        "zero_sample_codes": main_metric["zero_sample_codes"],
    }
    summary["summary_sha256"] = sha256_json(summary)
    receipt = {
        "version": "selling-point-g1-cost-receipt-v1",
        "currency": "CNY",
        "cost_basis": model_config["cost_basis"],
        "price_frozen_at": model_config["queried_at"],
        "price_source_url": model.price_source_url,
        "model": model.model,
        "input_cny_per_million_tokens": model.input_rate,
        "output_cny_per_million_tokens": model.output_rate,
        "main_cost_cny_upper_bound": main_cost,
        "second_cost_cny_upper_bound": round(second_cost, 8),
        "prior_new_cost_cny_upper_bound": prior_cost,
        "current_run_cost_cny_upper_bound": current_run_cost,
        "new_cost_cny_upper_bound": new_cost,
        "historical_cost_cny_upper_bound": HISTORICAL_COST_CNY_UPPER_BOUND,
        "budget_limit_cny": configured_budget,
    }
    receipt["receipt_sha256"] = sha256_json(receipt)
    paid._write_json_atomic(output_dir / "g1_summary.json", summary)  # noqa: SLF001
    paid._write_json_atomic(output_dir / "g1_row_results.json", row_output)  # noqa: SLF001
    paid._write_json_atomic(output_dir / "g1_receipt.json", receipt)  # noqa: SLF001
    (output_dir / "g1_report.md").write_text(
        _g1_report(summary), encoding="utf-8"
    )
    return {
        "summary": summary,
        "receipt": receipt,
        "output_dir": str(output_dir),
    }
