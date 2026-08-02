#!/usr/bin/env python3
"""Compare sealed blind content predictions with provisional source labels."""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PREDICTION_MAP = {"汽车类": "auto", "非汽车类": "non_auto"}


def optional_int(value):
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def main() -> int:
    predictions = [
        json.loads(line)
        for line in (ROOT / "pilot_blind_predictions.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    contents = {
        row["pilot_id"]: row
        for row in (
            json.loads(line)
            for line in (ROOT / "pilot_public_content.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    }
    with (ROOT / "pilot_sample_10_labels.csv").open(encoding="utf-8-sig", newline="") as handle:
        labels = {row["pilot_id"]: row for row in csv.DictReader(handle)}

    rows = []
    by_source = defaultdict(Counter)
    for prediction in predictions:
        pilot_id = prediction["pilot_id"]
        source_label = labels[pilot_id]["source_label"]
        mapped = PREDICTION_MAP.get(prediction["content_label"])
        if mapped is None:
            comparison = "abstain"
        elif mapped == source_label:
            comparison = "match"
        else:
            comparison = "mismatch"
        by_source[source_label][comparison] += 1
        rows.append(
            {
                "pilot_id": pilot_id,
                "source_label": source_label,
                "predicted_content_label": prediction["content_label"],
                "comparison": comparison,
                "confidence_band": prediction["confidence_band"],
                "subcategory": prediction["subcategory"],
                "audience_label": prediction["audience_label"],
                "comment_fetch_status": prediction["comment_fetch_status"],
                "content_side_acquisition_signal": prediction["content_side_acquisition_signal"],
                "content_only_action": prediction["content_only_action"],
                "overall_acquisition_tier": prediction["overall_acquisition_tier"],
            }
        )

    output = ROOT / "pilot_label_comparison.csv"
    with output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    evaluation_rows = []
    for prediction in predictions:
        evaluation_rows.append(
            {
                "pilot_id": prediction["pilot_id"],
                "note_id": contents[prediction["pilot_id"]]["note_id"],
                "url": contents[prediction["pilot_id"]]["url"],
                "pred_content_label": prediction["content_label"],
                "pred_content_confidence_band": prediction["confidence_band"],
                "pred_content_subcategory": prediction["subcategory"],
                "content_evidence_level": prediction["content_evidence_level"],
                "pred_audience_label": prediction["audience_label"],
                "comment_fetch_status": prediction["comment_fetch_status"],
                "comment_fetch_error_code": None,
                "platform_comment_count": optional_int(
                    contents[prediction["pilot_id"]].get("interactions", {}).get("commentCount")
                ),
                "retrieved_comment_count": prediction["retrieved_comment_count"],
                "valid_comment_count": prediction["valid_comment_count"],
                "comment_evidence_level": prediction["comment_evidence_level"],
                "pred_content_side_acquisition_signal": prediction["content_side_acquisition_signal"],
                "pred_content_only_action": prediction["content_only_action"],
                "pred_intent_stage": prediction["intent_stage"],
                "pred_recommended_destination": prediction["recommended_destination"],
                "pred_overall_acquisition_tier": prediction["overall_acquisition_tier"],
                "prediction_evidence_json": json.dumps(prediction["evidence"], ensure_ascii=False),
                "prediction_limitations": prediction["limitations"],
                "prediction_version": prediction["prediction_version"],
                "actual_status": "not_tested",
                "actual_clicks": None,
                "actual_installs": None,
                "actual_confirmed_new_users": None,
                "human_review_status": "pending",
                "human_content_label": None,
                "human_review_note": None,
            }
        )
    evaluation_path = ROOT / "pilot_evaluation_results.csv"
    with evaluation_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(evaluation_rows[0]))
        writer.writeheader()
        writer.writerows(evaluation_rows)

    evaluation_json_rows = []
    for row in evaluation_rows:
        item = dict(row)
        item["prediction_evidence"] = json.loads(item.pop("prediction_evidence_json"))
        evaluation_json_rows.append(item)
    (ROOT / "pilot_evaluation_results.jsonl").write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in evaluation_json_rows),
        encoding="utf-8",
    )

    totals = Counter(row["comparison"] for row in rows)
    summary = {
        "sample_size": len(rows),
        "source_counts": dict(Counter(row["source_label"] for row in rows)),
        "prediction_counts": dict(Counter(row["predicted_content_label"] for row in rows)),
        "comparison_counts": dict(totals),
        "by_source": {key: dict(value) for key, value in by_source.items()},
        "audience_evidence": "comment text was not retrieved for any of the 10 notes; valid comment count was not evaluated and no audience conclusions were forced",
        "interpretation_limit": "source_label is provisional; 10 notes validate the pipeline and report format, not formal accuracy",
    }
    (ROOT / "pilot_evaluation_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
