from __future__ import annotations

import unittest
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from dcar_eval.v8 import selling_point_g1 as g1
from dcar_eval.v8 import selling_point_paid_eval as paid

from dcar_eval.v8.selling_point_evidence import (
    build_evidence_package,
    load_evidence_config,
)
from dcar_eval.v8.selling_point_g1 import (
    build_confusion_graph,
    score_mask,
    validate_second_response,
)
from dcar_eval.v8.selling_point_offline import SellingPointOfflineError


class SellingPointG1BudgetTests(unittest.TestCase):
    def _run_second(self, output_dir: Path, budget: float, *, main_cost: float = 0):
        groups = [
            {"group_key": str(n), "content_id": n, "excel_rows": [n], "target": {}}
            for n in (1, 2)
        ]
        records = [
            {"group_key": str(n), "main_calls_used": 1,
             "C": {"trigger": True, "candidates": ["X1", "X2"]}}
            for n in (1, 2)
        ]
        model = paid.ModelPrice("test", "test", "test", 0, 1, False, "", None)
        with (
            patch.object(g1, "CharNgramTfidfIndex", return_value=Mock(index_sha256="index")),
            patch.object(paid, "manifest_groups", return_value=groups),
            patch.object(g1, "build_second_prompt", return_value={
                "system": "s", "user": "u", "prompt_version": "test"}),
            patch.object(paid.shutil, "which", return_value="/usr/bin/curl"),
            patch.object(paid.subprocess, "run", side_effect=subprocess.TimeoutExpired("curl", 50)) as requests,
            patch.object(paid.time, "sleep"),
        ):
            try:
                results = g1._run_second_union(
                    output_dir=output_dir,
                    manifest={"rows": [], "manifest_sha256": "manifest"},
                    main_results={str(n): {"decision": {"primary_code": "X1"}} for n in (1, 2)},
                    mask_analysis={"feasible_masks": ["C"], "records": records, "analysis_sha256": "masks"},
                    model=model, model_config={"config_sha256": "config"},
                    main_cost=main_cost, budget_limit_cny=budget, concurrency=2,
                    caller=paid.api_caller({"api_base": "https://example.invalid", "endpoint": "/chat", "api_key": "mock"}),
                )
            finally:
                self.requests_made = requests.call_count
        return results

    def test_second_reserves_both_transport_attempts_before_dispatch(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(g1.SellingPointG1Error):
                self._run_second(Path(directory), 0.002)
        self.assertEqual(self.requests_made, 0)

    def test_second_full_reservation_covers_concurrent_timeout_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            results = self._run_second(Path(directory), 0.0032)
        self.assertEqual(self.requests_made, 4)
        self.assertAlmostEqual(sum(r["cost_cny_upper_bound"] for r in results.values()), 0.0032)

    def test_second_reservation_includes_main_and_all_history_not_only_latest(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            for _ in range(2):
                paid._append_ndjson(output / "second_results.ndjson", {
                    "group_key": "1", "status": "error",
                    "validation_note": "transport_failure: timeout", "cost_cny_upper_bound": 0.0008,
                })
            with self.assertRaises(g1.SellingPointG1Error):
                self._run_second(output, 0.004, main_cost=0.0001)
        self.assertEqual(self.requests_made, 0)

    def test_resumed_g1_passes_all_second_history_to_main_budget(self):
        model = paid.ModelPrice("test", "test", "doubao-seed-2-1-pro-260628", 0, 1, False, "", None)
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(paid, "load_model_config", return_value={
                "models_loaded": [model], "budget_limit_cny": 200}),
            patch.object(paid, "run_bakeoff", side_effect=RuntimeError("stop before main")) as main,
        ):
            output = Path(directory)
            for cost in (0.001, 0.002):
                paid._append_ndjson(output / "second_results.ndjson", {
                    "group_key": "1", "cost_cny_upper_bound": cost})
            with self.assertRaisesRegex(RuntimeError, "stop before main"):
                g1.run_g1(manifest={}, model_config_path=Path("unused"), output_dir=output)
        self.assertAlmostEqual(main.call_args.kwargs["shared_spent_cny_upper_bound"], 0.003)


class SellingPointG1Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.evidence_config = load_evidence_config()

    @staticmethod
    def _main_result(code: str) -> dict[str, object]:
        return {
            "decision": {
                "primary_code": code,
                "confidence": 0.8,
                "top3": [{"code": code, "confidence": 0.8}],
            },
            "calls": [{}],
        }

    def test_confusion_edge_requires_two_same_direction_errors(self) -> None:
        rows: list[dict[str, object]] = []
        results: dict[str, dict[str, object]] = {}
        for index in range(1, 229):
            gold = "X1" if index <= 2 else "X2" if index == 3 else "X9"
            predicted = "X4" if index <= 3 else gold
            evidence_sha = f"{index:064x}"
            row = {
                "excel_row": index,
                "content_id": index,
                "evidence_sha256": evidence_sha,
                "gold_code": gold,
            }
            rows.append(row)
            results[f"{index}:{evidence_sha}"] = self._main_result(predicted)
        graph = build_confusion_graph(
            {"manifest_sha256": "a" * 64, "rows": rows}, results
        )
        self.assertEqual(
            [(edge["left"], edge["right"]) for edge in graph["edges"]],
            [("X1", "X4")],
        )
        self.assertEqual(graph["edges"][0]["weight"], 2)

    def test_one_second_ranking_can_be_scored_under_each_mask(self) -> None:
        rows: list[dict[str, object]] = []
        records: list[dict[str, object]] = []
        row_number = 0
        for code, count in (("X1", 112), ("E1", 93), ("M2", 23)):
            for _ in range(count):
                row_number += 1
                evidence_sha = f"{row_number:064x}"
                main = "X4" if row_number == 1 else code
                rows.append(
                    {
                        "excel_row": row_number,
                        "content_id": row_number,
                        "evidence_sha256": evidence_sha,
                        "gold_code": code,
                    }
                )
                records.append(
                    {
                        "group_key": f"{row_number}:{evidence_sha}",
                        "excel_rows": [row_number],
                        "main_predicted_code": main,
                        "C": {
                            "trigger": row_number == 1,
                            "candidates": ["X4", "X1"] if row_number == 1 else [main],
                        },
                        "E": {"trigger": False, "candidates": [main]},
                    }
                )
        first_key = str(records[0]["group_key"])
        second = {
            first_key: {
                "decision": {
                    "ranking": [
                        {"code": "X1", "confidence": 0.9},
                        {"code": "X4", "confidence": 0.2},
                    ]
                }
            }
        }
        manifest = {"rows": rows}
        component = score_mask(
            manifest, records, mask="C", second_results=second, oracle=False
        )
        edge = score_mask(
            manifest, records, mask="E", second_results=second, oracle=False
        )
        self.assertEqual(component["exact_count"], 228)
        self.assertEqual(edge["exact_count"], 227)
        self.assertEqual(component["triggered_groups"], 1)

    def test_second_response_must_cover_candidate_set_and_quote_source(self) -> None:
        text = "打开懂车帝车型库查看外观细节"
        target = {
            "original_channels": {"title": text, "body": "", "asr": "", "ocr": ""},
            "evidence_package": build_evidence_package(
                title=text,
                evidence_level="V3",
                evidence_sha256="1" * 64,
                config=self.evidence_config,
            ),
        }
        parsed = {
            "recommended_code": "X9",
            "ranking": [
                {"code": "X9", "confidence": 0.9},
                {"code": "X1", "confidence": 0.3},
            ],
            "channel": "title",
            "anchor_quote": "懂车帝车型库",
            "reason": "外观细节是明确主任务，维持X9。",
        }
        result = validate_second_response(
            parsed, target=target, candidates=["X9", "X1"]
        )
        self.assertEqual(result["recommended_code"], "X9")
        self.assertEqual(result["quote_status"], "accepted")
        with self.assertRaisesRegex(SellingPointOfflineError, "cover every candidate"):
            validate_second_response(
                {**parsed, "ranking": parsed["ranking"][:1]},
                target=target,
                candidates=["X9", "X1"],
            )


if __name__ == "__main__":
    unittest.main()
