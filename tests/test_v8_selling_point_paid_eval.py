from __future__ import annotations

import http.client
import json
import subprocess
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch
from unittest.mock import Mock

from dcar_eval.v8 import selling_point_paid_eval as paid

from dcar_eval.v8.selling_point_evidence import (
    build_evidence_package,
    load_evidence_config,
)
from dcar_eval.v8.selling_point_label_cards import load_label_cards
from dcar_eval.v8.selling_point_offline import CharNgramTfidfIndex
from dcar_eval.v8.selling_point_paid_eval import (
    ApiCallResult,
    DEFAULT_MODEL_CONFIG_PATH,
    GroupEvaluationError,
    ModelPrice,
    SellingPointCallError,
    SellingPointPaidEvalError,
    _model_catalog,
    _post_chat,
    estimate_reservation,
    evaluate_group,
    load_model_config,
    manifest_groups,
    model_concurrency_limit,
    retryable_transport_result,
    run_bakeoff,
    select_model_prices,
    terminal_results_by_key,
)


class SellingPointPaidEvalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.evidence_config = load_evidence_config()
        cls.labels = load_label_cards()

    def _row(
        self,
        row_number: int,
        content_id: int,
        code: str,
        text: str,
        *,
        level: str = "V3",
    ) -> dict[str, object]:
        package = build_evidence_package(
            title=text,
            evidence_level=level,
            evidence_sha256=f"{content_id:064x}"[-64:],
            config=self.evidence_config,
        )
        return {
            "excel_row": row_number,
            "content_id": content_id,
            "canonical_url": f"https://example.test/video/{content_id}",
            "gold_code": code,
            "retrieval_code": code,
            "implant_position": f"证据{row_number}",
            "video_summary": f"摘要{text}",
            "evidence_level": level,
            "evidence_sha256": f"{content_id:064x}"[-64:],
            "original_channels": {
                "title": text,
                "body": "",
                "asr": "",
                "ocr": "",
            },
            "evidence_package": package,
        }

    def _model(self) -> ModelPrice:
        return ModelPrice(
            slot="test",
            requested_model="test-model",
            model="test-model",
            input_rate=6.0,
            output_rate=30.0,
            supports_json_object=True,
            price_source_url="https://example.test/price",
            replacement_reason=None,
        )

    @staticmethod
    def _response(primary: str, first: float, second: float | None = None) -> str:
        top3 = [{"code": primary, "confidence": first}]
        if second is not None:
            top3.append(
                {
                    "code": "X1" if primary != "X1" else "X9",
                    "confidence": second,
                }
            )
        return json.dumps(
            {
                "primary_code": primary,
                "confidence": first,
                "top3": top3,
                "channel": "title",
                "anchor_quote": "懂车帝车型库",
                "reason": "车型库外观与看车任务明确",
            },
            ensure_ascii=False,
        )

    def test_current_model_config_freezes_three_supported_replacements(self) -> None:
        value = load_model_config(DEFAULT_MODEL_CONFIG_PATH)
        models = value["models_loaded"]
        self.assertEqual(len(models), 3)
        self.assertEqual(
            [model.model for model in models],
            [
                "doubao-seed-2-1-pro-260628",
                "doubao-seed-2-1-turbo-260628",
                "deepseek-v4-flash-ga-260731",
            ],
        )
        self.assertEqual(float(value["budget_limit_cny"]), 500.0)

    def test_g1_model_config_freezes_pro_and_200_cny_cap(self) -> None:
        value = load_model_config(
            Path(__file__).resolve().parents[1]
            / "config"
            / "selling_point_stage_a_g1_v11.json"
        )
        self.assertEqual(
            [model.model for model in value["models_loaded"]],
            ["doubao-seed-2-1-pro-260628"],
        )
        self.assertEqual(float(value["budget_limit_cny"]), 200.0)

    def test_correction_run_selects_only_the_frozen_winner(self) -> None:
        models = load_model_config(DEFAULT_MODEL_CONFIG_PATH)["models_loaded"]
        selected = select_model_prices(models, ["deepseek-v4-flash-ga-260731"])
        self.assertEqual([model.model for model in selected], [
            "deepseek-v4-flash-ga-260731"
        ])
        with self.assertRaisesRegex(SellingPointPaidEvalError, "not configured"):
            select_model_prices(models, ["missing-model"])

    def test_model_catalog_retries_one_incomplete_read(self) -> None:
        class Response:
            def __enter__(self) -> Response:
                return self

            def __exit__(self, *args: object) -> None:
                return None

            @staticmethod
            def read() -> bytes:
                return b'{"data":[{"id":"model","status":"Active"}]}'

        with (
            patch(
                "dcar_eval.v8.selling_point_paid_eval.urllib.request.urlopen",
                side_effect=[http.client.IncompleteRead(b"partial", 10), Response()],
            ) as urlopen,
            patch("dcar_eval.v8.selling_point_paid_eval.time.sleep") as sleep,
        ):
            catalog = _model_catalog(
                {"api_base": "https://example.test", "api_key": "key"}
            )
        self.assertEqual(catalog["model"]["status"], "Active")
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(2)

    def test_manifest_groups_dedupe_content_and_keep_v0_zero_call_group(self) -> None:
        rows = [
            self._row(45, 10, "X1", "懂车帝车型库外观"),
            self._row(168, 10, "X9", "懂车帝车型库外观"),
            self._row(98, 20, "X8", "", level="V0"),
        ]
        groups = manifest_groups(rows)
        self.assertEqual(len(groups), 2)
        duplicate = next(group for group in groups if group["content_id"] == 10)
        self.assertEqual(duplicate["excel_rows"], [45, 168])
        v0 = next(group for group in groups if group["content_id"] == 20)
        self.assertEqual(v0["evidence_level"], "V0")

    def test_reservation_includes_two_calls_and_uses_public_upper_bound(self) -> None:
        rows = [
            self._row(1, 1, "X9", "懂车帝车型库外观"),
            self._row(2, 2, "X1", "懂车帝车型PK对比"),
        ]
        groups = manifest_groups(rows)
        index = CharNgramTfidfIndex(rows)
        reservation = estimate_reservation(
            groups,
            index=index,
            models=[self._model()],
        )
        self.assertEqual(reservation["unique_model_targets"], 2)
        self.assertEqual(reservation["by_model"][0]["potential_calls"], 4)
        self.assertGreater(reservation["total_reserved_cny"], 0)

    def test_accepted_wide_margin_uses_one_call(self) -> None:
        rows = [
            self._row(1, 1, "X9", "懂车帝车型库外观"),
            self._row(2, 2, "X1", "懂车帝车型PK对比"),
        ]
        group = manifest_groups(rows)[0]
        index = CharNgramTfidfIndex(rows)
        calls: list[str] = []

        def caller(model: ModelPrice, system: str, user: str) -> ApiCallResult:
            del model, system, user
            calls.append("called")
            return ApiCallResult(self._response("X9", 0.91, 0.70), 100, 30, 10)

        result = evaluate_group(
            group,
            model=self._model(),
            index=index,
            caller=caller,
            valid_codes=self.labels["cards"],
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(result["decision"]["primary_code"], "X9")
        self.assertEqual(result["status"], "accepted")

    def test_close_top2_uses_exactly_one_second_call(self) -> None:
        rows = [
            self._row(1, 1, "X9", "懂车帝车型库外观"),
            self._row(2, 2, "X1", "懂车帝车型PK对比"),
        ]
        group = manifest_groups(rows)[0]
        index = CharNgramTfidfIndex(rows)
        responses = iter(
            [
                self._response("X9", 0.80, 0.70),
                self._response("X9", 0.90, 0.60),
            ]
        )

        def caller(model: ModelPrice, system: str, user: str) -> ApiCallResult:
            del model, system, user
            return ApiCallResult(next(responses), 100, 30, 10)

        result = evaluate_group(
            group,
            model=self._model(),
            index=index,
            caller=caller,
            valid_codes=self.labels["cards"],
        )
        self.assertEqual(len(result["calls"]), 2)
        self.assertEqual(result["decision"]["primary_code"], "X9")

    def test_g1_main_pass_defers_valid_boundary_second_call(self) -> None:
        rows = [
            self._row(1, 1, "X9", "懂车帝车型库外观"),
            self._row(2, 2, "X1", "懂车帝车型PK对比"),
        ]
        group = manifest_groups(rows)[0]
        index = CharNgramTfidfIndex(rows)
        calls: list[str] = []

        def caller(model: ModelPrice, system: str, user: str) -> ApiCallResult:
            del model, system, user
            calls.append("called")
            return ApiCallResult(self._response("X9", 0.80, 0.70), 100, 30, 10)

        result = evaluate_group(
            group,
            model=self._model(),
            index=index,
            caller=caller,
            valid_codes=self.labels["cards"],
            allow_boundary_second_pass=False,
        )
        self.assertEqual(len(calls), 1)
        self.assertEqual(result["decision"]["primary_code"], "X9")

    def test_transport_disconnect_retries_once_and_reserves_unknown_cost(self) -> None:
        response = json.dumps(
            {
                "choices": [{"message": {"content": "{}"}}],
                "usage": {"prompt_tokens": 100, "completion_tokens": 10},
            }
        ).encode("utf-8")

        with (
            patch(
                "dcar_eval.v8.selling_point_paid_eval.subprocess.run",
                side_effect=[
                    subprocess.TimeoutExpired("curl", 50),
                    subprocess.CompletedProcess(
                        args=["curl"],
                        returncode=0,
                        stdout=response + b"\n200",
                        stderr=b"",
                    ),
                ],
            ) as run,
            patch(
                "dcar_eval.v8.selling_point_paid_eval.shutil.which",
                return_value="/usr/bin/curl",
            ),
            patch("dcar_eval.v8.selling_point_paid_eval.time.sleep") as sleep,
        ):
            result = _post_chat(
                {
                    "api_base": "https://example.test/api/v3",
                    "endpoint": "/chat/completions",
                    "api_key": "not-a-real-key",
                },
                self._model(),
                "system",
                "user",
            )
        self.assertEqual(run.call_count, 2)
        sleep.assert_called_once_with(2)
        self.assertEqual(result.unmetered_transport_attempts, 1)
        self.assertGreater(result.reserved_input_tokens, 0)
        self.assertEqual(result.reserved_output_tokens, 800)
        command = run.call_args_list[0].args[0]
        self.assertIn("--max-time", command)
        self.assertEqual(command[command.index("--max-time") + 1], "45")
        self.assertNotIn("not-a-real-key", " ".join(command))

    def test_main_pending_reservation_includes_retries_and_other_phase_spend(self) -> None:
        model = ModelPrice("test", "test", "test", 0, 1, False, "", None)
        groups = [{"group_key": "1", "evidence_level": "V3", "target": {}}]
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(paid, "load_model_config", return_value={
                "models_loaded": [model], "budget_limit_cny": 0.006,
                "config_sha256": "config", "queried_at": "frozen", "cost_basis": "test"}),
            patch.object(paid, "CharNgramTfidfIndex", return_value=Mock(index_sha256="index")),
            patch.object(paid, "manifest_groups", return_value=groups),
            patch.object(paid, "build_prompt", return_value={"system": "s", "user": "u"}),
            patch.object(paid, "llm_config", return_value={}),
            patch.object(paid, "verify_model_availability", return_value={}),
            patch.object(paid, "evaluate_group") as evaluate,
        ):
            # A group can make two logical calls, each with two transport
            # attempts: 4 * 0.0008. The other phase already spent 0.003.
            with self.assertRaisesRegex(SellingPointPaidEvalError, "shared budget"):
                run_bakeoff(
                    manifest={"manifest_sha256": "manifest", "rows": [{}] * 228},
                    model_config_path=Path("unused"), output_dir=Path(directory),
                    concurrency=2, shared_spent_cny_upper_bound=0.003,
                    caller=Mock(),
                )
        evaluate.assert_not_called()

    def test_actual_prompt_budget_guard_is_shared_by_concurrent_callers(self) -> None:
        model = ModelPrice("test", "test", "test", 0, 1, False, "", None)
        caller = Mock(return_value=ApiCallResult("{}", 1, 1, 1))
        guarded = paid.budgeted_caller(caller, remaining_cny=0.0032)

        def invoke(_: int) -> bool:
            try:
                guarded(model, "system", "user")
                return True
            except SellingPointCallError as error:
                self.assertEqual(error.attempts, 0)
                self.assertEqual(error.reserved_output_tokens, 0)
                return False

        with ThreadPoolExecutor(max_workers=8) as executor:
            outcomes = list(executor.map(invoke, range(8)))
        self.assertEqual(sum(outcomes), 2)
        self.assertEqual(caller.call_count, 2)

    def test_actual_repair_prompt_is_budgeted_before_call(self) -> None:
        model = ModelPrice("test", "test", "test", 1, 0, False, "", None)
        caller = Mock(return_value=ApiCallResult("{}", 1, 1, 1))
        guarded = paid.budgeted_caller(caller, remaining_cny=0.001)
        guarded(model, "s", "u")
        with self.assertRaises(SellingPointCallError):
            guarded(model, "s", "x" * 5000)
        self.assertEqual(caller.call_count, 1)

    def test_rate_limit_retry_waits_for_account_window(self) -> None:
        rate_limited = subprocess.CompletedProcess(
            args=["curl"],
            returncode=0,
            stdout=(
                b'{"error":{"code":"ModelAccountRpmRateLimitExceeded"}}\n429'
            ),
            stderr=b"",
        )
        success = subprocess.CompletedProcess(
            args=["curl"],
            returncode=0,
            stdout=(
                b'{"choices":[{"message":{"content":"{}"}}],'
                b'"usage":{"prompt_tokens":100,"completion_tokens":10}}\n200'
            ),
            stderr=b"",
        )
        with (
            patch(
                "dcar_eval.v8.selling_point_paid_eval.subprocess.run",
                side_effect=[rate_limited, success],
            ),
            patch(
                "dcar_eval.v8.selling_point_paid_eval.shutil.which",
                return_value="/usr/bin/curl",
            ),
            patch("dcar_eval.v8.selling_point_paid_eval.time.sleep") as sleep,
        ):
            result = _post_chat(
                {
                    "api_base": "https://example.test/api/v3",
                    "endpoint": "/chat/completions",
                    "api_key": "not-a-real-key",
                },
                self._model(),
                "system",
                "user",
            )
        self.assertEqual(result.input_tokens, 100)
        sleep.assert_called_once_with(20)

    def test_only_transport_errors_are_resume_retryable(self) -> None:
        self.assertTrue(
            retryable_transport_result(
                {"status": "error", "validation_note": "transport_failure: 429"}
            )
        )
        self.assertFalse(
            retryable_transport_result(
                {"status": "error", "validation_note": "invalid JSON"}
            )
        )

    def test_deepseek_runtime_is_serialized_without_throttling_other_models(self) -> None:
        ordinary = self._model()
        deepseek = ModelPrice(
            slot="deepseek",
            requested_model="deepseek",
            model="deepseek-v4-flash-ga-260731",
            input_rate=1.0,
            output_rate=2.0,
            supports_json_object=False,
            price_source_url="https://example.test/price",
            replacement_reason=None,
        )
        self.assertEqual(model_concurrency_limit(ordinary, 6), 6)
        self.assertEqual(model_concurrency_limit(deepseek, 6), 1)

    def test_run_persists_other_results_after_one_transport_failure(self) -> None:
        rows = [
            self._row(row_number, 1, "X9", "懂车帝车型库外观")
            for row_number in range(1, 229)
        ]
        manifest = {"manifest_sha256": "a" * 64, "rows": rows}

        def fake_evaluate(
            group: dict[str, object],
            *,
            model: ModelPrice,
            **_: object,
        ) -> dict[str, object]:
            if model.slot == "quality_default":
                failure = SellingPointCallError(
                    "simulated timeout",
                    attempts=2,
                    reserved_input_tokens=100,
                    reserved_output_tokens=20,
                )
                raise GroupEvaluationError(
                    "simulated timeout", completed_calls=(), failure=failure
                )
            return {
                "model": model.model,
                "group_key": str(group["group_key"]),
                "status": "accepted",
                "calls": [
                    {
                        "input_tokens": 1,
                        "output_tokens": 1,
                        "latency_ms": 1,
                        "cost_cny_upper_bound": 0.01,
                    }
                ],
                "cost_cny_upper_bound": 0.01,
            }

        with (
            tempfile.TemporaryDirectory() as temporary,
            patch(
                "dcar_eval.v8.selling_point_paid_eval.llm_config",
                return_value={},
            ),
            patch(
                "dcar_eval.v8.selling_point_paid_eval.verify_model_availability",
                return_value={"queried_at": "ignored"},
            ),
            patch(
                "dcar_eval.v8.selling_point_paid_eval.evaluate_group",
                side_effect=fake_evaluate,
            ),
        ):
            output_dir = Path(temporary)
            run = run_bakeoff(
                manifest=manifest,
                model_config_path=DEFAULT_MODEL_CONFIG_PATH,
                output_dir=output_dir,
                concurrency=3,
                caller=lambda *_: ApiCallResult("{}", 1, 1, 1),
            )
            results = [
                json.loads(line)
                for line in (output_dir / "group_results.ndjson")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            failures = [
                json.loads(line)
                for line in (output_dir / "transport_failures.ndjson")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            receipt = json.loads(
                (output_dir / "receipt.json").read_text(encoding="utf-8")
            )
        self.assertEqual(len(results), 3)
        self.assertEqual(
            [result["status"] for result in results].count("error"), 1
        )
        self.assertEqual(len(failures), 1)
        self.assertEqual(
            run["summary"]["g1_contract"],
            {
                "pass_count": 160,
                "denominator": 228,
                "scene_denominators": {"X": 112, "E": 93, "M": 23},
                "scene_pass_counts": {"X": 84, "E": 66, "M": 14},
            },
        )
        self.assertEqual(
            run["summary"]["total_cost_cny_upper_bound"],
            receipt["actual_cost_cny_upper_bound"],
        )

    def test_validation_error_is_a_terminal_incorrect_result(self) -> None:
        rows = [
            self._row(row_number, 1, "X9", "懂车帝车型库外观")
            for row_number in range(1, 229)
        ]
        manifest = {"manifest_sha256": "b" * 64, "rows": rows}
        calls: list[str] = []

        def fake_evaluate(
            group: dict[str, object],
            *,
            model: ModelPrice,
            **_: object,
        ) -> dict[str, object]:
            calls.append(model.model)
            return {
                "model": model.model,
                "group_key": str(group["group_key"]),
                "status": (
                    "error" if model.slot == "quality_default" else "accepted"
                ),
                "decision": None,
                "calls": [
                    {
                        "input_tokens": 1,
                        "output_tokens": 1,
                        "latency_ms": 1,
                        "cost_cny_upper_bound": 0.01,
                    }
                ],
                "cost_cny_upper_bound": 0.01,
            }

        with (
            tempfile.TemporaryDirectory() as temporary,
            patch(
                "dcar_eval.v8.selling_point_paid_eval.llm_config",
                return_value={},
            ),
            patch(
                "dcar_eval.v8.selling_point_paid_eval.verify_model_availability",
                return_value={"queried_at": "ignored"},
            ),
            patch(
                "dcar_eval.v8.selling_point_paid_eval.evaluate_group",
                side_effect=fake_evaluate,
            ),
        ):
            output_dir = Path(temporary)
            first = run_bakeoff(
                manifest=manifest,
                model_config_path=DEFAULT_MODEL_CONFIG_PATH,
                output_dir=output_dir,
                concurrency=3,
                caller=lambda *_: ApiCallResult("{}", 1, 1, 1),
            )
            second = run_bakeoff(
                manifest=manifest,
                model_config_path=DEFAULT_MODEL_CONFIG_PATH,
                output_dir=output_dir,
                concurrency=3,
                caller=lambda *_: ApiCallResult("{}", 1, 1, 1),
            )
        self.assertEqual(len(calls), 3)
        self.assertEqual(first["newly_completed_group_model_results"], 3)
        self.assertEqual(second["newly_completed_group_model_results"], 0)

    def test_latest_terminal_attempt_wins_without_losing_attempt_history(self) -> None:
        results = [
            {"model": "m", "group_key": "g", "status": "error"},
            {"model": "m", "group_key": "g", "status": "accepted"},
        ]
        terminal = terminal_results_by_key(results)
        self.assertEqual(len(terminal), 1)
        self.assertEqual(terminal[("m", "g")]["status"], "accepted")


if __name__ == "__main__":
    unittest.main()
