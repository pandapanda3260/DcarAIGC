from __future__ import annotations

import copy
import hashlib
import math
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

import label_douyin_video_evidence_v3 as legacy  # type: ignore[import-untyped]
import tests.test_label_douyin_video_evidence_v3 as legacy_seed
from v8.matcher_dsl import (  # type: ignore[import-untyped]
    DEFAULT_BUNDLE_PATH,
    ENGINE_VERSION,
    POINT_SCENES,
    MatcherDslError,
    MaterializedMatcher,
    bundle_sha256,
    load_bundle,
    materialize_point_rule,
    match_materialized_rules,
    match_points,
    project_rule_explain,
    validate_bundle,
)


LEGACY_PATH = Path(legacy.__file__).resolve()
LEGACY_SHA256 = "38f647e9b05e38777bbe4727b5c563b67c61e28854d8b37027af8119023eefdc"
BUNDLE_SHA256 = "c7dbfc8a598d68fa4498598f8c87da9d4cef94aec1aa2cf9d553f28eeda2598b"


def _without_scene(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {key: value for key, value in item.items() if key != "scene"} for item in items
    ]


def _asr_match(bundle: dict[str, Any], text: str) -> list[dict[str, Any]]:
    return match_points(
        bundle,
        {},
        {"status": "success", "text": text, "avg_logprob": -0.1},
        {"status": "success", "combined_text": ""},
        "V2",
    )


class MatcherDslParityTest(unittest.TestCase):
    bundle: dict[str, Any]

    @classmethod
    def setUpClass(cls) -> None:
        cls.bundle = load_bundle()

    def test_legacy_sha_is_locked_and_existing_40_tests_are_full_output_oracles(
        self,
    ) -> None:
        self.assertEqual(
            hashlib.sha256(LEGACY_PATH.read_bytes()).hexdigest(), LEGACY_SHA256
        )
        self.assertEqual(self.bundle["legacy_matcher_sha256"], LEGACY_SHA256)
        original_match_points = legacy.match_points
        materialized_matcher = MaterializedMatcher(
            {
                point_id: materialize_point_rule(self.bundle, point_id)
                for point_id in POINT_SCENES
            }
        )
        compared = 0

        def oracle(
            row: dict[str, Any],
            transcript: dict[str, Any],
            ocr: dict[str, Any],
            evidence: str,
            visual: dict[str, Any] | None = None,
        ) -> list[dict[str, Any]]:
            nonlocal compared
            expected = original_match_points(row, transcript, ocr, evidence, visual)
            actual = match_points(self.bundle, row, transcript, ocr, evidence, visual)
            self.assertEqual(_without_scene(actual), expected)
            materialized = materialized_matcher.match_points(
                row, transcript, ocr, evidence, visual
            )
            self.assertEqual(materialized, actual)
            compared += 1
            return expected

        suite = unittest.defaultTestLoader.loadTestsFromTestCase(
            legacy_seed.V3RulesTest
        )
        result = unittest.TestResult()
        with patch.object(legacy_seed, "match_points", oracle):
            suite.run(result)
        self.assertEqual(result.testsRun, 40)
        self.assertEqual(compared, 40)
        self.assertEqual(result.failures, [])
        self.assertEqual(result.errors, [])

    def test_each_of_25_points_has_a_positive_legacy_parity_fixture(self) -> None:
        fixtures = {
            "E1": "在懂车帝买二手车，专业收车团队可以上门评估，完成卖车到账服务。",
            "E2": "在懂车帝买二手车，可以查看认证车源和透明车况，并获得平台保障。",
            "E3": "买二手车预算五万元，车型推荐要优先看可靠车源与行情信息。",
            "E4": "家庭通勤买二手车预算五万元，推荐适合家用的高性价比车型。",
            "E5": "在懂车帝买二手车，可以查看专业检测报告和车辆档案识别问题。",
            "E6": "在懂车帝买二手车先查询估价和市场行情，了解历史价格减少信息差。",
            "E7": "在懂车帝买二手车要了解过户和交易合同，平台提供权益保障。",
            "X1": "奥迪和宝马两款车怎么选，全面对比空间、动力和配置表现。",
            "X2": "懂车帝实测榜提供权威测评和真实车主口碑，帮助了解长期表现。",
            "X3": "奥迪新车现在一口价19.9万，懂车帝还可以查询最新报价。",
            "X4": "家庭通勤第一辆车该怎么选车，按预算推荐适合家用的车型。",
            "X5": "这款车不同版本配置对比，分析高低配和选装包的配置差异。",
            "X6": "一年养车成本怎么算，每年保险和油费合计需要多少费用。",
            "X7": "通过拆解和测试数据验证减配问题，识别配置骗局和营销话术。",
            "X8": "签订购车合同必须看清条款，注意交付权益和退定金流程。",
            "M1": "使用AI小懂预约试驾，还能提前获取详细试驾指南和试驾清单。",
            "M2": "使用AI小懂查询真实优惠和库存，了解现车展车和试驾车情况。",
            "M3": "使用AI小懂预约洗车和维修保养，并通过车档案管理日常用车。",
            "M4": "使用AI小懂自然语言问答选车，根据预算推荐车型并完成对比。",
            "M5": "使用AI小懂解释故障报警灯和车辆异响，并给出安全处置建议。",
            "M6": "使用AI小懂查询车辆估价，提前完成卖车流程所需的准备工作。",
            "C1": "机械把手和隐藏电动把手有什么区别，要注意断电时的故障风险。",
            "C2": "带你实拍这款在售车型的内饰和后排空间，展示座椅与后备箱。",
            "C3": "来懂车帝玩车社区参加主题改装大赛，分享自己的改装作品。",
            "C4": "这款新车正式上市，发布会公布官图、申报图和预售信息。",
        }
        self.assertEqual(set(fixtures), set(POINT_SCENES))
        for point_id, text in fixtures.items():
            with self.subTest(point_id=point_id):
                row: dict[str, Any] = {}
                transcript = {"text": text}
                ocr: dict[str, Any] = {"combined_text": ""}
                expected = legacy.match_points(row, transcript, ocr, "V2", {})
                actual = match_points(self.bundle, row, transcript, ocr, "V2", {})
                self.assertEqual(_without_scene(actual), expected)
                self.assertIn(point_id, {item["id"] for item in actual})

    def test_materialized_database_runtime_preserves_full_bundle_output(self) -> None:
        rules = {
            point_id: materialize_point_rule(self.bundle, point_id)
            for point_id in POINT_SCENES
        }
        matcher = MaterializedMatcher(rules)
        cases = (
            "买二手车先看事故车检测报告和市场行情，再了解过户交易保障。",
            "新车正式上市，公布一口价和高低配配置，家庭通勤应该怎么选车。",
            "使用AI小懂预约试驾、查询库存优惠并解释车辆故障报警灯。",
        )
        for text in cases:
            with self.subTest(text=text):
                expected = match_points(
                    self.bundle, {}, {"text": text}, {"combined_text": ""}, "V2", {}
                )
                actual = matcher.match_points(
                    {}, {"text": text}, {"combined_text": ""}, "V2", {}
                )
                self.assertEqual(actual, expected)
                self.assertEqual(
                    match_materialized_rules(
                        rules,
                        {},
                        {"text": text},
                        {"combined_text": ""},
                        "V2",
                        {},
                    ),
                    expected,
                )

    def test_materialized_runtime_rejects_partial_or_inconsistent_snapshots(
        self,
    ) -> None:
        rules = {
            point_id: materialize_point_rule(self.bundle, point_id)
            for point_id in POINT_SCENES
        }
        partial = dict(rules)
        partial.pop("C1")
        with self.assertRaisesRegex(MatcherDslError, "exactly the approved 25"):
            MaterializedMatcher(partial)

        inconsistent = copy.deepcopy(rules)
        inconsistent["C1"]["thresholds"]["review_min"] = 59
        with self.assertRaisesRegex(MatcherDslError, "shared matcher fields"):
            MaterializedMatcher(inconsistent)

        unsupported_limit = copy.deepcopy(rules)
        for rule in unsupported_limit.values():
            rule["thresholds"]["max_secondary"] = 3
        with self.assertRaisesRegex(MatcherDslError, "requires.*max_secondary=2"):
            MaterializedMatcher(unsupported_limit)

    def test_custom_source_scope_uses_source_text_not_source_name(self) -> None:
        x3 = match_points(
            self.bundle,
            {},
            {"text": "奥迪新车现在一口价19.9万，优惠信息已经公布。"},
            {"combined_text": ""},
            "V2",
        )
        x3_match = next(item for item in x3 if item["id"] == "X3")
        self.assertEqual(x3_match["source"], "ASR")
        x4 = match_points(
            self.bundle,
            {},
            {"text": "这是一个普通汽车介绍，没有选车建议。"},
            {"combined_text": "家庭通勤第一辆车怎么选车"},
            "V2",
        )
        x4_match = next(item for item in x4 if item["id"] == "X4")
        self.assertEqual(x4_match["source"], "OCR")

    def test_terms_near_handles_high_frequency_text_without_cartesian_work(
        self,
    ) -> None:
        far_apart = "预算" * 5000 + "填" * 100 + "选车" * 5000
        ids = {item["id"] for item in _asr_match(self.bundle, far_apart)}
        self.assertNotIn("X4", ids)
        close = "预算十五万以内，应该如何选车"
        self.assertIn("X4", {item["id"] for item in _asr_match(self.bundle, close)})


class MatcherDslSceneTest(unittest.TestCase):
    bundle: dict[str, Any]

    @classmethod
    def setUpClass(cls) -> None:
        cls.bundle = load_bundle()

    def test_dynamic_c_scenes_are_candidate_local_and_machine_readable(self) -> None:
        cases = (
            (
                "C1",
                "买二手车如何识别事故车和泡水车，先检查底盘与维修记录。",
                "used_car",
            ),
            (
                "C1",
                "汽车保养时机油和滤芯怎么换，今天讲清楚正确维修方法。",
                "media",
            ),
            (
                "C2",
                "带你实拍这辆二手车源的车况和表显里程，展示内饰与后排。",
                "used_car",
            ),
            (
                "C2",
                "带你实拍在售新车型的内饰和后排空间，展示座椅与后备箱。",
                "new_car",
            ),
            (
                "C3",
                "来懂车帝玩车社区参加主题改装大赛，分享自己的改装作品。",
                "media",
            ),
            (
                "C4",
                "二手车市场最新销量榜正式发布，二手行情出现明显变化。",
                "used_car",
            ),
            (
                "C4",
                "这款新车正式上市，发布会公布官图、申报图和预售信息。",
                "new_car",
            ),
            (
                "C4",
                "懂车帝资讯服务的年度内容栏目正式发布，持续提供行业信息。",
                "media",
            ),
        )
        for point_id, text, expected_scene in cases:
            with self.subTest(point_id=point_id, scene=expected_scene):
                item = next(
                    item
                    for item in _asr_match(self.bundle, text)
                    if item["id"] == point_id
                )
                self.assertEqual(item["scene"], expected_scene)

    def test_every_emitted_candidate_respects_its_point_scene_allowmap(self) -> None:
        text = (
            "在懂车帝买二手车先看事故车检测报告和市场行情；新车正式上市，"
            "同时用AI小懂查询库存并预约试驾。"
        )
        for item in _asr_match(self.bundle, text):
            self.assertIn(item["scene"], POINT_SCENES[item["id"]])


class MatcherDslValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.bundle = load_bundle()

    def _invalid(self, mutate: Any) -> None:
        value = copy.deepcopy(self.bundle)
        mutate(value)
        with self.assertRaises(MatcherDslError):
            validate_bundle(value)

    def test_bundle_hash_is_stable_and_validation_precedes_hashing(self) -> None:
        self.assertEqual(self.bundle["engine_version"], ENGINE_VERSION)
        self.assertEqual(bundle_sha256(self.bundle), BUNDLE_SHA256)
        self.assertEqual(load_bundle(DEFAULT_BUNDLE_PATH), self.bundle)
        changed = copy.deepcopy(self.bundle)
        changed["term_sets"]["M1_when"] = list(
            reversed(changed["term_sets"]["M1_when"])
        )
        self.assertNotEqual(bundle_sha256(changed), BUNDLE_SHA256)
        invalid = copy.deepcopy(self.bundle)
        invalid["unknown"] = True
        with self.assertRaises(MatcherDslError):
            bundle_sha256(invalid)

    def test_explain_projection_is_explicit_complete_and_immutable(self) -> None:
        for point_id, allowed_scenes in POINT_SCENES.items():
            with self.subTest(point_id=point_id):
                projection = project_rule_explain(self.bundle, point_id)
                self.assertTrue(projection["positive_evidence"])
                self.assertTrue(projection["negative_evidence"])
                self.assertTrue(projection["boundary_rules"])
                self.assertEqual(set(projection["scenes"]), allowed_scenes)
                projection["positive_evidence"].clear()
                self.assertTrue(
                    project_rule_explain(self.bundle, point_id)["positive_evidence"]
                )

    def test_unknown_keys_fail_closed_at_every_schema_layer(self) -> None:
        mutations = (
            lambda value: value["normalization"].__setitem__("unknown", True),
            lambda value: value["views"]["desc"].__setitem__("unknown", True),
            lambda value: value["predicates"]["is_used"].__setitem__("unknown", True),
            lambda value: value["rules"][0].__setitem__("unknown", True),
            lambda value: value["rules"][0]["evidence"].__setitem__("unknown", True),
            lambda value: value["rules"][-1]["scene"].__setitem__("unknown", True),
            lambda value: value["rules"][-1]["scene"]["cases"][0].__setitem__(
                "unknown", True
            ),
            lambda value: value["scoring"].__setitem__("unknown", True),
            lambda value: value["thresholds"].__setitem__("unknown", True),
        )
        for index, mutation in enumerate(mutations):
            with self.subTest(index=index):
                self._invalid(mutation)

    def test_missing_keys_unknown_ops_bindings_and_local_scope_fail_closed(
        self,
    ) -> None:
        self._invalid(lambda value: value["rules"][0]["when"].pop("args"))
        self._invalid(
            lambda value: value["rules"][0].__setitem__(
                "when", {"op": "python_call", "name": "legacy.match_points"}
            )
        )
        self._invalid(
            lambda value: value["views"]["desc"].__setitem__(
                "source", "filesystem.secret"
            )
        )
        self._invalid(
            lambda value: value["rules"][0].__setitem__(
                "when",
                {
                    "op": "contains_any",
                    "view": "$source_text",
                    "terms": "M1_when",
                },
            )
        )

    def test_terms_regex_distance_windows_scores_and_scenes_are_bounded(self) -> None:
        self._invalid(lambda value: value["term_sets"].__setitem__("M1_when", []))
        self._invalid(
            lambda value: value["predicates"]["current_price_mention"].__setitem__(
                "pattern", "("
            )
        )
        self._invalid(
            lambda value: value["predicates"]["current_price_mention"].__setitem__(
                "before", 501
            )
        )
        self._invalid(
            lambda value: value["predicates"]["x4_source_match"]["args"][1].__setitem__(
                "distance", 501
            )
        )
        self._invalid(
            lambda value: value["rules"][0]["score"].__setitem__("semantic_fit", 101)
        )
        self._invalid(lambda value: value["rules"][0].__setitem__("priority", -1))
        self._invalid(lambda value: value["rules"][6].__setitem__("scene", "new_car"))

    def test_scene_branches_explain_and_scoring_contract_cannot_be_partial(
        self,
    ) -> None:
        self._invalid(lambda value: value["rules"][-1]["scene"]["cases"].pop())
        self._invalid(
            lambda value: value["rules"][0]["explain"]["positive_evidence"].clear()
        )
        self._invalid(
            lambda value: value["rules"][0]["explain"]["negative_evidence"].append(
                value["rules"][0]["explain"]["negative_evidence"][0]
            )
        )
        self._invalid(
            lambda value: value["rules"][0]["explain"]["boundary_rules"].__setitem__(
                0, " 带空格 "
            )
        )
        self._invalid(lambda value: value["scoring"]["force_linkage_points"].pop())
        self._invalid(
            lambda value: value["scoring"]["video_scores"].__setitem__("V2", 15)
        )
        self._invalid(lambda value: value["scoring"]["caps"].__setitem__("V1", 75))

    def test_expression_kinds_and_finite_numbers_fail_closed(self) -> None:
        def set_compare_right(value: dict[str, Any], right: Any) -> None:
            value["predicates"]["is_used"]["args"][0]["args"][0]["right"] = right

        def set_eq_string(value: dict[str, Any]) -> None:
            expression = value["predicates"]["is_used"]["args"][0]["args"][0]
            expression["cmp"] = "eq"
            expression["right"] = "0"

        self._invalid(lambda value: set_compare_right(value, "0"))
        self._invalid(lambda value: set_compare_right(value, True))
        self._invalid(lambda value: set_compare_right(value, math.nan))
        self._invalid(lambda value: set_compare_right(value, math.inf))
        self._invalid(set_eq_string)
        self._invalid(
            lambda value: value["predicates"]["is_image_post"]["args"][0].__setitem__(
                "value", math.nan
            )
        )
        self._invalid(
            lambda value: value["rules"][0].__setitem__(
                "when",
                {
                    "op": "term_count",
                    "view": "combined",
                    "terms": "M1_when",
                },
            )
        )
        self._invalid(
            lambda value: value["views"]["used_basis"].__setitem__(
                "when",
                {
                    "op": "term_count",
                    "view": "combined",
                    "terms": "used_narrative",
                },
            )
        )
        self._invalid(
            lambda value: value["rules"][-1]["scene"]["cases"][0].__setitem__(
                "when",
                {
                    "op": "term_count",
                    "view": "combined",
                    "terms": "c4_used_scene",
                },
            )
        )
        self._invalid(
            lambda value: next(
                rule for rule in value["rules"] if rule["point_id"] == "X3"
            )["evidence"].__setitem__(
                "source_expr",
                {
                    "op": "term_count",
                    "view": "$source_text",
                    "terms": "X3_evidence",
                },
            )
        )
        self._invalid(
            lambda value: value["predicates"]["current_price_mention"].__setitem__(
                "where",
                {
                    "op": "term_count",
                    "view": "$nearby",
                    "terms": "price_specific_cues",
                },
            )
        )

    def test_unsafe_regex_and_runtime_input_errors_use_matcher_error(self) -> None:
        self._invalid(
            lambda value: value["predicates"]["current_price_mention"].__setitem__(
                "pattern", "(a+)+$"
            )
        )
        self._invalid(
            lambda value: value["predicates"]["current_price_mention"].__setitem__(
                "pattern", r"(a)\1"
            )
        )
        with self.assertRaises(MatcherDslError):
            match_points(
                self.bundle,
                {"media_type": "not-an-integer"},
                {"text": "普通汽车内容"},
                {"combined_text": ""},
                "V2",
            )
        with tempfile.TemporaryDirectory() as temp_dir:
            invalid_path = Path(temp_dir) / "invalid.json"
            invalid_path.write_text("{", encoding="utf-8")
            with self.assertRaises(MatcherDslError):
                load_bundle(invalid_path)

    def test_predicate_view_and_cross_dependency_cycles_are_rejected(self) -> None:
        self._invalid(
            lambda value: value["predicates"].__setitem__(
                "is_image_post", {"op": "ref", "name": "is_image_post"}
            )
        )
        self._invalid(
            lambda value: value["views"]["video_text"]["views"].append("video_text")
        )
        self._invalid(
            lambda value: value["views"]["used_basis"].__setitem__(
                "when", {"op": "ref", "name": "is_used"}
            )
        )


if __name__ == "__main__":
    unittest.main()
