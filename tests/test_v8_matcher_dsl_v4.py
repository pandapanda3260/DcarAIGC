from __future__ import annotations

import unittest
from typing import Any

from v8.matcher_dsl import (  # type: ignore[import-untyped]
    V4_BUNDLE_PATH,
    V5_1_POINT_SPEC,
    V5_2_POINT_SPEC,
    MatcherDslError,
    MaterializedMatcher,
    bundle_sha256,
    load_bundle,
    materialize_point_rule,
    match_points,
)


V4_BUNDLE_SHA256 = "5d5a823e1019600778c86e87325d32fad12619a55f3c02afe803f5a91d9886b8"
V5_2_MATCHER_SHA256 = "73fbaed084173d589e5d31d94d1bae0ba6ebcf859db77ae65abb8ef9aae008fe"


class MatcherDslV4Test(unittest.TestCase):
    bundle: dict[str, Any]
    matcher: MaterializedMatcher

    @classmethod
    def setUpClass(cls) -> None:
        cls.bundle = load_bundle(V4_BUNDLE_PATH, point_spec=V5_2_POINT_SPEC)
        rules = {
            point_id: materialize_point_rule(
                cls.bundle,
                point_id,
                point_spec=V5_2_POINT_SPEC,
            )
            for point_id in V5_2_POINT_SPEC
        }
        cls.matcher = MaterializedMatcher(rules, point_spec=V5_2_POINT_SPEC)

    def _match(self, text: str) -> list[dict[str, Any]]:
        return match_points(
            self.bundle,
            {},
            {"status": "success", "text": text, "avg_logprob": -0.1},
            {"status": "success", "combined_text": ""},
            "V2",
            {},
            point_spec=V5_2_POINT_SPEC,
        )

    def _ids(self, text: str) -> set[str]:
        return {str(item["id"]) for item in self._match(text)}

    def test_v5_2_point_spec_and_bundle_are_locked(self) -> None:
        expected = {
            *(f"E{index}" for index in range(1, 11)),
            *(f"X{index}" for index in range(1, 12)),
            *(f"M{index}" for index in range(1, 7)),
            "M8",
        }
        self.assertEqual(
            set(V5_1_POINT_SPEC),
            {
                *(f"E{index}" for index in range(1, 8)),
                *(f"X{index}" for index in range(1, 9)),
                *(f"M{index}" for index in range(1, 7)),
                *(f"C{index}" for index in range(1, 5)),
            },
        )
        self.assertEqual(set(V5_2_POINT_SPEC), expected)
        self.assertEqual({rule["point_id"] for rule in self.bundle["rules"]}, expected)
        self.assertNotIn("M7", expected)
        self.assertFalse(any(code.startswith("C") for code in expected))
        self.assertEqual(
            self.bundle["scoring"]["force_linkage_points"],
            [f"M{index}" for index in range(1, 7)],
        )
        self.assertEqual(
            bundle_sha256(self.bundle, point_spec=V5_2_POINT_SPEC),
            V4_BUNDLE_SHA256,
        )
        self.assertEqual(self.matcher.matcher_rule_sha256, V5_2_MATCHER_SHA256)
        with self.assertRaises(MatcherDslError):
            load_bundle(V4_BUNDLE_PATH)

    def test_c1_split_requires_explicit_scene_and_is_mutually_exclusive(self) -> None:
        used = self._ids(
            "买二手车如何识别事故车和泡水车，先检查底盘与维修记录，记住这些判断方法。"
        )
        new = self._ids(
            "新车交付验车时如何检查轮胎和刹车，提新车一定要注意这些步骤和方法。"
        )
        generic = self._ids(
            "机械把手和隐藏电动把手有什么区别，要注意断电时的故障风险。"
        )
        mixed = self._ids(
            "买二手车也有人说是新车，如何检查事故车、轮胎和底盘，记住判断方法。"
        )
        self.assertIn("E8", used)
        self.assertNotIn("X11", used)
        self.assertIn("X11", new)
        self.assertNotIn("E8", new)
        self.assertFalse({"E8", "X11"} & generic)
        self.assertIn("E8", mixed)
        self.assertNotIn("X11", mixed)

    def test_c2_and_c4_splits_are_mutually_exclusive(self) -> None:
        used_model = self._ids(
            "带你实拍这辆二手车源的车况和表显里程，展示内饰、后排和后备箱。"
        )
        new_model = self._ids(
            "带你实拍这款在售新车型的内饰和后排空间，展示座椅与后备箱。"
        )
        used_news = self._ids("二手车市场销量排行正式发布，本月二手行情出现明显变化。")
        new_news = self._ids("这款新车正式上市，发布会公布官图、申报图和预售信息。")
        default_media = self._ids(
            "懂车帝资讯服务的年度内容栏目正式发布，持续提供行业信息。"
        )
        self.assertIn("E9", used_model)
        self.assertNotIn("X9", used_model)
        self.assertIn("X9", new_model)
        self.assertNotIn("E9", new_model)
        self.assertIn("E10", used_news)
        self.assertNotIn("X10", used_news)
        self.assertIn("X10", new_news)
        self.assertNotIn("E10", new_news)
        self.assertNotIn("X10", default_media)

    def test_m8_and_materialized_runtime_emit_only_v5_2_codes(self) -> None:
        text = "来懂车帝玩车社区参加主题改装大赛，分享自己的改装作品。"
        direct = self._match(text)
        materialized = self.matcher.match_points(
            {},
            {"status": "success", "text": text, "avg_logprob": -0.1},
            {"status": "success", "combined_text": ""},
            "V2",
            {},
        )
        self.assertEqual(materialized, direct)
        self.assertIn("M8", {item["id"] for item in direct})
        self.assertTrue({item["id"] for item in direct} <= set(V5_2_POINT_SPEC))
        partial = {
            code: materialize_point_rule(
                self.bundle,
                code,
                point_spec=V5_2_POINT_SPEC,
            )
            for code in set(V5_2_POINT_SPEC) - {"M8"}
        }
        with self.assertRaisesRegex(MatcherDslError, "approved 28"):
            MaterializedMatcher(partial, point_spec=V5_2_POINT_SPEC)


if __name__ == "__main__":
    unittest.main()
