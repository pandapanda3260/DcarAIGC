from __future__ import annotations

import copy
import unittest
from typing import Any

from v8.matcher_dsl import (  # type: ignore[import-not-found]
    V4_BUNDLE_PATH,
    V5_1_POINT_SPEC,
    V5_2_POINT_SPEC,
    MatcherDslError,
    MaterializedMatcher,
    load_bundle,
    materialize_point_rule,
    match_points,
    validate_bundle,
)


def _matches(bundle: dict[str, object], text: str) -> list[dict[str, object]]:
    return match_points(
        bundle,
        {},
        {"status": "success", "text": text, "avg_logprob": -0.1},
        {"status": "success", "combined_text": ""},
        "V2",
        point_spec=V5_2_POINT_SPEC,
    )


class MatcherV52Test(unittest.TestCase):
    bundle: dict[str, Any]
    materialized: MaterializedMatcher

    @classmethod
    def setUpClass(cls) -> None:
        cls.bundle = load_bundle(V4_BUNDLE_PATH, point_spec=V5_2_POINT_SPEC)
        cls.materialized = MaterializedMatcher(
            {
                code: materialize_point_rule(
                    cls.bundle, code, point_spec=V5_2_POINT_SPEC
                )
                for code in V5_2_POINT_SPEC
            },
            point_spec=V5_2_POINT_SPEC,
        )

    def test_exact_28_point_contract_has_no_c_or_m7(self) -> None:
        self.assertEqual(set(V5_2_POINT_SPEC), {r["point_id"] for r in self.bundle["rules"]})
        self.assertEqual(len(V5_2_POINT_SPEC), 28)
        self.assertFalse({"C1", "C2", "C3", "C4", "M7"} & set(V5_2_POINT_SPEC))
        with self.assertRaises(MatcherDslError):
            validate_bundle(self.bundle, point_spec=V5_1_POINT_SPEC)
        partial = copy.deepcopy(self.bundle)
        partial["rules"] = partial["rules"][:-1]
        with self.assertRaises(MatcherDslError):
            validate_bundle(partial, point_spec=V5_2_POINT_SPEC)

    def test_scene_splits_are_mutually_exclusive(self) -> None:
        cases = (
            ("买二手车如何识别事故车和泡水车，先检查底盘与维修记录。", "E8", "X11"),
            ("新车交付验车要检查轮胎、底盘和漆面，今天讲清楚检查步骤。", "X11", "E8"),
            ("实拍这辆二手车源的车况和表显里程，展示内饰和后排空间。", "E9", "X9"),
            ("实拍这款在售新车型的内饰和后排空间，展示座椅与后备箱。", "X9", "E9"),
            ("二手车市场最新销量榜正式发布，二手行情出现明显变化。", "E10", "X10"),
            ("这款新车正式上市，发布会公布官图、申报图和预售信息。", "X10", "E10"),
        )
        for text, included, excluded in cases:
            with self.subTest(included=included):
                ids = {str(item["id"]) for item in _matches(self.bundle, text)}
                self.assertIn(included, ids)
                self.assertNotIn(excluded, ids)

    def test_generic_knowledge_and_old_media_default_do_not_become_new_car(self) -> None:
        generic = "汽车保养时机油和滤芯怎么换，今天讲清楚正确维修方法。"
        old_media = "懂车帝资讯服务的年度内容栏目正式发布，持续提供行业信息。"
        self.assertNotIn("X11", {str(i["id"]) for i in _matches(self.bundle, generic)})
        self.assertNotIn("X10", {str(i["id"]) for i in _matches(self.bundle, old_media)})

    def test_community_maps_exactly_to_m8_and_materialized_matches_direct(self) -> None:
        text = "来懂车帝玩车社区参加主题改装大赛，分享自己的改装作品。"
        direct = _matches(self.bundle, text)
        materialized = self.materialized.match_points(
            {},
            {"status": "success", "text": text, "avg_logprob": -0.1},
            {"status": "success", "combined_text": ""},
            "V2",
        )
        self.assertEqual(materialized, direct)
        m8 = next(item for item in direct if item["id"] == "M8")
        self.assertEqual(m8["scene"], "media")


if __name__ == "__main__":
    unittest.main()
