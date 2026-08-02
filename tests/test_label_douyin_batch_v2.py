#!/usr/bin/env python3

import unittest

from label_douyin_batch_v2 import classify_candidate_or_fallback, classify_official


def row(desc: str, uid: str = "test") -> dict[str, str]:
    return {"desc": desc, "uid": uid}


class TaxonomyV2RuleTests(unittest.TestCase):
    def test_brand_hashtag_alone_is_not_official(self) -> None:
        point, score, _, _ = classify_official(row("今天的车真帅 #懂车帝"))
        self.assertIsNone(point)
        self.assertEqual(score, 0)

    def test_non_auto_choice_is_not_vehicle_comparison(self) -> None:
        point, _, _, _ = classify_official(row("80亿和妈妈的微笑你选哪个？"))
        self.assertIsNone(point)

    def test_vague_vehicle_choice_is_pending_not_comprehensive_comparison(self) -> None:
        point, score, _, _ = classify_official(row("奥迪A4L和特斯拉Model 3选哪个？"))
        self.assertEqual(point, "X1")
        self.assertLess(score, 75)

    def test_multidimensional_vehicle_comparison_is_official(self) -> None:
        point, score, _, _ = classify_official(
            row("奥迪A4L和特斯拉Model 3全面对比：空间、动力、配置和长期成本怎么选")
        )
        self.assertEqual(point, "X1")
        self.assertGreaterEqual(score, 75)

    def test_explicit_platform_used_car_capability(self) -> None:
        desc = (
            "7款主流二手车APP对比，在懂车帝可以看个人车源和商家认证车源，"
            "还能完成选车、估价、验车一体化。#懂车帝"
        )
        point, score, level, _ = classify_official(row(desc))
        self.assertEqual(point, "E2")
        self.assertGreaterEqual(score, 75)
        self.assertEqual(level, "A")

    def test_vague_price_title_is_pending(self) -> None:
        point, score, _, _ = classify_official(
            row("奔驰C级大降价！#懂车帝", "294299856549959")
        )
        self.assertEqual(point, "X3")
        self.assertLess(score, 75)

    def test_numeric_new_car_price_is_official(self) -> None:
        point, score, _, _ = classify_official(row("奔驰GLC价格破新低，221,800就能开走"))
        self.assertEqual(point, "X3")
        self.assertGreaterEqual(score, 75)

    def test_chinese_written_exact_price_is_official(self) -> None:
        point, score, _, _ = classify_official(row("丰田锋兰达一口价七万两千八"))
        self.assertEqual(point, "X3")
        self.assertGreaterEqual(score, 75)

    def test_used_car_price_routes_to_used_market_data(self) -> None:
        point, score, _, _ = classify_official(row("这台二手车一口价只要16万 #懂车帝"))
        self.assertEqual(point, "E6")
        self.assertGreaterEqual(score, 75)

    def test_generic_knowledge_is_candidate_not_official(self) -> None:
        sample = row("汽车保养怎么做？教你三步避开常见故障。#懂车帝")
        point, _, _, _ = classify_official(sample)
        self.assertIsNone(point)
        candidate, score, _ = classify_candidate_or_fallback(sample, 1)
        self.assertEqual(candidate, "C1")
        self.assertGreaterEqual(score, 75)

    def test_ai_xiaodong_task_requires_explicit_name(self) -> None:
        point, score, level, _ = classify_official(row("打开AI小懂，一键预约试驾并生成试驾清单"))
        self.assertEqual(point, "M1")
        self.assertGreaterEqual(score, 90)
        self.assertEqual(level, "A")


if __name__ == "__main__":
    unittest.main()
