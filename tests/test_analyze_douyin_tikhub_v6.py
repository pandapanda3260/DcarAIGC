#!/usr/bin/env python3

import unittest

import analyze_douyin_tikhub_v6 as v6


class AnalyzeDouyinTikHubV6Test(unittest.TestCase):
    def test_audience_buckets(self) -> None:
        self.assertEqual(v6.audience_user_score("广州昨天刚提，锋兰达双擎领先版6.88万落地"), 100)
        self.assertEqual(v6.audience_user_score("日系车方向没错，就是底盘薄"), 70)
        self.assertEqual(v6.audience_user_score("这车真好看"), 30)
        self.assertEqual(v6.audience_user_score("这段感情看哭了"), 0)
        self.assertEqual(v6.audience_user_score("别克君威6年12万公里，只补过一次胎"), 100)
        self.assertEqual(v6.audience_user_score("直接订一辆"), 100)
        self.assertEqual(v6.audience_user_score("终于说实话了", context_automotive=True), 0)
        self.assertEqual(v6.audience_user_score("长度是多少", context_automotive=True), 70)

    def test_action_buckets(self) -> None:
        self.assertEqual(v6.action_user_score("我去懂车帝查一下价格"), 100)
        self.assertEqual(v6.action_user_score("在哪里提车，帮我买一辆"), 80)
        self.assertEqual(v6.action_user_score("这车哪个配置更好"), 50)
        self.assertEqual(v6.action_user_score("真不错"), 0)

    def test_comment_adjustment_is_capped(self) -> None:
        self.assertEqual(v6.recalibrate_content(90, 0), (85, -5.0))
        self.assertEqual(v6.recalibrate_content(10, 100), (15, 5.0))
        self.assertEqual(v6.recalibrate_content(None, 50), (None, 0.0))


if __name__ == "__main__":
    unittest.main()
