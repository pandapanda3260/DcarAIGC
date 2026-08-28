from __future__ import annotations

import unittest

from dcar_eval.v8.selling_point_label_cards import cards_for_prompt, load_label_cards


class SellingPointLabelCardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.value = load_label_cards()
        cls.cards = cls.value["cards"]

    def test_complete_28_code_contract(self) -> None:
        self.assertEqual(len(self.cards), 28)
        self.assertEqual(
            set(self.cards),
            {
                *(f"E{index}" for index in range(1, 11)),
                *(f"X{index}" for index in range(1, 12)),
                *(f"M{index}" for index in range(1, 7)),
                "M8",
            },
        )
        for card in self.cards.values():
            self.assertTrue(card["definition"])
            self.assertTrue(card["positive_evidence"])
            self.assertTrue(card["negative_evidence"])
            self.assertTrue(card["boundary_rules"])

    def test_user_approved_labels_are_exact(self) -> None:
        self.assertEqual(
            self.cards["E2"]["label"],
            "通过懂车帝购买海量靠谱二手车，查看透明车况和价格有保障",
        )
        self.assertEqual(
            self.cards["E6"]["label"],
            "通过懂车帝查询估价、差价、保值率和市场行情，减少价格信息差",
        )
        self.assertEqual(
            self.cards["X2"]["label"],
            "通过懂车帝查看权威榜单、第三方测评和真实车主口碑",
        )
        self.assertEqual(
            self.cards["X9"]["label"],
            "通过懂车帝查看新车车型细节、外观、真实影像和场景化体验",
        )
        self.assertEqual(
            self.cards["E4"]["label"],
            "通过懂车帝按用车场景获得二手车推荐方案",
        )

    def test_priority_rules_are_ordered_and_include_new_decisions(self) -> None:
        priorities = self.value["priority_rules"]
        self.assertEqual([item["id"] for item in priorities], ["P0", "P1", "P2", "P3", "P4"])
        self.assertIn("AI小懂", priorities[0]["rule"])
        self.assertIn("政府补贴", priorities[1]["rule"])
        self.assertIn("纯话题标签", priorities[1]["rule"])
        self.assertIn("履约保障", priorities[2]["rule"])
        self.assertIn("不得收窄候选", priorities[2]["rule"])

    def test_prompt_projection_is_deterministic(self) -> None:
        first = cards_for_prompt(self.value)
        second = cards_for_prompt(self.value)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 28)
        self.assertEqual([item["code"] for item in first], sorted(self.cards))


if __name__ == "__main__":
    unittest.main()
