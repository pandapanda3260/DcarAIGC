from __future__ import annotations

from copy import deepcopy
import unittest

from v8.insights import build_channel_conclusions
from v8.overview_selling_points import build_overview_selling_points


def row(code="X3", *, views=100, status="provided", freshness="fresh", **extra):
    return {
        "platform": "douyin", "content_direction": "new_car", "evidence_level": "V3",
        "selling_point_included": True, "primary_selling_point_code": code,
        "primary_label": "新车价格与优惠", "primary_tier": "core",
        "view_count": views, "view_count_status": status, "view_count_freshness": freshness,
        **extra,
    }


def details(rows, *, threshold=90):
    return build_overview_selling_points(
        rows, build_channel_conclusions(rows), minimum_view_coverage=threshold
    )


class OverviewSellingPointsTest(unittest.TestCase):
    def test_primary_included_counts_reconcile_and_use_channel_denominators(self):
        rows = [
            row(views=100), row(views=200),
            row("E8", views=100, primary_label="二手车实用知识", primary_tier="other", content_direction="used_car"),
            row("X1", views=400, selling_point_included=False),
        ]
        items = {item["code"]: item for item in details(rows)["douyin"]}
        self.assertEqual(set(items), {"X3", "E8"})
        self.assertEqual(sum(item["publication_count"] for item in items.values()), 3)
        self.assertEqual(sum(item["view_count"]["value"] for item in items.values()), 400)
        self.assertEqual(items["X3"]["label"], "新车价格与优惠")
        self.assertEqual(items["X3"]["tier"], "core")
        self.assertEqual(items["X3"]["count_share"]["percentage"], 50)
        self.assertEqual(items["X3"]["count_share"]["denominator"], 4)
        self.assertEqual(items["X3"]["exposure_share"]["percentage"], 37.5)
        self.assertEqual(items["X3"]["exposure_share"]["denominator"], 800)

    def test_real_zero_is_provided_and_different_from_missing(self):
        rows = [row("ZERO", views=0), row("MISSING", views=None, status="missing"), row("OTHER", views=100)]
        items = {item["code"]: item for item in details(rows)["douyin"]}
        self.assertEqual(items["ZERO"]["view_count"]["value"], 0)
        self.assertEqual(items["ZERO"]["view_count"]["status"], "available")
        self.assertEqual(items["ZERO"]["provided_view_items"], 1)
        self.assertEqual(items["ZERO"]["exposure_share"]["percentage"], 0)
        self.assertIsNone(items["MISSING"]["view_count"]["value"])
        self.assertEqual(items["MISSING"]["view_count"]["status"], "missing")
        self.assertEqual(items["MISSING"]["missing_view_items"], 1)
        self.assertIsNone(items["MISSING"]["exposure_share"]["percentage"])

    def test_partial_views_are_withheld_until_existing_coverage_gate(self):
        rows = [row() for _ in range(9)] + [row(views=None, status="missing")]
        at_gate = details(rows)["douyin"][0]
        self.assertEqual(at_gate["view_count"]["status"], "available")
        self.assertEqual(at_gate["view_count"]["coverage_percentage"], 90)
        self.assertEqual(at_gate["provided_view_items"], 9)
        below = details(rows + [row(views=None, status="missing")])["douyin"][0]
        self.assertEqual(below["view_count"]["status"], "below_threshold")
        self.assertEqual(below["view_count"]["value"], 900)
        self.assertEqual(below["missing_view_items"], 2)
        self.assertIsNone(below["exposure_share"]["percentage"])

    def test_rounded_coverage_cannot_pass_the_gate(self):
        rows = [row()] * 18008 + [row(views=None, status="missing")] * 2002
        item = details(rows)["douyin"][0]
        self.assertEqual(item["view_count"]["coverage_percentage"], 90)
        self.assertEqual(item["view_count"]["status"], "below_threshold")
        self.assertEqual(item["exposure_share"]["status"], "below_threshold")
        self.assertIsNone(item["exposure_share"]["percentage"])
        self.assertIn("18008/20010", item["view_count"]["reason"])
        rows[-1] = row()
        at_gate = details(rows)["douyin"][0]
        self.assertEqual(at_gate["provided_view_items"], 18009)
        self.assertEqual(at_gate["view_count"]["status"], "available")

    def test_stale_or_unknown_values_are_never_current(self):
        for freshness in ("stale", "unknown"):
            with self.subTest(freshness=freshness):
                item = details([row(freshness=freshness)])["douyin"][0]
                self.assertEqual(item["view_count"]["value"], 100)
                self.assertEqual(item["view_count"]["status"], "stale")
                self.assertEqual(item["stale_view_items"], 1)
                self.assertEqual(item["exposure_share"]["status"], "stale")
                self.assertIsNone(item["exposure_share"]["percentage"])

    def test_fresh_point_cannot_hide_stale_channel_denominator(self):
        rows = [row(), row("OTHER", views=200, freshness="stale", selling_point_included=False)]
        item = details(rows)["douyin"][0]
        self.assertEqual(item["view_count"]["status"], "available")
        self.assertEqual(item["stale_view_items"], 0)
        self.assertEqual(item["exposure_share"]["status"], "stale")
        self.assertIsNone(item["exposure_share"]["percentage"])

    def test_channel_classification_gate_is_inherited(self):
        rows = [row(), row("UNKNOWN", views=100, evidence_level=None, selling_point_included=False)]
        item = details(rows)["douyin"][0]
        self.assertEqual(item["view_count"]["status"], "available")
        self.assertEqual(item["exposure_share"]["status"], "below_threshold")
        self.assertEqual(item["exposure_share"]["coverage_percentage"], 50)
        self.assertIsNone(item["exposure_share"]["percentage"])
        self.assertIsNone(item["exposure_share"]["numerator"])

    def test_xiaohongshu_exposure_is_not_applicable_even_with_raw_placeholder(self):
        item = details([row(platform="xiaohongshu", views=0)])["xiaohongshu"][0]
        for key in ("view_count", "exposure_share"):
            self.assertEqual(item[key]["status"], "not_applicable")
        self.assertIsNone(item["view_count"]["value"])
        self.assertIsNone(item["view_count"]["coverage_percentage"])
        self.assertIsNone(item["exposure_share"]["percentage"])
        self.assertEqual(item["count_share"]["percentage"], 100)
        self.assertEqual(item["provided_view_items"], 0)
        self.assertEqual(item["missing_view_items"], 0)

    def test_invalid_field_is_not_fabricated(self):
        rows = [row("BAD", views=0, status="invalid")]
        items = details(rows)["douyin"]
        self.assertEqual([item["code"] for item in items], ["BAD"])
        self.assertIsNone(items[0]["view_count"]["value"])
        self.assertEqual(items[0]["view_count"]["status"], "missing")

    def test_missing_codes_preserve_counts_views_and_each_tier(self):
        rows = [
            row("X3", views=100), row(None, views=200),
            row("", views=300, primary_tier="other"),
            row(" ", views=None, status="missing", primary_tier="other"),
            row(None, views=500, selling_point_included=False),
        ]
        channels = build_channel_conclusions(rows)
        items = details(rows)["douyin"]
        by_code = {item["code"]: item for item in items}
        core = by_code["__unclassified_core__"]
        other = by_code["__unclassified_other__"]
        self.assertEqual(core["label"], "卖点编码缺失（待核对）")
        self.assertTrue(core["code_missing"])
        self.assertTrue(other["code_missing"])
        self.assertNotIn("code_missing", by_code["X3"])
        self.assertEqual(core["tier"], "core")
        self.assertEqual(core["publication_count"], 1)
        self.assertEqual(core["view_count"]["value"], 200)
        self.assertEqual(other["label"], core["label"])
        self.assertEqual(other["tier"], "other")
        self.assertEqual(other["publication_count"], 2)
        self.assertEqual(other["missing_view_items"], 1)
        self.assertEqual(other["view_count"]["status"], "below_threshold")
        summary = channels["douyin"]["summary"]["metrics"]
        self.assertEqual(sum(item["publication_count"] for item in items), summary["selling_point_count_share"]["numerator"])
        self.assertEqual(sum(item["view_count"]["value"] or 0 for item in items), summary["selling_point_exposure_share"]["numerator"])
        self.assertEqual(sum(item["publication_count"] for item in items if item["tier"] == "core"), summary["core_selling_point_count_share"]["numerator"])

    def test_missing_code_bucket_does_not_collide_with_an_existing_code(self):
        rows = [row("__unclassified_core__", primary_label="历史异常编码"), row(None, views=200)]
        items = {item["code"]: item for item in details(rows)["douyin"]}
        self.assertEqual(len(items), 2)
        self.assertEqual(items["__unclassified_core__"]["label"], "历史异常编码")
        self.assertNotIn("code_missing", items["__unclassified_core__"])
        self.assertTrue(items["__unclassified_core___"]["code_missing"])
        self.assertEqual(items["__unclassified_core___"]["label"], "卖点编码缺失（待核对）")
        self.assertEqual(items["__unclassified_core___"]["view_count"]["value"], 200)

    def test_empty_result_and_shared_report_output_are_unchanged(self):
        rows = [row()]
        channels = build_channel_conclusions(rows)
        before = deepcopy(channels)
        build_overview_selling_points(rows, channels, minimum_view_coverage=90)
        self.assertEqual(channels, before)
        self.assertNotIn("selling_points", channels["douyin"])
        self.assertEqual(len(channels["douyin"]["summary"]["metrics"]), 7)
        self.assertEqual(details([]), {"douyin": [], "xiaohongshu": []})


if __name__ == "__main__":
    unittest.main()
