from __future__ import annotations

import hashlib
import json
import unittest

from dcar_eval.v8.selling_point_evidence import (
    DEFAULT_CONFIG_PATH,
    PROJECT_ROOT,
    build_evidence_package,
    dedupe_ocr_observations,
    load_evidence_config,
    normalize_text,
)


class SellingPointEvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = load_evidence_config()

    def test_frozen_config_matches_current_matcher_source(self) -> None:
        self.assertEqual(len(self.config["base_ordered_replacements"]), 15)
        source = PROJECT_ROOT / self.config["source_matcher_bundle"]
        self.assertEqual(
            hashlib.sha256(source.read_bytes()).hexdigest(),
            self.config["source_matcher_bundle_sha256"],
        )
        replacements_payload = (
            json.dumps(
                self.config["base_ordered_replacements"],
                ensure_ascii=False,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        self.assertEqual(
            hashlib.sha256(replacements_payload).hexdigest(),
            self.config["source_ordered_replacements_sha256"],
        )
        self.assertEqual(DEFAULT_CONFIG_PATH.name, "selling_point_evidence_package_v2.json")

    def test_all_fifteen_base_replacements_are_unconditional(self) -> None:
        for source, target in self.config["base_ordered_replacements"]:
            with self.subTest(source=source):
                result = normalize_text(f"前{source}后", self.config)
                self.assertEqual(result.text, f"前{target}后")

    def test_dongchedi_stays_unconditional_for_real_cta_and_campaign(self) -> None:
        self.assertEqual(
            normalize_text("点击视频左下角懂车地链接", self.config).text,
            "点击视频左下角懂车帝链接",
        )
        self.assertEqual(
            normalize_text("懂车地616全民购车节", self.config).text,
            "懂车帝616全民购车节",
        )

    def test_only_risky_new_aliases_use_context_gates(self) -> None:
        self.assertEqual(
            normalize_text("打开懂车的APP查看", self.config).text,
            "打开懂车帝APP查看",
        )
        self.assertEqual(
            normalize_text("这是懂车的朋友", self.config).text,
            "这是懂车的朋友",
        )
        self.assertEqual(
            normalize_text("问AI助手小董怎么选车", self.config).text,
            "问AI助手AI小懂怎么选车",
        )
        self.assertEqual(normalize_text("小董今天休假", self.config).text, "小董今天休假")
        self.assertEqual(normalize_text("AI小董帮我选", self.config).text, "AI小懂帮我选")

    def test_span_map_returns_original_for_shorter_and_longer_aliases(self) -> None:
        shorter = normalize_text("总车第一", self.config)
        self.assertEqual(shorter.text, "懂车帝")
        self.assertEqual(shorter.original_quote(0, len(shorter.text)), "总车第一")

        longer = normalize_text("问AI助手小董选车", self.config)
        start = longer.text.index("AI小懂", len("问AI助手"))
        self.assertEqual(longer.original_quote(start, start + len("AI小懂")), "小董")

    def test_ocr_only_dedupes_adjacent_near_duplicates(self) -> None:
        mapped = dedupe_ocr_observations(
            ["懂车地链接", "懂车地链接。", "其他画面", "懂车地链接"],
            self.config,
        )
        self.assertEqual(mapped.text.count("懂车帝链接"), 2)
        self.assertIn("其他画面", mapped.text)

    def test_package_prioritizes_anchors_and_limits_keyframes(self) -> None:
        package = build_evidence_package(
            title="打开懂车地查看车型",
            body="政府补贴领取方式请在懂车帝搜索口令",
            asr="问AI小董怎么选车",
            ocr_observations=["懂车地链接", "懂车地链接。"],
            keyframes=[f"frame-{index}.jpg" for index in range(8)],
            evidence_level="V3",
            evidence_sha256="a" * 64,
            config=self.config,
        )
        self.assertEqual(package["version"], "evidence-package-v2")
        self.assertEqual(package["channels"]["title"], "打开懂车帝查看车型")
        self.assertEqual(
            len(package["span_maps"]["title"]),
            len(package["channels"]["title"]),
        )
        self.assertEqual(package["anchor_windows"][0]["priority"], 0)
        self.assertEqual(
            len(package["anchor_windows"][0]["span_map"]),
            len(package["anchor_windows"][0]["text"]),
        )
        self.assertIsNotNone(package["anchor_windows"][0]["original_start"])
        self.assertLessEqual(len(package["anchor_windows"]), 4)
        self.assertEqual(len(package["keyframes"]), 6)
        self.assertRegex(package["package_sha256"], r"^[0-9a-f]{64}$")

    def test_v0_is_minimal_and_deterministic(self) -> None:
        first = build_evidence_package(
            evidence_level="V0",
            evidence_sha256="b" * 64,
            config=self.config,
        )
        second = build_evidence_package(
            evidence_level="V0",
            evidence_sha256="b" * 64,
            config=self.config,
        )
        self.assertEqual(first, second)
        self.assertEqual(first["channels"], {})
        self.assertEqual(first["span_maps"], {})
        self.assertEqual(first["anchor_windows"], [])
        self.assertEqual(first["keyframes"], [])


if __name__ == "__main__":
    unittest.main()
