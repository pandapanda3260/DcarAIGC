from __future__ import annotations

import tempfile
import unittest
import zipfile
from collections import Counter
from pathlib import Path

from dcar_eval.v8.selling_point_evidence import (
    build_evidence_package,
    load_evidence_config,
)
from dcar_eval.v8.selling_point_label_cards import load_label_cards
from dcar_eval.v8.selling_point_offline import (
    CharNgramTfidfIndex,
    GoldRow,
    SellingPointOfflineError,
    apply_gold_overrides,
    build_prompt,
    hard_priority,
    prompt_quote_options,
    read_xlsx_sheet,
    second_call_reason,
    validate_model_response,
)


class SellingPointOfflineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = load_evidence_config()
        cls.labels = load_label_cards()

    def _row(
        self,
        row_number: int,
        content_id: int,
        code: str,
        text: str,
        *,
        retrieval_code: str | None = None,
        url: str | None = None,
        secret: str = "",
    ) -> dict[str, object]:
        package = build_evidence_package(
            title=text,
            evidence_level="V3",
            evidence_sha256=f"{row_number:064x}"[-64:],
            config=self.config,
        )
        return {
            "excel_row": row_number,
            "content_id": content_id,
            "canonical_url": url or f"https://example.test/video/{content_id}",
            "gold_code": code,
            "retrieval_code": retrieval_code or code,
            "implant_position": secret or f"植入证据{row_number}",
            "video_summary": f"视频摘要{text}",
            "evidence_level": "V3",
            "original_channels": {
                "title": text,
                "body": "",
                "asr": "",
                "ocr": "",
            },
            "evidence_package": package,
        }

    def test_xlsx_reader_handles_shared_strings_and_numeric_cells(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.xlsx"
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr(
                    "xl/workbook.xml",
                    '<?xml version="1.0" encoding="UTF-8"?>'
                    '<workbook xmlns="http://schemas.openxmlformats.org/'
                    'spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/'
                    'officeDocument/2006/relationships"><sheets><sheet name="准确度训练样本v2" '
                    'sheetId="1" r:id="rId1"/></sheets></workbook>',
                )
                archive.writestr(
                    "xl/_rels/workbook.xml.rels",
                    '<?xml version="1.0" encoding="UTF-8"?>'
                    '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/'
                    'relationships"><Relationship Id="rId1" Target="worksheets/sheet1.xml" '
                    'Type="http://schemas.openxmlformats.org/officeDocument/2006/'
                    'relationships/worksheet"/></Relationships>',
                )
                archive.writestr(
                    "xl/sharedStrings.xml",
                    '<?xml version="1.0" encoding="UTF-8"?>'
                    '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                    '<si><t>序号</t></si><si><t>链接</t></si><si><t>url</t></si></sst>',
                )
                archive.writestr(
                    "xl/worksheets/sheet1.xml",
                    '<?xml version="1.0" encoding="UTF-8"?>'
                    '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                    '<sheetData><row r="1"><c r="A1" t="s"><v>0</v></c>'
                    '<c r="B1" t="s"><v>1</v></c></row><row r="2">'
                    '<c r="A2"><v>1</v></c><c r="B2" t="s"><v>2</v></c>'
                    '</row></sheetData></worksheet>',
                )
            self.assertEqual(
                read_xlsx_sheet(path, "准确度训练样本v2"),
                [["序号", "链接"], [1, "url"]],
            )

    def test_retrieval_groups_duplicates_and_never_leaks_target_content(self) -> None:
        rows = [
            self._row(45, 10, "X1", "懂车帝车型库3D看车", retrieval_code="X9"),
            self._row(168, 10, "X9", "懂车帝车型库3D看车", retrieval_code="X9"),
            self._row(100, 20, "X9", "懂车帝车型库外观实拍"),
            self._row(167, 20, "X9", "懂车帝车型库外观实拍"),
            self._row(2, 30, "X9", "懂车帝车型外观细节"),
            self._row(3, 40, "X9", "懂车帝车型真实影像"),
            self._row(4, 50, "X1", "懂车帝车型PK对比"),
        ]
        index = CharNgramTfidfIndex(rows)
        self.assertEqual(len(index.rows), 5)
        target = rows[0]
        selected = index.select(target)
        self.assertNotIn(10, {item["content_id"] for item in selected})
        self.assertLessEqual(len(selected), 4)
        self.assertLessEqual(
            sum(item["code"] == "X9" for item in selected),
            2,
        )
        representative = next(row for row in index.rows if row["content_id"] == 10)
        self.assertEqual(representative["retrieval_code"], "X9")

    def test_hard_priority_order_is_p0_then_p1_then_p2(self) -> None:
        all_codes = self.labels["cards"]
        p0 = self._row(1, 1, "M2", "AI小懂讲政府补贴")["evidence_package"]
        self.assertEqual(hard_priority(p0, all_codes)["priority"], "P0")
        p1 = self._row(2, 2, "X8", "打开懂车帝领取政府补贴")["evidence_package"]
        self.assertEqual(hard_priority(p1, all_codes)["forced_code"], "X8")
        hashtag_only = self._row(4, 4, "X6", "油电成本怎么算 #政府补贴")[
            "evidence_package"
        ]
        self.assertEqual(hard_priority(hashtag_only, all_codes)["priority"], "P4")
        ocr_only = build_evidence_package(
            title="懂车帝车型库3D看车",
            ocr_observations=["推荐页出现政府补贴入口"],
            evidence_level="V3",
            evidence_sha256="f" * 64,
            config=self.config,
        )
        self.assertEqual(hard_priority(ocr_only, all_codes)["priority"], "P4")
        p2 = self._row(
            3,
            3,
            "E2",
            "懂车帝二手车透明车况，价格有保障",
        )["evidence_package"]
        p2_priority = hard_priority(p2, all_codes)
        self.assertEqual(p2_priority["priority"], "P2")
        self.assertIsNone(p2_priority["forced_code"])
        self.assertEqual(set(p2_priority["allowed_codes"]), set(all_codes))

    def test_gold_overlay_applies_exact_18_changes_and_freezes_blind_spots(self) -> None:
        original_counts = Counter(
            {
                "E1": 10,
                "E10": 9,
                "E2": 16,
                "E3": 10,
                "E4": 3,
                "E5": 11,
                "E6": 9,
                "E7": 9,
                "E8": 10,
                "E9": 8,
                "M2": 10,
                "M4": 7,
                "M5": 1,
                "M6": 1,
                "X1": 11,
                "X10": 1,
                "X11": 9,
                "X2": 12,
                "X3": 16,
                "X4": 10,
                "X5": 10,
                "X6": 10,
                "X7": 12,
                "X8": 10,
                "X9": 13,
            }
        )
        fixed = {
            45: "X1",
            58: "X5",
            67: "X5",
            **{row: "X11" for row in range(120, 129)},
            150: "E3",
            153: "E3",
            198: "E7",
            227: "E10",
            228: "E10",
            229: "E10",
        }
        for code in fixed.values():
            original_counts[code] -= 1
        pool = [code for code, count in sorted(original_counts.items()) for _ in range(count)]
        rows: list[GoldRow] = []
        for row_number in range(1, 230):
            if row_number == 129:
                rows.append(
                    GoldRow(row_number, None, None, "url-129", "", "", "剔除", "")
                )
                continue
            code = fixed[row_number] if row_number in fixed else pool.pop()
            rows.append(
                GoldRow(
                    row_number,
                    code,
                    self.labels["cards"][code]["label"],
                    f"url-{row_number}",
                    "",
                    "",
                    "原始",
                    "",
                )
            )
        self.assertFalse(pool)
        derived, metadata = apply_gold_overrides(
            rows,
            cards=self.labels["cards"],
            source_gold_sha256="b34b5f7b550b948ec5f704620653e6e57662814d22d105f15631cc79588d8eec",
        )
        self.assertEqual(metadata["change_count"], 18)
        self.assertEqual(metadata["scene_counts"], {"E": 93, "M": 23, "X": 112})
        self.assertEqual(metadata["zero_sample_codes"], ["M1", "M3", "X11"])
        self.assertEqual(
            metadata["low_sample_codes"],
            {"M5": 1, "M6": 1, "M8": 2, "X10": 1},
        )
        by_row = {row.row_number: row for row in derived}
        self.assertEqual(by_row[45].gold_code, "X9")
        self.assertEqual(by_row[58].gold_code, "M2")
        self.assertEqual(by_row[150].gold_code, "X8")
        self.assertEqual(by_row[198].gold_code, "E2")
        self.assertEqual(by_row[229].gold_code, "E6")

    def test_prompt_excludes_target_gold_notes_and_uses_leave_out_examples(self) -> None:
        target = self._row(1, 1, "X9", "懂车帝车型库外观", secret="TARGET_SECRET")
        rows = [
            target,
            self._row(2, 2, "X9", "懂车帝车型库实拍"),
            self._row(3, 3, "X1", "懂车帝车型PK"),
        ]
        index = CharNgramTfidfIndex(rows)
        prompt = build_prompt(target, index=index, label_cards=self.labels)
        self.assertNotIn("TARGET_SECRET", prompt["system"])
        self.assertNotIn("TARGET_SECRET", prompt["user"])
        self.assertNotIn(1, {item["content_id"] for item in prompt["examples"]})
        self.assertTrue(prompt["prompt_version"].startswith("selling-point-flat-28-v4-"))
        example_codes = [item["code"] for item in prompt["examples"]]
        self.assertEqual(len(example_codes), len(set(example_codes)))

    def test_quote_maps_normalized_alias_back_to_original(self) -> None:
        target = self._row(1, 1, "X9", "点击懂车地链接查看车型外观")
        priority = hard_priority(target["evidence_package"], self.labels["cards"])
        decision = validate_model_response(
            {
                "primary_code": "X9",
                "confidence": 0.91,
                "top3": [
                    {"code": "X9", "confidence": 0.91},
                    {"code": "X1", "confidence": 0.70},
                ],
                "channel": "title",
                "anchor_quote": "懂车帝",
                "reason": "车型外观与看车任务明确",
            },
            target=target,
            priority=priority,
            valid_codes=self.labels["cards"],
        )
        self.assertEqual(decision["status"], "accepted")
        self.assertEqual(decision["original_quote"], "懂车地")
        self.assertIsNone(second_call_reason(decision))

    def test_prompt_quote_options_are_short_exact_and_source_mappable(self) -> None:
        row = self._row(
            1,
            1,
            "X9",
            "打开董车帝车型库，连续查看车辆外观和内饰细节",
        )
        options = prompt_quote_options(row)
        self.assertTrue(options)
        self.assertTrue(all(6 <= len(item["quote"]) <= 48 for item in options))
        prompt = build_prompt(row, index=CharNgramTfidfIndex([row]))
        self.assertIn("quote_options", prompt["user"])

    def test_invalid_quote_degrades_but_closed_set_violation_fails(self) -> None:
        target = self._row(1, 1, "X9", "懂车帝车型库外观")
        priority = hard_priority(target["evidence_package"], self.labels["cards"])
        parsed = {
            "primary_code": "X9",
            "confidence": 0.8,
            "top3": [{"code": "X9", "confidence": 0.8}],
            "channel": "title",
            "anchor_quote": "不存在证据",
            "reason": "模型给出了无法回指的证据",
        }
        decision = validate_model_response(
            parsed,
            target=target,
            priority=priority,
            valid_codes=self.labels["cards"],
        )
        self.assertEqual(decision["status"], "degraded_quote")
        self.assertEqual(second_call_reason(decision), "structure_or_quote_repair")
        invalid = dict(parsed)
        invalid["primary_code"] = "BAD"
        with self.assertRaises(SellingPointOfflineError):
            validate_model_response(
                invalid,
                target=target,
                priority=priority,
                valid_codes=self.labels["cards"],
            )

    def test_second_call_is_reserved_for_close_top2(self) -> None:
        self.assertEqual(
            second_call_reason(
                {
                    "status": "accepted",
                    "top3": [
                        {"code": "X1", "confidence": 0.80},
                        {"code": "X5", "confidence": 0.70},
                    ],
                }
            ),
            "top2_boundary_second_pass",
        )


if __name__ == "__main__":
    unittest.main()
