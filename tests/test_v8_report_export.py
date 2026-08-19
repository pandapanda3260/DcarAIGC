from __future__ import annotations

import csv
import io
import struct
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from xml.etree import ElementTree

from v8.report_export import (
    build_report_detail_workbook,
    build_report_download_bundle,
    report_bundle_filename,
)
from v8.reports import render_summary_png, render_summary_svg


def _png_header(width: int, height: int) -> bytes:
    return b"\x89PNG\r\n\x1a\n" + struct.pack(">I4sII", 13, b"IHDR", width, height) + (b"x" * 1100)


_SHEET_NAMESPACE = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def _sheet_values(payload: bytes) -> list[list[str]]:
    root = ElementTree.fromstring(payload)
    values: list[list[str]] = []
    for row in root.findall(".//x:sheetData/x:row", _SHEET_NAMESPACE):
        row_values: list[str] = []
        for cell in row.findall("x:c", _SHEET_NAMESPACE):
            inline = cell.find("x:is", _SHEET_NAMESPACE)
            if inline is not None:
                row_values.append(
                    "".join(
                        value.text or ""
                        for value in inline.findall(".//x:t", _SHEET_NAMESPACE)
                    )
                )
                continue
            value = cell.find("x:v", _SHEET_NAMESPACE)
            row_values.append(value.text if value is not None and value.text else "")
        values.append(row_values)
    return values


class ReportExportTest(unittest.TestCase):
    def test_channel_insight_svg_uses_frozen_structure_and_partial_boundary(
        self,
    ) -> None:
        long_title = "超长 \x00& 标题 \x0b<验证> 渠道洞察报告" + "特别长" * 20
        report = {
            "report_version": "dcar-content-operations-report-v8.7",
            "metadata": {
                "revision": 7,
                "collection_cutoff_at": "2026-08-17T12:45:43Z",
            },
            "scope": {
                "period_start": "2026-08-03T00:00:00+08:00",
                "period_end": "2026-08-17T00:00:00+08:00",
                "timezone": "Asia/Shanghai",
            },
            "task": {
                "name": long_title,
                "task_status": "partial",
            },
            "summary_metrics": {
                "publication_count": {"value": 1234, "status": "available"},
                "active_account_count": {"value": 57, "status": "available"},
                "view_count": {"value": 987654321, "status": "below_threshold"},
                "verticality_rate": {
                    "percentage": 88.88,
                    "status": "below_threshold",
                },
                "estimated_new_users": {"status": "not_calculable"},
                "estimated_reactivated_users": {"status": "not_calculable"},
                "estimated_leads": {"status": "not_calculable"},
            },
            "platform_dimensions": [
                {"key": "douyin", "count": 740, "percentage": 60},
                {"key": "xiaohongshu", "count": 309, "percentage": 25},
                {"key": "kuaishou", "count": 185, "percentage": 15},
            ],
            "account_type_dimensions": [
                {"key": "mixed_edit", "count": 864, "percentage": 70},
                {"key": "original", "count": 247, "percentage": 20},
                {"key": "boutique_ip", "count": 111, "percentage": 9},
                {"key": "unknown", "count": 12, "percentage": 1},
            ],
            "content_direction_dimensions": [
                {"key": "unknown", "count": 629, "percentage": 51},
                {"key": "new_car", "count": 432, "percentage": 35},
                {"key": "media", "count": 86, "percentage": 7},
                {"key": "used_car", "count": 49, "percentage": 4},
                {"key": "other", "count": 38, "percentage": 3},
            ],
            "data_quality": {
                "discovery_coverage": 12.34,
                "metrics_freshness": 56.78,
                "evaluation_coverage": 67.89,
                "detail_coverage": 78.9,
                "media_terminal_coverage": 69.01,
                "duplicate_fingerprint_coverage": 50,
                "weekly_comment_coverage": 100,
                "duplicate_calibration_ready": True,
            },
            "data_quality_details": {
                "discovery_coverage": {
                    "status": "below_threshold",
                    "percentage": 12.34,
                },
                "metrics_freshness": {
                    "status": "below_threshold",
                    "percentage": 56.78,
                },
            },
        }

        svg = render_summary_svg(report)
        root = ElementTree.fromstring(svg)
        self.assertEqual(root.attrib["width"], "1200")
        self.assertEqual(root.attrib["height"], "675")
        self.assertEqual(root.attrib["viewBox"], "0 0 1200 675")
        text = "".join(
            "".join(element.itertext())
            for element in root.iter()
            if element.tag.endswith("text")
        )
        for expected in (
            "DCar Insight · 渠道与内容结构",
            "超长 & 标题 <验证>",
            "…",
            "部分完成",
            "已纳入内容中",
            "平台结构",
            "账号类型构成",
            "内容方向",
            "数据完整度",
            "1,234",
            "57",
            "抖音",
            "60.00%",
            "其他平台",
            "494 条",
            "混剪",
            "原创",
            "精品 IP",
            "待补齐",
            "新车",
            "其他",
            "12.34%",
            "56.78%",
            "67.89%",
            "78.90%",
            "69.01%",
            "50.00%",
            "6 项数据未达到要求",
            "拉新、拉活和线索暂时无法计算",
            "报告周期 14 天",
            "数据统计至 2026-08-17 20:45",
        ):
            self.assertIn(expected, text)
        self.assertNotIn("\x00", svg)
        self.assertNotIn("\x0b", svg)
        self.assertNotIn(long_title, text)
        for forbidden in (
            "partial",
            "mixed_edit",
            "boutique_ip",
            "unknown",
            "new_car",
            "987654321",
            "88.88%",
            "T00:00:00+08:00",
        ):
            self.assertNotIn(forbidden, text)

        legacy_report = {**report, "metadata": {"revision": 7}}
        legacy_text = "".join(ElementTree.fromstring(render_summary_svg(legacy_report)).itertext())
        self.assertIn("数据统计至 2026-08-17 00:00", legacy_text)

    def test_workbook_preserves_revision_rows_as_typed_formula_safe_cells(self) -> None:
        content_buffer = io.StringIO(newline="")
        content_writer = csv.DictWriter(
            content_buffer,
            fieldnames=[
                "duplicate_method",
                "content_id",
                "canonical_url",
                "platform_content_id",
                "link_id",
                "platform",
                "content_type",
                "published_at",
                "title",
                "account_uid",
                "account_name",
                "account_type",
                "content_direction",
                "evidence_level",
                "primary_selling_point_code",
                "primary_selling_point_label",
                "selling_point_score",
                "content_automotive_score",
                "view_count",
                "comment_count",
                "duplicate_original_link_id",
                "duplicate_confidence",
                "evaluation_current",
            ],
        )
        content_writer.writeheader()
        base = {
            "duplicate_method": "",
            "account_uid": "3190104393795291",
            "account_name": "+SUM(1,1)",
            "account_type": "boutique_ip",
            "content_direction": "new_car",
            "primary_selling_point_code": "",
            "primary_selling_point_label": "",
            "selling_point_score": "",
            "content_automotive_score": "",
            "view_count": "",
            "comment_count": "",
            "duplicate_original_link_id": "OLD123",
            "duplicate_confidence": "0.9",
            "evaluation_current": "True",
        }
        content_writer.writerow(
            {
                **base,
                "content_id": "7",
                "platform_content_id": "7668604214154726706",
                "link_id": "RC49YU",
                "platform": "douyin",
                "content_type": "video",
                "published_at": "2026-08-03T16:00:00Z",
                "canonical_url": "https://www.douyin.com/video/7668604214154726706",
                "title": '=HYPERLINK("https://evil.test")',
                "evidence_level": "V3",
                "primary_selling_point_code": "C1",
                "primary_selling_point_label": "汽车服务",
                "selling_point_score": "92",
                "content_automotive_score": "86",
                "view_count": "61853",
                "comment_count": "0",
            }
        )
        for index, (content_type, evidence_level) in enumerate(
            (("image", "V2"), ("video", "V2"), ("video", "V1"), ("video", "")),
            start=8,
        ):
            platform_content_id = f"6a337c3a000000000f014fc{index}"
            content_writer.writerow(
                {
                    **base,
                    "content_id": str(index),
                    "platform_content_id": platform_content_id,
                    "link_id": f"XH{index:04d}"[-6:],
                    "platform": "xiaohongshu",
                    "content_type": content_type,
                    "published_at": "2026-08-04T00:00:00+08:00",
                    "canonical_url": f"https://www.xiaohongshu.com/explore/{platform_content_id}",
                    "title": f"测试内容 {index}",
                    "evidence_level": evidence_level,
                }
            )
        content_csv = ("\ufeff" + content_buffer.getvalue()).encode("utf-8")
        channel_csv = (
            "\ufeffplatform,platform_label,scope,scope_label,publication_count,metric,metric_label,percentage,coverage_percentage,reason\r\n"
            "douyin,抖音,summary,汇总,1,content_verticality,内容垂直度,42.47,100,重复内容感知指纹尚未完成定标，重复率暂不可计算\r\n"
        ).encode("utf-8")
        workbook = build_report_detail_workbook(
            task={
                "id": "D8-C-TEST",
                "name": "=1+1",
                "period_start": "2026-08-03",
                "period_end": "2026-08-16",
                "task_status": "partial",
                "revision_created_at": "2026-08-04T00:00:00Z",
            },
            revision=2,
            content_csv=content_csv,
            channel_csv=channel_csv,
        )

        self.assertTrue(workbook.startswith(b"PK"))
        with zipfile.ZipFile(io.BytesIO(workbook)) as archive:
            self.assertIsNone(archive.testzip())
            workbook_xml = archive.read("xl/workbook.xml")
            self.assertIn("报告说明".encode(), workbook_xml)
            self.assertIn("内容明细".encode(), workbook_xml)
            self.assertIn("渠道结论".encode(), workbook_xml)
            content_sheet = archive.read("xl/worksheets/sheet2.xml")
            channel_sheet = archive.read("xl/worksheets/sheet3.xml")
            styles = archive.read("xl/styles.xml")
            for name in archive.namelist():
                if name.endswith(".xml"):
                    ElementTree.fromstring(archive.read(name))

        values = _sheet_values(content_sheet)
        self.assertEqual(
            values[0],
            [
                "周期",
                "报告任务",
                "平台作品编号",
                "系统内容编号",
                "平台",
                "内容类型",
                "发布时间（北京时间）",
                "内容链接",
                "标题",
                "平台账号编号",
                "账号名称",
                "账号类型",
                "内容方向",
                "资料完整度",
                "主要卖点编号",
                "卖点信息",
                "卖点评分",
                "内容垂直度",
                "播放/阅读数",
                "评论数",
            ],
        )
        self.assertEqual(len(values[0]), 20)
        self.assertEqual(values[1][0], "2026-08-03 至 2026-08-16")
        self.assertEqual(values[1][1], "D8-C-TEST")
        self.assertEqual(values[1][2], "7668604214154726706")
        self.assertEqual(values[1][2], values[1][7].rsplit("/", 1)[-1])
        self.assertEqual(values[1][3], "RC49YU")
        self.assertEqual(values[1][13], "资料完整（V3）")
        self.assertEqual(values[1][14:16], ["C1", "汽车服务"])
        self.assertEqual(values[2][14:16], ["卖点资料不足", "卖点资料不足"])
        self.assertEqual(
            {row[13] for row in values[1:]},
            {"资料完整（V3）", "资料较完整（V2，图文内容）", "资料较完整（V2）", "资料不足（V1）", "还没有评估"},
        )
        root = ElementTree.fromstring(content_sheet)
        cells = {
            cell.attrib["r"]: cell
            for cell in root.findall(".//x:sheetData/x:row/x:c", _SHEET_NAMESPACE)
        }
        auto_filter = root.find("x:autoFilter", _SHEET_NAMESPACE)
        self.assertIsNotNone(auto_filter)
        assert auto_filter is not None
        self.assertEqual(auto_filter.attrib["ref"], "A1:T6")
        self.assertEqual(cells["C2"].attrib.get("t"), "inlineStr")
        self.assertEqual(cells["C2"].attrib.get("s"), "12")
        self.assertEqual(cells["G2"].attrib.get("s"), "8")
        self.assertEqual(cells["G2"].find("x:v", _SHEET_NAMESPACE).text, "46238")
        for reference, expected in (("Q2", "92"), ("R2", "86"), ("S2", "61853"), ("T2", "0")):
            self.assertNotEqual(cells[reference].attrib.get("t"), "inlineStr")
            self.assertEqual(cells[reference].find("x:v", _SHEET_NAMESPACE).text, expected)

        # Untrusted task/content text stays inline and never becomes a formula.
        self.assertIn(b"=HYPERLINK", content_sheet)
        self.assertIn(b"+SUM(1,1)", content_sheet)
        self.assertNotIn(b"<f>", content_sheet)
        self.assertIn(
            b'<c r="J2" s="12" t="inlineStr"><is><t xml:space="preserve">3190104393795291</t>',
            content_sheet,
        )
        self.assertIn(b'numFmtId="49"', styles)
        self.assertIn(b'quotePrefix="1"', styles)

        channel_values = _sheet_values(channel_sheet)
        self.assertEqual(
            channel_values[1][9],
            "重复内容识别规则还没完成校验，暂时无法计算重复率",
        )
        channel_root = ElementTree.fromstring(channel_sheet)
        hidden_columns = {
            column.attrib["min"]
            for column in channel_root.findall("x:cols/x:col", _SHEET_NAMESPACE)
            if column.attrib.get("hidden") == "1"
        }
        self.assertEqual(hidden_columns, {"1", "3", "6"})
        self.assertNotIn(b"2026-08-03T16:00:00Z", content_sheet)
        self.assertIn(b"0.4247", channel_sheet)
        self.assertIn(b"1</v>", channel_sheet)

    def test_bundle_contains_exactly_one_image_and_one_native_workbook(self) -> None:
        content_csv = (
            "content_id,platform_content_id,link_id,platform,content_type,published_at,canonical_url,title,"
            "account_uid,account_name,account_type,content_direction,evidence_level,"
            "primary_selling_point_code,primary_selling_point_label,selling_point_score,"
            "content_automotive_score,view_count,comment_count\r\n"
            "1,7668604214154726706,ABC123,douyin,video,2026-08-03T02:00:00Z,"
            "https://www.douyin.com/video/7668604214154726706,test,,,,unknown,V3,,,,,,\r\n"
        ).encode()
        workbook = build_report_detail_workbook(
            task={
                "id": "D8-C-TEST",
                "name": "测试报告",
                "period_start": "2026-08-03",
                "period_end": "2026-08-03",
                "task_status": "succeeded",
            },
            revision=1,
            content_csv=content_csv,
            channel_csv=None,
        )
        bundle = build_report_download_bundle(
            image_extension="svg",
            image_bytes=b"<svg/>",
            workbook_bytes=workbook,
        )
        with zipfile.ZipFile(io.BytesIO(bundle)) as archive:
            self.assertEqual(
                archive.namelist(), ["01_图片报告.svg", "02_数据明细.xlsx"]
            )
            self.assertIsNone(archive.testzip())
            self.assertEqual(archive.read("01_图片报告.svg"), b"<svg/>")
            self.assertTrue(archive.read("02_数据明细.xlsx").startswith(b"PK"))
            self.assertTrue(
                all(
                    not name.startswith("/") and ".." not in Path(name).parts
                    for name in archive.namelist()
                )
            )
        self.assertEqual(
            report_bundle_filename(
                task_name="2026-08-03 至 2026-08-16 自定义报告",
                task_id="D8-C-TEST",
            ),
            "2026-08-03 至 2026-08-16 自定义报告.zip",
        )
        self.assertEqual(
            report_bundle_filename(
                task_name='  月报：A/B*测试?.zip  ',
                task_id="D8-C-TEST",
            ),
            "月报-A-B-测试-.zip",
        )
        self.assertEqual(
            report_bundle_filename(task_name="...", task_id="D8-C-TEST"),
            "D8-C-TEST.zip",
        )

    def test_summary_png_renderer_uses_aspect_ratio_preserving_renderer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            svg = root / "report.svg"
            png = root / "report.png"
            svg.write_text('<svg width="1200" height="675"/>', encoding="utf-8")

            def fake_run(command, **_kwargs):
                self.assertNotIn("qlmanage", command[0])
                png.write_bytes(_png_header(1200, 675))
                return SimpleNamespace(returncode=0)

            with (
                patch(
                    "v8.reports.shutil.which",
                    side_effect=lambda name: "/usr/bin/sips" if name == "sips" else None,
                ),
                patch("v8.reports.subprocess.run", side_effect=fake_run),
            ):
                self.assertTrue(render_summary_png(svg, png))
            self.assertTrue(png.is_file())

    def test_summary_png_renderer_rejects_square_thumbnail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            svg = root / "report.svg"
            png = root / "report.png"
            svg.write_text('<svg width="1200" height="675"/>', encoding="utf-8")

            def fake_run(_command, **_kwargs):
                png.write_bytes(_png_header(1600, 1600))
                return SimpleNamespace(returncode=0)

            with (
                patch(
                    "v8.reports.shutil.which",
                    side_effect=lambda name: "/usr/bin/sips" if name == "sips" else None,
                ),
                patch("v8.reports.subprocess.run", side_effect=fake_run),
            ):
                self.assertFalse(render_summary_png(svg, png))
            self.assertFalse(png.exists())


if __name__ == "__main__":
    unittest.main()
