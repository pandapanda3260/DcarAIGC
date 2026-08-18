from __future__ import annotations

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
from v8.reports import render_summary_png


def _png_header(width: int, height: int) -> bytes:
    return b"\x89PNG\r\n\x1a\n" + struct.pack(">I4sII", 13, b"IHDR", width, height) + (b"x" * 1100)


class ReportExportTest(unittest.TestCase):
    def test_workbook_preserves_revision_rows_as_typed_formula_safe_cells(self) -> None:
        content_csv = (
            "\ufeffcontent_id,link_id,platform,published_at,canonical_url,title,account_uid,view_count,evaluation_current\r\n"
            '7,ABC123,douyin,2026-08-03T02:00:00Z,https://example.test/7,"=HYPERLINK(""https://evil.test"")",3190104393795291,61853,True\r\n'
        ).encode("utf-8")
        channel_csv = (
            "\ufeffplatform,platform_label,scope,scope_label,publication_count,metric,metric_label,percentage,coverage_percentage,reason\r\n"
            "douyin,抖音,summary,汇总,1,content_verticality,内容垂直度,42.47,100,测试口径\r\n"
        ).encode("utf-8")
        workbook = build_report_detail_workbook(
            task={
                "id": "D8-C-TEST",
                "name": "测试报告",
                "period_start": "2026-08-03",
                "period_end": "2026-08-03",
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

        # Untrusted titles are inline strings, never workbook formulas.
        self.assertIn(b"=HYPERLINK", content_sheet)
        self.assertNotIn(b"<f>", content_sheet)
        self.assertIn(
            b'<c r="G2" s="12" t="inlineStr"><is><t xml:space="preserve">3190104393795291</t>',
            content_sheet,
        )
        self.assertIn(b'numFmtId="49"', styles)
        self.assertIn(b'quotePrefix="1"', styles)
        self.assertIn(b't="b"><v>1</v>', content_sheet)
        self.assertIn(b"46237.0833333333", content_sheet)
        self.assertNotIn(b"2026-08-03T02:00:00Z", content_sheet)
        self.assertIn(b"0.4247", channel_sheet)
        self.assertIn(b"1</v>", channel_sheet)

    def test_bundle_contains_exactly_one_image_and_one_native_workbook(self) -> None:
        workbook = build_report_detail_workbook(
            task={
                "id": "D8-C-TEST",
                "name": "测试报告",
                "period_start": "2026-08-03",
                "period_end": "2026-08-03",
                "task_status": "succeeded",
            },
            revision=1,
            content_csv=b"content_id,title\r\n1,test\r\n",
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
