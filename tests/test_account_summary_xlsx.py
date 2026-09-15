from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile

SPEC = importlib.util.spec_from_file_location("summary_cli", Path(__file__).resolve().parents[1] / "scripts/import_account_summary.py")
CLI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CLI)
S = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
P = "http://schemas.openxmlformats.org/package/2006/relationships"


def workbook(path, *, numeric_identifier=False, formula=False):
    root = ET.Element("worksheet", xmlns=S)
    data = ET.SubElement(root, "sheetData")
    for index, values in [(1, CLI.HEADERS), (2, ["抖音", "运营甲", "账号", "00001234567890123456789", "0000123456789", 38000, None, None, None, None, "0013800000000", "开卡乙", "001234567890123456", "持卡丙", None, None])]:
        row = ET.SubElement(data, "row", r=str(index))
        for col, value in enumerate(values):
            if value is None:
                continue
            cell = ET.SubElement(row, "c", r=f"{chr(65+col)}{index}")
            if index == 2 and (col == 5 or numeric_identifier and col == 3):
                ET.SubElement(cell, "v").text = str(value)
                cell.set("s", "1")
            else:
                cell.set("t", "inlineStr")
                ET.SubElement(ET.SubElement(cell, "is"), "t").text = str(value)
            if formula and index == 2 and col == 3:
                ET.SubElement(cell, "f").text = "1+1"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("xl/workbook.xml", f'<workbook xmlns="{S}" xmlns:r="{R}"><sheets><sheet name="账号汇总" sheetId="1" r:id="rId1"/></sheets></workbook>')
        z.writestr("xl/_rels/workbook.xml.rels", f'<Relationships xmlns="{P}"><Relationship Id="rId1" Target="/xl/worksheets/sheet1.xml" Type="{R}/worksheet"/></Relationships>')
        z.writestr("xl/worksheets/sheet1.xml", ET.tostring(root))
        z.writestr("xl/styles.xml", f'<styleSheet xmlns="{S}"><numFmts><numFmt numFmtId="164" formatCode="&quot;约&quot;0"/></numFmts><cellXfs><xf numFmtId="0"/><xf numFmtId="164"/></cellXfs></styleSheet>')
        z.writestr("xl/worksheets/_rels/sheet1.xml.rels", f'<Relationships xmlns="{P}"><Relationship Id="rId2" Target="../comments/comment1.xml" Type="{R}/comments"/></Relationships>')
        z.writestr("xl/comments/comment1.xml", f'<comments xmlns="{S}"><commentList><comment ref="C2" authorId="0"><text><t>来源 Sheet1 第2行；待核实批注</t></text></comment></commentList></comments>')


class SummaryWorkbookReadTest(unittest.TestCase):
    def test_exact_identifiers_roles_approximation_and_comment(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "accounts.xlsx"
            workbook(path)
            payload = CLI.read_workbook(path)
        row = payload["records"][0]
        self.assertEqual(len(row["raw"]), 16)
        self.assertEqual(row["raw"][CLI.HEADERS[3]], "00001234567890123456789")
        self.assertEqual(row["raw"]["uid"], "0000123456789")
        self.assertEqual(row["raw"]["手机号"], "0013800000000")
        self.assertEqual(row["raw"]["使用人证件号码"], "001234567890123456")
        self.assertEqual(row["raw"]["粉丝"], "约38000")
        self.assertEqual(row["raw"]["运营人员"], "运营甲")
        self.assertEqual(row["raw"]["手机号开卡人姓名"], "开卡乙")
        self.assertEqual(row["raw"]["持卡人"], "持卡丙")
        self.assertIsNone(row["raw"]["是否实名"])
        self.assertIn("Sheet1 第2行", row["comment"])

    def test_numeric_identifier_and_formula_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "accounts.xlsx"
            workbook(path, numeric_identifier=True)
            with self.assertRaisesRegex(ValueError, "not stored as text"):
                CLI.read_workbook(path)
            workbook(path, formula=True)
            with self.assertRaisesRegex(ValueError, "Formula"):
                CLI.read_workbook(path)

    def test_evidence_must_match_actual_workbook_values_and_notes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "accounts.xlsx"
            workbook(path)
            row = CLI.read_workbook(path)["records"][0]
            meta = {"headers": CLI.HEADERS, "rows": [{"excel_row": 2, "values": list(row["raw"].values()), "comment": row["comment"], "verified_uid": "0000123456789"}]}
            meta_path = Path(tmp) / "metadata.json"
            meta_path.write_text(json.dumps(meta))
            self.assertEqual(CLI.read_workbook(path, meta_path)["records"][0]["metadata"]["verified_uid"], "0000123456789")
            meta["rows"][0]["values"][4] = "99999999999"
            meta_path.write_text(json.dumps(meta))
            with self.assertRaisesRegex(ValueError, "mismatch at row 2"):
                CLI.read_workbook(path, meta_path)


if __name__ == "__main__":
    unittest.main()
