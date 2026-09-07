from __future__ import annotations

import io
import json
import unittest
import zipfile
from unittest.mock import patch
from xml.sax.saxutils import escape

from v8.account_roster import RosterError
from v8.account_roster_upload import decode_official_export


class OfficialRosterUploadTest(unittest.TestCase):
    def _matrix_xlsx(self, *, overrides: dict[str, str] | None = None, omit: str | None = None) -> bytes:
        # Mirrors the official 2026-08-28 export: 23 headers, inline strings,
        # blank numeric cells and no hyperlinks. Values below are synthetic.
        headers = [
            "账号", "账号唯一ID(矩阵通)", "头像", "团队-运营人", "团队", "运营人", "账号主页链接",
            "积分领取人", "账号唯一标识", "监测状态", "开启监测时间", "所属平台", "账号标签", "账号状态",
            "开放平台授权状态", "最后一次扫码授权时间（开放平台）", "账号授权失效时间（开放平台）",
            "创作者&发布授权状态", "最后一次扫码授权时间（创作者&发布）", "账号授权失效时间（创作者&发布）",
            "线索&直播授权状态", "最后一次扫码授权时间（线索&直播）", "账号授权失效时间（线索&直播）",
        ]
        rows: list[list[str | None]] = [
            ["抖音测试账号", "DY00000000001", "https://example.com/dy.jpg", "测试组-运营甲", "测试组", "运营甲",
             "https://www.douyin.com/user/MS4w.example", None, "00012345678", "监测中", "2026-08-27 21:28:24",
             "抖音", "", "正常", "未授权", None, None, "未授权", None, None, "未授权", None, None],
            ["小红书测试账号", "XHS00000000002", "https://example.com/xhs.jpg", "测试组-运营乙", "测试组", "运营乙",
             "https://www.xiaohongshu.com/user/profile/5c668b3e0000000012021605", None, "98765432100", "未监测", "",
             "小红书", "", "正常", "未授权", None, None, "未授权", None, None, "未授权", None, None],
        ]
        for key, replacement in (overrides or {}).items():
            rows[0][headers.index(key)] = replacement
        if omit is not None:
            index = headers.index(omit)
            headers.pop(index)
            for row in rows:
                row.pop(index)
        xml_rows = []
        grid: list[list[str | None]] = [list(headers), *rows]
        for row_index, row in enumerate(grid, 1):
            cells = []
            for column, value in enumerate(row, 1):
                address = f"{chr(64 + column)}{row_index}"
                if value is None:
                    cells.append(f'<c r="{address}" t="n"></c>')
                else:
                    cells.append(f'<c r="{address}" t="inlineStr"><is><t>{escape(value)}</t></is></c>')
            xml_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')
        result = io.BytesIO()
        with zipfile.ZipFile(result, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(
                "xl/worksheets/sheet1.xml",
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
                + "".join(xml_rows) + "</sheetData></worksheet>",
            )
        return result.getvalue()

    def _xlsx(self, *, numeric_uid: bool = False, formula: bool = False) -> bytes:
        grid = [
            ["平台", "矩阵账号ID", "主页链接", "平台UID", "昵称"],
            ["抖音", "matrix-1", "https://www.douyin.com/user/MS4w.one", "9876543212345678900", "汽车账号"],
        ]
        xml_rows = []
        for row_index, row in enumerate(grid, 1):
            cells = []
            for column, value in enumerate(row, 1):
                address = f"{chr(64 + column)}{row_index}"
                if numeric_uid and address == "D2":
                    cells.append(f'<c r="{address}"><v>{value}</v></c>')
                elif formula and address == "D2":
                    cells.append(f'<c r="{address}"><f>1+1</f><v>2</v></c>')
                else:
                    cells.append(f'<c r="{address}" t="inlineStr"><is><t>{escape(value)}</t></is></c>')
            xml_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')
        result = io.BytesIO()
        with zipfile.ZipFile(result, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(
                "xl/worksheets/sheet1.xml",
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
                + "".join(xml_rows) + "</sheetData></worksheet>",
            )
        return result.getvalue()

    def test_matrix_account_management_export_preserves_real_columns_and_text_ids(self) -> None:
        rows = decode_official_export(self._matrix_xlsx(), source_name="账号管理数据.xlsx")
        self.assertEqual([row["platform"] for row in rows], ["douyin", "xiaohongshu"])
        self.assertEqual([row["matrix_account_id"] for row in rows], ["DY00000000001", "XHS00000000002"])
        self.assertEqual(rows[0]["profile_ref"], "https://www.douyin.com/user/MS4w.example")
        self.assertEqual(rows[1]["profile_ref"], "https://www.xiaohongshu.com/user/profile/5c668b3e0000000012021605")
        self.assertEqual(rows[0]["nickname"], "抖音测试账号")
        self.assertEqual(rows[0]["metadata"]["display_account_id"], "00012345678")
        self.assertEqual(rows[1]["metadata"]["display_account_id"], "98765432100")
        self.assertEqual(rows[0]["metadata"]["avatar_url"], "https://example.com/dy.jpg")
        self.assertTrue(all(row["uid"] is None for row in rows))
        self.assertEqual([row["monitoring_status"] for row in rows], ["monitored", "not_monitored"])
        self.assertEqual(rows[0]["monitoring_started_at"], "2026-08-27T21:28:24+08:00")
        self.assertIsNone(rows[1]["monitoring_started_at"])
        self.assertTrue(all(row["authorization_status"] == "unauthorized" for row in rows))
        self.assertEqual(rows[0]["metadata"]["authorization_by_scope"], {
            "open_platform": "unauthorized", "creator_publish": "unauthorized", "leads_live": "unauthorized",
        })
        self.assertNotIn("operator", rows[0])
        self.assertNotIn("phone", rows[0])

    def test_matrix_export_does_not_infer_authorization_between_scopes(self) -> None:
        rows = decode_official_export(
            self._matrix_xlsx(overrides={"创作者&发布授权状态": "已授权"}), source_name="mixed.xlsx",
        )
        self.assertEqual(rows[0]["authorization_status"], "unknown")
        self.assertEqual(rows[0]["metadata"]["authorization_by_scope"]["creator_publish"], "authorized")
        self.assertEqual(rows[0]["metadata"]["authorization_by_scope"]["open_platform"], "unauthorized")
        row = decode_official_export(self._matrix_xlsx(omit="开放平台授权状态"), source_name="partial.xlsx")[0]
        self.assertEqual(row["authorization_status"], "unknown")
        with self.assertRaises(RosterError) as invalid:
            decode_official_export(self._matrix_xlsx(overrides={"线索&直播授权状态": "监测中"}), source_name="invalid.xlsx")
        self.assertEqual(invalid.exception.code, "invalid_account_state")

    def test_matrix_export_still_requires_stable_keys_and_valid_local_times(self) -> None:
        for key in ("账号唯一ID(矩阵通)", "账号主页链接"):
            with self.subTest(key=key), self.assertRaises(RosterError) as missing:
                decode_official_export(self._matrix_xlsx(overrides={key: ""}), source_name="empty.xlsx")
            self.assertEqual(missing.exception.code, "missing_stable_key")
            with self.subTest(omitted=key), self.assertRaises(RosterError) as omitted:
                decode_official_export(self._matrix_xlsx(omit=key), source_name="missing.xlsx")
            self.assertEqual(omitted.exception.code, "unrecognized_export")
        with self.assertRaises(RosterError) as invalid:
            decode_official_export(self._matrix_xlsx(overrides={"开启监测时间": "2026-02-30 21:00:00"}), source_name="invalid.xlsx")
        self.assertEqual(invalid.exception.code, "invalid_source_time")

    def test_xlsx_text_ids_and_unknown_authorization_are_preserved(self) -> None:
        rows = decode_official_export(self._xlsx(), source_name="官方导出.xlsx")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["uid"], "9876543212345678900")
        self.assertEqual(rows[0]["matrix_account_id"], "matrix-1")
        self.assertEqual(rows[0]["monitoring_status"], "unknown")
        self.assertEqual(rows[0]["authorization_status"], "unknown")

    def test_xlsx_numeric_long_ids_and_formulas_fail_closed(self) -> None:
        for kwargs, code in (
            ({"numeric_uid": True}, "rounded_numeric_id"),
            ({"formula": True}, "formula_in_export"),
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(RosterError) as raised:
                    decode_official_export(self._xlsx(**kwargs), source_name="official.xlsx")
                self.assertEqual(raised.exception.code, code)

    def test_csv_has_explicit_identity_columns_and_separate_states(self) -> None:
        csv_bytes = (
            "平台,矩阵账号ID,主页链接,平台UID,矩阵监测,矩阵授权,短号\n"
            "小红书,matrix-x,https://www.xiaohongshu.com/user/profile/5c668b3e0000000012021605,,已监测,未知,short-x\n"
        ).encode("utf-8-sig")
        row = decode_official_export(csv_bytes, source_name="official.csv")[0]
        self.assertIsNone(row["uid"])
        self.assertEqual(row["metadata"]["display_account_id"], "short-x")
        self.assertEqual(row["monitoring_status"], "monitored")
        self.assertEqual(row["authorization_status"], "unknown")
        with self.assertRaises(RosterError) as missing:
            decode_official_export(b"phone,nickname\n13800138000,test\n", source_name="legacy.csv")
        self.assertEqual(missing.exception.code, "unrecognized_export")

    def test_json_does_not_coerce_numeric_ids_or_statistics_rows(self) -> None:
        member = {
            "platform": "douyin", "matrix_account_id": "official-1",
            "profile_ref": "https://www.douyin.com/user/MS4w.one", "uid": "9876543212345678900",
        }
        good = decode_official_export(json.dumps([member]).encode(), source_name="export.json")
        self.assertEqual(good[0]["uid"], member["uid"])
        for row in (
            {**member, "uid": int(member["uid"])},
            {"uid": member["uid"], "cNickname": "统计账号", "cTotalFans": 10},
        ):
            with self.subTest(row=row), self.assertRaises(RosterError):
                decode_official_export(json.dumps([row]).encode(), source_name="export.json")

    def test_duplicate_columns_and_unrecognized_states_are_rejected(self) -> None:
        for source in (
            "平台,平台,矩阵账号ID,主页链接\n抖音,抖音,id,https://www.douyin.com/user/a\n",
            "平台,矩阵账号ID,主页链接,矩阵授权\n抖音,id,https://www.douyin.com/user/a,监测中\n",
        ):
            with self.subTest(source=source), self.assertRaises(RosterError):
                decode_official_export(source.encode(), source_name="bad.csv")

    def test_bad_archives_and_expansion_limit_are_rejected(self) -> None:
        with self.assertRaises(RosterError):
            decode_official_export(b"not a zip", source_name="bad.xlsx")
        with patch("v8.account_roster_upload.MAX_UNCOMPRESSED_BYTES", 10):
            with self.assertRaises(RosterError) as raised:
                decode_official_export(self._xlsx(), source_name="large.xlsx")
        self.assertEqual(raised.exception.code, "export_too_large")


if __name__ == "__main__":
    unittest.main()
