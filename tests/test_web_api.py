import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("dcar_web_api", ROOT / "app/api/server.py")
WEB_API = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(WEB_API)


class WebApiTest(unittest.TestCase):
    def test_douyin_input_validation_accepts_uid_and_link(self):
        result = WEB_API.validate_inputs(
            "douyin",
            "1619994549436234\nhttps://www.douyin.com/video/123\nbad",
        )
        self.assertEqual(result["total"], 3)
        self.assertEqual(result["valid_count"], 2)
        self.assertEqual(result["invalid_count"], 1)
        self.assertFalse(result["can_start"])

    def test_xiaohongshu_validation_rejects_douyin(self):
        result = WEB_API.validate_inputs(
            "xiaohongshu", "https://www.douyin.com/video/123"
        )
        self.assertEqual(result["valid_count"], 0)
        self.assertEqual(result["invalid_count"], 1)

    def test_overview_uses_formal_v7_dual_channel_report(self):
        result = WEB_API.overview()
        self.assertEqual(result["report_version"], "channel-structured-conclusions-v7.0")
        self.assertEqual(result["rule_version"], "dcar-evaluation-v5.0")
        self.assertEqual(result["channels"]["douyin"]["denominator"], 438)
        self.assertEqual(result["channels"]["xiaohongshu"]["denominator"], 338)
        self.assertFalse(result["workflow"]["provider_refresh_enabled"])
        self.assertGreaterEqual(result["revision"], 1)

    def test_preflight_is_read_only_and_reports_no_provider_calls(self):
        with WEB_API.connect() as connection:
            value = WEB_API.preflight(connection)
        self.assertEqual(value["provider_calls"], 0)
        self.assertEqual(value["channels"]["douyin"]["content_items"], 438)
        self.assertEqual(value["channels"]["xiaohongshu"]["content_items"], 338)

    def test_export_allowlist_contains_no_raw_comment_evidence(self):
        self.assertNotIn("comment-users", WEB_API.EXPORTS)
        self.assertIn("report-json", WEB_API.EXPORTS)


if __name__ == "__main__":
    unittest.main()
