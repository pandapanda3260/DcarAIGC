from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from workflow.bootstrap_corpus import bootstrap
from workflow.contracts import validate_report
from workflow.evaluation import evaluate_all
from workflow.reporting import build_report_revision, create_report_run
from workflow.storage import connect


ROOT = Path(__file__).resolve().parents[1]


class WorkflowReportingTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.db = self.root / "report.sqlite3"
        self.reports = self.root / "reports"
        bootstrap(self.db, ROOT)
        with connect(self.db) as connection:
            evaluate_all(connection)
        self.run_id = create_report_run(self.db, "report-test")

    def tearDown(self):
        self.temporary.cleanup()

    def test_v7_report_contains_full_details_and_explicit_exposure_boundary(self):
        report = build_report_revision(self.db, self.run_id, self.reports)
        validate_report(report)
        self.assertEqual(len(report["channels"]["douyin"]["content_details"]), 438)
        self.assertEqual(len(report["channels"]["xiaohongshu"]["content_details"]), 338)
        self.assertTrue(report["channels"]["douyin"]["exposure_distribution"]["coverage"]["calculable"])
        self.assertFalse(report["channels"]["xiaohongshu"]["exposure_distribution"]["coverage"]["calculable"])
        self.assertIsNone(report["channels"]["xiaohongshu"]["exposure_distribution"]["core_selling_point"]["numerator"])
        self.assertIn(
            "汽车内容",
            report["channels"]["douyin"]["verticality"]["content_automotive"]["qualitative"],
        )
        serialized = json.dumps(report, ensure_ascii=False)
        self.assertNotIn("actual_acquisition", serialized)
        output = self.reports / self.run_id / "revision_001"
        self.assertTrue((output / "report.json").exists())
        self.assertTrue((output / "report.md").exists())
        self.assertTrue((output / "core_summary.svg").exists())
        self.assertTrue((output / "core_summary.png").exists())
        markdown = (output / "report.md").read_text(encoding="utf-8")
        self.assertLess(markdown.index("## 抖音渠道"), markdown.index("## 小红书渠道"))
        self.assertLess(markdown.index("## 小红书渠道"), markdown.index("## 结论摘要"))
        self.assertLess(markdown.index("## 结论摘要"), markdown.index("## 抖音内容明细"))

if __name__ == "__main__":
    unittest.main()
