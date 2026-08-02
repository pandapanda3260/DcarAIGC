from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from workflow.bootstrap_corpus import bootstrap
from workflow.contracts import validate_report
from workflow.evaluation import evaluate_all
from workflow.reporting import build_report_revision, create_report_run, submit_manual_review
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

    def test_manual_review_recalculates_and_creates_a_new_revision(self):
        build_report_revision(self.db, self.run_id, self.reports)
        with connect(self.db) as connection:
            row = connection.execute(
                """
                SELECT r.content_item_id, r.evaluation_json
                FROM run_evaluations r JOIN content_items c ON c.id=r.content_item_id
                WHERE r.run_id=? AND c.platform='douyin'
                  AND json_extract(r.evaluation_json, '$.audience_automotive_score') IS NOT NULL
                LIMIT 1
                """,
                (self.run_id,),
            ).fetchone()
        evaluation = json.loads(row["evaluation_json"])
        reviewed = submit_manual_review(
            self.db,
            self.run_id,
            int(row["content_item_id"]),
            {"content_automotive_score": 81},
            "人工复核完整媒体后修正内容汽车性",
            reports_root=self.reports,
        )
        self.assertEqual(reviewed["metadata"]["revision"], 2)
        with connect(self.db) as connection:
            revisions = connection.execute(
                "SELECT revision, is_current FROM report_revisions WHERE run_id=? ORDER BY revision",
                (self.run_id,),
            ).fetchall()
            current = json.loads(connection.execute(
                "SELECT evaluation_json FROM run_evaluations WHERE run_id=? AND content_item_id=?",
                (self.run_id, int(row["content_item_id"])),
            ).fetchone()[0])
            run = connection.execute("SELECT report_stale, report_revision FROM runs WHERE id=?", (self.run_id,)).fetchone()
        self.assertEqual([(item["revision"], item["is_current"]) for item in revisions], [(1, 0), (2, 1)])
        self.assertEqual(current["content_automotive_score"], 81)
        self.assertNotEqual(current["acquisition_potential"], evaluation["acquisition_potential"])
        self.assertEqual((run["report_stale"], run["report_revision"]), (0, 2))


if __name__ == "__main__":
    unittest.main()
