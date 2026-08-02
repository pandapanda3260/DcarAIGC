from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from workflow.bootstrap_corpus import bootstrap
from workflow.evaluation import classify_xhs_evidence, evaluate_all
from workflow.storage import connect, migrate


ROOT = Path(__file__).resolve().parents[1]


class WorkflowEvaluationTest(unittest.TestCase):
    def test_video_cover_never_counts_as_complete_video_evidence(self):
        value = classify_xhs_evidence(
            {"note_type": "video", "desc": "汽车视频正文"},
            {
                "status": "complete",
                "video_expected": False,
                "video_path": "",
                "image_paths": ["cover.webp"],
            },
            {},
            {
                "status": "success",
                "source_kind": "all_original_images",
                "source_count": 1,
                "combined_text": "封面里有很多汽车相关文字",
            },
        )
        self.assertEqual(value, ("V1", "只有标题、正文或话题，完整媒体证据不足", False))

    def test_frozen_corpus_produces_one_v5_evaluation_per_content(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "evaluation.sqlite3"
            bootstrap(db, ROOT)
            with connect(db) as connection:
                migrate(connection)
                result = evaluate_all(connection)
                self.assertEqual(result["evaluated"], 776)
                self.assertEqual(result["channels"]["douyin"]["total"], 438)
                self.assertEqual(result["channels"]["xiaohongshu"]["total"], 338)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM evaluations").fetchone()[0], 776)
                columns = {
                    row["name"] for row in connection.execute("PRAGMA table_info(evaluations)")
                }
                self.assertNotIn("actual_acquisition_status", columns)
                bad = connection.execute(
                    """
                    SELECT COUNT(*) FROM evaluations
                    WHERE audience_automotive_score IS NULL AND acquisition_potential IS NOT NULL
                    """
                ).fetchone()[0]
                self.assertEqual(bad, 0)

    def test_douyin_full_media_and_existing_comment_gate_are_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "evaluation.sqlite3"
            bootstrap(db, ROOT)
            with connect(db) as connection:
                evaluate_all(connection)
                row = connection.execute(
                    """
                    SELECT COUNT(*) AS total,
                           SUM(evidence_level IN ('V2','V3')) AS identifiable,
                           SUM(audience_automotive_score IS NOT NULL) AS audience_scorable
                    FROM evaluations e JOIN content_items c ON c.id=e.content_item_id
                    WHERE c.platform='douyin'
                    """
                ).fetchone()
            self.assertEqual(row["total"], 438)
            self.assertEqual(row["identifiable"], 437)
            self.assertEqual(row["audience_scorable"], 67)


if __name__ == "__main__":
    unittest.main()
