from __future__ import annotations

import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from scripts import run_audience_classifier as runner
from v8.storage import connect, initialize_database, now_utc


class RunAudienceClassifierCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "audience.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
            captured_at = now_utc()
            for content_id, published_at in (
                (1, "2024-01-01T00:00:00Z"),
                (2, "2026-08-07T03:00:00Z"),
                (3, "2026-08-07T05:00:00Z"),
                (4, None),
            ):
                connection.execute(
                    """
                    INSERT INTO content_items(
                        id,link_id,platform,canonical_url,published_at,
                        imported_at,created_at,updated_at
                    ) VALUES (?,?,'douyin',?,?,?,?,?)
                    """,
                    (
                        content_id,
                        f"T{content_id:05d}",
                        f"https://www.douyin.com/video/{content_id}",
                        published_at,
                        captured_at,
                        captured_at,
                        captured_at,
                    ),
                )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_all_contents_uses_fixed_snapshot_and_as_of_context(self) -> None:
        calls: list[dict] = []

        def fake_classify(_connection, **kwargs):
            calls.append(kwargs)
            return {"total_users": 0, "label_counts": {}}

        output = StringIO()
        with patch.object(runner, "classify_window", side_effect=fake_classify), \
                redirect_stdout(output):
            exit_code = runner.main(
                [
                    "--db",
                    str(self.db),
                    "--all-contents",
                    "--snapshot-end",
                    "2026-08-07T12:00:00+08:00",
                ]
            )

        self.assertEqual(exit_code, 0)
        result = json.loads(output.getvalue())
        self.assertEqual(result["content_scope"], "all_contents_as_of_snapshot")
        self.assertEqual(result["snapshot_end"], "2026-08-07T04:00:00Z")
        self.assertEqual(result["content_count"], 3)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[-1]["content_ids"], [1, 2, 4])
        for call in calls:
            self.assertEqual(
                call["report_cutoff_at"], call["evidence_window_end"]
            )


if __name__ == "__main__":
    unittest.main()
