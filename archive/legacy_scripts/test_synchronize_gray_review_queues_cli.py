from __future__ import annotations

import importlib.util
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "synchronize_gray_review_queues.py"
SPEC = importlib.util.spec_from_file_location("synchronize_gray_review_queues", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cli)


class SynchronizeGrayReviewQueuesCliTest(unittest.TestCase):
    def test_dry_run_and_confirmed_apply_use_the_explicit_database(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "evaluation.sqlite3"
            database.write_bytes(b"fixture")
            stdout = io.StringIO()
            with (
                patch.object(
                    cli,
                    "plan_gray_review_queue_sync",
                    return_value={"plan_sha256": "a" * 64, "target_count": 1},
                ) as planned,
                redirect_stdout(stdout),
            ):
                code = cli.main(["--db", str(database)])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(stdout.getvalue())["mode"], "dry-run")
            planned.assert_called_once_with(db_path=database.resolve())

            stdout = io.StringIO()
            with (
                patch.object(
                    cli,
                    "apply_gray_review_queue_sync",
                    return_value={"applied_count": 1},
                ) as applied,
                redirect_stdout(stdout),
            ):
                code = cli.main(
                    [
                        "--db",
                        str(database),
                        "--apply",
                        "--confirm-plan-sha256",
                        "a" * 64,
                    ]
                )
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(stdout.getvalue())["mode"], "apply")
            applied.assert_called_once_with(
                expected_plan_sha256="a" * 64,
                db_path=database.resolve(),
            )

    def test_apply_requires_confirmation_before_dispatch(self) -> None:
        stderr = io.StringIO()
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "evaluation.sqlite3"
            database.write_bytes(b"fixture")
            with (
                patch.object(cli, "apply_gray_review_queue_sync") as applied,
                redirect_stderr(stderr),
            ):
                code = cli.main(["--db", str(database), "--apply"])
        self.assertEqual(code, 1)
        self.assertIn("requires --confirm-plan-sha256", stderr.getvalue())
        applied.assert_not_called()


if __name__ == "__main__":
    unittest.main()
