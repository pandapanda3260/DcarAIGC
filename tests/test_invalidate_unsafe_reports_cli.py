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
SCRIPT = ROOT / "scripts" / "invalidate_unsafe_reports.py"
SPEC = importlib.util.spec_from_file_location("invalidate_unsafe_reports", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cli)


class InvalidateUnsafeReportsCliTest(unittest.TestCase):
    def test_paths_are_required_and_resolved_for_dry_run_and_apply(self) -> None:
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            cli.build_parser().parse_args([])
        self.assertEqual(error.exception.code, 2)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = root / "database.sqlite3"
            manifest = root / "manifest.json"
            receipt = root / "receipt.json"
            artifacts = root / "artifacts"
            database.write_bytes(b"database")
            manifest.write_text("{}\n", encoding="utf-8")
            receipt.write_text("{}\n", encoding="utf-8")
            artifacts.mkdir()
            stdout = io.StringIO()
            with (
                patch.object(
                    cli,
                    "invalidate_unsafe_automatic_reports",
                    return_value={"updated_revisions": 3},
                ) as invoked,
                redirect_stdout(stdout),
            ):
                code = cli.main(
                    [
                        "--db",
                        str(database),
                        "--manifest",
                        str(manifest),
                        "--receipt",
                        str(receipt),
                        "--artifact-root",
                        str(artifacts),
                        "--apply",
                        "--close-rollback-window",
                    ]
                )
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout.getvalue())["ok"], True)
        invoked.assert_called_once_with(
            db_path=database.resolve(),
            manifest_path=manifest.resolve(),
            receipt_path=receipt.resolve(),
            artifact_root=artifacts.resolve(),
            apply=True,
            acknowledge_rollback_window_close=True,
        )

    def test_missing_database_fails_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = root / "missing.sqlite3"
            manifest = root / "manifest.json"
            receipt = root / "receipt.json"
            artifacts = root / "artifacts"
            manifest.write_text("{}\n", encoding="utf-8")
            receipt.write_text("{}\n", encoding="utf-8")
            artifacts.mkdir()
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                code = cli.main(
                    [
                        "--db",
                        str(database),
                        "--manifest",
                        str(manifest),
                        "--receipt",
                        str(receipt),
                        "--artifact-root",
                        str(artifacts),
                    ]
                )
            self.assertFalse(database.exists())
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(stderr.getvalue())["ok"], False)


if __name__ == "__main__":
    unittest.main()
