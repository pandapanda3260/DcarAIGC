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
SCRIPT = ROOT / "scripts" / "prepare_evaluation_v9_release.py"
SPEC = importlib.util.spec_from_file_location("prepare_evaluation_v9_release", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cli)


class PrepareEvaluationV9ReleaseCliTest(unittest.TestCase):
    def test_forwards_only_explicit_paths_and_returns_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            db = root / "database.sqlite3"
            manifest = root / "manifest.json"
            db.write_bytes(b"database")
            stdout = io.StringIO()
            stderr = io.StringIO()
            result = {"manifest_sha256": "a" * 64, "content_count": 2}
            with (
                patch.object(cli.management, "prepare_manifest", return_value=result) as invoked,
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                code = cli.main(
                    [
                        "--db",
                        str(db),
                        "--manifest-out",
                        str(manifest),
                        "--isolated-clone",
                    ]
                )
        self.assertEqual((code, stderr.getvalue()), (0, ""))
        self.assertEqual(json.loads(stdout.getvalue())["result"], result)
        invoked.assert_called_once_with(
            db_path=db.resolve(), manifest_path=manifest.resolve()
        )

    def test_nonformal_database_requires_explicit_isolated_clone(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            db = root / "database.sqlite3"
            manifest = root / "manifest.json"
            db.write_bytes(b"database")
            stderr = io.StringIO()
            with (
                patch.object(cli.management, "prepare_manifest") as invoked,
                redirect_stdout(io.StringIO()),
                redirect_stderr(stderr),
            ):
                code = cli.main(
                    ["--db", str(db), "--manifest-out", str(manifest)]
                )
        self.assertEqual(code, 1)
        self.assertIn("explicit --isolated-clone", stderr.getvalue())
        invoked.assert_not_called()

    def test_help_pins_schema_13_and_cutover_guards(self) -> None:
        help_text = cli.build_parser().format_help()
        self.assertIn("schema-v13", help_text)
        self.assertIn("--freeze-lock", help_text)
        self.assertIn("--isolated-clone", help_text)


if __name__ == "__main__":
    unittest.main()
