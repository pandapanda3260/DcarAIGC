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
SCRIPT = ROOT / "scripts" / "normalize_content_directions.py"
SPEC = importlib.util.spec_from_file_location("normalize_content_directions", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cli)


class NormalizeContentDirectionsCliTest(unittest.TestCase):
    def test_database_is_explicit_and_dispatches_the_resolved_path(self) -> None:
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            cli.build_parser().parse_args([])
        self.assertEqual(error.exception.code, 2)

        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "content.sqlite3"
            database.write_bytes(b"fixture")
            stdout = io.StringIO()
            with (
                patch.object(
                    cli,
                    "normalize_unknown_content_directions",
                    return_value={"updated_rows": 3},
                ) as invoked,
                redirect_stdout(stdout),
            ):
                code = cli.main(["--db", str(database)])
        self.assertEqual(code, 0)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {
                "command": "normalize-content-directions",
                "ok": True,
                "result": {"updated_rows": 3},
            },
        )
        invoked.assert_called_once_with(db_path=database.resolve())

    def test_missing_database_fails_without_creating_it(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "missing.sqlite3"
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                code = cli.main(["--db", str(database)])
            self.assertFalse(database.exists())
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(stderr.getvalue())["ok"], False)


if __name__ == "__main__":
    unittest.main()
