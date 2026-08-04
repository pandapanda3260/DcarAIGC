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
SCRIPT = ROOT / "scripts" / "repair_legacy_illegal_scenes.py"
SPEC = importlib.util.spec_from_file_location("repair_legacy_illegal_scenes", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cli)


class RepairLegacyIllegalScenesCliTest(unittest.TestCase):
    def test_all_operator_inputs_are_explicit_and_forwarded(self) -> None:
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            cli.build_parser().parse_args([])
        self.assertEqual(error.exception.code, 2)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = root / "database.sqlite3"
            manifest = root / "manifest.json"
            receipt = root / "receipt.json"
            for path in (database, manifest, receipt):
                path.write_text("{}\n", encoding="utf-8")
            stdout = io.StringIO()
            with (
                patch.object(
                    cli,
                    "repair_legacy_illegal_scene_chains",
                    return_value={"invalidated_count": 1514},
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
                        "--reason",
                        "approved repair",
                        "--expected-plan-sha256",
                        "a" * 64,
                        "--apply",
                        "--close-rollback-window",
                    ]
                )
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(stdout.getvalue())["ok"])
        invoked.assert_called_once_with(
            db_path=database.resolve(),
            manifest_path=manifest.resolve(),
            receipt_path=receipt.resolve(),
            operator_reason="approved repair",
            apply=True,
            expected_plan_sha256="a" * 64,
            acknowledge_rollback_window_close=True,
        )

    def test_missing_database_is_not_created(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = root / "missing.sqlite3"
            manifest = root / "manifest.json"
            receipt = root / "receipt.json"
            manifest.write_text("{}\n", encoding="utf-8")
            receipt.write_text("{}\n", encoding="utf-8")
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
                        "--reason",
                        "dry run",
                    ]
                )
            self.assertFalse(database.exists())
        self.assertEqual(code, 1)
        self.assertFalse(json.loads(stderr.getvalue())["ok"])

    def test_release_management_error_is_returned_as_json(self) -> None:
        stderr = io.StringIO()
        with (
            patch.object(
                cli,
                "repair_legacy_illegal_scene_chains",
                side_effect=cli.ReleaseManagementError("db locked"),
            ),
            redirect_stderr(stderr),
        ):
            code = cli.main(
                [
                    "--db",
                    "/tmp/existing.sqlite3",
                    "--manifest",
                    "/tmp/manifest.json",
                    "--receipt",
                    "/tmp/receipt.json",
                    "--reason",
                    "dry run",
                ]
            )
        self.assertEqual(code, 1)
        payload = json.loads(stderr.getvalue())
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "db locked")

    def test_close_acknowledgement_without_apply_is_rejected_before_service(
        self,
    ) -> None:
        stderr = io.StringIO()
        with (
            patch.object(cli, "repair_legacy_illegal_scene_chains") as invoked,
            redirect_stderr(stderr),
        ):
            code = cli.main(
                [
                    "--db",
                    "/tmp/existing.sqlite3",
                    "--manifest",
                    "/tmp/manifest.json",
                    "--receipt",
                    "/tmp/receipt.json",
                    "--reason",
                    "dry run",
                    "--close-rollback-window",
                ]
            )
        self.assertEqual(code, 1)
        self.assertFalse(json.loads(stderr.getvalue())["ok"])
        invoked.assert_not_called()


if __name__ == "__main__":
    unittest.main()
