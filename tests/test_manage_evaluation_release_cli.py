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
SCRIPT = ROOT / "scripts" / "manage_evaluation_release.py"
SPEC = importlib.util.spec_from_file_location("manage_evaluation_release", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cli)
storage = importlib.import_module("v8.storage")


class ManageEvaluationReleaseCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "database.sqlite3"
        self.db.write_bytes(b"explicit-database")
        self.manifest = self.root / "manifest.json"
        self.manifest.write_text("{}\n", encoding="utf-8")
        self.receipt_a = self.root / "rehearsal-a.json"
        self.receipt_b = self.root / "rehearsal-b.json"
        self.production_receipt = self.root / "production.json"
        self.receipt_a.write_text("{}\n", encoding="utf-8")
        self.receipt_b.write_text("{}\n", encoding="utf-8")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _base(self, command: str) -> list[str]:
        return [
            command,
            "--db",
            str(self.db),
            "--manifest",
            str(self.manifest),
        ]

    def _run(self, argv: list[str]) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = cli.main(argv)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_every_command_requires_explicit_database_and_manifest(self) -> None:
        parser = cli.build_parser()
        commands = (
            "status",
            "create",
            "backfill",
            "verify-ready",
            "activate",
            "abort",
            "rollback-before-resume",
        )
        for command in commands:
            with (
                self.subTest(command=command),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as error,
            ):
                parser.parse_args([command])
            self.assertEqual(error.exception.code, 2)

    def test_status_create_and_backfill_dispatch_only_explicit_paths(self) -> None:
        cases = (
            ("status", "status", {"state": "active"}),
            ("create", "create", {"state": "draft"}),
            ("backfill", "backfill", {"created": 3}),
        )
        for command, function_name, result in cases:
            with (
                self.subTest(command=command),
                patch.object(
                    cli.management, function_name, return_value=result
                ) as invoked,
            ):
                code, stdout, stderr = self._run(self._base(command))
            self.assertEqual(code, 0)
            self.assertEqual(stderr, "")
            self.assertEqual(
                json.loads(stdout),
                {"command": command, "ok": True, "result": result},
            )
            expected = {"db_path": self.db.resolve()}
            if command != "status":
                expected["manifest_path"] = self.manifest.resolve()
            invoked.assert_called_once_with(**expected)

    def test_verify_ready_forwards_two_approved_receipts_and_output(self) -> None:
        result = {"status": "ready", "core_sha256": "a" * 64}
        argv = [
            *self._base("verify-ready"),
            "--production",
            "--approved-receipt",
            str(self.receipt_a),
            "--approved-receipt",
            str(self.receipt_b),
            "--receipt-out",
            str(self.production_receipt),
        ]
        with patch.object(
            cli.management, "verify_ready", return_value=result
        ) as verify:
            code, stdout, stderr = self._run(argv)
        self.assertEqual((code, stderr), (0, ""))
        self.assertEqual(json.loads(stdout)["result"], result)
        verify.assert_called_once_with(
            db_path=self.db.resolve(),
            manifest_path=self.manifest.resolve(),
            receipt_path=self.production_receipt.resolve(),
            rehearsal_receipt_paths=(
                self.receipt_a.resolve(),
                self.receipt_b.resolve(),
            ),
            production=True,
        )

    def test_rehearsal_verify_ready_accepts_no_approved_receipts(self) -> None:
        result = {"status": "ready", "mode": "rehearsal"}
        argv = [
            *self._base("verify-ready"),
            "--receipt-out",
            str(self.production_receipt),
        ]
        with patch.object(
            cli.management, "verify_ready", return_value=result
        ) as verify:
            code, stdout, stderr = self._run(argv)
        self.assertEqual((code, stderr), (0, ""))
        self.assertEqual(json.loads(stdout)["result"], result)
        verify.assert_called_once_with(
            db_path=self.db.resolve(),
            manifest_path=self.manifest.resolve(),
            receipt_path=self.production_receipt.resolve(),
            rehearsal_receipt_paths=(),
            production=False,
        )

    def test_activate_consumes_existing_production_receipt_only(self) -> None:
        self.production_receipt.write_text("{}\n", encoding="utf-8")
        argv = [
            *self._base("activate"),
            "--receipt",
            str(self.production_receipt),
        ]
        with (
            patch.object(cli.management, "verify_ready") as verify,
            patch.object(
                cli.management, "activate", return_value={"status": "active"}
            ) as activate,
        ):
            code, stdout, stderr = self._run(argv)
        self.assertEqual((code, stderr), (0, ""))
        self.assertEqual(json.loads(stdout)["result"], {"status": "active"})
        verify.assert_not_called()
        activate.assert_called_once_with(
            db_path=self.db.resolve(),
            manifest_path=self.manifest.resolve(),
            receipt_path=self.production_receipt.resolve(),
        )

    def test_activate_surfaces_production_receipt_validation_failure(self) -> None:
        self.production_receipt.write_text("{}\n", encoding="utf-8")
        argv = [
            *self._base("activate"),
            "--receipt",
            str(self.production_receipt),
        ]
        with patch.object(
            cli.management,
            "activate",
            side_effect=cli.management.ReleaseManagementError("receipt mismatch"),
        ) as activate:
            code, stdout, stderr = self._run(argv)
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        activate.assert_called_once_with(
            db_path=self.db.resolve(),
            manifest_path=self.manifest.resolve(),
            receipt_path=self.production_receipt.resolve(),
        )
        error = json.loads(stderr)
        self.assertFalse(error["ok"])
        self.assertEqual(error["error"], "receipt mismatch")

    def test_abort_and_rollback_forward_operator_reason_and_receipt(self) -> None:
        receipt = self.root / "active-receipt.json"
        receipt.write_text("{}\n", encoding="utf-8")
        with patch.object(
            cli.management, "abort", return_value={"status": "failed"}
        ) as abort:
            code, _, stderr = self._run(
                [*self._base("abort"), "--reason", "operator abort"]
            )
        self.assertEqual((code, stderr), (0, ""))
        abort.assert_called_once_with(
            db_path=self.db.resolve(),
            manifest_path=self.manifest.resolve(),
            reason="operator abort",
        )

        with patch.object(
            cli.management,
            "rollback_before_resume",
            return_value={"status": "retired"},
        ) as rollback:
            code, _, stderr = self._run(
                [
                    *self._base("rollback-before-resume"),
                    "--receipt",
                    str(receipt),
                    "--reason",
                    "rollback boundary",
                ]
            )
        self.assertEqual((code, stderr), (0, ""))
        rollback.assert_called_once_with(
            db_path=self.db.resolve(),
            manifest_path=self.manifest.resolve(),
            receipt_path=receipt.resolve(),
            reason="rollback boundary",
        )

    def test_receipt_arguments_reject_ambiguous_or_destructive_paths(self) -> None:
        cases = (
            [
                *self._base("verify-ready"),
                "--production",
                "--approved-receipt",
                str(self.receipt_a),
                "--receipt-out",
                str(self.production_receipt),
            ],
            [
                *self._base("verify-ready"),
                "--production",
                "--approved-receipt",
                str(self.receipt_a),
                "--approved-receipt",
                str(self.receipt_a),
                "--receipt-out",
                str(self.production_receipt),
            ],
            [
                *self._base("verify-ready"),
                "--approved-receipt",
                str(self.receipt_a),
                "--approved-receipt",
                str(self.receipt_b),
                "--receipt-out",
                str(self.production_receipt),
            ],
            [
                *self._base("verify-ready"),
                "--production",
                "--approved-receipt",
                str(self.receipt_a),
                "--approved-receipt",
                str(self.receipt_b),
                "--receipt-out",
                str(self.receipt_a),
            ],
            [
                *self._base("activate"),
                "--receipt",
                str(self.root / "missing-production-receipt.json"),
            ],
        )
        with (
            patch.object(cli.management, "verify_ready") as verify,
            patch.object(cli.management, "activate") as activate,
        ):
            for argv in cases:
                with self.subTest(argv=argv):
                    code, stdout, stderr = self._run(argv)
                    self.assertEqual(code, 1)
                    self.assertEqual(stdout, "")
                    self.assertFalse(json.loads(stderr)["ok"])
        verify.assert_not_called()
        activate.assert_not_called()

    def test_missing_database_fails_without_creating_it_or_calling_lifecycle(
        self,
    ) -> None:
        missing = self.root / "missing.sqlite3"
        with patch.object(cli.management, "status") as status:
            code, stdout, stderr = self._run(
                [
                    "status",
                    "--db",
                    str(missing),
                    "--manifest",
                    str(self.manifest),
                ]
            )
        self.assertEqual(code, 1)
        self.assertEqual(stdout, "")
        self.assertIn("existing file", json.loads(stderr)["error"])
        self.assertFalse(missing.exists())
        status.assert_not_called()

    def test_status_runs_against_an_explicit_existing_v9_database(self) -> None:
        database = self.root / "status.sqlite3"
        with storage.connect(database) as connection:
            storage.initialize_database(connection)
            connection.commit()
        code, stdout, stderr = self._run(
            [
                "status",
                "--db",
                str(database),
                "--manifest",
                str(self.manifest),
            ]
        )
        self.assertEqual((code, stderr), (0, ""))
        result = json.loads(stdout)["result"]
        self.assertIsNone(result["release"])
        self.assertEqual(result["active_releases"], [])
        self.assertEqual(result["published_taxonomies"], [])

    def test_script_has_no_database_default_initialization_or_provider_import(
        self,
    ) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertNotIn("DEFAULT_DB", source)
        self.assertNotIn("initialize_database", source)
        self.assertNotIn("v8.providers", source)
        self.assertNotIn("from .providers", source)


if __name__ == "__main__":
    unittest.main()
