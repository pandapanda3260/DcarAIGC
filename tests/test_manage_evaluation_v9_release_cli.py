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
SCRIPT = ROOT / "scripts" / "manage_evaluation_v9_release.py"
SPEC = importlib.util.spec_from_file_location("manage_evaluation_v9_release", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cli)


class ManageEvaluationV9ReleaseCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.db = self.root / "database.sqlite3"
        self.manifest = self.root / "manifest.json"
        self.receipt = self.root / "receipt.json"
        self.db.write_bytes(b"database")
        self.manifest.write_text("{}\n", encoding="utf-8")
        self.receipt.write_text("{}\n", encoding="utf-8")
        self.manifest_sha = "a" * 64
        self.receipt_sha = "b" * 64

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _base(self, command: str) -> list[str]:
        return [
            command,
            "--db",
            str(self.db),
            "--manifest",
            str(self.manifest),
            "--manifest-sha256",
            self.manifest_sha,
            "--isolated-clone",
        ]

    def _run(self, argv: list[str]) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = cli.main(argv)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_every_command_requires_external_manifest_hash(self) -> None:
        for command in (
            "status",
            "create",
            "backfill",
            "verify-ready",
            "activate",
            "abort",
            "rollback-before-resume",
        ):
            with (
                self.subTest(command=command),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                cli.build_parser().parse_args(
                    [command, "--db", str(self.db), "--manifest", str(self.manifest)]
                )

    def test_command_help_pins_schema_13_and_cutover_guards(self) -> None:
        stdout = io.StringIO()
        with redirect_stdout(stdout), self.assertRaises(SystemExit):
            cli.build_parser().parse_args(["status", "--help"])
        help_text = stdout.getvalue()
        self.assertIn("schema-v13", help_text)
        self.assertIn("--freeze-lock", help_text)
        self.assertIn("--isolated-clone", help_text)

    def test_status_create_and_backfill_dispatch_explicit_hash(self) -> None:
        for command in ("status", "create", "backfill"):
            with (
                self.subTest(command=command),
                patch.object(cli.management, command, return_value={"status": command}) as invoked,
            ):
                code, stdout, stderr = self._run(self._base(command))
            self.assertEqual((code, stderr), (0, ""))
            self.assertEqual(json.loads(stdout)["result"], {"status": command})
            invoked.assert_called_once_with(
                db_path=self.db.resolve(),
                manifest_path=self.manifest.resolve(),
                manifest_sha256=self.manifest_sha,
            )

    def test_nonformal_database_requires_explicit_isolated_clone(self) -> None:
        argv = self._base("status")
        argv.remove("--isolated-clone")
        with patch.object(cli.management, "status") as invoked:
            code, _stdout, stderr = self._run(argv)
        self.assertEqual(code, 1)
        self.assertIn("explicit --isolated-clone", stderr)
        invoked.assert_not_called()

    def test_activate_and_rollback_forward_receipt_hash(self) -> None:
        cases: tuple[tuple[str, list[str]], ...] = (
            ("activate", []),
            ("rollback-before-resume", ["--reason", "operator rollback"]),
        )
        for command, suffix in cases:
            argv = [
                *self._base(command),
                "--receipt",
                str(self.receipt),
                "--receipt-sha256",
                self.receipt_sha,
                *suffix,
            ]
            with patch.object(
                cli.management, command.replace("-", "_"), return_value={"status": "ok"}
            ) as invoked:
                code, stdout, stderr = self._run(argv)
            self.assertEqual((code, stderr), (0, ""))
            self.assertEqual(json.loads(stdout)["result"], {"status": "ok"})
            expected = {
                "db_path": self.db.resolve(),
                "manifest_path": self.manifest.resolve(),
                "manifest_sha256": self.manifest_sha,
                "receipt_path": self.receipt.resolve(),
                "receipt_sha256": self.receipt_sha,
            }
            if command == "rollback-before-resume":
                expected["reason"] = "operator rollback"
            invoked.assert_called_once_with(**expected)

    def test_verify_and_abort_dispatch_without_hidden_defaults(self) -> None:
        receipt_out = self.root / "new-receipt.json"
        with patch.object(
            cli.management, "verify_ready", return_value={"status": "ready"}
        ) as verify:
            code, _stdout, stderr = self._run(
                [*self._base("verify-ready"), "--receipt-out", str(receipt_out)]
            )
        self.assertEqual((code, stderr), (0, ""))
        verify.assert_called_once_with(
            db_path=self.db.resolve(),
            manifest_path=self.manifest.resolve(),
            manifest_sha256=self.manifest_sha,
            receipt_path=receipt_out.resolve(),
        )
        with patch.object(
            cli.management, "abort", return_value={"status": "failed"}
        ) as abort:
            code, _stdout, stderr = self._run(
                [*self._base("abort"), "--reason", "operator abort"]
            )
        self.assertEqual((code, stderr), (0, ""))
        abort.assert_called_once_with(
            db_path=self.db.resolve(),
            manifest_path=self.manifest.resolve(),
            manifest_sha256=self.manifest_sha,
            reason="operator abort",
        )


if __name__ == "__main__":
    unittest.main()
