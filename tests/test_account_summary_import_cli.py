"""Exercise the real isolated Excel-import CLI against schema 21 and 22.

The embedded XLSX is an artifact-tool-generated, fictional two-row workbook.
Keeping its exact ZIP bytes makes the parser regression independent of optional
spreadsheet packages, including text identifiers longer than IEEE-754 precision.
"""
from __future__ import annotations

import base64
from contextlib import redirect_stdout
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

from v8 import schema_v22
from v8.storage import connect, initialize_database

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("account_summary_import_cli", ROOT / "scripts/import_account_summary.py")
cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cli)
AT = "2026-09-12T00:00:00Z"
FIXTURE_XLSX = (
    "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"
    "wLjrwLgbQG51NJecePL5lryU1PWyuaK7ZavmUVWz5OdojlFoyo2+3UjsO3USSTS15vi+YydXFVM3TBdfIZoqOY6fXkQF02/X/L9Q"
    "SwMEFAAAAAAA9YssXc7hFXAoAQAAKAEAAAsAAABfcmVscy8ucmVsc++7vzw/eG1sIHZlcnNpb249IjEuMCIgZW5jb2Rpbmc9InV0"
    "Zi04Ij8+PFJlbGF0aW9uc2hpcHMgeG1sbnM9Imh0dHA6Ly9zY2hlbWFzLm9wZW54bWxmb3JtYXRzLm9yZy9wYWNrYWdlLzIwMDYv"
    "cmVsYXRpb25zaGlwcyI+PFJlbGF0aW9uc2hpcCBUeXBlPSJodHRwOi8vc2NoZW1hcy5vcGVueG1sZm9ybWF0cy5vcmcvb2ZmaWNl"
    "RG9jdW1lbnQvMjAwNi9yZWxhdGlvbnNoaXBzL29mZmljZURvY3VtZW50IiBUYXJnZXQ9Ii94bC93b3JrYm9vay54bWwiIElkPSJS"
    "ZDQ5NDI4ODE2Yjg5NGViNyIgLz48L1JlbGF0aW9uc2hpcHM+UEsDBBQAAAAIAPWLLF3oVCTaEQEAAPICAAAaAAAAeGwvX3JlbHMv"
    "d29ya2Jvb2sueG1sLnJlbHO1kktOwzAQhq9ieU/stI7toqbdsGFbegHHnjxUPyLbhfRsLDgSV0BQhBLEgk03s/hH+vTNr3l/fdvu"
    "J2fRM8Q0BF/jsqAYgdfBDL6r8Tm3dxLvd9sDWJWH4FM/jAlNzvpU4z7n8Z6QpHtwKhVhBD8524boVE5FiB0ZlT6pDsiKUk7inIGX"
    "THS8jPAfYmjbQcND0GcHPv8BJilfLCSMjip2kGtMJvudFZOzGD2aGh9KI0uoqJDUSNbAGiNyM6Hcg4Olz1d0neXMiq+FFoxvBGcN"
    "U4Le0ir1KoJ5ynHw3e+25quZnlKS8nZTmVILtpLNLfVeQjylHiAv1X7izwMA8rw9EKWRSlVccMOYvrZHFp+7+wBQSwMEFAAAAAgA"
    "9YssXY2C2akWAQAAUwMAABMAAABbQ29udGVudF9UeXBlc10ueG1srZNBTsMwEEWvEnmLaqcsEEJJuwC2gAQXsJxJYtUeW55pSM/G"
    "giNxBVQHRYCQItRuPJvxe/8v5uPtvdqO3hUDJLIBa7GWpSgATWgsdrXYc7u6FttN9XKIQMXoHVIteuZ4oxSZHrwmGSLg6F0bktdM"
    "MqRORW12ugN1WZZXygRkQF7xkSE21R20eu+4uB8ZcNKO3onidto7qmqhY3TWaLYB1YDNL8kqtK010ASz94AsKSbQDfUA7J3MU3pt"
    "8SKD1Z/OBI7+J/1qJRO4vEO9jTQrHgdIyTZQPOnED9pDLdToFPHBAckzN8zQJTX34GF61ycHyJjFsr1O0DxzstidvfN39lKQ15B2"
    "+SOpPE7v/zPMzJ+DqHwim09QSwECFAMUAAAACAD1iyxdjVVJzcgAAAAkAQAADwAAAAAAAAAAAAAApIEAAAAAeGwvd29ya2Jvb2su"
    "eG1sUEsBAhQDFAAAAAgA9YssXVrp8WGnAQAAdgYAAA0AAAAAAAAAAAAAAKSB9QAAAHhsL3N0eWxlcy54bWxQSwECFAMUAAAACAD1"
    "iyxd+lwBWQMDAADaDQAAEwAAAAAAAAAAAAAApIHHAgAAeGwvdGhlbWUvdGhlbWUxLnhtbFBLAQIUAxQAAAAIAPWLLF0NHrnoZQAA"
    "AHMAAAAUAAAAAAAAAAAAAACkgfsFAAB4bC9zaGFyZWRTdHJpbmdzLnhtbFBLAQIUAxQAAAAIAPWLLF36pj/ZDgQAAO8OAAAYAAAA"
    "AAAAAAAAAACkgZIGAAB4bC93b3Jrc2hlZXRzL3NoZWV0MS54bWxQSwECFAMUAAAAAAD1iyxdzuEVcCgBAAAoAQAACwAAAAAAAAAA"
    "AAAApIHWCgAAX3JlbHMvLnJlbHNQSwECFAMUAAAACAD1iyxd6FQk2hEBAADyAgAAGgAAAAAAAAAAAAAApIEnDAAAeGwvX3JlbHMv"
    "d29ya2Jvb2sueG1sLnJlbHNQSwECFAMUAAAACAD1iyxdjYLZqRYBAABTAwAAEwAAAAAAAAAAAAAApIFwDQAAW0NvbnRlbnRfVHlw"
    "ZXNdLnhtbFBLBQYAAAAACAAIAAMCAAC3DgAAAAA="
)


class AccountSummaryImportCliTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.xlsx = self.root / "summary.xlsx"
        self.xlsx.write_bytes(base64.b64decode(FIXTURE_XLSX))
        self.sequence = 0
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))

    def database(self, version):
        path = self.root / f"schema{version}.sqlite3"
        with connect(path) as connection:
            initialize_database(connection, target_version=21)
            connection.execute("INSERT INTO accounts(id,phone,operator_name,created_at,updated_at) VALUES (99,'old phone','old operator',?,?)", (AT, AT))
            connection.execute("INSERT INTO account_platform_identities(id,account_id,platform,uid,nickname,created_at,updated_at) VALUES (99,99,'douyin','99999999','historical account',?,?)", (AT, AT))
            connection.execute("CREATE TABLE import_cli_history(id INTEGER PRIMARY KEY,account_id INTEGER NOT NULL REFERENCES accounts(id),value TEXT)")
            connection.execute("INSERT INTO import_cli_history VALUES (1,99,'preserve this exact historical value')")
            connection.commit()
            if version == 22:
                schema_v22.migrate(connection)
        return path

    def snapshot(self, path):
        with cli.readonly(path) as connection:
            schema = connection.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name").fetchall()
            values = {}
            for (name,) in connection.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
                quoted = '"' + name.replace('"', '""') + '"'
                values[name] = sorted(connection.execute("SELECT * FROM " + quoted).fetchall(), key=repr)
            return schema, values

    def run_cli(self, path, *, apply=True):
        self.sequence += 1
        report = self.root / f"report-{self.sequence}.json"
        backup = self.root / f"backup-{self.sequence}.sqlite3"
        argv = ["import_account_summary.py", "--db", str(path), "--xlsx", str(self.xlsx), "--report", str(report)]
        if apply:
            argv += ["--apply", "--backup", str(backup)]
        previous_umask = os.umask(0o077)
        try:
            with patch.object(sys, "argv", argv), redirect_stdout(io.StringIO()):
                cli.main()
        finally:
            os.umask(previous_umask)
        return json.loads(report.read_text()), backup

    def test_real_xlsx_schema21_and22_import_then_zero_write_replay(self):
        for version in (21, 22):
            with self.subTest(version=version):
                path = self.database(version)
                before = self.snapshot(path)
                result, backup = self.run_cli(path)
                self.assertGreater(result["writes"], 0)
                self.assertEqual(self.snapshot(backup), before)
                self.assertEqual(result["total"], 2)
                self.assertTrue(result["schema_unchanged"])
                self.assertEqual(result["foreign_key_check"], "ok")
                with cli.readonly(path) as connection:
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 1)
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM account_directory_rows").fetchone()[0], 2)
                    self.assertEqual(connection.execute("SELECT value FROM import_cli_history WHERE id=1").fetchone()[0], "preserve this exact historical value")
                    if version == 22:
                        self.assertEqual(connection.execute("SELECT COUNT(*) FROM account_intake_requests").fetchone()[0], 2)
                        inputs = [json.loads(row[0]) for row in connection.execute("SELECT input_json FROM account_intake_requests ORDER BY id")]
                        self.assertEqual(inputs[0]["uid"], "000123456789012345678901")
                after = self.snapshot(path)
                self.assertEqual(after[0], before[0])
                for table, rows in before[1].items():
                    if table not in {"account_directory_rows", "account_intake_requests", "sqlite_sequence"}:
                        self.assertEqual(after[1][table], rows, table)
                replay, _ = self.run_cli(path)
                self.assertEqual(replay["writes"], 0)
                self.assertEqual(self.snapshot(path), after)
                self.assertEqual(backup.stat().st_mode & 0o777, 0o600)

    def test_real_xlsx_identifiers_and_distinct_role_fields_remain_exact(self):
        path = self.database(22)
        self.run_cli(path)
        with cli.readonly(path) as connection:
            row = connection.execute("SELECT uid,display_account_id,phone,raw_json,account_status,operator_name FROM account_directory_rows WHERE platform='douyin'").fetchone()
            self.assertEqual(row[:3], ("000123456789012345678901", "000012345678901234567890", "00123456789"))
            self.assertEqual(row[4:], ("paused", "测试运营"))
            raw = json.loads(row[3])
            fields = raw["account_summary"]["fields"]
            self.assertEqual(fields["使用人证件号码"], "001234567890123456")
            self.assertEqual(fields["手机号开卡人姓名"], "测试开卡人")
            self.assertEqual(fields["持卡人"], "测试持卡人")
            self.assertEqual(connection.execute("SELECT typeof(uid),typeof(display_account_id),typeof(phone) FROM account_directory_rows WHERE platform='douyin'").fetchone(), ("text", "text", "text"))

    def test_dry_run_rolls_back_new_journal_directory_and_sequences(self):
        path = self.database(22)
        before = self.snapshot(path)
        result, backup = self.run_cli(path, apply=False)
        self.assertGreater(result["writes"], 0)
        self.assertEqual(result["mode"], "dry_run")
        self.assertFalse(backup.exists())
        self.assertEqual(self.snapshot(path), before)

    def test_mid_import_failure_rolls_back_every_row_and_keeps_backup(self):
        path = self.database(22)
        before = self.snapshot(path)
        from v8.account_summary_import import import_account_summary
        def fail_after_import(connection, payload, *, imported_at):
            import_account_summary(connection, payload, imported_at=imported_at)
            raise RuntimeError("injected after both new journal rows")
        with patch("v8.account_summary_import.import_account_summary", side_effect=fail_after_import):
            with self.assertRaisesRegex(RuntimeError, "injected"):
                self.run_cli(path)
        self.assertEqual(self.snapshot(path), before)
        self.assertEqual(self.snapshot(self.root / "backup-1.sqlite3"), before)
        self.assertFalse((self.root / "report-1.json").exists())

    def test_unrelated_same_count_update_insert_delete_and_sequence_write_are_denied(self):
        from v8.account_summary_import import import_account_summary
        for version in (21, 22):
            path = self.database(version)
            before = self.snapshot(path)
            for statement in (
                "UPDATE import_cli_history SET value='corrupt' WHERE id=1",
                "INSERT INTO import_cli_history VALUES (2,99,'unexpected')",
                "DELETE FROM import_cli_history WHERE id=1",
                "UPDATE sqlite_sequence SET seq=99999 WHERE name='accounts'",
                "DELETE FROM account_directory_rows",
            ):
                with self.subTest(version=version, statement=statement):
                    def corrupt(connection, payload, *, imported_at):
                        result = import_account_summary(connection, payload, imported_at=imported_at)
                        connection.execute(statement)
                        return result
                    with patch("v8.account_summary_import.import_account_summary", side_effect=corrupt):
                        with self.assertRaisesRegex(sqlite3.DatabaseError, "not authorized"):
                            self.run_cli(path)
                    self.assertEqual(self.snapshot(path), before)

    def test_trigger_cannot_write_unrelated_history(self):
        path = self.database(22)
        with connect(path) as connection:
            connection.execute("CREATE TRIGGER corrupt_import AFTER INSERT ON account_intake_requests BEGIN UPDATE import_cli_history SET value='corrupt'; END")
            connection.commit()
        before = self.snapshot(path)
        with self.assertRaisesRegex(sqlite3.DatabaseError, "not authorized"):
            self.run_cli(path)
        self.assertEqual(self.snapshot(path), before)

    def test_cli_enables_both_connection_safety_pragmas_before_import(self):
        path = self.database(22)
        from v8.account_summary_import import import_account_summary
        def check_safety(connection, payload, *, imported_at):
            self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
            self.assertEqual(connection.execute("PRAGMA recursive_triggers").fetchone()[0], 1)
            return import_account_summary(connection, payload, imported_at=imported_at)
        with patch("v8.account_summary_import.import_account_summary", side_effect=check_safety):
            result, _ = self.run_cli(path)
        self.assertEqual(result["after_counts"]["account_intake_requests"], 2)


if __name__ == "__main__":
    unittest.main()
