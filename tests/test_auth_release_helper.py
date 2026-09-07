from __future__ import annotations

import fcntl
import importlib.util
import json
import shutil
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "deploy/server/libexec/dcar-auth-release.py"
sys.path.insert(0, str(ROOT / "src/dcar_eval"))
from dcar_auth import store as auth_store  # noqa: E402


def load_helper():
    spec = importlib.util.spec_from_file_location("auth_release_test", HELPER)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


class SmokeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, duration):
        self.sleeps.append(duration)
        self.now += duration


class AuthReleaseHelperTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.helper = load_helper()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.releases = self.root / "releases"
        self.releases.mkdir()
        self.previous = self.releases / "previous"
        self.previous.mkdir()
        self.candidate = self.releases / "candidate"
        self.candidate.mkdir()
        (self.candidate / ".venv/bin").mkdir(parents=True)
        python = self.candidate / ".venv/bin/python"
        python.write_text(f'#!/bin/sh\nexec {shlex.quote(sys.executable)} "$@"\n')
        python.chmod(0o755)
        (self.candidate / "src").mkdir()
        (self.candidate / "src/dcar_eval").symlink_to(
            ROOT / "src/dcar_eval", target_is_directory=True
        )
        shutil.copytree(ROOT / "deploy/server", self.candidate / "deploy/server")
        self.current = self.root / "current"
        self.current.symlink_to(self.previous)
        for directory in ("runtime", "backups", "units", "libexec", "auth"):
            (self.root / directory).mkdir()
        self.database = self.root / "auth/sessions.sqlite3"
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "CREATE TABLE auth_sessions(token_sha256 TEXT PRIMARY KEY, username TEXT NOT NULL, credential_fingerprint TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL)"
            )
        self.htpasswd = self.root / "htpasswd"
        self.htpasswd.write_text(
            "operator:" + auth_store.hash_password("Safe passphrase 971!") + "\n"
        )
        self.old_htpasswd = self.htpasswd.read_bytes()
        for unit in self.helper.SERVICES:
            (self.root / "units" / unit).write_text("OLD " + unit)
        self.events: list[str] = []
        self.config = self.helper.ReleaseConfig(
            self.candidate,
            self.current,
            self.releases,
            self.root / "runtime",
            self.database,
            self.root / "backups",
            self.htpasswd,
            self.root / "auth/auth-changes.log",
            self.root / "runtime/release.json",
            "operator",
            "",
            Path("/unused/systemctl"),
            ("pre",),
            ("post",),
            self.root / "units",
            self.root / "libexec",
        )

    def service(self, config, action):
        self.events.append(action)

    def smoke(self, urls):
        self.events.append("smoke:" + urls[0])
        if urls[0] == "pre":
            self.assertEqual(self.current.resolve(), self.previous)
            self.assertNotIn("stop", self.events)

    def deploy(self, **kwargs):
        return self.helper.deploy_release(
            self.config, smoke_check=self.smoke, service_action=self.service, **kwargs
        )

    def test_real_migration_install_and_schema_zero_rollback(self):
        result = self.deploy()
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["auth_identity"]["superadmins"], 1)
        self.assertEqual(self.events[0:2], ["smoke:pre", "stop"])
        self.assertEqual(self.current.resolve(), self.candidate)
        self.assertTrue((self.root / "libexec/dcar-auth-backup").is_file())
        with patch.object(
            self.helper, "__file__", str(self.root / "libexec/dcar-auth-release")
        ):
            self.assertEqual(
                self.helper._backup_module().database_version(self.database), 3
            )
        self.assertIn(
            "LoadCredential", (self.root / "units/dcar-auth.service").read_text()
        )
        rolled = self.helper.rollback_release(
            self.config, smoke_check=lambda _: None, service_action=self.service
        )
        self.assertEqual(rolled["status"], "rolled_back")
        self.assertEqual(self.current.resolve(), self.previous)
        self.assertEqual(self.htpasswd.read_bytes(), self.old_htpasswd)
        for unit in self.helper.SERVICES:
            self.assertEqual((self.root / "units" / unit).read_text(), "OLD " + unit)
        self.assertFalse((self.root / "units/dcar-auth-backup.timer").exists())
        self.assertFalse((self.root / "libexec/dcar-auth-backup").exists())

    def test_import_failure_preserves_htpasswd_and_old_units(self):
        def admin(config, args):
            if args[0] == "import-htpasswd":
                raise self.helper.ReleaseError("simulated import failure")
            return self.helper._run_admin(config, args)

        with self.assertRaises(self.helper.ReleaseError):
            self.deploy(admin_runner=admin)
        self.assertEqual(self.current.resolve(), self.previous)
        self.assertEqual(self.htpasswd.read_bytes(), self.old_htpasswd)
        result = self.helper.rollback_release(
            self.config, smoke_check=lambda _: None, service_action=self.service
        )
        self.assertEqual(result["status"], "rolled_back")
        self.assertEqual(self.htpasswd.read_bytes(), self.old_htpasswd)

    def test_post_smoke_failure_keeps_receipt_for_complete_rollback(self):
        def smoke(urls):
            self.smoke(urls)
            if urls[0] == "post":
                raise self.helper.ReleaseError("post smoke failed")

        with self.assertRaises(self.helper.ReleaseError):
            self.helper.deploy_release(
                self.config, smoke_check=smoke, service_action=self.service
            )
        receipt = json.loads(self.config.receipt.read_text())
        self.assertEqual(receipt["status"], "failed_after_switch")
        self.assertFalse(receipt["services_started"])
        result = self.helper.rollback_release(
            self.config, smoke_check=lambda _: None, service_action=self.service
        )
        self.assertEqual(result["status"], "rolled_back")

    def test_lock_contention_changes_nothing(self):
        lock = self.config.lock_path.open("w")
        self.addCleanup(lock.close)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with self.assertRaisesRegex(self.helper.ReleaseError, "lock is busy"):
            self.deploy()
        self.assertEqual(self.events, ["smoke:pre"])
        self.assertFalse(self.config.receipt.exists())

    def test_disk_budget_accepts_96_percent_used_with_sufficient_capacity(self):
        gib = 1024**3
        with patch.object(
            self.helper, "_disk_usage", return_value=(1, 200 * gib, 8 * gib)
        ):
            result = self.deploy()
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["disk_preflight"], result["disk_locked"])
        disk = result["disk_locked"][0]
        self.assertEqual(disk["reserve_bytes"], 6 * gib)
        self.assertGreater(disk["planned_write_bytes"], self.database.stat().st_size)
        self.assertEqual(
            disk["required_available_bytes"],
            disk["reserve_bytes"] + disk["planned_write_bytes"],
        )

    def test_disk_budget_checks_absolute_and_percentage_floor_before_smoke(self):
        gib = 1024**3
        for total, available in ((10 * gib, 2 * gib - 1), (200 * gib, 5 * gib)):
            with (
                self.subTest(total=total),
                patch.object(
                    self.helper, "_disk_usage", return_value=(1, total, available)
                ),
            ):
                with self.assertRaisesRegex(
                    self.helper.ReleaseError, "insufficient disk headroom"
                ):
                    self.deploy()
            self.assertEqual(self.events, [])
            self.assertFalse(self.config.receipt.exists())
            self.assertEqual(self.current.resolve(), self.previous)
            self.assertEqual(
                self.helper._backup_module().database_version(self.database), 0
            )

    def test_disk_budget_rechecked_under_lock_before_service_stop(self):
        gib = 1024**3
        free = 10 * gib

        def checkpoint(name):
            nonlocal free
            if name == "after_pre_smoke":
                free = gib

        with patch.object(
            self.helper, "_disk_usage", side_effect=lambda _: (1, 100 * gib, free)
        ):
            with self.assertRaisesRegex(
                self.helper.ReleaseError, "insufficient disk headroom"
            ):
                self.deploy(checkpoint=checkpoint)
        self.assertEqual(self.events, ["smoke:pre"])
        self.assertFalse(self.config.receipt.exists())
        self.assertEqual(self.current.resolve(), self.previous)

    def test_disk_budget_covers_separate_backup_and_receipt_filesystems(self):
        gib = 1024**3
        for target in (self.config.backup_dir, self.config.receipt.parent):
            with self.subTest(target=target):

                def usage(path):
                    return (
                        (2, 20 * gib, gib)
                        if path == target
                        else (1, 100 * gib, 20 * gib)
                    )

                with patch.object(self.helper, "_disk_usage", side_effect=usage):
                    with self.assertRaisesRegex(
                        self.helper.ReleaseError, "insufficient disk headroom"
                    ):
                        self.deploy()
                self.assertEqual(self.events, [])
                self.assertFalse(self.config.receipt.exists())

    def test_disk_budget_scales_with_database_and_restore_backup_size(self):
        gib = 1024**3
        with self.database.open("r+b") as database:
            database.truncate(gib)
        with patch.object(
            self.helper, "_disk_usage", return_value=(1, 100 * gib, 8 * gib)
        ):
            with self.assertRaisesRegex(self.helper.ReleaseError, "planned writes"):
                self.helper._check_disk_headroom(self.config, operation="deploy")
        selected = self.root / "selected.sqlite3"
        with selected.open("wb") as backup:
            backup.truncate(3 * gib)
        with patch.object(
            self.helper, "_disk_usage", return_value=(1, 100 * gib, 12 * gib)
        ):
            with self.assertRaisesRegex(self.helper.ReleaseError, "planned writes"):
                self.helper._check_disk_headroom(
                    self.config, operation="restore", selected_backup=selected
                )

    def test_disk_budget_ignores_filesystems_not_written_by_operation(self):
        gib = 1024**3
        config = self.config._replace(htpasswd=self.root / "missing-nginx" / "htpasswd")
        with patch.object(
            self.helper, "_disk_usage", return_value=(1, 100 * gib, 20 * gib)
        ) as usage:
            self.helper._check_disk_headroom(config, operation="deploy")
            self.assertNotIn(
                config.htpasswd.parent, [call.args[0] for call in usage.call_args_list]
            )
            usage.reset_mock()
            self.helper._check_disk_headroom(config, operation="restore")
            measured = [call.args[0] for call in usage.call_args_list]
            self.assertNotIn(config.systemd_dir, measured)
            self.assertNotIn(config.libexec_dir, measured)
            self.assertNotIn(config.current_link.parent, measured)
            usage.reset_mock()
            self.helper._check_disk_headroom(
                config, operation="rollback", writes_htpasswd=True
            )
            self.assertIn(
                config.htpasswd.parent, [call.args[0] for call in usage.call_args_list]
            )

    def test_disk_usage_excludes_root_reserved_blocks(self):
        import os

        usage = os.statvfs_result(
            (4096, 4096, 1000000, 900000, 100000, 10, 9, 8, 0, 255)
        )
        with patch.object(self.helper.os, "statvfs", return_value=usage):
            device, total, available = self.helper._disk_usage(self.config.backup_dir)
        self.assertEqual(device, self.config.backup_dir.stat().st_dev)
        self.assertEqual(total, 1000000 * 4096)
        self.assertEqual(available, 100000 * 4096)

    def test_disk_budget_has_no_cli_or_environment_bypass(self):
        import os

        gib = 1024**3
        with patch.dict(
            os.environ,
            {"DCAR_AUTH_SKIP_DISK_CHECK": "1", "DCAR_AUTH_MIN_FREE_BYTES": "0"},
        ):
            with patch.object(
                self.helper, "_disk_usage", return_value=(1, 100 * gib, gib)
            ):
                with self.assertRaisesRegex(
                    self.helper.ReleaseError, "insufficient disk headroom"
                ):
                    self.deploy()
        options = self.helper.build_parser().format_help()
        self.assertNotIn("skip-disk", options)
        self.assertEqual(self.events, [])

    def interleaved_lock(self, mutation):
        original_lock = self.helper._release_lock

        @contextmanager
        def acquire(path):
            # Another compliant installer completes after the outer validation
            # but before this operation acquires the same real file lock.
            with original_lock(path):
                mutation()
            with original_lock(path):
                yield

        return patch.object(self.helper, "_release_lock", acquire)

    def test_rollback_rechecks_current_after_acquiring_lock(self):
        self.deploy()
        self.events.clear()
        other = self.releases / "another-installer"
        other.mkdir()
        before_database = self.database.read_bytes()
        before_receipt = self.config.receipt.read_bytes()
        with self.interleaved_lock(
            lambda: self.helper._switch_link(self.current, other)
        ):
            with self.assertRaisesRegex(
                self.helper.ReleaseError, "current release changed"
            ):
                self.helper.rollback_release(self.config, service_action=self.service)
        self.assertEqual(self.events, [])
        self.assertEqual(self.current.resolve(), other)
        self.assertEqual(self.database.read_bytes(), before_database)
        self.assertEqual(self.config.receipt.read_bytes(), before_receipt)

    def test_rollback_rechecks_receipt_after_acquiring_lock(self):
        self.deploy()
        self.events.clear()
        completed = json.loads(self.config.receipt.read_text())
        completed["status"] = "rolled_back"
        with self.interleaved_lock(
            lambda: self.helper._atomic_json(self.config.receipt, completed)
        ):
            with self.assertRaisesRegex(self.helper.ReleaseError, "receipt changed"):
                self.helper.rollback_release(self.config, service_action=self.service)
        self.assertEqual(self.events, [])
        self.assertEqual(self.current.resolve(), self.candidate)
        self.assertEqual(json.loads(self.config.receipt.read_text()), completed)

    def test_restore_rechecks_current_after_acquiring_lock(self):
        selected = self.helper._backup_module().create_backup(
            self.database, self.config.backup_dir, 0
        )
        self.helper._switch_link(self.current, self.candidate)
        before_database = self.database.read_bytes()
        with self.interleaved_lock(
            lambda: self.helper._switch_link(self.current, self.previous)
        ):
            with self.assertRaisesRegex(
                self.helper.ReleaseError, "current release changed"
            ):
                self.helper.restore_release(
                    self.config, Path(selected["path"]), 0, service_action=self.service
                )
        self.assertEqual(self.events, [])
        self.assertEqual(self.current.resolve(), self.previous)
        self.assertEqual(self.database.read_bytes(), before_database)
        self.assertFalse(self.config.receipt.exists())

    def test_deploy_and_restore_refuse_receipt_created_before_lock(self):
        selected = self.helper._backup_module().create_backup(
            self.database, self.config.backup_dir, 0
        )
        for operation in ("deploy", "restore"):
            with self.subTest(operation=operation):
                config = self.config._replace(
                    receipt=self.root / "runtime" / f"race-{operation}.json"
                )
                if operation == "restore":
                    self.helper._switch_link(self.current, self.candidate)
                self.events.clear()
                existing = {
                    "schema": self.helper.RECEIPT_SCHEMA,
                    "owner": "other operation",
                }
                with self.interleaved_lock(
                    lambda: self.helper._atomic_json(config.receipt, existing)
                ):
                    with self.assertRaisesRegex(
                        self.helper.ReleaseError, "receipt path must still be new"
                    ):
                        if operation == "deploy":
                            self.helper.deploy_release(
                                config,
                                smoke_check=lambda _: None,
                                service_action=self.service,
                            )
                        else:
                            self.helper.restore_release(
                                config,
                                Path(selected["path"]),
                                0,
                                service_action=self.service,
                            )
                self.assertEqual(self.events, [])
                self.assertEqual(json.loads(config.receipt.read_text()), existing)

    def test_real_loopback_smoke_checks_http_failures(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200 if self.path == "/ok" else 503)
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        base = f"http://127.0.0.1:{server.server_port}"
        self.helper._http_smoke([base + "/ok"])
        with self.assertRaises(self.helper.ReleaseError):
            self.helper._http_smoke(
                [base + "/failed"], readiness_timeout=0.05, retry_interval=0.01
            )

    def test_smoke_retries_connection_refused_then_accepts_200(self):
        clock = SmokeClock()
        response = MagicMock()
        response.__enter__.return_value.status = 200
        with patch.object(
            self.helper.urllib.request,
            "urlopen",
            side_effect=[ConnectionRefusedError("starting"), response],
        ) as opener:
            self.helper._http_smoke(
                ["http://localhost/health"],
                readiness_timeout=3,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )
        self.assertEqual(opener.call_count, 2)
        self.assertEqual(clock.sleeps, [1.0])
        self.assertEqual([call.kwargs["timeout"] for call in opener.call_args_list], [3, 2])
        response.__exit__.assert_called_once()

    def test_smoke_retries_temporary_503_then_accepts_200(self):
        clock = SmokeClock()
        response = MagicMock()
        response.__enter__.return_value.status = 200
        failure = self.helper.urllib.error.HTTPError(
            "http://localhost/health", 503, "starting", {}, None
        )
        with patch.object(
            self.helper.urllib.request, "urlopen", side_effect=[failure, response]
        ) as opener:
            self.helper._http_smoke(
                ["http://localhost/health"],
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )
        self.assertEqual(opener.call_count, 2)
        self.assertEqual(clock.sleeps, [1.0])

    def test_smoke_persistent_failure_stops_at_shared_deadline(self):
        clock = SmokeClock()
        with patch.object(
            self.helper.urllib.request,
            "urlopen",
            side_effect=self.helper.urllib.error.URLError("not listening"),
        ) as opener:
            with self.assertRaisesRegex(self.helper.ReleaseError, "deadline .*URLError"):
                self.helper._http_smoke(
                    ["http://localhost/health"],
                    readiness_timeout=2.5,
                    monotonic=clock.monotonic,
                    sleep=clock.sleep,
                )
        self.assertEqual(clock.now, 2.5)
        self.assertEqual(clock.sleeps, [1.0, 1.0, 0.5])
        self.assertEqual(opener.call_count, 3)
        self.assertEqual(
            [call.kwargs["timeout"] for call in opener.call_args_list], [2.5, 1.5, 0.5]
        )

    def test_smoke_http_4xx_fails_immediately_without_retry(self):
        for status in (400, 401, 403, 404, 429):
            with self.subTest(status=status):
                clock = SmokeClock()
                failure = self.helper.urllib.error.HTTPError(
                    "http://localhost/health", status, "failed", {}, None
                )
                with patch.object(
                    self.helper.urllib.request, "urlopen", side_effect=failure
                ) as opener:
                    with self.assertRaisesRegex(self.helper.ReleaseError, f"HTTP {status}"):
                        self.helper._http_smoke(
                            ["http://localhost/health"],
                            monotonic=clock.monotonic,
                            sleep=clock.sleep,
                        )
                opener.assert_called_once()
                self.assertEqual(clock.sleeps, [])

    def test_smoke_urls_share_one_deadline_and_reject_late_success(self):
        clock = SmokeClock()
        response = MagicMock()
        response.__enter__.return_value.status = 200

        def open_slow_response(request, *, timeout):
            clock.now += 2
            return response

        with patch.object(
            self.helper.urllib.request, "urlopen", side_effect=open_slow_response
        ) as opener:
            with self.assertRaisesRegex(self.helper.ReleaseError, "arrived after readiness deadline"):
                self.helper._http_smoke(
                    ["http://localhost/first", "http://localhost/second"],
                    readiness_timeout=3,
                    monotonic=clock.monotonic,
                    sleep=clock.sleep,
                )
        self.assertEqual([call.kwargs["timeout"] for call in opener.call_args_list], [3, 1])
        self.assertEqual(clock.sleeps, [])

    def test_smoke_rejects_empty_urls_without_request(self):
        with patch.object(self.helper.urllib.request, "urlopen") as opener:
            with self.assertRaisesRegex(self.helper.ReleaseError, "at least one smoke URL"):
                self.helper._http_smoke([])
        opener.assert_not_called()

    def test_cli_deploy_and_rollback_with_real_http_and_service_command_stub(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"status":"ok"}')

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        url = f"http://127.0.0.1:{server.server_port}/health"
        service_log = self.root / "service-commands.log"
        systemctl = self.root / "systemctl-stub"
        systemctl.write_text(
            "#!/bin/sh\n"
            f"printf '%s\\n' \"$*\" >> {shlex.quote(str(service_log))}\n"
            "if [ \"$1\" = show ]; then printf 'not-found\\n'; fi\n"
            "exit 0\n"
        )
        systemctl.chmod(0o755)
        common = [
            "--candidate-release",
            str(self.candidate),
            "--current-link",
            str(self.current),
            "--releases-root",
            str(self.releases),
            "--runtime-root",
            str(self.config.runtime_root),
            "--auth-database",
            str(self.database),
            "--backup-dir",
            str(self.config.backup_dir),
            "--htpasswd",
            str(self.htpasswd),
            "--change-log",
            str(self.config.change_log),
            "--receipt",
            str(self.config.receipt),
            "--superadmin",
            "operator",
            "--run-user",
            "",
            "--systemctl",
            str(systemctl),
            "--systemd-dir",
            str(self.config.systemd_dir),
            "--libexec-dir",
            str(self.config.libexec_dir),
            "--pre-smoke-url",
            url,
            "--post-smoke-url",
            url,
        ]
        for operation in ("deploy", "rollback"):
            result = subprocess.run(
                [sys.executable, str(HELPER), operation, *common],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("stop dcar-auth.service", service_log.read_text())
        self.assertIn("daemon-reload", service_log.read_text())
        self.assertEqual(self.current.resolve(), self.previous)
        self.assertEqual(self.htpasswd.read_bytes(), self.old_htpasswd)

    def test_restore_zero_one_two_three_preserves_current_and_revokes_credentials(self):
        self.helper._switch_link(self.current, self.candidate)
        backup = self.helper._backup_module()
        zero = backup.create_backup(self.database, self.config.backup_dir, 0)
        self.helper._run_admin(self.config, ["migrate"])
        self.helper._run_admin(
            self.config, ["import-htpasswd", "--source", str(self.htpasswd)]
        )
        self.helper._run_admin(self.config, ["set-role", "operator", "superadmin"])
        three = backup.create_backup(self.database, self.config.backup_dir, 3)
        with sqlite3.connect(self.database) as connection:
            connection.execute("PRAGMA user_version=2")
        two = backup.create_backup(self.database, self.config.backup_dir, 2)
        with sqlite3.connect(self.database) as connection:
            connection.execute("PRAGMA user_version=1")
            connection.execute("DROP TABLE auth_deleted_users")
        one = backup.create_backup(self.database, self.config.backup_dir, 1)
        for version, selected in ((0, zero), (1, one), (2, two), (3, three)):
            with self.subTest(version=version):
                config = self.config._replace(
                    receipt=self.root / "runtime" / f"restore-{version}.json"
                )
                before_bytes = self.database.read_bytes()
                result = self.helper.restore_release(
                    config, Path(selected["path"]), version, service_action=self.service
                )
                self.assertEqual(result["status"], "restored_reconciliation_required")
                self.assertFalse(result["services_started"])
                safety = Path(result["safety_directory"]) / self.database.name
                self.assertEqual(safety.read_bytes(), before_bytes)
                self.assertEqual(backup.database_version(self.database), 3)
                self.assertEqual(self.events[-1], "stop")

    def test_schema_one_rollback_restores_version_one_and_stays_stopped(self):
        self.helper._run_admin(self.config, ["migrate"])
        self.helper._run_admin(
            self.config, ["import-htpasswd", "--source", str(self.htpasswd)]
        )
        with sqlite3.connect(self.database) as connection:
            connection.execute("PRAGMA user_version=1")
            connection.execute("DROP TABLE auth_deleted_users")
        self.deploy()
        result = self.helper.rollback_release(
            self.config, smoke_check=lambda _: None, service_action=self.service
        )
        self.assertEqual(result["status"], "rolled_back_reconciliation_required")
        self.assertEqual(
            self.helper._backup_module().database_version(self.database), 1
        )
        self.assertEqual(self.current.resolve(), self.previous)
        self.assertEqual(self.htpasswd.read_bytes(), self.old_htpasswd)

    def _prepare_schema_two(self):
        self.helper._run_admin(self.config, ["migrate"])
        self.helper._run_admin(self.config, ["import-htpasswd", "--source", str(self.htpasswd)])
        self.helper._run_admin(self.config, ["set-role", "operator", "superadmin"])
        with sqlite3.connect(self.database) as connection:
            connection.execute("""CREATE TABLE auth_users_v2(
                username TEXT PRIMARY KEY COLLATE NOCASE, phone TEXT UNIQUE,
                password_hash TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','disabled')),
                role TEXT NOT NULL DEFAULT 'operator' CHECK(role IN ('superadmin','admin','operator')),
                created_at INTEGER NOT NULL, password_updated_at INTEGER NOT NULL)""")
            connection.execute("INSERT INTO auth_users_v2 SELECT * FROM auth_users")
            connection.execute("DROP TABLE auth_users")
            connection.execute("ALTER TABLE auth_users_v2 RENAME TO auth_users")
            connection.execute("PRAGMA user_version=2")

    def test_schema_two_rollback_preserves_new_state_and_stays_stopped(self):
        self._prepare_schema_two()
        self.deploy()
        store = auth_store.AuthStore(self.database, change_log_path=self.config.change_log)
        store.create_user("pending_user", auth_store.hash_password("Pending 739! safe"), role="new_user")
        store.create_session("operator", 3600)
        before = self.database.read_bytes()
        result = self.helper.rollback_release(
            self.config, smoke_check=lambda _: self.fail("old schema must stay stopped"),
            service_action=self.service,
        )
        self.assertEqual(result["status"], "rolled_back_reconciliation_required")
        self.assertFalse(result["services_started"])
        safety = Path(result["rollback_restore"]["safety_directory"]) / self.database.name
        self.assertEqual(safety.read_bytes(), before)
        self.assertEqual(self.helper._backup_module().database_version(self.database), 2)
        with sqlite3.connect(self.database) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM auth_sessions").fetchone()[0], 0)
        self.assertEqual(self.current.resolve(), self.previous)
        self.assertEqual(self.htpasswd.read_bytes(), self.old_htpasswd)

    def test_failed_schema_two_migration_never_restarts_old_code_on_schema_three(self):
        self._prepare_schema_two()

        def checkpoint(stage):
            if stage == "after_migration":
                raise RuntimeError("fail after schema upgrade")

        with self.assertRaises(self.helper.ReleaseError):
            self.deploy(checkpoint=checkpoint)
        receipt = json.loads(self.config.receipt.read_text())
        self.assertFalse(receipt["services_started"])
        self.assertEqual(receipt["manual_recovery_required"], "schema2_to3")
        self.assertNotIn("start", self.events)
        self.assertEqual(self.current.resolve(), self.previous)


if __name__ == "__main__":
    unittest.main()
