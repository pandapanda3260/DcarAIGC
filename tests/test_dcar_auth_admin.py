from __future__ import annotations

import io
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import AsyncMock, patch

from passlib.hash import sha512_crypt


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT / "src" / "dcar_eval"))

from dcar_auth import admin as auth_admin  # noqa: E402
from dcar_auth import store as auth_store  # noqa: E402
from dcar_auth.sms import SmsOutcome  # noqa: E402


HASH = sha512_crypt.using(rounds=5000).hash("correct-password")
PHONE = "13800138000"


class AuthAdminCliTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.db = self.root / "sessions.sqlite3"
        self.htpasswd = self.root / "users.htpasswd"
        self.htpasswd.write_text(f"operator:{HASH}\nSecond.Ops:{HASH}\n", encoding="utf-8")

    def _run(self, *arguments: str) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            try:
                code = auth_admin.main(["--db", str(self.db), *arguments])
            except SystemExit as exc:
                code = int(exc.code) if isinstance(exc.code, int) else 1
                if isinstance(exc.code, str):
                    err.write(exc.code)
        return code, out.getvalue(), err.getvalue()

    def _store(self) -> auth_store.AuthStore:
        store = auth_store.AuthStore(self.db, pepper=b"p" * 32)
        store.healthcheck()
        return store

    def _ordinary_commands(self) -> tuple[tuple[str, ...], ...]:
        return (
            ("list",),
            ("changes",),
            ("import-htpasswd", "--source", str(self.htpasswd)),
            ("export-htpasswd", "--output", str(self.root / "export.htpasswd")),
            ("allow-phone", PHONE),
            ("set-phone", "operator", PHONE),
            ("set-password", "operator"),
            ("disable-user", "operator"),
            ("enable-user", "operator"),
            ("revoke-sessions",),
            ("revoke-challenges",),
            ("set-role", "operator", "superadmin"),
            ("delete-user", "operator"),
        )

    def test_ordinary_commands_do_not_create_missing_database_or_parent(self) -> None:
        for nested in (False, True):
            self.db = self.root / "missing" / "sessions.sqlite3" if nested else self.root / "sessions.sqlite3"
            for command in self._ordinary_commands():
                with self.subTest(nested=nested, command=command[0]):
                    code, out, err = self._run(*command)
                    self.assertEqual(code, 2)
                    self.assertEqual(out, "")
                    self.assertIn("migrate", err)
                    self.assertFalse(self.db.exists())
                    self.assertFalse((self.root / "missing").exists())
        self.assertEqual(sorted(path.name for path in self.root.iterdir()), ["users.htpasswd"])

    def test_ordinary_commands_do_not_migrate_older_schemas(self) -> None:
        for version in (0, 1, 2):
            self.db = self.root / f"schema-{version}.sqlite3"
            if version in (1, 2):
                auth_store.AuthStore(self.db).initialize()
            with sqlite3.connect(self.db) as connection:
                if version == 0:
                    connection.execute(
                        "CREATE TABLE auth_sessions(token_sha256 TEXT PRIMARY KEY, username TEXT NOT NULL, "
                        "created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL)"
                    )
                    connection.execute("INSERT INTO auth_sessions VALUES('legacy', 'operator', 1, 9999999999)")
                elif version == 1:
                    connection.execute("DROP TABLE auth_deleted_users")
                    connection.execute("ALTER TABLE auth_users DROP COLUMN role")
                connection.execute(f"PRAGMA user_version={version}")
            before = self.db.read_bytes()
            change_log = self.root / "auth-changes.log"
            previous_log = change_log.read_bytes() if change_log.exists() else None
            for command in self._ordinary_commands():
                with self.subTest(version=version, command=command[0]):
                    code, out, err = self._run(*command)
                    self.assertEqual(code, 2)
                    self.assertEqual(out, "")
                    self.assertIn("migrate", err)
                    self.assertEqual(self.db.read_bytes(), before)
                    self.assertEqual(change_log.read_bytes() if change_log.exists() else None, previous_log)
            code, out, err = self._run("migrate")
            self.assertEqual((code, out, err), (0, f"user_version {version} -> 3\n", ""))
            self._store().healthcheck()
            if version == 0:
                with sqlite3.connect(self.db) as connection:
                    self.assertEqual(connection.execute("SELECT COUNT(*) FROM auth_sessions").fetchone()[0], 1)
            self.assertEqual(self._run("import-htpasswd", "--source", str(self.htpasswd), "--bootstrap-superadmin")[0], 0)

    def test_read_commands_do_not_reinitialize_current_schema_or_adjust_permissions(self) -> None:
        self.assertEqual(self._run("migrate")[0], 0)
        self.db.chmod(0o640)
        self.root.chmod(0o750)
        before = self.db.read_bytes()
        with patch.object(auth_store.AuthStore, "initialize", side_effect=AssertionError("implicit migration")):
            for command in (("list",), ("changes",)):
                with self.subTest(command=command[0]):
                    self.assertEqual(self._run(*command)[0], 0)
                    self.assertEqual(self.db.read_bytes(), before)
            self.assertEqual(self._run("allow-phone", PHONE)[0], 0)
        self.assertEqual(self.db.stat().st_mode & 0o777, 0o640)
        self.assertEqual(self.root.stat().st_mode & 0o777, 0o750)

    def test_ordinary_commands_do_not_repair_missing_tables_or_wal(self) -> None:
        self.assertEqual(self._run("migrate")[0], 0)
        for damage in ("missing_table", "wal"):
            with sqlite3.connect(self.db) as connection:
                if damage == "missing_table":
                    connection.execute("DROP TABLE auth_deleted_users")
                else:
                    connection.execute("PRAGMA journal_mode=WAL")
            before = self.db.read_bytes()
            for command in (("list",), ("allow-phone", PHONE)):
                with self.subTest(damage=damage, command=command[0]):
                    code, out, err = self._run(*command)
                    self.assertEqual(code, 2)
                    self.assertEqual(out, "")
                    self.assertIn("migrate", err)
                    self.assertEqual(self.db.read_bytes(), before)
            self.assertEqual(self._run("migrate")[0], 0)
            self.assertEqual(self._run("list")[0], 0)

    def test_healthcheck_does_not_recreate_database_removed_after_file_check(self) -> None:
        self.assertEqual(self._run("migrate")[0], 0)
        original = auth_store.AuthStore.healthcheck

        def disappear_before_connect(store: auth_store.AuthStore) -> None:
            store.path.unlink()
            original(store)

        with patch.object(auth_store.AuthStore, "healthcheck", disappear_before_connect):
            code, out, err = self._run("list")
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertIn("migrate", err)
        self.assertFalse(self.db.exists())

    def test_sms_test_does_not_open_or_initialize_any_database(self) -> None:
        self.db = self.root / "missing" / "sessions.sqlite3"
        with patch.object(auth_admin, "load_sms_credentials") as credentials, patch.object(
            auth_admin, "TencentSmsSender"
        ) as sender, patch.object(
            auth_admin, "_open_store", side_effect=AssertionError("opened database")
        ), patch.object(
            auth_store.AuthStore, "initialize", side_effect=AssertionError("initialized database")
        ):
            sender.return_value.send = AsyncMock(return_value=SmsOutcome("sent", "Ok"))
            sender.return_value.aclose = AsyncMock()
            code, out, err = self._run("sms-test", PHONE, "--sms-credentials-file", str(self.root / "credentials"))
            self.assertEqual((code, out, err), (0, "provider_code=sent Ok\n", ""))
            credentials.assert_called_once()
            sender.return_value.send.assert_awaited_once()
            sender.return_value.aclose.assert_awaited_once()
        self.assertFalse(self.db.parent.exists())

    def test_import_export_and_listing(self) -> None:
        self.assertEqual(self._run("migrate")[0], 0)
        code, out, _ = self._run("import-htpasswd", "--source", str(self.htpasswd))
        self.assertEqual(code, 0)
        self.assertIn("已导入 2 个账号", out)
        self.assertNotIn("operator", out)
        code, _, err = self._run("import-htpasswd", "--source", str(self.htpasswd))
        self.assertEqual(code, 2)
        self.assertIn("导入失败", err)
        code, out, _ = self._run("list")
        self.assertEqual(code, 0)
        self.assertIn("operator", out)
        self.assertIn("Second.Ops", out)
        self.assertIn("active", out)
        exported = self.root / "export" / "users.htpasswd"
        code, out, _ = self._run("export-htpasswd", "--output", str(exported))
        self.assertEqual(code, 0)
        self.assertIn("已导出 2 个账号", out)
        self.assertEqual(exported.read_text(encoding="utf-8"), self.htpasswd.read_text(encoding="utf-8"))
        self.assertEqual(oct(exported.stat().st_mode & 0o777), oct(0o640))

    def test_rejected_import_writes_nothing(self) -> None:
        self.assertEqual(self._run("migrate")[0], 0)
        self.htpasswd.write_text(f"operator:{HASH}\nOPERATOR:{HASH}\n", encoding="utf-8")
        code, _, err = self._run("import-htpasswd", "--source", str(self.htpasswd))
        self.assertEqual(code, 2)
        self.assertIn("duplicate username", err)
        self.assertEqual(self._store().user_count(), 0)

    def test_bootstrap_import_uses_first_valid_account_and_cannot_promote_existing_users(self) -> None:
        self.assertEqual(self._run("migrate")[0], 0)
        self.htpasswd.write_text(
            f"  #retired:{HASH}\noperator:{HASH}\nSecond.Ops:{HASH}\n",
            encoding="utf-8",
        )
        command = ("import-htpasswd", "--source", str(self.htpasswd), "--bootstrap-superadmin")
        self.assertEqual(self._run(*command)[0], 0)
        store = self._store()
        self.assertEqual(store.get_user("operator").role, "superadmin")
        self.assertEqual(store.get_user("Second.Ops").role, "operator")
        self.assertEqual(store.count_superadmins(), 1)
        last_change = store.read_changes()[-1]
        self.assertEqual((last_change["action"], last_change["target"], last_change["after"]),
                         ("user.set_role", "operator", {"role": "superadmin"}))
        self.assertEqual(self._run(*command)[0], 2)
        self.assertEqual(store.get_user("Second.Ops").role, "operator")

    def test_bootstrap_import_rolls_back_on_audit_failure_and_can_retry(self) -> None:
        self.assertEqual(self._run("migrate")[0], 0)
        command = ("import-htpasswd", "--source", str(self.htpasswd), "--bootstrap-superadmin")
        original = auth_store.AuthStore._record_change_intent
        def fail_role_intent(store, action, *args, **kwargs):
            if action == "user.set_role":
                raise auth_store.ChangeLogError("unavailable")
            return original(store, action, *args, **kwargs)
        with patch.object(auth_store.AuthStore, "_record_change_intent", fail_role_intent):
            self.assertEqual(self._run(*command)[0], 3)
        store = self._store()
        self.assertEqual(store.user_count(), 0)
        self.assertEqual(store.count_superadmins(), 0)
        self.assertEqual(self._run(*command)[0], 0)
        self.assertEqual(store.user_count(), 2)
        self.assertEqual(store.count_superadmins(), 1)

    def test_import_skips_disabled_comments_and_rejects_malformed_hash_atomically(self) -> None:
        self.assertEqual(self._run("migrate")[0], 0)
        self.htpasswd.write_text(f"#retired:{HASH}\noperator:{HASH}\n", encoding="utf-8")
        code, out, _ = self._run("import-htpasswd", "--source", str(self.htpasswd))
        self.assertEqual(code, 0)
        self.assertIn("已导入 1 个账号", out)
        self.assertIsNone(self._store().get_user("#retired"))
        other = self.root / "other.sqlite3"
        self.htpasswd.write_text(f"operator:{HASH}\nbroken:$6$not-a-valid-hash\n", encoding="utf-8")
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            self.assertEqual(auth_admin.main(["--db", str(other), "migrate"]), 0)
            code = auth_admin.main(["--db", str(other), "import-htpasswd", "--source", str(self.htpasswd)])
        self.assertEqual(code, 2)
        store = auth_store.AuthStore(other)
        self.assertEqual(store.user_count(), 0)

    def test_empty_export_requires_explicit_override_and_preserves_old_file(self) -> None:
        self.assertEqual(self._run("migrate")[0], 0)
        target = self.root / "rollback.htpasswd"
        original = self.htpasswd.read_text(encoding="utf-8")
        target.write_text(original, encoding="utf-8")
        code, _, err = self._run("export-htpasswd", "--output", str(target))
        self.assertEqual(code, 2)
        self.assertIn("no active authorized accounts", err)
        self.assertEqual(target.read_text(encoding="utf-8"), original)
        code, out, _ = self._run("export-htpasswd", "--output", str(target), "--allow-empty")
        self.assertEqual(code, 0)
        self.assertIn("已导出 0 个账号", out)
        self.assertEqual(target.read_text(encoding="utf-8"), "")

    def test_changes_returns_nonzero_for_unconfirmed_or_corrupt_audit(self) -> None:
        self.assertEqual(self._run("migrate")[0], 0)
        self._run("import-htpasswd", "--source", str(self.htpasswd))
        store = self._store()
        with patch.object(store, "_record_change_commit"):
            store.set_role("operator", "admin")
        code, out, err = self._run("changes", "--since", "2999-01-01T00:00:00+00:00")
        self.assertEqual(code, 3)
        pending = json.loads(out.strip())
        self.assertEqual(pending["state"], "conservative")
        self.assertEqual(pending["after"], {"role": "admin"})
        self.assertIn("保守撤权", err)
        with store.change_log_path.open("a", encoding="utf-8") as handle:
            handle.write("{broken}\n")
        code, out, err = self._run("changes")
        self.assertEqual(code, 3)
        self.assertEqual(out, "")
        self.assertIn("malformed JSON", err)
        code, _, err = self._run("set-role", "operator", "operator")
        self.assertEqual(code, 3)
        self.assertEqual(store.get_user("operator").role, "admin")

    def test_import_rolls_back_if_intent_cannot_be_durable(self) -> None:
        self.assertEqual(self._run("migrate")[0], 0)
        with patch.object(auth_store.AuthStore, "_append_change_event", side_effect=auth_store.ChangeLogError("fsync failed")):
            code, out, err = self._run("import-htpasswd", "--source", str(self.htpasswd))
        self.assertEqual(code, 3)
        self.assertEqual(out, "")
        self.assertIn("fsync failed", err)
        self.assertEqual(self._store().user_count(), 0)

    def test_phone_allowlist_and_binding(self) -> None:
        self.assertEqual(self._run("migrate")[0], 0)
        self._run("import-htpasswd", "--source", str(self.htpasswd))
        code, out, _ = self._run("allow-phone", PHONE, "--note", "Mark")
        self.assertEqual(code, 0)
        self.assertTrue(self._store().phone_allowed(PHONE))
        code, out, _ = self._run("allow-phone", PHONE, "--remove")
        self.assertEqual(code, 0)
        self.assertFalse(self._store().phone_allowed(PHONE))
        code, _, err = self._run("allow-phone", "12345")
        self.assertEqual(code, 1)
        self.assertIn("手机号格式不正确", err)
        code, out, _ = self._run("set-phone", "operator", PHONE)
        self.assertEqual(code, 0)
        code, _, err = self._run("set-phone", "Second.Ops", PHONE)
        self.assertEqual(code, 2)
        self.assertIn("已绑定其它账号", err)
        code, _, err = self._run("set-phone", "nobody", PHONE)
        self.assertEqual(code, 2)
        user = self._store().get_user("operator")
        assert user is not None
        self.assertEqual(user.phone, PHONE)

    def test_set_password_disable_enable_and_revocation(self) -> None:
        self.assertEqual(self._run("migrate")[0], 0)
        self._run("import-htpasswd", "--source", str(self.htpasswd))
        store = self._store()
        token = store.create_session("operator", 3600)
        with patch.object(auth_admin.getpass, "getpass", side_effect=["Fresh-passphrase-1", "Fresh-passphrase-1"]):
            code, out, _ = self._run("set-password", "operator")
        self.assertEqual(code, 0)
        self.assertIn("全部会话已失效", out)
        self.assertIsNone(store.resolve_session(token))
        user = store.get_user("operator")
        assert user is not None
        self.assertTrue(sha512_crypt.verify("Fresh-passphrase-1", user.password_hash))
        self.assertFalse(sha512_crypt.verify("correct-password", user.password_hash))
        with patch.object(auth_admin.getpass, "getpass", side_effect=["password", "password"]):
            code, _, err = self._run("set-password", "operator")
        self.assertEqual(code, 1)
        self.assertIn("过于常见", err)
        with patch.object(auth_admin.getpass, "getpass", side_effect=["Fresh-passphrase-2", "different"]):
            code, _, err = self._run("set-password", "operator")
        self.assertEqual(code, 1)
        self.assertIn("不一致", err)

        token = store.create_session("operator", 3600)
        code, out, _ = self._run("disable-user", "operator")
        self.assertEqual(code, 0)
        self.assertIsNone(store.resolve_session(token))
        exported = self.root / "rollback.htpasswd"
        code, out, _ = self._run("export-htpasswd", "--output", str(exported))
        self.assertIn("已导出 1 个账号", out)
        self.assertNotIn("operator:", exported.read_text(encoding="utf-8"))
        code, _, _ = self._run("enable-user", "operator")
        self.assertEqual(code, 0)
        code, out, _ = self._run("export-htpasswd", "--output", str(exported))
        self.assertIn("已导出 2 个账号", out)
        exported_hash = [line for line in exported.read_text(encoding="utf-8").splitlines() if line.startswith("operator:")][0].split(":", 1)[1]
        self.assertTrue(sha512_crypt.verify("Fresh-passphrase-1", exported_hash))

        store.create_session("operator", 3600)
        store.create_session("Second.Ops", 3600)
        code, out, _ = self._run("revoke-sessions", "--username", "operator")
        self.assertIn("已撤销 1 个会话", out)
        code, out, _ = self._run("revoke-sessions")
        self.assertIn("已撤销 1 个会话", out)
        store.allow_phone(PHONE)
        store.set_phone("operator", PHONE)
        challenge_id, _ = store.reserve_send("login", PHONE, "203.0.113.1", "123456")
        store.finish_send(challenge_id, "sent", "OK")
        code, out, _ = self._run("revoke-challenges")
        self.assertIn("已作废 1 条验证码 / 票据记录", out)
        self.assertEqual(store.counts()["auth_challenges"], 1)

    def test_missing_db_argument_is_rejected(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                with self.assertRaises(SystemExit) as caught:
                    auth_admin.main(["list"])
        self.assertIn("DCAR_AUTH_SESSION_DB", str(caught.exception.code))

    def test_cli_can_restrict_existing_account_to_new_user(self) -> None:
        self.assertEqual(self._run("migrate")[0], 0)
        self.assertEqual(self._run("import-htpasswd", "--source", str(self.htpasswd))[0], 0)
        store = self._store()
        token = store.create_session("operator", 3600)
        code, out, err = self._run("set-role", "operator", "new_user")
        self.assertEqual((code, out, err), (0, "operator new_user\n", ""))
        self.assertEqual(store.resolve_principal(token).role, "new_user")
        exported = self.root / "rollback.htpasswd"
        self.assertEqual(self._run("export-htpasswd", "--output", str(exported))[0], 0)
        self.assertNotIn("operator:", exported.read_text())
        self.assertEqual(store.read_changes()[-1]["after"], {"role": "new_user"})

    def test_migrate_roles_deletion_and_change_log(self) -> None:
        code, out, _ = self._run("migrate")
        self.assertEqual((code, out), (0, "user_version 0 -> 3\n"))
        code, out, _ = self._run("migrate")
        self.assertEqual((code, out), (0, "user_version 3 -> 3\n"))
        self.assertEqual(self._run("import-htpasswd", "--source", str(self.htpasswd))[0], 0)
        code, out, err = self._run("list")
        self.assertEqual(code, 0)
        self.assertIn("operator", out)
        self.assertIn("尚无超级管理员", err)
        # Imported accounts are operators; the first superadmin is CLI-only,
        # and the last one can neither be demoted nor deleted.
        code, out, _ = self._run("set-role", "operator", "superadmin")
        self.assertEqual((code, out), (0, "operator superadmin\n"))
        self.assertEqual(self._run("list")[2], "")
        code, _, err = self._run("set-role", "operator", "admin")
        self.assertEqual(code, 2)
        self.assertIn("至少保留一个超级管理员", err)
        code, _, err = self._run("delete-user", "operator")
        self.assertEqual(code, 2)
        self.assertEqual(self._run("set-role", "ghost", "admin")[0], 2)
        self.assertEqual(self._run("set-role", "operator", "root")[0], 2)  # argparse choices

        store = self._store()
        store.allow_phone(PHONE)
        store.set_phone("Second.Ops", PHONE)
        code, out, _ = self._run("delete-user", "second.ops")
        self.assertEqual(code, 0)
        self.assertIn("已删除 Second.Ops", out)
        self.assertIsNone(store.get_user("Second.Ops"))
        self.assertTrue(store.username_reserved("SECOND.OPS"))
        self.assertFalse(store.phone_allowed(PHONE))
        self.assertEqual(self._run("delete-user", "second.ops")[0], 2)
        code, _, err = self._run("import-htpasswd", "--source", str(self.htpasswd))
        self.assertEqual(code, 2)

        code, out, _ = self._run("changes")
        self.assertEqual(code, 0)
        actions = [json.loads(line)["action"] for line in out.splitlines()]
        self.assertEqual(
            actions,
            ["user.import", "user.set_role", "phone.allow", "user.set_phone", "user.delete"],
        )
        self.assertTrue(all(json.loads(line)["actor"].startswith("cli:") for line in out.splitlines()[:2]))
        self.assertEqual(self._run("changes", "--since", "2999-01-01T00:00:00+00:00"), (0, "", ""))
        code, _, err = self._run("changes", "--since", "yesterday")
        self.assertEqual(code, 1)
        self.assertIn("ISO-8601", err)
        elsewhere = self.root / "elsewhere.log"
        self.assertEqual(
            auth_admin.main(["--db", str(self.db), "--change-log", str(elsewhere), "allow-phone", "13900139000"]),
            0,
        )
        self.assertIn("phone.allow", elsewhere.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
