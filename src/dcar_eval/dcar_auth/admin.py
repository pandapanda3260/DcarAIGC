"""Offline account administration for the Dcar auth gateway.

Usage: ``python -m dcar_auth.admin --db PATH [--change-log PATH] <subcommand> ...``

The tool talks to the SQLite account store directly through ``AuthStore`` and
therefore shares the gateway's transaction contract; it is safe to run while the
gateway is serving.  It never imports ``dcar_auth.gateway``.  Every mutating
subcommand is recorded in the security change log as ``cli:<os user>``.
Only the explicit ``migrate`` subcommand creates or upgrades the account store;
ordinary commands require an existing, healthy current-version database.
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dcar_auth import store as auth_store
from dcar_auth.sms import TencentSmsSender, load_sms_credentials


def _default_db() -> Optional[str]:
    return os.environ.get("DCAR_AUTH_SESSION_DB") or None


def _default_change_log() -> Optional[str]:
    return os.environ.get("DCAR_AUTH_CHANGE_LOG") or None


def _actor() -> str:
    try:
        user = getpass.getuser()
    except (KeyError, OSError):
        user = "unknown"
    return f"cli:{user}"


def _open_store(arguments: argparse.Namespace) -> auth_store.AuthStore:
    if not arguments.db:
        raise SystemExit("--db 或 DCAR_AUTH_SESSION_DB 必须指定账号库路径")
    store = auth_store.AuthStore(
        Path(arguments.db),
        change_log_path=(Path(arguments.change_log) if arguments.change_log else None),
    )
    # Never let a routine listing or account change bootstrap/migrate the live
    # database before the release helper has stopped services and backed it up.
    # healthcheck uses a SQLite connection, so reject missing files first.
    if not store.path.is_file():
        raise RuntimeError("账号库不存在；请先通过发布流程或显式 migrate 创建 / 迁移账号库")
    try:
        store.healthcheck()
    except (RuntimeError, OSError, sqlite3.Error) as exc:
        raise RuntimeError(
            f"账号库未就绪：{exc}；请先通过发布流程或显式 migrate 完成迁移"
        ) from exc
    return store


def _format_time(value: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(value))


def _read_new_password(username: str, phone: Optional[str]) -> str:
    password = getpass.getpass("请输入新密码：")
    confirmation = getpass.getpass("请再次输入新密码：")
    if password != confirmation:
        raise SystemExit("两次输入的密码不一致")
    problem = auth_store.password_problem(
        password, username=username, phone=phone or ""
    )
    if problem == "invalid_password":
        raise SystemExit("密码长度需为 8–64 位")
    if problem is not None:
        raise SystemExit("密码过于常见，请换一个")
    return password


def cmd_import_htpasswd(arguments: argparse.Namespace) -> int:
    store = _open_store(arguments)
    try:
        count = store.import_htpasswd(
            Path(arguments.source),
            actor=_actor(),
            bootstrap_superadmin=arguments.bootstrap_superadmin,
        )
    except auth_store.HtpasswdImportError as exc:
        print(f"导入失败：{exc}", file=sys.stderr)
        return 2
    print(f"已导入 {count} 个账号")
    return 0


def cmd_export_htpasswd(arguments: argparse.Namespace) -> int:
    store = _open_store(arguments)
    try:
        count = store.export_htpasswd(
            Path(arguments.output), allow_empty=bool(arguments.allow_empty)
        )
    except (auth_store.HtpasswdImportError, auth_store.HtpasswdExportError) as exc:
        print(f"导出失败：{exc}", file=sys.stderr)
        return 2
    print(f"已导出 {count} 个账号")
    return 0


def cmd_allow_phone(arguments: argparse.Namespace) -> int:
    if not auth_store.valid_phone(arguments.phone):
        raise SystemExit("手机号格式不正确")
    store = _open_store(arguments)
    if arguments.remove:
        removed = store.remove_allowed_phone(arguments.phone, actor=_actor())
        print("已移出准入名单" if removed else "该手机号不在准入名单中")
        return 0
    store.allow_phone(arguments.phone, arguments.note or "", actor=_actor())
    print("已加入准入名单")
    return 0


def cmd_set_phone(arguments: argparse.Namespace) -> int:
    if not auth_store.valid_phone(arguments.phone):
        raise SystemExit("手机号格式不正确")
    store = _open_store(arguments)
    try:
        store.set_phone(arguments.username, arguments.phone, actor=_actor())
    except auth_store.UserNotFound:
        print("账号不存在", file=sys.stderr)
        return 2
    except auth_store.PhoneConflict:
        print("该手机号已绑定其它账号", file=sys.stderr)
        return 2
    print("已更新手机号")
    return 0


def cmd_set_password(arguments: argparse.Namespace) -> int:
    store = _open_store(arguments)
    user = store.get_user(arguments.username)
    if user is None:
        print("账号不存在", file=sys.stderr)
        return 2
    password = _read_new_password(user.username, user.phone)
    store.set_password(user.username, auth_store.hash_password(password), actor=_actor())
    print("已更新密码，该账号全部会话已失效")
    return 0


def cmd_set_status(arguments: argparse.Namespace, status: str) -> int:
    store = _open_store(arguments)
    try:
        store.set_status(arguments.username, status, actor=_actor())
    except auth_store.UserNotFound:
        print("账号不存在", file=sys.stderr)
        return 2
    print("已停用" if status == "disabled" else "已启用")
    return 0


def cmd_revoke_sessions(arguments: argparse.Namespace) -> int:
    store = _open_store(arguments)
    count = store.revoke_sessions(arguments.username, actor=_actor())
    print(f"已撤销 {count} 个会话")
    return 0


def cmd_revoke_challenges(arguments: argparse.Namespace) -> int:
    store = _open_store(arguments)
    count = store.revoke_challenges(arguments.username, actor=_actor())
    print(f"已作废 {count} 条验证码 / 票据记录")
    return 0


def cmd_list(arguments: argparse.Namespace) -> int:
    store = _open_store(arguments)
    users = store.list_users()
    if not users:
        print("（无账号）")
        return 0
    print(
        f"{'用户名':<24} {'手机号':<13} {'权限':<11} {'状态':<8} {'创建时间':<16} "
        f"{'改密时间':<16} 会话"
    )
    for user in users:
        print(
            f"{user.username:<24} {user.phone or '-':<13} {user.role:<11} {user.status:<8} "
            f"{_format_time(user.created_at):<16} "
            f"{_format_time(user.password_updated_at):<16} {user.active_sessions}"
        )
    if store.count_superadmins() == 0:
        print(
            "提示：尚无超级管理员；用 set-role <用户名> superadmin 指定一个，"
            "否则用户权限页无人可进。",
            file=sys.stderr,
        )
    return 0


def cmd_migrate(arguments: argparse.Namespace) -> int:
    if not arguments.db:
        raise SystemExit("--db 或 DCAR_AUTH_SESSION_DB 必须指定账号库路径")
    store = auth_store.AuthStore(
        Path(arguments.db),
        change_log_path=(Path(arguments.change_log) if arguments.change_log else None),
    )
    try:
        before, after = store.initialize()
    except auth_store.SchemaVersionError as exc:
        print(f"迁移失败：{exc}", file=sys.stderr)
        return 2
    store.healthcheck()
    print(f"user_version {before} -> {after}")
    return 0


def cmd_set_role(arguments: argparse.Namespace) -> int:
    store = _open_store(arguments)
    try:
        user = store.set_role(arguments.username, arguments.role, actor=_actor())
    except auth_store.UserNotFound:
        print("账号不存在", file=sys.stderr)
        return 2
    except auth_store.LastSuperadmin:
        print("至少保留一个超级管理员", file=sys.stderr)
        return 2
    print(f"{user.username} {user.role}")
    return 0


def cmd_delete_user(arguments: argparse.Namespace) -> int:
    store = _open_store(arguments)
    try:
        user = store.delete_user_cli(arguments.username, actor=_actor())
    except auth_store.UserNotFound:
        print("账号不存在", file=sys.stderr)
        return 2
    except auth_store.LastSuperadmin:
        print("至少保留一个超级管理员", file=sys.stderr)
        return 2
    print(f"已删除 {user.username}（用户名保留为墓碑，手机号退出准入名单，会话与验证码已失效）")
    return 0


def cmd_changes(arguments: argparse.Namespace) -> int:
    store = _open_store(arguments)
    since: Optional[datetime] = None
    if arguments.since:
        try:
            since = datetime.fromisoformat(arguments.since)
        except ValueError:
            raise SystemExit("--since 需要 ISO-8601 时间，例如 2026-09-05T00:00:00+08:00")
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
    entries = store.read_changes(since)
    for entry in entries:
        print(json.dumps(entry, ensure_ascii=False))
    conservative = [
        entry for entry in entries if entry.get("state") != "committed"
    ]
    if conservative:
        print(
            f"检测到 {len(conservative)} 条未确认安全变更 intent；"
            "恢复时必须保守撤权并人工复核",
            file=sys.stderr,
        )
        return 3
    return 0


def cmd_sms_test(arguments: argparse.Namespace) -> int:
    if not auth_store.valid_phone(arguments.phone):
        raise SystemExit("手机号格式不正确")
    credentials = load_sms_credentials(Path(arguments.sms_credentials_file))
    sender = TencentSmsSender(credentials)

    async def run() -> str:
        try:
            outcome = await sender.send(arguments.phone, auth_store.generate_code())
        finally:
            await sender.aclose()
        return f"{outcome.status} {outcome.provider_code}"

    result = asyncio.run(run())
    print(f"provider_code={result}")
    return 0 if result.startswith("sent") else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m dcar_auth.admin", description=__doc__
    )
    parser.add_argument("--db", default=_default_db(), help="账号库路径")
    parser.add_argument(
        "--change-log",
        default=_default_change_log(),
        help="安全变更日志路径（默认账号库旁的 auth-changes.log）",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    sub = commands.add_parser("migrate", help="建表 / 补列并把 user_version 升到当前版本（幂等）")
    sub.set_defaults(handler=cmd_migrate)

    sub = commands.add_parser("import-htpasswd", help="先 migrate，再从 htpasswd 一次性导入账号（默认全部为运营人员）")
    sub.add_argument("--source", required=True)
    sub.add_argument(
        "--bootstrap-superadmin", action="store_true",
        help="仅空库首次导入：同一事务内把首个有效账号设为超级管理员",
    )
    sub.set_defaults(handler=cmd_import_htpasswd)

    sub = commands.add_parser("export-htpasswd", help="导出 active 账号为 htpasswd")
    sub.add_argument("--output", required=True)
    sub.add_argument(
        "--allow-empty",
        action="store_true",
        help="显式允许生成空文件（默认拒绝，以免覆盖可回滚账号源）",
    )
    sub.set_defaults(handler=cmd_export_htpasswd)

    sub = commands.add_parser("allow-phone", help="维护运营人员准入名单（页面注册门控）")
    sub.add_argument("phone")
    sub.add_argument("--note", default="")
    sub.add_argument("--remove", action="store_true")
    sub.set_defaults(handler=cmd_allow_phone)

    sub = commands.add_parser("set-phone", help="绑定或改绑手机号（旧号同时退出准入名单）")
    sub.add_argument("username")
    sub.add_argument("phone")
    sub.set_defaults(handler=cmd_set_phone)

    sub = commands.add_parser("set-password", help="重置密码（交互输入）")
    sub.add_argument("username")
    sub.set_defaults(handler=cmd_set_password)

    sub = commands.add_parser("disable-user", help="停用账号并撤销会话与验证码")
    sub.add_argument("username")
    sub.set_defaults(handler=lambda arguments: cmd_set_status(arguments, "disabled"))

    sub = commands.add_parser("enable-user", help="重新启用账号")
    sub.add_argument("username")
    sub.set_defaults(handler=lambda arguments: cmd_set_status(arguments, "active"))

    sub = commands.add_parser("revoke-sessions", help="撤销会话（默认全部）")
    sub.add_argument("--username")
    sub.set_defaults(handler=cmd_revoke_sessions)

    sub = commands.add_parser("revoke-challenges", help="作废验证码与找回票据（默认全部；记录保留供限频计数）")
    sub.add_argument("--username")
    sub.set_defaults(handler=cmd_revoke_challenges)

    sub = commands.add_parser("set-role", help="设置权限等级（也用于恢复首个超级管理员）")
    sub.add_argument("username")
    sub.add_argument("role", choices=list(auth_store.ROLES))
    sub.set_defaults(handler=cmd_set_role)

    sub = commands.add_parser("delete-user", help="删除账号（墓碑保留用户名，手机号退出准入名单）")
    sub.add_argument("username")
    sub.set_defaults(handler=cmd_delete_user)

    sub = commands.add_parser("list", help="列出账号")
    sub.set_defaults(handler=cmd_list)

    sub = commands.add_parser("changes", help="按 JSON 行输出安全变更日志（恢复备份后对账用）")
    sub.add_argument("--since", help="ISO-8601 起始时间")
    sub.set_defaults(handler=cmd_changes)

    sub = commands.add_parser("sms-test", help="真实发送一条测试验证码")
    sub.add_argument("phone")
    sub.add_argument("--sms-credentials-file", required=True)
    sub.set_defaults(handler=cmd_sms_test)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        return int(arguments.handler(arguments))
    except auth_store.ChangeLogError as exc:
        print(f"安全变更日志不可用：{exc}", file=sys.stderr)
        return 3
    except RuntimeError as exc:
        print(f"认证存储不可用：{exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
