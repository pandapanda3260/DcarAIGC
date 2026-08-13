from __future__ import annotations

import argparse
import getpass
import os
import re
from pathlib import Path

from passlib.hash import sha512_crypt


USERNAME_PATTERN = re.compile(r"^[^:\s]{1,128}$")


def main() -> int:
    parser = argparse.ArgumentParser(description="Create the local Dcar login account")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--minimum-password-length", type=int, default=8)
    arguments = parser.parse_args()
    if arguments.minimum_password_length < 1:
        parser.error("密码最小长度必须是正整数")

    username = input("请输入本地登录账号：").strip()
    if not USERNAME_PATTERN.fullmatch(username):
        parser.error("登录账号不能为空、包含空白/冒号或超过 128 个字符")
    password = getpass.getpass("请输入本地登录密码：")
    confirmation = getpass.getpass("请再次输入密码：")
    if password != confirmation:
        parser.error("两次输入的密码不一致")
    if len(password) < arguments.minimum_password_length:
        parser.error(f"密码至少需要 {arguments.minimum_password_length} 个字符")

    password_hash = sha512_crypt.using(rounds=100_000).hash(password)
    arguments.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    open_mode = os.O_WRONLY | os.O_CREAT
    open_mode |= os.O_TRUNC if arguments.replace else os.O_EXCL
    file_descriptor = os.open(arguments.output, open_mode, 0o600)
    with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
        handle.write(f"{username}:{password_hash}\n")
    arguments.output.chmod(0o600)
    print(f"本地登录账号已创建：{username}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
