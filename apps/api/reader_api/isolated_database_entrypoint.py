from __future__ import annotations

import os
import re
import sys


_URL_UNRESERVED = re.compile(r"\A[A-Za-z0-9._~-]+\Z")


def run(
    *,
    phase: str,
    password_env: str,
    database_url_template: str,
    reject_ambient_database_url: bool = False,
) -> int:
    if reject_ambient_database_url and os.environ.get("DATABASE_URL", "").strip():
        print(
            f"{phase} 数据库入口拒绝继承外部 DATABASE_URL。",
            file=sys.stderr,
        )
        return 64
    password = os.environ.get(password_env, "")
    if _URL_UNRESERVED.fullmatch(password) is None:
        print(
            f"{password_env} 只能包含 ASCII URL 非保留字符"
            "（A-Z、a-z、0-9、.、_、~、-）。",
            file=sys.stderr,
        )
        return 64
    if len(sys.argv) < 2:
        print(f"{phase} 数据库入口缺少子命令。", file=sys.stderr)
        return 64

    database_url = database_url_template.format(password=password)
    os.environ["DATABASE_URL"] = database_url
    os.environ["READER_DEPLOYMENT_DATABASE_URL"] = database_url
    os.environ["READER_MIGRATION_DATABASE_URL"] = database_url
    os.execvp(sys.argv[1], sys.argv[1:])
