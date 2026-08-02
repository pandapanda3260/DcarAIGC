#!/usr/bin/env python3
"""Local-only HTTP API for the DCar Web MVP.

The MVP deliberately exposes cached evaluation and input validation only.
Provider refreshes remain disabled until an explicit API-budget control is added.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "app" / "data" / "web_mvp.sqlite3"
REPORT_DIR = ROOT / "reports" / "current"
REPORT_JSON = REPORT_DIR / "双渠道结构化结论_v6.2_TikHub_2026-08-02.json"
REPORT_MD = REPORT_DIR / "双渠道结构化结论报告_v6.2_TikHub_2026-08-02.md"
PIPELINE_SCRIPT = ROOT / "src" / "dcar_eval" / "restructure_channel_report_v6_tikhub.py"
PYTHONPATH = ROOT / "src" / "dcar_eval"

EXPORTS = {
    "report-json": REPORT_JSON,
    "report-markdown": REPORT_MD,
    "douyin-csv": REPORT_DIR / "抖音438条内容渠道评估_v6_TikHub补充_2026-08-02.csv",
    "xiaohongshu-csv": REPORT_DIR / "小红书渠道评估样本与数据缺口_v4_2026-08-02.csv",
    "summary-image": REPORT_DIR / "双渠道核心结论_v6_TikHub补充_2026-08-02.png",
}

DOUYIN_URL_RE = re.compile(r"https?://(?:www\.)?douyin\.com/", re.I)
XHS_URL_RE = re.compile(r"https?://(?:www\.)?xiaohongshu\.com/", re.I)
UID_RE = re.compile(r"^\d{6,24}$")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH, timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    with connect() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                mode TEXT NOT NULL,
                channel TEXT NOT NULL,
                status TEXT NOT NULL,
                progress INTEGER NOT NULL DEFAULT 0,
                input_count INTEGER NOT NULL DEFAULT 0,
                message TEXT NOT NULL DEFAULT '',
                output_path TEXT,
                output_sha256 TEXT
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at DESC)"
        )
        connection.commit()


def row_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def list_runs(limit: int = 20) -> list[dict[str, Any]]:
    with connect() as connection:
        rows = connection.execute(
            "SELECT * FROM runs ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [row_dict(row) for row in rows]


def get_run(run_id: str) -> dict[str, Any] | None:
    with connect() as connection:
        row = connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    return row_dict(row) if row else None


def update_run(run_id: str, **changes: Any) -> None:
    if not changes:
        return
    changes["updated_at"] = now_iso()
    columns = ", ".join(f"{key} = ?" for key in changes)
    values = list(changes.values()) + [run_id]
    with connect() as connection:
        connection.execute(f"UPDATE runs SET {columns} WHERE id = ?", values)
        connection.commit()


def load_report() -> dict[str, Any]:
    return json.loads(REPORT_JSON.read_text(encoding="utf-8"))


def overview() -> dict[str, Any]:
    report = load_report()
    channels = report["channels"]
    return {
        "status": "ready",
        "report_version": report["report_version"],
        "generated_at": report["generated_at"],
        "channels": {
            "douyin": {
                "denominator": channels["douyin"]["denominator"],
                "summary": channels["douyin"]["summary"],
            },
            "xiaohongshu": {
                "denominator": channels["xiaohongshu"]["denominator"],
                "summary": channels["xiaohongshu"]["summary"],
            },
        },
        "workflow": {
            "mode": "cache_only",
            "provider_refresh_enabled": False,
            "actual_acquisition_connected": False,
            "migration_regression": "pass",
        },
        "recent_runs": list_runs(8),
    }


def split_items(text: str) -> list[str]:
    return [line.strip() for line in text.replace(",", "\n").splitlines() if line.strip()]


def validate_inputs(channel: str, text: str) -> dict[str, Any]:
    items = split_items(text)
    unique = list(dict.fromkeys(items))
    valid: list[str] = []
    invalid: list[dict[str, str]] = []
    for item in unique:
        if channel == "douyin":
            ok = bool(DOUYIN_URL_RE.search(item) or UID_RE.fullmatch(item))
            reason = "不是抖音链接或6-24位数字UID"
        elif channel == "xiaohongshu":
            ok = bool(XHS_URL_RE.search(item))
            reason = "不是小红书内容链接"
        else:
            ok = False
            reason = "渠道只支持douyin或xiaohongshu"
        if ok:
            valid.append(item)
        else:
            invalid.append({"value": item[:200], "reason": reason})
    return {
        "channel": channel,
        "total": len(items),
        "unique": len(unique),
        "valid_count": len(valid),
        "invalid_count": len(invalid),
        "valid": valid,
        "invalid": invalid,
        "can_start": bool(valid) and not invalid,
        "mode": "validation_only",
    }


def execute_cached_regression(run_id: str) -> None:
    try:
        update_run(run_id, status="running", progress=15, message="正在读取本地缓存")
        before = sha256(REPORT_JSON)
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PYTHONPATH)
        update_run(run_id, progress=45, message="正在重建双渠道结构化报告")
        completed = subprocess.run(
            [sys.executable, str(PIPELINE_SCRIPT)],
            cwd=ROOT,
            env=env,
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )
        after = sha256(REPORT_JSON)
        if before != after:
            raise RuntimeError("缓存回归输出与冻结基线不一致")
        update_run(
            run_id,
            status="completed",
            progress=100,
            message="缓存回归通过，未调用付费API",
            output_path=str(REPORT_JSON.relative_to(ROOT)),
            output_sha256=after,
        )
    except Exception as exc:  # local job boundary
        update_run(
            run_id,
            status="failed",
            progress=100,
            message=f"{type(exc).__name__}: {exc}"[:500],
        )


class Handler(BaseHTTPRequestHandler):
    server_version = "DCarWebMVP/0.1"

    def log_message(self, format: str, *args: Any) -> None:
        sys.stderr.write("[web-api] " + format % args + "\n")

    def cors_headers(self) -> None:
        origin = self.headers.get("Origin") or ""
        allowed = origin if re.fullmatch(r"http://(?:localhost|127\.0\.0\.1):\d{2,5}", origin) else "http://localhost:4173"
        self.send_header("Access-Control-Allow-Origin", allowed)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def send_json(self, payload: Any, status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.cors_headers()
        self.end_headers()
        self.wfile.write(data)

    def read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length > 2_000_000:
            raise ValueError("请求内容超过2MB限制")
        raw = self.rfile.read(length) if length else b"{}"
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("请求必须是JSON对象")
        return value

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self.cors_headers()
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        route = parsed.path.rstrip("/") or "/"
        try:
            if route == "/api/health":
                self.send_json({"status": "ok", "mode": "local_cache_only"})
            elif route == "/api/overview":
                self.send_json(overview())
            elif route == "/api/report/latest":
                self.send_json(load_report())
            elif route == "/api/runs":
                self.send_json({"runs": list_runs()})
            elif route.startswith("/api/runs/"):
                run_id = route.rsplit("/", 1)[-1]
                run = get_run(run_id)
                self.send_json(run or {"error": "任务不存在"}, 200 if run else 404)
            elif route.startswith("/api/files/"):
                key = unquote(route.rsplit("/", 1)[-1])
                self.send_export(key)
            else:
                self.send_json({"error": "接口不存在"}, 404)
        except Exception as exc:
            self.send_json({"error": f"{type(exc).__name__}: {exc}"}, 500)

    def send_export(self, key: str) -> None:
        path = EXPORTS.get(key)
        if not path or not path.exists():
            self.send_json({"error": "导出文件不存在"}, 404)
            return
        content = path.read_bytes()
        mime = {
            ".json": "application/json; charset=utf-8",
            ".md": "text/markdown; charset=utf-8",
            ".csv": "text/csv; charset=utf-8",
            ".png": "image/png",
        }.get(path.suffix.lower(), "application/octet-stream")
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Content-Disposition", f"attachment; filename*=UTF-8''{path.name}")
        self.cors_headers()
        self.end_headers()
        self.wfile.write(content)

    def do_POST(self) -> None:  # noqa: N802
        route = urlparse(self.path).path.rstrip("/")
        try:
            payload = self.read_json()
            if route == "/api/inputs/validate":
                self.send_json(validate_inputs(str(payload.get("channel") or ""), str(payload.get("text") or "")))
                return
            if route == "/api/runs/cache-regression":
                run_id = uuid.uuid4().hex[:12]
                timestamp = now_iso()
                with connect() as connection:
                    connection.execute(
                        """
                        INSERT INTO runs (id, created_at, updated_at, mode, channel, status, progress, input_count, message)
                        VALUES (?, ?, ?, 'cache_regression', 'dual', 'queued', 0, 776, '任务已进入本地队列')
                        """,
                        (run_id, timestamp, timestamp),
                    )
                    connection.commit()
                threading.Thread(target=execute_cached_regression, args=(run_id,), daemon=True).start()
                self.send_json(get_run(run_id), 202)
                return
            self.send_json({"error": "接口不存在"}, 404)
        except (ValueError, json.JSONDecodeError) as exc:
            self.send_json({"error": str(exc)}, 400)
        except Exception as exc:
            self.send_json({"error": f"{type(exc).__name__}: {exc}"}, 500)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    init_db()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"DCar local API: http://{args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
