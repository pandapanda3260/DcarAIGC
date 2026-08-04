#!/usr/bin/env python3
"""Local-only HTTP API for the DCar v7 evaluation workflow."""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "app" / "data" / "web_mvp.sqlite3"
PYTHONPATH = ROOT / "src" / "dcar_eval"
if str(PYTHONPATH) not in sys.path:
    sys.path.insert(0, str(PYTHONPATH))

from workflow.cache_index import preflight  # noqa: E402
from workflow.reporting import (  # noqa: E402
    build_report_revision,
    create_report_run,
    submit_manual_review,
)
from workflow.storage import migrate  # noqa: E402
from workflow.tasks import (  # noqa: E402
    CURRENT_REPORT_VERSION,
    RunContext,
    TaskManager,
    promote_formal_baseline,
    recover_interrupted_runs,
)

EXPORTS = {
    "report-json": "report.json",
    "report-markdown": "report.md",
    "douyin-csv": "douyin_content_details.csv",
    "xiaohongshu-csv": "xiaohongshu_content_details.csv",
    "summary-image": "core_summary.png",
}

DOUYIN_URL_RE = re.compile(r"https?://(?:www\.)?douyin\.com/", re.I)
XHS_URL_RE = re.compile(r"https?://(?:www\.)?xiaohongshu\.com/", re.I)
UID_RE = re.compile(r"^\d{6,24}$")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH, timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    with connect() as connection:
        migrate(connection)


TASK_MANAGER = TaskManager(DB_PATH)


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


def _safe_project_path(value: str) -> Path:
    path = (ROOT / value).resolve()
    if path != ROOT.resolve() and ROOT.resolve() not in path.parents:
        raise ValueError("文件路径超出项目目录")
    return path


def formal_report_path() -> Path:
    with connect() as connection:
        row = connection.execute(
            """
            SELECT r.output_path
            FROM formal_baseline b JOIN runs r ON r.id=b.run_id
            WHERE b.singleton_id=1 AND r.status='completed' AND r.report_stale=0
            """
        ).fetchone()
        if not row:
            row = connection.execute(
                """
                SELECT output_path FROM runs
                WHERE status='completed' AND report_version=? AND report_stale=0
                  AND output_path IS NOT NULL
                ORDER BY updated_at DESC LIMIT 1
                """,
                (CURRENT_REPORT_VERSION,),
            ).fetchone()
    if not row or not row["output_path"]:
        raise FileNotFoundError("尚无可用的 v7 正式报告")
    path = _safe_project_path(str(row["output_path"]))
    if not path.exists():
        raise FileNotFoundError("正式报告文件不存在")
    return path


def load_report() -> dict[str, Any]:
    return json.loads(formal_report_path().read_text(encoding="utf-8"))


def load_run_report(run_id: str) -> dict[str, Any]:
    run = get_run(run_id)
    if not run or not run.get("output_path"):
        raise FileNotFoundError("任务报告尚未生成")
    return json.loads(_safe_project_path(str(run["output_path"])).read_text(encoding="utf-8"))


def export_paths() -> dict[str, Path]:
    report_path = formal_report_path()
    return {key: report_path.parent / filename for key, filename in EXPORTS.items()}


def overview() -> dict[str, Any]:
    report = load_report()
    channels = report["channels"]
    return {
        "status": "ready",
        "report_version": report["report_version"],
        "rule_version": report["rule_version"],
        "generated_at": report["metadata"]["generated_at"],
        "run_id": report["metadata"]["run_id"],
        "revision": report["metadata"]["revision"],
        "run_summary": report["run_summary"],
        "channels": {
            "douyin": {
                "denominator": channels["douyin"]["denominator"],
                "count_distribution": channels["douyin"]["count_distribution"],
                "verticality": channels["douyin"]["verticality"],
            },
            "xiaohongshu": {
                "denominator": channels["xiaohongshu"]["denominator"],
                "count_distribution": channels["xiaohongshu"]["count_distribution"],
                "verticality": channels["xiaohongshu"]["verticality"],
            },
        },
        "workflow": {
            "mode": "local_v7",
            "provider_refresh_enabled": False,
            "actual_acquisition_connected": False,
            "formal_baseline": True,
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


def execute_cached_regression(context: RunContext) -> None:
    context.progress(20, "正在读取运行级评估快照")
    context.checkpoint()
    context.progress(55, "正在生成并校验 v7 双渠道报告")
    build_report_revision(DB_PATH, context.run_id)
    context.progress(95, "v7 报告已生成，未调用付费 API")


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
                self.send_json({"status": "ok", "mode": "local_v7", "report_version": CURRENT_REPORT_VERSION})
            elif route == "/api/preflight":
                with connect() as connection:
                    self.send_json(preflight(connection))
            elif route == "/api/overview":
                self.send_json(overview())
            elif route == "/api/report/latest":
                self.send_json(load_report())
            elif route == "/api/runs":
                self.send_json({"runs": list_runs()})
            elif re.fullmatch(r"/api/runs/[a-zA-Z0-9_-]+/report", route):
                run_id = route.split("/")[-2]
                self.send_json(load_run_report(run_id))
            elif re.fullmatch(r"/api/runs/[a-zA-Z0-9_-]+/events", route):
                run_id = route.split("/")[-2]
                with connect() as connection:
                    events = [row_dict(row) for row in connection.execute(
                        "SELECT * FROM run_events WHERE run_id=? ORDER BY id", (run_id,)
                    ).fetchall()]
                self.send_json({"run_id": run_id, "events": events})
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
        path = export_paths().get(key)
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
            if route in {"/api/runs/cache-regression", "/api/runs/full"}:
                run_id = uuid.uuid4().hex[:12]
                create_report_run(DB_PATH, run_id, initial_status="queued")
                if route.endswith("cache-regression"):
                    update_run(run_id, mode="cache_regression", run_kind="regression")
                TASK_MANAGER.submit(run_id, execute_cached_regression)
                self.send_json(get_run(run_id), 202)
                return
            match = re.fullmatch(r"/api/runs/([a-zA-Z0-9_-]+)/cancel", route)
            if match:
                run_id = match.group(1)
                run = get_run(run_id)
                if not run:
                    self.send_json({"error": "任务不存在"}, 404)
                    return
                accepted = TASK_MANAGER.cancel(run_id)
                self.send_json(get_run(run_id), 202 if accepted else 409)
                return
            match = re.fullmatch(r"/api/runs/([a-zA-Z0-9_-]+)/resume", route)
            if match:
                run_id = match.group(1)
                run = get_run(run_id)
                if not run:
                    self.send_json({"error": "任务不存在"}, 404)
                    return
                if run["mode"] not in {"cache_regression", "report"} or run["status"] not in {"interrupted", "failed", "cancelled"}:
                    self.send_json({"error": "该任务当前不可恢复"}, 409)
                    return
                update_run(run_id, status="queued", progress=0, cancel_requested=0, message="恢复任务已进入队列")
                TASK_MANAGER.submit(run_id, execute_cached_regression)
                self.send_json(get_run(run_id), 202)
                return
            match = re.fullmatch(r"/api/runs/([a-zA-Z0-9_-]+)/baseline", route)
            if match:
                promote_formal_baseline(DB_PATH, match.group(1))
                self.send_json(get_run(match.group(1)))
                return
            match = re.fullmatch(r"/api/runs/([a-zA-Z0-9_-]+)/reviews", route)
            if match:
                patch = payload.get("patch")
                if not isinstance(patch, dict):
                    raise ValueError("patch 必须是 JSON 对象")
                report = submit_manual_review(
                    DB_PATH,
                    match.group(1),
                    int(payload.get("content_item_id")),
                    patch,
                    str(payload.get("reason") or ""),
                    reviewer=str(payload.get("reviewer") or "local-user"),
                )
                self.send_json(report)
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
    recovered = recover_interrupted_runs(DB_PATH)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"DCar local API: http://{args.host}:{args.port} · recovered={recovered}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        TASK_MANAGER.shutdown(wait=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
