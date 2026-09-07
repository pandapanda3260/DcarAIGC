"""Durable UI update queue. Calls the sealed Writer over HTTP, never its database.

Run as one loopback service with DCAR_UPDATE_QUEUE_DB and
DCAR_UPDATE_COORDINATOR_TOKEN_FILE. A disconnected browser cannot cancel work.
An interrupted/ambiguous POST is deliberately never replayed automatically.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hmac
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
from typing import Any, Callable, Iterator, Sequence
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field


SHANGHAI = ZoneInfo("Asia/Shanghai")
UNCERTAIN = "更新请求已发送，但未能确认最终结果。请先查看最新数据或联系管理员确认，系统不会自动重复更新。"
SCHEMA_VERSION = 1
APPLICATION_ID = 0x44435551
MAX_ACTIVE_JOBS = 50
COMMAND_CONTRACT = "content-update-writer-command-v1"


def writer_command(value: Any, content_id: int) -> dict[str, Any] | None:
    """Private durable pointer inside the existing result column, not a new schema."""
    if (not isinstance(value, dict) or value.get("contract") != COMMAND_CONTRACT
            or type(value.get("run_id")) is not int or value["run_id"] <= 0
            or type(value.get("content_id")) is not int or value["content_id"] != content_id):
        return None
    return {key: value[key] for key in ("contract", "run_id", "content_id")}


def stored_command(serialized: str | None, content_id: int) -> dict[str, Any] | None:
    try:
        value = json.loads(serialized) if serialized else None
    except (ValueError, TypeError):
        return None
    return writer_command(value.get("writer_command"), content_id) if isinstance(value, dict) else None


def command_terminal(payload: Any, command: dict[str, Any]) -> tuple[dict[str, Any], str | None] | None:
    """Map read_command's real work state; its provider_calls=0 is NOT a cost."""
    if (not isinstance(payload, dict) or type(payload.get("run_id")) is not int
            or payload["run_id"] != command["run_id"] or type(payload.get("content_id")) is not int
            or payload["content_id"] != command["content_id"] or payload.get("kind") != "manual_update"):
        return None
    status, work = payload.get("status"), payload.get("work")
    if not isinstance(work, list):
        return None
    if status in {"succeeded", "partial"}:
        if not work or not all(isinstance(item, dict) and type(item.get("id")) is int
                               and item["id"] > 0 and item.get("state") == "terminal"
                               and isinstance(item.get("completed_at"), str) and item["completed_at"]
                               for item in work):
            return None
        # The status endpoint currently has no authoritative cost/stage totals.
        # Never turn the SELECT's provider_calls=0 into a successful free update.
        result = safe_result({"status": status})
        assert result is not None
        return result, None
    if status == "failed" or status == "blocked" and not work:
        result = safe_result({"status": "failed"})
        assert result is not None
        return result, "command_blocked" if status == "blocked" else "command_failed"
    # A paid identity hold/provider block is recoverable shared work. Keep its
    # pointer; do not free the content for another POST while it can still run.
    return None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def business_day(timestamp: str) -> str:
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(SHANGHAI).date().isoformat()


@dataclass(frozen=True)
class Config:
    db_path: Path
    token: str
    writer_url: str = "http://127.0.0.1:8766"
    timeout_seconds: float = 900

    def __post_init__(self) -> None:
        url = urlsplit(self.writer_url)
        if (url.scheme != "http" or url.hostname not in {"127.0.0.1", "::1", "localhost"}
                or url.username or url.password or url.query or url.fragment or url.path not in {"", "/"}):
            raise ValueError("Writer must be a loopback HTTP origin")
        if len(self.token) < 32 or not self.token.isascii() or any(char.isspace() for char in self.token):
            raise ValueError("Coordinator token must contain at least 32 ASCII characters without whitespace")
        if not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("Invalid Writer timeout")
        # This service must never initialize or open the application's formal DB.
        if self.db_path.name in {"dcar_insight.sqlite3", "dcar_insight.db"}:
            raise ValueError("A separate update queue database is required")

    @classmethod
    def from_env(cls) -> "Config":
        path = Path(os.environ["DCAR_UPDATE_COORDINATOR_TOKEN_FILE"])
        info = path.stat()
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600 or info.st_uid != os.getuid():
            raise ValueError("Coordinator token file must be owned by the service user with mode 0600")
        return cls(
            db_path=Path(os.environ["DCAR_UPDATE_QUEUE_DB"]).expanduser().resolve(),
            token=path.read_text().strip(),
            writer_url=os.environ.get("DCAR_UPDATE_WRITER_URL", "http://127.0.0.1:8766").rstrip("/"),
        )


def safe_result(payload: Any) -> dict[str, Any] | None:
    """Only fields required by the UI; no receipts, provider URLs or raw errors."""
    if not isinstance(payload, dict) or payload.get("status") not in {"succeeded", "partial", "failed"}:
        return None
    result: dict[str, Any] = {"status": payload["status"]}
    cost = payload.get("provider_cost")
    result["provider_cost"] = cost if (type(cost) is int or type(cost) is float) and math.isfinite(cost) and cost >= 0 else None
    result["currency"] = "USD" if payload.get("currency") == "USD" else "unknown"
    stages = payload.get("stages")
    if isinstance(stages, list):
        result["stages"] = []
        for item in stages[:20]:
            item = item if isinstance(item, dict) else {}
            result["stages"].append({
                "stage": item.get("stage") if item.get("stage") in {
                    "detail", "detail_type_probe", "metrics", "comments",
                } else "unknown",
                "status": item.get("status") if item.get("status") in {
                    "succeeded", "replayed", "already_succeeded", "failed",
                } else "unknown",
            })
    metrics = payload.get("metrics")
    if isinstance(metrics, dict):
        fields = metrics.get("missing_fields")
        result["metrics"] = {
            "status": metrics.get("status") if metrics.get("status") in {"succeeded", "partial"} else "unknown",
            "missing_fields": [field for field in fields if field in {
                "view_count", "comment_count", "like_count", "share_count", "collect_count",
            }] if isinstance(fields, list) else [],
        }
    media = payload.get("media")
    if isinstance(media, dict):
        result["media"] = {"status": media.get("status") if media.get("status") in {
            "evidence_ready", "restore_required", "expired_non_replayable", "retryable_failed",
            "no_source", "legacy_source_skipped",
        } else "unknown"}
        if media.get("reason") == "original_restoring":
            result["media"]["reason"] = "original_restoring"
        if type(media.get("can_restore")) is bool:
            result["media"]["can_restore"] = media["can_restore"]
    return result


class QueueStore:
    def __init__(self, path: Path, *, clock: Callable[[], str] = utc_now):
        self.path = path
        self.clock = clock
        self._lock: Any = None

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = self.path.with_suffix(self.path.suffix + ".lock").open("a+")
        os.chmod(self._lock.name, 0o600)
        try:
            fcntl.flock(self._lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Inspect existing files read-only before issuing any mutation PRAGMA.
            if self.path.exists() and self.path.stat().st_size:
                with sqlite3.connect(f"{self.path.resolve().as_uri()}?mode=ro", uri=True) as db:
                    if db.execute("PRAGMA application_id").fetchone()[0] != APPLICATION_ID:
                        raise ValueError("Existing file is not an update queue database")
                    if db.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
                        raise ValueError("Unsupported update queue schema")
            with self.connection() as db:
                db.execute("PRAGMA journal_mode=WAL")
                db.executescript("""
                    CREATE TABLE IF NOT EXISTS update_jobs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        content_id INTEGER NOT NULL,
                        title TEXT NOT NULL,
                        status TEXT NOT NULL CHECK(status IN ('queued','running','succeeded','failed')),
                        stage TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        completed_at TEXT,
                        submitted_day TEXT NOT NULL,
                        result_json TEXT,
                        error TEXT,
                        error_code TEXT
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS update_jobs_active_content
                        ON update_jobs(content_id)
                        WHERE status IN ('queued','running') OR error_code='result_uncertain';
                    CREATE TABLE IF NOT EXISTS update_requests (
                        request_id TEXT PRIMARY KEY,
                        job_id INTEGER NOT NULL REFERENCES update_jobs(id),
                        created_at TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS update_requests_job ON update_requests(job_id);
                """)
                db.execute(f"PRAGMA application_id={APPLICATION_ID}")
                db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            os.chmod(self.path, 0o600)
            # After acquiring the exclusive process lock no previous worker can
            # still be alive. Its sent POST may nevertheless be alive in Writer.
            self.mark_interrupted()
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self._lock is not None:
            self._lock.close()
            self._lock = None

    def mark_interrupted(self) -> None:
        """Only POSTs lacking a durable acknowledgement become uncertain."""
        with self.connection(write=True) as db:
            timestamp = self.clock()
            for row in db.execute("SELECT id,content_id,result_json FROM update_jobs WHERE status='running'").fetchall():
                if stored_command(row["result_json"], row["content_id"]) is None:
                    db.execute("UPDATE update_jobs SET status='failed',stage='failed',updated_at=?,"
                               "completed_at=?,error=?,error_code='result_uncertain' WHERE id=?",
                               (timestamp, timestamp, UNCERTAIN, row["id"]))

    @contextmanager
    def connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=FULL")
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            if write:
                db.commit()
        except BaseException:
            if db.in_transaction:
                db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def public(db: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        requests = [item[0] for item in db.execute(
            "SELECT request_id FROM update_requests WHERE job_id=? ORDER BY rowid", (row["id"],))]
        job = {key: row[key] for key in (
            "id", "content_id", "title", "status", "stage", "created_at", "updated_at",
            "completed_at", "error", "error_code",
        )}
        persisted = json.loads(row["result_json"]) if row["result_json"] else None
        command = stored_command(row["result_json"], row["content_id"])
        job.update(request_id=requests[0], request_ids=requests,
                   stage_label={"queued": "等待更新", "updating": "正在更新数据",
                                "completed": "更新完成", "failed": "更新未完成"}[row["stage"]],
                   result=persisted.get("result") if isinstance(persisted, dict) and command is not None else persisted)
        if command is not None:
            job["writer_run_id"] = command["run_id"]
        return job

    def enqueue(self, content_id: int, request_id: str, title: str) -> dict[str, Any]:
        with self.connection(write=True) as db:
            row = db.execute("SELECT j.* FROM update_jobs j JOIN update_requests r ON r.job_id=j.id "
                             "WHERE r.request_id=?", (request_id,)).fetchone()
            if row is not None:
                if row["content_id"] != content_id:
                    raise HTTPException(409, "该请求编号已用于其他内容，请刷新后重试。")
                return self.public(db, row)
            row = db.execute("SELECT * FROM update_jobs WHERE content_id=? "
                             "AND (status IN ('queued','running') OR error_code='result_uncertain')",
                             (content_id,)).fetchone()
            timestamp = self.clock()
            if row is None:
                if db.execute("SELECT COUNT(*) FROM update_jobs WHERE status IN ('queued','running') "
                              "OR error_code='result_uncertain'").fetchone()[0] >= MAX_ACTIVE_JOBS:
                    raise HTTPException(429, "待更新任务较多，请等待部分任务完成后再提交。")
                job_id = db.execute("INSERT INTO update_jobs(content_id,title,status,stage,created_at,updated_at,"
                                    "submitted_day) VALUES (?,?,'queued','queued',?,?,?)",
                                    (content_id, title or f"内容 {content_id}", timestamp, timestamp,
                                     business_day(timestamp))).lastrowid
                row = db.execute("SELECT * FROM update_jobs WHERE id=?", (job_id,)).fetchone()
            db.execute("INSERT INTO update_requests(request_id,job_id,created_at) VALUES (?,?,?)",
                       (request_id, row["id"], timestamp))
            return self.public(db, row)

    def get(self, job_id: int) -> dict[str, Any]:
        with self.connection() as db:
            row = db.execute("SELECT * FROM update_jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise HTTPException(404, "更新任务不存在。")
            return self.public(db, row)

    def list(self, *, limit: int = 50) -> list[dict[str, Any]]:
        with self.connection() as db:
            rows = db.execute("SELECT * FROM update_jobs ORDER BY CASE WHEN status IN ('queued','running') "
                              "OR error_code='result_uncertain' "
                              "THEN 0 ELSE 1 END,id DESC LIMIT ?", (limit,)).fetchall()
            return [self.public(db, row) for row in rows]

    def claim(self) -> dict[str, Any] | None:
        with self.connection(write=True) as db:
            timestamp = self.clock()
            db.execute("UPDATE update_jobs SET status='failed',stage='failed',updated_at=?,completed_at=?,"
                       "error='排队任务已跨过提交日期，未发送更新请求。请重新提交。',error_code='queue_expired' "
                       "WHERE status='queued' AND submitted_day!=?",
                       (timestamp, timestamp, business_day(timestamp)))
            row = db.execute("SELECT * FROM update_jobs WHERE status='queued' ORDER BY id LIMIT 1").fetchone()
            if row is None:
                return None
            db.execute("UPDATE update_jobs SET status='running',stage='updating',updated_at=? WHERE id=?",
                       (timestamp, row["id"]))
            return self.public(db, db.execute("SELECT * FROM update_jobs WHERE id=?", (row["id"],)).fetchone())

    def accept_command(self, job_id: int, payload: Any) -> bool:
        with self.connection(write=True) as db:
            row = db.execute("SELECT * FROM update_jobs WHERE id=? AND status='running'", (job_id,)).fetchone()
            if (row is None or not isinstance(payload, dict) or payload.get("kind") != "manual_update"
                    or payload.get("status") not in {"pending", "running", "blocked", "succeeded", "partial", "failed"}):
                return False
            command = writer_command({"contract": COMMAND_CONTRACT, "run_id": payload.get("run_id"),
                                      "content_id": payload.get("content_id")}, row["content_id"])
            if command is None:
                return False
            previous = stored_command(row["result_json"], row["content_id"])
            if previous is not None:
                return previous == command
            db.execute("UPDATE update_jobs SET result_json=?,updated_at=? WHERE id=? AND status='running'",
                       (json.dumps({"writer_command": command}, separators=(",", ":")), self.clock(), job_id))
            return True

    def pending_commands(self) -> Sequence[tuple[int, dict[str, Any]]]:
        with self.connection() as db:
            rows = db.execute("SELECT id,content_id,result_json FROM update_jobs WHERE status='running' ORDER BY id LIMIT ?",
                              (MAX_ACTIVE_JOBS,)).fetchall()
            return [(row["id"], command) for row in rows
                    if (command := stored_command(row["result_json"], row["content_id"])) is not None]

    def finish(self, job_id: int, *, result: dict[str, Any] | None = None,
               error: str | None = None, error_code: str | None = None) -> None:
        with self.connection(write=True) as db:
            timestamp = self.clock()
            succeeded = result is not None and result["status"] in {"succeeded", "partial"} and error is None
            row = db.execute("SELECT content_id,result_json FROM update_jobs WHERE id=? AND status='running'", (job_id,)).fetchone()
            if row is None:
                return
            command = stored_command(row["result_json"], row["content_id"])
            persisted = {"writer_command": command, "result": result} if command is not None else result
            db.execute("UPDATE update_jobs SET status=?,stage=?,updated_at=?,completed_at=?,result_json=?,"
                       "error=?,error_code=? WHERE id=? AND status='running'",
                       ("succeeded" if succeeded else "failed", "completed" if succeeded else "failed",
                        timestamp, timestamp, json.dumps(persisted, ensure_ascii=False, allow_nan=False) if persisted else None,
                        error, error_code, job_id))


class Coordinator:
    def __init__(self, config: Config, *, transport: httpx.AsyncBaseTransport | None = None,
                 clock: Callable[[], str] = utc_now):
        self.config = config
        self.store = QueueStore(config.db_path, clock=clock)
        self.transport = transport
        self.wake = asyncio.Event()
        self.stopping = False

    async def process_one(self, client: httpx.AsyncClient) -> bool:
        # Bounded sweep before a new POST: a pending/failed GET cannot starve
        # another accepted command or queued content. The loop sleeps between
        # sweeps when no new POST was dispatched.
        for job_id, command in self.store.pending_commands():
            try:
                response = await client.get(f"/api/v8/contents/{command['content_id']}/update-data/commands/{command['run_id']}")
                terminal = command_terminal(response.json(), command) if response.status_code == 200 else None
            except (httpx.HTTPError, OSError, ValueError, TypeError):
                terminal = None
            if terminal is not None:
                command_result, code = terminal
                self.store.finish(job_id, result=command_result, error_code=code,
                    error="本次数据更新未完成，请查看最新数据。" if code is not None else None)
        job = self.store.claim()
        if job is None:
            return False
        try:
            response = await client.post(f"/api/v8/contents/{job['content_id']}/update-data")
        except (httpx.HTTPError, OSError):
            # Includes connect/read timeouts. Even a missing response is not
            # evidence that Writer did not receive or execute the request.
            self.store.finish(job["id"], error=UNCERTAIN, error_code="result_uncertain")
            return True
        if response.status_code == 202:
            try:
                accepted = self.store.accept_command(job["id"], response.json())
            except (ValueError, TypeError):
                accepted = False
            if not accepted:
                self.store.finish(job["id"], error=UNCERTAIN, error_code="result_uncertain")
        elif response.status_code == 200:
            try:
                result = safe_result(response.json())
            except (ValueError, TypeError):
                result = None
            if result is not None and result["status"] in {"succeeded", "partial"}:
                self.store.finish(job["id"], result=result)
            elif result is not None:
                self.store.finish(job["id"], result=result, error="本次数据更新失败，请查看最新数据。",
                                  error_code="update_failed")
            else:
                self.store.finish(job["id"], error=UNCERTAIN, error_code="result_uncertain")
        elif response.status_code in {400, 401, 403, 404, 409, 422, 429}:
            messages = {
                400: "更新请求未通过校验，请刷新后重试。",
                401: "更新服务暂不可用，请联系管理员。",
                403: "当前规则不允许更新该内容，请联系管理员。",
                404: "内容不存在或更新服务暂不可用。",
                409: "当前名单、采集周期或预算不允许更新；系统不会绕过固定取数规则。",
                422: "更新请求未通过校验，请刷新后重试。",
                429: "更新服务繁忙，请稍后重试。",
            }
            self.store.finish(job["id"], error=messages[response.status_code],
                              error_code=f"writer_http_{response.status_code}")
        else:
            # Gateway/server errors can occur after paid work was dispatched.
            self.store.finish(job["id"], error=UNCERTAIN, error_code="result_uncertain")
        return True

    async def run(self) -> None:
        async with httpx.AsyncClient(base_url=self.config.writer_url, timeout=self.config.timeout_seconds,
                                     transport=self.transport, trust_env=False, follow_redirects=False) as client:
            while not self.stopping:
                self.wake.clear()
                try:
                    if await self.process_one(client):
                        continue
                except Exception:
                    # The durable running record remains uncertain after any
                    # unexpected worker error. Do not let an in-memory failure
                    # prevent subsequent queued jobs from being inspected.
                    self.store.mark_interrupted()
                try:
                    await asyncio.wait_for(self.wake.wait(), timeout=2)
                except asyncio.TimeoutError:
                    pass


class SubmitUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(min_length=8, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")
    title: str = Field(default="", max_length=500)


def create_app(config: Config | None = None, *, transport: httpx.AsyncBaseTransport | None = None,
               clock: Callable[[], str] = utc_now, run_worker: bool = True) -> FastAPI:
    resolved = config or Config.from_env()
    coordinator = Coordinator(resolved, transport=transport, clock=clock)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        coordinator.store.open()
        worker = asyncio.create_task(coordinator.run()) if run_worker else None
        app.state.coordinator = coordinator
        try:
            yield
        finally:
            # Drain the current HTTP request. Queued work stays durable for the
            # next process; forced termination is handled conservatively at open.
            coordinator.stopping = True
            coordinator.wake.set()
            try:
                if worker is not None:
                    await worker
            finally:
                coordinator.store.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def authenticate(request: Request, call_next: Callable) -> Any:
        supplied = request.headers.get("X-Dcar-Update-Key", "")
        if not hmac.compare_digest(supplied.encode(), resolved.token.encode()):
            return JSONResponse({"detail": "无权访问更新任务。"}, status_code=403,
                                headers={"Cache-Control": "no-store"})
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/v8/contents/{content_id}/update-jobs")
    async def enqueue(content_id: int, body: SubmitUpdate) -> JSONResponse:
        if content_id <= 0:
            raise HTTPException(422, "内容编号无效。")
        job = coordinator.store.enqueue(content_id, body.request_id, body.title)
        coordinator.wake.set()
        return JSONResponse({"job": job}, status_code=202, headers={
            "Location": f"/api/v8/content-update-jobs/{job['id']}", "Retry-After": "2",
        })

    @app.get("/api/v8/content-update-jobs")
    async def list_jobs(limit: int = 50) -> dict[str, Any]:
        return {"jobs": coordinator.store.list(limit=max(1, min(limit, 200)))}

    @app.get("/api/v8/content-update-jobs/{job_id}")
    async def get_job(job_id: int) -> dict[str, Any]:
        return {"job": coordinator.store.get(job_id)}

    return app


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(create_app(), host="127.0.0.1", port=8767, access_log=False,
                timeout_graceful_shutdown=910)
