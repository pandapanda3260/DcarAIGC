"""Persistent, owner-scoped content exports; formal SQLite is opened read-only.

The short command process validates and journals requests. A detached --worker
process serializes exports under flock, holds one live-WAL read transaction, and
streams all matching pages into XLSX without entering the backend lifespan.
"""
from __future__ import annotations

import sys
import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import fcntl
import importlib.util
import heapq
import io
import json
import os
from pathlib import Path
import re
import sqlite3
import signal
from typing import Any, Iterator
import uuid
import zipfile

sys.dont_write_bytecode = True

# Python -I omits the script directory. Load this audited sibling explicitly.
_SEARCH_SPEC = importlib.util.spec_from_file_location(
    "dcar_export_content_search", Path(__file__).resolve().with_name("content_search.py")
)
search_helper = importlib.util.module_from_spec(_SEARCH_SPEC)
_SEARCH_SPEC.loader.exec_module(search_helper)

BEIJING = timezone(timedelta(hours=8))
PAGE_SIZE = 500
PAGE_TIMEOUT_SECONDS = 60.0
JOB_TIMEOUT_SECONDS = 1800
PUBLIC_FIELDS = (
    "id", "status", "filters", "created_at", "started_at", "completed_at",
    "total", "completed_rows", "filename", "error", "request_id",
)
FILTER_FIELDS = frozenset({
    "query", "platform", "account_group", "business_direction", "content_direction",
    "selling_point", "spu_series", "audience", "scene", "published_from", "published_to",
})
COLUMNS = (
    ("link_id", "系统内容编号"), ("platform_content_id", "平台作品编号"),
    ("platform", "平台"), ("title", "标题"), ("canonical_url", "内容链接"),
    ("content_type", "内容类型"), ("published_at", "发布时间（北京时间）"),
    ("raw_account_uid", "平台账号编号"), ("raw_account_name", "账号名称"),
    ("account_group", "账号分组"), ("business_direction", "业务方向"),
    ("content_direction", "作品内容方向"), ("primary_selling_point_code", "主要卖点编号"),
    ("view_count", "阅读/播放数"), ("like_count", "点赞数"), ("comment_count", "评论数"),
    ("metrics_captured_at", "指标更新时间（北京时间）"),
    ("view_updated_at", "阅读/播放更新时间（北京时间）"),
    ("like_updated_at", "点赞更新时间（北京时间）"),
    ("comment_updated_at", "评论更新时间（北京时间）"),
)
FILTER_LABELS = {
    "query": "关键词", "platform": "平台", "account_group": "账号分组",
    "business_direction": "业务方向", "content_direction": "作品内容方向",
    "selling_point": "卖点", "spu_series": "车系", "audience": "人群",
    "scene": "场景", "published_from": "发布时间开始日期", "published_to": "发布时间结束日期",
}


class ExportError(ValueError):
    def __init__(self, detail: str, status: int = 422):
        super().__init__(detail)
        self.status = status


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def identifier(value: Any) -> str:
    if not isinstance(value, str):
        raise ExportError("导出任务编号无效。")
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ExportError("导出任务编号无效。") from error
    if value not in (str(parsed), parsed.hex):
        raise ExportError("导出任务编号无效。")
    return str(parsed)


def owner_id(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ExportError("导出用户无效。")
    return value


def normalize_filters(value: Any, api: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) - (FILTER_FIELDS | {"page", "page_size"}):
        raise ExportError("导出条件包含不支持的字段。")
    # Validate pagination even though export scope deliberately excludes it.
    try:
        payload = search_helper.validate_request(value, api)
    except search_helper.RequestError as error:
        raise ExportError(str(error)) from error
    normalized = {}
    for key, item in payload.model_dump().items():
        if key not in FILTER_FIELDS or item is None or item == "":
            continue
        normalized[key] = item
    if not normalized or (set(normalized) == {"query"} and not normalized["query"].strip("%_\\ \t\r\n")):
        raise ExportError("请先选择发布时间或其他筛选条件，再导出筛选结果。")
    # Sorting provides stable durable idempotency across property orderings.
    return dict(sorted(normalized.items()))


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            os.chmod(temporary, 0o600)
            json.dump(value, stream, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


class Queue:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        (self.root / "jobs").mkdir(exist_ok=True, mode=0o700)
        (self.root / "files").mkdir(exist_ok=True, mode=0o700)

    @contextmanager
    def locked(self) -> Iterator[None]:
        with (self.root / "queue.lock").open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def worker_active(self) -> bool:
        with (self.root / "worker.lock").open("a+") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(lock, fcntl.LOCK_UN)
            return False

    def records(self) -> Iterator[dict[str, Any]]:
        for path in (self.root / "jobs").glob("*.json"):
            yield json.loads(path.read_text(encoding="utf-8"))

    def save(self, job: dict[str, Any]) -> None:
        # Callers hold queue.lock. Worker progress uses its original in-memory
        # job, so retain aliases accepted concurrently while that worker runs.
        path = self.root / "jobs" / f'{job["id"]}.json'
        aliases = {job["request_id"], *job.get("request_ids", [])}
        if path.is_file():
            existing = json.loads(path.read_text(encoding="utf-8"))
            aliases.update(existing.get("request_ids", [existing["request_id"]]))
        job["request_ids"] = sorted(aliases)
        atomic_json(path, job)

    def reconcile_file(self, job: dict[str, Any]) -> dict[str, Any]:
        # Only terminal success is eligible, and list/get hold queue.lock for
        # both the read and write so this cannot overwrite worker progress.
        if job["status"] == "succeeded":
            path = self.root / "files" / f'{job["id"]}.xlsx'
            if not path.is_file() or path.is_symlink():
                job.update(status="failed", completed_at=now(), filename=None,
                           error="导出文件已不可用，请重新生成。")
                self.save(job)
        return job

    def public(self, job: dict[str, Any]) -> dict[str, Any]:
        return {key: job.get(key) for key in PUBLIC_FIELDS}

    def read(self, owner: str, job_id: str) -> dict[str, Any]:
        path = self.root / "jobs" / f"{identifier(job_id)}.json"
        if not path.is_file():
            raise ExportError("导出任务不存在或不可访问。", 404)
        job = json.loads(path.read_text(encoding="utf-8"))
        if job.get("owner") != owner:
            raise ExportError("导出任务不存在或不可访问。", 404)
        return job

    def create(self, owner: str, request_id: str, filters: dict[str, Any]) -> dict[str, Any]:
        with self.locked():
            request_path = self.root / "requests" / owner / f"{request_id}.json"
            if request_path.is_file():
                request = json.loads(request_path.read_text(encoding="utf-8"))
                if request["filters"] != filters:
                    raise ExportError("该导出请求已使用其他筛选条件，请重新发起导出。", 409)
                job = self.reconcile_file(self.read(owner, request["job_id"]))
                return {"job": self.public(job), "reused": True, "worker_active": self.worker_active()}
            # The job journal stores every accepted request alias before its
            # lookup index, so a crash cannot lose durable idempotency.
            original = next((job for job in self.records() if job["owner"] == owner
                             and request_id in job.get("request_ids", [job["request_id"]])), None)
            if original is not None:
                if original["filters"] != filters:
                    raise ExportError("该导出请求已使用其他筛选条件，请重新发起导出。", 409)
                self.reconcile_file(original)
                atomic_json(request_path, {"job_id": original["id"], "filters": filters})
                return {"job": self.public(original), "reused": True, "worker_active": self.worker_active()}
            active = next((job for job in self.records() if job["owner"] == owner
                           and job["filters"] == filters and job["status"] in ("queued", "running")), None)
            job = active or dict(
                id=str(uuid.uuid4()), owner=owner, request_id=request_id, filters=filters,
                status="queued", created_at=now(), started_at=None, completed_at=None,
                total=None, completed_rows=0, filename=None, error=None,
            )
            job["request_ids"] = [*job.get("request_ids", []), request_id]
            self.save(job)
            atomic_json(request_path, {"job_id": job["id"], "filters": filters})
            return {"job": self.public(job), "reused": active is not None, "worker_active": self.worker_active()}

    def list(self, owner: str) -> dict[str, Any]:
        with self.locked():
            jobs = heapq.nlargest(20, (job for job in self.records() if job["owner"] == owner),
                                 key=lambda job: (job["created_at"], job["id"]))
            return {"jobs": [self.public(self.reconcile_file(job)) for job in jobs[:20]], "worker_active": self.worker_active()}

    def get(self, owner: str, job_id: str) -> dict[str, Any]:
        with self.locked():
            job = self.reconcile_file(self.read(owner, job_id))
            result = {"job": self.public(job), "worker_active": self.worker_active()}
            if job["status"] == "succeeded":
                path = self.root / "files" / f'{job["id"]}.xlsx'
                if path.is_file() and not path.is_symlink():
                    result["file_path"] = str(path)
            return result


class _OneRow:
    def __init__(self, row: Any):
        self.row = row

    def fetchone(self) -> Any:
        return self.row


class SnapshotConnection:
    """Keep the existing list query inside the worker's longer transaction.

    The API owns a per-call connection context and BEGIN. This dedicated process
    adapts only that ownership, preserving its SQL, visibility, and metric policy.
    Identical counts are cached because they cannot change inside this snapshot.
    """
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.counts: dict[Any, Any] = {}

    def __enter__(self) -> "SnapshotConnection":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def __getattr__(self, key: str) -> Any:
        return getattr(self.connection, key)

    def execute(self, sql: str, parameters: Any = ()) -> Any:
        if sql.strip().upper() == "BEGIN":
            if not self.connection.in_transaction:
                raise RuntimeError("export snapshot lost")
            return _OneRow(None)
        if "SELECT COUNT(*) FROM content_items" in sql:
            key = (sql, tuple(parameters))
            if key not in self.counts:
                self.counts[key] = self.connection.execute(sql, parameters).fetchone()
            return _OneRow(self.counts[key])
        return self.connection.execute(sql, parameters)


@contextmanager
def snapshot(db: Path, api: Any, storage: Any) -> Iterator[SnapshotConnection]:
    original = api.connect
    version = None
    if os.environ.get("DCAR_CONTENT_DATA_MODE", "formal") == "snapshot":
        version = search_helper.verify_database(db, Path(os.environ["DCAR_PROJECT_ROOT"]))
    with storage.live_wal_read_only_connections(), original(db, read_only=True) as connection:
        if version is not None:
            from v8.reader_readiness import database_version
            if database_version(db) != version:
                raise RuntimeError("snapshot database changed before export")
        connection.execute("BEGIN")
        adapted = SnapshotConnection(connection)

        def reuse(path: Path, *, read_only: bool = False) -> SnapshotConnection:
            if not read_only or Path(path).resolve() != db.resolve():
                raise RuntimeError("unexpected export connection")
            return adapted

        api.connect = reuse
        try:
            yield adapted
        finally:
            api.connect = original


def beijing_time(value: Any, report: Any) -> Any:
    if value in (None, ""):
        return None
    return report._parse_beijing_datetime(str(value)) or str(value)


def labels(report: Any) -> dict[str, dict[str, str]]:
    from v8.account_classification import ACCOUNT_GROUPS, BUSINESS_DIRECTIONS
    return {
        "platform": report._PLATFORM_LABELS, "content_type": report._CONTENT_TYPE_LABELS,
        "content_direction": report._CONTENT_DIRECTION_LABELS,
        "account_group": ACCOUNT_GROUPS, "business_direction": BUSINESS_DIRECTIONS,
    }


def row_values(item: dict[str, Any], report: Any) -> list[Any]:
    translations = labels(report)
    result = []
    for key, _label in COLUMNS:
        value = item.get(key)
        if key in translations:
            value = translations[key].get(value, value)
        elif key in {"link_id", "platform_content_id", "raw_account_uid"}:
            value = str(value) if value is not None else None
        elif key in {"view_updated_at", "like_updated_at", "comment_updated_at"}:
            field = key.removesuffix("_updated_at") + "_count"
            fact = (item.get("metric_fields") or {}).get(field) or {}
            value = beijing_time(fact.get("captured_at"), report)
        elif key in {"published_at", "metrics_captured_at"}:
            value = beijing_time(value, report)
        result.append(value)
    return result


def xml_row(number: int, values: list[Any], report: Any, *, header: bool = False) -> bytes:
    cells = []
    for index, value in enumerate(values, 1):
        style = 1 if header else 8 if isinstance(value, datetime) else 12 if isinstance(value, str) else 5 if isinstance(value, (int, float)) else 4
        cells.append(report._cell_xml(f"{report._column_name(index)}{number}", value, style))
    return f'<row r="{number}">{"".join(cells)}</row>'.encode("utf-8")


def explanation_rows(job: dict[str, Any], report: Any) -> list[list[Any]]:
    translations = labels(report)
    rows = [
        ["导出说明", "内容"],
        ["筛选范围", "当前已生效条件下的全部结果，包含所有分页。"],
        ["时间口径", "作品发布时间按北京时间自然日筛选，包含开始和结束当天；全部时间包含发布时间缺失的作品。"],
        ["指标口径", "阅读、点赞、评论为读取快照中的最新有效累计值，不是所选期间新增量；缺失值留空，真实零值保留为 0。"],
        ["指标更新时间", "各指标可能来自不同有效采集时间；总指标更新时间以及各指标更新时间均为北京时间。"],
        ["编号格式", "系统内容编号、平台作品编号、平台账号编号按文本保存，避免大编号精度丢失。"],
        ["生成时间（北京时间）", beijing_time(job["completed_at"], report)],
        ["读取快照开始时间（北京时间）", beijing_time(job["started_at"], report)],
        ["实际导出条数", job["completed_rows"]],
    ]
    for key, value in job["filters"].items():
        rows.append([FILTER_LABELS[key], translations.get(key, {}).get(value, "无" if value == "__none__" else value)])
    return rows


def generate(job: dict[str, Any], queue: Queue, db: Path, api: Any, storage: Any) -> None:
    from v8 import report_export as report
    partial = queue.root / "files" / f'{job["id"]}.partial'
    destination = partial.with_suffix(".xlsx")
    try:
        payload = search_helper.validate_request(job["filters"], api)
        with snapshot(db, api, storage) as connection, zipfile.ZipFile(
            partial, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True
        ) as archive:
            os.chmod(partial, 0o600)
            if any(job["filters"].get(key) for key in ("spu_series", "audience", "scene")) and not api.spu_domain_ready(connection):
                raise ExportError("当前数据尚不支持所选标签筛选，请调整筛选条件后重试。")
            page = 1
            with archive.open("xl/worksheets/sheet1.xml", "w", force_zip64=True) as detail:
                detail.write(b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews><sheetFormatPr defaultRowHeight="20"/><cols><col min="1" max="3" width="23" customWidth="1"/><col min="4" max="5" width="48" customWidth="1"/><col min="6" max="20" width="23" customWidth="1"/></cols><sheetData>')
                detail.write(xml_row(1, [title for _, title in COLUMNS], report, header=True))
                while True:
                    request = payload.model_copy(update={"page": page, "page_size": PAGE_SIZE})
                    result = api._content_search(request, db_path=db, read_only=True,
                                                 query_timeout_seconds=PAGE_TIMEOUT_SECONDS)
                    if job["total"] is None:
                        job["total"] = result["total"]
                        if job["total"] >= report.EXCEL_MAX_ROWS:
                            raise ExportError("筛选结果超过 Excel 行数上限，请缩小时间范围后重试。")
                    elif result["total"] != job["total"]:
                        raise RuntimeError("export snapshot count changed")
                    for item in result["items"]:
                        detail.write(xml_row(job["completed_rows"] + 2, row_values(item, report), report))
                        job["completed_rows"] += 1
                    with queue.locked():
                        queue.save(job)
                    if job["completed_rows"] >= job["total"]:
                        break
                    if not result["items"]:
                        raise RuntimeError("export ended before declared total")
                    page += 1
                last_column = report._column_name(len(COLUMNS))
                detail.write(f'</sheetData><autoFilter ref="A1:{last_column}{job["completed_rows"] + 1}"/></worksheet>'.encode())
            job["completed_at"] = now()
            notes = explanation_rows(job, report)
            # Reuse the standard workbook envelope and styles; only the large
            # detail sheet is replaced with the already streamed worksheet.
            sheets = [report._Worksheet("内容明细", [[""]], [[12]], [23]),
                      report._Worksheet("导出说明", notes,
                                        [[1, 1]] + [[12, 8 if isinstance(row[1], datetime) else 5 if isinstance(row[1], int) else 12] for row in notes[1:]],
                                        [32, 100], frozen_rows=1)]
            envelope = report._xlsx_bytes(sheets, title="发布内容明细", created_at=job["completed_at"])
            with zipfile.ZipFile(io.BytesIO(envelope)) as template:
                for name in template.namelist():
                    if name != "xl/worksheets/sheet1.xml":
                        archive.writestr(name, template.read(name))
        with partial.open("rb") as stream:
            os.fsync(stream.fileno())
        os.replace(partial, destination)
        job["status"] = "succeeded"
        job["filename"] = f'内容明细_{datetime.now(BEIJING):%Y%m%d_%H%M%S}_{job["id"][:8]}.xlsx'
        job["error"] = None
    except Exception as error:
        partial.unlink(missing_ok=True)
        job.update(status="failed", completed_at=now(), filename=None,
                   error=str(error) if isinstance(error, ExportError) else "内容表格生成失败，请重试；如仍失败，请缩小筛选范围。")
    with queue.locked():
        queue.save(job)


def run_worker(queue: Queue, db: Path, api: Any, storage: Any) -> bool:
    with (queue.root / "worker.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        try:
            # Owning flock proves no previous worker remains. Persisted running
            # jobs came from a terminated process and can now safely be retried.
            with queue.locked():
                for job in queue.records():
                    if job["status"] == "running":
                        job.update(status="failed", completed_at=now(), error="上次生成已中断，请重试。", filename=None)
                        (queue.root / "files" / f'{job["id"]}.partial').unlink(missing_ok=True)
                        queue.save(job)
            while True:
                with queue.locked():
                    job = min((item for item in queue.records() if item["status"] == "queued"),
                              key=lambda item: (item["created_at"], item["id"]), default=None)
                    if job is None:
                        # Release under the queue lock, so a concurrent create
                        # cannot observe active=True after the final queue scan.
                        fcntl.flock(lock, fcntl.LOCK_UN)
                        return True
                    job.update(status="running", started_at=now(), completed_rows=0, total=None)
                    queue.save(job)
                previous_handler = signal.signal(signal.SIGALRM, _timeout)
                signal.alarm(JOB_TIMEOUT_SECONDS)
                try:
                    generate(job, queue, db, api, storage)
                finally:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, previous_handler)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _timeout(_signal: int, _frame: Any) -> None:
    raise ExportError("生成用时过长，请缩小筛选范围后重试。", 503)


def fail_pending(queue: Queue) -> None:
    """Bootstrap failures must terminate queued work, never spin forever."""
    with (queue.root / "worker.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        try:
            with queue.locked():
                for job in queue.records():
                    if job["status"] in ("queued", "running"):
                        job.update(status="failed", completed_at=now(), filename=None,
                                   error="导出服务暂时不可用，请稍后重试。")
                        (queue.root / "files" / f'{job["id"]}.partial').unlink(missing_ok=True)
                        queue.save(job)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def command(value: dict[str, Any], queue: Queue, api: Any) -> dict[str, Any]:
    action = value.get("action")
    accepted = {"create": {"action", "owner", "request_id", "filters"},
                "list": {"action", "owner"}, "get": {"action", "owner", "id"}}
    if not isinstance(action, str) or action not in accepted or set(value) != accepted[action]:
        raise ExportError("导出请求格式无效。")
    owner = owner_id(value["owner"])
    if action == "create":
        return queue.create(owner, identifier(value["request_id"]), normalize_filters(value["filters"], api))
    if action == "list":
        return queue.list(owner)
    return queue.get(owner, value["id"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("db", "backend-root", "project-root", "jobs-root"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--worker", action="store_true")
    args = parser.parse_args(argv)
    queue = None
    try:
        if not all(path.is_absolute() for path in (args.db, args.backend_root, args.project_root, args.jobs_root)):
            raise RuntimeError("absolute paths required")
        queue = Queue(args.jobs_root)
        api, storage = search_helper.load_backend(args.backend_root, args.db, args.project_root)
        if args.worker:
            search_helper.verify_database(args.db, args.project_root)
            result = {"processed": run_worker(queue, args.db, api, storage)}
        else:
            value = search_helper.read_request(sys.stdin.buffer)
            result = command(value, queue, api)
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
        return 0
    except (ExportError, search_helper.RequestError) as error:
        if args.worker and queue is not None:
            fail_pending(queue)
        print(json.dumps({"error": {"status": getattr(error, "status", 422), "detail": str(error)}}, ensure_ascii=False))
    except Exception:
        if args.worker and queue is not None:
            fail_pending(queue)
        print(json.dumps({"error": {"status": 503, "detail": "内容导出暂时不可用，请稍后重试。"}}, ensure_ascii=False))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
