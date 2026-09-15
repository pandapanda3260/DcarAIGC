"""Loopback-only ASGI reader for the live SQLite WAL database.

Run with ``python -m v8.read_api --port 8768``. This factory never starts the
writer API lifespan, scheduler, paid providers, migrations or writer lock.
"""
import argparse
import hmac
import ipaddress
import json
import os
import re
import sqlite3
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from time import monotonic

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, StrictBool

from .read_cache import BoundedReadCache, DatabaseRevision, ReadCacheBusy
from .read_contract import READ_DOMAINS, READ_KEY_HEADER, READ_SCOPE_HEADER
from .runtime_database import DatabaseAccessMode, resolve_installed_database_access
from .storage import connect, live_wal_read_only_connections, require_schema_compatibility


@dataclass(frozen=True)
class ReadApiConfig:
    db_path: Path
    project_root: Path
    key_path: Path
    ttl_seconds: float = 30
    max_entries: int = 128
    max_bytes: int = 16 * 1024 * 1024
    # Only explicit in-process fixture injection can disable installed lineage
    # validation; there is deliberately no environment-variable bypass.
    test_fixture: bool = False

    @classmethod
    def from_env(cls):
        required = ("DCAR_V8_DB", "DCAR_PROJECT_ROOT", "DCAR_READ_API_KEY_FILE")
        if any(not os.environ.get(name) for name in required):
            raise RuntimeError("reader requires DCAR_V8_DB, DCAR_PROJECT_ROOT and DCAR_READ_API_KEY_FILE")
        return cls(*(Path(os.environ[name]) for name in required))

    def validate(self):
        if not self.test_fixture:
            return resolve_installed_database_access(
                DatabaseAccessMode.FORMAL_READ, database=self.db_path, project_root=self.project_root,
            )
        if not self.db_path.is_file():
            raise RuntimeError("missing test database")
        return None


def create_app(config: ReadApiConfig | None = None) -> FastAPI:
    config = config or ReadApiConfig.from_env()
    # Importing pure read functions also constructs the legacy app object;
    # its lifespan is never entered and its router is never mounted here.
    from . import api
    from .account_listing import account_detail, search_accounts

    class AccountReadSearch(api.AccountSearchRequest):
        compact: StrictBool = False

    class InvalidateRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")
        domains: list[str]

    def validate_database():
        access = config.validate()
        with live_wal_read_only_connections(), connect(config.db_path, read_only=True) as connection:
            require_schema_compatibility(connection, supported_versions=frozenset({19, 20, 21, 22, 23, 24}))
        return access

    @asynccontextmanager
    async def lifespan(application):
        access = validate_database()
        key = config.key_path.read_text(encoding="utf-8").strip()
        if not 32 <= len(key) <= 512 or not key.isascii() or any(ord(character) < 33 for character in key):
            raise RuntimeError("reader gateway key must contain 32 to 512 non-whitespace ASCII characters")
        revision = DatabaseRevision(config.db_path, validate_database)
        revision._open()
        application.state.gateway_key = key
        application.state.database_access = access
        application.state.cache = BoundedReadCache(revision, ttl_seconds=config.ttl_seconds,
            max_entries=config.max_entries, max_bytes=config.max_bytes)
        try:
            yield
        finally:
            revision.close()

    application = FastAPI(title="DCar isolated read API", docs_url=None, redoc_url=None,
                          openapi_url=None, lifespan=lifespan)
    application.add_middleware(api.JSONGZipMiddleware, minimum_size=1_000, compresslevel=5)

    @application.middleware("http")
    async def reader_boundary(request: Request, call_next):
        started = monotonic()
        request_id = uuid.uuid4().hex
        private_headers = {"Cache-Control": "private, no-store", "X-Dcar-Read-Request-Id": request_id}
        host = request.client.host if request.client else ""
        try:
            loopback = ipaddress.ip_address(host).is_loopback
        except ValueError:
            loopback = config.test_fixture and host == "testclient"
        expected = getattr(request.app.state, "gateway_key", "")
        supplied = request.headers.get(READ_KEY_HEADER, "")
        if not loopback or not expected or not hmac.compare_digest(supplied.encode(), expected.encode()):
            return JSONResponse({"detail": "reader gateway authorization required"}, status_code=403,
                                headers=private_headers)
        if request.url.path.startswith("/api/") and not request.headers.get(READ_SCOPE_HEADER):
            return JSONResponse({"detail": "reader authorization scope missing"}, status_code=403,
                                headers=private_headers)
        request.state.read_timings = {}
        response = await call_next(request)
        timings = request.state.read_timings
        timings["total"] = (monotonic() - started) * 1000
        response.headers["Server-Timing"] = ", ".join(
            f"{name};dur={value:.2f}" for name, value in timings.items()
            if re.fullmatch(r"[a-z_]+", name) and isinstance(value, (float, int))
        ) + f', cache;desc="{timings.get("cache", "bypass")}"'
        response.headers["Cache-Control"] = "private, no-store"
        response.headers["X-Dcar-Read-Request-Id"] = request_id
        response.headers["X-Dcar-Read-Service"] = "isolated-live-wal-v1"
        return response

    def cached(request: Request, domain: str, parameters: dict, loader):
        key = (request.headers[READ_SCOPE_HEADER], request.url.path,
               json.dumps(parameters, sort_keys=True, separators=(",", ":")))

        def read():
            with live_wal_read_only_connections():
                return loader()

        try:
            result = request.app.state.cache.get(domain, key, read, request.state.read_timings)
        except (ReadCacheBusy, TimeoutError) as error:
            raise HTTPException(status_code=503, detail="读取请求较多，请稍后重试。", headers={"Retry-After": "1"}) from error
        except sqlite3.OperationalError as error:
            raise HTTPException(status_code=503, detail="数据暂时无法读取，请稍后重试。") from error
        except (api.TaxonomyError, api.SpuAudienceError) as error:
            raise HTTPException(status_code=409, detail="数据暂时无法读取，请刷新页面后重试。") from error
        return Response(result.body, media_type="application/json", headers={"X-Dcar-Data-Revision": result.revision})

    @application.get("/internal/read/health")
    def health(request: Request):
        revision = request.app.state.cache.revision
        with revision.lock:
            revision._open()
            revision.connection.execute("SELECT 1").fetchone()
        return {"status": "ready", "service": "dcar-read-api", "read_only": True,
                "scheduler_enabled": False, "database_epoch": revision.epoch}

    @application.post("/internal/read/invalidate")
    def invalidate(request: Request, payload: InvalidateRequest):
        domains = frozenset(payload.domains)
        if not domains or not domains <= READ_DOMAINS:
            raise HTTPException(status_code=422, detail="invalid read domains")
        request.app.state.cache.invalidate(domains)
        return {"invalidated": sorted(domains)}

    @application.post("/api/v8/accounts/search")
    def accounts(request: Request, payload: AccountReadSearch):
        params = payload.model_dump()
        compact = params.pop("compact")
        search = api.AccountSearchRequest(**params)
        return cached(request, "accounts", {**params, "compact": compact}, lambda: search_accounts(
            search, db_path=config.db_path, read_only=True, compact=compact,
            timings=request.state.read_timings))

    @application.get("/api/v8/accounts/directory/{directory_row_id}")
    def account(request: Request, directory_row_id: int):
        if directory_row_id < 1:
            raise HTTPException(status_code=422, detail="invalid directory id")

        def load():
            result = account_detail(directory_row_id, db_path=config.db_path, read_only=True,
                                    timings=request.state.read_timings)
            if result is None:
                raise HTTPException(status_code=404, detail="账号不存在")
            return result

        return cached(request, "accounts", {"directory_id": directory_row_id}, load)

    @application.post("/api/v8/contents/search")
    def contents(request: Request, payload: api.ContentSearchRequest):
        return cached(request, "contents", payload.model_dump(), lambda: api._content_search(
            payload, db_path=config.db_path, read_only=True, local_media_read_only=False,
            query_timeout_seconds=20))

    @application.get("/api/v8/overview")
    def overview(request: Request):
        return cached(request, "overview", {}, lambda: api.v8_overview(
            config.db_path, read_only=True, timings=request.state.read_timings))

    @application.get("/api/v8/selling-points")
    def selling_points(request: Request):
        return cached(request, "selling-points", {}, lambda: api._selling_point_list(db_path=config.db_path, read_only=True))

    @application.get("/api/v8/spu-audience/assets")
    def spu_assets(request: Request):
        return cached(request, "spu", {}, lambda: api.list_spu_audience_assets(db_path=config.db_path, read_only=True))

    @application.get("/api/v8/spu-audience/stats")
    def spu_stats(request: Request, window: str = "all", platform: str = ""):
        if window not in api.STAT_WINDOWS or (platform and platform not in api.STAT_PLATFORMS):
            raise HTTPException(status_code=422, detail="所选统计条件无效，请刷新页面后重试。")
        return cached(request, "spu", {"window": window, "platform": platform}, lambda: api.build_spu_audience_stats(
            db_path=config.db_path, window=window, platform=platform, read_only=True))

    return application


def main():
    parser = argparse.ArgumentParser(description="Run the isolated, loopback-only DCar reader")
    parser.add_argument("--port", type=int, default=8768)
    args = parser.parse_args()
    import uvicorn
    uvicorn.run("v8.read_api:create_app", factory=True, host="127.0.0.1", port=args.port, workers=1,
                proxy_headers=False)


if __name__ == "__main__":
    main()
