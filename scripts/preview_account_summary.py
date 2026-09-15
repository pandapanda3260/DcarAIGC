#!/usr/bin/env python3
"""Loopback-only, read-only preview of an isolated account DB and local web code.

This development harness never uses the installed auth database or writer API.
It exposes a local preview session, so do not bind or proxy it onto a network.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import socket


def configure_read_only_paths(database: Path, *, code_root: Path, evidence_root: Path) -> Path:
    """Select existing evidence explicitly without acquiring Writer authority.

    runtime_paths requires both roots to resolve legacy relative raw paths.
    Merely setting DCAR_PROJECT_ROOT silently retains the checkout root. These
    environment values select paths only; this harness never installs a receipt,
    acquires a Writer lease, enables a scheduler or grants provider access.
    """
    for path in (code_root, evidence_root):
        if not path.is_absolute() or not path.is_dir() or path != path.resolve(strict=True):
            raise ValueError("Code and evidence roots must be existing absolute non-symlink directories")
    if code_root != evidence_root and (
        code_root.is_relative_to(evidence_root) or evidence_root.is_relative_to(code_root)
    ):
        raise ValueError("Separate code and evidence roots must be independent directories")
    os.environ["DCAR_V8_DB"] = str(database)
    os.environ["DCAR_PROJECT_ROOT"] = str(evidence_root)
    if code_root == evidence_root:
        os.environ.pop("DCAR_WRITER_SOURCE_ROOT", None)
    else:
        os.environ["DCAR_WRITER_SOURCE_ROOT"] = str(code_root)
    os.environ["DCAR_READ_ONLY"] = "1"
    os.environ["DCAR_SCHEDULER_ENABLED"] = "0"
    os.environ["DCAR_STARTUP_CATCHUP_ENABLED"] = "0"
    os.environ.pop("DCAR_DAILY_CAPTURE_RECONCILE_FROM", None)
    return evidence_root


def verify_read_only_evidence_root(evidence_root: Path) -> None:
    """Fail at startup if any imported legacy reader retained another root."""
    from v8 import artifact_paths, raw_archive, storage
    if any(module.PROJECT_ROOT != evidence_root for module in (artifact_paths, raw_archive, storage)):
        raise ValueError("Evidence readers were imported before the explicit roots were configured")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--evidence-root", type=Path, required=True,
                        help="Existing local root for database-relative raw evidence; read only")
    parser.add_argument("--port", type=int, default=4183)
    parser.add_argument("--web-port", type=int, default=4184)
    args = parser.parse_args()
    if args.port in {4173, 4174, 8765, 8766, 8767} or args.web_port in {4173, 4174, 8765, 8766, 8767} or args.port == args.web_port:
        parser.error("Use dedicated preview ports")
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "src" / "dcar_eval"))
    # Even importing v8.runtime_database executes v8.__init__ and imports
    # storage, so roots must be configured before the first v8 import.
    evidence_root = configure_read_only_paths(args.db.resolve(strict=True), code_root=root, evidence_root=args.evidence_root)
    from v8.runtime_database import resolve_isolated_candidate
    access = resolve_isolated_candidate(args.db)
    # Set explicit paths before importing runtime modules that resolve defaults.
    # No scheduler, writer lock or publisher is started by this process.
    from v8.api import ApiConfig, create_app
    from fastapi.responses import JSONResponse, Response
    import httpx
    import uvicorn
    verify_read_only_evidence_root(evidence_root)

    # Fail closed even if a future read path tries to call an external provider.
    original_connect = socket.socket.connect
    def loopback_connect(sock, address):
        if isinstance(address, tuple) and address[0] not in {"127.0.0.1", "::1"}:
            raise OSError("Account preview permits only loopback connections")
        return original_connect(sock, address)
    socket.socket.connect = loopback_connect

    config = ApiConfig(db_path=access.database, reports_root=access.database.parent / "preview-reports",
                       legacy_db_path=access.database.parent / "unused-legacy.sqlite3",
                       operator_freeze_lock=access.database.parent / "unused-preview-freeze.lock",
                       writer_lock=access.database.parent / "unused-preview-writer.lock",
                       read_only=True, scheduler_enabled=False, startup_catchup_enabled=False,
                       account_capture_live_status=True,
                       project_root=evidence_root)
    app = create_app(config)
    @app.middleware("http")
    async def preview_boundary(request, call_next):
        path = request.url.path
        if request.headers.get("host") not in {f"127.0.0.1:{args.port}", f"localhost:{args.port}"}:
            return JSONResponse({"detail": "Local preview host required"}, status_code=403)
        if path == "/auth/session" and request.method == "GET":
            response = JSONResponse({"authenticated": True, "username": "本地只读验收", "role": "admin"})
        elif path == "/api/v8/health" and request.method == "GET":
            # Existing evidence is read at its explicit root; this still is
            # account-preview health, not a full media/Writer health check.
            response = JSONResponse({"status": "ok", "mode": "isolated_account_preview", "read_only": True,
                "scheduler_enabled": False, "database_path": str(access.database),
                "code_root": str(root), "evidence_root": str(evidence_root),
                "capture_status_source": "current_local_evidence",
                "runtime_database_identity": access.health_identity()})
        elif path == "/api/v8/accounts/search" and request.method == "POST":
            response = await call_next(request)
        elif path.startswith(("/api/", "/auth/", "/workbench-api/")) or request.method not in {"GET", "HEAD"}:
            response = JSONResponse({"detail": "本地账号验收仅开放读取账号；其他操作未开放。", "code": "local_preview_read_only"}, status_code=403)
        else:
            async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=60) as client:
                target = f"http://127.0.0.1:{args.web_port}" + request.url.path
                if request.url.query:
                    target += "?" + request.url.query
                upstream = await client.request(request.method, target, headers={
                    **{key: value for key, value in request.headers.items() if key.lower() not in {"host", "connection", "accept-encoding", "cookie"}},
                    "accept-encoding": "identity",
                })
                response = Response(upstream.content, upstream.status_code, headers={key: value for key, value in upstream.headers.items() if key.lower() not in {"content-encoding", "transfer-encoding", "connection", "content-length", "set-cookie"}})
        response.headers["Cache-Control"] = "no-store"
        response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self' ws://127.0.0.1:" + str(args.web_port) + "; frame-src 'none'; object-src 'none'"
        response.headers["X-Dcar-Preview"] = "isolated-local-read-only"
        return response
    uvicorn.run(app, host="127.0.0.1", port=args.port, access_log=False)


if __name__ == "__main__":
    main()
