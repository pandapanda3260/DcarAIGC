"""Disposable API upstream for the auth-integration browser smoke.

This process never opens the Dcar application database.  The real auth gateway
proxies authenticated API requests here so the browser smoke can prove the
gateway/header boundary without touching a running local or production API.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


app = FastAPI()


@app.api_route(
    "/{path:path}",
    methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
)
async def stub(request: Request, path: str) -> JSONResponse:
    user = request.headers.get("x-dcar-authenticated-user", "")
    if path == "api/v8/health":
        return JSONResponse({"status": "ok", "mode": "auth_integration_smoke"})
    if path == "api/v8/overview":
        # Enough of the real response contract for OverviewPage/AppShell to
        # render when the gateway redirects a non-admin away from /users.
        return JSONResponse(
            {
                "windows": {},
                "data_quality": {
                    "missing_published_at": 0,
                    "duplicate_fingerprint_coverage": 100,
                    "confirmed_duplicate_count": 0,
                    "duplicate_calibration_ready": True,
                },
            }
        )
    if path == "api/v8/smoke":
        return JSONResponse({"upstream": "api", "user": user})
    return JSONResponse({"items": [], "total": 0, "user": user})
