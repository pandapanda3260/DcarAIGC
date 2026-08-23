from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from urllib.parse import parse_qs, urlencode

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse


@dataclass
class MockDouyinState:
    client_key: str
    client_secret: str
    open_id: str = "mock-open-id-a"
    nickname: str = "Mock 抖音账号"
    avatar: str = "https://avatar.example/mock.png"
    token_failure: bool = False
    userinfo_failure: bool = False
    token_calls: int = 0
    userinfo_calls: int = 0
    codes: dict[str, dict[str, str]] = field(default_factory=dict)


def create_mock_douyin_oauth(state: MockDouyinState) -> FastAPI:
    app = FastAPI(redirect_slashes=False)

    @app.get("/platform/oauth/connect/")
    async def authorize(request: Request) -> RedirectResponse:
        query = request.query_params
        if (
            query.get("client_key") != state.client_key
            or query.get("response_type") != "code"
            or not query.get("state")
            or not query.get("redirect_uri")
            or not query.get("scope")
        ):
            return RedirectResponse("/mock-invalid-request", status_code=303)
        code = secrets.token_urlsafe(24)
        state.codes[code] = {
            "open_id": state.open_id,
            "scope": str(query["scope"]),
        }
        separator = "&" if "?" in str(query["redirect_uri"]) else "?"
        return RedirectResponse(
            f"{query['redirect_uri']}{separator}"
            + urlencode({"code": code, "state": str(query["state"])}),
            status_code=303,
        )

    @app.post("/oauth/access_token/")
    async def access_token(request: Request) -> JSONResponse:
        state.token_calls += 1
        form = parse_qs((await request.body()).decode("utf-8"), keep_blank_values=True)
        code = form.get("code", [""])[0]
        issued = state.codes.pop(code, None)
        if (
            state.token_failure
            or form.get("client_key", [""])[0] != state.client_key
            or form.get("client_secret", [""])[0] != state.client_secret
            or form.get("grant_type", [""])[0] != "authorization_code"
            or issued is None
        ):
            return JSONResponse({"data": {"error_code": 10008}}, status_code=400)
        return JSONResponse(
            {
                "data": {
                    "error_code": 0,
                    "open_id": issued["open_id"],
                    "access_token": f"access-{secrets.token_urlsafe(24)}",
                    "refresh_token": f"refresh-{secrets.token_urlsafe(24)}",
                    "expires_in": 15 * 24 * 60 * 60,
                    "refresh_expires_in": 30 * 24 * 60 * 60,
                    "scope": issued["scope"],
                }
            }
        )

    @app.post("/oauth/userinfo/")
    async def userinfo(request: Request) -> JSONResponse:
        state.userinfo_calls += 1
        form = parse_qs((await request.body()).decode("utf-8"), keep_blank_values=True)
        if state.userinfo_failure or not form.get("access_token", [""])[0]:
            return JSONResponse({"data": {"error_code": 10010}}, status_code=400)
        return JSONResponse(
            {
                "data": {
                    "error_code": 0,
                    "open_id": form.get("open_id", [""])[0],
                    "nickname": state.nickname,
                    "avatar": state.avatar,
                }
            }
        )

    return app
