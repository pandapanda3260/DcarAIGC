from __future__ import annotations

import asyncio
import hashlib
import hmac
import html
import json
import secrets
import time
from contextlib import asynccontextmanager, suppress
from typing import Any, Callable, Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import Response

from .config import DouyinControlConfig
from .crypto import TokenCipher, read_shared_key
from .provider import DouyinOAuthClient, MockOAuthClient, TokenBundle
from .store import AuthorizationConflict, StateTransitionError, VaultStore
from .tokens import DouyinTokenManager


REQUESTED_SCOPES = ["user_info", "video.list"]
POST_ACTIONS = {
    "/api/douyin/accounts/search": "douyin-accounts-search",
    "/api/douyin/oauth/start": "douyin-oauth-start",
    "/api/douyin/oauth/confirm": "douyin-oauth-confirm",
    "/api/douyin/oauth/reject": "douyin-oauth-reject",
    "/api/douyin/authorizations/unbind": "douyin-authorization-unbind",
}


class AccountSearchPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str = Field(default="", max_length=100)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=50)


class OAuthStartPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account_id: int = Field(ge=1)
    platform_uid: str = Field(pattern=r"^\d{6,24}$")


class UnbindPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    authorization_id: str = Field(min_length=32, max_length=32, pattern=r"^[0-9a-f]+$")
    expected_version: int = Field(ge=1)


def _no_store(response: Response) -> Response:
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


def _request_id(request: Request) -> str:
    value = request.headers.get("x-request-id", "")
    if value and len(value) <= 128:
        return value
    return secrets.token_hex(12)


def _identity(request: Request) -> tuple[str, str]:
    return str(request.state.username), str(request.state.session_binding)


class AccountDirectory:
    def __init__(self, client: httpx.AsyncClient, api_upstream: str) -> None:
        self.client = client
        self.api_upstream = api_upstream

    async def search(
        self, *, query: str, page: int, page_size: int
    ) -> dict[str, Any]:
        try:
            response = await self.client.post(
                f"{self.api_upstream}/api/v8/accounts/search",
                json={
                    "query": query,
                    "platform": "douyin",
                    "page": page,
                    "page_size": page_size,
                },
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise RuntimeError("account_directory_unavailable") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
            raise RuntimeError("account_directory_invalid")

        projected: list[dict[str, Any]] = []
        for item in payload["items"]:
            if not isinstance(item, dict):
                continue
            identities = item.get("platforms")
            if not isinstance(identities, list):
                continue
            douyin_identities = [
                identity
                for identity in identities
                if isinstance(identity, dict)
                and identity.get("platform") == "douyin"
                and str(identity.get("uid", ""))
            ]
            if not douyin_identities:
                continue
            if len(douyin_identities) != 1:
                raise RuntimeError("account_directory_invalid")
            identity = douyin_identities[0]
            uid = str(identity.get("uid", ""))
            if not (6 <= len(uid) <= 24 and uid.isascii() and uid.isdigit()):
                raise RuntimeError("account_directory_invalid")
            try:
                account_id = int(item["id"])
            except (KeyError, TypeError, ValueError) as exc:
                raise RuntimeError("account_directory_invalid") from exc
            projected.append(
                {
                    "account_id": account_id,
                    "operator_name": str(item.get("operator_name") or ""),
                    "enabled": bool(item.get("enabled")),
                    "uid": uid,
                    "nickname": str(identity.get("nickname") or ""),
                }
            )
        try:
            total = int(payload.get("total", len(projected)))
            returned_page = int(payload.get("page", page))
            returned_page_size = int(payload.get("page_size", page_size))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("account_directory_invalid") from exc
        if total < 0 or returned_page < 1 or returned_page_size < 1:
            raise RuntimeError("account_directory_invalid")
        return {
            "items": projected,
            "total": total,
            "page": returned_page,
            "page_size": returned_page_size,
        }

    async def require_account(
        self, *, account_id: int, platform_uid: str
    ) -> dict[str, Any]:
        page = 1
        while True:
            result = await self.search(query=platform_uid, page=page, page_size=100)
            for account in result["items"]:
                if account["account_id"] != account_id or not account["enabled"]:
                    continue
                if hmac.compare_digest(account["uid"], platform_uid):
                    return account
            if page * result["page_size"] >= result["total"]:
                break
            page += 1
            if page > 1000:
                raise RuntimeError("account_directory_invalid")
        raise RuntimeError("account_identity_unavailable")


def _index_html(base_path: str, nonce: str) -> str:
    base_json = json.dumps(base_path, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>抖音账号授权 · Dcar Sentinel</title>
<style nonce="{nonce}">
body{{font-family:Arial,'PingFang SC',sans-serif;margin:0;background:#f3f6f7;color:#13262d}}
main{{max-width:820px;margin:48px auto;padding:0 20px}}section{{background:#fff;padding:24px;border-radius:14px;box-shadow:0 8px 28px #102c3518}}
h1{{margin-top:0}}form{{display:flex;gap:8px}}input{{flex:1;padding:11px;border:1px solid #cad5d8;border-radius:8px}}button{{padding:10px 16px;border:0;border-radius:8px;background:#102c35;color:white;cursor:pointer}}article{{border-top:1px solid #e4eaec;padding:14px 0}}.muted{{color:#68777d}}.error{{color:#a74343}}
</style></head><body><main><section><h1>抖音账号授权</h1>
<p class="muted">选择已建档的抖音账号。授权确认前不会写入正式业务库。</p>
<form id="search"><input id="query" maxlength="100" placeholder="账号名、昵称或 UID"><button>搜索</button></form>
<div id="message" role="status"></div><div id="items"></div><div><button id="previous">上一页</button><button id="next">下一页</button></div></section></main>
<script nonce="{nonce}">"use strict";const BASE={base_json};const items=document.getElementById('items');const message=document.getElementById('message');const previous=document.getElementById('previous');const next=document.getElementById('next');let currentPage=1;
async function search(page){{message.textContent='加载中…';items.textContent='';const query=document.getElementById('query').value;const response=await fetch(BASE+'/api/douyin/accounts/search',{{method:'POST',credentials:'include',headers:{{'Content-Type':'application/json','X-Dcar-Request':'douyin-accounts-search'}},body:JSON.stringify({{query:query,page:page,page_size:50}})}});const data=await response.json();if(!response.ok)throw new Error(data.detail||'账号加载失败');currentPage=data.page;previous.disabled=currentPage<=1;next.disabled=currentPage*data.page_size>=data.total;message.textContent=data.items.length?'':'没有找到可授权的抖音账号';data.items.forEach(account=>{{const row=document.createElement('article');const title=document.createElement('strong');title.textContent=(account.operator_name||'未命名账号')+' · '+(account.nickname||account.uid);const button=document.createElement('button');button.textContent='发起授权';button.style.float='right';button.onclick=async()=>{{button.disabled=true;const start=await fetch(BASE+'/api/douyin/oauth/start',{{method:'POST',credentials:'include',headers:{{'Content-Type':'application/json','X-Dcar-Request':'douyin-oauth-start'}},body:JSON.stringify({{account_id:account.account_id,platform_uid:account.uid}})}});const result=await start.json();if(!start.ok){{button.disabled=false;message.textContent=result.detail||'暂时无法发起授权';return}}window.location.assign(result.authorize_url)}};row.append(title,button);items.append(row)}});}}
function showError(error){{message.className='error';message.textContent=error.message}}document.getElementById('search').addEventListener('submit',event=>{{event.preventDefault();search(1).catch(showError)}});previous.onclick=()=>search(currentPage-1).catch(showError);next.onclick=()=>search(currentPage+1).catch(showError);search(1).catch(showError);
</script></body></html>"""


def _confirm_html(
    base_path: str,
    account: dict[str, Any],
    candidate: dict[str, Any],
    fingerprint: str,
    nonce: str,
) -> str:
    operator_name = html.escape(str(account.get("operator_name") or "未命名账号"))
    nickname = html.escape(str(candidate.get("nickname") or "未获取昵称"))
    uid = html.escape(str(account["uid"]))
    fingerprint_short = html.escape(fingerprint[:12])
    avatar_html = '<p><span class="avatar" aria-hidden="true">抖</span></p>'
    base_json = json.dumps(base_path, ensure_ascii=False)
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>确认抖音授权</title><style nonce="{nonce}">body{{font-family:Arial,'PingFang SC',sans-serif;background:#f3f6f7;color:#13262d}}main{{max-width:620px;margin:60px auto;background:white;padding:28px;border-radius:14px}}.avatar{{display:inline-grid;place-items:center;width:64px;height:64px;border-radius:50%;background:#eef3f4;color:#102c35;font-size:28px;font-weight:700}}button{{padding:11px 18px;margin-right:8px;border:0;border-radius:8px;background:#102c35;color:white}}button.secondary{{background:#68777d}}</style></head><body><main><h1>确认账号授权</h1><p>业务账号：{operator_name}</p><p>抖音昵称：{nickname}</p>{avatar_html}<p>抖音 UID：{uid}</p><p>Open ID 指纹：{fingerprint_short}</p><button id="confirm">确认绑定</button><button class="secondary" id="reject">拒绝并清除</button><p id="message"></p></main><script nonce="{nonce}">"use strict";const BASE={base_json};async function act(path,marker){{const response=await fetch(BASE+path,{{method:'POST',credentials:'include',headers:{{'Content-Type':'application/json','X-Dcar-Request':marker}},body:'{{}}'}});const data=await response.json();if(!response.ok){{document.getElementById('message').textContent=data.detail||'操作失败';return}}window.location.replace(data.redirect_to)}}document.getElementById('confirm').onclick=()=>act('/api/douyin/oauth/confirm','douyin-oauth-confirm');document.getElementById('reject').onclick=()=>act('/api/douyin/oauth/reject','douyin-oauth-reject');</script></body></html>"""


def create_app(
    config: Optional[DouyinControlConfig] = None,
    *,
    api_transport: Optional[httpx.AsyncBaseTransport] = None,
    oauth_transport: Optional[httpx.AsyncBaseTransport] = None,
    clock: Callable[[], float] = time.time,
) -> FastAPI:
    resolved = config or DouyinControlConfig.from_env()
    store = VaultStore(resolved.vault_path)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        edge_key = read_shared_key(resolved.edge_key_path, "Douyin Edge Key")
        machine_key = read_shared_key(
            resolved.machine_key_path, "Douyin Machine Key"
        )
        cipher = TokenCipher(
            resolved.fernet_keyring_path, resolved.open_id_hmac_key_path
        )
        await asyncio.to_thread(store.initialize)
        await asyncio.to_thread(store.expire_due_states, now=int(clock()))
        api_client = httpx.AsyncClient(
            transport=api_transport,
            timeout=httpx.Timeout(10, connect=2, read=10, write=5, pool=2),
            follow_redirects=False,
            trust_env=False,
        )
        oauth_client: Optional[httpx.AsyncClient] = None
        provider: Optional[MockOAuthClient | DouyinOAuthClient] = None
        token_manager: Optional[DouyinTokenManager] = None
        if resolved.authorization_enabled:
            if resolved.provider_mode == "mock":
                oauth_client = httpx.AsyncClient(
                    transport=oauth_transport,
                    timeout=httpx.Timeout(10, connect=2, read=10, write=5, pool=2),
                    follow_redirects=False,
                    trust_env=False,
                )
                provider = MockOAuthClient(resolved, oauth_client, clock=clock)
            else:
                provider = DouyinOAuthClient(resolved, clock=clock)
                token_manager = DouyinTokenManager(
                    store, cipher, provider, clock=clock
                )

        async def cleanup_expired_states() -> None:
            while True:
                await asyncio.sleep(resolved.cleanup_interval_seconds)
                await asyncio.to_thread(store.expire_due_states, now=int(clock()))

        cleanup_task = asyncio.create_task(cleanup_expired_states())
        application.state.edge_key = edge_key
        application.state.machine_key = machine_key
        application.state.cipher = cipher
        application.state.store = store
        application.state.api_client = api_client
        application.state.accounts = AccountDirectory(api_client, resolved.api_upstream)
        application.state.provider = provider
        application.state.token_manager = token_manager
        try:
            yield
        finally:
            cleanup_task.cancel()
            with suppress(asyncio.CancelledError):
                await cleanup_task
            await api_client.aclose()
            if oauth_client is not None:
                await oauth_client.aclose()
            if isinstance(provider, DouyinOAuthClient):
                await provider.aclose()

    application = FastAPI(
        title="Dcar Douyin control plane",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        redirect_slashes=False,
        lifespan=lifespan,
    )

    @application.middleware("http")
    async def trust_boundary(request: Request, call_next):
        path = request.url.path
        if path.startswith("/internal/v1/"):
            supplied = request.headers.get("x-dcar-machine-key", "")
            if (
                request.headers.get("x-dcar-edge-key")
                or request.headers.get("x-dcar-authenticated-user")
                or request.headers.get("x-dcar-session-binding")
                or not hmac.compare_digest(supplied, request.app.state.machine_key)
            ):
                return _no_store(Response(status_code=403))
        else:
            supplied = request.headers.get("x-dcar-edge-key", "")
            username = request.headers.get("x-dcar-authenticated-user", "")
            binding = request.headers.get("x-dcar-session-binding", "")
            if (
                request.headers.get("x-dcar-machine-key")
                or not hmac.compare_digest(supplied, request.app.state.edge_key)
                or not username
                or len(username) > 128
                or username == "temporary-bypass"
                or len(binding) != 64
                or any(character not in "0123456789abcdef" for character in binding)
            ):
                return _no_store(Response(status_code=403))
            request.state.username = username
            request.state.session_binding = binding
            if request.method == "POST":
                expected_action = POST_ACTIONS.get(path)
                if expected_action is None or not hmac.compare_digest(
                    request.headers.get("x-dcar-verified-action", ""),
                    expected_action,
                ):
                    return _no_store(Response(status_code=403))
        response = await call_next(request)
        return _no_store(response)

    @application.get("/internal/v1/health")
    async def internal_health(request: Request) -> dict[str, Any]:
        health = await asyncio.to_thread(request.app.state.store.healthcheck)
        return {"status": "ok", "vault": health}

    @application.get("/douyin", response_class=HTMLResponse)
    async def index() -> HTMLResponse:
        nonce = secrets.token_urlsafe(18)
        response = HTMLResponse(_index_html(resolved.public_base_path, nonce))
        response.headers["Content-Security-Policy"] = (
            f"default-src 'self'; style-src 'nonce-{nonce}'; "
            f"script-src 'nonce-{nonce}'; img-src 'self' data:; "
            "connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"
        )
        response.headers["X-Frame-Options"] = "DENY"
        return response

    @application.post("/api/douyin/accounts/search")
    async def account_search(
        request: Request, payload: AccountSearchPayload
    ) -> Response:
        if request.url.query:
            return JSONResponse(
                {"detail": "账号搜索条件必须放在 JSON 请求体中"}, status_code=422
            )
        try:
            return JSONResponse(
                await request.app.state.accounts.search(
                    query=payload.query,
                    page=payload.page,
                    page_size=payload.page_size,
                )
            )
        except RuntimeError:
            return JSONResponse(
                {"detail": "账号目录暂时不可用，请稍后重试"}, status_code=502
            )

    @application.post("/api/douyin/oauth/start")
    async def oauth_start(request: Request, payload: OAuthStartPayload) -> Response:
        if not resolved.authorization_enabled:
            return JSONResponse(
                {"detail": "抖音授权功能尚未启用"}, status_code=409
            )
        try:
            await request.app.state.accounts.require_account(
                account_id=payload.account_id,
                platform_uid=payload.platform_uid,
            )
        except RuntimeError:
            return JSONResponse(
                {"detail": "所选抖音账号已失效，请重新选择"}, status_code=409
            )
        username, session_binding = _identity(request)
        state = secrets.token_urlsafe(32)
        state_digest = hashlib.sha256(state.encode("utf-8")).hexdigest()
        now = int(clock())
        await asyncio.to_thread(
            request.app.state.store.create_state,
            state_digest=state_digest,
            bound_username=username,
            session_binding=session_binding,
            account_id=payload.account_id,
            platform_uid=payload.platform_uid,
            scopes=REQUESTED_SCOPES,
            expires_at=now + resolved.state_ttl_seconds,
            request_id=_request_id(request),
            now=now,
        )
        provider: Optional[MockOAuthClient | DouyinOAuthClient] = (
            request.app.state.provider
        )
        if provider is None:
            return JSONResponse(
                {"detail": "抖音授权功能尚未启用"}, status_code=409
            )
        authorize_url = provider.authorization_url(state, REQUESTED_SCOPES)
        return JSONResponse({"authorize_url": authorize_url})

    @application.get("/oauth/douyin/callback")
    async def oauth_callback(
        request: Request,
        code: Optional[str] = None,
        state: Optional[str] = None,
    ) -> Response:
        if not resolved.authorization_enabled:
            return RedirectResponse(
                resolved.public_route("/douyin?notice=oauth-disabled"),
                status_code=303,
            )
        if (
            code is None
            or state is None
            or not 1 <= len(code) <= 512
            or not 20 <= len(state) <= 256
        ):
            return RedirectResponse(
                resolved.public_route("/douyin?notice=oauth-invalid-response"),
                status_code=303,
            )
        username, session_binding = _identity(request)
        request_id = _request_id(request)
        state_digest = hashlib.sha256(state.encode("utf-8")).hexdigest()
        try:
            await asyncio.to_thread(
                request.app.state.store.begin_exchange,
                state_digest,
                username,
                session_binding,
                request_id=request_id,
                now=int(clock()),
            )
        except StateTransitionError:
            return RedirectResponse(
                resolved.public_route("/douyin?notice=oauth-state-invalid"),
                status_code=303,
            )
        except Exception:
            return RedirectResponse(
                resolved.public_route("/douyin?notice=oauth-failed"),
                status_code=303,
            )
        try:
            provider = request.app.state.provider
            if provider is None:
                raise RuntimeError("mock provider unavailable")
            bundle: TokenBundle = await provider.exchange_code(code)
            try:
                profile = await provider.userinfo(bundle)
            except RuntimeError:
                profile = {"nickname": "", "avatar": ""}
            open_id_fingerprint = request.app.state.cipher.open_id_fingerprint(
                bundle.open_id
            )
            candidate = {
                "open_id": bundle.open_id,
                "access_token": bundle.access_token,
                "refresh_token": bundle.refresh_token,
                "access_expires_at": bundle.access_expires_at,
                "refresh_expires_at": bundle.refresh_expires_at,
                "scopes": bundle.scopes,
                "nickname": str(profile.get("nickname") or ""),
                "avatar": str(profile.get("avatar") or ""),
            }
            ciphertext = request.app.state.cipher.encrypt(
                state_digest, "oauth_candidate", candidate
            )
            await asyncio.to_thread(
                request.app.state.store.store_candidate,
                state_digest=state_digest,
                ciphertext=ciphertext,
                open_id_fingerprint=open_id_fingerprint,
                confirmation_expires_at=(
                    int(clock()) + resolved.confirmation_ttl_seconds
                ),
                request_id=request_id,
                now=int(clock()),
            )
        except Exception:
            with suppress(Exception):
                await asyncio.to_thread(
                    request.app.state.store.fail_state,
                    state_digest,
                    "token_exchange_failed",
                    request_id=request_id,
                    actor=username,
                    now=int(clock()),
                )
            return RedirectResponse(
                resolved.public_route("/douyin?notice=oauth-failed"),
                status_code=303,
            )
        return RedirectResponse(
            resolved.public_route("/douyin/confirm"), status_code=303
        )

    @application.get("/douyin/confirm", response_class=HTMLResponse)
    async def confirmation_page(request: Request) -> Response:
        username, session_binding = _identity(request)
        pending = await asyncio.to_thread(
            request.app.state.store.current_pending,
            username,
            session_binding,
            now=int(clock()),
        )
        if pending is None:
            return RedirectResponse(resolved.public_route("/douyin"), status_code=303)
        try:
            candidate = request.app.state.cipher.decrypt(
                str(pending["state_digest"]),
                "oauth_candidate",
                bytes(pending["candidate_ciphertext"]),
            )
            account = await request.app.state.accounts.require_account(
                account_id=int(pending["account_id"]),
                platform_uid=str(pending["platform_uid"]),
            )
        except (RuntimeError, TypeError, ValueError):
            return RedirectResponse(
                resolved.public_route("/douyin?notice=confirmation-invalid"),
                status_code=303,
            )
        nonce = secrets.token_urlsafe(18)
        response = HTMLResponse(
            _confirm_html(
                resolved.public_base_path,
                account,
                candidate,
                str(pending["candidate_open_id_fingerprint"]),
                nonce,
            )
        )
        response.headers["Content-Security-Policy"] = (
            f"default-src 'self'; style-src 'nonce-{nonce}'; "
            f"script-src 'nonce-{nonce}'; connect-src 'self'; "
            "img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'"
        )
        response.headers["X-Frame-Options"] = "DENY"
        return response

    @application.post("/api/douyin/oauth/confirm")
    async def oauth_confirm(request: Request) -> Response:
        if not resolved.authorization_enabled:
            return JSONResponse(
                {"detail": "抖音授权功能尚未启用"}, status_code=409
            )
        username, session_binding = _identity(request)
        pending = await asyncio.to_thread(
            request.app.state.store.current_pending,
            username,
            session_binding,
            now=int(clock()),
        )
        if pending is None:
            return JSONResponse({"detail": "没有待确认的授权"}, status_code=409)
        try:
            account = await request.app.state.accounts.require_account(
                account_id=int(pending["account_id"]),
                platform_uid=str(pending["platform_uid"]),
            )
            del account
            state_digest = str(pending["state_digest"])
            candidate = request.app.state.cipher.decrypt(
                state_digest,
                "oauth_candidate",
                bytes(pending["candidate_ciphertext"]),
            )
            open_id_fingerprint = request.app.state.cipher.open_id_fingerprint(
                str(candidate["open_id"])
            )
            result = await asyncio.to_thread(
                request.app.state.store.confirm_authorization,
                state_digest=state_digest,
                bound_username=username,
                session_binding=session_binding,
                open_id_fingerprint=open_id_fingerprint,
                candidate=candidate,
                cipher=request.app.state.cipher,
                request_id=_request_id(request),
                now=int(clock()),
            )
        except AuthorizationConflict as exc:
            return JSONResponse(
                {"detail": "该抖音账号或业务账号已被其他授权占用", "reason": exc.reason_code},
                status_code=409,
            )
        except (RuntimeError, KeyError, TypeError, ValueError):
            return JSONResponse({"detail": "待确认授权已失效"}, status_code=409)
        return JSONResponse(
            {
                "authorization": result,
                "redirect_to": resolved.public_route("/douyin"),
            }
        )

    @application.post("/api/douyin/oauth/reject")
    async def oauth_reject(request: Request) -> Response:
        username, session_binding = _identity(request)
        rejected = await asyncio.to_thread(
            request.app.state.store.reject_current,
            username,
            session_binding,
            request_id=_request_id(request),
            now=int(clock()),
        )
        if not rejected:
            return JSONResponse({"detail": "没有待处理的授权"}, status_code=409)
        return JSONResponse({"redirect_to": resolved.public_route("/douyin")})

    @application.get("/api/douyin/authorizations")
    async def authorizations(request: Request) -> dict[str, Any]:
        username, _binding = _identity(request)
        items = await asyncio.to_thread(
            request.app.state.store.list_authorizations, username
        )
        return {"items": items}

    @application.post("/api/douyin/authorizations/unbind")
    async def authorization_unbind(
        request: Request, payload: UnbindPayload
    ) -> Response:
        username, _binding = _identity(request)
        try:
            unbound = await asyncio.to_thread(
                request.app.state.store.unbind,
                bound_username=username,
                authorization_id=payload.authorization_id,
                expected_version=payload.expected_version,
                request_id=_request_id(request),
                now=int(clock()),
            )
        except AuthorizationConflict:
            return JSONResponse(
                {"detail": "授权状态已变化，请刷新后重试"}, status_code=409
            )
        if not unbound:
            return JSONResponse({"detail": "未找到可解绑的授权"}, status_code=404)
        return JSONResponse({"status": "unbound"})

    return application


app = create_app()
