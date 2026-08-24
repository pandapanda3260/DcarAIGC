from __future__ import annotations

import asyncio
import hashlib
import hmac
import secrets
import time
from contextlib import asynccontextmanager, suppress
from typing import Any, Callable, Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import Response

from .config import DouyinControlConfig
from .crypto import TokenCipher, read_shared_key
from .provider import (
    DouyinOAuthClient,
    DouyinProviderError,
    MockOAuthClient,
    TokenBundle,
)
from .store import AuthorizationConflict, StateTransitionError, VaultStore
from .tokens import (
    DouyinTokenManager,
    ReauthorizationRequired,
    TokenLifecycleError,
)


REQUESTED_SCOPES = ["user_info", "video.list"]
POST_ACTIONS = {
    "/api/douyin/accounts/search": "douyin-accounts-search",
    "/api/douyin/oauth/start": "douyin-oauth-start",
    "/api/douyin/authorizations/match": "douyin-authorization-match",
    "/api/douyin/authorizations/reauthorize": "douyin-authorization-reauthorize",
    "/api/douyin/authorizations/unbind": "douyin-authorization-unbind",
}


class AccountSearchPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str = Field(default="", max_length=100)
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=50)


class OAuthStartPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AuthorizationMatchPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    authorization_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    account_id: int = Field(ge=1)
    platform_uid: str = Field(pattern=r"^\d{6,24}$")
    expected_version: int = Field(ge=1)


class ReauthorizePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    authorization_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    expected_version: int = Field(ge=1)


class UnbindPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    authorization_id: str = Field(min_length=32, max_length=32, pattern=r"^[0-9a-f]+$")
    expected_version: int = Field(ge=1)


class VideoListPagePayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    authorization_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    cursor: int = Field(ge=0, le=2**63 - 1)
    count: int = Field(ge=1, le=20)


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

    async def resolve_douyin_videos(self, video_ids: list[str]) -> dict[str, Any]:
        try:
            response = await self.client.post(
                f"{self.api_upstream}/api/v8/accounts/resolve-douyin-videos",
                json={"video_ids": video_ids},
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise RuntimeError("account_resolver_unavailable") from exc
        if not isinstance(payload, dict) or payload.get("status") not in {
            "matched",
            "unmatched",
            "ambiguous",
        }:
            raise RuntimeError("account_resolver_invalid")
        if payload["status"] == "matched":
            matched = payload.get("matched_account")
            if not isinstance(matched, dict):
                raise RuntimeError("account_resolver_invalid")
            try:
                account_id = int(matched["account_id"])
                platform_uid = str(matched["platform_uid"])
            except (KeyError, TypeError, ValueError) as exc:
                raise RuntimeError("account_resolver_invalid") from exc
            if account_id < 1 or not (
                6 <= len(platform_uid) <= 24
                and platform_uid.isascii()
                and platform_uid.isdigit()
            ):
                raise RuntimeError("account_resolver_invalid")
            return {
                "status": "matched",
                "account_id": account_id,
                "platform_uid": platform_uid,
            }
        return {"status": str(payload["status"])}


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

    @application.get("/internal/v1/authorizations")
    async def internal_authorizations(request: Request) -> dict[str, Any]:
        items = await asyncio.to_thread(
            request.app.state.store.list_active_authorizations
        )
        return {
            "items": [
                {
                    "authorization_id": item["id"],
                    **{key: value for key, value in item.items() if key != "id"},
                }
                for item in items
            ]
        }

    @application.post("/internal/v1/video-list/page")
    async def internal_video_list_page(
        request: Request, payload: VideoListPagePayload
    ) -> Response:
        token_manager = request.app.state.token_manager
        if token_manager is None:
            return JSONResponse(
                {"detail": "douyin_provider_unavailable"}, status_code=503
            )
        try:
            page = await token_manager.video_list_page(
                payload.authorization_id,
                cursor=payload.cursor,
                count=payload.count,
                actor="machine:douyin-openapi-sync",
                request_id=_request_id(request),
            )
        except ReauthorizationRequired as exc:
            return JSONResponse(
                {
                    "detail": "douyin_reauthorization_required",
                    "reason": exc.reason,
                },
                status_code=409,
            )
        except TokenLifecycleError as exc:
            if exc.reason == "authorization_not_found":
                return JSONResponse(
                    {
                        "detail": "douyin_authorization_not_found",
                        "reason": exc.reason,
                    },
                    status_code=404,
                )
            if exc.reason == "refresh_busy":
                return JSONResponse(
                    {
                        "detail": "douyin_token_temporarily_unavailable",
                        "reason": exc.reason,
                    },
                    status_code=503,
                    headers={"Retry-After": "1"},
                )
            return JSONResponse(
                {"detail": "douyin_token_error", "reason": exc.reason},
                status_code=502,
            )
        except DouyinProviderError as exc:
            return JSONResponse(
                {
                    "detail": "douyin_provider_error",
                    "operation": exc.operation,
                    "reason": exc.category,
                },
                status_code=502,
            )
        return JSONResponse(page.as_dict())

    def authorization_page(notice: str = "") -> str:
        target = resolved.public_route("/accounts/douyin-authorization")
        if notice:
            return f"{target}?notice={notice}"
        return target

    async def start_authorization(
        request: Request,
        *,
        target_authorization_id: str | None = None,
        target_authorization_version: int | None = None,
    ) -> Response:
        if not resolved.authorization_enabled:
            return JSONResponse(
                {"detail": "抖音授权功能尚未启用"}, status_code=409
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
            scopes=REQUESTED_SCOPES,
            expires_at=now + resolved.state_ttl_seconds,
            request_id=_request_id(request),
            target_authorization_id=target_authorization_id,
            target_authorization_version=target_authorization_version,
            now=now,
        )
        provider: Optional[MockOAuthClient | DouyinOAuthClient] = (
            request.app.state.provider
        )
        if provider is None:
            return JSONResponse(
                {"detail": "抖音授权功能尚未启用"}, status_code=409
            )
        return JSONResponse(
            {"authorize_url": provider.authorization_url(state, REQUESTED_SCOPES)}
        )

    @application.get("/douyin")
    async def index() -> RedirectResponse:
        return RedirectResponse(authorization_page(), status_code=303)

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
        del payload
        return await start_authorization(request)

    @application.get("/oauth/douyin/callback")
    async def oauth_callback(
        request: Request,
        code: Optional[str] = None,
        state: Optional[str] = None,
    ) -> Response:
        if not resolved.authorization_enabled:
            return RedirectResponse(
                authorization_page("oauth-disabled"),
                status_code=303,
            )
        if (
            code is None
            or state is None
            or not 1 <= len(code) <= 512
            or not 20 <= len(state) <= 256
        ):
            return RedirectResponse(
                authorization_page("oauth-invalid-response"),
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
                authorization_page("oauth-state-invalid"),
                status_code=303,
            )
        except Exception:
            return RedirectResponse(
                authorization_page("oauth-failed"),
                status_code=303,
            )
        try:
            provider = request.app.state.provider
            if provider is None:
                raise RuntimeError("mock provider unavailable")
            bundle: TokenBundle = await provider.exchange_code(code)
            try:
                profile = await provider.userinfo(bundle)
            except (DouyinProviderError, RuntimeError):
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
            staged = await asyncio.to_thread(
                request.app.state.store.stage_authorization_candidate,
                state_digest=state_digest,
                bound_username=username,
                session_binding=session_binding,
                open_id_fingerprint=open_id_fingerprint,
                candidate=candidate,
                cipher=request.app.state.cipher,
                request_id=request_id,
                now=int(clock()),
            )
            if not bool(staged["already_active"]):
                outcome = "unavailable"
                matched_account_id: int | None = None
                matched_platform_uid: str | None = None
                try:
                    page = await provider.video_list_page(
                        access_token=bundle.access_token,
                        open_id=bundle.open_id,
                        cursor=0,
                        count=20,
                    )
                    video_ids = [
                        str(item["video_id"])
                        for item in page.items
                        if isinstance(item, dict) and item.get("video_id")
                    ]
                    if video_ids:
                        resolved_match = (
                            await request.app.state.accounts.resolve_douyin_videos(
                                video_ids
                            )
                        )
                        outcome = str(resolved_match["status"])
                        if outcome == "matched":
                            matched_account_id = int(resolved_match["account_id"])
                            matched_platform_uid = str(
                                resolved_match["platform_uid"]
                            )
                    else:
                        outcome = "unmatched"
                except (DouyinProviderError, RuntimeError, TypeError, ValueError):
                    outcome = "unavailable"
                await asyncio.to_thread(
                    request.app.state.store.finalize_auto_match,
                    state_digest=state_digest,
                    authorization_id=str(staged["id"]),
                    expected_version=int(staged["version"]),
                    outcome=outcome,
                    actor=username,
                    request_id=request_id,
                    account_id=matched_account_id,
                    platform_uid=matched_platform_uid,
                    now=int(clock()),
                )
        except AuthorizationConflict as exc:
            with suppress(Exception):
                await asyncio.to_thread(
                    request.app.state.store.fail_state,
                    state_digest,
                    exc.reason_code,
                    request_id=request_id,
                    actor=username,
                    now=int(clock()),
                )
            return RedirectResponse(
                authorization_page("oauth-conflict"), status_code=303
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
                authorization_page("oauth-failed"),
                status_code=303,
            )
        return RedirectResponse(
            authorization_page("oauth-completed"), status_code=303
        )

    @application.get("/api/douyin/authorizations")
    async def authorizations(request: Request) -> dict[str, Any]:
        items = await asyncio.to_thread(request.app.state.store.list_authorizations)
        return {"items": items}

    @application.get("/api/douyin/authorization-statuses")
    async def authorization_statuses(request: Request) -> dict[str, Any]:
        items = await asyncio.to_thread(
            request.app.state.store.authorization_statuses,
            now=int(clock()),
        )
        return {"items": items}

    @application.post("/api/douyin/authorizations/match")
    async def authorization_match(
        request: Request, payload: AuthorizationMatchPayload
    ) -> Response:
        username, _binding = _identity(request)
        try:
            await request.app.state.accounts.require_account(
                account_id=payload.account_id,
                platform_uid=payload.platform_uid,
            )
            result = await asyncio.to_thread(
                request.app.state.store.manual_match,
                authorization_id=payload.authorization_id,
                account_id=payload.account_id,
                platform_uid=payload.platform_uid,
                expected_version=payload.expected_version,
                actor=username,
                request_id=_request_id(request),
                now=int(clock()),
            )
        except AuthorizationConflict as exc:
            return JSONResponse(
                {"detail": "授权状态已变化，请刷新后重试", "reason": exc.reason_code},
                status_code=409,
            )
        except RuntimeError:
            return JSONResponse(
                {"detail": "所选抖音账号已失效，请重新选择"}, status_code=409
            )
        return JSONResponse({"authorization": result})

    @application.post("/api/douyin/authorizations/reauthorize")
    async def authorization_reauthorize(
        request: Request, payload: ReauthorizePayload
    ) -> Response:
        authorization = await asyncio.to_thread(
            request.app.state.store.get_authorization,
            payload.authorization_id,
        )
        if authorization is None:
            return JSONResponse({"detail": "未找到授权"}, status_code=404)
        if int(authorization["version"]) != payload.expected_version:
            return JSONResponse(
                {"detail": "授权状态已变化，请刷新后重试"}, status_code=409
            )
        return await start_authorization(
            request,
            target_authorization_id=payload.authorization_id,
            target_authorization_version=payload.expected_version,
        )

    @application.post("/api/douyin/authorizations/unbind")
    async def authorization_unbind(
        request: Request, payload: UnbindPayload
    ) -> Response:
        username, _binding = _identity(request)
        try:
            unbound = await asyncio.to_thread(
                request.app.state.store.unbind,
                actor=username,
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
