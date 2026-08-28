"use client";

import Link from "next/link";
import { useEffect, useState, useSyncExternalStore } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import AppShell from "../../components/AppShell";
import { Feedback, Loading, Notice } from "../../components/Feedback";
import { markedJsonRequest, readJson } from "../../lib/api";
import { buildAccountSearchRequest } from "../../lib/queryContracts";
import { accountSearchQueryOptions, douyinAuthorizationsQueryOptions, douyinAuthorizationStatusesQueryOptions, queryKeys } from "../../lib/queries";
import type { Account, DouyinAuthorization } from "../../lib/types";

const CALLBACK_CHANNEL = "dcar-douyin-authorization";
const CALLBACK_MESSAGE = { type: "dcar-douyin-authorization-updated" } as const;
const PRODUCTION_AUTHORIZATION_URL = "https://origin.tj.cn/dcar/accounts/douyin-authorization";
const noticeCopy: Record<string, { tone: "error" | "success"; text: string }> = {
  "oauth-completed": { tone: "success", text: "抖音账号授权已完成，授权状态已更新。" },
  "oauth-disabled": { tone: "error", text: "抖音授权功能尚未启用。" },
  "oauth-invalid-response": { tone: "error", text: "抖音没有返回完整的授权结果，请重新扫码。" },
  "oauth-state-invalid": { tone: "error", text: "本次授权已失效，请重新扫码。" },
  "oauth-target-unavailable": { tone: "error", text: "当前业务账号已停用、抖音账号编号已变化或账号目录暂时不可用，请返回账号页确认后重新发起授权。" },
  "oauth-conflict": { tone: "error", text: "扫码账号与当前业务账号原有授权不一致，原授权未被覆盖。" },
  "oauth-failed": { tone: "error", text: "抖音授权没有完成，请稍后重试。" },
};
const statusCopy = { active: "已授权", unbound: "已解绑", pending_match: "历史待处理" } as const;

function formatTime(value: number | null) {
  if (value == null) return "—";
  return new Intl.DateTimeFormat("zh-CN", { year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false }).format(new Date(value * 1000));
}
function subscribeToLocation() { return () => {}; }
function readLocationSearch() { return window.location.search; }
function douyinIdentity(account: Account | undefined, uid: string) {
  return account?.platforms.find((identity) => identity.platform === "douyin" && identity.uid === uid);
}

export default function DouyinAuthorizationPage() {
  const queryClient = useQueryClient();
  const locationSearch = useSyncExternalStore(subscribeToLocation, readLocationSearch, () => "");
  const params = new URLSearchParams(locationSearch);
  const notice = params.get("notice") ?? "";
  const rawAccountId = params.get("account_id") ?? "";
  const platformUid = params.get("platform_uid") ?? "";
  const accountId = /^\d+$/.test(rawAccountId) ? Number(rawAccountId) : 0;
  const targetWasRequested = Boolean(rawAccountId || platformUid);
  const targetIsValid = Number.isSafeInteger(accountId) && accountId > 0 && /^\d{6,24}$/.test(platformUid);
  const targetRequest = buildAccountSearchRequest({ query: platformUid, accountType: "", direction: "", platform: "douyin" }, 1, 100);

  const sessionQuery = useQuery({ queryKey: ["auth", "session"], queryFn: () => readJson<{ authenticated: true; username: string }>("/auth/session"), staleTime: 60_000 });
  const isBypassMode = sessionQuery.data?.username === "temporary-bypass";
  const canUseControl = sessionQuery.isSuccess && !isBypassMode;
  const authorizationsQuery = useQuery({ ...douyinAuthorizationsQueryOptions(), enabled: canUseControl });
  const statusesQuery = useQuery({ ...douyinAuthorizationStatusesQueryOptions(), enabled: canUseControl });
  const targetAccountQuery = useQuery({ ...accountSearchQueryOptions(targetRequest), enabled: canUseControl && targetIsValid });
  const authorizations = authorizationsQuery.data?.items ?? [];
  const targetAccount = targetIsValid ? targetAccountQuery.data?.items.find((item) => item.id === accountId) : undefined;
  const targetIdentity = douyinIdentity(targetAccount, platformUid);
  const targetMatches = Boolean(targetAccount && targetIdentity);
  const targetCanAuthorize = Boolean(targetAccount?.enabled && targetIdentity);
  const activeTargetAuthorization = targetIsValid ? authorizations.find((item) => item.status === "active" && item.account_id === accountId && item.platform_uid === platformUid) : undefined;
  const callbackNotice = noticeCopy[notice] ?? null;
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [busyAction, setBusyAction] = useState("");

  const queryError = sessionQuery.isError ? (sessionQuery.error instanceof Error ? sessionQuery.error.message : "登录状态读取失败")
    : authorizationsQuery.isError ? (authorizationsQuery.error instanceof Error ? authorizationsQuery.error.message : "授权记录读取失败")
    : statusesQuery.isError ? (statusesQuery.error instanceof Error ? statusesQuery.error.message : "授权状态读取失败")
    : statusesQuery.data?.unavailable ? "抖音授权服务在当前环境未启用。"
    : targetIsValid && targetAccountQuery.isError ? (targetAccountQuery.error instanceof Error ? targetAccountQuery.error.message : "目标账号读取失败") : "";
  const authorizationDataReady = Boolean(authorizationsQuery.data && statusesQuery.data);
  const authorizationReadPending = !queryError && (sessionQuery.isPending || (canUseControl && !authorizationDataReady) || (canUseControl && targetIsValid && targetAccountQuery.isPending && !targetAccountQuery.data));
  const actionsAvailable = canUseControl && authorizationDataReady && !queryError;

  useEffect(() => {
    if (notice !== "oauth-completed") return;
    void Promise.all([
      queryClient.refetchQueries({ queryKey: queryKeys.douyinAuthorizations }),
      queryClient.refetchQueries({ queryKey: queryKeys.authorizationStatuses }),
    ]);
    if ("BroadcastChannel" in window) {
      const channel = new BroadcastChannel(CALLBACK_CHANNEL);
      channel.postMessage(CALLBACK_MESSAGE);
      channel.close();
    }
    if (window.opener && !window.opener.closed) window.opener.postMessage(CALLBACK_MESSAGE, window.location.origin);
  }, [notice, queryClient]);

  const counts = {
    active: authorizations.filter((item) => item.status === "active").length,
    pending: authorizations.filter((item) => item.status === "pending_match").length,
    unbound: authorizations.filter((item) => item.status === "unbound").length,
    authorized: statusesQuery.data?.items.filter((item) => item.authorized).length ?? 0,
  };
  async function refreshAuthorizationData() {
    await Promise.all([
      queryClient.refetchQueries({ queryKey: queryKeys.douyinAuthorizations }),
      queryClient.refetchQueries({ queryKey: queryKeys.authorizationStatuses }),
    ]);
  }
  async function startAuthorization() {
    if (!targetCanAuthorize) return;
    setBusyAction("start"); setError(""); setMessage("");
    try {
      const result = await readJson<{ authorize_url: string }>("/api/douyin/oauth/start", markedJsonRequest({ account_id: accountId, platform_uid: platformUid }, "douyin-oauth-start"));
      if (!result.authorize_url) throw new Error("抖音没有返回授权地址，请稍后重试。");
      window.location.assign(result.authorize_url);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "授权没有开始，请稍后重试。"); setBusyAction(""); }
  }
  async function reauthorize(item: DouyinAuthorization) {
    setBusyAction(`reauthorize:${item.id}`); setError(""); setMessage("");
    try {
      const result = await readJson<{ authorize_url: string }>("/api/douyin/authorizations/reauthorize", markedJsonRequest({ authorization_id: item.id, expected_version: item.version }, "douyin-authorization-reauthorize"));
      if (!result.authorize_url) throw new Error("抖音没有返回授权地址，请稍后重试。");
      window.location.assign(result.authorize_url);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "重新授权没有开始，请稍后重试。"); setBusyAction(""); }
  }
  async function unbind(item: DouyinAuthorization) {
    if (!window.confirm("确定解绑这个抖音开放平台授权吗？解绑后将停止使用该授权同步内容。")) return;
    setBusyAction(`unbind:${item.id}`); setError(""); setMessage("");
    try {
      await readJson("/api/douyin/authorizations/unbind", markedJsonRequest({ authorization_id: item.id, expected_version: item.version }, "douyin-authorization-unbind"));
      await refreshAuthorizationData();
      setMessage(item.status === "pending_match" ? "历史待处理授权已作废。" : "抖音开放平台授权已解绑。");
    } catch (reason) { setError(reason instanceof Error ? reason.message : "解绑失败，请稍后重试。"); }
    finally { setBusyAction(""); }
  }
  const productionUrl = targetIsValid ? `${PRODUCTION_AUTHORIZATION_URL}?account_id=${encodeURIComponent(String(accountId))}&platform_uid=${encodeURIComponent(platformUid)}` : PRODUCTION_AUTHORIZATION_URL;

  return <AppShell active="accounts">
    <Feedback error={error} message={message} onClose={() => { setError(""); setMessage(""); }} />
    {callbackNotice && <Notice tone={callbackNotice.tone}>{callbackNotice.text}</Notice>}
    <section className="page-stack wide-stack douyin-authorization-page">
      <div className="detail-toolbar"><div><span className="eyebrow">账号管理 · 抖音开放平台</span><h2>抖音开放平台授权</h2><p>{targetIsValid ? "本次扫码只会绑定到当前锁定的业务账号。" : "查看已有授权；发起新授权请从账号列表对应行进入。"}</p></div><div><Link className="secondary button-link" href="/accounts">返回账号页</Link></div></div>
      {authorizationReadPending ? <Loading label="正在确认抖音授权入口" /> : isBypassMode ? <article className="panel douyin-authorization-start"><div><span className="eyebrow">正式授权入口</span><h3>请在正式 HTTPS 工作台完成扫码</h3><p>当前本地工作台处于免登录模式，本地入口不会发起或管理抖音授权。</p></div><a className="primary button-link" href={productionUrl}>打开正式授权入口</a></article> : <>
        {queryError && <Notice tone="error">{queryError}</Notice>}
        {targetWasRequested && !targetIsValid && <Notice tone="error">授权目标参数无效，请返回账号页重新进入。</Notice>}
        {targetIsValid && targetAccountQuery.isSuccess && !targetMatches && <Notice tone="error">目标账号不存在或抖音账号编号已变化，请返回账号页刷新后重试。</Notice>}
        {targetMatches && !targetAccount?.enabled && <Notice tone="error">目标账号已停用，不能发起新授权或重新授权；已有授权仍可解绑。</Notice>}
        {authorizationDataReady && targetIsValid && targetMatches && <article className="panel douyin-authorization-target">
          <div><span className="eyebrow">已锁定业务账号</span><h3>{targetIdentity?.nickname || `抖音账号 ${platformUid}`}</h3><p>业务账号 #{accountId} · 抖音账号编号 {platformUid} · {targetAccount?.operator_name || "未填写运营人员"}</p></div>
          <div className="douyin-authorization-target-actions">{activeTargetAuthorization ? <><span className="douyin-authorization-status active">{activeTargetAuthorization.needs_reauthorization ? "需重新授权" : "已授权"}</span><button className="primary" type="button" disabled={Boolean(busyAction) || !actionsAvailable || !targetCanAuthorize} onClick={() => void reauthorize(activeTargetAuthorization)}>{busyAction.startsWith("reauthorize:") ? "正在打开…" : "重新扫码授权"}</button><button className="secondary danger-button" type="button" disabled={Boolean(busyAction) || !actionsAvailable} onClick={() => void unbind(activeTargetAuthorization)}>解绑</button></> : <button className="primary" type="button" disabled={Boolean(busyAction) || !actionsAvailable || !targetCanAuthorize} onClick={() => void startAuthorization()}>{busyAction === "start" ? "正在打开…" : targetCanAuthorize ? "开始扫码授权" : "账号已停用"}</button>}</div>
        </article>}
        {authorizationDataReady && !targetWasRequested && <>
          <article className="panel douyin-authorization-start"><div><span className="eyebrow">授权管理概览</span><h3>从账号列表锁定账号后扫码</h3><p>本页不提供统一扫码入口。请返回账号页，在对应抖音账号的“抖音开平授权”列点击“去授权”。</p></div><Link className="primary button-link" href="/accounts">返回账号列表</Link></article>
          <article className="panel douyin-authorization-summary" aria-label="抖音授权统计"><div><span>有效授权</span><strong>{counts.authorized}</strong></div><div><span>已绑定</span><strong>{counts.active}</strong></div><div><span>历史待处理</span><strong>{counts.pending}</strong></div><div><span>已解绑</span><strong>{counts.unbound}</strong></div></article>
          <article className="panel"><div className="panel-head"><div><span className="eyebrow">授权记录</span><h3>抖音开放平台授权</h3><p>已有授权可管理；旧版遗留的待匹配记录只允许作废，不再人工匹配。</p></div><span className="rule-chip">共 {authorizations.length} 条</span></div>
            {!authorizations.length ? <div className="empty-state"><strong>还没有抖音开放平台授权</strong><span>请从账号列表对应行进入授权。</span></div> : <div className="douyin-authorization-list">{authorizations.map((item) => <section className="douyin-authorization-card" key={item.id} data-status={item.status}>
              <div className="douyin-authorization-card-head"><div><span className={`douyin-authorization-status ${item.status}`}>{statusCopy[item.status]}</span><strong>{item.platform_uid ? `抖音账号 ${item.platform_uid}` : "旧版待处理授权"}</strong><p>{item.needs_reauthorization ? "授权即将失效，需要重新授权" : item.status === "pending_match" ? "该记录未锁定业务账号，只能作废后从账号行重新授权" : "授权状态正常"}</p></div><small>更新于 {formatTime(item.updated_at)}</small></div>
              <dl className="douyin-authorization-details"><div><dt>业务账号编号</dt><dd>{item.account_id ?? "—"}</dd></div><div><dt>Refresh Token 到期</dt><dd>{formatTime(item.refresh_expires_at)}</dd></div><div><dt>授权范围</dt><dd>{item.scopes.join("、") || "—"}</dd></div><div><dt>发起人</dt><dd>{item.bound_username || "—"}</dd></div></dl>
              <div className="douyin-authorization-actions">{item.status === "active" && item.account_id != null && item.platform_uid && <Link className="secondary button-link" href={`/accounts/douyin-authorization?account_id=${encodeURIComponent(String(item.account_id))}&platform_uid=${encodeURIComponent(item.platform_uid)}`}>管理</Link>}{item.status === "pending_match" && <button className="secondary danger-button" type="button" disabled={Boolean(busyAction) || !actionsAvailable} onClick={() => void unbind(item)}>作废</button>}</div>
            </section>)}</div>}
          </article>
        </>}
      </>}
    </section>
  </AppShell>;
}
