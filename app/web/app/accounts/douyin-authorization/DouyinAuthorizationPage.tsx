"use client";

import Link from "next/link";
import { useEffect, useState, useSyncExternalStore } from "react";
import type { FormEvent } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import AppShell from "../../components/AppShell";
import { Feedback, Loading, Notice } from "../../components/Feedback";
import { markedJsonRequest, readJson } from "../../lib/api";
import {
  douyinAuthorizationsQueryOptions,
  douyinAuthorizationStatusesQueryOptions,
  queryKeys,
} from "../../lib/queries";
import type {
  DouyinAccountDirectoryItem,
  DouyinAccountDirectoryResult,
  DouyinAuthorization,
} from "../../lib/types";

const CALLBACK_CHANNEL = "dcar-douyin-authorization";
const CALLBACK_MESSAGE = { type: "dcar-douyin-authorization-updated" } as const;
const PRODUCTION_AUTHORIZATION_URL = "https://origin.tj.cn/dcar/accounts/douyin-authorization";
const noticeCopy: Record<string, { tone: "error" | "success"; text: string }> = {
  "oauth-completed": { tone: "success", text: "抖音账号授权已完成，授权状态已更新。" },
  "oauth-disabled": { tone: "error", text: "抖音授权功能尚未启用。" },
  "oauth-invalid-response": { tone: "error", text: "抖音没有返回完整的授权结果，请重新扫码。" },
  "oauth-state-invalid": { tone: "error", text: "本次授权已失效，请重新扫码。" },
  "oauth-conflict": { tone: "error", text: "授权账号与原账号不一致，原授权未被覆盖。" },
  "oauth-failed": { tone: "error", text: "抖音授权没有完成，请稍后重试。" },
};
const statusCopy = { active: "已授权", unbound: "已解绑", pending_match: "等待匹配" } as const;
const matchReasonCopy: Record<string, string> = {
  matching: "正在自动匹配业务账号",
  no_match: "近期作品尚未匹配到业务账号",
  ambiguous_match: "近期作品匹配到多个业务账号",
  account_binding_conflict: "该业务账号已绑定其他抖音授权",
  auto_match_unavailable: "自动匹配暂时不可用",
  reauthorization_required: "需要重新授权",
};

function formatTime(value: number | null) {
  if (value == null) return "—";
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(value * 1000));
}

function subscribeToLocation() {
  return () => {};
}

function readLocationNotice() {
  return new URLSearchParams(window.location.search).get("notice") ?? "";
}

export default function DouyinAuthorizationPage() {
  const queryClient = useQueryClient();
  const sessionQuery = useQuery({
    queryKey: ["auth", "session"],
    queryFn: () => readJson<{ authenticated: true; username: string }>("/auth/session"),
    staleTime: 60_000,
  });
  const isBypassMode = sessionQuery.data?.username === "temporary-bypass";
  const canUseCurrentAuthorizationControl = sessionQuery.isSuccess && !isBypassMode;
  const authorizationsQuery = useQuery({
    ...douyinAuthorizationsQueryOptions(),
    enabled: canUseCurrentAuthorizationControl,
  });
  const statusesQuery = useQuery({
    ...douyinAuthorizationStatusesQueryOptions(),
    enabled: canUseCurrentAuthorizationControl,
  });
  const authorizations = authorizationsQuery.data?.items ?? [];
  const queryError = sessionQuery.isError
    ? sessionQuery.error instanceof Error
      ? sessionQuery.error.message
      : "登录状态读取失败"
    : authorizationsQuery.isError
    ? authorizationsQuery.error instanceof Error
      ? authorizationsQuery.error.message
      : "授权记录读取失败"
    : statusesQuery.isError
      ? statusesQuery.error instanceof Error
      ? statusesQuery.error.message
      : "授权状态读取失败"
      : "";
  const authorizationDataReady = Boolean(authorizationsQuery.data && statusesQuery.data);
  const authorizationReadPending = !queryError && (
    sessionQuery.isPending || (
      canUseCurrentAuthorizationControl
      && !authorizationDataReady
      && (authorizationsQuery.isPending || statusesQuery.isPending)
    )
  );
  const authorizationActionsAvailable = (
    canUseCurrentAuthorizationControl
    && authorizationDataReady
    && !queryError
  );
  const notice = useSyncExternalStore(subscribeToLocation, readLocationNotice, () => "");
  const callbackNotice = noticeCopy[notice] ?? null;
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [busyAction, setBusyAction] = useState("");
  const [manualAuthorizationId, setManualAuthorizationId] = useState<string | null>(null);
  const [accountQuery, setAccountQuery] = useState("");
  const [accountResults, setAccountResults] = useState<DouyinAccountDirectoryItem[]>([]);
  const [accountResultTotal, setAccountResultTotal] = useState(0);

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
    if (window.opener && !window.opener.closed) {
      window.opener.postMessage(CALLBACK_MESSAGE, window.location.origin);
    }
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
    setBusyAction("start"); setError(""); setMessage("");
    try {
      const result = await readJson<{ authorize_url: string }>(
        "/api/douyin/oauth/start",
        markedJsonRequest({}, "douyin-oauth-start"),
      );
      if (!result.authorize_url) throw new Error("抖音没有返回授权地址，请稍后重试。");
      window.location.assign(result.authorize_url);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "授权没有开始，请稍后重试。");
      setBusyAction("");
    }
  }

  async function reauthorize(item: DouyinAuthorization) {
    setBusyAction(`reauthorize:${item.id}`); setError(""); setMessage("");
    try {
      const result = await readJson<{ authorize_url: string }>(
        "/api/douyin/authorizations/reauthorize",
        markedJsonRequest({ authorization_id: item.id, expected_version: item.version }, "douyin-authorization-reauthorize"),
      );
      await refreshAuthorizationData();
      if (!result.authorize_url) throw new Error("抖音没有返回授权地址，请稍后重试。");
      window.location.assign(result.authorize_url);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "重新授权没有开始，请稍后重试。");
      setBusyAction("");
    }
  }

  async function unbind(item: DouyinAuthorization) {
    if (!window.confirm("确定解绑这个抖音开放平台授权吗？解绑后将停止使用该授权同步内容。")) return;
    setBusyAction(`unbind:${item.id}`); setError(""); setMessage("");
    try {
      await readJson(
        "/api/douyin/authorizations/unbind",
        markedJsonRequest({ authorization_id: item.id, expected_version: item.version }, "douyin-authorization-unbind"),
      );
      await refreshAuthorizationData();
      setMessage("抖音开放平台授权已解绑。");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "解绑失败，请稍后重试。");
    } finally {
      setBusyAction("");
    }
  }

  async function searchAccounts(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!manualAuthorizationId) return;
    setBusyAction(`search:${manualAuthorizationId}`); setError(""); setMessage("");
    try {
      const result = await readJson<DouyinAccountDirectoryResult>(
        "/api/douyin/accounts/search",
        markedJsonRequest({ query: accountQuery, page: 1, page_size: 50 }, "douyin-accounts-search"),
      );
      setAccountResults(result.items);
      setAccountResultTotal(result.total);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "账号搜索失败，请稍后重试。");
    } finally {
      setBusyAction("");
    }
  }

  async function matchAccount(item: DouyinAuthorization, account: DouyinAccountDirectoryItem) {
    setBusyAction(`match:${item.id}:${account.account_id}`); setError(""); setMessage("");
    try {
      await readJson(
        "/api/douyin/authorizations/match",
        markedJsonRequest({
          authorization_id: item.id,
          account_id: account.account_id,
          platform_uid: account.uid,
          expected_version: item.version,
        }, "douyin-authorization-match"),
      );
      setManualAuthorizationId(null);
      setAccountQuery("");
      setAccountResults([]);
      await refreshAuthorizationData();
      setMessage("抖音授权已关联到业务账号。");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "账号匹配失败，请稍后重试。");
    } finally {
      setBusyAction("");
    }
  }

  return <AppShell active="accounts">
    <Feedback error={error} message={message} onClose={() => { setError(""); setMessage(""); }} />
    {callbackNotice && <Notice tone={callbackNotice.tone}>{callbackNotice.text}</Notice>}
    <section className="page-stack wide-stack douyin-authorization-page">
      <div className="detail-toolbar"><div><span className="eyebrow">账号管理 · 抖音开放平台</span><h2>抖音开放平台授权</h2><p>达人在抖音官方页面扫码确认后，系统会自动匹配已有业务账号；扫码前不需要选择账号。</p></div><div><Link className="secondary button-link" href="/accounts">返回账号页</Link></div></div>

      {authorizationReadPending ? <Loading label="正在确认抖音授权入口" /> : isBypassMode ? <article className="panel douyin-authorization-start">
        <div><span className="eyebrow">正式授权入口</span><h3>请在正式 HTTPS 工作台完成扫码</h3><p>当前本地工作台处于免登录模式。为保证授权与真实登录用户、浏览器会话正确绑定，本地入口不会发起或管理抖音授权。</p></div>
        <a className="primary button-link" href={PRODUCTION_AUTHORIZATION_URL}>打开正式授权入口</a>
      </article> : <>
        {queryError && <Notice tone="error">{queryError}</Notice>}
        {authorizationDataReady && <>
          <article className="panel douyin-authorization-start">
            <div><span className="eyebrow">统一授权入口</span><h3>打开抖音APP扫码授权</h3><p>点击后将进入抖音官方授权页。授权成功会自动返回本页，并立即刷新授权状态。</p></div>
            <button className="primary" type="button" disabled={Boolean(busyAction) || !authorizationActionsAvailable} onClick={() => void startAuthorization()}>{busyAction === "start" ? "正在打开…" : "开始扫码授权"}</button>
          </article>

          <article className="panel douyin-authorization-summary" aria-label="抖音授权统计">
            <div><span>有效授权</span><strong>{counts.authorized}</strong></div>
            <div><span>已绑定</span><strong>{counts.active}</strong></div>
            <div><span>等待匹配</span><strong>{counts.pending}</strong></div>
            <div><span>已解绑</span><strong>{counts.unbound}</strong></div>
          </article>

          <article className="panel">
            <div className="panel-head"><div><span className="eyebrow">授权记录</span><h3>已扫码的抖音账号</h3><p>只有等待匹配的授权需要人工选择业务账号。</p></div><span className="rule-chip">共 {authorizations.length} 条</span></div>
            {!authorizations.length ? <div className="empty-state"><strong>还没有抖音开放平台授权</strong><span>点击“开始扫码授权”后，由达人在抖音官方页面完成扫码。</span></div> : <div className="douyin-authorization-list">
              {authorizations.map((item) => <section className="douyin-authorization-card" key={item.id} data-status={item.status}>
                <div className="douyin-authorization-card-head"><div><span className={`douyin-authorization-status ${item.status}`}>{statusCopy[item.status]}</span><strong>{item.platform_uid ? `抖音账号 ${item.platform_uid}` : "待识别的抖音账号"}</strong><p>{item.match_reason ? matchReasonCopy[item.match_reason] ?? "等待人工处理" : item.needs_reauthorization ? "授权即将失效，需要重新授权" : "授权状态正常"}</p></div><small>更新于 {formatTime(item.updated_at)}</small></div>
                <dl className="douyin-authorization-details"><div><dt>业务账号编号</dt><dd>{item.account_id ?? "—"}</dd></div><div><dt>Refresh Token 到期</dt><dd>{formatTime(item.refresh_expires_at)}</dd></div><div><dt>授权范围</dt><dd>{item.scopes.join("、") || "—"}</dd></div><div><dt>发起人</dt><dd>{item.bound_username || "—"}</dd></div></dl>
                {item.status !== "pending_match" && <div className="douyin-authorization-actions"><button className="secondary" type="button" disabled={Boolean(busyAction) || !authorizationActionsAvailable} onClick={() => void reauthorize(item)}>重新授权</button>{item.status === "active" && <button className="secondary danger-button" type="button" disabled={Boolean(busyAction) || !authorizationActionsAvailable} onClick={() => void unbind(item)}>解绑</button>}</div>}
                {item.status === "pending_match" && <div className="douyin-manual-match">
                  <button className="secondary" type="button" disabled={Boolean(busyAction) || !authorizationActionsAvailable} onClick={() => { setManualAuthorizationId((current) => current === item.id ? null : item.id); setAccountQuery(""); setAccountResults([]); }}>人工匹配业务账号</button>
                  {manualAuthorizationId === item.id && <form onSubmit={(event) => void searchAccounts(event)}><label htmlFor={`douyin-account-search-${item.id}`}>搜索已有抖音业务账号</label><div><input id={`douyin-account-search-${item.id}`} maxLength={100} value={accountQuery} onChange={(event) => setAccountQuery(event.target.value)} placeholder="运营人员、抖音账号编号或昵称" /><button className="secondary" type="submit" disabled={Boolean(busyAction) || !authorizationActionsAvailable}>搜索</button></div>{accountResults.length > 0 && <ul>{accountResults.map((account) => <li key={`${account.account_id}:${account.uid}`}><div><strong>{account.nickname || account.uid}</strong><span>抖音账号 {account.uid} · {account.operator_name || "未填写运营人员"}{account.enabled ? "" : " · 已停用"}</span></div><button className="secondary" type="button" disabled={Boolean(busyAction) || !authorizationActionsAvailable || !account.enabled} onClick={() => void matchAccount(item, account)}>选择并匹配</button></li>)}</ul>}{accountResults.length === 0 && accountResultTotal === 0 && <p className="douyin-search-hint">输入条件后搜索；全新或尚未采集过内容的账号可能需要人工确认。</p>}</form>}
                </div>}
              </section>)}
            </div>}
          </article>
        </>}
      </>}
    </section>
  </AppShell>;
}
