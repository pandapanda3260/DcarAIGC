"use client";

import { useEffect, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import AppShell from "../components/AppShell";
import { Feedback, Loading, Notice } from "../components/Feedback";
import { useDialogFocus } from "../components/useDialogFocus";
import { ApiRequestError, markedJsonRequest, readJson } from "../lib/api";
import { publicAssetPath } from "../lib/paths";
import { formatDateTime } from "../lib/format";
import { queryKeys, usersQueryOptions } from "../lib/queries";
import type { ManagedUser, UserRole } from "../lib/types";
import styles from "./UsersPage.module.css";

// 角色等级：能授予的最高角色 = 自己的角色；只能操作等级不高于自己的用户。服务端在事务内再判一次。
const ROLE_RANK: Record<UserRole, number> = { superadmin: 3, admin: 2, operator: 1, new_user: 0 };
const ROLE_LABELS: Record<UserRole, string> = { superadmin: "超级管理员", admin: "管理员", operator: "运营人员", new_user: "新用户" };
const ROLE_OPTIONS: UserRole[] = ["superadmin", "admin", "operator", "new_user"];
const STATUS_LABELS: Record<ManagedUser["status"], string> = { active: "正常", disabled: "已停用" };
const PHONE_PATTERN = /^1[3-9][0-9]{9}$/;

type EditForm = { username: string; phone: string; role: UserRole; password: string; isSelf: boolean };

function RegistrationTime({ value }: { value: ManagedUser["created_at"] }) {
  const [date, time] = formatDateTime(value).split(" ");
  return <span className={styles.registrationTime}>{date}{time && <>{" "}<span className={styles.time}>{time}</span></>}</span>;
}

function leaveRevokedManagementPage(reason: unknown) {
  if (reason instanceof ApiRequestError && reason.status === 403 && reason.code === "forbidden") {
    window.location.replace(publicAssetPath("/overview"));
  }
}

export default function UsersPage() {
  const queryClient = useQueryClient();
  const usersQuery = useQuery(usersQueryOptions());
  const [retrying, setRetrying] = useState(false);
  const [form, setForm] = useState<EditForm | null>(null);
  const [pendingDelete, setPendingDelete] = useState<ManagedUser | null>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const editDialogRef = useRef<HTMLElement | null>(null);
  const deleteDialogRef = useRef<HTMLElement | null>(null);

  const usersReadFailed = usersQuery.isLoadingError || retrying;
  const actor = usersQuery.data?.actor;
  const items = usersQuery.data?.items ?? [];
  const actorRank = actor ? ROLE_RANK[actor.role] : 0;

  useDialogFocus(form !== null, editDialogRef, { onClose: () => setForm(null), busy: saving });
  useDialogFocus(pendingDelete !== null, deleteDialogRef, { onClose: () => setPendingDelete(null), busy: saving });

  useEffect(() => {
    leaveRevokedManagementPage(usersQuery.error);
  }, [usersQuery.error]);

  function retryUsersRead() {
    if (retrying) return;
    setRetrying(true);
    void usersQuery.refetch().finally(() => setRetrying(false));
  }

  async function invalidateUsers(includeSession: boolean) {
    await queryClient.invalidateQueries({ queryKey: queryKeys.users, exact: true });
    if (includeSession) await queryClient.invalidateQueries({ queryKey: queryKeys.session, exact: true });
  }

  function canManage(user: ManagedUser) {
    return actorRank >= ROLE_RANK.admin && actorRank >= ROLE_RANK[user.role];
  }

  function isSelf(user: ManagedUser) {
    return actor !== undefined && user.username === actor.username;
  }

  function edit(user: ManagedUser) {
    setForm({ username: user.username, phone: user.phone ?? "", role: user.role, password: "", isSelf: isSelf(user) });
  }

  async function save() {
    if (!form || saving) return;
    const phone = form.phone.trim();
    if (phone && !PHONE_PATTERN.test(phone)) { setError("手机号格式不正确"); return; }
    const passwordLength = Array.from(form.password).length;
    if (form.password && (passwordLength < 8 || passwordLength > 64 || form.password.includes("\u0000"))) { setError("密码长度需为 8–64 位"); return; }
    setSaving(true); setError(""); setMessage("");
    try {
      await readJson("/auth/users/update", markedJsonRequest({ username: form.username, phone, role: form.role, password: form.password }, "user-update"));
      const editedSelf = form.isSelf;
      setForm(null);
      await invalidateUsers(editedSelf);
      setMessage("用户信息已更新");
    } catch (reason) {
      leaveRevokedManagementPage(reason);
      setError(reason instanceof Error ? reason.message : "用户信息保存失败");
    } finally {
      setSaving(false);
    }
  }

  async function remove() {
    if (!pendingDelete || saving) return;
    setSaving(true); setError(""); setMessage("");
    try {
      await readJson("/auth/users/delete", markedJsonRequest({ username: pendingDelete.username }, "user-delete"));
      setPendingDelete(null);
      await invalidateUsers(false);
      setMessage("用户已删除");
    } catch (reason) {
      leaveRevokedManagementPage(reason);
      setError(reason instanceof Error ? reason.message : "用户删除失败");
    } finally {
      setSaving(false);
    }
  }

  return <AppShell active="users">
    <Feedback error={error} message={message} onClose={() => { setError(""); setMessage(""); }} />
    {usersQuery.isError && usersQuery.data && <Notice tone="error">{`数据刷新失败，当前显示上次数据。${usersQuery.error instanceof Error ? usersQuery.error.message : ""}`}</Notice>}
    {usersQuery.isPending && !usersQuery.data && !usersReadFailed ? <Loading label="正在读取用户列表" /> : <section className="page-stack wide-stack">
      {!usersReadFailed && <div className={styles.tableTitle}><h2>用户列表</h2><span>共 {items.length} 个用户</span></div>}
      {usersReadFailed ? <article className="panel"><div className="empty-state"><strong>用户列表读取失败</strong><span>请检查网络后重新加载。</span><button type="button" className="secondary read-error-retry" disabled={retrying} onClick={retryUsersRead}>{retrying ? "正在重新加载…" : "重新加载"}</button></div></article> : <article className={`panel table-panel ${styles.tablePanel}`}>
        <div className="table-scroll"><table className={styles.table}>
          <caption className="visually-hidden">工作台用户：一个账号一行</caption>
          <thead><tr><th scope="col">账号</th><th scope="col">手机号</th><th scope="col">权限等级</th><th scope="col">状态</th><th scope="col">注册时间</th><th scope="col">操作</th></tr></thead>
          <tbody>
            {items.map((user) => <tr key={user.username} data-username={user.username}>
              <th scope="row"><span className={styles.username}>{user.username}</span>{isSelf(user) && <span className="muted-cell">（本人）</span>}</th>
              <td className={styles.phone}>{user.phone ?? "—"}</td>
              <td className={styles.role} data-role={user.role}>{ROLE_LABELS[user.role]}</td>
              <td><span className={styles.status} data-status={user.status}>{STATUS_LABELS[user.status]}</span></td>
              <td><RegistrationTime value={user.created_at} /></td>
              <td>{canManage(user) && <span className="row-actions">
                <button type="button" className="text-button" disabled={saving} onClick={() => edit(user)}>修改</button>
                {!isSelf(user) && <button type="button" className="text-button danger" disabled={saving} onClick={() => setPendingDelete(user)}>删除</button>}
              </span>}</td>
            </tr>)}
          </tbody>
        </table></div>
        {items.length === 0 && <div className="empty-state"><strong>还没有用户</strong><span>新用户注册后会显示在这里。</span></div>}
      </article>}
    </section>}
    {form && <div className="modal-backdrop" role="presentation"><section ref={editDialogRef} className="modal-panel operation-modal" role="dialog" aria-modal="true" aria-label="修改用户" tabIndex={-1}>
      <div className="panel-head"><div><h3>修改用户</h3></div><button type="button" className="modal-close" onClick={() => setForm(null)} disabled={saving} aria-label="关闭">×</button></div>
      <div className="modal-fields">
        <label>账号<input value={form.username} disabled readOnly /></label>
        <label>手机号<input inputMode="numeric" autoComplete="off" value={form.phone} disabled={saving} onChange={(event) => setForm({ ...form, phone: event.target.value })} /></label>
        <label>权限等级<select value={form.role} disabled={saving || form.isSelf} onChange={(event) => setForm({ ...form, role: event.target.value as UserRole })}>
          {ROLE_OPTIONS.filter((role) => ROLE_RANK[role] <= actorRank || role === form.role).map((role) => <option key={role} value={role}>{ROLE_LABELS[role]}</option>)}
        </select></label>
        {!form.isSelf && <label>新密码<input type="password" autoComplete="new-password" placeholder="留空则不修改" value={form.password} disabled={saving} onChange={(event) => setForm({ ...form, password: event.target.value })} /></label>}
      </div>
      <div className="modal-actions"><button type="button" className="secondary" disabled={saving} onClick={() => setForm(null)}>取消</button><button type="button" className="primary" disabled={saving} onClick={() => void save()}>{saving ? "保存中" : "保存"}</button></div>
    </section></div>}
    {pendingDelete && <div className="modal-backdrop" role="presentation"><section ref={deleteDialogRef} className="modal-panel compact-modal" role="dialog" aria-modal="true" aria-label="删除用户" tabIndex={-1}>
      <div className="panel-head"><div><h3>删除用户</h3></div><button type="button" className="modal-close" onClick={() => setPendingDelete(null)} disabled={saving} aria-label="关闭">×</button></div>
      <p>删除后「{pendingDelete.username}」将立即退出登录，其手机号不能再注册，该账号名不能再使用；相关历史记录保留。</p>
      <div className="modal-actions"><button type="button" className="secondary" disabled={saving} onClick={() => setPendingDelete(null)}>取消</button><button type="button" className="secondary danger-button" disabled={saving} onClick={() => void remove()}>{saving ? "删除中" : "确认删除"}</button></div>
    </section></div>}
  </AppShell>;
}
