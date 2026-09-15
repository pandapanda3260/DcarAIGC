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

function UserFormIcon({ name }: { name: "close" | "lock" | "shield" | "chevron" | "eye" | "eye-off" }) {
  return <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    {name === "close" ? <path d="m6 6 12 12M18 6 6 18" />
      : name === "lock" ? <><rect x="5" y="10" width="14" height="11" rx="2" /><path d="M8 10V7a4 4 0 0 1 8 0v3M12 14v3" /></>
      : name === "shield" ? <><path d="m12 3 8 3v6c0 5-8 9-8 9s-8-4-8-9V6l8-3Z" /><path d="m8.5 12 2.5 2.5 4.5-5" /></>
      : name === "chevron" ? <path d="m6 9 6 6 6-6" />
      : <><path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12Z" /><circle cx="12" cy="12" r="3" />{name === "eye-off" && <path d="m3 3 18 18" />}</>}
  </svg>;
}

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
  const [passwordVisible, setPasswordVisible] = useState(false);
  const editDialogRef = useRef<HTMLElement | null>(null);
  const deleteDialogRef = useRef<HTMLElement | null>(null);

  const usersReadFailed = usersQuery.isLoadingError || retrying;
  const actor = usersQuery.data?.actor;
  const items = usersQuery.data?.items ?? [];
  const actorRank = actor ? ROLE_RANK[actor.role] : 0;

  useDialogFocus(form !== null, editDialogRef, { onClose: closeEdit, busy: saving, initialFocus: "#edit-user-phone" });
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
    setError("");
    setMessage("");
    setPasswordVisible(false);
    setForm({ username: user.username, phone: user.phone ?? "", role: user.role, password: "", isSelf: isSelf(user) });
  }

  function closeEdit() {
    if (saving) return;
    setForm(null);
    setError("");
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
    <Feedback error={form ? "" : error} message={message} onClose={() => { setError(""); setMessage(""); }} />
    {usersQuery.isError && usersQuery.data && <Notice tone="error">{`数据刷新失败，当前显示上次数据。${usersQuery.error instanceof Error ? usersQuery.error.message : ""}`}</Notice>}
    {usersQuery.isPending && !usersQuery.data && !usersReadFailed ? <Loading label="正在读取用户列表" /> : <section className="page-stack wide-stack">
      {!usersReadFailed && <div className={styles.tableTitle}><h2>用户列表</h2><span>共 {items.length} 个用户</span></div>}
      {usersReadFailed ? <article className="panel"><div className="empty-state"><strong>用户列表读取失败</strong><span>请检查网络后重新加载。</span><button type="button" className="secondary read-error-retry" disabled={retrying} onClick={retryUsersRead}>{retrying ? "正在重新加载…" : "重新加载"}</button></div></article> : <article className={`panel table-panel ${styles.tablePanel}`}>
        <div className="table-scroll"><table className={styles.table}>
          <caption className="visually-hidden">工作台用户：一个账号一行</caption>
          <thead><tr><th scope="col">昵称 / 登录账号</th><th scope="col">手机号</th><th scope="col">权限等级</th><th scope="col">状态</th><th scope="col">注册时间</th><th scope="col">操作</th></tr></thead>
          <tbody>
            {items.map((user) => <tr key={user.username} data-username={user.username}>
              <th scope="row"><span className={styles.username}>{user.display_name || user.username}</span>{isSelf(user) && <span className="muted-cell">（本人）</span>}<span className={styles.loginAccount}>登录账号：{user.username}</span></th>
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
    {form && <div className={`modal-backdrop ${styles.editBackdrop}`} role="presentation"><section ref={editDialogRef} className={`modal-panel ${styles.editModal}`} role="dialog" aria-modal="true" aria-label="修改用户" aria-describedby="edit-user-description" aria-busy={saving} tabIndex={-1}>
      <header className={styles.editHeader}><div><h3>修改用户</h3><p id="edit-user-description">管理账号资料与访问权限</p></div><button type="button" className={styles.iconButton} onClick={closeEdit} disabled={saving} aria-label="关闭"><UserFormIcon name="close" /></button></header>
      <div className={styles.editBody}>
        <div className={styles.identityStrip}>
          <span className={styles.avatar} aria-hidden="true">{Array.from(form.username.trim())[0]?.toLocaleUpperCase() || "U"}</span>
          <div className={styles.identityText}><span className={styles.identityLabel}>登录账号</span><div id="edit-user-username" className={styles.identityName} aria-label="登录账号">{form.username}</div></div>
          <span className={styles.readonlyMark}><UserFormIcon name="lock" />只读</span>
        </div>
        <div className={styles.editFields}>
          <div className={styles.editField}>
            <label htmlFor="edit-user-phone">手机号</label>
            <input id="edit-user-phone" className={styles.editInput} type="tel" inputMode="numeric" autoComplete="off" placeholder="请输入手机号" value={form.phone} disabled={saving} onChange={(event) => setForm({ ...form, phone: event.target.value })} />
          </div>
          <div className={styles.editField}>
            <label htmlFor="edit-user-role">权限等级</label>
            <div className={styles.trailingControl}>
              <select id="edit-user-role" className={styles.editInput} value={form.role} disabled={saving || form.isSelf} onChange={(event) => setForm({ ...form, role: event.target.value as UserRole })}>
                {ROLE_OPTIONS.filter((role) => ROLE_RANK[role] <= actorRank || role === form.role).map((role) => <option key={role} value={role}>{ROLE_LABELS[role]}</option>)}
              </select>
              <span className={styles.fieldAdornment} aria-hidden="true"><UserFormIcon name="chevron" /></span>
            </div>
          </div>
          {!form.isSelf && <div className={styles.editField}>
            <div className={styles.passwordLabel}><label htmlFor="edit-user-password">新密码</label><span>可选</span></div>
            <div className={styles.trailingControl}><input id="edit-user-password" className={styles.editInput} type={passwordVisible ? "text" : "password"} autoComplete="new-password" placeholder="留空则不修改" value={form.password} disabled={saving} onChange={(event) => setForm({ ...form, password: event.target.value })} /><button type="button" className={`${styles.iconButton} ${styles.fieldAction}`} disabled={saving} aria-label={passwordVisible ? "隐藏新密码" : "显示新密码"} aria-pressed={passwordVisible} onClick={() => setPasswordVisible(!passwordVisible)}><UserFormIcon name={passwordVisible ? "eye-off" : "eye"} /></button></div>
          </div>}
        </div>
        {error && <p className={styles.formError} role="alert">{error}</p>}
      </div>
      <footer className={styles.editFooter}><span className={styles.footerHint}><UserFormIcon name="shield" />账号资料</span><div className={styles.editActions}><button type="button" className={styles.cancelButton} disabled={saving} onClick={closeEdit}>取消</button><button type="button" className={styles.saveButton} disabled={saving} onClick={() => void save()}>{saving ? "保存中…" : "保存修改"}</button></div></footer>
    </section></div>}
    {pendingDelete && <div className="modal-backdrop" role="presentation"><section ref={deleteDialogRef} className="modal-panel compact-modal" role="dialog" aria-modal="true" aria-label="删除用户" tabIndex={-1}>
      <div className="panel-head"><div><h3>删除用户</h3></div><button type="button" className="modal-close" onClick={() => setPendingDelete(null)} disabled={saving} aria-label="关闭">×</button></div>
      <p>删除后「<span className={styles.username}>{pendingDelete.username}</span>」将立即退出登录，当前权限会被撤回；再次注册后为新用户，需要管理员重新授权。相关历史记录保留。</p>
      <div className="modal-actions"><button type="button" className="secondary" disabled={saving} onClick={() => setPendingDelete(null)}>取消</button><button type="button" className="secondary danger-button" disabled={saving} onClick={() => void remove()}>{saving ? "删除中" : "确认删除"}</button></div>
    </section></div>}
  </AppShell>;
}
