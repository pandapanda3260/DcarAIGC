"use client";

import { useRef, useState, type FormEvent } from "react";
import { jsonRequest, readJson } from "../lib/api";
import styles from "./accounts.module.css";

type CreateAccountStatus = "daily" | "weekly" | "paused";
export type CreateAccountResponse = {
  message: string;
  account_id: number;
  platform: string;
  uid: string;
  account_status: CreateAccountStatus;
  activation_status: string;
  scheduled_effective_at: string | null;
  action: string;
};

export default function CreateAccountDialog({ accountManagementVersion, onClose, onCreated }: {
  accountManagementVersion?: number;
  onClose: () => void;
  onCreated: (response: CreateAccountResponse) => void;
}) {
  const [profileUrl, setProfileUrl] = useState("");
  const [phone, setPhone] = useState("");
  const [operatorName, setOperatorName] = useState("");
  const [accountStatus, setAccountStatus] = useState<CreateAccountStatus | "">("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const available = (accountManagementVersion ?? 1) >= 2;
  const unavailableMessage = "新增账号暂不可用，服务更新完成后可提交。";
  const pending = useRef(false);
  const request = useRef<{ intent: string; id: string } | null>(null);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (pending.current) return;
    if (!available) { setError(unavailableMessage); return; }
    if (!profileUrl.trim()) {
      setError("请填写账号主页链接。");
      return;
    }
    if (!accountStatus) {
      setError("请选择日更、周更或暂停。");
      return;
    }
    const body = {
      profile_url: profileUrl.trim(), phone: phone.trim(),
      operator_name: operatorName.trim(), account_status: accountStatus,
    };
    const intent = JSON.stringify(body);
    if (request.current?.intent !== intent) request.current = { intent, id: crypto.randomUUID() };
    pending.current = true;
    setSaving(true);
    setError("");
    try {
      const response = await readJson<CreateAccountResponse>("/api/v8/accounts", jsonRequest({
        ...body, request_id: request.current.id,
      }));
      onCreated(response);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "账号添加失败，请稍后重试。");
    } finally {
      pending.current = false;
      setSaving(false);
    }
  }

  return <div className="modal-backdrop" role="presentation">
    <section className="modal-panel operation-modal" role="dialog" aria-modal="true" aria-labelledby="create-account-title">
      <div className="panel-head"><h3 id="create-account-title">新增系统账号</h3><button className="modal-close" type="button" disabled={saving} onClick={onClose} aria-label="关闭">×</button></div>
      <p>粘贴主页链接，自动识别平台和账号。</p>
      <form onSubmit={(event) => void submit(event)} noValidate>
        <div className="modal-fields">
          <label>主页链接（必填）<input type="url" name="profile_url" required autoFocus value={profileUrl} disabled={saving} placeholder="粘贴抖音或小红书主页链接" aria-describedby="create-account-platforms" onChange={(event) => setProfileUrl(event.target.value)} /><span id="create-account-platforms" className={styles.statusHelp}>支持抖音、小红书；视频号、快手暂不支持采集。</span></label>
          <label>手机号（可留空）<input type="tel" name="phone" value={phone} disabled={saving} onChange={(event) => setPhone(event.target.value)} /></label>
          <label>运营人员（可留空）<input name="operator_name" value={operatorName} disabled={saving} onChange={(event) => setOperatorName(event.target.value)} /></label>
          <label>账号状态（必选）<select name="account_status" required value={accountStatus} disabled={saving} aria-describedby="create-account-status-help" onChange={(event) => setAccountStatus(event.target.value as CreateAccountStatus | "")}><option value="" disabled>请选择账号状态</option><option value="daily">日更</option><option value="weekly">周更</option><option value="paused">暂停</option></select><span id="create-account-status-help" className={styles.statusHelp}>日更、周更只标注作品更新频率，采集规则不变。</span></label>
        </div>
        <p className={styles.statusHelp}>已有账号的手机号、运营人员留空时保留原信息。</p>
        {accountStatus === "paused" && <p role="status">账号将保存为暂停，不加入当前生效名单，不采集，也不进入统计。</p>}
        {!available && <p role="status">{unavailableMessage}</p>}
        {error && <p role="alert">{error}</p>}
        <div className="modal-actions"><button className="secondary" type="button" disabled={saving} onClick={onClose}>取消</button><button className="primary" type="submit" disabled={saving || !available}>{saving ? "正在识别账号…" : "添加账号"}</button></div>
      </form>
    </section>
  </div>;
}
