"use client";

import { useRef, useState, type FormEvent } from "react";
import { jsonRequest, readJson } from "../lib/api";
import { accountGroupOptions, businessDirectionOptions } from "../lib/accountClassification";
import styles from "./accounts.module.css";

type CreateAccountStatus = "daily" | "weekly" | "paused";
export type CreateAccountResponse = {
  message: string;
  account_id: number | null;
  intake_id?: number;
  status?: "accepted" | "ready" | "conflict" | "blocked";
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
  const [platform, setPlatform] = useState("");
  const [uid, setUid] = useState("");
  const [displayId, setDisplayId] = useState("");
  const [phone, setPhone] = useState("");
  const [operatorName, setOperatorName] = useState("");
  const [accountGroup, setAccountGroup] = useState("unknown");
  const [businessDirection, setBusinessDirection] = useState("unknown");
  const [accountStatus, setAccountStatus] = useState<CreateAccountStatus | "">("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const unifiedIntake = (accountManagementVersion ?? 1) >= 3;
  const available = (accountManagementVersion ?? 1) >= 2;
  const unavailableMessage = "新增账号暂不可用，服务更新完成后可提交。";
  const pending = useRef(false);
  const request = useRef<{ intent: string; id: string } | null>(null);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (pending.current) return;
    if (!available) { setError(unavailableMessage); return; }
    if (!profileUrl.trim() && !(unifiedIntake && (uid.trim() || displayId.trim()))) {
      setError(unifiedIntake ? "请填写 UID、账号 ID 或官方主页链接中的一项。" : "请填写账号主页链接。");
      return;
    }
    if (unifiedIntake && !profileUrl.trim() && !platform) { setError("请选择账号平台。"); return; }
    if (!accountStatus) {
      setError("请选择日更、周更或暂停。");
      return;
    }
    const body = {
      ...(unifiedIntake ? { platform: platform || null, uid: uid.trim(), display_account_id: displayId.trim() } : {}),
      profile_url: profileUrl.trim(), phone: phone.trim(),
      operator_name: operatorName.trim(), account_status: accountStatus,
      account_group: accountGroup, business_direction: businessDirection,
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
      <p>{unifiedIntake ? "填写账号定位信息，系统自动补齐主页资料、核对账号是否一致，资料就绪后自动采集。" : "粘贴主页链接，自动识别平台和账号。"}</p>
      <form onSubmit={(event) => void submit(event)} noValidate>
        <div className="modal-fields">
          {unifiedIntake && <>
            <label>平台<select name="platform" value={platform} disabled={saving} onChange={(event) => setPlatform(event.target.value)}><option value="">根据官方链接识别，或手动选择</option><option value="douyin">抖音</option><option value="xiaohongshu">小红书</option><option value="kuaishou">快手</option><option value="wechat_channels">视频号</option></select></label>
            <label>UID（可留空）<input name="uid" type="text" value={uid} disabled={saving} onChange={(event) => setUid(event.target.value)} /></label>
            <label>账号 ID（可留空）<input name="display_account_id" type="text" value={displayId} disabled={saving} onChange={(event) => setDisplayId(event.target.value)} /></label>
          </>}
          <label>{unifiedIntake ? "官方主页或分享链接（可留空）" : "主页链接（必填）"}<input type="url" name="profile_url" required={!unifiedIntake} autoFocus value={profileUrl} disabled={saving} placeholder={unifiedIntake ? "粘贴抖音、小红书或快手官方主页链接" : "粘贴抖音或小红书主页链接"} aria-describedby="create-account-platforms" onChange={(event) => setProfileUrl(event.target.value)} /><span id="create-account-platforms" className={styles.statusHelp}>{unifiedIntake ? "UID、账号 ID、官方链接至少填写一项。抖音、小红书、快手支持官方主页及支持的短链；视频号请填写 sph 账号 ID 或 finder UID，作品分享链接不能定位账号。接收成功后可查看资料补齐结果，不表示已经完成作品抓取。" : "支持抖音、小红书；视频号、快手暂不支持采集。"}</span></label>
          <label>手机号（可留空）<input type="tel" name="phone" value={phone} disabled={saving} onChange={(event) => setPhone(event.target.value)} /></label>
          <label>运营人员（可留空）<input name="operator_name" value={operatorName} disabled={saving} onChange={(event) => setOperatorName(event.target.value)} /></label>
          <label>账号分组<select name="account_group" value={accountGroup} disabled={saving} onChange={(event) => setAccountGroup(event.target.value)}>{accountGroupOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label>
          <label>业务方向<select name="business_direction" value={businessDirection} disabled={saving} onChange={(event) => setBusinessDirection(event.target.value)}>{businessDirectionOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label>
          <label>账号状态（必选）<select name="account_status" required value={accountStatus} disabled={saving} aria-describedby="create-account-status-help" onChange={(event) => setAccountStatus(event.target.value as CreateAccountStatus | "")}><option value="" disabled>请选择账号状态</option><option value="daily">日更</option><option value="weekly">周更</option><option value="paused">暂停</option></select><span id="create-account-status-help" className={styles.statusHelp}>账号状态由人工维护；当前所有状态均参与自动采集。</span></label>
        </div>
        <p className={styles.statusHelp}>已有账号的手机号、运营人员留空时保留原信息。</p>
        {accountStatus === "paused" && <p role="status">账号将保存为暂停标签；当前仍按采集条件参与自动采集。</p>}
        {!available && <p role="status">{unavailableMessage}</p>}
        {error && <p role="alert">{error}</p>}
        <div className="modal-actions"><button className="secondary" type="button" disabled={saving} onClick={onClose}>取消</button><button className="primary" type="submit" disabled={saving || !available}>{saving ? (unifiedIntake ? "正在接收账号…" : "正在识别账号…") : "添加账号"}</button></div>
      </form>
    </section>
  </div>;
}
