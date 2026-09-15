"use client";

import { useRef, useState, type FormEvent } from "react";
import { jsonRequest, readJson } from "../lib/api";
import type { Account } from "../lib/types";
import type { CreateAccountResponse } from "./CreateAccountDialog";
import styles from "./accounts.module.css";

const platforms: Record<string, string> = { douyin: "抖音", xiaohongshu: "小红书", kuaishou: "快手", wechat_channels: "视频号" };

export default function RepairAccountIdentityDialog({ account, onClose, onSaved }: {
  account: Account;
  onClose: () => void;
  onSaved: (result: CreateAccountResponse) => void;
}) {
  const current = account.directory_locator;
  const platform = account.directory_platform ?? current?.platform ?? account.platforms[0]?.platform ?? "";
  const [uid, setUid] = useState(current?.uid ?? account.directory_uid ?? "");
  const [displayId, setDisplayId] = useState(current?.display_account_id ?? "");
  const [profileUrl, setProfileUrl] = useState(current?.profile_url ?? "");
  const [references, setReferences] = useState<Record<string, string>>(Object.fromEntries(
    Object.entries(current?.references ?? {}).filter((entry): entry is [string, string] => typeof entry[1] === "string")));
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const pending = useRef(false);
  const request = useRef<{ intent: string; id: string } | null>(null);
  const referenceKind = platform === "douyin" ? "sec_user_id" : platform === "kuaishou" ? "eid" : platform === "wechat_channels" ? "channel_id" : null;

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (pending.current) return;
    if (!account.directory_row_id || !account.locator_sha256) { setError("请刷新账号页后重新补充身份。"); return; }
    const typedReferences = Object.fromEntries(Object.entries(references).filter(([, value]) => value.trim()).map(([key, value]) => [key, value.trim()]));
    if (!uid.trim() && !displayId.trim() && !profileUrl.trim() && !Object.keys(typedReferences).length) {
      setError("请填写平台 UID、官方主页或平台定位信息中的一项。"); return;
    }
    const body = { expected_locator_sha256: account.locator_sha256, platform, uid: uid.trim(),
      display_account_id: displayId.trim(), profile_url: profileUrl.trim(), references: typedReferences };
    const intent = JSON.stringify(body);
    if (request.current?.intent !== intent) request.current = { intent, id: crypto.randomUUID() };
    pending.current = true; setSaving(true); setError("");
    try {
      const response = await readJson<CreateAccountResponse>(`/api/v8/account-directory/${account.directory_row_id}/identity`,
        jsonRequest({ ...body, request_id: request.current.id }));
      onSaved(response);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "身份补充失败，请刷新后重试。"); }
    finally { pending.current = false; setSaving(false); }
  }

  return <div className="modal-backdrop" role="presentation">
    <section className="modal-panel operation-modal" role="dialog" aria-modal="true" aria-labelledby="repair-account-identity-title"
      onKeyDown={(event) => {
        if (event.key === "Escape" && !saving) { event.stopPropagation(); onClose(); }
        if (event.key === "Tab") {
          const controls = event.currentTarget.querySelectorAll<HTMLElement>("button:not(:disabled),input:not(:disabled)");
          const first = controls[0], last = controls[controls.length - 1];
          if (event.shiftKey && event.target === first) { event.preventDefault(); last?.focus(); }
          else if (!event.shiftKey && event.target === last) { event.preventDefault(); first?.focus(); }
        }
      }}>
      <div className="panel-head"><h3 id="repair-account-identity-title">补充账号身份</h3><button className="modal-close" type="button" disabled={saving} onClick={onClose} aria-label="关闭身份补充">×</button></div>
      <p>为此条 {platforms[platform] ?? platform} 账号补充定位信息。系统核验后在原账号行接续准备和采集，导入资料与运营分类保留。</p>
      <form onSubmit={(event) => void submit(event)} noValidate>
        <div className="modal-fields">
          <label>平台 UID<input autoFocus type="text" name="uid" value={uid} disabled={saving} onChange={(event) => setUid(event.target.value)} /></label>
          <label>账号 ID<input type="text" name="display_account_id" value={displayId} disabled={saving} onChange={(event) => setDisplayId(event.target.value)} /></label>
          <label>官方主页链接<input type="url" name="profile_url" value={profileUrl} disabled={saving} onChange={(event) => setProfileUrl(event.target.value)} /></label>
          {referenceKind && <label>{platform === "douyin" ? "主页 sec_user_id（可留空）" : platform === "kuaishou" ? "主页 eid（可留空）" : "sph 账号标识（可留空）"}<input type="text" name={referenceKind} value={references[referenceKind] ?? ""} disabled={saving} onChange={(event) => setReferences({ ...references, [referenceKind]: event.target.value })} /></label>}
        </div>
        <p className={styles.statusHelp}>{platform === "wechat_channels" ? "视频号需要 finder UID 或 sph 账号标识，作品分享链接不能代替账号身份。" : platform === "kuaishou" ? "快手需要数字 UID、主页 eid 或官方主页链接；仅展示号不足以核验身份。" : "填写的信息必须指向同一账号；有冲突时系统会保留原因。"} 提交成功表示已接收准备任务，完成抓取后才显示已采集。</p>
        {error && <p role="alert">{error}</p>}
        <div className="modal-actions"><button type="button" className="secondary" disabled={saving} onClick={onClose}>取消</button><button type="submit" className="primary" disabled={saving}>{saving ? "正在保存…" : "保存并准备账号"}</button></div>
      </form>
    </section>
  </div>;
}
