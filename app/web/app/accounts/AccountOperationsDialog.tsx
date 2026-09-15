"use client";

import { accountGroupOptions, businessDirectionOptions } from "../lib/accountClassification";
import type { Account, AccountStatus } from "../lib/types";
import AccountDialogLayout, { AccountDialogIcon, AccountDialogIdentity } from "./AccountDialogLayout";
import styles from "./AccountDialogLayout.module.css";

export type EditableAccountStatus = Exclude<AccountStatus, "unmarked"> | "";
export type AccountForm = {
  id: number; phone: string; operatorName: string; accountGroup: string;
  businessDirection: string; accountStatus: EditableAccountStatus;
  originalAccountStatus: EditableAccountStatus;
};

type AccountOperationsDialogProps = {
  form: AccountForm; account?: Account; onChange: (form: AccountForm) => void;
  onClose: () => void; onSave: () => void; saving: boolean; error?: string;
};

export default function AccountOperationsDialog({ form, account, onChange, onClose, onSave, saving, error }: AccountOperationsDialogProps) {
  const directoryOnly = form.id < 0;
  return <AccountDialogLayout id="account-operations" title="修改账号运营信息" description="管理运营资料、账号分类与状态" ariaLabel="编辑账号" busy={saving} onClose={onClose}
    initialFocus={directoryOnly ? "#account-operations-group" : "#account-operations-phone"}
    footer={<>
      <span className={styles.footerHint}><AccountDialogIcon name="shield" />账号运营</span>
      <div className={styles.actions}><button type="button" className={styles.cancelButton} disabled={saving} onClick={onClose}>取消</button><button type="button" className={styles.primaryButton} disabled={saving} onClick={onSave}>{saving ? "保存中" : "保存修改"}</button></div>
    </>}>
    {account && <AccountDialogIdentity account={account} />}
    {directoryOnly && <p className={styles.notice}>尚未补充平台 UID，可先修改账号分组和业务方向；保存分类不会启动采集。</p>}
    <section className={styles.section} aria-labelledby="account-operations-contact-title">
      <h4 className={styles.sectionTitle} id="account-operations-contact-title">运营资料</h4>
      <div className={styles.fields}>
        <div className={styles.field}><label htmlFor="account-operations-phone">手机号<span className={styles.optional}>可留空</span></label><input id="account-operations-phone" className={styles.input} type="tel" autoComplete="off" disabled={directoryOnly || saving} value={form.phone} onChange={(event) => onChange({ ...form, phone: event.target.value })} /></div>
        <div className={styles.field}><label htmlFor="account-operations-operator">运营人员</label><input id="account-operations-operator" className={styles.input} autoComplete="off" disabled={directoryOnly || saving} value={form.operatorName} onChange={(event) => onChange({ ...form, operatorName: event.target.value })} /></div>
      </div>
      {!directoryOnly && <p className={styles.help}>手机号可留空，也可以由多个账号共用。</p>}
    </section>
    <section className={styles.section} aria-labelledby="account-operations-classification-title">
      <h4 className={styles.sectionTitle} id="account-operations-classification-title">分类与状态</h4>
      <div className={styles.fields}>
        <div className={styles.field}><label htmlFor="account-operations-group">账号分组</label><div className={styles.selectControl}><select id="account-operations-group" className={styles.input} name="account_group" disabled={saving} value={form.accountGroup} onChange={(event) => onChange({ ...form, accountGroup: event.target.value })}>{accountGroupOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select><span className={styles.adornment}><AccountDialogIcon name="chevron" /></span></div></div>
        <div className={styles.field}><label htmlFor="account-operations-direction">业务方向</label><div className={styles.selectControl}><select id="account-operations-direction" className={styles.input} name="business_direction" disabled={saving} value={form.businessDirection} onChange={(event) => onChange({ ...form, businessDirection: event.target.value })}>{businessDirectionOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}</select><span className={styles.adornment}><AccountDialogIcon name="chevron" /></span></div></div>
        {form.id > 0 && <div className={`${styles.field} ${styles.fullWidth}`}><label htmlFor="account-operations-status">账号状态</label><div className={styles.selectControl}><select id="account-operations-status" className={styles.input} value={form.accountStatus} disabled={saving} aria-describedby="account-status-help" onChange={(event) => onChange({ ...form, accountStatus: event.target.value as EditableAccountStatus })}>{!form.originalAccountStatus && <option value="" disabled>待标记</option>}<option value="daily">日更</option><option value="weekly">周更</option><option value="paused">暂停</option></select><span className={styles.adornment}><AccountDialogIcon name="chevron" /></span></div><p id="account-status-help" className={styles.help}>账号状态由人工维护；当前所有状态均参与自动采集。</p></div>}
      </div>
    </section>
    {error && <p className={styles.error} role="alert">{error}</p>}
  </AccountDialogLayout>;
}
