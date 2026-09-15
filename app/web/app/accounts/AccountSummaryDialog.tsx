"use client";

import { formatDateTime } from "../lib/format";
import type { Account } from "../lib/types";
import AccountDialogLayout, { AccountDialogIdentity } from "./AccountDialogLayout";
import styles from "./AccountSummaryDialog.module.css";

const accountSummaryGroups = [
  {
    id: "identity",
    title: "账号信息",
    fields: ["平台", "账号名称", "ID（抖音号/快手号/小红书号/视频号）", "uid", "粉丝", "更新状态"],
  },
  {
    id: "operations",
    title: "运营信息",
    fields: ["运营人员", "质量标签", "业务标签", "是否开通接单"],
  },
  {
    id: "contact",
    title: "联系与实名",
    fields: ["手机号", "手机号开卡人姓名", "使用人证件号码", "持卡人", "是否实名", "实名来源"],
  },
] as const;

function summaryValue(value: string | number | null | undefined) {
  return value == null || (typeof value === "string" && !value.trim()) ? "—" : String(value);
}

const preparationReasonFallbacks: Record<string, string> = {
  preparation_retry_raw_missing: "缺少可确认的完整原始响应证据",
  preparation_billing_unverified: "计费或退款结果尚未核清",
};

function preparationReasonLabel(preparation: NonNullable<Account["account_preparation"]>) {
  const detail = preparation.reason_label?.trim() || "";
  const reason = preparation.reason?.trim() || detail;
  return !detail || detail === reason ? preparationReasonFallbacks[reason] || detail || "请查看详细信息。" : detail;
}

export default function AccountSummaryDialog({ account, onClose }: { account: Account; onClose: () => void }) {
  const summary = account.account_summary;
  const preparationIssue = account.account_preparation?.state === "blocked" ? account.account_preparation : null;
  const captureIssue = account.automatic_capture?.eligible === false ? account.automatic_capture : null;
  return <AccountDialogLayout id="account-summary" title="账号资料" description="查看导入资料与来源记录" wide onClose={onClose}>
    <AccountDialogIdentity account={account} />
    {(preparationIssue || captureIssue) && <section className={styles.source} aria-labelledby="account-capture-issue-title">
      <h3 className={styles.sourceTitle} id="account-capture-issue-title">采集说明</h3>
      {preparationIssue && <p className={styles.captureIssue}>准备失败：{preparationReasonLabel(preparationIssue)}</p>}
      {captureIssue && <p className={styles.captureIssue}>暂不自动采集：{captureIssue.reason_code === "identity_unverified" ? "待补齐账号资料" : captureIssue.reason_label || "请查看详细信息。"}</p>}
      <details className={styles.comments}>
        <summary><span>详细信息</span><svg aria-hidden="true" viewBox="0 0 20 20" fill="none"><path d="m6 8 4 4 4-4" /></svg></summary>
        {preparationIssue && <p>{preparationIssue.message}<br />原因代码：{preparationIssue.reason || "—"}</p>}
        {captureIssue && <p>原因代码：{captureIssue.reason_code || "—"}</p>}
      </details>
    </section>}
    <p className={styles.contextNote}>以下为导入资料，未提供的信息显示“—”。运营人员、开卡人、证件使用人及持卡人分别记录。</p>
    {!summary && <p className={styles.emptyNotice}>此账号尚无导入资料。</p>}
    {Boolean(summary?.pending_fields?.length) && <p className={styles.pendingNotice} role="status">
      <strong>待核实字段</strong><span>{summary?.pending_fields?.join("、")}。原值及原因见下方批注。</span>
    </p>}
    <div className={styles.groups}>
      {accountSummaryGroups.map((group) => <section className={styles.group} key={group.id} aria-labelledby={`account-summary-${group.id}`}>
        <h3 className={styles.groupTitle} id={`account-summary-${group.id}`}>{group.title}</h3>
        <dl className={styles.fields}>
          {group.fields.map((field) => <div className={styles.field} key={field}>
            <dt>{field}</dt>
            <dd data-account-summary-field={field}>{summaryValue(summary?.fields[field])}</dd>
          </div>)}
        </dl>
      </section>)}
    </div>
    {summary && <section className={styles.source} aria-labelledby="account-summary-source-title">
      <h3 className={styles.sourceTitle} id="account-summary-source-title">来源记录</h3>
      <dl className={styles.sourceFields}>
        <div><dt>来源</dt><dd>{summaryValue(summary.source_name)}</dd></div>
        <div><dt>工作表</dt><dd>{summaryValue(summary.source_sheet)} · 第 {summary.source_row} 行</dd></div>
        <div className={styles.importedAt}><dt>导入时间</dt><dd><time dateTime={summary.imported_at}>{formatDateTime(summary.imported_at)}</time><span>（不是实时采集时间）</span></dd></div>
      </dl>
    </section>}
    <details className={styles.comments}>
      <summary tabIndex={0}><span>来源批注与待核实说明</span><svg aria-hidden="true" viewBox="0 0 20 20" fill="none"><path d="m6 8 4 4 4-4" /></svg></summary>
      <p>{summary?.comment || "无批注"}</p>
    </details>
  </AccountDialogLayout>;
}
