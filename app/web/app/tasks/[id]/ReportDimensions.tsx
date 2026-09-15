import { accountGroupLabel, businessDirectionLabel } from "../../lib/accountClassification";
import { label } from "../../lib/format";
import type { ReportView } from "../../lib/types";

export default function ReportDimensions({ report }: { report?: ReportView | null }) {
  return <div className="task-tab-body">
    <h4>账号分组</h4>
    <div className="dimension-list">{report?.account_group_dimensions?.map((item) => <div key={String(item.key)}>
      <strong>{accountGroupLabel(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span>
    </div>)}</div>
    <h4>业务方向</h4>
    <div className="dimension-list">{report?.business_direction_dimensions?.map((item) => <div key={String(item.key)}>
      <strong>{businessDirectionLabel(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span>
    </div>)}</div>
    {report && !report.account_group_dimensions && !report.business_direction_dimensions && <p className="empty-explanation">这份报告尚未记录账号分组和业务方向。</p>}
    <h4>作品内容方向</h4>
    <div className="dimension-list">{report?.content_direction_dimensions?.map((item) => <div key={String(item.key)}>
      <strong>{label(String(item.key))}</strong><span>{item.count} 条 · {item.percentage ?? "—"}%</span>
    </div>)}</div>
  </div>;
}
