"use client";

import { useEffect, useMemo, useState } from "react";
import Image from "next/image";

type MetricBase = {
  status: string;
  qualitative: string;
  scope: string;
  reason: string;
};

type RatioMetric = MetricBase & {
  kind: "ratio";
  numerator: number | null;
  denominator: number;
  percentage: number | null;
};

type ScoreMetric = MetricBase & {
  kind: "score";
  score: number | null;
  scale: 100;
  scorable_items: number;
  total_items: number;
  coverage_percentage: number | null;
};

type Metric = RatioMetric | ScoreMetric;

type DetailScore = {
  score: number | null;
  scale: number;
  status: string;
  qualitative: string;
};

type Detail = {
  content_item_id: number;
  platform_content_id: string;
  canonical_url: string;
  account_name: string;
  account_quality: string;
  caption: string;
  content_type: string;
  exposure_value: number | null;
  evidence_level: string;
  evidence_summary: string;
  valid_unique_commenters: number | null;
  comment_sample_status: string;
  selling_point: {
    id: string;
    label: string;
    tier: string;
    business_scene: string;
    score: number | null;
    qualitative: string;
    included: boolean;
    pending_review: boolean;
    no_match_reason: string;
  };
  content_automotive: DetailScore;
  audience_automotive: DetailScore;
  acquisition_potential: DetailScore;
  dcar_task_fit_score: number | null;
  action_intent_score: number | null;
};

type Distribution = {
  identifiable: RatioMetric;
  selling_point_covered: RatioMetric;
  core_selling_point: RatioMetric;
  other_selling_point: RatioMetric;
  diagnostics?: Record<string, number | null>;
  coverage?: Record<string, number | string | boolean | null>;
};

type Verticality = {
  content_automotive: ScoreMetric;
  audience_automotive: ScoreMetric;
  acquisition_potential: ScoreMetric;
  coverage: Record<string, number | null>;
};

type Scene = {
  publication_n: number;
  count_distribution: Distribution;
  exposure_distribution: Distribution;
  verticality: Verticality;
  scene_internal: {
    core_share_within_scene_publications: RatioMetric;
    selling_point_coverage_within_scene: RatioMetric;
  };
};

type Channel = {
  scope: string;
  denominator: number;
  count_distribution: Distribution;
  exposure_distribution: Distribution;
  verticality: Verticality;
  channel_targets: {
    core_selling_point_publication_share: {
      actual_percentage: number | null;
      minimum_percentage: number;
      maximum_percentage: number;
      status: string;
      gap_to_minimum_percentage_points: number;
    };
  };
  scenes: Record<string, Scene>;
  content_details: Detail[];
};

type Report = {
  report_version: string;
  rule_version: string;
  metadata: {
    run_id: string;
    revision: number;
    generated_at: string;
    corpus_snapshot_id: string;
  };
  run_summary: {
    content_items: number;
    douyin_items: number;
    xiaohongshu_items: number;
    manual_review_count: number;
    provider_usage: Array<{ provider: string; billed_requests: number; amount: number | null }>;
  };
  channels: { douyin: Channel; xiaohongshu: Channel };
  conclusion_summary: Array<{ title: string; text: string }>;
  assets: Array<{ label: string; path: string; type: string }>;
};

type Run = {
  id: string;
  created_at: string;
  mode: string;
  channel: string;
  status: string;
  progress: number;
  input_count: number;
  message: string;
  report_revision?: number;
  is_formal_baseline?: number;
};

type ValidationResult = {
  valid_count: number;
  invalid_count: number;
  total: number;
  invalid: Array<{ value: string; reason: string }>;
};

type OverviewResponse = { recent_runs?: Run[] };
type View = "dashboard" | "new" | "report" | "details" | "assets";
type ChannelKey = "douyin" | "xiaohongshu";

const API_BASE = "http://127.0.0.1:8765";
const channelNames: Record<ChannelKey, string> = { douyin: "抖音", xiaohongshu: "小红书" };
const navItems: Array<{ id: View; label: string; icon: string }> = [
  { id: "dashboard", label: "运行总览", icon: "⌂" },
  { id: "new", label: "新建评估", icon: "＋" },
  { id: "report", label: "结果报告", icon: "▤" },
  { id: "details", label: "内容明细", icon: "≡" },
  { id: "assets", label: "数据资产", icon: "◇" },
];

const distributionOrder: Array<[keyof Distribution, string]> = [
  ["identifiable", "可识别内容"],
  ["selling_point_covered", "卖点覆盖"],
  ["core_selling_point", "核心卖点覆盖"],
  ["other_selling_point", "其他卖点"],
];

const verticalityOrder: Array<[keyof Verticality, string]> = [
  ["content_automotive", "内容汽车性"],
  ["audience_automotive", "互动受众汽车性"],
  ["acquisition_potential", "懂车帝拉新潜力"],
];

function metricValue(metric: Metric) {
  return metric.kind === "score" ? metric.score : metric.percentage;
}

function metricDisplay(metric: Metric) {
  if (metric.kind === "score") {
    return metric.score === null
      ? "暂不可计算"
      : `${metric.score}/100 · 覆盖 ${metric.scorable_items}/${metric.total_items}`;
  }
  return metric.percentage === null
    ? "暂不可计算"
    : `${metric.numerator}/${metric.denominator}（${metric.percentage}%）`;
}

function scoreClass(value: number | null) {
  if (value === null) return "neutral";
  if (value >= 70) return "good";
  if (value >= 40) return "warn";
  return "risk";
}

function statusText(status: string) {
  const map: Record<string, string> = {
    queued: "排队中",
    running: "执行中",
    cancelling: "取消中",
    cancelled: "已取消",
    interrupted: "已中断",
    completed: "已完成",
    failed: "失败",
  };
  return map[status] ?? status;
}

function MetricCard({ label, metric, compact = false }: { label: string; metric: Metric; compact?: boolean }) {
  const value = metricValue(metric);
  const statusLabel = metric.status === "available" ? "全量" : metric.status === "sample_only" ? "样本" : "不可计算";
  return (
    <article className={`metric-card ${compact ? "compact" : ""}`}>
      <div className="metric-card-head">
        <span>{label}</span>
        <span className={`metric-status ${metric.status === "available" ? "available" : "limited"}`}>{statusLabel}</span>
      </div>
      <div className="metric-score-row">
        <strong className={scoreClass(value)}>{value === null ? "—" : value}</strong>
        <span>{value === null ? "暂不可计算" : metric.kind === "score" ? "/ 100" : "%"}</span>
      </div>
      <div className="score-track" aria-label={`${label} ${value ?? 0}`}>
        <i className={scoreClass(value)} style={{ width: `${Math.max(0, Math.min(100, value ?? 0))}%` }} />
      </div>
      <p className="metric-qualitative">{metric.qualitative}</p>
      <p className="metric-display">{metricDisplay(metric)}</p>
    </article>
  );
}

function ChannelSwitch({ value, onChange }: { value: ChannelKey; onChange: (value: ChannelKey) => void }) {
  return (
    <div className="channel-switch" role="tablist" aria-label="选择渠道">
      {(Object.keys(channelNames) as ChannelKey[]).map((key) => (
        <button key={key} className={value === key ? "active" : ""} onClick={() => onChange(key)} role="tab">
          {channelNames[key]}
        </button>
      ))}
    </div>
  );
}

export default function EvaluationDashboard() {
  const [report, setReport] = useState<Report | null>(null);
  const [apiOnline, setApiOnline] = useState(false);
  const [activeView, setActiveView] = useState<View>("dashboard");
  const [activeChannel, setActiveChannel] = useState<ChannelKey>("douyin");
  const [inputChannel, setInputChannel] = useState<ChannelKey>("douyin");
  const [inputText, setInputText] = useState("");
  const [validation, setValidation] = useState<ValidationResult | null>(null);
  const [runs, setRuns] = useState<Run[]>([]);
  const [currentRun, setCurrentRun] = useState<Run | null>(null);
  const [notice, setNotice] = useState("");
  const [search, setSearch] = useState("");
  const [detailLimit, setDetailLimit] = useState(30);
  const [reviewDetail, setReviewDetail] = useState<Detail | null>(null);
  const [reviewReason, setReviewReason] = useState("");
  const [reviewScores, setReviewScores] = useState<Record<string, string>>({});

  useEffect(() => {
    fetch("/data/latest-report.json")
      .then((response) => response.json() as Promise<Report>)
      .then((snapshot) => { if (snapshot?.metadata) setReport(snapshot); })
      .catch(() => setNotice("本地 v7 报告快照加载失败"));

    fetch(`${API_BASE}/api/overview`)
      .then((response) => {
        if (!response.ok) throw new Error("offline");
        return response.json() as Promise<OverviewResponse>;
      })
      .then((overview) => {
        setApiOnline(true);
        setRuns(overview.recent_runs ?? []);
        return fetch(`${API_BASE}/api/report/latest`);
      })
      .then((response) => response.json() as Promise<Report>)
      .then((latest) => setReport(latest))
      .catch(() => setApiOnline(false));
  }, []);

  useEffect(() => {
    if (!currentRun || !["queued", "running", "cancelling"].includes(currentRun.status)) return;
    const timer = window.setInterval(() => {
      fetch(`${API_BASE}/api/runs/${currentRun.id}`)
        .then((response) => response.json() as Promise<Run>)
        .then(async (run) => {
          setCurrentRun(run);
          setRuns((previous) => [run, ...previous.filter((item) => item.id !== run.id)]);
          if (run.status === "completed") {
            const result = await fetch(`${API_BASE}/api/runs/${run.id}/report`);
            if (result.ok) setReport(await result.json() as Report);
            setNotice("v7 报告已生成，可检查后设为正式基线");
          } else if (["failed", "cancelled", "interrupted"].includes(run.status)) {
            setNotice(run.message);
          }
        })
        .catch(() => setApiOnline(false));
    }, 900);
    return () => window.clearInterval(timer);
  }, [currentRun]);

  const channel = report?.channels[activeChannel];
  const details = useMemo(() => {
    if (!channel) return [];
    const keyword = search.trim().toLowerCase();
    const filtered = keyword
      ? channel.content_details.filter((item) =>
          [item.platform_content_id, item.caption, item.account_name, item.selling_point.business_scene, item.selling_point.label]
            .some((value) => String(value ?? "").toLowerCase().includes(keyword)),
        )
      : channel.content_details;
    return filtered.slice(0, detailLimit);
  }, [channel, search, detailLimit]);

  async function validateInput() {
    if (!inputText.trim()) {
      setValidation({ valid_count: 0, invalid_count: 0, total: 0, invalid: [] });
      return;
    }
    if (apiOnline) {
      const response = await fetch(`${API_BASE}/api/inputs/validate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ channel: inputChannel, text: inputText }),
      });
      setValidation(await response.json() as ValidationResult);
      return;
    }
    const items = inputText.split(/[\n,]+/).map((item) => item.trim()).filter(Boolean);
    const matcher = inputChannel === "douyin" ? /(^\d{6,24}$)|douyin\.com/i : /xiaohongshu\.com/i;
    const invalid = items.filter((item) => !matcher.test(item)).map((item) => ({ value: item, reason: "格式不符合所选渠道" }));
    setValidation({ total: items.length, valid_count: items.length - invalid.length, invalid_count: invalid.length, invalid });
  }

  async function runFullReport() {
    if (!apiOnline) {
      setNotice("请先启动本地任务服务");
      return;
    }
    const response = await fetch(`${API_BASE}/api/runs/full`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    const run = await response.json() as Run;
    if (!response.ok) {
      setNotice((run as unknown as { error?: string }).error ?? "任务创建失败");
      return;
    }
    setCurrentRun(run);
    setRuns((previous) => [run, ...previous.filter((item) => item.id !== run.id)]);
    setNotice("全量 v7 任务已进入单任务队列");
  }

  async function promoteCurrentRun() {
    if (!currentRun) return;
    const response = await fetch(`${API_BASE}/api/runs/${currentRun.id}/baseline`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    const value = await response.json() as Run & { error?: string };
    if (!response.ok) {
      setNotice(value.error ?? "正式基线设置失败");
      return;
    }
    setCurrentRun(value);
    setNotice(`已将 ${value.id} 设为正式基线`);
  }

  function openReview(detail: Detail) {
    setReviewDetail(detail);
    setReviewReason("");
    setReviewScores({
      content_automotive_score: detail.content_automotive.score?.toString() ?? "",
      audience_automotive_score: detail.audience_automotive.score?.toString() ?? "",
      dcar_task_fit_score: detail.dcar_task_fit_score?.toString() ?? "",
      action_intent_score: detail.action_intent_score?.toString() ?? "",
    });
  }

  async function submitReview() {
    if (!reviewDetail || !report || !reviewReason.trim()) {
      setNotice("人工复核必须填写理由");
      return;
    }
    const patch: Record<string, number | null> = {};
    Object.entries(reviewScores).forEach(([key, value]) => {
      patch[key] = value.trim() === "" ? null : Number(value);
    });
    const response = await fetch(`${API_BASE}/api/runs/${report.metadata.run_id}/reviews`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        content_item_id: reviewDetail.content_item_id,
        patch,
        reason: reviewReason,
      }),
    });
    const value = await response.json() as Report & { error?: string };
    if (!response.ok) {
      setNotice(value.error ?? "人工复核提交失败");
      return;
    }
    setReport(value);
    setReviewDetail(null);
    setNotice(`复核已应用，报告升级到 revision ${value.metadata.revision}`);
  }

  if (!report) {
    return <main className="loading-screen"><div className="loading-mark">D</div><p>正在读取 v7 正式报告…</p></main>;
  }

  const target = channel!.channel_targets.core_selling_point_publication_share;

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand"><div className="brand-mark">D</div><div><strong>DCar Insight</strong><span>内容评估工作台</span></div></div>
        <nav><p>工作区</p>{navItems.map((item) => <button key={item.id} className={activeView === item.id ? "active" : ""} onClick={() => setActiveView(item.id)}><span>{item.icon}</span>{item.label}</button>)}</nav>
        <div className="sidebar-foot"><span className={`live-dot ${apiOnline ? "online" : ""}`} /><div><strong>{apiOnline ? "本地服务已连接" : "v7 快照模式"}</strong><span>证据不足不补 0</span></div></div>
      </aside>

      <main className="main-area">
        <header className="topbar">
          <div><span className="eyebrow">DCar / 双渠道内容评估</span><h1>{navItems.find((item) => item.id === activeView)?.label}</h1></div>
          <div className="topbar-actions"><span className="rule-chip">规则 v5 · 报告 v7 · 修订 {report.metadata.revision}</span><button className="primary small" onClick={() => setActiveView("new")}>＋ 新建运行</button></div>
        </header>

        {notice && <div className="notice" role="status"><span>i</span>{notice}<button onClick={() => setNotice("")}>×</button></div>}

        {activeView === "dashboard" && (
          <div className="page-stack">
            <section className="hero-panel">
              <div><span className="success-pill">✓ v7 正式基线已验证</span><h2>从完整媒体证据到业务卖点与拉新潜力</h2><p>当前报告由 SQLite 正式基线动态读取。正文、视频、ASR、OCR、评论和曝光边界均可追溯。</p><div className="hero-actions"><button className="primary" onClick={runFullReport}>生成全量 v7 报告</button><button className="secondary" onClick={() => setActiveView("report")}>查看结构化结论</button></div></div>
              <div className="hero-summary"><div><span>已评估内容</span><strong>{report.run_summary.content_items}</strong><small>抖音 {report.run_summary.douyin_items} · 小红书 {report.run_summary.xiaohongshu_items}</small></div><div><span>正式运行</span><strong>{report.metadata.run_id}</strong><small>revision {report.metadata.revision}</small></div><div><span>人工复核</span><strong>{report.run_summary.manual_review_count}</strong><small>每次复核均生成新修订</small></div></div>
            </section>
            <section className="section-block"><div className="section-heading"><div><span className="eyebrow">FORMAL BASELINE</span><h2>双渠道关键结论</h2></div><ChannelSwitch value={activeChannel} onChange={setActiveChannel} /></div><div className="metric-grid"><MetricCard label="卖点覆盖（条数）" metric={channel!.count_distribution.selling_point_covered} /><MetricCard label="核心卖点覆盖（条数）" metric={channel!.count_distribution.core_selling_point} /><MetricCard label="内容汽车性" metric={channel!.verticality.content_automotive} /><MetricCard label="懂车帝拉新潜力" metric={channel!.verticality.acquisition_potential} /></div></section>
            <section className="two-column">
              <article className="panel"><div className="panel-head"><div><span className="eyebrow">WORKFLOW</span><h3>当前执行链路</h3></div><span className="mode-chip">单任务队列</span></div><ol className="workflow-list">{[["01", "证据预检", "先读终态缓存，避免重复调用"], ["02", "完整媒体判断", "视频需完整视频与 ASR/OCR，封面不冒充正片"], ["03", "v5 逐条评估", "卖点与三个命题统一输出分值和定性说明"], ["04", "v7 版本化报告", "汇总、三个场景、明细、图片与复核审计"]].map(([no, title, text]) => <li key={no}><b>{no}</b><div><strong>{title}</strong><span>{text}</span></div></li>)}</ol></article>
              <article className="panel"><div className="panel-head"><div><span className="eyebrow">RECENT RUNS</span><h3>最近任务</h3></div></div>{runs.length ? <div className="run-list">{runs.slice(0, 5).map((run) => <div key={run.id} className="run-row"><span className={`run-icon ${run.status}`}>{run.status === "completed" ? "✓" : "↻"}</span><div><strong>{run.mode} · {run.input_count} 条</strong><span>{run.message}</span></div><em>{statusText(run.status)}</em></div>)}</div> : <div className="empty-state"><strong>尚无运行记录</strong><span>可以生成第一份全量 v7 报告。</span></div>}</article>
            </section>
          </div>
        )}

        {activeView === "new" && (
          <div className="page-stack narrow">
            <section className="panel form-panel"><div className="section-heading"><div><span className="eyebrow">INPUT PREFLIGHT</span><h2>校验待评估链接或账号 UID</h2><p>已有语料优先复用缓存；本页面不会因重复点击无限调用供应商。</p></div><span className="safe-chip">最多两次尝试</span></div><label className="field-label">内容渠道</label><ChannelSwitch value={inputChannel} onChange={(value) => { setInputChannel(value); setValidation(null); }} /><label className="field-label" htmlFor="content-input">链接或账号 UID</label><textarea id="content-input" value={inputText} onChange={(event) => setInputText(event.target.value)} placeholder={inputChannel === "douyin" ? "每行一个抖音内容链接或账号 UID" : "每行一个小红书内容链接"} /><div className="form-foot"><span>支持换行或英文逗号分隔，自动去重。</span><button className="primary" onClick={validateInput}>校验输入</button></div></section>
            {validation && <section className="panel validation-panel"><div className="validation-numbers"><div><strong>{validation.total}</strong><span>输入总数</span></div><div className="good"><strong>{validation.valid_count}</strong><span>格式有效</span></div><div className={validation.invalid_count ? "risk" : ""}><strong>{validation.invalid_count}</strong><span>需修正</span></div></div>{validation.invalid_count > 0 && <div className="invalid-list">{validation.invalid.slice(0, 8).map((item, index) => <p key={index}><code>{item.value}</code><span>{item.reason}</span></p>)}</div>}</section>}
            <section className="panel cache-run-panel"><div><span className="eyebrow">FULL CORPUS RUN</span><h3>现有 {report.run_summary.content_items} 条内容全量重建</h3><p>复用本地证据生成新的运行快照和 v7 报告，不调用付费 API。</p></div><button className="secondary" onClick={runFullReport}>{currentRun && ["queued", "running"].includes(currentRun.status) ? "执行中…" : "生成全量报告"}</button>{currentRun && <div className="run-progress"><div><span>{currentRun.message}</span><strong>{currentRun.progress}%</strong></div><div className="progress-track"><i style={{ width: `${currentRun.progress}%` }} /></div>{currentRun.status === "completed" && !currentRun.is_formal_baseline && <button className="primary promote-button" onClick={promoteCurrentRun}>设为正式基线</button>}</div>}</section>
          </div>
        )}

        {activeView === "report" && (
          <div className="page-stack">
            <section className="report-intro"><div><span className="eyebrow">RUN {report.metadata.run_id} · REV {report.metadata.revision}</span><h2>双渠道结构化结论</h2><p>每个数值同时保留分母、覆盖状态、定性说明和数据边界。</p></div><ChannelSwitch value={activeChannel} onChange={setActiveChannel} /></section>
            <section className="panel"><div className="panel-head"><div><span className="channel-kicker">{channelNames[activeChannel]}</span><h3>1、汇总</h3><p>{channel!.scope}</p></div><strong className="denominator">{channel!.denominator}<span>条发布</span></strong></div><div className="target-line">核心卖点生产占比 {target.actual_percentage ?? "—"}% · 目标 60%–70% · {target.status === "within_target" ? "已达标" : `距最低目标 ${target.gap_to_minimum_percentage_points} 个百分点`}</div>
              <div className="metric-section"><h4>条数维度</h4><div className="metric-grid report-grid">{distributionOrder.map(([key, label]) => <MetricCard key={`count-${String(key)}`} label={label} metric={channel!.count_distribution[key] as RatioMetric} compact />)}</div></div>
              <div className="metric-section"><h4>曝光维度</h4><div className="metric-grid report-grid">{distributionOrder.map(([key, label]) => <MetricCard key={`exposure-${String(key)}`} label={label} metric={channel!.exposure_distribution[key] as RatioMetric} compact />)}</div></div>
              <div className="metric-section"><h4>内容垂直度</h4><div className="metric-grid vertical-grid">{verticalityOrder.map(([key, label]) => <MetricCard key={String(key)} label={label} metric={channel!.verticality[key] as ScoreMetric} compact />)}</div></div>
            </section>
            <section className="panel"><div className="panel-head"><div><h3>2、三个业务场景</h3><p>场景条数指标仍以渠道全部发布为分母；60%–70% 目标只在渠道级展示。</p></div></div><div className="scene-grid">{Object.entries(channel!.scenes).map(([name, scene]) => <article key={name} className="scene-card"><div><span>{name}</span><strong>{scene.publication_n}<small> 条</small></strong></div><dl><dt>卖点覆盖</dt><dd>{metricDisplay(scene.count_distribution.selling_point_covered)}</dd><dt>场景内部核心占比</dt><dd>{metricDisplay(scene.scene_internal.core_share_within_scene_publications)}</dd><dt>内容汽车性</dt><dd>{metricDisplay(scene.verticality.content_automotive)}</dd><dt>互动受众汽车性</dt><dd>{metricDisplay(scene.verticality.audience_automotive)}</dd><dt>拉新潜力</dt><dd>{metricDisplay(scene.verticality.acquisition_potential)}</dd></dl><p>{scene.verticality.acquisition_potential.qualitative}</p></article>)}</div></section>
            <section className="conclusion-panel"><span className="eyebrow">结论摘要</span><h3>正式基线的动态结论</h3><div className="conclusion-grid">{report.conclusion_summary.map((item, index) => <p key={`${item.title}-${index}`}><b>{String(index + 1).padStart(2, "0")}</b><span><strong>{item.title}</strong><br />{item.text}</span></p>)}</div></section>
          </div>
        )}

        {activeView === "details" && (
          <div className="page-stack"><section className="detail-toolbar"><div><span className="eyebrow">CONTENT DETAILS</span><h2>逐条内容明细</h2><p>当前显示 {channel!.content_details.length} 条；证据不足项不生成伪分值。</p></div><div><ChannelSwitch value={activeChannel} onChange={(value) => { setActiveChannel(value); setDetailLimit(30); }} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="搜索内容ID、账号、卖点或场景" aria-label="搜索内容明细" /></div></section><section className="panel table-panel"><div className="table-scroll"><table><thead><tr><th>内容</th><th>证据/场景</th><th>卖点判断</th><th>内容汽车性</th><th>互动受众</th><th>拉新潜力</th><th>复核</th></tr></thead><tbody>{details.map((item) => <tr key={item.content_item_id}><td><a href={item.canonical_url} target="_blank" rel="noreferrer">{item.platform_content_id} · {item.caption || "无标题"}</a><span>{item.account_name || channelNames[activeChannel]} · 曝光 {item.exposure_value ?? "缺失"}</span></td><td><span className="scene-tag">{item.evidence_level} · {item.selling_point.business_scene || "未分场景"}</span><br />{item.evidence_summary}</td><td>{item.selling_point.label || item.selling_point.no_match_reason || "未命中"}<br />{item.selling_point.score === null ? "暂不可计算" : `${item.selling_point.score}/100`} · {item.selling_point.qualitative}</td><td>{item.content_automotive.score === null ? "暂不可计算" : `${item.content_automotive.score}/100`}<br />{item.content_automotive.qualitative}</td><td>{item.audience_automotive.score === null ? "暂不可计算" : `${item.audience_automotive.score}/100`}<br />有效评论用户 {item.valid_unique_commenters ?? "—"}</td><td>{item.acquisition_potential.score === null ? "暂不可计算" : `${item.acquisition_potential.score}/100`}<br />{item.acquisition_potential.qualitative}</td><td><button className="text-button" disabled={!apiOnline} onClick={() => openReview(item)}>人工复核</button></td></tr>)}</tbody></table></div>{details.length < channel!.content_details.length && <button className="load-more" onClick={() => setDetailLimit((value) => value + 30)}>再显示 30 条</button>}</section></div>
        )}

        {activeView === "assets" && (
          <div className="page-stack"><section className="report-intro"><div><span className="eyebrow">DATA ASSETS</span><h2>数据资产与导出</h2><p>所有导出文件来自当前正式运行，不再绑定固定日期文件名。</p></div><span className="success-pill">✓ v7 合同通过</span></section><section className="asset-grid">{[{ title: "内容总量", value: report.run_summary.content_items.toString(), note: "抖音与小红书正式语料" }, { title: "报告版本", value: "v7", note: `规则 v5 · revision ${report.metadata.revision}` }, { title: "人工复核", value: report.run_summary.manual_review_count.toString(), note: "保留每次修订与原因" }, { title: "本地任务数据库", value: apiOnline ? "已连接" : "快照", note: report.metadata.corpus_snapshot_id }].map((item) => <article key={item.title} className="asset-card"><span>{item.title}</span><strong>{item.value}</strong><p>{item.note}</p></article>)}</section><section className="panel"><div className="panel-head"><div><span className="eyebrow">EXPORTS</span><h3>配套文件</h3></div></div><div className="export-list">{[["结构化结论 JSON", "report-json", "机器读取与二次分析"], ["完整报告 Markdown", "report-markdown", "汇总、三个场景与逐条明细"], ["抖音逐条结果 CSV", "douyin-csv", `${report.run_summary.douyin_items} 条抖音结果`], ["小红书逐条结果 CSV", "xiaohongshu-csv", `${report.run_summary.xiaohongshu_items} 条小红书结果`], ["核心结论分享图", "summary-image", "1600×1600 PNG，适合转发"]].map(([title, key, note]) => apiOnline ? <a key={key} href={`${API_BASE}/api/files/${key}`} download><span className="file-icon">⇩</span><div><strong>{title}</strong><span>{note}</span></div><em>下载</em></a> : <div className="offline-export" key={key}><span className="file-icon">·</span><div><strong>{title}</strong><span>启动本地服务后可下载</span></div></div>)}</div></section>{apiOnline && <section className="panel image-preview"><div className="panel-head"><div><span className="eyebrow">SHARE IMAGE</span><h3>核心结论图片</h3></div></div><Image src={`${API_BASE}/api/files/summary-image`} alt="双渠道核心结论分享图" width={1600} height={1600} unoptimized /></section>}<section className="panel guardrail"><div className="guardrail-icon">!</div><div><h3>数据边界</h3><p>当前只评价懂车帝拉新潜力，不输出实际新增效果。互动受众少于 20 个有效独立评论用户时，受众和拉新潜力均暂不可计算。</p></div></section></div>
        )}
      </main>

      {reviewDetail && (
        <div className="modal-backdrop" role="dialog" aria-modal="true" aria-label="人工复核">
          <section className="review-modal"><div className="panel-head"><div><span className="eyebrow">MANUAL REVIEW</span><h3>人工复核 · {reviewDetail.platform_content_id}</h3><p>提交后会重算该运行并生成新的报告修订。</p></div><button className="modal-close" onClick={() => setReviewDetail(null)}>×</button></div><div className="review-fields">{[["content_automotive_score", "内容汽车性"], ["audience_automotive_score", "互动受众汽车性"], ["dcar_task_fit_score", "懂车帝任务承接"], ["action_intent_score", "行动意图"]].map(([key, label]) => <label key={key}><span>{label}（0–100，空值表示不可计算）</span><input type="number" min="0" max="100" value={reviewScores[key] ?? ""} onChange={(event) => setReviewScores((previous) => ({ ...previous, [key]: event.target.value }))} /></label>)}</div><label className="review-reason"><span>复核理由</span><textarea value={reviewReason} onChange={(event) => setReviewReason(event.target.value)} placeholder="说明依据，例如：完整视频复核后确认汽车为主体。" /></label><div className="modal-actions"><button className="secondary" onClick={() => setReviewDetail(null)}>取消</button><button className="primary" onClick={submitReview}>提交并重算</button></div></section>
        </div>
      )}
    </div>
  );
}
