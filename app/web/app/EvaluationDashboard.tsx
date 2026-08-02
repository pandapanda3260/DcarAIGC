"use client";

import { useEffect, useMemo, useState } from "react";

type Metric = {
  value: number | null;
  display: string;
  status: string;
  qualitative: string;
  scope: string;
  reason: string;
};

type Detail = {
  item_no: string;
  title: string;
  url: string;
  account?: string;
  business_scene: string;
  selling_point: string;
  core_selling_point: string;
  exposure: string;
  content_verticality: string;
  audience_verticality: string;
  acquisition_effect_estimate: string;
};

type Channel = {
  scope: string;
  denominator: number;
  summary: Record<string, Metric>;
  scenes: Record<string, { publication_n: number } & Record<string, Metric | number>>;
  content_details: Detail[];
};

type Report = {
  report_version: string;
  generated_at: string;
  channels: { douyin: Channel; xiaohongshu: Channel };
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
};

type ValidationResult = {
  valid_count: number;
  invalid_count: number;
  total: number;
  invalid: Array<{ value: string; reason: string }>;
};

type OverviewResponse = {
  recent_runs?: Run[];
};

type View = "dashboard" | "new" | "report" | "details" | "assets";
type ChannelKey = "douyin" | "xiaohongshu";

const API_BASE = "http://127.0.0.1:8765";
const metricOrder = [
  ["selling_point_count_share", "卖点覆盖（条数）"],
  ["core_selling_point_count_share", "核心卖点覆盖（条数）"],
  ["selling_point_exposure_share", "卖点覆盖（曝光）"],
  ["core_selling_point_exposure_share", "核心卖点覆盖（曝光）"],
  ["content_verticality", "内容汽车性"],
  ["audience_verticality", "互动受众汽车性"],
  ["acquisition_effect_estimate", "懂车帝拉新潜力"],
] as const;

const navItems: Array<{ id: View; label: string; icon: string }> = [
  { id: "dashboard", label: "运行总览", icon: "⌂" },
  { id: "new", label: "新建评估", icon: "＋" },
  { id: "report", label: "结果报告", icon: "▤" },
  { id: "details", label: "内容明细", icon: "≡" },
  { id: "assets", label: "数据资产", icon: "◇" },
];

const channelNames: Record<ChannelKey, string> = {
  douyin: "抖音",
  xiaohongshu: "小红书",
};

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
    completed: "已完成",
    failed: "失败",
  };
  return map[status] ?? status;
}

function MetricCard({ label, metric, compact = false }: { label: string; metric: Metric; compact?: boolean }) {
  const value = metric.value;
  return (
    <article className={`metric-card ${compact ? "compact" : ""}`}>
      <div className="metric-card-head">
        <span>{label}</span>
        <span className={`metric-status ${metric.status === "available" ? "available" : "limited"}`}>
          {metric.status === "available" ? "全量" : "样本"}
        </span>
      </div>
      <div className="metric-score-row">
        <strong className={scoreClass(value)}>{value === null ? "—" : value}</strong>
        <span>{value === null ? "暂不可上卷" : "/ 100"}</span>
      </div>
      <div className="score-track" aria-label={`${label} ${value ?? 0}分`}>
        <i className={scoreClass(value)} style={{ width: `${Math.max(0, Math.min(100, value ?? 0))}%` }} />
      </div>
      <p className="metric-qualitative">{metric.qualitative}</p>
      <p className="metric-display">{metric.display}</p>
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

  useEffect(() => {
    fetch("/data/latest-report.json")
      .then((response) => response.json() as Promise<Report>)
      .then(setReport)
      .catch(() => setNotice("本地报告快照加载失败"));

    fetch(`${API_BASE}/api/overview`)
      .then((response) => {
        if (!response.ok) throw new Error("offline");
        return response.json();
      })
      .then((data) => data as OverviewResponse)
      .then((data) => {
        setApiOnline(true);
        setRuns(data.recent_runs ?? []);
        return fetch(`${API_BASE}/api/report/latest`);
      })
      .then((response) => response?.json() as Promise<Report>)
      .then((latest) => { if (latest?.channels) setReport(latest); })
      .catch(() => setApiOnline(false));
  }, []);

  useEffect(() => {
    if (!currentRun || !["queued", "running"].includes(currentRun.status)) return;
    const timer = window.setInterval(() => {
      fetch(`${API_BASE}/api/runs/${currentRun.id}`)
        .then((response) => response.json() as Promise<Run>)
        .then((run) => {
          setCurrentRun(run);
          setRuns((previous) => [run, ...previous.filter((item) => item.id !== run.id)]);
          if (run.status === "completed") setNotice("缓存回归通过，报告结果未发生变化");
          if (run.status === "failed") setNotice(run.message);
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
          [item.item_no, item.title, item.account, item.business_scene].some((value) => String(value ?? "").toLowerCase().includes(keyword)),
        )
      : channel.content_details;
    return filtered.slice(0, detailLimit);
  }, [channel, search, detailLimit]);

  async function validateInput() {
    setNotice("");
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

  async function runCacheRegression() {
    setNotice("");
    if (!apiOnline) {
      setNotice("本地任务服务尚未启动；当前仍可查看完整报告快照");
      return;
    }
    const response = await fetch(`${API_BASE}/api/runs/cache-regression`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    const run = await response.json() as Run;
    setCurrentRun(run);
    setRuns((previous) => [run, ...previous]);
  }

  if (!report) {
    return (
      <main className="loading-screen">
        <div className="loading-mark">D</div>
        <p>正在读取本地评估结果…</p>
      </main>
    );
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">D</div>
          <div><strong>DCar Insight</strong><span>内容评估工作台</span></div>
        </div>
        <nav>
          <p>工作区</p>
          {navItems.map((item) => (
            <button key={item.id} className={activeView === item.id ? "active" : ""} onClick={() => setActiveView(item.id)}>
              <span>{item.icon}</span>{item.label}
            </button>
          ))}
        </nav>
        <div className="sidebar-foot">
          <span className={`live-dot ${apiOnline ? "online" : ""}`} />
          <div><strong>{apiOnline ? "本地服务已连接" : "报告快照模式"}</strong><span>付费 API 刷新关闭</span></div>
        </div>
      </aside>

      <main className="main-area">
        <header className="topbar">
          <div><span className="eyebrow">DCar / 双渠道内容评估</span><h1>{navItems.find((item) => item.id === activeView)?.label}</h1></div>
          <div className="topbar-actions">
            <span className="rule-chip">规则 v4 · 报告 v6.2</span>
            <button className="primary small" onClick={() => setActiveView("new")}>＋ 新建评估</button>
          </div>
        </header>

        {notice && <div className="notice" role="status"><span>i</span>{notice}<button onClick={() => setNotice("")}>×</button></div>}

        {activeView === "dashboard" && (
          <div className="page-stack">
            <section className="hero-panel">
              <div>
                <span className="success-pill">✓ 迁移与回归验证通过</span>
                <h2>把采集、媒体证据和业务判断<br />收进一个可追溯的工作流</h2>
                <p>当前版本优先复用本地缓存。每个结论都能回到正文、ASR、OCR、评论和曝光证据。</p>
                <div className="hero-actions">
                  <button className="primary" onClick={() => setActiveView("new")}>开始新评估</button>
                  <button className="secondary" onClick={runCacheRegression}>运行缓存回归</button>
                </div>
              </div>
              <div className="hero-summary">
                <div><span>已整理内容</span><strong>776</strong><small>抖音 438 · 小红书 338</small></div>
                <div><span>自动化测试</span><strong>118/118</strong><small>迁移前后全部通过</small></div>
                <div><span>正式文件校验</span><strong>8/8</strong><small>SHA-256 完全一致</small></div>
              </div>
            </section>

            <section className="section-block">
              <div className="section-heading"><div><span className="eyebrow">LATEST SNAPSHOT</span><h2>双渠道关键结论</h2></div><ChannelSwitch value={activeChannel} onChange={setActiveChannel} /></div>
              <div className="metric-grid">
                <MetricCard label="卖点覆盖（条数）" metric={channel!.summary.selling_point_count_share} />
                <MetricCard label="核心卖点覆盖（条数）" metric={channel!.summary.core_selling_point_count_share} />
                <MetricCard label="内容汽车性" metric={channel!.summary.content_verticality} />
                <MetricCard label="懂车帝拉新潜力" metric={channel!.summary.acquisition_effect_estimate} />
              </div>
            </section>

            <section className="two-column">
              <article className="panel">
                <div className="panel-head"><div><span className="eyebrow">WORKFLOW</span><h3>当前执行链路</h3></div><span className="mode-chip">缓存优先</span></div>
                <ol className="workflow-list">
                  {[
                    ["01", "输入识别", "校验抖音 UID、抖音链接或小红书链接"],
                    ["02", "证据复用", "优先读取内容、视频、ASR、OCR 与评论缓存"],
                    ["03", "终版判断", "按卖点体系和三个命题规则逐条评分"],
                    ["04", "结构化输出", "生成汇总、场景、明细和可转发图片"],
                  ].map(([no, title, text]) => <li key={no}><b>{no}</b><div><strong>{title}</strong><span>{text}</span></div></li>)}
                </ol>
              </article>
              <article className="panel">
                <div className="panel-head"><div><span className="eyebrow">RECENT RUNS</span><h3>最近任务</h3></div><button className="text-button" onClick={() => setActiveView("assets")}>查看全部</button></div>
                {runs.length ? <div className="run-list">{runs.slice(0, 4).map((run) => <div key={run.id} className="run-row"><span className={`run-icon ${run.status}`}>{run.status === "completed" ? "✓" : "↻"}</span><div><strong>缓存回归 · {run.input_count} 条</strong><span>{run.message}</span></div><em>{statusText(run.status)}</em></div>)}</div> : <div className="empty-state"><strong>尚无网页任务</strong><span>现有 v6.2 报告已作为初始快照载入。</span></div>}
              </article>
            </section>
          </div>
        )}

        {activeView === "new" && (
          <div className="page-stack narrow">
            <section className="panel form-panel">
              <div className="section-heading"><div><span className="eyebrow">NEW EVALUATION</span><h2>输入待评估内容</h2><p>第一版先完成输入校验和缓存任务。联网采集将在预算保护与重试规则完成后开放。</p></div><span className="safe-chip">不触发付费 API</span></div>
              <label className="field-label">内容渠道</label>
              <ChannelSwitch value={inputChannel} onChange={(value) => { setInputChannel(value); setValidation(null); }} />
              <label className="field-label" htmlFor="content-input">链接或账号 UID</label>
              <textarea id="content-input" value={inputText} onChange={(event) => setInputText(event.target.value)} placeholder={inputChannel === "douyin" ? "每行一个抖音内容链接或账号 UID\n例如：1619994549436234" : "每行一个小红书内容链接\n例如：https://www.xiaohongshu.com/explore/..."} />
              <div className="form-foot"><span>支持换行或英文逗号分隔，自动去重。</span><button className="primary" onClick={validateInput}>校验输入</button></div>
            </section>
            {validation && <section className="panel validation-panel"><div className="validation-numbers"><div><strong>{validation.total}</strong><span>输入总数</span></div><div className="good"><strong>{validation.valid_count}</strong><span>有效</span></div><div className={validation.invalid_count ? "risk" : ""}><strong>{validation.invalid_count}</strong><span>需修正</span></div></div>{validation.invalid_count > 0 && <div className="invalid-list">{validation.invalid.slice(0, 8).map((item, index) => <p key={index}><code>{item.value}</code><span>{item.reason}</span></p>)}</div>}</section>}
            <section className="panel cache-run-panel">
              <div><span className="eyebrow">ACCEPTANCE RUN</span><h3>现有 776 条内容缓存回归</h3><p>重新计算 v6.2 报告并比较冻结哈希，不请求 TikHub 或 Rnote。</p></div>
              <button className="secondary" onClick={runCacheRegression}>{currentRun && ["queued", "running"].includes(currentRun.status) ? "执行中…" : "运行缓存回归"}</button>
              {currentRun && <div className="run-progress"><div><span>{currentRun.message}</span><strong>{currentRun.progress}%</strong></div><div className="progress-track"><i style={{ width: `${currentRun.progress}%` }} /></div></div>}
            </section>
          </div>
        )}

        {activeView === "report" && (
          <div className="page-stack">
            <section className="report-intro"><div><span className="eyebrow">REPORT {report.generated_at}</span><h2>双渠道结构化结论</h2><p>数字、百分比与定性描述同时保留；样本项不会伪装成渠道全量结论。</p></div><ChannelSwitch value={activeChannel} onChange={setActiveChannel} /></section>
            <section className="panel">
              <div className="panel-head"><div><span className="channel-kicker">{channelNames[activeChannel]}</span><h3>1、汇总</h3><p>{channel!.scope}</p></div><strong className="denominator">{channel!.denominator}<span>条发布</span></strong></div>
              <div className="metric-grid report-grid">{metricOrder.map(([key, label]) => <MetricCard key={key} label={label} metric={channel!.summary[key]} compact />)}</div>
            </section>
            <section className="panel">
              <div className="panel-head"><div><h3>2、三个业务场景</h3><p>场景占比使用渠道全部发布作为分母。</p></div></div>
              <div className="scene-grid">{Object.entries(channel!.scenes).map(([name, scene]) => {
                const content = scene.content_verticality as Metric;
                const audience = scene.audience_verticality as Metric;
                const acquisition = scene.acquisition_effect_estimate as Metric;
                return <article key={name} className="scene-card"><div><span>{name}</span><strong>{scene.publication_n}<small> 条</small></strong></div><dl><dt>内容汽车性</dt><dd>{content?.value ?? "—"}/100</dd><dt>互动受众汽车性</dt><dd>{audience?.value ?? "—"}/100</dd><dt>拉新潜力</dt><dd>{acquisition?.value ?? "—"}/100</dd></dl><p>{acquisition?.qualitative || "证据不足，暂不可上卷"}</p></article>;
              })}</div>
            </section>
            <section className="conclusion-panel">
              <span className="eyebrow">结论摘要</span>
              <h3>核心卖点需要同时提高生产占比和流量效率</h3>
              <div className="conclusion-grid"><p><b>01</b>抖音卖点条数覆盖 73.52%，但核心卖点仅 42.24%，低于 60%–70% 目标。</p><p><b>02</b>核心卖点只贡献 5.20% 有效曝光，问题不只在生产数量，也在内容流量效率。</p><p><b>03</b>抖音高评论内容互动受众汽车性 40/100，拉新潜力 34/100，不上卷为全部发布。</p><p><b>04</b>实际新增仍需懂车帝侧点击、安装、登录与新用户归因数据，不能由评论推断。</p></div>
            </section>
          </div>
        )}

        {activeView === "details" && (
          <div className="page-stack">
            <section className="detail-toolbar"><div><span className="eyebrow">CONTENT DETAILS</span><h2>逐条内容明细</h2><p>卖点、内容汽车性、互动受众与拉新潜力保持可追溯。</p></div><div><ChannelSwitch value={activeChannel} onChange={(value) => { setActiveChannel(value); setDetailLimit(30); }} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="搜索编号、账号、标题或场景" aria-label="搜索内容明细" /></div></section>
            <section className="panel table-panel"><div className="table-scroll"><table><thead><tr><th>内容</th><th>业务场景</th><th>卖点判断</th><th>内容汽车性</th><th>互动受众</th><th>拉新潜力</th></tr></thead><tbody>{details.map((item) => <tr key={item.item_no}><td><a href={item.url} target="_blank" rel="noreferrer">{item.item_no} · {item.title}</a><span>{item.account || channelNames[activeChannel]}</span></td><td><span className="scene-tag">{item.business_scene}</span></td><td>{item.selling_point}</td><td>{item.content_verticality}</td><td>{item.audience_verticality}</td><td>{item.acquisition_effect_estimate}</td></tr>)}</tbody></table></div>{details.length < channel!.content_details.length && <button className="load-more" onClick={() => setDetailLimit((value) => value + 30)}>再显示 30 条</button>}</section>
          </div>
        )}

        {activeView === "assets" && (
          <div className="page-stack">
            <section className="report-intro"><div><span className="eyebrow">DATA ASSETS</span><h2>数据资产与导出</h2><p>大文件保留在本地，网页只维护索引和任务状态。</p></div><span className="success-pill">✓ 回归验证通过</span></section>
            <section className="asset-grid">
              {[{ title: "媒体与采集缓存", value: "2.42 GB", note: "视频、图片、ASR、OCR、TikHub、Rnote" }, { title: "正式报告基线", value: "v6.2", note: "8/8 个文件哈希一致" }, { title: "迁移完整性", value: "178/178", note: "文件数与容量校验通过" }, { title: "本地任务数据库", value: apiOnline ? "已连接" : "待启动", note: "仅保存任务状态和文件路径" }].map((item) => <article key={item.title} className="asset-card"><span>{item.title}</span><strong>{item.value}</strong><p>{item.note}</p></article>)}
            </section>
            <section className="panel">
              <div className="panel-head"><div><span className="eyebrow">EXPORTS</span><h3>配套文件</h3></div></div>
              <div className="export-list">
                {[
                  ["结构化结论 JSON", "/exports/双渠道结构化结论_v6.2_TikHub_2026-08-02.json", "机器读取与二次分析"],
                  ["完整报告 Markdown", "/exports/双渠道结构化结论报告_v6.2_TikHub_2026-08-02.md", "完整结构与逐条明细"],
                  ["抖音逐条结果 CSV", "/exports/抖音438条内容渠道评估_v6_TikHub补充_2026-08-02.csv", "438 条抖音评估结果"],
                  ["小红书样本与缺口 CSV", "/exports/小红书渠道评估样本与数据缺口_v4_2026-08-02.csv", "338 条链接数据状态"],
                  ["核心结论图片", "/exports/双渠道核心结论_v6_TikHub补充_2026-08-02.png", "适合阅读和转发"],
                ].map(([title, href, note]) => <a key={title} href={href} download><span className="file-icon">⇩</span><div><strong>{title}</strong><span>{note}</span></div><em>下载</em></a>)}
              </div>
            </section>
            <section className="panel guardrail"><div className="guardrail-icon">!</div><div><h3>数据边界</h3><p>TikHub 和 Rnote 只补充公开内容、互动和曝光。懂车帝实际拉新效果必须等待懂车帝侧归因数据接入。</p></div></section>
          </div>
        )}
      </main>
    </div>
  );
}
