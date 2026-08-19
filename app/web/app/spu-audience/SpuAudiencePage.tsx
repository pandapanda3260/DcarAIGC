"use client";

import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowsClockwiseIcon,
  ArrowsLeftRightIcon,
  CalendarBlankIcon,
  CalendarCheckIcon,
  CalendarDotsIcon,
  CarIcon,
  DatabaseIcon,
  EyeIcon,
  MagnifyingGlassIcon,
  MapTrifoldIcon,
  PlusIcon,
  ShieldCheckIcon,
  StackIcon,
  TargetIcon,
  UsersThreeIcon,
  WarningIcon,
  XIcon,
} from "@phosphor-icons/react";
import AppShell from "../components/AppShell";
import { Feedback, Loading } from "../components/Feedback";
import { Pagination } from "../components/Pagination";
import { jsonRequest, readJson } from "../lib/api";
import { VehicleBrandLogo } from "./VehicleBrandLogo";
import {
  filterVehicleSeriesGroups,
  matchVehicleSeriesGroup,
  sortVehicleCatalogRows,
} from "./vehicleCatalogSort";
import type {
  SpuAssetRow,
  SpuAudienceAssets,
  SpuAudienceStats,
} from "../lib/types";

type SpuForm = {
  spuId: string | null;
  brand: string;
  series: string;
  trimLabel: string;
  modelYear: string;
  powertrain: string;
  bodyStyle: string;
  priceLow: string;
  priceHigh: string;
  aliases: string;
  ambiguousAliases: string;
  audiencePrimary: string;
  audienceSecondary: string;
};

const emptySpuForm: SpuForm = {
  spuId: null, brand: "", series: "", trimLabel: "", modelYear: "", powertrain: "",
  bodyStyle: "", priceLow: "", priceHigh: "", aliases: "", ambiguousAliases: "",
  audiencePrimary: "", audienceSecondary: "",
};

const statWindows = [
  { key: "yesterday", label: "昨天" },
  { key: "this_week", label: "本周" },
  { key: "last_week", label: "上周" },
] as const;

const powertrainLabels: Record<string, string> = {
  ev: "纯电", phev: "插混", erev: "增程", hev: "油混", ice: "燃油",
};

function formatVehicleMeta(row: SpuAssetRow) {
  const price = row.price_low != null && row.price_high != null
    ? `${row.price_low}–${row.price_high}万`
    : "";
  return [
    powertrainLabels[row.powertrain] ?? row.powertrain,
    row.body_style,
    row.model_year ? `${row.model_year}款` : "",
    price,
  ].filter(Boolean).join(" · ") || "—";
}

function formatCount(value: number | null | undefined) {
  if (value == null) return "暂不显示";
  return value.toLocaleString("zh-CN");
}

function formatPercentage(value: number | null | undefined) {
  return value == null ? "—" : `${value}%`;
}

function splitWords(value: string) {
  return value.split(/[,，、\s]+/).map((word) => word.trim()).filter(Boolean);
}

function formatShare(value: number | null | undefined) {
  return value == null ? "—" : `${value}%`;
}

// 「刷新数据」弹窗的重算范围（发布时间口径，与统计窗口同源；后端按 V2/V3 预过滤）
const refreshScopes = [
  {
    key: "yesterday", label: "昨天", note: "重新识别昨天发布且资料足够的内容",
    meta: "范围最小，完成最快", icon: CalendarBlankIcon,
  },
  {
    key: "this_week", label: "本周", note: "重新识别本周至今发布且资料足够的内容",
    meta: "适合跟进本周新增内容", icon: CalendarDotsIcon,
  },
  {
    key: "last_week", label: "上周", note: "重新识别上周发布且资料足够的内容",
    meta: "与默认统计窗口一致", badge: "推荐", icon: CalendarCheckIcon,
  },
  {
    key: "full", label: "全部内容", note: "重新识别全部资料足够的内容",
    meta: "覆盖最广，耗时最长", badge: "耗时较长", icon: DatabaseIcon,
  },
] as const;
type RefreshScopeKey = (typeof refreshScopes)[number]["key"];

export default function SpuAudiencePage() {
  const [assets, setAssets] = useState<SpuAudienceAssets | null>(null);
  const [stats, setStats] = useState<SpuAudienceStats | null>(null);
  const [statWindow, setStatWindow] = useState<string>("last_week");
  const [statPlatform, setStatPlatform] = useState<string>("");
  const [catalogPage, setCatalogPage] = useState(1);
  const [catalogPageSize, setCatalogPageSize] = useState(20);
  const [catalogQuery, setCatalogQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [saving, setSaving] = useState(false);
  const [refreshPicker, setRefreshPicker] = useState(false);
  const [selectedRefreshScope, setSelectedRefreshScope] = useState<RefreshScopeKey>("last_week");
  const [expandedSeries, setExpandedSeries] = useState<ReadonlySet<string>>(new Set());
  const [form, setForm] = useState<SpuForm | null>(null);
  const [error, setError] = useState("");
  const refreshTriggerRef = useRef<HTMLButtonElement>(null);
  const refreshDialogRef = useRef<HTMLElement>(null);
  const refreshRequestRef = useRef(false);

  const loadAssets = useCallback(async () => {
    const result = await readJson<SpuAudienceAssets>("/api/v8/spu-audience/assets");
    setAssets(result);
    return result;
  }, []);

  const loadStats = useCallback(async (windowKey: string, platform: string) => {
    const search = new URLSearchParams({ window: windowKey });
    if (platform) search.set("platform", platform);
    setStats(await readJson<SpuAudienceStats>(`/api/v8/spu-audience/stats?${search.toString()}`));
  }, []);

  useEffect(() => {
    // 规则资产与统计各自加载、各自容错：统计接口出错时规则区照常可用
    readJson<SpuAudienceAssets>("/api/v8/spu-audience/assets")
      .then(async (assetsResult) => {
        setAssets(assetsResult);
        // 有上次刷新后新增/重评估的内容 → 自动增量补算，页面打开即保鲜
        if (assetsResult.ready && (assetsResult.stale_content_count ?? 0) > 0 && assetsResult.last_run?.status !== "running") {
          try {
            await readJson("/api/v8/spu-audience/associate?mode=incremental", { method: "POST" });
            setAssets(await readJson<SpuAudienceAssets>("/api/v8/spu-audience/assets"));
          } catch {
            // 只读副本或并发刷新时静默跳过，等下次打开再补
          }
        }
      })
      .catch((reason) => setError(reason instanceof Error ? reason.message : "车型、人群和场景规则加载失败，请刷新页面重试。"))
      .finally(() => setLoading(false));
    readJson<SpuAudienceStats>("/api/v8/spu-audience/stats?window=last_week")
      .then(setStats)
      .catch((reason) => setError(reason instanceof Error ? reason.message : "统计数据加载失败，请刷新页面重试。"));
  }, []);

  const lastRunStatus = assets?.last_run?.status ?? null;
  useEffect(() => {
    if (!refreshPicker) return;
    const previouslyFocused = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const previousBodyOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const focusFrame = window.requestAnimationFrame(() => {
      refreshDialogRef.current?.querySelector<HTMLInputElement>('input[name="spu-refresh-scope"]:checked')?.focus();
    });
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        setRefreshPicker(false);
        return;
      }
      if (event.key !== "Tab") return;
      const dialog = refreshDialogRef.current;
      if (!dialog) return;
      const focusable = Array.from(dialog.querySelectorAll<HTMLElement>(
        'button:not([disabled]), input:not([disabled]), [href], [tabindex]:not([tabindex="-1"])',
      ));
      if (focusable.length === 0) {
        event.preventDefault();
        dialog.focus();
        return;
      }
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault(); last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault(); first.focus();
      }
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      window.cancelAnimationFrame(focusFrame);
      document.removeEventListener("keydown", handleKeyDown);
      document.body.style.overflow = previousBodyOverflow;
      window.requestAnimationFrame(() => previouslyFocused?.focus());
    };
  }, [refreshPicker]);

  useEffect(() => {
    if (lastRunStatus !== "running") return;
    const timer = window.setInterval(() => {
      void (async () => {
        try {
          const next = await readJson<SpuAudienceAssets>("/api/v8/spu-audience/assets");
          setAssets(next);
          const status = next.last_run?.status;
          if (status && status !== "running") {
            window.clearInterval(timer);
            const search = new URLSearchParams({ window: statWindow });
            if (statPlatform) search.set("platform", statPlatform);
            setStats(await readJson<SpuAudienceStats>(`/api/v8/spu-audience/stats?${search.toString()}`));
            if (status === "succeeded") {
              setError("");
            } else {
              setError("内容重新识别没有完成，请稍后重试；如果一直失败，请联系管理员。");
            }
          }
        } catch {
          // 网络抖动时等下一个轮询周期
        }
      })();
    }, 5000);
    return () => window.clearInterval(timer);
  }, [lastRunStatus, statWindow, statPlatform]);

  async function refreshData(scope: RefreshScopeKey) {
    if (refreshRequestRef.current || lastRunStatus === "running") return;
    refreshRequestRef.current = true;
    setRefreshPicker(false);
    setRunning(true); setError("");
    try {
      await readJson<{ run_id: number; status: string }>(
        `/api/v8/spu-audience/associate?mode=${scope}`, { method: "POST" },
      );
      await loadAssets();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "无法开始重新识别，请稍后重试。");
    } finally {
      refreshRequestRef.current = false;
      setRunning(false);
    }
  }

  function openRefreshPicker() {
    const defaultScope: RefreshScopeKey = statWindow === "yesterday" || statWindow === "this_week" || statWindow === "last_week"
      ? statWindow
      : "last_week";
    setSelectedRefreshScope(defaultScope);
    setRefreshPicker(true);
  }

  function editSpu(row?: SpuAssetRow) {
    setForm(row ? {
      spuId: row.spu_id,
      brand: row.brand,
      series: row.series,
      trimLabel: row.trim_label ?? "",
      modelYear: row.model_year == null ? "" : String(row.model_year),
      powertrain: row.powertrain,
      bodyStyle: row.body_style,
      priceLow: row.price_low == null ? "" : String(row.price_low),
      priceHigh: row.price_high == null ? "" : String(row.price_high),
      aliases: row.aliases.filter((item) => !item.ambiguous).map((item) => item.alias).join("，"),
      ambiguousAliases: row.aliases.filter((item) => item.ambiguous).map((item) => item.alias).join("，"),
      audiencePrimary: row.audience_primary ?? "",
      audienceSecondary: row.audience_secondary ?? "",
    } : { ...emptySpuForm });
  }

  async function saveSpu() {
    if (!form) return;
    setSaving(true); setError("");
    try {
      const aliases = [
        ...splitWords(form.aliases).map((alias) => ({ alias, alias_type: "official", ambiguous: false })),
        ...splitWords(form.ambiguousAliases).map((alias) => ({ alias, alias_type: "official", ambiguous: true })),
      ];
      await readJson("/api/v8/spu-audience/spu", jsonRequest({
        spu_id: form.spuId,
        brand: form.brand.trim(),
        series: form.series.trim(),
        trim_label: form.trimLabel.trim() || null,
        model_year: form.modelYear.trim() ? Number(form.modelYear) : null,
        powertrain: form.powertrain,
        body_style: form.bodyStyle.trim(),
        price_low: form.priceLow.trim() ? Number(form.priceLow) : null,
        price_high: form.priceHigh.trim() ? Number(form.priceHigh) : null,
        audience_primary: form.audiencePrimary || null,
        audience_secondary: form.audienceSecondary || null,
        aliases,
      }));
      setForm(null);
      try {
        await readJson("/api/v8/spu-audience/associate", { method: "POST" });
        await loadAssets();
      } catch {
        await loadAssets();
      }
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "车型保存失败，请检查填写内容后重试。");
    } finally {
      setSaving(false);
    }
  }

  const audienceLabelByCode = new Map((assets?.audiences ?? []).map((item) => [item.code, item.label]));
  const sceneLabelByCode = new Map((assets?.scenes ?? []).map((item) => [item.code, item.label]));
  const coreScenesByAudience = new Map((assets?.audience_scene_map ?? []).map((row) => [row.audience_code, row.core]));
  const spuStatsByKey = new Map((stats?.spu_rollup ?? []).map((row) => [row.key, row]));
  const audienceStatsByCode = new Map((stats?.audience_rollup ?? []).map((row) => [row.key, row]));
  const sceneStatsByCode = new Map((stats?.scene_rollup ?? []).map((row) => [row.key, row]));

  function renderAudience(primary: string | null, secondary: string | null) {
    if (!primary) return <span className="spu-audience-empty">未配置</span>;
    return (
      <span className="spu-audience-stack">
        <span className="spu-audience-primary">
          <strong>{primary}</strong>
          <span>{audienceLabelByCode.get(primary) ?? ""}</span>
        </span>
        {secondary && (
          <span className="spu-audience-secondary">
            次要 <strong>{secondary}</strong> {audienceLabelByCode.get(secondary) ?? ""}
          </span>
        )}
      </span>
    );
  }

  // 先按热门品牌与拼音全量排序，再按车系聚合：一车系一行，款型点开才展示；
  // "仅识别到车系"的残量（原车系兜底节点）只在展开时作为明细行出现。
  const catalogRows = useMemo(() => sortVehicleCatalogRows(assets?.spu ?? []), [assets]);
  const seriesGroups = useMemo(() => {
    const groups: Array<{ slug: string; seriesNode: SpuAssetRow; trims: SpuAssetRow[] }> = [];
    const groupBySlug = new Map<string, { slug: string; seriesNode: SpuAssetRow; trims: SpuAssetRow[] }>();
    for (const row of catalogRows) {
      let group = groupBySlug.get(row.series_slug);
      if (!group) {
        group = { slug: row.series_slug, seriesNode: row, trims: [] };
        groupBySlug.set(row.series_slug, group);
        groups.push(group);
      }
      if (row.is_series_node) group.seriesNode = row;
      else group.trims.push(row);
    }
    return groups;
  }, [catalogRows]);
  const seriesTotal = seriesGroups.length;
  const trimTotal = catalogRows.length - seriesTotal;
  const catalogBreakdown = `${seriesTotal} 个车系 · ${trimTotal} 个款型`;
  // 过滤只作用于已排序、已聚合的车系数组，因此结果顺序与每个车系的展开状态都保持不变。
  const filteredSeriesGroups = useMemo(
    () => filterVehicleSeriesGroups(seriesGroups, catalogQuery),
    [seriesGroups, catalogQuery],
  );
  const filteredSeriesTotal = filteredSeriesGroups.length;
  const catalogLastPage = Math.max(1, Math.ceil(filteredSeriesTotal / catalogPageSize));
  // 删行或换每页条数后当前页可能越界，取末页夹紧，避免翻到空白页
  const catalogSafePage = Math.min(catalogPage, catalogLastPage);
  const catalogPageGroups = filteredSeriesGroups.slice((catalogSafePage - 1) * catalogPageSize, catalogSafePage * catalogPageSize);
  const exposureAvailable = stats?.exposure_gate?.status === "available";
  const associationRunning = running || lastRunStatus === "running";
  const channelKeys = ["douyin", "xiaohongshu"] as const;


  function toggleSeries(slug: string) {
    setExpandedSeries((previous) => {
      const next = new Set(previous);
      if (next.has(slug)) next.delete(slug); else next.add(slug);
      return next;
    });
  }

  // 车系行统计 = 仅识别到车系的残量 + 全部款型之和；占比用全平台分母重算
  function seriesAggregate(group: { seriesNode: SpuAssetRow; trims: SpuAssetRow[] }) {
    let posts = 0;
    let views = 0;
    const channelPosts: Record<string, number> = { douyin: 0, xiaohongshu: 0 };
    const channelViews: Record<string, number> = { douyin: 0, xiaohongshu: 0 };
    for (const member of [group.seriesNode, ...group.trims]) {
      const memberStats = spuStatsByKey.get(member.spu_id);
      if (!memberStats) continue;
      posts += memberStats.posts;
      views += memberStats.views ?? 0;
      for (const key of channelKeys) {
        const channel = memberStats.channels?.[key];
        if (!channel) continue;
        channelPosts[key] += channel.posts;
        channelViews[key] += channel.views ?? 0;
      }
    }
    const channels: Record<string, { posts: number; views: number | null; post_share: number | null; view_share: number | null; post_denominator: number; view_denominator: number }> = {};
    for (const key of channelKeys) {
      const meta = stats?.channel_totals?.[key];
      const postDenominator = meta?.posts ?? 0;
      const viewDenominator = meta?.valid_views ?? 0;
      const viewsPublished = Boolean(meta?.views_published);
      channels[key] = {
        posts: channelPosts[key],
        views: viewsPublished ? channelViews[key] : null,
        post_share: postDenominator > 0 ? Math.round(channelPosts[key] * 1000 / postDenominator) / 10 : null,
        view_share: viewsPublished && viewDenominator > 0 ? Math.round(channelViews[key] * 1000 / viewDenominator) / 10 : null,
        post_denominator: postDenominator,
        view_denominator: viewDenominator,
      };
    }
    return { posts, views: exposureAvailable ? views : null, channels };
  }

  const shellActions = (
    <>
      <button ref={refreshTriggerRef} className="primary" disabled={loading || associationRunning || Boolean(assets && !assets.ready)} onClick={openRefreshPicker}>
        {associationRunning ? "正在重新识别…" : "重新识别内容"}
      </button>
      <button className="secondary spu-shell-action" disabled={loading || saving || Boolean(assets && !assets.ready)} onClick={() => editSpu()}>
        <PlusIcon size={15} weight="bold" aria-hidden />
        新增车型
      </button>
    </>
  );

  return (
    <AppShell active="spu-audience" actions={shellActions}>
      <Feedback error={error} onClose={() => setError("")} />
      {loading ? <Loading label="正在加载车型、人群和场景数据" /> : assets && !assets.ready ? (
        <section className="page-stack spu-audience-page"><article className="panel"><p>当前数据版本较旧，暂时不能使用 SPU 人群功能。请联系管理员升级后重试。</p></article></section>
      ) : (
        <section className="page-stack spu-audience-page">
          <section className="spu-summary" aria-labelledby="spu-summary-title">
            <header className="spu-summary-head">
              <h2 id="spu-summary-title">识别完成情况</h2>
              <span className="selling-point-window-control spu-window-control">
                <label htmlFor="spu-stat-window">统计窗口</label>
                <select
                  id="spu-stat-window"
                  className="selling-point-window-select"
                  value={statWindow}
                  onChange={(event) => { setStatWindow(event.target.value); void loadStats(event.target.value, statPlatform).catch((reason) => setError(reason instanceof Error ? reason.message : "统计数据加载失败，请刷新页面重试。")); }}
                >
                  {statWindows.map((item) => <option key={item.key} value={item.key}>{item.label}</option>)}
                </select>
                <select
                  className="selling-point-window-select"
                  aria-label="统计平台"
                  value={statPlatform}
                  onChange={(event) => { setStatPlatform(event.target.value); void loadStats(statWindow, event.target.value).catch((reason) => setError(reason instanceof Error ? reason.message : "统计数据加载失败，请刷新页面重试。")); }}
                >
                  <option value="">全部平台</option>
                  <option value="douyin">抖音</option>
                  <option value="xiaohongshu">小红书</option>
                </select>
              </span>
            </header>
            <div className="spu-summary-grid">
              {[
                { key: "spu", tone: "catalog", icon: <CarIcon size={22} weight="regular" aria-hidden />, label: "车型识别", value: stats?.coverage?.spu_percentage, note: catalogBreakdown },
                { key: "audience", tone: "audience", icon: <UsersThreeIcon size={22} weight="regular" aria-hidden />, label: "人群识别", value: stats?.coverage?.audience_percentage, note: `${(assets?.audiences ?? []).length} 类人群` },
                { key: "scene", tone: "scene", icon: <MapTrifoldIcon size={22} weight="regular" aria-hidden />, label: "场景识别", value: stats?.coverage?.scene_percentage, note: `${(assets?.scenes ?? []).length} 类场景` },
                { key: "trim", tone: "map", icon: <StackIcon size={22} weight="regular" aria-hidden />, label: "具体款型识别", value: stats?.coverage?.trim_percentage, note: "已识别到具体款型" },
                { key: "exposure", tone: "exposure", icon: <EyeIcon size={22} weight="regular" aria-hidden />, label: "已完成曝光分类", value: stats?.exposure_gate?.classified_share, note: exposureAvailable ? "数据足够，可以显示曝光占比" : "完成分类的曝光不足 90%，暂不显示曝光占比" },
              ].map((item) => (
                <article className="spu-summary-item" data-tone={item.tone} key={item.key}>
                  <span className="spu-block-icon">{item.icon}</span>
                  <div className="spu-summary-copy">
                    <span>{item.label}</span>
                    <strong>{formatPercentage(item.value)}</strong>
                    <small>{item.note}</small>
                  </div>
                </article>
              ))}
            </div>
          </section>

          <section className="spu-block" data-tone="catalog">
            <header className="spu-block-head">
              <div className="spu-block-title">
                <span className="spu-block-icon"><CarIcon size={20} weight="regular" aria-hidden /></span>
                <h2>车系与款型库</h2>
              </div>
              <div className="spu-block-side">
                <p className="spu-block-meta">
                  {catalogBreakdown}
                  <span aria-hidden>·</span>热门品牌优先
                  {stats?.totals && <><span aria-hidden>·</span>窗口内 {stats.totals.posts.toLocaleString("zh-CN")} 条发布</>}
                </p>
              </div>
            </header>
            <div className="spu-catalog-controls">
              <div className="spu-catalog-search-wrap">
                <label className="spu-catalog-search">
                  <MagnifyingGlassIcon size={14} weight="regular" aria-hidden />
                  <input
                    type="search"
                    value={catalogQuery}
                    aria-label="搜索品牌、车系、款型、别名或拼音"
                    aria-describedby="spu-catalog-search-status"
                    placeholder="搜索品牌、车系、款型、别名或拼音"
                    autoComplete="off"
                    spellCheck={false}
                    onChange={(event) => { setCatalogQuery(event.target.value); setCatalogPage(1); }}
                  />
                </label>
                <span id="spu-catalog-search-status" className="spu-catalog-search-status" role="status" aria-live="polite">
                  {catalogQuery.trim() ? `${filteredSeriesTotal} 个结果` : ""}
                </span>
              </div>
              {filteredSeriesTotal > 0 && <Pagination page={catalogSafePage} pageSize={catalogPageSize} total={filteredSeriesTotal} busy={saving} ariaLabel="车型库分页" unitLabel="个车系" placement="top" onChange={(next) => { setCatalogPage(next.page); if (next.pageSize) setCatalogPageSize(next.pageSize); }} />}
            </div>
            <div className="spu-table-wrap" role="region" aria-label="车系与款型库表格" tabIndex={0}>
              <table className="spu-catalog-table">
                <thead><tr><th>品牌</th><th>车系</th><th>款型</th><th>目标人群</th><th>主要用车场景</th><th>识别结果</th><th>抖音条数占比</th><th>抖音曝光占比</th><th>小红书条数占比</th><th>小红书曝光占比</th><th>操作</th></tr></thead>
                <tbody>
                  {catalogPageGroups.length === 0 && (
                    <tr className="spu-catalog-empty">
                      <td colSpan={11}>{catalogQuery.trim() ? `未找到匹配“${catalogQuery.trim()}”的车型` : "暂无车系数据"}</td>
                    </tr>
                  )}
                  {catalogPageGroups.map((group) => {
                    const seriesNode = group.seriesNode;
                    const catalogMatch = catalogQuery.trim() ? matchVehicleSeriesGroup(group, catalogQuery) : null;
                    const expanded = expandedSeries.has(group.slug);
                    const aggregate = seriesAggregate(group);
                    const seriesCoreScenes = seriesNode.audience_primary ? coreScenesByAudience.get(seriesNode.audience_primary) ?? [] : [];
                    const residualStats = spuStatsByKey.get(seriesNode.spu_id);
                    return (
                      <Fragment key={group.slug}>
                        <tr className="spu-series-row">
                          <td>
                            <div className="spu-brand-cell">
                              <VehicleBrandLogo brand={seriesNode.brand} />
                              <strong className="spu-brand-name">{seriesNode.brand}</strong>
                            </div>
                          </td>
                          <td><strong className="spu-series-name">{seriesNode.series}</strong></td>
                          <td>
                            {group.trims.length > 0 ? (
                              <button
                                type="button"
                                className="spu-series-toggle"
                                aria-expanded={expanded}
                                aria-label={`${expanded ? "收起" : "展开"}${seriesNode.series}的 ${group.trims.length} 个款型`}
                                onClick={() => toggleSeries(group.slug)}
                              >
                                <span className="spu-series-caret" aria-hidden>{expanded ? "▾" : "▸"}</span>
                                {group.trims.length} 个款型
                              </button>
                            ) : <span className="spu-series-node">暂无款型</span>}
                            <span className="cell-subline">
                              {catalogMatch?.kind === "trim" && catalogMatch.matchedTrimLabel
                                ? `匹配款型：${catalogMatch.matchedTrimLabel}${(catalogMatch.matchedTrimCount ?? 0) > 1 ? ` 等 ${catalogMatch.matchedTrimCount} 个` : ""}`
                                : formatVehicleMeta(seriesNode)}
                            </span>
                          </td>
                          <td className="spu-audience-cell">{renderAudience(seriesNode.audience_primary, seriesNode.audience_secondary)}</td>
                          <td className="spu-alias-cell">{seriesCoreScenes.length === 0 ? "—" : seriesCoreScenes.map((code) => <span className="scene-tag" key={code}>{sceneLabelByCode.get(code) ?? code}</span>)}</td>
                          <td>
                            {aggregate.posts > 0 ? (
                              <span className="selling-point-hit-value"><strong>{aggregate.posts.toLocaleString("zh-CN")}</strong><small>曝光 {formatCount(aggregate.views)}</small></span>
                            ) : <span className="selling-point-hit-empty">—</span>}
                          </td>
                          {channelKeys.map((channelKey) => {
                            const channel = aggregate.channels[channelKey];
                            return (
                              <Fragment key={channelKey}>
                                <td><span className={channel.post_share != null && channel.posts > 0 ? "selling-point-share-value" : "selling-point-hit-empty"} title={channel.post_denominator > 0 ? `${channel.posts.toLocaleString("zh-CN")} / ${channel.post_denominator.toLocaleString("zh-CN")} 条发布` : "窗口内无发布"}>{channel.posts > 0 ? formatShare(channel.post_share) : "—"}</span></td>
                                <td><span className={channel.view_share != null && (channel.views ?? 0) > 0 ? "selling-point-share-value" : "selling-point-hit-empty"} title={channel.view_share != null ? `${(channel.views ?? 0).toLocaleString("zh-CN")} / ${channel.view_denominator.toLocaleString("zh-CN")} 次曝光` : "曝光数据不足，暂不显示占比"}>{channel.view_share != null && (channel.views ?? 0) > 0 ? formatShare(channel.view_share) : "—"}</span></td>
                              </Fragment>
                            );
                          })}
                          <td><button className="text-button" disabled={saving} onClick={() => editSpu(seriesNode)}>编辑</button></td>
                        </tr>
                        {expanded && group.trims.map((row) => {
                          const coreScenes = row.audience_primary ? coreScenesByAudience.get(row.audience_primary) ?? [] : [];
                          const rowStats = spuStatsByKey.get(row.spu_id);
                          return (
                            <tr className="spu-trim-row" key={row.spu_id}>
                              <td />
                              <td className="spu-trim-lead" />
                              <td>
                                {row.trim_label}
                                <span className="cell-subline">{formatVehicleMeta(row)}</span>
                              </td>
                              <td className="spu-audience-cell">{renderAudience(row.audience_primary, row.audience_secondary)}</td>
                              <td className="spu-alias-cell">{coreScenes.length === 0 ? "—" : coreScenes.map((code) => <span className="scene-tag" key={code}>{sceneLabelByCode.get(code) ?? code}</span>)}</td>
                              <td>
                                {rowStats && rowStats.posts > 0 ? (
                                  <span className="selling-point-hit-value"><strong>{rowStats.posts.toLocaleString("zh-CN")}</strong><small>曝光 {formatCount(rowStats.views)}</small></span>
                                ) : <span className="selling-point-hit-empty">—</span>}
                              </td>
                              {channelKeys.map((channelKey) => {
                                const channel = rowStats?.channels?.[channelKey];
                                return (
                                  <Fragment key={channelKey}>
                                    <td><span className={channel?.post_share != null && channel.posts > 0 ? "selling-point-share-value" : "selling-point-hit-empty"} title={channel && channel.post_denominator > 0 ? `${channel.posts.toLocaleString("zh-CN")} / ${channel.post_denominator.toLocaleString("zh-CN")} 条发布` : "窗口内无发布"}>{channel && channel.posts > 0 ? formatShare(channel.post_share) : "—"}</span></td>
                                    <td><span className={channel?.view_share != null && (channel?.views ?? 0) > 0 ? "selling-point-share-value" : "selling-point-hit-empty"} title={channel?.view_share != null ? `${(channel.views ?? 0).toLocaleString("zh-CN")} / ${channel.view_denominator.toLocaleString("zh-CN")} 次曝光` : "曝光数据不足，暂不显示占比"}>{channel?.view_share != null && (channel?.views ?? 0) > 0 ? formatShare(channel.view_share) : "—"}</span></td>
                                  </Fragment>
                                );
                              })}
                              <td><button className="text-button" disabled={saving} onClick={() => editSpu(row)}>编辑</button></td>
                            </tr>
                          );
                        })}
                        {expanded && (
                          <tr className="spu-trim-row spu-residual-row" key={`${group.slug}-residual`}>
                            <td />
                            <td className="spu-trim-lead" />
                            <td>
                              <span className="spu-series-node" title="内容只识别到车系、未细化到具体款型时，计入这一行">仅识别到车系</span>
                            </td>
                            <td>—</td>
                            <td className="spu-alias-cell">—</td>
                            <td>
                              {residualStats && residualStats.posts > 0 ? (
                                <span className="selling-point-hit-value"><strong>{residualStats.posts.toLocaleString("zh-CN")}</strong><small>曝光 {formatCount(residualStats.views)}</small></span>
                              ) : <span className="selling-point-hit-empty">—</span>}
                            </td>
                            {channelKeys.map((channelKey) => {
                              const channel = residualStats?.channels?.[channelKey];
                              return (
                                <Fragment key={channelKey}>
                                  <td><span className={channel?.post_share != null && channel.posts > 0 ? "selling-point-share-value" : "selling-point-hit-empty"} title={channel && channel.post_denominator > 0 ? `${channel.posts.toLocaleString("zh-CN")} / ${channel.post_denominator.toLocaleString("zh-CN")} 条发布` : "窗口内无发布"}>{channel && channel.posts > 0 ? formatShare(channel.post_share) : "—"}</span></td>
                                  <td><span className={channel?.view_share != null && (channel?.views ?? 0) > 0 ? "selling-point-share-value" : "selling-point-hit-empty"} title={channel?.view_share != null ? `${(channel.views ?? 0).toLocaleString("zh-CN")} / ${channel.view_denominator.toLocaleString("zh-CN")} 次曝光` : "曝光数据不足，暂不显示占比"}>{channel?.view_share != null && (channel?.views ?? 0) > 0 ? formatShare(channel.view_share) : "—"}</span></td>
                                </Fragment>
                              );
                            })}
                            <td>—</td>
                          </tr>
                        )}
                      </Fragment>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {(stats?.footnotes ?? []).length > 0 && (
              <ul className="spu-footnotes">
                {(stats?.footnotes ?? []).map((note) => <li key={note}>{note}</li>)}
              </ul>
            )}
          </section>

          <div className="spu-dim-grid">
            <section className="spu-block" data-tone="audience">
              <header className="spu-block-head">
                <div className="spu-block-title">
                  <span className="spu-block-icon"><UsersThreeIcon size={20} weight="regular" aria-hidden /></span>
                  <h2>目标人群</h2>
                </div>
                <p className="spu-block-meta">共 {(assets?.audiences ?? []).length} 类</p>
              </header>
              <div className="spu-table-wrap">
                <table className="spu-dim-table">
                  <thead><tr><th>人群</th><th>定义</th><th>内容中出现的特征</th><th>发布条数</th><th>曝光量</th></tr></thead>
                  <tbody>
                    {(assets?.audiences ?? []).map((item) => {
                      const rowStats = audienceStatsByCode.get(item.code);
                      return (
                        <tr key={item.code}><td><strong>{item.code}</strong> {item.label}</td><td>{item.definition}</td><td className="spu-alias-cell">{item.signals.map((word) => <span className="scene-tag" key={word}>{word}</span>)}</td><td>{(rowStats?.posts ?? 0).toLocaleString("zh-CN")}</td><td>{formatCount(rowStats ? rowStats.views : exposureAvailable ? 0 : null)}</td></tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </section>
            <section className="spu-block" data-tone="scene">
              <header className="spu-block-head">
                <div className="spu-block-title">
                  <span className="spu-block-icon"><MapTrifoldIcon size={20} weight="regular" aria-hidden /></span>
                  <h2>用车场景</h2>
                </div>
                <p className="spu-block-meta">共 {(assets?.scenes ?? []).length} 类</p>
              </header>
              <div className="spu-table-wrap">
                <table className="spu-dim-table">
                  <thead><tr><th>场景</th><th>触发词</th><th>负向词</th><th>发布条数</th><th>曝光量</th></tr></thead>
                  <tbody>
                    {(assets?.scenes ?? []).map((item) => {
                      const rowStats = sceneStatsByCode.get(item.code);
                      return (
                        <tr key={item.code}><td><strong>{item.code}</strong> {item.label}</td><td className="spu-alias-cell">{item.triggers.map((word) => <span className="scene-tag" key={word}>{word}</span>)}</td><td className="spu-alias-cell">{item.negatives.length === 0 ? "—" : item.negatives.map((word) => <span className="scene-tag spu-alias-ambiguous" key={word}>{word}</span>)}</td><td>{(rowStats?.posts ?? 0).toLocaleString("zh-CN")}</td><td>{formatCount(rowStats ? rowStats.views : exposureAvailable ? 0 : null)}</td></tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </section>
          </div>

          <section className="spu-block" data-tone="map">
            <header className="spu-block-head">
              <div className="spu-block-title">
                <span className="spu-block-icon"><ArrowsLeftRightIcon size={20} weight="regular" aria-hidden /></span>
                  <h2>人群对应的用车场景</h2>
              </div>
                <p className="spu-block-meta">系统按目标人群推算车型的常见场景</p>
            </header>
            <div className="spu-table-wrap">
              <table className="spu-dim-table spu-map-table">
                <thead><tr><th>人群</th><th>主要场景</th><th>相关场景</th></tr></thead>
                <tbody>
                  {(assets?.audience_scene_map ?? []).map((row) => (
                    <tr key={row.audience_code}>
                      <td><strong>{row.audience_code}</strong> {audienceLabelByCode.get(row.audience_code) ?? ""}</td>
                      <td className="spu-alias-cell">{row.core.map((code) => <span className="scene-tag" key={code}>{sceneLabelByCode.get(code) ?? code}</span>)}</td>
                      <td className="spu-alias-cell">{row.related.map((code) => <span className="scene-tag" key={code}>{sceneLabelByCode.get(code) ?? code}</span>)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <div className="spu-gap-grid">
            <section className="spu-block" data-tone="gap">
              <header className="spu-block-head">
                <div className="spu-block-title">
                  <span className="spu-block-icon"><TargetIcon size={20} weight="regular" aria-hidden /></span>
                  <h2>已配置但没有内容</h2>
                </div>
                <p className="spu-block-meta">规则中已配置，但所选时间内没有相关内容<span aria-hidden>·</span>{(stats?.gaps?.missing ?? []).length} 项</p>
              </header>
              <div className="spu-block-body">
                {(stats?.gaps?.missing ?? []).length === 0 ? <p className="spu-empty-note">当前没有缺少内容的已配置场景。</p> : (
                  <ul className="spu-gap-list">
                    {(stats?.gaps?.missing ?? []).map((gap) => (
                      <li key={`${gap.series}|${gap.scene.code}`}>
                        <strong>{gap.series}</strong>（{gap.audience.code} {gap.audience.label} · {gap.series_posts} 条）缺少 <span className="scene-tag">{gap.scene.label}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </section>
            <section className="spu-block" data-tone="overflow">
              <header className="spu-block-head">
                <div className="spu-block-title">
                  <span className="spu-block-icon"><WarningIcon size={20} weight="regular" aria-hidden /></span>
                  <h2>内容已有但规则未配置</h2>
                </div>
                <p className="spu-block-meta">内容中已识别到，但规则里还没有配置<span aria-hidden>·</span>{(stats?.gaps?.overflow ?? []).length} 项</p>
              </header>
              <div className="spu-block-body">
                {(stats?.gaps?.overflow ?? []).length === 0 ? <p className="spu-empty-note">当前没有需要补充的规则。</p> : (
                  <ul className="spu-gap-list">
                    {(stats?.gaps?.overflow ?? []).map((gap) => (
                      <li key={`${gap.audience.code}|${gap.scene.code}`}>
                        {gap.audience.code} {gap.audience.label} × <span className="scene-tag">{gap.scene.label}</span>：{gap.posts.toLocaleString("zh-CN")} 条
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </section>
          </div>
        </section>
      )}

      {refreshPicker && (
        <div
          className="modal-backdrop spu-refresh-backdrop"
          role="presentation"
          onMouseDown={(event) => { if (event.target === event.currentTarget) setRefreshPicker(false); }}
        >
          <section
            ref={refreshDialogRef}
            className="modal-panel spu-refresh-modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="spu-refresh-title"
            aria-describedby="spu-refresh-description"
            tabIndex={-1}
          >
            <header className="spu-refresh-header">
              <span className="spu-refresh-header-icon" aria-hidden>
                <ArrowsClockwiseIcon size={22} weight="bold" />
              </span>
              <div className="spu-refresh-heading">
                <span className="eyebrow">重新识别内容</span>
                <h2 id="spu-refresh-title">选择要重新识别的内容范围</h2>
              </div>
              <button type="button" className="spu-refresh-close" onClick={() => setRefreshPicker(false)} aria-label="关闭内容范围选择窗口">
                <XIcon size={18} weight="bold" aria-hidden />
              </button>
            </header>

            <div className="spu-refresh-body">
              <p id="spu-refresh-description" className="spu-refresh-note">这里的日期范围与页面上选择的统计时间一致。开始后系统会在后台重新识别内容。</p>
              <div className="spu-refresh-guardrail">
                <ShieldCheckIcon size={18} weight="fill" aria-hidden />
                <span>只处理资料较完整、可以自动评估的内容；其他时间范围的数据不会改变。</span>
              </div>
              <fieldset className="spu-refresh-options">
                <legend className="visually-hidden">选择要重新识别的内容范围</legend>
                {refreshScopes.map((item) => {
                  const ScopeIcon = item.icon;
                  const selected = selectedRefreshScope === item.key;
                  return (
                    <label key={item.key} className="spu-refresh-option" data-selected={selected} data-scope={item.key}>
                      <input
                        className="spu-refresh-radio-input"
                        type="radio"
                        name="spu-refresh-scope"
                        value={item.key}
                        checked={selected}
                        onChange={() => setSelectedRefreshScope(item.key)}
                      />
                      <span className="spu-refresh-option-icon" aria-hidden><ScopeIcon size={19} weight="bold" /></span>
                      <span className="spu-refresh-option-content">
                        <span className="spu-refresh-option-title">
                          <strong>{item.label}</strong>
                          {"badge" in item && <small>{item.badge}</small>}
                        </span>
                        <span className="spu-refresh-option-note">{item.note}</span>
                        <span className="spu-refresh-option-meta">{item.meta}</span>
                      </span>
                      <span className="spu-refresh-radio" aria-hidden />
                    </label>
                  );
                })}
              </fieldset>
            </div>

            <footer className="spu-refresh-actions">
              <button type="button" className="secondary" onClick={() => setRefreshPicker(false)}>取消</button>
              <button type="button" className="primary" disabled={associationRunning} onClick={() => void refreshData(selectedRefreshScope)}>
                <ArrowsClockwiseIcon size={16} weight="bold" aria-hidden />
                开始重新识别
              </button>
            </footer>
          </section>
        </div>
      )}

      {form && (
        <div className="modal-backdrop" role="presentation">
          <section className="modal-panel operation-modal" role="dialog" aria-modal="true" aria-label="编辑车型">
            <div className="panel-head"><div><span className="eyebrow">车型资料</span><h3>{form.spuId ? "编辑车型" : "新增车型"}</h3></div><button className="modal-close" onClick={() => setForm(null)} aria-label="关闭">×</button></div>
            <div className="modal-fields">
              <label>品牌<input value={form.brand} disabled={Boolean(form.spuId)} onChange={(event) => setForm({ ...form, brand: event.target.value })} /></label>
              <label>车系<input value={form.series} disabled={Boolean(form.spuId)} onChange={(event) => setForm({ ...form, series: event.target.value })} /></label>
              <label className="span-two">款型名称（如果不填，系统会把只识别到车系的内容统计到这条记录中）<input value={form.trimLabel} placeholder="如：2026款 DM-i 55KM 领先型" onChange={(event) => setForm({ ...form, trimLabel: event.target.value })} /></label>
              <label>年款<input value={form.modelYear} placeholder="2026" onChange={(event) => setForm({ ...form, modelYear: event.target.value })} /></label>
              <label>能源形式<select value={form.powertrain} onChange={(event) => setForm({ ...form, powertrain: event.target.value })}><option value="">未填</option><option value="ev">纯电</option><option value="phev">插混</option><option value="erev">增程</option><option value="hev">油混</option><option value="ice">燃油</option></select></label>
              <label>车身形式<input value={form.bodyStyle} placeholder="SUV / 轿车 / MPV" onChange={(event) => setForm({ ...form, bodyStyle: event.target.value })} /></label>
              <label>指导价带（万）<span className="spu-price-pair"><input value={form.priceLow} placeholder="下限" onChange={(event) => setForm({ ...form, priceLow: event.target.value })} /><input value={form.priceHigh} placeholder="上限" onChange={(event) => setForm({ ...form, priceHigh: event.target.value })} /></span></label>
              <label className="span-two">识别别名（顿号或逗号分隔）<input value={form.aliases} placeholder="秦PLUS，秦plus" onChange={(event) => setForm({ ...form, aliases: event.target.value })} /></label>
              <label className="span-two">容易混淆的别名（只有上下文明确时才识别）<input value={form.ambiguousAliases} placeholder="秦" onChange={(event) => setForm({ ...form, ambiguousAliases: event.target.value })} /></label>
              <label>主要目标人群<select value={form.audiencePrimary} onChange={(event) => setForm({ ...form, audiencePrimary: event.target.value })}><option value="">未配置</option>{(assets?.audiences ?? []).map((item) => <option key={item.code} value={item.code}>{item.code} {item.label}</option>)}</select></label>
              <label>次要目标人群<select value={form.audienceSecondary} onChange={(event) => setForm({ ...form, audienceSecondary: event.target.value })}><option value="">未配置</option>{(assets?.audiences ?? []).map((item) => <option key={item.code} value={item.code}>{item.code} {item.label}</option>)}</select></label>
            </div>
            <div className="modal-actions">
              <button className="secondary" onClick={() => setForm(null)}>取消</button>
              <button className="primary" disabled={saving || !form.brand.trim() || !form.series.trim()} onClick={() => void saveSpu()}>{saving ? "保存中" : "保存车型"}</button>
            </div>
          </section>
        </div>
      )}
    </AppShell>
  );
}
