import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("content media modal is read-only, requests evidence once and embeds the official Douyin player safely", async () => {
  const [page, modal, hook, styles] = await Promise.all([
    readFile(new URL("../app/contents/ContentsPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/contents/ContentMediaModal.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/useDialogFocus.ts", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
  ]);

  // 点击媒体框后按三级判定挂载预览；更新记录显示时暂时卸载预览，避免两个模态框争抢焦点。
  assert.match(page, /const \[mediaItem, setMediaItem\] = useState<ContentItem \| null>\(null\);/);
  assert.match(page, /const recordsVisible = recordsOpen && contentUpdates\.available && !formOpen;/);
  assert.match(page, /\{mediaItem && !recordsVisible && <ContentMediaModal key=\{mediaItem\.id\} item=\{mediaItem\} action=\{resolveMediaAction\(mediaItem\)\} onClose=\{\(\) => setMediaItem\(null\)\} \/>\}/);

  // 只读：没有恢复、重处理、付费或任何写请求；Evidence 只读一次，不轮询。
  assert.doesNotMatch(modal, /requestMediaRestore|media\/retry|media\/restore|jsonRequest|markedJsonRequest|showToast|method:/);
  assert.equal((modal.match(/readQueryJson<EvidenceBundle>\(`\/api\/v8\/contents\/\$\{item\.id\}\/evidence`\)/g) ?? []).length, 1);
  assert.doesNotMatch(modal, /setInterval|setTimeout/);
  assert.match(modal, /const needsEvidence = action\.kind === "local";/);

  // 抖音官方播放器：地址只来自 contentMedia 的纯函数；属性与官方 iframe 代码一致并委托自动播放。
  assert.doesNotMatch(modal, /open\.douyin\.com/);
  assert.match(modal, /<iframe className="content-media-player" src=\{stage\.url\} title="抖音官方播放器" aria-describedby="content-media-player-help" allow="autoplay; fullscreen" referrerPolicy="unsafe-url" allowFullScreen \/>/);
  assert.doesNotMatch(modal, /sandbox=/);

  // 本地媒体：单舞台轮播 + 切换/关闭时停声停读 + 成员文件失败与不可用状态都有行内提示和原帖出口。
  assert.match(modal, /<video ref=\{videoRef\} className="content-media-video" src=\{src\} controls autoPlay playsInline preload="metadata"/);
  assert.match(modal, /video\.pause\(\);\s*video\.removeAttribute\("src"\);\s*video\.load\(\);/);
  assert.match(modal, /event\.key === "ArrowLeft"[\s\S]*event\.key === "ArrowRight"/);
  assert.match(modal, /<strong>文件无法读取<\/strong>/);
  assert.match(modal, /<strong>正在读取已保存的资料<\/strong>/);
  assert.match(modal, /<strong>内容资料读取失败<\/strong>/);
  assert.match(modal, /href=\{item\.canonical_url\} target="_blank" rel="noreferrer">打开原帖<\/a>/);
  assert.match(modal, /原件已归档或不可用，显示保留预览/);

  // 对话框语义与键盘约定走共享 hook（W3C APG dialog 模式）。
  assert.match(modal, /useDialogFocus\(true, panelRef, onClose\);/);
  assert.match(modal, /role="dialog" aria-modal="true" aria-labelledby="content-media-title" tabIndex=\{-1\}/);
  assert.match(modal, /<h3 id="content-media-title" title=\{title\}>/);
  // 操作区（打开原帖、关闭）在 DOM 里排在舞台之后、靠 grid 钉回右上角：Tab 从跨源 iframe 出来先落到父文档控件；只有一个关闭。
  assert.ok(modal.indexOf('className="content-media-stage"') < modal.indexOf('className="content-media-actions"'));
  assert.equal((modal.match(/aria-label="关闭"/g) ?? []).length, 1);
  assert.doesNotMatch(modal, /content-media-close|关闭预览|键盘：Tab|非公开或已删除/);
  assert.match(modal, /<p id="content-media-player-help" className="content-media-player-help">/);
  assert.match(modal, /若播放器空白或无法播放/);
  assert.match(modal, /打开原帖观看/);
  assert.match(modal, /onLoadedMetadata=\{\(event\) => onMeasure\(mediaRatio\(event\.currentTarget\.videoWidth, event\.currentTarget\.videoHeight\)\)\}/);
  assert.match(modal, /onLoad=\{\(event\) => measure\(current, mediaRatio\(event\.currentTarget\.naturalWidth, event\.currentTarget\.naturalHeight\)\)\}/);
  assert.match(modal, /"--media-ratio": ratio\.toFixed\(4\)/);
  assert.match(hook, /event\.key === "Escape"/);
  assert.match(hook, /event\.key !== "Tab"/);
  assert.match(hook, /document\.body\.style\.overflow = "hidden"/);
  assert.match(hook, /previouslyFocused\?\.focus\(\)/);
  assert.match(hook, /initialSelector = "\.modal-close"/);

  // 尺寸跟内容走：舞台高度由视口决定，面板宽度 = 内容宽高比 × 舞台高 + 内边距；抖音播放器固定竖版且不小于其 324px 最小布局。
  assert.match(styles, /\.modal-panel\.content-media-modal\s*\{[^}]*--media-stage-h:\s*clamp\(360px, calc\(100dvh - 164px\), 768px\);[^}]*--media-ratio:\s*0\.5625;[^}]*width:\s*clamp\(360px, calc\(var\(--media-w\) \+ 36px\), min\(960px, 100vw - 32px\)\);[^}]*grid-template-areas:\s*"head actions" "stage stage" "bar bar";/);
  assert.match(styles, /\.modal-panel\.content-media-modal\[data-stage="player"\]\s*\{[^}]*--media-w:\s*clamp\(324px, calc\(\(var\(--media-stage-h\) - 48px\) \* 9 \/ 16\), 480px\);/);
  assert.match(styles, /\.content-media-head h3\s*\{[^}]*-webkit-line-clamp:\s*2;/);
  assert.match(styles, /\.button-link\s*\{[^}]*white-space:\s*nowrap;/);
  assert.match(styles, /\.content-media-stage\s*\{[^}]*place-items:\s*center;[^}]*background:\s*#0f2026;/);
  assert.match(styles, /\.content-media-stage \.content-media-player\s*\{[^}]*width:\s*var\(--media-w\);[^}]*height:\s*100%;/);
  assert.match(styles, /\.content-media-stage \.empty-state, \.content-media-stage \.empty-state strong, \.content-media-stage \.empty-state span\s*\{[^}]*color:\s*#c7d3d7;/);
});
