import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

// 提示改为浮动 toast：不占版面（fixed 浮层）、相同文案去重、到时自动消失、每条可手动关闭。
test("notices render as floating toasts instead of in-flow banners", async () => {
  const [component, shell, styles] = await Promise.all([
    readFile(new URL("../app/components/Feedback.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/components/AppShell.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
  ]);

  // 浮层不占版面：viewport fixed 定位，容器不拦截指针，页面内容不被推挤
  assert.match(styles, /\.toast-viewport\s*\{[^}]*position:\s*fixed;[^}]*pointer-events:\s*none;/s);
  assert.match(styles, /\.toast\s*\{[^}]*pointer-events:\s*auto;/s);
  // 旧的占版面提示栏样式必须清除，防止回归
  assert.doesNotMatch(styles, /\.feedback-notice|\.error-notice|\.success-notice|\.inline-notice/);
  assert.doesNotMatch(component, /notice feedback-notice/);

  // 相同文案去重 + 自动消失 + 手动关闭 + 悬停暂停
  assert.match(component, /dedupeKey/);
  assert.match(component, /const TOAST_DURATION: Record<ToastTone, number> = \{ success: \d+, error: \d+ \}/);
  assert.match(component, /aria-label="关闭提示"/);
  assert.match(component, /onMouseEnter=\{pauseToastTimers\} onMouseLeave=\{resumeToastTimers\}/);

  // Notice / Feedback 保持原签名（各页面调用处不动），内部派发 toast；AppShell 挂载唯一的 ToastViewport
  assert.match(component, /export function Notice\(\{ tone = "success", children \}/);
  assert.match(component, /export function Feedback\(\{ error, message, onClose \}/);
  assert.match(shell, /<ToastViewport \/>/);
});

// 零散的自绘提示（新建报告弹窗的 modal-error、侧栏退出失败的 sidebar-logout-error）
// 也统一走 toast，不再各画各的横幅/浮签。
test("ad hoc banners are unified onto the toast system", async () => {
  const [styles, tasksPage, logoutButton] = await Promise.all([
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
    readFile(new URL("../app/tasks/TasksPage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/components/LogoutButton.tsx", import.meta.url), "utf8"),
  ]);

  assert.doesNotMatch(styles, /\.modal-error|\.sidebar-logout-error/);
  assert.doesNotMatch(tasksPage, /modal-error|createError/);
  assert.match(tasksPage, /showToast\("error", reason instanceof Error \? reason\.message : "报告创建失败，请检查日期后重试。"\)/);
  assert.doesNotMatch(logoutButton, /sidebar-logout-error|aria-describedby/);
  assert.match(logoutButton, /showToast\("error", "退出失败，请稍后重试"\)/);
});
