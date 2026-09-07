import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import test from "node:test";
import ts from "typescript";

const require = createRequire(import.meta.url);
const source = readFileSync(new URL("../app/contents/ContentTitle.tsx", import.meta.url), "utf8");
const code = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX } }).outputText;

// Geometry is controlled here, not supplied by a real browser. These tests verify when
// measurement runs and which layout is reused; actual inline/emoji geometry remains a UI check.
function harness() {
  let probes = 0, current, nextTimer = 0;
  const timers = new Map(), frames = new Map(), fontListeners = new Map(), resizeListeners = new Set();
  const fonts = {
    status: "loaded", ready: Promise.resolve(),
    addEventListener: (name, callback) => { if (!fontListeners.has(name)) fontListeners.set(name, new Set()); fontListeners.get(name).add(callback); },
    removeEventListener: (name, callback) => fontListeners.get(name)?.delete(callback),
  };
  class Element {
    constructor(config, probe = false) { this.config = config; this.probe = probe; this.children = []; this.dataset = {}; this.style = {}; this.classList = { add: () => {} }; this.textContent = ""; }
    cloneNode() { probes++; return new Element(this.config, true); }
    removeAttribute() {}
    setAttribute() {}
    appendChild(child) { this.children.push(child); return child; }
    remove() {}
    querySelector() { return this; }
    get scrollWidth() { return this.config.width; }
    getBoundingClientRect() {
      const characters = Array.from(this.children[0]?.textContent ?? "").length + (this.children.length > 1 ? 3 : 0);
      return { width: this.config.width, height: this.probe ? Math.max(1, Math.ceil(characters / Math.floor(this.config.width / 8))) * 20 : 40 };
    }
  }
  const react = {
    useId: () => "test-title",
    useRef: () => ({ current: current.element }),
    useState(initial) {
      const instance = current, index = instance.cursor++;
      if (!(index in instance.state)) instance.state[index] = initial;
      return [instance.state[index], (next) => { instance.state[index] = typeof next === "function" ? next(instance.state[index]) : next; }];
    },
    useLayoutEffect(effect) { if (!current.mounted) current.effect = effect; },
  };
  const document = { fonts, createElement: () => new Element(current.config) };
  const window = { addEventListener: (_, callback) => resizeListeners.add(callback), removeEventListener: (_, callback) => resizeListeners.delete(callback) };
  const getComputedStyle = (element) => ({ lineHeight: "20px", getPropertyValue: (name) => name === "font-weight" ? element.config.weight : name === "--list-action-size" ? element.config.actionSize : "default" });
  class ResizeObserver {
    constructor(callback) { this.callback = callback; }
    observe(element) { element.observer = this; }
    disconnect() {}
  }
  const compiled = { exports: {} };
  new Function("require", "module", "exports", "document", "window", "getComputedStyle", "ResizeObserver", "requestAnimationFrame", "cancelAnimationFrame", "setTimeout", "clearTimeout", code)(
    (name) => name === "react" ? react : require(name), compiled, compiled.exports,
    document, window, getComputedStyle, ResizeObserver,
    (callback) => { const id = ++nextTimer; frames.set(id, callback); return id; }, (id) => frames.delete(id),
    (callback, delay) => { const id = ++nextTimer; timers.set(id, { callback, delay }); return id; }, (id) => timers.delete(id),
  );
  return {
    fonts,
    get probes() { return probes; },
    mount(text, config = { width: 160, weight: "500", actionSize: "10px" }) {
      const instance = { config, state: [], cursor: 0, mounted: false, element: new Element(config) };
      const wrapped = compiled.exports.default({ text, href: "https://example.invalid/title" });
      function render() { current = instance; instance.cursor = 0; return wrapped.type(wrapped.props); }
      render();
      current = instance;
      const cleanup = instance.effect();
      instance.mounted = true;
      return {
        config, render, unmount: cleanup,
        get layout() { return instance.state[1]; },
        resize() { current = instance; instance.element.observer.callback(); },
      };
    },
    fontEvent(name) { for (const callback of [...(fontListeners.get(name) ?? [])]) callback(); },
    resizeWindow() { for (const callback of [...resizeListeners]) callback(); },
    expire() { for (const [id, timer] of [...timers]) { assert.equal(timer.delay, 60_000); timers.delete(id); timer.callback(); } },
    async settle() { await Promise.resolve(); for (const [id, callback] of [...frames]) { frames.delete(id); callback(); } },
  };
}

function buttons(node) {
  if (Array.isArray(node)) return node.flatMap(buttons);
  if (!node?.props) return [];
  return [...(node.type === "button" ? [node] : []), ...buttons(node.props.children)];
}

test("50 warm titles avoid both the resolved-font second pass and repeated navigation measurement", async () => {
  const view = harness();
  const titles = Array.from({ length: 50 }, (_, index) => `${index} ${"long title ".repeat(20)}`);
  const first = titles.map((text) => view.mount(text));
  assert.equal(view.probes, 50);
  await view.settle();
  assert.equal(view.probes, 50, "already loaded fonts must not schedule another measurement pass");
  const layouts = first.map((title) => title.layout);
  first.forEach((title) => title.unmount());
  const second = titles.map((text) => view.mount(text));
  await view.settle();
  assert.equal(view.probes, 50, "same text and geometry must reuse measured layouts on return");
  assert.deepEqual(second.map((title) => title.layout), layouts);
  const toggle = buttons(second[0].render())[0];
  assert.equal(toggle.props["aria-expanded"], false);
  toggle.props.onClick();
  assert.equal(buttons(second[0].render())[0].props["aria-expanded"], true);
  second.forEach((title) => title.unmount());
});

test("width, font style, action size and changed text never reuse an incompatible prefix", async () => {
  const view = harness();
  const title = view.mount("abcdefghij".repeat(20));
  const wide = title.layout.prefix;
  title.config.width = 80;
  title.resize();
  assert.ok(title.layout.prefix.length < wide.length);
  assert.equal(view.probes, 2);
  title.config.weight = "700";
  view.resizeWindow(); await view.settle();
  assert.equal(view.probes, 3);
  title.config.actionSize = "14px";
  title.resize();
  assert.equal(view.probes, 4);
  title.unmount();
  const changed = view.mount("Updated " + "abcdefghij".repeat(20));
  assert.match(changed.layout.prefix, /^Updated/);
  assert.equal(view.probes, 5);
  changed.unmount();
});

test("font completion or failure while pages are unmounted invalidates old measurements", async () => {
  const view = harness();
  const text = "font-sensitive title ".repeat(20);
  view.mount(text).unmount();
  view.fontEvent("loadingdone");
  view.mount(text).unmount();
  assert.equal(view.probes, 2);
  view.fontEvent("loadingerror");
  view.mount(text).unmount();
  assert.equal(view.probes, 3);
  view.fonts.status = "loading";
  let ready;
  view.fonts.ready = new Promise((resolve) => { ready = resolve; });
  const loading = view.mount(text);
  assert.equal(view.probes, 4, "in-progress font loads must bypass the cache");
  view.fonts.status = "loaded";
  view.fontEvent("loadingdone"); ready();
  await view.settle();
  assert.equal(view.probes, 5, "font readiness and its event coalesce into one measurement");
  loading.unmount();
});

test("title memoization evicts older entries, expires promptly and skips oversized text", () => {
  const view = harness();
  for (let index = 0; index < 257; index++) view.mount(`title-${index}`).unmount();
  assert.equal(view.probes, 257);
  view.mount("title-256").unmount();
  assert.equal(view.probes, 257);
  view.mount("title-0").unmount();
  assert.equal(view.probes, 258);
  view.expire();
  view.mount("title-256").unmount();
  assert.equal(view.probes, 259);
  const oversized = "x".repeat(4097);
  view.mount(oversized).unmount(); view.mount(oversized).unmount();
  assert.equal(view.probes, 261);
});
