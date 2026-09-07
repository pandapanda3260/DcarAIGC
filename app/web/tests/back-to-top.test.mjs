import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import vm from "node:vm";
import * as React from "react";
import * as jsxRuntime from "react/jsx-runtime";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";

const source = await readFile(new URL("../app/components/BackToTop.tsx", import.meta.url), "utf8");
const code = ts.transpileModule(source, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX },
}).outputText;

function loadComponent(react = React, globals = {}) {
  const modules = {
    react,
    "react/jsx-runtime": jsxRuntime,
    "@phosphor-icons/react": { ArrowUpIcon: (props) => React.createElement("svg", props) },
    "./BackToTop.module.css": { default: { button: "back-top", tooltip: "back-top-tooltip" } },
  };
  const context = vm.createContext({
    exports: {}, ...globals,
    fetch: () => assert.fail("returning to top must not request business data"),
    require: (name) => {
      assert.ok(Object.hasOwn(modules, name), `unexpected back-to-top dependency: ${name}`);
      return modules[name];
    },
  });
  vm.runInContext(code, context);
  return context.exports.default;
}

// Small event/hook doubles exercise this component's observable contracts only.
// Browser QA remains responsible for layout, native scrolling, focus order and observers.
function fixture({ scrollY = 0, innerHeight = 800, scrollHeight = 3200, reduceMotion = false } = {}) {
  const listeners = new Map();
  const eventTarget = (prefix) => ({
    addEventListener(type, fn) {
      const key = `${prefix}:${type}`;
      if (!listeners.has(key)) listeners.set(key, new Set());
      listeners.get(key).add(fn);
    },
    removeEventListener(type, fn) { listeners.get(`${prefix}:${type}`)?.delete(fn); },
    emit(type) { for (const fn of listeners.get(`${prefix}:${type}`) ?? []) fn(); },
  });
  const frames = new Map();
  const observers = [];
  const actions = [];
  const dialogs = [];
  const targets = new Map(["main-content", "custom-top"].map((id) => [id, {
    focus(options) { actions.push({ action: "focus", id, ...options }); },
  }]));
  let frameId = 0;
  const window = {
    ...eventTarget("window"), scrollY, scrollX: 37, innerHeight,
    requestAnimationFrame(fn) { frames.set(++frameId, fn); return frameId; },
    cancelAnimationFrame(id) { frames.delete(id); },
    getComputedStyle(element) { return element.style; },
    matchMedia() { return { matches: reduceMotion }; },
    scrollTo(options) { actions.push({ action: "scroll", ...options }); },
  };
  const document = {
    ...eventTarget("document"), body: {},
    documentElement: { clientHeight: innerHeight, scrollHeight },
    scrollingElement: { scrollHeight },
    querySelectorAll: () => dialogs,
    getElementById: (id) => targets.get(id) ?? null,
  };
  class Observer {
    active = true;
    constructor(callback) { this.callback = callback; observers.push(this); }
    observe() {}
    disconnect() { this.active = false; }
  }
  const states = [];
  const effects = [];
  let stateIndex = 0;
  let effectIndex = 0;
  const component = loadComponent({
    ...React,
    useState(initial) {
      const index = stateIndex++;
      if (!(index in states)) states[index] = typeof initial === "function" ? initial() : initial;
      return [states[index], (value) => { states[index] = typeof value === "function" ? value(states[index]) : value; }];
    },
    useEffect(setup, deps) {
      const index = effectIndex++;
      const previous = effects[index];
      if (!previous || deps.some((dep, i) => !Object.is(dep, previous.deps[i]))) {
        previous?.cleanup?.();
        effects[index] = { deps, setup };
      }
    },
  }, { window, document, MutationObserver: Observer, ResizeObserver: Observer });
  let props = { pageKey: "/contents" };
  function render(nextProps) {
    if (nextProps) props = { ...props, ...nextProps };
    stateIndex = 0;
    effectIndex = 0;
    const tree = component(props);
    for (const effect of effects) {
      if (effect.setup) { effect.cleanup = effect.setup(); delete effect.setup; }
    }
    return tree;
  }
  function flush() {
    for (const [id, callback] of [...frames]) { frames.delete(id); callback(); }
    return render();
  }
  const unmount = () => effects.forEach((effect) => effect.cleanup?.());
  render();
  return { window, document, dialogs, actions, frames, observers, listeners, render, flush, unmount,
    layoutChanged: () => observers.filter((observer) => observer.active).forEach((observer) => observer.callback()),
  };
}

test("back-to-top renders safely without a browser and exposes no initial control", () => {
  const BackToTop = loadComponent();
  assert.equal(renderToStaticMarkup(React.createElement(BackToTop, { pageKey: "/contents" })), "");
});

test("visibility requires more than one viewport and responds to viewport and content changes", (t) => {
  const page = fixture();
  t.after(page.unmount);
  assert.equal(page.flush(), null);
  for (const [scrollY, expected] of [[800, false], [801, true], [1200, true], [0, false]]) {
    page.window.scrollY = scrollY;
    page.window.emit("scroll");
    assert.equal(page.flush() !== null, expected, `scroll offset ${scrollY}`);
  }
  page.window.scrollY = 850;
  page.window.emit("scroll");
  assert.ok(page.flush());
  page.window.innerHeight = 900;
  page.window.emit("resize");
  assert.equal(page.flush(), null);
  page.window.innerHeight = 800;
  page.document.scrollingElement.scrollHeight = 800;
  page.layoutChanged();
  assert.equal(page.flush(), null, "short content must not retain a stale button");
  assert.deepEqual(page.actions, []);
});

test("visible modals hide the control and block activation even before the observer runs", (t) => {
  const page = fixture({ scrollY: 1200 });
  t.after(page.unmount);
  const staleButton = page.flush();
  const dialog = { getClientRects: () => [{}], checkVisibility: () => true };
  page.dialogs.push(dialog);
  staleButton.props.onClick({ detail: 1 });
  assert.deepEqual(page.actions, [], "a newly opened modal must block a stale click handler");
  page.layoutChanged();
  assert.equal(page.flush(), null);
  dialog.checkVisibility = () => false;
  page.layoutChanged();
  assert.ok(page.flush(), "a hidden dialog must not block page navigation");
  page.dialogs.length = 0;
  page.layoutChanged();
  assert.ok(page.flush());
  assert.deepEqual(page.actions, [], "modal and data layout changes must never scroll automatically");
});

test("the accessible icon button only scrolls; keyboard activation focuses the target without scrolling it", (t) => {
  const page = fixture({ scrollY: 1200 });
  t.after(page.unmount);
  const button = page.flush();
  assert.equal(button.type, "button");
  assert.equal(button.props.type, "button");
  assert.equal(button.props["aria-label"], "回到顶部");
  const html = renderToStaticMarkup(button);
  assert.match(html, /<svg[^>]*aria-hidden="true"/);
  assert.doesNotMatch(html, /<(?:a|form|input|select)\b/);
  button.props.onClick({ detail: 1 });
  assert.deepEqual(page.actions, [{ action: "scroll", top: 0, behavior: "smooth" }]);
  button.props.onClick({ detail: 0 });
  assert.deepEqual(page.actions.slice(1), [
    { action: "focus", id: "main-content", preventScroll: true },
    { action: "scroll", top: 0, behavior: "smooth" },
  ]);
});

test("reduced motion returns instantly and custom focus targets are respected", (t) => {
  const page = fixture({ scrollY: 1200, reduceMotion: true });
  t.after(page.unmount);
  page.flush();
  page.render({ targetId: "custom-top" }).props.onClick({ detail: 0 });
  assert.deepEqual(page.actions, [
    { action: "focus", id: "custom-top", preventScroll: true },
    { action: "scroll", top: 0, behavior: "instant" },
  ]);
});

test("route changes clear stale visibility; restored pages recalculate; unmount releases observers and queued work", () => {
  const page = fixture({ scrollY: 1200 });
  assert.ok(page.flush());
  page.window.scrollY = 0;
  assert.equal(page.render({ pageKey: "/accounts" }), null);
  assert.equal(page.flush(), null);
  assert.ok(page.observers.slice(0, 2).every((observer) => !observer.active));
  assert.ok([...page.listeners.values()].every((handlers) => handlers.size <= 1));
  page.window.scrollY = 1400;
  page.window.emit("pageshow");
  assert.ok(page.flush());
  page.window.emit("scroll");
  page.window.emit("resize");
  assert.ok(page.frames.size > 0);
  page.unmount();
  assert.equal(page.frames.size, 0);
  assert.ok(page.observers.every((observer) => !observer.active));
  assert.ok([...page.listeners.values()].every((handlers) => handlers.size === 0));
  assert.deepEqual(page.actions, []);
});
