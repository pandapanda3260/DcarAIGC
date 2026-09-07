import assert from "node:assert/strict";
import test from "node:test";
import { createNavigationFeedbackStore } from "../scripts/navigation-feedback.mjs";

function withLocation(t, href = "https://workbench.example/dcar/overview") {
  const previous = Object.getOwnPropertyDescriptor(globalThis, "window");
  Object.defineProperty(globalThis, "window", { configurable: true, value: { location: new URL(href) } });
  t.after(() => {
    if (previous) Object.defineProperty(globalThis, "window", previous);
    else delete globalThis.window;
  });
}

test("navigation feedback accepts only same-origin primary routes within the configured base path", (t) => {
  withLocation(t);
  const store = createNavigationFeedbackStore();
  for (const section of ["overview", "contents", "accounts", "selling-points", "spu-audience", "tasks", "users"]) {
    store.start(section, `/dcar/${section}`, "/dcar", "push");
    assert.deepEqual(store.getSnapshot(), { id: section, href: `/dcar/${section}`, section });
    assert.equal(store.getSnapshot(), store.getSnapshot(), "snapshots must stay referentially stable");
    assert.equal(store.getServerSnapshot(), null);
  }
  for (const href of ["https://other.example/dcar/contents", "/contents", "/dcar-other/contents", "/dcar/contents?content_id=42", "/dcar/tasks#new", "/dcar/tasks/42", "/dcar/login", "/dcar/unknown", "javascript:alert(1)"]) {
    store.start(1, "/dcar/contents", "/dcar", "push");
    store.start(2, href, "/dcar", "push");
    assert.equal(store.getSnapshot(), null, href);
  }
  store.start(3, "/dcar/contents", "/dcar", "push");
  store.start(4, "/dcar/contents", "/dcar", "refresh");
  assert.equal(store.getSnapshot(), null);
});

test("latest navigation wins and previous completion cannot clear a newer destination", (t) => {
  withLocation(t, "https://workbench.example/overview");
  const store = createNavigationFeedbackStore();
  const notifications = [];
  const unsubscribe = store.subscribe(() => notifications.push(store.getSnapshot()));
  store.start(1, "/contents", "", "push");
  store.start(2, "/tasks", "", "push");
  store.finish(1);
  assert.equal(store.getSnapshot().section, "tasks");
  assert.equal(notifications.length, 2);
  store.start(3, "/overview", "", "traverse");
  assert.equal(store.getSnapshot().section, "overview", "returning to the still committed source also starts feedback");
  store.finish(2);
  assert.equal(store.getSnapshot().id, 3);
  store.finish(3);
  assert.equal(store.getSnapshot(), null);
  assert.equal(notifications.length, 4);
  unsubscribe();
  store.start(4, "/tasks", "", "replace");
  assert.equal(notifications.length, 4);
});

test("reselecting the settled current URL preserves its page but return and traversal still start feedback", (t) => {
  withLocation(t, "https://workbench.example/contents");
  const store = createNavigationFeedbackStore();
  store.start(1, "/contents", "", "navigate");
  assert.equal(store.getSnapshot(), null);
  store.start(2, "/tasks", "", "navigate");
  store.start(3, "/contents", "", "navigate");
  assert.equal(store.getSnapshot().id, 3);
  store.finish(2);
  assert.equal(store.getSnapshot().id, 3);
  store.finish(3);
  store.start(4, "/contents", "", "traverse");
  assert.equal(store.getSnapshot().id, 4, "popstate has already updated window.location before traversal starts");
});
