import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("feedback remains above hero cards and reserves its own layout space", async () => {
  const [component, styles] = await Promise.all([
    readFile(new URL("../app/components/Feedback.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
  ]);

  assert.match(component, /notice feedback-notice/);
  assert.match(styles, /\.feedback-notice\s*\{[^}]*z-index:\s*20;[^}]*margin-bottom:\s*14px;/s);
  assert.match(styles, /\.feedback-notice \+ \.page-stack\s*\{\s*margin-top:\s*0;\s*\}/);
});
