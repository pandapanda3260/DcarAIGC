import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import http from "node:http";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const { build } = require("esbuild");
const { chromium } = require("playwright");
const root = fileURLToPath(new URL("../", import.meta.url));
const titles = [
  "真正想买车的四个瞬间。很多人不理解，普通人为什么一定要咬牙买一台车。是下班遇上暴雨，浑身淋透，在街头狼狈不堪。是逢年过节走亲戚，目睹没车带来的窘迫与难堪。",
  "年轻人第一台车🔥按预算选车清单！。年轻人人生第一台车，不知道自己预算该看什么车？ 从8万入门代步，一直到40万以上豪华轿车，全部给你整理好了。#年轻人第一台车 #买车攻略 #新手买车",
  "一家人👨‍👩‍👧‍👦出行🇨🇳买车👍🏽预算清单。".repeat(12),
  "ElectricVehicleComparisonWithoutSpaces".repeat(12),
  "短标题",
];

test("content title keeps its two-line ellipsis and toggle inline across widths", async (t) => {
  const temporary = await mkdtemp(path.join(tmpdir(), "dcar-title-browser-"));
  t.after(() => rm(temporary, { recursive: true, force: true }));
  const compiled = await build({ absWorkingDir: root, bundle: true, write: false,
    outdir: temporary, format: "iife", platform: "browser", jsx: "automatic",
    define: { "process.env.NODE_ENV": '"production"' },
    stdin: { resolveDir: root, loader: "tsx", contents: `
      import React from "react";
      import {createRoot} from "react-dom/client";
      import Title from "./app/contents/ContentTitle";
      import styles from "./app/contents/ContentsPage.module.css";
      const titles=${JSON.stringify(titles)};
      createRoot(document.getElementById("root")).render(<main style={{padding:24}}>
        <article className={styles.listPanel}><div className={styles.listBody}><section className={styles.dateGroup}>
          <table className={styles.table}><colgroup><col className={styles.contentColumn}/><col className={styles.sellingColumn}/><col className={styles.readColumn}/><col className={styles.commentColumn}/><col className={styles.likeColumn}/><col className={styles.classificationColumn}/><col className={styles.actionColumn}/></colgroup>
            <tbody>{titles.map((text,index)=><tr key={index}><td className={styles.contentCell}>
              <div className={styles.contentMain}><span/><div className={styles.contentCopy}><div className={styles.titleLine} id={"title-"+index}>
                <Title text={text} href={"https://example.invalid/content/"+index}/>
              </div></div></div></td><td/><td/><td/><td/><td/><td/></tr>)}</tbody>
          </table>
        </section></div></article>
      </main>);` },
  });
  const script = compiled.outputFiles.find((f) => f.path.endsWith(".js")).text;
  const styles = (await readFile(path.join(root, "app/globals.css"), "utf8")).replace(/^@import[^;]+;/, "")
    + compiled.outputFiles.find((f) => f.path.endsWith(".css")).text;
  const server = http.createServer((req, res) => {
    if (req.url === "/bundle.js") { res.setHeader("Content-Type", "application/javascript"); res.end(script); }
    else if (req.url === "/style.css") { res.setHeader("Content-Type", "text/css"); res.end(styles); }
    else { res.setHeader("Content-Type", "text/html; charset=utf-8"); res.end('<html><head><meta name="viewport" content="width=device-width, initial-scale=1"><link rel="stylesheet" href="/style.css"></head><body><div id="root"></div><script src="/bundle.js"></script></body></html>'); }
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const browser = await chromium.launch({ headless: true });
  t.after(() => browser.close());
  const page = await browser.newPage();
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const url = `http://127.0.0.1:${server.address().port}`;
  await page.goto(url);
  for (const width of [1512, 1200, 980, 600, 375, 1512]) {
    await page.setViewportSize({ width, height: 900 });
    await page.waitForFunction(() => document.querySelectorAll('.content-title[data-measured="true"]').length === 5);
    await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    const geometry = await page.locator('.content-title').evaluateAll((nodes) => nodes.map((node) => {
      const link = node.querySelector('a'), button = node.querySelector('button');
      const rect = node.getBoundingClientRect(), ellipsis = node.querySelector('.content-title-ellipsis')?.getBoundingClientRect();
      const range = document.createRange(); range.selectNodeContents(link);
      const lines = [...range.getClientRects()], last = lines.at(-1), first = lines[0];
      const tail = node.querySelector('.content-title-tail')?.getBoundingClientRect();
      return { prefix: link.textContent, full: link.getAttribute('aria-label'), hasToggle: !!button,
        height: rect.height, lineHeight: parseFloat(getComputedStyle(node).lineHeight), width: rect.width,
        firstLineGap: first ? rect.right - first.right : null,
        gap: ellipsis && last ? ellipsis.left - last.right : null,
        tailInside: !tail || tail.left >= rect.left - 0.5 && tail.right <= rect.right + 0.5 && tail.bottom <= rect.bottom + 0.5,
        tailOnLastLine: !tail || !!last && Math.abs(tail.top - last.top) < 2,
      };
    }));
    for (const [index, item] of geometry.entries()) {
      assert.ok(item.prefix.length > 0, `visible title at ${width}: ${JSON.stringify(item)}`);
      assert.equal(item.full, titles[index], `full accessible title at ${width}`);
      assert.ok(item.height <= 2 * item.lineHeight + 0.5, `two-line limit at ${width}: ${index}`);
      assert.ok(item.tailInside && item.tailOnLastLine, `tail stays on last line inside title at ${width}: ${index}`);
      if (item.hasToggle) {
        assert.ok(Math.abs(item.gap) < 1, `no detached ellipsis at ${width}: ${item.gap}`);
        const boundaries = new Set([0, ...Array.from(new Intl.Segmenter("zh-CN", { granularity: "grapheme" }).segment(titles[index]), (s) => s.index + s.segment.length)]);
        assert.ok(titles[index].startsWith(item.prefix) && boundaries.has(item.prefix.length), `no split emoji at ${width}`);
      }
    }
    assert.equal(geometry[4].hasToggle, false, `short title has no toggle at ${width}`);
    assert.ok(geometry[0].firstLineGap < geometry[0].lineHeight * 1.5, `first line uses the title width at ${width}`);
    if (process.env.DCAR_TITLE_SCREENSHOT_DIR && [1512, 375].includes(width)) {
      await page.screenshot({ path: path.join(process.env.DCAR_TITLE_SCREENSHOT_DIR, `title-inline-${width}.png`), fullPage: true });
    }
  }
  const target = page.locator('#title-0');
  await target.getByRole('button', { name: '展开' }).click();
  assert.equal(await target.locator('a').textContent(), titles[0]);
  assert.equal(await target.getByRole('button', { name: '收起' }).getAttribute('aria-expanded'), 'true');
  assert.equal(page.url(), url + '/');
  assert.equal(browser.contexts()[0].pages().length, 1);
  await page.setViewportSize({ width: 600, height: 900 });
  await target.getByRole('button', { name: '收起' }).click();
  await page.waitForFunction(() => {
    const node = document.querySelector('#title-0 .content-title');
    return node.getBoundingClientRect().height <= 2 * parseFloat(getComputedStyle(node).lineHeight) + 0.5;
  });
  assert.deepEqual(errors, []);
});
