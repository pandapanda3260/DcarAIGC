import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("SPU refresh modal selects a scope before one confirmed request", async () => {
  const page = await readFile(new URL("../app/spu-audience/SpuAudiencePage.tsx", import.meta.url), "utf8");

  for (const scope of ["yesterday", "this_week", "last_week", "full"]) {
    assert.match(page, new RegExp(`key: "${scope}"`));
  }
  assert.match(page, /checked=\{selected\}/);
  assert.match(page, /onChange=\{\(\) => setSelectedRefreshScope\(item\.key\)\}/);
  assert.match(page, /refreshData\(selectedRefreshScope\)/);
  assert.doesNotMatch(page, /refreshData\(item\.key\)/);
  assert.match(page, /refreshRequestRef\.current/);
  assert.match(page, /associate\?mode=\$\{mode\}/);
  assert.match(
    page,
    /await queryClient\.invalidateQueries\(\{ queryKey: queryKeys\.spuAssets, exact: true \}\)/,
  );
});

test("SPU refresh modal has scoped responsive styling and keyboard dialog behavior", async () => {
  const [page, styles] = await Promise.all([
    readFile(new URL("../app/spu-audience/SpuAudiencePage.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
  ]);

  assert.match(page, /aria-labelledby="spu-refresh-title"/);
  assert.match(page, /aria-describedby="spu-refresh-description"/);
  assert.match(page, /event\.key === "Escape"/);
  assert.match(page, /event\.key !== "Tab"/);
  assert.match(page, /document\.body\.style\.overflow = "hidden"/);
  assert.match(page, /<fieldset className="spu-refresh-options">/);
  assert.match(page, /推荐/);
  assert.match(page, /耗时较长/);

  assert.match(styles, /\.modal-panel\.spu-refresh-modal/);
  assert.match(styles, /grid-template-columns: repeat\(2, minmax\(0, 1fr\)\)/);
  assert.match(styles, /\.spu-refresh-option\[data-selected="true"\]/);
  assert.match(styles, /@media \(max-width: 600px\)/);
  assert.match(styles, /\.spu-refresh-options \{ grid-template-columns: 1fr; \}/);
});
