import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { proxyContentUpdates } from '../app/lib/contentUpdateProxy.ts';

function request(method = 'GET', extra = {}) {
  return new Request('http://127.0.0.1:4174/workbench-api/content-update-jobs', {
    method, headers: { 'x-dcar-authenticated-user': 'operator', ...extra },
    ...(method === 'POST' ? { body: JSON.stringify({ request_id: 'test-request' }) } : {}),
  });
}

test('content job adapter gates identity, fixed routes, and browser origin before forwarding', async () => {
  assert.equal((await proxyContentUpdates(new Request('http://localhost'), ['content-update-jobs'])).status, 401);
  assert.equal((await proxyContentUpdates(request(), ['..', 'readyz'])).status, 404);
  assert.equal((await proxyContentUpdates(request('POST'), ['contents', '1', 'update-jobs'])).status, 403);
  assert.equal((await proxyContentUpdates(request('POST', {
    origin: 'https://attacker.invalid', 'x-forwarded-host': 'localhost:4173',
    'x-forwarded-proto': 'http',
  }), ['contents', '1', 'update-jobs'])).status, 403);
});

test('adapter forwards only the server key and exact operation; never retries uncertain POST', async () => {
  const root = mkdtempSync(join(tmpdir(), 'dcar-update-proxy-'));
  const oldFile = process.env.DCAR_UPDATE_COORDINATOR_TOKEN_FILE;
  const oldFetch = globalThis.fetch;
  try {
    const file = join(root, 'key'); writeFileSync(file, 'fixture-key', { mode: 0o600 });
    process.env.DCAR_UPDATE_COORDINATOR_TOKEN_FILE = file;
    let calls = 0;
    globalThis.fetch = async (url, init) => {
      calls++;
      assert.equal(url, 'http://127.0.0.1:8767/api/v8/contents/1/update-jobs');
      assert.equal(init.headers['X-Dcar-Update-Key'], 'fixture-key');
      assert.equal(init.headers.Cookie, undefined);
      assert.equal(init.redirect, 'error');
      throw new Error('response lost');
    };
    const result = await proxyContentUpdates(request('POST', {
      origin: 'http://localhost:4173', 'x-forwarded-host': 'localhost:4173',
      'x-forwarded-proto': 'http', 'content-type': 'application/json',
    }), ['contents', '1', 'update-jobs']);
    assert.equal(result.status, 503); assert.equal(calls, 1);
    assert.equal((await result.text()).includes('fixture-key'), false);
  } finally {
    globalThis.fetch = oldFetch;
    if (oldFile === undefined) delete process.env.DCAR_UPDATE_COORDINATOR_TOKEN_FILE;
    else process.env.DCAR_UPDATE_COORDINATOR_TOKEN_FILE = oldFile;
    rmSync(root, { recursive: true, force: true });
  }
});
