import assert from 'node:assert/strict';
import test from 'node:test';
import { duplicateReminder } from '../app/lib/duplicateStatus.ts';

test('pending and failed duplicate work never becomes a negative conclusion', () => {
  assert.equal(duplicateReminder('pending', null), '查重处理中');
  assert.equal(duplicateReminder('failed', null), '查重失败，需重试');
  assert.equal(duplicateReminder('ready', null), '未发现重复');
});

test('confirmed independent relations and legacy copy remain usable', () => {
  assert.equal(duplicateReminder('pending', '000123'), '与内容 000123 重复');
  assert.equal(duplicateReminder(undefined, null, '—'), '—');
  assert.equal(duplicateReminder(undefined, null), '未发现重复');
});
