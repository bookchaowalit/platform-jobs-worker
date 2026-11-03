import assert from 'node:assert/strict';
import { test } from 'node:test';
import { cronMatches, parseCron } from '../src/cron.js';

const at = (iso: string) => new Date(iso);
const matches = (expr: string, iso: string) => cronMatches(parseCron(expr)!, at(iso));

test('steps, ranges and lists', () => {
  assert.ok(matches('47 */2 * * *', '2026-11-03T14:47:00Z'));
  assert.ok(!matches('47 */2 * * *', '2026-11-03T15:47:00Z'));
  assert.ok(matches('17 */6 * * *', '2026-11-03T18:17:00Z'));
  assert.ok(matches('2-59/5 * * * *', '2026-11-03T18:07:00Z'));
  assert.ok(!matches('2-59/5 * * * *', '2026-11-03T18:05:00Z'));
  assert.ok(matches('0 1,13 * * *', '2026-11-03T13:00:00Z'));
  assert.ok(matches('5/15 * * * *', '2026-11-03T13:50:00Z'));
});

test('day of week, including 7 as Sunday', () => {
  assert.ok(matches('30 2 * * 1', '2026-11-02T02:30:00Z')); // Monday
  assert.ok(!matches('30 2 * * 1', '2026-11-03T02:30:00Z'));
  assert.ok(matches('0 0 * * 7', '2026-11-08T00:00:00Z')); // Sunday
  assert.ok(matches('0 0 * * 1-5', '2026-11-06T00:00:00Z')); // Friday
});

test('day of month OR day of week when both are restricted', () => {
  assert.ok(matches('0 0 1 * 1', '2026-11-01T00:00:00Z')); // the 1st, a Sunday
  assert.ok(matches('0 0 1 * 1', '2026-11-02T00:00:00Z')); // a Monday
  assert.ok(!matches('0 0 1 * 1', '2026-11-03T00:00:00Z'));
});

test('a stepped wildcard day field keeps wildcard semantics (AND, not OR)', () => {
  assert.ok(matches('0 0 */1 * 1', '2026-11-02T00:00:00Z')); // Monday
  assert.ok(!matches('0 0 */1 * 1', '2026-11-03T00:00:00Z')); // Tuesday
  assert.ok(matches('0 0 */2 * *', '2026-11-03T00:00:00Z')); // day 3
  assert.ok(!matches('0 0 */2 * *', '2026-11-04T00:00:00Z'));
  assert.ok(!matches('0 0 1 * */7', '2026-11-03T00:00:00Z')); // */7 = Sunday only, ANDed with the 1st
});

test('invalid expressions are rejected', () => {
  for (const bad of ['* * * *', '60 * * * *', '* 24 * * *', '*/0 * * * *', '5-1 * * * *', 'a * * * *', '* * 0 * *']) {
    assert.equal(parseCron(bad), null, bad);
  }
});
