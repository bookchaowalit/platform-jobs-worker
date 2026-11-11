import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { afterEach, test } from 'node:test';
import { parseCron } from '../src/cron.js';
import { JOBS } from '../src/jobs.js';
import { handleQueue, handleRequest, handleScheduled, jobEnvelope, type Env, type JobEnvelope, type MessageLike } from '../src/worker.js';

// Test-only fixtures; not real credentials.
const jobsCredential = ['fixture', 'jobs', 'value'].join('-');
const dispatchCredential = ['fixture', 'dispatch', 'value'].join('-');
const realFetch = globalThis.fetch;
afterEach(() => { globalThis.fetch = realFetch; });

function makeEnv(extra: Partial<Env> = {}) {
  const sent: JobEnvelope[] = [];
  const env: Env = {
    JOBS_QUEUE: { async send(body) { sent.push(body as JobEnvelope); return null; } },
    JOBS_TOKEN: jobsCredential,
    GITHUB_DISPATCH_TOKEN: dispatchCredential,
    ...extra,
  };
  return { env, sent };
}

function enqueue(body: unknown, credential = jobsCredential) {
  return new Request('https://jobs.example.com/enqueue', {
    method: 'POST',
    headers: { authorization: `Bearer ${credential}`, 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
}

function message(body: JobEnvelope, attempts = 1) {
  const state = { acked: false, retryDelay: undefined as number | undefined };
  const m: MessageLike<JobEnvelope> = {
    id: 'm1', body, attempts,
    ack() { state.acked = true; },
    retry(options) { state.retryDelay = options?.delaySeconds ?? 0; },
  };
  return { m, state };
}

test('schedules the ten finance outbox relays hourly at :41-:50 (SPEC-005)', () => {
  const cells = ['budgets', 'accounts', 'transactions', 'cash-flow', 'reconciliations', 'business-assets', 'investments', 'liabilities', 'subscriptions', 'tax'];
  cells.forEach((cell, n) => {
    const job = JOBS.find((j) => j.name === `finance-relay-${cell}`);
    assert.ok(job, cell);
    assert.equal(job.cron, `${41 + n} * * * *`);
    assert.deepEqual(job.target, { kind: 'github_workflow', repo: `bookchaowalit/finance-${cell}-api`, workflow: 'relay-outbox.yml', ref: 'main', inputs: { source: 'jobs-queue' } });
  });
});

const marts = JOBS.find((j) => j.name === 'jobs-marts')!;

test('the Worker has one every-minute trigger and every registry cron parses', () => {
  const toml = readFileSync(new URL('../wrangler.toml', import.meta.url), 'utf8');
  assert.deepEqual(JSON.parse(/crons = (\[.*\])/.exec(toml)![1]), ['* * * * *']);
  for (const job of JOBS) if (job.cron) assert.ok(parseCron(job.cron), `${job.name}: ${job.cron}`);
  assert.equal(new Set(JOBS.map((j) => j.name)).size, JOBS.length, 'job names are unique');
});

test('a minute tick enqueues only the jobs due then, with a deterministic id per slot', async () => {
  const { env, sent } = makeEnv();
  const slot = Date.UTC(2026, 10, 3, 14, 47, 12); // seconds are dropped from the slot
  assert.equal(await handleScheduled(slot, env), 1);
  assert.equal(sent[0].id, 'jobs-marts:2026-11-03T14:47:00.000Z');
  assert.equal(sent[0].type, 'job.jobs-marts.requested');
  assert.equal(sent[0].producer, 'jobs-cron');
  assert.equal(await handleScheduled(Date.UTC(2026, 10, 3, 14, 48), env), 0);
  // Monday 2026-11-02 02:30 UTC: two weekly jobs share the slot.
  sent.length = 0;
  assert.equal(await handleScheduled(Date.UTC(2026, 10, 2, 2, 30), env), 2);
  assert.deepEqual(sent.map((m) => m.data.job).sort(), ['data-lake-cloud-contract', 'mcp-catalog-refresh']);
});

test('/enqueue needs the bearer credential and a registered job', async () => {
  const { env, sent } = makeEnv();
  assert.equal((await handleRequest(enqueue({ job: 'jobs-marts' }, 'wrong'), env)).status, 401);
  assert.equal((await handleRequest(enqueue({ job: 'https://evil.example.com' }), env)).status, 404);
  const ok = await handleRequest(enqueue({ job: 'jobs-marts' }), env);
  assert.equal(ok.status, 202);
  assert.equal(sent.length, 1);
  assert.match(sent[0].id, /^jobs-marts:manual:/);
  for (const bad of [null, [], 'jobs-marts', 7]) {
    assert.equal((await handleRequest(enqueue(bad), env)).status, 400, `rejects ${JSON.stringify(bad)}`);
  }
  const off = makeEnv({ JOBS_TOKEN: undefined });
  assert.equal((await handleRequest(enqueue({ job: 'jobs-marts' }), off.env)).status, 503);
});

test('consumer dispatches the GitHub workflow and acks on 204', async () => {
  const calls: { url: string; init: RequestInit }[] = [];
  globalThis.fetch = (async (url: string, init: RequestInit) => { calls.push({ url, init }); return new Response(null, { status: 204 }); }) as typeof fetch;
  const { env } = makeEnv();
  const { m, state } = message(jobEnvelope(marts, 'jobs-marts:x', new Date(), 'test'));
  await handleQueue({ queue: 'jobs', messages: [m] }, env);
  assert.equal(state.acked, true);
  assert.equal(calls[0].url, 'https://api.github.com/repos/bookchaowalit/data-lake-pipelines/actions/workflows/jobs-marts.yml/dispatches');
  assert.deepEqual(JSON.parse(calls[0].init.body as string), { ref: 'main', inputs: { source: 'jobs-queue' } });
  assert.equal((calls[0].init.headers as Record<string, string>).authorization, `Bearer ${dispatchCredential}`);
  assert.ok(calls[0].init.signal instanceof AbortSignal, 'dispatch is bounded by a timeout');
});

test('consumer retries with growing delay on failure, a missing credential, or a network error', async () => {
  globalThis.fetch = (async () => new Response('no', { status: 422 })) as typeof fetch;
  const failed = message(jobEnvelope(marts, 'a', new Date(), 'test'), 2);
  await handleQueue({ queue: 'jobs', messages: [failed.m] }, makeEnv().env);
  assert.equal(failed.state.acked, false);
  assert.equal(failed.state.retryDelay, 120);

  const noCredential = message(jobEnvelope(marts, 'b', new Date(), 'test'));
  await handleQueue({ queue: 'jobs', messages: [noCredential.m] }, makeEnv({ GITHUB_DISPATCH_TOKEN: undefined }).env);
  assert.equal(noCredential.state.retryDelay, 60);

  globalThis.fetch = (async () => { throw new Error('offline'); }) as typeof fetch;
  const offline = message(jobEnvelope(marts, 'c', new Date(), 'test'), 3);
  await handleQueue({ queue: 'jobs', messages: [offline.m] }, makeEnv().env);
  assert.equal(offline.state.retryDelay, 180);
});

test('consumer acks a job that is no longer registered', async () => {
  const body = { ...jobEnvelope(marts, 'd', new Date(), 'test'), data: { job: 'removed-job' } };
  const { m, state } = message(body);
  await handleQueue({ queue: 'jobs', messages: [m] }, makeEnv().env);
  assert.equal(state.acked, true);
});

test('/healthz answers 503 when the DLQ holds parked jobs', async () => {
  const healthy = makeEnv({ JOBS_DLQ: { async send() { return null; }, async metrics() { return { backlogCount: 0 }; } } });
  const ok = await handleRequest(new Request('https://jobs.example.com/healthz'), healthy.env);
  assert.equal(ok.status, 200);
  assert.deepEqual(await ok.json(), { ok: true, dlq_backlog: 0 });
  const parked = makeEnv({ JOBS_DLQ: { async send() { return null; }, async metrics() { return { backlogCount: 2 }; } } });
  const bad = await handleRequest(new Request('https://jobs.example.com/healthz'), parked.env);
  assert.equal(bad.status, 503);
  assert.deepEqual(await bad.json(), { ok: false, dlq_backlog: 2 });
});
