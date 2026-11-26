// Job queue Worker (ADR-0008 queue plane). Schedulers only enqueue; this Worker's queue consumer
// runs each job's target with retries, and after max_retries the message moves to jobs-dlq.
// The jobs-dlq consumer never runs a job again: it clears a parked job once a later run of the
// same workflow has succeeded, and keeps it (so /healthz stays 503) until then, re-parking a copy
// before queue retention could drop it.

import * as Sentry from '@sentry/cloudflare';
import { sentryOptions } from './sentry.js';
import { findJob, jobsDueAt, type JobDef } from './jobs.js';

export interface QueueLike {
  send(body: unknown, options?: { contentType?: 'json'; delaySeconds?: number }): Promise<unknown>;
  metrics?(): Promise<{ backlogCount: number }>;
}

export interface Env {
  JOBS_QUEUE: QueueLike;
  JOBS_DLQ?: QueueLike;
  JOBS_TOKEN?: string;
  GITHUB_DISPATCH_TOKEN?: string;
  GATEWAY_TOKEN?: string;
  SENTRY_DSN?: string;
}

export interface MessageLike<T> {
  readonly id: string;
  readonly body: T;
  readonly attempts: number;
  readonly timestamp: Date;
  ack(): void;
  retry(options?: { delaySeconds?: number }): void;
}

export interface MessageBatchLike<T> {
  readonly queue: string;
  readonly messages: readonly MessageLike<T>[];
}

export const RETRY_DELAY_SECONDS = 60;
// A parked job is checked again after half its age (time since its slot), from 15 min to 6 h, so a
// wide outage that parks hundreds of jobs costs few queue operations (Workers Free: 10,000 a day).
export const DLQ_MIN_RECHECK_SECONDS = 15 * 60;
export const DLQ_MAX_RECHECK_SECONDS = 6 * 3600;
// Queue retention (24 h on Workers Free) or max_retries would silently drop an unresolved job and
// turn /healthz green; a copy older than this is sent to jobs-dlq again first.
export const DLQ_RESEND_AFTER_MS = 18 * 3600_000;
export const DLQ_QUEUE = 'jobs-dlq';
export const TARGET_TIMEOUT_MS = 15_000;
const MAX_BODY_BYTES = 4 * 1024;

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
  });
}

const reject = (status: number, error: string) => json({ error }, status);

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

// ADR-0008 envelope. A scheduled run gets a deterministic id (job + slot), so consumers dedupe on it.
export function jobEnvelope(job: JobDef, id: string, occurredAt: Date, producer: string) {
  return {
    id,
    type: `job.${job.name}.requested`,
    version: 1,
    occurred_at: occurredAt.toISOString(),
    producer,
    correlation_id: id,
    causation_id: null,
    data: { job: job.name },
  };
}

export type JobEnvelope = ReturnType<typeof jobEnvelope>;

async function handleEnqueue(request: Request, env: Env): Promise<Response> {
  if (!env.JOBS_TOKEN) return reject(503, 'not_configured');
  const auth = request.headers.get('authorization') ?? '';
  if (!timingSafeEqual(auth, `Bearer ${env.JOBS_TOKEN}`)) return reject(401, 'unauthorized');
  const raw = await request.text();
  if (raw.length > MAX_BODY_BYTES) return reject(413, 'payload_too_large');
  let body: unknown;
  try {
    body = JSON.parse(raw);
  } catch {
    return reject(400, 'invalid_payload');
  }
  if (typeof body !== 'object' || body === null || Array.isArray(body)) return reject(400, 'invalid_payload');
  const name = (body as { job?: unknown }).job;
  const job = typeof name === 'string' ? findJob(name) : undefined;
  if (!job) return reject(404, 'unknown_job');
  const id = `${job.name}:manual:${crypto.randomUUID()}`;
  await env.JOBS_QUEUE.send(jobEnvelope(job, id, new Date(), 'enqueue-api'), { contentType: 'json' });
  return json({ queued: true, id }, 202);
}

async function handleHealth(env: Env): Promise<Response> {
  let dlqBacklog: number | null = null;
  try {
    dlqBacklog = env.JOBS_DLQ?.metrics ? (await env.JOBS_DLQ.metrics()).backlogCount : null;
  } catch {
    dlqBacklog = null;
  }
  // A parked job means a failed job: answer 503 so the uptime Worker alerts on Telegram.
  if (dlqBacklog !== null && dlqBacklog > 0) return json({ ok: false, dlq_backlog: dlqBacklog }, 503);
  return json({ ok: true, dlq_backlog: dlqBacklog });
}

export async function handleRequest(request: Request, env: Env): Promise<Response> {
  const { pathname } = new URL(request.url);
  if (pathname === '/healthz' && request.method === 'GET') return handleHealth(env);
  if (pathname === '/enqueue') {
    if (request.method !== 'POST') return reject(405, 'method_not_allowed');
    return handleEnqueue(request, env);
  }
  return reject(404, 'not_found');
}

// The every-minute trigger: enqueue the jobs whose cron matches this minute (UTC).
export async function handleScheduled(scheduledTime: number, env: Env): Promise<number> {
  const slot = new Date(Math.floor(scheduledTime / 60_000) * 60_000);
  const jobs = jobsDueAt(slot);
  for (const job of jobs) {
    await env.JOBS_QUEUE.send(jobEnvelope(job, `${job.name}:${slot.toISOString()}`, slot, 'jobs-cron'), { contentType: 'json' });
  }
  return jobs.length;
}

function githubHeaders(token: string): Record<string, string> {
  return {
    authorization: `Bearer ${token}`,
    accept: 'application/vnd.github+json',
    'x-github-api-version': '2022-11-28',
    'user-agent': 'solo-empire-jobs',
  };
}

// Returns true when the target accepted the job.
export async function runJob(job: JobDef, message: JobEnvelope, env: Env): Promise<boolean> {
  const target = job.target;
  if (target.kind === 'github_workflow') {
    if (!env.GITHUB_DISPATCH_TOKEN) return false;
    const res = await fetch(`https://api.github.com/repos/${target.repo}/actions/workflows/${target.workflow}/dispatches`, {
      method: 'POST',
      headers: { ...githubHeaders(env.GITHUB_DISPATCH_TOKEN), 'content-type': 'application/json' },
      body: JSON.stringify(target.inputs ? { ref: target.ref, inputs: target.inputs } : { ref: target.ref }),
      signal: AbortSignal.timeout(TARGET_TIMEOUT_MS),
    });
    return res.status === 204;
  }
  const token = env[target.tokenSecret];
  if (!token) return false;
  const res = await fetch(target.url, {
    method: 'POST',
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', 'idempotency-key': message.id },
    body: JSON.stringify(message),
    signal: AbortSignal.timeout(TARGET_TIMEOUT_MS),
  });
  return res.ok;
}

// True when a run of the job's workflow on its ref, created at or after the parked slot, succeeded.
// http jobs have no run history to read, so they stay parked until the message expires.
export async function laterRunSucceeded(job: JobDef, message: JobEnvelope, env: Env): Promise<boolean> {
  const target = job.target;
  if (target.kind !== 'github_workflow' || !env.GITHUB_DISPATCH_TOKEN) return false;
  const slot = new Date(message.occurred_at);
  if (Number.isNaN(slot.getTime())) return false;
  const url = new URL(`https://api.github.com/repos/${target.repo}/actions/workflows/${target.workflow}/runs`);
  url.searchParams.set('status', 'success');
  url.searchParams.set('branch', target.ref);
  url.searchParams.set('created', `>=${slot.toISOString()}`);
  url.searchParams.set('per_page', '1');
  const res = await fetch(url.toString(), {
    method: 'GET',
    headers: githubHeaders(env.GITHUB_DISPATCH_TOKEN),
    signal: AbortSignal.timeout(TARGET_TIMEOUT_MS),
  });
  if (!res.ok) return false;
  const body = (await res.json()) as { total_count?: unknown };
  return typeof body.total_count === 'number' && body.total_count > 0;
}

export function dlqRecheckDelay(occurredAt: string, now = Date.now()): number {
  const slot = Date.parse(occurredAt);
  if (Number.isNaN(slot)) return DLQ_MAX_RECHECK_SECONDS;
  const half = Math.floor((now - slot) / 2000);
  return Math.min(DLQ_MAX_RECHECK_SECONDS, Math.max(DLQ_MIN_RECHECK_SECONDS, half));
}

async function handleDeadLetters(batch: MessageBatchLike<JobEnvelope>, env: Env): Promise<void> {
  for (const message of batch.messages) {
    const name = message.body?.data?.job;
    const job = typeof name === 'string' ? findJob(name) : undefined;
    if (!job) {
      console.warn('jobs-dlq: unknown job dropped', message.id);
      message.ack();
      continue;
    }
    let cleared = false;
    try {
      cleared = await laterRunSucceeded(job, message.body, env);
    } catch {
      cleared = false;
    }
    if (cleared) {
      console.log('jobs-dlq: cleared, a later run succeeded', message.body.id);
      message.ack();
      continue;
    }
    const delaySeconds = dlqRecheckDelay(message.body.occurred_at);
    if (Date.now() - message.timestamp.getTime() < DLQ_RESEND_AFTER_MS) {
      message.retry({ delaySeconds });
      continue;
    }
    try {
      if (!env.JOBS_DLQ) throw new Error('JOBS_DLQ not bound');
      await env.JOBS_DLQ.send(message.body, { contentType: 'json', delaySeconds });
      message.ack();
    } catch {
      // Keep the old copy and try the re-park again soon; it still has hours of retention.
      message.retry({ delaySeconds: DLQ_MIN_RECHECK_SECONDS });
    }
  }
}

export async function handleQueue(batch: MessageBatchLike<JobEnvelope>, env: Env): Promise<void> {
  if (batch.queue === DLQ_QUEUE) return handleDeadLetters(batch, env);
  for (const message of batch.messages) {
    const name = message.body?.data?.job;
    const job = typeof name === 'string' ? findJob(name) : undefined;
    if (!job) {
      // Removed from the registry after it was queued: nothing to run.
      console.warn('jobs: unknown job dropped', message.id);
      message.ack();
      continue;
    }
    let ok = false;
    try {
      ok = await runJob(job, message.body, env);
    } catch {
      ok = false;
    }
    if (ok) message.ack();
    else message.retry({ delaySeconds: RETRY_DELAY_SECONDS * message.attempts });
  }
}

interface ScheduledControllerLike {
  readonly cron: string;
  readonly scheduledTime: number;
}

// Uncaught errors reach Sentry when SENTRY_DSN is set (errors only; see src/sentry.ts).
export default Sentry.withSentry((env: Env) => sentryOptions(env), {
  fetch(request: Request, env: Env): Promise<Response> {
    return handleRequest(request, env);
  },
  async scheduled(controller: ScheduledControllerLike, env: Env): Promise<void> {
    await handleScheduled(controller.scheduledTime, env);
  },
  queue(batch: MessageBatchLike<JobEnvelope>, env: Env): Promise<void> {
    return handleQueue(batch, env);
  },
});
