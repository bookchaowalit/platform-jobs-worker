// Job queue Worker (ADR-0008 queue plane). Schedulers only enqueue; this Worker's queue consumer
// runs each job's target with retries, and after max_retries the message moves to jobs-dlq.

import * as Sentry from '@sentry/cloudflare';
import { sentryOptions } from './sentry.js';
import { findJob, jobsForCron, type JobDef } from './jobs.js';

export interface QueueLike {
  send(body: unknown, options?: { contentType?: 'json' }): Promise<unknown>;
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
  ack(): void;
  retry(options?: { delaySeconds?: number }): void;
}

export interface MessageBatchLike<T> {
  readonly queue: string;
  readonly messages: readonly MessageLike<T>[];
}

export const RETRY_DELAY_SECONDS = 60;
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
  let body: { job?: unknown };
  try {
    body = JSON.parse(raw) as { job?: unknown };
  } catch {
    return reject(400, 'invalid_payload');
  }
  const job = typeof body.job === 'string' ? findJob(body.job) : undefined;
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

export async function handleScheduled(cron: string, scheduledTime: number, env: Env): Promise<number> {
  const jobs = jobsForCron(cron);
  const slot = new Date(scheduledTime);
  for (const job of jobs) {
    await env.JOBS_QUEUE.send(jobEnvelope(job, `${job.name}:${slot.toISOString()}`, slot, 'jobs-cron'), { contentType: 'json' });
  }
  return jobs.length;
}

// Returns true when the target accepted the job.
export async function runJob(job: JobDef, message: JobEnvelope, env: Env): Promise<boolean> {
  const target = job.target;
  if (target.kind === 'github_workflow') {
    if (!env.GITHUB_DISPATCH_TOKEN) return false;
    const res = await fetch(`https://api.github.com/repos/${target.repo}/actions/workflows/${target.workflow}/dispatches`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${env.GITHUB_DISPATCH_TOKEN}`,
        accept: 'application/vnd.github+json',
        'x-github-api-version': '2022-11-28',
        'user-agent': 'solo-empire-jobs',
        'content-type': 'application/json',
      },
      body: JSON.stringify({ ref: target.ref }),
    });
    return res.status === 204;
  }
  const token = env[target.tokenSecret];
  if (!token) return false;
  const res = await fetch(target.url, {
    method: 'POST',
    headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json', 'idempotency-key': message.id },
    body: JSON.stringify(message),
  });
  return res.ok;
}

export async function handleQueue(batch: MessageBatchLike<JobEnvelope>, env: Env): Promise<void> {
  for (const message of batch.messages) {
    const job = findJob(message.body?.data?.job ?? '');
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
    await handleScheduled(controller.cron, controller.scheduledTime, env);
  },
  queue(batch: MessageBatchLike<JobEnvelope>, env: Env): Promise<void> {
    return handleQueue(batch, env);
  },
});
