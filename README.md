# jobs-worker (`solo-empire-jobs`)

Cloudflare Worker for the ADR-0008 **queue plane**
(`architecture-docs:docs/architecture/decisions/ADR-0008-messaging-queue-and-event-broker.md`).
Schedulers only enqueue; this Worker's queue consumer runs the job with retries and parks a job
that keeps failing in a dead-letter queue.

```
Cloudflare cron ─┐                                      ┌─ github_workflow → workflow_dispatch
POST /enqueue ───┴─► queue `jobs` ─► consumer (retry) ──┤
                                         │              └─ http → target via gateway (bearer)
                                         └─ after 5 retries ─► `jobs-dlq` ─► /healthz 503 ─► uptime alert
```

## Behavior

| Route / handler | Behavior |
|---|---|
| `scheduled` | For each cron trigger, enqueues every job in `src/jobs.ts` with that cron. Message id = `<job>:<slot ISO time>`. |
| `POST /enqueue` | Bearer `JOBS_TOKEN`; body `{"job":"<name>"}`. Only jobs in the registry are accepted (`404 unknown_job`), so it can never call an arbitrary URL. `202 {"queued":true,"id":...}`. |
| `queue` (`jobs`) | Runs the job target. `github_workflow`: `POST /repos/{repo}/actions/workflows/{wf}/dispatches`, ack on `204`. `http`: POST the envelope with a bearer and `idempotency-key`, ack on 2xx. Otherwise retry after 60 s × attempt; after 5 retries → `jobs-dlq`. |
| `GET /healthz` | `200 {"ok":true,"dlq_backlog":0}`; `503` while `jobs-dlq` holds messages, which the uptime Worker (`jobs-worker` target) turns into a Telegram alert. |

Message body = ADR-0008 envelope: `{id,type:"job.<name>.requested",version,occurred_at,producer,correlation_id,causation_id,data:{job}}`.

## Add a job

1. Add a `JobDef` to `src/jobs.ts` (name, optional `cron`, target).
2. If it has a new cron string, add it to `[triggers] crons` in `wrangler.toml`; a test fails when the two lists differ.
3. A job that runs inline in an old scheduler: remove that schedule once the queued run has worked for 3 days (ADR-0008 M2).

## Local checks

```bash
npm ci
npm test
npm run typecheck
```

## State (2026-11-03)

Deployed (`33f6320b`) with the Infisical `/cloudflare` token: queues `jobs` and `jobs-dlq`, cron
`47 */2 * * *`, secrets `JOBS_TOKEN` and `GITHUB_DISPATCH_TOKEN` (both in Infisical `/jobs`).
Smoke: `/enqueue jobs-marts` → `202`, consumer dispatched `data-lake-pipelines` `jobs-marts.yml`
(run 41669082329, `workflow_dispatch`); unknown job `404`, no bearer `401`; `/healthz` `200`
with `dlq_backlog: 0`. Watched by the uptime Worker as `jobs-worker`.

First job `jobs-marts`: the workflow keeps its GitHub `schedule` until 2026-11-06 as a fallback
(the workflow is idempotent, so a duplicate run writes nothing), then the schedule is removed.

`GITHUB_DISPATCH_TOKEN` is the existing all-repo fine-grained token (`REGISTRY_EXPORT_GITHUB_TOKEN`).
Owner item: replace it with a fine-grained token limited to **Actions: write** on the job repos.

Rollback: `npx wrangler rollback`, or delete the cron trigger and restore the workflow schedule.
