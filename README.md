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
                                                                 └─ cleared once a later run succeeded
```

## Behavior

| Route / handler | Behavior |
|---|---|
| `scheduled` | One every-minute trigger (Workers Free allows 5 cron triggers per account). Each minute, `src/cron.ts` matches every job's own UTC cron and enqueues the due jobs. Message id = `<job>:<slot ISO time>`. |
| `POST /enqueue` | Bearer `JOBS_TOKEN`; body `{"job":"<name>"}`. Only jobs in the registry are accepted (`404 unknown_job`), so it can never call an arbitrary URL. `202 {"queued":true,"id":...}`. |
| `queue` (`jobs`) | Runs the job target. `github_workflow`: `POST /repos/{repo}/actions/workflows/{wf}/dispatches` with the job's `inputs`, ack on `204`. `http`: POST the envelope with a bearer and `idempotency-key`, ack on 2xx. Otherwise retry after 60 s × attempt; after 5 retries → `jobs-dlq`. |
| `queue` (`jobs-dlq`) | Never runs a job again. On arrival, then after half the parked slot's age (15 min to 6 h), it reads the workflow's runs (`status=success`, the job's `ref`, `created>=` the slot) and acks the message once one exists, so the alert clears by itself. Until then it stays parked; a copy older than 18 h is sent to `jobs-dlq` again before queue retention (24 h on Workers Free) can drop it, so `/healthz` never turns green on an unresolved job. An `http` job has no run history and stays until purged; a job no longer in the registry is dropped. |
| `GET /healthz` | `200 {"ok":true,"dlq_backlog":0}`; `503` while `jobs-dlq` holds messages, which the uptime Worker (`jobs-worker` target) turns into a Telegram alert. |

Message body = ADR-0008 envelope: `{id,type:"job.<name>.requested",version,occurred_at,producer,correlation_id,causation_id,data:{job}}`.

## Add a job

1. Add a `JobDef` to `src/jobs.ts` (name, optional UTC `cron`, target). No `wrangler.toml` change: the every-minute trigger covers every cron.
2. Check the target first: a dispatch to a ref that does not exist must answer `422 No ref found` (token can dispatch; nothing runs). `Cannot trigger ... disabled workflow` = leave it out.
3. Read the workflow for `github.event_name` checks: a dispatch is not a `schedule` event. Give it an input (as `jobs-marts` has `source: jobs-queue`) when the behavior differs.
4. Replace the workflow's `schedule:` with a comment naming the job, in the same change.

## Local checks

```bash
npm ci
npm test
npm run typecheck
```

## State (2026-11-03)

Deployed with the Infisical `/cloudflare` token: queues `jobs` and `jobs-dlq`, one cron trigger
`* * * * *`, secrets `JOBS_TOKEN` and `GITHUB_DISPATCH_TOKEN` (both in Infisical `/jobs`).
Watched by the uptime Worker as `jobs-worker`.

| Job | Cron (UTC) | Workflow | Moved |
|---|---|---|---|
| `jobs-marts` | `47 */2 * * *` | data-lake-pipelines `jobs-marts.yml` (input `source=jobs-queue`) | 2026-11-03 |
| `news-marts` | `35 */2 * * *` | data-lake-pipelines `news-marts.yml` (input `source=jobs-queue`) | 2026-11-09 |
| `sales-marts` | `20 * * * *` | data-lake-pipelines `sales-marts.yml` (input `source=jobs-queue`) | 2026-11-09 |
| `github-activity` | `10 3 * * *` | data-lake-pipelines `github-activity.yml` | 2026-11-12 (new) |
| `hf-trending` | `40 3 * * *` | data-lake-pipelines `hf-trending.yml` | 2026-11-12 (new) |
| `career-job-capture` | `17 */6 * * *` | career-job-scraper `scheduled-capture.yml` | 2026-11-03 |
| `org-hub-sync` | `0 2 * * *` | platform-ops-cli `org-hub-sync.yml` | 2026-11-03 |
| `data-lake-cloud-contract` | `30 2 * * 1` | data-lake-pipelines | 2026-11-03 |
| `free-cloud-e2e` | `17 3 * * 1` | data-lake-pipelines | 2026-11-03 |
| `polyglot-bench` | `0 2 * * 1` | devtools-polyglot-benchmark `bench.yml` | 2026-11-03 |
| `mcp-catalog-refresh` | `30 2 * * 1` | bookchaowalit-empire (solo-empire until 2026-11-12) | 2026-11-03 |
| `event-sink` | `5 * * * *` | data-lake-pipelines `event-sink.yml` (input `source=jobs-queue`) | 2026-11-04 |
| `posture-monitor` | `23 */6 * * *` | platform-posture-monitor `posture.yml` | 2026-11-17 |
| `monitor-watchdog` | `49 * * * *` | platform-uptime-worker `watchdog.yml` (its `:17` schedule stays) | 2026-11-17 (added) |
| `knowledge-relay` | `55 * * * *` | platform-knowledge-api `relay-outbox.yml` (input `source=jobs-queue`) | 2026-11-04 |
| `intake-relay` | `53 * * * *` | platform-intake-api `relay-outbox.yml` (input `source=jobs-queue`) | 2026-11-04 |
| `event-notifier` | `58 * * * *` | platform-event-notifier `notify.yml` (input `source=jobs-queue`) | 2026-11-04 |
| `vuln-digest` | `17 1 * * *` | platform-vuln-digest `digest.yml` (new, no inputs) | 2026-11-04 |

Kept on the GitHub cron on purpose: life-dashboard-web `autopilot.yml` (a dispatch skips the
`AUTOPILOT_ENABLED` gate and defaults to dry-run) and platform-ops-cli `ci-runner-failover.yml`
(a dispatch defaults to dry-run; it is the CI runner fallback). a2n-api `source-watch.yml` is a
disabled workflow.

Smoke: `/enqueue jobs-marts` → `202` → run 41678398137 (`workflow_dispatch`, `source=jobs-queue`)
completed `success`; unknown job `404`, no bearer `401`; `/healthz` `200` with `dlq_backlog: 0`.

`GITHUB_DISPATCH_TOKEN` is the existing all-repo fine-grained token (`REGISTRY_EXPORT_GITHUB_TOKEN`).
Owner item: replace it with a fine-grained token limited to **Actions: write** on the job repos.

Rollback: to a version older than the `jobs-dlq` consumer, first `npx wrangler queues consumer remove jobs-dlq solo-empire-jobs` (keeps the queue and its messages; the old code would otherwise run parked jobs again), then `npx wrangler rollback`, then restore the `schedule:` blocks named in the table.

Known gap: a wide outage that parks every scheduled job (~540 a day) can still exhaust the Workers Free Queues quota (10,000 operations a day, shared with `jobs`): the `jobs` retries alone cost about as much. The DLQ rechecks back off to 6 h to keep their share small.
