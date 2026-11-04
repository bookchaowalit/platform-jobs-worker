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
| `scheduled` | One every-minute trigger (Workers Free allows 5 cron triggers per account). Each minute, `src/cron.ts` matches every job's own UTC cron and enqueues the due jobs. Message id = `<job>:<slot ISO time>`. |
| `POST /enqueue` | Bearer `JOBS_TOKEN`; body `{"job":"<name>"}`. Only jobs in the registry are accepted (`404 unknown_job`), so it can never call an arbitrary URL. `202 {"queued":true,"id":...}`. |
| `queue` (`jobs`) | Runs the job target. `github_workflow`: `POST /repos/{repo}/actions/workflows/{wf}/dispatches` with the job's `inputs`, ack on `204`. `http`: POST the envelope with a bearer and `idempotency-key`, ack on 2xx. Otherwise retry after 60 s × attempt; after 5 retries → `jobs-dlq`. |
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
| `career-job-capture` | `17 */6 * * *` | career-job-scraper `scheduled-capture.yml` | 2026-11-03 |
| `org-hub-sync` | `0 2 * * *` | platform-ops-cli `org-hub-sync.yml` | 2026-11-03 |
| `data-lake-cloud-contract` | `30 2 * * 1` | data-lake-pipelines | 2026-11-03 |
| `free-cloud-e2e` | `17 3 * * 1` | data-lake-pipelines | 2026-11-03 |
| `polyglot-bench` | `0 2 * * 1` | devtools-polyglot-benchmark `bench.yml` | 2026-11-03 |
| `mcp-catalog-refresh` | `30 2 * * 1` | solo-empire | 2026-11-03 |
| `event-sink` | `5 * * * *` | data-lake-pipelines `event-sink.yml` (input `source=jobs-queue`) | 2026-11-04 |
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

Rollback: `npx wrangler rollback`, then restore the `schedule:` blocks named in the table.
