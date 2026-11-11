// Job registry (ADR-0008 queue plane). Only jobs listed here can be enqueued, so /enqueue can
// never be pointed at an arbitrary URL. `cron` is UTC, matched by src/cron.ts every minute.
// A job moved here from a workflow `schedule` has that schedule removed from the workflow.

import { cronMatches, parseCron } from './cron.js';

export type JobTarget =
  | { kind: 'github_workflow'; repo: string; workflow: string; ref: string; inputs?: Record<string, string> }
  | { kind: 'http'; url: string; tokenSecret: 'GATEWAY_TOKEN' };

export interface JobDef {
  name: string;
  cron?: string;
  target: JobTarget;
}

const gh = (repo: string, workflow: string, inputs?: Record<string, string>): JobTarget => ({
  kind: 'github_workflow',
  repo: `bookchaowalit/${repo}`,
  workflow,
  ref: 'main',
  ...(inputs ? { inputs } : {}),
});

export const JOBS: readonly JobDef[] = [
  // Jobs Bronze → Silver → Gold; `source` makes the run behave like a scheduled one (no forced BI deploy).
  { name: 'jobs-marts', cron: '47 */2 * * *', target: gh('data-lake-pipelines', 'jobs-marts.yml', { source: 'jobs-queue' }) },
  // Sales Bronze (commerce events) → Gold → revenue page (data-lake-pipelines docs/specs/sales-marts.md); 15 min after event-sink (:05).
  { name: 'sales-marts', cron: '20 * * * *', target: gh('data-lake-pipelines', 'sales-marts.yml', { source: 'jobs-queue' }) },
  // News Bronze → Silver → Gold → news-insights BI (docs/specs/news-marts.md); 20 min after news-capture (:15 every 2 h).
  { name: 'news-marts', cron: '35 */2 * * *', target: gh('data-lake-pipelines', 'news-marts.yml', { source: 'jobs-queue' }) },
  // Owner's public GitHub repos + events → Bronze (data-lake-pipelines docs/specs/github-activity.md); daily.
  { name: 'github-activity', cron: '10 3 * * *', target: gh('data-lake-pipelines', 'github-activity.yml') },
  // Track B job capture into Bronze (feeds jobs-marts 30 min later).
  { name: 'career-job-capture', cron: '17 */6 * * *', target: gh('career-job-scraper', 'scheduled-capture.yml') },
  { name: 'org-hub-sync', cron: '0 2 * * *', target: gh('platform-ops-cli', 'org-hub-sync.yml') },
  { name: 'data-lake-cloud-contract', cron: '30 2 * * 1', target: gh('data-lake-pipelines', 'data-lake-cloud-contract.yml') },
  { name: 'free-cloud-e2e', cron: '17 3 * * 1', target: gh('data-lake-pipelines', 'free-cloud-e2e.yml') },
  { name: 'polyglot-bench', cron: '0 2 * * 1', target: gh('devtools-polyglot-benchmark', 'bench.yml') },
  { name: 'mcp-catalog-refresh', cron: '30 2 * * 1', target: gh('solo-empire', 'mcp-catalog-refresh.yml') },
  // Intake event outbox → Kafka data.ingest.v1 (ADR-0008 M5, intake migration 0003); before event-sink at :05.
  { name: 'intake-relay', cron: '53 * * * *', target: gh('platform-intake-api', 'relay-outbox.yml', { source: 'jobs-queue' }) },
  // Knowledge store outbox → Kafka (ADR-0008 M5, SPEC-003); runs before event-sink at :05.
  // Finance outboxes → Kafka finance.events.v1 (finance-<cell>-api relay-outbox.yml, SPEC-005), one per cell at
  // :36–:45 (free minutes), before the lake sink at :05; heartbeats finance-relay-<cell> in the uptime Worker.
  ...['budgets', 'accounts', 'transactions', 'cash-flow', 'reconciliations', 'business-assets', 'investments', 'liabilities', 'subscriptions', 'tax']
    .map((cell, n): JobDef => ({ name: `finance-relay-${cell}`, cron: `${36 + n} * * * *`, target: gh(`finance-${cell}-api`, 'relay-outbox.yml', { source: 'jobs-queue' }) })),
  { name: 'knowledge-relay', cron: '55 * * * *', target: gh('platform-knowledge-api', 'relay-outbox.yml', { source: 'jobs-queue' }) },
  // Event plane → Bronze (ADR-0008 M4): heartbeat event, drain the Kafka topics, convert to Bronze.
  { name: 'event-sink', cron: '5 * * * *', target: gh('data-lake-pipelines', 'event-sink.yml', { source: 'jobs-queue' }) },
  // Kafka → Telegram for payments and failed intakes (ADR-0008 consumer #1, SPEC-001); after the relays at :53/:55.
  { name: 'event-notifier', cron: '58 * * * *', target: gh('platform-event-notifier', 'notify.yml', { source: 'jobs-queue' }) },
  // Dependabot alerts across own repos → Telegram on change (platform-vuln-digest SPEC-001); 08:17 Bangkok.
  { name: 'vuln-digest', cron: '17 1 * * *', target: gh('platform-vuln-digest', 'digest.yml') },
];

const PARSED = new Map(JOBS.filter((job) => job.cron).map((job) => [job.name, parseCron(job.cron as string)]));

export function findJob(name: string): JobDef | undefined {
  return JOBS.find((job) => job.name === name);
}

export function jobsDueAt(at: Date): JobDef[] {
  return JOBS.filter((job) => {
    const cron = PARSED.get(job.name);
    return cron ? cronMatches(cron, at) : false;
  });
}
