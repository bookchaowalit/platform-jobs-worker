// Job registry (ADR-0008 queue plane). Only jobs listed here can be enqueued, so /enqueue can
// never be pointed at an arbitrary URL. `cron` uses the same string as wrangler.toml [triggers].

export type JobTarget =
  | { kind: 'github_workflow'; repo: string; workflow: string; ref: string }
  | { kind: 'http'; url: string; tokenSecret: 'GATEWAY_TOKEN' };

export interface JobDef {
  name: string;
  cron?: string;
  target: JobTarget;
}

export const JOBS: readonly JobDef[] = [
  // Jobs Bronze → Silver → Gold. GitHub's own cron starts hours late; this tick is punctual and
  // the workflow is idempotent, so a duplicate run from the old schedule writes nothing.
  {
    name: 'jobs-marts',
    cron: '47 */2 * * *',
    target: { kind: 'github_workflow', repo: 'bookchaowalit/data-lake-pipelines', workflow: 'jobs-marts.yml', ref: 'main' },
  },
];

export function findJob(name: string): JobDef | undefined {
  return JOBS.find((job) => job.name === name);
}

export function jobsForCron(cron: string): JobDef[] {
  return JOBS.filter((job) => job.cron === cron);
}
