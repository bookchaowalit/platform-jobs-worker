// Minimal 5-field cron matcher (UTC), the dialect GitHub Actions and Cloudflare use:
// `*`, `n`, `a-b`, `*/n`, `a-b/n` and comma lists; day of week 0-7 (0 and 7 = Sunday).
// When both day of month and day of week are restricted, either one matching is enough.
// The Worker has one every-minute trigger (Workers Free allows 5 cron triggers per account),
// so each job's own schedule is matched here.

const RANGES: [number, number][] = [[0, 59], [0, 23], [1, 31], [1, 12], [0, 7]];

function parseField(field: string, [min, max]: [number, number]): Set<number> | null {
  const values = new Set<number>();
  for (const part of field.split(',')) {
    const match = /^(\*|\d+(?:-\d+)?)(?:\/(\d+))?$/.exec(part);
    if (!match) return null;
    const [, base, stepText] = match;
    const step = stepText === undefined ? 1 : Number(stepText);
    let lo = min;
    let hi = max;
    if (base !== '*') {
      const [a, b] = base.split('-').map(Number);
      lo = a;
      hi = b === undefined ? (stepText === undefined ? a : max) : b;
    }
    if (step < 1 || lo < min || hi > max || lo > hi) return null;
    for (let v = lo; v <= hi; v += step) values.add(v);
  }
  return values;
}

export interface Cron {
  fields: Set<number>[];
  domRestricted: boolean;
  dowRestricted: boolean;
}

export function parseCron(expression: string): Cron | null {
  const parts = expression.trim().split(/\s+/);
  if (parts.length !== 5) return null;
  const fields: Set<number>[] = [];
  for (let i = 0; i < 5; i++) {
    const set = parseField(parts[i], RANGES[i]);
    if (!set) return null;
    fields.push(set);
  }
  if (fields[4].has(7)) fields[4].add(0);
  return { fields, domRestricted: parts[2] !== '*', dowRestricted: parts[4] !== '*' };
}

export function cronMatches(cron: Cron, at: Date): boolean {
  const [minute, hour, dom, month, dow] = cron.fields;
  if (!minute.has(at.getUTCMinutes()) || !hour.has(at.getUTCHours()) || !month.has(at.getUTCMonth() + 1)) return false;
  const domOk = dom.has(at.getUTCDate());
  const dowOk = dow.has(at.getUTCDay());
  if (cron.domRestricted && cron.dowRestricted) return domOk || dowOk;
  return domOk && dowOk;
}
