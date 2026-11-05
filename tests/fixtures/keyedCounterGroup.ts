// A keyed CONCURRENCY group, for measuring admission.
//
// The library's only keyed group limits a rate, so its waiters are woken by
// time as well as by releases, and a measurement of admission cost through it
// is paced by the 50 ms heartbeat. This one frees capacity only on release, so
// a run is CPU-bound and the cost of admission is all that is measured.
//
// Test and benchmark scaffolding, not a published group. Written before
// `KeyedConcurrentLimitGroup` shipped (0.7.0); replacing it with that group is
// a follow-up in TODO_CHECKLIST.md.

import type { ResourceGroup, ResourceGroupMetrics } from '../../groups/resourceGroup.ts';

export class KeyedCounterGroup extends EventTarget implements ResourceGroup {
  readonly id: string;
  readonly type = 'test-keyed-counter';
  readonly keyed = true;
  readonly limit: number;
  /** Units held per key. A key is deleted when it returns to 0. */
  readonly running = new Map<string, number>();

  constructor(limit: number, id = 'keyed') {
    super();
    this.limit = limit;
    this.id = id;
  }

  keyFor(task: unknown): string | undefined {
    const key = (task as { limitKey?: string }).limitKey;
    return key === undefined || key === '' ? undefined : key;
  }

  /** Without a key: whether any key could take one more, an unknown key counting as free. */
  canRun(key?: string, cost = 1): boolean {
    if (key === undefined) return true;
    return (this.running.get(key) ?? 0) + cost <= this.limit;
  }

  onStart(key?: string, cost = 1): void {
    const k = key ?? '';
    this.running.set(k, (this.running.get(k) ?? 0) + cost);
  }

  onFinish(key?: string, cost = 1): void {
    const k = key ?? '';
    const next = (this.running.get(k) ?? 0) - cost;
    if (next <= 0) this.running.delete(k);
    else this.running.set(k, next);
  }

  getMetrics(): ResourceGroupMetrics {
    let running = 0;
    for (const n of this.running.values()) running += n;
    return { limit: this.limit, running, available: Math.max(0, this.limit - running), utilization: running / this.limit };
  }
}

/**
 * Count `canRun` calls and track the peak held per key, by wrapping the group's
 * own methods. `canRun` calls are the admission work the scheduler does: a
 * waiter re-examined N times costs N calls.
 */
export function instrument(group: ResourceGroup): { canRun: number; peak: Map<string, number> } {
  const counts = { canRun: 0, peak: new Map<string, number>() };
  const held = new Map<string, number>();
  const g = group as any;

  const canRun = g.canRun.bind(g);
  g.canRun = (...args: any[]) => { counts.canRun++; return canRun(...args); };

  if (g.onStart) {
    const onStart = g.onStart.bind(g);
    // The rest - a request - passes through, and the grant comes back
    g.onStart = (key?: string, cost = 1, ...rest: any[]) => {
      const k = key ?? '';
      const now = (held.get(k) ?? 0) + cost;
      held.set(k, now);
      counts.peak.set(k, Math.max(counts.peak.get(k) ?? 0, now));
      return onStart(key, cost, ...rest);
    };
  }
  if (g.onFinish) {
    const onFinish = g.onFinish.bind(g);
    g.onFinish = (key?: string, cost = 1, ...rest: any[]) => {
      const k = key ?? '';
      held.set(k, (held.get(k) ?? 0) - cost);
      return onFinish(key, cost, ...rest);
    };
  }
  return counts;
}
