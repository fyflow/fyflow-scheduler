// Admission benchmark - R0 of doc/design/admission-rework.md.
//
// The main suite measures throughput. That misses the admission rework's
// problems, which are about *work per task* growing with how many tasks wait,
// and about wake-up latency. So this measures those directly:
//
//   canRun/task      admission checks per task. Flat means O(1) per task;
//                    doubling with the queue depth means O(N^2) per run.
//   blocked/task     resource.blocked events per task: what a downstream fold
//                    has to process.
//   stall            the longest the main thread went unresponsive, sampled
//                    after the tasks are added, so enqueueing is excluded.
//   lists            blocked queues left at the end (total / non-empty). Any
//                    total above 0 is a queue nobody deleted.
//   seen/waiting     the most waiters getAdmissionQueue() reported, against the
//                    most tasks actually waiting. 1.0 is an honest queue.
//   heap             the peak heap growth over the run, sampled.
//
// Latency shapes report how long a waiter takes to start once its capacity
// is free.
//
// Run:      deno task benchmark:admission [--sizes 1000,10000] [--shapes global,keyed]
//                                         [--budget 20] [--json out.json]
//           deno task benchmark:admission --compare base.json head.json
//
// --budget (seconds, default 20) skips the next size of a shape whose
// projected time, assuming quadratic growth, is over budget. That projection
// is itself the finding: the skipped row says how long it would have taken.
//
// Like every benchmark here, absolute numbers mean little. Compare two
// commits run back to back on one machine (TODO_CHECKLIST.md). The ratios -
// canRun/task across sizes - are the signal, and they are machine-independent.

import { FyflowScheduler, FyflowTask, WorkerManager, ConcurrentLimitGroup, KeyedRateLimitGroup, KeyedConcurrentLimitGroup, UnitGroup } from '../index.ts';
import { KeyedCounterGroup, instrument } from '../tests/fixtures/keyedCounterGroup.ts';
import { RuntimeUtils } from './perfUtils.ts';

declare const process: any;

let workerUrl: string;
if (typeof Deno !== 'undefined') {
  workerUrl = new URL('./noopWorker.ts', import.meta.url).href;
} else {
  // @ts-expect-error - esbuild resolves ?worker-direct at build time
  workerUrl = new URL((await import('./noopWorker.ts?worker-direct')).default).href;
}

interface CostRow {
  shape: string;
  N: number;
  ms: number;
  tasksPerSec: number;
  canRunPerTask: number;
  blockedPerTask: number;
  unblockedPerTask: number;
  maxStallMs: number;
  listsLeft: number;
  listsNonEmpty: number;
  admissionSeen: number;
  waitingSeen: number;
  peakHeapMB: number;
  /** Rate-paced shapes only: wall time over the time the limits require. */
  overIdeal?: number;
  skipped?: string;
}

interface LatencyRow {
  shape: string;
  samples: number;
  meanMs: number;
  maxMs: number;
  /** A waiter that never started within the bound counts here instead. */
  missed: number;
}

const COST_SHAPES = ['none', 'global', 'keyed', 'keyed-wide', 'two-group', 'keyed-rate', 'keyed-multi', 'units'] as const;
const LATENCY_SHAPES = ['two-group-wake', 'resident-handoff'] as const;
type CostShape = typeof COST_SHAPES[number];

function heapUsed(): number {
  if (typeof Deno !== 'undefined') return Deno.memoryUsage().heapUsed;
  if (typeof process !== 'undefined') return process.memoryUsage().heapUsed;
  return 0;
}

function pool(extra: Record<string, unknown> = {}) {
  return new WorkerManager(workerUrl, { maxThreads: 4, maxConcurrentTasks: 50, inline: true, ...extra });
}

async function measureCost(shape: CostShape, N: number): Promise<CostRow> {
  const groups: Record<string, any> = {};
  let configFor: (i: number) => Record<string, unknown> = () => ({});
  // Keys carried in the payload, for groups that read them there
  let keysFor: ((i: number) => string | string[]) | undefined;
  let idealMs = 0;
  switch (shape) {
    case 'none':
      break;
    case 'global':
      groups.g = new ConcurrentLimitGroup(7, 'g');
      configFor = () => ({ workerGroups: ['g'] });
      break;
    case 'keyed':
      groups.k = new KeyedCounterGroup(2, 'k');
      configFor = i => ({ workerGroups: ['k'], limitKey: `key${i % 10}` });
      break;
    case 'keyed-wide':
      groups.k = new KeyedCounterGroup(1, 'k');
      // Four adjacent tasks per key, so each key blocks, then empties
      configFor = i => ({ workerGroups: ['k'], limitKey: `key${Math.floor(i / 4) % 1000}` });
      break;
    case 'two-group':
      groups.A = new ConcurrentLimitGroup(4, 'A');
      groups.B = new ConcurrentLimitGroup(4, 'B');
      configFor = i => ({ workerGroups: i % 3 === 0 ? ['A', 'B'] : i % 3 === 1 ? ['A'] : ['B'] });
      break;
    case 'keyed-multi':
      // Keyed phase B: 20% of tasks name two keys, held together. Its gate is
      // canRun/task and events/task flat across sizes
      groups.k = new KeyedConcurrentLimitGroup(2, { id: 'k', keyFrom: (t: any) => t.payload.keys });
      keysFor = i => (i % 5 === 0 ? [`key${i % 10}`, `key${(i + 3) % 10}`] : `key${i % 10}`);
      break;
    case 'units':
      // Unit grants phase 2: 8 named units, 30% of tasks pinned to one of
      // them - eight wait sets and "any". Its gate is the same: canRun/task and
      // events/task flat across sizes
      groups.u = new UnitGroup(8, { id: 'u' });
      configFor = i => ({ workerGroups: ['u'], ...(i % 10 < 3 ? { units: { u: { from: [i % 8] } } } : {}) });
      break;
    case 'keyed-rate': {
      const [limit, windowMs, keys] = [10, 20, 10];
      groups.r = new KeyedRateLimitGroup([{ limit, windowMs }], { id: 'r' });
      configFor = i => ({ workerGroups: ['r'], limitKey: `key${i % keys}` });
      idealMs = Math.ceil(N / keys / limit) * windowMs;
      break;
    }
  }
  const counters = Object.values(groups).map(g => instrument(g));
  const s = new FyflowScheduler({ W: pool() }, groups);
  let blocked = 0, unblocked = 0;
  s.addEventListener('resource.blocked', () => blocked++);
  s.addEventListener('resource.unblocked', () => unblocked++);

  const heap0 = heapUsed();
  let peakHeap = heap0;
  const start = performance.now();
  const promises = [];
  for (let i = 0; i < N; i++) {
    const payload: Record<string, unknown> = { taskId: i, yieldMacrotask: true };
    if (keysFor) payload.keys = keysFor(i);
    promises.push(s.addTask(new FyflowTask({ id: `t${i}`, workerType: 'W', payload, ...(keysFor ? { workerGroups: ['k'] } : configFor(i)) }), { createPromise: true }));
  }

  // Samplers start once the tasks are in, so enqueueing is not counted as a stall
  let maxStall = 0, last = performance.now();
  let admissionSeen = 0, waitingSeen = 0, ticks = 0;
  const sampler = setInterval(() => {
    const now = performance.now();
    maxStall = Math.max(maxStall, now - last - 5);
    last = now;
    peakHeap = Math.max(peakHeap, heapUsed());
    // The admission queue is O(waiters) to read, so read it rarely
    if (++ticks % 10 === 0 && Object.keys(groups).length > 0) {
      const queue = s.getAdmissionQueue();
      const listed = Object.values(queue).reduce((sum, q) => sum + q.length, 0);
      admissionSeen = Math.max(admissionSeen, listed);
      waitingSeen = Math.max(waitingSeen, N - s.stats.done - s.stats.running);
    }
  }, 5);
  await Promise.all(promises);
  clearInterval(sampler);
  const end = performance.now();
  // The gap since the last tick is a stall too, if the run ended inside one
  maxStall = Math.max(maxStall, end - last - 5);
  const ms = end - start;

  const queues: Map<string, { length: number }> = (s as any).blockedQueues;
  const listsLeft = queues.size;
  const listsNonEmpty = [...queues.values()].filter(q => q.length > 0).length;
  await s.shutdown();

  const canRun = counters.reduce((sum, c) => sum + c.canRun, 0);
  return {
    shape, N,
    ms: Math.round(ms),
    tasksPerSec: Math.round(N / ms * 1000),
    canRunPerTask: +(canRun / N).toFixed(1),
    blockedPerTask: +(blocked / N).toFixed(2),
    unblockedPerTask: +(unblocked / N).toFixed(2),
    maxStallMs: Math.round(maxStall),
    listsLeft, listsNonEmpty,
    admissionSeen, waitingSeen,
    peakHeapMB: +((peakHeap - heap0) / 1024 / 1024).toFixed(1),
    ...(idealMs ? { overIdeal: +(ms / idealMs).toFixed(2) } : {})
  };
}

const NEVER_MS = 1_000;

function startTime(s: FyflowScheduler, id: string): Promise<number> {
  return new Promise(resolve => {
    const listener = (e: any) => {
      if (e.detail?.id !== id) return;
      s.removeEventListener('task.running', listener);
      resolve(performance.now());
    };
    s.addEventListener('task.running', listener);
  });
}

function within(p: Promise<number>, ms: number): Promise<number | undefined> {
  return Promise.race([p, new Promise<undefined>(r => setTimeout(() => r(undefined), ms))]);
}

/**
 * A task needing A and B waits in A's queue, A frees while B is still held,
 * then B frees. The latency is from B's holder completing to the waiter
 * starting. With the default heartbeat, today's answer is "up to 50 ms".
 */
async function twoGroupWake(samples: number): Promise<LatencyRow> {
  const latencies: number[] = [];
  let missed = 0;
  for (let n = 0; n < samples; n++) {
    const A = new ConcurrentLimitGroup(1, 'A');
    const B = new ConcurrentLimitGroup(1, 'B');
    const s = new FyflowScheduler({ W: pool() }, { A, B });
    const freed = new Promise<number>(resolve => s.addEventListener('task.completed', (e: any) => {
      if (e.detail?.id === 'holdB') resolve(performance.now());
    }));
    const started = startTime(s, 'both');
    const task = (id: string, workerGroups: string[], delay: number) =>
      s.addTask(new FyflowTask({ id, workerType: 'W', workerGroups, payload: { delay } }), { createPromise: true });
    const done = Promise.all([task('holdA', ['A'], 20), task('holdB', ['B'], 40), task('both', ['A', 'B'], 1)]);
    const at = await within(started, NEVER_MS);
    if (at === undefined) missed++;
    else latencies.push(at - await freed);
    await done;
    await s.shutdown();
  }
  return latencyRow('two-group-wake', latencies, missed);
}

/**
 * Two pools sharing a limit-1 resident group take turns: each round, P's
 * worker must be torn down before Q's can exist. The latency is from P's
 * resident release to Q's task starting.
 */
async function residentHandoff(samples: number): Promise<LatencyRow> {
  const latencies: number[] = [];
  let missed = 0;
  for (let n = 0; n < samples; n++) {
    const gpu = new ConcurrentLimitGroup(1, 'gpu');
    const resident = () => pool({ maxThreads: 1, maxConcurrentTasks: 1, residentGroups: { gpu: 1 }, idleTimeout: 20, idleCheckIntervalMs: 5 });
    const s = new FyflowScheduler({ P: resident(), Q: resident() }, { gpu });
    const freed = new Promise<number>(resolve => s.addEventListener('resource.released', (e: any) => {
      if (e.detail?.lifetime === 'resident' && e.detail?.workerType === 'P') resolve(performance.now());
    }));
    const started = startTime(s, 'q');
    const done = Promise.all([
      s.addTask(new FyflowTask({ id: 'p', workerType: 'P', payload: { delay: 5 } }), { createPromise: true }),
      s.addTask(new FyflowTask({ id: 'q', workerType: 'Q', payload: { delay: 1 } }), { createPromise: true })
    ]);
    const at = await within(started, NEVER_MS);
    if (at === undefined) missed++;
    else latencies.push(at - await freed);
    await done;
    await s.shutdown();
  }
  return latencyRow('resident-handoff', latencies, missed);
}

function latencyRow(shape: string, latencies: number[], missed: number): LatencyRow {
  const mean = latencies.length ? latencies.reduce((a, b) => a + b, 0) / latencies.length : 0;
  return {
    shape, samples: latencies.length + missed,
    meanMs: +mean.toFixed(1),
    maxMs: +(latencies.length ? Math.max(...latencies) : 0).toFixed(1),
    missed
  };
}

// --- CLI -------------------------------------------------------------------

function parse(args: string[]) {
  const options = {
    sizes: [1_000, 10_000, 50_000],
    shapes: [...COST_SHAPES, ...LATENCY_SHAPES] as string[],
    budgetS: 20,
    samples: 20,
    json: undefined as string | undefined,
    compare: undefined as [string, string] | undefined,
  };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === '--sizes') options.sizes = args[++i].split(',').map(Number);
    else if (a === '--shapes') options.shapes = args[++i].split(',');
    else if (a === '--budget') options.budgetS = Number(args[++i]);
    else if (a === '--samples') options.samples = Number(args[++i]);
    else if (a === '--json') options.json = args[++i];
    else if (a === '--compare') options.compare = [args[++i], args[++i]];
    else throw new Error(`Unknown option ${a}`);
  }
  const known = new Set<string>([...COST_SHAPES, ...LATENCY_SHAPES]);
  const unknown = options.shapes.filter(s => !known.has(s));
  if (unknown.length) throw new Error(`Unknown shape(s) ${unknown.join(', ')}; known: ${[...known].join(', ')}`);
  return options;
}

function table(rows: Record<string, unknown>[]): string {
  if (rows.length === 0) return '';
  const cols = [...new Set(rows.flatMap(r => Object.keys(r)))];
  const cell = (v: unknown) => v === undefined ? '' : String(v);
  return [
    `| ${cols.join(' | ')} |`,
    `|${cols.map(() => '---').join('|')}|`,
    ...rows.map(r => `| ${cols.map(c => cell(r[c])).join(' | ')} |`)
  ].join('\n');
}

/** Head against base, per (shape, N), on the metrics the rework is meant to move. */
function compare(base: { cost: CostRow[]; latency: LatencyRow[] }, head: typeof base): string {
  const pct = (a: number, b: number) => a ? `${b >= a ? '+' : ''}${(((b - a) / a) * 100).toFixed(0)}%` : 'n/a';
  const cost = head.cost.flatMap(h => {
    const b = base.cost.find(r => r.shape === h.shape && r.N === h.N);
    if (!b || b.skipped || h.skipped) return [];
    return [{
      shape: h.shape, N: h.N,
      'canRun/task': `${b.canRunPerTask} -> ${h.canRunPerTask}`,
      'blocked/task': `${b.blockedPerTask} -> ${h.blockedPerTask}`,
      'tasks/s': `${b.tasksPerSec} -> ${h.tasksPerSec} (${pct(b.tasksPerSec, h.tasksPerSec)})`,
      'stall ms': `${b.maxStallMs} -> ${h.maxStallMs}`,
      'heap MB': `${b.peakHeapMB} -> ${h.peakHeapMB}`,
    }];
  });
  const latency = head.latency.flatMap(h => {
    const b = base.latency.find(r => r.shape === h.shape);
    return b ? [{ shape: h.shape, 'mean ms': `${b.meanMs} -> ${h.meanMs}`, 'max ms': `${b.maxMs} -> ${h.maxMs}`, missed: `${b.missed} -> ${h.missed}` }] : [];
  });
  return `${table(cost)}\n\n${table(latency)}`;
}

async function main() {
  const options = parse(RuntimeUtils.args);

  if (options.compare) {
    const [base, head] = await Promise.all(options.compare.map(async p => JSON.parse(await RuntimeUtils.readTextFile(p))));
    console.log(compare(base, head));
    return;
  }

  const cost: CostRow[] = [];
  const latency: LatencyRow[] = [];
  for (const shape of options.shapes) {
    if ((LATENCY_SHAPES as readonly string[]).includes(shape)) continue;
    // Rate-paced: the wall time is set by the limits, so keep it short
    const sizes = shape === 'keyed-rate' ? options.sizes.filter(n => n <= 2_000) : options.sizes;
    let tooSlow: string | undefined;
    for (const [index, N] of sizes.entries()) {
      if (tooSlow) {
        cost.push({ shape, N, skipped: tooSlow } as CostRow);
        console.log(`${shape} N=${N}: skipped - ${tooSlow}`);
        continue;
      }
      const row = await measureCost(shape as CostShape, N);
      cost.push(row);
      console.log(`${shape} N=${N}: ${row.ms}ms, canRun/task ${row.canRunPerTask}, blocked/task ${row.blockedPerTask}, stall ${row.maxStallMs}ms`);
      // Projected as if quadratic, which is the shape this exists to catch: at
      // 0adeefb the keyed shape took 16 s at 10K and 7.4 minutes at 50K
      const next = sizes[index + 1];
      const projectedMs = next ? row.ms * (next / N) ** 2 : 0;
      if (projectedMs > options.budgetS * 1000) {
        tooSlow = `N=${N} took ${(row.ms / 1000).toFixed(1)}s, so N=${next} projects past the ${options.budgetS}s budget (--budget to raise it)`;
      }
    }
  }
  if (options.shapes.includes('two-group-wake')) latency.push(await twoGroupWake(options.samples));
  if (options.shapes.includes('resident-handoff')) latency.push(await residentHandoff(options.samples));

  const runtime = typeof Deno !== 'undefined' ? `Deno ${Deno.version.deno}` : `Node ${process.version}`;
  console.log(`\n## Admission benchmark - ${runtime}\n`);
  console.log(table(cost.map(r => ({ ...r }))));
  console.log('');
  console.log(table(latency.map(r => ({ ...r }))));

  if (options.json) {
    await RuntimeUtils.writeTextFile(options.json, JSON.stringify({ runtime, at: new Date().toISOString(), cost, latency }, null, 2));
    console.log(`\nWritten to ${options.json}`);
  }
}

if (RuntimeUtils.isMainModule || (typeof process !== 'undefined' && process.argv[1]?.includes('admission'))) {
  await main();
}
