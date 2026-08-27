# FyFlow Scheduler — Reference for AI Agents

Complete usage reference for `fyflow-scheduler`. Everything here is exercised by
`tests/suites/docs.ts`, which runs on every `npm test` — if a snippet stops
working, the build fails.

**Package**: `fyflow-scheduler` (npm) / `@fyflow/scheduler` (JSR)
**Runtimes**: Deno, Node.js **≥ 22**, browser
**Dependencies**: none

---

## 1. What this library is

An in-memory scheduler that runs independent tasks in parallel across pools of
workers, with resource limits.

**Tasks are independent.** There is no dependency graph, no `dependsOn`, no
topological ordering. Every task runs as soon as (a) its worker pool has a free
slot and (b) every resource group it belongs to has capacity. To express "B after
A", either await A and then add B, or have A spawn B (§7).

### Things that do NOT exist — do not generate code using them

| Not available | Use instead |
|---|---|
| Task dependencies (`dependsOn`, `parents`, `children`) | Await a task, or spawn from inside a worker (§7) |
| `StrictLimitGroup`, token/semaphore acquisition | `ConcurrentLimitGroup` (soft) or `RateLimitGroup` |
| `DagScheduler`, `DagTask` | `FyflowScheduler`, `FyflowTask` |
| Priorities, cron/scheduled tasks, persistence | Not implemented |
| Cancelling a queued task | Not implemented |

Both group types are **optimistic**: a limit can be briefly exceeded by up to
`maxThreads × maxConcurrentTasks` under race conditions. Nothing here guarantees
a hard cap.

---

## 2. Minimal working program

```typescript
import { FyflowScheduler, FyflowTask, WorkerManager } from 'fyflow-scheduler';

// See §3 - this line differs per runtime
const workerUrl = new URL('./myWorker.js', import.meta.url).href;

const scheduler = new FyflowScheduler({
  MyWorker: new WorkerManager(workerUrl, { maxThreads: 4, inline: true })
});

const result = await scheduler.addTask(
  new FyflowTask({ id: 'task-1', workerType: 'MyWorker', payload: { value: 21 } }),
  { createPromise: true }          // REQUIRED to get a promise back
);

console.log(result);
await scheduler.shutdown();        // REQUIRED or the process stays alive
```

Two things bite everyone: `addTask` returns `undefined` without
`{ createPromise: true }`, and a scheduler that is never shut down keeps live
workers and a retry timer running.

---

## 3. Worker URLs — the most common mistake

Worker scripts are loaded by URL, and **resolution differs per runtime**. Getting
this wrong is the single most likely failure when writing code against this
library.

```typescript
let workerUrl: string;
if (typeof Deno !== "undefined") {
  // Deno loads the TypeScript source directly
  workerUrl = new URL("./workers/myWorker.ts", import.meta.url).href;
} else {
  // Node and browser go through the esbuild worker plugin, which rewrites
  // `?worker-direct` at build time into a bundled worker file
  // @ts-expect-error - resolved by the build, not by TypeScript
  workerUrl = new URL((await import("./workers/myWorker.ts?worker-direct")).default).href;
}
```

- The `?worker-direct` suffix is an **esbuild convention from this repo's
  `esbuild.config.js`**, not a standard. It only works in a build that includes
  that plugin.
- On Node/browser the code must be built (`npm run build` / `npm run build:dev`)
  before it runs. Deno needs no build step.
- The worker file must `export default` a class. Anything else fails with
  `Worker script <url> must export a default class`.

---

## 4. Writing a worker

```typescript
import { BaseWorker, WorkerConfig, BaseWorkerContext, TaskWorkerContext }
  from 'fyflow-scheduler';

export default class MyWorker extends BaseWorker {
  private multiplier: number;

  // The pool calls `new MyWorker(config, workerContext)`.
  // Forward BOTH arguments - dropping the second leaves `this.workerContext`
  // undefined and terminateWithError() silently does nothing.
  constructor(config: WorkerConfig = {}, workerContext?: BaseWorkerContext) {
    super(config, workerContext);
    this.multiplier = (config as any).multiplier ?? 1;
  }

  // setup and teardown are ABSTRACT on BaseWorker - both are required, even if
  // empty. Omitting them is a compile error.
  async setup(): Promise<void> {}
  async teardown(): Promise<void> {}

  async run(payload: any, context?: TaskWorkerContext): Promise<any> {
    context?.sendProgress(0.5, 'halfway');       // 0-1, NOT a percentage
    return { value: payload.value * this.multiplier };
  }
}
```

- `config` comes from the pool's `config` option and is shared by every worker
  instance in that pool.
- One instance is created per worker, lazily, and reused across tasks. Instance
  state persists between tasks — do not assume a fresh object per task.
- Throwing from `run()` fails that task. Throwing from `setup()` fails the
  worker.

---

## 5. API reference

### `new FyflowScheduler(workerPools, resourceGroups?, options?)`

| Argument | Type | Notes |
|---|---|---|
| `workerPools` | `Record<string, WorkerManager>` | Keys are the `workerType` values tasks refer to |
| `resourceGroups` | `Record<string, ResourceGroup>` | Keys are the group ids used in `groups` / `workerGroups` |
| `options.maxCompletedTasks` | `number` | Cap on retained terminal tasks. Default: unlimited |
| `options.periodicRetryIntervalMs` | `number` | Retry interval for blocked tasks. Default `50` |

| Member | Signature | Notes |
|---|---|---|
| `addTask` | `(task, opts?) => Promise<any> \| void` | Returns a promise only with `{ createPromise: true }`. **Throws** on unknown `workerType` |
| `addTasks` | `(tasks, opts?) => Promise<any>[] \| void` | Batched dispatch for bulk adds. **Throws** on unknown `workerType`, validating the whole batch before queueing any of it |
| `stats` | `{ queued, running, done, failed }` | Counts every task, including evicted ones |
| `tasks` | `Map<string, FyflowTask>` | Live and completed tasks (see `maxCompletedTasks`) |
| `getResourceMetrics()` | `Record<string, { limit, running, available, utilization }>` | `utilization` is 0–1 |
| `getResourceStats()` | `Record<string, { totalAcquired, totalReleased }>` | Lifetime counters |
| `shutdown()` | `Promise<void>` | Always call when finished |

### `new FyflowTask(config)`

| Field | Type | Default | Notes |
|---|---|---|---|
| `id` | `string` | — | Must be unique; reusing an id overwrites |
| `workerType` | `string` | — | Key into `workerPools` |
| `payload` | `any` | — | Passed verbatim to `run()` |
| `optional` | `boolean` | `false` | If it fails, resolves `null` instead of rejecting |
| `retryPolicy` | `{ maxRetries, backoffMs }` | none | |
| `workerGroups` | `string[]` | `[]` | **Added to** the pool's groups, not a replacement |
| `handleRejection` | `boolean` | `true` | Silences unhandled rejections for fire-and-forget |

Readable after the run: `state`, `result`, `error`, `attempts`, `startTime`,
`endTime`, `executionTime` (worker-measured ms, excludes queue wait).

States: `pending` → `running` → `done` | `failed`. A **non-optional** task that
fails with no retries left ends in **`user_action`**, not `failed`.

Methods: `onCompletePromise()` — this task only; `onCompleteDescendants()` —
this task plus everything it spawns (§7).

### `new WorkerManager(scriptUrl, options)`

| Option | Default | Notes |
|---|---|---|
| `maxThreads` | `2` | Worker instances in the pool |
| `maxConcurrentTasks` | `1` | Tasks per worker. Pool capacity = `maxThreads × maxConcurrentTasks` |
| `inline` | `false` | `true` runs in the main thread — right for async/IO, wrong for CPU-bound |
| `config` | `{}` | First constructor argument for every worker instance |
| `groups` | `[]` | Group ids every task in this pool must acquire |
| `idleTimeout` | `5000` | Ms before an idle worker is terminated. `0` = never |
| `requeueFailedTasks` | `true` | Requeue in-flight tasks when a worker dies |
| `maxWorkerRestarts` | `3` | Then `worker.restart_limit_exceeded` |

Management: `getWorkerIds()`, `getWorkerStatus(id)`, `getAllWorkerStatuses()`,
`restartWorker(id, newConfig?)`, `replaceWorker(...)` (alias),
`updateWorkerConfig(id, config)`, `shutdown()`.

`WorkerStatus` = `{ id, state, tasksCompleted, errorCount, uptime, currentTasks,
resourcesHeld, lastError? }`, where `state` is
`initializing | healthy | busy | failed | terminated`.

### Resource groups

```typescript
new ConcurrentLimitGroup(limit: number, id?: string)
new RateLimitGroup(windows: { limit: number, windowMs: number }[], id?: string)
```

Both expose `canRun()`, `getMetrics()`, `getStats()`. Tasks over a limit wait in
a blocked queue and are retried — never dropped. Multiple rate-limit windows are
enforced together.

---

## 6. Events

Attach with `addEventListener(name, e => ...)`; payload is in `e.detail`.

**On the scheduler:**

| Event | `e.detail` |
|---|---|
| `task.running` | the `FyflowTask` |
| `task.completed` | the `FyflowTask` (with `result`, `executionTime`) |
| `task.failed` | the task, plus `error` |
| `task.progress` | `{ taskId, workerId, progress (0–1), message, details }` |
| `task.user_action` | the task — failed, out of retries, not optional |
| `task.spawn_request` | `{ parentTask, spawnConfig, workerId, workerType }` |
| `task.spawn_failed` | `{ parentTask, spawnConfig, error }` |
| `scheduler.completed` | the `stats` object |

**On a `WorkerManager`:** `task.started`, `task.completed`, `task.failed`,
`task.progress`, `task.spawn_request`, `task.requeue_required`, `worker.failed`,
`worker.self_terminated`, `worker.restart_limit_exceeded`.

**Worker lifecycle, on a `WorkerManager`:**

| Event | `e.detail` |
|---|---|
| `worker.initialization.started` | `{ workerId, workerType, timestamp }` |
| `worker.initialization.completed` | `{ workerId, workerType, timestamp, duration }` |
| `worker.initialization.failed` | `{ workerId, workerType, timestamp, error }` |
| `worker.setup.started` | `{ workerId, workerType, timestamp }` |
| `worker.setup.completed` | `{ workerId, workerType, timestamp, duration }` |
| `worker.teardown.started` | `{ workerId, workerType, timestamp }` |
| `worker.teardown.completed` | `{ workerId, workerType, timestamp, duration }` |
| `worker.teardown.failed` | `{ workerId, workerType, timestamp, error }` |

`workerType` is `'inline'` or `'thread'`. Inline and threaded pools emit the same
set with the same shape. These fire on every worker creation and every
idle-timeout teardown, so keep their listeners cheap.

> `scheduler.completed` fires **every time the scheduler drains**, so it can fire
> more than once when tasks arrive in waves. It does account for tasks blocked on
> a resource group, so it will not fire while work is still waiting for capacity.

---

## 7. Spawning and workflow completion

A worker can create tasks while running:

```typescript
async run(payload, context) {
  context?.spawnTask({
    id: `${payload.id}-child`,
    workerType: 'MyWorker',        // must be a registered pool
    payload: { value: 1 },
    workerGroups: ['cpu']          // optional
  });
  return { ok: true };
}
```

To wait for a task *and everything it spawned*, transitively:

```typescript
scheduler.addTask(task);              // must be added FIRST
await task.onCompleteDescendants();   // rejects if called before addTask
```

- Resolves with the tracked task's own result once every descendant is terminal.
- A **descendant** failing does **not** reject — watch `task.failed` for those.
- The **tracked task** failing **does** reject, matching `onCompletePromise()`.
- Safe to call more than once, and after the workflow already finished.
- Rejects if the scheduler shuts down first.

This is lineage, not a dependency: spawning never changes dispatch order.

---

## 8. Common mistakes

| Symptom | Cause |
|---|---|
| `await scheduler.addTask(t)` resolves `undefined` | Missing `{ createPromise: true }` |
| Process never exits | `shutdown()` not called |
| `Unknown worker type: X` | `workerType` is not a key of `workerPools` |
| `Worker script <url> must export a default class` | Worker has no `export default class`, or the URL is wrong for the runtime (§3) |
| Worker fails to load only on Node | Missing `?worker-direct`, or the build was not run |
| `Task must be added to a scheduler before tracking descendants` | `onCompleteDescendants()` called before `addTask` |
| `terminateWithError` does nothing | Worker constructor did not forward `workerContext` to `super` |
| Progress bar shows 0–1 instead of 0–100 | `progress` is a fraction; multiply by 100 yourself |
| Compile error extending `BaseWorker` | `setup()` / `teardown()` not implemented — both are abstract |
| Task ends in `user_action`, not `failed` | Non-optional task, retries exhausted |
| Memory grows in a long-running process | Completed tasks are retained; set `maxCompletedTasks` |
| Limit briefly exceeded | Groups are optimistic by design (§1) |

---

## 9. Recipes

**Bulk work, waiting for all of it**

```typescript
const tasks = items.map((item, i) => new FyflowTask({
  id: `job-${i}`, workerType: 'MyWorker', payload: item
}));
const results = await Promise.all(
  scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]
);
```

**Rate-limited API calls**

```typescript
const api = new RateLimitGroup([{ limit: 10, windowMs: 1000 }], 'api');
const scheduler = new FyflowScheduler(
  { ApiWorker: new WorkerManager(url, { maxConcurrentTasks: 8, inline: true, groups: ['api'] }) },
  { api }
);
```

**Retries and optional work**

```typescript
new FyflowTask({
  id: 'flaky', workerType: 'MyWorker', payload: {},
  retryPolicy: { maxRetries: 3, backoffMs: 250 },
  optional: true          // resolves null instead of rejecting
});
```

**Monitoring**

```typescript
setInterval(() => {
  const m = scheduler.getResourceMetrics();
  console.log(scheduler.stats, `cpu ${m.cpu.running}/${m.cpu.limit}`);
}, 1000);
```

**Measuring worker startup cost**

```typescript
pool.addEventListener('worker.setup.completed', (e) => {
  if (e.detail.duration > 500) {
    console.warn(`slow setup: ${e.detail.workerId} took ${e.detail.duration}ms`);
  }
});
pool.addEventListener('worker.initialization.failed', (e) => {
  console.error(`worker could not start: ${e.detail.error.message}`);
});
```

**Supervising workers**

```typescript
pool.addEventListener('worker.failed', async (e) => {
  const status = pool.getWorkerStatus(e.detail.workerId);
  if (status && status.errorCount > 5) {
    await pool.restartWorker(e.detail.workerId, { resetState: true });
  }
});
```

**Long-running scheduler**

```typescript
const scheduler = new FyflowScheduler(pools, groups, {
  maxCompletedTasks: 10_000   // otherwise completed tasks are retained forever
});
```

---

## 10. Choosing worker settings

| Workload | Settings |
|---|---|
| Async / IO (HTTP, DB) | `inline: true`, `maxConcurrentTasks: 8–50`, `maxThreads: 1–2` |
| CPU-bound | `inline: false`, `maxConcurrentTasks: 1`, `maxThreads ≈ core count` |
| Mixed | Separate pools per workload; do not mix in one pool |

Inline workers share the main thread — a CPU-bound inline worker blocks the
scheduler itself.
