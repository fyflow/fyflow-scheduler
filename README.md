# FyFlow Scheduler

Zero-dependency parallel task scheduler with resource management and cross-platform support.

[![npm version](https://badge.fury.io/js/fyflow-scheduler.svg)](https://badge.fury.io/js/fyflow-scheduler)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Using this library from an AI agent?** [AGENTS.md](AGENTS.md) is a complete
> single-file reference: exact signatures, defaults, events, gotchas and recipes.
> Every snippet in it is executed by the test suite.

## Overview

FyFlow is a self-contained parallel task scheduler with resource management. Fully in-memory operation with zero dependencies. Extensible APIs allow enhancement for persistence, distributed execution, or multi-machine coordination.

## Installation

**Node.js and browser** - npm, which ships prebuilt bundles with the worker
bootstrap inlined, so no bundler configuration is needed:

```bash
npm install fyflow-scheduler
# or
yarn add fyflow-scheduler
```

**Deno** - JSR, which ships the TypeScript sources and loads the worker directly:

```bash
deno add jsr:@fyflow/scheduler
```

The two packages are not interchangeable. **The JSR package is Deno-only**: it
contains no Node-specific files, and importing it from Node or a browser throws
an error pointing you at npm. Node and browser support lives entirely in the npm
package.

## Quick Start

```typescript
import { FyflowScheduler, FyflowTask, WorkerManager, ConcurrentLimitGroup } from 'fyflow-scheduler';

// A pool must DECLARE the groups it uses - registering a group on the scheduler
// alone does not constrain anything
const workerPool = new WorkerManager('./worker.js', { maxThreads: 2, groups: ['cpu'] });

const scheduler = new FyflowScheduler(
  { MyWorker: workerPool },
  { cpu: new ConcurrentLimitGroup(4) }
);

const tasks = [
  new FyflowTask({ id: 'task1', workerType: 'MyWorker', payload: { data: 'input' } }),
  new FyflowTask({ id: 'task2', workerType: 'MyWorker', payload: { data: 'processed' } })
];

// createPromise is required to get results back - addTask is fire-and-forget
const results = await Promise.all(scheduler.addTasks(tasks, { createPromise: true }));

await scheduler.shutdown();   // required, or live workers keep the process alive
```

See [Loading Workers](#loading-workers-differs-per-runtime) for how `./worker.js`
is resolved - it differs between Deno, Node and the browser.

## Key Features

- **Parallel Task Execution**: High-performance concurrent task processing
- **Resource Management**: Named concurrency and rate-limit groups applied per pool or per task
- **Resident Resources**: Groups held for a worker's lifetime, with per-worker costs - for a model on a GPU, a connection, a licence seat
- **Dynamic Task Spawning**: Workers can create new tasks at runtime
- **Cross-Platform**: Node.js, Browser, and Deno support
- **Worker Pools**: Thread-based and inline execution modes
- **Progress Reporting**: Real-time task progress and event monitoring
- **Zero Dependencies**: Self-contained with no external requirements
- **In-Memory**: Fast coordination with extensible persistence APIs

## Basic Usage

### Creating Workers

A worker script must `export default` a class.

```typescript
import { BaseWorker } from 'fyflow-scheduler';

export default class MyWorker extends BaseWorker {
  // The pool calls `new MyWorker(config, workerContext)` - forward both
  constructor(config = {}, workerContext) {
    super(config, workerContext);
  }

  // setup and teardown are abstract on BaseWorker: both required, may be empty
  async setup() {}
  async teardown() {}

  async run(payload, context) {
    context?.sendProgress(0.5, "Processing...");   // progress is 0-1

    // Spawn additional tasks if needed
    context?.spawnTask({
      id: 'child-task',
      workerType: 'MyWorker',
      payload: { data: 'child-data' }
    });

    return { result: 'completed', data: payload.data };
  }
}
```

### Loading Workers

Point the pool at your worker file's URL.

**npm (Node and browser)** - ship a compiled `.js` worker:

```javascript
const workerUrl = new URL('./myWorker.js', import.meta.url).href;
const pool = new WorkerManager(workerUrl, { maxThreads: 4 });
```

No bundler plugin and no build step for the worker itself. Node cannot import
TypeScript, so a `.ts` worker fails with `Unknown file extension`; compile it as
part of your own build. In the browser, your bundler needs to emit the worker as
a separate asset, which most do automatically for
`new URL('./myWorker.js', import.meta.url)`.

**Deno (JSR)** - point at the TypeScript source, which Deno loads directly:

```typescript
const workerUrl = new URL('./myWorker.ts', import.meta.url).href;
```

> The `?worker-direct` suffix in this repository's examples and tests is a
> convention of its own esbuild config, used so the `.ts` examples run on both
> runtimes. It is not part of the published API and will not resolve in your
> code.

### Getting Results Back

`addTask` and `addTasks` are fire-and-forget by default and return nothing. Ask
for a promise explicitly:

```typescript
const result = await scheduler.addTask(task, { createPromise: true });

const results = await Promise.all(
  scheduler.addTasks(tasks, { createPromise: true })
);

await scheduler.shutdown();   // required, or workers keep the process alive
```

### Parallel Task Execution

```typescript
const tasks = [
  new FyflowTask({ id: 'fetch', workerType: 'DataWorker', payload: { url: 'api/data' } }),
  new FyflowTask({ id: 'validate', workerType: 'ValidationWorker', payload: { url: 'api/data2' } }),
  new FyflowTask({ id: 'process', workerType: 'ProcessWorker', payload: { batch: 1 } }),
  new FyflowTask({ id: 'save', workerType: 'SaveWorker', payload: { batch: 2 } })
];

// All tasks execute in parallel (subject to resource constraints)
tasks.forEach(task => scheduler.addTask(task));
```

### Resource Groups

```typescript
const cpuGroup = new ConcurrentLimitGroup(8);  // Max 8 concurrent CPU tasks
const gpuGroup = new ConcurrentLimitGroup(2);  // Max 2 concurrent GPU tasks

const scheduler = new FyflowScheduler(workerPools, {
  cpu: cpuGroup,
  gpu: gpuGroup
});

// Workers automatically use groups specified in WorkerManager
const gpuWorkerPool = new WorkerManager('./gpu-worker.js', {
  maxThreads: 2,
  groups: ['gpu']  // This pool uses GPU constraints
});
```

### Resident Groups

`groups` is scoped to a **task** - taken at dispatch, released when the task
settles. That cannot bound a resource the **worker** holds simply by existing,
like a model loaded in `setup()`. The worker outlives its task by the idle
timeout, so a second pool can load a second model onto a full GPU even with a
`ConcurrentLimitGroup(1)` behaving perfectly correctly.

`residentGroups` is held from worker creation to teardown, with a cost **per
worker**:

```typescript
const vram = new ConcurrentLimitGroup(24, 'vram');   // 24GB of VRAM

// A 20GB model - only one such worker fits
const big = new WorkerManager('./llama-worker.js', {
  maxThreads: 1,
  residentGroups: { vram: 20 },
  idleTimeout: 2000            // how long it keeps the GPU after its last task
});

// 2GB models - several coexist, but not alongside the big one
const embed = new WorkerManager('./embed-worker.js', {
  maxThreads: 4,
  residentGroups: { vram: 2 }
});

// Array form is shorthand for a cost of 1 per worker
const one = new WorkerManager('./worker.js', { residentGroups: ['gpu'] });

const scheduler = new FyflowScheduler({ Big: big, Embed: embed }, { vram });
```

A worker is not created while its cost does not fit, and tasks needing one wait
until a holder is torn down. Release happens on the ordinary idle-timeout path,
so a pool with work still arriving keeps its worker and its loaded model -
avoiding a reload is usually worth more than the wait. Admission is
head-of-line, so an expensive pool is not starved by cheaper ones queued behind
it.

Costs must be positive integers no larger than the group's limit, and the group
must exist; all three are rejected at construction.

> **`idleTimeout: 0` never releases.** That is fine when nothing else needs the
> group, but under contention it is a permanent deadlock - the waiting task
> stays `pending` forever. `scheduler.completed` correctly does not fire, but
> `stats` shows `queued=0 running=0` because blocked tasks leave the queued
> count, so a stalled scheduler looks idle. `resource.blocked` names the
> contended group as the task is queued, and `getAdmissionQueue()` says what is
> waiting and for how many units. Give any pool sharing a contended resident
> group a non-zero `idleTimeout`.

See `examples/resident-groups.ts` for a runnable walkthrough.

### Event Monitoring

```typescript
scheduler.addEventListener('task.completed', (e) => {
  console.log(`Task ${e.detail.id} completed:`, e.detail.result);
});

scheduler.addEventListener('task.progress', (e) => {
  // The detail is the task's fields spread flat, so the id is `id` - there is
  // no `taskId`. (The pool's own task.progress does carry `taskId`.)
  console.log(`Task ${e.detail.id}: ${(e.detail.progress * 100).toFixed(0)}%`);
});

scheduler.addEventListener('scheduler.completed', (e) => {
  console.log(`All tasks completed. Stats:`, e.detail);
});
```

## API Reference

### Core Classes

**FyflowScheduler(workerPools, resourceGroups, options?)**
- `options.maxCompletedTasks`: cap how many terminal tasks stay in
  `scheduler.tasks` (default: unlimited). Set it for long-lived schedulers -
  completed tasks otherwise pin their payloads and results forever
- `options.periodicRetryIntervalMs`: heartbeat for waking blocked tasks
  (default 50ms). Groups that free a slot retry their own queue directly; this
  covers what raises no signal - rate-limit windows rolling over, and resident
  admission
- `addTask(task, options?)`: Add a task. Fire-and-forget by default; pass
  `{ createPromise: true }` to get a promise back
- `addTasks(tasks, options?)`: Batch version, optimised for bulk additions.
  Throws on an unknown `workerType`, validating the batch before queueing any of it
- `stats`: Current execution statistics
- `getResourceMetrics()` / `getResourceStats()`: Per-group utilisation and lifetime counters
- `describeResources()`: The gauges every group presents. Schema, not state -
  safe to read once and cache
- `getAdmissionQueue(groupId?)`: Everything currently waiting on a resource
  group, in admission order, with the cost each waiter needs - the head is what
  explains a stall
- `shutdown()`: Terminate worker pools and release all listeners
- `addEventListener(event, handler)`: Event monitoring

**FyflowTask(config)**
- `id`: Unique task identifier
- `workerType`: Worker pool to use
- `payload`: Data passed to worker
- `workerGroups`: Optional resource group names
- `optional`: Mark task as optional (failures don't block workflow)
- `limitKey`: Bucket for keyed resource groups (see `KeyedRateLimitGroup`)
- `retryPolicy`: `{ maxRetries, backoffMs }`
- `onCompletePromise()`: Promise for this task alone
- `onCompleteDescendants()`: Promise for this task plus everything it spawns

**WorkerManager(scriptUrl, options)**
- `maxThreads`: Maximum worker **instances** (not OS threads). With
  `inline: false` each is a real worker thread; with `inline: true` they are
  objects in the main process, so only `maxThreads × maxConcurrentTasks` matters
  for throughput — the split controls how much per-instance state (connections,
  caches) is duplicated
- `maxConcurrentTasks`: Tasks per instance
- `groups`: Resource group names, acquired per **task**
- `residentGroups`: Groups held for a **worker's** lifetime, `string[]` (cost 1
  each) or `Record<string, number>` for weighted costs. Per worker, not per pool
- `inline`: Use inline execution (default: false)
- `idleTimeout`: Ms before an idle worker is terminated (0 = never, default 5000)
- `idleCheckIntervalMs`: How often idle workers are swept (default 5000). This is
  the floor on how promptly a resident group is handed over

**ConcurrentLimitGroup(limit, id?)**
- Resource constraint with a concurrent execution limit. Optimistic: may briefly
  exceed the limit by up to `maxThreads × maxConcurrentTasks` under contention

**RateLimitGroup(windows, id?)**
- Time-window throttling, e.g. `[{ limit: 10, windowMs: 1000 }]`. Multiple
  overlapping windows are enforced together

**KeyedRateLimitGroup(windows, options?)**
- The same windows applied **independently per key**, for when each endpoint,
  tenant or account has its own quota
- `options.keyFrom` derives the key from a task, defaulting to `task.limitKey`
- A task with no derivable key throws at `addTask` rather than silently sharing
  a bucket
- Blocked tasks queue per key, so a saturated key never delays another
- Idle keys are evicted after `options.idleKeyTtlMs` (default: twice the largest
  window), so high-cardinality keys do not grow without bound
- `getKeyMetrics(key)` and `getActiveKeys()` give the per-bucket view

**Worker management** (on a `WorkerManager`)
- `getWorkerIds()`: ids of the workers that currently exist (created lazily)
- `getWorkerStatus(id)` / `getAllWorkerStatuses()`: `{ id, state, tasksCompleted,
  errorCount, uptime, currentTasks, resourcesHeld, lastError? }`
- `restartWorker(id, newConfig?)` / `replaceWorker(...)`: terminate and rebuild a
  worker, optionally with new config. The replacement gets a new id
- `updateWorkerConfig(id, config)`: merge config into a live worker
- `getResidentUsage()`: units this pool currently holds in each resident group
- `shutdown()`: terminate the pool

**Worker self-termination** — a worker can ask to be torn down, e.g. after
detecting a bad connection. In-flight tasks are requeued when the pool's
`requeueFailedTasks` allows it:

```typescript
this.workerContext?.terminateWithError(new Error('connection lost'), { canRestart: true });
```

### When Workers Fail

The scheduler never discards queued work to make a broken pool look healthy:

- A worker that **dies or cannot be constructed**, with `requeueFailedTasks: true`
  (the default), has its tasks requeued. The pool retries up to
  `maxWorkerRestarts` times, then gives up. If it never manages to run a worker
  those tasks wait, and their promises stay pending - a pool that cannot keep or
  build a worker needs fixing, not its work thrown away.
- With `requeueFailedTasks: false` the tasks fail and their promises reject.

So watch the pool, not only the task. `worker.restart_limit_exceeded` means the
pool has given up rebuilding workers and needs intervention - it covers both a
worker that cannot be built and one that keeps dying.

### Events

On the **scheduler**: `task.running`, `task.completed`, `task.failed`,
`task.progress`, `task.user_action`, `task.spawn_request`, `task.spawn_failed`,
`scheduler.completed`, `resource.acquired`, `resource.released`,
`resource.blocked`, `resource.unblocked`. Every one carries a `timestamp`.

The four `resource.*` events let a resource view be a fold of a stream rather
than a poll of four accessors that describe four different instants. They fire
on the **scheduler only** - a `WorkerManager` never dispatches them, so a
consumer subscribing to both cannot double-count. See `AGENTS.md` section 6 for
the payload, the four ways a blocked task leaves its queue, and the two
conservation invariants worth folding against.

On a **WorkerManager**: `task.started`, `task.completed`, `task.failed`,
`task.progress`, `task.spawn_request`, `task.requeue_required`, `worker.failed`,
`worker.self_terminated`, `worker.restart_limit_exceeded`, plus the worker
lifecycle events `worker.initialization.started|completed|failed`,
`worker.setup.started|completed` and `worker.teardown.started|completed|failed`.

> An event `detail` is a **live object, not a snapshot**. For `task.running`,
> `task.completed`, `task.failed` and `task.user_action` it *is* the
> `FyflowTask` the scheduler keeps mutating, so `arr.push(e.detail)` gives you
> entries that all read the terminal state later. Listeners run synchronously -
> project the fields you need inside the listener instead. `task.progress`,
> `scheduler.completed` and the `resource.*` events are unaffected; their
> details are copies.

Lifecycle events carry `{ workerId, workerType, timestamp }`, plus `duration` on
`*.completed` and `error` on `*.failed`. Inline and threaded pools emit the same
set. They fire on every worker creation and idle-timeout teardown, so keep those
listeners cheap.

`scheduler.completed` fires whenever the scheduler drains, so it can fire more
than once if tasks arrive in waves.

### Worker Interface

Workers extend `BaseWorker` and implement:
- `constructor(config)`: Setup worker configuration
- `async run(payload, context)`: Execute task logic
- `async setup()`: Initialize worker (required by `BaseWorker`; may be empty)
- `async teardown()`: Cleanup worker (required by `BaseWorker`; may be empty)

## Performance

Measured with `npm run benchmark:quick`:

- **Throughput (inline)**: tens of thousands of tasks/sec at 10K tasks, falling
  at larger volumes as coordination grows relative to per-task work
- **Efficiency (threaded, CPU work)**: 96-98%. This is achieved average
  concurrency over configured concurrency - "how much of the parallelism I asked
  for did I get" - so steady-state coordination overhead is a few percent
- **Memory**: roughly 1MB per 1,000 in-flight tasks at 10K scale. Completed tasks
  are retained by default; see `maxCompletedTasks`

**Worker startup** is measured separately (`npm run benchmark:startup`), because
every other scenario pre-warms its pool. It is the largest difference between the
two worker types, and between runtimes:

| | Deno | Node 22 |
|---|---:|---:|
| 1 worker thread | ~12 ms | ~54 ms |
| 8 inline instances (each) | ~85 µs | ~324 µs |

Threads cost roughly three orders of magnitude more to start than inline
instances, so a short-lived threaded pool can spend most of its life starting up.

Numbers vary substantially between runs and machines, and a scenario's result
depends on what ran before it in the suite. Re-run the suite rather than relying
on these figures, and compare full-suite runs only against other full-suite runs.

## Platform Support

| Platform | Package | Worker Threads | Inline Workers | Build Required |
|----------|---------|---------------|----------------|----------------|
| Node.js  | npm `fyflow-scheduler` | ✅ | ✅ | Prebuilt in the package |
| Browser  | npm `fyflow-scheduler` | ✅ | ✅ | Prebuilt in the package |
| Deno     | JSR `@fyflow/scheduler` | ✅ | ✅ | None - sources are loaded directly |

Only four files differ between runtimes, and the split is resolved at build time
rather than at runtime: the worker bootstrap (`workerWrapper.ts` vs
`workerWrapper.node.ts`), the URL that locates it, and two feature checks in
`ThreadWrapper` for Node's `worker_threads` event emitter. Everything else -
the scheduler, worker manager and all resource groups - is platform-agnostic.

Node.js 22 or newer is required: the library and its tests rely on `CustomEvent`
being a global, which landed in Node 19.

## Development

```bash
# Build library
npm run build

# Run tests - core, error handling, spawning and the documentation examples
npm test                    # Node.js tests (requires Node >= 22)
npm run test:browser        # Browser tests (Playwright)
npm run test:deno           # Deno tests
npm run test:docs           # Just the executable documentation examples
npm run test:performance    # Contention scaling (not part of the default run)

# Type check library, tests, examples and benchmarks
deno task check

# Benchmarks
npm run benchmark           # Full benchmark suite
npm run benchmark:quick     # Quick performance test
npm run benchmark:startup   # Worker startup cost, for comparing runtimes
npm run benchmark:baseline  # The set behind benchmark-baseline-comprehensive.md
```

## Examples

See the `examples/` directory for comprehensive usage examples:
- `getting-started.ts`: Basic parallel task execution
- `enhanced-features.ts`: Progress reporting, dynamic task spawning and `onCompleteDescendants()`
- `worker-types.ts`: Thread vs inline worker comparison
- `performance-groups.ts`: Resource management, rate limits and constraints
- `resident-groups.ts`: Worker-lifetime resources and weighted per-worker costs
- `monitor-resource-health.ts`: Drop-in resource health monitor helper
- `deno-only/minimal.ts`: The plain Deno form - a worker URL with no build step
  and none of this repo's bundler convention

The examples above the `deno-only/` folder run on both Deno and Node, which is
why they carry the `?worker-direct` branch. Run them all with
`npm run build:dev && deno task examples`.

## License

MIT License.

## Links

- [GitHub Repository](https://github.com/fyflow/fyflow-scheduler)
- [NPM Package](https://www.npmjs.com/package/fyflow-scheduler)
- [Issue Tracker](https://github.com/fyflow/fyflow-scheduler/issues)