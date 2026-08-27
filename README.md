# FyFlow Scheduler

Zero-dependency parallel task scheduler with resource management and cross-platform support.

[![npm version](https://badge.fury.io/js/fyflow-scheduler.svg)](https://badge.fury.io/js/fyflow-scheduler)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Note**: This project was mostly "vibe coded" with [Claude Code](https://claude.ai/code) - it was an experiment, if such a complex library can be built with sufficient code quality.

> **Using this library from an AI agent?** [AGENTS.md](AGENTS.md) is a complete
> single-file reference: exact signatures, defaults, events, gotchas and recipes.
> Every snippet in it is executed by the test suite.

## Overview

FyFlow is a self-contained parallel task scheduler with resource management. Fully in-memory operation with zero dependencies. Extensible APIs allow enhancement for persistence, distributed execution, or multi-machine coordination.

## Installation

```bash
npm install fyflow-scheduler
# or
yarn add fyflow-scheduler
```

For Deno:
```bash
deno add @fyflow/scheduler
```

## Quick Start

```typescript
import { FyflowScheduler, FyflowTask, WorkerManager, ConcurrentLimitGroup } from 'fyflow-scheduler';

// Create worker pool
const workerPool = new WorkerManager('./worker.js', { maxThreads: 2 });

// Create scheduler with CPU constraints
const scheduler = new FyflowScheduler(
  { MyWorker: workerPool },
  { cpu: new ConcurrentLimitGroup(4) }
);

// Create parallel tasks
const tasks = [
  new FyflowTask({ id: 'task1', workerType: 'MyWorker', payload: { data: 'input' } }),
  new FyflowTask({ id: 'task2', workerType: 'MyWorker', payload: { data: 'processed' } })
];

// Execute
tasks.forEach(task => scheduler.addTask(task));
```

## Key Features

- **Parallel Task Execution**: High-performance concurrent task processing
- **Resource Management**: CPU/GPU constraints with concurrent execution limits
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

### Loading Workers (differs per runtime)

Worker URLs do not resolve the same way everywhere. Deno loads the TypeScript
source directly; Node and the browser go through this project's esbuild worker
plugin, which rewrites a `?worker-direct` import at build time.

```typescript
let workerUrl;
if (typeof Deno !== "undefined") {
  workerUrl = new URL("./workers/myWorker.ts", import.meta.url).href;
} else {
  // Requires a build (`npm run build`) - resolved by esbuild, not TypeScript
  workerUrl = new URL((await import("./workers/myWorker.ts?worker-direct")).default).href;
}
```

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

### Event Monitoring

```typescript
scheduler.addEventListener('task.completed', (e) => {
  console.log(`Task ${e.detail.id} completed:`, e.detail.result);
});

scheduler.addEventListener('task.progress', (e) => {
  // progress is 0-1
  console.log(`Task ${e.detail.taskId}: ${(e.detail.progress * 100).toFixed(0)}%`);
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
- `options.periodicRetryIntervalMs`: retry interval for blocked tasks (default 50ms)
- `addTask(task, options?)`: Add a task. Fire-and-forget by default; pass
  `{ createPromise: true }` to get a promise back
- `addTasks(tasks, options?)`: Batch version, optimised for bulk additions.
  Throws on an unknown `workerType`, validating the batch before queueing any of it
- `stats`: Current execution statistics
- `getResourceMetrics()` / `getResourceStats()`: Per-group utilisation and lifetime counters
- `shutdown()`: Terminate worker pools and release all listeners
- `addEventListener(event, handler)`: Event monitoring

**FyflowTask(config)**
- `id`: Unique task identifier
- `workerType`: Worker pool to use
- `payload`: Data passed to worker
- `workerGroups`: Optional resource group names
- `optional`: Mark task as optional (failures don't block workflow)
- `retryPolicy`: `{ maxRetries, backoffMs }`
- `onCompletePromise()`: Promise for this task alone
- `onCompleteDescendants()`: Promise for this task plus everything it spawns

**WorkerManager(scriptUrl, options)**
- `maxThreads`: Maximum worker threads
- `maxConcurrentTasks`: Tasks per worker
- `groups`: Resource group names
- `inline`: Use inline execution (default: false)
- `idleTimeout`: Ms before an idle worker is terminated (0 = never, default 5000)

**ConcurrentLimitGroup(limit, id?)**
- Resource constraint with a concurrent execution limit. Optimistic: may briefly
  exceed the limit by up to `maxThreads × maxConcurrentTasks` under contention

**RateLimitGroup(windows, id?)**
- Time-window throttling, e.g. `[{ limit: 10, windowMs: 1000 }]`. Multiple
  overlapping windows are enforced together

**Worker management** (on a `WorkerManager`)
- `getWorkerIds()`: ids of the workers that currently exist (created lazily)
- `getWorkerStatus(id)` / `getAllWorkerStatuses()`: `{ id, state, tasksCompleted,
  errorCount, uptime, currentTasks, resourcesHeld, lastError? }`
- `restartWorker(id, newConfig?)` / `replaceWorker(...)`: terminate and rebuild a
  worker, optionally with new config. The replacement gets a new id
- `updateWorkerConfig(id, config)`: merge config into a live worker
- `shutdown()`: terminate the pool

**Worker self-termination** — a worker can ask to be torn down, e.g. after
detecting a bad connection. In-flight tasks are requeued when the pool's
`requeueFailedTasks` allows it:

```typescript
this.workerContext?.terminateWithError(new Error('connection lost'), { canRestart: true });
```

### Events

On the **scheduler**: `task.running`, `task.completed`, `task.failed`,
`task.progress`, `task.user_action`, `task.spawn_request`, `task.spawn_failed`,
`scheduler.completed`.

On a **WorkerManager**: `task.started`, `task.completed`, `task.failed`,
`task.progress`, `task.spawn_request`, `task.requeue_required`, `worker.failed`,
`worker.self_terminated`, `worker.restart_limit_exceeded`, plus the worker
lifecycle events `worker.initialization.started|completed|failed`,
`worker.setup.started|completed` and `worker.teardown.started|completed|failed`.

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

Measured with `npm run benchmark:quick` on Node.js:
- **Inline Workers**: peaks above 70,000 tasks/sec at 10K tasks, dropping to
  ~6,000 tasks/sec at 100K as coordination grows relative to per-task work
- **Thread Workers**: ~80-90% efficiency for CPU workloads
- **Memory Usage**: ~1MB per 1,000 tasks at 10K scale, growing at larger volumes

Numbers vary substantially between runs and machines - re-run the suite rather
than relying on these figures.

## Platform Support

| Platform | Worker Threads | Inline Workers | Build Required |
|----------|---------------|----------------|----------------|
| Node.js  | ✅            | ✅             | Yes (esbuild)  |
| Browser  | ✅            | ✅             | Yes (esbuild)  |
| Deno     | ✅            | ✅             | No             |

Node.js 22 or newer is required: the library and its tests rely on `CustomEvent`
being a global, which landed in Node 19.

## Development

```bash
# Build library
npm run build

# Run tests (core + error handling + spawning)
npm test                    # Node.js tests
npm run test:browser        # Browser tests
npm run test:deno           # Deno tests
npm run test:performance    # Contention scaling (not part of the default run)

# Benchmarks
npm run benchmark          # Full benchmark suite
npm run benchmark:quick    # Quick performance test
```

## Examples

See the `examples/` directory for comprehensive usage examples:
- `getting-started.ts`: Basic parallel task execution
- `enhanced-features.ts`: Progress reporting, dynamic task spawning and `onCompleteDescendants()`
- `worker-types.ts`: Thread vs inline worker comparison
- `performance-groups.ts`: Resource management, rate limits and constraints
- `monitor-resource-health.ts`: Drop-in resource health monitor helper

## License

MIT License.

## Links

- [GitHub Repository](https://github.com/fyflow/fyflow-scheduler)
- [NPM Package](https://www.npmjs.com/package/fyflow-scheduler)
- [Issue Tracker](https://github.com/fyflow/fyflow-scheduler/issues)