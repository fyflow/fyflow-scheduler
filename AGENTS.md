# FyFlow Scheduler — Reference for AI Agents

Complete usage reference for `fyflow-scheduler`. Everything here is exercised by
`tests/suites/docs.ts`, which runs on every `npm test` — if a snippet stops
working, the build fails.

**Package**: `fyflow-scheduler` (npm) for Node **≥ 22** and browsers /
`jsr:@fyflow/scheduler` for Deno
**Dependencies**: none

The two are not interchangeable. The JSR package is **Deno-only** and contains no
Node-specific files; importing it outside Deno throws an error pointing at npm.
Node and browser support lives entirely in the npm package, which ships prebuilt
bundles with the worker bootstrap inlined.

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

## 3. Worker URLs

A worker is loaded by URL, and what you pass depends on your runtime.

### npm (Node and browser) - ship a `.js` worker

```typescript
const workerUrl = new URL('./myWorker.js', import.meta.url).href;

const pool = new WorkerManager(workerUrl, { maxThreads: 4 });
```

That is all. No bundler plugin, no build step for the worker, no query suffix.
Verified against the published package with both `inline: true` and
`inline: false`.

- The file must be **JavaScript**. Node cannot import TypeScript, so a `.ts`
  worker fails with `Unknown file extension ".ts"`. Compile your worker as part
  of your own build.
- In the browser the URL must be reachable by the page, and your bundler needs to
  emit the worker as a separate asset. Most bundlers do this for
  `new URL('./myWorker.js', import.meta.url)` automatically.

### Deno (JSR) - point at the TypeScript source

```typescript
const workerUrl = new URL('./myWorker.ts', import.meta.url).href;
```

Deno loads the source directly. No build step.

### Cross-runtime code

Only needed if one codebase must run on both:

```typescript
const workerUrl = typeof Deno !== 'undefined'
  ? new URL('./myWorker.ts', import.meta.url).href
  : new URL('./myWorker.js', import.meta.url).href;
```

> **`?worker-direct` is not part of this API.** You will see it in this
> repository's own examples and tests. It is a convention of *this repo's*
> `esbuild.config.js`, which inlines a `.ts` worker into the bundle so the
> examples can be written once and run on both runtimes. It does not exist in the
> published packages, and using it in your own code will fail to resolve.

The worker file must `export default` a class. Anything else fails with
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
- A worker can ask the pool to tear it down - e.g. after a connection goes bad -
  with `this.workerContext?.terminateWithError(err, { canRestart: true })`. The
  task running at the time rejects with a `WorkerTerminationError`, so catch that
  to tell worker shutdown apart from an ordinary task failure:

  ```typescript
  import { WorkerTerminationError } from 'fyflow-scheduler';

  try {
    await scheduler.addTask(task, { createPromise: true });
  } catch (error) {
    if (error instanceof WorkerTerminationError) { /* the worker went away */ }
  }
  ```
- One instance is created per worker, lazily, and reused across tasks. Instance
  state persists between tasks — do not assume a fresh object per task.
- Throwing from `run()` fails that task. Throwing from `setup()` fails the
  worker.

---

## 5. API reference

### Everything importable

```typescript
import {
  FyflowScheduler, FyflowTask, WorkerManager,   // core
  ConcurrentLimitGroup, RateLimitGroup, KeyedRateLimitGroup,
  BaseWorker, WorkerTerminationError            // worker authoring
} from 'fyflow-scheduler';

import type {
  FyflowSchedulerOptions, AddTaskOptions,
  WorkerManagerOptions, WorkerConfig, WorkerInterface, WorkerStatus,
  WorkerInstanceState, BaseWorkerContext, TaskWorkerContext, WorkerContext,
  SpawnTaskConfig, ProgressData,
  ResourceGroup, ResourceGroupMetrics, ResourceGroupStats,
  RateWindow, KeyedRateLimitGroupOptions, KeyedTaskLike,
  GaugeKind, GaugeSpec, GaugeReading, GaugeDescription,       // gauges
  ResourceLifetime, HolderKind, ResourceReleaseReason,        // resource events
  ResourceUnblockReason, ResourceEventDetail,
  ResourceAcquiredDetail, ResourceReleasedDetail,
  ResourceBlockedDetail, ResourceUnblockedDetail,
  AdmissionWaiter
} from 'fyflow-scheduler';
```

`ThreadWrapper` and `InlineWrapper` are also exported, but they are the internal
worker wrappers the pool manages for you - you should not construct them.


### `new FyflowScheduler(workerPools, resourceGroups?, options?)`

| Argument | Type | Notes |
|---|---|---|
| `workerPools` | `Record<string, WorkerManager>` | Keys are the `workerType` values tasks refer to |
| `resourceGroups` | `Record<string, ResourceGroup>` | Keys are the group ids used in `groups` / `workerGroups` |
| `options.maxCompletedTasks` | `number` | Cap on retained terminal tasks. Default: unlimited |
| `options.periodicRetryIntervalMs` | `number` | Heartbeat for waking blocked tasks. Default `50` |

| Member | Signature | Notes |
|---|---|---|
| `addTask` | `(task, opts?) => Promise<any> \| void` | Returns a promise only with `{ createPromise: true }`. **Throws** on unknown `workerType` |
| `addTasks` | `(tasks, opts?) => Promise<any>[] \| void` | Batched dispatch for bulk adds. **Throws** on unknown `workerType`, validating the whole batch before queueing any of it |
| `stats` | `{ queued, running, done, failed }` | Counts **tasks**, not attempts - a task that fails after two retries counts once. Includes evicted tasks |
| `tasks` | `Map<string, FyflowTask>` | Live and completed tasks (see `maxCompletedTasks`) |
| `getResourceMetrics()` | `Record<string, { limit, running, available, utilization }>` | `utilization` is 0–1 |
| `getResourceStats()` | `Record<string, { totalAcquired, totalReleased }>` | Lifetime counters |
| `describeResources()` | `Record<string, { gauges: GaugeSpec[] }>` | The gauges every group presents. **Schema, not state** — it cannot change over a group's lifetime, so read it once and cache it |
| `getAdmissionQueue(groupId?)` | `Record<string, AdmissionWaiter[]>` | Everything currently waiting on a resource group, in admission order — see below |
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
| `limitKey` | `string` | none | Bucket for keyed groups. Required if the task belongs to a `KeyedRateLimitGroup` with no custom `keyFrom` |
| `handleRejection` | `boolean` | `true` | Silences unhandled rejections for fire-and-forget |

Readable after the run: `state`, `result`, `error`, `attempts`, `startTime`,
`endTime`, `executionTime` (worker-measured ms, excludes queue wait).

States: `pending` → `running` → `done` | `failed`. A **non-optional** task that
fails with no retries left ends in **`user_action`**, not `failed`; an
**optional** one ends in `failed` and resolves `null`. Each attempt settles
exactly once, so `task.failed` fires once per failed task and the terminal state
does not change afterwards.

Methods: `onCompletePromise()` — this task only; `onCompleteDescendants()` —
this task plus everything it spawns (§7).

### `new WorkerManager(scriptUrl, options)`

| Option | Default | Notes |
|---|---|---|
| `maxThreads` | `2` | Worker **instances**, not OS threads — see below |
| `maxConcurrentTasks` | `1` | Tasks per instance. Pool capacity = `maxThreads × maxConcurrentTasks` |
| `inline` | `false` | `true` runs in the main process — right for async/IO, wrong for CPU-bound |
| `config` | `{}` | First constructor argument for every worker instance |
| `groups` | `[]` | Group ids every task in this pool must acquire |
| `residentGroups` | `{}` | Groups held for a **worker's** lifetime — see below |
| `idleTimeout` | `5000` | Ms before an idle worker is terminated. `0` = never |
| `idleCheckIntervalMs` | `5000` | How often idle workers are swept |
| `requeueFailedTasks` | `true` | Requeue in-flight tasks when a worker dies |
| `maxWorkerRestarts` | `3` | Then `worker.restart_limit_exceeded` |

Management: `getWorkerIds()`, `getWorkerStatus(id)`, `getAllWorkerStatuses()`,
`restartWorker(id, newConfig?)`, `replaceWorker(...)` (alias),
`updateWorkerConfig(id, config)`, `getResidentUsage()`, `shutdown()`.

#### `residentGroups` — resources a worker holds by existing

`groups` is scoped to a **task**: acquired at dispatch, released when the task
settles. That cannot express a resource held because the *worker* exists — a
model loaded on a GPU in `setup()`, a connection, a licence seat. The worker
outlives its task by `idleTimeout`, so a second pool can take the resource while
the first still holds it, even with a correctly behaving
`ConcurrentLimitGroup(1)`.

`residentGroups` is held from worker creation to teardown, with a cost **per
worker**:

```typescript
const vram = new ConcurrentLimitGroup(24, 'vram');

// 20 of 24 units per worker - only one of these may exist at a time
new WorkerManager(bigModelUrl, { maxThreads: 1, residentGroups: { vram: 20 } })

// 2 units each; four may exist alongside each other, none alongside the big one
new WorkerManager(smallModelUrl, { maxThreads: 4, residentGroups: { vram: 2 } })

// Shorthand for a cost of 1 per worker
new WorkerManager(url, { residentGroups: ['vram'] })
```

A worker is not created while its cost does not fit, and tasks needing one wait
until a holder is torn down — which happens on the normal idle-timeout path, so
a pool with work arriving keeps its worker and its loaded model. Admission is
head-of-line: a waiting expensive pool is not overtaken by cheaper ones.

Costs must be positive integers and no larger than the group's limit; both are
rejected at construction, as is an unknown group id. Because the release is
driven by the idle sweep, hand-over takes up to `idleTimeout + idleCheckIntervalMs`
— lower `idleCheckIntervalMs` if that matters.

##### `idleTimeout: 0` deadlocks a *contended* resident group

`idleTimeout: 0` means never terminate, so such a worker never releases its
units. This is deliberately not rejected, because it is fine whenever nothing
else needs the group — a sole holder, or pools whose costs all fit at once.

It deadlocks only under contention: once another pool needs units that a
never-terminating worker holds, the wait can never be satisfied. The blocked
task stays `pending` forever.

The failure is at least not disguised as success — `scheduler.completed` does
**not** fire, because it accounts for tasks blocked on a group. But `stats`
reads `queued=0 running=0`, since a blocked task is removed from `queued`, so
a stalled scheduler looks idle.

Three ways to see it, in order of directness:

```typescript
// 1. The event names the contended group as the task is queued
scheduler.addEventListener('resource.blocked', e =>
  console.log(`${e.detail.holderId} waiting on ${e.detail.groupId} (${e.detail.lifetime})`));

// 2. The accessor says what is waiting, and for how many units
scheduler.getAdmissionQueue();   // { vram: [{ holderId: 'b1', cost: 1, position: 0, ... }] }

// 3. And the group is pinned at its limit with nothing progressing
scheduler.getResourceMetrics();  // { vram: { limit: 1, running: 1, available: 0, ... } }
```

Give any pool that shares a contended resident group a non-zero `idleTimeout`.

##### Starvation is different, and expected

A pool with work arriving continuously keeps its worker, and so keeps its
units. A pool waiting on those units waits for the holder's backlog to drain —
measured at ~2s behind 60 queued tasks. This resolves on its own and is the
intended trade: holding the resource is what stops a hot model being reloaded
between tasks. Bound it with a shorter `idleTimeout` on the busy pool if the
wait matters more than the reload.

`WorkerStatus` = `{ id, state, tasksCompleted, errorCount, uptime, currentTasks,
resourcesHeld, lastError? }`, where `state` is
`initializing | healthy | busy | failed | terminated`.

#### What `maxThreads` means for an inline pool

`maxThreads` counts worker *instances*, not threads. With `inline: false` each
instance is a real worker thread. With `inline: true` they are all objects in the
main process — **no threads are created at all** — and concurrency comes from
overlapping `await`s on the event loop.

So for inline pools only the **product** matters for throughput. Measured with
120 tasks each awaiting 40ms:

| Config | Instances | Peak in-flight | Duration |
|---|---:|---:|---:|
| `maxThreads: 1, maxConcurrentTasks: 5` | 1 | 5 | 1115 ms |
| `maxThreads: 4, maxConcurrentTasks: 5` | 4 | 20 | 295 ms |
| `maxThreads: 1, maxConcurrentTasks: 20` | 1 | 20 | 292 ms |
| `maxThreads: 4, maxConcurrentTasks: 20` | 4 | 80 | 128 ms |

`4 × 5` and `1 × 20` are interchangeable for speed. What the split *does* change
is **state isolation**: each instance is constructed separately, so
`maxThreads: 4` gives four connection pools / caches / whatever per-instance
state your worker holds, while `maxThreads: 1` funnels every concurrent task
through a single object.

Neither knob buys parallelism for CPU-bound work inline — it is one event loop.
Use `inline: false` for that.

### Resource groups

```typescript
new ConcurrentLimitGroup(limit: number, id?: string)
new RateLimitGroup(windows: { limit: number, windowMs: number }[], id?: string)
new KeyedRateLimitGroup(windows, { id?, keyFrom?, idleKeyTtlMs? })
```

All three expose `canRun()`, `getMetrics()`, `getStats()`, and `describe()` /
`read()` for gauges (below). Tasks over a limit wait in a blocked queue and are
retried — never dropped. Multiple rate-limit windows are enforced together.

---

#### Per-key rate limits

`KeyedRateLimitGroup` applies its windows **independently per key**, for when
each endpoint, tenant or account has its own quota:

```typescript
const api = new KeyedRateLimitGroup(
  [{ limit: 10, windowMs: 1000 }],           // 10/sec PER KEY, not in total
  { id: 'api', keyFrom: t => t.payload.endpoint }
);

const pool = new WorkerManager(url, { groups: ['api'], inline: true });
const scheduler = new FyflowScheduler({ ApiWorker: pool }, { api });
```

- `keyFrom` defaults to `t => t.limitKey`, so tasks can carry the key directly:
  `new FyflowTask({ id, workerType, payload, limitKey: 'tenant-a' })`
- **A task with no derivable key throws at `addTask`**, rather than silently
  sharing a bucket or skipping the limit
- Blocked tasks are queued per key, so a saturated key never delays another one
- Idle keys are evicted once they hold no running tasks and have been quiet
  longer than `idleKeyTtlMs` (default: twice the largest window), so
  high-cardinality keys do not grow without bound
- `getMetrics()` is the aggregate across keys plus `activeKeys`;
  `getKeyMetrics(key)` gives one bucket, `getActiveKeys()` lists them

The limit is per key, not global: with 3 keys and a limit of 2, up to 6 tasks run
at once.

---

#### Gauges — how a group describes itself

A group's state is not always one number. `RateLimitGroup` enforces *10/sec AND
100/min* together, and no single bar can show that. Worse, `getMetrics()` is
actively misleading for a rate group — with 10 completions in the last second
and nothing in flight:

```typescript
group.getMetrics()  // { limit: 10, running: 0, available: 10 }  - looks idle
group.canRun()      // false                                      - actually saturated
```

So a group may declare **gauges** instead. A consumer renders one *kind*, never
a group type:

```typescript
group.describe()   // { gauges: GaugeSpec[] }   static:  id, label, kind, unit, limit, windowMs?
group.read(key?)   // GaugeReading[]            dynamic: id, value, limit, resetAt?
```

| Group | Gauges |
|---|---|
| `ConcurrentLimitGroup` | 1 × `level`, id `units` |
| `RateLimitGroup` | one `window` per configured window, ids `window-0`, `window-1`, … |
| `KeyedRateLimitGroup` | the same windows, **per key** — `read()` requires a key and returns `[]` without one |
| anything else | 1 × `level` derived from `getMetrics()` |

**Both methods are optional.** A group implementing only `canRun`, `onStart`,
`onFinish` and `getMetrics` — which is every group written before gauges existed,
and any you write yourself — is described as a single `level` gauge with no
changes. Implement them when one number would be a lie.

Two rules that are part of the contract, not implementation detail:

- **An unrecognised `kind` renders as `level`**, using `value` and `limit`.
  Without this, adding a third kind later breaks every viewer already deployed.
- **Readings are never clamped.** Groups are optimistic, so a `level` reading may
  exceed its limit; clamping would make the event disagree with `getMetrics()`
  exactly when the disagreement matters.

`ResourceGroup.type` is `string`, not a closed union, so a group defined outside
this library can name itself. Do not `switch` exhaustively on it — that is what
`kind` is for.

## 6. Events

Attach with `addEventListener(name, e => ...)`; payload is in `e.detail`.

**On the scheduler:**

| Event | `e.detail` |
|---|---|
| `task.running` | the `FyflowTask` |
| `task.completed` | the `FyflowTask` (with `result`, `executionTime`) |
| `task.failed` | the task, plus `error` |
| `task.progress` | the task's fields **spread flat** (so the id is `id`, **not** `taskId`), plus `{ workerId, workerType, progress (0–1), message, details, timestamp }` |
| `task.user_action` | the task — failed, out of retries, not optional. Fires once, only after the retry budget is spent |
| `task.spawn_request` | `{ parentTask, spawnConfig, workerId, workerType }` |
| `task.spawn_failed` | `{ parentTask, spawnConfig, error }` |
| `scheduler.completed` | a **copy** of `stats`, plus `timestamp` |
| `resource.acquired` | `ResourceEventDetail` — see below |
| `resource.released` | the same, plus `reason` |
| `resource.blocked` | the same, plus `queuePosition` |
| `resource.unblocked` | the same, plus `reason` |

Every event above carries a `timestamp` (`Date.now()` at emit). On the four
`task.*` events that is a field on the task itself — see the live-reference
warning immediately below.

> ⚠️ **An event `detail` is a live object, not a snapshot.** For `task.running`,
> `task.completed`, `task.failed` and `task.user_action`, `detail` **is** the
> `FyflowTask` — the same object the scheduler keeps mutating. Listeners run
> synchronously, so reading it inside the listener is correct; keeping it is not:
>
> ```javascript
> const seen = [];
> scheduler.addEventListener('task.running', e => seen.push(e.detail));
> // later: every entry reads state:'done'. Nothing threw. The log is just wrong.
> ```
>
> **Project what you need inside the listener** — `seen.push({ id: e.detail.id,
> state: e.detail.state, timestamp: e.detail.timestamp })` — rather than keeping
> the reference. This is why `timestamp` is not derivable from `startTime` or
> `endTime`: those are fields on the same live object.
>
> Not affected: `task.progress` (`detail` is a shallow copy),
> `scheduler.completed` (a copy of `stats`, not the live counters), and the four
> `resource.*` events (a fresh object per event). Details are not frozen or
> defensively copied on the hot path — this is a documented contract, not an
> oversight.

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
| `worker.setup.failed` | `{ workerId, workerType, timestamp, error }` |
| `worker.teardown.started` | `{ workerId, workerType, timestamp }` |
| `worker.teardown.completed` | `{ workerId, workerType, timestamp, duration }` |
| `worker.teardown.failed` | `{ workerId, workerType, timestamp, error }` |

`workerType` is `'inline'` or `'thread'`. Inline and threaded pools emit the same
set with the same shape. These fire on every worker creation and every
idle-timeout teardown, so keep their listeners cheap.

**Setup and teardown events are unconditional and always paired.** `setup()` and
`teardown()` are both optional on `WorkerInterface`, but the events are not: they
report that a worker is being initialized or destroyed, not that it implemented a
hook. Any worker that was constructed emits `worker.setup.started` followed by
exactly one of `worker.setup.completed` or `worker.setup.failed`, and
`worker.teardown.started` followed by exactly one of `worker.teardown.completed`
or `worker.teardown.failed`.

That holds whether the hook is absent, returns, throws, or — for teardown —
stops answering, in which case the pool gives up on it and reports the failure
itself. A `started` with no terminal event is a bug, so you can safely fold these
into worker state and expect every pair to close. A worker that was never
constructed emits nothing, since no worker is being initialized or destroyed.

A failing `setup()` emits **both** `worker.setup.failed` and, after it,
`worker.initialization.failed`. They are not duplicates: the first closes the
setup pair, the second is the signal the pool acts on when deciding to restart or
give up. Count failures from `worker.initialization.failed`, not from both.

**A failing `setup()` still tears the worker down.** `setup()` runs after the
instance is constructed, so one that throws partway may already hold something
only `teardown()` releases — a connection, a lock, a remote session. `teardown()`
is therefore invoked, with its own `worker.teardown.started` and terminal event,
before the initialization failure is reported. Write `teardown()` so it tolerates
partially-initialised state: it can run against an instance whose `setup()` never
finished. A `teardown()` that throws there is reported as
`worker.teardown.failed` and does not replace the setup error.

> `scheduler.completed` fires **every time the scheduler drains**, so it can fire
> more than once when tasks arrive in waves. It does account for tasks blocked on
> a resource group, so it will not fire while work is still waiting for capacity.

### `resource.*` — building a resource view from the stream

These exist so resource state can be a **fold of an event stream** rather than a
poll. Four accessors read in sequence describe four different instants; a stream
does not have that problem.

```typescript
interface ResourceEventDetail {
  groupId:    string;
  groupType:  string;              // the group's own `type`. Open - do not switch on it
  lifetime:   'task-held' | 'resident';
  holderKind: 'task' | 'worker';
  holderId:   string;              // task id, or worker id
  workerType: string;              // the POOL KEY - see the warning below
  cost:       number;              // units taken. Always 1 except for a weighted resident hold
  key?:       string;              // the bucket, for keyed groups
  timestamp:  number;
  readings:   GaugeReading[];      // group state AFTER the operation
}
```

| | Fired |
|---|---|
| `resource.acquired` | immediately after `onStart`, both lifetimes |
| `resource.released` | immediately after `onFinish`. `reason`: `'settled'` \| `'worker-teardown'` \| `'shutdown'` |
| `resource.blocked` | on **every** push onto a blocked queue, naming the one group the waiter is short of |
| `resource.unblocked` | on **every** exit from a blocked queue, tagged with which |

`'settled'` is broader than it sounds: it means *the task attempt that held this
ended*. A task released and re-acquired by a **retry**, or **requeued** because
its worker died, emits one `settled` release per attempt. Fold acquire/release
pairs per attempt, not per task — a task with two retries produces three pairs.

**`resource.*` fires on the scheduler only.** A `WorkerManager` never dispatches
them, not even for the resident holdings it acquires itself — the scheduler
injects a notifier and re-emits. Subscribing to both the scheduler and every pool
is the natural thing to do, and if pools mirrored these you would count every
resident acquire twice and see conservation drift by exactly the resident share.

> ⚠️ `workerType` here is the **pool key** in `workerPools`. On the `worker.*`
> lifecycle events — and on `task.progress`, where the wrapper's value
> overwrites the task's — the same field name means `'inline'` or `'thread'`.
> They are different things. `task.progress` is the one to watch: its detail is
> the task spread flat, so `workerType` *looks* like the task's own field and is
> not.

**A blocked task leaves its queue four ways**, and `unblocked.reason` says which:

| `reason` | What happened |
|---|---|
| `admitted` | Moved to the ready queue — the ordinary exit |
| `superseded` | Admitted via a *different* group and spliced out of this one |
| `orphaned` | Its worker pool no longer exists. The task is **failed terminally** — `task.failed`, then `user_action` unless it is optional, and its promise rejects. Retries are skipped: the pool is gone |
| `shutdown` | The scheduler tore down and discarded every remaining waiter |

Two invariants worth folding against, because a fold can stay internally
consistent while being wrong:

- **Holdings.** Over a run that completes, `Σ acquired.cost − Σ released.cost === 0`
  for every `(groupId, key)`, and never negative. At a quiescent moment the
  folded occupancy equals `getMetrics().running` for a **concurrent** group. It
  does not for a rate group, and should not — compare the latest `window`
  readings against `getStatus()` instead.
- **Queue membership.** `Σ blocked − Σ unblocked === 0` for every
  `(groupId, key, holderId)` over a run that reaches shutdown, so one group's
  queue is exactly reconstructible — not just the set of waiting tasks.

### `getAdmissionQueue(groupId?)` — what is waiting right now

```typescript
interface AdmissionWaiter {
  holderId:   string;                    // task id - blocked queues hold tasks, both lifetimes
  workerType: string;                    // its pool
  lifetime:   'task-held' | 'resident';  // what it is waiting to acquire
  cost:       number;                    // the pool's per-worker cost, or 1 for task-held
  position:   number;                    // 0-based; 0 is the head, and the head gates the rest
  key?:       string;                    // the bucket, for keyed groups
}
```

Keyed by **group id**, with the bucket on the waiter. A keyed group has one
sub-queue per key; they are concatenated in key order, each numbered from 0. A
group with an empty queue is absent rather than present-and-empty.

`cost` is what makes a stall legible: a 20-unit waiter at position 0 is the
entire reason four 2-unit pools are not starting, because head-of-line admission
means nothing behind it may overtake.

Uncapped and `O(total blocked tasks)` — the panel this exists for is the
starvation case, which is exactly when the queue is long. With `resource.blocked`
/ `resource.unblocked` available, prefer the stream: this is for late-joiner
catch-up, since two reads describe two instants and a view built from them is not
a fold.

### How a blocked task wakes up

Two mechanisms, and knowing which applies explains the latency you see:

- **Directly**, when a slot frees. Releasing a task-held group retries that
  group's queue in the same turn, so a task waiting on a concurrency limit
  starts as soon as one finishes.
- **On the heartbeat**, every `periodicRetryIntervalMs` (default 50ms), for
  everything with no such signal — a **rate-limit window** rolling over, and
  **resident admission**, neither of which raises an event when it becomes
  satisfiable.

The heartbeat runs on a fixed interval while work exists. It used to be
restarted on every settle, which made it a debounce: a scheduler completing
tasks faster than the interval pushed its deadline out forever and it never
fired, so rate-limited and resident waiters starved indefinitely beside a busy
pool. Fixed in 0.3.0 — worth knowing if you are reading behaviour from an
earlier version.

**Cost.** One acquire and one release per task per group: 5 000 tasks/s across 3
groups is 30 000 events/s. Nothing is built and no `read()` runs while nobody is
subscribed, so an unused subscription is free — but a subscribed listener is on
the hot path, and there is no throttling or coalescing in the library. Keep
listeners cheap, or coalesce on your side.

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
- `shutdown()` drains outstanding work first, so a workflow that can still finish
  does, and the wait resolves. It rejects only for work that can no longer run

This is lineage, not a dependency: spawning never changes dispatch order.

---

## 8. When workers fail

The scheduler never discards queued work to make a broken pool look healthy. What
happens to a task depends on *how* its worker failed, and the two outcomes are
quite different:

| Failure | Tasks | Promises |
|---|---|---|
| A worker **dies or cannot be constructed**, with `requeueFailedTasks` `true` (the default) | requeued, and wait | stay **pending** until a worker can run them |
| The same, with `requeueFailedTasks` `false` | fail | reject |

The first row is deliberate. A pool that cannot keep - or build - a worker is an
operational problem that needs the worker fixed, not a reason to destroy work
that will run correctly once it is. So those tasks queue, and
`await scheduler.addTask(task, { createPromise: true })` simply does not settle
until the pool recovers.

The pool retries up to `maxWorkerRestarts` times, then gives up and emits
`worker.restart_limit_exceeded`. Construction failures and runtime deaths follow
the same path, and inline and threaded pools behave identically.

**That means you must watch the pool, not just the task.** All of these are
emitted on the `WorkerManager`:

| Event | Meaning |
|---|---|
| `worker.initialization.failed` | a worker could not be constructed - its tasks are failing |
| `worker.failed` | a live worker died |
| `task.requeue_required` | a task went back to the queue because its worker died |
| `worker.restart_limit_exceeded` | the pool has stopped rebuilding workers - **intervention needed** |

```typescript
pool.addEventListener('worker.restart_limit_exceeded', (e) => {
  // The pool has given up. Anything queued for it will wait until you fix it.
  alert(`pool exhausted ${e.detail.maxRestarts} restarts, queued: ${scheduler.stats.queued}`);
});
pool.addEventListener('worker.initialization.failed', (e) => {
  // Workers cannot even be built - usually a bad worker URL (§3)
  console.error(e.detail.error);
});
```

> `worker.restart_limit_exceeded` is the one event that means *stop and fix
> something* - it fires for a pool that cannot build a worker as well as one whose
> workers keep dying. Everything above it is recoverable noise.

## 9. Common mistakes

| Symptom | Cause |
|---|---|
| `await scheduler.addTask(t)` resolves `undefined` | Missing `{ createPromise: true }` |
| Process never exits | `shutdown()` not called |
| `Unknown worker type: X` | `workerType` is not a key of `workerPools` |
| `Worker script <url> must export a default class` | Worker has no `export default class`, or the URL is wrong for the runtime (§3) |
| `Unknown file extension ".ts"` on Node | Node cannot import TypeScript - ship a compiled `.js` worker (§3) |
| `Task must be added to a scheduler before tracking descendants` | `onCompleteDescendants()` called before `addTask` |
| `terminateWithError` does nothing | Worker constructor did not forward `workerContext` to `super` |
| Progress bar shows 0–1 instead of 0–100 | `progress` is a fraction; multiply by 100 yourself |
| Compile error extending `BaseWorker` | `setup()` / `teardown()` not implemented — both are abstract |
| Task ends in `user_action`, not `failed` | Non-optional task, retries exhausted |
| Memory grows in a long-running process | Completed tasks are retained; set `maxCompletedTasks` |
| Limit briefly exceeded | Groups are optimistic by design (§1) |
| A resource group has no effect at all | The pool never declared it. Registering a group on the scheduler is not enough - add it to the pool's `groups` or the task's `workerGroups` |
| `Missing limit key for group "x"` | The task belongs to a `KeyedRateLimitGroup` but has no `limitKey`, and the group's `keyFrom` returned nothing |
| `Worker pool "x" no longer exists` | A task was waiting on a resource group when its pool was removed from `workerPools`. The library never does this, so something outside it mutated the map |

---

## 10. Recipes

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

**A separate quota per endpoint or tenant**

```typescript
const api = new KeyedRateLimitGroup(
  [{ limit: 10, windowMs: 1000 }],
  { id: 'api', keyFrom: t => t.payload.endpoint }
);
// Each endpoint gets its own 10/sec bucket, and a saturated one does not
// delay tasks belonging to the others
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

## 11. Choosing worker settings

| Workload | Settings |
|---|---|
| Async / IO (HTTP, DB) | `inline: true`, and pick the product: `maxThreads × maxConcurrentTasks` = how many requests may be in flight |
| CPU-bound | `inline: false`, `maxConcurrentTasks: 1`, `maxThreads ≈ core count` |
| Mixed | Separate pools per workload; do not mix in one pool |

Inline workers share the main thread — a CPU-bound inline worker blocks the
scheduler itself. For inline pools, split the product toward `maxThreads` when
you want more isolated worker instances (separate connections, caches), and
toward `maxConcurrentTasks` when one shared instance is fine.
