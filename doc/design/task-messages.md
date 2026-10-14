# Task and worker messages: an event channel from workers

**Status: open.** Answers fyflow-app "scheduler pass 1 #2" (a log channel from
workers). Tracked in `TODO_CHECKLIST.md`. Delete this file when it ships; move
the contract to `AGENTS.md` first.

**Builds on** `task.attemptNumber`, now shipped (`AGENTS.md` §5), and on the
known issue "A requeued task can be settled by its abandoned run" (the
per-dispatch token and wrappers forgetting requeued tasks), without which a
requeue produces two interleaved streams for one attempt.

## The request, and why it is answered differently

fyflow-app asked for `context.log(level, message, data?)` surfacing as a
`task.log` event with a per-task sequence number and a bound on volume.

The transport they need is general: a worker sends something, and the host
receives it attributed to the task or worker that sent it. Logging is one use of
that. If the scheduler defines it as "log", it also has to define levels and
their names, what `data` means, and how the result is formatted. Those are the
app's concerns, and any second use (metrics, partial results, model-loading
status) would then need its own API.

So workers get a named event channel, and a log is one name on it, in a shape
the app chooses:

```typescript
// worker, inside run()
context?.emit('log', { level: 'info', message: 'fetched 120 rows' });

// host
scheduler.addEventListener('task.message', (e) => {
  if (e.detail.name === 'log') appendLog(e.detail.taskId, e.detail.data);
});
```

## Decisions

| Decision | Choice | Why |
|---|---|---|
| Names | `emit()`, `task.message`, `worker.message` | `emit` is the familiar verb; `message` does not read like a scheduler event |
| Log shortcut | **None.** `AGENTS.md` gets a log recipe | A `context.log()` would fix level names in the public API |
| Events with no task (from the constructor, `setup()`, `teardown()`) | **Ship now**, as `worker.message` on the pool | Model-loading progress is the obvious use, and the protocol work is shared |
| Nobody listening | **Not sent.** The worker is told, and `emit()` does no work | A worker that logs heavily costs nothing until someone listens |
| `task.progress` | Unchanged | Last-value-wins, with its own established event; moving it would change its timing |
| Implementation | **One shared module, `core/messageChannel.ts`**, used by both worker wrappers and `InlineWrapper` | Gate, buffers, cap, clone, flush and per-attempt closing would otherwise be written three times and drift. The wrapper files stay separate, as decided |

## API

### Worker side

The context you call `emit()` on decides the attribution:

```typescript
interface BaseWorkerContext {
  // ...existing members
  /** Worker-level event -> `worker.message` on the pool. */
  emit: (name: string, data?: unknown) => void;
}

interface TaskWorkerContext extends BaseWorkerContext {
  // ...existing members
  /** Task-level event, for this attempt -> `task.message` on the scheduler. */
  emit: (name: string, data?: unknown) => void;
}
```

```typescript
export default class ModelWorker extends BaseWorker {
  async setup() {
    this.workerContext?.emit('loading', { stage: 'weights', fraction: 0.4 });  // worker.message
  }
  async run(payload: any, context?: TaskWorkerContext) {
    context?.emit('log', { level: 'debug', message: 'tokenised' });           // task.message
    this.workerContext?.emit('cache', { hits: 12 });                          // worker.message
  }
}
```

Both are fire-and-forget: they return at once, never throw, and deliver
asynchronously. A non-string `name` is converted with `String()` rather than
rejected, since `emit()` must not throw.

Adding a required member to both context interfaces is source-breaking for a
consumer who builds a context object literal, typically to unit-test a worker's
`run()` directly. The release notes must say so.

### `task.message`, on the scheduler only

```typescript
interface TaskMessageDetail {
  taskId:        string;
  workerId:      string;
  attemptNumber: number;   // task.attemptNumber, 1-based
  name:          string;
  data:          unknown;  // a snapshot taken at emit(), see Cloning
  seq:           number;   // per attempt, from 0: the index of the emit() call
  timestamp:     number;   // Date.now() at emit(), in the worker
  dropped?:      number;   // events discarded at the cap immediately before this one
  error?:        string;   // set, with data undefined, when data could not be cloned
}
```

**No `workerType`.** On `task.progress`, which fires on the same target for the
same task, `workerType` means `'inline'`/`'thread'`; on `resource.*` it means
the pool key. `AGENTS.md` already warns about that collision. A second task
stream using the field with either meaning would make it worse, and the pool is
one lookup away: `scheduler.tasks.get(taskId).workerType`.

Scheduler-only, like `resource.*`. The pool does not dispatch it: the scheduler
receives it through a notifier injected into the pool, the same mechanism
`_bindResidentGroups` uses for resident holdings. Without that, a consumer
listening to both the scheduler and the pool would see every message twice, and
the scheduler's own forwarding listener would count as a subscriber and keep the
gate open (see Gating). `AGENTS.md` §6 should note the asymmetry: `task.progress`
fires on pools and the scheduler, `task.message` only on the scheduler.

### `worker.message`, on the `WorkerManager`

```typescript
interface WorkerMessageDetail {
  workerId:   string;
  workerType: 'inline' | 'thread';  // as on every other worker.* event
  name:       string;
  data:       unknown;
  seq:        number;               // per worker instance, from 0
  timestamp:  number;
  dropped?:   number;
  error?:     string;
}
```

On the pool, because that is where the whole `worker.*` family lives and the
scheduler re-emits none of it. `workerType` keeps the meaning it has on every
other `worker.*` event.

### One event per scope, not one per name

Not `task.message.log`, `task.message.metrics`, and so on, because:

- **No collisions.** A worker naming an event `completed` cannot be mistaken for
  a scheduler event, whatever prefix scheme is used.
- **A closed event list.** `AGENTS.md` §6 lists every event and the docs suite
  checks it. A namespace the worker controls cannot be listed.
- **Gating stays simple.** "Is anyone listening" is one question per scope.

Both details are **fresh objects per event**, like `resource.*`, not the live
task. Messages are a stream a consumer keeps, such as an appended log, which is
the rule `AGENTS.md` §6 states: lifecycle transitions are the task, streams are
fresh objects.

## Delivery guarantees

These are the contract, and each gets a test.

**Task messages**

1. **In order, per attempt.** Delivered events have increasing `seq`. A gap is
   explained by `dropped` (the cap), or by the gate being closed when those
   emits happened (see Gating).
2. **Before settlement.** Every event an attempt emitted before `run()` returned
   or threw, while the gate was open, is dispatched before that attempt's
   `task.completed`, `task.failed`, `task.retrying` (#4) or `task.cancelled`
   (#1). A consumer can treat a settled attempt's log as complete.
3. **Nothing after settlement.** `emit()` on an attempt's context after that
   attempt settled (a stray promise in the worker) is discarded.

**Worker messages**

4. **In order, per worker instance.**
5. **Before the lifecycle event that follows.** Events emitted in the
   constructor or `setup()` arrive before `worker.setup.completed` /
   `worker.setup.failed`; events emitted in `teardown()` arrive before
   `worker.teardown.completed` / `worker.teardown.failed`.
6. **Nothing after teardown.**

**Both**

7. **Same behaviour inline and threaded.** See Parity.

**Unavoidable losses**, where something still buffered in a worker never
arrives: a worker crash or `onerror`; a threaded worker that does not acknowledge
teardown in time and is terminated (reported as `worker.teardown.failed`); a
forced stop after a cancel's grace period (#1); and the Node worker's
`process.exit` after a termination request, which is safe only because the
worker flushes before posting the error that precedes it.

Not guaranteed: ordering relative to `task.progress`, which uses its own path;
ordering between tasks; ordering between a task's messages and its worker's
messages.

### How the ordering guarantees are enforced

- **A port is FIFO.** The worker flushes its buffers before posting any message
  that ends something: `result`, `error`, `setup_completed`, `setup_failed`,
  `init`, and the teardown acknowledgement. Nothing emitted before it can arrive
  after it.
- **Inline settles synchronously, so it flushes earlier.** On an inline pool a
  failure settles the task from the wrapper's `task.failed` event, dispatched
  inside `runTask` (`inlineWrapper.ts`, the `catch` block), before the promise
  rejects. The flush must therefore come before the wrapper dispatches
  `task.completed` or `task.failed`, not merely before it resolves. Likewise
  before each lifecycle event.
- **An attempt's context closes at settle.** After `run()` returns or throws,
  `emit()` on that context is a no-op. A stray promise cannot reach the buffer.
- **Attempt identity comes from the dispatch token, not from the task id.** A
  task id is *not* guaranteed to run at most once at a time: after a requeue, the
  abandoned run keeps executing while the new attempt starts elsewhere.
  Reproduced on an inline pool; on a threaded pool the window is the 500 ms
  teardown handshake. The known-issue fix makes the wrapper forget a task at the
  moment it is requeued off it, so the host drops that run's messages
  (`callbacks.has(taskId)` in `ThreadWrapper`, `runningTaskIds` in
  `InlineWrapper`), and the scheduler's per-dispatch token makes any late
  outcome stale. Without that fix, the abandoned run and the new attempt would
  deliver two interleaved streams with the same `attemptNumber`, both starting
  at `seq` 0.

`task.progress` has none of this: `case 'progress'` dispatches whatever arrives,
with no running check. Deliberately left unchanged here.

### What `attemptNumber` is

`task.attemptNumber` as defined in `AGENTS.md` §5: 1-based,
set at dispatch, counting attempts spent before a Resume, and repeated by a
requeue. The scheduler copies it at re-emit, which is safe because of guarantee
2: every event of an attempt arrives before that attempt settles, and
`attemptNumber` changes only at the next dispatch.

## Gating: nothing is sent while nobody listens

Each worker holds two flags, `taskMessages` and `workerMessages`. While a flag is
off, `emit()` in that scope increments `seq` and returns: no clone, no buffer, no
`postMessage`.

**Who decides each flag**

- `taskMessages` is on while the **scheduler** has at least one external
  `task.message` listener. The scheduler tracks this the way it tracks
  `resource.*` subscribers today: a `Set` per event, so removing a listener that
  was never added cannot switch delivery off for one that is still there. On a
  change between none and some, it tells every pool.
- `workerMessages` is on while the **pool** has at least one `worker.message`
  listener. `WorkerManager` already overrides `addEventListener` to track
  listeners for cleanup, but has no matching `removeEventListener`; it needs one
  with the same `Set` semantics.

**How it reaches the worker**

- The pool keeps the current pair of flags and applies it to every wrapper:
  existing ones when it changes, new ones at creation.
- Inline: the wrapper reads the flags directly, so a change takes effect at once.
- Threaded: the wrapper sends a `gate` message carrying **both** flags, and the
  `init` message carries them too. Sending the full state each time makes the
  message idempotent, so order and duplicates do not matter. `init` applies its
  flags synchronously, before its first `await`, so a later `gate` message
  cannot be overwritten by an older `init`.
- The worker handles `gate` before its existing action dispatch, independently
  of whether a worker instance exists. Today an unrecognised or out-of-state
  action throws `Invalid action` (`workerWrapper.ts`, the final `else`), and a
  `gate` arriving after teardown, or before `init` completes, must not.

**What a consumer sees**

- A listener added before tasks are added sees everything.
- A listener added mid-run sees events from the moment the gate opens. On a
  threaded pool that is one message hop later, so emits in between are not sent.
  Because gated emits still use up a `seq`, a late subscriber's first event
  shows how many it missed (`seq: 57` means 57 earlier emits in that attempt).
- `dropped` counts only events discarded at the cap, never gated ones. "Nobody
  was listening" and "too many to send" are different facts.
- Over-counting (`{ once: true }`, or the same listener added twice) leaves the
  gate open with nobody listening. That only costs work nobody reads, which is
  the safe direction, and is the same trade `resource.*` makes.

`AGENTS.md` should say plainly: subscribe before adding tasks.

## Transport and bounds

The flood the app worries about is main-thread work: one `postMessage`, one
deserialisation and one `dispatchEvent` per event. So, when the gate is open:

- **Batched in the worker.** `emit()` appends to a buffer: one per running
  attempt, plus one for the worker itself. A flush sends one `messages` protocol
  message carrying every buffer's contents.
- **Flushed by time, checked in `emit()` itself.** A timer alone is not enough:
  a threaded worker in a synchronous CPU loop, exactly the case threaded pools
  exist for, never yields, so its timer never fires. Each `emit()` therefore
  compares the time since the last flush with `flushIntervalMs` and flushes in
  line when it has elapsed. A timer, armed only while something is buffered,
  flushes the trailing partial batch. An idle worker has no timer, and an inline
  pool leaves nothing on the main event loop.
- **Bounded per buffer.** Each buffer has a cap. Once it is full, further emits
  are discarded and counted until the next flush empties it, and the count
  travels on that buffer's next delivered event as `dropped`. Because the buffer
  empties at every flush, and the flush happens by time even inside a
  synchronous loop, this is a rate bound (at most `cap` per buffer per flush
  interval), not a lifetime one.
- **Always flushed before an ending message**, as described under Delivery.
- **Defaults are measured, not guessed.** Configured per pool, as
  `messages: { flushIntervalMs, maxBuffered }` on `WorkerManager`, with defaults
  chosen from a benchmark of a deliberately chatty worker, synchronous and
  asynchronous.

Dispatch on the host is not throttled. The bound is on what a worker can send,
which is the one side a consumer cannot control.

## Cloning: at `emit()`, not at the flush

On a threaded pool, `data` crosses a thread boundary and must survive
`structuredClone`. `postMessage` clones when it sends, which with batching is at
the flush, not at `emit()`. That leaves two problems:

- **The value sent would not be the value emitted.** A worker that emits an
  object and then mutates it before the flush would deliver the mutated version.
  Inline, with no clone at all, the host would see every later mutation. A log
  entry has to describe the moment it was written.
- **A clone failure would surface in the wrong place.** A `DataCloneError` thrown
  by the flush loses the whole batch, and by then the call that caused it has
  returned.

So `emit()` calls `structuredClone(data)` itself, on both pool types, once the
gate and cap checks have passed:

- The buffer holds snapshots, so mutating after `emit()` has no effect.
- A clone failure is caught at the call that caused it. That event is delivered
  as a marker, with `data` undefined and `error` set to the message, at its own
  `seq`. **`emit()` never throws** into user code.
- The flush cannot fail on cloning, so it needs no recovery path.
- Gated and over-cap emits return before cloning, so neither costs a clone.

**Cost.** Measured at about 2.1 µs per clone of a small log object on Deno
2.9.6. On an inline pool that is main-thread time: 10,000 emits per second costs
about 2% of the main thread, which is the price of parity and is paid only while
someone listens. Threaded pools clone twice, once at `emit()` and once by
`postMessage`, but in the worker. Sending the batch as a transferable could
remove the second clone, but only if a measurement shows the need.

## Parity between inline and threaded

The inline wrapper uses the same gate, buffers, flush, cap and clone, from the
same module, so a pool switching between `inline: true` and `inline: false`
behaves the same. Without the clone, an inline consumer would get the worker's
own object by reference and could come to depend on it: on mutations made after
`emit()`, or on values (functions, class instances) that a threaded pool could
not send. The lifecycle-event work in 2026 is the precedent: inline and threaded
pools emitting different things was treated as a bug.

The asynchronous delivery also stops a host listener from running synchronously
inside the worker's `run()`, which would otherwise be possible inline.

## The shared module

`core/messageChannel.ts` holds everything that is the same in all three places:
the two gate flags, the per-attempt and per-worker buffers, `seq`, the cap and
`dropped` accounting, the clone with its marker fallback, the time check and the
trailing timer, and closing an attempt. It is runtime-neutral: it takes a `send`
function and knows nothing about `postMessage`, `parentPort` or `EventTarget`.
Each wrapper constructs one and wires `send` to its own transport: the Deno and
browser worker to `self.postMessage`, the Node worker to `parentPort.postMessage`,
`InlineWrapper` to its own event dispatch.

This keeps the decision that the two worker wrapper files stay separate: the
runtime differences stay in them; only the logic that has none moves out.

**Publishing.** It is a new published file, so it needs its own `!core/...` line
in the `deno.json` `publish.exclude` allowlist, and it is reachable from the
worker wrappers, which are themselves outside the module graph. `deno task
jsr:smoke` runs a real threaded worker against the staged package, so a missing
allowlist line fails there rather than for consumers. On the npm side, esbuild
inlines it into each worker bundle.

## Where it lands

| File | Change |
|---|---|
| `core/messageChannel.ts` | **new**: the shared logic above |
| `core/workerInterface.ts` | `emit` on both contexts; `'messages'` in `WorkerMessage.type`; the two detail types |
| `core/workerWrapper.ts` | construct the channel; `gate` handled before the action dispatch; flush before ending messages; close the attempt context (Deno, browser) |
| `core/workerWrapper.node.ts` | the same, separately (Node) |
| `core/threadWrapper.ts` | `case 'messages'`: split into task and worker events, drop tasks not running here; `gate` in `init` and on change |
| `core/inlineWrapper.ts` | construct the channel; `emit` on both contexts it builds; flush before each `task.*` and lifecycle dispatch |
| `core/workerManager.ts` | flag state and propagation; `removeEventListener` override; the `messages` option; dispatch `worker.message`; hand task messages to the scheduler's notifier |
| `core/FyflowScheduler.ts` | `task.message` subscriber tracking and the switch to pools; inject the notifier; re-emit with `attemptNumber` |
| `deno.json` | allowlist line for `core/messageChannel.ts` |
| `index.ts` | export `TaskMessageDetail` and `WorkerMessageDetail` with `export type` |
| `AGENTS.md` | §4 worker example; §5 export list and the `messages` option; §6 both events, "scheduler-only", "subscribe before adding tasks"; a log recipe in §10 |

Roughly 350–450 lines with the shared module, against an estimated 450–600
written three times.

## Tests

Each runs on both pool types unless it says otherwise. The threaded runs are what
exercise `workerWrapper.node.ts` under `pnpm test` and `workerWrapper.ts` under
`deno task test`.

- **Order and `seq`** for both scopes.
- **Before settlement**: a worker emitting immediately before returning, and one
  emitting immediately before throwing. Mutation checks: remove the flush before
  `result` (threaded), and move the inline flush after the wrapper's `task.failed`
  dispatch; each must fail.
- **Before the lifecycle event**: emit in `setup()` arrives before
  `worker.setup.completed`, and in a throwing `setup()` before
  `worker.setup.failed`; emit in `teardown()` arrives before its terminal event.
- **After settlement and after teardown**: discarded. Mutation check: remove the
  context-closed flag and the test must fail.
- **Requeue**: a co-tenant requeued off a self-terminating worker produces one
  stream per attempt; the abandoned run's emits are not delivered. Depends on the
  known-issue fix.
- **Synchronous CPU loop**: a threaded worker emitting in a loop that never
  yields delivers more than `maxBuffered` events within one `run()`, flushed by
  time. Mutation check: remove the time check in `emit()` and it must fail.
- **Cap**: a worker emitting well past it; delivered events plus `dropped` equals
  emits made while the gate was open, and the `seq` gaps match.
- **Gate closed**: with no listener, a threaded worker posts no `messages`
  protocol messages at all, counted at the wrapper. Mutation check: force the
  flag on and the count must be non-zero.
- **Gate opening mid-run**: events start arriving, the first `seq` reflects
  earlier emits, and no `dropped` is reported for them.
- **Gate bookkeeping**: removing a listener that was never added does not close
  the gate; removing the last real one does. A `gate` message after teardown and
  before `init` completes does not throw in the worker.
- **No double delivery**: listening on both the scheduler and the pool yields each
  task message once, and the scheduler's internal wiring does not hold the gate
  open.
- **Cloning**: mutating after `emit()` changes nothing (mutation check: move the
  clone to the flush, and the threaded case must fail); uncloneable `data` gives
  a marker at its own `seq`; a non-string `name` is converted; neither throws in
  the worker.
- **Retry**: `seq` restarts and `attemptNumber` advances.
- **Parity**: the same worker, inline and threaded, produces the same details
  apart from `workerId`, `timestamp` and `workerType` on `worker.message`.
- **Shutdown**: messages arriving during `shutdown()`'s drain are delivered or
  dropped consistently, never thrown on.
- **Browser spec**, since the browser runs `workerWrapper.ts` under a different
  host.
- **Docs suite**: the new §6 rows, the worker example and the log recipe.
- **`jsr:smoke`**: passes with the new module, and fails with its allowlist line
  removed.
- **Benchmark**: `benchmark:quick` before and after, run back to back, since
  every task's context gains a member and every worker gains flag checks. Plus a
  chatty-worker scenario, gated and ungated, synchronous and asynchronous, used
  to choose the defaults.

## Open questions

1. **Default flush interval and cap.** To be chosen from the benchmark above.
