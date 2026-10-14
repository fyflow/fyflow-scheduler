# Task cancellation

**Status: open.** Answers fyflow-app "scheduler pass 1 #1". Tracked in
`TODO_CHECKLIST.md`. Also fixes the `scheduler.completed` known issue, which
cancellation would otherwise make common. Delete this file when it ships; move
the contract to `AGENTS.md` first.

**Builds on** attempt accounting, now shipped (`attemptNumber`, `task.retrying`,
and the backoff wait in `retryWaiting`; contract in `AGENTS.md` §5 and §6), and
on three known issues in `TODO_CHECKLIST.md`:

- "A requeued task can be settled by its abandoned run": the per-dispatch token
  (`task._dispatchToken`) and the wrappers' `forgetTask()`. Cancel wins only if a
  stale outcome cannot settle the task first. That fix deliberately stopped at
  forgetting: rejecting a forgotten task's pending promise is added here, where
  the forced stop first needs it.
- "Ready and blocked queues are quadratic above ~16K tasks": the head-indexed
  queue that constant-time cancel is built on.
- "A listener re-entering the dispatch loop can lose a task": the rule that every
  state and queue transition completes before an event is dispatched, which this
  plan extends to three more sites.

Task messages (#2) have shipped; `AGENTS.md` §6 is their contract, and
"Interplay" below says what cancelling means for them.

## The request

A user presses Cancel on a Run and expects work to stop. fyflow-app asks for
`scheduler.cancel(taskId, reason?)` that:

- removes a queued or blocked task so it never dispatches;
- signals a running task's worker, ideally with an `AbortSignal` on the context;
- may tear the worker down after a grace period;
- ends in a distinct `task.cancelled` (not `task.failed`, so no failure edge and
  no retry) carrying the reason;
- frees the task's resource-group holdings like any settlement.

Today `AGENTS.md` §1 says "Cancelling a queued task: Not implemented", and a
running worker cannot be told to stop. Orchestration's stopgap swaps later
dispatches for failing tasks, and tasks already running still finish.

## Decisions

| Decision | Choice | Why |
|---|---|---|
| Promise of a cancelled task | **Rejects with a new `CancelledError`**, optional or not | Cancel is the caller's own act, not a failure, so `optional` does not apply. `handleRejection` still silences fire-and-forget tasks |
| While cancelling | **`state` stays `'running'`**; `task.cancellation` is the marker. Only the terminal `'cancelled'` is a new state | Code that reads `'running'` as "in progress", in this repo and in consumers, stays correct. Guards such as the requeue listener's `state === 'running'` need no change |
| Inline worker ignoring the signal | **Keep waiting.** The task stays cancelling until `run()` returns, holding its resources | Nothing is handed out while still in use. Inline is for IO, where honouring the signal is usually one argument: `fetch(url, { signal })` |
| Cancel versus a late outcome | **Cancel wins.** Once accepted, the only terminal event is `task.cancelled`; a late result or error is discarded, with no retry and no requeue | `cancel()` resolving `true` means cancelled. Every caller would otherwise handle two outcomes |
| Running tasks | **Two events**: `task.cancelling` at the request, `task.cancelled` when the attempt has stopped | Resources are released at `task.cancelled`. Releasing at the request would hand a GPU or licence seat to the next task while the cancelled one still uses it |
| Forced stop | **Threaded pools only**, after a grace period; the task settles only **after the thread is dead** | An inline `run()` cannot be interrupted. Settling before the kill would release resources the stuck thread still holds |
| Other tasks on a force-stopped worker | **Always requeued**, whatever `requeueFailedTasks` says | They did nothing wrong; `requeueFailedTasks` is about worker failure, and this is not one |
| Restart budget | A forced stop **does not count** against `maxWorkerRestarts` | `poolRestartCount` never resets, so three cancels would otherwise end a pool's ability to replace workers |
| Grace period | `cancelGraceMs` per pool, **default 2000**; per-call `graceMs`; a repeat call can only shorten it | See "Grace period" |
| Cost per `cancel()` | **Constant**, using tombstones in head-indexed queues; no bulk API | See "Constant time" |
| Worker failure while cancelling | The task settles **when its attempt has actually stopped**, as for a forced stop, not at the failure | Otherwise a co-tenant's `terminateWithError` would settle a cancelling task, and release its resources, while its `run()` still executes |
| Re-adding a cancelled task object | `addTask` **throws** for an object whose `state` is `'cancelled'`; create a new `FyflowTask` | Its tombstone may still sit in a queue, so re-queueing the same object would either be skipped as a tombstone or revive the stale entry. A deterministic rule beats one that depends on whether the tombstone has been passed yet |
| Spawns from a cancelled parent | **Refused**: from a task whose cancel was accepted, or that ended `cancelled` | Requested scope only. Spawns from parents that ended any other way keep today's behaviour |
| `context.signal` | **Created eagerly** with each attempt's context | `new AbortController()` measured at about 28 ns on Deno 2.9.6, about 0.17% at 60,000 tasks/s. A lazy getter would need a "cancelled before first access" record for nothing |
| Event details | The live task, like every lifecycle transition | The rule stated in `AGENTS.md` §6 |
| Deferred | `descendants: true`, and groundwork for pause | Neither was requested; see "Deferred" |

## Where a task can be when cancel arrives

| # | Where | `state` | `stats` it is counted in | Holds task-held resources | How it is cancelled |
|---|---|---|---|---|---|
| 1 | Ready queue | `pending` | `queued` | no | tombstoned |
| 2 | A blocked queue (exactly one, see below) | `pending` | **none** | no | tombstoned, with `resource.unblocked` `'cancelled'` |
| 3 | Backoff wait (`retryWaiting`, from #4) | `pending` | `queued` | no | timer cleared, entry removed |
| 4 | Between `_dispatchTask` and the wrapper: `enqueueNoQueue` awaiting a fresh `ThreadWrapper` | `running` | `running` | yes | the pool does not call `runTask` |
| 5 | `ThreadWrapper.taskQueue`, waiting for the thread to initialise | `running` | `running` | yes | removed from `taskQueue` **and its promise rejected** |
| 6 | `InlineWrapper.runTask`, awaiting `_createWorkerInstance` | `running` | `running` | yes | `run()` is never called |
| 7 | Inside `run()` | `running` | `running` | yes | the signal; on a threaded pool, a forced stop after the grace period |

Notes on the table, each checked against the code:

- **A blocked task is in exactly one blocked queue.** There are two places that
  push onto one (`_checkAndBlockResident` and `_checkAndBlockResources`); each
  pushes onto a single queue and returns, and the dispatch loop shifts the task
  out of its ready queue. A blocked task only ever leaves for the ready queue,
  or stays where it is. So the `'superseded'` branch in
  `_retryBlockedTasksForGroup` (removing a task from its *other* blocked queues)
  is unreachable, and no test exercises it. Cancellation relies on the
  single-queue fact; removing the dead branch is a separate cleanup.
- **A blocked task is not in `stats.queued`.** It gave up that count when it left
  the ready queue (the comment above `_hasBlockedTasks`, and `AGENTS.md`: "a
  blocked task is removed from `queued`"). Cancelling one must not decrement
  `queued`; places 1 and 3 must.
- **Place 4 lasts one microtask.** A fresh `ThreadWrapper` reports
  `canAcceptTask() === true` until `_ensureInitialized` starts, so the poll in
  `enqueueNoQueue` resolves at once; only its `await` separates dispatch from
  `runTask`. It is reachable by a `cancel()` in the same turn as the dispatch, for
  example `addTask(t); cancel(t.id)`, which is common enough to need its own
  test. Waiting for initialisation is place 5.
- **Place 5 must reject.** Removing the entry from `taskQueue` is not enough: the
  promise `runTask` returned would never settle. `terminate()` does not clear
  `taskQueue` either.
- **`task.running` timing differs by pool type.** On an inline pool it fires at
  the start of `runTask`, before the worker instance exists, so place 6 has
  already emitted it. On a threaded pool, places 4 and 5 have not. So
  `task.cancelling` can arrive without a `task.running` before it.

Places 1–3 have nothing in flight, so cancel completes synchronously. Places 4–7
have been dispatched, so the scheduler hands the cancel to the pool and waits for
the attempt to end. It does not need to know which of 4–7 applies; the pool does.

One existing path must change. When `_dispatchTask` loses the capacity race
(`'Worker at maximum concurrent task capacity'`), it puts the task back on the
ready queue. With a cancel marker set, it settles the task as cancelled instead.
Once the pool checks for a cancel before calling `runTask` (place 4), this
branch is reachable only in the microtask between `runTask` rejecting and the
`.catch` running, so its test uses a stub pool rather than a real race.

## Constant time per `cancel()`

Cancelling a Run means cancelling many tasks in a loop, so the per-call cost
decides whether that loop is usable. Removing each task from its array as it is
cancelled (`indexOf` plus `splice`) is linear per call and quadratic per loop.
Measured on Deno 2.9.6, cancelling every task of one ready queue:

| Queued tasks | `splice` per cancel |
|---:|---:|
| 10,000 | 15–115 ms |
| 50,000 | 0.7–1.9 s |
| 100,000 | 2.3–21.7 s |

The range is cancel order (newest first, oldest first, random).

Two cheaper-looking designs were measured and rejected, and both failures shape
the one below:

- **Deferred compaction** (mark, then remove all marked tasks in one pass before
  any reader runs) measured 11.7 s for 50,000 cancels, worse than `splice`. Each
  cancel's own settle path checked completion and ran the dispatch loop, so the
  whole queue was compacted once per cancel.
- **Tombstones dropped with `shift()`** measured 3.3 s for 50,000 cancels in
  creation order. V8 makes `shift()` effectively free only below about 16,000
  elements; above that it copies the array on every call (0.1 µs per call at
  15,000, 79 µs at 17,000, 232 µs at 50,000). That cliff already costs every
  dispatch from a large queue today; it is logged as a known issue and is fixed
  first.

The design:

- **Queues are head-indexed.** The known-issue fix replaces the ready and blocked
  arrays with one small internal class, `TaskQueue` (`core/taskQueue.ts`): an
  array, a head index, and a live count. `push` appends; taking the head advances
  the index; `unshift` (the head-of-line and `stillBlocked` paths) steps it back
  when there is room; the array is trimmed when the head passes half its length,
  which is amortised constant time. Nothing in this class is about
  cancellation, so it is not part of this plan's line count.
- **Where a task is waiting is recorded on it.** An internal
  `_queuedIn: 'ready' | 'blocked' | 'backoff' | undefined`, plus the blocked
  queue id, updated at every move. `cancel()` reads it instead of searching, and
  it replaces the two `!queue.includes(task)` guards before a blocked push, which
  are linear today. The move sites, all of them:
  - in: `addTask`, `addTasks`, the two blocked pushes, the move back to ready in
    `_retryBlockedTasksForGroup`, the requeue push, the capacity-race push, the
    backoff map in and out;
  - out: the dispatch loop taking a task, `_retryBlockedTasksForGroup`'s logical
    exit (where `'admitted'` is emitted, not at its `shift()`), the orphan exit,
    `shutdown()` discarding the queues, and the dead `'superseded'` splice, if it
    is kept.
- **A cancelled undispatched task is a tombstone.** It is settled at once and
  left where it is. `TaskQueue` skips tombstones when it hands out its head, so
  every reader that takes a task (`_dispatchLoop`, `_retryBlockedTasksForGroup`)
  skips them without code of its own. Readers that look at whole queues filter
  them: `getAdmissionQueue`, which recomputes `position` from live entries, and
  `shutdown()`'s `'shutdown'` unblock loop, which would otherwise emit a second
  exit for a task that already had `'cancelled'` and drive a consumer's fold
  negative. `_retryBlockedTasksForGroup` also re-checks the tasks it has already
  collected into `tasksToRetry` and `stillBlocked` before moving them, because a
  listener can cancel one after the loop picked it up.
- **No compaction pass.** With a head index, a tombstone costs nothing until the
  head reaches it, and memory is bounded by the queue's size before cancelling.
- **Live counts answer the scans.** `_hasBlockedTasks` becomes a check of a
  global blocked count (an improvement: it currently iterates every queue). The
  "any ready tasks" checks in `_dispatchLoop` and `_schedulePeriodicRetry` read
  live counts. A new waiter's `queuePosition` is the live count, not the array
  length. `shutdown()` resets all of them.
- **Cancelling an undispatched task does no dispatch work.** It frees nothing
  (places 1–3 hold no resources), so it does not run the dispatch loop or retry
  blocked tasks. It only fires `scheduler.completed` if this was the last
  outstanding task. Running the full `_checkCompletion` per cancel would cost a
  pass over every group and key each time (its third branch calls
  `_retryBlockedTasks` when nothing runs and something is queued): linear per
  cancel again, worst with a keyed group of many keys.

So a cancelled task is never dispatched, never counted, never in an admission
view and never in a `queuePosition`, and a loop over `cancel()` is linear in
total. There is no bulk API, which would leave the plain loop, the obvious way
to cancel a Run, as the slow path.

The scale test must go through the real `cancel()`, in creation order and in
reverse, with a blocked queue, a keyed group with many keys, and a
`task.cancelled` listener attached. Each earlier design looked constant in
isolation and was not.

## Transitions complete before events

A listener can call `cancel()` from any event the scheduler emits, including
events emitted halfway through moving a task. The known issue "A listener
re-entering the dispatch loop can lose a task" shows this already loses tasks
today, through `addTask`. The rule this plan relies on: **a task's state, its
queue membership, `_queuedIn` and the counters are all updated before any event
about it is dispatched**, and code that resumes after an event re-checks the
cancel marker. Four sites break that rule today:

| Site | What is emitted mid-transition | Change |
|---|---|---|
| `_dispatchLoop` blocking a task | `resource.blocked`, before the ready queue's `shift()` and before the `stats.queued` decrement | Take the task out of the ready queue and decrement first, then call the blocking functions (the known-issue fix) |
| `_dispatchTask` | `resource.acquired`, while the task is still `pending` and counted in `queued`, before `enqueueNoQueue` | Set `state`, move the counters and `_queuedIn` first; check the cancel marker immediately before `enqueueNoQueue`, and if set, settle as cancelled instead of handing the task to the pool |
| `_settleTask` | `resource.released`, plus whatever `_retryBlockedTasksForGroup` dispatches, before the branch (complete, retry, requeue) is chosen | Check the cancel marker **after** the release, not only at the top, so a cancel accepted during the release still wins |
| The capacity-race `.catch` in `_dispatchTask` | `resource.released`, before the task is pushed back on the ready queue | The same: check the marker after the release, and settle as cancelled if set |

Without the `_dispatchTask` change, a cancel from a `resource.acquired` listener
has no correct outcome: treated as place 1 it double-decrements and the task
dispatches anyway; treated as running, the pool has not been given the task and
the cancel waits forever. Without the `_settleTask` change, a cancel from a
`resource.released` listener is accepted, emits `task.cancelling`, and is then
ignored, so the task completes, is requeued, or runs another attempt with the
marker set.

## API

### `scheduler.cancel(taskId, options?)`

```typescript
interface CancelOptions {
  /** Carried on the task and the CancelledError. */
  reason?: unknown;
  /** Threaded pools: ms to wait for the worker to stop before forcing it.
   *  Default: the pool's cancelGraceMs. 0 forces at once; Infinity never forces.
   *  Ignored by inline pools, which cannot be forced. */
  graceMs?: number;
}

cancel(taskId: string, options?: CancelOptions): Promise<boolean>;
```

- **Resolves when the task has stopped**: at once for places 1–3, when the
  attempt ends for 4–7.
- **`true`** if the task is cancelled, including when it already was. **`false`**
  if the id is unknown, or the task reached `done`, `failed` or `user_action`
  first. With `maxCompletedTasks` set, a cancelled task that has since been
  evicted is unknown, so a repeat call returns `false`; `AGENTS.md` should say
  so.
- **It does not throw because of task state**, so cancelling every task of a Run
  in a loop needs no guards. An invalid `graceMs` throws synchronously, as
  `addTask` throws for an unknown worker type, because it is a programming
  error, not a state: `TypeError` for a non-number, `RangeError` for a negative
  value or `NaN`, matching `attemptsSpent`. `cancel()` is therefore not declared
  `async`, which would turn that throw into a rejected promise; it returns its
  promise explicitly.
- **A repeat call** on a task already being cancelled returns the same promise.
  Its `graceMs`, if shorter than the time remaining, **shortens** the wait, so
  pressing Cancel a second time escalates. It never lengthens it.
- **`reason` is optional and never makes `cancel()` throw.** It is kept on the
  host as given. For a threaded worker it is also sent with the cancel message;
  if it cannot be cloned, the message is sent without it, and the worker's
  `signal.reason` is the standard `AbortError`.

### On `WorkerManager`

`cancelGraceMs`, default 2000. Used by threaded pools; accepted and ignored by
inline ones. Per-call `graceMs` overrides it. Validated at construction with the
same rules as `graceMs`, like the pool's other options.

**Timer limits.** `setTimeout` treats delays above 2³¹−1 ms, including
`Infinity`, as about 1 ms, and Deno prints a `TimeoutOverflowWarning` (measured).
So `Infinity` and anything above that limit arm **no timer**; otherwise "never
force" would mean "force at once". `WorkerManager.shutdown()` clears every
pending grace timer, so none holds the process open or fires against a worker
that is gone.

### Grace period

2000 ms, derived from the timeouts already in the code:

- **It must leave room inside the shutdown drain.** `shutdown()` waits up to
  5000 ms for running tasks, and a cancelling task counts as running. A forced
  stop then spends up to `TEARDOWN_ACK_TIMEOUT_MS` (500 ms) on the teardown
  handshake, and the task settles only after the thread is dead. 2000 + 500
  leaves half the drain spare; at 5000, cancelling and then shutting down would
  routinely hit the drain limit and log a misleading "could not complete"
  warning.
- **A cooperative worker does not need it.** One that honours the signal stops in
  milliseconds. The grace period covers a worker that has seen the signal and is
  still cleaning up, and the code already allows a worker's whole `teardown()`
  500 ms. 2000 is four times that.
- **The cost of forcing is real**, which is why it is not shorter: other tasks on
  that worker lose their progress, and anything loaded in `setup()` has to be
  loaded again. A pool holding an expensive model should make its worker honour
  the signal or raise `cancelGraceMs`, and `AGENTS.md` should say so.

### Worker side: `context.signal`

```typescript
interface TaskWorkerContext extends BaseWorkerContext {
  // ...existing members
  /** Aborted when the task is cancelled. Pass it on: fetch(url, { signal }). */
  readonly signal: AbortSignal;
}
```

One `AbortController` per attempt, created with the context. A required new
member on a public interface is source-breaking for a consumer who builds a
context literal to test a worker; the release notes must say so (the messages
plan adds `emit` to the same interfaces).

**What a worker sees.** `signal.aborted` becomes `true` and `abort` fires. If no
`reason` was given, or it could not be sent, `signal.reason` is the standard
`AbortError` `DOMException`. What `run()` does next does not matter to the
outcome (cancel wins): returning, throwing an `AbortError` or throwing anything
else all end in `task.cancelled`.

**Inline aborts in a microtask.** Calling `controller.abort()` synchronously
inside `scheduler.cancel()` would run the worker's abort handlers re-entrantly,
in the caller's stack. Deferring it by a microtask matches a threaded pool, where
the abort arrives as a message.

**The `cancel` protocol action** never replies, and both worker wrappers handle
it before their existing action dispatch. Today that dispatch's final branch
throws `Invalid action ... or worker not initialized` and posts an `error`
carrying the message's `taskId`, which here would be the cancelled task's id: a
cancel arriving after the attempt ended, or after teardown, would be reported as
a task failure.

**CPU-bound threaded workers.** A worker thread processes the cancel message on
its event loop, so a synchronous loop that never yields cannot see the signal at
all, and is stopped only by the forced stop. `AGENTS.md` gets a recipe: yield
periodically (`await new Promise(r => setTimeout(r))`) and check
`signal.aborted`. A shared-memory flag readable without yielding would need
`SharedArrayBuffer`, which a browser offers only under cross-origin isolation, so
it is left out.

### `CancelledError`

```typescript
export class CancelledError extends Error {
  readonly name: 'CancelledError';
  readonly taskId: string;
  readonly reason?: unknown;
}
```

A value export, like `WorkerTerminationError`, with its fields assigned in the
constructor (no parameter properties, which Node's type stripping rejects). It is
what the task's promise rejects with, what an `onCompleteDescendants()` wait on a
cancelled root rejects with, and what a refused spawn reports.

### Task state and fields

- `state` gains only the terminal `'cancelled'`. `TERMINAL_STATES` adds it.
- `task.cancellation`, set when a cancel is accepted and otherwise `undefined`:
  `{ reason?, requestedAt, forced }`. `forced` is set at settle: `true` when the
  worker was force-stopped. One field holding an object, allocated only for
  cancelled tasks, rather than three fields on every task. While it is set and
  `state` is `'running'`, the task is being cancelled.
- `task.error` is cleared at cancel. A task cancelled during a retry backoff
  would otherwise end `cancelled` carrying the previous attempt's error.
- `stats` gains `cancelled`, reset by `shutdown()` with the others. Additive:
  existing keys keep their meaning. Blocked tasks remain counted in no `stats`
  key, by existing design, so the public invariant stays "never exceeds the task
  count". Internally there is now a live blocked count, which makes an exact
  invariant testable: `queued + running + done + failed + cancelled + blocked`
  equals the number of tasks added. The tests assert that equality, because
  "never exceeds" cannot see a wrong decrement: the counters are clamped at 0
  with `Math.max`.
- `addTask` and `addTasks` throw for a task object whose `state` is
  `'cancelled'`. Re-adding a task object is allowed today and resets nothing; a
  cancelled object's tombstone may still sit in a queue, where re-queueing the
  same object would either be skipped as a tombstone or revive the stale entry.
  Re-running cancelled work means a new `FyflowTask`.

## Events

| Event | When | Detail |
|---|---|---|
| `task.cancelling` | A cancel was accepted for a dispatched task (places 4–7) | the task: `state: 'running'`, `cancellation` set |
| `task.cancelled` | The task has stopped, or was removed before dispatch | the task, `state: 'cancelled'` |
| `resource.unblocked` | A blocked task was cancelled | as today, `reason: 'cancelled'` |

`task.cancelling` is not emitted for places 1–3: there is no interval to show.
A fold must accept `task.cancelled` without `task.cancelling` before it, and
`task.cancelling` without `task.running` before it (places 4 and 5 on a threaded
pool).

Per task, for a running attempt:

```
task.running            (absent for places 4-5 on a threaded pool)
  task.cancelling
  task.message ...      (still delivered, until the attempt ends)
task.cancelled
```

Never after a cancel is accepted: `task.completed`, `task.failed`,
`task.user_action`, `task.retrying`, or a requeue.

`'cancelled'` is a new member of `ResourceUnblockReason`. That widens a published
union, which breaks an exhaustive `switch` in downstream code. It is the same
call as the `ResourceGroup.type` widening in 0.3.0: release it as a minor and put
the reasoning in the release commit. `AGENTS.md` should say what it already
implies for `ResourceGroup.type`: do not switch exhaustively on the reason, and
treat an unknown one as an exit.

Resource releases need no new reason: `'settled'` already means "the attempt that
held this ended". A forced stop also releases the worker's resident units, with
`'worker-teardown'` as today.

## Settlement

Cancel wins through the existing single settle path. `_settleTask` is where every
attempt outcome lands exactly once: **if the task has a `cancellation`, the
outcome becomes `cancelled`, whatever it was.** A result, an error and a worker
death all reach it. A requeue does too: the requeue listener only acts on
`state === 'running'`, which a cancelling task still has, so it reaches
`_settleTask` and settles as cancelled rather than being stuck. With a
`'cancelling'` state, that guard would have stranded the task forever, with its
resources and its `cancel()` promise.

**The order of checks at its top is fixed**: the per-dispatch token first, then
`_settling`, then the cancel marker. With the marker first, a stale outcome from
an earlier attempt would settle a re-dispatched attempt that is being cancelled
while it is still running. The marker is checked **again after the resource
release** (see "Transitions complete before events"), since a listener on
`resource.released` can cancel mid-settle.

**A worker failure does not settle a cancelling task at once.** Cancel wins
decides the outcome, not the moment. If a cancelling task's worker fails (a
co-tenant's `terminateWithError`, a crash), the task settles as cancelled when
its attempt has actually stopped: on a threaded pool once the failed worker's
thread is dead, on an inline pool when its `run()` returns. Otherwise its
resources would be released while its `run()` still executes, the one thing the
two-event design exists to prevent.

**The guard is the marker, not `_settling`.** `_settling` stays `true` after a
retry or requeue decision until the next dispatch, so every task in a backoff
(place 3), and many in ready and blocked queues, have it set. A cancel path
modelled on `_failOrphanedTask`, which returns early on `_settling`, would
silently do nothing for them. The same early return is an existing bug in
`_failOrphanedTask`, logged separately.

A cancelled settle releases task-held resources, decrements `stats.running`,
increments `stats.cancelled`, sets `state`, clears `error`, rejects the promise
with `CancelledError`, emits `task.cancelled`, settles descendant trackers,
records the terminal task for `maxCompletedTasks`, checks completion, and runs
the dispatch loop.

Places 1–3 must **not** go through `_settleTask`, which decrements
`stats.running` for a task that was only queued. They get a sibling,
`_cancelUndispatched`, which adjusts the counter the task is actually in (`queued`
for places 1 and 3, the internal blocked count for place 2), and which, unlike a
settle, neither runs the dispatch loop nor retries blocked tasks: the task held
nothing, so there is nothing to hand on. It fires `scheduler.completed` only if
this was the last outstanding task. That is what keeps a loop of cancels linear.

`_failOrphanedTask` gets a terminal-state guard in place of its `_settling`
check (the known-issue fix). An `'admitted'` listener can cancel a task that the
retry loop has already set aside as orphaned, and without the guard the orphan
sweep would settle it a second time.

## The forced stop

When the grace period expires on a threaded pool and the attempt is still
running, the pool **retires** that worker. Everything rests on one wrapper
operation, *release*: forget a task, with the existing `forgetTask()` (drop its
callback, so nothing the thread sends can settle it), and keep or reject its
pending promise, which this plan adds. Rejecting needs `forgetTask()` to hand the
promise's `reject` back rather than discard it. The order:

1. **Mark the worker `terminated`**, so it accepts no new tasks and its
   unexpected-exit handler ignores the exit about to be caused. (Nothing sets
   `terminated` today; the `alreadyTerminated` check in `_markWorkerAsFailed` is
   currently dead, and this becomes its first writer.)
2. **Detach every task on it** with *release*, before anything else can arrive.
   `terminate()` clears `callbacks` itself, so detaching afterwards would find
   nothing; and a result the thread posts during the teardown handshake would
   otherwise settle the cancelled task early, with `forced: false`, releasing its
   resources while the thread is still alive. The promises are held, not yet
   rejected.
3. **Reject the co-tenants' promises now** with the internal requeue error, which
   `_dispatchTask`'s `.catch` settles as `{ requeue: true }`, whatever
   `requeueFailedTasks` says. A co-tenant that is itself being cancelled is not a
   co-tenant: it is held with the forced task, and settles with it in step 5.
4. **`terminate()` the worker and await it.** That posts `teardown` and waits up to
   `TEARDOWN_ACK_TIMEOUT_MS` (500 ms): a worker stuck awaiting something can
   still run `teardown()` and release connections; one stuck in a synchronous
   loop cannot, and is killed. On Node, `worker_threads`' `terminate()` returns a
   promise, which `ThreadWrapper.terminate()` currently ignores; it has to await
   it, or "after the thread is dead" is not true there.
5. **Only then settle the cancelled task, or tasks**, by rejecting their held
   promises; cancel wins in `_settleTask`, with `forced: true`. Their task-held
   resources are released here, after the thread is dead.
6. **Remove the worker from `threads` and release its resident units**, also
   after the thread is dead.

No `worker.failed`, no restart-count increment, no replacement scheduled. The
next dispatch creates a worker on demand. Lifecycle events fire as for any
teardown. A worker already being retired is skipped by `WorkerManager.shutdown()`,
which would otherwise terminate it a second time and report a spurious
`worker.teardown.failed` from the second handshake.

Everything settles by promise rather than by event because `shutdown()` removes
the scheduler's pool listeners before its drain. That ordering is itself a known
issue; once it is fixed, the promise route remains the simpler one.

**Grace timers belong to an attempt**, not to a task id. A timer is keyed by the
task and its dispatch token, cleared when the task settles, and checked against
the current token when it fires. Otherwise a stale timer could retire the worker
now running a new task with a reused id, or act on a worker that has already
failed.

## Spawning

A spawn from a task whose cancel has been accepted, or that ended `cancelled`
(a stray promise in the worker), is refused and reported as `task.spawn_failed`
with a `CancelledError`. Today the spawn listener adds the child whatever the
parent's state. Spawns from parents that ended any other way are left as they
are: refusing those too would be a behaviour change nobody asked for.

`onCompleteDescendants()`: a cancelled root rejects with `CancelledError`; a
cancelled descendant does not reject, just as a failed one does not.

## Shutdown

`shutdown()` already waits for running tasks, for up to 5 seconds, and cancelling
tasks count as running, so most cancels complete inside that wait. For those that
do not:

- **A threaded task still cancelling when its pool shuts down** has really
  stopped once `pool.shutdown()` has terminated its worker. It settles as
  `cancelled` with `forced: true`, and its `cancel()` resolves `true`.
- **An inline task still cancelling** cannot be stopped, and may yet end. Its
  `cancel()` resolves `false` when shutdown completes, so no caller waits
  forever. This follows the precedent of `onCompleteDescendants()` waits, which
  shutdown rejects rather than drops.
- **Nothing settles after shutdown.** The promise path survives shutdown: when
  that inline `run()` finally returns, `_dispatchTask`'s `.then` would still call
  `_settleTask` and emit `task.cancelled` after `cancel()` had resolved `false`.
  A scheduler that has shut down ignores late outcomes, through a flag checked
  at the top of `_settleTask` alongside the token.

## `scheduler.completed`, fixed with this

`_checkCompletion` requires `stats.done > 0`. A run whose tasks all fail never
reports draining, and cancelling a whole Run would hit this every time. The
condition becomes "at least one task has reached any terminal state"
(`done + failed + cancelled > 0`). This is observable: `scheduler.completed` now
fires for an all-failed run, where it did not before. Release notes.

## Interplay with the other pass 1 work

- **Messages (#2, shipped)**: a cancelling attempt's messages keep flowing and
  must arrive before `task.cancelled`. Threaded, that holds already: the worker
  flushes before posting its result or error. Inline, `InlineWrapper` delivers
  pending messages synchronously (`_deliverNow()`) before dispatching
  `task.completed`/`task.failed`, so the settle that cancellation adds must come
  after that, not before it. A forced stop loses whatever is still buffered,
  like a crash (`AGENTS.md` §6, "Lost only with the worker"); add it to that
  list. Places 1–3 never ran, so they have no messages.
- **Attempts (#4)**: a cancelled task never emits `task.retrying`. Cancelling
  during a backoff is place 3. `attemptNumber` keeps the number of the attempt
  that was cancelled.

## Deferred

- **`descendants: true`**, cancelling a task's spawn lineage. Not requested:
  orchestration keeps its own map of the tasks it created. Before it is added, two
  things need answers: what it does when the root has already finished (the main
  "cancel the Run" case), and the lineage gap under `maxCompletedTasks`, where
  evicting a terminal child deletes its `spawnedChildren` entry and makes its live
  descendants unreachable. `onCompleteDescendants()` has that gap today.
- **Pause groundwork.** An earlier draft wrote the interrupt with a `'requeue'`
  outcome for a future pause. That is speculative, and keyed on the cancel marker
  it would contradict cancel wins. Pause remains `doc/design/pause-resume.md`,
  which is stale and needs rewriting on its own terms.

## Where it lands

| File | Change |
|---|---|
| `core/FyflowScheduler.ts` | `cancel()`; `_queuedIn`, tombstones, the blocked live count; cancel wins in `_settleTask`, with the check order and the re-check after release; the `resource.acquired` reordering and marker check in `_dispatchTask`; `_cancelUndispatched`; `stats.cancelled`; the terminal state; the re-add rule; spawn refusal; shutdown resolution and the shut-down flag; the completion condition; the capacity-race path; holding a cancelling task across a worker failure |
| `core/workerManager.ts` | `cancelGraceMs`; routing a cancel to the wrapper holding the task; place 4 (a record of enqueues in flight); grace timers keyed by attempt, cleared at settle and in `shutdown()`; retiring a worker without counting a restart, skipped by `shutdown()` |
| `core/threadWrapper.ts` | remove from `taskQueue` and reject; post `cancel`; await Node's `terminate()` promise |
| `core/inlineWrapper.ts` | `signal`; abort in a microtask; a record of tasks cancelled before `run()`, which then never starts |
| `core/workerWrapper.ts` | `cancel` action handled before the action dispatch, never replying; a map of per-attempt controllers, each created by the `run` handler before its first `await` (Deno, browser) |
| `core/workerWrapper.node.ts` | the same, separately (Node) |
| `core/workerInterface.ts` | `CancelledError`; `signal` on `TaskWorkerContext`; `'cancel'` in the protocol |
| `core/resourceEvents.ts` | `'cancelled'` in `ResourceUnblockReason` |
| `index.ts` | export `CancelledError`; `CancelOptions` with `export type` |
| `AGENTS.md` | §1 table row removed; §4 the signal and the CPU-bound recipe; §5 `cancel()`, `cancelGraceMs`, the state, `stats.cancelled`; §6 the two events, the new unblock reason, the live-task rule; §8 forced stop; §10 a "cancel a Run" recipe |

Roughly 700–800 lines of source, plus a large test diff. `TaskQueue`, the
per-dispatch token and the wrapper's *release* operation come from the known-issue
fixes and are not counted here.

New internal state: the `cancellation` marker; a pending `cancel()` promise per
cancelling task; `_queuedIn`; the blocked live count; the shut-down flag; the
pool's grace timers and its record of enqueues in flight (place 4); the inline
wrapper's record of tasks cancelled before `run()` (place 6); and one controller
per attempt, plus a controller map inside each worker thread.

The functions that get harder to read are `_settleTask` (check order, the
re-check, holding across a worker failure), `_dispatchTask` (reordering around
`resource.acquired`, the capacity-race `.catch`), `_retryBlockedTasksForGroup`
(re-checking collected tasks), the spawn listener, `_settleTracker`, and
`shutdown` on both classes. `_dispatchLoop` gets simpler, not harder: tombstone
skipping lives in `TaskQueue`.

## Tests

Every place on both pool types where it exists. Threaded runs exercise
`workerWrapper.node.ts` under `pnpm test` and `workerWrapper.ts` under
`deno task test`.

- **Places 1–3**: the task never dispatches; `task.cancelled` fires without
  `task.cancelling`; the promise rejects with `CancelledError`; the counters move
  the task from the right one (`queued` for 1 and 3, the blocked count for 2).
  Run place 2 with `stats.queued > 0`, so a wrong `queued` decrement is not
  hidden by the clamp at 0. Include a task in backoff and a requeued task in a
  ready queue, both of which have `_settling` set. Mutation check: guard on
  `_settling` and both must fail.
- **Place 2 fold**: `Σ blocked − Σ unblocked === 0` per
  `(groupId, key, holderId)` across a run that cancels blocked tasks, including
  one that then shuts down with tombstones still queued (no `'shutdown'` exit for
  a task that already had `'cancelled'`).
- **Place 4**: `addTask(t); cancel(t.id)` on a threaded pool with no worker yet;
  `runTask` is never called.
- **Places 5 and 6**: dispatched but not yet running; `run()` is never called, the
  attempt settles, and resources are released exactly once.
- **Place 7, cooperative**: a worker awaiting `signal` stops; the events run
  `task.cancelling` then `task.cancelled`.
- **Cancel wins**: a worker that returns a result after the signal still ends
  `cancelled`; one that throws ends `cancelled` with no `task.retrying` even with
  retries left. Mutation check: remove the cancel-wins line in `_settleTask` and
  each must fail.
- **Check order**: a stale outcome from an earlier attempt does not settle a
  re-dispatched attempt that is being cancelled. Mutation check: put the marker
  check before the token check and it must fail.
- **Worker failure while cancelling**, with `requeueFailedTasks: true` and with
  `false`, on both pool types: the task ends `cancelled`, and only once its
  attempt has stopped. On an inline pool, use a co-tenant's `terminateWithError`
  while the cancelling `run()` is still executing: `task.cancelled` must not
  fire, and its resources must stay held, until that `run()` returns.
- **Forced stop**: a threaded worker spinning synchronously is stopped after
  `graceMs` with `forced: true`; its task-held resources are released only after
  the thread is gone; a co-tenant (`maxConcurrentTasks: 2`) is requeued and
  completes, even with `requeueFailedTasks: false`, and its abandoned run's late
  result does not settle it; the pool's restart count is unchanged; the next task
  gets a new worker. Mutation check: route the stop through
  `_handleWorkerFailure` and the restart-count assertion must fail.
- **Forced stop, late result**: a worker that ignores the signal past the grace
  period, then posts a result during the teardown handshake, still ends
  `cancelled` with `forced: true`, settled after the thread is dead. Mutation
  check: detach after `terminate()` instead of before, and it settles early.
- **Two cancelling tasks on one worker**: both settle `cancelled` after the thread
  is dead; neither is requeued.
- **Stale grace timer**: a task cancelled, settled, and then re-added under the
  same id (as a new object) on the same pool is not force-stopped by the first
  attempt's timer.
- **Forced stop during shutdown**: a forced stop, and a co-tenant requeued by it,
  during `shutdown()`'s drain both settle. Until the known issue "`shutdown()`
  stops listening to its pools before draining them" ships, the mutation check is
  to report either by event instead of by promise, and that task must stay
  running until the drain gives up. Once that issue is fixed, the event route
  would pass too; the test then just guards the outcome.
- **Inline keeps waiting**: an inline worker ignoring the signal stays cancelling
  past `graceMs` with its resources held, then ends `cancelled` when `run()`
  returns.
- **Grace edge cases**: `graceMs: Infinity` and `2 ** 31` never force (mutation
  check: arm the timer anyway and the task is forced within milliseconds); a
  repeat `cancel()` with a shorter `graceMs` forces sooner, and with a longer one
  changes nothing; `pool.shutdown()` leaves no grace timer behind.
- **`cancel()` contract**: `true` for an already-cancelled task; `false` for
  unknown, for other terminal states, and for an evicted cancelled task under
  `maxCompletedTasks`; the same promise on a repeat call; an uncloneable `reason`
  does not throw, and the worker sees an `AbortError`; `TypeError` for a
  non-number `graceMs` and `RangeError` for a negative one, thrown synchronously
  (mutation check: declare `cancel()` `async` and the throw becomes a
  rejection); `cancelGraceMs` validated at construction.
- **Re-adding**: `addTask` and `addTasks` throw for a cancelled task object; a new
  object with the same id is accepted.
- **Worker protocol**: a `cancel` that reaches a threaded worker after the attempt
  ended, or after teardown, produces no reply and no task failure.
- **Re-entrancy**, each with a mutation check that removes the matching ordering
  or tombstone check:
  - a `resource.blocked` listener that cancels the task just blocked, and one that
    calls `addTask` (the known-issue reproduction: no task is lost);
  - a `resource.acquired` listener that cancels the task being dispatched: it
    never reaches the pool, ends `cancelled`, and is counted once;
  - a `resource.released` listener, during the settle of a task that fails with
    retries left, that cancels it: it ends `cancelled` with no further attempt;
  - a `task.running` listener that cancels a task further down the queue being
    dispatched;
  - a `resource.unblocked` `'admitted'` listener that cancels a task the retry
    loop has already collected, and one that cancels a task the loop has set
    aside as orphaned (settled once, not twice);
  - a `task.cancelled` listener that calls `getAdmissionQueue()` and `addTask()`:
    no cancelled task is visible, and the new waiter's `queuePosition` does not
    count one.
- **Cancel at scale**, through the real `cancel()` with a `task.cancelled` listener
  attached: every task of a 50,000-task ready queue, in creation order and in
  reverse; every task of a 50,000-task blocked queue; and a keyed group with many
  keys. Each finishes within a generous bound (tens of ms, not seconds).
  Mutation checks: take tombstones off the head with `Array.prototype.shift()`,
  and run the full `_checkCompletion` per cancel; each must fail. Building the
  blocked queue relies on the head-indexed queue too, since `addTasks` blocks one
  task per dispatch pass.
- **Capacity race**, with a stub pool whose `enqueueNoQueue` rejects with the
  capacity error after the test has set the cancel marker: the task ends
  `cancelled`, not requeued. A real race cannot reach this branch once place 4 is
  handled, and the obvious real construction (two same-turn dispatches onto a
  fresh wrapper) loses the race on the *first* task, so it exercises place 5
  instead. Mutation check: drop the marker check in the `.catch` and the task is
  requeued.
- **Spawns**: refused from a cancelling task and from a cancelled one; still
  accepted from a parent that completed normally.
- **Shutdown**: a threaded task still cancelling when its pool shuts down settles
  `cancelled` with `forced: true`; an inline one resolves its `cancel()` `false`,
  and when its `run()` later returns, no event is emitted.
- **Counters**: `queued + running + done + failed + cancelled + blocked` equals
  the number of tasks added after every scenario above, and `resource.*`
  acquire/release balances.
- **Completion**: `scheduler.completed` fires for a run where every task is
  cancelled, and for one where every task fails.
- **Shutdown**: pending `cancel()` promises resolve, bounded by a timeout so a
  regression fails rather than hangs.
- **Browser spec** for the threaded cancel message.
- **Docs suite**: every new `AGENTS.md` snippet, and the export-coverage test
  picks up `CancelledError`.
- **Benchmark**: `benchmark:quick` before and after, back to back. Every dispatch
  now allocates a controller and writes `_queuedIn`; this measures it.

## Open questions

None. The grace default, the per-call cost and the scope were settled from the
measurements and constraints above, and from two rounds of plan review.
