# Attempt accounting: resumed budgets, attempt numbers, `task.retrying`

**Status: open.** Answers fyflow-app "scheduler pass 1 #4". Tracked in
`TODO_CHECKLIST.md`. Also fixes the retry-backoff known issue, which this work
cannot ship without. Delete this file when it ships; move the contract to
`AGENTS.md` first.

**Depends on** the known issue "A requeued task can be settled by its abandoned
run" (the per-dispatch token). Without it, a requeue can fire `task.running`
twice for one attempt and let a stale result settle a new attempt.
`doc/design/task-messages.md` and `doc/design/task-cancellation.md` depend on
this plan for `attemptNumber`.

## The request

A Run interrupted by a process restart and Resumed re-adds its in-flight tasks
at the same id, and they start with a fresh retry budget. Orchestration measured
three runs of a two-attempt task across one kill. fyflow-app asks for:

- a task config field for attempts already spent, so the budget continues from
  there;
- an event when a retry starts, so a caller can persist attempts before the task
  settles.

## How it works today

- **`task.attempts` counts retries used, not attempts.** It is 0 on the first
  run and incremented in `_settleTask` when a retry is decided. A task with
  `maxRetries: 1` runs at most twice.
- **A requeue is not an attempt.** When a worker dies under a task, the task goes
  back on the queue with `attempts` unchanged.
- **`task.running` fires once per attempt**, from the pool's `task.started`. It
  is the live task. On a threaded pool it fires when the worker thread picks the
  attempt up; on an inline pool, at the start of `runTask`, before the worker
  instance is created.
- **A failure that will be retried is invisible on the scheduler.** `task.failed`
  is terminal-only by contract, so between attempts nothing is emitted.
- **Retry backoff is not enforced.** `_settleTask` pushes the task straight back
  onto the ready queue and only schedules an extra `_dispatchLoop()` after
  `backoffMs`, so any dispatch in between starts it early. Measured on Deno 2.9.6
  with `backoffMs: 1000`, an always-failing task, and an inline pool:
  - idle scheduler: gaps of 1016 and 1006 ms between attempts;
  - 300 short tasks alongside: gaps of 1232 and **1** ms. The first was
    1232 ms only because the retry waited behind the 300 tasks.

## Decisions

| Decision | Choice |
|---|---|
| Config field | `attemptsSpent`, a non-negative integer, default 0 |
| Attempt number field | `attemptNumber`, 1-based. Not `attempt`: next to the existing `attempts` (retries used, 0-based), a one-letter difference with an off-by-one meaning is a trap |
| Spent budget already used up | **The task runs once anyway.** `attemptsSpent` only reduces retries. Orchestration, which knows the budget, decides whether to add the task at all |
| Requeue after worker death | **Not an attempt**, as today. The requeued run repeats the same `attemptNumber` |
| Event for a retry | `task.retrying`, with **the live task** as its detail. See "Why the live task" |
| Backoff | Enforced: a retrying task waits outside the ready queue |
| Spawned tasks | `SpawnTaskConfig` does not get `attemptsSpent`. A spawned task is new work |

## API

### `FyflowTask`

| Member | Kind | Meaning |
|---|---|---|
| `attemptsSpent` | config | Attempts already made, for example before a restart. Default 0. A non-number throws `TypeError`; a negative, fractional, `NaN` or infinite value throws `RangeError`, both at construction |
| `attemptNumber` | field, **new** | 1-based number of the current attempt, or of the most recent one once settled. 0 before the first dispatch |
| `attempts` | field, unchanged | Retries used. Starts at `attemptsSpent` |

One rule ties them together, applied in `_dispatchTask`:
`attemptNumber = attempts + 1`. With no `attemptsSpent`, nothing changes for
existing code: `attempts` still starts at 0 and the first attempt is number 1.

"Run once anyway" needs no code of its own. The retry check is
`attempts < maxRetries`; with `attempts` starting at or above `maxRetries`, the
first failure is terminal, exactly as for a task on its last retry.

| `maxRetries` | `attemptsSpent` | Attempts this run | `attemptNumber` values |
|---:|---:|---:|---|
| 2 | 0 | up to 3 | 1, 2, 3 |
| 2 | 1 | up to 2 | 2, 3 |
| 2 | 3 | 1 | 4 |

### `task.retrying`

Fired when an attempt fails and a retry is decided, after the failed attempt's
resources are released and **before** the backoff timer is armed. Never fired for
the last attempt, which ends in `task.failed` as today. The detail is the task,
and at that moment:

| Field | Value |
|---|---|
| `state` | `'pending'` |
| `attemptNumber` | the attempt that just failed |
| `attempts` | already incremented: retries used, including this one |
| `error` | that attempt's error message |
| `timestamp` | stamped before the timer is armed. The next attempt starts no earlier than `timestamp + retryPolicy.backoffMs` |

`task.error` is now set on every failed attempt, not only the last, and cleared
when the next attempt is dispatched. A task that succeeds on a retry therefore
ends with no `error`, as today, and one that ends failed carries its last error,
as today. A task cancelled during its backoff ends with no `error` (see
`doc/design/task-cancellation.md`): the failure belonged to an earlier attempt,
not to how the task ended. Nothing reads `task.error` before a terminal settle
except the descendant tracker, which runs only on terminal states.

### Ordering, per task

```
task.running (attempt n)
  task.message ... (attempt n)           <- doc/design/task-messages.md
task.retrying (attempt n)
  ... at least backoffMs ...
task.running (attempt n+1)
  ...
task.completed | task.failed (+ task.user_action) | task.cancelled (#1)
```

A requeue inserts a second `task.running` with the **same** `attemptNumber`.
That relies on the dispatch token: without it, the abandoned run of a requeued
task can also settle it.

## Why the live task, not a fresh object

Two conventions coexist among the events today:

| Events | Detail | Allocation per event |
|---|---|---|
| `task.running`, `task.completed`, `task.failed`, `task.user_action` | the task itself | none |
| `task.progress` | a spread copy of the task | one object, every field |
| `task.spawn_request`, `task.spawn_failed` | a fresh object holding the live parent task | one small object |
| `resource.*`, `scheduler.completed` | a fresh object | one small object |
| `task.message` (planned) | a fresh object | one small object |

They are not arbitrary. Read together, they give one rule worth stating in
`AGENTS.md`: **a task's lifecycle transitions are the task itself; streams of
observations (progress, messages, resource state) are fresh objects.** A
transition describes the task, so the task is the natural payload; a stream item
describes a moment, and a consumer keeps it, so it has to be a snapshot.

`task.retrying` is a lifecycle transition: running back to pending. With the
live task:

- **It reads the same as its neighbours.** A listener handling `task.failed`
  handles `task.retrying` with the same code, and `attemptNumber` is the same
  field `task.running` gives:

  ```typescript
  const persist = (e) => store.attempt(e.detail.id, e.detail.attemptNumber, e.detail.error);
  scheduler.addEventListener('task.retrying', persist);
  scheduler.addEventListener('task.failed', persist);
  ```

- **It costs nothing.** No allocation; the only extra writes are `attemptNumber`
  at dispatch and `error` on a non-final failure. The target is ES2022, so class
  fields are defined at construction and the new field does not change object
  shape mid-life. The extra memory is one slot per retained task.
- **The hazard is the documented one.** Keeping the detail is wrong, exactly as
  for `task.running`, and the existing §6 warning covers it. That warning
  currently says "the four `task.*` events"; with this plan and cancellation it
  becomes seven (adding `task.retrying`, `task.cancelling` and
  `task.cancelled`), and the sentence needs rewriting as a rule rather than a
  count.

A fresh object would be safe to keep, but it would be the only lifecycle event
shaped differently, and its fields (`retryAt`, `nextAttempt`) are derivable from
the task. Retries are rare next to dispatches, so neither choice moves the
benchmarks: the deciding factor is consistency.

## Enforcing backoff

A retrying task leaves the ready queue for the duration of its backoff:

- `_settleTask`, on a retry, puts the task in a new map,
  `retryWaiting: Map<taskId, { task, timer }>`, emits `task.retrying`, then arms
  the timer. When it fires, the task moves to the back of its ready queue and
  `_dispatchLoop()` runs.
- **`stats.queued` still counts it**, as today, so `stats` keeps its shape and
  meaning, and `_checkCompletion` cannot report the scheduler drained while a
  retry is pending. No heartbeat runs on its behalf: `_schedulePeriodicRetry`
  arms only when a ready or blocked queue is non-empty, and none needs to,
  because the backoff timer is the wake-up.
- The task is in the map **before** the event fires, so a listener that cancels
  it (#1) finds it there, and its timer is cleared rather than requeueing a
  cancelled task.
- **Shutdown.** Dispatching during `shutdown()`'s drain is intended: `AGENTS.md`
  §7 promises that a workflow that can still finish does. So a backoff timer may
  fire during the drain and dispatch its retry, like any other settle. When the
  drain ends, every remaining timer in the map is cleared before the pools are
  shut down, so none can keep the process alive or fire into a pool that has
  been torn down. What happens to those waiting tasks matches what already
  happens to queued tasks at shutdown; this change does not alter it.

Behaviour change: under load, a retry now starts after `backoffMs`, where before
it could start almost at once. That is the documented behaviour, but it is
observable, and the release notes should say so.

## Resume, from orchestration's side

For the `AGENTS.md` recipe and the reply to fyflow-app:

1. On `task.running`, persist `max(stored, e.detail.attemptNumber)` for the task.
   The max makes a requeue (same number) harmless.
2. On Resume, re-add the task with `attemptsSpent` set to that stored value. The
   attempt that was in flight at the kill counts as spent.
3. If the stored value already uses up the budget, the scheduler runs it once
   more. If that is not wanted, do not add it: orchestration has everything it
   needs to decide.

Two edges, both documented with the recipe:

- An attempt dispatched to a threaded pool but not yet picked up by its thread
  when the process died never fired `task.running`, so it is not counted. It
  never started running, so that is correct. On an inline pool `task.running`
  fires slightly earlier, before the worker instance exists, which over-counts
  by at most that one attempt: the safe direction.
- Today, attempts dispatched during `shutdown()`'s drain fire no `task.running`,
  because the scheduler stops listening to its pools before the drain. That is
  the known issue "`shutdown()` stops listening to its pools before draining
  them", which should be fixed first; otherwise the recipe under-counts there.

## Where it lands

| File | Change |
|---|---|
| `core/FyflowScheduler.ts` | `attemptsSpent` and `attemptNumber` on `FyflowTask`; validation; `attemptNumber` and the `error` reset in `_dispatchTask`; `retryWaiting`, `task.retrying` and the timer in `_settleTask`; timers cleared at the end of the drain in `shutdown()` |
| `AGENTS.md` | §5 `FyflowTask` table (`attemptsSpent`, `attemptNumber`, and `attempts` described as retries used); §6 `task.retrying` row and the lifecycle-versus-stream rule replacing "the four `task.*` events"; a Resume recipe in §10 |

No worker protocol change, so neither worker wrapper file is touched. Roughly
60–90 lines of code, one new map, one timer per retrying task.

## Tests

- **Budget continues**: an always-failing task with `maxRetries: 2` and
  `attemptsSpent: 1` runs exactly twice; `task.running` reports attempt numbers 2
  and 3; final `attempts` is 2.
- **Spent budget runs once**: `attemptsSpent: 3` with `maxRetries: 2` runs exactly
  once, then `user_action` (or `failed` if optional).
- **Validation**: `TypeError` for a non-number; `RangeError` for negative,
  fractional, `NaN` and `Infinity`.
- **`task.retrying`**: fires once per non-final failure and never for the final
  one; carries the failed `attemptNumber` and its `error`; `task.failed` still
  fires exactly once. Mutation check: emit it on the final failure too, and the
  test must fail.
- **`error` reset**: a task failing once then succeeding ends `done` with no
  `error`.
- **Requeue**, on both pool types: a worker dying under a task gives two
  `task.running` events with the same `attemptNumber`, `attempts` is unchanged,
  and the task settles once, with the second run's result.
- **Backoff under load**: the measurement above as a test, asserting every gap is
  at least `backoffMs` minus 1 ms of timer slop. Mutation check: restore the
  immediate push and the loaded case must fail. The idle case alone would pass
  either way, which is why the load is required.
- **No early drain**: `scheduler.completed` does not fire during a backoff.
- **Shutdown during backoff**: a backoff that ends during the drain dispatches
  its retry; one that outlasts it is cleared, nothing is dispatched after the
  pools shut down, and the process exits, bounded by a timeout so a regression
  fails rather than hangs.
- **Re-entrancy**: a `task.retrying` listener that cancels the task (once #1
  exists) leaves no timer behind and no dispatch.
- **Settlement counters**: the existing invariant that
  `queued + running + done + failed` never exceeds the task count holds through
  backoffs.
- **Docs suite**: the new rows and the Resume recipe.
- **Benchmark**: `benchmark:quick` before and after, back to back.
  `attemptNumber` adds a write to every dispatch.

## Open questions

None at the scheduler level. Whether orchestration wants a task with a spent
budget re-added is orchestration's call, and step 3 of the Resume recipe gives it
what it needs.
