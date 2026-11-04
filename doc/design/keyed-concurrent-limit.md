# Keyed concurrency limits — plan

> **Status: open (2026-11-03).** Answers fyflow-app's "scheduler pass 2 #1".
> Phase 0 and phase A are accepted for the next release. Phase B is accepted
> but deferred. Delete this document once phase B ships, or once it is
> declined. Move anything durable to `AGENTS.md` first.

## The request

fyflow-app is adding file-system Node Types (copy, move, delete) behind one
Host-wide `disk` group, a `ConcurrentLimitGroup(4)`. Copies on two different
disks then hold each other up, although they compete for nothing. What they want
is the limit per disk. In the 2×2 of what we ship, global × keyed against rate ×
concurrency, this is the missing cell:

|            | global                 | keyed                       |
|------------|------------------------|-----------------------------|
| rate       | `RateLimitGroup`       | `KeyedRateLimitGroup`       |
| concurrent | `ConcurrentLimitGroup` | **`KeyedConcurrentLimitGroup`** |

They asked for three things: the group itself, gauges per key, and optionally a
task naming several keys (a C:→D: copy touches both disks).

## Findings that shape this

1. **Limits are already hard caps, but the docs say they are not.** `AGENTS.md`
   §1, `README.md` and the `ConcurrentLimitGroup` doc comment all call the groups
   "optimistic", able to overshoot by up to `maxThreads × maxConcurrentTasks`.
   The code does not do that. `_canDispatchTask` and `_dispatchTask` run in one
   synchronous turn of `_dispatchLoop` (`core/FyflowScheduler.ts` around line
   718). Resident admission checks and acquires in one turn
   (`_acquireResident`). The one race left, a thread filling up between the
   check and `enqueueNoQueue`, releases what it acquired and requeues the task.
   `testLimitsHoldAtScale` already fails if the peak counted inside `run()`
   exceeds the limit. The overshoot people see comes from *counting events*,
   which is a separate known issue in `TODO_CHECKLIST.md`.
2. **Each blocked task sits in exactly one blocked queue**: the first group that
   refused it. `_checkAndBlockResources` returns on the first failure. Say a task
   waits in group A's queue while group B is the full one. A release on B does
   not wake it, and only the periodic retry does (default every 50 ms; the
   `stillBlocked` path in `_retryBlockedTasksForGroup`). That is correct, but
   slow, and invisible to `getAdmissionQueue(B)`. Multi-key would make this the
   normal case rather than a corner.
3. **A custom group can already fake multi-key.** It returns a composite key
   (`"C:|D:"`) from `keyFor` and checks both counters in `canRun`. That is
   correct, but the blocked queue is the composite key's, so wake-ups again
   fall back to the 50 ms timer. Prompt multi-key wake-ups are the part only
   the scheduler can provide.

## Phase 0 — make "concurrency limits are hard caps" the contract

> **Done** on `admission-rework`. The contract is in `AGENTS.md` §1. The audit
> found finding 1 was not quite right: re-entry from `resource.acquired` could
> overshoot a second group, and from a resident acquire, `maxThreads`. Both
> fixed; a third re-entry bug, a task failing for good when its slot was taken,
> fixed too. Details in the `TODO_CHECKLIST.md` archive. For the release notes:
> strengthening the contract breaks no consumer, and downstream code that
> defends against an overshoot can drop it.

A prerequisite here, and required by `unit-grants.md`, which cannot tolerate
overshoot at all.

- Audit every `onStart` call site: `_dispatchTask`, `_acquireResident`, and
  anything reached from `resource.*` listeners that re-enter the scheduler.
  Confirm each one is preceded by a `canRun` in the same synchronous turn.
- Decide what a lowered `ConcurrentLimitGroup.limit` means. The proposal:
  running tasks keep their units, and nothing new is admitted until the count
  falls below the new limit. A reading above the limit is still possible this
  way, and is why readings stay unclamped.
- Rewrite `AGENTS.md` §1 and the §9 "Limit briefly exceeded" row, `README.md`
  (~line 418) and the `ConcurrentLimitGroup` doc comment. The new wording: no
  more tasks hold a concurrency group than its limit, counted from `onStart` to
  `onFinish`, and a count taken from events can overshoot (point at the known
  issue).
  - Rate groups need their own sentence. They are checked at dispatch too, but
    their limit is on completions in a window, so state what is and is not
    capped.
- Add a docs test for the claim, and mutation-check it: make `_dispatchTask`
  skip the `canRun` check and confirm that test fails.
- Strengthening a contract breaks no consumer. Say so in the release notes,
  since downstream libraries may have defensive code they can drop.

## Phase A — `KeyedConcurrentLimitGroup`, one key per task

```typescript
new KeyedConcurrentLimitGroup(limit: number, {
  id?: string,
  keyFrom?: (task) => string | undefined,   // default t => t.limitKey
  limitFor?: (key: string) => number,       // default () => limit
})
```

- **Keyed exactly like `KeyedRateLimitGroup`.** `keyed = true`; `keyFor` throws at
  `addTask` on a missing key; blocked tasks queue per key.
- **`limitFor`**, so an SSD and a USB disk need not share one number. It is
  called whenever a key's state is created, and it is not cached across
  eviction, so a consumer can change their answer between runs. Validate the
  result as a positive integer, and throw from `onStart`/`canRun` otherwise.
  Adding it now is cheap. Adding it later would change the constructor shape.
- **Eviction is immediate.** A key's state is dropped when its count returns to
  0. There is no window history, so no TTL is needed, which is why the
  `idleKeyTtlMs` option is absent.
- **`canRun(undefined)`**, the scheduler's pre-filter, is true if any known key
  has room or no key exists yet. The same rule as the keyed rate group.
- **`canRun(key, cost)` and `onStart(key, cost)` honour `cost`**, so the group
  works as a resident group too. But resident groups are acquired with no key
  today (`_acquireResident` passes `undefined`). Either reject a keyed group in
  `residentGroups` at construction, or define a key for it. **Proposal: reject
  it**, with a clear error, until someone asks.
- **Gauges.** One `level` gauge, id `units`, per key. `read()` without a key
  returns `[]`, as on `KeyedRateLimitGroup`, and `describe()` reports the
  default `limit`. A reading's `limit` is the key's own, from `limitFor`, which
  is why `GaugeReading` repeats it.
- **Metrics.** `getMetrics()` is the aggregate, plus `activeKeys`. Add
  `getKeyMetrics(key)` and `getActiveKeys()`, matching the keyed rate group.
- **Events.** It emits `capacity-exhausted` and `slot-released` with `key` in the
  detail.

### Delivery checklist

- `groups/keyedConcurrentLimitGroup.ts`, exported from the entry points.
- Add it to the `publish.exclude` allowlist in `deno.json`. Without that it
  publishes as nothing; `jsr:smoke` should be run anyway.
- `AGENTS.md`: the §5 constructor block, a "Per-key concurrency limits"
  subsection, and its row in the gauges table. The docs test asserts every export
  is covered.
- Tests, in core or resource-events:
  - per-key isolation: two keys at limit 2 each run 4 at once, and a third
    task on a saturated key waits while a fourth on a free key starts;
  - `limitFor`;
  - immediate eviction;
  - gauges per key;
  - the missing-key throw;
  - the keyed-resident rejection;
  - a scale case on many keys, in the spirit of `testLimitsHoldAtScale`,
    counting inside `run()`;
  - a cancellation case on a blocked key, alongside the existing cancellation
    tests on keyed groups.
- Mutation-check the isolation test: share one counter across keys and confirm
  it fails.
- Benchmarks: none should move, since there are no scheduler changes. Run
  `benchmark:quick` against the base anyway, per `TODO_CHECKLIST.md`.

## Phase B — several keys per task (deferred)

Accepted, but not built in the first round. Recorded here so the phase A shape
leaves room for it.

- `keyFor` may return `string | string[]`. Duplicates are removed: a C:→C: copy
  holds one unit, not two. An empty array is a missing key and throws.
- **The task is admitted only if every key has room.** Check and acquire happen
  in one synchronous turn, so all-or-nothing needs no locking, and no partial
  hold means no deadlock.
- **Internals:**
  - `_resourceKeys` changes from `Record<groupId, string>` to
    `Record<groupId, string[]>`.
  - `_groupsForTask` consumers iterate (group, key) pairs.
  - `resource.*` events are emitted once per (group, key); `key` stays a
    single string on every event, so the payload shape does not change.
  - `getAdmissionQueue` lists the task under each key it waits on.
- **Fix finding 2 by waiting on the binding constraint, not on every queue.**
  The first idea, registering a task in the queue of every (group, key) that
  refused it, is rejected. It is quadratic at scale:
  - `TaskQueue.remove()` is a linear scan plus `splice`. Admitting a task from
    one queue would remove it from k-1 others, and with N tasks behind a hot
    key that is O(N) per admission, O(N²) per run.
  - `tombstone()` is O(1), but needs the caller to know every queue the task is
    in, and leaves k-1 dead entries per task that each wake-up has to skip.
  - Each release would also wake a task from every queue it sits in. On top of
    the thundering herd below, that multiplies the work rather than adding to it.

  Instead, **a task waits in exactly one queue: that of the (group, key) refusing
  it now.** When it is woken and re-checks all its keys:
  - if another key refuses, it *migrates* there: O(k), with `resource.unblocked`
    (`migrated`, see `admission-rework.md` risk 4) from the old queue and `resource.blocked` into the new one;
  - otherwise it is admitted.

  That bounds memory at one entry per task, makes each release wake only tasks
  for which that release could matter, and a migrated task never rescans the
  queue it left. It also fixes multi-*group* waits, which have the same problem
  today. Cost: a migrated task joins the tail of its new queue and loses its
  place, which feeds the starvation limitation below. Ordering by arrival
  sequence would need a heap; not worth it unless measured.
  - Check the fold invariant: one `resource.blocked` per queue entered, one
    `resource.unblocked` per queue left.

- **Prerequisite: `admission-rework.md` R1 and R2.** Admit on wake, waiting
  in one list with migration, and the deletion of empty lists are planned and
  measured there; keyed groups are where today's cost is quadratic. Phase B
  is the first consumer of that model. Acceptance adds a multi-key shape to
  that plan's `admission` benchmark: 20% of tasks naming two keys, with
  `canRun` per task and events per task flat from 10K to 200K.

- **Known limitation, documented rather than solved.** A two-key task can be
  starved by a steady stream of one-key tasks on either disk, because
  admission is greedy and nothing reserves a key for it. Reservations would be a
  second head-of-line policy and cost every keyed group. Document it, and
  revisit if it is ever observed.
- `KeyedRateLimitGroup` gets multi-key for free from the scheduler side, but its
  own `canRun`/`onStart` are per key already, so nothing changes in the group.

## Open questions

- Is `limitFor` re-evaluated for a key that stays alive? Proposal: no, it is
  read once per key lifetime, so a running copy never sees its own limit move.
  State this in `AGENTS.md`.
- `getMetrics().limit` on the aggregate: the sum over active keys, or the
  default `limit`? The keyed rate group's answer should decide it, for
  consistency.
