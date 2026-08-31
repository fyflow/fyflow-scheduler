# Resource Events - counter-proposal

Response to `fyflow-react-components` `doc/upstream-spec.md` v0.1 (2026-08-29), which asks for
**S6** (resource acquire/release/blocked events), **S2** (timestamps), **S7** (the admission queue)
and **S8** (a documentation fix). Covers S6, S2, S7, S8. **S5 is declined** and stays declined.
S1 and S4 are out of scope here.

**Status**: 🟢 **Implemented** in 0.3.0, across three commits following [§12](#12-order-of-work).
The document is kept as written - it is the record of what was argued, not a changelog - with the
deviations listed under [§13](#13-what-shipped) at the end. The spec's shape is **not** adopted
verbatim: section 3 proposes a different payload and section 2 lists five places the spec disagrees
with the code.

**Verified against**: `fyflow-scheduler` at `3025eb2` (v0.2.1), which is the commit the spec pins.

---

## 1. Verification of the incoming spec

Every line number in `upstream-spec.md` was checked against `3025eb2` and **all of them are
correct** - 14 call sites in `FyflowScheduler.ts`, 3 in `workerManager.ts`, `_groupsForTask` at
264-268, the recorded-key comment at 645-651, head-of-line at 850-858, and the three `AGENTS.md`
cross-references. The claim that `task.progress`, `task.spawn_request` and `task.spawn_failed`
already carry timestamps is correct. §7.1's claim that task-held acquisitions are always cost 1 is
correct: line 600 passes no `cost` argument.

The spec is accurate. What follows are the exceptions, not a rebuttal.

### Answer to the question the spec asks back (§7.1)

> *is a weighted task-held cost intended eventually?*

The parameter already exists - `canRun/onStart/onFinish(key?, cost = 1)` on `ConcurrentLimitGroup`,
added for resident groups in 0.2.0. The task-held call site simply does not pass one, and there is
no plan to weight it. If it is ever wanted, only line 600 changes. The event shape carrying `cost`
is therefore right and forward-compatible.

## 2. Where the spec disagrees with the code

### 2.1 The blocked-queue key separator is `\x00`, not a space

`upstream-spec.md` §3 describes `blockedQueues` as keyed by *"`groupId key`"*. The actual key is
built with a NUL delimiter:

```
line 276:  return key === undefined ? groupId : `${groupId}\x00${key}`;
line 822:  const prefix = `${groupId}\x00`;
```

NUL is deliberate - it cannot occur in a group id or a key - and it renders as a space in most
tools, which is almost certainly how the spec got it. **An implementation splitting on `" "` would
silently mis-key every keyed group.** This is the one error in the spec that would have produced a
real bug.

### 2.2 A blocked task has four exits, not one

The spec places `resource.unblocked` in `_retryBlockedTasksForGroup`. A task actually leaves a
blocked queue four ways:

| | Path | Consequence |
|---|---|---|
| a | admitted to the ready queue | the intended one |
| b | `otherQueue.splice()` when admitted via a *different* group | must **not** emit a second unblocked |
| c | line 848 `if (!pool) continue;` | shifted out and **dropped entirely** - never requeued, no event |
| d | `shutdown()` line 1277 `blockedQueues.clear()` | every waiter discarded silently |

Rule 3's acquire/release pairing is achievable. The implied **blocked/unblocked** pairing is not,
unless (c) and (d) are given events or documented exclusions. See [4.4](#44-blocked-and-unblocked).

### 2.3 Multi-queue membership: defensive, not demonstrably reachable

`_checkAndBlockResources` returns on the **first** short group and queues on that one, guarded by
`includes(task)`, and `_retryBlockedTasksForGroup` splices an admitted task out of every *other*
group's queue - which reads as though a task can sit in two queues at once.

**Traced, and that is not established.** A not-yet-runnable task is returned to the *same* queue via
`stillBlocked`, and the blocked -> ready -> re-blocked path removes it from the first queue before
adding the second. The cross-queue splice looks **defensive** rather than reachable. An earlier
draft of this document asserted reachability; that was inferred from the code's existence rather
than verified, and is withdrawn.

This does not matter to the design, and that is the point: the event model in
[4.4](#44-blocked-and-unblocked) tracks **queue membership** rather than task blockedness, so it is
conserved whether or not the multi-queue case is reachable. It does not rest on an argument nobody
has proven.

### 2.4 Rate-limit groups have no `cost` parameter

`ResourceAcquiredDetail.cost` is typed as always meaningful. Only `ConcurrentLimitGroup` was
extended with cost in 0.2.0; `RateLimitGroup.onStart()` and `KeyedRateLimitGroup.onStart(key?)` take
none. `cost` is therefore always 1 for non-concurrent groups and must be documented as such.

### 2.5 `workerType` is not available where the spec assumes

`WorkerManager` has no `workerType` - it is the scheduler's `workerPools` key. Resident acquires
happen inside the pool, so something has to supply it.

Resolved by [6.2](#62-the-scheduler-is-the-only-emitter): the scheduler injects a notifier at bind
time, where it already knows the pool key, and closes over it. No change to
`_setupWorkerPoolListeners` is needed.

## 3. The central counter-proposal: self-describing gauges

**This is the part that departs from the spec, and it is the part most worth arguing about.**

### 3.1 The problem with a fixed payload

The spec's `ResourceAcquiredDetail` carries `running: number` and `limit: number`. That shape is
correct for `ConcurrentLimitGroup` and wrong for everything else:

- **`RateLimitGroup` enforces several overlapping windows together.** One number cannot represent
  *10/sec AND 100/min*. The spec says this itself in rule 6 - *"a UI drawing one bar for a
  multi-window group is drawing a lie"*.
- **`getMetrics()` is actively misleading for rate groups.** It reports `limit` = the most
  restrictive window's limit and `running` = the in-flight count, which are unrelated quantities.
  Concretely, with 10 completions in the last second and nothing in flight:

  ```
  getMetrics() -> { limit: 10, running: 0, available: 10 }   // looks idle
  canRun()     -> false                                       // actually saturated
  ```

  So a fold of `acquired - released` for a rate group reproduces `getMetrics().running`, a number
  that looks like utilisation and does not tell you whether the group can run. The spec's
  acceptance criterion 2 would pass and mean nothing there.
- **`ResourceGroup.type` is a closed union** - `'concurrent' | 'rate-limit' | 'keyed-rate-limit'` -
  so a consumer of this library cannot even declare a custom group type without a type error, and a
  viewer keyed on that union needs new code for every group anyone writes.

### 3.2 The shape

Render **gauge kinds**, not group types. Each group declares the gauges it presents; the viewer has
one renderer per *kind* and never learns a group's name.

```typescript
type GaugeKind = 'level' | 'window';   // small, closed today, extensible - see 3.5

/** Static. Declared once, never repeated in an event. */
interface GaugeSpec {
  id: string;          // stable within the group: 'units', 'per-second'
  label: string;       // human label
  kind: GaugeKind;
  unit?: string;       // 'units' | 'GB' | 'requests' | '$' - display only
  limit: number;
  windowMs?: number;   // kind 'window' only
}

/** Dynamic. Carried on every resource event. */
interface GaugeReading {
  id: string;
  value: number;
  limit: number;       // repeated deliberately: ConcurrentLimitGroup.limit is mutable
  resetAt?: number;    // kind 'window'
}
```

Added to the group interface, **both optional**:

```typescript
interface ResourceGroup {
  readonly type: string;                   // WIDENED from the closed union
  describe?(): { gauges: GaugeSpec[] };
  read?(key?: string): GaugeReading[];
}
```

Optional with a default derived from `getMetrics()`, so **every existing group and every future
third-party group works with no changes**. A group wanting fidelity implements `read()`.

### 3.3 How everything maps, with no new viewer code

| Group | Gauges | New viewer code |
|---|---|---|
| `ConcurrentLimitGroup` | 1 x `level`, unit `units` | - |
| `RateLimitGroup` | N x `window`, one per window, with `resetAt` | - |
| `KeyedRateLimitGroup` | N x `window` per key | - |
| *third-party* token bucket | 1 x `level` (tokens available) | **none** |
| *third-party* cost budget | 1 x `level`, unit `$` | **none** |
| *third-party* priority semaphore | N x `level`, one per band | **none** |

`RateLimitGroup.getStatus()` already computes exactly the per-window payload -
`RateWindowStatus` is `{ limit, windowMs, current, completed, running, remaining, resetTime }` plus
`canAcceptNew`. For that group `read()` is wiring, not new logic.

`KeyedRateLimitGroup` has **no** `getStatus()` - only `getMetrics()`, `getKeyMetrics(key)` and
`getActiveKeys()`. Its per-window view is genuinely new code. See [8](#8-open-questions).

### 3.4 This collapses the spec's B-vs-C choice

`upstream-spec.md` rule 6 offers two options for rate groups: force them into acquire/release, or
give them a separate `rate.consumed` carrying per-window counts, and states a preference for the
second.

**With gauges the choice disappears.** One event shape carries `readings: GaugeReading[]` - one
`level` reading for a concurrent group, N `window` readings for a rate group. The `window` readings
*are* the `rate.consumed` payload. No second event type, no special-casing, and a third-party group
is rendered for free.

The conservation fold survives, because `cost` and `holderId` stay on the event and are
group-agnostic.

### 3.5 The escape hatch, which must be specified up front

Two kinds is a bet. A percentile or histogram group would not fit. The vocabulary can grow only if
old viewers survive a new kind, so the rule is part of the contract:

> **An unrecognised `kind` renders as `level`, using `value` and `limit`.**

Without that rule, adding a third kind later breaks every deployed viewer.

## 4. S6 - the events

### 4.1 One detail shape

```typescript
type ResourceLifetime = 'task-held' | 'resident';
type HolderKind       = 'task' | 'worker';

interface ResourceEventDetail {
  groupId:    string;
  groupType:  string;            // open, see 3.2
  lifetime:   ResourceLifetime;
  holderKind: HolderKind;
  holderId:   string;            // task id, or worker id
  workerType: string;            // the pool key, for both kinds
  cost:       number;            // units taken. 1 for task-held and for non-concurrent groups
  key?:       string;
  timestamp:  number;            // Date.now(), see section 5
  readings:   GaugeReading[];    // group state AFTER the operation
}

interface ResourceReleasedDetail extends ResourceEventDetail {
  reason: 'settled' | 'worker-teardown' | 'shutdown';
}

interface ResourceBlockedDetail extends ResourceEventDetail {
  queuePosition: number;         // 0-based, in the queue it joined
  readings: GaugeReading[];      // state of the group it is SHORT OF
}

interface ResourceUnblockedDetail extends ResourceEventDetail {
  reason: 'admitted'    // moved to the ready queue - exit (a)
        | 'superseded'  // admitted via another group, spliced out here - exit (b)
        | 'orphaned'    // its pool no longer exists - exit (c)
        | 'shutdown';   // the scheduler is tearing down - exit (d)
}
```

| Event | Fired |
|---|---|
| `resource.acquired` | immediately after `onStart`, both lifetimes |
| `resource.released` | immediately after `onFinish`, all release paths |
| `resource.blocked` | on **every** push onto a blocked queue |
| `resource.unblocked` | on **every** exit from a blocked queue, tagged with which |
| `resource.declared` | once per group at scheduler construction, carrying `GaugeSpec[]` |

`resource.declared` keeps the schema out of the hot path: labels, kinds and units go out once, and
events carry only `{id, value, limit}` readings.

### 4.2 Call sites

| Lifetime | Acquire | Release |
|---|---|---|
| task-held | `FyflowScheduler` line 600 | line 654 |
| resident | `WorkerManager._bindResidentHolder` | `_releaseResidentFor`, `_releaseAllResident` |

**Resident acquires emit at *bind*, not at acquire.** `_acquireResident` increments
`residentPending` before a worker id exists; `_bindResidentHolder` attaches it. Both call sites bind
in the same synchronous turn, so binding is a safe emit point and it avoids the provisional-id
scheme the spec's rule 7 contemplates. A holding that is acquired and never bound is a bug, not a
state to model.

### 4.3 Rules

1. **Emit after mutating.** `readings` are the state *after* the operation.
2. **Do not clamp.** Groups are optimistic; a `level` reading may exceed its limit
   (`AGENTS.md` §38). Clamping would make the event disagree with `getMetrics()`.
3. **Every acquire has exactly one matching release** with the same `groupId`, `holderId`, `key`
   and `cost`. The release path already uses the **recorded** key rather than re-deriving it
   (lines 647-650); the events inherit that.
4. **A resident acquire is per worker.** `_releaseAllResident` releases several and emits one event
   each.
5. **`resource.blocked` names the one group the holder is short of**, which
   `_checkAndBlockResident` already computes at line 255 and `_checkAndBlockResources` at 535.
6. **Emission is guarded.** No detail object is built when nothing is listening - see
   [7](#7-cost).

### 4.4 Blocked and unblocked

**The unit of this invariant is `(groupId, key, holderId)` queue membership - not "is this task
waiting".** An earlier draft paired per task, and it was wrong: a consumer reconstructing *one
group's* queue could not do it, because exit (b) removed a task from another group's queue silently
and that queue then never drained in the fold.

The contract:

- `resource.blocked` fires on **every** push onto a blocked queue, carrying `groupId`, `key` and
  `queuePosition`.
- `resource.unblocked` fires on **every** exit from a blocked queue, carrying the same
  `(groupId, key, holderId)` and a `reason` naming which exit:

  | `reason` | Exit | Where |
  |---|---|---|
  | `admitted` | (a) | moved to the ready queue |
  | `superseded` | (b) | `otherQueue.splice()` - admitted via a different group |
  | `orphaned` | (c) | line 848, its pool no longer exists |
  | `shutdown` | (d) | `blockedQueues.clear()`, one per remaining waiter |

- There is no "already blocked, emit nothing" rule. Every push emits; every exit emits.

**Conservation.** For every `(groupId, key, holderId)`: `Σ blocked − Σ unblocked === 0` over a run
that reaches shutdown, and the per-group membership set is exactly reconstructible at any point.
This holds whether or not multi-queue membership is reachable
([2.3](#23-multi-queue-membership-defensive-not-demonstrably-reachable)), which is why it is
preferable to the per-task version even setting the consumer's requirement aside.

`queuePosition` is the position **at push time**. Positions of the tasks behind a departing waiter
shift without an event; a consumer folding membership derives order from arrival, which is what the
queue itself does. The field is a convenience for a late joiner, not the source of order.

**Exit (c) remains a pre-existing bug** - a task dropped there is lost regardless of whether an
event announces it. Emitting `orphaned` makes it observable rather than fixing it; the fix is
tracked separately in [8](#8-open-questions).

## 5. S2 - timestamps

Five events lack one. `Date.now()`, matching the convention every other `timestamp:` in the library
already uses - `performance.now()` appears only in duration measurement, which is the correct split.

| Event | Line |
|---|---|
| `scheduler.completed` | 320 |
| `task.completed` | 727 |
| `task.failed` | 775 |
| `task.user_action` | 777 |
| `task.running` | 974 |

Ships in the same commit as S6: a resource hold duration is the gap between an acquire and its
release, so a collector-stamped pair folds two event-loop delays into every measurement.

**Do not let `startTime` stand in for it.** For the four `task.*` events the detail **is** the
`FyflowTask`, so those fields read as whatever the object holds when the consumer looks - which is
[S8](#11-s8---details-are-live-references).

## 6. S7 - the admission queue

```typescript
interface AdmissionWaiter {
  holderId:   string;              // task id
  workerType: string;              // its pool
  lifetime:   ResourceLifetime;    // what it is waiting to acquire
  cost:       number;              // units it will need: the pool's resident cost
                                   // for 'resident', 1 for 'task-held' (see 2.4)
  position:   number;              // 0-based; 0 is the head, and the head gates the rest
  key?:       string;              // the bucket, for keyed groups
}

// on FyflowScheduler
getAdmissionQueue(groupId?: string): Record<string, AdmissionWaiter[]>;
```

**`lifetime` is not decoration.** `blockedQueues` holds waiters of both kinds - task-scoped blocks
from `_checkAndBlockResources` (line 544) and resident admission from `_checkAndBlockResident`
(line 257). An earlier draft defined `cost` as *"from the pool's residentGroups"*, which is
undefined for a task blocked on a rate limit in a pool that declares none. Since `resource.blocked`
covers both lifetimes, the two sides of criterion 4b would have described different sets and the
check could not have passed.

The name is broader than it reads: this is every blocked waiter, not only resident admission. It
keeps the spec's name because that is what the consumer's panel is called.

### 6.1 Keyed by group id, with `key` on the waiter

**Decided.** The composite `groupId\x00key` is an internal detail and does not leak. The `\x00` is
split once per queue while iterating the map, not once per waiter.

**Cost.** This is an accessor, not an event - it costs nothing when not called, and no listener
guard applies. When called it is `O(total blocked tasks)`, allocating one array per group. That is
fine for tens of waiters and worth documenting, because the panel exists for the starvation case
where a backlog can be large. The `groupId` filter narrows it, and with S6 shipping the accessor is
only needed for late-joiner catch-up rather than per frame.

### 6.2 The scheduler is the only emitter

**Decided, reversing an earlier draft.** That draft had `WorkerManager` dispatch `resource.*` and
the scheduler forward it. That is wrong: a consumer treating the scheduler and the pool as separate
event sources - which is the natural reading, since both are `EventTarget`s with documented events -
sees **one resident acquire twice**. Task-held acquires are emitted on the scheduler only, so the
double-count is exactly the resident share, and conservation
([9](#9-acceptance-criteria) rule 1) breaks silently by that amount.

This library has already been bitten by the same shape. `FyflowScheduler.ts:1063`:

> `// NOTE: Don't forward task.completed event here!` ...
> `// Forwarding here would cause duplicate events with stale task state.`

So: **`WorkerManager` does not dispatch `resource.*` at all.** The scheduler injects a notifier
callback into the pool, the pool calls it, and only the scheduler dispatches. The injection point
already exists - `_bindResidentGroups(registry)` is called once per pool at scheduler construction
and gains a second argument.

Two things fall out of this rather than needing separate solutions:

- **"Which source family is authoritative" needs no rule.** The scheduler is the only one that
  emits, so the question does not arise.
- **`workerType` is available for free** ([2.5](#25-workertype-is-not-available-where-the-spec-assumes)).
  The scheduler knows the pool key at bind time and the injected callback closes over it. No change
  to `_setupWorkerPoolListeners`.

A pool-level mirror of `resource.*` could be added later if a consumer wants one, but it would have
to be opt-in and documented as a duplicate. It is not in this change.

## 7. Cost

These are high-frequency events: one acquire and one release per task per group. A scheduler at
5 000 tasks/s across 3 groups adds 30 000 events/s.

Mitigations, all required rather than optional:

- **A listener guard.** No detail object and no `read()` call when nothing is subscribed. `read()`
  on a rate group filters arrays per window, so this is the difference between free and not.
- **Schema out of band.** `resource.declared` carries labels, kinds and units once;
  readings are `{id, value, limit}`.
- **No throttling or coalescing in the library.** The spec asks for none
  (`upstream-spec.md` §8) and choosing a constant before measuring a real run is how a wrong
  constant becomes load-bearing.

## 8. Open questions

- **Does `KeyedRateLimitGroup` get `read()` in this change or later?** It has no `getStatus()`, so
  its per-window view is new code. Deferring means keyed groups fall back to the `getMetrics()`
  default, which is the misleading one from [3.1](#31-the-problem-with-a-fixed-payload).
- **Is widening `type` to `string` acceptable?** It is source-breaking for any consumer doing an
  exhaustive `switch`. Nothing in this library branches on it, and the viewer does not exist yet, so
  now is the cheapest moment.
- **Exit (c) is a pre-existing task-loss bug.** Line 848 shifts a task out of a blocked queue and
  drops it when its pool is missing. `addTasks` validates worker types so it should be
  unreachable, but it is silent if it happens. Fix separately or leave?
- **Should `getAdmissionQueue` cap its output?** Not asked for. An unbounded walk is honest; a cap
  is a lie with a smaller allocation.

## 9. Acceptance criteria

Adopted from `upstream-spec.md` §9, amended where the gauge model changes them.

1. **Conservation of holdings.** Over a run that completes, for every `(groupId, key)`:
   `Σ acquired.cost − Σ released.cost === 0`, and the running total never goes negative.
   **Each acquire is emitted exactly once** - in particular a resident acquire is not counted twice
   by a consumer listening to both the scheduler and the pool ([6.2](#62-the-scheduler-is-the-only-emitter)).
1b. **Conservation of queue membership.** For every `(groupId, key, holderId)`:
   `Σ blocked − Σ unblocked === 0` over a run that reaches shutdown, and no `unblocked` arrives
   without a preceding `blocked` for the same triple. (That the fold *equals* the live queue is
   criterion 4b, which is checkable from outside; this one is about the stream alone.)
2. **Agreement with the accessor.** At any quiescent moment, the folded occupancy equals
   `getMetrics().running` for every **concurrent** group, including while over limit.
   *Amended:* this check is meaningless for rate groups (see [3.1](#31-the-problem-with-a-fixed-payload));
   for those, the latest `window` readings must equal `getStatus()`.
3. **The stall is visible.** The `idleTimeout: 0` resident deadlock (`AGENTS.md` §298-314) produces
   a `resource.blocked` naming the contended group, at a moment when `stats` reads
   `queued=0 running=0`.
4. **Head-of-line is legible.** With a 20-unit pool waiting behind a 24-unit group held by 2-unit
   workers, `getAdmissionQueue()` reports the 20-unit waiter at position 0 for the whole time the
   cheaper pools keep cycling.
4b. **The two routes agree.** At any moment between dispatch passes, with at least one group's
   queue non-empty, the membership folded from `resource.blocked` / `resource.unblocked` equals
   `getAdmissionQueue()` for every group - **in the same order**, and including waiters of both
   lifetimes.

   *Why it is worth its own criterion.* A fold that misses an emit stays internally consistent while
   being wrong; an accessor is right but has no history. Neither route can detect the drift, so only
   the comparison can - the same argument criterion 2 makes on the occupancy side. It is also the
   check that catches the specific mistake this design invites: `_retryBlockedTasksForGroup`
   physically shifts a task out of a queue and unshifts it back within one synchronous pass
   (`stillBlocked`, and the head-of-line `break`), and an `unblocked` emitted on the array operation
   rather than on the logical move to the ready queue drifts permanently and silently.

   *Quiescent means something different here than in criterion 2.* There, quiescent is "nothing in
   flight". Here the interesting state is precisely when tasks **are** blocked, so "drained" would
   make the check compare two empty maps and pass forever. It must be evaluated between dispatch
   passes with the queue non-empty.

   *Order, not just membership.* Every `unshift` returns tasks to positions they already held -
   `stillBlocked` preserves relative order, and head-of-line returns the head to the head - so queue
   order equals the order of `blocked` events among current members. Comparing ordered arrays is
   therefore achievable, and it is what criterion 4's head-of-line claim actually rests on.

5. **A third-party group renders with no viewer change.** A group implementing only `canRun`,
   `onStart`, `onFinish` and `getMetrics` produces a usable `level` gauge via the default; one
   implementing `describe()`/`read()` produces its own.

## 10. Not doing

- **S5, a snapshot accessor.** Declined by the consumer and declined here.
- **S4, source-side sequence numbers.** Out of scope.
- **S1, an eviction event.** Out of scope for this change; cheap, and worth revisiting.
- **Any change to `capacity-exhausted` or `slot-released`.**
- **Throttling or coalescing.** See [7](#7-cost).
- **Freezing event details.** S8 is a documentation fix; defensive copying on a hot path is the
  wrong trade for a zero-dependency library.

## 11. S8 - details are live references

For `task.running`, `task.completed`, `task.failed` and `task.user_action`, `detail` **is** the
`FyflowTask`; for `scheduler.completed` it is `this.stats`, mutated in place. Listeners run
synchronously, so reading and copying immediately is correct - but `arr.push(e.detail)` is the
natural thing to write, and every entry then reads whatever the object holds later. Nothing throws.

`task.progress` is **not** affected - its detail is `{...task, ...}`, a shallow copy.

A sentence in `AGENTS.md` §6 rather than a code change.

## 12. Order of work

1. **S6 + S2 + the gauge interface** - one commit. Both change what an event carries, and gauges
   change the payload S6 would otherwise ship.
2. **S7** - the accessor.
3. **S8** - documentation.

Stage 1 is the only one that blocks the consumer's `3a` and `3b` views.

## 13. What shipped

Everything above, in 0.3.0, with four deviations and two discoveries worth recording.

### 13.1 The open questions, answered

All four of [§8](#8-open-questions) were settled before implementation, and the consumer's review
had already answered three of them the same way.

- **`KeyedRateLimitGroup.read()` shipped now**, not later. It is ~30 lines mirroring
  `RateLimitGroup`, reusing the window arithmetic `_keyCanRun` already does, and the alternative
  the consumer would accept - declaring *no* gauges rather than a misleading one - required stub
  methods anyway. `read()` without a key returns `[]`: summing per-key buckets against a per-key
  limit is the number [3.1](#31-the-problem-with-a-fixed-payload) objects to.
- **`type` widened to `string`.**
- **Exit (c) emitted `orphaned` in the three event commits, and was fixed in a fourth.** Keeping the
  fix out of the event commits meant those three changed what is *observable* and not what
  *happens*. The fix itself did **not** route through `_settleTask` as the checklist first proposed:
  that decrements `stats.running` for a task that was only ever queued, and would hand the task to a
  retry that requeues it into a ready queue no dispatch can drain. See [13.6](#136-the-two-bugs-found-while-testing-were-fixed-too).
- **`getAdmissionQueue` is uncapped.**

### 13.2 `resource.declared` was replaced by an accessor

The plan has it dispatched once per group at scheduler construction. **Nothing can hear that**: a
consumer only obtains the instance after the constructor returns, so the event would have been dead
code by construction. `FyflowScheduler.describeResources()` returns the same `GaugeSpec[]` per group
instead, applying the `getMetrics()` default so a consumer does not reimplement it.

This is not [S5](#10-not-doing) creeping back in. The line is the one
[ADR 0013](../fyflow-react-components/doc/decisions) draws: **schema is time-invariant, state is
not.** Reading `describe()` cannot be stale or self-inconsistent; a state snapshot can, and stays
declined.

### 13.3 Where S2's timestamp lives

[§5](#5-s2---timestamps) says the five events get one but not *where*, and for the four `task.*`
events the detail **is** the `FyflowTask`, so there is no snapshot to put it on without the
defensive copy [§10](#10-not-doing) rules out.

It is a field on the task, stamped immediately before dispatch - live like every other field on
that detail, which is exactly what [S8](#11-s8---details-are-live-references) documents, and read
synchronously inside the listener. It is still not `startTime`: `task.running` is emitted from the
pool's `task.started`, well after `startTime` was set.

`scheduler.completed` is the exception and is copied - `{ ...stats, timestamp }`. It fires on every
drain, so a consumer keeping two of them held one live object twice. Four numbers on a
once-per-drain event is not the hot path §10 is about.

### 13.4 Two things found while testing, not while designing

**The blocked-queue separator was a raw NUL byte in the source.** [2.1](#21-the-blocked-queue-key-separator-is-x00-not-a-space)
is right that it renders as a space - and the reason the incoming spec got it wrong is that
`core/FyflowScheduler.ts` contained actual NUL bytes, which made the file read as *binary* to grep
and as a space to every editor. It is now `BLOCKED_QUEUE_SEPARATOR`, holding an escape sequence,
with a `_splitQueueId` inverse. The trap is unchanged; it is just visible.

**A continuously busy scheduler never runs its periodic retry.** Every settle calls
`_schedulePeriodicRetry`, which *cleared* the pending timer and started a new one, so a scheduler
settling tasks faster than `periodicRetryIntervalMs` reset the retry forever and
`_retryBlockedTasksForGroup` was never reached. Found because the first criterion-4 scenario churned
without pausing: instrumentation showed **zero** calls across a whole run with 12 of 24 units free,
and both criterion 4 and 4b passed while testing nothing.

It is worse than "slower than advertised". A release on a task-held group retries that group
directly, but a **rate-limit window rolling over** and **resident admission** have no such path, and
a direct reproduction had two rate-limited tasks starved through ten window rollovers while 156
tasks completed beside them. Fixed - see [13.6](#136-the-two-bugs-found-while-testing-were-fixed-too).

### 13.5 The criteria were mutation-checked, not asserted

[9](#9-acceptance-criteria) 4b exists to catch one specific mistake, so the test was checked against
it rather than trusted:

- Moving `unblocked`/`admitted` to the `shift()` out of the blocked queue - the drift 4b names -
  makes it fail with `fold [cheap-behind] vs accessor [big-1,cheap-behind]`.
- Removing the head-of-line `break` makes criterion 4 fail with *the cheap waiter reached "done"*.

Both passed *before* the scenario was fixed, which is the whole argument for the check.

### 13.6 The two bugs found while testing were fixed too

Both were found by instrumenting a test that was passing, and both are stalls rather than slow
paths, so neither was left as a follow-up.

**The periodic retry is a heartbeat, not a debounce.** `_schedulePeriodicRetry` now leaves a pending
timer alone instead of restarting it, and the callback clears the handle before re-arming. A
directly reproduced case - a rate-limited pool beside a busy one sharing no groups - went from **1
of 3 tasks completing across ten window rollovers** to all 3, with the busy pool's throughput
unchanged (152 vs 156 tasks). Benchmarked back to back on `benchmark:quick`: every delta inside the
40-70% run-to-run band `CLAUDE.md` documents for this suite, so no evidence of regression either
way.

**A waiter whose pool has gone is failed, not dropped.** Exit (c) now routes to `_failOrphanedTask`
rather than `continue`. It deliberately avoids `_settleTask`, and deliberately skips retries; the
reasoning is in `TODO_CHECKLIST.md` and in the method's own comment. It also sweeps the task out of
every other blocked queue so a consumer's queue fold drains.

Both are covered by tests that were mutation-checked, and the orphan test is **bounded** rather than
awaiting the promise: under the old behaviour the task never settles, so an unbounded await would
hang CI rather than fail it - the bug's signature and a missing assertion would have looked
identical.
