# Resident Resource Groups - Plan

Working notes for gating a resource that a **worker** holds for its lifetime,
rather than one a **task** holds while it runs.

**Status**: 🟢 Stages 1 and 2 shipped in 0.2.0. Stage 3 not started.

Shipped: `residentGroups` with per-worker weighted costs, head-of-line
admission, release on teardown and on worker death, construction-time
validation, and `idleCheckIntervalMs`. 12 tests in `tests/suites/resident-groups.ts`,
a worked example in `examples/resident-groups.ts`.

Not done: stage 3 (contended grace period), and runtime detection of the
`idleTimeout: 0` deadlock - documented instead, see Open questions.

## The problem

Motivating case: each task needs a different model loaded on a GPU. The model is
loaded in worker `setup()` and freed in `teardown()`, so VRAM is held for the
worker's whole lifetime. Nothing stops a second worker loading a second model
while the first is still resident.

Resource groups do not help, because they are scoped to the wrong interval:

| | Acquired | Released |
|---|---|---|
| Group slot (today) | `_dispatchTask` (`FyflowScheduler.ts:554`) | task settle (`:608`) |
| Model in VRAM | worker `setup()` | worker `teardown()` |

The worker interval strictly contains the task interval and outlives it by the
idle timeout, so a group can be at its limit of 1 and still have two models
resident.

## Evidence

Two pools (`llama`, `sdxl`), each `maxThreads: 1`, both declaring a shared
`ConcurrentLimitGroup(1, 'gpu')`:

```
LOAD   llama  resident=[llama]
RUN>   llama  a1
<DONE  llama  a1
LOAD   sdxl   resident=[llama,sdxl]     <- llama still resident
RUN>   sdxl   b1 resident=[llama,sdxl]
peak resident: 2  <<< VIOLATION
```

The group behaved correctly - the two `RUN`s never overlapped. It simply does
not gate the thing that holds the VRAM.

### No existing knob fixes it

- `idleTimeout: 1` - still peak 2. Idle reaping polls on a fixed
  `IDLE_CHECK_INTERVAL = 5000` (`workerManager.ts:103`), so teardown is never
  prompt and is never ordered against another pool's worker startup.
- `idleTimeout: 0` - means *never* terminate, so the first model is resident
  forever.
- Task-level `workerGroups` - same task scoping, same gap.

### Existing seam

`_releaseWorkerGroupResources()` (`workerManager.ts:370`) is an empty
placeholder whose comment reads *"actual implementation would coordinate with
groups"*. Worker-scoped group resources were anticipated and never built.

## Options considered

| | Shape | Framework change | Verdict |
|---|---|---|---|
| A | Worker-lifetime groups | moderate | incomplete without eviction |
| B | `beforeWorkerStart` / `afterWorkerStop` hooks | tiny | rejected - cannot evict |
| C | **A + contention-aware eviction** | moderate | **chosen** |
| D | One pool, model chosen by payload | none | rejected - see below |

**D rejected.** It holds the invariant by construction (verified: peak resident
1), but the worker swaps models per task, so interleaved arrivals thrash - 4
loads for 5 tasks in the test. C preserves pool affinity: while tasks for model
1 keep arriving, that pool keeps its worker and the model stays loaded. Model
loads are the dominant cost here, so affinity is the point, not an incidental
benefit.

**B rejected.** Smallest surface and it admits a real async mutex, but pool B's
`beforeWorkerStart` would wait forever behind an idle worker A. It solves
admission, which is the easy half.

## Spec (draft)

A resident group is held from worker creation to teardown, and a worker declares
how much of it it consumes.

```ts
// Unweighted - each worker takes one slot
new WorkerManager(url, { maxThreads: 1, residentGroups: ['vram'] })

// Weighted - this pool's model needs 20 of the group's 24 units
new WorkerManager(url, { maxThreads: 1, residentGroups: { vram: 20 } })

// A pool of small models: four fit alongside each other, none alongside the big one
new WorkerManager(url, { maxThreads: 4, residentGroups: { vram: 2 } })
```

- `residentGroups: string[] | Record<string, number>`. The array form is
  shorthand for a cost of 1 each, so nobody pays for weights who does not use
  them.
- Cost is **per worker**, not per pool - a pool with `maxThreads: 4` and
  `{ vram: 2 }` consumes 2 units per live worker, 8 at full spread. This is the
  reading that matches VRAM.
- Named `residentGroups`, not `workerGroups` - that name is taken by the
  task-level override on `FyflowTask`.
- Optional. A pool that declares none behaves exactly as today.
- Reuses the existing `ResourceGroup` types and registry, so
  `getResourceMetrics()` / `getStats()` report resident usage with no new API.
  Note the denomination changes: `available: 4` means four *units*, not four
  workers.
- Acquired when the scheduler decides to create a worker for the pool.
- Released after `teardown` completes, **and** when a worker dies - the latter is
  the placeholder at `workerManager.ts:370`.

### Strictness - probably not needed

First instinct was that optimistic groups cannot express a hard VRAM cap, since
an overshoot of one model is an OOM rather than a soft overage, and that this
would mean resurrecting `StrictLimitGroup` (implemented in `564d822`, removed in
`a035e45` - see TODO_CHECKLIST.md).

That is likely wrong. The overshoot that motivates strict groups comes from task
dispatch being decoupled from execution start. A resident slot is taken at
*decision* time, inside the scheduler's synchronous dispatch loop, so two pools
cannot both pass `canRun()` in the same tick.

**Confirmed in practice**: implemented without any new group type, and the
weighted test contends five pools over one group without ever exceeding the
limit. The heavier stress case in the test plan below is still the stronger
validator and has not been written.

## Eviction policy

The crux. A resident slot is held by an idle worker; another pool needs it.

Eager eviction - tear down as soon as no matching task is queued - is wrong. It
destroys the affinity that made C preferable to D: a pool with a steady trickle
of work would reload its model repeatedly. The deliberate idle timeout is a
feature and stays.

Direction:

1. **Keep the idle timeout as the release mechanism.** A resident worker is torn
   down on the same rules as today, releasing its slot.
2. **Make `IDLE_CHECK_INTERVAL` configurable.** Hardcoded 5 s currently bounds
   how promptly *any* worker is reaped, which sets a floor on hand-over latency
   that has nothing to do with the configured `idleTimeout`.
3. **Consider a shorter grace period when contended.** The scheduler already
   knows a task is blocked on group G (`blockedQueues`) and groups already emit
   events, so a `contended` signal is cheap. Open question below.

### Open questions

- **Contended grace period.** When another pool is waiting, should an idle
  resident worker be reaped sooner than its `idleTimeout`? Options: (a) no -
  simplest, hand-over latency is `idleTimeout` + check interval; (b) a separate
  shorter `contendedIdleTimeout`; (c) evict immediately once idle *and*
  contended. (c) reintroduces thrash when two pools are both busy.
- ~~**`idleTimeout: 0` + `residentGroups`.**~~ **Decided: document, do not
  reject.** Rejecting at construction would be wrong - the combination is
  legitimate whenever nothing contends for the group, such as a sole holder or
  pools whose costs all fit at once. It deadlocks only under contention, which
  is a runtime condition, not a static one.

  Measured: the blocked task stays `pending` forever and nothing fires to say
  so. It is not disguised as success - `scheduler.completed` correctly stays
  quiet - but `stats` reads `queued=0 running=0`, because a blocked task leaves
  the queued count, so a stalled scheduler looks idle.

  If this is ever revisited, the precise fix is runtime detection: a task
  blocked on a resident group whose every holder has `idleTimeout: 0` is
  provably unsatisfiable, and should raise an alarm the way an unstartable pool
  raises `worker.restart_limit_exceeded`. Roughly 30 lines. Not done - the
  behaviour is documented in AGENTS.md and CLAUDE.md instead.

- **Starvation is not deadlock, and is the intended trade.** A continuously fed
  pool keeps its worker and its units; a waiting pool waits for that backlog to
  drain - ~2s behind 60 queued tasks in a measurement. It resolves on its own,
  and holding the resource is exactly what stops a hot model being reloaded.
  Stage 3 exists to bound it if that wait ever costs more than the reloads.
- **Fairness.** Two waiting pools contending for one slot - FIFO, or is
  arbitrary acceptable for the first cut? Note weights raise the stakes: FIFO is
  what makes head-of-line admission a starvation *fix* rather than just a
  reordering, so the two decisions are linked.
- **Where does the pool get the group objects?** Pools hold group *ids*; the
  scheduler owns the `ResourceGroup` instances. Either the scheduler injects the
  registry into the pool, or it keeps acquisition entirely on its own side.
  Prefer the latter if it fits - fewer moving parts.

## Weighted costs

Models differ in size. Several tiny ones fit in VRAM at once; one big one takes
the lot. A worker declares how much of the resource it consumes, and the group
counts units rather than holders.

This is part of the feature, not a follow-up: the unweighted case is just
`cost = 1`, so building it in from the start costs nothing and avoids a second
pass over the acquisition path.

### Mechanism is small

Groups count units today, always 1 per holder. Weighting is an optional trailing
parameter:

```ts
canRun(key?: string, cost = 1): boolean { return this.running + cost <= this.limit; }
onStart(key?: string, cost = 1): void   { this.running += cost; }
onFinish(key?: string, cost = 1): void  { this.running -= cost; }
```

`canRun` / `onStart` / `onFinish` have only **three call sites** in the
scheduler, plus assertions in the test suites. A defaulted parameter is
backward compatible at every one, and `getMetrics()` keeps working - `limit`,
`running`, `available` and `utilization` just denominate units rather than
slots. Worth documenting: `available: 4` stops meaning "4 more workers".

The option shape is in the spec above. Validation worth having: reject a cost of
0, a negative cost, or a non-integer, and reject a cost that exceeds the group's
limit outright - that pool could never start a worker, and failing loudly at
construction beats a silent permanent block.

### Admission policy: head-of-line

Weights turn admission into a packing problem, which was the real worry - not
the API. The blocked queue must decide what to do when the task at the front
does not fit but a later one does. Simulated, 20-unit worker contending with a
stream of 2-unit workers, capacity 24:

| Policy | Undersubscribed | Saturated |
|---|---|---|
| head-of-line | BIG after 3 ticks, 150 small | BIG after 11 ticks, 466 small |
| skip-to-fit | BIG after 142 ticks, 150 small | **BIG never ran**, 480 small |

Skip-to-fit buys ~3% more small-worker throughput and starves the big worker
indefinitely. **Head-of-line blocking is the default**: the front of the blocked
queue waits for its full cost, and nothing behind it overtakes. It is both the
simpler implementation and the correct one, so weights do not drag in
reservations, priorities or fair-share scheduling.

Note this falls out of `_retryBlockedTasks` almost for free - it already shifts
from the front of the queue. The one required change is that its
`while (... && group.canRun(key))` guard must become cost-aware, or a queue
headed by a task that cannot fit will spin.

### Does not affect the strictness analysis

Check-and-add stays synchronous inside the dispatch loop, so the argument above
for not needing async acquisition holds unchanged for weighted costs.

### Caveat on those numbers

They come from a standalone allocator simulation, not the scheduler. Strong
enough to settle head-of-line versus skip-to-fit; the tick counts are not
framework behaviour and should not be quoted as such.

### Scope

Weighting applies to **resident groups only**. Out of scope: weighting
*task*-scoped groups, and weighting rate-limit groups (an expensive API call
costing 5 units against a quota is a real use case, but a separate one).

## Prototype approach

Staged, each stage independently testable:

1. **Weighted acquire/release.** `residentGroups` taken at worker creation,
   released on teardown and on worker death, with per-worker cost from the start
   - the array form is just cost 1. Head-of-line admission in
   `_retryBlockedTasks`, with its `canRun` guard made cost-aware. Construction-
   time validation of costs. Prevents the double-load immediately, and trades the
   violation for a stall - acceptable and observable at this stage.
2. **Configurable idle check interval.** Removes the fixed 5 s floor on
   hand-over latency. Independently useful regardless of the rest.
3. **Contention signal + grace period.** Only after 1 and 2 are measured, and
   only if hand-over latency is actually a problem. Decide the open question
   above with numbers rather than in advance.

Stage 1 is the whole correctness win. 2 and 3 are latency tuning.

## Test plan

- Port the repro above into a suite as a regression test: two pools, shared
  resident group of 1, assert peak residency never exceeds the limit.
- Affinity test: a run of same-model tasks must load the model **once**, proving
  C did not regress into D's thrash.
- Hand-over: after the first pool's worker idles out, the second pool must
  acquire and run.
- Worker death releases the slot (the `workerManager.ts:370` path).
- Stress: many pools contending on a small resident group, asserting the limit
  is never exceeded - this is what validates the "strictness not needed" claim.
- `maxThreads > 1` accounting: a pool with `{ vram: 2 }` and four workers must
  consume 8 units, not 2.

Weighted cases:

- Mixed sizes fit: with a limit of 24, four 2-unit workers and one 20-unit
  worker must never be resident together, but the four small ones must be.
- Sum never exceeds the limit under contention from pools of differing cost.
- Head-of-line: a blocked 20-unit pool must not be overtaken indefinitely by
  2-unit pools behind it in the queue. This is the starvation case the
  simulation identified, and the reason the policy is what it is.
- No spin: a blocked queue headed by a task whose cost cannot currently fit must
  not busy-loop in `_retryBlockedTasks`.
- Validation: cost of 0, negative, non-integer, or greater than the group limit
  is rejected at construction.
- Release restores exactly the acquired cost - a weighted worker dying must not
  leak or over-release units. Assert via `getResourceMetrics()`.
