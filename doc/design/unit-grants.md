# Units with identities — plan

> **Status: open, in progress on `admission-rework` (2026-11-04).** Answers
> fyflow-app's "scheduler pass 2 #2", P3. Phase 1 is built: grants, `UnitGroup`
> granting any unit, task-held and resident. The contract is in `AGENTS.md`
> §4 and §5. Phase 2, requests for specific units ("Requesting specific units"
> below), is next. Delete this in the release commit that ships both; move
> anything durable to `AGENTS.md` first.
>
> Decisions made building phase 1, against the open questions below:
> - A task's `context.grants` **does** include its worker's resident grants,
>   with the task's own winning on a clash. A pool listing one group in both
>   `groups` and `residentGroups` is not rejected.
> - A keyed group's entry is `{ [key]: grant }`, for one key too, so the shape
>   depends on the group and never on how many keys a task has.
> - The copy is frozen one level down, on threads too (re-frozen on arrival),
>   because it is shared by the work and the events.
> - `restartWorker` used to leave the old worker's resident holding filed
>   under its dead id, and give the replacement nothing. It now returns the old
>   holding and takes a fresh one, skipping the replacement if it no longer
>   fits.

## The request

Some limits count units that are interchangeable for admission but not for the
holder: two accelerator cards, licence seats, a port range, a set of accounts.
The limit is how many there are, and the task must also know *which* one it got.
A resident worker (`residentGroups`) needs the same, from its creation, to load
state onto its unit and keep it there.

Today a group counts and returns nothing. `onStart` returns `void`, and
nothing the group knows reaches the task or the worker. fyflow-app's stopgap is
a counting limit plus bookkeeping in each Plugin.

They asked for:

- a group whose units are named, granting a free unit on start and taking it
  back on finish or teardown;
- the grant readable where the work runs (the task context for a per-task
  hold, the worker context for a resident one, one entry per group);
- optionally, a cost above 1 that names every unit granted;
- optionally, custom groups returning a grant from `onStart`, so a consumer can
  write their own policy.

fyflow-orchestration would then have to pass grants on to its workers.

## What is fixed already, and what is not

- **Admission must be a hard cap.** A unit group that is asked to start with no
  free unit has nothing to grant. Phase 0 of `keyed-concurrent-limit.md` makes
  the hard cap a contract; this plan relies on it, and `UnitGroup.onStart` throws
  if it is ever violated. That throw is a scheduler bug report, not a race to
  handle.
- **Task-scoped holds always cost 1** (`_dispatchTask` hard-codes it). "A cost
  above 1" therefore applies to resident groups only, unless per-task cost is
  added, which nobody has asked for. Out of scope.
- **Resident acquisition happens before the worker exists.** `_acquireResident`
  runs, then the worker is created, then `_bindResidentHolder(workerId)` binds the
  hold. So the grant is known *before* the worker's `init`, and can travel in its
  config. That is what makes "readable from its creation" achievable.

## Shape

### The group

```typescript
new UnitGroup(units: readonly (string | number)[] | number, { id?: string })
// new UnitGroup(['gpu0', 'gpu1'])  or  new UnitGroup(2)  ->  units 0, 1
```

- `limit` is `units.length`, fixed for the group's lifetime. Adding or removing
  units at runtime is out of scope.
- `onStart(key, cost)` takes the lowest-index free units and returns them as an
  array, always an array, even at cost 1, so the shape never depends on the
  cost. It throws if fewer than `cost` are free.
- `onFinish(key, cost, grant)` frees exactly the units in `grant`, and throws on a
  unit it did not grant. Double-release is a bug worth surfacing.
- Gauges: one `level` gauge, as `ConcurrentLimitGroup`. Which units are busy goes
  on the events (below), not a gauge, because a gauge reading is a number.
- Not keyed. Units per key are out of scope.

### Requesting specific units

Not every holder accepts any unit. A task may need *the* card its data already
sits on, and a pool may be pinned to one device or allowed onto a subset. One
concept covers every case: **a request is a candidate set, and the group grants
`cost` units from it.**

| Want | Request |
|---|---|
| any unit | no request (the whole group is the candidate set) |
| exactly `gpu1` | `{ from: ['gpu1'] }`, cost 1 |
| any one of the two large cards | `{ from: ['gpu0', 'gpu1'] }`, cost 1 |
| both of them, together | `{ from: ['gpu0', 'gpu1'] }`, cost 2 |
| prefer `gpu1`, accept any | `{ prefer: ['gpu1'] }`: a soft hint that never blocks |

```typescript
// task-held: per group, on the task
new FyflowTask({ id, workerType, payload, units: { gpu: { from: ['gpu1'] } } })

// resident: per pool, where residentGroups already takes a cost
residentGroups: { gpu: { cost: 1, from: ['gpu0', 'gpu1'] } }
```

- **The pool case is the one-worker-per-device pattern.** `maxThreads: 2` with
  `from: ['gpu0', 'gpu1']` and cost 1 gives each worker its own card. A third
  worker never starts, because no candidate is free.
- **Validated up front**, like a missing limit key:
  - `addTask` throws on a request naming a unit the group does not have, or
    with `cost > from.length`;
  - a pool throws at construction on the same conditions in `residentGroups`.
- **`prefer` is a policy hint, not admission.** It changes which free unit is
  granted, never whether the task may start. This is also how "the unit a task
  used last" becomes expressible without a custom group: orchestration passes
  the last grant as `prefer`.
- **The request reaches the group opaquely**:
  - `canRun(key, cost, request)` and `onStart(key, cost, request)`;
  - `onFinish` gets the grant, as above.

  The scheduler does not interpret it, so a custom group can define its own
  request language ("any card with ≥ 16 GB free"). Only `UnitGroup` defines
  `from`/`prefer`. Where the request comes from is resolved as `keyFrom` is:
  the task's `units[groupId]` by default, overridable with a group-level
  `requestFrom(task)`.

#### Waiting for a specific unit: queues, wake-ups, fairness

This is where specific requests cost something. Each rule below is chosen so
that it does not reintroduce the scaling problems found in
`keyed-concurrent-limit.md` phase B.

- **One wait queue per distinct candidate set.** Plain "any" is one more set,
  the full one. An index from unit to the sets containing it decides what a
  release wakes: releasing `gpu1` wakes only the queues whose set contains
  `gpu1`, at O(sets containing it) per release.
  - The number of distinct sets is small in practice, since sets come from pool
    configuration and a handful of task patterns. It is still bounded:
    queues are deleted when they empty.
  - Each waiter is in one queue, so there is no multi-queue registration and
    no linear `remove()`.
- **No rescanning.** A task pinned to a busy `gpu1` is never looked at when
  `gpu0` frees. Today's single blocked queue would rescan every pinned waiter
  on every release, the same O(N)-per-release shape as the thundering herd.
- **FIFO across the sets that compete for the freed unit.** When `gpu1` frees,
  it goes to the *oldest* head among the queues that can use it, whether that
  head asked for `gpu1` exactly or for any unit. So a specific waiter cannot be
  starved by a stream of "any" tasks: once it is the oldest task that can use
  `gpu1`, it gets the next `gpu1`. This needs an arrival sequence number on
  each waiter, and comparing heads only.
- **It requires admission on wake.** A freed unit has to be *acquired* by the
  chosen waiter in the same turn as the release. If waking only moves the task
  to the ready queue, as today, a fresh task dispatched in between can take
  `gpu1` and the FIFO guarantee is gone. That is R2 of `admission-rework.md`,
  a prerequisite shared with keyed phase B.
- **Resident head-of-line admission must become per-set.** Today a resident
  waiter that does not fit blocks every waiter behind it. That is correct for
  weighted costs, and wrong for units: a pool pinned to a busy `gpu1` would
  hold up a pool that only wants a free `gpu0`. With a unit group, a waiter
  blocks only those younger waiters whose candidate sets overlap its own. For
  plain weighted groups (`ConcurrentLimitGroup`) the rule is unchanged.
- **The grant choice for "any".** Lowest free index by default. Open question:
  should the group avoid granting a unit that a specific waiter is queued for,
  when another free unit is available? It would help pinned work, but needs
  the scheduler to tell the group what is waiting. Defer until measured.

### The interface: additive, non-breaking

```typescript
interface ResourceGroup {
  canRun(key?: string, cost?: number, request?: unknown): boolean;
  onStart?(key?: string, cost?: number, request?: unknown): unknown;  // was void; a return value is the grant
  onFinish?(key?: string, cost?: number, grant?: unknown): void;
}
```

- A group that returns nothing grants nothing, so every existing and
  third-party group is unchanged. A `void` function is assignable to an
  `unknown`-returning signature, so implementers still compile.
- **A grant must survive `structuredClone`.** It is cloned once, at acquire, on
  inline pools too, so behaviour does not change between inline and threaded. The
  same parity rule as `emit()`. A grant that cannot be cloned fails the dispatch:
  the task fails with an error naming the group, and the units are released.
- The scheduler stores the *original* (pre-clone) grant and hands that to
  `onFinish`, so a custom group can use object identity.
- This is what enables the "prefer the unit a task used last" policy in a
  custom group: the group sees the key and can keep its own history.

### Where the grant is read

```typescript
// task-held: in run()
context.grants['gpu']          // e.g. ['gpu1']

// resident: from the constructor on, and in setup()
this.workerContext.grants['gpu']
```

- `grants: Readonly<Record<groupId, unknown>>` on `BaseWorkerContext`, and so
  on `TaskWorkerContext` too. An entry exists only for groups that returned a
  grant. It is a new **optional** member, unlike `emit` and `signal`, so
  hand-built context literals in consumers' unit tests keep compiling.
- **Open: does a task's context also show its worker's resident grants?**
  Proposal: yes, merged, with the task's own grants winning on a clash. A task
  wants "which GPU am I on" without caring whether it holds it for itself or
  inherits it from the worker. Check whether one pool can list a group both
  in `groups` and in `residentGroups`; if it can, decide whether that should
  be rejected.

### Lifetimes, and every path that releases

**Task-held:**

- `_dispatchTask` stores each grant on the task, next to `_resourceKeys`.
- `_releaseResourcesForTask` passes it to `onFinish` and clears it.
- The thread-capacity race path releases and requeues, so the re-dispatch
  gets a fresh grant.
- A requeue (a worker failed) is a new dispatch, so it gets a fresh grant,
  possibly a different unit. Document it: a task must read its grant on every
  attempt, not cache it across attempts.
- Cancellation releases at `task.cancelled`, after a forced stop only once the
  thread is dead. That rule already exists, and matters more here: the unit
  is a real device.

**Resident:**

- `_acquireResident` collects the grants.
- `residentPending` becomes a FIFO of pending grants rather than a count.
- `_bindResidentHolder` moves the head of that queue to the worker id.
- The grant is passed into worker creation (both wrappers' `init`).
- Release paths: `_releaseResidentFor` (teardown and shutdown) and
  `_releaseAllResident`. The latter includes holds that were acquired but
  never bound, which must release their *own* grants.
  - Audit each path for which grant it returns.
- A restarted worker is a new worker and gets a fresh grant. Same rule as a
  requeue.

### The protocol: both bootstraps

- **Task-held:** the grants travel with the task message to the thread, and the
  context is built in `workerWrapper.ts`, `workerWrapper.node.ts` and
  `inlineWrapper.ts`.
- **Resident:** the grants travel in the `init` config, and the
  `BaseWorkerContext` is built with them before the constructor runs.
- Per `CLAUDE.md`, a protocol change needs tests on a threaded pool
  (`inline: false`) in the shared suites, which run on Deno and Node, plus a
  browser spec. `emit()` and cancellation are the precedent.

### Events

- `resource.acquired` and `resource.released` gain an optional `grant`, the
  cloned value. A viewer can then show which card is busy.
- The payload type is additive, but downstream folds see a new field. Name it in
  the release notes.

## Tests to write

- Two tasks against `UnitGroup(['a','b'])` see different units, and a third waits
  and receives the freed unit. Run it inline and threaded, on Deno and Node, plus
  a browser spec.
- A resident pool of cost 2 against 4 units: each worker sees two distinct units
  from its constructor on; teardown frees exactly those units; a replacement
  worker gets a valid grant.
- A requeue and a restart get fresh grants. Cancel and forced stop free the unit
  only once the thread is dead.
- A custom group returning a grant (a "sticky" policy) is seen by the task and
  receives it back in `onFinish`.
- A non-cloneable grant fails the dispatch and frees the unit. A double release
  throws.
- Specific requests:
  - A task pinned to a busy `gpu1` waits, while "any" tasks keep using `gpu0`:
    no head-of-line blocking across disjoint sets.
  - When `gpu1` frees, the oldest waiter that can use it gets it, whether pinned
    or "any". A fresh task arriving in the same turn does not take it.
  - A pool with `from: ['gpu0','gpu1']` and `maxThreads: 3` runs exactly two
    workers, on distinct cards.
  - A resident pool pinned to a busy unit does not block a pool wanting a free
    one.
  - `prefer` is honoured when the preferred unit is free and ignored when it
    is not.
  - An unknown unit, or `cost > from.length`, throws at `addTask` or at pool
    construction.
  - Scale: 50K tasks across 8 units with 30% pinned. The work per release stays
    flat from 10K to 200K, measured as in `keyed-concurrent-limit.md` phase B.
- Mutation checks:
  - pass `undefined` instead of the grant to `onFinish`, and the release test
    fails;
  - drop the grant from the Node bootstrap only, and only the Node run fails.

## Out of scope

- Per-task cost above 1.
- Units per key.
- Adding or removing units at runtime.
- Scheduler-side affinity (sticky units), beyond the `prefer` hint. A custom
  group can implement that.
- Requesting units from inside a running worker (`setup()` or `run()` asking
  for another unit). Admission is synchronous, on the main thread, before
  dispatch, and an async acquire would be a different design.
