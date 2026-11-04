# Admission and wake-up rework — plan

> **Status: open (2026-11-03).** The foundation under phase B of
> `keyed-concurrent-limit.md` (several keys per task) and the specific-unit
> requests in `unit-grants.md`. Both need a waiter to be woken by the release
> that can actually admit it, at a cost that does not grow with the queue. It
> also closes three existing issues in `TODO_CHECKLIST.md`:
> - one release moves a whole blocked queue back to the ready queue;
> - empty blocked queues are never deleted;
> - the admission queue undercounts waiters.
>
> Delete this document once it ships. Move the contracts to `AGENTS.md` first.

## What happens today, measured

Measured at `0adeefb` on Deno 2.9.6, Windows. Inline pool, 4 threads × 50
concurrent tasks; trivial tasks that yield three microtasks. The probe scripts
are not in the repo yet; R0 turns them into a benchmark category.

**A global group is linear, but its admission queue is wrong.**
`ConcurrentLimitGroup(7)`:

| N | ms | tasks/s | `canRun` calls per task |
|---|---|---|---|
| 2,000 | 19 | 107K | 9 |
| 20,000 | 113 | 178K | 9 |

There is no quadratic term here. The reason: `_dispatchLoop` *breaks* on a
non-keyed block, so only the head of the ready queue ever enters the blocked
queue. The other waiters stay in the ready queue, which is why
`getAdmissionQueue()` shows 1-2 tasks while hundreds wait (the known issue).
The herd is real but tiny, because the queue it moves is tiny.

This also suggests that the known issue's 1,900 tasks/s under a limit-7 group
is not caused by the herd. Here the same shape ran at 178K/s. That figure was
measured with 30 ms tasks, where the limit itself caps throughput at about
230/s, so it may never have been a scheduler cost. Recheck before citing it.

**A keyed group is quadratic in each key's queue depth.**
`KeyedRateLimitGroup` with 10 per 20 ms per key, 10 keys:

| tasks per key | ms | `canRun` per task | `resource.blocked` per task | worst main-thread stall |
|---|---|---|---|---|
| 100 | 574 | 15 | 4.5 | 28 ms |
| 200 | 1,191 | 31 | 9.5 | 32 ms |
| 400 | 2,450 | 61 | 19.5 | 35 ms |
| 800 | 4,394 | 127 | 41.8 | **738 ms** |

Work per task doubles each time the queue depth doubles, so the total is
O(N²/keys). Here is the mechanism:
- The `blocked-keyed` path keeps scanning, so whole key queues fill up.
- Then `_retryBlockedTasksForGroup` loops `while canRun(key)`, but nothing
  is acquired until dispatch, so it moves the whole key queue back to the
  ready queue.
- The dispatch loop admits a few and re-blocks the rest.

Every task goes round that loop about N/(keys × 10) times. Each round is a
`resource.blocked` and `resource.unblocked` pair that a downstream fold must
process: 42 event pairs per task at 800 per key. Extrapolating, 50K tasks over
10 keys means about 800 `canRun` calls and 250 event pairs per task, with
stalls of seconds. This is the shape of multi-key and of unit sets, which is
why it has to be fixed first.

Wall time is also paced by the 50 ms periodic retry, not by the limit. The
ideal at 800 per key is 1.6 s, and it took 4.4 s. Rate windows free up with
time, not with a release, so polling is the only thing that wakes them.

**Empty blocked queues accumulate.** After a 10K-task keyed run, 100 keys left
100 queues and 1,000 keys left 1,000, all empty. `_blockedKeysFor` (a prefix
scan) and `_schedulePeriodicRetry` (`.some()`) walk all of them on every tick.

**A task blocked on two groups waits for the timer.** `both` needs A and B; A
and B are each held by a 50 ms task. Both groups were free at about 50 ms, and
`both` started at 125 ms. It waited in A's queue, A's release found B still
full, and B's release only looked at B's own queue, which was empty.

## Invariants the rework must keep

Each of these is enforced by an existing suite. None may be relaxed.

- **Synchronous admission.** Check and acquire in one turn. This is what makes
  limits hard caps (phase 0 of `keyed-concurrent-limit.md`).
- **Transitions before events.** A `resource.*` or `task.*` listener can
  re-enter the scheduler (`cancel`, `addTask`), and must find every task where
  it really is. No loop may cache a list across an emit. Today's code collects
  first and acts after; the new loop must do the same, or re-read the head on
  every iteration.
- **The fold invariant.** One `resource.blocked` per queue entered, one
  `resource.unblocked` per queue left, with reason `admitted`, `superseded` or
  `cancelled`. Consumers fold this stream.
- **O(1) cancel wherever a task waits** (50K cancels in 30-40 ms, a published
  claim). It relies on single membership plus `tombstone()`.
- **Resident head-of-line admission.** A resident waiter that does not fit
  blocks the waiters behind it, so expensive pools are not starved.
- **Exactly-once settlement and counter cardinality** (`stats.queued`,
  `running`, …), including the thread-capacity race that releases and requeues.

## The target model

1. **Every waiting task is in exactly one wait list,** that of a (group, key)
   currently refusing it. Waiters no longer stay in the ready queue behind a
   blocked head. The dispatch loop moves each refused task to its list and
   keeps scanning, bounded by `maxTasksPerLoop` as `blocked-keyed` is today.
   That makes `getAdmissionQueue()` true, and each task is checked roughly
   once on entry rather than on every pass.
2. **Admit on wake.** When a list is woken, take its head and re-check all of
   its constraints:
   - **all pass and the pool has room:** dispatch it right there, which
     acquires. Because the acquire happens immediately, `canRun` itself
     ends the loop: one release admits as many as it freed, for any group type,
     with no budget arithmetic.
   - **another constraint refuses:** *migrate* the task to that list (O(1)
     with `tombstone`; `migrated` out, `blocked` in), then continue with the
     next head. A migrated task is never rescanned by the list it left.
   - **the pool has no room:** put it at the head of the ready queue, still
     holding nothing, and stop waking this list for this pool. Waking more
     would only move tasks around.
3. **Lists exist only while non-empty,** with an index from group to the keys
   that have waiters. `_retryBlockedTasks` and the periodic timer cost
   O(non-empty lists), not O(keys ever seen).
4. **Wakes come from capacity changes, and polling is the fallback.**
   - A release (`onFinish`) wakes that (group, key)'s list.
   - Optionally, R3: time-based capacity is woken at the group's own
     `resetAt` (rate gauges already publish it) instead of on the next 50 ms
     tick.
   - Out-of-band changes (a raised `ConcurrentLimitGroup.limit`, a custom group
     whose capacity is external) are caught by the periodic retry, or by
     honouring the `slot-released` event groups already emit, which the
     scheduler currently ignores (`core/FyflowScheduler.ts` around line 1860).

What this gives the two features on top:

- **Several keys per task:** the task waits on whichever key refuses it, and
  migrates when woken while another key is still full. That is O(k) per wake.
- **Unit candidate sets:** one list per distinct set, and the release of unit
  `u` wakes the lists containing `u`. Admit-on-wake is what makes "the oldest
  waiter that can use `u` gets it" true. A fresh task can no longer slip in
  between a wake and a dispatch.

## Consumer-visible changes

All within the published contract, but a downstream fold will notice them, so
they go in the release notes:

- `getAdmissionQueue()` and `resource.blocked` now show **every** waiter, not
  the head of the ready queue only. Counts go up, from 1-2 to the real figure.
- `resource.blocked` and `resource.unblocked` volume falls from O(N/keys) per
  task to about 1 per task, plus migrations.
- `queuePosition` values change, because queues are now complete.
- `migrated`, a new `resource.unblocked` reason, appears whenever a waiter moves between lists (see risk 4).

## Phases

- **R0 — measure, and pin down today's behaviour.** Built; see "R0 results"
  below.
- **R1 — housekeeping, low risk. Done.**
  - A blocked queue exists only while it holds a waiter. It is deleted when
    it empties, but never while a retry loop is iterating it: `blockedInUse`
    counts that, by depth, so a listener that empties the queue mid-loop cannot
    detach it from the loop.
  - Keyed waiters are found through a per-group key index, not a prefix scan.
  - The heartbeat skips groups with nobody waiting before calling `canRun`,
    and the "anything blocked?" checks use the O(1) live counter.
  - Measured back to back against `2da4ef3`:
    - lists left after every shape go from up to 1,000 to 0;
    - keyed-wide at 50K runs at +28% throughput, with the worst stall down
      from 79 ms to 36 ms;
    - global at 50K averages 897 ms in both, over three alternating runs, so
      the -7% of a single run was noise;
    - `benchmark:quick` shows nothing beyond single-run noise.
  - Three tests guard it (deletion after every shape, deletion on cancel, keyed
    heartbeat through the index), each mutation-checked.
- **R2 — the core. Done** (see "R2 results" below). Single membership with
  migration, admit on wake, and every waiter in a list.
  - Acceptance:
    - `canRun` per task and events per task stay roughly flat from 1K to 50K
      on every shape;
    - the two-group latency is under 5 ms;
    - every existing suite is green;
    - no `benchmark:quick` regression outside the `TODO_CHECKLIST.md` ranges.
  - Mutation checks:
    - wake the whole list again: the flat-cost test must fail;
    - skip migration: the two-group latency test must fail.
- **R3 — optional: time-based wakes.** Rate groups woken at `resetAt`, and
  `slot-released` honoured. Expected to bring the keyed rate run near its
  1.6 s ideal. It is separable, so it can be decided after R2.

Order against the other plans:
- phase 0 (hard caps) and phase A (single-key `KeyedConcurrentLimitGroup`) do
  not depend on any of this and can go first;
- keyed phase B and unit candidate sets go after R2.

## R0 results

### What was built

- **`tests/harness.ts`: known gaps.** `runTest(name, fn, { knownGap: { reason,
  failsWith } })`. A gap must fail, and with an error containing `failsWith`.
  Failing for another reason is a real failure, and so is passing: the change
  that closes a gap has to delete its `knownGap` in the same commit.
  `tests/harness_test.ts`, run by `deno task test:scripts`, tests those rules.
- **`tests/suites/admission.ts`**, 16 tests (`deno task test:admission`):
  - wake paths with the heartbeat off, so a missed wake is a waiter that never
    starts rather than one that starts 50 ms late;
  - the heartbeat-only sources pinned with the heartbeat on;
  - the targets as known gaps;
  - guards that must stay green through R2: limits and both fold invariants
    under mixed contention (global, keyed, two-group, resident, with
    cancels), re-entrant cancel/add from `resource.acquired`, and flat cost
    for global and two-group shapes.
- **`tests/fixtures/keyedCounterGroup.ts`**: a keyed concurrency group, so
  keyed cost is measured without rate-window pacing. Delete it once
  `KeyedConcurrentLimitGroup` ships.
- **`benchmark/admission.ts`** (`deno task benchmark:admission`, `--json`,
  `--compare base.json head.json`). Tasks yield to the event loop once,
  through a `MessageChannel` rather than `setTimeout(0)`, which costs a ~15 ms
  timer tick on Windows. Without a yield, an all-microtask run starves every
  timer, including the stall sampler, so stalls read as 0.
- **`cancellation.ts` "Cancel A Blocked Task" rewritten.** The audit found it
  would certainly fail under R2. It took `stats.queued > 0` from waiters
  stranded behind a blocked head; it now gets them from a full pool, under
  either model. Mutation-checked: a wrong `queued` decrement on a blocked
  cancel fails it.

### Mutation checks

Each mutation was made, run against only the tests meant to catch it, and
reverted.

| Mutation | Caught by |
|---|---|
| no retry on task release | all four "Wake:" release and cancel tests |
| the heartbeat does nothing | all three "Heartbeat:" tests |
| `ConcurrentLimitGroup.canRun` always true | mixed contention: "g reached 15 against a limit of 3" |
| `admitted` exit not announced | mixed contention: membership unbalanced |
| a task cancelled by its own `resource.acquired` still dispatched | re-entrant: "Cancelled at admission, yet ran" |
| a waiter cancelled in transit re-queued | not by this suite. Caught by cancellation "Cancel From An Admitted Unblock" |

### The known gaps R2 and R3 must close

R2 closed all four of its gaps; the R3 one remains.

| Test | Closed by |
|---|---|
| Wake: the second group's release admits a two-group waiter | R2, migration |
| Wake: a resident release admits a waiting pool | R2 |
| Wake: a rate window rolling over admits without polling | R3 |
| Admission queue lists every waiter | R2 |
| Keyed admission cost is flat in queue depth | R2 |

### Baseline

Measured at `0adeefb` (benchmark code from this branch), Deno 2.9.6, Windows.
The JSON is not committed. Re-run the base before comparing, per
`TODO_CHECKLIST.md`.

| shape | N | ms | canRun/task | blocked/task | stall ms | lists left | admission seen / waiting | heap MB |
|---|---|---|---|---|---|---|---|---|
| none | 50,000 | 556 | 0 | 0 | 6 | 0 | - | 147 |
| global | 50,000 | 784 | 9 | 2 | 10 | 1 | 2 / 47,273 | 43 |
| keyed (10 keys) | 1,000 | 171 | 147 | 48 | 9 | 10 | 796 / 796 | 5 |
| keyed (10 keys) | 10,000 | 16,118 | 1,497 | 498 | 118 | 10 | 9,796 / 9,796 | 79 |
| keyed (10 keys) | 50,000 | **445,816** | 4,892 | 1,630 | **881** | 10 | 20,011 / 49,796 | 182 |
| keyed-wide (1,000 keys) | 50,000 | 4,691 | 7.3 | 1.8 | 49 | **1,000** | 1,041 / 48,772 | 131 |
| two-group | 50,000 | 3,213 | 11.7 | 2 | 20 | 2 | 4 / 49,240 | 110 |
| keyed-rate | 1,000 | 573 | 17 | 5 | 33 | 10 | 652 / 652 | 20 (2.9× the ideal time) |

| latency | mean | max |
|---|---|---|
| two-group wake (heartbeat on) | 14.8 ms | 16.2 ms |
| resident handoff (heartbeat on) | 31.8 ms | 34.8 ms |

Reading it:
- **Keyed is the choke.** 50K tasks over 10 keys took 7.4 minutes at
  112 tasks/s. That is about 81 million `resource.blocked` events.
- **Global and two-group are flat, but their admission queue is wrong.** It
  lists 2-4 waiters while ~49,000 wait.
- **The leak scales with key count**: 1,000 empty lists after the wide run.
- **R2's acceptance:**
  - every `canRun/task` and `blocked/task` flat across sizes;
  - admission seen ≈ waiting;
  - lists left 0 (R1);
  - both latencies under 5 ms with the heartbeat on;
  - no row's throughput below its base beyond the `TODO_CHECKLIST.md`
    ranges.

### What the order audit found

Every assertion in the suites, browser specs and docs tests that depends on
admission order, `resource.*` counts, `queuePosition`, the admission queue or
heartbeat timing was classified as either a contract (promised in
`AGENTS.md`, must keep passing) or incidental.

- **Will certainly fail under R2, now fixed:** the cancellation test above.
- **Two dependencies R2 has to design around**, now risks 14 and 15 below:
  - resident head-of-line on arrival;
  - collect-then-dispatch overshoot.
- **Stricter than the documented bound, and therefore useful guards.** These
  peak assertions are tighter than `AGENTS.md` §1's
  `maxThreads × maxConcurrentTasks`:
  - `core.ts` 381, 414 and 576;
  - `docs.ts` 261 and 991;
  - `tests/browser/core.spec.ts` 225.

  They pass today only because the blocked queue holds about 1 task. They are
  what catches risk 15, and they become the contract once phase 0 of
  `keyed-concurrent-limit.md` lands.
- **Count-sensitive contracts R2 must keep:**
  - `docs.ts:1703` asserts exactly one `resource.unblocked` for a waiter whose
    group stays full, so R2 must not add a block/unblock cycle there;
  - `resource-events.ts` criterion 4b requires the fold and
    `getAdmissionQueue()` to agree *in order*, so every push must go to the
    tail.
- **Stale comments to fix in R2's commit**:
  - `core.ts:739-743`;
  - `cancellation.ts:459` and `:1168`;
  - `resource-events.ts:1047-1052`;
  - `core.ts:646`, whose test "periodicRetryIntervalMs Is Honoured" never
    exercises the interval (a concurrency group is woken directly). The
    admission suite's heartbeat tests now cover it.
- **`AGENTS.md` text R2 changes:**
  - §6 "How a blocked task wakes up";
  - the `admitted` definition ("Moved to the ready queue");
  - "A blocked task leaves its queue five ways" (six, with `migrated`);
  - `position`, "the head gates the rest", which is not true today for
    task-held groups;
  - the `periodicRetryIntervalMs` description.

## R2 results

### What changed

- **Admit on wake** (`_admitFromQueue`). The head is examined in place, and each
  waiter is admitted and dispatched - `onStart` included - before the next is
  examined, so `canRun()` ends the loop (risk 15). A waiter whose pool has no
  free slot waits in the ready queue and counts against what the wake hands
  out (`parked`), or a full pool would drain the whole list into its ready queue.
- **A wake pump.** A wake requested during a wake is queued and run after it,
  never nested (risk 2).
- **Migration** (`_migrate`), with `unblocked.reason: 'migrated'` (risk 4).
  - Out of a task-held queue, to the other task-held group that refuses, or -
    if the pool's worker cost does not fit - to the resident group it is short
    of. That replaced a `break` that stopped the whole task-held queue at it.
  - **Never out of a resident queue** (risk 6, as planned). A resident head whose
    task-held group is full keeps its place and waits for the heartbeat. Moving
    it let the pools behind it take its units: tested.
  - **The entry is announced before the exit**: `blocked` for the new queue, then
    `unblocked: migrated` for the old one. Built the other way round, a cancel
    from the `migrated` listener emitted an exit from the new queue with no
    entry (risk 5); the test for it failed before the swap.
- **Resident releases wake their group** in a microtask, because the pool
  notifies from inside its own teardown (risk 1).
- **Arrivals are head-of-line** behind resident waiters (risk 14, decided).
- **The dispatch loop moves every refused arrival into its list**, up to
  `maxTasksPerLoop` per pass, instead of stopping at the first.
- **The capacity race** (risk 3) marks the pool, so neither `_poolHasRoom` nor
  `_canDispatchTask` offers it work until the 10 ms retry.
- **`superseded` is no longer emitted**: it needed a task in two queues. It stays
  in the union, so code naming it still compiles.

### Mutation checks

| Mutation | Caught by |
|---|---|
| arrivals skip head-of-line | resource-events criterion 4; docs "Resident Admission Is Head-Of-Line For Arrivals" |
| no migration (re-blocked through dispatch instead) | admission two-group wake: the moves read `admitted` then `blocked` |
| no wake on resident release | admission "A Resident Release Admits A Waiting Pool" |
| `parked` not counted | admission "A Full Pool Takes Only What A Release Freed" |
| a task-held queue stops at a waiter short of a worker | admission "A Task-Held Waiter Short Of A Worker Moves To The Resident Queue" |
| a resident head migrates | admission "A Resident Head Whose Own Group Is Full Keeps Its Place" |
| exit announced before entry | admission "Cancel During A Migration Leaves No Waiter Behind" (found the bug) |
| the pump's re-entrancy guard removed | **nothing**. No path found nests a wake; it is defensive |

### Measured, back to back against `8836eab` (R1), Deno 2.9.6, Windows

| | R1 | R2 |
|---|---|---|
| keyed (10 keys), 50K | 445.8 s (R0 baseline) | 1.18 s |
| keyed `canRun` / `blocked` per task, 50K | 4,892 / 1,630 | 5 / 1 |
| keyed-rate, 1K | 577-589 ms, `canRun` 15.5 | 560-566 ms, `canRun` 4.8 |
| global / two-group / keyed-wide, 50K (2 runs each) | 1,255, 1,091 / 1,276, 1,254 / 1,972, 1,942 ms | 1,076, 1,305 / 1,041, 1,082 / 1,270, 1,531 ms |
| admission queue seen / waiting, two-group 50K | 3 / 47,866 | 49,810 / 49,810 |
| two-group wake, heartbeat on | 15.7 ms | 0 ms |
| resident hand-over, heartbeat on | 31 ms | 0.8 ms |
| worst stall, global / two-group, 50K | 7-9 / 6-9 ms | 25-34 / 26-36 ms |
| peak heap, global 50K | 166-171 MB | 219-228 MB |

`benchmark:quick` is within the bands in both run orders; the one move past 10%
(-12.8% on the 50K stress scenario) was +1.2% with the order reversed.

**Two costs, recorded as follow-ups in `TODO_CHECKLIST.md`:**
- the worst stall, from moving up to 1,000 refused arrivals per dispatch pass;
- the heap, a `Set` entry per waiter (risk 9).

**Found on the way:** a pool's first worker becoming ready dispatches nothing,
so arrivals during its creation wait for the next settle or the heartbeat. R1
does the same. It is in `TODO_CHECKLIST.md`.

## Risks

Ranked by how likely they are to ship unnoticed. Each risk names the test or
measurement that has to catch it.

### Correctness

1. **The periodic retry hides lost wake-ups.** The 50 ms tick wakes everything
   eventually, so an R2 bug that forgets a wake path passes every suite with
   ~50 ms of extra latency per affected task. This is already happening:
   - a **resident release wakes nobody** today. `_releaseResidentFor` only
     notifies the scheduler for events, and returns early when nobody
     observes them, so a task blocked on resident capacity waits for the tick;
   - the two-group case above.

   Mitigation: run the wake tests with `periodicRetryIntervalMs` set
   effectively to infinity, so a missed wake shows up as a timeout rather than
   as latency. List every capacity source and its wake path:
   - task release;
   - resident release on teardown, idle termination, shutdown and a forced
     stop;
   - the thread-capacity race;
   - cancel of a holder;
   - a raised `limit`;
   - rate windows;
   - custom groups.

   Each needs a test, mutation-checked by deleting that one wake.
2. **Re-entrancy.** Admit-on-wake dispatches inside the release path, which
   runs inside `_settleTask`. A `resource.acquired` listener that cancels or adds
   tasks re-enters the release → wake → dispatch chain recursively, so the stack
   depth grows with the listener's behaviour, and a loop that cached its list
   head acts on a stale one. Mitigation: wakes go through a work queue drained
   by one non-reentrant pump (if a drain is already running, enqueue and
   return), never by direct recursion. Today's "collect, then act" pattern does
   the same job and must be kept. The existing re-entrancy tests in settlement
   and cancellation are the guard; add one that cancels from `resource.acquired`
   during a wake that is admitting several tasks.
3. **A spin on the thread-capacity race.** When `enqueueNoQueue` rejects with
   "Worker at maximum concurrent task capacity", the task releases and requeues.
   Under admit-on-wake that release wakes the list, which can dispatch the next
   waiter to the same full worker: a microtask loop with no timer in it. Today
   the `setTimeout(10)` breaks the cycle. Mitigation: while a pool is in that
   state, a wake must not dispatch to it, only move its waiters to the ready
   queue. Add a test that forces the race, with a worker whose
   `canAcceptTask()` lies.
4. **`superseded` changes meaning.** `AGENTS.md` §6 defines it as "admitted via a
   *different* group and spliced out of this one". Migration is the opposite:
   the task is still waiting, in another list. Downstream folds that read
   `superseded` as "now running" would be wrong. Mitigation: a new reason,
   `migrated`. The union is documented as open ("do not switch exhaustively"),
   so adding a reason is within contract; reusing one is not. The membership
   invariant (Σ blocked − Σ unblocked = 0 per group, key and holder) still holds,
   because a migration is one exit and one entry.
5. **Cancel bookkeeping.** O(1) cancel tombstones the one list recorded in
   `task._blockedQueueId`. Migration must update that before it emits, or a
   cancel from the `blocked` listener tombstones the wrong list and corrupts
   both lengths silently (`tombstone` trusts the caller). The cancellation suite
   covers every place a task can be; add "cancel during migration".
6. **Head-of-line for resident waiters.** Today's break is a scan of one queue.
   Once waiters migrate between lists, a resident waiter's "nothing behind me
   may overtake" has to hold across lists, or a cheap pool overtakes an expensive
   one by sitting in a different list, which reopens the starvation the rule
   exists for. Mitigation: resident waiters do not migrate in R2. They stay in
   their group's list, as now. The existing resident-groups suite is the guard.

### Performance

7. **Migration ping-pong.** A two-key task whose keys are both busy migrates
   C: → D: → C: once per release on the key it waits on. Each move is O(1), but
   one release can migrate every multi-key head in its list, and those tasks
   come back on the next release elsewhere. In an adversarial mix (two hot keys,
   many tasks naming both, a stream of single-key tasks), the work per task
   could again grow with run length. Mitigation:
   - a migrated task joins the tail, so it waits a full queue cycle before it
     can move again;
   - a per-wake scan budget, beyond which a wake defers to the next release;
   - an explicit ping-pong scenario in the `admission` benchmark with the same
     flat-cost acceptance. If it fails, fall back to waiting on the scarcest key.
8. **Fast paths must not regress.**
   - No-group tasks: `_checkAndBlockResources` returns at once without groups.
     The rework must not add per-task work there: no list bookkeeping, no
     sequence numbers.
   - A global group: today's `break` makes a pass O(1) once the group is full.
     "Move every refused task to its list and keep scanning" makes it O(tasks
     refused). That is once per task, but it is real work in a hot loop.
     Measured today: 178K tasks/s at limit 7 and about 2 `resource.blocked` per
     task. The new model should be ~1 per task. Acceptance: `benchmark:quick`
     and the `admission` global shape run back to back against the base, within
     the `TODO_CHECKLIST.md` ranges. Benchmarks are position-dependent, so
     compare full-suite runs with full-suite runs.
9. **Memory.** Blocked lists track membership with a `Set` (needed for O(1)
   `includes` and cancel); ready queues do not. Moving every waiter into lists
   adds a `Set` entry per waiting task: tens of MB at a million waiting tasks.
   Measure peak heap in the 50K and 200K shapes. If it matters, move membership
   to a field on the task (`task._blockedQueueId` already says where it is)
   instead of a `Set` per queue.
10. **Consumer-side cost.** `getAdmissionQueue()` will return every waiter:
    50K entries rather than 1-2. A viewer that polls it every frame now builds
    a 50K array 60 times a second. That is correct, but it is a cost shift onto
    consumers. Document that the event stream is the incremental view and
    `getAdmissionQueue()` a snapshot.

### Behaviour changes a consumer can see

11. **Dispatch order changes.**
    - Today a woken task is put back at the head of the ready queue and runs in
      the dispatch loop's worker-type order. Under admit-on-wake it runs in list
      order, immediately.
    - Across pools sharing a group, which pool gets freed capacity first can
      differ.
    - Tests that assert an order may encode today's incidental order.

    Mitigation, in R0: classify every order-sensitive assertion as a contract
    (keep it, and it must still pass) or incidental (rewrite it deliberately,
    in its own commit, before R2). This keeps R2's diff from being "tests
    edited until green".
12. **Downstream folds.** Event volume per task drops, `migrated` appears,
    admission counts go up from 1-2 to the real number, and `queuePosition`
    becomes meaningful. A sibling library folds this stream. Mitigation: ship it
    as its own minor release, with a release note naming each change, and tell
    the consumers before it ships, not after. A fold that is still consistent but
    wrong is exactly the failure the invariants in §6 exist to expose.
13. **Latency improves, which is also a change.** Tasks blocked on resident
    capacity, or on a second group, start up to ~50 ms sooner. Code that
    accidentally relied on that slack (for example, observing a task as blocked
    before it starts) could break. This is a low risk, and it gets one line in
    the release notes.

### Found by the R0 order audit

14. **Resident head-of-line holds only on the retry path today.**
    `_checkAndBlockResident` admits a *newly arriving* task whose worker cost
    fits without looking at the resident queue. Only
    `_retryBlockedTasksForGroup` applies "nothing behind may overtake". So
    `AGENTS.md`'s "a waiting expensive pool is not overtaken by cheaper ones"
    is not literally true today, and `resource-events.ts`'s head-of-line
    scenario *depends* on that overtaking: its churn rounds finish only
    because new Small-pool tasks get past `big-1`. If R2 makes arrivals queue
    behind existing waiters, which the contract implies, that scenario hangs.
    **Decided (2026-11-04): the contract wins.** A newly arriving task that
    would need a new worker queues behind existing resident waiters of that
    group, rather than being admitted past them. The `resource-events.ts`
    head-of-line scenario is rewritten so it does not rely on overtaking.
    The release notes say that arrivals no longer jump the queue. The
    alternative, narrowing `AGENTS.md` to "a waiter is not overtaken by
    waiters behind it", was rejected: it leaves a stream of cheap arrivals
    able to starve an expensive pool, the thing the rule exists to stop.
15. **Collect-then-dispatch overshoots once queues are complete.** Today's
    retry loop collects waiters while `canRun()` is still true, because
    `onStart` only runs later, in `_dispatchTask`. That is harmless only
    because the blocked queue holds about 1 task. With every waiter queued,
    the same shape would admit the whole queue on one release, past the limit.
    Admit-on-wake must call `onStart` for each waiter before checking the next.
    The strict peak assertions listed in the audit section are the guard;
    mutation-check R2 by collecting first and confirming they fail.

### Process

- **Blast radius.** R2 rewrites the code that the settlement, cancellation and
  resource-events suites (92 tests) were written to protect. Keep R1 and R2
  separate commits, keep R3 out of the same release as R2, and land R0's
  benchmark and the order-classification first, so that R2 is measured against
  a fixed baseline.
- **Rollback.** A flag that keeps the old wake path until R2 has a release
  behind it would double the code to keep correct, in the most delicate part of
  the scheduler. The proposal: no flag. Revert the R2 commit instead, which is
  why it must be one commit.

## Open questions

- **Cross-pool fairness on wake.** If a woken task's pool is full, a task of
  another pool can take the freed capacity first. Accept it and document it,
  or reserve? The proposal: accept. Reservation is the starvation machinery
  we deliberately avoided.
- **Should the dispatch loop move a refused task to its list and keep
  scanning, for non-keyed groups too?** This is what makes the admission queue
  true, and it is also a change in event volume and order for consumers. The
  proposal: yes, flagged in the release.
- **Resident head-of-line inside the new lists.** It needs the same "stop
  scanning" break that `_retryBlockedTasksForGroup` has now. Confirm it composes
  with migration: a resident waiter that migrates must keep its place
  relative to the waiters it was blocking.
