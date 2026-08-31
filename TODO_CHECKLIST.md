# FyFlow Development TODO Checklist

## Status Legend
- 🔵 **Not Started** - Task has not been initiated
- 🟡 **In Progress** - Task is currently being worked on
- 🟢 **Completed** - Task has been finished and verified

> **Note**: This checklist serves as a central task tracking document for FyFlow development. Each task should be refined and clearly defined before implementation. Claude will reference this file when performing changes to maintain alignment with project goals and priorities.

## Current Development Tasks

### Core Framework Improvements

#### 🟢 Strict Resource Limits with Token-Based Allocation (Opt-in)
**Goal**: Guarantee hard limits for physical resources (GPU memory, API quotas,
file handles) that must never be exceeded, via async token-based allocation.

**Outcome**: Implemented as `StrictLimitGroup` (commit `564d822`), then removed
again (commit `a035e45`) along with `groups/strictLimitGroup.ts` and
`examples/strict-limits.ts`. Both remaining group types are optimistic, so a
limit can still be briefly exceeded by up to `maxThreads × maxConcurrentTasks`
during race conditions.

The full design - token acquisition, FIFO wait queues, deadlock prevention
strategies and the unified `ResourceGroup` interface - is preserved in the git
history of this file if the work is ever picked up again. The unified interface
part of it did survive, as `groups/resourceGroup.ts`.

**Status**: 🟢 Closed - implemented and deliberately reverted

## Known Issues / Follow-ups

#### 🟢 A blocked task whose pool is missing was dropped silently
`_retryBlockedTasksForGroup` shifted a task out of a blocked queue and then did
`if (!pool) continue;`, leaving it in no queue at all. It never settled - no
`task.failed`, no `user_action`, no rejection - so a caller awaiting it waited
forever and `stats` never accounted for it.

`addTask`/`addTasks` validate worker types against `workerPools`, and the map is
never mutated after construction, so this should be unreachable. It was silent
when it was not.

**Outcome**: the task is now failed terminally via `_failOrphanedTask` -
`task.failed`, then `task.user_action` unless it is optional, and its promise
rejects with `Worker pool "X" no longer exists`. Two departures from the
ordinary failure path, both deliberate:

- **Retries are skipped.** The pool is gone, so another attempt cannot succeed,
  and `readyQueuesByWorker` keeps its entry for the type forever - a retry would
  requeue into a queue no dispatch can drain and leave `stats.queued`
  permanently above zero, wedging `scheduler.completed` rather than unwedging
  it.
- **It does not go through `_settleTask`**, which decrements `stats.running`. A
  blocked task was never running; it gave up its `queued` count when it left the
  ready queue, so settling it that way would decrement a count belonging to a
  different task.

The waiter is also swept out of every other blocked queue, each exit emitting
`resource.unblocked` with `reason: 'orphaned'`, so a consumer's queue fold
drains. Tested by deleting the pool from `workerPools` while a task is blocked -
the only way to reach a branch that guards an impossible state. The test is
bounded rather than awaiting the promise, because under the old behaviour it
never settles and an unbounded await would hang CI instead of failing it.

**Status**: 🟢 Completed


#### 🟢 addTasks strands tasks with an unknown workerType
`addTask` threw `Unknown worker type: X`, but `addTasks` skipped the queue push
while still incrementing `stats.queued`, leaving a task that could never
dispatch and a queued count that never returned to 0 - so `scheduler.completed`
never fired again and a typo in a batch silently wedged the run.

**Outcome**: `addTasks` now validates every task's worker type before touching
any state, so it throws like `addTask` and the call is atomic - a rejected batch
queues nothing and leaves no orphan entries in the task map. BREAKING for any
caller that relied on unknown types being silently skipped, though that path
wedged the scheduler.

**Status**: 🟢 Completed

#### 🟢 Worker lifecycle events are unreachable
`worker.initialization.*`, `worker.setup.*` and `worker.teardown.*` were
dispatched on the internal wrappers and never forwarded, so no consumer could
observe them - despite being listed as a feature. Inline and threaded wrappers
also emitted different subsets with different payload shapes, so forwarding them
as-is would have shipped an API that silently stopped working when a pool
switched to `inline: false`.

**Outcome**: Normalised first, then forwarded. Both wrapper types now emit all
eight with `{ workerId, workerType, timestamp }` plus `duration` on `*.completed`
and `error` on `*.failed`; threaded workers report setup timing from inside the
thread via new `setup_started` / `setup_completed` protocol messages.

Three latent bugs surfaced while testing this:
- `WorkerManager.shutdown()` removed listeners *before* terminating workers, so
  teardown events could never be seen on the shutdown path. Listeners are now
  dropped last, guarded by a `shuttingDown` flag so failures during teardown are
  not mistaken for crashes worth restarting.
- A threaded worker that threw during construction produced no event at all: the
  error carried `taskId: 'init'`, which has no registered callback, so it was
  dropped and initialization hung until its 30s timeout.
- `ThreadWrapper.runTask` awaited initialization inside an async promise
  executor, so an initialization rejection escaped as an unhandled rejection and
  crashed the process rather than failing the task.

**Status**: 🟢 Completed

#### 🟢 Builds do not clean their output directory
`dist/types/core/dagScheduler.d.ts` survived the deletion of its source in
`dd57083` by months, and `package.json` ships `dist/**/*`, so it would have been
published.

**Outcome**: `esbuild.config.js` now clears `dist/` (library build) or
`dev-dist/` (dev build) before writing anything. The stale declaration does not
reappear.

**Status**: 🟢 Completed

#### 🟢 Intermittent failure in the error handling suite
`Task Requeuing on Worker Failure` failed only inside a full test run, never
standalone, and reliably on Node.

**Outcome**: The test slept a fixed 200ms waiting for a worker that
self-terminates after 50ms - but two worker threads have to start first, and
measured Node thread startup is 50-70ms each, so under a full run's load the
budget expired before the event arrived. It now polls for the event with a 5s
ceiling, passing as soon as it fires. Four consecutive full Node runs and five
Deno runs clean.

Ruled out the `shuttingDown` guard added in `a74983e`, which suppresses exactly
this event during shutdown: with the guard disabled the failure still reproduced
3/3, so it was never the cause.

**Status**: 🟢 Completed

#### 🟢 worker.restart_limit_exceeded missed construction failures
`_handleWorkerFailure` decided whether to requeue a worker's tasks with
`canRestart !== false`, but decided whether to replace the worker with
`canRestart === true`. Construction failures set no `canRestart` at all, so they
fell through both branches of the second decision: no replacement was scheduled
and `worker.restart_limit_exceeded` was never emitted. A pool that could not build
a worker kept the dead one in its `maxThreads` slot forever and stayed silent to
anyone monitoring that event.

The two wrappers also disagreed. InlineWrapper reported a construction failure as
`worker.failed`; ThreadWrapper emitted only `worker.initialization.failed`, so the
same fault produced different task outcomes - inline stranded its tasks, threaded
failed them.

**Outcome**: ThreadWrapper now reports construction failures as `worker.failed`
too, and the replacement decision uses `canRestart !== false`, matching the
requeue decision beside it. Both wrappers now behave identically: tasks are
requeued, the pool retries up to `maxWorkerRestarts`, then emits
`worker.restart_limit_exceeded`. Covered by a settlement test asserting the alarm
fires for both pool types.

**Status**: 🟢 Completed

#### 🟢 A task could be settled twice
An outcome reaches the scheduler from two directions - the pool's `task.failed`
event and the settling of the pool's task promise - and each was a full,
near-duplicate settle. Measured on an ordinary task that simply throws:
`task.failed` emitted twice, `stats.failed` counted 2 (and **5** for one task
with `maxRetries: 2`), the terminal state raced from `user_action` to `failed`
milliseconds later, `task.user_action` fired while retries still remained so the
same task was requeued and rejected at once, and `stats.done` double-counted a
task that succeeded after a retry.

Worse, the event path never released resources: a worker dying with
`requeueFailedTasks: false` settled only through it, so the task's resource group
slot was never returned - measured as `running=1/2` still held after the task had
settled. A pool losing workers slowly drained group capacity until it stopped
dispatching.

**Outcome**: One `_settleTask` routine, idempotent per attempt via a flag cleared
on each dispatch. Both paths delegate to it; neither could be removed, since each
is the only settle path in some scenario (ordinary throw reaches both, a worker
dying with requeue off reaches only the event, a worker that cannot be
constructed reaches only the promise). Resources are released there and nowhere
else. `stats.failed` and `stats.done` now count tasks rather than attempts.

Covered by `tests/suites/settlement.ts` - 14 tests, 10 of which failed against
the old code.

**Status**: 🟢 Completed

#### 🟢 A requeued task could also be counted as failed
`_settleTask` made settlement idempotent per attempt, but requeue was still a
separate handler that cleared the settle flag, so the same failure could requeue a
task AND settle it as failed. Three tasks against an unstartable pool reported
`queued: 3` alongside `failed: 2` - five outcomes for three tasks - which cannot
both be true.

**Outcome**: Requeue is now a third outcome of `_settleTask`, alongside retry and
terminal, so one decision covers all three and the counters move once. The settle
flag stays set until the next dispatch clears it, so a late outcome from the
abandoned attempt cannot settle the new one. Two tests assert that
`queued + running + done + failed` never exceeds the task count, for both pool
types.

**Status**: 🟢 Completed

#### 🔵 Browser specs are not type checked
`deno task check` now covers the library, tests, examples and benchmarks, but
`tests/browser/*.spec.ts` is excluded. Those specs carry ~43 pre-existing errors,
almost all from `page.evaluate()` callbacks that run in browser context:
untyped `catch (error)` bindings (TS18046), and browser-only globals such as
`window.FyFlowTestRunner` and `performance.memory` (TS2339). Playwright does not
type check them either, so they are currently unchecked. Fixing them means
typing the `page.evaluate()` generics rather than a config change.

**Status**: 🔵 Not Started

#### 🔵 Benchmark scenarios are not isolated from each other
A scenario's measured throughput depends heavily on where it sits in the suite,
because earlier scenarios leave heap pressure behind. The same commit produces
6,282 tasks/sec for `Fire-and-Forget - 50K Tasks` inside the baseline suite and
~10,600 tasks/sec on its own. That makes cross-run comparison fragile and is
the main reason a stored baseline is hard to compare against. Forcing GC or
running each scenario in a fresh process would make the numbers comparable.

**Status**: 🔵 Not Started

#### 🟢 Benchmark "dependencies" category is misnamed
`SCENARIO_CATEGORIES.DEPENDENCIES` held three scenarios named "Deep Chain",
"Wide Tree" and "Mixed Dependencies", named after DAG workloads the framework
cannot express. `dependencyConfig` was declared on `BenchmarkConfig`, set on
those three, and never read - the task generator always produced independent
tasks.

**Outcome**: "Wide Tree" was byte-identical to
"Large Volume - 1K Independent Tasks (Inline)" once `dependencyConfig` was
ignored, so the suite was running the same benchmark twice under two names; it
is deleted. The other two are renamed for what they measure
("Inline Serial - 1K Tasks, 1 Per Instance" and
"Large Volume - 5K Independent Tasks (Inline)"). `dependencyConfig` and its
report-generator branches are gone.

Categories were index slices, so removing a scenario would have silently
reassigned every category after it - they are now looked up by name, which
throws on a typo at import. That also let the 8 previously uncategorised
scenarios be grouped: every scenario is now reachable by category, under two new
ones, VOLUME and CONCURRENCY.

**Status**: 🟢 Completed

#### 🔵 Performance suite is not part of the default test run
`tests/performance/contention-scaling.ts` is reachable via `test:performance`
but not included in `runAllSuites()`, so contention regressions are not caught
by `npm test` / `deno task test`.

**Status**: 🔵 Not Started

#### 🟢 Unbounded task retention
Completed tasks were never evicted from `FyflowScheduler.tasks`, so a long-lived
scheduler grew without limit - measured at ~7MB per 1,000 completed tasks with
8KB payloads, since each entry pins its payload and result.

**Outcome**: Added the opt-in `maxCompletedTasks` scheduler option (default
unset = retain everything, since most workloads have bounded task counts and
want completed tasks inspectable). With it set, growth flattens instead of
climbing: 8 batches of 2,000 tasks grew 210MB unset versus 102MB and flat from
batch 4 with a limit of 1,000.

Also removed a dead recovery scan in `_checkCompletion()` that walked every task
ever created looking for a `'dispatched'` state. That state stopped being
assigned in `dd57083` but its recovery block was left behind, so the scan always
found nothing while costing O(tasks) per call - 12ms at a million retained tasks.

**Status**: 🟢 Completed


## Task Lifecycle Process

Each task follows a standardized lifecycle with clear phases and deliverables. This ensures consistent quality, proper documentation, and maintainable codebase organization.

### Phase 1: Task Refinement
Before implementing any task:
1. **Define Scope** - Clearly outline what the task involves
2. **Identify Dependencies** - List prerequisite tasks or components
3. **Estimate Complexity** - Assess the effort and time required
4. **Plan Implementation** - Break down into smaller sub-tasks if needed
5. **Set Success Criteria** - Define how completion will be measured
6. **Update Status** - Mark task as 🟡 **In Progress**

### Phase 2: Implementation & Verification
During task implementation:
1. **Code Implementation** - Execute the planned changes
2. **Basic Functionality Testing** - Verify core functionality works
3. **Mandatory Benchmarking** - Run performance tests (see Mandatory Benchmarking Process below)
4. **Performance Validation** - Ensure results meet acceptance criteria
5. **Integration Testing** - Verify compatibility with existing features

### Phase 3: Completion & Documentation
After successful implementation:
1. **Update CLAUDE.md** - Document new features, API changes, and usage examples
2. **Update Project Structure** - Reflect any file/directory changes in documentation
3. **Archive Task** - Move task to "Recently Completed" section with brief summary
4. **Remove Implementation Details** - Clean up verbose planning details from active tasks
5. **Update Status** - Mark task as 🟢 **Completed**
6. **Commit Changes** - Include implementation, documentation updates, and task archival

### Documentation Standards
When updating CLAUDE.md during completion:
- **API Changes**: Update usage examples to show new patterns
- **New Features**: Add section describing capabilities and benefits
- **Project Structure**: Update file/directory listings if changed
- **Commands**: Update any new or changed CLI commands
- **Performance**: Include any significant performance improvements

## Mandatory Benchmarking Process

**IMPORTANT**: After implementing any task, the following benchmarking process MUST be completed before considering the task done:

### 1. Performance Impact Assessment
```bash
# Run quick benchmark with multiple runs for variance analysis
deno task benchmark:quick --runs 3

# Compare results with previous baseline in benchmark-baseline-comprehensive.md
```

### 2. Performance Acceptance Criteria

**✅ ACCEPTABLE**:
- Throughput changes within ±10% of baseline
- Scheduler overhead changes within ±20% of baseline
- Variance coefficient of variation (CV) ≤ 10% for stable metrics
- Memory usage growth ≤ 20% unless explicitly optimizing memory

**⚠️ REQUIRES INVESTIGATION**:
- Throughput regression > 10%
- Scheduler overhead increase > 20%
- High variance (CV > 10%) in previously stable metrics
- Significant memory usage increases without clear justification

**❌ UNACCEPTABLE**:
- Throughput regression > 25%
- Scheduler overhead increase > 50%
- System crashes or hangs under benchmark load
- Memory leaks or excessive memory growth

### 3. Baseline Update Process

Only after manual review and approval of benchmark results:

1. **Review Results**: Analyze performance impact and variance
2. **Document Changes**: Note any significant performance changes in commit
3. **Update Baseline**: Replace `benchmark-baseline-comprehensive.md` with new results
4. **Commit Changes**: Include both implementation and new baseline

### 4. Available Benchmark Commands

```bash
# Quick benchmarks (7 core scenarios: inline volume + threading)
deno task benchmark:quick

# Quick benchmarks with variance analysis
deno task benchmark:quick --runs 3

# Specific scenarios
deno task benchmark --scenarios "Large Volume - 1K Independent Tasks"

# Group contention analysis
deno task benchmark:contention

# Full benchmark suite
deno task benchmark

# Generate detailed reports
deno task benchmark:quick --format markdown --output latest-results.md
deno task benchmark:quick --format csv --output results.csv
```

### 5. Confidence Ranges (Current Baseline)

**Current Baseline**: See `benchmark-baseline-comprehensive.md` for current performance baseline covering:
- **Volume Tests**: 10K and 50K independent tasks (with promises)
- **Fire-and-Forget Mode**: 10K and 50K tasks (without promise creation)
- **Threading Efficiency**: 200 tasks on 2 threads, 200 tasks on 4 threads
- **High Contention**: 1K and 10K tasks with limited group slots

**Optional Promise Feature Analysis**: See `benchmark-comparison-optional-promises.md` for detailed before/after comparison.

Key performance characteristics (2026-08-27 baseline, Deno on Windows):
- **Inline Workers (Promises)**: ~62,000 tasks/sec (10K), ~10,200 tasks/sec (50K)
- **Fire-and-Forget Mode**: ~28,000 tasks/sec (10K), ~4,900 tasks/sec (50K)
- **Threading Efficiency**: 2 threads 96.3%, 4 threads 96.9% (pre-warmed)
- **High Contention**: 1K/4 slots 97.9%, 10K/4 slots 98.4%
- **Worker Startup**: threads ~12-54ms each cold; inline instances ~85-325us

**Reading `Overall Efficiency`**: it is achieved average concurrency divided by
the concurrency the scenario configured - "how much of the parallelism I asked
for did I get". It is only meaningful when tasks do real work. The high-volume
inline scenarios use no-op tasks with `taskDelay: 0`, so almost no time is spent
inside worker code and they read 1-5%; that is a measure of scheduler-overhead
dominance, not lost parallelism. The threaded and contention scenarios do real
work and their 96-98% is the number to watch for regressions.

Scenarios pre-warm their workers, so startup is excluded from efficiency. The
three `Worker Startup` scenarios carry that dimension instead - read their
`Worker Startup` line, not their efficiency, which is low by construction.

### 5a. Comparing runs - read this before calling something a regression

**Scenario order matters more than most code changes.** Later scenarios in a
suite run under heap pressure left by earlier ones, so the same scenario can
differ by 40-70% between a full-suite run and an isolated one. Measured on the
same commit:
- `Fire-and-Forget - 50K Tasks`: 6,282 tasks/sec in the baseline suite,
  ~10,300-10,800 tasks/sec run on its own
- `Threading Scalability - 200 Tasks, 2 Threads`: ~102-144 tasks/sec inside
  `benchmark:quick`, ~197-219 tasks/sec run on its own

Only compare like with like: full-suite numbers against full-suite numbers, in
the same category and the same order. To attribute a change to code rather than
environment, run the same scenarios on both commits back to back (a
`git worktree` at the older commit works well) instead of comparing against a
stored baseline recorded on another day.

### 6. Regression Detection

Performance regressions are automatically flagged when:
- Results fall outside confidence ranges above
- High variance scenarios (CV > 10%) indicate instability
- Memory usage increases significantly without justification

## Notes
- **Follow the Task Lifecycle Process** for all tasks (Refinement → Implementation → Completion)
- Update status emoji when starting/completing tasks (🔵 → 🟡 → 🟢)
- Add detailed notes or blockers as comments under each task
- Review and prioritize tasks regularly based on project needs
- **ALWAYS run benchmarks after task completion**
- **ALWAYS update CLAUDE.md and archive completed tasks** to maintain documentation currency
- Keep active task list focused by moving completed details to archive section

## Completed Tasks Archive

The following tasks have been completed and verified:

### ✅ Recently Completed (2026)
- **Test suite audited against the documentation** - two documentation tests were vacuous: `Doc: Rate Limit Group` passed with the rate limit removed entirely, and `Doc: Concurrent Limit Group` asserted only that tasks completed. Both now assert the limit binds - the rate-limit run drops from 417ms to 2ms without a limit, which is what makes the assertion meaningful. Re-enabled `Concurrent Rate Limit Enforcement`, which had been commented out as needing 'complex timing synchronisation' but is stable at 5.01-5.09 against a limit of 5 across repeated runs, leaving plain rate limiting with no coverage at all. Added tests for six documented claims that had none: onCompleteDescendants on shutdown, handleRejection, periodicRetryIntervalMs, replaceWorker, updateWorkerConfig and its constructor-copy caveat, and the task.started and worker.teardown.failed events. Every documented event, method and option is now exercised.
- **Requeue folded into the single settle path** - requeue was still a separate handler that cleared the settle flag, so a failure could requeue a task and count it failed at once; three tasks against an unstartable pool reported five outcomes. Requeue is now a third outcome of `_settleTask`, and the stats account for each task exactly once.
- **Unstartable pools now raise the alarm** - a pool whose workers could not be constructed never emitted `worker.restart_limit_exceeded` and never replaced the dead worker, because the replacement decision required `canRestart === true` while construction failures set no `canRestart`; it also kept the dead worker in its `maxThreads` slot, wedging inline pools permanently. The two wrappers disagreed as well, inline reporting `worker.failed` and threaded not, so the same fault stranded tasks on one and failed them on the other. Both now report construction failures identically and the pool retries then alarms.
- **Single task settlement path** - a failure arriving both as a worker event and as a promise rejection ran two near-duplicate settles, so `task.failed` fired twice, `stats.failed` counted 2 for one task (5 with two retries), the terminal state raced `user_action` to `failed`, `task.user_action` fired while retries remained, and a task settled only through the event path never released its resource group slot. Replaced with one idempotent `_settleTask`; added `tests/suites/settlement.ts` (14 tests, 10 failing beforehand).
- **Examples fixed and verified on both runtimes** - two of four examples were broken on Node (one erroring, one hanging) because they lacked the cross-runtime worker URL branch, and nothing ran them. All examples now run on Deno and Node, a Deno-only example shows the plain consumer form without this repo's `?worker-direct` convention, and `deno task examples` executes every one of them on both runtimes and fails on an error or a hang.
- **Scheduler counters can no longer go negative** - a failure that arrived both as a worker `task.failed` event and as a rejection of the pool's task promise was counted twice, driving `stats.running` negative. That is not cosmetic: `_checkCompletion` requires `running === 0`, so a negative count means the scheduler never sees itself idle, never clears the periodic retry timer, and the process hangs - which is exactly what `examples/worker-types.ts` did on Node, reporting `Running: -6` forever. Decrements are now clamped at zero, and `shutdown()`'s drain wait is bounded with a warning instead of looping forever on a task that can never complete.
- **Worker URL documentation corrected for consumers** - README and AGENTS.md told readers to load workers with `./myWorker.ts?worker-direct`, which is a convention of this repo's esbuild config and does not exist in either published package; following it fails. Replaced with what was actually verified against the built package: npm consumers ship a `.js` worker and pass `new URL('./myWorker.js', import.meta.url).href`, Deno consumers point at the `.ts` source.
- **JSR publishing (Deno-only)** - `jsr.json` was dead config (`deno publish` reads `deno.json`), and `workerWrapperUrl.ts` imported `./workerWrapper.ts?worker`, a query-suffixed specifier Deno resolves into the module graph but refuses to publish. Split the Deno path from the bundler path into `workerWrapperUrl.bundled.ts`, aliased by esbuild for npm builds, which also removed the last runtime `'Deno' in globalThis` branch from the published source. Merged the JSR config into `deno.json` with an allowlist-style `publish.exclude`, annotated 14 slow types, and added `scripts/jsr-smoke.ts`, which stages exactly the published file set and runs a real threaded worker against it - the dry run reports success even when `core/workerWrapper.ts` is missing, because nothing imports it. npm builds are unchanged and still inline the worker as a data URL and a Blob.
- **KeyedRateLimitGroup** - rate limiting applied independently per key, for endpoints or tenants with separate quotas. The key is derived by the group's `keyFrom` (defaulting to `task.limitKey`), so tasks need no boilerplate; a task with no derivable key throws at `addTask` rather than silently sharing a bucket. Required extending the `ResourceGroup` contract with optional key parameters, since the scheduler previously called `canRun`/`onStart`/`onFinish` with no arguments at all, and making blocked queues per (group, key). Two head-of-line problems had to be fixed for keys to isolate properly: the dispatch loop stopped scanning a worker queue on the first blocked task, and the blocked-queue retry dropped tasks that failed its multi-group check instead of requeueing them.
- **Benchmark dependency category removed** - three scenarios were named after DAG workloads the framework cannot express, driven by a `dependencyConfig` field the task generator never read. One was a byte-identical duplicate of an existing scenario and is deleted; the other two are renamed for the configuration they actually measure. Converted `SCENARIO_CATEGORIES` from positional index slices to name lookup, since removing a scenario would otherwise have silently reassigned every later category, and grouped the 8 scenarios that belonged to no category at all.
- **Benchmark warm-up phase and worker startup scenarios** - threading efficiency read 82-91% because workers are created lazily and a 200-task run charged thread startup to coordination overhead; pre-warming one throwaway task per worker instance shows steady state is 96-98%. Scenarios now warm by default (`warmupWorkers: false` opts out). Because that removes startup from the efficiency number, added three `Worker Startup` scenarios plus a `Worker Startup` metric taken from the newly forwarded lifecycle events, reporting per-worker init and setup durations - a direct cross-runtime comparison (threads 12-54ms each, inline instances 85-325us, Deno and Node 4x apart on a single thread). Switched lifecycle durations to `performance.now()` so sub-millisecond inline startup is measurable at all.
- **Efficiency metric denominator corrected** - `Overall Efficiency` divided by `maxConcurrentTasks` alone for inline pools, understating the concurrency ceiling by `maxThreads` and pinning every inline scenario at a clamped 100% with 0 coordination overhead. Inline pools create `maxThreads` worker *instances* in the main process, and measurement confirms `4x5` and `1x20` both peak at 20 in-flight tasks, so the ceiling is the product for both pool types. It also capped by `cpuSlots` unconditionally, though the `cpu` group is only created for threaded pools and only attached when `cpuWorker` is set. The denominator now starts at pool capacity and is capped only by groups the scenario actually applies. Nothing is clamped any more, and the baseline was regenerated. Documented what `maxThreads` means for inline pools - instances not threads, product sets concurrency, split sets state isolation - in JSDoc, README, AGENTS.md and CLAUDE.md, pinned by a doc test.
- **Worker lifecycle events normalised and forwarded** - eight `worker.initialization.*` / `setup.*` / `teardown.*` events were dispatched on internal wrappers and never forwarded, so nobody could observe them, and the two wrapper types emitted different subsets with different payloads. Both now emit the same set with the same shape and `WorkerManager` forwards all of them. Fixed three latent bugs found while testing it: shutdown dropped listeners before terminating workers, a threaded worker throwing in its constructor emitted nothing and hung until a 30s timeout, and an async promise executor turned an initialization failure into a process-killing unhandled rejection. Also fixed `_onTaskFailed` never rejecting the task promise, which left any failure arriving on that path hanging forever, and wired up `testWorkerInitializationFailure`, which had never been registered and never asserted anything.
- **Builds clear their output directory** - `dist/` and `dev-dist/` were never cleaned, so bundles and declarations for deleted modules survived indefinitely; `dist/types/core/dagScheduler.d.ts` outlived its source by months and would have been published, since `package.json` ships `dist/**/*`. Both builds now clear their output directory first.
- **addTasks rejects unknown worker types** - `addTasks` silently skipped tasks whose `workerType` was not registered while still counting them in `stats.queued`, so the count never returned to 0 and `scheduler.completed` never fired again. It now validates the whole batch up front and throws like `addTask`, leaving nothing queued and no orphan task-map entries when it rejects.
- **Opt-in retention limit for completed tasks** - Completed tasks were never evicted from `FyflowScheduler.tasks`, so long-lived schedulers grew without bound (~7MB per 1,000 completed tasks with 8KB payloads, since each entry pins its payload and result). Added the `maxCompletedTasks` scheduler option, defaulting to unset so existing behaviour is unchanged; with a limit set, memory growth flattens instead of climbing. Descendant trackers now hold their root task by reference so eviction cannot change how an `onCompleteDescendants()` wait settles. Also deleted a dead recovery scan in `_checkCompletion()` that walked every task ever created looking for a `'dispatched'` state that stopped being assigned in `dd57083` - it always found nothing while costing O(tasks) per call.
- **Documentation sweep after the framework-agnostic refactor** - Brought CLAUDE.md, README.md, TODO_CHECKLIST.md and the package metadata back in line with the code after StrictLimitGroup, task dependencies and the DagScheduler naming were removed. Corrected the project structure tree, benchmark category counts, resource group docs, `getResourceStats()` shape, task states and `onCompleteDescendants()` semantics; documented the full scheduler event list including `task.spawn_failed`; added the missing test commands; and recorded known issues (type checking coverage, stale benchmark baseline, misnamed benchmark category, performance suite not in the default run, unbounded task retention) as follow-ups rather than leaving them undocumented.
- **Revive `onCompleteDescendants()` for spawned task lineage** - Restored the documented workflow-completion API, which had been commented out and crashed `examples/enhanced-features.ts`. Replaced the removed DAG `children` field with a scheduler-side spawn lineage map. Fixed three bugs in the original implementation: failed descendants hung the wait forever, `addTask()` never set the scheduler back-reference the API needs, and trackers keyed by root task id silently replaced each other. Also fixed a crash where spawning an unregistered worker type threw out of the worker's event dispatch and took the scheduler down - such spawns now emit `task.spawn_failed`. Added `tests/suites/spawning.ts` (10 tests) and wired it into the default test run; spawning previously had no coverage at all.
- **Blocked tasks no longer stranded by premature completion** - `_checkCompletion()` only looked at `stats.queued` and `stats.running`, but tasks blocked on a resource group are subtracted from `stats.queued`. The scheduler therefore declared completion with work outstanding and cleared the periodic retry timer that was the only thing that could unblock it, permanently stranding those tasks - `examples/performance-groups.ts` was silently dropping 15 of its 25 rate-limited API tasks. Blocked tasks now count as outstanding work and the retry is re-armed when nothing is running to wake them. Added a core test covering both halves.
- **Restore worker execution time in benchmark metrics** - `Overall Efficiency` reported 0% for every scenario and `Coordination Overhead` degenerated to total duration, because the scheduler stopped forwarding the worker-measured `executionTime` when duplicate `task.completed` events were fixed. The scheduler now records that timing on the task without re-dispatching the event, and the benchmark runner warns if the timing feed ever goes empty again.
- **Repair the contention scaling suite and gate test failures** - The performance suite never ran a scenario: it awaited `Promise.all` on the fire-and-forget result of `addTasks()`, and the resulting `TypeError` was reported as "timed out". Fixed the promise usage, added a real timeout, corrected an invalid `taskDelay` option, added per-scenario cleanup, and bundled the suite for Node. Test failures now set a non-zero exit code - previously every suite exited 0 regardless of outcome.

### ✅ Recently Completed (2025)
- **Optional promise creation for high-volume task scenarios** - Implemented fire-and-forget mode as default for `addTask()` and `addTasks()` methods with optional promise creation via `{ createPromise: true }`. BREAKING CHANGE: Default behavior changed from automatic promise creation to fire-and-forget mode. Benchmark results show 2-4% throughput improvement and 24% memory reduction across all scales. Updated all tests and examples to use explicit promise creation where needed. Comprehensive before/after comparison documented in `benchmark-comparison-optional-promises.md`. Performance validation shows consistent gains with minimal scheduler overhead trade-off (microseconds).
- **Centralized idle worker management in WorkerManager** - Replaced per-worker timeout implementation (N timers for N workers) with centralized idle management using single timer per WorkerManager. Implemented comprehensive test coverage for all 3 idle timeout scenarios: undefined (default 5000ms), custom timeout values, and persistent workers (idleTimeout: 0). Added activity tracking with `lastActivityTime` and smart timer lifecycle management. Eliminated event loop pollution in high-concurrency scenarios while maintaining identical idle termination behavior. Performance benchmarks show no regression in scheduler overhead.
- **Group performance optimization for high-contention scenarios** - Implemented efficient dispatch algorithms to address O(n²) behavior when thousands of tasks compete for limited group resources. Enhanced group state management with pre-filtering and caching. Added priority-based task selection and fairness mechanisms. Reduced scheduler overhead from 12+ seconds to sub-second performance for 25K tasks with 2 group slots. Comprehensive benchmarking validates performance improvements across all scenarios.
- **Enhanced error handling foundation with worker self-termination and management APIs** - Implemented comprehensive worker failure management with two-level context architecture (worker-level and task-level contexts). Added worker self-termination capabilities, enhanced error events with rich context, worker status inspection APIs, and management APIs for external resilience systems. Includes task requeuing on worker failure, proper resource allocation strategies, and foundation for circuit breaker patterns. All worker failure scenarios properly handled with configurable restart behavior.
- **Professional library packaging with separated builds** - Restructured fyflow-scheduler as production-ready npm/JSR package with separated library and development builds. Library build (`dist/`) contains only core library code with TypeScript declarations. Development build (`dev-dist/`) contains tests, examples, and benchmarks. Updated package.json with proper exports, build scripts, and publishing configuration. Added prepublishOnly script and clean .gitignore structure. All 17 Node.js tests and 24 browser tests pass with new build system.
- **Cross-platform TypeScript source file fixes** - Fixed platform compatibility issues in source TypeScript files. Replaced `NodeJS.Timeout` with `ReturnType<typeof setTimeout>` for cross-platform timeout types. Updated `workerWrapperUrl.ts` to use platform-agnostic Deno detection without direct references. Enhanced TypeScript configuration with proper module and iteration support. Source files now compile cleanly across Node.js, Browser, and Deno environments while maintaining full functionality.
- **Framework runtime agnostic implementation with cross-platform bug fixes** - Implemented full Deno + Node.js ESM support with esbuild-based platform-specific builds, platform-specific workerWrapper files, automatic Worker import injection for Node.js, and data URL embedding. Fixed critical scheduler race conditions, thread worker path resolution, scheduler deadlock issues, and resource constraint enforcement. Comprehensive test suite validates all functionality across both platforms. Cross-platform build system with unified API achieved.
- **Enhanced worker communication protocol with progress reporting and dynamic task spawning** - Implemented proper message types replacing fake taskIds, real-time progress reporting with context.sendProgress(), dynamic task spawning with context.spawnTask(), enhanced event system with task.progress and task.spawn_request events, and comprehensive worker-level feedback. All existing workers remain compatible. New examples: progressWorker.ts and enhanced-features.ts.
- **Remove GlobalCPUManager and replace with CPU group constraints** - Successfully eliminated global CPU manager singleton in favor of ConcurrentLimitGroup-based CPU constraints. Achieved cleaner architecture with group-based resource management at WorkerManager level. Comprehensive benchmarking established new baseline covering both inline and threaded workers. Threading scalability tests show 1.93x efficiency with proper CPU workloads. Implemented coordination overhead measurement distinguishing scheduling costs from actual work. All 17/17 tests pass on both platforms. New baseline: `benchmark-quick-test.md`.
- **Fix thread worker initialization race conditions and CPU slot enforcement** - Resolved infinite retry loops in CPU slot management when multiple tasks start simultaneously. Fixed initialization race condition with `initializing` flag. Implemented smart task distribution with lazy initialization, conservative thread creation, and CPU-aware resource management. Fixed CPU slot enforcement edge case where tasks remained queued when CPU slots unavailable by implementing simple task requeuing without infinite loops. All 16/16 tests now pass on both Deno and Node.js platforms.
- **Fix async concurrent execution inside workers** - Workers can now handle multiple concurrent tasks
- **Clean up and refactor/rename of files** - Reorganized core files with cleaner naming structure
- **Fix stats tracking bug** - Stats now accurately reflect task completion state
- **Update WorkerManager constructor** - Now uses clean options object instead of positional parameters
- **Refactor to class-based unified worker interface** - All workers implement standardized class interface
- **Create comprehensive DAG scheduler benchmarking suite** - 17 scenarios with variance analysis
- **Eliminate web worker boilerplate** - Universal wrapper eliminates repetitive worker code
- **Mandatory benchmarking process** - Established performance regression testing workflow
- **Add multiple runs feature** - Variance analysis with coefficient of variation metrics
- **Restructure project as proper library** - Clean exports via index.ts, examples organized
- **Create worker interface** - WorkerInterface and BaseWorker for standardized worker API
- **Improvements to FyflowScheduler API** - Groups moved to WorkerManager level for cleaner API
- **Consolidate and streamline examples** - Reduced from 14 files to 8 files with focused functionality