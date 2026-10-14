# FyFlow Development TODO Checklist

## Status Legend
- 🔵 **Not Started** - Task has not been initiated
- 🟡 **In Progress** - Task is currently being worked on
- 🟢 **Completed** - Task has been finished and verified

> **Note**: This checklist serves as a central task tracking document for FyFlow development. Each task should be refined and clearly defined before implementation. Claude will reference this file when performing changes to maintain alignment with project goals and priorities.

## Current Development Tasks

No feature work is open. fyflow-app's pass 1 requests are answered: #1
(cancellation), #2 (a worker event channel, in place of a log API) and #4
(attempts already spent) are built, and #3 (`meta` on tasks) was declined in the
reply. Their summaries are in the archive below.

### Unreleased: notes for the next release

Everything on `main` since `v0.5.2`. A minor release (0.6.0) by the 0.3.0
precedent: the source-breaking items below are narrow, and the release commit
should give the reasoning for each.

**New**
- `scheduler.cancel(taskId, options?)`, `CancelOptions`, `CancelledError`,
  `task.cancelling` / `task.cancelled`, the `'cancelled'` state,
  `task.cancellation`, `stats.cancelled`, `context.signal`, and the pool option
  `cancelGraceMs` (`AGENTS.md` §4, §5, §6, §8).
- `emit()` on both worker contexts, `task.message` on the scheduler,
  `worker.message` on the pool, and the pool option `messages` (§4, §6).
- `attemptsSpent`, `task.attemptNumber` and `task.retrying` (§5, §6).

**Source-breaking**
- `emit` is a new required member of `BaseWorkerContext` and
  `TaskWorkerContext`, and `signal` of `TaskWorkerContext`: code that builds a
  context object literal, typically to unit-test a worker, must add them.
- `ResourceUnblockReason` gains `'cancelled'`, which breaks an exhaustive
  `switch` - the same call as `ResourceGroup.type` in 0.3.0. `AGENTS.md` now says
  not to switch exhaustively on it.
- `readyQueuesByWorker` and `blockedQueues` hold a `TaskQueue`, not an array.
  Public fields, never documented.

**Behaviour changes**
- `terminateWithError()` is a request and never throws, on both pool types;
  code after it runs. Called on a task's context on a threaded pool, that task
  now fails instead of being requeued. Called from `setup()`, it fires
  `worker.setup.completed`, not `.failed`.
- A retry waits out its `backoffMs` however busy the scheduler is, so retries
  start later under load than before.
- `scheduler.completed` fires for a run with no successes - all failed, or all
  cancelled.
- `onCompletePromise()` settles for a task that has already settled.
- `shutdown()` keeps listening to its pools during its drain, so a spawn or
  requeue reported then is no longer lost.
- A threaded worker's progress and spawns for a task it is not running are
  dropped.

**Fixed**
- A requeued task could be settled by the run it abandoned.
- A threaded self-terminating task was requeued forever instead of failing, and
  in the browser a termination was not recognised at all.
- One worker failure spent two restarts.
- A retried task blocked when its pool disappeared vanished without settling.
- A listener re-entering the dispatch loop could lose a task.
- A threaded `run()` returning `null` or `undefined` crashed the host process.

**Performance**
- Head-indexed queues: 50K inline tasks 5.9x faster, 100K 10.9x; throughput is
  flat from 10K to 100K. Cancelling is O(1) wherever a task waits.

## Known Issues / Follow-ups

Open issues, most important first. Closed ones are summarised in the archive.

#### 🔵 Counting `task.running` minus `task.completed` overshoots real concurrency
A task frees its slots before `task.completed` is emitted, and the freed slot
can be refilled first. `_settleTask` releases resources before emitting, the
release retries blocked tasks, and that can dispatch the next task, so its
`task.running` arrives first. Inline wrappers also free their slot when `run()`
returns, before the scheduler settles. Measured at 50K tasks on a 160-slot pool:
the event count peaked at 319, the count inside `run()` at 160. Under a group
limit of 7 the overshoot is 1. No limit is broken - releasing before emitting is
the "transitions complete before events" rule - but a consumer gauging
concurrency from events sees false overshoots. Several docs tests count this way
and only pass because they run few tasks. Proposed: document it in `AGENTS.md`
next to the events, pointing at `group.running` and `scheduler.stats.running`
instead, with a docs test; change the docs tests that count from events.

**Status**: 🔵 Not Started

#### 🔵 One release moves a whole blocked queue back to the ready queue
`_retryBlockedTasksForGroup` loops `while (blockedTasks.length > 0 &&
group.canRun(key))`, but nothing is acquired until dispatch, so `canRun()` stays
true and the first release moves every waiter back - 2,997 tasks for one free
slot. The dispatch loop then blocks one of them again and stops. No task is
lost or reordered, but the blocked queue stops describing who is waiting
(`getAdmissionQueue()` and `resource.blocked`/`unblocked` with it), and it may be
why a group-limited run is so much slower: 50K tasks at ~1,900 tasks/s under a
limit-7 group, against ~114,000 pool-only. Not yet investigated; the throughput
link is unconfirmed.

Still there after #1 (2026-10-14): 300 tasks of 30 ms on a 50-slot pool behind a
`ConcurrentLimitGroup(1)` - mid-run, `getAdmissionQueue('g')` listed 1-2
waiters while `stats.queued` was 258. A consumer building an admission view
from `resource.blocked`/`unblocked` - the 0.3.0 resource events, which a
downstream library folds - sees one or two tasks waiting on a group that 258
are waiting for.

**Status**: 🔵 Not Started

#### 🔵 A cancelling threaded task that fails its own worker settles at the request
A task being cancelled that calls `terminateWithError()` on its own `context`,
on a threaded pool, settles `cancelled` when the request arrives - while its
`run()` can keep going until the worker is torn down, up to about 500 ms, with
its resources already released. Everywhere else a cancelling task settles only
once its run has stopped. Any task failing itself this way behaves the same, so
it is not specific to cancellation; it has no test.

**Status**: 🔵 Not Started

#### 🔵 Rewrite `doc/design/pause-resume.md`, or delete it
It was written against an older architecture and is now marked abandoned at
its top, which satisfies `CLAUDE.md`'s rule that a plan is current or marked.
A pause, if wanted, needs a new plan: cancellation (#1) provides the
interrupt half - signal, grace period, forced stop - and deliberately added no
requeue outcome for it. Nothing asks for pause today.

**Status**: 🔵 Not Started

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

#### 🔵 Performance suite is not part of the default test run
`tests/performance/contention-scaling.ts` is reachable via `test:performance`
but not included in `runAllSuites()`, so contention regressions are not caught
by `npm test` / `deno task test`.

**Status**: 🔵 Not Started

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

**IMPORTANT**: After implementing any task that can affect performance, the
following must be completed before considering the task done. A change confined
to failure handling, docs or tests, which no benchmark scenario exercises, can say
so instead.

A benchmark result means something only next to another commit measured the same
way, on the same machine, at the same time. There is deliberately no stored
baseline: `benchmark-baseline-comprehensive.md` recorded absolute numbers from one
machine on one day, and was retired once a single change (`e599930`) made them
wrong by up to 10x. Git history keeps it.

### 1. Compare the change against its base

**Preferred: the `Benchmark` workflow** (`.github/workflows/benchmark.yml`,
manual). It runs HEAD and a base ref back to back in one job on one runner, and
writes the comparison to the job summary:

```bash
gh workflow run benchmark.yml -f base=<commit before the change> -f category=quick -f runs=3
gh run watch            # then read the job summary
```

Both commits must be pushed. GitHub runners are shared VMs, so read the two
columns against each other, never as absolute numbers; and their core count
differs from most dev machines, so threading scenarios behave differently there.

**Fallback: locally, back to back**, when the change is not pushed yet:

```bash
git worktree add --detach ../base <commit before the change>
# run the same command in both trees, alternating: base, head, base, head
deno task benchmark:variance        # = benchmark:quick --runs 3
```

Alternate the runs rather than running one commit then the other: a machine that
warms up or gets busier then biases whichever ran second. Compare medians across
all runs of each commit, and look at each side's spread before reading a
difference in medians.

### 2. Acceptance criteria, against the base column

**✅ ACCEPTABLE**:
- Throughput within ±10% of the base
- Scheduler overhead within ±20% of the base
- Memory growth ≤ 20% unless explicitly optimising memory

**⚠️ REQUIRES INVESTIGATION**:
- Throughput regression > 10% that holds in both run orders and outside the
  spread of both commits
- Scheduler overhead increase > 20%
- Memory increase without a clear justification

**❌ UNACCEPTABLE**:
- Throughput regression > 25%
- Scheduler overhead increase > 50%
- Crashes or hangs under benchmark load
- Memory leaks or excessive memory growth

### 3. Recording the result

Put the comparison in the task's entry in this file and in the commit message:
the scenarios that moved, by median, with the spread where it matters, and any
difference you judged to be noise along with why. A significant improvement
belongs in the release notes too.

### 4. Available benchmark commands

```bash
# Quick benchmarks (7 core scenarios: inline volume + threading)
deno task benchmark:quick

# Quick benchmarks with variance analysis
deno task benchmark:variance

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

### 5. Reading the numbers

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

**`Large Volume - 1K Independent Tasks (Inline)` is bimodal.** Each run lands
near either ~1150 or ~1300 tasks/sec, on any commit, and which mode a commit's
runs land in varies between comparisons. A ±10% move in its median says nothing
on its own; it never queues enough to reach most of the scheduler.

### 5a. Comparing runs - read this before calling something a regression

**Scenario order matters more than most code changes.** Later scenarios in a
suite run under heap pressure left by earlier ones, so the same scenario can
differ by 40-70% between a full-suite run and an isolated one. Measured on the
same commit:
- `Fire-and-Forget - 50K Tasks`: 6,282 tasks/sec in the baseline suite,
  ~10,300-10,800 tasks/sec run on its own
- `Threading Scalability - 200 Tasks, 2 Threads`: ~102-144 tasks/sec inside
  `benchmark:quick`, ~197-219 tasks/sec run on its own

Only compare like with like: the same category, the same order, the same machine,
the same session. Both procedures in step 1 guarantee that; a number recorded on
another day does not.

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
- **A browser performance spec no longer asserts a timing order** - "should measure Web Worker vs inline performance" required the Web Worker run to be the slower one, a race that failed once on WebKit in a full run. The overhead is now reported as a test annotation, and the spec shuts its two schedulers down, which it never did.
- **Cancel a task, queued or running (fyflow-app #1)** - `scheduler.cancel()` reaches all seven places a task can be: waiting tasks are removed at once, running ones see `context.signal`, and a threaded worker that ignores it is retired after `cancelGraceMs` (default 2000) - detached before teardown, co-tenants requeued, settled `forced` only once the thread is dead, not counted as a restart. Cancel wins over any late outcome; `task.cancelling` then `task.cancelled`, with resources released at the second. Built in four stages with 33 tests in a new `cancellation` suite, 4 docs tests and a three-engine browser spec; 49 mutations checked. Cancelling 50,000 tasks takes 30-40 ms. Found and fixed on the way: `scheduler.completed` never fired for a run with no successes; a threaded `run()` returning `null` or `undefined` crashed the host; "is any task blocked?" walked every blocked queue (1.8 s to cancel 50,000 tasks over 5,000 keys, now 29 ms via a live count kept by `TaskQueue`). Benchmark against `main`: 50K +0.5%, 100K -5.1% (inside the band), threaded within ±3%.
- **A worker event channel (fyflow-app #2)** - asked for as `context.log()`; built as `emit(name, data)` on both worker contexts, delivered as `task.message` on the scheduler and `worker.message` on the pool, with logging as one convention on top. Nothing is sent while nobody listens; events are batched (20 ms, cap 100 per buffer), ordered per attempt, delivered before their attempt settles, and every drop is counted. `core/messageChannel.ts` holds the logic once for both worker scripts and `InlineWrapper`. New `messages` suite (19 tests) and a browser spec.
- **Attempts already spent (fyflow-app #4)** - `attemptsSpent` continues a resumed task's retry budget, `task.attemptNumber` numbers attempts 1-based, and `task.retrying` fires when a retry is decided. A requeue is not an attempt. Fixed with it: a retry's backoff was not enforced under load - any other dispatch started it early, 1 ms after failing where 1000 ms was asked for.
- **Ready and blocked queues made linear** - both were arrays consumed with `shift()`, which V8 copies above ~16,000 elements, and blocked pushes were guarded by a linear `includes()` (42.7 s to build a 100,000-task blocked queue). An internal head-indexed `TaskQueue` with opt-in membership tracking: 50K inline tasks 5.9x faster, 100K 10.9x, confirmed on ubuntu-latest.
- **A requeued task is no longer settled by the run it abandoned** - after a worker failure the old run kept going and its outcome settled the new attempt. Attempts now carry a dispatch token checked at settle, and the failed worker forgets the task before the requeue is reported.
- **`shutdown()` hears its pools until the drain is done** - it removed its listeners before waiting for running tasks, so spawns and requeues reported during the drain were lost and `onCompleteDescendants()` resolved short.
- **`terminateWithError()` has one contract** - inline it threw (crashing the host from a timer), threaded it requeued the caller. It is now a request on both pool types, attributed by context: a task's context fails that task and requeues the rest, the worker's context requeues everything.
- **A threaded self-terminating task fails instead of being requeued forever** - the pool handled the termination before failing the task, so it ran again on every replacement. Termination errors are now matched by name too, which the browser build needed.
- **One worker failure is handled once** - it arrived as two events and spent two restarts, so `maxWorkerRestarts: 1` alarmed on the first self-termination.
- **A listener re-entering the dispatch loop no longer loses a task** - the loop peeked the head, emitted `resource.blocked`, then shifted, so a listener calling `addTask` left a task in no queue.
- **An orphaned task that was retried fails instead of vanishing** - `_failOrphanedTask` returned early on a flag every retried task carries.
- **Browser tests run serially locally**, as in CI - Playwright's default of half the cores made Firefox fail 6-9 tests per run on a 20-core machine.
- **Per-test timeout and `--only` / `--bail` / `--timeout` in the test runner** - a hung test stalled a mutation check for over an hour; every test now fails after 60 s, and a mutation check runs only the tests meant to catch it.
- **Build script stub restored on failure** - a failing `tsc` left a stubbed `core/workerWrapperUrl.ts` in the working tree; a dead hand-written `ThreadWrapper` declaration was removed.
- **A blocked task whose pool is missing fails instead of vanishing** (0.5.2) - it was shifted out of its queue and dropped. It now fails terminally, with `resource.unblocked` reason `'orphaned'`.
- **Intermittent error-handling test fixed** (0.5.2) - a fixed 200 ms sleep raced two Node thread startups under load; it now polls for the event.
- **Strict resource limits, closed** - `StrictLimitGroup` was implemented and deliberately reverted; both group types remain optimistic. The design is in this file's git history; the unified `ResourceGroup` interface survived.
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