// Admission and wake-up tests - R0 of doc/design/admission-rework.md.
//
// Three kinds of test, and the difference matters when one goes red:
//
// - **Wake paths, with the heartbeat off.** The 50 ms periodic retry wakes
//   every waiter eventually, so a missing wake-up costs latency rather than
//   correctness and passes every other suite. Each test here sets
//   `periodicRetryIntervalMs` out of reach, so a missed wake is a waiter that
//   never starts. Sources documented as heartbeat-only (`AGENTS.md` §6, "How a
//   blocked task wakes up") get a second test with the heartbeat on, pinning
//   what is promised today.
// - **Targets, as known gaps.** Behaviour the rework is meant to deliver,
//   asserted now and expected to fail with a stated error. The harness fails a
//   gap that starts passing, so the change that closes one has to remove its
//   `knownGap` in the same commit.
// - **Guards.** Properties that hold today and must still hold after the
//   rework: limits, the fold invariants, settlement under re-entrant
//   listeners, flat admission cost for a global group.
//
// Assertions are on framework observables only - events, stats, group state -
// never on state shared with a worker module, which does not survive the Node
// bundle split.

import {
  WorkerManager, FyflowScheduler, FyflowTask, ConcurrentLimitGroup, RateLimitGroup, KeyedRateLimitGroup
} from '../../index.ts';
import { runTest, type RunTestOptions, type TestResult } from '../harness.ts';
import { KeyedCounterGroup, instrument } from '../fixtures/keyedCounterGroup.ts';

// Node.js process declaration for cross-platform compatibility
declare const process: any;

interface TestSuiteResult {
  platform: string; totalTests: number; passed: number; failed: number;
  duration: number; results: TestResult[];
}

/** Far enough out that the heartbeat never fires during a test. */
const HEARTBEAT_OFF = 1_000_000_000;

/** A woken waiter must start within this of the capacity freeing. */
const WAKE_BOUND_MS = 25;

/** How long to wait before calling a waiter "never started". */
const NEVER_MS = 400;

const NEVER_STARTED = 'never started';

const R3 = 'doc/design/admission-rework.md R3';

class AdmissionTestSuite {
  private results: TestResult[] = [];
  private workerUrl = "";
  private schedulers: FyflowScheduler[] = [];

  private track(scheduler: FyflowScheduler): FyflowScheduler {
    this.schedulers.push(scheduler);
    return scheduler;
  }

  async cleanup(): Promise<void> {
    for (const scheduler of this.schedulers) {
      try { await scheduler.shutdown(); } catch { /* already down */ }
    }
    this.schedulers = [];
  }

  private async run(name: string, fn: () => Promise<void>, options?: RunTestOptions): Promise<void> {
    this.results.push(await runTest(name, async () => {
      try { await fn(); } finally { await this.cleanup(); }
    }, options));
  }

  private pool(extra: Record<string, any> = {}) {
    return new WorkerManager(this.workerUrl, {
      maxThreads: 4, maxConcurrentTasks: 20, inline: true, ...extra
    });
  }

  private task(id: string, config: Record<string, any> = {}, delay = 10) {
    const { workerType = 'W', ...rest } = config;
    return new FyflowTask({ id, workerType, payload: { taskId: id, delay }, ...rest });
  }

  /** `performance.now()` at the first `type` event matching `match`. */
  private when(target: EventTarget, type: string, match: (detail: any) => boolean): Promise<number> {
    return new Promise(resolve => {
      const listener = (e: Event) => {
        if (!match((e as CustomEvent).detail)) return;
        target.removeEventListener(type, listener);
        resolve(performance.now());
      };
      target.addEventListener(type, listener);
    });
  }

  private started(scheduler: FyflowScheduler, taskId: string): Promise<number> {
    return this.when(scheduler, 'task.running', d => d?.id === taskId);
  }

  /** Resolves with the start time, or rejects with NEVER_STARTED. */
  private async startsWithin(start: Promise<number>, taskId: string, ms = NEVER_MS): Promise<number> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        start,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error(`"${taskId}" ${NEVER_STARTED} within ${ms}ms`)), ms);
        })
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  private assertPrompt(label: string, freedAt: number, startedAt: number, bound = WAKE_BOUND_MS) {
    const latency = startedAt - freedAt;
    if (latency > bound) {
      throw new Error(`${label}: started ${latency.toFixed(1)}ms after capacity freed (bound ${bound}ms)`);
    }
  }

  async runAllTests(_exitOnComplete = true): Promise<TestSuiteResult> {
    if (typeof Deno !== "undefined") {
      this.workerUrl = new URL("../workers/testInlineWorker.ts", import.meta.url).href;
    } else {
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      this.workerUrl = new URL((await import('../workers/testInlineWorker.ts?worker-direct')).default).href;
    }

    const platform = typeof globalThis !== 'undefined' && 'Deno' in globalThis ? 'Deno' : 'Node.js';
    console.log(`🚀 FyFlow Admission Tests - ${platform}`);
    console.log('='.repeat(60));
    const startTime = performance.now();
    this.results = [];

    // Wake paths, heartbeat off
    await this.run('Wake: A Task Release Admits The Next Waiter', () => this.testWakeOnRelease(false));
    await this.run('Wake: A Task Release Admits The Next Waiter (threaded)', () => this.testWakeOnRelease(true));
    await this.run('Wake: A Keyed Release Admits That Key\'s Waiter', () => this.testWakeOnKeyedRelease());
    await this.run('Wake: Cancelling A Holder Admits The Next Waiter', () => this.testWakeOnCancel());
    await this.run('Wake: The Second Group\'s Release Admits A Two-Group Waiter', () => this.testWakeTwoGroups());
    await this.run('Wake: A Resident Release Admits A Waiting Pool', () => this.testWakeOnResidentRelease(HEARTBEAT_OFF));
    await this.run('Wake: A Worker Becoming Ready Admits Tasks That Arrived During Its Start-Up (threaded)', () => this.testWakeOnWorkerReady(true));
    await this.run('Wake: A Worker Becoming Ready Admits Tasks That Arrived During Its Start-Up (inline)', () => this.testWakeOnWorkerReady(false));
    await this.run('A Burst Onto An Empty Threaded Pool Dispatches Each Task Once', () => this.testBurstNoBounce());
    await this.run('Wake: A Rate Window Rolling Over Admits Without Polling', () => this.testWakeOnRateWindow(HEARTBEAT_OFF), {
      knownGap: {
        reason: `rate windows are heartbeat-only (AGENTS.md §6) - ${R3} wakes at resetAt`,
        failsWith: NEVER_STARTED
      }
    });

    // What the heartbeat promises today
    await this.run('Heartbeat: A Resident Release Admits A Waiting Pool', () => this.testWakeOnResidentRelease(50));
    await this.run('Heartbeat: A Rate Window Rolling Over Admits', () => this.testWakeOnRateWindow(50));
    await this.run('Heartbeat: Raising A Limit Admits Waiters', () => this.testRaisedLimit());
    await this.run('Heartbeat: A Keyed Rate Window Rolling Over Admits Every Key', () => this.testKeyedRateHeartbeat());

    // R1: blocked queues exist only while they hold a waiter
    await this.run('Empty Blocked Queues Are Deleted', () => this.testQueuesDeleted());
    await this.run('Cancelling The Last Waiter Deletes Its Queue', () => this.testCancelDeletesQueue());

    // Targets and guards on what admission costs
    await this.run('Admission Queue Lists Every Waiter', () => this.testAdmissionQueueComplete());
    await this.run('Global Group Admission Cost Is Flat', () => this.testCostFlat('global'));
    await this.run('Keyed Admission Cost Is Flat In Queue Depth', () => this.testCostFlat('keyed'));
    await this.run('Two-Group Admission Cost Is Flat', () => this.testCostFlat('two-group'));
    await this.run('A Full Pool Takes Only What A Release Freed', () => this.testFullPoolTakesOne());
    await this.run('Wake: A Task-Held Waiter Short Of A Worker Moves To The Resident Queue', () => this.testShortOfWorkerMoves());
    await this.run('A Resident Head Whose Own Group Is Full Keeps Its Place', () => this.testResidentHeadKeepsPlace());
    await this.run('Cancel During A Migration Leaves No Waiter Behind', () => this.testCancelDuringMigration());

    // Guards
    await this.run('Limits And Fold Invariants Hold Under Mixed Contention', () => this.testMixedContention());
    await this.run('Re-entrant Cancel And Add From resource.acquired Settle Everything', () => this.testReentrantListeners());
    await this.run('A Task Whose Slot Is Taken From resource.acquired Is Requeued, Not Failed', () => this.testSlotTakenRequeues(false));
    await this.run('A Task Whose Slot Is Taken From resource.acquired Is Requeued, Not Failed (threaded)', () => this.testSlotTakenRequeues(true));
    await this.run('Adding From A Resident Acquire Creates No Worker Past maxThreads', () => this.testResidentReentryMaxThreads(false));
    await this.run('Adding From A Resident Acquire Creates No Worker Past maxThreads (threaded)', () => this.testResidentReentryMaxThreads(true));

    const totalDuration = performance.now() - startTime;
    this.results = this.results.filter(r => !r.skipped);
    const passed = this.results.filter(r => r.passed).length;
    const failed = this.results.length - passed;
    const gaps = this.results.filter(r => r.knownGap);

    console.log(`\n📊 Admission Test Summary - ${platform}`);
    console.log('='.repeat(60));
    console.log(`Total Tests: ${this.results.length}`);
    console.log(`✅ Passed: ${passed}${gaps.length ? ` (${gaps.length} as known gaps)` : ''}`);
    console.log(`❌ Failed: ${failed}`);
    console.log(`⏱️  Duration: ${totalDuration.toFixed(1)}ms`);
    if (gaps.length > 0) {
      console.log('\n🟠 Known gaps:');
      gaps.forEach(r => console.log(`  • ${r.name}: ${r.knownGap}`));
    }
    if (failed > 0) {
      console.log('\n❌ Failed Tests:');
      this.results.filter(r => !r.passed).forEach(r => console.log(`  • ${r.name}: ${r.error}`));
    }

    return {
      platform, totalTests: this.results.length, passed, failed,
      duration: totalDuration, results: this.results
    };
  }

  // --- Wake paths -----------------------------------------------------------

  /** AGENTS.md §6: "Releasing a task-held group retries that group's queue in the same turn". */
  private async testWakeOnRelease(threaded: boolean) {
    const g = new ConcurrentLimitGroup(1, 'g');
    const s = this.track(new FyflowScheduler(
      { W: this.pool({ maxThreads: 2, maxConcurrentTasks: 1, inline: !threaded, groups: ['g'] }) },
      { g },
      { periodicRetryIntervalMs: HEARTBEAT_OFF }
    ));
    const freed = this.when(s, 'task.completed', d => d?.id === 'holder');
    const start = this.started(s, 'waiter');
    const done = Promise.all([
      s.addTask(this.task('holder', {}, 40), { createPromise: true }),
      s.addTask(this.task('waiter', {}, 5), { createPromise: true })
    ]);
    // Threaded pools start slowly; allow for it in the never-started bound
    const startedAt = await this.startsWithin(start, 'waiter', threaded ? 5_000 : NEVER_MS);
    this.assertPrompt('waiter', await freed, startedAt);
    await done;
  }

  /**
   * A task that arrives while the pool's only worker is starting finds no free
   * slot and no room for another worker, so it waits in the ready queue. The
   * worker becoming ready is what frees its slots: nothing settles, and with
   * the heartbeat off nothing else would run the dispatch loop until the first
   * task finished.
   */
  private async testWakeOnWorkerReady(threaded: boolean) {
    const pool = this.pool({ maxThreads: 1, maxConcurrentTasks: 8, inline: !threaded });
    const s = this.track(new FyflowScheduler({ W: pool }, {}, { periodicRetryIntervalMs: HEARTBEAT_OFF }));
    const initStarted = this.when(pool, 'worker.initialization.started', () => true);
    const firstStart = this.started(s, 'first');
    const firstDone = this.when(s, 'task.completed', d => d?.id === 'first');
    const first = s.addTask(this.task('first', {}, 200), { createPromise: true });
    await initStarted;
    const secondStart = this.started(s, 'second');
    const second = s.addTask(this.task('second', {}, 5), { createPromise: true });
    const startedAt = await this.startsWithin(secondStart, 'second', threaded ? 5_000 : NEVER_MS);
    // A missed wake starts it when `first` settles, 200ms later
    const done = await Promise.race([firstDone, Promise.resolve(Infinity)]);
    if (done !== Infinity) {
      throw new Error(`"second" waited for "first" to finish (started ${(startedAt - done).toFixed(1)}ms after it)`);
    }
    this.assertPrompt('second', await firstStart, startedAt);
    await Promise.all([first, second]);
  }

  /**
   * A burst onto a threaded pool with no workers yet: every dispatch creates a
   * worker. A task that loses its new worker's slot to the next task in the
   * same loop is put back and its groups released, then acquired again on the
   * retry - so a bounce shows as more acquires than tasks.
   */
  private async testBurstNoBounce() {
    const g = new ConcurrentLimitGroup(100, 'g');
    const pool = this.pool({ maxThreads: 4, maxConcurrentTasks: 1, inline: false, groups: ['g'] });
    const s = this.track(new FyflowScheduler({ W: pool }, { g }, { periodicRetryIntervalMs: HEARTBEAT_OFF }));
    let acquired = 0;
    s.addEventListener('resource.acquired', () => acquired++);
    const n = 4;
    await Promise.all(Array.from({ length: n }, (_, i) =>
      s.addTask(this.task(`t${i}`, {}, 50), { createPromise: true })));
    if (acquired !== n) {
      throw new Error(`${acquired - n} of ${n} tasks bounced off a new worker (${acquired} acquires for ${n} tasks)`);
    }
  }

  private async testWakeOnKeyedRelease() {
    const k = new KeyedCounterGroup(1, 'k');
    const s = this.track(new FyflowScheduler(
      { W: this.pool({ groups: ['k'] }) }, { k }, { periodicRetryIntervalMs: HEARTBEAT_OFF }
    ));
    const freed = this.when(s, 'task.completed', d => d?.id === 'holder');
    const start = this.started(s, 'waiter');
    const done = Promise.all([
      s.addTask(this.task('holder', { limitKey: 'a' }, 40), { createPromise: true }),
      s.addTask(this.task('other', { limitKey: 'b' }, 5), { createPromise: true }),
      s.addTask(this.task('waiter', { limitKey: 'a' }, 5), { createPromise: true })
    ]);
    this.assertPrompt('waiter', await freed, await this.startsWithin(start, 'waiter'));
    await done;
  }

  private async testWakeOnCancel() {
    const g = new ConcurrentLimitGroup(1, 'g');
    const s = this.track(new FyflowScheduler(
      { W: this.pool({ groups: ['g'] }) }, { g }, { periodicRetryIntervalMs: HEARTBEAT_OFF }
    ));
    const holder = new FyflowTask({ id: 'holder', workerType: 'W', payload: { taskId: 'holder', delay: 60 } });
    const held = s.addTask(holder, { createPromise: true }) as Promise<any>;
    held.catch(() => { /* cancelled */ });
    const start = this.started(s, 'waiter');
    const done = s.addTask(this.task('waiter', {}, 5), { createPromise: true });
    await new Promise(r => setTimeout(r, 20));
    const freed = this.when(s, 'task.cancelled', d => d?.id === 'holder');
    await s.cancel('holder', { graceMs: 0 });
    this.assertPrompt('waiter', await freed, await this.startsWithin(start, 'waiter'));
    await done;
  }

  private async testWakeTwoGroups() {
    // `both` is refused by A first, so it waits in A's queue. A frees at ~40ms
    // while B is still full; B frees at ~80ms.
    const A = new ConcurrentLimitGroup(1, 'A');
    const B = new ConcurrentLimitGroup(1, 'B');
    const s = this.track(new FyflowScheduler(
      { W: this.pool() }, { A, B }, { periodicRetryIntervalMs: HEARTBEAT_OFF }
    ));
    const freed = this.when(s, 'task.completed', d => d?.id === 'holdB');
    const start = this.started(s, 'both');
    const moves: string[] = [];
    for (const type of ['resource.blocked', 'resource.unblocked']) {
      s.addEventListener(type, (e: Event) => {
        const d = (e as CustomEvent).detail;
        if (d?.holderId === 'both') moves.push(`${type.slice(9)}:${d.groupId}${d.reason ? ':' + d.reason : ''}`);
      });
    }
    const done = Promise.all([
      s.addTask(this.task('holdA', { workerGroups: ['A'] }, 40), { createPromise: true }),
      s.addTask(this.task('holdB', { workerGroups: ['B'] }, 80), { createPromise: true }),
      s.addTask(this.task('both', { workerGroups: ['A', 'B'] }, 5), { createPromise: true })
    ]);
    const startedAt = await this.startsWithin(start, 'both');
    this.assertPrompt('both', await freed, startedAt);
    await done;
    // A's release cannot admit it, so it must move to B's queue as one move -
    // not be announced admitted and then blocked again. The entry is announced
    // before the exit (see _migrate).
    const expected = ['blocked:A', 'blocked:B', 'unblocked:A:migrated', 'unblocked:B:admitted'];
    if (moves.join(' ') !== expected.join(' ')) {
      throw new Error(`Moves were [${moves.join(', ')}], expected [${expected.join(', ')}]`);
    }
  }

  /**
   * Cancelled by the listener of its own `migrated` exit. It has already been
   * pushed onto the target queue, so the cancel must find it there - through
   * `_blockedQueueId`, which must be set before the event - and no `blocked`
   * may follow for the queue it has left.
   */
  private async testCancelDuringMigration() {
    const A = new ConcurrentLimitGroup(1, 'A');
    const B = new ConcurrentLimitGroup(1, 'B');
    const s = this.track(new FyflowScheduler(
      { W: this.pool() }, { A, B }, { periodicRetryIntervalMs: HEARTBEAT_OFF }
    ));
    const net = new Map<string, number>();
    for (const type of ['resource.blocked', 'resource.unblocked']) {
      s.addEventListener(type, (e: Event) => {
        const d = (e as CustomEvent).detail;
        const k = `${d.groupId}/${d.holderId}`;
        net.set(k, (net.get(k) ?? 0) + (type === 'resource.blocked' ? 1 : -1));
        if (d.reason === 'migrated' && d.holderId === 'both') s.cancel('both');
      });
    }
    const holders = [
      s.addTask(this.task('holdA', { workerGroups: ['A'] }, 30), { createPromise: true }),
      s.addTask(this.task('holdB', { workerGroups: ['B'] }, 80), { createPromise: true })
    ];
    const both = s.addTask(this.task('both', { workerGroups: ['A', 'B'] }, 5), { createPromise: true }) as Promise<any>;
    const settled = both.then(() => 'resolved', () => 'rejected');
    await Promise.all(holders);
    if (await settled !== 'rejected') throw new Error('The cancelled task did not reject');
    if (s.tasks.get('both')?.state !== 'cancelled') throw new Error(`both is ${s.tasks.get('both')?.state}`);
    const left = Object.values(s.getAdmissionQueue()).flat().map(w => w.holderId);
    if (left.length > 0) throw new Error(`Still queued after the cancel: ${left}`);
    if (s.blockedQueues.size !== 0) throw new Error(`${s.blockedQueues.size} blocked queue(s) left`);
    const unbalanced = [...net].filter(([, n]) => n !== 0);
    if (unbalanced.length > 0) throw new Error(`Membership unbalanced: ${unbalanced.map(([k, n]) => `${k}=${n}`)}`);
    if (!net.has('B/both')) throw new Error("both never reached B's queue - the migration did not happen");
  }

  /**
   * A waiter in a task-held queue whose pool would need a worker that does not
   * fit. It moves to the resident group's queue, so the task-held release
   * still admits the waiter behind it, and the worker's release admits it.
   * Stopping the task-held queue at it instead left both for the heartbeat.
   */
  private async testShortOfWorkerMoves() {
    const gpu = new ConcurrentLimitGroup(1, 'gpu');
    const G = new ConcurrentLimitGroup(1, 'G');
    const resident = () => this.pool({
      maxThreads: 1, maxConcurrentTasks: 1, residentGroups: { gpu: 1 }, idleTimeout: 60, idleCheckIntervalMs: 10
    });
    const s = this.track(new FyflowScheduler(
      { P: resident(), Q: resident(), R: this.pool() }, { gpu, G }, { periodicRetryIntervalMs: HEARTBEAT_OFF }
    ));
    // P's worker holds the gpu until it idles out; h holds G for 30ms
    await s.addTask(this.task('p', { workerType: 'P' }, 1), { createPromise: true });
    const freedG = this.when(s, 'task.completed', d => d?.id === 'h');
    const freedGpu = this.when(s, 'resource.released', d => d?.lifetime === 'resident' && d?.workerType === 'P');
    const startQ = this.started(s, 'q');
    const startR = this.started(s, 'r');
    const done = Promise.all([
      s.addTask(this.task('h', { workerType: 'R', workerGroups: ['G'] }, 30), { createPromise: true }),
      s.addTask(this.task('q', { workerType: 'Q', workerGroups: ['G'] }, 1), { createPromise: true }),
      s.addTask(this.task('r', { workerType: 'R', workerGroups: ['G'] }, 1), { createPromise: true })
    ]);
    // r needs no worker: G's release must reach it past q
    this.assertPrompt('r', await freedG, await this.startsWithin(startR, 'r'));
    const queued = s.getAdmissionQueue('gpu').gpu?.map(w => `${w.holderId}:${w.lifetime}`) ?? [];
    if (queued.join() !== 'q:resident') throw new Error(`gpu queue is [${queued}], expected [q:resident]`);
    this.assertPrompt('q', await freedGpu, await this.startsWithin(startQ, 'q', NEVER_MS + 100));
    await done;
  }

  /**
   * A resident head whose task-held group is full when its units free up. It
   * keeps its place rather than moving to that group's queue, or the cheap pool
   * behind it takes the units, and it no longer fits once its group frees - the
   * starvation head-of-line exists to prevent.
   */
  private async testResidentHeadKeepsPlace() {
    const gpu = new ConcurrentLimitGroup(2, 'gpu');
    const G = new ConcurrentLimitGroup(1, 'G');
    const s = this.track(new FyflowScheduler({
      Hold: this.pool({ maxThreads: 1, maxConcurrentTasks: 1, residentGroups: { gpu: 1 }, idleTimeout: 30, idleCheckIntervalMs: 10 }),
      Big: this.pool({ maxThreads: 1, maxConcurrentTasks: 1, residentGroups: { gpu: 2 }, idleTimeout: 30, idleCheckIntervalMs: 10 }),
      Cheap: this.pool({ maxThreads: 1, maxConcurrentTasks: 1, residentGroups: { gpu: 1 }, idleTimeout: 30, idleCheckIntervalMs: 10 }),
      R: this.pool()
    }, { gpu, G }, { periodicRetryIntervalMs: 50 }));

    await s.addTask(this.task('hold', { workerType: 'Hold' }, 1), { createPromise: true });
    const order: string[] = [];
    s.addEventListener('task.running', (e: any) => order.push(e.detail.id));
    // big first, while G is free, so it waits on gpu (2 needed, 1 free). Then h
    // takes G, past the Hold worker idling out. Then cheap, which fits on gpu
    // and queues behind big.
    const done = Promise.all([
      s.addTask(this.task('big', { workerType: 'Big', workerGroups: ['G'] }, 1), { createPromise: true }),
      s.addTask(this.task('h', { workerType: 'R', workerGroups: ['G'] }, 150), { createPromise: true }),
      s.addTask(this.task('cheap', { workerType: 'Cheap' }, 1), { createPromise: true })
    ]);
    const before = s.getAdmissionQueue('gpu').gpu?.map(w => w.holderId) ?? [];
    if (before.join() !== 'big,cheap') throw new Error(`Precondition: gpu queue is [${before}], expected [big,cheap]`);
    // After Hold's release, with G still held: big fits on gpu, but not on G
    await this.when(s, 'resource.released', d => d?.lifetime === 'resident' && d?.workerType === 'Hold');
    await new Promise(r => setTimeout(r, 60));
    const queued = s.getAdmissionQueue('gpu').gpu?.map(w => w.holderId) ?? [];
    if (queued.join() !== 'big,cheap') {
      throw new Error(`gpu queue is [${queued}] with G still held, expected [big,cheap] - big gave up its place`);
    }
    await done;
    const ran = order.filter(id => id === 'big' || id === 'cheap');
    if (ran.join() !== 'big,cheap') throw new Error(`Ran in order [${ran}], expected [big,cheap]`);
  }

  /**
   * A release on one pool waking waiters of another pool that is full. The
   * group has room for one; the pool for none. Exactly one waiter may leave the
   * queue for that pool's ready queue - not all of them, which would empty the
   * admission queue into a ready queue that re-blocks them one by one.
   */
  private async testFullPoolTakesOne() {
    const g = new ConcurrentLimitGroup(1, 'g');
    const s = this.track(new FyflowScheduler(
      { P1: this.pool({ maxThreads: 1, maxConcurrentTasks: 1 }), P2: this.pool({ maxThreads: 1, maxConcurrentTasks: 1 }) },
      { g }, { periodicRetryIntervalMs: HEARTBEAT_OFF }
    ));
    const held = s.addTask(this.task('holder', { workerType: 'P1', workerGroups: ['g'] }, 40), { createPromise: true });
    const waiters = Array.from({ length: 5 }, (_, i) =>
      s.addTask(this.task(`w${i}`, { workerType: 'P2', workerGroups: ['g'] }, 1), { createPromise: true })
    );
    // P2 had room, so the waiters blocked on g. Now fill P2 without g.
    const filler = s.addTask(this.task('filler', { workerType: 'P2' }, 150), { createPromise: true });
    await new Promise(r => setTimeout(r, 10));
    const before = s.getAdmissionQueue('g').g?.length ?? 0;
    if (before !== 5) throw new Error(`Precondition: ${before} waiters on g, expected 5`);
    await held;
    await new Promise(r => setTimeout(r, 0));
    const after = s.getAdmissionQueue('g').g?.map(w => w.holderId) ?? [];
    if (after.join(',') !== 'w1,w2,w3,w4') {
      throw new Error(`After one release the queue is [${after}] - expected [w1,w2,w3,w4]: one slot was freed, one waiter may go`);
    }
    await Promise.all([filler, ...waiters]);
  }

  /** Two pools sharing a limit-1 resident group: Q's worker can exist only once P's is torn down. */
  private async testWakeOnResidentRelease(heartbeatMs: number) {
    const gpu = new ConcurrentLimitGroup(1, 'gpu');
    const residentPool = () => this.pool({
      maxThreads: 1, maxConcurrentTasks: 1, residentGroups: { gpu: 1 },
      idleTimeout: 30, idleCheckIntervalMs: 10
    });
    const s = this.track(new FyflowScheduler(
      { P: residentPool(), Q: residentPool() }, { gpu }, { periodicRetryIntervalMs: heartbeatMs }
    ));
    const freed = this.when(s, 'resource.released', d => d?.lifetime === 'resident' && d?.workerType === 'P');
    const start = this.started(s, 'q');
    const done = Promise.all([
      s.addTask(this.task('p', { workerType: 'P' }, 10), { createPromise: true }),
      s.addTask(this.task('q', { workerType: 'Q' }, 5), { createPromise: true })
    ]);
    // Teardown waits out P's idle timeout, so allow for that before "never"
    const startedAt = await this.startsWithin(start, 'q', NEVER_MS + 100);
    // On the heartbeat a waiter may wait a full interval, plus a dispatch
    this.assertPrompt('q', await freed, startedAt, heartbeatMs === HEARTBEAT_OFF ? WAKE_BOUND_MS : heartbeatMs + WAKE_BOUND_MS);
    await done;
  }

  private async testWakeOnRateWindow(heartbeatMs: number) {
    const WINDOW = 40;
    const rate = new RateLimitGroup([{ limit: 1, windowMs: WINDOW }], 'rate');
    const s = this.track(new FyflowScheduler(
      { W: this.pool({ groups: ['rate'] }) }, { rate }, { periodicRetryIntervalMs: heartbeatMs }
    ));
    const firstStart = this.started(s, 'first');
    const start = this.started(s, 'second');
    const done = Promise.all([
      s.addTask(this.task('first', {}, 5), { createPromise: true }),
      s.addTask(this.task('second', {}, 5), { createPromise: true })
    ]);
    const startedAt = await this.startsWithin(start, 'second');
    // The window can free no earlier than WINDOW after the first started
    const freedBy = (await firstStart) + WINDOW + 5;
    this.assertPrompt('second', freedBy, startedAt, heartbeatMs === HEARTBEAT_OFF ? WAKE_BOUND_MS : heartbeatMs + WAKE_BOUND_MS);
    await done;
  }

  /** An out-of-band capacity change has no release to wake on. The heartbeat covers it. */
  private async testRaisedLimit() {
    const g = new ConcurrentLimitGroup(1, 'g');
    const s = this.track(new FyflowScheduler({ W: this.pool({ groups: ['g'] }) }, { g }));
    const start = this.started(s, 'waiter');
    const done = Promise.all([
      s.addTask(this.task('holder', {}, 400), { createPromise: true }),
      s.addTask(this.task('waiter', {}, 5), { createPromise: true })
    ]);
    await new Promise(r => setTimeout(r, 20));
    const raisedAt = performance.now();
    g.limit = 2;
    this.assertPrompt('waiter', raisedAt, await this.startsWithin(start, 'waiter'), 50 + WAKE_BOUND_MS);
    await done;
  }

  /**
   * Keyed waiters are found through the per-group key index, not a scan of
   * every queue. A heartbeat must still reach each key's queue.
   */
  private async testKeyedRateHeartbeat() {
    const WINDOW = 40;
    const api = new KeyedRateLimitGroup([{ limit: 1, windowMs: WINDOW }], { id: 'api' });
    const s = this.track(new FyflowScheduler({ W: this.pool({ groups: ['api'] }) }, { api }, { periodicRetryIntervalMs: 50 }));
    const keys = ['a', 'b', 'c'];
    const starts = keys.map(k => this.started(s, `${k}2`));
    const t0 = performance.now();
    const done = Promise.all(keys.flatMap(k => [
      s.addTask(this.task(`${k}1`, { limitKey: k }, 2), { createPromise: true }),
      s.addTask(this.task(`${k}2`, { limitKey: k }, 2), { createPromise: true })
    ]));
    for (const [i, k] of keys.entries()) {
      const at = await this.startsWithin(starts[i], `${k}2`);
      this.assertPrompt(`${k}2`, t0 + WINDOW, at, 50 + WAKE_BOUND_MS);
    }
    await done;
  }

  /** After every shape of wait has drained, no blocked queue is left. */
  private async testQueuesDeleted() {
    const groups = {
      g: new ConcurrentLimitGroup(1, 'g'),
      k: new KeyedCounterGroup(1, 'k'),
      gpu: new ConcurrentLimitGroup(1, 'gpu')
    };
    const s = this.track(new FyflowScheduler({
      W: this.pool(),
      P: this.pool({ maxThreads: 1, maxConcurrentTasks: 1, residentGroups: { gpu: 1 }, idleTimeout: 10, idleCheckIntervalMs: 5 }),
      Q: this.pool({ maxThreads: 1, maxConcurrentTasks: 1, residentGroups: { gpu: 1 }, idleTimeout: 10, idleCheckIntervalMs: 5 }),
    }, groups));
    const promises: Promise<any>[] = [];
    for (let i = 0; i < 20; i++) promises.push(s.addTask(this.task(`g${i}`, { workerGroups: ['g'] }, 1), { createPromise: true }) as Promise<any>);
    for (let i = 0; i < 200; i++) promises.push(s.addTask(this.task(`k${i}`, { workerGroups: ['k'], limitKey: `key${Math.floor(i / 4)}` }, 1), { createPromise: true }) as Promise<any>);
    for (let i = 0; i < 4; i++) promises.push(s.addTask(this.task(`r${i}`, { workerType: i % 2 ? 'Q' : 'P' }, 1), { createPromise: true }) as Promise<any>);

    // The test has to have built queues, or it proves nothing
    let peak = 0;
    const sampler = setInterval(() => { peak = Math.max(peak, s.blockedQueues.size); }, 1);
    await Promise.all(promises);
    clearInterval(sampler);
    if (peak < 10) throw new Error(`Only ${peak} blocked queues ever existed - the test proves nothing`);
    if (s.blockedQueues.size !== 0) {
      throw new Error(`${s.blockedQueues.size} blocked queues left behind: ${[...s.blockedQueues.keys()].slice(0, 5).map(k => JSON.stringify(k))}`);
    }
    // The key index must not outlive the queues it indexes
    const indexed = (s as any).blockedKeys?.size ?? 0;
    if (indexed !== 0) throw new Error(`The blocked-key index still holds ${indexed} group(s)`);
  }

  private async testCancelDeletesQueue() {
    const k = new KeyedCounterGroup(1, 'k');
    const s = this.track(new FyflowScheduler({ W: this.pool({ groups: ['k'] }) }, { k }));
    const held = s.addTask(this.task('holder', { limitKey: 'a' }, 200), { createPromise: true });
    const waiting = s.addTask(this.task('waiter', { limitKey: 'a' }, 1), { createPromise: true }) as Promise<any>;
    waiting.catch(() => { /* cancelled */ });
    await new Promise(r => setTimeout(r, 10));
    if (s.blockedQueues.size !== 1) throw new Error(`Precondition: ${s.blockedQueues.size} blocked queues`);
    await s.cancel('waiter');
    if ((s.blockedQueues.size as number) !== 0) throw new Error('The emptied queue survived its last waiter being cancelled');
    await held;
  }

  // --- Admission cost and honesty --------------------------------------------

  private async testAdmissionQueueComplete() {
    const N = 30;
    const g = new ConcurrentLimitGroup(1, 'g');
    const s = this.track(new FyflowScheduler({ W: this.pool({ groups: ['g'] }) }, { g }));
    const done = Promise.all(
      Array.from({ length: N }, (_, i) => s.addTask(this.task(`t${i}`, {}, 15), { createPromise: true }))
    );
    await new Promise(r => setTimeout(r, 50));
    const listed = s.getAdmissionQueue('g').g?.length ?? 0;
    // Everything not yet finished and not the one running is waiting on g
    const waiting = N - s.stats.done - g.running;
    await done;
    if (listed < waiting - 1) {
      throw new Error(`The admission queue lists ${listed} waiters while ${waiting} wait on the group`);
    }
  }

  /**
   * Admission work per task must not grow with how many tasks wait. Measured as
   * `canRun` calls and `resource.blocked` events per task, at two depths 8x
   * apart: flat is a ratio near 1, and the quadratic shape doubles with each
   * doubling of depth.
   */
  private async testCostFlat(shape: 'global' | 'keyed' | 'two-group') {
    const small = await this.measureShape(shape, 1);
    const large = await this.measureShape(shape, 8);
    const ratio = large.canRunPerTask / small.canRunPerTask;
    const eventRatio = large.blockedPerTask / Math.max(small.blockedPerTask, 0.5);
    console.log(`  ${shape}: canRun/task ${small.canRunPerTask.toFixed(1)} -> ${large.canRunPerTask.toFixed(1)}, ` +
      `blocked/task ${small.blockedPerTask.toFixed(1)} -> ${large.blockedPerTask.toFixed(1)}`);
    if (ratio > 1.5 || eventRatio > 1.5) {
      throw new Error(
        `${shape}: admission work per task grows with queue depth - canRun/task x${ratio.toFixed(1)}, ` +
        `resource.blocked/task x${eventRatio.toFixed(1)} for 8x the waiters`
      );
    }
  }

  private async measureShape(shape: 'global' | 'keyed' | 'two-group', scale: number) {
    const groups: Record<string, any> = shape === 'global'
      ? { g: new ConcurrentLimitGroup(7, 'g') }
      : shape === 'keyed'
        ? { k: new KeyedCounterGroup(2, 'k') }
        : { A: new ConcurrentLimitGroup(4, 'A'), B: new ConcurrentLimitGroup(4, 'B') };
    const counters = Object.values(groups).map(g => instrument(g));
    const s = this.track(new FyflowScheduler({ W: this.pool({ maxConcurrentTasks: 50 }) }, groups));
    let blocked = 0;
    s.addEventListener('resource.blocked', () => blocked++);

    const N = (shape === 'keyed' ? 500 : 1000) * scale;
    const tasks = Array.from({ length: N }, (_, i) => {
      const config: Record<string, any> =
        shape === 'global' ? { workerGroups: ['g'] }
        : shape === 'keyed' ? { workerGroups: ['k'], limitKey: `key${i % 10}` }
        : { workerGroups: i % 3 === 0 ? ['A', 'B'] : i % 3 === 1 ? ['A'] : ['B'] };
      return new FyflowTask({ id: `t${i}`, workerType: 'W', payload: { countConcurrency: true, taskId: i }, ...config });
    });
    await Promise.all(tasks.map(t => s.addTask(t, { createPromise: true })));
    await s.shutdown();
    const canRun = counters.reduce((sum, c) => sum + c.canRun, 0);
    return { canRunPerTask: canRun / N, blockedPerTask: blocked / N };
  }

  // --- Guards ----------------------------------------------------------------

  /**
   * Every kind of waiter at once - global, keyed, two-group and resident - with
   * cancels landing wherever tasks happen to be. Checks the limits and both
   * fold invariants of AGENTS.md §6.
   */
  private async testMixedContention() {
    const groups = {
      g: new ConcurrentLimitGroup(3, 'g'),
      k: new KeyedCounterGroup(2, 'k'),
      A: new ConcurrentLimitGroup(2, 'A'),
      B: new ConcurrentLimitGroup(2, 'B'),
      gpu: new ConcurrentLimitGroup(2, 'gpu')
    };
    const limits: Record<string, number> = { g: 3, k: 2, A: 2, B: 2, gpu: 2 };
    const counters = Object.fromEntries(Object.entries(groups).map(([id, g]) => [id, instrument(g)]));
    const s = this.track(new FyflowScheduler({
      W: this.pool({ maxConcurrentTasks: 10 }),
      R: this.pool({ maxThreads: 3, maxConcurrentTasks: 2, residentGroups: { gpu: 1 }, idleTimeout: 20, idleCheckIntervalMs: 10 }),
    }, groups));

    const membership = new Map<string, number>();
    const holdings = new Map<string, number>();
    let negative = '';
    const bump = (map: Map<string, number>, key: string, by: number) => {
      const next = (map.get(key) ?? 0) + by;
      map.set(key, next);
      return next;
    };
    s.addEventListener('resource.blocked', (e: any) => {
      const d = e.detail; bump(membership, `${d.groupId}|${d.key ?? ''}|${d.holderId}`, 1);
    });
    s.addEventListener('resource.unblocked', (e: any) => {
      const d = e.detail; bump(membership, `${d.groupId}|${d.key ?? ''}|${d.holderId}`, -1);
    });
    s.addEventListener('resource.acquired', (e: any) => {
      const d = e.detail; bump(holdings, `${d.groupId}|${d.key ?? ''}`, d.cost);
    });
    s.addEventListener('resource.released', (e: any) => {
      const d = e.detail;
      if (bump(holdings, `${d.groupId}|${d.key ?? ''}`, -d.cost) < 0) negative ||= `${d.groupId}|${d.key ?? ''}`;
    });

    const N = 300;
    const shapes = [['g'], ['k'], ['A'], ['B'], ['A', 'B'], ['g', 'k']];
    const promises: Promise<any>[] = [];
    for (let i = 0; i < N; i++) {
      const resident = i % 5 === 0;
      const workerGroups = resident ? [] : shapes[i % shapes.length];
      const p = s.addTask(new FyflowTask({
        id: `t${i}`, workerType: resident ? 'R' : 'W', workerGroups,
        limitKey: `key${i % 4}`, payload: { taskId: `t${i}`, delay: 1 + (i % 7) }
      }), { createPromise: true }) as Promise<any>;
      promises.push(p.then(() => 'completed', () => 'rejected'));
    }
    // Cancels land on waiting, running and finished tasks alike
    const cancels: Promise<boolean>[] = [];
    for (let i = 3; i < N; i += 7) {
      await new Promise(r => setTimeout(r, 1));
      cancels.push(s.cancel(`t${i}`, { graceMs: 0 }));
    }
    const outcomes = await Promise.all(promises);
    await Promise.all(cancels);

    const settled = outcomes.length;
    if (settled !== N) throw new Error(`${N - settled} tasks never settled`);
    const stats = s.stats;
    if (stats.done + stats.cancelled + stats.failed !== N) {
      throw new Error(`Counters do not add up: ${JSON.stringify(stats)}`);
    }
    if (stats.running !== 0 || stats.queued !== 0) throw new Error(`Left in flight: ${JSON.stringify(stats)}`);

    for (const [id, c] of Object.entries(counters)) {
      for (const [key, peak] of c.peak) {
        if (peak > limits[id]) throw new Error(`${id}${key ? `[${key}]` : ''} reached ${peak} against a limit of ${limits[id]}`);
      }
    }
    await s.shutdown();
    if (negative) throw new Error(`Holdings of ${negative} went negative`);
    const heldOver = [...holdings].filter(([, n]) => n !== 0);
    if (heldOver.length) throw new Error(`Holdings not released: ${JSON.stringify(heldOver)}`);
    const unbalanced = [...membership].filter(([, n]) => n !== 0);
    if (unbalanced.length) throw new Error(`Queue membership unbalanced: ${JSON.stringify(unbalanced.slice(0, 5))}`);
  }

  /**
   * A resource.acquired listener that re-enters the scheduler from inside
   * admission: it cancels the task being admitted, cancels the next waiter, and
   * adds a new task. Today's release path collects before acting; the rework
   * dispatches from inside it, so this is the case most likely to break.
   *
   * A task cancelled by its own `resource.acquired` must never reach a worker
   * (`_dispatchTask` settles it on the spot). The waiter-in-transit case, a
   * task cancelled after the retry loop collected it, is covered by the
   * cancellation suite's "Cancel From An Admitted Unblock".
   */
  /**
   * resource.acquired goes out before the task reaches a worker. A listener
   * adding a task there dispatches it at once, into the slot the first was
   * about to take - which then found no worker and no room for one, and failed
   * for good, ending in `user_action`. It did nothing wrong: it is requeued.
   */
  private async testSlotTakenRequeues(threaded: boolean) {
    const g = new ConcurrentLimitGroup(5, 'g');
    const s = this.track(new FyflowScheduler(
      { W: this.pool({ maxThreads: 1, maxConcurrentTasks: 1, inline: !threaded }) }, { g }
    ));
    let child: Promise<unknown> | undefined;
    s.addEventListener('resource.acquired', () => {
      child ??= s.addTask(this.task('child', {}, 10), { createPromise: true }) as Promise<unknown>;
    });
    const first = s.addTask(this.task('first', { workerGroups: ['g'] }, 10), { createPromise: true }) as Promise<unknown>;
    const outcome = await first.then(() => 'done', (e: any) => `failed: ${e?.message ?? e}`);
    await child;
    if (outcome !== 'done') throw new Error(`"first" ${outcome} (state ${s.tasks.get('first')?.state})`);
    if (g.getMetrics().running !== 0) throw new Error(`g still holds ${g.getMetrics().running}`);
  }

  /**
   * A worker's resident acquire is announced from inside its creation. A
   * listener adding a task there re-enters the dispatch loop, which must
   * already count the new worker: announced before it joined the pool, the
   * nested dispatch saw room for another and the pool outgrew maxThreads.
   */
  private async testResidentReentryMaxThreads(threaded: boolean) {
    const R = new ConcurrentLimitGroup(10, 'R');
    const pool = this.pool({ maxThreads: 1, maxConcurrentTasks: 1, inline: !threaded, residentGroups: { R: 1 } });
    const s = this.track(new FyflowScheduler({ W: pool }, { R }, { periodicRetryIntervalMs: HEARTBEAT_OFF }));
    let peakWorkers = 0, added = 0;
    const children: Promise<unknown>[] = [];
    s.addEventListener('resource.acquired', (e: any) => {
      peakWorkers = Math.max(peakWorkers, pool.getWorkerIds().length);
      if (e.detail.groupId === 'R' && added < 3) {
        children.push(s.addTask(this.task(`child-${added++}`, {}, 5), { createPromise: true }) as Promise<unknown>);
      }
    });
    await s.addTask(this.task('first', {}, 20), { createPromise: true });
    await Promise.all(children);
    const workers = Math.max(peakWorkers, pool.getWorkerIds().length);
    if (workers > 1) throw new Error(`maxThreads 1, but the pool had ${workers} workers`);
    if (R.getMetrics().running > 1) throw new Error(`R held ${R.getMetrics().running} units for one worker`);
  }

  private async testReentrantListeners() {
    const g = new ConcurrentLimitGroup(2, 'g');
    const s = this.track(new FyflowScheduler({ W: this.pool({ groups: ['g'] }) }, { g }));
    const promises: Promise<string>[] = [];
    const add = (id: string, delay: number) => {
      const p = s.addTask(this.task(id, {}, delay), { createPromise: true }) as Promise<any>;
      promises.push(p.then(() => 'completed', () => 'rejected'));
    };
    let added = 0;
    const selfCancelled = new Set<string>();
    const ran = new Set<string>();
    s.addEventListener('task.running', (e: any) => ran.add(e.detail.id));
    s.addEventListener('resource.acquired', (e: any) => {
      const id: string = e.detail.holderId;
      const m = /^w(\d+)$/.exec(id);
      if (!m) return;
      const i = Number(m[1]);
      if (i % 4 === 0) {
        selfCancelled.add(id);
        void s.cancel(id, { graceMs: 0 });
      } else if (i % 4 === 1) {
        void s.cancel(`w${i + 1}`, { graceMs: 0 });
        add(`fresh${added++}`, 3);
      }
    });

    add('h0', 20);
    add('h1', 20);
    for (let i = 0; i < 12; i++) add(`w${i}`, 3);

    // Fresh tasks are added while we wait, so wait until the count stops moving
    let seen = -1;
    while (seen !== promises.length) {
      seen = promises.length;
      await Promise.all(promises);
    }
    const stats = s.stats;
    if (stats.running !== 0 || stats.queued !== 0) throw new Error(`Left in flight: ${JSON.stringify(stats)}`);
    if (stats.done + stats.cancelled + stats.failed !== promises.length) {
      throw new Error(`Counters do not add up for ${promises.length} tasks: ${JSON.stringify(stats)}`);
    }
    if (g.running !== 0) throw new Error(`The group still holds ${g.running}`);
    if (added === 0 || selfCancelled.size === 0) throw new Error('The listener never ran - the test proves nothing');
    const leaked = [...selfCancelled].filter(id => ran.has(id));
    if (leaked.length) throw new Error(`Cancelled at admission, yet ran: ${leaked.join(', ')}`);
    const states = [...selfCancelled].map(id => s.tasks.get(id)?.state);
    if (states.some(state => state !== 'cancelled')) {
      throw new Error(`Cancelled at admission, yet ended ${JSON.stringify(states)}`);
    }
  }
}

// Auto-run tests when executed directly - handle both Deno and Node.js
if ((typeof Deno !== 'undefined' && import.meta.main) ||
    (typeof process !== 'undefined' && process.argv[1] && import.meta.url &&
     import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/')))) {
  const testSuite = new AdmissionTestSuite();
  try {
    await testSuite.runAllTests(true);
  } finally {
    await testSuite.cleanup();
  }
}

export { AdmissionTestSuite };
export default AdmissionTestSuite;
