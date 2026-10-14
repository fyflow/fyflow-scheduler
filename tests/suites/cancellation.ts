// Task cancellation: scheduler.cancel().
//
// A task can be in seven places when a cancel arrives. Waiting, and removed at
// once: 1. the ready queue, 2. a blocked queue, 3. a retry backoff. Dispatched,
// and settled once its attempt stops: 4. on its way to a new thread
// (enqueueNoQueue awaiting it), 5. in a thread's start-up queue, 6. inline,
// before run(), 7. inside run() - the only one where the worker sees its
// signal, and on a thread the only one that can need a forced stop. These
// tests pin each, the events a listener can cancel from mid-transition, the
// forced stop, and the counters. The counter check is exact - queued + running + done + failed +
// cancelled + blocked equals the tasks added - because the counters are clamped
// at 0, which hides a wrong decrement from a "never exceeds" check.

import { WorkerManager, FyflowScheduler, FyflowTask, ConcurrentLimitGroup, KeyedRateLimitGroup, CancelledError } from '../../index.ts';
import { runTest, type TestResult } from '../harness.ts';

// Node.js process declaration for cross-platform compatibility
declare const process: any;

interface TestSuiteResult {
  platform: string; totalTests: number; passed: number; failed: number;
  duration: number; results: TestResult[];
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

/** Cancelling 50,000 tasks: milliseconds when linear, seconds when not. */
const SCALE_BOUND_MS = 1000;

class CancellationTestSuite {
  private results: TestResult[] = [];
  private startTime = 0;
  private inlineWorkerUrl = "";
  private spawningWorkerUrl = "";
  private cancelWorkerUrl = "";
  private schedulers: FyflowScheduler[] = [];

  async cleanup(): Promise<void> {
    for (const scheduler of this.schedulers) {
      try { await scheduler.shutdown(); } catch { /* already down */ }
    }
    this.schedulers = [];
  }

  private async runTest(name: string, testFn: () => Promise<void>): Promise<TestResult> {
    return runTest(name, testFn);
  }

  async runAllTests(exitOnComplete = true): Promise<TestSuiteResult> {
    if (typeof Deno !== "undefined") {
      this.inlineWorkerUrl = new URL("../workers/testInlineWorker.ts", import.meta.url).href;
      this.spawningWorkerUrl = new URL("../workers/spawningWorker.ts", import.meta.url).href;
      this.cancelWorkerUrl = new URL("../workers/cancelWorker.ts", import.meta.url).href;
    } else {
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      this.inlineWorkerUrl = new URL((await import('../workers/testInlineWorker.ts?worker-direct')).default).href;
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      this.spawningWorkerUrl = new URL((await import('../workers/spawningWorker.ts?worker-direct')).default).href;
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      this.cancelWorkerUrl = new URL((await import('../workers/cancelWorker.ts?worker-direct')).default).href;
    }

    const platform = typeof globalThis !== 'undefined' && 'Deno' in globalThis ? 'Deno' : 'Node.js';
    console.log(`🚀 FyFlow Cancellation Tests - ${platform}`);
    console.log('='.repeat(60));

    this.startTime = performance.now();
    this.results = [];

    this.results.push(await this.runTest('Cancel A Queued Task', () => this.testQueued()));
    this.results.push(await this.runTest('Cancel A Blocked Task', () => this.testBlocked()));
    this.results.push(await this.runTest('Cancel During A Retry Backoff', () => this.testBackoff()));
    this.results.push(await this.runTest('Cancel A Running Task: Cancel Wins Over Its Result', () => this.testRunningResultDiscarded()));
    this.results.push(await this.runTest('Cancel A Running Task: No Retry After Its Failure', () => this.testRunningFailureNotRetried()));
    this.results.push(await this.runTest('Cancel From resource.acquired', () => this.testFromAcquired()));
    this.results.push(await this.runTest('Cancel From resource.released Mid-Settle', () => this.testFromReleased()));
    this.results.push(await this.runTest('Cancel From resource.blocked', () => this.testFromBlocked()));
    this.results.push(await this.runTest('Cancel From An Admitted Unblock', () => this.testFromAdmitted()));
    this.results.push(await this.runTest('A task.cancelled Listener Sees No Cancelled Waiter', () => this.testFromCancelled()));
    this.results.push(await this.runTest('cancel() Contract', () => this.testContract()));
    this.results.push(await this.runTest('A Cancelled Task Object Cannot Be Re-Added', () => this.testReAdd()));
    this.results.push(await this.runTest('scheduler.completed Without A Success', () => this.testCompletion()));
    this.results.push(await this.runTest('Spawns From A Cancelled Task Are Refused', () => this.testSpawnRefused()));
    this.results.push(await this.runTest('onCompleteDescendants Rejects For A Cancelled Root', () => this.testDescendants()));
    this.results.push(await this.runTest('Cancelled Before Reaching A New Thread', () => this.testBeforeThread()));
    this.results.push(await this.runTest('Cancelled While Its Thread Initialises', () => this.testThreadInitialising()));
    this.results.push(await this.runTest('Cancelled Before An Inline run()', () => this.testBeforeInlineRun()));
    this.results.push(await this.runTest('Cooperative Cancel: The Worker Sees The Signal', () => this.testCooperative()));
    this.results.push(await this.runTest('A Worker Ignoring The Signal Keeps The Task Cancelling', () => this.testIgnored()));
    this.results.push(await this.runTest('Worker Failure While Cancelling: Threaded', () => this.testFailureThreaded()));
    this.results.push(await this.runTest('Worker Failure While Cancelling: Inline', () => this.testFailureInline()));
    this.results.push(await this.runTest('A Stale Outcome Does Not Settle A Cancelling Attempt', () => this.testCheckOrder()));
    this.results.push(await this.runTest('Capacity Race: A Cancelling Task Is Not Requeued', () => this.testCapacityRace()));
    this.results.push(await this.runTest('Shutdown Resolves A Pending Inline Cancel', () => this.testShutdownInline()));
    this.results.push(await this.runTest('Forced Stop: A Spinning Worker', () => this.testForcedStop()));
    this.results.push(await this.runTest('Forced Stop: Results Posted During Teardown Settle Nothing', () => this.testForcedLateResult()));
    this.results.push(await this.runTest('Forced Stop: Two Cancelling Tasks On One Worker', () => this.testForcedTwo()));
    this.results.push(await this.runTest('Forced Stop: A Stale Grace Timer', () => this.testStaleGraceTimer()));
    this.results.push(await this.runTest('Forced Stop: During Shutdown', () => this.testForcedDuringShutdown()));
    this.results.push(await this.runTest('Shutdown Forces A Pending Threaded Cancel', () => this.testShutdownThreaded()));
    this.results.push(await this.runTest('Grace Period: Edge Cases', () => this.testGraceEdges()));
    this.results.push(await this.runTest('Cancel At Scale', () => this.testAtScale()));

    const totalDuration = performance.now() - this.startTime;
    this.results = this.results.filter(r => !r.skipped);
    const passed = this.results.filter(r => r.passed).length;
    const failed = this.results.length - passed;

    console.log(`\n📊 Cancellation Test Summary - ${platform}`);
    console.log('='.repeat(60));
    console.log(`Total Tests: ${this.results.length}`);
    console.log(`✅ Passed: ${passed}`);
    console.log(`❌ Failed: ${failed}`);
    console.log(`⏱️  Duration: ${totalDuration.toFixed(1)}ms`);

    if (failed > 0) {
      console.log('\n❌ Failed Tests:');
      this.results.filter(r => !r.passed).forEach(r => console.log(`  • ${r.name}: ${r.error}`));
      if (exitOnComplete) {
        if (typeof Deno !== 'undefined') { Deno.exit(1); } else { process.exit(1); }
      }
    } else {
      console.log('\n🎉 All cancellation tests passed!');
    }

    return { platform, totalTests: this.results.length, passed, failed, duration: totalDuration, results: this.results };
  }

  // --- helpers ---------------------------------------------------------------

  private make(poolOptions: any = {}, groups: Record<string, any> = {}, url = this.inlineWorkerUrl) {
    const pool = new WorkerManager(url, {
      maxThreads: 1, maxConcurrentTasks: 1, inline: true, idleTimeout: 0, ...poolOptions
    });
    const scheduler = new FyflowScheduler({ W: pool }, groups);
    this.schedulers.push(scheduler);
    return { pool, scheduler };
  }

  private task(id: string, payload: any = {}, extra: any = {}) {
    return new FyflowTask({ id, workerType: 'W', payload: { taskId: id, ...payload }, ...extra });
  }

  /** Every task.* event, in order, as `event:id`. */
  private events(scheduler: FyflowScheduler) {
    const log: string[] = [];
    for (const ev of ['task.running', 'task.cancelling', 'task.cancelled', 'task.completed', 'task.failed', 'task.retrying']) {
      scheduler.addEventListener(ev, (e: any) => log.push(`${ev.slice(5)}:${e.detail.id}`));
    }
    return log;
  }

  /** queued + running + done + failed + cancelled + blocked must equal the tasks added. */
  private assertAccounted(scheduler: FyflowScheduler, added: number, where: string) {
    const s = scheduler.stats as any;
    const blocked = Object.values(scheduler.getAdmissionQueue()).reduce((n, q) => n + q.length, 0);
    const total = s.queued + s.running + s.done + s.failed + s.cancelled + blocked;
    if (total !== added) {
      throw new Error(`${where}: ${JSON.stringify({ ...s, blocked })} accounts for ${total} of ${added}`);
    }
    // The count the drained check reads, kept by the queues themselves
    const live = (scheduler as any).blockedLive.live;
    if (live !== blocked) throw new Error(`${where}: live blocked count ${live}, ${blocked} actually blocked`);
  }

  private async rejection(promise: Promise<any> | void): Promise<any> {
    try {
      await Promise.race([promise, sleep(3000).then(() => { throw new Error('NEVER SETTLED'); })]);
    } catch (error) {
      return error;
    }
    return undefined;
  }

  /** task.progress from cancelWorker, as `id:message`, with each 'stopped' detail kept by id. */
  private progress(scheduler: FyflowScheduler) {
    const log: string[] = [];
    const stopped = new Map<string, any>();
    const at = new Map<string, number>();
    scheduler.addEventListener('task.progress', (e: any) => {
      log.push(`${e.detail.id}:${e.detail.message}`);
      at.set(`${e.detail.id}:${e.detail.message}`, performance.now());
      if (e.detail.message === 'stopped') stopped.set(e.detail.id, e.detail.details);
    });
    return { log, stopped, at };
  }

  /** When each task.cancelled fired, by task id. */
  private cancelledAt(scheduler: FyflowScheduler) {
    const at = new Map<string, number>();
    scheduler.addEventListener('task.cancelled', (e: any) => at.set(e.detail.id, performance.now()));
    return at;
  }

  /** The value, or a throw if it has not settled within `ms`. */
  private async within<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
    return await Promise.race([
      promise,
      sleep(ms).then((): never => { throw new Error(`${what}: not settled within ${ms} ms`); })
    ]);
  }

  /** When the first teardown of a worker of this pool ended, and how many began. */
  private teardownOf(pool: WorkerManager) {
    const seen = { endedAt: 0, started: 0 };
    pool.addEventListener('worker.teardown.started', () => { seen.started++; });
    for (const ev of ['worker.teardown.completed', 'worker.teardown.failed']) {
      pool.addEventListener(ev, () => { seen.endedAt ||= performance.now(); });
    }
    return seen;
  }

  /** Every time a task's run() began, by task id. */
  private entries(scheduler: FyflowScheduler) {
    const at = new Map<string, number[]>();
    scheduler.addEventListener('task.progress', (e: any) => {
      if (e.detail.message !== 'entered') return;
      if (!at.has(e.detail.id)) at.set(e.detail.id, []);
      at.get(e.detail.id)!.push(performance.now());
    });
    return at;
  }

  private async until(predicate: () => boolean, ms = 3000) {
    const deadline = performance.now() + ms;
    while (!predicate()) {
      if (performance.now() > deadline) throw new Error('Timed out waiting');
      await sleep(5);
    }
  }

  // --- places 1-3: never dispatched, or waiting to be again ---------------------

  private async testQueued(): Promise<void> {
    const { scheduler } = this.make();
    const log = this.events(scheduler);
    const busy = scheduler.addTask(this.task('busy', { delay: 100 }), { createPromise: true });
    const tasks = ['q1', 'q2', 'q3'].map(id => this.task(id, { delay: 5 }));
    const promises = tasks.map(t => scheduler.addTask(t, { createPromise: true }));
    this.assertAccounted(scheduler, 4, 'before');

    const cancelled = await scheduler.cancel('q2', { reason: 'user pressed cancel' });
    if (!cancelled) throw new Error('cancel() resolved false for a queued task');
    if (tasks[1].state !== 'cancelled') throw new Error(`state ${tasks[1].state}`);
    this.assertAccounted(scheduler, 4, 'after cancel');
    if (scheduler.stats.queued !== 2) throw new Error(`queued ${scheduler.stats.queued}, expected 2`);

    const error = await this.rejection(promises[1]);
    if (!(error instanceof CancelledError) || error.reason !== 'user pressed cancel' || error.taskId !== 'q2') {
      throw new Error(`Rejected with ${error}`);
    }
    await Promise.all([busy, promises[0], promises[2]]);
    if (log.some(e => e.endsWith(':q2') && e !== 'cancelled:q2')) throw new Error(`q2 events: ${log.filter(e => e.endsWith(':q2'))}`);
    if (!log.includes('cancelled:q2')) throw new Error('No task.cancelled');
    this.assertAccounted(scheduler, 4, 'end');
  }

  // Run with stats.queued > 0, so a wrong queued decrement is not hidden by
  // the clamp at 0
  private async testBlocked(): Promise<void> {
    const group = new ConcurrentLimitGroup(1);
    const { scheduler } = this.make({ maxConcurrentTasks: 1, maxThreads: 2, groups: ['g'] }, { g: group });
    const log = this.events(scheduler);
    const fold = new Map<string, number>();
    scheduler.addEventListener('resource.blocked', (e: any) => fold.set(e.detail.holderId, (fold.get(e.detail.holderId) ?? 0) + 1));
    const reasons: string[] = [];
    scheduler.addEventListener('resource.unblocked', (e: any) => {
      fold.set(e.detail.holderId, (fold.get(e.detail.holderId) ?? 0) - 1);
      reasons.push(`${e.detail.holderId}:${e.detail.reason}`);
    });

    scheduler.addTask(this.task('holder', { delay: 150 }));
    // One batch, so one dispatch pass: b1 is blocked, and the others wait in the
    // ready queue behind it
    scheduler.addTasks(['b1', 'b2', 'b3', 'b4'].map(id => this.task(id, { delay: 5 })));
    await sleep(20);
    const blockedBefore = scheduler.getAdmissionQueue('g').g?.map(w => w.holderId) ?? [];
    if (!blockedBefore.includes('b1') || scheduler.stats.queued === 0) {
      throw new Error(`Precondition: blocked ${blockedBefore}, queued ${scheduler.stats.queued}`);
    }
    const queuedBefore = scheduler.stats.queued;

    await scheduler.cancel('b1');
    if (scheduler.stats.queued !== queuedBefore) throw new Error(`Cancelling a blocked task moved queued ${queuedBefore} -> ${scheduler.stats.queued}`);
    if ((scheduler.getAdmissionQueue('g').g ?? []).some(w => w.holderId === 'b1')) throw new Error('Still in the admission queue');
    if (!reasons.includes('b1:cancelled')) throw new Error(`Unblock reasons: ${reasons}`);
    this.assertAccounted(scheduler, 5, 'after cancel');

    await this.until(() => scheduler.stats.done === 4);
    if (log.includes('running:b1')) throw new Error('b1 ran');
    for (const [holder, balance] of fold) if (balance !== 0) throw new Error(`Fold for ${holder} ended at ${balance}`);
    this.assertAccounted(scheduler, 5, 'end');
  }

  private async testBackoff(): Promise<void> {
    const { scheduler } = this.make();
    const log = this.events(scheduler);
    const task = this.task('flaky', { shouldThrow: true }, { retryPolicy: { maxRetries: 3, backoffMs: 300 } });
    const promise = scheduler.addTask(task, { createPromise: true });
    await this.until(() => log.includes('retrying:flaky'));
    if (task.error === undefined) throw new Error('Precondition: the failed attempt left no error');

    await scheduler.cancel('flaky');
    if (scheduler.stats.queued !== 0) throw new Error(`queued ${scheduler.stats.queued}`);
    if (task.error !== undefined) throw new Error(`A cancelled task kept the earlier attempt's error "${task.error}"`);
    if (!(await this.rejection(promise) instanceof CancelledError)) throw new Error('Promise did not reject with CancelledError');
    await sleep(400);
    if (log.filter(e => e === 'running:flaky').length !== 1) throw new Error(`Ran again after cancel: ${log}`);
    this.assertAccounted(scheduler, 1, 'end');
  }

  // --- dispatched: cancel wins ----------------------------------------------

  private async testRunningResultDiscarded(): Promise<void> {
    const group = new ConcurrentLimitGroup(1);
    const { pool, scheduler } = this.make({ groups: ['g'] }, { g: group }, this.cancelWorkerUrl);
    const log = this.events(scheduler);
    const { log: progress } = this.progress(scheduler);
    // The pool's own event: the attempt really did produce a result
    let produced = false;
    pool.addEventListener('task.completed', () => { produced = true; });
    const task = this.task('runner', { mode: 'ignore', delay: 80 });
    const promise = scheduler.addTask(task, { createPromise: true });
    // Inside run(): cancelled any earlier, it would never start
    await this.until(() => progress.includes('runner:entered'));

    let resolvedAt = 0;
    const cancelled = scheduler.cancel('runner', { reason: 'stop' }).then(v => { resolvedAt = performance.now(); return v; });
    const again = scheduler.cancel('runner');
    if (task.state !== 'running' || !task.cancellation) throw new Error(`While cancelling: state ${task.state}`);
    if (group.running !== 1) throw new Error('Released its resources at the request');

    if (await cancelled !== true || await again !== true) throw new Error('cancel() did not resolve true');
    if (!produced) throw new Error('Precondition: the attempt never produced its result');
    // Read through calls: the checks above narrowed both
    const state = (): string => task.state;
    const held = (): number => group.running;
    if (state() !== 'cancelled' || task.result !== undefined) throw new Error(`Ended ${state()}, result ${JSON.stringify(task.result)}`);
    if (held() !== 0) throw new Error(`Group still holds ${held()}`);
    if (!(await this.rejection(promise) instanceof CancelledError)) throw new Error('Promise did not reject with CancelledError');
    const order = log.filter(e => e.endsWith(':runner')).join(' ');
    if (order !== 'running:runner cancelling:runner cancelled:runner') throw new Error(order);
    if (!resolvedAt) throw new Error('cancel() never resolved');
    this.assertAccounted(scheduler, 1, 'end');
  }

  private async testRunningFailureNotRetried(): Promise<void> {
    const { pool, scheduler } = this.make({}, {}, this.cancelWorkerUrl);
    const log = this.events(scheduler);
    const { log: progress } = this.progress(scheduler);
    let threw = false;
    pool.addEventListener('task.failed', () => { threw = true; });
    // Cancelled while its first attempt runs; that attempt then throws
    const task = this.task('thrower', { mode: 'throw', delay: 50 }, { retryPolicy: { maxRetries: 3, backoffMs: 5 } });
    const promise = scheduler.addTask(task, { createPromise: true });
    await this.until(() => progress.includes('thrower:entered'));
    scheduler.cancel('thrower');
    await this.rejection(promise);
    await sleep(50);
    if (!threw) throw new Error('Precondition: the attempt never threw');
    if (task.state !== 'cancelled') throw new Error(`Ended ${task.state}`);
    if (log.some(e => e.startsWith('retrying') || e.startsWith('failed'))) throw new Error(`Events: ${log}`);
    this.assertAccounted(scheduler, 1, 'end');
  }

  // --- re-entrancy: a cancel from any event, mid-transition ---------------------

  private async testFromAcquired(): Promise<void> {
    const group = new ConcurrentLimitGroup(1);
    const { scheduler } = this.make({ groups: ['g'] }, { g: group });
    const log = this.events(scheduler);
    scheduler.addEventListener('resource.acquired', (e: any) => {
      if (e.detail.holderId === 'grabbed') scheduler.cancel('grabbed');
    });
    const task = this.task('grabbed', { delay: 5 });
    const error = await this.rejection(scheduler.addTask(task, { createPromise: true }));
    if (!(error instanceof CancelledError)) throw new Error(`Rejected with ${error}`);
    if (log.includes('running:grabbed')) throw new Error('It reached the pool');
    if (group.running !== 0) throw new Error(`Group holds ${group.running}`);
    if (scheduler.stats.cancelled !== 1) throw new Error(`cancelled ${scheduler.stats.cancelled}`);
    this.assertAccounted(scheduler, 1, 'end');
  }

  private async testFromReleased(): Promise<void> {
    const group = new ConcurrentLimitGroup(1);
    const { scheduler } = this.make({ groups: ['g'] }, { g: group });
    const log = this.events(scheduler);
    scheduler.addEventListener('resource.released', (e: any) => {
      if (e.detail.holderId === 'midsettle') scheduler.cancel('midsettle');
    });
    const task = this.task('midsettle', { shouldThrow: true }, { retryPolicy: { maxRetries: 3, backoffMs: 5 } });
    await this.rejection(scheduler.addTask(task, { createPromise: true }));
    await sleep(50);
    if (task.state !== 'cancelled') throw new Error(`Ended ${task.state}`);
    if (log.filter(e => e === 'running:midsettle').length !== 1 || log.includes('retrying:midsettle')) throw new Error(`Events: ${log}`);
    this.assertAccounted(scheduler, 1, 'end');
  }

  // Cancels the task just blocked, and adds another task - the re-entrancy
  // that once lost a task
  private async testFromBlocked(): Promise<void> {
    const group = new ConcurrentLimitGroup(1);
    const { scheduler } = this.make({ maxThreads: 2, groups: ['g'] }, { g: group });
    let added = 3;
    let reentered = false;
    scheduler.addEventListener('resource.blocked', (e: any) => {
      if (e.detail.holderId !== 'victim') return;
      scheduler.cancel('victim');
      if (!reentered) {
        reentered = true;
        scheduler.addTask(this.task('late', { delay: 5 }));
        added++;
      }
    });
    scheduler.addTask(this.task('holder', { delay: 60 }));
    scheduler.addTask(this.task('victim', { delay: 5 }));
    scheduler.addTask(this.task('after', { delay: 5 }));
    await this.until(() => scheduler.stats.done === 3);
    await sleep(20);
    if ((scheduler.tasks.get('victim') as any).state !== 'cancelled') throw new Error('victim was not cancelled');
    this.assertAccounted(scheduler, added, 'end');
  }

  // The retry loop has already collected the task when a listener on an
  // earlier task's 'admitted' cancels it
  private async testFromAdmitted(): Promise<void> {
    const group = new ConcurrentLimitGroup(2);
    const { scheduler } = this.make({ maxThreads: 3, groups: ['g'] }, { g: group });
    const log = this.events(scheduler);
    scheduler.addEventListener('resource.unblocked', (e: any) => {
      if (e.detail.reason === 'admitted' && e.detail.holderId === 'w1') scheduler.cancel('w2');
    });
    scheduler.addTask(this.task('h1', { delay: 50 }));
    scheduler.addTask(this.task('h2', { delay: 50 }));
    scheduler.addTask(this.task('w1', { delay: 5 }));
    scheduler.addTask(this.task('w2', { delay: 5 }));
    await this.until(() => scheduler.stats.done + scheduler.stats.cancelled === 4);
    await sleep(20);
    if (log.includes('running:w2')) throw new Error('w2 ran after being cancelled');
    this.assertAccounted(scheduler, 4, 'end');
  }

  private async testFromCancelled(): Promise<void> {
    const group = new ConcurrentLimitGroup(1);
    const { scheduler } = this.make({ maxThreads: 2, groups: ['g'] }, { g: group });
    let seen: string[] | undefined;
    let position: number | undefined;
    scheduler.addEventListener('resource.blocked', (e: any) => {
      if (e.detail.holderId === 'fresh') position = e.detail.queuePosition;
    });
    scheduler.addEventListener('task.cancelled', () => {
      seen = (scheduler.getAdmissionQueue('g').g ?? []).map(w => `${w.holderId}@${w.position}`);
      scheduler.addTask(this.task('fresh', { delay: 5 }));
    });
    scheduler.addTask(this.task('holder', { delay: 60 }));
    scheduler.addTask(this.task('x1', { delay: 5 }));
    scheduler.addTask(this.task('x2', { delay: 5 }));
    await sleep(10);
    // Only x1 is blocked (one per dispatch pass); queue x2 behind it
    await this.until(() => (scheduler.getAdmissionQueue('g').g ?? []).length >= 1);
    await scheduler.cancel('x1');
    if (!seen || seen.some(w => w.startsWith('x1'))) throw new Error(`Admission queue in the listener: ${seen}`);
    await this.until(() => scheduler.stats.done === 3);
    if (position !== undefined && position > (seen?.length ?? 0)) throw new Error(`fresh queuePosition ${position} counted a cancelled waiter`);
    this.assertAccounted(scheduler, 4, 'end');
  }

  // --- the API --------------------------------------------------------------

  private async testContract(): Promise<void> {
    const { scheduler } = this.make();
    if (await scheduler.cancel('nobody') !== false) throw new Error('Unknown id should resolve false');

    await scheduler.addTask(this.task('finished', { delay: 1 }), { createPromise: true });
    if (await scheduler.cancel('finished') !== false) throw new Error('A done task should resolve false');

    scheduler.addTask(this.task('blocker', { delay: 50 }));
    const queued = this.task('queued', { delay: 1 });
    scheduler.addTask(queued, { createPromise: true });
    if (await scheduler.cancel('queued') !== true) throw new Error('First cancel');
    if (await scheduler.cancel('queued') !== true) throw new Error('An already-cancelled task should resolve true');

    const running = this.task('running', { delay: 60 });
    scheduler.addTask(running);
    await this.until(() => running.state === 'running');
    const first = scheduler.cancel('running');
    if (scheduler.cancel('running') !== first) throw new Error('A repeat cancel should return the same promise');

    const throwsWith = (fn: () => unknown, type: Function) => {
      try { fn(); } catch (error) { return error instanceof type; }
      return false;
    };
    if (!throwsWith(() => scheduler.cancel('running', { graceMs: '1' as any }), TypeError)) throw new Error('graceMs string');
    if (!throwsWith(() => scheduler.cancel('running', { graceMs: -1 }), RangeError)) throw new Error('graceMs -1');
    if (!throwsWith(() => scheduler.cancel('running', { graceMs: NaN }), RangeError)) throw new Error('graceMs NaN');
    // Not a programming error: Infinity means never force
    scheduler.cancel('running', { graceMs: Infinity });
    await first;
  }

  private async testReAdd(): Promise<void> {
    const { scheduler } = this.make();
    scheduler.addTask(this.task('blocker', { delay: 30 }));
    const task = this.task('again', { delay: 1 });
    scheduler.addTask(task);
    await scheduler.cancel('again');
    for (const add of [() => scheduler.addTask(task), () => scheduler.addTasks([task])]) {
      let threw = false;
      try { add(); } catch { threw = true; }
      if (!threw) throw new Error('A cancelled task object was accepted again');
    }
    const fresh = this.task('again', { delay: 1 });
    await scheduler.addTask(fresh, { createPromise: true });
    if (fresh.state !== 'done') throw new Error(`A new object with the same id ended ${fresh.state}`);
  }

  private async testCompletion(): Promise<void> {
    for (const kind of ['cancelled', 'failed']) {
      const { scheduler } = this.make();
      let completed = 0;
      scheduler.addEventListener('scheduler.completed', () => completed++);
      const tasks = ['c1', 'c2', 'c3'].map(id => this.task(`${kind}-${id}`, kind === 'failed' ? { shouldThrow: true } : { delay: 20 }, { optional: true }));
      scheduler.addTasks(tasks);
      if (kind === 'cancelled') for (const t of tasks) scheduler.cancel(t.id);
      await sleep(150);
      if (completed < 1) throw new Error(`scheduler.completed never fired for an all-${kind} run`);
    }
  }

  private async testSpawnRefused(): Promise<void> {
    const { scheduler } = this.make({}, {}, this.spawningWorkerUrl);
    const refused: any[] = [];
    scheduler.addEventListener('task.spawn_failed', (e: any) => refused.push(e.detail));
    // Spawns two children after 60 ms; cancelled at 20 ms, while running
    const parent = new FyflowTask({ id: 'parent', workerType: 'W', payload: { spawn: 2, childId: 'kid', delay: 60 } });
    scheduler.addTask(parent);
    await sleep(20);
    await scheduler.cancel('parent');
    await sleep(20);
    if (scheduler.tasks.has('kid-0') || scheduler.tasks.has('kid-1')) throw new Error('A cancelled task spawned children');
    if (refused.length !== 2 || !(refused[0].error instanceof CancelledError)) throw new Error(`spawn_failed: ${refused.length}`);
  }

  private async testDescendants(): Promise<void> {
    const { scheduler } = this.make();
    scheduler.addTask(this.task('blocker', { delay: 30 }));
    const root = this.task('root', { delay: 1 });
    scheduler.addTask(root);
    const wait = root.onCompleteDescendants();
    await scheduler.cancel('root');
    if (!(await this.rejection(wait) instanceof CancelledError)) throw new Error('The wait did not reject with CancelledError');
  }


  // --- places 4-7: dispatched ------------------------------------------------

  // Place 4. addTask runs the dispatch synchronously, and on a threaded pool
  // with no worker yet, enqueueNoQueue awaits the new thread before calling
  // runTask - so a cancel in the same turn lands in between
  private async testBeforeThread(): Promise<void> {
    const group = new ConcurrentLimitGroup(1);
    const { pool, scheduler } = this.make({ inline: false, groups: ['g'] }, { g: group }, this.cancelWorkerUrl);
    const { log } = this.progress(scheduler);
    let started = 0;
    pool.addEventListener('task.started', () => started++);
    let acquired = 0, released = 0;
    scheduler.addEventListener('resource.acquired', () => acquired++);
    scheduler.addEventListener('resource.released', () => released++);

    const task = this.task('p4');
    const promise = scheduler.addTask(task, { createPromise: true });
    if (!(pool as any).enqueuing.has('p4')) throw new Error('Precondition: not between dispatch and its thread');
    if (await this.within(scheduler.cancel('p4'), 3000, 'cancel()') !== true) throw new Error('cancel() resolved false');

    if (!(await this.rejection(promise) instanceof CancelledError)) throw new Error('Promise did not reject with CancelledError');
    // Long enough for the thread to have started it, had it been handed over
    await sleep(400);
    if (started !== 0 || log.includes('p4:entered')) throw new Error('It reached the thread');
    if (acquired !== 1 || released !== 1 || group.running !== 0) throw new Error(`acquired ${acquired}, released ${released}, held ${group.running}`);
    this.assertAccounted(scheduler, 1, 'end');
  }

  // Place 5. Handed to a fresh thread, waiting in its queue for it to initialise
  private async testThreadInitialising(): Promise<void> {
    const { pool, scheduler } = this.make({ inline: false }, {}, this.cancelWorkerUrl);
    const { log } = this.progress(scheduler);
    let started = 0;
    pool.addEventListener('task.started', () => started++);

    const promise = scheduler.addTask(this.task('p5'), { createPromise: true });
    await sleep(0);
    const wrapper = pool.threads[0] as any;
    if (!wrapper || wrapper.initialized || !wrapper.taskQueue.some((t: any) => t.taskId === 'p5')) {
      throw new Error('Precondition: not waiting for its thread to initialise');
    }
    if (await this.within(scheduler.cancel('p5'), 3000, 'cancel()') !== true) throw new Error('cancel() resolved false');
    if (!(await this.rejection(promise) instanceof CancelledError)) throw new Error('Promise did not reject with CancelledError');

    await this.until(() => wrapper.initialized);
    await sleep(100);
    if (started !== 0 || log.includes('p5:entered')) throw new Error('It ran once the thread was ready');
    this.assertAccounted(scheduler, 1, 'end');
  }

  // Place 6. On an inline pool the task is running from the start of runTask,
  // before the worker instance exists
  private async testBeforeInlineRun(): Promise<void> {
    const { pool, scheduler } = this.make({}, {}, this.cancelWorkerUrl);
    const { log } = this.progress(scheduler);
    let poolFailed = 0;
    pool.addEventListener('task.failed', () => poolFailed++);

    const task = this.task('p6');
    const promise = scheduler.addTask(task, { createPromise: true });
    if (task.state !== 'running' || (pool.threads[0] as any)?.workerInstance) {
      throw new Error('Precondition: not waiting for the worker instance');
    }
    if (await this.within(scheduler.cancel('p6'), 3000, 'cancel()') !== true) throw new Error('cancel() resolved false');
    if (!(await this.rejection(promise) instanceof CancelledError)) throw new Error('Promise did not reject with CancelledError');
    await sleep(50);
    if (log.includes('p6:entered')) throw new Error('run() was called');
    if (poolFailed !== 0) throw new Error('The pool reported it failed');

    // The worker instance is fine: the next task runs on it
    await this.within(scheduler.addTask(this.task('next', { delay: 1 }), { createPromise: true }) as Promise<any>, 3000, 'next task');
    this.assertAccounted(scheduler, 2, 'end');
  }

  // Place 7, on both pool types: a worker honouring the signal stops at once
  private async testCooperative(): Promise<void> {
    for (const inline of [true, false]) {
      const kind = inline ? 'inline' : 'threaded';
      const { scheduler } = this.make({ inline }, {}, this.cancelWorkerUrl);
      const log = this.events(scheduler);
      const { log: progress, stopped } = this.progress(scheduler);

      // With a reason, which reaches the worker's signal
      const id = `coop-${kind}`;
      const promise = scheduler.addTask(this.task(id, { delay: 10_000 }), { createPromise: true });
      await this.until(() => progress.includes(`${id}:entered`));
      // The worker's abort handler reports synchronously; it must not run
      // inside the caller's cancel() - deferred inline, a message on a thread
      let insideCancel = false;
      let handlerRanInside: boolean | undefined;
      scheduler.addEventListener('task.progress', (e: any) => {
        if (e.detail.id === id && e.detail.message === 'abort-handler') handlerRanInside = insideCancel;
      });
      const start = performance.now();
      insideCancel = true;
      const request = scheduler.cancel(id, { reason: 'user stop' });
      insideCancel = false;
      if (await this.within(request, 3000, `${kind} cancel()`) !== true) {
        throw new Error(`${kind}: cancel() resolved false`);
      }
      if (performance.now() - start > 2000) throw new Error(`${kind}: took ${performance.now() - start} ms`);
      const seen = stopped.get(id);
      if (!seen?.aborted || seen.reason !== 'user stop') throw new Error(`${kind}: the worker saw ${JSON.stringify(seen)}`);
      if (handlerRanInside !== false) throw new Error(`${kind}: abort handler ${handlerRanInside ? 'ran inside cancel()' : 'never reported'}`);
      const order = log.filter(e => e.endsWith(`:${id}`)).join(' ');
      if (order !== `running:${id} cancelling:${id} cancelled:${id}`) throw new Error(`${kind}: ${order}`);
      if (!(await this.rejection(promise) instanceof CancelledError)) throw new Error(`${kind}: not a CancelledError`);

      // Without one - or, on a thread, with one that cannot be cloned - the
      // worker sees the standard AbortError, and the host keeps the reason
      const bare = `bare-${kind}`;
      const unclonable = () => 'not cloneable';
      const barePromise = scheduler.addTask(this.task(bare, { delay: 10_000 }), { createPromise: true });
      await this.until(() => progress.includes(`${bare}:entered`));
      await this.within(scheduler.cancel(bare, inline ? {} : { reason: unclonable }), 3000, `${kind} bare cancel()`);
      if (stopped.get(bare)?.reasonName !== 'AbortError') throw new Error(`${kind}: the worker saw ${JSON.stringify(stopped.get(bare))}`);
      const error = await this.rejection(barePromise);
      if (!inline && error?.reason !== unclonable) throw new Error('The host lost the reason');
      this.assertAccounted(scheduler, 2, kind);
    }
  }

  // A worker that does not look at its signal keeps the task cancelling, with
  // its resources held, until run() returns. On a thread this is also the
  // protocol check: a 'cancel' answered as an invalid action would reject the
  // task's promise and settle it at once
  private async testIgnored(): Promise<void> {
    for (const inline of [true, false]) {
      const kind = inline ? 'inline' : 'threaded';
      const group = new ConcurrentLimitGroup(1);
      const { scheduler } = this.make({ inline, groups: ['g'] }, { g: group }, this.cancelWorkerUrl);
      const { log: progress, at } = this.progress(scheduler);
      const cancelledAt = this.cancelledAt(scheduler);
      const id = `deaf-${kind}`;
      const task = this.task(id, { mode: 'ignore', delay: 600 });
      scheduler.addTask(task);
      await this.until(() => progress.includes(`${id}:entered`));

      // Inline: even a grace period of 0 cannot force it. Threaded: never forced
      const cancelled = scheduler.cancel(id, { graceMs: inline ? 0 : Infinity });
      await sleep(300);
      if (task.state !== 'running' || !task.cancellation) throw new Error(`${kind}: ${task.state} while its run was still going`);
      if (group.running !== 1) throw new Error(`${kind}: resources released while its run was still going`);
      if (await this.within(cancelled, 3000, `${kind} cancel()`) !== true) throw new Error(`${kind}: cancel() resolved false`);
      const ranFor = cancelledAt.get(id)! - at.get(`${id}:entered`)!;
      if (ranFor < 550) throw new Error(`${kind}: settled ${ranFor.toFixed(0)} ms after entering, before run() returned`);
      const ended = (): string => task.state;
      const held = (): number => group.running;
      if (ended() !== 'cancelled' || held() !== 0) throw new Error(`${kind}: ended ${ended()}, holding ${held()}`);
      this.assertAccounted(scheduler, 1, kind);
    }
  }

  // A co-tenant fails the worker while the task is cancelling. It settles as
  // cancelled - never requeued, never failed - and only once its run has
  // stopped: on a thread, when the thread is dead. Before the release, its
  // promise was dropped with the thread and it never settled at all.
  private async testFailureThreaded(): Promise<void> {
    for (const requeueFailedTasks of [true, false]) {
      const group = new ConcurrentLimitGroup(2);
      const { pool, scheduler } = this.make(
        { inline: false, maxConcurrentTasks: 2, requeueFailedTasks, groups: ['g'] }, { g: group }, this.cancelWorkerUrl
      );
      const log = this.events(scheduler);
      const { log: progress } = this.progress(scheduler);
      const cancelledAt = this.cancelledAt(scheduler);
      let teardownAt = 0;
      for (const ev of ['worker.teardown.completed', 'worker.teardown.failed']) {
        pool.addEventListener(ev, () => { teardownAt ||= performance.now(); });
      }

      const victim = this.task('victim', { mode: 'ignore', delay: 4000 });
      scheduler.addTask(victim);
      await this.until(() => progress.includes('victim:entered'));
      scheduler.addTask(this.task('cotenant', { mode: 'terminate', delay: 150 }, { optional: true }));
      const cancelled = scheduler.cancel('victim', { graceMs: Infinity });

      if (await this.within(cancelled, 3000, `requeue ${requeueFailedTasks}: cancel()`) !== true) throw new Error('cancel() resolved false');
      if (victim.state !== 'cancelled') throw new Error(`Ended ${victim.state}`);
      if (!teardownAt || cancelledAt.get('victim')! < teardownAt) throw new Error('Settled before its thread was torn down');
      if (log.filter(e => e.endsWith(':victim')).some(e => !/^(running|cancelling|cancelled):/.test(e))) throw new Error(`Events: ${log}`);
      await this.until(() => scheduler.stats.running === 0);
      if (group.running !== 0) throw new Error(`Group holds ${group.running}`);
      this.assertAccounted(scheduler, 2, `requeue ${requeueFailedTasks}`);
    }
  }

  // The inline case: the worker fails at once, but the cancelling run goes on,
  // and the task must not settle - or give back its resources - until it ends
  private async testFailureInline(): Promise<void> {
    for (const requeueFailedTasks of [true, false]) {
      const group = new ConcurrentLimitGroup(2);
      const { scheduler } = this.make({ maxConcurrentTasks: 2, requeueFailedTasks, groups: ['g'] }, { g: group }, this.cancelWorkerUrl);
      const log = this.events(scheduler);
      const { log: progress, at } = this.progress(scheduler);
      const cancelledAt = this.cancelledAt(scheduler);

      const victim = this.task('victim', { mode: 'ignore', delay: 500 });
      scheduler.addTask(victim);
      await this.until(() => progress.includes('victim:entered'));
      scheduler.addTask(this.task('cotenant', { mode: 'terminate', delay: 50 }, { optional: true }));
      const cancelled = scheduler.cancel('victim');

      await sleep(250);
      if (victim.state !== 'running') throw new Error(`requeue ${requeueFailedTasks}: ${victim.state} while its run was still going`);
      if (group.running < 1) throw new Error(`requeue ${requeueFailedTasks}: its resources were released while its run was still going`);
      if (await this.within(cancelled, 3000, 'cancel()') !== true) throw new Error('cancel() resolved false');
      const ranFor = cancelledAt.get('victim')! - at.get('victim:entered')!;
      if (ranFor < 450) throw new Error(`requeue ${requeueFailedTasks}: settled ${ranFor.toFixed(0)} ms after entering`);
      if (log.filter(e => e.endsWith(':victim')).some(e => !/^(running|cancelling|cancelled):/.test(e))) throw new Error(`Events: ${log}`);
      await this.until(() => scheduler.stats.running === 0);
      this.assertAccounted(scheduler, 2, `requeue ${requeueFailedTasks}`);
    }
  }

  // Attempt 1 is requeued off a failed worker and keeps running there; attempt
  // 2 is cancelled on a new worker. Attempt 1 ends first, and its outcome is
  // stale: the token check must come before the cancel marker, or that
  // outcome settles attempt 2 while it still runs
  private async testCheckOrder(): Promise<void> {
    const { scheduler } = this.make({ maxThreads: 2, maxConcurrentTasks: 2 }, {}, this.cancelWorkerUrl);
    const entered: number[] = [];
    scheduler.addEventListener('task.progress', (e: any) => {
      if (e.detail.id === 'twice' && e.detail.message === 'entered') entered.push(performance.now());
    });
    const cancelledAt = this.cancelledAt(scheduler);

    const task = this.task('twice', { mode: 'ignore', delay: 400 });
    scheduler.addTask(task);
    await this.until(() => entered.length === 1);
    // Fails the first worker 150 ms in: 'twice' is requeued onto a second one
    scheduler.addTask(this.task('breaker', { mode: 'terminate', delay: 150 }, { optional: true }));
    await this.until(() => entered.length === 2);
    const cancelled = scheduler.cancel('twice');

    if (await this.within(cancelled, 3000, 'cancel()') !== true) throw new Error('cancel() resolved false');
    const ranFor = cancelledAt.get('twice')! - entered[1];
    if (ranFor < 350) throw new Error(`Settled ${ranFor.toFixed(0)} ms into attempt 2 - by attempt 1's outcome`);
    this.assertAccounted(scheduler, 2, 'end');
  }

  // _dispatchTask's capacity-race branch puts a task back in the ready queue.
  // Being cancelled, it ends there instead. A real race cannot reach it any
  // more, so the pool's enqueue is replaced for one call
  private async testCapacityRace(): Promise<void> {
    const group = new ConcurrentLimitGroup(1);
    const { pool, scheduler } = this.make({ groups: ['g'] }, { g: group });
    let acquired = 0;
    scheduler.addEventListener('resource.acquired', () => acquired++);
    const real = pool.enqueueNoQueue.bind(pool);
    let raced = false;
    let redispatched = 0;
    pool.enqueueNoQueue = (taskId: string, payload: any) => {
      if (raced) {
        redispatched++;
        return real(taskId, payload);
      }
      raced = true;
      return new Promise((_, reject) => setTimeout(() => reject(new Error('Worker at maximum concurrent task capacity')), 30));
    };
    const task = this.task('racer', { delay: 1 });
    const promise = scheduler.addTask(task, { createPromise: true });
    if (!raced || task.state !== 'running') throw new Error('Precondition: not in the raced enqueue');
    const cancelled = scheduler.cancel('racer');
    if (await this.within(cancelled, 3000, 'cancel()') !== true) throw new Error('cancel() resolved false');
    if (!(await this.rejection(promise) instanceof CancelledError)) throw new Error('Promise did not reject with CancelledError');
    await sleep(50);
    const state = (): string => task.state;
    if (state() !== 'cancelled') throw new Error(`Ended ${state()}`);
    // Cancel wins, or the marker check at dispatch, would still end it
    // cancelled after going round again - the point is that it does not
    if (redispatched !== 0 || acquired !== 1) throw new Error(`Dispatched again after the cancel: ${acquired} acquisitions`);
    if (group.running !== 0) throw new Error(`Group holds ${group.running}`);
    this.assertAccounted(scheduler, 1, 'end');
  }

  // An inline run cannot be stopped. At shutdown its cancel() resolves false,
  // and when the run ends later, nothing is emitted
  private async testShutdownInline(): Promise<void> {
    const { scheduler } = this.make({}, {}, this.cancelWorkerUrl);
    const log = this.events(scheduler);
    const { log: progress } = this.progress(scheduler);
    const task = this.task('stubborn', { mode: 'ignore', delay: 6000 });
    scheduler.addTask(task);
    // Inside run(): cancelled any earlier, it would never start, and settle
    await this.until(() => progress.includes('stubborn:entered'));
    const cancelled = scheduler.cancel('stubborn');
    // shutdown() waits up to 5 s for running tasks; this one outlasts it
    await scheduler.shutdown();
    const outcome = await Promise.race([cancelled, sleep(500).then(() => 'NEVER SETTLED')]);
    if (outcome !== false) throw new Error(`cancel() resolved ${outcome}`);
    const before = log.length;
    await sleep(1200);
    if (log.length !== before) throw new Error(`Emitted after shutdown: ${log.slice(before)}`);
  }

  // --- the forced stop ----------------------------------------------------------

  // A worker spinning synchronously never reads its cancel message. After the
  // grace period its worker is retired: the task ends cancelled, forced, with
  // its resources released only once the thread is gone. Its co-tenant is
  // requeued - with requeueFailedTasks false, too - and completes on a new
  // worker. Not a failure: no worker.failed, and no restart counted.
  private async testForcedStop(): Promise<void> {
    const group = new ConcurrentLimitGroup(2);
    const { pool, scheduler } = this.make(
      { inline: false, maxConcurrentTasks: 2, requeueFailedTasks: false, groups: ['g'] }, { g: group }, this.cancelWorkerUrl
    );
    const log = this.events(scheduler);
    const entered = this.entries(scheduler);
    const cancelledAt = this.cancelledAt(scheduler);
    const teardown = this.teardownOf(pool);
    let workerFailed = 0;
    pool.addEventListener('worker.failed', () => workerFailed++);
    let releasedAt = 0;
    scheduler.addEventListener('resource.released', (e: any) => {
      if (e.detail.holderId === 'spinner') releasedAt = performance.now();
    });

    // The co-tenant first: once the spinner spins, nothing else on that thread runs
    const cotenant = this.task('cotenant', { mode: 'ignore', delay: 300 });
    const cotenantDone = scheduler.addTask(cotenant, { createPromise: true });
    await this.until(() => entered.has('cotenant'));
    const firstWorker = pool.getWorkerIds()[0];
    const spinner = this.task('spinner', { mode: 'spin', delay: 8000 });
    scheduler.addTask(spinner);
    await this.until(() => entered.has('spinner'));

    const start = performance.now();
    const cancelled = scheduler.cancel('spinner', { graceMs: 100 });
    if (await this.within(cancelled, 4000, 'cancel()') !== true) throw new Error('cancel() resolved false');
    const took = performance.now() - start;
    if (took < 100) throw new Error(`Settled ${took.toFixed(0)} ms after the cancel, inside the grace period`);
    if (spinner.state !== 'cancelled' || spinner.cancellation?.forced !== true) {
      throw new Error(`Ended ${spinner.state}, forced ${spinner.cancellation?.forced}`);
    }
    if (!teardown.endedAt || cancelledAt.get('spinner')! < teardown.endedAt) throw new Error('Settled before its thread was torn down');
    if (!releasedAt || releasedAt < teardown.endedAt) throw new Error('Released its resources before its thread was torn down');
    const spinnerEvents = log.filter(e => e.endsWith(':spinner')).join(' ');
    if (spinnerEvents !== 'running:spinner cancelling:spinner cancelled:spinner') throw new Error(spinnerEvents);

    await this.within(cotenantDone!, 4000, 'the co-tenant');
    const ended = (): string => cotenant.state;
    if (ended() !== 'done') throw new Error(`The co-tenant ended ${ended()}`);
    if (entered.get('cotenant')!.length !== 2) throw new Error(`The co-tenant ran ${entered.get('cotenant')!.length} times`);
    if (log.some(e => e.startsWith('failed:'))) throw new Error(`Events: ${log}`);
    const workers = pool.getWorkerIds();
    if (workers.length !== 1 || workers[0] === firstWorker) throw new Error(`Workers: ${workers}, first ${firstWorker}`);
    if (workerFailed !== 0) throw new Error(`worker.failed fired ${workerFailed} times`);
    if ((pool as any).poolRestartCount !== 0) throw new Error(`Restart count ${(pool as any).poolRestartCount}`);
    if (group.running !== 0) throw new Error(`Group holds ${group.running}`);
    this.assertAccounted(scheduler, 2, 'end');
  }

  // The worker ignores its signal, and both runs on it post their results
  // while its teardown() is still running. Detached first, neither settles
  // anything: the cancelled task ends forced once the thread is dead, and the
  // co-tenant completes from its second run
  private async testForcedLateResult(): Promise<void> {
    const { pool, scheduler } = this.make(
      { inline: false, maxConcurrentTasks: 2, config: { teardownMs: 400 } }, {}, this.cancelWorkerUrl
    );
    const entered = this.entries(scheduler);
    const cancelledAt = this.cancelledAt(scheduler);
    const teardown = this.teardownOf(pool);
    let completedAt = 0;
    scheduler.addEventListener('task.completed', (e: any) => {
      if (e.detail.id === 'cotenant') completedAt = performance.now();
    });

    const cotenant = this.task('cotenant', { mode: 'ignore', delay: 250 });
    const cotenantDone = scheduler.addTask(cotenant, { createPromise: true });
    const late = this.task('late', { mode: 'ignore', delay: 200 });
    scheduler.addTask(late);
    await this.until(() => entered.has('cotenant') && entered.has('late'));

    const cancelled = scheduler.cancel('late', { graceMs: 50 });
    if (await this.within(cancelled, 4000, 'cancel()') !== true) throw new Error('cancel() resolved false');
    if (late.state !== 'cancelled' || late.cancellation?.forced !== true) {
      throw new Error(`Ended ${late.state}, forced ${late.cancellation?.forced} - settled by its own result`);
    }
    if (!teardown.endedAt || cancelledAt.get('late')! < teardown.endedAt) throw new Error('Settled before its thread was torn down');

    await this.within(cotenantDone!, 4000, 'the co-tenant');
    const runs = entered.get('cotenant')!;
    if (runs.length !== 2) throw new Error(`The co-tenant ran ${runs.length} times - completed by its abandoned run`);
    if (completedAt - runs[1] < 200) throw new Error(`Completed ${(completedAt - runs[1]).toFixed(0)} ms into its second run`);
    this.assertAccounted(scheduler, 2, 'end');
  }

  // Two tasks on one worker, both being cancelled. Neither is a co-tenant to
  // requeue: both settle cancelled, forced, once the thread is dead
  private async testForcedTwo(): Promise<void> {
    const { pool, scheduler } = this.make({ inline: false, maxConcurrentTasks: 2 }, {}, this.cancelWorkerUrl);
    const log = this.events(scheduler);
    const entered = this.entries(scheduler);
    const cancelledAt = this.cancelledAt(scheduler);
    const teardown = this.teardownOf(pool);
    const tasks = ['a', 'b'].map(id => this.task(id, { mode: 'ignore', delay: 4000 }));
    for (const task of tasks) scheduler.addTask(task);
    await this.until(() => entered.has('a') && entered.has('b'));

    // b with the longer grace period: the forced stop of a takes it too
    const results = await this.within(Promise.all([
      scheduler.cancel('a', { graceMs: 100 }),
      scheduler.cancel('b', { graceMs: 60_000 })
    ]), 4000, 'cancel()');
    if (results.some(r => r !== true)) throw new Error(`cancel() resolved ${results}`);
    for (const task of tasks) {
      if (task.state !== 'cancelled' || task.cancellation?.forced !== true) {
        throw new Error(`${task.id} ended ${task.state}, forced ${task.cancellation?.forced}`);
      }
      if (!teardown.endedAt || cancelledAt.get(task.id)! < teardown.endedAt) throw new Error(`${task.id} settled before its thread was torn down`);
      const events = log.filter(e => e.endsWith(`:${task.id}`)).join(' ');
      if (events !== `running:${task.id} cancelling:${task.id} cancelled:${task.id}`) throw new Error(events);
    }
    if ((pool as any).poolRestartCount !== 0) throw new Error(`Restart count ${(pool as any).poolRestartCount}`);
    this.assertAccounted(scheduler, 2, 'end');
  }

  // A timer belongs to its attempt. The first task stops by itself inside its
  // grace period; a new task reusing its id then runs on the same worker past
  // the moment that timer was due, and is not forced
  private async testStaleGraceTimer(): Promise<void> {
    const { pool, scheduler } = this.make({ inline: false }, {}, this.cancelWorkerUrl);
    const entered = this.entries(scheduler);
    const first = this.task('reused', { mode: 'ignore', delay: 100 });
    scheduler.addTask(first);
    await this.until(() => entered.has('reused'));
    const worker = pool.getWorkerIds()[0];

    if (await this.within(scheduler.cancel('reused', { graceMs: 400 }), 3000, 'cancel()') !== true) throw new Error('cancel() resolved false');
    if (first.cancellation?.forced !== false) throw new Error('The first task was forced');

    const second = this.task('reused', { mode: 'ignore', delay: 700 });
    const done = scheduler.addTask(second, { createPromise: true });
    await this.within(done!, 3000, 'the second task');
    const ended = (): string => second.state;
    if (ended() !== 'done') throw new Error(`The second task ended ${ended()}`);
    if (pool.getWorkerIds().join() !== worker) throw new Error(`Its worker was replaced: ${pool.getWorkerIds()}`);
    this.assertAccounted(scheduler, 2, 'end');
  }

  // A forced stop during shutdown()'s drain: the cancelled task and the
  // requeued co-tenant both settle inside it. Then a pool shut down while it is
  // retiring a worker waits for that, rather than tearing it down twice.
  private async testForcedDuringShutdown(): Promise<void> {
    {
      const { scheduler } = this.make({ inline: false, maxConcurrentTasks: 2 }, {}, this.cancelWorkerUrl);
      const entered = this.entries(scheduler);
      const cotenant = this.task('cotenant', { mode: 'ignore', delay: 300 });
      const cotenantDone = scheduler.addTask(cotenant, { createPromise: true });
      await this.until(() => entered.has('cotenant'));
      const spinner = this.task('spinner', { mode: 'spin', delay: 8000 });
      scheduler.addTask(spinner);
      await this.until(() => entered.has('spinner'));

      const cancelled = scheduler.cancel('spinner', { graceMs: 100 });
      const start = performance.now();
      await scheduler.shutdown();
      const took = performance.now() - start;
      // The drain gives up after 5 s; settling inside it is the point
      if (took > 4000) throw new Error(`shutdown() took ${took.toFixed(0)} ms`);
      if (await this.within(cancelled, 500, 'cancel()') !== true) throw new Error('cancel() resolved false');
      if (spinner.state !== 'cancelled' || spinner.cancellation?.forced !== true) throw new Error(`Spinner ended ${spinner.state}`);
      await this.within(cotenantDone!, 500, 'the co-tenant');
      if (entered.get('cotenant')!.length !== 2) throw new Error('The co-tenant was not requeued');
    }
    {
      const { pool, scheduler } = this.make({ inline: false }, {}, this.cancelWorkerUrl);
      const entered = this.entries(scheduler);
      const teardown = this.teardownOf(pool);
      const spinner = this.task('spinner', { mode: 'spin', delay: 8000 });
      scheduler.addTask(spinner);
      await this.until(() => entered.has('spinner'));
      const cancelled = scheduler.cancel('spinner', { graceMs: 50 });
      // Retiring: its teardown has begun, and waits out the acknowledgement
      // timeout, since the thread cannot answer
      await this.until(() => teardown.started === 1);
      await pool.shutdown();
      if (teardown.started !== 1) throw new Error(`Torn down ${teardown.started} times`);
      if (await this.within(cancelled, 2000, 'cancel()') !== true) throw new Error('cancel() resolved false');
      if (spinner.cancellation?.forced !== true) throw new Error('Not forced');
    }
  }

  // Never forced, the task is still cancelling when shutdown() terminates its
  // worker. Its run has stopped then, so it ends cancelled, forced
  private async testShutdownThreaded(): Promise<void> {
    const { scheduler } = this.make({ inline: false }, {}, this.cancelWorkerUrl);
    const entered = this.entries(scheduler);
    const task = this.task('stubborn', { mode: 'ignore', delay: 8000 });
    scheduler.addTask(task);
    await this.until(() => entered.has('stubborn'));
    const cancelled = scheduler.cancel('stubborn', { graceMs: Infinity });
    await scheduler.shutdown();
    if (await this.within(cancelled, 500, 'cancel()') !== true) throw new Error('cancel() resolved false');
    if (task.state !== 'cancelled' || task.cancellation?.forced !== true) throw new Error(`Ended ${task.state}, forced ${task.cancellation?.forced}`);
  }

  private async testGraceEdges(): Promise<void> {
    // Beyond what setTimeout honours: never forced. Armed anyway, the timer
    // would fire at once
    for (const graceMs of [Infinity, 2 ** 31]) {
      const { scheduler } = this.make({ inline: false }, {}, this.cancelWorkerUrl);
      const entered = this.entries(scheduler);
      const cancelledAt = this.cancelledAt(scheduler);
      const task = this.task('patient', { mode: 'ignore', delay: 400 });
      scheduler.addTask(task);
      await this.until(() => entered.has('patient'));
      await this.within(scheduler.cancel('patient', { graceMs }), 3000, `graceMs ${graceMs}`);
      if (task.cancellation?.forced !== false) throw new Error(`graceMs ${graceMs}: forced`);
      const ranFor = cancelledAt.get('patient')! - entered.get('patient')![0];
      if (ranFor < 350) throw new Error(`graceMs ${graceMs}: settled ${ranFor.toFixed(0)} ms in`);
    }

    // A repeat call shortens the wait, and never lengthens it
    for (const [first, second] of [[60_000, 100], [100, 60_000]]) {
      const { scheduler } = this.make({ inline: false }, {}, this.cancelWorkerUrl);
      const entered = this.entries(scheduler);
      const task = this.task('pressed', { mode: 'ignore', delay: 8000 });
      scheduler.addTask(task);
      await this.until(() => entered.has('pressed'));
      const once = scheduler.cancel('pressed', { graceMs: first });
      await sleep(30);
      const twice = scheduler.cancel('pressed', { graceMs: second });
      if (once !== twice) throw new Error('A repeat call returned a different promise');
      await this.within(twice, 2000, `${first} then ${second}`);
      if (task.cancellation?.forced !== true) throw new Error(`${first} then ${second}: not forced`);
    }

    // The pool's cancelGraceMs, when a call gives none
    {
      const { scheduler } = this.make({ inline: false, cancelGraceMs: 100 }, {}, this.cancelWorkerUrl);
      const entered = this.entries(scheduler);
      const task = this.task('pooled', { mode: 'ignore', delay: 8000 });
      scheduler.addTask(task);
      await this.until(() => entered.has('pooled'));
      await this.within(scheduler.cancel('pooled'), 2000, 'cancelGraceMs');
      if (task.cancellation?.forced !== true) throw new Error('cancelGraceMs: not forced');
    }

    // pool.shutdown() leaves no timer behind to hold the process open
    {
      const { pool, scheduler } = this.make({ inline: false }, {}, this.cancelWorkerUrl);
      const entered = this.entries(scheduler);
      scheduler.addTask(this.task('left', { mode: 'ignore', delay: 8000 }));
      await this.until(() => entered.has('left'));
      scheduler.cancel('left', { graceMs: 60_000 });
      await pool.shutdown();
      if ((pool as any).graceTimers.size !== 0) throw new Error('A grace timer outlived pool.shutdown()');
    }

    // Validated at construction, by the same rules as graceMs
    if (new WorkerManager(this.cancelWorkerUrl).cancelGraceMs !== 2000) throw new Error('Default is not 2000');
    for (const [bad, kind] of [['5', TypeError], [-1, RangeError], [NaN, RangeError]] as const) {
      let thrown: unknown;
      try { new WorkerManager(this.cancelWorkerUrl, { cancelGraceMs: bad as any }); } catch (error) { thrown = error; }
      if (!(thrown instanceof kind)) throw new Error(`cancelGraceMs ${String(bad)}: threw ${thrown}`);
    }
  }

  // --- at scale -----------------------------------------------------------------

  /**
   * Cancels every task of `tasks`, through the real cancel() with a
   * task.cancelled listener attached, and returns the ms it took.
   */
  private cancelAll(scheduler: FyflowScheduler, ids: string[], label: string): number {
    let seen = 0;
    scheduler.addEventListener('task.cancelled', () => { seen++; });
    const start = performance.now();
    for (const id of ids) scheduler.cancel(id);
    const took = performance.now() - start;
    if (seen !== ids.length) throw new Error(`${label}: ${seen} of ${ids.length} task.cancelled`);
    return took;
  }

  // Cancelling every task of a large queue is linear. A cancel tombstones the
  // task where it sits, in O(1), and checks only whether the scheduler has
  // drained - a splice per cancel, or the full completion check, makes these
  // seconds rather than milliseconds.
  private async testAtScale(): Promise<void> {
    const N = 50_000;
    const timings: string[] = [];
    const bound = (label: string, took: number, ms: number) => {
      timings.push(`${label} ${took.toFixed(0)} ms`);
      if (took > ms) throw new Error(`${label}: ${took.toFixed(0)} ms to cancel ${N} tasks`);
    };

    // A ready queue behind one running task, cancelled in creation order and in
    // reverse
    for (const reverse of [false, true]) {
      const label = reverse ? 'ready, reversed' : 'ready';
      const { scheduler } = this.make({}, {}, this.cancelWorkerUrl);
      scheduler.addTask(this.task('front', { delay: 60_000 }));
      const tasks = Array.from({ length: N }, (_, i) => this.task(`r${i}`));
      scheduler.addTasks(tasks);
      if (scheduler.stats.queued !== N) throw new Error(`${label}: ${scheduler.stats.queued} queued`);
      const ids = tasks.map(t => t.id);
      if (reverse) ids.reverse();
      bound(label, this.cancelAll(scheduler, ids, label), SCALE_BOUND_MS);
      this.assertAccounted(scheduler, N + 1, label);
      await scheduler.cancel('front');
    }

    // A blocked queue, with nothing running: the group is held from outside
    {
      const group = new ConcurrentLimitGroup(1);
      const { scheduler } = this.make({ groups: ['g'] }, { g: group }, this.cancelWorkerUrl);
      group.onStart();
      const tasks = Array.from({ length: N }, (_, i) => this.task(`b${i}`));
      scheduler.addTasks(tasks);
      // A pass blocks one task and stops, as a blocked head gates the rest
      for (let i = 0; i < N && scheduler.stats.queued > 0; i++) (scheduler as any)._dispatchLoop();
      const blocked = scheduler.getAdmissionQueue('g').g?.length ?? 0;
      if (blocked !== N) throw new Error(`blocked: ${blocked} blocked`);
      bound('blocked', this.cancelAll(scheduler, tasks.map(t => t.id), 'blocked'), SCALE_BOUND_MS);
      this.assertAccounted(scheduler, N, 'blocked');
      group.onFinish();
    }

    // A keyed group with many keys, each saturated from outside, nothing
    // running; tasks made key by key, so cancelling in order empties the
    // blocked queues one after another
    {
      const KEYS = 5_000;
      const group = new KeyedRateLimitGroup([{ limit: 1, windowMs: 60_000 }], { id: 'k', keyFrom: (t: any) => t.payload.key });
      const { scheduler } = this.make({ groups: ['k'] }, { k: group }, this.cancelWorkerUrl);
      for (let k = 0; k < KEYS; k++) group.onStart(`key${k}`);
      const tasks = Array.from({ length: N }, (_, i) => this.task(`k${i}`, { key: `key${Math.floor(i / (N / KEYS))}` }));
      scheduler.addTasks(tasks);
      for (let i = 0; i < N && scheduler.stats.queued > 0; i++) (scheduler as any)._dispatchLoop();
      const blocked = Object.values(scheduler.getAdmissionQueue('k')).reduce((n, q) => n + q.length, 0);
      if (blocked !== N) throw new Error(`keyed: ${blocked} blocked`);
      bound('keyed', this.cancelAll(scheduler, tasks.map(t => t.id), 'keyed'), SCALE_BOUND_MS);
      this.assertAccounted(scheduler, N, 'keyed');
    }
    console.log(`   ${timings.join(', ')}`);
  }
}

// Auto-run tests when executed directly - handle both Deno and Node.js
if ((typeof Deno !== 'undefined' && import.meta.main) ||
    (typeof process !== 'undefined' && process.argv[1] && import.meta.url &&
     import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/')))) {
  const testSuite = new CancellationTestSuite();
  try {
    await testSuite.runAllTests(true);
  } finally {
    await testSuite.cleanup();
  }
}

export { CancellationTestSuite };
export default CancellationTestSuite;
