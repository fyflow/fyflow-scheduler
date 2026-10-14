// Task settlement tests.
//
// A task can reach the scheduler's settle logic from two directions: the pool's
// `task.failed` event, and the rejection of the pool's task promise. Both used
// to run a full settle, so an ordinary failure emitted task.failed twice,
// counted stats.failed twice, raced the terminal state between 'user_action' and
// 'failed', and - when only the event path ran - never released the task's
// resource group slots.
//
// Neither path can simply be removed: each is the ONLY settle path in some
// scenario, which these tests pin down.

import { WorkerManager, FyflowScheduler, FyflowTask, ConcurrentLimitGroup, WorkerTerminationError } from '../../index.ts';
import { runTest, type TestResult } from '../harness.ts';

// Node.js process declaration for cross-platform compatibility
declare const process: any;

interface TestSuiteResult {
  platform: string; totalTests: number; passed: number; failed: number;
  duration: number; results: TestResult[];
}

class SettlementTestSuite {
  private results: TestResult[] = [];
  private startTime = 0;
  private crashWorkerUrl = "";
  private selfTerminatingWorkerUrl = "";
  private requeueWorkerUrl = "";
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

  private async runTest(name: string, testFn: () => Promise<void>): Promise<TestResult> {
    return runTest(name, testFn);
  }

  async runAllTests(exitOnComplete = true): Promise<TestSuiteResult> {
    if (typeof Deno !== "undefined") {
      this.crashWorkerUrl = new URL("../workers/crashingWorker.ts", import.meta.url).href;
      this.selfTerminatingWorkerUrl = new URL("../workers/selfTerminatingWorker.ts", import.meta.url).href;
      this.requeueWorkerUrl = new URL("../workers/requeueWorker.ts", import.meta.url).href;
    } else {
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      this.crashWorkerUrl = new URL((await import('../workers/crashingWorker.ts?worker-direct')).default).href;
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      this.selfTerminatingWorkerUrl = new URL((await import('../workers/selfTerminatingWorker.ts?worker-direct')).default).href;
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      this.requeueWorkerUrl = new URL((await import('../workers/requeueWorker.ts?worker-direct')).default).href;
    }

    const platform = typeof globalThis !== 'undefined' && 'Deno' in globalThis ? 'Deno' : 'Node.js';
    console.log(`🚀 FyFlow Task Settlement Tests - ${platform}`);
    console.log('='.repeat(60));

    this.startTime = performance.now();
    this.results = [];

    this.results.push(await this.runTest('Failure Emits task.failed Exactly Once', () => this.testFailedEventOnce()));
    this.results.push(await this.runTest('Failure Counts stats.failed Exactly Once', () => this.testFailedCountedOnce()));
    this.results.push(await this.runTest('Failure Ends In user_action', () => this.testTerminalStateIsUserAction()));
    this.results.push(await this.runTest('Terminal State Is Stable', () => this.testTerminalStateStable()));
    this.results.push(await this.runTest('Optional Failure Resolves Null Once', () => this.testOptionalFailure()));
    this.results.push(await this.runTest('Retries Do Not Signal user_action Early', () => this.testRetriesNoEarlyUserAction()));
    this.results.push(await this.runTest('Retries Count One Failure', () => this.testRetriesCountOnce()));
    this.results.push(await this.runTest('Retry Then Success Counts As Done', () => this.testRetryThenSuccess()));
    this.results.push(await this.runTest('attemptsSpent Continues The Budget', () => this.testAttemptsSpentContinuesTheBudget()));
    this.results.push(await this.runTest('A Spent Budget Runs Once', () => this.testSpentBudgetRunsOnce()));
    this.results.push(await this.runTest('attemptsSpent Is Validated', () => this.testAttemptsSpentValidated()));
    this.results.push(await this.runTest('task.retrying Fires Once Per Retry', () => this.testRetryingEvent()));
    this.results.push(await this.runTest('A Retry That Succeeds Clears error', () => this.testRetrySuccessClearsError()));
    this.results.push(await this.runTest('Backoff Holds Under Load', () => this.testBackoffHoldsUnderLoad()));
    this.results.push(await this.runTest('No scheduler.completed During A Backoff', () => this.testNoCompletionDuringBackoff()));
    this.results.push(await this.runTest('Shutdown During A Backoff', () => this.testShutdownDuringBackoff()));
    this.results.push(await this.runTest('Success Settles Exactly Once', () => this.testSuccessOnce()));
    this.results.push(await this.runTest('Resources Released On Ordinary Failure', () => this.testResourcesReleasedOnFailure()));
    this.results.push(await this.runTest('Resources Released When Worker Dies', () => this.testResourcesReleasedOnWorkerDeath()));
    this.results.push(await this.runTest('Worker Death Settles The Task', () => this.testWorkerDeathSettles()));
    this.results.push(await this.runTest('Unconstructable Worker Follows The Requeue Contract', () => this.testUnconstructableWorkerSettles()));
    this.results.push(await this.runTest('Counters Stay Non-Negative', () => this.testCountersNonNegative()));
    this.results.push(await this.runTest('Unstartable Pool Raises The Alarm', () => this.testUnstartablePoolAlarms()));
    this.results.push(await this.runTest('Stats Account For Each Task Once', () => this.testStatsAccountEachTaskOnce()));
    this.results.push(await this.runTest('Requeue Does Not Also Count A Failure', () => this.testRequeueNotAlsoFailed()));
    this.results.push(await this.runTest('Self-Terminating Task Fails, Not Requeued', () => this.testSelfTerminatingTaskFails()));
    this.results.push(await this.runTest('One Worker Failure Is Handled Once', () => this.testOneFailureHandledOnce()));
    this.results.push(await this.runTest('terminateWithError From A Task Fails That Task Only', () => this.testTaskContextTermination()));
    this.results.push(await this.runTest('terminateWithError From A Timer Does Not Crash', () => this.testTaskContextTerminationFromTimer()));
    this.results.push(await this.runTest('terminateWithError From The Worker Requeues Every Task', () => this.testWorkerContextTermination()));
    this.results.push(await this.runTest('Requeue Ignores The Abandoned Run\'s Result', () => this.testRequeueAbandonedResultIgnored()));
    this.results.push(await this.runTest('Requeue Ignores The Abandoned Run\'s Failure', () => this.testRequeueAbandonedFailureIgnored()));
    this.results.push(await this.runTest('Requeue Drops The Abandoned Run\'s Progress And Spawns', () => this.testRequeueAbandonedSideEffectsDropped()));

    const totalDuration = performance.now() - this.startTime;
    this.results = this.results.filter(r => !r.skipped);
    const passed = this.results.filter(r => r.passed).length;
    const failed = this.results.length - passed;

    console.log(`\n📊 Settlement Test Summary - ${platform}`);
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
      console.log('\n🎉 All settlement tests passed!');
    }

    return { platform, totalTests: this.results.length, passed, failed, duration: totalDuration, results: this.results };
  }

  // --- helpers ---------------------------------------------------------------

  private crashingScheduler(groups: Record<string, any> = {}, poolOptions: any = {}) {
    const pool = new WorkerManager(this.crashWorkerUrl, {
      maxThreads: 1, maxConcurrentTasks: 1, inline: true, ...poolOptions
    });
    const scheduler = this.track(new FyflowScheduler({ CrashingWorker: pool }, groups));
    return { pool, scheduler };
  }

  private dyingScheduler(groups: Record<string, any> = {}, poolOptions: any = {}) {
    const pool = new WorkerManager(this.selfTerminatingWorkerUrl, {
      maxThreads: 1, maxConcurrentTasks: 1, inline: false,
      maxWorkerRestarts: 0, ...poolOptions
    });
    const scheduler = this.track(new FyflowScheduler({ SelfTerminatingWorker: pool }, groups));
    return { pool, scheduler };
  }

  private counted(scheduler: FyflowScheduler) {
    const counts: Record<string, number> = {};
    for (const ev of ['task.failed', 'task.user_action', 'task.completed', 'task.running']) {
      scheduler.addEventListener(ev, () => { counts[ev] = (counts[ev] ?? 0) + 1; });
    }
    return counts;
  }

  /** Await a task promise but never hang the suite on it. */
  private async settle(promise: Promise<any> | void, ms = 2500): Promise<string> {
    return await Promise.race([
      (promise as Promise<any>).then(v => `resolved:${JSON.stringify(v ?? null)}`).catch(() => 'rejected'),
      new Promise<string>(r => setTimeout(() => r('NEVER SETTLED'), ms))
    ]);
  }

  private failing(id: string, extra: any = {}) {
    return new FyflowTask({ id, workerType: 'CrashingWorker', payload: { action: 'throw-error' }, ...extra });
  }

  // --- tests -----------------------------------------------------------------

  private async testFailedEventOnce(): Promise<void> {
    const { scheduler } = this.crashingScheduler();
    const counts = this.counted(scheduler);

    await this.settle(scheduler.addTask(this.failing('once'), { createPromise: true }));
    await new Promise(r => setTimeout(r, 300)); // let any second settle land

    if (counts['task.failed'] !== 1) {
      throw new Error(`Expected 1 task.failed event, got ${counts['task.failed'] ?? 0}`);
    }
  }

  private async testFailedCountedOnce(): Promise<void> {
    const { scheduler } = this.crashingScheduler();
    await this.settle(scheduler.addTask(this.failing('counted'), { createPromise: true }));
    await new Promise(r => setTimeout(r, 300));

    if (scheduler.stats.failed !== 1) {
      throw new Error(`Expected stats.failed 1 for one task, got ${scheduler.stats.failed}`);
    }
    if (scheduler.stats.done !== 0) throw new Error(`Expected done 0, got ${scheduler.stats.done}`);
  }

  private async testTerminalStateIsUserAction(): Promise<void> {
    const { scheduler } = this.crashingScheduler();
    const task = this.failing('terminal');
    await this.settle(scheduler.addTask(task, { createPromise: true }));
    await new Promise(r => setTimeout(r, 300));

    // Documented: a non-optional task with no retries left ends in user_action
    if (task.state !== 'user_action') {
      throw new Error(`Expected state user_action, got ${task.state}`);
    }
  }

  private async testTerminalStateStable(): Promise<void> {
    const { scheduler } = this.crashingScheduler();
    const task = this.failing('stable');
    await this.settle(scheduler.addTask(task, { createPromise: true }));

    // The state right after the promise settles must survive - it used to be
    // overwritten by the second settle a few milliseconds later
    const immediately = task.state;
    await new Promise(r => setTimeout(r, 400));
    if (task.state !== immediately) {
      throw new Error(`Terminal state changed after settling: ${immediately} -> ${task.state}`);
    }
  }

  private async testOptionalFailure(): Promise<void> {
    const { scheduler } = this.crashingScheduler();
    const counts = this.counted(scheduler);
    const task = this.failing('optional', { optional: true });

    const outcome = await this.settle(scheduler.addTask(task, { createPromise: true }));
    await new Promise(r => setTimeout(r, 300));

    if (outcome !== 'resolved:null') throw new Error(`Expected resolved:null, got ${outcome}`);
    if (scheduler.stats.failed !== 1) throw new Error(`Expected stats.failed 1, got ${scheduler.stats.failed}`);
    if ((counts['task.user_action'] ?? 0) !== 0) {
      throw new Error('An optional task must not request user action');
    }
  }

  private async testRetriesNoEarlyUserAction(): Promise<void> {
    const { scheduler } = this.crashingScheduler();
    const counts = this.counted(scheduler);
    const task = this.failing('retry-signal', { retryPolicy: { maxRetries: 2, backoffMs: 5 } });

    await this.settle(scheduler.addTask(task, { createPromise: true }));
    await new Promise(r => setTimeout(r, 400));

    // Exactly one terminal signal, only after the retry budget is spent
    if ((counts['task.user_action'] ?? 0) !== 1) {
      throw new Error(`Expected 1 task.user_action after retries, got ${counts['task.user_action'] ?? 0}`);
    }
    if (task.attempts !== 2) throw new Error(`Expected 2 attempts, got ${task.attempts}`);
  }

  private async testRetriesCountOnce(): Promise<void> {
    const { scheduler } = this.crashingScheduler();
    const task = this.failing('retry-count', { retryPolicy: { maxRetries: 2, backoffMs: 5 } });
    await this.settle(scheduler.addTask(task, { createPromise: true }));
    await new Promise(r => setTimeout(r, 400));

    // stats.failed counts tasks that failed, matching stats.done - not attempts
    if (scheduler.stats.failed !== 1) {
      throw new Error(`Expected stats.failed 1 for one task with 2 retries, got ${scheduler.stats.failed}`);
    }
  }

  private async testRetryThenSuccess(): Promise<void> {
    const pool = new WorkerManager(this.crashWorkerUrl, {
      maxThreads: 1, maxConcurrentTasks: 1, inline: true
    });
    const scheduler = this.track(new FyflowScheduler({ CrashingWorker: pool }));

    // The worker fails the first attempt, then succeeds
    let attempt = 0;
    scheduler.addEventListener('task.running', (e: any) => {
      attempt++;
      if (attempt > 1) e.detail.payload.action = 'normal-task';
    });

    const task = new FyflowTask({
      id: 'retry-success', workerType: 'CrashingWorker',
      payload: { action: 'throw-error' },
      retryPolicy: { maxRetries: 3, backoffMs: 5 }
    });
    const outcome = await this.settle(scheduler.addTask(task, { createPromise: true }));
    await new Promise(r => setTimeout(r, 300));

    if (outcome === 'NEVER SETTLED') throw new Error('Task never settled after a retry');
    if (scheduler.stats.done !== 1) throw new Error(`Expected done 1, got ${scheduler.stats.done}`);
    if (scheduler.stats.failed !== 0) {
      throw new Error(`A task that eventually succeeded must not count as failed, got ${scheduler.stats.failed}`);
    }
  }

  // --- attempt accounting ----------------------------------------------------

  /** Every scheduler task event for one task id, in order, as `event:attemptNumber`. */
  private attemptLog(scheduler: FyflowScheduler, id: string) {
    const log: string[] = [];
    for (const ev of ['task.running', 'task.retrying', 'task.completed', 'task.failed']) {
      scheduler.addEventListener(ev, (e: any) => {
        if (e.detail.id === id) log.push(`${ev.slice(5)}:${e.detail.attemptNumber}`);
      });
    }
    return log;
  }

  private async testAttemptsSpentContinuesTheBudget(): Promise<void> {
    const { scheduler } = this.crashingScheduler();
    const log = this.attemptLog(scheduler, 'resumed');
    const task = this.failing('resumed', { retryPolicy: { maxRetries: 2, backoffMs: 5 }, attemptsSpent: 1 });
    const before = { attempts: task.attempts, attemptNumber: task.attemptNumber };
    if (before.attempts !== 1 || before.attemptNumber !== 0) {
      throw new Error(`Before dispatch: ${JSON.stringify(before)}`);
    }

    if (await this.settle(scheduler.addTask(task, { createPromise: true })) !== 'rejected') {
      throw new Error('Expected the task to reject once its budget was spent');
    }
    const expected = 'running:2,retrying:2,running:3,failed:3';
    if (log.join(',') !== expected) throw new Error(`Expected ${expected}, got ${log.join(',')}`);
    if (task.attempts !== 2) throw new Error(`Expected attempts 2, got ${task.attempts}`);
  }

  private async testSpentBudgetRunsOnce(): Promise<void> {
    for (const optional of [false, true]) {
      const { scheduler } = this.crashingScheduler();
      const log = this.attemptLog(scheduler, 'spent');
      const task = this.failing('spent', { retryPolicy: { maxRetries: 2, backoffMs: 5 }, attemptsSpent: 3, optional });
      await this.settle(scheduler.addTask(task, { createPromise: true }));

      if (log.join(',') !== 'running:4,failed:4') {
        throw new Error(`optional ${optional}: expected one attempt numbered 4, got ${log.join(',')}`);
      }
      const state = optional ? 'failed' : 'user_action';
      if (task.state !== state) throw new Error(`optional ${optional}: ended ${task.state}, expected ${state}`);
    }
  }

  private async testAttemptsSpentValidated(): Promise<void> {
    const make = (attemptsSpent: unknown) => () =>
      new FyflowTask({ id: 'v', workerType: 'CrashingWorker', payload: {}, attemptsSpent });
    const throwsWith = (fn: () => unknown, type: Function) => {
      try { fn(); } catch (error) { return error instanceof type; }
      return false;
    };
    for (const bad of ['1', null, {}]) {
      if (!throwsWith(make(bad), TypeError)) throw new Error(`attemptsSpent ${JSON.stringify(bad)} did not throw TypeError`);
    }
    for (const bad of [-1, 1.5, NaN, Infinity]) {
      if (!throwsWith(make(bad), RangeError)) throw new Error(`attemptsSpent ${bad} did not throw RangeError`);
    }
    if (make(undefined)().attempts !== 0 || make(2)().attempts !== 2) {
      throw new Error('A valid attemptsSpent was not applied');
    }
  }

  // task.retrying is a lifecycle transition, so its detail is the live task,
  // read inside the listener.
  private async testRetryingEvent(): Promise<void> {
    const { scheduler } = this.crashingScheduler();
    const counts = this.counted(scheduler);
    const log = this.attemptLog(scheduler, 'retried');
    const seen: string[] = [];
    scheduler.addEventListener('task.retrying', (e: any) => {
      const t = e.detail;
      seen.push(`${t.state}/${t.attemptNumber}/${t.attempts}/${t.error}/${typeof t.timestamp}`);
    });

    const task = this.failing('retried', { retryPolicy: { maxRetries: 2, backoffMs: 5 } });
    await this.settle(scheduler.addTask(task, { createPromise: true }));

    const expected = 'running:1,retrying:1,running:2,retrying:2,running:3,failed:3';
    if (log.join(',') !== expected) throw new Error(`Expected ${expected}, got ${log.join(',')}`);
    const error = 'Intentional crash during task execution';
    const expectedSeen = [`pending/1/1/${error}/number`, `pending/2/2/${error}/number`];
    if (JSON.stringify(seen) !== JSON.stringify(expectedSeen)) {
      throw new Error(`task.retrying details: ${JSON.stringify(seen)}`);
    }
    if (counts['task.failed'] !== 1) throw new Error(`Expected one task.failed, got ${counts['task.failed']}`);
  }

  private async testRetrySuccessClearsError(): Promise<void> {
    const { scheduler } = this.crashingScheduler();
    scheduler.addEventListener('task.retrying', (e: any) => { e.detail.payload.action = 'normal-task'; });
    const task = this.failing('recovers', { retryPolicy: { maxRetries: 1, backoffMs: 5 } });
    const outcome = await this.settle(scheduler.addTask(task, { createPromise: true }));

    if (!outcome.startsWith('resolved')) throw new Error(`Expected success on the retry, got ${outcome}`);
    if (task.state !== 'done' || task.attemptNumber !== 2) {
      throw new Error(`Expected done on attempt 2, got ${task.state} on ${task.attemptNumber}`);
    }
    if (task.error !== undefined) throw new Error(`A task that succeeded still carries error "${task.error}"`);
  }

  // The retry used to wait in the ready queue, so any dispatch started it
  // early: 1 ms after failing where 1000 ms was asked for. An idle scheduler
  // passes either way, which is why this runs under load.
  private async testBackoffHoldsUnderLoad(): Promise<void> {
    const BACKOFF = 150;
    const crashing = new WorkerManager(this.crashWorkerUrl, { maxThreads: 1, maxConcurrentTasks: 1, inline: true });
    const load = new WorkerManager(this.requeueWorkerUrl, { maxThreads: 1, maxConcurrentTasks: 5, inline: true });
    const scheduler = this.track(new FyflowScheduler({ CrashingWorker: crashing, Load: load }));

    const TOTAL = 301;
    const starts: number[] = [];
    let loadStarts = 0, loadStartsAtLastAttempt = 0, overCounted = 0;
    scheduler.addEventListener('task.running', (e: any) => {
      if (e.detail.id !== 'backoff') { loadStarts++; return; }
      starts.push(performance.now());
      loadStartsAtLastAttempt = loadStarts;
    });
    // Stats count tasks, not attempts, through every backoff
    scheduler.addEventListener('task.retrying', () => {
      const s = scheduler.stats;
      if (s.queued + s.running + s.done + s.failed > TOTAL) overCounted++;
    });

    const promise = scheduler.addTask(
      this.failing('backoff', { retryPolicy: { maxRetries: 2, backoffMs: BACKOFF } }),
      { createPromise: true }
    );
    scheduler.addTasks(Array.from({ length: TOTAL - 1 }, (_, i) => new FyflowTask({
      id: `load-${i}`, workerType: 'Load', payload: { ms: 10 }
    })));
    await this.settle(promise, 5000);

    if (starts.length !== 3) throw new Error(`Expected 3 attempts, got ${starts.length}`);
    // Not vacuous: the load was still being dispatched when the last attempt
    // started, so dispatches kept happening throughout both backoffs
    if (loadStartsAtLastAttempt >= TOTAL - 1) {
      throw new Error('Precondition: the load had finished before the last attempt');
    }
    const gaps = [starts[1] - starts[0], starts[2] - starts[1]].map(g => Math.round(g));
    // 1 ms of timer slop
    if (gaps.some(gap => gap < BACKOFF - 1)) {
      throw new Error(`Retries started ${gaps.join(' and ')} ms apart under load, backoff is ${BACKOFF} ms`);
    }
    if (overCounted > 0) throw new Error(`Stats summed past the task count ${overCounted} time(s)`);
  }

  private async testNoCompletionDuringBackoff(): Promise<void> {
    const { scheduler } = this.crashingScheduler();
    const completedAt: number[] = [];
    scheduler.addEventListener('scheduler.completed', () => completedAt.push(performance.now()));

    // A success first, so the scheduler has something to report
    await this.settle(scheduler.addTask(new FyflowTask({
      id: 'ok-first', workerType: 'CrashingWorker', payload: { action: 'normal-task' }
    }), { createPromise: true }));
    completedAt.length = 0;

    // The second task runs once the first has failed, and settles inside its
    // backoff - a settle is what makes the scheduler check whether it drained
    const task = this.failing('slow-retry', { retryPolicy: { maxRetries: 1, backoffMs: 200 } });
    const promise = scheduler.addTask(task, { createPromise: true });
    let duringBackoff = false;
    scheduler.addTask(new FyflowTask({
      id: 'settles-in-backoff', workerType: 'CrashingWorker', payload: { action: 'normal-task' }
    }));
    scheduler.addEventListener('task.completed', (e: any) => {
      if (e.detail.id === 'settles-in-backoff') duringBackoff = task.state === 'pending' && task.attempts === 1;
    });
    await this.settle(promise);
    if (!duringBackoff) throw new Error('Precondition: nothing settled during the backoff');
    const failedAt = performance.now();

    if (completedAt.some(at => at < failedAt - 5)) {
      throw new Error('scheduler.completed fired while a retry was waiting out its backoff');
    }
    if (completedAt.length !== 1) throw new Error(`Expected one scheduler.completed, got ${completedAt.length}`);
  }

  // Draining dispatches: a backoff that ends during shutdown's drain runs its
  // retry. One that outlasts the drain is discarded with the queues and must
  // not fire into a pool that has been shut down.
  private async testShutdownDuringBackoff(): Promise<void> {
    const crashing = new WorkerManager(this.crashWorkerUrl, { maxThreads: 1, maxConcurrentTasks: 2, inline: true });
    const load = new WorkerManager(this.requeueWorkerUrl, { maxThreads: 1, maxConcurrentTasks: 1, inline: true });
    const scheduler = this.track(new FyflowScheduler({ CrashingWorker: crashing, Load: load }));

    const short = this.failing('short-backoff', { retryPolicy: { maxRetries: 1, backoffMs: 100 } });
    const long = this.failing('long-backoff', { retryPolicy: { maxRetries: 1, backoffMs: 700 } });
    scheduler.addTask(new FyflowTask({ id: 'keeps-draining', workerType: 'Load', payload: { ms: 400 } }));
    scheduler.addTask(short);
    scheduler.addTask(long);

    const started = performance.now();
    while (!(short.attempts === 1 && long.attempts === 1) && performance.now() - started < 2000) {
      await new Promise(r => setTimeout(r, 5));
    }
    if (long.attempts !== 1) throw new Error('Precondition: the tasks never failed their first attempt');

    await Promise.race([
      scheduler.shutdown(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('shutdown() hung')), 3000))
    ]);
    // Past the long backoff's end
    await new Promise(r => setTimeout(r, 900 - (performance.now() - started)));

    if (short.attemptNumber !== 2) throw new Error(`The retry due during the drain did not run (attempt ${short.attemptNumber})`);
    if (long.attemptNumber !== 1) throw new Error(`A retry was dispatched after shutdown (attempt ${long.attemptNumber})`);
    // Its timer was cleared: it did not land back in a queue shutdown emptied
    const leftover = [...scheduler.readyQueuesByWorker.values()].reduce((n, q) => n + q.length, 0);
    if (leftover > 0) throw new Error(`${leftover} task(s) were queued after shutdown`);
  }

  private async testSuccessOnce(): Promise<void> {
    const { scheduler } = this.crashingScheduler();
    const counts = this.counted(scheduler);
    const task = new FyflowTask({
      id: 'ok', workerType: 'CrashingWorker', payload: { action: 'normal-task' }
    });
    await this.settle(scheduler.addTask(task, { createPromise: true }));
    await new Promise(r => setTimeout(r, 300));

    if (counts['task.completed'] !== 1) {
      throw new Error(`Expected 1 task.completed, got ${counts['task.completed'] ?? 0}`);
    }
    if (scheduler.stats.done !== 1) throw new Error(`Expected done 1, got ${scheduler.stats.done}`);
  }

  private async testResourcesReleasedOnFailure(): Promise<void> {
    const cpu = new ConcurrentLimitGroup(2, 'cpu');
    const { scheduler } = this.crashingScheduler({ cpu }, { groups: ['cpu'] });

    await this.settle(scheduler.addTask(this.failing('res-fail'), { createPromise: true }));
    await new Promise(r => setTimeout(r, 300));

    if (cpu.getMetrics().running !== 0) {
      throw new Error(`Group slot leaked on failure: ${cpu.getMetrics().running}/${cpu.getMetrics().limit} still held`);
    }
  }

  // The case where only the pool's event path settles the task - it never
  // released resources, so a pool losing workers slowly drained group capacity
  private async testResourcesReleasedOnWorkerDeath(): Promise<void> {
    const cpu = new ConcurrentLimitGroup(2, 'cpu');
    const { scheduler } = this.dyingScheduler({ cpu }, { groups: ['cpu'], requeueFailedTasks: false });

    const task = new FyflowTask({
      id: 'res-death', workerType: 'SelfTerminatingWorker',
      payload: { action: 'self-terminate-delayed', delay: 30 }
    });
    await this.settle(scheduler.addTask(task, { createPromise: true }));
    await new Promise(r => setTimeout(r, 500));

    if (cpu.getMetrics().running !== 0) {
      throw new Error(`Group slot leaked when the worker died: ${cpu.getMetrics().running}/${cpu.getMetrics().limit} still held`);
    }
  }

  private async testWorkerDeathSettles(): Promise<void> {
    const { scheduler } = this.dyingScheduler({}, { requeueFailedTasks: false });
    const task = new FyflowTask({
      id: 'death', workerType: 'SelfTerminatingWorker',
      payload: { action: 'self-terminate-delayed', delay: 30 }
    });

    const outcome = await this.settle(scheduler.addTask(task, { createPromise: true }));
    await new Promise(r => setTimeout(r, 300));

    if (outcome === 'NEVER SETTLED') throw new Error('A dying worker left its task unsettled');
    if (scheduler.stats.failed !== 1) {
      throw new Error(`Expected stats.failed 1, got ${scheduler.stats.failed}`);
    }
  }

  // Documented contract: a worker that cannot be constructed behaves like any
  // other dead worker. With requeue on (the default) its tasks wait for a fixed
  // pool; with requeue off they fail.
  private async testUnconstructableWorkerSettles(): Promise<void> {
    // requeue OFF - the task must fail rather than wait
    {
      const { scheduler } = this.crashingScheduler({}, {
        config: { crashOnInit: true }, inline: false, requeueFailedTasks: false
      });
      const task = new FyflowTask({
        id: 'noworker-fail', workerType: 'CrashingWorker', payload: { action: 'normal-task' }
      });
      const outcome = await this.settle(scheduler.addTask(task, { createPromise: true }));
      await new Promise(r => setTimeout(r, 300));

      if (outcome === 'NEVER SETTLED') {
        throw new Error('requeue off: the task should fail rather than wait');
      }
      if (scheduler.stats.failed !== 1) {
        throw new Error(`requeue off: expected stats.failed 1, got ${scheduler.stats.failed}`);
      }
      await scheduler.shutdown();
    }

    // requeue ON (default) - the task waits for the pool to be repaired
    {
      const { scheduler } = this.crashingScheduler({}, {
        config: { crashOnInit: true }, inline: false, maxWorkerRestarts: 1
      });
      const task = new FyflowTask({
        id: 'noworker-wait', workerType: 'CrashingWorker', payload: { action: 'normal-task' }
      });
      const outcome = await this.settle(scheduler.addTask(task, { createPromise: true }), 1500);

      if (outcome !== 'NEVER SETTLED') {
        throw new Error(`requeue on: the task should wait, not settle (${outcome})`);
      }
      if (task.state !== 'pending') {
        throw new Error(`requeue on: expected the task to be queued, got state ${task.state}`);
      }
      await scheduler.shutdown();
    }
  }

  // A pool whose workers cannot be constructed must eventually announce that it
  // has given up, or an operator monitoring worker.restart_limit_exceeded never
  // learns the pool needs fixing. Construction failures set no `canRestart`, and
  // both the restart branch and the alarm branch required `canRestart === true`,
  // so neither ran: the pool kept a dead worker in its slot and stayed silent.
  private async testUnstartablePoolAlarms(): Promise<void> {
    for (const inline of [true, false]) {
      const pool = new WorkerManager(this.crashWorkerUrl, {
        maxThreads: 1, maxConcurrentTasks: 1, inline,
        maxWorkerRestarts: 1, config: { crashOnInit: true }
      });
      const scheduler = this.track(new FyflowScheduler({ CrashingWorker: pool }));

      let alarms = 0;
      let workerFailures = 0;
      pool.addEventListener('worker.restart_limit_exceeded', () => { alarms++; });
      pool.addEventListener('worker.failed', () => { workerFailures++; });

      scheduler.addTask(new FyflowTask({
        id: `alarm-${inline}`, workerType: 'CrashingWorker', payload: { action: 'normal-task' }
      }));

      const deadline = performance.now() + 20000;
      while (alarms === 0 && performance.now() < deadline) {
        await new Promise(r => setTimeout(r, 100));
      }

      const kind = inline ? 'inline' : 'threaded';
      if (workerFailures === 0) {
        throw new Error(`${kind}: a worker that could not be constructed never reported worker.failed`);
      }
      if (alarms === 0) {
        throw new Error(`${kind}: pool exhausted its restarts without emitting worker.restart_limit_exceeded`);
      }
      await scheduler.shutdown();
    }
  }

  // queued + running + done + failed must never exceed the number of tasks. A
  // requeue used to increment queued while the same failure's promise settled the
  // task as failed, so three tasks against an unstartable pool reported
  // queued=3 alongside failed=2 - five outcomes for three tasks.
  private async testStatsAccountEachTaskOnce(): Promise<void> {
    for (const inline of [true, false]) {
      const pool = new WorkerManager(this.crashWorkerUrl, {
        maxThreads: 1, maxConcurrentTasks: 1, inline,
        maxWorkerRestarts: 1, config: { crashOnInit: true }
      });
      const scheduler = this.track(new FyflowScheduler({ CrashingWorker: pool }));

      let alarm = 0;
      pool.addEventListener('worker.restart_limit_exceeded', () => { alarm++; });

      const TASKS = 3;
      for (let i = 0; i < TASKS; i++) {
        scheduler.addTask(new FyflowTask({
          id: `acct-${inline}-${i}`, workerType: 'CrashingWorker', payload: { action: 'normal-task' }
        }));
      }

      const deadline = performance.now() + 20000;
      while (alarm === 0 && performance.now() < deadline) {
        await new Promise(r => setTimeout(r, 100));
      }
      await new Promise(r => setTimeout(r, 500));

      const s = scheduler.stats;
      const total = s.queued + s.running + s.done + s.failed;
      const kind = inline ? 'inline' : 'threaded';
      if (total > TASKS) {
        throw new Error(`${kind}: stats account for ${total} outcomes across ${TASKS} tasks - ${JSON.stringify(s)}`);
      }
      await scheduler.shutdown();
    }
  }

  // A worker dying under a task requeues it; that must not also count as a failure
  private async testRequeueNotAlsoFailed(): Promise<void> {
    const pool = new WorkerManager(this.selfTerminatingWorkerUrl, {
      maxThreads: 1, maxConcurrentTasks: 1, inline: false,
      requeueFailedTasks: true, maxWorkerRestarts: 1
    });
    const scheduler = this.track(new FyflowScheduler({ SelfTerminatingWorker: pool }));

    let requeues = 0;
    pool.addEventListener('task.requeue_required', () => { requeues++; });

    // Worker-level termination: it blames no task, so the running one is
    // requeued. (Through the task's own context it would fail that task instead.)
    scheduler.addTask(new FyflowTask({
      id: 'requeue-acct', workerType: 'SelfTerminatingWorker',
      payload: { action: 'worker-level-terminate', restartDelay: 1000 }
    }));

    const deadline = performance.now() + 6000;
    while (requeues === 0 && performance.now() < deadline) {
      await new Promise(r => setTimeout(r, 50));
    }
    await new Promise(r => setTimeout(r, 500));

    if (requeues === 0) throw new Error('The dying worker never requeued its task');

    const s = scheduler.stats;
    const total = s.queued + s.running + s.done + s.failed;
    if (total > 1) {
      throw new Error(`One task accounted ${total} times: ${JSON.stringify(s)}`);
    }
  }

  private async testCountersNonNegative(): Promise<void> {
    const { scheduler } = this.crashingScheduler();
    let lowest = { running: 0, queued: 0, failed: 0, done: 0 };
    const sample = () => {
      const s = scheduler.stats;
      lowest = {
        running: Math.min(lowest.running, s.running), queued: Math.min(lowest.queued, s.queued),
        failed: Math.min(lowest.failed, s.failed), done: Math.min(lowest.done, s.done)
      };
    };
    scheduler.addEventListener('task.failed', sample);
    scheduler.addEventListener('task.completed', sample);

    const tasks = Array.from({ length: 6 }, (_, i) => this.failing(`neg-${i}`));
    await Promise.all((scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[])
      .map(p => p.catch(() => null)));
    await new Promise(r => setTimeout(r, 400));
    sample();

    if (lowest.running < 0 || lowest.queued < 0 || lowest.failed < 0 || lowest.done < 0) {
      throw new Error(`A counter went negative: ${JSON.stringify(lowest)}`);
    }
    if (scheduler.stats.failed !== 6) {
      throw new Error(`Expected stats.failed 6 for 6 failing tasks, got ${scheduler.stats.failed}`);
    }
  }

  // AGENTS.md §4: "The task running at the time rejects with a
  // WorkerTerminationError". A threaded worker handled the termination request
  // before rejecting that task, so the pool requeued it instead, and the task
  // terminated every replacement worker in turn until the pool gave up.
  private async testSelfTerminatingTaskFails(): Promise<void> {
    for (const inline of [true, false]) {
      const kind = inline ? 'inline' : 'threaded';
      const pool = new WorkerManager(this.selfTerminatingWorkerUrl, {
        maxThreads: 1, maxConcurrentTasks: 1, inline, maxWorkerRestarts: 3
      });
      const scheduler = this.track(new FyflowScheduler({ SelfTerminatingWorker: pool }));
      const counts = this.counted(scheduler);
      let requeues = 0;
      pool.addEventListener('task.requeue_required', (e: any) => {
        if (e.detail.taskId === `terminating-${kind}`) requeues++;
      });

      const task = new FyflowTask({
        id: `terminating-${kind}`, workerType: 'SelfTerminatingWorker',
        payload: { action: 'self-terminate', canRestart: true, restartDelay: 10 }
      });
      let rejection: unknown;
      const promise = (scheduler.addTask(task, { createPromise: true }) as Promise<any>)
        .catch(error => { rejection = error; });
      const outcome = await Promise.race([
        promise.then(() => 'settled'),
        new Promise(r => setTimeout(() => r('NEVER SETTLED'), 3000))
      ]);
      await new Promise(r => setTimeout(r, 200));

      if (outcome !== 'settled') throw new Error(`${kind}: the task never settled (state ${task.state})`);
      if (!(rejection instanceof WorkerTerminationError)) {
        throw new Error(`${kind}: expected a WorkerTerminationError, got ${rejection}`);
      }
      if (task.state !== 'user_action') throw new Error(`${kind}: expected user_action, got ${task.state}`);
      if (counts['task.running'] !== 1) throw new Error(`${kind}: ran ${counts['task.running']} times`);
      if (counts['task.failed'] !== 1) throw new Error(`${kind}: task.failed fired ${counts['task.failed'] ?? 0} times`);
      if (requeues !== 0) throw new Error(`${kind}: the terminating task was requeued ${requeues} times`);
      await scheduler.shutdown();
    }
  }

  // --- terminateWithError(): a request, attributed by context ----------------
  //
  // It never throws. Through a task's context it fails that task with a
  // WorkerTerminationError and requeues the worker's others; through the
  // worker's own context it requeues every in-flight task. Inline it used to
  // throw, which worked only in run()'s own stack and crashed the host from a
  // timer; threaded it posted without a task, so the caller was requeued too.

  /** A caller and a co-tenant on one worker; the caller asks for termination. */
  private async terminationScenario(inline: boolean, callerPayload: any, restartDelay: number) {
    const kind = inline ? 'inline' : 'threaded';
    const pool = new WorkerManager(this.selfTerminatingWorkerUrl, {
      maxThreads: 1, maxConcurrentTasks: 2, inline, maxWorkerRestarts: 3
    });
    const scheduler = this.track(new FyflowScheduler({ SelfTerminatingWorker: pool }));

    const runs: Record<string, number> = {};
    scheduler.addEventListener('task.running', (e: any) => { runs[e.detail.id] = (runs[e.detail.id] ?? 0) + 1; });
    const failedEvents: Record<string, number> = {};
    scheduler.addEventListener('task.failed', (e: any) => { failedEvents[e.detail.id] = (failedEvents[e.detail.id] ?? 0) + 1; });
    const requeued: Record<string, number> = {};
    pool.addEventListener('task.requeue_required', (e: any) => {
      requeued[e.detail.taskId] = (requeued[e.detail.taskId] ?? 0) + 1;
    });
    let workerFailures = 0;
    pool.addEventListener('worker.failed', () => { workerFailures++; });

    // A worker that is still being created accepts nothing, so without this the
    // co-tenant would wait in the scheduler's queue instead of running beside
    // the caller, and there would be nothing on the worker to requeue
    await scheduler.addTask(new FyflowTask({
      id: `warmup-${kind}`, workerType: 'SelfTerminatingWorker', payload: { action: 'normal-task' }
    }), { createPromise: true });

    const caller = new FyflowTask({
      id: `caller-${kind}`, workerType: 'SelfTerminatingWorker',
      payload: { restartDelay, ...callerPayload }
    });
    const cotenant = new FyflowTask({
      id: `cotenant-${kind}`, workerType: 'SelfTerminatingWorker',
      // Long enough to still be running when the caller asks
      payload: { action: 'conditional-terminate', shouldTerminate: false, delay: 300 }
    });
    // The co-tenant first, so it is already running when the caller asks
    const cotenantPromise = scheduler.addTask(cotenant, { createPromise: true }) as Promise<any>;
    let callerError: unknown;
    const callerPromise = (scheduler.addTask(caller, { createPromise: true }) as Promise<any>)
      .then(() => 'resolved', error => { callerError = error; return 'rejected'; });

    return {
      kind, scheduler, caller, cotenant, runs, failedEvents, requeued,
      callerPromise, cotenantPromise,
      get callerError() { return callerError; },
      get workerFailures() { return workerFailures; }
    };
  }

  private async assertTaskContextTermination(callerPayload: any): Promise<void> {
    for (const inline of [true, false]) {
      const s = await this.terminationScenario(inline, callerPayload, 10);
      const callerOutcome = await Promise.race([
        s.callerPromise, new Promise(r => setTimeout(() => r('never settled'), 3000))
      ]);
      const cotenantOutcome = await this.settle(s.cotenantPromise, 5000);
      await new Promise(r => setTimeout(r, 200));

      if (callerOutcome !== 'rejected') throw new Error(`${s.kind}: caller ${callerOutcome}`);
      if (!(s.callerError instanceof WorkerTerminationError)) {
        throw new Error(`${s.kind}: caller rejected with ${s.callerError}, not a WorkerTerminationError`);
      }
      if (s.caller.state !== 'user_action') throw new Error(`${s.kind}: caller ended ${s.caller.state}`);
      if (s.runs[s.caller.id] !== 1) throw new Error(`${s.kind}: caller ran ${s.runs[s.caller.id]} times`);
      if (s.failedEvents[s.caller.id] !== 1) throw new Error(`${s.kind}: caller failed ${s.failedEvents[s.caller.id] ?? 0} times`);
      if (s.requeued[s.caller.id]) throw new Error(`${s.kind}: the caller was requeued`);
      if (s.requeued[s.cotenant.id] !== 1) throw new Error(`${s.kind}: co-tenant requeued ${s.requeued[s.cotenant.id] ?? 0} times`);
      if (!cotenantOutcome.startsWith('resolved')) throw new Error(`${s.kind}: co-tenant ${cotenantOutcome}`);
      if (s.workerFailures !== 1) throw new Error(`${s.kind}: worker.failed fired ${s.workerFailures} times`);
      await s.scheduler.shutdown();
    }
  }

  private async testTaskContextTermination(): Promise<void> {
    // Called directly in run(), which then carries on and returns
    await this.assertTaskContextTermination({ action: 'conditional-terminate', shouldTerminate: true, canRestart: true });
  }

  private async testTaskContextTerminationFromTimer(): Promise<void> {
    // Called from a setTimeout inside run(). Inline, this used to throw where
    // nothing could catch it: an uncaught exception that took the host down.
    await this.assertTaskContextTermination({ action: 'self-terminate-delayed', delay: 30 });
  }

  private async testWorkerContextTermination(): Promise<void> {
    for (const inline of [true, false]) {
      // A long restart delay keeps the requeued tasks waiting, so the assertion
      // sees the requeue itself rather than the reruns that follow it
      const s = await this.terminationScenario(inline, { action: 'worker-level-terminate' }, 1500);
      const deadline = performance.now() + 3000;
      while (s.workerFailures === 0 && performance.now() < deadline) {
        await new Promise(r => setTimeout(r, 10));
      }
      await new Promise(r => setTimeout(r, 200));

      if (s.workerFailures !== 1) throw new Error(`${s.kind}: worker.failed fired ${s.workerFailures} times`);
      for (const task of [s.caller, s.cotenant]) {
        if (s.requeued[task.id] !== 1) throw new Error(`${s.kind}: ${task.id} requeued ${s.requeued[task.id] ?? 0} times`);
        if (s.failedEvents[task.id]) throw new Error(`${s.kind}: ${task.id} failed`);
        if (task.state !== 'pending') throw new Error(`${s.kind}: ${task.id} is ${task.state}, expected pending`);
      }
      await s.scheduler.shutdown();
    }
  }

  // A self-terminating worker reports its failure twice - `worker.failed` and
  // `worker.termination_requested` - and the pool used to handle both: one
  // failure spent two restarts, and the worker was terminated twice.
  private async testOneFailureHandledOnce(): Promise<void> {
    for (const inline of [true, false]) {
      const kind = inline ? 'inline' : 'threaded';
      const pool = new WorkerManager(this.selfTerminatingWorkerUrl, {
        maxThreads: 1, maxConcurrentTasks: 1, inline, maxWorkerRestarts: 1
      });
      const scheduler = this.track(new FyflowScheduler({ SelfTerminatingWorker: pool }));

      let alarms = 0;
      pool.addEventListener('worker.restart_limit_exceeded', () => { alarms++; });
      const teardowns = new Map<string, number>();
      pool.addEventListener('worker.teardown.started', (e: any) => {
        teardowns.set(e.detail.workerId, (teardowns.get(e.detail.workerId) ?? 0) + 1);
      });
      let failedWorkerId = '';
      pool.addEventListener('worker.failed', (e: any) => { failedWorkerId ||= e.detail.workerId; });

      const dying = scheduler.addTask(new FyflowTask({
        id: `once-dying-${kind}`, workerType: 'SelfTerminatingWorker',
        payload: { action: 'self-terminate', canRestart: true, restartDelay: 10 }
      }), { createPromise: true });
      await this.settle(dying as Promise<any>);

      // The pool still has a restart left, so the next task gets a new worker
      const next = await this.settle(scheduler.addTask(new FyflowTask({
        id: `once-next-${kind}`, workerType: 'SelfTerminatingWorker', payload: { action: 'normal-task' }
      }), { createPromise: true }), 5000);
      // Long enough for a second teardown of the failed worker to show
      await new Promise(r => setTimeout(r, 300));

      if (!failedWorkerId) throw new Error(`${kind}: the worker never failed`);
      if (alarms !== 0) {
        throw new Error(`${kind}: one failure with maxWorkerRestarts 1 raised worker.restart_limit_exceeded`);
      }
      if (teardowns.get(failedWorkerId) !== 1) {
        throw new Error(`${kind}: the failed worker's teardown started ${teardowns.get(failedWorkerId) ?? 0} times`);
      }
      if (!next.startsWith('resolved')) {
        throw new Error(`${kind}: the next task did not complete on a replacement worker (${next})`);
      }
      await scheduler.shutdown();
    }
  }

  // --- requeue: the abandoned run must not settle the live attempt ------------
  //
  // When a worker fails, its running tasks are requeued and dispatched again,
  // but the run on the failed worker keeps executing: an inline run() cannot be
  // interrupted, and a threaded one lives until its worker's teardown is
  // acknowledged. Its outcome used to settle the NEW attempt.

  /**
   * Run one task, fail its worker partway through, and report what happened.
   *
   * The failure is injected as the `worker.failed` event a crashing worker
   * produces (ThreadWrapper reports an unexpected exit the same way), so the
   * test exercises the pool's real failure handling without needing a worker
   * that crashes on its first attempt but not its second.
   *
   * Timing is pinned rather than guessed. A second worker is warmed up first,
   * so the requeued task is re-dispatched at once instead of waiting 100ms or
   * more for a replacement thread. And RequeueWorker ends a run 50ms after its
   * worker's teardown starts, so the abandoned run's outcome always arrives
   * after that re-dispatch, and before a threaded worker's thread is killed.
   */
  private async requeueAbandonedRun(inline: boolean, payload: any) {
    // The live attempt's run, on a worker that is never torn down
    const LIVE_RUN_MS = 400;
    const pool = new WorkerManager(this.requeueWorkerUrl, {
      maxThreads: 2, maxConcurrentTasks: 1, inline,
      requeueFailedTasks: true, maxWorkerRestarts: 3
    });
    const scheduler = this.track(new FyflowScheduler({ Requeue: pool }));

    const starts: Array<{ workerId: string; at: number }> = [];
    pool.addEventListener('task.started', (e: any) => {
      if (e.detail.taskId === 'tenant') starts.push({ workerId: e.detail.workerId, at: performance.now() });
    });
    const outcomes: string[] = [];
    scheduler.addEventListener('task.completed', (e: any) => {
      if (e.detail.id === 'tenant') outcomes.push(`completed:${e.detail.result}`);
    });
    scheduler.addEventListener('task.failed', (e: any) => {
      if (e.detail.id === 'tenant') outcomes.push(`failed:${e.detail.error}`);
    });
    const progress: string[] = [];
    scheduler.addEventListener('task.progress', (e: any) => {
      if (e.detail.id === 'tenant') progress.push(e.detail.message);
    });
    // Emitted events go the same way as progress. Subscribing also opens the
    // message gate, which is closed while nobody listens.
    const messages: string[] = [];
    scheduler.addEventListener('task.message', (e: any) => {
      if (e.detail.taskId === 'tenant') messages.push(e.detail.data?.from);
    });
    // A requeue is not an attempt: both runs report the same number
    const attemptNumbers: number[] = [];
    scheduler.addEventListener('task.running', (e: any) => {
      if (e.detail.id === 'tenant') attemptNumbers.push(e.detail.attemptNumber);
    });

    const promise = scheduler.addTask(new FyflowTask({
      id: 'tenant', workerType: 'Requeue',
      payload: { ms: LIVE_RUN_MS, spawnWorkerType: 'Requeue', ...payload }
    }), { createPromise: true });
    // Lands on a second worker, since the first is busy, and leaves it idle and
    // initialised for the requeue
    const warmup = scheduler.addTask(new FyflowTask({
      id: 'warmup', workerType: 'Requeue', payload: { ms: 0 }
    }), { createPromise: true });

    const startDeadline = performance.now() + 5000;
    while (starts.length === 0 && performance.now() < startDeadline) {
      await new Promise(r => setTimeout(r, 5));
    }
    if (starts.length === 0) throw new Error('The task never started');
    const first = starts[0];
    if (await this.settle(warmup as Promise<any>, 5000) === 'NEVER SETTLED') {
      throw new Error('The warm-up task never finished');
    }

    const failedAt = performance.now();
    (pool as any).threads.find((w: any) => w.id === first.workerId).dispatchEvent(
      new CustomEvent('worker.failed', {
        detail: {
          workerId: first.workerId,
          error: new Error('simulated crash'),
          metadata: { failureType: 'runtime', canRestart: true, restartDelay: 10 },
          timestamp: Date.now()
        }
      })
    );

    const settled = await this.settle(promise as Promise<any>, 5000);
    // Long enough for a late outcome from either run to arrive
    await new Promise(r => setTimeout(r, 300));

    if (starts.length < 2) throw new Error(`The task was never re-dispatched (${starts.length} start)`);
    const second = starts[1];
    // A re-dispatch onto the failed worker would make every assertion below
    // compare a worker id with itself
    if (second.workerId === first.workerId) {
      throw new Error(`The task was re-dispatched onto the worker that failed (${first.workerId})`);
    }
    // Without this the test is vacuous: an abandoned run that finishes before
    // the re-dispatch meets the previous attempt's settle flag and is ignored
    // anyway, fix or no fix. It finishes 50ms after the failure.
    if (second.at - failedAt >= 50) {
      throw new Error(
        `Precondition not met: re-dispatch took ${(second.at - failedAt).toFixed(0)}ms ` +
        `after the failure, by when the abandoned run had already finished`
      );
    }

    return { scheduler, first, second, settled, outcomes, progress, attemptNumbers, messages };
  }

  private async testRequeueAbandonedResultIgnored(): Promise<void> {
    for (const inline of [true, false]) {
      const kind = inline ? 'inline' : 'threaded';
      const { scheduler, first, second, settled, outcomes, attemptNumbers } = await this.requeueAbandonedRun(inline, {});

      const tenant = scheduler.tasks.get('tenant')!;
      if (attemptNumbers.join(',') !== '1,1' || tenant.attempts !== 0) {
        throw new Error(
          `${kind}: a requeue counted as an attempt - attempt numbers ${attemptNumbers.join(',')}, ` +
          `attempts ${tenant.attempts}`
        );
      }

      if (outcomes.length !== 1) {
        throw new Error(`${kind}: expected one outcome, got ${JSON.stringify(outcomes)}`);
      }
      if (settled !== `resolved:${JSON.stringify(second.workerId)}`) {
        throw new Error(
          `${kind}: settled ${settled}, expected the live attempt's result ${second.workerId} ` +
          `(the abandoned run was on ${first.workerId})`
        );
      }
      // Two done: the task, and the warm-up task
      if (scheduler.stats.done !== 2 || scheduler.stats.running !== 0) {
        throw new Error(`${kind}: stats ${JSON.stringify(scheduler.stats)}`);
      }
      await scheduler.shutdown();
    }
  }

  // An inline run that throws reports it as a wrapper `task.failed` event, which
  // the scheduler settles from directly. That path carries no dispatch token,
  // so only the failed worker forgetting the task keeps it from failing the
  // live attempt.
  private async testRequeueAbandonedFailureIgnored(): Promise<void> {
    for (const inline of [true, false]) {
      const kind = inline ? 'inline' : 'threaded';
      // Both runs fail, each naming its own worker, so the assertion is on
      // which failure is reported: the abandoned run's arrives first.
      const { scheduler, second, outcomes } = await this.requeueAbandonedRun(inline, { fail: true });

      if (outcomes.length !== 1 || outcomes[0] !== `failed:failed on ${second.workerId}`) {
        throw new Error(`${kind}: expected the live attempt's failure only, got ${JSON.stringify(outcomes)}`);
      }
      await scheduler.shutdown();
    }
  }

  // Progress, emitted events and spawns sent by the abandoned run after the
  // re-dispatch belong to no live attempt and must be dropped, or the live
  // attempt reports another run's progress and the task spawns its children
  // twice.
  private async testRequeueAbandonedSideEffectsDropped(): Promise<void> {
    for (const inline of [true, false]) {
      const kind = inline ? 'inline' : 'threaded';
      const { scheduler, first, second, progress, messages } = await this.requeueAbandonedRun(inline, { report: true });

      if (progress.some(m => m === `from ${first.workerId}`)) {
        throw new Error(`${kind}: progress from the abandoned run was delivered: ${JSON.stringify(progress)}`);
      }
      if (messages.includes(first.workerId) || !messages.includes(second.workerId)) {
        throw new Error(`${kind}: expected the live attempt's emitted events only, got ${JSON.stringify(messages)}`);
      }
      if (scheduler.tasks.has(`tenant-child-${first.workerId}`)) {
        throw new Error(`${kind}: the abandoned run's spawn was added`);
      }
      if (!scheduler.tasks.has(`tenant-child-${second.workerId}`)) {
        throw new Error(`${kind}: the live attempt's spawn is missing`);
      }
      await scheduler.shutdown();
    }
  }
}

// Auto-run tests when executed directly - handle both Deno and Node.js
if ((typeof Deno !== 'undefined' && import.meta.main) ||
    (typeof process !== 'undefined' && process.argv[1] && import.meta.url &&
     import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/')))) {
  const testSuite = new SettlementTestSuite();
  try {
    await testSuite.runAllTests(true);
  } finally {
    await testSuite.cleanup();
  }
}

export { SettlementTestSuite };
export default SettlementTestSuite;
