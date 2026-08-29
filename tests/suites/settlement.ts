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

import { WorkerManager, FyflowScheduler, FyflowTask, ConcurrentLimitGroup } from '../../index.ts';

// Node.js process declaration for cross-platform compatibility
declare const process: any;

interface TestResult { name: string; passed: boolean; duration: number; error?: string; }
interface TestSuiteResult {
  platform: string; totalTests: number; passed: number; failed: number;
  duration: number; results: TestResult[];
}

class SettlementTestSuite {
  private results: TestResult[] = [];
  private startTime = 0;
  private crashWorkerUrl = "";
  private selfTerminatingWorkerUrl = "";
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
    console.log(`\n🧪 Running: ${name}`);
    const start = performance.now();
    try {
      await testFn();
      const duration = performance.now() - start;
      console.log(`✅ PASSED: ${name} (${duration.toFixed(1)}ms)`);
      return { name, passed: true, duration };
    } catch (error: any) {
      const duration = performance.now() - start;
      console.log(`❌ FAILED: ${name} (${duration.toFixed(1)}ms) - ${error.message}`);
      return { name, passed: false, duration, error: error.message };
    }
  }

  async runAllTests(exitOnComplete = true): Promise<TestSuiteResult> {
    if (typeof Deno !== "undefined") {
      this.crashWorkerUrl = new URL("../workers/crashingWorker.ts", import.meta.url).href;
      this.selfTerminatingWorkerUrl = new URL("../workers/selfTerminatingWorker.ts", import.meta.url).href;
    } else {
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      this.crashWorkerUrl = new URL((await import('../workers/crashingWorker.ts?worker-direct')).default).href;
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      this.selfTerminatingWorkerUrl = new URL((await import('../workers/selfTerminatingWorker.ts?worker-direct')).default).href;
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
    this.results.push(await this.runTest('Success Settles Exactly Once', () => this.testSuccessOnce()));
    this.results.push(await this.runTest('Resources Released On Ordinary Failure', () => this.testResourcesReleasedOnFailure()));
    this.results.push(await this.runTest('Resources Released When Worker Dies', () => this.testResourcesReleasedOnWorkerDeath()));
    this.results.push(await this.runTest('Worker Death Settles The Task', () => this.testWorkerDeathSettles()));
    this.results.push(await this.runTest('Unconstructable Worker Follows The Requeue Contract', () => this.testUnconstructableWorkerSettles()));
    this.results.push(await this.runTest('Counters Stay Non-Negative', () => this.testCountersNonNegative()));
    this.results.push(await this.runTest('Unstartable Pool Raises The Alarm', () => this.testUnstartablePoolAlarms()));
    this.results.push(await this.runTest('Stats Account For Each Task Once', () => this.testStatsAccountEachTaskOnce()));
    this.results.push(await this.runTest('Requeue Does Not Also Count A Failure', () => this.testRequeueNotAlsoFailed()));

    const totalDuration = performance.now() - this.startTime;
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

    scheduler.addTask(new FyflowTask({
      id: 'requeue-acct', workerType: 'SelfTerminatingWorker',
      payload: { action: 'self-terminate-delayed', delay: 30 }
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
