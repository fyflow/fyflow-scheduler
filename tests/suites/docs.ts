// Executable documentation examples
//
// Every recipe published in README.md and AGENTS.md runs here. If an API
// changes, these fail and the docs get fixed with the code, instead of rotting
// silently the way onCompleteDescendants() and the benchmark efficiency metric
// both did.
//
// Keep each test body as close as possible to the published snippet.

import {
  WorkerManager,
  FyflowScheduler,
  FyflowTask,
  ConcurrentLimitGroup,
  RateLimitGroup,
  KeyedRateLimitGroup
} from '../../index.ts';
import type { WorkerStatus } from '../../index.ts';

// Node.js process declaration for cross-platform compatibility
declare const process: any;

interface TestResult {
  name: string;
  passed: boolean;
  duration: number;
  error?: string;
}

interface TestSuiteResult {
  platform: string;
  totalTests: number;
  passed: number;
  failed: number;
  duration: number;
  results: TestResult[];
}

class DocsTestSuite {
  private results: TestResult[] = [];
  private startTime = 0;
  private workerUrl = "";
  private schedulers: FyflowScheduler[] = [];

  // RECIPE: cross-runtime worker URL resolution
  // Deno resolves the source file directly; Node and the browser go through the
  // esbuild worker plugin, which turns `?worker-direct` into a bundled worker.
  private async resolveWorkerUrl(): Promise<string> {
    if (typeof Deno !== "undefined") {
      return new URL("../workers/docsWorker.ts", import.meta.url).href;
    }
    // @ts-expect-error - esbuild rewrites the ?worker-direct import at build time
    return new URL((await import('../workers/docsWorker.ts?worker-direct')).default).href;
  }

  private async resolveNoTeardownWorkerUrl(): Promise<string> {
    if (typeof Deno !== "undefined") {
      return new URL("../workers/noTeardownWorker.ts", import.meta.url).href;
    }
    // @ts-expect-error - esbuild rewrites the ?worker-direct import at build time
    return new URL((await import('../workers/noTeardownWorker.ts?worker-direct')).default).href;
  }

  private makeScheduler(
    poolOptions: any = {},
    groups: Record<string, any> = {},
    schedulerOptions: any = {}
  ): FyflowScheduler {
    const pool = new WorkerManager(this.workerUrl, {
      maxThreads: 2,
      maxConcurrentTasks: 4,
      inline: true,
      ...poolOptions
    });
    const scheduler = new FyflowScheduler({ DocsWorker: pool }, groups, schedulerOptions);
    this.schedulers.push(scheduler);
    return scheduler;
  }

  async cleanup(): Promise<void> {
    for (const scheduler of this.schedulers) {
      try {
        await scheduler.shutdown();
      } catch (error) {
        console.warn(`⚠️ Error during scheduler cleanup:`, error);
      }
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
    this.workerUrl = await this.resolveWorkerUrl();

    const platform = typeof globalThis !== 'undefined' && 'Deno' in globalThis ? 'Deno' : 'Node.js';
    console.log(`🚀 FyFlow Documentation Examples - ${platform}`);
    console.log('='.repeat(60));

    this.startTime = performance.now();
    this.results = [];

    this.results.push(await this.runTest('Doc: Run One Task And Read Its Result', () => this.docSingleTask()));
    this.results.push(await this.runTest('Doc: Fire-And-Forget Is The Default', () => this.docFireAndForget()));
    this.results.push(await this.runTest('Doc: Batch Tasks With addTasks', () => this.docBatch()));
    this.results.push(await this.runTest('Doc: Worker Config From The Pool', () => this.docWorkerConfig()));
    this.results.push(await this.runTest('Doc: Concurrent Limit Group', () => this.docConcurrentGroup()));
    this.results.push(await this.runTest('Doc: Rate Limit Group', () => this.docRateLimitGroup()));
    this.results.push(await this.runTest('Doc: Resource Metrics And Stats', () => this.docResourceMonitoring()));
    this.results.push(await this.runTest('Doc: Progress Reporting', () => this.docProgress()));
    this.results.push(await this.runTest('Doc: Spawning And Descendants', () => this.docSpawning()));
    this.results.push(await this.runTest('Doc: Retry Policy', () => this.docRetry()));
    this.results.push(await this.runTest('Doc: Optional Tasks', () => this.docOptionalTask()));
    this.results.push(await this.runTest('Doc: Task Failure Handling', () => this.docFailure()));
    this.results.push(await this.runTest('Doc: Worker Status Inspection', () => this.docWorkerStatus()));
    this.results.push(await this.runTest('Doc: Restart A Worker', () => this.docRestartWorker()));
    this.results.push(await this.runTest('Doc: replaceWorker Is An Alias', () => this.docReplaceWorker()));
    this.results.push(await this.runTest('Doc: updateWorkerConfig And Its Caveat', () => this.docUpdateWorkerConfig()));
    this.results.push(await this.runTest('Doc: task.started Fires On The Pool', () => this.docTaskStartedEvent()));
    this.results.push(await this.runTest('Doc: Teardown Failure Is Reported', () => this.docTeardownFailure()));
    this.results.push(await this.runTest('Doc: Worker Self-Termination', () => this.docSelfTermination()));
    this.results.push(await this.runTest('Doc: Worker Lifecycle Events', () => this.docLifecycleEvents()));
    this.results.push(await this.runTest('Doc: Bounded Task Retention', () => this.docRetention()));
    this.results.push(await this.runTest('Doc: Waiting For The Whole Run', () => this.docSchedulerCompleted()));
    this.results.push(await this.runTest('Doc: Unknown Worker Type Behaviour', () => this.docUnknownWorkerType()));
    this.results.push(await this.runTest('Doc: Inline Concurrency Is The Product', () => this.docInlineConcurrency()));
    this.results.push(await this.runTest('Doc: Groups Must Be Declared By The Pool', () => this.docGroupsMustBeDeclared()));
    this.results.push(await this.runTest('Doc: Keyed Rate Limit Is Per Key', () => this.docKeyedRateLimit()));
    this.results.push(await this.runTest('Doc: Keyed Limit Isolates A Saturated Key', () => this.docKeyedIsolation()));
    this.results.push(await this.runTest('Doc: Keyed Limit Requires A Key', () => this.docKeyedRequiresKey()));
    this.results.push(await this.runTest('Doc: Keyed Limit Metrics And Eviction', () => this.docKeyedMetrics()));
    this.results.push(await this.runTest('Doc: An Event Detail Is A Live Reference', () => this.docDetailsAreLive()));
    this.results.push(await this.runTest('Doc: Teardown Events Fire Without A teardown() Method', () => this.docTeardownEventsAreUnconditional()));
    this.results.push(await this.runTest('Doc: AGENTS.md Covers Every Export', () => this.docExportsDocumented()));

    const totalDuration = performance.now() - this.startTime;
    const passed = this.results.filter(r => r.passed).length;
    const failed = this.results.length - passed;

    console.log(`\n📊 Documentation Examples Summary - ${platform}`);
    console.log('='.repeat(60));
    console.log(`Total Tests: ${this.results.length}`);
    console.log(`✅ Passed: ${passed}`);
    console.log(`❌ Failed: ${failed}`);
    console.log(`⏱️  Duration: ${totalDuration.toFixed(1)}ms`);

    if (failed > 0) {
      console.log('\n❌ Failed Tests:');
      this.results.filter(r => !r.passed).forEach(r => {
        console.log(`  • ${r.name}: ${r.error}`);
      });
      if (exitOnComplete) {
        if (typeof Deno !== 'undefined') { Deno.exit(1); } else { process.exit(1); }
      }
    } else {
      console.log('\n🎉 All documentation examples passed!');
    }

    return {
      platform,
      totalTests: this.results.length,
      passed,
      failed,
      duration: totalDuration,
      results: this.results
    };
  }

  // --- Recipes -------------------------------------------------------------

  private async docSingleTask(): Promise<void> {
    const scheduler = this.makeScheduler();

    const task = new FyflowTask({
      id: 'doc-single',
      workerType: 'DocsWorker',
      payload: { id: 'doc-single', value: 21 }
    });

    // addTask returns a promise ONLY when createPromise is requested
    const result = await scheduler.addTask(task, { createPromise: true });

    if (result.value !== 21) throw new Error(`Expected 21, got ${JSON.stringify(result)}`);
  }

  private async docFireAndForget(): Promise<void> {
    const scheduler = this.makeScheduler();

    // Without createPromise, addTask returns undefined - it does not block
    const returned = scheduler.addTask(new FyflowTask({
      id: 'doc-faf',
      workerType: 'DocsWorker',
      payload: { id: 'doc-faf', value: 1 }
    }));

    if (returned !== undefined) {
      throw new Error('addTask should return undefined without createPromise');
    }

    await this.waitFor(() => scheduler.stats.done === 1, 3000, 'task to complete');
  }

  private async docBatch(): Promise<void> {
    const scheduler = this.makeScheduler();

    const tasks = Array.from({ length: 10 }, (_, i) => new FyflowTask({
      id: `doc-batch-${i}`,
      workerType: 'DocsWorker',
      payload: { id: `doc-batch-${i}`, value: i }
    }));

    const results = await Promise.all(
      scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]
    );

    if (results.length !== 10) throw new Error(`Expected 10 results, got ${results.length}`);
  }

  private async docWorkerConfig(): Promise<void> {
    // `config` is handed to every worker instance's constructor
    const scheduler = this.makeScheduler({ config: { multiplier: 3 } });

    const result = await scheduler.addTask(new FyflowTask({
      id: 'doc-config',
      workerType: 'DocsWorker',
      payload: { id: 'doc-config', value: 5 }
    }), { createPromise: true });

    if (result.value !== 15) throw new Error(`Expected 5 * 3 = 15, got ${result.value}`);
  }

  private async docConcurrentGroup(): Promise<void> {
    const cpu = new ConcurrentLimitGroup(2, 'cpu');
    // Pool capacity is 16, far above the limit, so a peak near the limit can
    // only come from the group. Asserting "8 tasks completed" would have passed
    // with no group at all.
    const scheduler = this.makeScheduler({ groups: ['cpu'], maxThreads: 4, maxConcurrentTasks: 4 }, { cpu });

    let inFlight = 0, peak = 0;
    scheduler.addEventListener('task.running', () => { inFlight++; peak = Math.max(peak, inFlight); });
    scheduler.addEventListener('task.completed', () => { inFlight--; });

    const tasks = Array.from({ length: 8 }, (_, i) => new FyflowTask({
      id: `doc-cpu-${i}`, workerType: 'DocsWorker',
      payload: { id: `doc-cpu-${i}`, value: i, awaitMs: 40 }
    }));
    await Promise.all(scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]);

    if (scheduler.stats.done !== 8) throw new Error(`Expected 8 done, got ${scheduler.stats.done}`);

    // Groups are optimistic, so a brief overshoot is expected - but nothing near
    // the pool's own capacity of 16
    if (peak > 6) {
      throw new Error(`Group did not constrain concurrency: peak ${peak} against a limit of 2`);
    }
    if (cpu.getMetrics().running !== 0) {
      throw new Error(`Expected group drained, got ${cpu.getMetrics().running} running`);
    }
  }

  private async docRateLimitGroup(): Promise<void> {
    // Several windows are enforced together, so the tightest one binds
    const api = new RateLimitGroup([
      { limit: 4, windowMs: 200 },
      { limit: 50, windowMs: 60_000 }
    ], 'api');
    const scheduler = this.makeScheduler({ groups: ['api'], maxThreads: 4, maxConcurrentTasks: 4 }, { api });

    const tasks = Array.from({ length: 12 }, (_, i) => new FyflowTask({
      id: `doc-api-${i}`, workerType: 'DocsWorker', payload: { id: `doc-api-${i}`, value: i }
    }));

    const start = performance.now();
    await Promise.all(scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]);
    const elapsed = performance.now() - start;

    // Everything beyond the limit waits for the window instead of being dropped
    if (scheduler.stats.done !== 12) throw new Error(`Expected 12 done, got ${scheduler.stats.done}`);

    // 12 tasks at 4 per 200ms window need at least two window rollovers. These
    // tasks are otherwise instant - unthrottled the run finishes in ~50ms, so
    // this is what distinguishes a working rate limit from none at all.
    if (elapsed < 400) {
      throw new Error(`Rate limit did not throttle: 12 tasks at 4 per 200ms finished in ${elapsed.toFixed(0)}ms`);
    }
  }

  private async docResourceMonitoring(): Promise<void> {
    const cpu = new ConcurrentLimitGroup(4, 'cpu');
    const scheduler = this.makeScheduler({ groups: ['cpu'] }, { cpu });

    await scheduler.addTask(new FyflowTask({
      id: 'doc-metrics',
      workerType: 'DocsWorker',
      payload: { id: 'doc-metrics', value: 1 }
    }), { createPromise: true });

    const metrics = scheduler.getResourceMetrics();
    if (metrics.cpu.limit !== 4) throw new Error(`Expected limit 4, got ${metrics.cpu.limit}`);
    if (typeof metrics.cpu.available !== 'number') throw new Error('metrics.available missing');
    if (typeof metrics.cpu.utilization !== 'number') throw new Error('metrics.utilization missing');

    const stats = scheduler.getResourceStats();
    if (stats.cpu.totalAcquired < 1) throw new Error('stats.totalAcquired not tracked');
    if (typeof stats.cpu.totalReleased !== 'number') throw new Error('stats.totalReleased missing');
  }

  private async docProgress(): Promise<void> {
    const scheduler = this.makeScheduler();

    const seen: number[] = [];
    const details: any[] = [];
    scheduler.addEventListener('task.progress', (e: any) => {
      // progress is 0-1
      seen.push(e.detail.progress);
      details.push({ ...e.detail });
    });

    await scheduler.addTask(new FyflowTask({
      id: 'doc-progress',
      workerType: 'DocsWorker',
      payload: { id: 'doc-progress', value: 1, reportProgress: 4 }
    }), { createPromise: true });

    if (seen.length !== 4) throw new Error(`Expected 4 progress events, got ${seen.length}`);
    if (seen[seen.length - 1] !== 1) throw new Error(`Expected final progress 1, got ${seen[seen.length - 1]}`);

    // The scheduler's detail is the task spread flat, so the id is `id`. The
    // POOL's task.progress carries `taskId`; the two were documented as one
    // shape for a long time, and README's snippet printed "Task undefined".
    const first = details[0];
    if (first.id !== 'doc-progress') {
      throw new Error(`Expected detail.id "doc-progress", got "${first.id}"`);
    }
    if ('taskId' in first) {
      throw new Error('detail.taskId exists now - the docs say it does not and need updating');
    }
    // `workerType` is the WRAPPER's, not the pool key: the spread puts the pool
    // key there and the forwarder overwrites it. Easy to misread as the task's.
    if (first.workerType !== 'inline' && first.workerType !== 'thread') {
      throw new Error(
        `detail.workerType is "${first.workerType}" - if it is the pool key now, ` +
        `the warning in AGENTS.md section 6 needs updating`
      );
    }
    if (typeof first.timestamp !== 'number') throw new Error('task.progress carried no timestamp');
  }

  private async docSpawning(): Promise<void> {
    const scheduler = this.makeScheduler();

    const spawned: string[] = [];
    scheduler.addEventListener('task.spawn_request', (e: any) => {
      spawned.push(e.detail.spawnConfig.id);
    });

    const task = new FyflowTask({
      id: 'doc-spawn',
      workerType: 'DocsWorker',
      payload: { id: 'doc-spawn', value: 1, spawn: 3 }
    });

    // Must be added before tracking - onCompleteDescendants needs the scheduler
    scheduler.addTask(task);
    await task.onCompleteDescendants();

    if (spawned.length !== 3) throw new Error(`Expected 3 spawns, got ${spawned.length}`);
    if (scheduler.stats.done !== 4) throw new Error(`Expected 4 done (1 + 3), got ${scheduler.stats.done}`);
  }

  private async docRetry(): Promise<void> {
    const scheduler = this.makeScheduler();

    const task = new FyflowTask({
      id: 'doc-retry',
      workerType: 'DocsWorker',
      payload: { id: 'doc-retry', fail: true },
      retryPolicy: { maxRetries: 2, backoffMs: 10 }
    });

    let rejected = false;
    try {
      await scheduler.addTask(task, { createPromise: true });
    } catch {
      rejected = true;
    }

    if (!rejected) throw new Error('Expected the task to reject after exhausting retries');
    if (task.attempts !== 2) throw new Error(`Expected 2 retry attempts, got ${task.attempts}`);
  }

  private async docOptionalTask(): Promise<void> {
    const scheduler = this.makeScheduler();

    // An optional task that fails resolves null instead of rejecting
    const result = await scheduler.addTask(new FyflowTask({
      id: 'doc-optional',
      workerType: 'DocsWorker',
      payload: { id: 'doc-optional', fail: true },
      optional: true
    }), { createPromise: true });

    if (result !== null) throw new Error(`Expected null from a failed optional task, got ${result}`);
  }

  private async docFailure(): Promise<void> {
    const scheduler = this.makeScheduler();

    let failedId = '';
    scheduler.addEventListener('task.failed', (e: any) => { failedId = e.detail.id; });

    const task = new FyflowTask({
      id: 'doc-fail',
      workerType: 'DocsWorker',
      payload: { id: 'doc-fail', fail: true }
    });

    let message = '';
    try {
      await scheduler.addTask(task, { createPromise: true });
    } catch (error: any) {
      message = error.message;
    }

    if (!message.includes('asked to fail')) throw new Error(`Expected the worker error, got "${message}"`);
    if (failedId !== 'doc-fail') throw new Error('task.failed event not emitted for the failing task');
    // A failing non-optional task with no retries left ends in user_action
    if (task.state !== 'user_action') throw new Error(`Expected user_action, got ${task.state}`);
  }

  private async docWorkerStatus(): Promise<void> {
    const pool = new WorkerManager(this.workerUrl, {
      maxThreads: 1, maxConcurrentTasks: 1, inline: true
    });
    const scheduler = new FyflowScheduler({ DocsWorker: pool });
    this.schedulers.push(scheduler);

    await scheduler.addTask(new FyflowTask({
      id: 'doc-status',
      workerType: 'DocsWorker',
      payload: { id: 'doc-status', value: 1 }
    }), { createPromise: true });

    const ids = pool.getWorkerIds();
    if (ids.length !== 1) throw new Error(`Expected 1 worker, got ${ids.length}`);

    const status: WorkerStatus | null = pool.getWorkerStatus(ids[0]);
    if (!status) throw new Error('getWorkerStatus returned null for a live worker');
    if (status.tasksCompleted < 1) throw new Error(`Expected tasksCompleted >= 1, got ${status.tasksCompleted}`);
    if (!Array.isArray(status.currentTasks)) throw new Error('status.currentTasks missing');
    if (typeof status.uptime !== 'number') throw new Error('status.uptime missing');

    const all = pool.getAllWorkerStatuses();
    if (all.size !== 1) throw new Error(`Expected 1 status entry, got ${all.size}`);
  }

  private async docRestartWorker(): Promise<void> {
    const pool = new WorkerManager(this.workerUrl, {
      maxThreads: 1, maxConcurrentTasks: 1, inline: true
    });
    const scheduler = new FyflowScheduler({ DocsWorker: pool });
    this.schedulers.push(scheduler);

    await scheduler.addTask(new FyflowTask({
      id: 'doc-restart-1',
      workerType: 'DocsWorker',
      payload: { id: 'doc-restart-1', value: 2 }
    }), { createPromise: true });

    const originalId = pool.getWorkerIds()[0];

    // Restart with a different worker config
    const restarted = await pool.restartWorker(originalId, { multiplier: 10 });
    if (!restarted) throw new Error('restartWorker returned false');

    const newId = pool.getWorkerIds()[0];
    if (newId === originalId) throw new Error('Worker id unchanged after restart');

    const result = await scheduler.addTask(new FyflowTask({
      id: 'doc-restart-2',
      workerType: 'DocsWorker',
      payload: { id: 'doc-restart-2', value: 2 }
    }), { createPromise: true });

    if (result.value !== 20) throw new Error(`Expected the new config to apply (20), got ${result.value}`);
  }

  // Documented as an alias for restartWorker
  private async docReplaceWorker(): Promise<void> {
    const pool = new WorkerManager(this.workerUrl, {
      maxThreads: 1, maxConcurrentTasks: 1, inline: true
    });
    const scheduler = new FyflowScheduler({ DocsWorker: pool });
    this.schedulers.push(scheduler);

    await scheduler.addTask(new FyflowTask({
      id: 'replace-1', workerType: 'DocsWorker', payload: { id: 'replace-1', value: 2 }
    }), { createPromise: true });

    const originalId = pool.getWorkerIds()[0];
    const replaced = await pool.replaceWorker(originalId, { multiplier: 5 });
    if (!replaced) throw new Error('replaceWorker returned false for a live worker');
    if (pool.getWorkerIds()[0] === originalId) throw new Error('Worker id unchanged after replaceWorker');

    const result = await scheduler.addTask(new FyflowTask({
      id: 'replace-2', workerType: 'DocsWorker', payload: { id: 'replace-2', value: 2 }
    }), { createPromise: true });
    if (result.value !== 10) throw new Error(`Expected the new config to apply (10), got ${result.value}`);

    if (await pool.replaceWorker('no-such-worker')) {
      throw new Error('replaceWorker should return false for an unknown id');
    }
  }

  // updateWorkerConfig merges into the config object a live worker holds. The
  // documented caveat is that a worker which copied values in its constructor -
  // as DocsWorker copies `multiplier` - will not see the change; restartWorker is
  // what rebuilds it.
  private async docUpdateWorkerConfig(): Promise<void> {
    const pool = new WorkerManager(this.workerUrl, {
      maxThreads: 1, maxConcurrentTasks: 1, inline: true, config: { multiplier: 2 }
    });
    const scheduler = new FyflowScheduler({ DocsWorker: pool });
    this.schedulers.push(scheduler);

    const before = await scheduler.addTask(new FyflowTask({
      id: 'upd-1', workerType: 'DocsWorker', payload: { id: 'upd-1', value: 3 }
    }), { createPromise: true });
    if (before.value !== 6) throw new Error(`Expected 3 * 2 = 6, got ${before.value}`);

    const workerId = pool.getWorkerIds()[0];
    if (!await pool.updateWorkerConfig(workerId, { multiplier: 10 })) {
      throw new Error('updateWorkerConfig returned false for a live worker');
    }
    if (await pool.updateWorkerConfig('no-such-worker', {})) {
      throw new Error('updateWorkerConfig should return false for an unknown id');
    }

    const after = await scheduler.addTask(new FyflowTask({
      id: 'upd-2', workerType: 'DocsWorker', payload: { id: 'upd-2', value: 3 }
    }), { createPromise: true });

    // The caveat, asserted: the running instance copied multiplier at construction
    if (after.value !== 6) {
      throw new Error(
        `A worker that copied config in its constructor should not see updateWorkerConfig ` +
        `(expected 6, got ${after.value}) - if this changed, the documented caveat is wrong`
      );
    }
  }

  private async docTaskStartedEvent(): Promise<void> {
    const pool = new WorkerManager(this.workerUrl, {
      maxThreads: 1, maxConcurrentTasks: 1, inline: true
    });
    const scheduler = new FyflowScheduler({ DocsWorker: pool });
    this.schedulers.push(scheduler);

    // task.started is a pool event - the scheduler emits task.running instead
    const started: string[] = [];
    pool.addEventListener('task.started', (e: any) => started.push(e.detail.taskId));

    await scheduler.addTask(new FyflowTask({
      id: 'started', workerType: 'DocsWorker', payload: { id: 'started', value: 1 }
    }), { createPromise: true });

    if (started.length !== 1 || started[0] !== 'started') {
      throw new Error(`Expected one task.started for 'started', got ${JSON.stringify(started)}`);
    }
  }

  private async docTeardownFailure(): Promise<void> {
    const crashUrl = typeof Deno !== "undefined"
      ? new URL("../workers/crashingWorker.ts", import.meta.url).href
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      : new URL((await import('../workers/crashingWorker.ts?worker-direct')).default).href;

    // Both pool types, deliberately. This test used to cover inline only, which
    // is why threaded pools could emit worker.teardown.started and then nothing:
    // a worker whose teardown() throws replies with an error rather than the
    // teardown acknowledgement, so 'failed' was documented but unreachable and
    // terminate() sat out its full acknowledgement timeout.
    for (const inline of [true, false]) {
      const kind = inline ? 'inline' : 'threaded';
      const pool = new WorkerManager(crashUrl, {
        maxThreads: 1, maxConcurrentTasks: 1, inline, config: { crashOnTeardown: true }
      });
      const scheduler = new FyflowScheduler({ CrashingWorker: pool });

      let teardownFailed = 0;
      let teardownStarted = 0;
      let failureDetail: any = null;
      pool.addEventListener('worker.teardown.started', () => { teardownStarted++; });
      pool.addEventListener('worker.teardown.failed', (e: any) => {
        teardownFailed++;
        failureDetail = e.detail;
      });

      await scheduler.addTask(new FyflowTask({
        id: `teardown-${inline}`, workerType: 'CrashingWorker', payload: { action: 'normal-task' }
      }), { createPromise: true });

      // Shutdown tears the worker down, and its teardown throws
      await scheduler.shutdown();

      if (teardownFailed !== 1) {
        throw new Error(`${kind}: expected 1 worker.teardown.failed, got ${teardownFailed}`);
      }
      // Every started must reach a terminal event, or a consumer folding these
      // into worker state is left holding a worker that never finished tearing down
      if (teardownStarted !== 1) {
        throw new Error(`${kind}: expected 1 worker.teardown.started, got ${teardownStarted}`);
      }
      if (failureDetail?.workerType !== (inline ? 'inline' : 'thread')) {
        throw new Error(`${kind}: worker.teardown.failed has workerType ${failureDetail?.workerType}`);
      }
      if (!failureDetail?.error) {
        throw new Error(`${kind}: worker.teardown.failed carries no error`);
      }
    }

    // A worker that neither completes nor throws must still close the pair. The
    // wrapper's acknowledgement timeout has to stay below WorkerManager's own
    // termination timeout, or the manager abandons the terminate() it is
    // awaiting and the failure it reports is never observed by anyone.
    const hangPool = new WorkerManager(crashUrl, {
      maxThreads: 1, maxConcurrentTasks: 1, inline: false, config: { hangOnTeardown: true }
    });
    const hangScheduler = new FyflowScheduler({ CrashingWorker: hangPool });

    const hangSeen: string[] = [];
    for (const event of ['worker.teardown.started', 'worker.teardown.completed', 'worker.teardown.failed']) {
      hangPool.addEventListener(event, () => hangSeen.push(event.replace('worker.teardown.', '')));
    }

    await hangScheduler.addTask(new FyflowTask({
      id: 'teardown-hang', workerType: 'CrashingWorker', payload: { action: 'normal-task' }
    }), { createPromise: true });

    await hangScheduler.shutdown();

    if (!hangSeen.includes('started')) {
      throw new Error('hanging teardown: worker.teardown.started did not fire');
    }
    if (!hangSeen.includes('failed')) {
      throw new Error(
        `hanging teardown: expected worker.teardown.failed once the worker stopped ` +
        `answering, saw [${hangSeen.join(', ') || 'nothing'}]. A started with no ` +
        `terminal event leaves a consumer holding a worker that never finished.`
      );
    }
  }

  private async docSelfTermination(): Promise<void> {
    const pool = new WorkerManager(this.workerUrl, {
      maxThreads: 1, maxConcurrentTasks: 1, inline: true
    });
    const scheduler = new FyflowScheduler({ DocsWorker: pool });
    this.schedulers.push(scheduler);

    let selfTerminated = false;
    pool.addEventListener('worker.self_terminated', () => { selfTerminated = true; });

    scheduler.addTask(new FyflowTask({
      id: 'doc-selfterm',
      workerType: 'DocsWorker',
      payload: { id: 'doc-selfterm', value: 1, selfTerminate: true }
    }));

    await this.waitFor(() => selfTerminated, 3000, 'worker.self_terminated');
  }

  private async docLifecycleEvents(): Promise<void> {
    // Inline and threaded pools emit the SAME lifecycle events with the same
    // payload shape - the point of forwarding them at all
    for (const inline of [true, false]) {
      const pool = new WorkerManager(this.workerUrl, {
        maxThreads: 1, maxConcurrentTasks: 1, inline
      });
      const scheduler = new FyflowScheduler({ DocsWorker: pool });
      this.schedulers.push(scheduler);

      const expected = [
        'worker.initialization.started',
        'worker.initialization.completed',
        'worker.setup.started',
        'worker.setup.completed'
      ];
      const seen = new Map<string, any>();
      for (const event of [...expected, 'worker.teardown.started', 'worker.teardown.completed']) {
        pool.addEventListener(event, (e: any) => seen.set(event, e.detail));
      }

      await scheduler.addTask(new FyflowTask({
        id: `doc-lifecycle-${inline}`,
        workerType: 'DocsWorker',
        payload: { id: `doc-lifecycle-${inline}`, value: 1 }
      }), { createPromise: true });

      const kind = inline ? 'inline' : 'threaded';
      for (const event of expected) {
        const detail = seen.get(event);
        if (!detail) throw new Error(`${kind}: expected ${event}`);
        if (!detail.workerId) throw new Error(`${kind}: ${event} missing workerId`);
        if (typeof detail.timestamp !== 'number') throw new Error(`${kind}: ${event} missing timestamp`);
        if (detail.workerType !== (inline ? 'inline' : 'thread')) {
          throw new Error(`${kind}: ${event} has workerType ${detail.workerType}`);
        }
        if (event.endsWith('.completed') && typeof detail.duration !== 'number') {
          throw new Error(`${kind}: ${event} missing duration`);
        }
      }

      // Teardown events arrive on shutdown
      await scheduler.shutdown();
      for (const event of ['worker.teardown.started', 'worker.teardown.completed']) {
        if (!seen.has(event)) throw new Error(`${kind}: expected ${event} on shutdown`);
      }
    }
  }

  private async docRetention(): Promise<void> {
    const scheduler = this.makeScheduler({}, {}, { maxCompletedTasks: 3 });

    const tasks = Array.from({ length: 10 }, (_, i) => new FyflowTask({
      id: `doc-retain-${i}`,
      workerType: 'DocsWorker',
      payload: { id: `doc-retain-${i}`, value: i }
    }));
    await Promise.all(scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]);

    if (scheduler.tasks.size !== 3) throw new Error(`Expected 3 retained, got ${scheduler.tasks.size}`);
    if (scheduler.stats.done !== 10) throw new Error(`Expected stats.done 10, got ${scheduler.stats.done}`);
  }

  private async docSchedulerCompleted(): Promise<void> {
    const scheduler = this.makeScheduler();

    const completed = new Promise<any>(resolve => {
      scheduler.addEventListener('scheduler.completed', (e: any) => resolve(e.detail));
    });

    for (let i = 0; i < 5; i++) {
      scheduler.addTask(new FyflowTask({
        id: `doc-done-${i}`,
        workerType: 'DocsWorker',
        payload: { id: `doc-done-${i}`, value: i }
      }));
    }

    const stats = await completed;
    if (stats.done !== 5) throw new Error(`Expected 5 done, got ${stats.done}`);
  }

  // Both addTask and addTasks reject an unknown worker type, and addTasks
  // validates the whole batch before queueing any of it
  private async docUnknownWorkerType(): Promise<void> {
    const scheduler = this.makeScheduler();

    let threw = false;
    try {
      scheduler.addTask(new FyflowTask({ id: 'doc-bad-1', workerType: 'NoSuchWorker', payload: {} }));
    } catch (error: any) {
      threw = error.message.includes('Unknown worker type');
    }
    if (!threw) throw new Error('addTask should throw on an unknown workerType');

    // A valid task alongside an invalid one - the whole call must be rejected
    let batchThrew = false;
    try {
      scheduler.addTasks([
        new FyflowTask({ id: 'doc-bad-good', workerType: 'DocsWorker', payload: { id: 'doc-bad-good', value: 1 } }),
        new FyflowTask({ id: 'doc-bad-2', workerType: 'NoSuchWorker', payload: {} })
      ]);
    } catch (error: any) {
      batchThrew = error.message.includes('Unknown worker type');
    }
    if (!batchThrew) throw new Error('addTasks should throw on an unknown workerType');

    // Nothing from the rejected batch may be queued or registered
    if (scheduler.stats.queued !== 0) {
      throw new Error(`Rejected batch left ${scheduler.stats.queued} queued task(s)`);
    }
    if (scheduler.tasks.has('doc-bad-good') || scheduler.tasks.has('doc-bad-2')) {
      throw new Error('Rejected batch left orphan entries in the task map');
    }
  }

  // For an inline pool, maxThreads counts worker INSTANCES in the main process,
  // not threads. Only maxThreads x maxConcurrentTasks bounds concurrency, so
  // 4x5 and 1x20 behave the same - the documented table depends on this
  private async docInlineConcurrency(): Promise<void> {
    const peakFor = async (maxThreads: number, maxConcurrentTasks: number) => {
      const pool = new WorkerManager(this.workerUrl, {
        maxThreads, maxConcurrentTasks, inline: true, idleTimeout: 0
      });
      const scheduler = new FyflowScheduler({ DocsWorker: pool });
      this.schedulers.push(scheduler);

      let inFlight = 0, peak = 0;
      scheduler.addEventListener('task.running', () => { inFlight++; peak = Math.max(peak, inFlight); });
      scheduler.addEventListener('task.completed', () => { inFlight--; });

      const tasks = Array.from({ length: 60 }, (_, i) => new FyflowTask({
        id: `conc-${maxThreads}x${maxConcurrentTasks}-${i}`,
        workerType: 'DocsWorker',
        payload: { id: `conc-${i}`, value: i, awaitMs: 30 }
      }));
      await Promise.all(scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]);
      return { peak, instances: pool.getWorkerIds().length };
    };

    const split = await peakFor(4, 5);
    const single = await peakFor(1, 20);

    // Instances really are created - and they are not threads
    if (split.instances !== 4) throw new Error(`Expected 4 inline instances, got ${split.instances}`);
    if (single.instances !== 1) throw new Error(`Expected 1 inline instance, got ${single.instances}`);

    // Both configurations reach the same ceiling: the product, 20
    if (split.peak !== 20) throw new Error(`4x5 should peak at 20 in-flight, got ${split.peak}`);
    if (single.peak !== 20) throw new Error(`1x20 should peak at 20 in-flight, got ${single.peak}`);
  }

  // Registering a group on the scheduler does nothing on its own - the pool (or
  // the task) has to declare it. The Quick Start in README.md depends on this
  private async docGroupsMustBeDeclared(): Promise<void> {
    const peakWith = async (declare: boolean) => {
      const group = new ConcurrentLimitGroup(2, 'cpu');
      const pool = new WorkerManager(this.workerUrl, {
        maxThreads: 4, maxConcurrentTasks: 4, inline: true, idleTimeout: 0,
        ...(declare ? { groups: ['cpu'] } : {})
      });
      const scheduler = new FyflowScheduler({ DocsWorker: pool }, { cpu: group });
      this.schedulers.push(scheduler);

      let inFlight = 0, peak = 0;
      scheduler.addEventListener('task.running', () => { inFlight++; peak = Math.max(peak, inFlight); });
      scheduler.addEventListener('task.completed', () => { inFlight--; });

      const tasks = Array.from({ length: 40 }, (_, i) => new FyflowTask({
        id: `grp-${declare}-${i}`,
        workerType: 'DocsWorker',
        payload: { id: `grp-${i}`, value: i, awaitMs: 25 }
      }));
      await Promise.all(scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]);
      return peak;
    };

    const undeclared = await peakWith(false);
    const declared = await peakWith(true);

    // Undeclared: the limit of 2 is ignored entirely, so the pool's own capacity
    // (4 x 4) is the only bound
    if (undeclared <= 4) {
      throw new Error(`An undeclared group appears to constrain now (peak ${undeclared}) - docs need updating`);
    }
    // Declared: bounded by the limit, allowing for documented optimistic overshoot
    if (declared > 4) {
      throw new Error(`Declared group did not constrain: peak ${declared} for a limit of 2`);
    }
  }

  // Each key gets its own bucket, so N keys can run N x limit concurrently
  private async docKeyedRateLimit(): Promise<void> {
    const api = new KeyedRateLimitGroup(
      [{ limit: 2, windowMs: 200 }],
      { id: 'api', keyFrom: (t: any) => t.payload.endpoint }
    );
    const scheduler = this.makeScheduler({ groups: ['api'], maxThreads: 4, maxConcurrentTasks: 4 }, { api });

    const perKey = new Map<string, number>();
    let inFlight = 0, peak = 0;
    scheduler.addEventListener('task.running', (e: any) => {
      inFlight++; peak = Math.max(peak, inFlight);
      const k = e.detail.payload.endpoint;
      perKey.set(k, (perKey.get(k) ?? 0) + 1);
    });
    scheduler.addEventListener('task.completed', (e: any) => {
      inFlight--;
      const k = e.detail.payload.endpoint;
      perKey.set(k, (perKey.get(k) ?? 1) - 1);
    });

    const tasks = [];
    for (const endpoint of ['search', 'orders', 'users']) {
      for (let i = 0; i < 6; i++) {
        tasks.push(new FyflowTask({
          id: `keyed-${endpoint}-${i}`,
          workerType: 'DocsWorker',
          payload: { id: `keyed-${endpoint}-${i}`, value: i, endpoint, awaitMs: 30 }
        }));
      }
    }
    await Promise.all(scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]);

    if (scheduler.stats.done !== 18) throw new Error(`Expected 18 done, got ${scheduler.stats.done}`);
    // 3 keys x limit 2 means the total can exceed any single key's limit
    if (peak <= 2) {
      throw new Error(`Keys are not getting independent buckets - peak was ${peak} across 3 keys`);
    }
  }

  // A saturated key must not delay tasks belonging to another key
  private async docKeyedIsolation(): Promise<void> {
    const api = new KeyedRateLimitGroup(
      [{ limit: 1, windowMs: 400 }],
      { id: 'api', keyFrom: (t: any) => t.payload.endpoint }
    );
    const scheduler = this.makeScheduler({ groups: ['api'], maxThreads: 4, maxConcurrentTasks: 4 }, { api });

    // 'hot' saturates its own bucket for the whole window
    const hot = Array.from({ length: 5 }, (_, i) => new FyflowTask({
      id: `hot-${i}`,
      workerType: 'DocsWorker',
      payload: { id: `hot-${i}`, value: i, endpoint: 'hot', awaitMs: 10 }
    }));
    // 'cold' is added behind them and must not wait for 'hot' to drain
    const cold = new FyflowTask({
      id: 'cold-0',
      workerType: 'DocsWorker',
      payload: { id: 'cold-0', value: 0, endpoint: 'cold', awaitMs: 10 }
    });

    scheduler.addTasks(hot);
    const start = performance.now();
    await scheduler.addTask(cold, { createPromise: true });
    const coldWait = performance.now() - start;

    // Sharing a queue with 'hot' would mean waiting out its 400ms windows
    if (coldWait > 250) {
      throw new Error(`A saturated key delayed another key: cold task waited ${coldWait.toFixed(0)}ms`);
    }
  }

  // A task with no derivable key is rejected rather than silently sharing a bucket
  private async docKeyedRequiresKey(): Promise<void> {
    const api = new KeyedRateLimitGroup([{ limit: 5, windowMs: 100 }], { id: 'api' });
    const scheduler = this.makeScheduler({ groups: ['api'] }, { api });

    let message = '';
    try {
      scheduler.addTask(new FyflowTask({
        id: 'no-key', workerType: 'DocsWorker', payload: { id: 'no-key', value: 1 }
      }));
    } catch (error: any) {
      message = error.message;
    }
    if (!message.includes('Missing limit key')) {
      throw new Error(`Expected a missing-key error, got "${message || 'no error'}"`);
    }
    if (scheduler.stats.queued !== 0) throw new Error('Rejected task was still queued');

    // The default keyFrom reads task.limitKey
    const result = await scheduler.addTask(new FyflowTask({
      id: 'with-key', workerType: 'DocsWorker', limitKey: 'tenant-a',
      payload: { id: 'with-key', value: 7 }
    }), { createPromise: true });
    if (result.value !== 7) throw new Error('Task with a limitKey did not run');
  }

  private async docKeyedMetrics(): Promise<void> {
    const api = new KeyedRateLimitGroup(
      [{ limit: 4, windowMs: 50 }],
      { id: 'api', keyFrom: (t: any) => t.payload.endpoint, idleKeyTtlMs: 60 }
    );
    const scheduler = this.makeScheduler({ groups: ['api'] }, { api });

    const tasks = ['a', 'b'].flatMap(endpoint => [0, 1].map(i => new FyflowTask({
      id: `m-${endpoint}-${i}`,
      workerType: 'DocsWorker',
      payload: { id: `m-${endpoint}-${i}`, value: i, endpoint }
    })));
    await Promise.all(scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]);

    const metrics = api.getMetrics();
    if (metrics.limit !== 4) throw new Error(`Expected per-key limit 4, got ${metrics.limit}`);
    if (metrics.activeKeys !== 2) throw new Error(`Expected 2 active keys, got ${metrics.activeKeys}`);
    if (api.getKeyMetrics('a').limit !== 4) throw new Error('getKeyMetrics returned the wrong limit');

    // Idle keys are evicted so high-cardinality keys cannot grow without bound
    await new Promise(resolve => setTimeout(resolve, 150));
    if (api.getMetrics().activeKeys !== 0) {
      throw new Error(`Idle keys were not evicted: ${api.getActiveKeys().join(',')}`);
    }
  }

  // AGENTS.md is the reference an agent reads before writing code against this
  // library, so a new export that never reaches it is invisible to that audience.
  // Deno-only: the Node build runs from dev-dist and has no repo-relative access
  // to the markdown.
  /**
   * AGENTS.md section 6 warns that a `task.*` detail IS the live `FyflowTask`,
   * and that keeping the reference logs whatever the task holds later. This
   * pins that contract in both directions: keeping goes stale, projecting does
   * not, and `scheduler.completed` is exempt because it hands out a copy.
   *
   * If someone ever decides to snapshot details after all, this test fails and
   * the warning gets deleted with the change rather than outliving it.
   */
  // AGENTS.md: "Inline and threaded pools emit the same set with the same shape.
  // These fire on every worker creation and every idle-timeout teardown."
  //
  // teardown() is OPTIONAL on WorkerInterface, but the events are not: they say a
  // worker is being destroyed, which is true whether or not it implemented a
  // hook. A consumer tracking worker state has to be able to rely on that.
  //
  // Inline pools used to guard the events on the method existing, so a worker
  // without teardown() was destroyed silently while a threaded one emitted both.
  // Every other worker in tests/workers/ implements teardown(), which is why
  // nothing caught it.
  private async docTeardownEventsAreUnconditional(): Promise<void> {
    const workerUrl = await this.resolveNoTeardownWorkerUrl();

    for (const inline of [true, false]) {
      const kind = inline ? 'inline' : 'threaded';
      const pool = new WorkerManager(workerUrl, {
        maxThreads: 1, maxConcurrentTasks: 1, inline
      });
      const scheduler = new FyflowScheduler({ NoTeardownWorker: pool });
      this.schedulers.push(scheduler);

      const seen = new Map<string, any>();
      for (const event of ['worker.teardown.started', 'worker.teardown.completed', 'worker.teardown.failed']) {
        pool.addEventListener(event, (e: any) => seen.set(event, e.detail ?? {}));
      }

      await scheduler.addTask(new FyflowTask({
        id: `doc-teardown-optional-${inline}`,
        workerType: 'NoTeardownWorker',
        payload: { id: `doc-teardown-optional-${inline}`, value: 1 }
      }), { createPromise: true });

      await scheduler.shutdown();

      if (seen.has('worker.teardown.failed')) {
        throw new Error(`${kind}: a missing teardown() is not a failure, but worker.teardown.failed fired`);
      }

      for (const event of ['worker.teardown.started', 'worker.teardown.completed']) {
        const detail = seen.get(event);
        if (!detail) {
          throw new Error(
            `${kind}: ${event} did not fire for a worker without teardown(). ` +
            `The events report that a worker is being destroyed, not that it implemented a hook.`
          );
        }
        if (!detail.workerId) throw new Error(`${kind}: ${event} missing workerId`);
        if (typeof detail.timestamp !== 'number') throw new Error(`${kind}: ${event} missing timestamp`);
        if (detail.workerType !== (inline ? 'inline' : 'thread')) {
          throw new Error(`${kind}: ${event} has workerType ${detail.workerType}`);
        }
      }

      if (typeof seen.get('worker.teardown.completed').duration !== 'number') {
        throw new Error(`${kind}: worker.teardown.completed missing duration`);
      }
    }
  }

  private async docDetailsAreLive(): Promise<void> {
    const scheduler = this.makeScheduler();

    // RECIPE: the natural thing to write, and why it is wrong
    const kept: any[] = [];
    const projected: Array<{ id: string; state: string; timestamp: number }> = [];

    scheduler.addEventListener('task.running', (e: any) => {
      kept.push(e.detail);                       // a reference to a live object
      projected.push({                           // a value, read synchronously
        id: e.detail.id,
        state: e.detail.state,
        timestamp: e.detail.timestamp
      });
    });

    let completedDetail: any;
    scheduler.addEventListener('scheduler.completed', (e: any) => { completedDetail = e.detail; });

    await scheduler.addTask(new FyflowTask({
      id: 'doc-live-detail',
      workerType: 'DocsWorker',
      payload: { id: 'doc-live-detail', value: 1 }
    }), { createPromise: true });

    if (kept.length === 0) throw new Error('task.running never fired');

    // The kept reference now reads the terminal state, not the running one
    if (kept[0].state !== 'done') {
      throw new Error(
        `The kept detail reads state "${kept[0].state}" - if it is no longer live, ` +
        `the warning in AGENTS.md section 6 needs deleting`
      );
    }
    // The projection captured the state at emit and cannot drift
    if (projected[0].state !== 'running') {
      throw new Error(`The projection captured "${projected[0].state}", expected "running"`);
    }
    if (typeof projected[0].timestamp !== 'number') {
      throw new Error('task.running carried no timestamp');
    }

    await this.waitFor(() => completedDetail !== undefined, 2000, 'scheduler.completed');

    // scheduler.completed is exempt: its detail is a copy of stats, not the
    // live counters object, so holding two of them does not hold one twice
    if (completedDetail === (scheduler as any).stats) {
      throw new Error('scheduler.completed handed out the live stats object');
    }
    if (typeof completedDetail.timestamp !== 'number') {
      throw new Error('scheduler.completed carried no timestamp');
    }
  }

  private async docExportsDocumented(): Promise<void> {
    if (typeof Deno === 'undefined') return; // Covered by the Deno run

    const root = new URL('../../', import.meta.url);
    const indexSource = await Deno.readTextFile(new URL('index.ts', root));
    const agents = await Deno.readTextFile(new URL('AGENTS.md', root));

    const exported = new Set<string>();
    for (const match of indexSource.matchAll(/export (?:type )?\{([^}]+)\}/gs)) {
      for (const raw of match[1].split(',')) {
        // `X as Y` exports Y - taking the left side would miss renamed exports
        const name = raw.trim().split(' as ').pop()!.trim();
        if (name) exported.add(name);
      }
    }
    if (exported.size < 20) {
      throw new Error(`Only parsed ${exported.size} exports from index.ts - the parser is wrong, not the docs`);
    }

    const undocumented = [...exported].filter(name => !agents.includes(name)).sort();
    if (undocumented.length > 0) {
      throw new Error(`Exported but absent from AGENTS.md: ${undocumented.join(', ')}`);
    }
  }

  // --- helpers -------------------------------------------------------------

  private async waitFor(predicate: () => boolean, timeoutMs: number, what: string): Promise<void> {
    const start = performance.now();
    while (!predicate()) {
      if (performance.now() - start > timeoutMs) {
        throw new Error(`Timed out after ${timeoutMs}ms waiting for ${what}`);
      }
      await new Promise(r => setTimeout(r, 20));
    }
  }
}

// Auto-run tests when executed directly - handle both Deno and Node.js
if ((typeof Deno !== 'undefined' && import.meta.main) ||
    (typeof process !== 'undefined' && process.argv[1] && import.meta.url &&
     import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/')))) {
  const testSuite = new DocsTestSuite();
  try {
    await testSuite.runAllTests(true);
  } finally {
    await testSuite.cleanup();
  }
}

export { DocsTestSuite };
export default DocsTestSuite;
