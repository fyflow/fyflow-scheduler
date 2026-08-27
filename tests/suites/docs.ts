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
  RateLimitGroup
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
    this.results.push(await this.runTest('Doc: Worker Self-Termination', () => this.docSelfTermination()));
    this.results.push(await this.runTest('Doc: Worker Lifecycle Events', () => this.docLifecycleEvents()));
    this.results.push(await this.runTest('Doc: Bounded Task Retention', () => this.docRetention()));
    this.results.push(await this.runTest('Doc: Waiting For The Whole Run', () => this.docSchedulerCompleted()));
    this.results.push(await this.runTest('Doc: Unknown Worker Type Behaviour', () => this.docUnknownWorkerType()));

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
    const scheduler = this.makeScheduler({ groups: ['cpu'] }, { cpu });

    const tasks = Array.from({ length: 8 }, (_, i) => new FyflowTask({
      id: `doc-cpu-${i}`,
      workerType: 'DocsWorker',
      payload: { id: `doc-cpu-${i}`, value: i }
    }));
    await Promise.all(scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]);

    if (scheduler.stats.done !== 8) throw new Error(`Expected 8 done, got ${scheduler.stats.done}`);
    // Slots are returned when tasks finish
    if (cpu.getMetrics().running !== 0) {
      throw new Error(`Expected group drained, got ${cpu.getMetrics().running} running`);
    }
  }

  private async docRateLimitGroup(): Promise<void> {
    // Several windows are enforced together
    const api = new RateLimitGroup([
      { limit: 4, windowMs: 200 },
      { limit: 50, windowMs: 60_000 }
    ], 'api');
    const scheduler = this.makeScheduler({ groups: ['api'] }, { api });

    const tasks = Array.from({ length: 12 }, (_, i) => new FyflowTask({
      id: `doc-api-${i}`,
      workerType: 'DocsWorker',
      payload: { id: `doc-api-${i}`, value: i }
    }));
    await Promise.all(scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]);

    // Everything beyond the limit waits for the window instead of being dropped
    if (scheduler.stats.done !== 12) throw new Error(`Expected 12 done, got ${scheduler.stats.done}`);
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
    scheduler.addEventListener('task.progress', (e: any) => {
      // progress is 0-1
      seen.push(e.detail.progress);
    });

    await scheduler.addTask(new FyflowTask({
      id: 'doc-progress',
      workerType: 'DocsWorker',
      payload: { id: 'doc-progress', value: 1, reportProgress: 4 }
    }), { createPromise: true });

    if (seen.length !== 4) throw new Error(`Expected 4 progress events, got ${seen.length}`);
    if (seen[seen.length - 1] !== 1) throw new Error(`Expected final progress 1, got ${seen[seen.length - 1]}`);
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
