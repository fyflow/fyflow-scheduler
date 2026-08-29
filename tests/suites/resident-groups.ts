// Resident resource group tests.
//
// A resource group is scoped to a task: acquired at dispatch, released when the
// task settles. That cannot express a resource a worker holds because it
// *exists* - a model loaded on a GPU in setup(), a connection, a licence seat.
// The worker outlives its task by the idle timeout, so a second pool could load
// a second model while the first was still resident, even with a
// ConcurrentLimitGroup(1) that was behaving perfectly correctly.
//
// `residentGroups` holds from worker creation to teardown, with a per-worker
// cost, so a 20-unit model and four 2-unit models share 24 units without ever
// exceeding them.
//
// Assertions here are on framework observables - group metrics and pool
// residency - never on state shared with the worker module, which does not
// survive the Node bundle split.

import { WorkerManager, FyflowScheduler, FyflowTask, ConcurrentLimitGroup } from '../../index.ts';

// Node.js process declaration for cross-platform compatibility
declare const process: any;

interface TestResult { name: string; passed: boolean; duration: number; error?: string; }
interface TestSuiteResult {
  platform: string; totalTests: number; passed: number; failed: number;
  duration: number; results: TestResult[];
}

class ResidentGroupsTestSuite {
  private results: TestResult[] = [];
  private startTime = 0;
  private inlineWorkerUrl = "";
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

  /** A pool that holds `cost` units of `groupId` per worker. */
  private pool(cost: number | undefined, extra: Record<string, any> = {}) {
    return new WorkerManager(this.inlineWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: true,
      idleTimeout: 30,
      idleCheckIntervalMs: 15,
      ...(cost === undefined ? {} : { residentGroups: { vram: cost } }),
      ...extra
    });
  }

  private task(id: string, workerType: string, delay = 40) {
    return new FyflowTask({ id, workerType, payload: { taskId: id, delay } });
  }

  /** Samples a group's usage until `done`, returning the highest seen. */
  private async peakWhile(group: ConcurrentLimitGroup, done: Promise<any>): Promise<number> {
    let peak = 0;
    const sample = () => { peak = Math.max(peak, group.running); };
    const timer = setInterval(sample, 1);
    try { await done; } finally { clearInterval(timer); sample(); }
    return peak;
  }

  async runAllTests(_exitOnComplete = true): Promise<TestSuiteResult> {
    if (typeof Deno !== "undefined") {
      this.inlineWorkerUrl = new URL("../workers/testInlineWorker.ts", import.meta.url).href;
    } else {
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      this.inlineWorkerUrl = new URL((await import('../workers/testInlineWorker.ts?worker-direct')).default).href;
    }

    const platform = typeof globalThis !== 'undefined' && 'Deno' in globalThis ? 'Deno' : 'Node.js';
    console.log(`🚀 FyFlow Resident Group Tests - ${platform}`);
    console.log('='.repeat(60));

    this.startTime = performance.now();
    this.results = [];

    this.results.push(await this.runTest('Resident Slot Outlives The Task', () => this.testSlotOutlivesTask()));
    this.results.push(await this.runTest('Task Groups Release At Settle', () => this.testTaskGroupReleasesAtSettle()));
    this.results.push(await this.runTest('Two Pools Never Both Hold A Limit-1 Group', () => this.testMutualExclusion()));
    this.results.push(await this.runTest('Weighted Costs Never Exceed The Limit', () => this.testWeightedNeverExceeds()));
    this.results.push(await this.runTest('Affinity - A Run Of Tasks Keeps One Worker', () => this.testAffinity()));
    this.results.push(await this.runTest('Cost Is Per Worker, Not Per Pool', () => this.testCostPerWorker()));
    this.results.push(await this.runTest('Shutdown Releases Every Unit', () => this.testShutdownReleases()));
    this.results.push(await this.runTest('Array Form Means Cost 1', () => this.testArrayForm()));
    this.results.push(await this.runTest('Invalid Costs Are Rejected', () => this.testInvalidCosts()));
    this.results.push(await this.runTest('Cost Above The Group Limit Is Rejected', () => this.testCostAboveLimit()));
    this.results.push(await this.runTest('Unknown Resident Group Is Rejected', () => this.testUnknownGroup()));
    this.results.push(await this.runTest('No Resident Groups Leaves Behaviour Unchanged', () => this.testOptIn()));

    const totalDuration = performance.now() - this.startTime;
    const passed = this.results.filter(r => r.passed).length;
    const failed = this.results.length - passed;

    console.log(`\n📊 Resident Group Test Summary - ${platform}`);
    console.log('='.repeat(60));
    console.log(`Total Tests: ${this.results.length}`);
    console.log(`✅ Passed: ${passed}`);
    console.log(`❌ Failed: ${failed}`);
    console.log(`⏱️  Duration: ${totalDuration.toFixed(1)}ms`);

    if (failed > 0) {
      console.log('\n❌ Failed Tests:');
      this.results.filter(r => !r.passed).forEach(r => console.log(`  • ${r.name}: ${r.error}`));
    }

    return {
      platform, totalTests: this.results.length, passed, failed,
      duration: totalDuration, results: this.results
    };
  }

  /**
   * The whole point of the feature: the slot is still held once the task that
   * caused the worker to exist has settled.
   */
  private async testSlotOutlivesTask(): Promise<void> {
    const vram = new ConcurrentLimitGroup(1, 'vram');
    const scheduler = this.track(new FyflowScheduler({ A: this.pool(1) }, { vram }));

    await scheduler.addTask(this.task('a1', 'A'), { createPromise: true });

    // Checked synchronously after settle, so this cannot race the idle sweep.
    if (vram.running !== 1) {
      throw new Error(`Expected the worker to still hold 1 unit after its task settled, got ${vram.running}`);
    }
  }

  /** The contrast, and why a task-scoped group cannot express this. */
  private async testTaskGroupReleasesAtSettle(): Promise<void> {
    const vram = new ConcurrentLimitGroup(1, 'vram');
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool(undefined, { groups: ['vram'] }) }, { vram }
    ));

    await scheduler.addTask(this.task('a1', 'A'), { createPromise: true });

    if (vram.running !== 0) {
      throw new Error(`A task-scoped group should be free once the task settled, got ${vram.running}`);
    }
  }

  /**
   * Two pools, one unit. The second worker must not exist while the first does,
   * which is what stops a second model loading onto a full GPU.
   */
  private async testMutualExclusion(): Promise<void> {
    const vram = new ConcurrentLimitGroup(1, 'vram');
    const a = this.pool(1);
    const b = this.pool(1);
    const scheduler = this.track(new FyflowScheduler({ A: a, B: b }, { vram }));

    // Count live WORKERS, not group units. Under task-scoped groups the unit
    // count never exceeds 1 either - the violation was two workers existing at
    // once, each holding a model, which is exactly what this must catch.
    let peakWorkers = 0;
    const sample = () => {
      peakWorkers = Math.max(peakWorkers, a.getWorkerIds().length + b.getWorkerIds().length);
    };
    const timer = setInterval(sample, 1);
    try {
      await Promise.all([
        scheduler.addTask(this.task('a1', 'A'), { createPromise: true }),
        scheduler.addTask(this.task('b1', 'B'), { createPromise: true })
      ]);
    } finally { clearInterval(timer); sample(); }

    if (peakWorkers > 1) {
      throw new Error(`Two workers were alive at once under a limit-1 resident group (peak ${peakWorkers})`);
    }
  }

  /**
   * One 20-unit worker and four 2-unit workers over 24 units. The sum must never
   * exceed the limit, and must actually reach it - otherwise the test proves
   * nothing about packing.
   */
  private async testWeightedNeverExceeds(): Promise<void> {
    const vram = new ConcurrentLimitGroup(24, 'vram');
    const pools: Record<string, WorkerManager> = { Big: this.pool(20) };
    for (let i = 0; i < 4; i++) pools[`S${i}`] = this.pool(2);
    const scheduler = this.track(new FyflowScheduler(pools, { vram }));

    const done = Promise.all([
      scheduler.addTask(this.task('big', 'Big'), { createPromise: true }),
      ...[0, 1, 2, 3].map(i => scheduler.addTask(this.task(`s${i}`, `S${i}`), { createPromise: true }))
    ]);
    const peak = await this.peakWhile(vram, done);

    if (peak > 24) throw new Error(`Resident usage exceeded the limit: ${peak}/24`);
    if (peak < 22) throw new Error(`Expected the group to pack near its limit, peaked at only ${peak}/24`);
  }

  /**
   * The reason worker-lifetime groups beat swapping models inside one worker: a
   * run of tasks for the same pool reuses the loaded worker instead of paying
   * the load cost again.
   */
  private async testAffinity(): Promise<void> {
    const vram = new ConcurrentLimitGroup(1, 'vram');
    const pool = this.pool(1, { idleTimeout: 5000, idleCheckIntervalMs: 5000 });
    const scheduler = this.track(new FyflowScheduler({ A: pool }, { vram }));

    let initialised = 0;
    pool.addEventListener('worker.initialization.completed', () => { initialised++; });

    await Promise.all(Array.from({ length: 8 }, (_, i) =>
      scheduler.addTask(this.task(`a${i}`, 'A', 5), { createPromise: true })));

    if (initialised !== 1) {
      throw new Error(`8 tasks on one pool should reuse a single worker, initialised ${initialised}`);
    }
  }

  /** `maxThreads: 3` with cost 2 must consume 2 per live worker, not 2 per pool. */
  private async testCostPerWorker(): Promise<void> {
    const vram = new ConcurrentLimitGroup(24, 'vram');
    const pool = this.pool(2, { maxThreads: 3, idleTimeout: 5000, idleCheckIntervalMs: 5000 });
    const scheduler = this.track(new FyflowScheduler({ A: pool }, { vram }));

    await Promise.all(Array.from({ length: 3 }, (_, i) =>
      scheduler.addTask(this.task(`a${i}`, 'A', 60), { createPromise: true })));

    const live = pool.getWorkerIds().length;
    const usage = pool.getResidentUsage().vram;
    if (usage !== live * 2) {
      throw new Error(`Expected ${live} workers to hold ${live * 2} units, got ${usage}`);
    }
    if (vram.running !== usage) {
      throw new Error(`Group usage ${vram.running} disagrees with pool usage ${usage}`);
    }
  }

  /** Nothing may be left held once the pool is gone. */
  private async testShutdownReleases(): Promise<void> {
    const vram = new ConcurrentLimitGroup(24, 'vram');
    const scheduler = new FyflowScheduler(
      { A: this.pool(20, { idleTimeout: 5000, idleCheckIntervalMs: 5000 }) }, { vram }
    );

    await scheduler.addTask(this.task('a1', 'A'), { createPromise: true });
    if (vram.running === 0) throw new Error('Expected the worker to be holding units before shutdown');

    await scheduler.shutdown();
    if (vram.running !== 0) {
      throw new Error(`Shutdown leaked ${vram.running} units`);
    }
  }

  /** `residentGroups: ['vram']` is shorthand for one unit each. */
  private async testArrayForm(): Promise<void> {
    const vram = new ConcurrentLimitGroup(4, 'vram');
    const pool = new WorkerManager(this.inlineWorkerUrl, {
      maxThreads: 1, maxConcurrentTasks: 1, inline: true,
      idleTimeout: 5000, idleCheckIntervalMs: 5000,
      residentGroups: ['vram']
    });
    const scheduler = this.track(new FyflowScheduler({ A: pool }, { vram }));

    await scheduler.addTask(this.task('a1', 'A'), { createPromise: true });

    if (pool.getResidentUsage().vram !== 1) {
      throw new Error(`Array form should cost 1 per worker, got ${pool.getResidentUsage().vram}`);
    }
  }

  /** A cost that is not a positive integer is a configuration error. */
  private async testInvalidCosts(): Promise<void> {
    for (const bad of [0, -1, 1.5, NaN]) {
      let threw = false;
      try {
        new WorkerManager(this.inlineWorkerUrl, { residentGroups: { vram: bad } });
      } catch { threw = true; }
      if (!threw) throw new Error(`residentGroups cost ${bad} should have been rejected`);
    }
  }

  /** A pool that could never start a worker must fail loudly, not block forever. */
  private async testCostAboveLimit(): Promise<void> {
    const vram = new ConcurrentLimitGroup(8, 'vram');
    let threw = false;
    try {
      new FyflowScheduler({ A: this.pool(20) }, { vram });
    } catch { threw = true; }
    if (!threw) {
      throw new Error('A resident cost above the group limit should be rejected at construction');
    }
  }

  private async testUnknownGroup(): Promise<void> {
    let threw = false;
    try {
      new FyflowScheduler({ A: this.pool(1) }, {});
    } catch { threw = true; }
    if (!threw) throw new Error('An unknown resident group should be rejected at construction');
  }

  /** Pools that declare none are untouched - the feature is opt-in. */
  private async testOptIn(): Promise<void> {
    const scheduler = this.track(new FyflowScheduler({ A: this.pool(undefined) }, {}));
    const results = await Promise.all(Array.from({ length: 4 }, (_, i) =>
      scheduler.addTask(this.task(`a${i}`, 'A', 5), { createPromise: true })));

    if (results.length !== 4 || results.some(r => !r)) {
      throw new Error('A pool without resident groups should run normally');
    }
  }
}

// Auto-run tests when executed directly - handle both Deno and Node.js
if ((typeof Deno !== 'undefined' && import.meta.main) ||
    (typeof process !== 'undefined' && process.argv[1] && import.meta.url &&
     import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/')))) {
  const testSuite = new ResidentGroupsTestSuite();
  try {
    await testSuite.runAllTests(true);
  } finally {
    await testSuite.cleanup();
  }
}

export { ResidentGroupsTestSuite };
export default ResidentGroupsTestSuite;
