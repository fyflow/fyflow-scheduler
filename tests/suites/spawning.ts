// Dynamic task spawning and descendant tracking tests
//
// Descendants are tasks created at runtime via context.spawnTask(). This is
// lineage, not a scheduling dependency - it never affects dispatch order.

import { WorkerManager, FyflowScheduler, FyflowTask } from '../../index.ts';

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

class SpawningTestSuite {
  private results: TestResult[] = [];
  private startTime = 0;
  private spawningWorkerUrl: string = "";
  private schedulers: FyflowScheduler[] = [];

  private createScheduler(options: any = {}): FyflowScheduler {
    const pool = new WorkerManager(this.spawningWorkerUrl, {
      maxThreads: 4,
      maxConcurrentTasks: 4,
      inline: true
    });
    const scheduler = new FyflowScheduler({ SpawningWorker: pool }, {}, options);
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
    if (typeof Deno !== "undefined") {
      this.spawningWorkerUrl = new URL("../workers/spawningWorker.ts", import.meta.url).href;
    } else {
      // Node/Browser esbuild replaces this
      // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
      this.spawningWorkerUrl = new URL((await import('../workers/spawningWorker.ts?worker-direct')).default).href;
    }

    const platform = typeof globalThis !== 'undefined' && 'Deno' in globalThis ? 'Deno' : 'Node.js';
    console.log(`🚀 FyFlow Spawning & Descendant Tests - ${platform}`);
    console.log('='.repeat(60));

    this.startTime = performance.now();
    this.results = [];

    this.results.push(await this.runTest('Worker Spawns Child Tasks', () => this.testBasicSpawning()));
    this.results.push(await this.runTest('Descendants - Task Without Children', () => this.testNoDescendants()));
    this.results.push(await this.runTest('Descendants - Waits For Children', () => this.testWaitsForChildren()));
    this.results.push(await this.runTest('Descendants - Waits For Grandchildren', () => this.testWaitsForGrandchildren()));
    this.results.push(await this.runTest('Descendants - Rejects When Task Fails', () => this.testRejectsOnRootFailure()));
    this.results.push(await this.runTest('Descendants - Resolves Despite Child Failure', () => this.testResolvesOnChildFailure()));
    this.results.push(await this.runTest('Descendants - Rejects Before Task Added', () => this.testRejectsBeforeAdd()));
    this.results.push(await this.runTest('Descendants - Multiple Waiters On One Task', () => this.testMultipleWaiters()));
    this.results.push(await this.runTest('Descendants - Resolves When Already Complete', () => this.testAlreadyComplete()));
    this.results.push(await this.runTest('Spawn Of Unknown Worker Type Does Not Crash', () => this.testUnknownWorkerSpawn()));
    this.results.push(await this.runTest('Descendants - Survive Task Eviction', () => this.testDescendantsSurviveEviction()));
    this.results.push(await this.runTest('Descendants - Reject On Scheduler Shutdown', () => this.testDescendantsRejectOnShutdown()));

    const totalDuration = performance.now() - this.startTime;
    const passed = this.results.filter(r => r.passed).length;
    const failed = this.results.length - passed;

    console.log(`\n📊 Spawning Test Summary - ${platform}`);
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
        if (typeof Deno !== 'undefined') {
          Deno.exit(1);
        } else {
          process.exit(1);
        }
      }
    } else {
      console.log('\n🎉 All spawning tests passed!');
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

  // Spawning itself works: the worker's spawn requests become real tasks
  private async testBasicSpawning(): Promise<void> {
    const scheduler = this.createScheduler();
    const spawnRequests: string[] = [];
    scheduler.addEventListener('task.spawn_request', (e: any) => {
      spawnRequests.push(e.detail.spawnConfig.id);
    });

    const task = new FyflowTask({
      id: 'basic-spawn',
      workerType: 'SpawningWorker',
      payload: { spawn: 3, depth: 1, childId: 'basic' }
    });
    scheduler.addTask(task);
    await task.onCompleteDescendants();

    if (spawnRequests.length !== 3) {
      throw new Error(`Expected 3 spawn requests, got ${spawnRequests.length}`);
    }
    for (let i = 0; i < 3; i++) {
      const child = scheduler.tasks.get(`basic-${i}`);
      if (!child) throw new Error(`Spawned task basic-${i} was never added to the scheduler`);
      if (child.state !== 'done') throw new Error(`Spawned task basic-${i} is ${child.state}, expected done`);
    }
  }

  // A task that spawns nothing behaves like a plain completion wait
  private async testNoDescendants(): Promise<void> {
    const scheduler = this.createScheduler();
    const task = new FyflowTask({
      id: 'no-descendants',
      workerType: 'SpawningWorker',
      payload: { spawn: 0, childId: 'solo' }
    });
    scheduler.addTask(task);

    const result = await task.onCompleteDescendants();
    if (task.state !== 'done') throw new Error(`Expected done, got ${task.state}`);
    if (!result || result.id !== 'solo') {
      throw new Error(`Expected the task's own result, got ${JSON.stringify(result)}`);
    }
  }

  // The wait must not resolve until every spawned child has finished
  private async testWaitsForChildren(): Promise<void> {
    const scheduler = this.createScheduler();
    const task = new FyflowTask({
      id: 'waits-children',
      workerType: 'SpawningWorker',
      // Children outlive the parent, so resolving on the parent alone would be wrong
      payload: { spawn: 3, depth: 1, childId: 'wc', delay: 10, childDelay: 120 }
    });
    scheduler.addTask(task);
    await task.onCompleteDescendants();

    for (let i = 0; i < 3; i++) {
      const child = scheduler.tasks.get(`wc-${i}`);
      if (!child || child.state !== 'done') {
        throw new Error(`Resolved before child wc-${i} finished (state: ${child?.state})`);
      }
    }
  }

  // Tracking must follow lineage transitively, not just one level
  private async testWaitsForGrandchildren(): Promise<void> {
    const scheduler = this.createScheduler();
    const task = new FyflowTask({
      id: 'waits-grandchildren',
      workerType: 'SpawningWorker',
      payload: { spawn: 2, depth: 2, childId: 'gc', delay: 10, childDelay: 60 }
    });
    scheduler.addTask(task);
    await task.onCompleteDescendants();

    // depth 2 with 2 children each: 2 children + 4 grandchildren
    const expected = ['gc-0', 'gc-1', 'gc-0-0', 'gc-0-1', 'gc-1-0', 'gc-1-1'];
    for (const id of expected) {
      const descendant = scheduler.tasks.get(id);
      if (!descendant) throw new Error(`Descendant ${id} was never created`);
      if (descendant.state !== 'done') {
        throw new Error(`Resolved before descendant ${id} finished (state: ${descendant.state})`);
      }
    }
  }

  // The tracked task failing rejects, matching onCompletePromise()
  private async testRejectsOnRootFailure(): Promise<void> {
    const scheduler = this.createScheduler();
    const task = new FyflowTask({
      id: 'root-fails',
      workerType: 'SpawningWorker',
      payload: { spawn: 0, childId: 'rf', shouldThrow: true }
    });
    scheduler.addTask(task);

    let rejected = false;
    try {
      await task.onCompleteDescendants();
    } catch (_error) {
      rejected = true;
    }
    if (!rejected) throw new Error('Expected onCompleteDescendants to reject when the task failed');
  }

  // A failing descendant must not hang or reject the workflow wait
  private async testResolvesOnChildFailure(): Promise<void> {
    const scheduler = this.createScheduler();
    const task = new FyflowTask({
      id: 'child-fails',
      workerType: 'SpawningWorker',
      payload: { spawn: 2, depth: 1, childId: 'cf', childShouldThrow: true }
    });
    scheduler.addTask(task);

    const settled = await Promise.race([
      task.onCompleteDescendants().then(() => 'resolved').catch(() => 'rejected'),
      new Promise(resolve => setTimeout(() => resolve('hung'), 3000))
    ]);

    if (settled !== 'resolved') {
      throw new Error(`Expected resolution despite a failed child, got: ${settled}`);
    }
  }

  // Descendant tracking needs the scheduler back-reference addTask sets
  private async testRejectsBeforeAdd(): Promise<void> {
    const task = new FyflowTask({
      id: 'never-added',
      workerType: 'SpawningWorker',
      payload: { spawn: 0 }
    });

    let message = '';
    try {
      await task.onCompleteDescendants();
    } catch (error: any) {
      message = error.message;
    }
    if (!message.includes('added to a scheduler')) {
      throw new Error(`Expected an explanatory rejection, got: ${message || 'no rejection'}`);
    }
  }

  // One task can be awaited by more than one caller
  private async testMultipleWaiters(): Promise<void> {
    const scheduler = this.createScheduler();
    const task = new FyflowTask({
      id: 'multi-waiter',
      workerType: 'SpawningWorker',
      payload: { spawn: 2, depth: 1, childId: 'mw', childDelay: 60 }
    });
    scheduler.addTask(task);

    const first = task.onCompleteDescendants();
    const second = task.onCompleteDescendants();

    const settled = await Promise.race([
      Promise.all([first, second]).then(() => 'both'),
      new Promise(resolve => setTimeout(() => resolve('hung'), 3000))
    ]);

    if (settled !== 'both') throw new Error(`Expected both waiters to settle, got: ${settled}`);
  }

  // Tracking can start after the workflow has already settled
  private async testAlreadyComplete(): Promise<void> {
    const scheduler = this.createScheduler();
    const task = new FyflowTask({
      id: 'already-done',
      workerType: 'SpawningWorker',
      payload: { spawn: 2, depth: 1, childId: 'ad' }
    });
    scheduler.addTask(task);
    await task.onCompleteDescendants();

    // Second wait, started well after everything finished
    const settled = await Promise.race([
      task.onCompleteDescendants().then(() => 'resolved'),
      new Promise(resolve => setTimeout(() => resolve('hung'), 2000))
    ]);
    if (settled !== 'resolved') {
      throw new Error(`Expected an immediate resolution, got: ${settled}`);
    }
  }

  // Aggressive eviction must not change how a descendant wait settles: trackers
  // hold their root task by reference rather than looking it up in the map
  private async testDescendantsSurviveEviction(): Promise<void> {
    // Small enough that the root is evicted well before the workflow finishes
    const scheduler = this.createScheduler({ maxCompletedTasks: 1 });
    const task = new FyflowTask({
      id: 'evicted-root',
      workerType: 'SpawningWorker',
      payload: { spawn: 3, depth: 2, childId: 'ev', delay: 5, childDelay: 40 }
    });
    scheduler.addTask(task);

    const result = await task.onCompleteDescendants();

    // The worker echoes its own childId, so this is the root's own result and
    // not a descendant's - proving the tracker did not lose it to eviction
    if (!result || result.id !== 'ev' || result.spawned !== 3) {
      throw new Error(`Expected the root task's result, got ${JSON.stringify(result)}`);
    }
    if (scheduler.tasks.size > 1) {
      throw new Error(`Expected retention capped at 1, got ${scheduler.tasks.size}`);
    }
    // 1 root + 3 children + 9 grandchildren
    if (scheduler.stats.done !== 13) {
      throw new Error(`Expected 13 completed tasks, got ${scheduler.stats.done}`);
    }
  }

  // Documented: an outstanding wait rejects if the scheduler shuts down first,
  // rather than being dropped silently and leaving the caller awaiting forever
  private async testDescendantsRejectOnShutdown(): Promise<void> {
    // shutdown() drains outstanding work first - it keeps dispatching while tasks
    // are running - so a workflow that can still finish simply finishes and the
    // wait resolves. The rejection is for work that can no longer run, so this
    // uses a pool whose worker cannot be loaded at all.
    const pool = new WorkerManager(
      new URL("../workers/doesNotExist.ts", import.meta.url).href,
      { maxThreads: 1, maxConcurrentTasks: 1, inline: true, idleTimeout: 0 }
    );
    const scheduler = new FyflowScheduler({ SpawningWorker: pool });
    this.schedulers.push(scheduler);

    const task = new FyflowTask({
      id: 'shutdown-wait', workerType: 'SpawningWorker',
      payload: { spawn: 0, childId: 'sw' }
    });
    scheduler.addTask(task);

    const waiting = task.onCompleteDescendants().then(() => 'resolved').catch(
      (error: any) => `rejected:${error.message}`
    );

    await new Promise(resolve => setTimeout(resolve, 200));
    await scheduler.shutdown();

    const outcome = await Promise.race([
      waiting,
      new Promise<string>(resolve => setTimeout(() => resolve('NEVER SETTLED'), 3000))
    ]);

    if (!outcome.startsWith('rejected')) {
      throw new Error(`Expected the wait to reject on shutdown, got: ${outcome}`);
    }
    if (!outcome.includes('shut down')) {
      throw new Error(`Expected a shutdown-specific message, got: ${outcome}`);
    }
  }

  // A spawn the scheduler cannot satisfy must fail that spawn alone
  private async testUnknownWorkerSpawn(): Promise<void> {
    const scheduler = this.createScheduler();
    let spawnFailure: any = null;
    scheduler.addEventListener('task.spawn_failed', (e: any) => {
      spawnFailure = e.detail;
    });

    const task = new FyflowTask({
      id: 'bad-spawn',
      workerType: 'SpawningWorker',
      payload: { spawn: 1, depth: 1, childId: 'bs', spawnUnknownWorker: true }
    });
    scheduler.addTask(task);

    const settled = await Promise.race([
      task.onCompleteDescendants().then(() => 'resolved').catch(() => 'rejected'),
      new Promise(resolve => setTimeout(() => resolve('hung'), 3000))
    ]);

    if (settled !== 'resolved') {
      throw new Error(`A rejected spawn broke the workflow, got: ${settled}`);
    }
    if (!spawnFailure) throw new Error('Expected a task.spawn_failed event');
    if (!spawnFailure.error?.message?.includes('Unknown worker type')) {
      throw new Error(`Expected an unknown worker type error, got: ${spawnFailure.error?.message}`);
    }
    // The rejected spawn must leave no orphan entry behind
    if (scheduler.tasks.has('bs-orphan')) {
      throw new Error('Rejected spawn left an orphan entry in the task map');
    }
    // The valid sibling spawn must still have run
    const sibling = scheduler.tasks.get('bs-0');
    if (!sibling || sibling.state !== 'done') {
      throw new Error(`Valid sibling spawn did not complete (state: ${sibling?.state})`);
    }
  }
}

// Auto-run tests when executed directly - handle both Deno and Node.js
if ((typeof Deno !== 'undefined' && import.meta.main) ||
    (typeof process !== 'undefined' && process.argv[1] && import.meta.url &&
     import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/')))) {
  const testSuite = new SpawningTestSuite();
  try {
    await testSuite.runAllTests(true);
  } finally {
    await testSuite.cleanup();
  }
}

export { SpawningTestSuite };
export default SpawningTestSuite;
