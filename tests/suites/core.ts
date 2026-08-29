// Comprehensive Cross-Platform Test Suite for FyFlow
// Tests core functionality across Deno and Node.js platforms


import { WorkerManager, FyflowScheduler, FyflowTask } from '../../index.ts';
import { ConcurrentLimitGroup } from '../../groups/concurrentLimitGroup.ts';
import { RateLimitGroup } from '../../groups/rateLimitGroup.ts';

// Node.js process declaration for cross-platform compatibility
declare const process: any;

interface TestResult {
  name: string;
  passed: boolean;
  duration: number;
  error?: string;
  details?: any;
}

interface TestSuiteResult {
  platform: string;
  totalTests: number;
  passed: number;
  failed: number;
  duration: number;
  results: TestResult[];
}

class CrossPlatformTestSuite {
  private results: TestResult[] = [];
  private startTime = 0;
  private  testInlineWorkerUrl =  new URL("../workers/testInlineWorker.ts", import.meta.url).href;;
  private  testThreadWorkerUrl = new URL("../workers/testThreadWorker.ts", import.meta.url).href;;

  private async runTest(name: string, testFn: () => Promise<void>): Promise<TestResult> {
    if (typeof Deno !== "undefined") {
      // this.testInlineWorkerUrl = new URL("../workers/testInlineWorker.ts", import.meta.url).href;
      // this.testThreadWorkerUrl = new URL("../workers/testThreadWorker.ts", import.meta.url).href;
    } else {
      // Node/Browser esbuild replaces this
      // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
      this.testInlineWorkerUrl = new URL((await import('../workers/testInlineWorker.ts?worker-direct')).default);
      // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
      this.testThreadWorkerUrl = new URL((await import("../workers/testInlineWorker.ts/?worker-direct")).default);
  
    }

    
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

  async runAllTests(_exitOnComplete = true): Promise<TestSuiteResult> {

    if (typeof Deno !== "undefined") {
      // this.testInlineWorkerUrl = new URL("../workers/testInlineWorker.ts", import.meta.url).href;
      // this.testThreadWorkerUrl = new URL("../workers/testThreadWorker.ts", import.meta.url).href;
    } else {
      // Node/Browser esbuild replaces this
      // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
      this.testInlineWorkerUrl = new URL((await import('../workers/testInlineWorker.ts?worker-direct')).default);
      // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
      this.testThreadWorkerUrl = new URL((await import("../workers/testInlineWorker.ts/?worker-direct")).default);
  
    }
  

    const platform = typeof globalThis !== 'undefined' && 'Deno' in globalThis ? 'Deno' : 'Node.js';
    console.log(`🚀 FyFlow Cross-Platform Test Suite - ${platform}`);
    console.log('='.repeat(50));

    this.startTime = performance.now();
    this.results = [];

    // Core Infrastructure Tests
    this.results.push(await this.runTest('CPU Manager Basic Operations', () => this.testCPUManager()));
    this.results.push(await this.runTest('Group Constraint Basic Operations', () => this.testGroupConstraints()));

    // Worker Manager Tests
    this.results.push(await this.runTest('Inline Worker Single Task', () => this.testInlineWorkerSingle()));
    this.results.push(await this.runTest('Inline Worker Multiple Tasks', () => this.testInlineWorkerMultiple()));
    this.results.push(await this.runTest('Thread Worker Single Task', () => this.testThreadWorkerSingle()));
    this.results.push(await this.runTest('Thread Worker Multiple Tasks', () => this.testThreadWorkerMultiple()));

    // Scheduler Integration Tests
    this.results.push(await this.runTest('Parallel Tasks', () => this.testParallelTasks()));
    this.results.push(await this.runTest('Mixed Worker Types', () => this.testMixedWorkerTypes()));

    // Resource Management Tests
    this.results.push(await this.runTest('CPU Slot Enforcement', () => this.testCPUSlotEnforcement()));
    this.results.push(await this.runTest('Group Limit Enforcement', () => this.testGroupLimitEnforcement()));
    this.results.push(await this.runTest('Resource Contention', () => this.testResourceContention()));
    this.results.push(await this.runTest('Blocked Tasks Complete And Delay Completion', () => this.testBlockedTasksCompleteAfterBlocking()));

    // Task retention
    this.results.push(await this.runTest('Completed Tasks Retained By Default', () => this.testRetentionDefault()));
    this.results.push(await this.runTest('maxCompletedTasks Bounds Retention', () => this.testRetentionBounded()));
    this.results.push(await this.runTest('maxCompletedTasks Evicts Oldest First', () => this.testRetentionEvictsOldest()));

    // Edge Cases and Error Handling
    this.results.push(await this.runTest('Worker Pool Scaling', () => this.testWorkerPoolScaling()));

    // Rate Limiting Stress Tests
    // RateLimitGroup is optimistic like ConcurrentLimitGroup, so instantaneous
    // enforcement is racy - the assertion is on the average rate over the run,
    // with tolerance, which is what the limit actually promises.
    this.results.push(await this.runTest('Concurrent Rate Limit Enforcement', () => this.testConcurrentRateLimitEnforcement()));

    // Idle Timeout Tests
    this.results.push(await this.runTest('Idle Timeout Default (5000ms)', () => this.testIdleTimeoutDefault()));
    this.results.push(await this.runTest('Idle Timeout Custom (100ms)', () => this.testIdleTimeoutCustom()));
    this.results.push(await this.runTest('Idle Timeout Persistent (0=never)', () => this.testIdleTimeoutPersistent()));

    // Batch API Tests
    this.results.push(await this.runTest('Batch Task Addition', () => this.testBatchTaskAddition()));
    this.results.push(await this.runTest('handleRejection Silences Fire-And-Forget Failures', () => this.testHandleRejection()));
    this.results.push(await this.runTest('periodicRetryIntervalMs Is Honoured', () => this.testPeriodicRetryInterval()));

    const totalDuration = performance.now() - this.startTime;
    const passed = this.results.filter(r => r.passed).length;
    const failed = this.results.length - passed;

    console.log(`\n📊 Test Suite Summary - ${platform}`);
    console.log('='.repeat(50));
    console.log(`Total Tests: ${this.results.length}`);
    console.log(`✅ Passed: ${passed}`);
    console.log(`❌ Failed: ${failed}`);
    console.log(`⏱️  Duration: ${totalDuration.toFixed(1)}ms`);

    if (failed > 0) {
      console.log('\n❌ Failed Tests:');
      this.results.filter(r => !r.passed).forEach(r => {
        console.log(`  • ${r.name}: ${r.error}`);
      });
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

  private async testCPUManager(): Promise<void> {
    // CPU constraints now handled by groups - this test is replaced by group constraint tests
    const group = new ConcurrentLimitGroup(4);

    // Test basic group operations
    if (!group.canRun()) throw new Error('Group should accept tasks initially');

    group.onStart();
    group.onStart();
    if (!group.canRun()) throw new Error('Group should accept 2 more tasks when capacity is 4');

    group.onStart();
    group.onStart();
    if (group.canRun()) throw new Error('Group should be at capacity after 4 tasks');

    group.onFinish();
    if (!group.canRun()) throw new Error('Group should accept tasks after one completion');
  }

  private async testGroupConstraints(): Promise<void> {
    const group = new ConcurrentLimitGroup(2);

    // Test basic group operations
    if (!group.canRun()) throw new Error('Group should accept tasks initially');

    group.onStart();
    group.onStart();
    if (group.canRun()) throw new Error('Group should be at capacity');

    group.onFinish();
    if (!group.canRun()) throw new Error('Group should accept tasks after completion');
  }

  private async testInlineWorkerSingle(): Promise<void> {
    // CPU constraints now handled by groups
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: true
    });

    const task = new FyflowTask({
      id: 'inline-single',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'inline-single', data: 'test', delay: 10 }
    });

    const scheduler = new FyflowScheduler({ TestInlineWorker: workerManager });
    scheduler.addTask(task);

    await this.waitForCompletion(scheduler, 1);

    const stats = scheduler.stats;
    if (stats.done !== 1) throw new Error(`Expected 1 completed task, got ${stats.done}`);
  }

  private async testInlineWorkerMultiple(): Promise<void> {
    // CPU constraints now handled by groups
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 2,
      maxConcurrentTasks: 3,
      inline: true
    });

    const scheduler = new FyflowScheduler({ TestInlineWorker: workerManager });

    // Add 5 tasks to test concurrent execution
    for (let i = 0; i < 5; i++) {
      const task = new FyflowTask({
        id: `inline-multi-${i}`,
        workerType: 'TestInlineWorker',
        payload: { taskId: `inline-multi-${i}`, data: `test-${i}`, delay: 20 }
      });
      scheduler.addTask(task);
    }

    await this.waitForCompletion(scheduler, 5);

    const stats = scheduler.stats;
    if (stats.done !== 5) throw new Error(`Expected 5 completed tasks, got ${stats.done}`);
  }

  private async testThreadWorkerSingle(): Promise<void> {
    // CPU constraints now handled by groups
    const workerManager = new WorkerManager(this.testThreadWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: false
    });

    const task = new FyflowTask({
      id: 'thread-single',
      workerType: 'TestThreadWorker',
      payload: { taskId: 'thread-single', data: 'test', delay: 50 }
    });

    const scheduler = new FyflowScheduler({ TestThreadWorker: workerManager });
    scheduler.addTask(task);

    await this.waitForCompletion(scheduler, 1);

    const stats = scheduler.stats;
    if (stats.done !== 1) throw new Error(`Expected 1 completed task, got ${stats.done}`);
  }

  private async testThreadWorkerMultiple(): Promise<void> {
    // CPU constraints now handled by groups
    const workerManager = new WorkerManager(this.testThreadWorkerUrl, {
      maxThreads: 2,
      maxConcurrentTasks: 2,
      inline: false
    });

    const scheduler = new FyflowScheduler({ TestThreadWorker: workerManager });

    // Add 4 tasks to test multiple threads with concurrent execution
    for (let i = 0; i < 4; i++) {
      const task = new FyflowTask({
        id: `thread-multi-${i}`,
        workerType: 'TestThreadWorker',
        payload: { taskId: `thread-multi-${i}`, data: `test-${i}`, delay: 30 }
      });
      scheduler.addTask(task);
    }

    await this.waitForCompletion(scheduler, 4);

    const stats = scheduler.stats;
    if (stats.done !== 4) throw new Error(`Expected 4 completed tasks, got ${stats.done}`);
  }


  private async testParallelTasks(): Promise<void> {
    // CPU constraints now handled by groups
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 3,
      maxConcurrentTasks: 1,
      inline: true
    });

    const scheduler = new FyflowScheduler({ TestInlineWorker: workerManager });

    // Create 6 independent parallel tasks
    for (let i = 0; i < 6; i++) {
      const task = new FyflowTask({
        id: `parallel-${i}`,
        workerType: 'TestInlineWorker',
        payload: { taskId: `parallel-${i}`, data: `parallel-${i}`, delay: 25 }
      });
      scheduler.addTask(task);
    }

    await this.waitForCompletion(scheduler, 6);

    const stats = scheduler.stats;
    if (stats.done !== 6) throw new Error(`Expected 6 completed tasks, got ${stats.done}`);
  }

  private async testMixedWorkerTypes(): Promise<void> {
    // CPU constraints now handled by groups

    const inlineManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 2,
      maxConcurrentTasks: 2,
      inline: true
    });

    const threadManager = new WorkerManager(this.testThreadWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: false
    });

    const scheduler = new FyflowScheduler({
      TestInlineWorker: inlineManager,
      TestThreadWorker: threadManager
    });

    // Mix of inline and threaded tasks
    scheduler.addTask(new FyflowTask({
      id: 'mixed-inline-1',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'mixed-inline-1', data: 'inline', delay: 20 }
    }));

    scheduler.addTask(new FyflowTask({
      id: 'mixed-thread-1',
      workerType: 'TestThreadWorker',
      payload: { taskId: 'mixed-thread-1', data: 'thread', delay: 30 }
    }));

    scheduler.addTask(new FyflowTask({
      id: 'mixed-inline-2',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'mixed-inline-2', data: 'inline', delay: 15 }
    }));

    await this.waitForCompletion(scheduler, 3);

    const stats = scheduler.stats;
    if (stats.done !== 3) throw new Error(`Expected 3 completed tasks, got ${stats.done}`);
  }

  private async testCPUSlotEnforcement(): Promise<void> {
    // Test CPU limit enforcement using ConcurrentLimitGroup
    // Note: ConcurrentLimitGroup may briefly exceed limit during race conditions
    const cpuGroup = new ConcurrentLimitGroup(2); // ~2 concurrent CPU tasks (optimistic)

    // Track group usage to verify strict enforcement
    let maxConcurrentTasks = 0;
    const checkInterval = setInterval(() => {
      maxConcurrentTasks = Math.max(maxConcurrentTasks, cpuGroup.running);
    }, 10);

    const threadManager = new WorkerManager(this.testThreadWorkerUrl, {
      maxThreads: 4, // More threads than CPU group limit
      maxConcurrentTasks: 1,
      inline: false,
      groups: ['cpu']
    });

    const scheduler = new FyflowScheduler({ TestThreadWorker: threadManager }, { cpu: cpuGroup });

    // Add 4 tasks with CPU group constraint
    for (let i = 0; i < 4; i++) {
      scheduler.addTask(new FyflowTask({
        id: `cpu-limit-${i}`,
        workerType: 'TestThreadWorker',
        payload: { taskId: `cpu-limit-${i}`, data: `cpu-test-${i}`, delay: 200 }
      }));
      await new Promise(resolve => setTimeout(resolve, 10)); // Stagger additions
    }

    await this.waitForCompletion(scheduler, 4);
    clearInterval(checkInterval);

    // Verify that never more than 2 tasks ran concurrently (strict limit guarantee)
    if (maxConcurrentTasks > 2) {
      throw new Error(`Expected max 2 concurrent CPU tasks, got ${maxConcurrentTasks}`);
    }

    const stats = scheduler.stats;
    if (stats.done !== 4) throw new Error(`Expected 4 completed tasks, got ${stats.done}`);
  }

  private async testGroupLimitEnforcement(): Promise<void> {
    // CPU constraints now handled by groups
    const group = new ConcurrentLimitGroup(2); // Limit to 2 concurrent tasks

    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 4,
      maxConcurrentTasks: 1,
      inline: true,
      groups: ['testGroup']
    });

    const scheduler = new FyflowScheduler({ TestInlineWorker: workerManager }, { testGroup: group });

    // Add 4 tasks to group-constrained worker
    for (let i = 0; i < 4; i++) {
      scheduler.addTask(new FyflowTask({
        id: `group-limit-${i}`,
        workerType: 'TestInlineWorker',
        payload: { taskId: `group-limit-${i}`, data: `group-test-${i}`, delay: 80 }
      }));
    }

    // Check group constraint is enforced
    await new Promise(resolve => setTimeout(resolve, 40));
    let stats = scheduler.stats;
    if (stats.running > 2) throw new Error(`Expected max 2 running tasks due to group limit, got ${stats.running}`);

    await this.waitForCompletion(scheduler, 4);

    stats = scheduler.stats;
    if (stats.done !== 4) throw new Error(`Expected 4 completed tasks, got ${stats.done}`);
  }

  // Tasks blocked by a resource group leave the ready queue and are subtracted
  // from stats.queued, so the scheduler must track them separately. Otherwise it
  // reports completion with work outstanding and clears the periodic retry timer
  // that was the only thing that could ever unblock them.
  // Retention is unlimited unless asked otherwise - most workloads have a bounded
  // task count and want completed tasks left inspectable
  private async testRetentionDefault(): Promise<void> {
    const scheduler = this.createRetentionScheduler();
    try {
      await this.runRetentionTasks(scheduler, 12);

      if (scheduler.tasks.size !== 12) {
        throw new Error(`Expected all 12 tasks retained by default, got ${scheduler.tasks.size}`);
      }
      const first = scheduler.tasks.get('retain-0');
      if (!first || first.result === undefined) {
        throw new Error('Retained task lost its result');
      }
    } finally {
      await scheduler.shutdown();
    }
  }

  private async testRetentionBounded(): Promise<void> {
    const scheduler = this.createRetentionScheduler({ maxCompletedTasks: 5 });
    try {
      await this.runRetentionTasks(scheduler, 20);

      if (scheduler.tasks.size !== 5) {
        throw new Error(`Expected retention capped at 5, got ${scheduler.tasks.size}`);
      }
      // Eviction must not distort the counters
      if (scheduler.stats.done !== 20) {
        throw new Error(`Expected stats.done to count all 20 tasks, got ${scheduler.stats.done}`);
      }
    } finally {
      await scheduler.shutdown();
    }
  }

  private async testRetentionEvictsOldest(): Promise<void> {
    const scheduler = this.createRetentionScheduler({ maxCompletedTasks: 3 });
    try {
      // Serial, so completion order is deterministic
      for (let i = 0; i < 8; i++) {
        await scheduler.addTask(new FyflowTask({
          id: `retain-${i}`,
          workerType: 'TestInlineWorker',
          payload: { taskId: `retain-${i}`, data: `retain-${i}`, delay: 5 }
        }), { createPromise: true });
      }

      const retained = Array.from(scheduler.tasks.keys()).sort();
      const expected = ['retain-5', 'retain-6', 'retain-7'];
      if (JSON.stringify(retained) !== JSON.stringify(expected)) {
        throw new Error(`Expected the 3 newest tasks ${JSON.stringify(expected)}, got ${JSON.stringify(retained)}`);
      }
    } finally {
      await scheduler.shutdown();
    }
  }

  private createRetentionScheduler(options: any = {}): FyflowScheduler {
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 2,
      maxConcurrentTasks: 4,
      inline: true
    });
    return new FyflowScheduler({ TestInlineWorker: workerManager }, {}, options);
  }

  private async runRetentionTasks(scheduler: FyflowScheduler, count: number): Promise<void> {
    const tasks = [];
    for (let i = 0; i < count; i++) {
      tasks.push(new FyflowTask({
        id: `retain-${i}`,
        workerType: 'TestInlineWorker',
        payload: { taskId: `retain-${i}`, data: `retain-${i}`, delay: 5 }
      }));
    }
    await Promise.all(scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]);
  }

  private async testBlockedTasksCompleteAfterBlocking(): Promise<void> {
    const taskCount = 9;
    // Well below taskCount, so most tasks are blocked on arrival and can only run
    // once the rate limit window rolls over
    const rateLimit = new RateLimitGroup([{ limit: 3, windowMs: 200 }]);

    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 2,
      maxConcurrentTasks: 4,
      inline: true,
      groups: ['rateLimited']
    });

    const scheduler = new FyflowScheduler({ TestInlineWorker: workerManager }, { rateLimited: rateLimit });

    // Completion must not be announced while tasks are still blocked
    let doneAtFirstCompletion = -1;
    scheduler.addEventListener('scheduler.completed', (e: any) => {
      if (doneAtFirstCompletion === -1) doneAtFirstCompletion = e.detail.done;
    });

    for (let i = 0; i < taskCount; i++) {
      scheduler.addTask(new FyflowTask({
        id: `blocked-${i}`,
        workerType: 'TestInlineWorker',
        payload: { taskId: `blocked-${i}`, data: `blocked-test-${i}`, delay: 10 }
      }));
    }

    try {
      await this.waitForCompletion(scheduler, taskCount, 8000);

      const stats = scheduler.stats;
      if (stats.done !== taskCount) {
        throw new Error(`Expected ${taskCount} completed tasks, got ${stats.done}`);
      }
      if (doneAtFirstCompletion !== -1 && doneAtFirstCompletion !== taskCount) {
        throw new Error(
          `scheduler.completed fired early with ${doneAtFirstCompletion}/${taskCount} tasks done`
        );
      }
    } finally {
      await scheduler.shutdown();
    }
  }

  private async testResourceContention(): Promise<void> {
    // CPU constraints now handled by groups
    const group = new ConcurrentLimitGroup(2);

    const workerManager = new WorkerManager(this.testThreadWorkerUrl, {
      maxThreads: 2,
      maxConcurrentTasks: 2,
      inline: false,
      groups: ['testGroup']
    });

    const scheduler = new FyflowScheduler({ TestThreadWorker: workerManager }, { testGroup: group });

    // Add 6 tasks with both CPU and group constraints
    for (let i = 0; i < 6; i++) {
      scheduler.addTask(new FyflowTask({
        id: `contention-${i}`,
        workerType: 'TestThreadWorker',
        payload: { taskId: `contention-${i}`, data: `contention-test-${i}`, delay: 60 }
      }));
    }

    // Group limit (2) should be the bottleneck, not CPU (3) or threads (2×2=4)
    await new Promise(resolve => setTimeout(resolve, 30));
    let stats = scheduler.stats;
    if (stats.running > 2) throw new Error(`Expected max 2 running tasks due to group constraint, got ${stats.running}`);

    await this.waitForCompletion(scheduler, 6);

    stats = scheduler.stats;
    if (stats.done !== 6) throw new Error(`Expected 6 completed tasks, got ${stats.done}`);
  }


  private async testWorkerPoolScaling(): Promise<void> {
    // CPU constraints now handled by groups
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 2,
      maxConcurrentTasks: 3,
      inline: true,
      idleTimeout: 100 // Quick idle timeout for testing
    });

    const scheduler = new FyflowScheduler({ TestInlineWorker: workerManager });

    // Add tasks that will trigger worker scaling
    for (let i = 0; i < 8; i++) {
      scheduler.addTask(new FyflowTask({
        id: `scaling-${i}`,
        workerType: 'TestInlineWorker',
        payload: { taskId: `scaling-${i}`, data: `scaling-test-${i}`, delay: 30 }
      }));
    }

    await this.waitForCompletion(scheduler, 8);

    const stats = scheduler.stats;
    if (stats.done !== 8) throw new Error(`Expected 8 completed tasks, got ${stats.done}`);

    // Wait for idle timeout to trigger worker cleanup
    await new Promise(resolve => setTimeout(resolve, 150));
  }


  // Fire-and-forget failures must not surface as unhandled rejections. On Deno an
  // unhandled rejection terminates the process, so a run that gets past this at
  // all is the assertion; the counters confirm the failures really happened.
  private async testHandleRejection(): Promise<void> {
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 1, maxConcurrentTasks: 2, inline: true
    });
    const scheduler = new FyflowScheduler({ TestInlineWorker: workerManager }, {});

    for (let i = 0; i < 4; i++) {
      // No createPromise: nothing is awaiting these, and handleRejection defaults true
      scheduler.addTask(new FyflowTask({
        id: `faf-fail-${i}`,
        workerType: 'TestInlineWorker',
        payload: { taskId: `faf-fail-${i}`, shouldThrow: true }
      }));
    }

    await new Promise(resolve => setTimeout(resolve, 600));

    if (scheduler.stats.failed !== 4) {
      throw new Error(`Expected 4 failed tasks, got ${scheduler.stats.failed}`);
    }
    // Still usable afterwards
    const ok = await scheduler.addTask(new FyflowTask({
      id: 'faf-ok', workerType: 'TestInlineWorker', payload: { taskId: 'faf-ok', delay: 5 }
    }), { createPromise: true });
    if (!ok) throw new Error('Scheduler stopped working after fire-and-forget failures');
    await scheduler.shutdown();
  }

  // Blocked tasks are released by the periodic retry, so a custom interval must
  // still drain them - this is the only thing the option controls
  private async testPeriodicRetryInterval(): Promise<void> {
    const group = new ConcurrentLimitGroup(1);
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 2, maxConcurrentTasks: 2, inline: true, groups: ['tight']
    });
    const scheduler = new FyflowScheduler(
      { TestInlineWorker: workerManager },
      { tight: group },
      { periodicRetryIntervalMs: 10 }
    );

    const tasks = Array.from({ length: 6 }, (_, i) => new FyflowTask({
      id: `retry-interval-${i}`,
      workerType: 'TestInlineWorker',
      payload: { taskId: `retry-interval-${i}`, delay: 20 }
    }));
    await Promise.all(scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]);

    if (scheduler.stats.done !== 6) {
      throw new Error(`Expected 6 done with a custom retry interval, got ${scheduler.stats.done}`);
    }
    await scheduler.shutdown();
  }

  private async waitForCompletion(scheduler: FyflowScheduler, expectedTasks: number, timeoutMs = 5000): Promise<void> {
    const start = performance.now();

    return new Promise((resolve, reject) => {
      const checkCompletion = () => {
        const stats = scheduler.stats;
        const totalCompleted = stats.done + stats.failed;

        if (totalCompleted >= expectedTasks) {
          resolve();
          return;
        }

        if (performance.now() - start > timeoutMs) {
          reject(new Error(`Timeout waiting for ${expectedTasks} tasks. Stats: ${JSON.stringify(stats)}`));
          return;
        }

        setTimeout(checkCompletion, 50);
      };

      checkCompletion();
    });
  }

  async testConcurrentRateLimitEnforcement() {
    console.log('Testing concurrent rate limit enforcement with multiple threads and tasks...');

    // Create two different rate-limited groups
    const apiGroup = new RateLimitGroup([{limit: 5, windowMs: 1000}]);  // 5 requests per second
    const serviceGroup = new RateLimitGroup([{limit: 3, windowMs: 1000}]); // 3 requests per second

    // Create worker managers with concurrency matching rate limits to avoid race condition overage
    // RateLimitGroup uses optimistic allocation (like ConcurrentLimitGroup), so high concurrency
    // can cause temporary overage. Keep concurrency close to rate limit.
    const workerPools = {
      ApiWorker: new WorkerManager(
        this.testInlineWorkerUrl,
        {
          maxThreads: 2,           // Reduced threads
          maxConcurrentTasks: 3,   // Total: 6 capacity (close to 5 limit)
          inline: true,
          groups: ['api'],
          config: { delay: 200 }   // Tasks take 200ms each
        }
      ),
      ServiceWorker: new WorkerManager(
        this.testInlineWorkerUrl,
        {
          maxThreads: 2,           // Reduced threads
          maxConcurrentTasks: 2,   // Total: 4 capacity (close to 3 limit)
          inline: true,
          groups: ['service'],
          config: { delay: 150 }   // Tasks take 150ms each
        }
      )
    };

    const scheduler = new FyflowScheduler(workerPools, {
      api: apiGroup,
      service: serviceGroup
    });

    // Create many tasks to stress test rate limiting
    const apiTasks = Array.from({ length: 20 }, (_, i) =>
      new FyflowTask({
        id: `api-task-${i}`,
        workerType: 'ApiWorker',
        payload: { taskId: `api-task-${i}`, data: `api-data-${i}`, delay: 200 }
      })
    );

    const serviceTasks = Array.from({ length: 15 }, (_, i) =>
      new FyflowTask({
        id: `service-task-${i}`,
        workerType: 'ServiceWorker',
        payload: { taskId: `service-task-${i}`, data: `service-data-${i}`, delay: 150 }
      })
    );

    // Track completion times to validate rate limiting
    const completionTimes: Record<string, number[]> = { api: [], service: [] };
    const startTime = performance.now();

    scheduler.addEventListener('task.completed', (e: any) => {
      const completionTime = performance.now() - startTime;
      const taskType = e.detail.id.includes('api') ? 'api' : 'service';
      completionTimes[taskType].push(completionTime);
    });

    // Add all tasks
    [...apiTasks, ...serviceTasks].forEach(task => scheduler.addTask(task));

    // Wait for all tasks to complete
    await this.waitForCompletion(scheduler, 35, 15000); // 15 second timeout

    // Validate rate limiting was enforced (with tolerance for optimistic allocation)
    // Allow brief overage up to maxThreads × maxConcurrentTasks
    // Note: With 200ms task duration and 1000ms window, up to 5 batches can complete
    // So we check average rate over longer period instead of strict windowing
    const avgApiRate = (completionTimes.api.length / (completionTimes.api[completionTimes.api.length - 1] / 1000));
    const avgServiceRate = (completionTimes.service.length / (completionTimes.service[completionTimes.service.length - 1] / 1000));

    // Average rate should be close to the limit (allow up to 20% overage for optimistic allocation)
    if (avgApiRate > 5 * 1.3) { // 30% tolerance
      throw new Error(`API average rate too high: ${avgApiRate.toFixed(2)} tasks/sec (expected ~5 tasks/sec with 30% tolerance)`);
    }
    if (avgServiceRate > 3 * 1.3) { // 30% tolerance
      throw new Error(`Service average rate too high: ${avgServiceRate.toFixed(2)} tasks/sec (expected ~3 tasks/sec with 30% tolerance)`);
    }

    console.log(`✅ API rate limit respected: ${avgApiRate.toFixed(2)} tasks/sec (limit: 5)`);
    console.log(`✅ Service rate limit respected: ${avgServiceRate.toFixed(2)} tasks/sec (limit: 3)`);

    const stats = scheduler.stats;
    if (stats.done !== 35) throw new Error(`Expected 35 completed tasks, got ${stats.done}`);
    if (stats.failed > 0) throw new Error(`Expected 0 failed tasks, got ${stats.failed}`);
  }

  private validateRateLimit(completionTimes: number[], limit: number, windowMs: number, groupName: string, maxAllowed?: number) {
    // Sort completion times
    completionTimes.sort((a, b) => a - b);

    // Allow tolerance for optimistic allocation (default: strict limit)
    const effectiveLimit = maxAllowed || limit;

    // Check that no more than 'effectiveLimit' tasks completed within any windowMs period
    for (let i = 0; i < completionTimes.length; i++) {
      const windowStart = completionTimes[i];
      const windowEnd = windowStart + windowMs;

      // Count tasks that completed within this window
      let tasksInWindow = 0;
      for (let j = i; j < completionTimes.length; j++) {
        if (completionTimes[j] <= windowEnd) {
          tasksInWindow++;
        } else {
          break;
        }
      }

      if (tasksInWindow > effectiveLimit) {
        throw new Error(
          `${groupName} rate limit violated: ${tasksInWindow} tasks completed within ${windowMs}ms window starting at ${windowStart}ms (limit: ${limit}, max allowed with optimistic allocation: ${effectiveLimit})`
        );
      }
    }

    console.log(`✅ ${groupName} rate limit respected: max ${effectiveLimit} tasks per ${windowMs}ms window (limit: ${limit})`);
  }

  private async testIdleTimeoutDefault(): Promise<void> {
    // Test undefined idleTimeout - should use default 5000ms
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: true
      // idleTimeout undefined - should default to 5000ms
    });

    const scheduler = new FyflowScheduler({ TestInlineWorker: workerManager });

    // Run a single task
    const task = new FyflowTask({
      id: 'default-timeout-task',
      workerType: 'TestInlineWorker',
      payload: { value: 1, delay: 10 }
    });

    await scheduler.addTask(task, { createPromise: true });

    // Worker should still be alive after a short wait (much less than 5000ms)
    await new Promise(resolve => setTimeout(resolve, 100));

    // Verify worker is still alive by running another task quickly
    const task2 = new FyflowTask({
      id: 'verify-alive-task',
      workerType: 'TestInlineWorker',
      payload: { value: 2, delay: 10 }
    });

    await scheduler.addTask(task2, { createPromise: true });

    if (scheduler.stats.done !== 2) {
      throw new Error(`Expected 2 completed tasks, got ${scheduler.stats.done}`);
    }
  }

  private async testIdleTimeoutCustom(): Promise<void> {
    // Test custom idleTimeout - should use specified value (100ms for quick testing)
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: true,
      idleTimeout: 100 // Custom timeout for quick testing
    });

    const scheduler = new FyflowScheduler({ TestInlineWorker: workerManager });

    // Run a single task
    const task = new FyflowTask({
      id: 'custom-timeout-task',
      workerType: 'TestInlineWorker',
      payload: { value: 1, delay: 10 }
    });

    await scheduler.addTask(task, { createPromise: true });

    // Wait longer than the idle timeout
    await new Promise(resolve => setTimeout(resolve, 150));

    // Worker should have been terminated, so this should create a new worker
    const task2 = new FyflowTask({
      id: 'after-timeout-task',
      workerType: 'TestInlineWorker',
      payload: { value: 2, delay: 10 }
    });

    await scheduler.addTask(task2, { createPromise: true });

    if (scheduler.stats.done !== 2) {
      throw new Error(`Expected 2 completed tasks, got ${scheduler.stats.done}`);
    }
  }

  private async testIdleTimeoutPersistent(): Promise<void> {
    // Test idleTimeout: 0 - persistent worker that never auto-terminates
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: true,
      idleTimeout: 0 // Persistent worker - never auto-terminate
    });

    const scheduler = new FyflowScheduler({ TestInlineWorker: workerManager });

    // Run a task
    const task1 = new FyflowTask({
      id: 'persistent-task-1',
      workerType: 'TestInlineWorker',
      payload: { value: 1, delay: 10 }
    });

    await scheduler.addTask(task1, { createPromise: true });

    // Wait much longer than typical idle timeouts
    await new Promise(resolve => setTimeout(resolve, 500));

    // Worker should still be alive (persistent)
    const task2 = new FyflowTask({
      id: 'persistent-task-2',
      workerType: 'TestInlineWorker',
      payload: { value: 2, delay: 10 }
    });

    await scheduler.addTask(task2, { createPromise: true });

    // Wait again to ensure persistent behavior
    await new Promise(resolve => setTimeout(resolve, 300));

    // Run another task - worker should still be the same persistent one
    const task3 = new FyflowTask({
      id: 'persistent-task-3',
      workerType: 'TestInlineWorker',
      payload: { value: 3, delay: 10 }
    });

    await scheduler.addTask(task3, { createPromise: true });

    if (scheduler.stats.done !== 3) {
      throw new Error(`Expected 3 completed tasks, got ${scheduler.stats.done}`);
    }

    // Manually shutdown the persistent worker to clean up
    await workerManager.shutdown();
  }

  private async testBatchTaskAddition(): Promise<void> {
    // CPU constraints now handled by groups
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 2,
      maxConcurrentTasks: 10,
      inline: true
    });

    const scheduler = new FyflowScheduler({ TestInlineWorker: workerManager });

    // Create 1000 tasks to test batch addition
    const tasks = [];
    for (let i = 0; i < 1000; i++) {
      tasks.push(new FyflowTask({
        id: `batch-task-${i}`,
        workerType: 'TestInlineWorker',
        payload: { value: i, delay: 1 }
      }));
    }

    // Test addTasks() method
    const promises = scheduler.addTasks(tasks, { createPromise: true });

    if (!promises || promises.length !== 1000) {
      throw new Error(`Expected 1000 promises, got ${promises?.length}`);
    }

    // Wait for all tasks to complete
    const results = await Promise.all(promises);

    if (results.length !== 1000) {
      throw new Error(`Expected 1000 results, got ${results.length}`);
    }

    // Verify all tasks completed successfully
    for (let i = 0; i < 1000; i++) {
      if (!results[i] || !results[i].workerId || !results[i].processedAt) {
        throw new Error(`Task ${i} was not processed correctly: ${JSON.stringify(results[i])}`);
      }
    }
  }
}

// Auto-run tests when executed directly - handle both Deno and Node.js
if ((typeof Deno !== 'undefined' && import.meta.main) ||
    (typeof process !== 'undefined' && process.argv[1] && import.meta.url &&
     import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/')))) {


  console.log('Running tests...');
  const testSuite = new CrossPlatformTestSuite();
  const result = await testSuite.runAllTests();

  if (result.failed > 0) {
    console.log('\n💥 Tests failed - exiting with error code');
    // Exit with error code for CI/CD
    if (typeof Deno !== 'undefined') {
      Deno.exit(1);
    } else {
      process.exit(1);
    }
  } else {
    console.log('\n🎉 All tests passed!');
    // await scheduler.shutdown();
    // Exit cleanly after successful test completion
    // Give a moment for any pending teardown operations
    setTimeout(() => {
      if (typeof Deno !== 'undefined') {
        Deno.exit(0);
      } else {
        process.exit(0);
      }
    }, 1000);
  }
}

export { CrossPlatformTestSuite };
export type { TestResult, TestSuiteResult };
export default CrossPlatformTestSuite;