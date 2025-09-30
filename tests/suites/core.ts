// Comprehensive Cross-Platform Test Suite for FyFlow
// Tests core functionality across Deno and Node.js platforms


import { WorkerManager, DagScheduler, DagTask } from '../../index.ts';
import { ConcurrentLimitGroup } from '../../groups/concurrentLimitGroup.ts';
import { RateLimitGroup } from '../../groups/rateLimitGroup.ts';

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

  async runAllTests(exitOnComplete = true): Promise<TestSuiteResult> {

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
    this.results.push(await this.runTest('Simple Task Chain', () => this.testSimpleTaskChain()));
    this.results.push(await this.runTest('Parallel Tasks', () => this.testParallelTasks()));
    this.results.push(await this.runTest('Mixed Worker Types', () => this.testMixedWorkerTypes()));

    // Resource Management Tests
    this.results.push(await this.runTest('CPU Slot Enforcement', () => this.testCPUSlotEnforcement()));
    this.results.push(await this.runTest('Group Limit Enforcement', () => this.testGroupLimitEnforcement()));
    this.results.push(await this.runTest('Resource Contention', () => this.testResourceContention()));

    // Edge Cases and Error Handling
    this.results.push(await this.runTest('Task Dependency Resolution', () => this.testTaskDependencies()));
    this.results.push(await this.runTest('Worker Pool Scaling', () => this.testWorkerPoolScaling()));
    this.results.push(await this.runTest('Scheduler Completion Detection', () => this.testSchedulerCompletion()));

    // Rate Limiting Stress Tests
    this.results.push(await this.runTest('Concurrent Rate Limit Enforcement', () => this.testConcurrentRateLimitEnforcement()));

    // Idle Timeout Tests
    this.results.push(await this.runTest('Idle Timeout Default (5000ms)', () => this.testIdleTimeoutDefault()));
    this.results.push(await this.runTest('Idle Timeout Custom (100ms)', () => this.testIdleTimeoutCustom()));
    this.results.push(await this.runTest('Idle Timeout Persistent (0=never)', () => this.testIdleTimeoutPersistent()));

    // Batch API Tests
    this.results.push(await this.runTest('Batch Task Addition', () => this.testBatchTaskAddition()));

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

    const task = new DagTask({
      id: 'inline-single',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'inline-single', data: 'test', delay: 10 }
    });

    const scheduler = new DagScheduler({ TestInlineWorker: workerManager });
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

    const scheduler = new DagScheduler({ TestInlineWorker: workerManager });

    // Add 5 tasks to test concurrent execution
    for (let i = 0; i < 5; i++) {
      const task = new DagTask({
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

    const task = new DagTask({
      id: 'thread-single',
      workerType: 'TestThreadWorker',
      payload: { taskId: 'thread-single', data: 'test', delay: 50 }
    });

    const scheduler = new DagScheduler({ TestThreadWorker: workerManager });
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

    const scheduler = new DagScheduler({ TestThreadWorker: workerManager });

    // Add 4 tasks to test multiple threads with concurrent execution
    for (let i = 0; i < 4; i++) {
      const task = new DagTask({
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

  private async testSimpleTaskChain(): Promise<void> {
    // CPU constraints now handled by groups
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: true
    });

    const scheduler = new DagScheduler({ TestInlineWorker: workerManager });

    // Create task chain: A → B → C
    const taskA = new DagTask({
      id: 'chain-a',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'chain-a', data: 'step-a', delay: 10 }
    });

    const taskB = new DagTask({
      id: 'chain-b',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'chain-b', data: 'step-b', delay: 10 },
      parents: ['chain-a']
    });

    const taskC = new DagTask({
      id: 'chain-c',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'chain-c', data: 'step-c', delay: 10 },
      parents: ['chain-b']
    });

    scheduler.addTask(taskA);
    scheduler.addTask(taskB);
    scheduler.addTask(taskC);

    await this.waitForCompletion(scheduler, 3);

    const stats = scheduler.stats;
    if (stats.done !== 3) throw new Error(`Expected 3 completed tasks, got ${stats.done}`);
  }

  private async testParallelTasks(): Promise<void> {
    // CPU constraints now handled by groups
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 3,
      maxConcurrentTasks: 1,
      inline: true
    });

    const scheduler = new DagScheduler({ TestInlineWorker: workerManager });

    // Create 6 independent parallel tasks
    for (let i = 0; i < 6; i++) {
      const task = new DagTask({
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

    const scheduler = new DagScheduler({
      TestInlineWorker: inlineManager,
      TestThreadWorker: threadManager
    });

    // Mix of inline and threaded tasks
    scheduler.addTask(new DagTask({
      id: 'mixed-inline-1',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'mixed-inline-1', data: 'inline', delay: 20 }
    }));

    scheduler.addTask(new DagTask({
      id: 'mixed-thread-1',
      workerType: 'TestThreadWorker',
      payload: { taskId: 'mixed-thread-1', data: 'thread', delay: 30 }
    }));

    scheduler.addTask(new DagTask({
      id: 'mixed-inline-2',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'mixed-inline-2', data: 'inline', delay: 15 }
    }));

    await this.waitForCompletion(scheduler, 3);

    const stats = scheduler.stats;
    if (stats.done !== 3) throw new Error(`Expected 3 completed tasks, got ${stats.done}`);
  }

  private async testCPUSlotEnforcement(): Promise<void> {
    // CPU constraints now handled by groups - test that CPU group limits are enforced
    const cpuGroup = new ConcurrentLimitGroup(2); // Only 2 concurrent CPU tasks

    // Track group usage to verify enforcement
    let maxConcurrentTasks = 0;
    const originalOnStart = cpuGroup.onStart.bind(cpuGroup);
    cpuGroup.onStart = function() {
      originalOnStart();
      maxConcurrentTasks = Math.max(maxConcurrentTasks, this.running);
    };

    const threadManager = new WorkerManager(this.testThreadWorkerUrl, {
      maxThreads: 4, // More threads than CPU group limit
      maxConcurrentTasks: 1,
      inline: false
    });

    const scheduler = new DagScheduler({ TestThreadWorker: threadManager }, { cpu: cpuGroup });

    // Add 4 tasks with CPU group constraint
    for (let i = 0; i < 4; i++) {
      scheduler.addTask(new DagTask({
        id: `cpu-limit-${i}`,
        workerType: 'TestThreadWorker',
        payload: { taskId: `cpu-limit-${i}`, data: `cpu-test-${i}`, delay: 200 },
        workerGroups: ['cpu'] // Assign to CPU group
      }));
      await new Promise(resolve => setTimeout(resolve, 10)); // Stagger additions
    }

    await this.waitForCompletion(scheduler, 4);

    // Verify that never more than 2 tasks ran concurrently (CPU group limit)
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

    const scheduler = new DagScheduler({ TestInlineWorker: workerManager }, { testGroup: group });

    // Add 4 tasks to group-constrained worker
    for (let i = 0; i < 4; i++) {
      scheduler.addTask(new DagTask({
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

  private async testResourceContention(): Promise<void> {
    // CPU constraints now handled by groups
    const group = new ConcurrentLimitGroup(2);

    const workerManager = new WorkerManager(this.testThreadWorkerUrl, {
      maxThreads: 2,
      maxConcurrentTasks: 2,
      inline: false,
      groups: ['testGroup']
    });

    const scheduler = new DagScheduler({ TestThreadWorker: workerManager }, { testGroup: group });

    // Add 6 tasks with both CPU and group constraints
    for (let i = 0; i < 6; i++) {
      scheduler.addTask(new DagTask({
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

  private async testTaskDependencies(): Promise<void> {
    // CPU constraints now handled by groups
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 2,
      maxConcurrentTasks: 1,
      inline: true
    });

    const scheduler = new DagScheduler({ TestInlineWorker: workerManager });

    // Complex dependency tree: A → B, A → C, B → D, C → D
    scheduler.addTask(new DagTask({
      id: 'dep-a',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'dep-a', data: 'root', delay: 10 }
    }));

    scheduler.addTask(new DagTask({
      id: 'dep-b',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'dep-b', data: 'branch-1', delay: 10 },
      parents: ['dep-a']
    }));

    scheduler.addTask(new DagTask({
      id: 'dep-c',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'dep-c', data: 'branch-2', delay: 10 },
      parents: ['dep-a']
    }));

    scheduler.addTask(new DagTask({
      id: 'dep-d',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'dep-d', data: 'merge', delay: 10 },
      parents: ['dep-b', 'dep-c']
    }));

    await this.waitForCompletion(scheduler, 4);

    const stats = scheduler.stats;
    if (stats.done !== 4) throw new Error(`Expected 4 completed tasks, got ${stats.done}`);
  }

  private async testWorkerPoolScaling(): Promise<void> {
    // CPU constraints now handled by groups
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 2,
      maxConcurrentTasks: 3,
      inline: true,
      idleTimeout: 100 // Quick idle timeout for testing
    });

    const scheduler = new DagScheduler({ TestInlineWorker: workerManager });

    // Add tasks that will trigger worker scaling
    for (let i = 0; i < 8; i++) {
      scheduler.addTask(new DagTask({
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

  private async testSchedulerCompletion(): Promise<void> {
    // CPU constraints now handled by groups
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 2,
      maxConcurrentTasks: 2,
      inline: true
    });

    const scheduler = new DagScheduler({ TestInlineWorker: workerManager });

    // Test completion detection with mixed task types
    scheduler.addTask(new DagTask({
      id: 'completion-1',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'completion-1', data: 'test-1', delay: 20 }
    }));

    scheduler.addTask(new DagTask({
      id: 'completion-2',
      workerType: 'TestInlineWorker',
      payload: { taskId: 'completion-2', data: 'test-2', delay: 30 },
      parents: ['completion-1']
    }));

    await this.waitForCompletion(scheduler, 2);

    const stats = scheduler.stats;
    if (stats.done !== 2) throw new Error(`Expected 2 completed tasks, got ${stats.done}`);
    if (stats.running !== 0) throw new Error(`Expected 0 running tasks at completion, got ${stats.running}`);
    if (stats.queued !== 0) throw new Error(`Expected 0 queued tasks at completion, got ${stats.queued}`);
  }

  private async waitForCompletion(scheduler: DagScheduler, expectedTasks: number, timeoutMs = 5000): Promise<void> {
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

    // Create worker managers with high concurrency
    const workerPools = {
      ApiWorker: new WorkerManager(
        this.testInlineWorkerUrl,
        {
          maxThreads: 3,           // Multiple threads
          maxConcurrentTasks: 4,   // Multiple tasks per thread
          inline: true,
          groups: ['api'],
          config: { delay: 200 }   // Tasks take 200ms each
        }
      ),
      ServiceWorker: new WorkerManager(
        this.testInlineWorkerUrl,
        {
          maxThreads: 2,           // Multiple threads
          maxConcurrentTasks: 3,   // Multiple tasks per thread
          inline: true,
          groups: ['service'],
          config: { delay: 150 }   // Tasks take 150ms each
        }
      )
    };

    const scheduler = new DagScheduler(workerPools, {
      api: apiGroup,
      service: serviceGroup
    });

    // Create many tasks to stress test rate limiting
    const apiTasks = Array.from({ length: 20 }, (_, i) =>
      new DagTask({
        id: `api-task-${i}`,
        workerType: 'ApiWorker',
        payload: { taskId: `api-task-${i}`, data: `api-data-${i}`, delay: 200 }
      })
    );

    const serviceTasks = Array.from({ length: 15 }, (_, i) =>
      new DagTask({
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

    // Validate rate limiting was enforced
    this.validateRateLimit(completionTimes.api, 5, 1000, 'API');
    this.validateRateLimit(completionTimes.service, 3, 1000, 'Service');

    const stats = scheduler.stats;
    if (stats.done !== 35) throw new Error(`Expected 35 completed tasks, got ${stats.done}`);
    if (stats.failed > 0) throw new Error(`Expected 0 failed tasks, got ${stats.failed}`);
  }

  private validateRateLimit(completionTimes: number[], limit: number, windowMs: number, groupName: string) {
    // Sort completion times
    completionTimes.sort((a, b) => a - b);

    // Check that no more than 'limit' tasks completed within any windowMs period
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

      if (tasksInWindow > limit) {
        throw new Error(
          `${groupName} rate limit violated: ${tasksInWindow} tasks completed within ${windowMs}ms window starting at ${windowStart}ms (limit: ${limit})`
        );
      }
    }

    console.log(`✅ ${groupName} rate limit respected: max ${limit} tasks per ${windowMs}ms window`);
  }

  private async testIdleTimeoutDefault(): Promise<void> {
    // Test undefined idleTimeout - should use default 5000ms
    const workerManager = new WorkerManager(this.testInlineWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: true
      // idleTimeout undefined - should default to 5000ms
    });

    const scheduler = new DagScheduler({ TestInlineWorker: workerManager });

    // Run a single task
    const task = new DagTask({
      id: 'default-timeout-task',
      workerType: 'TestInlineWorker',
      payload: { value: 1, delay: 10 }
    });

    await scheduler.addTask(task);

    // Worker should still be alive after a short wait (much less than 5000ms)
    await new Promise(resolve => setTimeout(resolve, 100));

    // Verify worker is still alive by running another task quickly
    const task2 = new DagTask({
      id: 'verify-alive-task',
      workerType: 'TestInlineWorker',
      payload: { value: 2, delay: 10 }
    });

    await scheduler.addTask(task2);

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

    const scheduler = new DagScheduler({ TestInlineWorker: workerManager });

    // Run a single task
    const task = new DagTask({
      id: 'custom-timeout-task',
      workerType: 'TestInlineWorker',
      payload: { value: 1, delay: 10 }
    });

    await scheduler.addTask(task);

    // Wait longer than the idle timeout
    await new Promise(resolve => setTimeout(resolve, 150));

    // Worker should have been terminated, so this should create a new worker
    const task2 = new DagTask({
      id: 'after-timeout-task',
      workerType: 'TestInlineWorker',
      payload: { value: 2, delay: 10 }
    });

    await scheduler.addTask(task2);

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

    const scheduler = new DagScheduler({ TestInlineWorker: workerManager });

    // Run a task
    const task1 = new DagTask({
      id: 'persistent-task-1',
      workerType: 'TestInlineWorker',
      payload: { value: 1, delay: 10 }
    });

    await scheduler.addTask(task1);

    // Wait much longer than typical idle timeouts
    await new Promise(resolve => setTimeout(resolve, 500));

    // Worker should still be alive (persistent)
    const task2 = new DagTask({
      id: 'persistent-task-2',
      workerType: 'TestInlineWorker',
      payload: { value: 2, delay: 10 }
    });

    await scheduler.addTask(task2);

    // Wait again to ensure persistent behavior
    await new Promise(resolve => setTimeout(resolve, 300));

    // Run another task - worker should still be the same persistent one
    const task3 = new DagTask({
      id: 'persistent-task-3',
      workerType: 'TestInlineWorker',
      payload: { value: 3, delay: 10 }
    });

    await scheduler.addTask(task3);

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

    const scheduler = new DagScheduler({ TestInlineWorker: workerManager });

    // Create 1000 tasks to test batch addition
    const tasks = [];
    for (let i = 0; i < 1000; i++) {
      tasks.push(new DagTask({
        id: `batch-task-${i}`,
        workerType: 'TestInlineWorker',
        payload: { value: i, delay: 1 }
      }));
    }

    // Test addTasks() method
    const promises = scheduler.addTasks(tasks);

    if (promises.length !== 1000) {
      throw new Error(`Expected 1000 promises, got ${promises.length}`);
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