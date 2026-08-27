// Comprehensive Error Handling Tests for FyFlow
// Tests worker crashes, self-termination, and error recovery scenarios

import { WorkerManager, FyflowScheduler, FyflowTask } from '../../index.ts';
import { ConcurrentLimitGroup } from '../../groups/concurrentLimitGroup.ts';

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

class ErrorHandlingTestSuite {
  private results: TestResult[] = [];
  private startTime = 0;
  private crashWorkerUrl: string = "";
  private selfTerminatingWorkerUrl: string = "";
  private schedulers: FyflowScheduler[] = []; // Track all created schedulers for cleanup

  // Helper method to create scheduler and track it for cleanup
  private createScheduler(workerPools: any, groups: Record<string, any> = {}): FyflowScheduler {
    const scheduler = new FyflowScheduler(workerPools, groups);
    this.schedulers.push(scheduler);
    return scheduler;
  }

  // Cleanup all created schedulers (which will cleanup their WorkerManagers)
  async cleanup(): Promise<void> {
    console.log(`🔄 Cleaning up ${this.schedulers.length} schedulers...`);
    const cleanupPromises = this.schedulers.map(async (scheduler) => {
      try {
        await scheduler.shutdown();
      } catch (error) {
        console.warn(`⚠️ Error during scheduler cleanup:`, error);
      }
    });
    let i = 0;
    for (const cleanupPromise of cleanupPromises) {
      await cleanupPromise;
      i++;
      // console.log(`✅ Cleanup completed ${i} of ${cleanupPromises.length}`);
    }
    // await Promise.allSettled(cleanupPromises);
    this.schedulers = [];
    console.log(`✅ Cleanup completed`);
  }

  private async runTest(name: string, testFn: () => Promise<void>): Promise<TestResult> {
    // Initialize worker URLs based on platform
    if (typeof Deno !== "undefined") {
      this.crashWorkerUrl = new URL("../workers/crashingWorker.ts", import.meta.url).href;
      this.selfTerminatingWorkerUrl = new URL("../workers/selfTerminatingWorker.ts", import.meta.url).href;
    } else {
      // Node/Browser esbuild replaces this
      // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
      this.crashWorkerUrl = new URL((await import('../workers/crashingWorker.ts?worker-direct')).default).href;
      // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
      this.selfTerminatingWorkerUrl = new URL((await import('../workers/selfTerminatingWorker.ts?worker-direct')).default).href;
    }

    console.log(`\n🧪 Running: ${name}`);
    const start = performance.now();

    try {
      // Add timeout protection for all tests
      await Promise.race([
        testFn(),
        new Promise((_, reject) => setTimeout(() => reject(new Error('Test timeout - exceeded 10 seconds')), 10000))
      ]);
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
    const platform = typeof globalThis !== 'undefined' && 'Deno' in globalThis ? 'Deno' : 'Node.js';
    console.log(`🚀 FyFlow Error Handling Test Suite - ${platform}`);
    console.log('='.repeat(60));

    this.startTime = performance.now();
    this.results = [];

    // Core Worker Self-Termination Tests
    this.results.push(await this.runTest('Worker Self-Termination with canRestart=false', () => this.testWorkerSelfTerminationNoRestart()));
    this.results.push(await this.runTest('Worker Self-Termination from Setup', () => this.testWorkerSelfTerminationFromConstructor()));

    // Core Worker Crash Tests
    this.results.push(await this.runTest('Worker Runtime Crash during Task Execution', () => this.testWorkerRuntimeCrash()));
    this.results.push(await this.runTest('Worker Setup Method Failure', () => this.testWorkerSetupFailure()));
    this.results.push(await this.runTest('Worker Initialization Failure', () => this.testWorkerInitializationFailure()));

    // Core Event Emission Tests
    this.results.push(await this.runTest('Worker Failed Event Emission', () => this.testWorkerFailedEventEmission()));
    this.results.push(await this.runTest('Worker Self-Terminated Event Emission', () => this.testWorkerSelfTerminatedEventEmission()));

    // Core Resource Management Tests
    this.results.push(await this.runTest('maxThreads Slot Management on Failure', () => this.testMaxThreadsSlotManagement()));
    this.results.push(await this.runTest('Worker Status Inspection during Failure', () => this.testWorkerStatusInspection()));

    // Task Requeuing Test (Simplified)
    this.results.push(await this.runTest('Task Requeuing on Worker Failure', () => this.testTaskRequeuingOnWorkerFailure()));

    // Worker Management API Test
    this.results.push(await this.runTest('Worker Restart after Failure', () => this.testWorkerRestart()));

    const totalDuration = performance.now() - this.startTime;
    const passed = this.results.filter(r => r.passed).length;
    const failed = this.results.length - passed;

    console.log(`\n📊 Error Handling Test Summary - ${platform}`);
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

      // Exit with error for CI/CD
      if (exitOnComplete) {
        if (typeof Deno !== 'undefined') {
          Deno.exit(1);
        } else {
          process.exit(1);
        }
      }
    } else {
      console.log('\n🎉 All error handling tests passed!');
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


  // Test worker self-termination without restart
  private async testWorkerSelfTerminationNoRestart(): Promise<void> {
    const workerManager = new WorkerManager(this.selfTerminatingWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: false
    });

    const scheduler = this.createScheduler({ SelfTerminatingWorker: workerManager });

    let workerFailedEmitted = false;
    let restartMetadata: any = null;

    workerManager.addEventListener('worker.failed', (e: any) => {
      workerFailedEmitted = true;
      restartMetadata = e.detail.metadata;
    });

    const task = new FyflowTask({
      id: 'self-terminate-no-restart',
      workerType: 'SelfTerminatingWorker',
      payload: { action: 'self-terminate', canRestart: false }
    });

    try {
      await scheduler.addTask(task, { createPromise: true });
      throw new Error('Task should have failed due to worker self-termination');
    } catch (error: any) {
      // Expected
    }

    await new Promise(resolve => setTimeout(resolve, 100));

    if (!workerFailedEmitted) throw new Error('worker.failed event not emitted');
    if (restartMetadata?.canRestart !== false) throw new Error('canRestart should be false');
  }

  // Test worker self-termination from setup
  private async testWorkerSelfTerminationFromConstructor(): Promise<void> {
    const workerManager = new WorkerManager(this.selfTerminatingWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: true, // Use inline mode to avoid threading complexity
      config: { terminateInSetup: true, canRestart: false }
    });

    const scheduler = this.createScheduler({ SelfTerminatingWorker: workerManager });

    let workerFailedEmitted = false;
    let workerFailedPromise = new Promise(resolve => {
      workerManager.addEventListener('worker.failed', (e: any) => {
        workerFailedEmitted = true;
        resolve(e.detail);
      });
    });

    const task = new FyflowTask({
      id: 'self-terminate-setup',
      workerType: 'SelfTerminatingWorker',
      payload: { action: 'normal-task' } // Try normal task, but worker will terminate in setup
    });

    // Start task in background
    const taskPromise = scheduler.addTask(task, { createPromise: true })!.catch(() => {
      // Expected to fail
    });

    // Wait for either worker.failed event or timeout
    try {
      await Promise.race([
        workerFailedPromise,
        new Promise((_, reject) => setTimeout(() => reject(new Error('Worker failed event timeout')), 3000))
      ]);
    } catch (error: any) {
      throw new Error(`Setup termination test failed: ${error.message}`);
    }

    // Give the task promise a moment to settle
    await Promise.race([taskPromise, new Promise(resolve => setTimeout(resolve, 100))]);

    if (!workerFailedEmitted) throw new Error('worker.failed event not emitted for setup termination');
  }

  // Test worker runtime crash during task execution
  private async testWorkerRuntimeCrash(): Promise<void> {
    const workerManager = new WorkerManager(this.crashWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: true // Use inline mode to avoid thread import issues
    });

    const scheduler = this.createScheduler({ CrashingWorker: workerManager });

    // Track task failure events (not worker failure - worker should stay healthy)
    let taskFailedEmitted = false;
    let taskFailedPromise = new Promise(resolve => {
      scheduler.addEventListener('task.failed', (e: any) => {
        taskFailedEmitted = true;
        resolve(e.detail);
      });
    });

    const task = new FyflowTask({
      id: 'crash-test',
      workerType: 'CrashingWorker',
      payload: { action: 'throw-error' }
    });

    // Task should fail but worker should remain healthy
    try {
      await scheduler.addTask(task, { createPromise: true });
      throw new Error('Task should have failed due to runtime error');
    } catch (error: any) {
      // Expected - task should fail
    }

    // Wait for task failed event
    await Promise.race([
      taskFailedPromise,
      new Promise((_, reject) => setTimeout(() => reject(new Error('Task failed event timeout')), 1000))
    ]);

    // Give time for events to settle
    await new Promise(resolve => setTimeout(resolve, 100));

    // Verify that task failed event was emitted (but worker stayed healthy)
    if (!taskFailedEmitted) {
      throw new Error('Task crash was not detected - task.failed event not emitted');
    }

    // Verify worker is still healthy and can process another task
    const workerIds = workerManager.getWorkerIds();
    if (workerIds.length === 0) throw new Error('Worker should still exist after task crash');

    const healthyTask = new FyflowTask({
      id: 'healthy-test',
      workerType: 'CrashingWorker',
      payload: { action: 'normal-task' }
    });

    // Worker should be able to process a normal task after the crash
    const result = await scheduler.addTask(healthyTask, { createPromise: true });
    if (!result || !result.result) throw new Error('Worker should be able to process tasks after a task crash');
  }

  // Test worker initialization failure
  private async testWorkerInitializationFailure(): Promise<void> {
    const workerManager = new WorkerManager(this.crashWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: false,
      // crashOnInit is read from worker config, not the task payload - the
      // constructor throws before any task runs
      config: { crashOnInit: true }
    });

    const scheduler = this.createScheduler({ CrashingWorker: workerManager });

    let initFailedEmitted = false;

    workerManager.addEventListener('worker.initialization.failed', (e: any) => {
      initFailedEmitted = true;
    });

    const task = new FyflowTask({
      id: 'init-failure-test',
      workerType: 'CrashingWorker',
      payload: { action: 'crash-on-init' }
    });

    try {
      await scheduler.addTask(task, { createPromise: true });
      throw new Error('Task should have failed due to initialization failure');
    } catch (error: any) {
      // Expected
    }

    await new Promise(resolve => setTimeout(resolve, 200));

    // worker.initialization.failed is forwarded by WorkerManager, so a crash
    // during worker creation is observable rather than silent
    if (!initFailedEmitted) {
      throw new Error('worker.initialization.failed event not emitted');
    }
  }

  // Test worker setup method failure
  private async testWorkerSetupFailure(): Promise<void> {
    const workerManager = new WorkerManager(this.crashWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: true, // Use inline mode
      config: { crashInSetup: true } // Pass config to trigger setup crash
    });

    const scheduler = this.createScheduler({ CrashingWorker: workerManager });

    // Track worker failure events
    let workerFailedEmitted = false;
    let workerFailedPromise = new Promise(resolve => {
      workerManager.addEventListener('worker.failed', (e: any) => {
        workerFailedEmitted = true;
        resolve(e.detail);
      });
    });

    const task = new FyflowTask({
      id: 'setup-failure-test',
      workerType: 'CrashingWorker',
      payload: { action: 'normal-task' } // Try normal task, but setup will crash
    });

    // Start task in background
    const taskPromise = scheduler.addTask(task, { createPromise: true })!.catch((error) => {
      // Expected to fail due to setup crash
      return { taskFailed: true, error: error.message };
    });

    // Wait for worker failure event or timeout
    try {
      await Promise.race([
        workerFailedPromise,
        new Promise((_, reject) => setTimeout(() => reject(new Error('Setup failure timeout')), 3000))
      ]);
    } catch (error: any) {
      throw new Error(`Setup failure test failed: ${error.message}`);
    }

    // Give the task promise time to settle
    await Promise.race([taskPromise, new Promise(resolve => setTimeout(resolve, 100))]);

    if (!workerFailedEmitted) {
      throw new Error('Worker setup failure was not detected - worker.failed event not emitted');
    }
  }

  // Test task requeuing on worker failure
  private async testTaskRequeuingOnWorkerFailure(): Promise<void> {
    const workerManager = new WorkerManager(this.selfTerminatingWorkerUrl, {
      maxThreads: 2,
      maxConcurrentTasks: 1,
      inline: false,
      requeueFailedTasks: true
    });

    const scheduler = this.createScheduler({ SelfTerminatingWorker: workerManager });

    let taskRequeuedEmitted = false;

    workerManager.addEventListener('task.requeue_required', (e: any) => {
      taskRequeuedEmitted = true;
    });

    const task = new FyflowTask({
      id: 'requeue-test',
      workerType: 'SelfTerminatingWorker',
      retryPolicy: { maxRetries: 1 },
      payload: { action: 'self-terminate-delayed', delay: 50 }
    });

    scheduler.addEventListener('task.failed', (e: any) => {
      console.log('📡 task.failed event:', e.detail);
    });


    scheduler.addEventListener('task.requeue_required', (e: any) => {
      console.log('📡 task.requeue_required event:', e.detail);
    });

    try {
      scheduler.addTask(task);
      // await task.onCompleteDescendants()
      // console.log('✅ task.onCompleteDescendants completed', task);
      await new Promise(resolve => setTimeout(resolve, 200));
    } catch (error: any) {
      // May or may not throw depending on timing
    }

    if (!taskRequeuedEmitted) throw new Error('task.requeue_required event not emitted');
  }

  // Test multiple task requeuing
  private async testMultipleTaskRequeuing(): Promise<void> {
    const workerManager = new WorkerManager(this.selfTerminatingWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 3,
      inline: false,
      requeueFailedTasks: true
    });

    const scheduler = this.createScheduler({ SelfTerminatingWorker: workerManager });

    let requeuedTasks = 0;

    workerManager.addEventListener('task.requeue_required', (e: any) => {
      requeuedTasks++;
    });

    // Add multiple tasks that will be running when worker terminates
    for (let i = 0; i < 3; i++) {
      const task = new FyflowTask({
        id: `multi-requeue-${i}`,
        workerType: 'SelfTerminatingWorker',
        payload: { action: 'self-terminate-after-tasks', taskCount: 3 }
      });
      scheduler.addTask(task);
    }

    await new Promise(resolve => setTimeout(resolve, 300));

    if (requeuedTasks < 1) throw new Error('Expected at least 1 task to be requeued');
  }

  // Test resource release on worker failure
  private async testResourceReleaseOnFailure(): Promise<void> {
    const group = new ConcurrentLimitGroup(2);

    const workerManager = new WorkerManager(this.selfTerminatingWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: false,
      groups: ['testGroup']
    });

    const scheduler = this.createScheduler({ SelfTerminatingWorker: workerManager }, { testGroup: group });

    // Start a task that will cause worker to terminate
    const task = new FyflowTask({
      id: 'resource-release-test',
      workerType: 'SelfTerminatingWorker',
      payload: { action: 'self-terminate', canRestart: false }
    });

    try {
      scheduler.addTask(task);
      await new Promise(resolve => setTimeout(resolve, 200));
    } catch (error: any) {
      // Expected
    }

    // Verify group resources were released (group should accept new tasks)
    if (!group.canRun()) throw new Error('Group resources not released after worker failure');
  }

  // Test maxThreads slot management on failure
  private async testMaxThreadsSlotManagement(): Promise<void> {
    const workerManager = new WorkerManager(this.selfTerminatingWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: false
    });

    const scheduler = this.createScheduler({ SelfTerminatingWorker: workerManager });

    const task = new FyflowTask({
      id: 'slot-management-test',
      workerType: 'SelfTerminatingWorker',
      payload: { action: 'self-terminate', canRestart: false }
    });

    try {
      await scheduler.addTask(task, { createPromise: true });
    } catch (error: any) {
      // Expected
    }

    await new Promise(resolve => setTimeout(resolve, 100));

    // Check that worker manager still tracks the failed worker in its pool
    const workerIds = workerManager.getWorkerIds();
    if (workerIds.length !== 1) throw new Error('Failed worker should still occupy maxThreads slot');

    const status = workerManager.getWorkerStatus(workerIds[0]);
    if (!status || status.state !== 'failed') throw new Error('Worker should be marked as failed but still tracked');
  }

  // Test worker.failed event emission
  private async testWorkerFailedEventEmission(): Promise<void> {
    const workerManager = new WorkerManager(this.selfTerminatingWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: false
    });

    const scheduler = this.createScheduler({ SelfTerminatingWorker: workerManager });

    let eventDetails: any = null;

    workerManager.addEventListener('worker.failed', (e: any) => {
      eventDetails = e.detail;
    });

    const task = new FyflowTask({
      id: 'event-emission-test',
      workerType: 'SelfTerminatingWorker',
      payload: { action: 'self-terminate', canRestart: false }
    });

    try {
      await scheduler.addTask(task, { createPromise: true });
    } catch (error: any) {
      // Expected
    }

    await new Promise(resolve => setTimeout(resolve, 100));

    if (!eventDetails) throw new Error('worker.failed event not emitted');
    if (!eventDetails.workerId) throw new Error('Event missing workerId');
    if (!eventDetails.error) throw new Error('Event missing error');
    if (!eventDetails.metadata) throw new Error('Event missing metadata');
    if (eventDetails.metadata.failureType !== 'self_termination') throw new Error(`Event has wrong failureType: ${eventDetails.metadata.failureType}`);
    if (!eventDetails.timestamp) throw new Error('Event missing timestamp');
  }

  // Test worker.self_terminated event emission
  private async testWorkerSelfTerminatedEventEmission(): Promise<void> {
    const workerManager = new WorkerManager(this.selfTerminatingWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: false
    });

    const scheduler = this.createScheduler({ SelfTerminatingWorker: workerManager });

    let eventDetails: any = null;

    workerManager.addEventListener('worker.self_terminated', (e: any) => {
      eventDetails = e.detail;
    });

    const task = new FyflowTask({
      id: 'self-terminated-event-test',
      workerType: 'SelfTerminatingWorker',
      payload: { action: 'self-terminate', canRestart: false }
    });

    try {
      await scheduler.addTask(task, { createPromise: true });
    } catch (error: any) {
      // Expected
    }

    await new Promise(resolve => setTimeout(resolve, 100));

    if (!eventDetails) throw new Error('worker.self_terminated event not emitted');
    if (!eventDetails.workerId) throw new Error('Event missing workerId');
    if (!eventDetails.metadata) throw new Error('Event missing metadata');
    if (!eventDetails.timestamp) throw new Error('Event missing timestamp');
  }

  // Test worker status inspection during failure
  private async testWorkerStatusInspection(): Promise<void> {
    const workerManager = new WorkerManager(this.selfTerminatingWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: false
    });

    const scheduler = this.createScheduler({ SelfTerminatingWorker: workerManager });

    const task = new FyflowTask({
      id: 'status-inspection-test',
      workerType: 'SelfTerminatingWorker',
      payload: { action: 'self-terminate', canRestart: false }
    });

    try {
      await scheduler.addTask(task, { createPromise: true });
    } catch (error: any) {
      // Expected
    }

    await new Promise(resolve => setTimeout(resolve, 100));

    const workerIds = workerManager.getWorkerIds();
    if (workerIds.length === 0) throw new Error('No workers found');

    const status = workerManager.getWorkerStatus(workerIds[0]);
    if (!status) throw new Error('Worker status not available');
    if (status.state !== 'failed') throw new Error(`Expected failed state, got ${status.state}`);
    if (!status.lastError) throw new Error('lastError not recorded');
    if (status.errorCount !== 1) throw new Error(`Expected errorCount 1, got ${status.errorCount}`);

    const allStatuses = workerManager.getAllWorkerStatuses();
    if (allStatuses.size !== 1) throw new Error('getAllWorkerStatuses should return 1 worker');
  }

  // Test worker restart after failure
  private async testWorkerRestart(): Promise<void> {
    const workerManager = new WorkerManager(this.selfTerminatingWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: false
    });

    const scheduler = this.createScheduler({ SelfTerminatingWorker: workerManager });

    const task1 = new FyflowTask({
      id: 'restart-test-1',
      workerType: 'SelfTerminatingWorker',
      payload: { action: 'self-terminate', canRestart: false }
    });

    try {
      await scheduler.addTask(task1, { createPromise: true });
    } catch (error: any) {
      // Expected
    }

    await new Promise(resolve => setTimeout(resolve, 100));

    const originalWorkerId = workerManager.getWorkerIds()[0];

    // Restart the worker
    const restarted = await workerManager.restartWorker(originalWorkerId);
    if (!restarted) throw new Error('Worker restart failed');

    await new Promise(resolve => setTimeout(resolve, 100));

    // Verify new worker is created
    const newWorkerIds = workerManager.getWorkerIds();
    if (newWorkerIds.length !== 1) throw new Error('Should have exactly 1 worker after restart');
    if (newWorkerIds[0] === originalWorkerId) throw new Error('Worker ID should be different after restart');

    // Test that new worker can execute tasks
    const task2 = new FyflowTask({
      id: 'restart-test-2',
      workerType: 'SelfTerminatingWorker',
      payload: { action: 'normal-task' }
    });

    await scheduler.addTask(task2, { createPromise: true }); // Should succeed with new worker
  }
}

// Auto-run tests when executed directly - handle both Deno and Node.js
if ((typeof Deno !== 'undefined' && import.meta.main) ||
    (typeof process !== 'undefined' && process.argv[1] && import.meta.url &&
     import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/')))) {
  const testSuite = new ErrorHandlingTestSuite();
  try {
    await testSuite.runAllTests(true); // Enable exit on complete when run directly
  } finally {
    // Cleanup all schedulers to remove event listeners and terminate workers
    await (testSuite as any).cleanup();
  }
}

export { ErrorHandlingTestSuite };
export default ErrorHandlingTestSuite;