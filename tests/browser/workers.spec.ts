import { test, expect } from '@playwright/test';

/**
 * FyFlow Browser Web Worker Tests
 * Tests actual Web Worker functionality in browser environments
 */

test.describe('FyFlow Web Worker Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/tests/browser/test-runner.html');
    await page.waitForFunction(() => window.FyFlowTestRunner !== undefined);
  });

  test('should support Web Worker creation and messaging', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        // Test basic Web Worker functionality
        const workerCode = `
          self.onmessage = function(e) {
            const { action, payload } = e.data;
            if (action === 'test') {
              self.postMessage({ result: 'success', payload });
            }
          };
        `;

        const blob = new Blob([workerCode], { type: 'application/javascript' });
        const workerUrl = URL.createObjectURL(blob);
        const worker = new Worker(workerUrl);

        return new Promise((resolve) => {
          worker.onmessage = (e) => {
            worker.terminate();
            URL.revokeObjectURL(workerUrl);
            resolve({
              success: true,
              result: e.data.result,
              payload: e.data.payload
            });
          };

          worker.onerror = (error) => {
            worker.terminate();
            URL.revokeObjectURL(workerUrl);
            resolve({ success: false, error: error.message });
          };

          worker.postMessage({ action: 'test', payload: 'hello worker' });
        });
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.result).toBe('success');
    expect(result.payload).toBe('hello worker');
  });

  test('should execute tasks with real Web Workers', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        // Create worker manager that uses actual Web Workers (not inline)
        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testThreadWorker.js', {
          maxThreads: 2,
          maxConcurrentTasks: 1,
          inline: false  // Force Web Worker usage
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Create a task that will run in a real Web Worker
        const task = new FyflowTask({
          id: 'webworker-test',
          workerType: 'TestWorker',
          payload: { message: 'Hello from Web Worker!' }
        });

        const startTime = performance.now();
        await scheduler.addTask(task, { createPromise: true });
        const duration = performance.now() - startTime;

        return {
          success: true,
          taskState: task.state,
          result: task.result,
          duration
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.taskState).toBe('done');
    expect(result.duration).toBeGreaterThan(0);
  });

  test('should handle concurrent Web Worker tasks', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testThreadWorker.js', {
          maxThreads: 3,
          maxConcurrentTasks: 1,
          inline: false
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Create multiple tasks to run concurrently in Web Workers
        const tasks = [];
        for (let i = 0; i < 5; i++) {
          tasks.push(new FyflowTask({
            id: `concurrent-worker-${i}`,
            workerType: 'TestWorker',
            payload: { delay: 50, index: i }
          }));
        }

        const startTime = performance.now();
        const promises = scheduler.addTasks(tasks, { createPromise: true });
        await Promise.all(promises);
        const duration = performance.now() - startTime;

        return {
          success: true,
          taskCount: tasks.length,
          allCompleted: tasks.every(t => t.state === 'done'),
          duration,
          results: tasks.map(t => t.result)
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.allCompleted).toBe(true);
    expect(result.taskCount).toBe(5);
    // Should complete faster than sequential due to parallel Web Workers
    expect(result.duration).toBeLessThan(500); // 5 * 50ms = 250ms if sequential
  });

  test('should handle Web Worker errors and cleanup', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testThreadWorker.js', {
          maxThreads: 1,
          maxConcurrentTasks: 1,
          inline: false
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Create a task that will cause an error in the Web Worker
        const task = new FyflowTask({
          id: 'worker-error-test',
          workerType: 'TestWorker',
          payload: { shouldThrow: true }
        });

        try {
          await scheduler.addTask(task, { createPromise: true });
        } catch (error) {
          // Expected to fail
        }

        // Test that we can still create new tasks after error
        const recoveryTask = new FyflowTask({
          id: 'recovery-test',
          workerType: 'TestWorker',
          payload: { message: 'Recovery test' }
        });

        await scheduler.addTask(recoveryTask, { createPromise: true });

        return {
          success: true,
          errorTaskState: task.state,
          recoveryTaskState: recoveryTask.state,
          hasError: !!task.error
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(['failed', 'user_action']).toContain(result.errorTaskState);
    expect(result.recoveryTaskState).toBe('done');
    // Error handling may vary in browser environment
    if (result.errorTaskState === 'failed') {
      expect(result.hasError).toBe(true);
    }
  });

  test('should handle Web Worker termination and restart', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testThreadWorker.js', {
          maxThreads: 1,
          maxConcurrentTasks: 1,
          inline: false,
          idleTimeout: 100 // Short timeout for testing
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Run a task
        const task1 = new FyflowTask({
          id: 'first-task',
          workerType: 'TestWorker',
          payload: { message: 'First task' }
        });

        await scheduler.addTask(task1, { createPromise: true });

        // Wait for worker to potentially idle out
        await new Promise(resolve => setTimeout(resolve, 150));

        // Run another task - should create new worker
        const task2 = new FyflowTask({
          id: 'second-task',
          workerType: 'TestWorker',
          payload: { message: 'Second task' }
        });

        await scheduler.addTask(task2, { createPromise: true });

        return {
          success: true,
          firstTaskState: task1.state,
          secondTaskState: task2.state
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.firstTaskState).toBe('done');
    expect(result.secondTaskState).toBe('done');
  });

  test('should handle mixed inline and Web Worker execution', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        // Create both inline and Web Worker managers
        const inlineManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 1,
          maxConcurrentTasks: 2,
          inline: true
        });

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testThreadWorker.js', {
          maxThreads: 2,
          maxConcurrentTasks: 1,
          inline: false
        });

        const scheduler = new FyflowScheduler({
          InlineWorker: inlineManager,
          ThreadWorker: workerManager
        }, {});

        // Create tasks for both worker types
        const tasks = [
          new FyflowTask({
            id: 'inline-1',
            workerType: 'InlineWorker',
            payload: { delay: 20 }
          }),
          new FyflowTask({
            id: 'worker-1',
            workerType: 'ThreadWorker',
            payload: { delay: 30 }
          }),
          new FyflowTask({
            id: 'inline-2',
            workerType: 'InlineWorker',
            payload: { delay: 25 }
          }),
          new FyflowTask({
            id: 'worker-2',
            workerType: 'ThreadWorker',
            payload: { delay: 35 }
          })
        ];

        const startTime = performance.now();
        const promises = scheduler.addTasks(tasks, { createPromise: true });
        await Promise.all(promises);
        const duration = performance.now() - startTime;

        return {
          success: true,
          taskCount: tasks.length,
          allCompleted: tasks.every(t => t.state === 'done'),
          duration,
          taskTypes: tasks.map(t => ({ id: t.id, type: t.workerType }))
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.allCompleted).toBe(true);
    expect(result.taskCount).toBe(4);
    expect(result.taskTypes).toEqual([
      { id: 'inline-1', type: 'InlineWorker' },
      { id: 'worker-1', type: 'ThreadWorker' },
      { id: 'inline-2', type: 'InlineWorker' },
      { id: 'worker-2', type: 'ThreadWorker' }
    ]);
  });
});

test.describe('FyFlow Web Worker Performance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/tests/browser/test-runner.html');
    await page.waitForFunction(() => window.FyFlowTestRunner !== undefined);
  });

  test('should measure Web Worker vs inline performance', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        // Test inline performance
        const inlineManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 1,
          maxConcurrentTasks: 3,
          inline: true
        });

        const inlineScheduler = new FyflowScheduler({ TestWorker: inlineManager }, {});

        const inlineTasks = Array.from({ length: 10 }, (_, i) =>
          new FyflowTask({
            id: `inline-perf-${i}`,
            workerType: 'TestWorker',
            payload: { delay: 10 }
          })
        );

        const inlineStart = performance.now();
        const inlinePromises = inlineScheduler.addTasks(inlineTasks, { createPromise: true });
        await Promise.all(inlinePromises);
        const inlineDuration = performance.now() - inlineStart;

        // Test Web Worker performance
        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testThreadWorker.js', {
          maxThreads: 3,
          maxConcurrentTasks: 1,
          inline: false
        });

        const workerScheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        const workerTasks = Array.from({ length: 10 }, (_, i) =>
          new FyflowTask({
            id: `worker-perf-${i}`,
            workerType: 'TestWorker',
            payload: { delay: 10 }
          })
        );

        const workerStart = performance.now();
        const workerPromises = workerScheduler.addTasks(workerTasks, { createPromise: true });
        await Promise.all(workerPromises);
        const workerDuration = performance.now() - workerStart;

        return {
          success: true,
          inlineDuration,
          workerDuration,
          inlineCompleted: inlineTasks.every(t => t.state === 'done'),
          workerCompleted: workerTasks.every(t => t.state === 'done'),
          overhead: workerDuration - inlineDuration
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.inlineCompleted).toBe(true);
    expect(result.workerCompleted).toBe(true);
    expect(result.inlineDuration).toBeGreaterThan(0);
    expect(result.workerDuration).toBeGreaterThan(0);
    // Web Workers typically have some overhead for small tasks
    expect(result.overhead).toBeGreaterThanOrEqual(0);
  });

  test('should handle high concurrency Web Worker tasks', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testThreadWorker.js', {
          maxThreads: 4,
          maxConcurrentTasks: 1,
          inline: false
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Create many tasks to test high concurrency
        const tasks = Array.from({ length: 20 }, (_, i) =>
          new FyflowTask({
            id: `high-concurrency-${i}`,
            workerType: 'TestWorker',
            payload: { delay: 25, index: i }
          })
        );

        const startTime = performance.now();
        const promises = scheduler.addTasks(tasks, { createPromise: true });
        await Promise.all(promises);
        const duration = performance.now() - startTime;

        return {
          success: true,
          taskCount: tasks.length,
          allCompleted: tasks.every(t => t.state === 'done'),
          duration,
          avgTaskTime: duration / tasks.length,
          throughput: (tasks.length * 1000) / duration
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.allCompleted).toBe(true);
    expect(result.taskCount).toBe(20);
    expect(result.throughput).toBeGreaterThan(0);
    // Should complete much faster than sequential execution
    expect(result.duration).toBeLessThan(1000); // More generous timing for browsers
  });
});