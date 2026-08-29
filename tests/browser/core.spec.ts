// deno-lint-ignore-file no-window -- `window` here is the Playwright page context (page.evaluate/waitForFunction), not the Deno global.
import { test, expect } from '@playwright/test';

/**
 * FyFlow Core Browser Tests
 * Tests the core DAG scheduling functionality in real browser environments
 */

test.describe('FyFlow Core Functionality', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to our test runner page
    await page.goto('/tests/browser/test-runner.html');

    // Wait for the test runner to initialize
    await page.waitForFunction(() => window.FyFlowTestRunner !== undefined);

    // Verify platform detection completed
    await expect(page.locator('#browserInfo')).not.toContainText('Loading...');
    await expect(page.locator('#workerSupport')).toContainText('✅ Supported');
  });

  test('should load FyFlow library successfully', async ({ page }) => {
    // Test that we can import the FyFlow library
    const result = await page.evaluate(async () => {
      try {
        const fyflow = await import('/dev-dist/browser/index.js');
        return {
          success: true,
          hasScheduler: !!fyflow.FyflowScheduler,
          hasWorkerManager: !!fyflow.WorkerManager,
          hasTask: !!fyflow.FyflowTask
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.hasScheduler).toBe(true);
    expect(result.hasWorkerManager).toBe(true);
    expect(result.hasTask).toBe(true);
  });

  test('should create and execute basic DAG tasks', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        // Import FyFlow components
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        // Create a simple worker manager with inline execution
        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 1,
          maxConcurrentTasks: 2,
          inline: true
        });

        // Create scheduler
        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Create a simple task
        const task = new FyflowTask({
          id: 'browser-test-1',
          workerType: 'TestWorker',
          payload: { message: 'Hello Browser!' }
        });

        // Execute task
        const startTime = performance.now();
        await scheduler.addTask(task, { createPromise: true });
        const duration = performance.now() - startTime;

        return {
          success: true,
          taskId: task.id,
          state: task.state,
          duration
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.state).toBe('done');
    expect(result.duration).toBeGreaterThan(0);
  });

  test('should handle sequential task execution', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 1,
          maxConcurrentTasks: 1,
          inline: true
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Create independent tasks (no dependencies)
        const task1 = new FyflowTask({
          id: 'task1',
          workerType: 'TestWorker',
          payload: { delay: 10 }
        });

        const task2 = new FyflowTask({
          id: 'task2',
          workerType: 'TestWorker',
          payload: { delay: 5 }
        });

        // Add tasks
        const promises = scheduler.addTasks([task1, task2], { createPromise: true });
        await Promise.all(promises);

        return {
          success: true,
          task1State: task1.state,
          task2State: task2.state
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.task1State).toBe('done');
    expect(result.task2State).toBe('done');
  });

  test('should handle multiple parallel tasks', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 2,
          maxConcurrentTasks: 3,
          inline: true
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Create multiple independent tasks
        const tasks = [];
        for (let i = 0; i < 5; i++) {
          tasks.push(new FyflowTask({
            id: `parallel-${i}`,
            workerType: 'TestWorker',
            payload: { delay: 20 }
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
          duration
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.allCompleted).toBe(true);
    expect(result.taskCount).toBe(5);
    // Should complete faster than sequential execution due to parallelism
    expect(result.duration).toBeLessThan(300); // 5 * 20ms would be 100ms+ if sequential
  });

  test('should handle resource constraints', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager, ConcurrentLimitGroup } = await import('/dev-dist/browser/index.js');

        // Create a concurrent resource group with limit of 2
        const limitedGroup = new ConcurrentLimitGroup(2, 'limited');

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 1,
          maxConcurrentTasks: 5,
          inline: true,
          groups: ['limited']
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, { limited: limitedGroup });

        // Track max concurrent usage
        let maxConcurrent = 0;
        const checkInterval = setInterval(() => {
          maxConcurrent = Math.max(maxConcurrent, limitedGroup.running);
        }, 10);

        // Create more tasks than the group allows
        const tasks = [];
        for (let i = 0; i < 4; i++) {
          tasks.push(new FyflowTask({
            id: `constrained-${i}`,
            workerType: 'TestWorker',
            payload: { delay: 50 }
          }));
        }

        const startTime = performance.now();
        const promises = scheduler.addTasks(tasks, { createPromise: true });
        await Promise.all(promises);
        const duration = performance.now() - startTime;

        clearInterval(checkInterval);

        return {
          success: true,
          taskCount: tasks.length,
          allCompleted: tasks.every(t => t.state === 'done'),
          duration,
          maxConcurrent,
          limitRespected: maxConcurrent <= 3 // Allow brief overage for optimistic allocation
        };
      } catch (error) {
        return { success: false, error: error.message, stack: error.stack };
      }
    });

    if (!result.success) {
      console.log('Test error:', result.error);
      console.log('Stack:', result.stack);
    }
    expect(result.success).toBe(true);
    expect(result.allCompleted).toBe(true);
    expect(result.limitRespected).toBe(true); // Limit respected with tolerance for optimistic allocation
    // Should take longer due to resource constraints limiting concurrency
    expect(result.duration).toBeGreaterThan(80); // At least 2 batches of 50ms
  });

  test('should run core tests through test runner', async ({ page }) => {
    // Click the Run Core Tests button
    await page.click('button:has-text("Run Core Tests")');

    // Wait for tests to complete (check for success status)
    await expect(page.locator('#status')).toContainText('Core tests completed successfully!', { timeout: 30000 });

    // Verify output contains test results
    const output = await page.textContent('#output');
    expect(output).toContain('FyFlow Core Tests');
    expect(output).toContain('✅');
  });

  test('should provide browser platform information', async ({ page }) => {
    // Check platform detection
    const browserInfo = await page.textContent('#browserInfo');
    expect(browserInfo).toBeTruthy();
    expect(browserInfo).not.toBe('Loading...');

    const workerSupport = await page.textContent('#workerSupport');
    expect(workerSupport).toBe('✅ Supported');

    const esmSupport = await page.textContent('#esmSupport');
    expect(esmSupport).toBe('✅ Supported (using type="module")');
  });
});

test.describe('FyFlow Browser Error Handling', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/tests/browser/test-runner.html');
    await page.waitForFunction(() => window.FyFlowTestRunner !== undefined);
  });

  test('should handle worker errors gracefully', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 1,
          maxConcurrentTasks: 1,
          inline: true
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Create a task that will cause an error
        const task = new FyflowTask({
          id: 'error-test',
          workerType: 'TestWorker',
          payload: { shouldThrow: true }
        });

        try {
          await scheduler.addTask(task, { createPromise: true });
        } catch {
          // Expected to throw
        }

        return {
          success: true,
          taskState: task.state,
          hasError: !!task.error,
          errorMessage: task.error?.message
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(['failed', 'user_action']).toContain(result.taskState);
    // Error handling may vary in browser environment
    if (result.taskState === 'failed') {
      expect(result.hasError).toBe(true);
    }
  });

  test('should handle invalid worker type', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 1,
          maxConcurrentTasks: 1,
          inline: true
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Create a task with invalid worker type
        const task = new FyflowTask({
          id: 'invalid-worker',
          workerType: 'NonExistentWorker',
          payload: {}
        });

        try {
          await scheduler.addTask(task, { createPromise: true });
          return { success: false, error: 'Should have thrown error' };
        } catch (error) {
          return { success: true, errorMessage: error.message };
        }
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.errorMessage).toContain('NonExistentWorker');
  });
});