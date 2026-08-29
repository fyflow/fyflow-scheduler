// deno-lint-ignore-file no-window -- `window` here is the Playwright page context (page.evaluate/waitForFunction), not the Deno global.
import { test, expect } from '@playwright/test';

/**
 * FyFlow Browser Performance Tests
 * Tests performance characteristics specific to browser environments
 */

test.describe('FyFlow Browser Performance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/tests/browser/test-runner.html');
    await page.waitForFunction(() => window.FyFlowTestRunner !== undefined);
  });

  test('should measure task throughput in browser', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 1,
          maxConcurrentTasks: 5,
          inline: true
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Create many small tasks
        const taskCount = 100;
        const tasks = Array.from({ length: taskCount }, (_, i) =>
          new FyflowTask({
            id: `throughput-${i}`,
            workerType: 'TestWorker',
            payload: { delay: 1 } // Minimal delay
          })
        );

        const startTime = performance.now();
        const promises = scheduler.addTasks(tasks, { createPromise: true });
        await Promise.all(promises);
        const duration = performance.now() - startTime;

        const throughput = (taskCount * 1000) / duration; // tasks per second

        return {
          success: true,
          taskCount,
          duration,
          throughput,
          avgTaskTime: duration / taskCount
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.taskCount).toBe(100);
    expect(result.throughput).toBeGreaterThan(100); // At least 100 tasks/sec
    expect(result.avgTaskTime).toBeLessThan(100); // Less than 100ms per task on average
  });

  test('should measure memory usage patterns', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        // Measure baseline memory
        const getMemoryUsage = () => {
          if (performance.memory) {
            return {
              used: performance.memory.usedJSHeapSize,
              total: performance.memory.totalJSHeapSize,
              limit: performance.memory.jsHeapSizeLimit
            };
          }
          return null;
        };

        const baseline = getMemoryUsage();

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 2,
          maxConcurrentTasks: 3,
          inline: true
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Create and execute tasks in batches to measure memory growth
        const batchSize = 20;
        const batches = 5;
        const memoryMeasurements = [];

        for (let batch = 0; batch < batches; batch++) {
          const tasks = Array.from({ length: batchSize }, (_, i) =>
            new FyflowTask({
              id: `memory-batch-${batch}-task-${i}`,
              workerType: 'TestWorker',
              payload: { delay: 5 }
            })
          );

          const promises = scheduler.addTasks(tasks, { createPromise: true });
          await Promise.all(promises);

          const memory = getMemoryUsage();
          memoryMeasurements.push(memory);

          // Force garbage collection attempt
          if (window.gc) {
            window.gc();
          }
        }

        const final = getMemoryUsage();

        return {
          success: true,
          baseline,
          measurements: memoryMeasurements,
          final,
          totalTasks: batchSize * batches,
          memorySupported: !!baseline
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.totalTasks).toBe(100);

    if (result.memorySupported) {
      expect(result.baseline.used).toBeGreaterThan(0);
      expect(result.final.used).toBeGreaterThan(0);
      // Memory shouldn't grow excessively
      const memoryGrowth = result.final.used - result.baseline.used;
      expect(memoryGrowth).toBeLessThan(50 * 1024 * 1024); // Less than 50MB growth
    }
  });

  test('should measure scheduler overhead vs actual work', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        // Test with minimal work (scheduler overhead dominant)
        const fastManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 1,
          maxConcurrentTasks: 3,
          inline: true
        });

        const fastScheduler = new FyflowScheduler({ TestWorker: fastManager }, {});

        const fastTasks = Array.from({ length: 50 }, (_, i) =>
          new FyflowTask({
            id: `fast-${i}`,
            workerType: 'TestWorker',
            payload: { delay: 1 } // Very fast work
          })
        );

        const fastStart = performance.now();
        const fastPromises = fastScheduler.addTasks(fastTasks, { createPromise: true });
        await Promise.all(fastPromises);
        const fastDuration = performance.now() - fastStart;

        // Test with significant work (actual work dominant)
        const slowManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 1,
          maxConcurrentTasks: 3,
          inline: true
        });

        const slowScheduler = new FyflowScheduler({ TestWorker: slowManager }, {});

        const slowTasks = Array.from({ length: 50 }, (_, i) =>
          new FyflowTask({
            id: `slow-${i}`,
            workerType: 'TestWorker',
            payload: { delay: 20 } // Slower work
          })
        );

        const slowStart = performance.now();
        const slowPromises = slowScheduler.addTasks(slowTasks, { createPromise: true });
        await Promise.all(slowPromises);
        const slowDuration = performance.now() - slowStart;

        // Estimate overhead
        const workDifference = (20 - 1) * 50; // Additional work time
        const overhead = (slowDuration - fastDuration - workDifference) / 50; // Per task overhead

        return {
          success: true,
          fastDuration,
          slowDuration,
          overhead,
          efficiency: (workDifference / slowDuration) * 100 // Percentage of time doing actual work
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.fastDuration).toBeGreaterThan(0);
    expect(result.slowDuration).toBeGreaterThan(result.fastDuration);
    expect(result.efficiency).toBeGreaterThan(50); // At least 50% efficiency
    expect(result.overhead).toBeLessThan(25); // Less than 25ms overhead per task
  });

  test('should handle resource contention performance', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager, ConcurrentLimitGroup } = await import('/dev-dist/browser/index.js');

        // Test with no constraints (baseline)
        const unconstrainedManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 1,
          maxConcurrentTasks: 10,
          inline: true
        });

        const unconstrainedScheduler = new FyflowScheduler({ TestWorker: unconstrainedManager }, {});

        const unconstrainedTasks = Array.from({ length: 20 }, (_, i) =>
          new FyflowTask({
            id: `unconstrained-${i}`,
            workerType: 'TestWorker',
            payload: { delay: 10 }
          })
        );

        const unconstrainedStart = performance.now();
        const unconstrainedPromises = unconstrainedScheduler.addTasks(unconstrainedTasks, { createPromise: true });
        await Promise.all(unconstrainedPromises);
        const unconstrainedDuration = performance.now() - unconstrainedStart;

        // Test with concurrent constraints (optimistic limit enforcement)
        const limitGroup = new ConcurrentLimitGroup(3, 'limited');
        const constrainedManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 1,
          maxConcurrentTasks: 10,
          inline: true,
          groups: ['limited']
        });

        const constrainedScheduler = new FyflowScheduler({ TestWorker: constrainedManager }, { limited: limitGroup });

        const constrainedTasks = Array.from({ length: 20 }, (_, i) =>
          new FyflowTask({
            id: `constrained-${i}`,
            workerType: 'TestWorker',
            payload: { delay: 10 },
            workerGroups: ['limited']
          })
        );

        const constrainedStart = performance.now();
        const constrainedPromises = constrainedScheduler.addTasks(constrainedTasks, { createPromise: true });
        await Promise.all(constrainedPromises);
        const constrainedDuration = performance.now() - constrainedStart;

        const slowdown = (constrainedDuration / unconstrainedDuration) - 1;

        return {
          success: true,
          unconstrainedDuration,
          constrainedDuration,
          slowdown,
          contentionImpact: slowdown * 100 // Percentage slowdown
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.constrainedDuration).toBeGreaterThan(result.unconstrainedDuration);
    expect(result.slowdown).toBeGreaterThan(0);
    // Contention should cause measurable but reasonable slowdown
    expect(result.contentionImpact).toBeLessThan(200); // Less than 200% slowdown
  });

  test('should measure cross-browser compatibility performance', async ({ page, browserName }) => {
    const result = await page.evaluate(async (browserName) => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 2,
          maxConcurrentTasks: 3,
          inline: true
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Standard performance test
        const tasks = Array.from({ length: 30 }, (_, i) =>
          new FyflowTask({
            id: `browser-compat-${i}`,
            workerType: 'TestWorker',
            payload: { delay: 15 }
          })
        );

        const startTime = performance.now();
        const promises = scheduler.addTasks(tasks, { createPromise: true });
        await Promise.all(promises);
        const duration = performance.now() - startTime;

        const throughput = (tasks.length * 1000) / duration;

        return {
          success: true,
          browser: browserName,
          taskCount: tasks.length,
          duration,
          throughput,
          avgTaskTime: duration / tasks.length
        };
      } catch (error) {
        return { success: false, browser: browserName, error: error.message };
      }
    }, browserName);

    expect(result.success).toBe(true);
    expect(result.browser).toBe(browserName);
    expect(result.taskCount).toBe(30);
    expect(result.throughput).toBeGreaterThan(50); // At least 50 tasks/sec across all browsers

    // Browser-specific expectations
    if (browserName === 'chromium') {
      expect(result.throughput).toBeGreaterThan(100); // Chrome should be fastest
    }
  });
});

test.describe('FyFlow Browser Stress Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/tests/browser/test-runner.html');
    await page.waitForFunction(() => window.FyFlowTestRunner !== undefined);
  });

  test('should handle large task volumes', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 3,
          maxConcurrentTasks: 5,
          inline: true
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Large number of tasks
        const taskCount = 500;
        const tasks = Array.from({ length: taskCount }, (_, i) =>
          new FyflowTask({
            id: `stress-${i}`,
            workerType: 'TestWorker',
            payload: { delay: 2 }
          })
        );

        const startTime = performance.now();
        const promises = scheduler.addTasks(tasks, { createPromise: true });
        await Promise.all(promises);
        const duration = performance.now() - startTime;

        return {
          success: true,
          taskCount,
          duration,
          throughput: (taskCount * 1000) / duration,
          allCompleted: tasks.every(t => t.state === 'done')
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.allCompleted).toBe(true);
    expect(result.taskCount).toBe(500);
    expect(result.throughput).toBeGreaterThan(200); // At least 200 tasks/sec
    expect(result.duration).toBeLessThan(10000); // Complete within 10 seconds
  });

  test('should handle large parallel task volumes', async ({ page }) => {
    const result = await page.evaluate(async () => {
      try {
        const { FyflowScheduler, FyflowTask, WorkerManager } = await import('/dev-dist/browser/index.js');

        const workerManager = new WorkerManager('/dev-dist/browser/tests/workers/testInlineWorker.js', {
          maxThreads: 2,
          maxConcurrentTasks: 3,
          inline: true
        });

        const scheduler = new FyflowScheduler({ TestWorker: workerManager }, {});

        // Create many independent parallel tasks (no dependencies)
        const tasks = [];
        for (let i = 0; i < 30; i++) {
          tasks.push(new FyflowTask({
            id: `task-${i}`,
            workerType: 'TestWorker',
            payload: { delay: 5 }
          }));
        }

        const startTime = performance.now();
        const promises = scheduler.addTasks(tasks, { createPromise: true });
        await Promise.all(promises);
        const duration = performance.now() - startTime;

        return {
          success: true,
          taskCount: tasks.length,
          duration,
          allCompleted: tasks.every(t => t.state === 'done')
        };
      } catch (error) {
        return { success: false, error: error.message };
      }
    });

    expect(result.success).toBe(true);
    expect(result.allCompleted).toBe(true);
    expect(result.taskCount).toBe(30);
    expect(result.duration).toBeLessThan(5000); // Complete within 5 seconds
  });
});