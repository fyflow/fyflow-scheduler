import { BaseWorker } from "../../core/workerInterface.ts";
import type { WorkerConfig } from "../../core/workerInterface.ts";

/**
 * Test Inline Worker - Dedicated worker file for test suite inline execution
 */
export default class TestInlineWorker extends BaseWorker {
  private delay: number;

  constructor(config: WorkerConfig = {}) {
    super(config);
    this.delay = config.delay || 50;
  }

  async setup() {
    // console.log('TestInlineWorker setup completed');
  }

  async run(payload: any) {
    // Handle error testing
    if (payload.shouldThrow) {
      throw new Error('Test worker intentional error');
    }

    // Counts tasks actually inside run(), which is the ground truth for
    // concurrency - counting task.running/task.completed events overshoots.
    // Inline workers share the test's globalThis. Yields to microtasks only, so
    // a run of thousands of tasks is not paced by timer resolution.
    if (payload.countConcurrency) {
      const counter = ((globalThis as any).__fyflowConcurrency ??= { now: 0, peak: 0 });
      // Per key too, when the task names one or several: `byKey[key]` is { now, peak }
      const keys: string[] = payload.countKey === undefined ? []
        : Array.isArray(payload.countKey) ? payload.countKey : [payload.countKey];
      const byKey = keys.map(key => ((counter.byKey ??= {})[key] ??= { now: 0, peak: 0 }));
      counter.now++;
      counter.peak = Math.max(counter.peak, counter.now);
      for (const c of byKey) { c.now++; c.peak = Math.max(c.peak, c.now); }
      try {
        for (let i = 0; i < 3; i++) await Promise.resolve();
        return { taskId: payload.taskId };
      } finally {
        counter.now--;
        for (const c of byKey) c.now--;
      }
    }

    const delay = payload.delay || this.delay;
    await new Promise(resolve => setTimeout(resolve, delay));
    return {
      workerId: 'inline',
      taskId: payload.taskId,
      data: payload.data,
      message: payload.message,
      processedAt: new Date().toISOString(),
      delay
    };
  }

  async teardown() {
    // console.log('TestInlineWorker teardown completed');
  }
}