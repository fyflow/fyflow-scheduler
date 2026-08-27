// Contention scaling test - measures scheduler overhead when many tasks compete
// for a small number of resource-group slots.
//
// Tasks use the no-op worker with no artificial delay, so the measured duration is
// dominated by scheduling and group-contention handling rather than by actual work.
import { FyflowScheduler, FyflowTask } from "../../core/FyflowScheduler.ts";
import { WorkerManager } from "../../core/workerManager.ts";
import { ConcurrentLimitGroup } from "../../groups/concurrentLimitGroup.ts";

// Resolve worker URL cross-platform (same pattern as the benchmark suite)
let noopWorkerUrl: string;
if (typeof Deno !== "undefined") {
  noopWorkerUrl = new URL("../../benchmark/noopWorker.ts", import.meta.url).href;
} else {
  // Node/Browser esbuild replaces this
  // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
  noopWorkerUrl = new URL((await import("../../benchmark/noopWorker.ts?worker-direct")).default).href;
}

interface ScalingResult {
  taskCount: number;
  slots: number;
  passed: boolean;
  duration: number;
  throughput: number;
  failure?: string;
}

// Generous ceiling - contention handling is expected to be sub-second per scenario,
// so anything approaching this budget is a genuine scheduler problem.
function timeoutBudget(taskCount: number): number {
  return Math.max(10_000, taskCount * 2);
}

async function testContentionScaling(taskCount: number, slots: number): Promise<ScalingResult> {
  console.log(`🧪 Testing ${taskCount} tasks with ${slots} slots...`);

  const testGroup = new ConcurrentLimitGroup(slots);
  const workerManager = new WorkerManager(noopWorkerUrl, {
    maxThreads: 2,
    maxConcurrentTasks: 1,
    inline: false,
    config: { taskDelay: 0 }
  });

  const scheduler = new FyflowScheduler({ NoopWorker: workerManager }, { testGroup });

  const tasks = [];
  for (let i = 0; i < taskCount; i++) {
    tasks.push(new FyflowTask({
      id: `task-${i}`,
      workerType: 'NoopWorker',
      payload: { index: i },
      workerGroups: ['testGroup']
    }));
  }

  const budget = timeoutBudget(taskCount);
  let timer: ReturnType<typeof setTimeout> | undefined;
  const start = performance.now();

  try {
    // createPromise is opt-in - addTasks is fire-and-forget by default
    const promises = scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[];

    const timedOut = Symbol('timed-out');
    const timeout = new Promise<symbol>((resolve) => {
      timer = setTimeout(() => resolve(timedOut), budget);
    });

    const outcome = await Promise.race([Promise.all(promises), timeout]);
    const duration = performance.now() - start;

    if (outcome === timedOut) {
      console.log(`❌ ${taskCount} tasks timed out after ${duration.toFixed(1)}ms (budget ${budget}ms)`);
      return {
        taskCount, slots, passed: false, duration, throughput: 0,
        failure: `timed out after ${budget}ms`
      };
    }

    const throughput = taskCount / (duration / 1000);
    console.log(`✅ ${taskCount} tasks completed in ${duration.toFixed(1)}ms (${throughput.toFixed(1)} tasks/sec)`);
    return { taskCount, slots, passed: true, duration, throughput };

  } catch (error) {
    const duration = performance.now() - start;
    const message = error instanceof Error ? error.message : String(error);
    console.log(`❌ ${taskCount} tasks failed after ${duration.toFixed(1)}ms: ${message}`);
    return { taskCount, slots, passed: false, duration, throughput: 0, failure: message };

  } finally {
    if (timer !== undefined) clearTimeout(timer);
    await scheduler.shutdown();
  }
}

export async function runContentionScaling(): Promise<ScalingResult[]> {
  console.log("🚀 Contention Scaling Test");
  console.log("=========================");

  const results: ScalingResult[] = [];

  // Scale from reasonable to extreme contention (ratios 50:1 up to 12500:1)
  for (const taskCount of [100, 500, 1000, 2000, 5000, 10000, 25000]) {
    results.push(await testContentionScaling(taskCount, 2));
  }

  const failed = results.filter(r => !r.passed);

  console.log("\n📊 Contention Scaling Summary");
  console.log("=".repeat(50));
  console.log(`Total scenarios: ${results.length}`);
  console.log(`✅ Passed: ${results.length - failed.length}`);
  console.log(`❌ Failed: ${failed.length}`);

  if (failed.length > 0) {
    for (const f of failed) {
      console.log(`   - ${f.taskCount} tasks / ${f.slots} slots: ${f.failure}`);
    }
    throw new Error(`${failed.length}/${results.length} contention scaling scenarios failed`);
  }

  console.log("🏁 Test complete");
  return results;
}

await runContentionScaling();
