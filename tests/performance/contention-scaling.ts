// Quick test to demonstrate contention scaling issue
import { DagScheduler, DagTask } from "../../core/dagScheduler.ts";
import { WorkerManager } from "../../core/workerManager.ts";
import { ConcurrentLimitGroup } from "../../groups/concurrentLimitGroup.ts";

const noopWorkerUrl = new URL("../../benchmark/noopWorker.ts", import.meta.url).href;

async function testContentionScaling(taskCount: number, slots: number) {
  console.log(`🧪 Testing ${taskCount} tasks with ${slots} slots...`);

  const testGroup = new ConcurrentLimitGroup(slots);
  const workerManager = new WorkerManager(noopWorkerUrl, {
    maxThreads: 2,
    maxConcurrentTasks: 1,
    inline: false,
    taskDelay: 1
  });

  const scheduler = new DagScheduler({ NoopWorker: workerManager }, { testGroup });

  const tasks = [];
  for (let i = 0; i < taskCount; i++) {
    tasks.push(new DagTask({
      id: `task-${i}`,
      workerType: 'NoopWorker',
      payload: { index: i },
      workerGroups: ['testGroup']
    }));
  }

  const start = performance.now();
  try {
    const promises = scheduler.addTasks(tasks);
    await Promise.all(promises);
    const duration = performance.now() - start;
    const throughput = taskCount / (duration / 1000);
    console.log(`✅ ${taskCount} tasks completed in ${duration.toFixed(1)}ms (${throughput.toFixed(1)} tasks/sec)`);
  } catch (error) {
    const duration = performance.now() - start;
    console.log(`❌ ${taskCount} tasks timed out after ${duration.toFixed(1)}ms`);
  }

  // Skip cleanup for quick test
}

console.log("🚀 Contention Scaling Test");
console.log("=========================");

// Test scaling from reasonable to extreme contention
await testContentionScaling(100, 2);   // 50:1 ratio
await testContentionScaling(500, 2);   // 250:1 ratio
await testContentionScaling(1000, 2);  // 500:1 ratio
await testContentionScaling(2000, 2);  // 1000:1 ratio (should start showing issues)
await testContentionScaling(5000, 2);  // 2500:1 ratio (likely timeout)
await testContentionScaling(10000, 2); // 5000:1 ratio
await testContentionScaling(25000, 2); // 12500:1 ratio (extreme contention)

console.log("🏁 Test complete");