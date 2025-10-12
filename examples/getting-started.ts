import { WorkerManager, FyflowScheduler, FyflowTask, ConcurrentLimitGroup } from "../index.ts";

let simpleWorkerUrl: string;
if (typeof Deno !== "undefined") {
    simpleWorkerUrl = new URL("./workers/simpleWorker.ts", import.meta.url).href;
  } else {
    // Node/Browser esbuild replaces this
    // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
    simpleWorkerUrl = new URL((await import("./workers/simpleWorker.ts?worker-direct")).default);

  }
  console.log('simpleWorkerUrl', simpleWorkerUrl);
/**
 * Getting Started Example
 *
 * Demonstrates:
 * - Basic task creation and execution
 * - Simple worker setup
 * - Event listening
 * - Basic resource management
 */

console.log('🚀 FyFlow Getting Started Example');
console.log('=====================================\n');

// CPU constraints now handled by groups at scheduler level
console.log('📦 Using group-based CPU constraints');

// Step 2: Create worker pools
const workerPools = {
    SimpleWorker: new WorkerManager(
        simpleWorkerUrl,
        {
            maxThreads: 2,
            maxConcurrentTasks: 1,
            config: { groups: ['cpu'] } // Custom config passed to worker
        }
    )
};
console.log('🏭 Created SimpleWorker pool (2 threads, 1 task each)');
console.log('💻 Using CPU group with capacity: ', navigator.hardwareConcurrency);
// Step 3: Create scheduler with CPU group for threaded workers
const scheduler = new FyflowScheduler(
    workerPools,
    { cpu: new ConcurrentLimitGroup(navigator.hardwareConcurrency) } // Match worker capacity (2 threads × 1 task)
);
console.log('📋 Created DAG scheduler');

// Step 4: Set up event listeners
scheduler.addEventListener('task.running', (e: any) => {
    console.log(`🚀 RUNNING: ${e.detail.id}`);
});

scheduler.addEventListener('task.completed', (e: any) => {
    const task = e.detail;
    console.log(`✅ COMPLETED: ${task.id} - Result: ${task.result?.result}`);
});


// Step 5: Create independent tasks
const tasks = [
    new FyflowTask({
        id: 'prepare-data',
        workerType: 'SimpleWorker',
        payload: { task: 'data-preparation', data: 'raw-dataset.csv' }
    }),
    new FyflowTask({
        id: 'validate-data',
        workerType: 'SimpleWorker',
        payload: { task: 'data-validation', data: 'prepared-dataset.csv' }
    }),
    new FyflowTask({
        id: 'analyze-data',
        workerType: 'SimpleWorker',
        payload: { task: 'data-analysis', data: 'validated-dataset.csv' }
    }),
    new FyflowTask({
        id: 'generate-report',
        workerType: 'SimpleWorker',
        payload: { task: 'report-generation', data: 'analysis-results.json' }
    })
];

console.log('\n📋 Created 4 parallel tasks\n');

// Step 6: Add tasks to scheduler
tasks.forEach(task => {
    scheduler.addTask(task);
});

// Step 7: Monitor progress
const progressInterval = setInterval(() => {
    const stats = scheduler.stats;
    if (stats.queued > 0 || stats.running > 0) {
        console.log(`📊 Progress: Queued: ${stats.queued}, Running: ${stats.running}, Done: ${stats.done}`);
    }
}, 1000);

scheduler.addEventListener('scheduler.completed', (e: any) => {
    clearInterval(progressInterval);
    const stats = e.detail;
    console.log('\n🎯 All Tasks Completed!');
    console.log(`✅ Completed: ${stats.done} tasks`);
    console.log(`❌ Failed: ${stats.failed} tasks`);
});
