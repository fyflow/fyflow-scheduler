import { WorkerManager, FyflowScheduler, FyflowTask } from "../index.ts";

/**
 * Worker Types Example
 *
 * Demonstrates:
 * - Inline vs threaded worker comparison
 * - Concurrent execution patterns
 * - Performance characteristics
 * - Resource usage differences
 * - Optimal use cases for each worker type
 */

console.log('🚀 FyFlow Worker Types Comparison');
console.log('=================================\n');

// Step 1: Create resource manager

console.log('📦 Created CPU manager with 8 slots');

// Step 2: Create different worker pool configurations
const workerPools = {
    // Threaded workers: 1 CPU slot per thread, 1 task per thread
    CpuThreaded: new WorkerManager(
        new URL("./workers/cpuWorker.ts", import.meta.url).href,
        {
            maxThreads: 4,           // 4 threads
            maxConcurrentTasks: 1,   // 1 task per thread
            inline: false,           // Use worker threads
            config: { iterations: 100000 }
        }
    ),

    // Inline workers: 0 CPU slots, high concurrency for async work
    AsyncInline: new WorkerManager(
        new URL("./workers/asyncWorker.ts", import.meta.url).href,
        {
            maxThreads: 2,           // 2 worker instances
            maxConcurrentTasks: 10,  // 10 concurrent tasks per instance
            inline: true,            // Run in main thread
            config: { connectionPoolSize: 5 }
        }
    ),

    // Mixed: Simple workers can work both ways
    SimpleThreaded: new WorkerManager(
        new URL("./workers/simpleWorker.ts", import.meta.url).href,
        {
            maxThreads: 2,
            maxConcurrentTasks: 1,
            inline: false,
            config: { delay: 200 }
        }
    ),

    SimpleInline: new WorkerManager(
        new URL("./workers/simpleWorker.ts", import.meta.url).href,
        {
            maxThreads: 1,
            maxConcurrentTasks: 8,
            inline: true,
            config: { delay: 200 }
        }
    )
};

const scheduler = new FyflowScheduler(workerPools);

console.log('🏭 Created worker pools:');
console.log('  • CpuThreaded: 4 threads × 1 task = 4 CPU tasks (uses 4 CPU slots)');
console.log('  • AsyncInline: 2 instances × 10 tasks = 20 async tasks (uses 0 CPU slots)');
console.log('  • SimpleThreaded: 2 threads × 1 task = 2 simple tasks (uses 2 CPU slots)');
console.log('  • SimpleInline: 1 instance × 8 tasks = 8 simple tasks (uses 0 CPU slots)');
console.log('  📊 Total theoretical capacity: 34 concurrent tasks using 6/8 CPU slots\n');

// Step 3: Create diverse workloads
const cpuTasks = Array.from({ length: 6 }, (_, i) =>
    new FyflowTask({
        id: `cpu-intensive-${i}`,
        workerType: 'CpuThreaded',
        payload: {
            algorithm: i % 3 === 0 ? 'prime' : i % 3 === 1 ? 'fibonacci' : 'sort',
            data: `cpu-workload-${i}`
        }
    })
);

const asyncTasks = Array.from({ length: 15 }, (_, i) =>
    new FyflowTask({
        id: `async-io-${i}`,
        workerType: 'AsyncInline',
        payload: {
            data: `async-workload-${i}`,
            delay: 300 + Math.random() * 400 // Variable I/O latency
        }
    })
);

const simpleThreadedTasks = Array.from({ length: 4 }, (_, i) =>
    new FyflowTask({
        id: `simple-threaded-${i}`,
        workerType: 'SimpleThreaded',
        payload: {
            task: 'threaded-processing',
            data: `threaded-data-${i}`
        }
    })
);

const simpleInlineTasks = Array.from({ length: 10 }, (_, i) =>
    new FyflowTask({
        id: `simple-inline-${i}`,
        workerType: 'SimpleInline',
        payload: {
            task: 'inline-processing',
            data: `inline-data-${i}`
        }
    })
);

// Step 4: Set up monitoring
const workerTypeStats = {
    CpuThreaded: { completed: 0, totalTime: 0 },
    AsyncInline: { completed: 0, totalTime: 0 },
    SimpleThreaded: { completed: 0, totalTime: 0 },
    SimpleInline: { completed: 0, totalTime: 0 }
} as { [key: string]: { completed: number, totalTime: number } };

const startTime = performance.now();

scheduler.addEventListener('task.running', (e: any) => {
    console.log(`🚀 RUNNING: ${e.detail.id} (${e.detail.workerType})`);
});

scheduler.addEventListener('task.completed', (e: any) => {
    const task = e.detail;
    const duration = performance.now() - startTime;

    console.log(`✅ COMPLETED: ${task.id} (${task.workerType}) - ${Math.round(duration)}ms`);

    // Track stats by worker type
    if (workerTypeStats[task.workerType]) {
        workerTypeStats[task.workerType].completed++;
        workerTypeStats[task.workerType].totalTime = duration;
    }
});

scheduler.addEventListener('scheduler.completed', (e: any) => {
    const totalDuration = performance.now() - startTime;
    const stats = e.detail;

    console.log('\n🎯 All Tasks Completed!');
    console.log(`⏱️  Total time: ${Math.round(totalDuration)}ms`);
    console.log(`✅ Completed: ${stats.done} tasks`);
    console.log(`❌ Failed: ${stats.failed} tasks`);

    console.log('\n📊 Performance by Worker Type:');
    for (const [workerType, typeStats] of Object.entries(workerTypeStats)) {
        if (typeStats.completed > 0) {
            const avgTime = typeStats.totalTime / typeStats.completed;
            console.log(`  ${workerType}: ${typeStats.completed} tasks, avg ${Math.round(avgTime)}ms`);
        }
    }

    console.log('\n💡 Key Takeaways:');
    console.log('  • Threaded workers: Best for CPU-intensive, blocking operations');
    console.log('  • Inline workers: Best for I/O-bound, async operations');
    console.log('  • Resource usage: Threads consume CPU slots, inline workers don\'t');
    console.log('  • Concurrency: Inline workers excel at high-concurrency async work');
});

// Step 5: Add all tasks
console.log('📋 Adding tasks to scheduler...');
[...cpuTasks, ...asyncTasks, ...simpleThreadedTasks, ...simpleInlineTasks].forEach(task => {
    scheduler.addTask(task);
});

// Step 6: Progress monitoring
const progressInterval = setInterval(() => {
    const stats = scheduler.stats;
    if (stats.queued > 0 || stats.running > 0) {
        console.log(`📈 Progress: Queued: ${stats.queued}, Running: ${stats.running}, Done: ${stats.done}`);

        // Show completion by worker type
        const completionSummary = Object.entries(workerTypeStats)
            .map(([type, stats]) => `${type}: ${stats.completed}`)
            .join(', ');
        console.log(`📊 By type: ${completionSummary}`);
    } else if (stats.done > 0) {
        clearInterval(progressInterval);
    }
}, 2000);