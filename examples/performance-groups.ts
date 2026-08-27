import { WorkerManager, FyflowScheduler, FyflowTask, ConcurrentLimitGroup, RateLimitGroup } from "../index.ts";

let cpuWorkerUrl: any;
let asyncWorkerUrl: any;
let simpleWorkerUrl: any;
if (typeof Deno !== "undefined") {
    cpuWorkerUrl = new URL("./workers/cpuWorker.ts", import.meta.url).href;
    asyncWorkerUrl = new URL("./workers/asyncWorker.ts", import.meta.url).href;
    simpleWorkerUrl = new URL("./workers/simpleWorker.ts", import.meta.url).href;
  } else {
    // Node/Browser esbuild replaces this
    // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
    cpuWorkerUrl = new URL((await import("./workers/cpuWorker.ts?worker-direct")).default);
    // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
    asyncWorkerUrl = new URL((await import("./workers/asyncWorker.ts?worker-direct")).default);
    // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
    simpleWorkerUrl = new URL((await import("./workers/simpleWorker.ts?worker-direct")).default);

  }

/**
 * Performance & Groups Example
 *
 * Demonstrates:
 * - Resource management and constraints
 * - Different group types (concurrent limits, rate limits)
 * - Performance impact of resource contention
 * - Group inheritance from WorkerManager (NEW API)
 * - Load balancing and fairness
 */

console.log('🚀 FyFlow Performance & Resource Groups');
console.log('======================================\n');

// Step 1: Create resource managers and groups


// Different types of resource groups
const gpuGroup = new ConcurrentLimitGroup(4);     // Max 4 concurrent GPU tasks
const databaseGroup = new ConcurrentLimitGroup(6); // Max 6 concurrent DB connections
const apiGroup = new RateLimitGroup([{limit: 10, windowMs: 1000}]);     // Max 10 requests per second

console.log('📦 Created resource managers:');
console.log('  • CPU: 12 slots');
console.log('  • GPU: 4 concurrent limit');
console.log('  • Database: 6 concurrent limit');
console.log('  • API: 10 requests/second rate limit');

// Step 2: Create worker pools with group constraints
const workerPools = {
    // GPU-bound workers (inherit GPU constraint)
    GpuWorker: new WorkerManager(
        cpuWorkerUrl,
        {
            maxThreads: 6,
            maxConcurrentTasks: 1,
            groups: ['gpu'], // GPU constraint inherited by all tasks
            config: { iterations: 200000 }
        }
    ),

    // Database workers (inherit DB constraint)
    DatabaseWorker: new WorkerManager(
        asyncWorkerUrl,
        {
            maxThreads: 3,
            maxConcurrentTasks: 4,
            inline: true,
            groups: ['database'], // Database constraint inherited
            config: { connectionPoolSize: 2 }
        }
    ),

    // API workers (inherit rate limit)
    ApiWorker: new WorkerManager(
        asyncWorkerUrl,
        {
            maxThreads: 2,
            maxConcurrentTasks: 8,
            inline: true,
            groups: ['api'], // API rate limit inherited
            config: { connectionPoolSize: 4 }
        }
    ),

    // Multi-constraint workers (multiple groups)
    HeavyWorker: new WorkerManager(
        cpuWorkerUrl,
        {
            maxThreads: 2,
            maxConcurrentTasks: 1,
            groups: ['gpu', 'database'], // Multiple constraints
            config: { iterations: 500000 }
        }
    ),

    // Unconstrained workers
    SimpleWorker: new WorkerManager(
        simpleWorkerUrl,
        {
            maxThreads: 4,
            maxConcurrentTasks: 3,
            inline: true,
            config: { delay: 100 }
        }
    )
};

const scheduler = new FyflowScheduler(workerPools, {
    gpu: gpuGroup,
    database: databaseGroup,
    api: apiGroup
});

console.log('\n🏭 Created worker pools with resource constraints:');
console.log('  • GpuWorker: 6 threads, GPU constraint');
console.log('  • DatabaseWorker: 3×4 inline, Database constraint');
console.log('  • ApiWorker: 2×8 inline, API rate limit');
console.log('  • HeavyWorker: 2 threads, GPU + Database constraints');
console.log('  • SimpleWorker: 4×3 inline, no constraints');

// Step 3: Create workloads that test different resource scenarios
const gpuTasks = Array.from({ length: 12 }, (_, i) =>
    new FyflowTask({
        id: `gpu-task-${i}`,
        workerType: 'GpuWorker',
        payload: {
            algorithm: 'prime',
            data: `gpu-computation-${i}`
        }
    })
);

const databaseTasks = Array.from({ length: 20 }, (_, i) =>
    new FyflowTask({
        id: `db-task-${i}`,
        workerType: 'DatabaseWorker',
        payload: {
            data: `database-query-${i}`,
            delay: 200 + Math.random() * 300
        }
    })
);

const apiTasks = Array.from({ length: 25 }, (_, i) =>
    new FyflowTask({
        id: `api-task-${i}`,
        workerType: 'ApiWorker',
        payload: {
            data: `api-request-${i}`,
            delay: 100 + Math.random() * 200
        }
    })
);

const heavyTasks = Array.from({ length: 8 }, (_, i) =>
    new FyflowTask({
        id: `heavy-task-${i}`,
        workerType: 'HeavyWorker',
        payload: {
            algorithm: 'fibonacci',
            data: `heavy-computation-${i}`
        }
    })
);

const simpleTasks = Array.from({ length: 15 }, (_, i) =>
    new FyflowTask({
        id: `simple-task-${i}`,
        workerType: 'SimpleWorker',
        payload: {
            task: 'simple-processing',
            data: `simple-data-${i}`
        }
    })
);

// Step 4: Set up detailed monitoring
const resourceUsage = {
    gpu: { active: 0, peak: 0, total: 0 },
    database: { active: 0, peak: 0, total: 0 },
    api: { active: 0, peak: 0, total: 0 }
};

const workerStats = {} as { [key: string]: { count: number, totalTime: number } };
const startTime = performance.now();

scheduler.addEventListener('task.running', (e: any) => {
    const task = e.detail;
    console.log(`🚀 RUNNING: ${task.id} (${task.workerType})`);

    // Track resource usage (simplified - in real implementation this would be from groups)
    if (task.workerType === 'GpuWorker' || task.workerType === 'HeavyWorker') {
        resourceUsage.gpu.active++;
        resourceUsage.gpu.peak = Math.max(resourceUsage.gpu.peak, resourceUsage.gpu.active);
        resourceUsage.gpu.total++;
    }
    if (task.workerType === 'DatabaseWorker' || task.workerType === 'HeavyWorker') {
        resourceUsage.database.active++;
        resourceUsage.database.peak = Math.max(resourceUsage.database.peak, resourceUsage.database.active);
        resourceUsage.database.total++;
    }
    if (task.workerType === 'ApiWorker') {
        resourceUsage.api.active++;
        resourceUsage.api.peak = Math.max(resourceUsage.api.peak, resourceUsage.api.active);
        resourceUsage.api.total++;
    }
});

scheduler.addEventListener('task.completed', (e: any) => {
    const task = e.detail;
    const duration = performance.now() - startTime;

    console.log(`✅ COMPLETED: ${task.id} (${task.workerType}) - ${Math.round(duration)}ms`);

    // Update resource usage
    if (task.workerType === 'GpuWorker' || task.workerType === 'HeavyWorker') {
        resourceUsage.gpu.active--;
    }
    if (task.workerType === 'DatabaseWorker' || task.workerType === 'HeavyWorker') {
        resourceUsage.database.active--;
    }
    if (task.workerType === 'ApiWorker') {
        resourceUsage.api.active--;
    }

    // Track worker performance
    if (!workerStats[task.workerType]) {
        workerStats[task.workerType] = { count: 0, totalTime: 0 };
    }
    workerStats[task.workerType].count++;
    workerStats[task.workerType].totalTime = duration;
});

// Step 6: Completion handling
//
// Registered before any task is added, rather than on a timer that could fire
// after the scheduler has already completed. Tasks arrive in waves and the
// scheduler drains between them, so completion only counts once every wave has
// been submitted.
let allWavesAdded = false;

scheduler.addEventListener('scheduler.completed', (e: any) => {
    if (!allWavesAdded) return; // A lull between waves, not the end of the run

    const stats = e.detail;
    console.log('\n🎯 All Tasks Completed!');
    console.log(`✅ Completed: ${stats.done} tasks`);
    console.log(`❌ Failed: ${stats.failed} tasks`);

    clearInterval(progressInterval);

    // Give workers time to clean up, then exit
    setTimeout(() => {
        if (typeof Deno !== 'undefined') {
            Deno.exit(0);
        } else {
            process.exit(0);
        }
    }, 2000);
});

// Step 5: Add tasks with staggered timing to show resource contention
console.log('\n📋 Adding tasks to demonstrate resource contention...\n');

// Add tasks in waves to show different contention patterns
[...simpleTasks].forEach(task => scheduler.addTask(task));

setTimeout(() => {
    [...gpuTasks].forEach(task => scheduler.addTask(task));
}, 1000);

setTimeout(() => {
    [...databaseTasks].forEach(task => scheduler.addTask(task));
}, 2000);

setTimeout(() => {
    [...apiTasks].forEach(task => scheduler.addTask(task));
}, 3000);

setTimeout(() => {
    [...heavyTasks].forEach(task => scheduler.addTask(task));
    allWavesAdded = true; // Last wave - completion from here on is final
}, 4000);


const progressInterval = setInterval(() => {
    // console.log('monitoring')
    const stats = scheduler.stats;
    // if (stats.queued > 0 || stats.running > 0) {
        console.log(`📈 Stats: Queued: ${stats.queued}, Running: ${stats.running}, Done: ${stats.done}`);
        console.log(`🔧 Resources: GPU: ${resourceUsage.gpu.active}/4, DB: ${resourceUsage.database.active}/6, API: ${resourceUsage.api.active}/10`);
    // }
}, 1000);

