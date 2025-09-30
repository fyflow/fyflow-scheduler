import { WorkerManager, DagScheduler, DagTask, ConcurrentLimitGroup } from "../index.ts";

/**
 * Advanced Features Example
 *
 * Demonstrates:
 * - Dynamic task spawning
 * - Resource groups and constraints
 * - Complex workflow orchestration
 * - Worker groups at WorkerManager level (NEW API)
 * - Event-driven task spawning
 * - Task tree reconstruction and visualization
 */

console.log('🚀 FyFlow Advanced Features Example');
console.log('===================================\n');

// Step 1: Create resource managers

const gpuGroup = new ConcurrentLimitGroup(3); // Limit GPU-intensive tasks
console.log('📦 Created CPU manager (8 slots) and GPU group (3 concurrent)');

// Step 2: Create worker pools with groups (NEW API)
const workerPools = {
    DataProcessor: new WorkerManager(
        new URL("./workers/dataProcessor.ts", import.meta.url).href,
        {
            maxThreads: 2,
            maxConcurrentTasks: 1,
            config: { analysisDepth: 'deep' },
            idleTimeout: 200
        }
    ),

    // Progress-reporting workers for data processing
    ProgressWorker: new WorkerManager(
        new URL("./workers/progressWorker.ts", import.meta.url).href,
        {
            maxThreads: 2,
            maxConcurrentTasks: 1,
            groups: ['cpu'], // CPU constraint at WorkerManager level
            idleTimeout: 200
        }
    ),

    // CPU-intensive workers with GPU constraints
    CpuWorker: new WorkerManager(
        new URL("./workers/cpuWorker.ts", import.meta.url).href,
        {
            maxThreads: 3,
            maxConcurrentTasks: 1,
            groups: ['gpu'], // GPU constraint at WorkerManager level
            config: { iterations: 500000 },
            idleTimeout: 200
        }
    ),

    // High-concurrency async workers (no GPU needed)
    AsyncWorker: new WorkerManager(
        new URL("./workers/asyncWorker.ts", import.meta.url).href,
        {
            maxThreads: 2,
            maxConcurrentTasks: 5,
            inline: true,
            config: { connectionPoolSize: 3 },
            idleTimeout: 200
        }
    )
};

const cpuGroup = new ConcurrentLimitGroup(4); // Limit CPU-intensive tasks
const scheduler = new DagScheduler(workerPools, { gpu: gpuGroup, cpu: cpuGroup });
console.log('🏭 Created worker pools with resource constraints');

// Step 3: Set up event listeners for dynamic spawning
scheduler.addEventListener('task.running', (e: any) => {
    console.log(`🚀 RUNNING: ${e.detail.id}`);
});

scheduler.addEventListener('task.completed', (e: any) => {
    const task = e.detail;
    console.log(`✅ COMPLETED: ${task.id}`);
});

scheduler.addEventListener('task.spawn_request', (e: any) => {
    const { parentTask, spawnConfig } = e.detail;
    console.log(`🌱 Task ${parentTask.id} spawning: ${spawnConfig.id} (${spawnConfig.workerType})`);
});



// Step 5: Create initial data processing tasks
const initialTasks = [
    new DagTask({
        id: 'analyze-multimedia-dataset',
        workerType: 'DataProcessor',
        payload: {
            dataset: 'multimedia-collection-2024',
            analysisType: 'multimedia'
        }
    }),
    new DagTask({
        id: 'analyze-document-corpus',
        workerType: 'DataProcessor',
        payload: {
            dataset: 'research-papers-corpus',
            analysisType: 'documents'
        }
    }),
    new DagTask({
        id: 'analyze-mixed-data',
        workerType: 'DataProcessor',
        payload: {
            dataset: 'enterprise-data-lake',
            analysisType: 'mixed'
        }
    })
];

console.log('\n📊 Starting dynamic workflow with 3 analysis tasks...\n');

// Step 6: Add initial tasks
initialTasks.forEach(task => {
    scheduler.addTask(task);
});

// Step 7: Monitor progress and resource usage
const progressInterval = setInterval(() => {
    const stats = scheduler.stats;
    if (stats.queued > 0 || stats.running > 0) {
        console.log(`📈 Stats: Queued: ${stats.queued}, Running: ${stats.running}, Done: ${stats.done}, Failed: ${stats.failed}`);

        // Show task progress
    } else if (stats.done > 0) {
        clearInterval(progressInterval);
    }
}, 3000);

scheduler.addEventListener('scheduler.completed', (e: any) => {
    clearInterval(progressInterval);
    const stats = e.detail;
    console.log('\n🎯 All Tasks Completed!');
    console.log(`✅ Completed: ${stats.done} tasks`);
    console.log(`❌ Failed: ${stats.failed} tasks`);

    // Demonstrate task tree reconstruction
    displayTaskTree(scheduler);

    console.log('\n🎉 Advanced features demonstrated successfully!');
});

// Function to reconstruct and display the complete task tree
function displayTaskTree(scheduler: any) {
    const tasks = scheduler.tasks;
    const visited = new Set<string>();

    // Find root tasks (no parents)
    const rootTasks: string[] = Array.from(tasks.keys()).filter(id => {
        const task = tasks.get(id);
        return task && task.parents.length === 0;
    }) as string[];

    console.log("\n🌳 Task Tree Structure (Dynamic Workflow Orchestration):");
    console.log("=" .repeat(60));

    function printTaskTree(taskId: string, depth: number = 0) {
        if (visited.has(taskId)) return;
        visited.add(taskId);

        const task = tasks.get(taskId);
        if (!task) return;

        const indent = "  ".repeat(depth);
        const prefix = depth === 0 ? "📁" : "├─";
        const status = getStatusIcon(task.state);

        console.log(`${indent}${prefix} ${status} ${task.id} [${task.workerType}]`);

        // Print children in sorted order
        if (task.children.size > 0) {
            const childArray: string[] = Array.from(task.children).sort() as string[];
            childArray.forEach((childId: string) => {
                printTaskTree(childId, depth + 1);
            });
        }
    }

    rootTasks.forEach(rootId => printTaskTree(rootId));

    // Show tree statistics
    const stats = getTreeStatistics(scheduler);
    console.log("\n📊 Dynamic Tree Statistics:");
    console.log(`   Total tasks created: ${stats.total}`);
    console.log(`   Root workflows: ${stats.roots}`);
    console.log(`   Dynamically spawned: ${stats.spawned}`);
    console.log(`   Max workflow depth: ${stats.maxDepth}`);
}

function getStatusIcon(state: string): string {
    switch (state) {
        case 'pending': return '⏳';
        case 'running': return '🚀';
        case 'done': return '✅';
        case 'failed': return '❌';
        default: return '❓';
    }
}

function getTreeStatistics(scheduler: any) {
    const tasks = scheduler.tasks;
    const roots = Array.from(tasks.values()).filter((t: any) => t.parents.length === 0).length;
    const spawned = Array.from(tasks.values()).filter((t: any) => t.parents.length > 0).length;

    // Calculate max depth
    let maxDepth = 0;
    function calculateDepth(taskId: string, currentDepth: number = 0): number {
        const task = tasks.get(taskId);
        if (!task || task.children.size === 0) return currentDepth;

        let deepest = currentDepth;
        for (const childId of task.children) {
            const childDepth = calculateDepth(childId, currentDepth + 1);
            deepest = Math.max(deepest, childDepth);
        }
        return deepest;
    }

    for (const [taskId, task] of tasks) {
        if ((task as any).parents.length === 0) {
            maxDepth = Math.max(maxDepth, calculateDepth(taskId));
        }
    }

    return {
        total: tasks.size,
        roots,
        spawned,
        maxDepth
    };
}
