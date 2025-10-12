#!/usr/bin/env -S deno run --allow-read --allow-net --allow-write

/**
 * Enhanced Features Example - Demonstrates the new worker communication features
 *
 * This example showcases:
 * - Progress reporting from workers
 * - Dynamic task spawning using the new spawn API
 * - Real-time monitoring of worker events
 * - Enhanced worker communication protocol
 */

import { FyflowScheduler, FyflowTask } from "../core/FyflowScheduler.ts";
import { WorkerManager } from "../core/workerManager.ts";
import { ConcurrentLimitGroup } from "../groups/concurrentLimitGroup.ts";

console.log("🚀 Enhanced Features Example - Progress & Spawn API Demo");
console.log("=" .repeat(60));

// Create resource groups
const cpuGroup = new ConcurrentLimitGroup(4);

// Create worker managers for different worker types
const progressWorkerManager = new WorkerManager(
    new URL("./workers/progressWorker.ts", import.meta.url).href,
    { maxThreads: 2, maxConcurrentTasks: 1, inline: false }
);

const dataProcessorManager = new WorkerManager(
    new URL("./workers/dataProcessor.ts", import.meta.url).href,
    { maxThreads: 2, maxConcurrentTasks: 1, inline: true }
);

// Create scheduler with worker pools
const scheduler = new FyflowScheduler({
    ProgressWorker: progressWorkerManager,
    DataProcessor: dataProcessorManager
}, {
    cpu: cpuGroup
});

// Set up event listeners to monitor enhanced features
scheduler.addEventListener('task.progress', (e: any) => {
    const { id, progress, message, details } = e.detail;
    const progressBar = "█".repeat(Math.floor(progress * 20)) + "░".repeat(20 - Math.floor(progress * 20));
    console.log(`📊 [${id}] ${progressBar} ${(progress * 100).toFixed(1)}% - ${message}`);

    if (details) {
        console.log(`   └─ Details: processed=${details.itemsProcessed}, remaining=${details.itemsRemaining}, eta=${details.estimatedTimeRemainingMs}ms`);
    }
});

scheduler.addEventListener('task.spawn_request', (e: any) => {
    const { parentTask, spawnConfig } = e.detail;
    console.log(`🌱 Task ${parentTask.id} spawning: ${spawnConfig.id} (${spawnConfig.workerType})`);
});

scheduler.addEventListener('task.completed', (e: any) => {
    const { id, result } = e.detail;
    console.log(`✅ Task ${id} completed:`, result?.spawningMethod || 'basic');
});

scheduler.addEventListener('scheduler.completed', (e: any) => {
    console.log("\n🎉 All tasks completed!");
    console.log(`📈 Final stats:`, e.detail);
    console.log("\n🔍 Enhanced Features Demonstrated:");
    console.log("   ✓ Real-time progress reporting");
    console.log("   ✓ Dynamic task spawning via new API");
    console.log("   ✓ Enhanced worker communication protocol");
    console.log("   ✓ Event-driven monitoring");
});

async function runDemo() {
    console.log("\n🎯 Demo 1: Progress Reporting");
    console.log("-".repeat(40));

    // Create a task that demonstrates progress reporting
    const progressTask = new FyflowTask({
        id: "progress-demo",
        workerType: "ProgressWorker",
        payload: {
            operation: "data_processing",
            size: 100,
            includeDetails: true
        }
    });

    scheduler.addTask(progressTask);
    await progressTask.onCompleteDescendants();
    console.log("✅ Progress task and all descendants completed!");

    console.log("\n🎯 Demo 2: Dynamic Task Spawning with Descendants");
    console.log("-".repeat(50));

    // Create a task that demonstrates dynamic spawning
    const spawnTask = new FyflowTask({
        id: "spawn-demo",
        workerType: "DataProcessor",
        payload: {
            dataset: "sample-dataset",
            analysisType: "mixed",
            useSpawnAPI: true // Use new spawn API
        }
    });

    console.log("Adding spawn task and waiting for ALL descendants...");
    scheduler.addTask(spawnTask);
    await spawnTask.onCompleteDescendants();
    console.log("✅ Spawn task and ALL descendants completed!");
}

// Run the demo
await runDemo();