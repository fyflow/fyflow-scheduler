// Benchmark scenario definitions and execution
import { WorkerManager } from "../core/workerManager.ts";
import { DagScheduler, DagTask } from "../core/dagScheduler.ts";
import { ConcurrentLimitGroup } from "../groups/concurrentLimitGroup.ts";
import { PerfTimer, formatBytes, formatDuration, StatisticsCollector, RuntimeUtils, type PerfMeasurement } from "./perfUtils.ts";

export interface BenchmarkConfig {
    name: string;
    description: string;
    taskCount: number;
    workerConfig: {
        maxThreads: number;
        maxConcurrentTasks: number;
        inline: boolean;
        taskDelay?: number;
        cpuWorker?: boolean;
    };
    groupConfig?: {
        groupCount: number;
        groupLimit: number;
        tasksPerGroup?: number;
    };
    dependencyConfig?: {
        type: 'none' | 'chain' | 'tree' | 'mixed';
        depth?: number;
        fanout?: number;
    };
    cpuSlots: number;
}

export interface BenchmarkResult {
    config: BenchmarkConfig;
    metrics: {
        totalDuration: number;
        schedulerOverhead: number;
        asyncExecutionOverhead: number;
        coordinationOverhead: number; // New: pure scheduling/coordination overhead
        totalWorkerTime: number; // New: sum of all worker execution times
        overallEfficiency: number; // New: normalized efficiency percentage (0-100%)
        taskThroughput: number; // tasks/second
        memoryUsage: {
            peak: number;
            delta: number;
        };
        dispatchMetrics: {
            averageDispatchTime: number;
            maxDispatchTime: number;
            dispatchIterations: number;
        };
    };
    rawMeasurements: PerfMeasurement[];
}

export class BenchmarkRunner {
    private timer = new PerfTimer();
    private dispatchTimes = new StatisticsCollector();
    private workerExecutionTimes: number[] = []; // Track individual worker execution times

    async runBenchmark(config: BenchmarkConfig): Promise<BenchmarkResult> {
        console.log(`\n🚀 Running benchmark: ${config.name}`);
        console.log(`📝 ${config.description}`);
        console.log(`📊 Tasks: ${config.taskCount}, CPU Slots: ${config.cpuSlots}`);

        // Setup infrastructure
        this.timer.start('total');
        this.timer.start('setup');

        // Create groups if specified
        const groups: Record<string, ConcurrentLimitGroup> = {};
        if (config.groupConfig) {
            for (let i = 0; i < config.groupConfig.groupCount; i++) {
                groups[`group-${i}`] = new ConcurrentLimitGroup(config.groupConfig.groupLimit);
            }
        }

        // Create CPU group for threaded workers (replaces GlobalCPUManager)
        if (!config.workerConfig.inline) {
            groups['cpu'] = new ConcurrentLimitGroup(config.cpuSlots);
        }

        // Create worker manager with groups at pool level
        const workerManagerOptions: any = {
            maxThreads: config.workerConfig.maxThreads,
            maxConcurrentTasks: config.workerConfig.maxConcurrentTasks,
            inline: config.workerConfig.inline,
            config: { taskDelay: config.workerConfig.taskDelay || 0 }
        };

        // Add groups to worker manager if using group config
        if (config.groupConfig) {
            workerManagerOptions.groups = Object.keys(groups);
        }

        // Resolve worker URL cross-platform
        let noopWorkerUrl: string;

        let cpuWorker: WorkerManager | null = null;
        if (typeof Deno !== "undefined") {
            noopWorkerUrl = new URL("./noopWorker.ts", import.meta.url).href;
        } else {
            // Node/Browser esbuild replaces this
            // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
            noopWorkerUrl = new URL((await import("./noopWorker.ts?worker-direct")).default);
        }

        if(config.workerConfig.cpuWorker) {
            let cpuWorkerUrl: string;
            if (typeof Deno !== "undefined") {
                cpuWorkerUrl = new URL("./cpuWorker.ts", import.meta.url).href;
            } else {
                // Node/Browser esbuild replaces this
                // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
                cpuWorkerUrl = new URL((await import("./cpuWorker.ts?worker-direct")).default);
            }
            cpuWorker = new WorkerManager(
                cpuWorkerUrl,
                workerManagerOptions
            );
        }

        const workerManager = new WorkerManager(
            noopWorkerUrl,
            workerManagerOptions
        );

        const scheduler = new DagScheduler({ NoopWorker: workerManager, ...(cpuWorker ? { CPUWorker: cpuWorker } : {}) }, groups);

        const setupMeasurement = this.timer.end('setup');
        console.log(`⚙️  Setup completed in ${formatDuration(setupMeasurement.duration)}`);

        // Generate tasks
        this.timer.start('task-generation');
        const tasks = this.generateTasks(config);
        const taskGenMeasurement = this.timer.end('task-generation');
        console.log(`📋 Generated ${tasks.length} tasks in ${formatDuration(taskGenMeasurement.duration)}`);

        // Instrument scheduler for dispatch time measurement
        this.instrumentScheduler(scheduler);

        // Track worker execution times for coordination overhead calculation
        this.workerExecutionTimes = []; // Reset for each benchmark
        scheduler.addEventListener('task.completed', (e: any) => {
            if (e.detail?.executionTime !== undefined) {
                this.workerExecutionTimes.push(e.detail.executionTime);
            }
        });

        //add logic to monitor the number of tasks in the scheduler
        const monitorTasks = () => {
            const stats = scheduler.stats;
            console.log(`📊 Tasks in scheduler: q:${stats.queued} r:${stats.running} d:${stats.done} f:${stats.failed}`);
        };
        
        // const montiorInterval = setInterval(monitorTasks, 1000);

        // Execute benchmark
        this.timer.start('execution');
        // Track execution start time
        const _executionStart = performance.now();

        // Add all tasks - use batch API for high volume scenarios to prevent Node.js worker_threads overflow

        // if (tasks.length >= 10000) {
        //     scheduler.addTasks(tasks);
        // } else {
            for (const task of tasks) {
                scheduler.addTask(task);
            }

            
        // }

        console.log("📋 Tasks added to scheduler");



        // Wait for completion
        await this.waitForCompletion(scheduler, config.taskCount);
        // clearInterval(montiorInterval);

        // Track execution end time
        const _executionEnd = performance.now();
        const executionMeasurement = this.timer.end('execution');

        const totalMeasurement = this.timer.end('total');

        const dispatchStats = this.dispatchTimes.getStats();

        // Calculate scheduler overhead (dispatch time * iterations)
        const schedulerOverhead = dispatchStats.count > 0
            ? dispatchStats.mean * dispatchStats.count / 1000  // Convert μs to ms
            : 0;

        // Calculate async execution overhead (total execution time - theoretical minimum task time)
        const theoreticalTaskTime = config.taskCount * (config.workerConfig.taskDelay || 0);
        const asyncExecutionOverhead = Math.max(0, executionMeasurement.duration - theoreticalTaskTime);

        // Calculate coordination overhead: Total time - time if all work was sequential
        const totalWorkerTime = this.workerExecutionTimes.reduce((sum, time) => sum + time, 0);
        // For truly concurrent execution, the minimum possible time would be totalWorkerTime / maxConcurrency
        const maxPossibleConcurrency = config.workerConfig.inline
            ? config.workerConfig.maxConcurrentTasks
            : Math.min(config.workerConfig.maxThreads * config.workerConfig.maxConcurrentTasks, config.cpuSlots);
        const theoreticalMinTime = totalWorkerTime / maxPossibleConcurrency;
        const coordinationOverhead = Math.max(0, executionMeasurement.duration - theoreticalMinTime);

        // Calculate overall efficiency: (Total Worker Time / Total Duration) / Max Possible Concurrency
        // This gives us a normalized efficiency percentage (0-100%) comparable across different configurations
        const rawEfficiency = totalWorkerTime / executionMeasurement.duration;
        const overallEfficiency = Math.min(100, (rawEfficiency / maxPossibleConcurrency) * 100);

        const result: BenchmarkResult = {
            config,
            metrics: {
                totalDuration: totalMeasurement.duration,
                schedulerOverhead: schedulerOverhead,
                asyncExecutionOverhead: asyncExecutionOverhead,
                coordinationOverhead: coordinationOverhead,
                totalWorkerTime: totalWorkerTime,
                overallEfficiency: overallEfficiency,
                taskThroughput: config.taskCount / (executionMeasurement.duration / 1000),
                memoryUsage: {
                    peak: totalMeasurement.memoryAfter.heapUsed,
                    delta: totalMeasurement.memoryDelta.heapUsed
                },
                dispatchMetrics: {
                    averageDispatchTime: dispatchStats.mean,
                    maxDispatchTime: dispatchStats.max,
                    dispatchIterations: dispatchStats.count
                }
            },
            rawMeasurements: [setupMeasurement, taskGenMeasurement, executionMeasurement, totalMeasurement]
        };

        this.printResults(result);
        this.dispatchTimes.clear();

        return result;
    }

    private generateTasks(config: BenchmarkConfig): DagTask[] {
        // This method delegates to specific generators
        const _tasks: DagTask[] = [];

        switch (config.dependencyConfig?.type || 'none') {
            case 'none':
                return this.generateIndependentTasks(config);
            case 'chain':
                return this.generateChainTasks(config);
            case 'tree':
                return this.generateTreeTasks(config);
            case 'mixed':
                return this.generateMixedTasks(config);
            default:
                return this.generateIndependentTasks(config);
        }
    }

    private generateIndependentTasks(config: BenchmarkConfig): DagTask[] {
        const tasks: DagTask[] = [];

        for (let i = 0; i < config.taskCount; i++) {
            const taskConfig: any = {
                id: `task-${i}`,
                workerType: config.workerConfig.cpuWorker ? 'CPUWorker' : 'NoopWorker',
                payload: {
                    taskId: `task-${i}`,
                    benchmarkData: { index: i }
                }
            };

            // Add group assignments
            const workerGroups: string[] = [];

            // CPU workers automatically get CPU group
            if (config.workerConfig.cpuWorker && !config.workerConfig.inline) {
                workerGroups.push('cpu');
            }

            // Add custom groups if specified
            if (config.groupConfig) {
                // Distribute tasks across groups
                const groupId = i % config.groupConfig.groupCount;
                workerGroups.push(`group-${groupId}`);
            }

            if (workerGroups.length > 0) {
                taskConfig.workerGroups = workerGroups;
            }

            tasks.push(new DagTask(taskConfig));
        }

        return tasks;
    }

    private generateChainTasks(config: BenchmarkConfig): DagTask[] {
        const tasks: DagTask[] = [];
        const depth = config.dependencyConfig?.depth || config.taskCount;

        for (let i = 0; i < config.taskCount && i < depth; i++) {
            const parents = i > 0 ? [`chain-task-${i-1}`] : [];

            tasks.push(new DagTask({
                id: `chain-task-${i}`,
                workerType: 'NoopWorker',
                payload: {
                    taskId: `chain-task-${i}`,
                    benchmarkData: { chainIndex: i }
                },
                parents
            }));
        }

        return tasks;
    }

    private generateTreeTasks(config: BenchmarkConfig): DagTask[] {
        const tasks: DagTask[] = [];
        const fanout = config.dependencyConfig?.fanout || 10;
        let taskId = 0;

        // Root task
        tasks.push(new DagTask({
            id: `tree-task-${taskId++}`,
            workerType: 'NoopWorker',
            payload: {
                taskId: `tree-task-0`,
                benchmarkData: { treeLevel: 0, treeIndex: 0 }
            }
        }));

        // Generate tree levels
        let currentLevel = [`tree-task-0`];
        let level = 1;

        while (taskId < config.taskCount && currentLevel.length > 0) {
            const nextLevel: string[] = [];

            for (const parent of currentLevel) {
                for (let i = 0; i < fanout && taskId < config.taskCount; i++) {
                    const childId = `tree-task-${taskId++}`;
                    nextLevel.push(childId);

                    tasks.push(new DagTask({
                        id: childId,
                        workerType: 'NoopWorker',
                        payload: {
                            taskId: childId,
                            benchmarkData: { treeLevel: level, treeIndex: i }
                        },
                        parents: [parent]
                    }));
                }
            }

            currentLevel = nextLevel;
            level++;
        }

        return tasks;
    }

    private generateMixedTasks(config: BenchmarkConfig): DagTask[] {
        const chainTasks = Math.floor(config.taskCount * 0.3);
        const treeTasks = Math.floor(config.taskCount * 0.3);
        const independentTasks = config.taskCount - chainTasks - treeTasks;

        const tasks: DagTask[] = [];

        // Add chain tasks
        tasks.push(...this.generateChainTasks({
            ...config,
            taskCount: chainTasks,
            dependencyConfig: { type: 'chain' }
        }));

        // Add tree tasks
        tasks.push(...this.generateTreeTasks({
            ...config,
            taskCount: treeTasks,
            dependencyConfig: { type: 'tree', fanout: 5 }
        }));

        // Add independent tasks
        tasks.push(...this.generateIndependentTasks({
            ...config,
            taskCount: independentTasks,
            dependencyConfig: { type: 'none' }
        }));

        return tasks;
    }

    private instrumentScheduler(scheduler: DagScheduler) {
        // Hook into the scheduler's dispatch loop to measure dispatch times
        // deno-lint-ignore no-explicit-any
        const originalDispatch = (scheduler as any)._dispatchLoop;
        if (originalDispatch) {
            // deno-lint-ignore no-explicit-any
            (scheduler as any)._dispatchLoop = (...args: unknown[]) => {
                const start = performance.now();
                const result = originalDispatch.apply(scheduler, args);
                const end = performance.now();
                this.dispatchTimes.add(end - start);
                return result;
            };
        }
    }

    private waitForCompletion(scheduler: DagScheduler, expectedTasks: number): Promise<void> {
        return new Promise((resolve) => {
            const checkCompletion = () => {
                const stats = scheduler.stats;
                if (stats.done + stats.failed >= expectedTasks) {
                    resolve();
                } else {
                    setTimeout(checkCompletion, 100);
                }
            };
            checkCompletion();
        });
    }

    private printResults(result: BenchmarkResult) {
        console.log(`\n📈 Benchmark Results: ${result.config.name}`);
        console.log(`⏱️  Total Duration: ${formatDuration(result.metrics.totalDuration)}`);
        console.log(`🔧 Scheduler Overhead: ${formatDuration(result.metrics.schedulerOverhead)}`);
        console.log(`⚡ Async Execution Overhead: ${formatDuration(result.metrics.asyncExecutionOverhead)}`);
        console.log(`🎯 Coordination Overhead: ${formatDuration(result.metrics.coordinationOverhead)}`);
        console.log(`⚙️  Total Worker Time: ${formatDuration(result.metrics.totalWorkerTime)}`);
        console.log(`📈 Overall Efficiency: ${result.metrics.overallEfficiency.toFixed(1)}%`);
        console.log(`📊 Task Throughput: ${result.metrics.taskThroughput.toFixed(2)} tasks/sec`);
        console.log(`🧠 Memory Delta: ${formatBytes(result.metrics.memoryUsage.delta)}`);
        console.log(`🧠 Peak Memory: ${formatBytes(result.metrics.memoryUsage.peak)}`);
        console.log(`🔄 Avg Dispatch Time: ${formatDuration(result.metrics.dispatchMetrics.averageDispatchTime)}`);
        console.log(`🔄 Max Dispatch Time: ${formatDuration(result.metrics.dispatchMetrics.maxDispatchTime)}`);
        console.log(`🔄 Dispatch Iterations: ${result.metrics.dispatchMetrics.dispatchIterations}`);
    }
}