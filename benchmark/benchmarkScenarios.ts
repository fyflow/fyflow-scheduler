// Benchmark scenario definitions and execution
import { WorkerManager } from "../core/workerManager.ts";
import { FyflowScheduler, FyflowTask } from "../core/FyflowScheduler.ts";
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
    cpuSlots: number;
    fireAndForget?: boolean; // If true, use event-driven completion without promises
    /**
     * Run one throwaway task per worker instance before measuring, so worker
     * creation, module import and setup() land outside the measured window.
     *
     * Default true. Without it, a short run measures thread startup amortised
     * over the workload rather than steady-state coordination - measured, that
     * is the whole difference between 83% and 99% efficiency on 200 CPU tasks
     * across 4 threads. Set false to measure cold start deliberately.
     */
    warmupWorkers?: boolean;
}

export interface BenchmarkResult {
    config: BenchmarkConfig;
    metrics: {
        totalDuration: number;
        schedulerOverhead: number;
        asyncExecutionOverhead: number;
        coordinationOverhead: number; // New: pure scheduling/coordination overhead
        totalWorkerTime: number; // New: sum of all worker execution times
        /**
         * Worker startup cost, from the lifecycle events. Comparable across
         * runtimes - the same pool costs very different amounts to bring up on
         * Deno, Node and in a browser.
         */
        workerStartup: {
            workersStarted: number;
            avgInitMs: number;   // Creation through setup completion
            maxInitMs: number;
            avgSetupMs: number;  // The worker's own setup() only
        };
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
    private workerInitTimes: number[] = []; // Worker creation + setup, from lifecycle events
    private workerSetupTimes: number[] = [];

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

        const scheduler = new FyflowScheduler({ NoopWorker: workerManager, ...(cpuWorker ? { CPUWorker: cpuWorker } : {}) }, groups);

        // Worker startup cost, straight from the lifecycle events. Useful on its
        // own for comparing runtimes: the same pool costs very different amounts
        // to bring up on Deno, Node and in a browser.
        this.workerInitTimes = [];
        this.workerSetupTimes = [];
        for (const pool of [workerManager, cpuWorker].filter(Boolean) as WorkerManager[]) {
            pool.addEventListener('worker.initialization.completed', (e: any) => {
                if (typeof e.detail?.duration === 'number') this.workerInitTimes.push(e.detail.duration);
            });
            pool.addEventListener('worker.setup.completed', (e: any) => {
                if (typeof e.detail?.duration === 'number') this.workerSetupTimes.push(e.detail.duration);
            });
        }

        const setupMeasurement = this.timer.end('setup');
        console.log(`⚙️  Setup completed in ${formatDuration(setupMeasurement.duration)}`);

        // Generate tasks
        this.timer.start('task-generation');
        const tasks = this.generateIndependentTasks(config);
        const taskGenMeasurement = this.timer.end('task-generation');
        console.log(`📋 Generated ${tasks.length} tasks in ${formatDuration(taskGenMeasurement.duration)}`);

        // Instrument scheduler for dispatch time measurement
        this.instrumentScheduler(scheduler);

        // Warm the pool so worker creation and setup fall outside the measured
        // window. Workers are created lazily, so without this a short run
        // charges thread startup to coordination overhead.
        let warmupTasks = 0;
        if (config.warmupWorkers !== false) {
            // Warm only the pool the tasks will actually use - task generation
            // sends everything to one worker type
            const workerType = config.workerConfig.cpuWorker ? 'CPUWorker' : 'NoopWorker';
            const pool = config.workerConfig.cpuWorker ? cpuWorker! : workerManager;

            const warmups: FyflowTask[] = [];
            for (let i = 0; i < pool.maxThreads; i++) {
                warmups.push(new FyflowTask({
                    id: `warmup-${workerType}-${i}`,
                    workerType,
                    payload: { taskId: `warmup-${i}`, benchmarkData: { warmup: true } }
                }));
            }
            warmupTasks = warmups.length;
            await Promise.all(scheduler.addTasks(warmups, { createPromise: true }) as Promise<any>[]);
            console.log(`🔥 Warmed ${warmupTasks} worker instance(s)`);
        }

        // Track worker execution times for coordination overhead calculation.
        // Attached after warm-up so warm-up work is not counted.
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

        // Add all tasks - use fire-and-forget mode if configured
        if (config.fireAndForget) {
            // Fire-and-forget mode: no promises created
            for (const task of tasks) {
                scheduler.addTask(task); // Default: createPromise: false
            }
        } else {
            // Promise mode: create promises for waiting
            for (const task of tasks) {
                scheduler.addTask(task, { createPromise: true });
            }
        }

        console.log("📋 Tasks added to scheduler");

        // Wait for completion - stats include the warm-up tasks
        await this.waitForCompletion(scheduler, config.taskCount + warmupTasks);
        // clearInterval(montiorInterval);

        // Track execution end time
        const _executionEnd = performance.now();
        const executionMeasurement = this.timer.end('execution');

        const totalMeasurement = this.timer.end('total');

        const dispatchStats = this.dispatchTimes.getStats();
        const avg = (xs: number[]) => xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : 0;

        // Calculate scheduler overhead (dispatch time * iterations)
        const schedulerOverhead = dispatchStats.count > 0
            ? dispatchStats.mean * dispatchStats.count / 1000  // Convert μs to ms
            : 0;

        // Calculate async execution overhead (total execution time - theoretical minimum task time)
        const theoreticalTaskTime = config.taskCount * (config.workerConfig.taskDelay || 0);
        const asyncExecutionOverhead = Math.max(0, executionMeasurement.duration - theoreticalTaskTime);

        // Calculate coordination overhead: Total time - time if all work was sequential
        const totalWorkerTime = this.workerExecutionTimes.reduce((sum, time) => sum + time, 0);
        // Guard against the timing feed silently breaking: without per-task execution
        // times, both coordination overhead and overall efficiency are meaningless.
        if (this.workerExecutionTimes.length === 0) {
            console.warn(`⚠️  No worker execution times collected - 'task.completed' events carried no executionTime. Coordination overhead and overall efficiency are not measurable for this run.`);
        }
        // For truly concurrent execution, the minimum possible time would be
        // totalWorkerTime / maxConcurrency.
        //
        // The ceiling starts at pool capacity, which is
        // maxThreads x maxConcurrentTasks for BOTH pool types. Inline pools are
        // not single-instance: maxThreads inline worker instances are created,
        // each admitting maxConcurrentTasks, and their awaits overlap on the
        // event loop. Measured, 4x5 and 1x20 inline both peak at 20 in-flight
        // tasks. Using maxConcurrentTasks alone understated the ceiling by
        // maxThreads and pinned inline scenarios at a clamped 100%.
        //
        // Then cap by whichever resource groups this scenario ACTUALLY applies.
        // cpuSlots is not a universal limit: the 'cpu' group is only created for
        // threaded pools and only attached to tasks when cpuWorker is set, so
        // for an inline volume scenario it constrains nothing.
        let maxPossibleConcurrency =
            config.workerConfig.maxThreads * config.workerConfig.maxConcurrentTasks;

        if (config.workerConfig.cpuWorker && !config.workerConfig.inline) {
            maxPossibleConcurrency = Math.min(maxPossibleConcurrency, config.cpuSlots);
        }
        if (config.groupConfig) {
            maxPossibleConcurrency = Math.min(
                maxPossibleConcurrency,
                config.groupConfig.groupCount * config.groupConfig.groupLimit
            );
        }
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
                workerStartup: {
                    workersStarted: this.workerInitTimes.length,
                    avgInitMs: avg(this.workerInitTimes),
                    maxInitMs: this.workerInitTimes.length ? Math.max(...this.workerInitTimes) : 0,
                    avgSetupMs: avg(this.workerSetupTimes)
                },
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

    private generateIndependentTasks(config: BenchmarkConfig): FyflowTask[] {
        const tasks: FyflowTask[] = [];

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

            tasks.push(new FyflowTask(taskConfig));
        }

        return tasks;
    }


    private instrumentScheduler(scheduler: FyflowScheduler) {
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

    private waitForCompletion(scheduler: FyflowScheduler, expectedTasks: number): Promise<void> {
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
        const startup = result.metrics.workerStartup;
        if (startup.workersStarted > 0) {
            console.log(`🚀 Worker Startup: ${startup.workersStarted} worker(s), avg ${formatDuration(startup.avgInitMs)} init (${formatDuration(startup.avgSetupMs)} setup), max ${formatDuration(startup.maxInitMs)}`);
        }
        console.log(`📈 Overall Efficiency: ${result.metrics.overallEfficiency.toFixed(1)}%`);
        console.log(`📊 Task Throughput: ${result.metrics.taskThroughput.toFixed(2)} tasks/sec`);
        console.log(`🧠 Memory Delta: ${formatBytes(result.metrics.memoryUsage.delta)}`);
        console.log(`🧠 Peak Memory: ${formatBytes(result.metrics.memoryUsage.peak)}`);
        console.log(`🔄 Avg Dispatch Time: ${formatDuration(result.metrics.dispatchMetrics.averageDispatchTime)}`);
        console.log(`🔄 Max Dispatch Time: ${formatDuration(result.metrics.dispatchMetrics.maxDispatchTime)}`);
        console.log(`🔄 Dispatch Iterations: ${result.metrics.dispatchMetrics.dispatchIterations}`);
    }
}