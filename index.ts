// FyFlow - Distributed Task Processing Framework
// Main library exports

// Core scheduler and task management
export { FyflowScheduler, FyflowTask } from "./core/FyflowScheduler.ts";
export type { FyflowSchedulerOptions, AddTaskOptions } from "./core/FyflowScheduler.ts";

// Resource management - use ConcurrentLimitGroup for CPU constraints
// GlobalCPUManager removed in favor of group-based approach

// Groups
export { ConcurrentLimitGroup } from "./groups/concurrentLimitGroup.ts";
export { RateLimitGroup } from "./groups/rateLimitGroup.ts";
export type { ResourceGroup, ResourceGroupMetrics, ResourceGroupStats } from "./groups/resourceGroup.ts";

// Worker management
export { WorkerManager } from "./core/workerManager.ts";
export { ThreadWrapper } from "./core/threadWrapper.ts";
export { InlineWrapper } from "./core/inlineWrapper.ts";

// Types and interfaces
export type { WorkerManagerOptions } from "./core/workerManager.ts";
export type { WorkerInterface, WorkerConfig, BaseWorkerContext, TaskWorkerContext } from "./core/workerInterface.ts";
export { BaseWorker } from "./core/workerInterface.ts";

