// FyFlow - Distributed Task Processing Framework
// Main library exports

// Core scheduler and task management
export { DagScheduler, DagTask } from "./core/dagScheduler.ts";

// Resource management - use ConcurrentLimitGroup for CPU constraints
// GlobalCPUManager removed in favor of group-based approach

// Groups
export { ConcurrentLimitGroup } from "./groups/concurrentLimitGroup.ts";
export { RateLimitGroup } from "./groups/rateLimitGroup.ts";

// Worker management
export { WorkerManager } from "./core/workerManager.ts";
export { ThreadWrapper } from "./core/threadWrapper.ts";
export { InlineWrapper } from "./core/inlineWrapper.ts";

// Types and interfaces
export type { WorkerManagerOptions } from "./core/workerManager.ts";
export type { WorkerInterface, WorkerConfig } from "./core/workerInterface.ts";
export { BaseWorker } from "./core/workerInterface.ts";

