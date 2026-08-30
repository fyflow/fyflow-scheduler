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
export type { RateWindow } from "./groups/rateLimitGroup.ts";
export { KeyedRateLimitGroup } from "./groups/keyedRateLimitGroup.ts";
export type { KeyedRateLimitGroupOptions, KeyedTaskLike } from "./groups/keyedRateLimitGroup.ts";
export type {
  ResourceGroup,
  ResourceGroupMetrics,
  ResourceGroupStats,
  GaugeKind,
  GaugeSpec,
  GaugeReading,
  GaugeDescription
} from "./groups/resourceGroup.ts";

// Resource events (scheduler-only - a WorkerManager never dispatches these)
export type {
  ResourceLifetime,
  HolderKind,
  ResourceReleaseReason,
  ResourceUnblockReason,
  ResourceEventDetail,
  ResourceAcquiredDetail,
  ResourceReleasedDetail,
  ResourceBlockedDetail,
  ResourceUnblockedDetail,
  AdmissionWaiter
} from "./core/resourceEvents.ts";

// Worker management
export { WorkerManager } from "./core/workerManager.ts";
export { ThreadWrapper } from "./core/threadWrapper.ts";
export { InlineWrapper } from "./core/inlineWrapper.ts";

// Types and interfaces
export type { WorkerManagerOptions } from "./core/workerManager.ts";
export type {
  WorkerInterface,
  WorkerConfig,
  BaseWorkerContext,
  TaskWorkerContext,
  WorkerContext,
  SpawnTaskConfig,
  ProgressData,
  WorkerStatus,
  WorkerInstanceState
} from "./core/workerInterface.ts";
export { BaseWorker, WorkerTerminationError } from "./core/workerInterface.ts";

