/**
 * Resource Group Interfaces
 *
 * Unified interfaces for all resource group types (ConcurrentLimitGroup, RateLimitGroup)
 */

export interface ResourceGroupMetrics {
  limit: number;
  running: number;
  available: number;
  utilization: number; // 0-1
}

export interface ResourceGroupStats {
  totalAcquired: number;
  totalReleased: number;
}

export interface ResourceGroup extends EventTarget {
  readonly id: string;
  readonly type: 'concurrent' | 'rate-limit' | 'keyed-rate-limit';

  /**
   * True when this group limits per key rather than globally. The scheduler
   * resolves a key with {@link ResourceGroup.keyFor} and passes it to
   * `canRun`/`onStart`/`onFinish`, and queues blocked tasks per key so a
   * saturated key cannot stall the others.
   */
  readonly keyed?: boolean;

  /**
   * Key this task belongs to, for keyed groups. Returning undefined or an empty
   * string makes `addTask` throw - a task with no key would otherwise silently
   * bypass or share a bucket.
   */
  keyFor?(task: unknown): string | undefined;

  /**
   * Capacity check (synchronous). `key` is supplied only for keyed groups; a
   * keyed group called without one reports whether ANY key has capacity, which
   * the scheduler uses as a cheap pre-filter.
   *
   * `cost` is how many units the caller intends to take, defaulting to 1. Only
   * concurrency groups weigh it - see {@link ResourceGroup.onStart}.
   */
  canRun(key?: string, cost?: number): boolean;

  /**
   * Take `cost` units (default 1). Called synchronously at the moment the
   * scheduler commits to running a task, or to creating a worker that declares
   * the group in `residentGroups`.
   */
  onStart?(key?: string, cost?: number): void;

  /** Return `cost` units (default 1). Must mirror the matching `onStart`. */
  onFinish?(key?: string, cost?: number): void;

  // Get current metrics
  getMetrics(): ResourceGroupMetrics;

  // Get lifetime stats (optional)
  getStats?(): ResourceGroupStats;

  // Events emitted by all groups:
  // - 'slot-released': When resource becomes available
  // - 'capacity-exhausted': When limit reached
}
