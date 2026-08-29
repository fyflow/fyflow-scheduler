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
   */
  canRun(key?: string): boolean;

  // Get current metrics
  getMetrics(): ResourceGroupMetrics;

  // Get lifetime stats (optional)
  getStats?(): ResourceGroupStats;

  // Events emitted by all groups:
  // - 'slot-released': When resource becomes available
  // - 'capacity-exhausted': When limit reached
}
