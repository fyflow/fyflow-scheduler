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
  readonly type: 'concurrent' | 'rate-limit';

  // Capacity check (synchronous)
  canRun(): boolean;

  // Get current metrics
  getMetrics(): ResourceGroupMetrics;

  // Get lifetime stats (optional)
  getStats?(): ResourceGroupStats;

  // Events emitted by all groups:
  // - 'slot-released': When resource becomes available
  // - 'capacity-exhausted': When limit reached
}
