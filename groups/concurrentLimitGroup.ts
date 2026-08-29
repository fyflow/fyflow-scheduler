import type { ResourceGroup, ResourceGroupMetrics, ResourceGroupStats } from './resourceGroup.ts';

/**
 * ConcurrentLimitGroup - Optimistic resource limits with soft concurrency control
 *
 * Uses optimistic allocation: checks capacity at dispatch, acquires at execution.
 * May temporarily exceed limit by up to maxThreads × maxConcurrentTasks due to race conditions.
 *
 * Best for:
 * - CPU scheduling (soft limit, temporary overshoot acceptable)
 * - Throughput limiting (approximate rate control)
 * - Load balancing (advisory limits)
 *
 * Note: May temporarily exceed limit by up to maxThreads × maxConcurrentTasks due to race conditions.
 *
 * Example:
 * ```typescript
 * const cpuLimit = new ConcurrentLimitGroup(8, 'cpu'); // ~8 concurrent, may briefly exceed
 * ```
 */
export class ConcurrentLimitGroup extends EventTarget implements ResourceGroup {
    readonly id: string;
    readonly type = 'concurrent' as const;
    limit: number;
    running = 0;
    private stats = {
      totalAcquired: 0,
      totalReleased: 0
    };

    constructor(limit: number, id?: string) {
      super();
      this.limit = limit;
      this.id = id || `concurrent-${Math.random().toString(36).substr(2, 9)}`;
    }

    /**
     * Room for `cost` more units (default 1).
     *
     * Weighted callers are worker pools declaring this group in
     * `residentGroups`: a pool whose model needs 20 of 24 units asks for 20, and
     * is refused while four 2-unit workers hold 8.
     */
    canRun(_key?: string, cost: number = 1): boolean {
      return this.running + cost <= this.limit;
    }

    getMetrics(): ResourceGroupMetrics {
      return {
        limit: this.limit,
        running: this.running,
        available: Math.max(0, this.limit - this.running),
        utilization: this.running / this.limit
      };
    }

    getStats(): ResourceGroupStats {
      return {
        totalAcquired: this.stats.totalAcquired,
        totalReleased: this.stats.totalReleased
      };
    }

    onStart(_key?: string, cost: number = 1) {
      // Increment running count
      // Note: May temporarily exceed limit by 1 due to race conditions when multiple
      // workers start tasks simultaneously, but this is acceptable and resolves quickly
      this.running += cost;
      this.stats.totalAcquired += cost;

      // Emit capacity exhausted if we hit/exceed limit
      if (this.running >= this.limit) {
        this.dispatchEvent(new CustomEvent('capacity-exhausted', {
          detail: { running: this.running, limit: this.limit, groupId: this.id }
        }));
      }
    }

    onFinish(_key?: string, cost: number = 1) {
      const wasAtLimit = this.running >= this.limit;
      this.running = Math.max(0, this.running - cost);
      this.stats.totalReleased += cost;

      // Emit slot-released event if we were at limit and now have capacity
      if (wasAtLimit && this.canRun()) {
        this.dispatchEvent(new CustomEvent('slot-released', {
          detail: {
            groupType: 'concurrent-limit',
            groupId: this.id,
            running: this.running,
            limit: this.limit,
            available: this.limit - this.running
          }
        }));
      }
    }
  }
  