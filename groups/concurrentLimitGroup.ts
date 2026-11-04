import type {
  GaugeDescription,
  GaugeReading,
  ResourceGroup,
  ResourceGroupMetrics,
  ResourceGroupStats
} from './resourceGroup.ts';

/**
 * ConcurrentLimitGroup - at most `limit` tasks hold it at once.
 *
 * A hard cap: the scheduler checks and acquires a task's groups in one
 * synchronous turn, so no more tasks hold this group than its limit, counted
 * from `onStart` to `onFinish`. Lowering `limit` under running tasks keeps
 * their units; nothing new is admitted until the count is below it.
 *
 * Example:
 * ```typescript
 * const cpuLimit = new ConcurrentLimitGroup(8, 'cpu'); // at most 8 at once
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

    /** One `level` gauge: units held out of the limit. */
    describe(): GaugeDescription {
      return {
        gauges: [{
          id: 'units',
          label: this.id,
          kind: 'level',
          unit: 'units',
          limit: this.limit
        }]
      };
    }

    /**
     * Never clamped - a limit lowered under running tasks leaves the group
     * above it, and hiding that would make the reading disagree with
     * {@link getMetrics}.
     */
    read(): GaugeReading[] {
      return [{ id: 'units', value: this.running, limit: this.limit }];
    }

    onStart(_key?: string, cost: number = 1) {
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
  