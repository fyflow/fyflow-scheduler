import type {
  GaugeDescription,
  GaugeReading,
  ResourceGroup,
  ResourceGroupMetrics,
  ResourceGroupStats
} from './resourceGroup.ts';
import { normalizeKeys, type KeyedTaskLike } from './keyedRateLimitGroup.ts';

export interface KeyedConcurrentLimitGroupOptions {
  /** Stable id used in `workerGroups` / pool `groups`. Generated if omitted. */
  id?: string;
  /**
   * Which bucket a task belongs to. Defaults to `task.limitKey`:
   *
   * ```typescript
   * new KeyedConcurrentLimitGroup(4, { id: 'disk', keyFrom: t => t.payload.drive })
   * ```
   *
   * Return a list for a task that needs several keys at once - a copy from
   * C: to D: holds one unit of each disk, and is admitted only when both have
   * room. Duplicates collapse. Returning undefined, an empty string or an
   * empty list makes `addTask` throw.
   */
  keyFrom?: (task: KeyedTaskLike) => string | string[] | undefined;
  /**
   * The limit for one key, when keys should not share one number - an SSD
   * and a USB disk, say. Defaults to the constructor's `limit` for every key.
   *
   * Must return a positive integer. It is checked when a task is added, so a
   * bad answer throws from `addTask`. The value a key runs under is read when
   * the key becomes active - its first running task - and kept until it has
   * none again, so a running task never sees its own limit move. A key that
   * goes idle and comes back reads it afresh.
   */
  limitFor?: (key: string) => number;
}

interface KeyState {
  running: number;
  /** Read from `limitFor` when the key became active, and kept until idle. */
  limit: number;
}

/**
 * A concurrency limit applied independently per key: at most the key's limit
 * of tasks hold one key at once, and a saturated key never delays tasks of
 * another.
 *
 * ```typescript
 * const disk = new KeyedConcurrentLimitGroup(4, {     // 4 at once PER DISK
 *   id: 'disk',
 *   keyFrom: t => t.payload.drive,
 *   limitFor: drive => (drive === 'E:' ? 1 : 4)       // a slow USB disk
 * });
 * ```
 *
 * A hard cap, like {@link ConcurrentLimitGroup}. A key's state exists only
 * while a task holds it - there is no history to keep, so nothing needs an
 * idle timeout - which makes high-cardinality keys free once they are done.
 *
 * Not usable in `residentGroups`: a worker holds resident units across tasks,
 * and so across keys.
 */
export class KeyedConcurrentLimitGroup extends EventTarget implements ResourceGroup {
  readonly id: string;
  readonly type = 'keyed-concurrent' as const;
  readonly keyed = true;
  /** Default per-key limit, for every key `limitFor` does not answer differently. */
  readonly limit: number;

  private keyFrom: (task: KeyedTaskLike) => string | string[] | undefined;
  private limitFor?: (key: string) => number;
  private keys = new Map<string, KeyState>();
  private stats = {
    totalAcquired: 0,
    totalReleased: 0
  };

  constructor(limit: number, options: KeyedConcurrentLimitGroupOptions = {}) {
    super();
    if (!Number.isInteger(limit) || limit < 1) {
      throw new Error(`KeyedConcurrentLimitGroup limit must be a positive integer, got ${limit}`);
    }
    this.limit = limit;
    this.id = options.id || `keyed-concurrent-${Math.random().toString(36).substr(2, 9)}`;
    this.keyFrom = options.keyFrom ?? ((task: KeyedTaskLike) => task.limitKey);
    this.limitFor = options.limitFor;
  }

  /**
   * Bucket this task belongs to. The scheduler throws if this returns nothing.
   * Also where `limitFor` is checked, so a bad answer fails the `addTask`
   * rather than a dispatch already under way.
   */
  keyFor(task: unknown): string | string[] | undefined {
    const keys = normalizeKeys(this.keyFrom(task as KeyedTaskLike));
    for (const key of typeof keys === 'string' ? [keys] : keys ?? []) {
      if (!this.keys.has(key)) this._limitFor(key);
    }
    return keys;
  }

  /**
   * Room for `cost` more units under `key` (default 1).
   *
   * Without a key - how the scheduler pre-filters - whether any active key has
   * room, or no key is active yet. An inactive key always has room.
   */
  canRun(key?: string, cost: number = 1): boolean {
    if (key === undefined) {
      if (this.keys.size === 0) return true;
      for (const state of this.keys.values()) {
        if (state.running + cost <= state.limit) return true;
      }
      return false;
    }
    const state = this.keys.get(key);
    return (state?.running ?? 0) + cost <= (state?.limit ?? this._limitFor(key));
  }

  onStart(key?: string, cost: number = 1): void {
    const resolved = key ?? '';
    let state = this.keys.get(resolved);
    if (!state) {
      state = { running: 0, limit: this._limitFor(resolved) };
      this.keys.set(resolved, state);
    }
    state.running += cost;
    this.stats.totalAcquired += cost;

    if (state.running >= state.limit) {
      this.dispatchEvent(new CustomEvent('capacity-exhausted', {
        detail: { groupId: this.id, key: resolved, running: state.running, limit: state.limit }
      }));
    }
  }

  onFinish(key?: string, cost: number = 1): void {
    const resolved = key ?? '';
    const state = this.keys.get(resolved);
    if (!state) return; // Never acquired - nothing to release

    const wasAtLimit = state.running >= state.limit;
    state.running = Math.max(0, state.running - cost);
    this.stats.totalReleased += cost;
    const { running, limit } = state;
    // Dropped as soon as nothing holds it: there is no history to keep
    if (running === 0) this.keys.delete(resolved);

    if (wasAtLimit && running < limit) {
      this.dispatchEvent(new CustomEvent('slot-released', {
        detail: { groupId: this.id, key: resolved, running, limit, available: limit - running }
      }));
    }
  }

  /**
   * Aggregate view across active keys. `limit` is the default per-key limit,
   * as on {@link KeyedRateLimitGroup}; `available` and `utilization` are
   * against the sum of the active keys' own limits. Use
   * {@link KeyedConcurrentLimitGroup.getKeyMetrics} for one key.
   */
  getMetrics(): ResourceGroupMetrics & { activeKeys: number } {
    let running = 0, capacity = 0;
    for (const state of this.keys.values()) {
      running += state.running;
      capacity += state.limit;
    }
    if (this.keys.size === 0) capacity = this.limit;
    return {
      limit: this.limit,
      running,
      available: Math.max(0, capacity - running),
      utilization: capacity > 0 ? running / capacity : 0,
      activeKeys: this.keys.size
    };
  }

  /** One key's view. An inactive key reads as empty, under its `limitFor` limit. */
  getKeyMetrics(key: string): ResourceGroupMetrics {
    const state = this.keys.get(key);
    const limit = state?.limit ?? this._limitFor(key);
    const running = state?.running ?? 0;
    return {
      limit,
      running,
      available: Math.max(0, limit - running),
      utilization: running / limit
    };
  }

  /** Keys currently held by a task, busiest first. */
  getActiveKeys(): string[] {
    return [...this.keys.entries()]
      .sort((a, b) => b[1].running - a[1].running)
      .map(([key]) => key);
  }

  getStats(): ResourceGroupStats {
    return {
      totalAcquired: this.stats.totalAcquired,
      totalReleased: this.stats.totalReleased
    };
  }

  /** One `level` gauge, units held per key, against the default limit. */
  describe(): GaugeDescription {
    return {
      gauges: [{
        id: 'units',
        label: `${this.limit} per key`,
        kind: 'level',
        unit: 'units',
        limit: this.limit
      }]
    };
  }

  /**
   * One key's units, against that key's own limit - which is why a reading
   * repeats `limit`. Without a key this returns nothing, as on
   * {@link KeyedRateLimitGroup}: a sum over keys against a per-key limit says
   * nothing about whether anything may run.
   *
   * Never clamped. An inactive key reads as 0 without creating state for it.
   */
  read(key?: string): GaugeReading[] {
    if (key === undefined) return [];
    const state = this.keys.get(key);
    return [{ id: 'units', value: state?.running ?? 0, limit: state?.limit ?? this._limitFor(key) }];
  }

  private _limitFor(key: string): number {
    if (!this.limitFor) return this.limit;
    const limit = this.limitFor(key);
    if (!Number.isInteger(limit) || limit < 1) {
      throw new Error(
        `KeyedConcurrentLimitGroup "${this.id}": limitFor("${key}") returned ${limit} - it must be a positive integer`
      );
    }
    return limit;
  }
}
