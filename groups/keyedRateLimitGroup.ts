import type { ResourceGroup, ResourceGroupMetrics, ResourceGroupStats } from './resourceGroup.ts';
import type { RateWindow } from './rateLimitGroup.ts';

/** Shape the group needs from a task in order to derive its key. */
export interface KeyedTaskLike {
  id?: string;
  limitKey?: string;
  payload?: any;
}

export interface KeyedRateLimitGroupOptions {
  /** Stable id used in `workerGroups` / pool `groups`. Generated if omitted. */
  id?: string;
  /**
   * Which bucket a task belongs to. Defaults to `task.limitKey`, so tasks can
   * carry the key directly instead of the group deriving it:
   *
   * ```typescript
   * new KeyedRateLimitGroup(windows, { id: 'api', keyFrom: t => t.payload.endpoint })
   * ```
   *
   * Returning undefined or an empty string makes `addTask` throw.
   */
  keyFrom?: (task: KeyedTaskLike) => string | undefined;
  /**
   * How long a key with no running tasks is kept after its last activity.
   * Defaults to twice the largest window, which is the point past which the key
   * can no longer affect any limit. Without eviction, high-cardinality keys grow
   * without bound.
   */
  idleKeyTtlMs?: number;
}

interface KeyState {
  /** Completion timestamps per window index. */
  completed: number[][];
  /** Start timestamps of tasks currently running under this key. */
  runningStartTimes: number[];
  running: number;
  lastActivity: number;
}

/**
 * Rate limiting applied independently per key, for when different endpoints,
 * tenants or accounts each have their own quota.
 *
 * ```typescript
 * const api = new KeyedRateLimitGroup(
 *   [{ limit: 10, windowMs: 1000 }],          // 10/sec PER KEY, not in total
 *   { id: 'api', keyFrom: t => t.payload.endpoint }
 * );
 *
 * const pool = new WorkerManager(url, { groups: ['api'], inline: true });
 * new FyflowScheduler({ ApiWorker: pool }, { api });
 * ```
 *
 * Every window must have room in that key's bucket before a task runs. Tasks
 * over the limit wait in a per-key blocked queue, so a saturated key never
 * delays tasks belonging to another key.
 *
 * Like the other groups this is optimistic: a key's limit can be briefly
 * exceeded by up to `maxThreads x maxConcurrentTasks` under race conditions.
 */
export class KeyedRateLimitGroup extends EventTarget implements ResourceGroup {
  readonly id: string;
  readonly type = 'keyed-rate-limit' as const;
  readonly keyed = true;

  private windows: RateWindow[];
  private keyFrom: (task: KeyedTaskLike) => string | undefined;
  private idleKeyTtlMs: number;
  private keys = new Map<string, KeyState>();
  private stats = {
    totalAcquired: 0,
    totalReleased: 0
  };

  constructor(windows: RateWindow[], options: KeyedRateLimitGroupOptions = {}) {
    super();
    if (!windows.length) throw new Error('KeyedRateLimitGroup requires at least one window');

    this.windows = windows;
    this.id = options.id || `keyed-ratelimit-${Math.random().toString(36).substr(2, 9)}`;
    this.keyFrom = options.keyFrom ?? ((task: KeyedTaskLike) => task.limitKey);
    this.idleKeyTtlMs = options.idleKeyTtlMs ?? Math.max(...windows.map(w => w.windowMs)) * 2;
  }

  /** Bucket this task belongs to. The scheduler throws if this returns nothing. */
  keyFor(task: unknown): string | undefined {
    const key = this.keyFrom(task as KeyedTaskLike);
    return key === undefined || key === null || key === '' ? undefined : String(key);
  }

  /**
   * Whether one more task may start.
   *
   * With a key, checks that key's bucket. Without one - which is how the
   * scheduler pre-filters before walking blocked queues - reports whether any
   * known key has capacity, treating an unknown key as free.
   */
  canRun(key?: string): boolean {
    this._evictIdleKeys();

    if (key === undefined) {
      if (this.keys.size === 0) return true; // No key has been used yet
      for (const k of this.keys.keys()) {
        if (this._keyCanRun(k)) return true;
      }
      return false;
    }

    return this._keyCanRun(key);
  }

  onStart(key?: string): void {
    const state = this._stateFor(key ?? '');
    const now = Date.now();

    state.running++;
    state.runningStartTimes.push(now);
    state.lastActivity = now;
    this.stats.totalAcquired++;

    if (!this._keyCanRun(key ?? '')) {
      this.dispatchEvent(new CustomEvent('capacity-exhausted', {
        detail: { groupId: this.id, key: key ?? '' }
      }));
    }
  }

  onFinish(key?: string): void {
    const resolved = key ?? '';
    const state = this.keys.get(resolved);
    if (!state) return; // Key already evicted - nothing to release

    const now = Date.now();
    state.running = Math.max(0, state.running - 1);
    state.lastActivity = now;
    this.stats.totalReleased++;

    // Oldest running start time, FIFO, matching RateLimitGroup
    if (state.runningStartTimes.length > 0) state.runningStartTimes.shift();

    // A completed request still counts against every window it falls inside
    this.windows.forEach((_, index) => state.completed[index].push(now));

    this.dispatchEvent(new CustomEvent('slot-released', {
      detail: { groupId: this.id, key: resolved }
    }));
  }

  /**
   * Aggregate view across keys. `limit` is the per-key limit of the most
   * restrictive window, `running` is summed over every key. Use
   * {@link KeyedRateLimitGroup.getKeyMetrics} for one bucket.
   */
  getMetrics(): ResourceGroupMetrics & { activeKeys: number } {
    this._evictIdleKeys();

    const limit = this._tightestLimit();
    let running = 0;
    for (const state of this.keys.values()) running += state.running;

    const capacity = limit * Math.max(1, this.keys.size);
    return {
      limit,
      running,
      available: Math.max(0, capacity - running),
      utilization: capacity > 0 ? running / capacity : 0,
      activeKeys: this.keys.size
    };
  }

  /** Per-bucket view. Returns zeroes for a key that has not been used. */
  getKeyMetrics(key: string): ResourceGroupMetrics {
    const limit = this._tightestLimit();
    const state = this.keys.get(key);
    const running = state?.running ?? 0;

    return {
      limit,
      running,
      available: Math.max(0, limit - running),
      utilization: limit > 0 ? running / limit : 0
    };
  }

  /** Keys currently holding state, busiest first. */
  getActiveKeys(): string[] {
    this._evictIdleKeys();
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

  private _tightestLimit(): number {
    return this.windows.reduce((min, w) => Math.min(min, w.limit), this.windows[0].limit);
  }

  private _stateFor(key: string): KeyState {
    let state = this.keys.get(key);
    if (!state) {
      state = {
        completed: this.windows.map(() => []),
        runningStartTimes: [],
        running: 0,
        lastActivity: Date.now()
      };
      this.keys.set(key, state);
    }
    return state;
  }

  private _keyCanRun(key: string): boolean {
    const state = this.keys.get(key);
    if (!state) return true; // Untouched key has a full bucket

    const now = Date.now();

    for (let i = 0; i < this.windows.length; i++) {
      const window = this.windows[i];
      const windowStart = now - window.windowMs;

      // Drop timestamps that have fallen out of the window
      const completed = state.completed[i].filter(ts => ts >= windowStart);
      state.completed[i] = completed;

      const running = state.runningStartTimes.filter(ts => ts >= windowStart).length;

      if (completed.length + running >= window.limit) return false;
    }

    return true;
  }

  /**
   * Drop keys that hold no running tasks and have been quiet longer than
   * `idleKeyTtlMs`. Their windows can no longer constrain anything, so the state
   * is pure memory. Swept lazily rather than on a timer, so an idle group costs
   * nothing.
   */
  private _evictIdleKeys(): void {
    if (this.keys.size === 0) return;

    const cutoff = Date.now() - this.idleKeyTtlMs;
    for (const [key, state] of this.keys) {
      if (state.running === 0 && state.lastActivity < cutoff) {
        this.keys.delete(key);
      }
    }
  }
}
