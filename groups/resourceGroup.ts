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

/**
 * How a gauge is read, not what it measures.
 *
 * - `level` - units held right now out of a limit. A concurrency count, a VRAM
 *   budget, a token bucket's remaining tokens.
 * - `window` - events counted inside a rolling time window, which empties as
 *   the window slides rather than when anything is released.
 *
 * The vocabulary can grow, and a consumer must survive that:
 *
 * > **An unrecognised `kind` renders as `level`, using `value` and `limit`.**
 *
 * That rule is part of the contract. Without it, adding a third kind later
 * breaks every viewer already deployed.
 */
export type GaugeKind = 'level' | 'window';

/**
 * The static half of a gauge - declared once by {@link ResourceGroup.describe},
 * never repeated on an event.
 */
export interface GaugeSpec {
  /** Stable within the group, e.g. `'units'` or `'window-0'`. */
  id: string;
  /** Human label. */
  label: string;
  kind: GaugeKind;
  /** Display only: `'units'`, `'GB'`, `'requests'`, `'$'`. */
  unit?: string;
  limit: number;
  /** Kind `'window'` only. */
  windowMs?: number;
}

/**
 * The dynamic half - carried on every resource event, and the state *after*
 * whatever the event reports.
 *
 * `value` is not clamped: groups are optimistic, so a `level` reading may
 * exceed its limit. Clamping would make the event disagree with `getMetrics()`.
 */
export interface GaugeReading {
  /** Matches a {@link GaugeSpec.id} from the same group. */
  id: string;
  value: number;
  /** Repeated deliberately - `ConcurrentLimitGroup.limit` is mutable. */
  limit: number;
  /** Kind `'window'` only: when the window next frees a slot. */
  resetAt?: number;
}

/** What {@link ResourceGroup.describe} returns. */
export interface GaugeDescription {
  gauges: GaugeSpec[];
}

export interface ResourceGroup extends EventTarget {
  readonly id: string;
  /**
   * Open, not a closed union, so a group defined outside this library can name
   * itself. Nothing here branches on it and consumers should not either - a
   * viewer keys on {@link GaugeKind}, which is what `describe()` is for.
   */
  readonly type: string;

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

  /**
   * The gauges this group presents. Optional: a group that omits it is
   * described as a single `level` gauge derived from `getMetrics()`, so every
   * existing and third-party group is renderable with no changes.
   *
   * Implement it when one number cannot tell the truth - a rate group enforcing
   * several overlapping windows is the case this exists for.
   *
   * Schema only, and time-invariant: the gauges a group presents do not change
   * over its lifetime, which is why it is safe to read once and cache.
   */
  describe?(): GaugeDescription;

  /**
   * Current readings, one per gauge from {@link ResourceGroup.describe}, in the
   * same order. `key` is supplied for keyed groups.
   *
   * Called on the resource-event path, so keep it cheap. Optional, with the
   * same `getMetrics()`-derived default as `describe()`.
   */
  read?(key?: string): GaugeReading[];

  // Events emitted by all groups:
  // - 'slot-released': When resource becomes available
  // - 'capacity-exhausted': When limit reached
}
