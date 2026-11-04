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
 * `value` is not clamped: a `level` reading can exceed its limit - after the
 * limit is lowered under running tasks, or from a custom group. Clamping would
 * make the event disagree with `getMetrics()`.
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
   * Key this task belongs to, for keyed groups - or several, for a task that
   * needs more than one: a copy from C: to D: holds both disks. The task is
   * admitted only when every key has room, and holds one unit of each;
   * duplicates collapse. Returning undefined, an empty string or an empty list
   * makes `addTask` throw - a task with no key would otherwise silently bypass
   * or share a bucket.
   */
  keyFor?(task: unknown): string | string[] | undefined;

  /**
   * Capacity check (synchronous). `key` is supplied only for keyed groups; a
   * keyed group called without one reports whether ANY key has capacity, which
   * the scheduler uses as a cheap pre-filter.
   *
   * `cost` is how many units the caller intends to take, defaulting to 1. Only
   * concurrency groups weigh it - see {@link ResourceGroup.onStart}.
   *
   * `request` is what the holder asked of this group, passed through
   * unread: the scheduler never interprets it. Only a group that defines a
   * request language - `UnitGroup`'s `{ from, prefer }` - looks at it.
   */
  canRun(key?: string, cost?: number, request?: unknown): boolean;

  /**
   * Take `cost` units (default 1). Called synchronously at the moment the
   * scheduler commits to running a task, or to creating a worker that declares
   * the group in `residentGroups`.
   *
   * A return value other than `undefined` is a **grant**: what the holder
   * got, as opposed to how much - which card, which seat. The scheduler hands
   * a copy to the work (`context.grants[groupId]` for a task,
   * `workerContext.grants[groupId]` for a resident worker) and the original
   * back to `onFinish`. It must survive `structuredClone`; one that does not
   * fails the dispatch, and the units are released. A group returning
   * nothing grants nothing, which is every group that predates grants.
   */
  onStart?(key?: string, cost?: number, request?: unknown): unknown;

  /**
   * Return `cost` units (default 1). Must mirror the matching `onStart`.
   * `grant` is exactly what that `onStart` returned - the same object, not a
   * copy - or undefined if it returned nothing.
   */
  onFinish?(key?: string, cost?: number, grant?: unknown): void;

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
