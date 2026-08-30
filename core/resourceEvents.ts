/**
 * Resource event payloads.
 *
 * The scheduler is the **only** emitter of `resource.*`. A `WorkerManager` does
 * not dispatch them, even for resident holdings it acquires itself - the
 * scheduler injects a notifier and re-emits. A consumer subscribing to both the
 * scheduler and every pool, which is the natural reading since both are
 * `EventTarget`s with documented events, would otherwise count every resident
 * acquire twice and see conservation drift by exactly the resident share.
 *
 * @see AGENTS.md section 6 for the event list and the folding rules.
 */

import type { GaugeReading } from '../groups/resourceGroup.ts';

/**
 * How long a holding lasts.
 *
 * - `task-held` - acquired at dispatch, released when the task settles.
 * - `resident` - acquired when a worker is created, released at its teardown,
 *   so it outlives any one task by the idle timeout.
 */
export type ResourceLifetime = 'task-held' | 'resident';

/** What holds the units: a task, or a worker. */
export type HolderKind = 'task' | 'worker';

/**
 * Why a holding was returned.
 *
 * - `settled` - the task **attempt** that held it ended. That is broader than
 *   the name suggests: a task released and re-acquired by a retry, or requeued
 *   because its worker died, emits one `settled` release per attempt. Fold
 *   acquire/release pairs per attempt, not per task.
 * - `worker-teardown` - the worker holding it was terminated, by the idle sweep
 *   or because it died.
 * - `shutdown` - the pool or scheduler is tearing down.
 */
export type ResourceReleaseReason = 'settled' | 'worker-teardown' | 'shutdown';

/**
 * How a waiter left a blocked queue. Every exit is named, so a fold of
 * `resource.blocked` / `resource.unblocked` reconstructs one group's queue
 * exactly rather than only the set of waiting tasks.
 */
export type ResourceUnblockReason =
  /** Moved to the ready queue - the ordinary exit. */
  | 'admitted'
  /** Admitted via a different group and spliced out of this one. */
  | 'superseded'
  /**
   * Its worker pool no longer exists. The task is failed terminally rather than
   * dropped - `task.failed`, then `task.user_action` unless it is optional, and
   * its promise rejects. Retries are skipped, because the pool is gone.
   *
   * This should be unreachable: worker types are validated at `addTask` and the
   * library never mutates `workerPools`. It is modelled because it used to
   * happen silently.
   */
  | 'orphaned'
  /** The scheduler is tearing down and discarded every waiter. */
  | 'shutdown';

/** Common to every `resource.*` event. */
export interface ResourceEventDetail {
  groupId: string;
  /** The group's own `type`. Open - do not switch exhaustively on it. */
  groupType: string;
  lifetime: ResourceLifetime;
  holderKind: HolderKind;
  /** Task id for `task-held`, worker id for `resident`. */
  holderId: string;
  /** The pool key in `workerPools`, for both lifetimes. */
  workerType: string;
  /**
   * Units taken. Always 1 for task-held holdings and for any group that does
   * not weigh cost - only `ConcurrentLimitGroup` does.
   */
  cost: number;
  /** The bucket, for keyed groups. */
  key?: string;
  /** `Date.now()` at emit, matching every other timestamp in the library. */
  timestamp: number;
  /**
   * Group state **after** the operation, one entry per gauge the group
   * declares. Never clamped: an optimistic group may read above its limit.
   */
  readings: GaugeReading[];
}

/** `resource.acquired` - emitted immediately after `onStart`. */
export type ResourceAcquiredDetail = ResourceEventDetail;

/** `resource.released` - emitted immediately after `onFinish`. */
export interface ResourceReleasedDetail extends ResourceEventDetail {
  reason: ResourceReleaseReason;
}

/** `resource.blocked` - emitted on every push onto a blocked queue. */
export interface ResourceBlockedDetail extends ResourceEventDetail {
  /**
   * 0-based, in the queue it joined, **at push time**. Positions behind a
   * departing waiter shift with no event - a fold derives order from arrival,
   * which is what the queue itself does. This is a convenience for a late
   * joiner, not the source of order.
   */
  queuePosition: number;
}

/** `resource.unblocked` - emitted on every exit from a blocked queue. */
export interface ResourceUnblockedDetail extends ResourceEventDetail {
  reason: ResourceUnblockReason;
}

/**
 * One entry in {@link FyflowScheduler.getAdmissionQueue}.
 *
 * `lifetime` is not decoration: a blocked queue holds waiters of both kinds -
 * a task short of a rate limit and a task whose pool has no resident room sit
 * in the same structure.
 */
export interface AdmissionWaiter {
  /** Task id. Blocked queues hold tasks, whichever lifetime they wait on. */
  holderId: string;
  /** Its pool. */
  workerType: string;
  /** What it is waiting to acquire. */
  lifetime: ResourceLifetime;
  /**
   * Units it will need: the pool's per-worker cost for `resident`, 1 for
   * `task-held`.
   */
  cost: number;
  /**
   * 0-based **within its own queue**, and the head gates everything behind it.
   * A keyed group has one queue per key, so several waiters in the same group
   * can each be at position 0 - pair this with `key`.
   */
  position: number;
  /** The bucket, for keyed groups. */
  key?: string;
}
