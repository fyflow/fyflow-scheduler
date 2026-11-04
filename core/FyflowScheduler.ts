import type { GaugeDescription, GaugeReading } from '../groups/resourceGroup.ts';
import type {
  AdmissionWaiter,
  ResourceEventDetail,
  ResourceLifetime,
  ResourceReleaseReason,
  ResourceUnblockReason
} from './resourceEvents.ts';
import { TaskQueue } from './taskQueue.ts';
import type { TaskMessageDetail } from './workerInterface.ts';
import { AttemptStoppedError, CancelledError, freezeShallow } from './workerInterface.ts';
import { validateGraceMs } from './workerManager.ts';

/**
 * Delimiter between a group id and a key in a blocked-queue id.
 *
 * NUL, because it cannot occur in either half. It renders as a space in most
 * tools, which is how more than one reader has come to believe the separator
 * *is* a space - anything splitting on `' '` mis-keys every keyed group
 * silently. Use {@link FyflowScheduler._splitQueueId} rather than splitting by
 * hand.
 */
const BLOCKED_QUEUE_SEPARATOR = '\u0000';

/**
 * One thing a task holds, or would: a group, a key of a keyed group, and what
 * it asked of a group that takes requests.
 */
type Hold = { id: string; group: any; key: string | undefined; request?: unknown };

/** A pending wake of every wait set of one group, rather than of one queue. */
const SWEEP_PREFIX = '\u0001';

/**
 * A unit of work handed to a worker pool.
 *
 * Tasks are independent - there is no dependency graph. A task runs as soon as
 * its worker pool has capacity and every resource group it belongs to has a
 * free slot.
 *
 * ```typescript
 * const task = new FyflowTask({
 *   id: 'resize-image-42',
 *   workerType: 'ImageWorker',     // key in the scheduler's workerPools
 *   payload: { path: '/tmp/42.png' }
 * });
 * const result = await scheduler.addTask(task, { createPromise: true });
 * ```
 */
export class FyflowTask {
    id: string;
    workerType: string;
    payload: any;
    optional: boolean;
    retryPolicy?: {maxRetries:number, backoffMs:number};
    /**
     * Retries used so far, counted against `retryPolicy.maxRetries`. Starts at
     * `attemptsSpent`, so it is 0 on a task's first attempt unless one was given.
     */
    attempts = 0;
    /**
     * Attempts made before this task was added - for example before a process
     * restart. Only reduces the retries left: a task whose budget is already
     * spent still runs once.
     */
    readonly attemptsSpent: number;
    /**
     * 1-based number of the current attempt, or of the most recent one once the
     * task has settled; 0 before its first dispatch. Always `attempts + 1` at
     * dispatch. A requeue after a worker dies is not an attempt, so the re-run
     * repeats the same number.
     */
    attemptNumber = 0;
    /**
     * `pending` -> `running` -> `done` | `failed`.
     *
     * A non-optional task that fails with no retries left ends in `user_action`,
     * signalling that something outside the scheduler has to intervene. A task
     * cancelled with `scheduler.cancel()` ends in `cancelled`; while a running
     * one is being cancelled its state stays `running` and `cancellation` is set.
     */
    state: string = "pending";
    /**
     * Set when a cancel is accepted, otherwise undefined. `forced` is decided at
     * settle: true when the worker had to be stopped. While this is set and
     * `state` is `running`, the task is being cancelled.
     */
    cancellation?: { reason?: unknown; requestedAt: number; forced: boolean };
    result?: any;
    /**
     * Error message of the most recent failed attempt. Set on every failure,
     * including one that will be retried, and cleared when the next attempt is
     * dispatched - so a task that succeeds on a retry ends with none.
     */
    error?: string;
    resolve?: Function;
    reject?: Function;
    workerGroups?: string[];
    handleRejection: boolean; // If true (default), silently handle rejections for fire-and-forget
    startTime?: number; // Timestamp when task started execution
    endTime?: number; // Timestamp when task completed
    /**
     * `Date.now()` stamped immediately before the most recent `task.*` event
     * about this task was dispatched.
     *
     * The detail of those events *is* this object, so - like every other field
     * on it - this reads as whatever the task holds when you look, not when the
     * event fired. Read it inside the listener, which runs synchronously.
     * `startTime` and `endTime` are not substitutes: `task.running` is emitted
     * from the pool's `task.started`, well after `startTime` was set.
     */
    timestamp?: number;
    /**
     * Worker-measured execution time in ms (high resolution), set on completion.
     * Unlike `endTime - startTime` this excludes time spent waiting for a worker
     * slot. Cleared when a task is requeued or retried.
     */
    executionTime?: number;
    /**
     * Bucket this task belongs to for keyed resource groups. Read by the default
     * `keyFrom` of {@link KeyedRateLimitGroup}; a group given its own `keyFrom`
     * can derive the key from the payload instead.
     */
    limitKey?: string;
    /**
     * What this task asks of a group that takes requests, per group id - for
     * a {@link UnitGroup}, which units it will accept: `{ gpu: { from: ['gpu1'] } }`.
     * Read by the group's default `requestFrom`; validated at `addTask`.
     */
    units?: Record<string, unknown>;
    _resourcesPreAllocated?: boolean; // Flag to track if resources are pre-allocated
    /**
     * Keys resolved at dispatch, per keyed group id - one or more, without
     * duplicates. Held so release uses the same buckets acquisition used, even
     * if the payload changed meanwhile.
     */
    _resourceKeys?: Record<string, string[]>;
    /**
     * What each hold's `onStart` returned, for the holds that returned
     * something: `raw` goes back to `onFinish` as it was, `copy` is what the
     * work and the events see. Per attempt - a retry or requeue is granted
     * afresh.
     */
    _grants?: Map<string, { raw: unknown; copy: unknown }>;
    /**
     * Requests resolved with the keys, per group id, for groups that take one.
     * Held for the same reason: the acquire, the wait and the release all use
     * the request the task was admitted under.
     */
    _requests?: Record<string, unknown>;
    /**
     * When it joined its blocked queue, as a scheduler-wide sequence. A release
     * a group's several wait sets compete for goes to the oldest head.
     */
    _blockedSeq?: number;
    /** `context.grants` for this attempt, built from {@link _grants}. */
    _grantView?: Record<string, unknown>;
    /**
     * Set while this attempt is being settled, so the same outcome arriving from
     * both the pool's `task.failed` event and the task promise settles once.
     * Cleared on every dispatch, so each attempt gets its own settle.
     */
    _settling?: boolean;
    /**
     * Incremented by every dispatch, and captured by that dispatch's promise
     * handlers, so an outcome from an earlier attempt can be told apart from
     * the live one. A task id is not enough: after a requeue the abandoned run
     * keeps executing while the new attempt runs elsewhere.
     */
    _dispatchToken = 0;
    /**
     * Where this task is waiting, kept current at every move, so `cancel()`
     * goes straight to it instead of searching every queue. Undefined while it
     * is in none: dispatched, settled, or in transit between two queues.
     */
    _queuedIn?: 'ready' | 'blocked' | 'backoff';
    /** Which blocked queue, while `_queuedIn` is `blocked`. */
    _blockedQueueId?: string;
    /**
     * Admitted from a blocked queue and not yet dispatched. It has waited its
     * turn, so it is not held behind the resident queue again when it creates
     * a worker - see {@link FyflowScheduler._checkAndBlockResident}.
     */
    _admittedByWake?: boolean;
    /** The pending `cancel()` of a dispatched task, resolved when it settles. */
    _cancelPending?: { promise: Promise<boolean>; resolve: (cancelled: boolean) => void };

    // Private fields for resource management
    _scheduler?: FyflowScheduler; // Reference to scheduler for descendant tracking

    /**
     * @param config.id Unique task id. Reusing an id overwrites the earlier entry.
     * @param config.workerType Key into the scheduler's `workerPools`. An unknown
     *   value makes `addTask` throw.
     * @param config.payload Passed verbatim to the worker's `run()`.
     * @param config.optional Default `false`. An optional task that fails
     *   resolves `null` instead of rejecting.
     * @param config.retryPolicy `{ maxRetries, backoffMs }`. Omitted means no retries.
     * @param config.attemptsSpent Default `0`. Attempts already made, so a task
     *   re-added after a restart continues its retry budget instead of starting
     *   a fresh one.
     * @throws TypeError if `attemptsSpent` is not a number, RangeError if it is
     *   negative, fractional, `NaN` or infinite.
     * @param config.workerGroups Resource groups for this task, *in addition* to
     *   the groups its WorkerManager declares.
     * @param config.handleRejection Default `true`. Attaches a silent catch so a
     *   fire-and-forget failure does not surface as an unhandled rejection;
     *   `task.failed` is still emitted.
     * @param config.limitKey Bucket for keyed resource groups, e.g. the endpoint
     *   or tenant this task belongs to. Required when the task belongs to a
     *   keyed group that has no custom `keyFrom`.
     * @param config.units Requests for groups that take one, per group id - for
     *   a `UnitGroup`, `{ gpu: { from: ['gpu1'] } }` pins the task to one unit.
     *   A request naming a unit the group does not have throws at `addTask`.
     */
    constructor({id, workerType, payload, optional = false, retryPolicy, attemptsSpent = 0, workerGroups = [], handleRejection = true, limitKey, units}: any) {
      if (typeof attemptsSpent !== 'number') {
        throw new TypeError(`attemptsSpent must be a number, got ${typeof attemptsSpent}`);
      }
      if (!Number.isInteger(attemptsSpent) || attemptsSpent < 0) {
        throw new RangeError(`attemptsSpent must be a non-negative integer, got ${attemptsSpent}`);
      }
      this.attemptsSpent = attemptsSpent;
      this.attempts = attemptsSpent;
      this.id = id;
      this.limitKey = limitKey;
      this.units = units;
      this.workerType = workerType;
      this.payload = payload;
      this.optional = optional;
      this.retryPolicy = retryPolicy;
      this.workerGroups = workerGroups;
      this.handleRejection = handleRejection;
    }

    /**
     * Promise for this task alone, resolving with its result and rejecting if it
     * fails. Prefer `addTask(task, { createPromise: true })`, which wires this up
     * for you. Use `onCompleteDescendants()` to also wait for spawned tasks.
     */
    onCompletePromise(): Promise<any> {
      return new Promise((resolve, reject) => {
        this.resolve = resolve;
        this.reject = reject;
        // Already over - for example cancelled by a listener before addTask()
        // got as far as creating this promise. Settle it now, or it never will.
        switch (this.state) {
          case 'done': resolve(this.result); break;
          case 'failed': resolve(null); break;
          case 'user_action': reject(new Error(this.error ?? `Task ${this.id} failed`)); break;
          case 'cancelled': reject(new CancelledError(this.id, this.cancellation?.reason)); break;
        }
      });
    }

    /**
     * Wait for this task AND every task spawned from it (children, grandchildren, ...)
     * to reach a terminal state.
     *
     * Descendants come from runtime spawning via `context.spawnTask()` - this is
     * lineage, not a scheduling dependency, and does not affect dispatch order.
     *
     * Resolves with this task's result once the whole workflow has settled. A failed
     * descendant does not reject - descendant failures surface via `task.failed`
     * events - but this task failing rejects, matching `onCompletePromise()`.
     *
     * Must be called after the task has been added to a scheduler.
     */
    onCompleteDescendants(): Promise<any> {
      return new Promise((resolve, reject) => {
        // If task isn't added to scheduler yet, can't track descendants
        if (!this._scheduler) {
          reject(new Error('Task must be added to a scheduler before tracking descendants'));
          return;
        }

        this._scheduler._trackDescendants(this, resolve, reject);
      });
    }
  }
  
  export interface FyflowSchedulerOptions {
    periodicRetryIntervalMs?: number; // Default: 50ms - retry interval for blocked tasks
    /**
     * Maximum number of terminal (done/failed/user_action) tasks to keep in
     * `scheduler.tasks`. Once exceeded, the oldest-completed tasks are evicted
     * along with their payloads, results and spawn lineage.
     *
     * Default: undefined - keep everything, which is what most workloads want
     * since task counts are bounded and completed tasks stay inspectable. Set
     * this for long-lived schedulers, where retention is otherwise unbounded.
     *
     * `stats` counts every task regardless, and in-flight tasks are never
     * evicted.
     */
    maxCompletedTasks?: number;
  }

  /** Options for {@link FyflowScheduler.cancel}. */
  export interface CancelOptions {
    /** Kept on `task.cancellation` and the `CancelledError`, and sent to the worker. */
    reason?: unknown;
    /**
     * Threaded pools: ms to wait for the worker to stop before forcing it.
     * Default: the pool's `cancelGraceMs`. `0` forces at once; `Infinity` never
     * forces. Ignored by inline pools, which cannot be forced.
     */
    graceMs?: number;
  }

  export interface AddTaskOptions {
    createPromise?: boolean; // Default: false (fire-and-forget, no promise created)
  }

  interface DescendantTracker {
    rootId: string;
    // Held by reference, not looked up by id, so that evicting the task from
    // `scheduler.tasks` cannot change how the wait settles
    rootTask: FyflowTask;
    resolve: Function;
    reject: Function;
    pendingDescendants: Set<string>;
  }

  /**
   * Runs tasks in parallel across worker pools, subject to resource groups.
   *
   * ```typescript
   * const scheduler = new FyflowScheduler(
   *   { ImageWorker: new WorkerManager(workerUrl, { maxThreads: 4, groups: ['cpu'] }) },
   *   { cpu: new ConcurrentLimitGroup(8, 'cpu') }
   * );
   * scheduler.addTask(task);              // fire-and-forget
   * await scheduler.shutdown();           // always shut down when finished
   * ```
   *
   * Emits: `task.running`, `task.retrying`, `task.completed`, `task.failed`,
   * `task.cancelling`, `task.cancelled`, `task.progress`, `task.message`,
   * `task.user_action`, `task.spawn_request`, `task.spawn_failed` and
   * `scheduler.completed`.
   */
  export class FyflowScheduler extends EventTarget {
    tasks: Map<string, FyflowTask> = new Map<string, FyflowTask>();
    // Head-indexed queues rather than arrays: Array.shift() copies the whole
    // array once it passes ~16K elements, which made draining a large queue
    // quadratic. See core/taskQueue.ts.
    readyQueuesByWorker: Map<string, TaskQueue<FyflowTask>> = new Map<string, TaskQueue<FyflowTask>>(); // Per-worker-type queues for O(1) dispatch
    blockedQueues: Map<string, TaskQueue<FyflowTask>> = new Map<string, TaskQueue<FyflowTask>>(); // Grouped by blocking group IDs for faster lookup
    workerPools: any;
    groups: Record<string, any>;
    stats = {queued:0, running:0, done:0, failed:0, cancelled:0};
    /**
     * Set once `shutdown()` has finished. An outcome arriving afterwards - an
     * inline run that a cancel could not stop, returning at last - is ignored
     * rather than settling a task nobody is waiting for any more.
     */
    private shutDown = false;
    private retryTimer: ReturnType<typeof setTimeout> | null = null;
    /**
     * Tasks sitting out a retry backoff, by id. Held outside every queue, so no
     * dispatch can start them early - they used to wait in the ready queue,
     * where any dispatch did. Still counted in `stats.queued`, so the scheduler
     * cannot report itself drained while a retry is pending.
     */
    private retryWaiting = new Map<string, { task: FyflowTask; timer?: ReturnType<typeof setTimeout> }>();
    // Active onCompleteDescendants() waits, keyed by tracker id so one task can be
    // awaited more than once. Empty unless onCompleteDescendants() is used.
    private descendantTrackers = new Map<number, DescendantTracker>();
    private nextTrackerId = 0;
    // Spawn lineage: parent task id -> ids of tasks spawned from it. Populated only
    // when workers spawn tasks; carries no scheduling meaning.
    private spawnedChildren = new Map<string, Set<string>>();
    // Terminal task ids in completion order, used only when maxCompletedTasks is
    // set. A Set preserves insertion order, so the oldest entry is its first.
    private completedTaskIds = new Set<string>();
    private options: FyflowSchedulerOptions;

    // Track ALL event listeners for cleanup
    private allListeners = new Map<any, {event: string; listener: Function}[]>();

    /**
     * Subscribers to `resource.*`, per event name.
     *
     * These are high-frequency events - one acquire and one release per task
     * per group - so nothing is built and no `read()` is called while nobody is
     * listening. A Set rather than a counter on purpose: `removeEventListener`
     * with a function that was never added must not be able to decrement the
     * count to zero and silently stop delivery to a listener that is still
     * there. Over-counting (`{ once: true }`, or the same listener added twice)
     * only costs work nobody reads, which is the safe direction.
     */
    private resourceListeners = new Map<string, Set<unknown>>();

    /**
     * Subscribers to `task.message`, with the same Set semantics. While there
     * are none, every pool is told so and its workers send nothing at all.
     */
    private messageListeners = new Set<unknown>();

    constructor(workerPools: any, groups: Record<string, any> = {}, options: FyflowSchedulerOptions = {}) {
      super();
      this.workerPools = workerPools;
      this.groups = groups;
      this.options = {
        periodicRetryIntervalMs: 50, // Default to 50ms - optimal balance of speed and efficiency
        ...options
      };

      // Initialize per-worker-type ready queues
      for (const workerType of Object.keys(workerPools)) {
        this.readyQueuesByWorker.set(workerType, new TaskQueue<FyflowTask>());
      }

      // Pools are constructed with group ids; only the scheduler owns the
      // objects. Binding here also validates resident costs against their
      // group's limit, so an impossible pool throws at construction.
      // The notifier makes the scheduler the SOLE emitter of `resource.*`. A
      // pool dispatching its own would be double-counted by any consumer
      // listening to both, exactly as forwarding task.completed once was.
      for (const [workerType, pool] of Object.entries(workerPools) as [string, any][]) {
        pool?._bindResidentGroups?.(
          this.groups,
          (notice: any) => this._onResidentNotice(workerType, notice)
        );
        // Injected rather than listened for, for the same reason: task.message
        // fires here only, and a listener of ours would count as a subscriber
        // and hold every worker's gate open
        pool?._bindTaskMessages?.((detail: Omit<TaskMessageDetail, 'attemptNumber'>) => this._onTaskMessage(detail));
      }

      this._setupWorkerPoolListeners();
      this._setupGroupEventListeners();
    }

    /** True when no existing worker in the pool could take another task. */
    private _needsNewWorker(pool: any): boolean {
      return !pool.threads?.some((t: any) => t.canAcceptTask && t.canAcceptTask());
    }

    /** Resident groups a pool holds per worker, as {@link _groupsForTask} entries. */
    private _residentGroupsForPool(pool: any): Array<{ id: string; group: any; cost: number; request?: unknown }> {
      const declared: Record<string, number> = pool?.residentGroups || {};
      const requests: Record<string, unknown> = pool?.residentRequests || {};
      return Object.entries(declared)
        .map(([id, cost]) => ({ id, group: this.groups[id], cost, request: requests[id] }))
        .filter(entry => entry.group);
    }

    /**
     * Whether a resident waiter of `entry` would arrive behind someone. Per
     * wait set for a group that has them: a pool pinned to a busy unit must
     * not hold up a pool that wants a free one, so only a waiter whose set
     * overlaps this one's counts. Any waiter at all, for every other group.
     */
    private _residentQueueBehind(entry: { id: string; group: any; request?: unknown }): boolean {
      if (!FyflowScheduler._hasWaitSets(entry.group)) {
        return (this.blockedQueues.get(entry.id)?.length ?? 0) > 0;
      }
      const own = FyflowScheduler._waitSetOf(entry.group, entry.request);
      for (const queueId of this._queuesOfGroup(entry.id)) {
        if ((this.blockedQueues.get(queueId)?.length ?? 0) === 0) continue;
        const { set } = this._splitQueueId(queueId);
        if (entry.group.waitSetsOverlap?.(own, set) ?? own === set) return true;
      }
      return false;
    }

    /**
     * Block `task` when dispatching it would mean creating a worker its pool has
     * no resident capacity for.
     *
     * Only consulted on the create-a-worker path: a task landing on a worker
     * that already exists costs nothing extra, because the cost is per worker.
     */
    private _checkAndBlockResident(task: FyflowTask, pool: any): true | 'blocked' {
      const entries = this._residentGroupsForPool(pool);
      if (entries.length === 0) return true;

      // Head-of-line on arrival, too: a task that would create a worker queues
      // behind anyone already waiting on one of its resident groups, even when
      // its own cost fits. Admitting it would let a stream of cheap arrivals
      // keep an expensive waiter out forever - which used to happen, because
      // only the retry path applied the rule. A task admitted from a queue has
      // already waited its turn and is not sent back.
      const fits = pool.canHoldAnotherWorker?.() !== false;
      const behind = task._admittedByWake
        ? undefined
        : entries.find(entry => this._residentQueueBehind(entry));
      if (fits && !behind) return true;

      // Queue against the first group without room, or the queue it must wait
      // behind. That group's release wakes the task.
      const short = (fits ? behind : undefined)
        ?? entries.find(({ group, cost, request }) => !group.canRun(undefined, cost, request))
        ?? entries[0];
      const queueId = this._blockedQueueId(short.id, FyflowScheduler._waitSetOf(short.group, short.request));
      const queue = this._blockedQueue(queueId);
      // It has left the ready queue: count it out of `queued` before
      // resource.blocked fires, so a listener sees consistent stats
      this.stats.queued = Math.max(0, this.stats.queued - 1);
      task._queuedIn = 'blocked';
      task._blockedQueueId = queueId;
      if (!queue.includes(task)) {
        queue.push(task);
        task._blockedSeq = ++this.blockedSeq;
        this._noteResidentWait(task);
        if (this._resourceObserved) {
          this._emitResource('resource.blocked', short.group, {
            groupId: short.id,
            ...this._blockedHoldingFor(task, pool, short.id),
            holderKind: 'task',
            holderId: task.id,
            workerType: task.workerType,
            queuePosition: queue.length - 1
          });
        }
      }
      return 'blocked';
    }

    /** Every resource group a task must acquire: its own plus its pool's. */
    private _groupsForTask(task: FyflowTask, pool: any): Array<{ id: string; group: any }> {
      const ids = [...new Set([...(task.workerGroups || []), ...(pool?.groups || [])])];
      return ids
        .map(id => ({ id, group: this.groups[id] }))
        .filter(entry => entry.group);
    }

    /**
     * Blocked queues are per group, and per (group, key) for keyed groups, so a
     * saturated key never stalls tasks belonging to a different one.
     */
    private _blockedQueueId(groupId: string, key?: string): string {
      return key === undefined
        ? groupId
        : `${groupId}${BLOCKED_QUEUE_SEPARATOR}${key}`;
    }

    /**
     * Inverse of {@link _blockedQueueId}. The delimiter is NUL - splitting on a
     * space instead mis-keys every keyed group without erroring.
     */
    private _splitQueueId(queueId: string): { groupId: string; key?: string; set?: string; suffix?: string } {
      const separator = queueId.indexOf(BLOCKED_QUEUE_SEPARATOR);
      if (separator === -1) return { groupId: queueId };
      const groupId = queueId.slice(0, separator);
      const suffix = queueId.slice(separator + 1);
      // A keyed group's queues are per key; any other group's are per wait
      // set, which is never a key and never reaches an event as one
      return this.groups[groupId]?.keyed
        ? { groupId, key: suffix, suffix }
        : { groupId, set: suffix, suffix };
    }

    /** A group that queues its waiters per candidate set - `UnitGroup`. */
    private static _hasWaitSets(group: any): boolean {
      return typeof group?.waitSetFor === 'function';
    }

    /** The wait set a request queues in; undefined is the group's plain queue. */
    private static _waitSetOf(group: any, request: unknown): string | undefined {
      return request === undefined ? undefined : group?.waitSetFor?.(request);
    }

    /** The plain queue of a group and every per-key or per-set queue it has. */
    private _queuesOfGroup(groupId: string): string[] {
      const ids = [groupId];
      for (const suffix of this.blockedKeys.get(groupId) ?? []) ids.push(this._blockedQueueId(groupId, suffix));
      return ids;
    }

    /** Arrival order across every blocked queue - see {@link FyflowTask._blockedSeq}. */
    private blockedSeq = 0;

    /**
     * Tasks waiting in a resident queue - for a worker their pool cannot yet
     * create - per worker type, oldest first, with the `_blockedSeq` of that
     * wait. Pruned lazily: an entry is live only while its task is still
     * blocked with that same seq, since every push takes a new one. Kept so a
     * free slot finds its pool's waiters without walking queues that may hold
     * thousands of other pools' tasks.
     */
    private _residentWaits = new Map<string, Map<FyflowTask, number>>();
    private _admittingResidentRoom = false;

    private _noteResidentWait(task: FyflowTask): void {
      let waits = this._residentWaits.get(task.workerType);
      if (!waits) this._residentWaits.set(task.workerType, waits = new Map());
      // Re-inserted, so the map stays in seq order
      waits.delete(task);
      waits.set(task, task._blockedSeq!);
    }

    /**
     * Admit waiters for a new worker whose pool has a free slot on a worker it
     * already has. Run at the end of every dispatch loop, so after a settle.
     *
     * Such a task needs no resident capacity any more, so it leaves the
     * resident queue from wherever it is. Head-of-line orders waiters for
     * resident capacity, and this one takes none, so it overtakes nobody. It
     * used to wait for a resident release instead: with `idleTimeout: 0` the
     * pool's workers keep their units for good, so one that held the only unit
     * stalled its own pool's next task forever.
     */
    private _admitResidentWaitersWithRoom(): void {
      if (this._residentWaits.size === 0 || this._admittingResidentRoom) return;
      if (!this._hasBlockedTasks()) {
        // Every entry is stale; drop them, and the tasks they hold on to
        this._residentWaits.clear();
        return;
      }
      this._admittingResidentRoom = true;
      try {
        for (const [workerType, waits] of this._residentWaits) {
          const pool = this.workerPools[workerType];
          while (waits.size > 0 && pool && !this._needsNewWorker(pool)) {
            const [task, seq] = waits.entries().next().value!;
            waits.delete(task);
            if (task._queuedIn !== 'blocked' || task._blockedSeq !== seq) continue;
            this._admitOutOfTurn(task, pool);
          }
          if (waits.size === 0) this._residentWaits.delete(workerType);
        }
      } finally {
        this._admittingResidentRoom = false;
      }
    }

    /** Take a resident waiter out of its queue, wherever it is, and admit or move it. */
    private _admitOutOfTurn(task: FyflowTask, pool: any): void {
      const queueId = task._blockedQueueId!;
      const queue = this.blockedQueues.get(queueId);
      if (!queue) return;
      const { groupId } = this._splitQueueId(queueId);
      const group = this.groups[groupId];
      // Another of its holds full: wait there, where a release can admit it
      const refusing = this._holdsFor(task, pool).find(hold => !hold.group.canRun(hold.key, 1, hold.request));
      if (refusing) {
        this._migrate(task, pool, queue, groupId, undefined, refusing);
      } else {
        // Not tombstoned: unlike a cancelled task it may come back to this
        // queue, where a tombstone would hide it
        if (queue.at(0) === task) queue.shift();
        else queue.remove(task);
        this._admitWaiter(task, pool, group, groupId, undefined);
      }
      this._dropBlockedQueueIfEmpty(queueId);
    }

    /**
     * Resolve the bucket for every keyed group this task belongs to.
     *
     * @throws if a keyed group cannot derive a key. A keyless task would
     *   otherwise silently share a bucket with every other keyless task, or skip
     *   the limit entirely - both discovered only once something is throttled.
     */
    private _resolveResourceKeys(task: FyflowTask, pool: any): Record<string, string[]> {
      const keys: Record<string, string[]> = {};
      let requests: Record<string, unknown> | undefined;

      for (const { id, group } of this._groupsForTask(task, pool)) {
        // A group that takes requests resolves - and validates - the task's
        // here, so a request naming a unit that does not exist throws at
        // addTask rather than waiting forever
        if (typeof group.requestFor === 'function') {
          const request = group.requestFor(task, 1);
          if (request !== undefined) (requests ??= {})[id] = request;
        }
        if (!group.keyed) continue;

        // One key, or several: a copy from C: to D: holds both disks. Empty
        // entries are dropped and duplicates collapse, so a C: -> C: copy
        // holds one unit, not two; nothing left is a missing key.
        const raw = group.keyFor?.(task);
        const list = Array.isArray(raw) ? raw : [raw];
        const resolved = [...new Set(
          list.filter(k => k !== undefined && k !== null && k !== '').map(k => String(k))
        )];
        if (resolved.length === 0) {
          throw new Error(
            `Missing limit key for group "${id}" on task "${task.id}". ` +
            `Set task.limitKey, or give the group a keyFrom that derives one.`
          );
        }
        keys[id] = resolved;
      }

      task._requests = requests;
      return keys;
    }

    /**
     * Every (group, key) a task holds or would hold: one entry per key of a
     * keyed group, one with no key for any other. The unit admission and
     * release work in - a task naming two disks is two holds, checked
     * together and acquired together.
     */
    private _holdsFor(
      task: FyflowTask, pool: any, keys: Record<string, string[]> = task._resourceKeys ?? {}
    ): Hold[] {
      const holds: Hold[] = [];
      const requests = task._requests;
      for (const entry of this._groupsForTask(task, pool)) {
        // A keyless hold is the entry itself, `key` absent: this runs for every
        // waiter a wake examines, and a copy per group showed up as heap
        if (!entry.group.keyed) {
          const request = requests?.[entry.id];
          holds.push(request === undefined
            ? entry as Hold
            : { id: entry.id, group: entry.group, key: undefined, request });
          continue;
        }
        for (const key of keys[entry.id] ?? []) holds.push({ id: entry.id, group: entry.group, key });
      }
      return holds;
    }

    /** Where a hold's grant is kept on the task: per (group, key). */
    private static _holdId(groupId: string, key: string | undefined): string {
      return key === undefined ? groupId : `${groupId}${BLOCKED_QUEUE_SEPARATOR}${key}`;
    }

    /**
     * Copy this attempt's grants and build `context.grants` from them: per
     * group, the grant itself - or, for a keyed group, `{ [key]: grant }`, so
     * the shape depends on the group and never on how many keys a task has.
     *
     * Copied once, here, on inline pools as well: what the work sees is the
     * same whether or not a thread boundary is crossed, the rule `emit()`
     * follows. Frozen, one level down, because the copy is shared by the work
     * and the events.
     *
     * @returns the error for a grant that cannot be copied, which fails the
     *   attempt. Its units are still released: the original is kept.
     */
    private _copyGrants(task: FyflowTask, holds: Hold[]): Error | undefined {
      const view: Record<string, unknown> = {};
      let error: Error | undefined;
      for (const { id, group, key } of holds) {
        const grant = task._grants!.get(FyflowScheduler._holdId(id, key));
        if (!grant) continue;
        try {
          grant.copy = freezeShallow(structuredClone(grant.raw));
        } catch (e: any) {
          error ??= new Error(
            `Resource group "${id}" granted a value that cannot be copied with structuredClone: ${e?.message ?? e}`
          );
          continue;
        }
        if (group.keyed) ((view[id] ??= {}) as Record<string, unknown>)[key!] = grant.copy;
        else view[id] = grant.copy;
      }
      for (const value of Object.values(view)) freezeShallow(value);
      task._grantView = Object.freeze(view);
      return error;
    }

    // Any task waiting on a resource group. Blocked tasks are subtracted from
    // stats.queued when they leave the ready queue, so they are invisible to the
    // stats alone.
    //
    // O(1): every blocked queue keeps one shared live count. Visiting each queue
    // instead cost O(keys) per call, and a cancel asks on every call - so
    // cancelling every task of a keyed group with thousands of keys, nothing
    // running, took seconds.
    private _hasBlockedTasks(): boolean {
      return this.blockedLive.live > 0;
    }

    /** Tasks in every blocked queue together, kept by the queues themselves. */
    private blockedLive = { live: 0 };

    private _newBlockedQueue(): TaskQueue<FyflowTask> {
      return new TaskQueue<FyflowTask>({ trackMembership: true, counter: this.blockedLive });
    }

    /**
     * Keys with a blocked queue, per keyed group - or wait sets, for a group
     * that has them (see {@link _splitQueueId}). Kept in step with
     * `blockedQueues` by the two methods below, so finding a keyed group's
     * waiting keys is a lookup rather than a scan of every queue.
     */
    private blockedKeys = new Map<string, Set<string>>();

    /**
     * Queues being iterated right now, with their nesting depth. Such a queue
     * must not be deleted: the loop holds the object, a listener it calls can
     * empty the queue and a later push recreate it, and the loop would then
     * put tasks back into the detached object, where nothing ever finds them.
     */
    private blockedInUse = new Map<string, number>();

    /** The queue for `queueId`, created and indexed if it does not exist. */
    private _blockedQueue(queueId: string): TaskQueue<FyflowTask> {
      let queue = this.blockedQueues.get(queueId);
      if (!queue) {
        queue = this._newBlockedQueue();
        this.blockedQueues.set(queueId, queue);
        const { groupId, suffix } = this._splitQueueId(queueId);
        if (suffix !== undefined) {
          let keys = this.blockedKeys.get(groupId);
          if (!keys) this.blockedKeys.set(groupId, keys = new Set());
          keys.add(suffix);
        }
      }
      return queue;
    }

    /**
     * Delete `queueId`'s queue if it is empty and nothing is iterating it.
     *
     * Blocked queues exist only while they hold a waiter. They used to live
     * forever, so a keyed group kept one per key that had ever blocked - 1,000
     * empty queues after a 1,000-key run - and every heartbeat walked them all.
     */
    private _dropBlockedQueueIfEmpty(queueId: string | undefined): void {
      if (queueId === undefined || this.blockedInUse.has(queueId)) return;
      const queue = this.blockedQueues.get(queueId);
      if (!queue || queue.length > 0) return;
      this.blockedQueues.delete(queueId);
      const { groupId, suffix } = this._splitQueueId(queueId);
      if (suffix !== undefined) {
        const keys = this.blockedKeys.get(groupId);
        keys?.delete(suffix);
        if (keys?.size === 0) this.blockedKeys.delete(groupId);
      }
    }

    _checkCompletion() {
      const hasBlockedTasks = this._hasBlockedTasks();

      // Any task having ended counts, not only a success: a run whose tasks all
      // failed, or were all cancelled, never reported draining
      const ended = this.stats.done + this.stats.failed + this.stats.cancelled;
      if (this.stats.queued === 0 && this.stats.running === 0 && !hasBlockedTasks && ended > 0) {
        this._clearPeriodicRetry(); // Stop retries when all tasks are done
        // Copied, not `this.stats` itself: this fires on every drain, so a
        // consumer keeping two of them would otherwise hold one live object
        // twice and see both read as the latest counts
        this.dispatchEvent(new CustomEvent('scheduler.completed', {
          detail: { ...this.stats, timestamp: Date.now() }
        }));
      } else if (hasBlockedTasks && this.stats.queued === 0 && this.stats.running === 0) {
        // Nothing is running to release resources on completion, so the periodic
        // retry is the only thing that can ever unblock these tasks - for example
        // a rate limit window expiring. Make sure it is still armed.
        this._schedulePeriodicRetry();
      } else if (this.stats.queued > 0 && this.stats.running === 0) {
        // Tasks are queued but none running - retry so they get picked up.
        //
        // This branch used to also scan every task ever created looking for a
        // 'dispatched' state to recover. That state stopped being assigned in
        // dd57083, so the scan could only ever produce an empty array - while
        // still costing O(tasks) on every call (12ms at a million retained
        // tasks). Tasks stuck mid-flight are covered by task.requeue_required
        // and the periodic retry instead.
        this._retryBlockedTasks();
        this._schedulePeriodicRetry();
      }
    }
  
    /**
     * Queue a task and start dispatching.
     *
     * Fire-and-forget by default: returns `undefined` unless
     * `{ createPromise: true }` is passed, in which case it returns a promise
     * that resolves with the task's result or rejects if it fails.
     *
     * @throws if `task.workerType` is not a key in the scheduler's `workerPools`.
     */
    addTask(task: FyflowTask, options?: AddTaskOptions): Promise<any> | void {
      // Validate before registering anything, so a rejected task leaves no orphan
      // entry behind in the task map
      const workerQueue = this.readyQueuesByWorker.get(task.workerType);
      if (!workerQueue) {
        throw new Error(`Unknown worker type: ${task.workerType}`);
      }
      FyflowScheduler._assertNotCancelled(task);
      // Throws on a keyed group with no derivable key, before anything is registered
      this._resolveResourceKeys(task, this.workerPools[task.workerType]);

      // Set scheduler reference for descendant tracking
      task._scheduler = this;
      this.tasks.set(task.id, task);
      workerQueue.push(task);
      task._queuedIn = 'ready';

      this.stats.queued++;
      this._dispatchLoop();

      // Only create promise if explicitly requested
      if (options?.createPromise) {
        const promise = task.onCompletePromise();

        // For fire-and-forget tasks (default), silently handle rejections
        // The task.failed event is still dispatched for monitoring
        if (task.handleRejection) {
          promise.catch(() => {
            // Silent catch - rejection is already handled by task.failed event
          });
        }

        return promise;
      }
    }

    /**
     * A cancelled task object cannot be queued again: it may still sit in a
     * queue as a tombstone, where re-queueing the same object would either be
     * skipped or revive the stale entry. Re-running cancelled work means a new
     * `FyflowTask`.
     */
    private static _assertNotCancelled(task: FyflowTask) {
      if (task.state === 'cancelled') {
        throw new Error(`Task "${task.id}" was cancelled - create a new FyflowTask to run it again`);
      }
    }

    /**
     * Queue many tasks with a single dispatch pass - use this for bulk additions
     * (>1000 tasks) instead of calling `addTask` in a loop, which would run the
     * dispatch loop once per task.
     *
     * Like `addTask`, returns nothing unless `{ createPromise: true }` is passed;
     * with it, returns one promise per task in the same order:
     *
     * ```typescript
     * const results = await Promise.all(
     *   scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]
     * );
     * ```
     *
     * @throws if any task's `workerType` is not a key in the scheduler's
     *   `workerPools`. The batch is validated up front, so a rejected call
     *   queues nothing at all.
     */
    addTasks(tasks: FyflowTask[], options?: AddTaskOptions): Promise<any>[] | void {
      // Validate the whole batch before touching any state. Previously an
      // unknown worker type was skipped while still counting towards
      // stats.queued, leaving a task that could never dispatch and a queued
      // count that never returned to 0 - so scheduler.completed never fired
      // again. Rejecting up front also keeps the call atomic: either every task
      // is queued or none is.
      for (const task of tasks) {
        if (!this.readyQueuesByWorker.has(task.workerType)) {
          throw new Error(`Unknown worker type: ${task.workerType}`);
        }
        FyflowScheduler._assertNotCancelled(task);
        this._resolveResourceKeys(task, this.workerPools[task.workerType]);
      }

      const promises: Promise<any>[] = [];

      for (const task of tasks) {
        // Set scheduler reference for descendant tracking
        task._scheduler = this;

        this.tasks.set(task.id, task);

        // Queue task immediately (no dependency tracking)
        this.readyQueuesByWorker.get(task.workerType)!.push(task);
        task._queuedIn = 'ready';
        this.stats.queued++;

        // Only create promise if explicitly requested
        if (options?.createPromise) {
          const promise = task.onCompletePromise();

          // For fire-and-forget tasks (default), silently handle rejections
          // The task.failed event is still dispatched for monitoring
          if (task.handleRejection) {
            promise.catch(() => {
              // Silent catch - rejection is already handled by task.failed event
            });
          }

          promises.push(promise);
        }
        // Note: No _dispatchLoop() call here - batched at the end
      }

      // Single dispatch call for all tasks - prevents overwhelming worker_threads
      this._dispatchLoop();

      if (options?.createPromise) {
        return promises;
      }
    }
  
    // _onParentComplete(parent: FyflowTask) {
    //   // No dependency tracking - just trigger dispatch loop
    //   this._dispatchLoop();
    // }


    _dispatchLoop() {
      let tasksProcessed = 0;
      let blockedSkips = 0; // Bounds the scanning past blocked tasks
      const maxTasksPerLoop = 1000; // Prevent excessive iterations in extreme contention


      // Process per-worker-type queues for O(1) efficiency
      for (const [workerType, queue] of this.readyQueuesByWorker) {
        if (queue.length === 0) continue;

        const pool = this.workerPools[workerType];
        if (!pool) continue;

        // Process multiple tasks for this worker type until we hit limits
        while (queue.length > 0 && tasksProcessed < maxTasksPerLoop) {
          // A full pool is the common way out of this loop, so it is checked
          // without touching the queue: taking the head off and putting it back
          // would copy a large ready queue twice on every pass.
          if (!this._poolHasRoom(pool)) {
            // Threads may become available when other tasks complete or idle
            // timeout creates capacity
            this._schedulePeriodicRetry();
            break;
          }

          // Taken out of the ready queue BEFORE the checks below, which can
          // emit resource.blocked. It used to be peeked and shifted afterwards,
          // so a listener re-entering the scheduler (addTask runs this loop
          // again) left the outer shift() removing a different task - one that
          // then ran nowhere and never settled.
          const task = queue.shift()!;
          task._queuedIn = undefined;

          // Check if we can dispatch this task
          const canDispatchResult = this._canDispatchTask(task, pool);
          if (canDispatchResult === true) {
            // Dispatch the task (synchronous dispatch, async execution)
            this._dispatchTask(task, pool);
            tasksProcessed++;
          } else if (canDispatchResult === 'blocked' || canDispatchResult === 'blocked-keyed') {
            // Moved to a blocked queue; the blocking check has already taken it
            // out of stats.queued, before telling anyone.
            //
            // Keep scanning, keyed or not. The next task may need a group, or a
            // key, with room; and every waiter belongs in a blocked queue, not
            // stranded in the ready queue behind a blocked head - where it was
            // invisible to getAdmissionQueue() and resource.blocked, and woken
            // only by the dispatch loop happening to reach it.
            if (blockedSkips < maxTasksPerLoop) {
              blockedSkips++;
              continue;
            }

            // Stop processing this worker type for now
            break;
          } else {
            // canDispatchResult === false: no room after all. Unreachable while
            // _poolHasRoom() mirrors _canDispatchTask(); kept so a divergence
            // between them delays the task instead of losing it. Safe to put
            // back, as this path emits nothing.
            queue.unshift(task);
            task._queuedIn = 'ready';
            this._schedulePeriodicRetry();
            break;
          }
        }
      }

      this._admitResidentWaitersWithRoom();

      // Clear periodic retry if no more tasks queued or blocked
      const hasReadyTasks = Array.from(this.readyQueuesByWorker.values()).some(q => q.length > 0);
      const hasBlockedTasks = this._hasBlockedTasks();
      const hasQueuedInStats = this.stats.queued > 0;
      if (!hasReadyTasks && !hasBlockedTasks && !hasQueuedInStats) {
        this._clearPeriodicRetry();
      }
    }


    // Helper: Check resource availability and block if needed
    _checkAndBlockResources(task: FyflowTask, pool: any): true | 'blocked' | 'blocked-keyed' {
      const taskGroupIds = task.workerGroups || [];
      const workerGroupIds = pool.groups || [];

      // Fast path: no groups configured
      if (taskGroupIds.length === 0 && workerGroupIds.length === 0) {
        return true;
      }

      const keys = task._resourceKeys ?? this._resolveResourceKeys(task, pool);
      task._resourceKeys = keys;

      // Every hold must have room - every group, and every key of a keyed
      // one. Checked here and acquired by _dispatchTask in the same turn, so
      // all-or-nothing needs no locking, and with no partial hold there is
      // nothing to deadlock on.
      for (const { id, group, key, request } of this._holdsFor(task, pool, keys)) {
        if (!group.canRun(key, 1, request)) {
          const blockedOnKey = Boolean(group.keyed);
          // Blocked on this group - and on this key, when the group is keyed,
          // or in the queue of its candidate set, for a group that has them
          const queueId = this._blockedQueueId(id, key ?? FyflowScheduler._waitSetOf(group, request));
          const blockedQueue = this._blockedQueue(queueId);
          // It has left the ready queue: count it out of `queued` before
          // resource.blocked fires, so a listener sees consistent stats
          this.stats.queued = Math.max(0, this.stats.queued - 1);
          // Only a push is an event. Re-reaching a queue the task is already in
          // changes no membership, and emitting there would leave the fold one
          // blocked ahead of its unblocked permanently.
          task._queuedIn = 'blocked';
          task._blockedQueueId = queueId;
          if (!blockedQueue.includes(task)) {
            blockedQueue.push(task);
            task._blockedSeq = ++this.blockedSeq;
            if (this._resourceObserved) {
              this._emitResource('resource.blocked', group, {
                groupId: id,
                ...this._blockedHoldingFor(task, pool, id),
                holderKind: 'task',
                holderId: task.id,
                workerType: task.workerType,
                key,
                queuePosition: blockedQueue.length - 1
              });
            }
          }
          this._schedulePeriodicRetry();
          // A keyed block says nothing about the next task in the ready queue,
          // which may belong to a key with a free bucket
          return blockedOnKey ? 'blocked-keyed' : 'blocked';
        }
      }
      return true; // Resources available
    }

    /**
     * Whether the pool could take a task now: an existing worker with a free
     * slot, or room to create one. Exactly the condition under which
     * {@link _canDispatchTask} does not return `false`, checked without
     * touching a queue or emitting anything.
     */
    private _poolHasRoom(pool: any): boolean {
      if (pool._capacityRaceBackoff) return false;
      if (pool.threads.some((t: any) => t.canAcceptTask && t.canAcceptTask())) return true;
      return pool.threads.length < pool.maxThreads;
    }

    // NEW: Non-optimistic mode helper methods
    _canDispatchTask(task: FyflowTask, pool: any): boolean | 'blocked' | 'blocked-keyed' {
      // See the capacity race in _dispatchTask
      if (pool._capacityRaceBackoff) return false;
      // Check if we have an immediately available thread (no async creation)
      const availableThreads = pool.threads.filter((t: any) =>
        t.canAcceptTask && t.canAcceptTask()
      ).length;

      if (availableThreads > 0) {
        // Have available thread - check resources
        const resourceResult = this._checkAndBlockResources(task, pool);
        if (resourceResult !== true) {
          return resourceResult;
        }
        return true;
      }

      // No available threads - can we create one?
      // A starting worker is in `threads` already (same check as enqueueNoQueue)
      const canCreateThread = pool.threads.length < pool.maxThreads;
      if (canCreateThread) {
        // Check resource groups before creating thread
        const resourceResult = this._checkAndBlockResources(task, pool);
        if (resourceResult !== true) {
          return resourceResult;
        }
        // This dispatch creates a worker, so its resident cost has to fit too
        const residentResult = this._checkAndBlockResident(task, pool);
        if (residentResult !== true) {
          return residentResult;
        }
        // Resources available - can create thread and dispatch
        return true;
      }

      // No threads available and can't create more - task stays in ready queue
      return false;
    }

    _dispatchTask(task: FyflowTask, pool: any) {
      // Every transition first, then any event. resource.acquired below can
      // reach a listener that calls cancel(), which must find the task where it
      // really is: running, counted in `running`, out of every queue. It used to
      // fire while the task was still `pending` and counted in `queued`, where a
      // cancel had no correct outcome.
      task._queuedIn = undefined;
      // A fresh attempt may be settled again
      task._settling = false;
      task._admittedByWake = undefined;
      const token = ++task._dispatchToken;
      // A requeue repeats the number, because it leaves `attempts` alone
      task.attemptNumber = task.attempts + 1;
      // The previous attempt's failure is not this one's
      task.error = undefined;
      task.state = 'running';
      task.startTime = Date.now();
      this.stats.queued = Math.max(0, this.stats.queued - 1);
      this.stats.running++;

      // Acquire resources immediately (thread will be created if needed)
      const keys = task._resourceKeys ?? this._resolveResourceKeys(task, pool);
      task._resourceKeys = keys;
      task._resourcesPreAllocated = true;
      const holds = this._holdsFor(task, pool, keys);

      // Every group first, then the events. A resource.acquired listener can
      // re-enter the scheduler - addTask dispatches synchronously - and with
      // the events in between, a task dispatched from there passed canRun on a
      // group this one had checked but not yet taken, and both took it: a
      // concurrency group held more than its limit.
      for (const hold of holds) {
        const grant = hold.group.onStart(hold.key, 1, hold.request);
        if (grant !== undefined) {
          (task._grants ??= new Map()).set(FyflowScheduler._holdId(hold.id, hold.key), { raw: grant, copy: undefined });
        }
      }
      // Copied before any event, so the events carry it; a grant that cannot
      // be copied fails the attempt below, its units released by the settle
      const grantError = task._grants ? this._copyGrants(task, holds) : undefined;
      holds.forEach(({ id, group, key }) => {
        // After the mutation, so `readings` are the state a fold should land on
        if (this._resourceObserved) {
          const grant = task._grants?.get(FyflowScheduler._holdId(id, key));
          this._emitResource('resource.acquired', group, {
            groupId: id,
            lifetime: 'task-held',
            holderKind: 'task',
            holderId: task.id,
            workerType: task.workerType,
            cost: 1, // _dispatchTask passes no cost, so onStart defaults to 1
            key,
            ...(grant?.copy !== undefined ? { grant: grant.copy } : {})
          });
        }
      });

      // Cancelled by a resource.acquired listener: it never reaches the pool,
      // and settles as cancelled, releasing what it just acquired
      if (task.cancellation) {
        this._settleTask(task, pool, { ok: false, error: new CancelledError(task.id) }, token);
        return;
      }
      if (grantError) {
        this._settleTask(task, pool, { ok: false, error: grantError }, token);
        return;
      }

      // Get/create thread and execute task
      pool.enqueueNoQueue(task.id, task.payload, task._grantView)
        .then((result: any) => {
          this._onTaskComplete(task, result, pool, token);
        })
        .catch((error: any) => {
          // A co-tenant of a task whose worker was force-stopped. It did nothing
          // wrong, so it is requeued whatever requeueFailedTasks says. Being
          // cancelled itself, it settles cancelled, as forced, below.
          if (error instanceof AttemptStoppedError && error.outcome === 'requeue' && !task.cancellation) {
            this._settleTask(task, pool, { requeue: true }, token);
            return;
          }

          // The capacity race: the pool's room went between the check and the
          // hand-over. A worker filled up, or there was none and no room for
          // one - the second is what a resource.acquired listener adding a
          // task causes, since that task is dispatched into the slot this one
          // was about to take. Neither is the task's fault; it used to fail
          // the second way for good, in user_action.
          const message = typeof error?.message === 'string' ? error.message : '';
          if (message === 'Worker at maximum concurrent task capacity'
              || message.startsWith('No worker thread available')) {
            // Being cancelled: it ends here rather than going back in the queue
            if (task.cancellation) {
              this._onTaskFailed(task, error, pool, token);
              return;
            }

            // Revert state and stats changes
            task.state = 'pending';
            task.startTime = undefined;
            task.executionTime = undefined; // Discard timing from the abandoned attempt
            this.stats.running = Math.max(0, this.stats.running - 1);
            this.stats.queued++;

            // Back in the ready queue BEFORE the release, which can reach a
            // listener: a cancel from there must find it queued, not in transit
            const workerQueue = this.readyQueuesByWorker.get(task.workerType);
            if (workerQueue) {
              workerQueue.push(task);
              task._queuedIn = 'ready';
            }

            // Release pre-allocated resources. That release wakes waiters, which
            // are dispatched on the spot - to this same pool, whose capacity
            // check just lied, and so on with no timer in between. So the pool
            // reports no room until the retry below.
            pool._capacityRaceBackoff = true;
            this._releaseResourcesForTask(task, pool);

            // Retry dispatch (thread should be available soon)
            setTimeout(() => {
              pool._capacityRaceBackoff = false;
              this._dispatchLoop();
            }, 10);
          } else {
            this._onTaskFailed(task, error, pool, token);
          }
        });
    }

    _releaseResourcesForTask(task: FyflowTask, pool: any) {
      if (!task._resourcesPreAllocated) return;

      // Release the exact buckets that were acquired, not whatever keyFrom would
      // return now - a payload the worker mutated must not send the release to a
      // different key than the acquire
      const holds = this._holdsFor(task, pool);
      const grants = task._grants;
      // Taken off the task first: the release reaches listeners, and whatever
      // they do, these grants are returned once
      task._grants = undefined;
      task._grantView = undefined;

      // Release all resources first, then retry blocked tasks
      for (const { id, group, key } of holds) {
        // The original, as onStart returned it: a group may rely on identity
        const grant = grants?.get(FyflowScheduler._holdId(id, key));
        group.onFinish(key, 1, grant?.raw);
        if (this._resourceObserved) {
          this._emitResource('resource.released', group, {
            groupId: id,
            lifetime: 'task-held',
            holderKind: 'task',
            holderId: task.id,
            workerType: task.workerType,
            cost: 1,
            key,
            ...(grant?.copy !== undefined ? { grant: grant.copy } : {}),
            reason: 'settled'
          });
        }
      }

      task._resourcesPreAllocated = undefined;
      task._resourceKeys = undefined;

      // Retry blocked tasks AFTER all resources are released
      for (const { id, key } of holds) this._retryBlockedTasksForGroup(id, key);
    }

    /**
     * Where a task that reached a worker settles.
     *
     * The one exception is {@link _failOrphanedTask}, for a task that never
     * reached one - it must not take this path, which decrements
     * `stats.running` for a task that was only ever queued.
     *
     * An outcome can reach the scheduler twice - as the pool's `task.failed`
     * event and as the settling of the pool's task promise - and each of those
     * is the ONLY path in some situation, so neither can be dropped:
     *
     * - an ordinary task throw reaches both
     * - a worker that dies with `requeueFailedTasks: false` reaches only the event
     * - a worker that cannot be constructed reaches only the promise
     *
     * They used to be two near-duplicate implementations, so a single failure
     * emitted `task.failed` twice, counted `stats.failed` twice, raced the
     * terminal state between `user_action` and `failed`, and - because only the
     * promise path released resources - leaked a resource group slot whenever
     * only the event path ran.
     */
    private _settleTask(
      task: FyflowTask,
      pool: any,
      outcome: { ok: true; result: any } | { ok: false; error: any } | { requeue: true },
      token?: number
    ): void {
      // An outcome from an earlier attempt. Checked before _settling, which the
      // new attempt's dispatch has already cleared: after a requeue the
      // abandoned run keeps executing, and its result used to settle the live
      // attempt. Outcomes arriving as pool events carry no token; those are
      // kept off stale attempts by the failed worker forgetting the task.
      if (token !== undefined && token !== task._dispatchToken) return;
      // After shutdown() has finished, nobody is waiting any more
      if (this.shutDown) return;

      // One settle per attempt. Cleared in _dispatchTask, so a retried or
      // requeued task can settle again.
      if (task._settling) return;
      task._settling = true;

      // Released here and nowhere else, so every path returns its slots exactly once
      this._releaseResourcesForTask(task, pool);

      // Cancel wins: once accepted, whatever the attempt produced - a result,
      // an error, a worker death - the task ends cancelled, with no retry and
      // no requeue. Checked after the release rather than before it, because
      // the release can reach a listener that cancels this very task.
      if (task.cancellation) {
        // Stopped by terminating its worker, rather than by itself
        if ('error' in outcome && outcome.error instanceof AttemptStoppedError) {
          task.cancellation.forced = true;
        }
        pool?._clearCancelGrace?.(task.id, task._dispatchToken);
        this._settleCancelled(task);
        return;
      }

      // A worker died under this task and the pool wants it retried on another.
      // Handled here rather than in its own listener so it competes with the
      // other two outcomes for this attempt's single settle - otherwise the same
      // failure could requeue the task AND count it failed, which is how
      // stats.queued and stats.failed came to total more than the task count.
      if ('requeue' in outcome) {
        task.state = 'pending';
        task.startTime = undefined;
        task.executionTime = undefined; // Discard timing from the abandoned attempt
        task._resourcesPreAllocated = undefined;
        task._resourceKeys = undefined;
        this.stats.running = Math.max(0, this.stats.running - 1);
        this.stats.queued++;

        const requeueTarget = this.readyQueuesByWorker.get(task.workerType);
        if (requeueTarget) {
          requeueTarget.push(task);
          task._queuedIn = 'ready';
        }

        // _settling stays set until the next dispatch clears it, so a late
        // outcome from the abandoned attempt cannot settle the new one
        this._dispatchLoop();
        return;
      }

      if (outcome.ok) {
        task.state = 'done';
        task.endTime = Date.now();
        task.result = outcome.result;
        this.stats.running = Math.max(0, this.stats.running - 1);
        this.stats.done++;
        task.resolve?.(outcome.result);

        task.timestamp = Date.now();
        this.dispatchEvent(new CustomEvent('task.completed', { detail: task }));
        this._settleDescendantTrackers(task.id);
        this._recordTerminalTask(task.id);
        this._checkCompletion();

        // Resources are free again
        this._dispatchLoop();
        return;
      }

      const error = outcome.error;

      task.error = error instanceof Error ? error.message : String(error ?? 'Task failed');

      // Retry, if the policy still allows it
      if (task.attempts < (task.retryPolicy?.maxRetries || 0)) {
        task.attempts++;
        task.state = 'pending';
        task.startTime = undefined;
        task.executionTime = undefined; // Discard timing from the abandoned attempt
        task._resourcesPreAllocated = undefined;
        task._resourceKeys = undefined;
        this.stats.running = Math.max(0, this.stats.running - 1);
        this.stats.queued++;

        // Waits out its backoff outside the ready queue. It used to go straight
        // back on it, with only an extra dispatch pass scheduled after the
        // backoff - so under load any other dispatch started it early, 1 ms
        // after failing where 1000 ms was asked for.
        // In the map before the event, so a listener sees where it is.
        const waiting: { task: FyflowTask; timer?: ReturnType<typeof setTimeout> } = { task };
        this.retryWaiting.set(task.id, waiting);
        task._queuedIn = 'backoff';
        task.timestamp = Date.now();
        this.dispatchEvent(new CustomEvent('task.retrying', { detail: task }));

        // Armed after the event: the backoff counts from its timestamp
        if (this.retryWaiting.get(task.id) === waiting) {
          waiting.timer = setTimeout(() => {
            if (this.retryWaiting.get(task.id) !== waiting) return;
            this.retryWaiting.delete(task.id);
            this.readyQueuesByWorker.get(task.workerType)?.push(task);
            task._queuedIn = 'ready';
            this._dispatchLoop();
          }, task.retryPolicy?.backoffMs || 0);
        }

        // The failed attempt's slot is free now, for someone else
        this._dispatchLoop();
        return;
      }

      // Terminal. stats.failed counts tasks that failed, mirroring stats.done -
      // not attempts, which is why a task with retries counts once.
      task.endTime = Date.now();
      this.stats.running = Math.max(0, this.stats.running - 1);
      this.stats.failed++;

      if (task.optional) {
        // Optional work does not block the caller
        task.state = 'failed';
        task.resolve?.(null);
      } else {
        // Out of retries and not optional: something outside the scheduler has
        // to decide what happens next
        task.state = 'user_action';
        task.reject?.(error instanceof Error ? error : new Error(task.error));
      }

      // One stamp for both: they report the same moment
      task.timestamp = Date.now();
      this.dispatchEvent(new CustomEvent('task.failed', { detail: task }));
      if (!task.optional) {
        this.dispatchEvent(new CustomEvent('task.user_action', { detail: task }));
      }

      this._settleDescendantTrackers(task.id);
      this._recordTerminalTask(task.id);
      this._checkCompletion();

      // Resources are free again
      this._dispatchLoop();
    }

    /**
     * End a dispatched task as cancelled. Reached from `_settleTask` only, so the
     * attempt's resources have been released exactly once already.
     */
    private _settleCancelled(task: FyflowTask) {
      task.state = 'cancelled';
      task.endTime = Date.now();
      // A failure earlier in this task's life is not how it ended
      task.error = undefined;
      this.stats.running = Math.max(0, this.stats.running - 1);
      this.stats.cancelled++;
      this._finishCancelled(task);
      this._checkCompletion();
      // Resources are free again
      this._dispatchLoop();
    }

    /** What every cancelled task gets, dispatched or not, once its counters have moved. */
    private _finishCancelled(task: FyflowTask) {
      task.reject?.(new CancelledError(task.id, task.cancellation?.reason));
      task.timestamp = Date.now();
      this.dispatchEvent(new CustomEvent('task.cancelled', { detail: task }));
      const pending = task._cancelPending;
      task._cancelPending = undefined;
      pending?.resolve(true);
      this._settleDescendantTrackers(task.id);
      this._recordTerminalTask(task.id);
    }

    /**
     * Cancel a task.
     *
     * A task still waiting - queued, blocked on a resource group, or sitting out
     * a retry backoff - is removed at once and never dispatches. A running one is
     * signalled through `context.signal`, `task.cancelling` fires, and it ends
     * when its attempt stops: on a threaded pool the worker is forced after
     * `graceMs` if it has not; an inline run cannot be forced, and is waited for.
     * Either way it ends in `cancelled` and `task.cancelled`, whatever the
     * attempt produced, its promise rejects with a `CancelledError`, and it is
     * not retried or requeued.
     *
     * Resolves `true` once the task is cancelled, including when it already
     * was; `false` if the id is unknown or the task finished first. Never
     * rejects because of a task's state, so cancelling every task of a run in a
     * loop needs no guards. A repeat call returns the same promise, and a
     * shorter `graceMs` on it shortens the wait.
     *
     * @throws TypeError if `graceMs` is not a number, RangeError if it is
     *   negative or NaN - synchronously, as for any other programming error.
     */
    cancel(taskId: string, options: CancelOptions = {}): Promise<boolean> {
      // Not async: a bad argument throws here, rather than becoming a rejection
      const { reason, graceMs } = options;
      if (graceMs !== undefined) validateGraceMs(graceMs, 'graceMs');

      const task = this.tasks.get(taskId);
      if (!task) return Promise.resolve(false);
      if (task.state === 'cancelled') return Promise.resolve(true);
      if (this._isTerminal(task.state)) return Promise.resolve(false);

      if (task.state === 'running') {
        const pool = this.workerPools[task.workerType];
        if (task._cancelPending) {
          // Pressing Cancel again can only escalate
          if (graceMs !== undefined) pool?._shortenCancelGrace?.(task.id, task._dispatchToken, graceMs);
          return task._cancelPending.promise;
        }
        let resolve!: (cancelled: boolean) => void;
        const promise = new Promise<boolean>(r => { resolve = r; });
        task._cancelPending = { promise, resolve };
        task.cancellation = { reason, requestedAt: Date.now(), forced: false };
        task.error = undefined;
        task.timestamp = Date.now();
        this.dispatchEvent(new CustomEvent('task.cancelling', { detail: task }));
        // A listener may have let it settle already (cancel from resource.acquired)
        if (task.state === 'running') {
          pool?._cancelTask?.(task.id, task._dispatchToken, { reason, graceMs });
        }
        return promise;
      }

      this._cancelUndispatched(task, reason);
      return Promise.resolve(true);
    }

    /**
     * Places 1-3: a task that was never dispatched, or is waiting to be again.
     * It holds no resources, so nothing is handed on: unlike a settle, this does
     * not run the dispatch loop or retry blocked tasks. That keeps a loop of
     * cancels over a large queue linear.
     */
    private _cancelUndispatched(task: FyflowTask, reason: unknown) {
      // Out of where it waits, and out of the counter it is in, first
      switch (task._queuedIn) {
        case 'ready':
          this.readyQueuesByWorker.get(task.workerType)?.tombstone(task);
          this.stats.queued = Math.max(0, this.stats.queued - 1);
          break;
        case 'backoff': {
          const waiting = this.retryWaiting.get(task.id);
          if (waiting?.timer) clearTimeout(waiting.timer);
          this.retryWaiting.delete(task.id);
          this.stats.queued = Math.max(0, this.stats.queued - 1);
          break;
        }
        case 'blocked':
          // Blocked tasks are counted in no stats key
          this.blockedQueues.get(task._blockedQueueId!)?.tombstone(task);
          this._dropBlockedQueueIfEmpty(task._blockedQueueId);
          break;
        // undefined: in transit between queues, counted nowhere; whoever is
        // moving it sees the terminal state and leaves it
      }
      const leftQueue = task._queuedIn === 'blocked' ? task._blockedQueueId : undefined;
      task._queuedIn = undefined;
      task._blockedQueueId = undefined;

      task.state = 'cancelled';
      task.cancellation = { reason, requestedAt: Date.now(), forced: false };
      task.endTime = Date.now();
      task.error = undefined;
      this.stats.cancelled++;

      if (leftQueue !== undefined && this._resourceObserved) {
        const { groupId, key } = this._splitQueueId(leftQueue);
        this._emitResource('resource.unblocked', this.groups[groupId], {
          groupId,
          ...this._blockedHoldingFor(task, this.workerPools[task.workerType], groupId),
          holderKind: 'task',
          holderId: task.id,
          workerType: task.workerType,
          key,
          reason: 'cancelled'
        });
      }

      this._finishCancelled(task);

      // Only the drained check: the full _checkCompletion can sweep every group
      // and key, which per cancel would make cancelling a run quadratic
      if (this.stats.queued === 0 && this.stats.running === 0 && !this._hasBlockedTasks()) {
        this._clearPeriodicRetry();
        this.dispatchEvent(new CustomEvent('scheduler.completed', {
          detail: { ...this.stats, timestamp: Date.now() }
        }));
      }
    }

    _onTaskComplete(task: FyflowTask, result: any, pool: any, token?: number) {
      this._settleTask(task, pool, { ok: true, result }, token);
    }

    _onTaskFailed(task: FyflowTask, error: any, pool: any, token?: number) {
      this._settleTask(task, pool, { ok: false, error }, token);
    }

    _retryBlockedTasks() {
      // Check if any ready tasks exist across all worker types
      const hasReadyTasks = Array.from(this.readyQueuesByWorker.values()).some(q => q.length > 0);
      if (hasReadyTasks) {
        this._dispatchLoop();
      }

      // Then, retry blocked tasks for groups that now have capacity. Keyed
      // groups hold one queue per key, so each is retried on its own bucket -
      // otherwise a saturated key would gate every other key behind it.
      for (const [groupId, group] of Object.entries(this.groups)) {
        // Nobody waiting on it: skip before asking the group anything, so a
        // heartbeat costs O(groups with waiters) rather than O(groups)
        if (!group) continue;
        if (group.keyed || FyflowScheduler._hasWaitSets(group)
            ? !this.blockedKeys.has(groupId) && !this.blockedQueues.has(groupId)
            : !this.blockedQueues.has(groupId)) continue;
        if (!group.canRun()) continue;

        if (group.keyed) {
          for (const key of this._blockedKeysFor(groupId)) {
            this._retryBlockedTasksForGroup(groupId, key);
          }
        } else {
          this._retryBlockedTasksForGroup(groupId);
        }
      }

    }

    /**
     * Keys that currently have tasks blocked on this group. A copy, because
     * retrying a key can delete it from the index.
     */
    private _blockedKeysFor(groupId: string): string[] {
      return [...(this.blockedKeys.get(groupId) ?? [])];
    }

    /**
     * Wake the waiters of one (group, key) queue: admit as many as the group
     * now has room for.
     *
     * Through a pump, never recursively. Admitting a waiter dispatches it, which
     * reaches listeners, which can settle a task (a cancel), whose release wakes
     * a queue again - possibly this one. Nested, that recursion's depth is set
     * by listener behaviour, and an outer loop holding state across it acts on
     * a stale queue. So a wake requested while one is running is queued and run
     * after it, in the same synchronous turn.
     */
    _retryBlockedTasksForGroup(groupId: string, key?: string) {
      // A group with wait sets is woken as a whole: what was freed may suit
      // any of its sets, and they compete for it in arrival order
      const queueId = FyflowScheduler._hasWaitSets(this.groups[groupId])
        ? SWEEP_PREFIX + groupId
        : this._blockedQueueId(groupId, key);
      this.wakePending.add(queueId);
      if (this.waking) return;
      this.waking = true;
      try {
        while (this.wakePending.size > 0) {
          const next: string = this.wakePending.values().next().value!;
          this.wakePending.delete(next);
          if (next.startsWith(SWEEP_PREFIX)) this._sweepWaitSets(next.slice(SWEEP_PREFIX.length));
          else this._wakeQueue(next);
        }
      } finally {
        this.waking = false;
      }
    }

    /**
     * Wake a group whose waiters queue per candidate set: admit, one at a
     * time, the oldest head among all its queues that can go - whether it
     * asked for one unit in particular or for any. So a pinned waiter is not
     * starved by a stream of "any" tasks: once it is the oldest that can use
     * a freed unit, it gets it.
     *
     * Heads only, never a scan: a waiter pinned to a busy unit is skipped
     * along with its whole queue, at the cost of one comparison. A queue
     * whose head cannot go is set aside for the rest of the sweep.
     */
    private _sweepWaitSets(groupId: string) {
      // Listed once per sweep, not per admission: building the ids is string
      // work, and it showed as heap. A queue created during the sweep holds a
      // waiter that was just refused, which the next release wakes.
      const candidates = this._queuesOfGroup(groupId);
      // Each round admits or moves one waiter, or sets a queue aside, so this
      // bounds it whatever a listener does; it is never reached in practice
      let budget = 2 * this.blockedLive.live + candidates.length + 1;
      while (budget-- > 0 && candidates.length > 0) {
        let oldest = -1;
        let oldestSeq = Infinity;
        for (let i = 0; i < candidates.length; i++) {
          const head = this.blockedQueues.get(candidates[i])?.at(0);
          if (head && (head._blockedSeq ?? 0) < oldestSeq) {
            oldest = i;
            oldestSeq = head._blockedSeq ?? 0;
          }
        }
        if (oldest === -1) return;
        // Its head cannot go: set aside for the rest of the sweep
        if (this._wakeQueue(candidates[oldest], 1) === 0) candidates.splice(oldest, 1);
      }
    }

    private wakePending = new Set<string>();
    private waking = false;

    /** @returns how many waiters left the queue; at most `maxExits`. */
    private _wakeQueue(queueId: string, maxExits = Infinity): number {
      const blockedTasks = this.blockedQueues.get(queueId);
      if (!blockedTasks || blockedTasks.length === 0) return 0;

      // Held for the whole wake - see blockedInUse - and deleted after, if the
      // wake, or anything it reached, emptied it
      this.blockedInUse.set(queueId, (this.blockedInUse.get(queueId) ?? 0) + 1);
      try {
        const { groupId, key } = this._splitQueueId(queueId);
        return this._admitFromQueue(queueId, groupId, blockedTasks, key, maxExits);
      } finally {
        const depth = this.blockedInUse.get(queueId)! - 1;
        if (depth > 0) this.blockedInUse.set(queueId, depth);
        else this.blockedInUse.delete(queueId);
        this._dropBlockedQueueIfEmpty(queueId);
      }
    }

    /**
     * Admit on wake. Each waiter at the head is admitted and dispatched - which
     * acquires its groups - before the next one is examined, so `canRun()`
     * itself ends the loop: one release admits what it freed, and no more.
     *
     * It used to move every waiter back to the ready queue while `canRun()`
     * stayed true, because nothing was acquired until dispatch - the whole
     * queue for one free slot, all but one re-blocked by the dispatch loop.
     * Harmless while the queue held one task; quadratic once it held them all,
     * as a keyed group's queues always did (50K tasks over 10 keys: 7.4
     * minutes, 1,630 resource.blocked per task).
     *
     * The head is examined without being removed, so a waiter that cannot go
     * is never taken out and put back: every move below is a real exit, and
     * its event is emitted once it has landed.
     */
    private _admitFromQueue(
      queueId: string, groupId: string, blockedTasks: TaskQueue<FyflowTask>, key?: string, maxExits = Infinity
    ): number {
      const group = this.groups[groupId];
      if (!group) return 0;
      let exits = 0;

      // Waiters moved to a ready queue whose pool had no free slot. They hold
      // nothing yet, so they count against the capacity this wake hands out -
      // or a full pool would pull the whole queue into its ready queue. A
      // group that does not weigh `cost` (a rate group) cannot be asked for
      // more than one, so at most one goes that way per wake.
      let parked = 0;
      // Asked only once something has parked: most wakes never need it, and
      // a group with several wait sets is woken one queue at a time
      let weighsCost: boolean | undefined;
      // Waiters whose pool has gone. Failed after the loop, never during it.
      const orphaned: FyflowTask[] = [];

      while (blockedTasks.length > 0 && exits < maxExits) {
        const task = blockedTasks.at(0)!;
        if (parked > 0 && !(weighsCost ??= !group.canRun(key, Number.MAX_SAFE_INTEGER))) break;

        const pool = this.workerPools[task.workerType];
        if (!pool) {
          // Exit (c). Collected rather than failed here: failing dispatches
          // `task.failed` synchronously, and a listener re-entering the
          // scheduler would do so while this loop still walks `blockedTasks`.
          blockedTasks.shift();
          task._queuedIn = undefined;
          task._blockedQueueId = undefined;
          orphaned.push(task);
          exits++;
          continue;
        }

        // Waiting here for a worker's resident units, rather than for a group
        // the task itself holds - the same derivation `_blockedHoldingFor` uses
        // Built once per waiter: the checks below all read it
        const holds = this._holdsFor(task, pool);
        const own = holds.find(({ id, key: k }) => id === groupId && k === key);
        const residentWait = own === undefined;
        // What it asked of this group: its own request, or its pool's
        const request = residentWait ? pool.residentRequests?.[groupId] : own.request;
        if (!group.canRun(key, parked + 1, request)) break;

        // Its pool's per-worker cost does not fit.
        if (this._needsNewWorker(pool) && pool.canHoldAnotherWorker?.() === false) {
          // In a resident queue: stop. Head-of-line - nothing behind it may
          // overtake. Letting cheaper tasks through instead starves an
          // expensive pool indefinitely: a stream of 2-unit workers can keep a
          // 20-unit worker out of a 24-unit group forever.
          if (residentWait) break;
          // In a task-held queue: wait in the resident group it is short of,
          // whose release can admit it, at the back, behind the pools already
          // waiting there. Stopping here instead blocked every waiter behind
          // it - including ones that need no worker - until the heartbeat.
          const resident = this._residentGroupsForPool(pool);
          const short = resident.find(({ group: g, cost, request: r }) => !g.canRun(undefined, cost, r)) ?? resident[0];
          this._migrate(task, pool, blockedTasks, groupId, key, { ...short, key: undefined }, true);
          exits++;
          continue;
        }

        // Another of its holds still full - another group, or another key of
        // this one: wait there instead. Its release is what can admit the task
        // now, and this queue's never could - a task left here waited for the
        // heartbeat.
        const refusing = holds.find(hold => hold !== own && !hold.group.canRun(hold.key, 1, hold.request));
        if (refusing) {
          // Except out of a resident queue. Moved, it gives up its place at the
          // head; the pools behind it take the units it was waiting for, and
          // once its group frees it no longer fits and joins the back - the
          // same starvation as above. It keeps its place and is retried by
          // the heartbeat, a cost paid only by a pool whose worker and task
          // are both short at once.
          if (residentWait) break;
          this._migrate(task, pool, blockedTasks, groupId, key, refusing);
          exits++;
          continue;
        }

        // Exit (a): admitted
        blockedTasks.shift();
        exits++;
        if (this._admitWaiter(task, pool, group, groupId, key)) parked++;
      }

      // Last, with the queue in its final shape, so a listener re-entering on
      // task.failed sees consistent state
      for (const task of orphaned) this._failOrphanedTask(task, queueId);
      return exits;
    }

    /**
     * Exit (a), for a waiter already taken out of its queue. Into the ready
     * queue first, so stats and `_queuedIn` are true while its event is out;
     * dispatched from there.
     *
     * @returns true if it was left in the ready queue for want of a free slot.
     */
    private _admitWaiter(task: FyflowTask, pool: any, group: any, groupId: string, key: string | undefined): boolean {
      task._blockedQueueId = undefined;
      const workerQueue = this.readyQueuesByWorker.get(task.workerType)!;
      workerQueue.unshift(task);
      task._queuedIn = 'ready';
      task._admittedByWake = true;
      this.stats.queued++;
      if (this._resourceObserved) {
        this._emitResource('resource.unblocked', group, {
          groupId,
          ...this._blockedHoldingFor(task, pool, groupId),
          holderKind: 'task',
          holderId: task.id,
          workerType: task.workerType,
          key,
          reason: 'admitted'
        });
      }

      // A listener may have cancelled it, or moved things: dispatch it only
      // if it is still where it was put
      if (task._queuedIn !== 'ready' || workerQueue.at(0) !== task) return false;
      workerQueue.shift();
      task._queuedIn = undefined;
      const result = this._canDispatchTask(task, pool);
      if (result === true) {
        this._dispatchTask(task, pool);
      } else if (result === false) {
        // No free slot: it waits for one in the ready queue, which the
        // pool's next settle drains
        workerQueue.unshift(task);
        task._queuedIn = 'ready';
        return true;
      }
      // 'blocked': a listener changed something and it is queued again, its
      // events already out
      return false;
    }

    /**
     * Move the head of `from` to the queue of the group now refusing it - one
     * exit and one entry, so the membership fold stays balanced. It joins the
     * tail: it has not waited in that queue, and an insertion anywhere else
     * would break the order the fold reconstructs.
     *
     * The entry is announced before the exit. Both events go out with the task
     * already in its new queue, so a listener that cancels it - from either
     * event - has it leave that queue, with its own `cancelled`, and that needs
     * the `blocked` it balances to be out already. Announced the other way
     * round, a cancel from the `migrated` listener left an exit from the new
     * queue with no entry.
     */
    private _migrate(
      task: FyflowTask, pool: any, from: TaskQueue<FyflowTask>, fromGroupId: string, fromKey: string | undefined,
      to: Hold, toResident = false
    ) {
      const toKey = to.key;
      const toQueueId = this._blockedQueueId(to.id, toKey ?? FyflowScheduler._waitSetOf(to.group, to.request));
      if (from.at(0) === task) from.shift();
      else from.remove(task);
      const target = this._blockedQueue(toQueueId);
      target.push(task);
      task._blockedSeq = ++this.blockedSeq;
      if (toResident) this._noteResidentWait(task);
      task._queuedIn = 'blocked';
      task._blockedQueueId = toQueueId;
      if (!this._resourceObserved) return;
      this._emitResource('resource.blocked', to.group, {
        groupId: to.id,
        ...this._blockedHoldingFor(task, pool, to.id),
        holderKind: 'task',
        holderId: task.id,
        workerType: task.workerType,
        key: toKey,
        queuePosition: target.length - 1
      });
      // Unconditional: whatever that listener did, the task has left `from`
      this._emitResource('resource.unblocked', this.groups[fromGroupId], {
        groupId: fromGroupId,
        ...this._blockedHoldingFor(task, pool, fromGroupId),
        holderKind: 'task',
        holderId: task.id,
        workerType: task.workerType,
        key: fromKey,
        reason: 'migrated'
      });
    }

    /**
     * Fail a blocked task whose worker pool no longer exists.
     *
     * It has been shifted out of its queue and there is nowhere to put it back,
     * so it is failed terminally rather than dropped. Dropping it made it a
     * phantom: no `task.failed`, no `user_action`, no rejection, and an
     * `onCompletePromise()` that never settled - while `stats` never accounted
     * for it either.
     *
     * Two deliberate departures from the ordinary failure path:
     *
     * - **Retries are skipped.** The pool is gone, so another attempt cannot
     *   succeed. Worse, `readyQueuesByWorker` keeps its entry for the type
     *   forever, so a retry would requeue the task into a queue no dispatch can
     *   drain and leave `stats.queued` permanently above zero - wedging
     *   `scheduler.completed` instead of unwedging it.
     * - **It does not go through `_settleTask`**, which decrements
     *   `stats.running`. A blocked task was never running: it gave up its
     *   `queued` count when it left the ready queue, so settling it that way
     *   would decrement a count belonging to a different task.
     *
     * `addTask`/`addTasks` validate worker types and the library never mutates
     * `workerPools`, so this should be unreachable. It is handled because when
     * it was not, it was silent.
     */
    private _failOrphanedTask(task: FyflowTask, leftQueueId: string): void {
      // Guarded on the task having ended, not on `_settling`. That flag is per
      // attempt and stays set after a retry or requeue decision until the next
      // dispatch, so a waiter that had already failed once returned here
      // unsettled - shifted out of its queue and dropped, the phantom this
      // function exists to prevent.
      if (this._isTerminal(task.state)) return;
      task._settling = true;

      const error = new Error(
        `Worker pool "${task.workerType}" no longer exists - task "${task.id}" ` +
        `was waiting on a resource group and can never be dispatched`
      );

      // Announce the exit it has already made, then leave no membership behind:
      // a queue still holding it would never drain in a consumer's fold
      this._emitOrphanedExit(task, leftQueueId);
      const left: string[] = [];
      for (const [queueId, queue] of this.blockedQueues) {
        if (queue.remove(task)) left.push(queueId);
      }
      // Deleted before any event, and outside the iteration
      for (const queueId of left) this._dropBlockedQueueIfEmpty(queueId);
      for (const queueId of left) this._emitOrphanedExit(task, queueId);

      task.error = error.message;
      task.endTime = Date.now();
      this.stats.failed++;

      if (task.optional) {
        task.state = 'failed';
        task.resolve?.(null);
      } else {
        task.state = 'user_action';
        task.reject?.(error);
      }

      task.timestamp = Date.now();
      this.dispatchEvent(new CustomEvent('task.failed', { detail: task }));
      if (!task.optional) {
        this.dispatchEvent(new CustomEvent('task.user_action', { detail: task }));
      }

      this._settleDescendantTrackers(task.id);
      this._recordTerminalTask(task.id);
      this._checkCompletion();
    }

    /** One `orphaned` exit, for the queue named by `queueId`. */
    private _emitOrphanedExit(task: FyflowTask, queueId: string): void {
      if (!this._resourceObserved) return;

      const { groupId, key } = this._splitQueueId(queueId);
      this._emitResource('resource.unblocked', this.groups[groupId], {
        groupId,
        ...this._blockedHoldingFor(task, this.workerPools[task.workerType], groupId),
        holderKind: 'task',
        holderId: task.id,
        workerType: task.workerType,
        key,
        reason: 'orphaned'
      });
    }

    /**
     * Arm the fallback retry, if it is not already armed.
     *
     * This used to `clearTimeout` and start a fresh timer on every call, which
     * made it a debounce rather than a heartbeat - and it is armed from every
     * settle. A scheduler completing tasks faster than
     * `periodicRetryIntervalMs` therefore pushed the deadline out forever and
     * the retry **never fired at all**: measured at zero calls across 600ms
     * while 156 tasks completed, with two rate-limited tasks starved through
     * ten window rollovers.
     *
     * That is a stall, not a slow path. A task-held group is retried directly
     * by `_releaseResourcesForTask` when its own slot frees, so the damage was
     * confined to the waiters with no other wakeup: rate-limit windows rolling
     * over, and resident admission, neither of which raises an event.
     *
     * Leaving a pending timer alone is what makes it a heartbeat. It costs one
     * `_retryBlockedTasks()` per interval while work exists, and that
     * early-exits per group when the group is at capacity or its queue is
     * empty.
     */
    _schedulePeriodicRetry() {
      if (this.retryTimer) return;

      // Only schedule retry if there are queued tasks or blocked tasks (fallback for groups without events)
      const hasReadyTasks = Array.from(this.readyQueuesByWorker.values()).some(q => q.length > 0);
      const hasBlockedTasks = this._hasBlockedTasks();
      if (hasReadyTasks || hasBlockedTasks) {
        this.retryTimer = setTimeout(() => {
          // Cleared before the work, so the re-arm below is not a no-op
          this.retryTimer = null;
          this._retryBlockedTasks();
          this._schedulePeriodicRetry(); // Schedule next retry
        }, this.options.periodicRetryIntervalMs!); // Configurable retry interval (default 50ms)
      }
    }

    _clearPeriodicRetry() {
      if (this.retryTimer) {
        clearTimeout(this.retryTimer);
        this.retryTimer = null;
      }
    }

    // Helper to track internal listeners for cleanup
    // Override addEventListener to track ALL listeners for cleanup
    // TODO: likely unneccesary, verify
    // override addEventListener(event: string, listener: EventListenerOrEventListenerObject, options?: boolean | AddEventListenerOptions): void {
    //   super.addEventListener(event, listener, options);
    //   this._trackListener(this, event, listener as Function);
    // }

    private _addInternalListener(target: any, event: string, listener: Function) {
      this._trackListener(target, event, listener);
      target.addEventListener(event, listener);
    }

    private _trackListener(target: any, event: string, listener: Function) {
      if (!this.allListeners.has(target)) {
        this.allListeners.set(target, []);
      }
      this.allListeners.get(target)!.push({event, listener});
    }

    // Remove all event listeners (both internal and external)
    private _removeAllListeners() {
      for (const [target, listeners] of this.allListeners) {
        for (const {event, listener} of listeners) {
          try {
            target.removeEventListener(event, listener);
          } catch {
            // Target may already be destroyed - ignore
          }
        }
      }
      this.allListeners.clear();
    }

    /**
     * Tracked so `resource.*` emission can be skipped entirely when nothing is
     * subscribed. Every other event is unaffected.
     */
    override addEventListener(
      type: string,
      callback: EventListenerOrEventListenerObject | null,
      options?: AddEventListenerOptions | boolean
    ): void {
      super.addEventListener(type, callback, options);
      if (type === 'task.message') {
        if (!this.messageListeners.has(callback)) {
          this.messageListeners.add(callback);
          if (this.messageListeners.size === 1) this._setMessageGate(true);
        }
        return;
      }
      if (!type.startsWith('resource.')) return;

      let subscribers = this.resourceListeners.get(type);
      if (!subscribers) this.resourceListeners.set(type, subscribers = new Set());
      subscribers.add(callback);
    }

    override removeEventListener(
      type: string,
      callback: EventListenerOrEventListenerObject | null,
      options?: EventListenerOptions | boolean
    ): void {
      super.removeEventListener(type, callback, options);
      if (type === 'task.message') {
        if (this.messageListeners.delete(callback) && this.messageListeners.size === 0) {
          this._setMessageGate(false);
        }
        return;
      }
      if (!type.startsWith('resource.')) return;

      const subscribers = this.resourceListeners.get(type);
      if (!subscribers) return;
      subscribers.delete(callback);
      if (subscribers.size === 0) this.resourceListeners.delete(type);
    }

    private _setMessageGate(open: boolean) {
      for (const pool of Object.values(this.workerPools) as any[]) pool?._setTaskMessageGate?.(open);
    }

    /**
     * A task's emitted event, from its pool. The pool has already dropped events
     * from runs it abandoned; the state check covers anything that slips past
     * a settle. `attemptNumber` is safe to read here: an attempt's events all
     * arrive before it settles, and the number changes only at the next dispatch.
     */
    private _onTaskMessage(detail: Omit<TaskMessageDetail, 'attemptNumber'>) {
      const task = this.tasks.get(detail.taskId);
      if (!task || task.state !== 'running') return;
      // The pool built this object for us alone, so it is completed in place
      // rather than copied
      (detail as TaskMessageDetail).attemptNumber = task.attemptNumber;
      this.dispatchEvent(new CustomEvent('task.message', { detail }));
    }

    /** Whether building a resource event is worth the allocation. */
    private get _resourceObserved(): boolean {
      return this.resourceListeners.size > 0;
    }

    /**
     * The gauges a group presents, defaulting to a single `level` gauge derived
     * from `getMetrics()`.
     *
     * The default is what makes acceptance criterion 5 hold: a group
     * implementing only `canRun`/`onStart`/`onFinish`/`getMetrics` - which is
     * every group written before gauges existed, and every third-party one - is
     * still renderable without a line of viewer code.
     */
    private _describeGroup(group: any): GaugeDescription {
      if (typeof group?.describe === 'function') return group.describe();

      const metrics = group?.getMetrics?.();
      if (!metrics) return { gauges: [] };

      return {
        gauges: [{
          id: 'units',
          label: group.id ?? 'units',
          kind: 'level',
          unit: 'units',
          limit: metrics.limit
        }]
      };
    }

    /** Current readings for a group, with the same `getMetrics()` default. */
    private _readGroup(group: any, key?: string): GaugeReading[] {
      if (typeof group?.read === 'function') return group.read(key);

      const metrics = group?.getMetrics?.();
      if (!metrics) return [];

      return [{ id: 'units', value: metrics.running, limit: metrics.limit }];
    }

    /**
     * The gauges every group presents, keyed by group id.
     *
     * Schema, not state: the gauges a group declares do not change over its
     * lifetime, so this can be read once at attach and cached. That is the line
     * this accessor does not cross - it is not a state snapshot, which stays
     * declined.
     */
    describeResources(): Record<string, GaugeDescription> {
      const described: Record<string, GaugeDescription> = {};
      for (const [id, group] of Object.entries(this.groups)) {
        if (group) described[id] = this._describeGroup(group);
      }
      return described;
    }

    /**
     * Dispatch one `resource.*` event. Call sites guard on
     * {@link _resourceObserved} first, so neither this object nor the `read()`
     * behind it is built for nobody.
     */
    private _emitResource(
      name: 'resource.acquired' | 'resource.released' | 'resource.blocked' | 'resource.unblocked',
      group: any,
      detail: Omit<ResourceEventDetail, 'groupType' | 'timestamp' | 'readings'> & {
        reason?: ResourceReleaseReason | ResourceUnblockReason;
        queuePosition?: number;
      }
    ): void {
      this.dispatchEvent(new CustomEvent(name, {
        detail: {
          ...detail,
          groupType: group?.type ?? 'unknown',
          timestamp: Date.now(),
          // State AFTER the operation, and never clamped - a clamped reading
          // would disagree with getMetrics() for an over-limit group
          readings: this._readGroup(group, detail.key)
        }
      }));
    }

    /**
     * A pool took or gave back a worker-lifetime holding.
     *
     * `workerType` is closed over from the pool key, which is the only place it
     * exists - a `WorkerManager` does not know its own name.
     */
    private _onResidentNotice(
      workerType: string,
      notice: {
        type: 'acquired' | 'released';
        workerId: string;
        groupId: string;
        cost: number;
        grant?: unknown;
        reason?: ResourceReleaseReason;
      }
    ): void {
      // A worker gave its units back: wake the group's waiters. In a microtask,
      // because the pool notifies from inside its own teardown, and a wake
      // dispatches - possibly into that same pool - before it has finished.
      if (notice.type === 'released') {
        const groupId = notice.groupId;
        queueMicrotask(() => {
          if (!this.shutDown) this._retryBlockedTasksForGroup(groupId);
        });
      }

      if (!this._resourceObserved) return;

      this._emitResource(
        notice.type === 'acquired' ? 'resource.acquired' : 'resource.released',
        this.groups[notice.groupId],
        {
          groupId: notice.groupId,
          lifetime: 'resident',
          holderKind: 'worker',
          holderId: notice.workerId,
          workerType,
          cost: notice.cost,
          ...(notice.grant !== undefined ? { grant: notice.grant } : {}),
          ...(notice.type === 'released' ? { reason: notice.reason } : {})
        }
      );
    }

    /**
     * What a task blocked in `groupId`'s queue is waiting for, and how much.
     *
     * Derived rather than recorded, and used by BOTH the blocked/unblocked
     * events and {@link getAdmissionQueue}, so the two routes acceptance
     * criterion 4b compares cannot describe the same waiter differently.
     *
     * A group among the task's own groups is task-held, because
     * `_checkAndBlockResources` runs and returns before `_checkAndBlockResident`
     * is consulted - so that is the push that put the task in this queue.
     * Anything else is resident admission, costing the pool's per-worker units.
     */
    private _blockedHoldingFor(
      task: FyflowTask,
      pool: any,
      groupId: string
    ): { lifetime: ResourceLifetime; cost: number; request?: unknown } {
      const taskHeld = this._groupsForTask(task, pool).some(entry => entry.id === groupId);
      const request = taskHeld ? task._requests?.[groupId] : pool?.residentRequests?.[groupId];
      const holding: { lifetime: ResourceLifetime; cost: number; request?: unknown } = taskHeld
        ? { lifetime: 'task-held', cost: 1 }
        : { lifetime: 'resident', cost: pool?.residentGroups?.[groupId] ?? 1 };
      if (request !== undefined) holding.request = request;
      return holding;
    }

    _setupGroupEventListeners() {
      // Note: We no longer need event listeners on slot-released because we explicitly call
      // _retryBlockedTasksForGroup() after releasing resources in _releaseResourcesForTask()
    }

    _setupWorkerPoolListeners() {
      // Set up listeners for accurate task state tracking from workers
      Object.values(this.workerPools).forEach((pool: any) => {
        const taskStartedListener = (e: any) => {
          const task = this.tasks.get(e.detail.taskId);
          if (task) {
            // Just emit monitoring event - stats already updated by scheduler.
            // Not `startTime`, which was set at dispatch - this fires when the
            // worker actually picked the task up.
            task.timestamp = Date.now();
            this.dispatchEvent(new CustomEvent('task.running', {detail: task}));
          }
        };
        this._addInternalListener(pool, 'task.started', taskStartedListener);

        // A worker becoming ready frees its slots without anything settling.
        // Tasks that arrived during its start-up saw it as full, and no room
        // for another worker, so they wait in the ready queue: without this
        // they start at the next settle or the heartbeat. A microtask, because
        // the worker clears `initializing` after it emits this.
        const workerReadyListener = () => {
          queueMicrotask(() => { if (!this.shutDown) this._dispatchLoop(); });
        };
        this._addInternalListener(pool, 'worker.initialization.completed', workerReadyListener);

        // Handle progress updates from workers
        const taskProgressListener = (e: any) => {
          const task = this.tasks.get(e.detail.taskId);
          if (task) {
            // Forward progress event with task info
            const eventDetail = {
              ...task,
              progress: e.detail.progress,
              message: e.detail.message,
              details: e.detail.details,
              workerId: e.detail.workerId,
              workerType: e.detail.workerType,
              timestamp: e.detail.timestamp
            };
            this.dispatchEvent(new CustomEvent('task.progress', {detail: eventDetail}));
          }
        };
        this._addInternalListener(pool, 'task.progress', taskProgressListener);

        // Handle spawn task requests from workers
        const taskSpawnListener = (e: any) => {
          const task = this.tasks.get(e.detail.taskId);
          if (task && (task.cancellation || task.state === 'cancelled')) {
            // Refused from a task being cancelled, or that was: the caller asked
            // for this work to stop, and that includes work it would start
            this.dispatchEvent(new CustomEvent('task.spawn_failed', {
              detail: {
                parentTask: task,
                spawnConfig: e.detail.spawnConfig,
                error: new CancelledError(task.id, task.cancellation?.reason),
                workerId: e.detail.workerId,
                workerType: e.detail.workerType,
                timestamp: Date.now()
              }
            }));
            return;
          }
          if (task) {
            // Forward spawn request event with parent task info
            const eventDetail = {
              parentTask: task,
              spawnConfig: e.detail.spawnConfig,
              workerId: e.detail.workerId,
              workerType: e.detail.workerType,
              timestamp: e.detail.timestamp
            };
            this.dispatchEvent(new CustomEvent('task.spawn_request', {detail: eventDetail}));

            // Automatically spawn the task (can be disabled by external listeners)
            const spawnConfig = e.detail.spawnConfig;
            const childTask = new FyflowTask({
              id: spawnConfig.id,
              workerType: spawnConfig.workerType,
              payload: spawnConfig.payload,
              optional: spawnConfig.optional || false,
              retryPolicy: spawnConfig.retryPolicy,
              workerGroups: spawnConfig.workerGroups || []
            });

            // Add the spawned task to the scheduler. This runs inside a worker's
            // event dispatch, so a rejected spawn (e.g. an unknown worker type)
            // must fail only this spawn rather than propagate out and take the
            // scheduler down with it.
            try {
              this.addTask(childTask);
            } catch (error: any) {
              this.dispatchEvent(new CustomEvent('task.spawn_failed', {
                detail: {
                  parentTask: task,
                  spawnConfig,
                  error,
                  workerId: e.detail.workerId,
                  workerType: e.detail.workerType,
                  timestamp: Date.now()
                }
              }));
              return;
            }

            // Record spawn lineage and attach the child to any active descendant
            // trackers. Safe to do after addTask: task completion always resolves
            // through a promise callback, so the child cannot have finished yet.
            this._recordSpawn(task.id, childTask.id);
            this._addSpawnedTaskToTrackers(task.id, childTask);
          }
        };
        this._addInternalListener(pool, 'task.spawn_request', taskSpawnListener);
        
        //TODO: manually added this, verify it makes sense!!!
        const taskRequeueListener = (e: any) => {
          const task = this.tasks.get(e.detail.taskId);
          if (!task || task.state !== 'running') return;
          // Being cancelled: a requeue would contradict cancel wins, and settling
          // now would release its resources while its run may still execute.
          // It settles, as cancelled, when its attempt has actually stopped -
          // the pool rejects its promise once the failed worker is gone.
          if (task.cancellation) return;
          this._settleTask(task, this.workerPools[task.workerType], { requeue: true });
        };
        this._addInternalListener(pool, 'task.requeue_required', taskRequeueListener);

        const taskCompletedListener = (e: any) => {
          const task = this.tasks.get(e.detail.taskId);
          if (task) {
            // NOTE: Don't forward task.completed event here!
            // Worker emits this event BEFORE task state/result are updated.
            // _onTaskComplete (called from promise resolution) handles state updates
            // and emits the proper task.completed event with correct state.
            // Forwarding here would cause duplicate events with stale task state.
            //
            // We only record the worker-measured execution time, which is emitted
            // synchronously by the worker just before it resolves the task promise.
            // _onTaskComplete therefore sees it and includes it on the task it emits.
            if (e.detail.executionTime !== undefined) {
              task.executionTime = e.detail.executionTime;
            }
          }
        };
        this._addInternalListener(pool, 'task.completed', taskCompletedListener);

        const taskFailedListener = (e: any) => {
          const task = this.tasks.get(e.detail.taskId);
          if (!task) return;
          // Being cancelled: as for a requeue, it settles when its attempt has
          // stopped, through the promise path
          if (task.cancellation) return;
          // Same settle routine the promise path uses. For a worker that dies
          // with requeueFailedTasks off this is the only notification the
          // scheduler gets, so it cannot be skipped - and _settleTask makes it
          // harmless when the promise path also fires.
          this._settleTask(task, pool, { ok: false, error: e.detail.error });
        };
        this._addInternalListener(pool, 'task.failed', taskFailedListener);
      });
    }

    // --- Descendant tracking -------------------------------------------------
    // Descendants are tasks created at runtime via context.spawnTask(). This is
    // lineage only: it never influences dispatch order or readiness.

    private static readonly TERMINAL_STATES = new Set(['done', 'failed', 'user_action', 'cancelled']);

    private _isTerminal(state: string): boolean {
      return FyflowScheduler.TERMINAL_STATES.has(state);
    }

    // Record that childId was spawned from parentId
    private _recordSpawn(parentId: string, childId: string) {
      let children = this.spawnedChildren.get(parentId);
      if (!children) {
        children = new Set<string>();
        this.spawnedChildren.set(parentId, children);
      }
      children.add(childId);
    }

    _trackDescendants(task: FyflowTask, resolve: Function, reject: Function) {
      // Seed with the task itself plus every descendant already spawned from it,
      // so tracking can start at any point in the workflow
      const pendingDescendants = new Set<string>();
      this._collectPendingDescendants(task.id, pendingDescendants, new Set<string>());

      // Nothing left to wait for - the workflow has already settled
      if (pendingDescendants.size === 0) {
        this._settleTracker({ rootId: task.id, rootTask: task, resolve, reject, pendingDescendants });
        return;
      }

      this.descendantTrackers.set(this.nextTrackerId++, {
        rootId: task.id,
        rootTask: task,
        resolve,
        reject,
        pendingDescendants
      });
    }

    private _collectPendingDescendants(taskId: string, pending: Set<string>, visited: Set<string>) {
      if (visited.has(taskId)) return;
      visited.add(taskId);

      const task = this.tasks.get(taskId);
      if (task && !this._isTerminal(task.state)) {
        pending.add(taskId);
      }

      const children = this.spawnedChildren.get(taskId);
      if (children) {
        for (const childId of children) {
          this._collectPendingDescendants(childId, pending, visited);
        }
      }
    }

    // Called when a task reaches a terminal state (done, failed or user_action).
    // Does nothing unless maxCompletedTasks is configured.
    private _recordTerminalTask(taskId: string) {
      const limit = this.options.maxCompletedTasks;
      if (limit === undefined) return; // Default: retain everything

      this.completedTaskIds.add(taskId);

      // Evict oldest-first until back within the limit. Trackers hold their root
      // task by reference, so eviction cannot affect an in-flight
      // onCompleteDescendants() wait.
      while (this.completedTaskIds.size > limit) {
        const oldest = this.completedTaskIds.values().next().value;
        if (oldest === undefined) break;
        this.completedTaskIds.delete(oldest);
        this.tasks.delete(oldest);
        this.spawnedChildren.delete(oldest);
      }
    }

    // Called when a task reaches a terminal state (done, failed or user_action)
    private _settleDescendantTrackers(taskId: string) {
      if (this.descendantTrackers.size === 0) return; // Fast path - feature unused

      for (const [trackerId, tracker] of this.descendantTrackers) {
        // Only trackers actually waiting on this task are affected
        if (!tracker.pendingDescendants.delete(taskId)) continue;
        if (tracker.pendingDescendants.size > 0) continue;

        this.descendantTrackers.delete(trackerId);
        this._settleTracker(tracker);
      }
    }

    private _settleTracker(tracker: DescendantTracker) {
      const rootTask = tracker.rootTask;

      // Descendant failures do not reject - they surface via task.failed events -
      // but the tracked task failing rejects, matching onCompletePromise()
      if (rootTask.state === 'failed' || rootTask.state === 'user_action') {
        tracker.reject(new Error(rootTask.error || `Task ${tracker.rootId} failed`));
        return;
      }
      // Cancelling the root is the caller's own act, reported as such
      if (rootTask.state === 'cancelled') {
        tracker.reject(new CancelledError(tracker.rootId, rootTask.cancellation?.reason));
        return;
      }

      tracker.resolve(rootTask.result ?? null);
    }

    private _addSpawnedTaskToTrackers(parentTaskId: string, spawnedTask: FyflowTask) {
      if (this.descendantTrackers.size === 0) return; // Fast path - feature unused

      for (const tracker of this.descendantTrackers.values()) {
        // Track the child if its parent is the tracked root or is itself tracked
        if (tracker.rootId === parentTaskId || tracker.pendingDescendants.has(parentTaskId)) {
          if (!this._isTerminal(spawnedTask.state)) {
            tracker.pendingDescendants.add(spawnedTask.id);
          }
        }
      }
    }

    // Shutdown the scheduler and cleanup all resources
    /**
     * Get current metrics for all resource groups
     *
     * Returns real-time utilization and availability for monitoring
     *
     * @returns Record of group ID to metrics
     */
    getResourceMetrics(): Record<string, any> {
      const metrics: Record<string, any> = {};
      for (const [id, group] of Object.entries(this.groups)) {
        if (group && typeof group.getMetrics === 'function') {
          metrics[id] = group.getMetrics();
        }
      }
      return metrics;
    }

    /**
     * Every task currently waiting on a resource group, in admission order.
     *
     * The queue already exists as `blockedQueues`; this makes it readable. Its
     * head is what explains a stall - a 20-unit pool sitting at position 0 is
     * the entire reason four 2-unit pools are not starting, because nothing
     * behind it may overtake.
     *
     * Keyed by **group id**, with the bucket on the waiter, so the internal
     * composite queue id does not leak. A keyed group has one sub-queue per
     * key; they are concatenated in key order, each with its own positions, so
     * the result is deterministic and reproducible from the
     * `resource.blocked` / `resource.unblocked` stream.
     *
     * Cost is `O(total blocked tasks)` and it is not capped - the panel this
     * exists for is the starvation case, which is exactly when the queue is
     * long, and a cap would be a lie with a smaller allocation. Pass `groupId`
     * to narrow it.
     *
     * With `resource.blocked` / `resource.unblocked` shipping, this is for
     * late-joiner catch-up rather than per-frame polling: two reads describe
     * two instants, and a view built from them is not a fold.
     */
    getAdmissionQueue(groupId?: string): Record<string, AdmissionWaiter[]> {
      // Collected per (group, key) first so sub-queues can be ordered by key
      const byGroup = new Map<string, Array<{ key?: string; suffix?: string; waiters: AdmissionWaiter[] }>>();

      for (const [queueId, queue] of this.blockedQueues) {
        if (queue.length === 0) continue;

        const { groupId: id, key, suffix } = this._splitQueueId(queueId);
        if (groupId !== undefined && id !== groupId) continue;

        const waiters = queue.toArray().map((task, position) => {
          const pool = this.workerPools[task.workerType];
          const waiter: AdmissionWaiter = {
            holderId: task.id,
            workerType: task.workerType,
            // Same derivation the blocked/unblocked events use, so the two
            // routes cannot describe the same waiter differently
            ...this._blockedHoldingFor(task, pool, id),
            position
          };
          if (key !== undefined) waiter.key = key;
          return waiter;
        });

        const subQueues = byGroup.get(id) ?? [];
        subQueues.push({ key, suffix, waiters });
        byGroup.set(id, subQueues);
      }

      const admission: Record<string, AdmissionWaiter[]> = {};
      for (const [id, subQueues] of byGroup) {
        // Map iteration order is queue-creation order, which a consumer folding
        // the stream cannot reproduce. Key order can be.
        subQueues.sort((a, b) => (a.suffix ?? '').localeCompare(b.suffix ?? ''));
        admission[id] = subQueues.flatMap(sub => sub.waiters);
      }
      return admission;
    }

    /**
     * Get lifetime stats for resource groups
     *
     * Returns aggregated statistics for strict groups (acquisition times, rejections, etc.)
     *
     * @returns Record of group ID to stats (only strict groups provide stats)
     */
    getResourceStats(): Record<string, any> {
      const stats: Record<string, any> = {};
      for (const [id, group] of Object.entries(this.groups)) {
        if (group && typeof group.getStats === 'function') {
          stats[id] = group.getStats();
        }
      }
      return stats;
    }

    /**
     * Wait for running tasks, then terminate every worker pool, drop all
     * listeners and clear internal state.
     *
     * Always call this when finished - live workers and the periodic retry timer
     * will otherwise keep the process alive. Pending `onCompleteDescendants()`
     * waits reject.
     */
    async shutdown(): Promise<void> {

      // 1. Wait for any currently running tasks to complete. Draining is
      // dispatching: each settle starts the next outstanding task, so the pools
      // stay heard and the periodic retry stays armed until it is done. Their
      // listeners used to be removed first, which silently dropped everything
      // the pools reported meanwhile - task.running, progress, spawned children
      // and requeues alike.
      // Bounded: a task dispatched to a pool whose workers can no longer be
      // created never settles, and an unbounded wait turns that into a hang with
      // no diagnostic. Workers are terminated below regardless.
      const drainDeadline = Date.now() + 5000;
      while (this.stats.running > 0 && Date.now() < drainDeadline) {
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      if (this.stats.running > 0) {
        // A task being cancelled is not stuck in a broken pool: it is a worker
        // that has not stopped yet, and the shutdown is what ends it
        let stopping = 0;
        for (const task of this.tasks.values()) {
          if (task.state === 'running' && task.cancellation) stopping++;
        }
        const stuck = this.stats.running - stopping;
        if (stuck > 0) {
          console.warn(
            `⚠️ Shutdown proceeding with ${stuck} task(s) still marked running - ` +
            `they were dispatched to a pool that could not complete them`
          );
        }
        if (stopping > 0) {
          console.warn(
            `⚠️ Shutdown proceeding with ${stopping} task(s) still being cancelled - ` +
            `a threaded one ends cancelled with its worker, an inline one's cancel() resolves false`
          );
        }
      }

      // 2. Stop retrying, and stop listening to the pools before they are torn
      // down - otherwise the process stays alive. A backoff that ended during
      // the drain has dispatched its retry like any other work; one still
      // running is discarded with the queues, so it cannot fire into a pool
      // that no longer exists.
      this._clearPeriodicRetry();
      for (const { timer } of this.retryWaiting.values()) clearTimeout(timer);
      this.retryWaiting.clear();
      this._removeAllListeners();

      // 3. Clear all queues
      this.readyQueuesByWorker.forEach(q => q.clear());

      // Exit (d). Every remaining waiter is discarded, so each gets its
      // unblocked - without them a run that shuts down with tasks still blocked
      // leaves the fold holding a queue that never drains.
      if (this._resourceObserved) {
        for (const [queueId, queue] of this.blockedQueues) {
          const { groupId, key } = this._splitQueueId(queueId);
          const group = this.groups[groupId];
          for (const task of queue) {
            const pool = this.workerPools[task.workerType];
            this._emitResource('resource.unblocked', group, {
              groupId,
              ...this._blockedHoldingFor(task, pool, groupId),
              holderKind: 'task',
              holderId: task.id,
              workerType: task.workerType,
              key,
              reason: 'shutdown'
            });
          }
        }
      }
      this.blockedQueues.clear();
      this.blockedKeys.clear();
      this.blockedLive.live = 0;
      for (const task of this.tasks.values()) task._queuedIn = undefined;

      // 4. Shutdown all WorkerManager instances
      const workerManagerPromises = Object.values(this.workerPools).map(async (pool: any) => {
        if (pool && typeof pool.shutdown === 'function') {
          try {
            await pool.shutdown();
          } catch (error) {
            console.warn(`⚠️ Error shutting down worker pool:`, error);
          }
        }
      });

      let i = 0;
      for (const workerManagerPromise of workerManagerPromises) {
        await workerManagerPromise;
        i++;
      }

      // await Promise.all(workerManagerPromises);


      // A task still being cancelled. On a threaded pool its worker has just been
      // terminated, so it has really stopped: it ends cancelled, forced. An
      // inline run cannot be stopped and may yet end, so its cancel() resolves
      // false rather than leaving the caller waiting forever.
      for (const task of this.tasks.values()) {
        if (!task._cancelPending || task.state !== 'running') continue;
        const pool = this.workerPools[task.workerType];
        if (pool && !pool.inline) {
          task._settling = true;
          task.cancellation!.forced = true;
          this._releaseResourcesForTask(task, pool);
          this._settleCancelled(task);
        } else {
          const pending = task._cancelPending;
          task._cancelPending = undefined;
          pending.resolve(false);
        }
      }
      // Nothing settles from here on
      this.shutDown = true;

      // 5. Reject outstanding descendant waits - clearing them silently would
      // leave callers awaiting a promise that can never settle
      for (const tracker of this.descendantTrackers.values()) {
        tracker.reject(new Error('Scheduler shut down before all descendants completed'));
      }

      // 6. Clear all internal state
      this.tasks.clear();
      this.descendantTrackers.clear();
      this.spawnedChildren.clear();
      this.completedTaskIds.clear();
      this.stats = {queued: 0, running: 0, done: 0, failed: 0, cancelled: 0};

    }
  }
  