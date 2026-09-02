import { ThreadWrapper } from "./threadWrapper.ts";
import { InlineWrapper } from "./inlineWrapper.ts";
import { WorkerStatus } from "./workerInterface.ts";

type WorkerInstance = ThreadWrapper | InlineWrapper;

/**
 * A resident holding taken or returned by a pool.
 *
 * The pool reports it; the **scheduler** turns it into a `resource.*` event and
 * is the sole emitter. See `core/resourceEvents.ts` for why a pool-level mirror
 * would be a double count rather than a convenience.
 */
export type ResidentResourceNotice =
  | { type: 'acquired'; workerId: string; groupId: string; cost: number }
  | {
      type: 'released';
      workerId: string;
      groupId: string;
      cost: number;
      reason: 'worker-teardown' | 'shutdown';
    };

/** Injected by the scheduler at {@link WorkerManager._bindResidentGroups}. */
export type ResidentResourceNotifier = (notice: ResidentResourceNotice) => void;

export interface WorkerManagerOptions {
  /**
   * Maximum worker instances in this pool. Default 2.
   *
   * Despite the name this is a count of worker *instances*, not OS threads.
   * With `inline: false` each instance is a real worker thread. With
   * `inline: true` they all live in the main process, and the name is a
   * misnomer - see {@link WorkerManagerOptions.inline}.
   */
  maxThreads?: number;
  /**
   * Tasks each worker instance may run at once. Default 1. Total pool capacity
   * is `maxThreads x maxConcurrentTasks` for both pool types. Raise this for
   * async/IO workers; leave it at 1 for CPU-bound ones.
   */
  maxConcurrentTasks?: number;
  /** Ms an idle worker is kept before termination. Default 5000. `0` never terminates. */
  idleTimeout?: number;
  /**
   * How often idle workers are swept for termination, in ms. Default 5000.
   *
   * This is a floor on how promptly a worker actually goes away, independent of
   * `idleTimeout` - an `idleTimeout` of 50ms still means up to a 5s wait on the
   * default sweep. That matters for {@link WorkerManagerOptions.residentGroups},
   * where the sweep is what hands a held resource to a pool waiting for it.
   */
  idleCheckIntervalMs?: number;
  /**
   * Run workers in the main process instead of worker threads. Default false.
   * Inline suits async/IO work; threaded suits CPU-bound work, since an inline
   * worker shares the event loop and a synchronous task blocks the scheduler.
   *
   * For an inline pool, `maxThreads` creates that many instances of your worker
   * class in the same process - no threads are involved. Concurrency comes from
   * overlapping `await`s, so only the PRODUCT matters for throughput:
   * `maxThreads: 4, maxConcurrentTasks: 5` and
   * `maxThreads: 1, maxConcurrentTasks: 20` both cap at 20 in-flight tasks and
   * measure the same.
   *
   * What the split does change is state isolation: each instance is constructed
   * separately, so 4 instances means 4 connection pools, caches or whatever
   * per-instance state your worker holds, while 1 instance funnels all 20
   * concurrent tasks through one object.
   */
  inline?: boolean;
  /** Passed as the first constructor argument to every worker instance in this pool. */
  config?: any;
  /** Resource group ids every task in this pool must acquire before running. */
  groups?: string[];
  /**
   * Groups held for a **worker's whole lifetime**, from creation until teardown,
   * rather than for the duration of a task.
   *
   * For resources a worker holds because it exists - a model loaded on a GPU in
   * `setup()`, a database connection, a licence seat. A task-scoped group cannot
   * express these: it is released when the task settles, while the worker lives
   * on until its idle timeout, so a second worker can take the resource while
   * the first still holds it.
   *
   * ```typescript
   * residentGroups: ['vram']       // one unit per worker
   * residentGroups: { vram: 20 }   // this pool's model needs 20 units per worker
   * ```
   *
   * Cost is per worker, not per pool: `maxThreads: 4` with `{ vram: 2 }` uses 8
   * units at full spread. A worker is not created while its cost does not fit,
   * and tasks needing one wait until a holder is torn down.
   */
  residentGroups?: string[] | Record<string, number>;
  /** Requeue a worker's in-flight tasks when it fails, rather than failing them. Default true. */
  requeueFailedTasks?: boolean;
  /** Restarts allowed before the pool gives up and emits `worker.restart_limit_exceeded`. Default 3. */
  maxWorkerRestarts?: number;
}

/**
 * A pool of workers of one type, created from a single worker script.
 *
 * ```typescript
 * const pool = new WorkerManager(workerUrl, {
 *   maxThreads: 4,
 *   maxConcurrentTasks: 1,
 *   groups: ['cpu']
 * });
 * ```
 *
 * Emits: `task.started`, `task.completed`, `task.failed`, `task.progress`,
 * `task.spawn_request`, `task.requeue_required`, `worker.failed`,
 * `worker.self_terminated`, `worker.restart_limit_exceeded`, and the worker
 * lifecycle events `worker.initialization.started|completed|failed`,
 * `worker.setup.started|completed` and
 * `worker.teardown.started|completed|failed`.
 *
 * Lifecycle events carry `{ workerId, workerType, timestamp }` plus `duration`
 * on `*.completed` and `error` on `*.failed`. They fire on every worker
 * creation and idle-timeout teardown, so keep their listeners cheap.
 *
 * Workers are created lazily on first use, up to `maxThreads`.
 */
export class WorkerManager extends EventTarget {
  threads: WorkerInstance[] = [];
  maxThreads: number;
  maxConcurrentTasks: number;
  scriptUrl: string;
  idleTimeout: number;
  inline: boolean;
  config: any;
  groups: string[];
  requeueFailedTasks: boolean;
  maxWorkerRestarts: number;

  // Track restart count for entire pool (simpler and more logical)
  private poolRestartCount = 0;
  // Set for the duration of shutdown, so worker failures during teardown are not
  // mistaken for crashes worth restarting
  private shuttingDown = false;

  // Track ALL event listeners for cleanup
  private allListeners = new Map<any, {event: string; listener: Function}[]>();

  // Track initializing threads
  private initializingThreads = new Set<WorkerInstance>();

  /** Group id -> units one worker of this pool holds while it exists. */
  residentGroups: Record<string, number>;
  /**
   * The group objects behind {@link residentGroups}, injected by the scheduler -
   * pools are constructed with ids, and only the scheduler owns the registry.
   * Empty until then, which makes resident groups inert for a standalone pool.
   */
  private residentRegistry: Record<string, any> = {};
  /**
   * Ids of workers whose resident cost this pool holds. Keyed by worker rather
   * than counted, so releasing is idempotent: a failed worker is released by
   * `_releaseWorkerGroupResources` while it is still in `threads`, and must not
   * be released a second time if the idle sweep also reaches it.
   */
  private residentHolders = new Set<string>();
  /** Acquired, but the worker object does not exist yet. Same synchronous turn. */
  private residentPending = 0;
  /**
   * Injected alongside the registry. Undefined for a standalone pool, and for
   * one the scheduler bound before this existed.
   */
  private residentNotify?: ResidentResourceNotifier;

  private static _normalizeResidentGroups(
    declared: string[] | Record<string, number> | undefined
  ): Record<string, number> {
    if (!declared) return {};
    const costs: Record<string, number> = Array.isArray(declared)
      ? Object.fromEntries(declared.map(id => [id, 1]))
      : { ...declared };
    for (const [id, cost] of Object.entries(costs)) {
      if (!Number.isInteger(cost) || cost <= 0) {
        throw new Error(
          `residentGroups['${id}'] must be a positive integer, got ${cost}`
        );
      }
    }
    return costs;
  }

  /**
   * Called by the scheduler once, with the group registry. Also the point where
   * a cost larger than its group's limit is rejected: such a pool could never
   * start a worker, and failing here beats blocking forever at runtime.
   */
  _bindResidentGroups(registry: Record<string, any>, notify?: ResidentResourceNotifier) {
    for (const [id, cost] of Object.entries(this.residentGroups)) {
      const group = registry[id];
      if (!group) throw new Error(`Unknown resident group: ${id}`);
      const limit = group.getMetrics?.().limit;
      if (typeof limit === 'number' && cost > limit) {
        throw new Error(
          `residentGroups['${id}'] cost ${cost} exceeds the group limit of ${limit} - ` +
          `a worker in this pool could never start`
        );
      }
    }
    this.residentRegistry = registry;
    this.residentNotify = notify;
  }

  /** True when every resident group has room for one more worker of this pool. */
  canHoldAnotherWorker(): boolean {
    return Object.entries(this.residentGroups).every(([id, cost]) => {
      const group = this.residentRegistry[id];
      return !group || group.canRun(undefined, cost);
    });
  }

  /**
   * Take one worker's resident cost from every group, or nothing at all.
   * Synchronous, and called in the same turn as the capacity check, so no other
   * pool can interleave between the two.
   */
  private _acquireResident(): boolean {
    if (!this.canHoldAnotherWorker()) return false;
    for (const [id, cost] of Object.entries(this.residentGroups)) {
      this.residentRegistry[id]?.onStart?.(undefined, cost);
    }
    this.residentPending++;
    return true;
  }

  /**
   * Attach a just-acquired holding to the worker it was taken for.
   *
   * Also where the acquire is announced, rather than in
   * {@link _acquireResident}: the worker id does not exist yet at acquire time,
   * and both call sites bind in the same synchronous turn. A holding that is
   * acquired and never bound is a bug, not a state worth modelling.
   */
  private _bindResidentHolder(workerId: string) {
    if (this.residentPending === 0) return;
    this.residentPending--;
    this.residentHolders.add(workerId);

    if (!this.residentNotify) return;
    for (const [id, cost] of Object.entries(this.residentGroups)) {
      // Only groups the registry actually knows were charged by _acquireResident
      if (this.residentRegistry[id]) {
        this.residentNotify({ type: 'acquired', workerId, groupId: id, cost });
      }
    }
  }

  /** Give back one worker's resident cost. No-op if it holds none. */
  private _releaseResidentFor(
    workerId: string,
    reason: 'worker-teardown' | 'shutdown' = 'worker-teardown'
  ) {
    if (!this.residentHolders.delete(workerId)) return;
    for (const [id, cost] of Object.entries(this.residentGroups)) {
      const group = this.residentRegistry[id];
      if (!group) continue;
      group.onFinish?.(undefined, cost);
      this.residentNotify?.({ type: 'released', workerId, groupId: id, cost, reason });
    }
  }

  /** Give everything back, holders and any unbound acquisition. */
  private _releaseAllResident() {
    for (const workerId of [...this.residentHolders]) {
      this._releaseResidentFor(workerId, 'shutdown');
    }
    // An unbound acquisition never announced itself - emitting a release for it
    // would leave the fold one release ahead of its acquires forever
    while (this.residentPending > 0) {
      this.residentPending--;
      for (const [id, cost] of Object.entries(this.residentGroups)) {
        this.residentRegistry[id]?.onFinish?.(undefined, cost);
      }
    }
  }

  /** Units this pool currently holds in each resident group. Test/diagnostic. */
  getResidentUsage(): Record<string, number> {
    const held = this.residentHolders.size + this.residentPending;
    const usage: Record<string, number> = {};
    for (const [id, cost] of Object.entries(this.residentGroups)) {
      usage[id] = cost * held;
    }
    return usage;
  }

  // Centralized idle management
  private idleCheckTimer: ReturnType<typeof setInterval> | null = null;
  private readonly idleCheckIntervalMs: number;

  constructor(scriptUrl: string, options: WorkerManagerOptions = {}) {
    super();
    this.scriptUrl = scriptUrl;
    this.maxThreads = options.maxThreads ?? 2;
    this.maxConcurrentTasks = options.maxConcurrentTasks ?? 1;
    this.idleTimeout = options.idleTimeout ?? 5000;
    this.idleCheckIntervalMs = options.idleCheckIntervalMs ?? 5000;
    this.inline = options.inline ?? false;
    this.config = options.config ?? {};
    this.groups = options.groups ?? [];
    this.residentGroups = WorkerManager._normalizeResidentGroups(options.residentGroups);
    this.requeueFailedTasks = options.requeueFailedTasks ?? true; // Default to requeue as specified
    this.maxWorkerRestarts = options.maxWorkerRestarts ?? 3; // Default: 3 restarts per worker

    // Set up worker failure handling
    this._setupWorkerFailureHandling();
  }

  // Direct execution without internal queueing
  async enqueueNoQueue(taskId: string, payload: any): Promise<any> {
    // Find an available thread
    let targetThread = this.threads.find(t =>
      t.canAcceptTask && t.canAcceptTask()
    );

    // If no available thread, create one if under limit
    // Note: initializingThreads tracks threads being created to prevent race conditions
    if (!targetThread && (this.threads.length + this.initializingThreads.size) < this.maxThreads
        && this._acquireResident()) {
      const newThread = this.inline
        ? new InlineWrapper(this.scriptUrl, this.idleTimeout, this.maxConcurrentTasks, this.config)
        : new ThreadWrapper(this.scriptUrl, this.idleTimeout, this.maxConcurrentTasks, this.config);

      this._bindResidentHolder(newThread.id);

      // Track as initializing to prevent other tasks from creating duplicate threads
      this.initializingThreads.add(newThread);

      // Set up event listeners
      this._setupWorkerEventListeners(newThread);

      // Add to threads array and start idle timer
      this.threads.push(newThread);
      this.startIdleTimer();

      // For inline workers, thread is ready immediately
      if (this.inline) {
        this.initializingThreads.delete(newThread);
        targetThread = newThread;
      } else {
        // For threaded workers, wait for initialization
        await new Promise<void>((resolve) => {
          const checkInit = () => {
            if (newThread.canAcceptTask && newThread.canAcceptTask()) {
              this.initializingThreads.delete(newThread);
              resolve();
            } else {
              setTimeout(checkInit, 10); // Poll every 10ms
            }
          };
          checkInit();
        });
        targetThread = newThread;
      }
    }

    if (!targetThread) {
      // Also the path taken when a resident group had no room for another
      // worker. The scheduler blocks such tasks before dispatch, so reaching
      // here means capacity went away in between - requeue rather than fail.
      throw new Error(`No worker thread available (${this.threads.length}/${this.maxThreads})`);
    }

    // Execute task directly on thread
    return targetThread.runTask(taskId, payload);
  }

  private _createPredictiveThread(): WorkerInstance | null {
    // A replacement worker still has to fit in the resident groups.
    if (!this._acquireResident()) return null;

    const thread = this.inline
      ? new InlineWrapper(this.scriptUrl, this.idleTimeout, this.maxConcurrentTasks, this.config)
      : new ThreadWrapper(this.scriptUrl, this.idleTimeout, this.maxConcurrentTasks, this.config);

    this._bindResidentHolder(thread.id);

    // Set up event listeners first
    this._setupWorkerEventListeners(thread);

    // Add worker to pool and start idle timer if needed
    this.threads.push(thread);
    this.startIdleTimer(); // Start idle timer when first worker is added
    return thread;
  }

  // Override addEventListener to track ALL listeners for cleanup
  //TODO: likely unneccesary, verify
  override addEventListener(event: string, listener: EventListenerOrEventListenerObject, options?: boolean | AddEventListenerOptions): void {
    super.addEventListener(event, listener, options);
    this._trackListener(this, event, listener as Function);
  }

  // Helper to track listeners for cleanup
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

  // Remove ALL event listeners (both internal and external)
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

  private _setupWorkerEventListeners(worker: WorkerInstance) {
    // Track all worker listeners for cleanup
    const listeners = [
      { event: 'task.started', handler: (e: any) => this.dispatchEvent(new CustomEvent('task.started', { detail: e.detail })) },
      { event: 'task.completed', handler: (e: any) => this.dispatchEvent(new CustomEvent('task.completed', { detail: e.detail })) },
      { event: 'task.failed', handler: (e: any) => this.dispatchEvent(new CustomEvent('task.failed', { detail: e.detail })) },
      { event: 'task.progress', handler: (e: any) => this.dispatchEvent(new CustomEvent('task.progress', { detail: e.detail })) },
      { event: 'task.spawn_request', handler: (e: any) => this.dispatchEvent(new CustomEvent('task.spawn_request', { detail: e.detail })) },
      { event: 'worker.failed', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.failed', { detail: e.detail })) },
      // Lifecycle events - forwarded so pool owners can observe worker startup
      // cost, setup failures and teardown problems. Both wrapper types emit the
      // same set with the same payload shape.
      { event: 'worker.initialization.started', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.initialization.started', { detail: e.detail })) },
      { event: 'worker.initialization.completed', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.initialization.completed', { detail: e.detail })) },
      { event: 'worker.initialization.failed', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.initialization.failed', { detail: e.detail })) },
      { event: 'worker.setup.started', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.setup.started', { detail: e.detail })) },
      { event: 'worker.setup.completed', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.setup.completed', { detail: e.detail })) },
      { event: 'worker.setup.failed', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.setup.failed', { detail: e.detail })) },
      { event: 'worker.teardown.started', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.teardown.started', { detail: e.detail })) },
      { event: 'worker.teardown.completed', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.teardown.completed', { detail: e.detail })) },
      { event: 'worker.teardown.failed', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.teardown.failed', { detail: e.detail })) },
      { event: 'worker.self_terminated', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.self_terminated', { detail: e.detail })) },
      { event: 'worker.termination_requested', handler: (e: any) => this._handleWorkerTerminationRequest(e) }
    ];

    // Add all listeners and track them
    for (const {event, handler} of listeners) {
      this._addInternalListener(worker, event, handler);
    }
  }

  // Handle worker termination requests (new event-driven approach)
  private _handleWorkerTerminationRequest(e: any) {
    const { workerId, metadata } = e.detail;

    // Convert termination request to worker failure for unified handling
    const failureEvent = {
      detail: {
        workerId,
        error: new Error('Worker termination requested'),
        metadata: {
          failureType: 'runtime',
          ...metadata
        },
        timestamp: Date.now()
      }
    };

    // Use existing worker failure handling logic
    this._handleWorkerFailure(failureEvent);
  }

  // Worker failure handling setup (as specified in task)
  private _setupWorkerFailureHandling() {
    // Handle worker.failed events from all workers
    const failureListener = (e: any) => this._handleWorkerFailure(e);
    this._addInternalListener(this, 'worker.failed', failureListener);
  }

  // Unified worker failure handling (as specified in task)
  private _handleWorkerFailure(e: any) {
    // A worker "failing" while we are tearing the pool down is expected
    if (this.shuttingDown) return;

    const { workerId, error, metadata } = e.detail;
    const { canRestart, restartDelay } = metadata || {};
    const failedWorker = this.threads.find(w => w.id === workerId);

    if (!failedWorker) return;

    // CRITICAL: Requeue any in-progress tasks (Worker failure ≠ Task failure)
    const runningTaskIds = failedWorker.getRunningTaskIds();
    runningTaskIds.forEach(taskId => {
      // Only requeue if both manager allows requeuing AND worker can be restarted
      if (this.requeueFailedTasks && canRestart !== false) {
        // Emit task.requeue_required event for FyflowScheduler to handle
        this.dispatchEvent(new CustomEvent('task.requeue_required', {
          detail: { taskId, originalError: error, workerId }
        }));
      } else {
        // Mark tasks as failed instead of requeuing
        this.dispatchEvent(new CustomEvent('task.failed', {
          detail: {
            taskId,
            workerId,
            error: new Error(`Worker failed: ${error.message}`),
            timestamp: Date.now()
          }
        }));
      }
    });

    // CRITICAL: Mark as failed but keep maxThreads slot (don't remove immediately)
    this._markWorkerAsFailed(workerId, error, metadata);

    // CRITICAL: Release group resources immediately (can't process tasks)
    this._releaseWorkerGroupResources(workerId);

    // Replace unless the failure explicitly forbade it.
    //
    // This used to require `canRestart === true`, while the requeue decision
    // above uses `canRestart !== false`. Failures that set no canRestart at all -
    // which is every construction failure - fell through both branches: no
    // replacement was scheduled AND worker.restart_limit_exceeded was never
    // emitted, so an unstartable pool kept a dead worker in its maxThreads slot
    // and stayed silent to anyone monitoring that event.
    if (canRestart !== false) {
      if (this.poolRestartCount < this.maxWorkerRestarts) {
        this.poolRestartCount++;
        const delay = restartDelay || 5000; // Default 5 second delay
        setTimeout(() => {
          this._removeAndReplaceWorker(workerId);
        }, delay);
      } else {
        // Pool restart limit exceeded - no more restarts for this pool
        this.dispatchEvent(new CustomEvent('worker.restart_limit_exceeded', {
          detail: {
            workerId,
            poolRestartCount: this.poolRestartCount,
            maxRestarts: this.maxWorkerRestarts
          }
        }));
      }
    }
  }

  // Resource allocation implementation methods (as specified in task)
  private _markWorkerAsFailed(workerId: string, error: Error, metadata: any) {
    const worker = this.threads.find(w => w.id === workerId);
    if (worker) {
      // Check if worker is already terminated to avoid double termination
      const alreadyTerminated = (worker as any).state === 'terminated';

      // Mark as failed but keep in threads array (maintains maxThreads slot)
      (worker as any).state = 'failed';
      (worker as any).lastError = { timestamp: Date.now(), message: error.message, metadata };

      // Terminate the actual worker process/thread but keep tracking (if not already terminated)
      if (!alreadyTerminated) {
        worker.terminate().catch(() => {
          // Worker may already be terminated - ignore errors
        });
      }

      // Worker can no longer accept tasks but stays in pool count
      // This is handled by the updated canAcceptTask() method in wrappers
    }
  }

  private _releaseWorkerGroupResources(workerId: string) {
    // A dead worker holds nothing. Its resident units must come back or the
    // group leaks capacity permanently and every pool sharing it stalls.
    this._releaseResidentFor(workerId);
  }

  private _removeAndReplaceWorker(workerId: string) {
    const workerIndex = this.threads.findIndex(w => w.id === workerId);
    if (workerIndex !== -1) {
      // NOW we actually remove from pool and allow replacement
      this.threads.splice(workerIndex, 1);

      if (this.threads.length < this.maxThreads) {
        this._createPredictiveThread();
        // Pool restart count persists across worker replacements
      }
    }
  }

  // Worker Status Inspection APIs (as specified in task)
  /** Ids of the workers that currently exist in this pool (created lazily). */
  getWorkerIds(): string[] {
    return this.threads.map(w => w.id);
  }

  /**
   * Health and activity of one worker, or `null` if no such worker exists.
   * Useful for building supervision on top of `worker.failed`.
   */
  getWorkerStatus(workerId: string): WorkerStatus | null {
    const worker = this.threads.find(w => w.id === workerId) as any;
    if (!worker) return null;

    return {
      id: worker.id,
      state: worker.state || 'healthy',
      tasksCompleted: worker.tasksCompleted || 0,
      errorCount: worker.errorCount || 0,
      uptime: Date.now() - (worker.createdAt || Date.now()),
      currentTasks: worker.getRunningTaskIds(),
      lastError: worker.lastError,
      resourcesHeld: this.groups || [] // Groups this worker is assigned to
    };
  }

  /** `getWorkerStatus` for every live worker, keyed by worker id. */
  getAllWorkerStatuses(): Map<string, WorkerStatus> {
    const statuses = new Map<string, WorkerStatus>();
    this.threads.forEach(worker => {
      const status = this.getWorkerStatus(worker.id);
      if (status) {
        statuses.set(worker.id, status);
      }
    });
    return statuses;
  }

  // Worker Management APIs (as specified in task)
  /**
   * Terminate a worker and create a replacement, optionally merging `newConfig`
   * over the pool's `config`. The replacement gets a new id.
   *
   * @returns false if no worker with that id exists.
   */
  async restartWorker(workerId: string, newConfig?: any): Promise<boolean> {
    const workerIndex = this.threads.findIndex(w => w.id === workerId);
    if (workerIndex === -1) return false;

    const oldWorker = this.threads[workerIndex];
    await oldWorker.terminate();
    this.threads.splice(workerIndex, 1);

    // Create new worker with updated config
    const config = { ...this.config, ...(newConfig || {}) };
    const newWorker = this.inline
      ? new InlineWrapper(this.scriptUrl, this.idleTimeout, this.maxConcurrentTasks, config)
      : new ThreadWrapper(this.scriptUrl, this.idleTimeout, this.maxConcurrentTasks, config);

    this._setupWorkerEventListeners(newWorker);
    this.threads.push(newWorker);
    this.startIdleTimer(); // Ensure idle timer is running

    return true;
  }

  /** Alias for {@link WorkerManager.restartWorker}. */
  async replaceWorker(workerId: string, newConfig?: any): Promise<boolean> {
    // Same as restart for now
    return await this.restartWorker(workerId, newConfig);
  }

  /**
   * Merge `config` into a live worker's config object.
   *
   * Note this mutates the config a worker instance already holds - a worker that
   * copied values in its constructor will not see the change. Use
   * {@link WorkerManager.restartWorker} when the worker must be rebuilt.
   *
   * @returns false if no worker with that id exists.
   */
  async updateWorkerConfig(workerId: string, config: any): Promise<boolean> {
    const worker = this.threads.find(w => w.id === workerId) as any;
    if (!worker) return false;

    // Update the worker's config
    worker.config = { ...worker.config, ...config };

    // For thread workers, we would need to send a config update message
    // For inline workers, the config is already updated
    // Implementation depends on whether workers support hot config reload

    return true;
  }

  // Centralized idle management methods
  private startIdleTimer(): void {
    if (this.idleCheckTimer || this.threads.length === 0) return;

    this.idleCheckTimer = setInterval(() => {
      this.checkIdleWorkers();
    }, this.idleCheckIntervalMs);
  }

  private stopIdleTimer(): void {
    if (this.idleCheckTimer) {
      clearInterval(this.idleCheckTimer);
      this.idleCheckTimer = null;
    }
  }

  private checkIdleWorkers(): void {
    const now = Date.now();
    const workersToTerminate: WorkerInstance[] = [];

    for (const worker of this.threads) {
      // Skip persistent workers (idleTimeout: 0)
      if (worker.idleTimeout > 0 && worker.idle && (now - worker.lastActivityTime) > worker.idleTimeout) {
        workersToTerminate.push(worker);
      }
    }

    // Terminate idle workers (non-blocking)
    for (const worker of workersToTerminate) {
      this._removeIdleWorker(worker).catch(error => {
        console.warn(`Failed to remove idle worker ${worker.id}:`, error);
      });
    }

    // Stop timer if no workers remain
    if (this.threads.length === 0) {
      this.stopIdleTimer();
    }
  }

  private async _removeIdleWorker(worker: WorkerInstance): Promise<void> {
    const workerIndex = this.threads.findIndex(w => w.id === worker.id);
    if (workerIndex === -1) return;

    // Remove from threads array
    this.threads.splice(workerIndex, 1);
    // The model this worker loaded is gone with it - hand back its units so a
    // pool waiting on the group can start.
    this._releaseResidentFor(worker.id);

    // Terminate the worker
    try {
      await worker.terminate();
    } catch (error) {
      console.warn(`Failed to terminate idle worker ${worker.id}:`, error);
    }
  }

  // Shutdown all workers and cleanup resources
  /** Terminate every worker in the pool and drop its listeners. */
  async shutdown(): Promise<void> {
    this.shuttingDown = true;

    // Stop the centralized idle timer
    this.stopIdleTimer();

    // Listeners stay attached through termination so worker.teardown.* events
    // are observable on the shutdown path - they are dropped at the end instead.
    // The shuttingDown flag stops failure handling from restarting workers here.

    // Terminate all worker threads
    const shutdownPromises = this.threads.map(async (worker) => {
      try {
        return new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          console.warn(`⚠️ Timeout terminating worker ${worker.id}`);
          reject(new Error("Failed to terminate worker"));
        }, 1000);



        const terminatePromise = worker.terminate();
        terminatePromise.then(() => {
          clearTimeout(timeout);
          resolve();
        }).catch((error) => {
          clearTimeout(timeout);
          reject(error);
        });
      });

      } catch (error) {
        console.warn(`⚠️ Error terminating worker ${worker.id}:`, error);
        return Promise.resolve();
      }
    });

    await Promise.all(shutdownPromises);
    this.threads = [];
    this._releaseAllResident();

    // Remove ALL event listeners to prevent process hanging
    this._removeAllListeners();
  }
}
