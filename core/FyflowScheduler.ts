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
    /** Retry attempts used so far, counted against `retryPolicy.maxRetries`. */
    attempts = 0;
    /**
     * `pending` -> `running` -> `done` | `failed`.
     *
     * A non-optional task that fails with no retries left ends in `user_action`,
     * signalling that something outside the scheduler has to intervene.
     */
    state: string = "pending";
    result?: any;
    error?: string; // Error message if task failed
    resolve?: Function;
    reject?: Function;
    workerGroups?: string[];
    handleRejection: boolean; // If true (default), silently handle rejections for fire-and-forget
    startTime?: number; // Timestamp when task started execution
    endTime?: number; // Timestamp when task completed
    /**
     * Worker-measured execution time in ms (high resolution), set on completion.
     * Unlike `endTime - startTime` this excludes time spent waiting for a worker
     * slot. Cleared when a task is requeued or retried.
     */
    executionTime?: number;
    _resourcesPreAllocated?: boolean; // Flag to track if resources are pre-allocated

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
     * @param config.workerGroups Resource groups for this task, *in addition* to
     *   the groups its WorkerManager declares.
     * @param config.handleRejection Default `true`. Attaches a silent catch so a
     *   fire-and-forget failure does not surface as an unhandled rejection;
     *   `task.failed` is still emitted.
     */
    constructor({id, workerType, payload, optional = false, retryPolicy, workerGroups = [], handleRejection = true}: any) {
      this.id = id;
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
   * Emits: `task.running`, `task.completed`, `task.failed`, `task.progress`,
   * `task.user_action`, `task.spawn_request`, `task.spawn_failed` and
   * `scheduler.completed`.
   */
  export class FyflowScheduler extends EventTarget {
    tasks = new Map<string, FyflowTask>();
    readyQueuesByWorker = new Map<string, FyflowTask[]>(); // Per-worker-type queues for O(1) dispatch
    blockedQueues = new Map<string, FyflowTask[]>(); // Grouped by blocking group IDs for faster lookup
    workerPools: any;
    groups: Record<string, any>;
    stats = {queued:0, running:0, done:0, failed:0};
    private retryTimer: ReturnType<typeof setTimeout> | null = null;
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
        this.readyQueuesByWorker.set(workerType, []);
      }

      this._setupWorkerPoolListeners();
      this._setupGroupEventListeners();
    }

    // Any task waiting on a resource group. Blocked tasks are subtracted from
    // stats.queued when they leave the ready queue, so they are invisible to the
    // stats alone.
    private _hasBlockedTasks(): boolean {
      for (const queue of this.blockedQueues.values()) {
        if (queue.length > 0) return true;
      }
      return false;
    }

    _checkCompletion() {
      const hasBlockedTasks = this._hasBlockedTasks();

      if (this.stats.queued === 0 && this.stats.running === 0 && !hasBlockedTasks && this.stats.done > 0) {
        this._clearPeriodicRetry(); // Stop retries when all tasks are done
        this.dispatchEvent(new CustomEvent('scheduler.completed', {detail: this.stats}));
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

      // Set scheduler reference for descendant tracking
      task._scheduler = this;
      this.tasks.set(task.id, task);
      workerQueue.push(task);

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
      }

      const promises: Promise<any>[] = [];

      for (const task of tasks) {
        // Set scheduler reference for descendant tracking
        task._scheduler = this;

        this.tasks.set(task.id, task);

        // Queue task immediately (no dependency tracking)
        this.readyQueuesByWorker.get(task.workerType)!.push(task);
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
      const maxTasksPerLoop = 1000; // Prevent excessive iterations in extreme contention


      // Process per-worker-type queues for O(1) efficiency
      for (const [workerType, queue] of this.readyQueuesByWorker) {
        if (queue.length === 0) continue;

        const pool = this.workerPools[workerType];
        if (!pool) continue;

        // Process multiple tasks for this worker type until we hit limits
        while (queue.length > 0 && tasksProcessed < maxTasksPerLoop) {
          const task = queue[0]; // Peek at first task (don't remove yet)

          // Check if we can dispatch this task
          const canDispatchResult = this._canDispatchTask(task, pool);
          if (canDispatchResult === true) {
            queue.shift(); // Remove from queue
            // Dispatch the task (synchronous dispatch, async execution)
            this._dispatchTask(task, pool);
            tasksProcessed++;
          } else if (canDispatchResult === 'blocked') {
            // Task was moved to blocked queue - remove from ready queue
            queue.shift();
            // Decrement queued count since task is now blocked (not in ready queue)
            this.stats.queued--;
            // Stop processing this worker type for now
            break;
          } else {
            // canDispatchResult === false: No threads available
            // Keep task in ready queue and schedule retry
            // (threads may become available when other tasks complete or idle timeout creates capacity)
            this._schedulePeriodicRetry();
            break;
          }
        }
      }

      // Clear periodic retry if no more tasks queued or blocked
      const hasReadyTasks = Array.from(this.readyQueuesByWorker.values()).some(q => q.length > 0);
      const hasBlockedTasks = Array.from(this.blockedQueues.values()).some(q => q.length > 0);
      const hasQueuedInStats = this.stats.queued > 0;
      if (!hasReadyTasks && !hasBlockedTasks && !hasQueuedInStats) {
        this._clearPeriodicRetry();
      }
    }


    // Helper: Check resource availability and block if needed
    _checkAndBlockResources(task: FyflowTask, pool: any): boolean {
      const taskGroupIds = task.workerGroups || [];
      const workerGroupIds = pool.groups || [];

      // Fast path: no groups configured
      if (taskGroupIds.length === 0 && workerGroupIds.length === 0) {
        return true;
      }

      const allGroupIds = [...new Set([...taskGroupIds, ...workerGroupIds])];
      const allGroups = allGroupIds.map((gid: string) => this.groups[gid]).filter((g: any) => g);

      // Check if resources are available
      for (const group of allGroups) {
        if (!group.canRun()) {
          // Add to blocked queue
          const groupId = allGroupIds.find(gid => this.groups[gid] === group)!;
          if (!this.blockedQueues.has(groupId)) {
            this.blockedQueues.set(groupId, []);
          }
          const blockedQueue = this.blockedQueues.get(groupId)!;
          if (!blockedQueue.includes(task)) {
            blockedQueue.push(task);
          }
          this._schedulePeriodicRetry();
          return false; // Resources unavailable, task blocked
        }
      }
      return true; // Resources available
    }

    // NEW: Non-optimistic mode helper methods
    _canDispatchTask(task: FyflowTask, pool: any): boolean | 'blocked' {
      // Check if we have an immediately available thread (no async creation)
      const availableThreads = pool.threads.filter((t: any) =>
        t.canAcceptTask && t.canAcceptTask()
      ).length;

      if (availableThreads > 0) {
        // Have available thread - check resources
        if (!this._checkAndBlockResources(task, pool)) {
          return 'blocked';
        }
        return true;
      }

      // No available threads - can we create one?
      // Account for threads being initialized (same logic as enqueueNoQueue)
      const initializingCount = pool.initializingThreads ? pool.initializingThreads.size : 0;
      const canCreateThread = (pool.threads.length + initializingCount) < pool.maxThreads;
      if (canCreateThread) {
        // Check resource groups before creating thread
        if (!this._checkAndBlockResources(task, pool)) {
          return 'blocked';
        }
        // Resources available - can create thread and dispatch
        return true;
      }

      // No threads available and can't create more - task stays in ready queue
      return false;
    }

    _dispatchTask(task: FyflowTask, pool: any) {
      // Acquire resources immediately (thread will be created if needed)
      const taskGroupIds = task.workerGroups || [];
      const workerGroupIds = pool.groups || [];
      const allGroupIds = [...new Set([...taskGroupIds, ...workerGroupIds])];
      const allGroups = allGroupIds.map((gid: string) => this.groups[gid]).filter((g: any) => g);

      allGroups.forEach((g: any) => g.onStart());
      task._resourcesPreAllocated = true;

      // Update state and stats synchronously
      task.state = 'running';
      task.startTime = Date.now();
      this.stats.queued--;
      this.stats.running++;

      // Get/create thread and execute task
      pool.enqueueNoQueue(task.id, task.payload)
        .then((result: any) => {
          this._onTaskComplete(task, result, pool);
        })
        .catch((error: any) => {
          // Handle race condition: thread filled up between check and dispatch
          if (error.message === 'Worker at maximum concurrent task capacity') {
            // Revert state and stats changes
            task.state = 'pending';
            task.startTime = undefined;
            task.executionTime = undefined; // Discard timing from the abandoned attempt
            this.stats.running--;
            this.stats.queued++;

            // Release pre-allocated resources
            this._releaseResourcesForTask(task, pool);

            // Put task back in ready queue
            const workerQueue = this.readyQueuesByWorker.get(task.workerType);
            if (workerQueue) {
              workerQueue.push(task);
            }

            // Retry dispatch (thread should be available soon)
            setTimeout(() => this._dispatchLoop(), 10);
          } else {
            this._onTaskFailed(task, error, pool);
          }
        });
    }

    _releaseResourcesForTask(task: FyflowTask, pool: any) {
      if (!task._resourcesPreAllocated) return;

      const taskGroupIds = task.workerGroups || [];
      const workerGroupIds = pool.groups || [];
      const allGroupIds = [...new Set([...taskGroupIds, ...workerGroupIds])];

      // Release all resources first, then retry blocked tasks
      for (const groupId of allGroupIds) {
        const group = this.groups[groupId];
        if (group) {
          group.onFinish();
        }
      }

      task._resourcesPreAllocated = undefined;

      // Retry blocked tasks AFTER all resources are released
      for (const groupId of allGroupIds) {
        this._retryBlockedTasksForGroup(groupId);
      }
    }

    _onTaskComplete(task: FyflowTask, result: any, pool: any) {
      // Skip if already completed (prevent double-processing)
      if (task.state === 'done') {
        return;
      }

      // Release resources
      this._releaseResourcesForTask(task, pool);

      // Update stats and task state
      task.state = 'done';
      task.endTime = Date.now();
      task.result = result;
      this.stats.running--;
      this.stats.done++;
      task.resolve?.(result);

      // Check descendant trackers
      this._settleDescendantTrackers(task.id);
      this._recordTerminalTask(task.id);

      // Emit events
      this.dispatchEvent(new CustomEvent('task.completed', { detail: task }));
      // this._onParentComplete(task);
      this._checkCompletion();

      // CRITICAL: Dispatch more tasks now that resources are available
      this._dispatchLoop();
    }

    _onTaskFailed(task: FyflowTask, error: any, pool: any) {
      // Release resources
      this._releaseResourcesForTask(task, pool);

      // Handle retry or failure
      if (task.attempts < (task.retryPolicy?.maxRetries || 0)) {
        task.attempts++;
        task.state = 'pending';
        task.startTime = undefined;
        task.executionTime = undefined; // Discard timing from the abandoned attempt
        this.stats.running--;
        this.stats.queued++;

        const workerQueue = this.readyQueuesByWorker.get(task.workerType);
        if (workerQueue) {
          workerQueue.push(task);
        }

        setTimeout(() => this._dispatchLoop(), task.retryPolicy?.backoffMs || 0);
      } else {
        task.state = 'failed';
        task.error = error.message;
        this.stats.running--;
        this.stats.failed++;
        this.dispatchEvent(new CustomEvent('task.failed', { detail: task }));

        // Settle the caller's promise. Failures that arrive as a worker
        // `task.failed` event are rejected by the pool listener, but failures
        // that only surface here - a worker that cannot initialize, for
        // instance - reached no other rejection path and hung forever.
        if (task.optional) {
          task.resolve?.(null);
        } else {
          task.reject?.(error instanceof Error ? error : new Error(task.error || 'Task failed'));
        }

        this._settleDescendantTrackers(task.id);
        this._recordTerminalTask(task.id);
        this._checkCompletion();
      }
    }

    _retryBlockedTasks() {
      // Check if any ready tasks exist across all worker types
      const hasReadyTasks = Array.from(this.readyQueuesByWorker.values()).some(q => q.length > 0);
      if (hasReadyTasks) {
        this._dispatchLoop();
      }

      // Then, retry blocked tasks for groups that now have capacity
      for (const [groupId, group] of Object.entries(this.groups)) {
        if (group && group.canRun()) {
          this._retryBlockedTasksForGroup(groupId);
        }
      }

    }

    _retryBlockedTasksForGroup(groupId: string) {
      // Immediately retry tasks blocked by this specific group
      const blockedTasks = this.blockedQueues.get(groupId);
      if (!blockedTasks || blockedTasks.length === 0) return;

      const group = this.groups[groupId];
      if (!group || !group.canRun()) return;

      // Move tasks from blocked queue back to ready queue
      const tasksToRetry: FyflowTask[] = [];
      while (blockedTasks.length > 0 && group.canRun()) {
        const task = blockedTasks.shift()!;

        // Check if all groups for this task can now run
        const pool = this.workerPools[task.workerType];
        if (pool) {
          const taskGroupIds = task.workerGroups || [];
          const workerGroupIds = pool.groups || [];
          const allGroupIds = [...new Set([...taskGroupIds, ...workerGroupIds])];
          const allGroups = allGroupIds.map(gid => this.groups[gid]).filter(g => g);

          if (allGroups.every(g => g.canRun())) {
            // Remove from other blocked queues
            for (const otherGroupId of allGroupIds) {
              if (otherGroupId !== groupId) {
                const otherQueue = this.blockedQueues.get(otherGroupId);
                if (otherQueue) {
                  const index = otherQueue.indexOf(task);
                  if (index !== -1) otherQueue.splice(index, 1);
                }
              }
            }
            tasksToRetry.push(task);
          }
        }
      }

      // Add tasks back to ready queue and dispatch
      if (tasksToRetry.length > 0) {
        // Add to per-worker queues
        for (const task of tasksToRetry) {
          const workerQueue = this.readyQueuesByWorker.get(task.workerType);
          if (workerQueue) {
            workerQueue.unshift(task);
            // Increment queued count since task is back in ready queue
            this.stats.queued++;
          }
        }
        this._dispatchLoop();
      }
    }

    _schedulePeriodicRetry() {
      // Clear any existing timer
      if (this.retryTimer) {
        clearTimeout(this.retryTimer);
      }

      // Only schedule retry if there are queued tasks or blocked tasks (fallback for groups without events)
      const hasReadyTasks = Array.from(this.readyQueuesByWorker.values()).some(q => q.length > 0);
      const hasBlockedTasks = Array.from(this.blockedQueues.values()).some(queue => queue.length > 0);
      if (hasReadyTasks || hasBlockedTasks) {
        this.retryTimer = setTimeout(() => {
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
          } catch (error) {
            // Target may already be destroyed - ignore
          }
        }
      }
      this.allListeners.clear();
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
            // Just emit monitoring event - stats already updated by scheduler
            this.dispatchEvent(new CustomEvent('task.running', {detail: task}));
          }
        };
        this._addInternalListener(pool, 'task.started', taskStartedListener);

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
          const { taskId } = e.detail;
          const task = this.tasks.get(taskId);

          if (task && task.state === 'running') {
            // Release resources before requeuing
            const workerPool = this.workerPools[task.workerType];
            const wasStarted = task.startTime !== undefined;
            const wasPreAllocated = task._resourcesPreAllocated === true;

            if (workerPool && (wasStarted || wasPreAllocated)) {
              // Release resource group slots
              this._releaseResourcesForTask(task, workerPool);
            }

            // Reset task state for requeuing
            task.state = 'pending';
            task.startTime = undefined; // Clear start time since task is being requeued
            task.executionTime = undefined; // Discard timing from the abandoned attempt
            task._resourcesPreAllocated = undefined; // Clear pre-allocation flag for retry
            this.stats.running--;
            this.stats.queued++;

            // Add back to ready queue for rescheduling
            const workerQueue = this.readyQueuesByWorker.get(task.workerType);
            if (workerQueue) {
              workerQueue.push(task);
            }

            // Trigger immediate dispatch to reschedule
            this._dispatchLoop();
          }
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
          if (task) {
            task.state = 'failed';
            this.stats.running--;
            this.stats.failed++;

            if (task.attempts < (task.retryPolicy?.maxRetries || 0)) {
              task.attempts++;
              task.state = 'pending';
              task.startTime = undefined; // Clear start time for retry attempt
              task.executionTime = undefined; // Discard timing from the abandoned attempt
              task._resourcesPreAllocated = undefined; // Clear pre-allocation flag for retry
              this.stats.queued++;
              const workerQueue = this.readyQueuesByWorker.get(task.workerType);
              if (workerQueue) {
                workerQueue.push(task);
              }
              setTimeout(() => this._dispatchLoop(), task.retryPolicy?.backoffMs || 0);
            } else if (!task.optional) {
              task.state = 'user_action';
              this.dispatchEvent(new CustomEvent('task.user_action', {detail: task}));
              // Emit task.failed event for external listeners
              this.dispatchEvent(new CustomEvent('task.failed', {detail: {...task, error: e.detail.error}}));
              // Reject the task promise since it failed without retries
              task.reject?.(e.detail.error || new Error('Task failed'));
              this._settleDescendantTrackers(task.id);
              this._recordTerminalTask(task.id);
              this._checkCompletion();
            } else {
              task.resolve?.(null);
              this._settleDescendantTrackers(task.id);
              this._recordTerminalTask(task.id);
              this._checkCompletion();
            }
          }
        };
        this._addInternalListener(pool, 'task.failed', taskFailedListener);
      });
    }

    // --- Descendant tracking -------------------------------------------------
    // Descendants are tasks created at runtime via context.spawnTask(). This is
    // lineage only: it never influences dispatch order or readiness.

    private static readonly TERMINAL_STATES = new Set(['done', 'failed', 'user_action']);

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

      // 1. Clear periodic retry timer to stop any ongoing retries
      this._clearPeriodicRetry();

      // 2. Remove all internal event listeners to prevent process hanging
      this._removeAllListeners();

      // 2. Wait for any currently running tasks to complete
      while (this.stats.running > 0) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      // 3. Clear all queues
      this.readyQueuesByWorker.forEach(q => q.length = 0);
      this.blockedQueues.clear();

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
      this.stats = {queued: 0, running: 0, done: 0, failed: 0};

    }
  }
  