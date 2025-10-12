export class FyflowTask {
    id: string;
    workerType: string;
    payload: any;
    optional: boolean;
    retryPolicy?: {maxRetries:number, backoffMs:number};
    attempts = 0;
    state: string = "pending";
    result?: any;
    error?: string; // Error message if task failed
    resolve?: Function;
    reject?: Function;
    workerGroups?: string[];
    handleRejection: boolean; // If true (default), silently handle rejections for fire-and-forget
    startTime?: number; // Timestamp when task started execution
    endTime?: number; // Timestamp when task completed
    _resourcesPreAllocated?: boolean; // Flag to track if resources are pre-allocated

    // Private fields for resource management
    _scheduler?: FyflowScheduler; // Reference to scheduler for descendant tracking

    constructor({id, workerType, payload, optional = false, retryPolicy, workerGroups = [], handleRejection = true}: any) {
      this.id = id;
      this.workerType = workerType;
      this.payload = payload;
      this.optional = optional;
      this.retryPolicy = retryPolicy;
      this.workerGroups = workerGroups;
      this.handleRejection = handleRejection;
    }

    onCompletePromise(): Promise<any> {
      return new Promise((resolve, reject) => {
        this.resolve = resolve;
        this.reject = reject;
      });
    }

    // onCompleteDescendants(): Promise<any> {
    //   return new Promise((resolve, reject) => {
    //     // If task isn't added to scheduler yet, can't track descendants
    //     if (!this._scheduler) {
    //       reject(new Error('Task must be added to scheduler before tracking descendants'));
    //       return;
    //     }

    //     this._scheduler._trackDescendants(this, resolve, reject);
    //   });
    // }
  }
  
  export interface FyflowSchedulerOptions {
    periodicRetryIntervalMs?: number; // Default: 50ms - retry interval for blocked tasks
  }

  export interface AddTaskOptions {
    createPromise?: boolean; // Default: false (fire-and-forget, no promise created)
  }

  export class FyflowScheduler extends EventTarget {
    tasks = new Map<string, FyflowTask>();
    readyQueuesByWorker = new Map<string, FyflowTask[]>(); // Per-worker-type queues for O(1) dispatch
    blockedQueues = new Map<string, FyflowTask[]>(); // Grouped by blocking group IDs for faster lookup
    workerPools: any;
    groups: Record<string, any>;
    stats = {queued:0, running:0, done:0, failed:0};
    private retryTimer: ReturnType<typeof setTimeout> | null = null;
    private descendantTrackers = new Map<string, { resolve: Function, reject: Function, pendingDescendants: Set<string> }>;
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

    _checkCompletion() {
      if (this.stats.queued === 0 && this.stats.running === 0 && this.stats.done > 0) {
        this._clearPeriodicRetry(); // Stop retries when all tasks are done
        this.dispatchEvent(new CustomEvent('scheduler.completed', {detail: this.stats}));
      } else if (this.stats.queued > 0 && this.stats.running === 0) {
        // Tasks are queued but none running - recover stuck dispatched tasks and retry
        const allTasks = Array.from(this.tasks.values());
        const dispatchedTasks = allTasks.filter(t => t.state === 'dispatched');

        if (dispatchedTasks.length > 0) {
          // Only recover dispatched tasks after a reasonable timeout to avoid interfering with normal operation
          // This prevents recovery from interfering with tests and normal scheduler operation
          setTimeout(() => {
            if (this.stats.queued > 0 && this.stats.running === 0) {
              dispatchedTasks.forEach(task => {
                if (task.state === 'dispatched') {
                  task.state = 'pending';
                  const workerQueue = this.readyQueuesByWorker.get(task.workerType);
                  if (workerQueue) {
                    workerQueue.push(task);
                  }
                }
              });
              this._retryBlockedTasks();
            }
          }, 5000); // 5 second delay before recovery
        }

        this._retryBlockedTasks();
        this._schedulePeriodicRetry();
      }
    }
  
    addTask(task: FyflowTask, options?: AddTaskOptions): Promise<any> | void {
      this.tasks.set(task.id, task);

      // Add to per-worker-type queue
      const workerQueue = this.readyQueuesByWorker.get(task.workerType);
      if (workerQueue) {
        workerQueue.push(task);
      } else {
        throw new Error(`Unknown worker type: ${task.workerType}`);
      }

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
     * Add multiple tasks in batch - optimized for high-volume scenarios.
     * Prevents Node.js worker_threads message passing overflow by batching dispatch calls.
     * Use this for bulk task additions (>1000 tasks) to avoid overwhelming the runtime.
     */
    addTasks(tasks: FyflowTask[], options?: AddTaskOptions): Promise<any>[] | void {
      const promises: Promise<any>[] = [];

      for (const task of tasks) {
        // Set scheduler reference for descendant tracking
        task._scheduler = this;

        this.tasks.set(task.id, task);

        // Queue task immediately (no dependency tracking)
        const workerQueue = this.readyQueuesByWorker.get(task.workerType);
        if (workerQueue) {
          workerQueue.push(task);
        }
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
      // this._checkDescendantTrackers(task.id);

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
        }, this.options.periodicRetryIntervalMs!); // Configurable retry interval (default 100ms)
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
              parents: spawnConfig.parents || [task.id], // Default to parent task
              optional: spawnConfig.optional || false,
              retryPolicy: spawnConfig.retryPolicy,
              workerGroups: spawnConfig.workerGroups || []
            });

            // Add the spawned task to the scheduler
            this.addTask(childTask);

            // Add to descendant trackers
            // this._addSpawnedTaskToTrackers(task.id, childTask.id);
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
          }
        };
        // this._addInternalListener(pool, 'task.completed', taskCompletedListener); // DISABLED - causes duplicate events

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
              this._checkCompletion();
            } else {
              task.resolve?.(null);
              this._checkCompletion();
            }
          }
        };
        this._addInternalListener(pool, 'task.failed', taskFailedListener);
      });
    }

    // _trackDescendants(task: FyflowTask, resolve: Function, reject: Function) {
    //   // Start tracking this task and all its descendants
    //   const pendingDescendants = new Set<string>();

    //   // Add the task itself
    //   if (task.state !== 'done' && task.state !== 'failed') {
    //     pendingDescendants.add(task.id);
    //   }

    //   // Add all existing children recursively
    //   this._addDescendantsToSet(task, pendingDescendants);

    //   // If nothing to track, resolve immediately
    //   if (pendingDescendants.size === 0) {
    //     resolve(task.result || null);
    //     return;
    //   }

    //   // Store tracker for this task
    //   this.descendantTrackers.set(task.id, { resolve, reject, pendingDescendants });
    // }

    // _addDescendantsToSet(task: FyflowTask, pendingSet: Set<string>) {
    //   for (const childId of task.children) {
    //     const child = this.tasks.get(childId);
    //     if (child && child.state !== 'done' && child.state !== 'failed') {
    //       pendingSet.add(childId);
    //       this._addDescendantsToSet(child, pendingSet); // Recursive for grandchildren
    //     }
    //   }
    // }

    // _checkDescendantTrackers(completedTaskId: string) {
    //   // Check all descendant trackers to see if any are now complete
    //   for (const [rootTaskId, tracker] of this.descendantTrackers) {
    //     tracker.pendingDescendants.delete(completedTaskId);

    //     if (tracker.pendingDescendants.size === 0) {
    //       // All descendants complete - resolve the promise
    //       const rootTask = this.tasks.get(rootTaskId);
    //       tracker.resolve(rootTask?.result || null);
    //       this.descendantTrackers.delete(rootTaskId);
    //     }
    //   }
    // }

    // _addSpawnedTaskToTrackers(parentTaskId: string, spawnedTaskId: string) {
    //   // Add newly spawned task to any active descendant trackers
    //   for (const [rootTaskId, tracker] of this.descendantTrackers) {
    //     // If the parent is being tracked, add the spawned child
    //     if (tracker.pendingDescendants.has(parentTaskId) || rootTaskId === parentTaskId) {
    //       const spawnedTask = this.tasks.get(spawnedTaskId);
    //       if (spawnedTask && spawnedTask.state !== 'done' && spawnedTask.state !== 'failed') {
    //         tracker.pendingDescendants.add(spawnedTaskId);
    //       }
    //     }
    //   }
    // }

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


      // 5. Clear all internal state
      this.tasks.clear();
      this.descendantTrackers.clear();
      this.stats = {queued: 0, running: 0, done: 0, failed: 0};

    }
  }
  