export class DagTask {
    id: string;
    workerType: string;
    payload: any;
    parents: string[];
    children = new Set<string>();
    remainingParents: number;
    optional: boolean;
    retryPolicy?: {maxRetries:number, backoffMs:number};
    attempts = 0;
    state: string = "pending";
    result?: any;
    resolve?: Function;
    reject?: Function;
    workerGroups?: string[];
  
    constructor({id, workerType, payload, parents = [], optional = false, retryPolicy, workerGroups = []}: any) {
      this.id = id;
      this.workerType = workerType;
      this.payload = payload;
      this.parents = parents;
      this.remainingParents = parents.length;
      this.optional = optional;
      this.retryPolicy = retryPolicy;
      this.workerGroups = workerGroups;
    }
  
    onCompletePromise(): Promise<any> {
      return new Promise((resolve, reject) => {
        this.resolve = resolve;
        this.reject = reject;
      });
    }

    onCompleteDescendants(): Promise<any> {
      return new Promise((resolve, reject) => {
        // If task isn't added to scheduler yet, can't track descendants
        if (!this._scheduler) {
          reject(new Error('Task must be added to scheduler before tracking descendants'));
          return;
        }

        this._scheduler._trackDescendants(this, resolve, reject);
      });
    }

    _scheduler?: DagScheduler; // Reference to scheduler for descendant tracking
  }
  
  export interface DagSchedulerOptions {
    periodicRetryIntervalMs?: number; // Default: 100ms (was 1000ms originally, 2000ms after first optimization)
  }

  export class DagScheduler extends EventTarget {
    tasks = new Map<string, DagTask>();
    readyQueue: DagTask[] = [];
    blockedQueues = new Map<string, DagTask[]>(); // Grouped by blocking group IDs for faster lookup
    workerPools: any;
    groups: Record<string, any>;
    stats = {queued:0, running:0, done:0, failed:0};
    private retryTimer: ReturnType<typeof setTimeout> | null = null;
    private descendantTrackers = new Map<string, { resolve: Function, reject: Function, pendingDescendants: Set<string> }>;
    private options: DagSchedulerOptions;

    // Track ALL event listeners for cleanup
    private allListeners = new Map<any, {event: string; listener: Function}[]>();
  
    constructor(workerPools: any, groups: Record<string, any> = {}, options: DagSchedulerOptions = {}) {
      super();
      this.workerPools = workerPools;
      this.groups = groups;
      this.options = {
        periodicRetryIntervalMs: 50, // Default to 50ms - optimal balance of speed and efficiency
        ...options
      };
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
                  this.readyQueue.push(task);
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
  
    addTask(task: DagTask): Promise<any> {
      // Set up the promise BEFORE dispatching to avoid race conditions
      const promise = task.onCompletePromise();

      // Set scheduler reference for descendant tracking
      task._scheduler = this;

      this.tasks.set(task.id, task);
      for (const pid of task.parents) {
        const parent = this.tasks.get(pid);
        if (parent) parent.children.add(task.id);
      }
      if (task.remainingParents === 0) this.readyQueue.push(task);
      this.stats.queued++;
      this._dispatchLoop();
      return promise;
    }

    /**
     * Add multiple tasks in batch - optimized for high-volume scenarios.
     * Prevents Node.js worker_threads message passing overflow by batching dispatch calls.
     * Use this for bulk task additions (>1000 tasks) to avoid overwhelming the runtime.
     */
    addTasks(tasks: DagTask[]): Promise<any>[] {
      const promises: Promise<any>[] = [];

      for (const task of tasks) {
        // Set up the promise BEFORE dispatching to avoid race conditions
        const promise = task.onCompletePromise();
        promises.push(promise);

        // Set scheduler reference for descendant tracking
        task._scheduler = this;

        this.tasks.set(task.id, task);
        for (const pid of task.parents) {
          const parent = this.tasks.get(pid);
          if (parent) parent.children.add(task.id);
        }
        if (task.remainingParents === 0) this.readyQueue.push(task);
        this.stats.queued++;
        // Note: No _dispatchLoop() call here - batched at the end
      }

      // Single dispatch call for all tasks - prevents overwhelming worker_threads
      this._dispatchLoop();
      return promises;
    }
  
    _onParentComplete(parent: DagTask) {
      for (const cid of parent.children) {
        const child = this.tasks.get(cid);
        if (!child) continue;
        child.remainingParents--;
        if (child.remainingParents === 0) this.readyQueue.push(child);
      }
      this._dispatchLoop();
    }

  
    _dispatchLoop() {
      let tasksProcessed = 0;
      const maxTasksPerLoop = 1000; // Prevent excessive iterations in extreme contention

      while (this.readyQueue.length > 0 && tasksProcessed < maxTasksPerLoop) {
        const task = this.readyQueue.shift()!;
        const pool = this.workerPools[task.workerType];
        if (!pool) throw new Error(`Unknown worker type: ${task.workerType}`);

        // Combine task-level groups (backward compatibility) with worker manager groups
        const taskGroupIds = task.workerGroups || [];
        const workerGroupIds = pool.groups || [];
        const allGroupIds = [...new Set([...taskGroupIds, ...workerGroupIds])]; // Remove duplicates

        const allGroups = allGroupIds.map(gid => this.groups[gid]).filter(g => g); // Filter out undefined groups

        // Check group constraints and identify which groups are blocking
        const blockingGroups = allGroups.filter(g => !g.canRun());
        if (blockingGroups.length > 0) {
          // Add to blocked queues for each blocking group (enables targeted retry)
          const blockingGroupIds = allGroupIds.filter(gid => {
            const group = this.groups[gid];
            return group && !group.canRun();
          });

          for (const groupId of blockingGroupIds) {
            if (!this.blockedQueues.has(groupId)) {
              this.blockedQueues.set(groupId, []);
            }
            // Only add if not already in this blocked queue
            const queue = this.blockedQueues.get(groupId)!;
            if (!queue.includes(task)) {
              queue.push(task);
            }
          }

          // Schedule periodic retry as fallback for groups without events
          this._schedulePeriodicRetry();
          break;
        }
        allGroups.forEach(g => g.onStart());

        task.state = 'dispatched';
        // Note: queued count decremented when task actually starts running

        try {
          pool.enqueue({
            id: task.id,
            payload: task.payload,
            resolve: (res:any) => {
              // Group lifecycle management - finish groups when task completes
              for (const groupId of allGroupIds) {
                const group = this.groups[groupId];
                if (group) {
                  group.onFinish();
                  // Immediately check for blocked tasks on this group
                  this._retryBlockedTasksForGroup(groupId);
                }
              }
            },
            reject: (err:any) => {
              // Group lifecycle management - finish groups when task fails
              for (const groupId of allGroupIds) {
                const group = this.groups[groupId];
                if (group) {
                  group.onFinish();
                  // Immediately check for blocked tasks on this group
                  this._retryBlockedTasksForGroup(groupId);
                }
              }

              // If task fails to start, requeue it
              if (task.state === 'dispatched') {
                task.state = 'pending';
                this.readyQueue.push(task);
                this._schedulePeriodicRetry();
              }
            }
          });
        } catch (error) {
          // If pool.enqueue throws synchronously, requeue the task
          task.state = 'pending';
          this.readyQueue.push(task);
          // Release group resources and trigger retries
          for (const groupId of allGroupIds) {
            const group = this.groups[groupId];
            if (group) {
              group.onFinish();
              this._retryBlockedTasksForGroup(groupId);
            }
          }
        }
        tasksProcessed++;
      }

      // Clear periodic retry if no more tasks are queued or blocked
      const hasBlockedTasks = Array.from(this.blockedQueues.values()).some(queue => queue.length > 0);
      if (this.readyQueue.length === 0 && !hasBlockedTasks) {
        this._clearPeriodicRetry();
      }

      // Clean up empty blocked queues to avoid memory leaks
      for (const [groupId, queue] of this.blockedQueues.entries()) {
        if (queue.length === 0) {
          this.blockedQueues.delete(groupId);
        }
      }
    }

    _retryBlockedTasks() {
      // First, try any ready queue tasks
      if (this.readyQueue.length > 0) {
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
      const tasksToRetry: DagTask[] = [];
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
        this.readyQueue.unshift(...tasksToRetry);
        this._dispatchLoop();
      }
    }

    _schedulePeriodicRetry() {
      // Clear any existing timer
      if (this.retryTimer) {
        clearTimeout(this.retryTimer);
      }

      // Only schedule retry if there are queued tasks or blocked tasks (fallback for groups without events)
      const hasBlockedTasks = Array.from(this.blockedQueues.values()).some(queue => queue.length > 0);
      if (this.readyQueue.length > 0 || hasBlockedTasks) {
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
    override addEventListener(event: string, listener: EventListenerOrEventListenerObject, options?: boolean | AddEventListenerOptions): void {
      super.addEventListener(event, listener, options);
      this._trackListener(this, event, listener as Function);
    }

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
      // Set up event listeners for each group to enable immediate retry when slots become available
      for (const [groupId, group] of Object.entries(this.groups)) {
        if (group && typeof group.addEventListener === 'function') {
          const listener = () => {
            this._retryBlockedTasksForGroup(groupId);
          };
          this._addInternalListener(group, 'slot-released', listener);
        }
      }
    }

    _setupWorkerPoolListeners() {
      // Set up listeners for accurate task state tracking from workers
      Object.values(this.workerPools).forEach((pool: any) => {
        const taskStartedListener = (e: any) => {
          const task = this.tasks.get(e.detail.taskId);
          if (task) {
            task.state = 'running';
            this.stats.queued--; // Decrement when task actually starts running
            this.stats.running++;
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
            const childTask = new DagTask({
              id: spawnConfig.id,
              workerType: spawnConfig.workerType,
              payload: spawnConfig.payload,
              parents: spawnConfig.parents || [task.id], // Default to parent task
              optional: spawnConfig.optional || false,
              retryPolicy: spawnConfig.retryPolicy,
              workerGroups: spawnConfig.workerGroups || []
            });

            // Add child to parent's children set
            task.children.add(childTask.id);

            // Add the spawned task to the scheduler
            this.addTask(childTask);

            // Add to descendant trackers
            this._addSpawnedTaskToTrackers(task.id, childTask.id);
          }
        };
        this._addInternalListener(pool, 'task.spawn_request', taskSpawnListener);
        
        //TODO: manually added this, verify it makes sense!!!
        const taskRequeueListener = (e: any) => {
          const { taskId } = e.detail;
          const task = this.tasks.get(taskId);
          
          if (task && task.state === 'running') {
            // Reset task state for requeuing
            task.state = 'pending';
            this.stats.running--;
            this.stats.queued++;
            
            // Add back to ready queue for rescheduling
            this.readyQueue.push(task);
            
            
            // Trigger immediate dispatch to reschedule
            this._dispatchLoop();
          }
        };
        this._addInternalListener(pool, 'task.requeue_required', taskRequeueListener);

        const taskCompletedListener = (e: any) => {
          const task = this.tasks.get(e.detail.taskId);
          if (task) {
            task.state = 'done';
            task.result = e.detail.result;
            this.stats.running--;
            this.stats.done++;
            task.resolve?.(e.detail.result);

            // Check descendant trackers
            this._checkDescendantTrackers(task.id);

            this._onParentComplete(task);
            // Forward original event detail with execution timing, but include task info
            const eventDetail = {
              ...task,
              executionTime: e.detail.executionTime,
              workerId: e.detail.workerId,
              workerType: e.detail.workerType
            };
            this.dispatchEvent(new CustomEvent('task.completed', {detail: eventDetail}));
            this._retryBlockedTasks();
            this._checkCompletion();
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
              this.stats.queued++;
              this.readyQueue.push(task);
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

    _trackDescendants(task: DagTask, resolve: Function, reject: Function) {
      // Start tracking this task and all its descendants
      const pendingDescendants = new Set<string>();

      // Add the task itself
      if (task.state !== 'done' && task.state !== 'failed') {
        pendingDescendants.add(task.id);
      }

      // Add all existing children recursively
      this._addDescendantsToSet(task, pendingDescendants);

      // If nothing to track, resolve immediately
      if (pendingDescendants.size === 0) {
        resolve(task.result || null);
        return;
      }

      // Store tracker for this task
      this.descendantTrackers.set(task.id, { resolve, reject, pendingDescendants });
    }

    _addDescendantsToSet(task: DagTask, pendingSet: Set<string>) {
      for (const childId of task.children) {
        const child = this.tasks.get(childId);
        if (child && child.state !== 'done' && child.state !== 'failed') {
          pendingSet.add(childId);
          this._addDescendantsToSet(child, pendingSet); // Recursive for grandchildren
        }
      }
    }

    _checkDescendantTrackers(completedTaskId: string) {
      // Check all descendant trackers to see if any are now complete
      for (const [rootTaskId, tracker] of this.descendantTrackers) {
        tracker.pendingDescendants.delete(completedTaskId);

        if (tracker.pendingDescendants.size === 0) {
          // All descendants complete - resolve the promise
          const rootTask = this.tasks.get(rootTaskId);
          tracker.resolve(rootTask?.result || null);
          this.descendantTrackers.delete(rootTaskId);
        }
      }
    }

    _addSpawnedTaskToTrackers(parentTaskId: string, spawnedTaskId: string) {
      // Add newly spawned task to any active descendant trackers
      for (const [rootTaskId, tracker] of this.descendantTrackers) {
        // If the parent is being tracked, add the spawned child
        if (tracker.pendingDescendants.has(parentTaskId) || rootTaskId === parentTaskId) {
          const spawnedTask = this.tasks.get(spawnedTaskId);
          if (spawnedTask && spawnedTask.state !== 'done' && spawnedTask.state !== 'failed') {
            tracker.pendingDescendants.add(spawnedTaskId);
          }
        }
      }
    }

    // Shutdown the scheduler and cleanup all resources
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
      this.readyQueue = [];
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
  