import { ThreadWrapper } from "./threadWrapper.ts";
import { InlineWrapper } from "./inlineWrapper.ts";
import { WorkerStatus } from "./workerInterface.ts";

type WorkerInstance = ThreadWrapper | InlineWrapper;

export interface WorkerManagerOptions {
  maxThreads?: number;
  maxConcurrentTasks?: number;
  idleTimeout?: number;
  inline?: boolean;
  config?: any;
  groups?: string[];
  requeueFailedTasks?: boolean; // Default: true (as specified in task)
  maxWorkerRestarts?: number; // Default: 3 - prevent infinite restart loops
}

export class WorkerManager extends EventTarget {
  threads: WorkerInstance[] = [];
  taskQueue: {id:string, payload:any, resolve:(value:any)=>void, reject:(reason?:any)=>void}[] = [];
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

  // Track ALL event listeners for cleanup
  private allListeners = new Map<any, {event: string; listener: Function}[]>();

  // Smart dispatch state tracking
  private predictiveCapacity = 0; // Future capacity from initializing threads
  private initializingThreads = new Set<WorkerInstance>();
  private retryTimer: ReturnType<typeof setTimeout> | null = null;

  // Centralized idle management
  private idleCheckTimer: ReturnType<typeof setInterval> | null = null;
  private readonly IDLE_CHECK_INTERVAL = 5000; // Check every 5 seconds

  constructor(scriptUrl: string, options: WorkerManagerOptions = {}) {
    super();
    this.scriptUrl = scriptUrl;
    this.maxThreads = options.maxThreads ?? 2;
    this.maxConcurrentTasks = options.maxConcurrentTasks ?? 1;
    this.idleTimeout = options.idleTimeout ?? 5000;
    this.inline = options.inline ?? false;
    this.config = options.config ?? {};
    this.groups = options.groups ?? [];
    this.requeueFailedTasks = options.requeueFailedTasks ?? true; // Default to requeue as specified
    this.maxWorkerRestarts = options.maxWorkerRestarts ?? 3; // Default: 3 restarts per worker

    // Set up worker failure handling
    this._setupWorkerFailureHandling();
  }

  enqueue(task: {id:string, payload:any, resolve:(value:any)=>void, reject:(reason?:any)=>void}) {
    this.taskQueue.push(task);
    this._dispatchTasks();
  }

  _dispatchTasks(): number {
    let dispatched = 0;

    while (this.taskQueue.length > 0) {
      // 1. Try existing ready threads first (excludes initializing threads)
      let availableThread = this.threads.find(t =>
        t.canAcceptTask() && !this.initializingThreads.has(t)
      );

      if (availableThread) {
        this._dispatchTaskToThread(availableThread);
        dispatched++;
        continue;
      }

      // 2. Check if we need more threads (simplified capacity planning)
      const currentCapacity = this._getAvailableCapacity();
      const futureCapacity = currentCapacity + this.predictiveCapacity;
      const queuedTasks = this.taskQueue.length;

      // For lazy initialization, be more conservative about thread creation
      if (currentCapacity === 0 && this._canCreateThread()) {
        if (this._shouldCreateThread()) {
          this._createPredictiveThread();
          continue; // Try to dispatch to the newly created thread
        } else {
          // Can't create thread due to resource constraints - wait for resources
          this._scheduleSmartRetry();
          break;
        }
      } else if (futureCapacity < queuedTasks && this._canCreateThread()) {
        if (this._shouldCreateThread()) {
          this._createPredictiveThread();
          continue; // Try to dispatch to the newly created thread
        } else {
          // Can't create thread due to resource constraints - wait for resources
          this._scheduleSmartRetry();
          break;
        }
      }

      // 3. Sufficient future capacity exists - wait for threads to initialize
      if (this.initializingThreads.size > 0) {
        // For inline workers, don't delay - they initialize instantly
        if (this.inline) {
          break; // Inline workers don't need retry delays
        } else {
          // During high-volume scenarios, we need to be more aggressive about waiting for initialization
          // instead of just breaking, schedule a retry to revisit dispatch when threads are ready
          this._scheduleSmartRetry();
          break;
        }
      }

      // 4. No threads initializing and no capacity - schedule smart retry
      // For inline workers, avoid unnecessary delays
      if (!this.inline) {
        this._scheduleSmartRetry();
      }
      break;
    }

    return dispatched;
  }

  private _dispatchTaskToThread(thread: WorkerInstance) {
    const task = this.taskQueue.shift()!;

    // Double-check that thread can still accept tasks (race condition protection)
    if (!thread.canAcceptTask()) {
      // Put task back and retry later
      this.taskQueue.unshift(task);
      this._scheduleSmartRetry();
      return;
    }

    // For threaded workers, track initialization state when first task is dispatched
    if (!this.inline && !this.initializingThreads.has(thread) && !(thread as ThreadWrapper).initialized) {
      this.initializingThreads.add(thread);
      this.predictiveCapacity += this.maxConcurrentTasks;
      this._setupInitializationTracking(thread);
    }

    thread.runTask(task.id, task.payload)
      .then(task.resolve)
      .catch((error) => {
        // All errors are real task failures now
        task.reject(error);
      })
      .finally(() => {
        // Resume dispatch for queued tasks
        if (this.taskQueue.length > 0) {
          this._dispatchTasks();
        }
      });
  }

  private _getAvailableCapacity(): number {
    return this.threads
      .filter(t => !this.initializingThreads.has(t))
      .reduce((total, thread) => {
        const running = thread.runningTasks || 0;
        const capacity = thread.maxConcurrentTasks || this.maxConcurrentTasks;
        return total + Math.max(0, capacity - running);
      }, 0);
  }

  private _canCreateThread(): boolean {
    return this.threads.length < this.maxThreads;
  }

  private _shouldCreateThread(): boolean {
    // Always allow thread creation up to maxThreads
    // CPU constraints are handled at the group level now
    return true;
  }

  private _createPredictiveThread(): WorkerInstance {
    const thread = this.inline
      ? new InlineWrapper(this.scriptUrl, this.idleTimeout, this.maxConcurrentTasks, this.config)
      : new ThreadWrapper(this.scriptUrl, this.idleTimeout, this.maxConcurrentTasks, this.config);

    // Set up event listeners first
    this._setupWorkerEventListeners(thread);

    // Add worker to pool and start idle timer if needed
    this.threads.push(thread);
    this.startIdleTimer(); // Start idle timer when first worker is added
    return thread;
  }

  private _setupInitializationTracking(worker: WorkerInstance) {
    const onInitComplete = () => {
      this.initializingThreads.delete(worker);
      this.predictiveCapacity -= this.maxConcurrentTasks;

      // Resume dispatch now that thread is ready
      if (this.taskQueue.length > 0) {
        this._dispatchTasks();
      }
    };

    const onInitFailed = () => {
      this.initializingThreads.delete(worker);
      this.predictiveCapacity -= this.maxConcurrentTasks;
      this.threads = this.threads.filter(t => t !== worker);

      // Emit failure event for monitoring
      this.dispatchEvent(new CustomEvent('worker.initialization.failed', {
        detail: { workerId: worker.id, workerType: this.inline ? 'inline' : 'thread' }
      }));

      // Retry dispatch for queued tasks
      if (this.taskQueue.length > 0) {
        this._dispatchTasks();
      }
    };

    worker.addEventListener('worker.initialization.completed', onInitComplete);
    worker.addEventListener('worker.initialization.failed', onInitFailed);
  }

  private _scheduleSmartRetry() {
    if (this.retryTimer) return; // Already scheduled

    // Adaptive retry delay based on workload pattern
    const retryDelay = this.maxConcurrentTasks > 1 ? 5 : 20; // ms

    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      if (this.taskQueue.length > 0) {
        this._dispatchTasks();
      }
    }, retryDelay);
  }

  // Override addEventListener to track ALL listeners for cleanup
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
        } catch (error) {
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
      { event: 'task.completed', handler: (e: any) => {
        this.dispatchEvent(new CustomEvent('task.completed', { detail: e.detail }));
        if (this.taskQueue.length > 0) this._scheduleSmartRetry();
      }},
      { event: 'task.failed', handler: (e: any) => {
        this.dispatchEvent(new CustomEvent('task.failed', { detail: e.detail }));
        if (this.taskQueue.length > 0) this._scheduleSmartRetry();
      }},
      { event: 'task.progress', handler: (e: any) => this.dispatchEvent(new CustomEvent('task.progress', { detail: e.detail })) },
      { event: 'task.spawn_request', handler: (e: any) => this.dispatchEvent(new CustomEvent('task.spawn_request', { detail: e.detail })) },
      { event: 'worker.failed', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.failed', { detail: e.detail })) },
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
    const { workerId, error, metadata } = e.detail;
    const { failureType, canRestart, restartDelay } = metadata || {};
    const failedWorker = this.threads.find(w => w.id === workerId);

    if (!failedWorker) return;

    // CRITICAL: Requeue any in-progress tasks (Worker failure ≠ Task failure)
    const runningTaskIds = failedWorker.getRunningTaskIds();
    runningTaskIds.forEach(taskId => {
      // Only requeue if both manager allows requeuing AND worker can be restarted
      if (this.requeueFailedTasks && canRestart !== false) {
        // Emit task.requeue_required event for DagScheduler to handle
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

    // Schedule replacement only if canRestart=true, under restart limit, and after delay
    if (canRestart === true) {
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
    // Release any group resources (CPU slots, etc.) immediately
    // Worker failure shouldn't block group resources for other workers
    const worker = this.threads.find(w => w.id === workerId);
    if (worker) {
      // Release CPU slots, group constraints, etc.
      // (Implementation depends on existing resource management)
      // For now, this is a placeholder - actual implementation would coordinate with groups
    }
  }

  private _removeAndReplaceWorker(workerId: string) {
    const workerIndex = this.threads.findIndex(w => w.id === workerId);
    if (workerIndex !== -1) {
      // NOW we actually remove from pool and allow replacement
      this.threads.splice(workerIndex, 1);

      if (this.threads.length < this.maxThreads) {
        const newWorker = this._createPredictiveThread();
        // Pool restart count persists across worker replacements
      }
    }
  }

  // Worker Status Inspection APIs (as specified in task)
  getWorkerIds(): string[] {
    return this.threads.map(w => w.id);
  }

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

    this._setupInitializationTracking(newWorker);
    this._setupWorkerEventListeners(newWorker);
    this.threads.push(newWorker);
    this.startIdleTimer(); // Ensure idle timer is running

    return true;
  }

  async replaceWorker(workerId: string, newConfig?: any): Promise<boolean> {
    // Same as restart for now
    return await this.restartWorker(workerId, newConfig);
  }

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
    }, this.IDLE_CHECK_INTERVAL);
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

    // Terminate the worker
    try {
      await worker.terminate();
    } catch (error) {
      console.warn(`Failed to terminate idle worker ${worker.id}:`, error);
    }
  }

  // Shutdown all workers and cleanup resources
  async shutdown(): Promise<void> {

    // Stop the centralized idle timer
    this.stopIdleTimer();

    // Stop accepting new tasks
    this.taskQueue = [];

    // Remove ALL event listeners to prevent process hanging
    this._removeAllListeners();

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

  }
}
