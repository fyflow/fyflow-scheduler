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

  // Track initializing threads
  private initializingThreads = new Set<WorkerInstance>();

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

  // Direct execution without internal queueing
  async enqueueNoQueue(taskId: string, payload: any): Promise<any> {
    // Find an available thread
    let targetThread = this.threads.find(t =>
      t.canAcceptTask && t.canAcceptTask()
    );

    // If no available thread, create one if under limit
    // Note: initializingThreads tracks threads being created to prevent race conditions
    if (!targetThread && (this.threads.length + this.initializingThreads.size) < this.maxThreads) {
      const newThread = this.inline
        ? new InlineWrapper(this.scriptUrl, this.idleTimeout, this.maxConcurrentTasks, this.config)
        : new ThreadWrapper(this.scriptUrl, this.idleTimeout, this.maxConcurrentTasks, this.config);

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
      throw new Error(`No worker thread available (${this.threads.length}/${this.maxThreads})`);
    }

    // Execute task directly on thread
    return targetThread.runTask(taskId, payload);
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
      { event: 'task.completed', handler: (e: any) => this.dispatchEvent(new CustomEvent('task.completed', { detail: e.detail })) },
      { event: 'task.failed', handler: (e: any) => this.dispatchEvent(new CustomEvent('task.failed', { detail: e.detail })) },
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
