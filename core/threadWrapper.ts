// Compile-time polyfill for Node.js
// Uncomment the line below when building for Node.js
// import { Worker } from 'worker_threads';
import getWorkerUrl from "./workerWrapperUrl.ts";
import { WorkerInstanceExtensions, WorkerInstanceState, WorkerStatus, BaseWorkerContext, TaskWorkerContext, WorkerTerminationError } from "./workerInterface.ts";

export class ThreadWrapper extends EventTarget implements WorkerInstanceExtensions, WorkerInstanceState {
    worker: Worker | null = null;
    runningTasks = 0;
    maxConcurrentTasks: number;
    idleTimeout: number;
    callbacks = new Map<string, {resolve: Function, reject: Function}>();
    taskStartTimes = new Map<string, number>(); // Track task execution start times
    runningTaskData = new Map<string, {id: string, payload: any}>(); // Track task data for requeuing
    lastActivityTime: number = Date.now();
    config: any;
    id: string;
    initialized = false;
    originalScriptUrl: string;
    initializing = false;
    // Internal task queue to prevent worker message queue flooding
    taskQueue: Array<{taskId: string, payload: any, resolve: Function, reject: Function}> = [];

    // WorkerInstanceState implementation
    state: 'initializing' | 'healthy' | 'busy' | 'failed' | 'terminated' = 'initializing';
    lastError?: { timestamp: number; message: string; metadata: any };

    // Stats for WorkerStatus
    tasksCompleted = 0;
    errorCount = 0;
    createdAt = Date.now();

    // Pending termination request for post-task handling
    private pendingTerminationRequest: {
        error: Error;
        metadata: any;
        timestamp: number;
    } | null = null;

    //DENO SPECIFIC USE OF WORKER, node uses worker_threads
    constructor(scriptUrl: string, idleTimeout = 5000, maxConcurrentTasks = 1, config = {}) {
      super();
      //ideally we shim this
      this.originalScriptUrl = (scriptUrl || "").toString();
      // Compile-time path selection - will be replaced during build
      this.idleTimeout = idleTimeout;
      this.maxConcurrentTasks = maxConcurrentTasks;
      this.config = config;
      this.id = `thread-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;


    }

    canAcceptTask(): boolean {
      // Only check running tasks - respect maxConcurrentTasks limit
      // Internal queue is for buffering during async initialization, not for extra capacity
      const totalTasks = this.runningTasks + this.taskQueue.length;
      return totalTasks < this.maxConcurrentTasks && !this.initializing && this.state !== 'failed' && this.state !== 'terminated';
    }

    // WorkerInstanceExtensions implementation
    getRunningTaskIds(): string[] {
        const runningTaskIds = Array.from(this.callbacks.keys());
        const queuedTaskIds = this.taskQueue.map(task => task.taskId);
        return [...runningTaskIds, ...queuedTaskIds];
    }

    getRunningTasks(): Array<{id: string, payload: any}> {
        const runningTasks = Array.from(this.runningTaskData.values());
        const queuedTasks = this.taskQueue.map(task => ({id: task.taskId, payload: task.payload}));
        return [...runningTasks, ...queuedTasks];
    }

    // terminateWithError implementation - throws error for clean task completion
    private createTerminateWithError(): (error: Error, metadata?: { canRestart?: boolean; restartDelay?: number }) => void {
        return (error: Error, metadata = {}) => {
            // Store termination request for post-task handling
            this.pendingTerminationRequest = {
                error,
                metadata: {
                    failureType: 'runtime',
                    canRestart: metadata.canRestart,
                    restartDelay: metadata.restartDelay,
                    ...metadata
                },
                timestamp: Date.now()
            };

            // Throw special error to fail the task cleanly
            throw new WorkerTerminationError(error.message, metadata);
        };
    }

    // Handle termination request after task completion
    private _handleTerminationRequest(metadata: any) {
        // Update worker state
        this.state = 'failed';
        this.lastError = {
            timestamp: Date.now(),
            message: 'Worker termination requested',
            metadata
        };
        this.errorCount++;

        // Emit worker.failed event for external listeners
        this.dispatchEvent(new CustomEvent('worker.failed', {
            detail: {
                workerId: this.id,
                error: new Error('Worker termination requested'),
                metadata: {
                    failureType: 'self_termination',
                    ...metadata
                },
                timestamp: Date.now()
            }
        }));

        // Emit termination request event for WorkerManager to handle
        this.dispatchEvent(new CustomEvent('worker.termination_requested', {
            detail: {
                workerId: this.id,
                metadata,
                timestamp: Date.now()
            }
        }));

        // Also emit user-facing self-terminated event
        this.dispatchEvent(new CustomEvent('worker.self_terminated', {
            detail: {
                workerId: this.id,
                metadata,
                timestamp: Date.now()
            }
        }));
    }

    // Set up detection for unexpected worker process exits
    private _setupUnexpectedExitDetection() {
        if (!this.worker) return;

        // Handle worker errors
        this.worker.onerror = (error) => {
            this._handleUnexpectedExit('error', error);
        };

        // Handle worker termination (for environments that support it)
        if ('onclose' in this.worker) {
            (this.worker as any).onclose = () => {
                this._handleUnexpectedExit('close', new Error('Worker process closed unexpectedly'));
            };
        }

        // Handle message errors
        this.worker.onmessageerror = (error) => {
            this._handleUnexpectedExit('messageerror', error);
        };
    }

    // Handle unexpected worker exit/error
    private _handleUnexpectedExit(type: string, error: any) {
        // Only handle if worker is not already marked as terminated/failed
        if (this.state === 'terminated' || this.state === 'failed') {
            return;
        }

        // Update worker state
        this.state = 'failed';
        this.lastError = {
            timestamp: Date.now(),
            message: `Worker ${type}: ${error.message || error}`,
            metadata: { failureType: 'unexpected_exit', exitType: type }
        };
        this.errorCount++;

        // Emit worker failure event
        this.dispatchEvent(new CustomEvent('worker.failed', {
            detail: {
                workerId: this.id,
                error: new Error(`Worker ${type}: ${error.message || error}`),
                metadata: {
                    failureType: 'unexpected_exit',
                    exitType: type,
                    canRestart: true, // Unexpected exits can typically be restarted
                    restartDelay: 1000
                },
                timestamp: Date.now()
            }
        }));

        // Clean up worker state
        this.worker = null;
        this.initialized = false;
        this.runningTasks = 0;
        this.callbacks.clear();
        this.taskStartTimes.clear();
        this.runningTaskData.clear();
    }

    // Create BaseWorkerContext for worker constructor
    private createBaseWorkerContext(): BaseWorkerContext {
        return {
            workerId: this.id,
            terminateWithError: this.createTerminateWithError()
        };
    }

    get idle(): boolean {
      return this.runningTasks === 0 && this.taskQueue.length === 0;
    }

    updateActivity() {
      this.lastActivityTime = Date.now();
    }

    async _ensureInitialized() {
      if (this.initialized) return;
      this.initializing = true
      const workerWrapperURL = await getWorkerUrl();
      // console.log('workerWrapperURL', workerWrapperURL)
      this.worker = new Worker(workerWrapperURL, { type: "module" });

      // Set up unexpected exit detection
      this._setupUnexpectedExitDetection();

      this.worker.onmessage = (e) => {
        // console.log("MAIN THREAD: Received message from worker", e?.data ? e.data : e);
        const message = e.data ? e.data : e;
        const { type, taskId, data } = message;

        // Handle different message types
        switch (type) {
          case 'init':
            // console.log('worker.initialization.completed', this.id)
            this.initialized = true;
            this.dispatchEvent(new CustomEvent('worker.initialization.completed', {
              detail: { workerId: this.id }
            }));
            return;

          case 'teardown':
            this.dispatchEvent(new CustomEvent('worker.teardown.completed', {
              detail: { workerId: this.id }
            }));
            return;

          case 'progress':
            if (taskId) {
              this.dispatchEvent(new CustomEvent('task.progress', {
                detail: {
                  taskId,
                  workerId: this.id,
                  workerType: 'thread',
                  progress: data.progress,
                  message: data.message,
                  details: data.details,
                  timestamp: Date.now()
                }
              }));
            }
            return;

          case 'spawn_task':
            if (taskId) {
              this.dispatchEvent(new CustomEvent('task.spawn_request', {
                detail: {
                  taskId,
                  workerId: this.id,
                  workerType: 'thread',
                  spawnConfig: data,
                  timestamp: Date.now()
                }
              }));
            }
            return;

          case 'result':
          case 'error':{
            // Check if this is a worker termination request (either explicit or during task execution)
            if (data.isWorkerTerminationRequest) {
              // Handle worker termination request
              this._handleTerminationRequest(data.metadata);

              // If this was during task execution, still need to handle the task callback
              if (taskId !== 'worker_termination_request') {
                const cb = this.callbacks.get(taskId);
                if (cb) {
                  this.callbacks.delete(taskId);
                  cb.reject(new Error(data.message));
                }
              }
              return;
            }

            const cb = this.callbacks.get(taskId);
            if (!cb) return;

            // Calculate execution time
            const startTime = this.taskStartTimes.get(taskId);
            const executionTime = startTime ? performance.now() - startTime : 0;

            this.callbacks.delete(taskId);
            this.taskStartTimes.delete(taskId);
            this.runningTaskData.delete(taskId); // Clean up task data
            this.runningTasks--;

            // Update task stats and state
            this.tasksCompleted++;
            // Only update state if not already failed
            if (this.state !== 'failed' && this.state !== 'terminated') {
              this.state = this.runningTasks > 0 ? 'busy' : 'healthy';
            }

            // Emit task completion/failure events
            if (type === 'error') {
              this.dispatchEvent(new CustomEvent('task.failed', {
                detail: { taskId, workerId: this.id, workerType: 'thread', error: data, timestamp: Date.now(), executionTime }
              }));
              cb.reject(new Error(data.message || data));
            } else {
              this.dispatchEvent(new CustomEvent('task.completed', {
                detail: { taskId, workerId: this.id, workerType: 'thread', result: data, timestamp: Date.now(), executionTime }
              }));
              cb.resolve(data);
            }

            // Process any queued tasks now that capacity is available
            this._processTaskQueue();

            // Update activity time when task completes
            this.updateActivity();
            return;
          }
          default:
            console.warn(`Unknown message type from worker: ${type}`);
        }
      };
      //ideally we shim this
      if('on' in this.worker ){
        // console.log('added listener with "on"')
        //@ts-ignore for node
        this.worker.on("message", (message) => {
          // For Node.js, wrap the message in an event-like object
          this?.worker?.onmessage?.({ data: message } as MessageEvent);
        })
      }

      this.dispatchEvent(new CustomEvent('worker.initialization.started', {
        detail: { workerId: this.id, timestamp: Date.now() }
      }));

      return new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          this.initializing = false;
          this.removeEventListener('worker.initialization.completed', onInit);
          reject(new Error('Worker initialization timeout'));
        }, 30000); // 10 second timeout

        const onInit = () => {
          clearTimeout(timeout);
          this.initializing = false;
          this.removeEventListener('worker.initialization.completed', onInit);
          resolve();
        };

        this.addEventListener('worker.initialization.completed', onInit);
        // Convert relative paths to absolute URLs for proper worker imports
        const absoluteWorkerUrl = this.originalScriptUrl.startsWith('http') || this.originalScriptUrl.startsWith('file:')
          ? this.originalScriptUrl
          : new URL(this.originalScriptUrl, import.meta.url).href;
        // Pass workerId instead of BaseWorkerContext (functions can't be serialized)
        this.worker?.postMessage({
          taskId: 'init',
          action: 'init',
          config: this.config,
          workerUrl: absoluteWorkerUrl,
          workerId: this.id
        });
      });
    }
  
    async runTask(taskId: string, payload: any): Promise<any> {
      if (!this.canAcceptTask()) {
        throw new Error("Worker at maximum concurrent task capacity");
      }

      // Queue task FIRST (synchronously) to update canAcceptTask() immediately
      // This prevents race conditions where multiple tasks are dispatched before queue updates
      return new Promise(async (resolve, reject) => {
        this.taskQueue.push({ taskId, payload, resolve, reject });

        // Ensure worker is initialized before processing tasks
        await this._ensureInitialized();

        this.updateActivity();

        // Process the queue now that worker is ready
        this._processTaskQueue();
      });
    }

    private _processTaskQueue() {
      // Process queued tasks up to concurrent limit
      while (this.taskQueue.length > 0 && this.runningTasks < this.maxConcurrentTasks && this.initialized) {
        const task = this.taskQueue.shift()!;
        this.runningTasks++;

        // Emit task started event when task actually begins execution
        this.dispatchEvent(new CustomEvent('task.started', {
          detail: { taskId: task.taskId, workerId: this.id, workerType: 'thread', timestamp: Date.now() }
        }));

        this.callbacks.set(task.taskId, { resolve: task.resolve, reject: task.reject });
        this.taskStartTimes.set(task.taskId, performance.now()); // Record execution start time
        this.runningTaskData.set(task.taskId, {id: task.taskId, payload: task.payload}); // Track task data for requeuing
        this.worker?.postMessage({ taskId: task.taskId, payload: task.payload, action: 'run' });
      }
    }
  
    async terminate() {
      // Send teardown message to worker if initialized
      if (this.initialized) {
        this.dispatchEvent(new CustomEvent('worker.teardown.started', {
          detail: { workerId: this.id, timestamp: Date.now() }
        }));

        try {
          // Give worker a chance to clean up
          this.worker?.postMessage({ taskId: 'teardown', action: 'teardown' });

          // Wait a short time for teardown response
          await new Promise<void>((resolve) => {
            const timeout = setTimeout(resolve, 1000); // 1 second timeout
            const onTeardown = () => {
              clearTimeout(timeout);
              this.removeEventListener('worker.teardown.completed', onTeardown);
              resolve();
            };
            this.addEventListener('worker.teardown.completed', onTeardown);
          });
        } catch (error) {
          this.dispatchEvent(new CustomEvent('worker.teardown.failed', {
            detail: { workerId: this.id, error }
          }));
        }
      }

      this.worker?.terminate();
      this.worker = null;
      this.runningTasks = 0;
      this.callbacks.clear();
      this.initialized = false;
    }
  }
  