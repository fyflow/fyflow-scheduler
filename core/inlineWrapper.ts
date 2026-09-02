import { WorkerInstanceExtensions, WorkerInstanceState, BaseWorkerContext, TaskWorkerContext, WorkerTerminationError } from "./workerInterface.ts";

export class InlineWrapper extends EventTarget implements WorkerInstanceExtensions, WorkerInstanceState {
    runningTasks = 0;
    maxConcurrentTasks: number;
    runningTaskIds: Set<string> = new Set<string>();
    runningTaskData: Map<string, {id: string, payload: any}> = new Map<string, {id: string, payload: any}>();
    idleTimeout: number;
    lastActivityTime: number = Date.now();
    workerModule: any = null;
    workerInstance: any = null;
    scriptUrl: string;
    config: any;
    id: string;
    initializing = false;

    // WorkerInstanceState implementation
    state: 'initializing' | 'healthy' | 'busy' | 'failed' | 'terminated' = 'initializing';
    lastError?: { timestamp: number; message: string; metadata: any };

    // Stats for WorkerStatus
    tasksCompleted = 0;
    errorCount = 0;
    createdAt: number = Date.now();
    constructor(scriptUrl: string, idleTimeout = 5000, maxConcurrentTasks = 1, config = {}) {
        super();
        this.scriptUrl = scriptUrl;
        this.idleTimeout = idleTimeout;
        this.maxConcurrentTasks = maxConcurrentTasks;
        this.config = config;
        this.id = `inline-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    }

    canAcceptTask(): boolean {
        return this.runningTasks < this.maxConcurrentTasks && !this.initializing && this.state !== 'failed' && this.state !== 'terminated';
    }

    // WorkerInstanceExtensions implementation
    getRunningTaskIds(): string[] {
        return Array.from(this.runningTaskIds);
    }

    getRunningTasks(): Array<{id: string, payload: any}> {
        return Array.from(this.runningTaskData.values());
    }

    // terminateWithError implementation - throws error for clean task completion
    private createTerminateWithError(): (error: Error, metadata?: { canRestart?: boolean; restartDelay?: number }) => void {
        return (error: Error, metadata = {}) => {
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

    // Create BaseWorkerContext for worker constructor
    private createBaseWorkerContext(): BaseWorkerContext {
        return {
            workerId: this.id,
            terminateWithError: this.createTerminateWithError()
        };
    }

    get idle(): boolean {
        return this.runningTasks === 0;
    }

    updateActivity() {
        this.lastActivityTime = Date.now();
    }

    async _loadWorkerModule(): Promise<any> {
        if (!this.workerModule) {
            this.workerModule = await import(this.scriptUrl);
        }
        return this.workerModule;
    }

    async _createWorkerInstance(): Promise<any> {
        if (this.workerInstance) {
            return this.workerInstance;
        }
        this.initializing = true;

        let instance = null;
        try {
            const module = await this._loadWorkerModule();

            // Expect class-based worker (exports default class)
            if (!module.default || typeof module.default !== 'function' || !module.default.prototype) {
                throw new Error(`Worker script ${this.scriptUrl} must export a default class`);
            }

            const initStartTime = performance.now();
            this.dispatchEvent(new CustomEvent('worker.initialization.started', {
                detail: { workerId: this.id, workerType: 'inline', timestamp: Date.now() }
            }));

            // Instantiate worker class with BaseWorkerContext (as specified in task)
            const baseWorkerContext = this.createBaseWorkerContext();
            instance = new module.default(this.config, baseWorkerContext);

            // Call setup if it exists
            if (instance.setup) {
                this.dispatchEvent(new CustomEvent('worker.setup.started', {
                    detail: { workerId: this.id, workerType: 'inline', timestamp: Date.now() }
                }));

                const setupStartTime = performance.now();
                await instance.setup();

                this.dispatchEvent(new CustomEvent('worker.setup.completed', {
                    detail: {
                        workerId: this.id,
                        workerType: 'inline',
                        timestamp: Date.now(),
                        duration: performance.now() - setupStartTime
                    }
                }));
            }

            // Threaded workers emit this once the worker thread confirms init;
            // inline workers reach the equivalent point here
            this.dispatchEvent(new CustomEvent('worker.initialization.completed', {
                detail: {
                    workerId: this.id,
                    workerType: 'inline',
                    timestamp: Date.now(),
                    duration: performance.now() - initStartTime
                }
            }));

            this.workerInstance = instance;
            this.initializing = false;
            return instance;

        } catch (error) {
            // Always attempt teardown if instance was created
            if (instance?.teardown) {
                try {
                    await instance.teardown();
                } catch (teardownError) {
                    console.warn('Teardown failed during initialization cleanup:', teardownError);
                }
            }

            // Update worker state
            this.state = 'failed';
            this.lastError = {
                timestamp: Date.now(),
                message: error instanceof Error ? error.message : String(error),
                metadata: { stage: instance ? 'setup' : 'constructor' }
            };
            this.errorCount++;

            // Emit specific initialization failure event (maintain namespace consistency)
            this.dispatchEvent(new CustomEvent('worker.initialization.failed', {
                detail: {
                    workerId: this.id,
                    workerType: 'inline',
                    timestamp: Date.now(),
                    error,
                    stage: instance ? 'setup' : 'constructor'
                }
            }));

            // Also emit unified worker.failed event (as specified in task)
            this.dispatchEvent(new CustomEvent('worker.failed', {
                detail: {
                    workerId: this.id,
                    error,
                    metadata: {
                        failureType: 'initialization',
                        stage: instance ? 'setup' : 'constructor'
                    },
                    timestamp: Date.now()
                }
            }));

            this.initializing = false;
            throw error;
        }
    }


    async runTask(taskId: string, payload: any): Promise<any> {
        // Measure full task execution time including all coordination overhead
        
        if (!this.canAcceptTask()) {
            throw new Error("Worker at maximum concurrent task capacity");
        }
        
        // InlineWorkers don't consume CPU slots - they run in the same process
        this.updateActivity();
        this.runningTasks++;
        this.runningTaskIds.add(taskId);
        this.runningTaskData.set(taskId, {id: taskId, payload}); // Track task data for requeuing
        
        // Emit task started event when task actually begins execution
        this.dispatchEvent(new CustomEvent('task.started', {
            detail: { taskId, workerId: this.id, workerType: 'inline', timestamp: Date.now() }
        }));
        let fullExecutionStart
        try {
            const workerInstance = await this._createWorkerInstance();
            fullExecutionStart = performance.now();

            // Create TaskWorkerContext with both worker and task capabilities
            const context: TaskWorkerContext = {
                // Worker-level context (inherited from BaseWorkerContext)
                workerId: this.id,
                terminateWithError: this.createTerminateWithError(),
                // Task-level context
                taskId,
                sendProgress: (progress: number, message?: string, details?: any) => {
                    this.dispatchEvent(new CustomEvent('task.progress', {
                        detail: {
                            taskId,
                            workerId: this.id,
                            workerType: 'inline',
                            progress,
                            message,
                            details,
                            timestamp: Date.now()
                        }
                    }));
                },
                spawnTask: (config: any) => {
                    this.dispatchEvent(new CustomEvent('task.spawn_request', {
                        detail: {
                            taskId,
                            workerId: this.id,
                            workerType: 'inline',
                            spawnConfig: config,
                            timestamp: Date.now()
                        }
                    }));
                }
            };

            const result = await workerInstance.run(payload, context);

            // Calculate full execution time including all coordination overhead
            const fullExecutionTime = performance.now() - fullExecutionStart;

            // Emit task completed event with full execution timing
            this.dispatchEvent(new CustomEvent('task.completed', {
                detail: {
                    taskId,
                    workerId: this.id,
                    workerType: 'inline',
                    result,
                    timestamp: Date.now(),
                    executionTime: fullExecutionTime // Full task execution time including coordination
                }
            }));
            
            return result;
            
        } catch (error) {
            const fullExecutionTime = 0//performance.now() - fullExecutionStart;
            // Calculate execution time even for failed tasks

            // Check if this is a worker termination error
            if (error instanceof WorkerTerminationError) {
                // First emit task failed event
                this.dispatchEvent(new CustomEvent('task.failed', {
                    detail: { taskId, workerId: this.id, workerType: 'inline', error, timestamp: Date.now(), executionTime: fullExecutionTime }
                }));

                // Then handle worker termination request
                this._handleTerminationRequest(error.metadata);
                throw error;
            } else {
                // Regular task error - just emit task failed event
                this.dispatchEvent(new CustomEvent('task.failed', {
                    detail: { taskId, workerId: this.id, workerType: 'inline', error, timestamp: Date.now(), executionTime: fullExecutionTime }
                }));
                throw error;
            }

        } finally {
            // Always cleanup task tracking
            this.runningTasks--;
            this.runningTaskIds.delete(taskId);
            this.runningTaskData.delete(taskId); // Also clean up task data

            // Update task stats
            this.tasksCompleted++;
            // Only update state if not already failed
            if (this.state !== 'failed' && this.state !== 'terminated') {
              this.state = this.runningTasks > 0 ? 'busy' : 'healthy';
            }

            // Update activity time when task completes
            this.updateActivity();
        }
    }

    async terminate() {

        // Call teardown on worker instance if it exists
        await this._destroyWorkerInstance();

        // InlineWorkers don't hold CPU slots - nothing to release
        this.workerModule = null;
        this.runningTasks = 0;
        this.runningTaskIds.clear();
    }

    async _destroyWorkerInstance() {
        // Nothing was ever constructed, so no worker is being destroyed and there
        // is no lifecycle to report.
        if (!this.workerInstance) return;

        this.dispatchEvent(new CustomEvent('worker.teardown.started', {
            detail: { workerId: this.id, workerType: 'inline', timestamp: Date.now() }
        }));

        const teardownStartTime = performance.now();

        try {
            // teardown() is optional on WorkerInterface - the events are not. They
            // report that a worker is being destroyed, which is true whether or not
            // it chose to implement a hook, so a consumer tracking worker state can
            // rely on seeing them. Guarding the events on the method's existence
            // meant a worker without teardown() was destroyed silently, and made
            // inline pools emit a different set from threaded ones, which the
            // worker-side wrapper has always got right via `teardown?.()`.
            await this.workerInstance.teardown?.();

            this.dispatchEvent(new CustomEvent('worker.teardown.completed', {
                detail: {
                    workerId: this.id,
                    workerType: 'inline',
                    timestamp: Date.now(),
                    duration: performance.now() - teardownStartTime
                }
            }));
        } catch (teardownError) {
            // Emit failure event but continue with cleanup
            this.dispatchEvent(new CustomEvent('worker.teardown.failed', {
                detail: { workerId: this.id, workerType: 'inline', timestamp: Date.now(), error: teardownError }
            }));

            // Log for debugging but don't throw
            console.warn(`Worker ${this.id} teardown failed:`, teardownError);
        }

        // Always proceed with de-instantiation regardless of teardown success
        this.workerInstance = null;
        // Instance becomes eligible for garbage collection
    }
}