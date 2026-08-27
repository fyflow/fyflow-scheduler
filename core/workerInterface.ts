// Worker interface that all workers must implement
// Provides standardized lifecycle methods for the framework

// Special error class for worker termination requests
/**
 * Thrown into the task that was running when a worker asked to terminate itself
 * via `context.terminateWithError()`. Catch it to distinguish worker shutdown
 * from an ordinary task failure.
 */
export class WorkerTerminationError extends Error {
  public readonly metadata: { canRestart?: boolean; restartDelay?: number };

  constructor(message: string, metadata: { canRestart?: boolean; restartDelay?: number } = {}) {
    super(message);
    this.name = 'WorkerTerminationError';
    this.metadata = metadata;
  }
}

export interface WorkerConfig {
    [key: string]: any;
}

// Enhanced worker communication protocol
/**
 * Messages a worker thread sends back to its ThreadWrapper.
 *
 * `setup_started` / `setup_completed` exist so a threaded worker can report
 * setup timing from inside the thread, letting ThreadWrapper emit the same
 * `worker.setup.*` events an inline worker emits.
 */
export interface WorkerMessage {
    type: 'init' | 'teardown' | 'result' | 'error' | 'progress' | 'spawn_task'
        | 'setup_started' | 'setup_completed';
    taskId?: string; // Only for task-related messages
    data?: any; // Message-specific payload
    timestamp?: number;
}

export interface ProgressData {
    progress: number; // 0-1 (percentage)
    message?: string; // Optional progress description
    details?: any; // Additional progress details
}

export interface SpawnTaskConfig {
    id: string;
    workerType: string;
    payload: any;
    parents?: string[];
    optional?: boolean;
    retryPolicy?: { maxRetries: number; backoffMs: number };
    workerGroups?: string[];
}

// Two-Level Context Architecture
/**
 * Worker-level context, passed as the SECOND constructor argument to every
 * worker instance. Forward it to `super(config, workerContext)` - a worker that
 * only accepts `config` silently loses the ability to self-terminate.
 */
export interface BaseWorkerContext {
    /** Id of this worker instance, as reported by `WorkerManager.getWorkerIds()`. */
    workerId: string;
    /**
     * Ask the pool to tear this worker down, e.g. after detecting a corrupt
     * connection. In-flight tasks are requeued when the pool's
     * `requeueFailedTasks` allows it and `canRestart` is not false.
     */
    terminateWithError: (error: Error, metadata?: { canRestart?: boolean; restartDelay?: number }) => void;
}

/**
 * Task-level context, passed as the second argument to `run()`. Adds per-task
 * capabilities on top of {@link BaseWorkerContext}.
 */
export interface TaskWorkerContext extends BaseWorkerContext {
    /** Id of the task currently being run. */
    taskId: string;
    /**
     * Report progress as a fraction from 0 to 1 (NOT a percentage). Surfaces as
     * a `task.progress` event on the scheduler.
     */
    sendProgress: (progress: number, message?: string, details?: any) => void;
    /**
     * Create another task while this one runs. The spawned task is a descendant
     * of this one, so `parentTask.onCompleteDescendants()` waits for it.
     *
     * Spawning does not block: the call returns immediately and the task is
     * queued. A spawn naming an unregistered `workerType` emits
     * `task.spawn_failed` and fails only that spawn.
     */
    spawnTask: (config: SpawnTaskConfig) => void;
}

// Legacy alias for backwards compatibility
export interface WorkerContext extends TaskWorkerContext {}

export interface WorkerInterface {
    /**
     * Initialize the worker with configuration
     * Called once when the worker is first created
     */
    setup?(): Promise<void> | void;

    /**
     * Execute a task with the given payload
     * This is the main method where work is performed
     * @param payload - The task data to process
     * @param context - Task context with progress, spawn, and termination capabilities
     * @returns The result of the task execution
     */
    run(payload: any, context?: TaskWorkerContext): Promise<any> | any;

    /**
     * Clean up resources when the worker is terminated
     * Called when the worker is being shut down
     */
    teardown?(): Promise<void> | void;
}

/**
 * Abstract base class for workers providing common functionality
 * Workers can extend this class or implement WorkerInterface directly
 */
/**
 * Base class for workers. A worker script must `export default` a class.
 *
 * ```typescript
 * export default class MyWorker extends BaseWorker {
 *   constructor(config: WorkerConfig = {}, workerContext?: BaseWorkerContext) {
 *     super(config, workerContext);   // forward BOTH arguments
 *   }
 *   async setup() {}                  // required, may be empty
 *   async teardown() {}               // required, may be empty
 *   async run(payload: any, context?: TaskWorkerContext) {
 *     return payload.value * 2;
 *   }
 * }
 * ```
 *
 * `setup` and `teardown` are abstract: extending without them is a compile
 * error, even when there is nothing to do.
 */
export abstract class BaseWorker implements WorkerInterface {
    protected config: WorkerConfig;
    protected workerContext?: BaseWorkerContext;

    constructor(config: WorkerConfig = {}, workerContext?: BaseWorkerContext) {
        this.config = config;
        this.workerContext = workerContext;
    }

    abstract setup(): Promise<void>

    abstract run(payload: any, context?: TaskWorkerContext): Promise<any> | any;

    abstract teardown(): Promise<void> | void;

}

// Worker Instance State Management (for wrappers)
export interface WorkerInstanceState {
    state: 'initializing' | 'healthy' | 'busy' | 'failed' | 'terminated';
    lastError?: { timestamp: number; message: string; metadata: any };
}

// Required extensions for WorkerInstance (ThreadWrapper/InlineWrapper)
export interface WorkerInstanceExtensions {
    // Essential for task requeuing on worker failure
    getRunningTaskIds(): string[];
    getRunningTasks(): Array<{id: string, payload: any}>;

    // Essential for resource allocation behavior
    canAcceptTask(): boolean; // Must return false for failed workers
}

// Worker Status for inspection APIs
export interface WorkerStatus extends WorkerInstanceState {
    id: string;
    tasksCompleted: number;
    errorCount: number;
    uptime: number;
    currentTasks: string[]; // Running task IDs - needed for requeuing
    resourcesHeld: string[]; // Group names
}