// Worker interface that all workers must implement
// Provides standardized lifecycle methods for the framework

// Special error class for worker termination requests
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
export interface WorkerMessage {
    type: 'init' | 'teardown' | 'result' | 'error' | 'progress' | 'spawn_task';
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
export interface BaseWorkerContext {
    workerId: string;
    terminateWithError: (error: Error, metadata?: { canRestart?: boolean; restartDelay?: number }) => void;
}

export interface TaskWorkerContext extends BaseWorkerContext {
    taskId: string;
    sendProgress: (progress: number, message?: string, details?: any) => void;
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