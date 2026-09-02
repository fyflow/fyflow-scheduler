// Universal worker wrapper for thread-based workers
// Handles message protocol and dynamically imports the actual worker class

import { WorkerMessage, ProgressData, SpawnTaskConfig, BaseWorkerContext, TaskWorkerContext, WorkerTerminationError } from './workerInterface.ts';

let WorkerClass: any = null;
let workerInstance: any = null;
let workerId: string | null = null;

// Helper functions for sending messages
function sendMessage(message: WorkerMessage) {
    message.timestamp = Date.now();
    self.postMessage(message);
}

// Create BaseWorkerContext inside the worker thread
function createBaseWorkerContext(): BaseWorkerContext {
    return {
        workerId: workerId!,
        terminateWithError: (error: Error, metadata?: { canRestart?: boolean; restartDelay?: number }) => {
            // Send termination request message (works both sync and async)
            sendMessage({
                type: 'error',
                taskId: 'worker_termination_request',
                data: {
                    message: error.message,
                    isWorkerTerminationRequest: true,
                    metadata
                }
            });
        }
    };
}

function createWorkerContext(taskId: string): TaskWorkerContext {
    const baseContext = createBaseWorkerContext();
    return {
        // Worker-level context
        workerId: baseContext.workerId,
        terminateWithError: baseContext.terminateWithError,
        // Task-level context
        taskId,
        sendProgress: (progress: number, message?: string, details?: any) => {
            const progressData: ProgressData = { progress, message, details };
            sendMessage({ type: 'progress', taskId, data: progressData });
        },
        spawnTask: (config: SpawnTaskConfig) => {
            sendMessage({ type: 'spawn_task', taskId, data: config });
        }
    };
}

self.onmessage = async (e) => {
    const { taskId, payload, action, config, workerUrl, workerId: receivedWorkerId } = e.data;

    try {
        if (action === 'init') {
            // Store the worker ID for context creation
            workerId = receivedWorkerId;

            // Dynamically import the actual worker class
            const module = await import(workerUrl);
            WorkerClass = module.default;

            if (!WorkerClass || typeof WorkerClass !== 'function') {
                throw new Error(`Worker script ${workerUrl} must export a default class`);
            }

            // Create BaseWorkerContext inside the worker thread
            const baseWorkerContext = createBaseWorkerContext();

            // Initialize worker instance with BaseWorkerContext
            workerInstance = new WorkerClass(config, baseWorkerContext);

            // Report setup timing so ThreadWrapper can emit the same
            // worker.setup.* events an inline worker emits
            sendMessage({ type: 'setup_started' });
            const setupStart = performance.now();
            await workerInstance.setup?.();
            sendMessage({ type: 'setup_completed', data: { duration: performance.now() - setupStart } });

            sendMessage({ type: 'init' });
        } else if (action === 'run' && workerInstance && workerId) {
            // Create TaskWorkerContext with both worker and task capabilities
            const context = createWorkerContext(taskId);

            // Run task with context
            const result = await workerInstance.run(payload, context);
            sendMessage({ type: 'result', taskId, data: result });
        } else if (action === 'teardown' && workerInstance) {
            // Cleanup
            await workerInstance.teardown?.();
            workerInstance = null;
            WorkerClass = null;
            workerId = null;
            sendMessage({ type: 'teardown' });
        } else {
            throw new Error(`Invalid action: ${action} or worker not initialized`);
        }
    } catch (error: any) {
        // Check if this is a worker termination request
        if (error instanceof WorkerTerminationError) {
            // Send special termination error message
            sendMessage({
                type: 'error',
                taskId,
                data: {
                    message: error.message,
                    isWorkerTerminationRequest: true,
                    metadata: error.metadata
                }
            });
        } else {
            // Regular error
            sendMessage({ type: 'error', taskId, data: error.message });
        }
    }
};