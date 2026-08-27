// Node.js-specific worker wrapper
// Uses parentPort from worker_threads

import { parentPort, isMainThread } from 'node:worker_threads';
import { WorkerInterface, WorkerMessage, WorkerContext, ProgressData, SpawnTaskConfig, BaseWorkerContext, TaskWorkerContext, WorkerTerminationError } from './workerInterface.ts';

if (isMainThread) {
    throw new Error('workerWrapper.node.ts should only be used in worker threads');
}

let WorkerClass: any = null;
let workerInstance: any = null;
let workerId: string | null = null;

// Helper functions for sending messages
function sendMessage(message: WorkerMessage) {
    message.timestamp = Date.now();
    parentPort?.postMessage(message);
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

parentPort?.on('message', async (data: any) => {
    const { taskId, payload, action, config, workerUrl, workerId: receivedWorkerId } = data;

    try {
        if (action === 'init') {
            // Store the worker ID for context creation
            workerId = receivedWorkerId;

            // Dynamically import the actual worker class
            const workerModule = await import(workerUrl.startsWith('data') ?  new URL(workerUrl) : workerUrl);
            WorkerClass = workerModule.default as WorkerInterface;

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
            const setupStart = Date.now();
            await workerInstance.setup?.();
            sendMessage({ type: 'setup_completed', data: { duration: Date.now() - setupStart } });

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
            throw new Error(`Invalid action: "${action}" or worker not initialized`);
        }
    } catch (error: any) {
        // Check if this is a worker termination request
        // Use name-based check instead of instanceof to avoid cross-module issues
        if (error.name === 'WorkerTerminationError' && error.metadata) {
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
            // Give the message time to be sent before worker terminates
            await new Promise(resolve => setTimeout(resolve, 10));
            // Gracefully exit the worker thread instead of letting exception kill it
            process?.exit?.(0);
        } else {
            // Regular error
            sendMessage({ type: 'error', taskId, data: error.message });
        }
    }
});
