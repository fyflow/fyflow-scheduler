// Universal worker wrapper for thread-based workers
// Handles message protocol and dynamically imports the actual worker class

import type { WorkerMessage, ProgressData, SpawnTaskConfig, BaseWorkerContext, TaskWorkerContext } from './workerInterface.ts';
import { isWorkerTerminationError } from './workerInterface.ts';
import { WorkerMessageChannel } from './messageChannel.ts';

let WorkerClass: any = null;
let workerInstance: any = null;
let workerId: string | null = null;

// Messages that end something: an attempt, setup, the worker. A port is FIFO,
// so flushing the emitted events first is what guarantees they arrive before it
const ENDING_MESSAGES = new Set<WorkerMessage['type']>([
    'result', 'error', 'init', 'setup_completed', 'setup_failed', 'teardown'
]);

// Helper functions for sending messages
function sendMessage(message: WorkerMessage) {
    if (ENDING_MESSAGES.has(message.type)) channel.flush();
    message.timestamp = Date.now();
    self.postMessage(message);
}

// Created at load, with the gate closed: a 'gate' message can arrive before
// 'init' has finished, and must have somewhere to land. 'init' configures it.
const channel = new WorkerMessageChannel(
    (events) => sendMessage({ type: 'messages', data: events })
);

// The running attempts' controllers, so a 'cancel' can reach the signal of the
// attempt it names
const controllers = new Map<string, AbortController>();

// terminateWithError() is a request: it posts a message and returns, and never
// throws, so it is safe from a timer or a callback. Sent with the task's id from
// a task's context, that task fails and the worker's others are requeued; sent
// from the worker's own context, there is no task to blame and every in-flight
// task is requeued.
function requestTermination(
    taskId: string,
    error: Error,
    metadata?: { canRestart?: boolean; restartDelay?: number }
) {
    sendMessage({
        type: 'error',
        taskId,
        data: {
            message: error.message,
            isWorkerTerminationRequest: true,
            metadata
        }
    });
}

// Create BaseWorkerContext inside the worker thread
function createBaseWorkerContext(): BaseWorkerContext {
    return {
        workerId: workerId!,
        terminateWithError: (error, metadata) =>
            requestTermination('worker_termination_request', error, metadata),
        emit: (name, data) => channel.emitWorker(name, data)
    };
}

function createWorkerContext(
    taskId: string,
    stream: ReturnType<WorkerMessageChannel['openAttempt']>,
    signal: AbortSignal
): TaskWorkerContext {
    const baseContext = createBaseWorkerContext();
    return {
        // Worker-level context, except that terminating through it fails this
        // task in particular
        workerId: baseContext.workerId,
        terminateWithError: (error, metadata) => requestTermination(taskId, error, metadata),
        // Task-level context
        taskId,
        sendProgress: (progress: number, message?: string, details?: any) => {
            const progressData: ProgressData = { progress, message, details };
            sendMessage({ type: 'progress', taskId, data: progressData });
        },
        spawnTask: (config: SpawnTaskConfig) => {
            sendMessage({ type: 'spawn_task', taskId, data: config });
        },
        emit: (name, data) => channel.emitTask(stream, name, data),
        signal
    };
}

self.onmessage = async (e) => {
    const { taskId, payload, action, config, workerUrl, workerId: receivedWorkerId, gate, messages } = e.data;

    // Never replies, and is handled before the action dispatch: a cancel can
    // arrive after its attempt has ended or after teardown, and the 'Invalid
    // action' branch below would report it as a failure of that task. Abort
    // with no reason gives the standard AbortError.
    if (action === 'cancel') {
        controllers.get(taskId)?.abort(e.data.reason);
        return;
    }

    // Handled before the action dispatch, whatever state the worker is in: a
    // gate change can arrive before init completes or after teardown, and must
    // not reach the 'Invalid action' branch below
    if (action === 'gate') {
        channel.setGate(gate.task, gate.worker);
        return;
    }

    try {
        if (action === 'init') {
            // Store the worker ID for context creation
            workerId = receivedWorkerId;
            // Before the first await, so a 'gate' sent after this message is
            // applied after it rather than overwritten by it
            if (messages) channel.configure(messages);
            if (gate) channel.setGate(gate.task, gate.worker);

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
            try {
                await workerInstance.setup?.();
            } catch (setupError: any) {
                // Report the setup failure in its own right before letting it
                // become an initialization failure, so worker.setup.started
                // always reaches a terminal event of its own.
                sendMessage({ type: 'setup_failed', data: { message: setupError?.message ?? String(setupError) } });

                // The instance exists - it was constructed before setup ran - so if
                // setup threw partway it may hold something only teardown() frees.
                // Terminating the thread reclaims memory but not a remote session, a
                // lock, or a registered listener, so this ran nowhere and leaked.
                sendMessage({ type: 'teardown_started' });
                try {
                    await workerInstance.teardown?.();
                    channel.closeWorker();
                    sendMessage({ type: 'teardown' });
                } catch (teardownError: any) {
                    // Reported, never rethrown: the setup error below is the one the
                    // pool acts on, and a cleanup failure must not replace it.
                    channel.closeWorker();
                    sendMessage({ type: 'error', taskId: 'teardown', data: { message: teardownError?.message ?? String(teardownError) } });
                }
                workerInstance = null;
                WorkerClass = null;

                throw setupError;
            }
            sendMessage({ type: 'setup_completed', data: { duration: performance.now() - setupStart } });

            sendMessage({ type: 'init' });
        } else if (action === 'run' && workerInstance && workerId) {
            // Create TaskWorkerContext with both worker and task capabilities
            const stream = channel.openAttempt(taskId);
            // Before the first await, so a 'cancel' that follows this message
            // finds it
            const controller = new AbortController();
            controllers.set(taskId, controller);
            const context = createWorkerContext(taskId, stream, controller.signal);

            // Run task with context. The attempt's emits close when run() ends,
            // either way, so a stray promise cannot add to a settled attempt;
            // what it emitted is flushed ahead of the result or error.
            let result;
            try {
                result = await workerInstance.run(payload, context);
            } finally {
                channel.close(stream);
                if (controllers.get(taskId) === controller) controllers.delete(taskId);
            }
            sendMessage({ type: 'result', taskId, data: result });
        } else if (action === 'teardown' && workerInstance) {
            // Cleanup
            await workerInstance.teardown?.();
            workerInstance = null;
            WorkerClass = null;
            workerId = null;
            // Nothing after teardown
            channel.closeWorker();
            sendMessage({ type: 'teardown' });
        } else {
            throw new Error(`Invalid action: ${action} or worker not initialized`);
        }
    } catch (error: any) {
        // Check if this is a worker termination request
        // By name as well as identity: in the browser build this file is bundled
        // with its own copy of the class, so a worker's error fails instanceof
        if (isWorkerTerminationError(error)) {
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