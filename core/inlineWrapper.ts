import type { WorkerInstanceExtensions, WorkerInstanceState, BaseWorkerContext, TaskWorkerContext } from "./workerInterface.ts";
import { CancelledError, WorkerTerminationError, isWorkerTerminationError } from "./workerInterface.ts";
import { WorkerMessageChannel } from "./messageChannel.ts";
import { MessageStream } from "./messageChannel.ts";
import type { ChannelEvent, MessageOptions, MessageSink } from "./messageChannel.ts";

// Handed to an emit() that arrives once its run has ended or been abandoned, so
// a stray emit has nowhere to land. Shared: a closed stream accepts nothing.
const CLOSED_STREAM = new MessageStream();
CLOSED_STREAM.closed = true;

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
    // One termination per worker: a second terminateWithError() call, or a
    // thrown WorkerTerminationError after a call, must not report it again
    private terminationRequested = false;
    private terminationHandled = false;

    // The same channel a threaded worker runs, so a pool switching between
    // inline and threaded delivers the same events: same gate, cap, clone and
    // batching. Delivery is deferred to a microtask, so a host listener never
    // runs inside the worker's own emit() call.
    private messages: WorkerMessageChannel;
    /** Each task's stream, until its run has settled and been reported, or is abandoned. */
    private messageStreams = new Map<string, MessageStream>();
    private undelivered: ChannelEvent[] = [];
    private deliveryScheduled = false;
    /** Set by the pool. Receives each batch, already filtered. */
    messageSink?: MessageSink;

    /**
     * Each running attempt's controller, behind `context.signal`, and whether a
     * cancel has been asked for. Recorded synchronously, while the abort itself
     * is deferred: an attempt still waiting for the worker instance checks the
     * record and never starts.
     */
    private attempts = new Map<string, { controller: AbortController; cancelled: boolean; reason?: unknown }>();

    // WorkerInstanceState implementation
    state: 'initializing' | 'healthy' | 'busy' | 'failed' | 'terminated' = 'initializing';
    lastError?: { timestamp: number; message: string; metadata: any };

    // Stats for WorkerStatus
    tasksCompleted = 0;
    errorCount = 0;
    createdAt: number = Date.now();
    constructor(scriptUrl: string, idleTimeout = 5000, maxConcurrentTasks = 1, config = {}, messages?: MessageOptions) {
        super();
        this.messages = new WorkerMessageChannel((events) => this._queueDelivery(events), messages);
        this.scriptUrl = scriptUrl;
        this.idleTimeout = idleTimeout;
        this.maxConcurrentTasks = maxConcurrentTasks;
        this.config = config;
        this.id = `inline-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    }

    /** Open or close the message gate. Takes effect at once: the channel is in-process. */
    setMessageGate(task: boolean, worker: boolean) {
        this.messages.setGate(task, worker);
    }

    private _queueDelivery(events: ChannelEvent[]) {
        for (const event of events) this.undelivered.push(event);
        if (this.deliveryScheduled) return;
        this.deliveryScheduled = true;
        queueMicrotask(() => this._deliver());
    }

    /** Dispatch everything flushed so far. A task's events go only while its stream is live. */
    private _deliver(retiring?: string) {
        this.deliveryScheduled = false;
        const events = this.undelivered;
        this.undelivered = [];
        this.messageSink?.(
            events.filter(event => event.taskId === undefined || event.taskId === retiring || this.messageStreams.has(event.taskId)),
            this.id,
            'inline'
        );
    }

    /**
     * Flush and dispatch now, synchronously. Called before every event that
     * ends something - a task's completed/failed, a setup or teardown outcome -
     * so every event emitted before it arrives first. On a thread the port's
     * FIFO order gives that for free; here the settle is synchronous, so it has
     * to be done by hand.
     */
    //
    // `retiring` names a task whose stream has just been removed from
    // messageStreams: its events are still delivered, this once.
    private _deliverNow(retiring?: string) {
        this.messages.flush();
        if (this.undelivered.length > 0) this._deliver(retiring);
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

    // The run itself cannot be stopped. Removing it from runningTaskIds is what
    // runTask checks before reporting anything, so whatever it does from here on
    // is discarded rather than attributed to the task's live attempt.
    //
    // Its emitted events are discarded with it, unless `keepMessages`: a task
    // failing itself through terminateWithError() is forgotten at once, but what
    // it emitted before asking still belongs to its attempt and is delivered
    // ahead of its task.failed.
    forgetTask(taskId: string, keepMessages = false): void {
        if (!keepMessages) {
            const stream = this.messageStreams.get(taskId);
            if (stream) this.messages.discard(stream);
            this.messageStreams.delete(taskId);
        }
        if (!this.runningTaskIds.delete(taskId)) return;
        this.runningTasks--;
        this.runningTaskData.delete(taskId);
    }

    hasTask(taskId: string): boolean {
        return this.runningTaskIds.has(taskId);
    }

    cancelTask(taskId: string, reason?: unknown): void {
        const attempt = this.attempts.get(taskId);
        if (!attempt || attempt.cancelled) return;
        attempt.cancelled = true;
        attempt.reason = reason;
        // Deferred: aborting here would run the worker's abort handlers inside
        // the caller's cancel(), re-entrantly. A thread receives it as a message.
        // No reason gives the standard AbortError.
        queueMicrotask(() => attempt.controller.abort(reason));
    }

    /**
     * `terminateWithError()` is a request, never a throw. It used to throw a
     * WorkerTerminationError here, which only worked when called directly in
     * run()'s own stack: from a timer, an interval or a worker-level callback
     * the throw had nowhere to go and crashed the host process. A threaded
     * worker posts a message instead, and this matches it.
     *
     * Called through a task's context (`taskId` given), that task fails with a
     * WorkerTerminationError and the worker's other tasks are requeued. Called
     * through the worker's own context, there is no task to blame, so every
     * in-flight task is requeued.
     */
    private createTerminateWithError(taskId?: string): (error: Error, metadata?: { canRestart?: boolean; restartDelay?: number }) => void {
        return (error: Error, metadata = {}) => {
            // The calling task's outcome is decided now, and whatever its run
            // goes on to return is discarded
            const failing = taskId !== undefined && this.runningTaskIds.has(taskId) ? taskId : undefined;
            if (failing !== undefined) {
                // Its attempt ends here: nothing it emits from now on is delivered
                const stream = this.messageStreams.get(failing);
                if (stream) this.messages.close(stream);
                this.forgetTask(failing, true);
            }

            if (this.terminationRequested) return;
            this.terminationRequested = true;
            // Accept nothing more, before anything is reported: the reports
            // below free slots on this worker, and the scheduler dispatches into
            // a free slot synchronously
            this.state = 'failed';

            // Deferred, so no scheduler or listener code runs inside the
            // worker's own call - as on a thread, where this is a message
            queueMicrotask(() => {
                if (failing !== undefined) {
                    if (this.messageStreams.delete(failing)) this._deliverNow(failing);
                    this.dispatchEvent(new CustomEvent('task.failed', {
                        detail: {
                            taskId: failing, workerId: this.id, workerType: 'inline',
                            error: new WorkerTerminationError(error.message, metadata),
                            timestamp: Date.now(), executionTime: 0
                        }
                    }));
                }
                this._handleTerminationRequest(metadata);
            });
        };
    }

    // Handle termination request after task completion
    private _handleTerminationRequest(metadata: any) {
        if (this.terminationHandled) return;
        this.terminationHandled = true;
        this.terminationRequested = true;

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
            terminateWithError: this.createTerminateWithError(),
            emit: (name, data) => this.messages.emitWorker(name, data)
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

            // setup() is optional on WorkerInterface - the events are not. They
            // report that a worker is being initialized, which is true whether or
            // not it implemented a hook, so a consumer tracking worker state can
            // rely on seeing them. Guarding the events on the method's existence
            // meant a worker without setup() was constructed silently, and made
            // inline pools emit a different set from threaded ones - the
            // worker-side wrapper has always got this right via `setup?.()`.
            // Same defect the teardown events had.
            this.dispatchEvent(new CustomEvent('worker.setup.started', {
                detail: { workerId: this.id, workerType: 'inline', timestamp: Date.now() }
            }));

            const setupStartTime = performance.now();
            try {
                await instance.setup?.();
            } catch (setupError) {
                // Close the pair before the error becomes an initialization
                // failure. Without this, setup.started had no terminal event of
                // its own: the outer initialization pair closed, the inner setup
                // pair did not, and a consumer folding setup state was left
                // holding a worker that never finished setting up.
                this._deliverNow();
                this.dispatchEvent(new CustomEvent('worker.setup.failed', {
                    detail: {
                        workerId: this.id,
                        workerType: 'inline',
                        timestamp: Date.now(),
                        error: setupError
                    }
                }));
                // Rethrow: this is still an initialization failure, and
                // worker.initialization.failed remains the signal the pool acts on.
                throw setupError;
            }

            this._deliverNow();
            this.dispatchEvent(new CustomEvent('worker.setup.completed', {
                detail: {
                    workerId: this.id,
                    workerType: 'inline',
                    timestamp: Date.now(),
                    duration: performance.now() - setupStartTime
                }
            }));

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

            // setup() asked for this worker to be torn down. The pool has
            // already terminated it, but before this instance was stored, so
            // that teardown found nothing - and whatever setup() acquired would
            // leak. Tear it down here instead.
            if (this.terminationRequested) {
                await this._destroyWorkerInstance();
            }
            return instance;

        } catch (error) {
            // The instance was constructed before setup() ran, so if setup threw
            // partway it may hold something that only teardown() releases. Tear it
            // down, and report that as the lifecycle event it is - this used to
            // happen silently, which meant a worker was destroyed with no
            // worker.teardown.* pair at all, and the threaded path did not do it
            // at all.
            if (instance) {
                this.dispatchEvent(new CustomEvent('worker.teardown.started', {
                    detail: { workerId: this.id, workerType: 'inline', timestamp: Date.now() }
                }));
                const cleanupStart = performance.now();
                try {
                    await instance.teardown?.();
                    this.messages.closeWorker();
                    this._deliverNow();
                    this.dispatchEvent(new CustomEvent('worker.teardown.completed', {
                        detail: {
                            workerId: this.id,
                            workerType: 'inline',
                            timestamp: Date.now(),
                            duration: performance.now() - cleanupStart
                        }
                    }));
                } catch (teardownError) {
                    // Reported, never rethrown: the initialization failure below is
                    // the one the pool acts on, and masking it with a cleanup error
                    // would lose the reason the worker could not start.
                    this.messages.closeWorker();
                    this._deliverNow();
                    this.dispatchEvent(new CustomEvent('worker.teardown.failed', {
                        detail: { workerId: this.id, workerType: 'inline', timestamp: Date.now(), error: teardownError }
                    }));
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
        // Created eagerly with the attempt: about 28 ns, against a lazy getter
        // that would need its own record of a cancel arriving before first use
        const attempt: { controller: AbortController; cancelled: boolean; reason?: unknown } = { controller: new AbortController(), cancelled: false };
        this.attempts.set(taskId, attempt);
        // Cancelled before run() was called: nothing ran, so nothing to report
        let notStarted = false;
        
        // Emit task started event when task actually begins execution
        this.dispatchEvent(new CustomEvent('task.started', {
            detail: { taskId, workerId: this.id, workerType: 'inline', timestamp: Date.now() }
        }));
        let fullExecutionStart
        try {
            const workerInstance = await this._createWorkerInstance();
            // Requeued off this worker while it was being created - setup()
            // asking for the worker to be torn down does that. The task now
            // belongs to another worker, so it must not run here as well.
            if (!this.runningTaskIds.has(taskId)) return undefined;
            // Cancelled while the instance was being created: run() is never
            // called, and the rejection settles the task as cancelled
            if (attempt.cancelled) {
                notStarted = true;
                throw new CancelledError(taskId, attempt.reason);
            }
            fullExecutionStart = performance.now();

            // Created on the first emit(), so a task that never emits costs
            // nothing here: no allocation, no flush before it settles
            let stream: MessageStream | undefined;
            const streamFor = (): MessageStream => {
                if (stream) return stream;
                if (!this.runningTaskIds.has(taskId)) return CLOSED_STREAM;
                stream = this.messages.openAttempt(taskId);
                this.messageStreams.set(taskId, stream);
                return stream;
            };

            // Create TaskWorkerContext with both worker and task capabilities
            const context: TaskWorkerContext = {
                // Worker-level context (inherited from BaseWorkerContext), except
                // that terminating through it fails this task in particular
                workerId: this.id,
                terminateWithError: this.createTerminateWithError(taskId),
                // Task-level context
                taskId,
                sendProgress: (progress: number, message?: string, details?: any) => {
                    if (!this.runningTaskIds.has(taskId)) return;
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
                    if (!this.runningTaskIds.has(taskId)) return;
                    this.dispatchEvent(new CustomEvent('task.spawn_request', {
                        detail: {
                            taskId,
                            workerId: this.id,
                            workerType: 'inline',
                            spawnConfig: config,
                            timestamp: Date.now()
                        }
                    }));
                },
                emit: (name, data) => this.messages.emitTask(streamFor(), name, data),
                signal: attempt.controller.signal
            };

            // The attempt's emits close when run() ends, either way, so a stray
            // promise cannot add to a settled attempt
            let result;
            try {
                result = await workerInstance.run(payload, context);
            } finally {
                if (stream) this.messages.close(stream);
                else stream = CLOSED_STREAM;
            }

            // Forgotten while running: the task was requeued off this worker, and
            // reporting this result would settle the attempt that replaced it
            if (!this.runningTaskIds.has(taskId)) return result;

            // Calculate full execution time including all coordination overhead
            const fullExecutionTime = performance.now() - fullExecutionStart;

            // Everything it emitted goes first, then the stream is retired
            if (this.messageStreams.delete(taskId)) this._deliverNow(taskId);

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
            // Forgotten while running: as for a result above, nothing to report.
            // Nor for an attempt cancelled before it started: it did not fail.
            if (notStarted || !this.runningTaskIds.has(taskId)) throw error;

            // As for a result: its emitted events precede its task.failed
            if (this.messageStreams.delete(taskId)) this._deliverNow(taskId);

            const fullExecutionTime = 0//performance.now() - fullExecutionStart;
            // Calculate execution time even for failed tasks

            // Check if this is a worker termination error
            if (isWorkerTerminationError(error)) {
                // Re-created from this module's class when the worker's copy
                // differs, so a caller's documented `instanceof
                // WorkerTerminationError` check holds for the error it receives
                // The type guard cannot express "same name, other class", so the
                // foreign copy is typed by hand
                const foreign = error as Error & { metadata?: WorkerTerminationError['metadata'] };
                const termination = error instanceof WorkerTerminationError
                    ? error
                    : new WorkerTerminationError(foreign.message, foreign.metadata ?? {});

                // First emit task failed event
                this.dispatchEvent(new CustomEvent('task.failed', {
                    detail: { taskId, workerId: this.id, workerType: 'inline', error: termination, timestamp: Date.now(), executionTime: fullExecutionTime }
                }));

                // This task has failed, so it is no longer one of this worker's
                // running tasks: without this the pool also reported it as
                // requeued, which it never is
                this.forgetTask(taskId);

                // Then handle worker termination request
                this._handleTerminationRequest(termination.metadata);
                throw termination;
            } else {
                // Regular task error - just emit task failed event
                this.dispatchEvent(new CustomEvent('task.failed', {
                    detail: { taskId, workerId: this.id, workerType: 'inline', error, timestamp: Date.now(), executionTime: fullExecutionTime }
                }));
                throw error;
            }

        } finally {
            // A forgotten task was already untracked by forgetTask, and
            // terminate() clears the tracking wholesale, so decrementing
            // unconditionally drove runningTasks below zero
            if (this.runningTaskIds.delete(taskId)) this.runningTasks--;
            this.runningTaskData.delete(taskId); // Also clean up task data
            if (this.attempts.get(taskId) === attempt) this.attempts.delete(taskId);

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

            // Nothing after teardown; what it emitted goes before its outcome
            this.messages.closeWorker();
            this._deliverNow();
            this.dispatchEvent(new CustomEvent('worker.teardown.completed', {
                detail: {
                    workerId: this.id,
                    workerType: 'inline',
                    timestamp: Date.now(),
                    duration: performance.now() - teardownStartTime
                }
            }));
        } catch (teardownError) {
            this.messages.closeWorker();
            this._deliverNow();
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