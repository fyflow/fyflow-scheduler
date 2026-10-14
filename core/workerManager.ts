import { ThreadWrapper } from "./threadWrapper.ts";
import { InlineWrapper } from "./inlineWrapper.ts";
import type { TaskMessageDetail, WorkerStatus } from "./workerInterface.ts";
import { AttemptStoppedError, CancelledError } from "./workerInterface.ts";
import { resolveMessageOptions } from "./messageChannel.ts";
import type { ChannelEvent, MessageOptions } from "./messageChannel.ts";

type WorkerInstance = ThreadWrapper | InlineWrapper;

/**
 * The longest delay `setTimeout` honours. Above it, `Infinity` included, it
 * fires after about 1 ms - so "never force" would mean "force at once". Grace
 * periods longer than this arm no timer at all.
 */
const MAX_TIMER_MS = 2 ** 31 - 1;

/** A cancelled attempt's grace period. No `timer` when it is too long to arm one. */
interface GraceEntry {
  token?: number;
  deadline: number;
  timer?: ReturnType<typeof setTimeout>;
  fired: boolean;
}

/** The rule for `cancelGraceMs` and for `cancel()`'s `graceMs`. Returns it unchanged. */
export function validateGraceMs(value: unknown, name: string): number {
  if (typeof value !== 'number') throw new TypeError(`${name} must be a number, got ${typeof value}`);
  if (Number.isNaN(value) || value < 0) throw new RangeError(`${name} must be zero or more, got ${value}`);
  return value;
}

/**
 * A resident holding taken or returned by a pool.
 *
 * The pool reports it; the **scheduler** turns it into a `resource.*` event and
 * is the sole emitter. See `core/resourceEvents.ts` for why a pool-level mirror
 * would be a double count rather than a convenience.
 */
export type ResidentResourceNotice =
  | { type: 'acquired'; workerId: string; groupId: string; cost: number }
  | {
      type: 'released';
      workerId: string;
      groupId: string;
      cost: number;
      reason: 'worker-teardown' | 'shutdown';
    };

/** Injected by the scheduler at {@link WorkerManager._bindResidentGroups}. */
export type ResidentResourceNotifier = (notice: ResidentResourceNotice) => void;

/**
 * A task's emitted event, handed to the scheduler rather than dispatched here:
 * `task.message` is scheduler-only, so a consumer listening to both cannot
 * receive it twice. `attemptNumber` is added by the scheduler.
 */
export type TaskMessageNotifier = (detail: Omit<TaskMessageDetail, 'attemptNumber'>) => void;

export interface WorkerManagerOptions {
  /**
   * Maximum worker instances in this pool. Default 2.
   *
   * Despite the name this is a count of worker *instances*, not OS threads.
   * With `inline: false` each instance is a real worker thread. With
   * `inline: true` they all live in the main process, and the name is a
   * misnomer - see {@link WorkerManagerOptions.inline}.
   */
  maxThreads?: number;
  /**
   * Tasks each worker instance may run at once. Default 1. Total pool capacity
   * is `maxThreads x maxConcurrentTasks` for both pool types. Raise this for
   * async/IO workers; leave it at 1 for CPU-bound ones.
   */
  maxConcurrentTasks?: number;
  /** Ms an idle worker is kept before termination. Default 5000. `0` never terminates. */
  idleTimeout?: number;
  /**
   * How often idle workers are swept for termination, in ms. Default 5000.
   *
   * This is a floor on how promptly a worker actually goes away, independent of
   * `idleTimeout` - an `idleTimeout` of 50ms still means up to a 5s wait on the
   * default sweep. That matters for {@link WorkerManagerOptions.residentGroups},
   * where the sweep is what hands a held resource to a pool waiting for it.
   */
  idleCheckIntervalMs?: number;
  /**
   * Run workers in the main process instead of worker threads. Default false.
   * Inline suits async/IO work; threaded suits CPU-bound work, since an inline
   * worker shares the event loop and a synchronous task blocks the scheduler.
   *
   * For an inline pool, `maxThreads` creates that many instances of your worker
   * class in the same process - no threads are involved. Concurrency comes from
   * overlapping `await`s, so only the PRODUCT matters for throughput:
   * `maxThreads: 4, maxConcurrentTasks: 5` and
   * `maxThreads: 1, maxConcurrentTasks: 20` both cap at 20 in-flight tasks and
   * measure the same.
   *
   * What the split does change is state isolation: each instance is constructed
   * separately, so 4 instances means 4 connection pools, caches or whatever
   * per-instance state your worker holds, while 1 instance funnels all 20
   * concurrent tasks through one object.
   */
  inline?: boolean;
  /** Passed as the first constructor argument to every worker instance in this pool. */
  config?: any;
  /** Resource group ids every task in this pool must acquire before running. */
  groups?: string[];
  /**
   * Groups held for a **worker's whole lifetime**, from creation until teardown,
   * rather than for the duration of a task.
   *
   * For resources a worker holds because it exists - a model loaded on a GPU in
   * `setup()`, a database connection, a licence seat. A task-scoped group cannot
   * express these: it is released when the task settles, while the worker lives
   * on until its idle timeout, so a second worker can take the resource while
   * the first still holds it.
   *
   * ```typescript
   * residentGroups: ['vram']       // one unit per worker
   * residentGroups: { vram: 20 }   // this pool's model needs 20 units per worker
   * ```
   *
   * Cost is per worker, not per pool: `maxThreads: 4` with `{ vram: 2 }` uses 8
   * units at full spread. A worker is not created while its cost does not fit,
   * and tasks needing one wait until a holder is torn down.
   */
  residentGroups?: string[] | Record<string, number>;
  /** Requeue a worker's in-flight tasks when it fails, rather than failing them. Default true. */
  requeueFailedTasks?: boolean;
  /** Restarts allowed before the pool gives up and emits `worker.restart_limit_exceeded`. Default 3. */
  maxWorkerRestarts?: number;
  /**
   * Ms a threaded worker gets to stop a cancelled task by itself, through
   * `context.signal`, before the pool forces it: the worker is terminated, and
   * any other task it was running is requeued. Default 2000. `0` forces at
   * once; `Infinity` never forces. `scheduler.cancel()`'s `graceMs` overrides
   * it per call. Inline pools accept it and ignore it - an inline `run()`
   * cannot be stopped.
   *
   * A forced stop does not count against `maxWorkerRestarts`. It still costs
   * the worker: a pool that loads something expensive in `setup()` should
   * make its workers honour the signal, or raise this.
   *
   * @throws TypeError at construction for a non-number, RangeError for a
   *   negative value or NaN.
   */
  cancelGraceMs?: number;
  /**
   * Bounds on what each worker can send with `emit()`. Default
   * `{ flushIntervalMs: 20, maxBuffered: 100 }`: events are batched for up to
   * 20 ms, and each buffer - one per running attempt, one for the worker -
   * holds 100 between flushes, so at most 5,000 events/s each. Emits over the
   * cap are discarded and counted in the next event's `dropped`.
   *
   * Measured at the defaults: 16 threaded workers emitting in tight loops
   * delivered ~70,000 events/s with the main thread stalling at most 16-47 ms
   * (16 ms idle) - about its limit. More emitting workers, or a slower
   * listener, will saturate it. Raise `maxBuffered` for a worker that
   * emits large bursts; each delivered batch is one uninterrupted stretch of
   * main-thread work, so keep it in the hundreds.
   *
   * @throws RangeError at construction for a non-positive interval or a cap
   *   that is not a positive integer.
   */
  messages?: MessageOptions;
}

/**
 * A pool of workers of one type, created from a single worker script.
 *
 * ```typescript
 * const pool = new WorkerManager(workerUrl, {
 *   maxThreads: 4,
 *   maxConcurrentTasks: 1,
 *   groups: ['cpu']
 * });
 * ```
 *
 * Emits: `task.started`, `task.completed`, `task.failed`, `task.progress`,
 * `task.spawn_request`, `task.requeue_required`, `worker.failed`,
 * `worker.self_terminated`, `worker.restart_limit_exceeded`, and the worker
 * lifecycle events `worker.initialization.started|completed|failed`,
 * `worker.setup.started|completed` and
 * `worker.teardown.started|completed|failed`.
 *
 * Lifecycle events carry `{ workerId, workerType, timestamp }` plus `duration`
 * on `*.completed` and `error` on `*.failed`. They fire on every worker
 * creation and idle-timeout teardown, so keep their listeners cheap.
 *
 * Workers are created lazily on first use, up to `maxThreads`.
 */
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
  /** See {@link WorkerManagerOptions.cancelGraceMs}. */
  readonly cancelGraceMs: number;
  /** Resolved `messages` option, passed to every worker this pool creates. */
  readonly messages: Required<MessageOptions>;

  // Message gate. task.message is the scheduler's to open, worker.message this
  // pool's: each flag is on while that event has a listener. Held here and
  // applied to every wrapper, existing ones on change and new ones at creation.
  private taskMessageGate = false;
  private workerMessageListeners = new Set<unknown>();
  private taskMessageNotify?: TaskMessageNotifier;

  // Track restart count for entire pool (simpler and more logical)
  private poolRestartCount = 0;
  // Set for the duration of shutdown, so worker failures during teardown are not
  // mistaken for crashes worth restarting
  private shuttingDown = false;
  // Workers whose failure has been handled. One failure can be reported more
  // than once - a self-terminating worker emits both worker.failed and
  // worker.termination_requested - and handling each spent two restarts and
  // terminated the worker twice. Keyed by the wrapper, which a failed worker
  // never outlives: it is replaced, not revived.
  private failureHandled = new WeakSet<WorkerInstance>();

  // Track ALL event listeners for cleanup
  private allListeners = new Map<any, {event: string; listener: Function}[]>();

  // Track initializing threads
  private initializingThreads = new Set<WorkerInstance>();

  /**
   * Tasks between dispatch and their wrapper: `enqueueNoQueue` awaiting a new
   * thread, the only path with an await before `runTask`. A cancel arriving
   * here is recorded, and the task never reaches the wrapper.
   */
  private enqueuing = new Map<string, { cancelled: boolean; reason?: unknown }>();

  /**
   * Threaded pools: one entry per task being cancelled, from the cancel until
   * the scheduler settles it. Doubles as the record of which tasks on a worker
   * are being cancelled, which a forced stop must not requeue. `token` is the
   * attempt's: a timer belongs to an attempt, not to a task id, which a later
   * task may reuse.
   */
  private graceTimers = new Map<string, GraceEntry>();

  /**
   * Workers being retired by a forced stop, until they are dead and gone.
   * They accept nothing, are skipped by failure handling, the idle sweep and
   * `shutdown()`, which waits for these instead of terminating them again.
   */
  private retiring = new Map<WorkerInstance, Promise<void>>();

  /** Group id -> units one worker of this pool holds while it exists. */
  residentGroups: Record<string, number>;
  /**
   * The group objects behind {@link residentGroups}, injected by the scheduler -
   * pools are constructed with ids, and only the scheduler owns the registry.
   * Empty until then, which makes resident groups inert for a standalone pool.
   */
  private residentRegistry: Record<string, any> = {};
  /**
   * Ids of workers whose resident cost this pool holds. Keyed by worker rather
   * than counted, so releasing is idempotent: a failed worker is released by
   * `_releaseWorkerGroupResources` while it is still in `threads`, and must not
   * be released a second time if the idle sweep also reaches it.
   */
  private residentHolders = new Set<string>();
  /** Acquired, but the worker object does not exist yet. Same synchronous turn. */
  private residentPending = 0;
  /**
   * Injected alongside the registry. Undefined for a standalone pool, and for
   * one the scheduler bound before this existed.
   */
  private residentNotify?: ResidentResourceNotifier;

  private static _normalizeResidentGroups(
    declared: string[] | Record<string, number> | undefined
  ): Record<string, number> {
    if (!declared) return {};
    const costs: Record<string, number> = Array.isArray(declared)
      ? Object.fromEntries(declared.map(id => [id, 1]))
      : { ...declared };
    for (const [id, cost] of Object.entries(costs)) {
      if (!Number.isInteger(cost) || cost <= 0) {
        throw new Error(
          `residentGroups['${id}'] must be a positive integer, got ${cost}`
        );
      }
    }
    return costs;
  }

  /**
   * Called by the scheduler once, with the group registry. Also the point where
   * a cost larger than its group's limit is rejected: such a pool could never
   * start a worker, and failing here beats blocking forever at runtime.
   */
  _bindResidentGroups(registry: Record<string, any>, notify?: ResidentResourceNotifier) {
    for (const [id, cost] of Object.entries(this.residentGroups)) {
      const group = registry[id];
      if (!group) throw new Error(`Unknown resident group: ${id}`);
      const limit = group.getMetrics?.().limit;
      if (typeof limit === 'number' && cost > limit) {
        throw new Error(
          `residentGroups['${id}'] cost ${cost} exceeds the group limit of ${limit} - ` +
          `a worker in this pool could never start`
        );
      }
    }
    this.residentRegistry = registry;
    this.residentNotify = notify;
  }

  /** True when every resident group has room for one more worker of this pool. */
  canHoldAnotherWorker(): boolean {
    return Object.entries(this.residentGroups).every(([id, cost]) => {
      const group = this.residentRegistry[id];
      return !group || group.canRun(undefined, cost);
    });
  }

  /**
   * Take one worker's resident cost from every group, or nothing at all.
   * Synchronous, and called in the same turn as the capacity check, so no other
   * pool can interleave between the two.
   */
  private _acquireResident(): boolean {
    if (!this.canHoldAnotherWorker()) return false;
    for (const [id, cost] of Object.entries(this.residentGroups)) {
      this.residentRegistry[id]?.onStart?.(undefined, cost);
    }
    this.residentPending++;
    return true;
  }

  /**
   * Attach a just-acquired holding to the worker it was taken for.
   *
   * Also where the acquire is announced, rather than in
   * {@link _acquireResident}: the worker id does not exist yet at acquire time,
   * and both call sites bind in the same synchronous turn. A holding that is
   * acquired and never bound is a bug, not a state worth modelling.
   */
  private _bindResidentHolder(workerId: string) {
    if (this.residentPending === 0) return;
    this.residentPending--;
    this.residentHolders.add(workerId);

    if (!this.residentNotify) return;
    for (const [id, cost] of Object.entries(this.residentGroups)) {
      // Only groups the registry actually knows were charged by _acquireResident
      if (this.residentRegistry[id]) {
        this.residentNotify({ type: 'acquired', workerId, groupId: id, cost });
      }
    }
  }

  /** Give back one worker's resident cost. No-op if it holds none. */
  private _releaseResidentFor(
    workerId: string,
    reason: 'worker-teardown' | 'shutdown' = 'worker-teardown'
  ) {
    if (!this.residentHolders.delete(workerId)) return;
    for (const [id, cost] of Object.entries(this.residentGroups)) {
      const group = this.residentRegistry[id];
      if (!group) continue;
      group.onFinish?.(undefined, cost);
      this.residentNotify?.({ type: 'released', workerId, groupId: id, cost, reason });
    }
  }

  /** Give everything back, holders and any unbound acquisition. */
  private _releaseAllResident() {
    for (const workerId of [...this.residentHolders]) {
      this._releaseResidentFor(workerId, 'shutdown');
    }
    // An unbound acquisition never announced itself - emitting a release for it
    // would leave the fold one release ahead of its acquires forever
    while (this.residentPending > 0) {
      this.residentPending--;
      for (const [id, cost] of Object.entries(this.residentGroups)) {
        this.residentRegistry[id]?.onFinish?.(undefined, cost);
      }
    }
  }

  /** Units this pool currently holds in each resident group. Test/diagnostic. */
  getResidentUsage(): Record<string, number> {
    const held = this.residentHolders.size + this.residentPending;
    const usage: Record<string, number> = {};
    for (const [id, cost] of Object.entries(this.residentGroups)) {
      usage[id] = cost * held;
    }
    return usage;
  }

  // Centralized idle management
  private idleCheckTimer: ReturnType<typeof setInterval> | null = null;
  private readonly idleCheckIntervalMs: number;

  constructor(scriptUrl: string, options: WorkerManagerOptions = {}) {
    super();
    this.scriptUrl = scriptUrl;
    this.maxThreads = options.maxThreads ?? 2;
    this.maxConcurrentTasks = options.maxConcurrentTasks ?? 1;
    this.idleTimeout = options.idleTimeout ?? 5000;
    this.idleCheckIntervalMs = options.idleCheckIntervalMs ?? 5000;
    this.inline = options.inline ?? false;
    this.config = options.config ?? {};
    this.groups = options.groups ?? [];
    this.residentGroups = WorkerManager._normalizeResidentGroups(options.residentGroups);
    this.requeueFailedTasks = options.requeueFailedTasks ?? true; // Default to requeue as specified
    this.maxWorkerRestarts = options.maxWorkerRestarts ?? 3; // Default: 3 restarts per worker
    this.cancelGraceMs = validateGraceMs(options.cancelGraceMs ?? 2000, 'cancelGraceMs');
    this.messages = resolveMessageOptions(options.messages);

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
    if (!targetThread && (this.threads.length + this.initializingThreads.size) < this.maxThreads
        && this._acquireResident()) {
      const newThread = this._newWorker(this.config);

      this._bindResidentHolder(newThread.id);

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
        const entry: { cancelled: boolean; reason?: unknown } = { cancelled: false };
        this.enqueuing.set(taskId, entry);
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
        if (this.enqueuing.get(taskId) === entry) this.enqueuing.delete(taskId);
        // Cancelled meanwhile: it never reaches the thread, which stays, idle,
        // for the next task. The rejection settles it as cancelled.
        if (entry.cancelled) throw new CancelledError(taskId, entry.reason);
        targetThread = newThread;
      }
    }

    if (!targetThread) {
      // Also the path taken when a resident group had no room for another
      // worker. The scheduler blocks such tasks before dispatch, so reaching
      // here means capacity went away in between - requeue rather than fail.
      throw new Error(`No worker thread available (${this.threads.length}/${this.maxThreads})`);
    }

    // Execute task directly on thread
    return targetThread.runTask(taskId, payload);
  }

  /**
   * Called by the scheduler for a task it has dispatched to this pool and
   * accepted a cancel for. Finds where the attempt is - on its way to a worker,
   * or on one - and asks it to stop. Settles nothing: the attempt's promise
   * does, once it has stopped.
   *
   * On a threaded pool it also starts the grace period: `graceMs`, or the
   * pool's `cancelGraceMs`, after which a worker still running the attempt is
   * forced. `token` identifies the attempt; the scheduler clears the period
   * with {@link _clearCancelGrace} when the task settles.
   */
  _cancelTask(taskId: string, token?: number, options: { reason?: unknown; graceMs?: number } = {}): void {
    // Not during shutdown, which terminates every worker anyway
    if (!this.inline && !this.shuttingDown) {
      const graceMs = options.graceMs ?? this.cancelGraceMs;
      const entry: GraceEntry = { token, deadline: Date.now() + graceMs, fired: false };
      this.graceTimers.set(taskId, entry);
      this._armGraceTimer(taskId, entry, graceMs);
    }

    const enqueuing = this.enqueuing.get(taskId);
    if (enqueuing) {
      enqueuing.cancelled = true;
      enqueuing.reason = options.reason;
      return;
    }
    // A scan of at most maxThreads, at cancel time only, rather than a
    // task -> worker map that every dispatch would pay for. A worker that
    // failed under the task has forgotten it, so only the live attempt matches.
    for (const worker of this.threads) {
      if (worker.hasTask(taskId)) {
        worker.cancelTask(taskId, options.reason);
        return;
      }
    }
  }

  /**
   * A repeat `cancel()` with a `graceMs`: shortens the wait if that ends
   * sooner than what is left, and never lengthens it.
   */
  _shortenCancelGrace(taskId: string, token: number | undefined, graceMs: number): void {
    const entry = this.graceTimers.get(taskId);
    if (!entry || entry.token !== token || entry.fired) return;
    const deadline = Date.now() + graceMs;
    if (deadline >= entry.deadline) return;
    if (entry.timer !== undefined) clearTimeout(entry.timer);
    entry.timer = undefined;
    entry.deadline = deadline;
    this._armGraceTimer(taskId, entry, graceMs);
  }

  /** Called by the scheduler when a task it cancelled has settled. */
  _clearCancelGrace(taskId: string, token?: number): void {
    const entry = this.graceTimers.get(taskId);
    if (!entry || entry.token !== token) return;
    if (entry.timer !== undefined) clearTimeout(entry.timer);
    this.graceTimers.delete(taskId);
  }

  private _armGraceTimer(taskId: string, entry: GraceEntry, graceMs: number): void {
    // Infinity, or so long that setTimeout would fire it at once instead
    if (graceMs > MAX_TIMER_MS) return;
    entry.timer = setTimeout(() => {
      entry.timer = undefined;
      entry.fired = true;
      // Cleared, or replaced by a later attempt's cancel
      if (this.graceTimers.get(taskId) !== entry) return;
      // Only an attempt that is still on a worker: one still on its way to a
      // new thread never reaches it, and one in a thread's start-up queue was
      // rejected at the cancel
      const worker = this.threads.find(w => w.hasTask(taskId));
      if (worker instanceof ThreadWrapper) this._retireWorker(worker);
    }, graceMs);
  }

  /**
   * The forced stop: retire a worker that is still running a cancelled task
   * after its grace period. Not a failure - no `worker.failed`, no restart
   * counted, no replacement; the next dispatch creates a worker on demand.
   *
   * Every attempt on it settles only by its promise, and the cancelled ones
   * only once the thread is dead: their resources must not be released while
   * a stuck thread still runs with them.
   */
  private _retireWorker(worker: ThreadWrapper): Promise<void> {
    const inProgress = this.retiring.get(worker);
    if (inProgress) return inProgress;
    // Gone already, or failing: its own teardown settles what it held
    if (worker.state === 'failed' || worker.state === 'terminated' || !this.threads.includes(worker)) {
      return Promise.resolve();
    }

    // 1. It accepts nothing more, and the exit about to be caused is not
    // mistaken for a crash
    worker.state = 'terminated';

    // 2. Detach every attempt now, before anything else can arrive: a result
    // posted during the teardown handshake would otherwise settle the cancelled
    // task early, with its thread still alive. terminate() clears the
    // callbacks itself, so afterwards there would be nothing left to detach.
    const cancelled: ((reason?: any) => void)[] = [];
    const requeued: [string, (reason?: any) => void][] = [];
    for (const taskId of worker.getRunningTaskIds()) {
      const reject = worker.forgetTask(taskId);
      if (!reject) continue;
      if (this.graceTimers.has(taskId)) cancelled.push(() => reject(new AttemptStoppedError(taskId, 'forced')));
      else requeued.push([taskId, reject]);
    }

    // 3. Its co-tenants go back in the queue at once. They hold nothing the
    // thread can still use: their run is abandoned, and a late result from it
    // finds no callback.
    for (const [taskId, reject] of requeued) reject(new AttemptStoppedError(taskId, 'requeue'));

    const retired = (async () => {
      // 4. Teardown, up to its acknowledgement timeout, then the thread is
      // killed. A worker stuck in a synchronous loop never acknowledges.
      try {
        await worker.terminate();
      } catch {
        // Terminated either way
      }

      // 6, before 5: settling the cancelled tasks runs the scheduler's dispatch
      // loop, which can use this worker's slot and resident units only once
      // they are free
      const index = this.threads.indexOf(worker);
      if (index !== -1) this.threads.splice(index, 1);
      this._releaseResidentFor(worker.id);
      if (this.threads.length === 0) this.stopIdleTimer();
      this.retiring.delete(worker);

      // 5. Only now, with the thread dead, the cancelled tasks settle
      for (const settle of cancelled) settle();
    })();
    this.retiring.set(worker, retired);
    return retired;
  }

  /** Every worker is created here, so each one gets the message options and the current gate. */
  private _newWorker(config: any): WorkerInstance {
    const worker = this.inline
      ? new InlineWrapper(this.scriptUrl, this.idleTimeout, this.maxConcurrentTasks, config, this.messages)
      : new ThreadWrapper(this.scriptUrl, this.idleTimeout, this.maxConcurrentTasks, config, this.messages);
    worker.setMessageGate(this.taskMessageGate, this.workerMessageListeners.size > 0);
    worker.messageSink = (events, workerId, workerType) => this._onMessages(events, workerId, workerType);
    return worker;
  }

  /**
   * A delivered batch. A task's events go to the scheduler and never fire here,
   * so a consumer listening to both cannot count them twice; the worker's own
   * fire as `worker.message`. Each detail is built once, for the consumer.
   */
  private _onMessages(events: ChannelEvent[], workerId: string, workerType: 'inline' | 'thread') {
    for (const { taskId, name, data, seq, timestamp, dropped, error } of events) {
      if (taskId !== undefined) {
        // attemptNumber is the scheduler's to fill in; present from the start
        // so the object keeps one shape and its documented field order
        const detail: any = { taskId, workerId, attemptNumber: 0, name, data, seq, timestamp };
        if (dropped !== undefined) detail.dropped = dropped;
        if (error !== undefined) detail.error = error;
        this.taskMessageNotify?.(detail);
      } else {
        const detail: any = { workerId, workerType, name, data, seq, timestamp };
        if (dropped !== undefined) detail.dropped = dropped;
        if (error !== undefined) detail.error = error;
        this.dispatchEvent(new CustomEvent('worker.message', { detail }));
      }
    }
  }

  private _applyMessageGate() {
    const worker = this.workerMessageListeners.size > 0;
    for (const thread of this.threads) thread.setMessageGate(this.taskMessageGate, worker);
  }

  /**
   * Called by the scheduler: where this pool's `task.message` events go. The
   * pool never dispatches them itself.
   */
  _bindTaskMessages(notify: TaskMessageNotifier) {
    this.taskMessageNotify = notify;
  }

  /** Called by the scheduler when its `task.message` listeners go from none to some, or back. */
  _setTaskMessageGate(open: boolean) {
    if (this.taskMessageGate === open) return;
    this.taskMessageGate = open;
    this._applyMessageGate();
  }

  private _createPredictiveThread(): WorkerInstance | null {
    // A replacement worker still has to fit in the resident groups.
    if (!this._acquireResident()) return null;

    const thread = this._newWorker(this.config);

    this._bindResidentHolder(thread.id);

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
    // A Set, as the scheduler keeps for resource.*: removing a listener that was
    // never added must not close the gate on one that is still there
    if (event === 'worker.message' && !this.workerMessageListeners.has(listener)) {
      this.workerMessageListeners.add(listener);
      if (this.workerMessageListeners.size === 1) this._applyMessageGate();
    }
  }

  override removeEventListener(event: string, listener: EventListenerOrEventListenerObject | null, options?: boolean | EventListenerOptions): void {
    super.removeEventListener(event, listener, options);
    if (event === 'worker.message' && this.workerMessageListeners.delete(listener)) {
      if (this.workerMessageListeners.size === 0) this._applyMessageGate();
    }
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
        } catch {
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
      // Lifecycle events - forwarded so pool owners can observe worker startup
      // cost, setup failures and teardown problems. Both wrapper types emit the
      // same set with the same payload shape.
      { event: 'worker.initialization.started', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.initialization.started', { detail: e.detail })) },
      { event: 'worker.initialization.completed', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.initialization.completed', { detail: e.detail })) },
      { event: 'worker.initialization.failed', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.initialization.failed', { detail: e.detail })) },
      { event: 'worker.setup.started', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.setup.started', { detail: e.detail })) },
      { event: 'worker.setup.completed', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.setup.completed', { detail: e.detail })) },
      { event: 'worker.setup.failed', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.setup.failed', { detail: e.detail })) },
      { event: 'worker.teardown.started', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.teardown.started', { detail: e.detail })) },
      { event: 'worker.teardown.completed', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.teardown.completed', { detail: e.detail })) },
      { event: 'worker.teardown.failed', handler: (e: any) => this.dispatchEvent(new CustomEvent('worker.teardown.failed', { detail: e.detail })) },
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
    // A worker "failing" while we are tearing the pool down is expected
    if (this.shuttingDown) return;

    const { workerId, error, metadata } = e.detail;
    const { canRestart, restartDelay } = metadata || {};
    const failedWorker = this.threads.find(w => w.id === workerId);

    if (!failedWorker) return;
    // Being retired by a forced stop, which settles everything it held. A
    // termination request it sends meanwhile is not a failure to restart from.
    if (this.retiring.has(failedWorker)) return;
    if (this.failureHandled.has(failedWorker)) return;
    this.failureHandled.add(failedWorker);

    // Before any requeue is reported: the scheduler settles a requeue
    // synchronously and runs its dispatch loop, which would otherwise hand the
    // task straight back to this worker - its slot was just freed by forgetTask
    // below. _markWorkerAsFailed sets this too, but only after the requeues.
    (failedWorker as any).state = 'failed';

    // CRITICAL: Requeue any in-progress tasks (Worker failure ≠ Task failure)
    const runningTaskIds = failedWorker.getRunningTaskIds();
    // Promises that only this worker's thread could settle, rejected once it is dead
    const released: ((reason?: any) => void)[] = [];
    runningTaskIds.forEach(taskId => {
      // Before reporting anything: the run on this worker may still be going,
      // and once the task is requeued and dispatched again, its late outcome
      // would otherwise settle the new attempt
      const reject = failedWorker.forgetTask(taskId);
      if (reject) released.push(reject);

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
    const stopped = this._markWorkerAsFailed(workerId, error, metadata);

    // The scheduler has already settled every task reported above, so for them
    // this rejection is stale and ignored. It matters for a task being
    // cancelled, which the scheduler deliberately did not settle: it settles,
    // as cancelled, only now that its run has really stopped.
    if (released.length > 0) {
      const failure = new Error(`Worker failed: ${error.message}`);
      stopped.then(() => { for (const reject of released) reject(failure); });
    }

    // CRITICAL: Release group resources immediately (can't process tasks)
    this._releaseWorkerGroupResources(workerId);

    // Replace unless the failure explicitly forbade it.
    //
    // This used to require `canRestart === true`, while the requeue decision
    // above uses `canRestart !== false`. Failures that set no canRestart at all -
    // which is every construction failure - fell through both branches: no
    // replacement was scheduled AND worker.restart_limit_exceeded was never
    // emitted, so an unstartable pool kept a dead worker in its maxThreads slot
    // and stayed silent to anyone monitoring that event.
    if (canRestart !== false) {
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
  /** Resolves once the worker has been terminated, or at once if there was nothing to terminate. */
  private _markWorkerAsFailed(workerId: string, error: Error, metadata: any): Promise<void> {
    const worker = this.threads.find(w => w.id === workerId);
    let stopped: Promise<void> = Promise.resolve();
    if (worker) {
      // Check if worker is already terminated to avoid double termination
      const alreadyTerminated = (worker as any).state === 'terminated';

      // Mark as failed but keep in threads array (maintains maxThreads slot)
      (worker as any).state = 'failed';
      (worker as any).lastError = { timestamp: Date.now(), message: error.message, metadata };

      // Terminate the actual worker process/thread but keep tracking (if not already terminated)
      if (!alreadyTerminated) {
        stopped = worker.terminate().catch(() => {
          // Worker may already be terminated - ignore errors
        });
      }

      // Worker can no longer accept tasks but stays in pool count
      // This is handled by the updated canAcceptTask() method in wrappers
    }
    return stopped;
  }

  private _releaseWorkerGroupResources(workerId: string) {
    // A dead worker holds nothing. Its resident units must come back or the
    // group leaks capacity permanently and every pool sharing it stalls.
    this._releaseResidentFor(workerId);
  }

  private _removeAndReplaceWorker(workerId: string) {
    const workerIndex = this.threads.findIndex(w => w.id === workerId);
    if (workerIndex !== -1) {
      // NOW we actually remove from pool and allow replacement
      this.threads.splice(workerIndex, 1);

      if (this.threads.length < this.maxThreads) {
        this._createPredictiveThread();
        // Pool restart count persists across worker replacements
      }
    }
  }

  // Worker Status Inspection APIs (as specified in task)
  /** Ids of the workers that currently exist in this pool (created lazily). */
  getWorkerIds(): string[] {
    return this.threads.map(w => w.id);
  }

  /**
   * Health and activity of one worker, or `null` if no such worker exists.
   * Useful for building supervision on top of `worker.failed`.
   */
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

  /** `getWorkerStatus` for every live worker, keyed by worker id. */
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
  /**
   * Terminate a worker and create a replacement, optionally merging `newConfig`
   * over the pool's `config`. The replacement gets a new id.
   *
   * @returns false if no worker with that id exists.
   */
  async restartWorker(workerId: string, newConfig?: any): Promise<boolean> {
    const workerIndex = this.threads.findIndex(w => w.id === workerId);
    if (workerIndex === -1) return false;

    const oldWorker = this.threads[workerIndex];
    await oldWorker.terminate();
    this.threads.splice(workerIndex, 1);

    // Create new worker with updated config
    const config = { ...this.config, ...(newConfig || {}) };
    const newWorker = this._newWorker(config);

    this._setupWorkerEventListeners(newWorker);
    this.threads.push(newWorker);
    this.startIdleTimer(); // Ensure idle timer is running

    return true;
  }

  /** Alias for {@link WorkerManager.restartWorker}. */
  async replaceWorker(workerId: string, newConfig?: any): Promise<boolean> {
    // Same as restart for now
    return await this.restartWorker(workerId, newConfig);
  }

  /**
   * Merge `config` into a live worker's config object.
   *
   * Note this mutates the config a worker instance already holds - a worker that
   * copied values in its constructor will not see the change. Use
   * {@link WorkerManager.restartWorker} when the worker must be rebuilt.
   *
   * @returns false if no worker with that id exists.
   */
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
    }, this.idleCheckIntervalMs);
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
      // Skip persistent workers (idleTimeout: 0), and one a forced stop is
      // already terminating - it is idle because its tasks were detached
      if (this.retiring.has(worker)) continue;
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
    // The model this worker loaded is gone with it - hand back its units so a
    // pool waiting on the group can start.
    this._releaseResidentFor(worker.id);

    // Terminate the worker
    try {
      await worker.terminate();
    } catch (error) {
      console.warn(`Failed to terminate idle worker ${worker.id}:`, error);
    }
  }

  // Shutdown all workers and cleanup resources
  /** Terminate every worker in the pool and drop its listeners. */
  async shutdown(): Promise<void> {
    this.shuttingDown = true;

    // Stop the centralized idle timer
    this.stopIdleTimer();

    // No forced stop starts now: every worker is about to be terminated anyway
    for (const entry of this.graceTimers.values()) {
      if (entry.timer !== undefined) clearTimeout(entry.timer);
    }
    this.graceTimers.clear();

    // Listeners stay attached through termination so worker.teardown.* events
    // are observable on the shutdown path - they are dropped at the end instead.
    // The shuttingDown flag stops failure handling from restarting workers here.

    // Terminate all worker threads. One a forced stop is already retiring is
    // waited for instead: terminating it a second time would run a second
    // teardown handshake, and report it failed.
    const retiring = [...this.retiring.values()];
    const shutdownPromises = this.threads.filter(w => !this.retiring.has(w)).map(async (worker) => {
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

    await Promise.all([...shutdownPromises, ...retiring]);
    this.threads = [];
    this._releaseAllResident();

    // Remove ALL event listeners to prevent process hanging
    this._removeAllListeners();
  }
}
