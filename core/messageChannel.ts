/**
 * Worker -> host messages: `context.emit()` and `workerContext.emit()`.
 *
 * The logic shared by the three places a worker runs - the Deno/browser worker
 * wrapper, the Node worker wrapper and `InlineWrapper` - so the gate, buffers,
 * cap, clone and flush cannot drift apart between them. Runtime-neutral: it
 * takes a `send` function and knows nothing about `postMessage`, `parentPort`
 * or `EventTarget`.
 *
 * Internal: not exported from the package.
 */

/** Per-pool bounds on what a worker can send, as `WorkerManager`'s `messages` option. */
export interface MessageOptions {
  /** How long a buffer may hold events before they are sent. Default 20. */
  flushIntervalMs?: number;
  /**
   * Events each buffer - one per running attempt, one for the worker - holds
   * between flushes; further emits are discarded and counted. Default 100.
   */
  maxBuffered?: number;
}

/**
 * Chosen by measurement: 5,000 events/s per buffer keeps 16 threaded workers
 * emitting in tight loops just inside what the main thread can absorb (worst
 * stall 16-47 ms against 16 ms idle), and the cap keeps one delivered batch to
 * ~2 ms of main-thread work. Twice the rate stalled it for 65-165 ms.
 */
export const DEFAULT_MESSAGE_OPTIONS: Required<MessageOptions> = {
  flushIntervalMs: 20,
  maxBuffered: 100
};

/**
 * Validated copy of `options` with defaults applied.
 * @throws RangeError for a non-positive or non-finite interval, or a cap that
 *   is not a positive integer.
 */
export function resolveMessageOptions(options: MessageOptions = {}): Required<MessageOptions> {
  const resolved = { ...DEFAULT_MESSAGE_OPTIONS, ...options };
  const { flushIntervalMs, maxBuffered } = resolved;
  if (typeof flushIntervalMs !== 'number' || !Number.isFinite(flushIntervalMs) || flushIntervalMs <= 0) {
    throw new RangeError(`messages.flushIntervalMs must be a positive number, got ${flushIntervalMs}`);
  }
  if (!Number.isInteger(maxBuffered) || maxBuffered <= 0) {
    throw new RangeError(`messages.maxBuffered must be a positive integer, got ${maxBuffered}`);
  }
  return resolved;
}

/** One emitted event as it travels from a worker to its wrapper. */
export interface ChannelEvent {
  /** Set for a task's event; absent for the worker's own. */
  taskId?: string;
  name: string;
  data: unknown;
  seq: number;
  timestamp: number;
  dropped?: number;
  error?: string;
}

/**
 * Where a wrapper hands a delivered batch: set by its pool. A direct call rather
 * than an event per message, because a flooding worker delivers tens of
 * thousands a second and every hop is main-thread work. `taskId` is set on a
 * task's events and absent on the worker's own.
 */
export type MessageSink = (events: ChannelEvent[], workerId: string, workerType: 'inline' | 'thread') => void;

/** The message a discarded-at-the-cap marker carries. */
export const DISCARDED_AT_CAP = 'Discarded at the message cap';

/**
 * One stream: an attempt's, or the worker's own. Holds what has been emitted
 * since the last flush.
 */
export class MessageStream {
  seq = 0;
  closed = false;
  buffer: ChannelEvent[] = [];
  dropped = 0;
  /** The most recent discarded emit, kept so a stream that ends over the cap can still report its count. */
  lastDiscarded?: { name: string; seq: number; timestamp: number };
  readonly taskId?: string;

  // Plain assignments, not parameter properties: Node's type stripping cannot
  // erase those, and this file is published as source
  constructor(taskId?: string) {
    this.taskId = taskId;
  }
}

export class WorkerMessageChannel {
  /** Whether anyone listens to `task.message` / `worker.message`. Closed until told otherwise. */
  taskGate = false;
  workerGate = false;
  private options: Required<MessageOptions>;
  private readonly workerStream = new MessageStream();
  /** Streams holding something to send. A Set keeps flush order stable. */
  private pending = new Set<MessageStream>();
  private lastFlush = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private readonly send: (events: ChannelEvent[]) => void;

  constructor(send: (events: ChannelEvent[]) => void, options?: MessageOptions) {
    this.send = send;
    this.options = resolveMessageOptions(options);
  }

  configure(options: MessageOptions) {
    this.options = resolveMessageOptions(options);
  }

  setGate(taskGate: boolean, workerGate: boolean) {
    this.taskGate = taskGate;
    this.workerGate = workerGate;
  }

  openAttempt(taskId: string): MessageStream {
    return new MessageStream(taskId);
  }

  emitTask(stream: MessageStream, name: unknown, data: unknown) {
    this._emit(stream, this.taskGate, name, data);
  }

  emitWorker(name: unknown, data: unknown) {
    this._emit(this.workerStream, this.workerGate, name, data);
  }

  /**
   * Stop accepting emits on a stream. What it already holds is still sent by
   * the next flush - the caller flushes before the message that ends it.
   */
  close(stream: MessageStream) {
    if (stream.closed) return;
    stream.closed = true;
    this._carryTrailingDrops(stream);
  }

  /** Discard a stream outright, buffered events included: its run was abandoned. */
  discard(stream: MessageStream) {
    stream.closed = true;
    stream.buffer = [];
    stream.dropped = 0;
    this.pending.delete(stream);
  }

  /** Close the worker's own stream, at teardown. */
  closeWorker() {
    this.close(this.workerStream);
  }

  /** Send everything buffered, now, as one batch. */
  flush() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    // Nothing to send costs nothing, not even a clock read: this runs before
    // every inline settle and lifecycle event
    if (this.pending.size === 0) return;
    this.lastFlush = performance.now();

    const events: ChannelEvent[] = [];
    for (const stream of this.pending) {
      for (const event of stream.buffer) events.push(event);
      stream.buffer = [];
    }
    this.pending.clear();
    this.send(events);
  }

  private _emit(stream: MessageStream, gate: boolean, name: unknown, data: unknown) {
    // Gated emits still use a number, so a late subscriber can see how many it
    // missed
    const seq = stream.seq++;
    if (stream.closed || !gate) return;

    const now = performance.now();
    const due = now - this.lastFlush >= this.options.flushIntervalMs;

    if (stream.buffer.length >= this.options.maxBuffered) {
      stream.dropped++;
      stream.lastDiscarded = { name: String(name), seq, timestamp: Date.now() };
      // Time is checked here too: a synchronous loop that never yields never
      // fires the timer, and this is what empties the buffer for it
      if (due) this.flush();
      return;
    }

    // Cloned here rather than when sent: the event describes the moment it was
    // emitted, and a clone failure surfaces at the call that caused it rather
    // than losing a whole batch later
    let cloned: unknown;
    let error: string | undefined;
    try {
      cloned = structuredClone(data);
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }

    const event: ChannelEvent = { name: String(name), data: cloned, seq, timestamp: Date.now() };
    if (stream.taskId !== undefined) event.taskId = stream.taskId;
    if (error !== undefined) event.error = error;
    if (stream.dropped > 0) {
      event.dropped = stream.dropped;
      stream.dropped = 0;
      stream.lastDiscarded = undefined;
    }
    stream.buffer.push(event);
    this.pending.add(stream);

    if (due) {
      this.flush();
    } else if (!this.timer) {
      // Armed only while something is buffered, so an idle worker holds no timer
      this.timer = setTimeout(() => {
        this.timer = null;
        this.flush();
      }, this.options.flushIntervalMs - (now - this.lastFlush));
    }
  }

  /**
   * A stream ending over the cap has a count no later event will carry. Its
   * last discarded emit is delivered instead, without its data, so the count is
   * never lost: delivered events plus `dropped` always equals the emits made
   * while the gate was open.
   */
  private _carryTrailingDrops(stream: MessageStream) {
    if (stream.dropped === 0 || !stream.lastDiscarded) return;
    const { name, seq, timestamp } = stream.lastDiscarded;
    const marker: ChannelEvent = { name, data: undefined, seq, timestamp, error: DISCARDED_AT_CAP };
    if (stream.taskId !== undefined) marker.taskId = stream.taskId;
    if (stream.dropped > 1) marker.dropped = stream.dropped - 1;
    stream.dropped = 0;
    stream.lastDiscarded = undefined;
    stream.buffer.push(marker);
    this.pending.add(stream);
  }
}
