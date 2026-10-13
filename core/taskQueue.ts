/**
 * A FIFO queue with an O(1) `shift()`, for the scheduler's ready and blocked
 * queues.
 *
 * These used to be plain arrays consumed with `shift()`. V8 makes that free on
 * small arrays, but above roughly 16,000 elements it copies the whole array on
 * every call - measured on Deno 2.9.6 at 0.1 µs per shift at 15,000 elements,
 * 79 µs at 17,000 and 232 µs at 50,000 - so draining a large queue was
 * quadratic. Here the head is an index, and the consumed prefix is dropped only
 * once it is more than half the array, which keeps that cost amortised O(1).
 *
 * Membership tracking is opt-in, for the blocked queues, whose push is guarded
 * by "is this task already queued here?". A linear `includes()` made building a
 * large blocked queue one task at a time quadratic too. The ready queues never
 * ask, so they do not pay for the set.
 *
 * Internal: not exported from the package. Keeps the array-like surface the
 * scheduler and `examples/test-deadlock.ts` read (`length`, iteration).
 */
export class TaskQueue<T> implements Iterable<T> {
  private items: (T | undefined)[] = [];
  private head = 0;
  private readonly members?: Set<T>;

  constructor(options: { trackMembership?: boolean } = {}) {
    if (options.trackMembership) this.members = new Set<T>();
  }

  get length(): number {
    return this.items.length - this.head;
  }

  /** The item at `index` from the head, without removing it. */
  at(index: number): T | undefined {
    return index < 0 || index >= this.length ? undefined : this.items[this.head + index];
  }

  push(item: T): void {
    this.items.push(item);
    this.members?.add(item);
  }

  shift(): T | undefined {
    if (this.head >= this.items.length) return undefined;
    const item = this.items[this.head] as T;
    // Released so a consumed task can be collected before the next compaction
    this.items[this.head] = undefined;
    this.head++;
    this.members?.delete(item);

    if (this.head === this.items.length) {
      this.items = [];
      this.head = 0;
    } else if (this.head > 1024 && this.head * 2 > this.items.length) {
      this.items = this.items.slice(this.head);
      this.head = 0;
    }
    return item;
  }

  /** Puts items back at the head, in the order given. */
  unshift(...items: T[]): void {
    if (items.length === 0) return;
    if (items.length <= this.head) {
      // The common case - a task taken off the head and put straight back -
      // reuses the consumed slot instead of moving the whole queue
      this.head -= items.length;
      for (let i = 0; i < items.length; i++) this.items[this.head + i] = items[i];
    } else {
      this.items = [...items, ...this.items.slice(this.head)];
      this.head = 0;
    }
    if (this.members) for (const item of items) this.members.add(item);
  }

  /** O(1) with membership tracking, otherwise a linear scan. */
  includes(item: T): boolean {
    if (this.members) return this.members.has(item);
    return this.indexOf(item) !== -1;
  }

  /** Position from the head, or -1. Linear. */
  indexOf(item: T): number {
    for (let i = this.head; i < this.items.length; i++) {
      if (this.items[i] === item) return i - this.head;
    }
    return -1;
  }

  /** Removes the first occurrence of `item`. Linear; for rare exits only. */
  remove(item: T): boolean {
    const index = this.indexOf(item);
    if (index === -1) return false;
    this.items.splice(this.head + index, 1);
    this.members?.delete(item);
    return true;
  }

  clear(): void {
    this.items = [];
    this.head = 0;
    this.members?.clear();
  }

  toArray(): T[] {
    return this.items.slice(this.head) as T[];
  }

  *[Symbol.iterator](): Iterator<T> {
    for (let i = this.head; i < this.items.length; i++) yield this.items[i] as T;
  }
}
