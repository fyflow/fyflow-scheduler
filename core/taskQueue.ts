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
 * An item can be **tombstoned** in place, in O(1): a cancelled task is left
 * where it is rather than spliced out, which would make cancelling every task
 * of a large queue quadratic. A tombstone is invisible to every reader -
 * `length`, `shift()`, iteration, `includes()` - and costs nothing until the
 * head reaches it.
 *
 * Several queues can share a **live count**, kept by every operation, so the
 * scheduler can ask "is any task blocked?" in O(1) rather than visiting every
 * blocked queue - a keyed group has one per key, thousands of them, and the
 * question is asked on every cancel.
 *
 * Internal: not exported from the package. Keeps the array-like surface the
 * scheduler and `examples/test-deadlock.ts` read (`length`, iteration).
 */
export class TaskQueue<T> implements Iterable<T> {
  private items: (T | undefined)[] = [];
  private head = 0;
  private readonly members?: Set<T>;
  /** Items tombstoned but not yet passed by the head. Created on first use. */
  private dead?: Set<T>;
  /** Shared with other queues: their live items, together. */
  private readonly counter?: { live: number };

  constructor(options: { trackMembership?: boolean; counter?: { live: number } } = {}) {
    if (options.trackMembership) this.members = new Set<T>();
    this.counter = options.counter;
  }

  /** Live items: tombstones are not counted. */
  get length(): number {
    return this.items.length - this.head - (this.dead?.size ?? 0);
  }

  /** The live item at `index` from the head, without removing it. Linear when tombstones are present. */
  at(index: number): T | undefined {
    if (index < 0) return undefined;
    if (!this.dead?.size) {
      return index >= this.length ? undefined : this.items[this.head + index];
    }
    let live = 0;
    for (let i = this.head; i < this.items.length; i++) {
      const item = this.items[i] as T;
      if (this.dead.has(item)) continue;
      if (live++ === index) return item;
    }
    return undefined;
  }

  push(item: T): void {
    this.items.push(item);
    this.members?.add(item);
    if (this.counter) this.counter.live++;
  }

  shift(): T | undefined {
    while (this.head < this.items.length) {
      const item = this.items[this.head] as T;
      // Released so a consumed task can be collected before the next compaction
      this.items[this.head] = undefined;
      this.head++;
      this._compact();
      if (this.dead?.delete(item)) continue;
      this.members?.delete(item);
      if (this.counter) this.counter.live--;
      return item;
    }
    return undefined;
  }

  private _compact(): void {
    if (this.head === this.items.length) {
      this.items = [];
      this.head = 0;
    } else if (this.head > 1024 && this.head * 2 > this.items.length) {
      this.items = this.items.slice(this.head);
      this.head = 0;
    }
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
    if (this.counter) this.counter.live += items.length;
  }

  /**
   * Mark `item`, which the caller knows is in this queue, as gone. O(1): it
   * stays in place and is skipped when the head reaches it. Tombstoning an item
   * twice, or one not in the queue, corrupts `length`, so the caller must know
   * where the item is rather than ask.
   */
  tombstone(item: T): void {
    (this.dead ??= new Set<T>()).add(item);
    this.members?.delete(item);
    if (this.counter) this.counter.live--;
  }

  /** O(1) with membership tracking, otherwise a linear scan. */
  includes(item: T): boolean {
    if (this.members) return this.members.has(item);
    return this.indexOf(item) !== -1;
  }

  /** Position among live items from the head, or -1. Linear. */
  indexOf(item: T): number {
    if (this.dead?.has(item)) return -1;
    let live = 0;
    for (let i = this.head; i < this.items.length; i++) {
      const current = this.items[i] as T;
      if (current === item) return live;
      if (!this.dead?.has(current)) live++;
    }
    return -1;
  }

  /** Removes the first live occurrence of `item`. Linear; for rare exits only. */
  remove(item: T): boolean {
    if (this.dead?.has(item)) return false;
    for (let i = this.head; i < this.items.length; i++) {
      if (this.items[i] !== item) continue;
      this.items.splice(i, 1);
      this.members?.delete(item);
      if (this.counter) this.counter.live--;
      return true;
    }
    return false;
  }

  clear(): void {
    if (this.counter) this.counter.live -= this.length;
    this.items = [];
    this.head = 0;
    this.members?.clear();
    this.dead?.clear();
  }

  toArray(): T[] {
    return [...this];
  }

  *[Symbol.iterator](): Iterator<T> {
    for (let i = this.head; i < this.items.length; i++) {
      const item = this.items[i] as T;
      if (this.dead?.has(item)) continue;
      yield item;
    }
  }
}
