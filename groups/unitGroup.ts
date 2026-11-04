import type {
  GaugeDescription,
  GaugeReading,
  ResourceGroup,
  ResourceGroupMetrics,
  ResourceGroupStats
} from './resourceGroup.ts';

/** A unit's name: whatever identifies it to the work - a device, a seat, a port. */
export type UnitId = string | number;

/**
 * Which units a holder will accept. Without one, any free unit will do.
 *
 * - `from` - the candidate set: units are granted from these only. `['gpu1']`
 *   pins a holder to one unit; with a cost of 2, `['gpu0', 'gpu1']` asks for
 *   both of them together.
 * - `prefer` - a soft hint: these are granted first when free. It changes
 *   which unit is granted, never whether the holder may start.
 */
export interface UnitRequest {
  from?: readonly UnitId[];
  prefer?: readonly UnitId[];
}

/** A request as unit indices: `from` ascending, undefined for every unit. */
interface ResolvedRequest {
  from?: number[];
  prefer?: number[];
  waitSet?: string;
}

const NO_REQUEST: ResolvedRequest = {};

export interface UnitGroupOptions {
  /** Stable id used in `workerGroups`, a pool's `groups` or `residentGroups`. Generated if omitted. */
  id?: string;
  /**
   * The request a task makes, for a task-held hold. Defaults to the task's
   * `units[id]`:
   *
   * ```typescript
   * new FyflowTask({ id, workerType, payload, units: { gpu: { from: ['gpu1'] } } })
   * ```
   */
  requestFrom?: (task: { units?: Record<string, unknown>; payload?: any }) => UnitRequest | undefined;
}

/**
 * A limit whose units have names. A holder takes `cost` free units, and is
 * told which: `onStart` returns them as an array - always an array, at a cost
 * of 1 too - and the scheduler hands that grant to the work, as
 * `context.grants[id]` for a task and `workerContext.grants[id]` for a resident
 * worker.
 *
 * ```typescript
 * const gpu = new UnitGroup(['gpu0', 'gpu1'], { id: 'gpu' });   // or new UnitGroup(2): units 0, 1
 * // in run(): const [card] = context.grants.gpu;
 * ```
 *
 * A hard cap, like {@link ConcurrentLimitGroup}: the scheduler checks and
 * acquires in one synchronous turn, so `onStart` always finds the units it is
 * asked for. If it does not, it throws - that is a scheduler bug, not a race.
 * The lowest-index free unit is granted, after any `prefer`red one.
 *
 * Fixed for the group's lifetime: units cannot be added or removed. Not keyed.
 */
export class UnitGroup extends EventTarget implements ResourceGroup {
  readonly id: string;
  readonly type = 'unit' as const;
  /** Every unit, in the order lowest-index-first grants them. */
  readonly units: readonly UnitId[];
  /** `units.length`. */
  readonly limit: number;

  private index = new Map<UnitId, number>();
  private resolved = new WeakMap<UnitRequest, ResolvedRequest>();
  private busy: Uint8Array;
  private running = 0;
  private requestFrom: (task: any) => UnitRequest | undefined;
  private stats = {
    totalAcquired: 0,
    totalReleased: 0
  };

  constructor(units: readonly UnitId[] | number, options: UnitGroupOptions = {}) {
    super();
    if (typeof units === 'number') {
      if (!Number.isInteger(units) || units <= 0) {
        throw new Error(`UnitGroup needs a positive integer number of units, got ${units}`);
      }
      units = Array.from({ length: units }, (_, i) => i);
    }
    if (!Array.isArray(units) || units.length === 0) {
      throw new Error('UnitGroup needs at least one unit');
    }
    units.forEach((unit, i) => {
      if (typeof unit !== 'string' && typeof unit !== 'number') {
        throw new Error(`UnitGroup units must be strings or numbers, got ${typeof unit}`);
      }
      if (this.index.has(unit)) throw new Error(`UnitGroup unit ${JSON.stringify(unit)} is listed twice`);
      this.index.set(unit, i);
    });
    this.units = Object.freeze([...units]);
    this.limit = this.units.length;
    this.busy = new Uint8Array(this.limit);
    this.id = options.id || `unit-${Math.random().toString(36).substr(2, 9)}`;
    const id = this.id;
    this.requestFrom = options.requestFrom
      ?? ((task: any) => task?.units?.[id] as UnitRequest | undefined);
  }

  /**
   * The request `task` makes of this group, validated. Called by the scheduler
   * when the task is added, so a bad request throws from `addTask`.
   *
   * @throws on a request naming a unit this group does not have, or asking for
   *   more units than its `from` lists.
   */
  requestFor(task: unknown, cost = 1): UnitRequest | undefined {
    const request = this.requestFrom(task);
    this.checkRequest(request, cost);
    return request;
  }

  /**
   * Throws if `request` could never be granted `cost` units: it names a unit
   * this group does not have, or its `from` lists fewer than `cost`.
   */
  checkRequest(request: UnitRequest | undefined, cost = 1): void {
    if (request === undefined || request === null) {
      if (cost > this.limit) {
        throw new Error(`UnitGroup "${this.id}" has ${this.limit} units, a cost of ${cost} can never be granted`);
      }
      return;
    }
    if (typeof request !== 'object') {
      throw new Error(`A request of UnitGroup "${this.id}" must be an object like { from: [...] }, got ${typeof request}`);
    }
    for (const field of ['from', 'prefer'] as const) {
      const list = request[field];
      if (list === undefined) continue;
      if (!Array.isArray(list)) throw new Error(`UnitGroup "${this.id}": ${field} must be an array`);
      for (const unit of list) {
        if (!this.index.has(unit)) {
          throw new Error(`UnitGroup "${this.id}" has no unit ${JSON.stringify(unit)} (in ${field})`);
        }
      }
    }
    const candidates = request.from ? new Set(request.from).size : this.limit;
    if (cost > candidates) {
      throw new Error(
        `UnitGroup "${this.id}": a cost of ${cost} from ${candidates} candidate unit(s) can never be granted`
      );
    }
  }

  /**
   * The queue a waiter with `request` belongs in: one per distinct candidate
   * set, so a release wakes the sets it can satisfy and nothing else.
   * Undefined for "any unit". `prefer` does not count - it never blocks.
   */
  waitSetFor(request: UnitRequest | undefined): string | undefined {
    return request ? this.resolve(request).waitSet : undefined;
  }

  /**
   * Whether two wait sets share a unit - undefined being every unit. Resident
   * admission is head-of-line only between waiters whose sets overlap, so a
   * pool pinned to a busy unit does not hold up one that wants a free one.
   */
  waitSetsOverlap(a: string | undefined, b: string | undefined): boolean {
    if (a === undefined || b === undefined) return true;
    const members = new Set(a.split(','));
    return b.split(',').some(i => members.has(i));
  }

  /** Free units among the candidates, `cost` of them needed (default 1). */
  canRun(_key?: string, cost: number = 1, request?: UnitRequest): boolean {
    const from = request ? this.resolve(request).from : undefined;
    if (!from) return this.limit - this.running >= cost;
    let free = 0;
    for (const i of from) {
      if (!this.busy[i] && ++free >= cost) return true;
    }
    return false;
  }

  /**
   * A request as unit indices, worked out once per request object. The
   * scheduler passes the same object on every call for a task - the check,
   * each wake, the acquire - and allocating a set on each of those showed as
   * heap under a deep queue of pinned waiters.
   */
  private resolve(request: UnitRequest): ResolvedRequest {
    let resolved = this.resolved.get(request);
    if (resolved) return resolved;
    const indices = (list: readonly UnitId[] | undefined) => list === undefined
      ? undefined
      : [...new Set(list)].map(u => this.index.get(u)).filter((i): i is number => i !== undefined);
    const from = indices(request.from)?.sort((a, b) => a - b);
    // The whole group is "any": one queue, not two
    const whole = from === undefined || from.length === this.limit;
    resolved = {
      from: whole ? undefined : from,
      prefer: indices(request.prefer),
      waitSet: whole ? undefined : from!.join(',')
    };
    this.resolved.set(request, resolved);
    return resolved;
  }

  /**
   * Grant `cost` free units: preferred ones first, then lowest index, from the
   * candidates. Returns them as an array.
   *
   * @throws if fewer than `cost` candidates are free - admission is a hard
   *   cap, so this means the scheduler admitted what it should not have.
   */
  onStart(_key?: string, cost: number = 1, request?: UnitRequest): UnitId[] {
    const { from, prefer } = request ? this.resolve(request) : NO_REQUEST;
    const granted: number[] = [];
    const take = (i: number) => {
      if (granted.length >= cost || this.busy[i] || granted.includes(i)) return;
      if (from && !from.includes(i)) return;
      granted.push(i);
    };
    for (const i of prefer ?? []) take(i);
    if (from) for (const i of from) take(i);
    else for (let i = 0; i < this.limit && granted.length < cost; i++) take(i);

    if (granted.length < cost) {
      throw new Error(
        `UnitGroup "${this.id}" was asked for ${cost} unit(s) with ${granted.length} free - ` +
        `admission is a hard cap, so this is a scheduler bug`
      );
    }
    for (const i of granted) this.busy[i] = 1;
    this.running += granted.length;
    this.stats.totalAcquired += granted.length;
    return granted.map(i => this.units[i]);
  }

  /**
   * Free exactly the units in `grant`.
   *
   * @throws on a unit this group did not grant, or one already free - a
   *   double release is a bug worth surfacing, not something to absorb.
   */
  onFinish(_key?: string, _cost?: number, grant?: unknown): void {
    if (!Array.isArray(grant)) {
      throw new Error(`UnitGroup "${this.id}" released without the grant it returned`);
    }
    const indices = grant.map(unit => {
      const i = this.index.get(unit);
      if (i === undefined || !this.busy[i]) {
        throw new Error(`UnitGroup "${this.id}" released unit ${JSON.stringify(unit)}, which it had not granted`);
      }
      return i;
    });
    for (const i of indices) this.busy[i] = 0;
    this.running -= indices.length;
    this.stats.totalReleased += indices.length;
  }

  /** Whether `unit` is granted to someone right now. */
  isBusy(unit: UnitId): boolean {
    const i = this.index.get(unit);
    if (i === undefined) throw new Error(`UnitGroup "${this.id}" has no unit ${JSON.stringify(unit)}`);
    return this.busy[i] === 1;
  }

  /** Units granted right now, in unit order. */
  getBusyUnits(): UnitId[] {
    return this.units.filter((_, i) => this.busy[i]);
  }

  /** Units free right now, in unit order. */
  getFreeUnits(): UnitId[] {
    return this.units.filter((_, i) => !this.busy[i]);
  }

  getMetrics(): ResourceGroupMetrics {
    return {
      limit: this.limit,
      running: this.running,
      available: this.limit - this.running,
      utilization: this.running / this.limit
    };
  }

  getStats(): ResourceGroupStats {
    return { ...this.stats };
  }

  /**
   * One `level` gauge, as {@link ConcurrentLimitGroup}. Which units are busy is
   * not a number: it is on the `grant` of each `resource.acquired` and
   * `resource.released`, and {@link getBusyUnits}.
   */
  describe(): GaugeDescription {
    return {
      gauges: [{ id: 'units', label: this.id, kind: 'level', unit: 'units', limit: this.limit }]
    };
  }

  read(): GaugeReading[] {
    return [{ id: 'units', value: this.running, limit: this.limit }];
  }
}
