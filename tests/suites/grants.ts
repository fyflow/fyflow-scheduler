// Grants: what a resource group's onStart returns, carried to the work.
// UnitGroup is the group that grants - named units - and a custom group can
// return its own.
//
// Each test runs on both pool types unless it says otherwise. The threaded
// runs are what exercise workerWrapper.ts under Deno and workerWrapper.node.ts
// under Node: a grant crosses the thread boundary twice, with the task's run
// message and, for a resident worker, with its init.

import { WorkerManager, FyflowScheduler, FyflowTask, UnitGroup } from '../../index.ts';
import { runTest, type TestResult } from '../harness.ts';

// Node.js process declaration for cross-platform compatibility
declare const process: any;

interface TestSuiteResult {
  platform: string; totalTests: number; passed: number; failed: number;
  duration: number; results: TestResult[];
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const KINDS = [true, false] as const;
const kindOf = (inline: boolean) => inline ? 'inline' : 'threaded';
const same = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);

/** A custom group that grants an object, and checks it gets the same one back. */
class TokenGroup extends EventTarget {
  readonly type = 'token';
  running = 0;
  issued: object[] = [];
  returnedSame = 0;
  returnedOther = 0;
  constructor(readonly id: string, readonly limit = 4, private make = (n: number): object => ({ token: n })) { super(); }
  canRun(_key?: string, cost = 1) { return this.running + cost <= this.limit; }
  onStart(_key?: string, cost = 1) {
    this.running += cost;
    const grant = this.make(this.issued.length);
    this.issued.push(grant);
    return grant;
  }
  onFinish(_key?: string, cost = 1, grant?: unknown) {
    this.running -= cost;
    if (this.issued.includes(grant as object)) this.returnedSame++;
    else this.returnedOther++;
  }
  getMetrics() { return { limit: this.limit, running: this.running, available: this.limit - this.running, utilization: this.running / this.limit }; }
}

/** A keyed custom group granting `${key}#n`, to check the per-key shape. */
class KeyedTokenGroup extends EventTarget {
  readonly type = 'keyed-token';
  readonly keyed = true;
  running = 0;
  n = 0;
  constructor(readonly id: string) { super(); }
  keyFor(task: any) { return task.payload.keys; }
  canRun() { return true; }
  onStart(key?: string) { this.running++; return `${key}#${this.n++}`; }
  onFinish() { this.running--; }
  getMetrics() { return { limit: 100, running: this.running, available: 100 - this.running, utilization: this.running / 100 }; }
}

class GrantsTestSuite {
  private results: TestResult[] = [];
  private startTime = 0;
  private workerUrl = "";
  private cancelWorkerUrl = "";
  private schedulers: FyflowScheduler[] = [];
  private pools: WorkerManager[] = [];

  async cleanup(): Promise<void> {
    for (const scheduler of this.schedulers) {
      try { await scheduler.shutdown(); } catch { /* already down */ }
    }
    for (const pool of this.pools) {
      try { await pool.shutdown(); } catch { /* already down */ }
    }
    this.schedulers = [];
    this.pools = [];
  }

  private async runTest(name: string, testFn: () => Promise<void>): Promise<TestResult> {
    try {
      return await runTest(name, testFn);
    } finally {
      await this.cleanup();
    }
  }

  async runAllTests(exitOnComplete = true): Promise<TestSuiteResult> {
    if (typeof Deno !== "undefined") {
      this.workerUrl = new URL("../workers/grantWorker.ts", import.meta.url).href;
      this.cancelWorkerUrl = new URL("../workers/cancelWorker.ts", import.meta.url).href;
    } else {
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      this.workerUrl = new URL((await import('../workers/grantWorker.ts?worker-direct')).default).href;
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      this.cancelWorkerUrl = new URL((await import('../workers/cancelWorker.ts?worker-direct')).default).href;
    }

    const platform = typeof globalThis !== 'undefined' && 'Deno' in globalThis ? 'Deno' : 'Node.js';
    console.log(`🚀 FyFlow Grant Tests - ${platform}`);
    console.log('='.repeat(60));

    this.startTime = performance.now();
    this.results = [];

    this.results.push(await this.runTest('Two Tasks Get Different Units, A Third The Freed One', () => this.testDistinctUnits()));
    this.results.push(await this.runTest('Grants Ride On The Events, And The Fold Balances', () => this.testEvents()));
    this.results.push(await this.runTest('A Resident Worker Sees Its Units From The Constructor', () => this.testResident()));
    this.results.push(await this.runTest('Idle Teardown Frees Exactly The Worker\'s Units', () => this.testIdleTeardown()));
    this.results.push(await this.runTest('Task And Resident Grants Merge', () => this.testMerge()));
    this.results.push(await this.runTest('A Retry Is Granted Afresh', () => this.testRetry()));
    this.results.push(await this.runTest('restartWorker Gives The Replacement Its Own Grant', () => this.testRestart()));
    this.results.push(await this.runTest('A Custom Group\'s Grant Reaches run() And Returns To onFinish', () => this.testCustomGroup()));
    this.results.push(await this.runTest('A Keyed Group\'s Grants Are Per Key', () => this.testKeyedShape()));
    this.results.push(await this.runTest('A Grant That Cannot Be Copied Fails The Task And Frees The Units', () => this.testUncloneable()));
    this.results.push(await this.runTest('A Forced Stop Frees The Unit Only Once The Thread Is Dead', () => this.testForcedStop()));
    this.results.push(await this.runTest('UnitGroup Validates, And Refuses A Double Release', () => this.testValidation()));

    const totalDuration = performance.now() - this.startTime;
    this.results = this.results.filter(r => !r.skipped);
    const passed = this.results.filter(r => r.passed).length;
    const failed = this.results.length - passed;

    console.log(`\n📊 Grant Test Summary - ${platform}`);
    console.log('='.repeat(60));
    console.log(`Total Tests: ${this.results.length}`);
    console.log(`✅ Passed: ${passed}`);
    console.log(`❌ Failed: ${failed}`);
    console.log(`⏱️  Duration: ${totalDuration.toFixed(1)}ms`);

    if (failed > 0) {
      console.log('\n❌ Failed Tests:');
      this.results.filter(r => !r.passed).forEach(r => console.log(`  • ${r.name}: ${r.error}`));
      if (exitOnComplete) {
        if (typeof Deno !== 'undefined') { Deno.exit(1); } else { process.exit(1); }
      }
    } else {
      console.log('\n🎉 All grant tests passed!');
    }

    return { platform, totalTests: this.results.length, passed, failed, duration: totalDuration, results: this.results };
  }

  // --- helpers ---------------------------------------------------------------

  private setup(inline: boolean, groups: Record<string, any>, options: any = {}, url = this.workerUrl) {
    const pool = new WorkerManager(url, {
      maxThreads: 1, maxConcurrentTasks: 4, inline, idleTimeout: 0, ...options
    });
    this.pools.push(pool);
    const scheduler = new FyflowScheduler({ G: pool }, groups);
    this.schedulers.push(scheduler);
    return { pool, scheduler };
  }

  private task(id: string, payload: any = {}, extra: any = {}) {
    return new FyflowTask({ id, workerType: 'G', payload, ...extra });
  }

  private async settle(scheduler: FyflowScheduler, task: FyflowTask): Promise<{ ok: boolean; value: any }> {
    const outcome = await Promise.race([
      (scheduler.addTask(task, { createPromise: true }) as Promise<any>).then(
        value => ({ ok: true, value }), error => ({ ok: false, value: error })
      ),
      sleep(10_000).then(() => ({ ok: false, value: 'NEVER SETTLED' }))
    ]);
    if (outcome.value === 'NEVER SETTLED') throw new Error(`Task ${task.id} never settled`);
    return outcome;
  }

  private async succeed(scheduler: FyflowScheduler, task: FyflowTask): Promise<any> {
    const outcome = await this.settle(scheduler, task);
    if (!outcome.ok) throw new Error(`Precondition: task ${task.id} failed: ${outcome.value?.message ?? outcome.value}`);
    return outcome.value;
  }

  /** acquired/released per holder, with grants, in order. */
  private recordResource(scheduler: FyflowScheduler) {
    const log: Array<{ type: string; holderId: string; groupId: string; grant: any; lifetime: string }> = [];
    for (const type of ['resource.acquired', 'resource.released']) {
      scheduler.addEventListener(type, (e: any) => log.push({
        type: type.slice(9), holderId: e.detail.holderId, groupId: e.detail.groupId,
        grant: e.detail.grant, lifetime: e.detail.lifetime
      }));
    }
    return log;
  }

  // --- tests -----------------------------------------------------------------

  private async testDistinctUnits(): Promise<void> {
    for (const inline of KINDS) {
      const gpu = new UnitGroup(['a', 'b'], { id: 'gpu' });
      const { scheduler } = this.setup(inline, { gpu }, { groups: ['gpu'] });
      const log = this.recordResource(scheduler);

      const runs = [
        this.succeed(scheduler, this.task('t1', { holdMs: 80 })),
        this.succeed(scheduler, this.task('t2', { holdMs: 120 })),
        this.succeed(scheduler, this.task('t3'))
      ];
      // t3 waits: both units are out
      if (log.some(e => e.holderId === 't3')) throw new Error(`${kindOf(inline)}: t3 acquired with both units held`);
      const [r1, r2, r3] = await Promise.all(runs);

      const units = [r1.task.gpu, r2.task.gpu, r3.task.gpu];
      if (!units.every(u => Array.isArray(u) && u.length === 1)) {
        throw new Error(`${kindOf(inline)}: a grant is not a one-unit array: ${JSON.stringify(units)}`);
      }
      if (r1.task.gpu[0] === r2.task.gpu[0]) throw new Error(`${kindOf(inline)}: t1 and t2 share ${r1.task.gpu[0]}`);
      // t1 released first, so t3 has its unit
      if (r3.task.gpu[0] !== r1.task.gpu[0]) {
        throw new Error(`${kindOf(inline)}: t3 got ${r3.task.gpu[0]}, but ${r1.task.gpu[0]} was the one freed`);
      }
      if (!r1.frozen) throw new Error(`${kindOf(inline)}: context.grants is not frozen`);
      if (gpu.getBusyUnits().length !== 0) throw new Error(`${kindOf(inline)}: units left busy: ${gpu.getBusyUnits()}`);
    }
  }

  private async testEvents(): Promise<void> {
    for (const inline of KINDS) {
      const gpu = new UnitGroup(['a', 'b', 'c'], { id: 'gpu' });
      const { scheduler } = this.setup(inline, { gpu }, { groups: ['gpu'] });
      const log = this.recordResource(scheduler);
      const results = await Promise.all(Array.from({ length: 9 }, (_, i) =>
        this.succeed(scheduler, this.task(`e${i}`, { holdMs: 10 + (i % 3) * 15 }))));

      // Folding the events alone reconstructs which unit is busy
      const busy = new Map<string, string>();
      for (const e of log) {
        const [unit] = e.grant;
        if (e.type === 'acquired') {
          if (busy.has(unit)) throw new Error(`${kindOf(inline)}: ${unit} acquired by ${e.holderId} while ${busy.get(unit)} held it`);
          busy.set(unit, e.holderId);
        } else {
          if (busy.get(unit) !== e.holderId) throw new Error(`${kindOf(inline)}: ${e.holderId} released ${unit}, held by ${busy.get(unit)}`);
          busy.delete(unit);
        }
      }
      if (busy.size !== 0) throw new Error(`${kindOf(inline)}: fold ends with ${[...busy.keys()]} busy`);
      // And the event's grant is the one the task saw
      results.forEach((r, i) => {
        const acquired = log.find(e => e.type === 'acquired' && e.holderId === `e${i}`)!;
        if (!same(acquired.grant, r.task.gpu)) throw new Error(`${kindOf(inline)}: e${i} saw ${JSON.stringify(r.task.gpu)}, the event said ${JSON.stringify(acquired.grant)}`);
      });
    }
  }

  private async testResident(): Promise<void> {
    for (const inline of KINDS) {
      const gpu = new UnitGroup(['g0', 'g1', 'g2', 'g3'], { id: 'gpu' });
      const { pool, scheduler } = this.setup(inline, { gpu }, {
        maxThreads: 2, maxConcurrentTasks: 1, residentGroups: { gpu: 2 }
      });
      const log = this.recordResource(scheduler);
      const results = await Promise.all(Array.from({ length: 6 }, (_, i) =>
        this.succeed(scheduler, this.task(`r${i}`, { holdMs: 30 }))));

      const byWorker = new Map<string, any>();
      for (const r of results) {
        // From the constructor on, the same units: ctor, setup and every task
        if (!same(r.ctor, r.setup) || !same(r.ctor?.gpu, r.task.gpu)) {
          throw new Error(`${kindOf(inline)}: ctor ${JSON.stringify(r.ctor)}, setup ${JSON.stringify(r.setup)}, task ${JSON.stringify(r.task)}`);
        }
        if (!Array.isArray(r.ctor.gpu) || new Set(r.ctor.gpu).size !== 2) {
          throw new Error(`${kindOf(inline)}: a worker of cost 2 got ${JSON.stringify(r.ctor.gpu)}`);
        }
        byWorker.set(r.workerId, r.ctor.gpu);
        if (!same(pool.getResidentGrants(r.workerId)?.gpu, r.ctor.gpu)) {
          throw new Error(`${kindOf(inline)}: getResidentGrants disagrees with what the worker saw`);
        }
      }
      if (byWorker.size !== 2) throw new Error(`${kindOf(inline)}: ${byWorker.size} workers, expected 2`);
      const all = [...byWorker.values()].flat();
      if (new Set(all).size !== 4) throw new Error(`${kindOf(inline)}: workers share units: ${JSON.stringify([...byWorker])}`);

      const acquired = log.filter(e => e.lifetime === 'resident' && e.type === 'acquired');
      if (acquired.length !== 2 || !acquired.every(e => same(e.grant, byWorker.get(e.holderId)))) {
        throw new Error(`${kindOf(inline)}: resident acquires ${JSON.stringify(acquired)}`);
      }
      await scheduler.shutdown();
      const released = log.filter(e => e.lifetime === 'resident' && e.type === 'released');
      if (released.length !== 2 || !released.every(e => same(e.grant, byWorker.get(e.holderId)))) {
        throw new Error(`${kindOf(inline)}: resident releases ${JSON.stringify(released)}`);
      }
      if (gpu.getBusyUnits().length !== 0) throw new Error(`${kindOf(inline)}: units left busy after shutdown: ${gpu.getBusyUnits()}`);
    }
  }

  private async testIdleTeardown(): Promise<void> {
    for (const inline of KINDS) {
      const gpu = new UnitGroup(3, { id: 'gpu' });
      const { pool, scheduler } = this.setup(inline, { gpu }, {
        maxThreads: 1, maxConcurrentTasks: 1, residentGroups: { gpu: 2 }, idleTimeout: 50, idleCheckIntervalMs: 20
      });
      const log = this.recordResource(scheduler);
      const first = await this.succeed(scheduler, this.task('i1'));
      const deadline = Date.now() + 3000;
      while (pool.getWorkerIds().length > 0 && Date.now() < deadline) await sleep(20);
      if (pool.getWorkerIds().length > 0) throw new Error(`${kindOf(inline)}: the idle worker was never torn down`);
      if (gpu.getBusyUnits().length !== 0) throw new Error(`${kindOf(inline)}: teardown left ${gpu.getBusyUnits()} busy`);
      const released = log.find(e => e.type === 'released' && e.lifetime === 'resident');
      if (!released || released.holderId !== first.workerId || !same(released.grant, first.ctor.gpu)) {
        throw new Error(`${kindOf(inline)}: teardown released ${JSON.stringify(released)}, the worker held ${JSON.stringify(first.ctor.gpu)}`);
      }

      // A worker created afterwards gets a valid grant of its own
      const second = await this.succeed(scheduler, this.task('i2'));
      if (second.workerId === first.workerId || second.ctor.gpu?.length !== 2) {
        throw new Error(`${kindOf(inline)}: the next worker got ${JSON.stringify(second.ctor)}`);
      }
    }
  }

  private async testMerge(): Promise<void> {
    for (const inline of KINDS) {
      const gpu = new UnitGroup(['g0'], { id: 'gpu' });
      const seat = new UnitGroup(['s0', 's1'], { id: 'seat' });
      const { scheduler } = this.setup(inline, { gpu, seat }, {
        maxConcurrentTasks: 1, residentGroups: ['gpu']
      });
      const withSeat = await this.succeed(scheduler, this.task('m1', {}, { workerGroups: ['seat'] }));
      if (!same(withSeat.task, { gpu: ['g0'], seat: ['s0'] })) {
        throw new Error(`${kindOf(inline)}: task context.grants ${JSON.stringify(withSeat.task)}`);
      }
      if (!same(withSeat.ctor, { gpu: ['g0'] })) throw new Error(`${kindOf(inline)}: worker grants ${JSON.stringify(withSeat.ctor)}`);

      // A task holding nothing of its own still sees its worker's
      const plain = await this.succeed(scheduler, this.task('m2'));
      if (!same(plain.task, { gpu: ['g0'] })) throw new Error(`${kindOf(inline)}: plain task grants ${JSON.stringify(plain.task)}`);
    }

    // And nothing at all is an empty object, not undefined
    for (const inline of KINDS) {
      const { scheduler } = this.setup(inline, {});
      const none = await this.succeed(scheduler, this.task('n'));
      if (!same(none.task, {}) || !same(none.ctor, {})) {
        throw new Error(`${kindOf(inline)}: no groups gave task ${JSON.stringify(none.task)}, worker ${JSON.stringify(none.ctor)}`);
      }
    }
  }

  private async testRetry(): Promise<void> {
    for (const inline of KINDS) {
      const gpu = new UnitGroup(['a', 'b'], { id: 'gpu' });
      const { scheduler } = this.setup(inline, { gpu }, { groups: ['gpu'] });
      const log = this.recordResource(scheduler);
      const result = await this.succeed(scheduler, this.task('retry', { failFirst: true }, {
        retryPolicy: { maxRetries: 1, backoffMs: 1 }
      }));
      const mine = log.filter(e => e.holderId === 'retry').map(e => e.type);
      if (mine.join(',') !== 'acquired,released,acquired,released') {
        throw new Error(`${kindOf(inline)}: the retry's holds were ${mine}`);
      }
      const lastAcquire = log.filter(e => e.holderId === 'retry' && e.type === 'acquired').at(-1)!;
      if (!same(result.task.gpu, lastAcquire.grant)) {
        throw new Error(`${kindOf(inline)}: the retry ran with ${JSON.stringify(result.task.gpu)}, granted ${JSON.stringify(lastAcquire.grant)}`);
      }
      if (gpu.getBusyUnits().length !== 0) throw new Error(`${kindOf(inline)}: units left busy`);
    }
  }

  private async testRestart(): Promise<void> {
    for (const inline of KINDS) {
      const gpu = new UnitGroup(['g0', 'g1', 'g2'], { id: 'gpu' });
      const { pool, scheduler } = this.setup(inline, { gpu }, {
        maxThreads: 1, maxConcurrentTasks: 1, residentGroups: { gpu: 2 }
      });
      const first = await this.succeed(scheduler, this.task('w1'));
      if (!(await pool.restartWorker(first.workerId))) throw new Error('restartWorker found no worker');

      const [replacement] = pool.getWorkerIds();
      if (!replacement || replacement === first.workerId) throw new Error(`${kindOf(inline)}: no replacement worker`);
      if (pool.getResidentGrants(first.workerId) !== undefined) throw new Error(`${kindOf(inline)}: the old worker still holds its units`);
      const grants = pool.getResidentGrants(replacement);
      if ((grants?.gpu as unknown[])?.length !== 2) throw new Error(`${kindOf(inline)}: the replacement holds ${JSON.stringify(grants)}`);
      // Two units held, not four: the old holding came back
      if (gpu.getBusyUnits().length !== 2) throw new Error(`${kindOf(inline)}: ${gpu.getBusyUnits().length} units busy after the restart`);

      const second = await this.succeed(scheduler, this.task('w2'));
      if (second.workerId !== replacement || !same(second.ctor, grants)) {
        throw new Error(`${kindOf(inline)}: the replacement saw ${JSON.stringify(second.ctor)}, holds ${JSON.stringify(grants)}`);
      }
    }
  }

  private async testCustomGroup(): Promise<void> {
    for (const inline of KINDS) {
      const custom = new TokenGroup('custom');
      const { scheduler } = this.setup(inline, { custom }, { groups: ['custom'] });
      const results = await Promise.all([0, 1, 2].map(i => this.succeed(scheduler, this.task(`c${i}`, { holdMs: 10 }))));
      const tokens = results.map(r => r.task.custom?.token).sort();
      if (!same(tokens, [0, 1, 2])) throw new Error(`${kindOf(inline)}: tasks saw ${JSON.stringify(results.map(r => r.task))}`);
      // onFinish got the very object onStart returned, never the copy
      if (custom.returnedSame !== 3 || custom.returnedOther !== 0) {
        throw new Error(`${kindOf(inline)}: onFinish got the original ${custom.returnedSame} times, something else ${custom.returnedOther}`);
      }
    }
  }

  private async testKeyedShape(): Promise<void> {
    for (const inline of KINDS) {
      const disk = new KeyedTokenGroup('disk');
      const { scheduler } = this.setup(inline, { disk }, { groups: ['disk'] });
      const r = await this.succeed(scheduler, this.task('k', { keys: ['C:', 'D:'] }));
      const grants = r.task.disk;
      if (!grants || Object.keys(grants).sort().join(',') !== 'C:,D:' || !String(grants['C:']).startsWith('C:#')) {
        throw new Error(`${kindOf(inline)}: a two-key task saw ${JSON.stringify(r.task)}`);
      }
      const one = await this.succeed(scheduler, this.task('k1', { keys: 'C:' }));
      if (Object.keys(one.task.disk ?? {}).join(',') !== 'C:') throw new Error(`${kindOf(inline)}: a one-key task saw ${JSON.stringify(one.task)}`);
    }
  }

  private async testUncloneable(): Promise<void> {
    for (const inline of KINDS) {
      const bad = new TokenGroup('bad', 4, (n) => ({ n, fn: () => n }));
      const { scheduler } = this.setup(inline, { bad }, { groups: ['bad'] });
      const outcome = await this.settle(scheduler, this.task('u'));
      if (outcome.ok) throw new Error(`${kindOf(inline)}: a task with an uncloneable grant succeeded`);
      if (!String(outcome.value?.message).includes('"bad"')) {
        throw new Error(`${kindOf(inline)}: the error does not name the group: ${outcome.value?.message}`);
      }
      if (bad.running !== 0 || bad.returnedSame !== 1) {
        throw new Error(`${kindOf(inline)}: running ${bad.running}, onFinish with the original ${bad.returnedSame}`);
      }
    }

    // Resident: the worker is never created, the task fails, the units are back
    for (const inline of KINDS) {
      const bad = new TokenGroup('bad', 4, (n) => ({ n, fn: () => n }));
      const { pool, scheduler } = this.setup(inline, { bad }, { maxConcurrentTasks: 1, residentGroups: ['bad'] });
      const outcome = await this.settle(scheduler, this.task('ur'));
      if (outcome.ok || !String(outcome.value?.message).includes('"bad"')) {
        throw new Error(`${kindOf(inline)}: resident: ${outcome.ok ? 'succeeded' : outcome.value?.message}`);
      }
      if (bad.running !== 0 || pool.getWorkerIds().length !== 0) {
        throw new Error(`${kindOf(inline)}: resident: running ${bad.running}, workers ${pool.getWorkerIds().length}`);
      }
    }
  }

  /** Threaded only: an inline run cannot be stopped from outside. */
  private async testForcedStop(): Promise<void> {
    const gpu = new UnitGroup(['a'], { id: 'gpu' });
    const { pool, scheduler } = this.setup(false, { gpu }, {
      groups: ['gpu'], maxConcurrentTasks: 1, cancelGraceMs: 50
    }, this.cancelWorkerUrl);
    let workerAtRelease: string[] | undefined;
    let worker: string | undefined;
    scheduler.addEventListener('task.progress', () => { worker ??= pool.getWorkerIds()[0]; });
    scheduler.addEventListener('resource.released', (e: any) => {
      if (e.detail.holderId === 'spin') workerAtRelease = pool.getWorkerIds();
    });
    const task = this.task('spin', { mode: 'spin', delay: 1500 });
    const settled = this.settle(scheduler, task);
    const deadline = Date.now() + 5000;
    while (!worker && Date.now() < deadline) await sleep(10);
    if (!worker) throw new Error('the spinning task never started');
    if (!gpu.isBusy('a')) throw new Error('precondition: the unit is not held');
    void scheduler.cancel('spin');
    await sleep(20);
    if (!gpu.isBusy('a')) throw new Error('the unit was freed while the thread still ran');
    await settled;
    if (task.state !== 'cancelled' || !task.cancellation?.forced) throw new Error(`precondition: not a forced stop (${task.state})`);
    if (!workerAtRelease || workerAtRelease.includes(worker)) {
      throw new Error(`the unit was released with the thread still in the pool: ${JSON.stringify(workerAtRelease)}`);
    }
    if (gpu.isBusy('a')) throw new Error('the unit was never freed');
  }

  private async testValidation(): Promise<void> {
    const throws = (what: string, fn: () => unknown, includes: string) => {
      try { fn(); } catch (e: any) {
        if (!String(e?.message).includes(includes)) throw new Error(`${what}: threw "${e?.message}", expected "${includes}"`);
        return;
      }
      throw new Error(`${what}: did not throw`);
    };
    throws('no units', () => new UnitGroup([]), 'at least one unit');
    throws('zero units', () => new UnitGroup(0), 'positive integer');
    throws('a duplicate', () => new UnitGroup(['a', 'a']), 'listed twice');
    if (!same(new UnitGroup(3).units, [0, 1, 2])) throw new Error('UnitGroup(3) is not units 0, 1, 2');

    const g = new UnitGroup(['a', 'b'], { id: 'g' });
    const grant = g.onStart(undefined, 2);
    if (!same(grant, ['a', 'b'])) throw new Error(`a cost of 2 granted ${JSON.stringify(grant)}`);
    throws('over capacity', () => g.onStart(), 'scheduler bug');
    g.onFinish(undefined, 2, grant);
    throws('a double release', () => g.onFinish(undefined, 2, grant), 'had not granted');
    throws('an unknown unit', () => g.onFinish(undefined, 1, ['z']), 'had not granted');
    throws('no grant', () => g.onFinish(undefined, 1), 'without the grant');

    // A resident cost above the number of units can never be granted
    const pool = new WorkerManager(this.workerUrl, { inline: true, residentGroups: { gpu: 3 } });
    this.pools.push(pool);
    throws('a resident cost above the units', () => new FyflowScheduler({ G: pool }, { gpu: new UnitGroup(2, { id: 'gpu' }) }), 'exceeds the group limit');
  }
}

// Auto-run tests when executed directly - handle both Deno and Node.js
if ((typeof Deno !== 'undefined' && import.meta.main) ||
    (typeof process !== 'undefined' && process.argv[1] && import.meta.url &&
     import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/')))) {
  const testSuite = new GrantsTestSuite();
  try {
    await testSuite.runAllTests(true);
  } finally {
    await testSuite.cleanup();
  }
}

export { GrantsTestSuite };
export default GrantsTestSuite;
