// Grants: what a resource group's onStart returns, carried to the work.
// UnitGroup is the group that grants - named units - and a custom group can
// return its own.
//
// Each test runs on both pool types unless it says otherwise. The threaded
// runs are what exercise workerWrapper.ts under Deno and workerWrapper.node.ts
// under Node: a grant crosses the thread boundary twice, with the task's run
// message and, for a resident worker, with its init.

import { WorkerManager, FyflowScheduler, FyflowTask, UnitGroup, ConcurrentLimitGroup } from '../../index.ts';
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
  readonly grants = true;
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
  readonly grants = true;
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
    this.results.push(await this.runTest('A Group That Does Not Opt In Grants Nothing, Whatever onStart Returns', () => this.testNoOptIn()));
    this.results.push(await this.runTest('A Worker Created From A task.running Listener Keeps Its Own Units', () => this.testResidentReentry()));
    this.results.push(await this.runTest('A Keyed Group\'s Grants Are Per Key', () => this.testKeyedShape()));
    this.results.push(await this.runTest('A Grant That Cannot Be Copied Fails The Task And Frees The Units', () => this.testUncloneable()));
    this.results.push(await this.runTest('A Forced Stop Frees The Unit Only Once The Thread Is Dead', () => this.testForcedStop()));
    this.results.push(await this.runTest('UnitGroup Validates, And Refuses A Double Release', () => this.testValidation()));
    // Requests for specific units. Wake-ups run with the heartbeat off: with it
    // on, the periodic retry rescues a missed wake and the bug shows up only as
    // latency
    this.results.push(await this.runTest('Requests: A Pinned Waiter Does Not Hold Up Any-Unit Tasks', () => this.testPinnedDoesNotBlockAny()));
    this.results.push(await this.runTest('Requests: A Freed Unit Goes To The Oldest Waiter That Can Use It', () => this.testOldestWaiterWins()));
    this.results.push(await this.runTest('Requests: from Is A Candidate Set, prefer Never Blocks', () => this.testFromAndPrefer()));
    this.results.push(await this.runTest('Requests: Waiters Carry Their Request, In Events And The Admission Queue', () => this.testRequestVisible()));
    this.results.push(await this.runTest('Requests: Waiters Name Their Queue, Listed In Turn Order', () => this.testWaitSetVisible()));
    this.results.push(await this.runTest('Requests: A Cost-Only Pool Entry Is Not A Request', () => this.testCostOnlyEntry()));
    this.results.push(await this.runTest('Requests: A Resident Pool Takes Its Units From Its Set', () => this.testResidentFrom()));
    this.results.push(await this.runTest('Requests: A Pool Pinned To A Busy Unit Does Not Block One Wanting A Free One', () => this.testResidentDisjointSets()));
    this.results.push(await this.runTest('Requests: A Pool With More Threads Than Cards Runs On The Cards It Has', () => this.testMoreThreadsThanCards()));
    this.results.push(await this.runTest('Requests: One Release Admits One Waiter Into A Full Pool', () => this.testSweepParksOne()));
    this.results.push(await this.runTest('Requests: Bad Requests Throw Up Front', () => this.testRequestValidation()));

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

  private setup(inline: boolean, groups: Record<string, any>, options: any = {}, url = this.workerUrl, schedulerOptions: any = {}) {
    const pool = new WorkerManager(url, {
      maxThreads: 1, maxConcurrentTasks: 4, inline, idleTimeout: 0, ...options
    });
    this.pools.push(pool);
    const scheduler = new FyflowScheduler({ G: pool }, groups, schedulerOptions);
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
  // --- requests --------------------------------------------------------------

  private static readonly NO_HEARTBEAT = { periodicRetryIntervalMs: 1_000_000_000 };

  /** An inline pool already warm, so tasks wait on the group and never for a worker. */
  private async unitScheduler(units: string[]) {
    const gpu = new UnitGroup(units, { id: 'gpu' });
    const { pool, scheduler } = this.setup(true, { gpu }, { groups: ['gpu'], maxConcurrentTasks: 32 },
      this.workerUrl, GrantsTestSuite.NO_HEARTBEAT);
    await this.succeed(scheduler, this.task('warm'));
    const acquired: Array<{ id: string; unit: string }> = [];
    scheduler.addEventListener('resource.acquired', (e: any) => acquired.push({ id: e.detail.holderId, unit: e.detail.grant?.[0] }));
    return { gpu, pool, scheduler, acquired };
  }

  private pinned(id: string, units: string[] | undefined, payload: any = {}, prefer?: string[]) {
    const request: any = {};
    if (units) request.from = units;
    if (prefer) request.prefer = prefer;
    return this.task(id, payload, Object.keys(request).length ? { units: { gpu: request } } : {});
  }

  private async testPinnedDoesNotBlockAny(): Promise<void> {
    const { scheduler, acquired } = await this.unitScheduler(['g0', 'g1']);
    const moves: string[] = [];
    scheduler.addEventListener('resource.blocked', (e: any) => { if (e.detail.holderId === 'p2') moves.push('blocked'); });
    scheduler.addEventListener('resource.unblocked', (e: any) => { if (e.detail.holderId === 'p2') moves.push(e.detail.reason); });
    const p1 = this.succeed(scheduler, this.pinned('p1', ['g1'], { holdMs: 200 }));
    const p2 = this.succeed(scheduler, this.pinned('p2', ['g1']));            // waits: g1 is out
    const anys = [1, 2, 3, 4].map(i => this.succeed(scheduler, this.pinned(`a${i}`, undefined, { holdMs: 15 })));
    await Promise.all(anys);
    // Each "any" task ran on g0 in turn, while p2 still waited for g1
    if (acquired.some(a => a.id === 'p2')) throw new Error('p2 started before p1 released g1');
    const anyUnits = acquired.filter(a => a.id.startsWith('a')).map(a => a.unit);
    if (anyUnits.length !== 4 || anyUnits.some(u => u !== 'g0')) throw new Error(`any-unit tasks ran on ${anyUnits}`);
    const [, r2] = await Promise.all([p1, p2]);
    if (!same(r2.task.gpu, ['g1'])) throw new Error(`p2 was pinned to g1 and got ${JSON.stringify(r2.task.gpu)}`);
    // Never looked at while g0 came and went: one wait, one admission. A wake
    // that ignored its request admitted it on every g0 release, only for it
    // to bounce back to the tail - losing its place each time
    if (moves.join(',') !== 'blocked,admitted') throw new Error(`p2 moved ${moves.join(',')}`);
  }

  private async testOldestWaiterWins(): Promise<void> {
    // The pinned waiter arrives first: when g1 frees, it gets it, not the
    // younger "any" waiter that could use it just as well
    {
      const { scheduler, acquired } = await this.unitScheduler(['g0', 'g1']);
      const runs = [
        this.succeed(scheduler, this.pinned('h0', ['g0'], { holdMs: 200 })),
        this.succeed(scheduler, this.pinned('h1', ['g1'], { holdMs: 60 })),
        this.succeed(scheduler, this.pinned('pin', ['g1'], { holdMs: 10 })),
        this.succeed(scheduler, this.pinned('any', undefined, { holdMs: 10 }))
      ];
      await Promise.all(runs);
      const order = acquired.filter(a => a.id === 'pin' || a.id === 'any').map(a => `${a.id}:${a.unit}`).join(',');
      if (order !== 'pin:g1,any:g1') throw new Error(`pinned first: ${order}, expected pin:g1,any:g1`);
    }
    // The other way round: the older "any" waiter takes g1, and the pinned one
    // gets it next, before h0 frees g0
    {
      const { scheduler, acquired } = await this.unitScheduler(['g0', 'g1']);
      const runs = [
        this.succeed(scheduler, this.pinned('h0', ['g0'], { holdMs: 200 })),
        this.succeed(scheduler, this.pinned('h1', ['g1'], { holdMs: 60 })),
        this.succeed(scheduler, this.pinned('any', undefined, { holdMs: 30 })),
        this.succeed(scheduler, this.pinned('pin', ['g1'], { holdMs: 10 }))
      ];
      await Promise.all(runs);
      const order = acquired.filter(a => a.id !== 'warm').map(a => `${a.id}:${a.unit}`).join(',');
      if (order !== 'h0:g0,h1:g1,any:g1,pin:g1') throw new Error(`any first: ${order}`);
    }
  }

  private async testFromAndPrefer(): Promise<void> {
    const { scheduler } = await this.unitScheduler(['g0', 'g1', 'g2']);
    const preferred = await this.succeed(scheduler, this.pinned('pref', undefined, {}, ['g2']));
    if (!same(preferred.task.gpu, ['g2'])) throw new Error(`prefer g2, free: got ${JSON.stringify(preferred.task.gpu)}`);

    // g2 held: prefer is ignored, not waited for
    const holder = this.succeed(scheduler, this.pinned('hold', ['g2'], { holdMs: 80 }));
    const t0 = Date.now();
    const fallback = await this.succeed(scheduler, this.pinned('pref2', undefined, {}, ['g2']));
    if (!same(fallback.task.gpu, ['g0']) || Date.now() - t0 > 60) {
      throw new Error(`prefer g2, busy: got ${JSON.stringify(fallback.task.gpu)} after ${Date.now() - t0} ms`);
    }
    // from: one of a set - g2 is busy, so g1
    const either = await this.succeed(scheduler, this.pinned('either', ['g1', 'g2']));
    if (!same(either.task.gpu, ['g1'])) throw new Error(`from g1/g2 with g2 busy: got ${JSON.stringify(either.task.gpu)}`);
    // prefer inside from
    const both = await this.succeed(scheduler, this.pinned('both', ['g0', 'g1'], {}, ['g1']));
    if (!same(both.task.gpu, ['g1'])) throw new Error(`from g0/g1 prefer g1: got ${JSON.stringify(both.task.gpu)}`);
    await holder;
  }

  private async testRequestVisible(): Promise<void> {
    const { scheduler } = await this.unitScheduler(['g0', 'g1']);
    const blocked: any[] = [];
    scheduler.addEventListener('resource.blocked', (e: any) => blocked.push(e.detail));
    const runs = [
      this.succeed(scheduler, this.pinned('h', ['g1'], { holdMs: 60 })),
      this.succeed(scheduler, this.pinned('w', ['g1'])),
      this.succeed(scheduler, this.pinned('h2', ['g0'], { holdMs: 60 })),
      this.succeed(scheduler, this.pinned('w2', undefined))
    ];
    const queue = scheduler.getAdmissionQueue('gpu').gpu ?? [];
    const w = queue.find(e => e.holderId === 'w');
    const w2 = queue.find(e => e.holderId === 'w2');
    if (!w || !same(w.request, { from: ['g1'] }) || w.key !== undefined || w.position !== 0) {
      throw new Error(`the pinned waiter is listed as ${JSON.stringify(w)}`);
    }
    if (!w2 || w2.request !== undefined || w2.position !== 0) throw new Error(`the any waiter is listed as ${JSON.stringify(w2)}`);
    const event = blocked.find(d => d.holderId === 'w');
    if (!event || !same(event.request, { from: ['g1'] }) || event.key !== undefined) {
      throw new Error(`resource.blocked for the pinned waiter: ${JSON.stringify(event)}`);
    }
    await Promise.all(runs);
  }

  /**
   * A group with wait sets queues its waiters per set, each numbered from 0.
   * `waitSet` names the queue - absent for the group's plain queue - so a
   * position reads as "within (key, waitSet)". The sets are listed in turn
   * order, by when their head arrived, which is the order a release serves
   * them in. They used to be listed by the set's internal id, with nothing
   * telling the queues apart.
   */
  private async testWaitSetVisible(): Promise<void> {
    const { scheduler } = await this.unitScheduler(['g0', 'g1']);
    const blocked = new Map<string, any>();
    scheduler.addEventListener('resource.blocked', (e: any) => blocked.set(e.detail.holderId, e.detail));
    const runs = [
      this.succeed(scheduler, this.pinned('h0', ['g0'], { holdMs: 60 })),
      this.succeed(scheduler, this.pinned('h1', ['g1'], { holdMs: 60 })),
      // Arrival order: the g1 queue's head first, then two in the plain
      // queue - "any", and a prefer, which never blocks - then g0's
      this.succeed(scheduler, this.pinned('p1', ['g1'])),
      this.succeed(scheduler, this.pinned('a1', undefined)),
      this.succeed(scheduler, this.pinned('a2', undefined, {}, ['g1'])),
      this.succeed(scheduler, this.pinned('p0', ['g0']))
    ];
    const queue = scheduler.getAdmissionQueue('gpu').gpu ?? [];
    const listed = queue.map(w => `${w.holderId}@${w.position}`).join(',');
    if (listed !== 'p1@0,a1@0,a2@1,p0@0') throw new Error(`listed as ${listed}`);
    const set = (id: string) => queue.find(w => w.holderId === id)?.waitSet;
    if (set('p1') === undefined || set('p0') === undefined || set('p1') === set('p0')) {
      throw new Error(`pinned waiters' sets: p1 ${set('p1')}, p0 ${set('p0')}`);
    }
    if (set('a1') !== undefined || set('a2') !== undefined) throw new Error(`plain queue named: ${set('a1')}, ${set('a2')}`);
    // The events name the same queues, so a fold can number them the same way
    for (const w of queue) {
      if (blocked.get(w.holderId)?.waitSet !== w.waitSet) {
        throw new Error(`${w.holderId}: resource.blocked says ${blocked.get(w.holderId)?.waitSet}, the snapshot ${w.waitSet}`);
      }
    }
    await Promise.all(runs);
  }

  /**
   * `{ cost: 2 }` in `residentGroups` is `2` written as an object, and works
   * for a group that takes no requests. The empty rest used to be stored as
   * a request, so it threw "has a request, but group takes none".
   */
  private async testCostOnlyEntry(): Promise<void> {
    const vram = new ConcurrentLimitGroup(4, 'vram');
    const { pool, scheduler } = this.setup(true, { vram }, { residentGroups: { vram: { cost: 2 } } });
    await this.succeed(scheduler, this.task('t'));
    if (pool.getResidentUsage().vram !== 2) throw new Error(`usage ${JSON.stringify(pool.getResidentUsage())}`);
  }

  private async testResidentFrom(): Promise<void> {
    for (const inline of KINDS) {
      // Cost 2 from three, one of which another pool holds: the two others
      const gpu = new UnitGroup(['g0', 'g1', 'g2'], { id: 'gpu' });
      const other = new WorkerManager(this.workerUrl, { inline, maxThreads: 1, maxConcurrentTasks: 1, idleTimeout: 0, residentGroups: { gpu: { from: ['g1'] } } });
      const pinned = new WorkerManager(this.workerUrl, { inline, maxThreads: 1, maxConcurrentTasks: 1, idleTimeout: 0, residentGroups: { gpu: { cost: 2, from: ['g0', 'g1', 'g2'] } } });
      this.pools.push(other, pinned);
      const scheduler = new FyflowScheduler({ O: other, P: pinned }, { gpu });
      this.schedulers.push(scheduler);
      const o = await this.succeed(scheduler, new FyflowTask({ id: 'o', workerType: 'O', payload: {} }));
      const p = await this.succeed(scheduler, new FyflowTask({ id: 'p', workerType: 'P', payload: {} }));
      if (!same(o.ctor.gpu, ['g1'])) throw new Error(`${kindOf(inline)}: the g1-pinned pool got ${JSON.stringify(o.ctor.gpu)}`);
      if (!same(p.ctor.gpu, ['g0', 'g2'])) throw new Error(`${kindOf(inline)}: cost 2 from g0/g1/g2, g1 held: got ${JSON.stringify(p.ctor.gpu)}`);
    }
  }

  private async testResidentDisjointSets(): Promise<void> {
    const gpu = new UnitGroup(['g0', 'g1'], { id: 'gpu' });
    // A wants a second worker on g0, which its first holds; B wants g1, free
    const a = new WorkerManager(this.workerUrl, { inline: true, maxThreads: 2, maxConcurrentTasks: 1, idleTimeout: 0, residentGroups: { gpu: { from: ['g0'] } } });
    const b = new WorkerManager(this.workerUrl, { inline: true, maxThreads: 1, maxConcurrentTasks: 1, idleTimeout: 0, residentGroups: { gpu: { from: ['g1'] } } });
    this.pools.push(a, b);
    const scheduler = new FyflowScheduler({ A: a, B: b }, { gpu }, GrantsTestSuite.NO_HEARTBEAT);
    this.schedulers.push(scheduler);

    const a1 = this.succeed(scheduler, new FyflowTask({ id: 'a1', workerType: 'A', payload: { holdMs: 150 } }));
    // Waits for a worker on g0 - and runs on A's first once a1 is done
    const a2 = this.succeed(scheduler, new FyflowTask({ id: 'a2', workerType: 'A', payload: {} }));
    const waiting = scheduler.getAdmissionQueue('gpu').gpu ?? [];
    if (!waiting.some(w => w.holderId === 'a2' && w.lifetime === 'resident')) {
      throw new Error(`precondition: a2 should wait for a worker on g0, queue ${JSON.stringify(waiting)}`);
    }
    // B's set does not overlap A's waiter's, so it is not behind it
    const t0 = Date.now();
    const b1 = await this.succeed(scheduler, new FyflowTask({ id: 'b1', workerType: 'B', payload: {} }));
    if (Date.now() - t0 > 80 || !same(b1.ctor.gpu, ['g1'])) {
      throw new Error(`B waited ${Date.now() - t0} ms behind A's waiter, and got ${JSON.stringify(b1.ctor?.gpu)}`);
    }
    await a1;
    const r2 = await a2;
    if (!same(r2.ctor.gpu, ['g0'])) throw new Error(`a2 ran on a worker holding ${JSON.stringify(r2.ctor?.gpu)}`);
  }

  /**
   * One unit freed, and the pool its waiters run on is full: one waiter is
   * admitted, to wait in the ready queue for a slot, and the rest keep their
   * places. A parked waiter holds nothing, so it counts against what the
   * release freed - across every round of the sweep. Each round used to start
   * that count at 0, so one release moved every waiter into the ready queue.
   */
  private async testSweepParksOne(): Promise<void> {
    const gpu = new UnitGroup(['g0', 'g1'], { id: 'gpu' });
    const options = { inline: true, maxThreads: 2, maxConcurrentTasks: 1, idleTimeout: 0 };
    const holders = new WorkerManager(this.workerUrl, options);
    const full = new WorkerManager(this.workerUrl, options);
    this.pools.push(holders, full);
    const scheduler = new FyflowScheduler({ H: holders, F: full }, { gpu }, GrantsTestSuite.NO_HEARTBEAT);
    this.schedulers.push(scheduler);
    const onGpu = { workerGroups: ['gpu'] };
    const h1 = this.succeed(scheduler, new FyflowTask({ id: 'h1', workerType: 'H', payload: { holdMs: 40 }, ...onGpu }));
    const h2 = this.succeed(scheduler, new FyflowTask({ id: 'h2', workerType: 'H', payload: { holdMs: 300 }, ...onGpu }));
    const waiters = [0, 1, 2, 3].map(i =>
      this.succeed(scheduler, new FyflowTask({ id: `w${i}`, workerType: 'F', payload: {}, ...onGpu })));
    // F's slots fill with work that needs no unit
    const busy = [0, 1].map(i =>
      this.succeed(scheduler, new FyflowTask({ id: `b${i}`, workerType: 'F', payload: { holdMs: 150 } })));
    const queued = () => (scheduler.getAdmissionQueue('gpu').gpu ?? []).map(w => w.holderId).join(',');
    if (queued() !== 'w0,w1,w2,w3') throw new Error(`precondition: the queue is "${queued()}"`);
    await h1;
    await sleep(5);
    const after = queued();
    await Promise.all([h2, ...waiters, ...busy]);
    if (after !== 'w1,w2,w3') throw new Error(`one release left the queue "${after}", expected "w1,w2,w3"`);
  }

  /**
   * A pool allowed three workers but only two cards: two workers, on the two
   * cards, and every task runs on one of them. The third worker can never be
   * created, so the tasks waiting for it take the first free slot instead.
   */
  private async testMoreThreadsThanCards(): Promise<void> {
    for (const inline of [true, false]) {
      const gpu = new UnitGroup(['g0', 'g1', 'g2'], { id: 'gpu' });
      const pool = new WorkerManager(this.workerUrl, {
        inline, maxThreads: 3, maxConcurrentTasks: 1, idleTimeout: 0, residentGroups: { gpu: { from: ['g0', 'g1'] } }
      });
      this.pools.push(pool);
      const scheduler = new FyflowScheduler({ G: pool }, { gpu }, GrantsTestSuite.NO_HEARTBEAT);
      this.schedulers.push(scheduler);
      const results = await Promise.all(Array.from({ length: 8 }, (_, i) =>
        this.succeed(scheduler, new FyflowTask({ id: `t${i}`, workerType: 'G', payload: { holdMs: 20 } }))));
      const cards = new Set(results.map(r => JSON.stringify(r.ctor.gpu)));
      if (cards.size !== 2 || !cards.has('["g0"]') || !cards.has('["g1"]')) {
        throw new Error(`${kindOf(inline)}: workers held ${[...cards].join(' ')}`);
      }
      if (pool.threads.length !== 2) throw new Error(`${kindOf(inline)}: ${pool.threads.length} workers`);
      if (gpu.isBusy('g2')) throw new Error(`${kindOf(inline)}: g2 was taken`);
      await scheduler.shutdown();
    }
  }

  /**
   * A group that does not set `grants: true` grants nothing, whatever its
   * onStart returns - an `async onStart` returns a Promise, which cannot be
   * copied. It used to be taken for a grant, so every task through such a
   * group failed, and a resident one created no worker at all.
   */
  private async testNoOptIn(): Promise<void> {
    for (const inline of KINDS) {
      const returned: unknown[] = [];
      const legacy = (id: string) => ({
        id, type: 'legacy', running: 0, finishedWith: [] as unknown[],
        canRun() { return true; },
        async onStart() { /* written before grants: returns a Promise */ },
        onFinish(_k?: string, _c?: number, grant?: unknown) { returned.push(grant); },
        getMetrics() { return { limit: 1, running: 0, available: 1, utilization: 0 }; },
        addEventListener() {}, removeEventListener() {}, dispatchEvent() { return true; }
      });
      const { scheduler } = this.setup(inline, { legacy: legacy('legacy'), resident: legacy('resident') }, {
        groups: ['legacy'], residentGroups: { resident: 1 }
      });
      const r = await this.succeed(scheduler, this.task('t'));
      if (!same(r.task, {}) || !same(r.ctor, {})) {
        throw new Error(`${kindOf(inline)}: granted ${JSON.stringify(r.task)}, resident ${JSON.stringify(r.ctor)}`);
      }
      await scheduler.shutdown();
      // onFinish gets what was granted - nothing - as before grants existed
      if (returned.length !== 2 || returned.some(g => g !== undefined)) {
        throw new Error(`${kindOf(inline)}: onFinish got ${returned.map(g => String(g)).join(', ')}`);
      }
    }
  }

  /**
   * An inline worker announces its first task synchronously, before its
   * resident units are bound to it, so a listener adding a task from
   * `task.running` creates a second worker in between. Each keeps its own
   * card. Binding the oldest pending holding instead swapped them: each worker
   * was filed under the other's card, and freed it at teardown.
   */
  private async testResidentReentry(): Promise<void> {
    const gpu = new UnitGroup(['g0', 'g1'], { id: 'gpu' });
    const { pool, scheduler } = this.setup(true, { gpu }, {
      maxThreads: 2, maxConcurrentTasks: 1, residentGroups: { gpu: 1 }
    });
    let second: Promise<any> | undefined;
    scheduler.addEventListener('task.running', (e: any) => {
      if (e.detail.id === 'first' && !second) second = this.succeed(scheduler, this.task('second', { holdMs: 30 }));
    });
    const first = await this.succeed(scheduler, this.task('first', { holdMs: 30 }));
    if (!second) throw new Error('precondition: task.running never fired for first');
    const r2 = await second;
    if (first.workerId === r2.workerId || same(first.ctor.gpu, r2.ctor.gpu)) {
      throw new Error(`precondition: one worker per card, got ${JSON.stringify([first.ctor, r2.ctor])}`);
    }
    for (const r of [first, r2]) {
      const filed = pool.getResidentGrants(r.workerId)?.gpu;
      if (!same(filed, r.ctor.gpu)) {
        throw new Error(`worker ${r.workerId} runs on ${JSON.stringify(r.ctor.gpu)} but is filed under ${JSON.stringify(filed)}`);
      }
    }
  }

  private async testRequestValidation(): Promise<void> {
    const throws = (what: string, fn: () => unknown, includes: string) => {
      try { fn(); } catch (e: any) {
        if (!String(e?.message).includes(includes)) throw new Error(`${what}: threw "${e?.message}", expected "${includes}"`);
        return;
      }
      throw new Error(`${what}: did not throw`);
    };
    const { scheduler } = this.setup(true, { gpu: new UnitGroup(['g0', 'g1'], { id: 'gpu' }) }, { groups: ['gpu'] });
    throws('an unknown unit at addTask', () => scheduler.addTask(this.pinned('bad', ['g9'])), 'has no unit "g9"');
    throws('an unknown preferred unit', () => scheduler.addTask(this.pinned('bad2', undefined, {}, ['g9'])), 'has no unit "g9"');
    throws('a request that is not an object', () => scheduler.addTask(this.task('bad3', {}, { units: { gpu: 'g0' } })), 'must be an object');
    if (scheduler.stats.queued !== 0) throw new Error('a rejected task was queued');

    const make = (residentGroups: any, groups: any) => () => {
      const pool = new WorkerManager(this.workerUrl, { inline: true, residentGroups });
      this.pools.push(pool);
      return new FyflowScheduler({ G: pool }, groups);
    };
    throws('a resident unit that does not exist', make({ gpu: { from: ['g9'] } }, { gpu: new UnitGroup(['g0'], { id: 'gpu' }) }), 'has no unit "g9"');
    throws('a resident cost above its set', make({ gpu: { cost: 2, from: ['g0'] } }, { gpu: new UnitGroup(['g0', 'g1'], { id: 'gpu' }) }), 'can never be granted');
    throws('a request to a group that takes none', make({ vram: { from: ['x'] } }, { vram: { id: 'vram', type: 'x', canRun: () => true, getMetrics: () => ({ limit: 4 }) } }), 'takes none');
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
