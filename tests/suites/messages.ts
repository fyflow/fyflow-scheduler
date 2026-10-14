// Worker -> host messages: context.emit() -> task.message on the scheduler,
// workerContext.emit() -> worker.message on the pool.
//
// Each test runs on both pool types unless it says otherwise. The threaded runs
// are what exercise workerWrapper.ts under Deno and workerWrapper.node.ts under
// Node; inline runs the same channel in-process. Requeued runs are covered in
// the settlement suite, next to the harness that produces them.

import { WorkerManager, FyflowScheduler, FyflowTask } from '../../index.ts';

// Node.js process declaration for cross-platform compatibility
declare const process: any;

interface TestResult { name: string; passed: boolean; duration: number; error?: string; }
interface TestSuiteResult {
  platform: string; totalTests: number; passed: number; failed: number;
  duration: number; results: TestResult[];
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const KINDS = [true, false] as const;
const kindOf = (inline: boolean) => inline ? 'inline' : 'threaded';

class MessagesTestSuite {
  private results: TestResult[] = [];
  private startTime = 0;
  private workerUrl = "";
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
    console.log(`\n🧪 Running: ${name}`);
    const start = performance.now();
    try {
      await testFn();
      const duration = performance.now() - start;
      console.log(`✅ PASSED: ${name} (${duration.toFixed(1)}ms)`);
      return { name, passed: true, duration };
    } catch (error: any) {
      const duration = performance.now() - start;
      console.log(`❌ FAILED: ${name} (${duration.toFixed(1)}ms) - ${error.message}`);
      return { name, passed: false, duration, error: error.message };
    }
  }

  async runAllTests(exitOnComplete = true): Promise<TestSuiteResult> {
    if (typeof Deno !== "undefined") {
      this.workerUrl = new URL("../workers/messageWorker.ts", import.meta.url).href;
    } else {
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      this.workerUrl = new URL((await import('../workers/messageWorker.ts?worker-direct')).default).href;
    }

    const platform = typeof globalThis !== 'undefined' && 'Deno' in globalThis ? 'Deno' : 'Node.js';
    console.log(`🚀 FyFlow Worker Message Tests - ${platform}`);
    console.log('='.repeat(60));

    this.startTime = performance.now();
    this.results = [];

    this.results.push(await this.runTest('Task Messages Arrive In Order', () => this.testOrder()));
    this.results.push(await this.runTest('Task Messages Precede Their Settle', () => this.testBeforeSettlement()));
    this.results.push(await this.runTest('Worker Messages Precede Lifecycle Events', () => this.testBeforeLifecycle()));
    this.results.push(await this.runTest('Nothing After Settle Or Teardown', () => this.testNothingAfter()));
    this.results.push(await this.runTest('A Stray Emit Does Not Reach The Next Attempt', () => this.testStrayIntoNextAttempt()));
    this.results.push(await this.runTest('Inline Delivery Never Runs Inside emit()', () => this.testInlineDeliveryIsDeferred()));
    this.results.push(await this.runTest('Worker-Level emit() Goes To The Pool', () => this.testWorkerLevelFromRun()));
    this.results.push(await this.runTest('Self-Termination Delivers What Came Before', () => this.testTermination()));
    this.results.push(await this.runTest('A Loop That Never Yields Is Flushed By Time', () => this.testSyncLoopFlushedByTime()));
    this.results.push(await this.runTest('The Cap Accounts For Every Emit', () => this.testCapAccounting()));
    this.results.push(await this.runTest('Nothing Is Sent While Nobody Listens', () => this.testGateClosed()));
    this.results.push(await this.runTest('Subscribing Mid-Run Opens The Gate', () => this.testGateOpensMidRun()));
    this.results.push(await this.runTest('Gate Bookkeeping', () => this.testGateBookkeeping()));
    this.results.push(await this.runTest('task.message Fires On The Scheduler Only', () => this.testNoDoubleDelivery()));
    this.results.push(await this.runTest('Data Is Cloned At emit()', () => this.testCloning()));
    this.results.push(await this.runTest('A Retry Restarts seq', () => this.testRetry()));
    this.results.push(await this.runTest('Inline And Threaded Deliver The Same', () => this.testParity()));
    this.results.push(await this.runTest('Shutdown During A Stream Does Not Throw', () => this.testShutdownMidStream()));
    this.results.push(await this.runTest('messages Option Is Validated', () => this.testOptionValidation()));

    const totalDuration = performance.now() - this.startTime;
    const passed = this.results.filter(r => r.passed).length;
    const failed = this.results.length - passed;

    console.log(`\n📊 Worker Message Test Summary - ${platform}`);
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
      console.log('\n🎉 All worker message tests passed!');
    }

    return { platform, totalTests: this.results.length, passed, failed, duration: totalDuration, results: this.results };
  }

  // --- helpers ---------------------------------------------------------------

  private pool(inline: boolean, options: any = {}) {
    const pool = new WorkerManager(this.workerUrl, {
      maxThreads: 1, maxConcurrentTasks: 1, inline, idleTimeout: 0, ...options
    });
    this.pools.push(pool);
    return pool;
  }

  private scheduler(inline: boolean, options: any = {}) {
    const pool = this.pool(inline, options);
    const scheduler = new FyflowScheduler({ Msg: pool });
    this.schedulers.push(scheduler);
    return { pool, scheduler };
  }

  /** Task messages and settle events for every task, in arrival order. */
  private record(scheduler: FyflowScheduler) {
    const log: any[] = [];
    scheduler.addEventListener('task.message', (e: any) => log.push({ kind: 'message', ...e.detail }));
    for (const ev of ['task.completed', 'task.failed', 'task.retrying']) {
      scheduler.addEventListener(ev, (e: any) => log.push({ kind: ev.slice(5), taskId: e.detail.id, attemptNumber: e.detail.attemptNumber }));
    }
    return log;
  }

  private task(id: string, payload: any, extra: any = {}) {
    return new FyflowTask({ id, workerType: 'Msg', payload, ...extra });
  }

  private async run(scheduler: FyflowScheduler, task: FyflowTask): Promise<string> {
    const outcome = await Promise.race([
      (scheduler.addTask(task, { createPromise: true }) as Promise<any>).then(() => 'resolved', () => 'rejected'),
      sleep(5000).then(() => 'NEVER SETTLED')
    ]);
    if (outcome === 'NEVER SETTLED') throw new Error(`Task ${task.id} never settled`);
    return outcome;
  }

  /** As run(), for a task expected to succeed: a vacuous pass otherwise. */
  private async succeed(scheduler: FyflowScheduler, task: FyflowTask): Promise<void> {
    const outcome = await this.run(scheduler, task);
    if (outcome !== 'resolved') throw new Error(`Precondition: task ${task.id} ${outcome} (${task.error})`);
  }

  // --- tests -----------------------------------------------------------------

  private async testOrder(): Promise<void> {
    for (const inline of KINDS) {
      const { scheduler } = this.scheduler(inline);
      const log = this.record(scheduler);
      await this.succeed(scheduler, this.task('order', { mode: 'emit', count: 50, yieldEvery: 7 }));

      const messages = log.filter(m => m.kind === 'message');
      const seqs = messages.map(m => m.seq).join();
      const expected = Array.from({ length: 50 }, (_, i) => i).join();
      if (seqs !== expected) throw new Error(`${kindOf(inline)}: seq ${seqs}`);
      if (messages.some(m => m.data.i !== m.seq || m.name !== 'log' || m.attemptNumber !== 1 || !m.workerId)) {
        throw new Error(`${kindOf(inline)}: a detail is wrong: ${JSON.stringify(messages[0])}`);
      }
      if ('workerType' in messages[0]) throw new Error(`${kindOf(inline)}: task.message must not carry workerType`);
    }
  }

  // Emitted right before returning or throwing, so everything is still in the
  // worker's buffer when the attempt ends
  private async testBeforeSettlement(): Promise<void> {
    for (const inline of KINDS) {
      for (const throws of [false, true]) {
        const { scheduler } = this.scheduler(inline);
        const log = this.record(scheduler);
        await this.run(scheduler, this.task('settle', { mode: 'emit', count: 30, throws }));

        const end = log.findIndex(m => m.kind === (throws ? 'failed' : 'completed'));
        const before = log.slice(0, end).filter(m => m.kind === 'message').length;
        const after = log.slice(end).filter(m => m.kind === 'message').length;
        if (end === -1 || before !== 30 || after !== 0) {
          throw new Error(`${kindOf(inline)}, throws ${throws}: ${before} before the settle, ${after} after`);
        }
      }
    }
  }

  private async testBeforeLifecycle(): Promise<void> {
    for (const inline of KINDS) {
      for (const setupThrows of [false, true]) {
        const pool = this.pool(inline, { config: { emitInConstructor: true, emitInSetup: true, emitInTeardown: true, setupThrows } });
        const log: string[] = [];
        pool.addEventListener('worker.message', (e: any) => log.push(`message:${e.detail.name}`));
        for (const ev of ['worker.setup.completed', 'worker.setup.failed', 'worker.teardown.completed', 'worker.teardown.failed']) {
          pool.addEventListener(ev, () => log.push(ev));
        }
        await pool.enqueueNoQueue('lifecycle', { mode: 'emit', count: 0 }).catch(() => {});
        await pool.shutdown();
        await sleep(50);

        const setupEnd = setupThrows ? 'worker.setup.failed' : 'worker.setup.completed';
        const expected = ['message:constructed', 'message:loading', setupEnd, 'message:closing', 'worker.teardown.completed'];
        if (log.join() !== expected.join()) {
          throw new Error(`${kindOf(inline)}, setupThrows ${setupThrows}: ${log.join(' > ')}`);
        }
      }
    }
  }

  private async testNothingAfter(): Promise<void> {
    for (const inline of KINDS) {
      const { scheduler } = this.scheduler(inline);
      const log = this.record(scheduler);
      await this.succeed(scheduler, this.task('stray', { mode: 'stray' }));
      await sleep(100);
      const phases = log.filter(m => m.kind === 'message').map(m => m.data.phase);
      if (phases.join() !== 'running') throw new Error(`${kindOf(inline)}: after settle, delivered ${phases.join()}`);

      const pool = this.pool(inline, { config: { emitAfterTeardown: true } });
      const names: string[] = [];
      pool.addEventListener('worker.message', (e: any) => names.push(e.detail.name));
      await pool.enqueueNoQueue('t', { mode: 'emit', count: 0 });
      await pool.shutdown();
      await sleep(100);
      if (names.length > 0) throw new Error(`${kindOf(inline)}: after teardown, delivered ${names.join()}`);
    }
  }

  // The host drops events for a task no longer running on that worker, which
  // covers a stray emit after the task has settled for good. It cannot cover a
  // retry on the same worker: the task id is running there again, so only the
  // first attempt's context being closed keeps its stray emit out of the second.
  private async testStrayIntoNextAttempt(): Promise<void> {
    for (const inline of KINDS) {
      const { scheduler } = this.scheduler(inline);
      const log = this.record(scheduler);
      // The second attempt outlasts the first one's stray timer
      scheduler.addEventListener('task.retrying', (e: any) => {
        e.detail.payload = { mode: 'stream', forMs: 120, everyMs: 10 };
      });
      await this.succeed(scheduler, this.task('stray-retry', { mode: 'stray-throw' }, { retryPolicy: { maxRetries: 1, backoffMs: 5 } }));

      const misattributed = log.filter(m => m.kind === 'message' && m.data.phase === 'stray-from-attempt-1');
      if (misattributed.length > 0) {
        throw new Error(`${kindOf(inline)}: attempt 1's stray emit was delivered as attempt ${misattributed[0].attemptNumber}`);
      }
      if (!log.some(m => m.kind === 'message' && m.attemptNumber === 2)) {
        throw new Error(`${kindOf(inline)}: precondition - the second attempt delivered nothing`);
      }
    }
  }

  // Inline only: a threaded worker cannot run a host listener synchronously.
  // An emit() made after the flush interval has passed is flushed inside the
  // call, so a synchronous delivery would run the listener before it returned.
  private async testInlineDeliveryIsDeferred(): Promise<void> {
    const { scheduler } = this.scheduler(true);
    let returnedFirst: boolean | undefined;
    scheduler.addEventListener('task.message', () => {
      returnedFirst = (globalThis as any).__fyflowEmitReturned;
    });
    await this.succeed(scheduler, this.task('probe', { mode: 'probe' }));
    if (returnedFirst !== true) throw new Error(`The listener ran inside emit() (returnedFirst: ${returnedFirst})`);
  }

  private async testWorkerLevelFromRun(): Promise<void> {
    for (const inline of KINDS) {
      const { pool, scheduler } = this.scheduler(inline);
      const log = this.record(scheduler);
      const workerMessages: any[] = [];
      pool.addEventListener('worker.message', (e: any) => workerMessages.push(e.detail));
      await this.succeed(scheduler, this.task('w', { mode: 'worker' }));
      await sleep(50);

      const task = log.filter(m => m.kind === 'message').map(m => m.name);
      const cache = workerMessages.find(m => m.name === 'cache');
      if (task.join() !== 'log') throw new Error(`${kindOf(inline)}: task.message got ${task.join()}`);
      if (!cache || cache.data.hits !== 12 || cache.workerType !== kindOf(inline).replace('threaded', 'thread') || 'taskId' in cache) {
        throw new Error(`${kindOf(inline)}: worker.message was ${JSON.stringify(workerMessages)}`);
      }
    }
  }

  // terminateWithError() through the task context fails the task. What it
  // emitted before asking belongs to the attempt; what it emits after does not.
  private async testTermination(): Promise<void> {
    for (const inline of KINDS) {
      const { scheduler } = this.scheduler(inline, { maxWorkerRestarts: 0 });
      const log = this.record(scheduler);
      await this.run(scheduler, this.task('term', { mode: 'terminate' }));
      await sleep(100);

      const order = log.map(m => m.kind === 'message' ? m.data.phase : m.kind).join(' > ');
      if (order !== 'before-terminate > failed') throw new Error(`${kindOf(inline)}: ${order}`);
    }
  }

  // Threaded only: a worker thread in a synchronous loop never yields, so its
  // trailing timer never fires. Only the time check inside emit() flushes it.
  private async testSyncLoopFlushedByTime(): Promise<void> {
    const { scheduler } = this.scheduler(false);
    const log = this.record(scheduler);
    const firstArrivals: number[] = [];
    scheduler.addEventListener('task.message', () => firstArrivals.push(performance.now()));
    const start = performance.now();
    let settledAt = 0;
    scheduler.addEventListener('task.completed', () => { settledAt = performance.now(); });
    await this.succeed(scheduler, this.task('sync', { mode: 'sync-for', forMs: 300 }));

    const delivered = log.filter(m => m.kind === 'message' && !m.error).length;
    // More than one buffer's worth, so it flushed during the loop...
    if (delivered <= 100) throw new Error(`Only ${delivered} delivered from a 300 ms loop - not flushed by time`);
    // ...and the first batch arrived long before the run ended
    if (firstArrivals[0] - start > settledAt - start - 100) {
      throw new Error(`The first message arrived at ${(firstArrivals[0] - start).toFixed(0)} ms, the run ended at ${(settledAt - start).toFixed(0)} ms`);
    }
  }

  // delivered + dropped == emits, the seq gaps match `dropped`, and an attempt
  // ending over the cap still reports its trailing count
  private async testCapAccounting(): Promise<void> {
    for (const inline of KINDS) {
      const { scheduler } = this.scheduler(inline);
      const log = this.record(scheduler);
      let emitted = 0;
      scheduler.addEventListener('task.completed', (e: any) => { emitted = e.detail.result.emitted; });
      await this.succeed(scheduler, this.task('cap', { mode: 'sync-for', forMs: 100 }));

      const messages = log.filter(m => m.kind === 'message');
      const accounted = messages.reduce((n, m) => n + 1 + (m.dropped ?? 0), 0);
      if (accounted !== emitted) {
        throw new Error(`${kindOf(inline)}: ${messages.length} delivered + dropped = ${accounted}, but ${emitted} were emitted`);
      }
      let previous = -1;
      for (const m of messages) {
        if (m.seq - previous - 1 !== (m.dropped ?? 0)) {
          throw new Error(`${kindOf(inline)}: seq ${previous} -> ${m.seq} with dropped ${m.dropped}`);
        }
        previous = m.seq;
      }
      const last = messages[messages.length - 1];
      if (messages.every(m => !m.dropped)) throw new Error(`${kindOf(inline)}: precondition - the cap was never reached`);
      if (last.seq !== emitted - 1 && !(last.error && last.data === undefined)) {
        throw new Error(`${kindOf(inline)}: the trailing drops were not reported: last seq ${last.seq} of ${emitted}`);
      }
    }
  }

  // Threaded only: the gate is about what crosses the thread boundary
  private async testGateClosed(): Promise<void> {
    const { pool, scheduler } = this.scheduler(false);
    // Listeners on other events do not open it
    scheduler.addEventListener('task.completed', () => {});
    pool.addEventListener('worker.setup.completed', () => {});
    await this.succeed(scheduler, this.task('quiet', { mode: 'emit', count: 50 }));
    const batches = (pool as any).threads[0].messageBatches;
    if (batches !== 0) throw new Error(`${batches} message batches were posted with nobody listening`);
  }

  private async testGateOpensMidRun(): Promise<void> {
    for (const inline of KINDS) {
      const { scheduler } = this.scheduler(inline);
      const promise = this.run(scheduler, this.task('late', { mode: 'stream', forMs: 400, everyMs: 5 }));
      await sleep(150);
      const messages: any[] = [];
      scheduler.addEventListener('task.message', (e: any) => messages.push(e.detail));
      await promise;

      if (messages.length === 0) throw new Error(`${kindOf(inline)}: nothing arrived after subscribing`);
      if (messages[0].seq === 0) throw new Error(`${kindOf(inline)}: the first seq should show the earlier emits`);
      if (messages[0].dropped) throw new Error(`${kindOf(inline)}: gated emits were reported as dropped`);
    }
  }

  private async testGateBookkeeping(): Promise<void> {
    const { pool, scheduler } = this.scheduler(false);
    let delivered = 0;
    const listener = () => { delivered++; };
    // Read through a call: a literal comparison would narrow the counter
    const count = () => delivered;
    // Subscribed while the thread is still initialising: the gate travels with
    // init, and a separate gate message may race it
    const first = this.succeed(scheduler, this.task('a', { mode: 'emit', count: 5 }));
    scheduler.addEventListener('task.message', listener);
    await first;
    if (count() !== 5) throw new Error(`Subscribed during init: ${count()} of 5`);

    // Removing a listener that was never added does not close the gate
    scheduler.removeEventListener('task.message', () => {});
    await this.succeed(scheduler, this.task('b', { mode: 'emit', count: 5 }));
    if (count() !== 10) throw new Error(`After removing a stranger: ${count() - 5} of 5`);

    // Removing the last real one does
    scheduler.removeEventListener('task.message', listener);
    await sleep(20);
    const before = (pool as any).threads[0].messageBatches;
    await this.succeed(scheduler, this.task('c', { mode: 'emit', count: 5 }));
    const after = (pool as any).threads[0].messageBatches;
    if (after !== before) throw new Error(`${after - before} batches posted after the last listener left`);

    // A gate change after shutdown does not throw anywhere
    await scheduler.shutdown();
    scheduler.addEventListener('task.message', listener);
    pool.addEventListener('worker.message', listener);
  }

  // The scheduler receives task messages through an injected notifier, not a
  // listener, so the pool never dispatches them: no double count, and nothing
  // internal holds the gate open
  private async testNoDoubleDelivery(): Promise<void> {
    for (const inline of KINDS) {
      const { pool, scheduler } = this.scheduler(inline);
      let onScheduler = 0, onPool = 0;
      scheduler.addEventListener('task.message', () => onScheduler++);
      pool.addEventListener('task.message', () => onPool++);
      await this.succeed(scheduler, this.task('once', { mode: 'emit', count: 10 }));
      if (onScheduler !== 10 || onPool !== 0) {
        throw new Error(`${kindOf(inline)}: scheduler ${onScheduler}, pool ${onPool}`);
      }
    }
  }

  private async testCloning(): Promise<void> {
    for (const inline of KINDS) {
      const { scheduler } = this.scheduler(inline);
      const log = this.record(scheduler);
      await this.succeed(scheduler, this.task('mutate', { mode: 'mutate' }));
      const outcome = await this.run(scheduler, this.task('odd', { mode: 'odd' }));

      const messages = log.filter(m => m.kind === 'message');
      const mutated = messages.find(m => m.taskId === 'mutate' && m.name === 'log');
      if (mutated?.data.message !== 'before') {
        throw new Error(`${kindOf(inline)}: delivered ${mutated?.data.message}, not the value at emit()`);
      }
      const odd = messages.filter(m => m.taskId === 'odd');
      if (outcome !== 'resolved') throw new Error(`${kindOf(inline)}: emit() threw into the worker`);
      if (odd.map(m => m.name).join() !== '42,fn,after') throw new Error(`${kindOf(inline)}: names ${odd.map(m => m.name).join()}`);
      if (odd[1].data !== undefined || typeof odd[1].error !== 'string' || odd[1].seq !== 1) {
        throw new Error(`${kindOf(inline)}: uncloneable data gave ${JSON.stringify(odd[1])}`);
      }
    }
  }

  private async testRetry(): Promise<void> {
    for (const inline of KINDS) {
      const { scheduler } = this.scheduler(inline);
      const log = this.record(scheduler);
      await this.run(scheduler, this.task('retry', { mode: 'emit', count: 3, throws: true }, { retryPolicy: { maxRetries: 1, backoffMs: 5 } }));

      const shape = log.map(m => m.kind === 'message' ? `${m.attemptNumber}.${m.seq}` : `${m.kind}:${m.attemptNumber}`).join(' ');
      const expected = '1.0 1.1 1.2 retrying:1 2.0 2.1 2.2 failed:2';
      if (shape !== expected) throw new Error(`${kindOf(inline)}: ${shape}`);
    }
  }

  private async testParity(): Promise<void> {
    const shapes: string[] = [];
    for (const inline of KINDS) {
      const { pool, scheduler } = this.scheduler(inline, { config: { emitInSetup: true } });
      const log = this.record(scheduler);
      const workerMessages: any[] = [];
      pool.addEventListener('worker.message', (e: any) => workerMessages.push(e.detail));
      await this.succeed(scheduler, this.task('p1', { mode: 'emit', count: 20, yieldEvery: 6 }));
      await this.succeed(scheduler, this.task('p2', { mode: 'odd' }));
      await this.run(scheduler, this.task('p3', { mode: 'emit', count: 4, throws: true }));

      const strip = ({ workerId: _w, timestamp: _t, workerType: _k, ...rest }: any) => rest;
      shapes.push(JSON.stringify({ task: log.map(strip), worker: workerMessages.map(strip) }));
    }
    if (shapes[0] !== shapes[1]) throw new Error(`inline:   ${shapes[0]}\nthreaded: ${shapes[1]}`);
  }

  private async testShutdownMidStream(): Promise<void> {
    for (const inline of KINDS) {
      const { pool, scheduler } = this.scheduler(inline, { config: { emitInTeardown: true } });
      scheduler.addEventListener('task.message', () => {});
      pool.addEventListener('worker.message', () => {});
      scheduler.addTask(this.task('s', { mode: 'stream', forMs: 300, everyMs: 2 }));
      await sleep(100);
      await Promise.race([
        scheduler.shutdown(),
        sleep(8000).then(() => { throw new Error(`${kindOf(inline)}: shutdown hung`); })
      ]);
      await sleep(50);
    }
  }

  private async testOptionValidation(): Promise<void> {
    const bad = [{ flushIntervalMs: 0 }, { flushIntervalMs: -1 }, { flushIntervalMs: Infinity }, { maxBuffered: 0 }, { maxBuffered: 1.5 }];
    for (const messages of bad) {
      let threw: unknown;
      try { new WorkerManager(this.workerUrl, { messages }); } catch (error) { threw = error; }
      if (!(threw instanceof RangeError)) throw new Error(`messages ${JSON.stringify(messages)} did not throw RangeError`);
    }
    const pool = new WorkerManager(this.workerUrl, { messages: { maxBuffered: 500 } });
    if (pool.messages.maxBuffered !== 500 || pool.messages.flushIntervalMs !== 20) {
      throw new Error(`Defaults not applied: ${JSON.stringify(pool.messages)}`);
    }
  }
}

// Auto-run tests when executed directly - handle both Deno and Node.js
if ((typeof Deno !== 'undefined' && import.meta.main) ||
    (typeof process !== 'undefined' && process.argv[1] && import.meta.url &&
     import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/')))) {
  const testSuite = new MessagesTestSuite();
  try {
    await testSuite.runAllTests(true);
  } finally {
    await testSuite.cleanup();
  }
}

export { MessagesTestSuite };
export default MessagesTestSuite;
