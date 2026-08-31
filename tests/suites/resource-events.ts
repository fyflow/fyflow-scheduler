// Resource event tests.
//
// `resource.acquired` / `released` / `blocked` / `unblocked` exist so a
// consumer can build a resource view as a pure fold of a stream rather than by
// polling four accessors that describe four different instants.
//
// The acceptance criteria these check came from the resource-events plan,
// section 9. That plan shipped in 0.3.0 and was retired per the doc/design/
// convention - recover it with `git log -- doc/design/resource-events.md`.
// Each criterion exists because a fold can be internally consistent while
// being wrong. Conservation catches a missed emit; agreement with the accessor
// catches the fold and the group disagreeing; the single-emitter check catches
// the double count a consumer listening to both the scheduler and its pools
// would otherwise see.
//
// Assertions are on framework observables only - group metrics, pool residency
// and the event stream - never on state shared with the worker module, which
// does not survive the Node bundle split.

import {
  WorkerManager,
  FyflowScheduler,
  FyflowTask,
  ConcurrentLimitGroup,
  RateLimitGroup,
  KeyedRateLimitGroup
} from '../../index.ts';
import type {
  AdmissionWaiter,
  GaugeDescription,
  GaugeReading,
  ResourceBlockedDetail,
  ResourceEventDetail,
  ResourceGroupMetrics,
  ResourceReleasedDetail,
  ResourceUnblockedDetail
} from '../../index.ts';

// Node.js process declaration for cross-platform compatibility
declare const process: any;

interface TestResult { name: string; passed: boolean; duration: number; error?: string; }
interface TestSuiteResult {
  platform: string; totalTests: number; passed: number; failed: number;
  duration: number; results: TestResult[];
}

/** Every resource event seen, in order, tagged with which one it was. */
interface Recorded {
  name: 'acquired' | 'released' | 'blocked' | 'unblocked';
  detail: ResourceEventDetail & {
    reason?: string;
    queuePosition?: number;
  };
}

/**
 * A group written the way a third party would: `canRun`/`onStart`/`onFinish`/
 * `getMetrics` and nothing else. It must render through the `getMetrics()`
 * default with no viewer change, which is acceptance criterion 5.
 *
 * It also counts `read()`-shaped work, so the listener guard is testable.
 */
class TokenBucketGroup extends EventTarget {
  readonly id: string;
  readonly type: string = 'token-bucket'; // Deliberately outside the old closed union
  readonly limit: number;
  running = 0;
  metricsReads = 0;

  constructor(limit: number, id: string) {
    super();
    this.limit = limit;
    this.id = id;
  }

  canRun(_key?: string, cost = 1): boolean { return this.running + cost <= this.limit; }
  onStart(_key?: string, cost = 1): void { this.running += cost; }
  onFinish(_key?: string, cost = 1): void { this.running = Math.max(0, this.running - cost); }

  getMetrics(): ResourceGroupMetrics {
    this.metricsReads++;
    return {
      limit: this.limit,
      running: this.running,
      available: Math.max(0, this.limit - this.running),
      utilization: this.running / this.limit
    };
  }
}

/** A third-party group that describes itself, with a kind no viewer knows. */
class HistogramGroup extends TokenBucketGroup {
  override readonly type: string = 'histogram';

  describe(): GaugeDescription {
    return {
      gauges: [
        { id: 'p50', label: 'p50 latency', kind: 'histogram' as any, unit: 'ms', limit: 100 },
        { id: 'slots', label: 'slots', kind: 'level', unit: 'units', limit: this.limit }
      ]
    };
  }

  read(): GaugeReading[] {
    return [
      { id: 'p50', value: 7, limit: 100 },
      { id: 'slots', value: this.running, limit: this.limit }
    ];
  }
}

/**
 * Fold key for a `(groupId, key)` pair.
 *
 * JSON rather than a delimiter: the scheduler's own queue ids use NUL for this
 * exactly because no delimiter is safe, and a test that reproduces the trap it
 * is checking for is not much of a check.
 */
function bucketKey(groupId: string, key?: string): string {
  return JSON.stringify([groupId, key ?? null]);
}

class ResourceEventsTestSuite {
  private results: TestResult[] = [];
  private startTime = 0;
  private inlineWorkerUrl = "";
  private schedulers: FyflowScheduler[] = [];

  private track(scheduler: FyflowScheduler): FyflowScheduler {
    this.schedulers.push(scheduler);
    return scheduler;
  }

  async cleanup(): Promise<void> {
    for (const scheduler of this.schedulers) {
      try { await scheduler.shutdown(); } catch { /* already down */ }
    }
    this.schedulers = [];
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

  // --- helpers -------------------------------------------------------------

  /** Subscribe to all four events and collect them in arrival order. */
  private record(scheduler: FyflowScheduler): Recorded[] {
    const seen: Recorded[] = [];
    const names = ['acquired', 'released', 'blocked', 'unblocked'] as const;
    for (const name of names) {
      scheduler.addEventListener(`resource.${name}`, (e: any) => {
        // Copied field by field: the detail is built fresh per event, but
        // copying here is what the docs tell consumers to do
        seen.push({ name, detail: { ...e.detail } });
      });
    }
    return seen;
  }

  private pool(options: Record<string, any> = {}) {
    return new WorkerManager(this.inlineWorkerUrl, {
      maxThreads: 1,
      maxConcurrentTasks: 1,
      inline: true,
      idleTimeout: 30,
      idleCheckIntervalMs: 15,
      ...options
    });
  }

  private task(id: string, workerType: string, extra: Record<string, any> = {}) {
    return new FyflowTask({
      id,
      workerType,
      payload: { taskId: id, delay: 20 },
      ...extra
    });
  }

  /** `Σ acquired.cost − Σ released.cost` per `(groupId, key)`, plus the low-water mark. */
  private foldHoldings(seen: Recorded[]): { net: Map<string, number>; wentNegative: string[] } {
    const net = new Map<string, number>();
    const wentNegative: string[] = [];

    for (const { name, detail } of seen) {
      if (name !== 'acquired' && name !== 'released') continue;
      const bucket = bucketKey(detail.groupId, detail.key);
      const value = (net.get(bucket) ?? 0) + (name === 'acquired' ? detail.cost : -detail.cost);
      net.set(bucket, value);
      if (value < 0 && !wentNegative.includes(bucket)) wentNegative.push(bucket);
    }
    return { net, wentNegative };
  }

  /** Queue membership per `(groupId, key, holderId)`, in arrival order. */
  private foldMembership(seen: Recorded[]): {
    queues: Map<string, string[]>;
    unmatched: string[];
  } {
    const queues = new Map<string, string[]>();
    const unmatched: string[] = [];

    for (const { name, detail } of seen) {
      if (name !== 'blocked' && name !== 'unblocked') continue;
      const queueId = bucketKey(detail.groupId, detail.key);
      const members = queues.get(queueId) ?? [];

      if (name === 'blocked') {
        members.push(detail.holderId);
      } else {
        const index = members.indexOf(detail.holderId);
        // An unblocked with no preceding blocked for the same triple is the
        // failure criterion 1b names, not a fold that quietly ignores it
        if (index === -1) unmatched.push(`${queueId}/${detail.holderId}`);
        else members.splice(index, 1);
      }
      queues.set(queueId, members);
    }
    return { queues, unmatched };
  }

  private async waitFor(predicate: () => boolean, timeoutMs: number, what: string): Promise<void> {
    const start = performance.now();
    while (!predicate()) {
      if (performance.now() - start > timeoutMs) {
        throw new Error(`Timed out after ${timeoutMs}ms waiting for ${what}`);
      }
      await new Promise(r => setTimeout(r, 5));
    }
  }

  // --- suite ---------------------------------------------------------------

  async runAllTests(_exitOnComplete = true): Promise<TestSuiteResult> {
    if (typeof Deno !== "undefined") {
      this.inlineWorkerUrl = new URL("../workers/testInlineWorker.ts", import.meta.url).href;
    } else {
      // @ts-expect-error - esbuild resolves ?worker-direct at build time
      this.inlineWorkerUrl = new URL((await import('../workers/testInlineWorker.ts?worker-direct')).default).href;
    }

    const platform = typeof globalThis !== 'undefined' && 'Deno' in globalThis ? 'Deno' : 'Node.js';
    console.log(`🚀 FyFlow Resource Event Tests - ${platform}`);
    console.log('='.repeat(60));

    this.startTime = performance.now();
    this.results = [];

    this.results.push(await this.runTest('Criterion 1 - Holdings Are Conserved', () => this.testConservationOfHoldings()));
    this.results.push(await this.runTest('Criterion 1b - Queue Membership Is Conserved', () => this.testConservationOfMembership()));
    this.results.push(await this.runTest('Criterion 2 - The Fold Agrees With getMetrics()', () => this.testAgreementWithAccessor()));
    this.results.push(await this.runTest('Criterion 2 - Rate Readings Agree With getStatus()', () => this.testRateReadingsAgree()));
    this.results.push(await this.runTest('Criterion 3 - The idleTimeout:0 Stall Is Visible', () => this.testStallIsVisible()));
    this.results.push(await this.runTest('Criterion 5 - A Third-Party Group Renders Untouched', () => this.testThirdPartyDefault()));
    this.results.push(await this.runTest('Criterion 5 - A Self-Describing Group Keeps Its Own Gauges', () => this.testThirdPartyDescribes()));
    this.results.push(await this.runTest('Readings Are After The Mutation And Never Clamped', () => this.testReadingsAfterMutation()));
    this.results.push(await this.runTest('Nothing Is Built While Nobody Listens', () => this.testListenerGuard()));
    this.results.push(await this.runTest('The Scheduler Is The Only Emitter', () => this.testSingleEmitter()));
    this.results.push(await this.runTest('A Resident Acquire Carries Its Worker And Pool', () => this.testResidentIdentity()));
    this.results.push(await this.runTest('Blocked Names The Group It Is Short Of', () => this.testBlockedNamesGroup()));
    this.results.push(await this.runTest('A Rate-Limited Backlog Unblocks As Admitted', () => this.testAdmittedReason()));
    this.results.push(await this.runTest('Shutdown Unblocks Every Remaining Waiter', () => this.testShutdownUnblocks()));
    this.results.push(await this.runTest('A Retry Is One Acquire/Release Pair Per Attempt', () => this.testRetryPairsPerAttempt()));
    this.results.push(await this.runTest('Keyed Groups Read Per Bucket, Not In Aggregate', () => this.testKeyedReadings()));
    this.results.push(await this.runTest('S2 - The Five Events Carry A Timestamp', () => this.testTimestamps()));
    this.results.push(await this.runTest('Criterion 4 - Head-Of-Line Is Legible', () => this.testHeadOfLine()));
    this.results.push(await this.runTest('Criterion 4b - The Fold And The Accessor Agree, In Order', () => this.testFoldMatchesAccessor()));
    this.results.push(await this.runTest('The Admission Queue Splits On NUL, Not A Space', () => this.testAdmissionQueueKeying()));
    this.results.push(await this.runTest('The Admission Queue Filters By Group', () => this.testAdmissionQueueFilter()));
    this.results.push(await this.runTest('A Busy Scheduler Still Wakes Its Blocked Tasks', () => this.testRetryHeartbeatSurvivesLoad()));
    this.results.push(await this.runTest('A Waiter Whose Pool Vanishes Fails, Not Vanishes', () => this.testOrphanedWaiterFails()));

    const totalDuration = performance.now() - this.startTime;
    const passed = this.results.filter(r => r.passed).length;
    const failed = this.results.length - passed;

    console.log(`\n📊 Resource Event Test Summary - ${platform}`);
    console.log('='.repeat(60));
    console.log(`Total Tests: ${this.results.length}`);
    console.log(`✅ Passed: ${passed}`);
    console.log(`❌ Failed: ${failed}`);
    console.log(`⏱️  Duration: ${totalDuration.toFixed(1)}ms`);

    if (failed > 0) {
      console.log('\n❌ Failed Tests:');
      this.results.filter(r => !r.passed).forEach(r => console.log(`  • ${r.name}: ${r.error}`));
    }

    return {
      platform, totalTests: this.results.length, passed, failed,
      duration: totalDuration, results: this.results
    };
  }

  // --- criterion 1 ---------------------------------------------------------

  /**
   * Both lifetimes, over a run that reaches shutdown. Resident holdings are
   * only returned at teardown, so a run that stops at drain would show the
   * resident group permanently up and prove nothing.
   */
  private async testConservationOfHoldings(): Promise<void> {
    const cpu = new ConcurrentLimitGroup(2, 'cpu');
    const vram = new ConcurrentLimitGroup(4, 'vram');
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ maxThreads: 2, groups: ['cpu'], residentGroups: { vram: 2 } }) },
      { cpu, vram }
    ));
    const seen = this.record(scheduler);

    await Promise.all(
      Array.from({ length: 8 }, (_, i) =>
        scheduler.addTask(this.task(`c1-${i}`, 'A'), { createPromise: true })
      )
    );
    await scheduler.shutdown();

    const { net, wentNegative } = this.foldHoldings(seen);
    if (wentNegative.length > 0) {
      throw new Error(`Occupancy went negative for ${wentNegative.join(', ')}`);
    }
    for (const [bucket, value] of net) {
      if (value !== 0) throw new Error(`${bucket} folded to ${value}, expected 0`);
    }
    if (!net.has(bucketKey('cpu'))) throw new Error('No task-held events were recorded at all');
    if (!net.has(bucketKey('vram'))) throw new Error('No resident events were recorded at all');
  }

  // --- criterion 1b --------------------------------------------------------

  /**
   * A rate limit tight enough that most tasks block, run to shutdown. Every
   * push must be balanced by exactly one exit, and no exit may arrive for a
   * triple that never entered.
   */
  private async testConservationOfMembership(): Promise<void> {
    const api = new RateLimitGroup([{ limit: 2, windowMs: 60 }], 'api');
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ maxThreads: 2, maxConcurrentTasks: 4, groups: ['api'] }) },
      { api }
    ));
    const seen = this.record(scheduler);

    await Promise.all(
      Array.from({ length: 10 }, (_, i) =>
        scheduler.addTask(this.task(`c1b-${i}`, 'A'), { createPromise: true })
      )
    );
    await scheduler.shutdown();

    const { queues, unmatched } = this.foldMembership(seen);
    if (unmatched.length > 0) {
      throw new Error(`unblocked with no preceding blocked: ${unmatched.join(', ')}`);
    }
    for (const [queueId, members] of queues) {
      if (members.length !== 0) {
        throw new Error(`${queueId} still holds ${members.length} waiter(s) after shutdown`);
      }
    }

    const blocked = seen.filter(r => r.name === 'blocked').length;
    if (blocked === 0) {
      throw new Error('Nothing ever blocked - the rate limit was too loose to test conservation');
    }
  }

  // --- criterion 2 ---------------------------------------------------------

  /**
   * Quiescent here means nothing in flight. The resident group is the
   * interesting half: at drain its worker is still alive, so the fold has to
   * land on a non-zero number rather than trivially on zero.
   */
  private async testAgreementWithAccessor(): Promise<void> {
    const vram = new ConcurrentLimitGroup(8, 'vram');
    const cpu = new ConcurrentLimitGroup(2, 'cpu');
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ maxThreads: 2, idleTimeout: 5000, groups: ['cpu'], residentGroups: { vram: 3 } }) },
      { vram, cpu }
    ));
    const seen = this.record(scheduler);

    await Promise.all([
      scheduler.addTask(this.task('c2-a', 'A'), { createPromise: true }),
      scheduler.addTask(this.task('c2-b', 'A'), { createPromise: true })
    ]);

    // Checked synchronously after settle, so the idle sweep cannot race it
    const { net } = this.foldHoldings(seen);
    const metrics = scheduler.getResourceMetrics();

    for (const groupId of ['vram', 'cpu']) {
      const folded = net.get(bucketKey(groupId)) ?? 0;
      if (folded !== metrics[groupId].running) {
        throw new Error(
          `${groupId}: folded ${folded} but getMetrics().running is ${metrics[groupId].running}`
        );
      }
    }
    if ((net.get(bucketKey('vram')) ?? 0) === 0) {
      throw new Error('The resident fold was 0, so the check passed vacuously');
    }
  }

  /**
   * The amendment to criterion 2. `getMetrics()` on a rate group reports the
   * in-flight count against the tightest window's limit - two unrelated
   * quantities - so occupancy is the wrong check there and window readings are
   * the right one.
   */
  private async testRateReadingsAgree(): Promise<void> {
    const api = new RateLimitGroup(
      [{ limit: 4, windowMs: 1000 }, { limit: 20, windowMs: 60_000 }],
      'api'
    );
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ maxThreads: 1, maxConcurrentTasks: 2, groups: ['api'] }) },
      { api }
    ));
    const seen = this.record(scheduler);

    await Promise.all(
      Array.from({ length: 3 }, (_, i) =>
        scheduler.addTask(this.task(`rate-${i}`, 'A'), { createPromise: true })
      )
    );

    const latest = [...seen].reverse().find(r => r.detail.groupId === 'api');
    if (!latest) throw new Error('No api events were recorded');
    if (latest.detail.readings.length !== 2) {
      throw new Error(`Expected 2 window gauges, got ${latest.detail.readings.length}`);
    }

    latest.detail.readings.forEach((reading, index) => {
      if (reading.id !== `window-${index}`) {
        throw new Error(`Gauge ${index} is "${reading.id}", expected "window-${index}"`);
      }
    });

    // The criterion is an EQUALITY, so it is checked as one. Both are pure
    // functions of the same state and are called back to back with nothing in
    // flight, so only a clock tick separates them - and no request made
    // milliseconds ago can leave a 1s window on a 1ms tick.
    const readings = api.read();
    const status = api.getStatus();
    if (readings.length !== status.windows.length) {
      throw new Error(`read() gave ${readings.length} gauges, getStatus() ${status.windows.length} windows`);
    }
    readings.forEach((reading, index) => {
      const window = status.windows[index];
      if (reading.value !== window.current) {
        throw new Error(
          `window-${index}: read() says ${reading.value}, getStatus() says ${window.current}`
        );
      }
      if (reading.limit !== window.limit) {
        throw new Error(`window-${index} limit ${reading.limit} != getStatus() ${window.limit}`);
      }
    });
    if (readings[0].value === 0) {
      throw new Error('Both agreed on zero, so the equality was vacuous');
    }

    // `resetAt` is deliberately NOT getStatus().resetTime, which is
    // `windowStart + windowMs` and therefore always evaluates to now - it says
    // "resets immediately" however full the window is. read() reports when the
    // oldest counted request actually falls out.
    const now = Date.now();
    if (status.windows[1].resetTime > now + 1000) {
      throw new Error(
        `getStatus().resetTime looks fixed - if it is meaningful now, read() should use it`
      );
    }
    if (readings[1].resetAt === undefined || readings[1].resetAt <= now + 1000) {
      throw new Error(
        `The 60s window's resetAt is ${readings[1].resetAt}, expected well beyond now (${now})`
      );
    }

    const described = scheduler.describeResources();
    if (described.api.gauges.length !== 2 || described.api.gauges[0].kind !== 'window') {
      throw new Error(`Expected 2 window gauges from describe(), got ${JSON.stringify(described.api.gauges)}`);
    }
  }

  // --- criterion 3 ---------------------------------------------------------

  /**
   * The documented `idleTimeout: 0` resident deadlock. A worker that never
   * terminates never returns its units, so the second pool waits forever - and
   * `stats` reads `queued=0 running=0` because blocked tasks leave the queued
   * count, which is what makes a stalled scheduler look idle.
   *
   * The whole point of the event is that the stall is legible without that
   * folklore.
   */
  private async testStallIsVisible(): Promise<void> {
    const vram = new ConcurrentLimitGroup(1, 'vram');
    const scheduler = this.track(new FyflowScheduler(
      {
        A: this.pool({ idleTimeout: 0, residentGroups: { vram: 1 } }),
        B: this.pool({ idleTimeout: 0, residentGroups: { vram: 1 } })
      },
      { vram }
    ));
    const seen = this.record(scheduler);

    await scheduler.addTask(this.task('stall-a', 'A'), { createPromise: true });

    // Fire and forget: this one can never run, so awaiting it would hang
    scheduler.addTask(this.task('stall-b', 'B'));

    await this.waitFor(
      () => seen.some(r => r.name === 'blocked' && r.detail.holderId === 'stall-b'),
      2000,
      'a resource.blocked naming the contended group'
    );

    const blocked = seen.find(
      r => r.name === 'blocked' && r.detail.holderId === 'stall-b'
    )!.detail as ResourceBlockedDetail;

    if (blocked.groupId !== 'vram') {
      throw new Error(`blocked named "${blocked.groupId}", expected "vram"`);
    }
    if (blocked.lifetime !== 'resident') {
      throw new Error(`Expected lifetime "resident", got "${blocked.lifetime}"`);
    }
    if (blocked.workerType !== 'B') {
      throw new Error(`Expected workerType "B", got "${blocked.workerType}"`);
    }

    // The trap itself: the scheduler looks idle at exactly this moment
    if (scheduler.stats.queued !== 0 || scheduler.stats.running !== 0) {
      throw new Error(
        `Expected the stall to look idle, but stats read ` +
        `queued=${scheduler.stats.queued} running=${scheduler.stats.running}`
      );
    }
    const metrics = scheduler.getResourceMetrics();
    if (metrics.vram.running !== metrics.vram.limit) {
      throw new Error(`Expected vram pinned at its limit, got ${JSON.stringify(metrics.vram)}`);
    }
  }

  // --- criterion 5 ---------------------------------------------------------

  /** A group that knows nothing about gauges still produces a usable one. */
  private async testThirdPartyDefault(): Promise<void> {
    const bucket = new TokenBucketGroup(2, 'tokens');
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ groups: ['tokens'] }) },
      { tokens: bucket as any }
    ));
    const seen = this.record(scheduler);

    await scheduler.addTask(this.task('tp-1', 'A'), { createPromise: true });

    const described = scheduler.describeResources();
    const gauges = described.tokens.gauges;
    if (gauges.length !== 1 || gauges[0].kind !== 'level' || gauges[0].id !== 'units') {
      throw new Error(`Expected one default level gauge, got ${JSON.stringify(gauges)}`);
    }
    if (gauges[0].limit !== 2) {
      throw new Error(`Default gauge limit is ${gauges[0].limit}, expected 2`);
    }

    const acquired = seen.find(r => r.name === 'acquired');
    if (!acquired) throw new Error('No acquire was emitted for the third-party group');
    if (acquired.detail.groupType !== 'token-bucket') {
      throw new Error(
        `groupType is "${acquired.detail.groupType}" - a custom type must survive, ` +
        `which is what widening it off the closed union bought`
      );
    }
    if (acquired.detail.readings.length !== 1 || acquired.detail.readings[0].id !== 'units') {
      throw new Error(`Expected one default reading, got ${JSON.stringify(acquired.detail.readings)}`);
    }
  }

  /** A group that describes itself keeps its own gauges, unknown kinds included. */
  private async testThirdPartyDescribes(): Promise<void> {
    const histogram = new HistogramGroup(2, 'latency');
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ groups: ['latency'] }) },
      { latency: histogram as any }
    ));
    const seen = this.record(scheduler);

    await scheduler.addTask(this.task('tp-2', 'A'), { createPromise: true });

    const gauges = scheduler.describeResources().latency.gauges;
    if (gauges.length !== 2 || gauges[0].id !== 'p50') {
      throw new Error(`Expected the group's own two gauges, got ${JSON.stringify(gauges)}`);
    }
    // An unrecognised kind is passed through untouched - it is the viewer's job
    // to fall back to `level`, not the scheduler's to rewrite it
    if ((gauges[0].kind as string) !== 'histogram') {
      throw new Error(`Unknown kind was rewritten to "${gauges[0].kind}"`);
    }

    const acquired = seen.find(r => r.name === 'acquired');
    if (!acquired) throw new Error('No acquire was emitted for the self-describing group');
    if (acquired.detail.readings.length !== 2 || acquired.detail.readings[0].id !== 'p50') {
      throw new Error(`Expected the group's own readings, got ${JSON.stringify(acquired.detail.readings)}`);
    }
  }

  // --- rules ---------------------------------------------------------------

  /**
   * Readings are the state AFTER the operation, and an optimistic group over
   * its limit reports the real number. A clamped reading would make the event
   * disagree with `getMetrics()` precisely when the disagreement matters.
   */
  private async testReadingsAfterMutation(): Promise<void> {
    const cpu = new ConcurrentLimitGroup(1, 'cpu');
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ maxThreads: 2, maxConcurrentTasks: 2, groups: ['cpu'] }) },
      { cpu }
    ));

    const mismatches: string[] = [];
    let peakReading = 0;
    scheduler.addEventListener('resource.acquired', (e: any) => {
      const reading = e.detail.readings[0].value;
      peakReading = Math.max(peakReading, reading);
      // Read synchronously inside the listener, so `cpu.running` is still the
      // value the emit was built from
      if (reading !== cpu.running) mismatches.push(`${reading} != ${cpu.running}`);
    });

    await Promise.all(
      Array.from({ length: 6 }, (_, i) =>
        scheduler.addTask(this.task(`clamp-${i}`, 'A'), { createPromise: true })
      )
    );

    if (mismatches.length > 0) {
      throw new Error(`Reading disagreed with the group: ${mismatches.join(', ')}`);
    }
    if (peakReading < 1) throw new Error('No acquire readings were seen at all');
    if (peakReading > cpu.limit + 1) {
      // Not a failure of the events - just a note that clamping is untested
      // here. Over-limit readings are optimistic overshoot, which is allowed.
      console.log(`   (peak reading ${peakReading} over a limit of ${cpu.limit} - overshoot is expected)`);
    }
  }

  /**
   * These are the highest-frequency events in the library. Nothing may be
   * built, and no `read()` may run, while nobody is subscribed.
   */
  private async testListenerGuard(): Promise<void> {
    const bucket = new TokenBucketGroup(4, 'tokens');
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ groups: ['tokens'] }) },
      { tokens: bucket as any }
    ));

    await scheduler.addTask(this.task('guard-1', 'A'), { createPromise: true });
    const unobserved = bucket.metricsReads;

    const listener = () => {};
    scheduler.addEventListener('resource.acquired', listener);
    await scheduler.addTask(this.task('guard-2', 'A'), { createPromise: true });
    const observed = bucket.metricsReads;

    if (observed <= unobserved) {
      throw new Error('Subscribing changed nothing - the guard is not actually gating reads');
    }

    scheduler.removeEventListener('resource.acquired', listener);
    const beforeSilence = bucket.metricsReads;
    await scheduler.addTask(this.task('guard-3', 'A'), { createPromise: true });
    if (bucket.metricsReads !== beforeSilence) {
      throw new Error('Unsubscribing did not stop the reads');
    }

    // Removing a listener that was never added must not silence a real one
    scheduler.addEventListener('resource.acquired', listener);
    scheduler.removeEventListener('resource.acquired', () => {});
    const beforeNoise = bucket.metricsReads;
    await scheduler.addTask(this.task('guard-4', 'A'), { createPromise: true });
    if (bucket.metricsReads === beforeNoise) {
      throw new Error(
        'Removing a listener that was never registered silenced a live one - ' +
        'the guard is counting rather than tracking'
      );
    }
  }

  /**
   * A `WorkerManager` must not dispatch `resource.*`. A consumer subscribing to
   * both source families would otherwise count every resident acquire twice,
   * and conservation would drift by exactly the resident share.
   */
  private async testSingleEmitter(): Promise<void> {
    const vram = new ConcurrentLimitGroup(4, 'vram');
    const pool = this.pool({ residentGroups: { vram: 1 } });
    const scheduler = this.track(new FyflowScheduler({ A: pool }, { vram }));

    let fromPool = 0;
    for (const name of ['acquired', 'released', 'blocked', 'unblocked']) {
      pool.addEventListener(`resource.${name}`, () => { fromPool++; });
    }
    const seen = this.record(scheduler);

    await scheduler.addTask(this.task('emit-1', 'A'), { createPromise: true });
    await scheduler.shutdown();

    if (fromPool !== 0) {
      throw new Error(`The pool dispatched ${fromPool} resource event(s); it must dispatch none`);
    }
    const resident = seen.filter(r => r.detail.lifetime === 'resident');
    if (resident.length === 0) {
      throw new Error('No resident events reached the scheduler either, so nothing was proven');
    }
    const acquires = resident.filter(r => r.name === 'acquired');
    const workers = new Set(acquires.map(r => r.detail.holderId));
    if (acquires.length !== workers.size) {
      throw new Error(`${acquires.length} resident acquires for ${workers.size} worker(s) - double counted`);
    }
  }

  /** A resident holding names its worker and its pool, not `'inline'`. */
  private async testResidentIdentity(): Promise<void> {
    const vram = new ConcurrentLimitGroup(4, 'vram');
    const pool = this.pool({ residentGroups: { vram: 3 } });
    const scheduler = this.track(new FyflowScheduler({ ModelPool: pool }, { vram }));
    const seen = this.record(scheduler);

    await scheduler.addTask(this.task('res-1', 'ModelPool'), { createPromise: true });

    const acquired = seen.find(r => r.name === 'acquired' && r.detail.lifetime === 'resident');
    if (!acquired) throw new Error('No resident acquire was emitted');

    if (acquired.detail.holderKind !== 'worker') {
      throw new Error(`holderKind is "${acquired.detail.holderKind}", expected "worker"`);
    }
    if (acquired.detail.cost !== 3) {
      throw new Error(`cost is ${acquired.detail.cost}, expected the pool's 3`);
    }
    // `workerType` on the worker lifecycle events is 'inline' or 'thread'; here
    // it is the POOL KEY, which is the only place that name exists
    if (acquired.detail.workerType !== 'ModelPool') {
      throw new Error(`workerType is "${acquired.detail.workerType}", expected the pool key "ModelPool"`);
    }
    if (!pool.getWorkerIds().includes(acquired.detail.holderId)) {
      throw new Error(`holderId "${acquired.detail.holderId}" is not one of the pool's worker ids`);
    }

    await scheduler.shutdown();
    const released = seen.find(
      r => r.name === 'released' && r.detail.holderId === acquired.detail.holderId
    );
    if (!released) throw new Error('The resident holding was never released');
    if ((released.detail as ResourceReleasedDetail).reason !== 'shutdown') {
      throw new Error(`Release reason is "${(released.detail as ResourceReleasedDetail).reason}", expected "shutdown"`);
    }
  }

  /** Blocked reports the one group the waiter is short of, with its position. */
  private async testBlockedNamesGroup(): Promise<void> {
    const api = new RateLimitGroup([{ limit: 1, windowMs: 200 }], 'api');
    const cpu = new ConcurrentLimitGroup(8, 'cpu');
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ maxConcurrentTasks: 4, groups: ['api', 'cpu'] }) },
      { api, cpu }
    ));
    const seen = this.record(scheduler);

    await Promise.all(
      Array.from({ length: 4 }, (_, i) =>
        scheduler.addTask(this.task(`short-${i}`, 'A'), { createPromise: true })
      )
    );

    const blocked = seen.filter(r => r.name === 'blocked');
    if (blocked.length === 0) throw new Error('Nothing blocked, so nothing was proven');

    for (const record of blocked) {
      if (record.detail.groupId !== 'api') {
        throw new Error(`Blocked named "${record.detail.groupId}"; only "api" was short`);
      }
      const position = (record.detail as ResourceBlockedDetail).queuePosition;
      if (typeof position !== 'number' || position < 0) {
        throw new Error(`queuePosition is ${position}`);
      }
      if (record.detail.lifetime !== 'task-held' || record.detail.cost !== 1) {
        throw new Error(
          `Expected a task-held cost of 1, got ${record.detail.lifetime}/${record.detail.cost}`
        );
      }
    }
  }

  /** The ordinary exit: a rate window rolls over and the backlog is admitted. */
  private async testAdmittedReason(): Promise<void> {
    const api = new RateLimitGroup([{ limit: 2, windowMs: 80 }], 'api');
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ maxConcurrentTasks: 4, groups: ['api'] }) },
      { api }
    ));
    const seen = this.record(scheduler);

    await Promise.all(
      Array.from({ length: 6 }, (_, i) =>
        scheduler.addTask(this.task(`adm-${i}`, 'A'), { createPromise: true })
      )
    );

    const unblocked = seen.filter(r => r.name === 'unblocked') as Array<
      Recorded & { detail: ResourceUnblockedDetail }
    >;
    if (unblocked.length === 0) throw new Error('Nothing was ever unblocked');

    const admitted = unblocked.filter(r => r.detail.reason === 'admitted');
    if (admitted.length === 0) {
      throw new Error(
        `No "admitted" exits - reasons seen: ${[...new Set(unblocked.map(r => r.detail.reason))].join(', ')}`
      );
    }

    // Everything that blocked eventually ran, so every exit here is an
    // admission - a `superseded` or `orphaned` would mean something else
    for (const record of unblocked) {
      if (record.detail.reason !== 'admitted') {
        throw new Error(`Unexpected exit reason "${record.detail.reason}"`);
      }
    }
  }

  /** Exit (d): a waiter still queued at shutdown is discarded, and says so. */
  private async testShutdownUnblocks(): Promise<void> {
    const vram = new ConcurrentLimitGroup(1, 'vram');
    const scheduler = this.track(new FyflowScheduler(
      {
        A: this.pool({ idleTimeout: 0, residentGroups: { vram: 1 } }),
        B: this.pool({ idleTimeout: 0, residentGroups: { vram: 1 } })
      },
      { vram }
    ));
    const seen = this.record(scheduler);

    await scheduler.addTask(this.task('sd-a', 'A'), { createPromise: true });
    scheduler.addTask(this.task('sd-b', 'B'));

    await this.waitFor(
      () => seen.some(r => r.name === 'blocked' && r.detail.holderId === 'sd-b'),
      2000,
      'sd-b to block'
    );

    await scheduler.shutdown();

    const exit = seen.find(
      r => r.name === 'unblocked' && r.detail.holderId === 'sd-b'
    );
    if (!exit) throw new Error('The waiter was discarded at shutdown with no unblocked');
    if ((exit.detail as ResourceUnblockedDetail).reason !== 'shutdown') {
      throw new Error(`Exit reason is "${(exit.detail as ResourceUnblockedDetail).reason}", expected "shutdown"`);
    }

    const { queues, unmatched } = this.foldMembership(seen);
    if (unmatched.length > 0) throw new Error(`Unmatched exits: ${unmatched.join(', ')}`);
    for (const [queueId, members] of queues) {
      if (members.length !== 0) throw new Error(`${queueId} did not drain across shutdown`);
    }
  }

  /**
   * `reason: 'settled'` means *this attempt ended*, not *this task finished*.
   *
   * A retried task acquires and releases once per attempt, so a consumer
   * measuring hold duration sees several pairs for one task id and must not
   * assume they nest or that the first release is the last. Documented in
   * AGENTS.md section 6, and pinned here because the reason vocabulary is fixed
   * at three values and cannot grow a `retried`.
   */
  private async testRetryPairsPerAttempt(): Promise<void> {
    const cpu = new ConcurrentLimitGroup(4, 'cpu');
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ groups: ['cpu'] }) },
      { cpu }
    ));
    const seen = this.record(scheduler);

    const failing = scheduler.addTask(new FyflowTask({
      id: 'retry-1',
      workerType: 'A',
      payload: { shouldThrow: true },
      retryPolicy: { maxRetries: 2, backoffMs: 1 }
    }), { createPromise: true }) as Promise<any>;
    await failing.catch(() => { /* expected - three attempts, all failing */ });

    const mine = seen.filter(r => r.detail.holderId === 'retry-1');
    const acquires = mine.filter(r => r.name === 'acquired');
    const releases = mine.filter(r => r.name === 'released');

    // One initial attempt plus two retries
    if (acquires.length !== 3 || releases.length !== 3) {
      throw new Error(
        `Expected 3 acquire/release pairs for 3 attempts, got ${acquires.length}/${releases.length} - ` +
        `if that changed, the retry note in AGENTS.md section 6 needs updating`
      );
    }
    for (const release of releases) {
      if ((release.detail as ResourceReleasedDetail).reason !== 'settled') {
        throw new Error(`A retry release said "${(release.detail as ResourceReleasedDetail).reason}"`);
      }
    }
    // They alternate rather than nesting: nothing is held across a retry
    const order = mine.filter(r => r.name === 'acquired' || r.name === 'released').map(r => r.name);
    if (order.join(',') !== 'acquired,released,acquired,released,acquired,released') {
      throw new Error(`Attempts overlapped instead of alternating: ${order.join(',')}`);
    }

    const { net } = this.foldHoldings(seen);
    if ((net.get(bucketKey('cpu')) ?? 0) !== 0) {
      throw new Error(`cpu folded to ${net.get(bucketKey('cpu'))} after a retried task, expected 0`);
    }
  }

  /**
   * A keyed group's limits are per key, so a reading is only meaningful with
   * the key it was taken for. The keyless read reports nothing rather than an
   * aggregate that looks like utilisation and cannot say whether anything runs.
   */
  private async testKeyedReadings(): Promise<void> {
    const api = new KeyedRateLimitGroup([{ limit: 2, windowMs: 500 }], { id: 'api' });
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ maxConcurrentTasks: 4, groups: ['api'] }) },
      { api }
    ));
    const seen = this.record(scheduler);

    await Promise.all([
      scheduler.addTask(this.task('k-a1', 'A', { limitKey: 'alpha' }), { createPromise: true }),
      scheduler.addTask(this.task('k-a2', 'A', { limitKey: 'alpha' }), { createPromise: true }),
      scheduler.addTask(this.task('k-b1', 'A', { limitKey: 'beta' }), { createPromise: true })
    ]);

    const acquires = seen.filter(r => r.name === 'acquired');
    if (acquires.length === 0) throw new Error('No keyed acquires were recorded');

    for (const record of acquires) {
      if (!record.detail.key) throw new Error(`A keyed acquire arrived with no key`);
      if (record.detail.readings.length !== 1) {
        throw new Error(`Expected one window gauge per key, got ${record.detail.readings.length}`);
      }
      if (record.detail.readings[0].limit !== 2) {
        throw new Error(`Reading limit is ${record.detail.readings[0].limit}, expected the per-key 2`);
      }
    }

    const alpha = acquires.filter(r => r.detail.key === 'alpha');
    const beta = acquires.filter(r => r.detail.key === 'beta');
    if (alpha.length !== 2 || beta.length !== 1) {
      throw new Error(`Buckets were mixed up: alpha=${alpha.length} beta=${beta.length}`);
    }
    // Two tasks in alpha, one in beta: the second alpha reading must count both
    if (Math.max(...alpha.map(r => r.detail.readings[0].value)) < 2) {
      throw new Error(`alpha never read 2, so the buckets are not being tracked per key`);
    }
    if (beta[0].detail.readings[0].value !== 1) {
      throw new Error(`beta read ${beta[0].detail.readings[0].value}; alpha's traffic leaked into it`);
    }

    if (api.read().length !== 0) {
      throw new Error('A keyless read() returned an aggregate rather than nothing');
    }
  }

  // --- criteria 4 and 4b ---------------------------------------------------

  /**
   * A 20-unit pool waiting on a 24-unit group held by 2-unit workers.
   *
   * The cheap pools keep cycling - short idle timeout, work arriving - so units
   * are released and retaken throughout, and every release runs
   * `_retryBlockedTasksForGroup`. That is the loop where a task is shifted out
   * of a blocked queue and unshifted back within one synchronous pass, which is
   * exactly what criterion 4b exists to catch.
   *
   * `observe` is called between dispatch passes; it receives the scheduler,
   * because a sampler that closed over a variable assigned after this returns
   * would silently do nothing.
   */
  private async runHeadOfLineScenario(
    observe: (scheduler: FyflowScheduler, seen: Recorded[]) => void
  ): Promise<{ scheduler: FyflowScheduler; seen: Recorded[] }> {
    const vram = new ConcurrentLimitGroup(24, 'vram');

    const pools: Record<string, any> = {
      Big: this.pool({ idleTimeout: 5000, residentGroups: { vram: 20 } })
    };
    // Three pools that never idle out, holding 6 units for the whole run. With
    // 18 free at best, the 20-unit waiter can never fit however much the rest
    // of the group churns - so it stays at the head instead of being admitted
    // halfway through and ending the test early.
    for (let i = 0; i < 3; i++) {
      pools[`Hold${i}`] = this.pool({ idleTimeout: 5000, residentGroups: { vram: 2 } });
    }
    // Nine that do cycle. 3 x 2 + 9 x 2 = 24, so the group starts saturated.
    for (let i = 0; i < 9; i++) {
      pools[`Small${i}`] = this.pool({
        idleTimeout: 40,
        idleCheckIntervalMs: 15,
        residentGroups: { vram: 2 }
      });
    }
    // A cheap pool with NO worker yet, added while the group is full so it
    // queues. Once the Smalls idle out it would fit several times over, which
    // is the whole point: it must still not overtake. A task sent to an
    // already-warm pool costs nothing and would never queue at all.
    pools.Cheap = this.pool({ idleTimeout: 5000, residentGroups: { vram: 2 } });

    const scheduler = this.track(new FyflowScheduler(pools, { vram }));
    const seen = this.record(scheduler);

    // Fill the group first, so both waiters genuinely cannot fit
    await Promise.all([
      ...Array.from({ length: 3 }, (_, i) =>
        scheduler.addTask(this.task(`hold-${i}`, `Hold${i}`), { createPromise: true })
      ),
      ...Array.from({ length: 9 }, (_, i) =>
        scheduler.addTask(this.task(`fill-${i}`, `Small${i}`), { createPromise: true })
      )
    ]);

    // Fire and forget: neither can run yet, so awaiting them would hang
    scheduler.addTask(this.task('big-1', 'Big'));
    scheduler.addTask(this.task('cheap-behind', 'Cheap'));

    let freeWhileQueued = 0;
    const timer = setInterval(() => {
      freeWhileQueued = Math.max(freeWhileQueued, vram.limit - vram.running);
      observe(scheduler, seen);
    }, 10);
    try {
      // Rounds of churn separated by idle gaps. The gaps matter: every settle
      // calls _schedulePeriodicRetry, which CLEARS the pending timer and starts
      // a new one, so a continuously busy scheduler resets the 50ms retry
      // forever and `_retryBlockedTasksForGroup` is never reached at all. An
      // earlier version of this scenario churned without pausing and passed
      // while testing nothing.
      for (let round = 1; round <= 3; round++) {
        await Promise.all(
          Array.from({ length: 9 }, (_, i) =>
            scheduler.addTask(this.task(`churn-${round}-${i}`, `Small${i}`), { createPromise: true })
          )
        );
        // Longer than idleTimeout + idleCheckIntervalMs and than the retry
        // interval, so workers are reaped AND the retry actually fires
        await new Promise(resolve => setTimeout(resolve, 150));
      }
    } finally {
      clearInterval(timer);
    }

    // Head-of-line only means anything if the cheap waiter could have gone
    if (freeWhileQueued < 2) {
      throw new Error(
        `The group never had room for the cheap waiter (peak ${freeWhileQueued} free), ` +
        `so nothing could have overtaken and the scenario proves nothing`
      );
    }

    return { scheduler, seen };
  }

  /**
   * The head of a contended resident group is the whole explanation for a
   * stall, so it has to stay legible while cheaper work churns past it.
   */
  private async testHeadOfLine(): Promise<void> {
    let samples = 0;
    let sawSomeoneBehind = false;
    const problems: string[] = [];

    const { scheduler } = await this.runHeadOfLineScenario(sched => {
      const queue = sched.getAdmissionQueue('vram').vram;
      if (!queue || queue.length === 0) return;

      samples++;
      if (queue.length > 1) sawSomeoneBehind = true;

      const head = queue[0];
      if (head.holderId !== 'big-1') problems.push(`head was ${head.holderId}`);
      if (head.cost !== 20) problems.push(`head reported cost ${head.cost}`);
      if (head.lifetime !== 'resident') problems.push(`head reported ${head.lifetime}`);
      queue.forEach((waiter, index) => {
        if (waiter.position !== index) {
          problems.push(`${waiter.holderId} reports position ${waiter.position} at index ${index}`);
        }
      });
    });

    if (samples === 0) {
      throw new Error('The queue was never non-empty, so head-of-line was never observed');
    }
    if (!sawSomeoneBehind) {
      throw new Error('Nothing ever queued behind the head, so overtaking was untestable');
    }
    if (problems.length > 0) {
      throw new Error(`Head-of-line broken: ${[...new Set(problems)].join(', ')}`);
    }

    const expensive = scheduler.tasks.get('big-1');
    if (expensive?.state !== 'pending') {
      throw new Error(`The expensive task reached state "${expensive?.state}" - it should still be waiting`);
    }
    // The point of head-of-line: the cheap waiter fits every time a Small is
    // reaped, and still must not go first
    const cheap = scheduler.tasks.get('cheap-behind');
    if (cheap?.state !== 'pending') {
      throw new Error(
        `The cheap waiter reached "${cheap?.state}" - it overtook the 20-unit head, ` +
        `which is how an expensive pool starves forever`
      );
    }
  }

  /**
   * The two routes to the same fact, compared.
   *
   * A fold that misses an emit stays internally consistent while being wrong,
   * and an accessor is right but has no history - neither can detect the drift,
   * so only the comparison can. In particular it catches an `unblocked` tied to
   * the array operation rather than to the logical move to the ready queue.
   *
   * Sampled between dispatch passes with the queue **non-empty**. Defining
   * quiescent as "drained" here would compare two empty maps and pass forever.
   */
  private async testFoldMatchesAccessor(): Promise<void> {
    const mismatches: string[] = [];
    let comparisons = 0;

    const compare = (scheduler: FyflowScheduler, seen: Recorded[]) => {
      // Both reads are synchronous and back to back, and listeners only run
      // during a dispatch, so these describe one instant
      const admission = scheduler.getAdmissionQueue();
      const { queues } = this.foldMembership(seen);

      const nonEmpty = Object.entries(admission).filter(([, waiters]) => waiters.length > 0);
      if (nonEmpty.length === 0) return; // Nothing waiting: the check would be vacuous
      comparisons++;

      for (const [groupId, waiters] of nonEmpty) {
        // Every group here is non-keyed, so it has exactly one queue
        const folded = queues.get(bucketKey(groupId)) ?? [];
        const fromAccessor = waiters.map(w => w.holderId);
        if (folded.join(',') !== fromAccessor.join(',')) {
          mismatches.push(`${groupId}: fold [${folded}] vs accessor [${fromAccessor}]`);
        }
      }
    };

    const { scheduler, seen } = await this.runHeadOfLineScenario(compare);
    // And once more over the settled queue, which is still non-empty
    compare(scheduler, seen);

    if (comparisons === 0) {
      throw new Error('Never compared with a non-empty queue - the check would have passed vacuously');
    }
    if (mismatches.length > 0) {
      throw new Error(`The fold and the accessor disagree: ${[...new Set(mismatches)].join(' | ')}`);
    }

    // Conserved across shutdown too, which is what closes the queue fold
    await scheduler.shutdown();
    const { queues, unmatched } = this.foldMembership(seen);
    if (unmatched.length > 0) throw new Error(`Unmatched exits: ${unmatched.join(', ')}`);
    for (const [queueId, members] of queues) {
      if (members.length !== 0) throw new Error(`${queueId} did not drain across shutdown`);
    }
  }

  /**
   * Blocked queue ids join a group id and a key with NUL, which renders as a
   * space in most tools - so this group id contains a real space, which a
   * space-split would silently truncate.
   */
  private async testAdmissionQueueKeying(): Promise<void> {
    const api = new KeyedRateLimitGroup([{ limit: 1, windowMs: 3000 }], { id: 'api pool' });
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ maxConcurrentTasks: 4, groups: ['api pool'] }) },
      { 'api pool': api }
    ));

    // One per key runs; the rest queue in their own buckets
    const queued: Array<[string, string]> = [
      ['k-a1', 'alpha'], ['k-a2', 'alpha'], ['k-a3', 'alpha'],
      ['k-b1', 'beta'], ['k-b2', 'beta']
    ];
    for (const [id, key] of queued) {
      scheduler.addTask(this.task(id, 'A', { limitKey: key }));
    }

    await this.waitFor(
      () => (scheduler.getAdmissionQueue()['api pool']?.length ?? 0) >= 3,
      3000,
      'the per-key queues to fill'
    );

    const admission = scheduler.getAdmissionQueue();
    const groupIds = Object.keys(admission);
    if (!groupIds.includes('api pool')) {
      throw new Error(
        `Expected the group id "api pool" intact, got [${groupIds.join(', ')}] - ` +
        `splitting the queue id on a space would truncate it and mis-key every keyed group`
      );
    }

    const waiters = admission['api pool'];
    for (const waiter of waiters) {
      if (waiter.key !== 'alpha' && waiter.key !== 'beta') {
        throw new Error(`Waiter ${waiter.holderId} carries key "${waiter.key}"`);
      }
      if (waiter.lifetime !== 'task-held' || waiter.cost !== 1) {
        throw new Error(`Expected task-held/1, got ${waiter.lifetime}/${waiter.cost}`);
      }
    }

    const alpha = waiters.filter((w: AdmissionWaiter) => w.key === 'alpha');
    const beta = waiters.filter((w: AdmissionWaiter) => w.key === 'beta');
    if (alpha.length === 0 || beta.length === 0) {
      throw new Error(`Both buckets should be represented: alpha=${alpha.length} beta=${beta.length}`);
    }
    // Sub-queues are concatenated in key order, each numbered from 0, so the
    // result is reproducible rather than depending on map insertion order
    if (waiters.findIndex((w: AdmissionWaiter) => w.key === 'alpha') >
        waiters.findIndex((w: AdmissionWaiter) => w.key === 'beta')) {
      throw new Error('Sub-queues are not concatenated in key order');
    }
    alpha.forEach((waiter: AdmissionWaiter, index: number) => {
      if (waiter.position !== index) {
        throw new Error(`alpha waiter ${index} reports position ${waiter.position}`);
      }
    });
    beta.forEach((waiter: AdmissionWaiter, index: number) => {
      if (waiter.position !== index) {
        throw new Error(`beta waiter ${index} reports position ${waiter.position}`);
      }
    });
  }

  /** The `groupId` argument narrows the walk without changing the shape. */
  private async testAdmissionQueueFilter(): Promise<void> {
    const slow = new RateLimitGroup([{ limit: 1, windowMs: 3000 }], 'slow');
    const idle = new ConcurrentLimitGroup(8, 'idle');
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ maxConcurrentTasks: 4, groups: ['slow', 'idle'] }) },
      { slow, idle }
    ));

    for (let i = 0; i < 3; i++) scheduler.addTask(this.task(`f-${i}`, 'A'));

    await this.waitFor(
      () => (scheduler.getAdmissionQueue().slow?.length ?? 0) > 0,
      3000,
      'the slow group to back up'
    );

    const all = scheduler.getAdmissionQueue();
    const filtered = scheduler.getAdmissionQueue('slow');
    if (JSON.stringify(filtered.slow) !== JSON.stringify(all.slow)) {
      throw new Error('Filtering changed the waiters, not just which groups are listed');
    }
    if (Object.keys(filtered).length !== 1) {
      throw new Error(`Filtered result lists [${Object.keys(filtered)}], expected only "slow"`);
    }
    // A group with an empty queue is absent, not present-and-empty
    if ('idle' in all) {
      throw new Error('An empty group appeared in the admission queue');
    }
    if (Object.keys(scheduler.getAdmissionQueue('nope')).length !== 0) {
      throw new Error('An unknown group id returned something');
    }
  }

  /**
   * The periodic retry is a heartbeat, not a debounce.
   *
   * It is armed from every settle, and it used to `clearTimeout` and restart on
   * each call - so a scheduler completing tasks faster than
   * `periodicRetryIntervalMs` pushed the deadline out forever and it never
   * fired. Measured at zero calls across 600ms while 156 tasks completed.
   *
   * A rate-limit window rolling over is the sharpest case, because nothing else
   * can wake those tasks: a task-held group is retried directly when its own
   * slot frees, but a window expiring raises no event. The busy pool declares
   * no groups, so its settles retry nothing on the rated pool's behalf.
   */
  private async testRetryHeartbeatSurvivesLoad(): Promise<void> {
    const api = new RateLimitGroup([{ limit: 1, windowMs: 60 }], 'api');
    const scheduler = this.track(new FyflowScheduler(
      {
        Busy: this.pool({ maxConcurrentTasks: 4, idleTimeout: 5000 }),
        Rated: this.pool({ maxConcurrentTasks: 4, idleTimeout: 5000, groups: ['api'] })
      },
      { api }
    ));

    const done = new Set<string>();
    scheduler.addEventListener('task.completed', (e: any) => done.add(e.detail.id));

    // Fire and forget: under the bug these never run, so awaiting them hangs
    for (const id of ['rated-1', 'rated-2', 'rated-3']) {
      scheduler.addTask(this.task(id, 'Rated', {}));
    }

    // Keep the scheduler continuously busy on a pool that shares no groups
    const start = performance.now();
    let round = 0;
    while (performance.now() - start < 600) {
      round++;
      await Promise.all(
        Array.from({ length: 4 }, (_, i) =>
          scheduler.addTask(this.task(`busy-${round}-${i}`, 'Busy'), { createPromise: true })
        )
      );
    }

    const ran = ['rated-1', 'rated-2', 'rated-3'].filter(id => done.has(id));
    if (ran.length !== 3) {
      throw new Error(
        `Only ${ran.length}/3 rate-limited tasks ran in 600ms across ten 60ms windows ` +
        `(${[...done].filter(id => id.startsWith('busy')).length} busy tasks completed meanwhile) - ` +
        `the periodic retry is being starved by load`
      );
    }
    const stillWaiting = scheduler.getAdmissionQueue().api ?? [];
    if (stillWaiting.length !== 0) {
      throw new Error(`${stillWaiting.length} task(s) still queued on api after the run`);
    }
    // The busy pool must not have been starved in exchange
    if ([...done].filter(id => id.startsWith('busy')).length < 20) {
      throw new Error('The busy pool barely ran - the fix traded one starvation for another');
    }
  }

  /**
   * Exit (c): a blocked waiter whose worker pool no longer exists.
   *
   * It used to be shifted out of the queue and dropped - no `task.failed`, no
   * `user_action`, no rejection, and no `stats` entry. A caller awaiting it
   * waited forever, and a consumer folding the stream held it as pending with
   * nothing to age it out. It now fails terminally.
   *
   * `addTask` validates worker types and the library never mutates
   * `workerPools`, so reaching the branch means mutating it from outside. That
   * is the point: the branch exists for a state that should be impossible, and
   * it used to be silent when it was not.
   */
  private async testOrphanedWaiterFails(): Promise<void> {
    const cpu = new ConcurrentLimitGroup(1, 'cpu');
    const scheduler = this.track(new FyflowScheduler(
      { A: this.pool({ maxConcurrentTasks: 2, groups: ['cpu'], idleTimeout: 5000 }) },
      { cpu }
    ));
    const seen = this.record(scheduler);

    const failed: string[] = [];
    const userAction: string[] = [];
    let drained = false;
    scheduler.addEventListener('task.failed', (e: any) => failed.push(e.detail.id));
    scheduler.addEventListener('task.user_action', (e: any) => userAction.push(e.detail.id));
    scheduler.addEventListener('scheduler.completed', () => { drained = true; });

    // The first holds the only cpu slot long enough to set the trap; the second
    // queues behind it
    const first = scheduler.addTask(new FyflowTask({
      id: 'orph-1', workerType: 'A', payload: { taskId: 'orph-1', delay: 300 }
    }), { createPromise: true }) as Promise<any>;
    const second = scheduler.addTask(this.task('orph-2', 'A'), { createPromise: true }) as Promise<any>;

    await this.waitFor(
      () => (scheduler.getAdmissionQueue().cpu?.length ?? 0) > 0,
      2000,
      'orph-2 to block on cpu'
    );

    // The pool disappears while orph-2 is queued. Settling orph-1 releases cpu
    // and retries the queue, which is where the waiter meets a missing pool.
    const pool = (scheduler as any).workerPools.A;
    delete (scheduler as any).workerPools.A;

    await first;

    // Bounded on purpose. The bug this covers made the task settle NEVER, so an
    // unbounded await would hang the suite instead of failing it - the failure
    // mode and the missing-assertion mode would look identical from CI.
    let rejection: Error | undefined;
    let settled = false;
    await Promise.race([
      second.then(
        () => { settled = true; },
        (error: Error) => { settled = true; rejection = error; }
      ),
      new Promise(resolve => setTimeout(resolve, 2000))
    ]);

    // Put it back so cleanup can terminate the workers it still owns
    (scheduler as any).workerPools.A = pool;

    if (!settled) {
      throw new Error(
        'The orphaned task never settled in 2s - it was dropped from its queue ' +
        'and is now a phantom nothing can resolve or age out'
      );
    }
    if (!rejection) throw new Error('The orphaned task resolved instead of failing');
    if (!rejection.message.includes('orph-2') || !rejection.message.includes('"A"')) {
      throw new Error(`Rejection does not name the task and its pool: ${rejection.message}`);
    }

    const task = scheduler.tasks.get('orph-2');
    if (task?.state !== 'user_action') {
      throw new Error(`Expected state "user_action", got "${task?.state}"`);
    }
    if (!failed.includes('orph-2')) throw new Error('No task.failed for the orphaned task');
    if (!userAction.includes('orph-2')) throw new Error('No task.user_action for the orphaned task');

    // stats must account for it, and must not have stolen a running count from
    // somewhere else - a blocked task was never running
    if (scheduler.stats.failed !== 1 || scheduler.stats.done !== 1) {
      throw new Error(`Expected done=1 failed=1, got ${JSON.stringify(scheduler.stats)}`);
    }
    if (scheduler.stats.running !== 0 || scheduler.stats.queued !== 0) {
      throw new Error(`Expected the scheduler idle, got ${JSON.stringify(scheduler.stats)}`);
    }

    // Nothing left waiting, and the scheduler is free to report completion -
    // a retried orphan would sit in a ready queue no dispatch can drain
    if (Object.keys(scheduler.getAdmissionQueue()).length !== 0) {
      throw new Error(`Still waiting: ${JSON.stringify(scheduler.getAdmissionQueue())}`);
    }
    await this.waitFor(() => drained, 1000, 'scheduler.completed after the orphan failed');

    const exit = seen.find(r => r.name === 'unblocked' && r.detail.holderId === 'orph-2');
    if (!exit) throw new Error('No unblocked event for the orphaned waiter');
    if ((exit.detail as ResourceUnblockedDetail).reason !== 'orphaned') {
      throw new Error(`Exit reason is "${(exit.detail as ResourceUnblockedDetail).reason}"`);
    }

    const { queues, unmatched } = this.foldMembership(seen);
    if (unmatched.length > 0) throw new Error(`Unmatched exits: ${unmatched.join(', ')}`);
    for (const [queueId, members] of queues) {
      if (members.length !== 0) throw new Error(`${queueId} did not drain`);
    }
  }

  // --- S2 ------------------------------------------------------------------

  /** The five events that had no timestamp now carry one, as `Date.now()`. */
  private async testTimestamps(): Promise<void> {
    const scheduler = this.track(new FyflowScheduler({ A: this.pool() }, {}));

    const stamps: Record<string, number | undefined> = {};
    for (const name of ['task.running', 'task.completed', 'task.failed', 'task.user_action']) {
      scheduler.addEventListener(name, (e: any) => { stamps[name] = e.detail.timestamp; });
    }
    scheduler.addEventListener('scheduler.completed', (e: any) => {
      stamps['scheduler.completed'] = e.detail.timestamp;
      // The detail is a copy, not the live counters object
      if (e.detail === scheduler.stats) {
        throw new Error('scheduler.completed still hands out the live stats object');
      }
    });

    const before = Date.now();
    await scheduler.addTask(this.task('ts-ok', 'A'), { createPromise: true });
    const failing = scheduler.addTask(
      new FyflowTask({ id: 'ts-bad', workerType: 'A', payload: { shouldThrow: true } }),
      { createPromise: true }
    ) as Promise<any>;
    await failing.catch(() => { /* expected - this is what task.user_action is for */ });

    await this.waitFor(() => stamps['scheduler.completed'] !== undefined, 2000, 'the scheduler to drain');
    const after = Date.now();

    for (const name of [
      'task.running', 'task.completed', 'task.failed', 'task.user_action', 'scheduler.completed'
    ]) {
      const stamp = stamps[name];
      if (typeof stamp !== 'number') throw new Error(`${name} carried no timestamp`);
      if (stamp < before || stamp > after) {
        throw new Error(`${name} stamped ${stamp}, outside the run window ${before}..${after}`);
      }
    }
  }
}

export default ResourceEventsTestSuite;

// Auto-run tests when executed directly - handle both Deno and Node.js
if ((typeof Deno !== 'undefined' && import.meta.main) ||
    (typeof process !== 'undefined' && process.argv[1] && import.meta.url &&
     import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/')))) {
  const testSuite = new ResourceEventsTestSuite();
  try {
    await testSuite.runAllTests(true);
  } finally {
    await testSuite.cleanup();
  }
}
