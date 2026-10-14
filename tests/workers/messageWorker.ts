// Test worker for worker -> host messages (context.emit / workerContext.emit).
//
// Config:
//   emitInConstructor, emitInSetup, emitInTeardown: worker-level emits there
//   emitAfterTeardown: teardown() schedules a worker-level emit for later
//   setupThrows: setup() emits (if asked) then throws
//
// Payload `mode`:
//   'emit'        count emits, `yieldEvery` > 0 awaits a timer every N, then
//                 returns - or throws, with `throws`
//   'sync-loop'   count emits in a loop that never yields, `spin` work each
//   'sync-for'    emits in a loop that never yields, for `forMs`
//   'mutate'      emits an object, then mutates it
//   'odd'         a non-string name and an uncloneable value
//   'stray'       emits, returns, and emits again from a timer after settling
//   'worker'      worker-level emits from inside run()
//   'terminate'   emits, then asks for termination through the task context
//   'stream'      emits every `everyMs` for `forMs`, yielding between
//   'stray-throw' emits, schedules another emit 20 ms later, and throws
//   'probe'       emits, then records on globalThis that emit() has returned

import { BaseWorker } from '../../core/workerInterface.ts';
import type { BaseWorkerContext, TaskWorkerContext } from '../../core/workerInterface.ts';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const spin = (n: number) => { let x = 0; for (let i = 0; i < n; i++) x += Math.sqrt(i); return x; };

export default class MessageWorker extends BaseWorker {
  constructor(config: any = {}, workerContext?: BaseWorkerContext) {
    super(config, workerContext);
    if (config.emitInConstructor) workerContext?.emit('constructed', { at: 'constructor' });
  }

  async setup(): Promise<void> {
    if (this.config.emitInSetup) this.workerContext?.emit('loading', { stage: 'weights', fraction: 0.4 });
    if (this.config.setupThrows) throw new Error('setup failed on purpose');
  }

  async teardown(): Promise<void> {
    if (this.config.emitInTeardown) this.workerContext?.emit('closing', { at: 'teardown' });
    if (this.config.emitAfterTeardown) setTimeout(() => this.workerContext?.emit('late', {}), 10);
  }

  async run(payload: any, context?: TaskWorkerContext): Promise<any> {
    const emit = context!.emit;
    switch (payload.mode) {
      case 'emit': {
        for (let i = 0; i < payload.count; i++) {
          emit('log', { i });
          if (payload.yieldEvery && (i + 1) % payload.yieldEvery === 0) await sleep(payload.yieldMs ?? 1);
        }
        if (payload.throws) throw new Error('failed after emitting');
        return { emitted: payload.count };
      }
      case 'sync-loop': {
        const start = performance.now();
        for (let i = 0; i < payload.count; i++) {
          spin(payload.spin ?? 0);
          emit('tick', { i });
        }
        return { emitted: payload.count, ms: performance.now() - start };
      }
      case 'sync-for': {
        const until = performance.now() + payload.forMs;
        let i = 0;
        while (performance.now() < until) emit('tick', { i: i++ });
        return { emitted: i };
      }
      case 'mutate': {
        // A first emit is sent at once; the next waits in the buffer, which is
        // where a mutation could reach it if the clone were taken later
        emit('warm', {});
        const entry = { message: 'before' };
        emit('log', entry);
        entry.message = 'after';
        return 'mutated';
      }
      case 'odd': {
        emit(42 as any, { ok: true });
        emit('fn', { callback: () => 1 });
        emit('after', { ok: true });
        return 'odd';
      }
      case 'stray': {
        emit('log', { phase: 'running' });
        setTimeout(() => emit('log', { phase: 'after-settle' }), 20);
        return 'stray';
      }
      case 'stray-throw': {
        emit('log', { phase: 'attempt-1' });
        setTimeout(() => emit('log', { phase: 'stray-from-attempt-1' }), 20);
        throw new Error('first attempt fails');
      }
      case 'probe': {
        // Past the flush interval since the last flush (setup's), so this emit
        // is flushed - and, inline, queued for delivery - inside the call
        await sleep(payload.waitMs ?? 50);
        (globalThis as any).__fyflowEmitReturned = false;
        emit('probe', {});
        (globalThis as any).__fyflowEmitReturned = true;
        return 'probed';
      }
      case 'worker': {
        this.workerContext?.emit('cache', { hits: 12 });
        emit('log', { phase: 'task' });
        return 'worker';
      }
      case 'terminate': {
        emit('log', { phase: 'before-terminate' });
        context!.terminateWithError(new Error('terminating on purpose'), { canRestart: false });
        emit('log', { phase: 'after-terminate' });
        return 'terminated';
      }
      case 'stream': {
        const until = performance.now() + payload.forMs;
        let i = 0;
        while (performance.now() < until) {
          emit('tick', { i: i++ });
          await sleep(payload.everyMs);
        }
        return { emitted: i };
      }
      default:
        throw new Error(`Unknown mode ${payload.mode}`);
    }
  }
}
