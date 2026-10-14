// A worker for the cancellation suite, the same on inline and threaded pools.
//
// It reports through sendProgress, which reaches the scheduler as task.progress
// on both pool types: 'entered' as run() starts - the only reliable sign that
// it has, since state 'running' is set at dispatch - and 'stopped' with what it
// saw on its signal, so a test can check the in-thread reason too.
//
// payload.mode:
// - 'honour' (default): waits until the signal aborts, or `delay` passes
// - 'ignore': waits `delay`, never looking at the signal, then returns a result
// - 'throw': waits `delay`, never looking at the signal, then throws
// - 'terminate': calls terminateWithError() after `delay`, failing its worker

import type { TaskWorkerContext } from '../../core/workerInterface.ts';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export default class CancelWorker {
  async run(payload: any, context: TaskWorkerContext) {
    const { mode = 'honour', delay = 5000 } = payload;
    context.sendProgress(0, 'entered');

    if (mode === 'ignore') {
      await sleep(delay);
      return { ignored: true };
    }

    if (mode === 'throw') {
      await sleep(delay);
      throw new Error('Failed after the cancel');
    }

    if (mode === 'terminate') {
      await sleep(delay);
      context.terminateWithError(new Error('Co-tenant asked for its worker to go'));
      await sleep(5000);
      return { terminated: true };
    }

    const signal = context.signal;
    await new Promise<void>(resolve => {
      const timer = setTimeout(resolve, delay);
      if (signal.aborted) return resolve();
      signal.addEventListener('abort', () => {
        // Synchronously, from the handler: on an inline pool this shows whether
        // the abort ran inside the host's own cancel() call
        context.sendProgress(0.5, 'abort-handler');
        clearTimeout(timer);
        resolve();
      }, { once: true });
    });
    const reason = signal.reason;
    context.sendProgress(1, 'stopped', {
      aborted: signal.aborted,
      reasonName: typeof reason === 'object' && reason !== null ? (reason as any).name : undefined,
      reason: typeof reason === 'string' ? reason : undefined
    });
    return { aborted: signal.aborted };
  }
}
