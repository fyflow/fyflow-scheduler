// Test worker for requeue settlement.
//
// run() reports which worker instance ran it, so a test can tell the result of
// a requeued task's live attempt from the result of the attempt it abandoned.
//
// A run on a worker being torn down - the abandoned run, once the pool has
// failed its worker - finishes shortly after teardown() starts, while
// teardown() itself takes longer. That pins the abandoned run's outcome to a
// fixed point after the failure: after the task has been re-dispatched, and
// before a threaded worker's thread is killed, whatever thread startup costs.

import { BaseWorker } from '../../core/workerInterface.ts';
import type { BaseWorkerContext, TaskWorkerContext } from '../../core/workerInterface.ts';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export default class RequeueWorker extends BaseWorker {
  private tearingDown: Promise<void>;
  private startTeardown!: () => void;

  constructor(config: any = {}, workerContext?: BaseWorkerContext) {
    super(config, workerContext);
    this.tearingDown = new Promise(resolve => { this.startTeardown = resolve; });
  }

  async setup(): Promise<void> {}

  async teardown(): Promise<void> {
    this.startTeardown();
    // Keeps a threaded worker's thread alive past the abandoned run's end.
    // Below ThreadWrapper's 500ms acknowledgement timeout, which would kill it.
    await sleep(300);
  }

  async run(payload: any, context?: TaskWorkerContext): Promise<any> {
    await Promise.race([
      this.tearingDown.then(() => sleep(50)),
      sleep(payload.ms ?? 0)
    ]);

    if (payload.report) {
      context?.sendProgress(1, `from ${context.workerId}`);
      context?.spawnTask({
        id: `${context.taskId}-child-${context.workerId}`,
        workerType: payload.spawnWorkerType,
        payload: { ms: 0 }
      });
    }
    if (payload.fail) throw new Error(`failed on ${context?.workerId}`);
    return context?.workerId;
  }
}
