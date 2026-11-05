// A worker for the grants suite, the same on inline and threaded pools.
//
// It reports what it was granted:
// - the constructor and setup() record `workerContext.grants`
// - run() returns { task, ctor, setup, workerId }, with `task` the
//   attempt's `context.grants`
//
// payload:
// - holdMs: wait this long before returning, so units stay held
// - failFirst: throw on this task id's first attempt in this worker module -
//   a retry then lands on a fresh grant
// - honour: wait until the signal aborts (for cancellation), up to holdMs

import type { BaseWorkerContext, TaskWorkerContext } from '../../core/workerInterface.ts';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const failed = new Set<string>();

export default class GrantWorker {
  private ctor: unknown;
  private setupGrants: unknown;
  private workerContext?: BaseWorkerContext;

  constructor(_config: any = {}, workerContext?: BaseWorkerContext) {
    this.workerContext = workerContext;
    this.ctor = workerContext?.grants;
  }

  async setup() {
    this.setupGrants = this.workerContext?.grants;
  }

  async run(payload: any, context: TaskWorkerContext) {
    const { holdMs = 0, failFirst = false, honour = false } = payload ?? {};
    if (failFirst && !failed.has(context.taskId)) {
      failed.add(context.taskId);
      throw new Error('first attempt fails on purpose');
    }
    if (honour) {
      await new Promise<void>(resolve => {
        const timer = setTimeout(resolve, holdMs);
        context.signal.addEventListener('abort', () => { clearTimeout(timer); resolve(); });
      });
      context.signal.throwIfAborted();
    } else if (holdMs > 0) {
      await sleep(holdMs);
    }
    return {
      task: context.grants,
      ctor: this.ctor,
      setup: this.setupGrants,
      workerId: context.workerId,
      frozen: Object.isFrozen(context.grants)
    };
  }
}
