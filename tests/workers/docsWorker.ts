// Worker used by the documentation examples in tests/suites/docs.ts.
// Kept deliberately close to the snippet published in README.md / AGENTS.md.

import { BaseWorker, WorkerConfig, BaseWorkerContext, TaskWorkerContext } from '../../index.ts';

export default class DocsWorker extends BaseWorker {
  private multiplier: number;

  // The wrapper calls `new Worker(config, workerContext)`. Forward BOTH to super:
  // dropping the second argument leaves `this.workerContext` undefined, and
  // terminateWithError() then silently does nothing.
  constructor(config: WorkerConfig = {}, workerContext?: BaseWorkerContext) {
    super(config, workerContext);
    // Worker-level config comes from WorkerManager's `config` option
    this.multiplier = (config as any).multiplier ?? 1;
  }

  // setup/teardown are abstract on BaseWorker - implement both, even if empty
  async setup(): Promise<void> {}
  async teardown(): Promise<void> {}

  async run(payload: any, context?: TaskWorkerContext): Promise<any> {
    // Lets a test observe overlapping awaits
    if (payload.awaitMs) {
      await new Promise(resolve => setTimeout(resolve, payload.awaitMs));
    }

    if (payload.fail) {
      throw new Error('DocsWorker was asked to fail');
    }

    if (payload.reportProgress) {
      const steps = payload.reportProgress as number;
      for (let i = 0; i < steps; i++) {
        // progress is 0-1, not a percentage
        context?.sendProgress((i + 1) / steps, `step ${i + 1}/${steps}`, { step: i });
      }
    }

    if (payload.spawn) {
      for (let i = 0; i < payload.spawn; i++) {
        context?.spawnTask({
          id: `${payload.id}-child-${i}`,
          workerType: 'DocsWorker',
          payload: { id: `${payload.id}-child-${i}`, value: 1 }
        });
      }
    }

    if (payload.selfTerminate) {
      // Worker-level context, available via the constructor, not the task context
      this.workerContext?.terminateWithError(
        new Error('DocsWorker self-terminated'),
        { canRestart: true }
      );
    }

    return { id: payload.id, value: (payload.value ?? 0) * this.multiplier };
  }
}
