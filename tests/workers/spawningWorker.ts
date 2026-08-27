import { BaseWorker, WorkerConfig, TaskWorkerContext } from "../../core/workerInterface.ts";

/**
 * Test Spawning Worker - spawns tasks at runtime via context.spawnTask()
 *
 * Payload options:
 *   spawn:      number of children to spawn (default 0)
 *   depth:      remaining levels of nesting - children inherit depth - 1
 *   childId:    id prefix for spawned children
 *   shouldThrow: throw instead of completing
 *   spawnUnknownWorker: spawn a task for a worker type the scheduler doesn't have
 *   delay:      ms to wait before returning
 */
export default class SpawningWorker extends BaseWorker {
  constructor(config: WorkerConfig = {}) {
    super(config);
  }

  async setup() {}

  async teardown() {}

  async run(payload: any, context?: TaskWorkerContext) {
    const delay = payload.delay ?? 10;
    if (delay > 0) {
      await new Promise(resolve => setTimeout(resolve, delay));
    }

    if (payload.spawnUnknownWorker) {
      context?.spawnTask({
        id: `${payload.childId || 'child'}-orphan`,
        workerType: 'NoSuchWorkerType',
        payload: {}
      });
    }

    const spawnCount = payload.spawn || 0;
    const depth = payload.depth || 0;
    for (let i = 0; i < spawnCount; i++) {
      const childId = `${payload.childId || 'child'}-${i}`;
      context?.spawnTask({
        id: childId,
        workerType: 'SpawningWorker',
        payload: {
          // Children keep spawning until the requested depth is exhausted
          spawn: depth > 1 ? spawnCount : 0,
          depth: depth - 1,
          childId,
          delay: payload.childDelay ?? 10,
          shouldThrow: payload.childShouldThrow || false
        }
      });
    }

    // Throw only after spawning, so failure and spawning can be tested together
    if (payload.shouldThrow) {
      throw new Error('SpawningWorker intentional error');
    }

    return { id: payload.childId || 'root', spawned: spawnCount };
  }
}
