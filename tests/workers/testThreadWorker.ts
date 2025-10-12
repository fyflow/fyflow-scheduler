import { BaseWorker, WorkerConfig } from "../../core/workerInterface.ts";

/**
 * Test Thread Worker - Dedicated worker file for test suite threaded execution
 */
export default class TestThreadWorker extends BaseWorker {
  private delay: number;

  constructor(config: WorkerConfig = {}) {
    super(config);
    this.delay = config.delay || 100;
  }

  async setup() {
    // console.log('TestThreadWorker setup completed');
  }

  async run(payload: any) {
    // Handle error testing
    if (payload.shouldThrow) {
      throw new Error('Test worker intentional error');
    }

    const delay = payload.delay || this.delay;
    await new Promise(resolve => setTimeout(resolve, delay));
    return {
      workerId: 'thread',
      taskId: payload.taskId,
      data: payload.data,
      message: payload.message,
      index: payload.index,
      processedAt: new Date().toISOString(),
      delay
    };
  }

  async teardown() {
    // console.log('TestThreadWorker teardown completed');
  }
}