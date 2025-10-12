import { BaseWorker, WorkerConfig } from "../../core/workerInterface.ts";

/**
 * Test Inline Worker - Dedicated worker file for test suite inline execution
 */
export default class TestInlineWorker extends BaseWorker {
  private delay: number;

  constructor(config: WorkerConfig = {}) {
    super(config);
    this.delay = config.delay || 50;
  }

  async setup() {
    // console.log('TestInlineWorker setup completed');
  }

  async run(payload: any) {
    // Handle error testing
    if (payload.shouldThrow) {
      throw new Error('Test worker intentional error');
    }

    const delay = payload.delay || this.delay;
    await new Promise(resolve => setTimeout(resolve, delay));
    return {
      workerId: 'inline',
      taskId: payload.taskId,
      data: payload.data,
      message: payload.message,
      processedAt: new Date().toISOString(),
      delay
    };
  }

  async teardown() {
    // console.log('TestInlineWorker teardown completed');
  }
}