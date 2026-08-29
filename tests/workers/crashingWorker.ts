// Test worker that can crash in various ways for error handling tests

import { BaseWorker, BaseWorkerContext, TaskWorkerContext } from '../../core/workerInterface.ts';

export default class CrashingWorker extends BaseWorker {
  private shouldCrashOnInit = false;
  private shouldCrashInSetup = false;

  constructor(config: any = {}, workerContext?: BaseWorkerContext) {
    super(config, workerContext);

    // Check if we should crash during initialization
    if (config.crashOnInit) {
      this.shouldCrashOnInit = true;
      throw new Error('Intentional crash during worker initialization');
    }

    if (config.crashInSetup) {
      this.shouldCrashInSetup = true;
    }
  }

  async setup(): Promise<void> {
    // console.log('CrashingWorker setup started');

    if (this.shouldCrashInSetup) {
      throw new Error('Intentional crash during worker setup');
    }

    // console.log('CrashingWorker setup completed');
  }

  async run(payload: any, context?: TaskWorkerContext): Promise<any> {
    const { action } = payload;

    // console.log(`CrashingWorker executing action: ${action}`);

    switch (action) {
      case 'throw-error':
        throw new Error('Intentional crash during task execution');

      case 'crash-on-init':
        // This should have been handled in constructor, but just in case
        throw new Error('This should have crashed during init');

      case 'crash-in-setup':
        // This should have been handled in setup, but just in case
        throw new Error('This should have crashed during setup');

      case 'normal-task':
        await new Promise(resolve => setTimeout(resolve, 50));
        return {
          result: 'Normal task completed successfully',
          processedAt: new Date().toISOString()
        };

      case 'async-crash':
        // Simulate an async operation that crashes
        await new Promise(resolve => setTimeout(resolve, 10));
        throw new Error('Async operation crashed');

      case 'memory-leak-simulation':
        // Simulate a memory leak scenario (not a real leak, just for testing)
        const bigArray = new Array(100000).fill('memory-test');
        throw new Error('Simulated memory issue after allocation');

      default:
        return {
          result: `Unknown action: ${action}`,
          processedAt: new Date().toISOString()
        };
    }
  }

  async teardown(): Promise<void> {
    // Lets a test observe worker.teardown.failed, which nothing else emits
    if (this.config.crashOnTeardown) {
      throw new Error('Intentional crash during worker teardown');
    }
    // console.log('CrashingWorker teardown completed');
  }
}

// Web worker mode - handles both inline and threaded execution
if (typeof self !== 'undefined' && 'postMessage' in self) {
  // This code runs in the worker thread
  // The workerWrapper will handle the actual instantiation and lifecycle
}