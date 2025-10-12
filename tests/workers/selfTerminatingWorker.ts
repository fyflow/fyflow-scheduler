// Test worker that can self-terminate using the terminateWithError API

import { BaseWorker, BaseWorkerContext, TaskWorkerContext, WorkerTerminationError } from '../../core/workerInterface.ts';

export default class SelfTerminatingWorker extends BaseWorker {
  private terminationTimer: number | null = null;
  private backgroundMonitoringInterval: number | null = null;
  private taskCounter = 0;

  constructor(config: any = {}, workerContext?: BaseWorkerContext) {
    super(config, workerContext);

    // console.log('SelfTerminatingWorker initialized');

    // Set up background monitoring that could trigger self-termination
    this.setupBackgroundMonitoring();
  }

  private setupBackgroundMonitoring(): void {
    // Simulate background monitoring (e.g., database connection, memory usage)
    this.backgroundMonitoringInterval = setInterval(() => {
      // Simulate detecting a problematic condition
      if (Math.random() < 0.001) { // Very low probability for normal operations
        this.workerContext?.terminateWithError(
          new Error('Background monitoring detected critical issue'),
          { canRestart: true, restartDelay: 1000 }
        );
      }
    }, 1000);
  }

  async setup(): Promise<void> {
    // console.log('SelfTerminatingWorker setup started');

    // Check if we should terminate during setup
    if (this.config.terminateInSetup) {
      this.workerContext?.terminateWithError(
        new Error('Intentional termination during setup'),
        { canRestart: this.config.canRestart, restartDelay: this.config.restartDelay }
      );
      return;
    }

    // console.log('SelfTerminatingWorker setup completed');
  }

  async run(payload: any, context?: TaskWorkerContext): Promise<any> {
    const { action, canRestart, restartDelay, delay, taskCount } = payload;
    this.taskCounter++;

    // console.log(`SelfTerminatingWorker executing action: ${action} (task #${this.taskCounter})`);

    switch (action) {
      case 'self-terminate':
        // Immediate self-termination - throw WorkerTerminationError to mark worker as failed
        throw new WorkerTerminationError('Worker requested self-termination', {
          canRestart: canRestart !== false,
          restartDelay: restartDelay || 1000
        });

      case 'self-terminate-delayed':
        // Self-terminate after a delay
        // console.log('📡 self-terminate-delayed will terminate in', delay || 100);
        setTimeout(() => {
          // console.log('📡 self-terminate-delayed terminating worker.');
          context?.terminateWithError(
            new Error('Worker self-terminated after delay'),
            { canRestart: true, restartDelay: 100 }
          );
        }, delay || 100);

        // Keep the task running until termination
        // console.log('📡 self-terminate-delayed starting work');
        await new Promise(resolve => setTimeout(resolve, (delay || 100) + 50));
        // console.log('⚠️⚠️⚠️⚠️⚠️ self-terminate-delayed completed work');
        return { result: 'Task completed before termination' };

      case 'terminate-in-setup':
        // This should have been handled in setup
        throw new Error('This should have been handled in setup');

      case 'self-terminate-after-tasks':
        // Terminate after processing a certain number of tasks
        if (this.taskCounter >= (taskCount || 3)) {
          context?.terminateWithError(
            new Error(`Worker terminating after ${this.taskCounter} tasks`),
            { canRestart: false }
          );
        }

        await new Promise(resolve => setTimeout(resolve, 100));
        return {
          result: `Task ${this.taskCounter} completed`,
          processedAt: new Date().toISOString()
        };

      case 'conditional-terminate':
        // Terminate based on payload condition
        if (payload.shouldTerminate) {
          context?.terminateWithError(
            new Error('Conditional termination triggered'),
            { canRestart: payload.canRestart || false }
          );
        }

        await new Promise(resolve => setTimeout(resolve, 50));
        return {
          result: 'Conditional task completed',
          processedAt: new Date().toISOString()
        };

      case 'normal-task':
        // Normal task execution
        await new Promise(resolve => setTimeout(resolve, 50));
        return {
          result: 'Normal task completed successfully',
          taskNumber: this.taskCounter,
          processedAt: new Date().toISOString()
        };

      case 'memory-leak-detection':
        // Simulate detecting a memory leak and self-terminating
        setTimeout(() => {
          context?.terminateWithError(
            new Error('Memory leak detected, terminating worker'),
            { canRestart: true, restartDelay: 2000 }
          );
        }, 50);

        await new Promise(resolve => setTimeout(resolve, 100));
        return { result: 'Memory monitoring task completed' };

      case 'database-connection-lost':
        // Simulate database connection loss
        context?.terminateWithError(
          new Error('Database connection lost'),
          { canRestart: true, restartDelay: 5000 }
        );
        return { result: 'This should not be returned' };

      case 'worker-corruption-detected':
        // Simulate worker internal state corruption
        context?.terminateWithError(
          new Error('Worker internal state corrupted'),
          { canRestart: false } // Don't restart corrupted workers
        );
        return { result: 'This should not be returned' };

      default:
        await new Promise(resolve => setTimeout(resolve, 50));
        return {
          result: `Unknown action processed: ${action}`,
          taskNumber: this.taskCounter,
          processedAt: new Date().toISOString()
        };
    }
  }

  async teardown(): Promise<void> {
    if (this.terminationTimer) {
      clearTimeout(this.terminationTimer);
    }
    if (this.backgroundMonitoringInterval) {
      clearInterval(this.backgroundMonitoringInterval);
    }
    // console.log('SelfTerminatingWorker teardown completed');
  }
}

// Web worker mode - handles both inline and threaded execution
if (typeof self !== 'undefined' && 'postMessage' in self) {
  // This code runs in the worker thread
  // The workerWrapper will handle the actual instantiation and lifecycle
}