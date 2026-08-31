# Pause/Resume Implementation Analysis

This document outlines the implementation strategy for adding pause/resume functionality to the FyFlow scheduler without persistence.

## Complexity Assessment: **Low to Medium**

Much simpler than persistence because everything stays in memory - we're just controlling execution flow.

## What We Need to Control

**Scheduler Level:**
- Stop accepting new tasks into `readyQueue`
- Pause the `_dispatchLoop()` from sending tasks to workers
- Pause periodic retries and cleanup timers

**Worker Level:**
- Stop workers from accepting new tasks
- Let currently running tasks finish (or optionally interrupt them)
- Pause worker pool scaling/management

**Resource Groups:**
- Keep resource accounting intact
- Don't release resources during pause (they're still "held")

## State Machine Design

```typescript
type SchedulerState = 'running' | 'pausing' | 'paused' | 'resuming';

interface PauseOptions {
  mode: 'soft' | 'graceful' | 'immediate';
  timeout?: number; // For graceful mode
}
```

## Implementation Strategy

### 1. Core Scheduler Changes

```typescript
export class FyflowScheduler extends EventTarget {
  private state: SchedulerState = 'running';
  private pausePromise?: Promise<void>;

  async pause(options: PauseOptions = { mode: 'soft' }): Promise<void> {
    if (this.state === 'paused') return;
    if (this.state === 'pausing') {
      return this.pausePromise;
    }

    this.state = 'pausing';
    this.dispatchEvent(new CustomEvent('scheduler.pausing', { detail: { mode: options.mode } }));

    this.pausePromise = this._executePause(options);
    await this.pausePromise;

    this.state = 'paused';
    this.dispatchEvent(new CustomEvent('scheduler.paused', {
      detail: {
        queuedTasks: this.readyQueue.length,
        runningTasks: this.stats.running
      }
    }));
  }

  async resume(): Promise<void> {
    if (this.state === 'running') return;
    if (this.state !== 'paused') {
      throw new Error(`Cannot resume from state: ${this.state}`);
    }

    this.state = 'resuming';
    this.dispatchEvent(new CustomEvent('scheduler.resuming'));

    // Resume all worker pools
    const resumePromises = Object.values(this.workerPools).map(
      (pool: any) => pool.resume()
    );
    await Promise.all(resumePromises);

    this.state = 'running';

    // Restart processing
    this._dispatchLoop();
    if (this.readyQueue.length > 0) {
      this._schedulePeriodicRetry();
    }

    this.dispatchEvent(new CustomEvent('scheduler.resumed', {
      detail: {
        queuedTasks: this.readyQueue.length,
        runningTasks: this.stats.running
      }
    }));
  }

  private async _executePause(options: PauseOptions): Promise<void> {
    // Stop periodic retry timer
    this._clearPeriodicRetry();

    switch (options.mode) {
      case 'soft':
        await this._softPause();
        break;
      case 'graceful':
        await this._gracefulPause(options.timeout);
        break;
      case 'immediate':
        await this._immediatePause();
        break;
    }
  }

  private async _softPause(): Promise<void> {
    // Just stop dispatching - let running tasks continue
    const pausePromises = Object.values(this.workerPools).map(
      (pool: any) => pool.pause('soft')
    );
    await Promise.all(pausePromises);
  }

  private async _gracefulPause(timeout = 30000): Promise<void> {
    // Wait for currently dispatched tasks to start or fail
    const dispatchedTasks = Array.from(this.tasks.values())
      .filter(t => t.state === 'dispatched');

    if (dispatchedTasks.length > 0) {
      const waitPromise = this._waitForTasksToStartOrFail(dispatchedTasks);
      const timeoutPromise = new Promise<void>(resolve =>
        setTimeout(resolve, timeout)
      );

      await Promise.race([waitPromise, timeoutPromise]);
    }

    await this._softPause();
  }

  private async _immediatePause(): Promise<void> {
    // Terminate workers and mark running tasks as failed
    const pausePromises = Object.values(this.workerPools).map(
      (pool: any) => pool.pause('immediate')
    );
    await Promise.all(pausePromises);

    // Mark running tasks as failed
    Array.from(this.tasks.values())
      .filter(t => t.state === 'running' || t.state === 'dispatched')
      .forEach(task => {
        task.state = 'failed';
        task.result = new Error('Scheduler paused - task interrupted');
        this.stats.running--;
        this.stats.failed++;
        task.reject?.(task.result);
      });
  }

  private async _waitForTasksToStartOrFail(tasks: FyflowTask[]): Promise<void> {
    return new Promise(resolve => {
      let remaining = tasks.length;

      const checkTask = (task: FyflowTask) => {
        if (task.state === 'running' || task.state === 'failed' || task.state === 'done') {
          remaining--;
          if (remaining === 0) resolve();
        }
      };

      tasks.forEach(task => {
        // Check initial state
        checkTask(task);

        // Monitor state changes
        const listener = () => checkTask(task);
        this.addEventListener('task.running', listener);
        this.addEventListener('task.failed', listener);
        this.addEventListener('task.completed', listener);
      });

      // Failsafe - if already all resolved
      if (remaining === 0) resolve();
    });
  }

  // Modify existing _dispatchLoop to respect pause state
  _dispatchLoop() {
    // Early exit if not running
    if (this.state !== 'running') return;

    while (this.readyQueue.length > 0 && this.state === 'running') {
      const task = this.readyQueue.shift()!;
      const pool = this.workerPools[task.workerType];
      if (!pool) throw new Error(`Unknown worker type: ${task.workerType}`);

      // Check if we're still running (could have changed during loop)
      if (this.state !== 'running') {
        this.readyQueue.unshift(task); // Put task back
        break;
      }

      // ... rest of existing dispatch logic
    }

    // Don't schedule retry if paused
    if (this.readyQueue.length === 0 || this.state !== 'running') {
      this._clearPeriodicRetry();
    }
  }

  // Modify addTask to respect pause state
  addTask(task: FyflowTask): Promise<any> {
    if (this.state === 'paused' || this.state === 'pausing') {
      throw new Error(`Cannot add tasks while scheduler is ${this.state}`);
    }

    // ... existing addTask logic
    return promise;
  }

  // Utility methods
  isPaused(): boolean {
    return this.state === 'paused';
  }

  isRunning(): boolean {
    return this.state === 'running';
  }

  getState(): SchedulerState {
    return this.state;
  }
}
```

### 2. Worker Manager Changes

```typescript
export class WorkerManager extends EventTarget {
  private paused = false;
  private pauseMode: 'soft' | 'immediate' = 'soft';

  async pause(mode: 'soft' | 'immediate' = 'soft'): Promise<void> {
    if (this.paused) return;

    this.paused = true;
    this.pauseMode = mode;

    this.dispatchEvent(new CustomEvent('worker.pausing', {
      detail: { mode, queuedTasks: this.taskQueue.length }
    }));

    if (mode === 'immediate') {
      // Terminate all workers immediately
      const terminatePromises = this.workers.map(worker => worker.terminate?.());
      await Promise.all(terminatePromises);
      this.workers.clear();

      // Clear the task queue
      this.taskQueue.forEach(task => {
        task.reject(new Error('Worker pool paused - task terminated'));
      });
      this.taskQueue = [];
    }
    // For 'soft' mode, just stop accepting new tasks

    this.dispatchEvent(new CustomEvent('worker.paused', {
      detail: { mode, remainingWorkers: this.workers.size }
    }));
  }

  async resume(): Promise<void> {
    if (!this.paused) return;

    this.dispatchEvent(new CustomEvent('worker.resuming'));

    this.paused = false;

    // Process any queued tasks
    this._processQueue();

    this.dispatchEvent(new CustomEvent('worker.resumed', {
      detail: { queuedTasks: this.taskQueue.length }
    }));
  }

  enqueue(task: any) {
    if (this.paused) {
      task.reject(new Error('Worker pool is paused'));
      return;
    }

    // ... existing enqueue logic
  }

  isPaused(): boolean {
    return this.paused;
  }
}
```

## Pause Mode Comparison

### Soft Pause (Default/Recommended)
- **Behavior**: Stop dispatching new tasks, let running tasks continue
- **Speed**: Immediate
- **Safety**: High (no work lost)
- **Resource usage**: Continues until tasks complete
- **Use case**: Temporary pause, debugging, inspection

```typescript
await scheduler.pause({ mode: 'soft' });
// Immediately stops new work, existing work continues
```

### Graceful Pause
- **Behavior**: Wait for dispatched tasks to start, then stop accepting new ones
- **Speed**: Up to timeout duration
- **Safety**: High (no work lost)
- **Resource usage**: Minimal after timeout
- **Use case**: Maintenance windows, controlled shutdown

```typescript
await scheduler.pause({ mode: 'graceful', timeout: 30000 });
// Waits up to 30s for clean state, then pauses
```

### Immediate Pause
- **Behavior**: Terminate all workers, mark running tasks as failed
- **Speed**: Very fast
- **Safety**: Low (work lost)
- **Resource usage**: Immediate release
- **Use case**: Emergency stops, testing

```typescript
await scheduler.pause({ mode: 'immediate' });
// Kills everything immediately
```

## API Design

### Basic Operations
```typescript
// Pause operations
await scheduler.pause();                                    // Soft pause (default)
await scheduler.pause({ mode: 'soft' });                   // Explicit soft pause
await scheduler.pause({ mode: 'graceful', timeout: 10000 }); // Graceful with timeout
await scheduler.pause({ mode: 'immediate' });              // Emergency stop

// Resume operations
await scheduler.resume();                                   // Resume from any pause

// State checking
scheduler.isRunning();    // true/false
scheduler.isPaused();     // true/false
scheduler.getState();     // 'running' | 'pausing' | 'paused' | 'resuming'
```

### Event Monitoring
```typescript
scheduler.addEventListener('scheduler.pausing', (e) => {
  console.log(`Scheduler pausing with mode: ${e.detail.mode}`);
});

scheduler.addEventListener('scheduler.paused', (e) => {
  console.log(`Paused with ${e.detail.queuedTasks} queued, ${e.detail.runningTasks} running`);
});

scheduler.addEventListener('scheduler.resuming', () => {
  console.log('Scheduler resuming...');
});

scheduler.addEventListener('scheduler.resumed', (e) => {
  console.log(`Resumed with ${e.detail.queuedTasks} queued tasks`);
});
```

## Use Cases

### Development & Debugging
```typescript
// Pause to inspect state
await scheduler.pause();
console.log('Queue depth:', scheduler.readyQueue.length);
console.log('Running tasks:', scheduler.stats.running);
console.log('Task details:', Array.from(scheduler.tasks.values()));
await scheduler.resume();
```

### Maintenance Windows
```typescript
// Graceful pause before maintenance
console.log('Preparing for maintenance...');
await scheduler.pause({ mode: 'graceful', timeout: 60000 });
console.log('Scheduler paused, performing maintenance...');
// ... perform maintenance
console.log('Maintenance complete, resuming...');
await scheduler.resume();
```

### Load Management
```typescript
// Dynamic pause/resume based on system load
setInterval(async () => {
  const systemLoad = await getSystemLoad();

  if (systemLoad > 0.8 && scheduler.isRunning()) {
    console.log('High load detected, pausing scheduler');
    await scheduler.pause({ mode: 'soft' });
  } else if (systemLoad < 0.5 && scheduler.isPaused()) {
    console.log('Load normalized, resuming scheduler');
    await scheduler.resume();
  }
}, 5000);
```

### CLI Control
```typescript
// Signal handlers for operational control
process.on('SIGUSR1', async () => {
  if (scheduler.isRunning()) {
    console.log('SIGUSR1: Pausing scheduler');
    await scheduler.pause();
  } else {
    console.log('SIGUSR1: Resuming scheduler');
    await scheduler.resume();
  }
});

process.on('SIGUSR2', async () => {
  console.log('SIGUSR2: Emergency pause');
  await scheduler.pause({ mode: 'immediate' });
});
```

### Testing & Simulation
```typescript
// Controlled testing scenarios
test('pause/resume workflow', async () => {
  const tasks = createTestTasks(100);

  // Start processing
  tasks.forEach(task => scheduler.addTask(task));

  // Let some tasks start
  await new Promise(resolve => setTimeout(resolve, 100));

  // Pause and verify state
  await scheduler.pause();
  expect(scheduler.isPaused()).toBe(true);

  const runningCount = scheduler.stats.running;
  await new Promise(resolve => setTimeout(resolve, 100));

  // Running count shouldn't change while paused
  expect(scheduler.stats.running).toBe(runningCount);

  // Resume and verify processing continues
  await scheduler.resume();
  expect(scheduler.isRunning()).toBe(true);

  // Wait for completion
  await scheduler.onCompleted();
});
```

## Implementation Phases

### Phase 1: Basic Soft Pause
- Add state machine to FyflowScheduler
- Modify `_dispatchLoop()` to check state
- Basic pause/resume methods
- State checking utilities
- **Estimated time**: 1-2 days

### Phase 2: Worker Pool Integration
- Add pause/resume to WorkerManager
- Coordinate pause/resume across all pools
- Handle worker termination for immediate mode
- **Estimated time**: 2-3 days

### Phase 3: Advanced Pause Modes
- Implement graceful pause with timeout
- Implement immediate pause with cleanup
- Add comprehensive event system
- **Estimated time**: 2-3 days

### Phase 4: Polish & Testing
- Comprehensive test suite
- Error handling edge cases
- Documentation and examples
- Performance impact assessment
- **Estimated time**: 2-3 days

## Error Handling & Edge Cases

### State Transition Errors
```typescript
// Prevent invalid state transitions
if (this.state === 'pausing') {
  throw new Error('Pause already in progress');
}

if (this.state === 'resuming') {
  throw new Error('Resume already in progress');
}
```

### Resource Cleanup
```typescript
// Ensure resources are properly managed during pause/resume
// Resources held by running tasks remain allocated
// New resource requests are blocked while paused
```

### Race Conditions
```typescript
// Handle tasks that get dispatched during pause transition
if (this.state !== 'running') {
  this.readyQueue.unshift(task); // Put task back
  break;
}
```

### Worker Failures During Pause
```typescript
// Handle workers that fail while paused
// Ensure proper cleanup and state consistency
```

## Benefits

1. **Operational Control**: Easy maintenance windows and load management
2. **Debugging**: Inspect scheduler state without stopping the process
3. **Testing**: Controlled scenarios for testing complex workflows
4. **Resource Management**: Temporary resource relief without full shutdown
5. **Graceful Operations**: Better than process restart for temporary issues

## Total Implementation Estimate

**8-12 days** for complete implementation including:
- Core pause/resume functionality
- All three pause modes
- Worker pool integration
- Comprehensive testing
- Documentation and examples

Much simpler than persistence while providing significant operational value!