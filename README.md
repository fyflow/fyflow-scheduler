# FyFlow Scheduler

Zero-dependency parallel task scheduler with resource management and cross-platform support.

[![npm version](https://badge.fury.io/js/fyflow-scheduler.svg)](https://badge.fury.io/js/fyflow-scheduler)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Note**: This project was mostly "vibe coded" with [Claude Code](https://claude.ai/code) - it was an experiment, if such a complex library can be built with sufficient code quality.

## Overview

FyFlow is a self-contained parallel task scheduler with resource management. Fully in-memory operation with zero dependencies. Extensible APIs allow enhancement for persistence, distributed execution, or multi-machine coordination.

## Installation

```bash
npm install fyflow-scheduler
# or
yarn add fyflow-scheduler
```

For Deno:
```bash
deno add @fyflow/scheduler
```

## Quick Start

```typescript
import { FyflowScheduler, FyflowTask, WorkerManager, ConcurrentLimitGroup } from 'fyflow-scheduler';

// Create worker pool
const workerPool = new WorkerManager('./worker.js', { maxThreads: 2 });

// Create scheduler with CPU constraints
const scheduler = new FyflowScheduler(
  { MyWorker: workerPool },
  { cpu: new ConcurrentLimitGroup(4) }
);

// Create parallel tasks
const tasks = [
  new FyflowTask({ id: 'task1', workerType: 'MyWorker', payload: { data: 'input' } }),
  new FyflowTask({ id: 'task2', workerType: 'MyWorker', payload: { data: 'processed' } })
];

// Execute
tasks.forEach(task => scheduler.addTask(task));
```

## Key Features

- **Parallel Task Execution**: High-performance concurrent task processing
- **Resource Management**: CPU/GPU constraints with concurrent execution limits
- **Dynamic Task Spawning**: Workers can create new tasks at runtime
- **Cross-Platform**: Node.js, Browser, and Deno support
- **Worker Pools**: Thread-based and inline execution modes
- **Progress Reporting**: Real-time task progress and event monitoring
- **Zero Dependencies**: Self-contained with no external requirements
- **In-Memory**: Fast coordination with extensible persistence APIs

## Basic Usage

### Creating Workers

```typescript
import { BaseWorker } from 'fyflow-scheduler';

export default class MyWorker extends BaseWorker {
  constructor(config = {}) {
    super(config);
  }

  async run(payload, context) {
    // Task execution logic
    context?.sendProgress(0.5, "Processing...");

    // Spawn additional tasks if needed
    context?.spawnTask({
      id: 'child-task',
      workerType: 'MyWorker',
      payload: { data: 'child-data' }
    });

    return { result: 'completed', data: payload.data };
  }
}
```

### Parallel Task Execution

```typescript
const tasks = [
  new FyflowTask({ id: 'fetch', workerType: 'DataWorker', payload: { url: 'api/data' } }),
  new FyflowTask({ id: 'validate', workerType: 'ValidationWorker', payload: { url: 'api/data2' } }),
  new FyflowTask({ id: 'process', workerType: 'ProcessWorker', payload: { batch: 1 } }),
  new FyflowTask({ id: 'save', workerType: 'SaveWorker', payload: { batch: 2 } })
];

// All tasks execute in parallel (subject to resource constraints)
tasks.forEach(task => scheduler.addTask(task));
```

### Resource Groups

```typescript
const cpuGroup = new ConcurrentLimitGroup(8);  // Max 8 concurrent CPU tasks
const gpuGroup = new ConcurrentLimitGroup(2);  // Max 2 concurrent GPU tasks

const scheduler = new FyflowScheduler(workerPools, {
  cpu: cpuGroup,
  gpu: gpuGroup
});

// Workers automatically use groups specified in WorkerManager
const gpuWorkerPool = new WorkerManager('./gpu-worker.js', {
  maxThreads: 2,
  groups: ['gpu']  // This pool uses GPU constraints
});
```

### Event Monitoring

```typescript
scheduler.addEventListener('task.completed', (e) => {
  console.log(`Task ${e.detail.id} completed:`, e.detail.result);
});

scheduler.addEventListener('task.progress', (e) => {
  console.log(`Task ${e.detail.taskId}: ${e.detail.progress}%`);
});

scheduler.addEventListener('scheduler.completed', (e) => {
  console.log(`All tasks completed. Stats:`, e.detail);
});
```

## API Reference

### Core Classes

**FyflowScheduler(workerPools, resourceGroups)**
- `addTask(task)`: Add task to execution queue
- `stats`: Current execution statistics
- `addEventListener(event, handler)`: Event monitoring

**FyflowTask(config)**
- `id`: Unique task identifier
- `workerType`: Worker pool to use
- `payload`: Data passed to worker
- `workerGroups`: Optional resource group names
- `optional`: Mark task as optional (failures don't block workflow)

**WorkerManager(scriptUrl, options)**
- `maxThreads`: Maximum worker threads
- `maxConcurrentTasks`: Tasks per worker
- `groups`: Resource group names
- `inline`: Use inline execution (default: false)

**ConcurrentLimitGroup(limit)**
- Resource constraint with concurrent execution limit

### Worker Interface

Workers must extend `BaseWorker` and implement:
- `constructor(config)`: Setup worker configuration
- `async run(payload, context)`: Execute task logic
- `async setup()` (optional): Initialize worker
- `async teardown()` (optional): Cleanup worker

## Performance

Performance characteristics (Node.js):
- **Inline Workers**: 7,500+ tasks/sec for I/O workloads
- **Thread Workers**: ~45% efficiency for CPU workloads
- **Memory Usage**: ~2MB per 1,000 tasks
- **Coordination Overhead**: ~8% for threaded, minimal for inline

## Platform Support

| Platform | Worker Threads | Inline Workers | Build Required |
|----------|---------------|----------------|----------------|
| Node.js  | ✅            | ✅             | Yes (esbuild)  |
| Browser  | ✅            | ✅             | Yes (esbuild)  |
| Deno     | ✅            | ✅             | No             |

## Development

```bash
# Build library
npm run build

# Run tests
npm test                    # Node.js tests
npm run test:browser        # Browser tests
npm run test:deno          # Deno tests

# Benchmarks
npm run benchmark          # Full benchmark suite
npm run benchmark:quick    # Quick performance test
```

## Examples

See the `examples/` directory for comprehensive usage examples:
- `getting-started.ts`: Basic parallel task execution
- `enhanced-features.ts`: Progress reporting and dynamic task spawning
- `worker-types.ts`: Thread vs inline worker comparison
- `performance-groups.ts`: Resource management and constraints

## License

MIT License. See [LICENSE](LICENSE) for details.

## Links

- [GitHub Repository](https://github.com/fyflow/fyflow-scheduler)
- [NPM Package](https://www.npmjs.com/package/fyflow-scheduler)
- [Issue Tracker](https://github.com/fyflow/fyflow-scheduler/issues)