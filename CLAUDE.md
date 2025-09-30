# FyFlow - Distributed Task Processing Framework

> **Note**: The `node/` folder contains legacy reference code and should be ignored. This project is Deno-focused.

## Project Overview

FyFlow is a Deno-based distributed task processing framework that provides DAG (Directed Acyclic Graph) scheduling capabilities with resource management. The framework enables complex workflow orchestration with features like dynamic task spawning, CPU/GPU resource management, worker pooling, and retry mechanisms.

> **Task Management**: Development tasks are tracked in `TODO_CHECKLIST.md` with a standardized lifecycle process (Refinement → Implementation → Completion). Claude references this file when performing changes to ensure alignment with project priorities and follows the established completion procedures including documentation updates and task archival.

## Architecture Overview

### Core Components

**Task Scheduler (`core/dagScheduler.ts`)**
- Central orchestrator for DAG-based task execution
- Manages task dependencies and execution order
- Handles task states: pending → running → completed/failed
- Supports dynamic task spawning from completed tasks
- Event-driven architecture with custom event dispatching

**Resource Management**
- **Group-based constraints**: CPU limiting through ConcurrentLimitGroup instead of global singleton
- **Worker-level resource assignment**: Groups specified at WorkerManager level for cleaner architecture
- **Automatic resource cleanup**: Resource release on task completion
- **Configurable limits**: Per-worker-type and per-group resource allocation

**Worker Manager (`core/workerManager.ts`)**
- Manages pools of worker threads/inline workers with **robust resource management**
- **Options-based configuration**: Clean object-based constructor API
- **Config passing**: Worker configuration passed to class instances
- **Smart task distribution**: CPU-aware thread creation with predictive capacity planning
- **Lazy initialization**: Conservative thread creation to avoid wasteful resource allocation
- **Resilient CPU slot enforcement**: Handles CPU slot failures with task requeuing (no infinite loops)
- Task queuing and dispatching with concurrent execution support
- Support for both threaded and inline execution modes
- Multiplicative capacity: `maxThreads × maxConcurrentTasks`

**Worker Types**
- `ThreadWrapper` (`core/threadWrapper.ts`): Worker thread implementation with concurrent task support (1 CPU slot per thread)
  - **Initialization race condition protection**: `initializing` flag prevents multiple concurrent initializations
  - **Event-driven task redistribution**: CPU slot release events trigger automatic task retry
- `InlineWrapper` (`core/inlineWrapper.ts`): In-process execution for lightweight async tasks (0 CPU slots, runs in main thread)
  - **Initialization state management**: Prevents task execution during worker setup
- **Class-based worker interface**: All workers implemented as ES6 classes with unified API
- **Instance isolation**: Each worker manager gets isolated class instances
- **Lifecycle management**: Constructor, optional setup(), run(), optional teardown() methods
- **Event-driven monitoring**: Lifecycle events for initialization, setup, teardown
- Configurable concurrent task limits per worker instance

**Resource Groups (`groups/exampleGroup.ts`)**
- Constraint-based execution groups (e.g., GPU pools)
- Concurrent execution limits per group
- Group-based resource allocation

## Features

### 1. DAG Task Scheduling
- **Dependency Management**: Tasks can have parent/child relationships
- **Automatic Ordering**: Tasks execute only when dependencies are satisfied
- **Parallel Execution**: Independent tasks run concurrently
- **State Tracking**: Real-time task state monitoring (pending/running/done/failed)

### 2. Dynamic Task Spawning
- **Runtime Generation**: Tasks can spawn new tasks based on their results
- **Conditional Workflows**: Dynamic branching based on task outcomes
- **Cascading Execution**: Multi-level task generation
- **Relationship Tracking**: Parent-child task relationship monitoring

### 3. Resource Management
- **Group-based CPU Management**: ConcurrentLimitGroup-based CPU constraints replace global singleton
- **Worker-level Group Assignment**: Groups specified at WorkerManager level for cleaner API
- **Resource Groups**: GPU/memory pool constraints with concurrent execution limits
- **Configurable Limits**: Per-worker-type and per-group resource allocation
- **Automatic Release**: Resource cleanup on task completion

### 4. Worker Pool Management
- **Multiple Pool Types**: Different worker pools for different task types
- **Concurrent Execution**: Workers can handle multiple tasks simultaneously based on configuration
- **Auto-scaling**: Dynamic worker creation up to configured limits
- **Capacity Management**: Intelligent task distribution based on worker availability
- **Idle Management**: Automatic worker termination after idle timeout
- **Load Balancing**: Automatic task distribution across available workers

### 5. Error Handling & Resilience
- **Retry Mechanisms**: Configurable retry policies with backoff
- **Resource Constraint Recovery**: Automatic task requeuing when CPU slots unavailable (prevents infinite loops)
- **Initialization Race Protection**: Prevents multiple concurrent worker initializations
- **Optional Tasks**: Non-critical tasks that don't block workflow
- **Error Isolation**: Failed tasks don't affect independent tasks
- **User Action Events**: Manual intervention points for failed tasks
- **Event-driven Recovery**: CPU slot release events automatically trigger task redistribution

### 6. Enhanced Worker Communication & Observability
- **Real-time Progress Reporting**: Workers can report progress with `context.sendProgress(progress, message, details)`
- **Dynamic Task Spawning**: Workers can spawn tasks during execution with `context.spawnTask(config)`
- **Enhanced Message Protocol**: Proper message types (init, teardown, result, error, progress, spawn_task) replace fake taskIds
- **Workflow Completion Tracking**: `await task.onCompleteDescendants()` waits for task + all spawned children
- **Event-driven Architecture**: Complete visibility into task flow with progress and spawn events
- **Real-time Stats**: Accurate queue depth, running tasks, completion rates
- **Performance Metrics**: Task execution timing, coordination overhead measurement
- **Debug Logging**: Comprehensive task flow visibility with worker-level feedback

### 7. Performance Benchmarking Suite
- **Comprehensive Testing**: 17 predefined scenarios covering all performance aspects
- **Scheduler Overhead Isolation**: No-op workers to measure pure scheduler performance
- **Overall Efficiency Metric**: Normalized efficiency percentage (0-100%) comparable across configurations
- **Coordination Overhead Measurement**: Distinguishes scheduling costs from actual work execution time
- **Threading Scalability Tests**: CPU workers with proper workloads to measure threading efficiency
- **Memory Profiling**: Heap usage tracking and memory delta analysis
- **Group Contention Analysis**: Specialized tests for O(n²) bottleneck identification
- **Multiple Output Formats**: Console, Markdown, JSON, and CSV reports
- **Baseline Establishment**: Performance regression testing and optimization measurement

## Common Commands

```bash
# Run main application
deno task start

# Run specific examples
deno run --allow-read --allow-net examples/getting-started.ts      # Basic usage
deno run --allow-read --allow-net examples/advanced-features.ts    # Dynamic spawning
deno run --allow-read --allow-net examples/enhanced-features.ts    # Progress reporting & enhanced communication
deno run --allow-read --allow-net examples/worker-types.ts         # Worker comparison
deno run --allow-read --allow-net examples/performance-groups.ts   # Resource management

# Run performance benchmarks (cross-platform)
# Deno:
deno task benchmark          # All benchmarks
deno task benchmark:quick    # Quick volume tests
deno task benchmark:contention # Group contention tests
deno task benchmark:overlapping # Overlapping groups tests
deno task benchmark:help     # Show all options
# or directly:
deno run --allow-read --allow-net --allow-write benchmark/runBenchmarks.ts --format markdown --output report.md

# Node.js:
npm run benchmark          # All benchmarks
npm run benchmark:quick    # Quick volume tests
npm run benchmark:contention # Group contention tests
npm run benchmark:overlapping # Overlapping groups tests
npm run benchmark:help     # Show all options

# Type checking
deno check **/*.ts

# Linting
deno lint .

# Formatting
deno fmt .
```

## Project Structure

```
fyflow-new/
├── core/                         # Core execution engine
│   ├── dagScheduler.ts           # Main task scheduler
│   ├── workerManager.ts          # Worker pool management
│   ├── threadWrapper.ts          # Worker thread wrapper
│   ├── inlineWrapper.ts          # Inline worker implementation
│   └── workerInterface.ts        # Worker interface definitions
├── groups/                       # Resource group implementations
│   ├── concurrentLimitGroup.ts   # Concurrent execution groups
│   └── rateLimitGroup.ts         # Rate limiting groups
├── examples/                     # Example implementations
│   ├── workers/                  # Essential worker implementations
│   │   ├── simpleWorker.ts       # Basic class-based worker template
│   │   ├── asyncWorker.ts        # High-concurrency inline worker
│   │   ├── cpuWorker.ts          # Thread-based CPU-intensive worker
│   │   └── dataProcessor.ts      # Data analysis with spawning
│   ├── getting-started.ts        # Basic usage and task dependencies
│   ├── advanced-features.ts      # Dynamic spawning and resource groups
│   ├── worker-types.ts           # Inline vs threaded comparison
│   └── performance-groups.ts     # Resource management and constraints
├── benchmark/                    # Performance benchmarking suite
│   ├── perfUtils.ts              # Performance measurement utilities
│   ├── benchmarkScenarios.ts     # Benchmark execution framework
│   ├── predefinedScenarios.ts    # Pre-built benchmark scenarios
│   ├── reportGenerator.ts        # Report generation (markdown, JSON, CSV)
│   ├── runBenchmarks.ts          # Benchmark CLI runner
│   └── noopWorker.ts             # No-op worker for benchmarking
├── .vscode/                      # VS Code configuration
├── .claude/                      # Claude configuration
├── index.ts                      # Main library exports
├── deno.json                     # Deno configuration and tasks
├── TODO_CHECKLIST.md             # Development task tracking
└── package-lock.json            # Lock file
```

## Usage Examples

### Getting Started
For basic usage, see `examples/getting-started.ts` which demonstrates:
- Simple task creation and dependencies
- Worker setup and configuration
- Event listening and monitoring
- Basic resource management

### Basic Task Creation (NEW API)
```typescript
// Configure WorkerManager with groups (NEW: cleaner API)
const cpuGroup = new ConcurrentLimitGroup(4); // CPU constraint group
const workerManager = new WorkerManager(scriptUrl, {
  maxThreads: 4,
  maxConcurrentTasks: 1,
  groups: ['cpu']  // Groups specified once at pool level
});

// Tasks automatically inherit groups from their WorkerManager
const task = new DagTask({
  id: 'process-data',
  workerType: 'DataProcessor',
  payload: { data: 'raw-input' },
  parents: ['data-fetch']
  // No workerGroups needed - inherited from WorkerManager!
});
```

### Backward Compatibility
```typescript
// OLD API still works for transition
const task = new DagTask({
  id: 'process-data',
  workerType: 'DataProcessor',
  payload: { data: 'raw-input' },
  parents: ['data-fetch'],
  workerGroups: ['gpu']    // Task-level groups still supported
});
```

### Enhanced Worker Communication
For enhanced worker features, see `examples/enhanced-features.ts` which demonstrates:
- Real-time progress reporting during task execution
- Dynamic task spawning using the new spawn API
- Workflow completion tracking with `onCompleteDescendants()`
- Enhanced event-driven monitoring and observability

### Advanced Features
For complex workflows, see `examples/advanced-features.ts` which demonstrates:
- Dynamic task spawning based on results
- Resource groups and constraints
- Event-driven workflow orchestration
- Complex data processing pipelines

### Worker Types
For understanding different worker patterns, see `examples/worker-types.ts` which demonstrates:
- Inline vs threaded worker performance
- Concurrent execution patterns
- Resource usage differences
- Optimal use cases for each type

### Performance & Groups
For resource management, see `examples/performance-groups.ts` which demonstrates:
- Multiple resource group types
- Resource contention handling
- Performance monitoring
- Load balancing strategies

### Progress Reporting
Workers can report real-time progress during execution:
```typescript
// In worker run() method with context
async run(payload: any, context?: WorkerContext): Promise<any> {
  context?.sendProgress(0, "Starting processing...");

  for (let i = 0; i < items.length; i++) {
    await processItem(items[i]);
    const progress = (i + 1) / items.length;
    context?.sendProgress(progress, `Processed ${i + 1}/${items.length} items`);
  }

  return results;
}
```

### Dynamic Task Spawning
Workers can spawn tasks during execution using the context API:
```typescript
// In worker run method
async run(payload: any, context?: WorkerContext): Promise<any> {
  // Perform processing...

  // Spawn additional tasks based on results
  context?.spawnTask({
    id: 'child-task-1',
    workerType: 'ProcessorWorker',
    payload: { data: processedData },
    parents: [], // Optional parent dependencies
    workerGroups: ['cpu'] // Optional resource groups
  });

  return { processed: true };
}

// Listen for spawn events
scheduler.addEventListener('task.spawn_request', (e) => {
  const { parentTask, spawnConfig } = e.detail;
  console.log(`Task ${parentTask.id} spawning: ${spawnConfig.id}`);
});
```


### Workflow Completion Tracking
Wait for complete workflows including all spawned descendants:
```typescript
// Wait for task + all spawned children/grandchildren/etc
await dataAnalysisTask.onCompleteDescendants();

// vs regular completion (just the parent task)
await scheduler.addTask(dataAnalysisTask);
```

### Class-Based Worker Implementation
All workers are implemented as ES6 classes providing instance isolation and unified interface:
```typescript
// Worker class example
export default class MyWorker {
    private config: any;
    private connection: any;

    constructor(config: any = {}) {
        this.config = config;
    }

    async setup() {
        // Optional async initialization
        this.connection = await createConnection(this.config.host);
    }

    async teardown() {
        // Optional cleanup
        await this.connection?.close();
    }

    async run(payload: any) {
        // Process the task
        return await this.connection.process(payload.data);
    }
}

// Web worker mode - handles both inline and threaded execution
if (typeof self !== 'undefined' && 'postMessage' in self) {
    let workerInstance: MyWorker | null = null;

    self.onmessage = async (e) => {
        const { taskId, payload, action, config } = e.data;
        try {
            if (action === 'init') {
                workerInstance = new MyWorker(config);
                await workerInstance.setup?.();
                self.postMessage({ taskId, result: 'initialized' });
            } else if (action === 'run' && workerInstance) {
                const result = await workerInstance.run(payload);
                self.postMessage({ taskId, result });
            } else if (action === 'teardown' && workerInstance) {
                await workerInstance.teardown?.();
                workerInstance = null;
                self.postMessage({ taskId, result: 'teardown_complete' });
            }
        } catch (error: any) {
            self.postMessage({ taskId, error: error.message });
        }
    };
}
```

### Resource Configuration
```typescript
const cpuGroup = new ConcurrentLimitGroup(16); // 16 CPU slots
const gpuGroup = new ConcurrentLimitGroup(4);  // 4 concurrent GPU tasks

// Configure worker managers with concurrent execution and class-based workers
const cpuPool = new WorkerManager(scriptUrl, {
  maxThreads: 4,
  maxConcurrentTasks: 1,
  idleTimeout: 5000,
  groups: ['cpu'],  // Groups specified at WorkerManager level
  config: { /* worker-specific config */ }
});     // 4 threads, 1 task each

const asyncPool = new WorkerManager(scriptUrl, {
  maxThreads: 2,
  maxConcurrentTasks: 10,
  idleTimeout: 5000,
  inline: true,
  config: { /* worker-specific config */ }
}); // 2 inline workers, 10 concurrent tasks each
// Total capacity: 4 + 20 = 24 concurrent tasks
```

## Performance Benchmarking

The framework includes a comprehensive benchmarking suite to measure scheduler performance and identify bottlenecks.

### Quick Start
```bash
# Run basic performance tests
deno task benchmark:quick

# Run contention tests
deno task benchmark:contention

# Run overlapping groups tests
deno task benchmark:overlapping

# Test specific scenarios (direct command)
deno run --allow-read --allow-net --allow-write benchmark/runBenchmarks.ts --scenarios "High Contention - 1K Tasks, 4 Slots"

# Generate detailed markdown report
deno run --allow-read --allow-net --allow-write benchmark/runBenchmarks.ts --categories overlapping --format markdown --output contention-report.md
```

### Available Benchmark Categories

**Quick Tests** (3 scenarios): Fast volume scaling tests (1K-50K tasks)
```bash
deno task benchmark:quick
```

**Contention Tests** (3 scenarios): Group contention with limited slots
```bash
deno task benchmark:contention
```

**Overlapping Groups** (3 scenarios): Tasks belonging to multiple overlapping groups
```bash
deno task benchmark:overlapping
```

**Group Scaling** (3 scenarios): Many groups with distributed tasks
```bash
deno run --allow-read --allow-net --allow-write benchmark/runBenchmarks.ts --categories group_scaling
```

**Dependencies** (3 scenarios): Complex DAG dependency patterns
```bash
deno run --allow-read --allow-net --allow-write benchmark/runBenchmarks.ts --categories dependencies
```

**Stress Tests** (1 scenario): 100K+ tasks for memory pressure testing
```bash
deno run --allow-read --allow-net --allow-write benchmark/runBenchmarks.ts --categories stress
```

### Benchmark Metrics

- **Task Throughput**: Tasks processed per second
- **Overall Efficiency**: Normalized efficiency percentage (0-100%) comparable across configurations
- **Coordination Overhead**: Distinguishes scheduling costs from actual work execution time
- **Scheduler Overhead**: Time spent in scheduler logic vs actual work
- **Memory Usage**: Heap growth and peak memory consumption
- **Dispatch Times**: Average and maximum dispatch loop iteration times
- **Group Contention Impact**: Performance degradation with competing tasks

### Key Performance Insights

**Overall Efficiency Results**:
- **Inline Workers (Async/I/O workloads)**: 100% efficiency across all scales
- **Threaded Workers (CPU workloads)**: ~45% efficiency accounting for threading overhead
- **Threading Scalability**: Good scaling with more threads improving efficiency

**Performance Characteristics**:
- **Inline Workers**: Perfect for async/I/O with minimal coordination overhead
- **Threaded Workers**: Suitable for CPU-bound tasks with expected threading costs
- **Memory Usage**: Linear growth ~2MB per 1K tasks

## Development Workflow

### Task Lifecycle
All development follows a standardized 3-phase lifecycle:
1. **Refinement** - Planning and specification
2. **Implementation & Verification** - Code, test, benchmark
3. **Completion** - Documentation update, task archival

This ensures consistent quality and keeps both code and documentation current.

### Documentation Maintenance
- CLAUDE.md is updated during task completion to reflect all changes
- TODO_CHECKLIST.md active tasks focus on current work only
- Completed tasks are archived with brief summaries to maintain history
- API examples and project structure stay synchronized with codebase

For detailed task management process, see `TODO_CHECKLIST.md`.