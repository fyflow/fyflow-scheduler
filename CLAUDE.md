# FyFlow - Distributed Task Processing Framework

> **Note**: The `node/` folder contains legacy reference code and should be ignored. This project is Deno-focused.

## Project Overview

FyFlow is a Deno-based distributed task processing framework that provides parallel task execution with resource management. The framework enables high-performance workflow orchestration with features like dynamic task spawning, CPU/GPU resource management, worker pooling, and retry mechanisms.

> **Task Management**: Development tasks are tracked in `TODO_CHECKLIST.md` with a standardized lifecycle process (Refinement → Implementation → Completion). Claude references this file when performing changes to ensure alignment with project priorities and follows the established completion procedures including documentation updates and task archival.

## Architecture Overview

### Core Components

**Task Scheduler (`core/FyflowScheduler.ts`)**
- Central orchestrator for parallel task execution
- Handles task states: pending → running → completed/failed
- Supports dynamic task spawning from completed tasks
- Event-driven architecture with custom event dispatching
- All tasks execute in parallel (subject to resource constraints)

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

**Resource Groups**
- **ConcurrentLimitGroup** (`groups/concurrentLimitGroup.ts`): Optimistic soft limits with race-tolerant allocation
  - Best for: CPU scheduling, throughput limiting, load balancing
  - May briefly exceed limit by up to `maxThreads × maxConcurrentTasks`
  - Synchronous acquisition via `onStart()` / `onFinish()`
- **RateLimitGroup** (`groups/rateLimitGroup.ts`): Time-window based rate limiting
  - Best for: API rate limits, request throttling
  - Supports multiple overlapping time windows
  - Tracks both running and completed requests
- **Unified Interface**: All groups implement `ResourceGroup` interface with `getMetrics()` and `getStats()` for monitoring

## Features

### 1. Parallel Task Execution
- **High Concurrency**: All tasks execute in parallel subject to resource constraints
- **Parallel Execution**: Independent tasks run concurrently across worker pools
- **State Tracking**: Real-time task state monitoring (pending/running/done/failed)
- **Fire-and-forget**: Tasks can be added without waiting for completion

### 2. Dynamic Task Spawning
- **Runtime Generation**: Tasks can spawn new tasks based on their results
- **Conditional Workflows**: Dynamic branching based on task outcomes
- **Cascading Execution**: Multi-level task generation
- **Descendant Tracking**: `await task.onCompleteDescendants()` waits for task + all spawned children

### 3. Resource Management
- **Optimistic Allocation (ConcurrentLimitGroup)**:
  - Synchronous, fast allocation with race-tolerant over-allocation
  - Perfect for soft limits like CPU scheduling where brief overage is acceptable
  - May exceed limit by up to `maxThreads × maxConcurrentTasks` during race conditions
- **Rate Limiting (RateLimitGroup)**:
  - Time-window based request throttling
  - Multiple overlapping windows (e.g., 10/sec, 100/min simultaneously)
  - Tracks running and completed requests
- **Worker-level Group Assignment**: Groups specified at WorkerManager level for cleaner API
- **Mixed Group Support**: Tasks can use multiple resource groups simultaneously
- **Automatic Resource Cleanup**: Resource release on task completion/failure
- **Real-time Monitoring**: `scheduler.getResourceMetrics()` and `scheduler.getResourceStats()` for observability

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
deno run --allow-read --allow-net examples/getting-started.ts      # Basic parallel execution
deno run --allow-read --allow-net examples/enhanced-features.ts    # Progress reporting & dynamic spawning
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
│   ├── FyflowScheduler.ts           # Main task scheduler
│   ├── workerManager.ts          # Worker pool management
│   ├── threadWrapper.ts          # Worker thread wrapper
│   ├── inlineWrapper.ts          # Inline worker implementation
│   └── workerInterface.ts        # Worker interface definitions
├── groups/                       # Resource group implementations
│   ├── concurrentLimitGroup.ts   # Optimistic soft limits (race-tolerant)
│   ├── strictLimitGroup.ts       # Hard limits with async tokens (never exceeds)
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
│   ├── performance-groups.ts     # Resource management and constraints
│   └── strict-limits.ts          # Strict resource limits (GPU/API)
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
- Simple parallel task creation
- Worker setup and configuration
- Event listening and monitoring
- Basic resource management

### Basic Task Creation
```typescript
// Configure WorkerManager with groups
const cpuGroup = new ConcurrentLimitGroup(4); // CPU constraint group
const workerManager = new WorkerManager(scriptUrl, {
  maxThreads: 4,
  maxConcurrentTasks: 1,
  groups: ['cpu']  // Groups specified at pool level
});

// Create parallel tasks (no dependencies)
const task = new FyflowTask({
  id: 'process-data',
  workerType: 'DataProcessor',
  payload: { data: 'raw-input' }
  // Groups inherited from WorkerManager, or override with workerGroups
});

// All tasks execute in parallel
scheduler.addTask(task);
```

### Task-Level Group Override
```typescript
// Override worker manager groups at task level if needed
const task = new FyflowTask({
  id: 'gpu-intensive-task',
  workerType: 'DataProcessor',
  payload: { data: 'large-batch' },
  workerGroups: ['gpu', 'memory']  // Task-level override
});
```

### Enhanced Worker Communication
For enhanced worker features, see `examples/enhanced-features.ts` which demonstrates:
- Real-time progress reporting during task execution
- Dynamic task spawning using the new spawn API
- Workflow completion tracking with `onCompleteDescendants()`
- Enhanced event-driven monitoring and observability


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

### Strict Resource Limits (GPU/API)
For hard resource constraints, see `examples/strict-limits.ts` which demonstrates:
- StrictLimitGroup for GPU memory (never exceeds 4GB)
- Mixed strict + optimistic groups (GPU strict, CPU optimistic)
- Real-time resource monitoring and violation detection
- Async token-based resource acquisition
- Wait queue statistics and timeout handling

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

// vs regular completion (just this task)
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

#### Optimistic Limits (ConcurrentLimitGroup)
Best for soft limits where brief overage is acceptable:
```typescript
const cpuGroup = new ConcurrentLimitGroup(16, 'cpu'); // ~16 CPU cores (may briefly exceed)

const cpuPool = new WorkerManager(scriptUrl, {
  maxThreads: 4,
  maxConcurrentTasks: 1,
  groups: ['cpu']  // Optimistic allocation
});
// May briefly use 17/16 CPU during race conditions, but resolves quickly
```

#### Strict Limits (StrictLimitGroup)
Best for hard constraints that must NEVER be exceeded:
```typescript
const gpuMemory = new StrictLimitGroup(4, 'gpu'); // Exactly 4GB GPU memory (NEVER exceeds)
const apiLimit = new StrictLimitGroup(10, 'api'); // Exactly 10 concurrent API calls

const gpuPool = new WorkerManager(scriptUrl, {
  maxThreads: 4,
  maxConcurrentTasks: 2, // 8 total capacity
  groups: ['gpu', 'api']  // Strict allocation - tasks block until slots available
});
// Tasks wait in FIFO queue when GPU/API slots full - guaranteed never to exceed
```

#### Mixed Groups (Strict + Optimistic)
Combine both strategies for optimal performance:
```typescript
const gpuMemory = new StrictLimitGroup(4, 'gpu');    // STRICT: GPU memory
const cpuCores = new ConcurrentLimitGroup(8, 'cpu');  // OPTIMISTIC: CPU

const scheduler = new FyflowScheduler(
  { MLWorker: new WorkerManager(scriptUrl, {
    maxThreads: 4,
    maxConcurrentTasks: 2,
    groups: ['gpu', 'cpu']  // Both strict and optimistic
  })},
  { gpu: gpuMemory, cpu: cpuCores }
);

// Monitor resources in real-time
const metrics = scheduler.getResourceMetrics();
console.log(`GPU: ${metrics.gpu.running}/${metrics.gpu.limit} (${(metrics.gpu.utilization * 100).toFixed(1)}%)`);

// Get detailed stats for strict groups
const stats = scheduler.getResourceStats();
console.log(`GPU avg wait: ${stats.gpu.avgWaitTime}ms, queue depth: ${stats.gpu.currentWaiting}`);
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