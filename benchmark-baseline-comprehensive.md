# Benchmark Report: FyFlow Scheduler Performance Benchmark

**Description**: Comprehensive performance analysis of the FyFlow scheduler and worker management
**Timestamp**: 2026-08-27T20:08:18.122Z
**Environment**: Deno on windows

## Summary
- **Total Tests**: 11
- **Total Duration**: 3.52m
- **Average Throughput**: 622.14 tasks/sec
- **Peak Memory Usage**: 125.99 MB
- **Total Memory Delta**: 173.48 MB

## Detailed Results

### Large Volume - 10K Independent Tasks (Inline)

**Description**: 10,000 independent tasks testing scheduler scalability - inline workers

**Configuration**:
- Tasks: 10000
- CPU Slots: 8
- Worker Type: Inline
- Max Threads: 4
- Max Concurrent Tasks: 10

**Performance Metrics**:
- **Total Duration**: 245.72ms
- **Scheduler Overhead**: 179.81μs
- **Async Execution Overhead**: 231.28ms
- **Coordination Overhead**: 220.88ms
- **Total Worker Time**: 415.97ms
- **Overall Efficiency**: 4.5%
- **Worker Startup**: 4 worker(s), avg 108.50μs init, 15.40μs setup, max 365.40μs
- **Task Throughput**: 43237.05 tasks/sec
- **Memory Delta**: 7.58 MB
- **Peak Memory**: 14.08 MB
- **Avg Dispatch Time**: 8.99μs
- **Max Dispatch Time**: 2.81ms
- **Dispatch Iterations**: 20005

### Large Volume - 50K Independent Tasks (Inline)

**Description**: 50,000 independent tasks - stress test for memory and throughput - inline workers

**Configuration**:
- Tasks: 50000
- CPU Slots: 16
- Worker Type: Inline
- Max Threads: 8
- Max Concurrent Tasks: 20

**Performance Metrics**:
- **Total Duration**: 7.09s
- **Scheduler Overhead**: 6.50ms
- **Async Execution Overhead**: 7.07s
- **Coordination Overhead**: 6.94s
- **Total Worker Time**: 22.37s
- **Overall Efficiency**: 2.0%
- **Worker Startup**: 8 worker(s), avg 134.29μs init, 111.95μs setup, max 173.00μs
- **Task Throughput**: 7067.32 tasks/sec
- **Memory Delta**: 65.74 MB
- **Peak Memory**: 84.25 MB
- **Avg Dispatch Time**: 64.96μs
- **Max Dispatch Time**: 120.09ms
- **Dispatch Iterations**: 100009

### Fire-and-Forget - 10K Tasks (Inline)

**Description**: 10,000 tasks using event-driven completion (no promises created) - tests optional promise optimization

**Configuration**:
- Tasks: 10000
- CPU Slots: 8
- Worker Type: Inline
- Max Threads: 4
- Max Concurrent Tasks: 10

**Performance Metrics**:
- **Total Duration**: 312.39ms
- **Scheduler Overhead**: 241.25μs
- **Async Execution Overhead**: 306.87ms
- **Coordination Overhead**: 293.57ms
- **Total Worker Time**: 532.17ms
- **Overall Efficiency**: 4.3%
- **Worker Startup**: 4 worker(s), avg 29.10μs init, 21.22μs setup, max 30.00μs
- **Task Throughput**: 32587.10 tasks/sec
- **Memory Delta**: 6.70 MB
- **Peak Memory**: 62.42 MB
- **Avg Dispatch Time**: 12.06μs
- **Max Dispatch Time**: 2.70ms
- **Dispatch Iterations**: 20005

### Fire-and-Forget - 50K Tasks (Inline)

**Description**: 50,000 tasks using event-driven completion (no promises created) - stress test for optimization

**Configuration**:
- Tasks: 50000
- CPU Slots: 16
- Worker Type: Inline
- Max Threads: 8
- Max Concurrent Tasks: 20

**Performance Metrics**:
- **Total Duration**: 7.59s
- **Scheduler Overhead**: 7.01ms
- **Async Execution Overhead**: 7.58s
- **Coordination Overhead**: 7.47s
- **Total Worker Time**: 18.25s
- **Overall Efficiency**: 1.5%
- **Worker Startup**: 8 worker(s), avg 54.44μs init, 45.72μs setup, max 61.50μs
- **Task Throughput**: 6594.22 tasks/sec
- **Memory Delta**: 26.38 MB
- **Peak Memory**: 93.24 MB
- **Avg Dispatch Time**: 70.12μs
- **Max Dispatch Time**: 86.40ms
- **Dispatch Iterations**: 100009

### Threading Scalability - 200 Tasks, 2 Threads

**Description**: 200 CPU tasks on 2 threads - measure threading scalability

**Configuration**:
- Tasks: 200
- CPU Slots: 2
- Worker Type: Thread
- Max Threads: 2
- Max Concurrent Tasks: 1

**Performance Metrics**:
- **Total Duration**: 4.33s
- **Scheduler Overhead**: 34.84μs
- **Async Execution Overhead**: 4.19s
- **Coordination Overhead**: 155.85ms
- **Total Worker Time**: 8.06s
- **Overall Efficiency**: 96.3%
- **Worker Startup**: 2 worker(s), avg 36.44ms init, 83.50μs setup, max 55.69ms
- **Task Throughput**: 47.75 tasks/sec
- **Memory Delta**: 4.91 MB
- **Peak Memory**: 121.03 MB
- **Avg Dispatch Time**: 81.22μs
- **Max Dispatch Time**: 7.63ms
- **Dispatch Iterations**: 429

### Threading Scalability - 200 Tasks, 4 Threads

**Description**: 200 CPU tasks on 4 threads - measure threading scalability

**Configuration**:
- Tasks: 200
- CPU Slots: 4
- Worker Type: Thread
- Max Threads: 4
- Max Concurrent Tasks: 1

**Performance Metrics**:
- **Total Duration**: 4.48s
- **Scheduler Overhead**: 24.55μs
- **Async Execution Overhead**: 3.82s
- **Coordination Overhead**: 119.80ms
- **Total Worker Time**: 14.80s
- **Overall Efficiency**: 96.9%
- **Worker Startup**: 4 worker(s), avg 153.23ms init, 104.98μs setup, max 282.30ms
- **Task Throughput**: 52.37 tasks/sec
- **Memory Delta**: 4.81 MB
- **Peak Memory**: 125.99 MB
- **Avg Dispatch Time**: 53.15μs
- **Max Dispatch Time**: 5.06ms
- **Dispatch Iterations**: 462

### High Contention - 1K Tasks, 4 Slots

**Description**: 1,000 tasks competing for 4 group slots (simulating GPU contention)

**Configuration**:
- Tasks: 1000
- CPU Slots: 8
- Worker Type: Thread
- Max Threads: 2
- Max Concurrent Tasks: 1
- Groups: 1 groups, 4 limit each

**Performance Metrics**:
- **Total Duration**: 18.58s
- **Scheduler Overhead**: 174.43μs
- **Async Execution Overhead**: 8.11s
- **Coordination Overhead**: 375.77ms
- **Total Worker Time**: 35.47s
- **Overall Efficiency**: 97.9%
- **Worker Startup**: 2 worker(s), avg 103.48ms init, 72.55μs setup, max 146.82ms
- **Task Throughput**: 55.21 tasks/sec
- **Memory Delta**: --117.25 MB
- **Peak Memory**: 8.93 MB
- **Avg Dispatch Time**: 86.10μs
- **Max Dispatch Time**: 12.16ms
- **Dispatch Iterations**: 2026

### High Contention - 10K Tasks, 4 Slots

**Description**: 10,000 tasks competing for 4 group slots - O(n²) behavior test

**Configuration**:
- Tasks: 10000
- CPU Slots: 8
- Worker Type: Thread
- Max Threads: 2
- Max Concurrent Tasks: 1
- Groups: 1 groups, 4 limit each

**Performance Metrics**:
- **Total Duration**: 2.78m
- **Scheduler Overhead**: 1.90ms
- **Async Execution Overhead**: 1.94m
- **Coordination Overhead**: 2.69s
- **Total Worker Time**: 5.45m
- **Overall Efficiency**: 98.4%
- **Worker Startup**: 2 worker(s), avg 102.14ms init, 2.17ms setup, max 150.90ms
- **Task Throughput**: 60.16 tasks/sec
- **Memory Delta**: 56.54 MB
- **Peak Memory**: 66.10 MB
- **Avg Dispatch Time**: 94.55μs
- **Max Dispatch Time**: 13.89ms
- **Dispatch Iterations**: 20064

### Worker Startup - 1 Thread

**Description**: Cold start cost of a single worker thread

**Configuration**:
- Tasks: 4
- CPU Slots: 1
- Worker Type: Thread
- Max Threads: 1
- Max Concurrent Tasks: 1

**Performance Metrics**:
- **Total Duration**: 684.44ms
- **Scheduler Overhead**: 12.24μs
- **Async Execution Overhead**: 674.85ms
- **Coordination Overhead**: 581.36ms
- **Total Worker Time**: 93.49ms
- **Overall Efficiency**: 13.9%
- **Worker Startup**: 1 worker(s), avg 169.79ms init, 59.80μs setup, max 169.79ms
- **Task Throughput**: 5.93 tasks/sec
- **Memory Delta**: 159.69 KB
- **Peak Memory**: 71.53 MB
- **Avg Dispatch Time**: 815.70μs
- **Max Dispatch Time**: 11.45ms
- **Dispatch Iterations**: 15

### Worker Startup - 8 Threads

**Description**: Cold start cost of eight worker threads

**Configuration**:
- Tasks: 32
- CPU Slots: 8
- Worker Type: Thread
- Max Threads: 8
- Max Concurrent Tasks: 1

**Performance Metrics**:
- **Total Duration**: 1.32s
- **Scheduler Overhead**: 1.50μs
- **Async Execution Overhead**: 1.32s
- **Coordination Overhead**: 1.27s
- **Total Worker Time**: 372.32ms
- **Overall Efficiency**: 3.5%
- **Worker Startup**: 8 worker(s), avg 507.78ms init, 78.16μs setup, max 1.04s
- **Task Throughput**: 24.33 tasks/sec
- **Memory Delta**: --49.19 MB
- **Peak Memory**: 22.41 MB
- **Avg Dispatch Time**: 20.60μs
- **Max Dispatch Time**: 101.40μs
- **Dispatch Iterations**: 73

### Worker Startup - 8 Inline Instances

**Description**: Cold start cost of eight inline worker instances (no threads)

**Configuration**:
- Tasks: 32
- CPU Slots: 8
- Worker Type: Inline
- Max Threads: 8
- Max Concurrent Tasks: 1

**Performance Metrics**:
- **Total Duration**: 104.88ms
- **Scheduler Overhead**: 3.74μs
- **Async Execution Overhead**: 104.43ms
- **Coordination Overhead**: 104.07ms
- **Total Worker Time**: 2.89ms
- **Overall Efficiency**: 0.3%
- **Worker Startup**: 8 worker(s), avg 256.75μs init, 210.11μs setup, max 296.60μs
- **Task Throughput**: 306.43 tasks/sec
- **Memory Delta**: 686.48 KB
- **Peak Memory**: 17.61 MB
- **Avg Dispatch Time**: 58.52μs
- **Max Dispatch Time**: 633.10μs
- **Dispatch Iterations**: 64

## Performance Analysis

### Group Contention Analysis
| Test | Tasks | Group Slots | Throughput | Avg Dispatch Time | Max Dispatch Time |
|------|-------|-------------|------------|-------------------|-------------------|
| High Contention - 1K Tasks, 4 Slots | 1000 | 4 | 55.21 | 86.10μs | 12.16ms |
| High Contention - 10K Tasks, 4 Slots | 10000 | 4 | 60.16 | 94.55μs | 13.89ms |

### Task Volume Scaling
| Test | Tasks | Throughput | Memory Delta | Scheduler Overhead |
|------|-------|------------|--------------|-------------------|
| Large Volume - 10K Independent Tasks (Inline) | 10000 | 43237.05 | 7.58 MB | 179.81μs |
| Large Volume - 50K Independent Tasks (Inline) | 50000 | 7067.32 | 65.74 MB | 6.50ms |
| Fire-and-Forget - 10K Tasks (Inline) | 10000 | 32587.10 | 6.70 MB | 241.25μs |
| Fire-and-Forget - 50K Tasks (Inline) | 50000 | 6594.22 | 26.38 MB | 7.01ms |
| Threading Scalability - 200 Tasks, 2 Threads | 200 | 47.75 | 4.91 MB | 34.84μs |
| Threading Scalability - 200 Tasks, 4 Threads | 200 | 52.37 | 4.81 MB | 24.55μs |
| Worker Startup - 1 Thread | 4 | 5.93 | 159.69 KB | 12.24μs |
| Worker Startup - 8 Threads | 32 | 24.33 | --49.19 MB | 1.50μs |
| Worker Startup - 8 Inline Instances | 32 | 306.43 | 686.48 KB | 3.74μs |

## Recommendations

No specific performance issues detected. System is performing within expected parameters.