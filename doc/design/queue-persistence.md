# Queue Persistence Analysis

This document analyzes the complexity, approaches, and implementation strategies for making the FyFlow scheduler queue persistent across system restarts.

## Complexity Assessment: **Medium to High**

### What Needs to be Persisted

**Core State:**
- `scheduler.tasks` Map - All task definitions and current state
- `scheduler.readyQueue` - Tasks waiting to execute
- Task relationships (parents/children)
- Resource group states (CPU slots, rate limits, etc.)
- Worker pool states and configurations

**Runtime State:**
- Tasks currently running (need to handle gracefully on restart)
- Retry timers and backoff states
- Progress tracking state
- Descendant trackers for `onCompleteDescendants()`

### Architecture Approaches

**1. Event Sourcing (Recommended)**
- Persist all state changes as events: TaskAdded, TaskStarted, TaskCompleted, TaskSpawned
- Replay events on startup to reconstruct state
- Clean, auditable, handles complex state transitions well

**2. Snapshot + WAL (Write-Ahead Log)**
- Periodic snapshots of entire scheduler state
- Transaction log for changes since last snapshot
- Faster startup, but more complex serialization

**3. Direct State Serialization**
- JSON/binary dump of scheduler state
- Simple but fragile to schema changes

## Key Gotchas & Challenges

### 1. Worker State Handling
```typescript
// Running tasks are tricky - workers might be mid-execution
// Options:
// a) Mark as "unknown" state, let them report back
// b) Kill all workers on shutdown, restart tasks
// c) Graceful worker shutdown with task handoff
```

### 2. Task Dependencies
```typescript
// Parent-child relationships need careful reconstruction
// Spawned tasks might reference parents that haven't been restored yet
// Need topological restoration order
```

### 3. Resource Groups
```typescript
// CPU slots, rate limits need to be reset or restored
// What about tasks that held resources when system went down?
// Need resource cleanup/reclamation strategy
```

### 4. Timing & Retries
```typescript
// Retry timers, backoff states, periodic cleanup
// Should failed tasks retry immediately on restart?
// How to handle time-sensitive tasks?
```

### 5. Promise Resolution
```typescript
// task.resolve/reject functions can't be serialized
// Need to recreate promise chains on restoration
// onCompleteDescendants() promises need reconstruction
```

## State Loading Complexity - The Core Challenge

**Loading saved state is the biggest complexity** - persistence is straightforward, but reconstruction is where all the challenges lie.

### The "Easy" Parts of Loading
```typescript
// Basic task data - straightforward
const task = new FyflowTask({
  id: saved.id,
  workerType: saved.workerType,
  payload: saved.payload,
  parents: saved.parents
});
task.state = saved.state;
task.result = saved.result;
```

### The "Hard" Parts of Loading

**1. Promise Chain Reconstruction**
```typescript
// These don't survive serialization:
task.resolve = ???  // Was connected to user's await
task.reject = ???

// Need to rebuild the entire promise infrastructure
// Users might have been awaiting scheduler.addTask(task)
// Or task.onCompleteDescendants() - how do we reconnect?
```

**2. Parent-Child Relationship Rebuilding**
```typescript
// Order dependency nightmare:
// Task A spawned B, B spawned C
// But we load them as: C, A, B
// When we load C, A doesn't exist yet to add C to A.children

// Need topological loading or multi-pass reconstruction
for (const savedTask of savedTasks) {
  const task = recreateTask(savedTask);
  scheduler.tasks.set(task.id, task);
}
// THEN
for (const savedTask of savedTasks) {
  const task = scheduler.tasks.get(savedTask.id);
  // Now rebuild relationships - parents exist
  savedTask.parents.forEach(parentId => {
    const parent = scheduler.tasks.get(parentId);
    parent.children.add(task.id);
  });
}
```

**3. Worker Pool State Synchronization**
```typescript
// WorkerManager has running threads, queues, resource slots
// Some might be mid-task when we persisted
// Need to:
// a) Kill all workers ("mark as failed" approach)
// b) Or coordinate with workers to understand their state
// c) Or wait for workers to report back their status

// The "mark running tasks as failed" approach is cleanest:
for (const task of restoredTasks) {
  if (task.state === 'running' || task.state === 'dispatched') {
    task.state = 'failed'; // or 'pending' to retry
    task.attempts++; // might trigger retry logic
  }
}
```

**4. Resource Group State**
```typescript
// CPU slots, rate limits, quotas
// Were 3/4 CPU slots in use when we crashed?
// Need to reset or reconstruct resource states

// Simple approach: reset all resource groups on startup
cpuGroup.running = 0; // Reset to clean slate
rateLimitGroup.resetWindows(); // Clear rate limit history
```

**5. Descendant Trackers Reconstruction**
```typescript
// Someone called task.onCompleteDescendants()
// descendantTrackers Map had pending promises
// How do we reconnect the promises after restart?

// Probably need to abandon old promises and provide new mechanism:
// "Check if task completed while we were down"
if (task.state === 'done') {
  // Can't call original resolve() - promise is gone
  // Maybe emit events instead?
  this.dispatchEvent(new CustomEvent('task.restored_complete', {detail: task}));
}
```

## Recommended Implementation Strategy

### Configuration Options
```typescript
interface PersistenceConfig {
  onRunningTasks: 'fail' | 'retry' | 'wait';
  onDispatchedTasks: 'fail' | 'retry';
  gracefulShutdown: 'wait' | 'kill'; // Two modes for shutdown
  gracefulShutdownTimeout: number; // ms to wait for completion
}

// On ungraceful restart:
if (config.onRunningTasks === 'fail') {
  runningTasks.forEach(task => {
    task.state = 'failed';
    task.result = new Error('System restart - task interrupted');
  });
}
```

### Graceful Shutdown - Two Modes
```typescript
async gracefulShutdown(mode: 'wait' | 'kill') {
  this.accepting = false; // No new tasks

  const runningTasks = Array.from(this.tasks.values())
    .filter(t => t.state === 'running');

  if (runningTasks.length > 0) {
    if (mode === 'wait') {
      console.log(`Waiting for ${runningTasks.length} tasks to complete...`);
      await Promise.race([
        Promise.all(runningTasks.map(t => t.onCompletePromise())),
        new Promise(r => setTimeout(r, config.gracefulShutdownTimeout))
      ]);
    } else { // mode === 'kill'
      console.log(`Terminating ${runningTasks.length} running tasks...`);
      runningTasks.forEach(task => {
        task.state = 'failed';
        task.result = new Error('Graceful shutdown - task terminated');
      });
    }
  }

  await this.persist();
  await this.shutdown();
}
```

### Multi-Phase Loading Strategy
```typescript
async restore() {
  // Phase 1: Load raw task data
  const savedState = await this.storage.load();

  // Phase 2: Recreate tasks (no relationships yet)
  for (const saved of savedState.tasks) {
    const task = this.recreateTask(saved);
    this.tasks.set(task.id, task);
  }

  // Phase 3: Rebuild relationships
  for (const saved of savedState.tasks) {
    this.rebuildTaskRelationships(saved);
  }

  // Phase 4: Reset runtime state
  this.resetWorkerPools();
  this.resetResourceGroups();

  // Phase 5: Queue ready tasks
  this.rebuildReadyQueue();

  // Phase 6: Handle interrupted tasks
  this.handleInterruptedTasks();
}
```

### Promise Chain Handling
```typescript
// Simplest approach: don't try to restore old promises
// Instead: provide new ways to check completion

// Old way (before restart):
await scheduler.addTask(task);

// New way (after restart):
if (scheduler.isRestored && scheduler.hasTask(task.id)) {
  const restoredTask = scheduler.getTask(task.id);
  if (restoredTask.state === 'done') {
    // Already completed while we were down
    return restoredTask.result;
  } else {
    // Still pending, can await normally
    await restoredTask.onCompletePromise();
  }
}
```

## Implementation Phases

**Phase 1: Basic State Persistence**
```typescript
interface PersistedTask {
  id: string;
  workerType: string;
  payload: any;
  parents: string[];
  children: string[];
  state: 'pending' | 'dispatched' | 'running' | 'done' | 'failed';
  result?: any;
  attempts: number;
  // ... other serializable fields
}

class PersistentScheduler extends FyflowScheduler {
  async save(storage: PersistenceLayer) {
    const state = {
      tasks: Array.from(this.tasks.entries()),
      readyQueue: this.readyQueue.map(t => t.id),
      stats: this.stats,
      timestamp: Date.now()
    };
    await storage.write(state);
  }

  async restore(storage: PersistenceLayer) {
    const state = await storage.read();
    // Rebuild tasks, relationships, queues...
  }
}
```

**Phase 2: Event Sourcing**
```typescript
// All mutations become events
this.emit('task.added', { taskId, task });
this.emit('task.state.changed', { taskId, from: 'pending', to: 'running' });
this.emit('task.spawned', { parentId, childId, spawnConfig });

// Persistence layer captures all events
// Startup replays events to rebuild state
```

**Phase 3: Advanced Features**
- Graceful shutdown with worker coordination (wait/kill modes)
- Incremental persistence (only dirty state)
- Schema versioning for backwards compatibility
- Recovery strategies for corrupted state

## Storage Options

**Simple File-based:**
```typescript
// JSON files, rotation, atomic writes
await Deno.writeTextFile('scheduler-state.json', JSON.stringify(state));
```

**SQLite (Good balance):**
```sql
-- Tasks table, events table, relationships table
-- ACID transactions, good performance, single file
```

**Redis/External DB:**
```typescript
// For distributed scenarios, shared state
// Pub/sub for multi-instance coordination
```

## Recommended Sweet Spot Implementation

**Persistence: Event Sourcing**
- Log all mutations: TaskAdded, TaskCompleted, TaskFailed, etc.
- Easy to replay, debug, and reason about

**Loading: "Nuclear Reset" Approach**
- Mark all running/dispatched tasks as failed (or pending for retry)
- Reset all worker pools and resource groups
- Rebuild task relationships from scratch
- Don't try to restore old promises - create new ones

**Graceful Shutdown: Wait/Kill + Timeout**
- Stop accepting new tasks
- Wait for running tasks (with timeout) OR kill them immediately
- Persist final state
- Clean shutdown

This approach provides both disaster recovery AND graceful restarts without the nightmare complexity of trying to perfectly restore mid-execution state.

## Alternative Approach: External Persistence Module

### Feasibility: **High** - Minimal Core Changes Required

The current FyFlow architecture is surprisingly well-positioned for external persistence implementation due to its event-driven design and public APIs.

#### What's Already Available (No Changes Needed)

**1. Comprehensive Event System**
```typescript
// All necessary events are already emitted:
scheduler.addEventListener('task.completed', (e) => persistenceModule.logEvent(e));
scheduler.addEventListener('task.failed', (e) => persistenceModule.logEvent(e));
scheduler.addEventListener('task.spawn_request', (e) => persistenceModule.logEvent(e));
scheduler.addEventListener('scheduler.completed', (e) => persistenceModule.snapshot(e));

// Worker events too:
workerManager.addEventListener('task.started', (e) => persistenceModule.logEvent(e));
```

**2. Public State Access**
```typescript
// Scheduler exposes what we need:
scheduler.tasks         // Map<string, FyflowTask> - all tasks
scheduler.readyQueue    // FyflowTask[] - pending tasks
scheduler.stats         // {queued, running, done, failed}
scheduler.workerPools   // All worker managers

// Tasks expose their state:
task.id, task.state, task.result, task.parents, task.children
```

**3. Task Creation API**
```typescript
// Can recreate tasks externally:
const task = new FyflowTask({
  id: restored.id,
  workerType: restored.workerType,
  payload: restored.payload,
  parents: restored.parents
});
scheduler.addTask(task);
```

#### Minimal Required Core Changes

**Required Changes: ~15 lines of code maximum**

1. ✅ **Task.result property** - Already implemented
2. ✅ **Event system** - Already comprehensive
3. ✅ **Public state access** - Already available
4. ✅ **Batch task addition** - Already implemented (`addTasks()`)
5. 🔄 **getAllTasks() method** - 2 lines of code
6. 🔄 **setRestorationMode()** - Optional, 10 lines of code

```typescript
// Minor additions to FyflowScheduler:
class FyflowScheduler {
  getAllTasks(): FyflowTask[] {
    return Array.from(this.tasks.values());
  }

  setRestorationMode(enabled: boolean) {
    this.restorationMode = enabled;
    // Skip some validations during restoration
  }
}
```

#### External Module Implementation

```typescript
class FyFlowPersistence {
  private scheduler: FyflowScheduler;
  private storage: PersistenceStorage;

  constructor(scheduler: FyflowScheduler, storage: PersistenceStorage) {
    this.scheduler = scheduler;
    this.storage = storage;
    this.attachEventListeners();
  }

  private attachEventListeners() {
    // Log all state-changing events
    this.scheduler.addEventListener('task.completed', (e) => {
      this.logEvent('task.completed', e.detail);
    });

    this.scheduler.addEventListener('task.spawn_request', (e) => {
      this.logEvent('task.spawned', {
        parentId: e.detail.parentTask.id,
        childConfig: e.detail.spawnConfig
      });
    });

    // Periodic snapshots
    setInterval(() => this.snapshot(), 30000);
  }

  async restore(): Promise<void> {
    const state = await this.storage.load();
    if (!state) return;

    this.scheduler.setRestorationMode?.(true);

    // Recreate tasks in dependency order
    const tasksToRestore = this.sortTasksByDependencies(state.tasks);
    const recreatedTasks = tasksToRestore.map(taskData => {
      const task = new FyflowTask({
        id: taskData.id,
        workerType: taskData.workerType,
        payload: taskData.payload,
        parents: taskData.parents
      });

      task.state = taskData.state;
      task.result = taskData.result;
      task.attempts = taskData.attempts;

      return task;
    });

    await this.scheduler.addTasks(recreatedTasks);
    this.handleInterruptedTasks(recreatedTasks);
    this.scheduler.setRestorationMode?.(false);
  }
}
```

#### Usage - Zero Core Changes Required

```typescript
// User code - no changes to core FyFlow
import { FyflowScheduler, FyflowTask } from 'fyflow';
import { FyFlowPersistence } from 'fyflow-persistence'; // External module

const scheduler = new FyflowScheduler(workerPools, groups);

// Add persistence with external module
const storage = new SQLiteStorage('scheduler.db');
const persistence = new FyFlowPersistence(scheduler, storage);

// Restore on startup
await persistence.restore();

// Normal operation - persistence happens automatically via events
const task = new FyflowTask({...});
scheduler.addTask(task); // Automatically persisted
```

#### Benefits of External Module Approach

**✅ Zero Breaking Changes**: Core library unchanged
**✅ Optional Feature**: Users choose whether to use persistence
**✅ Pluggable Storage**: SQLite, Redis, file-based, etc.
**✅ Different Strategies**: Event sourcing, snapshots, hybrid
**✅ Version Independent**: Persistence module can evolve separately
**✅ Clean Separation**: Core library stays focused on scheduling

#### Potential External Modules

```typescript
// Different persistence strategies as separate packages
import { EventSourcingPersistence } from 'fyflow-persistence-events';
import { SnapshotPersistence } from 'fyflow-persistence-snapshots';
import { SQLitePersistence } from 'fyflow-persistence-sqlite';
import { RedisPersistence } from 'fyflow-persistence-redis';
```

## Complexity Estimate Comparison

### Built-in Persistence Approach
- **Basic persistence**: 1-2 weeks
- **Production-ready with edge cases**: 1-2 months
- **Event sourcing + advanced features**: 2-3 months

### External Module Approach
- **Core changes**: 1-2 hours (minimal additions)
- **External module basic**: 1 week
- **External module production-ready**: 2-3 weeks
- **Multiple storage backends**: 1 month

**Recommendation**: Start with the external module approach. The architecture is already persistence-ready, requiring almost no changes to the core codebase while providing maximum flexibility for users.

The loading complexity drops from "dragons everywhere" to "manageable complexity" with the nuclear reset approach while still providing the core benefits of persistence for both disaster recovery and long-running workflows.