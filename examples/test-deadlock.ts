import { FyflowScheduler, FyflowTask, WorkerManager, ConcurrentLimitGroup } from '../index.ts';

const cpuGroup = new ConcurrentLimitGroup(12, 'cpu');

  const cpuWorker = new WorkerManager(
    new URL('./workers/cpuWorker.ts', import.meta.url).href,
    {
      maxThreads: 30,
      maxConcurrentTasks: 1,
      groups: ['cpu'],
      inline: false,
      idleTimeout: 5000,
      config: { iterations: 1000000 }
    }
  );
  const cpuWorker2 = new WorkerManager(
    new URL('./workers/cpuWorker.ts', import.meta.url).href,
    {
      maxThreads: 30,
      maxConcurrentTasks: 1,
      groups: ['cpu'],
      inline: false,
      idleTimeout: 5000,
      config: { iterations: 1000000 }
    }
  );
  

const scheduler = new FyflowScheduler(
  { 
    CPUWorker: cpuWorker,
    CPUWorker2: cpuWorker2
   },
  { cpu: cpuGroup }
);
scheduler.addEventListener('task.completed', (e: any) => {
  scheduler.addTask(new FyflowTask({
    id: `task-cpue2-${e.detail.id}`,
    workerType: 'CPUWorker2',
    payload: { duration: 100, taskId: e.detail.payload.taskId}
  }));
  // const task = e.detail;
  // console.log(`✅ COMPLETED: ${task.id} - ${task.payload.taskId}`);
});

// Listen for failures
scheduler.addEventListener('task.failed', (e: any) => {
  const task = e.detail;
  console.log(`❌ FAILED: ${task.id} - ${task.error}`);
});

// Add 100 tasks
for (let i = 0; i < 100; i++) {
  scheduler.addTask(new FyflowTask({
    id: `task-${i}`,
    workerType: 'CPUWorker',
    payload: { duration: 100, taskId: i}
  }));
}

// Monitor for deadlock
const monitorInterval = setInterval(() => {
  const stats = scheduler.stats;
  const metrics = scheduler.getResourceMetrics();

  console.log(`Stats: Q:${stats.queued} R:${stats.running} D:${stats.done} F:${stats.failed}`);
  console.log(`CPU: ${metrics.cpu.running}/${metrics.cpu.limit}`);
  console.log(`Threads: ${cpuWorker.threads.length}`);
  console.log(`Threads2: ${cpuWorker2.threads.length}`);

  // Show ready queues
  let totalReady = 0;
  for (const [workerType, queue] of (scheduler as any).readyQueuesByWorker) {
    totalReady += queue.length;
    if (queue.length > 0) {
      console.log(`ReadyQueue[${workerType}]: ${queue.length} tasks`);
    }
  }
  if (totalReady === 0) {
    console.log('ReadyQueues: empty');
  }

  // Show blocked queues
  console.log(`BlockedQueues size: ${scheduler.blockedQueues.size}`);
  for (const [key, queue] of scheduler.blockedQueues) {
    console.log(`  ${key}: ${queue.length} tasks`);
  }
  console.log('');

  if (stats.done === 100) {
    clearInterval(monitorInterval);
    console.log('\n✅ All tasks completed!');
    scheduler.shutdown();
  }
}, 500);
