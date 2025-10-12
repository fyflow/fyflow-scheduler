/**
 * Resource Health Monitor
 *
 * Add this to your application to detect resource issues in real-time:
 * - Limit violations (should never happen)
 * - Resource leaks (acquired != released)
 * - Underutilization (capacity not being used)
 * - Thread starvation (0 threads with queued tasks)
 */

export function createResourceMonitor(scheduler: any, groups: Record<string, any>, workerPools: Record<string, any>) {
  let violations = 0;
  let samples = 0;
  let totalUtilization = 0;

  const interval = setInterval(() => {
    const metrics = scheduler.getResourceMetrics();
    const stats = scheduler.stats;
    samples++;

    console.log('\n' + '='.repeat(80));
    console.log(`📈 Stats: Queued: ${stats.queued}, Running: ${stats.running}, Done: ${stats.done}, Failed: ${stats.failed}`);

    // Check each resource group
    for (const [groupId, group] of Object.entries(groups)) {
      if (!group) continue;

      const groupMetrics = metrics[groupId];
      const groupStats = group.getStats?.() || {};

      totalUtilization += groupMetrics.utilization;

      // Check for violations
      if (groupMetrics.running > groupMetrics.limit) {
        violations++;
        console.error(`❌ VIOLATION: ${groupId} ${groupMetrics.running}/${groupMetrics.limit}`);
      }

      // Check for leaks
      const leaked = (groupStats.totalAcquired || 0) - (groupStats.totalReleased || 0) - groupMetrics.running;
      if (leaked > 0) {
        console.error(`❌ LEAK: ${groupId} has ${leaked} leaked resources`);
      }

      // Show status
      const util = (groupMetrics.utilization * 100).toFixed(1);
      const status = groupMetrics.running === 0 ? '💤' :
                     groupMetrics.running >= groupMetrics.limit ? '🔴' : '🟢';

      console.log(`   ${status} ${groupId}: ${groupMetrics.running}/${groupMetrics.limit} (${util}% util) | A:${groupStats.totalAcquired || 0} R:${groupStats.totalReleased || 0}`);
    }

    // Check worker pools
    console.log('\n  Worker Pools:');
    for (const [workerType, pool] of Object.entries(workerPools)) {
      const threads = pool.threads?.length || 0;
      const queue = pool.taskQueue?.length || 0;

      // Detect starvation: tasks queued but no threads
      if (queue > 0 && threads === 0) {
        console.warn(`   ⚠️  ${workerType}: 0 threads, ${queue} queued (starvation?)`);
      } else {
        console.log(`   ${workerType}: ${threads} threads, ${queue} queued`);
      }
    }

    // Summary
    const avgUtil = totalUtilization / (samples * Object.keys(groups).length);
    console.log(`\n  Summary: ${violations} violations, ${(avgUtil * 100).toFixed(1)}% avg utilization`);

  }, 2000); // Monitor every 2 seconds

  return {
    stop: () => clearInterval(interval),
    getStats: () => ({ violations, samples, avgUtilization: totalUtilization / (samples * Object.keys(groups).length) })
  };
}

// Usage example:
// const monitor = createResourceMonitor(scheduler, { cpu, fileAccess, db }, { CPUWorker, IOWorker });
//
// // Later:
// monitor.stop();
// const stats = monitor.getStats();
// console.log(`Violations: ${stats.violations}, Avg Util: ${(stats.avgUtilization * 100).toFixed(1)}%`);
