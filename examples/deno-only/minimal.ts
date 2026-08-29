#!/usr/bin/env -S deno run --allow-read --allow-net

/**
 * Minimal example - DENO ONLY.
 *
 * Excluded from the Node and browser builds on purpose, so it can show the
 * worker URL exactly as a Deno consumer writes it: a plain path to the
 * TypeScript source, with no build step and no bundler convention.
 *
 * The cross-runtime examples one level up need `?worker-direct`, which belongs
 * to this repository's esbuild config and is not part of the published API.
 *
 *   deno run --allow-read --allow-net examples/deno-only/minimal.ts
 */

import { FyflowScheduler, FyflowTask, WorkerManager, ConcurrentLimitGroup } from "../../index.ts";

// This is the whole story on Deno: point at the worker source.
const workerUrl = new URL("../workers/simpleWorker.ts", import.meta.url).href;

// A pool must declare the groups it uses - registering a group on the scheduler
// alone does not constrain anything.
const cpu = new ConcurrentLimitGroup(2, "cpu");
const pool = new WorkerManager(workerUrl, {
  maxThreads: 2,
  maxConcurrentTasks: 1,
  groups: ["cpu"],
  config: { delay: 20 }
});

const scheduler = new FyflowScheduler({ SimpleWorker: pool }, { cpu });

scheduler.addEventListener("task.completed", (e: any) => {
  console.log(`✅ ${e.detail.id}`);
});

// addTask and addTasks are fire-and-forget: ask for promises to get results back
const tasks = ["alpha", "beta", "gamma", "delta"].map((name, i) =>
  new FyflowTask({
    id: `task-${i}`,
    workerType: "SimpleWorker",
    payload: { task: name, data: `payload-${i}` }
  })
);

const results = await Promise.all(
  scheduler.addTasks(tasks, { createPromise: true }) as Promise<any>[]
);

console.log(`\n${results.length} tasks done, stats:`, scheduler.stats);
console.log(`cpu group:`, scheduler.getResourceMetrics().cpu);

// Always shut down, or live workers keep the process alive
await scheduler.shutdown();
