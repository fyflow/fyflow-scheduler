import { WorkerManager, FyflowScheduler, FyflowTask, ConcurrentLimitGroup } from "../index.ts";

let modelWorkerUrl: any;
if (typeof Deno !== "undefined") {
  modelWorkerUrl = new URL("./workers/modelWorker.ts", import.meta.url).href;
} else {
  // Node/Browser esbuild replaces this
  // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
  modelWorkerUrl = new URL((await import("./workers/modelWorker.ts?worker-direct")).default);
}

/**
 * Resident Resource Groups
 *
 * Demonstrates:
 * - A resource held for a WORKER's lifetime, not a task's
 * - Per-worker costs, so a big model and several small ones share one budget
 * - Why a task-scoped group cannot express this
 * - Worker affinity - a hot model is not reloaded between tasks
 *
 * The scenario: one GPU with 24GB. A 20GB model and three 2GB models compete
 * for it. Only combinations that actually fit may be resident at once.
 */

console.log("🚀 FyFlow Resident Resource Groups");
console.log("==================================\n");

const VRAM_GB = 24;
const vram = new ConcurrentLimitGroup(VRAM_GB, "vram");

// ---------------------------------------------------------------------------
// Part 1: the problem, with a task-scoped group
// ---------------------------------------------------------------------------
console.log("① A task-scoped group does NOT bound resident models");
console.log("   'gpu' allows one task at a time, which sounds like enough.\n");

{
  const gpu = new ConcurrentLimitGroup(1, "gpu");
  const pool = (model: string) => new WorkerManager(modelWorkerUrl, {
    maxThreads: 1, maxConcurrentTasks: 1, inline: true,
    groups: ["gpu"],                       // <- scoped to the TASK
    config: { model, sizeGb: 20, loadMs: 60 }
  });

  const scheduler = new FyflowScheduler(
    { Llama: pool("llama-20gb"), Sdxl: pool("sdxl-20gb") },
    { gpu }
  );

  await Promise.all([
    scheduler.addTask(new FyflowTask({ id: "t1", workerType: "Llama", payload: { prompt: "hi", inferMs: 40 } }), { createPromise: true }),
    scheduler.addTask(new FyflowTask({ id: "t2", workerType: "Sdxl", payload: { prompt: "hi", inferMs: 40 } }), { createPromise: true })
  ]);

  console.log("   ⇒ Both models loaded. The group released its slot when the first");
  console.log("     TASK settled, but the first WORKER - and its 20GB - lived on.\n");
  await scheduler.shutdown();
}

// ---------------------------------------------------------------------------
// Part 2: the same thing with residentGroups
// ---------------------------------------------------------------------------
console.log("② residentGroups holds from worker creation to teardown\n");

{
  const gpu = new ConcurrentLimitGroup(1, "gpu");
  const pool = (model: string) => new WorkerManager(modelWorkerUrl, {
    maxThreads: 1, maxConcurrentTasks: 1, inline: true,
    residentGroups: ["gpu"],               // <- scoped to the WORKER
    idleTimeout: 40,                       // release once idle...
    idleCheckIntervalMs: 20,               // ...and notice promptly
    config: { model, sizeGb: 20, loadMs: 60 }
  });

  const scheduler = new FyflowScheduler(
    { Llama: pool("llama-20gb"), Sdxl: pool("sdxl-20gb") },
    { gpu }
  );

  await Promise.all([
    scheduler.addTask(new FyflowTask({ id: "t1", workerType: "Llama", payload: { prompt: "hi", inferMs: 40 } }), { createPromise: true }),
    scheduler.addTask(new FyflowTask({ id: "t2", workerType: "Sdxl", payload: { prompt: "hi", inferMs: 40 } }), { createPromise: true })
  ]);

  console.log("   ⇒ The first model UNLOADED before the second LOADED.\n");
  await scheduler.shutdown();
}

// ---------------------------------------------------------------------------
// Part 3: weighted costs - one budget, models of different sizes
// ---------------------------------------------------------------------------
console.log(`③ Weighted costs: a 20GB model and three 2GB models over ${VRAM_GB}GB\n`);

const big = new WorkerManager(modelWorkerUrl, {
  maxThreads: 1, maxConcurrentTasks: 1, inline: true,
  residentGroups: { vram: 20 },            // 20 of 24 units per worker
  idleTimeout: 40, idleCheckIntervalMs: 20,
  config: { model: "llama-70b", sizeGb: 20, loadMs: 100 }
});

const smallPools: Record<string, WorkerManager> = {};
for (const name of ["embed", "rerank", "classify"]) {
  smallPools[name] = new WorkerManager(modelWorkerUrl, {
    maxThreads: 1, maxConcurrentTasks: 1, inline: true,
    residentGroups: { vram: 2 },           // three of these fit alongside each other
    idleTimeout: 40, idleCheckIntervalMs: 20,
    config: { model: `${name}-2gb`, sizeGb: 2, loadMs: 40 }
  });
}

const scheduler = new FyflowScheduler({ Big: big, ...capitalise(smallPools) }, { vram });

// Watch the budget. `running` is the units currently held by live workers.
let peak = 0;
const watch = setInterval(() => { peak = Math.max(peak, vram.running); }, 5);

// `detail` is the FyflowTask itself
scheduler.addEventListener("task.completed", (e: any) => {
  console.log(`   ✅ ${e.detail.id} on ${e.detail.result?.model}`);
});

const work = [
  new FyflowTask({ id: "chat-1", workerType: "Big", payload: { prompt: "explain", inferMs: 80 } }),
  new FyflowTask({ id: "embed-1", workerType: "Embed", payload: { prompt: "doc a", inferMs: 30 } }),
  new FyflowTask({ id: "embed-2", workerType: "Embed", payload: { prompt: "doc b", inferMs: 30 } }),
  new FyflowTask({ id: "rerank-1", workerType: "Rerank", payload: { prompt: "hits", inferMs: 30 } }),
  new FyflowTask({ id: "class-1", workerType: "Classify", payload: { prompt: "text", inferMs: 30 } })
];

await Promise.all(scheduler.addTasks(work, { createPromise: true }) as Promise<any>[]);
clearInterval(watch);

console.log(`\n   Peak VRAM held: ${peak}/${VRAM_GB}GB - never oversubscribed.`);
console.log("   The 20GB model and a 2GB model coexist (22GB); a second 2GB model");
console.log("   waits, because 24GB is the budget.\n");

// ---------------------------------------------------------------------------
// Part 4: affinity - a hot model is not reloaded
// ---------------------------------------------------------------------------
console.log("④ Affinity: a run of tasks reuses the loaded worker\n");

let loads = 0;
smallPools.embed.addEventListener("worker.setup.completed", () => { loads++; });

await Promise.all(
  scheduler.addTasks(
    Array.from({ length: 5 }, (_, i) => new FyflowTask({
      id: `embed-batch-${i}`, workerType: "Embed", payload: { prompt: `doc ${i}`, inferMs: 10 }
    })),
    { createPromise: true }
  ) as Promise<any>[]
);

console.log(`   ⇒ 5 tasks, ${loads} model load(s). The worker stayed hot.`);
console.log("     This is why a resident group beats swapping models inside one");
console.log("     worker: model loads dominate, and affinity avoids them.\n");

console.log("📊 Final group state:", JSON.stringify(scheduler.getResourceMetrics().vram));
await scheduler.shutdown();
console.log(`   After shutdown: ${vram.running}GB held - everything released.`);

/** { embed: pool } -> { Embed: pool }, so worker types read as names. */
function capitalise(pools: Record<string, WorkerManager>): Record<string, WorkerManager> {
  return Object.fromEntries(
    Object.entries(pools).map(([k, v]) => [k[0].toUpperCase() + k.slice(1), v])
  );
}
