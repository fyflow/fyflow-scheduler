// JSR publish smoke test.
//
// Copies EXACTLY the files `deno publish --dry-run` says it would publish into a
// scratch directory, then runs a real threaded worker task against that copy.
//
// This exists because a green dry run is not evidence the package works.
// `core/workerWrapper.ts` is not in the module graph - ThreadWrapper reaches it
// through `new URL(...)`, a string literal - so if the publish allowlist drops
// it, Deno reports no error and no warning, the package publishes, and every
// consumer fails the first time they spawn a thread worker.
//
// Run: deno task jsr:smoke

const repoRoot = new URL("../", import.meta.url);

async function publishedFiles(): Promise<string[]> {
  const command = new Deno.Command(Deno.execPath(), {
    args: ["publish", "--dry-run", "--allow-dirty"],
    cwd: new URL(repoRoot).pathname.replace(/^\/([A-Za-z]:)/, "$1"),
    stdout: "piped",
    stderr: "piped"
  });
  const { code, stdout, stderr } = await command.output();
  const output = new TextDecoder().decode(stdout) + new TextDecoder().decode(stderr);

  if (code !== 0) {
    console.error(output);
    throw new Error("deno publish --dry-run failed");
  }

  const files: string[] = [];
  let collecting = false;
  for (const rawLine of output.split("\n")) {
    // deno-lint-ignore no-control-regex -- stripping ANSI colour codes from deno publish output
    const line = rawLine.replace(/\x1b\[[0-9;]*m/g, "").trim();
    if (line.startsWith("Simulating publish")) { collecting = true; continue; }
    if (!collecting) continue;
    const match = line.match(/^file:\/\/\/(.+?) \(/);
    if (!match) continue;
    files.push(decodeURIComponent(match[1]));
  }
  if (files.length === 0) throw new Error("Could not parse the published file list");
  return files;
}

const files = await publishedFiles();
const rootPath = new URL(repoRoot).pathname.replace(/^\/([A-Za-z]:)/, "$1").replace(/\/$/, "");

console.log(`📦 ${files.length} files would be published`);

// The package must be Deno-only: nothing Node-specific may ship
const forbidden = files.filter(f =>
  /\.node\.ts$/.test(f) || /package\.json$/.test(f) || /esbuild\.config\.js$/.test(f) ||
  /tsconfig.*\.json$/.test(f) || /workerWrapperUrl\.bundled\.ts$/.test(f)
);
if (forbidden.length > 0) {
  console.error("❌ Node-specific files in the JSR package:");
  forbidden.forEach(f => console.error("   " + f));
  Deno.exit(1);
}

const staging = await Deno.makeTempDir({ prefix: "fyflow-jsr-" });
for (const absolute of files) {
  const relative = absolute.slice(rootPath.length + 1);
  const target = `${staging}/${relative}`;
  await Deno.mkdir(target.slice(0, target.lastIndexOf("/")), { recursive: true });
  await Deno.copyFile(absolute, target);
}
console.log(`📂 staged to ${staging}`);

// A consumer-supplied worker, deliberately outside the package
await Deno.writeTextFile(`${staging}/consumerWorker.ts`, `
export default class ConsumerWorker {
  async setup() {}
  async teardown() {}
  async run(payload: any) {
    return { doubled: payload.value * 2 };
  }
}
`);

// Exercise the package the way a consumer would: threaded workers, which is the
// only path that loads core/workerWrapper.ts
await Deno.writeTextFile(`${staging}/smoke.ts`, `
import { FyflowScheduler, FyflowTask, WorkerManager } from "./index.ts";

const workerUrl = new URL("./consumerWorker.ts", import.meta.url).href;
const pool = new WorkerManager(workerUrl, {
  maxThreads: 2, maxConcurrentTasks: 1, inline: false, idleTimeout: 0
});
const scheduler = new FyflowScheduler({ ConsumerWorker: pool });

const results = await Promise.all(
  scheduler.addTasks(
    [1, 2, 3].map(v => new FyflowTask({
      id: "t-" + v, workerType: "ConsumerWorker", payload: { value: v }
    })),
    { createPromise: true }
  ) as Promise<any>[]
);

const doubled = results.map(r => r.doubled).sort((a, b) => a - b);
if (JSON.stringify(doubled) !== JSON.stringify([2, 4, 6])) {
  throw new Error("Unexpected results: " + JSON.stringify(doubled));
}

await scheduler.shutdown();
console.log("THREAD_WORKER_OK");
`);

const run = new Deno.Command(Deno.execPath(), {
  args: ["run", "--allow-read", "--allow-net", `${staging}/smoke.ts`],
  stdout: "piped",
  stderr: "piped"
});
const { code, stdout, stderr } = await run.output();
const out = new TextDecoder().decode(stdout);
const err = new TextDecoder().decode(stderr);

if (code !== 0 || !out.includes("THREAD_WORKER_OK")) {
  console.error("❌ Threaded worker failed against the published file set");
  console.error(out);
  console.error(err);
  Deno.exit(1);
}

await Deno.remove(staging, { recursive: true });
console.log("✅ Threaded worker ran against the published files only");
console.log("✅ No Node-specific files in the package");
