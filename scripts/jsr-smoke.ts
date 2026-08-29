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

import { basename, dirname, fromFileUrl, join } from "jsr:@std/path@^1.1.2";
import { externalImports, parsePublishedFileUrls, stagingPlan } from "./publishOutput.ts";

const repoRoot = new URL("../", import.meta.url);

async function publishedFileUrls(): Promise<string[]> {
  const command = new Deno.Command(Deno.execPath(), {
    args: ["publish", "--dry-run", "--allow-dirty"],
    cwd: fromFileUrl(repoRoot),
    stdout: "piped",
    stderr: "piped"
  });
  const { code, stdout, stderr } = await command.output();
  const output = new TextDecoder().decode(stdout) + new TextDecoder().decode(stderr);

  if (code !== 0) {
    console.error(output);
    throw new Error("deno publish --dry-run failed");
  }

  const urls = parsePublishedFileUrls(output);
  if (urls.length === 0) throw new Error("Could not parse the published file list");
  return urls;
}

const fileUrls = await publishedFileUrls();
const files = fileUrls.map((url) => fromFileUrl(url));

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

// The package must have no dependencies. Everything it imports must be another
// file inside it.
//
// Nothing else catches this. `scripts/` uses @std/path and is kept out of the
// package by the publish allowlist alone - not by any dependency declaration,
// because Deno has no devDependencies. Widen that allowlist by one line, or
// import a script from a published module, and @std becomes a real dependency
// every consumer resolves. The smoke run below would not notice: it copies the
// published files and runs them, and a `jsr:` import simply resolves over the
// network and passes green.
const withExternalImports: string[] = [];
for (const file of files) {
  if (!/\.(ts|js|mjs|json)$/.test(file)) continue;
  const source = await Deno.readTextFile(file);
  const external = basename(file) === "deno.json"
    ? Object.values(JSON.parse(source).imports ?? {}) as string[]
    : externalImports(source);
  for (const specifier of external) {
    withExternalImports.push(`${basename(file)} -> ${specifier}`);
  }
}
if (withExternalImports.length > 0) {
  console.error("❌ The JSR package would carry dependencies:");
  withExternalImports.forEach(d => console.error("   " + d));
  Deno.exit(1);
}

const staging = await Deno.makeTempDir({ prefix: "fyflow-jsr-" });
for (const { source, target } of stagingPlan(fileUrls, repoRoot, staging)) {
  await Deno.mkdir(dirname(target), { recursive: true });
  await Deno.copyFile(source, target);
}
console.log(`📂 staged to ${staging}`);

// A consumer-supplied worker, deliberately outside the package
await Deno.writeTextFile(join(staging, "consumerWorker.ts"), `
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
await Deno.writeTextFile(join(staging, "smoke.ts"), `
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
  args: ["run", "--allow-read", "--allow-net", join(staging, "smoke.ts")],
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
console.log("✅ No dependencies - every import stays inside the package");
