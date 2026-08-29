// Runs every example and fails if one errors or hangs.
//
// This exists because nothing executed the examples: `deno task check` only
// type-checks them, so enhanced-features.ts and worker-types.ts were broken on
// Node for an unknown length of time while still being bundled into
// dev-dist/node/examples, which made them look supported.
//
// Run: deno task examples

const repoRoot = new URL("../", import.meta.url);
const rootPath = new URL(repoRoot).pathname.replace(/^\/([A-Za-z]:)/, "$1").replace(/\/$/, "");
// The slowest example takes ~12s, so this is a generous margin that still gives
// quick feedback when one hangs
const TIMEOUT_MS = 60_000;

// Skipped, with a reason. Anything not listed here runs, so a new example is
// covered by default rather than needing to be remembered.
const SKIP: Record<string, string> = {
  "test-deadlock.ts": "scratch scenario - saturates 30 threads and does not terminate"
};

interface Result { name: string; runtime: string; ok: boolean; detail: string; }
const results: Result[] = [];

async function run(name: string, runtime: string, cmd: string, args: string[]) {
  const started = performance.now();
  const process = new Deno.Command(cmd, { args, cwd: rootPath, stdout: "piped", stderr: "piped" }).spawn();

  const timer = setTimeout(() => { try { process.kill("SIGKILL"); } catch { /* already gone */ } }, TIMEOUT_MS);
  const { code } = await process.output();
  clearTimeout(timer);

  const elapsed = ((performance.now() - started) / 1000).toFixed(1);
  const timedOut = performance.now() - started >= TIMEOUT_MS - 500;
  const ok = code === 0 && !timedOut;

  results.push({
    name, runtime,
    ok,
    detail: ok ? `${elapsed}s` : (timedOut ? `HUNG after ${elapsed}s` : `exit ${code} after ${elapsed}s`)
  });
  console.log(`${ok ? "✅" : "❌"} ${runtime.padEnd(6)} ${name.padEnd(34)} ${results[results.length - 1].detail}`);
}

// --- Deno: every example, including the deno-only ones -----------------------
const denoExamples: string[] = [];
for await (const entry of Deno.readDir(new URL("examples/", repoRoot))) {
  if (entry.isFile && entry.name.endsWith(".ts")) denoExamples.push(entry.name);
}
const denoOnly: string[] = [];
for await (const entry of Deno.readDir(new URL("examples/deno-only/", repoRoot))) {
  if (entry.isFile && entry.name.endsWith(".ts")) denoOnly.push(entry.name);
}
denoExamples.sort();
denoOnly.sort();

console.log("Running examples on Deno");
for (const name of denoExamples) {
  if (SKIP[name]) { console.log(`⏭️  deno   ${name.padEnd(34)} skipped - ${SKIP[name]}`); continue; }
  await run(name, "deno", Deno.execPath(), ["run", "--allow-read", "--allow-net", "--allow-write", `examples/${name}`]);
}
for (const name of denoOnly) {
  await run(`deno-only/${name}`, "deno", Deno.execPath(), ["run", "--allow-read", "--allow-net", `examples/deno-only/${name}`]);
}

// --- Node: the bundled examples ----------------------------------------------
// deno-only/ is deliberately absent from the build, which is the point of it.
console.log("\nRunning examples on Node");
let nodeVersion = "";
try {
  const probe = await new Deno.Command("node", { args: ["--version"], stdout: "piped", stderr: "piped" }).output();
  nodeVersion = new TextDecoder().decode(probe.stdout).trim();
} catch {
  nodeVersion = "";
}
const major = Number(nodeVersion.replace(/^v/, "").split(".")[0]);

if (!nodeVersion || !(major >= 22)) {
  console.log(`⏭️  node   skipped - needs Node >= 22 on PATH, found ${nodeVersion || "none"}`);
  console.log("   (the Node half is where enhanced-features and worker-types were broken - run it before releasing)");
} else {
  let built = true;
  try { await Deno.stat(new URL("dev-dist/node/examples/", repoRoot)); } catch { built = false; }
  if (!built) {
    console.log("⏭️  node   skipped - run `npm run build:dev` first");
  } else {
    for (const name of denoExamples) {
      if (SKIP[name]) { console.log(`⏭️  node   ${name.padEnd(34)} skipped - ${SKIP[name]}`); continue; }
      await run(name, "node", "node", [`dev-dist/node/examples/${name.replace(/\.ts$/, ".js")}`]);
    }
  }
}

const failed = results.filter(r => !r.ok);
console.log(`\n${results.length - failed.length}/${results.length} example runs passed`);
if (failed.length > 0) {
  console.log("\nFailures:");
  failed.forEach(f => console.log(`  • ${f.runtime} ${f.name}: ${f.detail}`));
  Deno.exit(1);
}
