// Regression tests for the `deno publish --dry-run` parsing in jsr-smoke.ts.
//
// The bug these pin down only appeared on Linux, and development happens on
// Windows, so it reached the mirror's CI unnoticed. Rather than needing a Linux
// host to catch a repeat, every case runs its path maths through an explicitly
// chosen @std/path implementation - so the POSIX branch is exercised here on
// any machine, Windows included.
//
// Run: deno task test:scripts

import { assertEquals } from "jsr:@std/assert@^1.0.0";
import * as posix from "jsr:@std/path@^1.1.2/posix";
import * as windows from "jsr:@std/path@^1.1.2/windows";
import { externalImports, parsePublishedFileUrls, stagingPlan } from "./publishOutput.ts";

// Faithful to the real thing, ANSI colour codes and all: the header is wrapped
// in escapes, so the parser has to strip them before matching.
const HEADER = "\x1b[0m\x1b[1m\x1b[32mSimulating publish\x1b[0m of " +
  "\x1b[0m\x1b[38;5;245m@fyflow/scheduler@0.1.0\x1b[0m with files:";

function dryRunOutput(root: string, files: string[]): string {
  return [
    "\x1b[0m\x1b[32mCheck\x1b[0m index.ts",
    "Checking for slow types in the public API...",
    HEADER,
    ...files.map((f) => `   ${root}/${f} (16.3KB)`),
  ].join("\n");
}

const LINUX_ROOT = "file:///home/runner/work/fyflow-scheduler/fyflow-scheduler";
const WINDOWS_ROOT = "file:///C:/Users/matij/Documents/dev/fyflow-scheduler";
const PUBLISHED = ["README.md", "index.ts", "core/workerWrapper.ts"];

Deno.test("parses the file URLs out of a Linux dry run", () => {
  const urls = parsePublishedFileUrls(dryRunOutput(LINUX_ROOT, PUBLISHED));
  assertEquals(urls, [
    `${LINUX_ROOT}/README.md`,
    `${LINUX_ROOT}/index.ts`,
    `${LINUX_ROOT}/core/workerWrapper.ts`,
  ]);
});

Deno.test("parses the file URLs out of a Windows dry run", () => {
  const urls = parsePublishedFileUrls(dryRunOutput(WINDOWS_ROOT, PUBLISHED));
  assertEquals(urls, [
    `${WINDOWS_ROOT}/README.md`,
    `${WINDOWS_ROOT}/index.ts`,
    `${WINDOWS_ROOT}/core/workerWrapper.ts`,
  ]);
});

Deno.test("ignores everything before the file list", () => {
  // A `Check file:///...` line above the header must not be mistaken for a file.
  const output = [
    "\x1b[0m\x1b[32mCheck\x1b[0m file:///home/runner/work/r/index.ts (9KB)",
    HEADER,
    "   file:///home/runner/work/r/README.md (1KB)",
  ].join("\n");
  assertEquals(parsePublishedFileUrls(output), [
    "file:///home/runner/work/r/README.md",
  ]);
});

Deno.test("Linux staging keeps the leading slash and the whole file name", () => {
  // The exact CI failure: source lost its leading `/` and target lost its `R`.
  //   copy 'home/runner/.../README.md' -> '/tmp/fyflow-jsr-.../EADME.md'
  const urls = parsePublishedFileUrls(dryRunOutput(LINUX_ROOT, PUBLISHED));
  const plan = stagingPlan(urls, LINUX_ROOT + "/", "/tmp/fyflow-jsr-1b98", posix);

  assertEquals(plan[0], {
    source: "/home/runner/work/fyflow-scheduler/fyflow-scheduler/README.md",
    target: "/tmp/fyflow-jsr-1b98/README.md",
  });
  assertEquals(plan[2], {
    source: "/home/runner/work/fyflow-scheduler/fyflow-scheduler/core/workerWrapper.ts",
    target: "/tmp/fyflow-jsr-1b98/core/workerWrapper.ts",
  });
});

Deno.test("Windows staging maps drive-letter paths", () => {
  const urls = parsePublishedFileUrls(dryRunOutput(WINDOWS_ROOT, PUBLISHED));
  const plan = stagingPlan(urls, WINDOWS_ROOT + "/", "C:\\Temp\\fyflow-jsr-1b98", windows);

  assertEquals(plan[0], {
    source: "C:\\Users\\matij\\Documents\\dev\\fyflow-scheduler\\README.md",
    target: "C:\\Temp\\fyflow-jsr-1b98\\README.md",
  });
  assertEquals(plan[2], {
    source: "C:\\Users\\matij\\Documents\\dev\\fyflow-scheduler\\core\\workerWrapper.ts",
    target: "C:\\Temp\\fyflow-jsr-1b98\\core\\workerWrapper.ts",
  });
});

Deno.test("a repo root without a trailing slash stages the same way", () => {
  // jsr-smoke passes `new URL("../", ...)`, which always ends in `/`, but the
  // maths must not silently depend on that.
  const urls = parsePublishedFileUrls(dryRunOutput(LINUX_ROOT, ["README.md"]));
  assertEquals(
    stagingPlan(urls, LINUX_ROOT, "/tmp/s", posix)[0].target,
    "/tmp/s/README.md",
  );
});

Deno.test("percent-encoded paths are decoded", () => {
  // fromFileUrl handles this; the old code needed a separate decodeURIComponent.
  const urls = parsePublishedFileUrls(
    dryRunOutput("file:///home/dev%20work/fyflow", ["README.md"]),
  );
  assertEquals(
    stagingPlan(urls, "file:///home/dev%20work/fyflow/", "/tmp/s", posix)[0],
    { source: "/home/dev work/fyflow/README.md", target: "/tmp/s/README.md" },
  );
});

Deno.test("relative imports are not external", () => {
  const source = `
import { WorkerManager } from "./core/workerManager.ts";
import { FyflowScheduler } from "../core/FyflowScheduler.ts";
export * from "./groups/resourceGroup.ts";
export { ThreadWrapper } from "./core/threadWrapper.ts";
const mod = await import("./core/inlineWrapper.ts");
`;
  assertEquals(externalImports(source), []);
});

Deno.test("import.meta.url is not mistaken for a specifier", () => {
  // ThreadWrapper reaches workerWrapper.ts this way, so this must stay quiet.
  const source = `url = new URL("./workerWrapper.ts", import.meta.url).href;`;
  assertEquals(externalImports(source), []);
});

Deno.test("every flavour of external specifier is caught", () => {
  const source = `
import { join } from "jsr:@std/path@^1.1.2";
import esbuild from "npm:esbuild";
import { parentPort } from "node:worker_threads";
import x from "https://deno.land/std/path/mod.ts";
import y from "@std/path";
const z = await import("npm:chalk");
`;
  assertEquals(externalImports(source), [
    "jsr:@std/path@^1.1.2",
    "npm:esbuild",
    "node:worker_threads",
    "https://deno.land/std/path/mod.ts",
    "@std/path",
    "npm:chalk",
  ]);
});

Deno.test("commented-out imports are not dependencies", () => {
  // threadWrapper.ts really does carry the first of these, for the Node build.
  const source = `
// Uncomment the line below when building for Node.js
// import { Worker } from 'worker_threads';
/* import legacy from "npm:left-pad"; */
/**
 * import { docs } from "jsr:@std/example";
 */
import { WorkerInstanceState } from "./workerInterface.ts";
`;
  assertEquals(externalImports(source), []);
});

Deno.test("stripping comments does not swallow a URL specifier", () => {
  // The `//` in https:// must not be treated as the start of a comment.
  const source = `import x from "https://deno.land/std/path/mod.ts";`;
  assertEquals(externalImports(source), ["https://deno.land/std/path/mod.ts"]);
});
