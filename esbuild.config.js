// esbuild.config.js
// deno-lint-ignore-file no-process-global -- Node-only build script; `process` is the correct API here.

import { build } from "esbuild";
import { Buffer } from "node:buffer";
import { execSync } from "node:child_process";
import fs from 'node:fs';
import path from "node:path";

// Determine build type from command line args
const args = process.argv.slice(2);
const buildType = args.includes('--dev') ? 'development' : 'library';

// Clear the output directory before building. Without this, declarations and
// bundles for deleted modules survive - dist/types/core/dagScheduler.d.ts
// outlived its source by months, and package.json ships dist/**/*, so it would
// have been published.
const outputDir = buildType === 'development' ? 'dev-dist' : 'dist';
if (fs.existsSync(outputDir)) {
  fs.rmSync(outputDir, { recursive: true, force: true });
  console.log(`🧹 Cleared ${outputDir}/`);
}

// Deno loads core/workerWrapper.ts directly, so the published workerWrapperUrl.ts
// is Deno-only. Bundled targets need the variant that inlines the worker via the
// ?worker plugin, which Deno cannot publish - swap it in here rather than
// branching at runtime.
const bundledWorkerUrlPlugin = {
  name: "bundled-worker-url",
  setup(build) {
    build.onResolve({ filter: /workerWrapperUrl\.ts$/ }, args => {
      if (args.path.includes("workerWrapperUrl.bundled.ts")) return null;
      return {
        path: path.resolve(args.resolveDir, args.path.replace("workerWrapperUrl.ts", "workerWrapperUrl.bundled.ts"))
      };
    });
  }
};

const workerPlugin = {
  name: "worker-loader",
  setup(build) {
    build.onResolve({ filter: /\?worker(-direct)?$/ }, args => {
      // remove ?worker
      const isDirect = args.path.includes("?worker-direct");
      const file = args.path.replace(/\?worker(-direct)?$/, "");
      //use resolveDir to create full path
      const fullPath = path.resolve(args.resolveDir, file);

      // choose worker variant at build-time
      if (build.initialOptions.platform === "node") {
        return { path: fullPath.replace(".ts", isDirect ? ".ts" : ".node.ts"), namespace: "worker" };
      }
      return { path: fullPath, namespace: "worker" };
    });

    build.onLoad({ filter: /.*/, namespace: "worker" }, async (args) => {
      const result = await build.esbuild.build({
        entryPoints: [args.path],
        platform: build.initialOptions.platform,
        bundle: true,
        format: "esm",
        write: false,
      });
      const code = result.outputFiles[0].text;

      // Node → data URL, Browser → Blob
      return {
        contents: `
          let url;
          ${
            build.initialOptions.platform === "node"
              ? `url = "data:text/javascript;base64,${Buffer.from(code).toString("base64")}";`
              : `const blob = new Blob([${JSON.stringify(code)}], { type: "application/javascript" });
                 url = URL.createObjectURL(blob);`
          }
          export default new URL(url);
        `,
        loader: "js",
      };
    });
  },
};

// Inject Node.js Worker import for ThreadWrapper (Node platform only)
const injectWorkerImport = {
  name: "inject-worker-import",
  setup(build) {
    build.onLoad({ filter: /threadWrapper\.ts$/ }, async (args) => {
      const fs = await import("fs/promises");
      const source = await fs.readFile(args.path, "utf8");

      // Prepend the import for Node.js worker_threads
      const contents = `import { Worker } from "node:worker_threads";\n${source}`;
      return { contents, loader: "ts" };
    });
  },
};

// Rewrite .ts import/export specifiers to .js throughout a tree of .d.ts files.
function fixFileExtensions(dir) {
  const files = fs.readdirSync(dir, { withFileTypes: true });
  for (const file of files) {
    const fullPath = path.join(dir, file.name);
    if (file.isDirectory()) {
      fixFileExtensions(fullPath);
    } else if (file.name.endsWith('.d.ts')) {
      let content = fs.readFileSync(fullPath, 'utf-8');
      content = content.replace(/from "([^"]+)\.ts"/g, 'from "$1.js"');
      content = content.replace(/} from "([^"]+)\.ts"/g, '} from "$1.js"');
      fs.writeFileSync(fullPath, content);
    }
  }
}

if (buildType === 'development') {
  console.log("🔨 Building development files...");

  // Development Build for Node
  await build({
    entryPoints: [
      "index.ts",
      // Non-recursive on purpose: examples/deno-only/ must NOT be built for
      // Node or the browser - it demonstrates the plain Deno worker URL form
      "examples/*.ts",
      "tests/runner.ts",
      "tests/suites/core.ts",
      "tests/suites/error-handling.ts",
      "tests/suites/spawning.ts",
      "tests/suites/settlement.ts",
      "tests/suites/docs.ts",
      "tests/performance/contention-scaling.ts",
      "benchmark/runBenchmarks.ts"
    ],
    bundle: true,
    target: ["es2022"],
    platform: "node",
    format: "esm",
    outdir: "dev-dist/node",
    plugins: [bundledWorkerUrlPlugin, workerPlugin, injectWorkerImport],
  });

  console.log("✓ Node.js development build complete");

  // Development Build for Browser
  await build({
    entryPoints: [
      "index.ts",
      "examples/getting-started.ts",
      "tests/runner.ts",
      "tests/suites/core.ts",
      "tests/suites/error-handling.ts",
      "tests/workers/testInlineWorker.ts",
      "tests/workers/testThreadWorker.ts",
      "tests/workers/crashingWorker.ts",
      "tests/workers/selfTerminatingWorker.ts",
      "groups/concurrentLimitGroup.ts",
      "groups/rateLimitGroup.ts"
    ],
    bundle: true,
    platform: "browser",
    format: "esm",
    outdir: "dev-dist/browser",
    plugins: [bundledWorkerUrlPlugin, workerPlugin],
  });

  console.log("✓ Browser development build complete");

} else {
  console.log("📦 Building library files...");

  // Library Build for Node
  await build({
    entryPoints: ["index.ts"],
    bundle: true,
    target: ["es2022"],
    platform: "node",
    format: "esm",
    outdir: "dist/node",
    plugins: [bundledWorkerUrlPlugin, workerPlugin, injectWorkerImport],
  });

  console.log("✓ Node.js library build complete");

  // Library Build for Browser
  await build({
    entryPoints: ["index.ts"],
    bundle: true,
    platform: "browser",
    format: "esm",
    outdir: "dist/browser",
    plugins: [bundledWorkerUrlPlugin, workerPlugin],
  });

  console.log("✓ Browser library build complete");
}

// Generate TypeScript declarations (only for library build)
if (buildType === 'library') {
  try {
    console.log("🔨 Generating TypeScript declarations...");

    const typesDir = "./dist/types";
    const coreTypesDir = path.join(typesDir, "core");

    // Ensure directories exist
    fs.mkdirSync(typesDir, { recursive: true });
    fs.mkdirSync(coreTypesDir, { recursive: true });

    // Generate the ThreadWrapper declaration. Hand-written rather than derived
    // from the source, because the Worker types do not survive the round trip.
    const threadWrapperDeclaration = `import { WorkerInstanceExtensions, WorkerInstanceState } from "./workerInterface.js";

/**
 * ThreadWrapper - Worker thread wrapper for concurrent task execution
 *
 * Automatically generated declaration for Node.js Worker compatibility
 */
export declare class ThreadWrapper extends EventTarget implements WorkerInstanceExtensions, WorkerInstanceState {
    worker: Worker | null;
    runningTasks: number;
    maxConcurrentTasks: number;
    idleTimeout: number;
    callbacks: Map<string, {resolve: Function, reject: Function}>;
    taskStartTimes: Map<string, number>;
    runningTaskData: Map<string, {id: string, payload: any}>;
    taskQueue: Array<{taskId: string, payload: any, resolve: Function, reject: Function}>;
    idleTimer: NodeJS.Timeout | null;
    config: any;
    id: string;
    initialized: boolean;
    originalScriptUrl: string;
    initializing: boolean;

    constructor(scriptUrl: string, maxConcurrentTasks: number, idleTimeout: number, config?: any);
    initialize(): Promise<void>;
    runTask(taskId: string, payload: any): Promise<any>;
    terminate(): Promise<void>;
    isIdle(): boolean;
    canAcceptTask(): boolean;
    getStatus(): any;
    updateActivity(): void;
    private _ensureInitialized(): Promise<void>;
    private _processTaskQueue(): void;
    private _resetIdleTimer(): void;
}`;

    fs.writeFileSync(path.join(coreTypesDir, "threadWrapper.d.ts"), threadWrapperDeclaration);
    console.log("✓ ThreadWrapper declaration created");

    // Create a TypeScript-friendly version of workerWrapperUrl temporarily
    const originalWorkerUrl = fs.readFileSync("./core/workerWrapperUrl.ts", 'utf-8');
    const tempWorkerUrl = `// Temporary stub for TypeScript compilation
export default async function getWorkerUrl(): Promise<string> {
  return "";
}`;

    fs.writeFileSync("./core/workerWrapperUrl.ts", tempWorkerUrl);
    console.log("✓ Temporary workerWrapperUrl stub created");

    // Generate remaining declarations (excluding problematic files)
    execSync("npx tsc --project tsconfig.json", { stdio: "inherit" });
    console.log("✓ Main declarations generated");

    // Restore original workerWrapperUrl.ts
    fs.writeFileSync("./core/workerWrapperUrl.ts", originalWorkerUrl);
    console.log("✓ Original workerWrapperUrl.ts restored");

    // Fix file extensions in all .d.ts files (change .ts to .js)
    fixFileExtensions(typesDir);

    // Add ThreadWrapper export to main index.d.ts
    const indexDtsPath = path.join(typesDir, "index.d.ts");
    if (fs.existsSync(indexDtsPath)) {
      let indexContent = fs.readFileSync(indexDtsPath, 'utf-8');
      // Add the ThreadWrapper export only if not already present
      if (!indexContent.includes('export { ThreadWrapper }')) {
        indexContent = indexContent.replace(
          'export { InlineWrapper } from "./core/inlineWrapper.js";',
          'export { ThreadWrapper } from "./core/threadWrapper.js";\nexport { InlineWrapper } from "./core/inlineWrapper.js";'
        );
        fs.writeFileSync(indexDtsPath, indexContent);
        console.log("✓ ThreadWrapper export added to index.d.ts");
      } else {
        console.log("✓ ThreadWrapper export already present in index.d.ts");
      }
    }

    console.log("✓ File extensions fixed in declaration files");

  } catch (error) {
    console.error("❌ TypeScript declaration generation failed:", error.message);
    process.exit(1);
  }

  console.log("🎉 Library build complete - Node.js, Browser, and TypeScript declarations ready!");
} else {
  console.log("🎉 Development build complete - All files ready for testing!");
}
