// esbuild.config.js

import { build } from "esbuild";
import { Buffer } from "node:buffer";
import { execSync } from "node:child_process";
import fs from 'node:fs';
import path from "node:path";

// Determine build type from command line args
const args = process.argv.slice(2);
const buildType = args.includes('--dev') ? 'development' : 'library';

// const injectWorkerImport = {
//     name: "inject-worker-import",
//     setup(build) {
//       build.onLoad({ filter: /my-worker-using-file\.ts$/ }, async (args) => {
//         let fs = await import("fs/promises");
//         let source = await fs.readFile(args.path, "utf8");
  
//         // Prepend the import
//         const contents = `import { Worker } from "node:worker_threads";\n${source}`;
//         return { contents, loader: "ts" };
//       });
//     },
//   };

const workerPlugin = {
  name: "worker-loader",
  setup(build) {
    build.onResolve({ filter: /\?worker(-direct)?$/ }, args => {
      //console.log('onResolve', args);
      
      // remove ?worker
      const isDirect = args.path.includes("?worker-direct");
      const file = args.path.replace(/\?worker(-direct)?$/, "");
      //use resolveDir to create full path
      const fullPath = path.resolve(args.resolveDir, file);
      
      //console.log('fullPath', fullPath);
      
      // choose worker variant at build-time
      if (build.initialOptions.platform === "node") {
        return { path: fullPath.replace(".ts", isDirect ? ".ts" : ".node.ts"), namespace: "worker" };
      }
      return { path: fullPath, namespace: "worker" };
    });

    build.onLoad({ filter: /.*/, namespace: "worker" }, async (args) => {
        //console.log('onLoad', args);
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

const injectWorkerImport = {
    name: "inject-worker-import",
    setup(build) {
      build.onLoad({ filter: /threadWrapper\.ts$/ }, async (args) => {
        let fs = await import("fs/promises");
        let source = await fs.readFile(args.path, "utf8");
  
        // Prepend the import
        const contents = `import { Worker } from "node:worker_threads";\n${source}`;
        return { contents, loader: "ts" };
      });
    },
  };

if (buildType === 'development') {
  console.log("🔨 Building development files...");

  // Development Build for Node
  await build({
    entryPoints: [
      "index.ts",
      "examples/performance-groups.ts",
      "tests/runner.ts",
      "tests/suites/core.ts",
      "tests/suites/error-handling.ts",
      "benchmark/runBenchmarks.ts"
    ],
    bundle: true,
    target: ["es2022"],
    platform: "node",
    format: "esm",
    outdir: "dev-dist/node",
    plugins: [workerPlugin, injectWorkerImport],
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
    plugins: [workerPlugin],
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
    plugins: [workerPlugin, injectWorkerImport],
  });

  console.log("✓ Node.js library build complete");

  // Library Build for Browser
  await build({
    entryPoints: ["index.ts"],
    bundle: true,
    platform: "browser",
    format: "esm",
    outdir: "dist/browser",
    plugins: [workerPlugin],
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

  // Manual declaration for ThreadWrapper
  const threadWrapperDeclaration = `import { WorkerInstanceExtensions, WorkerInstanceState } from "./workerInterface.js";
export declare class ThreadWrapper extends EventTarget implements WorkerInstanceExtensions, WorkerInstanceState {
    worker: Worker | null;
    runningTasks: number;
    maxConcurrentTasks: number;
    idleTimeout: number;
    callbacks: Map<string, {resolve: Function, reject: Function}>;
    taskStartTimes: Map<string, number>;
    runningTaskData: Map<string, {id: string, payload: any}>;
    idleTimer: NodeJS.Timeout | null;
    config: any;
    id: string;
    initialized: boolean;
    originalScriptUrl: string;
    initializing: boolean;

    constructor(scriptUrl: string, maxConcurrentTasks: number, idleTimeout: number, config?: any);
    initialize(): Promise<void>;
    run(payload: any, taskId: string): Promise<any>;
    terminate(): Promise<void>;
    isIdle(): boolean;
    canAcceptTasks(): boolean;
    getStatus(): any;
}`;

  fs.writeFileSync(path.join(coreTypesDir, "threadWrapper.d.ts"), threadWrapperDeclaration);
  console.log("✓ Manual ThreadWrapper declaration created");

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
  function fixFileExtensions(dir) {
    const files = fs.readdirSync(dir, { withFileTypes: true });
    for (const file of files) {
      const fullPath = path.join(dir, file.name);
      if (file.isDirectory()) {
        fixFileExtensions(fullPath);
      } else if (file.name.endsWith('.d.ts')) {
        let content = fs.readFileSync(fullPath, 'utf-8');
        // Replace .ts extensions with .js in import/export statements
        content = content.replace(/from "([^"]+)\.ts"/g, 'from "$1.js"');
        content = content.replace(/} from "([^"]+)\.ts"/g, '} from "$1.js"');
        fs.writeFileSync(fullPath, content);
      }
    }
  }

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

