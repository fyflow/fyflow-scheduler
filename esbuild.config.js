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
      "tests/suites/resident-groups.ts",
      "tests/suites/resource-events.ts",
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

    // Every declaration, ThreadWrapper's included, comes from tsc. A
    // hand-written threadWrapper.d.ts used to be written here first, but tsc
    // overwrote it on every build, so it never shipped - and it had drifted
    // from the class (constructor arguments in the wrong order, missing
    // methods) for anyone reading this file to trust.

    // Create a TypeScript-friendly version of workerWrapperUrl temporarily
    const originalWorkerUrl = fs.readFileSync("./core/workerWrapperUrl.ts", 'utf-8');
    const tempWorkerUrl = `// Temporary stub for TypeScript compilation
export default async function getWorkerUrl(): Promise<string> {
  return "";
}`;

    fs.writeFileSync("./core/workerWrapperUrl.ts", tempWorkerUrl);
    console.log("✓ Temporary workerWrapperUrl stub created");

    try {
      execSync("npx tsc --project tsconfig.json", { stdio: "inherit" });
      console.log("✓ Declarations generated");
    } finally {
      // Restored even when tsc fails: the catch below exits the process, and a
      // source file left as the stub is easy to commit by accident
      fs.writeFileSync("./core/workerWrapperUrl.ts", originalWorkerUrl);
      console.log("✓ Original workerWrapperUrl.ts restored");
    }

    // Fix file extensions in all .d.ts files (change .ts to .js)
    fixFileExtensions(typesDir);
    console.log("✓ File extensions fixed in declaration files");

  } catch (error) {
    console.error("❌ TypeScript declaration generation failed:", error.message);
    process.exit(1);
  }

  console.log("🎉 Library build complete - Node.js, Browser, and TypeScript declarations ready!");
} else {
  console.log("🎉 Development build complete - All files ready for testing!");
}
