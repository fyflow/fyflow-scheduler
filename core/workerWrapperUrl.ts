// URL of the worker bootstrap script that ThreadWrapper spawns.
//
// This is the Deno path, and the only one JSR publishes: it points straight at
// the TypeScript source, which Deno loads directly.
//
// Node and browser builds never see this file. esbuild aliases it to
// `workerWrapperUrl.bundled.ts`, which inlines the bootstrap as a data: or Blob
// URL so npm consumers need no bundler configuration. That alias exists because
// the bundled variant imports `./workerWrapper.ts?worker`, and a query-suffixed
// specifier lands in Deno's module graph without being publishable - it made the
// whole package fail `deno publish` with `excluded-module`.
let url: string;

export default function getWorkerUrl(): Promise<string> {
  if (!url) {
    if (!("Deno" in globalThis)) {
      // Reachable only when the JSR package is imported outside Deno. Without
      // this the failure is a confusing "cannot load .ts" from deep inside the
      // worker spawn.
      return Promise.reject(new Error(
        "@fyflow/scheduler is the Deno build and cannot spawn workers on this runtime. " +
        "Install the npm package instead: npm install fyflow-scheduler"
      ));
    }
    url = new URL("./workerWrapper.ts", import.meta.url).href;
  }
  return Promise.resolve(url);
}
