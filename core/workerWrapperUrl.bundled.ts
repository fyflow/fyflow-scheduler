// Bundled builds only - esbuild aliases `workerWrapperUrl.ts` to this file for
// the Node and browser targets. Deno never imports it, and it is excluded from
// the JSR package.
//
// The `?worker` suffix is handled by the workerPlugin in esbuild.config.js: it
// builds `workerWrapper.ts` (or `workerWrapper.node.ts` on the Node target) as a
// standalone bundle and inlines it as a base64 `data:` URL for Node or a `Blob`
// URL for the browser, so npm consumers need no bundler configuration.
//
// This cannot live in the published module: Deno resolves `?worker` into its
// module graph but refuses to publish it, which fails the whole package with
// `error[excluded-module]`.
let url: string;

export default async function getWorkerUrl(): Promise<string> {
  if (url) return url;

  // @ts-expect-error - esbuild resolves the ?worker query parameter at build time
  url = (await import("./workerWrapper.ts?worker")).default;
  return url;
}
