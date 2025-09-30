// workerUrl.ts
let url: string;

// Platform detection without direct Deno reference
declare const Deno: any;

export default async function getWorkerUrl(): Promise<string> {
  if (url) return url;

  // Check if we're in Deno environment
  if (typeof globalThis !== 'undefined' && 'Deno' in globalThis) {
    url = new URL("./workerWrapper.ts", import.meta.url).href;
  } else {
    // Node/Browser esbuild replaces this
    // @ts-expect-error - esbuild will handle the ?worker query parameter at build time
    url = (await import("./workerWrapper.ts?worker")).default;
  }
  return url;
}
