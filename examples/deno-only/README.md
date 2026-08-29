# Deno-only examples

Everything in this folder targets **Deno only** and is deliberately excluded from
the Node and browser builds.

The examples one level up are written to run on both runtimes, which means they
carry this repository's `?worker-direct` build convention:

```typescript
// examples/worker-types.ts - repo-internal, do NOT copy into your project
workerUrl = new URL((await import("./workers/cpuWorker.ts?worker-direct")).default).href;
```

That suffix is handled by the esbuild plugin in this repo's `esbuild.config.js`.
It is not part of either published package, and it will not resolve in your code.

These examples show the plain form instead — no branch, no build step, no
bundler convention — which is exactly what a consumer of
`jsr:@fyflow/scheduler` writes:

```typescript
const workerUrl = new URL("./workers/simpleWorker.ts", import.meta.url).href;
```

Node and browser consumers use the npm package and ship a compiled `.js` worker:

```javascript
const workerUrl = new URL("./myWorker.js", import.meta.url).href;
```

See the "Loading Workers" section of the README for both.

## Running

```bash
deno run --allow-read --allow-net examples/deno-only/minimal.ts
```
