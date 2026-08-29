// Parsing and path maths for the `deno publish --dry-run` file list.
//
// Split out of jsr-smoke.ts so it can be tested: that script runs its whole
// smoke test at import time, so there is nothing importable to assert on.
//
// This existed inline as hand-rolled regexes over `file:///` URLs and broke on
// Linux in two ways at once. `/^file:\/\/\/(.+?) \(/` captured after the third
// slash, which is right for `file:///C:/...` but eats the leading slash of
// `file:///home/...`. The repo root was computed by a drive-letter regex that
// simply did not match on Linux, so it kept its leading slash - leaving the two
// off by one character, and `slice(root.length + 1)` then cut a character off
// every file name. CI copied `home/runner/.../README.md` to `.../EADME.md`.
//
// Everything here goes through @std/path rather than string surgery, and takes
// its path implementation as a parameter so the POSIX branch can be exercised
// from any host - see publishOutput_test.ts.

import * as nativePath from "jsr:@std/path@^1.1.2";

/** The slice of @std/path this module needs, so tests can supply posix or windows. */
export interface PathApi {
  fromFileUrl(url: string | URL): string;
  relative(from: string, to: string): string;
  join(...parts: string[]): string;
}

export interface StagedFile {
  /** Absolute path of the file as it exists in the repo. */
  source: string;
  /** Absolute path it should be copied to inside the staging directory. */
  target: string;
}

/**
 * The `file://` URLs `deno publish --dry-run` says it would publish.
 *
 * Returns URLs rather than paths: the URL form is identical on every platform,
 * so the caller decides when to convert and tests can assert without a host.
 */
export function parsePublishedFileUrls(output: string): string[] {
  const urls: string[] = [];
  let collecting = false;
  for (const rawLine of output.split("\n")) {
    // deno-lint-ignore no-control-regex -- stripping ANSI colour codes from deno publish output
    const line = rawLine.replace(/\x1b\[[0-9;]*m/g, "").trim();
    if (line.startsWith("Simulating publish")) { collecting = true; continue; }
    if (!collecting) continue;
    // Capture the whole URL. Anything narrower has to know where the path
    // starts, which is the platform-specific part that broke before.
    const match = line.match(/^(file:\/\/\S+) \(/);
    if (!match) continue;
    urls.push(match[1]);
  }
  return urls;
}

// Matches the specifier of a static import/export or a dynamic import().
// `import.meta.url` does not match: `.` is neither a quote nor an opening paren.
const SPECIFIER = /(?:\bfrom\s*|\bimport\s*\(\s*|\bimport\s+)["']([^"']+)["']/g;
const BLOCK_COMMENT = /\/\*[\s\S]*?\*\//g;
// `//` not preceded by a colon, so `https://` survives as part of a specifier.
const LINE_COMMENT = /(?<!:)\/\/.*$/gm;

/**
 * Every module specifier in `source` that is not relative.
 *
 * The published package has no dependencies: every import in it points at
 * another file in the package. Anything else - `jsr:`, `npm:`, `node:`, a URL,
 * or a bare specifier needing an import map - is a dependency a consumer would
 * have to resolve.
 *
 * Textual, not a parser, so comments are stripped first: threadWrapper.ts
 * carries a commented-out `import { Worker } from 'worker_threads'` for the
 * Node build, and that is documentation, not a dependency.
 */
export function externalImports(source: string): string[] {
  const code = source.replace(BLOCK_COMMENT, "").replace(LINE_COMMENT, "");
  const external: string[] = [];
  for (const [, specifier] of code.matchAll(SPECIFIER)) {
    if (specifier.startsWith("./") || specifier.startsWith("../")) continue;
    external.push(specifier);
  }
  return external;
}

/**
 * Where each published file lands under `staging`, preserving its path relative
 * to the repo root.
 */
export function stagingPlan(
  fileUrls: string[],
  repoRoot: string | URL,
  staging: string,
  path: PathApi = nativePath,
): StagedFile[] {
  const rootPath = path.fromFileUrl(repoRoot);
  return fileUrls.map((url) => {
    const source = path.fromFileUrl(url);
    return { source, target: path.join(staging, path.relative(rootPath, source)) };
  });
}
