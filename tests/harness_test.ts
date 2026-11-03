// Unit tests for the known-gap rules in tests/harness.ts. A gap that could pass
// silently, or fail for a reason other than the one it names, would hide
// exactly the regressions it was written to track.
//
// Run: deno task test:scripts

import { assertEquals, assertStringIncludes } from 'jsr:@std/assert@1';
import { runTest } from './harness.ts';

const gap = { reason: 'R2 closes it', failsWith: 'never started' };

Deno.test('a known gap that fails as predicted counts as passed', async () => {
  const result = await runTest('gap', () => Promise.reject(new Error('"w" never started within 400ms')), { knownGap: gap });
  assertEquals(result.passed, true);
  assertEquals(result.knownGap, 'R2 closes it');
});

Deno.test('a known gap that passes fails, telling you to remove it', async () => {
  const result = await runTest('gap', () => Promise.resolve(), { knownGap: gap });
  assertEquals(result.passed, false);
  assertStringIncludes(result.error ?? '', 'delete its knownGap');
});

Deno.test('a known gap that fails for another reason is a real failure', async () => {
  const result = await runTest('gap', () => Promise.reject(new TypeError('x is undefined')), { knownGap: gap });
  assertEquals(result.passed, false);
  assertStringIncludes(result.error ?? '', 'unexpected reason');
  assertEquals(result.knownGap, undefined);
});

Deno.test('a test without a known gap is unchanged', async () => {
  assertEquals((await runTest('ok', () => Promise.resolve())).passed, true);
  const failed = await runTest('bad', () => Promise.reject(new Error('boom')));
  assertEquals(failed.passed, false);
  assertEquals(failed.error, 'boom');
});
