// Shared test execution for every suite: one place for the per-test timeout and
// the --only / --bail / --timeout flags.
//
// The options are read from the command line here rather than passed down by
// tests/runner.ts, because on Node each suite is its own esbuild bundle with its
// own copy of this module - there is no shared module state to set. The bail
// state lives on globalThis for the same reason. In the browser there are no
// arguments, so every option keeps its default.

// Node.js process declaration for cross-platform compatibility
declare const process: any;

export interface TestResult {
  name: string;
  passed: boolean;
  duration: number;
  error?: string;
  /** Not run: excluded by --only, or after a failure under --bail. Suites drop these before counting. */
  skipped?: boolean;
  /** Failed as its {@link KnownGap} predicted. Counted as passed. */
  knownGap?: string;
}

/**
 * A test that asserts behaviour the code does not have yet, landed ahead of
 * the change that provides it - the target, written down and executed.
 *
 * Strict both ways, so a gap can neither hide a different bug nor outlive its
 * fix:
 * - it must fail, and with an error containing `failsWith`. Failing for any
 *   other reason (a typo, a TypeError) is a real failure.
 * - once it passes, it FAILS, telling you to delete the `knownGap` - the
 *   change that closed it has to say so, in the same commit.
 */
export interface KnownGap {
  /** Why it fails today, and what closes it - e.g. a design-doc phase. */
  reason: string;
  /** A substring of the error the gap produces today. */
  failsWith: string;
}

export interface RunTestOptions {
  knownGap?: KnownGap;
}

export interface HarnessOptions {
  /** Run only tests whose name contains one of these, case-insensitively. Empty runs everything. */
  only: string[];
  /** Skip every remaining test, in every suite, once one fails. */
  bail: boolean;
  /** A test still running after this long fails, so a hang cannot stall the run. */
  timeoutMs: number;
}

export const DEFAULT_TIMEOUT_MS = 60_000;

/** Splits the runner's arguments into positional ones (the suite name) and harness options. */
export function parseArgs(args: readonly string[]): { positional: string[]; options: HarnessOptions } {
  const positional: string[] = [];
  const options: HarnessOptions = { only: [], bail: false, timeoutMs: DEFAULT_TIMEOUT_MS };
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    const [flag, inline] = arg.startsWith('--') && arg.includes('=')
      ? [arg.slice(0, arg.indexOf('=')), arg.slice(arg.indexOf('=') + 1)]
      : [arg, undefined];
    const value = () => {
      const v = inline ?? args[++i];
      if (v === undefined) throw new Error(`${flag} needs a value`);
      return v;
    };
    if (flag === '--only') options.only.push(value().toLowerCase());
    else if (flag === '--bail') options.bail = true;
    else if (flag === '--timeout') {
      const ms = Number(value());
      if (!Number.isFinite(ms) || ms <= 0) throw new Error('--timeout needs a positive number of milliseconds');
      options.timeoutMs = ms;
    } else if (flag.startsWith('--')) throw new Error(`Unknown option ${flag}`);
    else positional.push(arg);
  }
  return { positional, options };
}

function cliArgs(): string[] {
  if (typeof Deno !== 'undefined') return [...Deno.args];
  if (typeof process !== 'undefined' && Array.isArray(process.argv)) return process.argv.slice(2);
  return [];
}

let cached: HarnessOptions | undefined;
export function harnessOptions(): HarnessOptions {
  return cached ??= parseArgs(cliArgs()).options;
}

const BAILED = Symbol.for('fyflow.tests.bailed');
const RAN = Symbol.for('fyflow.tests.ran');
const GAPS = Symbol.for('fyflow.tests.gaps');
const state = globalThis as unknown as { [BAILED]?: boolean; [RAN]?: number; [GAPS]?: string[] };

/** Tests actually run so far, across every suite: a --only that matches nothing must not pass. */
export function testsRun(): number {
  return state[RAN] ?? 0;
}

/**
 * Known gaps that failed as predicted so far, across every suite. Only visible
 * to the runner when suites share its module state - Deno and the browser. On
 * Node each suite is a separate bundle, so each suite prints its own.
 */
export function knownGapsSeen(): string[] {
  return state[GAPS] ?? [];
}

export async function runTest(
  name: string,
  testFn: () => Promise<void>,
  runOptions: RunTestOptions = {},
): Promise<TestResult> {
  const gap = runOptions.knownGap;
  const result = await runTestRaw(name, testFn, gap !== undefined);
  if (!gap || result.skipped) return result;

  if (result.passed) {
    const error = `Known gap now passes - delete its knownGap (${gap.reason})`;
    console.log(`❌ FAILED: ${name} - ${error}`);
    if (harnessOptions().bail) state[BAILED] = true;
    return { ...result, passed: false, error };
  }
  if (!result.error?.includes(gap.failsWith)) {
    const error = `Known gap failed for an unexpected reason: expected "${gap.failsWith}", got "${result.error}"`;
    console.log(`❌ FAILED: ${name} - ${error}`);
    if (harnessOptions().bail) state[BAILED] = true;
    return { ...result, error };
  }
  console.log(`🟠 KNOWN GAP: ${name} - ${gap.reason}`);
  (state[GAPS] ??= []).push(name);
  return { ...result, passed: true, knownGap: gap.reason };
}

/** `quietFailure`: the caller judges the failure (a known gap), so neither report it nor bail. */
async function runTestRaw(name: string, testFn: () => Promise<void>, quietFailure = false): Promise<TestResult> {
  const options = harnessOptions();
  const lower = name.toLowerCase();
  if (options.only.length > 0 && !options.only.some(o => lower.includes(o))) {
    return { name, passed: true, duration: 0, skipped: true };
  }
  if (state[BAILED]) return { name, passed: true, duration: 0, skipped: true };
  state[RAN] = (state[RAN] ?? 0) + 1;

  console.log(`\n🧪 Running: ${name}`);
  const start = performance.now();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      testFn(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`Timed out after ${options.timeoutMs}ms - the test is still running`)),
          options.timeoutMs,
        );
      }),
    ]);
    const duration = performance.now() - start;
    console.log(`✅ PASSED: ${name} (${duration.toFixed(1)}ms)`);
    return { name, passed: true, duration };
  } catch (error: any) {
    const duration = performance.now() - start;
    if (!quietFailure) {
      console.log(`❌ FAILED: ${name} (${duration.toFixed(1)}ms) - ${error?.message}`);
      if (options.bail) state[BAILED] = true;
    }
    return { name, passed: false, duration, error: error?.message };
  } finally {
    clearTimeout(timer);
  }
}
