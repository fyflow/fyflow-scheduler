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
const state = globalThis as unknown as { [BAILED]?: boolean; [RAN]?: number };

/** Tests actually run so far, across every suite: a --only that matches nothing must not pass. */
export function testsRun(): number {
  return state[RAN] ?? 0;
}

export async function runTest(name: string, testFn: () => Promise<void>): Promise<TestResult> {
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
    console.log(`❌ FAILED: ${name} (${duration.toFixed(1)}ms) - ${error?.message}`);
    if (options.bail) state[BAILED] = true;
    return { name, passed: false, duration, error: error?.message };
  } finally {
    clearTimeout(timer);
  }
}
