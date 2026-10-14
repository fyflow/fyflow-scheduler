// FyFlow Test Runner - Unified test execution for all test suites
// Supports running individual suites or all tests

// Node.js process declaration for cross-platform compatibility
declare const process: any;

import { DEFAULT_TIMEOUT_MS, parseArgs, testsRun, type HarnessOptions } from './harness.ts';

class TestRunner {
  private failedSuites: string[] = [];

  get hasFailures(): boolean {
    return this.failedSuites.length > 0;
  }

  async runSuite(name: string, suitePath: string): Promise<void> {
    console.log(`\n🧪 Running ${name} test suite...`);
    console.log('='.repeat(50));

    try {
      const module = await import(suitePath);

      // Check if it's a class-based test suite
      if (module.default && typeof module.default === 'function') {
        const suite = new module.default();
        try {
          // Prevent the suite from exiting the process
          const result = await suite.runAllTests(false);
          // Class-based suites report their own failure counts - surface them
          // so the runner can exit non-zero for CI/CD.
          if (result && result.failed > 0) {
            this.failedSuites.push(`${name} (${result.failed}/${result.totalTests} tests failed)`);
          }
        } finally {
          // Cleanup the suite to prevent resource leaks and infinite loops
          if (typeof suite.cleanup === 'function') {
            await suite.cleanup();
          }
        }
      } else {
        // It's a direct script - importing it runs the tests, and a failing
        // script throws, which lands in the catch below.
        console.log("Direct test script execution completed.");
      }

    } catch (error) {
      console.error(`❌ Failed to run ${name} suite:`, error);
      this.failedSuites.push(name);
    }
  }

  reportFailures(): void {
    if (this.failedSuites.length === 0) return;
    console.error(`\nFailed suites (${this.failedSuites.length}):`);
    for (const suite of this.failedSuites) {
      console.error(`  - ${suite}`);
    }
  }

  async runAllSuites(): Promise<void> {
    console.log('🚀 FyFlow Test Runner - Running All Test Suites');
    console.log('='.repeat(60));

    try {
      // Determine file extension based on runtime environment
      const isNode = typeof Deno === 'undefined' && typeof process !== 'undefined' && process.versions?.node;
      const ext = isNode ? '.js' : '.ts';

      // Run core functionality tests
      await this.runSuite('Core Functionality', `./suites/core${ext}`);

      // Run error handling tests
      await this.runSuite('Error Handling', `./suites/error-handling${ext}`);

      // Run spawning and descendant tracking tests
      await this.runSuite('Spawning', `./suites/spawning${ext}`);

      // Run task settlement tests
      await this.runSuite('Settlement', `./suites/settlement${ext}`);

      // Run resident (worker-lifetime) resource group tests
      await this.runSuite('Resident Groups', `./suites/resident-groups${ext}`);

      // Run resource event tests
      await this.runSuite('Resource Events', `./suites/resource-events${ext}`);


      // Run worker -> host message tests
      await this.runSuite('Worker Messages', `./suites/messages${ext}`);

      // Run the executable documentation examples
      await this.runSuite('Documentation Examples', `./suites/docs${ext}`);

      console.log('\n🎉 All test suites completed!');
    } catch (error) {
      console.error('❌ Test runner failed:', error);
    }
  }

  async runSpecificSuite(suiteName: string): Promise<void> {
    // Determine file extension based on runtime environment
    const isNode = typeof Deno === 'undefined' && typeof process !== 'undefined' && process.versions?.node;
    const ext = isNode ? '.js' : '.ts';

    const suiteMap: Record<string, string> = {
      'core': `./suites/core${ext}`,
      'error': `./suites/error-handling${ext}`,
      'error-handling': `./suites/error-handling${ext}`,
      'spawning': `./suites/spawning${ext}`,
      'settlement': `./suites/settlement${ext}`,
      'docs': `./suites/docs${ext}`,
      'resident': `./suites/resident-groups${ext}`,
      'resident-groups': `./suites/resident-groups${ext}`,
      'events': `./suites/resource-events${ext}`,
      'resource-events': `./suites/resource-events${ext}`,
      'messages': `./suites/messages${ext}`,
      'performance': `./performance/contention-scaling${ext}`
    };

    const suitePath = suiteMap[suiteName.toLowerCase()];
    if (!suitePath) {
      console.error(`❌ Unknown test suite: ${suiteName}`);
      console.log('Available suites:', Object.keys(suiteMap).join(', '));
      this.failedSuites.push(`${suiteName} (unknown suite)`);
      return;
    }

    await this.runSuite(suiteName, suitePath);
  }
}

// Command line interface
async function main() {
  const runner = new TestRunner();

  // Parse command line arguments: one optional suite name, plus the harness
  // options, which each suite also reads for itself (see tests/harness.ts)
  const args = typeof Deno !== 'undefined' ? Deno.args : process.argv.slice(2);
  if (args.includes('--help') || args.includes('-h')) {
    console.log('FyFlow Test Runner');
    console.log('Usage:');
    console.log('  deno run --allow-read --allow-net tests/runner.ts [suite] [options]');
    console.log('  deno task test:<suite> [options]');
    console.log('  pnpm run test:<suite> -- [options]');
    console.log('  pnpm run test:browser      # Browser tests with Playwright');
    console.log('');
    console.log('Available suites:');
    console.log('  core         - Core functionality tests');
    console.log('  error        - Error handling tests');
    console.log('  spawning     - Task spawning and descendant tracking tests');
    console.log('  settlement   - Task settlement and failure accounting tests');
    console.log('  resident     - Resident (worker-lifetime) resource groups');
    console.log('  events       - resource.* events, gauges and the admission queue');
    console.log('  messages     - Worker -> host messages via emit()');
    console.log('  docs         - Executable documentation examples');
    console.log('  performance  - Performance tests');
    console.log('');
    console.log('Options:');
    console.log('  --only <text>   Run only tests whose name contains <text> (case-insensitive).');
    console.log('                  Repeatable. Fails if no test matches.');
    console.log('  --bail          Stop at the first failing test, in every suite.');
    console.log(`  --timeout <ms>  Fail a test still running after <ms> (default ${DEFAULT_TIMEOUT_MS}).`);
    console.log('');
    console.log('Examples:');
    console.log('  tests/runner.ts                                   # Run all suites');
    console.log('  tests/runner.ts core                              # Run only core tests');
    console.log('  tests/runner.ts settlement --only "requeue" --bail');
    return;
  }

  let positional: string[];
  let options: HarnessOptions;
  try {
    ({ positional, options } = parseArgs(args));
  } catch (error: any) {
    console.error(`❌ ${error.message} - see --help`);
    exit(1);
    return;
  }

  if (positional.length > 1) {
    console.error(`❌ One suite at a time, got: ${positional.join(', ')}`);
    exit(1);
    return;
  }

  if (positional.length === 0) {
    // Run all suites by default
    await runner.runAllSuites();
  } else {
    // Run specific suite
    await runner.runSpecificSuite(positional[0]);
  }

  if (options.only.length > 0 && testsRun() === 0) {
    console.error(`
❌ --only ${options.only.map(o => JSON.stringify(o)).join(' --only ')} matched no test`);
    exit(1);
    return;
  }

  // Surface failures to CI/CD via the process exit code
  if (runner.hasFailures) {
    runner.reportFailures();
    exit(1);
  }
}

function exit(code: number): void {
  if (typeof Deno !== 'undefined') {
    Deno.exit(code);
  } else {
    process.exit(code);
  }
}

// Run if this file is executed directly
// Handle both Deno (import.meta.main) and Node.js (require.main === module) patterns
if ((typeof Deno !== 'undefined' && import.meta.main) ||
    (typeof process !== 'undefined' && process.argv[1] && import.meta.url &&
     import.meta.url.endsWith(process.argv[1].replace(/\\/g, '/')))) {
  main();
}

export default TestRunner;