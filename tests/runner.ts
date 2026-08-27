// FyFlow Test Runner - Unified test execution for all test suites
// Supports running individual suites or all tests

// Node.js process declaration for cross-platform compatibility
declare const process: any;

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
      'docs': `./suites/docs${ext}`,
      'performance': `./performance/contention-scaling${ext}`
    };

    const suitePath = suiteMap[suiteName.toLowerCase()];
    if (!suitePath) {
      console.error(`❌ Unknown test suite: ${suiteName}`);
      console.log('Available suites:', Object.keys(suiteMap).join(', '));
      return;
    }

    await this.runSuite(suiteName, suitePath);
  }
}

// Command line interface
async function main() {
  const runner = new TestRunner();

  // Parse command line arguments
  const args = typeof Deno !== 'undefined' ? Deno.args : process.argv.slice(2);

  if (args.length === 0) {
    // Run all suites by default
    await runner.runAllSuites();
  } else if (args[0] === '--help' || args[0] === '-h') {
    console.log('FyFlow Test Runner');
    console.log('Usage:');
    console.log('  deno run --allow-read --allow-net tests/runner.ts [suite]');
    console.log('  npm run test [suite]');
    console.log('  npm run test:browser      # Browser tests with Playwright');
    console.log('');
    console.log('Available suites:');
    console.log('  core        - Core functionality tests');
    console.log('  error       - Error handling tests');
    console.log('  spawning    - Task spawning and descendant tracking tests');
    console.log('  docs        - Executable documentation examples');
    console.log('  performance - Performance tests');
    console.log('');
    console.log('Browser testing:');
    console.log('  npm run test:browser         # All browsers');
    console.log('  npm run test:browser:headed  # With browser UI');
    console.log('  npm run test:browser:debug   # Debug mode');
    console.log('  npm run test:all             # Node.js + Browser tests');
    console.log('');
    console.log('Examples:');
    console.log('  tests/runner.ts           # Run all suites');
    console.log('  tests/runner.ts core      # Run only core tests');
    console.log('  tests/runner.ts error     # Run only error handling tests');
  } else {
    // Run specific suite
    await runner.runSpecificSuite(args[0]);
  }

  // Surface failures to CI/CD via the process exit code
  if (runner.hasFailures) {
    runner.reportFailures();
    if (typeof Deno !== 'undefined') {
      Deno.exit(1);
    } else {
      process.exit(1);
    }
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