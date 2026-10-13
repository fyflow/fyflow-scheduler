// deno-lint-ignore-file no-process-global -- Node-only Playwright config; `process.env` is the correct API here.
import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright Configuration for FyFlow Browser Testing
 * Tests FyFlow's DAG scheduler and worker functionality across browsers
 */
export default defineConfig({
  // Test directory for browser-specific tests
  testDir: './tests/browser',

  // Run tests in files in parallel
  fullyParallel: true,

  // Fail the build on CI if you accidentally left test.only in the source code
  forbidOnly: !!process.env.CI,

  // Retry on CI only
  retries: process.env.CI ? 2 : 0,

  // Opt out of parallel tests on CI
  // Capped locally, not left to Playwright's default of half the cores. Each
  // worker is a browser running scheduler threads of its own, and on a 20-core
  // Windows machine the default (10) failed 6-9 Firefox tests per run - page
  // loads and context teardown timing out - with a different set each time,
  // and occasionally one WebKit test. Measured on the full suite: 2 workers
  // failed 1 test in 4 runs, in ~1.5 min - faster than the failing default run
  // (4.7 min); 4 workers failed once in three runs. The remaining failure is
  // Firefox's first page load in a worker taking 25-33s against the 30s test
  // timeout, with Playwright's Firefox driver logging lost sessions.
  // Deliberately no local retries: a retried hang is still a hang.
  workers: process.env.CI ? 1 : 2,

  // Reporter to use
  reporter: [
    ['list'],
    ['html', { outputFolder: 'playwright-report' }],
    ['json', { outputFile: 'test-results/results.json' }]
  ],

  // Shared settings for all projects
  use: {
    // Base URL for tests (will serve built browser files)
    baseURL: 'http://localhost:3000',

    // Collect trace when retrying the failed test
    trace: 'on-first-retry',

    // Screenshot on failure
    screenshot: 'only-on-failure',

    // Record video on failure
    video: 'retain-on-failure',
  },

  // Configure projects for major browsers
  projects: [
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        // Enable Web Workers and ESM modules
        contextOptions: {
          permissions: ['clipboard-read', 'clipboard-write']
        }
      },
    },

    {
      name: 'firefox',
      use: { ...devices['Desktop Firefox'] },
    },

    {
      name: 'webkit',
      use: { ...devices['Desktop Safari'] },
    },

    // Mobile testing (optional - can be enabled later)
    // {
    //   name: 'Mobile Chrome',
    //   use: { ...devices['Pixel 5'] },
    // },
    // {
    //   name: 'Mobile Safari',
    //   use: { ...devices['iPhone 12'] },
    // },
  ],

  // Run your local dev server before starting the tests
  webServer: {
    command: 'npm run serve:test',
    port: 3000,
    reuseExistingServer: !process.env.CI,
    stdout: 'ignore',
    stderr: 'pipe',
  },

  // Test timeout
  timeout: 30000,

  // Global test timeout
  globalTimeout: 600000,

  // Expect timeout for assertions
  expect: {
    timeout: 10000
  },

  // Output directory for test artifacts
  outputDir: 'test-results/',
});