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
  // One worker, locally as in CI, not Playwright's default of half the cores.
  // Each worker is a browser running scheduler threads of its own, and on a
  // 20-core Windows machine the default (10) failed 6-9 Firefox tests per run -
  // page loads in beforeEach and context teardown timing out, a different set
  // each time - with Playwright's Firefox driver logging lost sessions. Two
  // workers looked enough (1 failure in 4 runs) and then failed 6 Firefox tests
  // in one run. Serial has not failed here: Firefox 25/25 three times, the full
  // suite 72/72 in ~2 min. Deliberately no local retries: a retried hang is
  // still a hang.
  workers: 1,

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