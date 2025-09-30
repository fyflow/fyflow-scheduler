# FyFlow Test Suite

Organized test structure for FyFlow's comprehensive testing framework.

## Structure

```
tests/
├── runner.ts              # Unified test runner
├── suites/                 # Core test suites
│   ├── core.ts            # Core functionality tests
│   └── error-handling.ts  # Error handling tests
├── browser/                # Browser-specific tests (Playwright)
│   ├── test-runner.html   # HTML test runner page
│   ├── core.spec.ts       # Browser core functionality tests
│   ├── workers.spec.ts    # Web Worker specific tests
│   └── performance.spec.ts # Browser performance tests
├── workers/                # Test worker implementations
│   ├── crashingWorker.ts  # Worker that crashes in various ways
│   ├── selfTerminatingWorker.ts # Worker that self-terminates
│   ├── testInlineWorker.ts # Test worker for inline execution
│   └── testThreadWorker.ts # Test worker for thread execution
└── performance/            # Performance and scaling tests
    └── contention-scaling.ts # Contention scaling tests
```

## Running Tests

### Deno (Primary Platform)

#### All Tests
```bash
deno task test
# or
deno run --allow-read --allow-net tests/runner.ts
```

#### Specific Test Suites
```bash
# Core functionality tests
deno task test:core

# Error handling tests
deno task test:error

# Performance tests
deno task test:performance
```

#### Individual Test Files
```bash
# Run a test suite directly
deno run --allow-read --allow-net tests/suites/core.ts
deno run --allow-read --allow-net tests/suites/error-handling.ts
```

### Node.js (Cross-Platform Support)

#### All Tests
```bash
npm test
# or
npm run test
```

#### Specific Test Suites
```bash
# Core functionality tests
npm run test:core

# Error handling tests
npm run test:error

# Performance tests
npm run test:performance
```

**Note**: Node.js test execution requires building first (`npm run build`). This is automatically handled by the npm scripts.

### Browser (Cross-Browser Support with Playwright)

#### All Browser Tests
```bash
npm run test:browser
# Runs tests across Chromium, Firefox, and WebKit
```

#### Browser-Specific Testing
```bash
# Run with browser UI visible
npm run test:browser:headed

# Debug mode with Playwright inspector
npm run test:browser:debug

# Test specific browsers
npm run test:browser:chromium
npm run test:browser:firefox
npm run test:browser:webkit
```

#### Complete Cross-Platform Testing
```bash
# Run all tests across all platforms
npm run test:all
# Equivalent to: npm run test && npm run test:browser
```

**Note**: Browser tests require building first (`npm run build`) and use a local HTTP server automatically started by Playwright.

### Manual Browser Testing

You can also run the browser tests manually by:

1. Building the project: `npm run build`
2. Starting a local server: `npm run serve:test`
3. Opening `http://localhost:3000/tests/browser/test-runner.html` in your browser

## Test Suites

### Core Tests (`suites/core.ts`)
- Basic DAG scheduling functionality
- Worker pool management
- Resource group constraints
- Task dependencies and execution order
- Cross-platform compatibility (Deno/Node.js)

### Error Handling Tests (`suites/error-handling.ts`)
- Worker self-termination scenarios
- Worker crash recovery
- Event emission validation
- Resource cleanup on failures
- Task requeuing mechanisms
- Worker restart behavior

### Performance Tests (`performance/`)
- Contention scaling under resource constraints
- Memory usage patterns
- Throughput measurements

### Browser Tests (`browser/`)

#### Core Browser Tests (`core.spec.ts`)
- FyFlow library loading and imports
- Basic DAG task creation and execution
- Task dependency resolution in browser environment
- Parallel task execution
- Resource constraint handling
- Error handling and recovery

#### Web Worker Tests (`workers.spec.ts`)
- Web Worker creation and messaging
- Real Web Worker task execution (vs inline)
- Concurrent Web Worker management
- Worker error handling and cleanup
- Worker termination and restart
- Mixed inline/Web Worker execution
- Performance comparison between execution modes

#### Browser Performance Tests (`performance.spec.ts`)
- Task throughput measurement
- Memory usage tracking (with performance.memory API)
- Scheduler overhead vs actual work ratio
- Resource contention performance impact
- Cross-browser compatibility benchmarks
- High-volume task handling (500+ tasks)
- Complex dependency graph performance

## Test Workers

### `crashingWorker.ts`
Test worker that can simulate various failure scenarios:
- Constructor crashes
- Setup method failures
- Runtime task execution errors
- Async operation failures

### `selfTerminatingWorker.ts`
Test worker that demonstrates controlled worker termination:
- Self-termination with configurable restart policies
- Delayed termination scenarios
- Metadata preservation

### `testInlineWorker.ts` / `testThreadWorker.ts`
Basic test workers for validating inline vs threaded execution patterns.

## Writing New Tests

### Deno/Node.js Tests
1. **Add to existing suite**: Extend `core.ts` or `error-handling.ts`
2. **Create new suite**: Add to `suites/` and update `runner.ts`
3. **Add test workers**: Place in `workers/` directory
4. **Performance tests**: Add to `performance/` directory

### Browser Tests
1. **Add to existing spec**: Extend `core.spec.ts`, `workers.spec.ts`, or `performance.spec.ts`
2. **Create new spec**: Add to `browser/` directory (must end with `.spec.ts`)
3. **Update HTML runner**: Modify `test-runner.html` if needed for new test types
4. **Build configuration**: Ensure new dependencies are included in `esbuild.config.js`

### Browser Test Guidelines
- Use `test.describe()` and `test()` from `@playwright/test`
- Tests run in real browser environments (Chromium, Firefox, WebKit)
- Access browser APIs through `page.evaluate()` for FyFlow operations
- Use `page.goto('/tests/browser/test-runner.html')` to load test environment
- Verify Web Worker support with `window.Worker !== undefined`
- Test both inline and actual Web Worker execution modes

## CI/CD Integration

Tests are designed for CI/CD environments:

### General
- Exit codes indicate success/failure
- Structured output for parsing
- Configurable timeouts
- Resource cleanup

### Browser Testing in CI
- Playwright automatically installs browser binaries
- Headless execution by default (suitable for CI)
- Cross-browser testing matrix support
- HTML and JSON test reports generated
- Screenshots and videos captured on failure
- Configurable retry policies for flaky tests

### CI Configuration Examples

#### GitHub Actions
```yaml
- name: Install dependencies
  run: npm ci

- name: Install Playwright browsers
  run: npx playwright install

- name: Run all tests
  run: npm run test:all
```

#### Local CI Testing
```bash
# Test the full CI pipeline locally
npm ci
npx playwright install
npm run test:all
```