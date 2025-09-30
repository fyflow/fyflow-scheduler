import { BaseWorker, WorkerConfig } from "../index.ts";
function heavyWork(iterations = 1000) {
    // A tight math loop that's reasonably heavy per iteration.
    // Doing many trig + pow ops prevents JS engines from optimizing it away.
    let x = 0.123456789;
    for (let i = 0; i < iterations; i++) {
      x += Math.sin(x) * Math.cos(x) + Math.tan(x % 1);
      x = x - Math.floor(x); // keep within 0..1
      // a couple of extra ops
      x += Math.pow(x, 1.0000001) - Math.sqrt(x + 1e-12);
    }
    // return so engine can't drop the loop as dead code
    return x;
  }
// No-operation worker for benchmarking scheduler overhead
// Minimizes actual work to isolate scheduler performance
export default class NoopWorker extends BaseWorker {
    constructor(config: WorkerConfig = {}) {
        super(config);
    }

    async setup() {
        // Minimal setup to simulate real worker initialization
        const delay = this.config.setupDelay || 0;
        if (delay > 0) {
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    async teardown() {
        // Minimal teardown
        const delay = this.config.teardownDelay || 0;
        if (delay > 0) {
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    async run(payload: any) {
        // Meaningful CPU work for threading scalability measurement
        // Increased to 100K iterations for stable multi-second benchmarks
        const result = heavyWork(100000);

        return {
            taskId: payload.taskId,
            benchmarkData: payload.benchmarkData,
            result: result,
            processed: true,
            timestamp: Date.now()
        };
    }
}