import { BaseWorker, WorkerConfig } from "../index.ts";

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
        // Minimal work - just return immediately or with tiny delay
        const delay = payload.delay || this.config.taskDelay || 0;
        if (delay > 0) {
            await new Promise(resolve => setTimeout(resolve, delay));
        }
        // await new Promise(resolve => setTimeout(resolve, 100));
        // for(let i = 0; i < 10000; i++) {
        //     Math.random();
        // }

        return {
            taskId: payload.taskId,
            benchmarkData: payload.benchmarkData,
            processed: true,
            timestamp: Date.now()
        };
    }
}