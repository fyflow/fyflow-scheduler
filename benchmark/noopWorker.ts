import { BaseWorker } from "../index.ts";
import type { WorkerConfig } from "../index.ts";

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
        // Yield to the event loop once. A task that only awaits microtasks
        // keeps every timer - including a benchmark's own sampler - from firing
        // until the whole run is over (benchmark/admission.ts). A message, not
        // setTimeout(0): that costs a whole timer tick, ~15 ms on Windows, and
        // the run would measure the clock rather than the scheduler.
        if (payload.yieldMacrotask) {
            await new Promise<void>(resolve => {
                const channel = new MessageChannel();
                channel.port1.onmessage = () => { channel.port1.close(); resolve(); };
                channel.port2.postMessage(0);
            });
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