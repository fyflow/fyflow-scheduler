import { BaseWorker, WorkerConfig } from "../../core/workerInterface.ts";

/**
 * Simple Worker - Basic class-based worker template
 *
 * Demonstrates:
 * - Basic worker class structure
 * - Constructor configuration
 * - Simple synchronous processing
 * - Clean class-based interface
 */
export default class SimpleWorker extends BaseWorker {
    private processingDelay: number;

    constructor(config: WorkerConfig = {}) {
        super(config);
        this.processingDelay = config.delay || 100;
    }

    async setup() {
        console.log(`SimpleWorker initialized with ${this.processingDelay}ms delay`);
    }

    async run(payload: any): Promise<any> {
        const { task, data } = payload;

        // Simulate simple processing
        await new Promise(resolve => setTimeout(resolve, this.processingDelay));

        return {
            result: `Processed ${task || 'task'}: ${data}`,
            processedAt: new Date().toISOString(),
            processingTime: this.processingDelay
        };
    }

    async teardown() {
        console.log('SimpleWorker cleanup completed');
    }
}
