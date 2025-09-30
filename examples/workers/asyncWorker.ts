import { WorkerInterface, WorkerConfig } from "../../core/workerInterface.ts";

/**
 * Async Worker - High-concurrency inline worker
 *
 * Demonstrates:
 * - High concurrent task handling (optimized for async work)
 * - Non-blocking I/O simulation
 * - Efficient resource usage for async workloads
 * - Worker interface implementation (no BaseWorker inheritance)
 */
export default class AsyncWorker implements WorkerInterface {
    private config: WorkerConfig;
    private connectionPool: any[];

    constructor(config: WorkerConfig = {}) {
        this.config = config;
        this.connectionPool = [];
    }

    async setup() {
        // Simulate async resource initialization (connections, etc.)
        const poolSize = this.config.connectionPoolSize || 5;
        for (let i = 0; i < poolSize; i++) {
            this.connectionPool.push({
                id: i,
                created: Date.now(),
                inUse: false
            });
        }
        // console.log(`AsyncWorker initialized with ${poolSize} connection pool`);
    }

    async run(payload: any): Promise<any> {
        const { data, delay = 500 } = payload;

        // Get available connection from pool
        const connection = this.connectionPool.find(c => !c.inUse);
        if (connection) {
            connection.inUse = true;
        }

        try {
            // Simulate async I/O operation (API call, database query, etc.)
            await new Promise(resolve => setTimeout(resolve, delay));

            return {
                result: `Async processed: ${data}`,
                connectionId: connection?.id || 'no-pool',
                processedAt: new Date().toISOString(),
                latency: delay
            };
        } finally {
            if (connection) {
                connection.inUse = false;
            }
        }
    }

    async teardown() {
        // Cleanup connections
        this.connectionPool.length = 0;
        console.log('AsyncWorker connection pool cleaned up');
    }
}
