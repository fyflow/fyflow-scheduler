import { WorkerInterface, WorkerConfig, WorkerContext } from "../../core/workerInterface.ts";

/**
 * Progress Worker - Demonstrates real-time progress reporting
 *
 * Demonstrates:
 * - Progress reporting during long-running tasks
 * - Different types of progress messages
 * - Progress with custom details/metadata
 * - Simulated data processing workflow
 */
export default class ProgressWorker implements WorkerInterface {
    private config: WorkerConfig;

    constructor(config: WorkerConfig = {}) {
        this.config = config;
    }

    async setup() {
        console.log('ProgressWorker initialized for progress reporting demonstration');
    }

    async run(payload: any, context?: WorkerContext): Promise<any> {
        const { operation = 'process', size = 1000, includeDetails = false } = payload;

        if (!context) {
            // Fallback for workers without progress support
            await this.simulateWork(size);
            return { operation, size, completed: true };
        }

        console.log(`Starting ${operation} with ${size} items...`);

        // Report initial progress
        context.sendProgress(0, `Starting ${operation}...`);

        const results = [];
        const batchSize = Math.max(1, Math.floor(size / 10)); // 10% increments

        for (let i = 0; i < size; i++) {
            // Simulate processing work
            await this.processItem(i);

            // Add result
            results.push({
                id: i,
                value: Math.random() * 100,
                processed: true,
                timestamp: Date.now()
            });

            // Report progress every batch
            if (i % batchSize === 0 || i === size - 1) {
                const progress = (i + 1) / size;
                const remaining = size - i - 1;
                const eta = remaining * 10; // Rough ETA in ms

                const message = `Processed ${i + 1}/${size} items (${(progress * 100).toFixed(1)}%)`;

                const details = includeDetails ? {
                    itemsProcessed: i + 1,
                    itemsRemaining: remaining,
                    estimatedTimeRemainingMs: eta,
                    currentBatch: Math.floor(i / batchSize) + 1,
                    totalBatches: Math.ceil(size / batchSize),
                    averageProcessingTime: 10 // ms per item
                } : undefined;

                context.sendProgress(progress, message, details);
            }
        }

        // Final progress report
        context.sendProgress(1.0, `Completed ${operation} of ${size} items`);

        return {
            operation,
            size,
            completed: true,
            results: results.slice(0, 5), // Return first 5 for brevity
            totalResults: results.length,
            processingTime: size * 10, // Simulated processing time
            timestamp: new Date().toISOString()
        };
    }

    private async processItem(_index: number): Promise<void> {
        // Simulate variable processing time
        const baseTime = 5;
        const variation = Math.random() * 10;
        await new Promise(resolve => setTimeout(resolve, baseTime + variation));
    }

    private async simulateWork(size: number): Promise<void> {
        // Fallback without progress reporting
        await new Promise(resolve => setTimeout(resolve, size * 10));
    }

    async teardown() {
        console.log('ProgressWorker cleanup completed');
    }
}