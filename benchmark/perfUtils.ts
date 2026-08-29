// Performance measurement utilities for benchmarking

// Node.js globals declared for cross-platform compatibility
declare const process: any;
declare const require: any;

export interface MemoryUsage {
    heapUsed: number;
    heapTotal: number;
    external: number;
    rss?: number;
}

// Cross-platform runtime utilities
export class RuntimeUtils {
    static get args(): string[] {
        if (typeof Deno !== 'undefined') {
            return Deno.args;
        } else if (typeof process !== 'undefined') {
            // @ts-ignore node specific
            return process.argv.slice(2);
        }
        return [];
    }

    static exit(code: number = 0): never {
        if (typeof Deno !== 'undefined') {
            Deno.exit(code);
        } else if (typeof process !== 'undefined') {
            // @ts-ignore node specific
            process.exit(code);
        }
        throw new Error(`Exit ${code}`);
    }

    static async writeTextFile(path: string, content: string): Promise<void> {
        if (typeof Deno !== 'undefined') {
            await Deno.writeTextFile(path, content);
        } else if (typeof require !== 'undefined') {
            // @ts-ignore node specific
            const fs = require('fs').promises;
            await fs.writeFile(path, content, 'utf8');
        } else {
            throw new Error('File writing not supported in this environment');
        }
    }

    static async readTextFile(path: string): Promise<string> {
        if (typeof Deno !== 'undefined') {
            return await Deno.readTextFile(path);
        } else if (typeof require !== 'undefined') {
            // @ts-ignore node specific
            const fs = require('fs').promises;
            return await fs.readFile(path, 'utf8');
        } else {
            throw new Error('File reading not supported in this environment');
        }
    }

    static get isMainModule(): boolean {
        if (typeof Deno !== 'undefined') {
            // @ts-ignore deno specific
            return import.meta.main === true;
        } else if (typeof process !== 'undefined') {
            // @ts-ignore node specific
            // For ES modules in Node.js, check if this is the entry point
            return process.argv[1] && process.argv[1].includes('runBenchmarks');
        }
        return false;
    }

    static resolveWorkerUrl(relativePath: string, baseUrl: string): string {
        if (typeof Deno !== 'undefined') {
            return new URL(relativePath, baseUrl).href;
        } else {
            // For Node.js with esbuild, use ?worker-direct since noopWorker doesn't have a .node.ts variant
            return relativePath + '?worker-direct';
        }
    }
}

export interface PerfMeasurement {
    name: string;
    duration: number;
    startTime: number;
    endTime: number;
    memoryBefore: MemoryUsage;
    memoryAfter: MemoryUsage;
    memoryDelta: MemoryUsage;
}

export class PerfTimer {
    private measurements = new Map<string, {
        startTime: number;
        startMemory: MemoryUsage;
    }>();

    start(name: string) {
        const startTime = performance.now();
        const startMemory = this.getMemoryUsage();

        this.measurements.set(name, {
            startTime,
            startMemory
        });
    }

    end(name: string): PerfMeasurement {
        const endTime = performance.now();
        const endMemory = this.getMemoryUsage();

        const measurement = this.measurements.get(name);
        if (!measurement) {
            throw new Error(`No measurement started for: ${name}`);
        }

        this.measurements.delete(name);

        const duration = endTime - measurement.startTime;
        const memoryDelta = this.calculateMemoryDelta(measurement.startMemory, endMemory);

        return {
            name,
            duration,
            startTime: measurement.startTime,
            endTime,
            memoryBefore: measurement.startMemory,
            memoryAfter: endMemory,
            memoryDelta
        };
    }

    private getMemoryUsage(): MemoryUsage {
        // Deno memory usage
        if (typeof Deno !== 'undefined' && Deno.memoryUsage) {
            const usage = Deno.memoryUsage();
            return {
                heapUsed: usage.heapUsed,
                heapTotal: usage.heapTotal,
                external: usage.external,
                rss: usage.rss
            };
        }

        // Node.js memory usage fallback
        try {
            // @ts-ignore node specific
            if (typeof process !== 'undefined' && process.memoryUsage) {
                // @ts-ignore node specific
                const usage = process.memoryUsage();
                return {
                    heapUsed: usage.heapUsed,
                    heapTotal: usage.heapTotal,
                    external: usage.external,
                    rss: usage.rss
                };
            }
        } catch {
            // Ignore process access errors
        }

        // Fallback for other environments
        return {
            heapUsed: 0,
            heapTotal: 0,
            external: 0
        };
    }

    private calculateMemoryDelta(before: MemoryUsage, after: MemoryUsage): MemoryUsage {
        return {
            heapUsed: after.heapUsed - before.heapUsed,
            heapTotal: after.heapTotal - before.heapTotal,
            external: after.external - before.external,
            rss: (after.rss && before.rss) ? after.rss - before.rss : undefined
        };
    }
}

export function formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';

    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(Math.abs(bytes)) / Math.log(k));

    const value = bytes / Math.pow(k, i);
    const sign = bytes < 0 ? '-' : '';

    return `${sign}${value.toFixed(2)} ${sizes[i]}`;
}

export function formatDuration(ms: number): string {
    if (ms < 1) return `${(ms * 1000).toFixed(2)}μs`;
    if (ms < 1000) return `${ms.toFixed(2)}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
    return `${(ms / 60000).toFixed(2)}m`;
}

export class StatisticsCollector {
    private values: number[] = [];

    add(value: number) {
        this.values.push(value);
    }

    getStats() {
        if (this.values.length === 0) {
            return {
                count: 0,
                min: 0,
                max: 0,
                mean: 0,
                median: 0,
                p95: 0,
                p99: 0,
                stdDev: 0
            };
        }

        const sorted = [...this.values].sort((a, b) => a - b);
        const count = sorted.length;
        const sum = sorted.reduce((a, b) => a + b, 0);
        const mean = sum / count;

        // Standard deviation
        const variance = sorted.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / count;
        const stdDev = Math.sqrt(variance);

        return {
            count,
            min: sorted[0],
            max: sorted[count - 1],
            mean,
            median: sorted[Math.floor(count / 2)],
            p95: sorted[Math.floor(count * 0.95)],
            p99: sorted[Math.floor(count * 0.99)],
            stdDev
        };
    }

    clear() {
        this.values = [];
    }
}