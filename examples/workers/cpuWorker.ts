import { BaseWorker, WorkerConfig } from "../../core/workerInterface.ts";

/**
 * CPU Worker - Thread-based CPU-intensive worker
 *
 * Demonstrates:
 * - CPU-intensive processing patterns
 * - Thread-based execution for true parallelism
 * - Single task per thread (maxConcurrentTasks = 1)
 * - Blocking computation simulation
 */
export default class CpuWorker extends BaseWorker {
    private iterations: number;

    constructor(config: WorkerConfig = {}) {
        super(config);
        this.iterations = config.iterations || 1000000;
    }

    async setup() {
        console.log(`CpuWorker initialized for CPU-intensive tasks (${this.iterations} iterations)`);
    }

    async run(payload: any): Promise<any> {
        // console.log('run', payload)
        const { algorithm = 'prime', data } = payload;
        const startTime = performance.now();

        let result: any;

        switch (algorithm) {
            case 'prime':
                result = this.computePrimes(this.iterations);
                break;
            case 'fibonacci':
                result = this.computeFibonacci(Math.min(this.iterations / 1000, 45));
                break;
            case 'sort':
                result = this.computeSort(this.iterations / 1000);
                break;
            default:
                result = this.computePrimes(this.iterations);
        }

        const duration = performance.now() - startTime;

        return {
            algorithm,
            data,
            result: result.value,
            count: result.count,
            cpuTime: Math.round(duration),
            processedAt: new Date().toISOString()
        };
    }

    private computePrimes(limit: number) {
        const primes = [];
        for (let n = 2; n < limit && primes.length < 1000; n++) {
            let isPrime = true;
            for (let i = 2; i * i <= n; i++) {
                if (n % i === 0) {
                    isPrime = false;
                    break;
                }
            }
            if (isPrime) primes.push(n);
        }
        return { value: primes.slice(-10), count: primes.length };
    }

    private computeFibonacci(n: number) {
        if (n <= 1) return { value: n, count: 1 };
        let a = 0, b = 1;
        for (let i = 2; i <= n; i++) {
            [a, b] = [b, a + b];
        }
        return { value: b, count: n };
    }

    private computeSort(size: number) {
        const arr = Array.from({ length: size }, () => Math.floor(Math.random() * 10000));
        const sorted = [...arr].sort((a, b) => a - b);
        return { value: sorted.slice(0, 10), count: size };
    }

    async teardown() {
        console.log('CpuWorker cleanup completed');
    }
}
