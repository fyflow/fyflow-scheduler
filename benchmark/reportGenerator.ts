// Benchmark report generation and analysis
/// <reference lib="deno.ns" />
import { BenchmarkResult } from "./benchmarkScenarios.ts";
import { formatBytes, formatDuration } from "./perfUtils.ts";

export interface BenchmarkSuite {
    name: string;
    description: string;
    timestamp: string;
    environment: {
        runtime: string;
        version?: string;
        platform: string;
        cpuCount: number;
    };
    results: BenchmarkResult[];
    summary: {
        totalTests: number;
        totalDuration: number;
        averageThroughput: number;
        peakMemoryUsage: number;
        totalMemoryDelta: number;
    };
}

export class ReportGenerator {
    generateSuiteReport(
        suiteName: string,
        description: string,
        results: BenchmarkResult[]
    ): BenchmarkSuite {
        const totalDuration = results.reduce((sum, r) => sum + r.metrics.totalDuration, 0);
        const totalTasks = results.reduce((sum, r) => sum + r.config.taskCount, 0);
        const averageThroughput = totalTasks / (totalDuration / 1000);
        const peakMemoryUsage = Math.max(...results.map(r => r.metrics.memoryUsage.peak));
        const totalMemoryDelta = results.reduce((sum, r) => sum + Math.max(0, r.metrics.memoryUsage.delta), 0);

        return {
            name: suiteName,
            description,
            timestamp: new Date().toISOString(),
            environment: this.getEnvironmentInfo(),
            results,
            summary: {
                totalTests: results.length,
                totalDuration,
                averageThroughput,
                peakMemoryUsage,
                totalMemoryDelta
            }
        };
    }

    generateMarkdownReport(suite: BenchmarkSuite): string {
        const md: string[] = [];

        // Header
        md.push(`# Benchmark Report: ${suite.name}`);
        md.push(`\n**Description**: ${suite.description}`);
        md.push(`**Timestamp**: ${suite.timestamp}`);
        md.push(`**Environment**: ${suite.environment.runtime} on ${suite.environment.platform}\n`);

        // Summary
        md.push("## Summary");
        md.push(`- **Total Tests**: ${suite.summary.totalTests}`);
        md.push(`- **Total Duration**: ${formatDuration(suite.summary.totalDuration)}`);
        md.push(`- **Average Throughput**: ${suite.summary.averageThroughput.toFixed(2)} tasks/sec`);
        md.push(`- **Peak Memory Usage**: ${formatBytes(suite.summary.peakMemoryUsage)}`);
        md.push(`- **Total Memory Delta**: ${formatBytes(suite.summary.totalMemoryDelta)}\n`);

        // Detailed Results
        md.push("## Detailed Results\n");

        for (const result of suite.results) {
            md.push(`### ${result.config.name}`);
            md.push(`\n**Description**: ${result.config.description}\n`);

            // Configuration
            md.push("**Configuration**:");
            md.push(`- Tasks: ${result.config.taskCount}`);
            md.push(`- CPU Slots: ${result.config.cpuSlots}`);
            md.push(`- Worker Type: ${result.config.workerConfig.inline ? 'Inline' : 'Thread'}`);
            md.push(`- Max Threads: ${result.config.workerConfig.maxThreads}`);
            md.push(`- Max Concurrent Tasks: ${result.config.workerConfig.maxConcurrentTasks}`);

            if (result.config.groupConfig) {
                md.push(`- Groups: ${result.config.groupConfig.groupCount} groups, ${result.config.groupConfig.groupLimit} limit each`);
            }

            if (result.config.dependencyConfig && result.config.dependencyConfig.type !== 'none') {
                md.push(`- Dependencies: ${result.config.dependencyConfig.type}`);
            }
            md.push("");

            // Metrics
            md.push("**Performance Metrics**:");
            md.push(`- **Total Duration**: ${formatDuration(result.metrics.totalDuration)}`);
            md.push(`- **Scheduler Overhead**: ${formatDuration(result.metrics.schedulerOverhead)}`);
            md.push(`- **Async Execution Overhead**: ${formatDuration(result.metrics.asyncExecutionOverhead)}`);
            md.push(`- **Coordination Overhead**: ${formatDuration(result.metrics.coordinationOverhead)}`);
            md.push(`- **Total Worker Time**: ${formatDuration(result.metrics.totalWorkerTime)}`);
            md.push(`- **Overall Efficiency**: ${result.metrics.overallEfficiency.toFixed(1)}%`);
            md.push(`- **Task Throughput**: ${result.metrics.taskThroughput.toFixed(2)} tasks/sec`);
            md.push(`- **Memory Delta**: ${formatBytes(result.metrics.memoryUsage.delta)}`);
            md.push(`- **Peak Memory**: ${formatBytes(result.metrics.memoryUsage.peak)}`);
            md.push(`- **Avg Dispatch Time**: ${formatDuration(result.metrics.dispatchMetrics.averageDispatchTime)}`);
            md.push(`- **Max Dispatch Time**: ${formatDuration(result.metrics.dispatchMetrics.maxDispatchTime)}`);
            md.push(`- **Dispatch Iterations**: ${result.metrics.dispatchMetrics.dispatchIterations}\n`);
        }

        // Performance Analysis
        md.push("## Performance Analysis\n");

        const contentionResults = suite.results.filter(r => r.config.groupConfig);
        if (contentionResults.length > 0) {
            md.push("### Group Contention Analysis");
            md.push("| Test | Tasks | Group Slots | Throughput | Avg Dispatch Time | Max Dispatch Time |");
            md.push("|------|-------|-------------|------------|-------------------|-------------------|");

            for (const result of contentionResults) {
                const slots = result.config.groupConfig?.groupLimit || 0;
                md.push(`| ${result.config.name} | ${result.config.taskCount} | ${slots} | ${result.metrics.taskThroughput.toFixed(2)} | ${formatDuration(result.metrics.dispatchMetrics.averageDispatchTime)} | ${formatDuration(result.metrics.dispatchMetrics.maxDispatchTime)} |`);
            }
            md.push("");
        }

        const volumeResults = suite.results.filter(r => !r.config.groupConfig && (!r.config.dependencyConfig || r.config.dependencyConfig.type === 'none'));
        if (volumeResults.length > 0) {
            md.push("### Task Volume Scaling");
            md.push("| Test | Tasks | Throughput | Memory Delta | Scheduler Overhead |");
            md.push("|------|-------|------------|--------------|-------------------|");

            for (const result of volumeResults) {
                md.push(`| ${result.config.name} | ${result.config.taskCount} | ${result.metrics.taskThroughput.toFixed(2)} | ${formatBytes(result.metrics.memoryUsage.delta)} | ${formatDuration(result.metrics.schedulerOverhead)} |`);
            }
            md.push("");
        }

        // Recommendations
        md.push("## Recommendations\n");
        md.push(this.generateRecommendations(suite));

        return md.join('\n');
    }

    generateJsonReport(suite: BenchmarkSuite): string {
        return JSON.stringify(suite, null, 2);
    }

    generateCsvReport(suite: BenchmarkSuite): string {
        const headers = [
            'Test Name',
            'Task Count',
            'CPU Slots',
            'Worker Type',
            'Max Threads',
            'Max Concurrent Tasks',
            'Total Duration (ms)',
            'Scheduler Overhead (ms)',
            'Async Execution Overhead (ms)',
            'Coordination Overhead (ms)',
            'Total Worker Time (ms)',
            'Overall Efficiency (%)',
            'Task Throughput (tasks/sec)',
            'Memory Delta (bytes)',
            'Peak Memory (bytes)',
            'Avg Dispatch Time (ms)',
            'Max Dispatch Time (ms)',
            'Dispatch Iterations',
            'Groups',
            'Group Limit'
        ];

        const rows = suite.results.map(result => [
            `"${result.config.name}"`,
            result.config.taskCount,
            result.config.cpuSlots,
            result.config.workerConfig.inline ? 'Inline' : 'Thread',
            result.config.workerConfig.maxThreads,
            result.config.workerConfig.maxConcurrentTasks,
            result.metrics.totalDuration.toFixed(2),
            result.metrics.schedulerOverhead.toFixed(2),
            result.metrics.asyncExecutionOverhead.toFixed(2),
            result.metrics.coordinationOverhead.toFixed(2),
            result.metrics.totalWorkerTime.toFixed(2),
            result.metrics.overallEfficiency.toFixed(1),
            result.metrics.taskThroughput.toFixed(2),
            result.metrics.memoryUsage.delta,
            result.metrics.memoryUsage.peak,
            result.metrics.dispatchMetrics.averageDispatchTime.toFixed(4),
            result.metrics.dispatchMetrics.maxDispatchTime.toFixed(4),
            result.metrics.dispatchMetrics.dispatchIterations,
            result.config.groupConfig?.groupCount || 0,
            result.config.groupConfig?.groupLimit || 0
        ]);

        return [headers.join(','), ...rows.map(row => row.join(','))].join('\n');
    }

    private getEnvironmentInfo() {
        let runtime = 'Unknown';
        let version = undefined;
        let platform = 'Unknown';
        let cpuCount = 1;

        if (typeof Deno !== 'undefined') {
            runtime = 'Deno';
            version = Deno.version.deno;
            platform = Deno.build.os;
            cpuCount = navigator.hardwareConcurrency || 1;
        } else {
            try {
                // @ts-ignore node specific
                // deno-lint-ignore no-process-global
                if (typeof process !== 'undefined') {
                    runtime = 'Node.js';
                    // @ts-ignore node specific
                    // deno-lint-ignore no-process-global
                    version = process.version;
                    // @ts-ignore node specific
                    // deno-lint-ignore no-process-global
                    platform = process.platform;
                    // @ts-ignore node specific
                    cpuCount = (globalThis as Record<string, unknown>).require?.('os')?.cpus?.()?.length || 1;
                }
            } catch {
                // Ignore process access errors
            }
        }

        return { runtime, version, platform, cpuCount };
    }

    private generateRecommendations(suite: BenchmarkSuite): string {
        const recommendations: string[] = [];

        // Analyze contention scenarios
        const contentionResults = suite.results.filter(r => r.config.groupConfig);
        const highContentionResults = contentionResults.filter(r =>
            r.metrics.dispatchMetrics.maxDispatchTime > 100 // > 100ms max dispatch time
        );

        if (highContentionResults.length > 0) {
            recommendations.push("### Group Contention Issues Detected");
            recommendations.push("High dispatch times detected in group contention scenarios:");
            for (const result of highContentionResults) {
                recommendations.push(`- **${result.config.name}**: Max dispatch time ${formatDuration(result.metrics.dispatchMetrics.maxDispatchTime)}`);
            }
            recommendations.push("\n**Recommendations**:");
            recommendations.push("- Implement event-driven dispatch instead of polling");
            recommendations.push("- Add group-aware queuing to avoid checking impossible conditions");
            recommendations.push("- Consider backoff strategies for repeatedly blocked tasks");
            recommendations.push("");
        }

        // Analyze memory usage
        const highMemoryResults = suite.results.filter(r =>
            r.metrics.memoryUsage.delta > 100 * 1024 * 1024 // > 100MB delta
        );

        if (highMemoryResults.length > 0) {
            recommendations.push("### High Memory Usage Detected");
            for (const result of highMemoryResults) {
                recommendations.push(`- **${result.config.name}**: ${formatBytes(result.metrics.memoryUsage.delta)} memory growth`);
            }
            recommendations.push("\n**Recommendations**:");
            recommendations.push("- Review object allocation patterns in scheduler");
            recommendations.push("- Implement object pooling for frequently created objects");
            recommendations.push("- Consider task batching to reduce memory overhead");
            recommendations.push("");
        }

        // Analyze throughput scaling
        const volumeResults = suite.results
            .filter(r => !r.config.groupConfig && (!r.config.dependencyConfig || r.config.dependencyConfig.type === 'none'))
            .sort((a, b) => a.config.taskCount - b.config.taskCount);

        if (volumeResults.length >= 2) {
            const scalingEfficiency = this.calculateScalingEfficiency(volumeResults);
            if (scalingEfficiency < 0.8) { // Less than 80% scaling efficiency
                recommendations.push("### Poor Task Volume Scaling");
                recommendations.push(`Throughput scaling efficiency: ${(scalingEfficiency * 100).toFixed(1)}%`);
                recommendations.push("\n**Recommendations**:");
                recommendations.push("- Optimize dispatch loop for better O(n) behavior");
                recommendations.push("- Review task queue management algorithms");
                recommendations.push("- Consider task batching for high-volume scenarios");
                recommendations.push("");
            }
        }

        return recommendations.length > 0 ? recommendations.join('\n') : "No specific performance issues detected. System is performing within expected parameters.";
    }

    private calculateScalingEfficiency(volumeResults: BenchmarkResult[]): number {
        if (volumeResults.length < 2) return 1;

        const first = volumeResults[0];
        const last = volumeResults[volumeResults.length - 1];

        const expectedThroughput = first.metrics.taskThroughput;
        const actualThroughput = last.metrics.taskThroughput;

        return Math.min(1, actualThroughput / expectedThroughput);
    }
}