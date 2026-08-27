// Main benchmark runner script
import { BenchmarkRunner } from "./benchmarkScenarios.ts";
import { ReportGenerator } from "./reportGenerator.ts";
import { RuntimeUtils } from "./perfUtils.ts";
import {
    getAllScenarios,
    getScenariosByCategory,
    getScenarioByName,
    SCENARIO_CATEGORIES
} from "./predefinedScenarios.ts";

function calculateRunStatistics(allResults: any[], scenarios: any[]): any[] {
    const aggregatedResults = [];

    for (const scenario of scenarios) {
        const scenarioResults = allResults.filter(r => r.config.name === scenario.name);
        if (scenarioResults.length === 0) continue;

        // Calculate statistics for key metrics
        const throughputs = scenarioResults.map(r => r.metrics.taskThroughput);
        const schedulerOverheads = scenarioResults.map(r => r.metrics.schedulerOverhead);
        const asyncOverheads = scenarioResults.map(r => r.metrics.asyncExecutionOverhead);
        const totalDurations = scenarioResults.map(r => r.metrics.totalDuration);

        const calculateStats = (values: number[]) => {
            const sorted = [...values].sort((a, b) => a - b);
            const mean = values.reduce((a, b) => a + b, 0) / values.length;
            const median = sorted[Math.floor(sorted.length / 2)];
            const min = sorted[0];
            const max = sorted[sorted.length - 1];
            const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / values.length;
            const stdDev = Math.sqrt(variance);
            const cv = (stdDev / mean) * 100; // Coefficient of variation as percentage

            return { mean, median, min, max, stdDev, cv };
        };

        const throughputStats = calculateStats(throughputs);
        const firstResult = scenarioResults[0];

        // Create aggregated result using median values with variance info
        const aggregatedResult = {
            ...firstResult,
            metrics: {
                ...firstResult.metrics,
                taskThroughput: throughputStats.median,
                schedulerOverhead: calculateStats(schedulerOverheads).median,
                asyncExecutionOverhead: calculateStats(asyncOverheads).median,
                totalDuration: calculateStats(totalDurations).median,
            },
            variance: {
                runs: scenarioResults.length,
                throughput: throughputStats,
                schedulerOverhead: calculateStats(schedulerOverheads),
                asyncExecutionOverhead: calculateStats(asyncOverheads),
                totalDuration: calculateStats(totalDurations),
            }
        };

        aggregatedResults.push(aggregatedResult);
    }

    // Print variance summary if multiple runs
    if (aggregatedResults.length > 0 && aggregatedResults[0].variance) {
        console.log("\n📊 Variance Analysis Summary:");
        for (const result of aggregatedResults) {
            const cv = result.variance.throughput.cv;
            const status = cv < 5 ? "✅ Low" : cv < 10 ? "⚠️ Medium" : "❌ High";
            console.log(`${result.config.name}: ${status} variance (${cv.toFixed(1)}% CV)`);
        }
        console.log("");
    }

    return aggregatedResults;
}

interface RunOptions {
    scenarios?: string[];
    categories?: string[];
    output?: string;
    format?: 'console' | 'markdown' | 'json' | 'csv';
    quick?: boolean;
    runs?: number;
}

async function main() {
    const args = RuntimeUtils.args;
    const options = parseArgs(args);

    console.log("🚀 FyFlow DAG Scheduler Benchmark Suite");
    console.log("=====================================\n");

    const runner = new BenchmarkRunner();
    const reportGenerator = new ReportGenerator();

    // Determine which scenarios to run
    let scenariosToRun;

    if (options.quick) {
        console.log("🏃 Running quick benchmark suite...\n");
        scenariosToRun = getScenariosByCategory('QUICK');
    } else if (options.scenarios) {
        scenariosToRun = options.scenarios
            .map(name => getScenarioByName(name))
            .filter(s => s !== undefined);

        if (scenariosToRun.length === 0) {
            console.error("❌ No valid scenarios found");
            showHelp();
            RuntimeUtils.exit(1);
        }
    } else if (options.categories) {
        scenariosToRun = [];
        for (const category of options.categories) {
            const categoryKey = category.toUpperCase() as keyof typeof SCENARIO_CATEGORIES;
            if (SCENARIO_CATEGORIES[categoryKey]) {
                scenariosToRun.push(...getScenariosByCategory(categoryKey));
            } else {
                console.error(`❌ Unknown category: ${category}`);
                showAvailableCategories();
                RuntimeUtils.exit(1);
            }
        }
    } else {
        console.log("🔥 Running full benchmark suite...\n");
        scenariosToRun = getAllScenarios();
    }

    const runs = options.runs || 1;
    console.log(`📋 Running ${scenariosToRun.length} benchmark scenarios${runs > 1 ? ` (${runs} runs each for variance analysis)` : ''}...\n`);

    // Run benchmarks (potentially multiple times)
    const allResults = [];
    const startTime = performance.now();

    for (let run = 1; run <= runs; run++) {
        if (runs > 1) {
            console.log(`\n🔄 === Benchmark Run ${run}/${runs} ===`);
        }

        for (let i = 0; i < scenariosToRun.length; i++) {
            const scenario = scenariosToRun[i];
            const scenarioLabel = runs > 1 ? `[Run ${run}] [${i + 1}/${scenariosToRun.length}]` : `[${i + 1}/${scenariosToRun.length}]`;
            console.log(`\n${scenarioLabel} Starting: ${scenario.name}`);

            try {
                const result = await runner.runBenchmark(scenario);
                allResults.push({ ...result, runNumber: run });
                console.log(`✅ Completed: ${scenario.name}`);
            // deno-lint-ignore no-explicit-any
            } catch (error: any) {
                console.error(`❌ Failed: ${scenario.name} - ${error.message}`);
            }

            // Small delay between benchmarks to allow GC
            if (i < scenariosToRun.length - 1 || run < runs) {
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    }

    const totalTime = performance.now() - startTime;

    // For multiple runs, calculate variance and summary statistics
    const results = runs > 1 ? calculateRunStatistics(allResults, scenariosToRun) : allResults;

    // Generate report
    const suite = reportGenerator.generateSuiteReport(
        "FyFlow Scheduler Performance Benchmark",
        "Comprehensive performance analysis of the FyFlow scheduler and worker management",
        results
    );

    console.log(`\n🎯 Benchmark suite completed in ${(totalTime / 1000).toFixed(2)}s`);
    const successfulRuns = runs > 1 ? results.length : results.length;
    const totalRuns = runs > 1 ? scenariosToRun.length * runs : scenariosToRun.length;
    console.log(`📊 Successfully ran ${successfulRuns}/${totalRuns} ${runs > 1 ? 'scenario runs' : 'scenarios'}${runs > 1 ? ` (${results.length} unique scenarios)` : ''}\n`);

    // Output results
    switch (options.format) {
        case 'markdown': {
            const mdReport = reportGenerator.generateMarkdownReport(suite);
            if (options.output) {
                await RuntimeUtils.writeTextFile(options.output, mdReport);
                console.log(`📄 Markdown report saved to: ${options.output}`);
            } else {
                console.log(mdReport);
            }
            break;
        }

        case 'json': {
            const jsonReport = reportGenerator.generateJsonReport(suite);
            if (options.output) {
                await RuntimeUtils.writeTextFile(options.output, jsonReport);
                console.log(`📄 JSON report saved to: ${options.output}`);
            } else {
                console.log(jsonReport);
            }
            break;
        }

        case 'csv': {
            const csvReport = reportGenerator.generateCsvReport(suite);
            if (options.output) {
                await RuntimeUtils.writeTextFile(options.output, csvReport);
                console.log(`📄 CSV report saved to: ${options.output}`);
            } else {
                console.log(csvReport);
            }
            break;
        }

        default:
            // Console output already shown during benchmark execution
            console.log("🎉 Benchmark suite complete!");
            console.log("\n💡 Use --format markdown/json/csv for detailed reports");
            console.log("💡 Use --output <file> to save reports to file");
            break;
    }
}

function parseArgs(args: string[]): RunOptions {
    const options: RunOptions = {
        format: 'console'
    };

    for (let i = 0; i < args.length; i++) {
        const arg = args[i];

        switch (arg) {
            case '--help':
            case '-h':
                showHelp();
                RuntimeUtils.exit(0);
                break;

            case '--quick':
            case '-q':
                options.quick = true;
                break;

            case '--scenarios':
            case '-s':
                if (i + 1 < args.length) {
                    const scenariosArg = args[++i];
                    // Use pipe (|) as delimiter instead of comma since scenario names contain commas
                    options.scenarios = scenariosArg.split('|').map(s => s.trim());
                }
                break;

            case '--categories':
            case '-c':
                if (i + 1 < args.length) {
                    options.categories = args[++i].split(',');
                }
                break;

            case '--output':
            case '-o':
                if (i + 1 < args.length) {
                    options.output = args[++i];
                }
                break;

            case '--format':
            case '-f':
                if (i + 1 < args.length) {
                    const format = args[++i];
                    if (['console', 'markdown', 'json', 'csv'].includes(format)) {
                        options.format = format as 'console' | 'markdown' | 'json' | 'csv';
                    } else {
                        console.error(`❌ Invalid format: ${format}`);
                        RuntimeUtils.exit(1);
                    }
                }
                break;

            case '--list-scenarios':
                listScenarios();
                RuntimeUtils.exit(0);
                break;

            case '--list-categories':
                showAvailableCategories();
                RuntimeUtils.exit(0);
                break;

            case '--runs':
            case '-r':
                if (i + 1 < args.length) {
                    const runs = parseInt(args[++i]);
                    if (runs > 0 && runs <= 10) {
                        options.runs = runs;
                    } else {
                        console.error(`❌ Invalid number of runs: ${runs} (must be 1-10)`);
                        RuntimeUtils.exit(1);
                    }
                }
                break;
        }
    }

    return options;
}

function showHelp() {
    console.log(`
FyFlow DAG Scheduler Benchmark Suite

USAGE:
    deno run --allow-read --allow-net runBenchmarks.ts [OPTIONS]

OPTIONS:
    -h, --help              Show this help message
    -q, --quick             Run only quick benchmark scenarios
    -s, --scenarios <list>  Run specific scenarios (pipe-separated: scenario1|scenario2)
    -c, --categories <list> Run specific categories (comma-separated)
    -f, --format <format>   Output format: console, markdown, json, csv (default: console)
    -o, --output <file>     Save report to file
    -r, --runs <number>     Number of benchmark runs for variance analysis (1-10, default: 1)
    --list-scenarios        List all available scenarios
    --list-categories       List all available categories

EXAMPLES:
    # Run quick benchmarks
    deno run --allow-read --allow-net runBenchmarks.ts --quick

    # Run quick benchmarks 3 times for variance analysis
    deno run --allow-read --allow-net runBenchmarks.ts --quick --runs 3

    # Run specific scenarios
    deno run --allow-read --allow-net runBenchmarks.ts --scenarios "Large Volume - 1K Independent Tasks|High Contention - 1K Tasks, 4 Slots"

    # Run contention category and save markdown report
    deno run --allow-read --allow-net runBenchmarks.ts --categories contention --format markdown --output report.md

    # Run all benchmarks and save JSON report
    deno run --allow-read --allow-net runBenchmarks.ts --format json --output benchmark-results.json
`);
}

function showAvailableCategories() {
    console.log("Available benchmark categories:");
    for (const [name, scenarios] of Object.entries(SCENARIO_CATEGORIES)) {
        console.log(`  ${name.toLowerCase()}: ${scenarios.length} scenarios`);
    }
}

function listScenarios() {
    console.log("Available benchmark scenarios:");
    const allScenarios = getAllScenarios();
    for (const scenario of allScenarios) {
        console.log(`  "${scenario.name}"`);
        console.log(`    ${scenario.description}`);
    }
}

// Run when this file is executed directly
await main();