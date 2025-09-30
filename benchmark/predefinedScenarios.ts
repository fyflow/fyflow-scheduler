// Predefined benchmark scenarios for comprehensive testing
import { BenchmarkConfig } from "./benchmarkScenarios.ts";

export const BENCHMARK_SCENARIOS: BenchmarkConfig[] = [
    // 1. Large Task Volume Scenarios - Inline Workers
    {
        name: "Large Volume - 1K Independent Tasks (Inline)",
        description: "1,000 independent tasks with minimal worker delay - inline workers",
        taskCount: 1000,
        cpuSlots: 8,
        workerConfig: {
            maxThreads: 4,
            maxConcurrentTasks: 5,
            inline: true,
            taskDelay: 1
        }
    },

    {
        name: "Large Volume - 10K Independent Tasks (Inline)",
        description: "10,000 independent tasks testing scheduler scalability - inline workers",
        taskCount: 10000,
        cpuSlots: 8,
        workerConfig: {
            maxThreads: 4,
            maxConcurrentTasks: 10,
            inline: true,
            taskDelay: 0
        }
    },

    {
        name: "Large Volume - 50K Independent Tasks (Inline)",
        description: "50,000 independent tasks - stress test for memory and throughput - inline workers",
        taskCount: 50000,
        cpuSlots: 16,
        workerConfig: {
            maxThreads: 8,
            maxConcurrentTasks: 20,
            inline: true,
            taskDelay: 0
        }
    },

    // 1b. Threading Scalability Scenarios - CPU Workers with controlled workload
    {
        name: "Threading Scalability - 200 Tasks, 2 Threads",
        description: "200 CPU tasks on 2 threads - measure threading scalability",
        taskCount: 200,
        cpuSlots: 2,
        workerConfig: {
            maxThreads: 2,
            maxConcurrentTasks: 1,
            inline: false,
            cpuWorker: true,
            taskDelay: 0
        }
    },

    {
        name: "Threading Scalability - 200 Tasks, 4 Threads",
        description: "200 CPU tasks on 4 threads - measure threading scalability",
        taskCount: 200,
        cpuSlots: 4,
        workerConfig: {
            maxThreads: 4,
            maxConcurrentTasks: 1,
            inline: false,
            cpuWorker: true,
            taskDelay: 0
        }
    },

    {
        name: "Threading Efficiency - 1K CPU Tasks",
        description: "1,000 CPU tasks testing thread coordination efficiency",
        taskCount: 1000,
        cpuSlots: 8,
        workerConfig: {
            maxThreads: 8,
            maxConcurrentTasks: 1,
            inline: false,
            cpuWorker: true,
            taskDelay: 0
        }
    },

    // 2. High Group Contention Scenarios
    {
        name: "High Contention - 1K Tasks, 4 Slots",
        description: "1,000 tasks competing for 4 group slots (simulating GPU contention)",
        taskCount: 1000,
        cpuSlots: 8,
        workerConfig: {
            maxThreads: 2,
            maxConcurrentTasks: 1,
            inline: false,
            taskDelay: 10
        },
        groupConfig: {
            groupCount: 1,
            groupLimit: 4,
            tasksPerGroup: 1000
        }
    },

    {
        name: "High Contention - 10K Tasks, 4 Slots",
        description: "10,000 tasks competing for 4 group slots - O(n²) behavior test",
        taskCount: 10000,
        cpuSlots: 8,
        workerConfig: {
            maxThreads: 2,
            maxConcurrentTasks: 1,
            inline: false,
            taskDelay: 5
        },
        groupConfig: {
            groupCount: 1,
            groupLimit: 4,
            tasksPerGroup: 10000
        }
    },

    {
        name: "Extreme Contention - 25K Tasks, 2 Slots",
        description: "25,000 tasks competing for 2 group slots - extreme bottleneck scenario",
        taskCount: 25000,
        cpuSlots: 16,
        workerConfig: {
            maxThreads: 4,
            maxConcurrentTasks: 1,
            inline: false,
            taskDelay: 1
        },
        groupConfig: {
            groupCount: 1,
            groupLimit: 2,
            tasksPerGroup: 25000
        }
    },

    // 3. Large Group Count Scenarios
    {
        name: "Many Groups - 100 Groups",
        description: "1,000 tasks distributed across 100 different groups",
        taskCount: 1000,
        cpuSlots: 8,
        workerConfig: {
            maxThreads: 4,
            maxConcurrentTasks: 5,
            inline: true,
            taskDelay: 1
        },
        groupConfig: {
            groupCount: 100,
            groupLimit: 2
        }
    },

    {
        name: "Many Groups - 500 Groups",
        description: "5,000 tasks distributed across 500 different groups",
        taskCount: 5000,
        cpuSlots: 16,
        workerConfig: {
            maxThreads: 8,
            maxConcurrentTasks: 10,
            inline: true,
            taskDelay: 0
        },
        groupConfig: {
            groupCount: 500,
            groupLimit: 3
        }
    },

    {
        name: "Many Groups - 1K Groups",
        description: "10,000 tasks distributed across 1,000 different groups",
        taskCount: 10000,
        cpuSlots: 16,
        workerConfig: {
            maxThreads: 8,
            maxConcurrentTasks: 10,
            inline: true,
            taskDelay: 0
        },
        groupConfig: {
            groupCount: 1000,
            groupLimit: 5
        }
    },

    // 4. Complex DAG Dependencies
    {
        name: "Deep Chain - 1K Tasks",
        description: "1,000 tasks in sequential dependency chain",
        taskCount: 1000,
        cpuSlots: 8,
        workerConfig: {
            maxThreads: 4,
            maxConcurrentTasks: 1,
            inline: true,
            taskDelay: 1
        },
        dependencyConfig: {
            type: 'chain',
            depth: 1000
        }
    },

    {
        name: "Wide Tree - 1K Tasks",
        description: "1,000 tasks in tree structure with fanout of 10",
        taskCount: 1000,
        cpuSlots: 8,
        workerConfig: {
            maxThreads: 4,
            maxConcurrentTasks: 5,
            inline: true,
            taskDelay: 1
        },
        dependencyConfig: {
            type: 'tree',
            fanout: 10
        }
    },

    {
        name: "Mixed Dependencies - 5K Tasks",
        description: "5,000 tasks with mixed dependency patterns (30% chain, 30% tree, 40% independent)",
        taskCount: 5000,
        cpuSlots: 16,
        workerConfig: {
            maxThreads: 8,
            maxConcurrentTasks: 10,
            inline: true,
            taskDelay: 0
        },
        dependencyConfig: {
            type: 'mixed'
        }
    },

    // 5. Mixed Workload Patterns
    {
        name: "Mixed Workload - CPU vs Async",
        description: "Mixed CPU-bound threads and async inline workers",
        taskCount: 2000,
        cpuSlots: 8,
        workerConfig: {
            maxThreads: 4,
            maxConcurrentTasks: 8,
            inline: true, // Will test both in the actual benchmark
            taskDelay: 2
        }
    },

    // 6. Overlapping Group Contention
    {
        name: "Overlapping Groups - 1K Tasks, 100 Groups",
        description: "1,000 tasks each belonging to 100 overlapping groups with tight limits",
        taskCount: 1000,
        cpuSlots: 8,
        workerConfig: {
            maxThreads: 2,
            maxConcurrentTasks: 1,
            inline: false,
            taskDelay: 5
        },
        groupConfig: {
            groupCount: 100,
            groupLimit: 2,
            tasksPerGroup: 1000 // All tasks belong to all groups
        }
    },

    {
        name: "Overlapping Groups - 5K Tasks, 50 Groups",
        description: "5,000 tasks each belonging to 50 overlapping groups - medium overlap scenario",
        taskCount: 5000,
        cpuSlots: 8,
        workerConfig: {
            maxThreads: 2,
            maxConcurrentTasks: 1,
            inline: false,
            taskDelay: 3
        },
        groupConfig: {
            groupCount: 50,
            groupLimit: 3,
            tasksPerGroup: 5000 // All tasks belong to all groups
        }
    },

    {
        name: "Overlapping Groups - 10K Tasks, 20 Groups",
        description: "10,000 tasks each belonging to 20 overlapping groups - extreme contention",
        taskCount: 10000,
        cpuSlots: 16,
        workerConfig: {
            maxThreads: 4,
            maxConcurrentTasks: 1,
            inline: false,
            taskDelay: 1
        },
        groupConfig: {
            groupCount: 20,
            groupLimit: 4,
            tasksPerGroup: 10000 // All tasks belong to all groups
        }
    },

    // 7. High Async Concurrency Scenarios
    {
        name: "High Async Concurrency - 1K Tasks, 500ms Delay",
        description: "1,000 tasks with 500ms async delay on single worker with 500+ concurrency",
        taskCount: 1000,
        cpuSlots: 8, // Should not be consumed by inline workers
        workerConfig: {
            maxThreads: 1,           // Single worker
            maxConcurrentTasks: 500, // High concurrency
            inline: true,            // Async inline workers (no CPU slots)
            taskDelay: 500           // 500ms async delay
        }
    },

    {
        name: "High Async Concurrency - 2K Tasks, 500ms Delay",
        description: "2,000 tasks with 500ms async delay on single worker with 1000+ concurrency",
        taskCount: 2000,
        cpuSlots: 8, // Should not be consumed by inline workers
        workerConfig: {
            maxThreads: 1,            // Single worker
            maxConcurrentTasks: 1000, // Even higher concurrency
            inline: true,             // Async inline workers (no CPU slots)
            taskDelay: 500            // 500ms async delay
        }
    },

    // 8. Memory Pressure Scenarios
    {
        name: "Memory Pressure - 100K Tasks",
        description: "100,000 tasks to test memory allocation and GC pressure",
        taskCount: 100000,
        cpuSlots: 16,
        workerConfig: {
            maxThreads: 8,
            maxConcurrentTasks: 5,
            inline: false,
            taskDelay: 0
        }
    },
    {
        name: "High concurrency - 25k Tasks, maxConcurrentTasks os 1",
        description: "25,000 tasks to test memory allocation and GC pressure",
        taskCount: 80000,
        cpuSlots: 16,
        workerConfig: {
            maxThreads: 8,
            maxConcurrentTasks: 1,
            inline: false,
            taskDelay: 0
        }
    },
    {
        name: "High concurrency - 70k",
        description: "70,000 tasks to test memory allocation and GC pressure",
        taskCount: 70000,
        cpuSlots: 16,
        workerConfig: {
            maxThreads: 4,
            maxConcurrentTasks: 2,
            inline: false,
            taskDelay: 0
        }
    },
    {
        name: "High concurrency - 90k",
        description: "90,000 tasks to test memory allocation and GC pressure",
        taskCount: 90000,
        cpuSlots: 16,
        workerConfig: {
            maxThreads: 4,
            maxConcurrentTasks: 2,
            inline: false,
            taskDelay: 0
        }
    },
    {
        name: "High concurrency - 90k 8x1",
        description: "90,000 tasks to test memory allocation and GC pressure",
        taskCount: 90000,
        cpuSlots: 16,
        workerConfig: {
            maxThreads: 8,
            maxConcurrentTasks: 1,
            inline: false,
            taskDelay: 0
        },
        // groupConfig: {
        //     groupCount: 1,
        //     groupLimit: 16,
        //     tasksPerGroup: 10000 // All tasks belong to all groups
        // }
    },
    {
        name: "Workload concurrency - 10 2x1 cpuWorker",
        description: "10 tasks to test memory allocation and GC pressure",
        taskCount: 100,
        cpuSlots: 2,
        workerConfig: {
            maxThreads: 2,
            maxConcurrentTasks: 1,
            inline: false,
            cpuWorker: true,
            taskDelay: 0
        },
        groupConfig: {
            groupCount: 1,
            groupLimit: 2,
            tasksPerGroup: 10000 // All tasks belong to all groups
        }
    },
    {
        name: "Workload concurrency - 10 8x1 cpuWorker",
        description: "100 tasks to test memory allocation and GC pressure",
        taskCount: 100,
        cpuSlots: 20,
        workerConfig: {
            maxThreads: 20,
            maxConcurrentTasks: 1,
            inline: false,
            cpuWorker: true,
            taskDelay: 0
        },
        groupConfig: {
            groupCount: 1,
            groupLimit: 20,
            tasksPerGroup: 10000 // All tasks belong to all groups
        }
    },
];

// Scenario categories for organized testing
export const SCENARIO_CATEGORIES = {
    QUICK: BENCHMARK_SCENARIOS.slice(0, 6), // Include both inline (0-2) and threaded (3-5) scenarios
    CONTENTION: BENCHMARK_SCENARIOS.slice(6, 9),
    GROUP_SCALING: BENCHMARK_SCENARIOS.slice(9, 12),
    DEPENDENCIES: BENCHMARK_SCENARIOS.slice(12, 15),
    MIXED: BENCHMARK_SCENARIOS.slice(15, 16),
    OVERLAPPING: BENCHMARK_SCENARIOS.slice(16, 19),
    STRESS: BENCHMARK_SCENARIOS.slice(19, 20)
};

export function getScenariosByCategory(category: keyof typeof SCENARIO_CATEGORIES): BenchmarkConfig[] {
    return SCENARIO_CATEGORIES[category];
}

export function getAllScenarios(): BenchmarkConfig[] {
    return BENCHMARK_SCENARIOS;
}

export function getScenarioByName(name: string): BenchmarkConfig | undefined {
    return BENCHMARK_SCENARIOS.find(scenario => scenario.name === name);
}