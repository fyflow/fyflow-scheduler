import { WorkerInterface, WorkerConfig, WorkerContext } from "../../core/workerInterface.ts";

/**
 * Data Processing Worker - Demonstrates dynamic task spawning
 *
 * Demonstrates:
 * - Data analysis and processing
 * - Dynamic task spawning based on analysis results
 * - Complex workflow orchestration
 * - Real-world data processing patterns
 */
export default class DataProcessor implements WorkerInterface {
    private config: WorkerConfig;

    constructor(config: WorkerConfig = {}) {
        this.config = config;
    }

    async setup() {
        console.log('DataProcessor initialized for data analysis and spawning');
    }

    async run(payload: any, context?: WorkerContext): Promise<any> {
        const { dataset, analysisType = 'mixed' } = payload;

        // Report initial progress
        context?.sendProgress(0, "Starting data analysis...");

        // Simulate data analysis
        await new Promise(resolve => setTimeout(resolve, 100 + Math.random() * 200));
        context?.sendProgress(0.3, "Analyzing dataset structure...");

        // Analyze the dataset and determine what processing is needed
        const analysis = this.analyzeDataset(dataset, analysisType);
        context?.sendProgress(0.6, "Generating processing tasks...");

        if (context && !payload.isChild) {
            // Spawn tasks during execution using new API
            this.spawnProcessingTasks(analysis, dataset, context);
            context.sendProgress(1.0, "Analysis complete, tasks spawned");

            return {
                analysis,
                processedAt: new Date().toISOString(),
                tasksSpawned: analysis.processingNeeded.length
            };
        } else {
            context?.sendProgress(1.0, "Analysis complete");

            return {
                analysis,
                processedAt: new Date().toISOString(),
                tasksSpawned: 0
            };
        }
    }

    private analyzeDataset(dataset: string, analysisType: string) {
        // Simulate different types of data discovery
        const baseComplexity = Math.random();

        const analysis = {
            dataset,
            analysisType,
            complexity: baseComplexity > 0.7 ? 'high' : baseComplexity > 0.3 ? 'medium' : 'low',
            dataTypes: this.detectDataTypes(analysisType),
            estimatedSize: Math.floor(Math.random() * 1000) + 100,
            processingNeeded: [] as { type: string; priority: string; estimatedTime: number }[]
        };

        // Determine what processing is needed based on data types
        analysis.processingNeeded = analysis.dataTypes.map(type => ({
            type,
            priority: Math.random() > 0.5 ? 'high' : 'normal',
            estimatedTime: Math.floor(Math.random() * 5000) + 1000
        }));

        return analysis;
    }

    private detectDataTypes(analysisType: string): string[] {
        const allTypes = ['text', 'image', 'audio', 'video', 'structured'];

        switch (analysisType) {
            case 'multimedia':
                return ['image', 'audio', 'video'];
            case 'documents':
                return ['text', 'structured'];
            case 'mixed': {
                // Randomly select 2-4 data types
                const count = Math.floor(Math.random() * 3) + 2;
                return allTypes.sort(() => Math.random() - 0.5).slice(0, count);
            }
            default:
                return ['text'];
        }
    }

    private spawnProcessingTasks(analysis: any, dataset: string, context: WorkerContext) {
        for (const processing of analysis.processingNeeded) {
            const taskId = `${processing.type}-${Math.random().toString(36).substr(2, 9)}`;

            let workerType: string;
            let groups: string[] = [];

            // Map data types to appropriate workers
            switch (processing.type) {
                case 'image':
                case 'video':
                    workerType = 'ProgressWorker'; // Use available worker
                    groups = ['cpu']; // These might need CPU resources
                    break;
                case 'audio':
                    workerType = 'ProgressWorker';
                    groups = ['cpu'];
                    break;
                case 'text':
                case 'structured':
                    workerType = 'DataProcessor'; // Use available worker
                    break;
                default:
                    workerType = 'ProgressWorker';
            }

            // Spawn task using new API
            context.spawnTask({
                id: taskId,
                workerType,
                payload: {
                    // For ProgressWorker, use smaller size for demo purposes
                    ...(workerType === 'ProgressWorker' ? {
                        operation: processing.type,
                        size: 50, // Smaller workload for faster demo
                        includeDetails: true
                    } : {
                        algorithm: processing.type === 'text' ? 'sort' : 'prime',
                        data: `${processing.type} data from ${dataset}`,
                    }),
                    isChild: true,
                    priority: processing.priority,
                    sourceDataset: dataset
                },
                workerGroups: groups // Groups will be combined with WorkerManager groups
            });

            console.log(`Spawned ${processing.type} processing task: ${taskId}`);
        }
    }


    async teardown() {
        console.log('DataProcessor cleanup completed');
    }
}
