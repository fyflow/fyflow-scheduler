/**
 * A worker that "loads a model onto a GPU" when it starts and frees it when it
 * stops - the shape that motivates `residentGroups`.
 *
 * The expensive thing happens in setup()/teardown(), not run(). That is the
 * whole point: the VRAM is held because the worker *exists*, for the worker's
 * whole lifetime, not just while a task is running.
 */
export default class ModelWorker {
  private model: string;
  private sizeGb: number;
  private loadMs: number;

  constructor(config: any = {}) {
    this.model = config.model ?? "unnamed";
    this.sizeGb = config.sizeGb ?? 1;
    this.loadMs = config.loadMs ?? 120;
  }

  async setup() {
    // Loading weights onto the GPU - slow, and the memory stays held afterwards
    await new Promise(resolve => setTimeout(resolve, this.loadMs));
    console.log(`   🟢 LOADED   ${this.model} (${this.sizeGb}GB)`);
  }

  async teardown() {
    console.log(`   ⚪ UNLOADED ${this.model} (${this.sizeGb}GB freed)`);
  }

  async run(payload: any) {
    await new Promise(resolve => setTimeout(resolve, payload.inferMs ?? 60));
    return { model: this.model, prompt: payload.prompt, tokens: 128 };
  }
}
