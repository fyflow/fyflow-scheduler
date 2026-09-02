// A worker that implements WorkerInterface directly and omits the OPTIONAL
// teardown() hook.
//
// It deliberately does NOT extend BaseWorker: setup() and teardown() are
// abstract there, so extending it cannot express this case at all. Every other
// worker in tests/workers/ implements teardown(), which is exactly why inline
// pools were able to skip their teardown events for a worker without one
// without any test noticing.

export default class NoTeardownWorker {
  private multiplier: number;

  constructor(config: any = {}) {
    this.multiplier = config.multiplier ?? 1;
  }

  // No setup(), no teardown() - both are optional on WorkerInterface.

  run(payload: any): any {
    return { id: payload?.id, value: (payload?.value ?? 0) * this.multiplier };
  }
}
