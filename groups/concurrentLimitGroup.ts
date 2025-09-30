export class ConcurrentLimitGroup extends EventTarget {
    limit: number;
    running = 0;

    constructor(limit: number) {
      super();
      this.limit = limit;
    }

    canRun() {
      return this.running < this.limit;
    }

    onStart() {
      this.running++;
    }

    onFinish() {
      const wasAtLimit = this.running >= this.limit;
      this.running = Math.max(0, this.running - 1);

      // Emit slot-released event if we were at limit and now have capacity
      if (wasAtLimit && this.canRun()) {
        this.dispatchEvent(new CustomEvent('slot-released', {
          detail: {
            groupType: 'concurrent-limit',
            running: this.running,
            limit: this.limit,
            available: this.limit - this.running
          }
        }));
      }
    }
  }
  