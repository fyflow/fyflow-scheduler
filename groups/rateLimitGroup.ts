import type { ResourceGroup, ResourceGroupMetrics, ResourceGroupStats } from './resourceGroup.ts';

/** Per-window view returned by {@link RateLimitGroup.getStatus}. */
export interface RateWindowStatus {
  limit: number;
  windowMs: number;
  current: number;
  completed: number;
  running: number;
  remaining: number;
  resetTime: number;
}

/** Snapshot returned by {@link RateLimitGroup.getStatus}. */
export interface RateLimitStatus {
  running: number;
  windows: RateWindowStatus[];
  canAcceptNew: boolean;
}

export interface RateWindow {
  limit: number;      // Maximum requests allowed
  windowMs: number;   // Time window in milliseconds
}

/**
 * Time-window throttling, for API quotas and similar limits.
 *
 * ```typescript
 * const api = new RateLimitGroup([
 *   { limit: 10,  windowMs: 1000 },   // 10 per second
 *   { limit: 100, windowMs: 60_000 }  // and 100 per minute
 * ], 'api');
 * ```
 *
 * Every window must have room before a task runs. Tasks over the limit wait in
 * the scheduler's blocked queue and are retried once a window rolls over - they
 * are never dropped.
 */
export class RateLimitGroup extends EventTarget implements ResourceGroup {
  readonly id: string;
  readonly type = 'rate-limit' as const;
  private windows: RateWindow[];
  private requestCounts: Map<string, [number, number][]> = new Map(); // windowIndex -> [timestamp, count][]
  private runningStartTimes: number[] = []; // Track start times of currently running tasks
  private running = 0;
  private stats = {
    totalAcquired: 0,
    totalReleased: 0
  };

  constructor(windows: RateWindow[], id?: string) {
    super();
    this.windows = windows;
    this.id = id || `ratelimit-${Math.random().toString(36).substr(2, 9)}`;
    // Initialize request tracking for each window
    this.windows.forEach((_, index) => {
      this.requestCounts.set(index.toString(), []);
    });
  }

  /**
   * Check if a new request can be accepted based on all rate limit windows
   */
  canRun(): boolean {
    const now = Date.now();

    // Check each window to see if we're within limits
    for (let i = 0; i < this.windows.length; i++) {
      const window = this.windows[i];
      const windowKey = i.toString();
      const requests = this.requestCounts.get(windowKey)!;

      // Clean up old completed requests outside the current window
      const windowStart = now - window.windowMs;
      const validCompletedRequests = requests.filter(([timestamp]) => timestamp >= windowStart);
      this.requestCounts.set(windowKey, validCompletedRequests);

      // Count currently running requests that started within this window
      const validRunningRequests = this.runningStartTimes.filter(startTime => startTime >= windowStart);

      // Total requests in window = completed + running
      const totalRequestsInWindow = validCompletedRequests.length + validRunningRequests.length;

      // Check if adding one more request would exceed the limit
      if (totalRequestsInWindow >= window.limit) {
        return false;
      }
    }

    return true;
  }

  getMetrics(): ResourceGroupMetrics {
    // For rate limiting, use the most restrictive window as the "limit"
    const mostRestrictiveWindow = this.windows.reduce((min, w) => w.limit < min.limit ? w : min, this.windows[0]);

    return {
      limit: mostRestrictiveWindow.limit,
      running: this.running,
      available: Math.max(0, mostRestrictiveWindow.limit - this.running),
      utilization: this.running / mostRestrictiveWindow.limit
    };
  }

  getStats(): ResourceGroupStats {
    return {
      totalAcquired: this.stats.totalAcquired,
      totalReleased: this.stats.totalReleased
    };
  }

  /**
   * Called when a task starts - tracks running count and reserves slot in rate limit
   */
  onStart(): void {
    const now = Date.now();
    this.running++;
    this.runningStartTimes.push(now);
    this.stats.totalAcquired++;
  }

  /**
   * Called when a task finishes - records completion in rate limit windows and removes from running
   */
  onFinish(): void {
    const now = Date.now();
    this.running = Math.max(0, this.running - 1);
    this.stats.totalReleased++;

    // Remove the oldest running start time (FIFO)
    if (this.runningStartTimes.length > 0) {
      this.runningStartTimes.shift();
    }

    // Record this completed request in all windows
    this.windows.forEach((_, index) => {
      const windowKey = index.toString();
      const requests = this.requestCounts.get(windowKey)!;
      requests.push([now, 1]);
    });
  }

  /**
   * Get current status for monitoring
   */
  getStatus(): RateLimitStatus {
    const now = Date.now();
    const windowStatus: RateWindowStatus[] = this.windows.map((window, index) => {
      const windowKey = index.toString();
      const requests = this.requestCounts.get(windowKey)!;
      const windowStart = now - window.windowMs;

      // Count completed requests in window
      const completedInWindow = requests.filter(([timestamp]) => timestamp >= windowStart).length;

      // Count running requests that started in window
      const runningInWindow = this.runningStartTimes.filter(startTime => startTime >= windowStart).length;

      // Total requests in window
      const currentCount = completedInWindow + runningInWindow;

      return {
        limit: window.limit,
        windowMs: window.windowMs,
        current: currentCount,
        completed: completedInWindow,
        running: runningInWindow,
        remaining: Math.max(0, window.limit - currentCount),
        resetTime: windowStart + window.windowMs
      };
    });

    return {
      running: this.running,
      windows: windowStatus,
      canAcceptNew: this.canRun()
    };
  }

  /**
   * Clear all rate limit history (useful for testing or reset)
   */
  reset(): void {
    this.requestCounts.clear();
    this.windows.forEach((_, index) => {
      this.requestCounts.set(index.toString(), []);
    });
    this.runningStartTimes = [];
    this.running = 0;
  }

  /**
   * Helper to create common rate limit configurations
   */
  static createWindows(configs: Array<{limit: number, seconds: number}>): RateWindow[] {
    return configs.map(config => ({
      limit: config.limit,
      windowMs: config.seconds * 1000
    }));
  }
}

// Helper function to create rate limit group with common patterns
export function createRateLimitGroup(configs: Array<{limit: number, seconds: number}>): RateLimitGroup {
  return new RateLimitGroup(RateLimitGroup.createWindows(configs));
}