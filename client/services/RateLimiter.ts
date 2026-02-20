// =============================================================================
// WebSocket Engine - Rate Limiter
// =============================================================================

export class RateLimiter {
  private tokens: number;
  private lastRefill: number;
  private readonly capacity: number;
  private readonly refillRate: number;
  private readonly refillInterval: number;

  constructor(
    capacity: number = 1000,
    refillRate: number = 100,
    refillInterval: number = 1000
  ) {
    this.capacity = capacity;
    this.refillRate = refillRate;
    this.refillInterval = refillInterval;
    this.tokens = capacity;
    this.lastRefill = Date.now();
  }

  // ---------------------------------------------------------------------------
  // Rate Limiting
  // ---------------------------------------------------------------------------

  canSend(): boolean {
    this.refill();

    if (this.tokens >= 1) {
      this.tokens--;
      return true;
    }

    return false;
  }

  tryConsume(tokens: number = 1): boolean {
    this.refill();

    if (this.tokens >= tokens) {
      this.tokens -= tokens;
      return true;
    }

    return false;
  }

  private refill(): void {
    const now = Date.now();
    const timePassed = now - this.lastRefill;

    if (timePassed >= this.refillInterval) {
      const intervalsElapsed = Math.floor(timePassed / this.refillInterval);
      const tokensToAdd = intervalsElapsed * this.refillRate;

      this.tokens = Math.min(this.capacity, this.tokens + tokensToAdd);
      this.lastRefill = now - (timePassed % this.refillInterval);
    }
  }

  // ---------------------------------------------------------------------------
  // Utilities
  // ---------------------------------------------------------------------------

  getRetryAfter(): number {
    if (this.tokens >= 1) {
      return 0;
    }

    const now = Date.now();
    const timeSinceLastRefill = now - this.lastRefill;
    const timeUntilNextRefill = this.refillInterval - timeSinceLastRefill;

    const tokensNeeded = 1 - this.tokens;
    const refillsNeeded = Math.ceil(tokensNeeded / this.refillRate);
    const totalWaitTime = (refillsNeeded - 1) * this.refillInterval + timeUntilNextRefill;

    return Math.max(0, totalWaitTime / 1000);
  }

  getAvailableTokens(): number {
    this.refill();
    return this.tokens;
  }

  getCapacity(): number {
    return this.capacity;
  }

  getUtilization(): number {
    return ((this.capacity - this.tokens) / this.capacity) * 100;
  }

  reset(): void {
    this.tokens = this.capacity;
    this.lastRefill = Date.now();
  }

  // ---------------------------------------------------------------------------
  // Statistics
  // ---------------------------------------------------------------------------

  getStats(): {
    availableTokens: number;
    capacity: number;
    utilizationPercent: number;
    refillRate: number;
    refillInterval: number;
    retryAfter: number;
  } {
    this.refill();

    return {
      availableTokens: this.tokens,
      capacity: this.capacity,
      utilizationPercent: this.getUtilization(),
      refillRate: this.refillRate,
      refillInterval: this.refillInterval,
      retryAfter: this.getRetryAfter(),
    };
  }
}
