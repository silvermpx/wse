// =============================================================================
// WebSocket Event System - Circuit Breaker
// =============================================================================

import { CircuitBreakerState } from '../types';
import { logger } from './logger';

export interface CircuitBreakerConfig {
  failureThreshold: number;
  resetTimeout: number;
  successThreshold: number;
  monitoringPeriod: number;
}

export class CircuitBreaker {
  private state: CircuitBreakerState = CircuitBreakerState.CLOSED;
  private failures = 0;
  private successes = 0;
  private lastFailureTime = 0;
  private nextRetryTime: number | null = null;
  private readonly config: CircuitBreakerConfig;

  constructor(config: Partial<CircuitBreakerConfig> = {}) {
    this.config = {
      failureThreshold: config.failureThreshold || 5,
      resetTimeout: config.resetTimeout || 60000,
      successThreshold: config.successThreshold || 3,
      monitoringPeriod: config.monitoringPeriod || 300000,
    };
  }

  // ---------------------------------------------------------------------------
  // State Management
  // ---------------------------------------------------------------------------

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    if (!this.canExecute()) {
      throw new Error('Circuit breaker is OPEN');
    }

    try {
      const result = await fn();
      this.recordSuccess();
      return result;
    } catch (error) {
      this.recordFailure();
      throw error;
    }
  }

  canExecute(): boolean {
    if (this.state === CircuitBreakerState.CLOSED) {
      return true;
    }

    if (this.state === CircuitBreakerState.OPEN) {
      const now = Date.now();

      if (this.nextRetryTime && now >= this.nextRetryTime) {
        this.state = CircuitBreakerState.HALF_OPEN;
        this.successes = 0;
        logger.info('Circuit breaker transitioning to HALF_OPEN');
        return true;
      }

      return false;
    }

    // Half-open state
    return true;
  }

  recordSuccess(): void {
    if (this.state === CircuitBreakerState.HALF_OPEN) {
      this.successes++;

      if (this.successes >= this.config.successThreshold) {
        this.close();
      }
    } else if (this.state === CircuitBreakerState.CLOSED) {
      this.failures = 0;
    }
  }

  recordFailure(): void {
    this.failures++;
    this.lastFailureTime = Date.now();

    if (this.state === CircuitBreakerState.HALF_OPEN) {
      this.open();
    } else if (this.state === CircuitBreakerState.CLOSED) {
      if (this.failures >= this.config.failureThreshold) {
        this.open();
      }
    }
  }

  // ---------------------------------------------------------------------------
  // State Transitions
  // ---------------------------------------------------------------------------

  private open(): void {
    this.state = CircuitBreakerState.OPEN;
    this.nextRetryTime = Date.now() + this.config.resetTimeout;

    logger.warn(`Circuit breaker OPENED after ${this.failures} failures`);
  }

  private close(): void {
    this.state = CircuitBreakerState.CLOSED;
    this.failures = 0;
    this.successes = 0;
    this.nextRetryTime = null;

    logger.info('Circuit breaker CLOSED');
  }

  // ---------------------------------------------------------------------------
  // Monitoring
  // ---------------------------------------------------------------------------

  getState(): CircuitBreakerState {
    return this.state;
  }

  getMetrics(): {
    state: CircuitBreakerState;
    failures: number;
    successes: number;
    lastFailureTime: number;
    nextRetryTime: number | null;
  } {
    return {
      state: this.state,
      failures: this.failures,
      successes: this.successes,
      lastFailureTime: this.lastFailureTime,
      nextRetryTime: this.nextRetryTime,
    };
  }

  reset(): void {
    this.state = CircuitBreakerState.CLOSED;
    this.failures = 0;
    this.successes = 0;
    this.lastFailureTime = 0;
    this.nextRetryTime = null;
  }

  // ---------------------------------------------------------------------------
  // Advanced Features
  // ---------------------------------------------------------------------------

  getHealthScore(): number {
    const now = Date.now();
    const timeSinceLastFailure = now - this.lastFailureTime;

    if (this.state === CircuitBreakerState.OPEN) {
      return 0;
    }

    if (this.state === CircuitBreakerState.HALF_OPEN) {
      return 50 * (this.successes / this.config.successThreshold);
    }

    if (this.failures === 0) {
      return 100;
    }

    const failureRate = this.failures / this.config.failureThreshold;
    const timeFactor = Math.min(1, timeSinceLastFailure / this.config.monitoringPeriod);

    return Math.round((1 - failureRate * 0.7) * timeFactor * 100);
  }

  shouldAlert(): boolean {
    return this.state === CircuitBreakerState.OPEN ||
           (this.state === CircuitBreakerState.CLOSED &&
            this.failures >= this.config.failureThreshold * 0.8);
  }
}

// Factory function for creating circuit breakers
export function createCircuitBreaker(
  name: string,
  config?: Partial<CircuitBreakerConfig>
): CircuitBreaker {
  const breaker = new CircuitBreaker(config);

  logger.debug(`Created circuit breaker: ${name}`);

  return breaker;
}
