// =============================================================================
// WebSocket Event System - Adaptive Quality Manager
// =============================================================================
//
// Adapts application behavior based on WebSocket connection quality.
// By default operates standalone (dispatches events only).
// Optionally accepts a React Query QueryClient to adjust query defaults.

import { ConnectionQuality } from '../types';
import { logger } from '../utils/logger';

// Industry-standard quality strategies
const STRATEGIES = {
  excellent: {
    staleTime: Infinity,
    refetchInterval: false,
    refetchOnWindowFocus: false,
    name: 'WebSocket Optimized',
  },
  good: {
    staleTime: 60000, // 1 min
    refetchInterval: false,
    refetchOnWindowFocus: false,
    name: 'WebSocket Primary',
  },
  fair: {
    staleTime: 10000, // 10 sec
    refetchInterval: 30000, // 30 sec polling backup
    refetchOnWindowFocus: true,
    name: 'Hybrid Mode',
  },
  poor: {
    staleTime: 5000, // 5 sec
    refetchInterval: 10000, // 10 sec aggressive polling
    refetchOnWindowFocus: true,
    name: 'Polling Fallback',
  },
  unknown: {
    staleTime: 5000,
    refetchInterval: 15000,
    refetchOnWindowFocus: true,
    name: 'Conservative',
  },
} as const;

/**
 * Minimal interface for QueryClient integration.
 * Matches TanStack Query's QueryClient.setDefaultOptions signature.
 */
interface QueryClientLike {
  setDefaultOptions: (options: { queries: Record<string, any> }) => void;
  invalidateQueries: () => void;
}

export class AdaptiveQualityManager {
  private currentQuality: ConnectionQuality | null = null;
  private lastUpdate: number = 0;
  private queryClient: QueryClientLike | null;

  /**
   * @param queryClient Optional QueryClient instance. When provided, React Query
   *   defaults are adjusted based on connection quality. When omitted, only
   *   CustomEvents are dispatched.
   */
  constructor(queryClient?: QueryClientLike | null) {
    this.queryClient = queryClient ?? null;
  }

  /**
   * Update strategy based on connection quality
   */
  updateQuality(quality: ConnectionQuality, metrics?: { latency?: number; jitter?: number; packetLoss?: number }): void {
    // Skip if quality hasn't changed
    if (quality === this.currentQuality) return;

    const strategy = STRATEGIES[quality] || STRATEGIES.unknown;
    const oldQuality = this.currentQuality;
    this.currentQuality = quality;
    this.lastUpdate = Date.now();

    logger.info(`[AdaptiveQuality] ${oldQuality || 'unknown'} -> ${quality}: ${strategy.name}`);
    if (metrics) {
      logger.info(`[AdaptiveQuality] Metrics:`, metrics);
    }

    // Update React Query defaults if client is available
    if (this.queryClient) {
      this.queryClient.setDefaultOptions({
        queries: {
          staleTime: strategy.staleTime,
          refetchInterval: strategy.refetchInterval,
          refetchOnWindowFocus: strategy.refetchOnWindowFocus,
          refetchOnReconnect: true,
          retry: 3,
          retryDelay: (attemptIndex: number) => Math.min(1000 * Math.pow(2, attemptIndex), 30000),
        },
      });

      // If degraded to poor, force refetch all queries
      if (quality === 'poor' && oldQuality !== 'poor') {
        logger.warn('[AdaptiveQuality] Degraded to poor - invalidating all queries');
        this.queryClient.invalidateQueries();
      }
    }

    // Dispatch event for UI notifications
    window.dispatchEvent(new CustomEvent('connectionQualityChanged', {
      detail: { quality, strategy: strategy.name, metrics },
    }));
  }

  /**
   * Reset to default conservative mode
   */
  reset(): void {
    logger.info('[AdaptiveQuality] Resetting to conservative mode');
    this.updateQuality(ConnectionQuality.UNKNOWN);
  }

  /**
   * Get current quality for diagnostics
   */
  getCurrentQuality(): ConnectionQuality | null {
    return this.currentQuality;
  }

  /**
   * Get strategy name for UI display
   */
  getCurrentStrategyName(): string {
    if (!this.currentQuality) return 'Unknown';
    return STRATEGIES[this.currentQuality]?.name || 'Unknown';
  }
}
