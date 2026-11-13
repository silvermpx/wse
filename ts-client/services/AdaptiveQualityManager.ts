// =============================================================================
// WebSocket Engine - Adaptive Quality Manager
// =============================================================================
//
// Turns the measured health of the realtime connection into a data-fetching
// strategy. An app that also loads data over HTTP with TanStack Query then
// polls only as much as the connection needs: hardly at all while the socket
// is fast and steady, more on a slow or unstable link, and steadily while it
// is down.
//
// Inputs: the round-trip time of every heartbeat (ping/PONG), the connection
// state, and the reconnects of the last few minutes. Output: the
// `connectionQualityChanged` window event and, when a QueryClient is given,
// the query defaults staleTime, refetchInterval and refetchOnWindowFocus.
// Every other default of the app is left alone, and the app's own values of
// those three come back when the session ends.

import { ConnectionQuality, ConnectionState } from '../types.js';
import { classifyLatency } from './NetworkMonitor.js';
import { dispatchWSEEvent } from '../utils/events.js';
import { logger } from '../utils/logger.js';

export interface QualityStrategy {
  name: string;
  staleTime: number;
  refetchInterval: number | false;
  refetchOnWindowFocus: boolean;
}

/** Query defaults per connection quality. */
export const QUALITY_STRATEGIES: Readonly<Record<
  ConnectionQuality.EXCELLENT | ConnectionQuality.GOOD | ConnectionQuality.FAIR | ConnectionQuality.POOR,
  QualityStrategy
>> = Object.freeze({
  [ConnectionQuality.EXCELLENT]: {
    name: 'WebSocket Optimized',
    staleTime: Infinity,
    refetchInterval: false,
    refetchOnWindowFocus: false,
  },
  [ConnectionQuality.GOOD]: {
    name: 'WebSocket Primary',
    staleTime: 60_000,
    refetchInterval: false,
    refetchOnWindowFocus: false,
  },
  [ConnectionQuality.FAIR]: {
    name: 'Hybrid Mode',
    staleTime: 10_000,
    refetchInterval: 30_000,
    refetchOnWindowFocus: true,
  },
  [ConnectionQuality.POOR]: {
    name: 'Polling Fallback',
    staleTime: 5_000,
    refetchInterval: 10_000,
    refetchOnWindowFocus: true,
  },
});

type QueryDefaults = { queries?: Record<string, unknown>; [key: string]: unknown };

/** The part of TanStack Query's QueryClient this manager uses (v4 and v5). */
export interface QueryClientLike {
  getDefaultOptions?: () => QueryDefaults;
  setDefaultOptions: (options: QueryDefaults) => void;
  invalidateQueries: () => unknown;
}

export interface AdaptiveQualityOptions {
  /** QueryClient whose query defaults follow the connection quality. */
  queryClient?: QueryClientLike | null;
  /**
   * An outage shorter than this (a quick or planned reconnect) changes
   * nothing; a longer one switches to the polling strategy. Default 5000.
   */
  outageGraceMs?: number;
}

export interface QualityChangeDetail {
  quality: ConnectionQuality;
  previous: ConnectionQuality | null;
  strategy: string;
  connected: boolean;
  metrics: { latency: number | null; jitter: number | null; recentReconnects: number };
}

const MANAGED_KEYS = ['staleTime', 'refetchInterval', 'refetchOnWindowFocus'] as const;
// Recent heartbeats only: a verdict should follow the link as it is now.
const RTT_WINDOW = 5;
const RECONNECT_WINDOW_MS = 5 * 60_000;
// This many reconnects inside the window mark the link unstable: the
// strategy keeps a polling safety net even when the round trips look fast.
const UNSTABLE_RECONNECTS = 3;
// A better verdict must hold for this many evaluations in a row before the
// strategy relaxes, so one fast heartbeat after a bad patch changes nothing.
const UPGRADE_CONFIRMATIONS = 2;
const DEFAULT_OUTAGE_GRACE_MS = 5000;

const RANK: Record<ConnectionQuality, number> = {
  [ConnectionQuality.EXCELLENT]: 4,
  [ConnectionQuality.GOOD]: 3,
  [ConnectionQuality.FAIR]: 2,
  [ConnectionQuality.POOR]: 1,
  [ConnectionQuality.UNKNOWN]: 0,
};

function strategyFor(quality: ConnectionQuality): QualityStrategy {
  return quality === ConnectionQuality.UNKNOWN
    ? QUALITY_STRATEGIES[ConnectionQuality.POOR]
    : QUALITY_STRATEGIES[quality];
}

export class AdaptiveQualityManager {
  private readonly queryClient: QueryClientLike | null;
  private readonly outageGraceMs: number;
  // The app's own values of the managed keys, read before the first change.
  private baseline: Record<string, unknown> | null = null;
  private applied: ConnectionQuality | null = null;
  private pending: { quality: ConnectionQuality; count: number } | null = null;
  private rtts: number[] = [];
  private reconnects: number[] = [];
  private connected = false;
  private everConnected = false;
  // Down for longer than the grace period (set by the outage timer).
  private outageLong = false;
  private planned = false;
  private outageTimer: ReturnType<typeof setTimeout> | null = null;
  private destroyed = false;

  constructor(options: AdaptiveQualityOptions = {}) {
    this.queryClient = options.queryClient ?? null;
    const grace = options.outageGraceMs ?? DEFAULT_OUTAGE_GRACE_MS;
    this.outageGraceMs = Number.isFinite(grace) && grace >= 0 ? grace : DEFAULT_OUTAGE_GRACE_MS;
    // Not connected yet: a first connection that takes longer than the grace
    // period is an outage too (the server may be unreachable).
    this.markDown();
  }

  /** One heartbeat round trip in ms. */
  recordLatency(rttMs: number): void {
    if (this.destroyed || !Number.isFinite(rttMs) || rttMs < 0) return;
    this.rtts.push(rttMs);
    if (this.rtts.length > RTT_WINDOW) this.rtts.shift();
    this.evaluate();
  }

  /** The connection state changed. */
  setConnectionState(state: ConnectionState): void {
    if (this.destroyed) return;

    if (state === ConnectionState.CONNECTED || state === ConnectionState.DEGRADED) {
      if (this.connected) return;
      if (this.everConnected && !this.planned) this.reconnects.push(Date.now());
      this.connected = true;
      this.everConnected = true;
      this.outageLong = false;
      this.planned = false;
      this.clearOutageTimer();
      // Round trips of the previous socket say nothing about this one.
      this.rtts = [];
      this.evaluate();
      return;
    }

    if (!this.connected) return;
    this.connected = false;
    this.markDown();
  }

  /**
   * The next reconnect is planned (forceReconnect, changeEndpoint), so it does
   * not count towards an unstable link. If it takes longer than the grace
   * period it is still an outage.
   */
  expectReconnect(): void {
    if (this.destroyed) return;
    this.planned = true;
  }

  getCurrentQuality(): ConnectionQuality | null {
    return this.applied;
  }

  getCurrentStrategyName(): string {
    return this.applied === null ? 'Unknown' : strategyFor(this.applied).name;
  }

  /** Forget the measurements and give the app its own query defaults back. */
  reset(): void {
    this.clearOutageTimer();
    this.pending = null;
    this.rtts = [];
    this.reconnects = [];
    this.restoreDefaults();
    this.applied = null;
  }

  destroy(): void {
    if (this.destroyed) return;
    this.reset();
    this.destroyed = true;
  }

  private evaluate(): void {
    if (this.destroyed) return;
    const quality = this.measure();
    if (quality === ConnectionQuality.UNKNOWN) return; // nothing measured yet

    if (this.applied !== null && RANK[quality] > RANK[this.applied]) {
      if (this.pending?.quality === quality) {
        this.pending.count++;
      } else {
        this.pending = { quality, count: 1 };
      }
      if (this.pending.count < UPGRADE_CONFIRMATIONS) return;
    }
    this.pending = null;
    this.apply(quality);
  }

  /** The connection went down: a short outage changes nothing, the timer judges a long one. */
  private markDown(): void {
    this.outageLong = false;
    this.clearOutageTimer();
    this.outageTimer = setTimeout(() => {
      this.outageTimer = null;
      this.outageLong = true;
      this.evaluate();
    }, this.outageGraceMs);
  }

  private measure(): ConnectionQuality {
    const now = Date.now();
    if (!this.connected) {
      // While down, live updates are not arriving: poll.
      return this.outageLong ? ConnectionQuality.POOR : (this.applied ?? ConnectionQuality.UNKNOWN);
    }
    if (this.rtts.length === 0) return ConnectionQuality.UNKNOWN;

    let quality = classifyLatency(this.average(), this.jitter());
    this.reconnects = this.reconnects.filter(t => now - t < RECONNECT_WINDOW_MS);
    if (this.reconnects.length >= UNSTABLE_RECONNECTS && RANK[quality] > RANK[ConnectionQuality.FAIR]) {
      quality = ConnectionQuality.FAIR;
    }
    return quality;
  }

  private apply(quality: ConnectionQuality): void {
    if (quality === this.applied) return;
    const previous = this.applied;
    this.applied = quality;
    const strategy = strategyFor(quality);
    logger.info(`[AdaptiveQuality] ${previous ?? 'unknown'} -> ${quality}: ${strategy.name}`);

    if (this.queryClient) {
      try {
        const current = this.queryClient.getDefaultOptions?.() ?? {};
        const queries = { ...(current.queries ?? {}) };
        if (this.baseline === null) {
          this.baseline = {};
          for (const key of MANAGED_KEYS) {
            if (key in queries) this.baseline[key] = queries[key];
          }
        }
        queries.staleTime = strategy.staleTime;
        queries.refetchInterval = strategy.refetchInterval;
        queries.refetchOnWindowFocus = strategy.refetchOnWindowFocus;
        // setDefaultOptions replaces all defaults: pass the app's others through.
        this.queryClient.setDefaultOptions({ ...current, queries });

        // Live updates may have been missed: refetch now, not at the next interval.
        if (quality === ConnectionQuality.POOR && previous !== null) {
          void Promise.resolve(this.queryClient.invalidateQueries()).catch((error: unknown) => {
            logger.warn('[AdaptiveQuality] invalidateQueries failed:', error);
          });
        }
      } catch (error) {
        logger.warn('[AdaptiveQuality] Updating the QueryClient defaults failed:', error);
      }
    }

    const detail: QualityChangeDetail = {
      quality,
      previous,
      strategy: strategy.name,
      connected: this.connected,
      metrics: {
        latency: this.rtts.length > 0 ? this.average() : null,
        jitter: this.rtts.length > 1 ? this.jitter() : null,
        recentReconnects: this.reconnects.length,
      },
    };
    dispatchWSEEvent('connectionQualityChanged', detail);
  }

  private restoreDefaults(): void {
    if (!this.queryClient || this.baseline === null) return;
    try {
      const current = this.queryClient.getDefaultOptions?.() ?? {};
      const queries = { ...(current.queries ?? {}) };
      for (const key of MANAGED_KEYS) {
        if (key in this.baseline) {
          queries[key] = this.baseline[key];
        } else {
          delete queries[key];
        }
      }
      this.queryClient.setDefaultOptions({ ...current, queries });
    } catch (error) {
      logger.warn('[AdaptiveQuality] Restoring the QueryClient defaults failed:', error);
    }
    this.baseline = null;
  }

  private average(): number {
    return this.rtts.reduce((a, b) => a + b, 0) / this.rtts.length;
  }

  private jitter(): number {
    if (this.rtts.length < 2) return 0;
    let sum = 0;
    for (let i = 1; i < this.rtts.length; i++) sum += Math.abs(this.rtts[i] - this.rtts[i - 1]);
    return sum / (this.rtts.length - 1);
  }

  private clearOutageTimer(): void {
    if (this.outageTimer) {
      clearTimeout(this.outageTimer);
      this.outageTimer = null;
    }
  }
}
