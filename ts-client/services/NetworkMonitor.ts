// =============================================================================
// WebSocket Engine - Network Monitor
// =============================================================================

import { NetworkDiagnostics, ConnectionQuality } from '../types';
import { useWSEStore } from '../stores/useWSEStore';
import { logger } from '../utils/logger';

export class NetworkMonitor {
  private latencyHistory: number[] = [];
  private lastDiagnostics: NetworkDiagnostics | null = null;
  private diagnosticsInterval: NodeJS.Timeout | null = null;

  constructor(
    private maxHistorySize = 100,
    private diagnosticsIntervalMs = 30000
  ) {
    if (typeof window !== 'undefined') {
      this.startDiagnostics();
    }
  }

  recordLatency(latency: number): void {
    // Fed from MessageProcessor's PONG handler (the single source of latency
    // truth). We deliberately do NOT also push to the store here: MessageProcessor
    // already records every RTT to the store, so writing it again would
    // double-count and skew the store's EMA / percentiles. The monitor keeps its
    // own window purely to derive jitter and quality.
    this.latencyHistory.push(latency);
    if (this.latencyHistory.length > this.maxHistorySize) {
      this.latencyHistory.shift();
    }
  }

  analyze(): NetworkDiagnostics {
    // With no RTT samples yet we report UNKNOWN rather than fabricating a verdict
    // -- an empty window must not read as "EXCELLENT".
    if (this.latencyHistory.length === 0) {
      const diagnostics: NetworkDiagnostics = {
        quality: ConnectionQuality.UNKNOWN,
        stability: 100,
        jitter: 0,
        packetLoss: 0,
        roundTripTime: 0,
        suggestions: [],
        lastAnalysis: Date.now(),
      };
      this.lastDiagnostics = diagnostics;
      return diagnostics;
    }

    const avgLatency = this.calculateAverage(this.latencyHistory);
    const jitter = this.calculateJitter();
    const quality = this.determineQuality(avgLatency, jitter);
    const suggestions = this.generateSuggestions(quality, avgLatency, jitter);

    const diagnostics: NetworkDiagnostics = {
      quality,
      stability: Math.max(0, 100 - (jitter / 100) * 50),
      jitter,
      // App-layer packet loss isn't a meaningful client metric over a pub/sub
      // WebSocket (the client receives far more than it sends, and TCP doesn't
      // surface frame loss), so we don't fabricate it. 0 == "not measured"; it
      // never skews the quality verdict, which is driven by real latency+jitter.
      packetLoss: 0,
      roundTripTime: avgLatency,
      suggestions,
      lastAnalysis: Date.now(),
    };

    this.lastDiagnostics = diagnostics;
    return diagnostics;
  }

  private calculateAverage(values: number[]): number {
    if (values.length === 0) return 0;
    return values.reduce((a, b) => a + b, 0) / values.length;
  }

  private calculateJitter(): number {
    if (this.latencyHistory.length < 2) return 0;

    let sumDiff = 0;
    for (let i = 1; i < this.latencyHistory.length; i++) {
      sumDiff += Math.abs(this.latencyHistory[i] - this.latencyHistory[i - 1]);
    }

    return sumDiff / (this.latencyHistory.length - 1);
  }

  private determineQuality(
    avgLatency: number,
    jitter: number
  ): ConnectionQuality {
    if (avgLatency > 300 || jitter > 100) {
      return ConnectionQuality.POOR;
    } else if (avgLatency > 150 || jitter > 50) {
      return ConnectionQuality.FAIR;
    } else if (avgLatency > 75 || jitter > 25) {
      return ConnectionQuality.GOOD;
    }
    return ConnectionQuality.EXCELLENT;
  }

  private generateSuggestions(
    quality: ConnectionQuality,
    avgLatency: number,
    jitter: number
  ): string[] {
    const suggestions: string[] = [];

    if (quality === ConnectionQuality.POOR || quality === ConnectionQuality.FAIR) {
      if (avgLatency > 200) {
        suggestions.push('High latency detected. Consider checking your network connection or switching to a closer server.');
      }
      if (jitter > 75) {
        suggestions.push('High network jitter detected. This may cause unstable connections.');
      }
      suggestions.push('Try closing unnecessary applications that use network bandwidth.');
      suggestions.push('Consider using a wired connection instead of Wi-Fi for better stability.');
    }

    return suggestions;
  }

  private startDiagnostics(): void {
    this.diagnosticsInterval = setInterval(() => {
      const diagnostics = this.analyze();

      const store = useWSEStore.getState();
      store.updateDiagnostics(diagnostics);

      if (diagnostics.quality === ConnectionQuality.POOR) {
        logger.warn('Poor connection quality detected:', diagnostics);
      }
    }, this.diagnosticsIntervalMs);
  }

  getLastDiagnostics(): NetworkDiagnostics | null {
    return this.lastDiagnostics;
  }

  getLatencyStats(): {
    current: number | null;
    average: number;
    min: number | null;
    max: number | null;
    jitter: number;
  } {
    const current = this.latencyHistory[this.latencyHistory.length - 1] || null;
    const average = this.calculateAverage(this.latencyHistory);
    const min = this.latencyHistory.length > 0 ? Math.min(...this.latencyHistory) : null;
    const max = this.latencyHistory.length > 0 ? Math.max(...this.latencyHistory) : null;
    const jitter = this.calculateJitter();

    return { current, average, min, max, jitter };
  }

  reset(): void {
    this.latencyHistory = [];
    this.lastDiagnostics = null;
  }

  destroy(): void {
    if (this.diagnosticsInterval) {
      clearInterval(this.diagnosticsInterval);
      this.diagnosticsInterval = null;
    }
    this.reset();
  }
}
