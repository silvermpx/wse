// =============================================================================
// WebSocket Engine - Network Monitor
// =============================================================================

import { NetworkDiagnostics, ConnectionQuality } from '../types';
import { useWSEStore } from '../stores/useWSEStore';
import { logger } from '../utils/logger';

export class NetworkMonitor {
  private latencyHistory: number[] = [];
  private packetsSent = 0;
  private packetsReceived = 0;
  private bytesHistory: Array<{ timestamp: number; bytes: number }> = [];
  private lastDiagnostics: NetworkDiagnostics | null = null;
  private diagnosticsInterval: NodeJS.Timeout | null = null;

  constructor(
    private maxHistorySize = 100,
    private diagnosticsIntervalMs = 30000
  ) {
    this.startDiagnostics();
  }

  recordLatency(latency: number): void {
    this.latencyHistory.push(latency);

    if (this.latencyHistory.length > this.maxHistorySize) {
      this.latencyHistory.shift();
    }

    const store = useWSEStore.getState();
    store.recordLatency(latency);
  }

  recordPacketSent(): void {
    this.packetsSent++;
  }

  recordPacketReceived(): void {
    this.packetsReceived++;
  }

  recordBytes(bytes: number): void {
    this.bytesHistory.push({
      timestamp: Date.now(),
      bytes
    });

    const cutoff = Date.now() - 60000;
    this.bytesHistory = this.bytesHistory.filter(b => b.timestamp > cutoff);
  }

  analyze(): NetworkDiagnostics {
    const avgLatency = this.calculateAverage(this.latencyHistory);
    const jitter = this.calculateJitter();
    const packetLoss = this.calculatePacketLoss();
    const quality = this.determineQuality(avgLatency, jitter, packetLoss);
    const suggestions = this.generateSuggestions(quality, avgLatency, jitter, packetLoss);

    const diagnostics: NetworkDiagnostics = {
      quality,
      stability: 100 - (jitter / 100) * 50,
      jitter,
      packetLoss,
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

  private calculatePacketLoss(): number {
    if (this.packetsSent === 0) return 0;
    return ((this.packetsSent - this.packetsReceived) / this.packetsSent) * 100;
  }

  private determineQuality(
    avgLatency: number,
    jitter: number,
    packetLoss: number
  ): ConnectionQuality {
    if (avgLatency > 300 || jitter > 100 || packetLoss > 5) {
      return ConnectionQuality.POOR;
    } else if (avgLatency > 150 || jitter > 50 || packetLoss > 2) {
      return ConnectionQuality.FAIR;
    } else if (avgLatency > 75 || jitter > 25 || packetLoss > 0.5) {
      return ConnectionQuality.GOOD;
    }
    return ConnectionQuality.EXCELLENT;
  }

  private generateSuggestions(
    quality: ConnectionQuality,
    avgLatency: number,
    jitter: number,
    packetLoss: number
  ): string[] {
    const suggestions: string[] = [];

    if (quality === ConnectionQuality.POOR || quality === ConnectionQuality.FAIR) {
      if (avgLatency > 200) {
        suggestions.push('High latency detected. Consider checking your network connection or switching to a closer server.');
      }
      if (jitter > 75) {
        suggestions.push('High network jitter detected. This may cause unstable connections.');
      }
      if (packetLoss > 3) {
        suggestions.push('Significant packet loss detected. Check for network congestion or interference.');
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
    this.packetsSent = 0;
    this.packetsReceived = 0;
    this.bytesHistory = [];
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
