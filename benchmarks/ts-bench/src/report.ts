import type { LatencySummary } from './stats.js';

export interface TierResult {
  tier: number;
  connected: number;
  errors: number;
  durationSecs: number;
  messagesSent: number;
  messagesReceived: number;
  bytesSent: number;
  latency: LatencySummary | null;
  extra: Record<string, unknown>;
}

export interface TestReport {
  testName: string;
  results: TierResult[];
}

export function toJson(reports: TestReport[]): string {
  return JSON.stringify(reports, null, 2);
}

export function toMarkdown(reports: TestReport[]): string {
  const lines: string[] = [];
  lines.push('# WSE TypeScript Benchmark Results\n');

  for (const report of reports) {
    lines.push(`## ${report.testName}\n`);
    if (report.results.length === 0) {
      lines.push('No results.\n');
      continue;
    }

    const hasLatency = report.results.some((r) => r.latency !== null);
    const hasMsgs = report.results.some((r) => r.messagesSent > 0);

    if (hasLatency && !hasMsgs) {
      lines.push('| Connections | p50 | p95 | p99 | p99.9 | Errors |');
      lines.push('|-------------|-----|-----|-----|-------|--------|');
      for (const r of report.results) {
        const l = r.latency;
        lines.push(
          `| ${r.tier} | ${l ? l.p50.toFixed(2) : '-'} ms | ${l ? l.p95.toFixed(2) : '-'} ms | ${l ? l.p99.toFixed(2) : '-'} ms | ${l ? l.p99_9.toFixed(2) : '-'} ms | ${r.errors} |`
        );
      }
    } else if (hasMsgs) {
      lines.push('| Connections | Msg/s | MB/s | Per-conn msg/s | Errors |');
      lines.push('|-------------|-------|------|---------------|--------|');
      for (const r of report.results) {
        const rate = r.durationSecs > 0 ? r.messagesSent / r.durationSecs : 0;
        const mbps = r.durationSecs > 0 ? r.bytesSent / r.durationSecs / 1_000_000 : 0;
        const perConn = r.connected > 0 ? Math.round(rate / r.connected) : 0;
        lines.push(
          `| ${r.tier} | ${fmtRate(rate)} | ${mbps.toFixed(1)} | ${perConn} | ${r.errors} |`
        );
      }
    } else {
      lines.push('| Connections | Connected | Errors | Duration |');
      lines.push('|-------------|-----------|--------|----------|');
      for (const r of report.results) {
        lines.push(
          `| ${r.tier} | ${r.connected} | ${r.errors} | ${r.durationSecs.toFixed(2)}s |`
        );
      }
    }

    lines.push('');
  }

  return lines.join('\n');
}

function fmtRate(rate: number): string {
  if (rate >= 1_000_000) return `${(rate / 1_000_000).toFixed(1)}M`;
  if (rate >= 1_000) return `${(rate / 1_000).toFixed(0)}K`;
  return rate.toFixed(0);
}
