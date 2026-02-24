import hdr from 'hdr-histogram-js';

export class LatencyHistogram {
  private hist: hdr.Histogram;

  constructor() {
    this.hist = hdr.build({
      lowestDiscernibleValue: 1,
      highestTrackableValue: 60_000_000, // 60s in microseconds
      numberOfSignificantValueDigits: 3,
    });
  }

  recordUs(us: number): void {
    this.hist.recordValue(Math.max(us, 1));
  }

  recordMs(ms: number): void {
    this.recordUs(Math.round(ms * 1000));
  }

  merge(other: LatencyHistogram): void {
    this.hist.add(other.hist);
  }

  len(): number {
    return this.hist.totalCount;
  }

  isEmpty(): boolean {
    return this.hist.totalCount === 0;
  }

  percentileMs(p: number): number {
    return this.hist.getValueAtPercentile(p) / 1000;
  }

  get minMs(): number {
    return this.hist.minNonZeroValue / 1000;
  }

  get maxMs(): number {
    return this.hist.maxValue / 1000;
  }

  get meanMs(): number {
    return this.hist.mean / 1000;
  }

  get stdevMs(): number {
    return this.hist.stdDeviation / 1000;
  }

  printSummary(prefix: string = '    '): void {
    if (this.isEmpty()) return;
    const fmt = (v: number) => v.toFixed(2);
    console.log(`${prefix}p50:          ${fmt(this.percentileMs(50))} ms`);
    console.log(`${prefix}p95:          ${fmt(this.percentileMs(95))} ms`);
    console.log(`${prefix}p99:          ${fmt(this.percentileMs(99))} ms`);
    console.log(`${prefix}p99.9:        ${fmt(this.percentileMs(99.9))} ms`);
    console.log(`${prefix}p99.99:       ${fmt(this.percentileMs(99.99))} ms`);
    console.log(`${prefix}min:          ${fmt(this.minMs)} ms`);
    console.log(`${prefix}max:          ${fmt(this.maxMs)} ms`);
    console.log(`${prefix}mean:         ${fmt(this.meanMs)} ms`);
    console.log(`${prefix}stdev:        ${fmt(this.stdevMs)} ms`);
  }

  toSummary(): LatencySummary {
    return {
      p50: this.percentileMs(50),
      p95: this.percentileMs(95),
      p99: this.percentileMs(99),
      p99_9: this.percentileMs(99.9),
      p99_99: this.percentileMs(99.99),
      min: this.minMs,
      max: this.maxMs,
      mean: this.meanMs,
      stdev: this.stdevMs,
    };
  }
}

export interface LatencySummary {
  p50: number;
  p95: number;
  p99: number;
  p99_9: number;
  p99_99: number;
  min: number;
  max: number;
  mean: number;
  stdev: number;
}

export function printHeader(title: string, description: string): void {
  console.log('');
  console.log('='.repeat(70));
  console.log(`  ${title}`);
  console.log(`  ${description}`);
  console.log('='.repeat(70));
}

export function fmtCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(0)}K`;
  return n.toString();
}

export function fmtBytesPerSec(bytesPerSec: number): string {
  if (bytesPerSec >= 1_000_000_000) return `${(bytesPerSec / 1_000_000_000).toFixed(1)} GB/s`;
  if (bytesPerSec >= 1_000_000) return `${(bytesPerSec / 1_000_000).toFixed(1)} MB/s`;
  if (bytesPerSec >= 1_000) return `${(bytesPerSec / 1_000).toFixed(1)} KB/s`;
  return `${bytesPerSec.toFixed(0)} B/s`;
}
