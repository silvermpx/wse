import { Command } from 'commander';

export type TestName =
  | 'connection-storm'
  | 'ping-latency'
  | 'throughput'
  | 'size-matrix'
  | 'format-comparison'
  | 'sustained-hold'
  | 'connection-limit';

export const ALL_TESTS: TestName[] = [
  'connection-storm',
  'ping-latency',
  'throughput',
  'size-matrix',
  'format-comparison',
  'sustained-hold',
  'connection-limit',
];

const DEFAULT_TIERS = [100, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 30_000, 50_000];
const EXTENDED_TIERS = [100, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 30_000, 50_000, 75_000, 100_000];

export interface Config {
  host: string;
  port: number;
  secret: string;
  test: TestName | 'all';
  tiers: number[] | null;
  duration: number;
  batchSize: number;
  maxConnections: number;
  output: 'text' | 'json' | 'markdown';
}

export function parseConfig(): Config {
  const program = new Command();

  program
    .name('wse-ts-bench')
    .description('WSE WebSocket Benchmark Suite (TypeScript)')
    .version('0.1.0')
    .option('--host <host>', 'Server host', '127.0.0.1')
    .option('--port <port>', 'Server port', '5006')
    .option('--secret <secret>', 'JWT secret', 'bench-secret-key-for-testing-only')
    .option('--test <test>', 'Test to run (or "all")', 'all')
    .option('--tiers <tiers>', 'Comma-separated connection tiers')
    .option('--duration <seconds>', 'Duration per tier in seconds', '10')
    .option('--batch-size <size>', 'Connection batch size', '500')
    .option('--max-connections <max>', 'Max connections for limit test', '100000')
    .option('--output <format>', 'Output format: text, json, markdown', 'text');

  program.parse();
  const opts = program.opts();

  return {
    host: opts.host,
    port: parseInt(opts.port, 10),
    secret: opts.secret,
    test: opts.test as TestName | 'all',
    tiers: opts.tiers ? opts.tiers.split(',').map((s: string) => parseInt(s.trim(), 10)) : null,
    duration: parseInt(opts.duration, 10),
    batchSize: parseInt(opts.batchSize, 10),
    maxConnections: parseInt(opts.maxConnections, 10),
    output: opts.output as 'text' | 'json' | 'markdown',
  };
}

export function tiersFor(config: Config, testName: TestName): number[] {
  if (config.tiers) return config.tiers;
  if (testName === 'connection-storm' || testName === 'connection-limit') {
    return EXTENDED_TIERS;
  }
  if (testName === 'format-comparison') {
    return [100, 1_000, 5_000, 10_000];
  }
  return DEFAULT_TIERS;
}
