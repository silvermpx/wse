import type { Config, TestName } from '../config.js';
import type { TierResult } from '../report.js';
import type { TestReport } from '../report.js';

import * as connectionStorm from './connection-storm.js';
import * as pingLatency from './ping-latency.js';
import * as throughput from './throughput.js';
import * as sizeMatrix from './size-matrix.js';
import * as formatComparison from './format-comparison.js';
import * as sustainedHold from './sustained-hold.js';
import * as connectionLimit from './connection-limit.js';

type TestRunner = (config: Config) => Promise<TierResult[]>;

const TEST_MAP: Record<TestName, { name: string; runner: TestRunner }> = {
  'connection-storm': { name: 'Connection Storm', runner: connectionStorm.run },
  'ping-latency': { name: 'Ping/Pong Latency', runner: pingLatency.run },
  'throughput': { name: 'Throughput Saturation', runner: throughput.run },
  'size-matrix': { name: 'Size Matrix', runner: sizeMatrix.run },
  'format-comparison': { name: 'Format Comparison', runner: formatComparison.run },
  'sustained-hold': { name: 'Sustained Hold', runner: sustainedHold.run },
  'connection-limit': { name: 'Connection Limit', runner: connectionLimit.run },
};

export async function runTests(config: Config): Promise<TestReport[]> {
  const reports: TestReport[] = [];

  const testsToRun: TestName[] =
    config.test === 'all'
      ? (Object.keys(TEST_MAP) as TestName[])
      : [config.test];

  for (const testName of testsToRun) {
    const test = TEST_MAP[testName];
    if (!test) {
      console.error(`Unknown test: ${testName}`);
      continue;
    }

    const results = await test.runner(config);
    reports.push({ testName: test.name, results });
  }

  return reports;
}
