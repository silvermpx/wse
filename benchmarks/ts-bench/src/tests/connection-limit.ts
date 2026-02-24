import type { Config } from '../config.js';
import type { TierResult } from '../report.js';
import * as protocol from '../protocol.js';
import { printHeader } from '../stats.js';
import { generateBenchToken } from '../jwt.js';

export async function run(config: Config): Promise<TierResult[]> {
  printHeader(
    'TEST 7: Connection Limit Finder',
    `Binary search for max stable connections (up to ${config.maxConnections}).`
  );

  const token = generateBenchToken(Buffer.from(config.secret), 'bench-user');
  const max = config.maxConnections;
  const results: TierResult[] = [];

  // Phase 1: Exponential probe
  console.log('\n  Phase 1: Exponential probe (doubling)');
  let current = 1_000;
  let lastSuccess = 0;
  let firstFailure = max;

  while (current <= max) {
    const { connected, errors, rate } = await probeConnections(config, token, current);
    const errorRate = errors / current;

    if (connected > 0 && errorRate < 0.05) {
      console.log(`    ${current} conns: OK (${errors} errors, ${Math.round(rate)} conn/s)`);
      lastSuccess = current;
      results.push(makeResult(current, connected, errors, rate));
      current *= 2;
    } else {
      console.log(
        `    ${current} conns: FAIL (${errors} errors = ${(errorRate * 100).toFixed(1)}%)`
      );
      firstFailure = current;
      results.push(makeResult(current, connected, errors, rate));
      break;
    }
  }

  if (lastSuccess === 0) {
    console.log('\n    Could not establish even 1000 connections.');
    return results;
  }

  if (firstFailure === max && lastSuccess >= max / 2) {
    console.log(`\n    All probes succeeded up to max (${max}).`);
    return results;
  }

  // Phase 2: Binary search
  console.log(`\n  Phase 2: Binary search [${lastSuccess} - ${firstFailure}]`);
  let lo = lastSuccess;
  let hi = firstFailure;

  while (hi - lo > 1_000) {
    let mid = Math.floor((lo + hi) / 2);
    mid = Math.floor(mid / 500) * 500;
    if (mid <= lo || mid >= hi) break;

    const { connected, errors, rate } = await probeConnections(config, token, mid);
    const errorRate = errors / mid;

    if (connected > 0 && errorRate < 0.05) {
      console.log(`    ${mid} conns: OK (${errors} errors, ${Math.round(rate)} conn/s)`);
      lo = mid;
      results.push(makeResult(mid, connected, errors, rate));
    } else {
      console.log(
        `    ${mid} conns: FAIL (${errors} errors = ${(errorRate * 100).toFixed(1)}%)`
      );
      hi = mid;
      results.push(makeResult(mid, connected, errors, rate));
    }

    // Cooldown
    await new Promise((r) => setTimeout(r, 2000));
  }

  console.log(`\n  Result: Max stable connections = ~${lo} (error boundary at ${hi})`);
  return results;
}

async function probeConnections(
  config: Config,
  token: string,
  n: number,
): Promise<{ connected: number; errors: number; rate: number }> {
  const start = process.hrtime.bigint();

  const connections = await protocol.connectBatch(
    config.host,
    config.port,
    token,
    n,
    config.batchSize,
    'compression=false&protocol_version=1'
  );

  const elapsed = Number(process.hrtime.bigint() - start) / 1_000_000_000;
  const connected = connections.length;
  const errors = n - connected;
  const rate = connected / elapsed;

  // Hold for stability check
  if (connected > 0) {
    await new Promise((r) => setTimeout(r, 3000));
  }

  await protocol.closeAll(connections);
  await new Promise((r) => setTimeout(r, 1000));

  return { connected, errors, rate };
}

function makeResult(tier: number, connected: number, errors: number, rate: number): TierResult {
  return {
    tier,
    connected,
    errors,
    durationSecs: 0,
    messagesSent: 0,
    messagesReceived: 0,
    bytesSent: 0,
    latency: null,
    extra: { accept_rate: rate },
  };
}
