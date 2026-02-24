import type { Config } from '../config.js';
import type { TierResult } from '../report.js';
import * as protocol from '../protocol.js';
import { LatencyHistogram, printHeader } from '../stats.js';
import { generateBenchToken } from '../jwt.js';
import { tiersFor } from '../config.js';
import type WebSocket from 'ws';

const PINGS_PER_CONNECTION = 50;

export async function run(config: Config): Promise<TierResult[]> {
  printHeader(
    'TEST 2: Ping/Pong Latency Under Concurrent Load',
    `N persistent connections, each pinging ${PINGS_PER_CONNECTION}x. Measures tail latency.`
  );

  const tiers = tiersFor(config, 'ping-latency');
  const token = generateBenchToken(Buffer.from(config.secret), 'bench-user');
  const results: TierResult[] = [];

  for (const n of tiers) {
    console.log(`\n  --- ${n} concurrent connections ---`);

    const connections = await protocol.connectBatch(
      config.host, config.port, token, n, config.batchSize,
      'compression=false&protocol_version=1'
    );

    if (connections.length === 0) {
      console.log('    FAILED: no connections');
      continue;
    }

    const actual = connections.length;

    // Each connection pings concurrently
    const pingPromises = connections.map((ws) => pingLoop(ws));
    const pingResults = await Promise.allSettled(pingPromises);

    const merged = new LatencyHistogram();
    const wsList: protocol.WsConnection[] = [];

    for (const r of pingResults) {
      if (r.status === 'fulfilled') {
        merged.merge(r.value.hist);
        wsList.push(r.value.ws);
      }
    }

    console.log(`    Total pings: ${merged.len()}`);
    merged.printSummary('    ');

    results.push({
      tier: n,
      connected: actual,
      errors: n - actual,
      durationSecs: 0,
      messagesSent: merged.len(),
      messagesReceived: merged.len(),
      bytesSent: 0,
      latency: merged.isEmpty() ? null : merged.toSummary(),
      extra: {},
    });

    await protocol.closeAll(wsList);
    console.log();
  }

  return results;
}

async function pingLoop(
  ws: protocol.WsConnection
): Promise<{ hist: LatencyHistogram; ws: protocol.WsConnection }> {
  const hist = new LatencyHistogram();

  for (let i = 0; i < PINGS_PER_CONNECTION; i++) {
    const t0 = process.hrtime.bigint();
    const ping = JSON.stringify({
      t: 'ping',
      p: { client_timestamp: Date.now() },
    });

    try {
      await new Promise<void>((resolve, reject) => {
        ws.send(ping, (err) => {
          if (err) reject(err);
          else resolve();
        });
      });
    } catch {
      break;
    }

    // Wait for PONG
    const gotPong = await waitForPong(ws, 5000);
    if (gotPong) {
      const rttUs = Number(process.hrtime.bigint() - t0) / 1000;
      hist.recordUs(Math.max(Math.round(rttUs), 1));
    }
  }

  return { hist, ws };
}

function waitForPong(ws: protocol.WsConnection, timeoutMs: number): Promise<boolean> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      ws.removeListener('message', handler);
      resolve(false);
    }, timeoutMs);

    function handler(data: WebSocket.RawData) {
      const parsed = protocol.parseWseMessage(data);
      if (parsed && protocol.isPong(parsed)) {
        clearTimeout(timer);
        ws.removeListener('message', handler);
        resolve(true);
      }
    }

    ws.on('message', handler);
  });
}
