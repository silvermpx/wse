import type { Config } from '../config.js';
import type { TierResult } from '../report.js';
import * as protocol from '../protocol.js';
import { LatencyHistogram, printHeader } from '../stats.js';
import { generateBenchToken } from '../jwt.js';
import { tiersFor } from '../config.js';
import type WebSocket from 'ws';

const PING_INTERVAL_SECS = 5;

export async function run(config: Config): Promise<TierResult[]> {
  const holdSecs = Math.max(config.duration, 30);

  printHeader(
    'TEST 6: Sustained Hold',
    `Hold N connections for ${holdSecs}s with periodic PING/PONG. Monitor stability.`
  );

  const tiers = tiersFor(config, 'sustained-hold');
  const token = generateBenchToken(Buffer.from(config.secret), 'bench-user');
  const results: TierResult[] = [];

  for (const n of tiers) {
    console.log(`\n  --- ${n} connections for ${holdSecs}s ---`);

    const connections = await protocol.connectBatch(
      config.host, config.port, token, n, config.batchSize,
      'compression=false&protocol_version=1'
    );

    if (connections.length === 0) {
      console.log('    FAILED: no connections');
      continue;
    }

    const actual = connections.length;
    let disconnected = 0;
    const totalStart = process.hrtime.bigint();

    // Run hold loops concurrently
    const holdPromises = connections.map((ws) =>
      holdLoop(ws, holdSecs, () => { disconnected++; })
    );

    // Progress reporter
    const progressTimer = setInterval(() => {
      const elapsed = Number(process.hrtime.bigint() - totalStart) / 1_000_000_000;
      process.stderr.write(
        `\r    Elapsed: ${Math.round(elapsed)}s / ${holdSecs}s | Disconnected: ${disconnected}`
      );
    }, 1000);

    const holdResults = await Promise.allSettled(holdPromises);
    clearInterval(progressTimer);
    process.stderr.write('\n');

    const merged = new LatencyHistogram();
    const wsList: protocol.WsConnection[] = [];
    let totalPings = 0;

    for (const r of holdResults) {
      if (r.status === 'fulfilled') {
        merged.merge(r.value.hist);
        totalPings += r.value.pingCount;
        wsList.push(r.value.ws);
      }
    }

    const elapsed = Number(process.hrtime.bigint() - totalStart) / 1_000_000_000;
    const survivalRate = ((actual - disconnected) / actual) * 100;

    console.log(`    Survival: ${actual - disconnected}/${actual} (${survivalRate.toFixed(1)}%)`);
    console.log(`    Total PINGs: ${totalPings}`);
    if (!merged.isEmpty()) {
      console.log('    PING/PONG latency:');
      merged.printSummary('      ');
    }

    results.push({
      tier: n,
      connected: actual,
      errors: disconnected,
      durationSecs: elapsed,
      messagesSent: totalPings,
      messagesReceived: Math.max(0, totalPings - disconnected),
      bytesSent: 0,
      latency: merged.isEmpty() ? null : merged.toSummary(),
      extra: {
        survival_rate_pct: survivalRate,
        disconnected,
        hold_seconds: holdSecs,
      },
    });

    await protocol.closeAll(wsList);
    console.log();
  }

  return results;
}

async function holdLoop(
  ws: protocol.WsConnection,
  holdSecs: number,
  onDisconnect: () => void,
): Promise<{ hist: LatencyHistogram; ws: protocol.WsConnection; pingCount: number }> {
  const hist = new LatencyHistogram();
  let pingCount = 0;
  const deadline = Date.now() + holdSecs * 1000;

  return new Promise((resolve) => {
    let dead = false;

    ws.on('close', () => {
      if (!dead) {
        dead = true;
        onDisconnect();
        resolve({ hist, ws, pingCount });
      }
    });

    ws.on('error', () => {
      if (!dead) {
        dead = true;
        onDisconnect();
        resolve({ hist, ws, pingCount });
      }
    });

    const interval = setInterval(async () => {
      if (dead || Date.now() >= deadline) {
        clearInterval(interval);
        if (!dead) resolve({ hist, ws, pingCount });
        return;
      }

      const t0 = process.hrtime.bigint();
      const ping = JSON.stringify({
        t: 'ping',
        p: { client_timestamp: Date.now() },
      });

      try {
        await new Promise<void>((res, rej) => {
          ws.send(ping, (err) => (err ? rej(err) : res()));
        });
        pingCount++;
      } catch {
        clearInterval(interval);
        if (!dead) {
          dead = true;
          onDisconnect();
          resolve({ hist, ws, pingCount });
        }
        return;
      }

      // Wait for pong with timeout
      const gotPong = await waitForPongHold(ws, 5000);
      if (gotPong) {
        const rttUs = Number(process.hrtime.bigint() - t0) / 1000;
        hist.recordUs(Math.max(Math.round(rttUs), 1));
      }
    }, PING_INTERVAL_SECS * 1000);
  });
}

function waitForPongHold(ws: protocol.WsConnection, timeoutMs: number): Promise<boolean> {
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
