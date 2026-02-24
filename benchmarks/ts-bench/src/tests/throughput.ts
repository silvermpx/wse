import type { Config } from '../config.js';
import type { TierResult } from '../report.js';
import * as protocol from '../protocol.js';
import { printHeader, fmtCount, fmtBytesPerSec } from '../stats.js';
import { generateBenchToken } from '../jwt.js';
import { tiersFor } from '../config.js';
import type WebSocket from 'ws';

export async function run(config: Config): Promise<TierResult[]> {
  printHeader(
    'TEST 3: Throughput Saturation',
    `All connections send as fast as possible for ${config.duration}s. Finds peak msg/s.`
  );

  const tiers = tiersFor(config, 'throughput');
  const token = generateBenchToken(Buffer.from(config.secret), 'bench-user');
  const payload = protocol.buildPayload(175);
  const msgBuf = Buffer.from(payload);
  const results: TierResult[] = [];

  for (const n of tiers) {
    console.log(`\n  --- ${n} connections ---`);

    const connections = await protocol.connectBatch(
      config.host, config.port, token, n, config.batchSize,
      'compression=false&protocol_version=1'
    );

    if (connections.length === 0) {
      console.log('    FAILED: no connections');
      continue;
    }

    const actual = connections.length;
    const durationMs = config.duration * 1000;

    const totalStart = process.hrtime.bigint();
    const deadline = Date.now() + durationMs;

    // Send as fast as possible on all connections
    const sendPromises = connections.map((ws) => sendLoop(ws, msgBuf, deadline));
    const sendResults = await Promise.allSettled(sendPromises);

    const totalSecs = Number(process.hrtime.bigint() - totalStart) / 1_000_000_000;

    let totalMessages = 0;
    let totalBytes = 0;
    const wsList: protocol.WsConnection[] = [];

    for (const r of sendResults) {
      if (r.status === 'fulfilled') {
        totalMessages += r.value.count;
        totalBytes += r.value.bytes;
        wsList.push(r.value.ws);
      }
    }

    const msgsPerSec = totalMessages / totalSecs;
    const bytesPerSec = totalBytes / totalSecs;
    const perConn = actual > 0 ? Math.round(msgsPerSec / actual) : 0;

    console.log(`    Duration: ${totalSecs.toFixed(2)}s`);
    console.log(
      `    Messages: ${totalMessages.toLocaleString()} (${fmtCount(Math.round(msgsPerSec))}/s)`
    );
    console.log(`    Bandwidth: ${fmtBytesPerSec(bytesPerSec)}`);
    console.log(`    Per-connection: ${perConn} msg/s`);

    results.push({
      tier: n,
      connected: actual,
      errors: n - actual,
      durationSecs: totalSecs,
      messagesSent: totalMessages,
      messagesReceived: 0,
      bytesSent: totalBytes,
      latency: null,
      extra: {
        msgs_per_sec: msgsPerSec,
        bytes_per_sec: bytesPerSec,
        per_conn_msgs: perConn,
      },
    });

    await protocol.closeAll(wsList);
    console.log();
  }

  // Print summary table
  if (results.length > 1) {
    console.log('  | Connections | Msg/s | MB/s | Per-conn msg/s | Errors |');
    console.log('  |-----------|-------|------|---------------|--------|');
    for (const r of results) {
      const rate = r.durationSecs > 0 ? r.messagesSent / r.durationSecs : 0;
      const mbps = r.durationSecs > 0 ? r.bytesSent / r.durationSecs / 1_000_000 : 0;
      const perConn = r.connected > 0 ? Math.round(rate / r.connected) : 0;
      const tierLabel = r.tier >= 1000 ? `${(r.tier / 1000).toFixed(0)}K` : `${r.tier}`;
      console.log(
        `  |  ${tierLabel.padEnd(8)} | ${fmtCount(Math.round(rate)).padEnd(8)} | ${mbps.toFixed(1).padEnd(6)} | ${String(perConn).padEnd(13)} | ${String(r.errors).padEnd(6)} |`
      );
    }
  }

  return results;
}

async function sendLoop(
  ws: protocol.WsConnection,
  msgBuf: Buffer,
  deadline: number,
): Promise<{ count: number; bytes: number; ws: protocol.WsConnection }> {
  let count = 0;
  let bytes = 0;
  const msgSize = msgBuf.length;

  return new Promise((resolve) => {
    function send() {
      while (Date.now() < deadline) {
        // Check backpressure — if buffered amount is too high, wait
        if ((ws as any).bufferedAmount > 16 * 1024 * 1024) {
          setTimeout(send, 1);
          return;
        }

        const ok = ws.send(msgBuf, { binary: false }, (err) => {
          if (err) {
            resolve({ count, bytes, ws });
          }
        });

        count++;
        bytes += msgSize;

        // Yield to event loop periodically
        if (count % 1000 === 0) {
          setImmediate(send);
          return;
        }
      }
      resolve({ count, bytes, ws });
    }

    send();
  });
}
