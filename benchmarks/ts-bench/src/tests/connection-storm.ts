import type { Config } from '../config.js';
import type { TierResult } from '../report.js';
import * as protocol from '../protocol.js';
import { LatencyHistogram, printHeader } from '../stats.js';
import { generateBenchToken } from '../jwt.js';
import { tiersFor } from '../config.js';

export async function run(config: Config): Promise<TierResult[]> {
  printHeader(
    'TEST 1: Connection Storm',
    'All connections opened as fast as possible. Measures accept rate + handshake latency.'
  );

  const tiers = tiersFor(config, 'connection-storm');
  const token = generateBenchToken(Buffer.from(config.secret), 'bench-user');
  const results: TierResult[] = [];

  for (const n of tiers) {
    console.log(`\n  --- ${n} connections ---`);

    let errors = 0;
    const allLatencies: number[] = [];
    const connections: protocol.WsConnection[] = [];
    const totalStart = process.hrtime.bigint();

    const useMultiIp = n > 60_000 && config.host === '127.0.0.1';
    let connIdx = 0;

    for (let start = 0; start < n; start += config.batchSize) {
      const end = Math.min(start + config.batchSize, n);
      const chunk = end - start;

      const promises: Promise<{ ws: protocol.WsConnection; latMs: number }>[] = [];

      for (let i = 0; i < chunk; i++) {
        const t0 = process.hrtime.bigint();

        let connectPromise: Promise<protocol.WsConnection>;
        if (useMultiIp) {
          const ipIdx = Math.floor(connIdx / 60_000) + 1;
          if (ipIdx > 254) throw new Error('exceeded 127.0.0.254');
          connIdx++;
          connectPromise = protocol.connectAndHandshakeFrom(
            config.host, config.port, token,
            'compression=false&protocol_version=1',
            `127.0.0.${ipIdx}`, 15
          );
        } else {
          connIdx++;
          connectPromise = protocol.connectAndHandshake(
            config.host, config.port, token,
            'compression=false&protocol_version=1', 15
          );
        }

        promises.push(
          connectPromise.then((ws) => {
            const latMs = Number(process.hrtime.bigint() - t0) / 1_000_000;
            return { ws, latMs };
          })
        );
      }

      const settled = await Promise.allSettled(promises);
      for (const r of settled) {
        if (r.status === 'fulfilled') {
          connections.push(r.value.ws);
          allLatencies.push(r.value.latMs);
        } else {
          errors++;
        }
      }

      process.stderr.write(`\r    Progress: ${connections.length}/${n}...`);

      if (end < n) {
        await new Promise((r) => setTimeout(r, 10));
      }
    }

    const totalSecs = Number(process.hrtime.bigint() - totalStart) / 1_000_000_000;

    process.stderr.write(
      `\r    Connected: ${connections.length}/${n} (${errors} failed)            \n`
    );
    console.log(`    Total time: ${totalSecs.toFixed(2)}s`);

    const hist = new LatencyHistogram();
    for (const lat of allLatencies) {
      hist.recordMs(lat);
    }

    if (allLatencies.length > 0) {
      const rate = allLatencies.length / totalSecs;
      console.log(`    Accept rate: ${Math.round(rate)} conn/s`);
      hist.printSummary('    ');
    }

    results.push({
      tier: n,
      connected: connections.length,
      errors,
      durationSecs: totalSecs,
      messagesSent: 0,
      messagesReceived: 0,
      bytesSent: 0,
      latency: hist.isEmpty() ? null : hist.toSummary(),
      extra: {
        accept_rate: totalSecs > 0 ? allLatencies.length / totalSecs : 0,
      },
    });

    await protocol.closeAll(connections);
    console.log();
  }

  return results;
}
