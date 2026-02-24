import type { Config } from '../config.js';
import type { TierResult } from '../report.js';
import * as protocol from '../protocol.js';
import { printHeader, fmtCount } from '../stats.js';
import { generateBenchToken } from '../jwt.js';
import { tiersFor } from '../config.js';
import { encode as msgpackEncode } from '@msgpack/msgpack';
import { deflateSync } from 'node:zlib';

export async function run(config: Config): Promise<TierResult[]> {
  printHeader(
    'TEST 5: Format Comparison (JSON vs MsgPack vs Compressed)',
    'Same payload, 3 wire formats. Measures serialization overhead.'
  );

  const tiers = tiersFor(config, 'format-comparison');
  const token = generateBenchToken(Buffer.from(config.secret), 'bench-user');
  const results: TierResult[] = [];

  // Build payloads in 3 formats
  const payload = {
    t: 'trade_update',
    p: {
      symbol: 'ES',
      side: 'buy',
      price: 5234.75,
      qty: 2,
      ts: '2026-02-23T14:30:00.123Z',
      account: 'acc_01HQ3',
      tags: ['momentum', 'breakout'],
      score: 0.87,
    },
  };

  const jsonBuf = Buffer.from(JSON.stringify(payload));
  const msgpackBuf = Buffer.concat([Buffer.from('M:'), Buffer.from(msgpackEncode(payload))]);
  const compressedBuf = Buffer.concat([Buffer.from('C:'), deflateSync(jsonBuf)]);

  console.log(`  Payload sizes:`);
  console.log(`    JSON:       ${jsonBuf.length} bytes`);
  console.log(`    MsgPack:    ${msgpackBuf.length} bytes (M: prefix + msgpack)`);
  console.log(`    Compressed: ${compressedBuf.length} bytes (C: prefix + zlib)`);

  for (const n of tiers) {
    console.log(`\n  === ${n} connections ===`);

    const formats: Array<{ name: string; buf: Buffer; binary: boolean }> = [
      { name: 'JSON', buf: jsonBuf, binary: false },
      { name: 'MsgPack', buf: msgpackBuf, binary: true },
      { name: 'Compressed', buf: compressedBuf, binary: true },
    ];

    const tierResults: Array<{ name: string; msgsPerSec: number }> = [];

    for (const fmt of formats) {
      process.stdout.write(`    ${fmt.name}:  `);

      const connections = await protocol.connectBatch(
        config.host, config.port, token, n, config.batchSize,
        'compression=false&protocol_version=1'
      );

      if (connections.length === 0) {
        console.log('FAILED');
        continue;
      }

      const durationMs = config.duration * 1000;
      const deadline = Date.now() + durationMs;
      const totalStart = process.hrtime.bigint();

      const sendPromises = connections.map((ws) =>
        formatSendLoop(ws, fmt.buf, fmt.binary, deadline)
      );
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
      tierResults.push({ name: fmt.name, msgsPerSec });
      console.log(`${fmtCount(Math.round(msgsPerSec))} msg/s`);

      results.push({
        tier: n,
        connected: connections.length,
        errors: n - connections.length,
        durationSecs: totalSecs,
        messagesSent: totalMessages,
        messagesReceived: 0,
        bytesSent: totalBytes,
        latency: null,
        extra: { format: fmt.name, msgs_per_sec: msgsPerSec },
      });

      await protocol.closeAll(wsList);
    }

    // Print comparison
    if (tierResults.length >= 2) {
      const jsonRate = tierResults.find((r) => r.name === 'JSON')?.msgsPerSec ?? 0;
      console.log(`\n    Summary @ ${n} connections:`);
      for (const r of tierResults) {
        const pct =
          jsonRate > 0 && r.name !== 'JSON'
            ? ` (${r.msgsPerSec > jsonRate ? '+' : ''}${(((r.msgsPerSec - jsonRate) / jsonRate) * 100).toFixed(1)}%)`
            : '';
        console.log(
          `      ${r.name}:${' '.repeat(12 - r.name.length)}${fmtCount(Math.round(r.msgsPerSec))} msg/s${pct}`
        );
      }
    }
  }

  return results;
}

async function formatSendLoop(
  ws: protocol.WsConnection,
  msgBuf: Buffer,
  binary: boolean,
  deadline: number,
): Promise<{ count: number; bytes: number; ws: protocol.WsConnection }> {
  let count = 0;
  let bytes = 0;
  const msgSize = msgBuf.length;

  return new Promise((resolve) => {
    let resolved = false;
    function done() {
      if (!resolved) { resolved = true; resolve({ count, bytes, ws }); }
    }
    function send() {
      while (Date.now() < deadline) {
        if ((ws as any).bufferedAmount > 16 * 1024 * 1024) {
          setTimeout(send, 1);
          return;
        }
        ws.send(msgBuf, { binary }, (err) => {
          if (err) done();
        });
        count++;
        bytes += msgSize;
        if (count % 1000 === 0) {
          setImmediate(send);
          return;
        }
      }
      done();
    }
    send();
  });
}
