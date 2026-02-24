import type { Config } from '../config.js';
import type { TierResult } from '../report.js';
import * as protocol from '../protocol.js';
import { printHeader, fmtCount, fmtBytesPerSec } from '../stats.js';
import { generateBenchToken } from '../jwt.js';
import { tiersFor } from '../config.js';

const PAYLOAD_SIZES = [64, 256, 1024, 4096, 16384, 65536];
const CELL_DURATION_SECS = 5;

export async function run(config: Config): Promise<TierResult[]> {
  printHeader(
    'TEST 4: Payload Size x Connections Matrix',
    `${PAYLOAD_SIZES.length} sizes x N tiers, ${CELL_DURATION_SECS}s per cell.`
  );

  const tiers = tiersFor(config, 'size-matrix');
  const token = generateBenchToken(Buffer.from(config.secret), 'bench-user');
  const results: TierResult[] = [];

  // Build payloads
  const payloads = PAYLOAD_SIZES.map((size) => {
    const payload = protocol.buildPayload(size);
    return { size, payload, buf: Buffer.from(payload) };
  });

  // Print header
  const tierLabels = tiers.map((t) => (t >= 1000 ? `${(t / 1000).toFixed(0)}K` : `${t}`));
  console.log(`\n  ### Messages per second\n`);
  console.log(`  | Size \\ Conns | ${tierLabels.join(' | ')} |`);
  console.log(`  |${'-'.repeat(13)}|${tierLabels.map(() => '------').join('|')}|`);

  for (const { size, buf } of payloads) {
    const sizeLabel =
      size >= 1024 ? `${(size / 1024).toFixed(0)} KB` : `${size} B`;
    const cells: string[] = [];

    for (const n of tiers) {
      const connections = await protocol.connectBatch(
        config.host, config.port, token, n, config.batchSize,
        'compression=false&protocol_version=1'
      );

      if (connections.length === 0) {
        cells.push('-');
        continue;
      }

      const actual = connections.length;
      const deadline = Date.now() + CELL_DURATION_SECS * 1000;
      const totalStart = process.hrtime.bigint();

      const sendPromises = connections.map((ws) => matrixSendLoop(ws, buf, deadline));
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
      cells.push(fmtCount(Math.round(msgsPerSec)));

      results.push({
        tier: n,
        connected: actual,
        errors: n - actual,
        durationSecs: totalSecs,
        messagesSent: totalMessages,
        messagesReceived: 0,
        bytesSent: totalBytes,
        latency: null,
        extra: { payload_size: size, msgs_per_sec: msgsPerSec },
      });

      await protocol.closeAll(wsList);
    }

    console.log(`  | **${sizeLabel}** | ${cells.join(' | ')} |`);
  }

  return results;
}

async function matrixSendLoop(
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
        if ((ws as any).bufferedAmount > 16 * 1024 * 1024) {
          setTimeout(send, 1);
          return;
        }
        ws.send(msgBuf, { binary: false }, (err) => {
          if (err) resolve({ count, bytes, ws });
        });
        count++;
        bytes += msgSize;
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
