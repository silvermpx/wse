import WebSocket from 'ws';
import { inflateSync } from 'node:zlib';
import { decode } from '@msgpack/msgpack';
import http from 'node:http';

export type WsConnection = WebSocket;

export async function connectAndHandshake(
  host: string,
  port: number,
  token: string,
  uriParams: string,
  timeoutSecs: number,
): Promise<WsConnection> {
  return new Promise<WsConnection>((resolve, reject) => {
    const url = `ws://${host}:${port}/wse?${uriParams}`;
    const ws = new WebSocket(url, {
      headers: { Cookie: `access_token=${token}` },
      handshakeTimeout: timeoutSecs * 1000,
    });

    const timer = setTimeout(() => {
      ws.terminate();
      reject(new Error('handshake timeout'));
    }, timeoutSecs * 1000);

    let gotServerReady = false;

    ws.on('message', (data: WebSocket.RawData) => {
      const parsed = parseWseMessage(data);
      if (!parsed) return;
      if (!gotServerReady && parsed.t === 'server_ready') {
        gotServerReady = true;
        const hello = JSON.stringify({
          t: 'client_hello',
          p: { client_version: 'wse-ts-bench/0.1.0', protocol_version: 1 },
        });
        ws.send(hello);
        clearTimeout(timer);
        resolve(ws);
      }
    });

    ws.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

export async function connectAndHandshakeFrom(
  host: string,
  port: number,
  token: string,
  uriParams: string,
  sourceAddr: string,
  timeoutSecs: number,
): Promise<WsConnection> {
  const agent = new http.Agent({ localAddress: sourceAddr });
  return new Promise<WsConnection>((resolve, reject) => {
    const url = `ws://${host}:${port}/wse?${uriParams}`;
    const ws = new WebSocket(url, {
      headers: { Cookie: `access_token=${token}` },
      handshakeTimeout: timeoutSecs * 1000,
      agent,
    });

    const timer = setTimeout(() => {
      ws.terminate();
      reject(new Error('handshake timeout'));
    }, timeoutSecs * 1000);

    let gotServerReady = false;

    ws.on('message', (data: WebSocket.RawData) => {
      const parsed = parseWseMessage(data);
      if (!parsed) return;
      if (!gotServerReady && parsed.t === 'server_ready') {
        gotServerReady = true;
        const hello = JSON.stringify({
          t: 'client_hello',
          p: { client_version: 'wse-ts-bench/0.1.0', protocol_version: 1 },
        });
        ws.send(hello);
        clearTimeout(timer);
        resolve(ws);
      }
    });

    ws.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

const PORTS_PER_IP = 60_000;

export async function connectBatch(
  host: string,
  port: number,
  token: string,
  n: number,
  batchSize: number,
  uriParams: string,
): Promise<WsConnection[]> {
  const useMultiIp = n > PORTS_PER_IP && host === '127.0.0.1';
  const connections: WsConnection[] = [];
  let errors = 0;
  let connIdx = 0;

  for (let start = 0; start < n; start += batchSize) {
    const end = Math.min(start + batchSize, n);
    const chunk = end - start;

    const promises: Promise<WsConnection>[] = [];
    for (let i = 0; i < chunk; i++) {
      if (useMultiIp) {
        const ipIdx = Math.floor(connIdx / PORTS_PER_IP) + 1;
        if (ipIdx > 254) throw new Error('exceeded 127.0.0.254');
        const sourceAddr = `127.0.0.${ipIdx}`;
        connIdx++;
        promises.push(
          connectAndHandshakeFrom(host, port, token, uriParams, sourceAddr, 15)
        );
      } else {
        connIdx++;
        promises.push(
          connectAndHandshake(host, port, token, uriParams, 15)
        );
      }
    }

    const results = await Promise.allSettled(promises);
    for (const r of results) {
      if (r.status === 'fulfilled') {
        connections.push(r.value);
      } else {
        errors++;
      }
    }

    process.stderr.write(`\r    Connecting: ${connections.length}/${n}...`);

    if (end < n) {
      await sleep(20);
    }
  }

  if (useMultiIp) {
    const ipsUsed = Math.floor(n / PORTS_PER_IP) + 1;
    process.stderr.write(
      `\r    Connected: ${connections.length}/${n} (${errors} failed, ${ipsUsed} source IPs)            \n`
    );
  } else {
    process.stderr.write(
      `\r    Connected: ${connections.length}/${n} (${errors} failed)            \n`
    );
  }

  return connections;
}

export function parseWseMessage(data: WebSocket.RawData): Record<string, unknown> | null {
  if (typeof data === 'string') {
    return parseTextFrame(data);
  }
  if (data instanceof Buffer) {
    return parseBinaryFrame(data);
  }
  if (data instanceof ArrayBuffer) {
    return parseBinaryFrame(Buffer.from(data));
  }
  if (Array.isArray(data)) {
    return parseBinaryFrame(Buffer.concat(data));
  }
  return null;
}

function parseTextFrame(text: string): Record<string, unknown> | null {
  const stripped = stripPrefix(text);
  try {
    return JSON.parse(stripped);
  } catch {
    return null;
  }
}

function parseBinaryFrame(data: Buffer): Record<string, unknown> | null {
  if (data.length >= 2 && data[0] === 0x43 && data[1] === 0x3a) {
    // C: zlib compressed
    try {
      const decompressed = inflateSync(data.subarray(2)).toString('utf8');
      return parseTextFrame(decompressed);
    } catch {
      return null;
    }
  }
  if (data.length >= 2 && data[0] === 0x4d && data[1] === 0x3a) {
    // M: msgpack
    try {
      return decode(data.subarray(2)) as Record<string, unknown>;
    } catch {
      return null;
    }
  }
  if (data.length >= 2 && data[0] === 0x45 && data[1] === 0x3a) {
    // E: encrypted — cannot decode without session key
    return null;
  }
  if (data.length > 0 && data[0] === 0x78) {
    // Raw zlib (magic byte)
    try {
      const decompressed = inflateSync(data).toString('utf8');
      return parseTextFrame(decompressed);
    } catch {
      return null;
    }
  }
  // Try as plain text in binary frame
  try {
    return parseTextFrame(data.toString('utf8'));
  } catch {
    return null;
  }
}

export function stripPrefix(text: string): string {
  if (text.startsWith('WSE{') || text.startsWith('WSE[')) return text.slice(3);
  if (text.startsWith('S{') || text.startsWith('S[')) return text.slice(1);
  if (text.startsWith('U{') || text.startsWith('U[')) return text.slice(1);
  return text;
}

export function isPong(msg: Record<string, unknown>): boolean {
  const t = msg.t;
  if (typeof t !== 'string') return false;
  return t.toLowerCase() === 'pong';
}

export function buildPing(): string {
  return JSON.stringify({
    t: 'ping',
    p: { client_timestamp: Date.now() },
  });
}

export function buildPayload(size: number): string {
  if (size <= 64) {
    return JSON.stringify({
      t: 'trade_update',
      p: { s: 'ES', px: 5234.75, q: 2 },
    });
  }
  const base = JSON.stringify({
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
  });
  if (base.length + 20 >= size) return base;
  const paddingNeeded = size - base.length - 20;
  const padding = 'x'.repeat(paddingNeeded);
  return JSON.stringify({
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
      data: padding,
    },
  });
}

export async function closeAll(connections: WsConnection[]): Promise<void> {
  const promises = connections.map(
    (ws) =>
      new Promise<void>((resolve) => {
        if (ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING) {
          resolve();
          return;
        }
        ws.on('close', () => resolve());
        ws.close();
        setTimeout(() => resolve(), 2000);
      })
  );
  await Promise.allSettled(promises);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
