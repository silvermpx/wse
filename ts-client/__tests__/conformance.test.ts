// The TypeScript client against the conformance server (crates/wse-conformance),
// in Node. Runs when WSE_CONFORMANCE holds the server's control URL:
//
//   cargo run -p wse-conformance -- serve        # prints the control URL
//   WSE_CONFORMANCE=ws://127.0.0.1:P/control npx vitest run --project client ts-client/__tests__/conformance.test.ts
//
// With WSE_CONFORMANCE_BRIDGE it runs the cases as the window of a bridge:
// the owner holds the connection, and the client the cases drive is a
// window's. `1`: a TypeScript owner, over a MessageChannel (agent
// ts-bridge); `rust`: the conformance server's Rust owner, over a WebSocket,
// as a Tauri application's Rust side holds it (agent rust-bridge).

import { describe, expect, it } from 'vitest';
import { createWSEClient, type WSEClientOptions } from '../core/client.js';
import { createBridgeClient, createBridgeOwner, messagePortChannel, type WindowChannel } from '../bridge/index.js';
import { logger, LogLevel } from '../utils/logger.js';
import { generateSigningKey } from '../utils/signing.js';
import { runCases, type Asked, type Made } from './conformance/driver.js';

const CONTROL = process.env.WSE_CONFORMANCE;
const BRIDGE = process.env.WSE_CONFORMANCE_BRIDGE ?? '';
if (process.env.WSE_CONFORMANCE_REQUIRED === '1' && !CONTROL) {
  throw new Error('WSE_CONFORMANCE_REQUIRED is set, but WSE_CONFORMANCE is not');
}

/** A client's connection as the case asks for it, and its signing key's public key. */
async function connection(asked: Asked): Promise<{ options: WSEClientOptions; publicKey: string | null }> {
  const security: Record<string, unknown> = { encryptionEnabled: asked.e2eKey !== null };
  if (asked.e2eKey) security.serverKeys = [asked.e2eKey];
  let publicKey: string | null = null;
  if (asked.signing) {
    const signing = await generateSigningKey();
    publicKey = signing.publicKey;
    security.signingKey = signing.keyPair;
    security.serverSigningKeys = asked.serverSigningKey ? [asked.serverSigningKey] : [];
    // The application binds the key (bind_key): the token names none.
    security.bindSession = false;
  }
  return {
    publicKey,
    options: {
      endpoints: [asked.url],
      auth: 'token',
      // Node's WebSocket takes headers: the token goes as the bearer.
      webSocket: (url) => new WebSocket(url, { headers: { Authorization: `Bearer ${asked.token}` } } as any),
      reconnection: { baseDelay: 50, maxDelay: 300, jitter: false },
      wireFormat: asked.format,
      security: security as any,
    },
  };
}

/** A client of its own. */
async function own(asked: Asked): Promise<Made> {
  const { options, publicKey } = await connection(asked);
  return { client: createWSEClient({ ...options, ...asked.application }), publicKey };
}

/** A window of a TypeScript owner, over a MessageChannel. */
async function bridged(asked: Asked): Promise<Made> {
  const { options, publicKey } = await connection(asked);
  const owner = createBridgeOwner(options);
  const { port1, port2 } = new MessageChannel();
  owner.attach(messagePortChannel(port1 as never));
  const client = createBridgeClient(messagePortChannel(port2 as never), asked.application);
  return { client, publicKey, close: () => owner.destroy() };
}

/** A window's channel over a WebSocket: one frame a message, JSON text. */
function socketChannel(ws: WebSocket): WindowChannel {
  let closed = false;
  let sink: { frame(frame: any): void; close(): void } | null = null;
  ws.onmessage = (event) => {
    if (!closed) sink?.frame(JSON.parse(String(event.data)));
  };
  ws.onclose = () => {
    if (closed) return;
    closed = true;
    sink?.close();
  };
  return {
    post: (frame) => {
      if (!closed) ws.send(JSON.stringify(frame));
    },
    listen: (next) => {
      sink = next;
    },
    close: () => {
      if (closed) return;
      closed = true;
      ws.close();
    },
  };
}

/**
 * A window of the conformance server's Rust owner, which holds the
 * connection to the case's server as the case asks for it, its signing key
 * among it.
 */
async function rustOwner(asked: Asked): Promise<Made> {
  const ws = new WebSocket(CONTROL!.replace(/\/control$/, '/bridge'));
  await new Promise<void>((resolve, reject) => {
    ws.onopen = () => resolve();
    ws.onerror = () => reject(new Error('no bridge host beside the conformance server'));
  });
  const answer = new Promise<any>((resolve) => {
    ws.onmessage = (event) => resolve(JSON.parse(String(event.data)));
  });
  ws.send(
    JSON.stringify({
      url: asked.url,
      token: asked.token,
      format: asked.format,
      e2e_key: asked.e2eKey,
      signing: asked.signing,
      server_signing_key: asked.serverSigningKey,
    }),
  );
  const ready = await answer;
  if (!ready.ready) throw new Error(`the bridge host: ${ready.error}`);
  return { client: createBridgeClient(socketChannel(ws), asked.application), publicKey: ready.public_key ?? null };
}

describe.skipIf(!CONTROL)('conformance', () => {
  it('runs every case the TypeScript client can, and the server judges each', async () => {
    logger.setLevel(process.env.WSE_CONFORMANCE_DEBUG === '1' ? LogLevel.DEBUG : LogLevel.ERROR);
    const failed = await runCases({
      control: CONTROL!,
      agent: BRIDGE === 'rust' ? 'rust-bridge' : BRIDGE ? 'ts-bridge' : 'ts-core',
      runtime: `node ${process.versions.node}`,
      os: process.platform,
      arch: process.arch,
      make: BRIDGE === 'rust' ? rustOwner : BRIDGE ? bridged : own,
      bridged: BRIDGE !== '',
      // WSE_CONFORMANCE_CASES=a,b runs those alone.
      only: process.env.WSE_CONFORMANCE_CASES?.split(','),
      trace: (name, trace) => {
        if (process.env.WSE_CONFORMANCE_TRACE === '1') console.log(name, JSON.stringify(trace));
      },
    });
    expect(failed).toEqual([]);
  }, 600_000);
});
