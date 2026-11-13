// @vitest-environment happy-dom
//
// wireFormat 'msgpack': the client asks the server for msgpack, reads it, and
// sends its own messages in it; control frames stay JSON.

import { decode, encode } from '@msgpack/msgpack';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ENDPOINT,
  FakeWebSocket,
  connectServer,
  installFakeWebSocket,
  resetClientState,
} from './helpers.js';
import { acquireSession, getCurrentSession, makeSessionKey, wseActions } from '../services/WSESession.js';
import type { UseWSEConfig } from '../services/WSESession.js';

function start(config: UseWSEConfig = {}) {
  return acquireSession({
    key: makeSessionKey('cookie', [ENDPOINT]),
    auth: 'cookie',
    initialTopics: [],
    config: { endpoints: [ENDPOINT], offline: { enabled: false }, ...config },
  });
}

/** `value` as the server sends msgpack: `M:` and the encoded value. */
function packed(value: unknown): ArrayBuffer {
  const body = encode(value);
  const frame = new Uint8Array(2 + body.byteLength);
  frame.set([77, 58]);
  frame.set(body, 2);
  return frame.buffer;
}

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
});

afterEach(() => {
  resetClientState();
  vi.unstubAllGlobals();
});

describe('msgpack wire format', () => {
  it('asks for msgpack, reads it and sends in it', async () => {
    const ticks: number[] = [];
    start({
      wireFormat: 'msgpack',
      registerHandlers: (p) => p.registerHandler('tick', (m) => ticks.push(m.p.n)),
    });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    expect(new URL(ws.url).searchParams.get('format')).toBe('msgpack');

    await connectServer(ws);
    await vi.waitFor(() => expect(getCurrentSession()!.connection!.isTransportReady()).toBe(true));
    ws.serverSend(packed({ c: 'U', t: 'tick', p: { n: 7 }, v: 1 }));
    await vi.waitFor(() => expect(ticks).toEqual([7]));

    expect(wseActions.sendMessage('order', { id: 1, note: undefined })).toBe('sent');
    const frame = new Uint8Array(ws.binaryFrames().at(-1)!);
    expect(Array.from(frame.subarray(0, 2))).toEqual([77, 58]);
    const sent = decode(frame.subarray(2)) as any;
    expect(sent.t).toBe('order');
    expect(sent.p).toEqual({ id: 1 });

    // Control frames stay JSON text.
    expect(ws.messagesOfType('client_hello')).toHaveLength(1);
  });

  it('stays JSON by default', async () => {
    start();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    expect(new URL(ws.url).searchParams.has('format')).toBe(false);
    await connectServer(ws);
    await vi.waitFor(() => expect(getCurrentSession()!.connection!.isTransportReady()).toBe(true));
    expect(wseActions.sendMessage('order', { id: 1 })).toBe('sent');
    expect(ws.binaryFrames()).toEqual([]);
    expect(ws.messagesOfType('order')).toHaveLength(1);
  });
});
