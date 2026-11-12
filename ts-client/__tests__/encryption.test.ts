// @vitest-environment happy-dom
//
// E2E encryption fails closed: nothing but the plaintext control frames
// (client_hello, ping, PONG) ever leaves the client before the key exists,
// held messages go out encrypted afterwards, and a server that does not
// confirm encryption gets the connection closed instead of plaintext.
// Real timers: WebCrypto resolves outside the microtask queue.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ENDPOINT,
  FakeWebSocket,
  installFakeWebSocket,
  resetClientState,
  serverHelloMessage,
  serverKeyExchange,
  serverReadyMessage,
  stamped,
} from './helpers.js';
import { acquireSession, getCurrentSession, makeSessionKey, wseActions } from '../services/WSESession.js';
import type { UseWSEConfig } from '../services/WSESession.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import { ConnectionState } from '../types.js';

function start(config: UseWSEConfig = {}, topics: string[] = ['orders']) {
  return acquireSession({
    key: makeSessionKey('cookie', [ENDPOINT]),
    auth: 'cookie',
    initialTopics: topics,
    config: {
      endpoints: [ENDPOINT],
      offline: { enabled: false },
      security: { encryptionEnabled: true },
      ...config,
    },
  });
}

let cleanups: Array<() => void> = [];

function recordEvents(name: string): any[] {
  const events: any[] = [];
  const listener = (e: Event) => events.push((e as CustomEvent).detail);
  window.addEventListener(name, listener);
  cleanups.push(() => window.removeEventListener(name, listener));
  return events;
}

async function orderIds(ws: FakeWebSocket, server: Awaited<ReturnType<typeof serverKeyExchange>>): Promise<number[]> {
  const msgs = await Promise.all(ws.binaryFrames().map(async (f) => JSON.parse(await server.decrypt(f))));
  return msgs.filter((m) => m.t === 'order').map((m) => m.p.id);
}

async function openAndHello(): Promise<{ ws: FakeWebSocket; hello: any }> {
  await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
  const ws = FakeWebSocket.last();
  ws.serverOpen();
  ws.serverSend(serverReadyMessage());
  await vi.waitFor(() => expect(ws.messagesOfType('client_hello')).toHaveLength(1));
  return { ws, hello: ws.messagesOfType('client_hello')[0] };
}

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
});

afterEach(() => {
  cleanups.forEach((fn) => fn());
  cleanups = [];
  resetClientState();
  vi.unstubAllGlobals();
});

describe('E2E encryption', () => {
  it('holds every application message until the key exists, then sends it encrypted', async () => {
    start();
    expect(wseActions.sendMessage('order', { id: 1 })).toBe('queued');

    const { ws, hello } = await openAndHello();
    expect(hello.p.features.encryption).toBe(true);
    expect(typeof hello.p.encryption_public_key).toBe('string');
    expect(wseActions.sendMessage('order', { id: 2 })).toBe('queued');

    // Not one plaintext application frame -- not even the resubscribe.
    expect(ws.appTextMessages()).toEqual([]);
    expect(ws.binaryFrames()).toEqual([]);

    const server = await serverKeyExchange(hello.p.encryption_public_key);
    ws.serverSend(serverHelloMessage({
      features: { compression: true, recovery: true, encryption: true },
      encryption_public_key: server.publicKeyB64,
    }));

    await vi.waitFor(() => expect(ws.binaryFrames().length).toBeGreaterThanOrEqual(4));
    const decrypted = await Promise.all(ws.binaryFrames().map(async (f) => JSON.parse(await server.decrypt(f))));
    expect(decrypted.map((m) => m.t)).toEqual(['subscription_update', 'sync_request', 'order', 'order']);
    expect(decrypted.filter((m) => m.t === 'order').map((m) => m.p.id)).toEqual([1, 2]);
    expect(decrypted[0].p.topics).toEqual(['orders']);

    expect(wseActions.sendMessage('order', { id: 3 })).toBe('sent');
    await vi.waitFor(() => expect(ws.binaryFrames()).toHaveLength(5));
    expect(ws.appTextMessages()).toEqual([]);
    expect(useWSEStore.getState().connectionState).toBe(ConnectionState.CONNECTED);
  });

  it('fails closed when server_hello does not confirm encryption', async () => {
    const failures = recordEvents('wseEncryptionFailed');
    start();
    wseActions.sendMessage('order', { id: 1 });

    const { ws } = await openAndHello();
    ws.serverSend(serverHelloMessage({ features: { encryption: false } }));

    await vi.waitFor(() => expect(useWSEStore.getState().connectionState).toBe(ConnectionState.ERROR));
    expect(failures).toHaveLength(1);
    expect(ws.closeCalls).toHaveLength(1);
    expect(ws.appTextMessages()).toEqual([]);
    expect(ws.binaryFrames()).toEqual([]);
    expect(useWSEStore.getState().lastError).toMatch(/Encryption failed/);

    // Fail closed means no automatic retry either.
    await new Promise((r) => setTimeout(r, 1_200));
    expect(FakeWebSocket.instances).toHaveLength(1);
  });

  it('fails closed on a KEY_EXCHANGE_FAILED error from the server', async () => {
    const failures = recordEvents('wseEncryptionFailed');
    start();
    const { ws } = await openAndHello();
    ws.serverSend({ c: 'WSE', t: 'error', p: { code: 'KEY_EXCHANGE_FAILED', message: 'bad key', recoverable: true } });

    await vi.waitFor(() => expect(failures).toHaveLength(1));
    expect(useWSEStore.getState().connectionState).toBe(ConnectionState.ERROR);
    expect(ws.appTextMessages()).toEqual([]);
  });

  it('decrypts frames that arrive before and during the key derivation, in order', async () => {
    const received: number[] = [];
    start({
      registerHandlers: (p) => p.registerHandler('price_update', (m) => received.push(m.o)),
    }, ['prices']);

    const { ws, hello } = await openAndHello();
    const server = await serverKeyExchange(hello.p.encryption_public_key);

    // The server encrypts fan-out as soon as it has processed client_hello;
    // frame 1 can overtake server_hello, frame 2 lands while the client derives.
    ws.serverSend(await server.encrypt(JSON.stringify(stamped('prices', 1))));
    ws.serverSend(serverHelloMessage({
      features: { encryption: true, recovery: true },
      encryption_public_key: server.publicKeyB64,
    }));
    ws.serverSend(await server.encrypt(JSON.stringify(stamped('prices', 2))));
    ws.serverSend(await server.encrypt(JSON.stringify(stamped('prices', 3))));

    await vi.waitFor(() => expect(received).toEqual([1, 2, 3]));
  });

  it('refuses a message marked encrypted on a session without encryption', async () => {
    acquireSession({
      key: makeSessionKey('cookie', [ENDPOINT]),
      auth: 'cookie',
      initialTopics: [],
      config: { endpoints: [ENDPOINT], offline: { enabled: false } },
    });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    ws.serverOpen();
    ws.serverSend(serverReadyMessage());
    await vi.waitFor(() => expect(ws.messagesOfType('client_hello')).toHaveLength(1));

    expect(wseActions.sendMessage('secret', { pin: 1234 }, { encrypted: true })).toBe('dropped');
    expect(wseActions.sendMessage('public', { ok: true })).toBe('sent');
    expect(ws.textMessages().map((m) => m.t)).not.toContain('secret');
  });

  it('never sends with the previous connection key after a reconnect', async () => {
    start({ reconnection: { baseDelay: 10, maxDelay: 20 } });
    const first = await openAndHello();
    const server1 = await serverKeyExchange(first.hello.p.encryption_public_key);
    first.ws.serverSend(serverHelloMessage({
      features: { encryption: true },
      encryption_public_key: server1.publicKeyB64,
    }));
    await vi.waitFor(() => expect(getCurrentSession()!.connection!.isTransportReady()).toBe(true));
    expect(wseActions.sendMessage('order', { id: 1 })).toBe('sent');
    await vi.waitFor(async () => expect(await orderIds(first.ws, server1)).toEqual([1]));

    first.ws.serverClose(1001);
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
    const ws2 = FakeWebSocket.last();
    ws2.serverOpen();
    ws2.serverSend(serverReadyMessage());
    expect(wseActions.sendMessage('order', { id: 2 })).toBe('queued');
    await vi.waitFor(() => expect(ws2.messagesOfType('client_hello')).toHaveLength(1));
    expect(ws2.binaryFrames()).toEqual([]);
    expect(ws2.appTextMessages()).toEqual([]);

    const hello2 = ws2.messagesOfType('client_hello')[0];
    expect(hello2.p.encryption_public_key).not.toBe(first.hello.p.encryption_public_key);
    const server2 = await serverKeyExchange(hello2.p.encryption_public_key);
    ws2.serverSend(serverHelloMessage({
      features: { encryption: true },
      encryption_public_key: server2.publicKeyB64,
    }));
    // Every frame on the new socket decrypts with the NEW key.
    await vi.waitFor(async () => expect(await orderIds(ws2, server2)).toEqual([2]));
  });

  it('re-sends, under the new key, a message whose socket closed while it was being encrypted', async () => {
    start({ reconnection: { baseDelay: 10, maxDelay: 20 } });
    const first = await openAndHello();
    const server1 = await serverKeyExchange(first.hello.p.encryption_public_key);
    first.ws.serverSend(serverHelloMessage({
      features: { encryption: true },
      encryption_public_key: server1.publicKeyB64,
    }));
    await vi.waitFor(() => expect(getCurrentSession()!.connection!.isTransportReady()).toBe(true));

    expect(wseActions.sendMessage('order', { id: 7 })).toBe('sent');
    first.ws.serverClose(1006); // before the async encryption finished

    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
    const ws2 = FakeWebSocket.last();
    ws2.serverOpen();
    ws2.serverSend(serverReadyMessage());
    await vi.waitFor(() => expect(ws2.messagesOfType('client_hello')).toHaveLength(1));
    const server2 = await serverKeyExchange(ws2.messagesOfType('client_hello')[0].p.encryption_public_key);
    ws2.serverSend(serverHelloMessage({
      features: { encryption: true },
      encryption_public_key: server2.publicKeyB64,
    }));

    await vi.waitFor(async () => expect(await orderIds(ws2, server2)).toEqual([7]));
    // Delivered once, on the new connection only.
    expect(await orderIds(first.ws, server1)).toEqual([]);
  });
});
