// @vitest-environment happy-dom
//
// E2E encryption fails closed: nothing but the plaintext control frames
// (client_hello, ping, PONG) ever leaves the client before the key exists,
// held messages go out encrypted afterwards, and a server that does not
// confirm encryption gets the connection closed instead of plaintext.
// Sessions also check the server's identity and seal every frame, control
// frames included, in a strict order. Real timers: WebCrypto resolves outside
// the microtask queue.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { encode } from '@msgpack/msgpack';
import {
  ENDPOINT,
  FakeWebSocket,
  installFakeWebSocket,
  resetClientState,
  serverHelloMessage,
  serverKeyExchange,
  serverReadyMessage,
  sessionIdentity,
  sessionServer,
  stamped,
} from './helpers.js';
import type { SessionIdentity, SessionServer } from './helpers.js';
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
      security: { encryptionEnabled: true, legacyE2e: true },
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

describe('E2E encryption, first design', () => {
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

/** Everything the client sealed on `ws` so far, opened in wire order (call again for more). */
function sealedReader(ws: FakeWebSocket, server: SessionServer): () => Promise<any[]> {
  let next = 0;
  const messages: any[] = [];
  return async () => {
    const frames = ws.binaryFrames();
    while (next < frames.length) {
      const text = await server.open(frames[next++]);
      if (text !== null) messages.push(JSON.parse(text));
    }
    return messages;
  };
}

const transportReady = () => !!getCurrentSession()?.connection?.isTransportReady();

describe('E2E sessions', () => {
  let identity: SessionIdentity;
  const pinned = () => ({ encryptionEnabled: true, serverKeys: [identity.publicKeyB64] });

  beforeEach(async () => {
    identity = await sessionIdentity();
  });

  /** Answer client_hello like the server: server_hello with the signed e2e answer. */
  async function answer(ws: FakeWebSocket, hello: any, signer: SessionIdentity | null = identity, connectionId = 'conn-1') {
    const server = await sessionServer(hello.p.e2e, connectionId, signer);
    ws.serverSend(serverHelloMessage({
      features: { compression: true, recovery: true, encryption: true },
      connection_id: connectionId,
      e2e: server.hello,
    }));
    return server;
  }

  it('pins the server identity, then seals every frame both ways', async () => {
    const prices: number[] = [];
    start({
      security: pinned(),
      registerHandlers: (p) => p.registerHandler('price_update', (m) => prices.push(m.o)),
    }, ['prices']);
    expect(wseActions.sendMessage('order', { id: 1 })).toBe('queued');

    const { ws, hello } = await openAndHello();
    expect(hello.p.features.encryption).toBe(true);
    expect(hello.p.encryption_public_key).toBeUndefined();
    expect(hello.p.e2e.suites).toEqual(['p256-aes256gcm']);
    expect(ws.binaryFrames()).toEqual([]);

    const server = await answer(ws, hello);
    const sent = sealedReader(ws, server);
    await vi.waitFor(async () => {
      expect((await sent()).map((m) => m.t)).toEqual(['subscription_update', 'sync_request', 'order']);
    });

    for (const offset of [1, 2, 3]) {
      ws.serverSend(await server.seal(JSON.stringify(stamped('prices', offset))));
    }
    await vi.waitFor(() => expect(prices).toEqual([1, 2, 3]));

    // A sealed ping gets a sealed PONG.
    ws.serverSend(await server.seal(JSON.stringify({ c: 'WSE', t: 'ping', p: { server_time: 5 }, v: 1 })));
    await vi.waitFor(async () => expect((await sent()).map((m) => m.t)).toContain('PONG'));

    // Not one plaintext frame after client_hello.
    expect(ws.textMessages().map((m) => m.t)).toEqual(['client_hello']);
    expect(useWSEStore.getState().connectionState).toBe(ConnectionState.CONNECTED);
  });

  it('skips heartbeats while the handshake is in flight, then seals them', async () => {
    start({ security: pinned(), diagnostics: { healthCheckInterval: 1_000 } } as UseWSEConfig);
    const { ws, hello } = await openAndHello();

    // The server takes only sealed frames once it has answered client_hello.
    await new Promise((r) => setTimeout(r, 1_300));
    expect(ws.messagesOfType('ping')).toEqual([]);

    const server = await answer(ws, hello);
    const sent = sealedReader(ws, server);
    await vi.waitFor(async () => expect((await sent()).map((m) => m.t)).toContain('ping'), { timeout: 3_000 });
    expect(ws.messagesOfType('ping')).toEqual([]);
  });

  it('refuses a server signed by a key it does not pin', async () => {
    const failures = recordEvents('wseEncryptionFailed');
    start({ security: pinned() });
    wseActions.sendMessage('order', { id: 1 });

    const { ws, hello } = await openAndHello();
    await answer(ws, hello, await sessionIdentity());

    await vi.waitFor(() => expect(useWSEStore.getState().connectionState).toBe(ConnectionState.ERROR));
    expect(failures).toHaveLength(1);
    expect(ws.closeCalls).toHaveLength(1);
    expect(ws.binaryFrames()).toEqual([]);
    expect(ws.appTextMessages()).toEqual([]);

    await new Promise((r) => setTimeout(r, 1_200));
    expect(FakeWebSocket.instances).toHaveLength(1);
  });

  it('refuses an unsigned server when it pins a key', async () => {
    const failures = recordEvents('wseEncryptionFailed');
    start({ security: pinned() });
    const { ws, hello } = await openAndHello();
    await answer(ws, hello, null);

    await vi.waitFor(() => expect(failures).toHaveLength(1));
    expect(ws.binaryFrames()).toEqual([]);
  });

  it('takes an unsigned server only when told to', async () => {
    start({ security: { encryptionEnabled: true, unauthenticated: true } });
    const { ws, hello } = await openAndHello();
    const server = await answer(ws, hello, null);

    await vi.waitFor(() => expect(transportReady()).toBe(true));
    expect(wseActions.sendMessage('order', { id: 9 })).toBe('sent');
    const sent = sealedReader(ws, server);
    await vi.waitFor(async () => expect((await sent()).filter((m) => m.t === 'order').map((m) => m.p.id)).toEqual([9]));
  });

  it('asks for a pinned key, an unauthenticated server or the first design', async () => {
    start({ security: { encryptionEnabled: true } });
    await vi.waitFor(() => expect(useWSEStore.getState().connectionState).toBe(ConnectionState.ERROR));
    expect(useWSEStore.getState().lastError).toMatch(/serverKeys/);
    expect(FakeWebSocket.instances).toHaveLength(0);
  });

  it('ends the session on a replayed frame', async () => {
    const failures = recordEvents('wseEncryptionFailed');
    const prices: number[] = [];
    start({
      security: pinned(),
      registerHandlers: (p) => p.registerHandler('price_update', (m) => prices.push(m.o)),
    }, ['prices']);
    const { ws, hello } = await openAndHello();
    const server = await answer(ws, hello);
    await vi.waitFor(() => expect(transportReady()).toBe(true));

    const frame = await server.seal(JSON.stringify(stamped('prices', 1)));
    ws.serverSend(frame);
    ws.serverSend(frame.slice(0));

    await vi.waitFor(() => expect(failures).toHaveLength(1));
    expect(prices).toEqual([1]);
    expect(useWSEStore.getState().connectionState).toBe(ConnectionState.ERROR);
  });

  it('reads msgpack sealed in a frame', async () => {
    const ticks: number[] = [];
    start({
      security: pinned(),
      registerHandlers: (p) => p.registerHandler('tick', (m) => ticks.push(m.p.n)),
    });
    const { ws, hello } = await openAndHello();
    const server = await answer(ws, hello);
    await vi.waitFor(() => expect(transportReady()).toBe(true));

    const body = encode({ c: 'U', t: 'tick', p: { n: 7 }, v: 1 });
    const framed = new Uint8Array(new ArrayBuffer(2 + body.length));
    framed.set([77, 58]);
    framed.set(body, 2);
    ws.serverSend(await server.seal(framed));
    ws.serverSend(await server.seal(JSON.stringify({ c: 'U', t: 'tick', p: { n: 8 }, v: 1 })));

    await vi.waitFor(() => expect(ticks).toEqual([7, 8]));
  });

  it('still opens a sealed frame it drops for its size, so the session goes on', async () => {
    const ticks: number[] = [];
    start({
      security: pinned(),
      maxMessageSize: 4_096,
      registerHandlers: (p) => p.registerHandler('tick', (m) => ticks.push(m.p.n)),
    });
    const { ws, hello } = await openAndHello();
    const server = await answer(ws, hello);
    await vi.waitFor(() => expect(transportReady()).toBe(true));

    ws.serverSend(await server.seal(JSON.stringify({ c: 'U', t: 'tick', p: { n: 1, blob: 'x'.repeat(8_000) }, v: 1 })));
    ws.serverSend(await server.seal(JSON.stringify({ c: 'U', t: 'tick', p: { n: 2 }, v: 1 })));

    await vi.waitFor(() => expect(ticks).toEqual([2]));
    expect(useWSEStore.getState().metrics.messagesDropped).toBe(1);
    expect(useWSEStore.getState().connectionState).toBe(ConnectionState.CONNECTED);
  });

  it('starts a fresh session on every connection', async () => {
    start({ security: pinned(), reconnection: { baseDelay: 10, maxDelay: 20 } });
    const first = await openAndHello();
    const server1 = await answer(first.ws, first.hello);
    await vi.waitFor(() => expect(transportReady()).toBe(true));
    expect(wseActions.sendMessage('order', { id: 1 })).toBe('sent');
    const sent1 = sealedReader(first.ws, server1);
    await vi.waitFor(async () => expect((await sent1()).filter((m) => m.t === 'order').map((m) => m.p.id)).toEqual([1]));

    first.ws.serverClose(1001);
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
    const ws2 = FakeWebSocket.last();
    ws2.serverOpen();
    ws2.serverSend(serverReadyMessage({ connection_id: 'conn-2' }));
    expect(wseActions.sendMessage('order', { id: 2 })).toBe('queued');
    await vi.waitFor(() => expect(ws2.messagesOfType('client_hello')).toHaveLength(1));
    expect(ws2.binaryFrames()).toEqual([]);

    const hello2 = ws2.messagesOfType('client_hello')[0];
    expect(hello2.p.e2e.client_public_key).not.toBe(first.hello.p.e2e.client_public_key);
    const server2 = await answer(ws2, hello2, identity, 'conn-2');
    const sent2 = sealedReader(ws2, server2);
    await vi.waitFor(async () => expect((await sent2()).filter((m) => m.t === 'order').map((m) => m.p.id)).toEqual([2]));
  });
});
