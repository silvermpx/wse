// @vitest-environment happy-dom
//
// Work the client takes on has a bound in messages and bytes: inbound frames
// waiting to be processed (a browser WebSocket has no backpressure: past the
// bound the connection is dropped and recovered), and frames waiting to be
// encrypted (past the bound a send waits in the queue instead of growing
// the chain). Real timers: WebCrypto resolves outside the microtask queue.

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
import { acquireSession, wseActions, makeSessionKey } from '../services/WSESession.js';
import type { UseWSEConfig } from '../services/WSESession.js';
import { useMessageQueueStore } from '../stores/useMessageQueueStore.js';

function start(config: UseWSEConfig = {}, topics: string[] = ['prices']) {
  return acquireSession({
    key: makeSessionKey('cookie', [ENDPOINT]),
    auth: 'cookie',
    initialTopics: topics,
    config: { endpoints: [ENDPOINT], offline: { enabled: false }, ...config },
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

describe('inbound frames', () => {
  it('past the bound drop the connection, which recovers from where delivery is', async () => {
    const overloaded = recordEvents('wseOverloaded');
    start({ performance: { maxPendingInbound: 5 } });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    ws.serverOpen();
    ws.serverSend(serverReadyMessage());
    await new Promise((resolve) => setTimeout(resolve, 20));

    // Fifty frames at once: more wait than the bound allows.
    for (let n = 1; n <= 50; n++) ws.serverSend(stamped('prices', n));
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
    expect(overloaded).toHaveLength(1);
    expect(overloaded[0]).toMatchObject({ frames: 5 });

    const next = FakeWebSocket.last();
    next.serverOpen();
    next.serverSend(serverReadyMessage());
    // The old connection's frames still being processed do not count against the new one.
    expect(FakeWebSocket.instances).toHaveLength(2);
    await vi.waitFor(() => expect(next.messagesOfType('subscription_update')).toHaveLength(1));
    const resubscribe = next.messagesOfType('subscription_update')[0];
    expect(resubscribe.p.recover).toBe(true);
    // From the last message delivered: none past the five the client took.
    expect(resubscribe.p.recovery.prices.offset).toBeLessThanOrEqual(5);
    expect(overloaded).toHaveLength(1);
    expect(FakeWebSocket.instances).toHaveLength(2);
  });
});

describe('frames waiting to be encrypted', () => {
  it('past the bound wait in the queue, and all go out in order', async () => {
    start({
      security: { encryptionEnabled: true, legacyE2e: true },
      performance: { maxPendingEncrypted: 2 },
    });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    ws.serverOpen();
    ws.serverSend(serverReadyMessage());
    await vi.waitFor(() => expect(ws.messagesOfType('client_hello')).toHaveLength(1));
    const hello = ws.messagesOfType('client_hello')[0];
    const server = await serverKeyExchange(hello.p.encryption_public_key);
    ws.serverSend(
      serverHelloMessage({
        features: { compression: true, recovery: true, encryption: true },
        encryption_public_key: server.publicKeyB64,
      }),
    );
    await vi.waitFor(() => expect(ws.binaryFrames().length).toBeGreaterThanOrEqual(2));
    const before = ws.binaryFrames().length;

    for (let id = 1; id <= 20; id++) wseActions.sendMessage('order', { id });
    // Two are being encrypted; the rest wait in the queue, not in the chain.
    expect(useMessageQueueStore.getState().size).toBeGreaterThanOrEqual(17);

    await vi.waitFor(() => expect(ws.binaryFrames().length).toBe(before + 20), { timeout: 5_000 });
    const sent = await Promise.all(ws.binaryFrames().slice(before).map(async (f) => JSON.parse(await server.decrypt(f))));
    expect(sent.map((m) => m.p.id)).toEqual(Array.from({ length: 20 }, (_, i) => i + 1));
    expect(useMessageQueueStore.getState().size).toBe(0);
  });
});
