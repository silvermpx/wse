// @vitest-environment happy-dom
//
// The framework-free client: clients of their own share nothing with each
// other or with the page's default client, hold one connection across a
// remount, and end for good on signOut.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  FakeWebSocket,
  connectServer,
  installFakeWebSocket,
  resetClientState,
  serverReadyMessage,
  stamped,
  tick,
} from './helpers.js';
import { createWSEClient, type WSEClient } from '../core/index.js';
import { useWSEStore } from '../stores/useWSEStore.js';

const A = 'ws://a.test/wse';
const B = 'ws://b.test/wse';

let clients: WSEClient[] = [];

function client(endpoint: string, options: Parameters<typeof createWSEClient>[0] = {}): WSEClient {
  const made = createWSEClient({ endpoints: [endpoint], offline: { enabled: false }, ...options });
  clients.push(made);
  return made;
}

const socketOf = (endpoint: string) => FakeWebSocket.instances.filter((ws) => ws.url.startsWith(endpoint));

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
  vi.useFakeTimers();
});

afterEach(() => {
  for (const made of clients) made.destroy();
  clients = [];
  resetClientState();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('a client of its own', () => {
  it('is inert until a share is taken', async () => {
    const c = client(A);
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(0);
    expect(c.state.getSnapshot().connection).toBe('pending');
    c.retain();
    await tick();
    expect(socketOf(A)).toHaveLength(1);
  });

  it('shares nothing with another client or with the default one', async () => {
    const a = client(A, { topics: ['prices'] });
    const b = client(B, { topics: ['news'] });
    const heardA: unknown[] = [];
    const heardB: unknown[] = [];
    a.onMessage('price_update', (m) => heardA.push(m.p));
    b.onMessage('price_update', (m) => heardB.push(m.p));
    const windowEvents = vi.fn();
    window.addEventListener('wseGap', windowEvents);
    a.retain();
    b.retain();
    await tick();
    const [wsA] = socketOf(A);
    const [wsB] = socketOf(B);
    await connectServer(wsA, { user_id: 'alice' });
    await connectServer(wsB, { user_id: 'bob' });

    expect(wsA.messagesOfType('subscription_update')[0].p.topics).toEqual(['prices']);
    expect(wsB.messagesOfType('subscription_update')[0].p.topics).toEqual(['news']);
    wsA.serverSend(stamped('prices', 1));
    wsA.serverSend(stamped('prices', 3)); // a gap: the event is a's alone
    await vi.advanceTimersByTimeAsync(5_000);
    expect(heardA).toHaveLength(2);
    expect(heardB).toHaveLength(0);
    expect(windowEvents).not.toHaveBeenCalled();
    expect(a.state.getSnapshot().ready).toBe(true);
    expect(b.state.getSnapshot().topics).toEqual(['news']);
    // The page's default client saw none of it.
    expect(useWSEStore.getState().activeTopics).toEqual([]);
    expect(useWSEStore.getState().metrics.messagesReceived).toBe(0);

    a.signOut();
    expect(wsB.closeCalls).toHaveLength(0);
    expect(b.state.getSnapshot().ready).toBe(true);
    window.removeEventListener('wseGap', windowEvents);
  });

  it('holds its queue limits before it starts', () => {
    // The envelope (id, timestamp) takes some 150 bytes of its own.
    const c = client(A, { performance: { memoryLimit: 400 } });
    expect(c.send('big', { text: 'x'.repeat(1000) })).toBe('dropped');
    expect(c.send('small', { n: 1 })).toBe('queued');
    const counted = client(B, { performance: { maxQueueSize: 1 } });
    expect(counted.send('one', {})).toBe('queued');
    expect(counted.send('two', {})).toBe('dropped');
  });

  it('is ready once the server took the connection, not when the socket opens', async () => {
    const c = client(A);
    c.retain();
    await tick();
    const ws = socketOf(A)[0];
    ws.serverOpen();
    await tick();
    expect(c.state.getSnapshot().ready).toBe(false);
    ws.serverSend(serverReadyMessage());
    await tick();
    expect(c.state.getSnapshot().ready).toBe(true);
    ws.serverClose(1006);
    await tick();
    expect(c.state.getSnapshot().ready).toBe(false);
  });

  it('keeps one connection when its last share is released and taken again at once', async () => {
    const c = client(A);
    const first = c.retain();
    await tick();
    first.release();
    const second = c.retain();
    await vi.advanceTimersByTimeAsync(10);
    expect(socketOf(A)).toHaveLength(1);
    second.release();
    second.release(); // once is enough
    await vi.advanceTimersByTimeAsync(10);
    expect(socketOf(A)[0].closeCalls.length).toBeGreaterThan(0);
  });

  it('holds topics while any lease of them is held', async () => {
    const c = client(A);
    c.retain();
    await tick();
    const ws = socketOf(A)[0];
    await connectServer(ws);
    const one = c.subscribe(['prices']);
    const two = c.subscribe(['prices']);
    const subscribes = () => ws.messagesOfType('subscription_update').filter((m) => m.p.action === 'subscribe');
    const unsubscribes = () => ws.messagesOfType('subscription_update').filter((m) => m.p.action === 'unsubscribe');
    await tick();
    expect(subscribes()).toHaveLength(1);
    one.release();
    await tick();
    expect(unsubscribes()).toHaveLength(0);
    two.release();
    await tick();
    expect(unsubscribes().map((m) => m.p.topics)).toEqual([['prices']]);
  });

  it('gives a message to every listener of its type', async () => {
    const c = client(A);
    const first = vi.fn();
    const second = vi.fn();
    const stopFirst = c.onMessage('price_update', first);
    c.onMessage('price_update', second);
    c.retain();
    await tick();
    const ws = socketOf(A)[0];
    await connectServer(ws);
    ws.serverSend({ c: 'U', t: 'price_update', p: { n: 1 }, v: 1 });
    await tick();
    stopFirst();
    ws.serverSend({ c: 'U', t: 'price_update', p: { n: 2 }, v: 1 });
    await tick();
    expect(first).toHaveBeenCalledTimes(1);
    expect(second).toHaveBeenCalledTimes(2);
  });

  it('keeps what is sent before it connects, and sends it once the server takes it', async () => {
    const c = client(A);
    expect(c.send('order', { qty: 1 })).toBe('queued');
    c.retain();
    await tick();
    const ws = socketOf(A)[0];
    await connectServer(ws);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(ws.messagesOfType('order').map((m) => m.p)).toEqual([{ qty: 1 }]);
  });

  it('drops a live message sent before it connects: it never goes out late', async () => {
    const c = client(A);
    expect(c.send('open_valve', { id: 1 }, { live: true })).toBe('dropped');
    c.retain();
    await tick();
    const ws = socketOf(A)[0];
    await connectServer(ws);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(ws.messagesOfType('open_valve')).toEqual([]);
    expect(c.send('open_valve', { id: 2 }, { live: true })).toBe('sent');
  });

  it('ends for good on signOut, and forgets what it kept', async () => {
    const storage = new Map<string, string>();
    const c = client(A, {
      topics: ['prices'],
      storage: {
        get: (key) => storage.get(key) ?? null,
        set: (key, value) => void storage.set(key, value),
        remove: (key) => void storage.delete(key),
        clear: () => storage.clear(),
        offlineDatabase: null,
      },
    });
    c.retain();
    await tick();
    await connectServer(socketOf(A)[0], { user_id: 'alice' });
    expect(storage.size).toBeGreaterThan(0);
    c.signOut();
    expect(storage.size).toBe(0);
    expect(c.state.getSnapshot().connection).toBe('disconnected');
    expect(() => c.retain()).toThrow(/signed out or destroyed/);
    expect(c.send('order', {})).toBe('dropped');
  });

  it('dispatches on the window only when asked to', async () => {
    const c = client(A, { windowEvents: true });
    const heard = vi.fn();
    window.addEventListener('wseGap', heard);
    c.retain();
    await tick();
    const ws = socketOf(A)[0];
    await connectServer(ws);
    ws.serverSend(stamped('prices', 1));
    ws.serverSend(stamped('prices', 3));
    await vi.advanceTimersByTimeAsync(5_000);
    expect(heard).toHaveBeenCalled();
    window.removeEventListener('wseGap', heard);
  });
});
