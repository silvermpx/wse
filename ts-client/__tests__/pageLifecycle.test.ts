// @vitest-environment happy-dom
//
// The page's lifecycle: pagehide lets the connection go (a page in the
// back/forward cache holds no socket), pageshow brings it back and recovers
// what was missed; a page coming back after a sleep does not trust a socket
// that heard nothing for too long.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { FakeWebSocket, connectServer, installFakeWebSocket, resetClientState, stamped, tick } from './helpers.js';
import { createWSEClient, type WSEClient } from '../core/index.js';
import { acquireSession, makeSessionKey } from '../services/WSESession.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import { ConnectionState } from '../types.js';

const URL = 'ws://wse.test/wse';

let clients: WSEClient[] = [];

function client(options: Parameters<typeof createWSEClient>[0] = {}): WSEClient {
  const made = createWSEClient({ endpoints: [URL], offline: { enabled: false }, ...options });
  clients.push(made);
  return made;
}

function page(type: 'pagehide' | 'pageshow', persisted: boolean): void {
  const event = new Event(type) as Event & { persisted: boolean };
  Object.defineProperty(event, 'persisted', { value: persisted });
  window.dispatchEvent(event);
}

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
  it('lets the connection go on pagehide and recovers on pageshow', async () => {
    const c = client({ topics: ['prices'] });
    c.retain();
    await tick();
    const ws1 = FakeWebSocket.last();
    await connectServer(ws1);
    ws1.serverSend(stamped('prices', 1));
    await tick();

    page('pagehide', true);
    await tick();
    expect(c.state.getSnapshot().paused).toEqual(['page']);
    expect(ws1.closeCalls).toHaveLength(1);

    page('pageshow', true);
    await tick();
    expect(c.state.getSnapshot().paused).toEqual([]);
    const ws2 = FakeWebSocket.last();
    expect(ws2).not.toBe(ws1);
    await connectServer(ws2);
    expect(ws2.messagesOfType('subscription_update')[0].p).toMatchObject({
      recover: true,
      recovery: { prices: { offset: 1 } },
    });
  });

  it('hears nothing of the page once it is done', async () => {
    const c = client();
    c.retain();
    await tick();
    c.destroy();
    page('pagehide', true);
    page('pageshow', true);
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(1);
  });
});

describe('the page-wide client', () => {
  it('closes its socket on pagehide, keeps its session, and reconnects on pageshow', async () => {
    acquireSession({
      key: makeSessionKey('cookie', [URL]),
      auth: 'cookie',
      initialTopics: ['prices'],
      config: { endpoints: [URL], offline: { enabled: false } },
    });
    await tick();
    const ws1 = FakeWebSocket.last();
    await connectServer(ws1);
    expect(useWSEStore.getState().connectionState).toBe(ConnectionState.CONNECTED);

    page('pagehide', true);
    await tick();
    expect(ws1.closeCalls).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(10_000);
    // No reconnect while the page is hidden.
    expect(FakeWebSocket.instances).toHaveLength(1);

    page('pageshow', true);
    await tick();
    const ws2 = FakeWebSocket.last();
    expect(ws2).not.toBe(ws1);
    await connectServer(ws2);
    expect(useWSEStore.getState().connectionState).toBe(ConnectionState.CONNECTED);
  });
});

describe('a page back from a sleep', () => {
  it('does not trust a socket that heard nothing for too long', async () => {
    const c = client({ topics: ['prices'] });
    c.retain();
    await tick();
    const ws1 = FakeWebSocket.last();
    await connectServer(ws1);
    // The machine slept: the clock moved on, no timer ran.
    vi.setSystemTime(Date.now() + 10 * 60_000);
    Object.defineProperty(document, 'visibilityState', { value: 'visible', configurable: true });
    document.dispatchEvent(new Event('visibilitychange'));
    await tick();
    expect(FakeWebSocket.last()).not.toBe(ws1);
  });
});
