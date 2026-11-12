// @vitest-environment happy-dom
//
// Connection lifecycle: reconnect policy, close-code contract, auth
// rejection, endpoint switching races, cookie refresh, rate limiting and
// messages sent while the connection is not ready.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ENDPOINT,
  FakeWebSocket,
  connectServer,
  installFakeWebSocket,
  resetClientState,
  tick,
} from './helpers.js';
import { acquireSession, makeSessionKey, wseActions, getCurrentSession } from '../services/WSESession.js';
import type { UseWSEConfig } from '../services/WSESession.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import { ConnectionState } from '../types.js';

function start(config: UseWSEConfig = {}, auth = 'cookie', topics: string[] | undefined = []) {
  return acquireSession({
    key: makeSessionKey(auth, [ENDPOINT]),
    auth,
    initialTopics: topics,
    config: { endpoints: [ENDPOINT], offline: { enabled: false }, ...config },
  });
}

const state = () => useWSEStore.getState().connectionState;

function recordEvents(name: string): any[] {
  const events: any[] = [];
  const listener = (e: Event) => events.push((e as CustomEvent).detail);
  window.addEventListener(name, listener);
  cleanups.push(() => window.removeEventListener(name, listener));
  return events;
}

let cleanups: Array<() => void> = [];

/** Advance fake time in 500 ms steps until the client opens another socket. */
async function advanceUntilNewSocket(limitMs: number): Promise<boolean> {
  const before = FakeWebSocket.instances.length;
  for (let waited = 0; waited < limitMs; waited += 500) {
    await vi.advanceTimersByTimeAsync(500);
    if (FakeWebSocket.instances.length > before) return true;
  }
  return false;
}

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
});

afterEach(() => {
  cleanups.forEach((fn) => fn());
  cleanups = [];
  resetClientState();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('reconnection policy', () => {
  it.each([
    ['the default policy', {}],
    ['maxAttempts: -1', { reconnection: { maxAttempts: -1 } }],
  ])('keeps reconnecting through 25+ failed attempts with %s', async (_label, config) => {
    vi.useFakeTimers();
    const states: string[] = [];
    cleanups.push(useWSEStore.subscribe((s) => states.push(s.connectionState)));

    start(config as UseWSEConfig);
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(1);

    for (let i = 0; i < 25; i++) {
      FakeWebSocket.last().serverClose(1006);
      await tick();
      expect(state()).toBe(ConnectionState.RECONNECTING);
      // Never more than maxDelay (30 s) or the circuit breaker's 60 s pause.
      expect(await advanceUntilNewSocket(61_000)).toBe(true);
      expect(FakeWebSocket.instances).toHaveLength(i + 2);
    }

    expect(states).not.toContain(ConnectionState.ERROR);
    expect(useWSEStore.getState().metrics.reconnectCount).toBe(25);

    await connectServer(FakeWebSocket.last());
    expect(state()).toBe(ConnectionState.CONNECTED);
    expect(useWSEStore.getState().metrics.reconnectCount).toBe(0);
  });

  it('caps the backoff at maxDelay instead of growing a second, longer delay', async () => {
    vi.useFakeTimers();
    start({ reconnection: { baseDelay: 1000, maxDelay: 5000, jitter: false } });
    await tick();

    // Four failures stay below the circuit breaker (5): the delay is pure backoff.
    for (let i = 0; i < 4; i++) {
      FakeWebSocket.last().serverClose(1006);
      await tick();
      await vi.advanceTimersByTimeAsync(5_000);
    }
    expect(FakeWebSocket.instances).toHaveLength(5);
  });

  it('honours a finite maxAttempts and reports when it gives up', async () => {
    vi.useFakeTimers();
    const failed = recordEvents('wseReconnectFailed');
    start({ reconnection: { maxAttempts: 3 } });
    await tick();

    for (let i = 0; i < 3; i++) {
      FakeWebSocket.last().serverClose(1006);
      await tick();
      await vi.advanceTimersByTimeAsync(61_000);
    }

    expect(FakeWebSocket.instances).toHaveLength(3);
    expect(state()).toBe(ConnectionState.ERROR);
    expect(failed).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(FakeWebSocket.instances).toHaveLength(3);
  });

  it('reconnects at once when the browser comes back online', async () => {
    vi.useFakeTimers();
    start({ reconnection: { baseDelay: 20_000, maxDelay: 30_000 } });
    await tick();
    FakeWebSocket.last().serverClose(1006);
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(1);

    window.dispatchEvent(new Event('online'));
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(2);
  });
});

describe('server-initiated closes', () => {
  it.each([1000, 1001, 1005, 1006, 1011, 1012, 1013, 4300, 4429])(
    'reconnects with backoff after the server closes with %i',
    async (code) => {
      vi.useFakeTimers();
      start();
      await tick();
      await connectServer(FakeWebSocket.last());
      expect(state()).toBe(ConnectionState.CONNECTED);

      FakeWebSocket.last().serverClose(code);
      await tick();
      expect(state()).toBe(ConnectionState.RECONNECTING);

      await vi.advanceTimersByTimeAsync(1_500);
      expect(FakeWebSocket.instances).toHaveLength(2);
      await connectServer(FakeWebSocket.last());
      expect(state()).toBe(ConnectionState.CONNECTED);
    },
  );

  it('does not reconnect after the client itself disconnects', async () => {
    vi.useFakeTimers();
    const session = start();
    await tick();
    await connectServer(FakeWebSocket.last());

    session.connection!.disconnect();
    FakeWebSocket.last().finishClose(1000);
    await vi.advanceTimersByTimeAsync(10 * 60_000);

    expect(FakeWebSocket.instances).toHaveLength(1);
    expect(state()).toBe(ConnectionState.DISCONNECTED);
  });
});

describe('authentication rejection', () => {
  it('stops reconnecting on 4401 and reports the failure once', async () => {
    vi.useFakeTimers();
    const failures = recordEvents('wseAuthFailed');
    start();
    await tick();

    const ws = FakeWebSocket.last();
    ws.serverOpen();
    ws.serverSend({ c: 'WSE', t: 'error', p: { code: 'AUTH_FAILED', message: 'Authentication failed' } });
    await tick();
    ws.serverClose(4401, 'Authentication failed');
    await tick();

    expect(state()).toBe(ConnectionState.ERROR);
    expect(failures).toHaveLength(1);
    expect(failures[0].code).toBe(4401);

    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(FakeWebSocket.instances).toHaveLength(1);
  });

  it('treats an AUTH_FAILED error followed by a plain close as a rejection too', async () => {
    vi.useFakeTimers();
    start();
    await tick();

    const ws = FakeWebSocket.last();
    ws.serverOpen();
    ws.serverSend({ c: 'WSE', t: 'error', p: { code: 'AUTH_FAILED', message: 'bad token' } });
    await tick();
    ws.serverClose(1005);
    await tick();

    expect(state()).toBe(ConnectionState.ERROR);
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(FakeWebSocket.instances).toHaveLength(1);
  });

  it('refreshes credentials once, retries once, and stops when rejected again', async () => {
    vi.useFakeTimers();
    const refresh = vi.fn().mockResolvedValue(undefined);
    const failures = recordEvents('wseAuthFailed');
    start({ refreshAuthToken: refresh });
    await tick();

    FakeWebSocket.last().serverOpen();
    FakeWebSocket.last().serverClose(4401);
    await tick();
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(failures).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(5_000);
    expect(FakeWebSocket.instances).toHaveLength(2);

    FakeWebSocket.last().serverOpen();
    FakeWebSocket.last().serverClose(4401);
    await tick();

    expect(state()).toBe(ConnectionState.ERROR);
    expect(failures).toHaveLength(1);
    expect(refresh).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(FakeWebSocket.instances).toHaveLength(2);
  });

  it('does not reset the backoff when a socket opens but never reaches server_ready', async () => {
    vi.useFakeTimers();
    start({ reconnection: { baseDelay: 1000, maxDelay: 30_000, jitter: false, mode: 'exponential', factor: 2 } });
    await tick();

    // open -> closed before server_ready, twice: delays 2 s, then 4 s.
    FakeWebSocket.last().serverOpen();
    FakeWebSocket.last().serverClose(1006);
    await tick();
    await vi.advanceTimersByTimeAsync(1_999);
    expect(FakeWebSocket.instances).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(FakeWebSocket.instances).toHaveLength(2);

    FakeWebSocket.last().serverOpen();
    FakeWebSocket.last().serverClose(1006);
    await tick();
    await vi.advanceTimersByTimeAsync(3_999);
    expect(FakeWebSocket.instances).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(FakeWebSocket.instances).toHaveLength(3);
  });

  it('forceReconnect() leaves the auth-failed state and connects again', async () => {
    vi.useFakeTimers();
    start();
    await tick();
    FakeWebSocket.last().serverOpen();
    FakeWebSocket.last().serverClose(4401);
    await tick();
    expect(state()).toBe(ConnectionState.ERROR);

    wseActions.forceReconnect();
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(2);
    await connectServer(FakeWebSocket.last());
    expect(state()).toBe(ConnectionState.CONNECTED);
  });
});

describe('endpoint switching and forced reconnects', () => {
  it('a late close event of the old socket cannot wipe the new socket (changeEndpoint)', async () => {
    vi.useFakeTimers();
    start();
    await tick();
    const oldWs = FakeWebSocket.last();
    await connectServer(oldWs);

    wseActions.changeEndpoint('ws://other.test/wse');
    await tick();
    const newWs = FakeWebSocket.last();
    expect(newWs).not.toBe(oldWs);
    expect(newWs.url).toContain('other.test');
    expect(oldWs.closeCalls).toHaveLength(1);

    // The browser delivers the old socket's close event after the switch.
    oldWs.finishClose(1000);
    await connectServer(newWs);

    expect(state()).toBe(ConnectionState.CONNECTED);
    expect(getCurrentSession()!.connection!.isTransportReady()).toBe(true);
    expect(wseActions.sendMessage('chat', { text: 'hi' })).toBe('sent');
    expect(newWs.appTextMessages().map((m) => m.t)).toContain('chat');
  });

  it('a late close event of the old socket cannot wipe the new socket (forceReconnect)', async () => {
    vi.useFakeTimers();
    start();
    await tick();
    const oldWs = FakeWebSocket.last();
    await connectServer(oldWs);

    wseActions.forceReconnect();
    await tick();
    const newWs = FakeWebSocket.last();
    await connectServer(newWs);
    oldWs.finishClose(1000);
    await tick();

    expect(state()).toBe(ConnectionState.CONNECTED);
    expect(wseActions.sendMessage('chat', { text: 'still works' })).toBe('sent');
  });

  it('forceReconnect() during an outage with the circuit breaker open connects at once', async () => {
    vi.useFakeTimers();
    start({ reconnection: { jitter: false } });
    await tick();
    for (let i = 0; i < 5; i++) {
      FakeWebSocket.last().serverClose(1006);
      await tick();
      if (i < 4) await vi.advanceTimersByTimeAsync(6_000);
    }
    expect(FakeWebSocket.instances).toHaveLength(5);
    expect(useWSEStore.getState().circuitBreaker.state).toBe('open');

    // The breaker pauses retries for 60 s...
    await vi.advanceTimersByTimeAsync(10_000);
    expect(FakeWebSocket.instances).toHaveLength(5);

    // ...but a manual reconnect resets it and connects now.
    wseActions.forceReconnect();
    await tick();
    expect(FakeWebSocket.instances).toHaveLength(6);
    await connectServer(FakeWebSocket.last());
    expect(state()).toBe(ConnectionState.CONNECTED);
  });
});

describe('periodic cookie refresh', () => {
  it('a failing refresh keeps a healthy connection and retries later', async () => {
    vi.useFakeTimers();
    const refresh = vi.fn().mockRejectedValue(new Error('auth endpoint 503'));
    const refreshFailures = recordEvents('wseTokenRefreshFailed');
    start({ refreshAuthToken: refresh }, 'cookie');
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);

    await vi.advanceTimersByTimeAsync(10 * 60_000 + 5_000);
    expect(refresh).toHaveBeenCalledTimes(3);
    expect(refreshFailures).toHaveLength(1);
    expect(ws.closeCalls).toHaveLength(0);
    expect(ws.readyState).toBe(FakeWebSocket.OPEN);
    expect(state()).toBe(ConnectionState.CONNECTED);
    expect(FakeWebSocket.instances).toHaveLength(1);

    // Retried with backoff (30 s), still without touching the socket.
    await vi.advanceTimersByTimeAsync(35_000);
    expect(refresh).toHaveBeenCalledTimes(6);
    expect(ws.closeCalls).toHaveLength(0);
  });
});

describe('sending while not ready', () => {
  it('queues messages sent before server_ready and sends them, cid intact, once ready', async () => {
    vi.useFakeTimers();
    start();
    expect(wseActions.sendMessage('chat', { n: 1 }, { correlation_id: 'req-1' })).toBe('queued');
    await tick();
    const ws = FakeWebSocket.last();
    ws.serverOpen();
    await tick();
    expect(wseActions.sendMessage('chat', { n: 2 })).toBe('queued');
    expect(ws.appTextMessages()).toEqual([]);

    ws.serverSend({ c: 'WSE', t: 'server_ready', p: { details: { connection_id: 'c', features: {} } }, v: 1 });
    await tick();

    const chats = ws.appTextMessages().filter((m) => m.t === 'chat');
    expect(chats.map((m) => m.p.n)).toEqual([1, 2]);
    expect(chats[0].cid).toBe('req-1');
  });

  it('delivers messages sent during a reconnect exactly once after it', async () => {
    vi.useFakeTimers();
    start();
    await tick();
    await connectServer(FakeWebSocket.last());
    FakeWebSocket.last().serverClose(1006);
    await tick();

    expect(wseActions.sendMessage('chat', { n: 1 })).toBe('queued');
    expect(wseActions.sendMessage('chat', { n: 2 })).toBe('queued');
    await vi.advanceTimersByTimeAsync(1_500);
    const ws2 = FakeWebSocket.last();
    await connectServer(ws2);

    expect(ws2.appTextMessages().filter((m) => m.t === 'chat').map((m) => m.p.n)).toEqual([1, 2]);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(ws2.appTextMessages().filter((m) => m.t === 'chat')).toHaveLength(2);
  });

  it('proceeds without server_ready after serverReadyTimeout (server without jwt_secret)', async () => {
    vi.useFakeTimers();
    start({}, 'cookie', ['news']);
    await tick();
    const ws = FakeWebSocket.last();
    ws.serverOpen();
    await tick();
    expect(ws.messagesOfType('subscription_update')).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(10_000);
    expect(ws.messagesOfType('client_hello')).toHaveLength(1);
    expect(ws.messagesOfType('subscription_update')[0].p.topics).toEqual(['news']);
  });
});

describe('outbound rate limit', () => {
  it('queues what exceeds the limit and sends it later, in order', async () => {
    vi.useFakeTimers();
    start({ rateLimit: { capacity: 5, refillRate: 5 } });
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);

    const statuses = Array.from({ length: 12 }, (_, n) => wseActions.sendMessage('reading', { n }));
    expect(statuses.filter((s) => s === 'sent')).toHaveLength(5);
    expect(statuses.filter((s) => s === 'queued')).toHaveLength(7);
    expect(statuses).not.toContain('dropped');

    await vi.advanceTimersByTimeAsync(2_100);
    const readings = ws.appTextMessages().filter((m) => m.t === 'reading').map((m) => m.p.n);
    expect(readings).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
  });

  it('never rate-limits the heartbeat ping or PONG replies', async () => {
    vi.useFakeTimers();
    start({ rateLimit: { capacity: 1, refillRate: 1 } });
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    wseActions.sendMessage('a', {});
    wseActions.sendMessage('b', {}); // exhausts the bucket

    ws.serverSend({ c: 'WSE', t: 'ping', p: { server_time: 'now' } });
    await tick();
    expect(ws.messagesOfType('PONG')).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(15_000);
    expect(ws.messagesOfType('ping').length).toBeGreaterThanOrEqual(1);
  });

  it('can be disabled', async () => {
    vi.useFakeTimers();
    start({ rateLimit: false });
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);

    const statuses = Array.from({ length: 2_000 }, (_, n) => wseActions.sendMessage('burst', { n }));
    expect(statuses.every((s) => s === 'sent')).toBe(true);
    expect(ws.appTextMessages().filter((m) => m.t === 'burst')).toHaveLength(2_000);
  });
});

describe('secure-context independence', () => {
  it('sends without crypto.randomUUID (plain-http pages)', async () => {
    vi.useFakeTimers();
    const cryptoObj = globalThis.crypto as any;
    Object.defineProperty(cryptoObj, 'randomUUID', { value: undefined, configurable: true, writable: true });
    cleanups.push(() => {
      delete cryptoObj.randomUUID;
    });

    start({}, 'cookie', ['news']);
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);

    expect(wseActions.sendMessage('chat', { text: 'hello' })).toBe('sent');
    const ids = ws.textMessages().map((m) => m.id).filter(Boolean);
    expect(ids.length).toBeGreaterThan(0);
    for (const id of ids) {
      expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    }
  });
});
