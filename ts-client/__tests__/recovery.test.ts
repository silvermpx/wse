// @vitest-environment happy-dom
//
// Inbound delivery: offset gaps never black a topic out, the recovery anchor
// is the last delivered position, a replay requested after a reconnect may
// fill a hole within a bounded window, no message is dropped for its age,
// and inbound size limits apply while inflating.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as pako from 'pako';
import {
  ENDPOINT,
  FakeWebSocket,
  connectServer,
  installFakeWebSocket,
  resetClientState,
  serverHelloMessage,
  stamped,
  tick,
} from './helpers.js';
import { acquireSession, makeSessionKey, wseActions } from '../services/WSESession.js';
import type { UseWSEConfig } from '../services/WSESession.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import type { WSEGapEvent } from '../types.js';

const EPOCH = '0000000a';

let received: Array<{ t: string; o?: number; p: any }>;
let gaps: WSEGapEvent[];
let cleanups: Array<() => void> = [];

function start(config: UseWSEConfig = {}, topics: string[] = ['prices']) {
  return acquireSession({
    key: makeSessionKey('cookie', [ENDPOINT]),
    auth: 'cookie',
    initialTopics: topics,
    config: {
      endpoints: [ENDPOINT],
      offline: { enabled: false },
      reconnection: { baseDelay: 1000, maxDelay: 1000, jitter: false },
      registerHandlers: (p) => {
        p.registerHandler('price_update', (m) => received.push(m));
        p.registerHandler('metrics_response', (m) => received.push(m));
      },
      ...config,
    },
  });
}

const offsets = () => received.filter((m) => m.t === 'price_update').map((m) => m.o);

async function send(ws: FakeWebSocket, ...msgs: unknown[]) {
  for (const m of msgs) ws.serverSend(m as Record<string, unknown>);
  await tick();
}

async function reconnect(oldWs: FakeWebSocket): Promise<FakeWebSocket> {
  oldWs.serverClose(1001);
  await tick();
  await vi.advanceTimersByTimeAsync(1_000);
  const ws = FakeWebSocket.last();
  expect(ws).not.toBe(oldWs);
  await connectServer(ws);
  return ws;
}

function recoveryReply(topic: string, offset: number, recovered: boolean, epoch = EPOCH) {
  return {
    c: 'WSE',
    t: 'subscription_update',
    p: {
      action: 'subscribe',
      success: true,
      success_topics: [topic],
      recovered,
      recovery: { [topic]: { epoch, offset, recovered, count: 0 } },
    },
    v: 1,
  };
}

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
  vi.useFakeTimers();
  received = [];
  gaps = [];
  const listener = (e: Event) => gaps.push((e as CustomEvent).detail);
  window.addEventListener('wseGap', listener);
  cleanups.push(() => window.removeEventListener('wseGap', listener));
});

afterEach(() => {
  cleanups.forEach((fn) => fn());
  cleanups = [];
  resetClientState();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('offset gaps', () => {
  it('delivers across a gap, reports it, and keeps delivering', async () => {
    start();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);

    await send(ws, stamped('prices', 1), stamped('prices', 2), stamped('prices', 5), stamped('prices', 6));

    expect(offsets()).toEqual([1, 2, 5, 6]);
    expect(gaps).toEqual([{
      topic: 'prices', epoch: EPOCH, fromOffset: 3, toOffset: 4, missed: 2, reason: 'offset_gap',
    }]);
    const s = useWSEStore.getState();
    expect(s.subscriptions.recoveryState.prices).toEqual({ epoch: EPOCH, offset: 6 });
    expect(s.metrics.gapsDetected).toBe(1);
    expect(s.metrics.messagesMissed).toBe(2);
    // No recovery request storm: the client did not resubscribe per gap.
    expect(ws.messagesOfType('subscription_update')).toHaveLength(1);
  });

  it('drops duplicates by (epoch, offset)', async () => {
    start();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    await send(ws, stamped('prices', 1), stamped('prices', 2), stamped('prices', 2), stamped('prices', 1), stamped('prices', 3));
    expect(offsets()).toEqual([1, 2, 3]);
    expect(gaps).toEqual([]);
  });

  it('forgets a topic position on unsubscribe, so a later resubscribe is not a gap', async () => {
    start();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    await send(ws, stamped('prices', 1), stamped('prices', 2));

    wseActions.unsubscribe(['prices']);
    expect(useWSEStore.getState().subscriptions.recoveryState.prices).toBeUndefined();
    wseActions.subscribe(['prices']);
    await send(ws, stamped('prices', 40));

    expect(offsets()).toEqual([1, 2, 40]);
    expect(gaps).toEqual([]);
  });

  it('reports an epoch change (server restart) and continues on the new epoch', async () => {
    start();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    await send(ws, stamped('prices', 7), stamped('prices', 0, '0000000b'), stamped('prices', 1, '0000000b'));

    expect(offsets()).toEqual([7, 0, 1]);
    expect(gaps.map((g) => g.reason)).toEqual(['epoch_changed']);
  });

  it('learns that the server supports recovery from server_hello', async () => {
    start();
    await tick();
    const ws = FakeWebSocket.last();
    ws.serverOpen();
    await tick();
    expect(useWSEStore.getState().subscriptions.serverRecoveryEnabled).toBe(false);

    // No server_ready (server without jwt_secret): the client proceeds after the timeout.
    await vi.advanceTimersByTimeAsync(10_000);
    expect(ws.messagesOfType('client_hello')).toHaveLength(1);
    await send(ws, serverHelloMessage({ features: { recovery: true } }));
    expect(useWSEStore.getState().subscriptions.serverRecoveryEnabled).toBe(true);
  });
});

describe('recovery after a reconnect', () => {
  it('resubscribes from the last delivered position and fills the hole in order', async () => {
    start();
    await tick();
    const ws1 = FakeWebSocket.last();
    await connectServer(ws1);
    await send(ws1, stamped('prices', 1), stamped('prices', 2), stamped('prices', 3));

    const ws2 = await reconnect(ws1);
    const resub = ws2.messagesOfType('subscription_update')[0];
    expect(resub.p).toMatchObject({
      action: 'subscribe',
      topics: ['prices'],
      recover: true,
      recovery: { prices: { epoch: EPOCH, offset: 3 } },
    });

    // A live message overtakes the replay: it is held, not delivered early or lost.
    await send(ws2, stamped('prices', 7));
    expect(offsets()).toEqual([1, 2, 3]);

    await send(ws2, stamped('prices', 4), stamped('prices', 5), stamped('prices', 6), recoveryReply('prices', 7, true));
    expect(offsets()).toEqual([1, 2, 3, 4, 5, 6, 7]);
    expect(gaps).toEqual([]);

    await send(ws2, stamped('prices', 8));
    expect(offsets()).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
  });

  it('delivers held messages with a gap event when no replay comes', async () => {
    start();
    await tick();
    const ws1 = FakeWebSocket.last();
    await connectServer(ws1);
    await send(ws1, stamped('prices', 1), stamped('prices', 2), stamped('prices', 3));

    const ws2 = await reconnect(ws1);
    await send(ws2, stamped('prices', 7), stamped('prices', 8));
    expect(offsets()).toEqual([1, 2, 3]);

    await vi.advanceTimersByTimeAsync(3_000);
    expect(offsets()).toEqual([1, 2, 3, 7, 8]);
    expect(gaps).toEqual([{
      topic: 'prices', epoch: EPOCH, fromOffset: 4, toOffset: 6, missed: 3, reason: 'offset_gap',
    }]);

    await send(ws2, stamped('prices', 9));
    expect(offsets()).toEqual([1, 2, 3, 7, 8, 9]);
  });

  it('a recovered:true reply never moves the anchor past what was delivered', async () => {
    start();
    await tick();
    const ws1 = FakeWebSocket.last();
    await connectServer(ws1);
    await send(ws1, stamped('prices', 1), stamped('prices', 2));

    const ws2 = await reconnect(ws1);
    // Truncated replay: only offset 3 arrives, but the reply reports head 400.
    await send(ws2, stamped('prices', 3), recoveryReply('prices', 400, true));
    expect(useWSEStore.getState().subscriptions.recoveryState.prices).toEqual({ epoch: EPOCH, offset: 3 });

    const ws3 = await reconnect(ws2);
    expect(ws3.messagesOfType('subscription_update')[0].p.recovery).toEqual({ prices: { epoch: EPOCH, offset: 3 } });
  });

  it('a recovered:false reply releases held messages and re-baselines forward with a gap event', async () => {
    start();
    await tick();
    const ws1 = FakeWebSocket.last();
    await connectServer(ws1);
    await send(ws1, stamped('prices', 1), stamped('prices', 2), stamped('prices', 3));

    const ws2 = await reconnect(ws1);
    await send(ws2, recoveryReply('prices', 10, false));
    expect(gaps).toEqual([{
      topic: 'prices', epoch: EPOCH, fromOffset: 4, toOffset: 10, missed: 7, reason: 'not_recovered',
    }]);

    await send(ws2, stamped('prices', 11));
    expect(offsets()).toEqual([1, 2, 3, 11]);
    expect(gaps).toHaveLength(1);
  });
});

describe('no age-based dropping', () => {
  it('delivers old-timestamped messages and does not throttle heartbeat/metrics-named events', async () => {
    start();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);

    const hourAgo = new Date(Date.now() - 3_600_000).toISOString();
    await send(ws,
      { c: 'U', t: 'price_update', ts: hourAgo, p: { n: 1 }, v: 1 },
      { c: 'U', t: 'price_update', ts: new Date(Date.now() + 60_000).toISOString(), p: { n: 2 }, v: 1 },
      { c: 'U', t: 'metrics_response', p: { n: 3 }, v: 1 },
      { c: 'U', t: 'metrics_response', p: { n: 4 }, v: 1 },
    );

    expect(received.map((m) => m.p.n)).toEqual([1, 2, 3, 4]);
  });
});

describe('inbound size limits', () => {
  it('drops a text frame above maxMessageSize', async () => {
    start({ maxMessageSize: 1024 });
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);

    await send(ws, { c: 'U', t: 'price_update', p: { pad: 'x'.repeat(2_000) }, v: 1 });
    await send(ws, { c: 'U', t: 'price_update', p: { n: 1 }, v: 1 });
    expect(received.map((m) => m.p.n)).toEqual([1]);
    expect(useWSEStore.getState().metrics.messagesDropped).toBe(1);
  });

  it('rejects a compressed frame that inflates past the limit, without inflating it all', async () => {
    start({ maxMessageSize: 1024 * 1024 });
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);

    // ~64 MiB of zeros compresses to ~64 KiB: a decompression bomb.
    const bomb = pako.deflate(new Uint8Array(64 * 1024 * 1024));
    const frame = new Uint8Array(2 + bomb.length);
    frame.set([67, 58]);
    frame.set(bomb, 2);
    const started = performance.now();
    await send(ws, frame.buffer);
    expect(performance.now() - started).toBeLessThan(2_000);
    expect(received).toEqual([]);
    expect(useWSEStore.getState().metrics.messagesDropped).toBe(1);
  });

  it('accepts a 1.5 MB compressed snapshot under the default 16 MiB limit', async () => {
    start();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);

    const big = JSON.stringify({ c: 'U', t: 'price_update', p: { n: 1, blob: 'y'.repeat(1_500_000) }, v: 1 });
    const zipped = pako.deflate(new TextEncoder().encode(big));
    const frame = new Uint8Array(2 + zipped.length);
    frame.set([67, 58]);
    frame.set(zipped, 2);
    await send(ws, frame.buffer);

    expect(received).toHaveLength(1);
    expect(received[0].p.blob).toHaveLength(1_500_000);
  });
});

describe('unhandled messages', () => {
  it('dispatches one wseMessage window event for a type without a handler', async () => {
    const seen: any[] = [];
    const listener = (e: Event) => seen.push((e as CustomEvent).detail);
    window.addEventListener('wseMessage', listener);
    cleanups.push(() => window.removeEventListener('wseMessage', listener));

    start();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    await send(ws, { c: 'U', t: 'notification', p: { text: 'hello' }, v: 1 });

    expect(seen).toHaveLength(1);
    expect(seen[0]).toMatchObject({ t: 'notification', p: { text: 'hello' } });
  });
});
