// @vitest-environment happy-dom
//
// What a client says of each topic (the server took it or refused it; a
// break in its messages happened or not), and the recover-or-resync mode: a
// topic that is not continuous holds its messages until the application
// brought its state up to date, and the checkpoint is what the application
// committed it applied.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { FakeWebSocket, connectServer, installFakeWebSocket, resetClientState, stamped, tick } from './helpers.js';
import { createWSEClient, type ResyncRequest, type ResyncResult, type WSEClient } from '../core/index.js';
import type { WSMessage } from '../types.js';

const URL = 'ws://wse.test/wse';
const EPOCH = '0000000a';

let clients: WSEClient[] = [];

function client(options: Parameters<typeof createWSEClient>[0] = {}): WSEClient {
  const made = createWSEClient({ endpoints: [URL], offline: { enabled: false }, ...options });
  clients.push(made);
  return made;
}

function reply(success: string[], failed: string[] = []) {
  return {
    c: 'WSE',
    t: 'subscription_update',
    p: { action: 'subscribe', success: true, success_topics: success, failed_topics: failed },
    v: 1,
  };
}

async function send(ws: FakeWebSocket, ...frames: Record<string, unknown>[]): Promise<void> {
  for (const frame of frames) ws.serverSend(frame);
  await tick();
}

/** A resync handler the test answers by hand. */
function resyncs() {
  const requests: ResyncRequest[] = [];
  const answers: Array<{ resolve: (result?: ResyncResult) => void; reject: (error: Error) => void }> = [];
  const resync = (request: ResyncRequest) =>
    new Promise<ResyncResult | void>((resolve, reject) => {
      requests.push(request);
      answers.push({ resolve, reject });
    });
  return { requests, answers, resync };
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

describe('each topic', () => {
  it('says whether the server took it and whether its messages broke off', async () => {
    const c = client({ topics: ['prices', 'secret'] });
    const heard: number[] = [];
    c.onMessage('price_update', (m) => heard.push(m.p.offset));
    c.retain();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    expect(c.state.getSnapshot().topicStates.prices).toEqual({
      subscription: 'requested',
      continuity: 'continuous',
      reason: null,
    });
    await send(ws, reply(['prices'], ['secret']));
    expect(c.state.getSnapshot().topicStates).toEqual({
      prices: { subscription: 'active', continuity: 'continuous', reason: null },
      secret: { subscription: 'denied', continuity: 'continuous', reason: null },
    });

    // Best effort: the messages keep coming, and the topic says it broke off.
    const breaks: unknown[] = [];
    c.on('wseContinuity', (detail) => breaks.push(detail));
    await send(ws, stamped('prices', 1), stamped('prices', 3));
    await vi.advanceTimersByTimeAsync(5_000);
    expect(heard).toEqual([1, 3]);
    expect(c.state.getSnapshot().topicStates.prices).toEqual({
      subscription: 'active',
      continuity: 'resync_required',
      reason: 'offset_gap',
    });
    expect(breaks).toEqual([{ topic: 'prices', continuity: 'resync_required', reason: 'offset_gap' }]);

    // The application brought it up to date itself.
    c.resync(['prices']);
    expect(c.state.getSnapshot().topicStates.prices.continuity).toBe('continuous');
  });

  it('a topic released goes from the states', async () => {
    const c = client();
    c.retain();
    const lease = c.subscribe(['news']);
    expect(c.state.getSnapshot().topicStates.news?.subscription).toBe('requested');
    lease.release();
    expect(c.state.getSnapshot().topicStates.news).toBeUndefined();
  });

  it('an offset JavaScript cannot hold exactly is a break, not a message', async () => {
    const c = client({ topics: ['prices'] });
    const heard: unknown[] = [];
    const gaps: any[] = [];
    c.onMessage('price_update', (m) => heard.push(m.o));
    c.on('wseGap', (gap) => gaps.push(gap));
    c.retain();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    await send(ws, reply(['prices']), stamped('prices', 1), stamped('prices', 2 ** 53 + 2));
    expect(heard).toEqual([1]);
    expect(gaps).toMatchObject([{ topic: 'prices', reason: 'unsafe_offset' }]);
    expect(c.state.getSnapshot().topicStates.prices.reason).toBe('unsafe_offset');
  });
});

describe('recover-or-resync', () => {
  it('is asked for with a resync handler and a consumer; an unknown mode is refused', () => {
    expect(() => client({ consumer: 'orders', delivery: { mode: 'recover-or-resync' } as any })).toThrow(/resync/);
    expect(() => client({ delivery: { mode: 'recover-or-resync', resync: async () => {} } })).toThrow(/consumer/);
    expect(() => client({ delivery: { mode: 'strong' } as any })).toThrow(/mode/);
  });

  it('holds a topic from a cold start until the application resynced it', async () => {
    const { requests, answers, resync } = resyncs();
    const c = client({ topics: ['prices'], consumer: 'board', delivery: { mode: 'recover-or-resync', resync } });
    const heard: number[] = [];
    c.onMessage('price_update', (m) => heard.push(m.p.offset));
    c.retain();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws, { user_id: 'alice' });
    expect(c.state.getSnapshot().topicStates.prices).toEqual({
      subscription: 'requested',
      continuity: 'resync_required',
      reason: 'cold_start',
    });
    // Asked once the server took the topic: whatever comes later is held.
    expect(requests).toHaveLength(0);
    await send(ws, reply(['prices']));
    expect(requests).toHaveLength(1);
    expect(requests[0]).toMatchObject({ topics: ['prices'], reasons: { prices: 'cold_start' }, committed: {} });
    expect(c.state.getSnapshot().topicStates.prices.continuity).toBe('resyncing');

    await send(ws, stamped('prices', 1), stamped('prices', 2));
    expect(heard).toEqual([]);
    // The state the application loaded has message 1 already.
    answers[0].resolve({ includes: (m: WSMessage) => Number(m.o) <= 1 });
    await tick();
    expect(heard).toEqual([2]);
    expect(c.state.getSnapshot().topicStates.prices).toEqual({
      subscription: 'active',
      continuity: 'continuous',
      reason: null,
    });
    await send(ws, stamped('prices', 3));
    expect(heard).toEqual([2, 3]);
  });

  it('resyncs after every reconnect, recovering from what the application committed', async () => {
    const { requests, answers, resync } = resyncs();
    const c = client({ topics: ['prices'], consumer: 'board', delivery: { mode: 'recover-or-resync', resync } });
    const heard: WSMessage[] = [];
    c.onMessage('price_update', (m) => heard.push(m));
    c.retain();
    await tick();
    const ws1 = FakeWebSocket.last();
    await connectServer(ws1, { user_id: 'alice' });
    await send(ws1, reply(['prices']));
    answers[0].resolve();
    await tick();
    await send(ws1, stamped('prices', 1), stamped('prices', 2), stamped('prices', 3), stamped('prices', 4));
    expect(heard.map((m) => m.o)).toEqual([1, 2, 3, 4]);

    // Applied out of order: the checkpoint moves only through what has no hole.
    c.commit(heard[0]);
    c.commit(heard[2]);
    expect(JSON.parse(c.checkpoint()).topics).toEqual({ prices: { [EPOCH]: 1 } });
    c.commit(heard[1]);
    const saved = JSON.parse(c.checkpoint());
    expect(saved).toMatchObject({ v: 1, realm: URL, user: 'alice', consumer: 'board', topics: { prices: { [EPOCH]: 3 } } });

    // Message 4 came but was not applied: the recovery starts after 3.
    ws1.serverClose(1001);
    await tick();
    expect(c.state.getSnapshot().topicStates.prices).toMatchObject({ continuity: 'resync_required', reason: 'reconnected' });
    // The reconnect's delay has a jitter.
    for (let i = 0; i < 60 && FakeWebSocket.last() === ws1; i++) await vi.advanceTimersByTimeAsync(500);
    const ws2 = FakeWebSocket.last();
    expect(ws2).not.toBe(ws1);
    await connectServer(ws2, { user_id: 'alice' });
    const resubscribe = ws2.messagesOfType('subscription_update')[0];
    expect(resubscribe.p).toMatchObject({ recover: true, recovery: { prices: { streams: { [EPOCH]: 3 } } } });
    await send(ws2, reply(['prices']), stamped('prices', 4));
    expect(requests).toHaveLength(2);
    expect(requests[1]).toMatchObject({ reasons: { prices: 'reconnected' }, committed: { prices: { [EPOCH]: 3 } } });
    expect(heard.map((m) => m.o)).toEqual([1, 2, 3, 4]);
    answers[1].resolve();
    await tick();
    // The replayed 4 comes again: the application applies a repeat safely.
    expect(heard.map((m) => m.o)).toEqual([1, 2, 3, 4, 4]);
  });

  it('a stream from offset 0 is in the checkpoint only once something of it was applied', async () => {
    const { answers, resync } = resyncs();
    const c = client({ topics: ['prices'], consumer: 'board', delivery: { mode: 'recover-or-resync', resync } });
    const heard: WSMessage[] = [];
    c.onMessage('price_update', (m) => heard.push(m));
    c.retain();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws, { user_id: 'alice' });
    await send(ws, reply(['prices']));
    answers[0].resolve();
    await tick();
    await send(ws, stamped('prices', 0), stamped('prices', 1));
    expect(JSON.parse(c.checkpoint()).topics).toEqual({});
    c.commit(heard[0]);
    expect(JSON.parse(c.checkpoint()).topics).toEqual({ prices: { [EPOCH]: 0 } });
  });

  it('a checkpoint is never another user’s', async () => {
    const { answers, resync } = resyncs();
    const c = client({ topics: ['prices'], consumer: 'board', delivery: { mode: 'recover-or-resync', resync } });
    const heard: WSMessage[] = [];
    c.onMessage('price_update', (m) => heard.push(m));
    c.retain();
    await tick();
    const ws1 = FakeWebSocket.last();
    await connectServer(ws1, { user_id: 'alice' });
    await send(ws1, reply(['prices']));
    answers[0].resolve();
    await tick();
    await send(ws1, stamped('prices', 1), stamped('prices', 2));
    c.commit(heard[0]);
    c.commit(heard[1]);
    expect(JSON.parse(c.checkpoint()).topics).toEqual({ prices: { [EPOCH]: 2 } });

    // The next connection is Bob's: nothing of what Alice applied.
    ws1.serverClose(1001);
    await tick();
    for (let i = 0; i < 60 && FakeWebSocket.last() === ws1; i++) await vi.advanceTimersByTimeAsync(500);
    const ws2 = FakeWebSocket.last();
    await connectServer(ws2, { user_id: 'bob' });
    expect(ws2.messagesOfType('subscription_update')[0].p.recovery).toBeUndefined();
    const saved = JSON.parse(c.checkpoint());
    expect(saved.user).toBe('bob');
    expect(saved.topics).toEqual({});
  });

  it('of one user in two realms are kept apart', async () => {
    const rejected: unknown[] = [];
    const open = async (realm: string) => {
      const { answers, resync } = resyncs();
      const c = client({
        storage: 'realm-check:alice',
        realm,
        topics: ['prices'],
        consumer: 'board',
        delivery: { mode: 'recover-or-resync', resync },
      });
      c.on('wseCheckpointRejected', (detail) => rejected.push({ realm, detail }));
      c.onMessage('price_update', (m) => c.commit(m));
      c.retain();
      await tick();
      const ws = FakeWebSocket.last();
      await connectServer(ws, { user_id: 'alice' });
      await send(ws, reply(['prices']));
      answers[0].resolve();
      await tick();
      return { c, ws };
    };
    try {
      const production = await open('production');
      await send(production.ws, stamped('prices', 7));
      expect(JSON.parse(production.c.checkpoint()).topics).toEqual({ prices: { [EPOCH]: 7 } });
      production.c.destroy();
      const staging = await open('staging');
      await send(staging.ws, stamped('prices', 2, '0000000b'));
      expect(JSON.parse(staging.c.checkpoint()).topics).toEqual({ prices: { '0000000b': 2 } });
      staging.c.destroy();

      const again = await open('production');
      expect(rejected).toEqual([]);
      expect(JSON.parse(again.c.checkpoint()).topics).toEqual({ prices: { [EPOCH]: 7 } });
    } finally {
      localStorage.clear();
    }
  });

  it('a checkpoint of another consumer is refused aloud and recovers nothing', async () => {
    const { resync } = resyncs();
    const theirs = JSON.stringify({ v: 1, realm: URL, user: 'alice', consumer: 'audit', schema: null, topics: { prices: { [EPOCH]: 9 } } });
    const c = client({
      topics: ['prices'],
      consumer: 'board', delivery: { mode: 'recover-or-resync', resync, checkpoint: theirs },
    });
    const rejected: unknown[] = [];
    c.on('wseCheckpointRejected', (detail) => rejected.push(detail));
    c.retain();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws, { user_id: 'alice' });
    expect(rejected).toEqual([{ reason: 'consumer' }]);
    expect(ws.messagesOfType('subscription_update')[0].p.recovery).toBeUndefined();
  });

  it('a checkpoint of this consumer and user recovers from where it stood', async () => {
    const { requests, resync } = resyncs();
    const ours = JSON.stringify({ v: 1, realm: URL, user: 'alice', consumer: 'board', schema: '2', topics: { prices: { [EPOCH]: 9 } } });
    const c = client({
      topics: ['prices'],
      consumer: 'board', delivery: { mode: 'recover-or-resync', schema: '2', resync, checkpoint: ours },
    });
    c.retain();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws, { user_id: 'alice' });
    expect(ws.messagesOfType('subscription_update')[0].p.recovery).toMatchObject({ prices: { streams: { [EPOCH]: 9 } } });
    await send(ws, reply(['prices']));
    expect(requests[0]).toMatchObject({ reasons: { prices: 'cold_start' }, committed: { prices: { [EPOCH]: 9 } } });
  });

  it('a send that needs a topic waits until the topic is continuous', async () => {
    const { answers, resync } = resyncs();
    const c = client({ topics: ['prices'], consumer: 'board', delivery: { mode: 'recover-or-resync', resync } });
    c.retain();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    await send(ws, reply(['prices']));
    expect(c.send('order', { id: 1 }, { requires: ['prices'] })).toBe('queued');
    // A live send cannot wait.
    expect(c.send('valve', { id: 2 }, { requires: ['prices'], live: true })).toBe('dropped');
    await tick();
    expect(ws.messagesOfType('order')).toHaveLength(0);
    answers[0].resolve();
    await tick();
    expect(ws.messagesOfType('order')).toHaveLength(1);
  });

  it('messages held past the limit start the resync over', async () => {
    const { requests, answers, resync } = resyncs();
    const c = client({
      topics: ['prices'],
      consumer: 'board', delivery: { mode: 'recover-or-resync', resync, maxHeld: 2 },
    });
    const heard: number[] = [];
    c.onMessage('price_update', (m) => heard.push(m.p.offset));
    c.retain();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    await send(ws, reply(['prices']), stamped('prices', 1), stamped('prices', 2), stamped('prices', 3));
    answers[0].resolve();
    await tick();
    // Something was lost to the limit: nothing is handed on as if complete.
    expect(heard).toEqual([]);
    expect(requests).toHaveLength(2);
    expect(requests[1].reasons).toEqual({ prices: 'overflow' });
    await send(ws, stamped('prices', 4));
    answers[1].resolve();
    await tick();
    expect(heard).toEqual([4]);
  });

  it('a resync that fails is tried again', async () => {
    const { requests, answers, resync } = resyncs();
    const c = client({ topics: ['prices'], consumer: 'board', delivery: { mode: 'recover-or-resync', resync } });
    c.retain();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws);
    await send(ws, reply(['prices']));
    answers[0].reject(new Error('the backend is down'));
    await tick();
    expect(c.state.getSnapshot().topicStates.prices).toMatchObject({
      continuity: 'resync_required',
      reason: 'resync_failed',
    });
    await vi.advanceTimersByTimeAsync(1_000);
    expect(requests).toHaveLength(2);
    expect(requests[1].reasons).toEqual({ prices: 'resync_failed' });
    answers[1].resolve();
    await tick();
    expect(c.state.getSnapshot().topicStates.prices.continuity).toBe('continuous');
  });
});

describe('sends that wait for their topics', () => {
  it('of one user never go out on the next user’s connection', async () => {
    const { requests, answers, resync } = resyncs();
    const c = client({ topics: ['prices'], consumer: 'board', delivery: { mode: 'recover-or-resync', resync } });
    c.retain();
    await tick();
    const alice = FakeWebSocket.last();
    await connectServer(alice, { user_id: 'alice' });
    await send(alice, reply(['prices']));
    expect(c.send('order', { madeBy: 'alice' }, { requires: ['prices'] })).toBe('queued');

    // Back after a pause, the browser's cookie is Bob's.
    c.pause('manual');
    await tick();
    c.resume('manual');
    await tick();
    const bob = FakeWebSocket.last();
    expect(bob).not.toBe(alice);
    await connectServer(bob, { user_id: 'bob' });
    await send(bob, reply(['prices']));
    answers[answers.length - 1].resolve();
    await tick();
    expect(requests.length).toBeGreaterThanOrEqual(2);
    expect(c.state.getSnapshot().topicStates.prices.continuity).toBe('continuous');
    expect(bob.messagesOfType('order')).toEqual([]);
    expect(alice.messagesOfType('order')).toEqual([]);
  });

  it('kept in the queue wait for their topics again after a reconnect', async () => {
    const { answers, resync } = resyncs();
    const c = client({
      topics: ['prices'],
      consumer: 'board',
      delivery: { mode: 'recover-or-resync', resync },
      rateLimit: { capacity: 2, refillRate: 1 },
    });
    c.retain();
    await tick();
    const first = FakeWebSocket.last();
    await connectServer(first);
    await send(first, reply(['prices']));
    answers[0].resolve();
    await tick();
    expect(c.state.getSnapshot().topicStates.prices.continuity).toBe('continuous');
    // The rate limit is spent: the order waits in the queue.
    c.send('note', {});
    expect(c.send('order', { id: 1 }, { requires: ['prices'] })).toBe('queued');
    expect(first.messagesOfType('order')).toHaveLength(0);

    c.pause('manual');
    await tick();
    c.resume('manual');
    await tick();
    const second = FakeWebSocket.last();
    expect(second).not.toBe(first);
    await connectServer(second);
    // The rate limit fills up again; prices is not continuous on this connection yet.
    await vi.advanceTimersByTimeAsync(3_000);
    expect(c.state.getSnapshot().topicStates.prices.continuity).not.toBe('continuous');
    expect(second.messagesOfType('order')).toHaveLength(0);

    await send(second, reply(['prices']));
    answers[answers.length - 1].resolve();
    await tick();
    expect(second.messagesOfType('order')).toHaveLength(1);
  });

  it('share the memory limit with the other sends, whichever came first', () => {
    const { resync } = resyncs();
    const options = {
      topics: ['news'],
      consumer: 'board',
      delivery: { mode: 'recover-or-resync' as const, resync },
      performance: { memoryLimit: 500 },
    };
    const waitingFirst = client(options);
    expect(waitingFirst.send('note', { text: 'a'.repeat(190) }, { requires: ['news'] })).toBe('queued');
    expect(waitingFirst.send('note', { text: 'b'.repeat(190) })).toBe('dropped');
    const ordinaryFirst = client(options);
    expect(ordinaryFirst.send('note', { text: 'b'.repeat(190) })).toBe('queued');
    expect(ordinaryFirst.send('note', { text: 'a'.repeat(190) }, { requires: ['news'] })).toBe('dropped');
  });

  it('count against the memory limit with what is queued', async () => {
    const { resync } = resyncs();
    const c = client({
      topics: ['news'],
      consumer: 'board',
      delivery: { mode: 'recover-or-resync', resync },
      performance: { memoryLimit: 500 },
    });
    expect(c.send('note', { text: 'x'.repeat(1000) }, { requires: ['news'] })).toBe('dropped');
    expect(c.send('note', { text: 'small' }, { requires: ['news'] })).toBe('queued');
    // Together they would pass it too.
    expect(c.send('note', { text: 'y'.repeat(300) }, { requires: ['news'] })).toBe('dropped');
  });
});

describe('the protocol’s own messages', () => {
  it('are handled for the client whatever the application listens to', async () => {
    const { requests, resync } = resyncs();
    const c = client({ topics: ['prices'], consumer: 'board', delivery: { mode: 'recover-or-resync', resync } });
    const seen: unknown[] = [];
    const stop = c.onMessage('subscription_update', (m) => seen.push(m.p));
    c.retain();
    await tick();
    const ws = FakeWebSocket.last();
    await connectServer(ws, { user_id: 'alice' });
    await send(ws, reply(['prices']));
    expect(seen).toHaveLength(1);
    expect(requests).toHaveLength(1);
    expect(c.state.getSnapshot().topicStates.prices.subscription).toBe('active');

    // Nor when it stops listening.
    stop();
    const news = c.subscribe(['news']);
    await tick();
    await send(ws, reply(['news']));
    expect(c.state.getSnapshot().topicStates.news.subscription).toBe('active');
    expect(requests).toHaveLength(2);
    news.release();
  });
});
