// The window bridge: one owner, one connection, windows over MessageChannel
// ports as Electron's would carry them.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { FakeWebSocket, installFakeWebSocket, resetClientState, serverReadyMessage, stamped } from './helpers.js';
import {
  BRIDGE_PROTOCOL,
  createBridgeClient,
  createBridgeOwner,
  messagePortChannel,
  type BridgeClient,
  type BridgeOwner,
  type OwnerFrame,
  type WindowFrame,
} from '../bridge/index.js';
import type { ResyncRequest } from '../core/delivery.js';
import type { WSMessage } from '../types.js';

const URL = 'ws://bridge.test/wse';

let owners: BridgeOwner[] = [];
let windows: BridgeClient[] = [];

function owner(options: Parameters<typeof createBridgeOwner>[0] = {}): BridgeOwner {
  const made = createBridgeOwner({ endpoints: [URL], reconnection: { baseDelay: 10, maxDelay: 10, jitter: false }, ...options });
  owners.push(made);
  return made;
}

/** A window of `o`, over a port pair. */
function window(o: BridgeOwner, options: Parameters<typeof createBridgeClient>[1] = {}): BridgeClient {
  const { port1, port2 } = new MessageChannel();
  o.attach(messagePortChannel<OwnerFrame, WindowFrame>(port1 as never));
  const made = createBridgeClient(messagePortChannel<WindowFrame, OwnerFrame>(port2 as never), options);
  windows.push(made);
  return made;
}

/** The server took the connection, and answers for the outbox. */
async function serve(ws: FakeWebSocket, user = 'ann'): Promise<void> {
  ws.serverOpen();
  ws.serverSend(serverReadyMessage({ user_id: user, features: { recovery: true, encryption: false, acks: true } }));
  ws.serverSend({ c: 'WSE', t: 'outbox', p: { acked: 0, node: 'n1' } });
  await settle();
}

function reply(success: string[]) {
  return { c: 'WSE', t: 'subscription_update', p: { action: 'subscribe', success: true, success_topics: success, failed_topics: [] }, v: 1 };
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 30));

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
});

afterEach(() => {
  for (const w of windows) w.destroy();
  for (const o of owners) o.destroy();
  windows = [];
  owners = [];
  vi.unstubAllGlobals();
});

describe('the window bridge', () => {
  it('serves two windows on one connection, each its own topics and listeners', async () => {
    const o = owner();
    const a = window(o);
    const b = window(o);
    const heard: Record<string, WSMessage[]> = { a: [], b: [] };
    a.onMessage('price_update', (m) => heard.a.push(m));
    b.onMessage('price_update', (m) => heard.b.push(m));
    a.retain();
    b.retain();
    a.subscribe(['prices']);
    b.subscribe(['prices', 'news']);
    await settle();
    expect(FakeWebSocket.instances).toHaveLength(1);
    const ws = FakeWebSocket.last();
    await serve(ws);
    await vi.waitFor(() => expect([a, b].map((w) => w.state.getSnapshot().ready)).toEqual([true, true]));
    ws.serverSend(reply(['prices', 'news']));
    ws.serverSend(stamped('prices', 1, '0000000a', { n: 1 }));
    ws.serverSend({ ...stamped('news', 1, '0000000b', { n: 2 }), tp: 'news' });
    await vi.waitFor(() => expect(heard.a.map((m) => m.p.n)).toEqual([1]));
    await vi.waitFor(() => expect(heard.b.map((m) => m.tp)).toEqual(['prices', 'news']));
    expect(a.state.getSnapshot().topicStates.prices.subscription).toBe('active');
    // One subscription each topic, for both windows.
    const subscribed = ws.messagesOfType('subscription_update').flatMap((m) => m.p.topics);
    expect(subscribed.filter((t: string) => t === 'prices')).toHaveLength(1);
  });

  it('sends a window’s message through the owner’s outbox, and tells that window alone what became of it', async () => {
    const o = owner();
    const a = window(o);
    const b = window(o);
    const outcomes = { a: [] as unknown[], b: [] as unknown[] };
    a.on('wseDelivered', (d) => outcomes.a.push(d));
    b.on('wseDelivered', (d) => outcomes.b.push(d));
    a.retain();
    await settle();
    await serve(FakeWebSocket.last());
    await vi.waitFor(() => expect(a.state.getSnapshot().ready).toBe(true));
    expect(a.send('order', { n: 1 }, { id: 'from-a' })).toBe('sent');
    expect(a.kept()).toBe(1);
    const ws = FakeWebSocket.last();
    await vi.waitFor(() => expect(ws.appTextMessages().map((m) => m.id)).toEqual(['from-a']));
    expect(ws.appTextMessages()[0].s).toBe(1);
    ws.serverSend({ c: 'WSE', t: 'ack', p: { s: 1 } });
    await vi.waitFor(() => expect(outcomes.a).toEqual([{ ids: ['from-a'] }]));
    expect(outcomes.b).toEqual([]);
    expect(a.kept()).toBe(0);
  });

  it('keeps the connection for the other windows when one goes', async () => {
    const o = owner();
    const a = window(o);
    const b = window(o);
    a.retain();
    b.retain();
    await settle();
    await serve(FakeWebSocket.last());
    await vi.waitFor(() => expect(b.state.getSnapshot().ready).toBe(true));
    a.destroy();
    await settle();
    expect(o.attached).toBe(1);
    expect(FakeWebSocket.instances).toHaveLength(1);
    expect(b.state.getSnapshot().ready).toBe(true);
    // The last share goes: so does the connection.
    b.destroy();
    await settle();
    expect(o.client.state.getSnapshot().connection).not.toBe('connected');
  });

  it('holds back a window that stops taking only for it, and breaks its topics when it falls too far behind', async () => {
    const o = owner({ limits: { queueItems: 20 } });
    const slow = window(o);
    const quick = window(o);
    const heard = { slow: 0, quick: 0 };
    const overloaded: unknown[] = [];
    slow.on('wseOverloaded', (d) => overloaded.push(d));
    slow.onMessage('price_update', () => heard.slow++);
    quick.onMessage('price_update', () => heard.quick++);
    slow.retain();
    quick.retain();
    slow.subscribe(['prices']);
    quick.subscribe(['prices']);
    await settle();
    const ws = FakeWebSocket.last();
    await serve(ws);
    ws.serverSend(reply(['prices']));
    await settle();
    slow.pause('busy');
    // Ten at a time: the quick window takes each ten before the next.
    for (let n = 1; n <= 60; n++) {
      ws.serverSend(stamped('prices', n, '0000000a', { n }));
      if (n % 10 === 0) await settle();
    }
    await vi.waitFor(() => expect(heard.quick).toBe(60));
    expect(heard.slow).toBe(0);
    slow.resume('busy');
    await vi.waitFor(() => expect(overloaded).toEqual([{ topics: ['prices'] }]));
    expect(slow.state.getSnapshot().topicStates.prices.continuity).not.toBe('continuous');
    expect(quick.state.getSnapshot().topicStates.prices.continuity).toBe('continuous');
  });

  it('ends the connection when the window that held the last share pauses, and makes it again on resume', async () => {
    const o = owner();
    const w = window(o);
    const heard: number[] = [];
    w.onMessage('price_update', (m) => heard.push(m.p.n));
    w.retain();
    w.subscribe(['prices']);
    await settle();
    const first = FakeWebSocket.last();
    await serve(first);
    first.serverSend(reply(['prices']));
    first.serverSend(stamped('prices', 1, '0000000a', { n: 1 }));
    await vi.waitFor(() => expect(heard).toEqual([1]));
    w.pause('background');
    expect(w.state.getSnapshot()).toMatchObject({ ready: false, paused: ['background'] });
    await vi.waitFor(() => expect(first.closeCalls.length).toBeGreaterThan(0));
    expect(FakeWebSocket.instances).toHaveLength(1);
    w.resume('background');
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
    const second = FakeWebSocket.last();
    await serve(second);
    // The topic is asked for again from where the owner was.
    const resubscribe = second.messagesOfType('subscription_update')[0];
    expect(resubscribe.p.topics).toEqual(['prices']);
    expect(resubscribe.p.recovery).toMatchObject({ prices: { offset: 1 } });
    await vi.waitFor(() => expect(w.state.getSnapshot().ready).toBe(true));
  });

  it('holds a recover-or-resync window’s topic until its own resync, as a client of its own', async () => {
    const o = owner();
    const requests: ResyncRequest[] = [];
    let done!: () => void;
    const w = window(o, {
      consumer: 'board',
      delivery: {
        mode: 'recover-or-resync',
        resync: (request) => {
          requests.push(request);
          return new Promise<void>((resolve) => (done = resolve));
        },
      },
    });
    const heard: number[] = [];
    w.onMessage('price_update', (m) => heard.push(m.p.n));
    w.retain();
    w.subscribe(['prices']);
    await settle();
    const ws = FakeWebSocket.last();
    await serve(ws);
    ws.serverSend(reply(['prices']));
    await vi.waitFor(() => expect(requests).toHaveLength(1));
    expect(requests[0].reasons).toEqual({ prices: 'cold_start' });
    ws.serverSend(stamped('prices', 1, '0000000a', { n: 1 }));
    await settle();
    expect(heard).toEqual([]);
    done();
    await vi.waitFor(() => expect(heard).toEqual([1]));
    expect(w.state.getSnapshot().topicStates.prices.continuity).toBe('continuous');
  });

  it('refuses a window of another protocol, and signs every window out with the user', async () => {
    const o = owner();
    const { port1, port2 } = new MessageChannel();
    o.attach(messagePortChannel<OwnerFrame, WindowFrame>(port1 as never));
    const frames: OwnerFrame[] = [];
    port2.addEventListener('message', (event) => frames.push(event.data as OwnerFrame));
    port2.start();
    port2.postMessage({ k: 'hello', v: BRIDGE_PROTOCOL + 1, agent: 'old' } satisfies WindowFrame);
    await vi.waitFor(() => expect(frames[0]).toMatchObject({ k: 'refused', reason: 'version' }));
    port2.close();

    const a = window(o);
    const b = window(o);
    a.retain();
    await settle();
    await serve(FakeWebSocket.last());
    await vi.waitFor(() => expect(b.bridge.phase).toBe('attached'));
    a.signOut();
    await vi.waitFor(() => expect(b.bridge).toEqual({ phase: 'revoked', reason: 'signed_out' }));
    expect(o.attached).toBe(0);
  });
});

describe('a window, its owner’s frames given by hand', () => {
  const conn = { connection: 'connected', ready: true, paused: [], endpoint: 'ws://localhost/', lastError: null, storage: 'memory' };

  /** A window whose owner's frames the test gives: welcomed as ann's. */
  function byHand(options: Parameters<typeof createBridgeClient>[1] = {}) {
    let sink: { frame(f: unknown): void; close(): void } | undefined;
    const posted: WindowFrame[] = [];
    const client = createBridgeClient(
      {
        listen: (s) => {
          sink = s as never;
        },
        post: (f) => posted.push(f),
        close: () => {},
      },
      options,
    );
    windows.push(client);
    const feed = (f: unknown) => sink!.frame(f);
    feed({ k: 'welcome', v: 1, att: 1, limits: {}, realm: 'test', account: { epoch: 0, user: 'ann' }, conn, gen: 1 });
    return { client, feed, posted };
  }

  const price = (o: number, secret = 'x') => ({ m: { t: 'price', p: { secret }, tp: 'prices', e: '0000000a', o } });

  it('hands on nothing it held paused once the window was signed out', () => {
    const { client, feed } = byHand();
    const heard: WSMessage[] = [];
    client.subscribe(['prices']);
    client.onMessage('price', (m) => heard.push(m));
    client.pause('hidden');
    feed({ k: 'b', s: 1, items: [price(1, 'the last user’s')] });
    feed({ k: 'revoked', reason: 'signed_out' });
    client.resume('hidden');
    feed({ k: 'b', s: 2, items: [price(2, 'late')] });
    expect(heard).toEqual([]);
  });

  it('starts the recovery from the checkpoint it was given', async () => {
    const requests: ResyncRequest[] = [];
    const checkpoint = JSON.stringify({
      v: 1,
      realm: 'test',
      user: 'ann',
      consumer: 'window',
      schema: null,
      topics: { prices: { '0000000a': 42 } },
    });
    const { client, feed } = byHand({
      consumer: 'window',
      realm: 'test',
      delivery: { mode: 'recover-or-resync', checkpoint, resync: async (r) => void requests.push(r) },
    });
    client.subscribe(['prices']);
    feed({ k: 'b', s: 1, items: [{ ev: 'subscribed', taken: ['prices'], refused: [] }] });
    await settle();
    expect(requests[0].committed).toEqual({ prices: { '0000000a': 42 } });
  });

  it('refuses a checkpoint of another user', async () => {
    const rejected: unknown[] = [];
    const checkpoint = JSON.stringify({ v: 1, realm: 'test', user: 'bob', consumer: 'window', schema: null, topics: { prices: { '0000000a': 7 } } });
    const requests: ResyncRequest[] = [];
    const { client, feed } = byHand({
      consumer: 'window',
      realm: 'test',
      delivery: { mode: 'recover-or-resync', checkpoint, resync: async (r) => void requests.push(r) },
    });
    client.on('wseCheckpointRejected', (detail) => rejected.push(detail));
    client.subscribe(['prices']);
    feed({ k: 'b', s: 1, items: [{ ev: 'subscribed', taken: ['prices'], refused: [] }] });
    await settle();
    expect(rejected).toHaveLength(1);
    expect(requests[0].committed).toEqual({});
  });

  it('breaks a topic off on an offset JavaScript cannot hold, as a client of its own does', () => {
    const { client, feed } = byHand();
    const gaps: Array<{ reason: string }> = [];
    const heard: WSMessage[] = [];
    client.subscribe(['prices']);
    client.on('wseGap', (detail) => gaps.push(detail));
    client.onMessage('price', (m) => heard.push(m));
    feed(JSON.parse('{"k":"b","s":1,"items":[{"m":{"t":"price","p":{},"tp":"prices","e":"0000000a","o":9007199254740993}}]}'));
    expect(heard).toEqual([]);
    expect(gaps.map((g) => g.reason)).toEqual(['unsafe_offset']);
  });

  it('counts the time of a send that waits for its topics from the send', async () => {
    let resynced!: () => void;
    const { client, feed, posted } = byHand({
      consumer: 'window',
      delivery: { mode: 'recover-or-resync', resync: () => new Promise<void>((resolve) => (resynced = resolve)) },
    });
    const notDelivered: Array<{ reason: string; ids: string[] }> = [];
    client.on('wseNotDelivered', (detail) => notDelivered.push(detail));
    client.subscribe(['prices']);
    feed({ k: 'b', s: 1, items: [{ ev: 'subscribed', taken: ['prices'], refused: [] }] });
    await settle();
    client.send('buy', { quantity: 1 }, { requires: ['prices'], ttl: 1, id: 'late' });
    await new Promise((resolve) => setTimeout(resolve, 10));
    resynced();
    await settle();
    expect(posted.filter((f) => f.k === 'send')).toEqual([]);
    expect(notDelivered).toEqual([{ reason: 'expired', ids: ['late'] }]);
  });
});

describe('the owner, at its windows’ edge', () => {
  it('never hands a window the credentials of its endpoint', async () => {
    const o = createBridgeOwner({ endpoints: ['ws://ann:password@localhost/wse?access_token=secret'], offline: { enabled: false } });
    owners.push(o);
    const frames: unknown[] = [];
    let sink: { frame(f: unknown): void } | undefined;
    o.attach({ post: (f) => frames.push(f), listen: (s) => (sink = s as never), close: () => {} });
    sink!.frame({ k: 'hello', v: 1, agent: 'test' });
    await settle();
    expect(JSON.stringify(frames)).not.toMatch(/password|access_token|secret/);
  });

  it('lets go of a window that sends what is not a frame, and goes on with the others', () => {
    const o = createBridgeOwner({ offline: { enabled: false } });
    owners.push(o);
    const sinks: Array<{ frame(f: unknown): void }> = [];
    const closed: number[] = [];
    for (const n of [1, 2]) {
      o.attach({ post: () => {}, listen: (s) => void sinks.push(s as never), close: () => void closed.push(n) });
      sinks[n - 1].frame({ k: 'hello', v: 1, agent: 'test' });
    }
    for (const bad of [null, 7, 'x', [], { k: 'send' }, { k: 'hold', topics: 'prices' }]) {
      expect(() => sinks[0].frame(bad)).not.toThrow();
    }
    expect(closed).toEqual([1]);
    expect(o.attached).toBe(1);
  });
});
