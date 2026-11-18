// The window bridge: one owner, one connection, windows over MessageChannel
// ports as Electron's would carry them.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { FakeWebSocket, installFakeWebSocket, resetClientState, serverReadyMessage, stamped } from './helpers.js';
import {
  BRIDGE_PROTOCOL,
  DEFAULT_BRIDGE_LIMITS,
  createBridgeClient,
  createBridgeOwner,
  messagePortChannel,
  type BridgeClient,
  type BridgeOwner,
  type OwnerFrame,
  type WindowChannel,
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
    // Handed to the owner's outbox: its outcome comes as an event.
    expect(a.send('order', { n: 1 }, { id: 'from-a' })).toBe('queued');
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

describe('the outcomes and topics of the windows’ sends', () => {
  it('says each window the outcome of its own send, whatever ids the other windows chose', async () => {
    const o = owner();
    const a = window(o);
    const b = window(o);
    const heardA: string[][] = [];
    const heardB: string[][] = [];
    a.on('wseDelivered', (d: { ids: string[] }) => heardA.push(d.ids));
    b.on('wseDelivered', (d: { ids: string[] }) => heardB.push(d.ids));
    a.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    await serve(ws);
    a.send('order', { n: 1 }, { id: 'order-1' });
    b.send('order', { n: 2 }, { id: 'order-1' });
    await vi.waitFor(() => expect(ws.messagesOfType('order')).toHaveLength(2));
    ws.serverSend({ c: 'WSE', t: 'ack', p: { s: 2 } });
    await vi.waitFor(() => {
      expect(heardA).toEqual([['order-1']]);
      expect(heardB).toEqual([['order-1']]);
    });
    expect([a.kept(), b.kept()]).toEqual([0, 0]);
  });

  it('says a window’s outcome to that window alone, not to the owner’s application as its own', async () => {
    const o = owner();
    const a = window(o);
    const heardByOwner: string[] = [];
    o.client.on('wseDelivered', (d: { ids: string[] }) => heardByOwner.push(...d.ids));
    const heard: string[][] = [];
    a.on('wseDelivered', (d: { ids: string[] }) => heard.push(d.ids));
    a.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    await serve(ws);
    a.send('order', { n: 1 }, { id: 'order-1' });
    await vi.waitFor(() => expect(ws.messagesOfType('order')).toHaveLength(1));
    o.client.send('note', { n: 2 }, { id: 'note-1' });
    await vi.waitFor(() => expect(ws.messagesOfType('note')).toHaveLength(1));
    ws.serverSend({ c: 'WSE', t: 'ack', p: { s: 2 } });
    await vi.waitFor(() => expect(heard).toEqual([['order-1']]));
    expect(heardByOwner).toEqual(['note-1']);
  });

  it('says a window its send ran out of time while the owner waited for a connection', async () => {
    const o = owner();
    const a = window(o);
    const notDelivered: Array<{ reason: string; ids: string[] }> = [];
    a.on('wseNotDelivered', (d: { reason: string; ids: string[] }) => notDelivered.push(d));
    a.retain();
    a.subscribe(['prices']);
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    await settle();
    a.send('cancel', { id: 9 }, { requires: ['prices'], ttl: 20, id: 'cancel-9' });
    await settle();
    await settle();
    const ws = FakeWebSocket.last();
    await serve(ws);
    await vi.waitFor(() => expect(notDelivered).toEqual([{ reason: 'expired', ids: ['cancel-9'] }]));
    expect(ws.messagesOfType('cancel')).toHaveLength(0);
  });

  it('says a window what became of a send it made while no one held the connection', async () => {
    const o = owner();
    const a = window(o);
    const heard: string[][] = [];
    a.on('wseDelivered', (d: { ids: string[] }) => heard.push(d.ids));
    await settle();
    // No share: the owner's client runs no connection, and keeps it for the next.
    a.send('order', { n: 1 }, { id: 'order-1' });
    await settle();
    a.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    await serve(ws);
    await vi.waitFor(() => expect(ws.messagesOfType('order')).toHaveLength(1));
    ws.serverSend({ c: 'WSE', t: 'ack', p: { s: 1 } });
    await vi.waitFor(() => expect(heard).toEqual([['order-1']]));
  });

  it('says a window the outcome of a send to a server that does not answer, said as it is sent', async () => {
    const o = owner();
    const a = window(o);
    const heard: string[][] = [];
    a.on('wseDelivered', (d: { ids: string[] }) => heard.push(d.ids));
    a.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    ws.serverOpen();
    ws.serverSend(serverReadyMessage({ user_id: 'ann', features: { recovery: true, encryption: false } }));
    await settle();
    a.send('order', { n: 1 }, { id: 'plain-1' });
    await vi.waitFor(() => expect(heard).toEqual([['plain-1']]));
    expect(a.kept()).toBe(0);
  });

  it('takes a window’s word on its topics for the connection it names, not one said before a loss', async () => {
    const o = owner();
    let sink: { frame(f: unknown): void } | undefined;
    o.attach({ post: () => {}, listen: (s) => (sink = s as never), close: () => {} });
    const feed = (f: unknown) => sink!.frame(f);
    feed({ k: 'hello', v: 1, agent: 'test' });
    feed({ k: 'share', on: true });
    feed({ k: 'hold', topics: ['prices'] });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const first = FakeWebSocket.last();
    await serve(first);
    const send = { k: 'send', n: 1, t: 'cancel', p: { id: 9 }, c: 1, a: 0, o: { id: 'cancel-9', requires: ['prices'] } };
    feed({ k: 'continuous', gen: 1, topics: ['prices'] });
    feed(send);
    await vi.waitFor(() => expect(first.messagesOfType('cancel')).toHaveLength(1));
    first.serverClose(1006);
    // Said before the window heard of the loss, it comes after it.
    feed({ k: 'continuous', gen: 1, topics: ['prices'] });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2), { timeout: 2_000 });
    const second = FakeWebSocket.last();
    await serve(second);
    await settle();
    expect(second.messagesOfType('cancel')).toHaveLength(0);
    feed({ k: 'continuous', gen: 2, topics: ['prices'] });
    await vi.waitFor(() => expect(second.messagesOfType('cancel')).toHaveLength(1));
  });

  it('after a reconnect, holds a window’s send that requires topics until that window resynced them', async () => {
    const o = owner();
    const resynced: Array<() => void> = [];
    const a = window(o, {
      consumer: 'board',
      delivery: { mode: 'recover-or-resync', resync: () => new Promise<void>((resolve) => resynced.push(resolve)) },
    });
    a.retain();
    a.subscribe(['prices']);
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const first = FakeWebSocket.last();
    await serve(first);
    first.serverSend(reply(['prices']));
    await vi.waitFor(() => expect(resynced).toHaveLength(1));
    resynced[0]();
    await settle();
    a.send('cancel', { id: 9 }, { requires: ['prices'], id: 'cancel-9' });
    await vi.waitFor(() => expect(first.messagesOfType('cancel')).toHaveLength(1));
    // The connection goes before its answer; on the next one the window's
    // prices is not continuous until it resynced it.
    first.serverClose(1006);
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2), { timeout: 2_000 });
    const second = FakeWebSocket.last();
    await serve(second);
    second.serverSend(reply(['prices']));
    await vi.waitFor(() => expect(resynced).toHaveLength(2));
    await settle();
    expect(second.messagesOfType('cancel')).toHaveLength(0);
    resynced[1]();
    await vi.waitFor(() => expect(second.messagesOfType('cancel')).toHaveLength(1));
  });
});

describe('a window, as its application reads it', () => {
  it('refuses a send once its owner is gone for good', async () => {
    const o = owner();
    const w = window(o);
    await vi.waitFor(() => expect(w.bridge.phase).toBe('attached'));
    o.destroy();
    await vi.waitFor(() => expect(['closed', 'revoked']).toContain(w.bridge.phase));
    expect(w.send('save', { n: 1 }, { id: 'after-close' })).toBe('dropped');
    expect(w.kept()).toBe(0);
  });

  it('keeps each of two sends of one id until the owner heard of it', async () => {
    const o = owner();
    const w = window(o);
    w.retain();
    await settle();
    const ws = FakeWebSocket.last();
    await serve(ws);
    await vi.waitFor(() => expect(w.state.getSnapshot().ready).toBe(true));
    w.send('save', { n: 1 }, { id: 'document' });
    w.send('save', { n: 2 }, { id: 'document' });
    await vi.waitFor(() => expect(ws.appTextMessages().length).toBe(2));
    expect(w.kept()).toBe(2);
    ws.serverSend({ c: 'WSE', t: 'ack', p: { s: 1 } });
    await vi.waitFor(() => expect(o.client.kept()).toBe(1));
    await settle();
    expect(w.kept()).toBe(1);
  });

  it('shows the topics it holds in its store as in its snapshot', async () => {
    const o = owner();
    const w = window(o);
    w.retain();
    await settle();
    const ws = FakeWebSocket.last();
    await serve(ws);
    const lease = w.subscribe(['prices']);
    await vi.waitFor(() => expect(ws.messagesOfType('subscription_update').length).toBeGreaterThan(0));
    ws.serverSend(reply(['prices']));
    await vi.waitFor(() => expect(w.state.getSnapshot().topicStates.prices.subscription).toBe('active'));
    expect(w.store.getState().activeTopics).toEqual(['prices']);
    lease.release();
    await vi.waitFor(() => expect(w.store.getState().activeTopics).toEqual([]));
  });
});

describe('a window behind its owner', () => {
  it('hears what became of its send though its queue overflowed', async () => {
    const o = owner({ limits: { queueItems: 4, inFlight: 1, batchItems: 1 } });
    const w = window(o);
    // Another window holds the connection while this one is paused.
    const other = window(o);
    other.retain();
    w.retain();
    await settle();
    const ws = FakeWebSocket.last();
    await serve(ws);
    const lease = w.subscribe(['prices']);
    await vi.waitFor(() => expect(ws.messagesOfType('subscription_update').length).toBeGreaterThan(0));
    ws.serverSend(reply(['prices']));
    await vi.waitFor(() => expect(w.state.getSnapshot().topicStates.prices.subscription).toBe('active'));
    const outcomes: string[] = [];
    w.on('wseDelivered', (d: { ids: string[] }) => outcomes.push(...d.ids));
    w.on('wseNotDelivered', (d: { reason: string; ids: string[] }) => outcomes.push(`${d.reason}:${d.ids}`));
    w.send('order', { n: 1 }, { id: 'kept' });
    await vi.waitFor(() => expect(ws.messagesOfType('order')).toHaveLength(1));
    const s = ws.messagesOfType('order')[0].s as number;
    // Paused, with a batch in hand: what comes waits at the owner, its
    // send's outcome among it.
    w.pause('test');
    ws.serverSend({ c: 'U', t: 'tick', tp: 'prices', p: { n: -1 } });
    await settle();
    ws.serverSend({ c: 'WSE', t: 'ack', p: { s } });
    await vi.waitFor(() => expect(o.client.kept()).toBe(0));
    for (let n = 0; n < 20; n++) ws.serverSend({ c: 'U', t: 'tick', tp: 'prices', p: { n } });
    await settle();
    w.resume('test');
    await vi.waitFor(() => expect(outcomes).toEqual(['kept']));
    expect(w.kept()).toBe(0);
    lease.release();
  });
});

describe('a window let go', () => {
  it('says a send is queued, the owner taking it or saying it did not', async () => {
    const o = owner({ offline: { enabled: true, maxSize: 1 } });
    const w = window(o);
    const refused: unknown[] = [];
    w.on('wseNotDelivered', (d) => refused.push(d));
    w.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    await serve(ws);
    await vi.waitFor(() => expect(w.state.getSnapshot().ready).toBe(true));
    expect(w.send('work', { n: 1 }, { id: 'first' })).toBe('queued');
    await vi.waitFor(() => expect(ws.messagesOfType('work')).toHaveLength(1));
    // The owner's outbox has no room: never said sent.
    expect(w.send('work', { n: 2 }, { id: 'second' })).toBe('queued');
    await vi.waitFor(() => expect(refused).toEqual([{ reason: 'dropped', ids: ['second'] }]));
    expect(ws.messagesOfType('work').map((m) => m.id)).toEqual(['first']);
  });

  it('hears once that what the owner held of it will not go when the owner signs out', async () => {
    const o = owner();
    const w = window(o);
    const refused: unknown[] = [];
    w.on('wseNotDelivered', (d) => refused.push(d));
    await vi.waitFor(() => expect(w.bridge.phase).toBe('attached'));
    w.send('draft', { text: 'hello' }, { id: 'draft-1' });
    await vi.waitFor(() => expect(o.client.kept()).toBe(1));
    o.signOut();
    await vi.waitFor(() => expect(w.bridge.phase).toBe('revoked'));
    await settle();
    expect(refused).toEqual([{ reason: 'cleared', ids: ['draft-1'] }]);
    expect(w.kept()).toBe(0);
  });

  it('hears what the server confirmed, not that it was cleared, when the owner signs out while it is paused', async () => {
    // One batch in hand at a time, one item a batch: the outcome waits at
    // the owner, or in the paused window.
    for (const limits of [{ inFlight: 1, batchItems: 1 }, { inFlight: 2, batchItems: 16 }]) {
      const o = owner({ limits });
      const w = window(o);
      o.client.retain();
      w.retain();
      const delivered: unknown[] = [];
      const refused: unknown[] = [];
      w.on('wseDelivered', (d) => delivered.push(d));
      w.on('wseNotDelivered', (d) => refused.push(d));
      await vi.waitFor(() => expect(FakeWebSocket.instances.length).toBeGreaterThan(0));
      const ws = FakeWebSocket.last();
      await serve(ws);
      await vi.waitFor(() => expect(w.state.getSnapshot().ready).toBe(true));
      w.send('order', { n: 1 }, { id: 'confirmed' });
      w.send('order', { n: 2 }, { id: 'unconfirmed' });
      await vi.waitFor(() => expect(ws.messagesOfType('order')).toHaveLength(2));
      w.pause('background');
      await settle();
      ws.serverSend({ c: 'WSE', t: 'ack', p: { s: ws.messagesOfType('order')[0].s } });
      await vi.waitFor(() => expect(o.client.kept()).toBe(1));
      await settle();
      o.signOut();
      await vi.waitFor(() => expect(w.bridge.phase).toBe('revoked'));
      await settle();
      expect(delivered).toEqual([{ ids: ['confirmed'] }]);
      expect(refused).toEqual([{ reason: 'cleared', ids: ['unconfirmed'] }]);
      expect(w.kept()).toBe(0);
    }
  });

  it('hears what the server confirmed when it signs out while it is paused', async () => {
    const o = owner();
    const w = window(o);
    o.client.retain();
    w.retain();
    const heard: unknown[] = [];
    w.on('wseDelivered', (d) => heard.push(['delivered', d]));
    w.on('wseNotDelivered', (d) => heard.push(['not', d]));
    await vi.waitFor(() => expect(FakeWebSocket.instances.length).toBeGreaterThan(0));
    const ws = FakeWebSocket.last();
    await serve(ws);
    await vi.waitFor(() => expect(w.state.getSnapshot().ready).toBe(true));
    w.send('order', { n: 1 }, { id: 'confirmed' });
    await vi.waitFor(() => expect(ws.messagesOfType('order')).toHaveLength(1));
    w.pause('background');
    await settle();
    ws.serverSend({ c: 'WSE', t: 'ack', p: { s: ws.messagesOfType('order')[0].s } });
    await vi.waitFor(() => expect(o.client.kept()).toBe(0));
    await settle();
    w.signOut();
    await vi.waitFor(() => expect(w.bridge.phase).toBe('revoked'));
    expect(heard).toEqual([['delivered', { ids: ['confirmed'] }]]);
    expect(w.kept()).toBe(0);
  });

  it('signing out itself, hears what the server confirmed that the owner still held for it', async () => {
    // One batch in hand at a time: a message is in hand, the outcome waits
    // at the owner.
    const o = owner({ limits: { inFlight: 1, batchItems: 1 } });
    const w = window(o);
    o.client.retain();
    w.retain();
    const delivered: string[] = [];
    const refused: unknown[] = [];
    const notes: unknown[] = [];
    w.on('wseDelivered', (d) => delivered.push(...d.ids));
    w.on('wseNotDelivered', (d) => refused.push(d));
    w.onMessage('news', (m) => notes.push(m.p));
    await vi.waitFor(() => expect(FakeWebSocket.instances.length).toBeGreaterThan(0));
    const ws = FakeWebSocket.last();
    await serve(ws);
    await vi.waitFor(() => expect(w.state.getSnapshot().ready).toBe(true));
    w.send('order', { n: 1 }, { id: 'confirmed' });
    await vi.waitFor(() => expect(ws.messagesOfType('order')).toHaveLength(1));
    w.pause('background');
    await settle();
    ws.serverSend({ c: 'U', t: 'news', p: { n: 1 }, v: 1 });
    await settle();
    ws.serverSend({ c: 'WSE', t: 'ack', p: { s: ws.messagesOfType('order')[0].s } });
    await vi.waitFor(() => expect(o.client.kept()).toBe(0));
    await settle();
    expect(w.kept()).toBe(1);
    w.signOut();
    // Nothing more goes once it signed out.
    expect(w.send('order', { n: 2 }, { id: 'late' })).toBe('dropped');
    await vi.waitFor(() => expect(w.bridge.phase).not.toBe('attached'));
    await settle();
    expect(delivered).toEqual(['confirmed']);
    expect(refused).toEqual([]);
    expect(notes).toEqual([]);
    expect(w.kept()).toBe(0);
    expect(o.attached).toBe(0);
  });

  it('signing out itself from an owner that does not answer, says what it held cleared in time', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      const { port1, port2 } = new MessageChannel();
      // An owner that takes the window and then answers nothing more.
      const silent = messagePortChannel<OwnerFrame, WindowFrame>(port1 as never);
      silent.listen({
        frame: (f) => {
          if (f.k === 'hello') {
            silent.post({
              k: 'welcome',
              v: BRIDGE_PROTOCOL,
              att: 1,
              limits: DEFAULT_BRIDGE_LIMITS,
              realm: null,
              account: { epoch: 0, user: 'ann' },
              conn: { connection: 'connected', ready: true, paused: [], endpoint: null, lastError: null, storage: 'memory' } as never,
              gen: 1,
            });
          }
        },
        close: () => {},
      });
      const w = createBridgeClient(messagePortChannel<WindowFrame, OwnerFrame>(port2 as never));
      windows.push(w);
      const refused: unknown[] = [];
      w.on('wseNotDelivered', (d) => refused.push(d));
      await vi.waitFor(() => expect(w.bridge.phase).toBe('attached'));
      w.send('order', {}, { id: 'held' });
      w.signOut();
      expect(refused).toEqual([]);
      await vi.advanceTimersByTimeAsync(5_000);
      await vi.waitFor(() => expect(refused).toEqual([{ reason: 'cleared', ids: ['held'] }]));
      expect(w.kept()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it('a send from a listener of the sign-out is dropped at once', async () => {
    const o = owner();
    const w = window(o);
    await vi.waitFor(() => expect(w.bridge.phase).toBe('attached'));
    let result: unknown;
    w.on('wseNotDelivered', (d) => {
      if (d.ids.includes('first')) result = w.send('followup', {}, { id: 'second' });
    });
    w.send('order', {}, { id: 'first' });
    await vi.waitFor(() => expect(o.client.kept()).toBe(1));
    o.signOut();
    await vi.waitFor(() => expect(w.bridge.phase).toBe('revoked'));
    await settle();
    expect(result).toBe('dropped');
    expect(w.kept()).toBe(0);
    // And from one of a sign-out of its own.
    const o2 = owner();
    const w2 = window(o2);
    await vi.waitFor(() => expect(w2.bridge.phase).toBe('attached'));
    let own: unknown;
    w2.on('wseNotDelivered', () => {
      own = w2.send('followup', {}, { id: 'third' });
    });
    w2.send('order', {}, { id: 'first' });
    await vi.waitFor(() => expect(o2.client.kept()).toBe(1));
    w2.signOut();
    await vi.waitFor(() => expect(own).toBe('dropped'));
    expect(w2.kept()).toBe(0);
  });

  it('hands on nothing more of a batch once a listener signed out', async () => {
    const o = owner({ limits: { inFlight: 1, batchItems: 16 } });
    o.client.retain();
    const w = window(o);
    const seen: number[] = [];
    w.onMessage('note', (m) => {
      seen.push((m.p as { n: number }).n);
      if ((m.p as { n: number }).n === 1) w.signOut();
    });
    w.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    await serve(ws);
    // Held while paused: one batch of three when the window is back.
    w.pause('hidden');
    await settle();
    for (const n of [0, 1, 2]) ws.serverSend({ c: 'U', t: 'note', p: { n }, v: 1 });
    await settle();
    w.resume('hidden');
    // Signed out through the owner.
    await vi.waitFor(() => expect(w.bridge.phase).toBe('revoked'));
    await settle();
    expect(seen).toEqual([0, 1]);
  });

  it('signs out through the owner when it signs out before its channel opened', async () => {
    const o = owner();
    const other = window(o);
    await vi.waitFor(() => expect(other.bridge.phase).toBe('attached'));
    let open: (channel: WindowChannel) => void = () => {};
    const late = createBridgeClient(() => new Promise<WindowChannel>((resolve) => (open = resolve)));
    windows.push(late);
    late.signOut();
    const { port1, port2 } = new MessageChannel();
    o.attach(messagePortChannel<OwnerFrame, WindowFrame>(port1 as never));
    open(messagePortChannel<WindowFrame, OwnerFrame>(port2 as never));
    await vi.waitFor(() => expect(other.bridge.phase).toBe('revoked'));
    expect(other.bridge.reason).toBe('signed_out');
  });
});
