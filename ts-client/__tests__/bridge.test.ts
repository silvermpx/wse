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
