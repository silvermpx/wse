// @vitest-environment happy-dom
//
// An encrypted send is accepted before it is sealed, and goes on the socket
// once the encryption is done. What may still change meanwhile -- the
// connection, the user, whether the topics it requires are continuous -- is
// checked right before the frame is written: a message that cannot go waits
// in the queue as it was (its user, its topics, its time to live), and one
// of another user never goes.

import 'fake-indexeddb/auto';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import {
  ENDPOINT,
  FakeWebSocket,
  installFakeWebSocket,
  resetClientState,
  serverHelloMessage,
  serverKeyExchange,
  serverReadyMessage,
} from './helpers.js';
import { createWSEClient, type WSEClient } from '../core/index.js';
import { SecurityManager } from '../utils/security.js';
import { ConnectionManager } from '../services/ConnectionManager.js';
import { createClientContext } from '../core/context.js';

let clients: WSEClient[] = [];

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
});

afterEach(() => {
  for (const made of clients) made.destroy();
  clients = [];
  resetClientState();
});

/** A resync handler that answers the first round at once and the others when the test says. */
function resyncs() {
  const later: Array<() => void> = [];
  let calls = 0;
  const resync = () => {
    calls++;
    if (calls === 1) return Promise.resolve();
    return new Promise<void>((resolve) => later.push(resolve));
  };
  return { resync, later };
}

function client(options: Parameters<typeof createWSEClient>[0]): WSEClient {
  const made = createWSEClient({
    endpoints: [ENDPOINT],
    offline: { enabled: false },
    reconnection: { baseDelay: 10, maxDelay: 20 },
    security: { encryptionEnabled: true, legacyE2e: true },
    ...options,
  });
  clients.push(made);
  return made;
}

/** The newest socket, opened as `user`'s with its encryption key exchanged. */
async function open(c: WSEClient, user: string) {
  const ws = FakeWebSocket.last();
  ws.serverOpen();
  ws.serverSend(serverReadyMessage({ user_id: user }));
  await vi.waitFor(() => expect(ws.messagesOfType('client_hello')).toHaveLength(1));
  const server = await serverKeyExchange(ws.messagesOfType('client_hello')[0].p.encryption_public_key);
  ws.serverSend(
    serverHelloMessage({ features: { encryption: true, recovery: true }, encryption_public_key: server.publicKeyB64 }),
  );
  await vi.waitFor(() => expect(c.state.getSnapshot().ready).toBe(true));
  const orders = async () => {
    const all = await Promise.all(ws.binaryFrames().map(async (frame) => JSON.parse(await server.decrypt(frame))));
    return all.filter((m) => m.t === 'order').map((m) => m.p);
  };
  return { ws, orders };
}

function subscribed(ws: FakeWebSocket) {
  ws.serverSend({ c: 'WSE', t: 'subscription_update', p: { action: 'subscribe', success: true, success_topics: ['prices'], failed_topics: [] }, v: 1 });
}

const settle = () => new Promise((resolve) => setTimeout(resolve, 300));

it('a send that requires topics waits when they stop being continuous while it is sealed', async () => {
  const { resync, later } = resyncs();
  const c = client({ topics: ['prices'], consumer: 'board', delivery: { mode: 'recover-or-resync', resync } });
  c.retain();
  await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
  const socket = await open(c, 'alice');
  subscribed(socket.ws);
  await vi.waitFor(() => expect(c.state.getSnapshot().topicStates.prices.continuity).toBe('continuous'));

  c.send('order', { id: 42 }, { requires: ['prices'] });
  c.resync(['prices']);
  await settle();
  expect(await socket.orders()).toEqual([]);

  // Resynced: it goes, once.
  later.forEach((resolve) => resolve());
  await vi.waitFor(async () => expect(await socket.orders()).toEqual([{ id: 42 }]));
});

it('a send that requires topics, cut off by a reconnect while it is sealed, waits for them there', async () => {
  const { resync, later } = resyncs();
  const c = client({ topics: ['prices'], consumer: 'board', delivery: { mode: 'recover-or-resync', resync } });
  c.retain();
  await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
  const first = await open(c, 'alice');
  subscribed(first.ws);
  await vi.waitFor(() => expect(c.state.getSnapshot().topicStates.prices.continuity).toBe('continuous'));

  expect(c.send('order', { id: 43 }, { requires: ['prices'] })).toBe('sent');
  first.ws.serverClose(1006);
  await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
  const second = await open(c, 'alice');
  await settle();
  expect(c.state.getSnapshot().topicStates.prices.continuity).not.toBe('continuous');
  expect(await second.orders()).toEqual([]);

  subscribed(second.ws);
  await vi.waitFor(() => expect(later.length).toBeGreaterThan(0));
  later.forEach((resolve) => resolve());
  await vi.waitFor(async () => expect(await second.orders()).toEqual([{ id: 43 }]));
});

it('a send sealed for one user never goes on the next one’s connection', async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => (release = resolve));
  let held = false;
  const encrypt = SecurityManager.prototype.encryptForTransport;
  vi.spyOn(SecurityManager.prototype, 'encryptForTransport').mockImplementation(async function (
    this: SecurityManager,
    data: string,
  ) {
    const frame = await encrypt.call(this, data);
    // Alice's order is sealed, and handed back only after Bob's connection.
    if (data.includes('created_as') && !held) {
      held = true;
      await gate;
    }
    return frame;
  });
  const c = client({ topics: ['prices'] });
  try {
    c.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const alice = await open(c, 'alice');
    expect(c.send('order', { created_as: 'alice' })).toBe('sent');
    await vi.waitFor(() => expect(held).toBe(true));
    alice.ws.serverClose(1006);
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
    const bob = await open(c, 'bob');
    release();
    await settle();
    expect(await bob.orders()).toEqual([]);
    expect(await alice.orders()).toEqual([]);
  } finally {
    release();
  }
});

it('to a server that does not answer, a kept message is delivered once written, not once accepted', async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => (release = resolve));
  let held = false;
  const encrypt = SecurityManager.prototype.encryptForTransport;
  const spy = vi
    .spyOn(SecurityManager.prototype, 'encryptForTransport')
    .mockImplementation(async function (this: SecurityManager, data: string) {
      const frame = await encrypt.call(this, data);
      if (data.includes('held_order') && !held) {
        held = true;
        await gate;
      }
      return frame;
    });
  const c = client({ offline: { enabled: true } });
  const delivered: string[] = [];
  c.on('wseDelivered', (detail: { ids: string[] }) => delivered.push(...detail.ids));
  try {
    c.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const a = await open(c, 'alice');
    c.send('order', { held_order: true }, { id: 'kept' });
    await vi.waitFor(() => expect(held).toBe(true));
    // Being sealed: not written, so not delivered, and still kept.
    expect(delivered).toEqual([]);
    expect(c.kept()).toBe(1);
    // The connection goes before it is written: it goes on the next one.
    a.ws.serverClose(1006);
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
    const b = await open(c, 'alice');
    release();
    await vi.waitFor(() => expect(delivered).toEqual(['kept']));
    expect(await a.orders()).toEqual([]);
    expect(await b.orders()).toEqual([{ held_order: true }]);
    expect(c.kept()).toBe(0);
  } finally {
    release();
    spy.mockRestore();
  }
});

it('goes on sealing what comes after a sealed frame the socket would not take, and says that one undelivered', async () => {
  const ctx = createClientContext();
  const undelivered: string[] = [];
  const manager = new ConnectionManager(['ws://test'], {} as never, () => {}, () => {}, undefined, {
    context: ctx,
    encryptionRequired: true,
    e2e: { legacy: true },
    onUndeliverable: (message) => undelivered.push(message.t),
  });
  const sent: unknown[] = [];
  let fail = true;
  const ws = {
    readyState: 1,
    send: (frame: unknown) => {
      if (fail) {
        fail = false;
        throw new Error('a write error');
      }
      sent.push(frame);
    },
    close: () => {},
  };
  const open = manager as unknown as { ws: unknown; isTransportReady: () => boolean; sealing: number };
  open.ws = ws;
  open.isTransportReady = () => true;
  ctx.security.encryptForTransport = async () => new ArrayBuffer(2);
  expect(manager.trySend({ t: 'first', p: {} })).toBe('sent');
  await vi.waitFor(() => expect(undelivered).toEqual(['first']));
  expect(manager.trySend({ t: 'second', p: {} })).toBe('sent');
  await vi.waitFor(() => expect(sent).toHaveLength(1));
  expect(open.sealing).toBe(0);
  manager.destroy();
});

it('says one outcome of each send to a server that does not answer, when an earlier one could not be sealed', async () => {
  const original = SecurityManager.prototype.encryptForTransport;
  const spy = vi
    .spyOn(SecurityManager.prototype, 'encryptForTransport')
    .mockImplementation(async function (this: SecurityManager, data: string) {
      if (data.includes('fails-first')) throw new Error('a local encryption failure');
      return original.call(this, data);
    });
  try {
    const c = client({ offline: { enabled: true } });
    const delivered: string[] = [];
    c.on('wseDelivered', (d: { ids: string[] }) => delivered.push(...d.ids));
    c.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const socket = await open(c, 'u');
    c.send('order', { n: 1 }, { id: 'fails-first' });
    c.send('order', { n: 2 }, { id: 'second' });
    await settle();
    // Only the second went: it alone is delivered, the first waits.
    expect(await socket.orders()).toEqual([{ n: 2 }]);
    expect(delivered).toEqual(['second']);
    expect(c.kept()).toBe(1);
  } finally {
    spy.mockRestore();
  }
});
