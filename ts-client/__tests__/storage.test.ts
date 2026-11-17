// @vitest-environment happy-dom
//
// What a client of its own keeps: its outbox is durable only for a consumer
// the application names, in an IndexedDB database of its own scope (the
// storage namespace, the consumer and the server's realm). Each tab keeps a
// producer of its own, held by a Web Lock; a page that opens takes over what
// a closed tab left. A storage that fails says so.

import 'fake-indexeddb/auto';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ENDPOINT,
  FakeWebSocket,
  installFakeLocks,
  installFakeWebSocket,
  resetClientState,
  serverReadyMessage,
} from './helpers.js';
import { OfflineQueue } from '../services/OfflineQueue.js';
import { createWSEClient, type WSEClient } from '../core/index.js';

const SCOPE = `wse:shop:alice:checkout@${ENDPOINT}`;
const CONFIG = { enabled: true, maxSize: 100, maxAge: 3_600_000, persistToStorage: true };

let clients: WSEClient[] = [];

function client(options: Parameters<typeof createWSEClient>[0] = {}): WSEClient {
  const made = createWSEClient({ endpoints: [ENDPOINT], storage: 'shop:alice', ...options });
  clients.push(made);
  return made;
}

function dropDb(name: string): Promise<void> {
  return new Promise((resolve) => {
    const req = indexedDB.deleteDatabase(name);
    req.onsuccess = () => resolve();
    req.onerror = () => resolve();
    req.onblocked = () => resolve();
  });
}

/** What the scope's database holds: each producer's messages, by id. */
async function stored(name = SCOPE): Promise<Record<string, string[]>> {
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
  try {
    if (!db.objectStoreNames.contains('outbox')) return {};
    const rows = await new Promise<any[]>((resolve, reject) => {
      const request = db.transaction(['outbox']).objectStore('outbox').getAll();
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    const out: Record<string, string[]> = {};
    for (const row of rows) (out[row.producer] ??= []).push(row.id);
    return out;
  } finally {
    db.close();
  }
}

/** The server of 3.2 took the connection, answers for numbered messages, and holds nothing of this producer yet. */
function answering(ws: FakeWebSocket, user = 'user-1', acked = 0): void {
  ws.serverOpen();
  ws.serverSend(serverReadyMessage({ user_id: user, features: { recovery: true, encryption: false, acks: true } }));
  ws.serverSend({ c: 'WSE', t: 'outbox', p: { acked, node: 'n1' } });
}

const orders = (ws: FakeWebSocket) => ws.appTextMessages().filter((m) => m.t === 'order');
const producerOf = (ws: FakeWebSocket) => ws.messagesOfType('client_hello')[0]?.p?.outbox?.producer as string;

beforeEach(async () => {
  installFakeWebSocket();
  installFakeLocks();
  resetClientState();
  await dropDb(SCOPE);
});

afterEach(async () => {
  for (const made of clients) made.destroy();
  clients = [];
  resetClientState();
  vi.unstubAllGlobals();
  await dropDb(SCOPE);
});

describe('the durable outbox', () => {
  it('stays in memory without a consumer, and says so', async () => {
    const c = client();
    c.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(c.state.getSnapshot().storage).toBe('memory');
  });

  it('keeps each scope apart, whatever its parts hold of the separators', async () => {
    const a = client({ storage: 'collision:alice', consumer: 'checkout', realm: 'prod' });
    await vi.waitFor(() => expect(a.state.getSnapshot().storage).toBe('durable'));
    expect(a.send('private', { secret: 'alice' }, { id: 'a' })).toBe('queued');
    await vi.waitFor(async () => expect(Object.values(await stored('wse:collision:alice:checkout@prod'))).toEqual([['a']]));
    a.destroy();
    const b = client({ storage: 'collision', consumer: 'alice:checkout', realm: 'prod' });
    await vi.waitFor(() => expect(b.state.getSnapshot().storage).toBe('durable'));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(b.kept()).toBe(0);
    b.destroy();
    await dropDb('wse:collision:alice:checkout@prod');
  });

  it('keeps what was sent for the next client of the scope, until the server took it', async () => {
    const first = client({ consumer: 'checkout' });
    await vi.waitFor(() => expect(first.state.getSnapshot().storage).toBe('durable'));
    expect(first.send('order', { n: 1 }, { id: 'order-1' })).toBe('queued');
    await vi.waitFor(async () => expect(Object.values(await stored())).toEqual([['order-1']]));
    first.destroy();

    const second = client({ consumer: 'checkout' });
    await vi.waitFor(() => expect(second.kept()).toBe(1));
    expect(second.state.getSnapshot().storage).toBe('durable');
    const delivered: string[] = [];
    second.on('wseDelivered', (detail) => delivered.push(...detail.ids));
    second.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    answering(ws);
    await vi.waitFor(() => expect(orders(ws)).toHaveLength(1));
    expect(orders(ws)[0]).toMatchObject({ s: 1, id: 'order-1' });
    expect(Object.keys(await stored())).toEqual([producerOf(ws)]);
    ws.serverSend({ c: 'WSE', t: 'ack', p: { s: 1 } });
    await vi.waitFor(() => expect(delivered).toEqual(['order-1']));
    await vi.waitFor(async () => expect(await stored()).toEqual({}));
    expect(second.kept()).toBe(0);
  });

  it('of one scope keeps a producer for each client, and each sends its own on its connection', async () => {
    const a = client({ consumer: 'checkout' });
    const b = client({ consumer: 'checkout' });
    await vi.waitFor(() => expect([a, b].map((c) => c.state.getSnapshot().storage)).toEqual(['durable', 'durable']));
    a.send('order', { n: 1 }, { id: 'from-a' });
    b.send('order', { n: 2 }, { id: 'from-b' });
    a.retain();
    b.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
    const sockets = [...FakeWebSocket.instances];
    for (const ws of sockets) answering(ws);
    await vi.waitFor(() => expect(sockets.map((ws) => orders(ws).length)).toEqual([1, 1]));
    expect(new Set(sockets.map(producerOf)).size).toBe(2);
    expect(sockets.flatMap((ws) => orders(ws).map((m) => m.id)).sort()).toEqual(['from-a', 'from-b']);
    // In the database, each under the producer whose connection sent it.
    const rows = await stored();
    for (const ws of sockets) expect(rows[producerOf(ws)]).toEqual([orders(ws)[0].id]);
  });

  it('a client that stays open does not take what another left; the next to open does', async () => {
    const open = client({ consumer: 'checkout' });
    await vi.waitFor(() => expect(open.state.getSnapshot().storage).toBe('durable'));
    const gone = client({ consumer: 'checkout' });
    await vi.waitFor(() => expect(gone.state.getSnapshot().storage).toBe('durable'));
    gone.send('order', { n: 1 }, { id: 'left' });
    gone.destroy();
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(open.kept()).toBe(0);
    const next = client({ consumer: 'checkout' });
    await vi.waitFor(() => expect(next.kept()).toBe(1));
  });

  it('goes with the page and reads the database again when the page is back', async () => {
    const page = client({ consumer: 'checkout' });
    page.retain();
    await vi.waitFor(() => expect(page.state.getSnapshot().storage).toBe('durable'));
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const before = FakeWebSocket.last();
    page.send('order', { n: 1 }, { id: 'kept' });
    await vi.waitFor(async () => expect(Object.values(await stored())).toEqual([['kept']]));

    // Into the back/forward cache: its producer is free, and another page takes it over.
    window.dispatchEvent(new Event('pagehide'));
    const other = client({ consumer: 'checkout' });
    await vi.waitFor(() => expect(other.kept()).toBe(1));

    // Back: the order is the other page's now, and goes on its connection only.
    window.dispatchEvent(new Event('pageshow'));
    await vi.waitFor(() => expect(FakeWebSocket.instances.length).toBeGreaterThan(1));
    const after = FakeWebSocket.last();
    expect(after).not.toBe(before);
    answering(after);
    await vi.waitFor(() => expect(page.state.getSnapshot().ready).toBe(true));
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(page.kept()).toBe(0);
    expect(orders(after)).toEqual([]);
    expect(producerOf(after)).not.toBe(producerOf(before));
  });

  it('drops another user’s messages, and keeps two realms apart', async () => {
    const scopeOf = (realm: string) => `wse:shop:alice:checkout@${realm}`;
    await dropDb(scopeOf('a'));
    await dropDb(scopeOf('b'));
    try {
      const alice = client({ consumer: 'checkout', realm: 'a' });
      alice.retain();
      await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
      answering(FakeWebSocket.last(), 'alice');
      await vi.waitFor(() => expect(alice.state.getSnapshot().ready).toBe(true));
      alice.pause('manual');
      await vi.waitFor(() => expect(alice.state.getSnapshot().ready).toBe(false));
      alice.send('order', { n: 1 }, { id: 'alices' });
      const elsewhere = client({ consumer: 'checkout', realm: 'b' });
      await vi.waitFor(() => expect(elsewhere.state.getSnapshot().storage).toBe('durable'));
      elsewhere.send('order', { n: 2 }, { id: 'in-b' });
      await vi.waitFor(async () => expect(Object.values(await stored(scopeOf('a')))).toEqual([['alices']]));
      await vi.waitFor(async () => expect(Object.values(await stored(scopeOf('b')))).toEqual([['in-b']]));
      alice.destroy();

      // Bob's page in realm a: alice's order is not his to send.
      const bob = client({ consumer: 'checkout', realm: 'a' });
      const dropped: unknown[] = [];
      bob.on('wseNotDelivered', (detail) => dropped.push(detail));
      await vi.waitFor(() => expect(bob.kept()).toBe(1));
      bob.retain();
      await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
      const ws = FakeWebSocket.last();
      answering(ws, 'bob');
      await vi.waitFor(() => expect(dropped).toEqual([{ reason: 'user_changed', ids: ['alices'] }]));
      expect(orders(ws)).toEqual([]);
      await vi.waitFor(async () => expect(await stored(scopeOf('a'))).toEqual({}));
    } finally {
      await dropDb(scopeOf('a'));
      await dropDb(scopeOf('b'));
      localStorage.clear();
    }
  });

  it('takes over what the queue of 3.1 kept, its user’s alone', async () => {
    const old = new OfflineQueue({ ...CONFIG, dbName: SCOPE });
    await old.initialize();
    expect(await old.enqueue({ t: 'order', p: { n: 1 }, id: 'from-3.1' } as any, undefined, 'user-1')).toBe(true);
    old.destroy();

    const c = client({ consumer: 'checkout' });
    await vi.waitFor(() => expect(c.kept()).toBe(1));
    c.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const ws = FakeWebSocket.last();
    answering(ws, 'user-1');
    await vi.waitFor(() => expect(orders(ws)).toHaveLength(1));
    expect(orders(ws)[0]).toMatchObject({ s: 1, id: 'from-3.1', p: { n: 1 } });
  });
});

describe('a storage that fails', () => {
  it('says so when the database cannot be opened', async () => {
    vi.spyOn(indexedDB, 'open').mockImplementation(() => {
      throw new DOMException('no database in this mode', 'InvalidStateError');
    });
    const c = client({ consumer: 'checkout' });
    const errors: any[] = [];
    c.on('wseStorageError', (detail) => errors.push(detail));
    c.retain();
    await vi.waitFor(() => expect(c.state.getSnapshot().storage).toBe('unavailable'));
    expect(errors).toMatchObject([{ operation: 'open', name: 'InvalidStateError', quota: false }]);
    // In memory, as before.
    expect(c.send('order', { n: 1 })).toBe('queued');
    expect(c.kept()).toBe(1);
  });

  it('says so without Web Locks (a page without HTTPS), and keeps what waits in memory', async () => {
    vi.stubGlobal('navigator', {});
    const c = client({ consumer: 'checkout' });
    const errors: any[] = [];
    c.on('wseStorageError', (detail) => errors.push(detail));
    await vi.waitFor(() => expect(c.state.getSnapshot().storage).toBe('unavailable'));
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({ operation: 'open' });
    expect(String(errors[0].message)).toContain('Web Locks');
  });

  it('says so when the database is full, and writes it whole once it can', async () => {
    const c = client({ consumer: 'checkout' });
    const errors: any[] = [];
    c.on('wseStorageError', (detail) => errors.push(detail));
    await vi.waitFor(() => expect(c.state.getSnapshot().storage).toBe('durable'));
    const put = IDBObjectStore.prototype.put;
    const full = vi.spyOn(IDBObjectStore.prototype, 'put').mockImplementation(function (this: IDBObjectStore, ...args) {
      if (this.name === 'outbox') throw new DOMException('full', 'QuotaExceededError');
      return put.apply(this, args as Parameters<typeof put>);
    });
    c.send('order', { n: 1 }, { id: 'first' });
    await vi.waitFor(() => expect(errors).toMatchObject([{ operation: 'write', quota: true }]));
    full.mockRestore();
    // A second later the outbox writes the database anew, with both.
    await new Promise((resolve) => setTimeout(resolve, 1_100));
    c.send('order', { n: 2 }, { id: 'second' });
    await vi.waitFor(async () => expect(Object.values(await stored())).toEqual([['first', 'second']]), {
      timeout: 3_000,
    });
  });

  it('says so when the page storage is full', async () => {
    vi.spyOn(localStorage, 'setItem').mockImplementation(() => {
      throw new DOMException('full', 'QuotaExceededError');
    });
    const c = client({ consumer: 'checkout' });
    const errors: any[] = [];
    c.on('wseStorageError', (detail) => errors.push(detail));
    c.retain();
    c.subscribe(['news']);
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    FakeWebSocket.last().serverOpen();
    FakeWebSocket.last().serverSend(serverReadyMessage({ user_id: 'alice' }));
    await vi.waitFor(() => expect(errors.length).toBeGreaterThan(0));
    expect(errors[0]).toMatchObject({ operation: 'write', quota: true });
  });
});
