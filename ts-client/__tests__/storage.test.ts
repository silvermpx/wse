// @vitest-environment happy-dom
//
// What a client of its own keeps: the offline queue is durable only for a
// consumer the application names, in a database of its own scope (the
// storage namespace, the consumer and the server's realm); two clients of
// one scope never both replay it -- the second is busy until the first lets
// go -- and a storage that fails says so.

import 'fake-indexeddb/auto';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ENDPOINT, FakeWebSocket, installFakeWebSocket, resetClientState, serverReadyMessage } from './helpers.js';
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

beforeEach(async () => {
  installFakeWebSocket();
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

describe('the durable queue', () => {
  it('stays in memory without a consumer, and says so', async () => {
    const c = client();
    c.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(c.state.getSnapshot().storage).toBe('memory');
  });

  it('is durable for a named consumer, in a database of its scope', async () => {
    const c = client({ consumer: 'checkout' });
    c.retain();
    await vi.waitFor(() => expect(c.state.getSnapshot().storage).toBe('durable'));
    expect(c.send('order', { id: 1 }, { offline: true })).toBe('queued');
    const queue = new OfflineQueue({ ...CONFIG, dbName: SCOPE });
    await queue.initialize();
    try {
      await vi.waitFor(async () => expect((await queue.getAll()).map((r) => r.message.t)).toEqual(['order']));
    } finally {
      queue.destroy();
    }
  });

  it('of one scope is replayed by one client; the other is busy until it lets go', async () => {
    const stored = new OfflineQueue({ ...CONFIG, dbName: SCOPE });
    await stored.initialize();
    expect(await stored.enqueue({ t: 'order', p: { id: 1 }, id: 'order-1' } as any)).toBe(true);
    stored.destroy();

    const first = client({ consumer: 'checkout' });
    first.retain();
    await vi.waitFor(() => expect(first.state.getSnapshot().storage).toBe('durable'));
    const second = client({ consumer: 'checkout' });
    const busy: unknown[] = [];
    second.on('wseStorage', (detail) => busy.push(detail));
    second.retain();
    await vi.waitFor(() => expect(second.state.getSnapshot().storage).toBe('busy'));
    expect(busy).toContainEqual({ state: 'busy' });

    const sockets = [...FakeWebSocket.instances];
    for (const ws of sockets) ws.serverOpen();
    for (const ws of sockets) ws.serverSend(serverReadyMessage());
    await vi.waitFor(() => {
      const orders = sockets.flatMap((ws) => ws.appTextMessages().filter((m) => m.t === 'order'));
      expect(orders).toHaveLength(1);
    });
    expect(sockets[0].appTextMessages().filter((m) => m.t === 'order')).toHaveLength(1);

    // The first lets go: the second takes the queue over.
    first.destroy();
    await vi.waitFor(() => expect(second.state.getSnapshot().storage).toBe('durable'), { timeout: 5_000 });
  });
});

describe('the lease of a scope', () => {
  const orders = (ws: FakeWebSocket) => ws.appTextMessages().filter((m) => m.t === 'order');

  it('taken over, finds what the last holder stored', async () => {
    const first = client({ consumer: 'checkout' });
    first.retain();
    await vi.waitFor(() => expect(first.state.getSnapshot().storage).toBe('durable'));
    const second = client({ consumer: 'checkout' });
    second.retain();
    await vi.waitFor(() => expect(second.state.getSnapshot().storage).toBe('busy'));
    const [, socket] = FakeWebSocket.instances;
    socket.serverOpen();
    socket.serverSend(serverReadyMessage());

    // The holder stores an order while it has no connection, then goes.
    expect(first.send('order', { id: 1 }, { offline: true })).toBe('queued');
    await new Promise((resolve) => setTimeout(resolve, 100));
    first.destroy();
    await vi.waitFor(() => expect(second.state.getSnapshot().storage).toBe('durable'), { timeout: 5_000 });
    await vi.waitFor(() => expect(orders(socket)).toHaveLength(1), { timeout: 5_000 });
  }, 20_000);

  it('held, finds what another client of the scope stored meanwhile', async () => {
    const holder = client({ consumer: 'checkout' });
    holder.retain();
    await vi.waitFor(() => expect(holder.state.getSnapshot().storage).toBe('durable'));
    const socket = FakeWebSocket.last();
    socket.serverOpen();
    socket.serverSend(serverReadyMessage());
    const other = client({ consumer: 'checkout' });
    other.retain();
    await vi.waitFor(() => expect(other.state.getSnapshot().storage).toBe('busy'));
    expect(other.send('order', { id: 2 }, { offline: true })).toBe('queued');
    await vi.waitFor(() => expect(orders(socket)).toHaveLength(1), { timeout: 8_000 });
  }, 20_000);
});

describe('a stored message being taken', () => {
  it('survives a pause in the middle, and goes once after it', async () => {
    const stored = new OfflineQueue({ ...CONFIG, dbName: SCOPE });
    await stored.initialize();
    expect(await stored.enqueue({ t: 'order', p: { id: 1 }, id: 'order-1' } as any)).toBe(true);
    stored.destroy();

    // The taking is held up, after its transaction, until the client paused.
    let go!: () => void;
    const held = new Promise<void>((resolve) => (go = resolve));
    const claim = OfflineQueue.prototype.claim;
    const taking = vi.spyOn(OfflineQueue.prototype, 'claim').mockImplementation(async function (
      this: OfflineQueue,
      limit: number,
    ) {
      const records = await claim.call(this, limit);
      if (records.length > 0) await held;
      return records;
    });

    const c = client({ consumer: 'checkout' });
    c.retain();
    await vi.waitFor(() => expect(c.state.getSnapshot().storage).toBe('durable'));
    const first = FakeWebSocket.last();
    first.serverOpen();
    first.serverSend(serverReadyMessage());
    await vi.waitFor(() => expect(taking).toHaveBeenCalled());
    c.pause('manual');
    go();
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(first.appTextMessages().filter((m) => m.t === 'order')).toEqual([]);
    taking.mockRestore();

    // Still stored.
    const check = new OfflineQueue({ ...CONFIG, dbName: SCOPE });
    await check.initialize();
    expect((await check.getAll()).map((r) => r.id)).toEqual(['order-1']);
    check.destroy();

    // Back: it goes, once.
    c.resume('manual');
    await vi.waitFor(() => expect(FakeWebSocket.last()).not.toBe(first));
    const second = FakeWebSocket.last();
    second.serverOpen();
    second.serverSend(serverReadyMessage());
    await vi.waitFor(() => expect(second.appTextMessages().filter((m) => m.t === 'order')).toHaveLength(1));
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(second.appTextMessages().filter((m) => m.t === 'order')).toHaveLength(1);
  });
});

describe('clients of one namespace in two realms', () => {
  it('keep their offline queues and their users apart', async () => {
    const queueOf = (realm: string) => `wse:shop:alice:checkout@${realm}`;
    await dropDb(queueOf('a'));
    await dropDb(queueOf('b'));
    const open = async (realm: string, user: string | null) => {
      const c = client({ consumer: 'checkout', realm });
      c.retain();
      await vi.waitFor(() => expect(c.state.getSnapshot().storage).toBe('durable'));
      if (user !== null) {
        const ws = FakeWebSocket.last();
        ws.serverOpen();
        ws.serverSend(serverReadyMessage({ user_id: user }));
        await vi.waitFor(() => expect(c.state.getSnapshot().ready).toBe(true));
      }
      return c;
    };
    try {
      (await open('a', 'alice')).destroy();
      (await open('b', 'bob')).destroy();
      // Offline in realm a: the order is written for a's user, not b's.
      const again = await open('a', null);
      expect(again.send('order', { id: 7 }, { offline: true })).toBe('queued');
      const stored = new OfflineQueue({ ...CONFIG, dbName: queueOf('a') });
      await stored.initialize();
      await vi.waitFor(async () => expect(await stored.getAll()).toHaveLength(1));
      expect((await stored.getAll())[0].owner).toBe('alice');
      stored.destroy();
      const ws = FakeWebSocket.last();
      ws.serverOpen();
      ws.serverSend(serverReadyMessage({ user_id: 'alice' }));
      await vi.waitFor(() => expect(ws.appTextMessages().filter((m) => m.t === 'order')).toHaveLength(1));
    } finally {
      await dropDb(queueOf('a'));
      await dropDb(queueOf('b'));
      localStorage.clear();
    }
  });
});

describe('a stored message that went', () => {
  it('and could not be removed is not sent again, and the failure is said', async () => {
    const stored = new OfflineQueue({ ...CONFIG, dbName: SCOPE });
    await stored.initialize();
    expect(await stored.enqueue({ t: 'order', p: { id: 1 }, id: 'order-1' } as any)).toBe(true);
    stored.destroy();

    // The transaction of its first removal is cut off.
    let cut = false;
    const remove = IDBObjectStore.prototype.delete;
    vi.spyOn(IDBObjectStore.prototype, 'delete').mockImplementation(function (this: IDBObjectStore, key) {
      const request = remove.call(this, key);
      if (key === 'order-1' && !cut) {
        cut = true;
        this.transaction.abort();
      }
      return request;
    });

    const c = client({ consumer: 'checkout' });
    const errors: any[] = [];
    c.on('wseStorageError', (detail) => errors.push(detail));
    c.retain();
    await vi.waitFor(() => expect(c.state.getSnapshot().storage).toBe('durable'));
    const ws = FakeWebSocket.last();
    ws.serverOpen();
    ws.serverSend(serverReadyMessage());
    await vi.waitFor(() => expect(errors).toMatchObject([{ operation: 'remove' }]));
    // Removed when tried again, a moment later; it went once.
    await vi.waitFor(
      async () => {
        const check = new OfflineQueue({ ...CONFIG, dbName: SCOPE });
        await check.initialize();
        const left = await check.getAll();
        check.destroy();
        expect(left).toEqual([]);
      },
      { timeout: 5_000, interval: 200 },
    );
    expect(ws.appTextMessages().filter((m) => m.t === 'order')).toHaveLength(1);
  });

  it('and could not be removed is not sent again after a pause either', async () => {
    const stored = new OfflineQueue({ ...CONFIG, dbName: SCOPE });
    await stored.initialize();
    expect(await stored.enqueue({ t: 'order', p: { id: 1 }, id: 'order-1' } as any)).toBe(true);
    stored.destroy();

    // Removal fails until the test lets it through.
    let failing = true;
    const remove = OfflineQueue.prototype.remove;
    vi.spyOn(OfflineQueue.prototype, 'remove').mockImplementation(async function (this: OfflineQueue, ids: string[]) {
      if (failing) throw new DOMException('the write was cut off', 'AbortError');
      return remove.call(this, ids);
    });

    const c = client({ consumer: 'checkout' });
    c.retain();
    await vi.waitFor(() => expect(c.state.getSnapshot().storage).toBe('durable'));
    const first = FakeWebSocket.last();
    first.serverOpen();
    first.serverSend(serverReadyMessage());
    await vi.waitFor(() => expect(first.appTextMessages().filter((m) => m.t === 'order')).toHaveLength(1));

    c.pause('manual');
    c.resume('manual');
    await vi.waitFor(() => expect(FakeWebSocket.last()).not.toBe(first));
    const second = FakeWebSocket.last();
    second.serverOpen();
    second.serverSend(serverReadyMessage());
    await new Promise((resolve) => setTimeout(resolve, 100));
    failing = false;
    await vi.waitFor(
      async () => {
        const check = new OfflineQueue({ ...CONFIG, dbName: SCOPE });
        await check.initialize();
        const left = await check.getAll();
        check.destroy();
        expect(left).toEqual([]);
      },
      { timeout: 5_000, interval: 200 },
    );
    expect(second.appTextMessages().filter((m) => m.t === 'order')).toEqual([]);
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
