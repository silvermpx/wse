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
