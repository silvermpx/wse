// IndexedDB offline queue: messages persisted while disconnected are
// replayed after the next server_ready, oldest first, and only the ones that
// actually went out are deleted. Records written by 2.x (WSMessage + 'pri',
// no 'priority' field) are replayed too.

import 'fake-indexeddb/auto';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ENDPOINT,
  FakeWebSocket,
  installFakeWebSocket,
  resetClientState,
  serverReadyMessage,
} from './helpers.js';
import { OfflineQueue } from '../services/OfflineQueue.js';
import {
  acquireSession,
  destroyCurrentSession,
  getCurrentSession,
  makeSessionKey,
  wseActions,
} from '../services/WSESession.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import { ConnectionState } from '../types.js';

const CONFIG = { enabled: true, maxSize: 100, maxAge: 3_600_000, persistToStorage: true };

function deleteDb(): Promise<void> {
  return new Promise((resolve) => {
    const req = indexedDB.deleteDatabase('wse_offline_queue');
    req.onsuccess = () => resolve();
    req.onerror = () => resolve();
    req.onblocked = () => resolve();
  });
}

beforeEach(async () => {
  installFakeWebSocket();
  resetClientState();
  await deleteDb();
});

afterEach(async () => {
  resetClientState();
  vi.unstubAllGlobals();
  await deleteDb();
});

describe('OfflineQueue', () => {
  it('reads records oldest first and removes only the given ids', async () => {
    const queue = new OfflineQueue(CONFIG);
    await queue.initialize();
    for (let n = 1; n <= 4; n++) {
      expect(await queue.enqueue({ id: `m${n}`, t: 'chat', p: { n }, pri: n % 2 ? 3 : 8, cid: `c${n}` })).toBe(true);
    }

    const all = await queue.getAll();
    expect(all.map((r) => r.message.p.n)).toEqual([1, 2, 3, 4]);
    expect(all[0].message.cid).toBe('c1');

    await queue.remove(['m1', 'm3']);
    expect((await queue.getAll()).map((r) => r.id)).toEqual(['m2', 'm4']);
    expect(queue.getStats().size).toBe(2);
    queue.destroy();
  });

  it('reports a full queue instead of silently discarding', async () => {
    const queue = new OfflineQueue({ ...CONFIG, maxSize: 2 });
    await queue.initialize();
    expect(await queue.enqueue({ id: 'a', t: 'x', p: {} })).toBe(true);
    expect(await queue.enqueue({ id: 'b', t: 'x', p: {} })).toBe(true);
    expect(await queue.enqueue({ id: 'c', t: 'x', p: {} })).toBe(false);
    queue.destroy();
  });

  it('reads records in the 2.x layout', async () => {
    const queue = new OfflineQueue(CONFIG);
    await queue.initialize();
    queue.destroy();

    await new Promise<void>((resolve, reject) => {
      const open = indexedDB.open('wse_offline_queue', 1);
      open.onsuccess = () => {
        const db = open.result;
        const tx = db.transaction(['messages'], 'readwrite');
        tx.objectStore('messages').put({ id: 'legacy-1', t: 'chat', p: { n: 1 }, pri: 8, v: 1, timestamp: Date.now() - 1_000, retries: 0 });
        tx.oncomplete = () => {
          db.close();
          resolve();
        };
        tx.onerror = () => reject(tx.error);
      };
      open.onerror = () => reject(open.error);
    });

    const reopened = new OfflineQueue(CONFIG);
    await reopened.initialize();
    const all = await reopened.getAll();
    expect(all).toHaveLength(1);
    expect(all[0].message).toEqual({ id: 'legacy-1', t: 'chat', p: { n: 1 }, pri: 8, v: 1 });
    reopened.destroy();
  });
});

describe('offline replay through the session', () => {
  it('persists {offline: true} sends while disconnected and replays them after server_ready', async () => {
    acquireSession({
      key: makeSessionKey('cookie', [ENDPOINT]),
      auth: 'cookie',
      initialTopics: [],
      config: { endpoints: [ENDPOINT] },
    });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    await vi.waitFor(() => expect(getCurrentSession()!.offlineQueue?.isAvailable()).toBe(true));

    expect(wseActions.sendMessage('chat', { n: 1 }, { offline: true, correlation_id: 'k1' })).toBe('queued');
    expect(wseActions.sendMessage('chat', { n: 2 }, { offline: true })).toBe('queued');
    await vi.waitFor(() => expect(getCurrentSession()!.getQueueSize()).toBe(2));

    const ws = FakeWebSocket.last();
    ws.serverOpen();
    ws.serverSend(serverReadyMessage());

    await vi.waitFor(() => {
      const chats = ws.appTextMessages().filter((m) => m.t === 'chat');
      expect(chats.map((m) => m.p.n)).toEqual([1, 2]);
      expect(chats[0].cid).toBe('k1');
    });
    await vi.waitFor(() => expect(getCurrentSession()!.offlineQueue!.getStats().size).toBe(0));
    expect(await getCurrentSession()!.offlineQueue!.getAll()).toEqual([]);
  });
});

describe('offline messages belong to their user', () => {
  function start() {
    acquireSession({
      key: makeSessionKey('cookie', [ENDPOINT]),
      auth: 'cookie',
      initialTopics: [],
      config: { endpoints: [ENDPOINT] },
    });
  }

  it("never replays one user's messages on another user's connection", async () => {
    // Alice connects once, then goes offline and sends.
    start();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    await vi.waitFor(() => expect(getCurrentSession()!.offlineQueue?.isAvailable()).toBe(true));
    const first = FakeWebSocket.last();
    first.serverOpen();
    first.serverSend(serverReadyMessage({ user_id: 'alice' }));
    await vi.waitFor(() => expect(useWSEStore.getState().connectionState).toBe(ConnectionState.CONNECTED));
    first.serverClose(1006);
    await vi.waitFor(() => expect(useWSEStore.getState().connectionState).not.toBe(ConnectionState.CONNECTED));
    expect(wseActions.sendMessage('transfer', { to: 'mallory' }, { offline: true })).toBe('queued');
    await vi.waitFor(() => expect(getCurrentSession()!.getQueueSize()).toBe(1));

    // A page reload: the module state is gone, IndexedDB and storage stay.
    destroyCurrentSession({ clearUserData: false });
    FakeWebSocket.reset();

    // Bob's connection on the same browser does not send it.
    start();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    const bob = FakeWebSocket.last();
    bob.serverOpen();
    bob.serverSend(serverReadyMessage({ user_id: 'bob' }));
    await vi.waitFor(() => expect(getCurrentSession()!.offlineQueue!.getStats().size).toBe(0));
    expect(bob.appTextMessages().filter((m) => m.t === 'transfer')).toEqual([]);
  });

  it("never sends one user's message that a full queue handed back after another took the connection", async () => {
    acquireSession({
      key: makeSessionKey('cookie', [ENDPOINT]),
      auth: 'cookie',
      initialTopics: [],
      config: { endpoints: [ENDPOINT], offline: { enabled: true, maxSize: 1 } },
    });
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(1));
    await vi.waitFor(() => expect(getCurrentSession()!.offlineQueue?.isAvailable()).toBe(true));
    const first = FakeWebSocket.last();
    first.serverOpen();
    first.serverSend(serverReadyMessage({ user_id: 'alice' }));
    await vi.waitFor(() => expect(useWSEStore.getState().connectionState).toBe(ConnectionState.CONNECTED));
    first.serverClose(1006);
    await vi.waitFor(() => expect(useWSEStore.getState().connectionState).not.toBe(ConnectionState.CONNECTED));
    // Alice fills the queue while offline.
    expect(wseActions.sendMessage('transfer', { n: 1 }, { offline: true })).toBe('queued');
    await vi.waitFor(() => expect(getCurrentSession()!.offlineQueue!.getStats().size).toBe(1));

    // The next connection opens; Alice sends once more, and the server takes
    // the connection for Bob before her write is refused by the full queue.
    await vi.waitFor(() => expect(FakeWebSocket.instances.length).toBeGreaterThan(1), { timeout: 5_000 });
    const bob = FakeWebSocket.last();
    bob.serverOpen();
    expect(wseActions.sendMessage('transfer', { n: 2 }, { offline: true })).toBe('queued');
    bob.serverSend(serverReadyMessage({ user_id: 'bob' }));

    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(bob.appTextMessages().filter((m) => m.t === 'transfer')).toEqual([]);
  });
});

describe('the offline queue keeps its limits', () => {
  const message = (id: string) => ({ t: 'order', id, p: {}, v: 1 }) as any;

  function putRaw(record: Record<string, unknown>): Promise<void> {
    return new Promise((resolve, reject) => {
      const open = indexedDB.open('wse_offline_queue', 1);
      open.onerror = () => reject(open.error);
      open.onsuccess = () => {
        const db = open.result;
        const tx = db.transaction(['messages'], 'readwrite');
        tx.objectStore('messages').put(record);
        tx.oncomplete = () => {
          db.close();
          resolve();
        };
        tx.onerror = () => reject(tx.error);
      };
    });
  }

  it('never hands out a message older than maxAge, even without a new enqueue', async () => {
    const writer = new OfflineQueue(CONFIG);
    await writer.initialize();
    writer.destroy();
    await putRaw({ id: 'old', timestamp: Date.now() - 60_061, retries: 0, message: message('old') });

    // Reopened with a one-second limit, and read without enqueueing first.
    const reader = new OfflineQueue({ ...CONFIG, maxAge: 1_000 });
    try {
      await reader.initialize();
      expect((await reader.getAll()).map((r) => r.id)).toEqual([]);
    } finally {
      reader.destroy();
    }
  });

  it('holds no more than maxSize under concurrent enqueues', async () => {
    const queue = new OfflineQueue({ ...CONFIG, maxSize: 1 });
    try {
      await queue.initialize();
      const stored = await Promise.all(Array.from({ length: 10 }, (_, i) => queue.enqueue(message(`m${i}`))));
      expect(stored.filter(Boolean)).toHaveLength(1);
      expect(await queue.getAll()).toHaveLength(1);
      // The id already there may still be written again (an update).
      const kept = (await queue.getAll())[0].id;
      expect(await queue.enqueue(message(kept))).toBe(true);
      expect(await queue.getAll()).toHaveLength(1);
    } finally {
      queue.destroy();
    }
  });
});

describe('two clients of one queue', () => {
  const DB = 'wse:shop:alice';

  function dropDb(): Promise<void> {
    return new Promise((resolve) => {
      const req = indexedDB.deleteDatabase(DB);
      req.onsuccess = () => resolve();
      req.onerror = () => resolve();
      req.onblocked = () => resolve();
    });
  }

  afterEach(dropDb);

  it('send a stored message once, whichever of them connects first', async () => {
    const stored = new OfflineQueue({ ...CONFIG, dbName: DB });
    await stored.initialize();
    expect(await stored.enqueue({ t: 'order', p: { id: 1 }, id: 'order-1' } as any)).toBe(true);
    stored.destroy();

    // Two tabs of one application and user: the same storage namespace.
    const { createWSEClient } = await import('../core/index.js');
    const tabs = [0, 1].map(() => createWSEClient({ endpoints: [ENDPOINT], storage: 'shop:alice' }));
    for (const tab of tabs) tab.retain();
    await vi.waitFor(() => expect(FakeWebSocket.instances).toHaveLength(2));
    const sockets = [...FakeWebSocket.instances];
    await new Promise((resolve) => setTimeout(resolve, 50));
    for (const ws of sockets) ws.serverOpen();
    for (const ws of sockets) ws.serverSend(serverReadyMessage());
    await vi.waitFor(() => {
      const orders = sockets.flatMap((ws) => ws.appTextMessages().filter((m) => m.t === 'order'));
      expect(orders.length).toBeGreaterThan(0);
    });
    await new Promise((resolve) => setTimeout(resolve, 200));
    const orders = sockets.flatMap((ws) => ws.appTextMessages().filter((m) => m.t === 'order'));
    expect(orders).toHaveLength(1);
    for (const tab of tabs) tab.destroy();
  });
});
