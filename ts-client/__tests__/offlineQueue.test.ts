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
import { acquireSession, getCurrentSession, makeSessionKey, wseActions } from '../services/WSESession.js';

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
        tx.objectStore('messages').put({ id: 'legacy-1', t: 'chat', p: { n: 1 }, pri: 8, v: 1, timestamp: 1, retries: 0 });
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
