// The IndexedDB offline queue of 3.1, as a class of its own: the client no
// longer uses it (its outbox keeps what waits, in IndexedDB too, and takes
// what this queue kept over). Records written by 2.x are read too.

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

  it('takes a database of version 1 over, its records kept, and leases it', async () => {
    await new Promise<void>((resolve, reject) => {
      const open = indexedDB.open('wse_offline_queue', 1);
      open.onupgradeneeded = () => {
        const store = open.result.createObjectStore('messages', { keyPath: 'id' });
        store.createIndex('timestamp', 'timestamp');
        store.createIndex('priority', 'priority');
      };
      open.onsuccess = () => {
        const db = open.result;
        const tx = db.transaction(['messages'], 'readwrite');
        tx.objectStore('messages').put({
          id: 'old-1',
          timestamp: Date.now() - 1_000,
          retries: 0,
          message: { id: 'old-1', t: 'chat', p: { n: 1 } },
        });
        tx.oncomplete = () => {
          db.close();
          resolve();
        };
        tx.onerror = () => reject(tx.error);
      };
      open.onerror = () => reject(open.error);
    });

    const queue = new OfflineQueue(CONFIG);
    await queue.initialize();
    try {
      expect((await queue.getAll()).map((r) => r.id)).toEqual(['old-1']);
      expect(await queue.acquire('a')).toBe(true);
      const other = new OfflineQueue(CONFIG);
      await other.initialize();
      try {
        // Held by another: this one does not replay.
        expect(await other.acquire('b')).toBe(false);
        expect(await other.claim(10)).toEqual([]);
        await queue.release('a');
        expect(await other.acquire('b')).toBe(true);
        expect((await other.claim(10)).map((r) => r.id)).toEqual(['old-1']);
      } finally {
        other.destroy();
      }
    } finally {
      queue.destroy();
    }
  });

  it('reads records in the 2.x layout', async () => {
    const queue = new OfflineQueue(CONFIG);
    await queue.initialize();
    queue.destroy();

    await new Promise<void>((resolve, reject) => {
      const open = indexedDB.open('wse_offline_queue');
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

describe('the offline queue keeps its limits', () => {
  const message = (id: string) => ({ t: 'order', id, p: {}, v: 1 }) as any;

  function putRaw(record: Record<string, unknown>): Promise<void> {
    return new Promise((resolve, reject) => {
      const open = indexedDB.open('wse_offline_queue');
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

