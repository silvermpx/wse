// The outbox in IndexedDB, against fake-indexeddb and Web Locks as a
// browser keeps them: what a page takes over is what the store holds once
// the page holds its lock.

import 'fake-indexeddb/auto';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { indexedDbOutbox } from '../services/IndexedDbOutbox';
import { noOutcomes, Outbox } from '../services/Outbox';
import { installFakeLocks } from './helpers';

const limits = { maxMessages: 100, maxBytes: 100_000, maxAge: 3_600_000 };

const later = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('the outbox in IndexedDB', () => {
  it('takes a producer over as it stands once its lock is held, with the user it was given meanwhile', async () => {
    installFakeLocks();
    const name = `handover-${Math.random()}`;
    const a = new Outbox(limits, { store: indexedDbOutbox(name) });
    await a.opened;
    const message = { t: 'order', p: {}, id: 'alice-command' };
    a.add(message, JSON.stringify(message), undefined, Date.now());
    await later(20);
    const sent: string[] = [];
    const hand = (outbox: Outbox, who: string) =>
      outbox.due(Date.now(), noOutcomes(), () => {
        sent.push(who);
        return true;
      });
    // While the next page waits for the lock, this one connects as Alice,
    // sends what it holds, and closes.
    const locks = navigator.locks as unknown as { request: (...args: unknown[]) => Promise<unknown> };
    const request = locks.request.bind(locks);
    let raced = false;
    locks.request = async (...args: unknown[]) => {
      if (!raced) {
        raced = true;
        a.connected('alice', Date.now());
        a.serverAnswers(true);
        a.answeredWith(0, [], 'n1');
        hand(a, 'alice');
        await later(30);
        hand(a, 'alice');
        a.close();
        await later(30);
      }
      return request(...args);
    };
    const b = new Outbox(limits, { store: indexedDbOutbox(name) });
    await b.opened;
    const changed = b.connected('bob', Date.now());
    b.serverAnswers(true);
    b.answeredWith(0, [], 'n1');
    hand(b, 'bob');
    await later(30);
    hand(b, 'bob');
    expect(raced).toBe(true);
    // Alice's command went on Alice's connection, and never as Bob's.
    expect(sent).toEqual(['alice']);
    expect(changed.notDelivered.get('user_changed')).toEqual(['alice-command']);
    b.close();
  });
});

describe('the queue of 3.1, in IndexedDB', () => {
  it('is taken over whole, the rows 2.x wrote too, in the order they were queued', async () => {
    installFakeLocks();
    const name = `legacy-${Math.random()}`;
    // 3.1's database: rows of its own, and rows 2.x wrote (the message itself, with its time).
    const opening = indexedDB.open(name, 2);
    opening.onupgradeneeded = () => opening.result.createObjectStore('messages', { keyPath: 'id' });
    const old = await new Promise<IDBDatabase>((resolve, reject) => {
      opening.onsuccess = () => resolve(opening.result);
      opening.onerror = () => reject(opening.error);
    });
    const now = Date.now();
    const tx = old.transaction('messages', 'readwrite');
    const rows = tx.objectStore('messages');
    rows.add({ id: 'b-nested', message: { id: 'b-nested', t: 'edit', p: { value: 2 } }, timestamp: now - 2000, retries: 0 });
    rows.add({ id: 'a-flat', t: 'edit', p: { value: 1 }, timestamp: now - 3000, retries: 1 });
    rows.add({ id: 'c-flat', t: 'edit', p: { value: 3 }, timestamp: now - 1000, retries: 0 });
    await new Promise<void>((resolve, reject) => {
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
    old.close();

    const box = new Outbox(limits, { store: indexedDbOutbox(name) });
    await box.opened;
    expect(box.size).toBe(3);
    box.connected('ann', Date.now());
    box.serverAnswers(true);
    box.answeredWith(0, [], 'n1');
    const sent: Array<{ id?: string; t: string; p?: unknown }> = [];
    const hand = () =>
      box.due(Date.now(), noOutcomes(), (message) => {
        sent.push(message);
        return true;
      });
    hand();
    await later(30);
    hand();
    expect(sent.map((m) => ({ id: m.id, t: m.t, p: m.p }))).toEqual([
      { id: 'a-flat', t: 'edit', p: { value: 1 } },
      { id: 'b-nested', t: 'edit', p: { value: 2 } },
      { id: 'c-flat', t: 'edit', p: { value: 3 } },
    ]);
    box.close();
  });
});
