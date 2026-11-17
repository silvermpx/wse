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
