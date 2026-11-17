import { describe, expect, it } from 'vitest';
import { noOutcomes, Outbox, type OutboxLine, type OutboxStore, type Outcomes, type StoredOutbox } from '../services/Outbox';
import type { WSMessage } from '../types';

const limits = { maxMessages: 100, maxBytes: 1 << 20, maxAge: 3_600_000 };

function message(id: string): WSMessage {
  return { t: 'm', p: {}, id };
}

function add(outbox: Outbox, id: string, ttl?: number, now = 0): number {
  return outbox.add(message(id), JSON.stringify(message(id)), ttl, now);
}

/** A message whose JSON is `size` long. */
function sized(outbox: Outbox, id: string, size: number): number {
  return outbox.add(message(id), 'x'.repeat(size), undefined, 0);
}

/** What goes to the connection's writer now: (number, id). */
function due(outbox: Outbox, now = 0, outcomes: Outcomes = noOutcomes()): Array<[number | undefined, string]> {
  const out: Array<[number | undefined, string]> = [];
  outbox.due(now, outcomes, (m, s) => {
    out.push([s, m.id!]);
    return true;
  });
  return out;
}

function answering(outbox: Outbox, acked = 0): Outcomes {
  outbox.connected('ann', 0);
  outbox.serverAnswers(true);
  return outbox.answeredWith(acked, [], 'n1');
}

describe('Outbox', () => {
  it('keeps a message until the answer, and sends it again after a reconnect', () => {
    const outbox = new Outbox(limits);
    add(outbox, 'a');
    add(outbox, 'b');
    expect(due(outbox)).toEqual([]);
    outbox.connected('ann', 0);
    outbox.serverAnswers(true);
    expect(due(outbox)).toEqual([]);
    expect(outbox.hello()).toMatchObject({ from: 1, written: 0 });
    outbox.answeredWith(0, [], 'n1');
    expect(due(outbox)).toEqual([[1, 'a'], [2, 'b']]);
    expect(outbox.acked(1).delivered).toEqual(['a']);
    // Lost before 2 was answered: it goes again on the next connection.
    outbox.disconnected();
    expect(outbox.hello()).toMatchObject({ written: 2, node: 'n1', from: 2 });
    expect(answering(outbox, 1).delivered).toEqual([]);
    expect(due(outbox)).toEqual([[2, 'b']]);
    add(outbox, 'c');
    expect(due(outbox)).toEqual([[3, 'c']]);
    expect(outbox.acked(3).delivered).toEqual(['b', 'c']);
    expect(outbox.size).toBe(0);
  });

  it('takes the answer before the server takes the connection', () => {
    const outbox = new Outbox(limits);
    add(outbox, 'a');
    outbox.serverAnswers(true);
    outbox.answeredWith(0, [], 'n1');
    expect(due(outbox)).toEqual([]);
    outbox.connected('ann', 0);
    expect(due(outbox)).toEqual([[1, 'a']]);
  });

  it('lets a server without answers take what was written', () => {
    const outbox = new Outbox(limits);
    add(outbox, 'a');
    outbox.connected(null, 0);
    outbox.serverAnswers(false);
    const outcomes = noOutcomes();
    expect(due(outbox, 0, outcomes)).toEqual([[undefined, 'a']]);
    expect(outcomes.delivered).toEqual(['a']);
    expect(outbox.size).toBe(0);
  });

  it('drops another user’s messages', () => {
    const outbox = new Outbox(limits);
    add(outbox, 'before any');
    outbox.connected('ann', 0);
    outbox.disconnected();
    add(outbox, 'anns');
    const outcomes = outbox.connected('bob', 0);
    expect([...outcomes.notDelivered]).toEqual([['user_changed', ['anns']]]);
    expect(outbox.size).toBe(1);
  });

  it('never sends a message again as another user than the one it was written as', () => {
    const outbox = new Outbox(limits);
    add(outbox, 'before any');
    answering(outbox);
    expect(due(outbox)).toEqual([[1, 'before any']]);
    // Lost before its answer; the next connection is bob's.
    outbox.disconnected();
    const outcomes = outbox.connected('bob', 0);
    expect([...outcomes.notDelivered]).toEqual([['user_changed', ['before any']]]);
    outbox.serverAnswers(true);
    outbox.answeredWith(0, [], 'n1');
    expect(due(outbox)).toEqual([]);
  });

  it('drops one not written within its time, and not one written', () => {
    const outbox = new Outbox(limits);
    add(outbox, 'late', 5, 0);
    add(outbox, 'on time', undefined, 0);
    expect(outbox.deadline()).toBe(5);
    const outcomes = noOutcomes();
    outbox.expire(10, outcomes);
    expect([...outcomes.notDelivered]).toEqual([['expired', ['late']]]);
    answering(outbox);
    add(outbox, 'quick', 5, 10);
    expect(due(outbox, 10)).toEqual([[2, 'on time'], [3, 'quick']]);
    outbox.disconnected();
    answering(outbox);
    const later = noOutcomes();
    expect(due(outbox, 100, later)).toEqual([[2, 'on time'], [3, 'quick']]);
    expect(later.notDelivered.size).toBe(0);
  });

  it('tells a refusal, one heard with the answer too', () => {
    const outbox = new Outbox(limits);
    answering(outbox);
    for (const id of ['a', 'b', 'c']) add(outbox, id);
    due(outbox);
    expect([...outbox.refused(1, 1, 'busy').notDelivered]).toEqual([['busy', ['a']]]);
    outbox.disconnected();
    outbox.connected('ann', 0);
    outbox.serverAnswers(true);
    const outcomes = outbox.answeredWith(3, [[2, 2, 'rate_limited']], 'n1');
    expect(outcomes.delivered).toEqual(['c']);
    expect([...outcomes.notDelivered]).toEqual([['rate_limited', ['b']]]);
    expect(outbox.size).toBe(0);
    expect(outbox.hello()).toMatchObject({ from: 4 });
  });

  it('clears, and tells what the server had in hand by its answer', () => {
    const outbox = new Outbox(limits);
    answering(outbox);
    add(outbox, 'a');
    add(outbox, 'b');
    due(outbox);
    add(outbox, 'c');
    expect([...outbox.clear().notDelivered]).toEqual([['cleared', ['c']]]);
    expect(outbox.size).toBe(0);
    expect(outbox.acked(1).delivered).toEqual(['a']);
    expect([...outbox.disconnected().notDelivered]).toEqual([['cleared', ['b']]]);
    answering(outbox);
    expect(due(outbox)).toEqual([]);
  });

  it('refuses past its limits', () => {
    const outbox = new Outbox({ maxMessages: 2, maxBytes: 100, maxAge: 1000 });
    expect(sized(outbox, 'a', 10)).toBe(1);
    expect(sized(outbox, 'b', 80)).toBe(2);
    expect(sized(outbox, 'c', 1)).toBe(0);
    answering(outbox);
    due(outbox);
    outbox.acked(2);
    expect(sized(outbox, 'd', 101)).toBe(0);
    expect(sized(outbox, 'e', 100)).toBe(3);
  });

  it('owes an answer for what it wrote, and to its hello while messages wait', () => {
    const outbox = new Outbox(limits);
    add(outbox, 'a');
    outbox.connected('ann', 0);
    outbox.serverAnswers(true);
    expect(outbox.owed()).toBe(true);
    outbox.answeredWith(0, [], 'n1');
    due(outbox);
    expect(outbox.owed()).toBe(true);
    outbox.acked(1);
    expect(outbox.owed()).toBe(false);
  });

  it('stops where the writer stops, and goes on from there', () => {
    const outbox = new Outbox(limits);
    answering(outbox);
    for (const id of ['a', 'b', 'c']) add(outbox, id);
    let taken = 0;
    outbox.due(0, noOutcomes(), () => taken++ < 1);
    expect(due(outbox)).toEqual([[2, 'b'], [3, 'c']]);
  });

  it('closes with what waits not delivered', () => {
    const outbox = new Outbox(limits);
    add(outbox, 'a');
    expect([...outbox.close().notDelivered]).toEqual([['closed', ['a']]]);
    expect(outbox.size).toBe(0);
  });

  it('keeps many acks cheap and in order', () => {
    const outbox = new Outbox({ ...limits, maxMessages: 10_000 });
    answering(outbox);
    for (let i = 0; i < 5000; i++) add(outbox, `m${i}`);
    expect(due(outbox).length).toBe(5000);
    let delivered = 0;
    for (let s = 64; s <= 5000; s += 64) delivered += outbox.acked(s).delivered.length;
    delivered += outbox.acked(5000).delivered.length;
    expect(delivered).toBe(5000);
    expect(outbox.size).toBe(0);
    add(outbox, 'next');
    expect(due(outbox)).toEqual([[5001, 'next']]);
  });
});

/** A store whose writes the test confirms, or fails, by hand. */
function handStore() {
  const lines: OutboxLine[] = [];
  const written: Array<() => void> = [];
  let report: (operation: string, error: unknown) => void = () => {};
  const store: OutboxStore = {
    open: (_state, told) => {
      report = told;
      return {
        read: null,
        log: {
          lines: 0,
          add: () => {},
          put: (line) => void lines.push(line),
          flush: (done?: () => void) => void (done && written.push(done)),
          forget: () => {},
          takeBroken: () => false,
          rewrite: (_state: StoredOutbox, done?: () => void) => void (done && written.push(done)),
          close: () => {},
        },
      };
    },
  };
  return {
    store,
    lines,
    /** The store wrote what it was handed. */
    write: () => written.splice(0).forEach((done) => done()),
    fail: () => report('write', new Error('disk full')),
  };
}

describe('Outbox, written ahead', () => {
  it('hands a numbered message to the connection only once the store holds it and its mark', async () => {
    const hand = handStore();
    const outbox = new Outbox(limits, { store: hand.store });
    let writable = 0;
    outbox.writable = () => void writable++;
    add(outbox, 'a', undefined, Date.now());
    answering(outbox);
    expect(due(outbox, Date.now())).toEqual([]);
    await Promise.resolve();
    expect(hand.lines).toContainEqual({ written: 1 });
    hand.write();
    expect(writable).toBe(1);
    expect(due(outbox, Date.now())).toEqual([[1, 'a']]);
  });

  it('lets messages go without waiting once the store failed', async () => {
    const hand = handStore();
    const outbox = new Outbox(limits, { store: hand.store });
    let writable = 0;
    outbox.writable = () => void writable++;
    add(outbox, 'a', undefined, Date.now());
    answering(outbox);
    expect(due(outbox, Date.now())).toEqual([]);
    hand.fail();
    expect(writable).toBe(1);
    expect(due(outbox, Date.now())).toEqual([[1, 'a']]);
  });
});
