// =============================================================================
// WebSocket Engine - Outbox
// =============================================================================
//
// Every message the client sends, except a live or signed one, waits here
// until the server answers for it, and goes again after a reconnect. A server
// that answers (`acks`, from 3.2) confirms what its application took (`ack`,
// up to and with a number) and says what it refused (`nack`). The messages go
// out in order, numbered (`s`, first in the object) per producer, one producer
// per outbox, and the server drops a repeat, so its application takes each
// message once. A server that does not answer takes a message once it is
// written, as before.

import type { WSMessage } from '../types.js';

/** What became of messages: delivered, or not and why (the reason, the ids). */
export interface Outcomes {
  delivered: string[];
  notDelivered: Map<string, string[]>;
}

export function noOutcomes(): Outcomes {
  return { delivered: [], notDelivered: new Map() };
}

/** Tells the application what became of messages: `wseDelivered` and `wseNotDelivered`. */
export function reportOutcomes(emit: (type: string, detail?: unknown) => void, outcomes: Outcomes): void {
  if (outcomes.delivered.length > 0) emit('wseDelivered', { ids: outcomes.delivered });
  for (const [reason, ids] of outcomes.notDelivered) emit('wseNotDelivered', { reason, ids });
}

function refuse(outcomes: Outcomes, why: string, id: string): void {
  const ids = outcomes.notDelivered.get(why);
  if (ids) ids.push(id);
  else outcomes.notDelivered.set(why, [id]);
}

function merge(into: Outcomes, from: Outcomes): void {
  for (const id of from.delivered) into.delivered.push(id);
  for (const [why, ids] of from.notDelivered) {
    for (const id of ids) refuse(into, why, id);
  }
}

export interface OutboxLimits {
  /** How many wait at most, those written and not answered yet included. */
  maxMessages: number;
  /** How many bytes of messages (their JSON) wait at most. */
  maxBytes: number;
  /** How long a message waits to be written first, in ms, unless it says otherwise. */
  maxAge: number;
}

/** A message kept. */
interface Kept {
  s: number;
  id: string;
  message: WSMessage;
  /** Its JSON as it was made: written as it is, after its number. */
  json: string;
  /** Until when it may wait to be written first (epoch ms). */
  until: number;
  /** Sent after a connection of the last user: that user's alone. */
  owned: boolean;
  size: number;
}

/** A random id: a producer's, 128 bits as 32 hex digits. */
function newProducer(): string {
  const bytes = new Uint8Array(16);
  globalThis.crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}

export class Outbox {
  /** The records, oldest first, from `head` on (taken off the front by moving it). */
  private records: Kept[] = [];
  private head = 0;
  /** Records from here on did not go to the current connection yet (an absolute index). */
  private cursor = 0;
  private next = 1;
  readonly producer: string;
  /** The node the client was connected to last, as it named itself. */
  private node: string | null = null;
  /** The highest number handed to a connection: one above it never reached a server. */
  private written = 0;
  /** Written on the current connection and cleared before the server answered for them. */
  private limbo: Array<[number, string]> = [];
  /** The user of the last connection; undefined before the first. */
  private user: string | null | undefined = undefined;
  private ready = false;
  private answered = false;
  private plain = false;
  private count = 0;
  private bytes = 0;
  /** The messages it took: one a vanished socket hands back waits here already. */
  private taken = new WeakSet<WSMessage>();
  /** When the first message not written yet runs out (epoch ms); Infinity: none. */
  private earliestMs = Infinity;
  /** Its timer, with or without a connection, when someone hears what ran out. */
  private timer: ReturnType<typeof setTimeout> | null = null;
  private timerAt = Infinity;

  /**
   * `onExpired` hears of the messages that ran out on the outbox's own timer
   * (none: they go when the connection next looks).
   */
  constructor(
    private readonly limits: OutboxLimits,
    producer?: string,
    private readonly onExpired?: (outcomes: Outcomes) => void,
  ) {
    this.producer = producer ?? newProducer();
  }

  /** Its timer stops: the client ended. */
  dispose(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    this.timerAt = Infinity;
  }

  /** The timer, for the first message not written yet to run out. */
  private arm(): void {
    if (!this.onExpired) return;
    const at = this.earliestMs;
    if (!Number.isFinite(at)) {
      this.dispose();
      return;
    }
    if (this.timer && this.timerAt <= at) return;
    if (this.timer) clearTimeout(this.timer);
    this.timerAt = at;
    // A timer runs at most about 24.8 days ahead.
    const delay = Math.min(Math.max(0, at - Date.now()), 2 ** 31 - 1);
    const timer = setTimeout(() => {
      this.timer = null;
      this.timerAt = Infinity;
      const outcomes = noOutcomes();
      this.expire(Date.now(), outcomes);
      this.onExpired?.(outcomes);
    }, delay);
    // Nothing for a process to stay up for.
    (timer as { unref?: () => void }).unref?.();
    this.timer = timer;
  }

  /** How many wait for the server's answer. */
  get size(): number {
    return this.count;
  }

  /** The server took the current connection (or it is one of a server before 3.0). */
  get connectionReady(): boolean {
    return this.ready;
  }

  /** `message` is one of the outbox's. */
  has(message: WSMessage): boolean {
    return this.taken.has(message);
  }

  /** What the client's hello says of it. */
  hello(): Record<string, unknown> {
    const first = this.records[this.head];
    return { producer: this.producer, node: this.node, written: this.written, from: first ? first.s : this.next };
  }

  /** When the first message not written yet runs out (epoch ms), or null. */
  deadline(): number | null {
    return Number.isFinite(this.earliestMs) ? this.earliestMs : null;
  }

  /**
   * Keeps a message the application sent, with its JSON: its number, or 0
   * when the outbox is full (or the message larger than it holds).
   */
  add(message: WSMessage, json: string, ttl: number | undefined, now: number): number {
    const size = json.length;
    if (this.count + 1 > this.limits.maxMessages || this.bytes + size > this.limits.maxBytes) return 0;
    const s = this.next++;
    const wait = ttl !== undefined && Number.isFinite(ttl) && ttl >= 0 ? ttl : this.limits.maxAge;
    const until = now + wait;
    this.records.push({
      s,
      id: message.id ?? String(s),
      message,
      json,
      until,
      owned: typeof this.user === 'string',
      size,
    });
    this.count++;
    this.bytes += size;
    this.taken.add(message);
    if (until < this.earliestMs) {
      this.earliestMs = until;
      this.arm();
    }
    return s;
  }

  /** The server took a connection of `user`: another user's messages go, and those that waited too long. */
  connected(user: string | null, now: number): Outcomes {
    const outcomes = noOutcomes();
    this.ready = true;
    if (this.user !== undefined && this.user !== user) {
      // What was sent before any connection goes on any.
      this.removeWhere('user_changed', outcomes, (_, r) => r.owned);
    }
    this.user = user;
    if (this.earliestMs <= now) this.expire(now, outcomes);
    return outcomes;
  }

  /** The server of this connection answers numbered messages (`acks`), or it does not. */
  serverAnswers(answers: boolean): void {
    this.plain = !answers;
  }

  /**
   * The server's answer to the hello: what it decided, the refusals among that
   * (first, last, why), and its name. It may come before the server took the
   * connection or after.
   */
  answeredWith(acked: number, refused: Array<[number, number, string]>, node: string): Outcomes {
    const outcomes = noOutcomes();
    for (const [from, to, why] of refused) merge(outcomes, this.refused(from, to, why));
    merge(outcomes, this.acked(acked));
    this.answered = true;
    this.node = node;
    return outcomes;
  }

  /** The server took every message up to and with `s`. */
  acked(s: number): Outcomes {
    const outcomes = noOutcomes();
    let gone = 0;
    while (this.head < this.records.length && this.records[this.head].s <= s) {
      const record = this.records[this.head];
      this.records[this.head] = undefined as unknown as Kept;
      this.head++;
      gone++;
      this.count--;
      this.bytes -= record.size;
      outcomes.delivered.push(record.id);
    }
    // The answer to a hello may take what this connection did not write yet.
    if (this.cursor < this.head) this.cursor = this.head;
    if (gone > 0) this.compact();
    while (this.limbo.length > 0 && this.limbo[0][0] <= s) {
      outcomes.delivered.push(this.limbo.shift()![1]);
    }
    return outcomes;
  }

  /** The server refused the messages `from` to `to` for good, for `why`. */
  refused(from: number, to: number, why: string): Outcomes {
    const outcomes = noOutcomes();
    this.removeWhere(why, outcomes, (_, r) => r.s >= from && r.s <= to);
    this.limbo = this.limbo.filter(([n, id]) => {
      const keep = n < from || n > to;
      if (!keep) refuse(outcomes, why, id);
      return keep;
    });
    return outcomes;
  }

  /**
   * Hands the connection's writer the messages it did not take yet, in
   * order, each with its number, once the server took the connection and its
   * mode is known; the writer says whether it took each (false: stop, the
   * rest waits). What waited too long to be sent goes instead.
   */
  due(
    now: number,
    outcomes: Outcomes,
    write: (message: WSMessage, s: number | undefined, json: string) => boolean,
  ): void {
    if (!this.ready || !(this.answered || this.plain)) return;
    if (this.earliestMs <= now) this.expire(now, outcomes);
    const numbered = this.answered;
    while (this.cursor < this.records.length) {
      const record = this.records[this.cursor];
      if (!write(record.message, numbered ? record.s : undefined, record.json)) return;
      this.cursor++;
      if (numbered) {
        if (record.s > this.written) this.written = record.s;
      } else {
        // A server that does not answer took what was written.
        this.records[this.cursor - 1] = undefined as unknown as Kept;
        this.head = this.cursor;
        this.count--;
        this.bytes -= record.size;
        outcomes.delivered.push(record.id);
      }
    }
    if (!numbered) this.compact();
  }

  /** The server owes an answer on this connection: for messages it has, or to the hello while some wait. */
  owed(): boolean {
    if (this.answered) {
      return this.limbo.length > 0 || (this.head < this.cursor);
    }
    return this.ready && !this.plain && this.count > 0;
  }

  /** The connection went: what waits goes on the next one. What was cleared while the server had it is not delivered. */
  disconnected(): Outcomes {
    const outcomes = noOutcomes();
    this.ready = false;
    this.answered = false;
    this.plain = false;
    this.cursor = this.head;
    for (const [, id] of this.limbo) refuse(outcomes, 'cleared', id);
    this.limbo = [];
    return outcomes;
  }

  /** The client ends: what waits goes with it (`closed`). */
  close(): Outcomes {
    const outcomes = this.disconnected();
    for (let i = this.head; i < this.records.length; i++) refuse(outcomes, 'closed', this.records[i].id);
    this.reset();
    return outcomes;
  }

  /**
   * The user signed out: every message goes. One the server has in hand on
   * this connection is delivered when it confirms it on it, and not when the
   * connection ends first.
   */
  clear(): Outcomes {
    const outcomes = noOutcomes();
    for (let i = this.head; i < this.records.length; i++) {
      const record = this.records[i];
      if (this.answered && i < this.cursor) this.limbo.push([record.s, record.id]);
      else refuse(outcomes, 'cleared', record.id);
    }
    this.reset();
    this.user = undefined;
    return outcomes;
  }

  /** Drops what waited longer than it may to be written. */
  expire(now: number, outcomes: Outcomes): void {
    const written = this.written;
    const cursor = this.cursor;
    this.removeWhere('expired', outcomes, (at, r) => at >= cursor && r.s > written && r.until < now);
    let earliest = Infinity;
    for (let i = this.head; i < this.records.length; i++) {
      const record = this.records[i];
      if (record.s > written && record.until < earliest) earliest = record.until;
    }
    this.earliestMs = earliest;
    this.arm();
  }

  private reset(): void {
    this.records = [];
    this.head = 0;
    this.cursor = 0;
    this.count = 0;
    this.bytes = 0;
    this.earliestMs = Infinity;
    this.dispose();
  }

  /** Removes the records `gone` names (by absolute index and record), telling why. */
  private removeWhere(why: string, outcomes: Outcomes, gone: (at: number, record: Kept) => boolean): void {
    const kept: Kept[] = [];
    let cursor = this.cursor - this.head;
    for (let i = this.head; i < this.records.length; i++) {
      const record = this.records[i];
      if (gone(i, record)) {
        this.count--;
        this.bytes -= record.size;
        refuse(outcomes, why, record.id);
        if (i < this.cursor) cursor--;
      } else {
        kept.push(record);
      }
    }
    this.records = kept;
    this.head = 0;
    this.cursor = cursor;
  }

  /** The records taken off the front go once they outweigh the rest. */
  private compact(): void {
    if (this.head > 1024 && this.head * 2 > this.records.length) {
      this.records = this.records.slice(this.head);
      this.cursor -= this.head;
      this.head = 0;
    }
  }
}
