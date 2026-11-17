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
//
// With a store (`fileOutbox` of `wse-client/node`) what it holds outlives the
// process: the store gets each change as a line, a batch of them once per
// turn of the event loop, and is written anew once what went outweighs what
// is kept.

import type { WSMessage } from '../types.js';
import { generateId } from '../utils/id.js';

/**
 * A message's tag (`client.send(type, payload, { [OUTBOX_TAG]: n })`), kept
 * with it in memory: what a bridge owner names the window that sent it by.
 * Its outcome comes back with it ([TaggedOutcome]).
 */
export const OUTBOX_TAG: unique symbol = Symbol('wse.outboxTag');

/** What became of a tagged message: delivered (`reason` null), or not and why. */
export interface TaggedOutcome {
  tag: number;
  id: string;
  reason: string | null;
}

/** What became of messages: delivered, or not and why (the reason, the ids). */
export interface Outcomes {
  delivered: string[];
  notDelivered: Map<string, string[]>;
  /** Those of tagged messages, each with its tag, in the order they came: theirs alone. */
  tagged: TaggedOutcome[];
}

export function noOutcomes(): Outcomes {
  return { delivered: [], notDelivered: new Map(), tagged: [] };
}

/**
 * Tells the application what became of messages: `wseDelivered` and
 * `wseNotDelivered`; those of tagged messages go to `tagged` alone (a
 * window's, to the window that sent it).
 */
export function reportOutcomes(
  emit: (type: string, detail?: unknown) => void,
  outcomes: Outcomes,
  tagged?: (entries: TaggedOutcome[]) => void,
): void {
  if (outcomes.delivered.length > 0) emit('wseDelivered', { ids: outcomes.delivered });
  for (const [reason, ids] of outcomes.notDelivered) emit('wseNotDelivered', { reason, ids });
  if (tagged && outcomes.tagged.length > 0) tagged(outcomes.tagged);
}

function deliver(outcomes: Outcomes, id: string, tag = 0): void {
  if (tag) outcomes.tagged.push({ tag, id, reason: null });
  else outcomes.delivered.push(id);
}

/** `id` (tagged `tag`, or not) was not delivered, for `why`. */
export function refuse(outcomes: Outcomes, why: string, id: string, tag = 0): void {
  if (tag) {
    outcomes.tagged.push({ tag, id, reason: why });
    return;
  }
  const ids = outcomes.notDelivered.get(why);
  if (ids) ids.push(id);
  else outcomes.notDelivered.set(why, [id]);
}

function merge(into: Outcomes, from: Outcomes): void {
  for (const id of from.delivered) into.delivered.push(id);
  for (const [why, ids] of from.notDelivered) {
    for (const id of ids) refuse(into, why, id);
  }
  for (const entry of from.tagged) into.tagged.push(entry);
}

export interface OutboxLimits {
  /** How many wait at most, those written and not answered yet included. */
  maxMessages: number;
  /** How many bytes of messages (their JSON) wait at most. */
  maxBytes: number;
  /** How long a message waits to be written first, in ms, unless it says otherwise. */
  maxAge: number;
}

/** A message as a store keeps it. */
export interface StoredMessage {
  s: number;
  id: string;
  /** When it was sent (epoch ms). */
  at: number;
  /** Until when it may wait to be written first (epoch ms). */
  until: number;
  /** Sent after a connection of the last user: that user's alone. */
  owned: boolean;
  /**
   * Topics each continuous right before it goes (`requires`), every time it
   * goes: once more after a reconnect too.
   */
  requires?: string[];
  /** Its JSON as the client made it, without its number. */
  json: string;
}

/** What a store holds. */
export interface StoredOutbox {
  producer: string;
  /** The node the client was connected to last, as it named itself. */
  node: string | null;
  /** The highest number handed to a connection. */
  written: number;
  /** Above every number used. */
  next: number;
  /** The user of the last connection; null: none, or none known. */
  user: string | null;
  records: readonly StoredMessage[];
}

/** What a store held when it was opened, each message as read too. */
export interface ReadOutbox extends StoredOutbox {
  records: Array<StoredMessage & { message: WSMessage }>;
}

/** A change to what a store holds, besides a message kept. */
export type OutboxLine =
  | { written: number }
  | { ack: number }
  | { drop: number[] }
  | { user: string | null }
  | { node: string };

/** A store, open: the outbox's changes go to it as lines. */
export interface OutboxLog {
  /** The lines in the store, those handed and not written yet included. */
  readonly lines: number;
  /** A message kept: into the batch. */
  add(record: StoredMessage): void;
  /** A change: into the batch. */
  put(line: OutboxLine): void;
  /**
   * The batch goes to be written; `written`, once it is (past a crash of the
   * process), with every batch before it. Not when the write fails.
   */
  flush(written?: () => void): void;
  /** The batch is in what is written anew. */
  forget(): void;
  /** A write failed since the last time it was asked: the store is to be written anew. */
  takeBroken(): boolean;
  /** Writes it anew, with `state` alone; `written`, once it is. */
  rewrite(state: StoredOutbox, written?: () => void): void;
  /**
   * Writes what it was handed, at once, and lets the store go: another
   * client may take it.
   */
  close(): void;
}

/** A store, opened: what it held and its log. */
export interface OpenedStore {
  /**
   * What it held, the producer to go on with included; null: nothing (a new
   * file), the outbox goes on with its own.
   */
  read: ReadOutbox | null;
  log: OutboxLog;
}

/**
 * Where an outbox keeps what it holds, so that it outlives the process or
 * the page: `fileOutbox(path)` of `wse-client/node`, `indexedDbOutbox(name)`.
 */
export interface OutboxStore {
  /**
   * Opens it, the outbox's alone from now on (a promise for a store that
   * opens in its own time). `state` says what the outbox holds whenever the
   * store must write it anew (the process ends); `report` hears of a write
   * that failed. Throws, or rejects, when it cannot be had.
   */
  open(
    state: () => StoredOutbox,
    report: (operation: string, error: unknown) => void,
  ): OpenedStore | Promise<OpenedStore>;
}

/** The message with its number first: `{"s":N,` before the rest. */
export function numbered(s: number, json: string): string {
  return json.length > 2 ? `{"s":${s},${json.slice(1)}` : `{"s":${s}}`;
}

/** The message without its number (`{"s":N,` taken off the front). */
export function unnumbered(text: string): string {
  if (!text.startsWith('{"s":')) return text;
  let at = 5;
  while (at < text.length && text.charCodeAt(at) >= 48 && text.charCodeAt(at) <= 57) at++;
  if (text.charCodeAt(at) === 44) at++; // ','
  return `{${text.slice(at)}`;
}

/** A message kept. */
interface Kept extends StoredMessage {
  message: WSMessage;
  size: number;
  /** Its tag ([OUTBOX_TAG]), in memory alone: 0 or none for one not tagged. */
  tag?: number;
}

/**
 * A random id: a producer's, as 32 hex digits, from the WebCrypto the client
 * uses (the one given with setWebCrypto in React Native).
 */
export function newProducer(): string {
  return generateId().replace(/-/g, '');
}

/** Monotonic ms, for how often a broken store is written anew. */
function monotonic(): number {
  return typeof performance !== 'undefined' ? performance.now() : Date.now();
}

export interface OutboxOptions {
  /** The producer's id (default: a random one, or the store's). */
  producer?: string;
  /**
   * Hears of the messages that ran out on the outbox's own timer (none: they
   * go when the connection next looks).
   */
  onExpired?: (outcomes: Outcomes) => void;
  /** Where it keeps what it holds, to outlive the process. */
  store?: OutboxStore;
  /** The store could not be opened (`open`) or written (`write`): what waits stays in memory. */
  onStorageError?: (operation: string, error: unknown) => void;
  /** The store opened (true) or could not be (false): after each open. */
  onOpen?: (durable: boolean) => void;
}

export class Outbox {
  /** The records, oldest first, from `head` on (taken off the front by moving it). */
  private records: Kept[] = [];
  private head = 0;
  /** Records from here on did not go to the current connection yet (an absolute index). */
  private cursor = 0;
  private next = 1;
  private producerId: string;
  /** The node the client was connected to last, as it named itself. */
  private node: string | null = null;
  /** The highest number handed to a connection: one above it never reached a server. */
  private written = 0;
  /**
   * Written ahead: the highest number the store was handed the mark of (that
   * up to it, messages may have gone), and the highest one it wrote. A
   * numbered message goes only once the store wrote its mark, so that after
   * a crash its number is never another's.
   */
  private promised = 0;
  private stored = 0;
  /** A write of the store failed: messages go without waiting for it, until it is written anew. */
  private degraded = false;
  /** Hears that messages waiting for the store may go now (the client writes the outbox). */
  writable: (() => void) | null = null;
  /** Written on the current connection and cleared before the server answered for them. */
  private limbo: Array<[number, string, number]> = [];
  /** The user of the last connection; undefined before the first. */
  private user: string | null | undefined = undefined;
  private ready = false;
  private answered = false;
  private plain = false;
  private count = 0;
  private bytes = 0;
  /** Places held for sends that wait for their topics ([hold]). */
  private held = 0;
  private heldBytes = 0;
  /** The messages it took: one a vanished socket hands back waits here already. */
  private taken = new WeakMap<WSMessage, Kept>();
  /** When the first message not written yet runs out (epoch ms); Infinity: none. */
  private earliestMs = Infinity;
  /** Its timer, with or without a connection, when someone hears what ran out. */
  private timer: ReturnType<typeof setTimeout> | null = null;
  private timerAt = Infinity;
  private readonly onExpired?: (outcomes: Outcomes) => void;
  /** The store's log, while one is open. */
  private log: OutboxLog | null = null;
  /** A batch for the store waits for the end of this turn of the event loop. */
  private flushQueued = false;
  /** A write of the store failed: it is written anew, at most once a second. */
  private owedRewrite = false;
  private healAt = 0;
  /** The rewrite owed, once the second after the last one is over. */
  private healTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly store: OutboxStore | null;
  private readonly report: (operation: string, error: unknown) => void;
  private readonly onOpen?: (durable: boolean) => void;
  /** The store opens: what is sent meanwhile waits, numbered after what it holds once it opened. */
  private opening: Promise<void> | null = null;
  /** Sent while no store was open (it opens, or the page went away): its log gets them once it opens. */
  private unlogged: Kept[] = [];
  /** The page went away: the store was let go, and is opened again when it is back. */
  private suspended = false;
  private suspendOnOpen = false;
  /** The user signed out before the store opened: it is cleared once it did. */
  private clearOnOpen = false;
  private closed = false;

  constructor(
    private readonly limits: OutboxLimits,
    options: OutboxOptions = {},
  ) {
    this.producerId = options.producer ?? newProducer();
    this.onExpired = options.onExpired;
    this.store = options.store ?? null;
    const told = options.onStorageError ?? (() => {});
    this.report = (operation, error) => {
      // What waits for the store goes without it, until it is written anew.
      if (operation === 'write' && !this.degraded) {
        this.degraded = true;
        this.writable?.();
      }
      told(operation, error);
    };
    this.onOpen = options.onOpen;
    if (this.store) this.open(this.store);
  }

  /** Settled once the store opened, or could not be: the connection waits for it. */
  get opened(): Promise<void> {
    return this.opening ?? Promise.resolve();
  }

  private open(store: OutboxStore): void {
    let opened: OpenedStore | Promise<OpenedStore>;
    try {
      opened = store.open(() => this.state(), this.report);
    } catch (error) {
      this.report('open', error);
      this.onOpen?.(false);
      return;
    }
    if (!(opened instanceof Promise)) {
      this.take(opened);
      return;
    }
    const opening: Promise<void> = opened.then(
      (o) => {
        if (this.opening === opening) this.opening = null;
        this.take(o);
        if (this.closed) this.letGo();
        else if (this.suspendOnOpen) this.suspend();
      },
      (error) => {
        if (this.opening === opening) this.opening = null;
        this.report('open', error);
        this.onOpen?.(false);
      },
    );
    this.opening = opening;
  }

  /**
   * The store opened: what it holds is what waits (what this outbox logged
   * before the page went away is in it, or another page's now), and what was
   * sent meanwhile is numbered after it.
   */
  private take({ read, log }: OpenedStore): void {
    const unlogged = this.unlogged;
    this.unlogged = [];
    if (read) {
      this.records = [];
      this.head = 0;
      this.cursor = 0;
      this.count = 0;
      this.bytes = 0;
      this.restore(read);
      for (const record of unlogged) {
        record.s = this.next++;
        this.records.push(record);
        this.count++;
        this.bytes += record.size;
        if (record.until < this.earliestMs) this.earliestMs = record.until;
      }
      this.arm();
    }
    this.log = log;
    for (const record of unlogged) log.add(record);
    if (this.clearOnOpen) {
      // Said when the user signed out; what the store held goes with it.
      this.clearOnOpen = false;
      this.clear();
    }
    this.flush();
    this.onOpen?.(true);
  }

  /** The store is let go as it stands: its log writes what it was handed. */
  private letGo(): void {
    const log = this.log;
    if (!log) return;
    // A rewrite owed goes now: the store is not this outbox's after this.
    this.flushNow(true);
    this.log = null;
    log.close();
  }

  /**
   * The page went away (into the back/forward cache, or for good): the store
   * gets what changed and is let go, for another page to take what waits.
   */
  suspend(): void {
    if (!this.store || this.closed) return;
    if (this.opening) {
      this.suspendOnOpen = true;
      return;
    }
    if (!this.log) return;
    this.letGo();
    this.suspended = true;
  }

  /** The page is back: the store is opened again, and read again. */
  resume(): void {
    this.suspendOnOpen = false;
    if (!this.suspended || !this.store || this.closed) return;
    this.suspended = false;
    this.open(this.store);
  }

  /** The producer's id: the hello names it. */
  get producer(): string {
    return this.producerId;
  }

  /** A store holds what it holds: it outlives the process. */
  get durable(): boolean {
    return this.log !== null;
  }

  /** Takes up what the store held. */
  private restore(read: ReadOutbox): void {
    this.producerId = read.producer || this.producerId;
    this.node = read.node;
    this.written = read.written;
    this.promised = read.written;
    this.stored = read.written;
    this.next = Math.max(1, read.next);
    this.user = read.user ?? undefined;
    let earliest = Infinity;
    for (const r of read.records) {
      const record: Kept = {
        s: r.s,
        id: r.id,
        at: r.at,
        until: r.until,
        owned: r.owned,
        requires: r.requires,
        json: r.json,
        message: r.message,
        size: r.json.length,
      };
      this.records.push(record);
      this.count++;
      this.bytes += record.size;
      this.taken.set(record.message, record);
      if (record.s > this.written && record.until < earliest) earliest = record.until;
    }
    this.earliestMs = earliest;
    this.arm();
  }

  /** What it holds, for the store to write anew. */
  private state(): StoredOutbox {
    return {
      producer: this.producerId,
      node: this.node,
      written: Math.max(this.written, this.promised),
      next: this.next,
      user: typeof this.user === 'string' ? this.user : null,
      records: this.records.slice(this.head),
    };
  }

  /** Its timers stop: the client ended. */
  dispose(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    this.timerAt = Infinity;
    if (this.healTimer) clearTimeout(this.healTimer);
    this.healTimer = null;
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

  /**
   * A message handed to be written later (it is sealed meanwhile), right
   * before it is: its record while it may still go (kept, handed on this
   * connection and not taken back); null when it may not; undefined when it
   * is not this outbox's.
   */
  handed(message: WSMessage): Kept | null | undefined {
    const record = this.taken.get(message);
    if (!record) return undefined;
    const at = this.handedAt(record.s);
    return at >= 0 && this.records[at] === record ? record : null;
  }

  /**
   * A message handed to be written later cannot go now (the topics it
   * requires broke off while it was sealed): it and those handed after it
   * are taken back, and go again in order, so the server sees no gap.
   */
  takeBack(message: WSMessage): void {
    const record = this.taken.get(message);
    const at = record ? this.handedAt(record.s) : -1;
    if (at >= 0 && this.records[at] === record) this.cursor = at;
  }

  /** Where the record numbered `s` is among those handed on this connection, or -1. */
  private handedAt(s: number): number {
    let low = this.head;
    let high = this.cursor;
    while (low < high) {
      const mid = (low + high) >>> 1;
      if (this.records[mid].s < s) low = mid + 1;
      else high = mid;
    }
    return low < this.cursor && this.records[low].s === s ? low : -1;
  }

  /** What the client's hello says of it. */
  hello(): Record<string, unknown> {
    const first = this.records[this.head];
    return { producer: this.producerId, node: this.node, written: this.written, from: first ? first.s : this.next };
  }

  /** When the first message not written yet runs out (epoch ms), or null. */
  deadline(): number | null {
    return Number.isFinite(this.earliestMs) ? this.earliestMs : null;
  }

  /**
   * Holds a place for a message of `size` the client keeps later (a send
   * that waits for its topics): false when there is none.
   */
  hold(size: number): boolean {
    if (
      this.count + this.held + 1 > this.limits.maxMessages ||
      this.bytes + this.heldBytes + size > this.limits.maxBytes
    ) {
      return false;
    }
    this.held++;
    this.heldBytes += size;
    return true;
  }

  /** A place held for a message of `size` is not needed any more. */
  unhold(size: number): void {
    this.held--;
    this.heldBytes -= size;
  }

  /** No place is held any more (the sends that waited went, another user's). */
  unholdAll(): void {
    this.held = 0;
    this.heldBytes = 0;
  }

  /**
   * Keeps a message the application sent, with its JSON: its number, or 0
   * when the outbox is full (or the message larger than it holds). `held`:
   * the size of the place it held ([hold]), which it takes.
   */
  add(
    message: WSMessage,
    json: string,
    ttl: number | undefined,
    now: number,
    requires?: readonly string[],
    tag?: number,
    held?: number,
  ): number {
    const size = json.length;
    if (held !== undefined) {
      this.unhold(held);
    } else if (
      this.count + this.held + 1 > this.limits.maxMessages ||
      this.bytes + this.heldBytes + size > this.limits.maxBytes
    ) {
      return 0;
    }
    const s = this.next++;
    const wait = ttl !== undefined && Number.isFinite(ttl) && ttl >= 0 ? ttl : this.limits.maxAge;
    // Whole ms, as a store writes them.
    const until = Math.min(Math.round(now + wait), Number.MAX_SAFE_INTEGER);
    const record: Kept = {
      s,
      id: message.id ?? String(s),
      at: Math.round(now),
      until,
      owned: typeof this.user === 'string',
      requires: requires && requires.length > 0 ? [...requires] : undefined,
      json,
      message,
      size,
      tag: tag || undefined,
    };
    this.records.push(record);
    this.count++;
    this.bytes += size;
    this.taken.set(message, record);
    if (until < this.earliestMs) {
      this.earliestMs = until;
      this.arm();
    }
    if (this.log) {
      this.log.add(record);
      this.flush();
    } else if (this.store && (this.opening || this.suspended)) {
      this.unlogged.push(record);
    }
    return s;
  }

  /** The server took a connection of `user`: another user's messages go, and those that waited too long. */
  connected(user: string | null, now: number): Outcomes {
    const outcomes = noOutcomes();
    this.ready = true;
    if (this.user === undefined || this.user !== user) {
      // What was sent before any connection goes on any, until a connection
      // wrote it: then it was that connection's user's (the server may have
      // taken it as theirs), and it never goes as another's.
      if (this.user !== undefined) {
        this.removeWhere('user_changed', outcomes, (_, r) => r.owned || r.s <= this.written);
      }
      this.user = user;
      this.put({ user });
    }
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
    if (this.node !== node) {
      this.node = node;
      this.put({ node });
    }
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
      deliver(outcomes, record.id, record.tag);
    }
    // The answer to a hello may take what this connection did not write yet.
    if (this.cursor < this.head) this.cursor = this.head;
    if (gone > 0) {
      this.compact();
      this.put({ ack: s });
    }
    while (this.limbo.length > 0 && this.limbo[0][0] <= s) {
      const [, id, tag] = this.limbo.shift()!;
      deliver(outcomes, id, tag);
    }
    return outcomes;
  }

  /** The server refused the messages `from` to `to` for good, for `why`. */
  refused(from: number, to: number, why: string): Outcomes {
    const outcomes = noOutcomes();
    this.removeWhere(why, outcomes, (_, r) => r.s >= from && r.s <= to);
    this.limbo = this.limbo.filter(([n, id, tag]) => {
      const keep = n < from || n > to;
      if (!keep) refuse(outcomes, why, id, tag);
      return keep;
    });
    return outcomes;
  }

  /**
   * Hands the connection's writer the messages it did not take yet, in
   * order, each with its number and the topics it requires, once the server
   * took the connection and its mode is known; the writer says whether it
   * took each (false: stop, the rest waits; `later`: it is written later,
   * [wroteLater]). What waited too long to be sent goes instead.
   */
  due(
    now: number,
    outcomes: Outcomes,
    write: (
      message: WSMessage,
      s: number | undefined,
      json: string,
      requires?: readonly string[],
      tag?: number,
    ) => boolean | 'later',
  ): void {
    if (!this.ready || !(this.answered || this.plain) || this.opening) return;
    if (this.earliestMs <= now) this.expire(now, outcomes);
    const numbered = this.answered;
    let taken = 0;
    while (this.cursor < this.records.length) {
      const record = this.records[this.cursor];
      // Written ahead: the store is handed the mark for every message kept
      // now, and they go once it wrote it.
      if (numbered && record.s > this.stored && this.log && !this.degraded) {
        this.promise(this.records[this.records.length - 1].s);
        break;
      }
      const took = write(record.message, numbered ? record.s : undefined, record.json, record.requires, record.tag);
      if (!took) break;
      this.cursor++;
      if (numbered) {
        if (record.s > this.written) this.written = record.s;
      } else if (took === true) {
        // A server that does not answer took what was written.
        this.records[this.cursor - 1] = undefined as unknown as Kept;
        this.head = this.cursor;
        this.count--;
        this.bytes -= record.size;
        deliver(outcomes, record.id, record.tag);
        taken = record.s;
      }
    }
    if (!numbered && taken > 0) {
      this.compact();
      this.put({ ack: taken });
    }
    this.flush();
  }

  /**
   * A message the writer took for later is on the socket now (its encryption
   * is done): a server that does not answer took it, and those handed before
   * it. The writer writes them in the order they were handed.
   */
  wroteLater(message: WSMessage): Outcomes {
    const outcomes = noOutcomes();
    if (!this.plain || this.answered) return outcomes;
    let at = -1;
    for (let i = this.head; i < this.cursor; i++) {
      if (this.records[i]?.message === message) {
        at = i;
        break;
      }
    }
    if (at < 0) return outcomes;
    // It alone is taken: one handed before it and not written (it could not
    // be sealed, or could not go then) was handed back, and waits.
    const record = this.records[at];
    this.count--;
    this.bytes -= record.size;
    deliver(outcomes, record.id, record.tag);
    if (at === this.head) {
      this.records[at] = undefined as unknown as Kept;
      this.head++;
      this.compact();
      this.put({ ack: record.s });
    } else {
      this.records.splice(at, 1);
      this.cursor--;
      this.put({ drop: [record.s] });
    }
    return outcomes;
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
    for (const [, id, tag] of this.limbo) refuse(outcomes, 'cleared', id, tag);
    this.limbo = [];
    return outcomes;
  }

  /**
   * The client ends. In memory what waits goes with it (`closed`); in a
   * store it waits for the next client, the store written at once.
   */
  close(): Outcomes {
    const outcomes = this.disconnected();
    this.closed = true;
    // In a store (or one that opens: what waits goes to it once it did).
    const kept = this.log !== null || this.opening !== null || this.suspended;
    if (this.log) {
      this.letGo();
    } else if (!kept) {
      for (let i = this.head; i < this.records.length; i++) {
        refuse(outcomes, 'closed', this.records[i].id, this.records[i].tag);
      }
    }
    if (!this.opening) this.reset();
    return outcomes;
  }

  /**
   * The user signed out: every message goes. One the server has in hand on
   * this connection is delivered when it confirms it on it, and not when the
   * connection ends first.
   */
  clear(): Outcomes {
    const outcomes = noOutcomes();
    const numbers: number[] = [];
    for (let i = this.head; i < this.records.length; i++) {
      const record = this.records[i];
      numbers.push(record.s);
      if (this.answered && i < this.cursor) this.limbo.push([record.s, record.id, record.tag ?? 0]);
      else refuse(outcomes, 'cleared', record.id, record.tag);
    }
    this.reset();
    this.unlogged = [];
    if (numbers.length > 0) this.put({ drop: numbers });
    this.user = undefined;
    this.put({ user: null });
    if (this.store && !this.log && (this.opening || this.suspended)) {
      // What the store holds goes too, once it is open.
      this.clearOnOpen = true;
      if (this.suspended && !this.closed) this.resume();
    }
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
    const numbers: number[] = [];
    let cursor = this.cursor - this.head;
    for (let i = this.head; i < this.records.length; i++) {
      const record = this.records[i];
      if (gone(i, record)) {
        this.count--;
        this.bytes -= record.size;
        refuse(outcomes, why, record.id, record.tag);
        numbers.push(record.s);
        if (i < this.cursor) cursor--;
      } else {
        kept.push(record);
      }
    }
    this.records = kept;
    this.head = 0;
    this.cursor = cursor;
    if (numbers.length > 0) this.put({ drop: numbers });
  }

  /** The records taken off the front go once they outweigh the rest. */
  private compact(): void {
    if (this.head > 1024 && this.head * 2 > this.records.length) {
      this.records = this.records.slice(this.head);
      this.cursor -= this.head;
      this.head = 0;
    }
  }

  /** A change for the store's batch. */
  private put(line: OutboxLine): void {
    if (!this.log) return;
    this.log.put(line);
    this.flush();
  }

  /** The store is to hold that messages up to `s` may have gone: in this turn's batch. */
  private promise(s: number): void {
    if (s <= this.promised) return;
    this.promised = s;
    this.put({ written: s });
  }

  /** The store wrote the marks up to `s`: what waited for them may go. */
  private wrote(s: number): void {
    if (s <= this.stored) return;
    this.stored = s;
    this.writable?.();
  }

  /** The store gets the batch at the end of this turn of the event loop: the changes of a turn at once. */
  private flush(): void {
    if (!this.log || this.flushQueued) return;
    this.flushQueued = true;
    queueMicrotask(() => {
      this.flushQueued = false;
      this.flushNow();
    });
  }

  /**
   * Hands the batch to the store; writes it anew instead once what went
   * outweighs what is kept, or after a write failed (the batch is in what is
   * written anew): at most once a second, the next one on a timer of its own
   * so that it comes with no further change, and at once when the store is
   * let go (`closing`).
   */
  private flushNow(closing = false): void {
    const log = this.log;
    if (!log) return;
    const promised = this.promised;
    const written = promised > this.stored ? () => this.wrote(promised) : undefined;
    if (log.takeBroken()) this.owedRewrite = true;
    if (this.owedRewrite) {
      const now = monotonic();
      if (closing || now >= this.healAt) {
        this.owedRewrite = false;
        this.healAt = now + 1000;
        if (this.healTimer) clearTimeout(this.healTimer);
        this.healTimer = null;
        // Written anew, it holds what went meanwhile: messages wait for it again.
        const state = this.state();
        this.promised = state.written;
        log.rewrite(state, () => {
          this.degraded = false;
          this.wrote(state.written);
        });
      } else {
        log.forget();
        if (!this.healTimer) {
          const timer = setTimeout(() => {
            this.healTimer = null;
            this.flushNow();
          }, this.healAt - now);
          // A process that ends writes its store as it goes (the file's exit).
          (timer as { unref?: () => void }).unref?.();
          this.healTimer = timer;
        }
      }
    } else if (log.lines > 2 * this.count + 64) {
      log.rewrite(this.state(), written);
    } else {
      log.flush(written);
    }
  }
}
