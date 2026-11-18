// =============================================================================
// WebSocket Engine - The outbox in IndexedDB (a page, a worker)
// =============================================================================
//
// One database per scope: `producers` (the producer's next number, the
// highest written, the node, the user) and `outbox` (each message kept, by
// producer and number). A tab's outbox is one producer, its while the tab
// holds the producer's Web Lock; the system lets go of the lock when the tab
// goes, however it goes. A page that opens takes over a producer whose lock
// is free and which holds messages (a closed or crashed tab's, or the page it
// reloads) and sends them on its own connection, else it makes a new one.
//
// The changes of a turn of the event loop go in one transaction, one
// transaction at a time (relaxed durability: past a crash of the tab or the
// browser, not a power cut). The page that goes (pagehide) lets the lock go,
// so the back/forward cache can keep it; it reads the database again when
// it is back, since another page may have taken its producer meanwhile.
//
// Version 3 of the database; version 2 is 3.1's queue, whose messages move
// into producers of their own, one per user.

import type { WSMessage } from '../types.js';
import type {
  OpenedStore,
  OutboxLine,
  OutboxLog,
  OutboxStore,
  ReadOutbox,
  StoredMessage,
  StoredOutbox,
} from './Outbox.js';
import { newProducer } from './Outbox.js';

const VERSION = 3;
const PRODUCERS = 'producers';
const OUTBOX = 'outbox';
/** 3.1's queue and its lease. */
const LEGACY = 'messages';
const LEGACY_LEASE = 'lease';
/** The mark of a 3.1 database whose messages move into another scope's, until it goes. */
const MOVING = 'moving';
/** How long 3.1's queue kept a message without a time of its own. */
const LEGACY_MAX_AGE = 3_600_000;

/** A producer, as the database keeps it. */
interface Meta {
  producer: string;
  next: number;
  written: number;
  node: string | null;
  user: string | null;
}

/** A message kept, as the database keeps it. */
interface Row extends StoredMessage {
  producer: string;
}

type Report = (operation: string, error: unknown) => void;

/** Topics a kept message requires, as the database holds them; anything else is none. */
function topicsOf(value: unknown): string[] | undefined {
  return Array.isArray(value) && value.length > 0 && value.every((t) => typeof t === 'string' && t.length > 0)
    ? (value as string[])
    : undefined;
}

function done<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function committed(tx: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error ?? new Error('the transaction was aborted'));
  });
}

/** Every message of `producer`. */
function rangeOf(producer: string): IDBKeyRange {
  return IDBKeyRange.bound([producer, 0], [producer, Infinity]);
}

/** A Web Lock taken at once, held until released; null: another holds it. */
function tryLock(name: string): Promise<(() => void) | null> {
  const locks = (globalThis.navigator as (Navigator & { locks?: LockManager }) | undefined)?.locks;
  if (!locks) {
    return Promise.reject(new Error('no Web Locks here (a page needs HTTPS for them): the outbox stays in memory'));
  }
  return new Promise((resolve, reject) => {
    locks
      .request(name, { ifAvailable: true }, (lock) => {
        if (!lock) {
          resolve(null);
          return undefined;
        }
        return new Promise<void>((release) => resolve(() => release()));
      })
      .catch(reject);
  });
}

/** A row of 3.1's queue, as 3.1 read it. */
interface LegacyRecord {
  id: string;
  message: object;
  timestamp?: unknown;
  expiresAt?: unknown;
  owner?: unknown;
}

/**
 * A row of 3.1's queue: its own (the message under `message`), or one 2.x
 * wrote (the message itself, with its `timestamp` and `retries`), which 3.1
 * kept and read; null: neither.
 */
function legacyRecord(raw: any): LegacyRecord | null {
  if (!raw || typeof raw !== 'object' || typeof raw.id !== 'string') return null;
  if (raw.message && typeof raw.message === 'object' && typeof raw.message.t === 'string') return raw;
  if (typeof raw.t === 'string') {
    const { timestamp, retries: _, ...message } = raw;
    return { id: raw.id, message, timestamp };
  }
  return null;
}

/**
 * 3.1's queue: its messages go into producers of their own, one per user,
 * for pages to take over; its stores go.
 */
function migrate(db: IDBDatabase, tx: IDBTransaction): void {
  const legacy = tx.objectStore(LEGACY);
  const read = legacy.getAll();
  read.onsuccess = () => {
    const groups = new Map<string | null, Row[]>();
    const records: LegacyRecord[] = [];
    for (const raw of read.result as unknown[]) {
      const record = legacyRecord(raw);
      if (record) records.push(record);
    }
    const time = (r: LegacyRecord) => (typeof r.timestamp === 'number' ? r.timestamp : 0);
    records.sort((a, b) => time(a) - time(b));
    const producers = new Map<string | null, string>();
    for (const record of records) {
      const user = typeof record.owner === 'string' ? record.owner : null;
      let producer = producers.get(user);
      if (!producer) {
        producer = newProducer();
        producers.set(user, producer);
        groups.set(user, []);
      }
      const rows = groups.get(user)!;
      const at = Math.round(time(record));
      rows.push({
        producer,
        s: rows.length + 1,
        id: record.id,
        at,
        until: typeof record.expiresAt === 'number' ? Math.round(record.expiresAt) : at + LEGACY_MAX_AGE,
        owned: user !== null,
        json: JSON.stringify(record.message),
      });
    }
    const outbox = tx.objectStore(OUTBOX);
    const metas = tx.objectStore(PRODUCERS);
    for (const [user, rows] of groups) {
      const producer = producers.get(user)!;
      metas.put({ producer, next: rows.length + 1, written: 0, node: null, user } satisfies Meta);
      for (const row of rows) outbox.put(row);
    }
    db.deleteObjectStore(LEGACY);
    if (db.objectStoreNames.contains(LEGACY_LEASE)) db.deleteObjectStore(LEGACY_LEASE);
  };
}

/** The stores of this version, 3.1's queue moved into them. */
function upgrade(db: IDBDatabase, tx: IDBTransaction): void {
  if (!db.objectStoreNames.contains(PRODUCERS)) db.createObjectStore(PRODUCERS, { keyPath: 'producer' });
  if (!db.objectStoreNames.contains(OUTBOX)) db.createObjectStore(OUTBOX, { keyPath: ['producer', 's'] });
  if (db.objectStoreNames.contains(LEGACY)) migrate(db, tx);
  else if (db.objectStoreNames.contains(LEGACY_LEASE)) db.deleteObjectStore(LEGACY_LEASE);
}

function openDatabase(name: string, report: Report): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name, VERSION);
    request.onerror = () => reject(request.error);
    // A page of 3.1 holds the database open: the upgrade waits for it.
    request.onblocked = () => report('blocked', new Error('another page holds the database in an older version'));
    request.onsuccess = () => resolve(request.result);
    request.onupgradeneeded = () => upgrade(request.result, request.transaction!);
  });
}

/** The database 3.1 kept a scope's outbox in, when this version names the scope's otherwise. */
export interface LegacyDatabase {
  name: string;
  /** Its name reads as a scope of another realm too (an `@` in it). */
  otherRealm: boolean;
}

/**
 * 3.1's database `name` upgraded and marked as moving, or one marked so
 * already (a move cut short); null when it is gone or a scope of this
 * version opened it as its own.
 */
function openMoving(name: string, report: Report): Promise<IDBDatabase | null> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name, VERSION);
    let gone = false;
    request.onerror = () => (gone ? resolve(null) : reject(request.error));
    request.onblocked = () => report('blocked', new Error('another page holds the database in an older version'));
    request.onupgradeneeded = (event) => {
      const tx = request.transaction!;
      // Deleted meanwhile: made anew by this open, it is not made at all.
      if (event.oldVersion === 0) {
        gone = true;
        tx.abort();
        return;
      }
      upgrade(request.result, tx);
      request.result.createObjectStore(MOVING);
    };
    request.onsuccess = () => {
      const db = request.result;
      if (db.objectStoreNames.contains(MOVING)) return resolve(db);
      db.close();
      resolve(null);
    };
  });
}

/** Every producer of `from` and what waits in it, put into `to` at once. */
async function copyInto(from: IDBDatabase, to: IDBDatabase): Promise<void> {
  const read = from.transaction([PRODUCERS, OUTBOX], 'readonly');
  const [metas, rows] = await Promise.all([
    done(read.objectStore(PRODUCERS).getAll()) as Promise<Meta[]>,
    done(read.objectStore(OUTBOX).getAll()) as Promise<Row[]>,
  ]);
  const tx = to.transaction([PRODUCERS, OUTBOX], 'readwrite');
  for (const meta of metas) tx.objectStore(PRODUCERS).put(meta);
  for (const row of rows) tx.objectStore(OUTBOX).put(row);
  await committed(tx);
}

/** Deletes `name`; one another page holds goes once that page closes it. */
function deleteWhenFree(name: string): Promise<void> {
  return new Promise((resolve) => {
    const request = indexedDB.deleteDatabase(name);
    request.onsuccess = () => resolve();
    request.onerror = () => resolve();
    request.onblocked = () => resolve();
  });
}

/**
 * 3.1's database of this scope (`legacy`), whose name this version gives
 * another scope: taken over into `db` when that name reads as this scope's
 * realm alone, once (its messages under their producers, a repeat put on
 * the same keys and taken by the server once), and it goes; with an `@` in
 * it, one of another realm may be its scope: left, and said. One a scope of
 * this version opened as its own is that scope's.
 */
async function takeLegacy(db: IDBDatabase, legacy: LegacyDatabase, report: Report): Promise<void> {
  if (typeof indexedDB.databases !== 'function') return;
  const listed = (await indexedDB.databases()).find((d) => d.name === legacy.name);
  if (!listed) return;
  if (legacy.otherRealm) {
    if ((listed.version ?? 0) < VERSION) {
      report(
        'legacy',
        new Error(`3.1 kept messages in ${legacy.name}, a name a scope of another realm reads too: they are not taken over`),
      );
    }
    return;
  }
  const locks = (globalThis.navigator as Navigator & { locks: LockManager }).locks;
  await locks.request(`wse-outbox-legacy:${legacy.name}`, async () => {
    const old = await openMoving(legacy.name, report);
    if (!old) return;
    try {
      await copyInto(old, db);
    } finally {
      old.close();
    }
    await deleteWhenFree(legacy.name);
  });
}

/** What the database holds of `meta`'s producer, each message as read too. */
/**
 * What `producer` holds now, its row and its messages read at once; null
 * when it is gone.
 */
async function readProducer(db: IDBDatabase, producer: string): Promise<{ meta: Meta; read: ReadOutbox } | null> {
  const tx = db.transaction([PRODUCERS, OUTBOX], 'readonly');
  const [meta, rows] = (await Promise.all([
    done(tx.objectStore(PRODUCERS).get(producer)),
    done(tx.objectStore(OUTBOX).getAll(rangeOf(producer))),
  ])) as [Meta | undefined, Row[]];
  if (!meta) return null;
  let next = Math.max(1, meta.next, meta.written + 1);
  const records: ReadOutbox['records'] = [];
  for (const row of rows) {
    let message: WSMessage;
    try {
      message = JSON.parse(row.json) as WSMessage;
    } catch {
      continue;
    }
    next = Math.max(next, row.s + 1);
    records.push({
      s: row.s,
      id: row.id,
      at: row.at,
      until: row.until,
      owned: row.owned,
      requires: topicsOf(row.requires),
      json: row.json,
      message,
    });
  }
  return { meta, read: { producer: meta.producer, node: meta.node, written: meta.written, next, user: meta.user, records } };
}

/** A step of a batch. */
type Op = { put: Row } | { delete: number } | { deleteTo: number } | { rewrite: Row[] };

class IdbLog implements OutboxLog {
  /** The database compacts itself: no lines to count. */
  readonly lines = 0;
  private ops: Op[] = [];
  private metaChanged = false;
  /** Batches handed and not in a transaction yet. */
  private queued: Op[] = [];
  private queuedMeta = false;
  /** Who hears that what was handed is written: with `queued`, and with the transaction that runs. */
  private queuedWritten: Array<() => void> = [];
  private running: Array<() => void> = [];
  private busy = false;
  private broken = false;
  /** A transaction failed: the database lacks what it held, and is written anew before anything is added. */
  private stale = false;
  private failing = false;
  private closed = false;

  constructor(
    private readonly db: IDBDatabase,
    private readonly meta: Meta,
    private readonly release: () => void,
    private readonly state: () => StoredOutbox,
    private readonly report: Report,
  ) {
    // Another page deletes the database (its user signed out) or upgrades
    // it: this one lets it, and keeps what waits in memory.
    db.onversionchange = () => {
      db.close();
      this.fail(new Error('another page deleted or upgraded the database'));
    };
  }

  private row(record: StoredMessage): Row {
    return {
      producer: this.meta.producer,
      s: record.s,
      id: record.id,
      at: record.at,
      until: record.until,
      owned: record.owned,
      ...(record.requires ? { requires: record.requires } : {}),
      json: record.json,
    };
  }

  add(record: StoredMessage): void {
    this.ops.push({ put: this.row(record) });
    if (record.s >= this.meta.next) {
      this.meta.next = record.s + 1;
      this.metaChanged = true;
    }
  }

  put(line: OutboxLine): void {
    if ('ack' in line) this.ops.push({ deleteTo: line.ack });
    else if ('drop' in line) for (const s of line.drop) this.ops.push({ delete: s });
    else {
      if ('written' in line) this.meta.written = line.written;
      else if ('user' in line) this.meta.user = line.user;
      else this.meta.node = line.node;
      this.metaChanged = true;
    }
  }

  flush(written?: () => void): void {
    if (this.ops.length > 0 || this.metaChanged) {
      for (const op of this.ops) this.queued.push(op);
      this.queuedMeta ||= this.metaChanged;
      this.ops = [];
      this.metaChanged = false;
    }
    if (written) {
      // With what is handed last: what waits, else the transaction that runs.
      if (this.queued.length > 0 || this.queuedMeta) this.queuedWritten.push(written);
      else if (this.busy) this.running.push(written);
      else written();
    }
    if (!this.busy) this.commit();
  }

  forget(): void {
    this.ops = [];
  }

  takeBroken(): boolean {
    const broken = this.broken;
    this.broken = false;
    return broken;
  }

  rewrite(state: StoredOutbox, written?: () => void): void {
    if (written) this.queuedWritten.push(written);
    this.meta.next = Math.max(this.meta.next, state.next);
    this.meta.written = state.written;
    this.meta.node = state.node;
    this.meta.user = state.user;
    // What waits to be written is in it.
    this.ops = [];
    this.metaChanged = false;
    this.queued = [{ rewrite: state.records.map((r) => this.row(r)) }];
    this.queuedMeta = true;
    if (!this.busy) this.commit();
  }

  /** What is queued, in one transaction; transactions of one database run in the order they were made. */
  private commit(): void {
    if (this.queued.length === 0 && !this.queuedMeta) return;
    const ops = this.queued;
    const rewrites = ops.some((op) => 'rewrite' in op);
    const written = this.queuedWritten;
    this.queued = [];
    this.queuedMeta = false;
    this.queuedWritten = [];
    if (this.stale && !rewrites) return;
    let tx: IDBTransaction;
    try {
      tx = this.db.transaction([PRODUCERS, OUTBOX], 'readwrite', { durability: 'relaxed' });
      const outbox = tx.objectStore(OUTBOX);
      const producer = this.meta.producer;
      for (const op of ops) {
        if ('put' in op) outbox.put(op.put);
        else if ('delete' in op) outbox.delete([producer, op.delete]);
        else if ('deleteTo' in op) outbox.delete(IDBKeyRange.bound([producer, 0], [producer, op.deleteTo]));
        else {
          outbox.delete(rangeOf(producer));
          for (const row of op.rewrite) outbox.put(row);
        }
      }
      tx.objectStore(PRODUCERS).put({ ...this.meta });
    } catch (error) {
      this.fail(error);
      return;
    }
    this.busy = true;
    this.running = written;
    tx.oncomplete = () => {
      this.busy = false;
      if (rewrites) this.stale = false;
      this.failing = false;
      const running = this.running;
      this.running = [];
      for (const told of running) told();
      if (!this.closed) this.commit();
    };
    const failed = () => {
      this.busy = false;
      this.running = [];
      this.fail(tx.error ?? new Error('the transaction was aborted'));
      if (!this.closed) this.commit();
    };
    tx.onerror = (event) => {
      // Said once, by onabort.
      event.preventDefault();
    };
    tx.onabort = failed;
    (tx as IDBTransaction & { commit?: () => void }).commit?.();
  }

  private fail(error: unknown): void {
    this.broken = true;
    this.stale = true;
    if (this.failing) return;
    this.failing = true;
    this.report('write', error);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.stale) this.rewrite(this.state());
    else this.flush();
    // The last transaction is made now, behind any that runs: a page that
    // takes the producer over reads after it.
    if (this.busy) {
      this.busy = false;
      this.commit();
    }
    this.release();
    // Closes once its transactions are done.
    this.db.close();
  }
}

/**
 * Keeps the outbox in the IndexedDB database `name`, so that what waits
 * outlives the page: a page that opens takes over what a closed or crashed
 * tab of the same scope left. Needs Web Locks (HTTPS); without them, or
 * without IndexedDB, it is a `wseStorageError` event and the outbox stays
 * in memory.
 */
export function indexedDbOutbox(name: string, legacy?: LegacyDatabase): OutboxStore {
  /** The producer this store had last: taken again first when the page is back. */
  let mine: string | null = null;
  return {
    async open(state, report): Promise<OpenedStore> {
      if (typeof indexedDB === 'undefined') throw new Error('no IndexedDB here: the outbox stays in memory');
      if (!(globalThis.navigator as (Navigator & { locks?: LockManager }) | undefined)?.locks) {
        throw new Error('no Web Locks here (a page needs HTTPS for them): the outbox stays in memory');
      }
      const db = await openDatabase(name, report);
      try {
        // What 3.1 kept of this scope under a name of its own.
        if (legacy) await takeLegacy(db, legacy, report).catch((error) => report('legacy', error));
        const metas = (await done(db.transaction([PRODUCERS], 'readonly').objectStore(PRODUCERS).getAll())) as Meta[];
        if (mine) metas.sort((a, b) => Number(b.producer === mine) - Number(a.producer === mine));
        for (const listed of metas) {
          const release = await tryLock(`wse-outbox:${name}:${listed.producer}`);
          if (!release) continue;
          // Read again now that it is this page's: the page that held it
          // may have used it meanwhile (its user named, a message sent).
          const taken = await readProducer(db, listed.producer).catch((error) => {
            release();
            throw error;
          });
          if (!taken) {
            release();
            continue;
          }
          const { meta, read } = taken;
          if (read.records.length === 0 && meta.producer !== mine) {
            // A producer nobody holds and nothing waits in: it goes.
            const tx = db.transaction([PRODUCERS, OUTBOX], 'readwrite');
            tx.objectStore(PRODUCERS).delete(meta.producer);
            tx.objectStore(OUTBOX).delete(rangeOf(meta.producer));
            await committed(tx).catch(() => {});
            release();
            continue;
          }
          mine = meta.producer;
          const kept: Meta = { ...meta, next: read.next };
          return { read, log: new IdbLog(db, kept, release, state, report) };
        }
        // None to take over: a producer of this page's own.
        const meta: Meta = { producer: newProducer(), next: 1, written: 0, node: null, user: null };
        const release = await tryLock(`wse-outbox:${name}:${meta.producer}`);
        if (!release) throw new Error('a new producer is held already');
        const tx = db.transaction([PRODUCERS], 'readwrite');
        tx.objectStore(PRODUCERS).put(meta);
        await committed(tx).catch((error) => {
          release();
          throw error;
        });
        mine = meta.producer;
        return {
          read: { producer: meta.producer, node: null, written: 0, next: 1, user: null, records: [] },
          log: new IdbLog(db, meta, release, state, report),
        };
      } catch (error) {
        db.close();
        throw error;
      }
    },
  };
}
