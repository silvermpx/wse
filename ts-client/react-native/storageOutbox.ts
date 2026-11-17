// =============================================================================
// WebSocket Engine - The outbox in the app's storage (React Native)
// =============================================================================
//
// React Native has neither files nor IndexedDB for a library to write: the
// app passes the key-value storage it has, AsyncStorage (or one with its
// API, such as expo-sqlite's kv-store) or MMKV. Each message kept is a key of
// its own, `wse-outbox:<name>:<s>`, so that no item grows large, and the
// producer one more, `wse-outbox:<name>`. The changes of a turn of the event
// loop go in one batch, one batch at a time (AsyncStorage's multiSet and
// multiRemove where it has them). An app runs one JavaScript runtime: the
// outbox of a name is one client's while it runs.

import type { WSMessage } from '../types.js';
import type {
  OpenedStore,
  OutboxLine,
  OutboxLog,
  OutboxStore,
  ReadOutbox,
  StoredMessage,
  StoredOutbox,
} from '../services/Outbox.js';

/** AsyncStorage, as far as the outbox uses it. */
export interface AsyncStorageLike {
  getItem(key: string): Promise<string | null>;
  setItem(key: string, value: string): Promise<void>;
  removeItem(key: string): Promise<void>;
  getAllKeys(): Promise<readonly string[]>;
  multiGet?(keys: readonly string[]): Promise<ReadonlyArray<readonly [string, string | null]>>;
  multiSet?(pairs: ReadonlyArray<readonly [string, string]>): Promise<void>;
  multiRemove?(keys: readonly string[]): Promise<void>;
}

/** MMKV, as far as the outbox uses it. */
export interface MMKVLike {
  getString(key: string): string | undefined;
  set(key: string, value: string): void;
  delete(key: string): void;
  getAllKeys(): string[];
}

/** What the outbox does with the storage. */
interface Storage {
  keys(): Promise<readonly string[]>;
  get(keys: readonly string[]): Promise<Array<string | null>>;
  set(pairs: Array<[string, string]>): Promise<void>;
  remove(keys: string[]): Promise<void>;
}

function storageOf(storage: AsyncStorageLike | MMKVLike): Storage {
  if ('getString' in storage) {
    const mmkv = storage;
    return {
      keys: async () => mmkv.getAllKeys(),
      get: async (keys) => keys.map((key) => mmkv.getString(key) ?? null),
      set: async (pairs) => {
        for (const [key, value] of pairs) mmkv.set(key, value);
      },
      remove: async (keys) => {
        for (const key of keys) mmkv.delete(key);
      },
    };
  }
  const async = storage;
  return {
    keys: () => async.getAllKeys(),
    get: async (keys) =>
      async.multiGet
        ? (await async.multiGet(keys)).map(([, value]) => value)
        : Promise.all(keys.map((key) => async.getItem(key))),
    set: async (pairs) => {
      if (pairs.length === 0) return;
      if (async.multiSet) await async.multiSet(pairs);
      else await Promise.all(pairs.map(([key, value]) => async.setItem(key, value)));
    },
    remove: async (keys) => {
      if (keys.length === 0) return;
      if (async.multiRemove) await async.multiRemove(keys);
      else await Promise.all(keys.map((key) => async.removeItem(key)));
    },
  };
}

/** A producer, as the storage keeps it. */
interface Meta {
  producer: string;
  next: number;
  written: number;
  node: string | null;
  user: string | null;
}

/** A message kept, as the storage keeps it (its number is in its key). */
interface Item {
  id: string;
  at: number;
  until: number;
  owned: boolean;
  /** Topics it requires each time it goes. */
  requires?: string[];
  json: string;
}

/** A kept message as the storage keeps it. */
function itemOf(record: StoredMessage): Item {
  const item: Item = { id: record.id, at: record.at, until: record.until, owned: record.owned, json: record.json };
  if (record.requires && record.requires.length > 0) item.requires = record.requires;
  return item;
}

/** Topics a kept message requires, as the storage holds them; anything else is none. */
function topicsOf(value: unknown): string[] | undefined {
  return Array.isArray(value) && value.length > 0 && value.every((t) => typeof t === 'string' && t.length > 0)
    ? (value as string[])
    : undefined;
}

function newProducer(): string {
  const bytes = new Uint8Array(16);
  globalThis.crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
}

const isCount = (value: unknown): value is number =>
  typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;

/** The names of this process's open outboxes: each is one client's. */
const open = new Set<string>();

class StorageLog implements OutboxLog {
  /** The storage keeps a key for each message: no lines to count. */
  readonly lines = 0;
  /** The numbers the storage holds a message of, none of them below `low`. */
  private readonly held: Set<number>;
  private low = 1;
  private sets = new Map<string, string>();
  private removes = new Set<string>();
  private metaChanged = false;
  private busy = false;
  /** A batch waits for the one that runs. */
  private waiting: { sets: Map<string, string>; removes: Set<string> } | null = null;
  private broken = false;
  /** A batch failed: the storage lacks what it held, and is written anew before anything is added. */
  private stale = false;
  private failing = false;
  private closed = false;

  constructor(
    private readonly storage: Storage,
    private readonly prefix: string,
    private readonly name: string,
    private readonly meta: Meta,
    held: Iterable<number>,
    private readonly report: (operation: string, error: unknown) => void,
  ) {
    this.held = new Set(held);
    let low = meta.next;
    for (const s of this.held) if (s < low) low = s;
    this.low = low;
  }

  private key(s: number): string {
    return `${this.prefix}${s}`;
  }

  add(record: StoredMessage): void {
    const key = this.key(record.s);
    const item = itemOf(record);
    this.removes.delete(key);
    this.sets.set(key, JSON.stringify(item));
    this.held.add(record.s);
    if (record.s >= this.meta.next) {
      this.meta.next = record.s + 1;
      this.metaChanged = true;
    }
  }

  private remove(s: number): void {
    const key = this.key(s);
    this.held.delete(s);
    this.sets.delete(key);
    this.removes.add(key);
  }

  put(line: OutboxLine): void {
    if ('ack' in line) {
      // From the lowest held on: each number once.
      for (let s = this.low; s <= line.ack; s++) if (this.held.has(s)) this.remove(s);
      this.low = Math.max(this.low, line.ack + 1);
    } else if ('drop' in line) {
      for (const s of line.drop) this.remove(s);
    } else {
      if ('written' in line) this.meta.written = line.written;
      else if ('user' in line) this.meta.user = line.user;
      else this.meta.node = line.node;
      this.metaChanged = true;
    }
  }

  flush(): void {
    if (this.sets.size === 0 && this.removes.size === 0 && !this.metaChanged) return;
    if (this.metaChanged) this.sets.set(this.name, JSON.stringify(this.meta));
    const batch = { sets: this.sets, removes: this.removes };
    this.sets = new Map();
    this.removes = new Set();
    this.metaChanged = false;
    if (this.waiting) {
      // Behind the one that waits: one batch, the later change of a key winning.
      for (const key of batch.removes) {
        this.waiting.sets.delete(key);
        this.waiting.removes.add(key);
      }
      for (const [key, value] of batch.sets) {
        this.waiting.removes.delete(key);
        this.waiting.sets.set(key, value);
      }
    } else {
      this.waiting = batch;
    }
    this.kick();
  }

  forget(): void {
    this.sets = new Map();
    this.removes = new Set();
  }

  takeBroken(): boolean {
    const broken = this.broken;
    this.broken = false;
    return broken;
  }

  rewrite(state: StoredOutbox): void {
    this.meta.next = Math.max(this.meta.next, state.next);
    this.meta.written = state.written;
    this.meta.node = state.node;
    this.meta.user = state.user;
    // Every key it holds goes, but the ones written again.
    const sets = new Map<string, string>([[this.name, JSON.stringify(this.meta)]]);
    const removes = new Set<string>([...this.held].map((s) => this.key(s)));
    this.held.clear();
    for (const record of state.records) {
      const key = this.key(record.s);
      const item = itemOf(record);
      removes.delete(key);
      sets.set(key, JSON.stringify(item));
      this.held.add(record.s);
    }
    this.sets = new Map();
    this.removes = new Set();
    this.metaChanged = false;
    this.waiting = { sets, removes };
    this.rewriting = true;
    this.kick();
  }

  /** The waiting batch is a whole one: it clears what a failed batch left. */
  private rewriting = false;

  private kick(): void {
    if (this.busy || !this.waiting) return;
    const batch = this.waiting;
    const whole = this.rewriting;
    this.waiting = null;
    this.rewriting = false;
    if (this.stale && !whole) return;
    this.busy = true;
    // Removed first, then set. A batch never names a key in both, so an app
    // killed between the two loses no message kept before, and keeps none
    // that went (a sign-out's: its user's messages go before the user does).
    const done = this.storage
      .remove([...batch.removes])
      .then(() => this.storage.set([...batch.sets]));
    done.then(
      () => {
        this.busy = false;
        if (whole) this.stale = false;
        this.failing = false;
        this.kick();
      },
      (error) => {
        this.busy = false;
        this.fail(error);
        this.kick();
      },
    );
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
    this.flush();
    open.delete(this.name);
  }
}

/**
 * Keeps the outbox in the app's storage under `name` (default `wse`), so
 * that what waits outlives the app: AsyncStorage
 * (`@react-native-async-storage/async-storage`, or one with its API), or an
 * MMKV instance. `createReactNativeClient({ offline: { store:
 * storageOutbox(AsyncStorage) } })`. One that cannot be read or written is
 * a `wseStorageError` event, and the outbox goes on in memory.
 */
export function storageOutbox(storage: AsyncStorageLike | MMKVLike, name = 'wse'): OutboxStore {
  const kv = storageOf(storage);
  const metaKey = `wse-outbox:${name}`;
  const prefix = `${metaKey}:`;
  return {
    async open(_state, report): Promise<OpenedStore> {
      if (open.has(metaKey)) throw new Error(`the outbox ${name} is another client's`);
      open.add(metaKey);
      try {
        const keys = (await kv.keys()).filter((key) => key === metaKey || key.startsWith(prefix));
        const values = keys.length > 0 ? await kv.get(keys) : [];
        let meta: Meta | null = null;
        const records: ReadOutbox['records'] = [];
        for (let i = 0; i < keys.length; i++) {
          const value = values[i];
          if (value === null) continue;
          let parsed: unknown;
          try {
            parsed = JSON.parse(value);
          } catch {
            continue;
          }
          if (keys[i] === metaKey) {
            const m = parsed as Partial<Meta>;
            if (typeof m?.producer === 'string') {
              meta = {
                producer: m.producer,
                next: isCount(m.next) ? m.next : 1,
                written: isCount(m.written) ? m.written : 0,
                node: typeof m.node === 'string' ? m.node : null,
                user: typeof m.user === 'string' ? m.user : null,
              };
            }
            continue;
          }
          const s = Number(keys[i].slice(prefix.length));
          const item = parsed as Partial<Item>;
          if (!isCount(s) || s === 0 || typeof item?.id !== 'string' || typeof item.json !== 'string') continue;
          if (!isCount(item.at) || !isCount(item.until) || typeof item.owned !== 'boolean') continue;
          let message: WSMessage;
          try {
            message = JSON.parse(item.json) as WSMessage;
          } catch {
            continue;
          }
          records.push({
            s,
            id: item.id,
            at: item.at,
            until: item.until,
            owned: item.owned,
            requires: topicsOf(item.requires),
            json: item.json,
            message,
          });
        }
        records.sort((a, b) => a.s - b.s);
        // Messages without their producer: written before it, or a producer
        // lost; they go on as this one's.
        meta ??= { producer: newProducer(), next: 1, written: 0, node: null, user: null };
        let next = Math.max(1, meta.next, meta.written + 1);
        for (const record of records) next = Math.max(next, record.s + 1);
        meta.next = next;
        const read: ReadOutbox = {
          producer: meta.producer,
          node: meta.node,
          written: meta.written,
          next,
          user: meta.user,
          records,
        };
        const log = new StorageLog(kv, prefix, metaKey, meta, records.map((r) => r.s), report);
        if (!keys.includes(metaKey)) {
          // The producer, kept before anything it sends.
          await kv.set([[metaKey, JSON.stringify(meta)]]);
        }
        return { read, log };
      } catch (error) {
        open.delete(metaKey);
        throw error;
      }
    },
  };
}
