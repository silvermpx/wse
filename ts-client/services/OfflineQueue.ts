// =============================================================================
// WebSocket Engine - Offline Queue (IndexedDB)
// =============================================================================

import type { WSMessage } from '../types.js';
import { logger } from '../utils/logger.js';

export interface OfflineQueueConfig {
  enabled: boolean;
  maxSize: number;
  maxAge: number;
  persistToStorage: boolean;
  /** The IndexedDB database. Default `wse_offline_queue` (the default client's). */
  dbName?: string;
}

/** One persisted message. `message` is the complete wire message (cid/enc kept). */
export interface OfflineRecord {
  id: string;
  timestamp: number;
  retries: number;
  message: WSMessage;
  /** Epoch ms after which the message is dropped instead of sent (MessageOptions.ttl). */
  expiresAt?: number;
  /** The user it was written for; replayed only on that user's connection. */
  owner?: string;
}

function requestToPromise<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function transactionDone(tx: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
}

/** Records written by 2.x stored the WSMessage itself plus timestamp/retries. */
function toRecord(raw: any): OfflineRecord | null {
  if (!raw || typeof raw !== 'object' || typeof raw.id !== 'string') return null;
  if (raw.message && typeof raw.message === 'object' && typeof raw.message.t === 'string') {
    return {
      id: raw.id,
      timestamp: typeof raw.timestamp === 'number' ? raw.timestamp : 0,
      retries: typeof raw.retries === 'number' ? raw.retries : 0,
      message: raw.message as WSMessage,
      expiresAt: typeof raw.expiresAt === 'number' ? raw.expiresAt : undefined,
      owner: typeof raw.owner === 'string' ? raw.owner : undefined,
    };
  }
  if (typeof raw.t === 'string') {
    const { timestamp, retries, ...message } = raw;
    return {
      id: raw.id,
      timestamp: typeof timestamp === 'number' ? timestamp : 0,
      retries: typeof retries === 'number' ? retries : 0,
      message: message as WSMessage,
    };
  }
  return null;
}

export class OfflineQueue {
  private dbName = 'wse_offline_queue';
  private storeName = 'messages';
  private db: IDBDatabase | null = null;
  private config: OfflineQueueConfig;
  private _size = 0;
  // Strictly increasing record timestamps: the 'timestamp' index orders equal
  // keys by id (a random UUID), which would break FIFO within one millisecond.
  private lastTimestamp = 0;

  constructor(config: OfflineQueueConfig) {
    this.config = config;
    if (config.dbName) this.dbName = config.dbName;
  }

  async initialize(): Promise<void> {
    if (!this.config.enabled || !this.config.persistToStorage) return;
    if (typeof indexedDB === 'undefined') return;

    const db = await new Promise<IDBDatabase>((resolve, reject) => {
      const request = indexedDB.open(this.dbName, 1);

      request.onerror = () => reject(request.error);
      request.onsuccess = () => resolve(request.result);

      request.onupgradeneeded = (event) => {
        const upgraded = (event.target as IDBOpenDBRequest).result;
        if (!upgraded.objectStoreNames.contains(this.storeName)) {
          const store = upgraded.createObjectStore(this.storeName, { keyPath: 'id' });
          store.createIndex('timestamp', 'timestamp');
          // Kept for schema compatibility with 2.x databases; not read any more
          // (records never carried a 'priority' field, so reading through this
          // index returned nothing and the queue never replayed).
          store.createIndex('priority', 'priority');
        }
      };
    });

    this.db = db;
    // What a page left behind longer ago than maxAge goes before anything
    // reads it.
    await this.cleanup();
    await this.refreshSize();
  }

  /** True when messages can be persisted (IndexedDB open and the queue enabled). */
  isAvailable(): boolean {
    return this.config.enabled && this.db !== null;
  }

  /**
   * Persist one message. Resolves false when the queue is disabled,
   * unavailable or full; the message is not stored then.
   */
  async enqueue(message: WSMessage, expiresAt?: number, owner?: string): Promise<boolean> {
    if (!this.isAvailable() || !this.db) return false;

    const id = message.id;
    if (!id) return false;

    await this.cleanup();
    const db = this.db;
    if (!db) return false;

    this.lastTimestamp = Math.max(Date.now(), this.lastTimestamp + 0.001);
    const record: OfflineRecord = { id, timestamp: this.lastTimestamp, retries: 0, message };
    if (expiresAt !== undefined) record.expiresAt = expiresAt;
    if (owner !== undefined) record.owner = owner;

    // The room is counted in the transaction that writes: IndexedDB runs
    // read-write transactions on one store one after another, so concurrent
    // enqueues (of this page or another tab) never all see the last free
    // place. A message already stored may be written again.
    const maxSize = this.config.maxSize;
    const stored = await new Promise<boolean>((resolve, reject) => {
      const tx = db.transaction([this.storeName], 'readwrite');
      const store = tx.objectStore(this.storeName);
      let written = false;
      const total = store.count();
      total.onsuccess = () => {
        const same = store.count(id);
        same.onsuccess = () => {
          if (same.result === 0 && total.result >= maxSize) return;
          store.put(record);
          written = true;
        };
      };
      tx.oncomplete = () => resolve(written);
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error);
    });
    await this.refreshSize();
    if (!stored) logger.warn(`Offline queue full (${maxSize}), message not persisted`);
    return stored;
  }

  /** All persisted messages, oldest first; none older than maxAge. */
  async getAll(): Promise<OfflineRecord[]> {
    if (!this.db) return [];

    await this.cleanup();
    if (!this.db) return [];
    const tx = this.db.transaction([this.storeName], 'readonly');
    const index = tx.objectStore(this.storeName).index('timestamp');

    const records = await new Promise<OfflineRecord[]>((resolve, reject) => {
      const out: OfflineRecord[] = [];
      const request = index.openCursor();
      request.onsuccess = () => {
        const cursor = request.result;
        if (cursor) {
          const record = toRecord(cursor.value);
          if (record) out.push(record);
          cursor.continue();
        } else {
          resolve(out);
        }
      };
      request.onerror = () => reject(request.error);
    });

    this._size = records.length;
    return records;
  }

  /**
   * Take up to `limit` messages out, oldest first, in one transaction: a
   * message is handed to one taker only, however many clients (tabs) share
   * the database. What the taker does not send it puts back (restore).
   */
  async claim(limit: number): Promise<OfflineRecord[]> {
    if (!this.db) return [];

    await this.cleanup();
    if (!this.db) return [];
    const tx = this.db.transaction([this.storeName], 'readwrite');
    const index = tx.objectStore(this.storeName).index('timestamp');
    const done = transactionDone(tx);

    const records = await new Promise<OfflineRecord[]>((resolve, reject) => {
      const out: OfflineRecord[] = [];
      const request = index.openCursor();
      request.onsuccess = () => {
        const cursor = request.result;
        if (cursor && out.length < limit) {
          // An unreadable record goes too: nothing could ever send it.
          const record = toRecord(cursor.value);
          if (record) out.push(record);
          cursor.delete();
          cursor.continue();
        } else {
          resolve(out);
        }
      };
      request.onerror = () => reject(request.error);
    });
    await done;
    await this.refreshSize();
    return records;
  }

  /** Put back messages taken with claim() and not sent: they keep their place. */
  async restore(records: OfflineRecord[]): Promise<void> {
    if (!this.db || records.length === 0) return;

    const tx = this.db.transaction([this.storeName], 'readwrite');
    const store = tx.objectStore(this.storeName);
    for (const record of records) store.put(record);
    await transactionDone(tx);
    await this.refreshSize();
  }

  /** Delete the given ids (the messages that were actually sent). */
  async remove(ids: string[]): Promise<void> {
    if (!this.db || ids.length === 0) return;

    const tx = this.db.transaction([this.storeName], 'readwrite');
    const store = tx.objectStore(this.storeName);
    for (const id of ids) {
      store.delete(id);
    }
    await transactionDone(tx);
    await this.refreshSize();
  }

  private async refreshSize(): Promise<void> {
    if (!this.db) return;
    try {
      const tx = this.db.transaction([this.storeName], 'readonly');
      this._size = await requestToPromise(tx.objectStore(this.storeName).count());
    } catch (error) {
      logger.debug('Offline queue: count failed', error);
    }
  }

  async clear(): Promise<void> {
    if (!this.db) return;

    const tx = this.db.transaction([this.storeName], 'readwrite');
    tx.objectStore(this.storeName).clear();
    await transactionDone(tx);
    this._size = 0;
  }

  /** Drop messages older than maxAge. */
  async cleanup(): Promise<void> {
    if (!this.db) return;

    const cutoff = Date.now() - this.config.maxAge;
    const tx = this.db.transaction([this.storeName], 'readwrite');
    const index = tx.objectStore(this.storeName).index('timestamp');
    let removed = 0;
    const done = transactionDone(tx);

    await new Promise<void>((resolve, reject) => {
      const request = index.openCursor(IDBKeyRange.upperBound(cutoff));
      request.onsuccess = () => {
        const cursor = request.result;
        if (cursor) {
          cursor.delete();
          removed++;
          cursor.continue();
        } else {
          resolve();
        }
      };
      request.onerror = () => reject(request.error);
    });
    await done;
    if (removed > 0) await this.refreshSize();
  }

  getStats() {
    return {
      size: this._size,
      capacity: this.config.maxSize,
      enabled: this.config.enabled,
      persistToStorage: this.config.persistToStorage,
    };
  }

  destroy() {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }
}
