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
}

/** One persisted message. `message` is the complete wire message (cid/enc kept). */
export interface OfflineRecord {
  id: string;
  timestamp: number;
  retries: number;
  message: WSMessage;
  /** Epoch ms after which the message is dropped instead of sent (MessageOptions.ttl). */
  expiresAt?: number;
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
  async enqueue(message: WSMessage, expiresAt?: number): Promise<boolean> {
    if (!this.isAvailable() || !this.db) return false;

    await this.cleanup();
    if (this._size >= this.config.maxSize) {
      logger.warn(`Offline queue full (${this.config.maxSize}), message not persisted`);
      return false;
    }

    const id = message.id;
    if (!id) return false;

    this.lastTimestamp = Math.max(Date.now(), this.lastTimestamp + 0.001);
    const record: OfflineRecord = { id, timestamp: this.lastTimestamp, retries: 0, message };
    if (expiresAt !== undefined) record.expiresAt = expiresAt;
    const tx = this.db.transaction([this.storeName], 'readwrite');
    tx.objectStore(this.storeName).put(record);
    await transactionDone(tx);
    await this.refreshSize();
    return true;
  }

  /** All persisted messages, oldest first. */
  async getAll(): Promise<OfflineRecord[]> {
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
