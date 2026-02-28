// =============================================================================
// WebSocket Engine - Offline Queue (IndexedDB)
// =============================================================================

export interface OfflineQueueConfig {
  enabled: boolean;
  maxSize: number;
  maxAge: number;
  persistToStorage: boolean;
}

export class OfflineQueue {
  private dbName = 'wse_offline_queue';
  private storeName = 'messages';
  private db: IDBDatabase | null = null;
  private config: OfflineQueueConfig;
  private _size = 0;

  constructor(config: OfflineQueueConfig) {
    this.config = config;
  }

  async initialize(): Promise<void> {
    if (!this.config.persistToStorage) return;
    if (typeof indexedDB === 'undefined') return;

    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbName, 1);

      request.onerror = () => reject(request.error);
      request.onsuccess = () => {
        this.db = request.result;
        // Sync _size from existing IndexedDB entries
        const tx = this.db.transaction([this.storeName], 'readonly');
        const countReq = tx.objectStore(this.storeName).count();
        countReq.onsuccess = () => { this._size = countReq.result; };
        resolve();
      };

      request.onupgradeneeded = (event) => {
        const db = (event.target as IDBOpenDBRequest).result;
        if (!db.objectStoreNames.contains(this.storeName)) {
          const store = db.createObjectStore(this.storeName, { keyPath: 'id' });
          store.createIndex('timestamp', 'timestamp');
          store.createIndex('priority', 'priority');
        }
      };
    });
  }

  async enqueue(message: any): Promise<void> {
    if (!this.config.enabled || !this.db) return;
    if (this._size >= this.config.maxSize) return;

    await this.cleanup();

    const transaction = this.db.transaction([this.storeName], 'readwrite');
    const store = transaction.objectStore(this.storeName);

    return new Promise((resolve, reject) => {
      const request = store.add({
        ...message,
        timestamp: Date.now(),
        retries: 0,
      });
      request.onsuccess = () => { this._size++; resolve(); };
      request.onerror = () => reject(request.error);
    });
  }

  async getAll(): Promise<any[]> {
    if (!this.db) return [];

    const transaction = this.db.transaction([this.storeName], 'readonly');
    const store = transaction.objectStore(this.storeName);
    const index = store.index('priority');

    return new Promise((resolve, reject) => {
      const request = index.openCursor(null, 'prev');
      const messages: any[] = [];

      request.onsuccess = (event) => {
        const cursor = (event.target as IDBRequest).result;
        if (cursor) {
          messages.push(cursor.value);
          cursor.continue();
        } else {
          resolve(messages);
        }
      };
      request.onerror = () => reject(request.error);
    });
  }

  async clear(): Promise<void> {
    if (!this.db) return;

    const transaction = this.db.transaction([this.storeName], 'readwrite');
    const store = transaction.objectStore(this.storeName);

    return new Promise((resolve, reject) => {
      const request = store.clear();
      request.onsuccess = () => { this._size = 0; resolve(); };
      request.onerror = () => reject(request.error);
    });
  }

  async cleanup(): Promise<void> {
    if (!this.db) return;

    const cutoff = Date.now() - this.config.maxAge;
    const transaction = this.db.transaction([this.storeName], 'readwrite');
    const store = transaction.objectStore(this.storeName);
    const index = store.index('timestamp');
    const range = IDBKeyRange.upperBound(cutoff);

    return new Promise((resolve, reject) => {
      const request = index.openCursor(range);
      request.onsuccess = (event) => {
        const cursor = (event.target as IDBRequest).result;
        if (cursor) {
          cursor.delete();
          this._size = Math.max(0, this._size - 1);
          cursor.continue();
        } else {
          resolve();
        }
      };
      request.onerror = () => reject(request.error);
    });
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
