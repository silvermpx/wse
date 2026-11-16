// =============================================================================
// WebSocket Engine - Client Context
// =============================================================================
//
// Everything one client keeps for itself: its state and outgoing queue, its
// E2E keys, whose data it holds, where its events go and what it stores. The
// services of a client read all of it from here, so two clients on one page
// share nothing. The default client (useWSE) gets the page's: the zustand
// stores, the window's events and the `wse_` keys of localStorage.

import { createStore, type StoreApi } from './store.js';
import { createWSEState, type WSEStore } from './state/wseState.js';
import { createQueueState, type MessageQueueStore } from './state/queueState.js';
import { SecurityManager } from '../utils/security.js';
import type { WSMessage } from '../types.js';
import type { TopicPosition } from '../services/EventSequencer.js';

/**
 * Whose data the client's queues hold: a new number each time it drops one
 * user's data (queue, topics, positions) for another's. A message made under
 * an older number is that user's, and it does not go out any more.
 */
export interface OwnerGeneration {
  current(): number;
  next(): number;
}

/** Where a client keeps what outlives a page (its topics, the offline queue's owner). */
export interface ClientStorage {
  get(key: string): string | null;
  set(key: string, value: string): void;
  remove(key: string): void;
  /** Removes everything this client keeps, its offline database included. */
  clear(): void;
  /** The IndexedDB database of the offline queue; null: no persistent queue. */
  readonly offlineDatabase: string | null;
}

/** An event of a client: `wseGap`, `wseMessage`, `wseEncryptionFailed`, ... */
export type Emit = (type: string, detail?: unknown) => void;

export interface ClientContext {
  readonly state: StoreApi<WSEStore>;
  readonly queue: StoreApi<MessageQueueStore>;
  readonly security: SecurityManager;
  readonly owner: OwnerGeneration;
  readonly emit: Emit;
  readonly storage: ClientStorage;
  /** A client of its own: its topics' delivery (core/delivery.ts). */
  readonly delivery?: DeliveryGate;
}

/** Where a client's delivery meets its services. */
export interface DeliveryGate {
  /** A message of a topic, about to reach the handlers: true when it is held instead. */
  hold(message: WSMessage): boolean;
  /** Where the recovery of `topics` starts (what the application committed); null: where delivery is. */
  recoveryPositions(topics: readonly string[]): Record<string, TopicPosition> | null;
}

export function ownerGeneration(): OwnerGeneration {
  let current = 0;
  return {
    current: () => current,
    next: () => ++current,
  };
}

/** Kept in memory only: what a client keeps goes with it. */
export function memoryStorage(): ClientStorage {
  const values = new Map<string, string>();
  return {
    get: (key) => values.get(key) ?? null,
    set: (key, value) => {
      values.set(key, value);
    },
    remove: (key) => {
      values.delete(key);
    },
    clear: () => values.clear(),
    offlineDatabase: null,
  };
}

/** The page's localStorage, when there is one and it may be used. */
function pageStorage(): Storage | null {
  try {
    return typeof localStorage !== 'undefined' ? localStorage : null;
  } catch {
    return null;
  }
}

/** Deletes an IndexedDB database, when the environment has IndexedDB. */
export function deleteDatabase(name: string): void {
  if (typeof indexedDB === 'undefined') return;
  try {
    indexedDB.deleteDatabase(name);
  } catch {
    // ignore
  }
}

/**
 * The page's localStorage under `prefix` (each key is `prefix + key`), and
 * an offline database named `database` (null: none). `clear` removes the
 * keys that start with `prefix` and deletes the database.
 */
export function prefixedStorage(prefix: string, database: string | null): ClientStorage {
  const each = (run: (storage: Storage) => void) => {
    const storage = pageStorage();
    if (!storage) return;
    try {
      run(storage);
    } catch {
      // Storage full, blocked or gone: what is kept is a convenience.
    }
  };
  return {
    get: (key) => {
      let value: string | null = null;
      each((storage) => {
        value = storage.getItem(prefix + key);
      });
      return value;
    },
    set: (key, value) => each((storage) => storage.setItem(prefix + key, value)),
    remove: (key) => each((storage) => storage.removeItem(prefix + key)),
    clear: () => {
      each((storage) => {
        const keys: string[] = [];
        for (let i = 0; i < storage.length; i++) {
          const key = storage.key(i);
          if (key && key.startsWith(prefix)) keys.push(key);
        }
        for (const key of keys) storage.removeItem(key);
      });
      if (database) deleteDatabase(database);
    },
    offlineDatabase: database,
  };
}

/**
 * The page's storage for a client of its own: keys under
 * `wse:<namespace>:`, and the offline queue in database `wse:<namespace>`.
 * The namespace names the application and the user, so clients of other
 * users, or of the default client (`wse_` keys), are never read.
 */
export function namespacedStorage(namespace: string): ClientStorage {
  return prefixedStorage(`wse:${namespace}:`, `wse:${namespace}`);
}

export interface ContextOptions {
  emit?: Emit;
  storage?: ClientStorage;
  delivery?: DeliveryGate;
}

/** A context of its own: new stores, keys and owner. */
export function createClientContext(options: ContextOptions = {}): ClientContext {
  return {
    state: createStore(createWSEState),
    queue: createStore(createQueueState),
    security: new SecurityManager(),
    owner: ownerGeneration(),
    emit: options.emit ?? (() => {}),
    storage: options.storage ?? memoryStorage(),
    delivery: options.delivery,
  };
}

// ---------------------------------------------------------------------------
// What a client keeps
// ---------------------------------------------------------------------------

/** Key of the topics a client subscribed to, with the user they were saved for. */
export const SUBSCRIPTIONS_KEY = 'subscriptions';
/** Key of the user whose messages the offline queue holds. */
export const OFFLINE_OWNER_KEY = 'offline_owner';

function topicList(value: unknown): string[] | null {
  if (!Array.isArray(value)) return null;
  return [...new Set(value.filter((t): t is string => typeof t === 'string' && t.length > 0))];
}

/** The topics saved for `userId`, or null (none, or another user's). */
export function loadTopics(storage: ClientStorage, userId?: string | null): string[] | null {
  try {
    const raw = storage.get(SUBSCRIPTIONS_KEY);
    if (!raw) return null;
    const parsed: unknown = JSON.parse(raw);
    const wanted = userId ?? null;
    if (Array.isArray(parsed)) {
      // Pre-3.0 format carried no user; only trust it without a user id.
      return wanted === null ? topicList(parsed) : null;
    }
    if (parsed && typeof parsed === 'object') {
      const record = parsed as { userId?: unknown; topics?: unknown };
      const owner = typeof record.userId === 'string' ? record.userId : null;
      if (owner !== wanted) return null;
      return topicList(record.topics);
    }
    return null;
  } catch {
    return null;
  }
}

export function saveTopics(storage: ClientStorage, topics: string[], userId?: string | null): void {
  storage.set(SUBSCRIPTIONS_KEY, JSON.stringify({ userId: userId ?? null, topics: topicList(topics) ?? [] }));
}
