// =============================================================================
// WebSocket Engine - Client (core)
// =============================================================================
//
// A client of its own: made inert, started by the first share taken of it
// (retain) and stopped one tick after the last is released, so a component
// that mounts, unmounts and mounts again (React Strict Mode, hot reload)
// keeps one connection. Its state, queue, keys, topics, events and storage
// are its own: two clients on one page share nothing. Frameworks hold it
// through their adapters (wse-client/react, /vue, /svelte).

import {
  ConnectionState,
  MessagePriority,
  type MessageOptions,
  type SendStatus,
  type SignedMessageOptions,
  type WSMessage,
} from '../types.js';
import { dispatchWSEEvent } from '../utils/events.js';
import { logger } from '../utils/logger.js';
import { EventSequencer } from '../services/EventSequencer.js';
import {
  WSESession,
  buildMessage,
  expiryOf,
  resolveOptions,
  uniqueTopics,
  type DataOwner,
  type UseWSEConfig,
} from '../services/Session.js';
import {
  createClientContext,
  memoryStorage,
  namespacedStorage,
  type ClientContext,
  type ClientStorage,
} from './context.js';
import type { StoreApi } from './store.js';
import type { WSEStore } from './state/wseState.js';

export interface WSEClientOptions extends UseWSEConfig {
  /**
   * The auth mode, which also names whose data the client holds. 'cookie'
   * (default): the browser sends the page's cookies, and the server
   * authenticates it by its cookie; the value itself is never sent. Outside
   * browsers a `webSocket` factory may send a token in a header.
   */
  auth?: string;
  /** Topics subscribed to from the start, besides those subscribe() adds. */
  topics?: string[];
  /**
   * What the client keeps across page loads (its topics, the offline queue):
   * a namespace of the page's storage, one that names the application and the
   * user ('myapp:alice'); a storage of its own; or, by default, nothing
   * (kept in memory, and the offline queue is not persisted).
   */
  storage?: string | ClientStorage;
  /** Also dispatch the client's events on the page's window, as useWSE does. Default false. */
  windowEvents?: boolean;
}

/** A share of a client (or of its topics), released once. */
export interface Lease {
  release(): void;
}

/** What a page shows of a client; the same object until something in it changes. */
export interface WSEClientSnapshot {
  readonly connection: ConnectionState;
  /** The reasons the connection is paused for (none: not paused). */
  readonly paused: readonly string[];
  /** The server took the connection: messages go out. */
  readonly ready: boolean;
  readonly endpoint: string | null;
  readonly topics: readonly string[];
  readonly lastError: string | null;
}

export interface WSEClientState {
  getSnapshot(): WSEClientSnapshot;
  /** Called on each change of the snapshot; returns the unsubscribe. */
  listen(listener: (snapshot: WSEClientSnapshot) => void): () => void;
  /** Svelte's store contract: called at once with the snapshot, then on each change. */
  subscribe(listener: (snapshot: WSEClientSnapshot) => void): () => void;
}

export interface WSEClient {
  /** Take a share of the client: the first one starts it. */
  retain(): Lease;
  readonly state: WSEClientState;
  /** All of the client's state (metrics, queue stats, diagnostics), as a store. */
  readonly store: StoreApi<WSEStore>;
  /** Hear the messages of `type`; returns the unsubscribe. Any number of listeners per type. */
  onMessage(type: string, listener: (message: WSMessage) => void): () => void;
  /** Hear the client's events (`wseGap`, `wseEncryptionFailed`, ...); returns the unsubscribe. */
  on(event: string, listener: (detail: any) => void): () => void;
  /** Hold `topics`: subscribed while any lease of them is held. */
  subscribe(topics: string[], options?: { recover?: boolean }): Lease;
  send(type: string, payload: unknown, options?: MessageOptions): SendStatus;
  sendSigned(type: string, payload: unknown, options?: SignedMessageOptions): Promise<SendStatus>;
  reconnect(): void;
  /**
   * Pause the connection for `reason` ('background', 'offline', 'manual', ...):
   * it ends while any reason holds, and shares, topics and positions stay.
   */
  pause(reason: string): void;
  /** Clear `reason`; once none is left the client connects again and recovers what it missed. */
  resume(reason: string): void;
  /**
   * The user signed out: the connection ends, and what the client held for
   * the user (queue, topics, positions, what it kept in storage) goes. The
   * client is done; a new sign-in makes a new client.
   */
  signOut(): void;
  /** The client is done: the connection ends; what it kept in storage stays. */
  destroy(): void;
}

// Released shares stop the client this much later: a remount in between keeps it.
const STOP_DELAY_MS = 0;

function shallowSame(a: WSEClientSnapshot, b: WSEClientSnapshot): boolean {
  return (
    a.connection === b.connection &&
    a.paused === b.paused &&
    a.ready === b.ready &&
    a.endpoint === b.endpoint &&
    a.lastError === b.lastError &&
    a.topics === b.topics
  );
}

export function createWSEClient(options: WSEClientOptions = {}): WSEClient {
  const auth = options.auth && options.auth.trim() !== '' ? options.auth : 'cookie';
  const listeners = new Map<string, Set<(detail: any) => void>>();
  const emit = (type: string, detail?: unknown) => {
    const set = listeners.get(type);
    if (set) {
      for (const listener of [...set]) {
        try {
          listener(detail);
        } catch (error) {
          logger.error(`A listener of ${type} failed:`, error);
        }
      }
    }
    if (options.windowEvents) dispatchWSEEvent(type, detail);
  };
  const storage: ClientStorage =
    typeof options.storage === 'string'
      ? namespacedStorage(options.storage)
      : (options.storage ?? memoryStorage());
  const ctx: ClientContext = createClientContext({ emit, storage });
  // The queue's limits hold from the first send, before anything starts.
  const { performance } = resolveOptions(options);
  ctx.queue.getState().setCapacity(performance.maxQueueSize);
  ctx.queue.getState().setMemoryLimit(performance.memoryLimit);
  const sequencer = new EventSequencer();
  const dataOwner: DataOwner = { auth, userId: null };

  let session: WSESession | null = null;
  let shares = 0;
  let stopTimer: ReturnType<typeof setTimeout> | null = null;
  let done = false;
  let paused: readonly string[] = [];
  const topicShares = new Map<string, number>();
  const messageListeners = new Map<string, Set<(message: WSMessage) => void>>();
  for (const topic of uniqueTopics(options.topics)) topicShares.set(topic, 1);

  // -- the snapshot ------------------------------------------------------

  const snapshotOf = (s: WSEStore): WSEClientSnapshot => ({
    connection: s.connectionState,
    paused,
    ready: s.transportReady && s.connectionState === ConnectionState.CONNECTED,
    endpoint: s.activeEndpoint,
    topics: s.activeTopics,
    lastError: s.lastError,
  });
  let snapshot = snapshotOf(ctx.state.getState());
  const snapshotListeners = new Set<(snapshot: WSEClientSnapshot) => void>();
  ctx.state.subscribe((s) => {
    const next = snapshotOf(s);
    if (shallowSame(next, snapshot)) return;
    snapshot = next;
    for (const listener of [...snapshotListeners]) listener(snapshot);
  });
  const state: WSEClientState = {
    getSnapshot: () => snapshot,
    listen: (listener) => {
      snapshotListeners.add(listener);
      return () => {
        snapshotListeners.delete(listener);
      };
    },
    subscribe: (listener) => {
      listener(snapshot);
      return state.listen(listener);
    },
  };

  // -- messages ------------------------------------------------------------

  const dispatcher = (type: string) => (message: WSMessage) => {
    for (const listener of [...(messageListeners.get(type) ?? [])]) {
      try {
        listener(message);
      } catch (error) {
        logger.error(`A listener of ${type} messages failed:`, error);
      }
    }
  };

  // -- the session -------------------------------------------------------------

  const start = () => {
    if (session || done || paused.length > 0) return;
    session = new WSESession(
      {
        key: auth,
        auth,
        // Only the topics held: no saved or default ones.
        initialTopics: [...topicShares.keys()],
        config: options,
      },
      { context: ctx, sequencer, dataOwner: () => dataOwner },
    );
    for (const type of messageListeners.keys()) {
      session.processor.registerHandler(type, dispatcher(type));
    }
    void session.start();
  };

  const stop = () => {
    const ended = session;
    session = null;
    ended?.destroy();
  };

  const live = () => (session && !session.destroyed ? session : null);

  const retain = (): Lease => {
    if (done) throw new Error('This WSE client was signed out or destroyed: make a new one');
    shares++;
    if (stopTimer) {
      clearTimeout(stopTimer);
      stopTimer = null;
    }
    start();
    let held = true;
    return {
      release: () => {
        if (!held) return;
        held = false;
        shares--;
        if (shares > 0 || done) return;
        stopTimer = setTimeout(() => {
          stopTimer = null;
          if (shares === 0) stop();
        }, STOP_DELAY_MS);
      },
    };
  };

  const subscribe = (topics: string[], subscribeOptions?: { recover?: boolean }): Lease => {
    const unique = uniqueTopics(topics);
    const added = unique.filter((topic) => {
      const n = topicShares.get(topic) ?? 0;
      topicShares.set(topic, n + 1);
      return n === 0;
    });
    if (added.length > 0 && !done) {
      const current = live();
      if (current) current.subscribe(added, subscribeOptions);
      else ctx.state.getState().addTopics(added);
    }
    let held = true;
    return {
      release: () => {
        if (!held) return;
        held = false;
        const removed = unique.filter((topic) => {
          const n = (topicShares.get(topic) ?? 1) - 1;
          if (n > 0) {
            topicShares.set(topic, n);
            return false;
          }
          topicShares.delete(topic);
          return true;
        });
        if (removed.length === 0 || done) return;
        const current = live();
        if (current) {
          current.unsubscribe(removed);
        } else {
          ctx.state.getState().removeTopics(removed);
          ctx.state.getState().removeRecoveryTopics(removed);
          sequencer.clearTopics(removed);
        }
      },
    };
  };

  const send = (type: string, payload: unknown, sendOptions: MessageOptions = {}): SendStatus => {
    if (done) return 'dropped';
    const current = live();
    if (current) return current.sendMessage(type, payload, sendOptions);
    // Not started yet: kept until the connection takes it, unless it is
    // for the connection of now only.
    if (sendOptions.live) {
      logger.warn(`${type} not sent: no live connection`);
      return 'dropped';
    }
    if (sendOptions.encrypted) {
      logger.error(`Refusing to queue ${type}: it requires encryption and no encrypted session exists`);
      return 'dropped';
    }
    const message = buildMessage(type, payload, sendOptions);
    const queue = ctx.queue.getState();
    const ok = queue.enqueue({
      id: message.id!,
      type,
      payload,
      priority: sendOptions.priority ?? MessagePriority.NORMAL,
      timestamp: Date.now(),
      retries: 0,
      correlation_id: message.cid,
      message,
      bytes: queue.memoryLimit > 0 ? JSON.stringify(message).length : undefined,
      expiresAt: expiryOf(sendOptions),
      owner: ctx.owner.current(),
    });
    return ok ? 'queued' : 'dropped';
  };

  const notifySnapshot = () => {
    const next = snapshotOf(ctx.state.getState());
    if (shallowSame(next, snapshot)) return;
    snapshot = next;
    for (const listener of [...snapshotListeners]) listener(snapshot);
  };

  const pause = (reason: string) => {
    if (done || paused.includes(reason)) return;
    paused = [...paused, reason];
    if (paused.length === 1) stop();
    notifySnapshot();
  };

  const resume = (reason: string) => {
    if (!paused.includes(reason)) return;
    paused = paused.filter((r) => r !== reason);
    notifySnapshot();
    if (paused.length === 0 && shares > 0) start();
  };

  const end = (clear: boolean) => {
    if (done) return;
    done = true;
    if (stopTimer) {
      clearTimeout(stopTimer);
      stopTimer = null;
    }
    stop();
    ctx.state.getState().clearTimers();
    if (clear) {
      ctx.owner.next();
      sequencer.reset();
      ctx.queue.getState().clearQueue();
      ctx.state.getState().reset();
      ctx.storage.clear();
    }
    ctx.state.getState().setConnectionState(ConnectionState.DISCONNECTED);
  };

  return {
    retain,
    state,
    store: ctx.state,
    onMessage: (type, listener) => {
      let set = messageListeners.get(type);
      if (!set) {
        set = new Set();
        messageListeners.set(type, set);
        live()?.processor.registerHandler(type, dispatcher(type));
      }
      set.add(listener);
      return () => {
        const current = messageListeners.get(type);
        if (!current?.delete(listener) || current.size > 0) return;
        messageListeners.delete(type);
        live()?.processor.unregisterHandler(type);
      };
    },
    on: (event, listener) => {
      let set = listeners.get(event);
      if (!set) {
        set = new Set();
        listeners.set(event, set);
      }
      set.add(listener);
      return () => {
        set!.delete(listener);
      };
    },
    subscribe,
    send,
    sendSigned: (type, payload, signOptions) => {
      const current = done ? null : live();
      if (!current) {
        logger.warn(`sendSigned(${type}): the client is not connected`);
        return Promise.resolve('dropped');
      }
      return current.sendSigned(type, payload, signOptions);
    },
    reconnect: () => live()?.forceReconnect(),
    pause,
    resume,
    signOut: () => end(true),
    destroy: () => end(false),
  };
}
