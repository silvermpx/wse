// =============================================================================
// WebSocket Engine - A window's client over the bridge
// =============================================================================
//
// The WSEClient a window holds when another holds the connection (Electron's
// main process, Tauri's Rust side, a worker): the adapters for React, Vue,
// Svelte, Angular and Solid take it as they take a client of its own. What
// depends on the application stays in the window: its listeners, its shares
// of the topics, the delivery's barrier, resync, commit and checkpoint, the
// sends that wait for their topics. The owner holds the connection, the
// token, the keys and the outbox, and hands the window its stream in batches
// it acknowledges as it takes them.

import { Delivery, checkDeliveryOptions, type DeliveryOptions, type TopicState } from '../core/delivery.js';
import { memoryStorage, type ClientStorage } from '../core/context.js';
import { createStore } from '../core/store.js';
import { createWSEState, type WSEStore } from '../core/state/wseState.js';
import type { Lease, WSEClient, WSEClientSnapshot, WSEClientState } from '../core/client.js';
import {
  ConnectionState,
  type MessageOptions,
  type SendStatus,
  type SignedMessageOptions,
  type WSMessage,
} from '../types.js';
import { generateId } from '../utils/id.js';
import { logger } from '../utils/logger.js';
import type { WindowChannel } from './channel.js';
import { BRIDGE_PROTOCOL, type BridgeItem, type ConnState, type OwnerFrame, type WindowFrame } from './protocol.js';

export interface BridgeClientOptions {
  /** This part of the application, by a stable name: what it commits is its. Required by recover-or-resync. */
  consumer?: string;
  /** The server the checkpoint is bound to (default: none). */
  realm?: string;
  /** How the topics' messages reach the application, as for a client of its own. */
  delivery?: DeliveryOptions;
  /** Where the checkpoint is kept (default: memory). */
  storage?: ClientStorage;
  /** A name the owner logs the window by. */
  agent?: string;
}

/** Where the window stands with its owner. */
export type BridgePhase = 'attaching' | 'attached' | 'refused' | 'revoked' | 'closed';

export interface BridgeClient extends WSEClient {
  readonly bridge: { readonly phase: BridgePhase; readonly reason: string | null };
}

const NO_CONN: ConnState = Object.freeze({
  connection: ConnectionState.PENDING,
  ready: false,
  paused: [],
  endpoint: null,
  lastError: null,
  storage: 'memory',
}) as ConnState;

function shallowSame(a: WSEClientSnapshot, b: WSEClientSnapshot): boolean {
  return (
    a.connection === b.connection &&
    a.paused === b.paused &&
    a.ready === b.ready &&
    a.endpoint === b.endpoint &&
    a.lastError === b.lastError &&
    a.topics === b.topics &&
    a.topicStates === b.topicStates &&
    a.storage === b.storage
  );
}

/**
 * A client over `channel` (or the channel a promise gives, Electron's port
 * say) to the owner of the connection. Frames sent before the channel is
 * there wait for it.
 */
export function createBridgeClient(
  channel: WindowChannel | (() => Promise<WindowChannel>),
  options: BridgeClientOptions = {},
): BridgeClient {
  const consumer = options.consumer && options.consumer !== '' ? options.consumer : null;
  checkDeliveryOptions(options.delivery, consumer ?? undefined);
  const storage = options.storage ?? memoryStorage();
  const store = createStore<WSEStore>(createWSEState);
  const listeners = new Map<string, Set<(detail: any) => void>>();
  const messageListeners = new Map<string, Set<(message: WSMessage) => void>>();
  const topicShares = new Map<string, number>();
  /** The last offset received of each stream of each topic. */
  const received = new Map<string, Record<string, number>>();
  /**
   * The window's sends the owner did not say the outcome of yet: how many
   * of each id (an application may give two the same).
   */
  const pending = new Map<string, number>();
  let pendingCount = 0;
  const pend = (id: string) => {
    pending.set(id, (pending.get(id) ?? 0) + 1);
    pendingCount++;
  };
  /** The owner said the outcome of one send of `id`. */
  const settled = (id: string) => {
    const n = pending.get(id);
    if (n === undefined) return;
    if (n > 1) pending.set(id, n - 1);
    else pending.delete(id);
    pendingCount--;
  };
  /** What the owner held of the window's sends went with the user: said once. */
  const signedOut = () => {
    if (pendingCount === 0) return;
    const ids: string[] = [];
    for (const [id, n] of pending) for (let i = 0; i < n; i++) ids.push(id);
    pending.clear();
    pendingCount = 0;
    emit('wseNotDelivered', { reason: 'cleared', ids });
  };
  /**
   * Sends waiting for the topics they require to be continuous, each with
   * the time it may wait (ms since the epoch), counted from its send.
   */
  let waiting: Array<{ type: string; payload: unknown; options: MessageOptions; until?: number }> = [];
  const signs = new Map<number, (status: SendStatus) => void>();
  let port: WindowChannel | null = null;
  let early: WindowFrame[] = [];
  let phase: BridgePhase = 'attaching';
  let reason: string | null = null;
  let conn: ConnState = NO_CONN;
  let user: string | null = null;
  let gen = 0;
  let took = 0;
  let shares = 0;
  /** What the owner was told: the window holds a share and is not paused. */
  let sharing = false;
  let shareTimer: ReturnType<typeof setTimeout> | null = null;
  let nextSend = 1;
  const paused: string[] = [];
  /** Batches held while the window is paused: not acknowledged, so the owner holds the rest. */
  let pausedBatches: Array<{ s: number; items: BridgeItem[] }> = [];
  let done = false;

  const emit = (type: string, detail?: unknown) => {
    const set = listeners.get(type);
    if (!set) return;
    for (const listener of [...set]) {
      try {
        listener(detail);
      } catch (error) {
        logger.error(`A listener of ${type} failed:`, error);
      }
    }
  };

  const post = (frame: WindowFrame) => {
    if (done && frame.k !== 'bye' && frame.k !== 'signOut') return;
    if (port) port.post(frame);
    else early.push(frame);
  };

  const dispatch = (message: WSMessage) => {
    const set = messageListeners.get(message.t);
    if (!set || set.size === 0) {
      emit('wseMessage', message);
      return;
    }
    for (const listener of [...set]) {
      try {
        listener(message);
      } catch (error) {
        logger.error(`A listener of ${message.t} messages failed:`, error);
      }
    }
  };

  const delivery = new Delivery(options.delivery, { consumer, realm: options.realm ?? null }, {
    route: dispatch,
    received: (topic) => ({ ...(received.get(topic) ?? {}) }),
    user: () => user,
    emit,
    changed: () => {
      refresh();
      release();
      reportContinuous();
    },
    storage,
  });

  // -- the snapshot ------------------------------------------------------------

  let topicList: readonly string[] = [];
  const snapshotOf = (): WSEClientSnapshot => ({
    connection: phase === 'attached' ? conn.connection : phase === 'attaching' ? ConnectionState.PENDING : ConnectionState.DISCONNECTED,
    paused: paused.length > 0 ? [...paused, ...conn.paused] : conn.paused,
    ready: phase === 'attached' && conn.ready && paused.length === 0,
    endpoint: conn.endpoint,
    topics: topicList,
    lastError: reason ?? conn.lastError,
    topicStates: delivery.topicStates() as Readonly<Record<string, TopicState>>,
    storage: conn.storage,
  });
  let snapshot = snapshotOf();
  const stateListeners = new Set<(snapshot: WSEClientSnapshot) => void>();
  const refresh = () => {
    const next = snapshotOf();
    if (shallowSame(next, snapshot)) return;
    const topicsChanged = next.topics !== snapshot.topics;
    snapshot = next;
    store.getState().setConnectionState(snapshot.connection);
    if (topicsChanged) store.getState().setActiveTopics([...snapshot.topics]);
    for (const listener of [...stateListeners]) {
      try {
        listener(snapshot);
      } catch (error) {
        logger.error('A listener of the client state failed:', error);
      }
    }
  };
  const state: WSEClientState = {
    getSnapshot: () => snapshot,
    listen: (listener) => {
      stateListeners.add(listener);
      return () => stateListeners.delete(listener);
    },
    subscribe: (listener) => {
      stateListeners.add(listener);
      listener(snapshot);
      return () => stateListeners.delete(listener);
    },
  };

  // -- the owner's frames -------------------------------------------------------

  /**
   * The topics it holds that hold none of its sends back, told the owner as
   * they change and at each connection: a send of the window that requires
   * one goes from the owner's outbox, after a reconnect too, only once the
   * window said so of that connection.
   */
  let toldContinuous: string | null = null;
  const reportContinuous = () => {
    if (phase !== 'attached') return;
    const topics = [...topicShares.keys()].filter((topic) => !delivery.holdsBack([topic])).sort();
    const key = `${gen}\u0000${topics.join('\u0000')}`;
    if (key === toldContinuous) return;
    toldContinuous = key;
    post({ k: 'continuous', gen, topics });
  };

  /** The window was let go (refused, signed out, its owner gone): nothing more is handed on. */
  const over = () => phase === 'refused' || phase === 'revoked' || phase === 'closed';

  const end = (next: BridgePhase, why: string | null) => {
    if (over()) return;
    phase = next;
    reason = why;
    // What it held paused is the last connection's: never handed on.
    pausedBatches = [];
    for (const resolve of signs.values()) resolve('dropped');
    signs.clear();
    // What waited for its topics never reached the owner: it goes.
    if (waiting.length > 0) {
      const ids = waiting.map((w) => w.options.id!);
      waiting = [];
      emit('wseNotDelivered', { reason: 'closed', ids });
    }
    // The connection is gone for good: what the window holds is not current.
    if (!done) delivery.connectionLost();
    refresh();
  };

  const take = (item: BridgeItem) => {
    if ('m' in item) {
      const message = item.m;
      if (message.tp && typeof message.o === 'number' && (!Number.isSafeInteger(message.o) || message.o < 0)) {
        // JavaScript cannot hold it exactly: ordering and dedup by it would be
        // wrong, so the message is refused and the topic breaks off, as a
        // client of its own does.
        const detail = {
          topic: message.tp,
          epoch: String(message.e),
          fromOffset: null,
          toOffset: null,
          missed: null,
          reason: 'unsafe_offset' as const,
        };
        delivery.brokeOff(message.tp, 'unsafe_offset');
        emit('wseGap', detail);
        return;
      }
      if (message.tp && message.e !== undefined && typeof message.o === 'number') {
        const streams = received.get(message.tp) ?? {};
        streams[String(message.e)] = message.o;
        received.set(message.tp, streams);
      }
      if (message.tp && delivery.hold(message)) return;
      dispatch(message);
      return;
    }
    switch (item.ev) {
      case 'connected':
        gen = item.gen;
        user = item.user;
        emit('wseConnected', { user: item.user, reconnected: item.gen > 1 });
        refresh();
        reportContinuous();
        return;
      case 'lost':
        delivery.connectionLost();
        refresh();
        return;
      case 'subscribed':
        // The checkpoint the window was given is taken here, as a client of
        // its own takes it when it subscribes: for the topics it holds, once
        // the connection named its user, checked against that user, the
        // realm, the consumer and the schema.
        delivery.recoveryPositions(item.taken);
        delivery.subscribed(item.taken, item.refused);
        return;
      case 'gap':
        delivery.brokeOff(item.detail.topic, item.detail.reason);
        emit('wseGap', item.detail);
        return;
      case 'overflow':
        for (const topic of item.topics) delivery.brokeOff(topic, 'overflow');
        emit('wseOverloaded', { topics: item.topics });
        return;
      case 'event': {
        const detail = item.detail as { ids?: string[] } | null;
        if ((item.name === 'wseDelivered' || item.name === 'wseNotDelivered') && detail?.ids) {
          for (const id of detail.ids) settled(id);
        }
        emit(item.name, item.detail);
        return;
      }
    }
  };

  const batch = (s: number, items: BridgeItem[]) => {
    if (over()) return;
    for (const item of items) {
      // A listener let the window go: nothing more of the batch is handed on.
      if (done || over()) return;
      take(item);
    }
    took = s;
    post({ k: 'ack', s });
  };

  const frame = (f: OwnerFrame) => {
    switch (f.k) {
      case 'welcome':
        phase = 'attached';
        conn = f.conn;
        user = f.account.user;
        gen = f.gen;
        refresh();
        reportContinuous();
        return;
      case 'refused':
        end('refused', `the owner refused the window: ${f.reason}${f.detail ? ` (${f.detail})` : ''}`);
        return;
      case 'conn':
        conn = f.state;
        refresh();
        return;
      case 'b':
        if (over()) return;
        if (f.s !== took + pausedBatches.length + 1) {
          logger.error(`[bridge] batch ${f.s} after ${took + pausedBatches.length}: the window stops`);
          end('revoked', 'protocol');
          port?.close();
          return;
        }
        if (paused.length > 0) pausedBatches.push({ s: f.s, items: f.items });
        else batch(f.s, f.items);
        return;
      case 'sent': {
        const sign = signs.get(f.n);
        if (sign) {
          signs.delete(f.n);
          sign(f.status);
          return;
        }
        if (f.status === 'dropped' && f.id) {
          settled(f.id);
          emit('wseNotDelivered', { reason: 'dropped', ids: [f.id] });
        }
        return;
      }
      case 'revoked':
        if (f.reason === 'signed_out') signedOut();
        end('revoked', f.reason);
        if (f.reason === 'signed_out') {
          delivery.forget();
          storage.clear();
        }
        return;
    }
  };

  const open = (opened: WindowChannel) => {
    if (done) {
      // Let go before its channel opened: a sign-out still reaches the owner,
      // which signs every window out; the rest of what waited is moot.
      if (early.some((f) => f.k === 'signOut')) {
        opened.post({ k: 'hello', v: BRIDGE_PROTOCOL, agent: options.agent ?? 'window' });
        opened.post({ k: 'signOut' });
      }
      early = [];
      opened.close();
      return;
    }
    port = opened;
    opened.listen({
      frame,
      close: () => end('closed', 'the owner went'),
    });
    opened.post({ k: 'hello', v: BRIDGE_PROTOCOL, agent: options.agent ?? 'window' });
    const queued = early;
    early = [];
    for (const f of queued) opened.post(f);
  };
  if (typeof channel === 'function') {
    channel().then(open, (error) => end('closed', `no channel to the owner: ${error instanceof Error ? error.message : String(error)}`));
  } else {
    open(channel);
  }

  // -- the window's doings ------------------------------------------------------

  /**
   * The window's share of the connection, as the owner counts it: held while
   * a lease is and the window is not paused. A paused window gives its share
   * back, so the connection ends when no one else holds one, as a client of
   * its own does; what comes meanwhile waits for it at the owner.
   */
  const share = () => {
    const want = shares > 0 && paused.length === 0 && !done;
    if (want === sharing) return;
    sharing = want;
    post({ k: 'share', on: want });
  };

  /** Sends whose topics are continuous now go. */
  const release = () => {
    if (waiting.length === 0) return;
    const now = Date.now();
    const still: typeof waiting = [];
    const late: string[] = [];
    for (const w of waiting) {
      if (w.until !== undefined && w.until <= now) late.push(w.options.id!);
      else if (delivery.continuous(w.options.requires ?? [])) go(w.type, w.payload, w.options, w.until);
      else still.push(w);
    }
    waiting = still;
    // Its time ran out while it waited: it does not go, and the window hears so.
    if (late.length > 0) emit('wseNotDelivered', { reason: 'expired', ids: late });
  };

  const go = (type: string, payload: unknown, sendOptions: MessageOptions, until?: number): string => {
    const id = sendOptions.id ?? generateId();
    if (!sendOptions.live) pend(id);
    post({
      k: 'send',
      n: nextSend++,
      t: type,
      p: payload,
      o: {
        id,
        priority: sendOptions.priority,
        // What is left of its time: it counts from its send.
        ttl: until !== undefined ? Math.max(0, until - Date.now()) : sendOptions.ttl,
        live: sendOptions.live,
        // The owner holds it, after a reconnect, until the window says they are continuous.
        requires: !sendOptions.live && sendOptions.requires?.length ? [...sendOptions.requires] : undefined,
        encrypted: sendOptions.encrypted,
        correlation_id: sendOptions.correlation_id,
      },
      c: gen,
      a: 0,
    });
    return id;
  };

  const retain = (): Lease => {
    if (done) throw new Error('This WSE client was signed out or destroyed: make a new one');
    shares++;
    if (shareTimer) {
      clearTimeout(shareTimer);
      shareTimer = null;
    }
    share();
    let held = true;
    return {
      release: () => {
        if (!held) return;
        held = false;
        shares--;
        if (shares > 0 || done) return;
        // A release and a retain in one turn (React's Strict Mode) leave the share as it was.
        shareTimer = setTimeout(() => {
          shareTimer = null;
          share();
        }, 0);
      },
    };
  };

  const subscribe = (topics: string[], _options?: { recover?: boolean }): Lease => {
    const unique = [...new Set(topics.filter((t) => typeof t === 'string' && t !== ''))];
    const added = unique.filter((topic) => {
      const n = topicShares.get(topic) ?? 0;
      topicShares.set(topic, n + 1);
      return n === 0;
    });
    if (added.length > 0 && !done) {
      delivery.track(added);
      post({ k: 'hold', topics: added });
      topicList = [...topicShares.keys()];
      refresh();
    }
    let held = true;
    return {
      release: () => {
        if (!held) return;
        held = false;
        const removed = unique.filter((topic) => {
          const n = (topicShares.get(topic) ?? 1) - 1;
          if (n > 0) topicShares.set(topic, n);
          else topicShares.delete(topic);
          return n === 0;
        });
        if (removed.length === 0 || done) return;
        delivery.untrack(removed);
        post({ k: 'drop', topics: removed });
        for (const topic of removed) received.delete(topic);
        topicList = [...topicShares.keys()];
        refresh();
      },
    };
  };

  const finish = () => {
    if (done) return;
    done = true;
    if (shareTimer) clearTimeout(shareTimer);
    shareTimer = null;
    delivery.close();
    end('closed', null);
  };

  const client: BridgeClient = {
    get bridge() {
      return { phase, reason };
    },
    retain,
    state,
    store,
    onMessage: (type, listener) => {
      let set = messageListeners.get(type);
      if (!set) {
        set = new Set();
        messageListeners.set(type, set);
        post({ k: 'listen', types: [type] });
      }
      set.add(listener);
      return () => {
        const current = messageListeners.get(type);
        if (!current?.delete(listener) || current.size > 0) return;
        messageListeners.delete(type);
        post({ k: 'unlisten', types: [type] });
      };
    },
    on: (event, listener) => {
      let set = listeners.get(event);
      if (!set) {
        set = new Set();
        listeners.set(event, set);
        // The catch-all hears every message no listener of its type takes.
        if (event === 'wseMessage') post({ k: 'listen', types: ['*'] });
      }
      set.add(listener);
      return () => {
        const current = listeners.get(event);
        if (!current?.delete(listener) || current.size > 0) return;
        listeners.delete(event);
        if (event === 'wseMessage') post({ k: 'unlisten', types: ['*'] });
      };
    },
    subscribe,
    send: (type, payload, sendOptions = {}) => {
      // Let go for good (refused, signed out, its owner gone): nothing more goes.
      if (done || over()) return 'dropped';
      if (sendOptions.live) {
        if (!snapshot.ready) return 'dropped';
        if (sendOptions.requires?.length && !delivery.continuous(sendOptions.requires)) return 'dropped';
      } else if (sendOptions.requires?.length && !delivery.continuous(sendOptions.requires)) {
        // It goes once its topics are continuous in this window.
        const ttl = sendOptions.ttl;
        waiting.push({
          type,
          payload,
          options: { ...sendOptions, id: sendOptions.id ?? generateId() },
          until: ttl !== undefined && Number.isFinite(ttl) && ttl >= 0 ? Date.now() + ttl : undefined,
        });
        return 'queued';
      }
      go(type, payload, sendOptions);
      // Handed to the owner: a live one goes now or not at all; the rest
      // waits in the owner's outbox, which says what became of it.
      return sendOptions.live ? 'sent' : 'queued';
    },
    sendSigned: (type, payload, signOptions: SignedMessageOptions = {}) => {
      if (done || phase !== 'attached') return Promise.resolve('dropped');
      const n = nextSend++;
      return new Promise<SendStatus>((resolve) => {
        signs.set(n, resolve);
        post({ k: 'sign', n, t: type, p: payload, o: signOptions, a: 0 });
      });
    },
    kept: () => pendingCount + waiting.length,
    reconnect: () => post({ k: 'reconnect' }),
    wake: () => post({ k: 'wake' }),
    close: async (deadlineMs: number) => {
      const until = Date.now() + Math.max(0, deadlineMs);
      while (!done && pendingCount + waiting.length > 0 && Date.now() < until) {
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      client.destroy();
    },
    pause: (why) => {
      if (done || paused.includes(why)) return;
      paused.push(why);
      share();
      refresh();
    },
    resume: (why) => {
      const at = paused.indexOf(why);
      if (at < 0) return;
      paused.splice(at, 1);
      if (paused.length > 0) {
        refresh();
        return;
      }
      // What came meanwhile, in order (the connection's end among it, when
      // the window's share was the last); the owner held the rest.
      const held = pausedBatches;
      pausedBatches = [];
      for (const b of held) batch(b.s, b.items);
      share();
      refresh();
    },
    commit: (message) => delivery.commit(message),
    checkpoint: () => delivery.checkpoint(),
    resync: (topics) => {
      if (!done) delivery.resync([...new Set(topics)]);
    },
    signOut: () => {
      if (done) return;
      post({ k: 'signOut' });
      signedOut();
      delivery.forget();
      storage.clear();
      finish();
      port?.close();
    },
    destroy: () => {
      if (done) return;
      post({ k: 'bye' });
      finish();
      port?.close();
    },
  };
  return client;
}
