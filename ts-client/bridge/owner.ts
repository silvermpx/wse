// =============================================================================
// WebSocket Engine - The window bridge's owner
// =============================================================================
//
// The owner holds one client, and so one connection, for every window it
// serves: their shares of it, the union of their topics, their sends into
// one outbox. It hands each window what that window holds and listens to, in
// batches the window acknowledges, and routes back what became of the
// window's own sends. It runs anywhere the core client runs: Electron's main
// process, a utility process, a worker, Node.

import { createWSEClient, type Lease, type WSEClient, type WSEClientOptions } from '../core/client.js';
import { logger } from '../utils/logger.js';
import type { WSEClientSnapshot } from '../core/client.js';
import type { WSMessage } from '../types.js';
import type { OwnerChannel } from './channel.js';
import {
  BRIDGE_PROTOCOL,
  DEFAULT_BRIDGE_LIMITS,
  isWindowFrame,
  type BridgeItem,
  type BridgeLimits,
  type ConnState,
  type WindowFrame,
} from './protocol.js';

export interface BridgeOwnerOptions extends WSEClientOptions {
  /** What the owner hands a window at once, and holds for it. */
  limits?: Partial<BridgeLimits>;
  /** Whether a window may sign a message of `type` with the owner's key (default: yes). */
  allowSign?: (type: string) => boolean;
}

export interface BridgeOwner {
  /** The owner's own client: its application holds a share and sends on it as on any. */
  readonly client: WSEClient;
  /** Serves a window over `channel`, until it says bye, its channel ends, or the owner does. */
  attach(channel: OwnerChannel): () => void;
  /** The windows served now. */
  readonly attached: number;
  /** The user signed out: every window is let go, and the client with what it held for the user. */
  signOut(): void;
  /** The owner ends: every window is let go; what the client keeps in storage stays. */
  destroy(): void;
}

/** Events of the owner's client every window hears as its own. */
const FORWARDED = [
  'wseServerError',
  'wseAuthFailed',
  'wseEncryptionFailed',
  'wseReconnectFailed',
  'wseOverloaded',
  'wseTokenRefreshFailed',
  'wseStorage',
  'wseStorageError',
  'wseCheckpointRejected',
];

interface Queued {
  item: BridgeItem;
  bytes: number;
}

/** A window the owner serves. */
interface Attachment {
  att: number;
  channel: OwnerChannel;
  welcomed: boolean;
  closed: boolean;
  share: Lease | null;
  holds: Set<string>;
  listens: Set<string>;
  queue: Queued[];
  queueBytes: number;
  /** The last batch handed, and the last the window took. */
  handed: number;
  took: number;
  pumping: boolean;
  lastReconnect: number;
}

/** The endpoint without its query or user: no secret leaves the owner. */
function bare(endpoint: string | null): string | null {
  if (!endpoint) return null;
  try {
    const url = new URL(endpoint);
    url.search = '';
    url.username = '';
    url.password = '';
    return url.toString();
  } catch {
    return null;
  }
}

function connOf(snapshot: WSEClientSnapshot): ConnState {
  return {
    connection: snapshot.connection,
    ready: snapshot.ready,
    paused: snapshot.paused,
    endpoint: bare(snapshot.endpoint),
    lastError: snapshot.lastError,
    storage: snapshot.storage,
  };
}

function sizeOf(message: WSMessage): number {
  try {
    return JSON.stringify(message).length;
  } catch {
    return 0;
  }
}

export function createBridgeOwner(options: BridgeOwnerOptions = {}): BridgeOwner {
  const { limits: partial, allowSign, ...clientOptions } = options;
  const limits: BridgeLimits = { ...DEFAULT_BRIDGE_LIMITS, ...partial };
  const attachments = new Map<number, Attachment>();
  /** Each topic the windows hold: how many, and the client's lease of it. */
  const topics = new Map<string, { count: number; lease: Lease }>();
  /** Whose send each outbox message is, by its id: its outcome goes there. */
  const sender = new Map<string, number>();
  let nextAtt = 1;
  let gen = 0;
  let user: string | null = null;
  let ready = false;
  let ended = false;

  const route = (message: WSMessage) => {
    let bytes = -1;
    for (const a of attachments.values()) {
      if (!a.welcomed || a.closed) continue;
      const wanted = message.tp ? a.holds.has(message.tp) : a.listens.has(message.t) || a.listens.has('*');
      if (!wanted) continue;
      if (bytes < 0) bytes = sizeOf(message);
      enqueue(a, { m: message }, bytes);
    }
  };

  const client = createWSEClient({ ...clientOptions, onEveryMessage: route });
  // What windows bind their checkpoints to: the endpoint without what it
  // carries of credentials (a user, a password, a token in its query).
  const realm = clientOptions.realm ?? bare(clientOptions.endpoints?.[0] ?? null);
  let conn = connOf(client.state.getSnapshot());

  const toAll = (item: BridgeItem) => {
    for (const a of attachments.values()) if (a.welcomed && !a.closed) enqueue(a, item, 0);
  };

  const enqueue = (a: Attachment, item: BridgeItem, bytes: number) => {
    a.queue.push({ item, bytes });
    a.queueBytes += bytes;
    if (a.queue.length > limits.queueItems || a.queueBytes > limits.queueBytes) {
      // The window fell behind past its limits: its queue goes, and what it
      // holds broke off; the connection and the other windows go on.
      logger.warn(`[bridge] window ${a.att} fell behind: its queue of ${a.queue.length} dropped`);
      a.queue = [{ item: { ev: 'overflow', topics: [...a.holds] }, bytes: 0 }];
      a.queueBytes = 0;
      const active = activeOf(a);
      if (active.length > 0) a.queue.push({ item: { ev: 'subscribed', taken: active, refused: [] }, bytes: 0 });
    }
    if (!a.pumping) {
      a.pumping = true;
      queueMicrotask(() => {
        a.pumping = false;
        pump(a);
      });
    }
  };

  /** Batches for the window, as many as it may have in hand. */
  const pump = (a: Attachment) => {
    while (!a.closed && a.queue.length > 0 && a.handed - a.took < limits.inFlight) {
      let count = 0;
      let bytes = 0;
      while (count < a.queue.length && count < limits.batchItems) {
        const next = a.queue[count].bytes;
        // A message larger than a batch goes alone.
        if (count > 0 && bytes + next > limits.batchBytes) break;
        bytes += next;
        count++;
      }
      const taken = a.queue.splice(0, count);
      a.queueBytes -= bytes;
      a.handed++;
      a.channel.post({ k: 'b', s: a.handed, items: taken.map((q) => q.item) });
    }
  };

  /** The topics `a` holds that the server took. */
  const activeOf = (a: Attachment): string[] => {
    const states = client.state.getSnapshot().topicStates;
    return [...a.holds].filter((topic) => states[topic]?.subscription === 'active');
  };

  const hold = (a: Attachment, topic: string) => {
    if (a.holds.has(topic)) return;
    a.holds.add(topic);
    const held = topics.get(topic);
    if (held) {
      held.count++;
      // The server took it for another window already: this one hears it now.
      if (client.state.getSnapshot().topicStates[topic]?.subscription === 'active') {
        enqueue(a, { ev: 'subscribed', taken: [topic], refused: [] }, 0);
      }
    } else {
      topics.set(topic, { count: 1, lease: client.subscribe([topic]) });
    }
  };

  const drop = (a: Attachment, topic: string) => {
    if (!a.holds.delete(topic)) return;
    const held = topics.get(topic);
    if (!held) return;
    held.count--;
    if (held.count === 0) {
      topics.delete(topic);
      held.lease.release();
    }
  };

  const detach = (a: Attachment) => {
    if (a.closed) return;
    a.closed = true;
    attachments.delete(a.att);
    a.share?.release();
    a.share = null;
    for (const topic of [...a.holds]) drop(a, topic);
    for (const [id, att] of sender) if (att === a.att) sender.delete(id);
    a.queue = [];
    a.channel.close();
  };

  const revoke = (a: Attachment, reason: 'signed_out' | 'owner_closed' | 'protocol', detail?: string) => {
    if (a.closed) return;
    a.channel.post({ k: 'revoked', reason, detail });
    detach(a);
  };

  const frame = (a: Attachment, f: WindowFrame) => {
    if (a.closed) return;
    if (!a.welcomed && f.k !== 'hello') {
      revoke(a, 'protocol', `${f.k} before hello`);
      return;
    }
    switch (f.k) {
      case 'hello':
        if (f.v !== BRIDGE_PROTOCOL) {
          a.channel.post({ k: 'refused', reason: 'version', detail: `the owner speaks ${BRIDGE_PROTOCOL}, not ${f.v}` });
          detach(a);
          return;
        }
        a.welcomed = true;
        a.channel.post({
          k: 'welcome',
          v: BRIDGE_PROTOCOL,
          att: a.att,
          limits,
          realm,
          account: { epoch: 0, user },
          conn,
          gen: ready ? gen : 0,
        });
        return;
      case 'share':
        if (f.on) a.share ??= client.retain();
        else {
          a.share?.release();
          a.share = null;
        }
        return;
      case 'hold':
        for (const topic of f.topics) hold(a, topic);
        return;
      case 'drop':
        for (const topic of f.topics) drop(a, topic);
        return;
      case 'listen':
        for (const type of f.types) a.listens.add(type);
        return;
      case 'unlisten':
        for (const type of f.types) a.listens.delete(type);
        return;
      case 'ack':
        if (f.s > a.handed || f.s < a.took) {
          revoke(a, 'protocol', `ack ${f.s} of batches ${a.took}..${a.handed}`);
          return;
        }
        a.took = f.s;
        pump(a);
        return;
      case 'send': {
        // A live one goes on the connection the window saw ready, or not at all.
        if (f.o.live && (!ready || f.c !== gen)) {
          a.channel.post({ k: 'sent', n: f.n, status: 'dropped', id: f.o.id });
          return;
        }
        const status = client.send(f.t, f.p, { ...f.o });
        if (status !== 'dropped' && !f.o.live) sender.set(f.o.id, a.att);
        a.channel.post({ k: 'sent', n: f.n, status, id: f.o.id });
        return;
      }
      case 'sign': {
        if (allowSign && !allowSign(f.t)) {
          a.channel.post({ k: 'sent', n: f.n, status: 'dropped', nonce: f.o.nonce });
          return;
        }
        void client.sendSigned(f.t, f.p, f.o).then(
          (status) => a.channel.post({ k: 'sent', n: f.n, status, nonce: f.o.nonce }),
          () => a.channel.post({ k: 'sent', n: f.n, status: 'dropped', nonce: f.o.nonce }),
        );
        return;
      }
      case 'reconnect': {
        // A window asks at most once in five seconds.
        const now = Date.now();
        if (now - a.lastReconnect < 5_000) return;
        a.lastReconnect = now;
        client.reconnect();
        return;
      }
      case 'wake':
        client.wake();
        return;
      case 'signOut':
        owner.signOut();
        return;
      case 'bye':
        detach(a);
        return;
    }
  };

  client.state.listen((snapshot) => {
    const next = connOf(snapshot);
    const changed =
      next.connection !== conn.connection ||
      next.ready !== conn.ready ||
      next.paused !== conn.paused ||
      next.endpoint !== conn.endpoint ||
      next.lastError !== conn.lastError ||
      next.storage !== conn.storage;
    conn = next;
    if (ready && !snapshot.ready) {
      ready = false;
      toAll({ ev: 'lost' });
    }
    if (!changed) return;
    for (const a of attachments.values()) if (a.welcomed && !a.closed) a.channel.post({ k: 'conn', state: conn });
  });
  client.on('wseConnected', (detail: { user: string | null }) => {
    gen++;
    ready = true;
    user = detail.user;
    toAll({ ev: 'connected', gen, user });
  });
  client.on('subscriptionUpdate', (reply: Record<string, unknown>) => {
    if (reply.action === 'unsubscribe') return;
    const strings = (value: unknown) => (Array.isArray(value) ? value.filter((v) => typeof v === 'string') : []);
    const taken = strings(reply.success_topics);
    const refused = strings(reply.failed_topics);
    for (const a of attachments.values()) {
      const mine = { taken: taken.filter((t) => a.holds.has(t)), refused: refused.filter((t) => a.holds.has(t)) };
      if (mine.taken.length + mine.refused.length > 0) enqueue(a, { ev: 'subscribed', ...mine }, 0);
    }
  });
  client.on('wseGap', (detail: { topic: string }) => {
    for (const a of attachments.values()) {
      if (a.holds.has(detail.topic)) enqueue(a, { ev: 'gap', detail: detail as never }, 0);
    }
  });
  // What became of a window's sends goes to that window alone.
  const outcome = (name: string) => (detail: { ids: string[]; reason?: string }) => {
    const byWindow = new Map<number, string[]>();
    for (const id of detail.ids) {
      const att = sender.get(id);
      if (att === undefined) continue;
      sender.delete(id);
      const ids = byWindow.get(att);
      if (ids) ids.push(id);
      else byWindow.set(att, [id]);
    }
    for (const [att, ids] of byWindow) {
      const a = attachments.get(att);
      if (a) enqueue(a, { ev: 'event', name, detail: { ...detail, ids } }, 0);
    }
  };
  client.on('wseDelivered', outcome('wseDelivered'));
  client.on('wseNotDelivered', outcome('wseNotDelivered'));
  for (const name of FORWARDED) client.on(name, (detail) => toAll({ ev: 'event', name, detail }));

  const owner: BridgeOwner = {
    client,
    get attached() {
      return attachments.size;
    },
    attach(channel) {
      if (ended) {
        channel.post({ k: 'refused', reason: 'closing' });
        channel.close();
        return () => {};
      }
      const a: Attachment = {
        att: nextAtt++,
        channel,
        welcomed: false,
        closed: false,
        share: null,
        holds: new Set(),
        listens: new Set(),
        queue: [],
        queueBytes: 0,
        handed: 0,
        took: 0,
        pumping: false,
        lastReconnect: 0,
      };
      attachments.set(a.att, a);
      channel.listen({
        // What is not a frame, or a frame that fails, lets go of its window
        // alone: the others, and the process, go on.
        frame: (f) => {
          if (!isWindowFrame(f)) {
            revoke(a, 'protocol', 'not a frame');
            return;
          }
          try {
            frame(a, f);
          } catch (error) {
            logger.error('[bridge] a window frame failed:', error);
            revoke(a, 'protocol', 'a frame that failed');
          }
        },
        close: () => detach(a),
      });
      return () => detach(a);
    },
    signOut() {
      if (ended) return;
      ended = true;
      for (const a of [...attachments.values()]) revoke(a, 'signed_out');
      client.signOut();
    },
    destroy() {
      if (ended) return;
      ended = true;
      for (const a of [...attachments.values()]) revoke(a, 'owner_closed');
      client.destroy();
    },
  };
  return owner;
}
