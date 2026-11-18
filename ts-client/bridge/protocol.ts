// =============================================================================
// WebSocket Engine - The window bridge's protocol
// =============================================================================
//
// One connection for every window of an application: its owner (Electron's
// main process, Tauri's Rust side, a worker) holds the connection, the token,
// the keys and the outbox; each window holds a client with the WSEClient
// interface over a channel to the owner. The window's frames ask (share the
// connection, hold topics, send); the owner's frames answer and hand the
// stream on, in batches the window acknowledges: at most `inFlight` batches
// it has not acknowledged, so a slow window holds back only its own queue,
// and a queue past its limits is dropped and its topics said to have broken
// off. No frame carries a token, a key or a cookie.

import type { ConnectionState, SendStatus, SignedMessageOptions, StorageState, WSEGapEvent, WSMessage } from '../types.js';

/** The protocol's major version: an owner and a window of others do not speak. */
export const BRIDGE_PROTOCOL = 1;

/** How much an owner hands a window at once, and holds for it. */
export interface BridgeLimits {
  /** Items in one batch at most (256). */
  batchItems: number;
  /** Bytes of one batch's messages at most, by their JSON (1 MiB); a larger message goes alone. */
  batchBytes: number;
  /** Batches handed and not acknowledged at most (2). */
  inFlight: number;
  /** Items waiting for a window at most (10,000); past it, or `queueBytes`, its queue is dropped. */
  queueItems: number;
  /** Bytes waiting for a window at most (64 MiB). */
  queueBytes: number;
}

export const DEFAULT_BRIDGE_LIMITS: Readonly<BridgeLimits> = Object.freeze({
  batchItems: 256,
  batchBytes: 1 << 20,
  inFlight: 2,
  queueItems: 10_000,
  queueBytes: 64 << 20,
});

/** The owner's connection, as a window shows it. */
export interface ConnState {
  connection: ConnectionState;
  ready: boolean;
  paused: readonly string[];
  /** Where it connects, without its query or user. */
  endpoint: string | null;
  lastError: string | null;
  storage: StorageState;
}

/** A send's options as they cross: the application's own. */
export interface SendFrameOptions {
  id: string;
  priority?: number;
  ttl?: number;
  live?: boolean;
  encrypted?: boolean;
  correlation_id?: string;
  /**
   * Topics continuous in the window when it goes, each time it goes: the
   * owner holds it, after a reconnect, until the window says they are.
   */
  requires?: string[];
}

/** What a window asks of its owner. */
export type WindowFrame =
  | { k: 'hello'; v: number; agent: string }
  /** The window's share of the connection, as it goes from none to some and back. */
  | { k: 'share'; on: boolean }
  | { k: 'hold'; topics: string[] }
  | { k: 'drop'; topics: string[] }
  /** Message types it hears outside its topics; '*' for every one. */
  | { k: 'listen'; types: string[] }
  | { k: 'unlisten'; types: string[] }
  /** It took every batch up to and with `s`. */
  | { k: 'ack'; s: number }
  /**
   * A send, numbered by the window (`n`); `c` the connection it saw ready,
   * for a live one; `a` the account it was made for.
   */
  | { k: 'send'; n: number; t: string; p: unknown; o: SendFrameOptions; c: number; a: number }
  | { k: 'sign'; n: number; t: string; p: unknown; o: SignedMessageOptions; a: number }
  /**
   * The topics the window holds that hold none of its sends back now, on the
   * connection `gen` names (after each change, and at each connection).
   */
  | { k: 'continuous'; gen: number; topics: string[] }
  | { k: 'reconnect' }
  | { k: 'wake' }
  | { k: 'signOut' }
  | { k: 'bye' };

const isNumber = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v);
const isNames = (v: unknown): v is string[] => Array.isArray(v) && v.every((t) => typeof t === 'string');
const isObject = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v);

/**
 * `f` is a frame of a window as the protocol has them: what crosses a port or
 * IPC is checked here before the owner reads it.
 */
export function isWindowFrame(f: unknown): f is WindowFrame {
  if (!isObject(f) || typeof f.k !== 'string') return false;
  switch (f.k) {
    case 'hello':
      return isNumber(f.v) && typeof f.agent === 'string';
    case 'share':
      return typeof f.on === 'boolean';
    case 'hold':
    case 'drop':
      return isNames(f.topics);
    case 'listen':
    case 'unlisten':
      return isNames(f.types);
    case 'ack':
      return isNumber(f.s);
    case 'send':
      return (
        isNumber(f.n) &&
        typeof f.t === 'string' &&
        isNumber(f.c) &&
        isNumber(f.a) &&
        isObject(f.o) &&
        typeof f.o.id === 'string' &&
        (f.o.ttl === undefined || isNumber(f.o.ttl)) &&
        (f.o.priority === undefined || isNumber(f.o.priority)) &&
        (f.o.requires === undefined || isNames(f.o.requires))
      );
    case 'continuous':
      return isNumber(f.gen) && isNames(f.topics);
    case 'sign':
      return isNumber(f.n) && typeof f.t === 'string' && isNumber(f.a) && isObject(f.o);
    case 'reconnect':
    case 'wake':
    case 'signOut':
    case 'bye':
      return true;
    default:
      return false;
  }
}

/** What the owner tells a window. */
export type OwnerFrame =
  | {
      k: 'welcome';
      v: number;
      /** The window's attachment, unique for the owner's life. */
      att: number;
      limits: BridgeLimits;
      realm: string | null;
      account: { epoch: number; user: string | null };
      conn: ConnState;
      /** The connection the owner has now (counted from 1; 0: none yet). */
      gen: number;
    }
  | { k: 'refused'; reason: 'version' | 'closing'; detail?: string }
  /**
   * The owner's connection as it is now: at once, beside the batches, so a
   * window that holds its batches (paused, or behind) still shows it.
   */
  | { k: 'conn'; state: ConnState }
  /** A batch: messages and events, in the order the owner had them. */
  | { k: 'b'; s: number; items: BridgeItem[] }
  /** What became of a send (`n`): its id, and a signed one's nonce. */
  | { k: 'sent'; n: number; status: SendStatus; id?: string; nonce?: string }
  /** The window is let go: the user signed out, the owner ended, or it broke the protocol. */
  | { k: 'revoked'; reason: 'signed_out' | 'owner_closed' | 'protocol'; detail?: string };

/** An item of a batch. */
export type BridgeItem =
  /** A message, as the owner's client hands it to its application. */
  | { m: WSMessage }
  /** The server took connection `gen`, of `user`. */
  | { ev: 'connected'; gen: number; user: string | null }
  /** The connection went. */
  | { ev: 'lost' }
  | { ev: 'subscribed'; taken: string[]; refused: string[] }
  | { ev: 'gap'; detail: WSEGapEvent }
  /** The window's queue passed its limits and was dropped: its topics broke off. */
  | { ev: 'overflow'; topics: string[] }
  /** An event of the owner's client, for the window's listeners (an outcome of its own sends among them). */
  | { ev: 'event'; name: string; detail: unknown };

/** What became of a window's send: said once, and the window waits for it. */
export function isOutcome(item: BridgeItem): boolean {
  return 'ev' in item && item.ev === 'event' && (item.name === 'wseDelivered' || item.name === 'wseNotDelivered');
}
