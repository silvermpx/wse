// =============================================================================
// WebSocket Engine - Delivery (core)
// =============================================================================
//
// What a client says of each topic it holds: whether the server took it
// (requested, active, denied) and whether its messages run without a break
// (continuous, resync_required, resyncing).
//
// In the recover-or-resync mode a topic that is not continuous -- from a cold
// start, after every reconnect, after a break -- holds its messages. Once the
// server took the topic, the application's resync handler brings its state up
// to date; then the messages held meanwhile go on, without those the loaded
// state has already. A topic that lost messages to the holding limit, or
// broke off again while it resynced, resyncs once more instead of passing for
// complete.
//
// The checkpoint is what the application committed it applied: per stream it
// moves only through what was applied without a hole (11 done before 10 does
// not count). It is bound to the server's realm, the user, the consumer and
// the application's schema, and a checkpoint of anything else is refused
// aloud. In the recover-or-resync mode a recovery starts from it, so a message
// that came but was not applied comes again.

import type { WSMessage } from '../types.js';
import type { TopicPosition } from '../services/EventSequencer.js';
import { CHECKPOINT_KEY, type ClientStorage } from './context.js';
import { logger } from '../utils/logger.js';

export type DeliveryMode = 'best-effort' | 'recover-or-resync';

/** Why a topic is not continuous. */
export type ContinuityReason =
  /** recover-or-resync: the client started; nothing it holds is known to be current. */
  | 'cold_start'
  /** recover-or-resync: a new connection, which proves nothing about what was missed. */
  | 'reconnected'
  /** Messages are missing in a stream. */
  | 'offset_gap'
  /** The server could not replay what was missed. */
  | 'not_recovered'
  /** The stream started over (its source changed). */
  | 'epoch_changed'
  /** An offset JavaScript cannot hold exactly. */
  | 'unsafe_offset'
  /** More messages came during a resync than the client holds. */
  | 'overflow'
  /** The application's resync handler failed. */
  | 'resync_failed'
  /** The application asked for a resync. */
  | 'requested';

export interface TopicState {
  readonly subscription: 'requested' | 'active' | 'denied';
  readonly continuity: 'continuous' | 'resync_required' | 'resyncing';
  /** Why it is not continuous; null while it is. */
  readonly reason: ContinuityReason | null;
}

export interface ResyncRequest {
  readonly topics: readonly string[];
  readonly reasons: Readonly<Record<string, ContinuityReason>>;
  /** What the consumer committed of each topic: stream (epoch) to the last offset applied. */
  readonly committed: Readonly<Record<string, Readonly<Record<string, number>>>>;
  /** Aborted once the resync is not wanted any more: the connection went, or the topic. */
  readonly signal: AbortSignal;
}

export interface ResyncResult {
  /** Of the messages held meanwhile, true for those the loaded state has already: they are dropped. */
  includes?: (message: WSMessage) => boolean;
}

export interface DeliveryOptions {
  /** 'best-effort' (default) or 'recover-or-resync'. */
  mode?: DeliveryMode;
  /**
   * Brings the application's state of `topics` up to date (from its backend)
   * and resolves once it applied it. Required by recover-or-resync; in
   * best-effort, called after a break while messages keep coming.
   */
  resync?: (request: ResyncRequest) => Promise<ResyncResult | void> | ResyncResult | void;
  /** The application's data schema: a checkpoint of another schema is refused. */
  schema?: string;
  /** The checkpoint the application saved last (client.checkpoint()); default: the client's storage. */
  checkpoint?: string | null;
  /** Messages a topic holds while it resyncs (default 10,000); more starts the resync over. */
  maxHeld?: number;
}

export interface DeliveryHooks {
  /** Hands a message on to the handlers, past the barrier. */
  route(message: WSMessage): void;
  /** Where the client is in each stream of `topic`: what it received. */
  received(topic: string): Record<string, number>;
  /** The connection's user (server_ready); null before it or without one. */
  user(): string | null;
  emit(type: string, detail: unknown): void;
  /** The topics' states changed. */
  changed(): void;
  storage: ClientStorage;
}

/** An epoch key as the server writes epochs, 8 lowercase hex digits; null when it is none. */
function epochKey(key: string): string | null {
  return /^[0-9a-fA-F]{1,8}$/.test(key) ? key.toLowerCase().padStart(8, '0') : null;
}

/** Why a saved checkpoint was not taken. */
export type CheckpointMismatch = 'malformed' | 'version' | 'realm' | 'user' | 'consumer' | 'schema';

interface Saved {
  v: 1;
  realm: string | null;
  user: string | null;
  consumer: string | null;
  schema: string | null;
  topics: Record<string, Record<string, number>>;
}

/** What a consumer applied of one stream: everything up to `base`, and some beyond. */
interface Progress {
  base: number;
  ahead: Set<number>;
}

interface Held {
  messages: WSMessage[];
  /** Messages were lost to the limit. */
  incomplete: boolean;
}

interface Run {
  controller: AbortController;
}

const DEFAULT_MAX_HELD = 10_000;
/** Commits held beyond a hole, per stream; more are not tracked. */
const MAX_AHEAD = 10_000;
const RETRY_FIRST_MS = 1_000;
const RETRY_MAX_MS = 30_000;
/** Reasons that concern what came before the topic's subscription: the coming resync covers them. */
const BEFORE_SUBSCRIPTION: ReadonlySet<ContinuityReason> = new Set(['not_recovered', 'epoch_changed']);

const CONTINUOUS = Object.freeze({ subscription: 'requested', continuity: 'continuous', reason: null }) as TopicState;

/**
 * Validates the options of a client: an unknown mode, or recover-or-resync
 * without a resync handler or a consumer, throws.
 */
export function checkDeliveryOptions(options: DeliveryOptions | undefined, consumer: string | undefined): void {
  const mode = options?.mode ?? 'best-effort';
  if (mode !== 'best-effort' && mode !== 'recover-or-resync') {
    throw new Error(`delivery.mode '${String(mode)}' is not one of 'best-effort' and 'recover-or-resync'`);
  }
  if (mode === 'recover-or-resync') {
    if (typeof options?.resync !== 'function') {
      throw new Error("delivery.mode 'recover-or-resync' needs a delivery.resync handler");
    }
    if (typeof consumer !== 'string' || consumer === '') {
      throw new Error("delivery.mode 'recover-or-resync' needs the client's consumer name");
    }
  }
}

export class Delivery {
  readonly mode: DeliveryMode;
  private readonly options: DeliveryOptions;
  private readonly hooks: DeliveryHooks;
  private readonly maxHeld: number;
  private states = new Map<string, TopicState>();
  private snapshot: Readonly<Record<string, TopicState>> = Object.freeze({});
  private held = new Map<string, Held>();
  private progress = new Map<string, Map<string, Progress>>();
  private runs = new Map<string, Run>();
  /** Topics that broke off while they resynced, and why. */
  private again = new Map<string, ContinuityReason>();
  private retries = new Map<string, { attempt: number; timer: ReturnType<typeof setTimeout> | null }>();
  private scheduled = false;
  private persistScheduled = false;
  /** The saved checkpoint, until the first connection says whose it is. */
  private pending: string | null;
  private realm: string | null;
  private closed = false;

  private readonly consumer: string | null;

  constructor(
    options: DeliveryOptions | undefined,
    scope: { consumer: string | null; realm: string | null },
    hooks: DeliveryHooks,
  ) {
    checkDeliveryOptions(options, scope.consumer ?? undefined);
    this.options = options ?? {};
    this.mode = this.options.mode ?? 'best-effort';
    this.hooks = hooks;
    this.maxHeld = Math.max(1, this.options.maxHeld ?? DEFAULT_MAX_HELD);
    this.consumer = scope.consumer;
    this.realm = scope.realm;
    this.pending =
      this.options.checkpoint !== undefined
        ? this.options.checkpoint
        : hooks.storage.get(CHECKPOINT_KEY);
  }

  private get barrier(): boolean {
    return this.mode === 'recover-or-resync';
  }

  /** The topics' states, the same object until one changes. */
  topicStates(): Readonly<Record<string, TopicState>> {
    return this.snapshot;
  }

  continuous(topics: readonly string[]): boolean {
    return topics.every((topic) => this.states.get(topic)?.continuity === 'continuous');
  }

  /**
   * One of `topics` the client holds is not continuous: a kept send that
   * requires them waits (one the client let go of holds nothing back).
   */
  holdsBack(topics: readonly string[]): boolean {
    return topics.some((topic) => {
      const state = this.states.get(topic);
      return state !== undefined && state.continuity !== 'continuous';
    });
  }

  // -- topics ------------------------------------------------------------------

  /** The client holds `topics` now. */
  track(topics: readonly string[]): void {
    let changed = false;
    for (const topic of topics) {
      if (this.states.has(topic)) continue;
      if (this.barrier) {
        this.states.set(topic, { subscription: 'requested', continuity: 'resync_required', reason: 'cold_start' });
        this.held.set(topic, { messages: [], incomplete: false });
      } else {
        this.states.set(topic, CONTINUOUS);
      }
      changed = true;
    }
    if (changed) this.publish();
  }

  /** The client let go of `topics`. */
  untrack(topics: readonly string[]): void {
    let changed = false;
    for (const topic of topics) {
      if (!this.states.delete(topic)) continue;
      this.held.delete(topic);
      this.progress.delete(topic);
      this.again.delete(topic);
      this.abort(topic);
      this.clearRetry(topic);
      changed = true;
    }
    if (changed) {
      this.publish();
      this.schedulePersist();
    }
  }

  /** The server's answer to a subscription: what it took and what it refused. */
  subscribed(taken: readonly string[], refused: readonly string[]): void {
    let changed = false;
    for (const topic of taken) {
      const state = this.states.get(topic);
      if (!state || state.subscription === 'active') continue;
      this.states.set(topic, { ...state, subscription: 'active' });
      changed = true;
    }
    for (const topic of refused) {
      const state = this.states.get(topic);
      if (!state || state.subscription === 'denied') continue;
      this.states.set(topic, { ...state, subscription: 'denied' });
      changed = true;
    }
    if (changed) this.publish();
    this.scheduleResync();
  }

  /** The connection went: every topic is asked for again, and nothing it held is current. */
  connectionLost(): void {
    let changed = false;
    for (const [topic, state] of this.states) {
      this.abort(topic);
      this.again.delete(topic);
      let next: TopicState = state.subscription === 'requested' ? state : { ...state, subscription: 'requested' };
      if (this.barrier) {
        this.held.set(topic, { messages: [], incomplete: false });
        if (state.continuity !== 'resync_required') next = { ...next, continuity: 'resync_required', reason: 'reconnected' };
      } else if (state.continuity === 'resyncing') {
        next = { ...next, continuity: 'resync_required' };
      }
      if (next === state) continue;
      this.states.set(topic, next);
      if (next.continuity !== state.continuity) {
        this.hooks.emit('wseContinuity', { topic, continuity: next.continuity, reason: next.reason });
      }
      changed = true;
    }
    if (changed) this.publish();
  }

  /** The topic's messages broke off (a gap the client could not fill). */
  brokeOff(topic: string, reason: ContinuityReason): void {
    const state = this.states.get(topic);
    if (!state) return;
    if (state.continuity === 'resyncing') {
      // What came before the subscription is in the state being loaded; a
      // message lost since may not be.
      if (!BEFORE_SUBSCRIPTION.has(reason)) this.again.set(topic, reason);
      return;
    }
    if (state.continuity === 'resync_required') return;
    if (this.barrier) this.held.set(topic, { messages: [], incomplete: false });
    this.setContinuity(topic, 'resync_required', reason);
    this.scheduleResync();
  }

  /** The application asks for `topics` to be brought up to date. */
  resync(topics: readonly string[]): void {
    for (const topic of topics) {
      const state = this.states.get(topic);
      if (!state) continue;
      if (!this.options.resync) {
        // Best effort without a handler: the application brought them up to date itself.
        if (state.continuity !== 'continuous') this.setContinuity(topic, 'continuous', null);
        continue;
      }
      if (state.continuity === 'resyncing') this.abort(topic);
      if (this.barrier) this.held.set(topic, { messages: [], incomplete: false });
      if (state.continuity !== 'resync_required') this.setContinuity(topic, 'resync_required', 'requested');
    }
    this.scheduleResync();
  }

  // -- messages ----------------------------------------------------------------

  /** A message of a topic, about to reach the handlers: true when the barrier holds it. */
  hold(message: WSMessage): boolean {
    const topic = message.tp;
    if (!topic) return false;
    const state = this.states.get(topic);
    if (!state) return false;
    if (this.barrier && state.continuity !== 'continuous') {
      const held = this.held.get(topic) ?? { messages: [], incomplete: false };
      this.held.set(topic, held);
      if (held.messages.length < this.maxHeld) held.messages.push(message);
      else held.incomplete = true;
      return true;
    }
    this.handedOn(topic, message);
    return false;
  }

  /** A message reached the handlers: a stream first seen starts its checkpoint just before it. */
  private handedOn(topic: string, message: WSMessage): void {
    this.generations.set(message, this.generation);
    const offset = offsetOf(message);
    if (offset === null) return;
    const epoch = String(message.e);
    let streams = this.progress.get(topic);
    if (!streams) {
      streams = new Map();
      this.progress.set(topic, streams);
    }
    if (!streams.has(epoch)) streams.set(epoch, { base: offset - 1, ahead: new Set() });
  }

  // -- the checkpoint ------------------------------------------------------------

  /** The consumer applied `message`: one handed on to an earlier user counts for nobody. */
  commit(message: WSMessage): void {
    const topic = message.tp;
    const offset = offsetOf(message);
    if (!topic || offset === null) return;
    if ((this.generations.get(message) ?? this.generation) !== this.generation) return;
    const progress = this.progress.get(topic)?.get(String(message.e));
    if (!progress || offset <= progress.base) return;
    if (offset === progress.base + 1) {
      progress.base = offset;
      while (progress.ahead.delete(progress.base + 1)) progress.base++;
    } else if (progress.ahead.size < MAX_AHEAD) {
      progress.ahead.add(offset);
    }
    this.schedulePersist();
  }

  /** The checkpoint, to save with the application's own state. */
  checkpoint(): string {
    const topics: Record<string, Record<string, number>> = {};
    for (const [topic, streams] of this.progress) {
      const out: Record<string, number> = {};
      // A stream nothing of which was applied (base -1) is left out.
      for (const [epoch, progress] of streams) if (progress.base >= 0) out[epoch] = progress.base;
      if (Object.keys(out).length > 0) topics[topic] = out;
    }
    const saved: Saved = {
      v: 1,
      realm: this.realm,
      user: this.hooks.user(),
      consumer: this.consumer,
      schema: this.options.schema ?? null,
      topics,
    };
    return JSON.stringify(saved);
  }

  /**
   * recover-or-resync: where the recovery of `topics` starts, from what the
   * consumer committed (null in best-effort: from where delivery is). The
   * saved checkpoint is taken here, once the connection named its user.
   */
  recoveryPositions(topics: readonly string[]): Record<string, TopicPosition> | null {
    this.userIs(this.hooks.user());
    if (!this.barrier) return null;
    this.takeSaved();
    const out: Record<string, TopicPosition> = {};
    for (const topic of topics) {
      const entries = [...(this.progress.get(topic) ?? [])]
        .filter(([, progress]) => progress.base >= 0)
        .map(([epoch, progress]) => [epoch, progress.base] as const);
      if (entries.length === 0) continue;
      const [epoch, offset] = entries[entries.length - 1];
      out[topic] = { epoch, offset, streams: Object.fromEntries(entries) };
    }
    return out;
  }

  /** The user of the last connection, once there was one. */
  private lastUser: string | null | undefined = undefined;
  /** The user generation: a new one starts with another user's connection. */
  private generation = 0;
  /** The generation each message was handed on in: a commit counts only in it. */
  private readonly generations = new WeakMap<WSMessage, number>();

  /** A connection of `user`: what another user applied is not this one's. */
  private userIs(user: string | null): void {
    if (this.lastUser !== undefined && this.lastUser !== user) {
      this.progress.clear();
      this.generation++;
      this.schedulePersist();
    }
    this.lastUser = user;
  }

  private takeSaved(): void {
    const text = this.pending;
    this.pending = null;
    if (!text) return;
    let saved: Saved;
    try {
      saved = JSON.parse(text);
    } catch {
      this.reject('malformed');
      return;
    }
    if (!saved || typeof saved !== 'object' || typeof saved.topics !== 'object' || saved.topics === null) {
      this.reject('malformed');
      return;
    }
    // Every offset a whole number, or the checkpoint is not one: the Rust and
    // Python clients read it so (crates/wse-client/tests/vectors/checkpoint.json).
    const read = new Map<string, Map<string, Progress>>();
    for (const [topic, streams] of Object.entries(saved.topics)) {
      if (!streams || typeof streams !== 'object' || Array.isArray(streams)) {
        this.reject('malformed');
        return;
      }
      const map = new Map<string, Progress>();
      for (const [key, base] of Object.entries(streams)) {
        if (!Number.isSafeInteger(base) || base < 0) {
          this.reject('malformed');
          return;
        }
        const epoch = epochKey(key);
        if (epoch !== null) map.set(epoch, { base, ahead: new Set() });
      }
      read.set(topic, map);
    }
    const mismatch: CheckpointMismatch | null =
      saved.v !== 1
        ? 'version'
        : (saved.realm ?? null) !== this.realm
          ? 'realm'
          : (saved.user ?? null) !== this.hooks.user()
            ? 'user'
            : (saved.consumer ?? null) !== (this.consumer)
              ? 'consumer'
              : (saved.schema ?? null) !== (this.options.schema ?? null)
                ? 'schema'
                : null;
    if (mismatch) {
      this.reject(mismatch);
      return;
    }
    for (const [topic, map] of read) {
      if (this.states.has(topic) && map.size > 0 && !this.progress.has(topic)) this.progress.set(topic, map);
    }
  }

  private reject(reason: CheckpointMismatch): void {
    logger.warn(`The saved checkpoint is not taken: its ${reason} is not this client's`);
    this.hooks.emit('wseCheckpointRejected', { reason });
  }


  private schedulePersist(): void {
    if (this.persistScheduled || this.closed) return;
    this.persistScheduled = true;
    queueMicrotask(() => {
      this.persistScheduled = false;
      if (!this.closed) this.persist();
    });
  }

  private persist(): void {
    this.hooks.storage.set(CHECKPOINT_KEY, this.checkpoint());
  }

  // -- resyncs ---------------------------------------------------------------------

  private scheduleResync(): void {
    if (this.scheduled || this.closed || !this.options.resync) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      if (this.closed) return;
      const topics = [...this.states]
        .filter(
          ([topic, state]) =>
            state.subscription === 'active' &&
            state.continuity === 'resync_required' &&
            !this.retries.get(topic)?.timer,
        )
        .map(([topic]) => topic);
      if (topics.length > 0) void this.run(topics);
    });
  }

  private async run(topics: string[]): Promise<void> {
    const run: Run = { controller: new AbortController() };
    const reasons: Record<string, ContinuityReason> = {};
    const committed: Record<string, Record<string, number>> = {};
    for (const topic of topics) {
      reasons[topic] = this.states.get(topic)!.reason ?? 'requested';
      const applied = [...(this.progress.get(topic) ?? [])].filter(([, progress]) => progress.base >= 0);
      if (applied.length > 0) {
        committed[topic] = Object.fromEntries(applied.map(([epoch, progress]) => [epoch, progress.base]));
      }
      this.runs.set(topic, run);
      this.setContinuity(topic, 'resyncing', reasons[topic], false);
    }
    this.publish();

    let result: ResyncResult | void;
    try {
      result = await this.options.resync!({ topics, reasons, committed, signal: run.controller.signal });
    } catch (error) {
      if (run.controller.signal.aborted) return;
      logger.warn(`The resync of ${topics.join(', ')} failed:`, error);
      for (const topic of topics) {
        if (this.runs.get(topic) !== run) continue;
        this.runs.delete(topic);
        this.setContinuity(topic, 'resync_required', 'resync_failed');
        this.retryLater(topic);
      }
      return;
    }
    if (run.controller.signal.aborted) return;

    const includes = result && typeof result.includes === 'function' ? result.includes : null;
    for (const topic of topics) {
      if (this.runs.get(topic) !== run) continue;
      this.runs.delete(topic);
      this.clearRetry(topic);
      const held = this.held.get(topic);
      const broke = held?.incomplete ? 'overflow' : this.again.get(topic);
      this.again.delete(topic);
      if (broke) {
        // Something may be missing between the state loaded and what follows.
        if (this.barrier) this.held.set(topic, { messages: [], incomplete: false });
        this.setContinuity(topic, 'resync_required', broke);
        continue;
      }
      this.held.delete(topic);
      const onward = (held?.messages ?? []).filter((message) => !(includes && safely(includes, message)));
      this.restart(topic, onward);
      this.setContinuity(topic, 'continuous', null);
      for (const message of onward) {
        if (this.states.get(topic)?.continuity !== 'continuous') break;
        this.handedOn(topic, message);
        this.hooks.route(message);
      }
    }
    this.scheduleResync();
  }

  /**
   * The state loaded covers each stream up to the first message handed on
   * after it, or up to what the client received when none is.
   */
  private restart(topic: string, onward: readonly WSMessage[]): void {
    const streams = new Map<string, Progress>();
    for (const [epoch, offset] of Object.entries(this.hooks.received(topic))) {
      streams.set(epoch, { base: offset, ahead: new Set() });
    }
    const first = new Set<string>();
    for (const message of onward) {
      const offset = offsetOf(message);
      const epoch = String(message.e);
      if (offset === null || first.has(epoch)) continue;
      first.add(epoch);
      streams.set(epoch, { base: offset - 1, ahead: new Set() });
    }
    this.progress.set(topic, streams);
    this.schedulePersist();
  }

  private retryLater(topic: string): void {
    const attempt = (this.retries.get(topic)?.attempt ?? 0) + 1;
    const delay = Math.min(RETRY_MAX_MS, RETRY_FIRST_MS * 2 ** (attempt - 1));
    const timer = setTimeout(() => {
      const retry = this.retries.get(topic);
      if (retry) retry.timer = null;
      this.scheduleResync();
    }, delay);
    this.retries.set(topic, { attempt, timer });
  }

  private clearRetry(topic: string): void {
    const retry = this.retries.get(topic);
    if (retry?.timer) clearTimeout(retry.timer);
    this.retries.delete(topic);
  }

  private abort(topic: string): void {
    const run = this.runs.get(topic);
    if (!run) return;
    this.runs.delete(topic);
    if (![...this.runs.values()].includes(run)) run.controller.abort();
  }

  // -- states ------------------------------------------------------------------------

  private setContinuity(
    topic: string,
    continuity: TopicState['continuity'],
    reason: ContinuityReason | null,
    publish = true,
  ): void {
    const state = this.states.get(topic);
    if (!state || (state.continuity === continuity && state.reason === reason)) return;
    this.states.set(topic, { subscription: state.subscription, continuity, reason });
    this.hooks.emit('wseContinuity', { topic, continuity, reason });
    if (publish) this.publish();
  }

  private publish(): void {
    this.snapshot = Object.freeze(Object.fromEntries(this.states));
    this.hooks.changed();
  }

  /** The client is done: nothing more runs; a pending checkpoint is written. */
  close(): void {
    if (this.closed) return;
    if (this.persistScheduled) this.persist();
    this.closed = true;
    for (const topic of [...this.runs.keys()]) this.abort(topic);
    for (const topic of [...this.retries.keys()]) this.clearRetry(topic);
  }

  /** The user signed out: what the client held of the user's goes. */
  forget(): void {
    this.progress.clear();
    this.pending = null;
  }
}

function offsetOf(message: WSMessage): number | null {
  if (message.o === undefined || message.o === null) return null;
  const offset = Number(message.o);
  return Number.isSafeInteger(offset) && offset >= 0 ? offset : null;
}

function safely(includes: (message: WSMessage) => boolean, message: WSMessage): boolean {
  try {
    return includes(message) === true;
  } catch (error) {
    logger.error('delivery includes() failed; the message is handed on:', error);
    return false;
  }
}
