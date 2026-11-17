// =============================================================================
// WebSocket Engine - Session (one connection and its services)
// =============================================================================
//
// A session owns everything one connection needs: MessageProcessor,
// ConnectionManager, NetworkMonitor and the IndexedDB offline queue, all of
// one client (its ClientContext). Initialization checks `destroyed` after
// every await: a token change or logout during init never leaves an
// untracked socket behind. Who holds sessions: the page's session manager
// for useWSE() (WSESession.ts), and each core client for itself.

import {
  ConnectionState,
  DiagnosticsConfig,
  MessageOptions,
  MessagePriority,
  OfflineConfig,
  PerformanceConfig,
  RateLimitConfig,
  ReconnectionStrategy,
  SecurityConfig,
  SendStatus,
  StorageState,
  WSMessage,
  SignedMessageOptions,
} from '../types.js';
import {
  DEFAULT_MAX_INBOUND_MESSAGE_SIZE,
  DEFAULT_RECOVERY_TIMEOUT_MS,
  DEFAULT_TOPICS,
  HEARTBEAT_INTERVAL,
  RECONNECT_MAX_ATTEMPTS,
  SERVER_READY_TIMEOUT,
  WS_PROTOCOL_VERSION,
  getEndpoints,
} from '../constants.js';
import { loadTopics, saveTopics, type ClientContext } from '../core/context.js';
import { ConnectionManager } from './ConnectionManager.js';
import { MessageProcessor } from './MessageProcessor.js';
import { NetworkMonitor } from './NetworkMonitor.js';
import { AdaptiveQualityManager, QueryClientLike } from './AdaptiveQualityManager.js';
import type { OfflineQueue } from './OfflineQueue.js';
import { indexedDbOutbox } from './IndexedDbOutbox.js';
import { Outbox, reportOutcomes, type OutboxStore } from './Outbox.js';

/** What a client tells of a storage that failed: what it did, and the error's name. */
export function storageErrorDetail(operation: string, error: unknown) {
  const name = error instanceof Error || (typeof DOMException !== 'undefined' && error instanceof DOMException)
    ? (error as Error).name
    : 'Error';
  const message = error instanceof Error ? error.message : String(error);
  return { operation, name, message, quota: name === 'QuotaExceededError' };
}
import { EventSequencer, type TopicPosition } from './EventSequencer.js';
import { EventHandlers } from '../handlers/EventHandlers.js';
import { logger } from '../utils/logger.js';
import { generateId } from '../utils/id.js';
import { signingKeyFrom, signMessage, type SigningKey } from '../utils/signing.js';

/** Handler registration surface handed to `registerHandlers`. */
export interface HandlerRegistry {
  registerHandler: (type: string, handler: (msg: any) => void) => void;
  getRegisteredHandlers?: () => string[];
}

/**
 * Options of useWSE() (third argument). Nested groups may be partial; missing
 * fields take the defaults below.
 */
export interface UseWSEConfig {
  /** WebSocket endpoints. Default: same origin, path /wse. */
  endpoints?: string[];
  reconnection?: Partial<ReconnectionStrategy>;
  security?: Partial<SecurityConfig>;
  performance?: Partial<PerformanceConfig>;
  offline?: Partial<OfflineConfig>;
  diagnostics?: Partial<DiagnosticsConfig>;

  /**
   * Refresh the auth cookie. Called every 10 minutes in cookie mode (a
   * failure keeps the connection and is retried with backoff) and once after
   * the server rejects the credentials (4401/4403). Must throw/reject when
   * the refresh fails -- e.g. check `response.ok` after fetch().
   */
  refreshAuthToken?: () => Promise<void>;

  /** Handler names that must be registered before connecting. */
  criticalHandlers?: string[];

  /** Register application handlers: `processor.registerHandler(type, fn)`. */
  registerHandlers?: (messageProcessor: HandlerRegistry) => void;

  /**
   * Opt-in: drop application messages of one type that arrive faster than
   * this (ms). 0 (default) delivers everything.
   */
  eventThrottleMs?: number;

  /** Outbound rate limit; `false` disables it. Default 1000 burst, 100 msg/s. */
  rateLimit?: RateLimitConfig | false;

  /**
   * 'msgpack' has the server send msgpack (binary, smaller than JSON), and
   * the client send in it too, except frames sealed for E2E. Default 'json'.
   */
  wireFormat?: 'json' | 'msgpack';

  /**
   * Inbound size limit in bytes (characters for text frames), checked while
   * inflating compressed frames. Default 16 MiB.
   */
  maxMessageSize?: number;

  /**
   * After a reconnect that asks the server to replay missed messages, hold
   * out-of-order messages this long while the replay fills the gap.
   * Default 3000 ms; 0 delivers at once (with a `wseGap` event).
   */
  recoveryTimeoutMs?: number;

  /**
   * Proceed without `server_ready` after this many ms (servers without
   * jwt_secret send none). Default 10000; 0 waits forever.
   */
  serverReadyTimeout?: number;

  /**
   * Makes the WebSocket for a URL; default the global WebSocket. Outside
   * browsers (Node, React Native) it may pass the headers that environment's
   * WebSocket allows, an Authorization one say.
   */
  webSocket?: (url: string) => WebSocket;

  /**
   * TanStack Query client whose query defaults (staleTime, refetchInterval,
   * refetchOnWindowFocus) follow the measured connection quality: little
   * polling while the socket is fast and steady, more when it is slow,
   * unstable or down. The app's other defaults are kept.
   */
  queryClient?: QueryClientLike;
}

/** Fully resolved options of one session. */
export interface ResolvedWSEOptions {
  endpoints: string[];
  reconnection: ReconnectionStrategy;
  security: SecurityConfig;
  performance: PerformanceConfig;
  offline: OfflineConfig;
  diagnostics: DiagnosticsConfig;
  rateLimit: RateLimitConfig | false;
  maxMessageSize: number;
  wireFormat: 'json' | 'msgpack';
  recoveryTimeoutMs: number;
  serverReadyTimeout: number;
  eventThrottleMs: number;
  criticalHandlers: string[];
  refreshAuthToken?: () => Promise<void>;
  registerHandlers?: (messageProcessor: HandlerRegistry) => void;
  queryClient?: QueryClientLike;
  webSocket?: (url: string) => WebSocket;
}

export const DEFAULT_RECONNECTION: ReconnectionStrategy = {
  mode: 'adaptive',
  baseDelay: 1000,
  maxDelay: 30000,
  maxAttempts: RECONNECT_MAX_ATTEMPTS, // -1: retry forever
  factor: 1.5,
  jitter: true,
};

export function resolveOptions(config: UseWSEConfig | undefined): ResolvedWSEOptions {
  const endpoints = config?.endpoints && config.endpoints.length > 0
    ? [...config.endpoints]
    : getEndpoints();

  const reconnection: ReconnectionStrategy = { ...DEFAULT_RECONNECTION, ...config?.reconnection };
  if (typeof reconnection.maxAttempts !== 'number' || Number.isNaN(reconnection.maxAttempts)) {
    reconnection.maxAttempts = RECONNECT_MAX_ATTEMPTS;
  }

  return {
    endpoints,
    reconnection,
    security: {
      encryptionEnabled: false,
      ...config?.security,
    },
    performance: {
      batchSize: 10,
      batchTimeout: 500,
      maxQueueSize: 10000,
      memoryLimit: 50 * 1024 * 1024,
      compressionThreshold: 1024,
      maxPendingInbound: 10_000,
      maxPendingInboundBytes: 64 * 1024 * 1024,
      maxPendingEncrypted: 256,
      maxPendingEncryptedBytes: 16 * 1024 * 1024,
      ...config?.performance,
    },
    offline: {
      enabled: true,
      maxSize: 100_000,
      maxAge: 3600000,
      persistToStorage: true,
      maxBytes: 16 * 1024 * 1024,
      ...config?.offline,
    },
    diagnostics: {
      enabled: false,
      metricsInterval: 60000,
      sampleRate: 0.1,
      healthCheckInterval: HEARTBEAT_INTERVAL,
      ...config?.diagnostics,
    },
    rateLimit: config?.rateLimit === false ? false : { ...config?.rateLimit },
    maxMessageSize: config?.maxMessageSize && config.maxMessageSize > 0
      ? config.maxMessageSize
      : DEFAULT_MAX_INBOUND_MESSAGE_SIZE,
    wireFormat: config?.wireFormat === 'msgpack' ? 'msgpack' : 'json',
    recoveryTimeoutMs: config?.recoveryTimeoutMs ?? DEFAULT_RECOVERY_TIMEOUT_MS,
    serverReadyTimeout: config?.serverReadyTimeout ?? SERVER_READY_TIMEOUT,
    eventThrottleMs: config?.eventThrottleMs ?? 0,
    criticalHandlers: config?.criticalHandlers ? [...config.criticalHandlers] : [],
    refreshAuthToken: config?.refreshAuthToken,
    registerHandlers: config?.registerHandlers,
    queryClient: config?.queryClient,
    webSocket: config?.webSocket,
  };
}

/**
 * The client's outbox, made with `offline`'s limits the first time; null
 * when `offline.enabled` is false.
 */
/**
 * Where the client's outbox keeps what waits: the store the application
 * named, else IndexedDB for a durable scope (a storage and a consumer
 * named), else memory (null).
 */
export function outboxStoreFor(ctx: ClientContext, offline: OfflineConfig): OutboxStore | null {
  if (!offline.enabled) return null;
  if (offline.store) return offline.store;
  const database = ctx.storage.offlineDatabase;
  return offline.persistToStorage && database ? indexedDbOutbox(database) : null;
}

export function ensureOutbox(ctx: ClientContext, offline: OfflineConfig): Outbox | null {
  if (!offline.enabled) return null;
  if (ctx.outbox.current) return ctx.outbox.current;
  const outbox = new Outbox(
    {
      maxMessages: offline.maxSize,
      maxBytes: offline.maxBytes ?? 16 * 1024 * 1024,
      maxAge: offline.maxAge,
    },
    {
      // What runs out on its own timer is said, with a connection or without.
      onExpired: (outcomes) => reportOutcomes(ctx.emit, outcomes),
      store: outboxStoreFor(ctx, offline) ?? undefined,
      // Said after this turn: a store is opened as the client is made,
      // before the application listens.
      onStorageError: (operation, error) =>
        queueMicrotask(() => ctx.emit('wseStorageError', storageErrorDetail(operation, error))),
      // Where what waits lives: in the store, or in memory when it failed.
      onOpen: (durable) => {
        const state = durable ? 'durable' : 'unavailable';
        const store = ctx.state.getState();
        if (store.storageState === state) return;
        store.setStorageState(state);
        queueMicrotask(() => ctx.emit('wseStorage', { state }));
      },
    },
  );
  ctx.outbox.current = outbox;
  return outbox;
}

export function uniqueTopics(topics: unknown): string[] {
  if (!Array.isArray(topics)) return [];
  return [...new Set(topics.filter((t): t is string => typeof t === 'string' && t.length > 0))];
}

export function buildMessage(type: string, payload: any, options: MessageOptions = {}): WSMessage {
  const message: WSMessage = {
    t: type,
    p: payload,
    id: options.id || generateId(),
    ts: new Date().toISOString(),
    v: WS_PROTOCOL_VERSION,
  };
  if (options.priority) message.pri = options.priority;
  if (options.correlation_id) message.cid = options.correlation_id;
  if (options.encrypted) message.enc = true;
  return message;
}

/** Epoch ms after which a message with `ttl` is no longer worth sending. */
export function expiryOf(options: MessageOptions): number | undefined {
  const ttl = options.ttl;
  return ttl !== undefined && Number.isFinite(ttl) && ttl >= 0 ? Date.now() + ttl : undefined;
}

// A signed message's nonce (`n`): 1 to this many characters.
const MAX_NONCE_LENGTH = 128;
/** How long a signed message may wait behind older ones before it is stale. */
const SIGNED_MESSAGE_TTL_MS = 10_000;

export interface SessionParams {
  key: string;
  auth: string;
  initialTopics?: string[];
  config?: UseWSEConfig;
}

/**
 * Whose data the client holds (the outgoing queue, topics, recovery
 * positions): the auth value it was made under and the user the server
 * confirmed for it. Kept apart from the session, which may end and come back
 * for someone else.
 */
export interface DataOwner {
  auth: string;
  userId: string | null;
}

/** What a session is of: its client's context, and what outlives the session. */
export interface SessionDeps {
  context: ClientContext;
  /** Dedup ids and per-topic recovery positions, kept across sessions of one user. */
  sequencer: EventSequencer;
  /** Whose data the client holds now. */
  dataOwner?: () => DataOwner | null;
  /** What setCompression() asked for (null: the option decides). */
  compression?: () => boolean | null;
  /** Answer the page's `wseRequestSnapshot` window events (the default client). */
  windowSnapshots?: boolean;
}

export class WSESession {
  readonly key: string;
  readonly auth: string;
  readonly options: ResolvedWSEOptions;
  readonly processor: MessageProcessor;
  connection: ConnectionManager | null = null;
  networkMonitor: NetworkMonitor | null = null;
  quality: AdaptiveQualityManager | null = null;
  /** Always null since 3.2: the outbox keeps what waits, in IndexedDB too. */
  readonly offlineQueue: OfflineQueue | null = null;
  refCount = 0;
  destroyed = false;

  private userId: string | null = null;
  private topicsResolved: boolean;
  /** The topics the consumers asked for: what another user starts from. */
  private readonly consumerTopics = new Set<string>();
  /** A consumer named its topics, an empty list included: no defaults then. */
  private topicsExplicit = false;
  private startPromise: Promise<void> | null = null;
  private windowListener: ((event: Event) => void) | null = null;
  private stopWatchingState: (() => void) | null = null;
  // This client's signing key, derived once from security.signingKey.
  private signingKey: Promise<SigningKey> | null = null;
  private readonly ctx: ClientContext;
  private readonly deps: SessionDeps;
  private readonly sequencer: EventSequencer;

  constructor(params: SessionParams, deps: SessionDeps) {
    this.key = params.key;
    this.auth = params.auth;
    this.options = resolveOptions(params.config);
    this.ctx = deps.context;
    this.deps = deps;
    this.sequencer = deps.sequencer;
    const sequencer = deps.sequencer;

    this.processor = new MessageProcessor(
      this.options.performance.batchSize,
      this.options.performance.batchTimeout,
      {
        maxMessageSize: this.options.maxMessageSize,
        recoveryTimeoutMs: this.options.recoveryTimeoutMs,
        eventThrottleMs: this.options.eventThrottleMs,
        sequencer,
        handlerSampleRate: this.options.diagnostics.enabled ? this.options.diagnostics.sampleRate : 0,
        context: this.ctx,
        maxPendingInbound: this.options.performance.maxPendingInbound,
        maxPendingInboundBytes: this.options.performance.maxPendingInboundBytes,
      },
    );

    this.processor.setServerSigningKeys(this.options.security.serverSigningKeys);

    EventHandlers.registerAll(this.processor);
    this.options.registerHandlers?.(this.processor);

    // Topics the consumer asked for are the base; without them the first
    // server_ready restores this user's saved subscriptions (or defaults).
    const initial = params.initialTopics !== undefined ? uniqueTopics(params.initialTopics) : null;
    if (initial !== null) {
      this.topicsExplicit = true;
      for (const topic of initial) this.consumerTopics.add(topic);
      this.ctx.state.getState().addTopics(initial);
    }
    this.topicsResolved = initial !== null || this.ctx.state.getState().activeTopics.length > 0;
  }

  /** A further useWSE() consumer joined this session. */
  attachConsumer(params: SessionParams): void {
    if (this.destroyed) return;
    params.config?.registerHandlers?.(this.processor);
    const topics = params.initialTopics !== undefined ? uniqueTopics(params.initialTopics) : [];
    if (params.initialTopics !== undefined) this.topicsExplicit = true;
    for (const topic of topics) this.consumerTopics.add(topic);
    if (topics.length > 0) {
      const active = new Set(this.ctx.state.getState().activeTopics);
      const missing = topics.filter(t => !active.has(t));
      if (missing.length > 0) this.subscribe(missing);
    }
  }

  start(): Promise<void> {
    if (!this.startPromise) {
      this.startPromise = this.run().catch((error) => {
        if (this.destroyed) return;
        logger.error('WSE initialization failed:', error);
        const store = this.ctx.state.getState();
        store.setLastError(error instanceof Error ? error.message : 'Initialization failed');
        store.setConnectionState(ConnectionState.ERROR);
      });
    }
    return this.startPromise;
  }

  private async run(): Promise<void> {
    const o = this.options;
    this.ctx.state.getState().updateConfig({ maxReconnectAttempts: o.reconnection.maxAttempts });
    this.ctx.queue.getState().setCapacity(o.performance.maxQueueSize);
    this.ctx.queue.getState().setMemoryLimit(o.performance.memoryLimit);
    // The client's outbox: made once, kept across its sessions.
    ensureOutbox(this.ctx, o.offline);

    if (o.criticalHandlers.length > 0) {
      const missing = o.criticalHandlers.filter(h => !this.processor.isHandlerRegistered(h));
      if (missing.length > 0) {
        throw new Error(`Failed to register critical handlers: ${missing.join(', ')}`);
      }
    }
    this.processor.setReady(true);

    if (o.security.encryptionEnabled) {
      try {
        await this.ctx.security.initialize({ encryptionEnabled: true });
      } catch (error) {
        if (this.destroyed) return;
        // Fail closed: no Web Crypto (e.g. a plain-http page) means no
        // encryption, and encryption was required -- do not connect.
        const reason = `encryption unavailable (${error instanceof Error ? error.message : String(error)})`;
        logger.error(`WSE: ${reason}; not connecting`);
        const store = this.ctx.state.getState();
        store.setLastError(`Encryption failed: ${reason}`);
        store.setConnectionState(ConnectionState.ERROR);
        this.ctx.emit('wseEncryptionFailed', { reason });
        return;
      }
    }
    if (this.destroyed) return;

    // The outbox's store opens in its own time (IndexedDB): the connection
    // waits for what it holds.
    await this.ctx.outbox.current?.opened;
    if (this.destroyed) return;

    this.networkMonitor = new NetworkMonitor(100, o.diagnostics.metricsInterval, o.diagnostics.enabled, this.ctx);
    // Quality tracking runs when something consumes it: a QueryClient to
    // steer, or diagnostics (the connectionQualityChanged event).
    if (o.queryClient || o.diagnostics.enabled) {
      const quality = new AdaptiveQualityManager({ queryClient: o.queryClient ?? null, emit: this.ctx.emit });
      this.quality = quality;
      this.stopWatchingState = this.ctx.state.subscribe((state, previous) => {
        if (state.connectionState !== previous.connectionState) {
          quality.setConnectionState(state.connectionState);
        }
      });
    }
    this.processor.setLatencyListener((latency) => {
      this.networkMonitor?.recordLatency(latency);
      this.quality?.recordLatency(latency);
    });

    const connection = new ConnectionManager(
      o.endpoints,
      o.reconnection,
      (data) => {
        void this.processor.processIncoming(data);
      },
      () => {
        // The ConnectionManager publishes its state to the store itself.
      },
      (details) => this.handleServerReady(details),
      {
        refreshAuthToken: o.refreshAuthToken,
        encryptionRequired: o.security.encryptionEnabled,
        e2e: {
          serverKeys: o.security.serverKeys,
          unauthenticated: o.security.unauthenticated,
          legacy: o.security.legacyE2e,
          // An E2E session with a signing key is bound to it, unless told not to.
          bindKey:
            o.security.encryptionEnabled && !o.security.legacyE2e && o.security.signingKey
            && o.security.bindSession !== false
              ? (this.signingKey ??= signingKeyFrom(o.security.signingKey))
              : undefined,
        },
        rateLimit: o.rateLimit,
        serverReadyTimeout: o.serverReadyTimeout,
        outboxHello: () => this.ctx.outbox.current?.hello() ?? null,
        onTransportReady: () => {
          this.ctx.state.getState().setTransportReady(true);
          this.processor.transportReady();
          void this.processor.flush();
        },
        onSocketClosed: () => {
          this.ctx.state.getState().setTransportReady(false);
          this.processor.handleConnectionClosed();
        },
        onUndeliverable: (message) => this.processor.requeue(message),
        mayStillGo: (message) => this.processor.mayStillGo(message),
        maxPendingEncrypted: o.performance.maxPendingEncrypted,
        maxPendingEncryptedBytes: o.performance.maxPendingEncryptedBytes,
        // The encryptions caught up: what waited in the queue goes on.
        onSendDrain: () => this.processor.flushSoon(),
        heartbeatInterval: o.diagnostics.healthCheckInterval,
        compression: {
          enabled: this.deps.compression?.() ?? o.performance.compressionThreshold > 0,
          threshold: o.performance.compressionThreshold,
        },
        wireFormat: o.wireFormat,
        webSocket: o.webSocket,
        context: this.ctx,
      },
    );
    this.connection = connection;
    this.processor.setConnectionManager(connection);
    if (this.deps.windowSnapshots) this.installWindowListeners();

    try {
      await connection.connect(this.auth);
    } catch (error) {
      if (!this.destroyed) {
        logger.warn('Initial connection attempt failed; reconnecting with backoff:', error instanceof Error ? error.message : error);
      }
    }
  }

  /**
   * The server took this connection for another user than the one whose
   * data the page holds: nothing of that user's goes out on it, neither its
   * queued messages nor its subscriptions or recovery positions. The
   * consumers' own topics are subscribed again, for the new user.
   */
  private forgetPreviousUser(previous: string, next: string): void {
    logger.warn(`The server took this connection for ${next}, not ${previous}: ${previous}'s queue, topics and positions are dropped`);
    this.ctx.owner.next();
    this.ctx.queue.getState().clearQueue();
    const store = this.ctx.state.getState();
    this.processor.forgetTopics(store.activeTopics);
    this.sequencer.reset();
    store.setActiveTopics([...this.consumerTopics]);
    this.topicsResolved = this.topicsExplicit || this.consumerTopics.size > 0;
  }

  /** server_ready: (re)subscribe every active topic, asking for a replay where possible. */
  private handleServerReady(details: any): void {
    if (this.destroyed) return;

    const userId = typeof details?.user_id === 'string' && details.user_id ? details.user_id : null;
    const dataOwner = this.deps.dataOwner?.() ?? null;
    if (dataOwner && dataOwner.auth === this.auth) {
      if (dataOwner.userId && userId && userId !== dataOwner.userId) {
        // Another user on the same auth value (a cookie that changed): before
        // anything is flushed or resubscribed, the previous user's queue,
        // topics and positions go.
        this.forgetPreviousUser(dataOwner.userId, userId);
      }
      if (userId) dataOwner.userId = userId;
    }
    this.userId = userId;

    const store = this.ctx.state.getState();
    if (!this.topicsResolved) {
      this.topicsResolved = true;
      if (store.activeTopics.length === 0) {
        store.setActiveTopics(loadTopics(this.ctx.storage, userId) ?? [...DEFAULT_TOPICS]);
      }
    }

    const topics = this.ctx.state.getState().activeTopics;
    saveTopics(this.ctx.storage, topics, userId);
    if (topics.length === 0) return;

    const payload: Record<string, any> = { action: 'subscribe', topics };
    // The anchor is the last DELIVERED position of each topic, or what the
    // application committed, in the recover-or-resync mode.
    const positions = this.recoveryPositions(topics);
    if (Object.keys(positions).length > 0) {
      payload.recover = true;
      payload.recovery = positions;
      this.processor.openRecoveryWindows(positions);
      logger.info('Requesting recovery for topics:', Object.keys(positions));
    }

    this.sendSystem('subscription_update', payload);
    this.requestSnapshot(topics);
  }

  /**
   * The page went into the back/forward cache or away: the socket goes, the
   * session stays; the outbox's store goes with the page (another page may
   * take what waits).
   */
  suspendForPage(): void {
    if (this.destroyed) return;
    this.connection?.suspend();
    this.ctx.outbox.current?.suspend();
  }

  /**
   * The page is back: the outbox reads its store again (another page may
   * have taken what waited), then the session connects again and recovers
   * what was missed.
   */
  resumeForPage(): void {
    if (this.destroyed || !this.connection?.isSuspended()) return;
    const outbox = this.ctx.outbox.current;
    outbox?.resume();
    void (outbox?.opened ?? Promise.resolve()).then(() => {
      if (!this.destroyed && this.connection?.isSuspended()) this.connection.forceReconnect();
    });
  }

  /**
   * Where the recovery of `topics` starts: what the application committed,
   * delivery set back to it, in the recover-or-resync mode; else where
   * delivery is.
   */
  private recoveryPositions(topics: string[]): Record<string, TopicPosition> {
    const committed = this.ctx.delivery?.recoveryPositions(topics) ?? null;
    if (!committed) return this.processor.getTopicPositions(topics);
    this.processor.rewindPositions(topics, committed);
    return committed;
  }

  private sendSystem(type: string, payload: any, priority: MessagePriority = MessagePriority.HIGH): SendStatus {
    if (this.destroyed) return 'dropped';
    return this.processor.enqueueOutgoing(buildMessage(type, payload, { priority }), priority, { system: true });
  }

  // ---------------------------------------------------------------------------
  // Actions
  // ---------------------------------------------------------------------------

  subscribe(topics: string[], options?: { recover?: boolean }): void {
    const unique = uniqueTopics(topics);
    if (unique.length === 0 || this.destroyed) return;

    const store = this.ctx.state.getState();
    store.addTopics(unique);
    this.topicsResolved = true;
    saveTopics(this.ctx.storage, this.ctx.state.getState().activeTopics, this.userId);

    // Before server_ready the resubscribe at server_ready covers these topics.
    if (!this.connection?.isReady()) return;

    const payload: Record<string, any> = { action: 'subscribe', topics: unique };
    // In the recover-or-resync mode a topic recovers from what the
    // application committed of it, if anything.
    const positions =
      this.ctx.delivery?.recoveryPositions(unique) || options?.recover ? this.recoveryPositions(unique) : {};
    if (Object.keys(positions).length > 0) {
      payload.recover = true;
      payload.recovery = positions;
      this.processor.openRecoveryWindows(positions);
    }
    this.sendSystem('subscription_update', payload);
  }

  unsubscribe(topics: string[]): void {
    const unique = uniqueTopics(topics);
    if (unique.length === 0 || this.destroyed) return;

    this.ctx.state.getState().removeTopics(unique);
    this.processor.forgetTopics(unique);
    saveTopics(this.ctx.storage, this.ctx.state.getState().activeTopics, this.userId);

    if (this.connection?.isReady()) {
      this.sendSystem('subscription_update', { action: 'unsubscribe', topics: unique });
    }
  }

  sendMessage(type: string, payload: any, options: MessageOptions = {}): SendStatus {
    if (this.destroyed) return 'dropped';
    if (options.encrypted && !this.options.security.encryptionEnabled) {
      logger.error(
        `Refusing to send ${type}: it requires encryption but security.encryptionEnabled is off`
      );
      const store = this.ctx.state.getState();
      store.updateMetrics({ messagesDropped: store.metrics.messagesDropped + 1 });
      return 'dropped';
    }
    const priority = options.priority ?? MessagePriority.NORMAL;
    if (options.live) {
      const generation = this.connection?.socketGeneration();
      if (!this.connection?.isTransportReady() || generation === undefined) {
        logger.warn(`${type} not sent: no live connection`);
        const store = this.ctx.state.getState();
        store.updateMetrics({ messagesDropped: store.metrics.messagesDropped + 1 });
        return 'dropped';
      }
      return this.processor.enqueueOutgoing(buildMessage(type, payload, options), priority, {
        ttl: options.ttl,
        liveOn: generation,
        requires: options.requires,
      });
    }
    return this.processor.enqueueOutgoing(buildMessage(type, payload, options), priority, {
      offline: !!options.offline && this.options.offline.enabled,
      ttl: options.ttl,
      requires: options.requires,
    });
  }

  /** See UseWSEReturn.sendSigned. */
  async sendSigned(type: string, payload: any, options: SignedMessageOptions = {}): Promise<SendStatus> {
    if (this.destroyed) return 'dropped';
    const keyPair = this.options.security.signingKey;
    if (!keyPair) {
      logger.error(`Refusing to sign ${type}: no security.signingKey`);
      return 'dropped';
    }
    // The server takes 1 to 128 characters (code points, as it counts them).
    const nonceLength = options.nonce === undefined ? 1 : [...options.nonce].length;
    if (nonceLength < 1 || nonceLength > MAX_NONCE_LENGTH) {
      logger.error(`Refusing to sign ${type}: a nonce is 1 to ${MAX_NONCE_LENGTH} characters`);
      return 'dropped';
    }
    // Signed for this server and valid for its signature window: it goes out
    // on the connection it was made for or not at all, never after a
    // reconnect (not from a queue, nor when the connection changes while it
    // is being signed).
    const generation = this.connection?.socketGeneration();
    const audience = await this.processor.signingAudience();
    if (!audience) {
      logger.error(`Refusing to sign ${type}: the server has not named a signing key it may sign for`);
      return 'dropped';
    }
    this.signingKey ??= signingKeyFrom(keyPair);
    let envelope;
    try {
      envelope = await signMessage(await this.signingKey, type, payload, audience, options.nonce ?? generateId());
    } catch (error) {
      logger.error(`Could not sign ${type}:`, error);
      return 'dropped';
    }
    if (this.destroyed) return 'dropped';
    if (
      !this.connection?.isTransportReady() ||
      generation === undefined ||
      this.connection.socketGeneration() !== generation
    ) {
      logger.warn(`Signed ${type} not sent: no live connection`);
      return 'dropped';
    }
    const message: WSMessage = { ...envelope, p: undefined, id: generateId() };
    return this.processor.enqueueOutgoing(message, options.priority ?? MessagePriority.NORMAL, {
      ttl: SIGNED_MESSAGE_TTL_MS,
      liveOn: generation,
    });
  }

  forceReconnect(): void {
    if (this.destroyed) return;
    if (!this.connection) {
      logger.warn('forceReconnect: the connection is still initializing');
      return;
    }
    this.quality?.expectReconnect();
    this.connection.forceReconnect();
  }

  changeEndpoint(endpoint: string): void {
    if (this.destroyed || !this.connection) return;
    this.quality?.expectReconnect();
    this.connection.changeEndpoint(endpoint);
  }

  /**
   * Ask the server to compress (or not) what it sends this client. The next
   * client_hello carries it, and a connected server is told at once.
   */
  setCompression(enabled: boolean): void {
    if (this.destroyed || !this.connection) return;
    this.connection.setCompression(enabled);
    if (this.connection.isReady()) {
      const { enabled: on, threshold } = this.connection.getCompression();
      this.sendSystem('client_config', { compression: on, compression_threshold: threshold });
    }
  }

  requestSnapshot(topics?: string[]): void {
    const list = topics ? uniqueTopics(topics) : this.ctx.state.getState().activeTopics;
    if (list.length === 0) return;
    this.sendSystem('sync_request', {
      topics: list,
      include_snapshots: true,
      include_history: false,
      last_sequence: 0,
    });
  }

  /** What waits in the outbox goes, not delivered (`cleared`). */
  async clearOfflineQueue(): Promise<void> {
    const outbox = this.ctx.outbox.current;
    if (outbox) reportOutcomes(this.ctx.emit, outbox.clear());
  }

  /** How many messages wait in the outbox for the server's answer. */
  kept(): number {
    return this.ctx.outbox.current?.size ?? 0;
  }

  getQueueSize(): number {
    return this.ctx.queue.getState().size + (this.ctx.outbox.current?.size ?? 0);
  }

  /** The network or the machine is back: see ConnectionManager.wake. */
  wake(): void {
    if (!this.destroyed) this.connection?.wake();
  }

  /**
   * Nothing waits to go out: the queue is empty, the server answered for
   * what the outbox held, and the socket wrote what it took.
   */
  drained(): boolean {
    const unanswered = this.ctx.outbox.current?.size ?? 0;
    return (
      this.ctx.queue.getState().size + unanswered === 0 &&
      (this.connection?.unsentBytes() ?? 0) === 0
    );
  }

  private installWindowListeners(): void {
    if (typeof window === 'undefined' || typeof window.addEventListener !== 'function') return;
    this.windowListener = (event: Event) => {
      const detail = (event as CustomEvent).detail || {};
      this.requestSnapshot(Array.isArray(detail.topics) ? detail.topics : undefined);
    };
    window.addEventListener('wseRequestSnapshot', this.windowListener);
  }

  destroy(): void {
    if (this.destroyed) return;
    this.destroyed = true;

    if (this.windowListener && typeof window !== 'undefined') {
      window.removeEventListener('wseRequestSnapshot', this.windowListener);
      this.windowListener = null;
    }

    this.stopWatchingState?.();
    this.stopWatchingState = null;
    try {
      // Gives the app its own query defaults back.
      this.quality?.destroy();
    } catch (error) {
      logger.error('Error destroying the adaptive quality manager:', error);
    }
    this.quality = null;

    try {
      this.connection?.destroy();
    } catch (error) {
      logger.error('Error destroying connection manager:', error);
    }
    this.connection = null;

    try {
      this.processor.destroy();
    } catch (error) {
      logger.error('Error destroying message processor:', error);
    }

    try {
      this.networkMonitor?.destroy();
    } catch (error) {
      logger.error('Error destroying network monitor:', error);
    }
    this.networkMonitor = null;

    try {
      this.ctx.security.destroy();
    } catch (error) {
      logger.error('Error destroying security manager:', error);
    }

    this.ctx.state.getState().setConnectionState(ConnectionState.DISCONNECTED);
  }
}
