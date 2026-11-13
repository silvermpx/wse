// =============================================================================
// WebSocket Engine - Session (one connection and its services)
// =============================================================================
//
// A session owns everything one connection needs: MessageProcessor,
// ConnectionManager, NetworkMonitor and the IndexedDB offline queue. useWSE()
// consumers share one session per page:
//
//   - acquireSession() / releaseSession() reference-count the consumers; the
//     session is destroyed one tick after the last consumer leaves (the tick
//     lets React StrictMode's unmount/remount keep the same session).
//   - The session key is the auth value plus the endpoints, so a consumer with
//     other endpoints gets a session for those endpoints.
//   - Initialization checks `destroyed` after every await: a token change or
//     logout during init never leaves an untracked socket behind.
//   - Logout (auth becomes empty) and a change of user clear per-user data:
//     stores, outgoing queues, recovery positions, saved subscriptions, the
//     IndexedDB offline queue and the opt-in error log.

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
  WSMessage,
} from '../types.js';
import {
  DEFAULT_MAX_INBOUND_MESSAGE_SIZE,
  DEFAULT_RECOVERY_TIMEOUT_MS,
  DEFAULT_TOPICS,
  HEARTBEAT_INTERVAL,
  RECONNECT_MAX_ATTEMPTS,
  SERVER_READY_TIMEOUT,
  WS_PROTOCOL_VERSION,
  clearWSEStorage,
  getEndpoints,
  loadSavedSubscriptions,
  saveSubscriptions,
} from '../constants.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import { useMessageQueueStore } from '../stores/useMessageQueueStore.js';
import { ConnectionManager } from './ConnectionManager.js';
import { MessageProcessor } from './MessageProcessor.js';
import { NetworkMonitor } from './NetworkMonitor.js';
import { AdaptiveQualityManager, QueryClientLike } from './AdaptiveQualityManager.js';
import { OfflineQueue } from './OfflineQueue.js';
import { EventSequencer } from './EventSequencer.js';
import { EventHandlers } from '../handlers/EventHandlers.js';
import { securityManager } from '../utils/security.js';
import { logger } from '../utils/logger.js';
import { generateId } from '../utils/id.js';
import { dispatchWSEEvent } from '../utils/events.js';

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
      ...config?.performance,
    },
    offline: {
      enabled: true,
      maxSize: 1000,
      maxAge: 3600000,
      persistToStorage: true,
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
  };
}

function uniqueTopics(topics: unknown): string[] {
  if (!Array.isArray(topics)) return [];
  return [...new Set(topics.filter((t): t is string => typeof t === 'string' && t.length > 0))];
}

function buildMessage(type: string, payload: any, options: MessageOptions = {}): WSMessage {
  const message: WSMessage = {
    t: type,
    p: payload,
    id: generateId(),
    ts: new Date().toISOString(),
    v: WS_PROTOCOL_VERSION,
  };
  if (options.priority) message.pri = options.priority;
  if (options.correlation_id) message.cid = options.correlation_id;
  if (options.encrypted) message.enc = true;
  return message;
}

/** Epoch ms after which a message with `ttl` is no longer worth sending. */
function expiryOf(options: MessageOptions): number | undefined {
  const ttl = options.ttl;
  return ttl !== undefined && Number.isFinite(ttl) && ttl >= 0 ? Date.now() + ttl : undefined;
}

// What setCompression() asked for; it outlives sessions (null: the option decides).
let compressionOverride: boolean | null = null;

export interface SessionParams {
  key: string;
  auth: string;
  initialTopics?: string[];
  config?: UseWSEConfig;
}

export class WSESession {
  readonly key: string;
  readonly auth: string;
  readonly options: ResolvedWSEOptions;
  readonly processor: MessageProcessor;
  connection: ConnectionManager | null = null;
  networkMonitor: NetworkMonitor | null = null;
  quality: AdaptiveQualityManager | null = null;
  offlineQueue: OfflineQueue | null = null;
  refCount = 0;
  destroyed = false;

  private userId: string | null = null;
  private topicsResolved: boolean;
  private startPromise: Promise<void> | null = null;
  private windowListener: ((event: Event) => void) | null = null;
  private stopWatchingState: (() => void) | null = null;

  constructor(params: SessionParams, sequencer: EventSequencer) {
    this.key = params.key;
    this.auth = params.auth;
    this.options = resolveOptions(params.config);

    this.processor = new MessageProcessor(
      this.options.performance.batchSize,
      this.options.performance.batchTimeout,
      {
        maxMessageSize: this.options.maxMessageSize,
        recoveryTimeoutMs: this.options.recoveryTimeoutMs,
        eventThrottleMs: this.options.eventThrottleMs,
        sequencer,
        handlerSampleRate: this.options.diagnostics.enabled ? this.options.diagnostics.sampleRate : 0,
      },
    );

    EventHandlers.registerAll(this.processor);
    this.options.registerHandlers?.(this.processor);

    // Topics the consumer asked for are the base; without them the first
    // server_ready restores this user's saved subscriptions (or defaults).
    const initial = params.initialTopics !== undefined ? uniqueTopics(params.initialTopics) : null;
    if (initial !== null) {
      useWSEStore.getState().addTopics(initial);
    }
    this.topicsResolved = initial !== null || useWSEStore.getState().activeTopics.length > 0;
  }

  /** A further useWSE() consumer joined this session. */
  attachConsumer(params: SessionParams): void {
    if (this.destroyed) return;
    params.config?.registerHandlers?.(this.processor);
    const topics = params.initialTopics !== undefined ? uniqueTopics(params.initialTopics) : [];
    if (topics.length > 0) {
      const active = new Set(useWSEStore.getState().activeTopics);
      const missing = topics.filter(t => !active.has(t));
      if (missing.length > 0) this.subscribe(missing);
    }
  }

  start(): Promise<void> {
    if (!this.startPromise) {
      this.startPromise = this.run().catch((error) => {
        if (this.destroyed) return;
        logger.error('WSE initialization failed:', error);
        const store = useWSEStore.getState();
        store.setLastError(error instanceof Error ? error.message : 'Initialization failed');
        store.setConnectionState(ConnectionState.ERROR);
      });
    }
    return this.startPromise;
  }

  private async run(): Promise<void> {
    const o = this.options;
    useWSEStore.getState().updateConfig({ maxReconnectAttempts: o.reconnection.maxAttempts });
    useMessageQueueStore.getState().setCapacity(o.performance.maxQueueSize);
    useMessageQueueStore.getState().setMemoryLimit(o.performance.memoryLimit);

    if (o.criticalHandlers.length > 0) {
      const missing = o.criticalHandlers.filter(h => !this.processor.isHandlerRegistered(h));
      if (missing.length > 0) {
        throw new Error(`Failed to register critical handlers: ${missing.join(', ')}`);
      }
    }
    this.processor.setReady(true);

    if (o.security.encryptionEnabled) {
      try {
        await securityManager.initialize({ encryptionEnabled: true });
      } catch (error) {
        if (this.destroyed) return;
        // Fail closed: no Web Crypto (e.g. a plain-http page) means no
        // encryption, and encryption was required -- do not connect.
        const reason = `encryption unavailable (${error instanceof Error ? error.message : String(error)})`;
        logger.error(`WSE: ${reason}; not connecting`);
        const store = useWSEStore.getState();
        store.setLastError(`Encryption failed: ${reason}`);
        store.setConnectionState(ConnectionState.ERROR);
        dispatchWSEEvent('wseEncryptionFailed', { reason });
        return;
      }
    }
    if (this.destroyed) return;

    if (o.offline.enabled && o.offline.persistToStorage) {
      const queue = new OfflineQueue(o.offline);
      try {
        await queue.initialize();
      } catch (error) {
        logger.warn('Offline queue unavailable:', error);
      }
      if (this.destroyed) {
        queue.destroy();
        return;
      }
      this.offlineQueue = queue;
      this.processor.setOfflineQueue(queue);
    }

    this.networkMonitor = new NetworkMonitor(100, o.diagnostics.metricsInterval, o.diagnostics.enabled);
    // Quality tracking runs when something consumes it: a QueryClient to
    // steer, or diagnostics (the connectionQualityChanged event).
    if (o.queryClient || o.diagnostics.enabled) {
      const quality = new AdaptiveQualityManager({ queryClient: o.queryClient ?? null });
      this.quality = quality;
      this.stopWatchingState = useWSEStore.subscribe((state, previous) => {
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
        },
        rateLimit: o.rateLimit,
        serverReadyTimeout: o.serverReadyTimeout,
        onTransportReady: () => {
          void this.processor.flush();
        },
        onSocketClosed: () => this.processor.handleConnectionClosed(),
        onUndeliverable: (message) => this.processor.requeue(message),
        heartbeatInterval: o.diagnostics.healthCheckInterval,
        compression: {
          enabled: compressionOverride ?? o.performance.compressionThreshold > 0,
          threshold: o.performance.compressionThreshold,
        },
        wireFormat: o.wireFormat,
      },
    );
    this.connection = connection;
    this.processor.setConnectionManager(connection);
    this.installWindowListeners();

    try {
      await connection.connect(this.auth);
    } catch (error) {
      if (!this.destroyed) {
        logger.warn('Initial connection attempt failed; reconnecting with backoff:', error instanceof Error ? error.message : error);
      }
    }
  }

  /** server_ready: (re)subscribe every active topic, asking for a replay where possible. */
  private handleServerReady(details: any): void {
    if (this.destroyed) return;

    const userId = typeof details?.user_id === 'string' && details.user_id ? details.user_id : null;
    this.userId = userId;

    const store = useWSEStore.getState();
    if (!this.topicsResolved) {
      this.topicsResolved = true;
      if (store.activeTopics.length === 0) {
        store.setActiveTopics(loadSavedSubscriptions(userId) ?? [...DEFAULT_TOPICS]);
      }
    }

    const topics = useWSEStore.getState().activeTopics;
    saveSubscriptions(topics, userId);
    if (topics.length === 0) return;

    const payload: Record<string, any> = { action: 'subscribe', topics };
    // The anchor is the last DELIVERED position of each topic.
    const positions = this.processor.getTopicPositions(topics);
    if (Object.keys(positions).length > 0) {
      payload.recover = true;
      payload.recovery = positions;
      this.processor.openRecoveryWindows(positions);
      logger.info('Requesting recovery for topics:', Object.keys(positions));
    }

    this.sendSystem('subscription_update', payload);
    this.requestSnapshot(topics);
  }

  private sendSystem(type: string, payload: any, priority: MessagePriority = MessagePriority.HIGH): SendStatus {
    if (this.destroyed) return 'dropped';
    return this.processor.enqueueOutgoing(buildMessage(type, payload, { priority }), priority);
  }

  // ---------------------------------------------------------------------------
  // Actions
  // ---------------------------------------------------------------------------

  subscribe(topics: string[], options?: { recover?: boolean }): void {
    const unique = uniqueTopics(topics);
    if (unique.length === 0 || this.destroyed) return;

    const store = useWSEStore.getState();
    store.addTopics(unique);
    this.topicsResolved = true;
    saveSubscriptions(useWSEStore.getState().activeTopics, this.userId);

    // Before server_ready the resubscribe at server_ready covers these topics.
    if (!this.connection?.isReady()) return;

    const payload: Record<string, any> = { action: 'subscribe', topics: unique };
    if (options?.recover) {
      const positions = this.processor.getTopicPositions(unique);
      if (Object.keys(positions).length > 0) {
        payload.recover = true;
        payload.recovery = positions;
        this.processor.openRecoveryWindows(positions);
      }
    }
    this.sendSystem('subscription_update', payload);
  }

  unsubscribe(topics: string[]): void {
    const unique = uniqueTopics(topics);
    if (unique.length === 0 || this.destroyed) return;

    useWSEStore.getState().removeTopics(unique);
    this.processor.forgetTopics(unique);
    saveSubscriptions(useWSEStore.getState().activeTopics, this.userId);

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
      const store = useWSEStore.getState();
      store.updateMetrics({ messagesDropped: store.metrics.messagesDropped + 1 });
      return 'dropped';
    }
    const priority = options.priority ?? MessagePriority.NORMAL;
    return this.processor.enqueueOutgoing(buildMessage(type, payload, options), priority, {
      offline: !!options.offline && this.options.offline.enabled,
      ttl: options.ttl,
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
    const list = topics ? uniqueTopics(topics) : useWSEStore.getState().activeTopics;
    if (list.length === 0) return;
    this.sendSystem('sync_request', {
      topics: list,
      include_snapshots: true,
      include_history: false,
      last_sequence: 0,
    });
  }

  async clearOfflineQueue(): Promise<void> {
    await this.offlineQueue?.clear();
  }

  getQueueSize(): number {
    return useMessageQueueStore.getState().size + (this.offlineQueue?.getStats().size ?? 0);
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
      this.offlineQueue?.destroy();
    } catch (error) {
      logger.error('Error destroying offline queue:', error);
    }
    this.offlineQueue = null;

    try {
      securityManager.destroy();
    } catch (error) {
      logger.error('Error destroying security manager:', error);
    }

    useWSEStore.getState().setConnectionState(ConnectionState.DISCONNECTED);
  }
}

// =============================================================================
// Session manager (one session per page)
// =============================================================================

const OFFLINE_DB_NAME = 'wse_offline_queue';

let currentSession: WSESession | null = null;
let pendingDestroy: ReturnType<typeof setTimeout> | null = null;
let userDataPresent = false;
// Dedup ids and per-topic recovery positions survive session re-creation for
// the same user (e.g. the last consumer unmounts and a new one mounts).
const sharedSequencer = new EventSequencer();

function cancelPendingDestroy(): void {
  if (pendingDestroy) {
    clearTimeout(pendingDestroy);
    pendingDestroy = null;
  }
}

function deleteOfflineDatabase(): void {
  if (typeof indexedDB === 'undefined') return;
  try {
    indexedDB.deleteDatabase(OFFLINE_DB_NAME);
  } catch {
    // ignore
  }
}

function clearUserData(): void {
  userDataPresent = false;
  sharedSequencer.reset();
  useWSEStore.getState().reset();
  useMessageQueueStore.getState().clearQueue();
  clearWSEStorage();
  deleteOfflineDatabase();
}

function destroySession(session: WSESession, clear: boolean): void {
  session.destroy();
  if (currentSession === session) currentSession = null;
  if (clear) clearUserData();
}

/** Build the session key: auth value + endpoints. */
export function makeSessionKey(auth: string, endpoints?: string[]): string {
  return `${auth}\u0000${endpoints && endpoints.length > 0 ? endpoints.join('\n') : ''}`;
}

/**
 * Join (or create) the session for `params.key`. A different key replaces the
 * current session; when the auth value differs too, per-user data is cleared.
 */
export function acquireSession(params: SessionParams): WSESession {
  cancelPendingDestroy();

  if (currentSession && !currentSession.destroyed && currentSession.key === params.key) {
    currentSession.refCount++;
    currentSession.attachConsumer(params);
    return currentSession;
  }

  if (currentSession && !currentSession.destroyed) {
    if (currentSession.refCount > 0) {
      logger.warn(
        'useWSE: another auth/endpoint configuration replaced the active connection ' +
        '(one WSE connection per page)'
      );
    }
    destroySession(currentSession, currentSession.auth !== params.auth);
  }

  const session = new WSESession(params, sharedSequencer);
  session.refCount = 1;
  currentSession = session;
  userDataPresent = true;
  void session.start();
  return session;
}

/** A consumer left; the session ends one tick after the last one leaves. */
export function releaseSession(session: WSESession): void {
  if (session.refCount > 0) session.refCount--;
  if (session !== currentSession || session.refCount > 0 || session.destroyed) return;

  cancelPendingDestroy();
  pendingDestroy = setTimeout(() => {
    pendingDestroy = null;
    if (currentSession === session && session.refCount === 0) {
      destroySession(session, false);
    }
  }, 0);
}

/**
 * The user signed out (a consumer rendered without auth): end the session and
 * clear per-user data. Ignored while another consumer still holds a session.
 */
export function signOut(): void {
  if (currentSession && !currentSession.destroyed) {
    if (currentSession.refCount > 0) {
      logger.debug('useWSE: a consumer without auth while another still holds the connection');
      return;
    }
    cancelPendingDestroy();
    destroySession(currentSession, true);
    return;
  }
  if (userDataPresent) {
    clearUserData();
  }
}

export function getCurrentSession(): WSESession | null {
  return currentSession && !currentSession.destroyed ? currentSession : null;
}

/** Tear the current session down now (page unload, debugging). */
export function destroyCurrentSession(options: { clearUserData?: boolean } = {}): void {
  cancelPendingDestroy();
  if (currentSession) {
    destroySession(currentSession, !!options.clearUserData);
  } else if (options.clearUserData) {
    clearUserData();
  }
}

export function getSharedSequencer(): EventSequencer {
  return sharedSequencer;
}

// =============================================================================
// Stable actions (identity never changes; always act on live state)
// =============================================================================

function subscribeAction(topics: string[], options?: { recover?: boolean }): void {
  const session = getCurrentSession();
  if (session) {
    session.subscribe(topics, options);
    return;
  }
  // No connection yet: remember the topics; the next server_ready subscribes them.
  useWSEStore.getState().addTopics(uniqueTopics(topics));
}

function unsubscribeAction(topics: string[]): void {
  const session = getCurrentSession();
  if (session) {
    session.unsubscribe(topics);
    return;
  }
  const unique = uniqueTopics(topics);
  useWSEStore.getState().removeTopics(unique);
  useWSEStore.getState().removeRecoveryTopics(unique);
  sharedSequencer.clearTopics(unique);
}

function sendMessageAction(type: string, payload: any, options: MessageOptions = {}): SendStatus {
  const session = getCurrentSession();
  if (session) return session.sendMessage(type, payload, options);

  // No session yet: keep the message; the next session flushes it.
  if (options.encrypted) {
    logger.error(`Refusing to queue ${type}: it requires encryption and no encrypted session exists`);
    return 'dropped';
  }
  const message = buildMessage(type, payload, options);
  const queueStore = useMessageQueueStore.getState();
  const ok = queueStore.enqueue({
    id: message.id!,
    type,
    payload,
    priority: options.priority ?? MessagePriority.NORMAL,
    timestamp: Date.now(),
    retries: 0,
    correlation_id: message.cid,
    message,
    bytes: queueStore.memoryLimit > 0 ? JSON.stringify(message).length : undefined,
    expiresAt: expiryOf(options),
  });
  return ok ? 'queued' : 'dropped';
}

function sendBatchAction(messages: Array<{ type: string; payload: any; options?: MessageOptions }>): SendStatus[] {
  return messages.map(({ type, payload, options }) => sendMessageAction(type, payload, options));
}

function forceReconnectAction(): void {
  const session = getCurrentSession();
  if (!session) {
    logger.warn('forceReconnect: no active WSE connection');
    return;
  }
  // Clears the store's circuit-breaker mirror too, so the UI does not show a stale OPEN.
  useWSEStore.getState().resetCircuitBreaker();
  session.forceReconnect();
}

function changeEndpointAction(endpoint: string): void {
  const session = getCurrentSession();
  if (!session) {
    logger.warn('changeEndpoint: no active WSE connection');
    return;
  }
  session.changeEndpoint(endpoint);
}

function requestSnapshotAction(topics?: string[]): void {
  getCurrentSession()?.requestSnapshot(topics);
}

async function clearOfflineQueueAction(): Promise<void> {
  const session = getCurrentSession();
  if (session) {
    await session.clearOfflineQueue();
  } else {
    deleteOfflineDatabase();
  }
}

function setCompressionAction(enabled: boolean): void {
  compressionOverride = !!enabled;
  useWSEStore.getState().updateConfig({ compressionEnabled: !!enabled });
  getCurrentSession()?.setCompression(!!enabled);
}

function getQueueSizeAction(): number {
  const session = getCurrentSession();
  return session ? session.getQueueSize() : useMessageQueueStore.getState().size;
}

export function collectDiagnostics(): Record<string, unknown> {
  const session = getCurrentSession();
  const store = useWSEStore.getState();
  const connection = session?.connection ?? null;
  return {
    timestamp: new Date().toISOString(),
    connection: {
      state: store.connectionState,
      activeEndpoint: store.activeEndpoint,
      connectionId: connection?.getConnectionId() ?? null,
      isReady: connection?.isReady() ?? false,
      transportReady: connection?.isTransportReady() ?? false,
      rateLimiter: connection?.getRateLimiterStats() ?? null,
      connectionPool: connection?.getConnectionPoolStats() ?? null,
      healthScores: connection?.getHealthScores() ?? null,
    },
    metrics: store.metrics,
    diagnostics: store.diagnostics,
    circuitBreaker: store.circuitBreaker,
    queue: {
      message: useMessageQueueStore.getState().stats,
      offline: session?.offlineQueue?.getStats() ?? null,
    },
    activeTopics: store.activeTopics,
    recoveryState: store.subscriptions.recoveryState,
    errorHistory: store.errorHistory,
    config: session
      ? { ...session.options, refreshAuthToken: undefined, registerHandlers: undefined, queryClient: undefined }
      : null,
    quality: session?.quality
      ? { current: session.quality.getCurrentQuality(), strategy: session.quality.getCurrentStrategyName() }
      : null,
    network: {
      monitor: session?.networkMonitor?.getLatencyStats() ?? null,
      lastDiagnostics: session?.networkMonitor?.getLastDiagnostics() ?? null,
    },
    handlers: session?.processor.getRegisteredHandlers() ?? [],
  };
}

function downloadDiagnosticsAction(): void {
  if (typeof document === 'undefined' || typeof URL === 'undefined' || typeof Blob === 'undefined') {
    logger.warn('downloadDiagnostics needs a browser');
    return;
  }
  const blob = new Blob([JSON.stringify(collectDiagnostics(), null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `wse-diagnostics-${Date.now()}.json`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function debugHandlersAction(): void {
  const session = getCurrentSession();
  if (!session) {
    logger.warn('debugHandlers: no active WSE connection');
    return;
  }
  const handlers = session.processor.getRegisteredHandlers();
  const store = useWSEStore.getState();
  logger.info('Registered handlers:', handlers);
  for (const name of session.options.criticalHandlers) {
    if (!handlers.includes(name)) logger.error(`Critical handler ${name} is NOT registered`);
  }
  logger.info('Circuit breaker:', store.circuitBreaker);
  logger.info('Active topics:', store.activeTopics);
}

/** The functions returned by useWSE(): module-level, so their identity never changes. */
export const wseActions = Object.freeze({
  subscribe: subscribeAction,
  unsubscribe: unsubscribeAction,
  forceReconnect: forceReconnectAction,
  changeEndpoint: changeEndpointAction,
  sendMessage: sendMessageAction,
  sendBatch: sendBatchAction,
  downloadDiagnostics: downloadDiagnosticsAction,
  clearOfflineQueue: clearOfflineQueueAction,
  getQueueSize: getQueueSizeAction,
  requestSnapshot: requestSnapshotAction,
  debugHandlers: debugHandlersAction,
  setCompression: setCompressionAction,
});

// Close the socket cleanly on page unload -- HMR-safe (replaces the previous listener).
if (typeof window !== 'undefined' && typeof window.addEventListener === 'function') {
  const g = globalThis as any;
  if (g.__wse_beforeunload_handler) {
    window.removeEventListener('beforeunload', g.__wse_beforeunload_handler);
  }
  const onUnload = () => {
    if (currentSession) destroyCurrentSession();
  };
  g.__wse_beforeunload_handler = onUnload;
  window.addEventListener('beforeunload', onUnload);
}
