// =============================================================================
// WebSocket Engine - The page's session (useWSE)
// =============================================================================
//
// useWSE() consumers share one session per page, of the default client (the
// page's stores, window events and `wse_` storage):
//
//   - acquireSession() / releaseSession() reference-count the consumers; the
//     session is destroyed one tick after the last consumer leaves (the tick
//     lets React StrictMode's unmount/remount keep the same session).
//   - The session key is the auth value plus the endpoints, so a consumer with
//     other endpoints gets a session for those endpoints.
//   - Logout (auth becomes empty) and a change of user clear per-user data:
//     stores, outgoing queues, recovery positions, saved subscriptions, the
//     IndexedDB offline queue and the opt-in error log.

import { nextOwnerEpoch } from './ownership.js';
import { MessagePriority, type MessageOptions, type SendStatus, type SignedMessageOptions } from '../types.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import { useMessageQueueStore } from '../stores/useMessageQueueStore.js';
import { EventSequencer } from './EventSequencer.js';
import { logger } from '../utils/logger.js';
import { deleteDatabase } from '../core/context.js';
import { LEGACY_OFFLINE_DATABASE, legacyContext } from '../legacy/context.js';
import {
  WSESession,
  buildMessage,
  expiryOf,
  uniqueTopics,
  type DataOwner,
  type SessionParams,
} from './Session.js';

export {
  WSESession,
  DEFAULT_RECONNECTION,
  resolveOptions,
  type HandlerRegistry,
  type ResolvedWSEOptions,
  type SessionParams,
  type UseWSEConfig,
} from './Session.js';

// =============================================================================
// Session manager (one session per page)
// =============================================================================

// What setCompression() asked for; it outlives sessions (null: the option decides).
let compressionOverride: boolean | null = null;

let currentSession: WSESession | null = null;
let pendingDestroy: ReturnType<typeof setTimeout> | null = null;
let userDataPresent = false;
/** Whose data the page holds; see DataOwner. */
let dataOwner: DataOwner | null = null;
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
  deleteDatabase(LEGACY_OFFLINE_DATABASE);
}

function clearUserData(): void {
  userDataPresent = false;
  dataOwner = null;
  nextOwnerEpoch();
  sharedSequencer.reset();
  useWSEStore.getState().reset();
  useMessageQueueStore.getState().clearQueue();
  legacyContext().storage.clear();
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

  // The data the page still holds from an earlier session (it ended without
  // a sign-out) belongs to another user when the auth value differs.
  if (userDataPresent && dataOwner && dataOwner.auth !== params.auth) {
    clearUserData();
  }
  if (!dataOwner || dataOwner.auth !== params.auth) {
    dataOwner = { auth: params.auth, userId: null };
  }

  const session = new WSESession(params, {
    context: legacyContext(),
    sequencer: sharedSequencer,
    dataOwner: () => dataOwner,
    compression: () => compressionOverride,
    windowSnapshots: true,
  });
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

  // No session yet: keep the message; the next session flushes it. A live
  // one is for the connection of now only, and there is none.
  if (options.live) {
    logger.warn(`${type} not sent: no live connection`);
    return 'dropped';
  }
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

function sendSignedAction(type: string, payload: any, options?: SignedMessageOptions): Promise<SendStatus> {
  const session = getCurrentSession();
  if (!session) {
    logger.warn(`sendSigned(${type}): no active WSE connection`);
    return Promise.resolve('dropped');
  }
  return session.sendSigned(type, payload, options);
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
      // The outbox: what waits for the server's answer, and where it is kept.
      offline: session
        ? {
            size: session.kept(),
            capacity: session.options.offline.maxSize,
            enabled: session.options.offline.enabled,
            persistToStorage: store.storageState === 'durable',
          }
        : null,
    },
    activeTopics: store.activeTopics,
    recoveryState: store.subscriptions.recoveryState,
    errorHistory: store.errorHistory,
    config: session
      ? { ...session.options, refreshAuthToken: undefined, registerHandlers: undefined, queryClient: undefined, webSocket: undefined }
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
  sendSigned: sendSignedAction,
  downloadDiagnostics: downloadDiagnosticsAction,
  clearOfflineQueue: clearOfflineQueueAction,
  getQueueSize: getQueueSizeAction,
  requestSnapshot: requestSnapshotAction,
  debugHandlers: debugHandlersAction,
  setCompression: setCompressionAction,
});

// The page's lifecycle -- HMR-safe (replaces the previous listeners):
// pagehide closes the socket and keeps the session (a page in the
// back/forward cache holds no socket; beforeunload would keep some browsers
// from caching it), pageshow connects again and recovers what was missed.
if (typeof window !== 'undefined' && typeof window.addEventListener === 'function') {
  const g = globalThis as any;
  if (g.__wse_beforeunload_handler) {
    window.removeEventListener('beforeunload', g.__wse_beforeunload_handler);
    g.__wse_beforeunload_handler = undefined;
  }
  if (g.__wse_page_handlers) {
    window.removeEventListener('pagehide', g.__wse_page_handlers.hide);
    window.removeEventListener('pageshow', g.__wse_page_handlers.show);
  }
  const hide = () => currentSession?.suspendForPage();
  const show = () => currentSession?.resumeForPage();
  g.__wse_page_handlers = { hide, show };
  window.addEventListener('pagehide', hide);
  window.addEventListener('pageshow', show);
}
