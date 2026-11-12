// =============================================================================
// WebSocket Engine - useWSE Hook
// =============================================================================

import { useCallback, useEffect, useMemo, useRef, useSyncExternalStore } from 'react';
import type { ConnectionMetrics, UseWSEReturn } from '../types.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import { useMessageQueueStore } from '../stores/useMessageQueueStore.js';
import { MessageProcessor } from '../services/MessageProcessor.js';
import {
  acquireSession,
  collectDiagnostics,
  destroyCurrentSession,
  getCurrentSession,
  makeSessionKey,
  releaseSession,
  signOut,
  wseActions,
} from '../services/WSESession.js';
import type { UseWSEConfig } from '../services/WSESession.js';
import { logger } from '../utils/logger.js';

export type { UseWSEConfig, HandlerRegistry } from '../services/WSESession.js';

/** `stats` is refreshed at most this often (ms), however fast messages arrive. */
export const STATS_REFRESH_MS = 1000;

const selectConnectionState = (s: ReturnType<typeof useWSEStore.getState>) => s.connectionState;
const selectActiveTopics = (s: ReturnType<typeof useWSEStore.getState>) => s.activeTopics;
const selectDiagnostics = (s: ReturnType<typeof useWSEStore.getState>) => s.diagnostics;

const JWT_SHAPE = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*$/;
let warnedAboutJwt = false;

/**
 * Connection metrics, re-rendering at most once per `intervalMs` (metrics
 * change with every message; subscribing to them directly re-rendered every
 * consumer per message).
 */
function useThrottledMetrics(intervalMs: number): ConnectionMetrics {
  const snapshot = useRef<ConnectionMetrics>(useWSEStore.getState().metrics);

  const subscribe = useCallback((notify: () => void) => {
    let timer: ReturnType<typeof setTimeout> | null = null;
    let lastPublished = 0;

    const publish = () => {
      timer = null;
      lastPublished = Date.now();
      const current = useWSEStore.getState().metrics;
      if (current !== snapshot.current) {
        snapshot.current = current;
        notify();
      }
    };

    const unsubscribe = useWSEStore.subscribe((state, prev) => {
      if (state.metrics === prev.metrics || timer) return;
      const wait = intervalMs - (Date.now() - lastPublished);
      if (wait <= 0) publish();
      else timer = setTimeout(publish, wait);
    });

    // Pick up changes made between render and subscription.
    if (useWSEStore.getState().metrics !== snapshot.current) {
      timer = setTimeout(publish, 0);
    }

    return () => {
      unsubscribe();
      if (timer) clearTimeout(timer);
    };
  }, [intervalMs]);

  const getSnapshot = useCallback(() => snapshot.current, []);
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
}

/**
 * React hook for a WSE connection.
 *
 * @param token Auth mode -- NOT a credential. Browsers cannot attach an
 *   Authorization header to a WebSocket, so the server authenticates the
 *   browser by its cookie (`jwt_cookie_name`, default `access_token`); the
 *   string is never sent. Pass `'cookie'` (recommended; also enables the
 *   periodic `refreshAuthToken`) once the user is signed in -- or for a
 *   server without auth -- and `undefined` when signed out: that closes the
 *   connection and clears per-user data (queues, recovery positions, saved
 *   subscriptions). Changing it from one non-empty value to another is
 *   treated as a new user.
 * @param initialTopics Topics to subscribe to. Without them the user's saved
 *   subscriptions (or DEFAULT_TOPICS) are restored on connect.
 * @param config Connection options; read when the connection is created.
 *
 * All returned functions are stable across renders. Every useWSE() call on a
 * page shares one connection per auth value and endpoint list; it closes one
 * tick after the last consumer unmounts.
 */
export function useWSE(
  token?: string,
  initialTopics?: string[],
  config?: UseWSEConfig
): UseWSEReturn {
  const auth = typeof token === 'string' && token.trim() !== '' ? token : undefined;
  const sessionKey = auth !== undefined ? makeSessionKey(auth, config?.endpoints) : null;

  // Latest arguments, read when the session is created -- inline config
  // objects and topic arrays (new identities every render) never reconnect.
  const latest = useRef({ auth, initialTopics, config });
  latest.current = { auth, initialTopics, config };

  useEffect(() => {
    if (sessionKey === null) {
      signOut();
      return;
    }

    const args = latest.current;
    if (!warnedAboutJwt && args.auth && args.auth !== 'cookie' && JWT_SHAPE.test(args.auth)) {
      warnedAboutJwt = true;
      logger.warn(
        'useWSE: the token argument looks like a JWT, but it is never sent -- browser WebSockets ' +
        'cannot carry an Authorization header. The server authenticates the browser by its cookie ' +
        "(jwt_cookie_name). Pass 'cookie' and set the JWT as that cookie."
      );
    }

    const session = acquireSession({
      key: sessionKey,
      auth: args.auth!,
      initialTopics: args.initialTopics,
      config: args.config,
    });
    return () => releaseSession(session);
  }, [sessionKey]);

  const connectionHealth = useWSEStore(selectConnectionState);
  const activeTopics = useWSEStore(selectActiveTopics);
  const diagnostics = useWSEStore(selectDiagnostics);
  const stats = useThrottledMetrics(STATS_REFRESH_MS);

  return useMemo<UseWSEReturn>(() => ({
    ...wseActions,
    stats,
    activeTopics,
    connectionHealth,
    diagnostics,
  }), [stats, activeTopics, connectionHealth, diagnostics]);
}

// Add debug utilities to window for development
if (typeof window !== 'undefined') {
  try {
    const isDev = typeof process !== 'undefined' && process.env?.NODE_ENV === 'development';
    if (isDev) {
      (window as any).WSE = {
        getStore: () => useWSEStore.getState(),
        getQueueStore: () => useMessageQueueStore.getState(),
        getSession: () => getCurrentSession(),
        getDiagnostics: () => collectDiagnostics(),
        resetCircuitBreaker: () => useWSEStore.getState().resetCircuitBreaker(),
        forceReconnect: () => wseActions.forceReconnect(),
        forceDisconnect: () => destroyCurrentSession(),
        enableDebug: () => MessageProcessor.enableDebugMode(true),
        disableDebug: () => MessageProcessor.enableDebugMode(false),
      };
    }
  } catch {
    // Ignore errors in environment detection
  }
}
