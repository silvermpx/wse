// =============================================================================
// WebSocket Event System - useWSE Hook
// =============================================================================

import { useEffect, useRef, useCallback, useMemo } from 'react';
import {
  UseWSEReturn,
  WSEConfig,
  MessageOptions,
  MessagePriority,
  ConnectionState,
  WSMessage,
  CircuitBreakerState,
} from '../types';
import {
  getEndpoints,
  getSavedSubscriptions,
  saveSubscriptions,
} from '../constants';
import { useWSEStore } from '../stores/useWSEStore';
import { useMessageQueueStore } from '../stores/useMessageQueueStore';
import { ConnectionManager, ConnectionManagerConfig } from '../services/ConnectionManager';
import { MessageProcessor } from '../services/MessageProcessor';
import { NetworkMonitor } from '../services/NetworkMonitor';
import { OfflineQueue } from '../services/OfflineQueue';
import { EventHandlers } from '../handlers/EventHandlers';
import { securityManager } from '../utils/security';
import { logger } from '../utils/logger';

// Default configuration matching backend expectations
const DEFAULT_CONFIG: Partial<WSEConfig> = {
  endpoints: [],
  reconnection: {
    mode: 'adaptive',
    baseDelay: 1000,
    maxDelay: 30000,
    maxAttempts: 10,
    factor: 1.5,
    jitter: true,
  },
  security: {
    encryptionEnabled: false,
    messageSignature: false,
  },
  performance: {
    batchSize: 10,
    batchTimeout: 500,
    compressionThreshold: 1024,
    maxQueueSize: 10000,
    memoryLimit: 50 * 1024 * 1024,
  },
  offline: {
    enabled: true,
    maxSize: 1000,
    maxAge: 3600000,
    persistToStorage: true,
  },
  diagnostics: {
    enabled: false,
    sampleRate: 0.1,
    metricsInterval: 60000,
    healthCheckInterval: 30000,
  },
};

// Global instance management with proper cleanup tracking
interface GlobalWSEInstance {
  token: string;
  connectionManager: ConnectionManager | null;
  messageProcessor: MessageProcessor | null;
  networkMonitor: NetworkMonitor | null;
  offlineQueue: OfflineQueue | null;
  initPromise: Promise<void> | null;
  cleanupFunction: (() => void) | null;
  destroyed: boolean;
}

let globalWSEInstance: GlobalWSEInstance | null = null;

// GLOBAL CONNECTION LOCK - prevents multiple simultaneous connections
let isGloballyConnecting = false;
let lastConnectionAttemptTime = 0;
const MIN_CONNECTION_INTERVAL = 2000;

// Circuit breaker check interval
let circuitBreakerCheckInterval: NodeJS.Timeout | null = null;

// Event deduplication at the module level
const processedEvents = new Map<string, number>();
const EVENT_CACHE_TIME = 60 * 1000;
const MAX_CACHED_EVENTS = 100;

// Cleanup old processed events
function cleanupProcessedEvents() {
  const now = Date.now();
  const entriesToDelete: string[] = [];

  for (const [eventId, timestamp] of processedEvents.entries()) {
    if (now - timestamp > EVENT_CACHE_TIME) {
      entriesToDelete.push(eventId);
    }
  }

  entriesToDelete.forEach(id => processedEvents.delete(id));

  if (processedEvents.size > MAX_CACHED_EVENTS) {
    const entries = Array.from(processedEvents.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, MAX_CACHED_EVENTS);

    processedEvents.clear();
    entries.forEach(([id, timestamp]) => processedEvents.set(id, timestamp));
  }
}

// Run cleanup every 30 seconds - handle HMR by clearing existing interval
if ((globalThis as any).__wse_cleanup_interval) {
  clearInterval((globalThis as any).__wse_cleanup_interval);
}
const cleanupInterval = setInterval(cleanupProcessedEvents, 30000);
(globalThis as any).__wse_cleanup_interval = cleanupInterval;

// Cleanup on module unload
if (typeof window !== 'undefined') {
  window.addEventListener('beforeunload', () => {
    clearInterval(cleanupInterval);
    delete (globalThis as any).__wse_cleanup_interval;

    if (globalWSEInstance && !globalWSEInstance.destroyed) {
      logger.info('[useWSE] Window unload - cleaning up global instance');
      destroyGlobalInstance();
    }
  });
}

// Global connection start time tracking
declare global {
  interface Window {
    __wseConnectionStartTime?: number;
  }
}

// Helper function to destroy global instance
function destroyGlobalInstance(): void {
  if (!globalWSEInstance) return;

  logger.info('[useWSE] Destroying global WSE instance');

  isGloballyConnecting = false;

  globalWSEInstance.destroyed = true;

  if (globalWSEInstance.cleanupFunction) {
    try {
      globalWSEInstance.cleanupFunction();
    } catch (e) {
      logger.error('[useWSE] Error calling cleanup function:', e);
    }
  }

  try {
    if (globalWSEInstance.connectionManager) {
      globalWSEInstance.connectionManager.destroy();
      globalWSEInstance.connectionManager = null;
    }
  } catch (e) {
    logger.error('[useWSE] Error destroying connection manager:', e);
  }

  try {
    if (globalWSEInstance.messageProcessor) {
      globalWSEInstance.messageProcessor.destroy();
      globalWSEInstance.messageProcessor = null;
    }
  } catch (e) {
    logger.error('[useWSE] Error destroying message processor:', e);
  }

  try {
    if (globalWSEInstance.networkMonitor) {
      globalWSEInstance.networkMonitor.destroy();
      globalWSEInstance.networkMonitor = null;
    }
  } catch (e) {
    logger.error('[useWSE] Error destroying network monitor:', e);
  }

  try {
    if (globalWSEInstance.offlineQueue) {
      globalWSEInstance.offlineQueue.destroy();
      globalWSEInstance.offlineQueue = null;
    }
  } catch (e) {
    logger.error('[useWSE] Error destroying offline queue:', e);
  }

  try {
    securityManager.destroy();
  } catch (e) {
    logger.error('[useWSE] Error destroying security manager:', e);
  }

  globalWSEInstance = null;
}

/**
 * Extended WSEConfig with pluggable callbacks for standalone usage.
 */
export interface UseWSEConfig extends Partial<WSEConfig> {
  /**
   * Optional callback to refresh the auth token. Applications provide their own
   * token refresh logic here (e.g., calling /api/auth/refresh).
   */
  refreshAuthToken?: () => Promise<void>;

  /**
   * Optional QueryClient-like object for AdaptiveQualityManager integration.
   * Pass your TanStack Query QueryClient here if you want React Query defaults
   * to adapt based on connection quality.
   */
  queryClient?: {
    setDefaultOptions: (options: { queries: Record<string, any> }) => void;
    invalidateQueries: () => void;
  } | null;

  /**
   * Optional list of critical handler names that MUST be registered before
   * the connection is considered ready. Defaults to empty (no critical handlers).
   */
  criticalHandlers?: string[];

  /**
   * Optional callback to register application-specific handlers.
   * Called during initialization with the MessageProcessor instance.
   */
  registerHandlers?: (messageProcessor: {
    registerHandler: (type: string, handler: (msg: any) => void) => void;
    getRegisteredHandlers?: () => string[];
  }) => void;
}

export function useWSE(
  token?: string,
  initialTopics?: string[],
  config?: UseWSEConfig
): UseWSEReturn {
  const validToken = useMemo(() => {
    if (!token || token.trim() === '') {
      logger.debug('WSE: No token provided');
      return undefined;
    }

    logger.debug('WSE: Token provided, length:', token.length);
    return token;
  }, [token]);

  // Stores
  const store = useWSEStore();
  const queueStore = useMessageQueueStore();

  // Services refs
  const connectionManagerRef = useRef<ConnectionManager | null>(null);
  const messageProcessorRef = useRef<MessageProcessor | null>(null);
  const networkMonitorRef = useRef<NetworkMonitor | null>(null);
  const offlineQueueRef = useRef<OfflineQueue | null>(null);

  // Track initialization and subscription state
  const initializedRef = useRef(false);
  const processingBatchRef = useRef(false);
  const cleanupRef = useRef<(() => void) | null>(null);
  const subscriptionsConfirmedRef = useRef(false);
  const snapshotRequestedRef = useRef(false);
  const handlersReadyRef = useRef(false);
  const reconnectAttemptsRef = useRef(0);
  const initialSyncSentRef = useRef(false);

  // Track current token to detect changes
  const currentTokenRef = useRef<string | undefined>(undefined);

  // Add timestamp tracking for rate limiting
  const lastEventTimestampRef = useRef<Map<string, number>>(new Map());
  const EVENT_THROTTLE_MS = 500;

  // Critical handlers from config (defaults to empty)
  const criticalHandlers = config?.criticalHandlers || [];

  // Memoize config with stable reference
  const finalConfig = useMemo<WSEConfig>(() => {
    const endpoints = config?.endpoints || getEndpoints();
    return {
      endpoints: endpoints.length > 0 ? endpoints : [],
      reconnection: {
        ...DEFAULT_CONFIG.reconnection!,
        ...config?.reconnection,
        maxAttempts: config?.reconnection?.maxAttempts === -1
          ? 10
          : (config?.reconnection?.maxAttempts || DEFAULT_CONFIG.reconnection!.maxAttempts!)
      },
      security: { ...DEFAULT_CONFIG.security!, ...config?.security },
      performance: { ...DEFAULT_CONFIG.performance!, ...config?.performance },
      offline: { ...DEFAULT_CONFIG.offline!, ...config?.offline },
      diagnostics: { ...DEFAULT_CONFIG.diagnostics!, ...config?.diagnostics },
    };
  }, [config?.endpoints?.join(','), config?.reconnection, config?.security, config?.performance, config?.offline, config?.diagnostics]);

  // Stable callback refs
  const stableCallbacks = useRef({
    onMessage: null as ((data: string | ArrayBuffer) => void) | null,
    onStateChange: null as ((state: ConnectionState) => void) | null,
    onServerReady: null as ((details: any) => void) | null,
  });

  // ---------------------------------------------------------------------------
  // Circuit Breaker Management
  // ---------------------------------------------------------------------------

  const startCircuitBreakerCheck = useCallback(() => {
    if (circuitBreakerCheckInterval) return;

    circuitBreakerCheckInterval = setInterval(() => {
      store.checkCircuitBreakerTimeout();

      if (store.canReconnect() && store.connectionState === ConnectionState.ERROR) {
        const manager = connectionManagerRef.current;
        if (manager && validToken) {
          logger.info('Circuit breaker timeout reached, attempting reconnection');
          manager.connect(validToken, store.activeTopics).catch(error => {
            logger.error('Auto-reconnection after circuit breaker timeout failed:', error);
          });
        }
      }
    }, 5000);
  }, [store, validToken]);

  const stopCircuitBreakerCheck = useCallback(() => {
    if (circuitBreakerCheckInterval) {
      clearInterval(circuitBreakerCheckInterval);
      circuitBreakerCheckInterval = null;
    }
  }, []);

  // ---------------------------------------------------------------------------
  // Handler Registration Helper
  // ---------------------------------------------------------------------------

  const ensureHandlersRegistered = useCallback(async (retries = 3): Promise<boolean> => {
    const processor = messageProcessorRef.current;
    if (!processor) {
      logger.error('Message processor not available');
      return false;
    }

    // If no critical handlers defined, just mark as ready
    if (criticalHandlers.length === 0) {
      handlersReadyRef.current = true;
      processor.setReady(true);
      return true;
    }

    for (let attempt = 0; attempt < retries; attempt++) {
      try {
        const registeredHandlers = processor.getRegisteredHandlers();
        const missingHandlers = criticalHandlers.filter(h => !registeredHandlers.includes(h));

        if (missingHandlers.length === 0) {
          handlersReadyRef.current = true;
          processor.setReady(true);
          logger.info('All critical handlers are registered and ready');
          return true;
        }

        logger.warn(`Attempt ${attempt + 1}: Missing handlers:`, missingHandlers);

        // Re-register all handlers
        EventHandlers.registerAll(processor);
        if (config?.registerHandlers) {
          config.registerHandlers(processor);
        }

        await new Promise(resolve => setTimeout(resolve, 100 * (attempt + 1)));

      } catch (error) {
        logger.error(`Handler registration attempt ${attempt + 1} failed:`, error);
      }
    }

    logger.error('Failed to register all critical handlers after retries');
    return false;
  }, [criticalHandlers, config?.registerHandlers]);

  // ---------------------------------------------------------------------------
  // Queue Processing
  // ---------------------------------------------------------------------------

  const processOfflineQueue = useCallback(async () => {
    const offlineQueue = offlineQueueRef.current;
    const connection = connectionManagerRef.current;

    if (!offlineQueue || !connection || !connection.isReady()) return;

    try {
      const messages = await offlineQueue.getAll();
      logger.info(`Processing ${messages.length} offline messages`);

      let allSent = true;
      for (const message of messages) {
        const sent = connection.send(message);
        if (!sent) {
          allSent = false;
          break;
        }
      }

      if (allSent) {
        await offlineQueue.clear();
      }
    } catch (error) {
      logger.error('Error processing offline queue:', error);
    }
  }, []);

  const processBatch = useCallback(async () => {
    if (processingBatchRef.current) return;
    processingBatchRef.current = true;

    try {
      const processor = messageProcessorRef.current;
      const connection = connectionManagerRef.current;
      const offlineQueue = offlineQueueRef.current;

      if (!processor || !connection) return;

      const messages = await processor.processBatch();
      for (const message of messages) {
        const sent = connection.send(message);
        if (!sent && offlineQueue) {
          await offlineQueue.enqueue(message);
        }
      }
    } catch (error) {
      logger.error('Error processing batch:', error);
    } finally {
      processingBatchRef.current = false;
    }
  }, []);

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  const sendMessage = useCallback((
    type: string,
    payload: any,
    options: MessageOptions = {}
  ) => {
    const message: WSMessage = {
      t: type,
      p: payload,
      id: crypto.randomUUID(),
      ts: new Date().toISOString(),
      v: 2,
    };

    if (options.priority) message.pri = options.priority;
    if (options.correlation_id) message.cid = options.correlation_id;
    if (options.encrypted) message.enc = true;

    if (!initializedRef.current || !messageProcessorRef.current || !connectionManagerRef.current) {
      logger.debug('WSE not fully initialized, queueing message for later');

      const retryCount = 5;
      let attempts = 0;

      const tryToSend = () => {
        attempts++;

        if (attempts > retryCount) {
          logger.error(`Failed to send queued message after retries:`, {
            messageType: type,
            payload: payload,
            attempts: attempts
          });
          return;
        }

        const processor = messageProcessorRef.current;
        const connection = connectionManagerRef.current;

        if (processor && connection && connection.isReady()) {
          const isCriticalMessage = ['subscription_update', 'sync_request', 'client_hello'].includes(type);

          if (connection.isConnected() && (connection.isReady() || isCriticalMessage)) {
            try {
              logger.debug(`Attempting to send queued message ${type} (attempt ${attempts})`);
              const sent = connection.send(message);
              if (!sent) {
                processor.queueOutgoing(message, options.priority || MessagePriority.NORMAL);
              }
              logger.info(`Queued message ${type} sent successfully after ${attempts} attempts`);
              return;
            } catch (error) {
              logger.error(`Error sending queued message ${type}:`, error);
            }
          } else {
            logger.debug(`Connection not ready for ${type}, isConnected: ${connection.isConnected()}, isReady: ${connection.isReady()}`);
          }
        }

        const delay = Math.min(100 * Math.pow(2, attempts - 1), 2000);
        logger.debug(`Retrying ${type} in ${delay}ms (attempt ${attempts}/${retryCount})`);
        setTimeout(tryToSend, delay);
      };

      setTimeout(tryToSend, 100);
      return;
    }

    const processor = messageProcessorRef.current;
    const connection = connectionManagerRef.current;
    const offlineQueue = offlineQueueRef.current;

    if (connection?.isReady()) {
      try {
        const sent = connection.send(message);
        if (!sent) {
          processor.queueOutgoing(message, options.priority || MessagePriority.NORMAL);
        }
      } catch (error) {
        logger.error('Error sending message:', error);
        processor.queueOutgoing(message, options.priority || MessagePriority.NORMAL);
      }
    } else if (options.offline && offlineQueue) {
      offlineQueue.enqueue(message).catch(error => {
        logger.error('Failed to enqueue offline message:', error);
      });
    } else {
      processor.queueOutgoing(message, options.priority || MessagePriority.NORMAL);
    }
  }, []);

  // ---------------------------------------------------------------------------
  // Message Handling Callbacks with Deduplication
  // ---------------------------------------------------------------------------

  const handleIncomingMessage = useCallback((data: string | ArrayBuffer) => {
    const processor = messageProcessorRef.current;
    const monitor = networkMonitorRef.current;

    if (!processor || !monitor) return;

    try {
      let parsed: any;

      if (typeof data === 'string') {
        if (data.toUpperCase().startsWith('PING') || data.startsWith('PONG:')) {
          processor.processIncoming(data).catch(error => {
            logger.error('Failed to process ping/pong:', error);
          });
          return;
        }

        let jsonData = data;
        if (data.startsWith('WSE{') || data.startsWith('WSE[')) {
          jsonData = data.slice(3);
        } else if (data.startsWith('S{') || data.startsWith('U{') || data.startsWith('S[') || data.startsWith('U[')) {
          jsonData = data.slice(1);
        }
        parsed = JSON.parse(jsonData);
      } else {
        const view = new Uint8Array(data);

        if (view.length >= 2 && view[0] === 67 && view[1] === 58) {
          processor.processIncoming(data).catch(error => {
            logger.error('Failed to process compressed message:', error);
          });
          return;
        }

        try {
          const text = new TextDecoder().decode(data);
          parsed = JSON.parse(text);
        } catch {
          processor.processIncoming(data).catch(error => {
            logger.error('Failed to process binary message:', error);
          });
          return;
        }
      }

      // Age filtering
      if (parsed && parsed.ts) {
        const eventTime = new Date(parsed.ts).getTime();
        const now = Date.now();
        const age = now - eventTime;
        const eventType = parsed.t;

        const connectionTime = Date.now() - (window.__wseConnectionStartTime || Date.now());
        const isNewConnection = connectionTime < 5000;

        if (isNewConnection) {
          if (eventType && eventType.includes('snapshot')) {
            logger.debug(`Accepting ${eventType} snapshot during new connection`);
          } else if (age > 1000) {
            logger.debug(`Rejecting old ${eventType} event during new connection (${Math.round(age / 1000)}s old)`);
            return;
          }
        } else {
          const maxAge = eventType && (
            eventType.includes('snapshot') ||
            eventType.includes('health_update') ||
            eventType.includes('connection_update')
          ) ? 60000 : 5000;
          if (age > maxAge) {
            logger.debug(`Rejecting old ${eventType} event (${Math.round(age / 1000)}s old)`);
            return;
          }
        }
      }

      // Check event ID for deduplication
      if (parsed && parsed.id) {
        if (processedEvents.size >= MAX_CACHED_EVENTS) {
          const oldestKey = processedEvents.keys().next().value;
          if (oldestKey) processedEvents.delete(oldestKey);
        }

        if (processedEvents.has(parsed.id)) {
          logger.debug(`Skipping globally duplicate event: ${parsed.id}`);
          return;
        }
        processedEvents.set(parsed.id, Date.now());
      }

      // Check event type throttling
      if (parsed && parsed.t) {
        const eventType = parsed.t;
        const now = Date.now();
        const lastTimestamp = lastEventTimestampRef.current.get(eventType) || 0;

        let throttleMs = EVENT_THROTTLE_MS;

        if (eventType === 'heartbeat') {
          throttleMs = 5000;
        } else if (eventType.includes('snapshot')) {
          throttleMs = 0;
        } else if (eventType === 'health_check' || eventType === 'health_check_response') {
          throttleMs = 10000;
        } else if (eventType === 'performance_metrics' || eventType === 'metrics_response') {
          throttleMs = 60000;
        }

        if (throttleMs > 0 && (now - lastTimestamp) < throttleMs) {
          logger.debug(`Throttling ${eventType} event (last: ${now - lastTimestamp}ms ago)`);
          return;
        }

        lastEventTimestampRef.current.set(eventType, now);
      }

    } catch (e) {
      logger.debug('Could not pre-parse message for deduplication:', e);
    }

    // Record metrics
    monitor.recordPacketReceived();
    if (data instanceof ArrayBuffer) {
      monitor.recordBytes(data.byteLength);
    } else {
      monitor.recordBytes(data.length);
    }

    // Process the message
    processor.processIncoming(data).catch(error => {
      logger.error('Failed to process incoming message:', error);
    });
  }, []);

  const handleStateChange = useCallback((state: ConnectionState) => {
    store.setConnectionState(state);

    if (state === ConnectionState.CONNECTED) {
      reconnectAttemptsRef.current = 0;
      store.resetCircuitBreaker();

      ensureHandlersRegistered().then((ready) => {
        if (ready) {
          processOfflineQueue();
        }
      });
    } else if (state === ConnectionState.ERROR) {
      startCircuitBreakerCheck();
    }
  }, [store, processOfflineQueue, ensureHandlersRegistered, startCircuitBreakerCheck]);

  const requestSnapshot = useCallback(async (topics: string[], retries = 3, delay = 1000) => {
    if (snapshotRequestedRef.current) {
      logger.info('Snapshot already requested, skipping duplicate');
      return;
    }

    const handlersReady = await ensureHandlersRegistered();
    if (!handlersReady && criticalHandlers.length > 0) {
      logger.error('Cannot request snapshot - handlers not ready');
      return;
    }

    for (let i = 0; i < retries; i++) {
      try {
        if (i > 0) {
          const waitTime = delay * Math.pow(2, i - 1);
          logger.info(`Waiting ${waitTime}ms before retry ${i + 1}/${retries}`);
          await new Promise(resolve => setTimeout(resolve, waitTime));
        }

        logger.info(`=== REQUESTING SNAPSHOTS (Attempt ${i + 1}/${retries}) ===`);
        logger.info('Topics for snapshot:', topics);

        sendMessage('sync_request', {
          topics,
          include_snapshots: true,
          include_history: false,
          last_sequence: 0,
        }, { priority: MessagePriority.HIGH });

        snapshotRequestedRef.current = true;
        break;
      } catch (error) {
        logger.warn(`Snapshot request attempt ${i + 1} failed:`, error);
        if (i === retries - 1) {
          logger.error('All snapshot request attempts failed');
        }
      }
    }
  }, [sendMessage, ensureHandlersRegistered, criticalHandlers.length]);

  const handleServerReady = useCallback((details: any) => {
    logger.info('=== SERVER READY RECEIVED ===');
    logger.info('Server ready details:', details);

    processedEvents.clear();
    lastEventTimestampRef.current.clear();

    initialSyncSentRef.current = false;
    snapshotRequestedRef.current = false;

    const savedTopics = store.activeTopics.length > 0
      ? store.activeTopics
      : (initialTopics || getSavedSubscriptions());

    // Use saved topics as-is. Applications can add their own default topics
    // via initialTopics parameter.
    const topics = [...new Set(savedTopics)];

    logger.info('Subscribing to topics:', topics);

    if (!initialSyncSentRef.current) {
      initialSyncSentRef.current = true;

      setTimeout(() => {
        sendMessage('subscription_update', {
          action: 'subscribe',
          topics,
        }, { priority: MessagePriority.HIGH });

        // Request initial snapshot after subscription
        setTimeout(() => {
          logger.info('Requesting initial snapshot after subscription');
          requestSnapshot(topics, 3, 1000);
        }, 500);
      }, 200);
    }

    store.setActiveTopics(topics);
    saveSubscriptions(topics);
  }, [store, initialTopics, sendMessage, requestSnapshot]);

  // Update stable callbacks
  useEffect(() => {
    stableCallbacks.current = {
      onMessage: handleIncomingMessage,
      onStateChange: handleStateChange,
      onServerReady: handleServerReady,
    };
  });

  // Add subscription confirmation handler
  useEffect(() => {
    const handleSubscriptionUpdate = (event: CustomEvent) => {
      const { success } = event.detail;
      if (success) {
        logger.info('Subscription confirmed');
        subscriptionsConfirmedRef.current = true;
      }
    };

    window.addEventListener('subscriptionUpdate', handleSubscriptionUpdate as EventListener);
    return () => {
      window.removeEventListener('subscriptionUpdate', handleSubscriptionUpdate as EventListener);
    };
  }, []);

  // Handle snapshot request events
  useEffect(() => {
    const handleSnapshotRequest = (event: CustomEvent) => {
      const { topics, include_snapshots, timestamp } = event.detail;
      logger.info('[useWSE] Snapshot request received via event:', { topics });

      const connection = connectionManagerRef.current;
      if (connection && connection.isReady()) {
        sendMessage('sync_request', {
          topics,
          include_snapshots,
          timestamp
        }, { priority: MessagePriority.HIGH });
        logger.info('[useWSE] Snapshot request sent successfully');
      } else {
        logger.warn('[useWSE] Cannot send snapshot request - connection not ready');
      }
    };

    window.addEventListener('wseRequestSnapshot', handleSnapshotRequest as EventListener);
    return () => {
      window.removeEventListener('wseRequestSnapshot', handleSnapshotRequest as EventListener);
    };
  }, [sendMessage]);

  // ---------------------------------------------------------------------------
  // Main Initialization Effect
  // ---------------------------------------------------------------------------

  useEffect(() => {
    const tokenChanged = currentTokenRef.current !== validToken;

    if (tokenChanged) {
      logger.info('WSE: Token state changed', {
        hadToken: !!currentTokenRef.current,
        hasToken: !!validToken,
        tokenChanged: true
      });

      if (currentTokenRef.current && globalWSEInstance && globalWSEInstance.token === currentTokenRef.current) {
        logger.info('WSE: Token changed - destroying old global instance');
        destroyGlobalInstance();
      }

      currentTokenRef.current = validToken;
    }

    if (!validToken) {
      if (initializedRef.current && cleanupRef.current) {
        logger.info('WSE: Token removed, cleaning up');
        cleanupRef.current();
        cleanupRef.current = null;
        initializedRef.current = false;

        if (globalWSEInstance && !globalWSEInstance.token) {
          destroyGlobalInstance();
        }
      }
      return;
    }

    if (!tokenChanged && initializedRef.current) {
      logger.debug('WSE: Token unchanged and already initialized');
      return;
    }

    if (globalWSEInstance && globalWSEInstance.token === validToken && !globalWSEInstance.destroyed) {
      logger.info('WSE: Reusing existing global instance');

      if (globalWSEInstance.initPromise) {
        logger.info('WSE: Waiting for pending initialization');
        globalWSEInstance.initPromise.then(() => {
          if (globalWSEInstance && globalWSEInstance.token === validToken && !globalWSEInstance.destroyed) {
            connectionManagerRef.current = globalWSEInstance.connectionManager;
            messageProcessorRef.current = globalWSEInstance.messageProcessor;
            networkMonitorRef.current = globalWSEInstance.networkMonitor;
            offlineQueueRef.current = globalWSEInstance.offlineQueue;
            initializedRef.current = true;
          }
        }).catch((error) => {
          logger.error('WSE: Pending initialization failed:', error);
          initializedRef.current = false;
          if (globalWSEInstance?.token === validToken) {
            destroyGlobalInstance();
          }
        });
        return;
      }

      connectionManagerRef.current = globalWSEInstance.connectionManager;
      messageProcessorRef.current = globalWSEInstance.messageProcessor;
      networkMonitorRef.current = globalWSEInstance.networkMonitor;
      offlineQueueRef.current = globalWSEInstance.offlineQueue;
      initializedRef.current = true;
      return;
    }

    const now = Date.now();
    if (isGloballyConnecting) {
      logger.warn('WSE: Another connection attempt in progress, skipping');
      return;
    }
    if (now - lastConnectionAttemptTime < MIN_CONNECTION_INTERVAL) {
      logger.warn(`WSE: Connection attempt too soon (${now - lastConnectionAttemptTime}ms since last), skipping`);
      return;
    }

    logger.info('=== INITIALIZING NEW WSE INSTANCE ===');

    isGloballyConnecting = true;
    lastConnectionAttemptTime = now;

    initializedRef.current = false;
    subscriptionsConfirmedRef.current = false;
    snapshotRequestedRef.current = false;
    handlersReadyRef.current = false;
    reconnectAttemptsRef.current = 0;
    initialSyncSentRef.current = false;

    let mounted = true;
    let connectionManager: ConnectionManager | null = null;
    let messageProcessor: MessageProcessor | null = null;
    let networkMonitor: NetworkMonitor | null = null;
    let offlineQueue: OfflineQueue | null = null;
    let batchInterval: NodeJS.Timeout | null = null;
    let diagnosticsInterval: NodeJS.Timeout | null = null;

    const cleanup = () => {
      logger.info('WSE: Cleaning up instance');
      mounted = false;
      initializedRef.current = false;
      subscriptionsConfirmedRef.current = false;
      snapshotRequestedRef.current = false;
      handlersReadyRef.current = false;
      reconnectAttemptsRef.current = 0;
      initialSyncSentRef.current = false;

      delete window.__wseConnectionStartTime;

      stopCircuitBreakerCheck();

      if (batchInterval) {
        clearInterval(batchInterval);
        batchInterval = null;
      }
      if (diagnosticsInterval) {
        clearInterval(diagnosticsInterval);
        diagnosticsInterval = null;
      }

      connectionManagerRef.current = null;
      messageProcessorRef.current = null;
      networkMonitorRef.current = null;
      offlineQueueRef.current = null;

      store.reset();
      queueStore.clearQueue();

      logger.info('WSE: Cleanup complete');
    };

    const initializeServices = async () => {
      try {
        window.__wseConnectionStartTime = Date.now();

        messageProcessor = new MessageProcessor(
          finalConfig.performance.batchSize,
          finalConfig.performance.batchTimeout
        );

        messageProcessorRef.current = messageProcessor;

        // Register default handlers (no-op in standalone) plus application handlers
        EventHandlers.registerAll(messageProcessor);
        if (config?.registerHandlers) {
          config.registerHandlers(messageProcessor);
        }
        logger.info('=== EVENT HANDLERS REGISTERED ===');

        // Verify critical handlers if any are defined
        if (criticalHandlers.length > 0) {
          try {
            const handlersReady = await messageProcessor.waitForHandlers(criticalHandlers, 5000);
            if (!handlersReady) {
              throw new Error('Failed to register critical handlers within timeout');
            }
          } catch (handlerError) {
            logger.error('Handler registration failed:', handlerError);
            throw new Error(`Failed to register critical handlers: ${(handlerError as Error).message}`);
          }
        }

        handlersReadyRef.current = true;
        messageProcessor.setReady(true);

        // Initialize SecurityManager if needed
        if (finalConfig.security.encryptionEnabled || finalConfig.security.messageSignature) {
          try {
            await securityManager.initialize({
              encryptionEnabled: finalConfig.security.encryptionEnabled,
              messageSigningEnabled: finalConfig.security.messageSignature,
            });
            logger.info('SecurityManager initialized', {
              encryption: finalConfig.security.encryptionEnabled,
              signing: finalConfig.security.messageSignature,
            });
          } catch (secError) {
            logger.warn('SecurityManager initialization failed:', secError);
          }
        }

        // Build ConnectionManager config
        const cmConfig: ConnectionManagerConfig = {
          refreshAuthToken: config?.refreshAuthToken,
          queryClient: config?.queryClient ?? null,
        };

        connectionManager = new ConnectionManager(
          finalConfig.endpoints,
          finalConfig.reconnection,
          (data) => stableCallbacks.current.onMessage?.(data),
          (state) => {
            handleStateChange(state);
          },
          (details) => stableCallbacks.current.onServerReady?.(details),
          cmConfig
        );

        messageProcessor.setConnectionManager(connectionManager);

        networkMonitor = new NetworkMonitor();
        offlineQueue = new OfflineQueue(finalConfig.offline);

        connectionManagerRef.current = connectionManager;
        networkMonitorRef.current = networkMonitor;
        offlineQueueRef.current = offlineQueue;

        initializedRef.current = true;

        try {
          await offlineQueue.initialize();
          logger.info('Offline queue initialized');
        } catch (offlineError) {
          logger.warn('Offline queue initialization failed:', offlineError);
        }

        const savedTopics = initialTopics || getSavedSubscriptions();
        const topics = [...new Set(savedTopics)];
        store.setActiveTopics(topics);

        if (!mounted) {
          logger.info('Component unmounted during initialization');
          return;
        }

        startCircuitBreakerCheck();

        logger.info('=== STARTING CONNECTION ===');
        logger.info('Endpoints:', finalConfig.endpoints);
        logger.info('Topics:', topics);

        try {
          await connectionManager.connect(validToken, topics);
        } catch (connectError) {
          logger.error('Connection failed:', connectError);
          store.setConnectionState(ConnectionState.ERROR);
          store.setLastError(`Failed to establish WebSocket connection: ${(connectError as Error).message}`);
        }

        if (!mounted) {
          logger.info('Component unmounted after connection');
          return;
        }

        batchInterval = setInterval(() => {
          if (processingBatchRef.current || !mounted) return;
          void processBatch().catch(error => {
            logger.error('Batch processing error:', error);
          });
        }, finalConfig.performance.batchTimeout);

        if (finalConfig.diagnostics.enabled) {
          diagnosticsInterval = setInterval(() => {
            if (!mounted || !networkMonitor) return;
            try {
              const diagnostics = networkMonitor.analyze();
              store.updateDiagnostics(diagnostics);
            } catch (diagError) {
              logger.error('Diagnostics update error:', diagError);
            }
          }, finalConfig.diagnostics.metricsInterval);
        }

        logger.info('=== WSE INITIALIZATION COMPLETE ===');

        isGloballyConnecting = false;

        cleanupRef.current = cleanup;

        if (globalWSEInstance && globalWSEInstance.token === validToken) {
          globalWSEInstance.connectionManager = connectionManager;
          globalWSEInstance.messageProcessor = messageProcessor;
          globalWSEInstance.networkMonitor = networkMonitor;
          globalWSEInstance.offlineQueue = offlineQueue;
          globalWSEInstance.cleanupFunction = cleanup;
          globalWSEInstance.initPromise = null;
        }

      } catch (error) {
        logger.error('Failed to initialize WSE:', error);

        isGloballyConnecting = false;

        initializedRef.current = false;
        store.setConnectionState(ConnectionState.ERROR);
        store.setLastError(error instanceof Error ? error.message : 'Initialization failed');

        try {
          if (connectionManager) connectionManager.destroy();
          if (messageProcessor) messageProcessor.destroy();
          if (networkMonitor) networkMonitor.destroy();
          if (offlineQueue) offlineQueue.destroy();
        } catch (cleanupError) {
          logger.error('Error during cleanup:', cleanupError);
        }

        connectionManagerRef.current = null;
        messageProcessorRef.current = null;
        networkMonitorRef.current = null;
        offlineQueueRef.current = null;

        if (globalWSEInstance?.token === validToken) {
          destroyGlobalInstance();
        }

        throw error;
      }
    };

    const initPromise = initializeServices().catch(error => {
      logger.error('WSE initialization failed:', error);
      isGloballyConnecting = false;
      cleanup();
    });

    globalWSEInstance = {
      token: validToken,
      connectionManager: null,
      messageProcessor: null,
      networkMonitor: null,
      offlineQueue: null,
      initPromise,
      cleanupFunction: null,
      destroyed: false,
    };

    return () => {
      logger.info('useWSE effect cleanup triggered');

      if (globalWSEInstance && globalWSEInstance.token === validToken) {
        if (currentTokenRef.current !== validToken) {
          logger.info('Token changing, destroying global instance immediately');
          destroyGlobalInstance();
        } else {
          // Keep global instance alive for reuse by next mount
          logger.info('Component unmounting, keeping global instance for reuse');
          return;
        }
      }

      // Only run cleanup if we're actually tearing down (token changed or no global instance)
      if (cleanupRef.current) {
        cleanupRef.current();
        cleanupRef.current = null;
      }
    };
  }, [validToken]);

  // ---------------------------------------------------------------------------
  // Public API Implementation
  // ---------------------------------------------------------------------------

  const subscribe = useCallback((topics: string[]) => {
    if (!topics.length) return;

    const currentTopics = store.activeTopics;
    const newTopics = [...new Set([...currentTopics, ...topics])];

    store.setActiveTopics(newTopics);
    saveSubscriptions(newTopics);

    sendMessage('subscription_update', {
      action: 'subscribe',
      topics,
    }, { priority: MessagePriority.HIGH });
  }, [store, sendMessage]);

  const unsubscribe = useCallback((topics: string[]) => {
    if (!topics.length) return;

    const currentTopics = store.activeTopics;
    const newTopics = currentTopics.filter(t => !topics.includes(t));

    store.setActiveTopics(newTopics);
    saveSubscriptions(newTopics);

    sendMessage('subscription_update', {
      action: 'unsubscribe',
      topics,
    }, { priority: MessagePriority.HIGH });
  }, [store, sendMessage]);

  const sendBatch = useCallback((messages: Array<{ type: string; payload: any }>) => {
    messages.forEach(({ type, payload }) => {
      sendMessage(type, payload);
    });
  }, [sendMessage]);

  const forceReconnect = useCallback(() => {
    const manager = connectionManagerRef.current;
    if (manager && validToken) {
      subscriptionsConfirmedRef.current = false;
      snapshotRequestedRef.current = false;
      handlersReadyRef.current = false;
      reconnectAttemptsRef.current = 0;
      initialSyncSentRef.current = false;

      processedEvents.clear();
      lastEventTimestampRef.current.clear();

      store.resetCircuitBreaker();
      store.updateMetrics({ reconnectCount: 0 });

      manager.disconnect();
      setTimeout(() => {
        manager.connect(validToken, store.activeTopics).catch(error => {
          logger.error('Force reconnection failed:', error);
        });
      }, 100);
    }
  }, [validToken, store]);

  const changeEndpoint = useCallback((endpoint: string) => {
    const manager = connectionManagerRef.current;
    if (manager) {
      subscriptionsConfirmedRef.current = false;
      snapshotRequestedRef.current = false;
      handlersReadyRef.current = false;
      reconnectAttemptsRef.current = 0;
      initialSyncSentRef.current = false;

      store.resetCircuitBreaker();
      manager.changeEndpoint(endpoint);
    }
  }, [store]);

  const setCompression = useCallback((enabled: boolean) => {
    store.updateConfig({ compressionEnabled: enabled });
    logger.info(`Compression ${enabled ? 'enabled' : 'disabled'}`);
  }, [store]);

  const downloadDiagnostics = useCallback(() => {
    const connection = connectionManagerRef.current;
    const processor = messageProcessorRef.current;
    const diagnostics = {
      timestamp: new Date().toISOString(),
      connection: {
        state: store.connectionState,
        endpoints: store.endpoints,
        activeEndpoint: store.activeEndpoint,
        connectionId: connection?.getConnectionId() || null,
        isReady: connection?.isReady() || false,
        rateLimiter: connection?.getRateLimiterStats() || null,
        connectionPool: connection?.getConnectionPoolStats() || null,
        healthScores: connection?.getHealthScores() || null,
      },
      metrics: store.metrics,
      diagnostics: store.diagnostics,
      circuitBreaker: store.circuitBreaker,
      queue: {
        message: queueStore.stats,
        offline: offlineQueueRef.current?.getStats() || null,
      },
      activeTopics: store.activeTopics,
      errorHistory: store.errorHistory,
      config: finalConfig,
      network: {
        monitor: networkMonitorRef.current?.getLatencyStats() || null,
        lastDiagnostics: networkMonitorRef.current?.getLastDiagnostics() || null,
      },
      handlers: {
        registered: processor?.getRegisteredHandlers() || [],
        subscriptionsConfirmed: subscriptionsConfirmedRef.current,
        snapshotRequested: snapshotRequestedRef.current,
        handlersReady: handlersReadyRef.current,
        initialSyncSent: initialSyncSentRef.current,
      },
      reconnectAttempts: reconnectAttemptsRef.current,
      deduplication: {
        processedEventsCount: processedEvents.size,
        eventThrottleCounts: Object.fromEntries(lastEventTimestampRef.current),
      },
    };

    const blob = new Blob([JSON.stringify(diagnostics, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `wse-diagnostics-${Date.now()}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }, [store, queueStore, finalConfig]);

  const clearOfflineQueue = useCallback(async () => {
    const offlineQueue = offlineQueueRef.current;
    if (offlineQueue) {
      await offlineQueue.clear();
      logger.info('Offline queue cleared');
    }
  }, []);

  const getQueueSize = useCallback(() => {
    return queueStore.size + (offlineQueueRef.current?.getStats().size || 0);
  }, [queueStore.size]);

  const requestSnapshotPublic = useCallback((topics?: string[]) => {
    logger.info('=== MANUALLY REQUESTING SNAPSHOT ===');
    snapshotRequestedRef.current = false;
    const snapshotTopics = topics || store.activeTopics;
    requestSnapshot(snapshotTopics, 1, 0);
  }, [requestSnapshot, store.activeTopics]);

  const debugHandlers = useCallback(() => {
    const processor = messageProcessorRef.current;
    if (processor) {
      const handlers = processor.getRegisteredHandlers();
      logger.info('=== CURRENT REGISTERED HANDLERS ===');
      logger.info('Total handlers:', handlers.length);
      logger.info('Handlers:', handlers);

      if (criticalHandlers.length > 0) {
        criticalHandlers.forEach(handler => {
          if (handlers.includes(handler)) {
            logger.info(`${handler} is registered`);
          } else {
            logger.error(`ERROR: ${handler} is NOT registered`);
          }
        });
      }

      logger.info('Subscriptions confirmed:', subscriptionsConfirmedRef.current);
      logger.info('Snapshot requested:', snapshotRequestedRef.current);
      logger.info('Handlers ready:', handlersReadyRef.current);
      logger.info('Initial sync sent:', initialSyncSentRef.current);
      logger.info('Circuit breaker:', store.circuitBreaker);
      logger.info('Can reconnect:', store.canReconnect());
      logger.info('Processed events count:', processedEvents.size);
      logger.info('Event throttle states:', Object.fromEntries(lastEventTimestampRef.current));
    } else {
      logger.error('Message processor not initialized');
    }
  }, [store, criticalHandlers]);

  // ---------------------------------------------------------------------------
  // Return API
  // ---------------------------------------------------------------------------

  return useMemo(() => ({
    // Connection management
    subscribe,
    unsubscribe,
    forceReconnect,
    changeEndpoint,

    // Messaging
    sendMessage,
    sendBatch,

    // Status & monitoring
    stats: store.metrics,
    activeTopics: store.activeTopics,
    connectionHealth: store.connectionState,
    diagnostics: store.diagnostics,

    // Advanced features
    setCompression,
    downloadDiagnostics,
    clearOfflineQueue,
    getQueueSize,

    // Snapshot request
    requestSnapshot: requestSnapshotPublic,

    // Debug features
    debugHandlers,
  }), [
    subscribe,
    unsubscribe,
    forceReconnect,
    changeEndpoint,
    sendMessage,
    sendBatch,
    store.metrics,
    store.activeTopics,
    store.connectionState,
    store.diagnostics,
    setCompression,
    downloadDiagnostics,
    clearOfflineQueue,
    getQueueSize,
    requestSnapshotPublic,
    debugHandlers,
  ]);
}

// Add debug utilities to window for development
if (typeof window !== 'undefined') {
  try {
    const isDev = typeof process !== 'undefined' && process.env?.NODE_ENV === 'development';
    if (isDev) {
      (window as any).WSE = {
        getStore: () => useWSEStore.getState(),
        getQueueStore: () => useMessageQueueStore.getState(),
        resetCircuitBreaker: () => {
          const store = useWSEStore.getState();
          store.resetCircuitBreaker();
        },
        forceReconnect: () => {
          const store = useWSEStore.getState();
          store.resetCircuitBreaker();
          window.dispatchEvent(new CustomEvent('wseForceReconnect'));
        },
        forceDisconnect: () => {
          logger.info('[WSE Debug] Force disconnect called');
          if (globalWSEInstance) {
            destroyGlobalInstance();
          }
        },
        enableDebug: () => MessageProcessor.enableDebugMode(true),
        disableDebug: () => MessageProcessor.enableDebugMode(false),
        getDebugLog: () => JSON.parse(sessionStorage.getItem('wse_debug_log') || '[]'),
        clearDebugLog: () => sessionStorage.removeItem('wse_debug_log'),
        checkCanReconnect: () => {
          const store = useWSEStore.getState();
          return store.canReconnect();
        },
        getCircuitBreaker: () => {
          const store = useWSEStore.getState();
          return store.circuitBreaker;
        },
        getProcessedEvents: () => processedEvents,
        clearProcessedEvents: () => processedEvents.clear(),
        getGlobalInstance: () => globalWSEInstance,
        destroyGlobalInstance: () => destroyGlobalInstance(),
        isConnecting: () => isGloballyConnecting,
        resetConnectionLock: () => { isGloballyConnecting = false; },
        getLastConnectionTime: () => lastConnectionAttemptTime,
      };

      logger.info('WSE debug utilities available at window.WSE');
    }
  } catch {
    // Ignore errors in environment detection
  }
}
