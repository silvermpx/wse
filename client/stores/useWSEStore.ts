// =============================================================================
// WebSocket Engine - WSE Store (Zustand)
// =============================================================================

import { create } from 'zustand';
import { subscribeWithSelector } from 'zustand/middleware';
import {
  WSEState,
  ConnectionState,
  NetworkDiagnostics,
  ConnectionInfo,
  ConnectionMetrics,
  CircuitBreakerInfo,
  CircuitBreakerState,
  DiagnosticsData,
  QueueStats,
  ConnectionQuality,
  PerformanceMetrics,
  SecurityInfo,
  SubscriptionInfo,
  OfflineQueueStats,
} from '../types';

interface WSEStore extends WSEState {
  // Actions - Connection
  setConnectionState: (state: ConnectionState) => void;
  setActiveEndpoint: (endpoint: string | null) => void;
  setPreferredEndpoint: (endpoint: string | null) => void;
  updateEndpoint: (endpoint: string, updates: Partial<ConnectionInfo>) => void;
  addEndpoint: (endpoint: string) => void;

  // Actions - Metrics
  updateMetrics: (updates: Partial<ConnectionMetrics>) => void;
  recordLatency: (latency: number) => void;
  incrementMetric: (key: keyof ConnectionMetrics, value?: number) => void;

  // Actions - Circuit Breaker
  updateCircuitBreaker: (updates: Partial<CircuitBreakerInfo>) => void;
  recordCircuitBreakerFailure: () => void;
  recordCircuitBreakerSuccess: () => void;
  resetCircuitBreaker: () => void;
  checkCircuitBreakerTimeout: () => void;

  // Actions - Subscriptions
  setActiveTopics: (topics: string[]) => void;
  addPendingSubscription: (topic: string) => void;
  confirmSubscription: (topic: string) => void;
  failSubscription: (topic: string) => void;

  // Actions - Queue
  updateQueueStats: (stats: Partial<QueueStats>) => void;
  updateOfflineQueue: (updates: Partial<OfflineQueueStats>) => void;

  // Actions - Diagnostics
  updateDiagnostics: (diagnostics: DiagnosticsData | null) => void;

  // Actions - Performance
  updatePerformance: (updates: Partial<PerformanceMetrics>) => void;

  // Actions - Security
  updateSecurity: (updates: Partial<SecurityInfo>) => void;

  // Actions - Errors
  setLastError: (error: string | null, code?: number) => void;
  clearErrors: () => void;

  // Actions - Configuration
  updateConfig: (updates: Partial<WSEState['config']>) => void;

  // Computed getters
  isConnected: () => boolean;
  canReconnect: () => boolean;
  getConnectionQuality: () => ConnectionQuality;

  // Reset
  reset: () => void;
}

const initialMetrics: ConnectionMetrics = {
  messagesReceived: 0,
  messagesSent: 0,
  messagesQueued: 0,
  messagesDropped: 0,
  compressionRatio: 1,
  compressionHits: 0,
  reconnectCount: 0,
  connectionAttempts: 0,
  successfulConnections: 0,
  failedConnections: 0,
  lastLatency: null,
  avgLatency: null,
  minLatency: null,
  maxLatency: null,
  latencyP95: null,
  latencyP99: null,
  bytesReceived: 0,
  bytesSent: 0,
  bandwidth: 0,
  messageRate: 0,
  connectedSince: null,
  lastMessageReceived: null,
  lastMessageSent: null,
  lastHealthCheck: null,
  lastErrorCode: null,
  lastErrorMessage: null,
  authFailures: 0,
  protocolErrors: 0,
  networkJitter: 0,
  packetLoss: 0,
};

const initialCircuitBreaker: CircuitBreakerInfo = {
  state: CircuitBreakerState.CLOSED,
  failures: 0,
  lastFailureTime: null,
  successCount: 0,
  nextRetryTime: null,
  threshold: 5,
  timeout: 60000,
};

const initialQueueStats: QueueStats = {
  size: 0,
  capacity: 10000,
  utilizationPercent: 0,
  oldestMessageAge: null,
  priorityDistribution: {},
  processingRate: 0,
  backpressure: false,
};

const initialOfflineQueue: OfflineQueueStats = {
  enabled: true,
  size: 0,
  capacity: 1000,
  oldestMessageAge: null,
  persistedToStorage: true,
};

const initialPerformance: PerformanceMetrics = {
  cpuUsage: 0,
  memoryUsage: 0,
  gcPauseTime: 0,
  eventLoopLag: 0,
};

const initialSecurity: SecurityInfo = {
  encryptionEnabled: false,
  encryptionAlgorithm: null,
  messageSigningEnabled: false,
  sessionKeyRotation: null,
  lastKeyRotation: null,
};

const initialSubscriptions: SubscriptionInfo = {
  topics: [],
  pendingSubscriptions: [],
  failedSubscriptions: [],
  lastUpdate: null,
};

const initialState: WSEState = {
  // Connection
  connectionState: ConnectionState.PENDING,
  activeEndpoint: null,
  preferredEndpoint: null,
  endpoints: [],

  // Metrics
  metrics: initialMetrics,
  diagnostics: null,
  performance: initialPerformance,

  // Circuit Breaker
  circuitBreaker: initialCircuitBreaker,

  // Queues
  messageQueue: initialQueueStats,
  offlineQueue: initialOfflineQueue,

  // Security
  security: initialSecurity,

  // Subscriptions
  subscriptions: initialSubscriptions,
  activeTopics: [],

  // Errors
  lastError: null,
  errorHistory: [],

  // Configuration
  config: {
    reconnectEnabled: true,
    maxReconnectAttempts: 10,
    compressionEnabled: true,
    batchingEnabled: true,
    offlineModeEnabled: true,
    healthCheckInterval: 30000,
    metricsInterval: 60000,
  },

  // Sequence
  sequence: 0,
};

// Latency tracking with bounded history
const LATENCY_HISTORY_SIZE = 100;
let latencyHistory: number[] = [];

// Circuit breaker auto-recovery timer
let circuitBreakerTimer: NodeJS.Timeout | null = null;

export const useWSEStore = create<WSEStore>()(
  subscribeWithSelector((set, get) => ({
    ...initialState,

    // Connection Actions
  setConnectionState: (state) => set((prev) => {
    if (prev.connectionState === state) {
      return prev;
    }

    if (state === ConnectionState.CONNECTED && circuitBreakerTimer) {
      clearTimeout(circuitBreakerTimer);
      circuitBreakerTimer = null;
    }

    return {
      connectionState: state,
      metrics: state === ConnectionState.CONNECTED && !prev.metrics.connectedSince
        ? { ...prev.metrics, connectedSince: Date.now() }
        : prev.metrics
    };
  }),

    setActiveEndpoint: (endpoint) => set({ activeEndpoint: endpoint }),
    setPreferredEndpoint: (endpoint) => set({ preferredEndpoint: endpoint }),

    updateEndpoint: (endpoint, updates) => set((state) => ({
      endpoints: state.endpoints.map(e =>
        e.endpoint === endpoint ? { ...e, ...updates } : e
      ),
    })),

    addEndpoint: (endpoint) => set((state) => {
      const existing = state.endpoints.find(e => e.endpoint === endpoint);
      if (existing) return state;

      return {
        endpoints: [...state.endpoints, {
          endpoint,
          state: ConnectionState.PENDING,
          healthScore: 100,
          lastConnected: null,
          averageLatency: null,
          failureCount: 0,
          successCount: 0,
        }],
      };
    }),

    // Optimized metrics updates
    updateMetrics: (updates) => set((state) => ({
      metrics: { ...state.metrics, ...updates },
    })),

    recordLatency: (latency) => set((state) => {
      latencyHistory.push(latency);
      if (latencyHistory.length > LATENCY_HISTORY_SIZE) {
        latencyHistory = latencyHistory.slice(-LATENCY_HISTORY_SIZE);
      }

      const metrics = { ...state.metrics };
      metrics.lastLatency = latency;

      if (metrics.avgLatency === null) {
        metrics.avgLatency = latency;
      } else {
        metrics.avgLatency = metrics.avgLatency * 0.9 + latency * 0.1;
      }

      if (metrics.minLatency === null || latency < metrics.minLatency) {
        metrics.minLatency = latency;
      }
      if (metrics.maxLatency === null || latency > metrics.maxLatency) {
        metrics.maxLatency = latency;
      }

      if (latencyHistory.length >= 10) {
        const sorted = [...latencyHistory].sort((a, b) => a - b);
        const p95Index = Math.floor(sorted.length * 0.95);
        const p99Index = Math.floor(sorted.length * 0.99);
        metrics.latencyP95 = sorted[p95Index];
        metrics.latencyP99 = sorted[p99Index];
      }

      metrics.lastMessageReceived = Date.now();

      return { metrics };
    }),

    incrementMetric: (key, value = 1) => set((state) => {
      const current = state.metrics[key];
      if (typeof current !== 'number') return state;

      return {
        metrics: {
          ...state.metrics,
          [key]: current + value,
        },
      };
    }),

    // Circuit Breaker Actions
    updateCircuitBreaker: (updates) => set((state) => {
      if (updates.state && updates.state !== state.circuitBreaker.state) {
        if (circuitBreakerTimer) {
          clearTimeout(circuitBreakerTimer);
          circuitBreakerTimer = null;
        }

        if (updates.state === CircuitBreakerState.OPEN && updates.nextRetryTime) {
          const timeUntilRetry = updates.nextRetryTime - Date.now();
          if (timeUntilRetry > 0) {
            circuitBreakerTimer = setTimeout(() => {
              const currentState = get();
              if (currentState.circuitBreaker.state === CircuitBreakerState.OPEN) {
                currentState.updateCircuitBreaker({
                  state: CircuitBreakerState.HALF_OPEN,
                  successCount: 0
                });
              }
            }, timeUntilRetry);
          }
        }
      }

      return {
        circuitBreaker: { ...state.circuitBreaker, ...updates },
      };
    }),

    recordCircuitBreakerFailure: () => set((state) => {
      const failures = state.circuitBreaker.failures + 1;
      const shouldOpen = failures >= state.circuitBreaker.threshold;

      const updates: Partial<CircuitBreakerInfo> = {
        failures,
        lastFailureTime: Date.now(),
      };

      if (shouldOpen && state.circuitBreaker.state !== CircuitBreakerState.OPEN) {
        updates.state = CircuitBreakerState.OPEN;
        updates.nextRetryTime = Date.now() + state.circuitBreaker.timeout;
        updates.successCount = 0;

        if (circuitBreakerTimer) {
          clearTimeout(circuitBreakerTimer);
        }
        circuitBreakerTimer = setTimeout(() => {
          const currentState = get();
          if (currentState.circuitBreaker.state === CircuitBreakerState.OPEN) {
            currentState.updateCircuitBreaker({
              state: CircuitBreakerState.HALF_OPEN,
              successCount: 0
            });
          }
        }, state.circuitBreaker.timeout);
      }

      return {
        circuitBreaker: {
          ...state.circuitBreaker,
          ...updates,
        },
      };
    }),

    recordCircuitBreakerSuccess: () => set((state) => {
      const successCount = state.circuitBreaker.successCount + 1;
      const updates: Partial<CircuitBreakerInfo> = {
        successCount,
      };

      if (state.circuitBreaker.state === CircuitBreakerState.HALF_OPEN && successCount >= 3) {
        updates.state = CircuitBreakerState.CLOSED;
        updates.failures = 0;
        updates.successCount = 0;
        updates.lastFailureTime = null;
        updates.nextRetryTime = null;
      }

      if (state.circuitBreaker.state === CircuitBreakerState.CLOSED) {
        updates.failures = 0;
      }

      return {
        circuitBreaker: {
          ...state.circuitBreaker,
          ...updates,
        },
      };
    }),

    resetCircuitBreaker: () => {
      if (circuitBreakerTimer) {
        clearTimeout(circuitBreakerTimer);
        circuitBreakerTimer = null;
      }
      set({ circuitBreaker: initialCircuitBreaker });
    },

    checkCircuitBreakerTimeout: () => set((state) => {
      if (state.circuitBreaker.state === CircuitBreakerState.OPEN &&
          state.circuitBreaker.nextRetryTime &&
          Date.now() >= state.circuitBreaker.nextRetryTime) {
        return {
          circuitBreaker: {
            ...state.circuitBreaker,
            state: CircuitBreakerState.HALF_OPEN,
            successCount: 0,
          },
        };
      }
      return state;
    }),

    // Subscription Actions
    setActiveTopics: (topics) => set({
      activeTopics: topics,
      subscriptions: {
        ...get().subscriptions,
        topics,
        lastUpdate: Date.now(),
      }
    }),

    addPendingSubscription: (topic) => set((state) => ({
      subscriptions: {
        ...state.subscriptions,
        pendingSubscriptions: [...new Set([...state.subscriptions.pendingSubscriptions, topic])],
      }
    })),

    confirmSubscription: (topic) => set((state) => ({
      activeTopics: [...new Set([...state.activeTopics, topic])],
      subscriptions: {
        ...state.subscriptions,
        topics: [...new Set([...state.subscriptions.topics, topic])],
        pendingSubscriptions: state.subscriptions.pendingSubscriptions.filter(t => t !== topic),
        failedSubscriptions: state.subscriptions.failedSubscriptions.filter(t => t !== topic),
        lastUpdate: Date.now(),
      }
    })),

    failSubscription: (topic) => set((state) => ({
      subscriptions: {
        ...state.subscriptions,
        pendingSubscriptions: state.subscriptions.pendingSubscriptions.filter(t => t !== topic),
        failedSubscriptions: [...new Set([...state.subscriptions.failedSubscriptions, topic])],
      }
    })),

    // Queue Actions
    updateQueueStats: (stats) => set((state) => ({
      messageQueue: { ...state.messageQueue, ...stats },
    })),

    updateOfflineQueue: (updates) => set((state) => ({
      offlineQueue: { ...state.offlineQueue, ...updates },
    })),

    // Diagnostics Actions
    updateDiagnostics: (diagnostics) => set({ diagnostics }),

    // Performance Actions
    updatePerformance: (updates) => set((state) => ({
      performance: { ...state.performance, ...updates },
    })),

    // Security Actions
    updateSecurity: (updates) => set((state) => ({
      security: { ...state.security, ...updates },
    })),

    // Error Actions
    setLastError: (error, code) => set((state) => {
      // When clearing error (null), don't add to history
      if (error === null) {
        return {
          lastError: null,
          metrics: {
            ...state.metrics,
            lastErrorMessage: null,
          },
        };
      }

      return {
        lastError: error,
        errorHistory: [
          ...state.errorHistory.slice(-49),
          { timestamp: Date.now(), error, code },
        ],
        metrics: {
          ...state.metrics,
          lastErrorCode: code ?? state.metrics.lastErrorCode,
          lastErrorMessage: error,
        },
      };
    }),

    clearErrors: () => set({ lastError: null, errorHistory: [] }),

    // Configuration Actions
    updateConfig: (updates) => set((state) => ({
      config: { ...state.config, ...updates },
    })),

    // Computed Getters
    isConnected: () => get().connectionState === ConnectionState.CONNECTED,

    canReconnect: () => {
      const state = get();

      if (!state.config.reconnectEnabled) {
        return false;
      }

      if (state.connectionState !== ConnectionState.DISCONNECTED &&
          state.connectionState !== ConnectionState.ERROR &&
          state.connectionState !== ConnectionState.RECONNECTING) {
        return false;
      }

      if (state.circuitBreaker.state === CircuitBreakerState.OPEN) {
        if (state.circuitBreaker.nextRetryTime &&
            Date.now() >= state.circuitBreaker.nextRetryTime) {
          state.updateCircuitBreaker({ state: CircuitBreakerState.HALF_OPEN });
          return true;
        }
        return false;
      }

      if (state.config.maxReconnectAttempts > 0 &&
          state.metrics.reconnectCount >= state.config.maxReconnectAttempts) {
        return false;
      }

      return true;
    },

    getConnectionQuality: () => {
      const { avgLatency, packetLoss } = get().metrics;
      if (!avgLatency) return ConnectionQuality.UNKNOWN;

      if (avgLatency < 50 && packetLoss < 0.1) return ConnectionQuality.EXCELLENT;
      if (avgLatency < 150 && packetLoss < 1) return ConnectionQuality.GOOD;
      if (avgLatency < 300 && packetLoss < 3) return ConnectionQuality.FAIR;
      return ConnectionQuality.POOR;
    },

    // Reset
    reset: () => {
      if (circuitBreakerTimer) {
        clearTimeout(circuitBreakerTimer);
        circuitBreakerTimer = null;
      }

      latencyHistory = [];

      set(initialState);
    },
  }))
);

// Selectors for common use cases
export const selectConnectionState = (state: WSEStore) => state.connectionState;
export const selectIsConnected = (state: WSEStore) => state.isConnected();
export const selectMetrics = (state: WSEStore) => state.metrics;
export const selectActiveTopics = (state: WSEStore) => state.activeTopics;
export const selectLastError = (state: WSEStore) => state.lastError;
export const selectCircuitBreaker = (state: WSEStore) => state.circuitBreaker;
export const selectQueueStats = (state: WSEStore) => state.messageQueue;
export const selectDiagnostics = (state: WSEStore) => state.diagnostics;
export const selectCanReconnect = (state: WSEStore) => state.canReconnect();
export const selectConnectionQuality = (state: WSEStore) => state.getConnectionQuality();

// Cleanup function for unmounting
export const cleanupWSEStore = () => {
  if (circuitBreakerTimer) {
    clearTimeout(circuitBreakerTimer);
    circuitBreakerTimer = null;
  }
};

// Expose store globally for dynamic staleTime checks (development only)
if (typeof window !== 'undefined') {
  try {
    const isDev = typeof process !== 'undefined' && process.env?.NODE_ENV === 'development';
    if (isDev) {
      (window as any).__WSE_STORE__ = useWSEStore;
    }
  } catch {
    // Ignore errors in environment detection
  }
}
