// =============================================================================
// WebSocket Event System - Type Definitions
// =============================================================================

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

export enum MessagePriority {
  CRITICAL = 10,
  HIGH = 8,
  NORMAL = 5,
  LOW = 3,
  BACKGROUND = 1
}

export enum ConnectionState {
  CONNECTING = "connecting",
  CONNECTED = "connected",
  RECONNECTING = "reconnecting",
  DISCONNECTED = "disconnected",
  ERROR = "error",
  PENDING = "pending",
  DEGRADED = "degraded"
}

export enum ConnectionQuality {
  EXCELLENT = "excellent",
  GOOD = "good",
  FAIR = "fair",
  POOR = "poor",
  UNKNOWN = "unknown"
}

export enum CircuitBreakerState {
  CLOSED = "closed",
  OPEN = "open",
  HALF_OPEN = "half-open"
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

export interface WSEConfig {
  endpoints: string[];
  reconnection: ReconnectionStrategy;
  security: SecurityConfig;
  performance: PerformanceConfig;
  offline: OfflineConfig;
  diagnostics: DiagnosticsConfig;
}

export interface ReconnectionStrategy {
  mode: 'exponential' | 'linear' | 'fibonacci' | 'adaptive';
  baseDelay: number;
  maxDelay: number;
  maxAttempts: number;
  factor: number;
  jitter: boolean;
}

export interface SecurityConfig {
  encryptionEnabled: boolean;
  publicKey?: string;
  sessionKey?: CryptoKey;
  messageSignature: boolean;
}

export interface PerformanceConfig {
  batchSize: number;
  batchTimeout: number;
  compressionThreshold: number;
  maxQueueSize: number;
  memoryLimit: number;
}

export interface OfflineConfig {
  enabled: boolean;
  maxSize: number;
  maxAge: number;
  persistToStorage: boolean;
}

export interface DiagnosticsConfig {
  enabled: boolean;
  sampleRate: number;
  metricsInterval: number;
  healthCheckInterval: number;
}

// ---------------------------------------------------------------------------
// Connection & Metrics
// ---------------------------------------------------------------------------

export interface ConnectionMetrics {
  messagesReceived: number;
  messagesSent: number;
  messagesQueued: number;
  messagesDropped: number;
  compressionRatio: number;
  compressionHits: number;
  reconnectCount: number;
  connectionAttempts: number;
  successfulConnections: number;
  failedConnections: number;
  lastLatency: number | null;
  avgLatency: number | null;
  minLatency: number | null;
  maxLatency: number | null;
  latencyP95: number | null;
  latencyP99: number | null;
  bytesReceived: number;
  bytesSent: number;
  bandwidth: number;
  messageRate: number;
  connectedSince: number | null;
  lastMessageReceived: number | null;
  lastMessageSent: number | null;
  lastHealthCheck: number | null;
  lastErrorCode: number | null;
  lastErrorMessage: string | null;
  authFailures: number;
  protocolErrors: number;
  networkJitter: number;
  packetLoss: number;
}

export interface ConnectionInfo {
  endpoint: string;
  state: ConnectionState;
  healthScore: number;
  lastConnected: number | null;
  averageLatency: number | null;
  failureCount: number;
  successCount: number;
}

export interface CircuitBreakerInfo {
  state: CircuitBreakerState;
  failures: number;
  lastFailureTime: number | null;
  successCount: number;
  nextRetryTime: number | null;
  threshold: number;
  timeout: number;
}

// ---------------------------------------------------------------------------
// Network Diagnostics
// ---------------------------------------------------------------------------

export interface NetworkDiagnostics {
  quality: ConnectionQuality;
  stability: number;
  jitter: number;
  packetLoss: number;
  roundTripTime: number;
  suggestions: string[];
  lastAnalysis: number | null;
}

// ---------------------------------------------------------------------------
// Messages & Events
// ---------------------------------------------------------------------------

export interface WSMessage {
  id?: string;
  t: string;
  p: any;
  v?: number;
  seq?: number;
  ts?: string;
  cid?: string;
  pri?: number;
  cmp?: boolean;
  enc?: boolean;
  sig?: string;
  event_version?: number;
  latency_ms?: number;
  wse_processing_ms?: number;
  trace_id?: string;
  _msg_cat?: 'WSE' | 'S' | 'U';
}

export interface QueuedMessage {
  id: string;
  type: string;
  payload: any;
  priority: number;
  timestamp: number;
  retries: number;
  encrypted?: boolean;
  correlation_id?: string;
}

export interface MessageOptions {
  priority?: MessagePriority;
  encrypted?: boolean;
  offline?: boolean;
  correlation_id?: string;
  ttl?: number;
}

// ---------------------------------------------------------------------------
// Queue Statistics
// ---------------------------------------------------------------------------

export interface QueueStats {
  size: number;
  capacity: number;
  utilizationPercent: number;
  oldestMessageAge: number | null;
  priorityDistribution: Record<number, number>;
  processingRate: number;
  backpressure: boolean;
}

export interface OfflineQueueStats {
  enabled: boolean;
  size: number;
  capacity: number;
  oldestMessageAge: number | null;
  persistedToStorage: boolean;
}

// ---------------------------------------------------------------------------
// Connection Pool
// ---------------------------------------------------------------------------

export interface ConnectionPoolConfig {
  endpoints: string[];
  maxPerEndpoint: number;
  healthCheckInterval: number;
}

export interface ConnectionPoolStats {
  endpoints: EndpointStats[];
  activeEndpoint: string | null;
  preferredEndpoint: string | null;
  totalConnections: number;
}

export interface EndpointStats {
  url: string;
  healthScore: number;
  connectionCount: number;
  isActive: boolean;
  isPreferred: boolean;
  lastFailure?: number;
  lastSuccess?: number;
}

// ---------------------------------------------------------------------------
// Adaptive Reconnection
// ---------------------------------------------------------------------------

export interface ConnectionHistory {
  timestamp: number;
  duration: number;
  success: boolean;
  endpoint: string;
}

export interface AdaptiveReconnectionStats {
  currentDelay: number;
  attempts: number;
  history: ConnectionHistory[];
  nextRetryTime: number | null;
}

// ---------------------------------------------------------------------------
// Security
// ---------------------------------------------------------------------------

export interface SecurityInfo {
  encryptionEnabled: boolean;
  encryptionAlgorithm: string | null;
  messageSigningEnabled: boolean;
  sessionKeyRotation: number | null;
  lastKeyRotation: number | null;
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

export interface SubscriptionInfo {
  topics: string[];
  pendingSubscriptions: string[];
  failedSubscriptions: string[];
  lastUpdate: number | null;
}

// ---------------------------------------------------------------------------
// Store States
// ---------------------------------------------------------------------------

export interface WSEState {
  connectionState: ConnectionState;
  activeEndpoint: string | null;
  preferredEndpoint: string | null;
  endpoints: ConnectionInfo[];
  metrics: ConnectionMetrics;
  diagnostics: NetworkDiagnostics | null;
  performance: PerformanceMetrics;
  circuitBreaker: CircuitBreakerInfo;
  messageQueue: QueueStats;
  offlineQueue: OfflineQueueStats;
  security: SecurityInfo;
  subscriptions: SubscriptionInfo;
  activeTopics: string[];
  lastError: string | null;
  errorHistory: Array<{
    timestamp: number;
    error: string;
    code?: number;
  }>;
  config: {
    reconnectEnabled: boolean;
    maxReconnectAttempts: number;
    compressionEnabled: boolean;
    batchingEnabled: boolean;
    offlineModeEnabled: boolean;
    healthCheckInterval: number;
    metricsInterval: number;
  };
  sequence: number;
}

export interface MessageQueueState {
  messages: QueuedMessage[];
  size: number;
  capacity: number;
  processing: boolean;
  stats: QueueStats;
}

export interface PerformanceMetrics {
  cpuUsage: number;
  memoryUsage: number;
  gcPauseTime: number;
  eventLoopLag: number;
}

// ---------------------------------------------------------------------------
// System Events
// ---------------------------------------------------------------------------

export interface SystemStatus {
  cpu_usage?: number;
  memory_usage?: number;
  api_status?: string;
  datafeed_status?: string;
  active_connections?: number;
  event_queue_size?: number;
  adapter_statuses?: Record<string, string>;
  module_statuses?: Record<string, Record<string, string>>;
  event_bus_health?: Record<string, any>;
  circuit_breakers?: Record<string, string>;
}

// ---------------------------------------------------------------------------
// Client Features
// ---------------------------------------------------------------------------

export interface ClientFeatures {
  compression: boolean;
  encryption: boolean;
  batching: boolean;
  priority_queue: boolean;
  circuit_breaker: boolean;
  offline_queue: boolean;
  message_signing: boolean;
  health_check: boolean;
  metrics: boolean;
}

export interface ClientCapabilities {
  protocol_version: number;
  client_version: string;
  features: ClientFeatures;
  limits: {
    max_message_size: number;
    max_queue_size: number;
    rate_limit: number;
    max_subscriptions: number;
  };
}

// ---------------------------------------------------------------------------
// Hook Return Types
// ---------------------------------------------------------------------------

export interface UseWSEReturn {
  subscribe: (topics: string[]) => void;
  unsubscribe: (topics: string[]) => void;
  forceReconnect: () => void;
  changeEndpoint: (endpoint: string) => void;
  sendMessage: (type: string, payload: any, options?: MessageOptions) => void;
  sendBatch: (messages: Array<{ type: string; payload: any }>) => void;
  stats: ConnectionMetrics;
  activeTopics: string[];
  connectionHealth: ConnectionState;
  diagnostics: NetworkDiagnostics | null;
  setCompression: (enabled: boolean) => void;
  downloadDiagnostics: () => void;
  clearOfflineQueue: () => Promise<void>;
  getQueueSize: () => number;
  requestSnapshot: () => void;
  debugHandlers: () => void;
}

// ---------------------------------------------------------------------------
// Service Interfaces
// ---------------------------------------------------------------------------

export interface IConnectionManager {
  connect(token: string, topics: string[], options?: ConnectionOptions): Promise<void>;
  disconnect(): void;
  send(message: WSMessage): boolean;
  isConnected(): boolean;
  changeEndpoint(endpoint: string): void;
}

export interface IMessageProcessor {
  processIncoming(data: string | ArrayBuffer): Promise<void>;
  processBatch(): Promise<WSMessage[]>;
  queueOutgoing(message: WSMessage, priority: MessagePriority): void;
  destroy(): void;
}

export interface INetworkMonitor {
  recordPacketSent(): void;
  recordPacketReceived(): void;
  recordBytes(bytes: number): void;
  recordLatency(latency: number): void;
  analyze(): NetworkDiagnostics;
  destroy(): void;
}

export interface IOfflineQueue {
  initialize(): Promise<void>;
  enqueue(message: any): Promise<void>;
  getAll(): Promise<any[]>;
  clear(): Promise<void>;
  getStats(): OfflineQueueStats;
  destroy(): void;
}

export interface IConnectionPool {
  getBestEndpoint(): string;
  recordSuccess(endpoint: string): void;
  recordFailure(endpoint: string): void;
  addConnection(endpoint: string, ws: WebSocket): void;
  removeConnection(endpoint: string, ws: WebSocket): void;
  getActiveConnection(): WebSocket | null;
  setActiveEndpoint(endpoint: string): void;
  setPreferredEndpoint(endpoint: string): void;
  hasEndpoint(endpoint: string): boolean;
  addEndpoint(endpoint: string): void;
  getHealthScores(): Record<string, number>;
  getEndpointStats(): EndpointStats[];
  getActiveEndpoint(): string | null;
  getPreferredEndpoint(): string | null;
  closeAll(): void;
  destroy(): void;
}

export interface IAdaptiveReconnection {
  recordAttempt(endpoint: string, success: boolean, duration: number): void;
  getNextDelay(): number;
  getCurrentDelay(): number;
  getHistory(): ConnectionHistory[];
  reset(): void;
  destroy(): void;
}

// ---------------------------------------------------------------------------
// Connection Options
// ---------------------------------------------------------------------------

export interface ConnectionOptions {
  compression?: boolean;
  encryption?: boolean;
  protocol_version?: number;
  client_version?: string;
  features?: Partial<ClientFeatures>;
}

// ---------------------------------------------------------------------------
// Error Types
// ---------------------------------------------------------------------------

export interface WSError {
  code: string;
  message: string;
  details?: Record<string, any>;
  severity: 'warning' | 'error' | 'critical';
  recoverable: boolean;
  retry_after?: number;
  suggested_action?: string;
}

// ---------------------------------------------------------------------------
// Rate Limiting
// ---------------------------------------------------------------------------

export interface RateLimitInfo {
  limit: number;
  window: number;
  current_usage: number;
  retry_after?: number;
}

// ---------------------------------------------------------------------------
// Type Aliases
// ---------------------------------------------------------------------------

export type DiagnosticsData = NetworkDiagnostics;

export type {
  WSEConfig as Config,
  UseWSEReturn as WSEHook,
  WSMessage as Message,
  ConnectionState as State,
  ConnectionMetrics as Metrics,
  NetworkDiagnostics as Diagnostics,
};
