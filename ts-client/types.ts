// =============================================================================
// WebSocket Engine - Type Definitions
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
  /**
   * Consecutive failed attempts before giving up. -1 (the default) retries
   * forever; 0 disables automatic reconnection. The count resets whenever a
   * connection reaches server_ready.
   */
  maxAttempts: number;
  factor: number;
  jitter: boolean;
}

export interface SecurityConfig {
  /**
   * Require E2E encryption (ECDH P-256 + AES-GCM-256). When true the client
   * never sends application messages in plaintext: they are held until the
   * key exchange completes, and the connection is closed with an error when
   * the server does not confirm encryption.
   */
  encryptionEnabled: boolean;
  /**
   * The server's E2E identity public keys (base64, its
   * e2e_identity_public_key()): one of them must sign each handshake, so
   * nothing in between (a TLS-terminating proxy) can answer for the server.
   * Two during a key rotation.
   */
  serverKeys?: string[];
  /** Accept a server without an identity key: protects only against passive observers. */
  unauthenticated?: boolean;
  /** Speak the first E2E design, for servers before 3.0 (no server identity). */
  legacyE2e?: boolean;
  publicKey?: string;
  sessionKey?: CryptoKey;
  messageSignature: boolean;
}

export interface PerformanceConfig {
  /** Messages sent per flush step of the outgoing queue. */
  batchSize: number;
  /** Delay (ms) before a queued message is flushed when it could not go out at once. */
  batchTimeout: number;
  /** Capacity of the in-memory outgoing queue. */
  maxQueueSize: number;
  /**
   * Bound on the in-memory outgoing queue's estimated size in bytes (the
   * serialized messages). When it is reached, a new message pushes out
   * queued messages of lower priority or is dropped. 0 disables the bound.
   * Default 50 MiB.
   */
  memoryLimit: number;
  /**
   * Ask the server to compress messages larger than this many bytes (sent in
   * client_hello). 0 asks for no compression. Default 1024. Compression can
   * also be switched at runtime with setCompression().
   */
  compressionThreshold: number;
}

export interface OfflineConfig {
  enabled: boolean;
  maxSize: number;
  maxAge: number;
  persistToStorage: boolean;
}

export interface DiagnosticsConfig {
  /** Run the network monitor and refresh `diagnostics` every metricsInterval ms. */
  enabled: boolean;
  metricsInterval: number;
  /**
   * With diagnostics enabled, the share of handled messages (0..1) whose
   * handler run time is measured into `stats.handlerTimeAvg` / `handlerTimeMax`.
   * Default 0.1.
   */
  sampleRate: number;
  /**
   * Interval (ms) of the heartbeat ping that measures the round-trip time and
   * detects dead sockets; the adaptive quality strategy is re-evaluated with
   * every answer. Default 15000.
   */
  healthCheckInterval: number;
}

/**
 * Client-side outbound rate limit (token bucket). Control frames (heartbeat
 * ping, PONG, client_hello) are never limited. A message that hits the limit
 * stays queued and is sent when tokens are available.
 */
export interface RateLimitConfig {
  /** Burst size in messages (default 1000). */
  capacity?: number;
  /** Sustained rate in messages per second (default 100). */
  refillRate?: number;
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
  /** Offset gaps detected on recovery-stamped topics (see the `wseGap` event). */
  gapsDetected: number;
  /** Messages known to be missing across all gaps (when the range is known). */
  messagesMissed: number;
  /** Mean run time (ms) of sampled message handlers (diagnostics.sampleRate). */
  handlerTimeAvg: number | null;
  /** Longest sampled handler run time (ms). */
  handlerTimeMax: number | null;
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
  c?: 'WSE' | 'S' | 'U';
  // Recovery stamp on topic publications: topic / epoch (8-hex) / offset.
  // Drives idempotent dedup, gap detection, and recovery from last-seen.
  tp?: string;
  e?: string;
  o?: number;
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
  /** The complete wire message (keeps cid/enc and the original id). */
  message?: WSMessage;
  /** Estimated size in bytes (serialized length), counted against memoryLimit. */
  bytes?: number;
  /** Epoch ms after which the message is dropped instead of sent (MessageOptions.ttl). */
  expiresAt?: number;
}

export interface MessageOptions {
  priority?: MessagePriority;
  /**
   * Refuse to send this message unless the session has E2E encryption
   * enabled (security.encryptionEnabled). It never falls back to plaintext.
   */
  encrypted?: boolean;
  /** Persist to IndexedDB while disconnected and replay after reconnecting. */
  offline?: boolean;
  correlation_id?: string;
  /**
   * Milliseconds the message stays worth sending. A message still queued
   * (in memory or offline) after that is dropped instead of sent late.
   */
  ttl?: number;
}

/**
 * Result of sendMessage():
 * - 'sent'    handed to the open socket (encrypted when encryption is on)
 * - 'queued'  held and sent automatically once the connection is ready
 * - 'dropped' rejected (outgoing queue full, encryption required but not
 *             enabled, or the message could not be serialised)
 */
export type SendStatus = 'sent' | 'queued' | 'dropped';

/**
 * Detail of the `wseGap` window event: messages of a recovery-stamped topic
 * were skipped. Delivery continues with the next available message; the
 * application can request a snapshot to resync its state.
 */
export interface WSEGapEvent {
  topic: string;
  epoch: string;
  /** First missing offset, or null when unknown (epoch changed). */
  fromOffset: number | null;
  /** Last missing offset (inclusive), or null when unknown. */
  toOffset: number | null;
  /** Number of missing messages, or null when unknown. */
  missed: number | null;
  reason: 'offset_gap' | 'not_recovered' | 'epoch_changed';
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
  /** Estimated size of the queued messages in bytes. */
  bytes: number;
  /** Bound on `bytes`; 0 means none. */
  memoryLimit: number;
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

export interface TopicRecoveryPosition {
  epoch: string;
  offset: number;
}

export interface SubscriptionInfo {
  topics: string[];
  pendingSubscriptions: string[];
  failedSubscriptions: string[];
  lastUpdate: number | null;
  recoveryState: Record<string, TopicRecoveryPosition>;
  serverRecoveryEnabled: boolean;
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
    /** Mirror of reconnection.maxAttempts (-1 = infinite); the ConnectionManager owns the policy. */
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
  /** Estimated size of the queued messages in bytes. */
  bytes: number;
  capacity: number;
  /** Bound on `bytes`; 0 means none. */
  memoryLimit: number;
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

/**
 * Value returned by useWSE(). Every function is stable (same identity on
 * every render) and acts on the live connection, so it is safe in effect
 * dependency lists.
 */
export interface UseWSEReturn {
  subscribe: (topics: string[], options?: { recover?: boolean }) => void;
  unsubscribe: (topics: string[]) => void;
  forceReconnect: () => void;
  changeEndpoint: (endpoint: string) => void;
  sendMessage: (type: string, payload: any, options?: MessageOptions) => SendStatus;
  sendBatch: (messages: Array<{ type: string; payload: any; options?: MessageOptions }>) => SendStatus[];
  /** Connection metrics, refreshed at most once per second. */
  stats: ConnectionMetrics;
  activeTopics: string[];
  connectionHealth: ConnectionState;
  diagnostics: NetworkDiagnostics | null;
  downloadDiagnostics: () => void;
  clearOfflineQueue: () => Promise<void>;
  getQueueSize: () => number;
  requestSnapshot: (topics?: string[]) => void;
  debugHandlers: () => void;
  /**
   * Ask the server to compress (true) or not compress (false) the messages it
   * sends this client. Applies to the current connection at once and to
   * every later one.
   */
  setCompression: (enabled: boolean) => void;
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
