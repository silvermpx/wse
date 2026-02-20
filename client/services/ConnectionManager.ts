// =============================================================================
// WebSocket Event System - Connection Manager
// =============================================================================

import {
  ConnectionState,
  ConnectionQuality,
  WSMessage,
  ReconnectionStrategy,
  MessagePriority,
} from '../types';
import { useWSEStore } from '../stores/useWSEStore';
import { logger } from '../utils/logger';
import { RateLimiter } from './RateLimiter';
import { ConnectionPool } from './ConnectionPool';
import { CircuitBreaker } from '../utils/circuitBreaker';
import { WS_PROTOCOL_VERSION, WS_CLIENT_VERSION, FEATURES } from '../constants';
import { AdaptiveQualityManager } from './AdaptiveQualityManager';

/**
 * Configuration for pluggable auth token refresh.
 * Applications provide their own refresh logic via this callback.
 */
export interface ConnectionManagerConfig {
  /**
   * Optional callback to refresh the auth token.
   * Called periodically and on auth failure. Should throw on failure.
   */
  refreshAuthToken?: () => Promise<void>;

  /**
   * Optional QueryClient-like object for AdaptiveQualityManager integration.
   */
  queryClient?: {
    setDefaultOptions: (options: { queries: Record<string, any> }) => void;
    invalidateQueries: () => void;
  } | null;
}

const DEFAULT_MAX_RECONNECT_ATTEMPTS = 10;
const MAX_TOKEN_REFRESH_ATTEMPTS = 3;

// Cached TextEncoder for efficient byte size calculation
const textEncoder = new TextEncoder();

export class ConnectionManager {
  private ws: WebSocket | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private reconnectAttempts = 0;
  private heartbeatInterval: NodeJS.Timeout | null = null;
  private token: string | null = null;
  private rateLimiter: RateLimiter;
  private serverReady = false;
  private clientHelloSent = false;
  private connectionId: string | null = null;

  // Token management
  private tokenRefreshTimer: NodeJS.Timeout | null = null;
  private tokenRefreshAttempts = 0;
  private usesCookieAuth = false;

  // Client hello retry tracking
  private clientHelloRetries = 0;
  private readonly MAX_CLIENT_HELLO_RETRIES = 10;

  // Connection pool
  private connectionPool: ConnectionPool;
  private currentEndpoint: string | null = null;

  // State tracking
  private isDestroyed = false;
  private isConnecting = false;
  private lastConnectionAttempt = 0;
  private consecutiveFailures = 0;

  // Circuit breaker for connection attempts
  private connectionCircuitBreaker: CircuitBreaker;

  // Adaptive quality management
  private adaptiveQuality: AdaptiveQualityManager;

  // Store server ready details temporarily
  private serverReadyDetails: any = null;
  private serverReadyProcessed = false;

  // Pluggable auth refresh
  private refreshAuthTokenFn: (() => Promise<void>) | null = null;

  constructor(
    private endpoints: string[],
    private reconnectionStrategy: ReconnectionStrategy,
    private onMessage: (data: string | ArrayBuffer) => void,
    private onStateChange: (state: ConnectionState) => void,
    private onServerReady?: (details: any) => void,
    config?: ConnectionManagerConfig
  ) {
    this.rateLimiter = new RateLimiter(1000, 100, 1000);

    this.connectionPool = new ConnectionPool({
      endpoints: this.endpoints,
      maxPerEndpoint: 1,
      healthCheckInterval: 30000,
      loadBalancingStrategy: 'weighted-random',
    });

    this.connectionCircuitBreaker = new CircuitBreaker({
      failureThreshold: 5,
      resetTimeout: 60000,
      successThreshold: 3,
    });

    // Initialize adaptive quality manager (QueryClient is optional)
    this.adaptiveQuality = new AdaptiveQualityManager(config?.queryClient ?? null);

    // Store pluggable auth refresh callback
    this.refreshAuthTokenFn = config?.refreshAuthToken ?? null;

    logger.info('[ConnectionManager] Initialized with endpoints:', this.endpoints);
  }

  // ---------------------------------------------------------------------------
  // Connection Management
  // ---------------------------------------------------------------------------

  async connect(token: string, topics: string[] = []): Promise<void> {
    if (!token) {
      logger.error('[ConnectionManager] No auth signal provided');
      throw new Error('Authentication required');
    }

    // Check if using cookie auth
    this.usesCookieAuth = token === 'cookie';

    if (this.isDestroyed) {
      throw new Error('ConnectionManager has been destroyed');
    }

    if (this.isConnecting) {
      logger.warn('[ConnectionManager] Connection already in progress');
      return;
    }

    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      logger.info('[ConnectionManager] Already connected');
      return;
    }

    if (this.ws) {
      logger.info('[ConnectionManager] Disconnecting existing connection');
      this.disconnect();
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    const now = Date.now();
    const timeSinceLastAttempt = now - this.lastConnectionAttempt;
    const minDelay = this.calculateMinConnectionDelay();

    if (timeSinceLastAttempt < minDelay) {
      const waitTime = minDelay - timeSinceLastAttempt;
      logger.info(`[ConnectionManager] Rate limiting connection attempt, waiting ${waitTime}ms`);
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }

    this.token = token;
    this.serverReady = false;
    this.clientHelloSent = false;
    this.clientHelloRetries = 0;
    this.tokenRefreshAttempts = 0;
    this.lastConnectionAttempt = Date.now();

    // Setup token refresh
    this.setupTokenRefresh();

    try {
      await this.connectionCircuitBreaker.execute(async () => {
        const endpoint = this.selectBestEndpoint();

        if (!endpoint) {
          throw new Error('No available endpoints');
        }

        await this.connectToEndpoint(endpoint, topics);
      });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);

      if (errorMessage === 'Circuit breaker is OPEN') {
        logger.error('[ConnectionManager] Connection circuit breaker is OPEN - too many failures');
        this.onStateChange(ConnectionState.ERROR);
        const store = useWSEStore.getState();
        store.setLastError('Connection circuit breaker open - too many failures');
      }
      throw error;
    }
  }

  private calculateMinConnectionDelay(): number {
    if (this.consecutiveFailures === 0) return 0;

    const baseDelay = 1000;
    const maxDelay = 60000;
    const delay = Math.min(baseDelay * Math.pow(2, this.consecutiveFailures - 1), maxDelay);

    return delay;
  }

  private async connectToEndpoint(endpoint: string, topics: string[]): Promise<void> {
    if (this.isDestroyed) return;

    logger.info(`[ConnectionManager] Connecting to endpoint: ${endpoint}`);
    logger.info(`[ConnectionManager] Topics: ${topics.join(', ')}`);

    this.isConnecting = true;

    const store = useWSEStore.getState();

    try {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        logger.info('[ConnectionManager] WebSocket already open, skipping connection');
        this.isConnecting = false;
        return;
      }

      if (this.ws) {
        logger.info('[ConnectionManager] Closing existing WebSocket connection');
        this.ws.close();
        this.ws = null;
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      this.onStateChange(ConnectionState.CONNECTING);
      this.currentEndpoint = endpoint;

      const url = new URL(endpoint);
      if (topics.length > 0) {
        url.searchParams.set('topics', topics.join(','));
      }
      url.searchParams.set('client_version', WS_CLIENT_VERSION);
      url.searchParams.set('protocol_version', String(WS_PROTOCOL_VERSION));
      url.searchParams.set('compression', String(FEATURES.COMPRESSION));
      url.searchParams.set('encryption', String(FEATURES.ENCRYPTION));

      logger.info(`[ConnectionManager] WebSocket URL: ${url.toString()}`);

      const connectionStart = Date.now();
      const ws = new WebSocket(url.toString());
      ws.binaryType = 'arraybuffer';

      this.ws = ws;
      this.setupWebSocketHandlers(ws, endpoint);

      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Connection timeout'));
        }, 10000);

        const handleOpen = () => {
          clearTimeout(timeout);
          ws.removeEventListener('open', handleOpen);
          ws.removeEventListener('error', handleError);
          ws.removeEventListener('close', handleClose);
          resolve();
        };

        const handleError = (event: Event) => {
          clearTimeout(timeout);
          ws.removeEventListener('open', handleOpen);
          ws.removeEventListener('error', handleError);
          ws.removeEventListener('close', handleClose);
          reject(new Error('Connection failed'));
        };

        const handleClose = (event: CloseEvent) => {
          clearTimeout(timeout);
          ws.removeEventListener('open', handleOpen);
          ws.removeEventListener('error', handleError);
          ws.removeEventListener('close', handleClose);

          if (event.code === 4401 || event.code === 4403 || event.code === 1008) {
            reject(new Error(`Authentication failed: ${event.reason || 'Invalid token'}`));
          } else {
            reject(new Error(`Connection closed: ${event.reason || 'Unknown reason'}`));
          }
        };

        ws.addEventListener('open', handleOpen);
        ws.addEventListener('error', handleError);
        ws.addEventListener('close', handleClose);
      });

      const connectionTime = Date.now() - connectionStart;

      this.onStateChange(ConnectionState.CONNECTED);
      store.setConnectionState(ConnectionState.CONNECTED);
      store.updateMetrics({ connectedSince: Date.now() });

      this.connectionPool.recordSuccess(endpoint, connectionTime);
      this.connectionPool.setActiveEndpoint(endpoint);
      this.connectionCircuitBreaker.recordSuccess();

      this.reconnectAttempts = 0;
      this.consecutiveFailures = 0;

      this.startHeartbeat();

      logger.info(`[ConnectionManager] Connected successfully to ${endpoint} in ${connectionTime}ms`);

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error('[ConnectionManager] Connection failed:', errorMessage);

      this.consecutiveFailures++;
      this.connectionPool.recordFailure(endpoint);
      this.connectionCircuitBreaker.recordFailure();

      if (this.ws) {
        this.ws.close();
        this.ws = null;
      }

      if (errorMessage.includes('Authentication failed')) {
        this.token = null;
        store.setLastError('Authentication failed', 401);
        store.incrementMetric('authFailures');

        window.dispatchEvent(new CustomEvent('wseAuthFailed', {
          detail: { reason: 'Backend rejected token' }
        }));
      }

      throw error;
    } finally {
      this.isConnecting = false;
    }
  }

  disconnect(): void {
    logger.info('[ConnectionManager] Disconnect called');

    this.serverReady = false;
    this.clientHelloSent = false;
    this.connectionId = null;
    this.isConnecting = false;
    this.serverReadyProcessed = false;
    this.serverReadyDetails = null;

    this.adaptiveQuality.reset();

    this.clearAllTimers();

    if (this.ws && this.currentEndpoint) {
      try {
        this.connectionPool.removeConnection(this.currentEndpoint, this.ws);
      } catch (e) {
        logger.error('[ConnectionManager] Error removing from connection pool:', e);
      }
    }

    if (this.ws) {
      if (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING) {
        logger.info('[ConnectionManager] Closing WebSocket connection');
        this.ws.close(1000, 'Client disconnect');
      }
      this.ws = null;
    }

    this.currentEndpoint = null;
    this.onStateChange(ConnectionState.DISCONNECTED);

    logger.info('[ConnectionManager] Disconnected');
  }

  private clearAllTimers(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }

    if (this.tokenRefreshTimer) {
      clearTimeout(this.tokenRefreshTimer);
      this.tokenRefreshTimer = null;
    }
  }

  // ---------------------------------------------------------------------------
  // Token Management
  // ---------------------------------------------------------------------------

  private setupTokenRefresh(): void {
    if (this.isDestroyed || !this.usesCookieAuth || !this.refreshAuthTokenFn) return;

    if (this.tokenRefreshTimer) {
      clearTimeout(this.tokenRefreshTimer);
      this.tokenRefreshTimer = null;
    }

    const REFRESH_INTERVAL = 10 * 60 * 1000; // 10 minutes

    this.tokenRefreshTimer = setTimeout(() => {
      this.refreshTokenWithRetry().catch(error => {
        logger.error('[ConnectionManager] Token refresh with retry failed:', error);
      });
    }, REFRESH_INTERVAL);

    logger.info(`[ConnectionManager] Token refresh scheduled in ${REFRESH_INTERVAL / 1000}s`);
  }

  private async refreshTokenWithRetry(): Promise<void> {
    if (this.isDestroyed || !this.refreshAuthTokenFn) return;

    if (this.tokenRefreshAttempts >= MAX_TOKEN_REFRESH_ATTEMPTS) {
      logger.error('[ConnectionManager] Max token refresh attempts exceeded');
      this.disconnect();
      window.dispatchEvent(new CustomEvent('authTokenExpired', {
        detail: {
          error: 'Max token refresh attempts exceeded',
          attempts: this.tokenRefreshAttempts
        }
      }));
      return;
    }

    for (let attempt = 0; attempt < MAX_TOKEN_REFRESH_ATTEMPTS; attempt++) {
      try {
        await this.refreshToken();
        this.tokenRefreshAttempts = 0;
        return;
      } catch (error) {
        this.tokenRefreshAttempts++;
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.warn(`[ConnectionManager] Token refresh attempt ${attempt + 1}/${MAX_TOKEN_REFRESH_ATTEMPTS} failed:`, errorMessage);

        if (attempt < MAX_TOKEN_REFRESH_ATTEMPTS - 1) {
          const delay = Math.min(1000 * Math.pow(2, attempt), 10000);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }

    logger.error('[ConnectionManager] All token refresh attempts failed');
    this.disconnect();
    window.dispatchEvent(new CustomEvent('authTokenExpired', {
      detail: {
        error: 'Token refresh failed after all attempts',
        attempts: this.tokenRefreshAttempts
      }
    }));
  }

  private async refreshToken(): Promise<void> {
    if (this.isDestroyed || !this.refreshAuthTokenFn) return;

    try {
      logger.info('[ConnectionManager] Refreshing token');
      await this.refreshAuthTokenFn();
      logger.info('[ConnectionManager] Token refreshed');

      // Schedule next refresh
      this.setupTokenRefresh();

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error('[ConnectionManager] Token refresh failed:', errorMessage);

      this.onStateChange(ConnectionState.ERROR);
      const store = useWSEStore.getState();
      store.setLastError('Authentication token refresh failed', 401);

      throw error;
    }
  }

  private async reconnectWithNewToken(): Promise<void> {
    if (this.isDestroyed) return;

    logger.info('[ConnectionManager] Reconnecting after token refresh');

    const store = useWSEStore.getState();
    const topics = store.activeTopics;

    if (this.ws) {
      this.ws.close(1000, 'Token refresh');
      this.ws = null;
    }

    await new Promise(resolve => setTimeout(resolve, 100));

    this.consecutiveFailures = 0;
    this.reconnectAttempts = 0;

    try {
      await this.connect('cookie', topics);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error('[ConnectionManager] Failed to reconnect after token refresh:', errorMessage);
      throw error;
    }
  }

  // ---------------------------------------------------------------------------
  // Reconnection with Circuit Breaker
  // ---------------------------------------------------------------------------

  private async reconnect(): Promise<void> {
    if (this.isDestroyed) return;

    const store = useWSEStore.getState();

    const maxAttempts = this.reconnectionStrategy.maxAttempts === -1
      ? DEFAULT_MAX_RECONNECT_ATTEMPTS
      : this.reconnectionStrategy.maxAttempts;

    if (!store.canReconnect()) {
      logger.warn('[ConnectionManager] Cannot reconnect - circuit breaker open or max attempts reached');
      return;
    }

    if (this.reconnectAttempts >= maxAttempts) {
      logger.error('[ConnectionManager] Max reconnection attempts reached');
      this.onStateChange(ConnectionState.ERROR);
      store.setLastError('Max reconnection attempts reached');
      return;
    }

    if (!this.connectionCircuitBreaker.canExecute()) {
      logger.warn('[ConnectionManager] Connection circuit breaker is OPEN, skipping reconnect');
      this.onStateChange(ConnectionState.ERROR);
      store.setLastError('Connection circuit breaker open');
      return;
    }

    try {
      if (!this.token) {
        throw new Error('No token available for reconnection');
      }

      const endpoint = this.selectBestEndpoint();
      if (!endpoint) {
        throw new Error('No available endpoints for reconnection');
      }

      await this.connectToEndpoint(endpoint, store.activeTopics);
      store.incrementMetric('reconnectCount');

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error('[ConnectionManager] Reconnection failed:', errorMessage);

      if (this.reconnectAttempts < maxAttempts && !this.isDestroyed) {
        this.scheduleReconnect();
      } else {
        this.onStateChange(ConnectionState.ERROR);
        store.setLastError('Max reconnection attempts reached');
      }
    }
  }

  private scheduleReconnect(): void {
    if (this.isDestroyed) return;

    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
    }

    const baseDelay = this.calculateReconnectDelay();
    const minDelay = this.calculateMinConnectionDelay();
    const delay = Math.max(baseDelay, minDelay);
    const finalDelay = Math.min(delay, 300000); // Cap at 5 minutes

    logger.info(`[ConnectionManager] Scheduling reconnect in ${finalDelay}ms (attempt ${this.reconnectAttempts + 1})`);

    this.reconnectTimer = setTimeout(() => {
      if (!this.isDestroyed) {
        this.reconnectAttempts++;
        this.lastConnectionAttempt = Date.now();
        this.reconnect().catch(error => {
          const errorMessage = error instanceof Error ? error.message : String(error);
          logger.error('[ConnectionManager] Reconnect failed:', errorMessage);
        });
      }
    }, finalDelay);
  }

  private calculateReconnectDelay(): number {
    const { mode, baseDelay, maxDelay, factor, jitter } = this.reconnectionStrategy;
    let delay: number;

    switch (mode) {
      case 'exponential':
        delay = Math.min(maxDelay, baseDelay * Math.pow(factor, this.reconnectAttempts));
        break;

      case 'linear':
        delay = Math.min(maxDelay, baseDelay + (this.reconnectAttempts * 1000));
        break;

      case 'fibonacci': {
        const fib = (n: number): number => {
          let a = 0, b = 1;
          for (let i = 0; i < n; i++) { [a, b] = [b, a + b]; }
          return a;
        };
        delay = Math.min(maxDelay, baseDelay * fib(Math.min(this.reconnectAttempts, 10)));
        break;
      }

      case 'adaptive':
      default:
        delay = Math.min(maxDelay, baseDelay * Math.pow(1.5, this.reconnectAttempts));
        break;
    }

    if (jitter) {
      const jitterAmount = delay * 0.2 * (Math.random() - 0.5);
      delay = Math.max(0, delay + jitterAmount);
    }

    return delay;
  }

  // ---------------------------------------------------------------------------
  // Message Handling
  // ---------------------------------------------------------------------------

  send(message: WSMessage): boolean {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN || this.isDestroyed) {
      logger.debug('[ConnectionManager] Cannot send - WebSocket not open');
      return false;
    }

    const isCriticalMessage = ['subscription_update', 'sync_request', 'client_hello'].includes(message.t);

    if (isCriticalMessage) {
      if (!this.serverReady) {
        logger.debug(`[ConnectionManager] Cannot send critical message ${message.t} - server not ready yet`);
        return false;
      }
    } else {
      if (!this.isReady()) {
        logger.debug(`[ConnectionManager] Cannot send non-critical message ${message.t} - not fully ready`);
        return false;
      }
    }

    if (!this.rateLimiter.canSend()) {
      logger.warn('[ConnectionManager] Rate limit exceeded');
      const store = useWSEStore.getState();
      store.incrementMetric('messagesDropped');

      const retryAfter = this.rateLimiter.getRetryAfter();
      logger.info(`[ConnectionManager] Rate limited. Retry after ${retryAfter}s`);

      window.dispatchEvent(new CustomEvent('clientRateLimitExceeded', {
        detail: { retryAfter, stats: this.rateLimiter.getStats() }
      }));

      return false;
    }

    try {
      const msgType = message.t || '';
      let prefix = 'U';
      if (msgType === 'client_hello' || msgType === 'subscription_update') {
        prefix = 'WSE';
      } else if (msgType === 'sync_request' || msgType.startsWith('request_')) {
        prefix = 'S';
      }

      const jsonData = JSON.stringify(message);
      const data = `${prefix}${jsonData}`;
      this.ws.send(data);

      const store = useWSEStore.getState();
      store.incrementMetric('messagesSent');
      store.incrementMetric('bytesSent', textEncoder.encode(data).byteLength);

      logger.debug(`[ConnectionManager] Sent ${isCriticalMessage ? 'critical' : ''} message: ${message.t}`);
      return true;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error('[ConnectionManager] Failed to send message:', errorMessage);
      return false;
    }
  }

  sendRaw(data: string | ArrayBuffer): boolean {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN || this.isDestroyed) {
      return false;
    }

    if (!this.rateLimiter.canSend()) {
      logger.warn('[ConnectionManager] Rate limit exceeded for raw message');
      return false;
    }

    try {
      this.ws.send(data);

      const store = useWSEStore.getState();
      store.incrementMetric('messagesSent');

      if (typeof data === 'string') {
        store.incrementMetric('bytesSent', textEncoder.encode(data).byteLength);
      } else {
        store.incrementMetric('bytesSent', data.byteLength);
      }

      return true;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error('[ConnectionManager] Failed to send raw message:', errorMessage);
      return false;
    }
  }

  // ---------------------------------------------------------------------------
  // Server Ready and Client Hello
  // ---------------------------------------------------------------------------

  handleServerReady(details: any): void {
    this.serverReady = true;
    this.connectionId = details.connection_id || null;
    this.serverReadyProcessed = true;
    this.serverReadyDetails = details;

    logger.info('[ConnectionManager] Server ready received:', details);

    if (!this.clientHelloSent) {
      this.sendClientHello();
    }

    if (this.onServerReady && !this.isDestroyed) {
      logger.info('[ConnectionManager] Notifying onServerReady callback');
      this.onServerReady(details);
    }
  }

  processPendingServerReady(): void {
    if (this.serverReadyProcessed && this.serverReadyDetails && !this.isDestroyed) {
      this.handleServerReady(this.serverReadyDetails);
    }
  }

  resetServerReadyFlag(): void {
    this.serverReadyProcessed = false;
    this.serverReadyDetails = null;
  }

  private sendClientHello(): void {
    if (this.clientHelloSent || this.isDestroyed) {
      return;
    }

    const message: WSMessage = {
      t: 'client_hello',
      p: {
        client_version: WS_CLIENT_VERSION,
        protocol_version: WS_PROTOCOL_VERSION,
        features: {
          compression: FEATURES.COMPRESSION,
          encryption: FEATURES.ENCRYPTION,
          batching: FEATURES.BATCHING,
          priority_queue: FEATURES.PRIORITY_QUEUE,
          circuit_breaker: FEATURES.CIRCUIT_BREAKER,
          offline_queue: FEATURES.OFFLINE_QUEUE,
          message_signing: FEATURES.MESSAGE_SIGNING,
          rate_limiting: FEATURES.RATE_LIMITING,
          health_check: FEATURES.HEALTH_CHECK,
          metrics: FEATURES.METRICS,
        },
        connection_id: this.connectionId,
      },
      id: crypto.randomUUID(),
      ts: new Date().toISOString(),
      v: WS_PROTOCOL_VERSION,
      pri: MessagePriority.CRITICAL,
    };

    const sent = this.send(message);
    if (sent) {
      this.clientHelloSent = true;
      this.clientHelloRetries = 0;
      logger.info('[ConnectionManager] Client hello sent successfully');
    } else if (!this.isDestroyed && this.clientHelloRetries < this.MAX_CLIENT_HELLO_RETRIES) {
      this.clientHelloRetries++;
      logger.warn(`[ConnectionManager] Failed to send client hello, retrying in 1s (attempt ${this.clientHelloRetries}/${this.MAX_CLIENT_HELLO_RETRIES})`);
      setTimeout(() => this.sendClientHello(), 1000);
    } else if (this.clientHelloRetries >= this.MAX_CLIENT_HELLO_RETRIES) {
      logger.error('[ConnectionManager] Max client hello retries reached, giving up');
    }
  }

  // ---------------------------------------------------------------------------
  // WebSocket Event Handlers
  // ---------------------------------------------------------------------------

  private setupWebSocketHandlers(ws: WebSocket, endpoint: string): void {
    ws.onopen = () => {
      logger.info('[ConnectionManager] WebSocket connection opened');
      this.connectionPool.recordSuccess(endpoint);
    };

    ws.onmessage = (event) => {
      if (this.isDestroyed) return;

      if (typeof event.data === 'string' && event.data.startsWith('PONG:')) {
        try {
          const timestamp = parseInt(event.data.substring(5), 10);
          const latency = Date.now() - timestamp;
          this.connectionPool.recordSuccess(endpoint, latency);

          let quality: ConnectionQuality;
          if (latency < 100) quality = ConnectionQuality.EXCELLENT;
          else if (latency < 300) quality = ConnectionQuality.GOOD;
          else if (latency < 1000) quality = ConnectionQuality.FAIR;
          else quality = ConnectionQuality.POOR;

          this.adaptiveQuality.updateQuality(quality, { latency });
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          logger.warn('[ConnectionManager] Failed to parse PONG timestamp:', errorMessage);
        }
      }

      this.onMessage(event.data);
    };

    ws.onerror = (error) => {
      if (this.isDestroyed) return;

      logger.error(`[ConnectionManager] WebSocket error on ${endpoint}:`, error);
      const store = useWSEStore.getState();
      store.recordCircuitBreakerFailure();
      this.connectionPool.recordFailure(endpoint);
    };

    ws.onclose = (event) => {
      if (this.isDestroyed) return;

      logger.info(`[ConnectionManager] WebSocket closed on ${endpoint}: code=${event.code}, reason=${event.reason}`);

      this.ws = null;
      this.serverReady = false;
      this.clientHelloSent = false;
      this.serverReadyProcessed = false;

      const store = useWSEStore.getState();

      if (this.currentEndpoint) {
        try {
          this.connectionPool.removeConnection(this.currentEndpoint, ws);
        } catch (e) {
          logger.error('[ConnectionManager] Error removing from pool on close:', e);
        }
      }

      if (event.code === 1000 || event.code === 1001) {
        this.onStateChange(ConnectionState.DISCONNECTED);
      } else if (event.code === 4401 || event.code === 4403) {
        this.onStateChange(ConnectionState.ERROR);
        store.setLastError('Authentication failed', event.code);
        store.incrementMetric('authFailures');

        this.token = null;

        window.dispatchEvent(new CustomEvent('wseAuthFailed', {
          detail: {
            code: event.code,
            reason: event.reason || 'Backend authentication failed'
          }
        }));

        if (this.refreshAuthTokenFn) {
          this.refreshTokenWithRetry().then(() => {
            if (!this.isDestroyed) {
              logger.info('[ConnectionManager] Token refreshed after auth close, reconnecting');
              this.onStateChange(ConnectionState.RECONNECTING);
              this.scheduleReconnect();
            }
          }).catch(error => {
            const errorMessage = error instanceof Error ? error.message : String(error);
            logger.error('[ConnectionManager] Token refresh after auth failure failed:', errorMessage);
          });
        }
      } else if (event.code === 4429) {
        this.onStateChange(ConnectionState.ERROR);
        store.setLastError('Rate limit exceeded', event.code);
      } else if (!this.isDestroyed) {
        this.onStateChange(ConnectionState.RECONNECTING);
        this.scheduleReconnect();
      }
    };
  }

  private selectBestEndpoint(): string | null {
    return this.connectionPool.getBestEndpoint();
  }

  private startHeartbeat(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
    }

    this.heartbeatInterval = setInterval(() => {
      if (this.isDestroyed) {
        if (this.heartbeatInterval) {
          clearInterval(this.heartbeatInterval);
          this.heartbeatInterval = null;
        }
        return;
      }

      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        try {
          const pingMessage = {
            t: 'PING',
            p: { timestamp: Date.now() },
            v: WS_PROTOCOL_VERSION
          };
          this.ws.send(`WSE${JSON.stringify(pingMessage)}`);
          const store = useWSEStore.getState();
          store.incrementMetric('messagesSent');
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          logger.error('[ConnectionManager] Failed to send PING:', errorMessage);
        }
      }
    }, 15000);
  }

  // ---------------------------------------------------------------------------
  // Public Methods
  // ---------------------------------------------------------------------------

  changeEndpoint(endpoint: string): void {
    if (this.isDestroyed) return;

    logger.info(`[ConnectionManager] Changing endpoint to: ${endpoint}`);

    this.connectionPool.setPreferredEndpoint(endpoint);
    this.connectionPool.addEndpoint(endpoint);

    this.disconnect();
    this.reconnectAttempts = 0;
    this.consecutiveFailures = 0;

    if (this.token) {
      const store = useWSEStore.getState();
      this.connect(this.token, store.activeTopics).catch(error => {
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.error('[ConnectionManager] Failed to change endpoint:', errorMessage);
      });
    } else {
      logger.warn('[ConnectionManager] Cannot change endpoint: no token available');
    }
  }

  isConnected(): boolean {
    return !this.isDestroyed && this.ws !== null && this.ws.readyState === WebSocket.OPEN;
  }

  isReady(): boolean {
    return this.isConnected() && this.serverReady;
  }

  isFullyReady(): boolean {
    return this.isConnected() && this.serverReady && this.clientHelloSent;
  }

  getConnectionId(): string | null {
    return this.connectionId;
  }

  getRateLimiterStats(): any {
    return this.rateLimiter.getStats();
  }

  getConnectionPoolStats(): any {
    return this.connectionPool.getDetailedStats();
  }

  setLoadBalancingStrategy(strategy: 'weighted-random' | 'least-connections' | 'round-robin'): void {
    this.connectionPool.setLoadBalancingStrategy(strategy);
  }

  getHealthScores(): Record<string, number> {
    return this.connectionPool.getHealthScores();
  }

  getConnectionPool(): ConnectionPool {
    return this.connectionPool;
  }

  getAdaptiveQuality(): { quality: string | null; strategy: string } {
    return {
      quality: this.adaptiveQuality.getCurrentQuality(),
      strategy: this.adaptiveQuality.getCurrentStrategyName(),
    };
  }

  destroy(): void {
    logger.info('[ConnectionManager] Destroy called');

    this.isDestroyed = true;

    if (this.ws) {
      try {
        this.ws.onopen = null;
        this.ws.onmessage = null;
        this.ws.onerror = null;
        this.ws.onclose = null;

        if (this.ws.readyState === WebSocket.OPEN ||
            this.ws.readyState === WebSocket.CONNECTING) {
          this.ws.close(1000, 'Destroying connection manager');
        }
      } catch (e) {
        logger.error('[ConnectionManager] Error closing WebSocket:', e);
      }
      this.ws = null;
    }

    this.clearAllTimers();

    try {
      this.connectionPool.destroy();
    } catch (e) {
      logger.error('[ConnectionManager] Error destroying connection pool:', e);
    }

    this.connectionCircuitBreaker.reset();

    logger.info('[ConnectionManager] Destroyed');
  }
}
