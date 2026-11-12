// =============================================================================
// WebSocket Engine - Connection Manager
// =============================================================================
//
// Owns one WebSocket at a time and the whole reconnection policy.
//
// Close-code contract (shared with the Python client):
//   - A close the client initiated (disconnect(), destroy(), forceReconnect(),
//     changeEndpoint(), fail-closed encryption) is final for that socket; the
//     old socket's handlers are detached first, so a late close event can
//     never touch the new socket.
//   - Every server- or network-initiated close (1000/1001 server stop, 1005
//     kick, 1006, 1011-1013, 4300 drain, 4429 rate limit, ...) reconnects with
//     backoff. A single failed attempt is never terminal.
//   - 4401/4403 (or an AUTH_FAILED/AUTH_REQUIRED error before the close) is an
//     auth rejection: with refreshAuthToken the credentials are refreshed and
//     the client retries once; otherwise (or when that retry is rejected too)
//     it stops reconnecting, enters ERROR and dispatches `wseAuthFailed`.
//   - reconnection.maxAttempts counts consecutive failed attempts; -1 means
//     infinite. The count (and the backoff) resets only when a connection
//     reaches server_ready -- a socket that opens and is then rejected does
//     not reset it.

import {
  ConnectionState,
  WSMessage,
  ReconnectionStrategy,
  MessagePriority,
  RateLimitConfig,
  CircuitBreakerState,
} from '../types.js';
import { useWSEStore } from '../stores/useWSEStore.js';
import { logger } from '../utils/logger.js';
import { RateLimiter } from './RateLimiter.js';
import { ConnectionPool } from './ConnectionPool.js';
import { CircuitBreaker } from '../utils/circuitBreaker.js';
import {
  WS_PROTOCOL_VERSION,
  WS_CLIENT_VERSION,
  FEATURES,
  HEARTBEAT_INTERVAL,
  IDLE_TIMEOUT,
  CONNECTION_TIMEOUT,
  SERVER_READY_TIMEOUT,
  ENCRYPTION_HANDSHAKE_TIMEOUT,
  RATE_LIMIT_CAPACITY,
  RATE_LIMIT_REFILL_RATE,
} from '../constants.js';
import { securityManager } from '../utils/security.js';
import { generateId } from '../utils/id.js';
import { dispatchWSEEvent } from '../utils/events.js';

/**
 * Outcome of trySend():
 * - 'sent'         handed to the socket (or to the encryption pipeline)
 * - 'not_ready'    no open socket, no server_ready yet, or encryption pending
 * - 'rate_limited' the client-side rate limit is exhausted; retry later
 * - 'error'        the message cannot be serialised; retrying will not help
 */
export type SendResult = 'sent' | 'not_ready' | 'rate_limited' | 'error';

/**
 * Configuration for the connection manager.
 */
export interface ConnectionManagerConfig {
  /**
   * Optional callback to refresh the auth cookie. Called every 10 minutes in
   * cookie mode and once after an auth rejection. Must throw (reject) when the
   * refresh fails.
   */
  refreshAuthToken?: () => Promise<void>;

  /**
   * Require E2E encryption: application messages are held until the ECDH key
   * exchange completes and the connection fails closed when it cannot.
   */
  encryptionRequired?: boolean;

  /** Outbound rate limit; false disables it. Default 1000 burst, 100/s. */
  rateLimit?: RateLimitConfig | false;

  /** Proceed without server_ready after this many ms (0 = wait forever). */
  serverReadyTimeout?: number;

  /** Fail closed when the server's key does not arrive within this many ms. */
  encryptionHandshakeTimeout?: number;

  /** Give up on a socket that has not opened after this many ms. */
  connectionTimeout?: number;

  /** Called whenever application messages can be sent again. */
  onTransportReady?: () => void;

  /** Called after the current socket is gone (closed, timed out, replaced). */
  onSocketClosed?: () => void;

  /** Called with a message accepted by trySend() whose socket vanished before it went out. */
  onUndeliverable?: (message: WSMessage) => void;
}

type StopReason = 'none' | 'manual' | 'auth' | 'encryption' | 'max_attempts';
type EncryptionState = 'off' | 'pending' | 'ready' | 'failed';

const WS_CONNECTING = 0;
const WS_OPEN = 1;

const MAX_TOKEN_REFRESH_ATTEMPTS = 3;
const TOKEN_REFRESH_INTERVAL = 10 * 60 * 1000; // 10 minutes
const TOKEN_REFRESH_RETRY_BASE = 30 * 1000;
const DEFAULT_SERVER_PING_INTERVAL = 25000;

function createRateLimiter(config: RateLimitConfig | false | undefined): RateLimiter | null {
  if (config === false) return null;
  const capacity = config?.capacity ?? RATE_LIMIT_CAPACITY;
  const refillRate = config?.refillRate ?? RATE_LIMIT_REFILL_RATE;
  if (!(capacity > 0) || !(refillRate > 0)) return null;
  return new RateLimiter(capacity, refillRate, 1000);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export class ConnectionManager {
  private ws: WebSocket | null = null;
  private socketId = 0;
  private pendingAttempt: { socketId: number; settle: (error?: Error) => void } | null = null;

  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private connectTimeoutTimer: ReturnType<typeof setTimeout> | null = null;
  private serverReadyTimer: ReturnType<typeof setTimeout> | null = null;
  private encryptionTimer: ReturnType<typeof setTimeout> | null = null;
  private heartbeatInterval: ReturnType<typeof setInterval> | null = null;
  private tokenRefreshTimer: ReturnType<typeof setTimeout> | null = null;

  private reconnectAttempts = 0;
  private token: string | null = null;
  private usesCookieAuth = false;
  private rateLimiter: RateLimiter | null;
  private serverReady = false;
  private clientHelloSent = false;
  private connectionId: string | null = null;
  private readonly encryptionRequired: boolean;
  private encryptionState: EncryptionState = 'off';
  private authRejectedOnSocket = false;
  private authRefreshTried = false;
  private stopReason: StopReason = 'manual'; // nothing to reconnect before connect()
  private tokenRefreshFailures = 0;
  private lastInboundAt = 0;
  private serverPingInterval = DEFAULT_SERVER_PING_INTERVAL;
  private state: ConnectionState = ConnectionState.PENDING;

  // Connection pool
  private connectionPool: ConnectionPool;
  private currentEndpoint: string | null = null;

  private isDestroyed = false;

  // Circuit breaker for connection attempts: a pause, never a death sentence.
  private connectionCircuitBreaker: CircuitBreaker;

  // Serialized queue for encrypted sends (preserves message ordering)
  private encryptedSendQueue: Promise<void> = Promise.resolve();

  private refreshAuthTokenFn: (() => Promise<void>) | null;
  private readonly config: ConnectionManagerConfig;
  private browserListenersInstalled = false;

  constructor(
    private endpoints: string[],
    private reconnectionStrategy: ReconnectionStrategy,
    private onMessage: (data: string | ArrayBuffer) => void,
    private onStateChange: (state: ConnectionState) => void,
    private onServerReady?: (details: any) => void,
    config?: ConnectionManagerConfig
  ) {
    this.config = config ?? {};
    this.encryptionRequired = !!this.config.encryptionRequired;
    this.rateLimiter = createRateLimiter(this.config.rateLimit);

    this.connectionPool = new ConnectionPool({
      endpoints: this.endpoints,
      maxPerEndpoint: 1,
      healthCheckInterval: 30000,
      loadBalancingStrategy: 'weighted-random',
    });

    this.connectionCircuitBreaker = new CircuitBreaker({
      failureThreshold: 5,
      resetTimeout: 60000,
      successThreshold: 1,
    });

    this.refreshAuthTokenFn = this.config.refreshAuthToken ?? null;

    logger.debug('[ConnectionManager] Initialized with endpoints:', this.endpoints);
  }

  // ---------------------------------------------------------------------------
  // Connection Management
  // ---------------------------------------------------------------------------

  /**
   * Start connecting (and keep reconnecting per the policy). Resolves when the
   * socket opens; rejects when this first attempt fails -- reconnection then
   * continues in the background unless the failure was an auth rejection.
   *
   * `token` is the auth mode, not a credential: browsers cannot attach an
   * Authorization header to a WebSocket, so the server authenticates the
   * browser by its cookie (jwt_cookie_name). 'cookie' additionally enables the
   * periodic refreshAuthToken call.
   */
  async connect(token: string, _topics: string[] = []): Promise<void> {
    if (!token) {
      logger.error('[ConnectionManager] No auth signal provided');
      throw new Error('Authentication required');
    }

    if (this.isDestroyed) {
      throw new Error('ConnectionManager has been destroyed');
    }

    this.token = token;
    this.usesCookieAuth = token === 'cookie';
    this.stopReason = 'none';
    this.installBrowserListeners();

    if (this.ws && (this.ws.readyState === WS_OPEN || this.ws.readyState === WS_CONNECTING)) {
      logger.debug('[ConnectionManager] Already connected or connecting');
      return;
    }

    this.clearReconnectTimer();
    this.setupTokenRefresh();
    await this.openSocket();
  }

  private get maxAttempts(): number {
    const max = this.reconnectionStrategy.maxAttempts;
    if (typeof max !== 'number' || Number.isNaN(max) || max < 0) return Infinity;
    return max;
  }

  private buildUrl(endpoint: string): string {
    const url = new URL(endpoint);
    url.searchParams.set('client_version', WS_CLIENT_VERSION);
    url.searchParams.set('protocol_version', String(WS_PROTOCOL_VERSION));
    url.searchParams.set('compression', String(FEATURES.COMPRESSION));
    url.searchParams.set('encryption', String(this.encryptionRequired));
    return url.toString();
  }

  private openSocket(): Promise<void> {
    if (this.isDestroyed || this.stopReason !== 'none') return Promise.resolve();

    this.clearReconnectTimer();

    // Never orphan a live socket: one connection at a time.
    const previous = this.ws;
    if (previous) {
      this.closeQuietly(previous, 1000, 'Client reconnect');
      this.teardownSocket(previous);
    }

    if (!this.connectionCircuitBreaker.canExecute()) {
      logger.warn('[ConnectionManager] Connection circuit breaker is OPEN, delaying reconnect');
      this.setState(ConnectionState.RECONNECTING);
      this.scheduleReconnect();
      return Promise.resolve();
    }

    let endpoint: string;
    let ws: WebSocket;
    try {
      if (typeof WebSocket === 'undefined') {
        throw new Error('WebSocket is not available in this environment');
      }
      endpoint = this.connectionPool.getBestEndpoint();
      ws = new WebSocket(this.buildUrl(endpoint));
    } catch (error) {
      logger.error('[ConnectionManager] Cannot open a WebSocket:', errorMessage(error));
      useWSEStore.getState().setLastError(`Cannot open a WebSocket: ${errorMessage(error)}`);
      this.recordAttemptFailure(null);
      this.setState(ConnectionState.RECONNECTING);
      this.scheduleReconnect();
      return Promise.reject(error instanceof Error ? error : new Error(String(error)));
    }

    ws.binaryType = 'arraybuffer';
    const socketId = ++this.socketId;
    this.ws = ws;
    this.currentEndpoint = endpoint;
    this.resetSocketState();

    if (this.state !== ConnectionState.RECONNECTING) {
      this.setState(ConnectionState.CONNECTING);
    }
    useWSEStore.getState().incrementMetric('connectionAttempts');
    logger.info(`[ConnectionManager] Connecting to ${endpoint}`);

    const startedAt = Date.now();

    return new Promise<void>((resolve, reject) => {
      let settled = false;
      const settle = (error?: Error) => {
        if (settled) return;
        settled = true;
        if (this.pendingAttempt?.socketId === socketId) this.pendingAttempt = null;
        if (error) reject(error);
        else resolve();
      };
      this.pendingAttempt = { socketId, settle };

      this.connectTimeoutTimer = setTimeout(() => {
        this.connectTimeoutTimer = null;
        if (socketId !== this.socketId || ws.readyState === WS_OPEN) return;
        logger.warn(`[ConnectionManager] Connection to ${endpoint} timed out`);
        settle(new Error('Connection timeout'));
        this.handleSocketGone(ws, endpoint, 1006, 'Connection timeout');
      }, this.config.connectionTimeout ?? CONNECTION_TIMEOUT);

      ws.onopen = () => {
        if (socketId !== this.socketId) return;
        this.clearConnectTimeout();
        this.handleOpen(endpoint, Date.now() - startedAt);
        settle();
      };

      ws.onmessage = (event: MessageEvent) => {
        if (socketId !== this.socketId || this.isDestroyed) return;
        this.lastInboundAt = Date.now();
        this.onMessage(event.data);
      };

      ws.onerror = () => {
        if (socketId !== this.socketId) return;
        logger.warn(`[ConnectionManager] WebSocket error on ${endpoint}`);
      };

      ws.onclose = (event: CloseEvent) => {
        if (socketId !== this.socketId) return;
        settle(new Error(`Connection closed (code ${event.code})`));
        this.handleSocketGone(ws, endpoint, event.code, event.reason);
      };
    });
  }

  private handleOpen(endpoint: string, connectMs: number): void {
    const store = useWSEStore.getState();
    this.lastInboundAt = Date.now();

    this.connectionPool.recordSuccess(endpoint, connectMs);
    this.connectionPool.setActiveEndpoint(endpoint);
    store.setActiveEndpoint(endpoint);
    store.updateMetrics({
      connectedSince: Date.now(),
      successfulConnections: store.metrics.successfulConnections + 1,
    });

    // Deliberately NOT resetting the backoff here: the server upgrades the
    // socket before it rejects bad credentials, so "opened" is not "accepted".
    this.setState(ConnectionState.CONNECTED);
    this.startHeartbeat();
    this.startServerReadyTimer();

    logger.info(`[ConnectionManager] Connected to ${endpoint} in ${connectMs}ms`);
  }

  /** The current socket is gone (close event, timeout, idle watchdog). */
  private handleSocketGone(ws: WebSocket, endpoint: string, code: number, reason: string): void {
    if (ws !== this.ws) return;

    const reachedReady = this.serverReady;
    const authRejected = code === 4401 || code === 4403 || this.authRejectedOnSocket;

    this.closeQuietly(ws, 4000, reason || 'closed');
    this.teardownSocket(ws);

    logger.info(`[ConnectionManager] WebSocket closed on ${endpoint}: code=${code}${reason ? `, reason=${reason}` : ''}`);

    if (this.isDestroyed || this.stopReason !== 'none') return;

    if (!reachedReady) {
      this.recordAttemptFailure(endpoint);
    }

    if (authRejected) {
      this.handleAuthRejection(code, reason);
      return;
    }

    if (code !== 1000 && code !== 1001) {
      useWSEStore.getState().setLastError(
        `Connection closed (code ${code}${reason ? `: ${reason}` : ''})`,
        code,
      );
    }

    this.setState(ConnectionState.RECONNECTING);
    this.scheduleReconnect();
  }

  private recordAttemptFailure(endpoint: string | null): void {
    this.reconnectAttempts++;
    this.connectionCircuitBreaker.recordFailure();
    if (endpoint) this.connectionPool.recordFailure(endpoint);
    const store = useWSEStore.getState();
    store.updateMetrics({
      reconnectCount: this.reconnectAttempts, // consecutive failed attempts
      failedConnections: store.metrics.failedConnections + 1,
    });
    this.mirrorCircuitBreaker();
  }

  private handleAuthRejection(code: number, reason: string): void {
    const store = useWSEStore.getState();
    const effectiveCode = code === 4403 ? 4403 : 4401;
    store.setLastError('Authentication failed', effectiveCode);
    store.incrementMetric('authFailures');

    if (this.refreshAuthTokenFn && !this.authRefreshTried) {
      this.authRefreshTried = true;
      logger.warn('[ConnectionManager] Authentication rejected; refreshing credentials before one more attempt');
      this.setState(ConnectionState.RECONNECTING);
      this.refreshTokenWithRetry().then(
        () => {
          // Skip when forceReconnect() already opened a socket meanwhile.
          if (!this.isDestroyed && this.stopReason === 'none' && !this.ws && !this.reconnectTimer) {
            this.scheduleReconnect();
          }
        },
        (error) => {
          if (!this.isDestroyed && this.stopReason === 'none') {
            this.enterAuthFailed(effectiveCode, reason, errorMessage(error));
          }
        },
      );
      return;
    }

    this.enterAuthFailed(effectiveCode, reason);
  }

  private enterAuthFailed(code: number, reason: string, refreshError?: string): void {
    this.stopReason = 'auth';
    this.clearReconnectTimer();
    this.clearTokenRefreshTimer();
    logger.error(
      '[ConnectionManager] Authentication rejected; not reconnecting until new credentials arrive ' +
      '(sign in again, then call forceReconnect() or remount with a new auth value)'
    );
    this.setState(ConnectionState.ERROR);
    dispatchWSEEvent('wseAuthFailed', {
      code,
      reason: reason || 'Authentication failed',
      refreshFailed: refreshError !== undefined,
      refreshError,
    });
  }

  /**
   * Close the current socket on purpose and stop reconnecting. A later
   * connect() or forceReconnect() starts again.
   */
  disconnect(): void {
    logger.info('[ConnectionManager] Disconnect called');

    this.stopReason = 'manual';
    this.clearReconnectTimer();
    this.clearTokenRefreshTimer();

    const ws = this.ws;
    if (ws) {
      this.closeQuietly(ws, 1000, 'Client disconnect');
      this.teardownSocket(ws);
    }

    this.currentEndpoint = null;
    this.setState(ConnectionState.DISCONNECTED);
  }

  /** Detach every handler of `ws`, then close it if it is still alive. */
  private closeQuietly(ws: WebSocket, code: number, reason: string): void {
    ws.onopen = null;
    ws.onmessage = null;
    ws.onerror = null;
    ws.onclose = null;
    try {
      if (ws.readyState === WS_OPEN || ws.readyState === WS_CONNECTING) {
        ws.close(code, reason);
      }
    } catch (error) {
      logger.debug('[ConnectionManager] Error closing WebSocket:', error);
    }
  }

  /** Reset everything that belongs to one socket. */
  private resetSocketState(): void {
    this.serverReady = false;
    this.clientHelloSent = false;
    this.connectionId = null;
    this.authRejectedOnSocket = false;
    this.encryptionState = this.encryptionRequired ? 'pending' : 'off';
  }

  private teardownSocket(ws: WebSocket): void {
    const wasCurrent = ws === this.ws;
    if (wasCurrent) this.ws = null;

    if (this.currentEndpoint) {
      try {
        this.connectionPool.removeConnection(this.currentEndpoint, ws);
      } catch (e) {
        logger.debug('[ConnectionManager] Error removing from connection pool:', e);
      }
    }

    this.stopHeartbeat();
    this.clearConnectTimeout();
    this.clearServerReadyTimer();
    this.clearEncryptionTimer();
    this.resetSocketState();
    this.encryptedSendQueue = Promise.resolve();

    // Fresh ECDH key pair for the next connection; a derivation still in
    // flight for this one can no longer install its key.
    if (this.encryptionRequired) {
      securityManager.resetEncryption();
    }

    if (this.pendingAttempt) {
      const pending = this.pendingAttempt;
      this.pendingAttempt = null;
      pending.settle(new Error('Connection attempt ended'));
    }

    if (wasCurrent) {
      try {
        this.config.onSocketClosed?.();
      } catch (error) {
        logger.error('[ConnectionManager] onSocketClosed callback failed:', error);
      }
    }
  }

  private setState(state: ConnectionState): void {
    this.state = state;
    try {
      useWSEStore.getState().setConnectionState(state);
    } catch {
      // Store unavailable (should not happen)
    }
    try {
      this.onStateChange(state);
    } catch (error) {
      logger.error('[ConnectionManager] onStateChange callback failed:', error);
    }
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private clearConnectTimeout(): void {
    if (this.connectTimeoutTimer) {
      clearTimeout(this.connectTimeoutTimer);
      this.connectTimeoutTimer = null;
    }
  }

  private clearServerReadyTimer(): void {
    if (this.serverReadyTimer) {
      clearTimeout(this.serverReadyTimer);
      this.serverReadyTimer = null;
    }
  }

  private clearEncryptionTimer(): void {
    if (this.encryptionTimer) {
      clearTimeout(this.encryptionTimer);
      this.encryptionTimer = null;
    }
  }

  private clearTokenRefreshTimer(): void {
    if (this.tokenRefreshTimer) {
      clearTimeout(this.tokenRefreshTimer);
      this.tokenRefreshTimer = null;
    }
  }

  private stopHeartbeat(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  private clearAllTimers(): void {
    this.clearReconnectTimer();
    this.clearConnectTimeout();
    this.clearServerReadyTimer();
    this.clearEncryptionTimer();
    this.clearTokenRefreshTimer();
    this.stopHeartbeat();
  }

  // ---------------------------------------------------------------------------
  // Token Management
  // ---------------------------------------------------------------------------

  private setupTokenRefresh(): void {
    // Periodic refresh keeps the auth cookie fresh in cookie mode. It never
    // tears the connection down: a failed refresh is retried with backoff,
    // and only a real rejection by the server (4401/4403) ends the session.
    this.clearTokenRefreshTimer();
    if (this.isDestroyed || !this.usesCookieAuth || !this.refreshAuthTokenFn) return;
    this.scheduleTokenRefresh(TOKEN_REFRESH_INTERVAL);
  }

  private scheduleTokenRefresh(delay: number): void {
    this.clearTokenRefreshTimer();
    this.tokenRefreshTimer = setTimeout(() => {
      this.tokenRefreshTimer = null;
      void this.runPeriodicTokenRefresh();
    }, delay);
  }

  private async runPeriodicTokenRefresh(): Promise<void> {
    if (this.isDestroyed || this.stopReason !== 'none') return;

    try {
      await this.refreshTokenWithRetry();
      this.tokenRefreshFailures = 0;
      if (!this.isDestroyed && this.stopReason === 'none') {
        this.scheduleTokenRefresh(TOKEN_REFRESH_INTERVAL);
      }
    } catch (error) {
      if (this.isDestroyed || this.stopReason !== 'none') return;
      this.tokenRefreshFailures++;
      const retryIn = Math.min(
        TOKEN_REFRESH_INTERVAL,
        TOKEN_REFRESH_RETRY_BASE * Math.pow(2, Math.min(this.tokenRefreshFailures - 1, 10)),
      );
      logger.warn(
        `[ConnectionManager] Periodic token refresh failed (${errorMessage(error)}); ` +
        `keeping the connection, retrying in ${Math.round(retryIn / 1000)}s`
      );
      useWSEStore.getState().setLastError('Token refresh failed (connection kept)');
      dispatchWSEEvent('wseTokenRefreshFailed', {
        error: errorMessage(error),
        failures: this.tokenRefreshFailures,
        retryInMs: retryIn,
      });
      this.scheduleTokenRefresh(retryIn);
    }
  }

  /** Call refreshAuthToken up to 3 times; rejects when every attempt failed. */
  private async refreshTokenWithRetry(): Promise<void> {
    if (!this.refreshAuthTokenFn) {
      throw new Error('No refreshAuthToken callback configured');
    }

    let lastError: unknown = null;
    for (let attempt = 0; attempt < MAX_TOKEN_REFRESH_ATTEMPTS; attempt++) {
      if (this.isDestroyed) throw new Error('ConnectionManager has been destroyed');
      try {
        await this.refreshAuthTokenFn();
        return;
      } catch (error) {
        lastError = error;
        logger.warn(
          `[ConnectionManager] Token refresh attempt ${attempt + 1}/${MAX_TOKEN_REFRESH_ATTEMPTS} failed:`,
          errorMessage(error),
        );
        if (attempt < MAX_TOKEN_REFRESH_ATTEMPTS - 1) {
          const delay = Math.min(1000 * Math.pow(2, attempt), 10000);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError instanceof Error ? lastError : new Error(String(lastError));
  }

  // ---------------------------------------------------------------------------
  // Reconnection
  // ---------------------------------------------------------------------------

  private scheduleReconnect(): void {
    if (this.isDestroyed || this.stopReason !== 'none') return;

    if (this.reconnectAttempts >= this.maxAttempts) {
      this.stopReason = 'max_attempts';
      logger.error(`[ConnectionManager] Max reconnection attempts (${this.maxAttempts}) reached`);
      useWSEStore.getState().setLastError('Max reconnection attempts reached');
      this.setState(ConnectionState.ERROR);
      dispatchWSEEvent('wseReconnectFailed', { attempts: this.reconnectAttempts });
      return;
    }

    let delay = this.calculateReconnectDelay();
    const breaker = this.connectionCircuitBreaker.getMetrics();
    if (breaker.state === CircuitBreakerState.OPEN && breaker.nextRetryTime) {
      delay = Math.max(delay, breaker.nextRetryTime - Date.now());
    }
    delay = Math.max(0, Math.round(delay));

    this.clearReconnectTimer();
    logger.info(`[ConnectionManager] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts + 1})`);

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.openSocket().catch(() => {
        // Failure already handled (another attempt is scheduled)
      });
    }, delay);
  }

  private calculateReconnectDelay(): number {
    const { mode, baseDelay, maxDelay, factor, jitter } = this.reconnectionStrategy;
    const attempts = Math.min(this.reconnectAttempts, 64); // no overflow after hours of outage
    let delay: number;

    switch (mode) {
      case 'exponential':
        delay = Math.min(maxDelay, baseDelay * Math.pow(factor, attempts));
        break;

      case 'linear':
        delay = Math.min(maxDelay, baseDelay + (attempts * 1000));
        break;

      case 'fibonacci': {
        const fib = (n: number): number => {
          let a = 0, b = 1;
          for (let i = 0; i < n; i++) { [a, b] = [b, a + b]; }
          return a;
        };
        delay = Math.min(maxDelay, baseDelay * fib(Math.min(attempts, 10) + 1));
        break;
      }

      case 'adaptive':
      default:
        delay = Math.min(maxDelay, baseDelay * Math.pow(1.5, attempts));
        break;
    }

    if (!Number.isFinite(delay)) delay = maxDelay;

    if (jitter) {
      const jitterAmount = delay * 0.2 * (Math.random() - 0.5);
      delay = Math.max(0, delay + jitterAmount);
    }

    return delay;
  }

  // Reconnect at once when the network comes back or the tab becomes visible
  // (laptop wake), instead of sleeping through the remaining backoff.
  private readonly handleOnline = (): void => {
    this.wakeUp('network back online');
  };

  private readonly handleVisibilityChange = (): void => {
    if (typeof document !== 'undefined' && document.visibilityState === 'visible') {
      this.wakeUp('page visible again');
    }
  };

  private wakeUp(reason: string): void {
    if (this.isDestroyed || !this.token) return;
    if (this.stopReason !== 'none' && this.stopReason !== 'max_attempts') return;
    if (this.ws && (this.ws.readyState === WS_OPEN || this.ws.readyState === WS_CONNECTING)) return;

    if (this.stopReason === 'max_attempts') {
      this.stopReason = 'none';
      this.reconnectAttempts = 0;
    }
    this.connectionCircuitBreaker.reset();
    this.mirrorCircuitBreaker();
    logger.info(`[ConnectionManager] ${reason}: reconnecting now`);
    this.clearReconnectTimer();
    this.openSocket().catch(() => {
      // Failure already handled
    });
  }

  private installBrowserListeners(): void {
    if (this.browserListenersInstalled) return;
    if (typeof window === 'undefined' || typeof window.addEventListener !== 'function') return;
    window.addEventListener('online', this.handleOnline);
    if (typeof document !== 'undefined' && typeof document.addEventListener === 'function') {
      document.addEventListener('visibilitychange', this.handleVisibilityChange);
    }
    this.browserListenersInstalled = true;
  }

  private removeBrowserListeners(): void {
    if (!this.browserListenersInstalled) return;
    if (typeof window !== 'undefined' && typeof window.removeEventListener === 'function') {
      window.removeEventListener('online', this.handleOnline);
    }
    if (typeof document !== 'undefined' && typeof document.removeEventListener === 'function') {
      document.removeEventListener('visibilitychange', this.handleVisibilityChange);
    }
    this.browserListenersInstalled = false;
  }

  private mirrorCircuitBreaker(): void {
    const m = this.connectionCircuitBreaker.getMetrics();
    try {
      useWSEStore.getState().updateCircuitBreaker({
        state: m.state,
        failures: m.failures,
        successCount: m.successes,
        lastFailureTime: m.lastFailureTime || null,
        nextRetryTime: m.nextRetryTime,
      });
    } catch {
      // ignore
    }
  }

  // ---------------------------------------------------------------------------
  // Message Handling
  // ---------------------------------------------------------------------------

  /**
   * True when application messages can go out now: socket open, server_ready
   * received and -- when encryption is required -- the transport key derived.
   */
  isTransportReady(): boolean {
    if (this.isDestroyed || !this.ws || this.ws.readyState !== WS_OPEN || !this.serverReady) return false;
    return !this.encryptionRequired || this.encryptionState === 'ready';
  }

  /** True between server_ready and the end of the key exchange on an encrypted connection. */
  isAwaitingEncryptionKey(): boolean {
    return this.encryptionRequired && this.encryptionState === 'pending' && this.ws !== null;
  }

  /** Milliseconds until the rate limiter has a token again (0 when unlimited). */
  getRetryAfterMs(): number {
    if (!this.rateLimiter) return 0;
    return Math.max(10, Math.ceil(this.rateLimiter.getRetryAfter() * 1000));
  }

  /**
   * Send one application message. Never sends plaintext when encryption is
   * required; never drops silently -- the caller re-queues on anything but
   * 'sent'/'error'.
   */
  trySend(message: WSMessage): SendResult {
    if (!this.isTransportReady()) {
      return 'not_ready';
    }

    if (this.rateLimiter && !this.rateLimiter.canSend()) {
      const retryAfter = this.rateLimiter.getRetryAfter();
      logger.debug(`[ConnectionManager] Rate limited. Retry after ${retryAfter}s`);
      dispatchWSEEvent('clientRateLimitExceeded', {
        retryAfter,
        stats: this.rateLimiter.getStats(),
      });
      return 'rate_limited';
    }

    let data: string;
    try {
      const msgType = message.t || '';
      let category: 'U' | 'S' | 'WSE' = 'U';
      if (msgType === 'client_hello' || msgType === 'subscription_update') {
        category = 'WSE';
      } else if (msgType === 'sync_request' || msgType.startsWith('request_')) {
        category = 'S';
      }
      data = JSON.stringify({ c: category, ...message });
    } catch (error) {
      logger.error('[ConnectionManager] Cannot serialise message:', errorMessage(error));
      return 'error';
    }

    const ws = this.ws!;
    try {
      if (this.encryptionRequired) {
        const socketId = this.socketId;
        this.encryptedSendQueue = this.encryptedSendQueue.then(async () => {
          if (socketId !== this.socketId || this.ws !== ws || ws.readyState !== WS_OPEN) {
            this.reportUndeliverable(message);
            return;
          }
          let frame: ArrayBuffer;
          try {
            frame = await securityManager.encryptForTransport(data);
          } catch (error) {
            if (socketId !== this.socketId) {
              this.reportUndeliverable(message);
            } else {
              logger.error('[ConnectionManager] Encryption failed, message dropped:', errorMessage(error));
            }
            return;
          }
          if (socketId !== this.socketId || this.ws !== ws || ws.readyState !== WS_OPEN) {
            this.reportUndeliverable(message);
            return;
          }
          ws.send(frame);
        });
      } else {
        ws.send(data);
      }
    } catch (error) {
      logger.error('[ConnectionManager] Failed to send message:', errorMessage(error));
      return 'not_ready';
    }

    this.recordSent(data.length);
    return 'sent';
  }

  /** Boolean form of trySend() (true = handed to the socket). */
  send(message: WSMessage): boolean {
    return this.trySend(message) === 'sent';
  }

  private reportUndeliverable(message: WSMessage): void {
    try {
      this.config.onUndeliverable?.(message);
    } catch (error) {
      logger.error('[ConnectionManager] onUndeliverable callback failed:', error);
    }
  }

  private recordSent(bytes: number): void {
    const store = useWSEStore.getState();
    store.updateMetrics({
      messagesSent: store.metrics.messagesSent + 1,
      bytesSent: store.metrics.bytesSent + bytes,
      lastMessageSent: Date.now(),
    });
  }

  /**
   * Send a raw frame through the rate limiter. Refused on an encrypted
   * connection: raw frames cannot be encrypted and must never leak plaintext.
   */
  sendRaw(data: string | ArrayBuffer): boolean {
    if (!this.ws || this.ws.readyState !== WS_OPEN || this.isDestroyed) {
      return false;
    }
    if (this.encryptionRequired) {
      logger.warn('[ConnectionManager] sendRaw refused: encryption is required on this connection');
      return false;
    }
    if (this.rateLimiter && !this.rateLimiter.canSend()) {
      logger.warn('[ConnectionManager] Rate limit exceeded for raw message');
      return false;
    }
    return this.sendDirect(data);
  }

  /**
   * Send a protocol control frame (heartbeat ping, PONG, client_hello):
   * plaintext and never rate limited. RFC 6455 wants PONGs "as soon as is
   * practical", and the server answers only plaintext pings.
   */
  sendControl(data: string | ArrayBuffer): boolean {
    if (!this.ws || this.ws.readyState !== WS_OPEN || this.isDestroyed) {
      return false;
    }
    return this.sendDirect(data);
  }

  private sendDirect(data: string | ArrayBuffer): boolean {
    try {
      this.ws!.send(data);
      this.recordSent(typeof data === 'string' ? data.length : data.byteLength);
      return true;
    } catch (error) {
      logger.error('[ConnectionManager] Failed to send message:', errorMessage(error));
      return false;
    }
  }

  // ---------------------------------------------------------------------------
  // Server Ready, Client Hello, Server Hello
  // ---------------------------------------------------------------------------

  private startServerReadyTimer(): void {
    this.clearServerReadyTimer();
    const timeout = this.config.serverReadyTimeout ?? SERVER_READY_TIMEOUT;
    if (!(timeout > 0)) return;
    const socketId = this.socketId;
    this.serverReadyTimer = setTimeout(() => {
      this.serverReadyTimer = null;
      if (socketId !== this.socketId || this.serverReady) return;
      logger.info(
        `[ConnectionManager] No server_ready within ${timeout}ms (server without jwt_secret?); proceeding`
      );
      this.handleServerReady({ connection_id: null, synthesized: true });
    }, timeout);
  }

  handleServerReady(details: any): void {
    if (this.isDestroyed || !this.ws || this.ws.readyState !== WS_OPEN) return;

    if (details && typeof details.connection_id === 'string') {
      this.connectionId = details.connection_id;
    }
    if (this.serverReady) {
      logger.debug('[ConnectionManager] server_ready already processed for this socket');
      return;
    }

    this.serverReady = true;
    this.clearServerReadyTimer();

    // The connection was accepted: this is where the backoff resets.
    this.reconnectAttempts = 0;
    this.authRefreshTried = false;
    this.connectionCircuitBreaker.recordSuccess();
    this.mirrorCircuitBreaker();
    useWSEStore.getState().updateMetrics({ reconnectCount: 0 });

    logger.info('[ConnectionManager] Server ready');

    if (this.encryptionRequired) {
      this.encryptionState = 'pending';
      this.startEncryptionTimer();
    }
    void this.sendClientHello();

    if (this.onServerReady && !this.isDestroyed) {
      try {
        this.onServerReady(details);
      } catch (error) {
        logger.error('[ConnectionManager] onServerReady callback failed:', error);
      }
    }

    if (!this.encryptionRequired) {
      this.notifyTransportReady();
    }
  }

  private startEncryptionTimer(): void {
    this.clearEncryptionTimer();
    const timeout = this.config.encryptionHandshakeTimeout ?? ENCRYPTION_HANDSHAKE_TIMEOUT;
    if (!(timeout > 0)) return;
    const socketId = this.socketId;
    this.encryptionTimer = setTimeout(() => {
      this.encryptionTimer = null;
      if (socketId === this.socketId && this.encryptionState === 'pending') {
        this.failEncryption(`no server key within ${timeout}ms`);
      }
    }, timeout);
  }

  private notifyTransportReady(): void {
    try {
      this.config.onTransportReady?.();
    } catch (error) {
      logger.error('[ConnectionManager] onTransportReady callback failed:', error);
    }
  }

  private async sendClientHello(): Promise<void> {
    if (this.clientHelloSent || this.isDestroyed) {
      return;
    }

    const socketId = this.socketId;
    let encryptionPublicKey: string | null = null;
    if (this.encryptionRequired) {
      try {
        await securityManager.generateKeyPair();
        encryptionPublicKey = await securityManager.getPublicKeyRaw();
      } catch (error) {
        if (socketId === this.socketId) this.failEncryption(`cannot generate the ECDH key pair (${errorMessage(error)})`);
        return;
      }
      if (!encryptionPublicKey) {
        if (socketId === this.socketId) this.failEncryption('cannot export the ECDH public key');
        return;
      }
    }

    if (socketId !== this.socketId || this.clientHelloSent || !this.ws || this.ws.readyState !== WS_OPEN) {
      return;
    }

    const payload: Record<string, any> = {
      client_version: WS_CLIENT_VERSION,
      protocol_version: WS_PROTOCOL_VERSION,
      features: {
        compression: FEATURES.COMPRESSION,
        encryption: this.encryptionRequired && encryptionPublicKey !== null,
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
    };

    if (encryptionPublicKey) {
      payload.encryption_public_key = encryptionPublicKey;
    }

    const message: WSMessage = {
      t: 'client_hello',
      p: payload,
      id: generateId(),
      ts: new Date().toISOString(),
      v: WS_PROTOCOL_VERSION,
      pri: MessagePriority.CRITICAL,
    };

    // A control frame: plaintext by necessity (it carries our public key)
    // and exempt from the rate limiter.
    if (this.sendControl(JSON.stringify({ c: 'WSE', ...message }))) {
      this.clientHelloSent = true;
      logger.debug('[ConnectionManager] Client hello sent');
    }
  }

  /**
   * Process server_hello. On an encrypted connection this completes the key
   * exchange and only then releases the held messages; anything else fails
   * closed.
   */
  async handleServerHello(payload: any): Promise<void> {
    if (typeof payload?.ping_interval === 'number' && payload.ping_interval > 0) {
      this.serverPingInterval = payload.ping_interval;
    }

    if (!this.encryptionRequired || this.encryptionState !== 'pending' || this.isDestroyed) return;

    const socketId = this.socketId;
    const key = payload?.encryption_public_key;
    if (!payload?.features?.encryption || typeof key !== 'string' || key.length === 0) {
      this.failEncryption('the server did not confirm encryption in server_hello');
      return;
    }

    try {
      await securityManager.setServerPublicKeyRaw(key);
    } catch (error) {
      if (socketId === this.socketId) {
        this.failEncryption(`key exchange failed (${errorMessage(error)})`);
      }
      return;
    }

    if (socketId !== this.socketId || this.encryptionState !== 'pending') return;
    if (!securityManager.isTransportEncryptionReady()) {
      this.failEncryption('no transport key after the key exchange');
      return;
    }

    this.encryptionState = 'ready';
    this.clearEncryptionTimer();
    useWSEStore.getState().updateSecurity({
      encryptionEnabled: true,
      encryptionAlgorithm: 'AES-GCM-256',
    });
    logger.info('[ConnectionManager] E2E encryption established');
    this.notifyTransportReady();
  }

  /**
   * Encryption is required but cannot be established: close the connection,
   * stop reconnecting and report the error. Held messages stay queued and are
   * sent (encrypted) after a later successful forceReconnect().
   */
  failEncryption(reason: string): void {
    if (!this.encryptionRequired || this.isDestroyed) return;
    if (this.stopReason === 'encryption') return;

    logger.error(
      `[ConnectionManager] E2E encryption could not be established (${reason}); ` +
      'closing the connection without sending any plaintext'
    );

    this.stopReason = 'encryption';
    this.clearReconnectTimer();
    const ws = this.ws;
    if (ws) {
      this.closeQuietly(ws, 1000, 'Encryption negotiation failed');
      this.teardownSocket(ws);
    }
    this.encryptionState = 'failed';

    useWSEStore.getState().setLastError(`Encryption failed: ${reason}`);
    this.setState(ConnectionState.ERROR);
    dispatchWSEEvent('wseEncryptionFailed', { reason });
  }

  /** The server reported AUTH_FAILED/AUTH_REQUIRED: treat the coming close as an auth rejection. */
  markAuthRejected(): void {
    if (this.ws) this.authRejectedOnSocket = true;
  }

  // ---------------------------------------------------------------------------
  // Heartbeat and dead-socket watchdog
  // ---------------------------------------------------------------------------

  private startHeartbeat(): void {
    this.stopHeartbeat();

    this.heartbeatInterval = setInterval(() => {
      if (this.isDestroyed) {
        this.stopHeartbeat();
        return;
      }

      const ws = this.ws;
      if (!ws || ws.readyState !== WS_OPEN) return;

      // The server pings every ping_interval and answers our pings, so a
      // healthy socket never stays silent this long; a half-open one does
      // (e.g. after a laptop sleep).
      const idleLimit = Math.max(IDLE_TIMEOUT, this.serverPingInterval * 2.5);
      if (Date.now() - this.lastInboundAt > idleLimit) {
        logger.warn(`[ConnectionManager] No data from the server for ${idleLimit}ms; reconnecting`);
        this.handleSocketGone(ws, this.currentEndpoint ?? '', 1006, 'Idle timeout');
        return;
      }

      this.sendControl(JSON.stringify({
        c: 'WSE',
        t: 'ping',
        p: { timestamp: Date.now() },
        v: WS_PROTOCOL_VERSION,
      }));
    }, HEARTBEAT_INTERVAL);
  }

  // ---------------------------------------------------------------------------
  // Public Methods
  // ---------------------------------------------------------------------------

  /**
   * Drop the current socket and connect again at once, clearing the backoff,
   * the circuit breaker and any auth/encryption stop. Safe while connected,
   * connecting or parked in ERROR.
   */
  forceReconnect(): void {
    if (this.isDestroyed) return;

    logger.info('[ConnectionManager] Force reconnect');
    this.connectionCircuitBreaker.reset();
    this.mirrorCircuitBreaker();
    this.reconnectAttempts = 0;
    this.authRefreshTried = false;
    this.stopReason = 'none';
    this.clearReconnectTimer();
    useWSEStore.getState().updateMetrics({ reconnectCount: 0 });

    const ws = this.ws;
    if (ws) {
      this.closeQuietly(ws, 1000, 'Client reconnect');
      this.teardownSocket(ws);
    }

    if (!this.token) {
      logger.warn('[ConnectionManager] Cannot reconnect: connect() was never called');
      return;
    }

    this.installBrowserListeners();
    this.setupTokenRefresh();
    this.setState(ConnectionState.RECONNECTING);
    this.openSocket().catch(() => {
      // Failure already handled
    });
  }

  changeEndpoint(endpoint: string): void {
    if (this.isDestroyed) return;

    logger.info(`[ConnectionManager] Changing endpoint to: ${endpoint}`);

    this.connectionPool.addEndpoint(endpoint);
    this.connectionPool.setPreferredEndpoint(endpoint);
    this.forceReconnect();
  }

  isConnected(): boolean {
    return !this.isDestroyed && this.ws !== null && this.ws.readyState === WS_OPEN;
  }

  isReady(): boolean {
    return this.isConnected() && this.serverReady;
  }

  isFullyReady(): boolean {
    return this.isTransportReady() && this.clientHelloSent;
  }

  getConnectionId(): string | null {
    return this.connectionId;
  }

  getState(): ConnectionState {
    return this.state;
  }

  getRateLimiterStats(): any {
    return this.rateLimiter ? this.rateLimiter.getStats() : null;
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

  destroy(): void {
    logger.debug('[ConnectionManager] Destroy called');

    this.isDestroyed = true;
    this.stopReason = 'manual';
    this.removeBrowserListeners();
    this.clearAllTimers();

    const ws = this.ws;
    if (ws) {
      this.closeQuietly(ws, 1000, 'Destroying connection manager');
      this.teardownSocket(ws);
    }

    try {
      this.connectionPool.destroy();
    } catch (e) {
      logger.error('[ConnectionManager] Error destroying connection pool:', e);
    }

    this.connectionCircuitBreaker.reset();
  }
}
