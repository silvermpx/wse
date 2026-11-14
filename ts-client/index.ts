// =============================================================================
// WebSocket Engine - Public Exports
// =============================================================================

// ---------------------------------------------------------------------------
// Main Hook
// ---------------------------------------------------------------------------

export { useWSE, STATS_REFRESH_MS } from './hooks/useWSE.js';
export type { UseWSEConfig, HandlerRegistry } from './hooks/useWSE.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export * from './types.js';

// ---------------------------------------------------------------------------
// Stores
// ---------------------------------------------------------------------------

export { useWSEStore } from './stores/useWSEStore.js';
export { useMessageQueueStore } from './stores/useMessageQueueStore.js';

// ---------------------------------------------------------------------------
// Utils
// ---------------------------------------------------------------------------

export { logger, Logger, LogLevel } from './utils/logger.js';
export { CircuitBreaker, createCircuitBreaker } from './utils/circuitBreaker.js';
export { generateId } from './utils/id.js';
export {
  generateSigningKey,
  signingKeyFrom,
  signMessage,
  ServerSigningKeys,
  SignatureError,
} from './utils/signing.js';
export type { SignatureProof, SigningKey } from './utils/signing.js';
export { EventSequencer } from './services/EventSequencer.js';
export { compressionManager, CompressionManager, DecompressedSizeLimitError } from './protocols/compression.js';
export { eventTransformer } from './protocols/transformer.js';

// ---------------------------------------------------------------------------
// Services (for advanced usage)
// ---------------------------------------------------------------------------

export { ConnectionManager } from './services/ConnectionManager.js';
export type { ConnectionManagerConfig, SendResult } from './services/ConnectionManager.js';
export { MessageProcessor } from './services/MessageProcessor.js';
export type { MessageProcessorOptions, ProcessorConnection } from './services/MessageProcessor.js';
export { RateLimiter } from './services/RateLimiter.js';
export { ConnectionPool } from './services/ConnectionPool.js';
export type { ConnectionPoolConfig } from './services/ConnectionPool.js';
export { NetworkMonitor } from './services/NetworkMonitor.js';
export { AdaptiveQualityManager, QUALITY_STRATEGIES } from './services/AdaptiveQualityManager.js';
export type {
  AdaptiveQualityOptions,
  QualityChangeDetail,
  QualityStrategy,
  QueryClientLike,
} from './services/AdaptiveQualityManager.js';
export { OfflineQueue } from './services/OfflineQueue.js';
export type { OfflineQueueConfig, OfflineRecord } from './services/OfflineQueue.js';

// ---------------------------------------------------------------------------
// Handlers (for extending)
// ---------------------------------------------------------------------------

export { EventHandlers } from './handlers/EventHandlers.js';
export { registerAllHandlers } from './handlers/index.js';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

export * from './constants.js';

// ---------------------------------------------------------------------------
// Version Info
// ---------------------------------------------------------------------------

import { WS_CLIENT_VERSION } from './constants.js';

/** Version of this package (same value the client sends in client_hello). */
export const WSE_VERSION: string = WS_CLIENT_VERSION;
