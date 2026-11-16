// =============================================================================
// WebSocket Engine - Core (wse-client/core)
// =============================================================================
//
// The client without a framework: no React, no zustand, nothing run or read
// from the page on import. Frameworks hold it through wse-client/react, /vue
// and /svelte; plain pages, workers and Node use it as it is.

export { createWSEClient } from './client.js';
export type { Lease, WSEClient, WSEClientOptions, WSEClientSnapshot, WSEClientState } from './client.js';
export type {
  CheckpointMismatch,
  ContinuityReason,
  DeliveryMode,
  DeliveryOptions,
  ResyncRequest,
  ResyncResult,
  TopicState,
} from './delivery.js';

export { createStore } from './store.js';
export type { GetState, SetState, StateCreator, StoreApi } from './store.js';
export {
  createClientContext,
  memoryStorage,
  namespacedStorage,
  prefixedStorage,
  scopedStorage,
} from './context.js';
export type {
  ClientContext,
  ClientStorage,
  ContextOptions,
  Emit,
  OwnerGeneration,
  StorageErrorListener,
  StorageOperation,
} from './context.js';
export type { WSEStore, InboundRecoveryPosition } from './state/wseState.js';
export type { MessageQueueStore } from './state/queueState.js';

export * from '../types.js';
export type { UseWSEConfig, HandlerRegistry } from '../services/Session.js';

// The services, each with a context of its own unless given one.
export { ConnectionManager } from '../services/ConnectionManager.js';
export type { ConnectionManagerConfig, SendResult } from '../services/ConnectionManager.js';
export { MessageProcessor } from '../services/MessageProcessor.js';
export type { MessageProcessorOptions, ProcessorConnection } from '../services/MessageProcessor.js';
export { NetworkMonitor } from '../services/NetworkMonitor.js';
export { AdaptiveQualityManager, QUALITY_STRATEGIES } from '../services/AdaptiveQualityManager.js';
export type { AdaptiveQualityOptions, QualityChangeDetail, QualityStrategy, QueryClientLike } from '../services/AdaptiveQualityManager.js';
export { EventSequencer } from '../services/EventSequencer.js';
export type { TopicPosition } from '../services/EventSequencer.js';
export { RateLimiter } from '../services/RateLimiter.js';
export { ConnectionPool } from '../services/ConnectionPool.js';
export type { ConnectionPoolConfig } from '../services/ConnectionPool.js';
export { OfflineQueue } from '../services/OfflineQueue.js';
export type { OfflineQueueConfig, OfflineRecord } from '../services/OfflineQueue.js';

export { logger, Logger, LogLevel } from '../utils/logger.js';
export { CircuitBreaker, createCircuitBreaker } from '../utils/circuitBreaker.js';
export { generateId } from '../utils/id.js';
export { setWebCrypto } from '../utils/webcrypto.js';
export { generateSigningKey, signingKeyFrom, signMessage, ServerSigningKeys, SignatureError } from '../utils/signing.js';
export type { SignatureProof, SigningKey } from '../utils/signing.js';
export { CompressionManager, DecompressedSizeLimitError } from '../protocols/compression.js';

import { WS_CLIENT_VERSION } from '../constants.js';

/** Version of this package (same value the client sends in client_hello). */
export const WSE_VERSION: string = WS_CLIENT_VERSION;
