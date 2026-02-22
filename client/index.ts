// =============================================================================
// WebSocket Engine - Public Exports
// =============================================================================

// ---------------------------------------------------------------------------
// Main Hook
// ---------------------------------------------------------------------------

export { useWSE } from './hooks/useWSE';
export type { UseWSEConfig } from './hooks/useWSE';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export * from './types';

// ---------------------------------------------------------------------------
// Stores
// ---------------------------------------------------------------------------

export { useWSEStore } from './stores/useWSEStore';
export { useMessageQueueStore } from './stores/useMessageQueueStore';

// ---------------------------------------------------------------------------
// Utils
// ---------------------------------------------------------------------------

export { logger, Logger, LogLevel } from './utils/logger';
export { CircuitBreaker, createCircuitBreaker } from './utils/circuitBreaker';
export { EventSequencer } from './services/EventSequencer';
export { AdaptiveQualityManager } from './services/AdaptiveQualityManager';
export { compressionManager } from './protocols/compression';
export { eventTransformer } from './protocols/transformer';

// ---------------------------------------------------------------------------
// Services (for advanced usage)
// ---------------------------------------------------------------------------

export { ConnectionManager } from './services/ConnectionManager';
export type { ConnectionManagerConfig } from './services/ConnectionManager';
export { MessageProcessor } from './services/MessageProcessor';
export { RateLimiter } from './services/RateLimiter';
export { ConnectionPool } from './services/ConnectionPool';
export type { ConnectionPoolConfig } from './services/ConnectionPool';
export { NetworkMonitor } from './services/NetworkMonitor';
export { OfflineQueue } from './services/OfflineQueue';
export type { OfflineQueueConfig } from './services/OfflineQueue';

// ---------------------------------------------------------------------------
// Handlers (for extending)
// ---------------------------------------------------------------------------

export { EventHandlers } from './handlers/EventHandlers';
export { registerAllHandlers } from './handlers/index';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

export * from './constants';

// ---------------------------------------------------------------------------
// Version Info
// ---------------------------------------------------------------------------

export const WSE_VERSION = '1.3.2';
