// =============================================================================
// WebSocket Engine - Constants
// =============================================================================

// ---------------------------------------------------------------------------
// Protocol Constants
// ---------------------------------------------------------------------------

export const WS_PROTOCOL_VERSION = 1;
export const WS_CLIENT_VERSION = '2.0.0';

// ---------------------------------------------------------------------------
// Connection Constants
// ---------------------------------------------------------------------------

export const HEARTBEAT_INTERVAL = 15000; // 15 seconds
export const IDLE_TIMEOUT = 40000; // 40 seconds
export const CONNECTION_TIMEOUT = 10000; // 10 seconds
export const HEALTH_CHECK_INTERVAL = 30000; // 30 seconds
export const METRICS_INTERVAL = 60000; // 60 seconds

// ---------------------------------------------------------------------------
// Reconnection Constants
// ---------------------------------------------------------------------------

export const RECONNECT_BASE_DELAY = 1000; // 1 second
export const RECONNECT_MAX_DELAY = 30000; // 30 seconds
export const RECONNECT_MAX_ATTEMPTS = -1; // -1 for infinite
export const RECONNECT_FACTOR = 1.5;
export const RECONNECT_JITTER = true;

// ---------------------------------------------------------------------------
// Circuit Breaker Constants
// ---------------------------------------------------------------------------

export const CIRCUIT_BREAKER_THRESHOLD = 5;
export const CIRCUIT_BREAKER_TIMEOUT = 60000; // 1 minute
export const CIRCUIT_BREAKER_SUCCESS_THRESHOLD = 3;

// ---------------------------------------------------------------------------
// Queue Constants
// ---------------------------------------------------------------------------

export const MESSAGE_QUEUE_MAX_SIZE = 1000;
export const OFFLINE_QUEUE_MAX_SIZE = 1000;
export const BATCH_SIZE = 10;
export const BATCH_TIMEOUT = 100; // milliseconds

// ---------------------------------------------------------------------------
// Compression Constants
// ---------------------------------------------------------------------------

export const COMPRESSION_THRESHOLD = 1024; // 1KB
export const COMPRESSION_LEVEL = 6; // 1-9, where 9 is the best compression

// ---------------------------------------------------------------------------
// Rate Limiting Constants
// ---------------------------------------------------------------------------

export const RATE_LIMIT_CAPACITY = 1000;
export const RATE_LIMIT_REFILL_RATE = 100; // tokens per second
export const RATE_LIMIT_REFILL_INTERVAL = 1.0; // seconds

// ---------------------------------------------------------------------------
// Sequencing Constants
// ---------------------------------------------------------------------------

export const SEQUENCE_WINDOW_SIZE = 1000;
export const MAX_OUT_OF_ORDER = 100;
export const DUPLICATE_WINDOW_SIZE = 1000;

// ---------------------------------------------------------------------------
// Storage Constants
// ---------------------------------------------------------------------------

export const STORAGE_KEY_PREFIX = 'wse_';
export const SUBSCRIPTION_STORAGE_KEY = `${STORAGE_KEY_PREFIX}subscriptions`;
export const ENDPOINTS_STORAGE_KEY = `${STORAGE_KEY_PREFIX}endpoints`;
export const METRICS_STORAGE_KEY = `${STORAGE_KEY_PREFIX}metrics`;
export const DIAGNOSTICS_STORAGE_KEY = `${STORAGE_KEY_PREFIX}diagnostics`;

// ---------------------------------------------------------------------------
// Default Topics
// ---------------------------------------------------------------------------

// Override these in your application by passing topics to useWSE()
export const DEFAULT_TOPICS: string[] = [
    'system_events',
];

// ---------------------------------------------------------------------------
// Message Headers
// ---------------------------------------------------------------------------

export const MESSAGE_HEADERS = {
    COMPRESSED: 'C:',
    MSGPACK: 'M:',
    ENCRYPTED: 'E:',
} as const;

// ---------------------------------------------------------------------------
// Memory Limits
// ---------------------------------------------------------------------------

export const MEMORY_LIMITS = {
    MAX_MESSAGE_SIZE: 1048576, // 1MB
    MAX_QUEUE_MEMORY: 50 * 1024 * 1024, // 50MB
    MAX_HISTORY_SIZE: 1000,
    MAX_ERROR_HISTORY: 50,
} as const;

// ---------------------------------------------------------------------------
// Feature Flags
// ---------------------------------------------------------------------------

export const FEATURES = {
    COMPRESSION: true,
    ENCRYPTION: false, // Disabled by default in a client
    BATCHING: true,
    PRIORITY_QUEUE: true,
    CIRCUIT_BREAKER: true,
    OFFLINE_QUEUE: true,
    MESSAGE_SIGNING: false,
    RATE_LIMITING: true,
    HEALTH_CHECK: true,
    METRICS: true,
} as const;

// ---------------------------------------------------------------------------
// Event Type Mappings
// ---------------------------------------------------------------------------

// Application-specific event type mappings.
// Register your own mappings by extending this record at runtime.
export const INTERNAL_TO_WS_EVENT_TYPE_MAP: Record<string, string> = {};

// ---------------------------------------------------------------------------
// Error Codes
// ---------------------------------------------------------------------------

export const ERROR_CODES = {
    // WebSocket standard codes
    NORMAL_CLOSURE: 1000,
    GOING_AWAY: 1001,
    PROTOCOL_ERROR: 1002,
    UNSUPPORTED_DATA: 1003,
    POLICY_VIOLATION: 1008,
    MESSAGE_TOO_BIG: 1009,
    SERVER_ERROR: 1011,

    // Custom application codes (4000-4999)
    AUTH_FAILED: 4401,
    AUTH_EXPIRED: 4403,
    RATE_LIMIT_EXCEEDED: 4429,
    SUBSCRIPTION_FAILED: 4430,
    INVALID_MESSAGE: 4400,
    SERVER_OVERLOAD: 4503,
} as const;

// ---------------------------------------------------------------------------
// Performance Thresholds
// ---------------------------------------------------------------------------

export const PERFORMANCE_THRESHOLDS = {
    LATENCY: {
        EXCELLENT: 50,
        GOOD: 150,
        FAIR: 300,
    },
    JITTER: {
        EXCELLENT: 25,
        GOOD: 50,
        FAIR: 100,
    },
    PACKET_LOSS: {
        EXCELLENT: 0.1,
        GOOD: 1,
        FAIR: 3,
    },
} as const;

// ---------------------------------------------------------------------------
// Helper Functions
// ---------------------------------------------------------------------------

/**
 * Get WebSocket endpoints. By default uses same-origin /wse path.
 * Override this by providing endpoints in WSEConfig.
 */
export function getEndpoints(): string[] {
    // Clear any cached endpoints to avoid old/multiple endpoints
    try {
        localStorage.removeItem(ENDPOINTS_STORAGE_KEY);
    } catch {
        // Ignore storage errors
    }

    const endpoints: string[] = [];

    // Default fallback - same-origin WebSocket
    if (endpoints.length === 0) {
        const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
        const host = window.location.host;
        endpoints.push(`${protocol}://${host}`);
    }

    // Normalize endpoints: strip trailing slash, append /wse only if no path
    const normalizedEndpoints = endpoints.map(endpoint => {
        endpoint = endpoint.replace(/\/$/, '');
        try {
            const url = new URL(endpoint);
            // Only append /wse if the URL has no meaningful path
            if (url.pathname === '/' || url.pathname === '') {
                return `${endpoint}/wse`;
            }
            return endpoint;
        } catch {
            // Not a valid URL, return as-is
            return endpoint;
        }
    });

    // Remove duplicates
    const uniqueEndpoints = Array.from(new Set(normalizedEndpoints));

    // Return only the FIRST endpoint to avoid multiple connection attempts
    const singleEndpoint = uniqueEndpoints.length > 0 ? [uniqueEndpoints[0]] : [];

    return singleEndpoint;
}

export function getSavedSubscriptions(): string[] {
    try {
        const saved = localStorage.getItem(SUBSCRIPTION_STORAGE_KEY);
        return saved ? JSON.parse(saved) : DEFAULT_TOPICS;
    } catch {
        return DEFAULT_TOPICS;
    }
}

export function saveSubscriptions(topics: string[]): void {
    try {
        localStorage.setItem(SUBSCRIPTION_STORAGE_KEY, JSON.stringify(topics));
    } catch {
        // Ignore storage errors
    }
}

export function clearWSEStorage(): void {
    try {
        Object.keys(localStorage)
            .filter(key => key.startsWith(STORAGE_KEY_PREFIX))
            .forEach(key => localStorage.removeItem(key));
    } catch {
        // Ignore storage errors
    }
}

// ---------------------------------------------------------------------------
// Type Guards
// ---------------------------------------------------------------------------

export const isValidPriority = (priority: number): boolean => {
    return [1, 3, 5, 8, 10].includes(priority);
};

export const isValidEventType = (type: string): boolean => {
    return type in INTERNAL_TO_WS_EVENT_TYPE_MAP ||
        Object.values(INTERNAL_TO_WS_EVENT_TYPE_MAP).includes(type);
};

export const isCompressedMessage = (data: ArrayBuffer): boolean => {
    const view = new Uint8Array(data);
    return view.length >= 2 && view[0] === 67 && view[1] === 58; // 'C:'
};

export const isMsgPackMessage = (data: ArrayBuffer): boolean => {
    const view = new Uint8Array(data);
    return view.length >= 2 && view[0] === 77 && view[1] === 58; // 'M:'
};

export const isEncryptedMessage = (data: ArrayBuffer): boolean => {
    const view = new Uint8Array(data);
    return view.length >= 2 && view[0] === 69 && view[1] === 58; // 'E:'
};

// ---------------------------------------------------------------------------
// Export all as default for convenience
// ---------------------------------------------------------------------------

export default {
    WS_PROTOCOL_VERSION,
    WS_CLIENT_VERSION,
    HEARTBEAT_INTERVAL,
    IDLE_TIMEOUT,
    CONNECTION_TIMEOUT,
    HEALTH_CHECK_INTERVAL,
    METRICS_INTERVAL,
    RECONNECT_BASE_DELAY,
    RECONNECT_MAX_DELAY,
    RECONNECT_MAX_ATTEMPTS,
    RECONNECT_FACTOR,
    RECONNECT_JITTER,
    CIRCUIT_BREAKER_THRESHOLD,
    CIRCUIT_BREAKER_TIMEOUT,
    CIRCUIT_BREAKER_SUCCESS_THRESHOLD,
    MESSAGE_QUEUE_MAX_SIZE,
    OFFLINE_QUEUE_MAX_SIZE,
    BATCH_SIZE,
    BATCH_TIMEOUT,
    COMPRESSION_THRESHOLD,
    COMPRESSION_LEVEL,
    RATE_LIMIT_CAPACITY,
    RATE_LIMIT_REFILL_RATE,
    RATE_LIMIT_REFILL_INTERVAL,
    SEQUENCE_WINDOW_SIZE,
    MAX_OUT_OF_ORDER,
    DUPLICATE_WINDOW_SIZE,
    STORAGE_KEY_PREFIX,
    SUBSCRIPTION_STORAGE_KEY,
    ENDPOINTS_STORAGE_KEY,
    METRICS_STORAGE_KEY,
    DIAGNOSTICS_STORAGE_KEY,
    DEFAULT_TOPICS,
    INTERNAL_TO_WS_EVENT_TYPE_MAP,
    ERROR_CODES,
    MESSAGE_HEADERS,
    PERFORMANCE_THRESHOLDS,
    MEMORY_LIMITS,
    FEATURES,
};
