# wse-client (TypeScript / React)

React client for [WSE (WebSocket Engine)](https://github.com/niceguy135/wse) - real-time event streaming with a single React hook.

[![npm](https://img.shields.io/npm/v/wse-client)](https://www.npmjs.com/package/wse-client)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0+-blue)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](../LICENSE)

## Installation

```bash
npm install wse-client
```

Peer dependencies: `react >= 18`, `zustand >= 4`.

## Quick Start

```tsx
import { useWSE, ConnectionState } from 'wse-client';

function Dashboard() {
  const { connectionHealth, activeTopics } = useWSE(
    'jwt-token',                                   // token
    ['notifications', 'live_data'],                // initial topics
    { endpoints: ['ws://localhost:5007/wse'] }     // config
  );

  useEffect(() => {
    const handler = (e: CustomEvent) => {
      console.log('New notification:', e.detail);
    };
    window.addEventListener('notifications', handler);
    return () => window.removeEventListener('notifications', handler);
  }, []);

  return <div>Status: {connectionHealth}</div>;
}
```

## useWSE Hook

The main entry point. Manages connection lifecycle, subscriptions, and message dispatch.

### Signature

```tsx
useWSE(token?: string, initialTopics?: string[], config?: UseWSEConfig): UseWSEReturn
```

- **token** - JWT auth token (optional, pass `undefined` to skip authentication)
- **initialTopics** - topics to subscribe to on connect
- **config** - connection and behavior options (see Configuration below)

### Return Values

```tsx
const {
  // Connection management
  subscribe,          // (topics: string[], options?: { recover?: boolean }) => void
  unsubscribe,        // (topics: string[]) => void
  forceReconnect,     // () => void
  changeEndpoint,     // (endpoint: string) => void

  // Messaging
  sendMessage,        // (type: string, payload: any, options?: MessageOptions) => void
  sendBatch,          // (messages: Array<{ type: string; payload: any }>) => void

  // Status and monitoring
  stats,              // ConnectionMetrics object
  activeTopics,       // string[] - currently subscribed topics
  connectionHealth,   // ConnectionState enum
  diagnostics,        // NetworkDiagnostics | null

  // Advanced features
  setCompression,     // (enabled: boolean) => void
  downloadDiagnostics,// () => void
  clearOfflineQueue,  // () => Promise<void>
  getQueueSize,       // () => number
  requestSnapshot,    // (topics?: string[]) => void
  debugHandlers,      // () => void
} = useWSE(token, topics, config);
```

### Configuration

The third argument to `useWSE` accepts a `UseWSEConfig` object. Token and topics are passed as separate arguments (see Signature above).

```tsx
useWSE('jwt-token', ['notifications'], {
  // Endpoints (optional, defaults to same-origin /wse)
  endpoints: ['ws://localhost:5007/wse'],

  // Token refresh callback (optional, called on auth failure)
  refreshAuthToken: async () => {
    const res = await fetch('/api/refresh');
    // Update your token state; no return value needed
  },

  // Handler registration (optional)
  registerHandlers: (processor) => {
    processor.registerHandler('my_event', (msg) => {
      console.log('Received:', msg);
    });
  },
  criticalHandlers: ['my_event'],              // Block until these handlers are registered

  // TanStack Query integration (optional)
  queryClient: queryClient,                    // Adapts stale times based on connection quality

  // Reconnection (all optional)
  reconnection: {
    mode: 'adaptive',                          // 'exponential' | 'linear' | 'fibonacci' | 'adaptive'
    baseDelay: 1000,                           // ms
    maxDelay: 30000,                           // ms
    maxAttempts: 10,                           // -1 for infinite
    factor: 1.5,
    jitter: true,
  },

  // Encryption (optional)
  security: {
    encryptionEnabled: false,                  // ECDH P-256 + AES-GCM-256
    messageSignature: false,                   // HMAC-SHA256 signing
  },

  // Performance tuning (optional)
  performance: {
    batchSize: 10,                             // Messages per batch
    batchTimeout: 500,                         // ms - batch flush interval
    compressionThreshold: 1024,                // bytes - zlib threshold
    maxQueueSize: 10000,                       // Max pending outbound messages
    memoryLimit: 50 * 1024 * 1024,             // 50 MB memory budget
  },

  // Offline queue (optional)
  offline: {
    enabled: true,                             // Enable offline message queueing
    maxSize: 1000,                             // Max queued messages
    maxAge: 3600000,                           // 1 hour max age (ms)
    persistToStorage: true,                    // Persist to IndexedDB
  },

  // Diagnostics (optional)
  diagnostics: {
    enabled: false,                            // Enable metrics collection
    sampleRate: 0.1,                           // Sample 10% of messages
    metricsInterval: 60000,                    // Metrics report interval (ms)
    healthCheckInterval: 30000,                // Health check interval (ms)
  },
});
```

## Receiving Events

WSE dispatches events as browser `CustomEvent`s on `window`, keyed by topic name:

```tsx
useEffect(() => {
  const handler = (e: CustomEvent) => {
    const { type, payload } = e.detail;
    console.log(type, payload);
  };
  window.addEventListener('my_topic', handler);
  return () => window.removeEventListener('my_topic', handler);
}, []);
```

Or use `registerHandlers` for centralized handling:

```tsx
const { stats } = useWSE('token', ['prices'], {
  endpoints: ['ws://localhost:5007/wse'],
  registerHandlers: (processor) => {
    processor.registerHandler('price_update', (msg) => {
      updateChart(msg.p);
    });
  },
});
```

## Sending Messages

```tsx
const { sendMessage } = useWSE(token, topics, config);

// Basic message
sendMessage('chat_message', { text: 'Hello' });

// With priority
sendMessage('alert', { msg: 'critical' }, {
  priority: MessagePriority.CRITICAL,
});

// With correlation ID (for request-response patterns)
sendMessage('query', { filter: 'active' }, {
  correlation_id: 'req-123',
});
```

### Message Priority Levels

| Level | Value | Use Case |
|-------|-------|----------|
| `CRITICAL` | 10 | Alerts, errors, urgent notifications |
| `HIGH` | 8 | User actions, real-time updates |
| `NORMAL` | 5 | Standard messages (default) |
| `LOW` | 3 | Background sync, non-urgent data |
| `BACKGROUND` | 1 | Logging, analytics, telemetry |

## Stores (Zustand)

For accessing WSE state outside of React components:

```tsx
import { useWSEStore, useMessageQueueStore } from 'wse-client';

// Connection state
const state = useWSEStore.getState().connectionState;
const metrics = useWSEStore.getState().metrics;

// Message queue
const queueSize = useMessageQueueStore.getState().size;
```

## Features

| Feature | Description |
|---------|-------------|
| **Auto-reconnection** | Exponential, linear, fibonacci, or adaptive backoff with jitter |
| **Offline queue** | IndexedDB persistence for messages sent while disconnected, replayed on reconnect |
| **E2E encryption** | ECDH P-256 key exchange + AES-GCM-256 encryption, per-connection session keys via Web Crypto API |
| **Message signing** | HMAC-SHA256 integrity verification with nonce-based replay prevention |
| **Compression** | Automatic zlib compression for messages above threshold (default 1 KB) |
| **MessagePack** | Binary encoding support via `@msgpack/msgpack` for smaller payloads |
| **Batching** | Automatic message batching to reduce WebSocket frame overhead |
| **Rate limiting** | Client-side token bucket (1000 tokens, 100/sec refill) |
| **Circuit breaker** | Connection storm prevention (5 failures -> 60s cooldown -> half-open probe) |
| **Event sequencing** | Duplicate detection and out-of-order message buffering |
| **Connection pool** | Multi-endpoint with adaptive health scoring and load balancing |
| **Network monitor** | Real-time latency, jitter, packet loss measurement, quality scoring |
| **Priority queues** | 5 priority levels from CRITICAL to BACKGROUND |
| **Adaptive quality** | Dynamic adjustment of compression and batching based on network conditions |
| **Token refresh** | Automatic token refresh via `refreshAuthToken` callback on auth failure |

## Exports

```tsx
// Main hook and config type
import { useWSE } from 'wse-client';
import type { UseWSEConfig } from 'wse-client';

// Types (enums and interfaces)
import {
  MessagePriority,
  ConnectionState,
  ConnectionQuality,
  CircuitBreakerState,
} from 'wse-client';
import type {
  WSEConfig,
  UseWSEReturn,
  ConnectionMetrics,
  MessageOptions,
  NetworkDiagnostics,
} from 'wse-client';

// Stores (Zustand)
import { useWSEStore, useMessageQueueStore } from 'wse-client';

// Services (advanced usage)
import {
  ConnectionManager,
  MessageProcessor,
  RateLimiter,
  ConnectionPool,
  NetworkMonitor,
  OfflineQueue,
  EventSequencer,
  AdaptiveQualityManager,
} from 'wse-client';

// Utils
import { logger, CircuitBreaker, createCircuitBreaker } from 'wse-client';
import { compressionManager } from 'wse-client';
import { eventTransformer } from 'wse-client';

// Handlers (for extending)
import { EventHandlers, registerAllHandlers } from 'wse-client';

// Constants
import { WSE_VERSION, WS_CLIENT_VERSION, WS_PROTOCOL_VERSION } from 'wse-client';
```

## Wire Protocol

Speaks WSE wire protocol v1, identical to the Python client:

- **Text frames:** Category prefix (`WSE{`, `S{`, `U{`) + JSON envelope
- **Binary frames:** Codec prefix (`C:` zlib, `M:` msgpack, `E:` AES-GCM) + payload
- **Heartbeat:** JSON PING/PONG with latency tracking
- **Protocol negotiation:** `client_hello`/`server_hello` handshake with feature discovery

Full protocol specification: [PROTOCOL.md](../docs/PROTOCOL.md)

## Requirements

- React 18+
- TypeScript 5+
- zustand 4+ (peer dependency)

Optional:
- `@msgpack/msgpack` (MessagePack binary transport)
- `pako` (zlib compression, bundled)

## License

MIT
