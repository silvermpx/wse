# wse-client (React)

React client for [WSE (WebSocket Engine)](https://github.com/silvermpx/wse) -- real-time event streaming with a single hook.

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
import { useWSE } from 'wse-client';

function Dashboard() {
  const { isConnected, connectionHealth } = useWSE({
    topics: ['notifications', 'live_data'],
    endpoints: ['ws://localhost:5006/wse'],
  });

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

```tsx
const {
  isConnected,        // boolean -- WebSocket open
  isReady,            // boolean -- server_ready received
  connectionHealth,   // "excellent" | "good" | "fair" | "poor" | "unknown"
  connectionState,    // ConnectionState enum
  metrics,            // ConnectionMetrics object
  circuitBreaker,     // CircuitBreakerInfo
  sendMessage,        // (type, payload, options?) => void
  subscribe,          // (topics: string[]) => void
  unsubscribe,        // (topics: string[]) => void
  requestSnapshot,    // (topics: string[]) => void
  disconnect,         // () => void
  reconnect,          // () => void
} = useWSE(config);
```

### Configuration

```tsx
useWSE({
  // Required
  endpoints: ['ws://localhost:5006/wse'],
  topics: ['notifications'],

  // Authentication
  token: 'jwt-token',

  // Reconnection (all optional)
  reconnection: {
    mode: 'adaptive',       // 'exponential' | 'linear' | 'fibonacci' | 'adaptive'
    baseDelay: 1000,        // ms
    maxDelay: 30000,        // ms
    maxAttempts: 10,        // -1 for infinite
    factor: 1.5,
    jitter: true,
  },

  // Encryption (optional, requires server support)
  security: {
    encryptionEnabled: false,
    messageSignature: false,
  },

  // Performance tuning (optional)
  performance: {
    batchSize: 10,
    batchTimeout: 500,            // ms
    compressionThreshold: 1024,   // bytes
    maxQueueSize: 10000,
    memoryLimit: 50 * 1024 * 1024,
  },

  // Offline queue (optional)
  offline: {
    enabled: true,
    maxSize: 1000,
    maxAge: 3600000,              // ms
    persistToStorage: true,       // IndexedDB
  },

  // Diagnostics (optional)
  diagnostics: {
    enabled: false,
    sampleRate: 0.1,
    metricsInterval: 60000,       // ms
    healthCheckInterval: 30000,   // ms
  },
});
```

## Receiving Events

WSE dispatches events as browser `CustomEvent`s on `window`, keyed by topic name:

```tsx
useEffect(() => {
  const handler = (e: CustomEvent) => {
    const { type, payload } = e.detail;  // WSE message fields
    console.log(type, payload);
  };
  window.addEventListener('my_topic', handler);
  return () => window.removeEventListener('my_topic', handler);
}, []);
```

## Sending Messages

```tsx
const { sendMessage } = useWSE(config);

// Basic
sendMessage('chat_message', { text: 'Hello' });

// With priority
sendMessage('alert', { msg: 'critical' }, {
  priority: MessagePriority.CRITICAL,
});
```

## Stores (Zustand)

For accessing WSE state outside of React components:

```tsx
import { useWSEStore } from 'wse-client';

// In any component or store
const state = useWSEStore.getState().connectionState;
const metrics = useWSEStore.getState().metrics;
```

## Features

| Feature | Description |
|---------|-------------|
| **Auto-reconnection** | Exponential/linear/fibonacci/adaptive backoff with jitter |
| **Offline queue** | IndexedDB persistence, replays on reconnect |
| **E2E encryption** | ECDH P-256 + AES-GCM-256, per-connection session keys |
| **Message signing** | HMAC-SHA256 integrity verification |
| **Compression** | Automatic zlib for messages > 1 KB |
| **MessagePack** | Binary encoding support via `@msgpack/msgpack` |
| **Batching** | Automatic message batching (10 msgs / 100ms) |
| **Rate limiting** | Token bucket (1000 tokens, 100/sec refill) |
| **Circuit breaker** | Prevents connection storms (5 failures -> 60s cooldown) |
| **Event sequencing** | Duplicate detection + out-of-order buffering |
| **Connection pool** | Multi-endpoint with health scoring and load balancing |
| **Network monitor** | Latency, jitter, packet loss, quality scoring |
| **Priority queues** | 5 priority levels (CRITICAL to BACKGROUND) |

## Exports

```tsx
// Main hook
import { useWSE } from 'wse-client';

// Types
import {
  WSEConfig, MessagePriority, ConnectionState,
  ConnectionQuality, CircuitBreakerState,
} from 'wse-client';

// Stores
import { useWSEStore, useMessageQueueStore } from 'wse-client';

// Services (advanced)
import {
  ConnectionManager, MessageProcessor, RateLimiter,
  ConnectionPool, NetworkMonitor, OfflineQueue,
  EventSequencer, AdaptiveQualityManager,
} from 'wse-client';

// Utils
import { logger, CircuitBreaker } from 'wse-client';
```

## Wire Protocol

Speaks WSE wire protocol v1, identical to the Python client:

- **Text frames:** Category prefix (`WSE{`, `S{`, `U{`) + JSON envelope
- **Binary frames:** Codec prefix (`C:` zlib, `M:` msgpack, `E:` AES-GCM) + payload
- **Heartbeat:** JSON PING/PONG every 15s with latency tracking

Full protocol spec: [PROTOCOL.md](../docs/PROTOCOL.md)

## Requirements

- React 18+
- TypeScript 5+
- zustand 4+ (peer dependency)

## License

MIT
