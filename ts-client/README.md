# wse-client (TypeScript / React)

React client for [WSE (WebSocket Engine)](https://github.com/silvermpx/wse) - real-time event streaming with a single React hook.

[![npm](https://img.shields.io/npm/v/wse-client)](https://www.npmjs.com/package/wse-client)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0+-blue)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](../LICENSE)

## Installation

```bash
npm install wse-client
```

Peer dependencies: `react >= 18`, `zustand >= 4`.

The package is ESM (`"type": "module"`, explicit `.js` import paths). It loads in bundlers
(Vite, webpack, esbuild, Next.js) and directly in Node.js (SSR, tests, scripts) without a bundler.

## Quick Start

```tsx
import { useMemo, useState } from 'react';
import { useWSE, type UseWSEConfig } from 'wse-client';

function Prices({ signedIn }: { signedIn: boolean }) {
  const [prices, setPrices] = useState<Record<string, number>>({});

  // Handlers are registered by message type (the "t" your server publishes).
  const config = useMemo<UseWSEConfig>(() => ({
    endpoints: ['ws://localhost:5007/wse'],
    registerHandlers: (processor) => {
      processor.registerHandler('price_update', (msg) => {
        setPrices((prev) => ({ ...prev, [msg.p.symbol]: msg.p.price }));
      });
    },
  }), []);

  const { connectionHealth, stats } = useWSE(
    signedIn ? 'cookie' : undefined, // auth mode, see "Authentication"
    ['prices'],                      // topics
    config,
  );

  return (
    <div>
      Status: {connectionHealth} | Latency: {stats.avgLatency?.toFixed(1) ?? '-'} ms
      <ul>{Object.entries(prices).map(([s, p]) => <li key={s}>{s}: {p}</li>)}</ul>
    </div>
  );
}
```

## Authentication

Browsers cannot attach an `Authorization` header to a WebSocket, so the server authenticates a
browser by its **cookie** (`jwt_cookie_name` on the server, default `access_token`). The client never
sends the first argument of `useWSE` anywhere; it is the auth *mode*:

| Value | Meaning |
|-------|---------|
| `'cookie'` | Connect; the browser sends its cookies on the upgrade. Also enables the periodic `refreshAuthToken` call. Use it for a server without `jwt_secret` too. |
| `undefined` / `''` | Signed out: close the connection and clear per-user data (outgoing queues, IndexedDB offline queue, recovery positions, saved subscriptions, the opt-in error log). |
| any other string | Connect like `'cookie'`; a change from one non-empty value to another is treated as a different user (reconnect and clear per-user data). Do not pass a rotating JWT here - it is not sent, and each rotation would reset the session. A JWT-shaped value logs a warning. |

When the server rejects the credentials (close code 4401/4403, or an `AUTH_FAILED` error before the
close) the client calls `refreshAuthToken` once (if configured) and retries once; when that is
rejected too, or without `refreshAuthToken`, it **stops reconnecting**, enters `ERROR` and dispatches
`wseAuthFailed`. Sign the user in again and call `forceReconnect()` (or remount with a new auth value).

`refreshAuthToken` must reject when the refresh fails - with `fetch`, check `response.ok`:

```ts
refreshAuthToken: async () => {
  const res = await fetch('/api/auth/refresh', { method: 'POST', credentials: 'include' });
  if (!res.ok) throw new Error(`refresh failed: ${res.status}`);
},
```

In cookie mode it runs every 10 minutes. A failed periodic refresh never closes a healthy connection;
it is retried with backoff and reported with `wseTokenRefreshFailed`.

## useWSE Hook

```tsx
useWSE(token?: string, initialTopics?: string[], config?: UseWSEConfig): UseWSEReturn
```

- **token** - auth mode (see above)
- **initialTopics** - topics to subscribe to. Without them the signed-in user's saved subscriptions
  (stored per user id from `server_ready`) or `DEFAULT_TOPICS` are used
- **config** - options (below). Read when the connection is created; inline objects are fine

All `useWSE()` calls on a page share **one connection** per auth value and endpoint list. It is
created by the first consumer and closed one tick after the last consumer unmounts (React StrictMode
safe). Every returned function is **stable** - the same identity on every render, acting on live
state - so it is safe in effect dependency lists:

```tsx
useEffect(() => {
  subscribe([`chat:${channelId}`]);
  return () => unsubscribe([`chat:${channelId}`]);
}, [channelId, subscribe, unsubscribe]); // runs once per channel, never in a loop
```

### Return Values

```tsx
const {
  subscribe,          // (topics, options?: { recover?: boolean }) => void
  unsubscribe,        // (topics) => void
  forceReconnect,     // () => void - also leaves ERROR after an auth/encryption failure
  changeEndpoint,     // (endpoint) => void

  sendMessage,        // (type, payload, options?: MessageOptions) => 'sent' | 'queued' | 'dropped'
  sendBatch,          // (Array<{ type, payload, options? }>) => SendStatus[]

  stats,              // ConnectionMetrics, refreshed at most once per second
  activeTopics,       // string[]
  connectionHealth,   // ConnectionState
  diagnostics,        // NetworkDiagnostics | null (only with diagnostics.enabled)

  downloadDiagnostics,// () => void
  clearOfflineQueue,  // () => Promise<void>
  getQueueSize,       // () => number (in-memory + IndexedDB)
  requestSnapshot,    // (topics?) => void - sends a sync_request
  debugHandlers,      // () => void
  setCompression,     // (enabled) => void - ask the server to compress what it sends (or not)
} = useWSE(token, topics, config);
```

### Configuration

```tsx
useWSE('cookie', ['notifications'], {
  endpoints: ['ws://localhost:5007/wse'],     // default: same origin, path /wse

  refreshAuthToken: async () => { /* see Authentication */ },

  registerHandlers: (processor) => {          // application handlers by message type
    processor.registerHandler('my_event', (msg) => console.log(msg.p));
  },
  criticalHandlers: ['my_event'],             // refuse to start if these are missing
  eventThrottleMs: 0,                         // opt-in: drop same-type events faster than this (ms)

  reconnection: {
    mode: 'adaptive',                         // 'exponential' | 'linear' | 'fibonacci' | 'adaptive'
    baseDelay: 1000,                          // ms
    maxDelay: 30000,                          // ms, the backoff never exceeds this
    maxAttempts: -1,                          // consecutive failures before giving up; -1 = forever (default)
    factor: 1.5,                              // for 'exponential'
    jitter: true,
  },

  security: {
    encryptionEnabled: false,                 // require E2E encryption (fails closed, see below)
  },

  performance: {
    batchSize: 10,                            // messages per flush step of the outgoing queue
    batchTimeout: 500,                        // ms
    maxQueueSize: 10000,                      // capacity of the in-memory outgoing queue
    memoryLimit: 50 * 1024 * 1024,            // bytes the outgoing queue may hold; 0 = no bound
    compressionThreshold: 1024,               // ask the server to compress messages above this size; 0 = never
  },

  offline: {
    enabled: true,                            // allow { offline: true } sends
    maxSize: 1000,
    maxAge: 3600000,                          // ms
    persistToStorage: true,                   // IndexedDB
  },

  diagnostics: {
    enabled: false,                           // run the network monitor (no timer when false)
    metricsInterval: 60000,                   // ms between diagnostics updates
    sampleRate: 0.1,                          // share of handled messages timed (stats.handlerTimeAvg/Max)
    healthCheckInterval: 15000,               // heartbeat ping: round-trip time and dead-socket check
  },

  queryClient,                                // TanStack QueryClient to steer (see Adaptive Quality)

  rateLimit: { capacity: 1000, refillRate: 100 }, // outbound token bucket (msg burst, msg/s); false = off
  maxMessageSize: 16 * 1024 * 1024,           // inbound limit in bytes, checked while inflating
  recoveryTimeoutMs: 3000,                    // hold window for replays after a reconnect; 0 = off
  serverReadyTimeout: 10000,                  // proceed without server_ready (server without jwt_secret); 0 = wait
});
```

### Compression

The client tells the server in `client_hello` whether it wants compressed messages, and from which size
(`performance.compressionThreshold`). `setCompression(false)` asks the connected server to stop
compressing for this client at once (`client_config` message) and every later connection to start
without it; `setCompression(true)` turns it back on. A fast LAN client can skip the inflate work, a
mobile client on a slow link can ask for more compression.

### Adaptive Quality

Pass your TanStack Query client and its query defaults follow the measured connection:

```tsx
const queryClient = useQueryClient();
useWSE('cookie', topics, { queryClient });
```

| Quality | When | staleTime | refetchInterval | refetchOnWindowFocus |
|---|---|---|---|---|
| excellent | round trip under 75 ms, steady | Infinity | off | no |
| good | under 150 ms | 60 s | off | no |
| fair | under 300 ms, or 3+ reconnects in 5 minutes | 10 s | 30 s | yes |
| poor | slower, very jittery, or disconnected longer than 5 s | 5 s | 10 s | yes |

- The quality comes from the round trip of every heartbeat (`diagnostics.healthCheckInterval`), the
  connection state and recent reconnects. Nothing changes until the first round trip is measured.
- A worse quality applies at once (and refetches active queries when it drops to poor); a better one
  must be measured twice in a row, so a single fast heartbeat after a bad patch changes nothing.
- A short outage or a planned reconnect (`forceReconnect`, `changeEndpoint`) changes nothing.
- Only `staleTime`, `refetchInterval` and `refetchOnWindowFocus` are set; your other defaults stay,
  and your own values come back when the connection ends.
- Every change is also dispatched as the `connectionQualityChanged` window event
  (`detail: { quality, previous, strategy, connected, metrics }`), with or without a QueryClient when
  `diagnostics.enabled` is set. `AdaptiveQualityManager` is exported for use outside React.

## Receiving Messages

Register handlers by message type with `registerHandlers` (above). Messages that no handler claims
are dispatched as **one** window event, `wseMessage`, whose `detail` is the whole message:

```tsx
useEffect(() => {
  const onMessage = (e: Event) => {
    const msg = (e as CustomEvent).detail;   // { t, p, tp?, e?, o?, ... }
    if (msg.t === 'notification') notify(msg.p);
  };
  window.addEventListener('wseMessage', onMessage);
  return () => window.removeEventListener('wseMessage', onMessage);
}, []);
```

Other window events the client dispatches:

| Event | When |
|-------|------|
| `wseGap` | Messages of a recovery-stamped topic were skipped (`WSEGapEvent`: topic, epoch, fromOffset, toOffset, missed, reason) |
| `wseAuthFailed` | The client stopped reconnecting because the server rejected the credentials |
| `wseEncryptionFailed` | Encryption was required but could not be established; the connection was closed |
| `wseTokenRefreshFailed` | A periodic `refreshAuthToken` call failed (connection kept, retried later) |
| `wseReconnectFailed` | `reconnection.maxAttempts` consecutive attempts failed |
| `connectionQualityChanged` | The measured connection quality changed (see Adaptive Quality) |
| `presenceJoin` / `presenceLeave` / `presenceUpdate` | Presence events (`presence_join` / `presence_leave` / `presence_update`) |
| `subscriptionUpdate` | The server answered a subscription request |
| `serverError`, `rateLimitExceeded`, `clientRateLimitExceeded` | Server errors and rate limits |
| `snapshotComplete` | The server finished sending a snapshot |

## Sending Messages

```tsx
const { sendMessage } = useWSE('cookie', topics, config);

sendMessage('chat_message', { text: 'Hello' });                          // 'sent' | 'queued' | 'dropped'
sendMessage('alert', { msg: 'critical' }, { priority: MessagePriority.CRITICAL });
sendMessage('query', { filter: 'active' }, { correlation_id: 'req-123' });
sendMessage('draft', { text }, { offline: true });                       // survives reloads (IndexedDB)
sendMessage('quote_request', { symbol }, { ttl: 5000 });                  // worthless after 5 s
```

Nothing is lost because the connection was busy: a message that cannot go out at once (before
`server_ready`, while reconnecting, before the encryption key exists, or over the rate limit) is kept
and sent automatically, in priority then FIFO order, as soon as possible. `'dropped'` means the
outgoing queue was full or the message cannot be sent safely (see encryption).

- `priority`: when the outgoing queue is full (by `maxQueueSize` or by `memoryLimit`), a message can
  push out only messages of lower priority; otherwise it is dropped.
- `ttl`: milliseconds the message stays worth sending. One still queued after that (waiting for the
  connection, in memory or in IndexedDB) is dropped instead of sent late, and counted in
  `stats.messagesDropped`.
- `offline`: persist to IndexedDB while disconnected and replay after reconnecting, also after a reload.
- `encrypted`: refuse to send unless E2E encryption is on; never falls back to plaintext.
- `correlation_id`: sent as `cid`.

## Reconnection and Close Codes

- Any close the server or network initiates - `1000`/`1001` (server stop), `1005` (kick), `1006`,
  `1011`-`1013`, `4300` (drain), `4429` - is followed by a reconnect with backoff.
- `4401`/`4403` stop reconnecting (see Authentication).
- A single failed attempt is never terminal. With the default `maxAttempts: -1` the client retries
  forever; the backoff resets only when a connection reaches `server_ready`.
- It reconnects at once when the browser goes back online or the tab becomes visible again, and
  replaces a socket that went silent (no data for 2.5 server ping intervals).
- A close the client initiates (`forceReconnect`, `changeEndpoint`, unmount, sign-out) never
  disturbs the next socket.

## Recovery and Gaps

When the server runs with `recovery_enabled`, topic publications carry `tp`/`e`/`o`
(topic, epoch, offset). The client:

- drops duplicates by `(epoch, offset)`;
- delivers past a gap at once and reports it with `wseGap` (and `stats.gapsDetected` /
  `stats.messagesMissed`) - a topic is never blacked out;
- after a reconnect resubscribes with `recover: true` from the **last delivered** offset of each
  topic, and holds out-of-order messages for up to `recoveryTimeoutMs` while the replay fills the hole;
- forgets a topic's position when you unsubscribe from it.

## Encryption

With `security.encryptionEnabled: true` the client **fails closed**:

- Only the control frames `client_hello` (which carries the client's public key), heartbeat `ping`
  and `PONG` are ever sent in plaintext. Every application message, including the resubscribe, waits
  until the ECDH key exchange completes and is then sent AES-GCM encrypted.
- Encrypted frames that arrive while the key is being derived are kept and decrypted once it exists.
- If the server does not confirm encryption in `server_hello`, reports `KEY_EXCHANGE_FAILED`, or no
  key arrives in time, the connection is closed, the state becomes `ERROR` and `wseEncryptionFailed`
  is dispatched. Nothing is sent in plaintext instead.
- Web Crypto needs a secure context (https:// or localhost); without it an encrypted session does not
  connect at all.
- A message sent with `{ encrypted: true }` on a session without encryption is refused (`'dropped'`).

## Stores (Zustand)

For accessing WSE state outside of React components:

```tsx
import { useWSEStore, useMessageQueueStore } from 'wse-client';

const state = useWSEStore.getState().connectionState;
const metrics = useWSEStore.getState().metrics;
const queueSize = useMessageQueueStore.getState().size;

// Select narrowly in components to avoid re-rendering on every message:
const health = useWSEStore((s) => s.connectionState);
```

## Logging

The logger defaults to `WARN` in production and `INFO` in development (`WSE_LOG_LEVEL` /
`VITE_LOG_LEVEL` override it). Error entries are kept in localStorage only after
`logger.setPersistErrors(true)`, and never with their data.

## Features

| Feature | Description |
|---------|-------------|
| **Auto-reconnection** | Exponential, linear, fibonacci or adaptive backoff with jitter, infinite by default |
| **Offline queue** | `{ offline: true }` messages persist in IndexedDB and replay after reconnecting |
| **E2E encryption** | ECDH P-256 + AES-GCM-256, per-connection keys via Web Crypto; fails closed |
| **Compression** | Inflates `C:` (zlib) frames from the server with a size limit |
| **MessagePack** | Decodes `M:` binary frames via `@msgpack/msgpack` |
| **Rate limiting** | Configurable client-side token bucket (default 1000 burst, 100 msg/s); control frames exempt |
| **Circuit breaker** | Pauses reconnects after 5 consecutive failures (60 s), reset by `forceReconnect` |
| **Idempotent delivery** | Per-topic `(epoch, offset)` dedup, gap reporting, recovery from the last delivered offset |
| **Connection pool** | Multi-endpoint with health scoring |
| **Network monitor** | Latency and jitter from PONG round-trips (with `diagnostics.enabled`) |
| **Priority queues** | 5 priority levels from CRITICAL to BACKGROUND |
| **Token refresh** | `refreshAuthToken` every 10 minutes (cookie mode) and once after an auth rejection |

## Exports

```tsx
import { useWSE } from 'wse-client';
import type { UseWSEConfig, UseWSEReturn, SendStatus, WSEGapEvent, MessageOptions } from 'wse-client';
import { MessagePriority, ConnectionState, ConnectionQuality, CircuitBreakerState } from 'wse-client';
import { useWSEStore, useMessageQueueStore } from 'wse-client';
import {
  ConnectionManager, MessageProcessor, RateLimiter, ConnectionPool,
  NetworkMonitor, OfflineQueue, EventSequencer,
} from 'wse-client';
import { logger, CircuitBreaker, createCircuitBreaker, generateId, compressionManager, eventTransformer } from 'wse-client';
import { EventHandlers, registerAllHandlers } from 'wse-client';
import { WSE_VERSION, WS_CLIENT_VERSION, WS_PROTOCOL_VERSION, clearWSEStorage } from 'wse-client';
```

## Upgrading from 2.x

- The package is ESM-only (`"type": "module"`).
- `maxAttempts` defaults to `-1` (infinite) and `-1` really means infinite; the store no longer caps
  reconnects at 10.
- Server closes `1000`/`1001` reconnect; `4401`/`4403` stop reconnecting.
- `sendMessage` returns `'sent' | 'queued' | 'dropped'`; queued messages are no longer lost.
- Messages are no longer dropped for their timestamp age, and `heartbeat`/`health_check`/`metrics_response`
  events are no longer throttled by name (`eventThrottleMs` remains as an opt-in).
- Options that used to do nothing now work: `AdaptiveQualityManager` with the `queryClient` option
  measures the connection and leaves your other query defaults alone (it used to set every query to
  15 s polling); `setCompression()` and `performance.compressionThreshold` reach the server;
  `performance.memoryLimit` bounds the outgoing queue; `MessageOptions.ttl` drops stale messages;
  `diagnostics.sampleRate` times handlers; `diagnostics.healthCheckInterval` sets the heartbeat.
- `wseRecoveryGap` became `wseGap` (with the missing range) and no longer triggers a resubscribe.
- The periodic refresh failure event is `wseTokenRefreshFailed` (was `authTokenExpired`, which also
  closed the connection).
- Saved subscriptions are stored per user id; the 2.x format is only reused when the server reports no user.
- The `topics` query parameter is no longer added to the WebSocket URL (the server never read it).

## Wire Protocol

Speaks WSE wire protocol v1, identical to the Python client:

- **Text frames:** JSON with `c` field for category (`WSE`, `S`, `U`)
- **Binary frames:** Codec prefix (`C:` zlib, `M:` msgpack, `E:` AES-GCM) + payload
- **Recovery stamp:** topic publications carry `tp` (topic), `e` (epoch, 8-hex), `o` (offset)
- **Heartbeat:** JSON PING/PONG with latency tracking
- **Protocol negotiation:** `client_hello`/`server_hello` handshake with feature discovery

Full protocol specification: [PROTOCOL.md](../docs/PROTOCOL.md)

## Requirements

- React 18+
- zustand 4+ (peer dependency)
- A browser, or Node.js with a global `WebSocket` (Node 22+)

## License

MIT
