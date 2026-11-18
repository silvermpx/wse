# wse-client (TypeScript)

TypeScript client for [WSE](https://github.com/silvermpx/wse), the WebSocket engine for reactive user interfaces: topics, message recovery, reconnects, an outbox that sends each message once (offline too), end-to-end encryption and signed messages. It runs without a framework, and comes with adapters for React, Vue, Svelte and React Native.

[![npm](https://img.shields.io/npm/v/wse-client)](https://www.npmjs.com/package/wse-client)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0+-blue)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](../LICENSE)

## Installation

```bash
npm install wse-client
```

| Import | What it is | Needs |
|--------|------------|-------|
| `wse-client/core` | The client without a framework: pages, workers, Node | nothing |
| `wse-client/react` | A provider and hooks over a core client | `react >= 18` |
| `wse-client/vue` | A plugin and composables over a core client | `vue >= 3.3` |
| `wse-client/svelte` | Stores over a core client | nothing (Svelte's store contract) |
| `wse-client/react-native` | A core client for React Native apps | `react-native` |
| `wse-client/node` | The outbox in a file, for Node and Electron's main process | Node |
| `wse-client/bridge` | One connection for every window: an owner and the windows' clients | nothing |
| `wse-client/electron` | Electron: the connection in the main process, a client in each window | Electron |
| `wse-client/tauri` | Tauri: a window's client over the Rust side's connection (`tauri-plugin-wse`) | Tauri 2 |
| `wse-client` | The page-wide `useWSE` hook (described below), as in 3.0 | `react >= 18`, `zustand >= 4` |

React, Vue and zustand are optional peer dependencies: an application installs only what the
import it uses needs.

The package is ESM (`"type": "module"`, explicit `.js` import paths). It loads in bundlers
(Vite, webpack, esbuild, Next.js) and directly in Node.js (SSR, tests, scripts) without a bundler.

## A Client of Its Own (core)

```ts
import { createWSEClient } from 'wse-client/core';

const client = createWSEClient({ endpoints: ['wss://example.com/wse'] });
const lease = client.retain();                          // the first share connects
const prices = client.subscribe(['prices']);            // held until prices.release()
const stop = client.onMessage('price_update', (msg) => render(msg.p));
client.state.subscribe((s) => showStatus(s.connection)); // now, then on each change
client.send('order', { id: 1 });
client.send('open_valve', { id: 7 }, { live: true });   // on this connection or not at all
client.pause('background');                              // the connection ends, positions stay
client.resume('background');                             // back, and what was missed comes
client.wake();                                           // the network is back: connect now
await client.close(2000);                                // send what it holds (2 s at most), then end
client.signOut();                                        // the user's queue, topics and positions go
```

A client holds nothing of the page: two clients on one page share no state, and importing
`wse-client/core` reads or changes nothing. It connects while any share (`retain()`) is held and
stops when the last is released; `subscribe(topics)` holds topics the same way, and `onMessage` and
`on` take any number of listeners. With `storage: 'myapp:alice'` it keeps its topics in the page's
storage under that name, apart for each consumer and realm (staging beside production keeps its
own); by default it keeps them in memory. `destroy()` ends it and keeps what it stored; `signOut()`
ends it and forgets the user.

Outside a page (Node, Electron's main process, a worker) the client hears no `online` or
visibility events: `wake()` tells it the network or the machine is back, so a client waiting out
its backoff connects at once and a connection silent past its idle limit (the machine slept) is
made again. `close(deadlineMs)` sends what the client holds within that time, then ends as
`destroy()` does, for a host that quits. A storage named for the page where the runtime has none
keeps the topics and the checkpoint in memory, and says so with a `wseStorageError` (operation
`open`) as the client starts; the outbox still goes to IndexedDB where the runtime has it, and
`state.getSnapshot().storage` reads `unavailable` when it cannot.

The outbox (see [Sending Messages](#sending-messages)) outlives the page only for a part of the application named by a
stable `consumer` (`consumer: 'checkout'`): it lives in an IndexedDB database of its own scope -- the
storage name, the consumer and the realm (the first endpoint, or `realm`) -- so another server or
another part of the application never reads it. Each client of the scope (each tab) keeps a producer
of its own there, held by a Web Lock while the tab lives, and sends what it holds on its own
connection, as its own user. A page that opens takes over what a closed or crashed tab of the scope
left (or the page it reloads), and sends it under that tab's producer, so the server still takes each
message once; another user's connection drops it (`user_changed`). The changes of a turn of the event
loop go to the database in one transaction. The page that goes (`pagehide`) lets its producer go, so
the back/forward cache can keep it, and reads the database again when it is back. Web Locks need a
secure context (HTTPS, or localhost): without them the outbox stays in memory. What the queue of 3.1
kept in the database is taken over and sent, each message for its user only.
`state.getSnapshot().storage` says where the outbox lives (`memory`, `durable`, `unavailable`), each
change is a `wseStorage` event, and a storage that fails (a database that will not open, a full
quota, another page deleting the database) is a `wseStorageError` event with the operation and the
error's name, never silent; the outbox goes on in memory and writes the database anew once it can.

Outside browsers, a `webSocket` factory makes the socket, and may send headers (a token in
`Authorization`).

## React, Vue, Svelte, Angular, Solid

```tsx
import { createWSEClient, WSEProvider, useWSEState, useWSETopics, useWSEMessage } from 'wse-client/react';

const client = createWSEClient({ endpoints: ['wss://example.com/wse'] });

function Prices() {
  const { connection } = useWSEState();               // useSyncExternalStore
  useWSETopics(['prices']);                            // held while mounted
  useWSEMessage('price_update', (msg) => update(msg.p));
  return <span>{connection}</span>;
}

root.render(<WSEProvider client={client}><Prices /></WSEProvider>);
```

```ts
// Vue 3: app.use(wsePlugin(client)), then in setup()
import { useWSEState, useWSETopics, useWSEMessage } from 'wse-client/vue';
const state = useWSEState();                           // a shallow ref
useWSETopics(['prices']);
useWSEMessage('price_update', (msg) => update(msg.p));

// Svelte: stores, $-prefixed in components
import { wseState, wseMessage } from 'wse-client/svelte';
const state = wseState(client);
const lastPrice = wseMessage(client, 'price_update', ['prices']);
```

```ts
// Angular 20+: plain functions for an injection context, no decorators
import { provideWSEClient, injectWSEState, injectWSETopics, injectWSEMessage, createWSEClient } from 'wse-client/angular';

bootstrapApplication(App, {
  providers: [provideWSEClient(() => createWSEClient({ endpoints: ['wss://example.com/wse'] }))],
});

@Component({ selector: 'app-prices', template: '{{ state().connection }}' })
class Prices {
  state = injectWSEState();                            // a signal
  topic = signal('prices');
  constructor() {
    injectWSETopics(() => [this.topic()]);             // a list, a signal or a computation
    injectWSEMessage('price_update', (msg) => update(msg.p));
  }
}
```

`provideWSEClient` makes one client per injector (per request on a server) and destroys it with
the injector. The `inject*` functions take the client's connection, topics and listeners after the
first render in a browser, outside Angular's zone, and let them go with their injector; a changed
topic list is taken before the old one is let go, so a topic in both keeps its recovery position. A
message handler gets the client's own message object (commit it when your application has applied
it), and its failure goes to the application's `ErrorHandler`.

```tsx
// Solid 1.9 and 2.0: functions for a component, no JSX in the library
import { WSEContext, createWSEState, createWSETopics, onWSEMessage } from 'wse-client/solid';

function Prices() {
  const state = createWSEState();                      // an accessor
  createWSETopics(() => [topic()]);                    // a list, or an accessor of one
  onWSEMessage('price_update', (msg) => update(msg.p));
  return <span>{state().connection}</span>;
}

<WSEContext.Provider value={client}><Prices /></WSEContext.Provider>  // Solid 2: <WSEContext value={client}>
```

Each function takes the client as its last argument, as an accessor of one (a new client after a
sign-in: it is taken before the old one is let go), or from `WSEContext`. They take the client's
connection, topics and listeners after the first render, never on a server or while hydrating, and
let them go with their owner. The same tests run on Solid 1.9 and on 2.0, and the server render on
Solid 1.9.

The provider, the composables and the stores hold the client, its topics and listeners while a
component uses them. Effects and mount hooks do not run while rendering on a server, so server
rendering (Next.js, Nuxt, SvelteKit) opens no socket and leaves no timer: the page shows the
client's first state (`pending`), hydrates without a mismatch, and connects in the browser. The
tests render React, Vue and a compiled Svelte 5 page on a server without a window and hydrate
them, and render Angular on a server, two requests at once, each with a client of its own. On a
server a client never uses a localStorage of the runtime's own (Node 25 has one).

In the browser a client of its own follows the page: `pagehide` pauses it for the reason `page`
(a page in the back/forward cache holds no socket), `pageshow` resumes it, and it recovers what was
missed. A page back on screen, or back online, does not trust a socket that heard nothing for longer
than the idle limit (a machine that slept): it connects again. The page-wide `useWSE` does the same
with `pagehide` and `pageshow`; it no longer listens to `beforeunload`, which keeps some browsers
from caching the page.

## React Native

```ts
import { AppState } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';
import QuickCrypto from 'react-native-quick-crypto';
import { createReactNativeClient, storageOutbox } from 'wse-client/react-native';

const client = createReactNativeClient({
  endpoints: ['wss://example.com/wse'],
  token: () => auth.accessToken(),   // sent as Authorization: Bearer at each connection
  appState: AppState,                // pauses 5 s after the app goes to the background
  crypto: QuickCrypto,               // for end-to-end encryption and signed messages
  offline: { store: storageOutbox(AsyncStorage) },  // what waits outlives the app
});
```

The client pauses while the app is in the background and reconnects in the foreground, recovering
what it missed. Use it with the React adapter as with any core client.

`storageOutbox(storage, name?)` keeps the outbox in the app's storage, so that what waits outlives
the app being closed or killed: AsyncStorage, a storage with its API (expo-sqlite's kv-store), or an
MMKV instance. Each message is a key of its own (`wse-outbox:wse:<number>`), the producer one more;
the changes of a turn of the event loop go in one batch (`multiRemove`, then `multiSet`), so an app
killed in between loses nothing it kept before and keeps nothing that went. A storage that fails is a
`wseStorageError` event, and the outbox goes on in memory. Nothing of React Native is imported: the
app passes the storage, as it passes `AppState`.

## One Connection for Every Window (bridge)

An application of several windows (Electron, Tauri, a page and its workers) keeps one connection:
an owner holds it, with the token, the keys and the outbox, and each window holds a client with
the `WSEClient` interface over a channel to the owner. The adapters for React, Vue, Svelte, Angular
and Solid take a window's client as they take a client of their own.

```ts
import { createBridgeClient, createBridgeOwner, messagePortChannel } from 'wse-client/bridge';

// Where the connection lives (Electron's main process, a worker):
const owner = createBridgeOwner({ endpoints: ['wss://example.com/wse'] });
const { port1, port2 } = new MessageChannel();
owner.attach(messagePortChannel(port1));

// In a window, with the other port:
const client = createBridgeClient(messagePortChannel(port2));
client.retain();
client.subscribe(['prices']);
client.onMessage('price_update', (message) => render(message.p));
```

The owner subscribes the union of the windows' topics once each, and hands each window what it
holds and listens to, in batches it acknowledges as it takes them: at most two in hand, so a window
that falls behind holds back only its own queue, and one past 10,000 messages (or 64 MiB) loses its
queue and hears its topics broke off (`wseOverloaded`; a recover-or-resync window resyncs them).
A window's sends go through the owner's outbox (`send` says `queued`, a live one `sent`), and what
became of each (`wseDelivered`, `wseNotDelivered`; `cleared` for what the owner held when the user
signed out) reaches that window alone, whatever ids the windows chose. What depends on the
application stays in the window: its listeners, the delivery's barrier, resync, commit and
checkpoint, the sends that wait for their topics; after a reconnect, a window's send that requires
topics waits in the owner's outbox until that window resynced them. A window's `pause` holds back
its stream and gives its share of the connection back, so the connection ends when no one else
holds one, as a client of its own does; `signOut` signs every window out. No frame carries a
token, a key or a cookie.

## Electron

The connection lives in the main process, with the token and the outbox, and each window's page
holds a client over a port the preload hands it:

```ts
// The main process.
import { app, BrowserWindow, ipcMain, MessageChannelMain } from 'electron';
import { join } from 'node:path';
import { createWSEOwner, wsePreloadPath } from 'wse-client/electron';
import { fileOutbox } from 'wse-client/node';

await app.whenReady();
const owner = createWSEOwner({
  electron: { ipcMain, MessageChannelMain },
  endpoints: ['wss://example.com/wse'],
  token: () => currentToken(),            // Authorization: Bearer, asked at each attempt; never in a window
  offline: { store: fileOutbox(join(app.getPath('userData'), 'wse-outbox')) },
});

const win = new BrowserWindow({ webPreferences: { preload: wsePreloadPath } });
owner.serve(win.webContents);
await win.loadFile('index.html');
```

```ts
// A window's page.
import { createElectronClient } from 'wse-client/electron/renderer';

const client = createElectronClient();   // a WSEClient: <WSEProvider client={client}> and the other adapters take it
client.retain();
client.subscribe(['prices']);
```

An application with a preload of its own imports `wse-client/electron/preload` there instead of
giving the window `wsePreloadPath` (a main process the application bundles does the same). The
preload needs nothing but Electron's `ipcRenderer`, and runs in a sandboxed renderer. A page gets a
port only from the top frame of a window the owner serves, at a URL the application allows: by
default its own pages (a file, a scheme of its own such as `app://`, or a development server on
`localhost`), never a page of the web; `allow: (url) => ...` decides otherwise. A page that goes
lets its share go, and a window that closes lets all of its pages go. `owner.signOut()` signs every
window out and forgets what the client kept for the user; the next user gets a new owner. The main
process connects with Node's WebSocket (Node 22 and newer); with an older Electron, pass a
`webSocket` factory.

An example application, two windows on one connection, is in
[bindings/electron/example](https://github.com/silvermpx/wse/tree/main/bindings/electron/example);
its `main.ts` says how to run it against the example server.

## Tauri

The connection lives on the Rust side, in the `tauri-plugin-wse` crate (an owner of
`wse_client::bridge`, with the token and the outbox), and each window's page holds a client through
the plugin's commands:

```rust
use tauri_plugin_wse::{WseExt, wse_client};

tauri::Builder::default()
    .plugin(tauri_plugin_wse::init())
    .setup(|app| {
        let mut config = wse_client::Config::new("wss://example.com/wse");
        config.token = Some(token); // the signed-in user's
        // It returns at once: the connection is made in the background.
        tauri::async_runtime::block_on(app.wse().start(config))?;
        Ok(())
    })
```

The windows that may use it get the plugin's permission, `wse:default`, in their capabilities. Then
in a page:

```ts
import { invoke, Channel } from '@tauri-apps/api/core';
import { createTauriClient } from 'wse-client/tauri';

const client = createTauriClient({ invoke, Channel });   // a WSEClient, as any
client.retain();
client.subscribe(['prices']);
```

Tauri's own API is passed in, so the package depends on no version of it (`window.__TAURI__.core`
gives the same with `withGlobalTauri`). The page's frames go one command at a time, in order, those
of one turn together; the owner's come on a Channel. A page that reloads, or a window that closes,
lets its share go; `app.wse().start(config)` again (another user) lets the last connection's pages
go, and `app.wse().sign_out()` lets every page go and forgets what the connection kept for the
user. An example application is in
[bindings/tauri/example](https://github.com/silvermpx/wse/tree/main/bindings/tauri/example).

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
| `undefined` / `''` | Signed out: close the connection and clear per-user data (outgoing queues, the outbox and its IndexedDB database, recovery positions, saved subscriptions, the opt-in error log). |
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
  sendSigned,         // (type, payload, { nonce?, priority? }) => Promise<SendStatus>

  stats,              // ConnectionMetrics, refreshed at most once per second
  activeTopics,       // string[]
  connectionHealth,   // ConnectionState
  diagnostics,        // NetworkDiagnostics | null (only with diagnostics.enabled)

  downloadDiagnostics,// () => void
  clearOfflineQueue,  // () => Promise<void> - what waits in the outbox goes ('cleared')
  getQueueSize,       // () => number (the outgoing queue and the outbox)
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
    // circuitBreaker: { failureThreshold: 5, resetTimeout: 60000 }, // pause after failures in a row (off by default)
  },

  security: {
    encryptionEnabled: false,                 // require E2E encryption (fails closed, see below)
    serverKeys: [SERVER_KEY],                 // with encryption: the server's identity key(s) to pin
    // unauthenticated: true,                 //   or accept any server (passive listeners only)
    // legacyE2e: true,                       //   or the first design, for servers before 3.0
    signingKey: keyPair,                      // this client's P-256 key pair for sendSigned()
    // bindSession: false,                    //   with encryption, a key bound by set_client_key() (see below)
    serverSigningKeys: [SERVER_SIGNING_KEY],  // the server's signing key(s) its signed messages are checked against
  },

  wireFormat: 'json',                         // 'msgpack': msgpack both ways (not inside E2E frames)

  performance: {
    batchSize: 10,                            // messages per flush step of the outgoing queue
    batchTimeout: 500,                        // ms
    maxQueueSize: 10000,                      // capacity of the in-memory outgoing queue
    memoryLimit: 50 * 1024 * 1024,            // bytes the outgoing queue may hold; 0 = no bound
    compressionThreshold: 1024,               // ask the server to compress messages above this size; 0 = never
  },

  offline: {
    enabled: true,                            // the outbox: every message waits for the server's answer
    maxSize: 100000,                          // messages that wait at most, those not answered yet included
    maxBytes: 16 * 1024 * 1024,               // bytes of messages that wait at most
    maxAge: 3600000,                          // ms a message may wait to be written first
    persistToStorage: true,                   // with a consumer named, the outbox outlives the page (IndexedDB)
    // store: fileOutbox(path),               // Node, Electron's main process: the outbox in a file
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

Register handlers by message type with `registerHandlers` (above). The client handles its own
protocol messages first (`server_ready`, `subscription_update`, `PONG`, snapshots, presence and the
like): a handler for one of those types runs after the client's and cannot replace it, and
`clearHandlers()` clears only the application's. Messages that no handler claims are dispatched as
**one** window event, `wseMessage`, whose `detail` is the whole message:

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
sendMessage('order', { item: 'tea' }, { id: 'order-17' });              // its outcome names it
sendMessage('alert', { msg: 'critical' }, { priority: MessagePriority.CRITICAL });
sendMessage('query', { filter: 'active' }, { correlation_id: 'req-123' });
sendMessage('quote_request', { symbol }, { ttl: 5000 });                  // worthless after 5 s
sendMessage('open_valve', { id: 'v-7' }, { live: true });                 // now or never
```

Every message, except a live or signed one, waits in the outbox until the server answers for it,
goes again after a reconnect, and is numbered: a server of 3.2 drops a repeat of one it took. What
the server cannot tell (the client comes from a cluster node that is gone, or its handler panicked
on the message) reaches the server's application marked `possible_repeat`, and an application that
must apply an order once checks the message's `id` against what it applied. The client says what
became of each, by its id: `wseDelivered`
(`{ ids }`) once the server took it, `wseNotDelivered` (`{ reason, ids }`) when it will not:
the server refused it (`busy`, `rate_limited`, `missing`, `not_ready`), it was not written within
its time (`expired`), it was the last user's (`user_changed`), the user signed out (`cleared`), or
the client ended with it (`closed`). The outbox sends in the order of sending, whatever the
priority. `close(deadlineMs)` waits for the server's answers too. With a server older than 3.2 a
message is gone once written, as before. `offline: { enabled: false }` has no outbox: a message
that cannot go out at once waits in the outgoing queue, in priority then FIFO order. `kept()` says
how many messages wait for the server's answer.

In Node and in Electron's main process the outbox can live in a file, so that what waits outlives
the process:

```ts
import { createWSEClient } from 'wse-client/core';
import { fileOutbox } from 'wse-client/node';

const client = createWSEClient({
  endpoints: ['wss://api.example.com/wse'],
  offline: { store: fileOutbox('/var/lib/app/wse-outbox') },
});
```

It is the file the Rust and Python clients write: each line checked with a CRC-32 and a salt, read
up to the first that does not check. The event loop never waits for the disk: a batch of lines goes
to the file and to the disk once per turn of the loop, the next batch taking what came meanwhile,
and the file is written anew once what went outweighs what is kept. A message goes on the
connection once the file holds it, and that it may have gone, so that after a crash its number is
never another message's (IndexedDB and the React Native storage alike). When the process ends, what is
not written yet is written at once. The file is the client's alone while the process runs: the
system holds it for the process (a lock on `wse-outbox.lock` on macOS, the file opened for this
process alone on Windows, an abstract socket named after the file on Linux, which needs Node 22 or
newer) and lets go when the process ends, however it ends. A file that cannot be opened or written is a `wseStorageError` event
and the outbox goes on in memory; `state.getSnapshot().storage` reads `durable` while the file
holds it. `destroy()` leaves what waits in the file for the next client, `signOut()` clears it.

`'dropped'` means the outbox (or the outgoing queue) was full or the message cannot be sent safely
(see encryption).

- `priority`: when the outgoing queue is full (by `maxQueueSize` or by `memoryLimit`), a message can
  push out only messages of lower priority; otherwise it is dropped.
- `id`: the message's id, the application's own (one it keeps itself, say); by default a random
  UUID. The outcome events name it.
- `ttl`: milliseconds the message stays worth sending. One not written within it (waiting for the
  connection, in the outbox or in IndexedDB) is dropped instead of sent late (`wseNotDelivered`,
  `expired`); once written it waits for the server's answer.
- `offline`: ignored since 3.2. Every message but a live one waits in the outbox, which outlives the
  page when the client has a store (IndexedDB for a durable scope).
- `live`: only on the connection that is ready now. It is never kept for a later connection, persisted
  or replayed after a reconnect; without a ready connection it is
  `'dropped'`. For commands that must not run late (a valve, a door, an order). Signed messages
  (`sendSigned`) are always live.
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
- on a gap, asks the server to replay the topic from the **last delivered** offset, on a live
  connection as after a reconnect, and holds the messages after the hole for up to
  `recoveryTimeoutMs` while the replay fills it; a replay the server cut at its budget is asked for
  again from where delivery got to;
- what no replay fills is delivered past the hole and reported with `wseGap` (and
  `stats.gapsDetected` / `stats.messagesMissed`): a topic is never blacked out, and a stream the
  server said it cannot replay is not asked for again;
- forgets a topic's position when you unsubscribe from it.

### Topic states and recover-or-resync

A client of its own says of each topic it holds whether the server took it and whether its
messages run without a break: `state.getSnapshot().topicStates.prices` is
`{ subscription: 'requested' | 'active' | 'denied', continuity: 'continuous' | 'resync_required' |
'resyncing', reason }`, and each change of continuity is a `wseContinuity` event with the topic and
the reason (`offset_gap`, `not_recovered`, `epoch_changed`, `unsafe_offset`, `overflow`, ...).

By default (`delivery: { mode: 'best-effort' }`) messages keep coming after a break; the
application reloads what depends on the topic and calls `client.resync(['prices'])`, or gives a
`resync` handler that the client calls itself.

Where every fact counts, `recover-or-resync` puts a barrier in front of each topic that is not
continuous: from a cold start, after every reconnect and after a break, its messages are held
until the application's handler brought its state up to date:

```ts
const client = createWSEClient({
  endpoints: ['wss://example.com/wse'],
  storage: 'board:alice',
  consumer: 'board',                   // the one consumer that commits
  delivery: {
    mode: 'recover-or-resync',
    schema: '3',                       // a checkpoint of another schema is refused
    resync: async ({ topics, reasons, committed, signal }) => {
      const state = await fetch(`/api/board?since=${JSON.stringify(committed)}`, { signal });
      const { version } = await apply(await state.json());
      // Of what came meanwhile, drop what the state has already.
      return { includes: (msg) => msg.p.version <= version };
    },
  },
});
client.onMessage('order_update', (msg) => {
  applyToStore(msg.p);                 // safe to apply twice
  client.commit(msg);                  // the checkpoint moves through what was applied
});
client.send('cancel', { id: 9 }, { requires: ['orders'] }); // goes once orders is continuous
```

The checkpoint is what the consumer committed, per stream only through what was applied without a
hole. It is bound to the realm (the first endpoint, or `realm`), the user, the consumer
and the schema; the client keeps it in its storage, and `client.checkpoint()` gives it to save with
the application's own state (pass it back as `delivery.checkpoint`). A checkpoint of anything else
is refused with a `wseCheckpointRejected` event. After a reconnect the recovery starts from it, so
a message that came but was not applied comes again: the application applies repeats safely. A
message handed on before the connection became another user's counts for nobody when it is
committed later. A send that requires topics waits in the outgoing queue, in its place, until each
of them is continuous on the connection, and then joins the outbox with them: each time it goes,
after a reconnect too, they are checked right before it goes, so it does not go before the resync,
and while a topic the client holds is not continuous it waits in its place with what was sent
after it. Its `ttl` counts from the send. It belongs to the user who made it (dropped when the user
changes, never sent as the next one), and while it waits in the queue it counts against
`maxQueueSize` and `memoryLimit` with every other send.
Messages held past `maxHeld` (10,000), or a break during the resync, start the resync over; a
handler that fails is called again (1 s, then twice as long, up to 30 s). Offsets JavaScript cannot
hold exactly (above 2^53 - 1) are refused and break the topic off.

### Bounds

What the client takes on has a bound in messages and bytes (`performance`): inbound frames waiting
to be processed (`maxPendingInbound`, 10,000; `maxPendingInboundBytes`, 64 MiB) and frames waiting
to be encrypted (`maxPendingEncrypted`, 256; `maxPendingEncryptedBytes`, 16 MiB), besides the
outgoing queue's `maxQueueSize` and `memoryLimit`. A browser WebSocket cannot slow the server down:
past the inbound bound the client drops the connection (a `wseOverloaded` event) and recovers what
it missed from where delivery is. Past the encryption bound a message waits in the outgoing queue,
in order, until the encryptions catch up; control frames keep their place in the sealed sequence.

## Encryption

With `security.encryptionEnabled: true` every frame is sealed for the server alone, and the server
proves who it is: pin its identity key, what its `e2e_identity_public_key()` returns, in
`security.serverKeys` (two during a key rotation). `security.unauthenticated: true` accepts any
server instead (safe only from passive listeners), `security.legacyE2e: true` speaks the first design
to servers before 3.0; with none of the three the session does not start. Setup, guarantees and the
wire format: [SECURITY.md](../docs/SECURITY.md#end-to-end-encryption).

The client **fails closed**:

- Only `client_hello` (which carries the offer) goes in plaintext. After the handshake every frame
  is sealed, both ways, heartbeats included; application messages, the resubscribe among them, wait
  for the handshake, and a heartbeat due meanwhile is skipped.
- If the server's identity does not check, a frame does not open (replayed, reordered, forged), or
  the server refuses (`KEY_EXCHANGE_FAILED`, `E2E_UNSUPPORTED`, no key in time), the connection is
  closed, the state becomes `ERROR` and `wseEncryptionFailed` is dispatched. Nothing is sent in
  plaintext instead.
- Web Crypto needs a secure context (https:// or localhost); without it an encrypted session does not
  connect at all.
- A message sent with `{ encrypted: true }` on a session without encryption is refused (`'dropped'`).

## Signed Messages

A message that may be disputed (an order) can go signed with this client's own key, and the
server's signed messages arrive with their proof:

```tsx
import { generateSigningKey } from 'wse-client';

const key = await generateSigningKey();   // the private key cannot be exported
// Keep key.keyPair (IndexedDB stores CryptoKeyPair as is). Your backend puts key.jwk in the
// token's "cnf" claim; the client proves the key inside its end-to-end session, so the token is
// of no use to anyone without the key.

const { sendSigned } = useWSE(token, topics, {
  security: {
    encryptionEnabled: true,
    serverKeys: [SERVER_KEY],
    signingKey: key.keyPair,
    serverSigningKeys: [SERVER_SIGNING_KEY],
  },
});
await sendSigned('order', { side: 'buy', qty: '1.5' }, { nonce: 'o-17' });
```

A backend can bind the key with `set_client_key()` instead and leave it out of the token; the
client then signs with or without encryption, and with encryption needs `security.bindSession: false`.

The server answers with a signed `receipt` message (`p.n` is the nonce); a message the server
signed arrives with `message.signature` (`body`, `sig`, `kid`, `verified`), the proof to keep. One no
pinned key verifies, or signed for another topic than the one it came on or for another user, is dropped. Signing is
asynchronous: await `sendSigned` before sending what must follow it. Setup and guarantees:
[SECURITY.md](../docs/SECURITY.md#signed-messages).

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

The logger defaults to `WARN` in production and `INFO` in development (`process.env.NODE_ENV`).
`WSE_LOG_LEVEL`, in `process.env` or set on `globalThis` before the client loads, overrides it, and so does
`logger.setLevel()`. Error entries are kept in localStorage only after
`logger.setPersistErrors(true)`, and never with their data.

## Features

| Feature | Description |
|---------|-------------|
| **Auto-reconnection** | Exponential, linear, fibonacci or adaptive backoff with jitter, infinite by default |
| **Outbox** | Every message waits until the server answers for it and goes again after a reconnect, taken once; in IndexedDB or a file it outlives the page or the process |
| **E2E encryption** | Sessions with a pinned server identity via Web Crypto; fails closed |
| **Compression** | Inflates `C:` (zlib) frames from the server with a size limit |
| **MessagePack** | `wireFormat: 'msgpack'`: msgpack both ways via `@msgpack/msgpack` (24-41% fewer bytes than JSON) |
| **Rate limiting** | Configurable client-side token bucket (default 1000 burst, 100 msg/s); control frames exempt |
| **Circuit breaker** | Off by default: the backoff alone spaces reconnects (at most `maxDelay` apart). `reconnection.circuitBreaker: { failureThreshold, resetTimeout }` pauses them after that many failures in a row; `forceReconnect` resets it |
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
- `wseRecoveryGap` became `wseGap` (with the missing range), reported once a replay did not fill the hole.
- The periodic refresh failure event is `wseTokenRefreshFailed` (was `authTokenExpired`, which also
  closed the connection).
- Saved subscriptions are stored per user id; the 2.x format is only reused when the server reports no user.
- The `topics` query parameter is no longer added to the WebSocket URL (the server never read it).

## Wire Protocol

Speaks WSE wire protocol v1, identical to the Python client:

- **Text frames:** JSON with `c` field for category (`WSE`, `S`, `U`)
- **Binary frames:** Codec prefix (`C:` zlib, `M:` msgpack, `E:` E2E sealed) + payload
- **Recovery stamp:** topic publications carry `tp` (topic), `e` (epoch, 8-hex), `o` (offset)
- **Heartbeat:** JSON PING/PONG with latency tracking
- **Protocol negotiation:** `client_hello`/`server_hello` handshake with feature discovery

Full protocol specification: [PROTOCOL.md](../docs/PROTOCOL.md)

## Requirements

- For `wse-client/core`: a browser, or Node.js with a global `WebSocket` (Node 22+)
- For `wse-client/electron`: Electron with Node 22 or newer in its main process, or a `webSocket` factory
- For `useWSE` (the root entry): React 18+ and zustand 4+; for the adapters: React 18+, Vue 3.3+,
  Svelte 5, Angular 20+ or Solid 1.9+ (optional peer dependencies)

## License

MIT
