## 3.2.0

- An outbox: every message sent, except a live or signed one, waits until
  the server answers for it and goes again after a reconnect, so a 3.2
  server takes each once; in memory, or in a file with
  `offline: wseOffline(path: ...)`. The `delivered` and `notDelivered` items
  say what became of each by its id; `sendWith` sends with the app's own
  id, a `ttl`, topics it requires, its correlation id and priority, or
  live; `clear()` empties it on sign-out.
- `WseClient.start`, beside `connect`, returns at once: the connection is
  made in the background, and what is sent meanwhile waits in the outbox.
- `sendRequiring`: a send that waits until its topics are continuous.
- A message item gives its correlation id, priority and time.
- The reconnect settings: the first wait, the longest, the factor, the
  jitter and the attempts before the client gives up.
- A connection that died while the device slept is made again at the first
  heartbeat after waking (15 s at most).
- Every message carries an `id`, as from the other clients.
- The conformance cases of the WSE repository run through this package and
  pass.

## 3.1.0

- The WSE client for Dart and Flutter: the Rust client through
  flutter_rust_bridge, with prebuilt libraries for Android, iOS, macOS, Linux
  and Windows, checked by their SHA-256.
