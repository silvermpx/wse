# wse-client-native

The [WSE](https://github.com/silvermpx/wse) Rust client as mobile bindings need it: plain types any binding generator
can carry (UniFFI for Swift and Kotlin, flutter_rust_bridge for Dart), one tokio runtime for every client of the
process, and calls that can be cancelled without losing anything.

- `NativeClient::connect(config, tokens, &cancel)` runs on the shared `runtime()` and gives up as soon as `cancel` is
  cancelled. `tokens` is the application's `TokenSource`, asked for the token before each connection attempt.
- `next_batch(ack, max_items, max_bytes, &cancel)` returns what the server sent, in order, at most `max_items` and
  about `max_bytes` at a time. A batch is returned again until the next call acknowledges its id, so a call cancelled
  after the batch was taken loses nothing. One reader at a time; `disconnect` wakes a waiting reader with the end.
- `send`, `try_send` and `send_signed` take the payload as JSON text; messages come with their payload as JSON text
  (from msgpack too), their topic position, and the exact signed body when signed.
- Errors carry an `ErrorKind` to match on and a message.

Applications use the language packages built on it; this crate is for binding authors.
