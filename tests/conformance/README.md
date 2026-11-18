# Conformance

Every WSE client is checked against one server and one judge, so that the clients do the same things
the same way.

- `cases.json`: the cases. Each has a server of its own (`server`: its history, `recovery`; its
  keys, `e2e` and `signing`; `hold`, a type whose first message the application holds until the
  case cuts the connection, so what the client sends meanwhile is unanswered at the cut), steps
  for the client and for the server in one list, and the rules it is judged by.
- `agents.json`: the clients a release needs, and the features each must claim. A case a client's
  features do not reach is not applicable to it; every case they reach must pass.
- `crates/wse-conformance`: the server and the judge. A driver connects to its control channel,
  says who it is, and runs each case it can: it does the client's steps with its client, asks the
  server for the server's steps in their place, and records what its client saw and did (its
  trace). The judge holds the trace and what the server published and took to the case's rules.
  A driver asserts nothing.
- The drivers: `ts-client/__tests__/conformance.test.ts` (TypeScript), `crates/wse-client/tests/conformance.rs`
  (Rust), `python-client/tests/test_conformance.py` (Python). The packages' drivers run on the
  systems apps run on: Swift's (`bindings/swift/Tests`) on the Mac and an iOS simulator, Kotlin's
  (`bindings/kotlin/src/conformance`) on the JVM and an Android emulator, Dart's
  (`bindings/dart/test/conformance`) on the desktop and, in the example app, on an iOS simulator and
  an Android emulator; the TypeScript driver also runs in Electron, Tauri and a React Native app on
  an Android emulator (`.github/scripts/rn-app-check.sh`). Each of these runs has a manifest here of
  its own (`swift-ios.json`, `kotlin-android.json`, `dart-ios.json`, `dart-android.json`,
  `react-native.json`, ...).

```bash
cargo run -p wse-conformance -- serve --out target/conformance   # prints {"ready":true,"control":"ws://..."}
WSE_CONFORMANCE=ws://127.0.0.1:P/control npx vitest run --project client ts-client/__tests__/conformance.test.ts
WSE_CONFORMANCE=ws://127.0.0.1:P/control cargo test -p wse-client --test conformance
WSE_CONFORMANCE=ws://127.0.0.1:P/control pytest python-client/tests/test_conformance.py
cargo run -p wse-conformance -- check --manifest tests/conformance/agents.json --reports target/conformance
```

`cargo xtask check conformance` does all of it. A driver runs the cases one at a time;
`WSE_CONFORMANCE_CASES=a,b` runs those alone, and `WSE_CONFORMANCE_TRACE=1` prints each trace.

## The control channel

A WebSocket of JSON requests, `{id, op, ...}`, each answered `{id, ok, ...}`:

- `hello {agent, version, runtime, os, arch, features}`: the cases the driver can run;
- `begin {case}`: the case's server (`url`, by the host the driver reached the control channel by:
  `10.0.2.2` from an Android emulator), its users' `tokens`, the server's `identity_key` and
  `signing_key`, and the `steps`;
- `act {step}`: a server step: `publish {topic, count}` (answers the numbers published, `p.n`),
  `await_subscribers {topic, count}`, `kick`, `stop`, `restart` (a new server on the same port:
  a new history and epoch), `sleep {ms}`;
- `trace {events}`: what the client saw, in pieces;
- `end`: the verdict, and the case's server goes;
- `bye`: the report, `<agent>.json` and `<agent>.junit.xml` (also written when the channel
  closes): what the driver said it is, the commit the server was started in (`source`, git's
  `HEAD`) and the server's version, and each case's verdict. `check` fails a report that names
  no version, runtime, system or architecture, one whose runtime or system is not the one its
  manifest names for the agent (`runtime`, `os`: words one of which it must have, in any case),
  and one from another commit than `--source COMMIT`, or than git's `HEAD` where it runs (in a
  copy of the tree with no git, `WSE_SOURCE` names the commit, for the server and the check).

## The trace

In the order the client saw things, each with `t`, the milliseconds since the case began:
`ready`; `message {type, topic, n, payload}`; `break {topic, kind}` (a gap, a continuity broken);
`resync {topics}` and `resynced {topics}`; `sent {id, type, live, ok}`; `delivered {ids}` and
`not_delivered {reason, ids}`; `step {server}`; `timeout {step, waiting}` and `error {message}`
when the driver could not go on.

## The rules

`connections`, `echoed`, `order`, `no_duplicates`, `all_published_delivered`, `no_silent_loss`,
`break_said`, `no_break`, `taken_once`, `taken_in_order`, `delivered_all`, `never_taken`,
`signed_taken`, `held_until_resynced`, `back_within` (the client is back within a time after
the server cut or restarted), `unanswered_at_cut` (the server cut the connection while a send was
not answered yet, and the client came back after it), and `taken_msgpack` and `taken_sealed` (the
messages of a type came in MessagePack, or in an end-to-end session, as the server took them,
whatever the driver says). In every case a timeout or a driver that could not go on fails, and
so does an outcome (`delivered`, `not_delivered`) of a message the application never sent.
