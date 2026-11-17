# WSE client for Swift

The WSE client for iOS 15+ and macOS 12+: the Rust client (`wse-client` on
crates.io) through UniFFI, with an async API. It keeps a connection to a WSE
server, reconnects when the connection is lost, recovers the messages it
missed, and hands the server's messages to the app in order.

## Install

The Swift package lives in its own repository, which each release writes:

```swift
.package(url: "https://github.com/silvermpx/wse-swift", from: "3.1.0")
```

with the product `WseFFI` in the target's dependencies. It downloads the
release's `WseFFIC-<version>.xcframework.zip`, checked against its checksum.

## Build

```sh
bindings/swift/build.sh          # WseFFIC.xcframework and Sources/WseFFI/WseFFI.swift
```

The XCFramework holds the Rust library for macOS (Apple silicon and Intel),
iOS devices, and the iOS simulator (Apple silicon and Intel). It needs Rust
with the targets `aarch64-apple-darwin x86_64-apple-darwin aarch64-apple-ios
aarch64-apple-ios-sim x86_64-apple-ios`, and Xcode. Each release attaches
the built `WseFFIC-<version>.xcframework.zip` and its SwiftPM checksum.

A checkout's package is added to an app by its path (`bindings/swift`),
product `WseFFI`.

## Use

```swift
import WseFFI

let client = try await WseClient.connect(
    WseConfig(url: "wss://example.com/wse", token: token, topics: ["prices"]))
try await client.send(kind: "order", json: #"{"id":1}"#)
try client.trySendLive(kind: "open_valve", json: #"{"id":7}"#)  // on this connection or not at all
for try await item in client.events() {
    switch item {
    case let .message(message):
        print(message.topic ?? "", message.kind, message.json ?? "")
    case let .gap(topic, _, _, _, _):
        print("lost messages on \(topic): reload it")
    default:
        break
    }
}
```

On iOS, `WseAppLifecycle(client: client)` pauses the client five seconds after
the app went to the background and resumes it when it comes back (the whole
app: one scene closing does not pause it), and `WseNetworkMonitor(client:
client)` pauses it while there is no network path and makes the connection
again when the path moves to other interfaces (macOS too). Both pause for
reasons of their own: `try client.pause(reason: "manual")` stays until the
app resumes it. Keep them as long as the client.

A wss:// server's certificate is checked by the system's verifier (the
default, `trust: nil`): the roots the device trusts. `trust:
.roots(certificates: [ca])` trusts only a private CA's certificate (PEM or
DER), `.mozilla` the Mozilla roots built into the library. ws:// sends
everything in the clear and needs `insecure: true` (for development).

`client.topicStates()` says of each topic whether the server took it and
whether its messages run without a break. Where every fact counts,
`delivery: WseDelivery(consumer: "board")` (recover-or-resync) holds a topic
that is not continuous -- from a cold start, after every reconnect, after a
break -- until the app brought its state up to date: a `.resync(round:
topics: reasons: committed:)` item asks for it, and `client.resynced(round:)`
hands on what was held. After applying a message the app calls
`client.commit(message)`; `client.checkpoint()` is what to save with its own
state and give back as `WseDelivery(checkpoint:)`, and recovery starts from
it, so a message can come twice: apply a repeat safely. The user changes
where the events show the server took the connection for another user: what
came before is the last user's, and a message of theirs committed later
counts for nobody. `client.sendRequiring(kind:json:topics:)` sends once each
of the topics is continuous on the connection, checked right before it goes
(after a reconnect it waits for the resync); sends that require nothing go
past it, and once the server takes a connection for another user it goes
nowhere.

Every message sent, except a live or signed one, waits in the outbox until
the server answers for it, goes again after a reconnect, and reaches the
server's application once. `.delivered(ids:)` names the messages the server
took; `.notDelivered(reason:ids:)` those it will not take and why: the server
refused it (`busy`, `rate_limited`, `missing`, `not_ready`), it was not
written within its time (`expired`), it was the last user's
(`user_changed`), the app cleared the outbox (`cleared`), or the client
closed with it in memory (`closed`).
`client.sendWith(kind:json:options:)` sends with the app's own id
(`WseSendOptions(id: "order-17")`) and returns it; `client.clear()` empties
the outbox on sign-out.

The outbox is in memory by default. With `offline: WseOffline(path: path)`
(a file in the app's own storage) it outlives the app. At most
`maxMessages` (100,000) and `maxBytes` (16 MiB) wait, the ones not answered
yet included: past either `send` waits for room and `trySend` throws
`.Full`. One not written within `maxAgeMs` (an hour), or its `ttlMs`, is
dropped. `client.kept()` says how many wait. A file that cannot be opened
or written is a `.storageFailed` item, and the outbox goes on in memory.
`WseOffline(disabled: true)` has no outbox: a send with no connection
fails, as before 3.2.

`WseClient.connect` returns once the server took the connection, and fails
when the first attempt does. For an app that may start without a network,
`WseClient.start` returns at once: the connection is made in the
background, a failed first attempt made again as any later one, and what
is sent meanwhile waits in the outbox.

`events()` is the client's stream, until it closes; one reader at a time.
A Task cancelled while reading loses nothing: the next read gets the rest;
a loop left and a new one go on from the same place.
Cancelling the Task of `connect` gives up the attempt. `disconnect` sends
what is queued and closes.

## Tests

```sh
bindings/swift/run-tests.sh               # on the Mac
SIMULATOR=1 bindings/swift/run-tests.sh   # and on an iOS simulator
```

They run against the Rust client's example server, which the script starts.
