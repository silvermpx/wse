# wse_client

The WSE client for Dart and Flutter. It keeps a connection to a WSE server,
reconnects when the connection is lost, recovers the messages it missed, and
hands the server's messages to the app in order. It is the Rust client
(`wse-client` on crates.io) through flutter_rust_bridge.

Platforms: Android (API 24+), iOS 15+, macOS 10.15+, Linux and Windows.
Flutter 3.38 or newer. These are the library's minimums; the app's Flutter
may ask for more (Flutter 3.50 makes macOS apps for macOS 12 and newer).

## Install

```yaml
dependencies:
  wse_client: ^3.1.0
```

The package's build hook takes the prebuilt Rust library of the app's target
from the package's GitHub release and uses it only when its SHA-256 is the one
the release listed, so the app needs no Rust toolchain. Prebuilt libraries
exist for Android (arm64-v8a, armeabi-v7a, x86_64), iOS (devices, and
simulators on Apple silicon and Intel), macOS (Apple silicon and Intel),
Linux (x64, arm64) and Windows (x64, arm64).

To build the library from source instead (a Rust toolchain through rustup,
plus Xcode for Apple targets or the Android NDK for Android), add to the
app's `pubspec.yaml`:

```yaml
hooks:
  user_defines:
    wse_client:
      build: source
```

An iOS app sets its minimum iOS version to 15.0. An Android app has the
`INTERNET` permission in its main manifest, and a macOS app the
`com.apple.security.network.client` entitlement.

## Use

```dart
import 'package:wse_client/wse_client.dart';

Future<void> main() async {
  await WseClient.init();
  final client = await WseClient.connect(
    wseConfig('wss://example.com/wse', token: token, topics: ['prices']),
  );
  await client.send('order', '{"id": 1}');
  client.trySendLive('open_valve', '{"id": 7}'); // on this connection or not at all
  await for (final item in client.events()) {
    switch (item.kind) {
      case WseItemKind.message:
        print('${item.topic} ${item.name}: ${item.json}');
      case WseItemKind.gap:
        print('lost messages on ${item.topic}: reload it');
      default:
        break;
    }
  }
}
```

`events()` is the client's stream: messages, connections and disconnections,
subscriptions, gaps it could not recover, server errors, and the end when the
client closes. One listener reads it at a time; a listener that stops in
between loses nothing, the next one gets the rest. `close()` sends what is
queued and closes; a closed client's calls throw a `WseError` of kind
`closed`.

## The outbox

Every message sent, except a live or signed one, waits in the outbox until
the server answers for it, goes again after a reconnect, and reaches the
server's application once. The `delivered` item names the messages the
server took (`ids`); `notDelivered` names those it will not take and why
(`reason`): the server refused it (`busy`, `rate_limited`, `missing`,
`not_ready`), it was not written within its time (`expired`), it was the
last user's (`user_changed`), the app cleared the outbox (`cleared`), or
the client closed with it in memory (`closed`).

```dart
final id = await client.sendWith('order', '{"item":"tea"}', id: 'order-17');
// ... later, on sign-out:
client.clear();
```

`sendWith` also names a request's `correlationId`, which the reply names
back (the message item's `correlationId`), and its `priority` (10 critical,
8 high, 5 normal, 3 low, 1 background), for the server's app; `live: true`
sends on the connection that is ready now, or not at all.

The outbox is in memory by default. With `offline: wseOffline(path: file)`
(a file in the app's own storage) it outlives the app. At most
`maxMessages` (100,000) and `maxBytes` (16 MiB) wait, the ones not answered
yet included: past either `send` waits for room and `trySend` throws
`full`. One not written within `maxAge` (an hour), or the `ttl` it was sent
with, is dropped. `client.kept` says how many wait. A file that cannot be
opened or written is a `storageFailed` item (`name`: what failed,
`reason`: why), and the outbox goes on in memory. `wseOffline(disabled:
true)` has no outbox: a send with no connection fails, as before 3.2.

`WseClient.connect` completes once the server took the connection, and
fails when the first attempt does. For an app that may start without a
network, `WseClient.start` completes at once: the connection is made in the
background, a failed first attempt made again as any later one, and what is
sent meanwhile waits in the outbox.

## Topic states and recover-or-resync

`client.topicStates` says of each topic whether the server took it and
whether its messages run without a break. Where every fact counts, a
recover-or-resync delivery holds a topic that is not continuous -- from a
cold start, after every reconnect, after a break -- until the app brought
its state up to date:

```dart
final client = await WseClient.connect(
  wseConfig(url, topics: ['orders'], delivery: wseDelivery('board', checkpoint: saved)),
);
await for (final item in client.events()) {
  switch (item.kind) {
    case WseItemKind.resync:
      await loadState(item.topics, item.committed);
      client.resynced(item.round!); // what was held comes next
    case WseItemKind.message:
      apply(item); // safe to apply twice
      client.commit(item);
      save(client.checkpoint());
    default:
      break;
  }
}
```

The checkpoint is bound to the realm, the user, the consumer and the schema;
one of anything else is refused with a `checkpointRejected` item. Recovery
starts from it, so a message that came but was not applied comes again.
`await client.sendRequiring('cancel', json, ['orders'])` sends once each of
the topics is continuous on the connection, checked right before it goes
(after a reconnect it waits for the resync); sends that require nothing go
past it, and once the server takes a connection for another user it goes
nowhere.

## Certificates

A wss:// server's certificate is checked by the system's verifier (the
default, `WseTrust.platform`): the roots the device trusts. On Android the
package's Android part sets it up as the app starts. `WseTrust.roots([ca])`
trusts only a private CA's certificate (PEM or DER), `WseTrust.mozilla` the
Mozilla roots built into the library:

```dart
final ca = await rootBundle.load('assets/plant-ca.pem');
final client = await WseClient.connect(
  wseConfig(url, trust: WseTrust.roots([ca.buffer.asUint8List()])),
);
```

ws:// sends everything in the clear and needs `insecure: true` (for
development).

## Background and network

`WseAppLifecycle(client)` pauses the client five seconds after the app went
to the background and resumes it when the app comes back. On a desktop a
minimized window keeps running and keeps receiving: there it pauses the
client only with `pauseMinimized: true`, and a window that only lost focus
never pauses it.
`WseNetworkMonitor(client)` pauses it while the device has no network and
makes the connection again when the network moves to other interfaces (Wi-Fi
to mobile, say); the app feeds it what it sees, with connectivity_plus for
one:

```dart
final lifecycle = WseAppLifecycle(client);
final network = WseNetworkMonitor(client);
Connectivity().onConnectivityChanged.listen(
  (results) => network.update(
    usable: !results.contains(ConnectivityResult.none),
    interfaces: [for (final result in results) result.name],
  ),
);
```

Both pause for reasons of their own: `client.pause('manual')` stays until
the app calls `client.resume('manual')`. A paused client keeps its topics and
positions; once no reason is left it connects again and recovers what it
missed. Dispose of both with the client.

In `flutter test` the package loads the library its build hook made for the
host, so tests that use the client run on the developer's machine too.

## Desktop apps

On Windows, macOS and Linux the package works as it does on a phone, and the
release build takes the same prebuilt library: the app runs from any folder
and needs no Rust toolchain. The example runs on all three
(`flutter run -d macos`, `windows` or `linux`). A macOS app runs in the App
Sandbox, which lets it open connections only with this entitlement in
`macos/Runner/DebugProfile.entitlements` and
`macos/Runner/Release.entitlements`:

```xml
<key>com.apple.security.network.client</key>
<true/>
```

A Linux app builds with GTK 3, as every Flutter app on Linux. A minimized
window keeps receiving (`WseAppLifecycle`, above).

## License

MIT
