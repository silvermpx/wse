# wse_client

The WSE client for Dart and Flutter. It keeps a connection to a WSE server,
reconnects when the connection is lost, recovers the messages it missed, and
hands the server's messages to the app in order. It is the Rust client
(`wse-client` on crates.io) through flutter_rust_bridge.

Platforms: Android (API 24+), iOS 15+, macOS 10.15+, Linux and Windows.
Flutter 3.38 or newer.

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
Linux (x64, arm64) and Windows (x64).

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
queued and closes.

In `flutter test` the package loads the library its build hook made for the
host, so tests that use the client run on the developer's machine too.

## License

MIT
