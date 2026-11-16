# wse_client example

A Flutter app on the WSE client, for iOS and Android: it connects, shows the
connection's status and the last tick of the `ticks` topic, in German or
English; every word it shows has both.

Run it against the Rust client's example server:

```sh
cargo run -p wse-client --example server          # prints ws://127.0.0.1:5007/wse
flutter run --dart-define=WSE_URL=ws://127.0.0.1:5007/wse
```

An Android emulator reaches the server through `adb reverse tcp:5007 tcp:5007`.
`run-integration-tests.sh <device id>` starts the server and runs the
integration test on a device: an iOS simulator's id, or an Android emulator's
(`emulator-5554`).
