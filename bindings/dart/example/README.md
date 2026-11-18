# wse_client example

A Flutter app on the WSE client, for Android, iOS, macOS, Windows and Linux:
it connects, shows the connection's status and the last tick of the `ticks`
topic, in German or English; every word it shows has both, the menu bar of
macOS included.

Run it against the Rust client's example server:

```sh
cargo run -p wse-client --example server          # prints ws://127.0.0.1:5007/wse
flutter run --dart-define=WSE_URL=ws://127.0.0.1:5007/wse
flutter run -d macos --dart-define=WSE_URL=ws://127.0.0.1:5007/wse   # or windows, linux
```

An Android emulator reaches the server through `adb reverse tcp:5007 tcp:5007`.
`run-integration-tests.sh <device id>` starts the server and runs the
integration test on a device: an iOS simulator's id, an Android emulator's
(`emulator-5554`), or this desktop's (`macos`, `windows`, `linux`; on a Linux
server under `xvfb-run`). With `MODE=release` it builds `lib/check.dart` in
release mode instead and, on a desktop, runs the built app from a folder of
its own. On an iOS simulator or an Android emulator the integration test also
runs the conformance cases against a conformance server of its own, and
checks them (agents `dart-ios` and `dart-android`).
