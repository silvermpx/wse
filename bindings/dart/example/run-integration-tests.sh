#!/usr/bin/env bash
# Runs the example app's integration test on DEVICE (a flutter device id:
# macos, an iOS simulator's id, an Android emulator's) against the Rust
# client's example server; the package's build hook builds the Rust library
# for the device. FLUTTER names the flutter to run.
set -euo pipefail
device="${1:?usage: run-integration-tests.sh <device id>}"
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
flutter="${FLUTTER:-flutter}"
export DART_SUPPRESS_ANALYTICS=true FLUTTER_SUPPRESS_ANALYTICS=true
port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"
cd "$root"
cargo build -q -p wse-client --example server
log="$root/target/flutter-example-server.log"
WSE_PORT="$port" "$root/target/debug/examples/server" > "$log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  grep -q "ws://" "$log" && break
  sleep 0.1
done
# An Android emulator reaches the host's port through adb.
if [[ "$device" == emulator-* ]]; then
  adb -s "$device" reverse "tcp:$port" "tcp:$port"
fi
cd "$here"
"$flutter" pub get --suppress-analytics > /dev/null
"$flutter" test integration_test -d "$device" --dart-define="WSE_URL=ws://127.0.0.1:$port/wse" --suppress-analytics
