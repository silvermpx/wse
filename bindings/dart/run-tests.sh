#!/usr/bin/env bash
# Generates the bindings (flutter_rust_bridge_codegen 2.13.0), starts the
# Rust client's example server and runs the Dart tests on this machine; the
# build hook builds the Rust library. FLUTTER names the flutter to run.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
flutter="${FLUTTER:-flutter}"
export DART_SUPPRESS_ANALYTICS=true FLUTTER_SUPPRESS_ANALYTICS=true
cd "$here"
"$flutter" pub get --suppress-analytics > /dev/null
CARGO_TARGET_DIR="$root/target/frb-expand" flutter_rust_bridge_codegen generate
port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"
cd "$root"
cargo build -q -p wse-client --example server
WSE_PORT="$port" "$root/target/debug/examples/server" > "$root/target/dart-test-server.log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  grep -q "ws://" "$root/target/dart-test-server.log" && break
  sleep 0.1
done
export WSE_TEST_URL="ws://127.0.0.1:$port/wse"
# On this machine the tests load the library built for it.
cargo build -q -p wse-dart
export FRB_DART_LOAD_EXTERNAL_LIBRARY_NATIVE_LIB_DIR="$root/target/debug/"
cd "$here"
"$flutter" test --suppress-analytics
