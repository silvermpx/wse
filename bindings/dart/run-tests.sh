#!/usr/bin/env bash
# Generates the bindings (flutter_rust_bridge_codegen 2.13.0), starts the
# Rust client's example server and runs the Dart tests on this machine; the
# build hook builds the Rust library. FLUTTER names the flutter to run.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
# Cargo's target directory: CARGO_TARGET_DIR (relative to the repository), else target/.
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
export CARGO_TARGET_DIR="$target"
flutter="${FLUTTER:-flutter}"
# The codegen runs the flutter on PATH: make it this one.
if [[ "$flutter" == */* ]]; then
  PATH="$(cd "$(dirname "$flutter")" && pwd):$PATH"
fi
export DART_SUPPRESS_ANALYTICS=true FLUTTER_SUPPRESS_ANALYTICS=true
cd "$here"
"$flutter" pub get --suppress-analytics > /dev/null
CARGO_TARGET_DIR="$target/frb-expand" flutter_rust_bridge_codegen generate
# In the workspace's own style, as cargo fmt --check wants it.
(cd "$root" && cargo fmt -p wse-dart)
# Clean as pub.dev scores it: no analysis issues, the formatter's own layout.
dart="$(dirname "$(command -v "$flutter")")/dart"
"$flutter" analyze --suppress-analytics lib test hook
"$dart" format --output=none --set-exit-if-changed lib test hook example/lib example/integration_test
port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"
cd "$root"
cargo build -q -p wse-client --example server
WSE_PORT="$port" "$target/debug/examples/server" > "$target/dart-test-server.log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  grep -q "ws://" "$target/dart-test-server.log" && break
  sleep 0.1
done
export WSE_TEST_URL="ws://127.0.0.1:$port/wse"
cd "$here"
"$flutter" test --suppress-analytics
