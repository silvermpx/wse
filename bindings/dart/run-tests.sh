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
# Its wss:// door on the next port, with a CA of its own made for the run.
WSE_PORT="$port" WSE_TLS_PORT="$((port + 1))" WSE_TLS_CA="$target/dart-test-ca.pem" \
  "$target/debug/examples/server" > "$target/dart-test-server.log" 2>&1 &
server=$!
# The conformance server, each case on a server of its own.
cargo build -q -p wse-conformance
conformance="$(mktemp -d "$target/dart-conformance.XXXXXX")"
"$target/debug/wse-conformance" serve --out "$conformance" > "$conformance/serve.log" 2>&1 &
judge=$!
trap 'kill "$server" "$judge" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  grep -q "signing_public_key" "$target/dart-test-server.log" && break
  sleep 0.1
done
for _ in $(seq 1 100); do
  grep -q '"ready"' "$conformance/serve.log" && break
  sleep 0.1
done
WSE_CONFORMANCE="$(sed -n 's/.*"control":"\([^"]*\)".*/\1/p' "$conformance/serve.log")"
export WSE_CONFORMANCE WSE_CONFORMANCE_REQUIRED=1
export WSE_TEST_URL="ws://127.0.0.1:$port/wse" WSE_TEST_TLS_URL="wss://localhost:$((port + 1))/wse"
export WSE_TEST_CA="$target/dart-test-ca.pem"
# The keys its E2E identity and its signed messages go with.
WSE_TEST_IDENTITY="$(sed -n 's/^e2e_identity_public_key //p' "$target/dart-test-server.log")"
WSE_TEST_SIGNING="$(sed -n 's/^signing_public_key //p' "$target/dart-test-server.log")"
export WSE_TEST_IDENTITY WSE_TEST_SIGNING
cd "$here"
"$flutter" test --suppress-analytics
"$target/debug/wse-conformance" check --manifest "$root/tests/conformance/dart.json" --reports "$conformance"
