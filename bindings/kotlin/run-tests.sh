#!/usr/bin/env bash
# Builds the native library and the bindings, starts the Rust client's
# example server and the conformance server, runs the Kotlin tests on the
# JVM (the conformance cases among them), then checks the cases' report.
# GRADLE names the gradle to run (default: gradle on PATH).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
# Cargo's target directory: CARGO_TARGET_DIR (relative to the repository), else target/.
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
export CARGO_TARGET_DIR="$target"
"$here/build.sh"
port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"
cd "$root"
cargo build -q -p wse-client --example server
# Its wss:// door on the next port, with a CA of its own made for the run.
WSE_PORT="$port" WSE_TLS_PORT="$((port + 1))" WSE_TLS_CA="$target/kotlin-test-ca.pem" \
  "$target/debug/examples/server" > "$target/kotlin-test-server.log" 2>&1 &
server=$!
# The conformance server, each case on a server of its own.
cargo build -q -p wse-conformance
conformance="$(mktemp -d "$target/kotlin-conformance.XXXXXX")"
"$target/debug/wse-conformance" serve --out "$conformance" > "$conformance/serve.log" 2>&1 &
judge=$!
trap 'kill "$server" "$judge" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  grep -q "signing_public_key" "$target/kotlin-test-server.log" && break
  sleep 0.1
done
for _ in $(seq 1 100); do
  grep -q '"ready"' "$conformance/serve.log" && break
  sleep 0.1
done
WSE_CONFORMANCE="$(sed -n 's/.*"control":"\([^"]*\)".*/\1/p' "$conformance/serve.log")"
export WSE_CONFORMANCE WSE_CONFORMANCE_REQUIRED=1
export WSE_TEST_URL="ws://127.0.0.1:$port/wse" WSE_NATIVE_LIB_DIR="$target/debug"
export WSE_TEST_TLS_URL="wss://localhost:$((port + 1))/wse" WSE_TEST_CA="$target/kotlin-test-ca.pem"
# The keys its E2E identity and its signed messages go with.
WSE_TEST_IDENTITY="$(sed -n 's/^e2e_identity_public_key //p' "$target/kotlin-test-server.log")"
WSE_TEST_SIGNING="$(sed -n 's/^signing_public_key //p' "$target/kotlin-test-server.log")"
export WSE_TEST_IDENTITY WSE_TEST_SIGNING
cd "$here"
"${GRADLE:-gradle}" --no-daemon -q test
"$target/debug/wse-conformance" check --manifest "$root/tests/conformance/kotlin.json" --reports "$conformance"
