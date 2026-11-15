#!/usr/bin/env bash
# Builds the native library and the bindings, starts the Rust client's
# example server and runs the Kotlin tests on the JVM. GRADLE names the
# gradle to run (default: gradle on PATH).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
"$here/build.sh"
port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"
cd "$root"
cargo build -q -p wse-client --example server
WSE_PORT="$port" "$root/target/debug/examples/server" > "$root/target/kotlin-test-server.log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  grep -q "ws://" "$root/target/kotlin-test-server.log" && break
  sleep 0.1
done
export WSE_TEST_URL="ws://127.0.0.1:$port/wse"
cd "$here"
"${GRADLE:-gradle}" --no-daemon -q test
