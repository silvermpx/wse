#!/usr/bin/env bash
# Builds the package's native part, starts the Rust client's example server
# and runs the tests: on the Mac (swift test), and with SIMULATOR=1 also on
# an iOS simulator.
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
WSE_PORT="$port" "$target/debug/examples/server" > "$target/swift-test-server.log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  grep -q "ws://" "$target/swift-test-server.log" && break
  sleep 0.1
done
export WSE_TEST_URL="ws://127.0.0.1:$port/wse"
cd "$here"
swift test
if [[ "${SIMULATOR:-0}" == "1" ]]; then
  device="$(xcrun simctl list devices available | grep -m1 -E 'iPhone' | sed -E 's/.*\(([0-9A-F-]{36})\).*/\1/')"
  # xcodebuild hands TEST_RUNNER_ variables to the tests, without the prefix.
  TEST_RUNNER_WSE_TEST_URL="$WSE_TEST_URL" \
    xcodebuild test -scheme WseClient -destination "id=$device"
fi
