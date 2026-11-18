#!/usr/bin/env bash
# The Electron application of tests/clients/electron, built against the
# packed wse-client (TGZ) installed the way an application installs it,
# outside the checkout: its main process takes wse-client/electron and
# wse-client/node from node_modules, its two windows wse-client/react's
# hooks and a preload each: the package's, and the application's own that
# imports it, bundled as an application bundles it. It runs against the Rust client's example
# server, built here (on Linux in a virtual display), and checks two windows
# on one connection, then one closed and the other going on.
#
#   .github/scripts/electron-consumer-check.sh npm-dist/wse-client-3.2.0.tgz
set -euo pipefail
tgz="${1:?usage: electron-consumer-check.sh TGZ}"
tgz="$(cd "$(dirname "$tgz")" && pwd)/$(basename "$tgz")"
root="$(cd "$(dirname "$0")/../.." && pwd)"
# Cargo's target directory: CARGO_TARGET_DIR (relative to the repository), else target/.
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
export CARGO_TARGET_DIR="$target"
# The physical path: on macOS a temporary folder is a link into /private.
work="$(cd "$(mktemp -d)" && pwd -P)"
server=
trap 'if [ -n "$server" ]; then kill "$server" 2> /dev/null || true; fi; rm -rf "${work:?}"' EXIT

for file in package.json package-lock.json tsconfig.json vite.config.mjs vite.preload.config.mjs index.html page.ts preload.ts main.mjs; do
  cp "$root/tests/clients/electron/$file" "$work/"
done
(
  cd "$work"
  npm ci --no-audit --no-fund --loglevel=error
  npm install --no-save --no-audit --no-fund --loglevel=error "$tgz"
  npx install-electron > /dev/null
  npm run build --silent
)

(cd "$root" && cargo build -q -p wse-client --example server)
program="$target/debug/examples/server"
[[ -f "$program.exe" ]] && program="$program.exe"
port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"
WSE_PORT="$port" "$program" > "$work/server.log" 2>&1 &
server=$!
for _ in $(seq 1 100); do grep -q '^signing_public_key' "$work/server.log" && break; sleep 0.1; done
display=
if [ "$(uname)" = Linux ] && [ -z "${DISPLAY:-}" ]; then display='xvfb-run -a'; fi
WSE_URL="ws://127.0.0.1:$port/wse" $display "$work/node_modules/.bin/electron" "$work"
