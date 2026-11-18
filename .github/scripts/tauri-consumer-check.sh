#!/usr/bin/env bash
# The Tauri application of tests/clients/tauri, built against the packed
# wse-client (TGZ) and the crates as crates.io will give them (CRATES: a
# folder of the .crate files of wse, wse-client and tauri-plugin-wse, as
# `cargo package` writes them), outside the checkout: its Rust side starts
# the plugin's connection, its two windows render with wse-client/react's
# hooks over wse-client/tauri. It runs against the Rust client's example
# server, built here (on Linux in a virtual display), and checks two windows
# on one connection, then one closed and the other going on.
#
#   .github/scripts/tauri-consumer-check.sh npm-dist/wse-client-3.2.0.tgz crates/
set -euo pipefail
tgz="${1:?usage: tauri-consumer-check.sh TGZ CRATES}"
crates="${2:?usage: tauri-consumer-check.sh TGZ CRATES}"
tgz="$(cd "$(dirname "$tgz")" && pwd)/$(basename "$tgz")"
crates="$(cd "$crates" && pwd)"
root="$(cd "$(dirname "$0")/../.." && pwd)"
# Cargo's target directory: CARGO_TARGET_DIR (relative to the repository), else target/.
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
export CARGO_TARGET_DIR="$target"
# The physical path: on macOS a temporary folder is a link into /private.
work="$(cd "$(mktemp -d)" && pwd -P)"
server=
trap 'if [ -n "$server" ]; then kill "$server" 2> /dev/null || true; fi; rm -rf "${work:?}"' EXIT

for file in package.json package-lock.json tsconfig.json vite.config.mjs index.html page.ts; do
  cp "$root/tests/clients/tauri/$file" "$work/"
done
cp -R "$root/tests/clients/tauri/src-tauri" "$work/"
# Each crate unpacked, and the application patched to it, as crates.io's
# package of it would be: nothing of the checkout's sources.
mkdir -p "$work/crates"
for crate in "$crates"/*.crate; do
  tar -xzf "$crate" -C "$work/crates"
done
{
  echo
  echo '[patch.crates-io]'
  for dir in "$work"/crates/*/; do
    dir="${dir%/}"
    name="$(basename "$dir" | sed -E 's/-[0-9]+\.[0-9]+\.[0-9]+.*$//')"
    # Cargo on Windows reads C:/... and not /c/...: a literal TOML string.
    if command -v cygpath > /dev/null; then dir="$(cygpath -m "$dir")"; fi
    echo "$name = { path = '$dir' }"
  done
} >> "$work/src-tauri/Cargo.toml"
# The application asks for exactly the plugin unpacked: a release in the
# registry that also matches its range is never taken in its place.
plugin_dirs=("$work"/crates/tauri-plugin-wse-*/)
plugin="$(basename "${plugin_dirs[0]%/}")"
version="${plugin#tauri-plugin-wse-}"
sed -i.bak -E "s/^tauri-plugin-wse = \"[^\"]*\"/tauri-plugin-wse = \"=$version\"/" "$work/src-tauri/Cargo.toml"
rm "$work/src-tauri/Cargo.toml.bak"
grep -q "^tauri-plugin-wse = \"=$version\"" "$work/src-tauri/Cargo.toml" || {
  echo "the application's tauri-plugin-wse is not pinned to $version" >&2
  exit 1
}
(
  cd "$work"
  npm ci --no-audit --no-fund --loglevel=error
  npm install --no-save --no-audit --no-fund --loglevel=error "$tgz"
  npm run build --silent
)
# Its own target folder: an application's build, not the workspace's.
CARGO_TARGET_DIR="$target/tauri-check" cargo build --manifest-path "$work/src-tauri/Cargo.toml"
# Built with the packages unpacked here: none of them came from a registry.
for dir in "$work"/crates/*/; do
  name="$(basename "${dir%/}" | sed -E 's/-[0-9]+\.[0-9]+\.[0-9]+.*$//')"
  if awk -v name="$name" '
    $0 == "name = \"" name "\"" { inside = 1; next }
    /^\[\[package\]\]/ { inside = 0 }
    inside && /^source = / { found = 1 }
    END { exit !found }
  ' "$work/src-tauri/Cargo.lock"; then
    echo "the application took $name from a registry, not the package unpacked" >&2
    exit 1
  fi
done
program="$target/tauri-check/debug/wse-tauri-check"
[[ -f "$program.exe" ]] && program="$program.exe"

(cd "$root" && cargo build -q -p wse-client --example server)
example="$target/debug/examples/server"
[[ -f "$example.exe" ]] && example="$example.exe"
port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"
WSE_PORT="$port" "$example" > "$work/server.log" 2>&1 &
server=$!
for _ in $(seq 1 100); do grep -q '^signing_public_key' "$work/server.log" && break; sleep 0.1; done
display=
if [ "$(uname)" = Linux ] && [ -z "${DISPLAY:-}" ]; then display='xvfb-run -a'; fi
WSE_URL="ws://127.0.0.1:$port/wse" $display "$program"
