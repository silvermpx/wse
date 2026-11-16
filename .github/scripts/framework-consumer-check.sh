#!/usr/bin/env bash
# An application of tests/clients built against the packed wse-client (TGZ),
# installed the way a project installs it, and checked in Node and Chromium
# (tests/clients/harness): angular20, angular21 or angular22, solid1
# (SolidStart on Solid 1.9) or solid2 (Solid 2.0): a production build, server
# rendering for two requests at once, hydration, one connection, ticks,
# German and English. It runs against the Rust client's example server,
# built here.
#
#   .github/scripts/framework-consumer-check.sh npm-dist/wse-client-3.2.0.tgz angular22
set -euo pipefail
tgz="${1:?usage: framework-consumer-check.sh TGZ FIXTURE}"
fixture="${2:?usage: framework-consumer-check.sh TGZ FIXTURE}"
tgz="$(cd "$(dirname "$tgz")" && pwd)/$(basename "$tgz")"
root="$(cd "$(dirname "$0")/../.." && pwd)"
# Cargo's target directory: CARGO_TARGET_DIR (relative to the repository), else target/.
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
export CARGO_TARGET_DIR="$target" NG_CLI_ANALYTICS=false
# The physical path: on macOS a temporary folder is a link into /private.
work="$(cd "$(mktemp -d)" && pwd -P)"
trap 'rm -rf "${work:?}"' EXIT

case "$fixture" in
  angular20 | angular21 | angular22)
    major="${fixture#angular}"
    mkdir -p "$work/src"
    cp -R "$root/tests/clients/angular/app/." "$work/src/"
    for file in package.json package-lock.json angular.json tsconfig.json tsconfig.app.json; do
      cp "$root/tests/clients/angular/$major/$file" "$work/"
    done
    cp "$root/tests/clients/angular/$major/app.config.ts" "$work/src/app/"
    (
      cd "$work"
      npm ci --no-audit --no-fund --loglevel=error
      npm install --no-save --no-audit --no-fund --loglevel=error "$tgz"
      npx ng build
    )
    kind=angular
    ;;
  solid1 | solid2)
    major="${fixture#solid}"
    cp -R "$root/tests/clients/solid/$major/." "$work/"
    for file in Page.tsx text.ts endpoint.ts; do
      cp "$root/tests/clients/solid/app/$file" "$work/src/"
    done
    (
      cd "$work"
      npm ci --no-audit --no-fund --loglevel=error
      npm install --no-save --no-audit --no-fund --loglevel=error "$tgz"
      # The page against the framework's types and ours, every declaration
      # checked (the framework's own dependencies are not ours to check).
      npx tsc -p tsconfig.check.json
      npx vite build
    )
    kind="solid$major"
    ;;
  *)
    echo "no application $fixture: angular20, angular21, angular22, solid1 or solid2" >&2
    exit 2
    ;;
esac

(cd "$root" && cargo build -q -p wse-client --example server)
program="$target/debug/examples/server"
[[ -f "$program.exe" ]] && program="$program.exe"
(
  cd "$root/tests/clients/harness"
  npm ci --no-audit --no-fund --loglevel=error
  npx playwright install chromium > /dev/null
)
node "$root/tests/clients/harness/page.mjs" "$kind" "$work" "$program"
