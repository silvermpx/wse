#!/usr/bin/env bash
# Install a packed wse-client tarball into a fresh consumer project and use it
# the two ways applications do:
#   1. bundle it with esbuild (what Vite and most React toolchains do), which
#      fails on an import a bundler cannot resolve or an export that does not
#      exist -- the class that broke every bundler consumer of 2.4.2 while tsc
#      and the unit tests passed;
#   2. import it in plain Node as an ES module (SSR, tests, scripts), which
#      fails on extensionless relative specifiers or a missing "type".
#
# usage: npm-consumer-check.sh path/to/wse-client-X.Y.Z.tgz
set -euo pipefail

if [[ $# -ne 1 || ! -f "$1" ]]; then
  echo "usage: $0 path/to/wse-client-X.Y.Z.tgz" >&2
  exit 2
fi
tarball="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
esbuild_version="0.28.2"

work="$(mktemp -d)"
trap 'rm -rf "${work:?}"' EXIT
cd "$work"

cat > package.json <<'EOF'
{ "name": "wse-client-consumer-check", "version": "0.0.0", "private": true, "type": "module" }
EOF
# react and zustand are wse-client's peer dependencies; install scripts stay off.
npm install --no-audit --no-fund --ignore-scripts \
  "$tarball" react zustand "esbuild@${esbuild_version}"

cat > entry.js <<'EOF'
import * as wse from 'wse-client';

const names = Object.keys(wse);
if (names.length === 0) throw new Error('wse-client exports nothing');
console.log(`wse-client: ${names.length} exports`);
EOF

cat > bundle.mjs <<'EOF'
import { build } from 'esbuild';

const result = await build({
  entryPoints: ['entry.js'],
  bundle: true,
  format: 'esm',
  platform: 'browser',
  outfile: 'out/bundle.js',
  logLevel: 'warning',
});
if (result.errors.length || result.warnings.length) {
  console.error(`esbuild: ${result.errors.length} error(s), ${result.warnings.length} warning(s)`);
  process.exit(1);
}
console.log('esbuild bundle OK');
EOF

echo "::group::Bundle with esbuild ${esbuild_version}"
node bundle.mjs
echo "::endgroup::"

echo "::group::Import in Node as an ES module"
node entry.js
echo "::endgroup::"
