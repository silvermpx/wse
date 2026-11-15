#!/usr/bin/env bash
# Install a packed wse-client tarball into fresh consumer projects and use it
# the ways applications do. A project without React or zustand imports the
# core and the Svelte entry; one with React, zustand and Vue uses every entry:
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

echo "::group::The core without React or zustand"
mkdir "$work/bare"
cd "$work/bare"
cat > package.json <<'EOF'
{ "name": "wse-client-core-check", "version": "0.0.0", "private": true, "type": "module" }
EOF
npm install --no-audit --no-fund --ignore-scripts "$tarball"
cat > core.js <<'EOF'
import { createWSEClient } from 'wse-client/core';
import { wseState } from 'wse-client/svelte';

const client = createWSEClient({ endpoints: ['ws://localhost:1/wse'] });
let seen = null;
wseState(client).subscribe((state) => { seen = state.connection; })();
if (seen !== 'pending') throw new Error(`wse-client/core: unexpected state ${seen}`);
client.destroy();
console.log('wse-client/core and wse-client/svelte without React or zustand: OK');
EOF
node core.js
echo "::endgroup::"

mkdir "$work/full"
cd "$work/full"

cat > package.json <<'EOF'
{ "name": "wse-client-consumer-check", "version": "0.0.0", "private": true, "type": "module" }
EOF
# react, zustand and vue are wse-client's (optional) peer dependencies;
# install scripts stay off.
npm install --no-audit --no-fund --ignore-scripts \
  "$tarball" react zustand vue "esbuild@${esbuild_version}"

cat > entry.js <<'EOF'
import * as wse from 'wse-client';
import * as core from 'wse-client/core';
import * as react from 'wse-client/react';
import * as vue from 'wse-client/vue';
import * as svelte from 'wse-client/svelte';

for (const [name, entry] of Object.entries({ 'wse-client': wse, core, react, vue, svelte })) {
  const names = Object.keys(entry);
  if (names.length === 0) throw new Error(`${name} exports nothing`);
  console.log(`${name}: ${names.length} exports`);
}
if (typeof react.WSEProvider !== 'function' || typeof vue.wsePlugin !== 'function') {
  throw new Error('an adapter is missing its entry points');
}
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

echo "::group::Types and package layout (attw, publint)"
# ESM only: CommonJS consumers use a dynamic import.
npx --yes @arethetypeswrong/cli@0.18.5 "$tarball" --profile esm-only
npx --yes publint@0.3.25 --strict "$tarball"
echo "::endgroup::"
