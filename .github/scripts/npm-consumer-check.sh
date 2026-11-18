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
import * as reactNative from 'wse-client/react-native';

for (const [name, entry] of Object.entries({ 'wse-client': wse, core, react, vue, svelte, 'react-native': reactNative })) {
  const names = Object.keys(entry);
  if (names.length === 0) throw new Error(`${name} exports nothing`);
  console.log(`${name}: ${names.length} exports`);
}
if (typeof react.WSEProvider !== 'function' || typeof vue.wsePlugin !== 'function' || typeof reactNative.createReactNativeClient !== 'function') {
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

# The React Native entry bundled as an app's bundler does (package.json's
# sideEffects honoured), then run where Hermes before React Native 0.85
# runs it: no TextEncoder, no TextDecoder, no web APIs.
cat > rn-entry.js <<'EOF'
import { createReactNativeClient } from 'wse-client/react-native';

const appState = { currentState: 'active', addEventListener: () => ({ remove() {} }) };
const client = createReactNativeClient({ endpoints: ['wss://example.com/wse'], appState });
if (client.state.getSnapshot().connection !== 'pending') throw new Error('not pending');
client.destroy();
globalThis.wseCheck = 'ok';
EOF
cat > rn-run.mjs <<'EOF'
import { build } from 'esbuild';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const result = await build({
  entryPoints: ['rn-entry.js'],
  bundle: true,
  format: 'iife',
  platform: 'neutral',
  mainFields: ['module', 'main'],
  conditions: ['react-native', 'import', 'default'],
  define: { 'process.env.NODE_ENV': '"production"' },
  outfile: 'out/rn.js',
  logLevel: 'warning',
});
if (result.errors.length || result.warnings.length) process.exit(1);
const sandbox = { console, setTimeout, clearTimeout, setInterval, clearInterval, queueMicrotask };
vm.createContext(sandbox);
vm.runInContext(readFileSync('out/rn.js', 'utf8'), sandbox, { filename: 'rn.js' });
if (sandbox.wseCheck !== 'ok') throw new Error('the React Native bundle did not run to its end');
console.log('the React Native entry runs without TextEncoder and TextDecoder');
EOF
echo "::group::The React Native entry, bundled, without TextEncoder and TextDecoder"
node rn-run.mjs
echo "::endgroup::"

echo "::group::Types and package layout (attw, publint)"
# ESM only: CommonJS consumers use a dynamic import.
npx --yes @arethetypeswrong/cli@0.18.5 "$tarball" --profile esm-only
npx --yes publint@0.3.25 --strict "$tarball"
echo "::endgroup::"

echo "::group::Every source map's sources are in the package"
mkdir "$work/unpacked"
tar -xzf "$tarball" -C "$work/unpacked"
node --input-type=module - "$work/unpacked/package" <<'NODE'
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
const root = resolve(process.argv[2]);
const maps = readdirSync(root, { recursive: true }).filter((f) => f.endsWith('.map'));
const missing = [];
for (const map of maps) {
  const { sources = [], sourceRoot = '' } = JSON.parse(readFileSync(join(root, map), 'utf8'));
  for (const source of sources) {
    const at = resolve(dirname(join(root, map)), sourceRoot, source);
    if (!at.startsWith(root) || !existsSync(at)) missing.push(`${map}: ${source}`);
  }
}
if (maps.length === 0 || missing.length > 0) {
  console.error(`${missing.length} sources missing of ${maps.length} maps`, missing.slice(0, 5));
  process.exit(1);
}
console.log(`${maps.length} maps, every source in the package`);
NODE
echo "::endgroup::"
