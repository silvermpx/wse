#!/usr/bin/env bash
# The React Native gate: the packed wse-client in an app's bundle, as React
# Native builds it -- Metro resolving the package's exports, React Native's
# Babel preset transforming it -- then compiled to Hermes bytecode with the
# Hermes compiler React Native ships. It fails when Metro cannot resolve or
# transform the package, or Hermes cannot compile what it made.
#
# usage: rn-bundle-check.sh path/to/wse-client-X.Y.Z.tgz
set -euo pipefail

if [[ $# -ne 1 || ! -f "$1" ]]; then
  echo "usage: $0 path/to/wse-client-X.Y.Z.tgz" >&2
  exit 2
fi
tarball="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
# React Native 0.88's own: its Metro and its Hermes compiler.
metro_version="0.87.3"
preset_version="0.88.1"
hermes_version="260318099.0.5"

work="$(mktemp -d)"
trap 'rm -rf "${work:?}"' EXIT
cd "$work"
cat > package.json <<'JSON'
{ "name": "wse-client-react-native-check", "version": "0.0.0", "private": true }
JSON
npm install --no-audit --no-fund --ignore-scripts "$tarball" react@19 \
  "metro@$metro_version" "metro-config@$metro_version" \
  "@react-native/babel-preset@$preset_version" "@react-native/metro-babel-transformer@$preset_version" \
  "hermes-compiler@$hermes_version" @babel/core > /dev/null

cat > index.js <<'JS'
import { createReactNativeClient, useWSEState } from 'wse-client/react-native';

const appState = { currentState: 'active', addEventListener: () => ({ remove() {} }) };
const client = createReactNativeClient({ endpoints: ['wss://example.com/wse'], appState });
if (client.state.getSnapshot().connection !== 'pending') throw new Error('not pending');
if (typeof useWSEState !== 'function') throw new Error('useWSEState missing');
client.destroy();
JS
cat > babel.config.js <<'JS'
module.exports = { presets: ['module:@react-native/babel-preset'] };
JS
cat > metro.config.js <<'JS'
module.exports = {
  projectRoot: __dirname,
  resolver: {
    // As React Native resolves a package: its exports, the react-native
    // condition first.
    unstable_enablePackageExports: true,
    unstable_conditionNames: ['react-native', 'import', 'require', 'default'],
    platforms: ['ios', 'android'],
  },
  transformer: {
    babelTransformerPath: require.resolve('@react-native/metro-babel-transformer'),
  },
};
JS

for platform in android ios; do
  npx --no-install metro build index.js --platform "$platform" --minify false --out "bundle.$platform.js" > /dev/null
  hermesc="$(node -e "const p=require('path');const os=process.platform==='darwin'?'osx-bin':process.platform==='win32'?'win64-bin':'linux64-bin';console.log(p.join(p.dirname(require.resolve('hermes-compiler/package.json')),'hermesc',os,'hermesc'))")"
  "$hermesc" -emit-binary -O -out "bundle.$platform.hbc" "bundle.$platform.js"
  echo "$platform: $(wc -c < "bundle.$platform.js") bytes of JavaScript, $(wc -c < "bundle.$platform.hbc") bytes of Hermes bytecode"
done
