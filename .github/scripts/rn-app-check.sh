#!/usr/bin/env bash
# The packed wse-client in a React Native app on an Android emulator: an app
# made from React Native's template, built for release (Hermes bytecode, as
# an app ships), installed on DEVICE and run against the Rust client's example
# server. The app connects with React Native's own WebSocket, hears a tick and
# the answer to an echo, and logs the outcome (ReactNativeJS: "WSE_RN ok", or
# what went wrong); then it runs the conformance cases against a conformance
# server of its own (the TypeScript driver, agent react-native, its WebCrypto
# from react-native-quick-crypto), which are checked here. It shows no text.
# Needs ANDROID_HOME and a JDK.
#
#   .github/scripts/rn-app-check.sh npm-dist/wse-client-3.2.0.tgz emulator-5554
set -euo pipefail
if [[ $# -ne 2 || ! -f "$1" ]]; then
  echo "usage: $0 path/to/wse-client-X.Y.Z.tgz DEVICE" >&2
  exit 2
fi
tarball="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
device="$2"
root="$(cd "$(dirname "$0")/../.." && pwd)"
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
# The React Native rn-bundle-check.sh bundles for, and the CLI that makes
# the app from its template.
rn_version="0.88.1"
cli_version="21.0.0"
# WebCrypto for E2E and signed messages, as an app adds it.
crypto_packages=(react-native-quick-crypto@1.1.9 react-native-nitro-modules@0.37.2 react-native-quick-base64@3.0.1)
app=dev.wse.rncheck

work="$(mktemp -d)"
port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"
# An example's selection would leave the conformance server out: two builds.
(cd "$root" && cargo build -q --locked -p wse-client --example server && cargo build -q --locked -p wse-conformance)
WSE_PORT="$port" "$target/debug/examples/server" > "$work/server.log" 2>&1 &
server=$!
# The conformance server, each case on a server of its own.
mkdir "$work/conformance"
"$target/debug/wse-conformance" serve --out "$work/conformance" > "$work/conformance/serve.log" 2>&1 &
judge=$!
trap 'kill "$server" "$judge" 2>/dev/null || true; adb -s "$device" uninstall "$app" > /dev/null 2>&1 || true; rm -rf "${work:?}"' EXIT
for _ in $(seq 1 50); do
  grep -q "signing_public_key" "$work/server.log" && break
  sleep 0.1
done
for _ in $(seq 1 100); do
  grep -q '"ready"' "$work/conformance/serve.log" && break
  sleep 0.1
done
control="$(sed -n 's/.*"control":"ws:\/\/127.0.0.1:\([0-9]*\)\/control".*/\1/p' "$work/conformance/serve.log")"
[[ -n "$control" ]] || { cat "$work/conformance/serve.log" >&2; exit 1; }

cd "$work"
npx --yes "@react-native-community/cli@$cli_version" init WseRnCheck --version "$rn_version" \
  --package-name "$app" --directory app --skip-git-init --install-pods false --pm npm \
  --skip-install > init.log 2>&1 || { tail -30 init.log >&2; exit 1; }
cd app
# The app is built and run, never linted or unit-tested: the template's lint
# and test tools stay out, and with them their releases' breakages (a
# typescript-eslint published without its types package stops every install).
npm pkg delete devDependencies.eslint devDependencies.prettier devDependencies.jest \
  devDependencies.react-test-renderer devDependencies.@react-native/eslint-config \
  devDependencies.@react-native/jest-preset devDependencies.@types/jest \
  devDependencies.@types/react-test-renderer
# npm can name a version a moment before it serves it (a package being
# published): the template's install is tried again a minute later.
for attempt in 1 2 3; do
  npm install --no-audit --no-fund > install.log 2>&1 && break
  if [[ $attempt == 3 ]] || ! grep -q ETARGET install.log; then
    tail -30 install.log >&2
    exit 1
  fi
  sleep 60
done
npm install --no-audit --no-fund "$tarball" "${crypto_packages[@]}" > /dev/null
# The emulator reaches the host's loopback at 10.0.2.2: the example server,
# and the conformance server's control channel and the cases' servers.
cat > wse-check.json <<JSON
{ "url": "ws://10.0.2.2:$port/wse", "control": "ws://10.0.2.2:$control/control" }
JSON
# The TypeScript driver of the conformance cases; its imports are types only.
cp "$root/ts-client/__tests__/conformance/driver.ts" conformance.ts
cat > App.tsx <<'TSX'
import { useEffect } from 'react';
import { AppState, Platform } from 'react-native';
import QuickCrypto from 'react-native-quick-crypto';
import { generateSigningKey, setWebCrypto } from 'wse-client/core';
import { createReactNativeClient } from 'wse-client/react-native';
import { runCases, type Asked, type Made } from './conformance';
import config from './wse-check.json';

// E2E and signed messages take their WebCrypto from quick-crypto, a signing
// key made before any client too.
setWebCrypto(QuickCrypto as unknown as Crypto);

const client = createReactNativeClient({ endpoints: [config.url], appState: AppState });

/** A case's client, as the conformance driver asks for it. */
async function make(asked: Asked): Promise<Made> {
  const security: Record<string, unknown> = { encryptionEnabled: asked.e2eKey !== null };
  if (asked.e2eKey) security.serverKeys = [asked.e2eKey];
  let publicKey: string | null = null;
  if (asked.signing) {
    const signing = await generateSigningKey();
    publicKey = signing.publicKey;
    security.signingKey = signing.keyPair;
    security.serverSigningKeys = asked.serverSigningKey ? [asked.serverSigningKey] : [];
    // The application binds the key (bind_key): the token names none.
    security.bindSession = false;
  }
  const made = createReactNativeClient({
    endpoints: [asked.url],
    token: asked.token,
    appState: AppState,
    crypto: QuickCrypto as unknown as Crypto,
    reconnection: { baseDelay: 50, maxDelay: 300, jitter: false },
    wireFormat: asked.format,
    security: security as never,
    ...asked.application,
  });
  return { client: made, publicKey };
}

/** The conformance cases; the server judges them, this says when they are done. */
function conformance() {
  runCases({
    control: config.control,
    agent: 'react-native',
    runtime: typeof (globalThis as { HermesInternal?: unknown }).HermesInternal === 'object' ? 'hermes' : 'react-native',
    os: `${Platform.OS} ${Platform.Version}`,
    arch: '',
    make,
    bridged: false,
  }).then(
    (failed) => console.log(`WSE_RN_CONFORMANCE ${failed.length === 0 ? 'ok' : `failed: ${failed.join(' | ')}`}`),
    (error) => console.log(`WSE_RN_CONFORMANCE error: ${error}`),
  );
}

export default function App() {
  useEffect(() => {
    const lease = client.retain();
    const ticks = client.subscribe(['ticks']);
    let tick = false;
    let echoed = false;
    let said = false;
    const say = (outcome: string) => {
      if (said) return;
      said = true;
      console.log(`WSE_RN ${outcome}`);
    };
    const timer = setTimeout(() => say(`no ${tick ? 'echo' : 'tick'} in 30 s`), 30_000);
    const done = () => {
      if (!tick || !echoed || said) return;
      clearTimeout(timer);
      say('ok');
      conformance();
    };
    const offTick = client.onMessage('tick', () => {
      tick = true;
      done();
    });
    const offEcho = client.onMessage('echoed', (msg) => {
      if (msg.p?.text !== 'hermes') return say(`the echo said ${JSON.stringify(msg.p)}`);
      echoed = true;
      done();
    });
    client.send('echo', { text: 'hermes' });
    return () => {
      clearTimeout(timer);
      offTick();
      offEcho();
      ticks.release();
      lease.release();
    };
  }, []);
  return null;
}
TSX
# The example server speaks ws://, which a release build refuses unless the
# app allows it.
manifest=android/app/src/main/AndroidManifest.xml
sed -i.bak -E 's/ android:usesCleartextTraffic="[^"]*"//; s/<application/<application android:usesCleartextTraffic="true"/' "$manifest"
rm -f "$manifest.bak"
abi="${ABI:-x86_64}"
(cd android && ./gradlew --no-daemon -q assembleRelease -PreactNativeArchitectures="$abi") > gradle.log 2>&1 \
  || { tail -40 gradle.log >&2; exit 1; }

adb -s "$device" install -r android/app/build/outputs/apk/release/app-release.apk > /dev/null
adb -s "$device" logcat -c
adb -s "$device" shell am start -n "$app/.MainActivity" > /dev/null
outcome=""
for _ in $(seq 1 60); do
  outcome="$(adb -s "$device" logcat -d -s ReactNativeJS:V | sed -n 's/.*WSE_RN //p' | tail -1 | tr -d '\r')"
  [[ -n "$outcome" ]] && break
  sleep 1
done
if [[ "$outcome" != ok ]]; then
  adb -s "$device" logcat -d -s ReactNativeJS:V AndroidRuntime:E | tail -40 >&2 || true
fi
echo "React Native $rn_version, release build on Hermes: ${outcome:-no answer in 60 s}"
[[ "$outcome" == ok ]]
# The conformance cases, then the judge's verdict on them.
said=""
for _ in $(seq 1 600); do
  said="$(adb -s "$device" logcat -d -s ReactNativeJS:V | sed -n 's/.*WSE_RN_CONFORMANCE //p' | tail -1 | tr -d '\r')"
  [[ -n "$said" ]] && break
  sleep 1
done
echo "React Native's conformance cases: ${said:-no answer in 600 s}"
if [[ -z "$said" ]]; then
  adb -s "$device" logcat -d -s ReactNativeJS:V AndroidRuntime:E | tail -40 >&2 || true
fi
"$target/debug/wse-conformance" check --manifest "$root/tests/conformance/react-native.json" --reports "$work/conformance"
