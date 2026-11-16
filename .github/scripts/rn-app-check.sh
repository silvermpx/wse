#!/usr/bin/env bash
# The packed wse-client in a React Native app on an Android emulator: an app
# made from React Native's template, built for release (Hermes bytecode, as
# an app ships), installed on DEVICE and run against the Rust client's example
# server. The app connects with React Native's own WebSocket, hears a tick and
# the answer to an echo, and logs the outcome (ReactNativeJS: "WSE_RN ok", or
# what went wrong). It shows no text. Needs ANDROID_HOME and a JDK.
#
#   .github/scripts/rn-app-check.sh npm-dist/wse-client-3.1.0.tgz emulator-5554
set -euo pipefail
if [[ $# -ne 2 || ! -f "$1" ]]; then
  echo "usage: $0 path/to/wse-client-X.Y.Z.tgz DEVICE" >&2
  exit 2
fi
tarball="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
device="$2"
root="$(cd "$(dirname "$0")/../.." && pwd)"
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
# The React Native rn-bundle-check.sh bundles for.
rn_version="0.88.1"
app=dev.wse.rncheck

work="$(mktemp -d)"
port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"
(cd "$root" && cargo build -q --locked -p wse-client --example server)
WSE_PORT="$port" "$target/debug/examples/server" > "$work/server.log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true; adb -s "$device" uninstall "$app" > /dev/null 2>&1 || true; rm -rf "${work:?}"' EXIT
for _ in $(seq 1 50); do
  grep -q "signing_public_key" "$work/server.log" && break
  sleep 0.1
done

cd "$work"
npx --yes @react-native-community/cli@latest init WseRnCheck --version "$rn_version" \
  --package-name "$app" --directory app --skip-git-init --install-pods false --pm npm > init.log 2>&1 \
  || { tail -30 init.log >&2; exit 1; }
cd app
npm install --no-audit --no-fund "$tarball" > /dev/null
# The emulator reaches the host's loopback at 10.0.2.2.
cat > wse-check.json <<JSON
{ "url": "ws://10.0.2.2:$port/wse" }
JSON
cat > App.tsx <<'TSX'
import { useEffect } from 'react';
import { AppState } from 'react-native';
import { createReactNativeClient } from 'wse-client/react-native';
import config from './wse-check.json';

const client = createReactNativeClient({ endpoints: [config.url], appState: AppState });

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
      if (!tick || !echoed) return;
      clearTimeout(timer);
      say('ok');
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
