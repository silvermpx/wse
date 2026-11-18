#!/usr/bin/env bash
# Builds the release AAR (or takes the one WSE_AAR names) and an app that uses
# it with R8 on (consumer/), from a Maven repository with its POM as its one
# dependency, and checks on the emulator ANDROID_SERIAL names
# that Android's certificate verifier turns down a server of an unknown CA,
# that the app hears a tick, that an end-to-end encrypted session echoes and
# that a signed message gets a signed receipt: what JNA, the bindings and the
# verifier reach by reflection or JNI survived shrinking.
# Needs ANDROID_HOME (and, to build the AAR, ANDROID_NDK_HOME); GRADLE names
# the gradle; ABI the emulator's (default x86_64).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
# Cargo's target directory: CARGO_TARGET_DIR (relative to the repository), else target/.
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
export CARGO_TARGET_DIR="$target"
export PATH="$ANDROID_HOME/platform-tools:$PATH"
: "${ANDROID_SERIAL:?the emulator to run on}"
gradle="${GRADLE:-gradle}"

if [[ -n "${WSE_AAR:-}" ]]; then
  # A release's AAR, as it ships.
  WSE_AAR="$(cd "$(dirname "$WSE_AAR")" && pwd)/$(basename "$WSE_AAR")"
else
  "$here/../build.sh"
  "$here/build-libs.sh" "${ABI:-x86_64}"
  export WSE_JNI_LIBS="$target/android-jniLibs" WSE_ANDROID_VERIFIER="$target/android-verifier/rustls-platform-verifier.jar"
  (cd "$here" && "$gradle" --no-daemon -q assembleRelease)
  WSE_AAR="$(ls "$here"/build/outputs/aar/*-release.aar)"
fi
# The AAR as Maven Central gets it, under its name and with its POM, in a
# repository of its own: the app names that one dependency alone.
version="$(sed -n 's/^version = "\(.*\)"$/\1/p' "$here/../build.gradle.kts")"
maven="$target/android-consumer-maven"
mkdir -p "$maven/dist"
cp "$WSE_AAR" "$maven/dist/wse-client-android-$version.aar"
"$root/.github/scripts/maven-central.sh" "$version" "$maven" --repository "$maven/dist/wse-client-android-$version.aar"
export WSE_MAVEN_REPO="$maven/repository" WSE_VERSION="$version"
(cd "$here/consumer" && "$gradle" --no-daemon -q assembleRelease)
apk="$(ls "$here"/consumer/build/outputs/apk/release/*-release.apk)"

cd "$root"
port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"
cargo build -q -p wse-client --example server
WSE_PORT="$port" WSE_TLS_PORT="$((port + 1))" WSE_TLS_CA="$target/consumer-test-ca.pem" \
  "$target/debug/examples/server" > "$target/consumer-test-server.log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  grep -q "signing_public_key" "$target/consumer-test-server.log" && break
  sleep 0.1
done

adb install -r "$apk" > /dev/null
adb logcat -c
adb shell am start -n dev.wse.consumer/.MainActivity --es url "ws://10.0.2.2:$port/wse" \
  --es tls_url "wss://10.0.2.2:$((port + 1))/wse" \
  --es identity "$(sed -n 's/^e2e_identity_public_key //p' "$target/consumer-test-server.log")" \
  --es signing "$(sed -n 's/^signing_public_key //p' "$target/consumer-test-server.log")" > /dev/null
outcome=""
for _ in $(seq 1 60); do
  outcome="$(adb logcat -d -s WseConsumer:I | sed -n 's/.*WseConsumer: //p' | tail -1 | tr -d '\r')"
  [[ -n "$outcome" ]] && break
  sleep 1
done
if [[ "$outcome" != "ok" ]]; then
  # What the app said, and how it ended if it crashed.
  adb logcat -d -b crash | tail -40 >&2 || true
  adb logcat -d | grep -E "WseConsumer|dev.wse|AndroidRuntime|rustls" | tail -40 >&2 || true
fi
adb uninstall dev.wse.consumer > /dev/null || true
echo "the app built with R8: ${outcome:-no answer in 60 s}"
[[ "$outcome" == "ok" ]]
