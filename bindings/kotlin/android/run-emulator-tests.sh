#!/usr/bin/env bash
# Builds the bindings and the Android library of ABI (build-libs.sh), starts
# the Rust client's example server and the conformance server, and runs the
# instrumented tests, the conformance cases among them (agent kotlin-android,
# checked afterwards): on the emulator ANDROID_SERIAL names, or on one it
# starts without a window (IMAGE picks its system image, default Android 36,
# x86_64). Needs ANDROID_HOME and ANDROID_NDK_HOME; GRADLE names the gradle.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
# Cargo's target directory: CARGO_TARGET_DIR (relative to the repository), else target/.
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
export CARGO_TARGET_DIR="$target"
image="${IMAGE:-system-images;android-36;google_apis;x86_64}"
abi="${ABI:-x86_64}"
export PATH="$ANDROID_HOME/cmdline-tools/latest/bin:$ANDROID_HOME/platform-tools:$ANDROID_HOME/emulator:$PATH"
"$here/../build.sh"
"$here/build-libs.sh" "$abi"
export WSE_JNI_LIBS="$target/android-jniLibs" WSE_ANDROID_VERIFIER="$target/android-verifier/rustls-platform-verifier.jar"
cd "$root"

port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"
cargo build -q -p wse-client --example server
# Its wss:// door on the next port, with a CA of its own made for the run.
WSE_PORT="$port" WSE_TLS_PORT="$((port + 1))" WSE_TLS_CA="$target/android-test-ca.pem" \
  "$target/debug/examples/server" > "$target/android-test-server.log" 2>&1 &
server=$!
# The conformance server, each case on a server of its own.
cargo build -q -p wse-conformance
conformance="$(mktemp -d "$target/android-conformance.XXXXXX")"
"$target/debug/wse-conformance" serve --out "$conformance" > "$conformance/serve.log" 2>&1 &
judge=$!
emu=""
added=""
finish() {
  if [[ -n "$added" ]]; then adb shell rm -f "$added" 2>/dev/null || true; fi
  if [[ -n "$emu" ]]; then
    adb emu kill 2>/dev/null || true
    kill "$emu" 2>/dev/null || true
  fi
  kill "$server" "$judge" 2>/dev/null || true
}
trap finish EXIT
if [[ -z "${ANDROID_SERIAL:-}" ]]; then
  echo no | avdmanager create avd -n wse-test -k "$image" -d pixel_6 --force > /dev/null
  emulator -avd wse-test -no-window -no-audio -no-snapshot -no-boot-anim -gpu swiftshader_indirect \
    > "$target/android-emulator.log" 2>&1 &
  emu=$!
  export ANDROID_SERIAL=emulator-5554
fi
adb wait-for-device
until [[ "$(adb shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')" == "1" ]]; do sleep 2; done
echo "emulator up: page size $(adb shell getconf PAGE_SIZE | tr -d '\r')"
for _ in $(seq 1 50); do
  grep -q "signing_public_key" "$target/android-test-server.log" && break
  sleep 0.1
done
for _ in $(seq 1 100); do
  grep -q '"ready"' "$conformance/serve.log" && break
  sleep 0.1
done
# The emulator reaches the control channel, and so each case's server, as
# 10.0.2.2.
control="$(sed -n 's/.*"control":"ws:\/\/127.0.0.1:\([0-9]*\)\/control".*/\1/p' "$conformance/serve.log")"
[[ -n "$control" ]] || { cat "$conformance/serve.log" >&2; exit 1; }
# The run's CA among those the device's owner added (as root: a google_apis
# image), so Android's own verifier is seen to connect; taken out afterwards.
ca="$target/android-test-ca.pem"
store=/data/misc/user/0/cacerts-added
adb root > /dev/null
adb wait-for-device
until [[ "$(adb shell id -u 2>/dev/null | tr -d '\r')" == "0" ]]; do sleep 1; done
adb shell mkdir -p "$store"
added="$store/$(openssl x509 -inform PEM -subject_hash_old -noout -in "$ca").0"
adb push "$ca" "$added" > /dev/null
adb shell "chmod 644 $added && chown system:system $added && restorecon $added"
cd "$here"
"${GRADLE:-gradle}" --no-daemon -q connectedDebugAndroidTest \
  -Pandroid.testInstrumentationRunnerArguments.wseUrl="ws://10.0.2.2:$port/wse" \
  -Pandroid.testInstrumentationRunnerArguments.wseTlsUrl="wss://10.0.2.2:$((port + 1))/wse" \
  -Pandroid.testInstrumentationRunnerArguments.wseCa="$(base64 < "$ca" | tr -d '\n')" \
  -Pandroid.testInstrumentationRunnerArguments.wseCaTrusted=1 \
  -Pandroid.testInstrumentationRunnerArguments.wseIdentity="$(sed -n 's/^e2e_identity_public_key //p' "$target/android-test-server.log")" \
  -Pandroid.testInstrumentationRunnerArguments.wseSigning="$(sed -n 's/^signing_public_key //p' "$target/android-test-server.log")" \
  -Pandroid.testInstrumentationRunnerArguments.wseConformance="ws://10.0.2.2:$control/control"
"$target/debug/wse-conformance" check --manifest "$root/tests/conformance/kotlin-android.json" --reports "$conformance"
