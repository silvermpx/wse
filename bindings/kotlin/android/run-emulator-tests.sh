#!/usr/bin/env bash
# Builds the bindings and the Android library of ABI (build-libs.sh), starts
# the Rust client's example server and runs the instrumented tests: on the
# emulator ANDROID_SERIAL names, or on one it starts without a window (IMAGE
# picks its system image, default Android 36, x86_64). Needs ANDROID_HOME
# and ANDROID_NDK_HOME; GRADLE names the gradle.
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
if [[ -z "${ANDROID_SERIAL:-}" ]]; then
  echo no | avdmanager create avd -n wse-test -k "$image" -d pixel_6 --force > /dev/null
  emulator -avd wse-test -no-window -no-audio -no-snapshot -no-boot-anim -gpu swiftshader_indirect \
    > "$target/android-emulator.log" 2>&1 &
  emu=$!
  export ANDROID_SERIAL=emulator-5554
  trap 'adb emu kill 2>/dev/null || true; kill "$server" "$emu" 2>/dev/null || true' EXIT
else
  trap 'kill "$server" 2>/dev/null || true' EXIT
fi
adb wait-for-device
until [[ "$(adb shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')" == "1" ]]; do sleep 2; done
echo "emulator up: page size $(adb shell getconf PAGE_SIZE | tr -d '\r')"
for _ in $(seq 1 50); do
  grep -q "tls_ca" "$target/android-test-server.log" && break
  sleep 0.1
done
cd "$here"
"${GRADLE:-gradle}" --no-daemon -q connectedDebugAndroidTest \
  -Pandroid.testInstrumentationRunnerArguments.wseUrl="ws://10.0.2.2:$port/wse" \
  -Pandroid.testInstrumentationRunnerArguments.wseTlsUrl="wss://10.0.2.2:$((port + 1))/wse" \
  -Pandroid.testInstrumentationRunnerArguments.wseCa="$(base64 < "$target/android-test-ca.pem" | tr -d '\n')"
