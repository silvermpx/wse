#!/usr/bin/env bash
# Builds the bindings and the Android library of ABI (build-libs.sh), starts
# the Rust client's example server and runs the instrumented tests: on the
# emulator ANDROID_SERIAL names, or on one it starts without a window (IMAGE
# picks its system image, default Android 36, x86_64). Needs ANDROID_HOME
# and ANDROID_NDK_HOME; GRADLE names the gradle.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
image="${IMAGE:-system-images;android-36;google_apis;x86_64}"
abi="${ABI:-x86_64}"
export PATH="$ANDROID_HOME/cmdline-tools/latest/bin:$ANDROID_HOME/platform-tools:$ANDROID_HOME/emulator:$PATH"
"$here/../build.sh"
"$here/build-libs.sh" "$abi"
export WSE_JNI_LIBS="$root/target/android-jniLibs"
cd "$root"

port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"
cargo build -q -p wse-client --example server
WSE_PORT="$port" "$root/target/debug/examples/server" > "$root/target/android-test-server.log" 2>&1 &
server=$!
if [[ -z "${ANDROID_SERIAL:-}" ]]; then
  echo no | avdmanager create avd -n wse-test -k "$image" -d pixel_6 --force > /dev/null
  emulator -avd wse-test -no-window -no-audio -no-snapshot -no-boot-anim -gpu swiftshader_indirect \
    > "$root/target/android-emulator.log" 2>&1 &
  emu=$!
  export ANDROID_SERIAL=emulator-5554
  trap 'adb emu kill 2>/dev/null || true; kill "$server" "$emu" 2>/dev/null || true' EXIT
else
  trap 'kill "$server" 2>/dev/null || true' EXIT
fi
adb wait-for-device
until [[ "$(adb shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')" == "1" ]]; do sleep 2; done
echo "emulator up: page size $(adb shell getconf PAGE_SIZE | tr -d '\r')"
cd "$here"
"${GRADLE:-gradle}" --no-daemon -q connectedDebugAndroidTest \
  -Pandroid.testInstrumentationRunnerArguments.wseUrl="ws://10.0.2.2:$port/wse"
