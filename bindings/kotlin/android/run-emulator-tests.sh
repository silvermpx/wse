#!/usr/bin/env bash
# Builds the Android libraries (cargo-ndk) and the bindings, starts an
# emulator without a window and the Rust client's example server, and runs
# the instrumented tests. Needs ANDROID_HOME, ANDROID_NDK_HOME, JAVA_HOME and
# GRADLE; IMAGE picks the system image (default Android 36, x86_64).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
image="${IMAGE:-system-images;android-36;google_apis;x86_64}"
abi="${ABI:-x86_64}"
export PATH="$ANDROID_HOME/cmdline-tools/latest/bin:$ANDROID_HOME/platform-tools:$ANDROID_HOME/emulator:$PATH"
"$here/../build.sh"
cd "$root"
jni="$root/target/android-jniLibs"
cargo ndk -t "$abi" -P 26 -o "$jni" build -p wse-ffi --release --locked
export WSE_JNI_LIBS="$jni"

port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"
cargo build -q -p wse-client --example server
WSE_PORT="$port" "$root/target/debug/examples/server" > "$root/target/android-test-server.log" 2>&1 &
server=$!
echo no | avdmanager create avd -n wse-test -k "$image" -d pixel_6 --force > /dev/null
emulator -avd wse-test -no-window -no-audio -no-snapshot -no-boot-anim -gpu swiftshader_indirect \
  > "$root/target/android-emulator.log" 2>&1 &
emu=$!
trap 'adb -s emulator-5554 emu kill 2>/dev/null || true; kill "$server" "$emu" 2>/dev/null || true' EXIT
adb wait-for-device
until [[ "$(adb shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')" == "1" ]]; do sleep 2; done
echo "emulator up: page size $(adb shell getconf PAGE_SIZE | tr -d '\r')"
cd "$here"
"$GRADLE" --no-daemon -q connectedDebugAndroidTest \
  -Pandroid.testInstrumentationRunnerArguments.wseUrl="ws://10.0.2.2:$port/wse"
