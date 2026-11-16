#!/usr/bin/env bash
# Runs the example app's integration test on DEVICE (a flutter device id: an
# iOS simulator's, an Android emulator's, or this desktop's: macos, windows,
# linux) against the Rust client's example server and its wss:// door; the
# package's build hook builds the Rust library for the device. FLUTTER names
# the flutter to run. MODE=release builds lib/check.dart in release mode
# instead: on an Android emulator it is installed and run, and its outcome
# read from the log; on a desktop the built app is copied to a folder whose
# path has a space and a letter outside ASCII and run from there; for an iOS
# simulator, which runs debug builds only, the release build is made for a
# device. APP names another copy of the example to run (default: this one),
# such as the one in a staged package whose build hook takes prebuilt
# libraries. Linux runs need a display (xvfb-run on a server).
set -euo pipefail
device="${1:?usage: run-integration-tests.sh <device id>}"
root="$(cd "$(dirname "$0")/../../.." && pwd)"
here="$(cd "${APP:-$(dirname "$0")}" && pwd)"
# Cargo's target directory: CARGO_TARGET_DIR (relative to the repository), else target/.
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
export CARGO_TARGET_DIR="$target"
flutter="${FLUTTER:-flutter}"
export DART_SUPPRESS_ANALYTICS=true FLUTTER_SUPPRESS_ANALYTICS=true
port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"

# Runs a command for at most SECONDS: its exit code, or 124 once it had to be
# stopped (macOS has no timeout(1)).
run_for() {
  local seconds="$1"
  shift
  "$@" &
  local pid=$! tenths=0
  while kill -0 "$pid" 2>/dev/null; do
    if ((tenths >= seconds * 10)); then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
      return 124
    fi
    sleep 0.1
    tenths=$((tenths + 1))
  done
  wait "$pid"
}

cd "$root"
cargo build -q -p wse-client --example server
program="$target/debug/examples/server"
[[ -f "$program.exe" ]] && program="$program.exe"
log="$target/flutter-example-server.log"
ca="$target/flutter-example-ca.pem"
WSE_PORT="$port" WSE_TLS_PORT="$((port + 1))" WSE_TLS_CA="$ca" "$program" > "$log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  grep -q "signing_public_key" "$log" && break
  sleep 0.1
done
# An Android emulator reaches the host's ports through adb.
if [[ "$device" == emulator-* ]]; then
  adb -s "$device" reverse "tcp:$port" "tcp:$port"
  adb -s "$device" reverse "tcp:$((port + 1))" "tcp:$((port + 1))"
fi
# The package's Android part carries the Java part of Android's certificate
# verifier; a staged package has its own.
if [[ -z "${APP:-}" && "$device" == emulator-* ]]; then
  "$root/bindings/rustls-platform-verifier.sh" "$here/../android/libs/rustls-platform-verifier.jar"
fi
defines=(
  --dart-define="WSE_URL=ws://127.0.0.1:$port/wse"
  --dart-define="WSE_TLS_URL=wss://127.0.0.1:$((port + 1))/wse"
  --dart-define="WSE_CA=$(base64 < "$ca" | tr -d '\n')"
  --dart-define="WSE_IDENTITY=$(sed -n 's/^e2e_identity_public_key //p' "$log")"
  --dart-define="WSE_SIGNING=$(sed -n 's/^signing_public_key //p' "$log")"
)
cd "$here"
"$flutter" pub get --suppress-analytics > /dev/null
case "${MODE:-debug}" in
  debug)
    "$flutter" test integration_test -d "$device" --suppress-analytics "${defines[@]}"
    ;;
  release)
    case "$device" in
      macos | windows | linux)
        "$flutter" build "$device" --release -t lib/check.dart --suppress-analytics "${defines[@]}"
        base="$(mktemp -d)"
        place="$base/WSE Prüfung"
        mkdir -p "$place"
        case "$device" in
          macos)
            cp -R build/macos/Build/Products/Release/wse_client_example.app "$place/"
            app="$place/wse_client_example.app/Contents/MacOS/wse_client_example"
            ;;
          linux)
            cp -R build/linux/*/release/bundle "$place/"
            app="$place/bundle/wse_client_example"
            ;;
          windows)
            cp -R build/windows/*/runner/Release "$place/"
            app="$place/Release/wse_client_example.exe"
            ;;
        esac
        code=0
        WSE_CHECK_FILE="$place/outcome" run_for 120 "$app" > "$place/output.log" 2>&1 || code=$?
        outcome="$(cat "$place/outcome" 2>/dev/null || sed -n 's/.*WSE_CHECK //p' "$place/output.log" | tail -1)"
        outcome="$(printf '%s' "$outcome" | tr -d '\r\n')"
        echo "the release build, run from $place: ${outcome:-no answer} (exit $code)"
        if [[ "$code" != 0 || "$outcome" != ok ]]; then
          tail -40 "$place/output.log" >&2 || true
          exit 1
        fi
        rm -rf "${base:?}"
        exit 0
        ;;
      emulator-*) ;;
      *)
        "$flutter" build ios --release --no-codesign -t lib/check.dart --suppress-analytics "${defines[@]}"
        echo "built lib/check.dart for iOS in release mode (simulators run debug builds only)"
        exit 0
        ;;
    esac
    "$flutter" build apk --release -t lib/check.dart --suppress-analytics "${defines[@]}"
    app=dev.wse.wse_client_example
    adb -s "$device" install -r build/app/outputs/flutter-apk/app-release.apk > /dev/null
    adb -s "$device" logcat -c
    adb -s "$device" shell am start -n "$app/.MainActivity" > /dev/null
    outcome=""
    for _ in $(seq 1 60); do
      outcome="$(adb -s "$device" logcat -d -s flutter:I | sed -n 's/.*WSE_CHECK //p' | tail -1 | tr -d '\r')"
      [[ -n "$outcome" ]] && break
      sleep 1
    done
    if [[ "$outcome" != ok ]]; then
      adb -s "$device" logcat -d -b crash | tail -40 >&2 || true
    fi
    adb -s "$device" uninstall "$app" > /dev/null || true
    echo "the release build: ${outcome:-no answer in 60 s}"
    [[ "$outcome" == ok ]]
    ;;
  *)
    echo "MODE is debug or release" >&2
    exit 2
    ;;
esac
