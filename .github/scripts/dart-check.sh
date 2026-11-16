#!/usr/bin/env bash
# The staged Dart package's tests on this runner, its build hook taking this
# platform's prebuilt library from BINARIES and checking it against the list
# the package carries -- never a build from source. The example server, with
# its wss:// door and its keys, runs beside them. Run from the repository's
# root, PACKAGE and BINARIES there too (relative paths read the same on every
# runner, Windows included).
#
#   .github/scripts/dart-check.sh dart-package dart-binaries
set -euo pipefail
package="${1:?usage: dart-check.sh PACKAGE BINARIES}"
binaries="${2:?usage: dart-check.sh PACKAGE BINARIES}"
cargo build -q --locked -p wse-client --example server
server=target/debug/examples/server
[ -f "$server.exe" ] && server="$server.exe"
WSE_PORT=5019 WSE_TLS_PORT=5020 WSE_TLS_CA=dart-check-ca.pem "$server" > dart-check-server.log 2>&1 &
pid=$!
trap 'kill "$pid" 2>/dev/null || true' EXIT
for _ in $(seq 1 100); do
  grep -q '^signing_public_key ' dart-check-server.log && break
  sleep 0.1
done
rm -rf dart-check
cp -r "$package" dart-check
cat >> dart-check/pubspec.yaml <<YAML
hooks:
  user_defines:
    wse_client:
      build: binary
      binaries: ../$binaries
YAML
cd dart-check
flutter pub get
WSE_TEST_URL="$(sed -n 's/^url //p' ../dart-check-server.log)" \
WSE_TEST_TLS_URL="wss://localhost:5020/wse" \
WSE_TEST_CA=../dart-check-ca.pem \
WSE_TEST_IDENTITY="$(sed -n 's/^e2e_identity_public_key //p' ../dart-check-server.log)" \
WSE_TEST_SIGNING="$(sed -n 's/^signing_public_key //p' ../dart-check-server.log)" \
  flutter test
