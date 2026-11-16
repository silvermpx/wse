#!/usr/bin/env bash
# The staged Dart package's example app on a phone's system: its integration
# test on DEVICE (an Android emulator's or an iOS simulator's flutter id),
# and on an Android emulator its release build too, the package's build hook
# taking the device's prebuilt library from BINARIES and checking it against
# the list the package carries -- never a build from source. The app runs
# from a copy of the package in WORK (default dart-mobile-check).
#
#   .github/scripts/dart-mobile-check.sh dart-package dart-binaries emulator-5554
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
package="$(cd "${1:?usage: dart-mobile-check.sh PACKAGE BINARIES DEVICE}" && pwd)"
binaries="$(cd "${2:?usage: dart-mobile-check.sh PACKAGE BINARIES DEVICE}" && pwd)"
device="${3:?usage: dart-mobile-check.sh PACKAGE BINARIES DEVICE}"
work="${WORK:-dart-mobile-check}"
rm -rf "${work:?}"
cp -r "$package" "$work"
cat >> "$work/example/pubspec.yaml" <<YAML

hooks:
  user_defines:
    wse_client:
      build: binary
      binaries: $binaries
YAML
APP="$work/example" "$root/bindings/dart/example/run-integration-tests.sh" "$device"
if [[ "$device" == emulator-* ]]; then
  APP="$work/example" MODE=release "$root/bindings/dart/example/run-integration-tests.sh" "$device"
fi
