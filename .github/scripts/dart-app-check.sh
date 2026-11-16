#!/usr/bin/env bash
# The staged Dart package's example app on DEVICE: an Android emulator's or
# an iOS simulator's flutter id, or this desktop (macos, windows, linux). Its
# integration test, then its release build -- run on an Android emulator,
# copied and run on a desktop, made for an iPhone beside an iOS simulator
# (which runs debug builds only) -- the package's build hook taking each
# target's prebuilt library from BINARIES and checking it against the list
# the package carries: never a build from source. The app runs from a copy
# of the package in WORK (default dart-app-check). Linux needs a display
# (xvfb-run on a server).
#
#   .github/scripts/dart-app-check.sh dart-package dart-binaries emulator-5554
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
package="$(cd "${1:?usage: dart-app-check.sh PACKAGE BINARIES DEVICE}" && pwd)"
binaries="$(cd "${2:?usage: dart-app-check.sh PACKAGE BINARIES DEVICE}" && pwd)"
# Windows reads the path as Windows writes it.
command -v cygpath > /dev/null && binaries="$(cygpath -m "$binaries")"
device="${3:?usage: dart-app-check.sh PACKAGE BINARIES DEVICE}"
work="${WORK:-dart-app-check}"
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
APP="$work/example" MODE=release "$root/bindings/dart/example/run-integration-tests.sh" "$device"
