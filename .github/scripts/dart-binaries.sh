#!/usr/bin/env bash
# Builds the Dart package's Rust library (wse-dart) for each TARGET and copies
# it into OUT under the name the package's build hook downloads:
# wse_dart-VERSION-TARGET.EXT. Android goes through cargo-ndk (API 24,
# ANDROID_NDK_HOME) and must load on 16 KB pages; Apple targets are built for
# iOS 15 and macOS 10.15.
#
#   .github/scripts/dart-binaries.sh 3.1.0 dist aarch64-apple-ios x86_64-apple-darwin
set -euo pipefail
version="${1:?usage: dart-binaries.sh VERSION OUT TARGET...}"
out="${2:?usage: dart-binaries.sh VERSION OUT TARGET...}"
shift 2
root="$(cd "$(dirname "$0")/../.." && pwd)"
target_dir="${CARGO_TARGET_DIR:-$root/target}"
export IPHONEOS_DEPLOYMENT_TARGET=15.0 MACOSX_DEPLOYMENT_TARGET=10.15
mkdir -p "$out"
cd "$root"
for target in "$@"; do
  case "$target" in
    *-linux-android*)
      cargo ndk -t "$target" -P 24 build -p wse-dart --release --locked
      file=libwse_dart.so
      objdump="$(ls "$ANDROID_NDK_HOME"/toolchains/llvm/prebuilt/*/bin/llvm-objdump | head -1)"
      if "$objdump" -p "$target_dir/$target/release/$file" | grep LOAD | grep -qv 'align 2\*\*1[4-9]'; then
        echo "$target: a segment is aligned below 16 KB" >&2
        exit 1
      fi
      ;;
    *-apple-*)
      cargo build -p wse-dart --release --locked --target "$target"
      file=libwse_dart.dylib
      ;;
    *-windows-*)
      cargo build -p wse-dart --release --locked --target "$target"
      file=wse_dart.dll
      ;;
    *)
      cargo build -p wse-dart --release --locked --target "$target"
      file=libwse_dart.so
      ;;
  esac
  cp "$target_dir/$target/release/$file" "$out/wse_dart-$version-$target.${file##*.}"
  echo "built $out/wse_dart-$version-$target.${file##*.}"
done
