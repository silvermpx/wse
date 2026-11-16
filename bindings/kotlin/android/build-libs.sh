#!/usr/bin/env bash
# Builds the Android libraries of wse-ffi with cargo-ndk into
# target/android-jniLibs, one folder per ABI (default: arm64-v8a,
# armeabi-v7a and x86_64), and checks that each loads on 16 KB pages. Also
# fetches the Java part of Android's certificate verifier into
# target/android-verifier/rustls-platform-verifier.jar, for the AAR.
# Needs ANDROID_NDK_HOME.
#
#   bindings/kotlin/android/build-libs.sh            # every ABI
#   bindings/kotlin/android/build-libs.sh x86_64     # the emulator's
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
# Cargo's target directory: CARGO_TARGET_DIR (relative to the repository), else target/.
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
export CARGO_TARGET_DIR="$target"
abis=("$@")
[[ ${#abis[@]} -gt 0 ]] || abis=(arm64-v8a armeabi-v7a x86_64)
jni="$target/android-jniLibs"
args=()
for abi in "${abis[@]}"; do args+=(-t "$abi"); done
cd "$root"
cargo ndk "${args[@]}" -P 26 -o "$jni" build -p wse-ffi --release --locked
objdump="$(ls "$ANDROID_NDK_HOME"/toolchains/llvm/prebuilt/*/bin/llvm-objdump | head -1)"
for abi in "${abis[@]}"; do
  lib="$jni/$abi/libwse_ffi.so"
  if "$objdump" -p "$lib" | grep LOAD | grep -qv 'align 2\*\*1[4-9]'; then
    echo "$lib has a segment aligned below 16 KB" >&2
    exit 1
  fi
done
echo "built ${abis[*]} into $jni (16 KB pages)"

"$root/bindings/rustls-platform-verifier.sh" "$target/android-verifier/rustls-platform-verifier.jar"
