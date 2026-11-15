#!/usr/bin/env bash
# Builds the Swift package's native part: the Rust library for macOS, iOS and
# the iOS simulator in one XCFramework (WseFFIC.xcframework), and the Swift
# bindings UniFFI generates for it (Sources/WseFFI/WseFFI.swift).
#
#   bindings/swift/build.sh            # release
#   PROFILE=debug bindings/swift/build.sh
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
profile="${PROFILE:-release}"
targets=(aarch64-apple-darwin aarch64-apple-ios aarch64-apple-ios-sim)
export IPHONEOS_DEPLOYMENT_TARGET="${IPHONEOS_DEPLOYMENT_TARGET:-15.0}"
export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-12.0}"
cd "$root"

# The bindings come from an unstripped debug build of the host library.
cargo build -p wse-ffi --lib
host_lib="$root/target/debug/libwse_ffi.dylib"
gen="$root/target/swift-gen"
rm -rf "${gen:?}"
mkdir -p "$gen/include/WseFFIC"
cargo run -q -p wse-bindgen --bin uniffi-bindgen-swift -- "$host_lib" "$here/Sources/WseFFI" --swift-sources
cargo run -q -p wse-bindgen --bin uniffi-bindgen-swift -- "$host_lib" "$gen/include/WseFFIC" \
  --headers --modulemap --module-name WseFFIC --modulemap-filename module.modulemap \
  --link-frameworks Security --link-frameworks CoreFoundation

flag=(--release)
[[ "$profile" == "debug" ]] && flag=()
args=()
for target in "${targets[@]}"; do
  cargo rustc -p wse-ffi --lib "${flag[@]}" --locked --target "$target" --crate-type staticlib
  args+=(-library "$root/target/$target/$profile/libwse_ffi.a" -headers "$gen/include")
done
rm -rf "${here:?}/WseFFIC.xcframework"
xcodebuild -create-xcframework "${args[@]}" -output "$here/WseFFIC.xcframework" >/dev/null
echo "built $here/WseFFIC.xcframework ($profile) and Sources/WseFFI/WseFFI.swift"
