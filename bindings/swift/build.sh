#!/usr/bin/env bash
# Builds the Swift package's native part: the Rust library for macOS (Apple
# silicon and Intel), iOS and the iOS simulator (both) in one XCFramework
# (WseFFIC.xcframework), and the Swift bindings UniFFI generates for it
# (Sources/WseFFI/WseFFI.swift).
#
#   bindings/swift/build.sh            # release
#   PROFILE=debug bindings/swift/build.sh
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
# Cargo's target directory: CARGO_TARGET_DIR (relative to the repository), else target/.
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
export CARGO_TARGET_DIR="$target"
profile="${PROFILE:-release}"
targets=(aarch64-apple-darwin x86_64-apple-darwin aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios)
export IPHONEOS_DEPLOYMENT_TARGET="${IPHONEOS_DEPLOYMENT_TARGET:-15.0}"
export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-12.0}"
cd "$root"

# The bindings come from an unstripped debug build of the host library.
cargo build -p wse-ffi --lib
host_lib="$target/debug/libwse_ffi.dylib"
gen="$target/swift-gen"
rm -rf "${gen:?}"
mkdir -p "$gen/include/WseFFIC"
cargo run -q -p wse-bindgen --bin uniffi-bindgen-swift -- "$host_lib" "$here/Sources/WseFFI" --swift-sources
# UniFFI leaves spaces at line ends.
sed -i.bak -E 's/[[:space:]]+$//' "$here/Sources/WseFFI/WseFFI.swift" && rm -f "$here/Sources/WseFFI/WseFFI.swift.bak"
cargo run -q -p wse-bindgen --bin uniffi-bindgen-swift -- "$host_lib" "$gen/include/WseFFIC" \
  --headers --modulemap --module-name WseFFIC --modulemap-filename module.modulemap \
  --link-frameworks Security --link-frameworks CoreFoundation

flag=(--release)
[[ "$profile" == "debug" ]] && flag=()
for triple in "${targets[@]}"; do
  cargo rustc -p wse-ffi --lib "${flag[@]}" --locked --target "$triple" --crate-type staticlib
done
lib() { echo "$target/$1/$profile/libwse_ffi.a"; }
# One slice per platform: the Mac's and the simulator's hold both architectures.
mkdir -p "$gen/macos" "$gen/ios-simulator"
lipo -create "$(lib aarch64-apple-darwin)" "$(lib x86_64-apple-darwin)" -output "$gen/macos/libwse_ffi.a"
lipo -create "$(lib aarch64-apple-ios-sim)" "$(lib x86_64-apple-ios)" -output "$gen/ios-simulator/libwse_ffi.a"
rm -rf "${here:?}/WseFFIC.xcframework"
xcodebuild -create-xcframework \
  -library "$gen/macos/libwse_ffi.a" -headers "$gen/include" \
  -library "$(lib aarch64-apple-ios)" -headers "$gen/include" \
  -library "$gen/ios-simulator/libwse_ffi.a" -headers "$gen/include" \
  -output "$here/WseFFIC.xcframework" >/dev/null
echo "built $here/WseFFIC.xcframework ($profile) and Sources/WseFFI/WseFFI.swift"
