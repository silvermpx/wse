#!/usr/bin/env bash
# Builds the host native library of wse-ffi (target/debug) and generates the
# Kotlin bindings into src/main/kotlin.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
cd "$root"
cargo build -p wse-ffi --lib
lib="$root/target/debug/libwse_ffi.so"
[[ -f "$lib" ]] || lib="$root/target/debug/libwse_ffi.dylib"
# For the JVM, and for Android (its own cleaner), from the same library.
cargo run -q -p wse-bindgen --bin uniffi-bindgen -- generate -l kotlin --no-format -o "$here/generated/jvm" "$lib"
cargo run -q -p wse-bindgen --bin uniffi-bindgen -- generate -l kotlin --no-format \
  --config "$here/android/uniffi.toml" -o "$here/generated/android" "$lib"
echo "built $lib and the Kotlin bindings"

