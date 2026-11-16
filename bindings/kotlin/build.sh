#!/usr/bin/env bash
# Builds the host native library of wse-ffi (target/debug) and generates the
# Kotlin bindings into src/main/kotlin.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
# Cargo's target directory: CARGO_TARGET_DIR (relative to the repository), else target/.
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
export CARGO_TARGET_DIR="$target"
cd "$root"
cargo build -p wse-ffi --lib
lib="$target/debug/libwse_ffi.so"
[[ -f "$lib" ]] || lib="$target/debug/libwse_ffi.dylib"
# For the JVM, and for Android (its own cleaner), from the same library.
cargo run -q -p wse-bindgen --bin uniffi-bindgen -- generate -l kotlin --no-format -o "$here/generated/jvm" "$lib"
cargo run -q -p wse-bindgen --bin uniffi-bindgen -- generate -l kotlin --no-format \
  --config "$here/android/uniffi.toml" -o "$here/generated/android" "$lib"
# UniFFI leaves spaces at line ends and blank lines at the end.
for file in "$here"/generated/*/dev/wse/ffi/*.kt; do
  sed -i.bak -E 's/[[:space:]]+$//' "$file" && rm -f "$file.bak"
  printf '%s\n' "$(cat "$file")" > "$file.tmp" && mv "$file.tmp" "$file"
done
echo "built $lib and the Kotlin bindings"
