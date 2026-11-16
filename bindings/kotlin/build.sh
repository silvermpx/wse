#!/usr/bin/env bash
# Builds the host native library of wse-ffi (target/debug) and generates the
# Kotlin bindings into generated/jvm and generated/android.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
# Cargo's target directory: CARGO_TARGET_DIR (relative to the repository), else target/.
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
export CARGO_TARGET_DIR="$target"
cd "$root"
cargo build -p wse-ffi --lib
for lib in "$target/debug/libwse_ffi.so" "$target/debug/libwse_ffi.dylib" "$target/debug/wse_ffi.dll"; do
  [[ -f "$lib" ]] && break
done
# For the JVM, and for Android (its own cleaner), from the same library.
cargo run -q -p wse-bindgen --bin uniffi-bindgen -- generate -l kotlin --no-format -o "$here/generated/jvm" "$lib"
cargo run -q -p wse-bindgen --bin uniffi-bindgen -- generate -l kotlin --no-format \
  --config "$here/android/uniffi.toml" -o "$here/generated/android" "$lib"
# UniFFI leaves spaces at line ends and blank lines at the end.
for file in "$here"/generated/*/dev/wse/ffi/*.kt; do
  sed -i.bak -E 's/[[:space:]]+$//' "$file" && rm -f "$file.bak"
  printf '%s\n' "$(cat "$file")" > "$file.tmp" && mv "$file.tmp" "$file"
done
# On the JVM the bindings take the library's name from WseNativeLibrary
# (src/jvm): a musl system loads the musl build the JAR carries.
jvm="$here/generated/jvm/dev/wse/ffi/wse_ffi.kt"
if [[ "$(grep -c '^    return "wse_ffi"$' "$jvm")" != 1 ]]; then
  echo "the JVM bindings find their library another way now: $jvm" >&2
  exit 1
fi
sed -i.bak 's/^    return "wse_ffi"$/    return WseNativeLibrary.name/' "$jvm" && rm -f "$jvm.bak"
echo "built $lib and the Kotlin bindings"
