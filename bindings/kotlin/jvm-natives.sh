#!/usr/bin/env bash
# Builds the Rust library (wse-ffi) for each TARGET and puts it in OUT where
# the JVM package carries it: under JNA's prefix for the platform
# (OUT/linux-x86-64/libwse_ffi.so, OUT/darwin-aarch64/libwse_ffi.dylib,
# OUT/win32-x86-64/wse_ffi.dll, ...), a Linux build for musl under the prefix
# with -musl, where WseNativeLibrary.kt takes it from. PROFILE=debug builds a
# debug library (for a check of this machine's own); the default is release.
# Where a Linux build runs decides the oldest glibc it loads on: the release
# builds them in the containers the Python wheels are built in.
#
#   bindings/kotlin/jvm-natives.sh OUT x86_64-unknown-linux-gnu aarch64-apple-darwin ...
set -euo pipefail
out="${1:?usage: jvm-natives.sh OUT TARGET...}"
shift
[[ $# -gt 0 ]] || { echo "usage: jvm-natives.sh OUT TARGET..." >&2; exit 2; }
root="$(cd "$(dirname "$0")/../.." && pwd)"
mkdir -p "$out"
out="$(cd "$out" && pwd)"
# Cargo's target directory: CARGO_TARGET_DIR (relative to the repository), else target/.
target_dir="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
export CARGO_TARGET_DIR="$target_dir"
export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-10.15}"
profile="${PROFILE:-release}"
host="$(rustc -vV | sed -n 's/^host: //p')"
cd "$root"
for target in "$@"; do
  case "$target" in
    x86_64-unknown-linux-gnu) prefix=linux-x86-64 file=libwse_ffi.so ;;
    aarch64-unknown-linux-gnu) prefix=linux-aarch64 file=libwse_ffi.so ;;
    x86_64-unknown-linux-musl) prefix=linux-x86-64-musl file=libwse_ffi.so ;;
    aarch64-unknown-linux-musl) prefix=linux-aarch64-musl file=libwse_ffi.so ;;
    x86_64-apple-darwin) prefix=darwin-x86-64 file=libwse_ffi.dylib ;;
    aarch64-apple-darwin) prefix=darwin-aarch64 file=libwse_ffi.dylib ;;
    x86_64-pc-windows-msvc) prefix=win32-x86-64 file=wse_ffi.dll ;;
    aarch64-pc-windows-msvc) prefix=win32-aarch64 file=wse_ffi.dll ;;
    *) echo "the JVM package has no place for $target" >&2; exit 2 ;;
  esac
  args=(build -p wse-ffi --lib --locked)
  [[ "$profile" == release ]] && args+=(--release)
  # This machine's own target builds where build.sh builds, and shares its work.
  built="$target_dir/$profile"
  if [[ "$target" != "$host" ]]; then
    args+=(--target "$target")
    built="$target_dir/$target/$profile"
  fi
  if [[ "$target" == *-musl ]]; then
    # Linked to musl itself, the libc of the JVM that loads it, with the
    # unwinder inside: Alpine's Java images may have no libgcc_s.so.1, so the
    # libgcc_s Rust's standard library asks for is Rust's own static
    # libunwind (the unwinder of a static musl program) under that name.
    link="$target_dir/$target/static-unwind"
    mkdir -p "$link"
    cp "$(rustc --print sysroot)/lib/rustlib/$target/lib/self-contained/libunwind.a" "$link/libgcc_s.a"
    RUSTFLAGS="${RUSTFLAGS:-} -C target-feature=-crt-static -L native=$link" cargo "${args[@]}"
    if readelf -d "$built/$file" | grep -q 'NEEDED.*libgcc_s'; then
      echo "$built/$file still needs libgcc_s.so.1" >&2
      exit 1
    fi
  else
    cargo "${args[@]}"
  fi
  mkdir -p "$out/$prefix"
  cp "$built/$file" "$out/$prefix/$file"
  echo "$prefix/$file ($(du -h "$out/$prefix/$file" | cut -f1))"
done
