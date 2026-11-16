#!/usr/bin/env bash
# Stages the Dart package as pub.dev gets it, into OUT: the committed
# bindings/dart, its build hook listing the prebuilt libraries in BINARIES
# with their SHA-256, its Rust crate on the published wse-client (a crate of
# its own, with the workspace's Cargo.lock), the Java part of Android's
# certificate verifier, and the license.
#
#   .github/scripts/stage-dart-package.sh 3.1.0 dart-binaries dart-package
set -euo pipefail
version="${1:?usage: stage-dart-package.sh VERSION BINARIES OUT}"
binaries="${2:?usage: stage-dart-package.sh VERSION BINARIES OUT}"
out="${3:?usage: stage-dart-package.sh VERSION BINARIES OUT}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
sha256() { python3 -c 'import hashlib, sys; print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())' "$1"; }

declared="$(sed -n 's/^version: //p' "$root/bindings/dart/pubspec.yaml")"
if [[ "$declared" != "$version" ]]; then
  echo "bindings/dart/pubspec.yaml has version $declared, not $version" >&2
  exit 1
fi
shopt -s nullglob
libraries=("$binaries"/wse_dart-"$version"-*)
if [[ ${#libraries[@]} -eq 0 ]]; then
  echo "no wse_dart-$version-* libraries in $binaries" >&2
  exit 1
fi

rm -rf "${out:?}"
mkdir -p "$out"
git -C "$root" archive --format=tar HEAD bindings/dart | tar -x -C "$out" --strip-components=2
cp "$root/LICENSE" "$out/LICENSE"
"$root/bindings/rustls-platform-verifier.sh" "$out/android/libs/rustls-platform-verifier.jar"
rm -f "$out"/android/libs/*.aar

{
  echo "// The release's prebuilt libraries: target triple to SHA-256. The release"
  echo "// writes them; a checkout of the repository lists none and builds from source."
  echo "const version = '$version';"
  echo "const binaries = <String, String>{"
  for library in "${libraries[@]}"; do
    name="$(basename "$library")"
    triple="${name#wse_dart-"$version"-}"
    echo "  '${triple%.*}': '$(sha256 "$library")',"
  done
  echo "};"
} > "$out/lib/src/hook/binaries.dart"

# The crate builds from crates.io's wse-client, outside any workspace.
sed -i.bak -E 's|^(wse-client = \{ version = "=[^"]+"), path = "[^"]+"|\1|' "$out/rust/Cargo.toml"
rm -f "$out/rust/Cargo.toml.bak"
if grep -q 'path = ' "$out/rust/Cargo.toml"; then
  echo "rust/Cargo.toml still names a path" >&2
  exit 1
fi
printf '\n[workspace]\n' >> "$out/rust/Cargo.toml"
cp "$root/Cargo.lock" "$out/rust/Cargo.lock"

sed -i.bak '/^publish_to: none$/d' "$out/pubspec.yaml"
rm -f "$out/pubspec.yaml.bak"
echo "staged $out with ${#libraries[@]} prebuilt libraries"
