#!/usr/bin/env bash
# Writes the Java part of Android's certificate verifier to JAR: the classes
# of the rustls-platform-verifier crate's Android component, of the version
# Cargo.lock names, from the crate's own Maven repository and only with the
# SHA-256 rustls-platform-verifier.sha256 lists. The Kotlin library and the
# Flutter package carry it.
#
#   bindings/rustls-platform-verifier.sh target/android-verifier/rustls-platform-verifier.jar
set -euo pipefail
jar="${1:?usage: rustls-platform-verifier.sh JAR}"
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/.." && pwd)"
version="$(grep -A1 '^name = "rustls-platform-verifier-android"$' "$root/Cargo.lock" | sed -n 's/^version = "\(.*\)"$/\1/p')"
aar="rustls-platform-verifier-$version.aar"
want="$(sed -n "s/^\([0-9a-f]\{64\}\)  $aar\$/\1/p" "$here/rustls-platform-verifier.sha256")"
if [[ -z "$version" || -z "$want" ]]; then
  echo "${aar}: no SHA-256 in rustls-platform-verifier.sha256" >&2
  exit 1
fi
sha256() { if command -v sha256sum > /dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1; }
dir="$(dirname "$jar")"
mkdir -p "$dir"
if [[ ! -f "$dir/$aar" || "$(sha256 "$dir/$aar")" != "$want" ]]; then
  curl -fsSLo "$dir/$aar" \
    "https://github.com/rustls/rustls-platform-verifier/raw/maven-archive/android-release-support/maven/org/rustls/rustls-platform-verifier/$version/$aar"
fi
if [[ "$(sha256 "$dir/$aar")" != "$want" ]]; then
  echo "$aar does not have the SHA-256 rustls-platform-verifier.sha256 lists" >&2
  exit 1
fi
unzip -p "$dir/$aar" classes.jar > "$jar"
echo "the verifier's Java part $version in $jar"
