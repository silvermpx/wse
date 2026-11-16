#!/usr/bin/env bash
# Checks the JVM package as a Java application gets it: jvm-consumer/ takes it
# from a Maven repository, with the dependencies its POM names, and runs
# against the Rust client's example server (Main.java says what it checks).
#
# WSE_MAVEN_REPO names the repository (maven-central.sh lays one out under
# OUT/repository). Without it, the package is made here as the release makes
# it, with this machine's own library (PROFILE=debug unless given).
# ALPINE=1 (on Linux) runs the application in Alpine's Java image too, a
# musl system, in a container beside this machine (ALPINE_JAVA_IMAGE, by
# default eclipse-temurin:21-jre-alpine, made for x64 and arm64); a package
# made here then carries the musl library as well, built in the container
# MUSL_IMAGE names.
# GRADLE names the gradle; the application is built and run by the JDK of
# CONSUMER_JAVA_HOME (default: JAVA_HOME, else java on PATH).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
# Cargo's target directory: CARGO_TARGET_DIR (relative to the repository), else target/.
target="$(cd "$root" && mkdir -p "${CARGO_TARGET_DIR:-target}" && cd "${CARGO_TARGET_DIR:-target}" && pwd)"
export CARGO_TARGET_DIR="$target"
gradle="${GRADLE:-gradle}"
version="$(sed -n 's/^version = "\(.*\)"$/\1/p' "$here/build.gradle.kts")"
# A path as the JVM reads it: Windows reads it the way Windows writes it.
jvm_path() { if command -v cygpath > /dev/null; then cygpath -m "$1"; else printf '%s' "$1"; fi; }

if [[ -z "${WSE_MAVEN_REPO:-}" ]]; then
  "$here/build.sh"
  natives="$target/jvm-natives"
  rm -rf "${natives:?}"
  host="$(rustc -vV | sed -n 's/^host: //p')"
  PROFILE="${PROFILE:-debug}" "$here/jvm-natives.sh" "$natives" "$host"
  if [[ -n "${ALPINE:-}" ]]; then
    # Built in the container as root, into a directory of its own.
    musl="$root/target/jvm-musl"
    docker run --rm -v "$root:/io" -w /io -e CARGO_TARGET_DIR=target/jvm-musl -e PROFILE="${PROFILE:-debug}" \
      "${MUSL_IMAGE:?MUSL_IMAGE names the container to build the musl library in}" \
      bash bindings/kotlin/jvm-natives.sh target/jvm-musl/natives "${host%-gnu}-musl"
    cp -R "$musl/natives/." "$natives/"
  fi
  (cd "$here" && WSE_JVM_NATIVES="$(jvm_path "$natives")" "$gradle" --no-daemon -q jar)
  bash "$root/.github/scripts/maven-central.sh" "$version" "$target/jvm-maven" --repository \
    "$here/build/libs/wse-client-jvm-$version.jar"
  WSE_MAVEN_REPO="$target/jvm-maven/repository"
fi

java_home="${CONSUMER_JAVA_HOME:-${JAVA_HOME:-}}"
java="${java_home:+$java_home/bin/}java"
(
  cd "$here/jvm-consumer"
  rm -rf build/install
  JAVA_HOME="$java_home" WSE_MAVEN_REPO="$(jvm_path "$WSE_MAVEN_REPO")" WSE_VERSION="$version" \
    "$gradle" --no-daemon -q installDist
)
"$java" -version 2>&1 | head -1

cd "$root"
cargo build -q -p wse-client --example server
program="$target/debug/examples/server"
[[ -f "$program.exe" ]] && program="$program.exe"
port="${WSE_PORT:-$((20000 + RANDOM % 20000))}"
log="$target/jvm-consumer-server.log"
ca="$target/jvm-consumer-ca.pem"
WSE_PORT="$port" WSE_TLS_PORT="$((port + 1))" WSE_TLS_CA="$ca" "$program" > "$log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do
  grep -q "signing_public_key" "$log" && break
  sleep 0.1
done
identity="$(sed -n 's/^e2e_identity_public_key //p' "$log")"
signing="$(sed -n 's/^signing_public_key //p' "$log")"
app="$here/jvm-consumer/build/install/wse-jvm-consumer"

echo "the application on this machine:"
"$java" --enable-native-access=ALL-UNNAMED -cp "$(jvm_path "$app")/lib/*" dev.wse.consumer.Main \
  "ws://127.0.0.1:$port/wse" "wss://localhost:$((port + 1))/wse" "$(jvm_path "$ca")" "$identity" "$signing"
if [[ -n "${ALPINE:-}" ]]; then
  echo "the application in Alpine:"
  docker run --rm --network host -v "$app:/app:ro" -v "$ca:/ca.pem:ro" \
    "${ALPINE_JAVA_IMAGE:-eclipse-temurin:21-jre-alpine}" \
    java --enable-native-access=ALL-UNNAMED -cp "/app/lib/*" dev.wse.consumer.Main \
    "ws://127.0.0.1:$port/wse" "wss://localhost:$((port + 1))/wse" /ca.pem "$identity" "$signing"
fi
