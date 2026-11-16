#!/usr/bin/env bash
# The WSE client's packages for Maven Central, as the Central Portal takes
# them: for each FILE, the Android library (wse-client-android-VERSION.aar)
# or the JVM library (wse-client-jvm-VERSION.jar), with its POM, a sources
# jar and a javadoc jar (a placeholder: the docs are the README), each signed
# with the release key and with its checksums, in Maven's repository layout
# under OUT/repository, and all of it in one bundle, OUT/bundle.zip.
#
#   --repository  lays out only the libraries and their POMs, unsigned: a
#             repository to install them from in a check (no zip needed);
#   --check   also asks the Portal whether it takes the token (a dry run of a
#             release: nothing is uploaded);
#   --upload  uploads the bundle (published once the Portal validated it) and
#             waits until Maven Central has it; a package of this version
#             already there is left out of it.
#
#   .github/scripts/maven-central.sh 3.2.0 out [--repository|--check|--upload] dist/wse-client-android-3.2.0.aar dist/wse-client-jvm-3.2.0.jar
#
# Signing: MAVEN_GPG_PRIVATE_KEY (armored) and MAVEN_GPG_PASSPHRASE; without
# them the files are left unsigned (a dry run of a fork, or a repository to
# install from in a check). Upload: MAVEN_CENTRAL_USERNAME and
# MAVEN_CENTRAL_PASSWORD, the Portal's user token.
set -euo pipefail
usage="usage: maven-central.sh VERSION OUT [--repository|--check|--upload] FILE..."
version="${1:?$usage}"
out="${2:?$usage}"
shift 2
mode=""
if [[ "${1:-}" == --repository || "${1:-}" == --check || "${1:-}" == --upload ]]; then
  mode="$1"
  shift
fi
[[ $# -gt 0 ]] || { echo "$usage" >&2; exit 2; }
root="$(cd "$(dirname "$0")/../.." && pwd)"
# Absolute: the steps below change directories.
mkdir -p "$out"
out="$(cd "$out" && pwd)"
group=io.github.silvermpx
work="$(mktemp -d)"
trap 'rm -rf "${work:?}"' EXIT
rm -rf "${out:?}/repository" "${out:?}/bundle.zip"

# The POM of ARTIFACT.
pom() {
  local artifact="$1" packaging name description dependencies
  case "$artifact" in
    wse-client-android)
      packaging=aar
      name="WSE client for Android"
      description="The WSE client for Android apps: topics, message recovery, reconnects, end-to-end encryption and signed messages, on the WSE Rust client."
      dependencies="$(dependency org.jetbrains.kotlin kotlin-stdlib 2.4.21 compile
        dependency org.jetbrains.kotlinx kotlinx-coroutines-core 1.11.0 compile
        dependency net.java.dev.jna jna 5.19.1 runtime aar
        dependency androidx.annotation annotation 1.9.1 runtime
        dependency androidx.lifecycle lifecycle-process 2.11.0 runtime
        dependency androidx.startup startup-runtime 1.2.0 runtime)"
      ;;
    wse-client-jvm)
      packaging=jar
      name="WSE client for the JVM"
      description="The WSE client for Java and Kotlin on the JVM (Linux, macOS and Windows, x64 and arm64): topics, message recovery, reconnects, end-to-end encryption and signed messages, on the WSE Rust client."
      dependencies="$(dependency org.jetbrains.kotlin kotlin-stdlib 2.4.21 compile
        dependency org.jetbrains.kotlinx kotlinx-coroutines-core-jvm 1.11.0 compile
        dependency net.java.dev.jna jna 5.19.1 runtime)"
      ;;
  esac
  cat <<POM
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>$group</groupId>
  <artifactId>$artifact</artifactId>
  <version>$version</version>
  <packaging>$packaging</packaging>
  <name>$name</name>
  <description>$description</description>
  <url>https://github.com/silvermpx/wse</url>
  <licenses>
    <license>
      <name>MIT License</name>
      <url>https://opensource.org/licenses/MIT</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <developers>
    <developer>
      <id>silvermpx</id>
      <name>silvermpx</name>
      <url>https://github.com/silvermpx</url>
    </developer>
  </developers>
  <scm>
    <connection>scm:git:https://github.com/silvermpx/wse.git</connection>
    <developerConnection>scm:git:ssh://git@github.com/silvermpx/wse.git</developerConnection>
    <url>https://github.com/silvermpx/wse</url>
    <tag>v$version</tag>
  </scm>
  <dependencies>
$dependencies
  </dependencies>
</project>
POM
}

# A <dependency> of a POM: GROUP ARTIFACT VERSION SCOPE [TYPE].
dependency() {
  printf '    <dependency>\n      <groupId>%s</groupId>\n      <artifactId>%s</artifactId>\n      <version>%s</version>\n' "$1" "$2" "$3"
  [[ -z "${5:-}" ]] || printf '      <type>%s</type>\n' "$5"
  printf '      <scope>%s</scope>\n    </dependency>\n' "$4"
}

packages=()
for file in "$@"; do
  case "$(basename "$file")" in
    "wse-client-android-$version.aar")
      artifact=wse-client-android ext=aar
      sources="bindings/kotlin/src/main/kotlin bindings/kotlin/android/src/main/kotlin bindings/kotlin/generated/android"
      ;;
    "wse-client-jvm-$version.jar")
      artifact=wse-client-jvm ext=jar
      sources="bindings/kotlin/src/main/kotlin bindings/kotlin/src/jvm/kotlin bindings/kotlin/generated/jvm"
      ;;
    *) echo "neither the Android nor the JVM library of $version: $file" >&2; exit 2 ;;
  esac
  base="$artifact-$version"
  if [[ "$mode" == --upload ]] && [[ "$(curl -sS -o /dev/null -w '%{http_code}' --retry 3 \
    "https://repo1.maven.org/maven2/${group//.//}/$artifact/$version/$base.pom" || true)" == 200 ]]; then
    echo "$group:$artifact:$version is on Maven Central already (an earlier attempt): left out"
    continue
  fi
  dir="$out/repository/${group//.//}/$artifact/$version"
  mkdir -p "$dir"
  cp "$file" "$dir/$base.$ext"
  pom "$artifact" > "$dir/$base.pom"
  packages+=("$group:$artifact:$version")
  [[ "$mode" != --repository ]] || continue
  # The Kotlin sources the library is built from, and a javadoc placeholder.
  rm -rf "${work:?}/sources" "${work:?}/javadoc"
  mkdir -p "$work/sources" "$work/javadoc"
  for src in $sources; do
    (cd "$root/$src" && tar -cf - .) | (cd "$work/sources" && tar -xf -)
  done
  printf 'The %s. Its documentation: https://github.com/silvermpx/wse/tree/main/bindings/kotlin\n' \
    "$(sed -n 's:.*<name>\(WSE client for [^<]*\)</name>.*:\1:p' "$dir/$base.pom")" > "$work/javadoc/README.txt"
  (cd "$work/sources" && zip -qr -X "$dir/$base-sources.jar" .)
  (cd "$work/javadoc" && zip -qr -X "$dir/$base-javadoc.jar" .)
done
if [[ ${#packages[@]} -eq 0 ]]; then
  echo "everything of $version is on Maven Central: nothing to publish"
  exit 0
fi
if [[ "$mode" == --repository ]]; then
  echo "laid out ${packages[*]} in $out/repository"
  exit 0
fi

if [[ -n "${MAVEN_GPG_PRIVATE_KEY:-}" ]]; then
  export GNUPGHOME="$work/gnupg"
  mkdir -m 700 "$GNUPGHOME"
  printf '%s\n' "$MAVEN_GPG_PRIVATE_KEY" | gpg --batch --quiet --import
fi
sum() { if command -v "${1}sum" > /dev/null; then "${1}sum" "$2"; else shasum -a "${1#sha}" "$2"; fi | cut -d' ' -f1; }
while IFS= read -r file; do
  if [[ -n "${MAVEN_GPG_PRIVATE_KEY:-}" ]]; then
    gpg --batch --quiet --yes --pinentry-mode loopback --passphrase "${MAVEN_GPG_PASSPHRASE:?no passphrase for the key}" \
      --armor --detach-sign --output "$file.asc" "$file"
    gpg --batch --quiet --verify "$file.asc" "$file"
  fi
  md5 -q "$file" > "$file.md5" 2>/dev/null || md5sum "$file" | cut -d' ' -f1 > "$file.md5"
  sum sha1 "$file" > "$file.sha1"
  sum sha256 "$file" > "$file.sha256"
  sum sha512 "$file" > "$file.sha512"
done < <(find "$out/repository" -type f | sort)
(cd "$out/repository" && zip -qr -X "../bundle.zip" .)
unzip -l "$out/bundle.zip"
if [[ -z "${MAVEN_GPG_PRIVATE_KEY:-}" ]]; then
  echo "built without signatures: no MAVEN_GPG_PRIVATE_KEY"
  [[ -z "$mode" ]] || { echo "Maven Central takes signed files only" >&2; exit 1; }
fi
[[ -n "$mode" ]] || exit 0

# The token's two parts, without the spaces or line breaks a paste can add.
user="${MAVEN_CENTRAL_USERNAME:?no MAVEN_CENTRAL_USERNAME}"
pass="${MAVEN_CENTRAL_PASSWORD:?no MAVEN_CENTRAL_PASSWORD}"
auth="Authorization: Bearer $(printf '%s:%s' "${user//[[:space:]]/}" "${pass//[[:space:]]/}" | base64 | tr -d '\n')"
if [[ "$mode" == --check ]]; then
  first="${packages[0]#"$group:"}"
  code="$(curl -sS -o /dev/null -w '%{http_code}' --retry 3 -H "$auth" \
    "https://central.sonatype.com/api/v1/publisher/published?namespace=$group&name=${first%%:*}&version=$version" || true)"
  if [[ "$code" != 200 ]]; then
    echo "the Central Portal turned the token down (HTTP $code)" >&2
    exit 1
  fi
  echo "the Portal takes the token; the release uploads ${packages[*]}"
  exit 0
fi
id="$(curl -fsS --retry 3 -X POST -H "$auth" \
  -F "bundle=@$out/bundle.zip;type=application/octet-stream" \
  "https://central.sonatype.com/api/v1/publisher/upload?publishingType=AUTOMATIC&name=wse-client-$version")"
echo "uploaded as deployment $id"
# Validated, then published: the Portal says when Maven Central has it, and
# each state it passes through is printed as it comes.
seen=""
for _ in $(seq 1 180); do
  status="$(curl -fsS --retry 3 -X POST -H "$auth" "https://central.sonatype.com/api/v1/publisher/status?id=$id")"
  state="$(printf '%s' "$status" | python3 -c 'import json, sys; print(json.load(sys.stdin)["deploymentState"])')"
  if [[ "$state" != "$seen" ]]; then
    echo "$(date -u +%H:%M:%S) $state"
    seen="$state"
  fi
  case "$state" in
    PUBLISHED) echo "published: ${packages[*]}"; exit 0 ;;
    FAILED) echo "$status" >&2; exit 1 ;;
  esac
  sleep 20
done
echo "the Portal still says $state after an hour; it publishes on its own once validated" >&2
