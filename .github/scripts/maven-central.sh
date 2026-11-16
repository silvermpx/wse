#!/usr/bin/env bash
# The Android library for Maven Central, as the Central Portal takes it: the
# AAR, its POM, a sources jar and a javadoc jar (a placeholder: the docs are
# the README), each signed with the release key and with its checksums, in a
# bundle in Maven's repository layout under OUT/bundle.zip.
#
#   --check   also asks the Portal whether it takes the token (a dry run of a
#             release: nothing is uploaded);
#   --upload  uploads it (published once the Portal validated it) and waits
#             until Maven Central has it; a version already there is left
#             alone.
#
#   .github/scripts/maven-central.sh 3.1.0 dist/wse-client-android-3.1.0.aar out [--check|--upload]
#
# Signing: MAVEN_GPG_PRIVATE_KEY (armored) and MAVEN_GPG_PASSPHRASE; without
# them the bundle is built unsigned (a dry run of a fork). Upload:
# MAVEN_CENTRAL_USERNAME and MAVEN_CENTRAL_PASSWORD, the Portal's user token.
set -euo pipefail
version="${1:?usage: maven-central.sh VERSION AAR OUT [--upload]}"
aar="${2:?usage: maven-central.sh VERSION AAR OUT [--upload]}"
out="${3:?usage: maven-central.sh VERSION AAR OUT [--upload]}"
upload="${4:-}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
group=io.github.silvermpx
artifact=wse-client-android
base="$artifact-$version"
dir="$out/repository/${group//.//}/$artifact/$version"

published="https://repo1.maven.org/maven2/${group//.//}/$artifact/$version/$base.pom"
if [[ "$upload" == --upload ]] \
  && [[ "$(curl -sS -o /dev/null -w '%{http_code}' --retry 3 "$published" || true)" == 200 ]]; then
  echo "$group:$artifact:$version is on Maven Central already (an earlier attempt); nothing to publish."
  exit 0
fi

rm -rf "${out:?}/repository" "${out:?}/bundle.zip"
mkdir -p "$dir"
cp "$aar" "$dir/$base.aar"

cat > "$dir/$base.pom" <<POM
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>$group</groupId>
  <artifactId>$artifact</artifactId>
  <version>$version</version>
  <packaging>aar</packaging>
  <name>WSE client for Android</name>
  <description>The WSE client for Android apps: topics, message recovery, reconnects, end-to-end encryption and signed messages, on the WSE Rust client.</description>
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
    <dependency>
      <groupId>org.jetbrains.kotlin</groupId>
      <artifactId>kotlin-stdlib</artifactId>
      <version>2.4.21</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>org.jetbrains.kotlinx</groupId>
      <artifactId>kotlinx-coroutines-core</artifactId>
      <version>1.11.0</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>net.java.dev.jna</groupId>
      <artifactId>jna</artifactId>
      <version>5.19.1</version>
      <type>aar</type>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>androidx.annotation</groupId>
      <artifactId>annotation</artifactId>
      <version>1.9.1</version>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>androidx.lifecycle</groupId>
      <artifactId>lifecycle-process</artifactId>
      <version>2.11.0</version>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>androidx.startup</groupId>
      <artifactId>startup-runtime</artifactId>
      <version>1.2.0</version>
      <scope>runtime</scope>
    </dependency>
  </dependencies>
</project>
POM

# The Kotlin sources the AAR is built from, and a javadoc placeholder.
work="$(mktemp -d)"
trap 'rm -rf "${work:?}"' EXIT
mkdir -p "$work/sources" "$work/javadoc"
for src in bindings/kotlin/src/main/kotlin bindings/kotlin/android/src/main/kotlin bindings/kotlin/generated/android; do
  (cd "$root/$src" && tar -cf - .) | (cd "$work/sources" && tar -xf -)
done
printf 'The WSE client for Android. Its documentation: https://github.com/silvermpx/wse/tree/main/bindings/kotlin\n' \
  > "$work/javadoc/README.txt"
(cd "$work/sources" && zip -qr -X "$dir/$base-sources.jar" .)
(cd "$work/javadoc" && zip -qr -X "$dir/$base-javadoc.jar" .)

if [[ -n "${MAVEN_GPG_PRIVATE_KEY:-}" ]]; then
  export GNUPGHOME="$work/gnupg"
  mkdir -m 700 "$GNUPGHOME"
  printf '%s\n' "$MAVEN_GPG_PRIVATE_KEY" | gpg --batch --quiet --import
fi
sum() { if command -v "${1}sum" > /dev/null; then "${1}sum" "$2"; else shasum -a "${1#sha}" "$2"; fi | cut -d' ' -f1; }
for file in "$dir"/*; do
  name="$(basename "$file")"
  if [[ -n "${MAVEN_GPG_PRIVATE_KEY:-}" ]]; then
    gpg --batch --quiet --yes --pinentry-mode loopback --passphrase "${MAVEN_GPG_PASSPHRASE:?no passphrase for the key}" \
      --armor --detach-sign --output "$file.asc" "$file"
    gpg --batch --quiet --verify "$file.asc" "$file"
  fi
  md5 -q "$file" > "$file.md5" 2>/dev/null || md5sum "$file" | cut -d' ' -f1 > "$file.md5"
  sum sha1 "$file" > "$file.sha1"
  sum sha256 "$file" > "$file.sha256"
  sum sha512 "$file" > "$file.sha512"
  echo "$name"
done
(cd "$out/repository" && zip -qr -X "../bundle.zip" .)
unzip -l "$out/bundle.zip"
if [[ -z "${MAVEN_GPG_PRIVATE_KEY:-}" ]]; then
  echo "built without signatures: no MAVEN_GPG_PRIVATE_KEY"
  [[ -z "$upload" ]] || { echo "Maven Central takes signed files only" >&2; exit 1; }
fi
[[ -n "$upload" ]] || exit 0

auth="Authorization: Bearer $(printf '%s:%s' "${MAVEN_CENTRAL_USERNAME:?no MAVEN_CENTRAL_USERNAME}" "${MAVEN_CENTRAL_PASSWORD:?no MAVEN_CENTRAL_PASSWORD}" | base64 | tr -d '\n')"
if [[ "$upload" == --check ]]; then
  code="$(curl -sS -o /dev/null -w '%{http_code}' --retry 3 -H "$auth" \
    "https://central.sonatype.com/api/v1/publisher/published?namespace=$group&name=$artifact&version=$version" || true)"
  if [[ "$code" != 200 ]]; then
    echo "the Central Portal turned the token down (HTTP $code)" >&2
    exit 1
  fi
  echo "the Portal takes the token; the release uploads $group:$artifact:$version"
  exit 0
fi
id="$(curl -fsS --retry 3 -X POST -H "$auth" \
  -F "bundle=@$out/bundle.zip;type=application/octet-stream" \
  "https://central.sonatype.com/api/v1/publisher/upload?publishingType=AUTOMATIC&name=$base")"
echo "uploaded as deployment $id"
# Validated, then published: the Portal says when Maven Central has it.
for _ in $(seq 1 180); do
  status="$(curl -fsS --retry 3 -X POST -H "$auth" "https://central.sonatype.com/api/v1/publisher/status?id=$id")"
  state="$(printf '%s' "$status" | python3 -c 'import json, sys; print(json.load(sys.stdin)["deploymentState"])')"
  case "$state" in
    PUBLISHED) echo "$group:$artifact:$version is published"; exit 0 ;;
    FAILED) echo "$status" >&2; exit 1 ;;
  esac
  sleep 20
done
echo "the Portal still says $state after an hour; it publishes on its own once validated" >&2
