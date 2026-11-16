#!/usr/bin/env bash
# The Swift package as Swift Package Manager takes it, in a repository of its
# own (github.com/silvermpx/wse-swift): Package.swift naming this release's
# XCFramework (its URL and checksum), the Swift sources, a README and the
# license, written to OUT.
#
#   --check  builds the package on this Mac against the XCFramework itself,
#            and tries the deploy key when there is one;
#   --push   commits it to the repository and tags the version, with
#            SWIFT_REPO_DEPLOY_KEY (a deploy key that may write); a version
#            tagged there already is left alone.
#
#   .github/scripts/swift-repo.sh 3.1.0 dist/WseFFIC-3.1.0.xcframework.zip out --check
set -euo pipefail
usage="usage: swift-repo.sh VERSION XCFRAMEWORK_ZIP OUT --check|--push"
version="${1:?$usage}"
zip="${2:?$usage}"
out="${3:?$usage}"
mode="${4:?$usage}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
# Absolute: the steps below change directories.
mkdir -p "$out"
out="$(cd "$out" && pwd)"
remote=git@github.com:silvermpx/wse-swift.git
checksum="$(tr -d '[:space:]' < "$zip.checksum")"
url="https://github.com/silvermpx/wse/releases/download/v$version/$(basename "$zip")"

# Package.swift with the XCFramework as `binary` names it.
package() {
  cat <<SWIFT
// swift-tools-version:5.10
// The WSE client for Swift (iOS 15+, macOS 12+), version $version of
// https://github.com/silvermpx/wse; its release writes this repository.
import PackageDescription

let package = Package(
    name: "WseClient",
    platforms: [.iOS(.v15), .macOS(.v12)],
    products: [.library(name: "WseFFI", targets: ["WseFFI"])],
    targets: [
        $1,
        .target(name: "WseFFI", dependencies: ["WseFFIC"]),
    ],
    swiftLanguageVersions: [.v5]
)
SWIFT
}

rm -rf "${out:?}"
mkdir -p "$out/Sources/WseFFI"
cp "$root"/bindings/swift/Sources/WseFFI/*.swift "$out/Sources/WseFFI/"
cp "$root/LICENSE" "$out/LICENSE"
package ".binaryTarget(name: \"WseFFIC\", url: \"$url\", checksum: \"$checksum\")" > "$out/Package.swift"
cat > "$out/README.md" <<MD
# WSE client for Swift

The Swift package of [WSE](https://github.com/silvermpx/wse), the WebSocket
engine for reactive user interfaces: iOS 15+ and macOS 12+. This repository
is written by WSE's releases; the code, the issues and the documentation live
in the main repository, in
[bindings/swift](https://github.com/silvermpx/wse/tree/main/bindings/swift).

\`\`\`swift
.package(url: "https://github.com/silvermpx/wse-swift", from: "$version")
\`\`\`

and the product \`WseFFI\` in the target's dependencies.
MD

# The deploy key, as SSH reads it, when there is one.
key=""
work="$(mktemp -d)"
trap 'rm -rf "${work:?}"' EXIT
if [[ -n "${SWIFT_REPO_DEPLOY_KEY:-}" ]]; then
  key="$work/deploy-key"
  printf '%s\n' "$SWIFT_REPO_DEPLOY_KEY" > "$key"
  chmod 600 "$key"
  export GIT_SSH_COMMAND="ssh -i $key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"
fi

case "$mode" in
  --check)
    # The same package, the XCFramework beside it instead of downloaded.
    local_copy="$work/package"
    cp -R "$out" "$local_copy"
    unzip -q "$zip" -d "$local_copy"
    package '.binaryTarget(name: "WseFFIC", path: "WseFFIC.xcframework")' > "$local_copy/Package.swift"
    (cd "$local_copy" && swift build)
    if [[ -n "$key" ]]; then
      git ls-remote "$remote" > /dev/null
      echo "the deploy key reaches $remote"
    fi
    echo "the Swift package of $version builds; its release commits it to $remote"
    ;;
  --push)
    [[ -n "$key" ]] || { echo "no SWIFT_REPO_DEPLOY_KEY" >&2; exit 1; }
    repo="$work/repo"
    git clone --quiet "$remote" "$repo"
    if git -C "$repo" rev-parse -q --verify "refs/tags/$version" > /dev/null; then
      echo "$remote has $version already (an earlier attempt); nothing to push."
      exit 0
    fi
    find "${repo:?}" -mindepth 1 -maxdepth 1 ! -name .git -exec rm -rf {} +
    cp -R "$out"/. "$repo"/
    git -C "$repo" add -A
    git -C "$repo" -c user.name="github-actions[bot]" \
      -c user.email="41898282+github-actions[bot]@users.noreply.github.com" \
      commit --quiet -m "WSE $version"
    git -C "$repo" tag "$version"
    git -C "$repo" push --quiet origin HEAD:main "refs/tags/$version"
    echo "$remote: $version"
    ;;
  *)
    echo "$usage" >&2
    exit 2
    ;;
esac
