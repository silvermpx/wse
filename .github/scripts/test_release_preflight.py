#!/usr/bin/env python3
"""The release checks' own cases, the failing ones above all: a check that
cannot fail checks nothing.

    python3 .github/scripts/test_release_preflight.py

Standard library only, as release_preflight.py.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_preflight as rp  # noqa: E402

VERSION = "3.2.0"


# The release's wse-server wheels as release.yml's matrix builds them,
# written out here on their own: the check must want each of them, so a
# platform dropped from its table fails here too. Each platform's abi3 wheel,
# and its free-threaded ones where there is a CPython of them.
WHEEL_PLATFORMS = {
    "manylinux_2_28_x86_64": True,
    "manylinux_2_28_aarch64": True,
    "musllinux_1_2_x86_64": False,
    "musllinux_1_2_aarch64": False,
    "macosx_10_12_x86_64": True,
    "macosx_11_0_arm64": True,
    "win_amd64": True,
    "win_arm64": True,
}
FREE_THREADED = ("cp314t", "cp315t")


# Where release.yml's publishing jobs take each file from: the artifact
# (the directory the gate downloads it into) that holds it.
MOBILE_HOMES = {
    "Swift XCFramework": "swift-package",
    "Swift XCFramework checksum": "swift-package",
    "Android library": "android-package",
    "Dart package": "dart-package",
}


def release_files() -> list[str]:
    """Every file a release of VERSION holds, under its artifact."""
    files = [
        f"{MOBILE_HOMES.get(kind, 'dart-binaries-' + kind.rsplit(' ', 1)[-1])}/{name}"
        for name, kind in rp._mobile_files(VERSION).items()
    ]
    files.append(f"jvm-package/wse-client-jvm-{VERSION}.jar")
    for platform, free_threaded in WHEEL_PLATFORMS.items():
        files.append(f"wheels-{platform}/wse_server-{VERSION}-cp312-abi3-{platform}.whl")
        if free_threaded:
            for tag in FREE_THREADED:
                files.append(
                    f"wheels-{platform}/wse_server-{VERSION}-{tag[:-1]}-{tag}-{platform}.whl"
                )
    files += [
        f"sdist/wse_server-{VERSION}.tar.gz",
        f"python-client-dist/wse_client-{VERSION}-py3-none-any.whl",
        f"python-client-dist/wse_client-{VERSION}.tar.gz",
        f"npm-package/wse-client-{VERSION}.tgz",
        # What Maven Central gets beside the JAR, in an artifact of its own.
        f"jvm-maven-repository/wse-client-jvm/{VERSION}/wse-client-jvm-{VERSION}.pom",
    ]
    return files


def check_artifacts(names: list[str]) -> tuple[int, str]:
    """The artifact check over a download of the run's artifacts holding
    ``names`` (each under its artifact): its exit code and what it printed."""
    with tempfile.TemporaryDirectory() as directory:
        for name in names:
            path = Path(directory) / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = rp.cmd_artifacts(
                argparse.Namespace(version=VERSION, npm_version=VERSION, dirs=[directory])
            )
    return code, out.getvalue()


class Artifacts(unittest.TestCase):
    def test_a_complete_release_passes(self) -> None:
        code, out = check_artifacts(release_files())
        self.assertEqual(code, 0, out)

    def test_a_platform_without_its_wheel_is_no_release(self) -> None:
        names = [n for n in release_files() if not n.endswith("-abi3-win_arm64.whl")]
        code, out = check_artifacts(names)
        self.assertEqual(code, 1)
        self.assertIn("no wse-server wheel for Windows arm64 abi3", out)

    def test_a_missing_free_threaded_wheel_is_no_release(self) -> None:
        names = [n for n in release_files() if not n.endswith("-cp315t-macosx_11_0_arm64.whl")]
        code, out = check_artifacts(names)
        self.assertEqual(code, 1)
        self.assertIn("no wse-server wheel for macOS arm64 cp315t", out)

    def test_one_wheel_for_all_platforms_is_no_release(self) -> None:
        names = [n for n in release_files() if "/wse_server-" not in n or "macosx_11_0_arm64" in n]
        code, out = check_artifacts(names)
        self.assertEqual(code, 1)
        self.assertIn("Linux x86_64 (glibc) abi3", out)

    def test_a_release_without_the_jvm_library_fails(self) -> None:
        names = [n for n in release_files() if not n.endswith(f"wse-client-jvm-{VERSION}.jar")]
        code, out = check_artifacts(names)
        self.assertEqual(code, 1)
        self.assertIn("no JVM library", out)

    def test_a_jvm_library_only_beside_maven_centrals_files_is_no_release(self) -> None:
        # The Maven job takes the JAR from jvm-package: a copy elsewhere is not it.
        names = [n for n in release_files() if not n.startswith("jvm-package/")]
        names.append(f"jvm-maven-repository/wse-client-jvm/{VERSION}/wse-client-jvm-{VERSION}.jar")
        code, out = check_artifacts(names)
        self.assertEqual(code, 1)
        self.assertIn("no JVM library", out)

    def test_a_file_outside_its_artifact_does_not_count(self) -> None:
        names = [n for n in release_files() if not n.startswith("npm-package/")]
        names.append(f"sdist/wse-client-{VERSION}.tgz")
        code, out = check_artifacts(names)
        self.assertEqual(code, 1)
        self.assertIn("no npm tarball", out)

    def test_an_abi3_wheel_for_a_newer_python_than_the_package_needs_is_no_release(self) -> None:
        # requires-python is >=3.12: an abi3 wheel of 3.13 leaves 3.12 out.
        names = [
            n.replace("-cp312-abi3-macosx_11_0_arm64", "-cp313-abi3-macosx_11_0_arm64")
            for n in release_files()
        ]
        code, out = check_artifacts(names)
        self.assertEqual(code, 1)
        self.assertIn("no wse-server wheel for macOS arm64 abi3", out)

    def test_a_release_without_a_dart_library_fails(self) -> None:
        gone = f"wse_dart-{VERSION}-aarch64-linux-android.so"
        names = [n for n in release_files() if not n.endswith(gone)]
        code, out = check_artifacts(names)
        self.assertEqual(code, 1)
        self.assertIn("no Dart library aarch64-linux-android", out)

    def test_a_file_of_another_version_fails(self) -> None:
        code, out = check_artifacts(
            [*release_files(), "python-client-dist/wse_client-3.1.0.tar.gz"]
        )
        self.assertEqual(code, 1)
        self.assertIn("does not carry the release version", out)


class Versions(unittest.TestCase):
    """The tag against the files' versions, these set to a prerelease when
    asked (in memory: no file is changed)."""

    def setUp(self) -> None:
        self.sources = rp.collect()
        self.collect = rp.collect
        self.addCleanup(setattr, rp, "collect", self.collect)
        handle, self.outputs = tempfile.mkstemp()
        os.close(handle)
        self.addCleanup(os.unlink, self.outputs)
        os.environ["GITHUB_OUTPUT"] = self.outputs
        self.addCleanup(os.environ.pop, "GITHUB_OUTPUT", None)

    def run_versions(self, tag: str | None, prerelease: bool) -> tuple[int, dict[str, str], str]:
        if prerelease:
            sources = [
                dataclasses.replace(s, value="3.3.0-rc.1" if s.semver else "3.3.0rc1")
                if s.value is not None
                else s
                for s in self.sources
            ]
            rp.collect = lambda: sources
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = rp.cmd_versions(argparse.Namespace(tag=tag))
        lines = Path(self.outputs).read_text().splitlines()
        return code, dict(line.split("=", 1) for line in lines if line), out.getvalue()

    def test_the_release_is_tagged_as_its_packages_spell_the_version(self) -> None:
        code, outputs, out = self.run_versions("v3.3.0-rc.1", prerelease=True)
        self.assertEqual(code, 0, out)
        self.assertEqual(outputs["tag"], "v3.3.0-rc.1")
        self.assertEqual(outputs["prerelease"], "true")

    def test_a_tag_spelled_otherwise_is_refused(self) -> None:
        code, outputs, out = self.run_versions("v3.3.0rc1", prerelease=True)
        self.assertEqual(code, 1)
        self.assertIn("Tag v3.3.0-rc.1 instead of v3.3.0rc1", out)
        self.assertEqual(outputs, {})

    def test_a_tag_of_another_version_is_refused(self) -> None:
        code, outputs, _ = self.run_versions("v9.9.9", prerelease=False)
        self.assertEqual(code, 1)
        self.assertEqual(outputs, {})

    def test_a_run_without_a_tag_names_the_tag_to_come(self) -> None:
        own = next(s.value for s in self.sources if s.path == "package.json")
        code, outputs, out = self.run_versions(None, prerelease=False)
        self.assertEqual(code, 0, out)
        self.assertEqual(outputs["tag"], f"v{own}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
