#!/usr/bin/env python3
"""Release checks for wse-server (PyPI), wse-client (PyPI) and wse-client (npm).

Subcommands:

  versions [--tag TAG]
      Every file that states the release version must agree. With a tag
      (v3.0.0, v3.0.0rc1 or v3.0.0-rc.1) they must equal the tag; without one
      they must equal wse-server's own version. Writes ``version`` (PEP 440),
      ``npm-version`` and ``prerelease`` to $GITHUB_OUTPUT when it is set.

  cargo-config
      No committed cargo config may tune the build to one CPU (target-cpu or
      target-feature in rustflags): the sdist ships rust/.cargo/config.toml and
      maturin applies it to every wheel it builds.

  artifacts --version V --npm-version NV DIR [DIR ...]
      Every built file carries the release version and every package is there.

Standard library only (Python 3.11+), so it runs on any runner Python.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]

# X.Y.Z with an optional pre-release in PEP 440 (3.0.0rc1) or SemVer
# (3.0.0-rc.1) spelling. Post, dev and local versions cannot be published to
# both registries under one name, so they are refused.
_VERSION_RE = re.compile(
    r"^(?P<release>\d+\.\d+\.\d+)"
    r"(?:[-_.]?(?P<pre>a|alpha|b|beta|c|rc|pre|preview)[-_.]?(?P<num>\d+))?$",
    re.IGNORECASE,
)
_PRE = {
    "a": "a",
    "alpha": "a",
    "b": "b",
    "beta": "b",
    "c": "rc",
    "rc": "rc",
    "pre": "rc",
    "preview": "rc",
}
# What npm and Cargo accept and this script can map back to PEP 440.
_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(?:-(?:a|alpha|b|beta|rc)\.?\d+)?$")


class CheckError(Exception):
    """A version string this pipeline cannot publish."""


def normalize(raw: str) -> str:
    """Return the PEP 440 form of a release version, e.g. 3.0.0-rc.1 -> 3.0.0rc1."""
    m = _VERSION_RE.match(raw.strip())
    if not m:
        raise CheckError(
            f"{raw!r} is not a version this pipeline publishes "
            "(X.Y.Z, optionally followed by aN, bN or rcN)"
        )
    out = m.group("release")
    if m.group("pre"):
        out += _PRE[m.group("pre").lower()] + m.group("num")
    return out


def is_prerelease(version: str) -> bool:
    m = _VERSION_RE.match(version)
    return bool(m and m.group("pre"))


def gh_error(path: str, line: int | None, message: str) -> None:
    where = f"file={path}" + (f",line={line}" if line else "")
    print(f"::error {where}::{message}")


def line_of(path: Path, pattern: str) -> int | None:
    """1-based number of the first line matching ``pattern``, for annotations."""
    rx = re.compile(pattern)
    try:
        for n, text in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if rx.search(text):
                return n
    except OSError:
        return None
    return None


@dataclass
class Source:
    label: str
    path: str
    value: str | None  # None: not stated literally in this file
    line: int | None = None
    note: str = ""
    semver: bool = False  # npm and Cargo need SemVer spelling


def _toml(rel: str) -> dict[str, Any]:
    with open(ROOT / rel, "rb") as fh:
        return tomllib.load(fh)


def _json(rel: str) -> dict[str, Any]:
    with open(ROOT / rel, encoding="utf-8") as fh:
        data: dict[str, Any] = json.load(fh)
        return data


def _project_version(rel: str, label: str, dynamic_note: str) -> Source:
    project = _toml(rel).get("project", {})
    if "version" in project:
        return Source(label, rel, str(project["version"]), line_of(ROOT / rel, r"^version\s*="))
    if "version" in project.get("dynamic", []):
        return Source(label, rel, None, note=dynamic_note)
    raise CheckError(f"{rel}: [project] has neither a version nor a dynamic version")


def _literal(rel: str, name: str, label: str, required: bool) -> Source | None:
    """A module-level ``name = "x.y.z"`` (Python) or ``export const name = '...'`` (TS).

    An assignment that is not a string literal (derived from another constant
    or from package metadata) is reported as derived and not compared: it is
    correct by construction as long as its source is.
    """
    path = ROOT / rel
    if not path.exists():
        if required:
            raise CheckError(f"{rel} is missing")
        return None
    assign = re.compile(
        rf"^(?:export\s+const\s+)?{re.escape(name)}\s*(?::\s*[\w.\[\]| ]+)?=\s*(?P<rhs>.+?);?\s*(?:#.*|//.*)?$"
    )
    literal = re.compile(r"""^(['"])(?P<value>[^'"]*)\1$""")
    lines = path.read_text(encoding="utf-8").splitlines()
    for n, text in enumerate(lines, 1):
        m = assign.match(text)  # column 0 only: module level, not a fallback branch
        if not m:
            continue
        lit = literal.match(m.group("rhs").strip())
        if lit:
            return Source(label, rel, lit.group("value"), n)
        return Source(label, rel, None, n, note="derived, not a literal")
    if required:
        raise CheckError(f"{rel}: no module-level {name} = '...'")
    indented = re.compile(rf"^\s+{re.escape(name)}\s*=")
    for n, text in enumerate(lines, 1):
        if indented.match(text):
            return Source(label, rel, None, n, note="set at run time, not a literal")
    return None


def collect() -> list[Source]:
    sources: list[Source] = []

    sources.append(
        _project_version(
            "pyproject.toml",
            "wse-server version",
            "dynamic: maturin takes it from rust/Cargo.toml",
        )
    )

    cargo = _toml("rust/Cargo.toml")["package"]
    crate = cargo["name"]
    sources.append(
        Source(
            "Rust crate version",
            "rust/Cargo.toml",
            str(cargo["version"]),
            line_of(ROOT / "rust/Cargo.toml", r"^version\s*="),
            semver=True,
        )
    )
    locked = [p for p in _toml("rust/Cargo.lock").get("package", []) if p.get("name") == crate]
    if len(locked) != 1:
        raise CheckError(
            f"rust/Cargo.lock: expected one [[package]] named {crate}, found {len(locked)}"
        )
    sources.append(
        Source(
            "Rust crate version in Cargo.lock",
            "rust/Cargo.lock",
            str(locked[0]["version"]),
            line_of(ROOT / "rust/Cargo.lock", rf'^name = "{re.escape(crate)}"'),
            semver=True,
        )
    )

    sources.append(
        _project_version(
            "python-client/pyproject.toml",
            "wse-client (PyPI) version",
            "dynamic: hatchling reads wse_client/_version.py",
        )
    )
    client_version = _literal(
        "python-client/wse_client/_version.py",
        "__version__",
        "wse_client.__version__",
        required=True,
    )
    assert client_version is not None  # required=True raises instead
    sources.append(client_version)

    package = _json("package.json")
    sources.append(
        Source(
            "wse-client (npm) version",
            "package.json",
            str(package["version"]),
            line_of(ROOT / "package.json", r'^\s*"version"\s*:'),
            semver=True,
        )
    )
    lock = _json("package-lock.json")
    if "version" in lock:
        sources.append(
            Source(
                "package-lock.json version",
                "package-lock.json",
                str(lock["version"]),
                line_of(ROOT / "package-lock.json", r'^\s*"version"\s*:'),
                semver=True,
            )
        )
    root_pkg = lock.get("packages", {}).get("", {})
    if "version" in root_pkg:
        sources.append(
            Source(
                'package-lock.json packages[""].version',
                "package-lock.json",
                str(root_pkg["version"]),
                semver=True,
            )
        )

    # Version constants in code: compared when they are literals.
    for rel, name, label in (
        ("wse_server/__init__.py", "__version__", "wse_server.__version__"),
        ("python-client/wse_client/constants.py", "CLIENT_VERSION", "wse_client CLIENT_VERSION"),
        ("ts-client/constants.ts", "WS_CLIENT_VERSION", "TS WS_CLIENT_VERSION"),
        ("ts-client/index.ts", "WSE_VERSION", "TS WSE_VERSION"),
    ):
        found = _literal(rel, name, label, required=False)
        if found is not None:
            sources.append(found)
    return sources


def write_outputs(values: dict[str, str]) -> None:
    out = os.environ.get("GITHUB_OUTPUT")
    if not out:
        return
    with open(out, "a", encoding="utf-8") as fh:
        for key, value in values.items():
            fh.write(f"{key}={value}\n")


def cmd_versions(args: argparse.Namespace) -> int:
    sources = collect()
    errors = 0

    if args.tag:
        tag = args.tag.removeprefix("refs/tags/")
        if not tag.startswith("v"):
            gh_error(
                ".github/workflows/release.yml", None, f"release tags look like v3.0.0, got {tag!r}"
            )
            return 1
        expected = normalize(tag[1:])
        origin = f"tag {tag}"
    else:
        # wse-server's own version: pyproject.toml, or Cargo.toml when dynamic.
        own = next(
            s
            for s in sources
            if s.value is not None and s.label in ("wse-server version", "Rust crate version")
        )
        assert own.value is not None
        expected = normalize(own.value)
        origin = f"{own.path} (no tag: checking that every file agrees)"

    print(f"Release version {expected}, from {origin}\n")
    width = max(len(s.path) for s in sources)
    for s in sources:
        if s.value is None:
            status, shown = "skip", "-"
        else:
            shown = s.value
            try:
                ok = normalize(s.value) == expected
                if ok and s.semver and not _SEMVER_RE.match(s.value):
                    ok = False
                    s.note = "npm and Cargo need SemVer spelling, e.g. 3.0.0-rc.1"
            except CheckError as exc:
                ok = False
                s.note = str(exc)
            status = "ok" if ok else "MISMATCH"
            if not ok:
                errors += 1
                gh_error(
                    s.path,
                    s.line,
                    f"{s.label} is {s.value!r}, the release is {expected}. {s.note}".strip(),
                )
        note = f"  ({s.note})" if s.note else ""
        print(f"  {status:<8}  {shown:<14}  {s.path:<{width}}  {s.label}{note}")

    if errors:
        print(f"\n{errors} version(s) disagree with {expected}. Bump them together before tagging.")
        return 1

    npm_version = next(s.value for s in sources if s.path == "package.json")
    assert npm_version is not None
    write_outputs(
        {
            "version": expected,
            "npm-version": npm_version,
            "prerelease": "true" if is_prerelease(expected) else "false",
        }
    )
    print(f"\nAll versions agree: {expected} (npm {npm_version}).")
    return 0


_CPU_FLAGS = ("target-cpu", "target-feature")


def _rustflags(table: dict[str, Any]) -> list[str]:
    flags = table.get("rustflags", [])
    return flags.split() if isinstance(flags, str) else [str(f) for f in flags]


def cmd_cargo_config(_args: argparse.Namespace) -> int:
    errors = 0
    checked = []
    for rel in (
        ".cargo/config.toml",
        ".cargo/config",
        "rust/.cargo/config.toml",
        "rust/.cargo/config",
    ):
        path = ROOT / rel
        if not path.exists():
            continue
        checked.append(rel)
        data = tomllib.loads(path.read_text(encoding="utf-8"))
        tables = {"build": (data.get("build", {}), r"^\[build\]")}
        tables.update(
            {
                f"target.{k}": (v, rf"^\[target\.['\"]?{re.escape(k)}['\"]?\]")
                for k, v in data.get("target", {}).items()
                if isinstance(v, dict)
            }
        )
        for name, (table, header) in tables.items():
            bad = [f for f in _rustflags(table) if any(c in f for c in _CPU_FLAGS)]
            if bad:
                errors += 1
                gh_error(
                    rel,
                    line_of(path, header),
                    f"[{name}] rustflags {bad} tune the build to one CPU. The sdist ships this file and "
                    "maturin applies it to the wheels; keep CPU tuning a local opt-in "
                    '(RUSTFLAGS="-C target-cpu=native").',
                )
    print("Checked: " + (", ".join(checked) or "no committed cargo config"))
    return 1 if errors else 0


def _artifact_version(name: str) -> tuple[str, str] | None:
    """(package, version) from a wheel, sdist or npm tarball file name."""
    if name.endswith(".whl"):
        dist, version = name.split("-")[:2]
        return dist, version
    if name.endswith(".tar.gz"):
        dist, _, version = name[: -len(".tar.gz")].rpartition("-")
        return dist, version
    if name.endswith(".tgz") and name.startswith("wse-client-"):
        return "npm:wse-client", name[len("wse-client-") : -len(".tgz")]
    return None


def cmd_artifacts(args: argparse.Namespace) -> int:
    expected = normalize(args.version)
    kinds = {
        ("wse_server", ".whl"): "wse-server wheels",
        ("wse_server", ".tar.gz"): "wse-server sdist",
        ("wse_client", ".whl"): "wse-client wheel",
        ("wse_client", ".tar.gz"): "wse-client sdist",
        ("npm:wse-client", ".tgz"): "npm tarball",
    }
    counts = dict.fromkeys(kinds.values(), 0)
    errors = 0
    files = sorted(p for d in args.dirs for p in Path(d).rglob("*") if p.is_file())
    for path in files:
        parsed = _artifact_version(path.name)
        if parsed is None:
            print(f"  ignored    {path}")
            continue
        dist, version = parsed
        suffix = ".tar.gz" if path.name.endswith(".tar.gz") else path.suffix
        kind = kinds.get((dist, suffix))
        if kind is None:
            errors += 1
            gh_error(str(path), None, f"unexpected release file {path.name}")
            continue
        counts[kind] += 1
        if dist.startswith("npm:"):
            ok = version == args.npm_version
        else:
            try:
                ok = normalize(version) == expected
            except CheckError:
                ok = False
        print(f"  {'ok' if ok else 'MISMATCH':<9}  {path}")
        if not ok:
            errors += 1
            gh_error(str(path), None, f"{path.name} does not carry the release version {expected}")
    for kind, count in counts.items():
        print(f"{count:4d}  {kind}")
        if count == 0:
            errors += 1
            gh_error(
                ".github/workflows/release.yml", None, f"no {kind} among the release artifacts"
            )
    return 1 if errors else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("versions", help="check that every version agrees (with the tag, if given)")
    p.add_argument("--tag", help="the release tag, e.g. v3.0.0 or refs/tags/v3.0.0")
    p.set_defaults(func=cmd_versions)
    p = sub.add_parser(
        "cargo-config", help="refuse CPU-specific rustflags in committed cargo configs"
    )
    p.set_defaults(func=cmd_cargo_config)
    p = sub.add_parser("artifacts", help="check the version of every built release file")
    p.add_argument("--version", required=True, help="the release version (PEP 440)")
    p.add_argument("--npm-version", required=True, help="the npm version (SemVer)")
    p.add_argument("dirs", nargs="+", help="directories holding the downloaded artifacts")
    p.set_defaults(func=cmd_artifacts)
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except CheckError as exc:
        print(f"::error::{exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
