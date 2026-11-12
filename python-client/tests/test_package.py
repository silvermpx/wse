"""Package metadata: version and the PEP 561 marker."""

from __future__ import annotations

import importlib.metadata
import importlib.resources
import json
import tomllib
from pathlib import Path

import pytest

import wse_client
from wse_client.constants import CLIENT_VERSION

ROOT = Path(__file__).resolve().parent.parent


def test_client_version_is_the_package_version() -> None:
    """The handshake said 2.0.7 for every 2.x release."""
    assert CLIENT_VERSION == wse_client.__version__


def test_installed_metadata_matches() -> None:
    try:
        installed = importlib.metadata.version("wse-client")
    except importlib.metadata.PackageNotFoundError:
        pytest.skip("wse-client is not installed")
    assert installed == wse_client.__version__


def test_pyproject_reads_the_version_from_the_package() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert "version" in pyproject["project"]["dynamic"]
    assert "version" not in pyproject["project"]
    assert pyproject["tool"]["hatch"]["version"]["path"] == "wse_client/_version.py"


def test_client_hello_carries_the_package_version() -> None:
    import asyncio

    from wse_client.connection import ConnectionManager

    sent: list[str] = []

    class WS:
        async def send(self, data: str) -> None:
            sent.append(data)

    async def run() -> None:
        conn = ConnectionManager("ws://x/wse")
        ws = WS()
        conn._ws = ws
        await conn._send_client_hello(ws)

    asyncio.run(run())
    assert json.loads(sent[0])["p"]["client_version"] == wse_client.__version__


def test_py_typed_marker_ships() -> None:
    """'Typing :: Typed' without py.typed made type checkers ignore
    the annotations."""
    assert importlib.resources.files("wse_client").joinpath("py.typed").is_file()
