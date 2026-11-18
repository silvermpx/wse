"""The examples of the repository, run as their users run them."""

from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import signal
import sys

import pytest

from _fakes import FakeWSEServer

EXAMPLES = pathlib.Path(__file__).resolve().parents[2] / "examples"


def example(name: str):
    spec = importlib.util.spec_from_file_location(name, EXAMPLES / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
async def test_the_client_example_stops_on_a_signal_while_the_server_is_quiet():
    client_python = example("client_python")
    async with FakeWSEServer() as server:
        running = asyncio.create_task(
            client_python.main(server.url, "token", ["prices"])
        )
        while not server.connections:
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.2)
        # Nothing comes from the server: the signal alone ends it.
        os.kill(os.getpid(), signal.SIGINT)
        await asyncio.wait_for(running, timeout=5)
