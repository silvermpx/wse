"""The clients' outboxes on the Python server: a numbered message reaches
drain_inbound once with its number, the client hears an ack, a repeat is
confirmed again and not queued, and what the server cannot tell is marked."""

import asyncio
import json

import pytest
import websockets

from tests.conftest import JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET, make_token
from wse_server._wse_accel import RustWSEServer


def start_server(port: int) -> RustWSEServer:
    srv = RustWSEServer(
        "127.0.0.1",
        port,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
    )
    srv.enable_drain_mode()
    srv.start()
    return srv


async def next_of(ws, kind: str) -> dict:
    async with asyncio.timeout(5):
        while True:
            message = json.loads(await ws.recv())
            if message.get("t") == kind:
                return message


async def outbox_client(port: int, user: str, outbox: dict):
    """A client of `user` with the outbox `outbox`, and the server's answer."""
    ws = await websockets.connect(
        f"ws://127.0.0.1:{port}/wse",
        additional_headers={"Authorization": f"Bearer {make_token(user)}"},
    )
    hello = {"t": "client_hello", "p": {"protocol_version": 2, "outbox": outbox}}
    await ws.send(json.dumps(hello))
    return ws, (await next_of(ws, "outbox"))["p"]


async def send(ws, kind: str, s: int) -> None:
    await ws.send(json.dumps({"t": kind, "p": {}, "id": f"m{s}", "s": s}))


async def taken(srv: RustWSEServer, count: int) -> list[dict]:
    """The next `count` messages drain_inbound hands out."""
    out: list[dict] = []
    async with asyncio.timeout(5):
        while len(out) < count:
            out += [e[2] for e in srv.drain_inbound(64, 0) if e[0] == "msg"]
            if len(out) < count:
                await asyncio.sleep(0.02)
    return out


async def acked(ws, s: int) -> None:
    while (await next_of(ws, "ack"))["p"]["s"] < s:
        pass


@pytest.mark.asyncio
async def test_a_message_is_queued_once_and_confirmed(server_port):
    srv = start_server(server_port)
    try:
        ws, answer = await outbox_client(server_port, "ann", {"producer": "p1"})
        assert answer["acked"] == 0
        await send(ws, "a", 1)
        await send(ws, "b", 2)
        got = await taken(srv, 2)
        assert [(m["t"], m["s"]) for m in got] == [("a", 1), ("b", 2)]
        assert all("possible_repeat" not in m for m in got)
        await acked(ws, 2)
        # A repeat: confirmed again, not queued.
        await send(ws, "b", 2)
        await acked(ws, 2)
        await asyncio.sleep(0.2)
        assert [e for e in srv.drain_inbound(64, 0) if e[0] == "msg"] == []
        await ws.close()
    finally:
        srv.stop()


@pytest.mark.asyncio
async def test_what_the_server_cannot_tell_is_marked(server_port):
    srv = start_server(server_port)
    try:
        outbox = {"producer": "p2", "node": "gone", "written": 1}
        ws, answer = await outbox_client(server_port, "ann", outbox)
        assert answer["acked"] == 0
        await send(ws, "m", 1)
        await send(ws, "m", 2)
        first, second = await taken(srv, 2)
        assert first["possible_repeat"] is True
        assert "possible_repeat" not in second
        await ws.close()
    finally:
        srv.stop()
