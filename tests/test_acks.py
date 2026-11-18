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


@pytest.mark.asyncio
async def test_a_server_that_queues_nothing_refuses_what_it_did_not_take(server_port):
    """Neither drain mode nor on_message: a numbered message reaches nobody,
    and the client is told so, never that it was taken."""
    srv = RustWSEServer(
        "127.0.0.1",
        server_port,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
    )
    srv.start()
    try:
        ws, answer = await outbox_client(server_port, "ann", {"producer": "p9"})
        assert answer["acked"] == 0
        await send(ws, "order", 1)
        nack = await next_of(ws, "nack")
        assert (nack["p"]["s"], nack["p"]["code"]) == (1, "busy")
        await ws.close()
    finally:
        srv.stop()


@pytest.mark.asyncio
async def test_a_message_the_client_was_told_ran_out_never_reaches_the_application(server_port):
    """The client wrote a message whose bytes are held up on the way, lets
    that connection go, and is back on another one, where it is told the
    message was not taken: it runs out. The held bytes come through on the
    old connection after that, and the application does not take them."""
    from contextlib import suppress

    from wse_client import AsyncWSEClient, ReconnectConfig

    srv = start_server(server_port)
    holding = asyncio.Event()
    release = asyncio.Event()
    held: list[int] = []
    first: list[asyncio.StreamWriter] = []
    links: set[asyncio.Task] = set()

    async def relay(reader, writer):
        """A proxy that, once asked, holds up the next bytes the client's
        first connection sends."""
        links.add(asyncio.current_task())
        is_first = not first
        if is_first:
            first.append(writer)
        up_reader, up_writer = await asyncio.open_connection("127.0.0.1", server_port)

        async def pipe(src, dst, from_client):
            with suppress(ConnectionError, OSError):
                while data := await src.read(65536):
                    if is_first and from_client and holding.is_set() and not held:
                        held.append(len(data))
                        await release.wait()
                    dst.write(data)
                    await dst.drain()
                # One side closed: so does the other, after what it sent.
                if dst.can_write_eof():
                    dst.write_eof()

        down = asyncio.create_task(pipe(up_reader, writer, False))
        try:
            await pipe(reader, up_writer, True)
        finally:
            down.cancel()
            with suppress(asyncio.CancelledError):
                await down
            up_writer.close()
            writer.close()

    proxy = await asyncio.start_server(relay, "127.0.0.1", 0)
    proxy_port = proxy.sockets[0].getsockname()[1]
    client = AsyncWSEClient(
        f"ws://127.0.0.1:{proxy_port}/wse",
        token=make_token(),
        reconnect=ReconnectConfig(base_delay=0.06, max_delay=0.12),
    )
    outcomes: list[tuple[str, str]] = []
    client.on("delivered")(lambda e: outcomes.extend(("delivered", i) for i in e.payload["ids"]))
    client.on("not_delivered")(
        lambda e: outcomes.extend((e.payload["reason"], i) for i in e.payload["ids"])
    )

    async def until(done):
        async with asyncio.timeout(8):
            while not done():
                await asyncio.sleep(0.01)

    try:
        await client.connect()
        await client.send("control", {}, id="control")
        await until(lambda: ("delivered", "control") in outcomes)
        holding.set()
        await client.send("order", {"n": 2}, id="order", ttl=0.14)
        await until(lambda: bool(held))
        await asyncio.sleep(0.22)  # its time runs out, its bytes still held
        first[0].transport.abort()
        await until(lambda: any(i == "order" for _, i in outcomes))
        assert ("expired", "order") in outcomes, outcomes
        release.set()
        await asyncio.sleep(0.5)  # the held bytes reach the server
        took = [e[2].get("id") for e in srv.drain_inbound(64, 0) if e[0] == "msg"]
        assert "order" not in took, took
    finally:
        release.set()
        await client.disconnect()
        proxy.close()
        await proxy.wait_closed()
        for link in list(links):
            link.cancel()
        await asyncio.gather(*links, return_exceptions=True)
        srv.stop()
