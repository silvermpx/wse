"""In-process stand-in for the WSE server, for client lifecycle tests.

It speaks just enough of the protocol: server_ready (optional), server_hello
in answer to client_hello (with the ECDH key exchange when asked), PONG for
pings, scripted closes. It listens on 127.0.0.1 on a free port in
51000-51999. The real-server suite (RustWSEServer) comes separately.
"""

from __future__ import annotations

import asyncio
import base64
import json
import random
from typing import Any, Awaitable, Callable

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

PORTS = range(51000, 52000)
CONTROL_TYPES = frozenset({"client_hello", "ping", "PONG"})


async def until(predicate: Callable[[], Any], timeout: float = 3.0) -> Any:
    """Poll *predicate* until it returns something truthy."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if loop.time() > deadline:
            raise AssertionError("timed out waiting for condition")
        await asyncio.sleep(0.01)


class FakeConn:
    """One client connection as the fake server sees it."""

    def __init__(self, ws: Any, index: int) -> None:
        self.ws = ws
        self.index = index
        self.headers = ws.request.headers
        self.path = ws.request.path
        self.messages: list[dict[str, Any]] = []  # decoded (decrypted) JSON
        self.plain_types: list[str] = []  # "t" of frames sent in plaintext
        self.encrypted_types: list[str] = []  # "t" of frames sent as E:
        self.cipher: Any = None  # server-side SecurityManager once negotiated
        self.client_public_key: str | None = None
        self.closed = asyncio.Event()

    def types(self) -> list[str]:
        return [m.get("t", "") for m in self.messages]

    def find(self, t: str) -> list[dict[str, Any]]:
        return [m for m in self.messages if m.get("t") == t]

    async def send_json(self, obj: dict[str, Any], *, encrypt: bool = True) -> None:
        data = json.dumps(obj)
        if encrypt and self.cipher is not None:
            await self.ws.send(b"E:" + self.cipher.encrypt(data))
        else:
            await self.ws.send(data)


class FakeWSEServer:
    """Scripted WSE server.

    Args:
        send_server_ready: Send server_ready right after the upgrade (the
            Rust-JWT path). False mimics an application that never sends it.
        answer_hello: Answer client_hello with server_hello.
        encryption: Complete the ECDH exchange when the client asks.
        recovery: Announce recovery in server_ready / server_hello.
        reject_auth: Callable(conn) -> bool; True rejects the connection like
            the Rust server does (AUTH_FAILED error + close 4401).
        on_connect: Coroutine run after server_ready for each connection.
        on_message: Coroutine run for each decoded client message.
        hello_delay: Seconds to wait before answering client_hello.
        upgrade_delay: Callable(index) -> seconds to delay the HTTP upgrade.
    """

    def __init__(
        self,
        *,
        send_server_ready: bool = True,
        answer_hello: bool = True,
        encryption: bool = True,
        recovery: bool = False,
        reject_auth: Callable[[FakeConn], bool] | None = None,
        on_connect: Callable[[FakeConn], Awaitable[None]] | None = None,
        on_message: Callable[[FakeConn, dict[str, Any]], Awaitable[None]] | None = None,
        hello_delay: float = 0.0,
        upgrade_delay: Callable[[int], float] | None = None,
    ) -> None:
        self.send_server_ready = send_server_ready
        self.answer_hello = answer_hello
        self.encryption = encryption
        self.recovery = recovery
        self.reject_auth = reject_auth
        self.on_connect = on_connect
        self.on_message = on_message
        self.hello_delay = hello_delay
        self.upgrade_delay = upgrade_delay
        self.connections: list[FakeConn] = []
        self.port: int | None = None
        self._server: Any = None
        self._requests = 0

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/wse"

    async def __aenter__(self) -> FakeWSEServer:
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.stop()

    async def start(self, port: int | None = None) -> None:
        candidates = [port] if port is not None else list(PORTS)
        if port is None:
            random.shuffle(candidates)
        for candidate in candidates:
            try:
                self._server = await serve(
                    self._handle,
                    "127.0.0.1",
                    candidate,
                    process_request=self._process_request,
                    max_size=None,
                )
            except OSError:
                continue
            self.port = candidate
            return
        raise RuntimeError("no free port in 51000-51999")

    async def stop(self) -> None:
        """Stop listening and close every connection with 1001 (going away),
        like the WSE server's stop()."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _process_request(self, connection: Any, request: Any) -> None:
        index = self._requests
        self._requests += 1
        if self.upgrade_delay is not None:
            delay = self.upgrade_delay(index)
            if delay:
                await asyncio.sleep(delay)
        return None

    async def _handle(self, ws: Any) -> None:
        conn = FakeConn(ws, len(self.connections))
        self.connections.append(conn)
        try:
            if self.reject_auth is not None and self.reject_auth(conn):
                await ws.send(
                    json.dumps(
                        {
                            "c": "WSE",
                            "t": "error",
                            "p": {"code": "AUTH_FAILED", "message": "bad token"},
                            "v": 1,
                        }
                    )
                )
                await ws.close(4401, "Authentication failed")
                return
            if self.send_server_ready:
                await ws.send(
                    json.dumps(
                        {
                            "c": "WSE",
                            "t": "server_ready",
                            "seq": 1,
                            "p": {
                                "message": "Connection established",
                                "details": {
                                    "connection_id": f"conn-{conn.index}",
                                    "features": {"recovery": self.recovery},
                                },
                            },
                            "v": 1,
                        }
                    )
                )
            if self.on_connect is not None:
                await self.on_connect(conn)
            async for frame in ws:
                msg = self._decode(conn, frame)
                if msg is None:
                    continue
                conn.messages.append(msg)
                t = msg.get("t")
                if t == "client_hello" and self.answer_hello:
                    await self._hello(conn, msg)
                elif t == "ping":
                    ts = msg.get("p", {}).get("timestamp", 0)
                    await ws.send(
                        json.dumps(
                            {
                                "c": "WSE",
                                "t": "PONG",
                                "p": {"client_timestamp": ts, "server_timestamp": ts},
                                "v": 1,
                            }
                        )
                    )
                if self.on_message is not None:
                    await self.on_message(conn, msg)
        except ConnectionClosed:
            pass
        finally:
            conn.closed.set()

    @staticmethod
    def _decode(conn: FakeConn, frame: str | bytes) -> dict[str, Any] | None:
        if isinstance(frame, bytes) and frame[:2] == b"E:":
            assert conn.cipher is not None, "encrypted frame before the key exchange"
            msg = json.loads(conn.cipher.decrypt(frame[2:]))
            conn.encrypted_types.append(msg.get("t", ""))
            return msg
        try:
            msg = json.loads(frame)
        except ValueError:
            conn.plain_types.append("<non-json>")
            return None
        conn.plain_types.append(msg.get("t", ""))
        return msg

    async def _hello(self, conn: FakeConn, msg: dict[str, Any]) -> None:
        if self.hello_delay:
            await asyncio.sleep(self.hello_delay)
        p = msg.get("p", {})
        features: dict[str, Any] = {
            "compression": True,
            "recovery": self.recovery,
            "encryption": False,
        }
        reply: dict[str, Any] = {
            "c": "WSE",
            "t": "server_hello",
            "p": {
                "server_version": "fake",
                "protocol_version": 1,
                "features": features,
                "limits": {
                    "max_message_size": 1_048_576,
                    "rate_limit_capacity": 100_000,
                    "rate_limit_refill": 10_000,
                },
                "ping_interval": 25_000,
                "connection_id": f"conn-{conn.index}",
            },
            "v": 1,
        }
        client_key = p.get("encryption_public_key")
        if self.encryption and p.get("features", {}).get("encryption") and client_key:
            from wse_client.security import SecurityManager

            server = SecurityManager(legacy=True)
            server_pub = server.generate_keypair()
            server.derive_shared_secret(base64.b64decode(client_key))
            server.stop_key_rotation()
            conn.cipher = server
            conn.client_public_key = client_key
            features["encryption"] = True
            reply["p"]["encryption_public_key"] = base64.b64encode(server_pub).decode()
        await conn.ws.send(json.dumps(reply))
