"""Connection lifecycle against an in-process fake server (ports 51000-51999).

Close codes, failed reconnect attempts, auth rejection, token refresh,
endpoint change, large and malformed frames, offline queue replay.
"""

from __future__ import annotations

import asyncio
import json
import time

import pytest

from _fakes import FakeWSEServer, until
from wse_client import (
    AsyncWSEClient,
    ConnectionState,
    ReconnectConfig,
    SendResult,
    WSEAuthError,
    WSEConnectionError,
)

FAST = ReconnectConfig(base_delay=0.05, max_delay=0.2, jitter=False)


def make_client(url: str, **kwargs) -> AsyncWSEClient:
    kwargs.setdefault("reconnect", FAST)
    client = AsyncWSEClient(url, **kwargs)
    # Many failed attempts in a row would open the breaker for 60 s.
    client._circuit_breaker._failure_threshold = 10_000
    return client


def record_states(client: AsyncWSEClient) -> list[ConnectionState]:
    states: list[ConnectionState] = []
    original = client._connection._on_state_change

    def hook(state: ConnectionState) -> None:
        states.append(state)
        original(state)

    client._connection._on_state_change = hook
    return states


async def push_event(conn, t: str, **payload) -> None:
    await conn.send_json({"c": "U", "t": t, "p": payload, "v": 1})


async def next_event(client: AsyncWSEClient, timeout: float = 3.0):
    """Next event, skipping server "error" events (forwarded to the user too)."""
    while True:
        event = await client.recv(timeout=timeout)
        if event.type != "error":
            return event


class TestServerInitiatedClose:
    """Every server close but an auth rejection is reconnected."""

    @pytest.mark.parametrize("code", [1000, 1001, 1011, 4300])
    async def test_close_code_reconnects_and_stream_continues(self, code):
        async with FakeWSEServer() as srv:
            client = make_client(srv.url)
            states = record_states(client)
            await client.connect()
            await srv.connections[0].ws.close(code, "bye")
            await until(lambda: len(srv.connections) >= 2 and client.is_ready)
            assert client._connection.last_close == (code, "bye")
            await push_event(srv.connections[1], "after_reconnect")
            event = await client.recv(timeout=2.0)
            assert event.type == "after_reconnect"
            assert ConnectionState.RECONNECTING in states
            assert ConnectionState.ERROR not in states
            await client.disconnect()

    async def test_server_stop_and_restart(self):
        async with FakeWSEServer() as srv:
            client = make_client(srv.url)
            await client.connect()
            port = srv.port
            await srv.stop()  # closes with 1001 like WSE stop()
            await until(lambda: client.state == ConnectionState.RECONNECTING)
            assert not client.is_connected  # no stale CONNECTED any more
            await srv.start(port=port)
            await until(lambda: len(srv.connections) == 2 and client.is_ready)
            await push_event(srv.connections[1], "fresh")
            assert (await client.recv(timeout=2.0)).type == "fresh"
            await client.disconnect()


class TestFailedReconnectAttempts:
    async def test_failed_attempts_do_not_end_the_stream(self):
        """A failed reconnect attempt used to set ERROR, end `async for`
        and make recv() raise while the client was still reconnecting."""
        async with FakeWSEServer() as srv:
            client = make_client(srv.url)
            states = record_states(client)
            await client.connect()
            port = srv.port

            received: list[str] = []

            async def consume():
                async for event in client:
                    received.append(event.type)
                    if event.type == "after":
                        return

            consumer = asyncio.create_task(consume())
            await srv.stop()
            await asyncio.sleep(0.6)  # several failed attempts, server down
            assert not consumer.done()
            assert client.state == ConnectionState.RECONNECTING
            await srv.start(port=port)
            await until(lambda: client.is_ready, timeout=5.0)
            await push_event(srv.connections[-1], "after")
            await asyncio.wait_for(consumer, timeout=3.0)
            assert received == ["after"]
            assert ConnectionState.ERROR not in states
            await client.disconnect()

    async def test_max_attempts_ends_with_error(self):
        async with FakeWSEServer() as srv:
            client = make_client(
                srv.url,
                reconnect=ReconnectConfig(
                    base_delay=0.05, max_delay=0.1, max_attempts=2, jitter=False
                ),
            )
            await client.connect()
            await srv.stop()
            with pytest.raises(WSEConnectionError, match="Gave up"):
                await asyncio.wait_for(client.recv(), timeout=5.0)
            assert client.state == ConnectionState.ERROR
            assert isinstance(client.last_error, WSEConnectionError)
            await client.disconnect()


class TestAuth:
    async def test_connect_raises_auth_error_without_waiting(self):
        """Connect() waited 10 s for server_ready, then returned."""
        async with FakeWSEServer(reject_auth=lambda conn: True) as srv:
            client = make_client(srv.url, token="expired")
            start = time.monotonic()
            with pytest.raises(WSEAuthError):
                await client.connect()
            assert time.monotonic() - start < 3.0
            assert client.state == ConnectionState.ERROR
            await asyncio.sleep(0.3)
            assert len(srv.connections) == 1  # nothing reconnects
            await client.disconnect()

    async def test_close_during_handshake_raises_connection_error(self):
        async def on_connect(conn):
            await conn.ws.close(1011, "boom")

        async with FakeWSEServer(send_server_ready=False, on_connect=on_connect) as srv:
            client = make_client(srv.url)
            with pytest.raises(WSEConnectionError):
                await client.connect()
            await asyncio.sleep(0.3)
            assert len(srv.connections) == 1
            await client.disconnect()

    async def test_rejection_on_reconnect_raises_and_keeps_token(self):
        rejecting = {"on": False}
        async with FakeWSEServer(reject_auth=lambda conn: rejecting["on"]) as srv:
            client = make_client(srv.url, token="t1")
            await client.connect()
            rejecting["on"] = True
            await srv.connections[0].ws.close(1001)
            with pytest.raises(WSEAuthError):
                await asyncio.wait_for(next_event(client), timeout=5.0)
            assert client.state == ConnectionState.ERROR
            # The token is no longer wiped on 4401.
            assert client._connection._token == "t1"

            # set_token() + force_reconnect() revive the client.
            rejecting["on"] = False
            client.set_token("t2")
            await client.force_reconnect()
            await until(lambda: client.is_ready, timeout=5.0)
            assert srv.connections[-1].headers["Authorization"] == "Bearer t2"
            await push_event(srv.connections[-1], "back")
            assert (await next_event(client)).type == "back"
            await client.disconnect()

    @pytest.mark.parametrize("async_provider", [False, True])
    async def test_token_provider_refreshes_after_rejection(self, async_provider):
        """A token_provider is asked before every attempt; a 4401 on
        reconnect fetches a fresh token instead of ending the client."""
        accepted = {"Bearer good-1"}
        tokens = iter(["good-1", "expired", "good-2", "good-3"])
        calls: list[str] = []

        def next_token() -> str:
            token = next(tokens)
            calls.append(token)
            return token

        async def async_next_token() -> str:
            return next_token()

        provider = async_next_token if async_provider else next_token
        async with FakeWSEServer(
            reject_auth=lambda conn: conn.headers.get("Authorization") not in accepted
        ) as srv:
            client = make_client(srv.url, token_provider=provider)
            await client.connect()
            accepted.clear()
            accepted.add("Bearer good-2")
            await srv.connections[0].ws.close(1001)
            await until(lambda: client.is_ready and len(srv.connections) == 3, 5.0)
            assert calls == ["good-1", "expired", "good-2"]
            assert client.state == ConnectionState.CONNECTED
            await push_event(srv.connections[-1], "refreshed")
            assert (await next_event(client)).type == "refreshed"
            await client.disconnect()

    async def test_token_provider_gives_up_after_repeated_rejections(self):
        async with FakeWSEServer(
            reject_auth=lambda conn: conn.index > 0  # only the first one passes
        ) as srv:
            client = make_client(srv.url, token_provider=lambda: "always-bad")
            await client.connect()
            await srv.connections[0].ws.close(1001)
            with pytest.raises(WSEAuthError):
                await asyncio.wait_for(next_event(client), timeout=5.0)
            await client.disconnect()


class TestChangeEndpoint:
    async def test_change_endpoint_resubscribes(self):
        """A client that never reconnected sent no subscription to the
        new endpoint (topics only travelled in the URL, which the server
        ignores)."""
        async with FakeWSEServer() as a, FakeWSEServer() as b:
            client = make_client(a.url)
            await client.connect()
            assert await client.subscribe(["prices"])
            await client.change_endpoint(b.url)
            conn = await until(lambda: b.connections and b.connections[0])
            sub = await until(lambda: conn.find("subscription_update"))
            assert sub[0]["p"]["topics"] == ["prices"]
            assert "topics=" not in conn.path
            assert client.subscribed_topics == {"prices"}
            await client.disconnect()

    async def test_reconnect_resubscribes_with_recovery_positions(self):
        async with FakeWSEServer(recovery=True) as srv:
            client = make_client(srv.url)
            await client.connect()
            await client.subscribe(["prices"])
            await srv.connections[0].send_json(
                {"tp": "prices", "e": "0000000a", "o": 7, "c": "U", "t": "p", "p": {}}
            )
            await client.recv(timeout=2.0)
            await srv.connections[0].ws.close(1001)
            conn = await until(lambda: len(srv.connections) == 2 and srv.connections[1])
            sub = await until(lambda: conn.find("subscription_update"))
            p = sub[0]["p"]
            assert p["topics"] == ["prices"]
            assert p["recover"] is True
            assert p["recovery"] == {"prices": {"epoch": "0000000a", "offset": 7}}
            await client.disconnect()


class TestFrames:
    async def test_message_larger_than_1mib_is_delivered(self):
        """The inbound limit was hard-coded to 1 MiB (connection dropped
        with 1009 on a bigger snapshot)."""
        blob = "x" * 1_500_000

        async def on_connect(conn):
            await conn.ws.send(
                json.dumps({"c": "S", "t": "snapshot", "p": {"blob": blob}}).encode()
            )

        async with FakeWSEServer(on_connect=on_connect) as srv:
            client = make_client(srv.url)
            await client.connect()
            event = await client.recv(timeout=3.0)
            assert event.type == "snapshot"
            assert len(event.payload["blob"]) == 1_500_000
            assert len(srv.connections) == 1
            await client.disconnect()

    async def test_bad_frames_do_not_drop_the_connection(self):
        """One malformed frame tore the connection down."""

        async def on_connect(conn):
            await conn.ws.send(b"M:\xc1\xc1")
            await conn.ws.send("not json at all")
            await conn.ws.send(
                json.dumps(
                    {"c": "WSE", "t": "subscription_update", "p": {"recovery": None}}
                )
            )
            await push_event(conn, "good")

        async with FakeWSEServer(on_connect=on_connect) as srv:
            client = make_client(srv.url)
            await client.connect()
            assert (await client.recv(timeout=2.0)).type == "good"
            assert client.is_connected
            assert len(srv.connections) == 1
            await client.disconnect()


class TestHandshakeWithoutServerReady:
    async def test_server_hello_completes_the_handshake(self):
        """An application that never sends server_ready no longer costs a
        10 s wait: client_hello goes out at open and server_hello answers."""
        async with FakeWSEServer(send_server_ready=False, recovery=True) as srv:
            client = make_client(srv.url)
            start = time.monotonic()
            await client.connect()
            assert time.monotonic() - start < 2.0
            assert client.recovery_enabled  # Learned from server_hello
            await client.disconnect()


class TestOfflineQueueReplay:
    async def test_queued_message_is_sent_once_after_reconnect(self):
        async with FakeWSEServer() as srv:
            client = make_client(srv.url, offline_queue=True)
            await client.connect()
            port = srv.port
            await srv.stop()
            await until(lambda: not client.is_connected)
            assert await client.send("order", {"id": 1}) is SendResult.QUEUED
            await srv.start(port=port)
            conn = await until(lambda: len(srv.connections) == 2 and srv.connections[1])
            await until(lambda: conn.find("order"), timeout=3.0)
            await asyncio.sleep(0.2)
            assert len(conn.find("order")) == 1
            assert client._offline_queue.size == 0
            await client.disconnect()


class TestDisconnect:
    async def test_disconnect_ends_iteration_and_closes_socket(self):
        async with FakeWSEServer() as srv:
            client = make_client(srv.url)
            await client.connect()
            await client.disconnect()
            assert client.state == ConnectionState.DISCONNECTED
            await asyncio.wait_for(srv.connections[0].closed.wait(), timeout=2.0)
            with pytest.raises(StopAsyncIteration):
                await client.__anext__()

    async def test_async_handler_awaiting_disconnect_closes_the_socket(self):
        """The teardown cancelled the handler's own task half way."""

        async def on_connect(conn):
            await push_event(conn, "shutdown")

        async with FakeWSEServer(on_connect=on_connect) as srv:
            client = make_client(srv.url)
            done = asyncio.Event()

            @client.on("shutdown")
            async def handler(event):
                await client.disconnect()
                done.set()

            await client.connect()
            await asyncio.wait_for(done.wait(), timeout=3.0)
            await asyncio.wait_for(srv.connections[0].closed.wait(), timeout=2.0)
            assert client.state == ConnectionState.DISCONNECTED
