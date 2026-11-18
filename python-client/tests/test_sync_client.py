"""Tests for SyncWSEClient."""

from wse_client.sync_client import SyncWSEClient
from wse_client.types import ConnectionQuality, ConnectionState


class TestSyncClientInit:
    def test_send_passes_live_through(self):
        client = SyncWSEClient("ws://localhost:5006/wse")
        seen = {}

        class Async:
            async def send(self, *args, **kwargs):
                seen.update(kwargs)
                return SendResult.SENT

        client._run = lambda make, timeout, default: asyncio.run(make(Async()))
        assert client.send("open_valve", {}, live=True) is SendResult.SENT
        assert seen["live"] is True

    def test_defaults(self):
        client = SyncWSEClient("ws://localhost:5006/wse")
        assert client.state == ConnectionState.DISCONNECTED
        assert client.is_connected is False
        assert client.is_ready is False
        assert client.is_fully_ready is False
        assert client.connection_quality == ConnectionQuality.UNKNOWN
        assert client.queue_size == 0
        assert client.subscribed_topics == set()
        assert client.get_stats() == {}

    def test_handler_registration(self):
        client = SyncWSEClient("ws://localhost:5006/wse")
        received = []

        @client.on("test")
        def handler(event):
            received.append(event)

        assert "test" in client._handlers
        assert len(client._handlers["test"]) == 1

    def test_wildcard_handler(self):
        client = SyncWSEClient("ws://localhost:5006/wse")

        @client.on_any
        def handler(event):
            pass

        assert len(client._wildcard_handlers) == 1

    def test_off_removes_handler(self):
        client = SyncWSEClient("ws://localhost:5006/wse")

        @client.on("test")
        def handler(event):
            pass

        client.off("test", handler)
        assert len(client._handlers["test"]) == 0

    def test_close_alias(self):
        client = SyncWSEClient("ws://localhost:5006/wse")
        # Should not raise even when not connected
        client.disconnect()


# -- Against the fake server (a background thread runs the server loop) -------

import asyncio  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402

import pytest  # noqa: E402

from _fakes import FakeWSEServer  # noqa: E402
from wse_client import (  # noqa: E402
    ReconnectConfig,
    SendResult,
    WSEAuthError,
    WSEConnectionError,
    WSETimeoutError,
)


class ServerThread:
    """Runs a FakeWSEServer on its own event loop thread."""

    def __init__(self, **kwargs) -> None:
        self.loop = asyncio.new_event_loop()
        self.server = FakeWSEServer(**kwargs)
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)

    def __enter__(self) -> FakeWSEServer:
        self.thread.start()
        self.call(self.server.start())
        return self.server

    def __exit__(self, *exc) -> None:
        self.call(self.server.stop())
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5)
        self.loop.close()

    def call(self, coro, timeout: float = 10.0):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)


def wait_until(predicate, timeout: float = 5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.01)
    raise AssertionError("timed out waiting for condition")


FAST = ReconnectConfig(base_delay=0.05, max_delay=0.2, jitter=False)


class TestSyncLifecycle:
    def test_run_forever_raises_when_the_client_ends(self):
        """Run_forever() spun forever and recv() blocked forever after
        the background client stopped; no error handler fired."""
        st = ServerThread()
        with st as srv:
            client = SyncWSEClient(
                srv.url,
                reconnect=ReconnectConfig(base_delay=0.05, max_attempts=0),
            )
            errors: list[Exception] = []

            def slow_handler(error: Exception) -> None:
                # run_forever() raises only once the handler has run.
                time.sleep(0.2)
                errors.append(error)

            client.set_error_handler(slow_handler)
            client.connect(timeout=5)
            assert client.is_connected
            st.call(srv.connections[0].ws.close(1001))
            with pytest.raises(WSEConnectionError, match="Gave up"):
                client.run_forever()
            assert isinstance(client.background_error, WSEConnectionError)
            assert len(errors) == 1
            with pytest.raises(WSEConnectionError):
                client.recv(timeout=1)
            # A new connect() starts over.
            client.connect(timeout=5)
            assert client.is_connected
            client.close()

    def test_a_handler_that_closes_the_client_keeps_the_error(self):
        st = ServerThread()
        with st as srv:
            client = SyncWSEClient(
                srv.url,
                reconnect=ReconnectConfig(base_delay=0.05, max_attempts=0),
            )
            client.set_error_handler(lambda error: client.close())
            client.connect(timeout=5)
            st.call(srv.connections[0].ws.close(1001))
            with pytest.raises(WSEConnectionError, match="Gave up"):
                client.run_forever()

    def test_recv_raises_auth_error(self):
        rejecting = {"on": False}
        st = ServerThread(reject_auth=lambda conn: rejecting["on"])
        with st as srv:
            client = SyncWSEClient(srv.url, token="t", reconnect=FAST)
            client.connect(timeout=5)
            rejecting["on"] = True
            st.call(srv.connections[0].ws.close(1001))
            with pytest.raises(WSEAuthError):
                while True:
                    event = client.recv(timeout=5)
                    assert event.type == "error"  # AUTH_FAILED forwarded first
            client.close()

    def test_connect_auth_failure_raises_auth_error(self):
        st = ServerThread(reject_auth=lambda conn: True)
        with st as srv:
            client = SyncWSEClient(srv.url, token="bad", reconnect=FAST)
            with pytest.raises(WSEAuthError):
                client.connect(timeout=5)
            assert not client.is_connected

    def test_connect_timeout_then_retry_is_not_clobbered(self):
        """The timed-out attempt's thread kept running and, when it
        finished, cleared the fields of the retry's connection."""
        st = ServerThread(upgrade_delay=lambda index: 1.0 if index == 0 else 0.0)
        with st as srv:
            client = SyncWSEClient(srv.url, reconnect=FAST)
            with pytest.raises(WSETimeoutError):
                client.connect(timeout=0.3)
            client.connect(timeout=5)
            time.sleep(1.3)  # the first attempt would have finished by now
            assert client.is_connected
            assert client.send("still_here", {}) is SendResult.SENT
            conn = wait_until(
                lambda: [c for c in srv.connections if c.find("still_here")]
            )
            assert conn
            client.close()

    def test_close_ends_run_forever(self):
        st = ServerThread()
        with st as srv:
            client = SyncWSEClient(srv.url, reconnect=FAST)
            client.connect(timeout=5)
            threading.Timer(0.2, client.close).start()
            client.run_forever()  # returns, does not raise
            assert not client.is_connected

    def test_stats_are_read_on_the_loop(self):
        """Get_stats() from another thread ran the limiter / breaker
        read-modify-write paths concurrently with the loop thread."""
        st = ServerThread()
        with st as srv:
            client = SyncWSEClient(srv.url, reconnect=FAST)
            client.connect(timeout=5)
            calls: list[str] = []
            session = client._session
            original = session.client.get_stats

            def tracking_get_stats():
                calls.append(threading.current_thread().name)
                return original()

            session.client.get_stats = tracking_get_stats
            stats = client.get_stats()
            assert stats["state"] == "connected"
            assert calls == ["wse-client"]
            assert client.subscribed_topics == set()
            client.close()


class TestSyncDelivery:
    def test_recover_or_resync_from_the_applications_thread(self):
        """The delivery through the sync client: the resync event comes in
        the stream, resynced() and commit() are called from the
        application's thread, and the checkpoint reads what was committed."""
        import json

        from wse_client import Continuity, DeliveryConfig

        async def on_message(conn, msg):
            p = msg.get("p", {})
            if msg.get("t") == "subscription_update" and p.get("action") == "subscribe":
                await conn.send_json(
                    {
                        "c": "WSE",
                        "t": "subscription_update",
                        "p": {"success_topics": p["topics"]},
                        "v": 1,
                    }
                )
                # Sent while the application loads its state: held.
                for o in (1, 2):
                    await conn.send_json(
                        {
                            "c": "U",
                            "t": "price",
                            "tp": "prices",
                            "e": "0000000a",
                            "o": o,
                            "p": {"n": o},
                            "v": 1,
                        }
                    )

        st = ServerThread(on_message=on_message, recovery=True)
        with st as srv:
            client = SyncWSEClient(
                srv.url,
                reconnect=FAST,
                topics=["prices"],
                delivery=DeliveryConfig(consumer="board"),
            )
            client.connect(timeout=5)

            def next_of(kind: str):
                while True:
                    event = client.recv(timeout=5)
                    if event.type == kind:
                        return event

            resync = next_of("resync")
            assert client.topic_states["prices"].continuity is Continuity.RESYNCING
            client.resynced(resync.payload["round"])
            ticks = [next_of("price"), next_of("price")]
            assert [t.offset for t in ticks] == [1, 2]
            for tick in ticks:
                client.commit(tick)
            saved = json.loads(client.checkpoint())
            assert saved["topics"] == {"prices": {"0000000a": 2}}
            assert saved["consumer"] == "board"
            client.close()


class TestSyncOutbox:
    def test_the_handlers_hear_what_became_of_a_message(self):
        st = ServerThread()
        with st as srv:
            client = SyncWSEClient(srv.url, reconnect=FAST)
            delivered: list[str] = []
            client.on("delivered")(lambda e: delivered.extend(e.payload["ids"]))
            client.connect(timeout=5)
            sent = client.send("order", {}, id="mine-1")
            assert sent in (SendResult.SENT, SendResult.QUEUED)
            # The outcome reaches the handler; recv() never returns it.
            with pytest.raises(WSETimeoutError):
                client.recv(timeout=0.5)
            assert delivered == ["mine-1"]
            assert client.kept == 0
            client.close()

    def test_every_outcome_reaches_the_handlers_when_the_reader_is_behind(self):
        st = ServerThread()
        with st as srv:
            client = SyncWSEClient(srv.url, reconnect=FAST, queue_size=1)
            delivered: list[str] = []
            client.on("delivered")(lambda e: delivered.extend(e.payload["ids"]))
            client.connect(timeout=5)
            # Reading, and behind: the stream holds one item.
            with pytest.raises(WSETimeoutError):
                while True:
                    client.recv(timeout=0.05)
            client.send("order", {"n": 1}, id="one")
            client.send("order", {"n": 2}, id="two")
            deadline = time.monotonic() + 5
            while sorted(delivered) != ["one", "two"] and time.monotonic() < deadline:
                with pytest.raises(WSETimeoutError):
                    client.recv(timeout=0.05)
            assert sorted(delivered) == ["one", "two"]
            assert client.kept == 0
            client.close()

    def test_outcomes_not_read_yet_wait_for_the_next_connection(self):
        st = ServerThread()
        with st as srv:
            client = SyncWSEClient(srv.url, reconnect=FAST, queue_size=1)
            delivered: list[str] = []
            client.on("delivered")(lambda e: delivered.extend(e.payload["ids"]))
            client.connect(timeout=5)
            with pytest.raises(WSETimeoutError):
                while True:
                    client.recv(timeout=0.05)
            # Two outcomes nobody read, more than the stream holds of events.
            for n in (1, 2):
                client.send("order", {"n": n}, id=f"sent-{n}")
                deadline = time.monotonic() + 5
                while client.kept and time.monotonic() < deadline:
                    time.sleep(0.01)
                assert client.kept == 0
            client.close()
            client.connect(timeout=5)
            assert client.is_connected
            deadline = time.monotonic() + 5
            while (
                sorted(delivered) != ["sent-1", "sent-2"]
                and time.monotonic() < deadline
            ):
                with pytest.raises(WSETimeoutError):
                    client.recv(timeout=0.05)
            assert sorted(delivered) == ["sent-1", "sent-2"]
            client.close()

    def test_a_connect_after_start_returns_once_the_connection_is_up(self):
        st = ServerThread(upgrade_delay=lambda _index: 0.35)
        with st as srv:
            client = SyncWSEClient(srv.url, reconnect=FAST)
            client.start()
            client.connect(timeout=5)
            assert client.is_connected
            client.close()

    def test_a_connect_after_a_drop_returns_once_the_connection_is_up_again(self):
        # The first connection at once, the next after a while.
        st = ServerThread(upgrade_delay=lambda index: 0.0 if index == 0 else 0.35)
        with st as srv:
            client = SyncWSEClient(srv.url, reconnect=FAST)
            client.connect(timeout=5)
            st.call(srv.connections[0].ws.close(1001))
            wait_until(lambda: not client.is_connected)
            client.connect(timeout=5)
            assert client.is_connected
            assert client.is_ready
            client.close()

    def test_a_connect_made_while_one_is_on_its_way_gets_its_outcome(self):
        st = ServerThread(upgrade_delay=lambda _index: 0.25)
        with st as srv:
            client = SyncWSEClient(srv.url, reconnect=FAST)
            first = threading.Thread(target=client.connect, kwargs={"timeout": 5})
            first.start()
            time.sleep(0.05)
            client.connect(timeout=5)
            # Returned once the connection was made, as the first does.
            assert client.is_connected
            first.join(timeout=5)
            assert not first.is_alive()
            client.close()

    def test_a_clear_with_no_connection_tells_the_handlers(self):
        client = SyncWSEClient("ws://localhost:5006/wse")
        cleared: list[dict] = []
        client.on("not_delivered")(lambda e: cleared.append(e.payload))
        outbox = client._client_kwargs["_outbox"]
        outbox.add("a", '{"t":"m","p":{},"id":"a"}', None, time.time())
        assert client.kept == 1
        client.clear()
        with pytest.raises(WSETimeoutError):
            client.recv(timeout=0.2)
        assert cleared == [{"reason": "cleared", "ids": ["a"]}]
        assert client.kept == 0
