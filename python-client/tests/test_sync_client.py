"""Tests for SyncWSEClient."""

from wse_client.sync_client import SyncWSEClient
from wse_client.types import ConnectionQuality, ConnectionState


class TestSyncClientInit:
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
            client.set_error_handler(errors.append)
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
