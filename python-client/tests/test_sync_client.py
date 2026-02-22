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
