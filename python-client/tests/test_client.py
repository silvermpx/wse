"""Tests for AsyncWSEClient (unit tests with mocked WebSocket)."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from wse_client.client import AsyncWSEClient, _INTERNAL_ONLY_EVENTS
from wse_client.types import (
    ConnectionQuality,
    ConnectionState,
    MessagePriority,
    WSEEvent,
)


@pytest.fixture
def client():
    """Create a client with mocked connection manager."""
    c = AsyncWSEClient("ws://localhost:5006/wse", token="test-jwt")
    # Mock the connection manager to avoid real WebSocket
    c._connection = MagicMock()
    c._connection.is_connected = True
    c._connection.is_ready = True
    c._connection._client_hello_sent = True
    c._connection.state = ConnectionState.CONNECTED
    c._connection.connection_id = "test-conn-id"
    c._connection.send = AsyncMock(return_value=True)
    c._connection.disconnect = AsyncMock()
    c._connection.force_reconnect = AsyncMock()
    c._connected = True
    return c


class TestProperties:
    def test_is_connected(self, client):
        assert client.is_connected is True

    def test_is_ready(self, client):
        assert client.is_ready is True

    def test_is_fully_ready(self, client):
        assert client.is_fully_ready is True

    def test_is_fully_ready_false_when_not_hello(self, client):
        client._connection._client_hello_sent = False
        assert client.is_fully_ready is False

    def test_state(self, client):
        assert client.state == ConnectionState.CONNECTED

    def test_connection_quality_unknown_initially(self, client):
        assert client.connection_quality == ConnectionQuality.UNKNOWN

    def test_queue_size_initially_zero(self, client):
        assert client.queue_size == 0

    def test_subscribed_topics_initially_empty(self, client):
        assert client.subscribed_topics == set()


class TestSend:
    @pytest.mark.asyncio
    async def test_send_returns_true(self, client):
        ok = await client.send("test", {"key": "value"})
        assert ok is True
        client._connection.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_increments_stats(self, client):
        await client.send("test", {"data": 1})
        assert client._stats.messages_sent == 1
        assert client._stats.bytes_sent > 0

    @pytest.mark.asyncio
    async def test_send_with_priority(self, client):
        await client.send("cmd", {}, priority=MessagePriority.CRITICAL)
        sent_data = client._connection.send.call_args[0][0]
        parsed = json.loads(sent_data[1:])  # Strip U prefix
        assert parsed["pri"] == MessagePriority.CRITICAL

    @pytest.mark.asyncio
    async def test_send_with_retry_succeeds_first_try(self, client):
        ok = await client.send_with_retry("test", {"x": 1})
        assert ok is True
        assert client._connection.send.call_count == 1

    @pytest.mark.asyncio
    async def test_send_with_retry_retries_on_failure(self, client):
        client._connection.send = AsyncMock(side_effect=[False, False, True])
        ok = await client.send_with_retry("test", {}, max_retries=3)
        assert ok is True
        assert client._connection.send.call_count == 3

    @pytest.mark.asyncio
    async def test_send_with_retry_fails_after_max(self, client):
        client._connection.send = AsyncMock(return_value=False)
        ok = await client.send_with_retry("test", {}, max_retries=2)
        assert ok is False


class TestSubscribe:
    @pytest.mark.asyncio
    async def test_subscribe_adds_topics(self, client):
        ok = await client.subscribe(["topic_a", "topic_b"])
        assert ok is True
        assert "topic_a" in client.subscribed_topics
        assert "topic_b" in client.subscribed_topics

    @pytest.mark.asyncio
    async def test_subscribe_deduplicates(self, client):
        await client.subscribe(["topic_a"])
        await client.subscribe(["topic_a"])  # Already subscribed
        # Second call should not send (no new topics)
        assert client._connection.send.call_count == 1

    @pytest.mark.asyncio
    async def test_unsubscribe_removes_topics(self, client):
        await client.subscribe(["topic_a", "topic_b"])
        await client.unsubscribe(["topic_a"])
        assert "topic_a" not in client.subscribed_topics
        assert "topic_b" in client.subscribed_topics


class TestRequestSnapshot:
    @pytest.mark.asyncio
    async def test_request_snapshot_sends(self, client):
        client._subscribed_topics = {"prices"}
        ok = await client.request_snapshot()
        assert ok is True
        assert client._snapshot_requested is True

    @pytest.mark.asyncio
    async def test_request_snapshot_dedup(self, client):
        client._snapshot_requested = True
        ok = await client.request_snapshot()
        assert ok is True
        client._connection.send.assert_not_called()


class TestSystemHandlers:
    def test_server_ready_sets_flag(self, client):
        event = WSEEvent(
            type="server_ready",
            payload={"connection_id": "srv-123"},
        )
        client._handle_server_ready(event)
        assert client._server_ready_event.is_set()
        client._connection.handle_server_ready.assert_called_once_with("srv-123")

    def test_server_hello_stores_features(self, client):
        event = WSEEvent(
            type="server_hello",
            payload={
                "server_version": "2.0",
                "features": {"compression": True, "encryption": True},
                "max_message_size": 2_000_000,
                "rate_limit": 500,
            },
        )
        client._handle_server_hello(event)
        assert client._server_features == {"compression": True, "encryption": True}
        assert client._server_max_message_size == 2_000_000
        assert client._server_rate_limit == 500

    def test_error_handler_logs_auth(self, client):
        event = WSEEvent(
            type="error",
            payload={"code": "AUTH_FAILED", "message": "Bad token"},
        )
        # Should not raise
        client._handle_error(event)

    def test_error_handler_logs_rate_limit(self, client):
        event = WSEEvent(
            type="error",
            payload={
                "code": "RATE_LIMIT_EXCEEDED",
                "message": "Too fast",
                "retry_after": 5,
            },
        )
        client._handle_error(event)

    def test_snapshot_complete_resets_flag(self, client):
        client._snapshot_requested = True
        event = WSEEvent(
            type="snapshot_complete",
            payload={"topics": ["prices"], "count": 10},
        )
        client._handle_snapshot_complete(event)
        assert client._snapshot_requested is False

    def test_config_update_stores_rate_limit(self, client):
        event = WSEEvent(
            type="config_update",
            payload={"rate_limit": 2000},
        )
        client._handle_config_update(event)
        assert client._server_rate_limit == 2000

    def test_pong_records_latency(self, client):
        import time

        ts = time.time() * 1000 - 50  # 50ms ago
        event = WSEEvent(
            type="PONG",
            payload={"timestamp": ts},
        )
        client._handle_pong_event(event)
        assert client._stats.last_latency_ms is not None
        assert client._stats.last_latency_ms > 0

    def test_priority_message_unwraps(self, client):
        inner = {"t": "notification", "p": {"msg": "hello"}, "id": "inner-1"}
        event = WSEEvent(
            type="priority_message",
            payload={"message": inner},
        )
        # Should unwrap and dispatch the inner event
        client._handle_priority_message(event)
        # Inner event should be in queue
        assert client._event_queue.qsize() == 1

    def test_connection_quality_updates_stats(self, client):
        event = WSEEvent(
            type="connection_quality",
            payload={"quality": "excellent", "latency_ms": 25},
        )
        client._handle_connection_quality(event)
        assert client._stats.connection_quality == ConnectionQuality.EXCELLENT
        assert client._stats.last_latency_ms == 25


class TestDispatch:
    def test_internal_events_not_queued(self, client):
        # Skip events that need async loop (health_check, sync_request, metrics_request)
        async_handlers = {"health_check", "sync_request", "metrics_request"}
        for evt_type in _INTERNAL_ONLY_EVENTS - async_handlers:
            event = WSEEvent(type=evt_type, payload={})
            client._dispatch_event(event)
        assert client._event_queue.qsize() == 0

    def test_user_events_queued(self, client):
        event = WSEEvent(type="notification", payload={"msg": "hi"})
        client._dispatch_event(event)
        assert client._event_queue.qsize() == 1

    def test_handler_registration(self, client):
        received = []

        @client.on("test_event")
        def handler(event):
            received.append(event)

        event = WSEEvent(type="test_event", payload={"data": 1})
        client._dispatch_event(event)
        assert len(received) == 1

    def test_wildcard_handler(self, client):
        received = []

        @client.on_any
        def handler(event):
            received.append(event)

        client._dispatch_event(WSEEvent(type="a", payload={}))
        client._dispatch_event(WSEEvent(type="b", payload={}))
        assert len(received) == 2

    def test_off_removes_handler(self, client):
        received = []

        @client.on("test_event")
        def handler(event):
            received.append(event)

        client.off("test_event", handler)
        client._dispatch_event(WSEEvent(type="test_event", payload={}))
        assert len(received) == 0


class TestMessageProcessing:
    def test_decode_text_message(self, client):
        data = 'WSE{"t":"notification","p":{"msg":"hello"},"id":"evt-1","seq":0}'
        client._on_raw_message(data)
        assert client._stats.messages_received == 1
        assert client._event_queue.qsize() == 1

    def test_duplicate_message_filtered(self, client):
        data = 'WSE{"t":"notification","p":{},"id":"dup-1"}'
        client._on_raw_message(data)
        client._on_raw_message(data)
        # Only first should be queued
        assert client._event_queue.qsize() == 1

    def test_batch_message_unwrapped(self, client):
        batch = json.dumps(
            {
                "t": "batch",
                "p": {
                    "messages": [
                        {"t": "a", "p": {"n": 1}, "id": "b1"},
                        {"t": "b", "p": {"n": 2}, "id": "b2"},
                    ],
                },
            }
        )
        client._on_raw_message(batch)
        assert client._event_queue.qsize() == 2


class TestStats:
    def test_get_stats_structure(self, client):
        stats = client.get_stats()
        assert "state" in stats
        assert "is_ready" in stats
        assert "is_fully_ready" in stats
        assert "connection_id" in stats
        assert "subscribed_topics" in stats
        assert "messages_received" in stats
        assert "messages_sent" in stats
        assert "sequencer" in stats
        assert "network" in stats
        assert "circuit_breaker" in stats
        assert "rate_limiter" in stats

    def test_stats_network_structure(self, client):
        stats = client.get_stats()
        net = stats["network"]
        assert "quality" in net
        assert "latency_ms" in net
        assert "jitter_ms" in net
        assert "packet_loss" in net
        assert "stability" in net


class TestForceReconnect:
    @pytest.mark.asyncio
    async def test_force_reconnect(self, client):
        await client.force_reconnect()
        client._connection.force_reconnect.assert_called_once()
        assert not client._server_ready_event.is_set()


class TestChangeEndpoint:
    @pytest.mark.asyncio
    async def test_change_endpoint(self, client):
        client._connection.connect = AsyncMock()
        await client.change_endpoint("ws://new-server:5006/wse")
        client._connection.disconnect.assert_called_once()
        assert client._connection._url == "ws://new-server:5006/wse"
