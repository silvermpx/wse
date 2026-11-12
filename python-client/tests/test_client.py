"""Tests for AsyncWSEClient (unit tests with mocked WebSocket)."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from wse_client.client import AsyncWSEClient, _INTERNAL_ONLY_EVENTS
from wse_client.types import (
    ConnectionQuality,
    ConnectionState,
    MessagePriority,
    SendResult,
    WSEEvent,
)


@pytest.fixture
def client():
    """Create a client with mocked connection manager."""
    import asyncio

    c = AsyncWSEClient("ws://localhost:5006/wse", token="test-jwt")
    # Initialize lazy asyncio primitives (normally done in connect())
    c._event_queue = asyncio.Queue(maxsize=c._queue_size)
    c._server_ready_event = asyncio.Event()
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
        assert ok is SendResult.SENT
        assert ok  # truthy, so `if await client.send(...)` keeps working
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
        parsed = json.loads(sent_data)
        assert parsed["pri"] == MessagePriority.CRITICAL

    @pytest.mark.asyncio
    async def test_send_with_retry_succeeds_first_try(self, client):
        ok = await client.send_with_retry("test", {"x": 1})
        assert ok is SendResult.SENT
        assert client._connection.send.call_count == 1

    @pytest.mark.asyncio
    async def test_send_with_retry_retries_on_failure(self, client):
        client._connection.send = AsyncMock(side_effect=[False, False, True])
        ok = await client.send_with_retry("test", {}, max_retries=3)
        assert ok is SendResult.SENT
        assert client._connection.send.call_count == 3

    @pytest.mark.asyncio
    async def test_send_with_retry_fails_after_max(self, client):
        client._connection.send = AsyncMock(return_value=False)
        ok = await client.send_with_retry("test", {}, max_retries=2)
        assert ok is SendResult.FAILED
        assert not ok


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
        sent = json.loads(client._connection.send.call_args[0][0])
        assert sent["t"] == "sync_request"
        assert sent["p"]["topics"] == ["prices"]

    @pytest.mark.asyncio
    async def test_request_snapshot_dedups_identical_in_flight(self, client):
        assert await client.request_snapshot(["prices"]) is True
        assert await client.request_snapshot(["prices"]) is True
        assert client._connection.send.call_count == 1

    @pytest.mark.asyncio
    async def test_request_snapshot_other_topics_always_sent(self, client):
        # The second request used to return True without sending,
        # because nothing in the server ever sends snapshot_complete.
        assert await client.request_snapshot(["prices"]) is True
        assert await client.request_snapshot(["orders"]) is True
        assert client._connection.send.call_count == 2
        topics = [
            json.loads(c[0][0])["p"]["topics"]
            for c in client._connection.send.call_args_list
        ]
        assert topics == [["prices"], ["orders"]]

    @pytest.mark.asyncio
    async def test_request_snapshot_resent_after_window(self, client, monkeypatch):
        import wse_client.client as client_mod

        monkeypatch.setattr(client_mod, "SNAPSHOT_DEDUP_WINDOW", 0.0)
        assert await client.request_snapshot(["prices"]) is True
        assert await client.request_snapshot(["prices"]) is True
        assert client._connection.send.call_count == 2


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
        # Limits are nested under "limits" with capacity/refill (the real server
        # wire shape, server.rs build_server_hello), and ping_interval is top-level.
        event = WSEEvent(
            type="server_hello",
            payload={
                "server_version": "2.0",
                "features": {"compression": True, "encryption": True},
                "limits": {
                    "max_message_size": 2_000_000,
                    "rate_limit_capacity": 500,
                    "rate_limit_refill": 100,
                },
                "ping_interval": 25_000,
            },
        )
        client._handle_server_hello(event)
        assert client._server_features == {"compression": True, "encryption": True}
        assert client._server_max_message_size == 2_000_000
        assert client._server_rate_limit == 500
        assert client._server_ping_interval_ms == 25_000

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
                "code": "RATE_LIMITED",
                "message": "Too fast",
                "retry_after": 5,
            },
        )
        client._handle_error(event)

    @pytest.mark.asyncio
    async def test_snapshot_complete_resets_flag(self, client):
        assert await client.request_snapshot(["prices"]) is True
        event = WSEEvent(
            type="snapshot_complete",
            payload={"topics": ["prices"], "count": 10},
        )
        client._handle_snapshot_complete(event)
        assert await client.request_snapshot(["prices"]) is True
        assert client._connection.send.call_count == 2

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


def _mock_client(**kwargs):
    """A client whose transport is a mock (connected unless told otherwise)."""
    import asyncio

    c = AsyncWSEClient("ws://localhost:5006/wse", **kwargs)
    c._event_queue = asyncio.Queue(maxsize=c._queue_size)
    c._server_ready_event = asyncio.Event()
    c._connection = MagicMock()
    c._connection.is_connected = True
    c._connection.state = ConnectionState.CONNECTED
    c._connection.send = AsyncMock(return_value=True)
    c._connection.destroy = AsyncMock()
    c._connection._pool = None
    return c


class TestOfflineQueueOptIn:
    """The offline queue is opt-in and a failed send is never replayed."""

    @pytest.mark.asyncio
    async def test_failed_send_is_not_queued_by_default(self):
        c = _mock_client()
        c._connection.is_connected = False
        result = await c.send("place_order", {"id": 1})
        assert result is SendResult.FAILED
        assert c._offline_queue.size == 0

    @pytest.mark.asyncio
    async def test_send_while_disconnected_is_queued_when_enabled(self):
        c = _mock_client(offline_queue=True)
        c._connection.is_connected = False
        result = await c.send("place_order", {"id": 1})
        assert result is SendResult.QUEUED
        assert result  # accepted: the caller must not retry
        assert c._offline_queue.size == 1

    @pytest.mark.asyncio
    async def test_send_with_retry_never_queues_copies(self):
        c = _mock_client(offline_queue=True)
        c._connection.send = AsyncMock(return_value=False)  # connected, sends fail
        result = await c.send_with_retry("place_order", {"id": 1}, max_retries=3)
        assert result is SendResult.FAILED
        assert c._offline_queue.size == 0
        # One id across every attempt, so the server can dedupe retries.
        ids = {
            json.loads(call[0][0])["id"] for call in c._connection.send.call_args_list
        }
        assert c._connection.send.call_count == 3
        assert len(ids) == 1

    @pytest.mark.asyncio
    async def test_send_with_retry_queues_once_when_disconnected(self):
        c = _mock_client(offline_queue=True)
        c._connection.is_connected = False
        result = await c.send_with_retry("place_order", {"id": 1}, max_retries=5)
        assert result is SendResult.QUEUED
        assert c._offline_queue.size == 1

    @pytest.mark.asyncio
    async def test_flush_sends_queued_messages_once(self):
        c = _mock_client(offline_queue=True)
        c._connection.is_connected = False
        await c.send("a", {})
        await c.send("b", {}, priority=MessagePriority.CRITICAL)
        c._connection.is_connected = True
        await c._flush_offline_queue()
        sent = [
            json.loads(call[0][0])["t"] for call in c._connection.send.call_args_list
        ]
        assert sent == ["b", "a"]  # highest priority first
        assert c._offline_queue.size == 0


class TestRateLimit:
    """The outbound limiter is configurable, can be disabled, and only
    applies to application sends."""

    @pytest.mark.asyncio
    async def test_limit_and_burst_are_configurable(self):
        c = _mock_client(rate_limit=1, rate_limit_burst=3)
        results = [await c.send("tick", {"n": i}) for i in range(5)]
        assert results[:3] == [SendResult.SENT] * 3
        assert results[3:] == [SendResult.FAILED] * 2
        assert c.get_stats()["rate_limiter"]["rate_limited"] == 2

    @pytest.mark.asyncio
    async def test_rate_limited_send_is_not_queued(self):
        c = _mock_client(rate_limit=1, rate_limit_burst=1, offline_queue=True)
        assert await c.send("tick", {}) is SendResult.SENT
        assert await c.send("tick", {}) is SendResult.FAILED
        assert c._offline_queue.size == 0

    @pytest.mark.asyncio
    async def test_none_disables_the_limiter(self):
        c = _mock_client(rate_limit=None)
        results = [await c.send("tick", {}) for _ in range(2000)]
        assert all(r is SendResult.SENT for r in results)
        assert c.get_stats()["rate_limiter"] is None

    @pytest.mark.asyncio
    async def test_default_stays_100_per_second_burst_1000(self):
        c = _mock_client()
        stats = c.get_stats()["rate_limiter"]
        assert stats["capacity"] == 1000
        assert stats["refill_rate"] == 100

    @pytest.mark.asyncio
    async def test_protocol_frames_bypass_the_limiter(self):
        c = _mock_client(rate_limit=1, rate_limit_burst=1)
        assert await c.send("tick", {}) is SendResult.SENT
        assert await c.send("tick", {}) is SendResult.FAILED
        # Subscriptions and protocol replies still go out.
        assert await c.subscribe(["prices"]) is True
        assert await c._send_internal("sync_response", {}, priority=5) is True

    def test_invalid_rate_limit_rejected(self):
        with pytest.raises(ValueError):
            AsyncWSEClient("ws://x/wse", rate_limit=0)


class TestMalformedPayloads:
    """A malformed system payload is dropped, never fatal."""

    def test_subscription_update_with_null_recovery(self, client):
        client._on_raw_message(
            '{"c":"WSE","t":"subscription_update","p":{"success_topics":["a"],'
            '"recovery":null}}'
        )
        client._on_raw_message('{"t":"after","p":{},"id":"x1"}')
        assert client._event_queue.qsize() == 1

    def test_server_ready_with_bad_details(self, client):
        client._on_raw_message('{"c":"WSE","t":"server_ready","p":{"details":null}}')
        client._on_raw_message('{"c":"WSE","t":"server_ready","p":{"details":"x"}}')
        assert client._server_ready_event.is_set()

    def test_corrupt_msgpack_frame_is_dropped(self, client):
        client._on_raw_message(b"M:\xc1\xc1\xc1")
        client._on_raw_message('{"t":"after","p":{},"id":"x2"}')
        assert client._event_queue.qsize() == 1

    def test_error_with_bad_details(self, client):
        client._on_raw_message(
            '{"c":"WSE","t":"error","p":{"code":"SUBSCRIPTION_FAILED","details":5}}'
        )
        # Forwarded to the user like any error event.
        assert client._event_queue.qsize() == 1


class TestDisconnectFromHandler:
    @pytest.mark.asyncio
    async def test_async_handler_can_await_disconnect(self):
        """Disconnect() used to cancel the handler's own task, aborting
        the teardown half way (socket left open)."""
        import asyncio

        c = _mock_client()
        finished = asyncio.Event()

        @c.on("shutdown")
        async def handler(event):
            await c.disconnect()
            finished.set()

        c._dispatch_event(WSEEvent(type="shutdown", payload={}))
        await asyncio.wait_for(finished.wait(), timeout=2.0)
        c._connection.destroy.assert_awaited_once()


class TestIterationEnd:
    @pytest.mark.asyncio
    async def test_disconnect_ends_iteration_cleanly(self):
        c = _mock_client()
        c._dispatch_event(WSEEvent(type="a", payload={}))
        await c.disconnect()
        received = [e.type async for e in c]
        assert received == ["a"]

    @pytest.mark.asyncio
    async def test_terminal_error_is_raised_to_consumers(self):
        from wse_client.errors import WSEAuthError

        c = _mock_client()
        c._dispatch_event(WSEEvent(type="a", payload={}))
        c._on_transport_terminal(WSEAuthError("rejected"))
        assert (await c.recv()).type == "a"
        with pytest.raises(WSEAuthError):
            await c.recv()
        with pytest.raises(WSEAuthError):
            async for _ in c:
                pass
        assert c.last_error is not None

    @pytest.mark.asyncio
    async def test_state_changes_do_not_end_iteration(self):
        """A failed reconnect attempt is not the end of the stream."""
        import asyncio

        c = _mock_client()
        for state in (
            ConnectionState.RECONNECTING,
            ConnectionState.ERROR,
            ConnectionState.DISCONNECTED,
        ):
            c._on_state_change(state)
        with pytest.raises(asyncio.TimeoutError):
            await c.recv(timeout=0.05)

    @pytest.mark.asyncio
    async def test_connect_drops_stale_end_markers(self):
        from wse_client.errors import WSEConnectionError

        c = _mock_client()
        c._connection.state = ConnectionState.ERROR
        c._on_transport_terminal(WSEConnectionError("old failure"))
        c._connection.connect = AsyncMock()
        c._initial_topics = []
        await c.connect()
        c._dispatch_event(WSEEvent(type="fresh", payload={}))
        assert (await c.recv(timeout=1)).type == "fresh"
