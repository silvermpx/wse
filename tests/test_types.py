"""Tests for WSE core types."""

import os
import sys
from datetime import UTC, datetime

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from wse_server.core.types import (
    DeliveryGuarantee,
    EventMetadata,
    EventPriority,
    Subscription,
    SubscriptionStats,
)

# =========================================================================
# EventPriority
# =========================================================================


class TestEventPriority:
    def test_values(self):
        assert EventPriority.CRITICAL.value == 10
        assert EventPriority.HIGH.value == 8
        assert EventPriority.NORMAL.value == 5
        assert EventPriority.LOW.value == 3
        assert EventPriority.BACKGROUND.value == 1

    def test_ordering(self):
        """Higher priority has higher numeric value."""
        assert EventPriority.CRITICAL.value > EventPriority.HIGH.value
        assert EventPriority.HIGH.value > EventPriority.NORMAL.value
        assert EventPriority.NORMAL.value > EventPriority.LOW.value
        assert EventPriority.LOW.value > EventPriority.BACKGROUND.value

    def test_all_members(self):
        members = list(EventPriority)
        assert len(members) == 5

    def test_from_value(self):
        assert EventPriority(10) == EventPriority.CRITICAL
        assert EventPriority(5) == EventPriority.NORMAL
        assert EventPriority(1) == EventPriority.BACKGROUND

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError):
            EventPriority(99)


# =========================================================================
# DeliveryGuarantee
# =========================================================================


class TestDeliveryGuarantee:
    def test_values(self):
        assert DeliveryGuarantee.AT_MOST_ONCE.value == "at_most_once"
        assert DeliveryGuarantee.AT_LEAST_ONCE.value == "at_least_once"
        assert DeliveryGuarantee.EXACTLY_ONCE.value == "exactly_once"

    def test_all_members(self):
        members = list(DeliveryGuarantee)
        assert len(members) == 3

    def test_from_value(self):
        assert DeliveryGuarantee("at_most_once") == DeliveryGuarantee.AT_MOST_ONCE


# =========================================================================
# EventMetadata
# =========================================================================


class TestEventMetadata:
    def test_defaults(self):
        meta = EventMetadata()
        assert meta.event_id is not None
        assert len(meta.event_id) > 0
        assert meta.timestamp is not None
        assert meta.version == 1
        assert meta.priority == EventPriority.NORMAL
        assert meta.ttl is None
        assert meta.correlation_id is None
        assert meta.causation_id is None
        assert meta.source is None
        assert meta.compressed is False
        assert meta.encrypted is False
        assert meta.user_id is None
        assert meta.connection_id is None

    def test_custom_values(self):
        now = datetime.now(UTC)
        meta = EventMetadata(
            event_id="custom_id",
            timestamp=now,
            version=1,
            priority=EventPriority.CRITICAL,
            ttl=60,
            correlation_id="cor_123",
            causation_id="cau_456",
            source="test_source",
            compressed=True,
            encrypted=True,
            user_id="user_789",
            connection_id="conn_abc",
        )
        assert meta.event_id == "custom_id"
        assert meta.timestamp == now
        assert meta.version == 1
        assert meta.priority == EventPriority.CRITICAL
        assert meta.ttl == 60
        assert meta.correlation_id == "cor_123"
        assert meta.causation_id == "cau_456"
        assert meta.source == "test_source"
        assert meta.compressed is True
        assert meta.encrypted is True
        assert meta.user_id == "user_789"
        assert meta.connection_id == "conn_abc"

    def test_unique_event_ids(self):
        """Each EventMetadata instance gets a unique event_id."""
        m1 = EventMetadata()
        m2 = EventMetadata()
        assert m1.event_id != m2.event_id

    def test_timestamp_is_utc(self):
        meta = EventMetadata()
        assert meta.timestamp.tzinfo is not None


# =========================================================================
# Subscription
# =========================================================================


class TestSubscription:
    def test_defaults(self):
        sub = Subscription()
        assert sub.id is not None
        assert sub.subscriber_id == ""
        assert sub.topics == set()
        assert sub.handler is None
        assert sub.filters == {}
        assert sub.transform is None
        assert sub.active is True
        assert sub.delivery_guarantee == DeliveryGuarantee.AT_LEAST_ONCE
        assert sub.max_retries == 3
        assert sub.retry_delay == 1.0
        assert sub.dead_letter_topic is None
        assert sub.batch_size == 100
        assert sub.batch_timeout == 0.1
        assert sub.last_delivered_id is None
        assert sub.last_delivery_time is None

    def test_custom_values(self):
        sub = Subscription(
            id="sub_1",
            subscriber_id="user_abc",
            topics={"orders", "prices"},
            filters={"symbol": "AAPL"},
            active=False,
            delivery_guarantee=DeliveryGuarantee.EXACTLY_ONCE,
            max_retries=5,
            retry_delay=2.0,
            dead_letter_topic="dlq.orders",
            batch_size=50,
            batch_timeout=0.5,
        )
        assert sub.id == "sub_1"
        assert sub.subscriber_id == "user_abc"
        assert "orders" in sub.topics
        assert "prices" in sub.topics
        assert sub.filters == {"symbol": "AAPL"}
        assert sub.active is False
        assert sub.delivery_guarantee == DeliveryGuarantee.EXACTLY_ONCE
        assert sub.max_retries == 5
        assert sub.dead_letter_topic == "dlq.orders"

    def test_unique_subscription_ids(self):
        s1 = Subscription()
        s2 = Subscription()
        assert s1.id != s2.id

    def test_mutable_topics(self):
        sub = Subscription()
        sub.topics.add("new_topic")
        assert "new_topic" in sub.topics

    def test_created_at_is_set(self):
        sub = Subscription()
        assert sub.created_at is not None
        assert sub.created_at.tzinfo is not None

    def test_handler_protocol(self):
        """Subscription can accept any callable handler."""

        async def my_handler(event: dict) -> None:
            pass

        sub = Subscription(handler=my_handler)
        assert sub.handler is my_handler


# =========================================================================
# SubscriptionStats
# =========================================================================


class TestSubscriptionStats:
    def test_defaults(self):
        stats = SubscriptionStats()
        assert stats.messages_received == 0
        assert stats.messages_processed == 0
        assert stats.messages_failed == 0
        assert stats.messages_filtered == 0
        assert stats.messages_transformed == 0
        assert stats.messages_duplicate == 0
        assert stats.last_message_at is None
        assert stats.average_processing_time == 0.0
        assert stats.total_processing_time == 0.0

    def test_mutable_counters(self):
        stats = SubscriptionStats()
        stats.messages_received = 10
        stats.messages_processed = 8
        stats.messages_failed = 2
        assert stats.messages_received == 10
        assert stats.messages_processed == 8
        assert stats.messages_failed == 2

    def test_last_message_at(self):
        stats = SubscriptionStats()
        now = datetime.now(UTC)
        stats.last_message_at = now
        assert stats.last_message_at == now
