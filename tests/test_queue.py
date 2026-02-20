"""Tests for PriorityMessageQueue."""

import asyncio
import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from server.connection.queue import PriorityMessageQueue, QueuedMessage


# =========================================================================
# QueuedMessage dataclass
# =========================================================================


class TestQueuedMessage:
    def test_creation(self):
        msg = QueuedMessage(message={"t": "test"}, priority=5, timestamp=1.0)
        assert msg.message == {"t": "test"}
        assert msg.priority == 5
        assert msg.timestamp == 1.0
        assert msg.retry_count == 0

    def test_retry_count(self):
        msg = QueuedMessage(message={}, priority=5, timestamp=1.0, retry_count=3)
        assert msg.retry_count == 3


# =========================================================================
# PriorityMessageQueue -- basic operations
# =========================================================================


class TestPriorityMessageQueueBasic:
    @pytest.mark.asyncio
    async def test_enqueue_and_size(self):
        q = PriorityMessageQueue(max_size=100)
        result = await q.enqueue({"t": "test"}, priority=5)
        assert result is True
        assert q.size == 1

    @pytest.mark.asyncio
    async def test_enqueue_multiple_priorities(self):
        q = PriorityMessageQueue(max_size=100)
        await q.enqueue({"t": "low"}, priority=3)
        await q.enqueue({"t": "high"}, priority=8)
        await q.enqueue({"t": "critical"}, priority=10)
        assert q.size == 3

    @pytest.mark.asyncio
    async def test_dequeue_batch_empty(self):
        q = PriorityMessageQueue(max_size=100)
        batch = await q.dequeue_batch()
        assert batch == []

    @pytest.mark.asyncio
    async def test_dequeue_returns_messages(self):
        q = PriorityMessageQueue(max_size=100, batch_size=10)
        await q.enqueue({"t": "msg1"}, priority=5)
        await q.enqueue({"t": "msg2"}, priority=5)
        batch = await q.dequeue_batch()
        assert len(batch) == 2
        assert q.size == 0

    @pytest.mark.asyncio
    async def test_priority_distribution_tracking(self):
        q = PriorityMessageQueue(max_size=100)
        await q.enqueue({"t": "a"}, priority=10)
        await q.enqueue({"t": "b"}, priority=5)
        await q.enqueue({"t": "c"}, priority=5)
        assert q.priority_distribution[10] == 1
        assert q.priority_distribution[5] == 2

    @pytest.mark.asyncio
    async def test_oldest_message_timestamp(self):
        q = PriorityMessageQueue(max_size=100)
        assert q.oldest_message_timestamp is None
        await q.enqueue({"t": "first"}, priority=5)
        assert q.oldest_message_timestamp is not None


# =========================================================================
# Priority ordering
# =========================================================================


class TestPriorityOrdering:
    @pytest.mark.asyncio
    async def test_critical_before_low(self):
        q = PriorityMessageQueue(max_size=100, batch_size=10)
        await q.enqueue({"t": "low"}, priority=3)
        await q.enqueue({"t": "bg"}, priority=1)
        await q.enqueue({"t": "critical"}, priority=10)
        await q.enqueue({"t": "normal"}, priority=5)
        await q.enqueue({"t": "high"}, priority=8)

        batch = await q.dequeue_batch()
        priorities = [p for p, _ in batch]
        # Should come out sorted: 10, 8, 5, 3, 1
        assert priorities == [10, 8, 5, 3, 1]

    @pytest.mark.asyncio
    async def test_same_priority_fifo(self):
        q = PriorityMessageQueue(max_size=100, batch_size=10)
        await q.enqueue({"order": 1}, priority=5)
        await q.enqueue({"order": 2}, priority=5)
        await q.enqueue({"order": 3}, priority=5)

        batch = await q.dequeue_batch()
        orders = [msg["order"] for _, msg in batch]
        assert orders == [1, 2, 3]


# =========================================================================
# Max size and dropping
# =========================================================================


class TestMaxSizeAndDropping:
    @pytest.mark.asyncio
    async def test_drops_low_priority_when_full(self):
        q = PriorityMessageQueue(max_size=2, batch_size=10)
        # Fill with LOW priority
        await q.enqueue({"t": "low1"}, priority=3)
        await q.enqueue({"t": "low2"}, priority=3)
        assert q.size == 2

        # Add HIGH priority -- should drop a LOW message
        result = await q.enqueue({"t": "high"}, priority=8)
        assert result is True
        assert q.size == 2  # size stays at max
        assert q.dropped_count[3] == 1  # one LOW was dropped

    @pytest.mark.asyncio
    async def test_drops_background_first(self):
        q = PriorityMessageQueue(max_size=2, batch_size=10)
        await q.enqueue({"t": "bg"}, priority=1)
        await q.enqueue({"t": "low"}, priority=3)
        assert q.size == 2

        result = await q.enqueue({"t": "normal"}, priority=5)
        assert result is True
        # BACKGROUND (1) should be dropped first
        assert q.dropped_count[1] == 1

    @pytest.mark.asyncio
    async def test_low_priority_dropped_when_full_with_high_only(self):
        q = PriorityMessageQueue(max_size=2, batch_size=10)
        await q.enqueue({"t": "high1"}, priority=8)
        await q.enqueue({"t": "high2"}, priority=8)
        assert q.size == 2

        # LOW priority cannot displace HIGH, so it is dropped itself
        result = await q.enqueue({"t": "low"}, priority=3)
        assert result is False
        assert q.dropped_count[3] == 1

    @pytest.mark.asyncio
    async def test_total_dropped_in_stats(self):
        q = PriorityMessageQueue(max_size=1, batch_size=10)
        await q.enqueue({"t": "first"}, priority=8)
        # This LOW msg will be dropped since queue is full of HIGH only
        await q.enqueue({"t": "dropped"}, priority=3)

        stats = q.get_stats()
        assert stats["total_dropped"] >= 1

    @pytest.mark.asyncio
    async def test_invalid_priority_normalized(self):
        q = PriorityMessageQueue(max_size=100, batch_size=10)
        # Priority 7 is not in {1, 3, 5, 8, 10} -- should be normalized
        result = await q.enqueue({"t": "odd"}, priority=7)
        assert result is True
        batch = await q.dequeue_batch()
        assert len(batch) == 1
        # 7 is closest to 8
        assert batch[0][0] == 8


# =========================================================================
# Batch dequeue
# =========================================================================


class TestBatchDequeue:
    @pytest.mark.asyncio
    async def test_batch_size_limit(self):
        q = PriorityMessageQueue(max_size=100, batch_size=3)
        for i in range(10):
            await q.enqueue({"i": i}, priority=5)

        batch = await q.dequeue_batch()
        assert len(batch) == 3
        assert q.size == 7

    @pytest.mark.asyncio
    async def test_batch_crosses_priorities(self):
        q = PriorityMessageQueue(max_size=100, batch_size=5)
        await q.enqueue({"t": "crit"}, priority=10)
        await q.enqueue({"t": "high"}, priority=8)
        await q.enqueue({"t": "norm"}, priority=5)

        batch = await q.dequeue_batch()
        assert len(batch) == 3
        priorities = [p for p, _ in batch]
        assert priorities == [10, 8, 5]

    @pytest.mark.asyncio
    async def test_multiple_dequeue_batches(self):
        q = PriorityMessageQueue(max_size=100, batch_size=2)
        for i in range(5):
            await q.enqueue({"i": i}, priority=5)

        batch1 = await q.dequeue_batch()
        batch2 = await q.dequeue_batch()
        batch3 = await q.dequeue_batch()
        assert len(batch1) == 2
        assert len(batch2) == 2
        assert len(batch3) == 1

    @pytest.mark.asyncio
    async def test_dequeue_resets_oldest_timestamp(self):
        q = PriorityMessageQueue(max_size=100, batch_size=10)
        await q.enqueue({"t": "a"}, priority=5)
        assert q.oldest_message_timestamp is not None

        await q.dequeue_batch()
        assert q.oldest_message_timestamp is None


# =========================================================================
# Clear and stats
# =========================================================================


class TestClearAndStats:
    @pytest.mark.asyncio
    async def test_clear(self):
        q = PriorityMessageQueue(max_size=100)
        await q.enqueue({"t": "a"}, priority=5)
        await q.enqueue({"t": "b"}, priority=10)
        assert q.size == 2

        q.clear()
        assert q.size == 0
        assert q.oldest_message_timestamp is None

    @pytest.mark.asyncio
    async def test_get_stats_structure(self):
        q = PriorityMessageQueue(max_size=100)
        await q.enqueue({"t": "a"}, priority=5)
        stats = q.get_stats()
        assert "size" in stats
        assert "capacity" in stats
        assert "utilization_percent" in stats
        assert "priority_distribution" in stats
        assert "priority_queue_depths" in stats
        assert "dropped_by_priority" in stats
        assert "total_dropped" in stats
        assert "backpressure" in stats

    @pytest.mark.asyncio
    async def test_backpressure_flag(self):
        q = PriorityMessageQueue(max_size=10)
        # Fill to 90% (9 messages) to trigger backpressure (threshold is 80%)
        for i in range(9):
            await q.enqueue({"i": i}, priority=5)
        stats = q.get_stats()
        assert stats["backpressure"] is True

    @pytest.mark.asyncio
    async def test_no_backpressure_when_low(self):
        q = PriorityMessageQueue(max_size=100)
        await q.enqueue({"t": "a"}, priority=5)
        stats = q.get_stats()
        assert stats["backpressure"] is False

    @pytest.mark.asyncio
    async def test_utilization_percent(self):
        q = PriorityMessageQueue(max_size=10)
        await q.enqueue({"t": "a"}, priority=5)
        stats = q.get_stats()
        assert stats["utilization_percent"] == 10.0
