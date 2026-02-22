"""Tests for connection pool."""

from wse_client.connection_pool import ConnectionPool
from wse_client.types import LoadBalancingStrategy


class TestConnectionPool:
    def test_round_robin(self):
        pool = ConnectionPool(
            ["ws://a", "ws://b", "ws://c"],
            strategy=LoadBalancingStrategy.ROUND_ROBIN,
        )
        results = [pool.select_endpoint() for _ in range(6)]
        assert results == ["ws://a", "ws://b", "ws://c", "ws://a", "ws://b", "ws://c"]

    def test_least_connections(self):
        pool = ConnectionPool(
            ["ws://a", "ws://b"],
            strategy=LoadBalancingStrategy.LEAST_CONNECTIONS,
        )
        pool.add_connection("ws://a")
        pool.add_connection("ws://a")
        assert pool.select_endpoint() == "ws://b"

    def test_weighted_random_prefers_healthy(self):
        pool = ConnectionPool(
            ["ws://good", "ws://bad"],
            strategy=LoadBalancingStrategy.WEIGHTED_RANDOM,
        )
        # Tank the "bad" endpoint
        for _ in range(10):
            pool.record_failure("ws://bad")

        # Sample many times
        choices = [pool.select_endpoint() for _ in range(100)]
        good_count = choices.count("ws://good")
        assert good_count > 70  # Should strongly prefer healthy endpoint

    def test_record_success_and_failure(self):
        pool = ConnectionPool(["ws://a"])
        pool.record_failure("ws://a")
        stats = pool.get_stats()
        assert stats[0]["failures"] == 1

        pool.record_success("ws://a", latency_ms=50.0)
        stats = pool.get_stats()
        assert stats[0]["successes"] == 1

    def test_add_remove_connections(self):
        pool = ConnectionPool(["ws://a"])
        pool.add_connection("ws://a")
        pool.add_connection("ws://a")
        stats = pool.get_stats()
        assert stats[0]["active"] == 2

        pool.remove_connection("ws://a")
        stats = pool.get_stats()
        assert stats[0]["active"] == 1
