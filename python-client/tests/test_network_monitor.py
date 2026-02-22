"""Tests for network monitor."""

from wse_client.network_monitor import NetworkMonitor
from wse_client.types import ConnectionQuality


class TestNetworkMonitor:
    def test_unknown_quality_initially(self):
        nm = NetworkMonitor()
        assert nm.quality == ConnectionQuality.UNKNOWN

    def test_unknown_with_one_sample(self):
        nm = NetworkMonitor()
        nm.record_pong(25.0)
        # Need at least 2 samples for analysis
        assert nm.quality == ConnectionQuality.UNKNOWN

    def test_excellent_quality(self):
        nm = NetworkMonitor()
        for _ in range(5):
            nm.record_pong(20.0)
        assert nm.quality == ConnectionQuality.EXCELLENT

    def test_good_quality(self):
        nm = NetworkMonitor()
        for _ in range(5):
            nm.record_pong(100.0)
        assert nm.quality == ConnectionQuality.GOOD

    def test_fair_quality(self):
        nm = NetworkMonitor()
        for _ in range(5):
            nm.record_pong(200.0)
        assert nm.quality == ConnectionQuality.FAIR

    def test_poor_quality(self):
        nm = NetworkMonitor()
        for _ in range(5):
            nm.record_pong(500.0)
        assert nm.quality == ConnectionQuality.POOR

    def test_jitter_calculation(self):
        nm = NetworkMonitor()
        # Alternating latencies = high jitter
        for lat in [10.0, 100.0, 10.0, 100.0, 10.0]:
            nm.record_pong(lat)
        diag = nm.analyze()
        assert diag.jitter > 50.0

    def test_packet_loss(self):
        nm = NetworkMonitor()
        nm.record_ping()
        nm.record_ping()
        nm.record_ping()
        nm.record_ping()
        nm.record_pong(20.0)
        nm.record_pong(20.0)
        # 4 pings, 2 pongs = 50% loss
        diag = nm.analyze()
        assert diag.packet_loss == 50.0

    def test_stability(self):
        nm = NetworkMonitor()
        for _ in range(10):
            nm.record_pong(20.0)
        diag = nm.analyze()
        assert diag.stability > 0.9

    def test_suggestions_for_poor(self):
        nm = NetworkMonitor()
        for _ in range(5):
            nm.record_pong(500.0)
        diag = nm.analyze()
        assert len(diag.suggestions) > 0

    def test_reset(self):
        nm = NetworkMonitor()
        nm.record_ping()
        nm.record_pong(25.0)
        nm.record_pong(25.0)
        nm.reset()
        assert nm.quality == ConnectionQuality.UNKNOWN

    def test_window_size_limits(self):
        nm = NetworkMonitor(window_size=5)
        for i in range(20):
            nm.record_pong(float(i * 10))
        # Should only keep last 5 samples
        diag = nm.analyze()
        assert diag.round_trip_time > 100  # Average of 150,160,170,180,190
