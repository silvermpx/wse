"""The Python package around the extension: version and integrations."""

from importlib.metadata import version

import pytest

import wse_server


def test_version_comes_from_the_installed_distribution():
    assert wse_server.__version__ == version("wse-server")


def test_prometheus_collector_exposes_server_metrics(server_no_auth):
    prometheus_client = pytest.importorskip("prometheus_client")
    from wse_server.prometheus import WSECollector

    registry = prometheus_client.CollectorRegistry()
    registry.register(WSECollector(server_no_auth))
    text = prometheus_client.generate_latest(registry).decode()
    assert "wse_connections 0.0" in text
    assert "wse_uptime_seconds" in text
