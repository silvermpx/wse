"""Expose a RustWSEServer's metrics through prometheus_client.

Most Python services already serve ``/metrics`` from a prometheus_client
registry. Registering a collector puts the server's metrics on that same
endpoint, with no second exporter::

    from prometheus_client import REGISTRY
    from wse_server.prometheus import WSECollector

    REGISTRY.register(WSECollector(server))

Requires the ``prometheus`` extra: ``pip install wse-server[prometheus]``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from prometheus_client.parser import text_string_to_metric_families

if TYPE_CHECKING:
    from collections.abc import Iterator

    from prometheus_client.metrics_core import Metric

    from wse_server._wse_accel import RustWSEServer


class WSECollector:
    """A prometheus_client collector that reads the server on every scrape.

    The metrics are the ones ``RustWSEServer.prometheus_metrics()`` renders
    (connections, queue depths, per-topic counters, cluster and recovery
    figures), parsed into metric families so the registry can merge them with
    the application's own metrics.
    """

    def __init__(self, server: RustWSEServer) -> None:
        self._server = server

    def collect(self) -> Iterator[Metric]:
        yield from text_string_to_metric_families(self._server.prometheus_metrics())

    def describe(self) -> list[Metric]:
        # Metric names depend on live topics, so nothing is declared up front;
        # an empty description keeps registration from scraping the server.
        return []
