"""The Rust objects used from many Python threads at once.

In a real deployment one server object is shared: a drain loop waits in
``drain_inbound()`` while other threads broadcast, and ``stop()`` comes from yet
another thread. On free-threaded Python (3.14t, 3.15t) any two calls can overlap,
so every test here releases its threads together and checks that no call fails
and nothing is lost or handed out twice.
"""

import subprocess
import sys
import sysconfig
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from wse_server import (
    RustCompressionManager,
    RustEventSequencer,
    RustPriorityMessageQueue,
    RustPriorityQueue,
    RustSequencer,
    RustTokenBucket,
    RustWSEServer,
)

THREADS = 8
CALLS = 2_000

FREE_THREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


def run_in_threads(fn, threads=THREADS):
    """Run ``fn(index)`` in ``threads`` threads released together; return the results.

    An exception in any thread is raised here, so it fails the test.
    """
    barrier = threading.Barrier(threads)

    def worker(index):
        barrier.wait()
        return fn(index)

    with ThreadPoolExecutor(threads) as pool:
        return list(pool.map(worker, range(threads)))


def drain_server(port):
    srv = RustWSEServer("127.0.0.1", port, max_connections=10)
    srv.enable_drain_mode()
    return srv


# ---------------------------------------------------------------------------
# Server lifecycle across threads
# ---------------------------------------------------------------------------


class TestServerAcrossThreads:
    def test_stop_while_another_thread_drains(self, server_port):
        """stop() works while a drain loop waits in drain_inbound().

        This is the documented shutdown pattern. stop() used to fail with
        "RuntimeError: Already borrowed" (also with the GIL, which
        drain_inbound() releases while it waits) and the server kept running.
        """
        srv = drain_server(server_port)
        srv.start()
        time.sleep(0.05)  # let the listener bind

        def drain_loop():
            while srv.is_running():
                srv.drain_inbound(256, 50)

        worker = threading.Thread(target=drain_loop, daemon=True)
        worker.start()
        time.sleep(0.2)
        assert worker.is_alive()  # the loop runs: it waits inside drain_inbound()
        srv.stop()
        worker.join(timeout=5)
        assert not worker.is_alive()
        assert not srv.is_running()

    def test_running_as_soon_as_start_returns(self, server_port):
        """A drain loop started right after start() must not see a stale False."""
        srv = drain_server(server_port)
        srv.start()
        try:
            assert srv.is_running()
        finally:
            srv.stop()

    def test_concurrent_start_runs_one_server(self, server_port):
        """Of several start() calls at once, exactly one starts the server."""
        srv = drain_server(server_port)

        def start(_):
            try:
                srv.start()
            except RuntimeError as exc:
                return str(exc)
            return "started"

        try:
            outcomes = run_in_threads(start, threads=4)
        finally:
            srv.stop()
        assert outcomes.count("started") == 1
        assert all("already running" in o for o in outcomes if o != "started")
        assert not srv.is_running()

    def test_concurrent_stop(self, server_port):
        """Several stop() calls at once all return, and the server is stopped."""
        srv = drain_server(server_port)
        srv.start()
        run_in_threads(lambda _: srv.stop(), threads=4)
        assert not srv.is_running()

    def test_restart_after_stop(self, server_port):
        srv = drain_server(server_port)
        srv.start()
        srv.stop()
        srv.start()
        try:
            assert srv.is_running()
        finally:
            srv.stop()

    def test_drain_and_publish_from_many_threads(self, server_port):
        """Drain loops, broadcasts and health reads overlap without errors."""
        srv = drain_server(server_port)
        srv.start()

        def work(index):
            # Each thread runs half a second from its own release, and makes at
            # least one call: on a busy runner a thread can start late.
            stop_at = time.monotonic() + 0.5
            calls = 0
            while True:
                if index % 4 == 0:
                    srv.drain_inbound(64, 5)
                elif index % 4 == 1:
                    srv.broadcast_local("topic", '{"t":"x","p":{}}')
                elif index % 4 == 2:
                    srv.broadcast_all('{"t":"y","p":{}}')
                else:
                    assert srv.health_snapshot()["connections"] == 0
                    srv.prometheus_metrics()
                calls += 1
                if time.monotonic() >= stop_at:
                    return calls

        try:
            assert all(calls > 0 for calls in run_in_threads(work))
        finally:
            srv.stop()


# ---------------------------------------------------------------------------
# Utility classes shared between threads
# ---------------------------------------------------------------------------


class TestUtilitiesAcrossThreads:
    def test_sequencer_numbers_stay_unique(self):
        seq = RustSequencer(10_000)
        chunks = run_in_threads(lambda _: [seq.next_seq() for _ in range(CALLS)])
        numbers = sorted(n for chunk in chunks for n in chunk)
        assert numbers == list(range(1, THREADS * CALLS + 1))
        assert seq.get_current_seq() == THREADS * CALLS

    def test_sequencer_dedup_from_many_threads(self):
        seq = RustSequencer(THREADS * CALLS)
        fresh = run_in_threads(
            lambda index: sum(not seq.is_duplicate(f"{index}-{i}") for i in range(CALLS))
        )
        assert sum(fresh) == THREADS * CALLS
        assert seq.seen_count() == THREADS * CALLS

    def test_event_sequencer_numbers_stay_unique(self):
        seq = RustEventSequencer()
        chunks = run_in_threads(lambda _: [seq.next_seq() for _ in range(CALLS)])
        numbers = sorted(n for chunk in chunks for n in chunk)
        assert numbers == list(range(1, THREADS * CALLS + 1))

    def test_event_sequencer_keeps_each_topic_in_order(self):
        seq = RustEventSequencer()
        events_per_topic = 500

        def feed(index):
            topic = f"topic-{index}"
            delivered = []
            # Out of order on purpose: every pair arrives swapped (2, 1, 4, 3, ...).
            for n in range(1, events_per_topic + 1, 2):
                for sequence in (n + 1, n):
                    out = seq.process_sequenced_event(topic, sequence, {"n": sequence})
                    if out:
                        delivered.extend(event["n"] for event in out)
            return delivered

        for delivered in run_in_threads(feed):
            # The first event of a topic starts it, so 2 comes first and 1 is late.
            assert delivered == [2, *range(3, events_per_topic + 1)]
        stats = seq.get_sequence_stats()
        assert stats["total_topics"] == THREADS
        assert stats["total_buffered"] == 0

    def test_token_bucket_never_overspends(self):
        bucket = RustTokenBucket(1000.0, 0.0)  # no refill: 1000 tokens, ever
        granted = run_in_threads(lambda _: sum(bucket.acquire(1.0) for _ in range(CALLS)))
        assert sum(granted) == 1000
        assert bucket.tokens == 0.0

    def test_priority_queue_keeps_every_message(self):
        total = THREADS * CALLS
        queue = RustPriorityQueue(total)
        pushed = run_in_threads(
            lambda index: sum(queue.push(i % 10, (index, i)) for i in range(CALLS))
        )
        assert sum(pushed) == total
        assert queue.len() == total

        taken = run_in_threads(lambda _: len(queue.drain(CALLS)))
        assert sum(taken) == total
        assert queue.len() == 0

    def test_message_queue_keeps_every_message(self):
        total = THREADS * CALLS
        queue = RustPriorityMessageQueue(max_size=total, batch_size=50)
        accepted = run_in_threads(
            lambda index: sum(
                queue.enqueue({"i": i}, (1, 3, 5, 8, 10)[i % 5]) for i in range(CALLS)
            )
        )
        assert sum(accepted) == total
        assert queue.size == total

        def consume(_):
            taken = 0
            while batch := queue.dequeue_batch():
                taken += len(batch)
            return taken

        assert sum(run_in_threads(consume)) == total
        assert queue.size == 0

    def test_compression_manager_counts_every_call(self):
        manager = RustCompressionManager()
        payload = b"wse " * 1024
        calls = CALLS // 4
        run_in_threads(lambda _: [manager.compress(payload) for _ in range(calls)])
        stats = manager.get_stats()
        assert stats["total_compressed"] == THREADS * calls
        assert stats["compression_failures"] == 0


# ---------------------------------------------------------------------------
# Free-threaded build only
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not FREE_THREADED, reason="needs a free-threaded build of Python")
def test_import_keeps_the_gil_disabled():
    """Importing the extension must not turn the GIL back on.

    A module that does not declare free-threading support makes CPython enable
    the GIL with a RuntimeWarning; -W error turns that warning into a failure.
    """
    code = "import sys, wse_server; sys.exit(1 if sys._is_gil_enabled() else 0)"
    result = subprocess.run(
        [sys.executable, "-W", "error::RuntimeWarning", "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
