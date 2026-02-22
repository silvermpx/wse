"""Multi-Process WSE Benchmark.

Stress-tests the Rust WSE server with N parallel processes.
Measures true aggregate server capacity under concurrent load.

Tests:
  1. Connection Latency   — N workers connecting simultaneously
  2. Ping/Pong RTT        — N persistent connections pinging
  3. Aggregate Throughput  — N workers sending as fast as possible
  4. Rapid Connect/Disconnect — N workers cycling connections
  5. Message Size Impact   — N workers at various payload sizes
  6. Binary Frame          — N workers sending binary frames
  7. Sustained Load        — N workers for duration
  8. MsgPack Throughput    — N workers sending msgpack
  9. MsgPack Sustained     — N workers sustained msgpack

Usage:
    python benchmarks/bench_wse_multiprocess.py --token <JWT>       # 16 workers
    python benchmarks/bench_wse_multiprocess.py --token <JWT> --workers 64
    python benchmarks/bench_wse_multiprocess.py --token <JWT> --duration 10

    # With login (requires running app with auth endpoint):
    python benchmarks/bench_wse_multiprocess.py --email user@test.com --password pass

    # Generate token from bench_server.py:
    python benchmarks/bench_server.py --token-only
"""

import argparse
import asyncio
import json
import multiprocessing
import os
import statistics
import sys
import time
import zlib
from datetime import datetime

import msgpack
import websockets

# -- Config -------------------------------------------------------------------

WS_HOST = "127.0.0.1"
WS_PORT = int(os.environ.get("WSE_RUST_PORT", "5006"))
TIMEOUT = 10


# -- Helpers ------------------------------------------------------------------


def decode_wse(msg) -> dict | None:
    if isinstance(msg, bytes):
        if msg.startswith(b"C:"):
            msg = zlib.decompress(msg[2:]).decode()
        elif msg.startswith(b"E:"):
            return None
        else:
            msg = msg.decode()
    if isinstance(msg, str):
        if msg.startswith("WSE"):
            msg = msg[3:]
        return json.loads(msg)
    return None


def rust_uri(fmt: str = "json") -> str:
    base = f"ws://{WS_HOST}:{WS_PORT}/wse?compression=false&protocol_version=1"
    if fmt == "msgpack":
        base += "&format=msgpack"
    return base


def cookie_hdr(token: str) -> dict:
    return {"Cookie": f"access_token={token}"}


async def connect_ws(
    token: str, retries: int = 3, fmt: str = "json"
) -> websockets.WebSocketClientProtocol:
    """Connect and wait for server_ready, with retry on failure."""
    for attempt in range(retries):
        try:
            ws = await websockets.connect(
                rust_uri(fmt),
                additional_headers=cookie_hdr(token),
                open_timeout=TIMEOUT,
                max_size=2**20,
            )
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=TIMEOUT)
                data = decode_wse(msg)
                if isinstance(data, dict) and data.get("t") == "server_ready":
                    return ws
        except Exception:
            if attempt < retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
            else:
                raise


async def drain_buffered(ws, timeout: float = 0.5):
    """Drain any buffered messages (snapshots, subscriptions) after connect."""
    try:
        while True:
            await asyncio.wait_for(ws.recv(), timeout=timeout)
    except TimeoutError:
        pass


async def async_wait_start(start_event: multiprocessing.Event, ws=None, timeout: float = 30.0):
    """Wait for start signal without blocking the event loop."""
    deadline = time.perf_counter() + timeout
    while not start_event.is_set():
        if time.perf_counter() > deadline:
            break
        if ws is not None:
            try:
                await asyncio.wait_for(ws.recv(), timeout=0.05)
            except TimeoutError:
                pass
            except websockets.ConnectionClosed:
                raise
        else:
            await asyncio.sleep(0.01)


def fmt(v: float) -> str:
    return f"{v:,.2f}"


def print_header(num: int, title: str, detail: str = ""):
    print(f"\n{'=' * 60}")
    print(f"  Test {num}: {title}")
    if detail:
        print(f"  {detail}")
    print(f"{'=' * 60}")


def print_latency_stats(values: list[float], unit: str = "ms"):
    if not values:
        print("  NO DATA")
        return
    s = sorted(values)
    print(f"    Mean:   {fmt(statistics.mean(s))} {unit}")
    print(f"    Median: {fmt(statistics.median(s))} {unit}")
    if len(s) >= 20:
        print(f"    p95:    {fmt(s[int(len(s) * 0.95)])} {unit}")
        print(f"    p99:    {fmt(s[min(int(len(s) * 0.99), len(s) - 1)])} {unit}")
    print(f"    Min:    {fmt(min(s))} {unit}")
    print(f"    Max:    {fmt(max(s))} {unit}")


# =============================================================================
# Worker process
# =============================================================================


def worker_fn(
    worker_id: int,
    token: str,
    test_name: str,
    test_params: dict,
    result_queue: multiprocessing.Queue,
    ready_counter: multiprocessing.Value,
    start_event: multiprocessing.Event,
):
    """Worker: connects, signals ready, waits for GO, runs test."""

    async def _run():
        try:
            # Stagger connections to avoid overwhelming the server
            await asyncio.sleep(worker_id * 0.05)

            if test_name == "connect_latency":
                rounds = test_params.get("rounds", 5)
                with ready_counter.get_lock():
                    ready_counter.value += 1
                await async_wait_start(start_event)

                latencies = []
                for _ in range(rounds):
                    t0 = time.perf_counter()
                    try:
                        async with websockets.connect(
                            rust_uri(),
                            additional_headers=cookie_hdr(token),
                            open_timeout=TIMEOUT,
                        ) as ws:
                            while True:
                                msg = await asyncio.wait_for(ws.recv(), timeout=TIMEOUT)
                                data = decode_wse(msg)
                                if isinstance(data, dict) and data.get("t") == "server_ready":
                                    latencies.append((time.perf_counter() - t0) * 1000)
                                    break
                    except Exception:
                        pass
                result_queue.put({"worker_id": worker_id, "latencies": latencies})

            elif test_name == "ping_rtt":
                rounds = test_params.get("rounds", 20)
                ws = await connect_ws(token)
                await drain_buffered(ws)
                with ready_counter.get_lock():
                    ready_counter.value += 1
                await async_wait_start(start_event, ws)

                latencies = []
                for _ in range(rounds):
                    t0 = time.perf_counter()
                    pong = await ws.ping()
                    await pong
                    latencies.append((time.perf_counter() - t0) * 1000)
                await ws.close()
                result_queue.put({"worker_id": worker_id, "latencies": latencies})

            elif test_name in ("throughput", "throughput_msgpack"):
                n = test_params.get("n", 5000)
                use_msgpack = test_name == "throughput_msgpack"
                if use_msgpack:
                    msg = msgpack.packb({"t": "bench_noop", "p": {"w": worker_id}})
                else:
                    msg = json.dumps({"t": "bench_noop", "p": {"w": worker_id}})
                ws = await connect_ws(token, fmt="msgpack" if use_msgpack else "json")
                await drain_buffered(ws)
                with ready_counter.get_lock():
                    ready_counter.value += 1
                await async_wait_start(start_event, ws)

                t0 = time.perf_counter()
                for _ in range(n):
                    await ws.send(msg)
                elapsed = time.perf_counter() - t0
                await ws.close()
                result_queue.put({"worker_id": worker_id, "count": n, "elapsed": elapsed})

            elif test_name == "rapid_connect":
                cycles = test_params.get("cycles", 5)
                with ready_counter.get_lock():
                    ready_counter.value += 1
                await async_wait_start(start_event)

                times = []
                for _ in range(cycles):
                    t0 = time.perf_counter()
                    try:
                        async with websockets.connect(
                            rust_uri(),
                            additional_headers=cookie_hdr(token),
                            open_timeout=TIMEOUT,
                        ) as ws:
                            while True:
                                msg = await asyncio.wait_for(ws.recv(), timeout=TIMEOUT)
                                data = decode_wse(msg)
                                if isinstance(data, dict) and data.get("t") == "server_ready":
                                    times.append((time.perf_counter() - t0) * 1000)
                                    break
                    except Exception:
                        pass
                result_queue.put({"worker_id": worker_id, "latencies": times})

            elif test_name == "message_size":
                size = test_params["size"]
                n = test_params.get("n", 300)
                payload = json.dumps({"t": "ping", "p": {"d": "x" * size}})
                actual_bytes = len(payload.encode())
                ws = await connect_ws(token)
                await drain_buffered(ws)
                with ready_counter.get_lock():
                    ready_counter.value += 1
                await async_wait_start(start_event, ws)

                t0 = time.perf_counter()
                for _ in range(n):
                    await ws.send(payload)
                elapsed = time.perf_counter() - t0
                await ws.close()
                result_queue.put(
                    {
                        "worker_id": worker_id,
                        "size": actual_bytes,
                        "count": n,
                        "elapsed": elapsed,
                    }
                )

            elif test_name == "binary":
                n = test_params.get("n", 1000)
                payload = json.dumps({"t": "ping", "p": {"d": "B" * 1024}}).encode()
                ws = await connect_ws(token)
                await drain_buffered(ws)
                with ready_counter.get_lock():
                    ready_counter.value += 1
                await async_wait_start(start_event, ws)

                t0 = time.perf_counter()
                for _ in range(n):
                    await ws.send(payload)
                elapsed = time.perf_counter() - t0
                await ws.close()
                result_queue.put(
                    {
                        "worker_id": worker_id,
                        "count": n,
                        "elapsed": elapsed,
                        "bytes_per_msg": len(payload),
                    }
                )

            elif test_name in ("sustained", "sustained_msgpack"):
                duration_s = test_params.get("duration", 5.0)
                use_msgpack = test_name == "sustained_msgpack"
                if use_msgpack:
                    msg = msgpack.packb({"t": "bench_noop", "p": {"w": worker_id}})
                else:
                    msg = json.dumps({"t": "bench_noop", "p": {"w": worker_id}})
                ws = await connect_ws(token, fmt="msgpack" if use_msgpack else "json")
                await drain_buffered(ws)
                with ready_counter.get_lock():
                    ready_counter.value += 1
                await async_wait_start(start_event, ws)

                total_sent = 0
                per_second = []
                t_start = time.perf_counter()
                interval_start = t_start
                interval_count = 0
                while True:
                    now = time.perf_counter()
                    if now - t_start >= duration_s:
                        break
                    await ws.send(msg)
                    total_sent += 1
                    interval_count += 1
                    if now - interval_start >= 1.0:
                        per_second.append(interval_count)
                        interval_count = 0
                        interval_start = now
                if interval_count > 0:
                    per_second.append(interval_count)
                elapsed = time.perf_counter() - t_start
                await ws.close()
                result_queue.put(
                    {
                        "worker_id": worker_id,
                        "total_sent": total_sent,
                        "elapsed": elapsed,
                        "per_second": per_second,
                    }
                )

        except Exception as e:
            result_queue.put({"worker_id": worker_id, "error": str(e)})

    asyncio.run(_run())


def run_parallel_test(n_workers: int, token: str, test_name: str, test_params: dict) -> list[dict]:
    """Spawn N workers, wait for all to be ready, fire GO, collect results."""
    result_queue = multiprocessing.Queue()
    ready_counter = multiprocessing.Value("i", 0)
    start_event = multiprocessing.Event()

    processes = []
    for i in range(n_workers):
        p = multiprocessing.Process(
            target=worker_fn,
            args=(
                i,
                token,
                test_name,
                test_params,
                result_queue,
                ready_counter,
                start_event,
            ),
        )
        p.start()
        processes.append(p)

    # Wait for workers to signal ready (or timeout)
    deadline = time.time() + 30.0
    while time.time() < deadline:
        with ready_counter.get_lock():
            if ready_counter.value >= n_workers:
                break
        time.sleep(0.05)

    with ready_counter.get_lock():
        n_ready = ready_counter.value
    if n_ready < n_workers:
        print(f"  WARNING: Only {n_ready}/{n_workers} workers ready, starting anyway")

    # Small sleep to let last worker's barrier settle
    time.sleep(0.05)
    start_event.set()

    timeout = test_params.get("duration", 10) + 30
    for p in processes:
        p.join(timeout=timeout)

    results = []
    while not result_queue.empty():
        results.append(result_queue.get_nowait())
    results.sort(key=lambda r: r.get("worker_id", 0))
    return results


# =============================================================================
# Aggregate helpers
# =============================================================================


def aggregate_throughput(results: list[dict], n_workers: int) -> tuple[int, float, float, int, int]:
    """Returns (total_msgs, wall_clock_rate, max_elapsed, ok, errors)."""
    total = 0
    max_elapsed = 0.0
    errors = 0
    for r in results:
        if r.get("error"):
            errors += 1
        else:
            count = r.get("count", r.get("total_sent", 0))
            total += count
            elapsed = r.get("elapsed", 0)
            if elapsed > max_elapsed:
                max_elapsed = elapsed
    ok = n_workers - errors
    rate = total / max_elapsed if max_elapsed > 0 else 0
    return total, rate, max_elapsed, ok, errors


# =============================================================================
# Test runners
# =============================================================================


def run_test_1(n_workers: int, token: str):
    print_header(1, "Connection Latency", f"{n_workers} workers x 5 rounds each")
    results = run_parallel_test(n_workers, token, "connect_latency", {"rounds": 5})

    all_lat = []
    errors = []
    for r in results:
        if r.get("error"):
            errors.append(r)
        else:
            all_lat.extend(r.get("latencies", []))

    print(f"  Workers: {n_workers - len(errors)}/{n_workers} OK, {len(all_lat)} measurements")
    print_latency_stats(all_lat)
    if errors:
        print(f"  First error: {errors[0].get('error', '?')}")


def run_test_2(n_workers: int, token: str):
    print_header(2, "Ping/Pong RTT", f"{n_workers} concurrent connections x 20 pings")
    results = run_parallel_test(n_workers, token, "ping_rtt", {"rounds": 20})

    all_lat = []
    errors = []
    for r in results:
        if r.get("error"):
            errors.append(r)
        else:
            all_lat.extend(r.get("latencies", []))

    print(f"  Workers: {n_workers - len(errors)}/{n_workers} OK, {len(all_lat)} measurements")
    print_latency_stats(all_lat)
    if errors:
        print(f"  First error: {errors[0].get('error', '?')}")


def run_test_3(n_workers: int, token: str):
    print_header(3, "Aggregate Throughput", f"{n_workers} workers x 10,000 msgs")
    results = run_parallel_test(n_workers, token, "throughput", {"n": 10000})

    total, rate, max_elapsed, ok, n_err = aggregate_throughput(results, n_workers)
    print(f"  Workers:        {ok}/{n_workers} OK")
    print(f"  Total messages: {total:,}")
    print(f"  Wall time:      {max_elapsed:.3f}s")
    print(f"  Aggregate rate: {rate:,.0f} msg/sec")
    if ok:
        print(f"  Per worker avg: {rate / ok:,.0f} msg/sec")
    if n_err:
        errs = [r for r in results if r.get("error")]
        print(f"  First error: {errs[0].get('error', '?')}")


def run_test_4(n_workers: int, token: str):
    print_header(4, "Rapid Connect/Disconnect", f"{n_workers} workers x 5 cycles")
    results = run_parallel_test(n_workers, token, "rapid_connect", {"cycles": 5})

    all_lat = []
    errors = []
    for r in results:
        if r.get("error"):
            errors.append(r)
        else:
            all_lat.extend(r.get("latencies", []))

    print(f"  Workers: {n_workers - len(errors)}/{n_workers} OK, {len(all_lat)} cycles")
    print_latency_stats(all_lat)
    if errors:
        print(f"  First error: {errors[0].get('error', '?')}")


def run_test_5(n_workers: int, token: str):
    print_header(5, "Message Size Impact", f"{n_workers} workers per size")

    size_configs = [
        (64, 5000),
        (256, 5000),
        (1024, 3000),
        (4096, 2000),
        (16384, 1000),
        (65536, 500),
    ]

    for size, n_msgs in size_configs:
        results = run_parallel_test(n_workers, token, "message_size", {"size": size, "n": n_msgs})
        total, rate, max_elapsed, ok, n_err = aggregate_throughput(results, n_workers)
        actual_bytes = 0
        for r in results:
            if not r.get("error") and r.get("size"):
                actual_bytes = r["size"]
                break

        if ok and actual_bytes and max_elapsed > 0:
            mb_s = (actual_bytes * rate) / 1024 / 1024
            print(
                f"  {actual_bytes:>6} bytes: {rate:>10,.0f} msg/s | "
                f"{mb_s:>7.1f} MB/s | {ok}/{n_workers} workers | {max_elapsed:.3f}s"
            )
        else:
            errs = [r for r in results if r.get("error")]
            err_msg = errs[0].get("error", "?") if errs else "timeout"
            print(f"  {size:>6} bytes: FAILED ({n_err} errors: {err_msg[:60]})")


def run_test_6(n_workers: int, token: str):
    print_header(6, "Binary Frame Throughput", f"{n_workers} workers x 5,000 msgs")
    results = run_parallel_test(n_workers, token, "binary", {"n": 5000})

    total, rate, max_elapsed, ok, n_err = aggregate_throughput(results, n_workers)
    bpm = 0
    for r in results:
        if not r.get("error") and r.get("bytes_per_msg"):
            bpm = r["bytes_per_msg"]
            break

    mb_s = (bpm * rate) / 1024 / 1024 if bpm else 0
    print(f"  Workers:        {ok}/{n_workers} OK")
    print(f"  Total messages: {total:,}")
    print(f"  Wall time:      {max_elapsed:.3f}s")
    print(f"  Aggregate rate: {rate:,.0f} msg/sec")
    print(f"  Throughput:     {mb_s:.1f} MB/s")


def run_test_7(n_workers: int, token: str, duration: float):
    print_header(7, "Sustained Aggregate Load", f"{n_workers} workers x {duration}s")
    results = run_parallel_test(n_workers, token, "sustained", {"duration": duration})

    total_msgs = 0
    max_elapsed = 0.0
    all_per_second = []
    errors = []
    for r in results:
        if r.get("error"):
            errors.append(r)
        else:
            total_msgs += r["total_sent"]
            if r["elapsed"] > max_elapsed:
                max_elapsed = r["elapsed"]
            if r.get("per_second"):
                all_per_second.append(r["per_second"])

    ok = n_workers - len(errors)
    rate = total_msgs / max_elapsed if max_elapsed > 0 else 0
    print(f"  Workers:        {ok}/{n_workers} OK")
    print(f"  Total messages: {total_msgs:,}")
    print(f"  Wall time:      {max_elapsed:.2f}s")
    print(f"  Aggregate rate: {rate:,.0f} msg/sec")
    if ok:
        print(f"  Per worker avg: {rate / ok:,.0f} msg/sec")

    if all_per_second:
        max_secs = max(len(ps) for ps in all_per_second)
        agg = []
        print("\n  Per-second aggregate:")
        for si in range(max_secs):
            sec_total = sum(ps[si] for ps in all_per_second if si < len(ps))
            agg.append(sec_total)
            bar = "#" * min(int(sec_total / 5000), 60)
            print(f"    [{si + 1}s] {sec_total:>10,} msg/s  {bar}")
        if len(agg) > 1:
            mean = statistics.mean(agg)
            stddev = statistics.stdev(agg)
            print(f"  Mean:   {mean:,.0f} msg/s")
            print(f"  Stddev: {stddev:,.0f} msg/s ({stddev / mean * 100:.1f}%)")

    if errors:
        print(f"\n  WARNING: {len(errors)} worker(s) failed")
        print(f"  First error: {errors[0].get('error', '?')}")


def run_test_8(n_workers: int, token: str):
    print_header(8, "MsgPack Throughput", f"{n_workers} workers x 10,000 msgs (msgpack)")
    results = run_parallel_test(n_workers, token, "throughput_msgpack", {"n": 10000})

    total, rate, max_elapsed, ok, n_err = aggregate_throughput(results, n_workers)
    print(f"  Workers:        {ok}/{n_workers} OK")
    print(f"  Total messages: {total:,}")
    print(f"  Wall time:      {max_elapsed:.3f}s")
    print(f"  Aggregate rate: {rate:,.0f} msg/sec")
    if ok:
        print(f"  Per worker avg: {rate / ok:,.0f} msg/sec")
    if n_err:
        errs = [r for r in results if r.get("error")]
        print(f"  First error: {errs[0].get('error', '?')}")


def run_test_9(n_workers: int, token: str, duration: float):
    print_header(
        9,
        "MsgPack Sustained Load",
        f"{n_workers} workers x {duration}s (msgpack)",
    )
    results = run_parallel_test(n_workers, token, "sustained_msgpack", {"duration": duration})

    total_msgs = 0
    max_elapsed = 0.0
    all_per_second = []
    errors = []
    for r in results:
        if r.get("error"):
            errors.append(r)
        else:
            total_msgs += r["total_sent"]
            if r["elapsed"] > max_elapsed:
                max_elapsed = r["elapsed"]
            if r.get("per_second"):
                all_per_second.append(r["per_second"])

    ok = n_workers - len(errors)
    rate = total_msgs / max_elapsed if max_elapsed > 0 else 0
    print(f"  Workers:        {ok}/{n_workers} OK")
    print(f"  Total messages: {total_msgs:,}")
    print(f"  Wall time:      {max_elapsed:.2f}s")
    print(f"  Aggregate rate: {rate:,.0f} msg/sec")
    if ok:
        print(f"  Per worker avg: {rate / ok:,.0f} msg/sec")

    if all_per_second:
        max_secs = max(len(ps) for ps in all_per_second)
        agg = []
        print("\n  Per-second aggregate:")
        for si in range(max_secs):
            sec_total = sum(ps[si] for ps in all_per_second if si < len(ps))
            agg.append(sec_total)
            bar = "#" * min(int(sec_total / 5000), 60)
            print(f"    [{si + 1}s] {sec_total:>10,} msg/s  {bar}")
        if len(agg) > 1:
            mean = statistics.mean(agg)
            stddev = statistics.stdev(agg)
            print(f"  Mean:   {mean:,.0f} msg/s")
            print(f"  Stddev: {stddev:,.0f} msg/s ({stddev / mean * 100:.1f}%)")

    if errors:
        print(f"\n  WARNING: {len(errors)} worker(s) failed")
        print(f"  First error: {errors[0].get('error', '?')}")


# =============================================================================
# Main
# =============================================================================


def main():
    parser = argparse.ArgumentParser(description="Multi-Process WSE Benchmark")
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Parallel workers for every test (default: 16)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=5.0,
        help="Duration for sustained test (default: 5s)",
    )
    parser.add_argument(
        "--token",
        type=str,
        default=None,
        help="Pre-generated JWT token (skip HTTP login)",
    )
    parser.add_argument("--host", type=str, default=None, help="Override WS_HOST")
    parser.add_argument("--port", type=int, default=None, help="Override WS_PORT")
    parser.add_argument("--email", type=str, default=None, help="Login email (if no --token)")
    parser.add_argument("--password", type=str, default=None, help="Login password (if no --token)")
    parser.add_argument(
        "--api-port",
        type=int,
        default=5005,
        help="API port for HTTP login (default: 5005)",
    )
    args = parser.parse_args()

    global WS_HOST, WS_PORT
    if args.host:
        WS_HOST = args.host
    if args.port:
        WS_PORT = args.port

    n = args.workers

    print("=" * 60)
    print("  WSE Benchmark (Multi-Process)")
    print(f"  Server:   ws://{WS_HOST}:{WS_PORT}")
    print(f"  Workers:  {n} parallel processes per test")
    print(f"  Duration: {args.duration}s (sustained test)")
    print(f"  Time:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    if args.token:
        token = args.token
        print(f"\n  Using provided token: {token[:20]}...")
    elif args.email and args.password:
        import httpx as _httpx

        print("\n  Authenticating via HTTP login...")

        async def _login():
            url = f"http://{WS_HOST}:{args.api_port}/auth/login"
            async with _httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(url, json={"email": args.email, "password": args.password})
                if resp.status_code == 200:
                    data = resp.json()
                    t = data.get("access_token") or data.get("token")
                    if t:
                        return t
                    for cookie in resp.cookies.jar:
                        if cookie.name == "access_token":
                            return cookie.value
                return None

        token = asyncio.run(_login())
        if not token:
            print("  FATAL: Cannot get JWT token. Check credentials.")
            sys.exit(1)
        print(f"  Token: {token[:20]}...")
    else:
        print(
            "\n  FATAL: Provide --token or (--email + --password)."
            "\n  Generate token: python benchmarks/bench_server.py --token-only"
        )
        sys.exit(1)

    tests = [
        lambda: run_test_1(n, token),
        lambda: run_test_2(n, token),
        lambda: run_test_3(n, token),
        lambda: run_test_4(n, token),
        lambda: run_test_5(n, token),
        lambda: run_test_6(n, token),
        lambda: run_test_7(n, token, args.duration),
        lambda: run_test_8(n, token),
        lambda: run_test_9(n, token, args.duration),
    ]
    for i, test_fn in enumerate(tests):
        test_fn()
        if i < len(tests) - 1:
            time.sleep(2.0)

    print(f"\n{'=' * 60}")
    print("  Benchmark complete")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
