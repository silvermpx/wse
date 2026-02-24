"""WSE Brutal Concurrency Benchmark.

Determines absolute server limits under extreme concurrent load.
Uses asyncio for thousands of connections per process (not 1 process per connection).

Tests:
  1. Connection Storm    -- how fast can server accept N connections?
  2. Ping/Pong Latency   -- p50/p95/p99/p99.9 at each concurrency level
  3. Throughput Saturation -- msg/s degradation curve as connections increase
  4. Sustained Hold      -- can N connections stay alive for M seconds?
  5. orjson vs json      -- serialization benchmark (194-byte trading payload)
  6. Message Size Impact -- throughput at 64B to 64KB payload sizes
  7. Format Throughput   -- JSON vs msgpack over actual WebSocket connections

Designed for AMD EPYC 7502P (64 threads, ulimit 65536).

Usage:
    python benchmarks/bench_server.py --token-only   # get JWT
    python benchmarks/bench_server.py --max-connections 30000 &

    python benchmarks/bench_brutal.py --token <JWT>
    python benchmarks/bench_brutal.py --token <JWT> --tiers 100,500,1000,5000,10000,20000
    python benchmarks/bench_brutal.py --token <JWT> --tiers 100,500 --duration 3  # quick
"""

import argparse
import asyncio
import json
import multiprocessing
import os
import statistics
import sys
import threading
import time
import zlib
from datetime import datetime

import websockets
from websockets.sync.client import connect as sync_ws_connect

try:
    import msgpack as _msgpack
    HAS_MSGPACK = True
except ImportError:
    HAS_MSGPACK = False

try:
    import orjson as _orjson
    HAS_ORJSON = True
except ImportError:
    HAS_ORJSON = False

# -- Config -------------------------------------------------------------------

WS_HOST = "127.0.0.1"
WS_PORT = int(os.environ.get("WSE_RUST_PORT", "5006"))
TIMEOUT = 15

DEFAULT_TIERS = [100, 500, 1000, 5000, 10000, 15000]
DEFAULT_TIERS_HEAVY = [100, 500, 1000]

SMALL_PAYLOAD = {
    "t": "trade_update",
    "p": {
        "symbol": "ES",
        "side": "buy",
        "price": 5234.75,
        "qty": 2,
        "ts": "2026-02-23T14:30:00.123Z",
        "account": "acc_01HQ3",
        "tags": ["momentum", "breakout"],
        "score": 0.87,
    },
}
SMALL_JSON = json.dumps(SMALL_PAYLOAD)
SMALL_SIZE = len(SMALL_JSON.encode())

# Msgpack payload: M: prefix + msgpack bytes (WSE binary protocol)
if HAS_MSGPACK:
    SMALL_MSGPACK = b"M:" + _msgpack.packb(SMALL_PAYLOAD)
    SMALL_MSGPACK_SIZE = len(SMALL_MSGPACK)
else:
    SMALL_MSGPACK = None
    SMALL_MSGPACK_SIZE = 0


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


def ws_uri() -> str:
    return f"ws://{WS_HOST}:{WS_PORT}/wse?compression=false&protocol_version=1"


def cookie_hdr(token: str) -> dict:
    return {"Cookie": f"access_token={token}"}


def pct(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = int(len(sorted_vals) * p / 100)
    return sorted_vals[min(idx, len(sorted_vals) - 1)]


def flush():
    sys.stdout.flush()


def print_header(title: str, detail: str = ""):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    if detail:
        print(f"  {detail}")
    print(f"{'=' * 70}")
    flush()


def print_latency(vals: list[float], unit: str = "ms"):
    if not vals:
        print("    NO DATA")
        return
    s = sorted(vals)
    print(f"    p50:    {pct(s, 50):>10.2f} {unit}")
    print(f"    p95:    {pct(s, 95):>10.2f} {unit}")
    print(f"    p99:    {pct(s, 99):>10.2f} {unit}")
    if len(s) >= 100:
        print(f"    p99.9:  {pct(s, 99.9):>10.2f} {unit}")
    print(f"    min:    {min(s):>10.2f} {unit}")
    print(f"    max:    {max(s):>10.2f} {unit}")
    print(f"    mean:   {statistics.mean(s):>10.2f} {unit}")


# -- Async connection helper --------------------------------------------------


async def connect_one(token: str):
    """Connect and wait for server_ready."""
    ws = await websockets.connect(
        ws_uri(),
        additional_headers=cookie_hdr(token),
        open_timeout=TIMEOUT,
        max_size=2**20,
        write_limit=2**20,
        ping_interval=None,
    )
    while True:
        msg = await asyncio.wait_for(ws.recv(), timeout=TIMEOUT)
        data = decode_wse(msg)
        if isinstance(data, dict) and data.get("t") == "server_ready":
            return ws


async def connect_batch(token: str, n: int, batch_size: int = 100) -> list:
    """Connect N websockets in batches to avoid handshake storm."""
    connections = []
    for start in range(0, n, batch_size):
        end = min(start + batch_size, n)
        chunk_size = end - start
        tasks = [connect_one(token) for _ in range(chunk_size)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if not isinstance(r, BaseException):
                connections.append(r)
        # Brief pause between batches
        if end < n:
            await asyncio.sleep(0.05)
        sys.stdout.write(f"\r    Connecting: {len(connections)}/{n}...")
        sys.stdout.flush()
    print(f"\r    Connected: {len(connections)}/{n}            ")
    return connections


async def close_all(connections: list):
    """Close all connections gracefully."""
    tasks = []
    for ws in connections:
        tasks.append(ws.close())
    await asyncio.gather(*tasks, return_exceptions=True)


# =============================================================================
# Test 1: Connection Storm
# =============================================================================


async def test_connection_storm(tiers: list[int], token: str):
    print_header(
        "TEST 1: Connection Storm",
        "All connections opened as fast as possible per tier. Measures accept rate."
    )

    for n in tiers:
        print(f"\n  --- {n} connections ---")

        latencies = []
        errors = 0
        connections = []

        t_total_start = time.perf_counter()

        # Open connections in batches, measure each
        batch_size = min(200, n)
        for start in range(0, n, batch_size):
            end = min(start + batch_size, n)
            chunk = end - start

            async def _connect_timed():
                t0 = time.perf_counter()
                try:
                    ws = await connect_one(token)
                    lat = (time.perf_counter() - t0) * 1000
                    return ws, lat
                except Exception:
                    return None, None

            tasks = [_connect_timed() for _ in range(chunk)]
            results = await asyncio.gather(*tasks)
            for ws, lat in results:
                if ws is not None:
                    connections.append(ws)
                    latencies.append(lat)
                else:
                    errors += 1

            sys.stdout.write(f"\r    Progress: {len(connections)}/{n}...")
            sys.stdout.flush()
            if end < n:
                await asyncio.sleep(0.02)

        t_total = time.perf_counter() - t_total_start
        latencies.sort()

        print(f"\r    Connected: {len(connections)}/{n} ({errors} failed)")
        print(f"    Total time: {t_total:.2f}s")
        if latencies:
            rate = len(latencies) / t_total
            print(f"    Accept rate: {rate:,.0f} conn/s")
            print_latency(latencies)

        await close_all(connections)
        print()
        flush()


# =============================================================================
# Test 2: Ping/Pong Latency Under Load
# =============================================================================


async def test_echo_latency(tiers: list[int], token: str):
    print_header(
        "TEST 2: Ping/Pong Latency Under Concurrent Load",
        "N persistent connections, each pinging 20x. Measures tail latency."
    )

    for n in tiers:
        print(f"\n  --- {n} concurrent connections ---")
        connections = await connect_batch(token, n)

        if not connections:
            print("    FAILED: no connections")
            continue

        # Each connection pings 20 times
        async def _ping_many(ws, rounds=20):
            lats = []
            for _ in range(rounds):
                try:
                    t0 = time.perf_counter()
                    pong = await ws.ping()
                    await asyncio.wait_for(pong, timeout=5)
                    lats.append((time.perf_counter() - t0) * 1000)
                except Exception:
                    break
            return lats

        tasks = [_ping_many(ws) for ws in connections]
        results = await asyncio.gather(*tasks)

        all_lat = []
        for lats in results:
            all_lat.extend(lats)
        all_lat.sort()

        print(f"    Workers: {len(connections)}, {len(all_lat)} measurements")
        print_latency(all_lat)

        await close_all(connections)
        print()
        flush()


# =============================================================================
# Test 3: Throughput Saturation (1 process per connection)
# =============================================================================


def _throughput_worker(worker_id, host, port, token, duration, result_queue, start_event, payload_bytes=None, ready_counter=None):
    """One process = one sync websocket, tight send loop. True parallelism."""
    time.sleep(worker_id * 0.003)  # stagger connections (3ms per worker)

    uri = f"ws://{host}:{port}/wse?compression=false&protocol_version=1"
    payload = payload_bytes if payload_bytes is not None else SMALL_JSON
    try:
        ws = sync_ws_connect(
            uri,
            additional_headers={"Cookie": f"access_token={token}"},
            open_timeout=60,
        )
        while True:
            msg = ws.recv(timeout=30)
            if isinstance(msg, str) and "server_ready" in msg:
                break
    except Exception as e:
        result_queue.put({"id": worker_id, "count": 0, "elapsed": 0, "error": str(e)})
        return

    # Signal that this worker is connected and ready
    if ready_counter is not None:
        with ready_counter.get_lock():
            ready_counter.value += 1

    start_event.wait(timeout=180)

    count = 0
    send_error = ""
    t0 = time.perf_counter()
    end = t0 + duration
    try:
        while time.perf_counter() < end:
            ws.send(payload)
            count += 1
    except Exception as e:
        send_error = f"{type(e).__name__}: {e}"[:200]
    elapsed = time.perf_counter() - t0

    try:
        ws.close()
    except Exception:
        pass
    result = {"id": worker_id, "count": count, "elapsed": elapsed}
    if count == 0:
        result["error"] = send_error or "count=0 but no exception caught"
    result_queue.put(result)


def _throughput_worker_async(proc_id, n_conns, host, port, token, duration, result_queue, start_event, payload_bytes=None):
    """One process manages N async websocket connections. For high concurrency (2K+).

    Uses asyncio to multiplex many connections per process, avoiding the OS limit
    of 1-process-per-connection. Each sender yields to the event loop every 20
    messages to prevent starvation.
    """
    payload = payload_bytes if payload_bytes is not None else SMALL_JSON

    async def _run():
        uri = f"ws://{host}:{port}/wse?compression=false&protocol_version=1"
        headers = {"Cookie": f"access_token={token}"}

        # Connect all websockets in batches
        connections = []
        batch = 100
        for s in range(0, n_conns, batch):
            e = min(s + batch, n_conns)

            async def _connect():
                ws = await websockets.connect(
                    uri, additional_headers=headers,
                    open_timeout=30, max_size=2**20, ping_interval=None,
                )
                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=15)
                    if isinstance(msg, str) and "server_ready" in msg:
                        return ws

            tasks = [_connect() for _ in range(e - s)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if not isinstance(r, BaseException):
                    connections.append(r)
            if e < n_conns:
                await asyncio.sleep(0.05)

        ok_conns = len(connections)
        errs = n_conns - ok_conns

        # Wait for barrier
        start_event.wait(timeout=120)

        # Each connection runs a send coroutine with cooperative yielding
        counts = [0] * ok_conns

        async def _sender(idx, ws):
            end = time.perf_counter() + duration
            c = 0
            try:
                while time.perf_counter() < end:
                    await ws.send(payload)
                    c += 1
                    # Yield every 20 sends to prevent event loop starvation
                    if c % 20 == 0:
                        await asyncio.sleep(0)
            except Exception:
                pass
            counts[idx] = c

        t0 = time.perf_counter()
        await asyncio.gather(*[_sender(i, ws) for i, ws in enumerate(connections)])
        elapsed = time.perf_counter() - t0

        # Cleanup
        for ws in connections:
            try:
                await ws.close()
            except Exception:
                pass

        total = sum(counts)
        result_queue.put({
            "id": proc_id, "count": total, "elapsed": elapsed,
            "ok_conns": ok_conns, "err_conns": errs,
        })

    asyncio.run(_run())


# Threshold: above this, use async workers (multiple conns per process)
# On EPYC (128GB, 64 cores): ~4000 sync processes fit in RAM (~15-30MB each)
ASYNC_THRESHOLD = 5000


def _run_throughput_tier(n: int, token: str, duration: int, payload: str = None, label: str = ""):
    """Run throughput test for a single tier. Returns (rate, per_conn, mb_s, ok, errs, msg_size).

    Strategy:
      - n <= ASYNC_THRESHOLD: 1 process per connection (max per-conn throughput)
      - n > ASYNC_THRESHOLD: N_PROCS processes, each with n/N_PROCS async connections
    """
    msg_bytes = payload if payload is not None else SMALL_JSON
    msg_size = len(msg_bytes.encode() if isinstance(msg_bytes, str) else msg_bytes)

    if n > ASYNC_THRESHOLD:
        return _run_throughput_tier_async(n, token, duration, msg_bytes, msg_size, label)
    return _run_throughput_tier_sync(n, token, duration, msg_bytes, msg_size, label)


def _run_throughput_tier_sync(n: int, token: str, duration: int, msg_bytes, msg_size: int, label: str):
    """1 process per connection. Best throughput, limited to ~1500 connections."""
    result_queue = multiprocessing.Queue()
    start_event = multiprocessing.Event()
    ready_counter = multiprocessing.Value("i", 0)

    # Spawn in batches of 200
    processes = []
    batch = 200
    for s in range(0, n, batch):
        e = min(s + batch, n)
        for i in range(s, e):
            p = multiprocessing.Process(
                target=_throughput_worker,
                args=(i, WS_HOST, WS_PORT, token, duration, result_queue, start_event, msg_bytes, ready_counter),
                daemon=True,
            )
            p.start()
            processes.append(p)
        sys.stdout.write(f"\r    {label}Spawning: {len(processes)}/{n}...")
        sys.stdout.flush()
        if e < n:
            time.sleep(0.5)

    # Wait for ALL workers to connect (barrier pattern)
    # Max stagger: n * 3ms. Max handshake: ~60s. Total budget: stagger + 90s.
    max_wait = n * 0.003 + 90
    poll_interval = 0.5
    waited = 0
    while waited < max_wait:
        with ready_counter.get_lock():
            ready = ready_counter.value
        # Also count workers that already errored out (they put results in queue)
        alive = sum(1 for p in processes if p.is_alive())
        # All connected OR all processes done (connected + errored = total)
        if ready >= n or (ready + (n - alive)) >= n:
            break
        sys.stdout.write(f"\r    {label}{ready}/{n} connected ({waited:.0f}s)...        ")
        sys.stdout.flush()
        time.sleep(poll_interval)
        waited += poll_interval

    with ready_counter.get_lock():
        final_ready = ready_counter.value
    sys.stdout.write(f"\r    {label}{final_ready}/{n} connected ({waited:.0f}s), GO!        \n")
    sys.stdout.flush()

    start_event.set()
    time.sleep(duration + 30)

    for p in processes:
        if p.is_alive():
            p.kill()
    for p in processes:
        p.join(timeout=0.5)

    results = []
    while not result_queue.empty():
        results.append(result_queue.get_nowait())

    total_msgs = sum(r["count"] for r in results)
    max_elapsed = max((r["elapsed"] for r in results if r.get("elapsed", 0) > 0), default=1)
    ok = sum(1 for r in results if r["count"] > 0)
    zero_count = sum(1 for r in results if r["count"] == 0)
    missing = n - len(results)
    errs = n - ok

    if missing > 0 or zero_count > 0:
        sys.stdout.write(f"\r    {label}  [diag] results={len(results)}, ok={ok}, zero_count={zero_count}, missing/killed={missing}\n")
        sys.stdout.flush()

    # Show first few error details for debugging
    error_results = [r for r in results if r.get("error")]
    if error_results:
        sample = error_results[:3]
        for r in sample:
            sys.stdout.write(f"\r    {label}  [err worker {r['id']}] {r['error'][:120]}\n")
        if len(error_results) > 3:
            sys.stdout.write(f"\r    {label}  ... and {len(error_results) - 3} more errors\n")
        sys.stdout.flush()

    rate = total_msgs / max_elapsed if max_elapsed > 0 else 0
    per_conn = rate / ok if ok else 0
    mb_s = (total_msgs * msg_size) / max_elapsed / 1024 / 1024 if max_elapsed > 0 else 0

    return rate, per_conn, mb_s, ok, errs, msg_size


def _run_throughput_tier_async(n: int, token: str, duration: int, msg_bytes, msg_size: int, label: str):
    """Multiple async connections per process. Scales to 10K+ connections."""
    cpu = os.cpu_count() or 8
    n_procs = min(cpu, 32, n)  # one process per core, max 32
    conns_per_proc = n // n_procs
    remainder = n % n_procs

    result_queue = multiprocessing.Queue()
    start_event = multiprocessing.Event()

    processes = []
    for i in range(n_procs):
        c = conns_per_proc + (1 if i < remainder else 0)
        p = multiprocessing.Process(
            target=_throughput_worker_async,
            args=(i, c, WS_HOST, WS_PORT, token, duration, result_queue, start_event, msg_bytes),
            daemon=True,
        )
        p.start()
        processes.append(p)
        sys.stdout.write(f"\r    {label}Proc {i+1}/{n_procs} ({c} conns)...")
        sys.stdout.flush()

    # Wait for all processes to connect their websockets
    # Batched at 100/proc with 50ms gaps: (conns_per_proc/100)*0.05 + handshake overhead
    connect_est = (conns_per_proc / 100) * 0.05 + 15
    sys.stdout.write(f"\r    {label}{n_procs} procs x ~{conns_per_proc} conns = {n}, connecting ({connect_est:.0f}s)...")
    sys.stdout.flush()
    time.sleep(connect_est)

    start_event.set()
    time.sleep(duration + 20)

    for p in processes:
        if p.is_alive():
            p.kill()
    for p in processes:
        p.join(timeout=2)

    results = []
    while not result_queue.empty():
        results.append(result_queue.get_nowait())

    total_msgs = sum(r["count"] for r in results)
    max_elapsed = max((r["elapsed"] for r in results if r.get("elapsed", 0) > 0), default=1)
    ok_conns = sum(r.get("ok_conns", 0) for r in results)
    err_conns = sum(r.get("err_conns", 0) for r in results)

    rate = total_msgs / max_elapsed if max_elapsed > 0 else 0
    per_conn = rate / ok_conns if ok_conns else 0
    mb_s = (total_msgs * msg_size) / max_elapsed / 1024 / 1024 if max_elapsed > 0 else 0

    return rate, per_conn, mb_s, ok_conns, err_conns, msg_size


def test_throughput(tiers: list[int], token: str, duration: int):
    """Throughput saturation: 1 process per connection (true parallelism)."""
    print_header(
        "TEST 3: Throughput Saturation",
        f"1 process per connection, tight send loop. {SMALL_SIZE}-byte JSON msgs for {duration}s.\n"
        f"  Max practical: ~1000 connections on 64 cores (OS scheduling limit)."
    )

    print(f"\n  {'Conns':>8} | {'Mode':>5} | {'Total msg/s':>12} | {'Per-conn':>10} | {'MB/s':>8} | {'OK':>6} | {'Errors':>6}")
    print(f"  {'-'*8}-+-{'-'*5}-+-{'-'*12}-+-{'-'*10}-+-{'-'*8}-+-{'-'*6}-+-{'-'*6}")

    for n in tiers:
        mode = "async" if n > ASYNC_THRESHOLD else "sync"
        rate, per_conn, mb_s, ok, errs, _ = _run_throughput_tier(n, token, duration)
        print(f"\r  {n:>8,} | {mode:>5} | {rate:>12,.0f} | {per_conn:>10,.0f} | {mb_s:>8.1f} | {ok:>6} | {errs:>6}")
        flush()

    print()
    flush()


# =============================================================================
# Test 4: Sustained Hold
# =============================================================================


async def test_sustained(tiers: list[int], token: str, duration: int):
    print_header(
        "TEST 4: Sustained Connection Hold",
        f"N connections held for {duration}s with periodic pings."
    )

    for n in tiers:
        print(f"\n  --- {n} connections for {duration}s ---")
        connections = await connect_batch(token, n)

        if not connections:
            print("    FAILED: no connections")
            continue

        async def _hold(ws):
            ok = 0
            t0 = time.perf_counter()
            while time.perf_counter() - t0 < duration:
                try:
                    pong = await ws.ping()
                    await asyncio.wait_for(pong, timeout=10)
                    ok += 1
                except Exception:
                    return ok, False
                await asyncio.sleep(1.0)
            return ok, True

        # Global timeout so we never hang forever
        tasks = [_hold(ws) for ws in connections]
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=duration + 30,
            )
            # Filter out exceptions from results
            clean = []
            for r in results:
                if isinstance(r, BaseException):
                    clean.append((0, False))
                else:
                    clean.append(r)
            results = clean
        except asyncio.TimeoutError:
            print("    TIMEOUT: test took too long, skipping")
            await close_all(connections)
            print()
            flush()
            continue

        alive = sum(1 for _, a in results if a)
        dead = sum(1 for _, a in results if not a)
        total_pings = sum(p for p, _ in results)

        print(f"    Alive:    {alive}/{len(connections)}")
        print(f"    Dropped:  {dead}")
        print(f"    Pings OK: {total_pings:,}")

        await close_all(connections)
        print()
        flush()


# =============================================================================
# Test 5: orjson vs json
# =============================================================================


def test_json_comparison():
    print_header(
        "TEST 5: json vs orjson vs msgpack Serialization",
        f"Payload: {SMALL_SIZE} bytes (trading message). 1M iterations."
    )

    import timeit
    import gc

    n = 1_000_000

    gc.disable()
    json_enc = timeit.timeit(lambda: json.dumps(SMALL_PAYLOAD), number=n)
    json_dec = timeit.timeit(lambda: json.loads(SMALL_JSON), number=n)
    gc.enable()

    json_enc_us = json_enc / n * 1_000_000
    json_dec_us = json_dec / n * 1_000_000

    print(f"\n  {'Library':>10} | {'Encode (us)':>11} | {'Decode (us)':>11} | {'Enc ops/s':>12} | {'Dec ops/s':>12} | {'Bytes':>6}")
    print(f"  {'-'*10}-+-{'-'*11}-+-{'-'*11}-+-{'-'*12}-+-{'-'*12}-+-{'-'*6}")
    print(f"  {'json':>10} | {json_enc_us:>11.3f} | {json_dec_us:>11.3f} | {n/json_enc:>12,.0f} | {n/json_dec:>12,.0f} | {SMALL_SIZE:>6}")

    orjson_enc = orjson_dec = None
    try:
        import orjson
        orjson_bytes = orjson.dumps(SMALL_PAYLOAD)
        orjson_size = len(orjson_bytes)

        gc.disable()
        orjson_enc = timeit.timeit(lambda: orjson.dumps(SMALL_PAYLOAD), number=n)
        orjson_dec = timeit.timeit(lambda: orjson.loads(orjson_bytes), number=n)
        gc.enable()

        orjson_enc_us = orjson_enc / n * 1_000_000
        orjson_dec_us = orjson_dec / n * 1_000_000

        print(f"  {'orjson':>10} | {orjson_enc_us:>11.3f} | {orjson_dec_us:>11.3f} | {n/orjson_enc:>12,.0f} | {n/orjson_dec:>12,.0f} | {orjson_size:>6}")
    except ImportError:
        print(f"  {'orjson':>10} | {'N/A':>11} | {'N/A':>11} | {'not installed':>12} | {'':>12} | {'':>6}")

    msgpack_enc = msgpack_dec = None
    try:
        import msgpack
        msgpack_bytes = msgpack.packb(SMALL_PAYLOAD)
        msgpack_size = len(msgpack_bytes)

        gc.disable()
        msgpack_enc = timeit.timeit(lambda: msgpack.packb(SMALL_PAYLOAD), number=n)
        msgpack_dec = timeit.timeit(lambda: msgpack.unpackb(msgpack_bytes), number=n)
        gc.enable()

        msgpack_enc_us = msgpack_enc / n * 1_000_000
        msgpack_dec_us = msgpack_dec / n * 1_000_000

        print(f"  {'msgpack':>10} | {msgpack_enc_us:>11.3f} | {msgpack_dec_us:>11.3f} | {n/msgpack_enc:>12,.0f} | {n/msgpack_dec:>12,.0f} | {msgpack_size:>6}")
    except ImportError:
        print(f"  {'msgpack':>10} | {'N/A':>11} | {'N/A':>11} | {'not installed':>12} | {'':>12} | {'':>6}")

    print()
    if orjson_enc:
        print(f"    orjson vs json:   encode {json_enc/orjson_enc:.1f}x, decode {json_dec/orjson_dec:.1f}x")
    if msgpack_enc:
        print(f"    msgpack vs json:  encode {json_enc/msgpack_enc:.1f}x, decode {json_dec/msgpack_dec:.1f}x")
        print(f"    msgpack size:     {msgpack_size} bytes ({100 - msgpack_size * 100 // SMALL_SIZE}% smaller than JSON)")
    print(f"    Payload: {SMALL_SIZE} bytes (JSON)")
    print()


# =============================================================================
# Test 6: Message Size Impact
# =============================================================================


def test_message_sizes(token: str, duration: int, n_conns: int = 64):
    """Throughput at various message sizes using multiprocessing."""
    print_header(
        "TEST 6: Message Size Impact",
        f"{n_conns} connections per size, {duration}s each. (1 proc per conn)"
    )

    sizes = [
        ("64B", 64),
        ("256B", 256),
        ("1KB", 1024),
        ("4KB", 4096),
        ("16KB", 16384),
        ("64KB", 65536),
    ]

    print(f"\n  {'Size':>8} | {'Actual':>8} | {'Total msg/s':>12} | {'MB/s':>8} | {'GB/s':>6} | {'OK':>4} | {'Err':>4}")
    print(f"  {'-'*8}-+-{'-'*8}-+-{'-'*12}-+-{'-'*8}-+-{'-'*6}-+-{'-'*4}-+-{'-'*4}")

    for label, target_size in sizes:
        payload = json.dumps({"t": "bench", "p": {"d": "x" * target_size}})
        rate, per_conn, mb_s, ok, errs, actual_size = _run_throughput_tier(
            n_conns, token, duration, payload=payload, label=f"[{label}] "
        )
        gb_s = mb_s / 1024
        print(f"\r  {label:>8} | {actual_size:>6}B | {rate:>12,.0f} | {mb_s:>8.1f} | {gb_s:>6.2f} | {ok:>4} | {errs:>4}")
        flush()

    print()
    flush()


# =============================================================================
# Test 7: Format Throughput Comparison (JSON vs msgpack over the wire)
# =============================================================================


def test_format_throughput(token: str, duration: int, n_conns: int = 500):
    """Compare JSON vs msgpack throughput over actual WebSocket connections."""
    print_header(
        "TEST 7: Wire Format Throughput (JSON vs msgpack)",
        f"{n_conns} connections, {duration}s each. Same payload, different encoding."
    )

    formats = [
        ("JSON (text)", SMALL_JSON, SMALL_SIZE),
    ]
    if HAS_MSGPACK:
        formats.append(("msgpack (M:)", SMALL_MSGPACK, SMALL_MSGPACK_SIZE))
    else:
        print("    msgpack not installed, skipping msgpack test")

    print(f"\n  {'Format':>16} | {'Payload':>8} | {'Total msg/s':>12} | {'Per-conn':>10} | {'MB/s':>8} | {'OK':>6} | {'Err':>6}")
    print(f"  {'-'*16}-+-{'-'*8}-+-{'-'*12}-+-{'-'*10}-+-{'-'*8}-+-{'-'*6}-+-{'-'*6}")

    for fmt_label, payload, payload_size in formats:
        rate, per_conn, mb_s, ok, errs, _ = _run_throughput_tier(
            n_conns, token, duration, payload=payload, label=f"[{fmt_label[:6]}] "
        )
        print(f"\r  {fmt_label:>16} | {payload_size:>6}B | {rate:>12,.0f} | {per_conn:>10,.0f} | {mb_s:>8.1f} | {ok:>6} | {errs:>6}")
        flush()

    print()
    flush()


# =============================================================================
# Main
# =============================================================================


async def async_main(args):
    tiers = [int(t.strip()) for t in args.tiers.split(",")]
    heavy_tiers = [int(t.strip()) for t in args.tiers_heavy.split(",")]
    tests = [int(t.strip()) for t in args.tests.split(",")]

    cpu_count = os.cpu_count() or "?"
    print("=" * 70)
    print("  WSE Brutal Concurrency Benchmark")
    print(f"  Server:     ws://{WS_HOST}:{WS_PORT}/wse")
    print(f"  CPU cores:  {cpu_count} (Tokio uses all by default)")
    print(f"  Tiers:      {tiers}")
    print(f"  Heavy tiers: {heavy_tiers} (tests 3-4)")
    print(f"  Duration:   {args.duration}s per test")
    print(f"  Payload:    {SMALL_SIZE} bytes")
    print(f"  Tests:      {tests}")
    print(f"  Time:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  PID:        {os.getpid()}")
    print("=" * 70)
    flush()

    if 1 in tests:
        await test_connection_storm(tiers, args.token)
        print("    Cooldown 5s (clearing TIME_WAIT sockets)...")
        flush()
        await asyncio.sleep(5)

    if 2 in tests:
        await test_echo_latency(tiers, args.token)
        print("    Cooldown 5s...")
        flush()
        await asyncio.sleep(5)

    if 3 in tests:
        test_throughput(heavy_tiers, args.token, args.duration)

    if 4 in tests:
        await test_sustained(heavy_tiers, args.token, args.duration)

    if 5 in tests:
        test_json_comparison()

    if 6 in tests:
        test_message_sizes(args.token, args.duration, n_conns=64)

    if 7 in tests:
        test_format_throughput(args.token, args.duration, n_conns=500)

    print("=" * 70)
    print("  Benchmark complete")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    flush()


def _apply_config(host: str, port: int):
    global WS_HOST, WS_PORT
    WS_HOST = host
    WS_PORT = port


def main():
    parser = argparse.ArgumentParser(description="WSE Brutal Concurrency Benchmark")
    parser.add_argument("--token", required=True, help="JWT token")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5006)
    parser.add_argument("--tiers", default=",".join(str(t) for t in DEFAULT_TIERS),
                        help="Connection tiers (default: 100,500,1000,2000,5000,10000)")
    parser.add_argument("--tiers-heavy", default=",".join(str(t) for t in DEFAULT_TIERS_HEAVY),
                        help="Tiers for heavy tests 3-4,6 (default: 100,500,1000)")
    parser.add_argument("--duration", type=int, default=5, help="Seconds per test (default: 5)")
    parser.add_argument("--tests", default="1,2,3,4,5,6,7", help="Tests to run (1-7, default: all)")
    args = parser.parse_args()

    _apply_config(args.host, args.port)

    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
