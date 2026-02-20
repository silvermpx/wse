"""WSE (WebSocket Engine) Performance Benchmark.

Tests:
1. Connection latency (time to connect + auth + server_ready)
2. Message throughput (messages/sec received)
3. Ping/pong round-trip latency
4. Concurrent connections scalability
5. Message receive latency under load

Usage:
    python benchmarks/bench_wse.py [--host HOST] [--port PORT]
                                   [--login-url LOGIN_URL]
                                   [--email EMAIL] [--password PASSWORD]
"""

import argparse
import asyncio
import json
import statistics
import time
from datetime import datetime

import zlib

import httpx
import websockets


# -- Config ------------------------------------------------------------------

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5005
DEFAULT_LOGIN_URL = "/auth/login"


# -- WSE Protocol helper -----------------------------------------------------

def decode_wse_message(msg) -> dict | None:
    """Decode a WSE message (handles WSE prefix, C: compression, E: encryption)."""
    if isinstance(msg, bytes):
        if msg.startswith(b"C:"):
            msg = zlib.decompress(msg[2:]).decode()
        elif msg.startswith(b"E:"):
            return None  # Can't decrypt
        else:
            msg = msg.decode()
    if isinstance(msg, str):
        if msg.startswith("WSE"):
            msg = msg[3:]
        return json.loads(msg)
    return None


# -- Auth helper -------------------------------------------------------------

async def get_jwt_token(host: str, port: int, login_url: str,
                        email: str, password: str) -> str | None:
    """Login and get JWT access token."""
    url = f"http://{host}:{port}{login_url}"
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json={
            "email": email,
            "password": password,
        })
        if resp.status_code == 200:
            data = resp.json()
            token = data.get("access_token") or data.get("token")
            if token:
                return token
            # Try cookies
            for cookie in resp.cookies.jar:
                if cookie.name == "access_token":
                    return cookie.value
        # Try extracting from cookies in response
        cookies = {c.name: c.value for c in resp.cookies.jar}
        if "access_token" in cookies:
            return cookies["access_token"]
        print(f"  Login failed: {resp.status_code} {resp.text[:200]}")
        return None


# -- Test 1: Connection Latency ----------------------------------------------

async def bench_connection_latency(host: str, port: int, token: str, rounds: int = 20):
    """Measure time from connect to receiving server_ready."""
    print(f"\n{'='*60}")
    print(f"Test 1: Connection Latency ({rounds} rounds)")
    print(f"{'='*60}")

    latencies = []
    for i in range(rounds):
        uri = f"ws://{host}:{port}/wse?token={token}&compression=false"
        t0 = time.perf_counter()
        try:
            async with websockets.connect(uri, open_timeout=5) as ws:
                # Wait for server_ready message
                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=5)
                    data = decode_wse_message(msg)
                    if isinstance(data, dict) and data.get("t") == "server_ready":
                        t1 = time.perf_counter()
                        latencies.append((t1 - t0) * 1000)
                        break
        except Exception as e:
            print(f"  Round {i+1}: FAILED ({e})")

    if latencies:
        print(f"  Rounds:  {len(latencies)}/{rounds}")
        print(f"  Mean:    {statistics.mean(latencies):.2f} ms")
        print(f"  Median:  {statistics.median(latencies):.2f} ms")
        print(f"  p95:     {sorted(latencies)[int(len(latencies)*0.95)]:.2f} ms")
        print(f"  Min:     {min(latencies):.2f} ms")
        print(f"  Max:     {max(latencies):.2f} ms")
    return latencies


# -- Test 2: Ping/Pong RTT --------------------------------------------------

async def bench_ping_pong_rtt(host: str, port: int, token: str, rounds: int = 100):
    """Measure WebSocket ping/pong round-trip time."""
    print(f"\n{'='*60}")
    print(f"Test 2: Ping/Pong RTT ({rounds} rounds)")
    print(f"{'='*60}")

    uri = f"ws://{host}:{port}/wse?token={token}&compression=false"
    latencies = []

    try:
        async with websockets.connect(uri, open_timeout=5) as ws:
            # Drain server_ready
            await asyncio.wait_for(ws.recv(), timeout=5)
            await asyncio.sleep(0.1)

            for i in range(rounds):
                t0 = time.perf_counter()
                pong = await ws.ping()
                await pong
                t1 = time.perf_counter()
                latencies.append((t1 - t0) * 1000)

    except Exception as e:
        print(f"  Error: {e}")

    if latencies:
        print(f"  Rounds:  {len(latencies)}/{rounds}")
        print(f"  Mean:    {statistics.mean(latencies):.2f} ms")
        print(f"  Median:  {statistics.median(latencies):.2f} ms")
        print(f"  p95:     {sorted(latencies)[int(len(latencies)*0.95)]:.2f} ms")
        print(f"  p99:     {sorted(latencies)[int(len(latencies)*0.99)]:.2f} ms")
        print(f"  Min:     {min(latencies):.2f} ms")
        print(f"  Max:     {max(latencies):.2f} ms")
    return latencies


# -- Test 3: Message Send Throughput -----------------------------------------

async def bench_send_throughput(host: str, port: int, token: str, n_messages: int = 1000):
    """Measure how fast client can send messages to server."""
    print(f"\n{'='*60}")
    print(f"Test 3: Client->Server Throughput ({n_messages} messages)")
    print(f"{'='*60}")

    uri = f"ws://{host}:{port}/wse?token={token}&compression=false"
    msg = json.dumps({"action": "ping", "ts": "2026-01-01T00:00:00Z"})

    try:
        async with websockets.connect(uri, open_timeout=5) as ws:
            # Drain initial messages
            await asyncio.wait_for(ws.recv(), timeout=5)
            await asyncio.sleep(0.2)

            t0 = time.perf_counter()
            for _ in range(n_messages):
                await ws.send(msg)
            t1 = time.perf_counter()

            elapsed = t1 - t0
            rps = n_messages / elapsed
            print(f"  Messages: {n_messages}")
            print(f"  Time:     {elapsed:.3f}s")
            print(f"  Rate:     {rps:,.0f} msg/sec")
            print(f"  Per msg:  {elapsed/n_messages*1000:.3f} ms")
            return rps

    except Exception as e:
        print(f"  Error: {e}")
        return 0


# -- Test 4: Rapid Connect/Disconnect ----------------------------------------

async def bench_concurrent_connections(host: str, port: int, token: str, n_conns: int = 10):
    """Test rapid connect/disconnect cycles."""
    print(f"\n{'='*60}")
    print(f"Test 4: Rapid Connect/Disconnect ({n_conns} sequential)")
    print(f"{'='*60}")

    uri = f"ws://{host}:{port}/wse?token={token}&compression=false"
    times = []

    for i in range(n_conns):
        t0 = time.perf_counter()
        try:
            async with websockets.connect(uri, open_timeout=5) as ws:
                msg = await asyncio.wait_for(ws.recv(), timeout=5)
                t1 = time.perf_counter()
                times.append((t1 - t0) * 1000)
        except Exception as e:
            print(f"  Conn {i+1}: FAILED ({e})")

    if times:
        print(f"  Connections: {len(times)}/{n_conns}")
        print(f"  Mean:    {statistics.mean(times):.2f} ms")
        print(f"  Median:  {statistics.median(times):.2f} ms")
        print(f"  Min:     {min(times):.2f} ms")
        print(f"  Max:     {max(times):.2f} ms")
        print(f"  Total:   {sum(times):.0f} ms")
    return times


# -- Test 5: Message Size Impact ---------------------------------------------

async def bench_message_sizes(host: str, port: int, token: str):
    """Test send performance with different message sizes (fresh connection per size)."""
    print(f"\n{'='*60}")
    print(f"Test 5: Message Size Impact")
    print(f"{'='*60}")

    sizes = [64, 256, 1024, 4096, 16384, 65536]
    n_per_size = 200

    for size in sizes:
        uri = f"ws://{host}:{port}/wse?token={token}&compression=false"
        try:
            async with websockets.connect(uri, open_timeout=5, max_size=2**20) as ws:
                # Drain initial messages
                while True:
                    try:
                        await asyncio.wait_for(ws.recv(), timeout=0.5)
                    except asyncio.TimeoutError:
                        break

                payload = json.dumps({"action": "ping", "data": "x" * size})
                actual_size = len(payload.encode())

                t0 = time.perf_counter()
                for _ in range(n_per_size):
                    await ws.send(payload)
                t1 = time.perf_counter()

                elapsed = t1 - t0
                rps = n_per_size / elapsed
                throughput_mb = (actual_size * n_per_size) / elapsed / 1024 / 1024
                print(f"  {actual_size:>6} bytes: {rps:>8,.0f} msg/s | {throughput_mb:>6.1f} MB/s | {elapsed/n_per_size*1000:.3f} ms/msg")

        except Exception as e:
            print(f"  {size:>6} bytes: ERROR - {e}")


# -- Main --------------------------------------------------------------------

async def main(host: str, port: int, login_url: str, email: str, password: str):
    print("=" * 60)
    print("  WSE Performance Benchmark")
    print(f"  Server: ws://{host}:{port}/wse")
    print(f"  Time:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Get auth token
    print("\n  Authenticating...")
    token = await get_jwt_token(host, port, login_url, email, password)
    if not token:
        print("  FATAL: Cannot get JWT token. Is the server running?")
        return
    print(f"  Token: {token[:20]}...")

    # Run tests
    await bench_connection_latency(host, port, token)
    await bench_ping_pong_rtt(host, port, token)
    await bench_send_throughput(host, port, token)
    await bench_concurrent_connections(host, port, token)
    await bench_message_sizes(host, port, token)

    print(f"\n{'='*60}")
    print("  Benchmark complete")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WSE Benchmark")
    parser.add_argument("--host", default=DEFAULT_HOST,
                        help="Server host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help="Server port (default: 5005)")
    parser.add_argument("--login-url", default=DEFAULT_LOGIN_URL,
                        help="Login endpoint path (default: /auth/login)")
    parser.add_argument("--email", required=True,
                        help="Login email for authentication")
    parser.add_argument("--password", required=True,
                        help="Login password for authentication")
    args = parser.parse_args()
    asyncio.run(main(args.host, args.port, args.login_url, args.email, args.password))
