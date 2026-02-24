"""Fan-out benchmark server.

Starts the Rust WSE server and continuously broadcasts or publishes messages.
Rust benchmark client (wse-bench --test fanout-broadcast/fanout-pubsub/fanout-multi)
connects and measures receive throughput.

Modes:
  broadcast  -- server.broadcast_all() to ALL connections (no Redis)
  pubsub     -- server.broadcast() via Redis to subscribed connections
  subscribe  -- subscribe-only mode for multi-instance test (Server B)

Usage:
    # Test 8: Fan-out Broadcast (no Redis)
    python benchmarks/bench_fanout_server.py --mode broadcast
    wse-bench --test fanout-broadcast

    # Test 9: Multi-Instance Fan-out (two servers + Redis)
    python benchmarks/bench_fanout_server.py --mode pubsub --port 5006 --redis-url redis://localhost:6379
    python benchmarks/bench_fanout_server.py --mode subscribe --port 5007 --redis-url redis://localhost:6379
    wse-bench --test fanout-multi --port 5006 --port2 5007
"""

import argparse
import signal
import sys
import time
import threading

from wse_server._wse_accel import RustWSEServer, rust_jwt_encode

JWT_SECRET = b"bench-secret-key-for-testing-only"
JWT_ISSUER = "wse-bench"
JWT_AUDIENCE = "wse-bench"

BENCH_TOPIC = "bench_topic"


def generate_token(user_id: str = "bench-user") -> str:
    claims = {
        "sub": user_id,
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "exp": int(time.time()) + 3600,
        "iat": int(time.time()),
    }
    return rust_jwt_encode(claims, JWT_SECRET)


def drain_loop(server, mode: str, stop_event: threading.Event):
    """Drain inbound events. For pubsub/subscribe modes, auto-subscribe connections."""
    subscribe_mode = mode in ("pubsub", "subscribe")
    while not stop_event.is_set():
        try:
            batch = server.drain_inbound(256, 50)
            if batch:
                for event in batch:
                    event_type = event[0]
                    conn_id = event[1]
                    if subscribe_mode and event_type in ("connect", "auth_connect"):
                        server.subscribe_connection(conn_id, [BENCH_TOPIC])
            else:
                time.sleep(0.001)
        except Exception as e:
            print(f"[fanout-server] drain error: {e}", file=sys.stderr)
            time.sleep(0.1)


def publish_loop(server, mode: str, stop_event: threading.Event):
    """Continuously broadcast or publish messages at max rate."""
    seq = 0
    published = 0
    start = time.time()
    last_report = start
    interval_count = 0

    while not stop_event.is_set():
        ts_us = int(time.time() * 1_000_000)
        msg = f'{{"t":"fanout_tick","p":{{"seq":{seq},"ts_us":{ts_us},"s":"ES","px":5234.75,"q":2}}}}'

        try:
            if mode == "broadcast":
                server.broadcast_all(msg)
            elif mode == "pubsub":
                server.broadcast(BENCH_TOPIC, msg)
                # Yield GIL so drain_loop can process subscribe events
                if seq % 100 == 0:
                    time.sleep(0)
            else:
                # subscribe mode: no publishing
                time.sleep(1)
                continue
        except Exception:
            time.sleep(0.01)
            continue

        seq += 1
        published += 1
        interval_count += 1

        # Report stats every 5 seconds (per-interval rate, not cumulative)
        now = time.time()
        if now - last_report >= 5.0:
            interval_secs = now - last_report
            interval_rate = interval_count / interval_secs
            conns = server.get_connection_count()
            print(
                f"[fanout-server] {published:,} total ({interval_rate:,.0f}/s last {interval_secs:.0f}s), "
                f"{conns} connections",
                file=sys.stderr,
            )
            last_report = now
            interval_count = 0


def main():
    parser = argparse.ArgumentParser(description="Fan-out benchmark server")
    parser.add_argument(
        "--mode",
        choices=["broadcast", "pubsub", "subscribe"],
        required=True,
        help="broadcast=no Redis, pubsub=Redis publish, subscribe=Redis receive only",
    )
    parser.add_argument("--port", type=int, default=5006)
    parser.add_argument("--max-connections", type=int, default=60000)
    parser.add_argument("--redis-url", default=None, help="Redis URL for pubsub/subscribe modes")
    args = parser.parse_args()

    if args.mode in ("pubsub", "subscribe") and not args.redis_url:
        print("ERROR: --redis-url required for pubsub/subscribe mode", file=sys.stderr)
        sys.exit(1)

    server = RustWSEServer(
        "0.0.0.0",
        args.port,
        args.max_connections,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
    )
    server.enable_drain_mode()
    server.start()
    print(f"[fanout-server] Rust WSE on :{args.port} (mode={args.mode})")

    if args.redis_url:
        server.connect_redis(args.redis_url)
        print(f"[fanout-server] Redis connected: {args.redis_url}")

    token = generate_token()
    print(f"[fanout-server] JWT: {token}")
    print("[fanout-server] Ready. Press Ctrl+C to stop.")

    stop = threading.Event()

    def handle_signal(*_):
        stop.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Drain thread (processes connections, auto-subscribes)
    drain_thread = threading.Thread(target=drain_loop, args=(server, args.mode, stop), daemon=True)
    drain_thread.start()

    # Publish thread (broadcasts or publishes at max rate)
    pub_thread = threading.Thread(
        target=publish_loop, args=(server, args.mode, stop), daemon=True
    )
    pub_thread.start()

    # Wait for stop signal
    try:
        while not stop.is_set():
            stop.wait(1.0)
    except KeyboardInterrupt:
        stop.set()

    server.stop()
    print("[fanout-server] Stopped.")


if __name__ == "__main__":
    main()
