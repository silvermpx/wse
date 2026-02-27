"""Fan-out benchmark server.

Starts the Rust WSE server and continuously broadcasts or publishes messages.
Rust benchmark client (wse-bench --test fanout-broadcast/fanout-cluster)
connects and measures receive throughput.

Modes:
  broadcast        -- server.broadcast_all() to ALL connections
  cluster          -- server.broadcast() via cluster to subscribed connections
  cluster-subscribe -- subscribe-only mode for multi-instance cluster test (Server B)

Usage:
    # Test 8: Fan-out Broadcast
    python benchmarks/bench_fanout_server.py --mode broadcast
    wse-bench --test fanout-broadcast

    # Test 11: Multi-Instance Fan-out (two servers + Cluster protocol)
    python benchmarks/bench_fanout_server.py --mode cluster --port 5006 --peers 127.0.0.1:5007
    python benchmarks/bench_fanout_server.py --mode cluster-subscribe --port 5007 --peers 127.0.0.1:5006
    wse-bench --test fanout-cluster --port 5006 --port2 5007

    # Test 12: Multi-Instance Fan-out with TLS (separate cluster ports)
    python benchmarks/bench_fanout_server.py --mode cluster --port 5006 --peers 127.0.0.1:6007 \\
        --cluster-port 6006 --cluster-addr 127.0.0.1:6006 --generate-tls
    python benchmarks/bench_fanout_server.py --mode cluster-subscribe --port 5007 --peers 127.0.0.1:6006 \\
        --cluster-port 6007 --cluster-addr 127.0.0.1:6007 \\
        --tls-cert /tmp/wse_tls_test/server.crt --tls-key /tmp/wse_tls_test/server.key --tls-ca /tmp/wse_tls_test/ca.crt
    wse-bench --test fanout-cluster-tls --port 5006 --port2 5007
"""

import argparse
import os
import signal
import subprocess
import sys
import time
import threading

from wse_server._wse_accel import RustWSEServer, rust_jwt_encode

JWT_SECRET = b"bench-secret-key-for-testing-only"
JWT_ISSUER = "wse-bench"
JWT_AUDIENCE = "wse-bench"

BENCH_TOPIC = "bench_topic"
TLS_CERT_DIR = "/tmp/wse_tls_test"


def generate_tls_certs():
    """Generate self-signed test certs in a fixed directory."""
    os.makedirs(TLS_CERT_DIR, exist_ok=True)

    ca_key = f"{TLS_CERT_DIR}/ca.key"
    ca_crt = f"{TLS_CERT_DIR}/ca.crt"
    srv_key = f"{TLS_CERT_DIR}/server.key"
    srv_csr = f"{TLS_CERT_DIR}/server.csr"
    srv_crt = f"{TLS_CERT_DIR}/server.crt"

    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048",
        "-keyout", ca_key, "-out", ca_crt,
        "-days", "1", "-nodes", "-subj", "/CN=wse-test-ca",
    ], check=True, capture_output=True)

    subprocess.run([
        "openssl", "req", "-newkey", "rsa:2048",
        "-keyout", srv_key, "-out", srv_csr,
        "-nodes", "-subj", "/CN=localhost",
        "-addext", "subjectAltName=IP:127.0.0.1",
    ], check=True, capture_output=True)

    subprocess.run([
        "openssl", "x509", "-req", "-in", srv_csr,
        "-CA", ca_crt, "-CAkey", ca_key,
        "-CAcreateserial", "-out", srv_crt, "-days", "1",
        "-copy_extensions", "copyall",
    ], check=True, capture_output=True)

    print(f"[fanout-server] TLS certs generated in {TLS_CERT_DIR}")
    return srv_crt, srv_key, ca_crt


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
    """Drain inbound events. For cluster modes, auto-subscribe connections."""
    subscribe_mode = mode in ("cluster", "cluster-subscribe")
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
            elif mode == "cluster":
                server.broadcast(BENCH_TOPIC, msg)
                # Yield GIL so drain_loop can process subscribe events
                if seq % 100 == 0:
                    time.sleep(0)
            else:
                # cluster-subscribe mode: no publishing
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
        choices=["broadcast", "cluster", "cluster-subscribe"],
        required=True,
        help="broadcast=all connections, cluster=cluster publish, cluster-subscribe=cluster receive only",
    )
    parser.add_argument("--port", type=int, default=5006)
    parser.add_argument("--max-connections", type=int, default=60000)
    parser.add_argument("--peers", nargs="+", default=None,
                        help="Cluster peer addresses for cluster modes, e.g. 127.0.0.1:5007")
    parser.add_argument("--tls-cert", default=None, help="Path to TLS certificate PEM")
    parser.add_argument("--tls-key", default=None, help="Path to TLS private key PEM")
    parser.add_argument("--tls-ca", default=None, help="Path to CA certificate PEM")
    parser.add_argument("--generate-tls", action="store_true",
                        help=f"Generate self-signed test certs in {TLS_CERT_DIR}")
    parser.add_argument("--cluster-port", type=int, default=None,
                        help="Cluster listen port (separate from WebSocket port)")
    parser.add_argument("--cluster-addr", default=None,
                        help="Cluster advertise address host:port")
    args = parser.parse_args()

    if args.mode in ("cluster", "cluster-subscribe") and not args.peers and not args.cluster_port:
        print("ERROR: --peers or --cluster-port required for cluster/cluster-subscribe mode", file=sys.stderr)
        sys.exit(1)

    if args.generate_tls:
        cert, key, ca = generate_tls_certs()
        args.tls_cert = cert
        args.tls_key = key
        args.tls_ca = ca

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

    if args.peers or args.cluster_port:
        peers = args.peers or []
        server.connect_cluster(
            peers,
            tls_cert=args.tls_cert,
            tls_key=args.tls_key,
            tls_ca=args.tls_ca,
            cluster_port=args.cluster_port,
            cluster_addr=args.cluster_addr,
        )
        if peers:
            print(f"[fanout-server] Cluster peers: {peers}")
        if args.tls_cert:
            print(f"[fanout-server] TLS enabled (cert={args.tls_cert})")
        if args.cluster_port:
            print(f"[fanout-server] Cluster port: {args.cluster_port}, addr: {args.cluster_addr}")

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
