"""Battle test server -- comprehensive feature testing.

Starts WSE server with drain mode. Handles client subscribe commands
and continuously broadcasts to multiple channels for correctness verification.

The server publishes in round-robin to:
  1. broadcast_all (channel="all")
  2. topic "battle_all" (channel="battle_all")
  3. topic "battle_half" (channel="battle_half")
  4. topic "battle.glob_test" (channel="battle.glob_test")

Clients subscribe via messages: {"t":"battle_cmd","p":{"action":"subscribe","topics":[...]}}
Clients query health via:      {"t":"battle_cmd","p":{"action":"health"}}

Modes:
  standalone       -- single server, all features (default)
  cluster          -- publisher side (Server A), connects to cluster peers
  cluster-subscribe -- subscriber side (Server B), handles client subscriptions

Usage:
    # Standalone battle test
    python benchmarks/bench_battle_server.py
    wse-bench --test battle-standalone

    # Cluster battle test (two terminals)
    python benchmarks/bench_battle_server.py --mode cluster --port 5006 --peers 127.0.0.1:5007
    python benchmarks/bench_battle_server.py --mode cluster-subscribe --port 5007 --peers 127.0.0.1:5006
    wse-bench --test battle-cluster --port 5006 --port2 5007

    # Cluster with TLS
    python benchmarks/bench_battle_server.py --mode cluster --port 5006 --peers 127.0.0.1:5007 --generate-tls
    python benchmarks/bench_battle_server.py --mode cluster-subscribe --port 5007 --peers 127.0.0.1:5006 \\
        --tls-cert /tmp/wse_tls_test/server.crt --tls-key /tmp/wse_tls_test/server.key --tls-ca /tmp/wse_tls_test/ca.crt

    # Cluster with gossip discovery (3 nodes)
    python benchmarks/bench_battle_server.py --mode cluster --port 5006 --peers 127.0.0.1:5007 \\
        --cluster-port 6006 --cluster-addr 127.0.0.1:6006
    python benchmarks/bench_battle_server.py --mode cluster --port 5007 --peers 127.0.0.1:5006 \\
        --cluster-port 6007 --cluster-addr 127.0.0.1:6007
    python benchmarks/bench_battle_server.py --mode cluster-subscribe --port 5008 \\
        --cluster-port 6008 --cluster-addr 127.0.0.1:6008 --seeds 127.0.0.1:6007
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

TOPICS = ["battle_all", "battle_half", "battle.glob_test"]

TLS_CERT_DIR = "/tmp/wse_tls_test"


def generate_token(user_id: str = "bench-user") -> str:
    claims = {
        "sub": user_id,
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "exp": int(time.time()) + 3600,
        "iat": int(time.time()),
    }
    return rust_jwt_encode(claims, JWT_SECRET)


def generate_tls_certs():
    """Generate self-signed test certs in a fixed directory."""
    os.makedirs(TLS_CERT_DIR, exist_ok=True)

    ca_key = f"{TLS_CERT_DIR}/ca.key"
    ca_crt = f"{TLS_CERT_DIR}/ca.crt"
    srv_key = f"{TLS_CERT_DIR}/server.key"
    srv_csr = f"{TLS_CERT_DIR}/server.csr"
    srv_crt = f"{TLS_CERT_DIR}/server.crt"

    # Generate CA
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048",
        "-keyout", ca_key, "-out", ca_crt,
        "-days", "1", "-nodes", "-subj", "/CN=wse-test-ca",
    ], check=True, capture_output=True)

    # Generate server cert signed by CA
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

    print(f"[battle-server] TLS certs generated in {TLS_CERT_DIR}")
    return srv_crt, srv_key, ca_crt


def drain_loop(server, stop_event: threading.Event):
    """Process inbound events. Handle subscribe and health commands from clients."""
    while not stop_event.is_set():
        try:
            batch = server.drain_inbound(256, 50)
            if batch:
                for event in batch:
                    event_type = event[0]
                    conn_id = event[1]
                    if event_type == "msg":
                        data = event[2]
                        if isinstance(data, dict) and data.get("t") == "battle_cmd":
                            p = data.get("p", {})
                            action = p.get("action")
                            if action == "subscribe":
                                topics = p.get("topics", [])
                                if topics:
                                    server.subscribe_connection(conn_id, topics)
                            elif action == "subscribe_presence":
                                topics = p.get("topics", [])
                                presence_data = p.get("presence_data", {})
                                if topics:
                                    server.subscribe_connection(
                                        conn_id, topics, presence_data
                                    )
                            elif action == "presence_query":
                                topic = p.get("topic", "")
                                if topic:
                                    members = server.presence(topic)
                                    response = {
                                        "t": "presence_result",
                                        "p": {"topic": topic, "members": members},
                                    }
                                    server.send_event(conn_id, response, 0)
                            elif action == "update_presence":
                                data = p.get("data", {})
                                try:
                                    server.update_presence(conn_id, data)
                                except Exception:
                                    pass  # Connection may not have presence
                            elif action == "subscribe_recovery":
                                topics = p.get("topics", [])
                                recover = p.get("recover", False)
                                recovery_info = p.get("recovery", {})
                                if topics:
                                    ep = None
                                    off = None
                                    if recover and recovery_info:
                                        first_info = next(iter(recovery_info.values()), {})
                                        ep = first_info.get("epoch")
                                        off = first_info.get("offset")
                                    result = server.subscribe_with_recovery(
                                        conn_id, topics,
                                        recover=recover,
                                        epoch=ep,
                                        offset=int(off) if off is not None else None,
                                    )
                                    import json
                                    topics_info = result.get("topics", {})
                                    response = json.dumps({
                                        "t": "subscription_update",
                                        "p": {
                                            "action": "subscribe",
                                            "success": True,
                                            "success_topics": topics,
                                            "recoverable": True,
                                            "recovered": result.get("recovered", False),
                                            "recovery": {
                                                t: {
                                                    "epoch": info.get("epoch"),
                                                    "offset": info.get("offset"),
                                                    "recovered": info.get("recovered", False),
                                                    "count": info.get("count", 0),
                                                }
                                                for t, info in topics_info.items()
                                                if isinstance(info, dict)
                                            },
                                        },
                                    })
                                    server.send(conn_id, f"WSE{response}")
                            elif action == "publish_messages":
                                # Publish N messages to a topic (for recovery buffer testing)
                                topic = p.get("topic", "battle_all")
                                count = min(p.get("count", 100), 10000)
                                import json as _json
                                for i in range(count):
                                    ts_us = int(time.time() * 1_000_000)
                                    msg = f'{{"t":"battle","p":{{"ch":"{topic}","seq":{i},"ts_us":{ts_us}}}}}'
                                    server.broadcast(topic, msg)
                                ack = _json.dumps({
                                    "t": "publish_ack",
                                    "p": {"topic": topic, "count": count}
                                })
                                server.send(conn_id, f"WSE{ack}")
                            elif action == "health":
                                health = server.health_snapshot()
                                response = {
                                    "t": "health_response",
                                    "p": health,
                                }
                                server.send_event(conn_id, response, 0)
            else:
                time.sleep(0.001)
        except Exception as e:
            print(f"[battle-server] drain error: {e}", file=sys.stderr)
            time.sleep(0.1)


def publish_loop(server, mode: str, message_size: int, stop_event: threading.Event):
    """Broadcast to all channels in round-robin."""
    seq = 0
    published = 0
    start = time.time()
    last_report = start
    interval_count = 0
    num_channels = len(TOPICS) + 1  # +1 for broadcast_all

    # Build padding for large messages
    padding = ""
    if message_size > 100:
        padding_len = message_size - 90  # approximate base message size
        padding = ',"pad":"' + "x" * max(0, padding_len) + '"'

    while not stop_event.is_set():
        # cluster-subscribe mode: no publishing, just handle subscriptions
        if mode == "cluster-subscribe":
            time.sleep(1)
            continue

        ts_us = int(time.time() * 1_000_000)
        cycle = seq % num_channels

        try:
            if cycle == 0:
                msg = f'{{"t":"battle","p":{{"ch":"all","seq":{seq},"ts_us":{ts_us}{padding}}}}}'
                server.broadcast_all(msg)
            else:
                topic = TOPICS[cycle - 1]
                msg = f'{{"t":"battle","p":{{"ch":"{topic}","seq":{seq},"ts_us":{ts_us}{padding}}}}}'
                server.broadcast(topic, msg)
                # Yield GIL for drain_loop to process subscribe events
                if seq % 100 == 0:
                    time.sleep(0)
        except Exception:
            time.sleep(0.01)
            continue

        seq += 1
        published += 1
        interval_count += 1

        now = time.time()
        if now - last_report >= 5.0:
            interval_secs = now - last_report
            interval_rate = interval_count / interval_secs
            conns = server.get_connection_count()
            print(
                f"[battle-server] {published:,} total ({interval_rate:,.0f}/s), "
                f"{conns} connections",
                file=sys.stderr,
            )
            last_report = now
            interval_count = 0


def main():
    parser = argparse.ArgumentParser(description="Battle test server")
    parser.add_argument(
        "--mode",
        choices=["standalone", "cluster", "cluster-subscribe"],
        default="standalone",
        help="standalone=single server, cluster=publisher side, cluster-subscribe=subscriber side",
    )
    parser.add_argument("--port", type=int, default=5006)
    parser.add_argument("--max-connections", type=int, default=60000)
    parser.add_argument("--peers", nargs="+", default=None,
                        help="Cluster peer addresses, e.g. 127.0.0.1:5007")
    parser.add_argument("--message-size", type=int, default=100,
                        help="Approximate message size in bytes (default: 100)")
    parser.add_argument("--no-publish", action="store_true",
                        help="Disable publish loop (for feature-only tests like presence/recovery)")
    parser.add_argument("--recovery", action="store_true",
                        help="Enable message recovery (ring buffer per topic)")

    # TLS options
    parser.add_argument("--tls-cert", default=None, help="Path to TLS certificate PEM")
    parser.add_argument("--tls-key", default=None, help="Path to TLS private key PEM")
    parser.add_argument("--tls-ca", default=None, help="Path to CA certificate PEM")
    parser.add_argument("--generate-tls", action="store_true",
                        help=f"Generate self-signed test certs in {TLS_CERT_DIR}")

    # Gossip discovery options
    parser.add_argument("--cluster-port", type=int, default=None,
                        help="Cluster listen port (for gossip discovery)")
    parser.add_argument("--cluster-addr", default=None,
                        help="Cluster advertise address host:port (required with --seeds)")
    parser.add_argument("--seeds", nargs="+", default=None,
                        help="Seed addresses for gossip discovery, e.g. 127.0.0.1:6007")

    args = parser.parse_args()

    if args.mode in ("cluster", "cluster-subscribe") and not args.peers and not args.seeds and not args.cluster_port:
        print("ERROR: --peers, --seeds, or --cluster-port required for cluster modes", file=sys.stderr)
        sys.exit(1)

    # Generate TLS certs if requested
    if args.generate_tls:
        cert, key, ca = generate_tls_certs()
        args.tls_cert = cert
        args.tls_key = key
        args.tls_ca = ca

    server_kwargs = dict(
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
        presence_enabled=True,
    )
    if args.recovery:
        server_kwargs.update(
            recovery_enabled=True,
            recovery_buffer_size=4096,
            recovery_ttl=300,
            recovery_max_messages=2000,
        )
    server = RustWSEServer(
        "0.0.0.0",
        args.port,
        args.max_connections,
        **server_kwargs,
    )
    server.enable_drain_mode()
    server.start()
    print(f"[battle-server] Rust WSE on :{args.port} (mode={args.mode})")

    if args.peers or args.seeds or args.cluster_port:
        peers = args.peers or []
        server.connect_cluster(
            peers,
            tls_cert=args.tls_cert,
            tls_key=args.tls_key,
            tls_ca=args.tls_ca,
            cluster_port=args.cluster_port,
            seeds=args.seeds,
            cluster_addr=args.cluster_addr,
        )
        if args.peers:
            print(f"[battle-server] Cluster peers: {args.peers}")
        if args.seeds:
            print(f"[battle-server] Seeds: {args.seeds}")
        if args.tls_cert:
            print(f"[battle-server] TLS enabled (cert={args.tls_cert})")
        if args.cluster_port:
            print(f"[battle-server] Cluster port: {args.cluster_port}, addr: {args.cluster_addr}")

    token = generate_token()
    print(f"[battle-server] JWT: {token}")
    if args.message_size != 100:
        print(f"[battle-server] Message size: ~{args.message_size} bytes")
    print("[battle-server] Ready. Press Ctrl+C to stop.")

    stop = threading.Event()

    def handle_signal(*_):
        stop.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    drain_thread = threading.Thread(target=drain_loop, args=(server, stop), daemon=True)
    drain_thread.start()

    if not args.no_publish:
        pub_thread = threading.Thread(
            target=publish_loop, args=(server, args.mode, args.message_size, stop), daemon=True
        )
        pub_thread.start()
    else:
        print("[battle-server] Publish loop disabled (--no-publish)")


    try:
        while not stop.is_set():
            stop.wait(1.0)
    except KeyboardInterrupt:
        stop.set()

    time.sleep(0.5)  # let daemon threads release borrows
    try:
        server.stop()
    except RuntimeError:
        pass  # threads may still hold borrow
    print("[battle-server] Stopped.")


if __name__ == "__main__":
    main()
