"""Multi-instance WSE with direct cluster protocol (no Redis).

Run two instances on different ports. Messages published on one
are delivered to subscribers on the other via direct TCP mesh.

    pip install wse-server

    # Terminal 1 (port 5007, peers with 5008)
    python examples/standalone_cluster.py --port 5007 --peers 127.0.0.1:5008

    # Terminal 2 (port 5008, peers with 5007)
    python examples/standalone_cluster.py --port 5008 --peers 127.0.0.1:5007

Connect a client to either port -- it receives messages from both instances.
"""

import argparse
import json
import signal
import threading
import time

from wse_server._wse_accel import RustWSEServer, rust_jwt_encode

JWT_SECRET = b"my-secret-key"
JWT_ISSUER = "my-app"
JWT_AUDIENCE = "my-api"


def make_token(user_id: str = "user-1") -> str:
    return rust_jwt_encode(
        {
            "sub": user_id,
            "iss": JWT_ISSUER,
            "aud": JWT_AUDIENCE,
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        },
        JWT_SECRET,
    )


def event_loop(server: RustWSEServer, stop: threading.Event):
    while not stop.is_set():
        events = server.drain_inbound(256, 50)
        for event in events:
            event_type = event[0]
            conn_id = event[1]

            if event_type == "auth_connect":
                user_id = event[2]
                print(f"[+] {user_id} connected")
                server.subscribe_connection(conn_id, ["notifications"])

            elif event_type == "disconnect":
                print(f"[-] {conn_id} disconnected")


def publisher(server: RustWSEServer, stop: threading.Event, port: int):
    """Each instance publishes with its port in the payload."""
    seq = 0
    while not stop.is_set():
        msg = json.dumps({
            "t": "notification",
            "p": {"text": f"Hello from :{port}", "seq": seq},
        })
        # broadcast() sends to local subscribers AND publishes via cluster
        # so subscribers on other instances receive it too
        server.broadcast("notifications", msg)
        seq += 1
        time.sleep(1.0)


def main():
    parser = argparse.ArgumentParser(description="Multi-instance WSE with cluster protocol")
    parser.add_argument("--port", type=int, default=5007)
    parser.add_argument("--peers", nargs="+", required=True,
                        help="Peer WS addresses, e.g. 127.0.0.1:5008")
    args = parser.parse_args()

    server = RustWSEServer(
        "0.0.0.0", args.port,
        max_connections=10_000,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
    )
    server.enable_drain_mode()
    server.start()

    try:
        server.connect_cluster(args.peers)
        print(f"Cluster peers: {args.peers}")
    except Exception as e:
        print(f"Cluster connect failed: {e}")
        server.stop()
        return

    token = make_token()
    print(f"WSE listening on :{args.port}")
    print(f"Token: {token}")
    print("Press Ctrl+C to stop.\n")

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    t1 = threading.Thread(target=event_loop, args=(server, stop), daemon=True)
    t2 = threading.Thread(target=publisher, args=(server, stop, args.port), daemon=True)
    t1.start()
    t2.start()

    while not stop.is_set():
        stop.wait(1.0)

    server.stop()
    print("Stopped.")


if __name__ == "__main__":
    main()
