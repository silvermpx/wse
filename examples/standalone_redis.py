"""Multi-instance WSE with Redis pub/sub.

Run two instances on different ports. Messages published on one
are delivered to subscribers on the other via Redis.

    pip install wse-server

    # Terminal 1 (publishes every second)
    python examples/standalone_redis.py --port 5007

    # Terminal 2 (publishes every second)
    python examples/standalone_redis.py --port 5008

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
        # broadcast() sends to local subscribers AND publishes via Redis
        # so subscribers on other instances receive it too
        server.broadcast("notifications", msg)
        seq += 1
        time.sleep(1.0)


def main():
    parser = argparse.ArgumentParser(description="Multi-instance WSE with Redis")
    parser.add_argument("--port", type=int, default=5007)
    parser.add_argument("--redis-url", default="redis://localhost:6379")
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
        server.connect_redis(args.redis_url)
        print(f"Redis: {args.redis_url}")
    except Exception as e:
        print(f"Redis not available ({e}) -- running without cross-instance delivery")

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
