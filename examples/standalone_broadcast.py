"""Standalone WSE server with topic subscriptions and fan-out.

Clients connect and get auto-subscribed to 'prices'.
Server publishes price ticks every 100ms.

Three broadcast methods:
  - broadcast_all(data)           -- send to ALL connections (no topics)
  - broadcast_local(topic, data)  -- send to topic subscribers (single instance)
  - broadcast(topic, data)        -- send to topic subscribers (all instances via cluster)

    pip install wse-server
    python examples/standalone_broadcast.py
"""

import json
import signal
import threading
import time

from wse_server._wse_accel import RustWSEServer, rust_jwt_encode

HOST = "0.0.0.0"
PORT = 5007
JWT_SECRET = b"change-me-to-a-secure-32-byte-key!"
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
    """Handle connections and auto-subscribe to topics."""
    while not stop.is_set():
        events = server.drain_inbound(256, 50)
        for event in events:
            event_type = event[0]
            conn_id = event[1]

            if event_type == "auth_connect":
                user_id = event[2]
                print(f"[+] {user_id} connected")
                # subscribe this connection to the prices topic
                server.subscribe_connection(conn_id, ["prices"])

            elif event_type == "msg":
                data = event[2]
                # client can request additional topics at runtime
                if isinstance(data, dict) and data.get("t") == "subscribe":
                    topics = data.get("p", {}).get("topics", [])
                    if topics:
                        server.subscribe_connection(conn_id, topics)
                        print(f"[sub] {conn_id} subscribed to {topics}")

            elif event_type == "disconnect":
                print(f"[-] {conn_id} disconnected")


def publisher(server: RustWSEServer, stop: threading.Event):
    """Publish price ticks every 100ms to the 'prices' topic."""
    seq = 0
    start = time.time()

    while not stop.is_set():
        msg = json.dumps({
            "t": "price_update",
            "p": {
                "symbol": "AAPL",
                "price": round(187.42 + seq * 0.01, 2),
                "seq": seq,
            },
        })

        # broadcast_local sends to subscribers of 'prices' on this instance
        server.broadcast_local("prices", msg)

        seq += 1

        # stats every 10 seconds
        if seq % 100 == 0:
            elapsed = time.time() - start
            conns = server.get_connection_count()
            subs = server.get_topic_subscriber_count("prices")
            if conns > 0:
                print(f"[pub] {seq} ticks, {conns} connections, {subs} subscribed to prices")

        time.sleep(0.1)


def main():
    server = RustWSEServer(
        HOST, PORT,
        max_connections=10_000,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
    )
    server.enable_drain_mode()
    server.start()

    token = make_token()
    print(f"WSE listening on {HOST}:{PORT}")
    print(f"Token: {token}")
    print("Publishing price ticks to 'prices' topic every 100ms")
    print("Press Ctrl+C to stop.\n")

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    t1 = threading.Thread(target=event_loop, args=(server, stop), daemon=True)
    t2 = threading.Thread(target=publisher, args=(server, stop), daemon=True)
    t1.start()
    t2.start()

    while not stop.is_set():
        stop.wait(1.0)

    server.stop()
    print("Stopped.")


if __name__ == "__main__":
    main()
