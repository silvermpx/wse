"""Standalone WSE server with message recovery on reconnect.

Demonstrates message recovery: when a client disconnects and reconnects,
it can recover missed messages from the server's per-topic ring buffer
using epoch+offset tracking.

    pip install wse-server

    # Terminal 1: Start server with recovery enabled
    python examples/standalone_recovery.py

    # Terminal 2: connect a client (wse-client or the TypeScript client),
    # subscribe to 'prices', disconnect, wait a few seconds, reconnect:
    # the client recovers the missed messages by itself.
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
    """Handle connections. The clients' subscribe and recovery requests are
    answered by the server itself (client_subscriptions=True): it replays
    what each topic missed from the client's own position, then replies."""
    while not stop.is_set():
        for event_type, conn_id, data in server.drain_inbound(256, 50):
            if event_type == "auth_connect":
                print(f"[+] {data} connected ({conn_id})")
                # What this user's clients may subscribe to.
                server.set_topic_acl(conn_id, allow=["prices", "news:*"])
            elif event_type == "msg":
                print(f"    [{conn_id}] {data}")
            elif event_type == "disconnect":
                print(f"[-] {conn_id} disconnected")


def publisher(server: RustWSEServer, stop: threading.Event):
    """Publish price ticks every 500ms to the 'prices' topic."""
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
        server.broadcast_local("prices", msg)
        seq += 1

        if seq % 20 == 0:
            elapsed = time.time() - start
            conns = server.get_connection_count()
            health = server.health_snapshot()
            recovery_topics = health.get("recovery_topic_count", 0)
            recovery_bytes = health.get("recovery_total_bytes", 0)
            print(
                f"[pub] {seq} ticks in {elapsed:.0f}s | {conns} conns | "
                f"recovery: {recovery_topics} topics, {recovery_bytes} bytes"
            )

        time.sleep(0.5)


def main():
    server = RustWSEServer(
        HOST, PORT,
        max_connections=10_000,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
        # Enable message recovery
        recovery_enabled=True,
        # Answer the clients' subscribe/recover requests in the server
        client_subscriptions=True,
        recovery_buffer_size=256,  # 256 messages per topic
        recovery_ttl=300,          # 5 minutes
        recovery_max_messages=500,
    )
    server.enable_drain_mode()
    server.start()

    token = make_token()
    print(f"WSE listening on {HOST}:{PORT} (recovery enabled)")
    print(f"Token: {token}")
    print()
    print("Recovery demo (with wse-client or the TypeScript client):")
    print("  1. Connect and subscribe to 'prices'")
    print("  2. Disconnect, wait a few seconds (messages accumulate in the buffer)")
    print("  3. Reconnect: the client re-subscribes with its last position")
    print("  4. It receives the missed messages before new ones")
    print()
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
