"""Standalone WSE server with message recovery on reconnect.

Demonstrates message recovery: when a client disconnects and reconnects,
it can recover missed messages from the server's per-topic ring buffer
using epoch+offset tracking.

    pip install wse-server

    # Terminal 1: Start server with recovery enabled
    python examples/standalone_recovery.py

    # Terminal 2: Connect a WebSocket client, subscribe to 'prices',
    # disconnect, wait a few seconds, reconnect with recover=true
    # to receive missed messages.
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
    """Handle connections and subscriptions with recovery support."""
    while not stop.is_set():
        events = server.drain_inbound(256, 50)
        for event in events:
            event_type = event[0]
            conn_id = event[1]

            if event_type == "auth_connect":
                user_id = event[2]
                print(f"[+] {user_id} connected ({conn_id})")
                # Subscribe with recovery -- returns per-topic epoch/offset
                result = server.subscribe_with_recovery(
                    conn_id, ["prices"], recover=False,
                )
                print(f"    subscribe result: {result}")

            elif event_type == "msg":
                data = event[2]
                if isinstance(data, dict):
                    msg_type = data.get("t")
                    payload = data.get("p", {})

                    if msg_type == "subscription_update":
                        action = payload.get("action")
                        topics = payload.get("topics", [])
                        recover = payload.get("recover", False)
                        recovery_info = payload.get("recovery", {})

                        if action == "subscribe" and topics:
                            # Use per-topic epoch/offset from client
                            epoch = None
                            offset = None
                            if recover and recovery_info:
                                # Use the first topic's recovery info
                                first_info = next(iter(recovery_info.values()), {})
                                epoch = first_info.get("epoch")
                                offset = first_info.get("offset")

                            result = server.subscribe_with_recovery(
                                conn_id, topics,
                                recover=recover,
                                epoch=epoch,
                                offset=int(offset) if offset is not None else None,
                            )
                            recovered = result.get("recovered", False)
                            topics_info = result.get("topics", {})

                            # Send subscription confirmation with recovery info
                            response = json.dumps({
                                "c": "WSE",
                                "t": "subscription_update",
                                "p": {
                                    "action": "subscribe",
                                    "success": True,
                                    "success_topics": topics,
                                    "failed_topics": [],
                                    "recoverable": True,
                                    "recovered": recovered,
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
                                "v": 1,
                            })
                            server.send(conn_id, response)
                            print(f"    [{conn_id}] subscribe {topics} recover={recover} -> {result}")

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
                f"[pub] {seq} ticks | {conns} conns | "
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
    print("Recovery demo:")
    print("  1. Connect a client and subscribe to 'prices'")
    print("  2. Note the epoch+offset from subscription response")
    print("  3. Disconnect the client")
    print("  4. Wait a few seconds (messages accumulate in buffer)")
    print("  5. Reconnect with recover=true and epoch+offset")
    print("  6. Client receives missed messages before new ones")
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
