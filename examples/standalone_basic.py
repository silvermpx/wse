"""Basic standalone WSE server.

Start the Rust WebSocket server, handle connections,
echo messages back to the sender.

    pip install wse-server
    python examples/standalone_basic.py
"""

import signal
import threading
import time

from wse_server._wse_accel import RustWSEServer, rust_jwt_encode

# -- Config -------------------------------------------------------------------

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


# -- Event loop ---------------------------------------------------------------

def event_loop(server: RustWSEServer, stop: threading.Event):
    """Drain inbound events and handle them.

    Events are tuples: (event_type, conn_id, payload)

    Event types:
      - "auth_connect": new connection, payload = user_id
      - "msg":          parsed JSON from client, payload = dict
      - "raw":          unparsed text frame, payload = str
      - "bin":          binary frame, payload = bytes
      - "disconnect":   connection closed, no payload
    """
    connections: dict[str, str] = {}  # conn_id -> user_id

    while not stop.is_set():
        # drain_inbound(batch_size, timeout_ms)
        # blocks up to timeout_ms for the first event,
        # then drains up to batch_size without blocking
        events = server.drain_inbound(256, 50)

        for event in events:
            event_type = event[0]
            conn_id = event[1]

            if event_type == "auth_connect":
                user_id = event[2]
                connections[conn_id] = user_id
                print(f"[+] {user_id} connected ({conn_id})")
                # send a welcome message
                server.send(conn_id, '{"c":"U","t":"welcome","p":{"msg":"hello from WSE"},"v":1}')

            elif event_type == "msg":
                data = event[2]  # parsed dict
                user = connections.get(conn_id, "?")
                print(f"[msg] {user}: {data}")
                # echo back to sender
                server.send_event(conn_id, data)

            elif event_type == "disconnect":
                user = connections.pop(conn_id, "?")
                print(f"[-] {user} disconnected")

        # print active connections periodically
        if server.get_connection_count() > 0:
            pass  # server.get_connection_count() is lock-free (AtomicUsize)


# -- Main ---------------------------------------------------------------------

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
    print(f"Connect: ws://{HOST}:{PORT}/wse")
    print("Press Ctrl+C to stop.\n")

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    thread = threading.Thread(target=event_loop, args=(server, stop), daemon=True)
    thread.start()

    while not stop.is_set():
        stop.wait(1.0)

    server.stop()
    print("Stopped.")


if __name__ == "__main__":
    main()
