"""Presence tracking example.

Shows per-topic presence: who is currently in a room,
real-time join/leave notifications, presence data updates.

    pip install wse-server
    python examples/standalone_presence.py
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
    """Handle connections with presence tracking.

    Presence events:
      - "presence_join":   a user's first connection joined a topic
      - "presence_leave":  a user's last connection left a topic
    These are user-level: if the same user opens 3 tabs, only one
    join fires. Leave fires when the last tab closes.
    """
    connections: dict[str, str] = {}  # conn_id -> user_id

    while not stop.is_set():
        events = server.drain_inbound(256, 50)

        for event in events:
            event_type = event[0]

            if event_type == "auth_connect":
                conn_id = event[1]
                user_id = event[2]
                connections[conn_id] = user_id
                print(f"[+] {user_id} connected ({conn_id})")

                # Subscribe to "lobby" with presence data.
                # The presence dict can contain anything your app needs:
                # status, avatar URL, device type, etc.
                server.subscribe_connection(
                    conn_id,
                    ["lobby"],
                    {"status": "online", "name": user_id},
                )

            elif event_type == "presence_join":
                # event[1] is None (user-level, not connection-level)
                payload = event[2]  # {"topic": ..., "user_id": ..., "data": ...}
                print(
                    f"[presence] {payload['user_id']} joined "
                    f"{payload['topic']} - {payload['data']}"
                )

                # Query current members
                members = server.presence("lobby")
                print(f"  Members in lobby: {len(members)}")
                for uid, info in members.items():
                    print(f"    {uid}: {info['data']} ({info['connections']} conn)")

            elif event_type == "presence_leave":
                payload = event[2]
                print(
                    f"[presence] {payload['user_id']} left "
                    f"{payload['topic']}"
                )

                # Quick stats (O(1), no iteration)
                stats = server.presence_stats("lobby")
                print(
                    f"  Lobby: {stats['num_users']} users, "
                    f"{stats['num_connections']} connections"
                )

            elif event_type == "msg":
                conn_id = event[1]
                data = event[2]
                user = connections.get(conn_id, "?")
                print(f"[msg] {user}: {data}")

                # Example: update presence data on "set_status" message
                if data.get("t") == "set_status":
                    new_status = data.get("p", {}).get("status", "online")
                    server.update_presence(conn_id, {"status": new_status, "name": user})
                    print(f"  Updated {user} status to {new_status}")

            elif event_type == "disconnect":
                conn_id = event[1]
                user = connections.pop(conn_id, "?")
                print(f"[-] {user} disconnected")


# -- Main ---------------------------------------------------------------------

def main():
    server = RustWSEServer(
        HOST, PORT,
        max_connections=10_000,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
        # Enable presence tracking
        presence_enabled=True,
        presence_max_data_size=4096,  # 4 KB per user
    )
    server.enable_drain_mode()
    server.start()

    token = make_token()
    print(f"WSE listening on {HOST}:{PORT} (presence enabled)")
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
