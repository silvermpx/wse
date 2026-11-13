"""Real RustWSEServer for the TS client end-to-end suite (realServer.test.ts).

Driven over stdin ("publish N", "kick", "stop", "start", "quit"); reports
events as JSON lines on stdout. Handles client subscription_update with
subscribe_with_recovery() and replies like examples/standalone_recovery.py.
Signs E2E sessions with an identity key made for the run (the same one
across a restart), which the client pins. Uses an installed wse-server, or
the prebuilt module of this repository; needs cryptography for the key.
"""

import json
import sys
import threading
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3]))
from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import ec  # noqa: E402

from wse_server._wse_accel import RustWSEServer, rust_jwt_encode  # noqa: E402

PORT = int(sys.argv[1])
SECRET = b"e2e-secret-key-32-bytes-long!!!!"
IDENTITY = ec.generate_private_key(ec.SECP256R1()).private_bytes(
    serialization.Encoding.PEM,
    serialization.PrivateFormat.PKCS8,
    serialization.NoEncryption(),
)


def emit(obj):
    print(json.dumps(obj), flush=True)


def make_token():
    now = int(time.time())
    return rust_jwt_encode({"sub": "alice", "exp": now + 3600, "iat": now}, SECRET)


class Runner:
    def __init__(self):
        self.server = None
        self.stop_evt = None
        self.thread = None
        self.seq = 0

    def start(self):
        s = RustWSEServer(
            "127.0.0.1",
            PORT,
            jwt_secret=SECRET,
            recovery_enabled=True,
            recovery_buffer_size=1024,
            e2e_identity_key=IDENTITY,
        )
        s.enable_drain_mode()
        s.start()
        self.server = s
        self.stop_evt = threading.Event()
        self.thread = threading.Thread(target=self.loop, args=(s, self.stop_evt), daemon=True)
        self.thread.start()
        emit({"event": "started"})

    def stop(self):
        if self.server:
            self.stop_evt.set()
            self.thread.join(timeout=2)
            self.server.stop()
            self.server = None
            emit({"event": "stopped"})

    def loop(self, s, stop):
        while not stop.is_set():
            for ev in s.drain_inbound(256, 50):
                kind, cid = ev[0], ev[1]
                data = ev[2] if len(ev) > 2 else None
                if kind == "auth_connect":
                    emit({"event": "auth_connect", "user": data})
                elif kind == "msg" and isinstance(data, dict):
                    t = data.get("t")
                    p = data.get("p") or {}
                    if t == "subscription_update" and p.get("action") == "subscribe":
                        topics = p.get("topics") or []
                        recover = bool(p.get("recover"))
                        first = (
                            next(iter((p.get("recovery") or {}).values()), {}) if recover else {}
                        )
                        offset = first.get("offset")
                        res = s.subscribe_with_recovery(
                            cid,
                            topics,
                            recover=recover,
                            epoch=first.get("epoch"),
                            offset=int(offset) if offset is not None else None,
                        )
                        s.send(
                            cid,
                            json.dumps(
                                {
                                    "c": "WSE",
                                    "t": "subscription_update",
                                    "p": {
                                        "action": "subscribe",
                                        "success": True,
                                        "success_topics": topics,
                                        "recovered": res.get("recovered"),
                                        "recovery": {
                                            tp: {
                                                "epoch": i.get("epoch"),
                                                "offset": i.get("offset"),
                                                "recovered": i.get("recovered"),
                                                "count": i.get("count"),
                                            }
                                            for tp, i in res.get("topics", {}).items()
                                        },
                                    },
                                    "v": 1,
                                }
                            ),
                        )
                        emit(
                            {
                                "event": "subscribe",
                                "topics": topics,
                                "recover": recover,
                                "recovered": res.get("recovered"),
                            }
                        )
                    elif t == "echo":
                        s.send(cid, json.dumps({"c": "U", "t": "echo_reply", "p": p, "v": 1}))
                        emit({"event": "echo", "payload": p})
                    else:
                        emit({"event": "msg", "t": t})
                elif kind == "disconnect":
                    emit({"event": "disconnect"})

    def publish(self, n):
        for _ in range(n):
            self.seq += 1
            self.server.broadcast_local(
                "prices", json.dumps({"t": "price_update", "p": {"seq": self.seq}})
            )
        emit({"event": "published", "upto": self.seq})


r = Runner()
r.start()
emit({"event": "identity", "key": r.server.e2e_identity_public_key()})
emit({"event": "token", "token": make_token()})
for line in sys.stdin:
    cmd = line.strip().split()
    if not cmd:
        continue
    if cmd[0] == "publish":
        r.publish(int(cmd[1]))
    elif cmd[0] == "kick":
        for cid in r.server.get_connections():
            r.server.disconnect(cid)
        emit({"event": "kicked"})
    elif cmd[0] == "stop":
        r.stop()
    elif cmd[0] == "start":
        r.start()
    elif cmd[0] == "quit":
        r.stop()
        break
